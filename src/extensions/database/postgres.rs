use claviron::db::DbService;
use futures_util::TryStreamExt;
use sqlx::encode::{Encode, IsNull};
use sqlx::error::BoxDynError;
use sqlx::postgres::types::Oid;
use sqlx::postgres::{
    PgArgumentBuffer, PgArguments, PgConnection, PgQueryResult, PgRow,
    PgTypeInfo,
};
use sqlx::query::Query;
use sqlx::{
    Column, Either, Postgres, Row, Transaction, Type, TypeInfo, ValueRef,
};

use super::stream::DatabaseStreamSender;
use super::{
    DatabaseAccessMode, DatabaseColumn, DatabaseError, DatabaseIsolationLevel,
    DatabaseParameter, DatabaseQuery, DatabaseQueryResult,
    DatabaseTransactionSettings, DatabaseValue,
};

pub(super) async fn begin(
    service: &DbService,
    connection: &str,
    settings: DatabaseTransactionSettings,
) -> Result<Transaction<'static, Postgres>, DatabaseError> {
    let pool = pool(service, connection)?;
    let statement = transaction_statement(settings)?;
    Ok(pool.begin_with(sqlx::AssertSqlSafe(statement)).await?)
}

pub(super) async fn execute(
    service: &DbService,
    query: DatabaseQuery,
) -> Result<DatabaseQueryResult, DatabaseError> {
    let pool = pool(service, &query.connection)?;
    let mut connection = pool.acquire().await?;
    execute_on(&mut connection, query).await
}

pub(super) async fn execute_transaction(
    transaction: &mut Transaction<'static, Postgres>,
    query: DatabaseQuery,
) -> Result<DatabaseQueryResult, DatabaseError> {
    execute_on(transaction, query).await
}

pub(super) async fn stream(
    service: &DbService,
    query: DatabaseQuery,
    batch_size: usize,
    sender: &DatabaseStreamSender,
) -> Result<(), DatabaseError> {
    let pool = pool(service, &query.connection)?;
    let mut connection = pool.acquire().await?;
    stream_on(&mut connection, query, batch_size, sender).await
}

pub(super) async fn stream_transaction(
    transaction: &mut Transaction<'static, Postgres>,
    query: DatabaseQuery,
    batch_size: usize,
    sender: &DatabaseStreamSender,
) -> Result<(), DatabaseError> {
    stream_on(transaction, query, batch_size, sender).await
}

async fn stream_on(
    connection: &mut PgConnection,
    query: DatabaseQuery,
    batch_size: usize,
    sender: &DatabaseStreamSender,
) -> Result<(), DatabaseError> {
    let mut statement = sqlx::query(sqlx::AssertSqlSafe(query.sql.as_str()));
    for parameter in query.parameters {
        statement = bind(statement, parameter)?;
    }

    let mut rows = statement.fetch(connection);
    let mut batch = DatabaseQueryResult::default();
    loop {
        let row = tokio::select! {
            () = sender.closed() => return Ok(()),
            row = rows.try_next() => row?,
        };
        let Some(row) = row else {
            break;
        };
        push_row(&mut batch, row)?;
        if batch.rows.len() == batch_size
            && sender.send(Ok(std::mem::take(&mut batch))).await.is_err()
        {
            return Ok(());
        }
    }
    if !batch.rows.is_empty() {
        let _ = sender.send(Ok(batch)).await;
    }
    Ok(())
}

async fn execute_on(
    connection: &mut PgConnection,
    query: DatabaseQuery,
) -> Result<DatabaseQueryResult, DatabaseError> {
    // SQL is the explicit input of this driver primitive; values remain
    // separate bind parameters and are never interpolated here.
    let mut statement = sqlx::query(sqlx::AssertSqlSafe(query.sql.as_str()));
    for parameter in query.parameters {
        statement = bind(statement, parameter)?;
    }

    #[allow(deprecated)]
    let mut stream = statement.fetch_many(connection);
    let mut result = DatabaseQueryResult::default();
    while let Some(item) = stream.try_next().await? {
        match item {
            Either::Left(done) => accumulate_result(&mut result, done),
            Either::Right(row) => push_row(&mut result, row)?,
        }
    }
    Ok(result)
}

fn pool(
    service: &DbService,
    connection: &str,
) -> Result<sqlx::PgPool, DatabaseError> {
    service.postgres_named(connection).ok_or_else(|| {
        DatabaseError::ConnectionNotRegistered {
            backend: "postgres",
            connection: connection.to_owned(),
        }
    })
}

fn transaction_statement(
    settings: DatabaseTransactionSettings
) -> Result<String, DatabaseError> {
    let mut statement = String::from("BEGIN");
    if let Some(level) = settings.isolation_level {
        statement.push_str(" ISOLATION LEVEL ");
        statement.push_str(match level {
            DatabaseIsolationLevel::ReadUncommitted => "READ UNCOMMITTED",
            DatabaseIsolationLevel::ReadCommitted => "READ COMMITTED",
            DatabaseIsolationLevel::RepeatableRead => "REPEATABLE READ",
            DatabaseIsolationLevel::Serializable => "SERIALIZABLE",
            DatabaseIsolationLevel::Snapshot => {
                return Err(DatabaseError::UnsupportedIsolationLevel {
                    backend: "postgres",
                });
            }
        });
    }
    if let Some(mode) = settings.access_mode {
        statement.push_str(match mode {
            DatabaseAccessMode::ReadOnly => " READ ONLY",
            DatabaseAccessMode::ReadWrite => " READ WRITE",
        });
    }
    Ok(statement)
}

fn bind<'q>(
    query: Query<'q, Postgres, PgArguments>,
    parameter: DatabaseParameter,
) -> Result<Query<'q, Postgres, PgArguments>, DatabaseError> {
    Ok(match parameter {
        DatabaseParameter::Null => query.bind(PgNull),
        DatabaseParameter::Boolean(value) => query.bind(value),
        DatabaseParameter::I32(value) => query.bind(value),
        DatabaseParameter::I64(value) => query.bind(
            value
                .parse::<i64>()
                .map_err(|_| DatabaseError::InvalidInteger(value.clone()))?,
        ),
        DatabaseParameter::F64(value) => query.bind(value),
        DatabaseParameter::String(value) => query.bind(value),
        DatabaseParameter::Bytes(value) => query.bind(value),
        DatabaseParameter::Json(value) => query.bind(sqlx::types::Json(value)),
    })
}

/// PostgreSQL OID zero asks the server to infer the parameter type from the
/// SQL expression. This keeps an untyped JavaScript `null` usable for any
/// nullable target column instead of incorrectly declaring it as TEXT.
struct PgNull;

impl Type<Postgres> for PgNull {
    fn type_info() -> PgTypeInfo {
        PgTypeInfo::with_oid(Oid(0))
    }
}

impl Encode<'_, Postgres> for PgNull {
    fn encode_by_ref(
        &self,
        _buffer: &mut PgArgumentBuffer,
    ) -> Result<IsNull, BoxDynError> {
        Ok(IsNull::Yes)
    }
}

fn accumulate_result(
    output: &mut DatabaseQueryResult,
    result: PgQueryResult,
) {
    let affected = output.num_affected_rows.parse::<u64>().unwrap_or_default();
    output.num_affected_rows =
        affected.saturating_add(result.rows_affected()).to_string();
}

fn push_row(
    output: &mut DatabaseQueryResult,
    row: PgRow,
) -> Result<(), DatabaseError> {
    if output.columns.is_empty() {
        output.columns = row
            .columns()
            .iter()
            .map(|column| DatabaseColumn {
                name: column.name().to_owned(),
                type_name: column.type_info().name().to_owned(),
            })
            .collect();
    }
    let values = row
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            decode(&row, index, column.name(), column.type_info().name())
        })
        .collect::<Result<Vec<_>, _>>()?;
    output.rows.push(values);
    Ok(())
}

fn decode(
    row: &PgRow,
    index: usize,
    column: &str,
    type_name: &str,
) -> Result<DatabaseValue, DatabaseError> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(DatabaseValue::Null);
    }
    Ok(match type_name {
        "BOOL" => DatabaseValue::Boolean(row.try_get(index)?),
        "INT2" => DatabaseValue::I32(i32::from(row.try_get::<i16, _>(index)?)),
        "INT4" => DatabaseValue::I32(row.try_get(index)?),
        "INT8" => DatabaseValue::I64(row.try_get::<i64, _>(index)?.to_string()),
        "FLOAT4" => {
            DatabaseValue::F64(f64::from(row.try_get::<f32, _>(index)?))
        }
        "FLOAT8" => DatabaseValue::F64(row.try_get(index)?),
        "BYTEA" => DatabaseValue::Bytes(row.try_get(index)?),
        "JSON" | "JSONB" => DatabaseValue::Json(
            row.try_get::<sqlx::types::Json<serde_json::Value>, _>(index)?.0,
        ),
        "CHAR" | "NAME" | "TEXT" | "VARCHAR" | "BPCHAR" | "UNKNOWN" => {
            DatabaseValue::String(row.try_get(index)?)
        }
        _ => {
            return Err(DatabaseError::UnsupportedColumnType {
                backend: "postgres",
                column: column.to_owned(),
                type_name: type_name.to_owned(),
            });
        }
    })
}
