use claviron::db::DbService;
use futures_util::TryStreamExt;
use sqlx::mysql::{
    MySqlArguments, MySqlConnection, MySqlQueryResult, MySqlRow,
};
use sqlx::pool::MaybePoolConnection;
use sqlx::query::Query;
use sqlx::{
    Column, Either, MySql, Row, SqlSafeStr, Transaction, TypeInfo, ValueRef,
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
) -> Result<Transaction<'static, MySql>, DatabaseError> {
    let pool = pool(service, connection)?;
    let mut connection = pool.acquire().await?;
    if let Some(level) = settings.isolation_level {
        let statement = match level {
            DatabaseIsolationLevel::ReadUncommitted => {
                "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
            }
            DatabaseIsolationLevel::ReadCommitted => {
                "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
            }
            DatabaseIsolationLevel::RepeatableRead => {
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            }
            DatabaseIsolationLevel::Serializable => {
                "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            }
            DatabaseIsolationLevel::Snapshot => {
                return Err(DatabaseError::UnsupportedIsolationLevel {
                    backend: "mysql",
                });
            }
        };
        sqlx::query(sqlx::AssertSqlSafe(statement))
            .execute(&mut *connection)
            .await?;
    }
    let statement = match settings.access_mode {
        Some(DatabaseAccessMode::ReadOnly) => "START TRANSACTION READ ONLY",
        Some(DatabaseAccessMode::ReadWrite) => "START TRANSACTION READ WRITE",
        None => "START TRANSACTION",
    };
    Ok(Transaction::begin(
        MaybePoolConnection::PoolConnection(connection),
        Some(sqlx::AssertSqlSafe(statement).into_sql_str()),
    )
    .await?)
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
    transaction: &mut Transaction<'static, MySql>,
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
    transaction: &mut Transaction<'static, MySql>,
    query: DatabaseQuery,
    batch_size: usize,
    sender: &DatabaseStreamSender,
) -> Result<(), DatabaseError> {
    stream_on(transaction, query, batch_size, sender).await
}

async fn stream_on(
    connection: &mut MySqlConnection,
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
    connection: &mut MySqlConnection,
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
) -> Result<sqlx::MySqlPool, DatabaseError> {
    service.mysql_named(connection).ok_or_else(|| {
        DatabaseError::ConnectionNotRegistered {
            backend: "mysql",
            connection: connection.to_owned(),
        }
    })
}

fn bind<'q>(
    query: Query<'q, MySql, MySqlArguments>,
    parameter: DatabaseParameter,
) -> Result<Query<'q, MySql, MySqlArguments>, DatabaseError> {
    Ok(match parameter {
        DatabaseParameter::Null => query.bind(Option::<String>::None),
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

fn accumulate_result(
    output: &mut DatabaseQueryResult,
    result: MySqlQueryResult,
) {
    let affected = output.num_affected_rows.parse::<u64>().unwrap_or_default();
    output.num_affected_rows =
        affected.saturating_add(result.rows_affected()).to_string();
    if result.last_insert_id() != 0 {
        output.insert_id = Some(result.last_insert_id().to_string());
    }
}

fn push_row(
    output: &mut DatabaseQueryResult,
    row: MySqlRow,
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
    row: &MySqlRow,
    index: usize,
    column: &str,
    type_name: &str,
) -> Result<DatabaseValue, DatabaseError> {
    if row.try_get_raw(index)?.is_null() {
        return Ok(DatabaseValue::Null);
    }
    Ok(match type_name {
        "BOOLEAN" => DatabaseValue::Boolean(row.try_get(index)?),
        "TINYINT" => {
            DatabaseValue::I32(i32::from(row.try_get::<i8, _>(index)?))
        }
        "SMALLINT" => {
            DatabaseValue::I32(i32::from(row.try_get::<i16, _>(index)?))
        }
        "MEDIUMINT" | "INT" => DatabaseValue::I32(row.try_get(index)?),
        "BIGINT" => {
            DatabaseValue::I64(row.try_get::<i64, _>(index)?.to_string())
        }
        "TINYINT UNSIGNED" => {
            DatabaseValue::I32(i32::from(row.try_get::<u8, _>(index)?))
        }
        "SMALLINT UNSIGNED" => {
            DatabaseValue::I32(i32::from(row.try_get::<u16, _>(index)?))
        }
        "MEDIUMINT UNSIGNED" | "INT UNSIGNED" => DatabaseValue::I64(
            i64::from(row.try_get::<u32, _>(index)?).to_string(),
        ),
        "BIGINT UNSIGNED" => {
            DatabaseValue::U64(row.try_get::<u64, _>(index)?.to_string())
        }
        "FLOAT" => DatabaseValue::F64(f64::from(row.try_get::<f32, _>(index)?)),
        "DOUBLE" => DatabaseValue::F64(row.try_get(index)?),
        "JSON" => DatabaseValue::Json(
            row.try_get::<sqlx::types::Json<serde_json::Value>, _>(index)?.0,
        ),
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB"
        | "LONGBLOB" => DatabaseValue::Bytes(row.try_get(index)?),
        "CHAR" | "VARCHAR" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT"
        | "LONGTEXT" | "ENUM" | "SET" => {
            DatabaseValue::String(row.try_get(index)?)
        }
        _ => {
            return Err(DatabaseError::UnsupportedColumnType {
                backend: "mysql",
                column: column.to_owned(),
                type_name: type_name.to_owned(),
            });
        }
    })
}
