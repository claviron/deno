use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use claviron::db::DbService;
use deno_core::{Extension, OpState, op2};
use deno_error::JsErrorBox;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::ExtensionContext;
use crate::backend::deno::{ExtensionFactory, ExtensionModule};

#[cfg(feature = "mysql")]
mod mysql;
mod postgres;
mod stream;

use stream::{
    DatabaseStreamBatch, DatabaseStreamHandleRequest, DatabaseStreamOpenResult,
    DatabaseStreamRequest,
};

const MODULES: &[ExtensionModule] = &[ExtensionModule::new(
    "claviron:db",
    include_str!("database/modules/db.js"),
)];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
enum DatabaseBackend {
    Postgres,
    Mysql,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum DatabaseParameter {
    Null,
    Boolean(bool),
    I32(i32),
    I64(String),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseQuery {
    backend: DatabaseBackend,
    connection: String,
    sql: String,
    parameters: Vec<DatabaseParameter>,
    transaction: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum DatabaseIsolationLevel {
    #[serde(rename = "read uncommitted")]
    ReadUncommitted,
    #[serde(rename = "read committed")]
    ReadCommitted,
    #[serde(rename = "repeatable read")]
    RepeatableRead,
    #[serde(rename = "serializable")]
    Serializable,
    #[serde(rename = "snapshot")]
    Snapshot,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
enum DatabaseAccessMode {
    #[serde(rename = "read only")]
    ReadOnly,
    #[serde(rename = "read write")]
    ReadWrite,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseTransactionSettings {
    isolation_level: Option<DatabaseIsolationLevel>,
    access_mode: Option<DatabaseAccessMode>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseBeginRequest {
    backend: DatabaseBackend,
    connection: String,
    settings: DatabaseTransactionSettings,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseTransactionRequest {
    transaction: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseBeginResult {
    transaction: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum DatabaseValue {
    Null,
    Boolean(bool),
    I32(i32),
    I64(String),
    #[cfg(feature = "mysql")]
    U64(String),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseColumn {
    name: String,
    type_name: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
struct DatabaseQueryResult {
    columns: Vec<DatabaseColumn>,
    rows: Vec<Vec<DatabaseValue>>,
    num_affected_rows: String,
    insert_id: Option<String>,
}

impl Default for DatabaseQueryResult {
    fn default() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            num_affected_rows: "0".to_owned(),
            insert_id: None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
enum DatabaseError {
    #[error(
        "JavaScript database connection is not registered: {backend}.{connection}"
    )]
    ConnectionNotRegistered { backend: &'static str, connection: String },

    #[cfg(not(feature = "mysql"))]
    #[error("JavaScript database backend is not compiled: {0}")]
    BackendNotCompiled(&'static str),

    #[error("invalid 64-bit integer database parameter: {0}")]
    InvalidInteger(String),

    #[error("invalid JavaScript database transaction handle: {0}")]
    InvalidTransaction(String),

    #[error("JavaScript database transaction is not active: {0}")]
    TransactionNotActive(String),

    #[error("JavaScript database transaction is busy: {0}")]
    TransactionBusy(String),

    #[error("invalid JavaScript database stream handle: {0}")]
    InvalidStream(String),

    #[error("JavaScript database stream is not active: {0}")]
    StreamNotActive(String),

    #[error("database stream batch size must be greater than zero")]
    InvalidBatchSize,

    #[error("database stream task failed: {0}")]
    StreamTask(String),

    #[error(
        "JavaScript database transaction {transaction} belongs to {actual_backend}.{actual_connection}, not {requested_backend}.{requested_connection}"
    )]
    TransactionMismatch {
        transaction: String,
        actual_backend: &'static str,
        actual_connection: String,
        requested_backend: &'static str,
        requested_connection: String,
    },

    #[error(
        "{backend} does not support the requested transaction isolation level: snapshot"
    )]
    UnsupportedIsolationLevel { backend: &'static str },

    #[error(
        "unsupported {backend} column type '{type_name}' for column '{column}'"
    )]
    UnsupportedColumnType {
        backend: &'static str,
        column: String,
        type_name: String,
    },

    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
}

#[derive(Clone)]
struct DatabaseContext {
    service: Arc<DbService>,
    transactions: Arc<Mutex<HashMap<u64, TransactionSlot>>>,
    next_transaction: Arc<AtomicU64>,
    streams: stream::DatabaseStreams,
}

type TransactionSlot = Arc<Mutex<TransactionState>>;

impl DatabaseContext {
    fn new(service: Arc<DbService>) -> Self {
        Self {
            service,
            transactions: Arc::new(Mutex::new(HashMap::new())),
            next_transaction: Arc::new(AtomicU64::new(1)),
            streams: stream::DatabaseStreams::default(),
        }
    }

    fn transaction_id(&self) -> u64 {
        self.next_transaction.fetch_add(1, Ordering::Relaxed)
    }
}

enum TransactionState {
    Active(ActiveTransaction),
    Streaming,
    Finishing,
}

struct ActiveTransaction {
    backend: DatabaseBackend,
    connection: String,
    inner: DatabaseTransaction,
}

enum DatabaseTransaction {
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
    #[cfg(feature = "mysql")]
    Mysql(sqlx::Transaction<'static, sqlx::MySql>),
}

/// Adds the reload-aware `claviron:db` primitive to each JavaScript isolate.
///
/// The extension retains `DbService`, not a concrete pool snapshot. Every new
/// query or transaction therefore resolves the current named pool after a
/// database configuration reload. An active transaction intentionally stays
/// pinned to the pool connection on which it began.
pub struct DatabaseExtension {
    service: Arc<DbService>,
}

impl DatabaseExtension {
    pub fn new(service: Arc<DbService>) -> Self {
        Self { service }
    }
}

#[op2]
#[serde]
async fn op_claviron_database_execute(
    state: Rc<RefCell<OpState>>,
    #[serde] query: DatabaseQuery,
) -> Result<DatabaseQueryResult, JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    execute(&context, query).await.map_err(js_error)
}

#[op2]
#[serde]
async fn op_claviron_database_begin(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DatabaseBeginRequest,
) -> Result<DatabaseBeginResult, JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    begin(&context, request).await.map_err(js_error)
}

#[op2]
async fn op_claviron_database_commit(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DatabaseTransactionRequest,
) -> Result<(), JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    finish(&context, request, true).await.map_err(js_error)
}

#[op2]
async fn op_claviron_database_rollback(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DatabaseTransactionRequest,
) -> Result<(), JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    finish(&context, request, false).await.map_err(js_error)
}

#[op2]
#[serde]
async fn op_claviron_database_stream_open(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DatabaseStreamRequest,
) -> Result<DatabaseStreamOpenResult, JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    stream::open(&context, request).await.map_err(js_error)
}

#[op2]
#[serde]
async fn op_claviron_database_stream_next(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DatabaseStreamHandleRequest,
) -> Result<DatabaseStreamBatch, JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    stream::next(&context, request).await.map_err(js_error)
}

#[op2]
async fn op_claviron_database_stream_close(
    state: Rc<RefCell<OpState>>,
    #[serde] request: DatabaseStreamHandleRequest,
) -> Result<(), JsErrorBox> {
    let context = state.borrow().borrow::<DatabaseContext>().clone();
    stream::close(&context, request).await.map_err(js_error)
}

async fn execute(
    context: &DatabaseContext,
    query: DatabaseQuery,
) -> Result<DatabaseQueryResult, DatabaseError> {
    if let Some(handle) = query.transaction.as_deref() {
        let id = parse_transaction_id(handle)?;
        let slot =
            context.transactions.lock().await.get(&id).cloned().ok_or_else(
                || DatabaseError::TransactionNotActive(handle.to_owned()),
            )?;
        let mut slot = slot.lock().await;
        let TransactionState::Active(transaction) = &mut *slot else {
            return Err(DatabaseError::TransactionBusy(handle.to_owned()));
        };
        validate_transaction(handle, transaction, &query)?;
        return match &mut transaction.inner {
            DatabaseTransaction::Postgres(transaction) => {
                postgres::execute_transaction(transaction, query).await
            }
            #[cfg(feature = "mysql")]
            DatabaseTransaction::Mysql(transaction) => {
                mysql::execute_transaction(transaction, query).await
            }
        };
    }

    match query.backend {
        DatabaseBackend::Postgres => {
            postgres::execute(&context.service, query).await
        }
        DatabaseBackend::Mysql => {
            #[cfg(feature = "mysql")]
            {
                mysql::execute(&context.service, query).await
            }
            #[cfg(not(feature = "mysql"))]
            {
                let _ = context;
                let _ = query;
                Err(DatabaseError::BackendNotCompiled("mysql"))
            }
        }
    }
}

async fn begin(
    context: &DatabaseContext,
    request: DatabaseBeginRequest,
) -> Result<DatabaseBeginResult, DatabaseError> {
    let backend = request.backend;
    let connection = request.connection;
    let inner = match backend {
        DatabaseBackend::Postgres => DatabaseTransaction::Postgres(
            postgres::begin(&context.service, &connection, request.settings)
                .await?,
        ),
        DatabaseBackend::Mysql => {
            #[cfg(feature = "mysql")]
            {
                DatabaseTransaction::Mysql(
                    mysql::begin(
                        &context.service,
                        &connection,
                        request.settings,
                    )
                    .await?,
                )
            }
            #[cfg(not(feature = "mysql"))]
            {
                return Err(DatabaseError::BackendNotCompiled("mysql"));
            }
        }
    };
    let id = context.transaction_id();
    context.transactions.lock().await.insert(
        id,
        Arc::new(Mutex::new(TransactionState::Active(ActiveTransaction {
            backend,
            connection,
            inner,
        }))),
    );
    Ok(DatabaseBeginResult { transaction: format!("tx-{id}") })
}

async fn finish(
    context: &DatabaseContext,
    request: DatabaseTransactionRequest,
    commit: bool,
) -> Result<(), DatabaseError> {
    let id = parse_transaction_id(&request.transaction)?;
    let slot = context.transactions.lock().await.get(&id).cloned().ok_or_else(
        || DatabaseError::TransactionNotActive(request.transaction.clone()),
    )?;
    let transaction = {
        let mut state = slot.lock().await;
        match std::mem::replace(&mut *state, TransactionState::Finishing) {
            TransactionState::Active(transaction) => transaction,
            other => {
                *state = other;
                return Err(DatabaseError::TransactionBusy(
                    request.transaction,
                ));
            }
        }
    };
    context.transactions.lock().await.remove(&id);
    match transaction.inner {
        DatabaseTransaction::Postgres(transaction) => {
            if commit {
                transaction.commit().await?;
            } else {
                transaction.rollback().await?;
            }
        }
        #[cfg(feature = "mysql")]
        DatabaseTransaction::Mysql(transaction) => {
            if commit {
                transaction.commit().await?;
            } else {
                transaction.rollback().await?;
            }
        }
    }
    Ok(())
}

fn parse_transaction_id(handle: &str) -> Result<u64, DatabaseError> {
    handle
        .strip_prefix("tx-")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| DatabaseError::InvalidTransaction(handle.to_owned()))
}

fn validate_transaction(
    handle: &str,
    transaction: &ActiveTransaction,
    query: &DatabaseQuery,
) -> Result<(), DatabaseError> {
    if transaction.backend == query.backend
        && transaction.connection == query.connection
    {
        return Ok(());
    }
    Err(DatabaseError::TransactionMismatch {
        transaction: handle.to_owned(),
        actual_backend: transaction.backend.name(),
        actual_connection: transaction.connection.clone(),
        requested_backend: query.backend.name(),
        requested_connection: query.connection.clone(),
    })
}

impl DatabaseBackend {
    const fn name(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
        }
    }
}

fn js_error(error: DatabaseError) -> JsErrorBox {
    JsErrorBox::generic(error.to_string())
}

deno_core::extension!(
    claviron_database_extension,
    ops = [
        op_claviron_database_begin,
        op_claviron_database_commit,
        op_claviron_database_execute,
        op_claviron_database_rollback,
        op_claviron_database_stream_close,
        op_claviron_database_stream_next,
        op_claviron_database_stream_open,
    ],
    esm_entry_point = "ext:claviron_database_extension/database.js",
    esm = [dir "src/extensions/database", "database.js"],
    options = {
        context: DatabaseContext,
    },
    state = |state, options| {
        state.put(options.context);
    },
);

impl ExtensionFactory for DatabaseExtension {
    fn extension(
        &self,
        _context: ExtensionContext,
    ) -> Extension {
        claviron_database_extension::init(DatabaseContext::new(Arc::clone(
            &self.service,
        )))
    }

    fn modules(&self) -> &'static [ExtensionModule] {
        MODULES
    }
}

#[cfg(test)]
#[path = "../../tests/extensions/database.rs"]
mod tests;
