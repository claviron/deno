use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use claviron::db::DbService;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinHandle;

use super::{
    ActiveTransaction, DatabaseBackend, DatabaseContext, DatabaseError,
    DatabaseQuery, DatabaseQueryResult, DatabaseTransaction, TransactionSlot,
    TransactionState, parse_transaction_id, validate_transaction,
};

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct DatabaseStreamRequest {
    #[serde(flatten)]
    query: DatabaseQuery,
    batch_size: usize,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct DatabaseStreamHandleRequest {
    stream: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct DatabaseStreamOpenResult {
    stream: String,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(super) struct DatabaseStreamBatch {
    done: bool,
    result: Option<DatabaseQueryResult>,
}

#[derive(Clone, Default)]
pub(super) struct DatabaseStreams {
    entries: Arc<Mutex<HashMap<u64, Arc<DatabaseStream>>>>,
    next: Arc<AtomicU64>,
}

pub(super) type DatabaseStreamSender =
    mpsc::Sender<Result<DatabaseQueryResult, DatabaseError>>;

struct DatabaseStream {
    receiver: Mutex<mpsc::Receiver<Result<DatabaseQueryResult, DatabaseError>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl DatabaseStreams {
    fn id(&self) -> u64 {
        self.next.fetch_add(1, Ordering::Relaxed)
    }
}

pub(super) async fn open(
    context: &DatabaseContext,
    request: DatabaseStreamRequest,
) -> Result<DatabaseStreamOpenResult, DatabaseError> {
    if request.batch_size == 0 {
        return Err(DatabaseError::InvalidBatchSize);
    }

    let transaction = match request.query.transaction.as_deref() {
        Some(handle) => {
            let id = parse_transaction_id(handle)?;
            let slot = context
                .transactions
                .lock()
                .await
                .get(&id)
                .cloned()
                .ok_or_else(|| {
                    DatabaseError::TransactionNotActive(handle.to_owned())
                })?;
            let transaction = {
                let mut state = slot.lock().await;
                let current =
                    std::mem::replace(&mut *state, TransactionState::Streaming);
                match current {
                    TransactionState::Active(transaction) => {
                        if let Err(error) = validate_transaction(
                            handle,
                            &transaction,
                            &request.query,
                        ) {
                            *state = TransactionState::Active(transaction);
                            return Err(error);
                        }
                        transaction
                    }
                    other => {
                        *state = other;
                        return Err(DatabaseError::TransactionBusy(
                            handle.to_owned(),
                        ));
                    }
                }
            };
            Some((transaction, slot))
        }
        None => None,
    };

    let (sender, receiver) = mpsc::channel(1);
    let service = Arc::clone(&context.service);
    let query = request.query;
    let batch_size = request.batch_size;
    let task = tokio::spawn(async move {
        run(service, query, batch_size, transaction, sender).await;
    });
    let id = context.streams.id();
    context.streams.entries.lock().await.insert(
        id,
        Arc::new(DatabaseStream {
            receiver: Mutex::new(receiver),
            task: Mutex::new(Some(task)),
        }),
    );
    Ok(DatabaseStreamOpenResult { stream: format!("stream-{id}") })
}

async fn run(
    service: Arc<DbService>,
    query: DatabaseQuery,
    batch_size: usize,
    transaction: Option<(ActiveTransaction, TransactionSlot)>,
    sender: DatabaseStreamSender,
) {
    match transaction {
        Some((mut transaction, slot)) => {
            let result = match &mut transaction.inner {
                DatabaseTransaction::Postgres(inner) => {
                    super::postgres::stream_transaction(
                        inner, query, batch_size, &sender,
                    )
                    .await
                }
                #[cfg(feature = "mysql")]
                DatabaseTransaction::Mysql(inner) => {
                    super::mysql::stream_transaction(
                        inner, query, batch_size, &sender,
                    )
                    .await
                }
            };
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
            *slot.lock().await = TransactionState::Active(transaction);
        }
        None => {
            let result = match query.backend {
                DatabaseBackend::Postgres => {
                    super::postgres::stream(
                        &service, query, batch_size, &sender,
                    )
                    .await
                }
                DatabaseBackend::Mysql => {
                    #[cfg(feature = "mysql")]
                    {
                        super::mysql::stream(
                            &service, query, batch_size, &sender,
                        )
                        .await
                    }
                    #[cfg(not(feature = "mysql"))]
                    {
                        let _ = service;
                        let _ = query;
                        Err(DatabaseError::BackendNotCompiled("mysql"))
                    }
                }
            };
            if let Err(error) = result {
                let _ = sender.send(Err(error)).await;
            }
        }
    }
}

pub(super) async fn next(
    context: &DatabaseContext,
    request: DatabaseStreamHandleRequest,
) -> Result<DatabaseStreamBatch, DatabaseError> {
    let id = parse_id(&request.stream)?;
    let stream =
        context.streams.entries.lock().await.get(&id).cloned().ok_or_else(
            || DatabaseError::StreamNotActive(request.stream.clone()),
        )?;
    let message = stream.receiver.lock().await.recv().await;
    match message {
        Some(Ok(result)) => {
            Ok(DatabaseStreamBatch { done: false, result: Some(result) })
        }
        Some(Err(error)) => {
            close(context, request).await?;
            Err(error)
        }
        None => {
            close(context, request).await?;
            Ok(DatabaseStreamBatch { done: true, result: None })
        }
    }
}

pub(super) async fn close(
    context: &DatabaseContext,
    request: DatabaseStreamHandleRequest,
) -> Result<(), DatabaseError> {
    let id = parse_id(&request.stream)?;
    let Some(stream) = context.streams.entries.lock().await.remove(&id) else {
        return Ok(());
    };
    stream.receiver.lock().await.close();
    if let Some(task) = stream.task.lock().await.take() {
        task.await
            .map_err(|error| DatabaseError::StreamTask(error.to_string()))?;
    }
    Ok(())
}

fn parse_id(handle: &str) -> Result<u64, DatabaseError> {
    handle
        .strip_prefix("stream-")
        .and_then(|id| id.parse().ok())
        .ok_or_else(|| DatabaseError::InvalidStream(handle.to_owned()))
}

#[cfg(test)]
#[path = "../../../tests/extensions/database/stream.rs"]
mod tests;
