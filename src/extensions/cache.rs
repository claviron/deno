use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use claviron::cache::{CacheJson, CacheProvider, CacheRepository, CacheValue};
use deno_core::{Extension, OpState, op2};
use deno_error::JsErrorBox;
use serde::{Deserialize, Serialize};

use crate::ExtensionContext;
use crate::backend::deno::{ExtensionFactory, ExtensionModule};

const MODULES: &[ExtensionModule] = &[ExtensionModule::new(
    "claviron:cache",
    include_str!("cache/modules/cache.js"),
)];

#[derive(Clone)]
struct CacheContext {
    provider: Option<Arc<CacheProvider>>,
    repositories: Arc<Mutex<HashMap<u64, Arc<CacheRepository>>>>,
    next_repository: Arc<AtomicU64>,
}

impl CacheContext {
    fn new(provider: Option<Arc<CacheProvider>>) -> Self {
        Self {
            provider,
            repositories: Arc::new(Mutex::new(HashMap::new())),
            next_repository: Arc::new(AtomicU64::new(1)),
        }
    }

    fn provider(&self) -> Result<&CacheProvider, CacheError> {
        self.provider.as_deref().ok_or(CacheError::ProviderUnavailable)
    }

    fn repository(
        &self,
        handle: &str,
    ) -> Result<Arc<CacheRepository>, CacheError> {
        let id = handle
            .strip_prefix("cache-")
            .and_then(|id| id.parse::<u64>().ok())
            .ok_or_else(|| CacheError::InvalidHandle(handle.to_owned()))?;
        self.repositories
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or_else(|| CacheError::InvalidHandle(handle.to_owned()))
    }
}

#[derive(Debug, thiserror::Error)]
enum CacheError {
    #[error("Claviron cache provider is not registered")]
    ProviderUnavailable,

    #[error("cache store name must not be empty")]
    EmptyStoreName,

    #[error("invalid JavaScript cache handle: {0}")]
    InvalidHandle(String),

    #[error("invalid cache integer: {0}")]
    InvalidInteger(String),

    #[error("cache TTL must be a signed 64-bit integer")]
    InvalidTtl,

    #[error(transparent)]
    Cache(#[from] claviron::cache::Error),
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheStoreRequest {
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CacheKeyRequest {
    handle: String,
    key: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CachePutRequest {
    handle: String,
    key: String,
    value: CacheWireValue,
    ttl_seconds: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum CacheWireValue {
    Null,
    Boolean(bool),
    I64(String),
    U64(String),
    F64(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<CacheWireValue>),
    Object(Vec<(String, CacheWireValue)>),
}

/// Adds named Claviron cache stores to each JavaScript isolate.
///
/// A store handle pins the repository generation resolved when `store()` was
/// called. Calling `store()` again after a cache reload sees the new catalog.
pub struct CacheExtension {
    provider: Option<Arc<CacheProvider>>,
}

impl CacheExtension {
    pub fn new(provider: Option<Arc<CacheProvider>>) -> Self {
        Self { provider }
    }
}

#[op2]
#[string]
fn op_claviron_cache_store(
    state: &OpState,
    #[serde] request: CacheStoreRequest,
) -> Result<String, JsErrorBox> {
    let context = state.borrow::<CacheContext>();
    let provider = context.provider().map_err(js_error)?;
    let repository = match request.name {
        Some(name) => {
            if name.is_empty() {
                return Err(js_error(CacheError::EmptyStoreName));
            }
            provider.store(&name)
        }
        None => provider.default_store(),
    }
    .map_err(CacheError::from)
    .map_err(js_error)?;
    let id = context.next_repository.fetch_add(1, Ordering::Relaxed);
    context.repositories.lock().unwrap().insert(id, repository);
    Ok(format!("cache-{id}"))
}

#[op2]
#[serde]
async fn op_claviron_cache_get(
    state: Rc<RefCell<OpState>>,
    #[serde] request: CacheKeyRequest,
) -> Result<Option<CacheWireValue>, JsErrorBox> {
    let context = state.borrow().borrow::<CacheContext>().clone();
    let repository = context.repository(&request.handle).map_err(js_error)?;
    repository
        .get_value(&request.key)
        .await
        .map_err(CacheError::from)
        .map(|value| value.map(CacheWireValue::from))
        .map_err(js_error)
}

#[op2]
async fn op_claviron_cache_put(
    state: Rc<RefCell<OpState>>,
    #[serde] request: CachePutRequest,
) -> Result<(), JsErrorBox> {
    let context = state.borrow().borrow::<CacheContext>().clone();
    let repository = context.repository(&request.handle).map_err(js_error)?;
    let ttl = parse_ttl(request.ttl_seconds.as_deref()).map_err(js_error)?;
    if matches!(ttl, CacheTtl::Forget) {
        return repository
            .forget(&request.key)
            .await
            .map_err(CacheError::from)
            .map_err(js_error);
    }
    let value = CacheValue::try_from(request.value).map_err(js_error)?;
    repository
        .put_value(&request.key, value, ttl.duration())
        .await
        .map_err(CacheError::from)
        .map_err(js_error)
}

#[op2]
async fn op_claviron_cache_forget(
    state: Rc<RefCell<OpState>>,
    #[serde] request: CacheKeyRequest,
) -> Result<(), JsErrorBox> {
    let context = state.borrow().borrow::<CacheContext>().clone();
    let repository = context.repository(&request.handle).map_err(js_error)?;
    repository
        .forget(&request.key)
        .await
        .map_err(CacheError::from)
        .map_err(js_error)
}

#[op2]
async fn op_claviron_cache_clear(
    state: Rc<RefCell<OpState>>,
    #[string] handle: String,
) -> Result<(), JsErrorBox> {
    let context = state.borrow().borrow::<CacheContext>().clone();
    let repository = context.repository(&handle).map_err(js_error)?;
    repository.clear().await.map_err(CacheError::from).map_err(js_error)
}

enum CacheTtl {
    None,
    Forget,
    Duration(Duration),
}

impl CacheTtl {
    fn duration(&self) -> Option<Duration> {
        match self {
            Self::Duration(duration) => Some(*duration),
            Self::None | Self::Forget => None,
        }
    }
}

fn parse_ttl(value: Option<&str>) -> Result<CacheTtl, CacheError> {
    let Some(value) = value else {
        return Ok(CacheTtl::None);
    };
    let seconds = value.parse::<i64>().map_err(|_| CacheError::InvalidTtl)?;
    if seconds <= 0 {
        Ok(CacheTtl::Forget)
    } else {
        Ok(CacheTtl::Duration(Duration::from_secs(seconds as u64)))
    }
}

impl TryFrom<CacheWireValue> for CacheValue {
    type Error = CacheError;

    fn try_from(value: CacheWireValue) -> Result<Self, Self::Error> {
        match value {
            CacheWireValue::Null => Ok(Self::Null),
            CacheWireValue::Boolean(value) => Ok(Self::Bool(value)),
            CacheWireValue::I64(value) => value
                .parse()
                .map(Self::I64)
                .map_err(|_| CacheError::InvalidInteger(value)),
            CacheWireValue::U64(value) => value
                .parse()
                .map(Self::U64)
                .map_err(|_| CacheError::InvalidInteger(value)),
            CacheWireValue::F64(value) if value.is_finite() => {
                Ok(Self::F64(value))
            }
            CacheWireValue::F64(_) => {
                Err(CacheError::InvalidInteger("non-finite f64".into()))
            }
            CacheWireValue::String(value) => Ok(Self::String(value)),
            CacheWireValue::Bytes(value) => Ok(Self::Bytes(value)),
            CacheWireValue::Array(values) => values
                .into_iter()
                .map(CacheJson::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map(CacheJson::Array)
                .map(Self::Json),
            CacheWireValue::Object(values) => values
                .into_iter()
                .map(|(key, value)| {
                    CacheJson::try_from(value).map(|value| (key, value))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(CacheJson::Object)
                .map(Self::Json),
        }
    }
}

impl TryFrom<CacheWireValue> for CacheJson {
    type Error = CacheError;

    fn try_from(value: CacheWireValue) -> Result<Self, Self::Error> {
        match CacheValue::try_from(value)? {
            CacheValue::Null => Ok(Self::Null),
            CacheValue::Bool(value) => Ok(Self::Bool(value)),
            CacheValue::I64(value) => Ok(Self::I64(value)),
            CacheValue::U64(value) => Ok(Self::U64(value)),
            CacheValue::F64(value) => Ok(Self::F64(value)),
            CacheValue::String(value) => Ok(Self::String(value)),
            CacheValue::Json(value) => Ok(value),
            CacheValue::Bytes(_) => Err(CacheError::Cache(claviron::cache::Error::Rejected(
                "byte arrays cannot be nested inside cache arrays or objects".into()
            )))
        }
    }
}

impl From<CacheValue> for CacheWireValue {
    fn from(value: CacheValue) -> Self {
        match value {
            CacheValue::Null => Self::Null,
            CacheValue::Bool(value) => Self::Boolean(value),
            CacheValue::I64(value) => Self::I64(value.to_string()),
            CacheValue::U64(value) => Self::U64(value.to_string()),
            CacheValue::F64(value) => Self::F64(value),
            CacheValue::String(value) => Self::String(value),
            CacheValue::Bytes(value) => Self::Bytes(value),
            CacheValue::Json(value) => Self::from(value),
        }
    }
}

impl From<CacheJson> for CacheWireValue {
    fn from(value: CacheJson) -> Self {
        match value {
            CacheJson::Null => Self::Null,
            CacheJson::Bool(value) => Self::Boolean(value),
            CacheJson::I64(value) => Self::I64(value.to_string()),
            CacheJson::U64(value) => Self::U64(value.to_string()),
            CacheJson::F64(value) => Self::F64(value),
            CacheJson::String(value) => Self::String(value),
            CacheJson::Array(values) => {
                Self::Array(values.into_iter().map(Self::from).collect())
            }
            CacheJson::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
        }
    }
}

fn js_error(error: CacheError) -> JsErrorBox {
    JsErrorBox::generic(error.to_string())
}

deno_core::extension!(
    claviron_cache_extension,
    ops = [
        op_claviron_cache_clear,
        op_claviron_cache_forget,
        op_claviron_cache_get,
        op_claviron_cache_put,
        op_claviron_cache_store,
    ],
    esm_entry_point = "ext:claviron_cache_extension/cache.js",
    esm = [dir "src/extensions/cache", "cache.js"],
    options = {
        context: CacheContext,
    },
    state = |state, options| {
        state.put(options.context);
    },
);

impl ExtensionFactory for CacheExtension {
    fn extension(
        &self,
        _context: ExtensionContext,
    ) -> Extension {
        crate::engine::embedded::extension(claviron_cache_extension::init(
            CacheContext::new(self.provider.clone()),
        ))
    }

    fn modules(&self) -> &'static [ExtensionModule] {
        MODULES
    }
}

#[cfg(test)]
#[path = "../../tests/extensions/cache.rs"]
mod tests;
