use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("JavaScript application name cannot be empty")]
    EmptyApplicationName,

    #[error("JavaScript main module path is not an absolute file path: {0}")]
    InvalidModulePath(PathBuf),

    #[error("JavaScript application instance count must be greater than zero")]
    InvalidInstanceCount,

    #[error("JavaScript application returned an invalid response")]
    InvalidApplicationResponse,

    #[error("invalid or duplicate JavaScript extension module: {0}")]
    InvalidExtensionModule(&'static str),

    #[error("JavaScript application is unavailable: {0}")]
    ApplicationUnavailable(String),

    #[error("JavaScript application is not running: {0}")]
    ApplicationNotRunning(String),

    #[error("JavaScript application does not export WebSocket handlers: {0}")]
    WebSocketUnavailable(String),

    #[error("JavaScript application request channel closed")]
    RequestChannelClosed,

    #[error("JavaScript application admission is full")]
    AdmissionFull,

    #[error("JavaScript application dispatch queue timed out after {0:?}")]
    DispatchQueueTimedOut(Duration),

    #[error("JavaScript application execution timed out after {0:?}")]
    ExecutionTimedOut(Duration),

    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    #[error(transparent)]
    Engine(#[from] deno_core::error::CoreError),

    #[error(transparent)]
    JavaScript(#[from] Box<deno_core::error::JsError>),
}

pub type Result<T> = std::result::Result<T, Error>;
