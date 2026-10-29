#[cfg(feature = "websocket")]
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

/// Standard HTTP request moved to the persistent JavaScript application.
pub type ApplicationRequest = http::Request<RequestBody>;

/// A request's body as it reaches the isolate: whole, or piece by piece as it
/// arrives, which the handler reads as the `Request`'s `ReadableStream`. The
/// request policy chooses (`requests.buffering`, `buffer_size`).
#[derive(Debug)]
pub enum RequestBody {
    /// No body crosses the application boundary.
    Empty,
    Whole(Vec<u8>),
    /// Pieces as they arrive; an error ends the body as a failed read.
    Stream(mpsc::Receiver<Result<bytes::Bytes, String>>),
}

/// Standard HTTP response with either complete bytes or a live body.
pub type ApplicationResponse = http::Response<super::ResponseBody>;

/// Where the head goes when the handler answers, and an error when it never
/// does.
pub type ReplySender =
    oneshot::Sender<std::result::Result<ApplicationResponse, String>>;

/// Pieces of a reply in flight between the isolate and the client. Small on
/// purpose, as SAPI's: a client that stops reading is felt by the handler at
/// once, as a `writeBody` that waits.
pub(crate) const RESPONSE_BODY_CAPACITY: usize = 2;

#[cfg(feature = "websocket")]
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum WebSocketEvent {
    Open { id: u32, url: String, headers: Vec<(String, String)> },
    Message { id: u32, message: WebSocketMessage },
    Close { id: u32, code: u16, reason: String, was_clean: bool },
}

#[cfg(feature = "websocket")]
#[derive(Debug, Serialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub(crate) enum WebSocketMessage {
    Text(String),
    Binary(Vec<u8>),
}

#[cfg(feature = "websocket")]
#[derive(Debug)]
pub(crate) enum WebSocketAction {
    Text(String),
    Binary(Vec<u8>),
    Close { code: u16, reason: String },
}

#[cfg(feature = "websocket")]
pub(crate) struct PendingWebSocketEvent(pub String);

#[cfg(feature = "websocket")]
pub(crate) type WebSocketActionSender = mpsc::Sender<WebSocketAction>;
