use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

/// Framework-neutral request delivered to one persistent JavaScript application.
#[derive(Debug)]
pub struct ApplicationRequest {
    pub method: String,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: RequestBody,
}

/// A request's body as it reaches the isolate: whole, or piece by piece as it
/// arrives, which the handler reads as the `Request`'s `ReadableStream`. The
/// request policy chooses (`requests.buffering`, `buffer_size`).
#[derive(Debug)]
pub enum RequestBody {
    Whole(Vec<u8>),
    /// Pieces as they arrive; an error ends the body as a failed read.
    Stream(mpsc::Receiver<Result<bytes::Bytes, String>>),
}

/// Framework-neutral response returned by a JavaScript Fetch handler: its
/// head, and its body as it is produced. The head is handed over as soon as
/// the handler returns its `Response`; the body follows piece by piece while
/// the isolate keeps running, so a stream (SSE, a download) reaches the client
/// as it is written. Whether it is held instead is the reply policy's.
#[derive(Debug)]
pub struct ApplicationResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: mpsc::Receiver<bytes::Bytes>,
}

/// Where the head goes when the handler answers, and an error when it never
/// does.
pub type ReplySender =
    oneshot::Sender<std::result::Result<ApplicationResponse, String>>;

/// The reply sender, waiting in the isolate's state for the handler's head.
pub(crate) struct PendingReply(pub ReplySender);

/// Where the body's pieces go once the head is out. Dropped when the handler
/// finishes, which ends the body.
pub(crate) struct ResponseBodySender(pub mpsc::Sender<bytes::Bytes>);

/// Pieces of a reply in flight between the isolate and the client. Small on
/// purpose, as SAPI's: a client that stops reading is felt by the handler at
/// once, as a `writeBody` that waits.
pub(crate) const RESPONSE_BODY_CAPACITY: usize = 2;

/// The request waiting for its isolate: the head as JSON, the body as
/// bytes. Two parts because a body crosses into V8 as a `Uint8Array`; as JSON
/// every byte became a number and a comma, and a 24 KB page roughly 90 KB of
/// text to write and parse.
pub(crate) struct PendingRequest {
    pub head: String,
    pub body: Vec<u8>,
}

/// A streamed request body, read piece by piece by the isolate. Shared so an
/// op can wait on it without holding the isolate's state borrowed.
pub(crate) struct PendingRequestStream(
    pub  std::rc::Rc<
        tokio::sync::Mutex<mpsc::Receiver<Result<bytes::Bytes, String>>>,
    >,
);

/// What crosses as JSON of a request: everything but the body, and whether
/// the body comes whole or as a stream.
#[derive(Serialize)]
pub(crate) struct RequestHead<'a> {
    pub method: &'a str,
    pub url: &'a str,
    pub headers: &'a [(String, String)],
    pub streamed: bool,
}

/// What crosses as JSON of a reply: everything but the body.
#[derive(Deserialize)]
pub(crate) struct ResponseHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

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
