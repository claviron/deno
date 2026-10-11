use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use claviron::runtime::AppState;
use claviron::runtime::registry::{
    Error as RegistryError, Result as RegistryResult,
};
use claviron::websocket::{
    H1UpgradeAccept, H1UpgradeContext, H1UpgradeHandler, H1UpgradeReject,
    H1UpgradeResult, H1UpgradeState, is_h1_header_upgrade,
    websocket_response_headers,
};
use fastwebsockets::{
    FragmentCollector, Frame, OpCode, Payload, Role, WebSocket,
};
use http::Request;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tracing::{debug, warn};

use super::JsApplicationService;
use super::pool::WebSocketLease;
use crate::application::{WebSocketAction, WebSocketMessage};

/// Connects one exact H1 WebSocket route to a persistent JS application pool.
pub struct JsWebSocketUpgradeHandler {
    service: Arc<JsApplicationService>,
    application: Arc<str>,
}

impl JsWebSocketUpgradeHandler {
    pub fn new(
        service: Arc<JsApplicationService>,
        application: impl Into<Arc<str>>,
    ) -> Self {
        Self { service, application: application.into() }
    }
}

#[async_trait]
impl H1UpgradeHandler for JsWebSocketUpgradeHandler {
    fn name(&self) -> &'static str {
        "js_websocket"
    }

    fn is_upgrade(
        &self,
        req: &Request<Body>,
    ) -> bool {
        is_h1_header_upgrade(req, "websocket")
    }

    async fn accept(
        &self,
        _state: &AppState,
        ctx: &H1UpgradeContext,
    ) -> H1UpgradeResult {
        let headers = websocket_response_headers(ctx, None)
            .map_err(|error| H1UpgradeReject::bad_request(error.to_string()))?;
        let lease = self.service.prepare_websocket(&self.application).map_err(
            |error| match error {
                crate::Error::ApplicationNotRunning(_)
                | crate::Error::RequestChannelClosed
                | crate::Error::WebSocketUnavailable(_) => {
                    H1UpgradeReject::service_unavailable(error.to_string())
                }
                _ => H1UpgradeReject::internal(error.to_string()),
            },
        )?;
        Ok(H1UpgradeAccept::with_state(headers, lease))
    }

    async fn handle_upgraded(
        &self,
        state: AppState,
        ctx: H1UpgradeContext,
        accepted: Option<H1UpgradeState>,
        upgraded: Upgraded,
    ) -> RegistryResult<()> {
        let lease = accepted
            .ok_or_else(|| {
                RegistryError::msg("JavaScript WebSocket lease missing")
            })?
            .take::<WebSocketLease>()?;
        let host = ctx
            .headers
            .get(http::header::HOST)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned)
            .unwrap_or_else(|| ctx.host.to_string());
        let url = format!("wss://{host}{}", ctx.uri);
        serve_websocket(upgraded, state, url, header_pairs(&ctx.headers), lease)
            .await
            .map_err(|error| RegistryError::recoverable(error.to_string()))
    }
}

/// The request headers as the application's `open` callback sees them.
pub fn header_pairs(headers: &http::HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            (
                name.as_str().to_owned(),
                String::from_utf8_lossy(value.as_bytes()).into_owned(),
            )
        })
        .collect()
}

/// Runs one WebSocket session over an already-upgraded transport: the H1
/// `101` path and the H2 extended-CONNECT path both end here, on the same
/// application instance the lease was prepared on. Frames are the
/// application's business from the first byte; nothing here knows which
/// HTTP version carried the handshake.
pub async fn serve_websocket(
    upgraded: Upgraded,
    state: AppState,
    url: String,
    headers: Vec<(String, String)>,
    lease: WebSocketLease,
) -> crate::Result<()> {
    let label = url.clone();
    let mut session = lease.open(url, headers).await?;
    let max_message_size = session.max_message_size();

    let mut raw =
        WebSocket::after_handshake(TokioIo::new(upgraded), Role::Server);
    raw.set_auto_close(true);
    raw.set_auto_pong(true);
    raw.set_max_message_size(max_message_size);
    let mut socket = FragmentCollector::new(raw);

    let (close, notify_application) = loop {
        tokio::select! {
            action = session.recv() => {
                match action {
                    Some(WebSocketAction::Text(text)) => {
                        socket.write_frame(Frame::text(Payload::Owned(text.into_bytes()))).await
                            .map_err(socket_error)?;
                    }
                    Some(WebSocketAction::Binary(bytes)) => {
                        socket.write_frame(Frame::binary(Payload::Owned(bytes))).await
                            .map_err(socket_error)?;
                    }
                    Some(WebSocketAction::Close { code, reason }) => {
                        socket.write_frame(Frame::close(code, reason.as_bytes())).await
                            .map_err(socket_error)?;
                        break ((code, reason, true), true);
                    }
                    None => {
                        let (code, reason) = if state.is_shutting_down() {
                            (1001, "Claviron shutting down")
                        } else {
                            (1011, "application stopped")
                        };
                        let _ = socket.write_frame(Frame::close(code, reason.as_bytes())).await;
                        break ((code, reason.to_owned(), state.is_shutting_down()), false);
                    }
                }
            }
            frame = socket.read_frame() => {
                let frame = frame.map_err(socket_error)?;
                if frame.payload.len() > max_message_size {
                    let _ = socket.write_frame(Frame::close(1009, b"message too large")).await;
                    break ((1009, "message too large".to_owned(), false), true);
                }
                match frame.opcode {
                    OpCode::Text => {
                        let text = String::from_utf8(Vec::from(frame.payload)).map_err(|error| {
                            crate::Error::ApplicationUnavailable(format!("invalid WebSocket text: {error}"))
                        })?;
                        if let Err(error) = session.message(WebSocketMessage::Text(text)).await {
                            warn!(socket = %label, %error, "JavaScript WebSocket message failed");
                            let _ = socket.write_frame(Frame::close(1011, b"application error")).await;
                            break ((1011, "application error".to_owned(), false), true);
                        }
                    }
                    OpCode::Binary => {
                        if let Err(error) = session
                            .message(WebSocketMessage::Binary(Vec::from(frame.payload)))
                            .await
                        {
                            warn!(socket = %label, %error, "JavaScript WebSocket message failed");
                            let _ = socket.write_frame(Frame::close(1011, b"application error")).await;
                            break ((1011, "application error".to_owned(), false), true);
                        }
                    }
                    OpCode::Close => {
                        break (close_details(&frame.payload), true);
                    }
                    OpCode::Ping | OpCode::Pong => {}
                    _ => {}
                }
            }
        }
    };

    if notify_application
        && let Err(error) = session.close(close.0, close.1, close.2).await
    {
        debug!(%error, "JavaScript WebSocket close callback unavailable");
    }
    Ok(())
}

fn close_details(payload: &[u8]) -> (u16, String, bool) {
    if payload.len() < 2 {
        return (1005, String::new(), true);
    }
    let code = u16::from_be_bytes([payload[0], payload[1]]);
    let reason = String::from_utf8_lossy(&payload[2..]).into_owned();
    (code, reason, true)
}

fn socket_error(error: fastwebsockets::WebSocketError) -> crate::Error {
    crate::Error::ApplicationUnavailable(format!(
        "WebSocket transport failed: {error}"
    ))
}

#[cfg(test)]
#[path = "../../tests/server/websocket.rs"]
mod tests;
