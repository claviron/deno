use std::collections::HashMap;

use deno_core::{Extension, OpState, op2};
use deno_error::JsErrorBox;

use crate::application::{
    PendingWebSocketEvent, WebSocketAction, WebSocketActionSender,
};

#[derive(Default)]
pub(crate) struct WebSocketSinks {
    sinks: HashMap<u32, WebSocketActionSender>,
}

impl WebSocketSinks {
    pub(crate) fn insert(
        &mut self,
        id: u32,
        sender: WebSocketActionSender,
    ) {
        self.sinks.insert(id, sender);
    }

    pub(crate) fn remove(
        &mut self,
        id: u32,
    ) {
        self.sinks.remove(&id);
    }

    fn send(
        &self,
        id: u32,
        action: WebSocketAction,
    ) -> Result<(), JsErrorBox> {
        self.sinks
            .get(&id)
            .ok_or_else(|| {
                JsErrorBox::generic(format!("WebSocket session {id} is closed"))
            })?
            .try_send(action)
            .map_err(|error| {
                JsErrorBox::generic(format!(
                    "WebSocket session {id} cannot send: {error}"
                ))
            })
    }
}

#[op2]
#[string]
fn op_claviron_websocket_event(state: &mut OpState) -> String {
    state.take::<PendingWebSocketEvent>().0
}

#[op2(fast)]
fn op_claviron_websocket_send_text(
    state: &OpState,
    id: u32,
    #[string] data: String,
) -> Result<(), JsErrorBox> {
    state.borrow::<WebSocketSinks>().send(id, WebSocketAction::Text(data))
}

#[op2(fast)]
fn op_claviron_websocket_send_binary(
    state: &OpState,
    id: u32,
    #[buffer] data: &[u8],
) -> Result<(), JsErrorBox> {
    state
        .borrow::<WebSocketSinks>()
        .send(id, WebSocketAction::Binary(data.to_vec()))
}

#[op2(fast)]
fn op_claviron_websocket_close(
    state: &OpState,
    id: u32,
    code: u16,
    #[string] reason: String,
) -> Result<(), JsErrorBox> {
    let protocol_code =
        (1000..=1014).contains(&code) && !matches!(code, 1004..=1006);
    if !protocol_code && !(3000..=4999).contains(&code) {
        return Err(JsErrorBox::type_error(
            "WebSocket close code is not valid for a server close frame",
        ));
    }
    if reason.len() > 123 {
        return Err(JsErrorBox::type_error(
            "WebSocket close reason exceeds 123 bytes",
        ));
    }
    state
        .borrow::<WebSocketSinks>()
        .send(id, WebSocketAction::Close { code, reason })
}

deno_core::extension!(
    claviron_websocket_extension,
    ops = [
        op_claviron_websocket_event,
        op_claviron_websocket_send_text,
        op_claviron_websocket_send_binary,
        op_claviron_websocket_close,
    ],
    esm_entry_point = "ext:claviron_websocket_extension/websocket.js",
    esm = [dir "src/extensions/websocket", "websocket.js"],
    state = |state| {
        state.put(WebSocketSinks::default());
    },
);

pub(crate) fn extension() -> Extension {
    claviron_websocket_extension::init()
}
