//! `claviron:orbit/invoke` — send invocations, and serve them.
//!
//! The consumer side follows the events inbox rather than PHP's blocking
//! receive: every subscription gets a pump task waiting on its own ring
//! cursor and forwarding into one channel per isolate, and the async receive
//! op takes from that channel. A synchronous subscribe therefore never
//! contends with a receive that is already waiting, and dropping the isolate
//! drops the pumps.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use claviron::app::EventValue;
use claviron::orbit::{Invocation, InvocationBus};
use deno_core::{OpState, op2};
use deno_error::JsErrorBox;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::{
    OrbitContext, OrbitError, event_value_to_json, js_error,
    json_to_event_value,
};

#[derive(Debug, Deserialize)]
pub(super) struct SendRequest {
    operation: String,
    payload: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct OperationRequest {
    operation: String,
}

/// Between a pump and the isolate. Small on purpose: the ring retains what
/// the isolate has not read, and a pump that cannot send simply waits with
/// its cursor where it is.
const INBOX_CAPACITY: usize = 16;

type Received = Result<Invocation, String>;

pub(super) struct InvocationInbox {
    sender: mpsc::Sender<Received>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<Received>>,
    pumps: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl InvocationInbox {
    pub(super) fn new() -> Self {
        let (sender, receiver) = mpsc::channel(INBOX_CAPACITY);
        Self {
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            pumps: Mutex::new(HashMap::new()),
        }
    }

    fn subscribe(
        &self,
        bus: &Arc<InvocationBus>,
        operation: String,
        cancellation: CancellationToken,
    ) -> Result<bool, JsErrorBox> {
        let mut pumps = self.pumps.lock().unwrap();
        if pumps.contains_key(&operation) {
            return Ok(false);
        }
        // Registers the ring's readiness fd with the runtime this op runs on.
        let mut subscription =
            bus.subscribe(operation.clone()).map_err(|error| {
                js_error(OrbitError::Invocation(error.to_string()))
            })?;
        let sender = self.sender.clone();
        let name = operation.clone();
        let pump = tokio::spawn(async move {
            loop {
                // Stopping takes no new invocation. Ending the pump drops
                // the subscription and closes the channel, which is how the
                // program's own `serve` loop learns to wind down; what it
                // has already accepted is untouched.
                let received = tokio::select! {
                    () = cancellation.cancelled() => return,
                    received = subscription.receive() => received
                };
                let next = match received {
                    Ok(invocation) => Ok(invocation),
                    // The invocations it missed are gone either way; say so
                    // and carry on from where the cursor is now.
                    Err(claviron::orbit::invoke::Error::Lagged(skipped)) => {
                        tracing::warn!(
                            operation = %name,
                            skipped,
                            "JavaScript isolate lagged behind the invocation ring"
                        );
                        continue;
                    }
                    Err(error) => Err(error.to_string()),
                };
                let last = next.is_err();
                if sender.send(next).await.is_err() || last {
                    return;
                }
            }
        });
        pumps.insert(operation, pump);
        Ok(true)
    }

    fn unsubscribe(
        &self,
        operation: &str,
    ) -> bool {
        match self.pumps.lock().unwrap().remove(operation) {
            Some(pump) => {
                pump.abort();
                true
            }
            None => false,
        }
    }

    fn is_empty(&self) -> bool {
        self.pumps.lock().unwrap().is_empty()
    }
}

impl Drop for InvocationInbox {
    fn drop(&mut self) {
        for (_, pump) in self.pumps.lock().unwrap().drain() {
            pump.abort();
        }
    }
}

fn invocation_json(invocation: &Invocation) -> Result<String, String> {
    // The ring carries event values; bytes in another codec come through as
    // text, the way PHP receives them as a binary string.
    let payload = match EventValue::from_bytes(&invocation.payload) {
        Ok(value) => event_value_to_json(&value)?,
        Err(_) => serde_json::Value::String(
            String::from_utf8_lossy(&invocation.payload).into_owned(),
        ),
    };
    serde_json::to_string(&serde_json::json!({
        "id": invocation.id.to_string(),
        "operation": invocation.operation,
        "payload": payload,
        "submittedAt": invocation.submitted_at.as_unix_ms()
    }))
    .map_err(|error| error.to_string())
}

#[op2]
#[string]
pub(super) fn op_claviron_orbit_invoke_send(
    state: &OpState,
    #[serde] request: SendRequest,
) -> Result<String, JsErrorBox> {
    let bus = state.borrow::<OrbitContext>().invocations().map_err(js_error)?;
    let payload: serde_json::Value = serde_json::from_str(&request.payload)
        .map_err(|error| js_error(OrbitError::Invocation(error.to_string())))?;
    let payload = json_to_event_value(payload)
        .and_then(|value| value.to_bytes().map_err(|error| error.to_string()))
        .map_err(|error| js_error(OrbitError::Invocation(error)))?;
    bus.submit(&request.operation, &payload)
        .map(|id| id.to_string())
        .map_err(|error| js_error(OrbitError::Invocation(error.to_string())))
}

#[op2]
pub(super) fn op_claviron_orbit_invoke_subscribe(
    state: &OpState,
    #[serde] request: OperationRequest,
) -> Result<bool, JsErrorBox> {
    let cancellation = state.borrow::<crate::ExtensionContext>().cancellation();
    let context = state.borrow::<OrbitContext>();
    let bus = context.invocations().map_err(js_error)?;
    context.invocation_inbox.subscribe(&bus, request.operation, cancellation)
}

/// What a receive says when this isolate is stopping: an error, so the
/// program's loop ends rather than reading it as "nothing yet" and asking
/// again.
fn stopping() -> Result<Option<String>, JsErrorBox> {
    Err(js_error(OrbitError::Invocation(
        "invocation inbox closed: this JavaScript runtime is stopping"
            .to_owned(),
    )))
}

#[op2]
pub(super) fn op_claviron_orbit_invoke_unsubscribe(
    state: &OpState,
    #[serde] request: OperationRequest,
) -> bool {
    state
        .borrow::<OrbitContext>()
        .invocation_inbox
        .unsubscribe(&request.operation)
}

/// The next invocation under any subscribed operation. Zero waits; a timeout
/// that runs out is `null`, and so is nothing else: a pump that fails ends
/// the receive with its error, because there will be no more from it.
///
/// Stopping ends it too, and has to. The inbox keeps a sender of its own, so
/// the pumps ending does not close the channel; a receive with nothing to
/// hand over would wait for an invocation this isolate will not be here to
/// take. Ending it is how the program's own `serve` loop learns to return.
#[op2]
#[string]
pub(super) async fn op_claviron_orbit_invoke_receive(
    state: Rc<RefCell<OpState>>,
    #[smi] timeout_ms: u32,
) -> Result<Option<String>, JsErrorBox> {
    let (inbox, cancellation) = {
        let state = state.borrow();
        (
            Arc::clone(&state.borrow::<OrbitContext>().invocation_inbox),
            state.borrow::<crate::ExtensionContext>().cancellation(),
        )
    };
    if inbox.is_empty() {
        return Err(js_error(OrbitError::Invocation(
            "subscribe to at least one operation before receiving".to_owned(),
        )));
    }
    let mut receiver = inbox.receiver.lock().await;
    let received = if timeout_ms == 0 {
        tokio::select! {
            () = cancellation.cancelled() => return stopping(),
            received = receiver.recv() => received
        }
    } else {
        tokio::select! {
            () = cancellation.cancelled() => return stopping(),
            received = tokio::time::timeout(
                Duration::from_millis(u64::from(timeout_ms)),
                receiver.recv()
            ) => match received {
                Ok(received) => received,
                Err(_) => return Ok(None)
            }
        }
    };
    match received {
        Some(Ok(invocation)) => invocation_json(&invocation)
            .map(Some)
            .map_err(|error| js_error(OrbitError::Invocation(error))),
        Some(Err(error)) => Err(js_error(OrbitError::Invocation(error))),
        None => Err(js_error(OrbitError::Invocation(
            "invocation inbox closed".to_owned(),
        ))),
    }
}
