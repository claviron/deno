use std::cell::RefCell;
#[cfg(not(feature = "server"))]
use std::io::Write;
use std::rc::Rc;

use deno_core::{Extension, OpState, op2};

use crate::ExtensionContext;
use crate::application::{
    PendingReply, PendingRequest, PendingRequestStream, RESPONSE_BODY_CAPACITY,
    ResponseBodySender, ResponseHead,
};

#[op2]
#[string]
fn op_claviron_application_name(state: &OpState) -> String {
    state.borrow::<ExtensionContext>().application_name().to_owned()
}

#[op2(fast)]
fn op_claviron_runtime_available() -> bool {
    true
}

#[op2(fast)]
fn op_claviron_is_cancelled(state: &OpState) -> bool {
    state.borrow::<ExtensionContext>().is_cancelled()
}

#[op2]
async fn op_claviron_cancelled(state: Rc<RefCell<OpState>>) {
    let cancellation =
        state.borrow().borrow::<ExtensionContext>().cancellation();
    cancellation.cancelled_owned().await;
}

#[op2]
#[string]
fn op_claviron_environment(
    state: &OpState,
    #[string] name: String,
) -> Option<String> {
    state.borrow::<ExtensionContext>().environment(&name).map(str::to_owned)
}

#[op2]
#[string]
async fn op_claviron_runtime_yield(#[string] value: String) -> String {
    tokio::task::yield_now().await;
    value
}

#[op2]
#[string]
fn op_claviron_application_request(state: &mut OpState) -> String {
    std::mem::take(&mut state.borrow_mut::<PendingRequest>().head)
}

/// The waiting request's body, as a `Uint8Array`. Taken once, after its head.
#[op2]
#[buffer]
fn op_claviron_application_request_body(state: &mut OpState) -> Vec<u8> {
    state.take::<PendingRequest>().body
}

/// The next piece of a streamed request body; empty at its end. A body read
/// that failed — too large, stalled, its client gone — fails this read, so the
/// handler's stream errors instead of ending as if whole.
#[op2]
#[buffer]
async fn op_claviron_application_request_read(
    state: Rc<RefCell<OpState>>
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let receiver = state
        .borrow()
        .try_borrow::<PendingRequestStream>()
        .map(|body| body.0.clone());
    let Some(receiver) = receiver else { return Ok(Vec::new()) };
    let next = receiver.lock().await.recv().await;
    match next {
        Some(Ok(chunk)) => Ok(chunk.to_vec()),
        Some(Err(error)) => Err(deno_error::JsErrorBox::generic(error)),
        None => Ok(Vec::new()),
    }
}

/// The handler answered: its head goes to the caller now, and the body
/// follows through `op_claviron_application_response_write`.
#[op2(fast)]
fn op_claviron_application_response_head(
    state: &mut OpState,
    #[string] head: String,
) -> Result<(), deno_error::JsErrorBox> {
    let head: ResponseHead = serde_json::from_str(&head).map_err(|error| {
        deno_error::JsErrorBox::type_error(error.to_string())
    })?;
    let Some(PendingReply(reply)) = state.try_take::<PendingReply>() else {
        return Err(deno_error::JsErrorBox::type_error(
            "the response head was already sent",
        ));
    };
    let (sender, receiver) = tokio::sync::mpsc::channel(RESPONSE_BODY_CAPACITY);
    state.put(ResponseBodySender(sender));
    let _ = reply.send(Ok(crate::ApplicationResponse {
        status: head.status,
        headers: head.headers,
        body: receiver,
    }));
    Ok(())
}

/// One piece of the reply's body. Waits while the client is not reading;
/// `false` means the client is gone and the handler should stop.
#[op2]
async fn op_claviron_application_response_write(
    state: Rc<RefCell<OpState>>,
    #[buffer(copy)] chunk: Vec<u8>,
) -> bool {
    let sender = state
        .borrow()
        .try_borrow::<ResponseBodySender>()
        .map(|body| body.0.clone());
    match sender {
        Some(sender) => sender.send(bytes::Bytes::from(chunk)).await.is_ok(),
        None => false,
    }
}

#[op2(fast)]
fn op_claviron_console_write(
    state: &OpState,
    #[string] message: &str,
    is_error: bool,
) -> bool {
    #[cfg(not(any(feature = "server", feature = "tasks")))]
    let _ = state;

    #[cfg(feature = "tasks")]
    if let Some(output) = state.borrow::<ExtensionContext>().task_output() {
        return if is_error {
            output.write_stderr(message.as_bytes()) && output.flush_stderr()
        } else {
            output.write_stdout(message.as_bytes()) && output.flush_stdout()
        };
    }

    #[cfg(feature = "server")]
    {
        let context = state.borrow::<ExtensionContext>();
        let application = context.application_name();
        let instance = context.application_instance().unwrap_or(0);
        let message = message.trim_end_matches(['\r', '\n']);
        if is_error {
            tracing::error!(runtime = "javascript", %application, instance, "{message}");
        } else {
            tracing::info!(runtime = "javascript", %application, instance, "{message}");
        }
        true
    }

    #[cfg(not(feature = "server"))]
    if is_error {
        std::io::stderr().lock().write_all(message.as_bytes()).is_ok()
            && std::io::stderr().lock().flush().is_ok()
    } else {
        std::io::stdout().lock().write_all(message.as_bytes()).is_ok()
            && std::io::stdout().lock().flush().is_ok()
    }
}

deno_core::extension!(
    claviron_runtime_extension,
    ops = [
        op_claviron_application_name,
        op_claviron_runtime_available,
        op_claviron_is_cancelled,
        op_claviron_cancelled,
        op_claviron_environment,
        op_claviron_runtime_yield,
        op_claviron_application_request,
        op_claviron_application_request_body,
        op_claviron_application_request_read,
        op_claviron_application_response_head,
        op_claviron_application_response_write,
        op_claviron_console_write,
    ],
    esm_entry_point = "ext:claviron_runtime_extension/runtime.js",
    esm = [dir "src/extensions/runtime", "runtime.js"],
    options = {
        context: ExtensionContext,
    },
    state = |state, options| {
        state.put(options.context);
    },
);

pub(crate) fn extension(context: ExtensionContext) -> Extension {
    claviron_runtime_extension::init(context)
}
