use std::cell::RefCell;
#[cfg(not(feature = "server"))]
use std::io::Write;
use std::rc::Rc;

use deno_core::{Extension, OpState, op2};

use crate::ExtensionContext;
use crate::application::{
    RESPONSE_BODY_CAPACITY, RequestResource, RequestResources, ResponseHead,
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

fn request_resource(
    state: &OpState,
    id: u64,
) -> Result<Rc<RequestResource>, deno_error::JsErrorBox> {
    let request =
        state.borrow::<RequestResources>().get(id).ok_or_else(|| {
            deno_error::JsErrorBox::type_error("request is closed")
        })?;
    if request.closed.get() {
        return Err(deno_error::JsErrorBox::type_error("request is closed"));
    }
    Ok(request)
}

#[op2]
#[string]
fn op_claviron_application_request(
    state: &OpState,
    #[number] id: u64,
) -> Result<String, deno_error::JsErrorBox> {
    let resource = request_resource(state, id)?;
    let mut pending = resource.request.borrow_mut();
    let request = pending.as_mut().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("request was already consumed")
    })?;
    Ok(std::mem::take(&mut request.head))
}

#[op2]
#[buffer]
fn op_claviron_application_request_body(
    state: &OpState,
    #[number] id: u64,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let resource = request_resource(state, id)?;
    let request = resource.request.borrow_mut().take().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("request body was already consumed")
    })?;
    Ok(request.body)
}

/// Only a live request body enters this path; its receiver has one owner.
#[op2]
#[buffer]
async fn op_claviron_application_request_read(
    state: Rc<RefCell<OpState>>,
    #[number] id: u64,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    let resource = request_resource(&state.borrow(), id)?;
    let mut receiver = resource.input.borrow_mut().take().ok_or_else(|| {
        deno_error::JsErrorBox::type_error(
            "request body is closed or already being read",
        )
    })?;
    let next = tokio::select! {
        biased;
        _ = resource.lifetime.cancellation.cancelled() => return Err(deno_error::JsErrorBox::generic("request cancelled")),
        next = receiver.recv() => next,
    };
    if !resource.closed.get() {
        *resource.input.borrow_mut() = Some(receiver);
    }
    match next {
        Some(Ok(chunk)) => Ok(chunk.to_vec()),
        Some(Err(error)) => Err(deno_error::JsErrorBox::generic(error)),
        None => Ok(Vec::new()),
    }
}

/// Static replies cross the bridge once, without a stream channel or pump.
#[op2(fast)]
fn op_claviron_application_response_whole(
    state: &OpState,
    #[number] id: u64,
    #[string] head: String,
    #[buffer(copy)] body: Vec<u8>,
) -> Result<bool, deno_error::JsErrorBox> {
    let resource = request_resource(state, id)?;
    let head: ResponseHead = serde_json::from_str(&head).map_err(|error| {
        deno_error::JsErrorBox::type_error(error.to_string())
    })?;
    let reply = resource.reply.borrow_mut().take().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("response head was already sent")
    })?;
    Ok(reply
        .send(Ok(crate::ApplicationResponse {
            status: head.status,
            headers: head.headers,
            body: crate::ResponseBody::Whole(bytes::Bytes::from(body)),
        }))
        .is_ok())
}

/// The first live response commits its head and creates only its own channel.
#[op2(fast)]
fn op_claviron_application_response_head(
    state: &OpState,
    #[number] id: u64,
    #[string] head: String,
) -> Result<bool, deno_error::JsErrorBox> {
    let resource = request_resource(state, id)?;
    let head: ResponseHead = serde_json::from_str(&head).map_err(|error| {
        deno_error::JsErrorBox::type_error(error.to_string())
    })?;
    let reply = resource.reply.borrow_mut().take().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("response head was already sent")
    })?;
    let (sender, receiver) = tokio::sync::mpsc::channel(RESPONSE_BODY_CAPACITY);
    let (completion, finished) = tokio::sync::oneshot::channel();
    *resource.output.borrow_mut() = Some(sender);
    *resource.completion.borrow_mut() = Some(completion);
    Ok(reply
        .send(Ok(crate::ApplicationResponse {
            status: head.status,
            headers: head.headers,
            body: crate::ResponseBody::Stream(crate::ResponseStream::new(
                receiver,
                finished,
                resource.lifetime.cancellation.clone(),
            )),
        }))
        .is_ok())
}

#[op2]
async fn op_claviron_application_response_write(
    state: Rc<RefCell<OpState>>,
    #[number] id: u64,
    #[buffer(copy)] chunk: Vec<u8>,
) -> Result<bool, deno_error::JsErrorBox> {
    let resource = request_resource(&state.borrow(), id)?;
    let sender = resource.output.borrow().clone().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("response is not a live stream")
    })?;
    Ok(tokio::select! {
        biased;
        _ = resource.lifetime.cancellation.cancelled() => false,
        result = sender.send(bytes::Bytes::from(chunk)) => result.is_ok(),
    })
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
        op_claviron_application_response_whole,
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
        state.put(RequestResources::default());
    },
);

pub(crate) fn extension(context: ExtensionContext) -> Extension {
    claviron_runtime_extension::init(context)
}
