use std::cell::RefCell;
#[cfg(not(feature = "server"))]
use std::io::Write;
use std::rc::Rc;

use deno_core::{Extension, OpState, op2, v8};

use crate::ExtensionContext;
use crate::application::{
    RESPONSE_BODY_CAPACITY, RequestResource, RequestResources,
};
use crate::engine::request_data::RequestData;

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

/// Notify the owner; it releases request resources after returning from V8.
#[op2(fast)]
fn op_claviron_application_request_complete(
    state: &OpState,
    #[number] id: u64,
) {
    if let Some(request) = state.borrow::<RequestResources>().get(id) {
        request.complete_execution(Ok(()));
    }
}

#[op2(fast, reentrant)]
fn op_claviron_application_request_error<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    state: Rc<RefCell<OpState>>,
    #[number] id: u64,
    error: v8::Local<'s, v8::Value>,
) {
    let request = state.borrow().borrow::<RequestResources>().get(id);
    if let Some(request) = request {
        // Exception conversion can execute guest getters. Retain no OpState
        // or request-field borrow while preserving Deno's rejection formatting.
        let error =
            deno_core::error::exception_to_err(scope, error, true, true);
        request.complete_execution(Err(error.to_string()));
    }
}

fn byte_string<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    bytes: &[u8],
) -> Result<v8::Local<'s, v8::String>, deno_error::JsErrorBox> {
    v8::String::new_from_one_byte(scope, bytes, v8::NewStringType::Normal)
        .ok_or_else(|| {
            deno_error::JsErrorBox::range_error("HTTP field exceeds V8 limits")
        })
}

#[op2]
fn op_claviron_application_request_method<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    #[cppgc] data: &RequestData,
) -> Result<v8::Local<'s, v8::String>, deno_error::JsErrorBox> {
    byte_string(scope, data.method.as_str().as_bytes())
}

#[op2]
#[string]
fn op_claviron_application_request_url(#[cppgc] data: &RequestData) -> String {
    data.uri.to_string()
}

#[op2]
fn op_claviron_application_request_headers<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    #[cppgc] data: &RequestData,
) -> Result<v8::Local<'s, v8::Array>, deno_error::JsErrorBox> {
    let mut headers = Vec::with_capacity(data.headers.len());
    for (name, value) in &data.headers {
        let name = byte_string(scope, name.as_str().as_bytes())?;
        let value = byte_string(scope, value.as_bytes().trim_ascii())?;
        headers.push(
            v8::Array::new_with_elements(scope, &[name.into(), value.into()])
                .into(),
        );
    }
    Ok(v8::Array::new_with_elements(scope, &headers))
}

#[op2]
fn op_claviron_application_request_header<'s>(
    scope: &mut v8::PinScope<'s, '_>,
    #[cppgc] data: &RequestData,
    #[string] name: &str,
) -> Result<Option<v8::Local<'s, v8::String>>, deno_error::JsErrorBox> {
    let mut values = data.headers.get_all(name).iter();
    let Some(first) = values.next() else { return Ok(None) };
    let Some(second) = values.next() else {
        return byte_string(scope, first.as_bytes().trim_ascii()).map(Some);
    };
    let mut joined = first.as_bytes().trim_ascii().to_vec();
    for value in std::iter::once(second).chain(values) {
        joined.extend_from_slice(b", ");
        joined.extend_from_slice(value.as_bytes().trim_ascii());
    }
    byte_string(scope, &joined).map(Some)
}

#[op2]
#[buffer]
fn op_claviron_application_request_body(
    scope: &mut v8::PinScope,
    #[cppgc] data: &RequestData,
) -> Result<Vec<u8>, deno_error::JsErrorBox> {
    data.take_body(scope)
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
#[op2(fast, reentrant)]
fn op_claviron_application_response_whole(
    scope: &mut v8::PinScope,
    state: Rc<RefCell<OpState>>,
    #[number] id: u64,
    status: f64,
    headers: v8::Local<v8::Value>,
    body: v8::Local<v8::Value>,
) -> Result<bool, deno_error::JsErrorBox> {
    let status = response_status(status)?;
    let headers = response_headers(scope, headers)?;
    // Own guest bytes before handing the reply to another thread. Static text
    // goes straight to UTF-8 here, without an intermediate JS Uint8Array.
    let body = if body.is_null_or_undefined() {
        Vec::new()
    } else if let Ok(text) = v8::Local::<v8::String>::try_from(body) {
        text.to_rust_string_lossy(scope).into_bytes()
    } else if let Ok(view) = v8::Local::<v8::Uint8Array>::try_from(body) {
        let mut bytes = vec![0; view.byte_length()];
        view.copy_contents(&mut bytes);
        bytes
    } else {
        return Err(deno_error::JsErrorBox::type_error(
            "response body must be text or bytes",
        ));
    };
    let resource = request_resource(&state.borrow(), id)?;
    let reply = resource.reply.borrow_mut().take().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("response head was already sent")
    })?;
    let mut response = crate::ApplicationResponse::new(
        crate::ResponseBody::Whole(bytes::Bytes::from(body)),
    );
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(reply.send(Ok(response)).is_ok())
}

/// The first live response commits its head and creates only its own channel.
#[op2(fast, reentrant)]
fn op_claviron_application_response_head(
    scope: &mut v8::PinScope,
    state: Rc<RefCell<OpState>>,
    #[number] id: u64,
    status: f64,
    headers: v8::Local<v8::Value>,
) -> Result<bool, deno_error::JsErrorBox> {
    let status = response_status(status)?;
    let headers = response_headers(scope, headers)?;
    let resource = request_resource(&state.borrow(), id)?;
    let reply = resource.reply.borrow_mut().take().ok_or_else(|| {
        deno_error::JsErrorBox::type_error("response head was already sent")
    })?;
    let (sender, receiver) = tokio::sync::mpsc::channel(RESPONSE_BODY_CAPACITY);
    let (completion, finished) = tokio::sync::oneshot::channel();
    *resource.output.borrow_mut() = Some(sender);
    *resource.completion.borrow_mut() = Some(completion);
    let mut response = crate::ApplicationResponse::new(
        crate::ResponseBody::Stream(crate::ResponseStream::new(
            receiver,
            finished,
            resource.lifetime.cancellation.clone(),
        )),
    );
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    Ok(reply.send(Ok(response)).is_ok())
}

fn response_status(
    status: f64
) -> Result<http::StatusCode, deno_error::JsErrorBox> {
    if !(100.0..1000.0).contains(&status) || status.fract() != 0.0 {
        return Err(deno_error::JsErrorBox::type_error(
            "invalid HTTP response status",
        ));
    }
    http::StatusCode::from_u16(status as u16)
        .map_err(|error| deno_error::JsErrorBox::type_error(error.to_string()))
}

/// Decode V8 ByteStrings directly into owned HTTP headers.
/// Array access can invoke guest getters; hold no OpState/resource borrow here.
fn response_headers(
    scope: &mut v8::PinScope,
    headers: v8::Local<v8::Value>,
) -> Result<http::HeaderMap, deno_error::JsErrorBox> {
    fn bytes(
        scope: &mut v8::PinScope,
        value: v8::Local<v8::Value>,
    ) -> Result<Vec<u8>, deno_error::JsErrorBox> {
        let string =
            v8::Local::<v8::String>::try_from(value).map_err(|_| {
                deno_error::JsErrorBox::type_error(
                    "response header must be a string",
                )
            })?;
        if !string.contains_only_onebyte() {
            return Err(deno_error::JsErrorBox::type_error(
                "response header must be a ByteString",
            ));
        }
        let mut bytes = vec![0; string.length()];
        string.write_one_byte_v2(scope, 0, &mut bytes, v8::WriteFlags::empty());
        Ok(bytes)
    }
    // Deno's ready responses carry either no headers or one content type.
    // Keep that representation instead of manufacturing a header pair array.
    if headers.is_null_or_undefined() {
        return Ok(http::HeaderMap::new());
    }
    if headers.is_string() {
        let value = http::HeaderValue::from_maybe_shared(bytes::Bytes::from(
            bytes(scope, headers)?,
        ))
        .map_err(|error| {
            deno_error::JsErrorBox::type_error(error.to_string())
        })?;
        let mut result = http::HeaderMap::with_capacity(1);
        result.insert(http::header::CONTENT_TYPE, value);
        return Ok(result);
    }
    let headers = v8::Local::<v8::Array>::try_from(headers).map_err(|_| {
        deno_error::JsErrorBox::type_error("response headers must be an array")
    })?;
    let mut result = http::HeaderMap::try_with_capacity(
        headers.length() as usize
    )
    .map_err(|error| deno_error::JsErrorBox::type_error(error.to_string()))?;
    for index in 0..headers.length() {
        let pair = headers
            .get_index(scope, index)
            .and_then(|value| v8::Local::<v8::Array>::try_from(value).ok())
            .filter(|pair| pair.length() == 2)
            .ok_or_else(|| {
                deno_error::JsErrorBox::type_error(
                    "response header must be a pair",
                )
            })?;
        let name = pair.get_index(scope, 0).ok_or_else(|| {
            deno_error::JsErrorBox::type_error(
                "response header name is unavailable",
            )
        })?;
        let name = http::HeaderName::from_bytes(&bytes(scope, name)?).map_err(
            |error| deno_error::JsErrorBox::type_error(error.to_string()),
        )?;
        let value = pair.get_index(scope, 1).ok_or_else(|| {
            deno_error::JsErrorBox::type_error(
                "response header value is unavailable",
            )
        })?;
        let value = http::HeaderValue::from_maybe_shared(bytes::Bytes::from(
            bytes(scope, value)?,
        ))
        .map_err(|error| {
            deno_error::JsErrorBox::type_error(error.to_string())
        })?;
        result.try_append(name, value).map_err(|error| {
            deno_error::JsErrorBox::type_error(error.to_string())
        })?;
    }
    Ok(result)
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
        op_claviron_application_request_complete,
        op_claviron_application_request_error,
        op_claviron_application_request_method,
        op_claviron_application_request_url,
        op_claviron_application_request_headers,
        op_claviron_application_request_header,
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
    crate::engine::embedded::extension(claviron_runtime_extension::init(
        context,
    ))
}
