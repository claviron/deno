use std::cell::{Cell, RefCell};
use std::future::poll_fn;
use std::task::{Context, Poll};

use deno_core::futures::future::{Either, LocalBoxFuture, select};
use deno_core::futures::stream::FuturesUnordered;
use deno_core::futures::{FutureExt, StreamExt};
use deno_core::v8;

use super::CoreEngine;
use crate::application::{RequestLifetime, RequestResource, RequestResources};
use crate::{ApplicationRequest, Error, RequestBody, Result};

type Call = LocalBoxFuture<
    'static,
    std::result::Result<v8::Global<v8::Value>, Box<deno_core::error::JsError>>,
>;
pub(super) type PendingRequests =
    FuturesUnordered<LocalBoxFuture<'static, Completion>>;

pub(super) enum Completion {
    Request {
        id: u64,
        result: std::result::Result<(), String>,
    },
    Cancelled {
        id: u64,
        call: Call,
    },
    #[cfg(feature = "server")]
    Command {
        result: std::result::Result<(), String>,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
        #[cfg(feature = "websocket")]
        socket: Option<(u32, bool)>,
    },
}

impl CoreEngine {
    pub(super) fn function(
        &mut self,
        source: &'static str,
    ) -> Result<v8::Global<v8::Function>> {
        let value = self
            .runtime
            .execute_script("claviron:application/function", source)?;
        deno_core::scope!(scope, self.runtime);
        let value = v8::Local::new(scope, value);
        let function = v8::Local::<v8::Function>::try_from(value)
            .map_err(|_| Error::InvalidApplicationResponse)?;
        Ok(v8::Global::new(scope, function))
    }

    fn request_call(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
        lifetime: RequestLifetime,
    ) -> Result<(u64, Call, tokio_util::sync::CancellationToken)> {
        let function =
            self.dispatch.as_ref().ok_or(Error::InvalidApplicationResponse)?;
        let (parts, body) = request.into_parts();
        let (body, input, has_body) = match body {
            RequestBody::Empty => (Vec::new(), None, false),
            RequestBody::Whole(body) => {
                let has_body = !body.is_empty();
                (body, None, has_body)
            }
            RequestBody::Stream(input) => (Vec::new(), Some(input), true),
        };
        let streamed = input.is_some();
        let cancellation = lifetime.cancellation.clone();
        let id = self
            .runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<RequestResources>()
            .insert(RequestResource {
                request_body: RefCell::new(Some(body)),
                has_input: input.is_some(),
                input: RefCell::new(input),
                reply: RefCell::new(Some(reply)),
                output: RefCell::new(None),
                completion: RefCell::new(None),
                lifetime,
                closed: Cell::new(false),
            })
            .map_err(|error| Error::ApplicationUnavailable(error.into()))?;
        let arguments: Result<[v8::Global<v8::Value>; 6]> = (|| {
            deno_core::scope!(scope, self.runtime);
            let string = |value: &str| {
                v8::String::new(scope, value).ok_or_else(|| {
                    Error::ApplicationUnavailable(
                        "request string exceeds V8 limits".into(),
                    )
                })
            };
            let method = string(parts.method.as_str())?;
            let url = string(&parts.uri.to_string())?;
            let mut headers = Vec::with_capacity(parts.headers.len());
            for (name, value) in &parts.headers {
                let name = string(name.as_str())?;
                // HTTP field values and Fetch ByteStrings use Latin-1, not UTF-8.
                let value = v8::String::new_from_one_byte(
                    scope,
                    value.as_bytes(),
                    v8::NewStringType::Normal,
                )
                .ok_or_else(|| {
                    Error::ApplicationUnavailable(
                        "request header exceeds V8 limits".into(),
                    )
                })?;
                let pair = v8::Array::new_with_elements(
                    scope,
                    &[name.into(), value.into()],
                );
                headers.push(pair.into());
            }
            let headers = v8::Array::new_with_elements(scope, &headers);
            let arguments: [v8::Local<v8::Value>; 6] = [
                v8::Number::new(scope, id as f64).into(),
                method.into(),
                url.into(),
                headers.into(),
                v8::Boolean::new(scope, streamed).into(),
                v8::Boolean::new(scope, has_body).into(),
            ];
            Ok(arguments.map(|value| v8::Global::new(scope, value)))
        })();
        let arguments = match arguments {
            Ok(arguments) => arguments,
            Err(error) => {
                if let Some(resource) = self
                    .runtime
                    .op_state()
                    .borrow_mut()
                    .borrow_mut::<RequestResources>()
                    .take(id)
                {
                    resource.finish(Err(error.to_string()));
                }
                return Err(error);
            }
        };
        let call =
            self.runtime.call_with_args(function, &arguments).boxed_local();
        Ok((id, call, cancellation))
    }

    pub(crate) fn start_request(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
        lifetime: RequestLifetime,
    ) -> Result<()> {
        let (id, call, cancellation) =
            self.request_call(request, reply, lifetime)?;
        self.track_request(id, call, cancellation);
        Ok(())
    }

    fn track_request(
        &mut self,
        id: u64,
        call: Call,
        cancellation: tokio_util::sync::CancellationToken,
    ) {
        self.pending.push(
            async move {
                match select(call, cancellation.cancelled_owned().boxed_local())
                    .await
                {
                    Either::Left((result, _)) => Completion::Request {
                        id,
                        result: result
                            .map(|_| ())
                            .map_err(|error| error.to_string()),
                    },
                    Either::Right(((), call)) => {
                        Completion::Cancelled { id, call }
                    }
                }
            }
            .boxed_local(),
        );
    }

    /// The convenience API waits for this call and preserves its error result.
    /// The managed HTTP path uses start_request and needs no completion channel.
    pub(crate) async fn handle(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
    ) -> Result<()> {
        let (id, call, cancellation) =
            self.request_call(request, reply, Default::default())?;
        let (completed, mut done) = tokio::sync::oneshot::channel();
        let call = async move {
            let result = call.await;
            let _ = completed.send(
                result.as_ref().map(|_| ()).map_err(|error| error.to_string()),
            );
            result
        }
        .boxed_local();
        self.track_request(id, call, cancellation);
        let result = tokio::select! {
            result = &mut done => result,
            driven = self.drive_event_loop() => { driven?; done.await },
        };
        result
            .map_err(|_| Error::RequestChannelClosed)?
            .map_err(Error::ApplicationUnavailable)
    }

    #[cfg(feature = "server")]
    pub(super) fn schedule_command(
        &mut self,
        script: &'static str,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
        #[cfg(feature = "websocket")] socket: Option<(u32, bool)>,
    ) {
        let promise = match self
            .runtime
            .execute_script("claviron:application/command", script)
        {
            Ok(promise) => promise,
            Err(error) => {
                #[cfg(feature = "websocket")]
                if let Some((id, _)) = socket {
                    self.remove_websocket_sink(id);
                }
                let _ = reply.send(Err(error.to_string()));
                return;
            }
        };
        let call = self.runtime.resolve(promise);
        self.pending.push(
            async move {
                Completion::Command {
                    result: call
                        .await
                        .map(|_| ())
                        .map_err(|error| error.to_string()),
                    reply,
                    #[cfg(feature = "websocket")]
                    socket,
                }
            }
            .boxed_local(),
        );
    }

    /// The owner drives all live calls together; no request borrows JsRuntime
    /// across its own I/O wait or keeps the command mailbox exclusive.
    pub(crate) async fn drive_event_loop(&mut self) -> Result<()> {
        poll_fn(|cx| self.poll_requests(cx)).await
    }

    #[cfg(feature = "server")]
    pub(super) async fn drain_requests(&mut self) -> Result<()> {
        poll_fn(|cx| {
            let progress = self.poll_requests(cx);
            if let Poll::Ready(Err(error)) = progress {
                return Poll::Ready(Err(error));
            }
            if self.pending.is_empty() {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        })
        .await
    }

    fn poll_requests(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<()>> {
        let runtime = self.runtime.poll_event_loop(cx, Default::default());
        while let Poll::Ready(Some(completion)) =
            self.pending.poll_next_unpin(cx)
        {
            match completion {
                Completion::Request { id, result } => {
                    if let Some(resource) = self
                        .runtime
                        .op_state()
                        .borrow_mut()
                        .borrow_mut::<RequestResources>()
                        .take(id)
                    {
                        resource.finish(result);
                    }
                }
                #[cfg(feature = "server")]
                Completion::Command {
                    result,
                    reply,
                    #[cfg(feature = "websocket")]
                    socket,
                } => {
                    #[cfg(feature = "websocket")]
                    if let Some((id, always)) = socket
                        && (always || result.is_err())
                    {
                        self.remove_websocket_sink(id);
                    }
                    let _ = reply.send(result);
                }
                Completion::Cancelled { id, call } => {
                    let argument = {
                        deno_core::scope!(scope, self.runtime);
                        let id = v8::Number::new(scope, id as f64);
                        v8::Global::new(scope, v8::Local::<v8::Value>::from(id))
                    };
                    if let Some(cancel) = &self.cancel {
                        // The cancellation callback is synchronous. Its AbortSignal
                        // wakes only this call; guest cleanup remains owned below.
                        drop(self.runtime.call_with_args(cancel, &[argument]));
                    }
                    self.pending.push(
                        async move {
                            Completion::Request {
                                id,
                                result: call
                                    .await
                                    .map(|_| ())
                                    .map_err(|error| error.to_string()),
                            }
                        }
                        .boxed_local(),
                    );
                    cx.waker().wake_by_ref();
                }
            }
        }
        match runtime {
            Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
            Poll::Ready(Ok(())) if self.pending.is_empty() => {
                Poll::Ready(Ok(()))
            }
            _ => Poll::Pending,
        }
    }
}
