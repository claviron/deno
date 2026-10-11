use std::path::Path;
use std::rc::Rc;

#[cfg(feature = "orbit")]
use claviron::app::ApplicationEvent;
use deno_core::{JsRuntime, ModuleSpecifier, RuntimeOptions};

use super::platform::runtime_extensions;
use super::typescript::transpile_extension;
use super::{ClavironModuleLoader, TypeScriptModuleLoader};
use crate::application::PendingRequest;
#[cfg(feature = "websocket")]
use crate::application::{
    PendingWebSocketEvent, WebSocketActionSender, WebSocketEvent,
};
use crate::backend::deno::SharedExtensionFactory;
#[cfg(feature = "orbit")]
use crate::extensions::orbit::{
    PendingApplicationEvent, application_event_json,
};
#[cfg(feature = "websocket")]
use crate::extensions::websocket::WebSocketSinks;
use crate::{ApplicationRequest, Error, ExtensionContext, Result};

#[cfg(not(feature = "fetch"))]
const WEB_BOOTSTRAP: &str = include_str!("web.js");
const DISPATCH_SCRIPT: &str =
    "globalThis[Symbol.for('claviron.application.dispatch')]()";
#[cfg(feature = "server")]
const SHUTDOWN_SCRIPT: &str =
    "globalThis[Symbol.for('claviron.application.shutdown')]()";
#[cfg(feature = "orbit")]
const APPLICATION_EVENT_DISPATCH_SCRIPT: &str =
    "globalThis[Symbol.for('claviron.orbit.events')].dispatch()";
#[cfg(feature = "websocket")]
const WEBSOCKET_BOOTSTRAP: &str = include_str!("websocket.js");
#[cfg(feature = "websocket")]
const WEBSOCKET_DISPATCH_SCRIPT: &str =
    "globalThis[Symbol.for('claviron.application.websocket.dispatch')]()";
#[cfg(not(feature = "websocket"))]
const WEBSOCKET_BOOTSTRAP: &str = "";

pub(crate) struct CoreEngine {
    runtime: JsRuntime,
    #[cfg(feature = "websocket")]
    websocket: bool,
}

impl CoreEngine {
    #[cfg(test)]
    pub(crate) fn new(application_name: &str) -> Self {
        Self::with_context(ExtensionContext::new(application_name), &[])
            .expect("the built-in JavaScript extension registry is valid")
    }

    #[cfg(test)]
    pub(crate) fn with_extensions(
        application_name: &str,
        factories: &[SharedExtensionFactory],
    ) -> Result<Self> {
        Self::with_context(ExtensionContext::new(application_name), factories)
    }

    pub(crate) fn with_context(
        context: ExtensionContext,
        factories: &[SharedExtensionFactory],
    ) -> Result<Self> {
        let modules = factories
            .iter()
            .flat_map(|factory| factory.modules().iter().copied());
        let module_loader = Rc::new(ClavironModuleLoader::with_modules(
            Rc::new(TypeScriptModuleLoader::default()),
            modules,
        )?);
        let runtime = JsRuntime::new(RuntimeOptions {
            module_loader: Some(module_loader),
            extension_transpiler: Some(Rc::new(transpile_extension)),
            extensions: runtime_extensions(context, factories),
            ..Default::default()
        });

        Ok(Self {
            runtime,
            #[cfg(feature = "websocket")]
            websocket: false,
        })
    }

    #[cfg(feature = "server")]
    pub(crate) fn isolate_handle(&mut self) -> deno_core::v8::IsolateHandle {
        self.runtime.v8_isolate().thread_safe_handle()
    }

    #[cfg(feature = "server")]
    pub(crate) fn cancel_terminate_execution(&mut self) {
        self.runtime.v8_isolate().cancel_terminate_execution();
    }

    pub(crate) async fn boot(
        &mut self,
        main_module: &Path,
    ) -> Result<()> {
        let main_module = ModuleSpecifier::from_file_path(main_module)
            .map_err(|()| {
                Error::InvalidModulePath(main_module.to_path_buf())
            })?;
        #[cfg(not(feature = "fetch"))]
        self.runtime.execute_script("claviron:web", WEB_BOOTSTRAP)?;

        let bootstrap_specifier =
            ModuleSpecifier::parse("claviron:application/bootstrap")
                .expect("valid internal application module URL");
        let main_module_json = serde_json::to_string(main_module.as_str())?;
        let websocket_bootstrap = WEBSOCKET_BOOTSTRAP;
        let bootstrap = format!(
            r#"
                import application from {main_module_json};

                const fetch = typeof application === "function"
                    ? application
                    : application?.fetch;
                if (typeof fetch !== "function") {{
                    throw new TypeError("JavaScript application must default-export a fetch function or an object with fetch(request)");
                }}

                const runtime = globalThis[Symbol.for("claviron.runtime")];
                globalThis[Symbol.for("claviron.application.dispatch")] = async () => {{
                    const raw = JSON.parse(runtime.takeApplicationRequest());
                    let requestBody;
                    if (raw.streamed) {{
                        // Pieces as they arrive; an empty one is the end.
                        requestBody = new ReadableStream({{
                            async pull(controller) {{
                                const chunk = await runtime.readRequestBody();
                                if (chunk.length === 0) controller.close();
                                else controller.enqueue(chunk);
                            }},
                        }});
                    }} else {{
                        const whole = runtime.takeApplicationRequestBody();
                        requestBody = whole.length === 0 ? undefined : whole;
                    }}
                    const request = new Request(raw.url, {{
                        method: raw.method,
                        headers: raw.headers,
                        body: requestBody,
                        duplex: "half",
                    }});
                    const response = await fetch.call(application, request);
                    if (!(response instanceof Response)) {{
                        throw new TypeError("JavaScript application fetch handler must return a Response");
                    }}
                    runtime.respondHead(JSON.stringify({{
                        status: response.status,
                        headers: Array.from(response.headers),
                    }}));
                    if (response.body !== null) {{
                        const reader = response.body.getReader();
                        while (true) {{
                            const {{ done, value }} = await reader.read();
                            if (done) break;
                            if (value.length !== 0 && !(await runtime.writeBody(value))) {{
                                // The client is gone: stop the source too.
                                await reader.cancel();
                                break;
                            }}
                        }}
                    }}
                }};

                globalThis[Symbol.for("claviron.application.shutdown")] = async () => {{
                    await application?.shutdown?.call(application);
                }};

                {websocket_bootstrap}
            "#
        );
        let module_id = self
            .runtime
            .load_main_es_module_from_code(&bootstrap_specifier, bootstrap)
            .await?;
        let evaluation = self.runtime.mod_evaluate(module_id);

        self.runtime.run_event_loop(Default::default()).await?;
        evaluation.await?;

        #[cfg(feature = "websocket")]
        {
            let value = self.runtime.execute_script(
                "claviron:application/websocket-capability",
                "Boolean(globalThis[Symbol.for('claviron.application.websocket.available')])"
            )?;
            deno_core::scope!(scope, self.runtime);
            self.websocket =
                deno_core::v8::Local::new(scope, value).boolean_value(scope);
        }

        Ok(())
    }

    /// Load and evaluate a module for what it does, not for what it exports.
    ///
    /// [`Self::boot`] wraps the main module in a bootstrap that imports its
    /// default export and calls it per request — an application. A task has no
    /// request to answer: it *is* the work, so the module is the main module,
    /// its top level runs, and whatever it leaves on the event loop keeps it
    /// alive. Returns when the module has settled and the loop is idle.
    #[cfg(feature = "tasks")]
    pub(crate) async fn boot_program(
        &mut self,
        main_module: &Path,
    ) -> Result<()> {
        let main_module = ModuleSpecifier::from_file_path(main_module)
            .map_err(|()| {
                Error::InvalidModulePath(main_module.to_path_buf())
            })?;
        #[cfg(not(feature = "fetch"))]
        self.runtime.execute_script("claviron:web", WEB_BOOTSTRAP)?;

        let module_id = self.runtime.load_main_es_module(&main_module).await?;
        let evaluation = self.runtime.mod_evaluate(module_id);
        self.runtime.run_event_loop(Default::default()).await?;
        evaluation.await?;
        Ok(())
    }

    /// Run one request. Its head goes to `reply` as soon as the handler
    /// returns a `Response` (from inside the isolate, through an op), and the
    /// body follows while this keeps driving the event loop; returns when the
    /// handler has finished writing. A handler that fails before answering
    /// sends its error to `reply` instead.
    pub(crate) async fn handle(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
    ) -> Result<()> {
        let outcome = self.dispatch(request, reply).await;
        let op_state = self.runtime.op_state();
        let mut state = op_state.borrow_mut();
        // The body ends when its sender goes; an unread request body is let
        // go with the request.
        let _ = state.try_take::<crate::application::ResponseBodySender>();
        let _ = state.try_take::<crate::application::PendingRequestStream>();
        let _ = state.try_take::<PendingRequest>();
        if let Some(crate::application::PendingReply(reply)) =
            state.try_take::<crate::application::PendingReply>()
        {
            let error = match &outcome {
                Err(error) => error.to_string(),
                Ok(()) => {
                    "JavaScript application handler ended without a response"
                        .to_owned()
                }
            };
            let _ = reply.send(Err(error));
        }
        outcome
    }

    async fn dispatch(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
    ) -> Result<()> {
        let (body, stream) = match request.body {
            crate::RequestBody::Whole(body) => (body, None),
            crate::RequestBody::Stream(receiver) => {
                (Vec::new(), Some(receiver))
            }
        };
        let head = serde_json::to_string(&crate::application::RequestHead {
            method: &request.method,
            url: &request.url,
            headers: &request.headers,
            streamed: stream.is_some(),
        })?;
        {
            let op_state = self.runtime.op_state();
            let mut state = op_state.borrow_mut();
            state.put(PendingRequest { head, body });
            if let Some(receiver) = stream {
                state.put(crate::application::PendingRequestStream(
                    std::rc::Rc::new(tokio::sync::Mutex::new(receiver)),
                ));
            }
            state.put(crate::application::PendingReply(reply));
        }
        let promise = self
            .runtime
            .execute_script("claviron:application/dispatch", DISPATCH_SCRIPT)?;
        #[allow(
            deprecated,
            reason = "deno_core compatibility API drives the event loop"
        )]
        self.runtime.resolve_value(promise).await?;
        Ok(())
    }

    /// Stop this isolate: take no new work, then let what is running end.
    ///
    /// The two are not the same, and treating them as one is what wedged a
    /// worker. A script parked on an orbit cell is waiting for an event
    /// from another process — an event that is not coming, because this
    /// process is the one leaving. That wait is told so, up front, and the
    /// script's own error path runs as part of the work this waits for. An
    /// operation already under way is not disturbed; only waiting is.
    ///
    /// Then the event loop is driven until it settles, so a shutdown hook
    /// that awaits, or a rejected wait whose `catch` has cleanup to do,
    /// actually finishes before the isolate goes away. Nothing here is
    /// bounded by a clock: an application that never finishes shows up as a
    /// drain that does not end, which is the same contract HTTP draining
    /// has.
    #[cfg(feature = "server")]
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        #[cfg(feature = "orbit")]
        crate::extensions::orbit::interrupt_cell_waits(
            &self.runtime.op_state().borrow(),
        );

        let promise = self
            .runtime
            .execute_script("claviron:application/shutdown", SHUTDOWN_SCRIPT)?;
        #[allow(
            deprecated,
            reason = "deno_core compatibility API drives the event loop"
        )]
        self.runtime.resolve_value(promise).await?;
        self.runtime.run_event_loop(Default::default()).await?;
        Ok(())
    }

    /// Drive detached promises and timers while the application worker is idle.
    #[cfg(feature = "server")]
    pub(crate) async fn drive_event_loop(&mut self) -> Result<()> {
        self.runtime.run_event_loop(Default::default()).await?;
        Ok(())
    }

    #[cfg(feature = "orbit")]
    pub(crate) async fn application_event(
        &mut self,
        event: ApplicationEvent,
    ) -> Result<()> {
        let event = application_event_json(&event)
            .map_err(Error::ApplicationUnavailable)?;
        self.runtime
            .op_state()
            .borrow_mut()
            .put(PendingApplicationEvent(event));
        let promise = self.runtime.execute_script(
            "claviron:application/fleet-event-dispatch",
            APPLICATION_EVENT_DISPATCH_SCRIPT,
        )?;
        #[allow(
            deprecated,
            reason = "deno_core compatibility API drives the event loop"
        )]
        self.runtime.resolve_value(promise).await?;
        Ok(())
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn supports_websocket(&self) -> bool {
        self.websocket
    }

    #[cfg(feature = "websocket")]
    pub(crate) async fn websocket_open(
        &mut self,
        event: WebSocketEvent,
        sender: WebSocketActionSender,
    ) -> Result<()> {
        let id = match &event {
            WebSocketEvent::Open { id, .. } => *id,
            _ => unreachable!("websocket_open requires an open event"),
        };
        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<WebSocketSinks>()
            .insert(id, sender);
        if let Err(error) = self.dispatch_websocket(event).await {
            self.remove_websocket_sink(id);
            return Err(error);
        }
        Ok(())
    }

    #[cfg(feature = "websocket")]
    pub(crate) async fn websocket_event(
        &mut self,
        event: WebSocketEvent,
    ) -> Result<()> {
        self.dispatch_websocket(event).await
    }

    #[cfg(feature = "websocket")]
    pub(crate) async fn websocket_close(
        &mut self,
        event: WebSocketEvent,
    ) -> Result<()> {
        let WebSocketEvent::Close { id, .. } = &event else {
            unreachable!("websocket_close requires a close event")
        };
        let id = *id;
        let result = self.dispatch_websocket(event).await;
        self.remove_websocket_sink(id);
        result
    }

    #[cfg(feature = "websocket")]
    async fn dispatch_websocket(
        &mut self,
        event: WebSocketEvent,
    ) -> Result<()> {
        let event = serde_json::to_string(&event)?;
        self.runtime.op_state().borrow_mut().put(PendingWebSocketEvent(event));
        let promise = self.runtime.execute_script(
            "claviron:application/websocket-dispatch",
            WEBSOCKET_DISPATCH_SCRIPT,
        )?;
        #[allow(
            deprecated,
            reason = "deno_core compatibility API drives the event loop"
        )]
        self.runtime.resolve_value(promise).await?;
        Ok(())
    }

    #[cfg(feature = "websocket")]
    fn remove_websocket_sink(
        &mut self,
        id: u32,
    ) {
        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<WebSocketSinks>()
            .remove(id);
    }
}

#[cfg(test)]
#[path = "../../tests/engine/core.rs"]
mod tests;
