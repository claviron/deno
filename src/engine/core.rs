use std::path::Path;
use std::rc::Rc;

#[cfg(feature = "orbit")]
use claviron::app::ApplicationEvent;
use deno_core::{JsRuntime, ModuleSpecifier, RuntimeOptions};

use super::platform::runtime_extensions;
use super::typescript::transpile_extension;
use super::{ClavironModuleLoader, TypeScriptModuleLoader};
#[path = "requests.rs"]
mod requests;
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
use crate::{Error, ExtensionContext, Result};

#[cfg(not(feature = "fetch"))]
const WEB_BOOTSTRAP: &str = include_str!("web.js");

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
    dispatch: Option<deno_core::v8::Global<deno_core::v8::Function>>,
    cancel: Option<deno_core::v8::Global<deno_core::v8::Function>>,
    pending: requests::PendingRequests,
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
            dispatch: None,
            cancel: None,
            pending: Default::default(),
            #[cfg(feature = "websocket")]
            websocket: false,
        })
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
            "import application from {main_module_json};\n({})(application);\n{websocket_bootstrap}",
            include_str!("application.js"),
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

        self.dispatch = Some(self.function(
            "globalThis[Symbol.for('claviron.application.dispatch')]",
        )?);
        self.cancel = Some(self.function(
            "globalThis[Symbol.for('claviron.application.cancel')]",
        )?);
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

        self.runtime.execute_script(
            "claviron:application/drain",
            "globalThis[Symbol.for('claviron.application.drain')]()",
        )?;
        // Accepted calls finish before application resources are torn down.
        // Background timers are left for shutdown() to clean up afterwards.
        self.drain_requests().await?;

        let (reply, finished) = tokio::sync::oneshot::channel();
        self.schedule_command(
            SHUTDOWN_SCRIPT,
            reply,
            #[cfg(feature = "websocket")]
            None,
        );
        self.drive_event_loop().await?;
        finished
            .await
            .map_err(|_| Error::RequestChannelClosed)?
            .map_err(Error::ApplicationUnavailable)
    }

    #[cfg(feature = "orbit")]
    pub(crate) fn application_event(
        &mut self,
        event: ApplicationEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let event = match application_event_json(&event) {
            Ok(event) => event,
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
                return;
            }
        };
        self.runtime
            .op_state()
            .borrow_mut()
            .put(PendingApplicationEvent(event));
        self.schedule_command(
            APPLICATION_EVENT_DISPATCH_SCRIPT,
            reply,
            #[cfg(feature = "websocket")]
            None,
        );
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn supports_websocket(&self) -> bool {
        self.websocket
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn websocket_open(
        &mut self,
        event: WebSocketEvent,
        sender: WebSocketActionSender,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let id = match &event {
            WebSocketEvent::Open { id, .. } => *id,
            _ => unreachable!("websocket_open requires an open event"),
        };
        self.runtime
            .op_state()
            .borrow_mut()
            .borrow_mut::<WebSocketSinks>()
            .insert(id, sender);
        self.dispatch_websocket(event, reply, Some((id, false)));
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn websocket_event(
        &mut self,
        event: WebSocketEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        self.dispatch_websocket(event, reply, None);
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn websocket_close(
        &mut self,
        event: WebSocketEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        let WebSocketEvent::Close { id, .. } = &event else {
            unreachable!("websocket_close requires a close event")
        };
        let id = *id;
        self.dispatch_websocket(event, reply, Some((id, true)));
    }

    #[cfg(feature = "websocket")]
    fn dispatch_websocket(
        &mut self,
        event: WebSocketEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
        socket: Option<(u32, bool)>,
    ) {
        let event = match serde_json::to_string(&event) {
            Ok(event) => event,
            Err(error) => {
                let _ = reply.send(Err(error.to_string()));
                return;
            }
        };
        self.runtime.op_state().borrow_mut().put(PendingWebSocketEvent(event));
        self.schedule_command(WEBSOCKET_DISPATCH_SCRIPT, reply, socket);
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
