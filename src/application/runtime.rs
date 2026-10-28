use std::sync::Arc;

#[cfg(feature = "orbit")]
use claviron::app::ApplicationEvent;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "websocket")]
use crate::application::{WebSocketActionSender, WebSocketEvent};
use crate::backend::deno::SharedExtensionFactory;
use crate::engine::CoreEngine;
use crate::extensions::FetchRootCertStore;
use crate::{
    ApplicationDefinition, ApplicationRequest, ExtensionContext, Result,
};

/// One booted slim JavaScript application generation.
///
/// This value owns a V8 isolate and therefore must remain on the thread that
/// created it.
pub struct CoreApplication {
    definition: ApplicationDefinition,
    engine: CoreEngine,
}

impl CoreApplication {
    pub async fn boot(definition: ApplicationDefinition) -> Result<Self> {
        let context = ExtensionContext::new(definition.name())
            .with_application_instance(0)
            .with_environment(definition.environment_values());
        let mut engine = CoreEngine::with_context(context, &[])?;
        engine.boot(definition.main_module()).await?;

        Ok(Self { definition, engine })
    }

    #[doc(hidden)]
    pub async fn boot_with_extensions(
        definition: ApplicationDefinition,
        extensions: &[SharedExtensionFactory],
    ) -> Result<Self> {
        Self::boot_instance_with_extensions(
            definition,
            extensions,
            0,
            CancellationToken::new(),
            Default::default(),
            Arc::from("Claviron"),
        )
        .await
    }

    pub(crate) async fn boot_instance_with_extensions(
        definition: ApplicationDefinition,
        extensions: &[SharedExtensionFactory],
        instance: usize,
        cancellation: CancellationToken,
        fetch_root_cert_store: FetchRootCertStore,
        fetch_user_agent: Arc<str>,
    ) -> Result<Self> {
        let context = ExtensionContext::new(definition.name())
            .with_application_instance(instance)
            .with_environment(definition.environment_values())
            .with_cancellation(cancellation)
            .with_fetch_root_cert_store(fetch_root_cert_store)
            .with_fetch_user_agent(fetch_user_agent);
        let mut engine = CoreEngine::with_context(context, extensions)?;
        engine.boot(definition.main_module()).await?;

        Ok(Self { definition, engine })
    }

    pub fn definition(&self) -> &ApplicationDefinition {
        &self.definition
    }

    /// Run one request; its head and then its body go to `reply` as the
    /// handler produces them. Returns when the handler has finished.
    pub async fn handle(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
    ) -> Result<()> {
        self.engine.handle(request, reply).await
    }

    /// Start a request without waiting for its handler or live response body.
    /// The host must keep driving [`Self::drive_event_loop`] alongside its inbox.
    pub fn dispatch(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
    ) -> Result<()> {
        self.engine.start_request(request, reply, Default::default())
    }

    #[cfg(feature = "server")]
    pub(crate) fn start_request(
        &mut self,
        request: ApplicationRequest,
        reply: crate::application::ReplySender,
        lifetime: super::RequestLifetime,
    ) -> Result<()> {
        self.engine.start_request(request, reply, lifetime)
    }

    #[cfg(feature = "server")]
    pub(crate) async fn shutdown(&mut self) -> Result<()> {
        self.engine.shutdown().await
    }

    /// Advance all accepted calls and background JavaScript on the owner thread.
    /// Pending I/O uses runtime wakeups; no polling interval is introduced.
    pub async fn drive_event_loop(&mut self) -> Result<()> {
        self.engine.drive_event_loop().await
    }

    #[cfg(feature = "orbit")]
    pub(crate) fn application_event(
        &mut self,
        event: ApplicationEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        self.engine.application_event(event, reply)
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn supports_websocket(&self) -> bool {
        self.engine.supports_websocket()
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn websocket_open(
        &mut self,
        event: WebSocketEvent,
        sender: WebSocketActionSender,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        self.engine.websocket_open(event, sender, reply)
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn websocket_event(
        &mut self,
        event: WebSocketEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        self.engine.websocket_event(event, reply)
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn websocket_close(
        &mut self,
        event: WebSocketEvent,
        reply: tokio::sync::oneshot::Sender<std::result::Result<(), String>>,
    ) {
        self.engine.websocket_close(event, reply)
    }
}
