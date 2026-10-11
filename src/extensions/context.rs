use std::collections::BTreeMap;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

/// Feature-stable carrier for the optional outbound-fetch trust store.
///
/// Keeping a real type when `fetch` is disabled prevents server/application
/// code from degenerating into unit arguments and feature-specific call sites.
#[derive(Clone, Debug, Default)]
pub(crate) struct FetchRootCertStore {
    #[cfg(feature = "fetch")]
    store: Option<Arc<deno_tls::rustls::RootCertStore>>,
}

impl FetchRootCertStore {
    #[cfg(feature = "fetch")]
    pub(crate) fn new(
        store: Option<Arc<deno_tls::rustls::RootCertStore>>
    ) -> Self {
        Self { store }
    }

    #[cfg(feature = "fetch")]
    fn store(&self) -> Option<Arc<deno_tls::rustls::RootCertStore>> {
        self.store.clone()
    }
}

/// Engine-independent state available to Claviron JavaScript extensions.
#[derive(Clone, Debug)]
pub struct ExtensionContext {
    application_name: Arc<str>,
    application_instance: Option<usize>,
    environment: Arc<BTreeMap<String, String>>,
    cancellation: CancellationToken,
    #[cfg(feature = "fetch")]
    fetch_root_cert_store: FetchRootCertStore,
    #[cfg(feature = "fetch")]
    fetch_user_agent: Arc<str>,
    #[cfg(feature = "tasks")]
    task_output: Option<claviron::tasks::TaskOutput>,
}

impl ExtensionContext {
    pub fn new(application_name: impl Into<Arc<str>>) -> Self {
        Self {
            application_name: application_name.into(),
            application_instance: None,
            environment: Arc::default(),
            cancellation: CancellationToken::new(),
            #[cfg(feature = "fetch")]
            fetch_root_cert_store: FetchRootCertStore::default(),
            #[cfg(feature = "fetch")]
            fetch_user_agent: Arc::from("Claviron"),
            #[cfg(feature = "tasks")]
            task_output: None,
        }
    }

    pub fn application_name(&self) -> &str {
        &self.application_name
    }

    pub(crate) fn with_application_instance(
        mut self,
        instance: usize,
    ) -> Self {
        self.application_instance = Some(instance);
        self
    }

    #[cfg(any(feature = "server", test))]
    pub(crate) fn application_instance(&self) -> Option<usize> {
        self.application_instance
    }

    pub fn with_environment(
        mut self,
        environment: Arc<BTreeMap<String, String>>,
    ) -> Self {
        self.environment = environment;
        self
    }

    pub fn environment(
        &self,
        name: &str,
    ) -> Option<&str> {
        self.environment.get(name).map(String::as_str)
    }

    pub fn with_cancellation(
        mut self,
        cancellation: CancellationToken,
    ) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    #[cfg(feature = "fetch")]
    pub(crate) fn with_fetch_root_cert_store(
        mut self,
        fetch_root_cert_store: FetchRootCertStore,
    ) -> Self {
        self.fetch_root_cert_store = fetch_root_cert_store;
        self
    }

    #[cfg(not(feature = "fetch"))]
    pub(crate) fn with_fetch_root_cert_store(
        self,
        _fetch_root_cert_store: FetchRootCertStore,
    ) -> Self {
        self
    }

    #[cfg(feature = "fetch")]
    pub(crate) fn fetch_root_cert_store(
        &self
    ) -> Option<Arc<deno_tls::rustls::RootCertStore>> {
        self.fetch_root_cert_store.store()
    }

    #[cfg(feature = "fetch")]
    pub(crate) fn with_fetch_user_agent(
        mut self,
        user_agent: impl Into<Arc<str>>,
    ) -> Self {
        self.fetch_user_agent = user_agent.into();
        self
    }

    #[cfg(not(feature = "fetch"))]
    pub(crate) fn with_fetch_user_agent(
        self,
        _user_agent: impl Into<Arc<str>>,
    ) -> Self {
        self
    }

    #[cfg(feature = "fetch")]
    pub(crate) fn fetch_user_agent(&self) -> &str {
        &self.fetch_user_agent
    }

    #[cfg(feature = "tasks")]
    pub(crate) fn with_task_output(
        mut self,
        output: claviron::tasks::TaskOutput,
    ) -> Self {
        self.task_output = Some(output);
        self
    }

    #[cfg(feature = "tasks")]
    pub(crate) fn task_output(&self) -> Option<&claviron::tasks::TaskOutput> {
        self.task_output.as_ref()
    }
}

#[cfg(test)]
#[path = "../../tests/extensions/context.rs"]
mod tests;
