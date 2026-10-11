use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
#[cfg(feature = "orbit")]
use claviron::app::ApplicationEvent;
use claviron::app::environment::{ApplicationEnvironments, EnvironmentContext};
use claviron::app::management::{
    ApplicationFailed, ApplicationStarted, ApplicationStopped,
};
use claviron::app::{
    ApplicationId, ApplicationLifecycle, ApplicationLimits, ApplicationName,
    ApplicationRetryPolicy, ApplicationRuntime,
};
use claviron::runtime::registry::{self, Component, Runnable, priority};
use claviron::runtime::{
    AppState, ProcessEventBus, RunContext, ServiceStartPolicy,
};
use tracing::{error, info};

use super::JsApplicationController;
#[cfg(feature = "websocket")]
use super::pool::WebSocketLease;
use super::pool::{ApplicationPool, JsPoolSnapshot, WorkerBoot};
use crate::backend::deno::SharedExtensionFactory;
#[cfg(feature = "websocket")]
use crate::config::WebSocketConfig;
use crate::config::{
    ApplicationDispatch, EngineConfig, JsConfigProvider, default_limits,
    default_retry_policy,
};
use crate::extensions::FetchRootCertStore;
use crate::{
    ApplicationDefinition, ApplicationRequest, ApplicationResponse, Error,
    Result,
};

/// Worker-local owner of named persistent JavaScript application pools.
pub struct JsApplicationService {
    applications: RwLock<BTreeMap<String, ManagedApplication>>,
    operation: tokio::sync::Mutex<()>,
    extensions: Arc<[SharedExtensionFactory]>,
    fetch_root_cert_store: RwLock<FetchRootCertStore>,
    /// `js.yaml`'s `engine:` as `JsConfigProvider` applied it; a pool takes
    /// the values at prepare time.
    engine: RwLock<EngineSettings>,
    sealed: AtomicBool,
    running: AtomicBool,
}

struct ManagedApplication {
    definition: ApplicationDefinition,
    environment: BTreeMap<String, String>,
    dispatch: ApplicationDispatch,
    limits: ApplicationLimits,
    retry: ApplicationRetryPolicy,
    /// This application's own drain deadline; `None` inherits the engine's.
    graceful_timeout: Option<Duration>,
    enabled: bool,
    lifecycle: ApplicationLifecycle,
    generation: u64,
    pool: Option<Arc<ApplicationPool>>,
    last_error: Option<String>,
}

pub(crate) struct JsApplicationStatus {
    pub(crate) name: String,
    pub(crate) enabled: bool,
    pub(crate) lifecycle: ApplicationLifecycle,
    pub(crate) generation: u64,
    pub(crate) pool: Option<JsPoolSnapshot>,
    pub(crate) last_error: Option<String>,
}

/// The engine-wide settings a pool is built with.
#[derive(Clone)]
struct EngineSettings {
    fetch_user_agent: Arc<str>,
    trust_certstore: bool,
    command_queue_capacity: usize,
    graceful_timeout: Option<Duration>,
    // Read only where the transport exists; the pool takes it at prepare time.
    #[cfg(feature = "websocket")]
    websocket: WebSocketConfig,
}

impl EngineSettings {
    fn from_config(engine: &EngineConfig) -> Self {
        Self {
            fetch_user_agent: Arc::from(engine.fetch.user_agent.as_str()),
            trust_certstore: engine.fetch.trust_certstore,
            command_queue_capacity: engine.command_queue_capacity,
            graceful_timeout: engine.graceful_timeout,
            #[cfg(feature = "websocket")]
            websocket: engine.websocket,
        }
    }
}

impl JsApplicationService {
    pub fn new() -> Self {
        Self {
            applications: RwLock::new(BTreeMap::new()),
            operation: tokio::sync::Mutex::new(()),
            extensions: Arc::from([]),
            fetch_root_cert_store: RwLock::new(Default::default()),
            engine: RwLock::new(EngineSettings::from_config(
                &EngineConfig::default(),
            )),
            sealed: AtomicBool::new(false),
            running: AtomicBool::new(false),
        }
    }

    /// Apply `js.yaml`'s `engine:`; pools prepared from now on use it.
    pub(crate) fn configure_engine(
        &self,
        engine: &EngineConfig,
    ) {
        *self.engine.write().unwrap() = EngineSettings::from_config(engine);
    }

    /// The fetch identity and trust the engine was configured with, for a
    /// task's isolate.
    pub(crate) fn fetch_settings(&self) -> (Arc<str>, bool) {
        let engine = self.engine.read().unwrap();
        (Arc::clone(&engine.fetch_user_agent), engine.trust_certstore)
    }

    pub fn with_extensions(
        extensions: impl IntoIterator<Item = SharedExtensionFactory>
    ) -> Self {
        Self {
            extensions: extensions.into_iter().collect::<Vec<_>>().into(),
            ..Self::new()
        }
    }

    /// Contribute a desired application before this service generation starts.
    /// Re-registering the exact same definition is idempotent for host aliases.
    pub fn register_application(
        &self,
        definition: ApplicationDefinition,
    ) -> Result<()> {
        self.register_application_with_policy(
            definition,
            ApplicationDispatch::RoundRobin,
            default_limits(),
            default_retry_policy(),
            None,
        )
    }

    pub(crate) fn register_application_with_policy(
        &self,
        definition: ApplicationDefinition,
        dispatch: ApplicationDispatch,
        limits: ApplicationLimits,
        retry: ApplicationRetryPolicy,
        graceful_timeout: Option<Duration>,
    ) -> Result<()> {
        let mut applications = self.applications.write().unwrap();
        if let Some(existing) = applications.get(definition.name()) {
            return if existing.definition == definition
                && existing.dispatch == dispatch
                && existing.limits == limits
                && existing.retry == retry
                && existing.graceful_timeout == graceful_timeout
            {
                Ok(())
            } else {
                Err(Error::ApplicationUnavailable(format!(
                    "JavaScript application '{}' has conflicting definitions",
                    definition.name()
                )))
            };
        }
        if self.sealed.load(Ordering::Acquire) {
            return Err(Error::ApplicationUnavailable(format!(
                "JavaScript application '{}' was registered after the generation started",
                definition.name()
            )));
        }
        applications.insert(
            definition.name().to_owned(),
            ManagedApplication {
                environment: BTreeMap::new(),
                definition,
                dispatch,
                limits,
                retry,
                graceful_timeout,
                enabled: true,
                lifecycle: ApplicationLifecycle::Stopped,
                generation: 0,
                pool: None,
                last_error: None,
            },
        );
        Ok(())
    }

    pub fn is_configured(
        &self,
        name: &str,
    ) -> bool {
        self.applications.read().unwrap().contains_key(name)
    }

    /// Run one request on `name`. `queue_timeout` is its route's
    /// `requests.queue_timeout`: the wait for an isolate (unset waits, zero
    /// rejects at once).
    pub async fn execute(
        &self,
        name: &str,
        request: ApplicationRequest,
        queue_timeout: Option<Duration>,
    ) -> Result<ApplicationResponse> {
        let pool = self
            .applications
            .read()
            .unwrap()
            .get(name)
            .filter(|application| {
                application.lifecycle == ApplicationLifecycle::Running
            })
            .and_then(|application| application.pool.clone())
            .ok_or_else(|| Error::ApplicationNotRunning(name.to_owned()))?;
        pool.execute(request, queue_timeout).await
    }

    #[cfg(feature = "orbit")]
    async fn application_event(
        &self,
        event: ApplicationEvent,
    ) -> Result<()> {
        let pools = self
            .applications
            .read()
            .unwrap()
            .iter()
            .filter(|(name, application)| {
                application.lifecycle == ApplicationLifecycle::Running
                    && event.target().is_none_or(|target| {
                        target.runtime().as_str() == "js"
                            && target.name().as_str() == name.as_str()
                    })
            })
            .filter_map(|(_, application)| application.pool.clone())
            .collect::<Vec<_>>();
        for pool in pools {
            // Topic subscriptions live inside each application isolate. An
            // isolate without a matching listener discards the event.
            //
            // Keep every instance of a subscribed application coherent: this
            // is fleet broadcast, not queue/consumer-group dispatch.
            pool.application_event(event.clone()).await?;
        }
        Ok(())
    }

    #[cfg(feature = "websocket")]
    pub fn prepare_websocket(
        &self,
        name: &str,
    ) -> Result<WebSocketLease> {
        let pool = self
            .applications
            .read()
            .unwrap()
            .get(name)
            .filter(|application| {
                application.lifecycle == ApplicationLifecycle::Running
            })
            .and_then(|application| application.pool.clone())
            .ok_or_else(|| Error::ApplicationNotRunning(name.to_owned()))?;
        pool.prepare_websocket()
    }

    pub(crate) fn statuses(&self) -> Vec<JsApplicationStatus> {
        self.applications
            .read()
            .unwrap()
            .iter()
            .map(|(name, application)| application_status(name, application))
            .collect()
    }

    pub(crate) fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<JsApplicationStatus> {
        self.applications
            .read()
            .unwrap()
            .get(name.as_str())
            .map(|application| application_status(name.as_str(), application))
    }

    pub(crate) async fn start_application(
        &self,
        events: &ProcessEventBus,
        name: &str,
    ) -> Result<()> {
        let _operation = self.operation.lock().await;
        if !self.running.load(Ordering::Acquire) {
            return Err(Error::ApplicationUnavailable(
                "JavaScript application service is not running".to_owned(),
            ));
        }
        self.applications
            .write()
            .unwrap()
            .get_mut(name)
            .ok_or_else(|| {
                Error::ApplicationUnavailable(format!(
                    "JavaScript application '{name}' is not configured"
                ))
            })?
            .enabled = true;
        self.start_application_locked(events, name).await
    }

    async fn start_application_locked(
        &self,
        events: &ProcessEventBus,
        name: &str,
    ) -> Result<()> {
        let (definition, dispatch, limits, retry, application_graceful_timeout) = {
            let mut applications = self.applications.write().unwrap();
            let application = applications.get_mut(name).ok_or_else(|| {
                Error::ApplicationUnavailable(format!(
                    "JavaScript application '{name}' is not configured"
                ))
            })?;
            application.last_error = None;
            if application.pool.is_some() {
                application.lifecycle = ApplicationLifecycle::Running;
                return Ok(());
            }
            application.lifecycle = ApplicationLifecycle::Starting;
            (
                application
                    .definition
                    .clone()
                    .resolved_environment(application.environment.clone()),
                application.dispatch,
                application.limits,
                application.retry,
                application.graceful_timeout,
            )
        };

        let instances = definition.instance_count();
        let fetch_root_cert_store =
            self.fetch_root_cert_store.read().unwrap().clone();
        let engine = self.engine.read().unwrap().clone();
        match ApplicationPool::start(
            WorkerBoot::new(
                definition,
                Arc::clone(&self.extensions),
                fetch_root_cert_store,
                Arc::clone(&engine.fetch_user_agent),
                engine.command_queue_capacity,
                application_graceful_timeout.or(engine.graceful_timeout),
            ),
            dispatch,
            limits,
            retry,
            #[cfg(feature = "websocket")]
            engine.websocket,
        )
        .await
        {
            Ok(pool) => {
                let generation = {
                    let mut applications = self.applications.write().unwrap();
                    let application = applications
                        .get_mut(name)
                        .expect("application retained");
                    application.generation =
                        application.generation.saturating_add(1);
                    application.lifecycle = ApplicationLifecycle::Running;
                    application.pool = Some(pool);
                    application.generation
                };
                info!(
                    application = name,
                    instances,
                    generation,
                    "🧩 JavaScript application pool started"
                );
                events.emit(ApplicationStarted {
                    id: javascript_application_id(name),
                    generation,
                });
                Ok(())
            }
            Err(error) => {
                let message = error.to_string();
                let generation = {
                    let mut applications = self.applications.write().unwrap();
                    let application = applications
                        .get_mut(name)
                        .expect("application retained");
                    application.lifecycle = ApplicationLifecycle::Failed;
                    application.last_error = Some(message.clone());
                    application.generation
                };
                error!(application = name, %error, "JavaScript application failed to start");
                events.emit(ApplicationFailed {
                    id: javascript_application_id(name),
                    generation,
                    error: message,
                });
                Err(error)
            }
        }
    }

    pub(crate) async fn stop_application(
        &self,
        events: &ProcessEventBus,
        name: &str,
    ) -> Result<()> {
        let _operation = self.operation.lock().await;
        if !self.running.load(Ordering::Acquire) {
            return Err(Error::ApplicationUnavailable(
                "JavaScript application service is not running".to_owned(),
            ));
        }
        self.stop_application_locked(events, name, false).await
    }

    async fn stop_application_locked(
        &self,
        events: &ProcessEventBus,
        name: &str,
        preserve_enabled: bool,
    ) -> Result<()> {
        let (pool, generation) = {
            let mut applications = self.applications.write().unwrap();
            let application = applications.get_mut(name).ok_or_else(|| {
                Error::ApplicationUnavailable(format!(
                    "JavaScript application '{name}' is not configured"
                ))
            })?;
            if !preserve_enabled {
                application.enabled = false;
            }
            let Some(pool) = application.pool.clone() else {
                if !preserve_enabled {
                    application.lifecycle = ApplicationLifecycle::Stopped;
                }
                return Ok(());
            };
            application.lifecycle = ApplicationLifecycle::Stopping;
            (pool, application.generation)
        };

        pool.shutdown().await;
        {
            let mut applications = self.applications.write().unwrap();
            let application =
                applications.get_mut(name).expect("application retained");
            application.pool = None;
            application.lifecycle = ApplicationLifecycle::Stopped;
        }
        info!(
            application = name,
            generation, "🔸 JavaScript application pool stopped"
        );
        events.emit(ApplicationStopped {
            id: javascript_application_id(name),
            generation,
        });
        Ok(())
    }

    pub(crate) async fn restart_application(
        &self,
        events: &ProcessEventBus,
        name: &str,
    ) -> Result<()> {
        let _operation = self.operation.lock().await;
        if !self.running.load(Ordering::Acquire) {
            return Err(Error::ApplicationUnavailable(
                "JavaScript application service is not running".to_owned(),
            ));
        }
        {
            let mut applications = self.applications.write().unwrap();
            let application = applications.get_mut(name).ok_or_else(|| {
                Error::ApplicationUnavailable(format!(
                    "JavaScript application '{name}' is not configured"
                ))
            })?;
            application.enabled = true;
        }
        self.stop_application_locked(events, name, true).await?;
        self.start_application_locked(events, name).await
    }

    async fn shutdown_generation(
        &self,
        events: &ProcessEventBus,
    ) {
        let _operation = self.operation.lock().await;
        let names = self
            .applications
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in names {
            let _ = self.stop_application_locked(events, &name, true).await;
        }
    }

    async fn environment_changed(
        &self,
        events: &ProcessEventBus,
        name: &str,
        values: BTreeMap<String, String>,
    ) -> Result<()> {
        let _operation = self.operation.lock().await;
        let enabled = {
            let mut applications = self.applications.write().unwrap();
            let Some(application) = applications.get_mut(name) else {
                return Ok(());
            };
            if application.environment == values {
                return Ok(());
            }
            application.environment = values;
            application.enabled
        };
        if enabled && self.running.load(Ordering::Acquire) {
            self.stop_application_locked(events, name, true).await?;
            self.start_application_locked(events, name).await?;
        }
        Ok(())
    }
}

fn javascript_runtime() -> ApplicationRuntime {
    ApplicationRuntime::new("js").expect("static JavaScript runtime name")
}

fn javascript_application_id(name: &str) -> ApplicationId {
    ApplicationId::new(
        javascript_runtime(),
        ApplicationName::new(name)
            .expect("validated JavaScript application name"),
    )
}

fn application_status(
    name: &str,
    application: &ManagedApplication,
) -> JsApplicationStatus {
    JsApplicationStatus {
        name: name.to_owned(),
        lifecycle: application.lifecycle,
        enabled: application.enabled,
        generation: application.generation,
        pool: application.pool.as_ref().map(|pool| pool.snapshot()),
        // A pool that is up but has a slot in backoff carries the reason
        // there; the service field only holds why a whole start failed.
        last_error: application
            .pool
            .as_ref()
            .and_then(|pool| pool.last_error())
            .or_else(|| application.last_error.clone()),
    }
}

impl Default for JsApplicationService {
    fn default() -> Self {
        Self::new()
    }
}

impl Component<AppState> for JsApplicationService {
    type Deps = ();

    fn name(&self) -> &'static str {
        "JsApplicationService"
    }

    fn run_priority(&self) -> Option<u8> {
        Some(priority::EARLY)
    }

    fn as_runnable(
        self: Arc<Self>
    ) -> Option<Arc<dyn Runnable<AppState, Self::Deps>>> {
        Some(self)
    }
}

#[async_trait]
impl Runnable<AppState> for JsApplicationService {
    fn start_policy(
        &self,
        state: &AppState,
    ) -> ServiceStartPolicy {
        if state.is_master() || self.applications.read().unwrap().is_empty() {
            ServiceStartPolicy::Unavailable
        } else {
            ServiceStartPolicy::Automatic
        }
    }

    async fn run(
        self: Arc<Self>,
        _deps: (),
        state: AppState,
        context: RunContext,
    ) -> registry::Result<()> {
        let trust_certstore = self.engine.read().unwrap().trust_certstore;
        *self.fetch_root_cert_store.write().unwrap() =
            configured_fetch_root_cert_store(&state, trust_certstore)?;
        let environment_context = EnvironmentContext::new(&state)
            .map_err(|error| registry::Error::msg(error.to_string()))?;
        let definitions = self
            .applications
            .read()
            .unwrap()
            .iter()
            .map(|(name, app)| (name.clone(), app.definition.clone()))
            .collect::<Vec<_>>();
        let mut environments = ApplicationEnvironments::default();
        for (name, definition) in definitions {
            let environment = environment_context
                .observe(
                    definition.environment_files(),
                    definition.environment_values().as_ref().clone(),
                )
                .map_err(|error| {
                    registry::Error::msg(format!(
                        "JavaScript application '{name}': {error}"
                    ))
                })?;
            let values = environment
                .snapshot()
                .map_err(|error| registry::Error::msg(error.to_string()))?
                .as_ref()
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            self.applications
                .write()
                .unwrap()
                .get_mut(&name)
                .expect("registered application")
                .environment = values;
            environments.insert(name, environment);
        }
        self.sealed.store(true, Ordering::Release);
        self.running.store(true, Ordering::Release);
        let names = self
            .applications
            .read()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for name in names {
            if !self
                .applications
                .read()
                .unwrap()
                .get(&name)
                .is_some_and(|application| application.enabled)
            {
                continue;
            }
            if let Err(error) =
                self.start_application(state.events().bus(), &name).await
            {
                self.running.store(false, Ordering::Release);
                self.shutdown_generation(state.events().bus()).await;
                self.sealed.store(false, Ordering::Release);
                return Err(registry::Error::recoverable(error.to_string()));
            }
        }

        loop {
            tokio::select! {
                biased;
                _ = context.cancelled() => break,
                (name, candidate) = environments.changed() => {
                    match candidate {
                        Ok(snapshot) => {
                            let values = snapshot.as_ref().iter().map(|(key, value)| (key.clone(), value.clone())).collect();
                            if let Err(error) = self.environment_changed(state.events().bus(), &name, values).await {
                                error!(application = %name, %error, "JavaScript application environment renewal failed");
                            }
                        }
                        Err(error) => error!(application = %name, %error, "JavaScript application environment rejected; previous execution retained"),
                    }
                }
            }
        }

        self.running.store(false, Ordering::Release);
        self.shutdown_generation(state.events().bus()).await;
        self.sealed.store(false, Ordering::Release);
        Ok(())
    }
}

pub(crate) fn configured_fetch_root_cert_store(
    _state: &AppState,
    trust_certstore: bool,
) -> registry::Result<FetchRootCertStore> {
    #[cfg(not(feature = "tls"))]
    let _ = trust_certstore;

    #[cfg(feature = "tls")]
    {
        if !trust_certstore {
            return Ok(FetchRootCertStore::new(Some(Arc::new(
                deno_tls::create_default_root_cert_store(),
            ))));
        }
        let Some(authority) =
            _state.registry_ref().resolve::<claviron::tls::CertAuthority>()
        else {
            return Ok(FetchRootCertStore::new(None));
        };
        let mut roots = deno_tls::create_default_root_cert_store();
        for root in authority.roots() {
            roots.add(root.clone()).map_err(|error| {
                registry::Error::msg(format!(
                    "JavaScript fetch trust store rejected authority {authority:?}: {error}"
                ))
            })?;
        }
        Ok(FetchRootCertStore::new(Some(Arc::new(roots))))
    }

    #[cfg(all(feature = "fetch", not(feature = "tls")))]
    {
        Ok(FetchRootCertStore::new(None))
    }

    #[cfg(not(feature = "fetch"))]
    {
        Ok(FetchRootCertStore::default())
    }
}

pub fn register(state: &AppState) -> registry::Result<()> {
    #[cfg(feature = "orbit")]
    {
        // Each is idempotent, and each is needed here: the extension resolves
        // its providers when it is built, so one registered later would be
        // "absent" in every isolate for the life of the process.
        claviron::orbit::register_counter(state)?;
        claviron::orbit::register_cells(state)?;
        claviron::orbit::register_invoke(state)?;
        claviron::orbit::register_lock(state)?;
        claviron::orbit::register(state)?;
    }

    if state.is_master() {
        return Ok(());
    }

    // `js.yaml` is prepared by Congif before boot; its owner hands this
    // service the engine settings and the applications in `configure`.
    let service = Arc::new(JsApplicationService::with_extensions(
        configured_extensions(state)?,
    ));
    if state.registry_ref().resolve::<JsConfigProvider>().is_none() {
        state.bind_config_environment::<JsConfigProvider>()?;
        state.registry_ref().register(Arc::new(JsConfigProvider::new(
            state.config_dir().join("js.yaml"),
            Arc::clone(&service),
        )))?;
    }

    // Hand the task service an engine for its `js:` kind. Installing also
    // registers, so this is safe whichever of the two boots first.
    #[cfg(feature = "tasks")]
    crate::task::register(state).map_err(registry::Error::msg)?;

    info!(
        "📦 JavaScript runtime registered; applications follow the prepared configuration"
    );

    JsApplicationController::register(state, Arc::clone(&service))?;

    #[cfg(feature = "orbit")]
    {
        let listener = Arc::clone(&service);
        state.events().listen::<ApplicationEvent, _>(move |_, event| {
            let listener = Arc::clone(&listener);
            async move {
                listener
                    .application_event(event)
                    .await
                    .map_err(|error| registry::Error::msg(error.to_string()))
            }
        })?;
        if let Some(events) = state
            .registry_ref()
            .resolve::<claviron::orbit::ApplicationEventRegistry>(
        ) {
            events.register_bridge::<ApplicationEvent>(state)?;
        }
    }

    state.registry_ref().register(service)?;
    #[cfg(feature = "edge")]
    crate::routing::edge::register(state)?;
    Ok(())
}

/// Every extension this build carries, resolved against the live registry.
///
/// Shared with the `js:` task executor deliberately: a task is not a lesser
/// application. It reaches the same cache, the same database and the same
/// Orbit fleet, because the reason to write one is usually that it needs them.
pub(crate) fn configured_extensions(
    _state: &AppState
) -> registry::Result<Vec<SharedExtensionFactory>> {
    #[allow(unused_mut)]
    let mut extensions = Vec::new();

    #[cfg(feature = "cache")]
    {
        use crate::extensions::cache::CacheExtension;

        let cache =
            _state.registry_ref().resolve::<claviron::cache::CacheProvider>();
        extensions.push(
            Arc::new(CacheExtension::new(cache)) as SharedExtensionFactory
        );
    }

    #[cfg(feature = "log")]
    {
        use crate::extensions::log::LogExtension;

        let channels = _state
            .registry_ref()
            .resolve::<claviron::logging::pipeline::LogChannels>();
        extensions.push(
            Arc::new(LogExtension::new(channels)) as SharedExtensionFactory
        );
    }

    #[cfg(feature = "postgres")]
    {
        use crate::extensions::database::DatabaseExtension;

        let database = _state
            .registry_ref()
            .resolve::<claviron::db::DbService>()
            .ok_or_else(|| {
                registry::Error::msg(
                    "the JavaScript engine requires the registered DbService",
                )
            })?;
        extensions.push(Arc::new(DatabaseExtension::new(database))
            as SharedExtensionFactory);
    }

    #[cfg(feature = "orbit")]
    {
        use crate::extensions::orbit::OrbitExtension;

        let counter = _state
            .registry_ref()
            .resolve::<claviron::orbit::CounterProvider>()
            .ok_or_else(|| {
                registry::Error::msg(
                    "the JavaScript engine requires the Orbit CounterProvider",
                )
            })?;
        let fleet = _state
            .registry_ref()
            .resolve::<claviron::orbit::FleetProvider>()
            .ok_or_else(|| {
                registry::Error::msg(
                    "the JavaScript engine requires the Orbit FleetProvider",
                )
            })?;
        // Cells and invocations are what a fleet registers; a process without
        // them still gets the modules, which then report themselves absent.
        let orbit =
            OrbitExtension::new(counter, fleet, _state.events().bus().clone())
                .with_cells(
                    _state
                        .registry_ref()
                        .resolve::<claviron::orbit::CellsProvider>(),
                )
                .with_invocations(
                    _state
                        .registry_ref()
                        .resolve::<claviron::orbit::InvocationProvider>(),
                );
        extensions.push(Arc::new(orbit) as SharedExtensionFactory);
    }

    Ok(extensions)
}

#[cfg(test)]
#[path = "../../tests/server/service.rs"]
mod tests;
