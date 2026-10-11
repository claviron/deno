use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use claviron::app::{ApplicationLimits, ApplicationRetryPolicy};
use claviron::runtime::congif::{
    self, ConfigFile, ConfigObserver, Configurable,
};
use claviron::runtime::registry::Component;
use claviron::runtime::{AppState, Register};
use serde::{Deserialize, Deserializer};

use crate::server::service::JsApplicationService;
use crate::{ApplicationDefinition, Error, Result};

/// `js.yaml` as Congif prepares it; an absent file is the empty document.
#[derive(Clone, Debug, Default, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct JsConfig {
    pub engine: EngineConfig,
    #[serde(deserialize_with = "unique_names")]
    pub applications: BTreeMap<String, ApplicationConfig>,
}

impl JsConfig {
    pub fn definitions(
        &self
    ) -> std::result::Result<Vec<ApplicationDefinition>, String> {
        self.applications
            .iter()
            .map(|(name, config)| {
                config.definition(name).map_err(|error| {
                    format!("js.yaml applications.{name}: {error}")
                })
            })
            .collect()
    }

    pub(crate) fn application_policy(
        &self,
        name: &str,
    ) -> Option<(
        ApplicationDispatch,
        ApplicationLimits,
        ApplicationRetryPolicy,
        Option<Duration>,
    )> {
        self.applications.get(name).map(|application| {
            (
                application.dispatch,
                ApplicationLimits::new(
                    application.max_waiting,
                    application.execution_timeout,
                ),
                application.retry.policy(),
                application.graceful_timeout,
            )
        })
    }

    fn validate(&self) -> std::result::Result<(), String> {
        if self.engine.fetch.user_agent.trim().is_empty() {
            return Err(
                "js.yaml engine.fetch.user_agent must not be empty".to_owned()
            );
        }
        if self.engine.command_queue_capacity == 0
            || self.engine.command_queue_capacity
                > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(format!(
                "js.yaml engine.command_queue_capacity must be between 1 and {}",
                tokio::sync::Semaphore::MAX_PERMITS
            ));
        }
        if self.engine.websocket.max_message_size == 0 {
            return Err(
                "js.yaml engine.websocket.max_message_size must be greater than zero".to_owned()
            );
        }
        if self.engine.websocket.action_queue_capacity == 0 {
            return Err("js.yaml engine.websocket.action_queue_capacity must be greater than zero"
                .to_owned());
        }
        for (name, application) in &self.applications {
            if application.requests.is_some() {
                return Err(format!(
                    "js.yaml applications.{name}.requests is gone: max_waiting and execution_timeout go directly under the application; dispatch_queue_timeout is requests.queue_timeout, written on the route that selects it"
                ));
            }
            if application.max_waiting == 0
                || application.max_waiting > tokio::sync::Semaphore::MAX_PERMITS
            {
                return Err(format!(
                    "js.yaml applications.{name}.max_waiting must be between 1 and {}",
                    tokio::sync::Semaphore::MAX_PERMITS
                ));
            }
            if application
                .execution_timeout
                .is_some_and(|timeout| timeout.is_zero())
            {
                return Err(format!(
                    "js.yaml applications.{name}.execution_timeout must be greater than zero or null"
                ));
            }
            application
                .retry
                .policy()
                .validate(&format!("js.yaml applications.{name}.retry"))?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct EngineConfig {
    pub fetch: FetchConfig,
    pub websocket: WebSocketConfig,
    /// Commands waiting for one serialized application isolate.
    #[serde(default = "default_command_queue_capacity")]
    pub command_queue_capacity: usize,
    /// How long a stopping isolate may take to finish the work it already
    /// accepted. `null`/omitted waits indefinitely and never interrupts:
    /// no deadline, no kill. A finite value is the operator authorising
    /// the opposite — the drain spends it waiting for the application to
    /// notice `isCancelled()` and wind down, then stops driving the event
    /// loop and reports what was still running. Each application may
    /// override this with its own `graceful_timeout`. Zero is a choice, not
    /// an error: it authorises abandoning the guest at once.
    #[serde(
        default,
        deserialize_with = "claviron::helpers::de::deserialize_optional_duration"
    )]
    pub graceful_timeout: Option<Duration>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            fetch: FetchConfig::default(),
            websocket: WebSocketConfig::default(),
            command_queue_capacity: default_command_queue_capacity(),
            graceful_timeout: None,
        }
    }
}

/// Engine-wide WebSocket transport limits, declared beside `fetch` because
/// both describe what the runtime does on the application's behalf rather
/// than how one application is dispatched.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct WebSocketConfig {
    /// Largest single message accepted from a client and the ceiling the
    /// frame collector reassembles to. An oversized message closes the
    /// socket with 1009 rather than reaching the isolate.
    pub max_message_size: usize,
    /// Actions buffered from one application toward one socket. The
    /// application blocks once this is full, which is the backpressure a
    /// slow reader deserves.
    pub action_queue_capacity: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            max_message_size: default_websocket_max_message_size(),
            action_queue_capacity: default_websocket_action_queue_capacity(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct FetchConfig {
    pub user_agent: String,
    pub trust_certstore: bool,
}

impl Default for FetchConfig {
    fn default() -> Self {
        Self { user_agent: "Claviron".to_owned(), trust_certstore: true }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ApplicationConfig {
    pub module: PathBuf,
    #[serde(default = "default_instances")]
    pub instances: usize,
    #[serde(default)]
    pub dispatch: ApplicationDispatch,
    /// Callers admitted at once, running and waiting for an isolate.
    #[serde(default = "default_max_waiting")]
    pub max_waiting: usize,
    /// Hard budget for one accepted handler; `null` leaves it unbounded. An
    /// expired handler is terminated inside its isolate. Running, not request
    /// handling: the wait for an isolate is the route's `requests.queue_timeout`.
    #[serde(
        default = "default_execution_timeout",
        deserialize_with = "claviron::helpers::de::deserialize_optional_duration"
    )]
    pub execution_timeout: Option<Duration>,
    /// Removed; read only to say where each field went.
    #[serde(default)]
    pub requests: Option<Ignored>,
    #[serde(default)]
    pub retry: ApplicationRetryConfig,
    /// This application's own drain deadline. `null`/omitted inherits
    /// `engine.graceful_timeout`, which itself defaults to waiting
    /// indefinitely.
    #[serde(
        default,
        deserialize_with = "claviron::helpers::de::deserialize_optional_duration"
    )]
    pub graceful_timeout: Option<Duration>,
    #[serde(default, alias = "environment")]
    pub env: BTreeMap<String, String>,
    #[serde(default, deserialize_with = "claviron::helpers::de::one_or_many")]
    pub env_file: Vec<PathBuf>,
}

/// When a failed instance is replaced. The same three keys PHP applications
/// declare, resolving to the same `claviron-app` policy, so the two runtimes
/// cannot drift into two different backoff curves.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct ApplicationRetryConfig {
    /// Consecutive failures allowed before the instance is left down.
    /// null retries for as long as the application is enabled.
    pub max_attempts: Option<u32>,
    #[serde(deserialize_with = "claviron::helpers::de::deserialize_duration")]
    pub backoff: Duration,
    #[serde(deserialize_with = "claviron::helpers::de::deserialize_duration")]
    pub backoff_max: Duration,
}

impl ApplicationRetryConfig {
    fn policy(self) -> ApplicationRetryPolicy {
        ApplicationRetryPolicy::new(
            self.backoff,
            self.backoff_max,
            self.max_attempts,
        )
    }
}

impl Default for ApplicationRetryConfig {
    fn default() -> Self {
        let policy = ApplicationRetryPolicy::default();
        Self {
            max_attempts: policy.max_attempts(),
            backoff: policy.backoff(),
            backoff_max: policy.backoff_max(),
        }
    }
}

impl ApplicationConfig {
    fn definition(
        &self,
        name: &str,
    ) -> Result<ApplicationDefinition> {
        let module = if self.module.is_absolute() {
            self.module.clone()
        } else {
            std::env::current_dir()
                .map_err(|error| {
                    Error::ApplicationUnavailable(error.to_string())
                })?
                .join(&self.module)
        };
        ApplicationDefinition::new(name, module)
            .and_then(|definition| definition.instances(self.instances))
            .map(|definition| {
                definition
                    .environments(self.env.clone())
                    .env_files(self.env_file.clone())
            })
    }
}

/// How HTTP requests are assigned to one application's fixed isolate set.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ApplicationDispatch {
    #[default]
    RoundRobin,
}

fn unique_names<'de, D, T>(
    deserializer: D
) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Names<T>(std::marker::PhantomData<T>);

    impl<'de, T: Deserialize<'de>> serde::de::Visitor<'de> for Names<T> {
        type Value = BTreeMap<String, T>;

        fn expecting(
            &self,
            formatter: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            formatter.write_str("a map of unique JavaScript application names")
        }

        fn visit_map<M: serde::de::MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut entries = BTreeMap::new();
            while let Some((name, value)) = map.next_entry::<String, T>()? {
                if entries.insert(name.clone(), value).is_some() {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate JavaScript application: {name}"
                    )));
                }
            }
            Ok(entries)
        }
    }

    deserializer.deserialize_map(Names(std::marker::PhantomData))
}

fn default_instances() -> usize {
    1
}

fn default_command_queue_capacity() -> usize {
    64
}

fn default_websocket_max_message_size() -> usize {
    1024 * 1024
}

fn default_websocket_action_queue_capacity() -> usize {
    64
}

pub(crate) fn default_retry_policy() -> ApplicationRetryPolicy {
    ApplicationRetryPolicy::default()
}

pub(crate) fn default_limits() -> ApplicationLimits {
    ApplicationLimits::new(default_max_waiting(), default_execution_timeout())
}

fn default_max_waiting() -> usize {
    256
}

fn default_execution_timeout() -> Option<Duration> {
    Some(Duration::from_secs(30))
}

/// A removed section: accepted whatever it holds, and equal to itself.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Ignored;

impl<'de> Deserialize<'de> for Ignored {
    fn deserialize<D: Deserializer<'de>>(
        deserializer: D
    ) -> std::result::Result<Self, D::Error> {
        serde::de::IgnoredAny::deserialize(deserializer).map(|_| Self)
    }
}

/// The owner of `js.yaml`. Congif prepares it before boot and in the reload
/// batch; `configure` hands [`JsApplicationService`] its engine settings and
/// declares its applications, before the service generation starts. A change
/// prepared while a generation runs applies its engine settings to the next
/// one; a new or changed application is refused until the service restarts,
/// which the refusal says.
pub struct JsConfigProvider {
    config_path: PathBuf,
    service: Arc<JsApplicationService>,
}

impl JsConfigProvider {
    pub fn new(
        config_path: PathBuf,
        service: Arc<JsApplicationService>,
    ) -> Self {
        Self { config_path, service }
    }

    fn apply(
        &self,
        config: &JsConfig,
    ) -> Result<()> {
        self.service.configure_engine(&config.engine);
        for definition in
            config.definitions().map_err(Error::ApplicationUnavailable)?
        {
            let (dispatch, limits, retry, graceful_timeout) = config
                .application_policy(definition.name())
                .expect("definition came from the same JavaScript config");
            self.service.register_application_with_policy(
                definition,
                dispatch,
                limits,
                retry,
                graceful_timeout,
            )?;
        }
        Ok(())
    }
}

impl Register<AppState> for JsConfigProvider {
    fn register(state: &AppState) -> claviron::runtime::registry::Result<()> {
        crate::server::service::register(state)
    }
}

#[async_trait]
impl Configurable for JsConfigProvider {
    type Config = JsConfig;

    fn source(&self) -> ConfigFile {
        ConfigFile::yaml(&self.config_path)
    }

    /// No file, no application: the service stays registered and idle.
    fn absent_source(&self) -> Option<JsConfig> {
        Some(JsConfig::default())
    }

    fn validate_config(
        &self,
        config: &JsConfig,
    ) -> congif::Result<()> {
        config.validate().map_err(congif::Error::invalid)
    }

    async fn configure(
        &self,
        config: &JsConfig,
    ) -> congif::Result<()> {
        self.apply(config).map_err(congif::Error::msg)
    }

    fn as_config_observer(
        &self
    ) -> Option<&dyn ConfigObserver<Config = Self::Config>> {
        Some(self)
    }
}

#[async_trait]
impl ConfigObserver for JsConfigProvider {
    async fn config_changed(
        &self,
        config: &JsConfig,
    ) -> congif::Result<()> {
        self.apply(config).map_err(congif::Error::msg)
    }
}

impl Component<AppState> for JsConfigProvider {
    type Deps = ();

    fn name(&self) -> &'static str {
        "JsConfigProvider"
    }

    fn as_configurable(&self) -> Option<&dyn congif::ConfigRegistration> {
        Some(self)
    }
}
