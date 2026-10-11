use std::sync::Arc;

use async_trait::async_trait;
use claviron::app::{
    ApplicationControlResult, ApplicationController, ApplicationId,
    ApplicationManager, ApplicationName, ApplicationPoolStatus,
    ApplicationRuntime, ApplicationSnapshot,
};
use claviron::runtime::{AppState, ProcessEventBus};

use super::JsApplicationService;
use super::service::JsApplicationStatus;

/// Common application-management adapter for the slim JavaScript runtime.
pub struct JsApplicationController {
    runtime: ApplicationRuntime,
    events: ProcessEventBus,
    service: Arc<JsApplicationService>,
}

impl JsApplicationController {
    pub fn new(
        state: &AppState,
        service: Arc<JsApplicationService>,
    ) -> Self {
        Self {
            runtime: javascript_runtime(),
            events: state.events().bus().clone(),
            service,
        }
    }

    pub(crate) fn register(
        state: &AppState,
        service: Arc<JsApplicationService>,
    ) -> claviron::runtime::registry::Result<()> {
        if let Some(manager) =
            state.registry_ref().resolve::<ApplicationManager>()
        {
            manager.add(Arc::new(Self::new(state, service))).map_err(
                |error| {
                    claviron::runtime::registry::Error::msg(error.to_string())
                },
            )?;
        }
        Ok(())
    }

    fn snapshot(
        &self,
        status: JsApplicationStatus,
    ) -> ApplicationSnapshot {
        ApplicationSnapshot {
            id: ApplicationId::new(
                self.runtime.clone(),
                ApplicationName::new(status.name)
                    .expect("validated JavaScript application name"),
            ),
            lifecycle: status.lifecycle,
            enabled: status.enabled,
            generation: status.generation,
            pool: status.pool.map(|pool| ApplicationPoolStatus {
                accepting: Some(
                    status.lifecycle
                        == claviron::app::ApplicationLifecycle::Running,
                ),
                instances: count(pool.instances),
                ready: Some(count(pool.ready)),
                busy: Some(count(pool.busy)),
                // No slot rotation in JavaScript: an instance is replaced
                // outright, never retired while it finishes its work.
                retiring: None,
                backing_off: Some(count(pool.backing_off)),
                exhausted: Some(count(pool.exhausted)),
            }),
            last_error: status.last_error,
        }
    }
}

#[async_trait]
impl ApplicationController for JsApplicationController {
    fn runtime(&self) -> &ApplicationRuntime {
        &self.runtime
    }

    fn list(&self) -> Vec<ApplicationSnapshot> {
        self.service
            .statuses()
            .into_iter()
            .map(|status| self.snapshot(status))
            .collect()
    }

    fn status(
        &self,
        name: &ApplicationName,
    ) -> Option<ApplicationSnapshot> {
        self.service.status(name).map(|status| self.snapshot(status))
    }

    async fn start(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.service
            .start_application(&self.events, name.as_str())
            .await
            .map_err(|error| Box::new(error) as _)
    }

    async fn stop(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.service
            .stop_application(&self.events, name.as_str())
            .await
            .map_err(|error| Box::new(error) as _)
    }

    async fn restart(
        &self,
        name: &ApplicationName,
    ) -> ApplicationControlResult<()> {
        self.service
            .restart_application(&self.events, name.as_str())
            .await
            .map_err(|error| Box::new(error) as _)
    }
}

fn javascript_runtime() -> ApplicationRuntime {
    ApplicationRuntime::new("js").expect("static JavaScript runtime name")
}

fn count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}
