use std::fs;
use std::sync::Arc;

use claviron::app::management::{ApplicationStarted, ApplicationStopped};
use claviron::app::{
    ApplicationId, ApplicationLifecycle, ApplicationManager, ApplicationName,
    ApplicationRuntime,
};
use claviron::runtime::{
    AppState, FleetCapacity, FleetNodeId, RunMode, WorkerCount, WorkerId,
};
use tempfile::tempdir;

use super::{JsApplicationController, JsApplicationService, register};
use crate::ApplicationDefinition;

#[tokio::test]
async fn environment_notification_updates_only_the_stopped_target_without_starting_it()
 {
    let service = JsApplicationService::new();
    for name in ["example", "other"] {
        service
            .register_application(
                ApplicationDefinition::new(name, "/app.js").unwrap(),
            )
            .unwrap();
        service.applications.write().unwrap().get_mut(name).unwrap().enabled =
            false;
    }
    service.running.store(true, std::sync::atomic::Ordering::Release);
    let events = claviron::runtime::ProcessEventBus::new();
    service
        .environment_changed(
            &events,
            "example",
            [("VALUE".into(), "new".into())].into(),
        )
        .await
        .unwrap();
    let applications = service.applications.read().unwrap();
    let target = &applications["example"];
    assert_eq!(target.environment["VALUE"], "new");
    assert!(!target.enabled);
    assert!(target.pool.is_none());
    assert_eq!(target.generation, 0);
    assert_eq!(target.lifecycle, ApplicationLifecycle::Stopped);
    assert!(applications["other"].environment.is_empty());
}

fn supervised_state(
    run_mode: RunMode,
    node_id: u16,
) -> AppState {
    AppState::initialize(
        run_mode,
        FleetNodeId::new(node_id),
        WorkerCount::new(2),
        FleetCapacity::new(3).unwrap(),
        Some("js-registration-test".into()),
        ".",
    )
}

#[test]
fn registers_application_service_only_where_javascript_can_run() {
    let master = supervised_state(RunMode::Master, 0);
    register(&master).unwrap();
    assert!(master.registry_ref().resolve::<JsApplicationService>().is_none());

    let worker = supervised_state(RunMode::Worker(WorkerId::new(1)), 1);
    #[cfg(feature = "postgres")]
    claviron::db::register(&worker).unwrap();
    claviron::app::register(&worker).unwrap();
    register(&worker).unwrap();
    let service =
        worker.registry_ref().resolve::<JsApplicationService>().unwrap();
    service
        .register_application(
            ApplicationDefinition::new("api", "/app.js").unwrap(),
        )
        .unwrap();
    let manager =
        worker.registry_ref().resolve::<ApplicationManager>().unwrap();
    let id = ApplicationId::new(
        ApplicationRuntime::new("js").unwrap(),
        ApplicationName::new("api").unwrap(),
    );
    assert_eq!(
        manager.status(&id).unwrap().lifecycle,
        ApplicationLifecycle::Stopped
    );

    let standalone = AppState::standalone("js-registration-test", ".");
    #[cfg(feature = "postgres")]
    claviron::db::register(&standalone).unwrap();
    register(&standalone).unwrap();
    assert!(
        standalone.registry_ref().resolve::<JsApplicationService>().is_some()
    );
}

#[tokio::test]
async fn controller_manages_one_application_and_reports_real_generations() {
    let directory = tempdir().unwrap();
    let module = directory.path().join("application.js");
    fs::write(
        &module,
        r#"export default { fetch() { return new Response("ready"); } };"#,
    )
    .unwrap();

    let state = AppState::standalone("js-controller-test", ".");
    let service = Arc::new(JsApplicationService::new());
    service
        .register_application(
            ApplicationDefinition::new("api", module)
                .unwrap()
                .instances(2)
                .unwrap(),
        )
        .unwrap();
    service.running.store(true, std::sync::atomic::Ordering::Release);

    let manager = ApplicationManager::new();
    manager
        .add(Arc::new(JsApplicationController::new(
            &state,
            Arc::clone(&service),
        )))
        .unwrap();
    let id = ApplicationId::new(
        ApplicationRuntime::new("js").unwrap(),
        ApplicationName::new("api").unwrap(),
    );
    let mut started = state.events().subscribe::<ApplicationStarted>();
    let mut stopped = state.events().subscribe::<ApplicationStopped>();

    manager.start(&id).await.unwrap();
    let first = started.recv().await.unwrap();
    let snapshot = manager.status(&id).unwrap();
    assert_eq!(first.id, id);
    assert_eq!(first.generation, 1);
    assert_eq!(snapshot.lifecycle, ApplicationLifecycle::Running);
    assert_eq!(snapshot.generation, 1);
    assert_eq!(snapshot.pool.unwrap().instances, 2);

    manager.restart(&id).await.unwrap();
    assert_eq!(stopped.recv().await.unwrap().generation, 1);
    assert_eq!(started.recv().await.unwrap().generation, 2);
    assert_eq!(manager.status(&id).unwrap().generation, 2);

    manager.stop(&id).await.unwrap();
    assert_eq!(stopped.recv().await.unwrap().generation, 2);
    let snapshot = manager.status(&id).unwrap();
    assert_eq!(snapshot.lifecycle, ApplicationLifecycle::Stopped);
    assert!(!snapshot.enabled);
    assert!(snapshot.pool.is_none());
}
