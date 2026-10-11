use std::fs;
use std::sync::Arc;
use std::time::Duration;

use claviron::app::ApplicationRetryPolicy;
use tempfile::tempdir;

#[cfg(feature = "websocket")]
use super::WebSocketAction;
use super::{ApplicationPool, WorkerBoot};
use crate::ApplicationDefinition;
use crate::backend::deno::SharedExtensionFactory;
use crate::config::{ApplicationDispatch, default_limits};

#[cfg(feature = "websocket")]
#[tokio::test]
async fn drives_websocket_timers_without_another_request() {
    let directory = tempdir().unwrap();
    let module = directory.path().join("application.js");
    fs::write(&module, include_str!("../fixtures/server/timer_socket.js"))
        .unwrap();

    let definition = ApplicationDefinition::new("timer-test", module).unwrap();
    let extensions = Arc::<[SharedExtensionFactory]>::from([]);
    let pool = ApplicationPool::start(
        WorkerBoot::new(
            definition,
            extensions,
            Default::default(),
            Arc::from("Claviron"),
            64,
            None,
        ),
        ApplicationDispatch::RoundRobin,
        default_limits(),
        crate::config::default_retry_policy(),
        #[cfg(feature = "websocket")]
        crate::config::WebSocketConfig::default(),
    )
    .await
    .unwrap();
    let mut session = pool
        .prepare_websocket()
        .unwrap()
        .open("wss://example.test/socket".to_owned(), Vec::new())
        .await
        .unwrap();

    assert!(
        matches!(session.recv().await, Some(WebSocketAction::Text(value)) if value == "open")
    );
    let action = tokio::time::timeout(Duration::from_secs(1), session.recv())
        .await
        .expect("timer must fire while the command mailbox is idle");
    assert!(
        matches!(action, Some(WebSocketAction::Text(value)) if value == "timer")
    );

    pool.shutdown().await;
    assert!(
        matches!(session.recv().await, Some(WebSocketAction::Text(value)) if value == "cancelled:true")
    );
}

/// A slot whose isolate owner refuses to boot must not take the pool down
/// with it, and must stop retrying once its budget is spent. Without this,
/// a broken module either fails the whole application or loops forever and
/// the management snapshot never says which.
#[tokio::test]
async fn a_failing_instance_backs_off_and_then_reports_exhausted() {
    let directory = tempdir().unwrap();
    let module = directory.path().join("application.js");
    fs::write(&module, include_str!("../fixtures/server/boot_failure.js"))
        .unwrap();

    let definition =
        ApplicationDefinition::new("boot-failure-test", module).unwrap();
    let pool = ApplicationPool::start(
        WorkerBoot::new(
            definition,
            Arc::<[SharedExtensionFactory]>::from([]),
            Default::default(),
            Arc::from("Claviron"),
            64,
            None,
        ),
        ApplicationDispatch::RoundRobin,
        default_limits(),
        ApplicationRetryPolicy::new(
            Duration::from_millis(5),
            Duration::from_millis(5),
            Some(2),
        ),
        #[cfg(feature = "websocket")]
        crate::config::WebSocketConfig::default(),
    )
    .await
    .expect("a failed boot leaves the pool standing");

    let first = pool.snapshot();
    assert_eq!(first.instances, 0);
    assert_eq!(
        first.backing_off, 1,
        "the first failure schedules another attempt"
    );
    assert_eq!(first.exhausted, 0);

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let last = loop {
        let snapshot = pool.snapshot();
        if snapshot.exhausted == 1 {
            break snapshot;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the retry budget never ran out"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(
        last.backing_off, 0,
        "an exhausted slot is no longer waiting to retry"
    );
    assert!(
        pool.last_error().is_some(),
        "the snapshot carries why the instance is down"
    );

    pool.shutdown().await;
}
