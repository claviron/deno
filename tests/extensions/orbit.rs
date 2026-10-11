use claviron::app::EventValue;

use super::{
    EventSubscriptions, EventWaiter, JavaScriptLock, LockKey, LockType,
    MAX_SAFE_JAVASCRIPT_INTEGER, application_event_json, event_value_to_json,
    json_to_event_value,
};
use claviron::app::{
    ApplicationEvent, ApplicationId, ApplicationName, ApplicationRuntime,
};
use claviron::orbit::transport::LinkInterest;
use claviron::runtime::ProcessEventBus;
use std::sync::Arc;

fn application(name: &str) -> ApplicationId {
    ApplicationId::new(
        ApplicationRuntime::new("js").unwrap(),
        ApplicationName::new(name).unwrap(),
    )
}

#[test]
fn callback_interest_is_idempotent_and_released_on_unsubscribe_or_drop() {
    let interest = Arc::new(LinkInterest::default());
    let watched = interest.watch();
    let mut subscriptions =
        EventSubscriptions::new(Some(Arc::clone(&interest)));
    subscriptions.subscribe("example.shared.v1".into());
    subscriptions.subscribe("example.shared.v1".into());
    assert_eq!(watched.borrow().as_ref(), &["example.shared.v1".to_string()]);
    subscriptions.unsubscribe("example.shared.v1");
    assert!(watched.borrow().is_empty());
    subscriptions.subscribe("example.shared.v1".into());
    drop(subscriptions);
    assert!(watched.borrow().is_empty());
}

#[test]
fn waiters_and_isolates_hold_independent_topic_interest() {
    let bus = ProcessEventBus::new();
    let interest = Arc::new(LinkInterest::default());
    let watched = interest.watch();
    let mut first = EventSubscriptions::new(Some(Arc::clone(&interest)));
    let mut second = EventSubscriptions::new(Some(Arc::clone(&interest)));
    first.subscribe("example.shared.v1".into());
    second.subscribe("example.shared.v1".into());
    let waiter = EventWaiter::new(
        &bus,
        application("example"),
        "example.shared.v1".into(),
        Some(&interest),
    );
    first.unsubscribe("example.shared.v1");
    second.unsubscribe("example.shared.v1");
    assert_eq!(watched.borrow().as_ref(), &["example.shared.v1".to_string()]);
    drop(waiter);
    assert!(watched.borrow().is_empty());
}

#[test]
fn one_shot_admission_filters_topics_and_foreign_application_targets() {
    let bus = ProcessEventBus::new();
    let mut waiter = EventWaiter::new(
        &bus,
        application("example"),
        "example.shared.v1".into(),
        None,
    );
    for _ in 0..256 {
        bus.emit(
            ApplicationEvent::new("example.other.v1", EventValue::Null)
                .unwrap(),
        );
        bus.emit(
            ApplicationEvent::for_application(
                application("other"),
                "example.shared.v1",
                EventValue::Null,
            )
            .unwrap(),
        );
    }
    let shared =
        ApplicationEvent::new("example.shared.v1", EventValue::Null).unwrap();
    bus.emit(shared.clone());
    assert_eq!(waiter.receiver.try_recv().unwrap().id(), shared.id());
    let targeted = ApplicationEvent::for_application(
        application("example"),
        "example.shared.v1",
        EventValue::Null,
    )
    .unwrap();
    bus.emit(targeted.clone());
    assert_eq!(waiter.receiver.try_recv().unwrap().id(), targeted.id());
    assert!(matches!(
        waiter.receiver.try_recv(),
        Err(tokio::sync::broadcast::error::TryRecvError::Empty)
    ));
}

#[test]
fn javascript_locks_have_their_own_namespace() {
    let key = LockKey::new::<JavaScriptLock>("demo");
    assert_eq!(key.namespace(), JavaScriptLock::NAMESPACE.as_bytes());
    assert_eq!(key.label(), b"demo");
}

#[test]
fn shared_login_snapshot_arrives_as_json_without_auth_specific_transport() {
    let bus = ProcessEventBus::new();
    let interest = Arc::new(LinkInterest::default());
    let watched = interest.watch();
    let mut waiter = EventWaiter::new(
        &bus,
        application("example"),
        "claviron.auth.event.v1".into(),
        Some(&interest),
    );
    assert_eq!(
        watched.borrow().as_ref(),
        &["claviron.auth.event.v1".to_string()]
    );
    let payload = serde_json::json!({
        "version": 1,
        "kind": "login",
        "subject": "subject-one",
        "guard": "web",
        "remember": false,
        "username": "example-user",
        "name": "Example User",
        "occurred_at": "2026-01-01T00:00:00Z",
        "ip": "192.0.2.1",
        "identity": "01234567-89ab-4cde-8fab-0123456789ab"
    });
    bus.emit(
        ApplicationEvent::new(
            "claviron.auth.event.v1",
            EventValue::from_json(&payload),
        )
        .unwrap(),
    );
    let event = waiter.receiver.try_recv().unwrap();
    let json: serde_json::Value =
        serde_json::from_str(&application_event_json(&event).unwrap()).unwrap();
    assert_eq!(json["topic"], "claviron.auth.event.v1");
    assert_eq!(json["payload"], payload);
    drop(waiter);
    assert!(watched.borrow().is_empty());
}

#[test]
fn application_event_integers_never_lose_precision_at_the_javascript_boundary()
{
    let safe = serde_json::json!(MAX_SAFE_JAVASCRIPT_INTEGER);
    assert_eq!(
        json_to_event_value(safe).unwrap(),
        EventValue::Int(MAX_SAFE_JAVASCRIPT_INTEGER)
    );
    assert!(
        json_to_event_value(serde_json::json!(MAX_SAFE_JAVASCRIPT_INTEGER + 1))
            .is_err()
    );
    assert!(
        event_value_to_json(&EventValue::Int(MAX_SAFE_JAVASCRIPT_INTEGER + 1))
            .is_err()
    );
}
