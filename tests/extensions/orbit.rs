use claviron::app::EventValue;

use super::{
    JavaScriptLock, LockKey, LockType, MAX_SAFE_JAVASCRIPT_INTEGER,
    event_value_to_json, json_to_event_value,
};

#[test]
fn javascript_locks_have_their_own_namespace() {
    let key = LockKey::new::<JavaScriptLock>("demo");
    assert_eq!(key.namespace(), JavaScriptLock::NAMESPACE.as_bytes());
    assert_eq!(key.label(), b"demo");
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
