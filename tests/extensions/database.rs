use super::{
    DatabaseAccessMode, DatabaseBackend, DatabaseIsolationLevel,
    DatabaseParameter, DatabaseQuery, DatabaseTransactionSettings,
    parse_transaction_id,
};

#[test]
fn decodes_the_backend_neutral_query_contract() {
    let query: DatabaseQuery = serde_json::from_value(serde_json::json!({
        "backend": "postgres",
        "connection": "default",
        "sql": "select $1::int4",
        "parameters": [{ "type": "i32", "value": 42 }],
        "transaction": null
    }))
    .unwrap();

    assert_eq!(query.backend, DatabaseBackend::Postgres);
    assert_eq!(query.parameters, vec![DatabaseParameter::I32(42)]);
}

#[test]
fn decodes_kysely_transaction_settings() {
    let settings: DatabaseTransactionSettings =
        serde_json::from_value(serde_json::json!({
            "isolationLevel": "repeatable read",
            "accessMode": "read only"
        }))
        .unwrap();

    assert_eq!(
        settings.isolation_level,
        Some(DatabaseIsolationLevel::RepeatableRead)
    );
    assert_eq!(settings.access_mode, Some(DatabaseAccessMode::ReadOnly));
}

#[test]
fn accepts_only_local_opaque_transaction_handles() {
    assert_eq!(parse_transaction_id("tx-42").unwrap(), 42);
    assert!(parse_transaction_id("42").is_err());
    assert!(parse_transaction_id("tx-other").is_err());
}
