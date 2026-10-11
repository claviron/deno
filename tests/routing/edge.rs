use super::*;
use claviron::edge::Host;
use claviron::http::ext::PathPrefix;

fn prefix(value: &str) -> PathPrefix {
    serde_yaml::from_value(serde_yaml::Value::String(value.into())).unwrap()
}

fn source(value: &str) -> serde_yaml::Value {
    serde_yaml::from_str(value).unwrap()
}

fn configured_state() -> AppState {
    let state = AppState::standalone("js-route-test", ".");
    let service = Arc::new(JsApplicationService::new());
    service
        .register_application(
            crate::ApplicationDefinition::new("example", "/app.js").unwrap(),
        )
        .unwrap();
    // DI only: compilation needs declarations, not an executing V8 pool.
    state.registry_ref().insert(service).unwrap();
    state
}

#[test]
fn registration_uses_the_existing_edge_registry_and_preserves_plain_axum_consumers()
 {
    let state = AppState::standalone("js-route-test", ".");
    register(&state).unwrap();
    state.registry_ref().insert(Arc::new(HostAppRegistry::default())).unwrap();
    register(&state).unwrap();
    // The registry's existing duplicate guard proves that the `js` codec was
    // installed; registration neither boots a host nor starts an application.
    let error = register(&state).unwrap_err().to_string();
    assert!(error.contains("route extension 'js' is already registered"));
}

fn resolve(
    state: &AppState,
    host: &HostConfig,
    value: &str,
) -> Result<RouteEffect> {
    JsRouteExtension.resolve(
        &RouteContext {
            state,
            host: &"example.test".parse::<Host>().unwrap(),
            config: host,
            route: host,
            prefix: &prefix("/api"),
        },
        &source(value),
    )
}

#[test]
fn compiles_a_declared_application_without_starting_the_engine() {
    let state = configured_state();
    let host: HostConfig = serde_yaml::from_str("{}").unwrap();
    assert!(matches!(
        resolve(&state, &host, "application: example").unwrap(),
        RouteEffect::Handler(_)
    ));
}

#[test]
fn rejects_unknown_applications_during_compilation_with_host_and_route_context()
{
    let state = configured_state();
    let host: HostConfig = serde_yaml::from_str("{}").unwrap();
    let error = resolve(&state, &host, "application: missing")
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("example.test"));
    assert!(error.contains("/api"));
    assert!(error.contains("application 'missing' is absent"));
}

#[test]
fn rejects_missing_service_during_compilation() {
    let state = AppState::standalone("js-route-test", ".");
    let host: HostConfig = serde_yaml::from_str("{}").unwrap();
    let error = resolve(&state, &host, "application: example")
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("JavaScript service is not registered"));
}

#[test]
fn rejects_ambiguous_mounts_empty_names_and_unknown_fields() {
    for value in [
        "application: example\npath: /other",
        "application: example\npath: null",
        "application: ''",
        "application: example\napplicaton: typo",
    ] {
        assert!(parse(&source(value)).is_err(), "{value}");
    }
}

#[cfg(feature = "websocket")]
#[test]
fn websocket_paths_are_relative_to_the_selected_mount() {
    for (mount, socket, expected) in [
        ("/", "/socket", "/socket"),
        ("/api/", "/socket/", "/api/socket"),
        ("/api", "/", "/api"),
    ] {
        assert_eq!(
            mounted_path(&prefix(mount), &prefix(socket)).as_ref(),
            expected
        );
    }
}

#[cfg(feature = "websocket")]
#[test]
fn websocket_compilation_requires_the_transport_gate_and_rejects_duplicates() {
    let state = configured_state();
    let closed: HostConfig = serde_yaml::from_str("{}").unwrap();
    let declaration = "application: example\nwebsocket: { path: /socket }";
    let error =
        resolve(&state, &closed, declaration).err().unwrap().to_string();
    assert!(error.contains("transport.h1.upgrade"));

    let open: HostConfig =
        serde_yaml::from_str("transport: { h1: { upgrade: true } }").unwrap();
    assert!(matches!(
        resolve(&state, &open, declaration).unwrap(),
        RouteEffect::Handler(_)
    ));
    let duplicates = "application: example\nwebsockets: [{ path: /socket }, { path: /socket/ }]";
    let error = resolve(&state, &open, duplicates).err().unwrap().to_string();
    assert!(error.contains("duplicate H1 upgrade binding"));
    // Each candidate owns fresh bindings; re-preparing the same declaration
    // never collides with the previous snapshot or a failed candidate.
    assert!(resolve(&state, &open, declaration).is_ok());
}

#[cfg(not(feature = "websocket"))]
#[test]
fn websocket_declarations_are_not_silently_ignored_without_support() {
    let state = configured_state();
    let host: HostConfig = serde_yaml::from_str("{}").unwrap();
    let error = resolve(
        &state,
        &host,
        "application: example\nwebsocket: { path: /socket }",
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("websocket feature"));
}
