use super::*;
use claviron_types::Host;
use claviron::http::config::transport::TransportPolicy;
use claviron::http::ext::PathPrefix;
use claviron::http::policy::HttpPolicy;
use claviron::pipeline::Site;
use claviron::pipeline::factory::RouteProtocol;

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
    // Preparation resolves declarations without starting a V8 pool.
    state.registry_ref().insert(service).unwrap();
    state
}

#[test]
fn registration_installs_one_backend_factory_in_a_site_registry() {
    let site = claviron::pipeline::SiteRegistry::default();
    register(&site).unwrap();
    let error = register(&site).unwrap_err().to_string();
    assert!(error.contains("backend 'js' is already registered"));
}

fn prepare(
    state: &AppState,
    host: Option<&Host>,
    transport: &TransportPolicy,
    protocol: RouteProtocol,
    value: Option<&str>,
) -> Result<Vec<Arc<dyn BackendHandler>>> {
    let mut site = Site::new(".");
    if let Some(value) = value {
        site.extensions.insert("js".into(), source(value));
    }
    JsBackendFactory.prepare(&BackendContext {
        state,
        host,
        site: &site,
        route: &site,
        prefix: &prefix("/api"),
        policy: HttpPolicy::unlimited(),
        transport,
        protocol: Some(protocol),
        listener: None,
    })
}

fn compile(
    state: &AppState,
    transport: &str,
    value: &str,
) -> Result<Vec<Arc<dyn BackendHandler>>> {
    prepare(
        state,
        Some(&"example.test".parse().unwrap()),
        &serde_yaml::from_str(transport).unwrap(),
        RouteProtocol::Http,
        Some(value),
    )
}

#[test]
fn compiles_a_declared_application_with_backend_identity_and_no_engine_start() {
    let state = configured_state();
    let owners = compile(&state, "{}", "application: example").unwrap();
    assert_eq!(owners.len(), 1);
    assert_eq!(owners[0].contract().identity, BackendIdentity::Request);
    assert!(matches!(owners[0].contract().reply, BackendReply::Owner));
    assert_eq!(JsBackendFactory.placement(), BackendPlacement::AfterFiles);
}

#[test]
fn absent_or_null_directive_needs_no_service_and_creates_no_owner() {
    let state = AppState::standalone("js-route-test", ".");
    for value in [None, Some("null")] {
        assert!(
            prepare(
                &state,
                None,
                &TransportPolicy::default(),
                RouteProtocol::Http,
                value
            )
            .unwrap()
            .is_empty()
        );
    }
}

#[test]
fn fetch_backend_does_not_require_a_virtual_host() {
    let owners = prepare(
        &configured_state(),
        None,
        &TransportPolicy::default(),
        RouteProtocol::Http,
        Some("application: example"),
    )
    .unwrap();
    assert_eq!(owners.len(), 1);
}

#[test]
fn rejects_unknown_applications_with_host_and_route_context() {
    let error = compile(&configured_state(), "{}", "application: missing")
        .err()
        .unwrap()
        .to_string();
    assert!(error.contains("example.test"));
    assert!(error.contains("/api"));
    assert!(error.contains("application 'missing' is absent"));
}

#[test]
fn rejects_missing_service_during_preparation() {
    let state = AppState::standalone("js-route-test", ".");
    let error = compile(&state, "{}", "application: example")
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
fn websocket_preparation_uses_effective_transport_and_rejects_duplicates() {
    let state = configured_state();
    let declaration = "application: example\nwebsocket: { path: /socket }";
    let error = compile(&state, "{}", declaration).err().unwrap().to_string();
    assert!(error.contains("transport.h1.upgrade"));

    for open in ["h1: { upgrade: true }", "h2: { connect_protocol: true }"] {
        let owners = compile(&state, open, declaration).unwrap();
        assert_eq!(owners.len(), 2);
        let duplicates = "application: example\nwebsockets: [{ path: /socket }, { path: /socket/ }]";
        let error =
            compile(&state, open, duplicates).err().unwrap().to_string();
        assert!(error.contains("duplicate H1 upgrade binding"));
        assert!(compile(&state, open, declaration).is_ok());
    }
}

#[cfg(feature = "websocket")]
#[test]
fn quic_prepares_fetch_without_tcp_websocket_owners() {
    let owners = prepare(
        &configured_state(),
        None,
        &TransportPolicy::default(),
        RouteProtocol::Quic,
        Some("application: example\nwebsocket: { path: /socket }"),
    )
    .unwrap();
    assert_eq!(owners.len(), 1);
}

#[cfg(feature = "websocket")]
#[test]
fn socket_miss_returns_the_same_unread_request_synchronously() {
    struct Unread;
    impl http_body::Body for Unread {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<
            Option<
                std::result::Result<http_body::Frame<Self::Data>, Self::Error>,
            >,
        > {
            panic!("socket selection must not read the body")
        }
    }
    let state = configured_state();
    let owners = compile(
        &state,
        "h1: { upgrade: true }",
        "application: example\nwebsocket: { path: /socket }",
    )
    .unwrap();
    let body = claviron::http::boxed_body(Unread);
    let mut request = Request::builder()
        .method("POST")
        .uri("/api/fetch?q=1")
        .body(body)
        .unwrap();
    let marker = Arc::new(());
    request.extensions_mut().insert(Arc::clone(&marker));
    let DispatchCall::Ready(ServiceResult::Unhandled(request)) =
        owners[0].call(request, None)
    else {
        panic!("a socket miss must return the request inline");
    };
    assert_eq!(request.uri(), "/api/fetch?q=1");
    assert_eq!(request.method(), "POST");
    assert!(Arc::ptr_eq(
        request.extensions().get::<Arc<()>>().unwrap(),
        &marker
    ));
}

#[cfg(not(feature = "websocket"))]
#[test]
fn websocket_declarations_require_support() {
    let error = compile(
        &configured_state(),
        "{}",
        "application: example\nwebsocket: { path: /socket }",
    )
    .err()
    .unwrap()
    .to_string();
    assert!(error.contains("websocket feature"));
}
