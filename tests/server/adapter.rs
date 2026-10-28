use claviron::http::empty_body;

use super::*;

#[test]
fn fetch_url_preserves_authority_path_and_query_with_the_accepted_scheme() {
    use http::uri::Scheme;
    for authority in ["app.test:8080", "[::1]:8443"] {
        for scheme in [Scheme::HTTP, Scheme::HTTPS] {
            let mut parts = Request::builder()
                .uri("/app/a%2Fb?q=%2F")
                .header("host", authority)
                .header("x-forwarded-proto", "spoofed")
                .body(())
                .unwrap()
                .into_parts()
                .0;
            parts.extensions.insert(scheme.clone());
            assert_eq!(
                application_url(&parts).unwrap(),
                format!("{scheme}://{authority}/app/a%2Fb?q=%2F")
            );
            assert_eq!(parts.uri, "/app/a%2Fb?q=%2F");
        }
    }
    let mut parts = Request::builder()
        .uri("https://app.test:8080/x?y=1")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    parts.extensions.insert(Scheme::HTTP);
    assert_eq!(application_url(&parts).unwrap(), "http://app.test:8080/x?y=1");
    assert_eq!(parts.uri, "https://app.test:8080/x?y=1");
}

#[test]
fn missing_context_does_not_invent_https_or_localhost() {
    let mut parts = Request::builder()
        .uri("/")
        .header("host", "app.test")
        .body(())
        .unwrap()
        .into_parts()
        .0;
    assert!(application_url(&parts).is_err());
    parts.extensions.insert(http::uri::Scheme::HTTP);
    parts.headers.remove(http::header::HOST);
    assert!(application_url(&parts).is_err());
}

#[tokio::test]
async fn stopped_application_returns_service_unavailable() {
    let service = Arc::new(JsApplicationService::new());
    let adapter = JsApplicationAdapter::new(service, "stopped");
    let request =
        Request::builder().uri("http://app.test/").body(empty_body()).unwrap();

    let response = adapter.handle(request).await;

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        response.headers().get(RETRY_AFTER),
        Some(&HeaderValue::from_static("1"))
    );
}

#[test]
fn application_execution_errors_remain_internal_server_errors() {
    let error = Error::ApplicationUnavailable("handler failed".to_owned());

    assert_eq!(status_for_error(&error), StatusCode::INTERNAL_SERVER_ERROR);
}

/// GET and HEAD must not enter request-body inspection, even for metadata.
#[tokio::test]
async fn bodyless_fetch_methods_never_inspect_the_request_body() {
    struct Uninspected;
    impl http_body::Body for Uninspected {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;
        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<
            Option<Result<http_body::Frame<Self::Data>, Self::Error>>,
        > {
            panic!("bodyless request must not be read")
        }
        fn is_end_stream(&self) -> bool {
            panic!("bodyless request must not be inspected")
        }
        fn size_hint(&self) -> http_body::SizeHint {
            panic!("bodyless request must not be sized")
        }
    }
    let adapter = JsApplicationAdapter::new(
        Arc::new(JsApplicationService::new()),
        "stopped",
    );
    for method in [http::Method::GET, http::Method::HEAD] {
        let request = Request::builder()
            .method(method)
            .uri("http://example.test/")
            .body(claviron::http::boxed_body(Uninspected))
            .unwrap();
        assert_eq!(
            adapter.handle(request).await.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
