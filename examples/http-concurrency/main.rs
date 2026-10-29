//! Explicit, process-local acceptance harness; no listeners or fleet resources.
use std::sync::Arc;
use std::time::Instant;

use claviron::app::management::ApplicationStarted;
use claviron::http::{RouteBody, empty_body};
use claviron::runtime::registry::Runnable;
use claviron::runtime::{AppState, RunContext};
use claviron_deno::{
    ApplicationDefinition, CoreApplication, JsApplicationAdapter,
    JsApplicationService,
};
use deno_core::futures::{StreamExt, stream::FuturesUnordered};
use http::Request;
use http_body_util::BodyExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn request(path: &str) -> Request<RouteBody> {
    Request::builder()
        .uri(format!("http://example.test{path}"))
        .body(empty_body())
        .unwrap()
}

struct Upload(mpsc::Receiver<bytes::Bytes>);
impl http_body::Body for Upload {
    type Data = bytes::Bytes;
    type Error = std::convert::Infallible;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Option<Result<http_body::Frame<Self::Data>, Self::Error>>,
    > {
        self.0
            .poll_recv(cx)
            .map(|next| next.map(|bytes| Ok(http_body::Frame::data(bytes))))
    }
}

async fn get(
    adapter: &JsApplicationAdapter,
    path: &str,
) -> bytes::Bytes {
    let response = adapter.handle(request(path)).await;
    assert_eq!(response.status(), 200);
    response.into_body().collect().await.unwrap().to_bytes()
}

async fn completion_errors(module: &std::path::Path) {
    let definition = ApplicationDefinition::new("completion", module).unwrap();
    let mut application = CoreApplication::boot(definition).await.unwrap();
    for (path, message) in [
        ("/throw", "synchronous failure"),
        ("/reject", "rejected failure"),
        ("/async-reject", "asynchronous failure"),
        ("/throw-undefined", "undefined"),
        ("/reject-null", "null"),
        ("/invalid-response", "fetch handler must return a Response"),
        ("/reentrant-error", "reentrant failure"),
    ] {
        let request = Request::builder()
            .uri(format!("http://example.test{path}"))
            .body(claviron_deno::RequestBody::Empty)
            .unwrap();
        let (reply, replied) = tokio::sync::oneshot::channel();
        let error =
            application.handle(request, reply).await.unwrap_err().to_string();
        assert!(error.contains(message), "{error}");
        if matches!(path, "/throw-undefined" | "/reject-null") {
            assert!(error.contains("Uncaught (in promise)"), "{error}");
        } else {
            assert!(error.contains("\n    at "), "{error}");
        }
        assert!(replied.await.unwrap().unwrap_err().contains(message));

        let request = Request::builder()
            .uri("http://example.test/json")
            .body(claviron_deno::RequestBody::Empty)
            .unwrap();
        let (reply, replied) = tokio::sync::oneshot::channel();
        application.handle(request, reply).await.unwrap();
        let mut response = replied.await.unwrap().unwrap();
        assert_eq!(
            response.body_mut().recv().await.unwrap().unwrap(),
            "{\"ok\":true}"
        );
    }
    println!(
        "synchronous throws, asynchronous rejections and reentrant error formatting preserved request-local failures"
    );
}

#[tokio::main]
async fn main() {
    let args = std::env::args().collect::<Vec<_>>();
    let state = AppState::standalone("http-concurrency-example", ".");
    let mut started = state.events().subscribe::<ApplicationStarted>();
    let service = Arc::new(JsApplicationService::new());
    let module = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("examples/http-concurrency/application.js");
    service
        .register_application(
            ApplicationDefinition::new("example", module.clone()).unwrap(),
        )
        .unwrap();
    let cancellation = CancellationToken::new();
    let running = tokio::spawn(Arc::clone(&service).run(
        (),
        state,
        RunContext::new(cancellation.clone()),
    ));
    started.recv().await.unwrap();
    let adapter = JsApplicationAdapter::new(service, "example");
    let mut draining = None;
    let mut draining_request = None;
    if args.get(1).is_some_and(|arg| arg == "--benchmark") {
        let count: usize = args[2].parse().unwrap();
        let concurrency: usize = args[3].parse().unwrap();
        assert!(count > 0 && concurrency > 0);
        for _ in 0..concurrency {
            assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
        }
        let start = Instant::now();
        let mut calls = FuturesUnordered::new();
        for lane in 0..concurrency {
            let adapter = &adapter;
            calls.push(async move {
                for _ in (lane..count).step_by(concurrency) {
                    assert_eq!(get(adapter, "/json").await, "{\"ok\":true}");
                }
            });
        }
        while calls.next().await.is_some() {}
        println!(
            "requests={count} concurrency={concurrency} elapsed_us={} us_per_request={:.3}",
            start.elapsed().as_micros(),
            start.elapsed().as_secs_f64() * 1e6 / count as f64
        );
    } else {
        // Direct embedded Deno ops require an owner-thread Tokio runtime.
        tokio::task::spawn_blocking(move || {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(completion_errors(&module));
        })
        .await
        .unwrap();
        for path in ["/throw", "/reject", "/async-reject", "/invalid-response"]
        {
            assert_eq!(adapter.handle(request(path)).await.status(), 500);
            assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
        }
        for content_type in ["application/octet-stream", "text/event-stream"] {
            let mut opening = request("/stream");
            opening
                .headers_mut()
                .insert("x-content-type", content_type.parse().unwrap());
            let response = adapter.handle(opening).await;
            assert_eq!(response.status(), 200);
            let mut body = response.into_body();
            assert_eq!(
                body.frame().await.unwrap().unwrap().into_data().unwrap(),
                "open"
            );
            assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
            assert_eq!(get(&adapter, "/release").await, "released");
            assert_eq!(body.collect().await.unwrap().to_bytes(), "closed");
        }
        println!(
            "one isolate: ordinary HTTP completed alongside open generic and SSE streams"
        );

        let mut uploads = Vec::new();
        for id in ["left", "right"] {
            let (sender, receiver) = mpsc::channel(1);
            let request = Request::builder()
                .method("POST")
                .uri("http://example.test/echo")
                .header("x-request", id)
                .body(claviron::http::boxed_body(Upload(receiver)))
                .unwrap();
            let adapter = adapter.clone();
            let response =
                tokio::spawn(async move { adapter.handle(request).await });
            sender
                .send(bytes::Bytes::copy_from_slice(id.as_bytes()))
                .await
                .unwrap();
            uploads.push((sender, response));
        }
        assert_eq!(get(&adapter, "/uploads-ready").await, "ready");
        assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
        for id in ["right", "left"] {
            let (sender, response) = uploads.pop().unwrap();
            sender.send(bytes::Bytes::from_static(b"-end")).await.unwrap();
            drop(sender);
            let response = response.await.unwrap();
            assert_eq!(response.headers()["x-request"], id);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                format!("{id}-end")
            );
        }
        println!(
            "concurrent uploads retained their own bytes and headers and completed out of order"
        );

        let mut flood = adapter.handle(request("/flood")).await.into_body();
        flood.frame().await.unwrap().unwrap();
        assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
        drop(flood);
        assert_eq!(get(&adapter, "/cancelled").await, "cancelled");
        println!(
            "backpressure and client disconnect affected only their own producer"
        );

        let waiting = {
            let adapter = adapter.clone();
            tokio::spawn(
                async move { adapter.handle(request("/waiting")).await },
            )
        };
        assert_eq!(get(&adapter, "/waiting-started").await, "started");
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        assert_eq!(get(&adapter, "/waiting-ended").await, "ended");
        assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
        println!(
            "dropping a caller before its response head delivered only its own AbortSignal"
        );

        let mut failing =
            adapter.handle(request("/failing-stream")).await.into_body();
        assert_eq!(
            failing.frame().await.unwrap().unwrap().into_data().unwrap(),
            "prefix"
        );
        assert_eq!(get(&adapter, "/fail").await, "failed");
        assert!(failing.collect().await.is_err());
        assert_eq!(get(&adapter, "/json").await, "{\"ok\":true}");
        println!(
            "a failed stream remained an error without stopping the isolate"
        );
        let pending = {
            let adapter = adapter.clone();
            tokio::spawn(async move { adapter.handle(request("/drain")).await })
        };
        assert_eq!(get(&adapter, "/drain-started").await, "started");
        draining_request = Some(pending);
        let mut body = adapter.handle(request("/stream")).await.into_body();
        body.frame().await.unwrap().unwrap();
        draining = Some(body);
    }
    cancellation.cancel();
    if let Some(body) = draining {
        assert_eq!(body.collect().await.unwrap().to_bytes(), "closed");
        println!("application shutdown drained its accepted response producer");
    }
    if let Some(response) = draining_request {
        let response = response.await.unwrap();
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "drained"
        );
    }
    running.await.unwrap().unwrap();
}
