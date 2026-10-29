use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use deno_core::{Extension, OpState, op2};
use tempfile::tempdir;

use super::CoreEngine;
use crate::ExtensionContext;
use crate::backend::deno::{
    ExtensionFactory, ExtensionModule, SharedExtensionFactory,
};
struct TestValue(String);

#[op2]
#[string]
fn op_claviron_test_value(state: &OpState) -> String {
    state.borrow::<TestValue>().0.clone()
}

deno_core::extension!(
    claviron_test_extension,
    ops = [op_claviron_test_value],
    esm_entry_point = "ext:claviron_test_extension/extension.js",
    esm = [dir "tests/fixtures", "extension.js"],
    options = {
        value: String,
    },
    state = |state, options| {
        state.put(TestValue(options.value));
    },
);

const TEST_MODULES: &[ExtensionModule] = &[ExtensionModule::new(
    "claviron:test",
    r#"
            const extension = globalThis[Symbol.for("claviron.test")];
            export const value = extension.value;
        "#,
)];

struct TestExtensionFactory;

impl ExtensionFactory for TestExtensionFactory {
    fn extension(
        &self,
        context: ExtensionContext,
    ) -> Extension {
        claviron_test_extension::init(context.application_name().to_owned())
    }

    fn modules(&self) -> &'static [ExtensionModule] {
        TEST_MODULES
    }
}

#[tokio::test(flavor = "current_thread")]
async fn boots_module_with_claviron_runtime_extension() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(
        &main_module,
        r#"
                import {
                    applicationName,
                    environment,
                    runtimeAvailable,
                    yieldToRuntime,
                } from "claviron:runtime";

                if (!runtimeAvailable()) {
                    throw new Error("Claviron runtime is unavailable");
                }

                if (applicationName() !== "test-application") {
                    throw new Error(`unexpected application: ${applicationName()}`);
                }

                if (environment("APP_ENV") !== "test" || environment("MISSING") !== null) {
                    throw new Error("application environment is unavailable");
                }

                if (await yieldToRuntime("ready") !== "ready") {
                    throw new Error("async Claviron op did not resolve");
                }

                export default {
                    fetch() {
                        return new Response("ready");
                    },
                };

            "#
    )
    .unwrap();

    let environment =
        Arc::new(BTreeMap::from([("APP_ENV".to_owned(), "test".to_owned())]));
    let context =
        ExtensionContext::new("test-application").with_environment(environment);
    let mut engine = CoreEngine::with_context(context, &[]).unwrap();
    engine.boot(&main_module).await.unwrap();
}

/// A body crosses into the isolate and back as bytes, not as JSON: every
/// byte value survives, including the ones a text path would mangle.
#[tokio::test(flavor = "current_thread")]
async fn carries_request_and_reply_bodies_as_bytes() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(
        &main_module,
        r#"
                export default {
                    async fetch(request) {
                        const body = new Uint8Array(await request.arrayBuffer());
                        return new Response(body, {
                            status: 201,
                            headers: { "x-length": String(body.length) },
                        });
                    },
                };
            "#
    )
    .unwrap();

    let mut engine = CoreEngine::with_context(
        ExtensionContext::new("test-application"),
        &[],
    )
    .unwrap();
    engine.boot(&main_module).await.unwrap();
    let body: Vec<u8> = (0..=255).chain([0, 255, 0]).collect();
    let (reply, replied) = tokio::sync::oneshot::channel();
    // The body flows only while the handler runs, so it is read alongside.
    let (handled, received) = tokio::join!(
        engine.handle(
            http::Request::builder()
                .method("POST")
                .uri("http://example.test/echo")
                .header("content-type", "application/octet-stream")
                .body(crate::RequestBody::Whole(body.clone()))
                .unwrap(),
            reply
        ),
        async {
            let mut response = replied.await.unwrap().unwrap();
            let mut received = Vec::new();
            while let Some(chunk) = response.body_mut().recv().await {
                received.extend_from_slice(&chunk.unwrap());
            }
            let (parts, _) = response.into_parts();
            (parts.status, parts.headers, received)
        }
    );
    handled.unwrap();
    let (status, headers, received) = received;
    assert_eq!(status, 201);
    assert_eq!(headers["x-length"], body.len().to_string());
    assert_eq!(received, body);
}

#[tokio::test(flavor = "current_thread")]
async fn typed_http_metadata_survives_without_json_for_whole_and_streamed_replies()
 {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(&main_module, r#"
        JSON.parse = JSON.stringify = () => { throw new Error("JSON bridge used"); };
        export default async function (request) {
            if (request.method !== "PATCH" || !request.url.endsWith("?value=%C4%B1") ||
                request.headers.get("x-byte") !== "caf\u00e9" ||
                request.headers.get("x-repeat") !== "one, two") {
                throw new Error("request metadata changed");
            }
            const bytes = new Uint8Array(await request.arrayBuffer());
            const body = request.url.includes("/stream?")
                ? new ReadableStream({start(controller) { controller.enqueue(bytes); controller.close(); }})
                : bytes;
            const response = new Response(body, {status: 207, headers: [
                ["x-byte", "caf\u00e9"], ["x-repeat", "one"], ["x-repeat", "two"],
                ["set-cookie", "a=1"], ["set-cookie", "b=2"]
            ]});
            return response;
        }
    "#).unwrap();
    let mut engine =
        CoreEngine::with_context(ExtensionContext::new("metadata"), &[])
            .unwrap();
    engine.boot(&main_module).await.unwrap();
    for path in ["whole", "stream"] {
        let mut request = http::Request::builder()
            .method("PATCH")
            .uri(format!("http://example.test/{path}?value=%C4%B1"))
            .header("x-repeat", "one")
            .header("x-repeat", "two")
            .body(crate::RequestBody::Whole(vec![0, 128, 255]))
            .unwrap();
        request.headers_mut().insert(
            "x-byte",
            http::HeaderValue::from_bytes(b"caf\xe9").unwrap(),
        );
        let (reply, replied) = tokio::sync::oneshot::channel();
        let (handled, response) =
            tokio::join!(engine.handle(request, reply), async {
                let mut response = replied.await.unwrap().unwrap();
                assert_eq!(response.status(), 207);
                assert_eq!(response.headers()["x-byte"].as_bytes(), b"caf\xe9");
                assert_eq!(
                    response
                        .headers()
                        .get_all("x-repeat")
                        .iter()
                        .map(|value| value.to_str().unwrap())
                        .collect::<Vec<_>>()
                        .join(", "),
                    "one, two"
                );
                #[cfg(feature = "fetch")]
                assert_eq!(
                    response
                        .headers()
                        .get_all("set-cookie")
                        .iter()
                        .map(|value| value.as_bytes())
                        .collect::<Vec<_>>(),
                    [b"a=1".as_slice(), b"b=2".as_slice()]
                );
                let mut body = Vec::new();
                while let Some(chunk) = response.body_mut().recv().await {
                    body.extend_from_slice(&chunk.unwrap());
                }
                body
            });
        handled.unwrap();
        assert_eq!(response, [0, 128, 255]);
    }
}

/// A streamed request body reaches the handler as its `ReadableStream`,
/// piece by piece; a failed read fails that stream instead of ending it as if
/// the body were whole.
#[tokio::test(flavor = "current_thread")]
async fn streams_a_request_body_and_fails_a_cut_one() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(
        &main_module,
        r#"
                export default {
                    async fetch(request) {
                        try {
                            const body = new Uint8Array(await request.arrayBuffer());
                            return new Response(body);
                        } catch (error) {
                            return new Response(String(error), { status: 499 });
                        }
                    },
                };
            "#
    )
    .unwrap();
    let mut engine = CoreEngine::with_context(
        ExtensionContext::new("test-application"),
        &[],
    )
    .unwrap();
    engine.boot(&main_module).await.unwrap();

    for cut in [false, true] {
        let (pieces, receiver) = tokio::sync::mpsc::channel(1);
        let (reply, replied) = tokio::sync::oneshot::channel();
        let request = http::Request::builder()
            .method("POST")
            .uri("http://example.test/echo")
            .body(crate::RequestBody::Stream(receiver))
            .unwrap();
        let (handled, _, (status, received)) = tokio::join!(
            engine.handle(request, reply),
            async move {
                pieces
                    .send(Ok(bytes::Bytes::from_static(b"one ")))
                    .await
                    .unwrap();
                pieces
                    .send(Ok(bytes::Bytes::from_static(b"two ")))
                    .await
                    .unwrap();
                tokio::task::yield_now().await;
                let last = if cut {
                    Err("client gone".to_owned())
                } else {
                    Ok(bytes::Bytes::from_static(b"three"))
                };
                pieces.send(last).await.unwrap();
            },
            async {
                let mut response = replied.await.unwrap().unwrap();
                let mut received = Vec::new();
                while let Some(chunk) = response.body_mut().recv().await {
                    received.extend_from_slice(&chunk.unwrap());
                }
                (response.status().as_u16(), received)
            }
        );
        handled.unwrap();
        if cut {
            assert_eq!(status, 499);
        } else {
            assert_eq!(status, 200);
            assert_eq!(received, b"one two three");
        }
    }
}

#[cfg(feature = "fetch")]
#[tokio::test(flavor = "current_thread")]
async fn lazy_fetch_requests_keep_clone_bodies_and_exposed_headers() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(&main_module, r#"
        let calls = 0, ignored, retained;
        export default async function(request) {
            if (!(request instanceof Request)) throw new Error("not a Fetch Request");
            if (calls++ === 0) {
                ignored = request;
                return new Response("ignored");
            }
            if (retained && (retained.get("X-Repeat") !== "one, two" ||
                Array.from(retained).length !== 2)) throw new Error("retained headers lost");
            if (ignored) {
                if (ignored.url !== "http://example.test/echo?q=1" || ignored.method !== "POST" ||
                    ignored.headers.get("x-repeat") !== "one, two" || ignored.bodyUsed) {
                    throw new Error("retained request lost its values");
                }
                if ((await ignored.bytes()).join() !== "0,128,255" || !ignored.bodyUsed) {
                    throw new Error("retained request lost its buffered body");
                }
                if (ignored.signal.aborted) throw new Error("successful request was aborted");
                ignored = null;
            }
            retained = request.headers;
            if (retained.get("X-Repeat") !== "one, two" || retained.get("missing") !== null ||
                retained.get("content-type") !== "application/octet-stream") throw new Error("lazy headers changed");
            const cloned = request.clone();
            cloned.headers.set("x-repeat", "clone");
            if (request.headers.get("x-repeat") !== "one, two") throw new Error("clone shares headers");
            if (request.method !== "POST" || request.url !== "http://example.test/echo?q=1" ||
                cloned.url !== request.url || cloned.signal.aborted || request.bodyUsed) throw new Error("request changed");
            const [one, two] = await Promise.all([request.bytes(), cloned.bytes()]);
            if (!request.bodyUsed || !cloned.bodyUsed || one.join() !== "0,128,255" ||
                two.join() !== one.join()) throw new Error("clone body changed");
            let used = false;
            try { await request.text(); } catch { used = true; }
            if (!used) throw new Error("body consumed twice");
            return new Response(one);
        }
    "#).unwrap();
    let mut engine =
        CoreEngine::with_context(ExtensionContext::new("lazy"), &[]).unwrap();
    engine.boot(&main_module).await.unwrap();
    for index in 0..3 {
        if index > 0 {
            // Retained Request values must root their native data across GC.
            engine.runtime.v8_isolate().low_memory_notification();
        }
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        sender
            .send(Ok(bytes::Bytes::from_static(&[0, 128, 255])))
            .await
            .unwrap();
        drop(sender);
        let body = if index < 2 {
            crate::RequestBody::Whole(vec![0, 128, 255])
        } else {
            crate::RequestBody::Stream(receiver)
        };
        let request = http::Request::builder()
            .method("POST")
            .uri("http://example.test/echo?q=1")
            .header("content-type", "application/octet-stream")
            .header("x-repeat", "one")
            .header("x-repeat", "two")
            .body(body)
            .unwrap();
        let (reply, replied) = tokio::sync::oneshot::channel();
        engine.handle(request, reply).await.unwrap();
        let response = replied.await.unwrap().unwrap();
        let crate::ResponseBody::Whole(body) = response.into_body() else {
            panic!("expected whole response")
        };
        assert_eq!(
            body.as_ref(),
            if index == 0 { b"ignored".as_slice() } else { &[0, 128, 255] }
        );
    }
}

#[cfg(feature = "fetch")]
#[tokio::test(flavor = "current_thread")]
async fn ready_fetch_responses_keep_wire_headers_and_owned_body_bytes() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(&main_module, r#"
        let calls = 0, previous, mutable;
        export default function() {
            if (previous) {
                if (!previous.bodyUsed) throw new Error("body was not consumed");
                let used = false;
                try { previous.clone(); } catch { used = true; }
                if (!used) throw new Error("sent response cloned");
            }
            if (mutable) mutable.fill(7);
            let response;
            switch (calls++) {
                case 0: response = new Response("caf\u00e9 \ud800"); break;
                case 1: response = Response.json({ ok: true }); break;
                case 2:
                    mutable = new Uint8Array([42, 0, 128, 255, 42]);
                    response = new Response(mutable.subarray(1, 4)); break;
                case 3:
                    response = new Response("changed", { status: 201, headers: [
                        ["x-repeat", "one"], ["x-repeat", "two"],
                        ["set-cookie", "a=1"], ["set-cookie", "b=2"]
                    ]});
                    response.headers.set("content-type", "application/example"); break;
                case 4:
                    response = new Response("mutated");
                    response.headers.set("content-type", "application/changed"); break;
                case 5: previous = null; return new Response(null, { status: 204 });
                case 6:
                    response = new Response("locked");
                    response.body.getReader(); return response;
                case 7:
                    response = new Response("consumed");
                    return response.text().then(() => response);
                default: throw new Error("unexpected request");
            }
            previous = response;
            Object.defineProperty(response, "headers", { get() { throw new Error("public headers used"); } });
            return response;
        }
        // The bridge must pass static text directly, including lone-surrogate
        // replacement, rather than allocating another JS byte buffer.
        TextEncoder.prototype.encode = () => { throw new Error("TextEncoder used by bridge"); };
    "#).unwrap();
    let mut engine =
        CoreEngine::with_context(ExtensionContext::new("ready"), &[]).unwrap();
    engine.boot(&main_module).await.unwrap();
    let mut bodies = Vec::new();
    for index in 0..6 {
        let request = http::Request::builder()
            .uri("http://example.test/")
            .body(crate::RequestBody::Empty)
            .unwrap();
        let (reply, replied) = tokio::sync::oneshot::channel();
        engine.handle(request, reply).await.unwrap();
        let response = replied.await.unwrap().unwrap();
        assert_eq!(
            response.status().as_u16(),
            match index {
                3 => 201,
                5 => 204,
                _ => 200,
            }
        );
        let expected_type = match index {
            0 => Some("text/plain;charset=UTF-8"),
            1 => Some("application/json"),
            3 => Some("application/example"),
            4 => Some("application/changed"),
            _ => None,
        };
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .map(|value| value.to_str().unwrap()),
            expected_type
        );
        if index == 3 {
            assert_eq!(
                response.headers().get_all("x-repeat").iter().count(),
                2
            );
            assert_eq!(
                response.headers().get_all("set-cookie").iter().count(),
                2
            );
        }
        let crate::ResponseBody::Whole(body) = response.into_body() else {
            panic!("expected whole response")
        };
        bodies.push(body);
    }
    assert_eq!(bodies[0].as_ref(), "café \u{fffd}".as_bytes());
    assert_eq!(bodies[1].as_ref(), br#"{"ok":true}"#);
    assert_eq!(bodies[2].as_ref(), &[0, 128, 255]);
    assert_eq!(bodies[3].as_ref(), b"changed");
    assert_eq!(bodies[4].as_ref(), b"mutated");
    assert!(bodies[5].is_empty());
    for _ in 0..2 {
        let request = http::Request::builder()
            .uri("http://example.test/")
            .body(crate::RequestBody::Empty)
            .unwrap();
        let (reply, replied) = tokio::sync::oneshot::channel();
        let error = engine.handle(request, reply).await.unwrap_err();
        assert!(error.to_string().contains("Response body is already used"));
        assert!(replied.await.unwrap().is_err());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn accepts_external_extensions_with_public_claviron_modules() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(
        &main_module,
        r#"
                import { value } from "claviron:test";

                if (value() !== "external-application") {
                    throw new Error("external extension context is unavailable");
                }

                export default () => new Response("ready");
            "#
    )
    .unwrap();

    let extensions: Vec<SharedExtensionFactory> =
        vec![Arc::new(TestExtensionFactory)];
    let mut engine =
        CoreEngine::with_extensions("external-application", &extensions)
            .unwrap();
    engine.boot(&main_module).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn exposes_deno_web_platform_globals() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(&main_module, include_str!("../fixtures/engine/web_platform.js"))
        .unwrap();

    let mut engine = CoreEngine::new("web-platform-application");
    engine.boot(&main_module).await.unwrap();
}

#[cfg(feature = "tasks")]
#[tokio::test(flavor = "current_thread")]
async fn routes_task_console_streams_through_task_output() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("task.js");
    let stdout = directory.path().join("worker.log");
    let stderr = directory.path().join("error.log");
    fs::write(&main_module, "console.log('worker'); console.error('error');")
        .unwrap();
    let output = claviron::tasks::LogConfig::Split {
        stdout: claviron::tasks::LogTarget::Path(stdout.clone()),
        stderr: claviron::tasks::LogTarget::Path(stderr.clone()),
    }
    .open(None, "console-task");
    let context =
        ExtensionContext::new("console-task").with_task_output(output);
    let mut engine = CoreEngine::with_context(context, &[]).unwrap();

    engine.boot_program(&main_module).await.unwrap();

    assert_eq!(fs::read_to_string(stdout).unwrap(), "worker\n");
    assert_eq!(fs::read_to_string(stderr).unwrap(), "error\n");
}
