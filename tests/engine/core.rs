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
            const entries = Array.from(response.headers);
            const name = entries[0][0];
            Object.defineProperty(entries[0], 0, {get() {
                if (!globalThis[Symbol.for("claviron.runtime")].runtimeAvailable()) throw new Error("reentry failed");
                return name;
            }});
            response.headers[Symbol.iterator] = () => entries.values();
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
                assert_eq!(response.headers()["x-repeat"], "one, two");
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
