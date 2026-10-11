use std::fs;
use std::sync::Arc;

use tempfile::tempdir;

use super::LogExtension;
use crate::backend::deno::SharedExtensionFactory;
use crate::engine::CoreEngine;

/// Without a pipeline every channel is undeclared, and saying so is the
/// channel's and the sink's job; a record still crosses the op boundary in
/// the shape the op reads, and is refused rather than dropped silently.
#[tokio::test(flavor = "current_thread")]
async fn undeclared_channels_throw_and_records_cross_the_op_boundary() {
    let directory = tempdir().unwrap();
    let main_module = directory.path().join("application.js");
    fs::write(
        &main_module,
        r#"
            import { channel } from "claviron:log";
            import { getClavironSink } from "claviron:log/logtape";

            for (const open of [() => channel("app"), () => getClavironSink("app")]) {
                let message = null;
                try { open(); } catch (error) { message = error.message; }
                if (message !== 'Claviron log channel "app" is not declared in logging.yaml') {
                    throw new Error(`unexpected: ${message}`);
                }
            }

            const log = globalThis[Symbol.for("claviron.log")];
            const written = log.write(0, "warning", new Error("failed"), {
                order: 7,
                ratio: 1.5,
                big: 2 ** 60,
                paid: false,
                customer: { id: 3, note: null },
                items: [1, 2],
                cause: new TypeError("bad"),
            }, "billing");
            if (written !== false) {
                throw new Error("a record was accepted without a pipeline");
            }

            export default () => new Response("ready");
        "#
    )
    .unwrap();

    let extensions: Vec<SharedExtensionFactory> =
        vec![Arc::new(LogExtension::new(None))];
    let mut engine =
        CoreEngine::with_extensions("log-application", &extensions).unwrap();
    engine.boot(&main_module).await.unwrap();
}
