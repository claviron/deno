//! `claviron:log`: JavaScript writing records to a log channel `logging.yaml`
//! declares, and `claviron:log/logtape`, a LogTape sink on one.
//!
//! ```js
//! import { channel } from "claviron:log";
//!
//! const log = channel("app");            // throws if undeclared
//! log.warn("order failed", { order: 7 });
//! ```
//!
//! The record goes into this process's lane of the fleet's log ring and the
//! call returns; the fleet's one writer formats and writes it. The target is
//! the application's name unless the channel or the sink names one.

use std::sync::Arc;

use claviron::logging::pipeline::{
    ChannelId, Level, LogChannels, Record, Value,
};
use deno_core::{Extension, OpState, op2};
use serde::Deserialize;

use crate::ExtensionContext;
use crate::backend::deno::{ExtensionFactory, ExtensionModule};

const MODULES: &[ExtensionModule] = &[
    ExtensionModule::new("claviron:log", include_str!("log/modules/log.js")),
    ExtensionModule::new(
        "claviron:log/logtape",
        include_str!("log/modules/logtape.js"),
    ),
];

#[derive(Clone)]
struct LogContext {
    channels: Option<Arc<LogChannels>>,
    target: Arc<str>,
}

#[derive(Debug, Deserialize)]
struct LogRequest {
    channel: u16,
    level: String,
    target: Option<String>,
    message: String,
    fields: Vec<(String, WireValue)>,
}

#[derive(Debug, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum WireValue {
    String(String),
    I64(i64),
    F64(f64),
    Bool(bool),
}

impl WireValue {
    fn borrow(&self) -> Value<'_> {
        match self {
            Self::String(text) => Value::Str(text),
            Self::I64(number) => Value::I64(*number),
            Self::F64(number) => Value::F64(*number),
            Self::Bool(flag) => Value::Bool(*flag),
        }
    }
}

/// Declared channels for each JavaScript isolate; `None` when this process
/// has no log pipeline, and every channel then reads as undeclared.
pub struct LogExtension {
    channels: Option<Arc<LogChannels>>,
}

impl LogExtension {
    pub fn new(channels: Option<Arc<LogChannels>>) -> Self {
        Self { channels }
    }
}

/// The id of a declared channel, or -1: undeclared, or no pipeline.
#[op2(fast)]
fn op_claviron_log_channel(
    state: &OpState,
    #[string] name: &str,
) -> i32 {
    state
        .borrow::<LogContext>()
        .channels
        .as_ref()
        .and_then(|channels| channels.channel(name))
        .map_or(-1, |id| i32::from(id.raw()))
}

/// Publish one record; false when it could not be (no pipeline, a ring
/// write that failed).
#[op2]
fn op_claviron_log(
    state: &OpState,
    #[serde] request: LogRequest,
) -> bool {
    let context = state.borrow::<LogContext>();
    let Some(channels) = context.channels.as_ref() else { return false };
    let (level, lossy) = map_level(&request.level);
    let lowered = request.level.to_ascii_lowercase();
    let mut fields: Vec<(&str, Value<'_>)> = request
        .fields
        .iter()
        .map(|(key, value)| (key.as_str(), value.borrow()))
        .collect();
    if lossy {
        fields.push(("psr_level", Value::Str(&lowered)));
    }
    let record = Record {
        level,
        target: request.target.as_deref().unwrap_or(&context.target),
        message: &request.message,
        fields: &fields,
    };
    channels.emit(ChannelId::from_raw(request.channel), &record).is_ok()
}

/// JavaScript's and LogTape's levels onto the pipeline's five; `fatal` merges
/// into error and keeps its name, as Python's `critical` does.
fn map_level(name: &str) -> (Level, bool) {
    match name.to_ascii_lowercase().as_str() {
        "trace" => (Level::Trace, false),
        "debug" => (Level::Debug, false),
        "info" => (Level::Info, false),
        "warn" | "warning" => (Level::Warn, false),
        "error" => (Level::Error, false),
        "fatal" | "critical" => (Level::Error, true),
        _ => (Level::Info, true),
    }
}

deno_core::extension!(
    claviron_log_extension,
    ops = [op_claviron_log, op_claviron_log_channel],
    esm_entry_point = "ext:claviron_log_extension/log.js",
    esm = [dir "src/extensions/log", "log.js"],
    options = {
        context: LogContext,
    },
    state = |state, options| {
        state.put(options.context);
    },
);

impl ExtensionFactory for LogExtension {
    fn extension(
        &self,
        context: ExtensionContext,
    ) -> Extension {
        claviron_log_extension::init(LogContext {
            channels: self.channels.clone(),
            target: Arc::from(context.application_name()),
        })
    }

    fn modules(&self) -> &'static [ExtensionModule] {
        MODULES
    }
}

#[cfg(test)]
#[path = "../../tests/extensions/log.rs"]
mod tests;
