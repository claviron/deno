//! Running a `js:` task on the embedded JavaScript engine.
//!
//! `claviron-tasks` owns the trigger, the scope, the dispatch, the attempt
//! budget and the drain deadline. This module owns one thing: how a single
//! occurrence reaches V8. The whole coupling is one installed closure, the same
//! one `claviron-php-embed` installs for `php:`.
//!
//! A task is not an application. An application exports a handler and answers
//! requests; a task is the work itself, so its module is loaded as the main
//! module, its top level runs, and whatever it leaves on the event loop keeps
//! it alive — a scheduled script that finishes, or a program that never does.
//!
//! Cancellation is simpler here than on the PHP side and worth saying why: the
//! event loop is a future, so stopping is dropping it. There is no interrupt to
//! land and no native call to miss, which is the whole of what makes a
//! cooperative PHP drain delicate.

use std::path::PathBuf;
use std::sync::Arc;

use claviron::runtime::AppState;
use claviron::tasks::service::TaskService;
use claviron::tasks::{
    Error as TaskError, Executor, TaskExecution, TaskExecutorKind, TaskResult,
};

use crate::application::ApplicationDefinition;
use crate::backend::deno::SharedExtensionFactory;
use crate::config::FetchConfig;
use crate::engine::CoreEngine;
use crate::extensions::ExtensionContext;
use crate::server::service::{
    configured_extensions, configured_fetch_root_cert_store,
};

/// Install the JavaScript executor on the task service, if one is present.
///
/// Register `TaskService` first. Installing the executor registers existing
/// `js:` tasks; the service's boot and reload also load later declarations.
pub fn register(state: &AppState) -> Result<(), String> {
    let Some(tasks) = state.registry_ref().resolve::<TaskService>() else {
        // A build that carries the engine but not the task service is a valid
        // composition; there is simply nothing to install into.
        return Ok(());
    };
    tasks.set_executor(TaskExecutorKind::Js, executor());
    Ok(())
}

/// The existing task executor contract, for explicit consumer registration.
pub fn executor() -> Executor {
    Arc::new(|execution: TaskExecution| Box::pin(run(execution)))
}

/// One occurrence, on its own OS thread with its own current-thread runtime.
///
/// V8 isolates are pinned to the thread that made them, which is why this is a
/// thread rather than a task — the same reason `server::pool` spawns one per
/// application instance.
async fn run(execution: TaskExecution) -> TaskResult<()> {
    let TaskExecution {
        task,
        spec,
        state,
        cancellation,
        runtime: task_runtime,
        graceful_timeout,
        ..
    } = execution;

    let module = match (&spec.cwd, PathBuf::from(&spec.script).is_absolute()) {
        (Some(cwd), false) => PathBuf::from(cwd).join(&spec.script),
        _ => PathBuf::from(&spec.script),
    };
    let module = std::fs::canonicalize(&module).map_err(|error| {
        TaskError::other(format!(
            "js task '{task}': {}: {error}",
            module.display()
        ))
    })?;

    let mut environment = spec.env.clone();
    // The same identity a command task receives, in the same spelling.
    for (key, value) in task_runtime.identity_env() {
        environment.entry(key.to_owned()).or_insert(value);
    }

    let definition = ApplicationDefinition::new(task.clone(), module)
        .map_err(|error| TaskError::other(error.to_string()))?
        .environments(environment);

    // The same set an application instance is built with. Without this a task
    // could not publish an event, let alone wait for one.
    let extensions: Vec<SharedExtensionFactory> = configured_extensions(&state)
        .map_err(|error| {
            TaskError::other(format!("js task '{task}': {error}"))
        })?;
    // The engine the service was configured with; a build without the
    // service runs on the engine defaults.
    let (fetch_user_agent, trust_certstore) = state
        .registry_ref()
        .resolve::<crate::server::service::JsApplicationService>()
        .map(|service| service.fetch_settings())
        .unwrap_or_else(|| {
            let fetch = FetchConfig::default();
            (Arc::from(fetch.user_agent.as_str()), fetch.trust_certstore)
        });
    let fetch_root_cert_store =
        configured_fetch_root_cert_store(&state, trust_certstore).map_err(
            |error| TaskError::other(format!("js task '{task}': {error}")),
        )?;

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let thread_cancellation = cancellation.clone();
    let thread_task = task.clone();
    let thread = std::thread::Builder::new()
        .name(format!("js:task:{task}"))
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = done_tx.send(Err(error.to_string()));
                    return;
                }
            };
            runtime.block_on(async move {
                let output =
                    spec.log.open(spec.cwd.as_deref().map(std::path::Path::new), &thread_task);
                let context = ExtensionContext::new(definition.name())
                    .with_environment(definition.environment_values())
                    .with_cancellation(thread_cancellation.clone())
                    .with_fetch_root_cert_store(fetch_root_cert_store)
                    .with_fetch_user_agent(fetch_user_agent)
                    .with_task_output(output);
                let mut engine = match CoreEngine::with_context(context, &extensions) {
                    Ok(engine) => engine,
                    Err(error) => {
                        let _ = done_tx.send(Err(error.to_string()));
                        return;
                    }
                };
                // Cancellation stops the program taking new work; it does
                // not take the engine away from work already running. The
                // invocation pump closes, the program's own loops see
                // `isCancelled()` and wind down, and this waits for that.
                //
                // The deadline belongs to the task, not to this module, and
                // it starts here where the drain does. `None` waits
                // indefinitely and never interrupts — `GRACEFUL_OWNERSHIP:
                // NO_IMPLICIT_TERMINATION` — and a value is the operator
                // authorising the interrupt, which is the only thing that
                // ends a program that never asks.
                let program = engine.boot_program(definition.main_module());
                tokio::pin!(program);
                let outcome = tokio::select! {
                    result = &mut program => {
                        result.map_err(|error| format!("js task '{thread_task}': {error}"))
                    }
                    () = thread_cancellation.cancelled() => {
                        match graceful_timeout {
                            Some(grace) => {
                                match tokio::time::timeout(grace, &mut program).await {
                                    Ok(result) => result.map_err(|error| {
                                        format!("js task '{thread_task}': {error}")
                                    }),
                                    Err(_) => {
                                        tracing::warn!(
                                            task = %thread_task,
                                            ?grace,
                                            "JavaScript task did not finish within its \
                                             graceful_timeout; stopping without it"
                                        );
                                        Ok(())
                                    }
                                }
                            }
                            None => (&mut program).await.map_err(|error| {
                                format!("js task '{thread_task}': {error}")
                            })
                        }
                    }
                };
                let _ = done_tx.send(outcome);
            });
        })
        .map_err(|error| TaskError::other(format!("js task '{task}': {error}")))?;

    let outcome = done_rx.await.unwrap_or_else(|_| {
        Err(format!("js task '{task}' ended without reporting"))
    });
    // Join on a blocking pool thread: the isolate tears down on its own thread.
    let _ = tokio::task::spawn_blocking(move || thread.join()).await;
    outcome.map_err(TaskError::other)
}
