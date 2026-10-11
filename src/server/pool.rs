#[cfg(feature = "websocket")]
use std::sync::atomic::AtomicU32;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[cfg(feature = "orbit")]
use claviron::app::ApplicationEvent;
use claviron::app::{ApplicationLimits, ApplicationRetryPolicy};
use deno_core::v8;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

#[cfg(feature = "websocket")]
use crate::application::{
    WebSocketAction, WebSocketActionSender, WebSocketEvent, WebSocketMessage,
};
use crate::backend::deno::SharedExtensionFactory;
use crate::config::ApplicationDispatch;
#[cfg(feature = "websocket")]
use crate::config::WebSocketConfig;
use crate::extensions::FetchRootCertStore;
use crate::{
    ApplicationDefinition, ApplicationRequest, ApplicationResponse,
    CoreApplication, Error, Result,
};

enum WorkerCommand {
    Execute {
        request: ApplicationRequest,
        started: oneshot::Sender<()>,
        reply:
            oneshot::Sender<std::result::Result<ApplicationResponse, String>>,
        control: Arc<RequestControl>,
        _slot: OwnedSemaphorePermit,
    },
    #[cfg(feature = "orbit")]
    ApplicationEvent {
        event: ApplicationEvent,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    #[cfg(feature = "websocket")]
    WebSocketOpen {
        event: WebSocketEvent,
        actions: WebSocketActionSender,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    #[cfg(feature = "websocket")]
    WebSocketEvent {
        event: WebSocketEvent,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    #[cfg(feature = "websocket")]
    WebSocketClose {
        event: WebSocketEvent,
        reply: oneshot::Sender<std::result::Result<(), String>>,
    },
    Shutdown,
}

struct ApplicationWorker {
    sender: mpsc::Sender<WorkerCommand>,
    request_slot: Arc<Semaphore>,
    isolate: v8::IsolateHandle,
    thread: Mutex<Option<JoinHandle<()>>>,
    /// Requests this generation of the instance completed. A worker that
    /// served anything proved itself, so its failure counter starts over —
    /// only a boot or crash loop without progress keeps backing off.
    served: Arc<AtomicU64>,
    #[cfg(feature = "websocket")]
    supports_websocket: bool,
}

/// One fixed instance position. The worker inside it is replaced under the
/// retry policy; the position itself lives as long as the pool.
struct Slot {
    instance: usize,
    /// Bumped for every boot attempt. An exit names the generation it
    /// belongs to, so a stale report — a failed boot reports twice, once
    /// through the error and once through the thread — cannot take down
    /// the worker that replaced it.
    generation: AtomicU64,
    worker: Mutex<Option<Arc<ApplicationWorker>>>,
    health: Mutex<SlotHealth>,
}

#[derive(Default)]
struct SlotHealth {
    /// Consecutive failures without a completed request.
    failures: u32,
    /// When the next attempt may run. `None` while a worker is live.
    retry_at: Option<Instant>,
}

impl Slot {
    fn live(&self) -> Option<Arc<ApplicationWorker>> {
        self.worker.lock().unwrap().clone()
    }
}

/// Why a worker thread ended, as its own thread reports it on the way out.
struct WorkerExit {
    instance: usize,
    generation: u64,
    error: Option<String>,
}

struct RequestControl {
    active: AtomicBool,
    timed_out: AtomicBool,
}

impl RequestControl {
    fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            timed_out: AtomicBool::new(false),
        }
    }
}

pub(crate) struct ApplicationPool {
    name: Arc<str>,
    slots: Vec<Arc<Slot>>,
    next: AtomicUsize,
    cancellation: CancellationToken,
    dispatch: ApplicationDispatch,
    limits: ApplicationLimits,
    retry: ApplicationRetryPolicy,
    admission: Semaphore,
    /// Everything a replacement worker needs, kept because a slot may be
    /// refilled long after `start` returned.
    boot: WorkerBoot,
    /// Why the most recent attempt failed, for the management snapshot.
    last_error: Mutex<Option<String>>,
    #[cfg(feature = "websocket")]
    next_websocket: AtomicU32,
    #[cfg(feature = "websocket")]
    websocket: WebSocketConfig,
}

/// The immutable inputs of one worker boot.
#[derive(Clone)]
pub(super) struct WorkerBoot {
    definition: ApplicationDefinition,
    extensions: Arc<[SharedExtensionFactory]>,
    fetch_root_cert_store: FetchRootCertStore,
    fetch_user_agent: Arc<str>,
    command_queue_capacity: usize,
    /// How long a stopping isolate may take to finish accepted work.
    /// `None` waits indefinitely, which is the default.
    graceful_timeout: Option<Duration>,
}

impl WorkerBoot {
    pub(super) fn new(
        definition: ApplicationDefinition,
        extensions: Arc<[SharedExtensionFactory]>,
        fetch_root_cert_store: FetchRootCertStore,
        fetch_user_agent: Arc<str>,
        command_queue_capacity: usize,
        graceful_timeout: Option<Duration>,
    ) -> Self {
        Self {
            definition,
            extensions,
            fetch_root_cert_store,
            fetch_user_agent,
            command_queue_capacity,
            graceful_timeout,
        }
    }
}

/// What the management snapshot can say about a JavaScript pool. The
/// counters PHP reports have the same meaning here; `retiring` has no
/// JavaScript equivalent because there is no slot rotation.
pub(crate) struct JsPoolSnapshot {
    pub(crate) instances: usize,
    pub(crate) ready: usize,
    pub(crate) busy: usize,
    pub(crate) backing_off: usize,
    pub(crate) exhausted: usize,
}

#[cfg(feature = "websocket")]
/// A reserved seat on one application instance, redeemed by the transport
/// that finishes the handshake. Opaque outside the crate.
pub struct WebSocketLease {
    id: u32,
    worker: mpsc::Sender<WorkerCommand>,
    limits: WebSocketConfig,
}

#[cfg(feature = "websocket")]
pub(crate) struct WebSocketSession {
    id: u32,
    worker: mpsc::Sender<WorkerCommand>,
    actions: mpsc::Receiver<WebSocketAction>,
    limits: WebSocketConfig,
}

impl ApplicationPool {
    pub(crate) async fn start(
        boot: WorkerBoot,
        dispatch: ApplicationDispatch,
        limits: ApplicationLimits,
        retry: ApplicationRetryPolicy,
        #[cfg(feature = "websocket")] websocket: WebSocketConfig,
    ) -> Result<Arc<Self>> {
        let cancellation = CancellationToken::new();
        let instances = boot.definition.instance_count();
        let pool = Arc::new(Self {
            name: Arc::from(boot.definition.name()),
            slots: (0..instances)
                .map(|instance| {
                    Arc::new(Slot {
                        instance,
                        generation: AtomicU64::new(0),
                        worker: Mutex::new(None),
                        health: Mutex::new(SlotHealth::default()),
                    })
                })
                .collect(),
            next: AtomicUsize::new(0),
            cancellation,
            dispatch,
            limits,
            retry,
            admission: Semaphore::new(limits.max_waiting()),
            boot,
            last_error: Mutex::new(None),
            #[cfg(feature = "websocket")]
            next_websocket: AtomicU32::new(1),
            #[cfg(feature = "websocket")]
            websocket,
        });

        let (exits, exit_receiver) = mpsc::unbounded_channel();
        // A first boot is the first attempt, not a separate kind of event:
        // a slot that fails here backs off and is retried exactly like one
        // that dies later. The pool is returned either way, and a pool with
        // no live slot rejects dispatch while it recovers.
        for slot in &pool.slots {
            pool.fill(slot, &exits).await;
        }
        tokio::spawn(supervise(Arc::downgrade(&pool), exits, exit_receiver));
        Ok(pool)
    }

    /// Boot one worker into `slot`, recording the outcome against its retry
    /// budget. Returns whether the slot is now live.
    async fn fill(
        &self,
        slot: &Arc<Slot>,
        exits: &mpsc::UnboundedSender<WorkerExit>,
    ) -> bool {
        let generation = slot.generation.fetch_add(1, Ordering::SeqCst) + 1;
        match start_worker(
            self.boot.clone(),
            slot.instance,
            generation,
            self.cancellation.clone(),
            exits.clone(),
        )
        .await
        {
            Ok(worker) => {
                *slot.worker.lock().unwrap() = Some(Arc::new(worker));
                let mut health = slot.health.lock().unwrap();
                health.retry_at = None;
                true
            }
            Err(error) => {
                self.record_failure(slot, 0, error.to_string());
                false
            }
        }
    }

    /// Fold one failure into a slot's budget. `served` is what the dead
    /// worker completed: anything above zero proves the generation was
    /// useful, so the counter starts over.
    fn record_failure(
        &self,
        slot: &Arc<Slot>,
        served: u64,
        error: String,
    ) {
        let mut health = slot.health.lock().unwrap();
        if served > 0 {
            health.failures = 0;
        }
        health.failures = health.failures.saturating_add(1);
        let exhausted = self.retry.exhausted(health.failures);
        if exhausted {
            health.retry_at = None;
            error!(
                application = %self.name,
                instance = slot.instance,
                attempts = health.failures,
                %error,
                "JavaScript application instance failed; retry budget exhausted"
            );
        } else {
            let delay = self.retry.delay_for(health.failures);
            health.retry_at = Some(Instant::now() + delay);
            warn!(
                application = %self.name,
                instance = slot.instance,
                attempts = health.failures,
                ?delay,
                %error,
                "JavaScript application instance stopped; retry scheduled"
            );
        }
        *self.last_error.lock().unwrap() = Some(error);
    }

    /// Earliest moment a down slot becomes eligible for another attempt.
    fn next_retry(&self) -> Option<Instant> {
        self.slots
            .iter()
            .filter_map(|slot| slot.health.lock().unwrap().retry_at)
            .min()
    }

    pub(crate) fn last_error(&self) -> Option<String> {
        self.last_error.lock().unwrap().clone()
    }

    pub(crate) fn snapshot(&self) -> JsPoolSnapshot {
        let mut snapshot = JsPoolSnapshot {
            instances: 0,
            ready: 0,
            busy: 0,
            backing_off: 0,
            exhausted: 0,
        };
        for slot in &self.slots {
            match slot.live() {
                Some(worker) => {
                    snapshot.instances += 1;
                    snapshot.ready += 1;
                    if worker.request_slot.available_permits() == 0 {
                        snapshot.busy += 1;
                    }
                }
                None => {
                    if self
                        .retry
                        .exhausted(slot.health.lock().unwrap().failures)
                    {
                        snapshot.exhausted += 1;
                    } else {
                        snapshot.backing_off += 1;
                    }
                }
            }
        }
        snapshot
    }

    /// Round-robin across the slots that currently hold a worker. A slot in
    /// backoff is skipped rather than dispatched into a closed channel.
    fn select_live(&self) -> Option<Arc<ApplicationWorker>> {
        let live = self
            .slots
            .iter()
            .filter_map(|slot| slot.live())
            .collect::<Vec<_>>();
        if live.is_empty() {
            return None;
        }
        let index = match self.dispatch {
            ApplicationDispatch::RoundRobin => {
                self.next.fetch_add(1, Ordering::Relaxed) % live.len()
            }
        };
        Some(Arc::clone(&live[index]))
    }

    /// `queue_timeout` is the route's `requests.queue_timeout`: how long this
    /// request may wait for its isolate. Unset waits; zero rejects at once.
    pub(crate) async fn execute(
        &self,
        request: ApplicationRequest,
        queue_timeout: Option<Duration>,
    ) -> Result<ApplicationResponse> {
        let _admission =
            self.admission.try_acquire().map_err(|_| Error::AdmissionFull)?;
        let worker = self.select_live().ok_or_else(|| {
            Error::ApplicationNotRunning(self.name.to_string())
        })?;
        let worker = worker.as_ref();
        let immediate = queue_timeout.is_some_and(|timeout| timeout.is_zero());
        let deadline =
            queue_timeout.map(|timeout| tokio::time::Instant::now() + timeout);
        let expired =
            || Error::DispatchQueueTimedOut(queue_timeout.unwrap_or_default());
        let slot = if immediate {
            worker
                .request_slot
                .clone()
                .try_acquire_owned()
                .map_err(|_| expired())?
        } else {
            within(deadline, worker.request_slot.clone().acquire_owned())
                .await
                .ok_or_else(expired)?
                .map_err(|_| Error::RequestChannelClosed)?
        };
        let sender = if immediate {
            worker.sender.try_reserve().map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => expired(),
                mpsc::error::TrySendError::Closed(_) => {
                    Error::RequestChannelClosed
                }
            })?
        } else {
            within(deadline, worker.sender.reserve())
                .await
                .ok_or_else(expired)?
                .map_err(|_| Error::RequestChannelClosed)?
        };
        let (started, accepted) = oneshot::channel();
        let (reply, response) = oneshot::channel();
        let control = Arc::new(RequestControl::new());
        sender.send(WorkerCommand::Execute {
            request,
            started,
            reply,
            control: Arc::clone(&control),
            _slot: slot,
        });
        if !immediate {
            match within(deadline, accepted).await {
                Some(Ok(())) => {}
                Some(Err(_)) => return Err(Error::RequestChannelClosed),
                None => {
                    expire_request(&control, &worker.isolate);
                    return Err(expired());
                }
            }
        }
        let result = match self.limits.execution_timeout() {
            Some(timeout) => {
                match tokio::time::timeout(timeout, response).await {
                    Ok(result) => result,
                    Err(_) => {
                        expire_request(&control, &worker.isolate);
                        return Err(Error::ExecutionTimedOut(timeout));
                    }
                }
            }
            None => response.await,
        };
        result
            .map_err(|_| Error::RequestChannelClosed)?
            .map_err(Error::ApplicationUnavailable)
    }

    #[cfg(feature = "orbit")]
    pub(crate) async fn application_event(
        &self,
        event: ApplicationEvent,
    ) -> Result<()> {
        for slot in &self.slots {
            let Some(worker) = slot.live() else { continue };
            send_worker_command(&worker.sender, |reply| {
                WorkerCommand::ApplicationEvent { event: event.clone(), reply }
            })
            .await?;
        }
        Ok(())
    }

    pub(crate) async fn shutdown(&self) {
        self.cancellation.cancel();
        let workers = self
            .slots
            .iter()
            .filter_map(|slot| slot.worker.lock().unwrap().take());
        let workers = workers.collect::<Vec<_>>();
        for worker in &workers {
            let _ = worker.sender.send(WorkerCommand::Shutdown).await;
        }
        let threads = workers
            .iter()
            .filter_map(|worker| worker.thread.lock().unwrap().take())
            .collect::<Vec<_>>();
        let _ = tokio::task::spawn_blocking(move || {
            for thread in threads {
                let _ = thread.join();
            }
        })
        .await;
    }

    #[cfg(feature = "websocket")]
    pub(crate) fn prepare_websocket(&self) -> Result<WebSocketLease> {
        let candidates = self
            .slots
            .iter()
            .filter_map(|slot| slot.live())
            .filter(|worker| worker.supports_websocket)
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(Error::WebSocketUnavailable(self.name.to_string()));
        }
        let index =
            self.next.fetch_add(1, Ordering::Relaxed) % candidates.len();
        let id = self.next_websocket.fetch_add(1, Ordering::Relaxed);
        let id = if id == 0 {
            self.next_websocket.fetch_add(1, Ordering::Relaxed)
        } else {
            id
        };
        Ok(WebSocketLease {
            id,
            worker: candidates[index].sender.clone(),
            limits: self.websocket,
        })
    }
}

fn expire_request(
    control: &RequestControl,
    isolate: &v8::IsolateHandle,
) {
    control.timed_out.store(true, Ordering::SeqCst);
    if control.active.load(Ordering::SeqCst) {
        isolate.terminate_execution();
    }
}

#[cfg(feature = "websocket")]
impl WebSocketLease {
    pub(crate) async fn open(
        self,
        url: String,
        headers: Vec<(String, String)>,
    ) -> Result<WebSocketSession> {
        let (actions, receiver) =
            mpsc::channel(self.limits.action_queue_capacity);
        let event = WebSocketEvent::Open { id: self.id, url, headers };
        send_worker_command(&self.worker, |reply| {
            WorkerCommand::WebSocketOpen { event, actions, reply }
        })
        .await?;
        Ok(WebSocketSession {
            id: self.id,
            worker: self.worker,
            actions: receiver,
            limits: self.limits,
        })
    }
}

#[cfg(feature = "websocket")]
impl WebSocketSession {
    /// The ceiling the socket loop enforces in both directions, taken from
    /// the engine config the lease was prepared with.
    pub(crate) fn max_message_size(&self) -> usize {
        self.limits.max_message_size
    }

    pub(crate) async fn message(
        &self,
        message: WebSocketMessage,
    ) -> Result<()> {
        let event = WebSocketEvent::Message { id: self.id, message };
        send_worker_command(&self.worker, |reply| {
            WorkerCommand::WebSocketEvent { event, reply }
        })
        .await
    }

    pub(crate) async fn close(
        &self,
        code: u16,
        reason: String,
        was_clean: bool,
    ) -> Result<()> {
        let event =
            WebSocketEvent::Close { id: self.id, code, reason, was_clean };
        send_worker_command(&self.worker, |reply| {
            WorkerCommand::WebSocketClose { event, reply }
        })
        .await
    }

    pub(crate) async fn recv(&mut self) -> Option<WebSocketAction> {
        self.actions.recv().await
    }
}

#[cfg(any(feature = "orbit", feature = "websocket"))]
async fn send_worker_command(
    worker: &mpsc::Sender<WorkerCommand>,
    command: impl FnOnce(
        oneshot::Sender<std::result::Result<(), String>>,
    ) -> WorkerCommand,
) -> Result<()> {
    let (reply, response) = oneshot::channel();
    worker
        .send(command(reply))
        .await
        .map_err(|_| Error::RequestChannelClosed)?;
    response
        .await
        .map_err(|_| Error::RequestChannelClosed)?
        .map_err(Error::ApplicationUnavailable)
}

async fn start_worker(
    boot: WorkerBoot,
    instance: usize,
    generation: u64,
    cancellation: CancellationToken,
    exits: mpsc::UnboundedSender<WorkerExit>,
) -> Result<ApplicationWorker> {
    let (sender, mut receiver) = mpsc::channel(boot.command_queue_capacity);
    let (ready_tx, ready_rx) = oneshot::channel();
    let served = Arc::new(AtomicU64::new(0));
    let thread_served = Arc::clone(&served);
    let thread_name = format!("js:{}:{instance}", boot.definition.name());
    let thread = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
                Ok(runtime) => runtime,
                Err(error) => {
                    let message = error.to_string();
                    let _ = ready_tx.send(Err(message.clone()));
                    let _ = exits.send(WorkerExit { instance, generation, error: Some(message) });
                    return;
                }
            };
            let mut shutdown_requested = false;
            runtime.block_on(async move {
                let mut application =
                    match CoreApplication::boot_instance_with_extensions(
                        boot.definition,
                        boot.extensions.as_ref(),
                        instance,
                        cancellation,
                        boot.fetch_root_cert_store,
                        boot.fetch_user_agent
                    )
                    .await
                    {
                        Ok(mut application) => {
                            let supports_websocket = {
                                #[cfg(feature = "websocket")]
                                {
                                    application.supports_websocket()
                                }
                                #[cfg(not(feature = "websocket"))]
                                {
                                    false
                                }
                            };
                            let isolate = application.isolate_handle();
                            let _ = ready_tx.send(Ok((supports_websocket, isolate)));
                            application
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let _ = ready_tx.send(Err(message.clone()));
                            let _ = exits.send(WorkerExit { instance, generation, error: Some(message) });
                            return;
                        }
                    };

                let mut drive_runtime = true;
                loop {
                    let command = if drive_runtime {
                        tokio::select! {
                            command = receiver.recv() => command,
                            result = application.drive_event_loop() => {
                                if let Err(error) = result {
                                    error!(application = application.definition().name(), %error, "JavaScript background task failed");
                                }
                                drive_runtime = false;
                                continue;
                            }
                        }
                    } else {
                        receiver.recv().await
                    };

                    let Some(command) = command else { break };
                    let executed = matches!(command, WorkerCommand::Execute { .. });
                    if !handle_command(&mut application, command, boot.graceful_timeout).await {
                        shutdown_requested = true;
                        break;
                    }
                    if executed {
                        thread_served.fetch_add(1, Ordering::Relaxed);
                    }
                    drive_runtime = true;
                }
                if !shutdown_requested {
                    // The mailbox closed without a Shutdown command, so the
                    // isolate is gone for a reason nobody asked for.
                    let _ = exits.send(WorkerExit { instance, generation, error: None });
                }
            });
        })
        .map_err(|error| Error::ApplicationUnavailable(error.to_string()))?;

    let (_supports_websocket, isolate) = ready_rx
        .await
        .map_err(|_| Error::RequestChannelClosed)?
        .map_err(Error::ApplicationUnavailable)?;
    Ok(ApplicationWorker {
        sender,
        request_slot: Arc::new(Semaphore::new(1)),
        isolate,
        thread: Mutex::new(Some(thread)),
        served,
        #[cfg(feature = "websocket")]
        supports_websocket: _supports_websocket,
    })
}

async fn handle_command(
    application: &mut CoreApplication,
    command: WorkerCommand,
    graceful_timeout: Option<Duration>,
) -> bool {
    match command {
        WorkerCommand::Execute { request, started, reply, control, _slot } => {
            if control.timed_out.load(Ordering::SeqCst) {
                return true;
            }
            control.active.store(true, Ordering::SeqCst);
            if control.timed_out.load(Ordering::SeqCst) {
                control.active.store(false, Ordering::SeqCst);
                application.cancel_terminate_execution();
                return true;
            }
            let _ = started.send(());
            // The head goes to `reply` from inside the isolate as soon as the
            // handler answers; this returns once the body is written, which
            // is how long the request holds its isolate.
            if let Err(error) = application.handle(request, reply).await {
                tracing::debug!(%error, "JavaScript application request ended with an error");
            }
            control.active.store(false, Ordering::SeqCst);
            if control.timed_out.load(Ordering::SeqCst) {
                application.cancel_terminate_execution();
            }
        }
        #[cfg(feature = "orbit")]
        WorkerCommand::ApplicationEvent { event, reply } => {
            let result = application
                .application_event(event)
                .await
                .map_err(|error| error.to_string());
            let _ = reply.send(result);
        }
        #[cfg(feature = "websocket")]
        WorkerCommand::WebSocketOpen { event, actions, reply } => {
            let result = application
                .websocket_open(event, actions)
                .await
                .map_err(|error| error.to_string());
            let _ = reply.send(result);
        }
        #[cfg(feature = "websocket")]
        WorkerCommand::WebSocketEvent { event, reply } => {
            let result = application
                .websocket_event(event)
                .await
                .map_err(|error| error.to_string());
            let _ = reply.send(result);
        }
        #[cfg(feature = "websocket")]
        WorkerCommand::WebSocketClose { event, reply } => {
            let result = application
                .websocket_close(event)
                .await
                .map_err(|error| error.to_string());
            let _ = reply.send(result);
        }
        WorkerCommand::Shutdown => {
            // Stopping waits for the work this isolate already accepted.
            // How long it may wait is the operator's to declare: `None` is
            // the default and means indefinitely — no deadline, no kill.
            // A declared deadline is the operator authorising the drain to
            // stop waiting and say what was still running.
            let name = application.definition().name().to_owned();
            let drained = match graceful_timeout {
                Some(deadline) => {
                    match tokio::time::timeout(deadline, application.shutdown())
                        .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            warn!(
                                application = %name,
                                ?deadline,
                                "JavaScript application did not finish within its \
                                 graceful_timeout; stopping without it"
                            );
                            return false;
                        }
                    }
                }
                None => application.shutdown().await,
            };
            if let Err(error) = drained {
                error!(application = %name, %error, "JavaScript application shutdown hook failed");
            }
            return false;
        }
    }
    true
}

/// Replaces failed instances under the retry policy for as long as the pool
/// lives. Recovery for a worker that has already booted is this loop's job;
/// nothing above it sees an instance come and go.
async fn supervise(
    pool: Weak<ApplicationPool>,
    exits: mpsc::UnboundedSender<WorkerExit>,
    mut exit_receiver: mpsc::UnboundedReceiver<WorkerExit>,
) {
    loop {
        let Some(pool) = pool.upgrade() else { return };
        if pool.cancellation.is_cancelled() {
            return;
        }
        let due = pool.next_retry();
        let cancelled = pool.cancellation.clone();
        let exit = tokio::select! {
            () = cancelled.cancelled() => return,
            exit = exit_receiver.recv() => match exit {
                Some(exit) => Some(exit),
                None => return
            },
            () = sleep_until(due) => None
        };

        if let Some(WorkerExit { instance, generation, error }) = exit {
            let Some(slot) = pool.slots.get(instance) else { continue };
            if slot.generation.load(Ordering::SeqCst) != generation {
                continue;
            }
            // A worker the pool already dropped was replaced or shut down
            // deliberately; its exit is not a failure.
            let served = match slot.worker.lock().unwrap().take() {
                Some(worker) => worker.served.load(Ordering::Relaxed),
                None => continue,
            };
            pool.record_failure(
                slot,
                served,
                error.unwrap_or_else(|| "instance exited".to_owned()),
            );
        }

        let now = Instant::now();
        let ready = pool
            .slots
            .iter()
            .filter(|slot| {
                slot.worker.lock().unwrap().is_none()
                    && slot
                        .health
                        .lock()
                        .unwrap()
                        .retry_at
                        .is_some_and(|retry_at| retry_at <= now)
            })
            .cloned()
            .collect::<Vec<_>>();
        for slot in ready {
            if pool.cancellation.is_cancelled() {
                return;
            }
            if pool.fill(&slot, &exits).await {
                info!(
                    application = %pool.name,
                    instance = slot.instance,
                    "🧩 JavaScript application instance replaced"
                );
            }
        }
    }
}

/// Waits until `deadline`, or forever when no slot is waiting to be retried.
async fn sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline))
                .await
        }
        None => std::future::pending().await,
    }
}

#[cfg(test)]
#[path = "../../tests/server/pool.rs"]
mod tests;

/// `future` until `deadline`, or as long as it takes when there is none.
async fn within<F: std::future::Future>(
    deadline: Option<tokio::time::Instant>,
    future: F,
) -> Option<F::Output> {
    match deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, future).await.ok(),
        None => Some(future.await),
    }
}
