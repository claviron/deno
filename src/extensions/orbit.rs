use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use claviron::app::{
    ApplicationEvent, ApplicationId, ApplicationName, ApplicationRuntime,
    EventArrayKey, EventValue,
};
use claviron::orbit::lock::{
    LockAcquire, LockKey, LockOwner, LockType, OrbitLock,
};
use claviron::orbit::transport::{InterestGuard, LinkInterest};
use claviron::orbit::{
    Cells, CellsProvider, Counter, CounterProvider, FleetProvider,
    InvocationBus, InvocationProvider,
};
use claviron::runtime::ProcessEventBus;
use deno_core::{Extension, OpState, op2};
use deno_error::JsErrorBox;
use serde::Deserialize;
use tokio::sync::broadcast;

use crate::ExtensionContext;
use crate::backend::deno::{ExtensionFactory, ExtensionModule};

const MAX_SAFE_JAVASCRIPT_INTEGER: i64 = 9_007_199_254_740_991;

const MODULES: &[ExtensionModule] = &[
    ExtensionModule::new(
        "claviron:orbit/counter",
        include_str!("orbit/modules/counter.js"),
    ),
    ExtensionModule::new(
        "claviron:orbit/lock",
        include_str!("orbit/modules/lock.js"),
    ),
    ExtensionModule::new(
        "claviron:orbit/events",
        include_str!("orbit/modules/events.js"),
    ),
    ExtensionModule::new(
        "claviron:orbit/invoke",
        include_str!("orbit/modules/invoke.js"),
    ),
    ExtensionModule::new(
        "claviron:orbit/cell",
        include_str!("orbit/modules/cell.js"),
    ),
];

mod cell;
mod invoke;

use invoke::InvocationInbox;

pub(crate) struct PendingApplicationEvent(pub(crate) String);

struct JavaScriptLock;

impl LockType for JavaScriptLock {
    const NAMESPACE: &'static str = "claviron.js";
}

#[derive(Clone)]
struct OrbitContext {
    application: ApplicationId,
    counter_provider: Arc<CounterProvider>,
    events: ProcessEventBus,
    event_waiters: Arc<Mutex<HashMap<u32, EventWaiter>>>,
    subscriptions: Arc<Mutex<EventSubscriptions>>,
    next_event_waiter: Arc<AtomicU32>,
    fleet_provider: Arc<FleetProvider>,
    /// This isolate's own place in the application event stream.
    ///
    /// Admission follows the isolate's live topic subscriptions; unrelated
    /// events cannot exhaust a task's inbox before it next receives.
    inbox: Arc<tokio::sync::Mutex<broadcast::Receiver<ApplicationEvent>>>,
    lock: Arc<Mutex<Option<OrbitLock>>>,
    cells_provider: Option<Arc<CellsProvider>>,
    invocation_provider: Option<Arc<InvocationProvider>>,
    /// This isolate's invocation subscriptions and the channel they feed.
    invocation_inbox: Arc<InvocationInbox>,
    /// Cell waits this isolate currently has parked.
    cell_waits: Arc<CellWaits>,
}

/// The cells a script is parked on, so a stopping isolate can tell them so.
///
/// A cell wait parks a blocking-pool thread in the kernel until another
/// process writes. That is right until this process is the one going away:
/// the write is never coming, the thread never returns, and the runtime
/// cannot finish its own drop while its blocking pool still holds a thread.
///
/// Waking is the answer, not a deadline. [`Self::interrupt`] sets the word
/// every waiter watches and then wakes each cell — the same set-then-wake
/// the write path uses. The script's own error path then runs, and that is
/// what stopping waits for: no new work is accepted, but work already
/// running still finishes.
#[derive(Default)]
pub(crate) struct CellWaits {
    interrupted: AtomicBool,
    parked: Mutex<HashMap<u64, (Arc<Cells>, String)>>,
    next: AtomicU64,
}

impl CellWaits {
    /// The word a parked waiter watches besides the cell's change count.
    pub(crate) fn interrupted(&self) -> &AtomicBool {
        &self.interrupted
    }

    /// Announce a wait before it parks, so stopping can find its cell.
    pub(crate) fn register(
        &self,
        cells: Arc<Cells>,
        id: String,
    ) -> u64 {
        let handle = self.next.fetch_add(1, Ordering::Relaxed);
        self.parked.lock().unwrap().insert(handle, (cells, id));
        handle
    }

    pub(crate) fn finish(
        &self,
        handle: u64,
    ) {
        self.parked.lock().unwrap().remove(&handle);
    }

    /// End every parked wait: set the word first, then wake, so a waiter
    /// already parked is released and one not yet parked reads the word.
    pub(crate) fn interrupt(&self) {
        self.interrupted.store(true, Ordering::SeqCst);
        for (cells, id) in self.parked.lock().unwrap().values() {
            cell::wake_waiters(cells, id);
        }
    }
}

/// A callback subscription owns one Link declaration per topic. One-shot
/// waiters own independent guards, so neither can withdraw the other's topic.
struct EventSubscriptions {
    interest: Option<Arc<LinkInterest>>,
    topics: HashMap<String, Option<InterestGuard>>,
}

impl EventSubscriptions {
    fn new(interest: Option<Arc<LinkInterest>>) -> Self {
        Self { interest, topics: HashMap::new() }
    }

    fn declare(
        &self,
        topic: &str,
    ) -> Option<InterestGuard> {
        self.interest.as_ref().map(|interest| interest.add([topic]))
    }

    fn subscribe(
        &mut self,
        topic: String,
    ) {
        if !self.topics.contains_key(&topic) {
            let guard = self.declare(&topic);
            self.topics.insert(topic, guard);
        }
    }

    fn unsubscribe(
        &mut self,
        topic: &str,
    ) {
        self.topics.remove(topic);
    }
}

struct EventWaiter {
    receiver: broadcast::Receiver<ApplicationEvent>,
    _interest: Option<InterestGuard>,
}

impl EventWaiter {
    fn new(
        events: &ProcessEventBus,
        application: ApplicationId,
        topic: String,
        interest: Option<&Arc<LinkInterest>>,
    ) -> Self {
        let selected = topic.clone();
        let receiver =
            events.subscribe_filtered(move |event: &ApplicationEvent| {
                event.topic().as_str() == selected
                    && event
                        .target()
                        .is_none_or(|target| target == &application)
            });
        Self {
            receiver,
            _interest: interest.map(|interest| interest.add([topic])),
        }
    }
}

/// Tell this isolate's parked cell waits that it is stopping. Called from
/// the engine's own shutdown, on the thread that owns the isolate, before
/// the shutdown hook runs — so the script's error path is part of the work
/// that stopping waits for.
pub(crate) fn interrupt_cell_waits(state: &OpState) {
    if let Some(context) = state.try_borrow::<OrbitContext>() {
        context.cell_waits.interrupt();
    }
}

impl OrbitContext {
    pub(crate) fn cell_waits(&self) -> Arc<CellWaits> {
        Arc::clone(&self.cell_waits)
    }

    fn accepts(
        &self,
        event: &ApplicationEvent,
    ) -> bool {
        event.target().is_none_or(|target| target == &self.application)
    }
}

impl OrbitContext {
    fn counter(&self) -> Result<Arc<Counter>, OrbitError> {
        self.counter_provider.handle().ok_or(OrbitError::CounterUnavailable)
    }

    fn cells(&self) -> Result<Arc<Cells>, OrbitError> {
        self.cells_provider
            .as_ref()
            .and_then(|provider| provider.handle())
            .ok_or(OrbitError::CellsUnavailable)
    }

    fn invocations(&self) -> Result<Arc<InvocationBus>, OrbitError> {
        self.invocation_provider
            .as_ref()
            .and_then(|provider| provider.handle())
            .ok_or(OrbitError::InvocationUnavailable)
    }

    fn lock(&self) -> Result<OrbitLock, OrbitError> {
        let mut slot = self.lock.lock().unwrap();
        if let Some(lock) = slot.as_ref() {
            return Ok(lock.clone());
        }
        let fleet =
            self.fleet_provider.handle().ok_or(OrbitError::FleetUnavailable)?;
        let lock = OrbitLock::new(fleet)
            .map_err(|error| OrbitError::Lock(error.to_string()))?;
        *slot = Some(lock.clone());
        Ok(lock)
    }
}

#[derive(Debug, thiserror::Error)]
pub(super) enum OrbitError {
    #[error("Orbit counter is unavailable")]
    CounterUnavailable,

    #[error("Orbit fleet is unavailable")]
    FleetUnavailable,

    #[error("Orbital cells are not available in this process")]
    CellsUnavailable,

    #[error("Orbit invocation transport is not available in this process")]
    InvocationUnavailable,

    #[error("Orbit invocation failed: {0}")]
    Invocation(String),

    #[error("invalid signed 64-bit counter amount: {0}")]
    InvalidAmount(String),

    #[error("lock TTL must be a positive 64-bit integer")]
    InvalidTtl,

    #[error("Orbit lock owner is not valid UTF-8")]
    InvalidOwner,

    #[error("application event is invalid: {0}")]
    ApplicationEvent(String),

    #[error("Orbit counter operation failed: {0}")]
    Counter(String),

    #[error("Orbit lock operation failed: {0}")]
    Lock(String),
}

#[derive(Debug, Deserialize)]
struct CounterKeyRequest {
    key: String,
}

#[derive(Debug, Deserialize)]
struct CounterUpdateRequest {
    key: String,
    amount: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LockAcquireRequest {
    key: String,
    owner: String,
    ttl_ms: String,
}

#[derive(Debug, Deserialize)]
struct LockOwnedRequest {
    key: String,
    owner: String,
}

#[derive(Debug, Deserialize)]
struct LockKeyRequest {
    key: String,
}

#[derive(Debug, Deserialize)]
struct EventPublishRequest {
    topic: String,
    payload: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventWaitRequest {
    handle: u32,
    topic: String,
    timeout_ms: u32,
}

pub(super) fn json_to_event_value(
    value: serde_json::Value
) -> Result<EventValue, String> {
    Ok(match value {
        serde_json::Value::Null => EventValue::Null,
        serde_json::Value::Bool(value) => EventValue::Bool(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                if !(-MAX_SAFE_JAVASCRIPT_INTEGER..=MAX_SAFE_JAVASCRIPT_INTEGER)
                    .contains(&value)
                {
                    return Err("application event integer exceeds JavaScript's safe range".into());
                }
                EventValue::Int(value)
            } else {
                EventValue::Float(value.as_f64().ok_or_else(|| {
                    "application event number is not representable as f64"
                        .to_owned()
                })?)
            }
        }
        serde_json::Value::String(value) => {
            EventValue::String(value.into_bytes())
        }
        serde_json::Value::Array(values) => EventValue::Array(
            values
                .into_iter()
                .enumerate()
                .map(|(index, value)| {
                    Ok((
                        EventArrayKey::Int(
                            i64::try_from(index).unwrap_or(i64::MAX),
                        ),
                        json_to_event_value(value)?,
                    ))
                })
                .collect::<Result<_, String>>()?,
        ),
        serde_json::Value::Object(values) => EventValue::Array(
            values
                .into_iter()
                .map(|(key, value)| {
                    Ok((
                        EventArrayKey::String(key),
                        json_to_event_value(value)?,
                    ))
                })
                .collect::<Result<_, String>>()?,
        ),
    })
}

pub(super) fn event_value_to_json(
    value: &EventValue
) -> Result<serde_json::Value, String> {
    match value {
        EventValue::Null => Ok(serde_json::Value::Null),
        EventValue::Bool(value) => Ok(serde_json::Value::Bool(*value)),
        EventValue::Int(value) => {
            if !(-MAX_SAFE_JAVASCRIPT_INTEGER..=MAX_SAFE_JAVASCRIPT_INTEGER).contains(value) {
                return Err("application event integer exceeds JavaScript's safe range".into());
            }
            Ok(serde_json::Value::Number((*value).into()))
        }
        EventValue::Float(value) => serde_json::Number::from_f64(*value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| "application event contains a non-finite float".to_owned()),
        EventValue::String(value) => std::str::from_utf8(value)
            .map(|value| serde_json::Value::String(value.to_owned()))
            .map_err(|_| {
                "application event contains a binary string unavailable to JavaScript".to_owned()
            }),
        EventValue::Array(entries) => {
            let sequential = entries.iter().enumerate().all(|(index, (key, _))| {
                matches!(key, EventArrayKey::Int(key) if *key == i64::try_from(index).unwrap_or(i64::MAX))
            });
            if sequential {
                return entries
                    .iter()
                    .map(|(_, value)| event_value_to_json(value))
                    .collect::<Result<Vec<_>, _>>()
                    .map(serde_json::Value::Array);
            }

            let mut object = serde_json::Map::new();
            for (key, value) in entries {
                let key = match key {
                    EventArrayKey::Int(key) => key.to_string(),
                    EventArrayKey::String(key) => key.clone()
                };
                if object.insert(key.clone(), event_value_to_json(value)?).is_some() {
                    return Err(format!("application event contains duplicate object key {key}"));
                }
            }
            Ok(serde_json::Value::Object(object))
        }
    }
}

pub(crate) fn application_event_json(
    event: &ApplicationEvent
) -> Result<String, String> {
    let mut value = serde_json::Map::new();
    value.insert(
        "id".to_owned(),
        serde_json::Value::String(event.id().to_string()),
    );
    value.insert(
        "topic".to_owned(),
        serde_json::Value::String(event.topic().to_string()),
    );
    value.insert("payload".to_owned(), event_value_to_json(event.payload())?);
    serde_json::to_string(&value).map_err(|error| error.to_string())
}

/// Adds fleet-shared counters, non-blocking locks, and application events to
/// each JavaScript isolate.
///
/// The extension exposes only atomic primitives. Waiting, retries, callbacks,
/// and owner generation remain application policy.
pub struct OrbitExtension {
    counter_provider: Arc<CounterProvider>,
    events: ProcessEventBus,
    fleet_provider: Arc<FleetProvider>,
    cells_provider: Option<Arc<CellsProvider>>,
    invocation_provider: Option<Arc<InvocationProvider>>,
    link_interest: Option<Arc<LinkInterest>>,
}

impl OrbitExtension {
    /// Bind topic declarations to the process's existing Link bridges.
    pub fn with_link_interest(
        mut self,
        interest: Option<Arc<LinkInterest>>,
    ) -> Self {
        self.link_interest = interest;
        self
    }

    pub fn new(
        counter_provider: Arc<CounterProvider>,
        fleet_provider: Arc<FleetProvider>,
        events: ProcessEventBus,
    ) -> Self {
        Self {
            counter_provider,
            events,
            fleet_provider,
            cells_provider: None,
            invocation_provider: None,
            link_interest: None,
        }
    }

    /// `Claviron\Runtime\Orbital`'s JavaScript twin, `claviron:orbit/cell`. Without
    /// a provider the module loads and every call reports the cells absent.
    pub fn with_cells(
        mut self,
        provider: Option<Arc<CellsProvider>>,
    ) -> Self {
        self.cells_provider = provider;
        self
    }

    /// `claviron:orbit/invoke`, sending and serving invocations. Without a
    /// provider the module loads and every call reports the transport absent.
    pub fn with_invocations(
        mut self,
        provider: Option<Arc<InvocationProvider>>,
    ) -> Self {
        self.invocation_provider = provider;
        self
    }
}

#[op2]
fn op_claviron_orbit_event_publish(
    state: &OpState,
    #[serde] request: EventPublishRequest,
) -> Result<(), JsErrorBox> {
    let context = state.borrow::<OrbitContext>();
    let payload = serde_json::from_str(&request.payload).map_err(|error| {
        js_error(OrbitError::ApplicationEvent(error.to_string()))
    })?;
    let payload = json_to_event_value(payload)
        .map_err(|error| js_error(OrbitError::ApplicationEvent(error)))?;
    let event = ApplicationEvent::for_application(
        context.application.clone(),
        request.topic,
        payload,
    )
    .map_err(|error| {
        js_error(OrbitError::ApplicationEvent(error.to_string()))
    })?;
    context.events.emit(event);
    Ok(())
}

#[op2(fast)]
fn op_claviron_orbit_event_node_id(state: &OpState) -> Result<u32, JsErrorBox> {
    let fleet = state
        .borrow::<OrbitContext>()
        .fleet_provider
        .handle()
        .ok_or_else(|| js_error(OrbitError::FleetUnavailable))?;
    Ok(u32::from(fleet.node_id().get()))
}

#[op2(fast)]
fn op_claviron_orbit_event_subscribe(
    state: &OpState,
    #[string] topic: String,
) {
    state
        .borrow::<OrbitContext>()
        .subscriptions
        .lock()
        .unwrap()
        .subscribe(topic);
}

#[op2(fast)]
fn op_claviron_orbit_event_unsubscribe(
    state: &OpState,
    #[string] topic: String,
) {
    state
        .borrow::<OrbitContext>()
        .subscriptions
        .lock()
        .unwrap()
        .unsubscribe(&topic);
}

#[op2(fast)]
fn op_claviron_orbit_event_wait_begin(
    state: &OpState,
    #[string] topic: String,
) -> u32 {
    let context = state.borrow::<OrbitContext>();
    let mut waiters = context.event_waiters.lock().unwrap();
    let handle = loop {
        let handle = context.next_event_waiter.fetch_add(1, Ordering::Relaxed);
        if handle != 0 && !waiters.contains_key(&handle) {
            break handle;
        }
    };
    let interest = context.subscriptions.lock().unwrap().interest.clone();
    let waiter = EventWaiter::new(
        &context.events,
        context.application.clone(),
        topic,
        interest.as_ref(),
    );
    waiters.insert(handle, waiter);
    handle
}

#[op2]
#[string]
async fn op_claviron_orbit_event_wait(
    state: Rc<RefCell<OpState>>,
    #[serde] request: EventWaitRequest,
) -> Result<Option<String>, JsErrorBox> {
    let context = state.borrow().borrow::<OrbitContext>().clone();
    let mut waiter = context
        .event_waiters
        .lock()
        .unwrap()
        .remove(&request.handle)
        .ok_or_else(|| {
            js_error(OrbitError::ApplicationEvent(
                "event waiter is unavailable".into(),
            ))
        })?;
    let deadline = (request.timeout_ms != 0).then(|| {
        tokio::time::Instant::now()
            + Duration::from_millis(u64::from(request.timeout_ms))
    });

    loop {
        let received = match deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline, waiter.receiver.recv())
                    .await
                {
                    Ok(received) => received,
                    Err(_) => return Ok(None),
                }
            }
            None => waiter.receiver.recv().await,
        };
        match received {
            Ok(event)
                if context.accepts(&event)
                    && event.topic().as_str() == request.topic.as_str() =>
            {
                return application_event_json(&event).map(Some).map_err(
                    |error| js_error(OrbitError::ApplicationEvent(error)),
                );
            }
            Ok(_) => continue,
            Err(broadcast::error::RecvError::Lagged(_)) => return Ok(None),
            Err(broadcast::error::RecvError::Closed) => {
                return Err(js_error(OrbitError::ApplicationEvent(
                    "event bus closed".to_owned(),
                )));
            }
        }
    }
}

#[op2]
#[string]
fn op_claviron_orbit_event_take(
    state: &mut OpState
) -> Result<String, JsErrorBox> {
    state
        .try_take::<PendingApplicationEvent>()
        .map(|event| event.0)
        .ok_or_else(|| JsErrorBox::generic("no pending Orbit event"))
}

/// Await the next application event for this isolate.
///
/// The counterpart of PHP's `claviron_event_receive()`, and the reason a `js:`
/// task needs no timer: a pending async op is a pending future on the event
/// loop, so awaiting the next event is both how the task reacts and how it
/// stays alive. `timeout_ms` of zero waits indefinitely — there is nothing to
/// poll for, and a task that wakes once a second to find nothing is the sleep
/// this replaces.
///
/// Topic filtering stays in JavaScript, where the listener registry already
/// lives. Returns `null` when the wait timed out, or when this isolate fell
/// behind and the channel dropped events; both mean "ask again". A closed bus
/// is an error, because there will never be another event.
#[op2]
#[string]
async fn op_claviron_orbit_event_receive(
    state: Rc<RefCell<OpState>>,
    #[smi] timeout_ms: u32,
) -> Result<Option<String>, JsErrorBox> {
    let inbox = Arc::clone(&state.borrow().borrow::<OrbitContext>().inbox);
    let mut inbox = inbox.lock().await;
    let deadline = (timeout_ms != 0).then(|| {
        tokio::time::Instant::now()
            + Duration::from_millis(u64::from(timeout_ms))
    });
    loop {
        let received = match deadline {
            Some(deadline) => {
                match tokio::time::timeout_at(deadline, inbox.recv()).await {
                    Ok(received) => received,
                    Err(_) => return Ok(None),
                }
            }
            None => inbox.recv().await,
        };
        match received {
            Ok(event)
                if state.borrow().borrow::<OrbitContext>().accepts(&event) =>
            {
                return application_event_json(&event).map(Some).map_err(
                    |error| js_error(OrbitError::ApplicationEvent(error)),
                );
            }
            Ok(_) => continue,
            // Lagged is not a failure: the caller loops, and the events it missed
            // are gone either way. Saying so out loud beats a silent gap.
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                tracing::warn!(
                    skipped,
                    "JavaScript isolate lagged behind the application event bus"
                );
                return Ok(None);
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Err(js_error(OrbitError::ApplicationEvent(
                    "event bus closed".to_owned(),
                )));
            }
        }
    }
}

#[op2]
#[string]
fn op_claviron_orbit_counter_get(
    state: &OpState,
    #[serde] request: CounterKeyRequest,
) -> Result<Option<String>, JsErrorBox> {
    state
        .borrow::<OrbitContext>()
        .counter()
        .and_then(|counter| {
            counter
                .get(&request.key)
                .map(|value| value.map(|value| value.to_string()))
                .map_err(|error| OrbitError::Counter(error.to_string()))
        })
        .map_err(js_error)
}

#[op2]
#[string]
fn op_claviron_orbit_counter_increment(
    state: &OpState,
    #[serde] request: CounterUpdateRequest,
) -> Result<String, JsErrorBox> {
    update_counter(state, request, Counter::increment)
}

#[op2]
#[string]
fn op_claviron_orbit_counter_decrement(
    state: &OpState,
    #[serde] request: CounterUpdateRequest,
) -> Result<String, JsErrorBox> {
    update_counter(state, request, Counter::decrement)
}

#[op2]
fn op_claviron_orbit_counter_reset(
    state: &OpState,
    #[serde] request: CounterKeyRequest,
) -> Result<(), JsErrorBox> {
    let counter = state.borrow::<OrbitContext>().counter().map_err(js_error)?;
    counter
        .reset(&request.key)
        .map_err(|error| js_error(OrbitError::Counter(error.to_string())))
}

fn update_counter(
    state: &OpState,
    request: CounterUpdateRequest,
    operation: fn(&Counter, &str, i64) -> claviron::orbit::counter::Result<i64>,
) -> Result<String, JsErrorBox> {
    let amount = request.amount.parse::<i64>().map_err(|_| {
        js_error(OrbitError::InvalidAmount(request.amount.clone()))
    })?;
    let counter = state.borrow::<OrbitContext>().counter().map_err(js_error)?;
    operation(&counter, &request.key, amount)
        .map(|value| value.to_string())
        .map_err(|error| js_error(OrbitError::Counter(error.to_string())))
}

#[op2]
fn op_claviron_orbit_lock_try_acquire(
    state: &OpState,
    #[serde] request: LockAcquireRequest,
) -> Result<bool, JsErrorBox> {
    let ttl = request
        .ttl_ms
        .parse::<u64>()
        .ok()
        .filter(|ttl| *ttl > 0)
        .map(Duration::from_millis)
        .ok_or_else(|| js_error(OrbitError::InvalidTtl))?;
    let lock = state.borrow::<OrbitContext>().lock().map_err(js_error)?;
    let key = LockKey::new::<JavaScriptLock>(request.key);
    let owner = LockOwner::from(request.owner);
    lock.try_acquire(&key, &owner, ttl)
        .map(|result| matches!(result, LockAcquire::Acquired(_)))
        .map_err(|error| js_error(OrbitError::Lock(error.to_string())))
}

#[op2]
fn op_claviron_orbit_lock_release(
    state: &OpState,
    #[serde] request: LockOwnedRequest,
) -> Result<bool, JsErrorBox> {
    let lock = state.borrow::<OrbitContext>().lock().map_err(js_error)?;
    lock.release_owned(
        &LockKey::new::<JavaScriptLock>(request.key),
        &LockOwner::from(request.owner),
    )
    .map_err(|error| js_error(OrbitError::Lock(error.to_string())))
}

#[op2]
#[string]
fn op_claviron_orbit_lock_owner(
    state: &OpState,
    #[serde] request: LockKeyRequest,
) -> Result<Option<String>, JsErrorBox> {
    let lock = state.borrow::<OrbitContext>().lock().map_err(js_error)?;
    let Some(lease) = lock
        .current(&LockKey::new::<JavaScriptLock>(request.key))
        .map_err(|error| js_error(OrbitError::Lock(error.to_string())))?
    else {
        return Ok(None);
    };
    std::str::from_utf8(lease.owner.as_bytes())
        .map(str::to_owned)
        .map(Some)
        .map_err(|_| js_error(OrbitError::InvalidOwner))
}

#[op2]
fn op_claviron_orbit_lock_force_release(
    state: &OpState,
    #[serde] request: LockKeyRequest,
) -> Result<bool, JsErrorBox> {
    let lock = state.borrow::<OrbitContext>().lock().map_err(js_error)?;
    lock.force_release(&LockKey::new::<JavaScriptLock>(request.key))
        .map_err(|error| js_error(OrbitError::Lock(error.to_string())))
}

pub(super) fn js_error(error: OrbitError) -> JsErrorBox {
    JsErrorBox::generic(error.to_string())
}

deno_core::extension!(
    claviron_orbit_extension,
    ops = [
        op_claviron_orbit_counter_decrement,
        op_claviron_orbit_counter_get,
        op_claviron_orbit_counter_increment,
        op_claviron_orbit_counter_reset,
        op_claviron_orbit_event_node_id,
        op_claviron_orbit_event_publish,
        op_claviron_orbit_event_subscribe,
        op_claviron_orbit_event_unsubscribe,
        op_claviron_orbit_event_receive,
        op_claviron_orbit_event_take,
        op_claviron_orbit_event_wait,
        op_claviron_orbit_event_wait_begin,
        op_claviron_orbit_lock_force_release,
        op_claviron_orbit_lock_owner,
        op_claviron_orbit_lock_release,
        op_claviron_orbit_lock_try_acquire,
        cell::op_claviron_orbit_cell_add,
        cell::op_claviron_orbit_cell_add_until,
        cell::op_claviron_orbit_cell_allocate,
        cell::op_claviron_orbit_cell_append,
        cell::op_claviron_orbit_cell_get,
        cell::op_claviron_orbit_cell_open,
        cell::op_claviron_orbit_cell_release,
        cell::op_claviron_orbit_cell_set,
        cell::op_claviron_orbit_cell_version,
        cell::op_claviron_orbit_cell_wait_changed,
        invoke::op_claviron_orbit_invoke_receive,
        invoke::op_claviron_orbit_invoke_send,
        invoke::op_claviron_orbit_invoke_subscribe,
        invoke::op_claviron_orbit_invoke_unsubscribe,
    ],
    esm_entry_point = "ext:claviron_orbit_extension/orbit.js",
    esm = [
        dir "src/extensions/orbit",
        "cell.js",
        "counter.js",
        "events.js",
        "invoke.js",
        "lock.js",
        "orbit.js",
    ],
    options = {
        context: OrbitContext,
    },
    state = |state, options| {
        state.put(options.context);
    },
);

impl ExtensionFactory for OrbitExtension {
    fn extension(
        &self,
        context: ExtensionContext,
    ) -> Extension {
        let application = ApplicationId::new(
            ApplicationRuntime::new("js")
                .expect("static JavaScript runtime identity"),
            ApplicationName::new(context.application_name())
                .expect("validated JavaScript application name"),
        );
        let subscriptions = Arc::new(Mutex::new(EventSubscriptions::new(
            self.link_interest.clone(),
        )));
        let selected = Arc::downgrade(&subscriptions);
        let target = application.clone();
        let inbox =
            self.events.subscribe_filtered(move |event: &ApplicationEvent| {
                selected.upgrade().is_some_and(|selected| {
                    selected
                        .lock()
                        .unwrap()
                        .topics
                        .contains_key(event.topic().as_str())
                }) && event
                    .target()
                    .is_none_or(|application| application == &target)
            });
        crate::engine::embedded::extension(claviron_orbit_extension::init(
            OrbitContext {
                application,
                counter_provider: Arc::clone(&self.counter_provider),
                event_waiters: Arc::new(Mutex::new(HashMap::new())),
                inbox: Arc::new(tokio::sync::Mutex::new(inbox)),
                subscriptions,
                events: self.events.clone(),
                next_event_waiter: Arc::new(AtomicU32::new(1)),
                fleet_provider: Arc::clone(&self.fleet_provider),
                lock: Arc::new(Mutex::new(None)),
                cells_provider: self.cells_provider.clone(),
                invocation_provider: self.invocation_provider.clone(),
                invocation_inbox: Arc::new(InvocationInbox::new()),
                cell_waits: Arc::default(),
            },
        ))
    }

    fn modules(&self) -> &'static [ExtensionModule] {
        MODULES
    }
}

#[cfg(test)]
#[path = "../../tests/extensions/orbit.rs"]
mod tests;
