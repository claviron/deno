//! Dispatch contracts with manually driven inboxes; no isolate or owner thread.

use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use claviron::app::ApplicationLimits;
use tokio::sync::Semaphore;
#[cfg(feature = "websocket")]
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use super::{
    ApplicationPool, ApplicationWorker, ExecuteRequest, Slot, SlotHealth,
    WorkerBoot, WorkerCommand, mailbox,
};
use crate::application::{ReplySender, RequestCounters, RequestLifetime};
use crate::config::{ApplicationDispatch, default_retry_policy};
use crate::{
    ApplicationDefinition, ApplicationRequest, ApplicationResponse, Error,
    RequestBody, ResponseBody,
};

struct Owner {
    inbox: mailbox::Receiver<WorkerCommand>,
    counters: Arc<RequestCounters>,
}

impl Owner {
    async fn request(&mut self) -> ExecuteRequest {
        let Some(WorkerCommand::Execute(request)) = self.inbox.recv().await
        else {
            panic!("expected an HTTP command");
        };
        request
    }

    fn active(&self) -> usize {
        self.counters.active.load(Ordering::Relaxed)
    }
}

fn dispatch_pool(
    instances: usize,
    limits: ApplicationLimits,
) -> (ApplicationPool, Vec<Owner>) {
    let mut owners = Vec::new();
    let slots = (0..instances)
        .map(|instance| {
            let (sender, inbox) = mailbox::channel(1);
            let counters = Arc::new(RequestCounters::default());
            owners.push(Owner { inbox, counters: Arc::clone(&counters) });
            Arc::new(Slot {
                instance,
                generation: AtomicU64::new(1),
                worker: Mutex::new(Some(Arc::new(ApplicationWorker {
                    sender,
                    thread: Mutex::new(None),
                    counters,
                    #[cfg(feature = "websocket")]
                    supports_websocket: false,
                }))),
                health: Mutex::new(SlotHealth::default()),
            })
        })
        .collect();
    // The definition is retained pool state only: no module is read or booted.
    let definition = ApplicationDefinition::new(
        "dispatch-test",
        std::env::temp_dir().join("dispatch-test.js"),
    )
    .unwrap()
    .instances(instances)
    .unwrap();
    let pool = ApplicationPool {
        name: Arc::from("dispatch-test"),
        slots,
        next: AtomicUsize::new(0),
        cancellation: CancellationToken::new(),
        dispatch: ApplicationDispatch::RoundRobin,
        limits,
        retry: default_retry_policy(),
        admission: Arc::new(Semaphore::new(limits.max_waiting())),
        boot: WorkerBoot::new(
            definition,
            Arc::from([]),
            Default::default(),
            Arc::from("Claviron"),
            1,
            None,
        ),
        last_error: Mutex::new(None),
        #[cfg(feature = "websocket")]
        next_websocket: std::sync::atomic::AtomicU32::new(1),
        #[cfg(feature = "websocket")]
        websocket: crate::config::WebSocketConfig::default(),
    };
    (pool, owners)
}

fn request() -> ApplicationRequest {
    ApplicationRequest::new(RequestBody::Empty)
}

fn response() -> ApplicationResponse {
    ApplicationResponse::new(ResponseBody::Whole(Bytes::from_static(b"ok")))
}

fn accept(request: ExecuteRequest) -> (ReplySender, RequestLifetime) {
    let mut accepted = None;
    request
        .start(|_, reply, lifetime| {
            accepted = Some((reply, lifetime));
            Ok(())
        })
        .unwrap();
    accepted.expect("the live request must enter guest execution")
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn disabled_timeouts_need_only_reply_and_retain_guest_admission() {
    let (pool, mut owners) = dispatch_pool(1, ApplicationLimits::new(1, None));
    let mut call = std::pin::pin!(pool.execute(request(), None));
    assert_pending(call.as_mut()).await;
    let queued = owners[0].request().await;
    assert!(queued.started.is_none());
    let (reply, execution) = accept(queued);
    reply.send(Ok(response())).unwrap();
    call.await.unwrap();

    assert_eq!(pool.admission.available_permits(), 0);
    assert_eq!(owners[0].active(), 1);
    drop(execution);
    assert_eq!(pool.admission.available_permits(), 1);
    assert_eq!(owners[0].active(), 0);
}

#[tokio::test]
async fn cancelled_slow_owner_backlog_can_exhaust_shared_admission() {
    let (pool, mut owners) = dispatch_pool(2, ApplicationLimits::new(2, None));
    let mut first = Box::pin(pool.execute(request(), None));
    assert_pending(first.as_mut()).await;

    let mut healthy = std::pin::pin!(pool.execute(request(), None));
    assert_pending(healthy.as_mut()).await;
    let (reply, execution) = accept(owners[1].request().await);
    reply.send(Ok(response())).unwrap();
    healthy.await.unwrap();
    drop(execution);

    let mut second = Box::pin(pool.execute(request(), None));
    assert_pending(second.as_mut()).await;
    drop(first);
    drop(second);
    assert_eq!(owners[0].active(), 2);
    assert_eq!(owners[1].active(), 0);
    assert_eq!(pool.admission.available_permits(), 0);
    assert!(matches!(
        pool.execute(request(), None).await,
        Err(Error::AdmissionFull)
    ));

    for remaining in [1, 0] {
        let queued = owners[0].request().await;
        assert!(queued.lifetime.cancellation.is_cancelled());
        queued
            .start(|_, _, _| panic!("cancelled queued work entered the guest"))
            .unwrap();
        assert_eq!(owners[0].active(), remaining);
        assert_eq!(pool.admission.available_permits(), 2 - remaining);
    }

    // The rejected dispatch did not advance round-robin. The healthy owner
    // becomes reachable again once the slow owner's backlog is consumed.
    let mut resumed = std::pin::pin!(pool.execute(request(), None));
    assert_pending(resumed.as_mut()).await;
    let (reply, execution) = accept(owners[1].request().await);
    reply.send(Ok(response())).unwrap();
    resumed.await.unwrap();
    drop(execution);
    assert_eq!(pool.admission.available_permits(), 2);
    assert_eq!(owners[1].active(), 0);
}

#[cfg(feature = "websocket")]
#[tokio::test]
async fn zero_queue_timeout_bypasses_full_controls_but_waits_for_reply() {
    let (pool, mut owners) = dispatch_pool(1, ApplicationLimits::new(1, None));
    let (control_reply, _control_response) = oneshot::channel();
    pool.slots[0]
        .live()
        .unwrap()
        .sender
        .send_control(WorkerCommand::WebSocketClose {
            event: super::WebSocketEvent::Close {
                id: 1,
                code: 1000,
                reason: String::new(),
                was_clean: true,
            },
            reply: control_reply,
        })
        .await
        .unwrap();
    let mut call =
        std::pin::pin!(pool.execute(request(), Some(Duration::ZERO)));
    assert_pending(call.as_mut()).await;
    assert_eq!(owners[0].active(), 1);
    assert!(matches!(
        pool.execute(request(), Some(Duration::ZERO)).await,
        Err(Error::AdmissionFull)
    ));
    assert!(matches!(
        owners[0].inbox.recv().await,
        Some(WorkerCommand::WebSocketClose { .. })
    ));
    let queued = owners[0].request().await;
    assert!(queued.started.is_none());
    assert_pending(call.as_mut()).await;
    let (reply, execution) = accept(queued);
    reply.send(Ok(response())).unwrap();
    call.await.unwrap();
    drop(execution);
    assert_eq!(owners[0].active(), 0);
    assert_eq!(pool.admission.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn positive_queue_timeout_cancels_but_retains_queued_admission() {
    let queue_timeout = Duration::from_secs(10);
    let (pool, mut owners) = dispatch_pool(1, ApplicationLimits::new(1, None));
    let mut call = std::pin::pin!(pool.execute(request(), Some(queue_timeout)));
    assert_pending(call.as_mut()).await;
    tokio::time::advance(queue_timeout).await;
    assert!(
        matches!(call.await, Err(Error::DispatchQueueTimedOut(value)) if value == queue_timeout)
    );
    assert_eq!(pool.admission.available_permits(), 0);
    assert_eq!(owners[0].active(), 1);

    let queued = owners[0].request().await;
    assert!(queued.started.is_some());
    assert!(queued.lifetime.cancellation.is_cancelled());
    queued
        .start(|_, _, _| panic!("expired queued work entered the guest"))
        .unwrap();
    assert_eq!(owners[0].active(), 0);
    assert_eq!(pool.admission.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn acknowledgement_before_deadline_starts_a_separate_execution_budget() {
    let queue_timeout = Duration::from_secs(10);
    let execution_timeout = Duration::from_secs(4);
    let (pool, mut owners) =
        dispatch_pool(1, ApplicationLimits::new(1, Some(execution_timeout)));
    let mut call = std::pin::pin!(pool.execute(request(), Some(queue_timeout)));
    assert_pending(call.as_mut()).await;
    tokio::time::advance(Duration::from_secs(8)).await;
    assert_pending(call.as_mut()).await;
    let queued = owners[0].request().await;
    assert!(queued.started.is_some());
    let (reply, execution) = accept(queued);
    assert_pending(call.as_mut()).await;
    // The original queue deadline passes, but the execution budget has not.
    tokio::time::advance(Duration::from_secs(3)).await;
    reply.send(Ok(response())).unwrap();
    call.await.unwrap();
    assert!(!execution.cancellation.is_cancelled());
    drop(execution);
    assert_eq!(owners[0].active(), 0);
    assert_eq!(pool.admission.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn execution_only_timeout_waits_for_owner_before_starting_its_budget() {
    let execution_timeout = Duration::from_secs(4);
    let (pool, mut owners) =
        dispatch_pool(1, ApplicationLimits::new(1, Some(execution_timeout)));
    let mut call = std::pin::pin!(pool.execute(request(), None));
    assert_pending(call.as_mut()).await;
    tokio::time::advance(execution_timeout * 2).await;
    assert_pending(call.as_mut()).await;
    let queued = owners[0].request().await;
    assert!(queued.started.is_some());
    let (_reply, execution) = accept(queued);
    assert_pending(call.as_mut()).await;
    tokio::time::advance(execution_timeout).await;
    assert!(
        matches!(call.await, Err(Error::ExecutionTimedOut(value)) if value == execution_timeout)
    );
    assert!(execution.cancellation.is_cancelled());
    assert_eq!(owners[0].active(), 1);
    assert_eq!(pool.admission.available_permits(), 0);
    drop(execution);
    assert_eq!(owners[0].active(), 0);
    assert_eq!(pool.admission.available_permits(), 1);
}

#[tokio::test(start_paused = true)]
async fn zero_queue_timeout_starts_execution_budget_without_acknowledgement() {
    let execution_timeout = Duration::from_secs(4);
    let (pool, mut owners) =
        dispatch_pool(1, ApplicationLimits::new(1, Some(execution_timeout)));
    let mut call =
        std::pin::pin!(pool.execute(request(), Some(Duration::ZERO)));
    assert_pending(call.as_mut()).await;
    tokio::time::advance(execution_timeout).await;
    assert!(
        matches!(call.await, Err(Error::ExecutionTimedOut(value)) if value == execution_timeout)
    );
    let queued = owners[0].request().await;
    assert!(queued.started.is_none());
    assert!(queued.lifetime.cancellation.is_cancelled());
    queued
        .start(|_, _, _| panic!("expired queued work entered the guest"))
        .unwrap();
    assert_eq!(owners[0].active(), 0);
    assert_eq!(pool.admission.available_permits(), 1);
}

#[tokio::test]
async fn owner_loss_fails_unacknowledged_and_immediate_dispatch() {
    for queue_timeout in [None, Some(Duration::ZERO)] {
        let (pool, owners) = dispatch_pool(1, ApplicationLimits::new(1, None));
        let counters = Arc::clone(&owners[0].counters);
        let mut call = std::pin::pin!(pool.execute(request(), queue_timeout));
        assert_pending(call.as_mut()).await;
        drop(owners);
        assert!(matches!(call.await, Err(Error::RequestChannelClosed)));
        assert_eq!(counters.active.load(Ordering::Relaxed), 0);
        assert_eq!(pool.admission.available_permits(), 1);
        assert!(matches!(
            pool.execute(request(), queue_timeout).await,
            Err(Error::RequestChannelClosed)
        ));
        assert_eq!(counters.active.load(Ordering::Relaxed), 0);
        assert_eq!(pool.admission.available_permits(), 1);
    }
}
