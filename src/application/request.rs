//! Request-local bridge resources. V8 state is confined to its owner thread.
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use super::ReplySender;

/// A complete response avoids a channel; a live producer allocates one on demand.
#[derive(Debug)]
pub enum ResponseBody {
    Whole(Bytes),
    Stream(ResponseStream),
}

impl ResponseBody {
    /// Read the next piece, preserving a producer failure as an error.
    pub async fn recv(&mut self) -> Option<Result<Bytes, String>> {
        match self {
            Self::Whole(bytes) => {
                (!bytes.is_empty()).then(|| Ok(std::mem::take(bytes)))
            }
            Self::Stream(stream) => {
                std::future::poll_fn(|cx| stream.poll_recv(cx)).await
            }
        }
    }
}

/// A live response owns only its own producer's cancellation and backpressure.
#[derive(Debug)]
pub struct ResponseStream {
    pub(crate) receiver: mpsc::Receiver<Bytes>,
    pub(crate) completion: oneshot::Receiver<Result<(), String>>,
    pub(crate) cancellation: CancellationToken,
    ended: bool,
}

impl ResponseStream {
    pub(crate) fn new(
        receiver: mpsc::Receiver<Bytes>,
        completion: oneshot::Receiver<Result<(), String>>,
        cancellation: CancellationToken,
    ) -> Self {
        Self { receiver, completion, cancellation, ended: false }
    }

    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Bytes, String>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(bytes)) => return Poll::Ready(Some(Ok(bytes))),
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => {}
        }
        let result = std::task::ready!(Pin::new(&mut self.completion).poll(cx));
        self.ended = true;
        Poll::Ready(match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => {
                Some(Err("JavaScript response producer disappeared".into()))
            }
        })
    }
}

impl Drop for ResponseStream {
    fn drop(&mut self) {
        if !self.ended {
            self.cancellation.cancel();
        }
    }
}

/// Owned by the guest execution, including after its HTTP head has been sent.
pub(crate) struct RequestLifetime {
    pub cancellation: CancellationToken,
    #[cfg(feature = "server")]
    pub _admission: Option<tokio::sync::OwnedSemaphorePermit>,
    #[cfg(feature = "server")]
    pub counters: Option<std::sync::Arc<RequestCounters>>,
}

impl Default for RequestLifetime {
    fn default() -> Self {
        Self {
            cancellation: CancellationToken::new(),
            #[cfg(feature = "server")]
            _admission: None,
            #[cfg(feature = "server")]
            counters: None,
        }
    }
}

#[cfg(feature = "server")]
#[derive(Default)]
pub(crate) struct RequestCounters {
    pub active: std::sync::atomic::AtomicUsize,
    pub served: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "server")]
impl Drop for RequestLifetime {
    fn drop(&mut self) {
        if let Some(counters) = &self.counters {
            counters.active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
}

pub(crate) struct RequestResource {
    pub has_input: bool,
    // A read takes this receiver until it completes; concurrent reads are rejected.
    pub input: RefCell<Option<mpsc::Receiver<Result<Bytes, String>>>>,
    pub reply: RefCell<Option<ReplySender>>,
    pub output: RefCell<Option<mpsc::Sender<Bytes>>>,
    pub completion: RefCell<Option<oneshot::Sender<Result<(), String>>>>,
    pub lifetime: RequestLifetime,
    pub closed: Cell<bool>,
}

impl RequestResource {
    pub fn finish(
        &self,
        result: Result<(), String>,
    ) {
        if self.closed.replace(true) {
            return;
        }
        self.input.borrow_mut().take();
        let streaming = self.output.borrow_mut().take().is_some();
        if let Some(reply) = self.reply.borrow_mut().take() {
            let error = result.as_ref().err().cloned().unwrap_or_else(|| {
                "JavaScript application handler ended without a response".into()
            });
            let _ = reply.send(Err(error));
        }
        #[cfg(feature = "server")]
        if result.is_ok()
            && let Some(counters) = &self.lifetime.counters
        {
            counters.served.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        if let Some(completion) = self.completion.borrow_mut().take() {
            let _ = completion.send(result);
        }
        // Wake only this request's pending bridge reads and writes.
        if self.has_input || streaming {
            self.lifetime.cancellation.cancel();
        }
    }
}

impl Drop for RequestResource {
    fn drop(&mut self) {
        self.finish(Err(
            "JavaScript application stopped before the request finished".into(),
        ));
    }
}

/// Handles cross into JavaScript Numbers, so all 53 integer bits must remain
/// exact. Never wrap or recycle one: a retained JS closure cannot target a
/// later request. Live entries remain bounded by application admission.
#[derive(Default)]
pub(crate) struct RequestResources {
    next: u64,
    entries: BTreeMap<u64, Rc<RequestResource>>,
}

impl RequestResources {
    pub fn insert(
        &mut self,
        resource: RequestResource,
    ) -> Result<u64, &'static str> {
        // Number.MAX_SAFE_INTEGER is the wire representation's fixed limit.
        if self.next > (1_u64 << 53) - 1 {
            return Err("JavaScript request handle space exhausted");
        }
        let id = self.next;
        self.next += 1;
        self.entries.insert(id, Rc::new(resource));
        Ok(id)
    }

    pub fn get(
        &self,
        id: u64,
    ) -> Option<Rc<RequestResource>> {
        self.entries.get(&id).cloned()
    }

    pub fn take(
        &mut self,
        id: u64,
    ) -> Option<Rc<RequestResource>> {
        self.entries.remove(&id)
    }
}

#[cfg(test)]
#[path = "../../tests/application/request.rs"]
mod tests;
