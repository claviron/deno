use super::*;

fn resource(reply: ReplySender) -> RequestResource {
    RequestResource {
        has_input: false,
        request_body: RefCell::new(None),
        input: RefCell::new(None),
        reply: RefCell::new(Some(reply)),
        output: RefCell::new(None),
        completion: RefCell::new(None),
        lifetime: RequestLifetime::default(),
        closed: Cell::new(false),
    }
}

#[tokio::test]
async fn request_handles_do_not_reuse_or_close_another_request() {
    let mut resources = RequestResources::default();
    let (first, first_reply) = oneshot::channel();
    let (second, mut second_reply) = oneshot::channel();
    let first = resources.insert(resource(first)).unwrap();
    let second = resources.insert(resource(second)).unwrap();
    resources.take(first).unwrap().finish(Err("closed".into()));
    assert!(first_reply.await.unwrap().is_err());
    assert!(matches!(
        second_reply.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    let (third, _) = oneshot::channel();
    let third = resources.insert(resource(third)).unwrap();
    assert_ne!(first, third);
    assert!(resources.get(first).is_none());
    assert!(!resources.get(second).unwrap().closed.get());
}

#[tokio::test]
async fn producer_failure_is_reported_after_its_queued_bytes() {
    let (reply, _) = oneshot::channel();
    let request = resource(reply);
    let (sender, receiver) = mpsc::channel(1);
    let (completion, finished) = oneshot::channel();
    sender.send(Bytes::from_static(b"prefix")).await.unwrap();
    *request.output.borrow_mut() = Some(sender);
    *request.completion.borrow_mut() = Some(completion);
    let mut body = ResponseBody::Stream(ResponseStream::new(
        receiver,
        finished,
        request.lifetime.cancellation.clone(),
    ));
    request.finish(Err("producer failed".into()));
    assert_eq!(body.recv().await.unwrap().unwrap(), "prefix");
    assert_eq!(body.recv().await.unwrap().unwrap_err(), "producer failed");
    assert!(body.recv().await.is_none());
}

#[tokio::test]
async fn dropping_one_response_stream_cancels_only_its_producer() {
    let first = CancellationToken::new();
    let second = CancellationToken::new();
    let (_sender, receiver) = mpsc::channel(1);
    let (_completion, finished) = oneshot::channel();
    drop(ResponseStream::new(receiver, finished, first.clone()));
    assert!(first.is_cancelled());
    assert!(!second.is_cancelled());
}

#[cfg(feature = "server")]
#[tokio::test]
async fn admission_outlives_the_response_head_and_releases_with_execution() {
    let admission = std::sync::Arc::new(tokio::sync::Semaphore::new(1));
    let counters = std::sync::Arc::new(RequestCounters::default());
    counters.active.store(1, std::sync::atomic::Ordering::Relaxed);
    let (reply, replied) = oneshot::channel();
    let mut request = resource(reply);
    request.lifetime._admission =
        Some(admission.clone().acquire_owned().await.unwrap());
    request.lifetime.counters = Some(counters.clone());
    request
        .reply
        .borrow_mut()
        .take()
        .unwrap()
        .send(Ok(crate::ApplicationResponse::new(ResponseBody::Whole(
            Bytes::new(),
        ))))
        .unwrap();
    replied.await.unwrap().unwrap();
    assert_eq!(admission.available_permits(), 0);
    request.finish(Ok(()));
    drop(request);
    assert_eq!(admission.available_permits(), 1);
    assert_eq!(counters.active.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(counters.served.load(std::sync::atomic::Ordering::Relaxed), 1);
}

#[test]
fn request_handle_exhaustion_never_wraps_into_an_existing_request() {
    let mut resources =
        RequestResources { next: (1_u64 << 53) - 1, ..Default::default() };
    let (reply, _) = oneshot::channel();
    let last = resources.insert(resource(reply)).unwrap();
    let (reply, _) = oneshot::channel();
    assert!(resources.insert(resource(reply)).is_err());
    assert!(resources.get(last).is_some());
    assert_eq!(resources.next, 1_u64 << 53);
}
