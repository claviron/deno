use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Wake;

use super::*;

#[derive(Default)]
struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn poll(
    receiver: &mut ExecutionReceiver,
    waker: &Waker,
) -> Poll<Outcome> {
    Pin::new(receiver).poll(&mut Context::from_waker(waker))
}

#[test]
fn completion_before_first_poll_preserves_success_and_failure() {
    for expected in [Ok(()), Err("handler failed".into())] {
        let (sender, mut receiver) = execution_completion();
        sender.complete(expected.clone());
        assert_eq!(poll(&mut receiver, Waker::noop()), Poll::Ready(expected));
    }
}

#[test]
fn completion_wakes_only_its_own_waiter() {
    let (first, mut first_receiver) = execution_completion();
    let (second, mut second_receiver) = execution_completion();
    let first_count = Arc::new(WakeCount::default());
    let second_count = Arc::new(WakeCount::default());
    let first_waker = Waker::from(Arc::clone(&first_count));
    let second_waker = Waker::from(Arc::clone(&second_count));
    assert!(poll(&mut first_receiver, &first_waker).is_pending());
    assert!(poll(&mut second_receiver, &second_waker).is_pending());

    first.complete(Ok(()));
    assert_eq!(first_count.0.load(Ordering::Relaxed), 1);
    assert_eq!(second_count.0.load(Ordering::Relaxed), 0);
    assert_eq!(poll(&mut first_receiver, &first_waker), Poll::Ready(Ok(())));
    assert!(poll(&mut second_receiver, &second_waker).is_pending());
    second.complete(Ok(()));
    assert_eq!(poll(&mut second_receiver, &second_waker), Poll::Ready(Ok(())));
}

#[test]
fn sender_drop_wakes_a_waiter_with_an_execution_error() {
    let (sender, mut receiver) = execution_completion();
    let count = Arc::new(WakeCount::default());
    let waker = Waker::from(Arc::clone(&count));
    assert!(poll(&mut receiver, &waker).is_pending());
    drop(sender);
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        poll(&mut receiver, &waker),
        Poll::Ready(Err("JavaScript request execution disappeared".into()))
    );
}

#[test]
fn sender_drop_before_first_poll_is_not_a_lost_notification() {
    let (sender, mut receiver) = execution_completion();
    drop(sender);
    assert!(matches!(poll(&mut receiver, Waker::noop()), Poll::Ready(Err(_))));
}

#[test]
fn receiver_drop_releases_its_waker_before_the_sender_finishes() {
    let (sender, mut receiver) = execution_completion();
    let count = Arc::new(WakeCount::default());
    let retained = Arc::downgrade(&count);
    let waker = Waker::from(count);
    assert!(poll(&mut receiver, &waker).is_pending());
    drop(waker);
    assert!(retained.upgrade().is_some());
    drop(receiver);
    assert!(retained.upgrade().is_none());
    sender.complete(Ok(()));
}

#[test]
fn moving_a_wait_to_another_task_replaces_its_waker() {
    let (sender, mut receiver) = execution_completion();
    let previous = Arc::new(WakeCount::default());
    let current = Arc::new(WakeCount::default());
    assert!(
        poll(&mut receiver, &Waker::from(Arc::clone(&previous))).is_pending()
    );
    assert!(
        poll(&mut receiver, &Waker::from(Arc::clone(&current))).is_pending()
    );
    sender.complete(Ok(()));
    assert_eq!(previous.0.load(Ordering::Relaxed), 0);
    assert_eq!(current.0.load(Ordering::Relaxed), 1);
    assert_eq!(poll(&mut receiver, Waker::noop()), Poll::Ready(Ok(())));
}
