use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::channel;

#[derive(Debug)]
enum Command {
    Http(OwnedSemaphorePermit),
    Control(u8),
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}

#[tokio::test]
async fn admitted_http_bypasses_full_control_queue_and_retains_admission() {
    let (sender, mut receiver) = channel(1);
    let admission = Arc::new(Semaphore::new(2));
    sender.send_control(Command::Control(1)).await.unwrap();
    for _ in 0..2 {
        let permit = Arc::clone(&admission).try_acquire_owned().unwrap();
        sender.send_admitted(Command::Http(permit)).unwrap();
    }
    assert!(admission.try_acquire().is_err());
    assert!(matches!(receiver.recv().await, Some(Command::Control(1))));

    let Some(Command::Http(first)) = receiver.recv().await else {
        panic!("first admitted request was not delivered");
    };
    let Some(Command::Http(second)) = receiver.recv().await else {
        panic!("second admitted request was not delivered");
    };
    // Dequeuing starts guest work; it must not admit another HTTP request.
    assert!(admission.try_acquire().is_err());
    drop(first);
    assert_eq!(admission.available_permits(), 1);
    drop(second);
    assert_eq!(admission.available_permits(), 2);
}

#[tokio::test]
async fn controls_wait_for_dequeue_and_share_http_fifo_order() {
    let (sender, mut receiver) = channel(1);
    let admission = Arc::new(Semaphore::new(1));
    sender.send_control(Command::Control(1)).await.unwrap();
    let mut waiting = std::pin::pin!(sender.send_control(Command::Control(2)));
    assert_pending(waiting.as_mut()).await;

    let permit = admission.try_acquire_owned().unwrap();
    sender.send_admitted(Command::Http(permit)).unwrap();
    assert!(matches!(receiver.recv().await, Some(Command::Control(1))));
    waiting.await.unwrap();
    assert!(matches!(receiver.recv().await, Some(Command::Http(_))));
    assert!(matches!(receiver.recv().await, Some(Command::Control(2))));
}

#[tokio::test]
async fn owner_loss_wakes_control_waiters_and_releases_queued_admission() {
    let (sender, receiver) = channel(1);
    let admission = Arc::new(Semaphore::new(1));
    sender.send_control(Command::Control(1)).await.unwrap();
    let permit = Arc::clone(&admission).try_acquire_owned().unwrap();
    sender.send_admitted(Command::Http(permit)).unwrap();
    let mut waiting = std::pin::pin!(sender.send_control(Command::Control(2)));
    assert_pending(waiting.as_mut()).await;

    drop(receiver);
    assert!(matches!(waiting.await.unwrap_err().0, Command::Control(2)));
    assert_eq!(admission.available_permits(), 1);

    let permit = Arc::clone(&admission).try_acquire_owned().unwrap();
    drop(sender.send_admitted(Command::Http(permit)).unwrap_err());
    assert_eq!(admission.available_permits(), 1);
}

#[tokio::test]
async fn cancelled_control_reservation_does_not_consume_capacity() {
    let (sender, mut receiver) = channel(1);
    sender.send_control(Command::Control(1)).await.unwrap();
    {
        let mut waiting =
            std::pin::pin!(sender.send_control(Command::Control(2)));
        assert_pending(waiting.as_mut()).await;
    }
    assert!(matches!(receiver.recv().await, Some(Command::Control(1))));
    sender.send_control(Command::Control(3)).await.unwrap();
    assert!(matches!(receiver.recv().await, Some(Command::Control(3))));
}
