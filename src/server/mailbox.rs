//! One ordered inbox: HTTP carries application admission; controls reserve space.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

pub(super) struct Sender<T> {
    sender: mpsc::UnboundedSender<Message<T>>,
    controls: Arc<Semaphore>,
}

pub(super) struct Receiver<T> {
    receiver: mpsc::UnboundedReceiver<Message<T>>,
    controls: Arc<Semaphore>,
}

struct Message<T> {
    command: T,
    control: Option<OwnedSemaphorePermit>,
}

pub(super) fn channel<T>(control_capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(control_capacity > 0, "control mailbox capacity must be positive");
    let (sender, receiver) = mpsc::unbounded_channel();
    let controls = Arc::new(Semaphore::new(control_capacity));
    (
        Sender { sender, controls: Arc::clone(&controls) },
        Receiver { receiver, controls },
    )
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            controls: Arc::clone(&self.controls),
        }
    }
}

impl<T> Sender<T> {
    /// The command must own the pool's admission permit until guest completion.
    pub(super) fn send_admitted(
        &self,
        command: T,
    ) -> Result<(), mpsc::error::SendError<T>> {
        self.sender
            .send(Message { command, control: None })
            .map_err(|error| mpsc::error::SendError(error.0.command))
    }

    /// Capacity is held only until the owner takes the command from its inbox.
    pub(super) async fn send_control(
        &self,
        command: T,
    ) -> Result<(), mpsc::error::SendError<T>> {
        let control = match Arc::clone(&self.controls).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return Err(mpsc::error::SendError(command)),
        };
        self.sender
            .send(Message { command, control: Some(control) })
            .map_err(|error| mpsc::error::SendError(error.0.command))
    }
}

impl<T> Receiver<T> {
    pub(super) async fn recv(&mut self) -> Option<T> {
        let Message { command, control } = self.receiver.recv().await?;
        drop(control);
        Some(command)
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // A control sender waiting for space must observe owner loss as well.
        self.controls.close();
    }
}

#[cfg(test)]
#[path = "../../tests/server/mailbox.rs"]
mod tests;
