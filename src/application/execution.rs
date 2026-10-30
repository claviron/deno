//! One request's completion, confined to its isolate owner thread.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

type Outcome = Result<(), String>;

struct State {
    result: Cell<Option<Outcome>>,
    waker: Cell<Option<Waker>>,
    receiving: Cell<bool>,
}

pub(crate) struct ExecutionSender(Option<Rc<State>>);

pub(crate) struct ExecutionReceiver(Rc<State>);

pub(crate) fn execution_completion() -> (ExecutionSender, ExecutionReceiver) {
    let state = Rc::new(State {
        result: Cell::new(None),
        waker: Cell::new(None),
        receiving: Cell::new(true),
    });
    (ExecutionSender(Some(Rc::clone(&state))), ExecutionReceiver(state))
}

impl State {
    fn complete(
        &self,
        result: Outcome,
    ) {
        if self.receiving.get() {
            self.result.set(Some(result));
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }

    fn take_result(&self) -> Option<Outcome> {
        let result = self.result.take()?;
        self.receiving.set(false);
        self.waker.take();
        Some(result)
    }
}

impl ExecutionSender {
    pub(crate) fn complete(
        mut self,
        result: Outcome,
    ) {
        if let Some(state) = self.0.take() {
            state.complete(result);
        }
    }
}

impl Drop for ExecutionSender {
    fn drop(&mut self) {
        if let Some(state) = self.0.take() {
            state.complete(Err(
                "JavaScript request execution disappeared".into()
            ));
        }
    }
}

impl Future for ExecutionReceiver {
    type Output = Outcome;

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Outcome> {
        let state = &self.0;
        assert!(state.receiving.get(), "execution polled after completion");
        if let Some(result) = state.take_result() {
            return Poll::Ready(result);
        }
        let waker = match state.waker.take() {
            Some(waker) if waker.will_wake(cx.waker()) => waker,
            _ => cx.waker().clone(),
        };
        state.waker.set(Some(waker));
        // Waker cloning/dropping may call user code. Keep no borrow across it
        // and observe completion even if it happened during registration.
        state.take_result().map_or(Poll::Pending, Poll::Ready)
    }
}

impl Drop for ExecutionReceiver {
    fn drop(&mut self) {
        self.0.receiving.set(false);
        self.0.waker.take();
        self.0.result.take();
    }
}

#[cfg(test)]
#[path = "../../tests/application/execution.rs"]
mod tests;
