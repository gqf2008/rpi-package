//! Cancellation token shared between a tool's `cancel`/`destroy`, its worker
//! thread, and — for `dispatch` — the per-node workers it starts.
//!
//! A tool call runs on a worker thread so `poll` can stay non-blocking. The host
//! signals cancellation through the ABI `cancel` entry point, which flips this
//! flag; the worker observes it and kills whatever child process it is waiting
//! on. A node token can additionally be cancelled on its own, so a mid-wave
//! `plan cancel` can stop one node without stopping the whole call.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct Control {
    primary: Arc<AtomicBool>,
    /// Cancelled whenever the owning call is; checked alongside `primary`.
    linked: Option<Arc<AtomicBool>>,
}

impl Control {
    pub fn cancel(&self) {
        self.primary.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.primary.load(Ordering::Acquire)
            || self
                .linked
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Acquire))
    }

    /// `Err` once cancelled — the worker's cheap checkpoint.
    pub fn check(&self) -> Result<(), String> {
        if self.is_cancelled() {
            Err("cancelled".into())
        } else {
            Ok(())
        }
    }

    /// A token sharing `flag`, so a caller can expose one cancellation signal to
    /// many holders.
    pub fn from_flag(flag: Arc<AtomicBool>) -> Control {
        Control {
            primary: flag,
            linked: None,
        }
    }

    /// A token cancelled by its own [`cancel`](Self::cancel) *or* whenever
    /// `parent` is cancelled.
    pub fn child_of(parent: &Control) -> Control {
        Control {
            primary: Arc::new(AtomicBool::new(false)),
            linked: Some(Arc::clone(&parent.primary)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_child_is_cancelled_by_its_parent_or_on_its_own() {
        let parent = Control::default();
        let child = Control::child_of(&parent);
        assert!(!child.is_cancelled());
        assert!(!parent.is_cancelled());

        parent.cancel();
        assert!(child.is_cancelled(), "parent cancel reaches the child");

        // The reverse does not hold: cancelling a node token leaves the call alive.
        let parent = Control::default();
        let child = Control::child_of(&parent);
        child.cancel();
        assert!(child.is_cancelled());
        assert!(!parent.is_cancelled());
    }
}
