//! Cancellation token shared between a tool's `cancel`/`destroy` and its worker
//! thread.
//!
//! A tool call runs on a worker thread so `poll` can stay non-blocking. The host
//! signals cancellation through the ABI `cancel` entry point, which flips this
//! flag; the worker observes it and kills whatever child process it is waiting
//! on.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
pub struct Control(Arc<AtomicBool>);

impl Control {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    /// `Err` once cancelled — the worker's cheap checkpoint.
    pub fn check(&self) -> Result<(), String> {
        if self.is_cancelled() {
            Err("cancelled".into())
        } else {
            Ok(())
        }
    }
}
