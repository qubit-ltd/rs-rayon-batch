// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{
    panic::panic_any,
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    thread,
    time::Duration,
};

use qubit_function::Runnable;

/// Test task behavior used to keep executor coverage in one monomorphization.
#[derive(Debug, Clone)]
enum TestTaskAction {
    /// Complete successfully without extra side effects.
    Succeed,
    /// Increment the supplied counter, then succeed.
    CountSuccess {
        /// Counter incremented by this task.
        counter: Arc<AtomicUsize>,
    },
    /// Return a task error.
    Fail {
        /// Error returned by the task.
        error: &'static str,
    },
    /// Sleep for the supplied duration, then return a task error.
    FailAfterSleep {
        /// Error returned by the task.
        error: &'static str,
        /// Sleep duration.
        duration: Duration,
    },
    /// Panic with an owned string payload while running.
    PanicString {
        /// Panic message.
        message: &'static str,
    },
    /// Panic with a non-string payload while running.
    PanicUsize {
        /// Panic payload.
        payload: usize,
    },
    /// Sleep for the supplied duration, then succeed.
    SleepSuccess {
        /// Sleep duration.
        duration: Duration,
    },
    /// Track active concurrency while sleeping, then succeed.
    TrackConcurrency {
        /// Active task counter.
        active: Arc<AtomicUsize>,
        /// Maximum observed active task count.
        max_active: Arc<AtomicUsize>,
        /// Sleep duration.
        duration: Duration,
    },
}

/// Configurable runnable task for executor tests.
#[derive(Debug, Clone)]
pub struct TestTask {
    /// Behavior executed by this task.
    action: TestTaskAction,
}

impl TestTask {
    /// Creates a task that succeeds.
    ///
    /// # Returns
    ///
    /// A successful test task.
    pub const fn succeed() -> Self {
        Self {
            action: TestTaskAction::Succeed,
        }
    }

    /// Creates a task that increments `counter` and succeeds.
    ///
    /// # Parameters
    ///
    /// * `counter` - Counter incremented by the task.
    ///
    /// # Returns
    ///
    /// A counting successful test task.
    pub fn count_success(counter: Arc<AtomicUsize>) -> Self {
        Self {
            action: TestTaskAction::CountSuccess { counter },
        }
    }

    /// Creates a task that fails with `error`.
    ///
    /// # Parameters
    ///
    /// * `error` - Error returned by the task.
    ///
    /// # Returns
    ///
    /// A failing test task.
    pub const fn fail(error: &'static str) -> Self {
        Self {
            action: TestTaskAction::Fail { error },
        }
    }

    /// Creates a task that sleeps before failing with `error`.
    ///
    /// # Parameters
    ///
    /// * `error` - Error returned by the task.
    /// * `duration` - Sleep duration before returning the error.
    ///
    /// # Returns
    ///
    /// A delayed failing test task.
    pub const fn fail_after_sleep(
        error: &'static str,
        duration: Duration,
    ) -> Self {
        Self {
            action: TestTaskAction::FailAfterSleep { error, duration },
        }
    }

    /// Creates a task that panics with an owned `String` payload.
    ///
    /// # Parameters
    ///
    /// * `message` - Panic message.
    ///
    /// # Returns
    ///
    /// A panicking test task.
    pub const fn panic_string(message: &'static str) -> Self {
        Self {
            action: TestTaskAction::PanicString { message },
        }
    }

    /// Creates a task that panics with a non-string payload.
    ///
    /// # Parameters
    ///
    /// * `payload` - Panic payload.
    ///
    /// # Returns
    ///
    /// A panicking test task.
    pub const fn panic_usize(payload: usize) -> Self {
        Self {
            action: TestTaskAction::PanicUsize { payload },
        }
    }

    /// Creates a task that sleeps and then succeeds.
    ///
    /// # Parameters
    ///
    /// * `duration` - Sleep duration.
    ///
    /// # Returns
    ///
    /// A sleeping successful test task.
    pub const fn sleep_success(duration: Duration) -> Self {
        Self {
            action: TestTaskAction::SleepSuccess { duration },
        }
    }

    /// Creates a task that tracks active concurrency while sleeping.
    ///
    /// # Parameters
    ///
    /// * `active` - Active task counter.
    /// * `max_active` - Maximum active task counter.
    /// * `duration` - Sleep duration.
    ///
    /// # Returns
    ///
    /// A concurrency-tracking successful task.
    pub fn track_concurrency(
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
        duration: Duration,
    ) -> Self {
        Self {
            action: TestTaskAction::TrackConcurrency {
                active,
                max_active,
                duration,
            },
        }
    }
}

impl Runnable<&'static str> for TestTask {
    /// Runs this configured test task.
    ///
    /// # Returns
    ///
    /// `Ok(())` for successful actions, or `Err(&'static str)` for
    /// [`TestTaskAction::Fail`].
    ///
    /// # Panics
    ///
    /// Panics when configured with one of the panic actions.
    fn run(&mut self) -> Result<(), &'static str> {
        match &self.action {
            TestTaskAction::Succeed => Ok(()),
            TestTaskAction::CountSuccess { counter } => {
                counter.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            TestTaskAction::Fail { error } => Err(*error),
            TestTaskAction::FailAfterSleep { error, duration } => {
                thread::sleep(*duration);
                Err(*error)
            }
            TestTaskAction::PanicString { message } => {
                panic_any((*message).to_owned())
            }
            TestTaskAction::PanicUsize { payload } => panic_any(*payload),
            TestTaskAction::SleepSuccess { duration } => {
                thread::sleep(*duration);
                Ok(())
            }
            TestTaskAction::TrackConcurrency {
                active,
                max_active,
                duration,
            } => {
                let current = active.fetch_add(1, Ordering::AcqRel) + 1;
                update_max(max_active, current);
                thread::sleep(*duration);
                active.fetch_sub(1, Ordering::AcqRel);
                Ok(())
            }
        }
    }
}

/// Updates `max_active` when `current` is greater than the stored value.
///
/// # Parameters
///
/// * `max_active` - Atomic maximum value to update.
/// * `current` - Candidate active count.
fn update_max(max_active: &AtomicUsize, current: usize) {
    let mut observed = max_active.load(Ordering::Acquire);
    while current > observed {
        match max_active.compare_exchange(
            observed,
            current,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(value) => observed = value,
        }
    }
}
