/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Shared test support for `qubit-rayon-batch`.

use std::{
    any::Any,
    panic::panic_any,
    sync::{
        Arc,
        Mutex,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    thread,
    time::Duration,
};

use qubit_function::Runnable;

use qubit_batch::ProgressReporter;

/// Progress callback that should panic during a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressPanicPhase {
    /// Panic from [`ProgressReporter::start`].
    Start,
    /// Panic from [`ProgressReporter::process`].
    Process,
    /// Panic from [`ProgressReporter::finish`].
    Finish,
}

/// Recorded progress event produced by a test reporter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProgressEvent {
    /// Batch start notification.
    Start {
        /// Declared task count.
        total_count: usize,
    },
    /// In-flight progress notification.
    Process {
        /// Declared task count.
        total_count: usize,
        /// Number of active tasks at callback time.
        active_count: usize,
        /// Number of completed tasks at callback time.
        completed_count: usize,
        /// Elapsed time since batch start.
        elapsed: Duration,
    },
    /// Batch finish notification.
    Finish {
        /// Declared task count.
        total_count: usize,
        /// Total elapsed time.
        elapsed: Duration,
    },
}

/// Progress reporter that records all callbacks in memory.
#[derive(Debug, Default)]
pub struct RecordingProgressReporter {
    /// Recorded lifecycle events.
    events: Mutex<Vec<ProgressEvent>>,
}

impl RecordingProgressReporter {
    /// Creates an empty recording reporter.
    ///
    /// # Returns
    ///
    /// A recording reporter with no stored events.
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a snapshot of all recorded progress events.
    ///
    /// # Returns
    ///
    /// A cloned list of progress events in callback order.
    pub fn events(&self) -> Vec<ProgressEvent> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl ProgressReporter for RecordingProgressReporter {
    fn start(&self, total_count: usize) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProgressEvent::Start { total_count });
    }

    fn process(
        &self,
        total_count: usize,
        active_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProgressEvent::Process {
                total_count,
                active_count,
                completed_count,
                elapsed,
            });
    }

    fn finish(&self, total_count: usize, elapsed: Duration) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(ProgressEvent::Finish {
                total_count,
                elapsed,
            });
    }
}

/// Progress reporter that panics from one configured lifecycle callback.
#[derive(Debug, Clone, Copy)]
pub struct PanickingProgressReporter {
    /// Callback phase that should panic.
    phase: ProgressPanicPhase,
    /// Panic payload message.
    message: &'static str,
}

impl PanickingProgressReporter {
    /// Creates a reporter that panics from `phase`.
    ///
    /// # Parameters
    ///
    /// * `phase` - Callback phase that should panic.
    /// * `message` - Panic payload message.
    ///
    /// # Returns
    ///
    /// A panicking progress reporter.
    pub const fn new(phase: ProgressPanicPhase, message: &'static str) -> Self {
        Self { phase, message }
    }

    /// Panics when `phase` matches this reporter's configured phase.
    ///
    /// # Parameters
    ///
    /// * `phase` - Current callback phase.
    ///
    /// # Panics
    ///
    /// Panics with this reporter's configured message when `phase` matches.
    fn panic_if_configured(&self, phase: ProgressPanicPhase) {
        if self.phase == phase {
            panic_any(self.message);
        }
    }
}

impl ProgressReporter for PanickingProgressReporter {
    fn start(&self, _total_count: usize) {
        self.panic_if_configured(ProgressPanicPhase::Start);
    }

    fn process(
        &self,
        _total_count: usize,
        _active_count: usize,
        _completed_count: usize,
        _elapsed: Duration,
    ) {
        self.panic_if_configured(ProgressPanicPhase::Process);
    }

    fn finish(&self, _total_count: usize, _elapsed: Duration) {
        self.panic_if_configured(ProgressPanicPhase::Finish);
    }
}

/// Extracts a string message from a panic payload.
///
/// # Parameters
///
/// * `payload` - Panic payload captured by `catch_unwind`.
///
/// # Returns
///
/// `Some(message)` for `&'static str` and `String` payloads, or `None` for
/// other payload types.
pub fn panic_payload_message(payload: &(dyn Any + Send)) -> Option<&str> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some(*message)
    } else {
        payload.downcast_ref::<String>().map(String::as_str)
    }
}

/// Test task behavior used to keep executor coverage in one monomorphization.
#[derive(Debug, Clone)]
pub enum TestTaskAction {
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
    pub const fn fail_after_sleep(error: &'static str, duration: Duration) -> Self {
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
    /// Panics when configured with [`TestTaskAction::Panic`].
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
            TestTaskAction::PanicString { message } => panic_any((*message).to_owned()),
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
        match max_active.compare_exchange(observed, current, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(value) => observed = value,
        }
    }
}
