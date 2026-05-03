/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    panic::panic_any,
    sync::Mutex,
    time::Duration,
};

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
