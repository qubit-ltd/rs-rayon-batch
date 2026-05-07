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
};

use qubit_progress::{
    ProgressEvent,
    ProgressPhase,
    ProgressReporter,
};

/// Progress callback that should panic during a test.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressPanicPhase {
    /// Panic from a started progress event.
    Start,
    /// Panic from a running progress event.
    Process,
    /// Panic from a finished progress event.
    Finish,
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
    fn report(&self, event: &ProgressEvent) {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
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
    fn report(&self, event: &ProgressEvent) {
        match event.phase() {
            ProgressPhase::Started => self.panic_if_configured(ProgressPanicPhase::Start),
            ProgressPhase::Running => self.panic_if_configured(ProgressPanicPhase::Process),
            ProgressPhase::Finished => self.panic_if_configured(ProgressPanicPhase::Finish),
            ProgressPhase::Failed | ProgressPhase::Canceled => {}
        }
    }
}
