// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{panic::panic_any, sync::Mutex};

use qubit_progress::{Event, Phase, Reporter};

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
    events: Mutex<Vec<Event>>,
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
    pub fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

impl Reporter for RecordingProgressReporter {
    fn report(&self, event: &Event) -> Result<(), qubit_progress::ReporterError> {
        self.events
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(event.clone());
        Ok(())
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

impl Reporter for PanickingProgressReporter {
    fn report(&self, event: &Event) -> Result<(), qubit_progress::ReporterError> {
        match event.phase() {
            Phase::Started => self.panic_if_configured(ProgressPanicPhase::Start),
            Phase::Running => self.panic_if_configured(ProgressPanicPhase::Process),
            Phase::Succeeded => self.panic_if_configured(ProgressPanicPhase::Finish),
            Phase::Failed | Phase::Cancelled => {}
        }
        Ok(())
    }
}
