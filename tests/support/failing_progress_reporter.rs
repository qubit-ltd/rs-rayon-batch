// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_progress::Event;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;

/// Progress reporter that fails after a configured number of successful calls.
pub struct FailingProgressReporter {
    successful_calls: usize,
    call_count: AtomicUsize,
}

impl FailingProgressReporter {
    /// Creates a reporter that fails after `successful_calls` reports.
    pub const fn after_successes(successful_calls: usize) -> Self {
        Self {
            successful_calls,
            call_count: AtomicUsize::new(0),
        }
    }
}

impl Reporter for FailingProgressReporter {
    /// Accepts the configured prefix, then returns a synthetic I/O error.
    fn report(&self, _event: &Event) -> Result<(), ReporterError> {
        let call_index = self.call_count.fetch_add(1, Ordering::AcqRel);
        if call_index < self.successful_calls {
            Ok(())
        } else {
            Err(ReporterError::new(io::Error::other(
                "synthetic progress report failure",
            )))
        }
    }
}
