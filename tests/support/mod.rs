// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared test support for `qubit-rayon-batch`.

mod failing_progress_reporter;
mod panic_payload;
mod progress_reporter;
mod test_callable;
mod test_task;

pub use failing_progress_reporter::FailingProgressReporter;
pub use panic_payload::panic_payload_message;
pub use progress_reporter::{
    PanickingProgressReporter, ProgressPanicPhase, RecordingProgressReporter,
};
pub use test_callable::TestCallable;
pub use test_task::TestTask;
