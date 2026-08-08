// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::time::Duration;

/// Default interval between progress callbacks.
pub(crate) const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

/// Default maximum size that still uses sequential execution.
pub(crate) const DEFAULT_SEQUENTIAL_THRESHOLD: usize = 100;

/// Default prefix used for Rayon worker thread names.
pub(crate) const DEFAULT_THREAD_NAME_PREFIX: &str = "qubit-rayon-batch";
