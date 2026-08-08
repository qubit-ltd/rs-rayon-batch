// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for Rayon scheduler errors.

use qubit_rayon_batch::RayonBatchScheduleError;

#[test]
fn scheduler_error_has_stable_display() {
    assert_eq!(
        RayonBatchScheduleError::WorkChannelDisconnected.to_string(),
        "Rayon batch work channel disconnected"
    );
}
