// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for Rayon batch defaults.

#[test]
fn default_thread_name_prefix_is_stable() {
    let executor = qubit_rayon_batch::RayonBatchExecutor::builder()
        .thread_count(1)
        .build()
        .expect("default configuration should build");
    assert_eq!(executor.thread_count(), 1);
}
