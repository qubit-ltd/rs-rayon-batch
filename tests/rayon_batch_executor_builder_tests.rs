// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for Rayon batch executor configuration.

use qubit_rayon_batch::RayonBatchExecutor;

#[test]
fn test_builder_uses_configured_thread_count() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(1)
        .build()
        .expect("configuration with one worker should build");
    assert_eq!(
        executor.thread_count(),
        1,
        "the executor should retain the configured worker count"
    );
}
