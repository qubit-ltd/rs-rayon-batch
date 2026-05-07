/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`RayonBatchExecutorBuildError`](qubit_rayon_batch::RayonBatchExecutorBuildError).

use qubit_rayon_batch::{
    RayonBatchExecutor,
    RayonBatchExecutorBuildError,
};

#[test]
fn test_rayon_batch_executor_build_error_formats_zero_thread_count() {
    let error = RayonBatchExecutor::builder()
        .num_threads(0)
        .build()
        .err()
        .expect("zero thread count should fail");

    assert!(matches!(
        error,
        RayonBatchExecutorBuildError::ZeroThreadCount
    ));
    assert_eq!(
        error.to_string(),
        "rayon batch executor thread count must be greater than zero"
    );
}

#[test]
fn test_rayon_batch_executor_build_error_formats_zero_stack_size() {
    let error = RayonBatchExecutor::builder()
        .stack_size(0)
        .build()
        .err()
        .expect("zero stack size should fail");

    assert!(matches!(error, RayonBatchExecutorBuildError::ZeroStackSize));
    assert_eq!(
        error.to_string(),
        "rayon batch executor worker stack size must be greater than zero"
    );
}
