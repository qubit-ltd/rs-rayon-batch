// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Applies the same public contract to sequential, scoped, and Rayon backends.

mod internal;

use internal::assert_backend_contract;
use internal::assert_failure_policy_contract;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_rayon_batch::RayonBatchExecutor;

/// Includes both forced parallel paths and their small-batch fallbacks.
#[test]
fn test_backends_share_execution_and_output_contracts() {
    assert_backend_contract(&SequentialBatchExecutor::new());
    for threshold in [0, 100] {
        assert_backend_contract(
            &ParallelBatchExecutor::builder()
                .thread_count(2)
                .sequential_threshold(threshold)
                .build()
                .expect("standard executor must build"),
        );
        assert_backend_contract(
            &RayonBatchExecutor::builder()
                .thread_count(2)
                .sequential_threshold(threshold)
                .build()
                .expect("Rayon executor must build"),
        );
    }
}

/// Compares policies while allowing already accepted parallel tasks to drain.
#[test]
fn test_backends_share_cooperative_failure_policy() {
    let policy = TaskFailurePolicy::StopOnFirstFailure;
    assert_failure_policy_contract(&SequentialBatchExecutor::builder().task_failure_policy(policy).build());
    for threshold in [0, usize::MAX] {
        assert_failure_policy_contract(
            &ParallelBatchExecutor::builder()
                .thread_count(2)
                .sequential_threshold(threshold)
                .task_failure_policy(policy)
                .build()
                .expect("standard executor must build"),
        );
        assert_failure_policy_contract(
            &RayonBatchExecutor::builder()
                .thread_count(2)
                .sequential_threshold(threshold)
                .task_failure_policy(policy)
                .build()
                .expect("Rayon executor must build"),
        );
    }
}
