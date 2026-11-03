// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Applies the same public contract to sequential, scoped, and Rayon backends.

use std::num::NonZeroUsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

mod internal;
mod watchdog;

use internal::assert_backend_contract;
use internal::assert_failure_policy_contract;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_rayon_batch::RayonBatchExecutor;
use watchdog::run_with_watchdog;

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
    const CHILD: &str = "QUBIT_RAYON_BACKEND_FAILURE_POLICY_CHILD";
    if std::env::var_os(CHILD).is_none() {
        run_with_watchdog(
            "test_backends_share_cooperative_failure_policy",
            CHILD,
            "backend cooperative failure policy",
        );
        return;
    }

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

#[test]
fn test_rayon_stop_after_failures_preserves_partial_results() {
    const COUNT: usize = 4096;
    for limit in [2usize, 3] {
        for (workers, threshold) in [(2, 0), (2, usize::MAX), (1, 0)] {
            let executor = RayonBatchExecutor::builder()
                .thread_count(workers)
                .sequential_threshold(threshold)
                .task_failure_policy(TaskFailurePolicy::StopAfterFailures(
                    NonZeroUsize::new(limit).expect("positive limit"),
                ))
                .build()
                .expect("executor should build");
            let sequential = workers == 1 || threshold == usize::MAX;
            let executed = AtomicUsize::new(0);
            let source = (0..COUNT).map(|index| {
                let executed = &executed;
                move || {
                    executed.fetch_add(1, Ordering::AcqRel);
                    match index % 3 {
                        0 => Ok::<(), &'static str>(()),
                        1 => Err("task error"),
                        _ => panic!("task panic contributes to policy"),
                    }
                }
            });
            let outcome = executor
                .execute_with_count(source, COUNT)
                .expect("policy stop should return an outcome");
            assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
            assert_eq!(outcome.completed_count(), executed.load(Ordering::Acquire));
            assert!(outcome.completed_count() < COUNT);
            assert!(outcome.failed_count() > 0);
            assert!(outcome.panicked_count() > 0);
            if sequential {
                assert_eq!(outcome.failure_count(), limit);
            } else {
                assert!(outcome.failure_count() >= limit);
            }

            let executed = AtomicUsize::new(0);
            let source = (0..COUNT).map(|index| {
                let executed = &executed;
                move || {
                    executed.fetch_add(1, Ordering::AcqRel);
                    match index % 3 {
                        0 => Ok::<usize, &'static str>(index),
                        1 => Err("callable error"),
                        _ => panic!("callable panic contributes to policy"),
                    }
                }
            });
            let result = executor
                .call_with_count(source, COUNT)
                .expect("policy stop should preserve outputs");
            let outcome = result.outcome();
            assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
            assert_eq!(outcome.completed_count(), executed.load(Ordering::Acquire));
            assert!(outcome.completed_count() < COUNT);
            assert!(outcome.failed_count() > 0);
            assert!(outcome.panicked_count() > 0);
            assert_eq!(result.outputs().len(), outcome.succeeded_count());
            assert!(!result.outputs().is_empty());
            if sequential {
                assert_eq!(outcome.failure_count(), limit);
            } else {
                assert!(outcome.failure_count() >= limit);
            }
            for output in result.outputs() {
                assert_eq!(output.index(), *output.value());
                assert_eq!(output.index() % 3, 0);
                assert!(
                    !outcome
                        .failures()
                        .iter()
                        .any(|failure| failure.index() == output.index())
                );
            }
            assert!(
                result
                    .outputs()
                    .windows(2)
                    .all(|pair| pair[0].index() < pair[1].index())
            );
        }
    }
}
