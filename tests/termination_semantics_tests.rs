// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Failure-policy termination describes source observation, not task success.
use std::sync::mpsc;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_rayon_batch::RayonBatchExecutor as Executor;

#[test]
fn test_last_declared_failure_does_not_prove_exhaustion() {
    let executor = SequentialBatchExecutor::builder()
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .build();
    let outcome = executor
        .execute_with_count([|| Err::<(), _>("failed")], 1)
        .expect("policy outcome");
    assert_eq!(outcome.completed_count(), outcome.task_count());
    assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
    assert!(!outcome.is_success());
}

#[test]
fn test_source_exhaustion_precedes_failure_and_count_validation() {
    for expected in [1usize, 2] {
        let executor = Executor::builder()
            .thread_count(2)
            .sequential_threshold(0)
            .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
            .build()
            .expect("valid executor");
        let (sender, receiver) = mpsc::channel();
        let mut sender = Some(sender);
        let mut task = Some(move || {
            receiver
                .recv_timeout(Duration::from_secs(3))
                .expect("source exhaustion signal");
            Err::<(), _>("failed after exhaustion")
        });
        let source = std::iter::from_fn(move || {
            if task.is_some() {
                task.take()
            } else {
                if let Some(sender) = sender.take() {
                    sender.send(()).expect("waiting task");
                }
                None
            }
        });
        let result = executor.execute_with_count(source, expected);
        if expected == 1 {
            let outcome = result.expect("exact source");
            assert_eq!(outcome.termination(), BatchTermination::Finished);
            assert_eq!(outcome.failed_count(), 1);
            assert!(!outcome.is_success());
        } else {
            match result.expect_err("short exhausted source") {
                BatchExecutionError::CountShortfall {
                    expected,
                    actual,
                    outcome,
                    ..
                } => {
                    assert_eq!((expected, actual), (2, 1));
                    assert_eq!(outcome.failed_count(), 1);
                }
                error => panic!("unexpected error: {error:?}"),
            }
        }
    }
}

#[test]
fn test_fallback_does_not_pull_after_failure() {
    for (workers, threshold) in [(1, 0), (2, 100)] {
        let executor = Executor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
            .build()
            .expect("valid executor");
        let mut pulls = 0;
        let source = std::iter::from_fn(|| {
            pulls += 1;
            assert_eq!(pulls, 1, "source must not be probed after policy stop");
            Some(|| Err::<(), _>("failed"))
        });
        let outcome = executor.execute_with_count(source, 1).expect("policy outcome");
        assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
        assert_eq!(pulls, 1);
    }
}
