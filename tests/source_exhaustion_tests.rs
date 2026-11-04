// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for the Rayon source exhaustion contract.

use std::iter::from_fn;
use std::sync::mpsc;
use std::time::Duration;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::TaskFailurePolicy;
use qubit_rayon_batch::RayonBatchExecutor as DedicatedRayonBatchExecutor;

#[test]
fn test_rayon_preserves_exhausted_source_contract() {
    for expected in [1usize, 2] {
        let executor = DedicatedRayonBatchExecutor::builder()
            .thread_count(2)
            .sequential_threshold(0)
            .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
            .build()
            .expect("Rayon executor should build");
        let (sender, receiver) = mpsc::channel();
        let mut sender = Some(sender);
        let mut task = Some(move || {
            receiver
                .recv_timeout(Duration::from_secs(3))
                .expect("source should be observed as exhausted");
            Err::<(), _>("failed after exhaustion")
        });
        let source = from_fn(move || {
            if task.is_some() {
                task.take()
            } else {
                if let Some(sender) = sender.take() {
                    sender.send(()).expect("task should still be waiting");
                }
                None
            }
        });
        let result = executor.execute_with_count(source, expected);
        if expected == 1 {
            let outcome = result.expect("exact source should return an outcome");
            assert_eq!(
                outcome.termination(),
                BatchTermination::Finished,
                "an exhausted exact source should finish despite task failure"
            );
            assert_eq!(outcome.completed_count(), 1, "the sole task should complete");
            assert_eq!(outcome.failed_count(), 1, "the sole task should fail");
            assert!(!outcome.is_success(), "task failure should prevent success");
        } else {
            match result.expect_err("short exhausted source must fail count validation") {
                BatchExecutionError::CountShortfall {
                    expected,
                    actual,
                    outcome,
                    ..
                } => {
                    assert_eq!(
                        (expected, actual),
                        (2, 1),
                        "the exhausted source should report one task against two expected"
                    );
                    assert_eq!(outcome.completed_count(), 1, "the sole task should complete");
                    assert_eq!(outcome.failed_count(), 1, "the sole task should fail");
                    assert!(!outcome.is_success(), "task failure should prevent success");
                }
                error => panic!("unexpected error: {error:?}"),
            }
        }
    }
}
