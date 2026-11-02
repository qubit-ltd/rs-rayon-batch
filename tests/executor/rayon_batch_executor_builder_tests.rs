// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`RayonBatchExecutorBuilder`](qubit_rayon_batch::RayonBatchExecutorBuilder).

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use qubit_batch::BatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_progress::Phase;
use qubit_rayon_batch::RayonBatchExecutor;
use qubit_rayon_batch::RayonBatchExecutorBuildError;

use crate::support::RecordingProgressReporter;
use crate::support::TestTask;

#[test]
fn test_thread_name_prefix_rejects_nul() {
    for prefix in ["\0worker", "bad\0worker", "worker\0"] {
        let result = RayonBatchExecutor::builder()
            .thread_count(2)
            .thread_name_prefix(prefix)
            .build();
        assert!(matches!(
            result,
            Err(RayonBatchExecutorBuildError::InvalidThreadNamePrefix)
        ));
    }
}

#[test]
fn test_thread_name_prefix_preserves_existing_validation_priority() {
    let zero_threads = RayonBatchExecutor::builder()
        .thread_count(0)
        .stack_size(0)
        .thread_name_prefix("bad\0worker")
        .build();
    assert!(matches!(
        zero_threads,
        Err(RayonBatchExecutorBuildError::ZeroThreadCount)
    ));
    let zero_stack = RayonBatchExecutor::builder()
        .thread_count(2)
        .stack_size(0)
        .thread_name_prefix("bad\0worker")
        .build();
    assert!(matches!(zero_stack, Err(RayonBatchExecutorBuildError::ZeroStackSize)));
}

#[test]
fn test_thread_name_prefix_accepts_empty_and_unicode() {
    for prefix in ["", "批处理"] {
        let executor = RayonBatchExecutor::builder()
            .thread_count(2)
            .thread_name_prefix(prefix)
            .build()
            .expect("a prefix without NUL must build");
        assert_eq!(executor.thread_count(), 2);
    }
}

#[test]
fn test_rayon_batch_executor_builder_applies_custom_execution_settings() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(7)
        .report_interval(Duration::from_millis(15))
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .thread_name_prefix(String::from("builder-test-worker"))
        .stack_size(2 * 1024 * 1024)
        .build()
        .expect("custom builder settings should build");

    assert_eq!(executor.thread_count(), 2);
    assert_eq!(executor.sequential_threshold(), 7);
    assert_eq!(executor.report_interval(), Duration::from_millis(15));
    assert_eq!(executor.task_failure_policy(), TaskFailurePolicy::StopOnFirstFailure);
}

#[test]
fn test_rayon_batch_executor_builder_uses_shared_reporter_arc() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(1))
        .reporter_arc(reporter.clone())
        .build()
        .expect("builder should accept shared reporter");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let outcome = executor
        .execute_with_count(tasks, 2)
        .expect("batch should execute with shared reporter");
    let events = reporter.events();

    assert_eq!(outcome.completed_count(), 2);
    assert!(events.iter().any(|event| event.phase() == Phase::Started));
    assert!(events.iter().any(|event| event.phase() == Phase::Succeeded));
}

#[test]
fn test_rayon_batch_executor_builder_no_reporter_replaces_custom_reporter() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .reporter_arc(reporter.clone())
        .no_reporter()
        .build()
        .expect("builder should reset to no-op reporter");
    let tasks = vec![TestTask::succeed()];

    let outcome = executor
        .execute_with_count(tasks, 1)
        .expect("batch should execute with no-op reporter");

    assert_eq!(outcome.completed_count(), 1);
    assert!(reporter.events().is_empty());
}

#[test]
fn test_builder_thread_name_prefix_applies_to_shared_pool_workers() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .thread_name_prefix("builder-test-worker")
        .build()
        .expect("named executor must build");
    let caller = thread::current().id();
    let cloned = executor.clone();
    for current in [&executor, &cloned] {
        let result = current
            .call((0..4).map(|_| {
                || {
                    let worker = thread::current();
                    Ok::<_, ()>((worker.id(), worker.name().map(str::to_owned)))
                }
            }))
            .expect("named worker batch must complete");
        assert!(result.outcome().is_success());
        assert_eq!(result.outcome().completed_count(), 4);
        assert_eq!(result.outputs().len(), 4);
        for output in result.outputs() {
            let (worker_id, name) = output.value();
            assert_ne!(*worker_id, caller);
            assert!(
                matches!(name.as_deref(), Some("builder-test-worker-0" | "builder-test-worker-1")),
                "unexpected worker name: {name:?}"
            );
        }
    }
}
