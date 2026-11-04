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
        assert!(
            matches!(result, Err(RayonBatchExecutorBuildError::InvalidThreadNamePrefix)),
            "prefix {prefix:?} containing NUL must be rejected"
        );
    }
}

#[test]
fn test_thread_name_prefix_preserves_existing_validation_priority() {
    let zero_threads = RayonBatchExecutor::builder()
        .thread_count(0)
        .stack_size(0)
        .thread_name_prefix("bad\0worker")
        .build();
    assert!(
        matches!(zero_threads, Err(RayonBatchExecutorBuildError::ZeroThreadCount)),
        "zero thread count must take priority over invalid stack size and prefix"
    );
    let zero_stack = RayonBatchExecutor::builder()
        .thread_count(2)
        .stack_size(0)
        .thread_name_prefix("bad\0worker")
        .build();
    assert!(
        matches!(zero_stack, Err(RayonBatchExecutorBuildError::ZeroStackSize)),
        "zero stack size must take priority over an invalid prefix"
    );
}

#[test]
fn test_thread_name_prefix_accepts_empty_and_unicode() {
    for prefix in ["", "批处理"] {
        let executor = RayonBatchExecutor::builder()
            .thread_count(2)
            .thread_name_prefix(prefix)
            .build()
            .expect("a prefix without NUL must build");
        assert_eq!(
            executor.thread_count(),
            2,
            "builder must preserve the configured thread count"
        );
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

    assert_eq!(
        executor.thread_count(),
        2,
        "builder must preserve the configured thread count"
    );
    assert_eq!(
        executor.sequential_threshold(),
        7,
        "builder must preserve the sequential threshold"
    );
    assert_eq!(
        executor.report_interval(),
        Duration::from_millis(15),
        "builder must preserve the report interval"
    );
    assert_eq!(
        executor.task_failure_policy(),
        TaskFailurePolicy::StopOnFirstFailure,
        "builder must preserve the task failure policy"
    );
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

    assert_eq!(
        outcome.completed_count(),
        2,
        "both tasks must complete with the shared reporter"
    );
    assert!(
        events.iter().any(|event| event.phase() == Phase::Started),
        "the shared reporter must observe batch start"
    );
    assert!(
        events.iter().any(|event| event.phase() == Phase::Succeeded),
        "the shared reporter must observe batch success"
    );
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

    assert_eq!(
        outcome.completed_count(),
        1,
        "the task must complete with the no-op reporter"
    );
    assert!(
        reporter.events().is_empty(),
        "the replaced reporter must receive no events"
    );
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
        assert!(result.outcome().is_success(), "named worker tasks must succeed");
        assert_eq!(
            result.outcome().completed_count(),
            4,
            "all named worker tasks must complete"
        );
        assert_eq!(
            result.outputs().len(),
            4,
            "each named worker task must produce one output"
        );
        for output in result.outputs() {
            let (worker_id, name) = output.value();
            assert_ne!(
                *worker_id, caller,
                "tasks must run on pool workers rather than the caller"
            );
            assert!(
                matches!(name.as_deref(), Some("builder-test-worker-0" | "builder-test-worker-1")),
                "unexpected worker name: {name:?}"
            );
        }
    }
}
