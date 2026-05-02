/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`RayonBatchExecutor`](qubit_rayon_batch::RayonBatchExecutor).

use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::{
        Arc,
        atomic::{
            AtomicUsize,
            Ordering,
        },
    },
    time::Duration,
};

use qubit_rayon_batch::{
    BatchExecutionError,
    BatchExecutor,
    RayonBatchExecutor,
    RayonBatchExecutorBuildError,
};

use crate::support::{
    PanickingProgressReporter,
    ProgressEvent,
    ProgressPanicPhase,
    RecordingProgressReporter,
    TestTask,
    panic_payload_message,
};

#[test]
fn test_rayon_batch_executor_build_rejects_invalid_thread_count() {
    let error = RayonBatchExecutor::builder()
        .num_threads(0)
        .build()
        .err()
        .expect("zero thread count should be rejected");

    assert!(matches!(
        error,
        RayonBatchExecutorBuildError::ZeroThreadCount
    ));
}

#[test]
fn test_rayon_batch_executor_build_rejects_zero_report_interval() {
    let error = RayonBatchExecutor::builder()
        .report_interval(Duration::ZERO)
        .build()
        .err()
        .expect("zero report interval should be rejected");

    assert!(matches!(
        error,
        RayonBatchExecutorBuildError::ZeroReportInterval
    ));
}

#[test]
fn test_rayon_batch_executor_build_rejects_zero_stack_size() {
    let error = RayonBatchExecutor::builder()
        .stack_size(0)
        .build()
        .err()
        .expect("zero stack size should be rejected");

    assert!(matches!(error, RayonBatchExecutorBuildError::ZeroStackSize));
}

#[test]
fn test_rayon_batch_executor_build_reports_thread_pool_failure() {
    let error = RayonBatchExecutor::builder()
        .num_threads(2)
        .stack_size(usize::MAX)
        .build()
        .err()
        .expect("impossibly large stack size should make Rayon fail");

    assert!(matches!(
        error,
        RayonBatchExecutorBuildError::BuildFailed { .. }
    ));
}

#[test]
fn test_rayon_batch_executor_new_default_and_accessors() {
    let executor = RayonBatchExecutor::new(2).expect("rayon batch executor should build");
    let default_executor = RayonBatchExecutor::default();

    assert!(RayonBatchExecutor::default_num_threads() >= 1);
    assert_eq!(executor.num_threads(), 2);
    assert_eq!(
        executor.sequential_threshold(),
        RayonBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    assert_eq!(
        executor.report_interval(),
        RayonBatchExecutor::DEFAULT_REPORT_INTERVAL
    );
    executor.reporter().start(0);
    assert!(default_executor.num_threads() >= 1);
}

#[test]
fn test_rayon_batch_executor_builder_custom_options() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(3)
        .report_interval(Duration::from_millis(25))
        .reporter(RecordingProgressReporter::new())
        .no_reporter()
        .thread_name_prefix("qubit-rayon-batch-test")
        .stack_size(2 * 1024 * 1024)
        .build()
        .expect("rayon batch executor should build with custom options");

    assert_eq!(executor.num_threads(), 2);
    assert_eq!(executor.sequential_threshold(), 3);
    assert_eq!(executor.report_interval(), Duration::from_millis(25));
}

#[test]
fn test_rayon_batch_executor_executes_successfully() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let counter = Arc::new(AtomicUsize::new(0));
    let tasks = (0..8)
        .map(|_| TestTask::count_success(Arc::clone(&counter)))
        .collect::<Vec<_>>();

    let result = executor
        .execute(tasks, 8)
        .expect("rayon batch should succeed");

    assert_eq!(counter.load(Ordering::Acquire), 8);
    assert_eq!(result.completed_count(), 8);
    assert_eq!(result.succeeded_count(), 8);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_rayon_batch_executor_collects_failures_and_panics() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::fail("failed"),
        TestTask::panic_string("panic in rayon batch"),
        TestTask::panic_usize(7),
    ];

    let result = executor
        .execute(tasks, 4)
        .expect("task failures should stay in the batch result");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.succeeded_count(), 1);
    assert_eq!(result.failed_count(), 1);
    assert_eq!(result.panicked_count(), 2);
    assert_eq!(result.failures().len(), 3);
    assert_eq!(
        result.failures()[1].error().panic_message(),
        Some("panic in rayon batch")
    );
    assert_eq!(result.failures()[2].error().panic_message(), None);
}

#[test]
fn test_rayon_batch_executor_orders_failures_by_task_index() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::fail_after_sleep("slow failure", Duration::from_millis(50)),
        TestTask::fail("fast failure"),
    ];

    let result = executor
        .execute(tasks, 2)
        .expect("task failures should stay in the batch result");

    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 0);
    assert_eq!(result.failures()[1].index(), 1);
}

#[test]
fn test_rayon_batch_executor_reports_count_shortfall() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute(tasks, 3)
        .expect_err("shortfall should be reported");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            result,
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(result.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_handles_huge_declared_count_without_preallocation() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed()];

    let error = executor
        .execute(tasks, usize::MAX)
        .expect_err("shortfall should be reported without preallocating count");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            result,
        } => {
            assert_eq!(expected, usize::MAX);
            assert_eq!(actual, 1);
            assert_eq!(result.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_reports_count_exceeded() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute(tasks, 1)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 1);
            assert_eq!(observed_at_least, 2);
            assert_eq!(result.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_reports_count_exceeded_in_parallel_path() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::succeed(),
        TestTask::succeed(),
    ];

    let error = executor
        .execute(tasks, 2)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            result,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(result.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_runs_tasks_concurrently() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let tasks = (0..8)
        .map(|_| {
            TestTask::track_concurrency(
                Arc::clone(&active),
                Arc::clone(&max_active),
                Duration::from_millis(30),
            )
        })
        .collect::<Vec<_>>();

    let result = executor
        .execute(tasks, 8)
        .expect("rayon batch should succeed");

    assert_eq!(result.completed_count(), 8);
    assert!(max_active.load(Ordering::Acquire) > 1);
}

#[test]
fn test_rayon_batch_executor_falls_back_to_sequential_below_threshold() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(4)
        .sequential_threshold(10)
        .build()
        .expect("rayon batch executor should build");
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let tasks = (0..4)
        .map(|_| {
            TestTask::track_concurrency(
                Arc::clone(&active),
                Arc::clone(&max_active),
                Duration::from_millis(10),
            )
        })
        .collect::<Vec<_>>();

    let result = executor.execute(tasks, 4).expect("batch should succeed");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(max_active.load(Ordering::Acquire), 1);
}

#[test]
fn test_rayon_batch_executor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(10))
        .reporter_arc(reporter.clone())
        .build()
        .expect("rayon batch executor should build");
    let tasks = (0..4)
        .map(|_| TestTask::sleep_success(Duration::from_millis(60)))
        .collect::<Vec<_>>();

    let result = executor.execute(tasks, 4).expect("batch should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 4);
    assert!(matches!(
        events.first(),
        Some(ProgressEvent::Start { total_count: 4 })
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        ProgressEvent::Process {
            total_count: 4,
            active_count,
            ..
        } if *active_count > 0
    )));
    assert!(events.iter().all(|event| match event {
        ProgressEvent::Process { active_count, .. } => *active_count <= 2,
        _ => true,
    }));
    assert!(matches!(
        events.last(),
        Some(ProgressEvent::Finish { total_count: 4, .. })
    ));
}

#[test]
fn test_rayon_batch_executor_preserves_progress_reporter_process_panic() {
    const PANIC_MESSAGE: &str = "progress reporter process panic";
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(1))
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Process,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("rayon batch executor should build");
    let tasks = (0..2)
        .map(|_| TestTask::sleep_success(Duration::from_millis(50)))
        .collect::<Vec<_>>();

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute(tasks, 2)))
        .expect_err("progress reporter panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_propagates_progress_reporter_start_panic() {
    const PANIC_MESSAGE: &str = "progress reporter start panic";
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Start,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute(tasks, 2)))
        .expect_err("progress reporter start panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_propagates_progress_reporter_finish_panic() {
    const PANIC_MESSAGE: &str = "progress reporter finish panic";
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Finish,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute(tasks, 2)))
        .expect_err("progress reporter finish panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}
