// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`RayonBatchExecutor`](qubit_rayon_batch::RayonBatchExecutor).

use std::{
    panic::{
        AssertUnwindSafe,
        catch_unwind,
        panic_any,
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
    ProgressCounter,
    ProgressEvent,
    ProgressPhase,
    RayonBatchExecutor,
    RayonBatchExecutorBuildError,
};

use crate::support::{
    PanickingProgressReporter,
    ProgressPanicPhase,
    RecordingProgressReporter,
    TestCallable,
    TestTask,
    panic_payload_message,
};

/// Returns the single task counter carried by a progress event.
///
/// # Parameters
///
/// * `event` - Progress event emitted by the executor under test.
///
/// # Returns
///
/// The only counter recorded in the event.
///
/// # Panics
///
/// Panics if the event does not carry exactly one counter.
fn task_counter(event: &ProgressEvent) -> &ProgressCounter {
    match event.counters() {
        [counter] => counter,
        counters => panic!(
            "progress event should contain exactly one task counter, got {}",
            counters.len()
        ),
    }
}

#[test]
fn test_rayon_batch_executor_build_rejects_invalid_thread_count() {
    let error = RayonBatchExecutor::builder()
        .thread_count(0)
        .build()
        .err()
        .expect("zero thread count should be rejected");

    assert!(matches!(
        error,
        RayonBatchExecutorBuildError::ZeroThreadCount
    ));
}

#[test]
fn test_rayon_batch_executor_build_allows_zero_report_interval() {
    let executor = RayonBatchExecutor::builder()
        .report_interval(Duration::ZERO)
        .build()
        .expect("zero report interval should be accepted");

    assert_eq!(executor.report_interval(), Duration::ZERO);
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
        .thread_count(2)
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
    let executor =
        RayonBatchExecutor::new(2).expect("rayon batch executor should build");
    let default_executor = RayonBatchExecutor::default();

    assert!(RayonBatchExecutor::default_thread_count() >= 1);
    assert_eq!(executor.thread_count(), 2);
    assert_eq!(
        executor.sequential_threshold(),
        RayonBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    assert_eq!(
        executor.report_interval(),
        RayonBatchExecutor::DEFAULT_REPORT_INTERVAL
    );
    assert!(Arc::strong_count(executor.reporter()) >= 1);
    assert!(default_executor.thread_count() >= 1);
}

#[test]
fn test_rayon_batch_executor_builder_custom_options() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(3)
        .report_interval(Duration::from_millis(25))
        .reporter(RecordingProgressReporter::new())
        .no_reporter()
        .thread_name_prefix("qubit-rayon-batch-test")
        .stack_size(2 * 1024 * 1024)
        .build()
        .expect("rayon batch executor should build with custom options");

    assert_eq!(executor.thread_count(), 2);
    assert_eq!(executor.sequential_threshold(), 3);
    assert_eq!(executor.report_interval(), Duration::from_millis(25));
}

#[test]
fn test_rayon_batch_executor_executes_successfully() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let counter = Arc::new(AtomicUsize::new(0));
    let tasks = (0..8)
        .map(|_| TestTask::count_success(Arc::clone(&counter)))
        .collect::<Vec<_>>();

    let result = executor
        .execute_with_count(tasks, 8)
        .expect("rayon batch should succeed");

    assert_eq!(counter.load(Ordering::Acquire), 8);
    assert_eq!(result.completed_count(), 8);
    assert_eq!(result.succeeded_count(), 8);
    assert_eq!(result.failure_count(), 0);
}

#[test]
fn test_rayon_batch_executor_for_each_executes_on_rayon_path() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let counter = Arc::new(AtomicUsize::new(0));

    let outcome = executor
        .for_each_with_count(0..8, 8, {
            let counter = Arc::clone(&counter);
            move |value| {
                assert!(value < 8);
                counter.fetch_add(1, Ordering::AcqRel);
                Ok::<(), &'static str>(())
            }
        })
        .expect("for_each batch should succeed");

    assert_eq!(counter.load(Ordering::Acquire), 8);
    assert_eq!(outcome.completed_count(), 8);
    assert_eq!(outcome.succeeded_count(), 8);
}

#[test]
fn test_rayon_batch_executor_supports_non_static_for_each_items() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let values = [1, 2, 3, 4];

    let outcome = executor
        .for_each_with_count(values.iter(), values.len(), |value| {
            assert!(*value > 0);
            Ok::<(), &'static str>(())
        })
        .expect("borrowed items should be accepted by the scoped rayon path");

    assert_eq!(outcome.completed_count(), values.len());
    assert_eq!(outcome.succeeded_count(), values.len());
}

#[test]
fn test_rayon_batch_executor_call_collects_values_by_index() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestCallable::returning(10),
        TestCallable::fail("failed"),
        TestCallable::panic("panic in callable"),
        TestCallable::returning(40),
    ];

    let result = executor
        .call_with_count(tasks, 4)
        .expect("callable failures should stay in the batch result");

    assert_eq!(result.values(), &[Some(10), None, None, Some(40)]);
    assert_eq!(result.outcome().completed_count(), 4);
    assert_eq!(result.outcome().failed_count(), 1);
    assert_eq!(result.outcome().panicked_count(), 1);
    assert_eq!(result.outcome().failures()[0].index(), 1);
    assert_eq!(result.outcome().failures()[1].index(), 2);
}

#[test]
fn test_rayon_batch_executor_call_reports_count_mismatches() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");

    let shortfall = executor
        .call_with_count(vec![TestCallable::returning(10)], 2)
        .expect_err("call shortfall should be reported");
    match shortfall {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(actual, 1);
            assert_eq!(outcome.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    let exceeded = executor
        .call_with_count(
            vec![
                TestCallable::returning(10),
                TestCallable::returning(20),
                TestCallable::returning(30),
            ],
            2,
        )
        .expect_err("call overflow should be reported");
    match exceeded {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_collects_failures_and_panics() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
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
        .execute_with_count(tasks, 4)
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
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::fail_after_sleep("slow failure", Duration::from_millis(50)),
        TestTask::fail("fast failure"),
    ];

    let result = executor
        .execute_with_count(tasks, 2)
        .expect("task failures should stay in the batch result");

    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 0);
    assert_eq!(result.failures()[1].index(), 1);
}

#[test]
fn test_rayon_batch_executor_reports_count_shortfall() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute_with_count(tasks, 3)
        .expect_err("shortfall should be reported");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_handles_huge_declared_count_without_preallocation()
{
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed()];

    let error = executor
        .execute_with_count(tasks, usize::MAX)
        .expect_err("shortfall should be reported without preallocating count");

    match error {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
        } => {
            assert_eq!(expected, usize::MAX);
            assert_eq!(actual, 1);
            assert_eq!(outcome.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_reports_count_exceeded() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute_with_count(tasks, 1)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
        } => {
            assert_eq!(expected, 1);
            assert_eq!(observed_at_least, 2);
            assert_eq!(outcome.completed_count(), 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_reports_count_exceeded_in_parallel_path() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(1)
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::succeed(),
        TestTask::succeed(),
    ];

    let error = executor
        .execute_with_count(tasks, 2)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
        } => {
            assert_eq!(expected, 2);
            assert_eq!(observed_at_least, 3);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_runs_tasks_concurrently() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
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
        .execute_with_count(tasks, 8)
        .expect("rayon batch should succeed");

    assert_eq!(result.completed_count(), 8);
    assert!(max_active.load(Ordering::Acquire) > 1);
}

#[test]
fn test_rayon_batch_executor_falls_back_to_sequential_below_threshold() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
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

    let result = executor
        .execute_with_count(tasks, 4)
        .expect("batch should succeed");

    assert_eq!(result.completed_count(), 4);
    assert_eq!(max_active.load(Ordering::Acquire), 1);
}

#[test]
fn test_rayon_batch_executor_reports_progress() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(10))
        .reporter_arc(reporter.clone())
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::fail("fast failure"),
        TestTask::sleep_success(Duration::from_millis(60)),
        TestTask::succeed(),
        TestTask::sleep_success(Duration::from_millis(60)),
    ];

    let result = executor
        .execute_with_count(tasks, 4)
        .expect("batch should succeed");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.failed_count(), 1);
    assert!(matches!(events.first(), Some(event)
        if event.phase() == ProgressPhase::Started
            && task_counter(event).total_count() == Some(4)
    ));
    assert!(events.iter().any(|event| matches!(
        event.phase(),
        ProgressPhase::Running
    ) && task_counter(event).total_count()
        == Some(4)
        && task_counter(event).active_count() > 0));
    assert!(events.iter().any(|event| matches!(
        event.phase(),
        ProgressPhase::Running
    )
        && (task_counter(event).succeeded_count() > 0
            || task_counter(event).failed_count() > 0)));
    assert!(events.iter().all(|event| match event {
        event if event.phase() == ProgressPhase::Running =>
            task_counter(event).active_count() <= 2,
        _ => true,
    }));
    assert!(matches!(events.last(), Some(event)
        if event.phase() == ProgressPhase::Finished
            && task_counter(event).total_count() == Some(4)
            && task_counter(event).completed_count() == 4
    ));
}

#[test]
fn test_rayon_batch_executor_reports_progress_with_zero_interval() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::ZERO)
        .reporter_arc(reporter.clone())
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::sleep_success(Duration::from_millis(10)),
        TestTask::fail("failed"),
        TestTask::sleep_success(Duration::from_millis(10)),
    ];

    let result = executor
        .execute_with_count(tasks, 3)
        .expect("task failure should stay in the batch result");
    let events = reporter.events();

    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.failed_count(), 1);
    assert!(
        events
            .iter()
            .any(|event| event.phase() == ProgressPhase::Running)
    );
    assert!(matches!(events.last(), Some(event)
        if event.phase() == ProgressPhase::Finished
            && task_counter(event).completed_count() == 3
            && task_counter(event).failed_count() == 1
    ));
}

#[test]
fn test_rayon_batch_executor_reports_failed_progress_for_zero_interval_count_exceeded()
 {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::ZERO)
        .reporter_arc(reporter.clone())
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![
        TestTask::succeed(),
        TestTask::succeed(),
        TestTask::succeed(),
    ];

    let error = executor
        .execute_with_count(tasks, 2)
        .expect_err("overflow should be reported");
    let events = reporter.events();

    assert!(matches!(
        error,
        BatchExecutionError::CountExceeded {
            expected: 2,
            observed_at_least: 3,
            ..
        }
    ));
    assert!(matches!(events.last(), Some(event)
        if event.phase() == ProgressPhase::Failed
            && task_counter(event).total_count() == Some(2)
            && task_counter(event).completed_count() == 2
    ));
}

#[test]
fn test_rayon_batch_executor_propagates_iterator_panic_without_hanging_progress_loop()
 {
    const PANIC_MESSAGE: &str = "iterator panic in rayon batch";
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::ZERO)
        .build()
        .expect("rayon batch executor should build");
    let tasks = (0..3).map(|index| {
        if index == 1 {
            panic_any(PANIC_MESSAGE);
        }
        TestTask::sleep_success(Duration::from_millis(5))
    });

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 3)
    }))
    .expect_err("iterator panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_preserves_progress_reporter_zero_interval_process_panic()
 {
    const PANIC_MESSAGE: &str = "zero interval progress reporter process panic";
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::ZERO)
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Process,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("rayon batch executor should build");
    let tasks = (0..2)
        .map(|_| TestTask::sleep_success(Duration::from_millis(10)))
        .collect::<Vec<_>>();

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 2)
    }))
    .expect_err("zero-interval progress reporter panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_preserves_progress_reporter_process_panic() {
    const PANIC_MESSAGE: &str = "progress reporter process panic";
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
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

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 2)
    }))
    .expect_err("progress reporter panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_propagates_progress_reporter_start_panic() {
    const PANIC_MESSAGE: &str = "progress reporter start panic";
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Start,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 2)
    }))
    .expect_err("progress reporter start panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_propagates_progress_reporter_finish_panic() {
    const PANIC_MESSAGE: &str = "progress reporter finish panic";
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .reporter(PanickingProgressReporter::new(
            ProgressPanicPhase::Finish,
            PANIC_MESSAGE,
        ))
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| {
        executor.execute_with_count(tasks, 2)
    }))
    .expect_err("progress reporter finish panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}
