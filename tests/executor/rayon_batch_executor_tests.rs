// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Tests for [`RayonBatchExecutor`](qubit_rayon_batch::RayonBatchExecutor).

use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::panic::panic_any;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;

use crossbeam_channel::bounded;
use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTaskError;
use qubit_batch::ProgressFailure;
use qubit_function::Runnable;
use qubit_progress::AutoReporterError;
use qubit_progress::Event;
use qubit_progress::MetricSnapshot;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;
use qubit_rayon_batch::RayonBatchExecutor;
use qubit_rayon_batch::RayonBatchExecutorBuildError;

use crate::support::FailingProgressReporter;
use crate::support::PanickingProgressReporter;
use crate::support::ProgressPanicPhase;
use crate::support::RecordingProgressReporter;
use crate::support::TestCallable;
use crate::support::TestTask;
use crate::support::panic_payload_message;
use crate::watchdog::run_with_watchdog;

#[test]
fn test_rayon_batch_executor_returns_progress_report_error() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .reporter(FailingProgressReporter::after_successes(1))
        .build()
        .expect("valid executor configuration should build");

    let error = executor
        .execute_with_count([TestTask::succeed(), TestTask::succeed()], 2)
        .expect_err("failing reporter should fail batch execution");

    assert!(matches!(&error, BatchExecutionError::ProgressReport { .. }));
    assert_eq!(error.outcome().completed_count(), 2);
}

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
fn task_counter(event: &Event) -> &MetricSnapshot {
    match event.metrics() {
        [counter] => counter,
        counters => panic!(
            "progress event should contain exactly one task counter, got {}",
            counters.len()
        ),
    }
}

/// Coordinates task completion with a progress callback.
#[derive(Default)]
struct ProgressTestGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl ProgressTestGate {
    /// Waits until the reporter releases the gate, failing with a bounded
    /// timeout otherwise.
    fn wait(&self) {
        let released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        let (released, _) = self
            .changed
            .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
            .unwrap_or_else(PoisonError::into_inner);
        assert!(*released, "progress reporter did not release task gate before timeout");
    }

    /// Releases all tasks waiting for a qualifying progress event.
    fn release(&self) {
        *self.released.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

/// Records events and releases gated tasks when a qualifying running snapshot
/// arrives.
struct CoordinatedProgressReporter {
    gate: Arc<ProgressTestGate>,
    recording: Arc<RecordingProgressReporter>,
    require_completion: bool,
    panic_message: Option<&'static str>,
}

impl Reporter for CoordinatedProgressReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        self.recording.report(event)?;
        if event.phase() == Phase::Running {
            let counter = task_counter(event);
            if counter.active() > 0 && (!self.require_completion || counter.completed() > 0) {
                self.gate.release();
                if let Some(message) = self.panic_message {
                    panic_any(message);
                }
            }
        }
        Ok(())
    }
}

#[test]
fn test_rayon_batch_executor_build_rejects_invalid_thread_count() {
    let error = RayonBatchExecutor::builder()
        .thread_count(0)
        .build()
        .err()
        .expect("zero thread count should be rejected");

    assert!(matches!(error, RayonBatchExecutorBuildError::ZeroThreadCount));
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

    assert!(matches!(error, RayonBatchExecutorBuildError::BuildFailed { .. }));
}

#[test]
fn test_rayon_batch_executor_new_default_and_accessors() {
    let executor = RayonBatchExecutor::new(2).expect("rayon batch executor should build");
    let default_executor = RayonBatchExecutor::default();

    assert!(RayonBatchExecutor::default_thread_count() >= 1);
    assert_eq!(executor.thread_count(), 2);
    assert_eq!(
        executor.sequential_threshold(),
        RayonBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD
    );
    assert_eq!(executor.report_interval(), RayonBatchExecutor::DEFAULT_REPORT_INTERVAL);
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

    assert_eq!(
        result
            .outputs()
            .iter()
            .map(|output| (output.index(), *output.value()))
            .collect::<Vec<_>>(),
        vec![(0, 10), (3, 40)]
    );
    assert_eq!(result.outcome().completed_count(), 4);
    assert_eq!(result.outcome().failed_count(), 1);
    assert_eq!(result.outcome().panicked_count(), 1);
    assert_eq!(result.outcome().failures()[0].index(), 1);
    assert_eq!(result.outcome().failures()[1].index(), 2);
}

#[test]
fn test_rayon_batch_executor_call_orders_sparse_outputs_after_out_of_order_completion() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("rayon batch executor should build");
    let (release_sender, release_receiver) = bounded::<()>(1);
    let tasks = (0..4).map(move |index| {
        let release_sender = release_sender.clone();
        let release_receiver = release_receiver.clone();
        move || match index {
            0 => {
                release_receiver
                    .recv_timeout(Duration::from_secs(5))
                    .expect("task 3 should release task 0 after task 1 output is collected");
                Ok::<i32, &'static str>(10)
            }
            1 => Ok(20),
            2 => Err("expected failure"),
            3 => {
                // With two workers, task 1's output is collected before this task starts.
                release_sender
                    .send_timeout((), Duration::from_secs(5))
                    .expect("task 3 should release task 0");
                Ok(40)
            }
            _ => unreachable!(),
        }
    });

    let result = executor
        .call_with_count(tasks, 4)
        .expect("task failures should stay in the batch result");
    let outcome = result.outcome();
    assert_eq!(outcome.task_count(), 4);
    assert_eq!(outcome.completed_count(), 4);
    assert_eq!(outcome.succeeded_count(), 3);
    assert_eq!(outcome.failed_count(), 1);
    assert_eq!(outcome.panicked_count(), 0);
    assert!(!outcome.is_success());
    assert_eq!(
        result
            .outputs()
            .iter()
            .map(|output| (output.index(), *output.value()))
            .collect::<Vec<_>>(),
        vec![(0, 10), (1, 20), (3, 40)]
    );
    assert_eq!(outcome.failures().len(), 1);
    assert_eq!(outcome.failures()[0].index(), 2);
    assert_eq!(
        outcome.failures()[0].error(),
        &BatchTaskError::Failed("expected failure")
    );
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
    match shortfall.source() {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            ..
        } => {
            assert_eq!(*expected, 2);
            assert_eq!(*actual, 1);
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
    match exceeded.source() {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
            ..
        } => {
            assert_eq!(*expected, 2);
            assert_eq!(*observed_at_least, 3);
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
    let (release_sender, release_receiver) = bounded::<()>(1);
    let tasks = (0..3).map(move |index| {
        let release_sender = release_sender.clone();
        let release_receiver = release_receiver.clone();
        move || match index {
            0 => {
                release_receiver
                    .recv_timeout(Duration::from_secs(5))
                    .expect("task 2 should release task 0 after task 1 is recorded");
                Err::<(), &'static str>("slow failure")
            }
            1 => Err("fast failure"),
            2 => {
                // Task 2 runs on the idle worker only after task 1's execute_task returns.
                release_sender
                    .send_timeout((), Duration::from_secs(5))
                    .expect("task 2 should release task 0");
                Ok(())
            }
            _ => unreachable!(),
        }
    });

    let result = executor
        .execute_with_count(tasks, 3)
        .expect("task failures should stay in the batch result");

    assert_eq!(result.task_count(), 3);
    assert_eq!(result.completed_count(), 3);
    assert_eq!(result.succeeded_count(), 1);
    assert_eq!(result.failed_count(), 2);
    assert_eq!(result.panicked_count(), 0);
    assert_eq!(result.failures().len(), 2);
    assert_eq!(result.failures()[0].index(), 0);
    assert_eq!(result.failures()[1].index(), 1);
    assert_eq!(result.failures()[0].error(), &BatchTaskError::Failed("slow failure"));
    assert_eq!(result.failures()[1].error(), &BatchTaskError::Failed("fast failure"));
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
            ..
        } => {
            assert_eq!(expected, 3);
            assert_eq!(actual, 2);
            assert_eq!(outcome.completed_count(), 2);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn test_rayon_batch_executor_handles_huge_declared_count_without_preallocation() {
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
            ..
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
            ..
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
    let tasks = vec![TestTask::succeed(), TestTask::succeed(), TestTask::succeed()];

    let error = executor
        .execute_with_count(tasks, 2)
        .expect_err("overflow should be reported");

    match error {
        BatchExecutionError::CountExceeded {
            expected,
            observed_at_least,
            outcome,
            ..
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
    const CHILD: &str = "QUBIT_RAYON_CONCURRENCY_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        run_with_watchdog(
            "executor::rayon_batch_executor_tests::test_rayon_batch_executor_runs_tasks_concurrently",
            CHILD,
            "concurrent task execution",
        );
        return;
    }
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .build()
        .expect("two-worker executor should build");
    let caller = thread::current().id();
    let barrier = Barrier::new(2);
    let tasks = (0..2).map(|_| {
        || {
            barrier.wait();
            assert_ne!(thread::current().id(), caller);
            Ok::<(), &'static str>(())
        }
    });
    let outcome = executor.execute_with_count(tasks, 2).expect("exact batch");
    assert!(outcome.is_success(), "both tasks must succeed");
    assert_eq!(outcome.completed_count(), 2);
}

#[test]
fn test_rayon_batch_executor_falls_back_to_sequential_below_threshold() {
    for (workers, threshold, count, on_caller) in [
        (4, 10, 0usize, true),
        (4, 10, 9, true),
        (4, 10, 10, true),
        (1, 0, 2, true),
        (4, 10, 11, false),
    ] {
        let executor = RayonBatchExecutor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("valid path-test executor");
        let caller = thread::current().id();
        let tasks = (0..count).map(|_| {
            move || {
                assert_eq!(thread::current().id() == caller, on_caller);
                Ok::<(), &'static str>(())
            }
        });
        let outcome = executor.execute_with_count(tasks, count).expect("exact batch");
        assert!(outcome.is_success(), "thread placement must match the selected path");
        assert_eq!(outcome.completed_count(), count);
    }
}

#[test]
fn test_rayon_batch_executor_reports_progress() {
    let gate = Arc::new(ProgressTestGate::default());
    let recording = Arc::new(RecordingProgressReporter::new());
    let reporter = CoordinatedProgressReporter {
        gate: Arc::clone(&gate),
        recording: Arc::clone(&recording),
        require_completion: true,
        panic_message: None,
    };
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(10))
        .reporter(reporter)
        .build()
        .expect("rayon batch executor should build");
    let tasks = (0..4).map(|index| {
        let gate = Arc::clone(&gate);
        move || {
            if index == 0 {
                Err("fast failure")
            } else {
                gate.wait();
                Ok(())
            }
        }
    });

    let result = executor.execute_with_count(tasks, 4).expect("batch should succeed");
    let events = recording.events();

    assert_eq!(result.completed_count(), 4);
    assert_eq!(result.failed_count(), 1);
    assert_eq!(result.panicked_count(), 0);
    assert!(matches!(events.first(), Some(event)
        if event.phase() == Phase::Started
            && task_counter(event).total() == Some(4)
    ));
    assert!(events.iter().any(|event| matches!(event.phase(), Phase::Running)
        && task_counter(event).total() == Some(4)
        && task_counter(event).active() > 0));
    assert!(events.iter().any(|event| matches!(event.phase(), Phase::Running)
        && (task_counter(event).succeeded() > 0 || task_counter(event).failed() > 0)));
    assert!(events.iter().all(|event| match event {
        event if event.phase() == Phase::Running => task_counter(event).active() <= 2,
        _ => true,
    }));
    assert!(matches!(events.last(), Some(event)
        if event.phase() == Phase::Failed
            && task_counter(event).total() == Some(4)
            && task_counter(event).completed() == 4
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
    assert!(events.iter().any(|event| event.phase() == Phase::Running));
    assert!(matches!(events.last(), Some(event)
        if event.phase() == Phase::Failed
            && task_counter(event).completed() == 3
            && task_counter(event).failed() == 1
    ));
}

#[test]
fn test_rayon_batch_executor_reports_failed_progress_for_zero_interval_count_exceeded() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::ZERO)
        .reporter_arc(reporter.clone())
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed(), TestTask::succeed()];

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
        if event.phase() == Phase::Failed
            && task_counter(event).total() == Some(2)
            && task_counter(event).completed() == 2
    ));
}

#[test]
fn test_rayon_batch_executor_propagates_iterator_panic_without_hanging_progress_loop() {
    const CHILD: &str = "QUBIT_RAYON_SOURCE_PANIC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        run_with_watchdog(
            "executor::rayon_batch_executor_tests::test_rayon_batch_executor_propagates_iterator_panic_without_hanging_progress_loop",
            CHILD,
            "source panic progress-loop cleanup",
        );
        return;
    }
    /// Records automatic running reports before releasing the source.
    struct SourcePanicReporter {
        gate: Arc<ProgressTestGate>,
        recording: Arc<RecordingProgressReporter>,
    }

    impl Reporter for SourcePanicReporter {
        fn report(&self, event: &Event) -> Result<(), ReporterError> {
            self.recording.report(event)?;
            if event.phase() == Phase::Running {
                self.gate.release();
            }
            Ok(())
        }
    }

    const PANIC_MESSAGE: &str = "iterator panic in rayon batch";
    for interval in [None, Some(Duration::ZERO), Some(Duration::from_millis(10))] {
        let gate = Arc::new(ProgressTestGate::default());
        let recording = Arc::new(RecordingProgressReporter::new());
        let mut builder = RayonBatchExecutor::builder()
            .thread_count(2)
            .sequential_threshold(1)
            .report_interval(Duration::ZERO);
        if let Some(interval) = interval {
            builder = builder.report_interval(interval).reporter(SourcePanicReporter {
                gate: Arc::clone(&gate),
                recording: Arc::clone(&recording),
            });
        }
        let executor = builder.build().expect("rayon batch executor should build");
        let completed = Arc::new(AtomicUsize::new(0));
        let tasks = (0..3).map(|index| {
            if index == 1 {
                if interval.is_some() {
                    gate.wait();
                }
                panic_any(PANIC_MESSAGE);
            }
            TestTask::count_success(Arc::clone(&completed))
        });

        let payload = catch_unwind(AssertUnwindSafe(|| executor.execute_with_count(tasks, 3)))
            .expect_err("iterator panic should be propagated");
        assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
        assert_eq!(completed.load(Ordering::Acquire), 1, "mode: {interval:?}");
        if interval.is_some() {
            assert!(
                recording.events().iter().any(|event| event.phase() == Phase::Running),
                "automatic reporter must run before source panic: {interval:?}"
            );
        }

        let caller = thread::current().id();
        let outcome = executor
            .for_each(0..2, |_| {
                assert_ne!(thread::current().id(), caller);
                Ok::<(), ()>(())
            })
            .expect("executor should remain usable");
        assert!(outcome.is_success(), "mode: {interval:?}");
        assert_eq!(outcome.completed_count(), 2);
    }
}

#[test]
fn test_runnable_drop_panic_propagates_without_poisoning_executor() {
    const MARKER: &str = "QUBIT_RAYON_RUNNABLE_DROP_PANIC_CHILD";
    if std::env::var_os(MARKER).is_none() {
        run_with_watchdog(
            "executor::rayon_batch_executor_tests::test_runnable_drop_panic_propagates_without_poisoning_executor",
            MARKER,
            "runnable drop panic cleanup",
        );
        return;
    }

    struct DropPanicRunnable {
        ran: Arc<AtomicUsize>,
    }

    impl Runnable<&'static str> for DropPanicRunnable {
        fn run(&mut self) -> Result<(), &'static str> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl Drop for DropPanicRunnable {
        fn drop(&mut self) {
            panic!("runnable drop panic");
        }
    }

    for (workers, threshold) in [(2, 0), (2, 100), (1, 0)] {
        let executor = RayonBatchExecutor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("valid executor");
        let ran = Arc::new(AtomicUsize::new(0));
        let payload = catch_unwind(AssertUnwindSafe(|| {
            executor.execute_with_count([DropPanicRunnable { ran: Arc::clone(&ran) }], 1)
        }))
        .expect_err("Runnable destruction must unwind to the caller");
        assert_eq!(payload.downcast_ref::<&str>(), Some(&"runnable drop panic"));
        assert_eq!(ran.load(Ordering::SeqCst), 1);

        let caller = thread::current().id();
        let recovered = executor
            .for_each(0..101, |_| {
                if workers > 1 {
                    assert_ne!(thread::current().id(), caller);
                }
                Ok::<(), &'static str>(())
            })
            .expect("executor remains usable after caught unwind");
        assert!(recovered.is_success());
        assert_eq!(recovered.completed_count(), 101);
    }
}

#[test]
fn test_rayon_batch_executor_preserves_progress_reporter_zero_interval_process_panic() {
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

    let error = executor
        .execute_with_count(tasks, 2)
        .expect_err("zero-interval progress reporter panic should be returned");
    let BatchExecutionError::ProgressReport { source, .. } = error else {
        panic!("progress reporter panic should be a progress report error");
    };
    let ProgressFailure::AutoReporter(AutoReporterError::Panicked(panic)) = source.as_ref() else {
        panic!("progress reporter panic should preserve structured panic information");
    };
    assert_eq!(panic.message(), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_preserves_progress_reporter_process_panic() {
    const PANIC_MESSAGE: &str = "progress reporter process panic";
    let gate = Arc::new(ProgressTestGate::default());
    let recording = Arc::new(RecordingProgressReporter::new());
    let reporter = CoordinatedProgressReporter {
        gate: Arc::clone(&gate),
        recording,
        require_completion: false,
        panic_message: Some(PANIC_MESSAGE),
    };
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(1))
        .reporter(reporter)
        .build()
        .expect("rayon batch executor should build");
    let tasks = (0..2).map(|_| {
        let gate = Arc::clone(&gate);
        move || {
            gate.wait();
            Ok::<(), &'static str>(())
        }
    });

    let error = executor
        .execute_with_count(tasks, 2)
        .expect_err("progress reporter panic should be returned");
    let BatchExecutionError::ProgressReport { source, .. } = error else {
        panic!("progress reporter panic should be a progress report error");
    };
    let ProgressFailure::AutoReporter(AutoReporterError::Panicked(panic)) = source.as_ref() else {
        panic!("progress reporter panic should preserve structured panic information");
    };
    assert_eq!(panic.message(), Some(PANIC_MESSAGE));
}

#[test]
fn test_rayon_batch_executor_propagates_progress_reporter_start_panic() {
    const PANIC_MESSAGE: &str = "progress reporter start panic";
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(1)
        .reporter(PanickingProgressReporter::new(ProgressPanicPhase::Start, PANIC_MESSAGE))
        .build()
        .expect("rayon batch executor should build");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute_with_count(tasks, 2)))
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

    let payload = catch_unwind(AssertUnwindSafe(|| executor.execute_with_count(tasks, 2)))
        .expect_err("progress reporter finish panic should be propagated");

    assert_eq!(panic_payload_message(payload.as_ref()), Some(PANIC_MESSAGE));
}
