// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Callable outputs and thread placement across fallback and parallel paths.
#![allow(clippy::result_large_err)]
use std::cell::Cell;
use std::env;
use std::io;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Barrier;
use std::thread;
use std::vec::IntoIter;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTaskError;
use qubit_function::Callable;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;
use qubit_rayon_batch::RayonBatchExecutor as Executor;

use self::watchdog::run_with_watchdog;

mod watchdog;

/// Owned output without `Clone`, ensuring execution moves task results intact.
#[derive(Debug)]
struct NonClone {
    value: usize,
}

#[test]
fn test_callable_fn_mut_send_not_sync_matrix() {
    for (workers, threshold) in [(2, 0), (2, 100), (1, 0)] {
        let executor = Executor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("type-boundary executor should build");
        let tasks = (0..8usize).map(|index| {
            let state = Cell::new(index);
            let mut calls = 0usize;
            move || {
                calls += 1;
                state.set(state.get() + calls);
                if index == 3 {
                    Err(Cell::new(state.get()))
                } else {
                    Ok(Cell::new(state.get()))
                }
            }
        });
        let result = executor.call(tasks).expect("exact callable source");
        let outcome = result.outcome();
        assert_eq!(outcome.task_count(), 8);
        assert_eq!(outcome.completed_count(), 8);
        assert_eq!(outcome.succeeded_count(), 7);
        assert_eq!(outcome.failed_count(), 1);
        assert_eq!(outcome.panicked_count(), 0);
        assert!(!outcome.is_success());
        assert_eq!(outcome.failures().len(), 1);
        assert_eq!(outcome.failures()[0].index(), 3);
        match outcome.failures()[0].error() {
            BatchTaskError::Failed(error) => assert_eq!(error.get(), 4),
            error => panic!("expected callable error, got {error:?}"),
        }
        assert_eq!(
            result.outputs().iter().map(|output| output.index()).collect::<Vec<_>>(),
            vec![0, 1, 2, 4, 5, 6, 7]
        );
        for output in result.outputs() {
            assert_eq!(output.value().get(), output.index() + 1);
        }
    }
}

#[test]
fn test_callable_matrix_preserves_values_and_thread_placement() {
    for (workers, threshold) in [(2, 100), (2, 0), (1, 0)] {
        let executor = Executor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("valid executor");
        for count in [0usize, 1, 32, 100, 101] {
            let caller = thread::current().id();
            let sequential = workers == 1 || count <= threshold;
            let result = executor
                .call((0..count).map(|index| {
                    move || {
                        assert_eq!(thread::current().id() == caller, sequential);
                        Ok::<_, ()>(NonClone { value: index })
                    }
                }))
                .expect("exact batch");
            assert!(result.outcome().is_success());
            assert_eq!(result.outputs().len(), count);
            for (index, output) in result.outputs().iter().enumerate() {
                assert_eq!(output.index(), index);
                assert_eq!(output.value().value, index);
            }
        }
    }
}

/// Separates body panic, destructor panic, and task failure by task index.
struct DropCallable {
    index: usize,
}
impl Callable<NonClone, &'static str> for DropCallable {
    fn call(&mut self) -> Result<NonClone, &'static str> {
        if self.index == 1 {
            panic!("body panic");
        }
        if self.index == 3 {
            return Err("task error");
        }
        Ok(NonClone { value: self.index })
    }
}
impl Drop for DropCallable {
    fn drop(&mut self) {
        if self.index == 2 {
            panic!("drop panic");
        }
    }
}
#[test]
fn test_callable_panics_and_errors_preserve_sparse_successes() {
    for threshold in [0, 100] {
        let executor = Executor::builder()
            .thread_count(2)
            .sequential_threshold(threshold)
            .build()
            .expect("valid executor");
        let result = executor
            .call((0..5).map(|index| DropCallable { index }))
            .expect("task outcomes");
        assert_eq!(result.outcome().panicked_count(), 2);
        assert_eq!(result.outcome().failed_count(), 1);
        assert_eq!(result.outcome().completed_count(), 5);
        assert_eq!(
            result.outputs().iter().map(|o| o.index()).collect::<Vec<_>>(),
            vec![0, 4]
        );
        assert_eq!(
            result
                .outcome()
                .failures()
                .iter()
                .map(|f| f.index())
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }
}

/// Reports a terminal failure only, making error precedence deterministic.
struct TerminalFailure;
impl Reporter for TerminalFailure {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if matches!(event.phase(), Phase::Succeeded | Phase::Failed) {
            Err(ReporterError::new(io::Error::other("terminal rejected")))
        } else {
            Ok(())
        }
    }
}
#[test]
fn test_count_and_report_errors_preserve_outputs() {
    for threshold in [0, 100] {
        for expected in [1usize, 2, 3] {
            let executor = Executor::builder()
                .thread_count(2)
                .sequential_threshold(threshold)
                .reporter(TerminalFailure)
                .build()
                .expect("valid executor");
            let error = executor
                .call_with_count((0..2).map(|value| move || Ok::<_, ()>(NonClone { value })), expected)
                .expect_err("count or terminal error");
            let completed = expected.min(2);
            assert_eq!(error.outcome().completed_count(), completed);
            assert_eq!(error.outputs().len(), completed);
            assert_eq!(error.outputs()[0].value().value, 0);
            match (expected, error.source()) {
                (1, BatchExecutionError::CountExceeded { report_error, .. })
                | (3, BatchExecutionError::CountShortfall { report_error, .. }) => {
                    assert!(report_error.is_some())
                }
                (2, BatchExecutionError::ProgressReport { .. }) => {}
                (_, error) => panic!("unexpected error: {error:?}"),
            }
        }
    }
}

/// Conversion itself performs a nested call, before its first next().
struct ReentrantSource {
    executor: Executor,
}
impl IntoIterator for ReentrantSource {
    type Item = fn() -> Result<thread::ThreadId, ()>;
    type IntoIter = IntoIter<Self::Item>;
    fn into_iter(self) -> Self::IntoIter {
        let caller = thread::current().id();
        let nested = self
            .executor
            .call((0..2).map(|_| move || Ok::<_, ()>(thread::current().id())))
            .expect("nested conversion batch");
        assert!(
            nested.outputs().iter().all(|o| *o.value() == caller),
            "into_iter must be protected by active guard"
        );
        let task: Self::Item = || Ok(thread::current().id());
        vec![task, task].into_iter()
    }
}

#[test]
fn test_callable_reentrancy_boundaries() {
    if env::var_os("QUBIT_CALLABLE_REENTRANCY_CHILD").is_none() {
        run_with_watchdog(
            "test_callable_reentrancy_boundaries",
            "QUBIT_CALLABLE_REENTRANCY_CHILD",
            "callable reentrancy",
        );
        return;
    }
    let executor = Executor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("pool");
    let caller = thread::current().id();
    let converted = executor
        .call_with_count(
            ReentrantSource {
                executor: executor.clone(),
            },
            2,
        )
        .expect("outer conversion batch");
    assert!(
        converted.outputs().iter().all(|o| *o.value() != caller),
        "ordinary call must remain parallel"
    );

    let source_executor = executor.clone();
    let source = (0..2).map(move |_| {
        let current = thread::current().id();
        let nested = source_executor
            .call([|| Ok::<_, ()>(thread::current().id())])
            .expect("source next nesting");
        assert_eq!(*nested.outputs()[0].value(), current);
        || Ok::<_, ()>(1)
    });
    assert!(executor.call(source).expect("source next batch").outcome().is_success());

    let different_pool = Executor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("second pool");
    let barrier = Barrier::new(2);
    let tasks = (0..2).map(|_| {
        || {
            barrier.wait();
            let current = thread::current().id();
            let same = executor
                .call([|| Ok::<_, ()>(thread::current().id())])
                .expect("same pool call");
            assert_eq!(*same.outputs()[0].value(), current);
            let different = different_pool
                .call([|| Ok::<_, ()>(thread::current().id())])
                .expect("different pool call");
            assert_ne!(*different.outputs()[0].value(), current);
            Ok::<_, ()>(NonClone { value: 9 })
        }
    });
    assert_eq!(executor.call(tasks).expect("nested worker calls").outputs().len(), 2);
}

/// Source-conversion panics must not leave the callable execution guard active.
#[test]
fn test_callable_conversion_panic_does_not_leak_active_guard() {
    const CHILD: &str = "QUBIT_CALLABLE_CONVERSION_PANIC_CHILD";
    if env::var_os(CHILD).is_none() {
        run_with_watchdog(
            "test_callable_conversion_panic_does_not_leak_active_guard",
            CHILD,
            "callable source-conversion panic recovery",
        );
        return;
    }

    /// Panics before producing an iterator to exercise execution-guard cleanup.
    struct PanicOnConversion;
    impl IntoIterator for PanicOnConversion {
        type Item = fn() -> Result<thread::ThreadId, ()>;
        type IntoIter = IntoIter<Self::Item>;

        fn into_iter(self) -> Self::IntoIter {
            panic!("callable source conversion panic");
        }
    }

    let executor = Executor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("two-worker executor must build");
    let panic = catch_unwind(AssertUnwindSafe(|| executor.call_with_count(PanicOnConversion, 2)))
        .expect_err("source conversion panic must propagate");
    assert_eq!(
        panic.downcast_ref::<&str>().copied(),
        Some("callable source conversion panic")
    );

    let caller = thread::current().id();
    let result = executor
        .call((0..2).map(|_| || Ok::<_, ()>(thread::current().id())))
        .expect("executor must remain reusable after conversion panic");
    assert!(result.outcome().is_success());
    assert_eq!(result.outcome().completed_count(), 2);
    assert_eq!(result.outputs().len(), 2);
    for (index, output) in result.outputs().iter().enumerate() {
        assert_eq!(output.index(), index);
        assert_ne!(*output.value(), caller, "recovery call must remain parallel");
    }
}
