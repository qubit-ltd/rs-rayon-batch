// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for nested Rayon batch execution.

#![allow(clippy::result_large_err)]

use std::env::var_os;
use std::iter::once;
use std::panic::AssertUnwindSafe;
use std::panic::catch_unwind;
use std::sync::Barrier;
use std::thread;

use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::TaskFailurePolicy;
use qubit_rayon_batch::RayonBatchExecutor;

use self::watchdog::run_with_watchdog;

mod watchdog;

const SAME_POOL_CHILD: &str = "QUBIT_BATCH_NESTED_SAME_POOL_TEST_CHILD";
const DIFFERENT_POOL_CHILD: &str = "QUBIT_BATCH_NESTED_DIFFERENT_POOL_TEST_CHILD";
const DIFFERENT_POOL_SOURCE_CHILD: &str = "QUBIT_BATCH_DIFFERENT_POOL_SOURCE_CHILD";

/// Exercises successful same-pool nesting for one executor configuration.
///
/// # Parameters
///
/// * `thread_count` - Number of workers in the dedicated Rayon pool.
/// * `sequential_threshold` - Sequential fallback threshold for both levels.
/// * `inner_count` - Number of items in each nested batch.
/// * `outer_count` - Number of items in the outer batch.
///
/// # Panics
///
/// Panics if the executor cannot be built or either batch violates the
/// expected same-thread sequential fallback behavior.
fn run_same_pool_success_case(
    thread_count: usize,
    sequential_threshold: usize,
    inner_count: usize,
    outer_count: usize,
) {
    let executor = RayonBatchExecutor::builder()
        .thread_count(thread_count)
        .sequential_threshold(sequential_threshold)
        .build()
        .expect("the same-pool test executor should build");
    let nested_executor = executor.clone();
    let barrier = Barrier::new(thread_count);

    let outcome = executor
        .for_each(0..outer_count, |outer_index| {
            if outer_index >= thread_count {
                return Ok::<(), &'static str>(());
            }
            barrier.wait();
            let caller = thread::current().id();
            let nested_outcome = nested_executor
                .for_each(0..inner_count, |_| {
                    assert_eq!(
                        thread::current().id(),
                        caller,
                        "same-pool nesting must stay on the caller thread"
                    );
                    Ok::<(), &'static str>(())
                })
                .expect("same-pool nested execution should return an outcome");
            assert!(
                nested_outcome.is_success(),
                "nested execution must complete successfully"
            );
            Ok::<(), &'static str>(())
        })
        .expect("same-pool outer execution should return an outcome");

    assert!(outcome.is_success(), "outer execution must complete successfully");
}

/// Exercises nested failure and panic outcomes on the same Rayon pool.
///
/// # Panics
///
/// Panics if the executor cannot be built or nested execution does not retain
/// sequential failure-policy and panic-capture semantics.
fn run_same_pool_failure_cases() {
    let stop_executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure)
        .build()
        .expect("the stop-on-failure executor should build");
    let nested_stop_executor = stop_executor.clone();
    let stop_barrier = Barrier::new(2);
    let stop_outcome = stop_executor
        .for_each(0..2, |_| {
            stop_barrier.wait();
            let nested_outcome = nested_stop_executor
                .for_each(0..16, |index| {
                    if index == 0 {
                        Err("expected nested failure")
                    } else {
                        Ok(())
                    }
                })
                .expect("same-pool nested failure should return an outcome");
            assert_eq!(
                nested_outcome.completed_count(),
                1,
                "nested execution must stop after its first failure"
            );
            assert_eq!(
                nested_outcome.failed_count(),
                1,
                "the first nested task must be recorded as failed"
            );
            assert_eq!(
                nested_outcome.termination(),
                BatchTermination::StoppedByTaskFailurePolicy,
                "nested termination must retain the stop-on-first-failure policy"
            );
            Ok::<(), &'static str>(())
        })
        .expect("same-pool outer execution should return an outcome");
    assert!(
        stop_outcome.is_success(),
        "the outer batch must handle the observed nested failure"
    );

    let panic_executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("the panic-capture executor should build");
    let nested_panic_executor = panic_executor.clone();
    let panic_barrier = Barrier::new(2);
    let panic_outcome = panic_executor
        .for_each(0..2, |_| {
            panic_barrier.wait();
            let nested_outcome = nested_panic_executor
                .for_each(0..16, |index| {
                    if index == 0 {
                        panic!("expected nested panic");
                    }
                    Ok::<(), &'static str>(())
                })
                .expect("same-pool nested panic should return an outcome");
            assert_eq!(
                nested_outcome.completed_count(),
                16,
                "panic capture must allow every nested task to complete"
            );
            assert_eq!(
                nested_outcome.panicked_count(),
                1,
                "the nested panic must be recorded exactly once"
            );
            Ok::<(), &'static str>(())
        })
        .expect("same-pool outer execution should return an outcome");
    assert!(
        panic_outcome.is_success(),
        "the outer batch must handle the captured nested panic"
    );
}

#[test]
fn test_nested_same_pool_completes() {
    if var_os(SAME_POOL_CHILD).is_some() {
        for (thread_count, sequential_threshold, inner_count, outer_count) in
            [(2, 0, 16, 2), (4, 0, 16, 4), (2, 100, 128, 128)]
        {
            run_same_pool_success_case(thread_count, sequential_threshold, inner_count, outer_count);
        }
        run_same_pool_failure_cases();
        return;
    }

    run_with_watchdog("test_nested_same_pool_completes", SAME_POOL_CHILD, "same-pool nesting");
}

#[test]
fn test_nested_different_pool_remains_parallel() {
    if var_os(DIFFERENT_POOL_CHILD).is_some() {
        let outer_executor = RayonBatchExecutor::builder()
            .thread_count(2)
            .sequential_threshold(0)
            .build()
            .expect("the outer executor should build");
        let inner_executor = RayonBatchExecutor::builder()
            .thread_count(2)
            .sequential_threshold(0)
            .build()
            .expect("the independent inner executor should build");
        let barrier = Barrier::new(2);

        let outcome = outer_executor
            .for_each(0..2, |_| {
                barrier.wait();
                let caller = thread::current().id();
                let nested_outcome = inner_executor
                    .for_each(0..16, |_| {
                        assert_ne!(
                            thread::current().id(),
                            caller,
                            "parallel execution must use a pool worker"
                        );
                        Ok::<(), &'static str>(())
                    })
                    .expect("different-pool nested execution should return an outcome");
                assert!(
                    nested_outcome.is_success(),
                    "nested execution must complete successfully"
                );
                Ok::<(), &'static str>(())
            })
            .expect("different-pool outer execution should return an outcome");

        assert!(outcome.is_success(), "outer execution must complete successfully");
        return;
    }

    run_with_watchdog(
        "test_nested_different_pool_remains_parallel",
        DIFFERENT_POOL_CHILD,
        "different-pool nesting",
    );
}

#[test]
fn test_nested_from_source_producer_completes() {
    const CHILD: &str = "QUBIT_BATCH_NESTED_SOURCE_CHILD";
    if var_os(CHILD).is_none() {
        run_with_watchdog(
            "test_nested_from_source_producer_completes",
            CHILD,
            "source-producer nesting",
        );
        return;
    }
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("the source producer test executor should build");
    let nested = executor.clone();
    let barrier = Barrier::new(3);
    let producer = thread::current().id();
    let tasks = (0..3).map(|index| {
        if index == 2 {
            barrier.wait();
            let inner = nested
                .for_each(0..2, |_| {
                    assert_eq!(
                        thread::current().id(),
                        producer,
                        "same-pool source nesting must stay on the producer thread"
                    );
                    Ok::<(), ()>(())
                })
                .expect("source producer nested execution should return");
            assert!(inner.is_success(), "source-producer nesting must complete successfully");
        }
        let barrier = &barrier;
        move || {
            if index < 2 {
                barrier.wait();
            }
            Ok::<(), ()>(())
        }
    });
    assert!(
        executor
            .execute(tasks)
            .expect("outer execution should return")
            .is_success(),
        "source-producer reentry must leave the outer batch successful"
    );
}

#[test]
fn test_source_panic_does_not_leak_active_guard() {
    const CHILD: &str = "QUBIT_RAYON_GUARD_CLEANUP_CHILD";
    if var_os(CHILD).is_none() {
        run_with_watchdog(
            "test_source_panic_does_not_leak_active_guard",
            CHILD,
            "source-panic guard cleanup",
        );
        return;
    }

    struct PanicSource;

    impl Iterator for PanicSource {
        type Item = fn() -> Result<(), ()>;

        fn next(&mut self) -> Option<Self::Item> {
            panic!("source panic");
        }
    }

    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("the guard cleanup executor should build");
    let result = catch_unwind(AssertUnwindSafe(|| executor.execute_with_count(PanicSource, 2)));
    assert!(result.is_err(), "the source panic must unwind out of execution");
    let caller = thread::current().id();
    assert!(
        executor
            .for_each(0..2, |_| {
                assert_ne!(
                    thread::current().id(),
                    caller,
                    "parallel execution must use a pool worker"
                );
                Ok::<(), ()>(())
            })
            .expect("executor should remain usable after source panic")
            .is_success(),
        "source panic cleanup must preserve successful parallel execution"
    );
}

#[test]
fn test_source_producer_can_use_a_different_pool_in_parallel() {
    if var_os(DIFFERENT_POOL_SOURCE_CHILD).is_none() {
        run_with_watchdog(
            "test_source_producer_can_use_a_different_pool_in_parallel",
            DIFFERENT_POOL_SOURCE_CHILD,
            "parallel different-pool source",
        );
        return;
    }

    let outer = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("outer executor should build");
    let inner = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("inner executor should build");
    let tasks = (0..2).map(|_| {
        let producer = thread::current().id();
        let rendezvous = Barrier::new(2);
        let nested = inner
            .for_each(0..2, |_| {
                assert_ne!(
                    thread::current().id(),
                    producer,
                    "different-pool source nesting must use an independent worker"
                );
                rendezvous.wait();
                Ok::<(), ()>(())
            })
            .expect("different pool source execution should return");
        assert!(
            nested.is_success(),
            "different-pool source nesting must complete successfully"
        );
        assert_eq!(
            nested.completed_count(),
            2,
            "both different-pool nested tasks must complete"
        );
        || Ok::<(), ()>(())
    });
    let outcome = outer.execute(tasks).expect("outer execution should return");
    assert!(outcome.is_success(), "outer execution must complete successfully");
    assert_eq!(outcome.completed_count(), 2, "both source-produced tasks must complete");
}

#[test]
fn test_nested_task_failure_is_mapped_to_outer_failure() {
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .build()
        .expect("nested example executor should build");
    for fail_inner in [false, true] {
        let nested_executor = executor.clone();
        let outcome = executor
            .execute_with_count(
                once(move || {
                    let nested_outcome = nested_executor
                        .execute_with_count(
                            [move || {
                                if fail_inner {
                                    Err::<(), &'static str>("invalid nested record")
                                } else {
                                    Ok(())
                                }
                            }],
                            1,
                        )
                        .map_err(|_| "nested batch failed")?;
                    if nested_outcome.is_success() {
                        Ok(())
                    } else {
                        Err("nested task failed")
                    }
                }),
                1,
            )
            .expect("outer batch should return an outcome");
        assert_eq!(
            outcome.completed_count(),
            1,
            "the outer task must complete for fail_inner={fail_inner}"
        );
        assert_eq!(
            outcome.is_success(),
            !fail_inner,
            "outer success must reflect nested failure for fail_inner={fail_inner}"
        );
        assert_eq!(
            outcome.failed_count(),
            usize::from(fail_inner),
            "nested failure must map to exactly one outer failure"
        );
        assert_eq!(
            outcome.panicked_count(),
            0,
            "nested task failure must not become an outer panic"
        );
        if fail_inner {
            assert_eq!(
                outcome.failures()[0].index(),
                0,
                "nested failure must be attributed to the first outer task"
            );
        }
    }
}
