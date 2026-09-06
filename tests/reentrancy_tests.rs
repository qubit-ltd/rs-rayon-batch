// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Regression tests for nested Rayon batch execution.

#![allow(clippy::result_large_err)]

use std::process::Command;
use std::process::Stdio;
use std::sync::Barrier;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::TaskFailurePolicy;
use qubit_rayon_batch::RayonBatchExecutor;

const SAME_POOL_CHILD: &str = "QUBIT_BATCH_NESTED_SAME_POOL_TEST_CHILD";
const DIFFERENT_POOL_CHILD: &str = "QUBIT_BATCH_NESTED_DIFFERENT_POOL_TEST_CHILD";
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(10);

/// Runs one test in an independently terminable subprocess.
///
/// The environment marker prevents recursive subprocess creation. The parent
/// polls only to enforce the watchdog; barriers inside the child establish the
/// concurrency conditions exercised by each test.
///
/// # Parameters
///
/// * `test_name` - Exact test-harness name to execute in the child.
/// * `child_environment` - Environment marker that selects the child branch.
///
/// # Panics
///
/// Panics if the child cannot be managed, exits unsuccessfully, or exceeds the
/// watchdog timeout.
fn run_with_watchdog(test_name: &str, child_environment: &str) {
    let mut child = Command::new(std::env::current_exe().expect("the current test executable should be available"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(child_environment, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("the nested batch test subprocess should start");
    let deadline = Instant::now() + WATCHDOG_TIMEOUT;
    loop {
        if let Some(status) = child
            .try_wait()
            .expect("the nested batch test subprocess should remain observable")
        {
            assert!(status.success(), "nested subprocess failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("the timed-out nested batch test subprocess should be killed");
            let _ = child.wait();
            panic!("nested batch exceeded the 10-second watchdog");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

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
                    assert_eq!(thread::current().id(), caller);
                    Ok::<(), &'static str>(())
                })
                .expect("same-pool nested execution should return an outcome");
            assert!(nested_outcome.is_success());
            Ok::<(), &'static str>(())
        })
        .expect("same-pool outer execution should return an outcome");

    assert!(outcome.is_success());
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
            assert_eq!(nested_outcome.completed_count(), 1);
            assert_eq!(nested_outcome.failed_count(), 1);
            assert_eq!(
                nested_outcome.termination(),
                BatchTermination::StoppedByTaskFailurePolicy
            );
            Ok::<(), &'static str>(())
        })
        .expect("same-pool outer execution should return an outcome");
    assert!(stop_outcome.is_success());

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
            assert_eq!(nested_outcome.completed_count(), 16);
            assert_eq!(nested_outcome.panicked_count(), 1);
            Ok::<(), &'static str>(())
        })
        .expect("same-pool outer execution should return an outcome");
    assert!(panic_outcome.is_success());
}

#[test]
fn nested_same_pool_completes() {
    if std::env::var_os(SAME_POOL_CHILD).is_some() {
        for (thread_count, sequential_threshold, inner_count, outer_count) in
            [(2, 0, 16, 2), (4, 0, 16, 4), (2, 100, 128, 128)]
        {
            run_same_pool_success_case(thread_count, sequential_threshold, inner_count, outer_count);
        }
        run_same_pool_failure_cases();
        return;
    }

    run_with_watchdog("nested_same_pool_completes", SAME_POOL_CHILD);
}

#[test]
fn nested_different_pool_remains_parallel() {
    if std::env::var_os(DIFFERENT_POOL_CHILD).is_some() {
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
                        assert_ne!(thread::current().id(), caller);
                        Ok::<(), &'static str>(())
                    })
                    .expect("different-pool nested execution should return an outcome");
                assert!(nested_outcome.is_success());
                Ok::<(), &'static str>(())
            })
            .expect("different-pool outer execution should return an outcome");

        assert!(outcome.is_success());
        return;
    }

    run_with_watchdog("nested_different_pool_remains_parallel", DIFFERENT_POOL_CHILD);
}

#[test]
fn nested_from_source_producer_completes() {
    const CHILD: &str = "QUBIT_BATCH_NESTED_SOURCE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        run_with_watchdog("nested_from_source_producer_completes", CHILD);
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
                    assert_eq!(thread::current().id(), producer);
                    Ok::<(), ()>(())
                })
                .expect("source producer nested execution should return");
            assert!(inner.is_success());
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
            .is_success()
    );
}

#[test]
fn source_panic_does_not_leak_active_guard() {
    use std::panic::AssertUnwindSafe;
    use std::panic::catch_unwind;

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
    assert!(result.is_err());
    let caller = thread::current().id();
    assert!(
        executor
            .for_each(0..2, |_| {
                assert_ne!(thread::current().id(), caller);
                Ok::<(), ()>(())
            })
            .expect("executor should remain usable after source panic")
            .is_success()
    );
}

#[test]
fn source_producer_can_use_a_different_pool_in_parallel() {
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
    let producer = thread::current().id();
    let tasks = (0..2).map(|_| {
        let inner = inner.clone();
        move || {
            assert!(
                inner
                    .for_each(0..2, |_| {
                        assert_ne!(thread::current().id(), producer);
                        Ok::<(), ()>(())
                    })
                    .expect("different pool nested execution should return")
                    .is_success()
            );
            Ok::<(), ()>(())
        }
    });
    assert!(
        outer
            .execute(tasks)
            .expect("outer execution should return")
            .is_success()
    );
}
