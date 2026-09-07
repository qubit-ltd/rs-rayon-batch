// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Behavioral assertions independent of a backend's scheduler error type.

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTaskError;
use qubit_batch::BatchTermination;

/// Checks empty, counted, fallible, panicking, borrowed, and callable work.
/// The executor must use the continue policy. Assertions avoid elapsed times
/// and scheduling-dependent completion order.
pub fn assert_backend_contract<B: BatchExecutor>(executor: &B) {
    let empty = executor
        .for_each([] as [usize; 0], |_| Ok::<(), &'static str>(()))
        .expect("empty source must succeed");
    assert!(empty.is_success());
    assert_eq!(empty.completed_count(), 0);

    let calls = AtomicUsize::new(0);
    let short = executor
        .for_each_with_count(0..3, 4, |_| {
            calls.fetch_add(1, Ordering::Relaxed);
            Ok::<(), &'static str>(())
        })
        .expect_err("short source must fail count validation");
    match short {
        BatchExecutionError::CountShortfall {
            expected,
            actual,
            outcome,
            ..
        } => {
            assert_eq!((expected, actual, outcome.completed_count()), (4, 3, 3));
        }
        error => panic!("unexpected count error: {error:?}"),
    }
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    for count in [0, 3] {
        calls.store(0, Ordering::Relaxed);
        let error = executor
            .for_each_with_count(0..count + 2, count, |_| {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok::<(), &'static str>(())
            })
            .expect_err("excess input must be observed without execution");
        match error {
            BatchExecutionError::CountExceeded {
                expected,
                observed_at_least,
                outcome,
                ..
            } => {
                assert_eq!((expected, observed_at_least), (count, count + 1));
                assert_eq!(outcome.completed_count(), count);
            }
            error => panic!("unexpected count error: {error:?}"),
        }
        assert_eq!(calls.load(Ordering::Relaxed), count);
    }

    let result = executor
        .call((0..4).map(|index| {
            move || match index {
                1 => Err("invalid row"),
                2 => panic!("row panic"),
                _ => Ok(index * 10),
            }
        }))
        .expect("task failures must remain in the callable outcome");
    assert_eq!(result.outcome().termination(), BatchTermination::Finished);
    assert_eq!(result.outcome().completed_count(), 4);
    assert_eq!(result.outcome().succeeded_count(), 2);
    assert_eq!(result.outcome().failed_count(), 1);
    assert_eq!(result.outcome().panicked_count(), 1);
    let outputs: Vec<_> = result
        .outputs()
        .iter()
        .map(|output| (output.index(), *output.value()))
        .collect();
    assert_eq!(outputs, [(0, 0), (3, 30)]);
    let failures = result.outcome().failures();
    assert_eq!(
        failures.iter().map(|failure| failure.index()).collect::<Vec<_>>(),
        [1, 2]
    );
    assert!(matches!(failures[0].error(), BatchTaskError::Failed("invalid row")));
    assert!(failures[1].error().is_panicked());

    let partial = executor
        .call_with_count((0..3).map(|index| move || Ok::<_, &'static str>(index)), 4)
        .expect_err("call must preserve outputs on a batch count error");
    assert_eq!(partial.outcome().succeeded_count(), 3);
    assert_eq!(
        partial
            .outputs()
            .iter()
            .map(|output| (output.index(), *output.value()))
            .collect::<Vec<_>>(),
        [(0, 0), (1, 1), (2, 2)]
    );

    let borrowed = [String::from("first"), String::from("second")];
    let result = executor
        .call(borrowed.iter().map(|item| move || Ok::<_, &'static str>(item.as_str())))
        .expect("scoped callables may borrow inputs and return borrowed values");
    assert_eq!(*result.outputs()[0].value(), "first");
    assert_eq!(*result.outputs()[1].value(), "second");
}

/// Checks cooperative stopping without assuming the scheduler's exact window.
/// Configure the supplied executor with StopOnFirstFailure. An infinite lazy
/// source is safe here because every task fails and admission must stop.
pub fn assert_failure_policy_contract<B: BatchExecutor>(executor: &B) {
    let completed = AtomicUsize::new(0);
    let source = std::iter::repeat_with(|| {
        || {
            completed.fetch_add(1, Ordering::Relaxed);
            Err::<(), &'static str>("stop")
        }
    });
    let outcome = executor
        .execute_with_count(source, 1_000_000)
        .expect("failure policy must return a partial outcome");
    assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
    assert!(outcome.completed_count() > 0);
    assert!(outcome.completed_count() < outcome.task_count());
    assert_eq!(outcome.completed_count(), outcome.failed_count());
    assert_eq!(outcome.completed_count(), completed.load(Ordering::Relaxed));
}
