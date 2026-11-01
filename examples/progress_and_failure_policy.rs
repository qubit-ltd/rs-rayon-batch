// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Demonstrates progress reporting and cooperative failure policies.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use qubit_batch::BatchExecutor;
use qubit_batch::BatchTermination;
use qubit_batch::TaskFailurePolicy;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;
use qubit_rayon_batch::RayonBatchExecutor;

/// Counts lifecycle events for this introductory example.
#[derive(Default)]
struct CountingReporter {
    started: AtomicUsize,
    terminal: AtomicUsize,
    running: AtomicUsize,
}

impl Reporter for CountingReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        match event.phase() {
            Phase::Started => {
                self.started.fetch_add(1, Ordering::Relaxed);
            }
            Phase::Running => {
                self.running.fetch_add(1, Ordering::Relaxed);
            }
            Phase::Succeeded | Phase::Failed => {
                self.terminal.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        Ok(())
    }
}

fn main() {
    let reporter = Arc::new(CountingReporter::default());
    let limit = NonZeroUsize::new(2).expect("positive failure limit");
    for policy in [
        TaskFailurePolicy::Continue,
        TaskFailurePolicy::StopOnFirstFailure,
        TaskFailurePolicy::StopAfterFailures(limit),
    ] {
        let executor = RayonBatchExecutor::builder()
            .thread_count(2)
            .sequential_threshold(0)
            .report_interval(Duration::from_millis(10))
            .reporter_arc(reporter.clone())
            .task_failure_policy(policy)
            .build()
            .expect("example executor should build");
        let calls = (0..64usize).map(|index| {
            move || {
                if index % 4 == 0 {
                    Err("invalid record")
                } else {
                    Ok(index * 2)
                }
            }
        });
        let result = executor
            .call(calls)
            .expect("count and reporting should succeed");
        let outcome = result.outcome();
        assert!(!outcome.is_success());
        assert_eq!(result.outputs().len(), outcome.succeeded_count());
        for output in result.outputs() {
            assert_eq!(*output.value(), output.index() * 2);
        }
        match policy {
            TaskFailurePolicy::Continue => {
                assert_eq!(outcome.termination(), BatchTermination::Finished);
                assert_eq!(outcome.completed_count(), 64);
                assert_eq!(outcome.failed_count(), 16);
            }
            TaskFailurePolicy::StopOnFirstFailure => {
                assert!(outcome.failure_count() >= 1);
            }
            TaskFailurePolicy::StopAfterFailures(limit) => {
                assert!(outcome.failure_count() >= limit.get());
            }
            _ => panic!("example only selects the three documented policies"),
        }
        println!(
            "policy={policy:?}, termination={:?}, completed={}, failures={}",
            outcome.termination(),
            outcome.completed_count(),
            outcome.failure_count()
        );
    }
    assert_eq!(reporter.started.load(Ordering::Relaxed), 3);
    assert_eq!(reporter.terminal.load(Ordering::Relaxed), 3);
    println!(
        "running events={}",
        reporter.running.load(Ordering::Relaxed)
    );
}
