// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Manual, process-isolated diagnostics for cyclic cross-pool calls.

use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

#[test]
#[ignore = "manual diagnostic: may terminate stalled subprocesses"]
fn test_cross_pool_cycle_probe() {
    const MARKER: &str = "QUBIT_RAYON_CYCLE_PROBE";
    if let Some(case) = std::env::var_os(MARKER) {
        let case = case.to_str().expect("ASCII case");
        let (workers, items) = case.split_once(':').expect("workers:items");
        let workers: usize = workers.parse().expect("worker count");
        let items: usize = items.parse().expect("item count");
        let a = RayonBatchExecutor::builder()
            .thread_count(workers)
            .sequential_threshold(0)
            .build()
            .expect("pool A");
        let b = RayonBatchExecutor::builder()
            .thread_count(workers)
            .sequential_threshold(0)
            .build()
            .expect("pool B");
        let a_entered = Arc::new(Barrier::new(workers));
        let b_entered = Arc::new(Barrier::new(workers));
        let b_started = AtomicUsize::new(0);
        let result = a.for_each(0..workers, |_| {
            a_entered.wait();
            let inner = b.for_each(0..items, |_| {
                let ordinal = b_started.fetch_add(1, Ordering::SeqCst);
                if ordinal < workers {
                    eprintln!("RB003 B_TO_A_ENTERED ordinal={ordinal}");
                    b_entered.wait();
                }
                let nested = a.for_each(0..items, |_| Ok::<(), ()>(()));
                assert!(nested.expect("nested A outcome").is_success());
                Ok::<(), ()>(())
            });
            assert!(inner.expect("B outcome").is_success());
            Ok::<(), ()>(())
        });
        assert!(result.expect("outer A outcome").is_success());
        eprintln!("RB003 COMPLETED");
        return;
    }

    for workers in [2usize, 4] {
        for round in 1..=3 {
            for items in [1usize, 32] {
                let mut child = Command::new(std::env::current_exe().expect("test executable"))
                    .args(["--ignored", "--exact", "test_cross_pool_cycle_probe", "--nocapture"])
                    .env(MARKER, format!("{workers}:{items}"))
                    .stdin(Stdio::null())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::piped())
                    .spawn()
                    .expect("probe child");
                let deadline = Instant::now() + Duration::from_secs(10);
                let timed_out = loop {
                    if child.try_wait().expect("child status").is_some() {
                        break false;
                    }
                    if Instant::now() >= deadline {
                        // The child may exit between try_wait and kill. Always
                        // consume it with wait_with_output below, which reaps
                        // either the killed child or that just-exited child.
                        let _ = child.kill();
                        break true;
                    }
                    thread::sleep(Duration::from_millis(10));
                };
                let output = child.wait_with_output().expect("reap probe child");
                let stderr = String::from_utf8_lossy(&output.stderr);
                let entered_count = stderr
                    .lines()
                    .filter(|line| line.contains("RB003 B_TO_A_ENTERED"))
                    .count();
                eprintln!(
                    "RB003 workers={workers} items={items} round={round} timeout={timed_out} status={} b_to_a_entered={entered_count}\n{stderr}",
                    output.status,
                );
                assert_eq!(entered_count, workers, "all B-to-A workers must enter");
                if items == 1 {
                    assert!(!timed_out && output.status.success(), "control must complete");
                } else if !timed_out {
                    assert!(output.status.success(), "probe panic is not evidence of deadlock");
                }
            }
        }
    }
}
