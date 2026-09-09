// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures sequential, scoped-thread, and Rayon execution costs.

use std::hint::black_box;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_batch::BatchExecutor;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::SequentialBatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

/// Performs repeatable CPU work with optional skew.
fn work(seed: usize, rounds: usize, skewed: bool) {
    let rounds = if skewed && seed.is_multiple_of(16) {
        rounds * 16
    } else {
        rounds
    };
    let mut value = black_box(seed as u64);
    for step in 0..rounds {
        value = black_box(value.rotate_left(7).wrapping_mul(6_364_136_223_846_793_005) ^ step as u64);
    }
    black_box(value);
}

/// Measures one batch and validates counts before timing.
fn register_executor<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(label);
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    for count in [32usize, 100, 101, 1024, 65536] {
        for rounds in [0usize, 64, 4096] {
            for skewed in [false, true] {
                let id = BenchmarkId::new(format!("rounds-{rounds}-skew-{skewed}"), count);
                group.bench_function(id, |bencher| {
                    let check = executor
                        .for_each_with_count(0..count, count, |_| Ok::<(), ()>(()))
                        .expect("setup batch");
                    assert_eq!(check.completed_count(), count);
                    bencher.iter(|| {
                        let outcome = executor
                            .for_each_with_count(0..count, count, |index| {
                                work(index, rounds, skewed);
                                Ok::<(), ()>(())
                            })
                            .expect("benchmark batch should succeed");
                        let _ = black_box(outcome);
                    });
                });
            }
        }
    }
    group.finish();
}

/// Includes caller thread creation and joins in the measured operation.
fn register_concurrent<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(format!("concurrent-{label}"));
    group.sample_size(30);
    for callers in [2usize, 4] {
        group.bench_function(BenchmarkId::from_parameter(callers), |bencher| {
            let check = executor
                .for_each_with_count(0..1024, 1024, |_| Ok::<(), ()>(()))
                .expect("setup batch");
            assert_eq!(check.completed_count(), 1024);
            bencher.iter(|| {
                std::thread::scope(|scope| {
                    let mut handles = Vec::with_capacity(callers);
                    for _ in 0..callers {
                        handles.push(scope.spawn(|| {
                            executor
                                .for_each_with_count(0..1024, 1024, |index| {
                                    work(index, 64, true);
                                    Ok::<(), ()>(())
                                })
                                .expect("concurrent benchmark batch should succeed")
                                .completed_count()
                        }));
                    }
                    for handle in handles {
                        black_box(handle.join().expect("benchmark caller should join"));
                    }
                });
            });
        });
    }
    group.finish();
}

/// Reuses caller threads while measuring request handoff and execution.
fn register_steady_callers<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(format!("steady-callers-{label}"));
    group.sample_size(30);
    for callers in [2usize, 4] {
        group.bench_function(BenchmarkId::from_parameter(callers), |bencher| {
            thread::scope(|scope| {
                let mut connections = Vec::with_capacity(callers);
                let mut handles = Vec::with_capacity(callers);
                for _ in 0..callers {
                    let (request_sender, request_receiver) = mpsc::channel::<()>();
                    let (result_sender, result_receiver) = mpsc::channel();
                    handles.push(scope.spawn(move || {
                        while request_receiver.recv().is_ok() {
                            let outcome = executor
                                .for_each_with_count(0..1024, 1024, |index| {
                                    work(index, 64, true);
                                    Ok::<(), ()>(())
                                })
                                .expect("steady caller batch should succeed");
                            if result_sender.send(outcome.completed_count()).is_err() {
                                break;
                            }
                        }
                    }));
                    connections.push((request_sender, result_receiver));
                }
                for (sender, _) in &connections {
                    sender.send(()).expect("caller must accept setup work");
                }
                for (_, receiver) in &connections {
                    assert_eq!(receiver.recv().expect("setup work must finish"), 1024);
                }
                bencher.iter(|| {
                    for (sender, _) in &connections {
                        sender.send(()).expect("caller must accept measured work");
                    }
                    for (_, receiver) in &connections {
                        black_box(receiver.recv().expect("measured work must finish"));
                    }
                });
                drop(connections);
                for handle in handles {
                    handle.join().expect("steady caller must join");
                }
            });
        });
    }
    group.finish();
}

/// Registers the runtime comparison matrix.
fn benchmarks(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    register_executor(criterion, "sequential", &sequential);
    for threads in [1usize, 2, 4]
        .into_iter()
        .filter(|threads| *threads <= std::thread::available_parallelism().map(usize::from).unwrap_or(1))
    {
        let scoped = ParallelBatchExecutor::builder()
            .thread_count(threads)
            .sequential_threshold(0)
            .build()
            .expect("scoped executor should build");
        let rayon = RayonBatchExecutor::builder()
            .thread_count(threads)
            .sequential_threshold(0)
            .build()
            .expect("Rayon executor should build");
        register_executor(criterion, &format!("scoped-{threads}"), &scoped);
        register_executor(criterion, &format!("rayon-{threads}"), &rayon);
        register_concurrent(criterion, &format!("scoped-{threads}"), &scoped);
        register_concurrent(criterion, &format!("rayon-{threads}"), &rayon);
        register_steady_callers(criterion, &format!("rayon-{threads}"), &rayon);
        register_gate(criterion, &format!("rayon-{threads}"), &rayon);
    }
}

/// Selected gate workloads include a slow producer and skewed worker costs.
fn register_gate<E: BatchExecutor>(c: &mut Criterion, label: &str, executor: &E) {
    let mut group = c.benchmark_group(format!("gate-{label}"));
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(500));
    for (name, rounds, skewed, slow_source) in [
        ("fine", 64usize, false, false),
        ("noop", 0, false, false),
        ("heavy", 4096, false, false),
        ("skew", 4096, true, false),
        ("slow-source", 64, false, true),
    ] {
        let run = || {
            executor
                .for_each_with_count(
                    (0..1024).inspect(|index| {
                        if slow_source && index % 16 == 0 {
                            work(*index, 4096, false);
                        }
                    }),
                    1024,
                    |index| {
                        work(index, rounds, skewed);
                        Ok::<(), ()>(())
                    },
                )
                .expect("gate batch")
        };
        assert_eq!(run().completed_count(), 1024);
        group.bench_function(name, |b| b.iter(|| black_box(run())));
    }
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
