// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Measures sequential, scoped-thread, and Rayon execution costs.

use std::hint::black_box;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_batch::BatchExecutor;
use qubit_batch::ParallelBatchExecutor;
use qubit_batch::SequentialBatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

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

fn register_executor<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(label);
    group.sample_size(30);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(2));
    for count in [32usize, 100, 101, 1024] {
        for rounds in [0usize, 64, 4096] {
            for skewed in [false, true] {
                let id = BenchmarkId::new(format!("rounds-{rounds}-skew-{skewed}"), count);
                group.bench_function(id, |bencher| {
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

fn register_concurrent<E: BatchExecutor>(criterion: &mut Criterion, label: &str, executor: &E) {
    let mut group = criterion.benchmark_group(format!("concurrent-{label}"));
    group.sample_size(30);
    for callers in [2usize, 4] {
        group.bench_function(BenchmarkId::from_parameter(callers), |bencher| {
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
                        assert_eq!(handle.join().expect("benchmark caller should join"), 1024);
                    }
                });
            });
        });
    }
    group.finish();
}

fn benchmarks(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    register_executor(criterion, "sequential", &sequential);
    for threads in [2usize, 4] {
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
    }
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
