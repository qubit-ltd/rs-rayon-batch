// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! End-to-end callable collection costs with pools constructed outside timing.
use std::hint::black_box;
use std::time::Duration;

use criterion::BenchmarkId;
use criterion::Criterion;
use criterion::criterion_group;
use criterion::criterion_main;
use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor as Executor;

/// Measures scalar and owned outputs for one executor configuration.
fn measure(c: &mut Criterion, label: &str, executor: &Executor) {
    let mut group = c.benchmark_group(label);
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(500));
    for count in [0usize, 1, 32, 100, 101, 1024] {
        let check = executor
            .call((0..count).map(|i| move || Ok::<_, ()>(i)))
            .expect("setup batch");
        assert_eq!(check.outcome().succeeded_count(), count);
        assert_eq!(check.outputs().len(), count);
        group.bench_with_input(BenchmarkId::new("scalar", count), &count, |b, &n| {
            b.iter(|| {
                black_box(
                    executor
                        .call((0..n).map(|i| move || Ok::<_, ()>(i)))
                        .expect("measured batch"),
                )
            });
        });
        let check = executor
            .call((0..count).map(|i| move || Ok::<_, ()>(i.to_string())))
            .expect("owned setup");
        assert_eq!(check.outputs().len(), count);
        group.bench_with_input(BenchmarkId::new("owned", count), &count, |b, &n| {
            b.iter(|| {
                black_box(
                    executor
                        .call((0..n).map(|i| move || Ok::<_, ()>(i.to_string())))
                        .expect("owned batch"),
                )
            });
        });
    }
    group.finish();
}
/// Registers fallback, forced parallel, and single-worker configurations.
fn benchmarks(c: &mut Criterion) {
    for (name, workers, threshold) in [("default", 2, 100), ("parallel", 2, 0), ("one-worker", 1, 0)] {
        let executor = Executor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("executor");
        measure(c, name, &executor);
    }
}
criterion_group!(benches, benchmarks);
criterion_main!(benches);
