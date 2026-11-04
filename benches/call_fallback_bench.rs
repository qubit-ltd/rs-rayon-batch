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
fn measure_callable_outputs(criterion: &mut Criterion, label: &str, executor: &Executor) {
    let mut group = criterion.benchmark_group(label);
    group.sample_size(20);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_millis(500));
    for task_count in [0usize, 1, 32, 100, 101, 1024] {
        let check = executor
            .call((0..task_count).map(|i| move || Ok::<_, ()>(i)))
            .expect("setup batch");
        assert_eq!(check.outcome().succeeded_count(), task_count);
        assert_eq!(check.outputs().len(), task_count);
        group.bench_with_input(
            BenchmarkId::new("scalar", task_count),
            &task_count,
            |bencher, &input_task_count| {
                bencher.iter(|| {
                    black_box(
                        executor
                            .call((0..input_task_count).map(|i| move || Ok::<_, ()>(i)))
                            .expect("measured batch"),
                    )
                });
            },
        );
        let check = executor
            .call((0..task_count).map(|i| move || Ok::<_, ()>(i.to_string())))
            .expect("owned setup");
        assert_eq!(check.outputs().len(), task_count);
        group.bench_with_input(
            BenchmarkId::new("owned", task_count),
            &task_count,
            |bencher, &input_task_count| {
                bencher.iter(|| {
                    black_box(
                        executor
                            .call((0..input_task_count).map(|i| move || Ok::<_, ()>(i.to_string())))
                            .expect("owned batch"),
                    )
                });
            },
        );
    }
    group.finish();
}
/// Registers fallback, forced parallel, and single-worker configurations.
fn benchmark_call_fallback(criterion: &mut Criterion) {
    for (name, workers, threshold) in [("default", 2, 100), ("parallel", 2, 0), ("one-worker", 1, 0)] {
        let executor = Executor::builder()
            .thread_count(workers)
            .sequential_threshold(threshold)
            .build()
            .expect("executor");
        measure_callable_outputs(criterion, name, &executor);
    }
}
criterion_group!(benches, benchmark_call_fallback);
criterion_main!(benches);
