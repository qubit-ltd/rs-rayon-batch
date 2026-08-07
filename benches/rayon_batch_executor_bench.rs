// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Comparative baselines for sequential, scoped-thread, and Rayon execution.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use qubit_batch::{BatchExecutor, ParallelBatchExecutor, SequentialBatchExecutor};
use qubit_function::Runnable;
use qubit_rayon_batch::RayonBatchExecutor;

/// Batch sizes around the executors' default sequential threshold.
const BATCH_SIZES: [usize; 5] = [32, 64, 100, 128, 256];

/// Task that measures executor dispatch overhead without application work.
#[derive(Clone, Copy)]
struct NoOpTask;

impl Runnable<()> for NoOpTask {
    /// Completes without performing application work.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())`.
    #[inline]
    fn run(&mut self) -> Result<(), ()> {
        Ok(())
    }
}

/// Task that performs deterministic CPU work before completing.
#[derive(Clone, Copy)]
struct CpuTask {
    /// Per-task seed that prevents identical loop inputs.
    seed: u64,
}

impl Runnable<()> for CpuTask {
    /// Performs a bounded CPU workload.
    ///
    /// # Returns
    ///
    /// Always returns `Ok(())` after black-boxing the calculated value.
    #[inline]
    fn run(&mut self) -> Result<(), ()> {
        let mut value = self.seed;
        for _ in 0..256 {
            value = value
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
        }
        black_box(value);
        Ok(())
    }
}

/// Benchmarks no-op execution across all supported executor implementations.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_no_op_execution(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark executor configuration should be valid");
    let rayon = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark Rayon executor configuration should be valid");
    let mut group = criterion.benchmark_group("batch_executor_no_op");

    for task_count in BATCH_SIZES {
        benchmark_no_op_case(&mut group, "sequential", &sequential, task_count);
        benchmark_no_op_case(&mut group, "scoped_parallel", &parallel, task_count);
        benchmark_no_op_case(&mut group, "rayon", &rayon, task_count);
    }
    group.finish();
}

/// Benchmarks CPU-bound execution across all supported executor
/// implementations.
///
/// # Parameters
///
/// * `criterion` - Criterion registry receiving benchmark cases.
fn benchmark_cpu_execution(criterion: &mut Criterion) {
    let sequential = SequentialBatchExecutor::new();
    let parallel = ParallelBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark executor configuration should be valid");
    let rayon = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("benchmark Rayon executor configuration should be valid");
    let mut group = criterion.benchmark_group("batch_executor_cpu");

    for task_count in BATCH_SIZES {
        benchmark_cpu_case(&mut group, "sequential", &sequential, task_count);
        benchmark_cpu_case(&mut group, "scoped_parallel", &parallel, task_count);
        benchmark_cpu_case(&mut group, "rayon", &rayon, task_count);
    }
    group.finish();
}

/// Registers one no-op benchmark case for `executor`.
///
/// # Parameters
///
/// * `group` - Benchmark group receiving the case.
/// * `executor_name` - Stable name displayed by Criterion.
/// * `executor` - Executor implementation under measurement.
/// * `task_count` - Number of tasks in each measured batch.
fn benchmark_no_op_case<E>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    executor_name: &str,
    executor: &E,
    task_count: usize,
) where
    E: BatchExecutor,
{
    group.bench_with_input(
        BenchmarkId::new(executor_name, task_count),
        &task_count,
        |bencher, &task_count| {
            bencher.iter(|| {
                let _ = black_box(
                    executor
                        .execute_with_count((0..task_count).map(|_| NoOpTask), task_count)
                        .expect("no-op batch should succeed"),
                );
            });
        },
    );
}

/// Registers one CPU-bound benchmark case for `executor`.
///
/// # Parameters
///
/// * `group` - Benchmark group receiving the case.
/// * `executor_name` - Stable name displayed by Criterion.
/// * `executor` - Executor implementation under measurement.
/// * `task_count` - Number of tasks in each measured batch.
fn benchmark_cpu_case<E>(
    group: &mut criterion::BenchmarkGroup<'_, criterion::measurement::WallTime>,
    executor_name: &str,
    executor: &E,
    task_count: usize,
) where
    E: BatchExecutor,
{
    group.bench_with_input(
        BenchmarkId::new(executor_name, task_count),
        &task_count,
        |bencher, &task_count| {
            bencher.iter(|| {
                let _ = black_box(
                    executor
                        .execute_with_count(
                            (0..task_count).map(|seed| CpuTask { seed: seed as u64 }),
                            task_count,
                        )
                        .expect("CPU batch should succeed"),
                );
            });
        },
    );
}

criterion_group!(benches, benchmark_no_op_execution, benchmark_cpu_execution,);
criterion_main!(benches);
