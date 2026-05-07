/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
//! Tests for [`RayonBatchExecutorBuilder`](qubit_rayon_batch::RayonBatchExecutorBuilder).

use std::{
    sync::Arc,
    time::Duration,
};

use qubit_rayon_batch::{
    BatchExecutor,
    ProgressPhase,
    RayonBatchExecutor,
};

use crate::support::{
    RecordingProgressReporter,
    TestTask,
};

#[test]
fn test_rayon_batch_executor_builder_applies_custom_execution_settings() {
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(7)
        .report_interval(Duration::from_millis(15))
        .thread_name_prefix(String::from("builder-test-worker"))
        .stack_size(2 * 1024 * 1024)
        .build()
        .expect("custom builder settings should build");

    assert_eq!(executor.num_threads(), 2);
    assert_eq!(executor.sequential_threshold(), 7);
    assert_eq!(executor.report_interval(), Duration::from_millis(15));
}

#[test]
fn test_rayon_batch_executor_builder_uses_shared_reporter_arc() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .num_threads(2)
        .sequential_threshold(1)
        .report_interval(Duration::from_millis(1))
        .reporter_arc(reporter.clone())
        .build()
        .expect("builder should accept shared reporter");
    let tasks = vec![TestTask::succeed(), TestTask::succeed()];

    let outcome = executor
        .execute(tasks, 2)
        .expect("batch should execute with shared reporter");
    let events = reporter.events();

    assert_eq!(outcome.completed_count(), 2);
    assert!(
        events
            .iter()
            .any(|event| event.phase() == ProgressPhase::Started)
    );
    assert!(
        events
            .iter()
            .any(|event| event.phase() == ProgressPhase::Finished)
    );
}

#[test]
fn test_rayon_batch_executor_builder_no_reporter_replaces_custom_reporter() {
    let reporter = Arc::new(RecordingProgressReporter::new());
    let executor = RayonBatchExecutor::builder()
        .reporter_arc(reporter.clone())
        .no_reporter()
        .build()
        .expect("builder should reset to no-op reporter");
    let tasks = vec![TestTask::succeed()];

    let outcome = executor
        .execute(tasks, 1)
        .expect("batch should execute with no-op reporter");

    assert_eq!(outcome.completed_count(), 1);
    assert!(reporter.events().is_empty());
}
