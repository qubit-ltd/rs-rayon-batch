// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Rayon Batch
//!
//! Rayon-backed batch executor implementation for Qubit.
//!
//! [`RayonBatchExecutor`] provides the Rayon execution backend. The
//! [`BatchExecutor`](qubit_batch::BatchExecutor) trait and batch result types
//! belong to `qubit-batch`; progress reporting through
//! [`Reporter`](qubit_progress::Reporter) belongs to
//! `qubit-progress`. Cloning an executor shares its Rayon pool and configuration.
//!
//! Small batches, executors configured with one worker, calls made from a
//! worker in the same pool, and lazy-source producers that reenter that pool
//! use sequential execution. A source should produce promptly: a paused source
//! can temporarily delay progress for other batches using the same cloned
//! pool, and timely progress across those batches is not guaranteed.
//!
//! See the [English user guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.md)
//! and [简体中文使用指南](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.zh_CN.md)
//! for setup, progress reporting, and failure-policy examples.
//!
//! # Example
//!
//! ```rust
//! use qubit_batch::BatchExecutor;
//! use qubit_rayon_batch::RayonBatchExecutor;
//!
//! let executor = RayonBatchExecutor::builder()
//!     .thread_count(2)
//!     .sequential_threshold(0)
//!     .build()
//!     .expect("rayon batch executor should build");
//!
//! let tasks = (0..4).map(|index| move || Ok::<_, &'static str>(index * 2));
//! let outcome = executor
//!     .call(tasks)
//!     .expect("task source should be valid");
//!
//! assert!(outcome.outcome().is_success());
//! assert_eq!(outcome.outputs().len(), 4);
//! assert_eq!(outcome.outputs()[3].index(), 3);
//! assert_eq!(*outcome.outputs()[3].value(), 6);
//! ```

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

mod active_batch_guard;
mod constants;
mod rayon_batch_executor;
mod rayon_batch_executor_build_error;
mod rayon_batch_executor_builder;
mod rayon_batch_schedule_error;

pub use rayon_batch_executor::RayonBatchExecutor;
pub use rayon_batch_executor_build_error::RayonBatchExecutorBuildError;
pub use rayon_batch_executor_builder::RayonBatchExecutorBuilder;
pub use rayon_batch_schedule_error::RayonBatchScheduleError;
