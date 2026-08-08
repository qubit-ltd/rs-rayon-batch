// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! # Qubit Rayon Batch
//!
//! Rayon-backed batch executor implementation.

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

mod constants;
mod rayon_batch_executor;
mod rayon_batch_executor_build_error;
mod rayon_batch_executor_builder;
mod rayon_batch_schedule_error;

pub use rayon_batch_executor::RayonBatchExecutor;
pub use rayon_batch_executor_build_error::RayonBatchExecutorBuildError;
pub use rayon_batch_executor_builder::RayonBatchExecutorBuilder;
pub use rayon_batch_schedule_error::RayonBatchScheduleError;
