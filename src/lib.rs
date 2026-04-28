/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026.
 *    Haixing Hu, Qubit Co. Ltd.
 *
 *    All rights reserved.
 *
 ******************************************************************************/
//! # Qubit Rayon Batch
//!
//! Rayon-backed batch executor implementation.
//!
//! # Author
//!
//! Haixing Hu

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]

mod rayon_batch_executor;
mod rayon_batch_executor_build_error;
mod rayon_batch_executor_builder;

pub use qubit_batch::{
    BatchExecutionError,
    BatchExecutionResult,
    BatchExecutionResultBuildError,
    BatchExecutor,
    BatchTaskError,
    BatchTaskFailure,
    NoOpProgressReporter,
    ProgressReporter,
    SequentialBatchExecutor,
};
pub use rayon_batch_executor::RayonBatchExecutor;
pub use rayon_batch_executor_build_error::RayonBatchExecutorBuildError;
pub use rayon_batch_executor_builder::RayonBatchExecutorBuilder;
