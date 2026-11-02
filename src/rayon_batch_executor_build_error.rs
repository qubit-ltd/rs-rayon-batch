// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use rayon::ThreadPoolBuildError as RayonThreadPoolBuildError;
use thiserror::Error;

/// Error returned when [`crate::RayonBatchExecutorBuilder`] cannot build an
/// executor.
///
/// # Examples
///
/// ```
/// use qubit_rayon_batch::RayonBatchExecutorBuildError;
///
/// let error = RayonBatchExecutorBuildError::ZeroThreadCount;
/// assert!(matches!(error, RayonBatchExecutorBuildError::ZeroThreadCount));
/// ```
#[must_use]
#[derive(Debug, Error)]
pub enum RayonBatchExecutorBuildError {
    /// The configured Rayon thread count is zero.
    #[error("rayon batch executor thread count must be greater than zero")]
    ZeroThreadCount,

    /// The configured worker stack size is zero.
    #[error("rayon batch executor worker stack size must be greater than zero")]
    ZeroStackSize,

    /// The configured worker-thread name prefix contains a NUL byte.
    #[error("rayon batch executor thread name prefix must not contain NUL bytes")]
    InvalidThreadNamePrefix,

    /// Rayon rejected the underlying thread-pool configuration.
    #[error("failed to build rayon batch executor: {source}")]
    BuildFailed {
        /// Underlying Rayon thread-pool build error.
        #[from]
        source: RayonThreadPoolBuildError,
    },
}
