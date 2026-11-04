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
/// Configuration validation failures identify the rejected setting. If Rayon
/// fails to create the worker pool, [`Self::BuildFailed`] owns the underlying
/// error and exposes it through [`std::error::Error::source`].
///
/// # Examples
///
/// ```
/// use qubit_rayon_batch::RayonBatchExecutor;
/// use qubit_rayon_batch::RayonBatchExecutorBuildError;
///
/// let result = RayonBatchExecutor::builder().thread_count(0).build();
/// assert!(matches!(
///     result,
///     Err(RayonBatchExecutorBuildError::ZeroThreadCount)
/// ));
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
