/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use thiserror::Error;

/// Error returned when [`crate::RayonBatchExecutorBuilder`] cannot build an
/// executor.
///
#[derive(Debug, Error)]
pub enum RayonBatchExecutorBuildError {
    /// The configured Rayon thread count is zero.
    #[error("rayon batch executor thread count must be greater than zero")]
    ZeroThreadCount,

    /// The configured worker stack size is zero.
    #[error("rayon batch executor worker stack size must be greater than zero")]
    ZeroStackSize,

    /// The configured progress-report interval is zero.
    #[error("rayon batch executor report interval must be greater than zero")]
    ZeroReportInterval,

    /// Rayon rejected the underlying thread-pool configuration.
    #[error("failed to build rayon batch executor: {source}")]
    BuildFailed {
        /// Underlying Rayon thread-pool build error.
        #[from]
        source: rayon::ThreadPoolBuildError,
    },
}
