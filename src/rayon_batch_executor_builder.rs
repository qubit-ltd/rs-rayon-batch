/*******************************************************************************
 *
 *    Copyright (c) 2025 - 2026 Haixing Hu.
 *
 *    SPDX-License-Identifier: Apache-2.0
 *
 *    Licensed under the Apache License, Version 2.0.
 *
 ******************************************************************************/
use std::{
    sync::Arc,
    time::Duration,
};

use qubit_batch::{
    NoOpProgressReporter,
    ProgressReporter,
};

use crate::{
    RayonBatchExecutor,
    RayonBatchExecutorBuildError,
};

/// Default worker-thread name prefix for [`RayonBatchExecutor`].
const DEFAULT_THREAD_NAME_PREFIX: &str = "qubit-rayon-batch";

/// Builder for [`RayonBatchExecutor`].
///
pub struct RayonBatchExecutorBuilder {
    /// Number of Rayon worker threads to create.
    num_threads: usize,
    /// Maximum batch size that still uses sequential execution.
    sequential_threshold: usize,
    /// Minimum interval between progress callbacks.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
    /// Prefix used when naming Rayon worker threads.
    thread_name_prefix: String,
    /// Optional worker stack size in bytes.
    stack_size: Option<usize>,
}

impl RayonBatchExecutorBuilder {
    /// Sets the Rayon worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `num_threads` - Number of Rayon worker threads.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn num_threads(mut self, num_threads: usize) -> Self {
        self.num_threads = num_threads;
        self
    }

    /// Sets the sequential fallback threshold.
    ///
    /// # Parameters
    ///
    /// * `sequential_threshold` - Maximum batch size that still runs
    ///   sequentially.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn sequential_threshold(mut self, sequential_threshold: usize) -> Self {
        self.sequential_threshold = sequential_threshold;
        self
    }

    /// Sets the progress-report interval.
    ///
    /// # Parameters
    ///
    /// * `report_interval` - Minimum interval between progress callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn report_interval(mut self, report_interval: Duration) -> Self {
        self.report_interval = report_interval;
        self
    }

    /// Sets the progress reporter used by the executor.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Reporter receiving batch lifecycle callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter<R>(mut self, reporter: R) -> Self
    where
        R: ProgressReporter + 'static,
    {
        self.reporter = Arc::new(reporter);
        self
    }

    /// Sets the progress reporter used by the executor.
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared reporter receiving batch lifecycle callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter_arc(mut self, reporter: Arc<dyn ProgressReporter>) -> Self {
        self.reporter = reporter;
        self
    }

    /// Resets the progress reporter to the no-op implementation.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn no_reporter(mut self) -> Self {
        self.reporter = Arc::new(NoOpProgressReporter);
        self
    }

    /// Sets the Rayon worker-thread name prefix.
    ///
    /// # Parameters
    ///
    /// * `thread_name_prefix` - Prefix appended with the worker index.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn thread_name_prefix(mut self, thread_name_prefix: &str) -> Self {
        self.thread_name_prefix = thread_name_prefix.to_owned();
        self
    }

    /// Sets the Rayon worker-thread stack size.
    ///
    /// # Parameters
    ///
    /// * `stack_size` - Stack size in bytes for each worker thread.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn stack_size(mut self, stack_size: usize) -> Self {
        self.stack_size = Some(stack_size);
        self
    }

    /// Builds the configured Rayon batch executor.
    ///
    /// # Returns
    ///
    /// A configured Rayon batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`RayonBatchExecutorBuildError`] when the supplied
    /// configuration is invalid or Rayon rejects it.
    pub fn build(self) -> Result<RayonBatchExecutor, RayonBatchExecutorBuildError> {
        RayonBatchExecutor::from_parts(
            self.num_threads,
            self.sequential_threshold,
            self.report_interval,
            self.reporter,
            self.thread_name_prefix,
            self.stack_size,
        )
    }
}

impl Default for RayonBatchExecutorBuilder {
    /// Creates a builder with default Rayon batch settings.
    ///
    /// # Returns
    ///
    /// A builder configured with default Rayon and progress-report settings.
    fn default() -> Self {
        Self {
            num_threads: RayonBatchExecutor::default_num_threads(),
            sequential_threshold: RayonBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD,
            report_interval: RayonBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
            thread_name_prefix: DEFAULT_THREAD_NAME_PREFIX.to_owned(),
            stack_size: None,
        }
    }
}
