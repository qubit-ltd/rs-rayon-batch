// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{
    sync::Arc,
    time::Duration,
};

use qubit_progress::{
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
pub struct RayonBatchExecutorBuilder {
    /// Number of Rayon worker threads to create.
    pub(crate) thread_count: usize,
    /// Maximum batch size that still uses sequential execution.
    pub(crate) sequential_threshold: usize,
    /// Minimum interval between due-based running progress callbacks.
    pub(crate) report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    pub(crate) reporter: Arc<dyn ProgressReporter>,
    /// Prefix used when naming Rayon worker threads.
    pub(crate) thread_name_prefix: String,
    /// Optional worker stack size in bytes.
    pub(crate) stack_size: Option<usize>,
}

impl RayonBatchExecutorBuilder {
    /// Sets the Rayon worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `thread_count` - Number of Rayon worker threads.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn thread_count(mut self, thread_count: usize) -> Self {
        self.thread_count = thread_count;
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
    /// * `report_interval` - Minimum interval between due-based running
    ///   progress callbacks. Use [`Duration::ZERO`] to report at every
    ///   implementation-defined running progress point.
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
    pub fn thread_name_prefix(
        mut self,
        thread_name_prefix: impl Into<String>,
    ) -> Self {
        self.thread_name_prefix = thread_name_prefix.into();
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
    #[inline]
    pub fn build(
        self,
    ) -> Result<RayonBatchExecutor, RayonBatchExecutorBuildError> {
        if self.thread_count == 0 {
            return Err(RayonBatchExecutorBuildError::ZeroThreadCount);
        }
        if self.stack_size == Some(0) {
            return Err(RayonBatchExecutorBuildError::ZeroStackSize);
        }
        let prefix = self.thread_name_prefix.clone();
        let mut builder = rayon::ThreadPoolBuilder::new()
            .num_threads(self.thread_count)
            .thread_name(move |index| format!("{prefix}-{index}"));
        if let Some(stack_size) = self.stack_size {
            builder = builder.stack_size(stack_size);
        }
        let pool = builder.build()?;
        Ok(RayonBatchExecutor::new_with_rayon(pool, self))
    }
}

impl Default for RayonBatchExecutorBuilder {
    /// Creates a builder with default Rayon batch settings.
    ///
    /// # Returns
    ///
    /// A builder configured with default Rayon and progress-report settings.
    #[inline]
    fn default() -> Self {
        Self {
            thread_count: RayonBatchExecutor::default_thread_count(),
            sequential_threshold:
                RayonBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD,
            report_interval: RayonBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoOpProgressReporter),
            thread_name_prefix: DEFAULT_THREAD_NAME_PREFIX.to_owned(),
            stack_size: None,
        }
    }
}
