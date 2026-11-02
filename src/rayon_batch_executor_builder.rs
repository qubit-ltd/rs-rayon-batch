// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::time::Duration;

use qubit_batch::TaskFailurePolicy;
use qubit_progress::NoopReporter;
use qubit_progress::Reporter;

use crate::RayonBatchExecutor;
use crate::RayonBatchExecutorBuildError;

/// Builder for configuring a [`RayonBatchExecutor`].
///
/// The builder controls worker-pool size, sequential fallback, progress
/// reporting, task-failure policy, worker names, and worker stack size.
///
/// # Examples
///
/// ```
/// use qubit_rayon_batch::RayonBatchExecutor;
///
/// let executor = RayonBatchExecutor::builder()
///     .thread_count(2)
///     .build()
///     .expect("rayon batch executor should build");
///
/// assert_eq!(executor.thread_count(), 2);
/// ```
#[must_use = "build or retain the configured executor builder"]
pub struct RayonBatchExecutorBuilder {
    /// Number of Rayon worker threads to create.
    pub(crate) thread_count: usize,
    /// Maximum batch size that still uses sequential execution.
    pub(crate) sequential_threshold: usize,
    /// Minimum interval between due-based running progress callbacks.
    pub(crate) report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    pub(crate) reporter: Arc<dyn Reporter>,
    /// Policy applied after task failures in Rayon workers.
    pub(crate) task_failure_policy: TaskFailurePolicy,
    /// Prefix used when naming Rayon worker threads.
    pub(crate) thread_name_prefix: String,
    /// Optional worker stack size in bytes.
    pub(crate) stack_size: Option<usize>,
}

impl RayonBatchExecutorBuilder {
    /// Sets the Rayon worker-thread count. The default is the available
    /// parallelism, or 1 if it cannot be detected.
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

    /// Sets the sequential fallback threshold. The default is 100.
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

    /// Sets the progress-report interval. The default is 5 seconds.
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

    /// Sets the progress reporter used by the executor. The default is
    /// [`NoopReporter`].
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
        R: Reporter + 'static,
    {
        self.reporter = Arc::new(reporter);
        self
    }

    /// Sets the progress reporter used by the executor. The default is
    /// [`NoopReporter`].
    ///
    /// # Parameters
    ///
    /// * `reporter` - Shared reporter receiving batch lifecycle callbacks.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn reporter_arc(mut self, reporter: Arc<dyn Reporter>) -> Self {
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
        self.reporter = Arc::new(NoopReporter);
        self
    }

    /// Sets the policy that controls Rayon source acceptance after task
    /// errors or captured panics.
    ///
    /// The default is [`TaskFailurePolicy::Continue`].
    ///
    /// # Parameters
    ///
    /// * `task_failure_policy` - Policy applied after a worker task fails.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub const fn task_failure_policy(mut self, task_failure_policy: TaskFailurePolicy) -> Self {
        self.task_failure_policy = task_failure_policy;
        self
    }

    /// Sets the Rayon worker-thread name prefix. The default is
    /// `qubit-rayon-batch`; worker names append `-<index>`. Empty and Unicode
    /// prefixes are allowed. A prefix containing NUL is rejected by
    /// [`build`](Self::build) with
    /// [`RayonBatchExecutorBuildError::InvalidThreadNamePrefix`].
    ///
    /// # Parameters
    ///
    /// * `thread_name_prefix` - Prefix appended with the worker index.
    ///
    /// # Returns
    ///
    /// This builder for fluent configuration.
    #[inline]
    pub fn thread_name_prefix(mut self, thread_name_prefix: impl Into<String>) -> Self {
        self.thread_name_prefix = thread_name_prefix.into();
        self
    }

    /// Sets the Rayon worker-thread stack size. By default, the stack size is
    /// unspecified and Rayon/platform defaults apply. The actual size is
    /// platform-dependent; zero is rejected by [`build`](Self::build).
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
    /// Returns [`RayonBatchExecutorBuildError`] when the thread count or stack
    /// size is zero, the thread-name prefix contains NUL, or Rayon rejects the
    /// configuration. These validations occur in that order.
    #[inline]
    pub fn build(self) -> Result<RayonBatchExecutor, RayonBatchExecutorBuildError> {
        if self.thread_count == 0 {
            return Err(RayonBatchExecutorBuildError::ZeroThreadCount);
        }
        if self.stack_size == Some(0) {
            return Err(RayonBatchExecutorBuildError::ZeroStackSize);
        }
        if self.thread_name_prefix.contains('\0') {
            return Err(RayonBatchExecutorBuildError::InvalidThreadNamePrefix);
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
            sequential_threshold: RayonBatchExecutor::DEFAULT_SEQUENTIAL_THRESHOLD,
            report_interval: RayonBatchExecutor::DEFAULT_REPORT_INTERVAL,
            reporter: Arc::new(NoopReporter),
            task_failure_policy: TaskFailurePolicy::Continue,
            thread_name_prefix: crate::constants::DEFAULT_THREAD_NAME_PREFIX.to_owned(),
            stack_size: None,
        }
    }
}
