// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crossbeam_channel::Receiver;
use crossbeam_channel::bounded;
use qubit_batch::BatchCallError;
use qubit_batch::BatchCallResult;
use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchOutcome;
use qubit_batch::SequentialBatchExecutor;
use qubit_batch::TaskFailurePolicy;
use qubit_batch::execute::spi::ParallelBatchExecutionContext;
use qubit_batch::execute::spi::ParallelBatchExecutionCoordinator;
use qubit_batch::execute::spi::ParallelBatchTask;
use qubit_batch::execute::spi::call_with_executor;
use qubit_function::Callable;
use qubit_function::Runnable;
use qubit_progress::Reporter;
use rayon::ThreadPool as RayonThreadPool;

use crate::RayonBatchExecutorBuildError;
use crate::RayonBatchExecutorBuilder;
use crate::RayonBatchScheduleError;
use crate::internal::ActiveBatchGuard;

/// Parallel batch executor backed by a dedicated Rayon thread pool.
///
/// The executor runs small batches sequentially when the declared batch size is
/// at or below the configured sequential threshold. It also runs calls
/// sequentially on the calling thread when configured with one worker, when a
/// call is made from one of its own Rayon workers, or when a lazy-source
/// producer reenters the same pool. A threshold of zero disables only the
/// batch-size fallback for non-empty batches. Clones share the same Rayon pool
/// and executor configuration.
///
/// # Execution limitations
///
/// Direct same-pool reentry runs sequentially. This protection does not
/// guarantee progress for synchronous cycles across distinct pools, such as
/// A -> B -> A. Move orchestration outside worker pools or remove the cyclic
/// dependency. See the
/// [English user guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.md)
/// and [Chinese user guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.zh_CN.md).
///
/// # Examples
///
/// ```rust
/// use qubit_batch::BatchExecutor;
/// use qubit_rayon_batch::RayonBatchExecutor;
///
/// let executor = RayonBatchExecutor::builder()
///     .thread_count(2)
///     .sequential_threshold(0)
///     .build()
///     .expect("rayon batch executor should build");
///
/// let tasks = (0..4).map(|value| move || {
///     assert!(value < 4);
///     Ok::<(), &'static str>(())
/// });
/// let outcome = executor
///     .execute_with_count(tasks, 4)
///     .expect("range should match the declared count");
///
/// assert!(outcome.is_success());
/// ```
#[derive(Clone)]
pub struct RayonBatchExecutor {
    /// Dedicated Rayon pool used for parallel batch execution.
    pool: Arc<RayonThreadPool>,
    /// Number of Rayon worker threads configured for this executor.
    thread_count: usize,
    /// Maximum batch size that still uses sequential execution.
    sequential_threshold: usize,
    /// Shared coordinator used for parallel execution flow.
    coordinator: ParallelBatchExecutionCoordinator,
    /// Policy applied after task failures in Rayon workers.
    task_failure_policy: TaskFailurePolicy,
}

impl RayonBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = crate::constants::DEFAULT_REPORT_INTERVAL;

    /// Default sequential fallback threshold.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = crate::constants::DEFAULT_SEQUENTIAL_THRESHOLD;

    /// Returns the default Rayon worker-thread count used by the builder.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[must_use]
    #[inline]
    pub fn default_thread_count() -> usize {
        thread::available_parallelism().map(usize::from).unwrap_or(1)
    }

    /// Creates a builder for configuring a Rayon batch executor.
    ///
    /// # Returns
    ///
    /// A builder initialized with default Rayon settings.
    #[inline]
    pub fn builder() -> RayonBatchExecutorBuilder {
        RayonBatchExecutorBuilder::default()
    }

    /// Creates a Rayon batch executor with the supplied worker-thread count.
    ///
    /// # Parameters
    ///
    /// * `thread_count` - Number of Rayon worker threads to create.
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
    pub fn new(thread_count: usize) -> Result<Self, RayonBatchExecutorBuildError> {
        Self::builder().thread_count(thread_count).build()
    }

    /// Crate-private executor built from an existing Rayon pool and a consumed
    /// [`RayonBatchExecutorBuilder`].
    ///
    /// `thread_name_prefix` and `stack_size` on `builder` are ignored here;
    /// those apply only while constructing the Rayon pool upstream.
    ///
    /// # Parameters
    ///
    /// * `pool` - Pre-built Rayon thread pool consumed and wrapped by this
    ///   executor.
    /// * `builder` - Consumed builder carrying the validated executor
    ///   configuration (`thread_count`, `sequential_threshold`,
    ///   `report_interval`, and `reporter`).
    ///
    /// # Returns
    ///
    /// A new [`RayonBatchExecutor`] using the supplied pool and configuration.
    #[inline]
    pub(crate) fn new_with_rayon(pool: RayonThreadPool, builder: RayonBatchExecutorBuilder) -> Self {
        Self {
            pool: Arc::new(pool),
            thread_count: builder.thread_count,
            sequential_threshold: builder.sequential_threshold,
            coordinator: ParallelBatchExecutionCoordinator::new(builder.reporter, builder.report_interval),
            task_failure_policy: builder.task_failure_policy,
        }
    }

    /// Returns the configured Rayon worker-thread count.
    ///
    /// Rayon may cap this value when building the pool. Parallel scheduling
    /// uses the actual pool capacity, while this getter continues to return
    /// the configured value.
    ///
    /// # Returns
    ///
    /// The configured worker-thread count.
    #[must_use = "inspect the configured worker count"]
    #[inline]
    pub const fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Bounds scheduler receivers and queue capacity by the actual Rayon pool.
    ///
    /// # Parameters
    /// * `count` - Declared number of tasks in this batch.
    ///
    /// # Returns
    /// The smaller of the actual pool capacity and the declared task count.
    fn worker_count(&self, count: usize) -> usize {
        self.pool.current_num_threads().min(count)
    }

    /// Returns the configured sequential fallback threshold.
    ///
    /// # Returns
    ///
    /// The maximum task count that still uses sequential execution.
    #[must_use = "inspect the configured sequential threshold"]
    #[inline]
    pub const fn sequential_threshold(&self) -> usize {
        self.sequential_threshold
    }

    /// Returns the configured task-failure policy.
    #[must_use = "inspect the configured task-failure policy"]
    #[inline]
    pub const fn task_failure_policy(&self) -> TaskFailurePolicy {
        self.task_failure_policy
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between progress callbacks.
    #[must_use = "inspect the configured report interval"]
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.coordinator.report_interval()
    }

    /// Returns the progress reporter used by this executor.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[must_use = "inspect the configured progress reporter"]
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
        self.coordinator.reporter()
    }

    /// Creates the sequential fallback with this executor's batch settings.
    ///
    /// # Returns
    ///
    /// A sequential executor sharing the reporter, interval, and failure
    /// policy.
    #[inline]
    fn sequential_executor(&self) -> SequentialBatchExecutor {
        SequentialBatchExecutor::builder()
            .report_interval(self.coordinator.report_interval())
            .reporter_arc(Arc::clone(self.coordinator.reporter()))
            .task_failure_policy(self.task_failure_policy)
            .build()
    }
}

impl Default for RayonBatchExecutor {
    /// Creates a default Rayon batch executor.
    ///
    /// # Returns
    ///
    /// A default-configured Rayon batch executor.
    ///
    /// # Panics
    ///
    /// Panics if Rayon rejects the default thread-pool configuration.
    #[inline]
    fn default() -> Self {
        Self::builder()
            .build()
            .expect("default rayon batch executor should build")
    }
}

impl BatchExecutor for RayonBatchExecutor {
    type SchedulerError = RayonBatchScheduleError;

    /// Executes counted callables and retains indexed successful outputs.
    ///
    /// Ordinary calls use Rayon workers. Small batches, single-worker
    /// executors, and same-pool worker or source-producer reentry collect
    /// outputs sequentially on the calling thread.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Callable source, consumed once during execution.
    /// * `count` - Exact declared count used for fallback and validation.
    ///
    /// # Returns
    ///
    /// Indexed successful values with the final or policy-stopped outcome.
    /// Failed tasks have no output at their index; successful outputs retain
    /// their input indices.
    ///
    /// # Errors
    ///
    /// Returns count-validation, progress-reporting, or scheduling failures
    /// with partial values. A count shortfall is detected when real source
    /// exhaustion is observed before `count` tasks; excess input is detected
    /// when an additional item beyond `count` is observed.
    ///
    /// A task-failure policy may stop admission before the source count is
    /// fully validated. An `Ok` result, even with all declared tasks completed,
    /// does not by itself prove complete source-count validation. Inspect the
    /// enclosing error, termination, completion counts, and indexed failures
    /// together. See the
    /// [termination guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.md#completion-counts-and-termination)
    /// and [Chinese user guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.zh_CN.md).
    ///
    /// # Panics
    ///
    /// Task panics are captured in the outcome. Source conversion and
    /// iteration panics, and synchronous reporter panics, propagate to the
    /// caller. On the parallel path, panics from the scoped automatic running
    /// reporter are returned as structured progress errors. When progress
    /// reporting is enabled on the parallel path, creating the scoped
    /// automatic reporter thread may also panic if the operating system cannot
    /// create it. This creation failure is not converted into a batch error
    /// or a structured partial result.
    fn call_with_count<C, R, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchCallResult<R, E>, BatchCallError<R, E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = C>,
        C: Callable<R, E> + Send,
        R: Send,
        E: Send,
    {
        let active_guard = ActiveBatchGuard::enter(&self.pool);
        let reentrant = active_guard.is_none() || self.pool.current_thread_index().is_some();
        if reentrant || count <= self.sequential_threshold || self.thread_count <= 1 {
            let sequential = self.sequential_executor();
            return sequential
                .call_with_count(tasks, count)
                .map_err(|error| error.map_scheduler_error(|never| match never {}));
        }
        // The helper defers user IntoIterator conversion until execute enters
        // the guard again. Keeping this guard would force a false reentry.
        drop(active_guard);
        call_with_executor(self, tasks, count)
    }

    /// Executes counted runnable tasks and returns their batch outcome.
    ///
    /// Ordinary calls use Rayon workers. Small batches, single-worker
    /// executors, and same-pool worker or source-producer reentry run
    /// tasks sequentially on the calling thread.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Runnable task source, consumed once during execution.
    /// * `count` - Declared task count expected from `tasks`.
    ///
    /// # Returns
    ///
    /// The batch outcome with completion counts and task failures.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when reporting fails,
    /// a count-shortfall error when real source exhaustion is observed before
    /// `count` tasks, or a count-exceeded error when an additional item beyond
    /// `count` is observed. Returns [`BatchExecutionError::IncompleteSchedule`]
    /// when an accepted task is not completed by the Rayon scheduler, or
    /// [`BatchExecutionError::ScheduleFailed`] if the internal work channel
    /// disconnects, with [`RayonBatchScheduleError`] as its source.
    ///
    /// A task-failure policy may stop admission before the source count is
    /// fully validated. An `Ok` result, even with all declared tasks completed,
    /// does not by itself prove complete source-count validation. Inspect the
    /// enclosing error, termination, completion counts, and indexed failures
    /// together. See the
    /// [termination guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.md#completion-counts-and-termination)
    /// and [Chinese user guide](https://github.com/qubit-ltd/rs-rayon-batch/blob/main/doc/user_guide.zh_CN.md).
    ///
    /// # Panics
    ///
    /// Unwinding panics from `Runnable::run` are captured in the outcome.
    /// Panics from destroying the runnable task may propagate to the caller
    /// without a structured partial outcome. Process-aborting panics cannot
    /// be converted into task failures. Source conversion and iteration
    /// panics, and synchronous reporter panics, propagate to the caller. On
    /// the parallel path, panics from the scoped automatic running reporter
    /// are returned as structured progress errors. When progress reporting is
    /// enabled on the parallel path, creating the scoped automatic reporter
    /// thread may also panic if the operating system cannot create it. This
    /// creation failure is not converted into a batch error or a structured
    /// partial result.
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E, Self::SchedulerError>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        let active_guard = ActiveBatchGuard::enter(&self.pool);
        let reentrant = active_guard.is_none() || self.pool.current_thread_index().is_some();
        if reentrant || count <= self.sequential_threshold || self.thread_count <= 1 {
            let sequential = self.sequential_executor();
            return sequential
                .execute_with_count(tasks, count)
                .map_err(|error| error.map_scheduler_error(|never| match never {}));
        }

        let worker_count = self.worker_count(count);
        self.coordinator
            .execute_with_source(tasks, count, self.task_failure_policy, move |source, context| {
                self.pool.in_place_scope_fifo(|scope| {
                    let (work_sender, work_receiver) = bounded(worker_count);
                    for _ in 0..worker_count {
                        let worker_receiver = work_receiver.clone();
                        let worker_context = &context;
                        scope.spawn_fifo(move |_| {
                            run_rayon_worker(worker_receiver, worker_context);
                        });
                    }
                    drop(work_receiver);

                    for task in source.by_ref() {
                        if work_sender.send(task).is_err() {
                            return Err(RayonBatchScheduleError::WorkChannelDisconnected);
                        }
                    }
                    drop(work_sender);
                    Ok(())
                })
            })
    }
}

/// Runs Rayon work items until the producer closes the work channel.
///
/// # Parameters
///
/// * `work_receiver` - Clone of the crossbeam work receiver. Crossbeam clones
///   share the queue safely across Rayon workers.
fn run_rayon_worker<T, E>(work_receiver: Receiver<ParallelBatchTask<T>>, context: &ParallelBatchExecutionContext<E>)
where
    T: Runnable<E> + Send,
    E: Send,
{
    loop {
        let Ok(task) = work_receiver.recv() else {
            break;
        };
        context.execute_task(task);
    }
}

#[cfg(test)]
mod tests {
    use rayon::ThreadPoolBuilder;

    use crate::RayonBatchExecutor;

    #[test]
    fn test_worker_count_uses_actual_pool_capacity() {
        let pool = ThreadPoolBuilder::new().num_threads(2).build().expect("small pool");
        let builder = RayonBatchExecutor::builder().thread_count(usize::MAX);
        let executor = RayonBatchExecutor::new_with_rayon(pool, builder);
        assert_eq!(executor.thread_count(), usize::MAX);
        for (count, expected) in [(0, 0), (1, 1), (2, 2), (3, 2), (usize::MAX, 2)] {
            assert_eq!(executor.worker_count(count), expected);
        }
    }
}
