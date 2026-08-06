// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::{
    sync::{
        Arc, Mutex, PoisonError,
        mpsc::{self, Receiver},
    },
    thread,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError, BatchExecutor, BatchOutcome, ParallelBatchExecutionCoordinator,
    ParallelBatchExecutionContext, SequentialBatchExecutor, TaskFailurePolicy,
};
use qubit_function::Runnable;
use qubit_progress::Reporter;
use rayon::ThreadPool as RayonThreadPool;

use crate::{RayonBatchExecutorBuildError, RayonBatchExecutorBuilder};

/// Indexed task sent to Rayon worker loops.
struct RayonWorkItem<T> {
    /// Zero-based task index within the declared batch.
    index: usize,
    /// Task payload.
    task: T,
}

/// Parallel batch executor backed by a dedicated Rayon thread pool.
///
/// The executor runs small batches sequentially when the declared batch size is
/// at or below the configured sequential threshold.
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
}

impl RayonBatchExecutor {
    /// Default interval between progress callbacks.
    pub const DEFAULT_REPORT_INTERVAL: Duration = Duration::from_secs(5);

    /// Default sequential fallback threshold.
    pub const DEFAULT_SEQUENTIAL_THRESHOLD: usize = 100;

    /// Returns the default Rayon worker-thread count used by the builder.
    ///
    /// # Returns
    ///
    /// The available CPU parallelism, or `1` if it cannot be detected.
    #[inline]
    pub fn default_thread_count() -> usize {
        thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
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
    pub(crate) fn new_with_rayon(
        pool: RayonThreadPool,
        builder: RayonBatchExecutorBuilder,
    ) -> Self {
        Self {
            pool: Arc::new(pool),
            thread_count: builder.thread_count,
            sequential_threshold: builder.sequential_threshold,
            coordinator: ParallelBatchExecutionCoordinator::new(
                builder.reporter,
                builder.report_interval,
            ),
        }
    }

    /// Returns the configured Rayon worker-thread count.
    ///
    /// # Returns
    ///
    /// The configured worker-thread count.
    #[inline]
    pub const fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// Returns the configured sequential fallback threshold.
    ///
    /// # Returns
    ///
    /// The maximum task count that still uses sequential execution.
    #[inline]
    pub const fn sequential_threshold(&self) -> usize {
        self.sequential_threshold
    }

    /// Returns the configured progress-report interval.
    ///
    /// # Returns
    ///
    /// The minimum interval between progress callbacks.
    #[inline]
    pub const fn report_interval(&self) -> Duration {
        self.coordinator.report_interval()
    }

    /// Returns the progress reporter used by this executor.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn Reporter> {
        &self.coordinator.reporter()
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
    /// Executes the batch on Rayon workers when the batch is large enough.
    ///
    /// # Parameters
    ///
    /// * `tasks` - Task source for the batch.
    /// * `count` - Declared task count expected from `tasks`.
    ///
    /// # Returns
    ///
    /// A structured batch result when reporting and count validation succeed,
    /// or a batch-level error with the attached partial result.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError::ProgressReport`] when reporting fails, or
    /// a count-mismatch variant when `tasks` yields fewer or more tasks than
    /// `count`.
    ///
    /// # Panics
    ///
    /// Panics from tasks are captured in the result. Panics from synchronous
    /// progress callbacks are propagated to the caller; panics from the
    /// scoped running reporter are returned as [`qubit_batch::ProgressFailure`].
    fn execute_with_count<T, E, I>(
        &self,
        tasks: I,
        count: usize,
    ) -> Result<BatchOutcome<E>, BatchExecutionError<E>>
    where
        I: IntoIterator<Item = T>,
        T: Runnable<E> + Send,
        E: Send,
    {
        if count <= self.sequential_threshold || self.thread_count <= 1 {
            let sequential = SequentialBatchExecutor::builder()
                .report_interval(self.coordinator.report_interval())
                .reporter_arc(Arc::clone(self.coordinator.reporter()))
                .task_failure_policy(TaskFailurePolicy::Continue)
                .build();
            return sequential.execute_with_count(tasks, count);
        }

        let worker_count = self.thread_count.min(count);
        self.coordinator.execute(
            tasks,
            count,
            move |tasks, count, context| {
                self.pool.in_place_scope_fifo(|scope| {
                    let (work_sender, work_receiver) = mpsc::sync_channel(worker_count);
                    let work_receiver = Arc::new(Mutex::new(work_receiver));
                    for _ in 0..worker_count {
                        let worker_receiver = Arc::clone(&work_receiver);
                        let worker_context = &context;
                        scope.spawn_fifo(move |_| {
                            run_rayon_worker(worker_receiver, worker_context);
                        });
                    }
                    drop(work_receiver);

                    let mut observed_count = 0usize;
                    for task in tasks {
                        if context.reporting_failed() {
                            break;
                        }
                        observed_count = context.record_task_observed();
                        if observed_count > count {
                            break;
                        }
                        if work_sender
                            .send(RayonWorkItem {
                                index: observed_count - 1,
                                task,
                            })
                            .is_err()
                        {
                            break;
                        }
                    }
                    drop(work_sender);
                    observed_count
                })
            },
        )
    }
}

/// Runs Rayon work items until the producer closes the work channel.
///
/// # Parameters
///
/// * `work_receiver` - Shared task receiver protected because standard
///   receivers are not `Sync`.
/// * `state` - Shared execution state updated by each task.
/// * `progress_point_handle` - Worker-side progress-point handle.
fn run_rayon_worker<T, E>(
    work_receiver: Arc<Mutex<Receiver<RayonWorkItem<T>>>>,
    context: &ParallelBatchExecutionContext<E>,
) where
    T: Runnable<E> + Send,
    E: Send,
{
    loop {
        let received = work_receiver
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recv();
        let Ok(RayonWorkItem { index, task }) = received else {
            break;
        };
        if context.reporting_failed() {
            break;
        }
        context
            .execute_task(index, task)
            .expect("producer must assign an in-range task index");
    }
}
