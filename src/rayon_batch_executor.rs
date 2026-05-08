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
    panic::{
        AssertUnwindSafe,
        catch_unwind,
    },
    sync::{
        Arc,
        Mutex,
        PoisonError,
        mpsc::{
            self,
            Receiver,
        },
    },
    thread,
    time::Duration,
};

use qubit_batch::{
    BatchExecutionError,
    BatchExecutionState,
    BatchExecutor,
    BatchOutcome,
    BatchTaskError,
    SequentialBatchExecutor,
};
use qubit_function::Runnable;
use qubit_progress::{
    Progress,
    ProgressReporter,
    RunningProgressPointHandle,
};
use rayon::ThreadPool as RayonThreadPool;

use crate::{
    RayonBatchExecutorBuildError,
    RayonBatchExecutorBuilder,
};

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
/// use qubit_rayon_batch::{
///     BatchExecutor,
///     RayonBatchExecutor,
/// };
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
///     .execute(tasks, 4)
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
    /// Interval between progress callbacks while a batch is running.
    report_interval: Duration,
    /// Reporter receiving batch lifecycle callbacks.
    reporter: Arc<dyn ProgressReporter>,
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
            report_interval: builder.report_interval,
            reporter: builder.reporter,
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
        self.report_interval
    }

    /// Returns the progress reporter used by this executor.
    ///
    /// # Returns
    ///
    /// A shared reference to the configured progress reporter.
    #[inline]
    pub fn reporter(&self) -> &Arc<dyn ProgressReporter> {
        &self.reporter
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
    /// A structured batch result when the declared task count matches, or a
    /// batch-count mismatch error with the attached partial result.
    ///
    /// # Errors
    ///
    /// Returns [`BatchExecutionError`] when `tasks` yields fewer or more tasks
    /// than `count`.
    ///
    /// # Panics
    ///
    /// Panics from tasks are captured in the result. Panics from the configured
    /// progress reporter are propagated to the caller.
    fn execute<T, E, I>(
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
            let sequential = SequentialBatchExecutor::new()
                .with_report_interval(self.report_interval)
                .with_reporter_arc(Arc::clone(&self.reporter));
            return sequential.execute(tasks, count);
        }

        let state = Arc::new(BatchExecutionState::new(count));
        let progress = Progress::new(self.reporter.as_ref(), self.report_interval);
        progress.report_started(state.progress_counters());
        let mut observed_count = 0usize;
        let worker_count = self.thread_count.min(count);

        thread::scope(|thread_scope| {
            let reporter_state = Arc::clone(&state);
            let running_progress = progress
                .spawn_running_reporter(thread_scope, move || reporter_state.progress_counters());
            let running_point_handle = running_progress.point_handle();

            self.pool.in_place_scope_fifo(|scope| {
                let (work_sender, work_receiver) = mpsc::sync_channel(worker_count);
                let work_receiver = Arc::new(Mutex::new(work_receiver));
                for _ in 0..worker_count {
                    let worker_receiver = Arc::clone(&work_receiver);
                    let worker_state = Arc::clone(&state);
                    let running_point_handle = running_point_handle.clone();
                    scope.spawn_fifo(move |_| {
                        run_rayon_worker(worker_receiver, worker_state, running_point_handle);
                    });
                }
                drop(work_receiver);

                for task in tasks {
                    observed_count = state.record_task_observed();
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
            });

            running_progress.stop_and_join();
        });

        let state =
            Arc::into_inner(state).expect("rayon batch execution state should have a single owner");
        if observed_count < count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.into_outcome(failed.elapsed());
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: observed_count,
                outcome: result,
            })
        } else if observed_count > count {
            let failed = progress.report_failed(state.progress_counters());
            let result = state.into_outcome(failed.elapsed());
            Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: observed_count,
                outcome: result,
            })
        } else {
            let finished = progress.report_finished(state.progress_counters());
            let result = state.into_outcome(finished.elapsed());
            Ok(result)
        }
    }
}

/// Runs Rayon work items until the producer closes the work channel.
///
/// # Parameters
///
/// * `work_receiver` - Shared task receiver protected because standard receivers
///   are not `Sync`.
/// * `state` - Shared execution state updated by each task.
/// * `progress_point_handle` - Worker-side progress-point handle.
fn run_rayon_worker<T, E>(
    work_receiver: Arc<Mutex<Receiver<RayonWorkItem<T>>>>,
    state: Arc<BatchExecutionState<E>>,
    progress_point_handle: RunningProgressPointHandle,
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
        run_rayon_task(&state, index, task);
        progress_point_handle.report();
    }
}

/// Executes one task on a Rayon worker and updates shared statistics.
///
/// # Parameters
///
/// * `state` - Shared execution state updated by the task.
/// * `index` - Zero-based task index within the batch.
/// * `task` - Runnable task executed on the current Rayon worker.
fn run_rayon_task<T, E>(state: &BatchExecutionState<E>, index: usize, mut task: T)
where
    T: Runnable<E>,
    E: Send,
{
    state.record_task_started();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    match outcome {
        Ok(Ok(())) => state.record_task_succeeded(),
        Ok(Err(error)) => state.record_task_failed(index, error),
        Err(payload) => {
            state.record_task_panicked(index, BatchTaskError::from_panic_payload(payload.as_ref()));
        }
    }
}
