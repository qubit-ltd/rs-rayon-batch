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
    any::Any,
    fmt,
    panic::{
        AssertUnwindSafe,
        catch_unwind,
        resume_unwind,
    },
    sync::{
        Arc,
        Mutex,
        mpsc::{
            self,
            Receiver,
            RecvTimeoutError,
        },
    },
    thread,
    time::{
        Duration,
        Instant,
    },
};

use qubit_atomic::AtomicCount;
use qubit_batch::{
    BatchExecutionError,
    BatchExecutor,
    BatchOutcome,
    BatchOutcomeBuilder,
    BatchTaskError,
    BatchTaskFailure,
    ProgressCounters,
    ProgressPhase,
    ProgressReporter,
    SequentialBatchExecutor,
};
use qubit_function::Runnable;
use qubit_progress::Progress;
use rayon::ThreadPool as RayonThreadPool;

use crate::{
    RayonBatchExecutorBuildError,
    RayonBatchExecutorBuilder,
};

/// Parallel batch executor backed by a dedicated Rayon thread pool.
///
/// The executor runs small batches sequentially when the declared batch size is
/// at or below the configured sequential threshold.
///
#[derive(Clone)]
pub struct RayonBatchExecutor {
    /// Dedicated Rayon pool used for parallel batch execution.
    pool: Arc<RayonThreadPool>,
    /// Number of Rayon worker threads configured for this executor.
    num_threads: usize,
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
    pub fn default_num_threads() -> usize {
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
    /// * `num_threads` - Number of Rayon worker threads to create.
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
    pub fn new(num_threads: usize) -> Result<Self, RayonBatchExecutorBuildError> {
        Self::builder().num_threads(num_threads).build()
    }

    /// Returns the configured Rayon worker-thread count.
    ///
    /// # Returns
    ///
    /// The configured worker-thread count.
    #[inline]
    pub const fn num_threads(&self) -> usize {
        self.num_threads
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

    /// Builds a Rayon batch executor from validated configuration.
    ///
    /// # Parameters
    ///
    /// * `num_threads` - Number of Rayon worker threads to create.
    /// * `sequential_threshold` - Sequential fallback threshold.
    /// * `report_interval` - Minimum interval between progress callbacks.
    /// * `reporter` - Reporter receiving batch lifecycle callbacks.
    /// * `thread_name_prefix` - Prefix used when naming Rayon workers.
    /// * `stack_size` - Optional worker stack size in bytes.
    ///
    /// # Returns
    ///
    /// A configured Rayon batch executor.
    ///
    /// # Errors
    ///
    /// Returns [`RayonBatchExecutorBuildError`] when the supplied
    /// configuration is invalid or Rayon rejects it.
    pub(crate) fn from_parts(
        num_threads: usize,
        sequential_threshold: usize,
        report_interval: Duration,
        reporter: Arc<dyn ProgressReporter>,
        thread_name_prefix: String,
        stack_size: Option<usize>,
    ) -> Result<Self, RayonBatchExecutorBuildError> {
        if num_threads == 0 {
            return Err(RayonBatchExecutorBuildError::ZeroThreadCount);
        }
        if report_interval.is_zero() {
            return Err(RayonBatchExecutorBuildError::ZeroReportInterval);
        }
        if stack_size == Some(0) {
            return Err(RayonBatchExecutorBuildError::ZeroStackSize);
        }
        let prefix = thread_name_prefix;
        let mut builder = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .thread_name(move |index| format!("{prefix}-{index}"));
        if let Some(stack_size) = stack_size {
            builder = builder.stack_size(stack_size);
        }
        let pool = Arc::new(builder.build()?);
        Ok(Self {
            pool,
            num_threads,
            sequential_threshold,
            report_interval,
            reporter,
        })
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
        E: Send + fmt::Debug,
    {
        if count <= self.sequential_threshold || self.num_threads <= 1 {
            let sequential = SequentialBatchExecutor::new()
                .with_report_interval(self.report_interval)
                .with_reporter_arc(Arc::clone(&self.reporter));
            return sequential.execute(tasks, count);
        }

        let progress_state = Arc::new(RayonBatchProgressState::new());
        let result_state = Arc::new(RayonBatchResultState::new());
        let reporter = Arc::clone(&self.reporter);
        let progress = Progress::new(reporter.as_ref(), self.report_interval);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            progress_state.progress_counters(count),
            Duration::ZERO,
        );
        let start = progress.started_at();
        let progress_reporter = Arc::clone(&reporter);
        let reporter_state = Arc::clone(&progress_state);
        let report_interval = self.report_interval;
        let (stop_sender, stop_receiver) = mpsc::channel();
        let progress_thread = thread::spawn(move || {
            run_progress_loop(
                progress_reporter,
                reporter_state,
                count,
                start,
                report_interval,
                stop_receiver,
            );
        });

        let mut actual_count = 0usize;
        self.pool.in_place_scope_fifo(|scope| {
            for task in tasks {
                if actual_count == count {
                    actual_count += 1;
                    break;
                }
                let index = actual_count;
                actual_count += 1;
                let task_progress_state = Arc::clone(&progress_state);
                let task_result_state = Arc::clone(&result_state);
                scope.spawn_fifo(move |_| {
                    run_rayon_task(task_progress_state, task_result_state, index, task);
                });
            }
        });

        let _ = stop_sender.send(());
        if let Err(payload) = progress_thread.join() {
            resume_unwind(payload);
        }

        let completed_count = progress_state.completed_count.get();
        let elapsed = progress.elapsed();
        let result = Arc::into_inner(result_state)
            .expect("rayon batch result state should have a single owner")
            .into_outcome(count, completed_count, elapsed);
        if actual_count < count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: actual_count,
                outcome: result,
            })
        } else if actual_count > count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: actual_count,
                outcome: result,
            })
        } else {
            progress.report_with_elapsed(
                ProgressPhase::Finished,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Ok(result)
        }
    }
}

/// Shared progress counters for a running Rayon batch.
struct RayonBatchProgressState {
    /// Number of tasks currently running on worker threads.
    active_count: AtomicCount,
    /// Number of completed tasks.
    completed_count: AtomicCount,
    /// Number of successful tasks.
    succeeded_count: AtomicCount,
    /// Number of failed tasks.
    failed_count: AtomicCount,
    /// Number of panicked tasks.
    panicked_count: AtomicCount,
}

impl RayonBatchProgressState {
    /// Creates fresh progress state for one Rayon batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters.
    fn new() -> Self {
        Self {
            active_count: AtomicCount::zero(),
            completed_count: AtomicCount::zero(),
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
        }
    }

    /// Builds generic progress counters from active progress state.
    ///
    /// # Parameters
    ///
    /// * `total_count` - Declared total task count.
    /// # Returns
    ///
    /// Progress counters suitable for reporter events.
    fn progress_counters(&self, total_count: usize) -> ProgressCounters {
        ProgressCounters::new(Some(total_count))
            .with_active_count(self.active_count.get())
            .with_completed_count(self.completed_count.get())
            .with_succeeded_count(self.succeeded_count.get())
            .with_failed_count(self.failed_count.get() + self.panicked_count.get())
    }
}

/// Shared result counters and failure storage for a running Rayon batch.
struct RayonBatchResultState<E> {
    /// Number of successful tasks.
    succeeded_count: AtomicCount,
    /// Number of failed tasks.
    failed_count: AtomicCount,
    /// Number of panicked tasks.
    panicked_count: AtomicCount,
    /// Detailed task failure list.
    failures: Mutex<Vec<BatchTaskFailure<E>>>,
}

impl<E> RayonBatchResultState<E> {
    /// Creates fresh result state for one Rayon batch execution.
    ///
    /// # Returns
    ///
    /// Shared state with zeroed counters and no recorded failures.
    fn new() -> Self {
        Self {
            succeeded_count: AtomicCount::zero(),
            failed_count: AtomicCount::zero(),
            panicked_count: AtomicCount::zero(),
            failures: Mutex::new(Vec::new()),
        }
    }

    /// Builds a structured batch result from the collected counters.
    ///
    /// # Parameters
    ///
    /// * `task_count` - Declared batch task count.
    /// * `completed_count` - Number of tasks completed by workers.
    /// * `elapsed` - Total elapsed wall-clock time.
    ///
    /// # Returns
    ///
    /// A structured batch execution result.
    fn into_outcome(
        self,
        task_count: usize,
        completed_count: usize,
        elapsed: Duration,
    ) -> BatchOutcome<E> {
        let failures = self
            .failures
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        BatchOutcomeBuilder::builder(task_count)
            .completed_count(completed_count)
            .succeeded_count(self.succeeded_count.get())
            .failed_count(self.failed_count.get())
            .panicked_count(self.panicked_count.get())
            .elapsed(elapsed)
            .failures(failures)
            .build()
            .expect("rayon batch executor should collect consistent counters")
    }
}

/// Executes one task on a Rayon worker and updates shared statistics.
///
/// # Parameters
///
/// * `progress_state` - Shared progress counters updated by the task.
/// * `result_state` - Shared result state updated for the task outcome.
/// * `index` - Zero-based task index within the batch.
/// * `task` - Runnable task executed on the current Rayon worker.
fn run_rayon_task<T, E>(
    progress_state: Arc<RayonBatchProgressState>,
    result_state: Arc<RayonBatchResultState<E>>,
    index: usize,
    mut task: T,
) where
    T: Runnable<E>,
    E: Send + fmt::Debug,
{
    progress_state.active_count.inc();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    progress_state.active_count.dec();
    match outcome {
        Ok(Ok(())) => {
            progress_state.completed_count.inc();
            progress_state.succeeded_count.inc();
            result_state.succeeded_count.inc();
        }
        Ok(Err(error)) => {
            progress_state.completed_count.inc();
            progress_state.failed_count.inc();
            result_state.failed_count.inc();
            lock_failures(&result_state.failures)
                .push(BatchTaskFailure::new(index, BatchTaskError::Failed(error)));
        }
        Err(payload) => {
            progress_state.completed_count.inc();
            progress_state.panicked_count.inc();
            result_state.panicked_count.inc();
            lock_failures(&result_state.failures).push(BatchTaskFailure::new(
                index,
                panic_payload_to_error(payload.as_ref()),
            ));
        }
    }
}

/// Runs the periodic progress loop for one Rayon batch execution.
///
/// # Parameters
///
/// * `reporter` - Reporter receiving progress callbacks.
/// * `state` - Shared batch state read by the reporting loop.
/// * `total_count` - Declared task count for the batch.
/// * `start` - Batch start time.
/// * `report_interval` - Delay between progress callbacks.
/// * `stop_receiver` - Stop signal receiver used by the caller thread.
fn run_progress_loop(
    reporter: Arc<dyn ProgressReporter>,
    state: Arc<RayonBatchProgressState>,
    total_count: usize,
    start: Instant,
    report_interval: Duration,
    stop_receiver: Receiver<()>,
) {
    let progress = Progress::from_start(reporter.as_ref(), report_interval, start);
    loop {
        match stop_receiver.recv_timeout(progress.report_interval()) {
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
            Err(RecvTimeoutError::Timeout) => {
                progress.report_running(state.progress_counters(total_count));
            }
        }
    }
}

/// Builds generic progress counters from a completed batch outcome.
///
/// # Parameters
///
/// * `outcome` - Batch outcome containing final task counters.
///
/// # Returns
///
/// Progress counters suitable for a terminal progress event.
fn outcome_progress_counters<E>(outcome: &BatchOutcome<E>) -> ProgressCounters {
    ProgressCounters::new(Some(outcome.task_count()))
        .with_completed_count(outcome.completed_count())
        .with_succeeded_count(outcome.succeeded_count())
        .with_failed_count(outcome.failed_count() + outcome.panicked_count())
}

/// Acquires the failure list while tolerating poisoned mutexes.
///
/// # Parameters
///
/// * `failures` - Mutex protecting the detailed failure list.
///
/// # Returns
///
/// A mutex guard for the failure list.
fn lock_failures<E>(
    failures: &Mutex<Vec<BatchTaskFailure<E>>>,
) -> std::sync::MutexGuard<'_, Vec<BatchTaskFailure<E>>> {
    failures
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Converts a panic payload into a batch task panic error.
///
/// # Parameters
///
/// * `payload` - Panic payload captured by `catch_unwind`.
///
/// # Returns
///
/// A panicked task error containing a string message when the payload carries
/// one.
fn panic_payload_to_error<E>(payload: &(dyn Any + Send)) -> BatchTaskError<E> {
    match panic_payload_message(payload) {
        Some(message) => BatchTaskError::panicked(message),
        None => BatchTaskError::panicked_without_message(),
    }
}

/// Extracts a readable panic message from a panic payload.
///
/// # Parameters
///
/// * `payload` - Panic payload captured by `catch_unwind`.
///
/// # Returns
///
/// A cloned panic message when `payload` is `&'static str` or `String`.
fn panic_payload_message(payload: &(dyn Any + Send)) -> Option<String> {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        Some((*message).to_owned())
    } else {
        payload.downcast_ref::<String>().cloned()
    }
}
