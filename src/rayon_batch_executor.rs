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
    panic::{
        AssertUnwindSafe,
        catch_unwind,
        resume_unwind,
    },
    sync::{
        Arc,
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
    ProgressCounters,
    ProgressPhase,
    ProgressReporter,
};
use rayon::ThreadPool as RayonThreadPool;

use crate::{
    RayonBatchExecutorBuildError,
    RayonBatchExecutorBuilder,
};

/// Signal sent to the progress reporter thread.
enum ProgressLoopSignal {
    /// A worker reached a running progress point.
    RunningPoint,
    /// Execution is complete and the reporter thread should stop.
    Stop,
}

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
        E: Send,
    {
        if count <= self.sequential_threshold || self.num_threads <= 1 {
            let sequential = SequentialBatchExecutor::new()
                .with_report_interval(self.report_interval)
                .with_reporter_arc(Arc::clone(&self.reporter));
            return sequential.execute(tasks, count);
        }

        let state = Arc::new(BatchExecutionState::new(count));
        let reporter = Arc::clone(&self.reporter);
        let progress = Progress::new(reporter.as_ref(), self.report_interval);
        progress.report_with_elapsed(
            ProgressPhase::Started,
            state.progress_counters(),
            Duration::ZERO,
        );
        let start = progress.started_at();
        let mut observed_count = 0usize;

        thread::scope(|thread_scope| {
            let (progress_sender, progress_receiver) = mpsc::channel();
            let progress_thread = {
                let progress_reporter = Arc::clone(&reporter);
                let reporter_state = Arc::clone(&state);
                let report_interval = self.report_interval;
                thread_scope.spawn(move || {
                    run_progress_loop(
                        progress_reporter,
                        reporter_state,
                        start,
                        report_interval,
                        progress_receiver,
                    );
                })
            };

            let report_on_worker_completion = self.report_interval.is_zero();
            self.pool.in_place_scope_fifo(|scope| {
                for task in tasks {
                    observed_count = state.record_task_observed();
                    if observed_count > count {
                        break;
                    }
                    let index = observed_count - 1;
                    let task_state = Arc::clone(&state);
                    let worker_progress_sender = progress_sender.clone();
                    scope.spawn_fifo(move |_| {
                        run_rayon_task(task_state, index, task);
                        if report_on_worker_completion {
                            let _ = worker_progress_sender.send(ProgressLoopSignal::RunningPoint);
                        }
                    });
                }
            });

            let _ = progress_sender.send(ProgressLoopSignal::Stop);
            if let Err(payload) = progress_thread.join() {
                resume_unwind(payload);
            }
        });

        let elapsed = progress.elapsed();
        let result = Arc::into_inner(state)
            .expect("rayon batch execution state should have a single owner")
            .into_outcome(elapsed);
        if observed_count < count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Err(BatchExecutionError::CountShortfall {
                expected: count,
                actual: observed_count,
                outcome: result,
            })
        } else if observed_count > count {
            progress.report_with_elapsed(
                ProgressPhase::Failed,
                outcome_progress_counters(&result),
                result.elapsed(),
            );
            Err(BatchExecutionError::CountExceeded {
                expected: count,
                observed_at_least: observed_count,
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

/// Executes one task on a Rayon worker and updates shared statistics.
///
/// # Parameters
///
/// * `state` - Shared execution state updated by the task.
/// * `index` - Zero-based task index within the batch.
/// * `task` - Runnable task executed on the current Rayon worker.
fn run_rayon_task<T, E>(state: Arc<BatchExecutionState<E>>, index: usize, mut task: T)
where
    T: Runnable<E>,
    E: Send,
{
    state.record_task_started();
    let outcome = catch_unwind(AssertUnwindSafe(|| task.run()));
    match outcome {
        Ok(Ok(())) => state.record_task_succeeded(),
        Ok(Err(error)) => state.record_task_failed(index, error),
        Err(payload) => state.record_task_panicked(index, panic_payload_to_error(payload.as_ref())),
    }
}

/// Runs the periodic progress loop for one Rayon batch execution.
///
/// # Parameters
///
/// * `reporter` - Reporter receiving progress callbacks.
/// * `state` - Shared batch state read by the reporting loop.
/// * `start` - Batch start time.
/// * `report_interval` - Delay between progress callbacks.
/// * `signal_receiver` - Progress-point and stop signal receiver used by the
///   caller and worker threads.
fn run_progress_loop<E>(
    reporter: Arc<dyn ProgressReporter>,
    state: Arc<BatchExecutionState<E>>,
    start: Instant,
    report_interval: Duration,
    signal_receiver: Receiver<ProgressLoopSignal>,
) where
    E: Send,
{
    let mut progress = Progress::from_start(reporter.as_ref(), report_interval, start);
    loop {
        match receive_progress_signal(&signal_receiver, report_interval) {
            ProgressLoopWait::Signal(ProgressLoopSignal::RunningPoint) => {
                progress.report_running_if_due(state.progress_counters());
            }
            ProgressLoopWait::Signal(ProgressLoopSignal::Stop) | ProgressLoopWait::Disconnected => {
                break;
            }
            ProgressLoopWait::Timeout => {
                progress.report_running_if_due(state.progress_counters());
            }
        }
    }
}

/// Receives one progress-loop signal.
///
/// # Parameters
///
/// * `signal_receiver` - Signal receiver shared with workers and the caller.
/// * `report_interval` - Configured progress-report interval.
///
/// # Returns
///
/// A worker or stop signal, a timeout marker for positive intervals, or a
/// disconnected marker when all senders have disconnected.
fn receive_progress_signal(
    signal_receiver: &Receiver<ProgressLoopSignal>,
    report_interval: Duration,
) -> ProgressLoopWait {
    if report_interval.is_zero() {
        return match signal_receiver.recv() {
            Ok(signal) => ProgressLoopWait::Signal(signal),
            Err(_) => ProgressLoopWait::Disconnected,
        };
    }
    match signal_receiver.recv_timeout(report_interval) {
        Ok(signal) => ProgressLoopWait::Signal(signal),
        Err(RecvTimeoutError::Timeout) => ProgressLoopWait::Timeout,
        Err(RecvTimeoutError::Disconnected) => ProgressLoopWait::Disconnected,
    }
}

/// Result of waiting for a progress-loop signal.
enum ProgressLoopWait {
    /// A worker or stop signal was received.
    Signal(ProgressLoopSignal),
    /// No signal arrived before the positive report interval elapsed.
    Timeout,
    /// All senders were dropped.
    Disconnected,
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
        .with_failed_count(outcome.failure_count())
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
