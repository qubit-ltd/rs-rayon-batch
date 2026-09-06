// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Runtime exit and admission-contract tests for the Rayon executor.
#![allow(clippy::result_large_err)]

use std::io;
use std::process::Command;
use std::process::Stdio;
use std::sync::Arc;
use std::sync::Barrier;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
use qubit_batch::BatchTaskError;
use qubit_function::Runnable;
use qubit_progress::Event;
use qubit_progress::Phase;
use qubit_progress::Reporter;
use qubit_progress::ReporterError;
use qubit_rayon_batch::RayonBatchExecutor;

const CONCURRENT_CALLS_CHILD: &str = "QUBIT_BATCH_RAYON_CONCURRENT_CALLS_TEST_CHILD";
const ADMISSION_WINDOW_CHILD: &str = "QUBIT_BATCH_RAYON_ADMISSION_WINDOW_TEST_CHILD";
const REPORTER_FAILURE_CHILD: &str = "QUBIT_BATCH_RAYON_REPORTER_FAILURE_TEST_CHILD";
const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(10);
const COORDINATION_TIMEOUT: Duration = Duration::from_secs(2);

/// Runs one test in an independently terminable subprocess.
fn run_with_watchdog(test_name: &str, child_environment: &str) {
    let mut child = Command::new(std::env::current_exe().expect("the current test executable should be available"))
        .arg("--exact")
        .arg(test_name)
        .arg("--nocapture")
        .env(child_environment, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("the runtime-contract test subprocess should start");
    let deadline = Instant::now() + WATCHDOG_TIMEOUT;
    loop {
        if let Some(status) = child
            .try_wait()
            .expect("the runtime-contract subprocess should remain observable")
        {
            assert!(status.success(), "runtime-contract subprocess failed: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child
                .kill()
                .expect("the timed-out runtime-contract subprocess should be killed");
            let _ = child.wait();
            panic!("runtime contract exceeded the 10-second watchdog");
        }
        thread::sleep(Duration::from_millis(10));
    }
}

/// Waits for an atomic counter to reach a lower bound.
fn wait_for_count(counter: &AtomicUsize, expected: usize) {
    let deadline = Instant::now() + COORDINATION_TIMEOUT;
    while counter.load(Ordering::Acquire) < expected {
        assert!(
            Instant::now() < deadline,
            "counter did not reach {expected} before the coordination deadline"
        );
        thread::yield_now();
    }
}

/// Mutable state protected by [`TaskGate`].
#[derive(Default)]
struct TaskGateState {
    permits: usize,
    released: bool,
}

/// Gate that releases worker tasks individually or together.
#[derive(Default)]
struct TaskGate {
    state: Mutex<TaskGateState>,
    changed: Condvar,
}

impl TaskGate {
    /// Blocks until all held tasks are released.
    fn wait(&self) {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = self
            .changed
            .wait_while(state, |state| !state.released && state.permits == 0)
            .unwrap_or_else(PoisonError::into_inner);
        if !state.released {
            state.permits -= 1;
        }
    }

    /// Releases one waiting task.
    fn release_one(&self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.permits += 1;
        self.changed.notify_one();
    }

    /// Releases every current and future waiter.
    fn release_all(&self) {
        self.state.lock().unwrap_or_else(PoisonError::into_inner).released = true;
        self.changed.notify_all();
    }
}

/// Causal events emitted by the reporter and the observed task source.
#[derive(Debug)]
enum RuntimeEvent {
    ReporterWaiting,
    ReporterReturningError,
    TaskObserved(usize),
    SourceStopped(usize),
}

/// Sends an event when the reporter exits through its error return.
struct ReportReturnSignal {
    events: mpsc::Sender<RuntimeEvent>,
}

impl Drop for ReportReturnSignal {
    fn drop(&mut self) {
        self.events
            .send(RuntimeEvent::ReporterReturningError)
            .expect("the test should observe the reporter error return");
    }
}

/// Reporter whose first running event fails after a test-controlled handshake.
struct CoordinatedRunningFailureReporter {
    allow_failure: Mutex<mpsc::Receiver<()>>,
    events: mpsc::Sender<RuntimeEvent>,
}

impl Reporter for CoordinatedRunningFailureReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if event.phase() != Phase::Running {
            return Ok(());
        }
        self.events
            .send(RuntimeEvent::ReporterWaiting)
            .expect("the test should observe the running report");
        self.allow_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recv()
            .expect("the test should release the running reporter");
        let _return_signal = ReportReturnSignal {
            events: self.events.clone(),
        };
        Err(ReporterError::new(io::Error::other(
            "synthetic running progress failure",
        )))
    }
}

/// Runnable task held by a gate until the test advances execution.
struct GatedTask {
    gate: Arc<TaskGate>,
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
}

impl Runnable<&'static str> for GatedTask {
    fn run(&mut self) -> Result<(), &'static str> {
        self.started.fetch_add(1, Ordering::AcqRel);
        self.gate.wait();
        self.completed.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

/// Lazy task source that reports every observation and its early drop.
struct ObservedTaskSource {
    next_index: usize,
    item_count: usize,
    gate: Arc<TaskGate>,
    started: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    events: mpsc::Sender<RuntimeEvent>,
}

impl Iterator for ObservedTaskSource {
    type Item = GatedTask;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index == self.item_count {
            return None;
        }
        self.next_index += 1;
        self.events
            .send(RuntimeEvent::TaskObserved(self.next_index))
            .expect("the test should observe source consumption");
        Some(GatedTask {
            gate: Arc::clone(&self.gate),
            started: Arc::clone(&self.started),
            completed: Arc::clone(&self.completed),
        })
    }
}

impl Drop for ObservedTaskSource {
    fn drop(&mut self) {
        self.events
            .send(RuntimeEvent::SourceStopped(self.next_index))
            .expect("the test should observe the task source stopping");
    }
}

#[test]
fn test_cloned_executor_concurrent_calls_remain_independent() {
    if std::env::var_os(CONCURRENT_CALLS_CHILD).is_none() {
        run_with_watchdog(
            "test_cloned_executor_concurrent_calls_remain_independent",
            CONCURRENT_CALLS_CHILD,
        );
        return;
    }

    const ITEM_COUNT: usize = 64;
    let executor = RayonBatchExecutor::builder()
        .thread_count(4)
        .sequential_threshold(0)
        .build()
        .expect("the shared Rayon executor should build");
    let start = Arc::new(Barrier::new(2));
    let first_count = Arc::new(AtomicUsize::new(0));
    let second_count = Arc::new(AtomicUsize::new(0));
    let worker_gate = Arc::new(TaskGate::default());
    let (worker_ready_sender, worker_ready_receiver) = mpsc::channel();

    let (first_outcome, second_outcome) = thread::scope(|scope| {
        let first_executor = executor.clone();
        let first_start = Arc::clone(&start);
        let first_count_by_task = Arc::clone(&first_count);
        let first_worker_gate = Arc::clone(&worker_gate);
        let first_worker_ready = worker_ready_sender.clone();
        let first = scope.spawn(move || {
            first_start.wait();
            first_executor.for_each(0..ITEM_COUNT, move |index| {
                if index == 0 {
                    first_worker_ready
                        .send(1)
                        .expect("the first batch worker should report readiness");
                    first_worker_gate.wait();
                }
                first_count_by_task.fetch_add(1, Ordering::AcqRel);
                Ok::<(), &'static str>(())
            })
        });

        let second_executor = executor.clone();
        let second_start = Arc::clone(&start);
        let second_count_by_task = Arc::clone(&second_count);
        let second_worker_gate = Arc::clone(&worker_gate);
        let second_worker_ready = worker_ready_sender.clone();
        let second = scope.spawn(move || {
            second_start.wait();
            second_executor.for_each(0..ITEM_COUNT, move |index| {
                if index == 0 {
                    second_worker_ready
                        .send(2)
                        .expect("the second batch worker should report readiness");
                    second_worker_gate.wait();
                }
                second_count_by_task.fetch_add(1, Ordering::AcqRel);
                if index == 7 { Err("second batch marker") } else { Ok(()) }
            })
        });

        let mut ready_batches = [
            worker_ready_receiver
                .recv_timeout(COORDINATION_TIMEOUT)
                .expect("one batch worker should reach the rendezvous"),
            worker_ready_receiver
                .recv_timeout(COORDINATION_TIMEOUT)
                .expect("both batch workers should reach the rendezvous"),
        ];
        ready_batches.sort_unstable();
        assert_eq!(ready_batches, [1, 2]);
        worker_gate.release_all();

        (
            first
                .join()
                .expect("the first producer should not panic")
                .expect("the first batch should complete"),
            second
                .join()
                .expect("the second producer should not panic")
                .expect("the second batch should complete"),
        )
    });

    assert_eq!(first_count.load(Ordering::Acquire), ITEM_COUNT);
    assert_eq!(second_count.load(Ordering::Acquire), ITEM_COUNT);
    assert_eq!(first_outcome.completed_count(), ITEM_COUNT);
    assert_eq!(first_outcome.succeeded_count(), ITEM_COUNT);
    assert_eq!(second_outcome.completed_count(), ITEM_COUNT);
    assert_eq!(second_outcome.succeeded_count(), ITEM_COUNT - 1);
    assert_eq!(second_outcome.failed_count(), 1);
    assert_eq!(second_outcome.failures()[0].index(), 7);
    assert_eq!(
        second_outcome.failures()[0].error(),
        &BatchTaskError::Failed("second batch marker")
    );
}

#[test]
fn test_blocked_workers_bound_unfinished_admission_window() {
    if std::env::var_os(ADMISSION_WINDOW_CHILD).is_none() {
        run_with_watchdog(
            "test_blocked_workers_bound_unfinished_admission_window",
            ADMISSION_WINDOW_CHILD,
        );
        return;
    }

    const WORKER_COUNT: usize = 2;
    const ITEM_COUNT: usize = 64;
    const MAX_UNFINISHED_ACCEPTED: usize = 2 * WORKER_COUNT + 1;
    let executor = RayonBatchExecutor::builder()
        .thread_count(WORKER_COUNT)
        .sequential_threshold(0)
        .build()
        .expect("the admission-window executor should build");
    let observed = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(TaskGate::default());

    let observed_by_iterator = Arc::clone(&observed);
    let started_by_task = Arc::clone(&started);
    let completed_by_task = Arc::clone(&completed);
    let gate_by_task = Arc::clone(&gate);
    let tasks = (0..ITEM_COUNT).map(move |_| {
        observed_by_iterator.fetch_add(1, Ordering::AcqRel);
        let started = Arc::clone(&started_by_task);
        let completed = Arc::clone(&completed_by_task);
        let gate = Arc::clone(&gate_by_task);
        move || {
            started.fetch_add(1, Ordering::AcqRel);
            gate.wait();
            completed.fetch_add(1, Ordering::AcqRel);
            Ok::<(), &'static str>(())
        }
    });
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = result_sender.send(executor.execute_with_count(tasks, ITEM_COUNT));
    });

    wait_for_count(&started, WORKER_COUNT);
    wait_for_count(&observed, MAX_UNFINISHED_ACCEPTED);
    assert_eq!(completed.load(Ordering::Acquire), 0);
    assert!(
        observed.load(Ordering::Acquire) <= MAX_UNFINISHED_ACCEPTED,
        "the producer consumed beyond the bounded unfinished window"
    );

    gate.release_all();
    let outcome = result_receiver
        .recv_timeout(COORDINATION_TIMEOUT)
        .expect("the released batch should return")
        .expect("the released batch should complete successfully");
    assert_eq!(outcome.completed_count(), ITEM_COUNT);
    assert_eq!(completed.load(Ordering::Acquire), ITEM_COUNT);
}

#[test]
fn test_running_reporter_failure_drains_accepted_tasks() {
    if std::env::var_os(REPORTER_FAILURE_CHILD).is_none() {
        run_with_watchdog(
            "test_running_reporter_failure_drains_accepted_tasks",
            REPORTER_FAILURE_CHILD,
        );
        return;
    }

    const WORKER_COUNT: usize = 2;
    const ITEM_COUNT: usize = 64;
    const MAX_UNFINISHED_ACCEPTED: usize = 2 * WORKER_COUNT + 1;
    let (allow_failure_sender, allow_failure_receiver) = mpsc::sync_channel(0);
    let (event_sender, event_receiver) = mpsc::channel();
    let reporter = CoordinatedRunningFailureReporter {
        allow_failure: Mutex::new(allow_failure_receiver),
        events: event_sender.clone(),
    };
    let executor = RayonBatchExecutor::builder()
        .thread_count(WORKER_COUNT)
        .sequential_threshold(0)
        .report_interval(Duration::from_millis(1))
        .reporter(reporter)
        .build()
        .expect("the reporter-failure executor should build");
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(TaskGate::default());
    let tasks = ObservedTaskSource {
        next_index: 0,
        item_count: ITEM_COUNT,
        gate: Arc::clone(&gate),
        started: Arc::clone(&started),
        completed: Arc::clone(&completed),
        events: event_sender,
    };
    let (result_sender, result_receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let _ = result_sender.send(executor.execute_with_count(tasks, ITEM_COUNT));
    });

    let mut observed_count = 0;
    let mut reporter_waiting = false;
    while observed_count < MAX_UNFINISHED_ACCEPTED
        || started.load(Ordering::Acquire) < WORKER_COUNT
        || !reporter_waiting
    {
        match event_receiver
            .recv_timeout(COORDINATION_TIMEOUT)
            .expect("the initial admission and reporter events should arrive")
        {
            RuntimeEvent::ReporterWaiting => reporter_waiting = true,
            RuntimeEvent::TaskObserved(count) => observed_count = count,
            RuntimeEvent::ReporterReturningError => {
                panic!("the reporter must remain blocked before test release")
            }
            RuntimeEvent::SourceStopped(count) => {
                panic!("the source stopped unexpectedly after {count} observations")
            }
        }
    }
    assert_eq!(observed_count, MAX_UNFINISHED_ACCEPTED);
    assert_eq!(completed.load(Ordering::Acquire), 0);
    assert!(matches!(result_receiver.try_recv(), Err(mpsc::TryRecvError::Empty)));
    allow_failure_sender
        .send(())
        .expect("the running reporter should be released to fail");
    match event_receiver
        .recv_timeout(COORDINATION_TIMEOUT)
        .expect("the reporter error-return event should arrive")
    {
        RuntimeEvent::ReporterReturningError => {}
        RuntimeEvent::ReporterWaiting => {
            panic!("the running reporter should only wait once")
        }
        RuntimeEvent::TaskObserved(count) => {
            panic!("workers are blocked, but the source observed task {count}")
        }
        RuntimeEvent::SourceStopped(count) => {
            panic!("the source stopped before reporter error after {count} observations")
        }
    }

    gate.release_one();
    let stopped_after_observations = loop {
        match event_receiver
            .recv_timeout(COORDINATION_TIMEOUT)
            .expect("the source should stop after the reporter error")
        {
            RuntimeEvent::TaskObserved(count) => {
                observed_count = count;
                gate.release_one();
            }
            RuntimeEvent::SourceStopped(count) => break count,
            RuntimeEvent::ReporterWaiting => {
                panic!("the running reporter should only run once")
            }
            RuntimeEvent::ReporterReturningError => {
                panic!("the running reporter should only return one error")
            }
        }
    };
    assert_eq!(observed_count, stopped_after_observations);
    assert!(stopped_after_observations < ITEM_COUNT);
    gate.release_all();

    let error = result_receiver
        .recv_timeout(COORDINATION_TIMEOUT)
        .expect("the reporter-failed batch should return")
        .expect_err("the running reporter failure should fail the batch");
    assert!(matches!(error, BatchExecutionError::ProgressReport { .. }));
    let completed_count = error.outcome().completed_count();
    assert_eq!(started.load(Ordering::Acquire), completed_count);
    assert_eq!(completed.load(Ordering::Acquire), completed_count);
    assert_eq!(stopped_after_observations, completed_count + 1);
}
