// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        https://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use qubit_batch::BatchExecutionError;
use qubit_batch::BatchExecutor;
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
    let mut child = Command::new(
        std::env::current_exe().expect("the current test executable should be available"),
    )
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
            assert!(
                status.success(),
                "runtime-contract subprocess failed: {status}"
            );
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

/// Gate that holds worker tasks until the test releases them together.
#[derive(Default)]
struct TaskGate {
    released: Mutex<bool>,
    changed: Condvar,
}

impl TaskGate {
    /// Blocks until all held tasks are released.
    fn wait(&self) {
        let released = self.released.lock().unwrap_or_else(PoisonError::into_inner);
        let _released = self
            .changed
            .wait_while(released, |released| !*released)
            .unwrap_or_else(PoisonError::into_inner);
    }

    /// Releases every current and future waiter.
    fn release_all(&self) {
        *self.released.lock().unwrap_or_else(PoisonError::into_inner) = true;
        self.changed.notify_all();
    }
}

/// Reporter whose first running event fails after a test-controlled handshake.
struct CoordinatedRunningFailureReporter {
    running_started: mpsc::SyncSender<()>,
    allow_failure: Mutex<mpsc::Receiver<()>>,
    returning_error: Arc<AtomicBool>,
}

impl Reporter for CoordinatedRunningFailureReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if event.phase() != Phase::Running {
            return Ok(());
        }
        self.running_started
            .send(())
            .expect("the test should observe the running report");
        self.allow_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recv()
            .expect("the test should release the running reporter");
        self.returning_error.store(true, Ordering::Release);
        Err(ReporterError::new(io::Error::other(
            "synthetic running progress failure",
        )))
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

    let (first_outcome, second_outcome) = thread::scope(|scope| {
        let first_executor = executor.clone();
        let first_start = Arc::clone(&start);
        let first_count_by_task = Arc::clone(&first_count);
        let first = scope.spawn(move || {
            first_start.wait();
            first_executor.for_each(0..ITEM_COUNT, move |_| {
                first_count_by_task.fetch_add(1, Ordering::AcqRel);
                Ok::<(), &'static str>(())
            })
        });

        let second_executor = executor.clone();
        let second_start = Arc::clone(&start);
        let second_count_by_task = Arc::clone(&second_count);
        let second = scope.spawn(move || {
            second_start.wait();
            second_executor.for_each(0..ITEM_COUNT, move |_| {
                second_count_by_task.fetch_add(1, Ordering::AcqRel);
                Ok::<(), &'static str>(())
            })
        });

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
    assert_eq!(second_outcome.succeeded_count(), ITEM_COUNT);
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
    let (running_sender, running_receiver) = mpsc::sync_channel(0);
    let (allow_failure_sender, allow_failure_receiver) = mpsc::sync_channel(0);
    let returning_error = Arc::new(AtomicBool::new(false));
    let reporter = CoordinatedRunningFailureReporter {
        running_started: running_sender,
        allow_failure: Mutex::new(allow_failure_receiver),
        returning_error: Arc::clone(&returning_error),
    };
    let executor = RayonBatchExecutor::builder()
        .thread_count(WORKER_COUNT)
        .sequential_threshold(0)
        .report_interval(Duration::from_millis(1))
        .reporter(reporter)
        .build()
        .expect("the reporter-failure executor should build");
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
    running_receiver
        .recv_timeout(COORDINATION_TIMEOUT)
        .expect("the running reporter should attempt a report");
    assert!(matches!(
        result_receiver.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    allow_failure_sender
        .send(())
        .expect("the running reporter should be released to fail");
    while !returning_error.load(Ordering::Acquire) {
        thread::yield_now();
    }
    thread::sleep(Duration::from_millis(10));
    gate.release_all();

    let error = result_receiver
        .recv_timeout(COORDINATION_TIMEOUT)
        .expect("the reporter-failed batch should return")
        .expect_err("the running reporter failure should fail the batch");
    assert!(matches!(error, BatchExecutionError::ProgressReport { .. }));
    let completed_count = error.outcome().completed_count();
    assert_eq!(completed.load(Ordering::Acquire), completed_count);
    assert_eq!(completed_count, MAX_UNFINISHED_ACCEPTED);
    assert!(observed.load(Ordering::Acquire) <= completed_count + 1);
    assert!(observed.load(Ordering::Acquire) < ITEM_COUNT);
}
