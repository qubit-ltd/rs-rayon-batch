// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared subprocess watchdog for tests that must remain independently
//! terminable.

use std::env::current_exe;
use std::process::Child;
use std::process::Command;
use std::process::Stdio;
use std::str::from_utf8;
use std::thread;
use std::time::Duration;
use std::time::Instant;

const WATCHDOG_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Runs one exactly named test in an independently terminable subprocess.
///
/// # Parameters
///
/// * `test_name` - Complete test harness name selected with `--exact`.
/// * `marker` - Environment variable that selects the child branch.
/// * `scenario` - Short description included in watchdog diagnostics.
///
/// # Panics
///
/// Panics if the test name is missing or cannot be uniquely listed, if the
/// subprocess cannot be managed or exits unsuccessfully, or if it exceeds the
/// watchdog timeout. A selected ignored test is executed as well.
pub(crate) fn run_with_watchdog(test_name: &str, marker: &str, scenario: &str) {
    let executable = current_exe()
        .unwrap_or_else(|error| panic!("{scenario} ({test_name}): cannot locate current test executable: {error}"));
    let listing = Command::new(&executable)
        .args(["--list", "--format", "terse", "--exact", test_name])
        .stdin(Stdio::null())
        .output()
        .unwrap_or_else(|error| panic!("{scenario} ({test_name}): cannot list test: {error}"));
    assert!(
        listing.status.success(),
        "{scenario} ({test_name}): test listing failed: {}; stderr={}",
        listing.status,
        String::from_utf8_lossy(&listing.stderr)
    );
    let listed = from_utf8(&listing.stdout)
        .unwrap_or_else(|error| panic!("{scenario} ({test_name}): invalid test listing UTF-8: {error}"));
    let expected = format!("{test_name}: test");
    let records: Vec<_> = listed.lines().filter(|line| !line.is_empty()).collect();
    assert_eq!(
        records,
        [expected.as_str()],
        "{scenario} ({test_name}): expected exactly one test; stderr={}",
        String::from_utf8_lossy(&listing.stderr)
    );

    let mut child = Command::new(executable)
        .arg("--exact")
        .arg(test_name)
        .arg("--include-ignored")
        .arg("--nocapture")
        .env(marker, "1")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap_or_else(|error| panic!("{scenario} ({test_name}): cannot spawn test subprocess: {error}"));

    let deadline = Instant::now() + WATCHDOG_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => return,
            Ok(Some(status)) => {
                panic!("{scenario} ({test_name}): test subprocess exited unsuccessfully: {status}")
            }
            Ok(None) => {}
            Err(error) => {
                let cleanup = terminate_and_reap(&mut child);
                panic!("{scenario} ({test_name}): cannot query test subprocess status: {error}; {cleanup}");
            }
        }

        if Instant::now() >= deadline {
            let cleanup = terminate_and_reap(&mut child);
            panic!("{scenario} ({test_name}): timed out after 10 seconds; {cleanup}");
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Kills a child if it is still running and waits for it to be reaped.
fn terminate_and_reap(child: &mut Child) -> String {
    let kill_result = child.kill();
    let wait_result = child.wait();
    match (kill_result, wait_result) {
        (_, Ok(status)) if status.success() => "subprocess reaped successfully".to_owned(),
        (Ok(()), Ok(status)) => format!("subprocess reaped with status {status}"),
        (Err(kill_error), Ok(status)) => {
            format!("kill returned {kill_error} (child may have exited concurrently); reaped with status {status}")
        }
        (kill_result, Err(wait_error)) => match kill_result {
            Ok(()) => format!("subprocess was killed but wait failed: {wait_error}"),
            Err(kill_error) => format!("kill failed: {kill_error}; wait failed: {wait_error}"),
        },
    }
}
