// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Contract tests for the shared subprocess watchdog.

use std::env::var;
use std::env::var_os;
use std::panic::catch_unwind;

mod watchdog;
use self::watchdog::run_with_watchdog;

#[test]
fn test_missing_target_is_rejected() {
    let error = catch_unwind(|| {
        run_with_watchdog(
            "watchdog_target_that_does_not_exist",
            "QUBIT_WATCHDOG_MISSING_CHILD",
            "missing target regression",
        );
    })
    .expect_err("a missing target must fail");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .expect("watchdog diagnostic must be text");
    assert!(
        message.contains("missing target regression"),
        "watchdog diagnostic must contain {:?}: {message}",
        "missing target regression"
    );
    assert!(
        message.contains("watchdog_target_that_does_not_exist"),
        "watchdog diagnostic must contain {:?}: {message}",
        "watchdog_target_that_does_not_exist"
    );
}

#[test]
fn test_valid_target_runs() {
    const MARKER: &str = "QUBIT_WATCHDOG_VALID_CHILD";
    if var_os(MARKER).is_some() {
        assert_eq!(
            var(MARKER).expect("watchdog child marker must be valid Unicode"),
            "1",
            "watchdog must set the child marker to 1"
        );
        return;
    }
    run_with_watchdog("test_valid_target_runs", MARKER, "valid target regression");
}

#[test]
fn test_ignored_target_is_executed() {
    let result = catch_unwind(|| {
        run_with_watchdog(
            "test_ignored_failure_fixture",
            "QUBIT_WATCHDOG_IGNORED_CHILD",
            "ignored target regression",
        );
    });
    let error = result.expect_err("the selected fixture must run and fail");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .expect("watchdog diagnostic must be text");
    assert!(
        message.contains("ignored target regression"),
        "watchdog diagnostic must contain {:?}: {message}",
        "ignored target regression"
    );
    assert!(
        message.contains("test_ignored_failure_fixture"),
        "watchdog diagnostic must contain {:?}: {message}",
        "test_ignored_failure_fixture"
    );
    assert!(
        message.contains("test subprocess exited unsuccessfully"),
        "watchdog diagnostic must contain {:?}: {message}",
        "test subprocess exited unsuccessfully"
    );
}

#[test]
#[ignore = "failure fixture selected only by watchdog contract test"]
fn test_ignored_failure_fixture() {
    assert!(
        var_os("QUBIT_WATCHDOG_IGNORED_CHILD").is_some(),
        "the ignored failure fixture must be launched by the watchdog"
    );
    panic!("intentional child failure");
}
