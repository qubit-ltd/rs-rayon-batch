// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Configurable callable tasks for executor tests.

use std::panic::panic_any;

use qubit_function::Callable;

/// Test callable behavior used by executor call tests.
#[derive(Debug, Clone)]
enum TestCallableAction {
    /// Return the supplied value.
    Return {
        /// Success value returned by this callable.
        value: i32,
    },
    /// Return a callable error.
    Fail {
        /// Error returned by this callable.
        error: &'static str,
    },
    /// Panic while calling.
    Panic {
        /// Panic message.
        message: &'static str,
    },
}

/// Configurable callable task for executor tests.
#[derive(Debug, Clone)]
pub struct TestCallable {
    /// Behavior executed by this callable.
    action: TestCallableAction,
}

impl TestCallable {
    /// Creates a callable that returns `value`.
    ///
    /// # Parameters
    ///
    /// * `value` - Success value returned by this callable.
    ///
    /// # Returns
    ///
    /// A successful callable.
    pub const fn returning(value: i32) -> Self {
        Self {
            action: TestCallableAction::Return { value },
        }
    }

    /// Creates a callable that returns `error`.
    ///
    /// # Parameters
    ///
    /// * `error` - Error returned by this callable.
    ///
    /// # Returns
    ///
    /// A failing callable.
    pub const fn fail(error: &'static str) -> Self {
        Self {
            action: TestCallableAction::Fail { error },
        }
    }

    /// Creates a callable that panics with `message`.
    ///
    /// # Parameters
    ///
    /// * `message` - Panic message.
    ///
    /// # Returns
    ///
    /// A panicking callable.
    pub const fn panic(message: &'static str) -> Self {
        Self {
            action: TestCallableAction::Panic { message },
        }
    }
}

impl Callable<i32, &'static str> for TestCallable {
    /// Runs this configured test callable.
    ///
    /// # Returns
    ///
    /// `Ok(i32)` for successful actions, or `Err(&'static str)` for
    /// [`TestCallableAction::Fail`].
    ///
    /// # Panics
    ///
    /// Panics when configured with [`TestCallableAction::Panic`].
    fn call(&mut self) -> Result<i32, &'static str> {
        match &self.action {
            TestCallableAction::Return { value } => Ok(*value),
            TestCallableAction::Fail { error } => Err(*error),
            TestCallableAction::Panic { message } => panic_any(*message),
        }
    }
}
