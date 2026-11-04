// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared subprocess watchdog for tests that must remain independently
//! terminable.

mod subprocess_tests;

pub(crate) use self::subprocess_tests::run_with_watchdog;
