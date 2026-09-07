// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Shared contract assertions for every supported execution backend.

mod backend_contract_tests;

pub use backend_contract_tests::assert_backend_contract;
pub use backend_contract_tests::assert_failure_policy_contract;
