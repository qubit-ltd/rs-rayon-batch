// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
//! Internal implementation details for Rayon batch execution.

mod active_batch_guard;

pub(crate) use active_batch_guard::ActiveBatchGuard;
