// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use thiserror::Error;

/// Errors returned when Rayon cannot receive a scheduled batch task.
#[derive(Debug, Clone, Copy, Error, Eq, PartialEq)]
pub enum RayonBatchScheduleError {
    /// The internal work channel disconnected before all accepted tasks were
    /// sent.
    #[error("Rayon batch work channel disconnected")]
    WorkChannelDisconnected,
}
