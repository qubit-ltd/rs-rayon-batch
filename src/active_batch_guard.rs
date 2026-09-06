// =============================================================================
//    Copyright (c) 2025 - 2026 Haixing Hu.
//
//    SPDX-License-Identifier: Apache-2.0
//
//    Licensed under the Apache License, Version 2.0.
// =============================================================================
use std::cell::RefCell;
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use rayon::ThreadPool;

thread_local! {
    static ACTIVE_POOLS: RefCell<Vec<usize>> = const { RefCell::new(Vec::new()) };
}

/// Marks a Rayon pool as active on the current thread for one call.
pub(crate) struct ActiveBatchGuard {
    pool_id: usize,
    /// Makes the guard thread-bound so it cannot be moved into a worker.
    thread_bound: PhantomData<Rc<()>>,
}

impl ActiveBatchGuard {
    /// Enters `pool` on the current thread, returning `None` on reentry.
    pub(crate) fn enter(pool: &Arc<ThreadPool>) -> Option<Self> {
        let pool_id = Arc::as_ptr(pool) as usize;
        ACTIVE_POOLS.with(|active| {
            let mut active = active.borrow_mut();
            if active.contains(&pool_id) {
                None
            } else {
                active.push(pool_id);
                Some(Self {
                    pool_id,
                    thread_bound: PhantomData,
                })
            }
        })
    }
}

impl Drop for ActiveBatchGuard {
    /// Removes this pool from the current thread's active set.
    fn drop(&mut self) {
        ACTIVE_POOLS.with(|active| {
            let mut active = active.borrow_mut();
            if let Some(index) = active.iter().rposition(|id| *id == self.pool_id) {
                active.remove(index);
            }
        });
    }
}
