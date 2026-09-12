# Qubit Rayon Batch

[![Rust CI](https://github.com/qubit-ltd/rs-rayon-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-rayon-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-rayon-batch/coverage-badge.json)](https://qubit-ltd.github.io/rs-rayon-batch/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-rayon-batch.svg?color=blue)](https://crates.io/crates/qubit-rayon-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![中文文档](https://img.shields.io/badge/文档-中文版-blue.svg)](README.zh_CN.md)

Rayon-backed batch executor implementation for the Qubit Rust libraries.

## Overview

Qubit Rayon Batch provides the CPU-bound Rayon implementation for the
`qubit-batch` API. It keeps runtime-specific dependencies out of the core batch
crate while still sharing the same result, error, and progress-reporting types.

The crate provides:

- `RayonBatchExecutor`: a Rayon-backed batch executor with a dedicated thread
  pool.
- `RayonBatchExecutorBuilder`: configuration for worker count, sequential
  execution threshold, progress reporting, thread names, and stack size.
- `RayonBatchExecutorBuildError`: build-time validation and Rayon pool errors.

The executor owns one dedicated Rayon pool and reuses it across calls. Batches
at or below `sequential_threshold`, an executor configured with one worker, or a
call made from a worker in that same pool use the sequential executor supplied
by `qubit-batch`. Larger batches are submitted through Rayon’s
`in_place_scope_fifo`. The same-pool branch is a reentrancy safeguard: nested
work runs on the current worker instead of waiting for the pool workers
occupied by its parent task.

Import core batch and progress types directly from `qubit-batch` and
`qubit-progress`; this crate exports only its Rayon-specific executor API.

## Features

- Run CPU-oriented batch work on a dedicated Rayon pool.
- Delegate small batches and same-pool nested batches to `qubit-batch`'s
  sequential executor while preserving the parallel executor's collect-all
  task-failure behavior.
- Keep stable task indexes for failures even when work finishes out of order.
- Capture task panics as batch failures while propagating progress-reporter
  panics.
- Reuse the `qubit-batch` core API without forcing Rayon on sequential users.

`BatchExecutor::call` runs each `Callable` through its `&mut self` call
operation (the callable equivalent of `FnMut`). Rayon calls still require the
callable, result, and error types to be `Send`; the `for_each` trait methods
retain their `Fn + Send + Sync` action bound. A successful input count measures
items accepted by the batch API. Domain measurements such as affected database
rows belong in the application result, not in that input count.

Callable results retain successful values and failure details, so their result
storage is O(S + F) for S successes and F failures, plus the sizes of the
stored values and errors. The API is not a streaming result channel. When a
large logical input is split into chunks, each chunk has its own outcome and
retry boundary; a failed chunk does not provide automatic cross-chunk retry or
global failure policy. The [user guide](doc/user_guide.md) includes a bounded,
bilingual chunking example.

## Installation

```toml
[dependencies]
qubit-batch = "0.12"
qubit-rayon-batch = "0.10"
```

## Quick Start

```rust
use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

let executor = RayonBatchExecutor::builder()
    .thread_count(4)
    .sequential_threshold(1)
    .build()
    .expect("rayon batch executor should be created");

let tasks = (0..8).map(|_| || Ok::<(), &'static str>(()));
let result = executor
    .execute_with_count(tasks, 8)
    .expect("batch should succeed");

assert_eq!(result.completed_count(), 8);
assert_eq!(result.failure_count(), 0);
```

See the [English user guide](doc/user_guide.md) for execution contracts,
same-pool reentrancy, result memory, and chunk retry boundaries. Chinese
readers can use the [中文用户手册](doc/user_guide.zh_CN.md).

## Source reentrancy and termination

When a scheduler produces a lazy source, same-pool reentry from that producer
thread is detected and falls back to sequential execution. Reentry from a
Rayon worker has the same fallback, while independent pools and cloned
executors remain parallel. Use `ParallelBatchExecutionContext::next_task` so
source exhaustion is distinguished from a failure-policy stop; accepted tokens
are drained before return.

Callable small-batch and single-worker fallbacks collect outputs directly on
the caller thread; Rayon same-pool reentry uses that path too. A policy stop can
still have `completed_count == task_count`, while `Finished` can include task
failures. Use counters and failure indexes alongside termination when deciding
what to retry; see the [user guide](doc/user_guide.md).

## Testing

```bash
# Run tests with the default feature set
cargo test

# Run tests with all declared features
cargo test --all-features

# Project CI checks
./ci-check.sh

# Check code coverage
./coverage.sh
```

## License

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the
full license text.

## Contributing

Contributions are welcome. Please follow the Rust API guidelines, keep public
API documentation and tests current, and run `./align-ci.sh` to format code and
`./ci-check.sh` to satisfy CI requirements before submitting a pull request.

## Author

**Haixing Hu** - *Qubit Co. Ltd.*

Repository: [https://github.com/qubit-ltd/rs-rayon-batch](https://github.com/qubit-ltd/rs-rayon-batch)
