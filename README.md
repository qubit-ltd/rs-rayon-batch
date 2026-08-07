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
  fallback threshold, progress reporting, thread names, and stack size.
- `RayonBatchExecutorBuildError`: build-time validation and Rayon pool errors.

Import core batch and progress types directly from `qubit-batch` and
`qubit-progress`; this crate exports only its Rayon-specific executor API.

## Features

- Run CPU-oriented batch work on a dedicated Rayon pool.
- Fall back to sequential execution for small batches while preserving the
  parallel executor's collect-all task-failure behavior.
- Keep stable task indexes for failures even when work finishes out of order.
- Capture task panics as batch failures while propagating progress-reporter
  panics.
- Reuse the `qubit-batch` core API without forcing Rayon on sequential users.

## Installation

```toml
[dependencies]
qubit-batch = "0.10"
qubit-rayon-batch = "0.8"
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

## Testing

Run the local checks from the crate root:

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

To match the repository CI environment, run:

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## Contributing

Issues and pull requests are welcome. Keep changes focused, add regression
tests for behavior changes, and update both README files when public APIs or
user-visible behavior changes.

By contributing, you agree that your contribution is licensed under the same
[Apache License, Version 2.0](LICENSE) as this project.

## License

Copyright (c) 2026. Haixing Hu.

This software is licensed under the [Apache License, Version 2.0](LICENSE).

## Author

**Haixing Hu** — Qubit Co. Ltd.

| | |
| --- | --- |
| **Repository** | [github.com/qubit-ltd/rs-rayon-batch](https://github.com/qubit-ltd/rs-rayon-batch) |
| **API documentation** | [docs.rs/qubit-rayon-batch](https://docs.rs/qubit-rayon-batch) |
| **Crate** | [crates.io/crates/qubit-rayon-batch](https://crates.io/crates/qubit-rayon-batch) |
