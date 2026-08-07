# Qubit Rayon Batch

[![Rust CI](https://github.com/qubit-ltd/rs-rayon-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-rayon-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-rayon-batch/coverage-badge.json)](https://qubit-ltd.github.io/rs-rayon-batch/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-rayon-batch.svg?color=blue)](https://crates.io/crates/qubit-rayon-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Documentation](https://img.shields.io/badge/docs-English-blue.svg)](README.md)

面向 Qubit Rust 库的 Rayon 批量执行器实现。

## 概述

Qubit Rayon Batch 为 `qubit-batch` API 提供面向 CPU 密集型任务的 Rayon
实现。它把运行时相关依赖从核心 batch crate 中拆出来，同时继续复用相同的结果、
错误和进度回调类型。

它提供：

- `RayonBatchExecutor`：基于专用 Rayon 线程池的批量执行器。
- `RayonBatchExecutorBuilder`：配置工作线程数量、顺序退化阈值、进度回调、
  线程名和栈大小。
- `RayonBatchExecutorBuildError`：构建期配置校验和 Rayon 线程池错误。

核心批处理和进度类型请直接从 `qubit-batch` 与 `qubit-progress` 导入；本 crate
只导出 Rayon 专属的执行器 API。

## 特性

- 使用专用 Rayon 线程池执行面向 CPU 的批量任务。
- 对小批量任务自动退化为顺序执行。
- 即使任务乱序完成，也能用稳定任务索引记录失败项。
- 将任务 panic 捕获为批量失败，同时继续传播进度回调中的 panic。
- 复用 `qubit-batch` 核心 API，不让顺序执行用户被迫引入 Rayon。

## 安装

```toml
[dependencies]
qubit-batch = "0.10"
qubit-rayon-batch = "0.8"
```

## 快速开始

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

## 测试

在 crate 根目录执行本地检查：

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

若要与仓库 CI 环境保持一致，请运行：

```bash
./align-ci.sh
./ci-check.sh
./coverage.sh json
```

## 贡献

欢迎通过 Issue 与 Pull Request 参与本仓库。请保持变更聚焦；修改行为时补充回归
测试；公开 API 或用户可见行为变化时同步更新中英文 README。

向本仓库贡献内容即表示您同意以与本项目相同的
[Apache License, Version 2.0](LICENSE) 授权您的贡献。

## 许可证

Copyright (c) 2026. Haixing Hu.

本软件依据 [Apache License, Version 2.0](LICENSE) 授权。

## 作者

**Haixing Hu** — Qubit Co. Ltd.

| | |
| --- | --- |
| **源码仓库** | [github.com/qubit-ltd/rs-rayon-batch](https://github.com/qubit-ltd/rs-rayon-batch) |
| **API 文档** | [docs.rs/qubit-rayon-batch](https://docs.rs/qubit-rayon-batch) |
| **Crate 发布** | [crates.io/crates/qubit-rayon-batch](https://crates.io/crates/qubit-rayon-batch) |
