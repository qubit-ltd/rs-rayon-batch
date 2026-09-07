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
- `RayonBatchExecutorBuilder`：配置工作线程数量、顺序执行阈值、进度回调、
  线程名和栈大小。
- `RayonBatchExecutorBuildError`：构建期配置校验和 Rayon 线程池错误。

执行器拥有一个专用 Rayon 线程池，并在多次调用之间复用它。当批次大小不超过
`sequential_threshold`、执行器只配置了一个 worker，或调用发生在该线程池自己的 worker 中
时，使用 `qubit-batch` 提供的顺序执行器。更大的批次通过 Rayon 的 `in_place_scope_fifo` 提交。
同池分支用于保护重入：嵌套工作会在当前 worker 上执行，不会等待被父任务占用的线程池 worker。

核心批处理和进度类型请直接从 `qubit-batch` 与 `qubit-progress` 导入；本 crate
只导出 Rayon 专属的执行器 API。

## 特性

- 使用专用 Rayon 线程池执行面向 CPU 的批量任务。
- 将小批量任务和同池嵌套批次交给 `qubit-batch` 的顺序执行器。
- 即使任务乱序完成，也能用稳定任务索引记录失败项。
- 将任务 panic 捕获为批量失败，同时继续传播进度回调中的 panic。
- 复用 `qubit-batch` 核心 API，不让顺序执行用户被迫引入 Rayon。

`BatchExecutor::call` 通过 `Callable` 的 `&mut self` 调用操作执行每个任务，其语义等同于
`FnMut`。Rayon 路径仍要求 callable、返回值和错误类型实现 `Send`；`for_each` trait 方法
保留 `Fn + Send + Sync` 的 action 约束。成功输入数表示批处理 API 成功处理了多少输入项；
数据库影响行数等业务指标应由应用结果单独记录，不能混入这个输入计数。

Callable 结果会保存成功值和失败详情，因此当成功数为 S、失败数为 F 时，结果存储为
O(S + F)，还要加上值和错误自身的堆内存开销。该 API 不是流式结果通道。将一个大的逻辑
输入拆成多个 chunk 时，每个 chunk 都有独立结果和重试边界；失败不会自动触发跨 chunk 重试，
也不会自动提供全局 failure policy。详见[用户手册](doc/user_guide.zh_CN.md)中的有界分块
示例（同时提供英文版本）。

## 安装

```toml
[dependencies]
qubit-batch = "0.11"
qubit-rayon-batch = "0.9"
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

有关执行契约、同池重入、结果内存和 chunk 重试边界，请阅读[中文用户手册](doc/user_guide.zh_CN.md)。
英文读者可查看[English user guide](doc/user_guide.md)。

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

## 来源重入与终止

当调度器正在生产惰性来源时，会检测生产线程上的同池重入并回退到顺序执行；Rayon
worker 上的重入也采用相同回退。独立线程池和克隆的执行器仍保持并行。请使用
`ParallelBatchExecutionContext::next_task` 区分来源耗尽与失败策略停止；返回前会排空
已经接受的 token。
