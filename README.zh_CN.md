# Qubit Rayon Batch

[![Rust CI](https://github.com/qubit-ltd/rs-rayon-batch/actions/workflows/ci.yml/badge.svg)](https://github.com/qubit-ltd/rs-rayon-batch/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/endpoint?url=https://qubit-ltd.github.io/rs-rayon-batch/coverage-badge.json)](https://qubit-ltd.github.io/rs-rayon-batch/coverage/)
[![Crates.io](https://img.shields.io/crates/v/qubit-rayon-batch.svg?color=blue)](https://crates.io/crates/qubit-rayon-batch)
[![Rust](https://img.shields.io/badge/rust-1.94+-blue.svg?logo=rust)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![English Document](https://img.shields.io/badge/Document-English-blue.svg)](README.md)

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
qubit-batch = "0.12"
qubit-rayon-batch = "0.10"
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

## 来源重入与终止

当调度器正在生产惰性来源时，会检测生产线程上的同池重入并回退到顺序执行；Rayon
worker 上的重入也采用相同回退。独立线程池和克隆的执行器仍保持并行。请使用
`ParallelBatchExecutionContext::next_task` 区分来源耗尽与失败策略停止；返回前会排空
已经接受的 token。

小批次和单 worker 的 callable 调用直接在调用线程收集输出，Rayon 同池重入也采用该路径。
策略停止时，完成数可能已经等于声明数；`Finished` 也可能包含失败任务。判断重试范围时，
应同时检查完成计数、失败下标和终止原因，详见[用户手册](doc/user_guide.zh_CN.md)。

## 测试

```bash
# 使用默认 feature 集运行测试
cargo test

# 使用项目声明的全部 feature 运行测试
cargo test --all-features

# 运行项目 CI 检查
./ci-check.sh

# 检查代码覆盖率
./coverage.sh
```

## 许可证

Copyright (c) 2025 - 2026. Haixing Hu. All rights reserved.

本项目基于 Apache License 2.0 授权。完整许可证文本请参阅
[LICENSE](LICENSE)。

## 贡献

欢迎贡献。请遵循 Rust API 指南，及时更新公共 API 文档与测试，并在提交
Pull Request 前运行 `./align-ci.sh`格式化代码，运行`./ci-check.sh`对齐CI要求。

## 作者

**Haixing Hu** - *Qubit Co. Ltd.*

仓库地址：[https://github.com/qubit-ltd/rs-rayon-batch](https://github.com/qubit-ltd/rs-rayon-batch)
