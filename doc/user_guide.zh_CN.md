# Qubit Rayon Batch 用户手册

[English user guide](user_guide.md) · [README](../README.zh_CN.md) ·
[API 文档](https://docs.rs/qubit-rayon-batch)

本文适用于 `qubit-rayon-batch` 0.8 和 Rust 1.94 及以上版本。面向需要处理有限 CPU
批次，并希望在专用 Rayon 线程池上复用 `qubit-batch` 结果与错误模型的应用或库作者。

## 手册目标与读者

当每个输入项都是相互独立的 runnable 或 callable，且调用方需要任务失败、稳定下标、耗时
和进度信息时，使用 `RayonBatchExecutor`。执行器在自身生命周期内持有并复用专用线程池；
它是可重复调用的批量执行器，不是常驻队列、调度器或重试服务。通用的结果、processor
以及错误类型由配套的 `qubit-batch` crate 提供。

## 概念模型

```text
有限输入 ──> RayonBatchExecutor ──> BatchOutcome / BatchCallResult
                   │                  ├─ 输入计数与失败详情
                   │                  ├─ callable 的成功返回值
                   │                  └─ 批次错误附带的部分结果
                   ├─ 多次调用复用专用线程池
                   ├─ 小批次使用 qubit-batch 的顺序执行器
                   ├─ 同池重入使用 qubit-batch 的顺序执行器
                   └─ 大批次使用 Rayon in_place_scope_fifo
```

当声明数量不超过 `sequential_threshold`、worker 数为 1，或者调用者已经位于该执行器的
Rayon 线程池中时，执行器会选择顺序路径。大批次通过 Rayon 的 `in_place_scope_fifo` 提交。
同池分支用于保护重入：任务可以调用同一执行器的 clone，嵌套批次会在当前 worker 上顺序执行，
不会等待被父任务占用的 worker。

## 实战场景：执行 CPU 校验批次

假设服务需要校验八条相互独立的记录，并保留每条失败记录的原始位置。成功标准是七个
callable 成功返回值，以及一个带下标的任务失败。

### 安装 crate

```toml
[dependencies]
qubit-batch = "0.10"
qubit-rayon-batch = "0.8"
```

### 执行 callable 任务

```rust
use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

let executor = RayonBatchExecutor::builder()
    .thread_count(4)
    .sequential_threshold(0)
    .build()
    .expect("rayon batch executor should build");

let calls = (0..8).map(|value| move || {
    if value == 3 {
        Err::<usize, &'static str>("invalid record")
    } else {
        Ok(value * 2)
    }
});
let result = executor.call(calls).expect("the range has an exact length");

assert_eq!(result.outcome().task_count(), 8);
assert_eq!(result.outcome().succeeded_count(), 7);
assert_eq!(result.outcome().failed_count(), 1);
assert_eq!(result.outcome().failures()[0].index(), 3);
assert_eq!(result.outputs().len(), 7);
```

`BatchCallResult` 会按照 callable 的原始下标稀疏保存成功值，`BatchOutcome` 会按下标保存
失败详情。callable 返回的错误属于任务失败；输入数量不匹配或进度上报失败属于批次级
错误，并会附带当时已经统计到的部分结果。

## 核心工作流

针对一类工作负载创建一个执行器，然后复用它处理不同大小的有限批次。应通过实际负载
测量选择 `thread_count` 与 `sequential_threshold`。阈值为 0 表示对每个尚未运行在同一线程池
中的非空批次请求 Rayon worker。

`execute` 与 `execute_with_count` 接收 `Runnable` 任务。`call` 与 `call_with_count` 接收
`Callable`，并收集成功返回值。Exact-size 版本从迭代器推导声明数量；如果数量来自数据库
或其他独立服务边界，应使用 `*_with_count` 并把该数量视为边界契约。输入来源只消费一次，
已接收任务完成后方法才返回。

`BatchExecutor::for_each` 适合把输入项转成任务，但其 trait action 必须满足 `Fn + Send + Sync`，
因为输入项可能并发执行。Callable 的语义不同：`Callable::call` 接收 `&mut self`，因此相当于
`FnMut`。Rayon trait 路径仍要求 callable、成功值和错误实现 `Send`；不能借此使用具体
`SequentialBatchExecutor` 的非 `Send` callable 路径。并行任务应各自拥有状态，不要让多个
worker 共享一个可变 callable。

## 进阶用法

### 同一线程池的重入

`RayonBatchExecutor` 实现了 `Clone`；clone 之间共享线程池和执行器配置。如果某个任务在该
线程池上运行时调用同一执行器的 clone，嵌套调用会在当前 worker 上顺序执行。这样仍保持
同步调用契约，并避免嵌套线程池死锁。嵌套调用的结果不会自动合并到外层结果；任务应按应用
的错误模型检查或转换这个结果。

例如，外层任务可以执行一个小的嵌套批次，并把批次级错误转换成外层任务的错误类型：

```rust
use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

let executor = RayonBatchExecutor::builder()
    .thread_count(2)
    .sequential_threshold(0)
    .build()
    .expect("rayon batch executor should build");
let nested_executor = executor.clone();
let outcome = executor
    .execute_with_count(
        std::iter::once(move || {
            nested_executor
                .execute_with_count([|| Ok::<(), &'static str>(())], 1)
                .map(|_| ())
                .map_err(|_| "nested batch failed")
        }),
        1,
    )
    .expect("outer batch should succeed");

assert!(outcome.is_success());
```

来自普通外部线程的调用可以并发使用执行器 clone。各次调用的输入来源、结果和 callable
值彼此独立。

### 输入数量与业务影响量

执行器的计数描述输入任务。对于 `qubit-batch` 提供的 processor API，`processed_count` 表示
成功处理的输入项，并满足 `processed_count <= completed_count <= item_count`。一次数据库
导入可能成功处理 2 个输入项，却影响 6 行数据；此时应在应用或领域结果中独立记录
`affected_rows = 6`，不要把 6 当成成功输入数。

### 结果内存

一个 callable 批次有 S 个成功值、F 条失败详情时，保留的结果数据量为 O(S + F)，还要加上
每个值和错误自身的堆内存。执行器不是恒定内存的流式结果通道。逻辑输入较大时，应拆成
多个 chunk，并在继续处理前及时消费或丢弃当前 chunk 的输出。

### 有界分块

下面的模式把大范围输入拆成多个批次，使每个 callable 结果保持在有界大小。每次循环都会
创建独立的 `BatchCallResult`；是否继续由外层循环决定。

```rust
use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

let executor = RayonBatchExecutor::builder()
    .thread_count(4)
    .sequential_threshold(0)
    .build()
    .expect("rayon batch executor should build");
let mut source = 0..10_000usize;
let mut sum = 0usize;

loop {
    let chunk: Vec<_> = source.by_ref().take(256).collect();
    if chunk.is_empty() {
        break;
    }
    let result = executor
        .call(chunk.into_iter().map(|item| move || Ok::<_, ()>(item)))
        .expect("each chunk has an exact length");
    assert!(result.outcome().is_success());
    for output in result.into_outputs() {
        sum += *output.value();
    }
}

assert_eq!(sum, (0..10_000usize).sum());
```

这些是多个独立批次。这个模式不提供全局 failure policy、全局稳定下标，也不会自动跨
chunk 重试。如果应用需要全局位置，应把 chunk 起点 offset 加到每个 `output.index()`。
对于 `ChunkedBatchProcessor`，`ChunkFailed` 携带失败 chunk 之前成功 chunk 的聚合结果；这
个前缀不能证明失败 chunk 没有产生副作用，因此只有在 delegate 的事务和幂等规则允许时
才能重试。手动分块调用也遵守同样的边界：检查失败 chunk，由外层应用决定停止、补偿或
重试。

## 错误与诊断

成功返回的 callable 结果应分两部分检查：

1. 检查 `result.outcome().is_success()`、各项计数和 `failures()`。
2. 检查或消费 `result.outputs()`；其下标对应原始 callable 位置，任务失败时可能是稀疏的。

出现 `BatchCallError` 时，应先查看附带的 outcome 和部分输出，再决定是否重试。任务 panic
会被捕获为任务失败；输入迭代器 panic 和同步进度回调 panic 会传播给调用者；自动 running
reporter 失败会作为批次级进度错误返回。调度失败或数量不匹配同样会保留错误发生前已经
统计到的工作。

## 排障

| 现象 | 检查项 | 处理方式 |
| --- | --- | --- |
| 小批次没有使用 Rayon worker | `sequential_threshold()` 与声明数量 | 先基于基准测试，再考虑降低阈值。 |
| 任务调用了同一个执行器 | 任务是否运行在该执行器线程池中 | 嵌套调用会使用顺序回退；在任务内检查它的结果。 |
| `call` 在编译期拒绝类型 | `Callable` 以及 callable/返回值/错误的 `Send` | Rayon 路径使用可发送类型；本地非 `Send` callable 改用 `SequentialBatchExecutor`。 |
| `Ok` 结果仍有失败 | `outcome().is_success()` 与 `failures()` | 逐项处理带下标的任务错误或 panic。 |
| 重试 chunk 造成重复副作用 | delegate 的事务和幂等保证 | 不要只依据成功前缀盲目重试；按领域结果和局部边界处理。 |
| 大批次内存持续增长 | 成功值与失败详情的数量和大小 | 拆成独立 chunk，并及时消费输出。 |

## 限制与最佳实践

- 输入必须有限且只消费一次。本 crate 不提供持久化调度、队列、存储或跨调用重试编排。
- 线程池可以复用，但每个批次的结果仍会保留到调用方消费或释放。
- `for_each` action 必须满足 `Fn + Send + Sync`；callable 通过 `&mut self` 表现为
  `FnMut`，并且 Rayon 路径要求相关类型实现 `Send`。
- 同池重入会使用顺序执行，不会额外制造并行度，也不会自动合并嵌套结果。
- 进度间隔只会在实现定义的进度点做节流，不保证到达墙上时限就立即产生事件。
- 重试行为应依据副作用、事务和幂等规则决定；部分结果表示统计边界，不表示未报告工作
  一定没有发生。

## 延伸阅读

- [README](../README.zh_CN.md)
- [English user guide](user_guide.md)
- [API 文档](https://docs.rs/qubit-rayon-batch)
- [核心 batch crate](https://docs.rs/qubit-batch)
- [Crate 发布页](https://crates.io/crates/qubit-rayon-batch)

## 来源耗尽与重入

运行时相关的调度器应通过 `ParallelBatchExecutionContext::next_task` 拉取惰性来源。
它会把来源返回的 `None` 记录为耗尽；耗尽之前的 `None` 可能表示 reporter 或失败策略
已经停止准入。返回前始终会排空已经接受的 token。已经观察到来源耗尽时，会先返回声明
数量不足；否则提前因策略停止会返回 `StoppedByTaskFailurePolicy` 的部分结果。`Finished`
并不表示结果一定成功。

Rayon worker 上的同池重入和惰性来源生产线程上的同池重入都会回退到顺序执行。独立线程
池和并发克隆仍保持并行。
