# Qubit Rayon Batch 用户手册

[English user guide](user_guide.md) · [README](../README.zh_CN.md) ·
[API 文档](https://docs.rs/qubit-rayon-batch)

本文适用于 `qubit-rayon-batch` 0.9 和 Rust 1.94 及以上版本。面向需要处理有限 CPU
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
qubit-batch = "0.14"
qubit-rayon-batch = "0.9"
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
测量选择 `thread_count` 与 `sequential_threshold`。阈值为 0 只关闭非空批次按大小触发的
顺序回退；单 worker、同池 worker 调用和惰性来源生产线程在同池重入时，仍会在当前调用线程
顺序执行。

| 配置 | 默认值 | 约定 |
| --- | --- | --- |
| `thread_count` | 可用并行度；检测失败时为 1 | 必须大于 0。Rayon 可能会截断配置值；实际调度遵循线程池容量，而 `thread_count()` 仍返回配置值。 |
| `sequential_threshold` | 100 | 声明数量小于等于此值时顺序执行；0 仅关闭非空批次按大小触发的回退。 |
| `report_interval` | 5 秒 | 设为 0 时在实现定义的 running 进度点报告；不保证按墙上时间定期心跳。 |
| `reporter` | `NoopReporter` | `reporter_arc` 共享配置的 reporter；`no_reporter` 将其重置。 |
| `task_failure_policy` | `TaskFailurePolicy::Continue` | 停止策略是协作式的；返回前会排空已接纳任务。 |
| `thread_name_prefix` | `qubit-rayon-batch` | worker 名称追加 `-<index>`；允许空串和 Unicode；含 NUL 时 build 返回 `InvalidThreadNamePrefix`。 |
| `stack_size` | 未显式设置 | 使用 Rayon/平台默认值；显式设为 0 会被拒绝；实际大小取决于平台。只配置 Rayon worker；调用线程上的顺序回退使用调用线程已有栈，从 worker 重入时使用当前 worker 已有栈。 |

即使 `sequential_threshold` 为 0，一个 worker 以及同池 worker/来源重入仍会顺序执行。

`execute` 与 `execute_with_count` 接收 `Runnable` 任务。`call` 与 `call_with_count` 接收
`Callable`，并收集成功返回值。Exact-size 版本从迭代器推导声明数量；如果数量来自数据库
或其他独立服务边界，应使用 `*_with_count` 并把该数量视为边界契约。输入来源只消费一次，
已接收任务完成后方法才返回。

`BatchExecutor::for_each` 适合把输入项转成任务，但其 trait action 必须满足 `Fn + Send + Sync`，
因为输入项可能并发执行。Callable 的语义不同：`Callable::call` 接收 `&mut self`，因此相当于
`FnMut`。Rayon trait 路径仍要求 callable、成功值和错误实现 `Send`；不能借此使用具体
`SequentialBatchExecutor` 的非 `Send` callable 路径。并行任务应各自拥有状态，不要让多个
worker 共享一个可变 callable。

### 接入进度上报

默认 reporter 是 `NoopReporter`。如需观察生命周期事件，实现
`qubit_progress::Reporter`，并通过 `reporter_arc` 传入共享 reporter；使用
`report_interval` 限制 running 更新频率。下面的示例会统计任务开始和结束事件。

使用下面的示例前，请在已有的 `[dependencies]` 表中添加以下直接依赖。
示例直接使用进度类型；仅有传递依赖时，应用不能直接导入 `qubit_progress`。

```toml
qubit-progress = { version = "0.9", default-features = false }
```

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use qubit_batch::BatchExecutor;
use qubit_progress::{Event, Phase, Reporter, ReporterError};
use qubit_rayon_batch::RayonBatchExecutor;

#[derive(Default)]
struct CountingReporter(AtomicUsize);

impl Reporter for CountingReporter {
    fn report(&self, event: &Event) -> Result<(), ReporterError> {
        if matches!(event.phase(), Phase::Started | Phase::Succeeded | Phase::Failed) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

fn main() {
    let reporter = Arc::new(CountingReporter::default());
    let executor = RayonBatchExecutor::builder()
        .thread_count(2)
        .sequential_threshold(0)
        .reporter_arc(reporter.clone())
        .report_interval(Duration::from_millis(10))
        .build()
        .expect("executor should build");

    let success = executor.call([|| Ok::<usize, &'static str>(2)])
        .expect("batch should be counted");
    assert!(success.outcome().is_success());
    assert_eq!(*success.outputs()[0].value(), 2);

    let failure = executor.call((0..4).map(|index| move || {
        if index == 3 { Err("invalid record") } else { Ok(index * 2) }
    })).expect("task failure is part of the outcome");
    assert!(!failure.outcome().is_success());
    assert_eq!(failure.outcome().failed_count(), 1);
    assert_eq!(failure.outputs().len(), failure.outcome().succeeded_count());
    assert!(reporter.0.load(Ordering::Relaxed) >= 4);
}
```

`Started` 和终态 `Succeeded` 或 `Failed` 描述批次。`Running` 是可选更新，可能一次也不产生，
尤其是短批次。两个 worker 和阈值 0 会让非空批次走并行路径。短批次仍可能没有 `Running` 事件；
生产参数应基于实际测量选择。执行器 clone 共享 reporter，因此并发批次的回调事件可能交错；事件
不提供应用级批次关联。应用负责关联、指标存储、日志和重试决策。上面的计数 reporter 仅用于入门演示，不能
替代生产监控。回调不应等待依赖同一线程池的工作：来源阻塞时，同池 worker
可能持续被占用，无法保证其他批次及时获得进展。

### 选择任务失败策略

默认策略是 `TaskFailurePolicy::Continue`。`StopOnFirstFailure` 在一次失败后停止接纳新工作；
`StopAfterFailures(NonZeroUsize)` 在达到配置阈值后停止接纳。任务返回错误和被捕获的任务
panic 都计为失败。这些策略是协作式的：返回前会排空已接纳任务，最终失败数可能超过阈值，
`completed_count()` 也可能等于声明的任务数。
即使 `call` 返回 `Ok`，也要检查 outcome：

```rust
use std::num::NonZeroUsize;
use qubit_batch::{BatchExecutor, BatchTermination, TaskFailurePolicy};
use qubit_rayon_batch::RayonBatchExecutor;

fn main() {
    let limit = NonZeroUsize::new(2).expect("positive limit");
    for policy in [
        TaskFailurePolicy::Continue,
        TaskFailurePolicy::StopOnFirstFailure,
        TaskFailurePolicy::StopAfterFailures(limit),
    ] {
        let executor = RayonBatchExecutor::builder()
            .thread_count(2)
            .sequential_threshold(0)
            .task_failure_policy(policy)
            .build()
            .expect("executor should build");
        let calls = (0..16usize).map(|index| move || {
            if index % 4 == 0 { Err("invalid record") } else { Ok(index * 2) }
        });
        let result = executor.call(calls).expect("task errors are in the outcome");
        let outcome = result.outcome();
        assert!(!outcome.is_success());
        assert!(outcome.completed_count() <= outcome.task_count());
        assert_eq!(result.outputs().len(), outcome.succeeded_count());
        match policy {
            TaskFailurePolicy::Continue => {
                assert_eq!(outcome.termination(), BatchTermination::Finished);
                assert_eq!(outcome.failed_count(), 4);
                assert_eq!(outcome.completed_count(), 16);
            }
            TaskFailurePolicy::StopOnFirstFailure => assert!(outcome.failure_count() >= 1),
            TaskFailurePolicy::StopAfterFailures(limit) => {
                assert!(outcome.failure_count() >= limit.get());
            }
            _ => unreachable!(),
        }
        for output in result.outputs() {
            assert_eq!(*output.value(), output.index() * 2);
        }
    }
}
```

两个 worker 和阈值 0 会让这些非空批次走并行路径。短批次可能没有 `Running` 事件；生产参数应
基于实际测量选择。停止策略会在返回前排空已接纳的工作，因此失败数可能超过阈值，完成数也可能
达到声明的任务数。

[进度与失败策略可运行示例](../examples/progress_and_failure_policy.rs)展示三种策略，并检查保留的
输出下标和值。批次级错误也可能附带部分 outcome 和输出；不要将它们视为成功完成的批次。

运行该示例：

```bash
cargo run --locked --example progress_and_failure_policy
```

## 进阶用法

### 同一线程池的重入

`RayonBatchExecutor` 实现了 `Clone`；clone 之间共享线程池和执行器配置。如果某个任务在该
线程池上运行时调用同一执行器的 clone，嵌套调用会在当前 worker 上顺序执行。这样仍保持
同步调用契约，并避免嵌套线程池死锁。嵌套调用的结果不会自动合并到外层结果；任务应按应用
的错误模型检查或转换这个结果。

直接同池重入会回退到顺序执行；该保护不保证不同线程池之间同步循环调用（如 A → B → A）
的进展。在本次测试的大批次嵌套场景中，有界生产发送使 worker 无法进入 Rayon 可协助执行任务的
scope 等待阶段。应将编排移到工作线程池之外，或消除循环依赖。

下面的示例先转换内层批次级错误，再检查内层 outcome；内层任务失败时，显式返回外层任务错误。示例同时运行内层成功和失败两个分支。

```rust
use qubit_batch::BatchExecutor;
use qubit_rayon_batch::RayonBatchExecutor;

let executor = RayonBatchExecutor::builder()
    .thread_count(2)
    .sequential_threshold(0)
    .build()
    .expect("rayon batch executor should build");
for fail_inner in [false, true] {
    let nested_executor = executor.clone();
    let outcome = executor
        .execute_with_count(
            std::iter::once(move || {
                let nested_outcome = nested_executor
                    .execute_with_count(
                        [move || {
                            if fail_inner {
                                Err::<(), &'static str>("invalid nested record")
                            } else {
                                Ok(())
                            }
                        }],
                        1,
                    )
                    .map_err(|_| "nested batch failed")?;
                if nested_outcome.is_success() {
                    Ok(())
                } else {
                    Err("nested task failed")
                }
            }),
            1,
        )
        .expect("outer batch should return an outcome");
    assert_eq!(outcome.completed_count(), 1);
    assert_eq!(outcome.is_success(), !fail_inner);
    assert_eq!(outcome.failed_count(), usize::from(fail_inner));
    assert_eq!(outcome.panicked_count(), 0);
    if fail_inner {
        assert_eq!(outcome.failures()[0].index(), 0);
    }
}
```

批次返回 Ok 不代表任务全部成功。该示例把内层失败归并为一个外层任务错误，不会合并内层失败下标；应用需要明细时，应自行保留或转换内层 outcome。

来自普通外部线程的调用可以并发使用执行器 clone。各次调用的输入来源、结果和 callable
值彼此独立，但 clone 共享 worker 线程池。一个来源阻塞时，不能保证另一个 clone 的任务及时
获得进展或公平调度。应让来源及时产出，并避免回调或任务等待依赖同一线程池的工作。观察到的
延迟不足以证明永久死锁；来源恢复后，有限工作可以继续排空。

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

## 完成数量与终止原因

最后一个声明任务失败时，即使完成数量已经等于声明数量，来源也可能尚未返回 `None`。
顺序执行器会立即按失败策略停止，不会为了确认来源耗尽再拉取一项：

```rust
use qubit_batch::{BatchTermination, SequentialBatchExecutor, TaskFailurePolicy};
let executor = SequentialBatchExecutor::builder()
    .task_failure_policy(TaskFailurePolicy::StopOnFirstFailure).build();
let outcome = executor.execute_with_count([|| Err::<(), _>("invalid")], 1)
    .expect("policy stop returns an outcome");
assert_eq!(outcome.completed_count(), 1);
assert_eq!(outcome.termination(), BatchTermination::StoppedByTaskFailurePolicy);
assert!(!outcome.is_success());
```

并行执行中，生产线程可能先观察到 `None`，工作线程随后才失败。这时相同输入可以返回
`Finished`；若来源数量不足，则返回 `CountShortfall`。因此，阈值回退或同池重入可能改变
终止标签，但不改变已经发生的任务结果。`Finished` 不代表任务全部成功；策略停止时，
`completed_count == task_count` 也不能证明来源计数已经验证。选择重试项时，应结合外层
批次错误、完成计数和失败下标判断。已接纳的并行任务会继续执行，所以最终失败数可能超过
配置的停止阈值。

小批次和单 worker 的 callable 调用直接使用顺序执行器收集输出；Rayon 同池重入也走该路径。
trait 的 `Send` 要求与稀疏、有序的结果契约保持不变。通用并行适配器还会延迟调用自定义
`IntoIterator::into_iter`，使其发生在执行器开始消费来源、重入保护已经生效之后。此保证针对显式计数适配路径；
`call` 仍会先转换精确长度来源以读取 `len()`，然后再选择执行路径。

## 错误与诊断

成功返回的 callable 结果应分两部分检查：

1. 检查 `result.outcome().is_success()`、各项计数和 `failures()`。
2. 检查或消费 `result.outputs()`；其下标对应原始 callable 位置，任务失败时可能是稀疏的。

出现 `BatchCallError` 时，应先查看附带的 outcome 和部分输出，再决定是否重试。Runnable
任务体的 unwinding panic 会被捕获为任务失败；Runnable 自身析构时的 panic 可能传播给调用者，
且不保证返回结构化部分结果。Callable 适配器在正常完成时于捕获边界内析构已消费的 callable，
这不意味着裸 Runnable 的析构也具有相同保证。导致进程终止的 panic 无法被捕获。输入迭代器
panic 和同步 `Started`/终态进度回调 panic 会传播给调用者。reporter 返回的错误会成为批次级
进度错误；正在运行的 scoped 自动 reporter 回调 panic 也会包装为批次级进度错误。并行路径启用
reporter 时，每个批次可能额外需要一个 Rayon 工作线程池之外的 scoped 操作系统线程。
`thread_count`、`thread_name_prefix` 和 `stack_size` 配置的是 Rayon worker，不控制这个报告线程。
`NoopReporter` 会禁用报告且不会创建该线程。线程池构建成功后，后续报告线程创建仍可能失败并
导致 panic，且不会返回结构化批次错误或部分结果。这与正在运行的 reporter callback 内部 panic
不同，后者会转换为进度错误。调度失败或数量不匹配同样会保留错误发生前已经统计到的工作。

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
- clone 共享 worker 容量；来源阻塞时，不保证跨批次公平性或及时进展。
- 重试行为应依据副作用、事务和幂等规则决定；部分结果表示统计边界，不表示未报告工作
  一定没有发生。

## 延伸阅读

- [性能测量](performance.zh_CN.md)

- [README](../README.zh_CN.md)
- [English user guide](user_guide.md)
- [API 文档](https://docs.rs/qubit-rayon-batch)
- [核心 batch crate](https://docs.rs/qubit-batch)
- [Crate 发布页](https://crates.io/crates/qubit-rayon-batch)

## 来源耗尽与重入

运行时相关的调度器消费 `execute_with_source` 提供的惰性来源；上下文准入方法仅供
crate 内部使用。来源真实返回的 `None` 记录为耗尽；此前的 `None` 可能表示 reporter 或失败策略
已经停止准入。返回前始终会排空已经接受的 token。已经观察到来源耗尽时，会先返回声明
数量不足；否则提前因策略停止会返回 `StoppedByTaskFailurePolicy` 的部分结果。`Finished`
并不表示结果一定成功。

Rayon worker 上的同池重入和惰性来源生产线程上的同池重入都会回退到顺序执行。clone 共享
线程池容量；来源阻塞时，另一个 clone 的批次可能延迟，且没有跨批次公平性保证。独立线程池
拥有独立 worker 容量，但会增加线程资源。
