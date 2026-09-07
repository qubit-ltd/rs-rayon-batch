# Qubit Rayon Batch User Guide

[中文用户手册](user_guide.zh_CN.md) · [README](../README.md) ·
[API documentation](https://docs.rs/qubit-rayon-batch)

Applies to `qubit-rayon-batch` 0.9 and Rust 1.94 or later. This guide is for an
application or library author that has a finite CPU-oriented batch to execute
and wants the `qubit-batch` result and error model on a dedicated Rayon pool.

## Purpose and Audience

Use `RayonBatchExecutor` when each input is an independent runnable or
callable operation and the caller needs task failures, stable indexes, timing,
and progress reporting. The executor owns a dedicated pool for its lifetime;
it is a reusable batch executor, not a persistent queue, scheduler, or retry
service. The companion `qubit-batch` crate defines the shared result,
processor, and error types.

## Conceptual Model

```text
finite source ──> RayonBatchExecutor ──> BatchOutcome / BatchCallResult
                         │                 ├─ input counters and failures
                         │                 ├─ successful callable values
                         │                 └─ partial result on batch errors
                         ├─ dedicated pool reused across calls
                         ├─ small batches use qubit-batch's sequential executor
                         ├─ same-pool reentry uses qubit-batch's sequential executor
                         └─ larger batches use Rayon in_place_scope_fifo
```

The executor chooses sequential execution when the declared count is at or
below `sequential_threshold`, when it has one worker, or when the caller is
already running on this executor's own Rayon pool. Larger batches are submitted
through Rayon’s `in_place_scope_fifo`. The same-pool branch is a reentrancy
safeguard: a task can call a clone of the same executor and the nested batch
runs sequentially on the current worker instead of waiting for a worker that
is occupied by its parent task.

## Scenario: Run a CPU Validation Batch

Suppose a service validates eight independent records and needs every failure
with its original position. The success criterion is seven successful callable
values and one indexed task failure.

### Install the crates

```toml
[dependencies]
qubit-batch = "0.11"
qubit-rayon-batch = "0.9"
```

### Execute callable tasks

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

`BatchCallResult` keeps successful values sparsely by their original callable
index, while `BatchOutcome` keeps failures in index order. A returned callable
error is a task failure; a source count mismatch or progress failure is a
batch-level error with the partial result attached.

## Core Workflow

Build one executor for the workload, then reuse it for calls of different
finite sizes. Choose `thread_count` and `sequential_threshold` from measured
workloads. A threshold of zero asks for Rayon workers for every non-empty batch
that is not already running in the same pool.

`execute` and `execute_with_count` accept `Runnable` tasks. `call` and
`call_with_count` adapt `Callable` tasks and collect successful values. The
exact-size forms derive the declared count from the iterator; use a
`*_with_count` method when the count comes from a separate database or service
boundary. The source is consumed once, and accepted tasks finish before the
method returns.

`BatchExecutor::for_each` is a convenient item adapter, but its trait action
must satisfy `Fn + Send + Sync` because items may execute concurrently. A
callable is different: `Callable::call` receives `&mut self`, so its operation
has `FnMut` semantics. The Rayon trait path still requires the callable,
successful value, and error to be `Send`; it does not provide the concrete
sequential executor's non-`Send` callable path. Give each parallel task its
own state rather than sharing one mutable callable across workers.

## Advanced Usage

### Reentrant use of the same pool

`RayonBatchExecutor` implements `Clone`; clones share the pool and the
executor configuration. If a task running on that pool invokes a clone, the
nested call is executed sequentially on the current worker. This preserves the
synchronous call contract and avoids nested pool deadlock. It does not merge
the nested call's outcome into the outer outcome: the task must inspect or
translate that result according to the application's error model.

For example, an outer task can make a small nested call and translate its
batch-level error into the outer task's error type:

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

Calls made from ordinary external threads can use executor clones
concurrently. Their sources, outcomes, and callable values remain independent.

### Input counts and business effects

Executor counters describe input tasks. For processor APIs from `qubit-batch`,
`processed_count` means successful input items and obeys
`processed_count <= completed_count <= item_count`. A database import may have
two successful input items that affect six rows; keep `affected_rows = 6` in
the application or domain result rather than reporting six processed inputs.

### Result memory

For a callable batch with S successful values and F failure details, the
retained result data is O(S + F), plus the heap size of each value and error.
The executor is therefore not a constant-memory streaming channel. Consume or
discard each completed chunk's outputs before starting more work when the
logical input is large.

### Bounded chunking

The following pattern keeps each callable result bounded while processing a
large range. Every iteration creates an independent `BatchCallResult`; the
outer loop decides whether to continue.

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

These are separate batches. The pattern does not provide one global failure
policy, global stable indexes, or automatic retry across chunks. If an
application needs a global input position, add the chunk start offset to each
`output.index()`. For a `ChunkedBatchProcessor`, `ChunkFailed` exposes the
aggregate result for the successful chunks before the failing chunk. That
prefix does not prove the failed chunk had no side effects, so retry only when
the delegate's transaction and idempotency rules make it safe. The same
boundary applies to manually chunked calls: inspect the failing chunk and let
the outer application choose whether to stop, compensate, or retry it.

## Errors and Diagnostics

Inspect a successful call in two parts:

1. Check `result.outcome().is_success()`, counters, and `failures()`.
2. Consume or inspect `result.outputs()`; output indexes identify the original
   callable positions and are sparse when tasks failed.

On `BatchCallError`, inspect the attached outcome and partial outputs before
choosing a retry. Task panics are captured as task failures. Iterator panics
and synchronous reporter panics propagate to the caller; an automatic running
reporter failure is represented as a batch-level progress error. A scheduler
failure or count mismatch also carries the work that was accounted for before
the error.

## Troubleshooting

| Symptom | Check | Action |
| --- | --- | --- |
| A small batch does not use Rayon workers | `sequential_threshold()` and declared count | Lower the threshold only after benchmarking. |
| A task invokes the same executor | Whether the task runs on this executor's pool | The nested call uses the sequential fallback; inspect its result inside the task. |
| `call` rejects a type at compile time | `Callable`, `Send` for callable/value/error | Use the Rayon trait path with sendable values, or use `SequentialBatchExecutor` for local non-`Send` callables. |
| `Ok` result contains failures | `outcome().is_success()` and `failures()` | Classify each indexed task error or panic. |
| A chunk retry duplicates effects | Delegate transaction and idempotency guarantees | Do not retry blindly from the successful prefix; use the domain's local result and retry boundary. |
| Memory grows with a large batch | Number and size of successful values and failures | Split into independent chunks and consume outputs promptly. |

## Limitations and Best Practices

- Inputs are finite and consumed once. The crate does not provide durable
  scheduling, queues, persistence, or cross-call retry orchestration.
- The pool is reusable, but each batch still retains its result until the
  caller drops or consumes it.
- `for_each` actions must be `Fn + Send + Sync`; callable operations are
  `FnMut`-like through `&mut self`, with Rayon `Send` requirements.
- Same-pool reentrancy uses sequential execution. It does not create extra
  parallelism or combine nested outcomes.
- Progress intervals throttle implementation-defined progress points; they do
  not guarantee an immediate event at a wall-clock deadline.
- Choose retry behavior from side-effect, transaction, and idempotency rules;
  a partial result is an accounting boundary, not proof that unreported work
  did not happen.

## Further Reading

- [README](../README.md)
- [中文用户手册](user_guide.zh_CN.md)
- [API documentation](https://docs.rs/qubit-rayon-batch)
- [Core batch crate](https://docs.rs/qubit-batch)
- [Crate package](https://crates.io/crates/qubit-rayon-batch)

## Source exhaustion and reentrancy

Runtime-specific schedulers should pull lazy sources through
`ParallelBatchExecutionContext::next_task`. It records a source `None` as
exhaustion, while a `None` before exhaustion can mean that a reporter or the
failure policy stopped admission. Accepted tokens are always drained before
return. If exhaustion was observed, a declared-count shortfall is reported
before the failure-policy termination; otherwise an early policy stop returns a
partial outcome with `StoppedByTaskFailurePolicy`. `Finished` does not imply
that the outcome is successful.

Both a same-pool Rayon worker reentry and same-pool reentry from a lazy-source
producer fall back to sequential execution. Independent pools and concurrent
clones retain parallel execution.
