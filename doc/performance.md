# Rayon batch performance

[中文](performance.zh_CN.md) · [User guide](user_guide.md)

## Callable fallback measurements (2026-09-09)

Environment: Intel Core i5-9600K, six cores, Linux x86_64 7.0.0-30-generic,
Rust 1.94.0 / LLVM 21.1.8, release profile, NoopReporter. Baselines are
qubit-batch `62e2a3d` and qubit-rayon-batch `99ce8c5`, with the same new
benchmark sources overlaid. Path dependency qubit-progress is 0.8.3 in both.
The candidate adds direct sequential callable collection and reusable chunks;
the production Rayon shared queue is unchanged.

`cargo bench --locked --bench call_fallback_bench` covers 0/1/32/100/101/1024
items, scalar/String outputs, two workers with threshold 100 or 0, and one
worker with threshold 0. Executors are constructed outside timing; standard
scoped workers are still created inside each parallel call. Each point uses
20 samples, 200 ms warmup, and 500 ms requested measurement. Untimed checks
validate success and output counts. Result destruction is included.

Selected means and 95% confidence intervals, in microseconds:

| Configuration / items | Baseline mean [95% CI] | Refined mean [95% CI] |
| --- | ---: | ---: |
| default / 32 | 4.049 [3.958, 4.150] | 2.738 [2.712, 2.768] |
| default / 100 | 11.190 [11.016, 11.401] | 7.475 [7.408, 7.552] |
| one-worker / 1024 | 107.131 [105.137, 109.186] | 88.815 [81.444, 98.578] |
| parallel / 1024 | 2057.423 [2036.004, 2079.127] | 2399.644 [2144.694, 2781.124] |

These are sequentially collected local measurements, not a portable speedup
promise. Worker creation and system scheduling make tiny forced-parallel calls
especially noisy. Do not change the default threshold of 100 from these data.
Criterion raw estimates and sample data are emitted under `target/criterion`.
Use the same benchmark source and dependency checkout for baseline comparison.

## Queue candidate rejected by correctness

A separate candidate assigned each worker its own capacity-one channel, with
round-robin producer dispatch. Although the nominal admission window remained
2W+1, `test_cloned_executor_concurrent_calls_remain_independent` timed out twice
(2-second rendezvous, then the 10-second process watchdog). The same test
passes with the retained shared receiver.

A slow task can hold one consumer while the producer blocks on that consumer's
full queue. Another consumer can then wait on its empty queue, occupying a Rayon
thread needed by the other batch. The shared queue lets an available consumer
drain the remaining work. This code-path analysis explains the observed
rendezvous starvation; it is not evidence of a defect in the retained scheduler.

The candidate was rejected before performance selection. Three candidate timing
rounds and the 10% improvement / 5% regression gate are therefore inapplicable:
correctness is a prerequisite, not a tradeoff for throughput. No candidate
feature or scheduler-selection API was added.

For the retained scheduler, `cargo bench --locked --bench scheduling_matrix_bench
-- 'gate-rayon-[24]'` measures no-op, fine CPU, heavy CPU, skewed CPU and slow
source cases. Each case has 1024 items, 30 samples, 200 ms warmup and 500 ms
requested measurement, and was repeated three times. The `steady-callers-rayon`
groups reuse 2/4 caller threads; handoff remains timed. The full matrix also
covers 32/100/101/1024/65536 items and 1/2/4 workers where the CPU permits.
