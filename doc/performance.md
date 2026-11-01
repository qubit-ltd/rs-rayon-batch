# Rayon batch performance

[中文](performance.zh_CN.md) · [User guide](user_guide.md)

## Callable fallback measurements (2026-09-09)

Environment: Intel Core i5-9600K, six cores, Linux x86_64 7.0.0-30-generic,
Rust 1.94.0 / LLVM 21.1.8, release profile, NoopReporter. Baselines are
qubit-batch `62e2a3d` and qubit-rayon-batch `99ce8c5`, with the same new
benchmark sources overlaid. Path dependency qubit-progress is 0.8.3 in both.

### Historical snapshot record

The retained record identifies the baseline commits as qubit-batch
`62e2a3d47793a9035e475f09eedc9fe2dab5db15` and qubit-rayon-batch
`99ce8c5457569dba6ab09db4144f57cca9e3b2f6`, but does not identify the measured
candidate commits, the exact benchmark overlay revision, or the candidate
dependency snapshot. The original Criterion samples are not retained in the
repository. Related implementation commits describe code history, not which
builds produced these measurements. The table is a historical observation,
not a reproducible comparison of the current release. Running the current
benchmark measures the current workload; it does not recreate the complete
historical comparison. See the related [rs-batch performance record](https://github.com/qubit-ltd/rs-batch/blob/main/doc/performance.md#callable-fallback-measurements-2026-09-09).

The historical record describes the candidate as adding direct sequential
callable collection and reusable chunks; the production Rayon shared queue was
unchanged.

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
A new run emits Criterion raw estimates and sample data under
`target/criterion`; this is the output location for new measurements, not a
retained location for the historical samples. Use the same benchmark source
and dependency checkout for any new baseline comparison.

## CPU workload audit (2026-11-01)

The current `CpuTask::run` workload was checked in the compiled benchmark
target before considering a change. The run used commit `b1a09a5`, the existing
benchmark source, the default release profile, Rust 1.94.0 / LLVM 21.1.8, and
an Intel Core i5-9600K (6 cores) on Linux x86_64 7.0.0-38-generic. The
`Cargo.lock` SHA-256 was
`ac46a0543e12d089ae226a329783880c57542549d5c141e0f8114b2f3eeac735`.
The worktree was clean at that baseline; when measurement began, its diff was
limited to the five T1–T3 files (76 insertions, 83 deletions), and the
benchmark source itself had no diff.

The actual target was compiled with
`cargo rustc --locked --offline --release --bench rayon_batch_executor_bench -- --emit=asm`.
In `target/release/deps/rayon_batch_executor_bench-cc4c68cf540d74c2.s`, the
inlined `ParallelBatchExecutionContext::execute_task` path at `.LBB42_3` and
the `benchmark_cpu_case<ParallelBatchExecutor>` worker paths at `.LBB414_15`
and `.LBB414_27` contain a loop initialized with 256, followed by `imulq`,
`addq`, and a counter decrement of 8. LLVM combined each eight-step portion
of the recurrence into one equivalent multiply-add; the loop still runs 32
times per task, preserving all 256 serial recurrence steps. The final value
reaches the existing `black_box` barrier. The workload was therefore not
folded away, and no benchmark code change was made.

Three runs used the same command with `rb005-before-1`, `rb005-before-2`, and
`rb005-before-3` as the `--save-baseline` value:
`cargo bench --locked --bench rayon_batch_executor_bench -- batch_executor_cpu --save-baseline <name>`.
Criterion used its default 100 samples, 3-second
warmup, and 5-second target measurement. It warned that some points could not
complete 100 samples in the target time; the sample count and timing settings
were left unchanged. Values below are means in microseconds with 95%
confidence intervals, read from Criterion's slope estimates.

| Case | Round 1 | Round 2 | Round 3 |
| --- | ---: | ---: | ---: |
| sequential / 32 | 5.122 [4.613, 5.689] | 6.487 [6.137, 6.856] | 3.998 [3.707, 4.334] |
| sequential / 64 | 7.073 [6.586, 7.583] | 19.580 [18.304, 20.845] | 18.859 [16.869, 21.221] |
| sequential / 100 | 39.711 [37.215, 42.272] | 29.657 [27.337, 32.085] | 17.872 [16.134, 19.835] |
| sequential / 128 | 19.346 [17.991, 20.889] | 39.604 [35.886, 43.392] | 42.111 [39.267, 44.735] |
| sequential / 256 | 22.049 [20.515, 23.745] | 83.997 [78.671, 88.865] | 23.062 [21.211, 25.185] |
| scoped_parallel / 32 | 120.118 [96.932, 150.970] | 17,202.896 [15,250.316, 19,169.576] | 19,745.589 [18,070.891, 21,486.337] |
| scoped_parallel / 64 | 263.524 [210.656, 327.716] | 24,246.597 [17,235.986, 31,948.594] | 41,114.036 [37,123.446, 45,377.662] |
| scoped_parallel / 100 | 82,301.412 [70,616.831, 94,589.492] | 49,191.797 [39,446.507, 59,350.373] | 61,246.612 [51,597.635, 71,237.835] |
| scoped_parallel / 128 | 453.510 [263.058, 694.050] | 39,518.429 [32,968.483, 46,519.593] | 61,168.034 [49,660.002, 73,207.181] |
| scoped_parallel / 256 | 2,947.095 [1,967.110, 4,019.337] | 55,616.219 [39,072.960, 73,983.197] | 834.934 [267.342, 1,613.536] |
| rayon / 32 | 24.745 [16.196, 39.102] | 23,584.835 [19,676.930, 27,525.912] | 137.386 [132.676, 145.750] |
| rayon / 64 | 15.808 [15.280, 16.486] | 2,981.920 [844.427, 5,742.047] | 6,647.650 [3,288.597, 10,536.149] |
| rayon / 100 | 1,460.148 [576.762, 2,856.013] | 14,973.517 [6,110.016, 26,214.852] | 7,681.798 [3,864.360, 12,050.569] |
| rayon / 128 | 49.109 [46.609, 52.108] | 6,487.184 [1,709.577, 12,839.068] | 562.100 [530.841, 621.618] |
| rayon / 256 | 2,026.947 [976.221, 3,411.025] | 4,568.617 [1,950.743, 8,212.061] | 2,514.571 [1,507.484, 3,791.988] |

Criterion sample data and estimates are retained locally under
`target/criterion/batch_executor_cpu/<executor>/<size>/rb005-before-<round>/`,
where `<round>` is 1, 2, or 3.
The runs varied substantially: for example, `scoped_parallel / 32` ranged
from 120 microseconds in round 1 to 17–20 milliseconds in rounds 2 and 3;
many parallel points also had numerous severe outliers. This host was not
stable enough for an executor comparison or a production speedup claim. The
assembly rules out the suspected removal of the CPU workload, so the current
benchmark remains unchanged.

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
