# Rayon 批量执行性能

[English](performance.md) · [用户手册](user_guide.zh_CN.md)

## Callable 回退测量（2026-09-09）

环境为 Intel Core i5-9600K（6核）、Linux x86_64 7.0.0-30-generic、Rust 1.94.0 /
LLVM 21.1.8、release 构建和 NoopReporter。基线是 qubit-batch `62e2a3d` 与
qubit-rayon-batch `99ce8c5`，覆盖相同的新 benchmark 源文件；两组都使用本地
qubit-progress 0.8.3。

### 历史快照记录

保留记录标明的基线提交为 qubit-batch
`62e2a3d47793a9035e475f09eedc9fe2dab5db15` 和 qubit-rayon-batch
`99ce8c5457569dba6ab09db4144f57cca9e3b2f6`，但没有记录实测候选提交、benchmark
overlay 的精确版本或候选依赖快照。仓库未保留原始 Criterion 样本。相关实现提交描述的是
代码历史，不能证明哪些构建产生了这些测量。表格是历史观测，不能作为当前版本的可复现比较。
运行当前 benchmark 测量的是当前负载，不能重建完整的历史比较。参见相关的
[rs-batch 性能记录](https://github.com/qubit-ltd/rs-batch/blob/main/doc/performance.zh_CN.md#callable-回退测量2026-09-09)。

历史记录将候选描述为加入直接顺序 callable 收集及缓冲区复用；生产 Rayon 共享队列保持原样。

运行 `cargo bench --locked --bench call_fallback_bench` 可覆盖 0/1/32/100/101/1024 项、
标量和 String 返回值，以及双 worker（阈值100/0）和单 worker（阈值0）配置。执行器及其专用
Rayon 工作线程池在计时外创建，工作线程跨调用复用。每次并行调用仍包含 scope 内任务调度和
工作通道成本。本 benchmark 使用 NoopReporter，不创建自动报告线程。一般情况下，并行批次启用
报告后可能额外创建一个 Rayon 池之外的 scoped 操作系统报告线程；这与 Rayon 工作线程的复用
是两件事。每点20个样本、预热200毫秒、请求测量500毫秒；计时前校验成功数和输出数，计时包含
结果析构。

部分结果如下，单位为微秒，方括号内为均值的95%置信区间：

| 配置 / 项数 | 基线均值 [95% CI] | 改进均值 [95% CI] |
| --- | ---: | ---: |
| default / 32 | 4.049 [3.958, 4.150] | 2.738 [2.712, 2.768] |
| default / 100 | 11.190 [11.016, 11.401] | 7.475 [7.408, 7.552] |
| one-worker / 1024 | 107.131 [105.137, 109.186] | 88.815 [81.444, 98.578] |
| parallel / 1024 | 2057.423 [2036.004, 2079.127] | 2399.644 [2144.694, 2781.124] |

这些数据来自本机串行测量，不构成通用加速承诺。每批次调度成本和系统调度可能影响强制并行的
极小批次；现有测量没有分离各项成本的具体贡献。不应据此修改默认阈值100。新运行会在
`target/criterion` 写出 Criterion 原始估计与样本；
这是新测量的产物位置，历史样本并未保存在此处。进行新的基线对比时，应使用相同 benchmark
源码和依赖检出版本。

## 修正前的 CPU 负载核查（2026-11-01）

在考虑修改前，先检查了已编译 benchmark 目标中的当前 `CpuTask::run` 负载。本次使用
`b1a09a5` 提交、现有 benchmark 源码、默认 release profile、Rust 1.94.0 / LLVM 21.1.8、
Intel Core i5-9600K（6 核）和 Linux x86_64 7.0.0-38-generic。`Cargo.lock` 的 SHA-256 为
`ac46a0543e12d089ae226a329783880c57542549d5c141e0f8114b2f3eeac735`。
该基线提交的 worktree 初始干净；开始测量时，差异仅包括 T1～T3 的五个文件（增加76行、删除
83行），benchmark 源文件没有差异。

实际目标通过
`cargo rustc --locked --offline --release --bench rayon_batch_executor_bench -- --emit=asm`
编译。在 `target/release/deps/rayon_batch_executor_bench-cc4c68cf540d74c2.s` 中，内联的
`ParallelBatchExecutionContext::execute_task` 路径 `.LBB42_3`，以及
`benchmark_cpu_case<ParallelBatchExecutor>` worker 路径 `.LBB414_15`、`.LBB414_27`，包含
从 256 开始的循环、`imulq`、`addq` 和每轮减 8 的计数器。LLVM 将递推的每八步合并为一次
等价乘加；每个任务仍运行32轮，保留全部256步串行递推。最终值仍传入原有的 `black_box`
屏障。因此负载没有被整体折叠消除，本次没有修改 benchmark 代码。

三轮测量使用相同命令，`--save-baseline` 分别为 `rb005-before-1`、`rb005-before-2`、
`rb005-before-3`：
`cargo bench --locked --bench rayon_batch_executor_bench -- batch_executor_cpu --save-baseline <名称>`。
Criterion 使用默认100个样本、3秒预热和5秒目标测量时间。部分点
出现无法在目标时间内完成100个样本的提示；样本数和计时参数均未调整。下表以微秒为单位，
列出均值及95%置信区间，数值来自 Criterion 的 slope estimates。

| 用例 | 第1轮 | 第2轮 | 第3轮 |
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

Criterion 样本与估计已保留在本地目录
`target/criterion/batch_executor_cpu/<executor>/<size>/rb005-before-<轮次>/`，其中轮次为1、2或3。
三轮波动明显：
例如 `scoped_parallel / 32` 从第1轮的120微秒变为第2、3轮的17至20毫秒；多个并行点也出现较多
严重离群值。该主机环境不足以支持可靠的执行器对比或生产加速结论。当时的汇编排除了 CPU 负载被消除的疑点；后续 barrier 修改见下文修正后的负载测量。

## 修正后的 CPU 负载测量（2026-11-03）

现在 `CpuTask::run` 在256次递推循环的每次迭代输入处调用
`black_box(value)`。乘数、加数、迭代次数、循环后的 barrier 和 benchmark ID 均保持不变。
通过 `cargo rustc --locked --release --bench rayon_batch_executor_bench -- --emit=asm`
生成的 release 汇编中，`.LBB42_3`、`.LBB414_15`、`.LBB414_27`、`.LBB417_47` 和
`.LBB417_62` 展示了256次循环计数、每次迭代一次乘法和递增、计数器每次减1，以及循环输入处的
black-box 编译器屏障（`#APP` / `#NO_APP`）。这是 executor worker 执行 `CpuTask` 的编译路径。
此前核查中的八步合并负载与本次修正后的负载不可直接比较；本文不作前后速度结论。

运行 ID `RB-20261103T123700Z` 使用的 benchmark 源码 SHA-256 为
`256c7518d5378bb37698614fa288c7ec276c715c9a2e34f4602e5063a11107bf`，`Cargo.toml` SHA-256 为
`946349a0ec2b9708b5887462f908772b5232097d034a7d34fa3dc3b5455320b7`，`Cargo.lock` SHA-256 为
`ac46a0543e12d089ae226a329783880c57542549d5c141e0f8114b2f3eeac735`。测量基于提交
`c19b736b654ac4c42744d66620de9358200527d9`，当时 worktree 仅有本次 benchmark 源码修改。
主机为 Linux x86_64，内核 `6.8.0-147-generic`，Intel Xeon E5-2680 v4 2.40 GHz，可用逻辑 CPU 为56。
Rust 1.94.0（LLVM 21.1.8），Cargo 1.94.0。依赖由 `Cargo.lock` 锁定，无外部 path 或 patch 依赖。

只运行了一次目标 benchmark；Criterion 使用默认参数（100个样本、3秒预热、每点请求测量5秒），
并行 executor 使用4个 worker，过滤条件和 baseline 参数如下：

```sh
cargo bench --locked --bench rayon_batch_executor_bench -- batch_executor_cpu --save-baseline rb-cpu-20261103T123700Z
```

Criterion 估计值单位为微秒，格式为均值 [95%置信区间]，仅代表本次运行：

| Executor / 项数 | 32 | 64 | 100 | 128 | 256 |
| --- | ---: | ---: | ---: | ---: | ---: |
| sequential | 30.089 [29.452, 30.745] | 56.322 [56.080, 56.647] | 86.350 [86.318, 86.385] | 110.40 [110.37, 110.44] | 223.03 [221.14, 225.08] |
| scoped_parallel | 149.45 [146.59, 152.88] | 164.25 [161.36, 167.61] | 163.35 [160.82, 166.71] | 184.39 [176.97, 193.86] | 437.29 [279.79, 673.26] |
| rayon | 26.836 [24.924, 29.834] | 50.589 [44.681, 57.766] | 75.376 [75.131, 75.560] | 93.413 [93.100, 93.698] | 129.14 [124.97, 133.13] |

部分用例出现明显离群值：sequential / 32 为20%，rayon / 64 为24%，scoped_parallel / 256 为17%。
其中 scoped_parallel / 256 的区间较宽。这是一次本地运行，不能构成稳定的 executor 对比或性能提升结论。
仅本机可访问的完整证据包（含日志、汇编、源码快照和本次运行 Criterion 样本及估计）位于
`/data/working/qubit/evidence/rayon-batch/RB-20261103T123700Z`；归档 SHA-256：`c03c2446e5c7b272ade1a8a6e385d364bb6ad9f28f1954541ebae6d172cd3646`。
归档及校验文件在此仅本机可访问目录中无限期保留，不可公开访问。

## 因正确性失败淘汰队列候选

独立实验把共享接收队列改成每 worker 一个容量1的通道，由生产者轮询派发。虽然名义接纳
窗口仍为2W+1，`test_cloned_executor_concurrent_calls_remain_independent` 连续两次超时：
先超过2秒 rendezvous 等待时间，再由10秒进程 watchdog 终止。保留的共享队列通过相同测试。

从代码路径看，慢任务占用消费者后，生产者可能阻塞在该消费者已满的队列；另一个消费者
随后等待自己的空队列，继续占着另一批次需要的 Rayon 线程。共享队列允许空闲消费者继续
排空剩余工作。这能解释观察到的 rendezvous 饥饿，并不说明保留的生产调度有同样缺陷。

候选在进入性能选型前已被淘汰，因此不再执行候选三轮耗时比较，也不套用“改善10%、退化
不超过5%”的性能门槛：正确性是前提，不能拿吞吐量交换。本轮没有增加实验 feature 或调度
策略选择 API。

保留实现可运行 `cargo bench --locked --bench scheduling_matrix_bench -- 'gate-rayon-[24]'`
测量无操作、细粒度CPU、重CPU、倾斜CPU和慢来源五组。本轮每组1024项、30个样本、预热
200毫秒、请求测量500毫秒，重复三轮。`steady-callers-rayon` 组复用2/4个调用线程，计时仍含
请求交接。完整矩阵另外覆盖32/100/101/1024/65536项和机器核心数允许的1/2/4个 worker。

## 报告器开销对照（2026-11-02）

本地实验比较 `NoopReporter` 路径与已启用的轻量 `CountingReporter`，测量的是端到端报告
生命周期和基础设施成本；两者差值不等同于单独创建操作系统线程的成本。

benchmark 源码基于提交
`40d599e3989ff7920c218aa5a6c8f016bf040641`。测量时工作树还包含新增测试和 benchmark 改动
（这两个文件合计增加156行）；benchmark 源文件 SHA-256 为
`d7a49c6961c39efba1c1e1c97f98097a298f7093e405b1e4fe766a58128d7177`，`Cargo.lock` SHA-256 为
`ac46a0543e12d089ae226a329783880c57542549d5c141e0f8114b2f3eeac735`。环境为 Rust 1.94.0 /
LLVM 21.1.8、Linux x86_64 7.0.0-38-generic、Intel Core i5-9600K（6核），可用并行度为6。
两种模式均使用2个 Rayon worker、阈值100、报告间隔5秒，以及默认 Continue 失败策略。

单调用用例为100、101、1024项，30个样本、200毫秒预热、请求测量500毫秒。稳态调用线程
helper 复用2或4个调用线程，使用30个样本及 Criterion 默认的3秒预热和5秒请求测量。执行器、
线程池和报告器在计时区间外构造；每批次报告线程的生命周期仍包含在 API 调用计时内。稳态组
计入请求交接时间。每一轮都按顺序测量两种模式，只改变 baseline 名称：

```sh
cargo bench --locked --bench scheduling_matrix_bench -- '^(reporting-|steady-callers-reporting-)' --save-baseline reporting-round-1
cargo bench --locked --bench scheduling_matrix_bench -- '^(reporting-|steady-callers-reporting-)' --save-baseline reporting-round-2
cargo bench --locked --bench scheduling_matrix_bench -- '^(reporting-|steady-callers-reporting-)' --save-baseline reporting-round-3
```

下表为 Criterion `estimates.json` 中的均值估计及95%置信区间，单位为微秒。Criterion 原始
数据曾作为本地实验产物生成于 `target/criterion/<组名>/<用例>/reporting-round-<轮次>/`，未作为
样本提交到仓库。

| 用例 | 第1轮均值 [95% CI] | 第2轮均值 [95% CI] | 第3轮均值 [95% CI] |
| --- | ---: | ---: | ---: |
| reporting-disabled / 100 | 82.355 [70.205, 95.116] | 45.903 [40.995, 51.341] | 82.442 [68.487, 98.767] |
| reporting-disabled / 101 | 2575.427 [794.759, 5265.859] | 680.388 [664.279, 702.225] | 717.557 [704.302, 732.845] |
| reporting-disabled / 1024 | 40729.013 [12116.166, 80253.485] | 7117.721 [6762.251, 7601.442] | 7088.033 [6211.107, 7865.550] |
| reporting-enabled / 100 | 118.029 [103.691, 134.610] | 167.699 [127.005, 213.235] | 33.809 [33.765, 33.858] |
| reporting-enabled / 101 | 98244.539 [69830.194, 129549.164] | 170943.114 [120583.297, 223595.447] | 397.273 [160.723, 728.499] |
| reporting-enabled / 1024 | 143362.953 [68153.020, 247070.752] | 443469.157 [208957.485, 749421.079] | 1477.091 [1035.331, 1998.550] |
| steady-callers-reporting-disabled / 2 | 159396.177 [116354.272, 205774.010] | 3899.164 [2424.711, 5651.181] | 817.536 [727.444, 964.829] |
| steady-callers-reporting-disabled / 4 | 253270.066 [166887.852, 350385.758] | 25273.894 [3455.167, 62245.422] | 1834.421 [1731.757, 1939.352] |
| steady-callers-reporting-enabled / 2 | 227573.458 [160273.752, 300244.301] | 401913.502 [264272.999, 559008.907] | 5319.069 [2408.108, 9061.631] |
| steady-callers-reporting-enabled / 4 | 24744.774 [7807.463, 48325.367] | 602639.392 [357777.797, 898043.696] | 3828.232 [2939.602, 4916.129] |

多项测量出现 severe outliers，Criterion 也提示30个样本无法在请求测量时间内完成。若干用例的
三轮结果相差数个数量级，因此该主机上的数据无法支持稳定性能结论。本次运行确认启用报告的
路径实际生效，且生命周期回调成对出现。不要求 Running 回调次数，也不以耗时阈值判断正确性。
这些结果不足以支持修改默认阈值或报告架构。

测量完成后，`align-ci.sh` 对 benchmark 源码只做了格式排版调整；最终对齐后的源文件 SHA-256 为
`a1ccf12f020f61697d3348fd6e643b28a8ccdbafbb8c67ed637d2fb767867133`。对齐脚本清理了 `target`
下的构建产物，但保留了 Criterion 输出；按验证后的清理流程会删除这些本地样本，以上估计则
保存在本文中。

## 后续实验归档

今后每次用于支持正式性能结论的实验，都应以唯一运行 ID 建立证据包。普通开发者试跑无需归档。
本规程不补造历史样本，也不改变上文所述限制；不要仅为填充归档规程而运行性能矩阵。

证据包应保存在仓库 `target/`、临时目录和清理脚本管理路径以外的持久位置，并使用绝对路径。
以下 GNU/Linux shell 模板创建新的证据包，并记录起始提交、工作树状态、tracked 修改、工具链版本、
锁文件、清单及 benchmark 源码：

```sh
# 选择 target 和清理脚本管理范围之外的持久目录。
evidence_root=/absolute/persistent/evidence
run_id=rayon-batch-YYYYMMDDTHHMMSSZ
bundle="$evidence_root/$run_id"
mkdir -p "$evidence_root"
mkdir "$bundle"
git rev-parse HEAD > "$bundle/commit.txt"
git status --porcelain=v1 > "$bundle/status.txt"
git diff --binary HEAD > "$bundle/tracked.diff"
rustc -Vv > "$bundle/rustc.txt"
cargo -V > "$bundle/cargo.txt"
cp Cargo.lock Cargo.toml "$bundle/"
cp -R benches "$bundle/benches"
```

测量前，将 `commands.txt`、`environment.txt` 和 `dependencies.txt` 写入证据包。执行时记录完整 benchmark
命令、过滤条件、baseline 名称及每次运行的顺序，并保留标准输出和错误日志。记录测量时实际使用的
操作系统、CPU、可用并行度、Criterion 参数及 benchmark 源码版本。除已跟踪文件外，也要快照相关的未
跟踪源码，并记录实际测量所用 `Cargo.toml`、`Cargo.lock`、benchmark 源码和其他源码快照的 SHA-256。
在 `dependencies.txt` 中记录外部 path/patch 依赖的版本、提交和 dirty 状态，并纳入必要源码快照；没有此类
依赖时明确写明无。记录归档生成时间和证据保留期限。

测量后，把完整日志放入证据包，并复制本次运行对应的 Criterion 数据且保留目录结构。如果复制整个
Criterion 树，应列出本次运行所属的确切 benchmark group 和 baseline，避免把旧数据误认为本次样本。为
证据包文件生成清单，创建归档时不得覆盖旧归档，并在包外记录归档摘要：

```sh
cp -R target/criterion "$bundle/criterion"
(cd "$bundle" && find . -type f ! -name SHA256SUMS -exec sha256sum {} + > SHA256SUMS)
test ! -e "$bundle.tar.gz"
tar -czf "$bundle.tar.gz" -C "$evidence_root" "$run_id"
sha256sum "$bundle.tar.gz" > "$bundle.tar.gz.sha256"
sha256sum -c "$bundle.tar.gz.sha256"
tar -tzf "$bundle.tar.gz" > "$bundle.archive-files.txt"
```

任一步骤失败都应停止。将归档解压到独立校验目录，并在那里运行 `sha256sum -c SHA256SUMS`。只有在解压、
包外 `sha256sum -c "$bundle.tar.gz.sha256"` 校验以及清单中每个条目都验证成功后，才能清理本地 Criterion
样本。按记录的保留策略保存归档、包外摘要和校验结果。不得静默覆盖已有运行 ID 或归档。

每条正式性能记录都应引用运行 ID、稳定的证据包位置、归档 SHA-256 和保留策略。本机路径必须标注“仅本机
可访问”，不得称为公开可复核。只有在获得相应授权后才能上传或发布附件。本规程不配置凭据或上传服务；
尚无真实正式实验时，不得虚构归档链接。
