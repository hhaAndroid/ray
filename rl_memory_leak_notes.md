# RL 内存泄漏排查笔记

本文记录一次 64 卡 RL 任务的 head 节点内存持续增长排查过程。目标不是只复盘单次事故，而是沉淀一套可复用的 Linux/Ray 内存排查方法。

## 背景

任务启动后，通过 rjob 监控页面观察到一个明显现象：只有 head 节点内存会随着运行时间慢慢增加，其余 worker 节点没有同样趋势。

这个现象很重要。它说明问题大概率不是所有 rollout worker 或训练 worker 同步泄漏，而是某个只存在于 head 的组件在持续持有内存，例如 driver、replay buffer、Ray head 侧 actor、调度/聚合逻辑等。因此排查时可以先聚焦 head 节点，再用 Ray 的 cluster 视角确认对象来源和引用关系。

本次保留的最小证据集放在当前目录的 `mem_debug/` 下，便于后续分享时不依赖完整实验目录。

## 总结论

这次内存增长不是单纯的 RSS 虚高，也不是 rollout 并发瞬时抬高后稳定。最终判断是：

`sampler.py:put_to_ray` 把多模态输入 `pixel_values` 放入 Ray object store 后，部分样本组被业务过滤成 `FILTERED`，但这些 `FILTERED` group 仍被写入 head 侧 replay buffer。训练只消费 `COMPLETED` group，`FILTERED` group 不会被消费，因此 replay buffer 长期保留了包含 Ray ObjectRef 的 `RolloutState`，导致 head driver 私有内存和 Ray object 引用持续增长。

核心代码链路：

- `xtuner/v1/rl/agent_loop_manager/sampler.py`：`put_to_ray` 中 `data.mm_info["pixel_values"] = ray.put(pixel_values)`
- `xtuner/v1/rl/agent_loop_manager/produce_utils.py`：无效 group 被标成 `Status.FILTERED` 后仍然 `replay_buffer.put`
- `xtuner/v1/rl/replay_buffer.py`：`ReplayBuffer.put` 对 `FILTERED` 不做清理或丢弃
- `xtuner/v1/rl/replay_buffer.py`：`take_batch` 默认只消费 `COMPLETED`

所以 `ray.put` 本身不是错误。真正的问题是创建出来的 ObjectRef 被不可消费的 `FILTERED` group 长期持有。

## 监控方案

通用监控脚本保存在 `.dev_scripts/ray_debug_snapshot.sh`。本文证据目录中也保留了一份副本：`mem_debug/debug.sh`。每次执行会生成一个 `mem_YYYYMMDD_HHMMSS/` 快照目录，主要采集：

- `ray status`
- `ray summary objects`
- `ray memory --stats-only`
- `ray summary tasks`
- `ray summary actors`
- `ray list tasks --detail`
- `ray list actors --detail`
- 可选：`ray list objects --detail`
- 当前节点 RSS 最大的前 N 个进程
- 这些进程的 `/proc/${pid}/smaps_rollup`

核心逻辑可以简化成：

```bash
ray status > ray_status.txt
ray summary objects > summary_objects.txt
ray memory --stats-only > memory.txt
ray summary tasks > summary_tasks.txt
ray summary actors > summary_actors.txt

ray list tasks --detail --format=json --limit "${RAY_STATE_LIST_LIMIT}" > tasks.json
ray list actors --detail --format=json --limit "${RAY_STATE_LIST_LIMIT}" > actors.json

if [[ "${INCLUDE_OBJECT_DETAILS}" == "1" ]]; then
  ray list objects --detail --format=json --limit "${RAY_STATE_LIST_LIMIT}" > objects.json
fi

ps -eo pid,ppid,rss,vsz,comm,args --sort=-rss > top_rss.txt

awk -v n="${SMAPS_TOP_N}" '$1 ~ /^[0-9]+$/ && count < n { print $1; count++ }' \
  top_rss.txt > smaps_pids.txt

while read -r pid; do
  cp "/proc/${pid}/smaps_rollup" "smaps_${pid}.txt"
done < smaps_pids.txt
```

启动方式示例：

```bash
tmux new -s mem_debug # 先在 head 节点启动 tmux

# 内部运行额外监控脚本
while true; do
  WORK_DIR=/path/to/work_dir \
  OUT_ROOT=/path/to/work_dir \
  SMAPS_TOP_N=30 \
  INCLUDE_OBJECT_DETAILS=0 \
  RAY_STATE_LIST_LIMIT=100 \
  bash .dev_scripts/ray_debug_snapshot.sh
  sleep 180
done
```

说明：

- 如果问题只出现在 head 节点，脚本先跑在 head 节点即可。
- `top_rss.txt` 和 `smaps_*.txt` 是节点本地视角。
- `ray status`、`ray memory`、`ray summary objects` 是 Ray cluster 视角。
- `summary_tasks.txt` / `summary_actors.txt` 用于快速看 Ray task/actor 是否有排队或异常状态。
- `tasks.json` / `actors.json` 默认只保留 100 条，避免监控文件过大；需要深挖排队时可以外部设置 `RAY_STATE_LIST_LIMIT=100000`。
- `INCLUDE_OBJECT_DETAILS` 默认是 0，不采集 `objects.json`；需要按 `ip / pid / call_site` 聚合 Ray object 明细时，再显式设置为 1。

注意：如果要看到 ray 调用栈，需要在启动训练前，这个非常关键。

```shellscript
export RAY_record_ref_creation_sites=1
```

## 核心证据

### 1. head driver 的私有内存持续增长

仅看 `top` 的 RSS 不够，因为 Ray/PyTorch/CUDA 场景里 RSS 可能包含大量共享内存映射。更可靠的是看 `/proc/${pid}/smaps_rollup` 里的 `Private_Dirty`。

本次 driver PID 是 3135。早期和后期快照：

- `mem_debug/smaps_3135_20260614_122337.txt`
- `mem_debug/smaps_3135_20260615_020037.txt`

关键变化：

```text
2026-06-14 12:23  Private_Dirty:      895016 kB 约 0.9 GiB
2026-06-15 02:00  Private_Dirty:   103363904 kB 约 98.6 GiB
```

这说明 head driver 自身确实在持续持有私有内存，不只是 RSS 因共享内存映射而显得很大。

### 2. Ray object store 同时有大量对象堆积

`mem_debug/memory_20260615_020405.txt`：

```text
Plasma memory usage 204631 MiB, 249043 objects, 13.41% full, 7.99% needed
```

含义：

- `204631 MiB`：整个 Ray cluster 的 plasma object store 实际占用约 200 GiB
- `249043 objects`：Ray object 数量已经达到 24.9 万
- `13.41% full`：object store 总容量使用率
- `7.99% needed`：Ray 认为仍被引用、不能随便释放的对象占比

这说明 Ray object 层面也存在大量对象积累。单看这行还不能定位是谁持有，需要继续看 `ray summary objects`。

### 3. `sampler.py:put_to_ray` 是关键 object 来源

`mem_debug/summary_objects_20260615_013339.txt` 中已经出现关键 callsite：

```text
(put object)
| .../xtuner/v1/rl/agent_loop_manager/sampler.py:put_to_ray:70

LOCAL_REFERENCE: 1307 # 1307 个是因为本地 Python 引用还在而存活
USED_BY_PENDING_TASK: 44 # 44 个是因为 pending task 还要用而存活
TOTAL_NUM_NODES: 1 # 单节点引用
TOTAL_NUM_WORKERS: 1
TOTAL_OBJECTS: 1351 #s ampler.py:put_to_ray:70 创建出来了 1351 个 Ray objects
TOTAL_SIZE_MB: 12978.7
```

同一类对象在后续 `mem_debug/summary_objects_20260615_020405.txt` 中增长为：

```text
sampler.py:put_to_ray:70
LOCAL_REFERENCE: 3916
TOTAL_NUM_NODES: 1
TOTAL_NUM_WORKERS: 1
TOTAL_OBJECTS: 3916
TOTAL_SIZE_MB: 38027.1
```

这几个字段组合起来非常关键：

- `put object`：对象由 `ray.put()` 创建。
- `sampler.py:put_to_ray:70`：对象创建位置是 sampler 的 `put_to_ray`。
- `TOTAL_NUM_NODES=1`、`TOTAL_NUM_WORKERS=1`：对象集中在一个节点、一个 worker/driver，而不是 64 个 worker 分散持有。
- `LOCAL_REFERENCE` 很多：本地 Python 进程仍持有 ObjectRef，所以 Ray 不能释放。
- 总大小从约 12.7 GiB 增长到约 37.1 GiB：不是一次性瞬时峰值，而是在持续积累。

### 4. replay buffer 中 `FILTERED` group 单调增长

训练日志中 `leftover_filtered` 从 step 1 到 step 24 持续增长：

```text
step 1   leftover_filtered=1728
step 10  leftover_filtered=15814
step 20  leftover_filtered=34199
step 24  leftover_filtered=42314
```

这条线和 Ray object 增长、driver `Private_Dirty` 增长能对上。`FILTERED` group 是不可训练消费的，但它仍留在 replay buffer 里，成为长期持有 ObjectRef 的容器。

## Linux 进程内存怎么看

### top 里的 VIRT / RES / SHR

`top` 或 `ps` 常见字段：

- `VIRT`：虚拟地址空间总量。包括预留地址、mmap 文件、CUDA 映射、Ray plasma 映射等，不等价于真实物理内存占用。排查泄漏时通常不是主指标。
- `RES`：Resident memory，基本对应 RSS，表示当前驻留在物理内存里的页面总量。包含共享页，所以可能虚高。
- `SHR`：RSS 中可共享的部分，例如动态库、共享内存、Ray plasma mmap 等。

粗略关系：

```text
RES ≈ RSS
RES - SHR ≈ 进程独占内存的粗略估计
```

但 `RES - SHR` 只是近似，最终确认建议看 `smaps_rollup`。

### /proc/${pid}/smaps_rollup

`/proc/${pid}/smaps_rollup` 是 Linux 对单个进程所有内存映射的汇总。普通 `smaps` 会列出每一段映射，`smaps_rollup` 则给总览，更适合快速排查。

常用命令：

```bash
cat /proc/${pid}/smaps_rollup
egrep '^(Rss|Pss|Shared|Private|Anonymous|Swap):' /proc/${pid}/smaps_rollup
```

核心指标：

- `Rss`：进程驻留物理内存总量，基本对应 `top RES`。包含共享页，不能单独判断真实独占内存。
- `Pss`：Proportional Set Size，按比例分摊共享页后的内存。分摊规则是：私有页全部算给当前进程；共享页按映射它的进程数量平均分。比如一块 100 GiB 的 Ray plasma 共享内存同时被 10 个进程映射，那么每个进程的 RSS 都可能包含这 100 GiB，但每个进程的 PSS 只分到约 10 GiB。PSS 因此更适合估算“这个进程对系统总内存压力的公平占比”。
- `Shared_Clean / Shared_Dirty`：共享内存部分。Ray plasma、mmap、动态库等会体现在这里。
- `Private_Clean / Private_Dirty`：进程私有部分。排查泄漏时尤其关注 `Private_Dirty`。
- `Anonymous`：匿名内存，通常来自 heap、Python 对象、tensor、内存池等。
- `Swap`：被换出到 swap 的内存。非 0 说明系统内存压力已经比较大。

实战判断：

```text
Rss 高，Shared 高，Private_Dirty 不高：
  多半是共享内存映射导致 RSS 看起来大。

Rss 高，Pss 也高：
  这个进程对系统真实内存压力也高。

Private_Dirty 持续上涨：
  高度怀疑进程自己在持续持有对象、缓存或引用。

Anonymous 持续上涨：
  倾向于进程 heap、Python 对象、tensor 或内存池增长。
```

本次正是因为 driver 的 `Private_Dirty` 从约 0.9 GiB 涨到约 98.6 GiB，才确认 head driver 不是单纯 RSS 假胖。

## Ray 内存怎么看

### ray status

`mem_debug/ray_status_20260615_020405.txt`：

```text
Active: 8 nodes
Pending: (no pending nodes)
Recent failures: (no failures)

119.06GiB/1.46TiB object_store_memory # 这个值可能不太准，主要知道这个 1.46T 咋来的，是全局的
```

这个命令主要看 Ray 集群健康状态和资源是否接近耗尽：

- 8 个节点都 active
- 没有 pending node
- 没有 recent failure
- 没有 pending resource demand

这里的 `object_store_memory` 是 Ray resource accounting 口径，和 `ray memory` 的 plasma full 口径不完全一样。这个快照里：

```text
ray status: 119.06GiB / 1.46TiB object_store_memory # 注意这个 119G 是啥意思
ray memory: 204631 MiB, 13.41% full, 7.99% needed # 这个是后面 log 内容
```

`119.06GiB` 更接近 `1.46TiB * 7.99%`，也就是 `needed` 部分，而不是 plasma 总占用。

### Ray object store 容量

Ray 每个节点会有自己的 object store。没有显式设置时，Ray 会自动计算：

```text
object_store_memory = available_memory * 0.3
然后受默认上限和 /dev/shm 限制
```

本次 Ray 版本的默认单节点上限是：

```text
DEFAULT_OBJECT_STORE_MAX_MEMORY_BYTES = 200000000000  # 200 GB
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.3
```

所以 8 节点集群总 object store 容量约为：

```text
200 GB * 8 ≈ 1.46 TiB
```

注意：这个容量只是 Ray object store，不包括 Python heap、PyTorch CPU 内存、driver 私有内存、raylet 自身内存等。

### ray memory --stats-only

`ray memory --stats-only` 用来快速看 Ray object store 总体情况：

```text
Plasma memory usage 204631 MiB, 249043 objects, 13.41% full, 7.99% needed
```

排查时关注两个趋势：

- `Plasma memory usage` 是否持续上涨
- `objects` 数量是否持续上涨

如果只是并发高，plasma 可能升高后回落。如果对象数随着 step 持续增长，通常说明有 ObjectRef 或对象生命周期没有释放。

### ray summary objects

`ray summary objects` 是这次定位的关键命令。它按 callsite 汇总当前 Ray ObjectRef。

核心字段：

- `TOTAL_OBJECTS`：这一组 object 数量
- `TOTAL_SIZE_MB`：这一组 object 总大小
- `TOTAL_NUM_NODES`：分布在几个节点
- `TOTAL_NUM_WORKERS`：分布在几个 worker/driver
- `REF_TYPE_COUNTS`：对象为什么还活着
- `TASK_STATE_COUNTS`：相关 task 状态

常见 `REF_TYPE_COUNTS`：

- `LOCAL_REFERENCE`：某个 Python 进程本地变量或对象里还持有 ObjectRef。
- `PINNED_IN_MEMORY`：对象正在被使用、反序列化或作为 task 参数使用，不能释放。
- `USED_BY_PENDING_TASK`：对象被 pending task 使用。
- `CAPTURED_IN_OBJECT`：ObjectRef 被嵌套存进另一个 Ray object 里。

判断思路：

```text
TOTAL_NUM_NODES=8, TOTAL_NUM_WORKERS≈64：
  更像 rollout 并发阶段的分布式运行中对象。

TOTAL_NUM_NODES=1, TOTAL_NUM_WORKERS=1, LOCAL_REFERENCE 很多：
  更像某个单点进程长期持有引用，尤其可疑。
```

例如 `LMDeployWorker.generate`：

```text
(deserialize actor task arg) xtuner.v1.rl.rollout.lmdeploy.LMDeployWorker.generate
LOCAL_REFERENCE: 1275
PINNED_IN_MEMORY: 250
TOTAL_NUM_NODES: 8
TOTAL_NUM_WORKERS: 63
TOTAL_OBJECTS: 1525
TOTAL_SIZE_MB: 5445.11
```

这是全 cluster 聚合，不是某个单节点。它分布在 8 个节点、63 个 worker 上，符合 rollout 并发期间的正常形态，因此不是首要泄漏嫌疑。

对比 `sampler.py:put_to_ray:70`：

```text
LOCAL_REFERENCE: 3916
TOTAL_NUM_NODES: 1
TOTAL_NUM_WORKERS: 1
TOTAL_OBJECTS: 3916
TOTAL_SIZE_MB: 38027.1
```

这个集中在单节点单 worker，并且随时间增长，才是更强的泄漏线索。

### callsite_enabled=false 的正确理解

`ray summary objects` 顶部可能显示：

```text
callsite_enabled: false
```

不能简单理解为“callsite 完全没生效”。Ray summary 是聚合结果，只要返回对象里存在某些 `call_site=disabled` 的对象，整体就可能显示 false。

正确读法：

- 如果关键大对象 group 有明确 callsite，例如 `sampler.py:put_to_ray:70`，说明这部分对象来源已经记录到了。
- 如果 `disabled` group 很小，可以忽略。
- 如果 `disabled` group 很大，说明仍有重要对象来源不清，需要补充环境变量或更细粒度监控。

本次 `summary_objects_20260615_013339.txt` 中虽然 `callsite_enabled=false`，但 `sampler.py:put_to_ray:70` 已经清楚记录了约 12.7 GiB / 1351 objects，因此不影响使用这条证据。

## 本次分析过程

1. 先用 rjob 判断问题只发生在 head 节点。
2. 用 `top_rss.txt` 找到 head 上 RSS 最大的进程，driver PID 3135 排第一。
3. 用 `smaps_rollup` 确认 driver `Private_Dirty` 持续上涨，排除单纯共享内存 RSS 虚高。
4. 用 `ray memory --stats-only` 确认 Ray object store 对象数和 plasma 使用量也在增长。
5. 用 `ray summary objects` 找到重要 object 来源：`sampler.py:put_to_ray:70`。
6. 观察该 callsite 是 `TOTAL_NUM_NODES=1`、`TOTAL_NUM_WORKERS=1`、大量 `LOCAL_REFERENCE`，说明不是 64 个 worker 均匀持有，而是单点长期持有。
7. 对齐训练日志，发现 `leftover_filtered` 随 step 单调增长。
8. 阅读代码，确认 `FILTERED` group 仍进入 replay buffer，且不会被训练消费。

## 修复方向

优先修复方向：

```text
FILTERED group 不应完整进入 replay buffer。
```

可选策略：

1. 无效 group 只记录统计信息，不保存完整 `RolloutState`。
2. 如果必须保存状态，只保存轻量 metadata，不保留 `mm_info.pixel_values`、response、logprobs、routed_experts 等重字段。
3. 在 `ReplayBuffer.put` 或 `produce_utils.put_generated_group` 中对 `Status.FILTERED` 直接 drop。
4. 增加 replay buffer 清理逻辑，定期删除不可消费状态，例如 `FILTERED` / `FAILED`。

最直接的验证标准：

- `leftover_filtered` 不再随 step 单调增长，或增长后会被清理回落。
- `sampler.py:put_to_ray:70` 的 `TOTAL_OBJECTS` / `TOTAL_SIZE_MB` 不再线性增长。
- head driver `Private_Dirty` 在前几个 step 后趋于稳定。
- Ray object count 不再随训练 step 单调增长。

## 下次实验建议

启动前建议继续设置：

```bash
export RAY_record_ref_creation_sites=1
```

如果使用 `ray job submit --runtime-env-json`，也可以在 `env_vars` 中显式加一份：

```json
"RAY_record_ref_creation_sites": "1"
```

这不是因为本次一定没传进去，而是为了减少不同进程环境继承的不确定性，让 `ray summary objects` 更干净。

监控频率建议：

- 初始复现阶段：60s 可以接受
- 长时间实验：120s 或 180s 更合适
- 如果只看趋势，不需要 `ray list objects --detail` 每次都跑，保持默认 `INCLUDE_OBJECT_DETAILS=0` 即可
- `RAY_STATE_LIST_LIMIT` 默认 100，适合长期监控；如果要分析 Ray 内部排队，可以临时调大到 10000 或 100000。

## 证据文件

当前文档引用的最小证据集：

```text
mem_debug/debug.sh
mem_debug/top_rss_20260615_020037.txt
mem_debug/smaps_3135_20260614_122337.txt
mem_debug/smaps_3135_20260615_020037.txt
mem_debug/ray_status_20260615_020405.txt
mem_debug/memory_20260615_020405.txt
mem_debug/summary_objects_20260615_013339.txt
mem_debug/summary_objects_20260615_020405.txt
```
