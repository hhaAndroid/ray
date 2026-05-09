# Ray Actor 调度策略与 Placement Group 资源约束机制说明

## 1. 文档目标与讨论范围

本文用于说明 Ray 中 actor 创建时的节点调度流程，以及 `scheduling_strategy`、Placement Group、bundle 资源约束之间的关系。

重点讨论：

```text
1. actor 创建时，GCS、raylet、WorkerPool 分别负责什么。
2. Ray 暴露给用户的 scheduling_strategy 有哪些。
3. Placement Group 的 placement strategy 与 actor/task 的 scheduling_strategy 有什么区别。
4. actor 使用 PlacementGroupSchedulingStrategy 时，资源请求如何被 PG 限制。
5. 典型 actor 调度场景如何从源码视角理解。
```

本文主要讨论 actor creation 的集群级调度。actor 创建完成后，actor method 调用不会重新做节点选择。

## 2. Actor 创建调度主流程

从调度视角看，Ray actor 的生命周期可以先分成两个阶段：

```text
A.remote()
  |
  v
actor creation task 调度阶段
  - GCS 注册 actor 元数据
  - GCS 选 initial forwarding raylet
  - raylet 根据资源和 scheduling_strategy 选节点
  - 这里会涉及 DEFAULT/top-k、SPREAD、NodeAffinity、NodeLabel、PG 资源约束
  - 目标 raylet lease worker
  - worker 上启动 actor
  |
  v
actor 已经存在
  - 后续 a.f.remote() 不再做节点级调度
  - 请求直接发送到这个 actor 所在 worker
  - actor worker 内部再按同步/异步、concurrency group 等规则执行 method
```

因此，本文讨论的 scheduling strategy、PG bundle 约束、top-k、spillback 等内容，主要发生在 **actor creation task 调度阶段**。

actor 创建完成之后，后续 actor method 调用不会再重新选择节点。它们只是在已经确定的 actor worker 上排队或并发执行。

## 3. 核心概念

### 3.1 Actor creation task

创建 actor 时，Ray 内部会生成一个 actor creation task。这个 task 的目标不是执行普通函数，而是 lease 一个 worker，然后在这个 worker 上启动 actor。

后续 actor method 调用不再重新做集群级节点调度。actor 已经固定在哪个 worker 上，method 调用会直接发给这个 actor worker。

### 3.2 GCS

GCS 管 actor 元数据和 actor 生命周期。创建 actor 会先注册到 GCS。

但 GCS actor scheduler 不是复杂资源调度器。它只选一个 initial forwarding raylet：

```text
有资源需求：优先 owner node，否则随机 alive node
无资源需求：随机 alive node
```

源码：`src/ray/gcs/actor/gcs_actor_scheduler.cc:83`

### 3.3 Initial forwarding raylet 不等于最终运行节点

GCS actor scheduler 在 actor 有资源需求时，会优先选择 owner node 作为 initial forwarding raylet：

```cpp
if (!lease_spec.GetRequiredResources().IsEmpty()) {
  auto maybe_node = gcs_node_manager_.GetAliveNode(actor->GetOwnerNodeID());
  node = maybe_node.has_value() ? maybe_node.value()
                                : gcs_node_manager_.SelectRandomAliveNode();
}
```

这里的 owner node 不是固定 head node，而是创建 actor 的 owner 所在节点：

```text
driver 跑在 head 上:
  owner node = head
  initial forwarding raylet 优先 head raylet

driver 跑在 worker node 上:
  owner node = 这个 worker node
  initial forwarding raylet 优先这个 worker node

某个 actor 内部创建子 actor:
  owner node = 父 actor 所在节点
  initial forwarding raylet 优先父 actor 所在节点
```

因此，如果 driver 运行在 head 节点上，GCS 确实会优先把 actor creation lease request 发给 head raylet。但这不表示 actor 一定会在 head 节点启动。

initial forwarding raylet 只是第一个参与调度的入口 raylet。它会基于 cluster resource view 做节点选择。如果它判断 actor 更适合运行在其他节点，会在 reply 中返回 `retry_at_raylet_address`，让请求 spillback 到目标 raylet。

```text
driver 在 head 上
  -> GCS 优先选择 head raylet 作为 forwarding raylet
  -> head raylet 根据资源和调度策略选择最终节点
  -> 如果最终节点是 worker node，则请求 spillback 到 worker node raylet
  -> worker node raylet lease worker 并启动 actor
```

所以需要区分：

```text
initial forwarding raylet:
  第一个被请求参与调度的 raylet。

final actor node:
  actor 最终真正启动的节点。
```

如果 head 节点声明了大量 CPU/GPU，并且调度策略选择 head，那么 actor 可能在 head 上启动。如果 head 不声明对应资源，或者 PG bundle / GPU / custom resource 在其他节点，actor 会被调度到对应节点。

### 3.4 Raylet

Raylet 是每个节点上的调度和执行管理进程。

Raylet 不是只看本机资源。它本地维护一份 cluster resource view，也就是对其他节点资源状态的缓存。真正的 task / actor creation 节点选择主要发生在 raylet 的 lease scheduling 路径里。

源码入口：

- `src/ray/raylet/scheduling/cluster_lease_manager.cc:214`
- `src/ray/raylet/scheduling/cluster_resource_scheduler.cc:155`

### 3.5 WorkerPool

Raylet 选中本机后，会通过 WorkerPool 找一个 idle worker 或启动新 worker。

所以 actor 创建可以理解为：

```text
调度选节点
  -> 目标 raylet lease worker
  -> WorkerPool 提供 worker
  -> actor 在这个 worker 上启动
```

### 3.6 Feasible 与 Available

调度路径中经常出现两个判断维度：

```text
feasible:
  节点总资源能不能满足请求。
  例如节点总共 8 GPU，请求 4 GPU，则 feasible。

available:
  节点当前空闲资源能不能满足请求。
  例如节点总共 8 GPU，但现在只剩 2 GPU，请求 4 GPU，则 feasible 但 not available。
```

调度策略通常先找 feasible 节点，再优先 available 节点。

## 4. 两类 Strategy 的概念边界

### 4.1 Actor/task 的 scheduling_strategy

这是 `ray.remote` / `.options()` 里的 actor/task 调度策略。

常见值：

```python
scheduling_strategy="DEFAULT"
scheduling_strategy="SPREAD"
scheduling_strategy=PlacementGroupSchedulingStrategy(...)
scheduling_strategy=NodeAffinitySchedulingStrategy(...)
scheduling_strategy=NodeLabelSchedulingStrategy(...)
```

源码类型定义：`python/ray/util/scheduling_strategies.py:223`

注意：现代 API 中，`scheduling_strategy` 是一个字段，通常是单选语义：

```text
PlacementGroupSchedulingStrategy(...) 和 "SPREAD" 不是两个同时叠加的用户参数。
```

如果 actor/task 使用了 `PlacementGroupSchedulingStrategy`，它的用户层 scheduling_strategy 就是 PG strategy，不是同时又是 `"SPREAD"`。

### 4.2 Placement Group 的 placement strategy

这是创建 PG 时的 bundle 放置策略：

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}],
    strategy="STRICT_SPREAD",
)
```

PG placement strategy 决定的是 bundle 如何预留资源：

```text
PACK:
  尽量把 bundles 放到一起。

SPREAD:
  尽量把 bundles 分散。

STRICT_PACK:
  所有 bundles 必须放在同一个节点。

STRICT_SPREAD:
  每个 bundle 必须放在不同节点。
```

文档：`doc/source/ray-core/scheduling/placement-group.rst:431`

内部对应：

```text
PACK          -> BUNDLE_PACK
SPREAD        -> BUNDLE_SPREAD
STRICT_PACK   -> BUNDLE_STRICT_PACK
STRICT_SPREAD -> BUNDLE_STRICT_SPREAD
```

源码：`src/ray/gcs/gcs_placement_group_scheduler.cc:498`

## 5. Raylet 内部调度策略

从单个 actor/task 的节点选择角度看，Raylet 内部最复杂的是 `DEFAULT` 对应的 `HYBRID` 策略。它需要综合 feasible / available、资源利用率打分、top-k、preferred node、locality、GPU node avoidance、spillback 和 stale resource view 等因素。

其他 actor/task scheduling strategy 通常更偏向明确规则或约束过滤：

```text
SPREAD:
  round-robin 式分散。

NodeAffinity:
  优先或强制目标 node。

NodeLabel:
  根据 hard / soft label 过滤候选节点。

RANDOM:
  随机选择可行可用节点，主要用于 zero-resource actor。

PlacementGroupSchedulingStrategy:
  主要通过 PG 专属资源限制候选节点。
```

但需要注意，Placement Group 创建阶段本身是另一套 bundle 级调度。`PACK`、`SPREAD`、`STRICT_PACK`、`STRICT_SPREAD` 需要处理多个 bundles 的整体放置，不应和单个 actor/task 的节点选择复杂度混为一谈。

### 5.1 HYBRID，也就是 DEFAULT

用户层 `"DEFAULT"` 进入 raylet 后主要对应内部 `HYBRID`。

基础流程：

```text
1. 遍历 cluster resource view 里的节点。
2. 找 feasible 且 available 的节点。
3. 根据资源利用率打分。
4. 选 top-k 低分节点。
5. 在 top-k 中随机选一个。
```

文档：`doc/source/ray-core/scheduling/index.rst:51`

源码：`src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc:89`

#### 5.1.1 DEFAULT/HYBRID 的两个目标

DEFAULT/HYBRID 不是单纯选择“当前最空闲的节点”。它主要在两个目标之间做折中：

```text
局部性优先:
  如果一个节点已经是 preferred node，例如本地节点、owner 节点，或者上层调度路径根据 locality 选择的节点，
  Ray 会尽量优先使用它。这样可以减少跨节点拉取对象、启动环境、调度转发等开销。

资源敏感:
  Ray 同时会检查节点资源是否 feasible / available，并根据节点资源利用率进行打分。
```

在源码里，preferred node 的优先级不是无条件最高。`GetBestNode()` 只有在 preferred node 的分数不高于当前最低分节点时，才直接返回 preferred node：

```cpp
if (preferred_node_id.has_value()) {
  if (preferred_node_score <= node_scores.front().second) {
    return preferred_node_id.value();
  }
}
```

源码：`src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc:71`

这意味着 DEFAULT/HYBRID 的倾向是：

```text
如果 preferred node 足够好，就优先用 preferred node。
如果 preferred node 明显更忙，就转向其他更合适的节点。
```

#### 5.1.2 阈值机制：scheduler_spread_threshold

HYBRID 会为节点计算一个 score。源码入口是 `ComputeNodeScoreImpl()`：

```cpp
float critical_resource_utilization =
    node_resources.CalculateCriticalResourceUtilization();
if (critical_resource_utilization < spread_threshold) {
  critical_resource_utilization = 0;
}
return critical_resource_utilization;
```

源码：`src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc:44`

默认阈值来自：

```cpp
RAY_CONFIG(float, scheduler_spread_threshold, 0.5)
```

源码：`src/ray/common/ray_config_def.h:223`

因此默认情况下：

```text
节点利用率 < 0.5:
  score = 0。
  在调度器看来，10% 和 40% 利用率的节点都属于足够空闲。

节点利用率 >= 0.5:
  score = 实际 critical resource utilization。
  此时 60% 利用率节点会比 80% 利用率节点更优。
```

这个阈值用于在 locality 和负载均衡之间折中。配置注释里也明确说明：低阈值会鼓励更强的 load spreading；默认 0.5 则避免调度器为了 10% 和 11% 这类细微差异频繁跨节点切换。

需要注意，源码里的 `CalculateCriticalResourceUtilization()` 当前主要计算 CPU、memory、object store memory 的最高利用率：

```cpp
for (const auto &i : {CPU, MEM, OBJECT_STORE_MEM}) {
  ...
  float utilization = 1 - (cur_available / cur_total.Double());
  highest = max(highest, utilization);
}
```

源码：`src/ray/common/scheduling/cluster_resource_data.cc:62`

GPU、自定义资源等仍然会参与 feasible / available 判断，但这里的 score 不是简单地对所有资源做平均利用率。

#### 5.1.3 Top-k 随机选择

HYBRID 不会永远选择排序后的第 1 个节点。它会先按 score 排序，然后从 top-k 个低分节点中随机选一个：

```cpp
size_t node_index = absl::Uniform<size_t>(
    bitgenref_, 0u, std::min(num_candidate_nodes, node_scores.size()));
return node_scores[node_index].first;
```

源码：`src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc:77`

`k` 的计算方式：

```cpp
size_t num_candidate_nodes =
    std::max<int32_t>(
        schedule_top_k_absolute,
        static_cast<int32_t>(nodes_.size() * scheduler_top_k_fraction));
```

源码：`src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc:156`

默认配置：

```text
scheduler_top_k_fraction = 0.2
scheduler_top_k_absolute = 1
```

源码：`src/ray/common/ray_config_def.h:229`

也就是说，默认情况下 Ray 会从约前 20% 的低分节点中随机选择，并保证 k 至少为 1。

这样做有两个直接效果：

```text
避免热点:
  如果所有任务都选择唯一排名第 1 的节点，大量并发调度请求可能把该节点打满。

缓解冷启动排队:
  从 top-k 中随机选择，可以让多个节点并行启动 worker / runtime env，
  而不是所有请求都集中在同一个节点上排队。
```

#### 5.1.4 available 优先，其次才是 feasible-but-unavailable

HYBRID 会先尝试从 available nodes 中选择：

```cpp
if (!available_nodes.empty()) {
  return GetBestNode(available_nodes, ...);
}
```

只有当没有 available node，并且调用方允许 `require_node_available=false` 时，才会考虑 feasible but unavailable nodes：

```cpp
else if (!feasible_and_unavailable_nodes.empty() && !require_node_available) {
  return GetBestNode(feasible_and_unavailable_nodes, ...);
}
```

源码：`src/ray/raylet/scheduling/policy/hybrid_scheduling_policy.cc:162`

所以 DEFAULT/HYBRID 可以概括为：

```text
先过滤可行节点；
优先选择当前可用节点；
对节点按 critical resource utilization 打分；
利用 threshold 避免过度追求细粒度负载均衡；
从 top-k 低分节点随机选；
如果 preferred node 足够好，则优先 preferred node。
```

### 5.2 SPREAD

用户层 `"SPREAD"` 进入 raylet 后对应内部 `SPREAD`。

它会在节点之间 round-robin 式分散，优先 available 节点。

源码：`src/ray/raylet/scheduling/policy/spread_scheduling_policy.cc:25`

### 5.3 RANDOM

这是内部特殊策略，常见于 zero-resource actor。

例如 actor creation 资源请求为空，并且不是 hard node affinity，Ray 会随机选择节点。

对于不消耗任何资源的 actor，也就是 `num_cpus=0` 且没有其他资源需求的 actor，Ray 会进行特殊处理：在集群中随机选择一个节点，而不考虑资源利用率。由于节点是随机选择的，这类 zero-resource actor 在效果上会被打散到整个集群中。

文档中也明确说明：

```text
Currently Ray handles actors that don't require any resources
(i.e., num_cpus=0 with no other resources) specially by randomly choosing
a node in the cluster without considering resource utilization.
Since nodes are randomly chosen, actors that don't require any resources
are effectively SPREAD across the cluster.
```

对应源码分支：

```cpp
if (actor_creation && resource_request.IsEmpty() &&
    !IsHardNodeAffinitySchedulingStrategy(scheduling_strategy)) {
  return scheduling_policy_->Schedule(resource_request, SchedulingOptions::Random());
}
```

源码：`src/ray/raylet/scheduling/cluster_resource_scheduler.cc:163`

### 5.4 NODE_AFFINITY

对应 `NodeAffinitySchedulingStrategy`。

它指定目标 node：

```text
soft=False:
  目标节点不存在或 infeasible，就失败。

soft=True:
  目标节点不存在或 infeasible，可以 fallback 到 HYBRID。
```

源码：`src/ray/raylet/scheduling/policy/node_affinity_scheduling_policy.cc:20`

### 5.5 NODE_LABEL

对应 `NodeLabelSchedulingStrategy`。

它先按 hard label 过滤，再按 soft label 优先，最后从候选节点随机选。

源码：`src/ray/raylet/scheduling/policy/node_label_scheduling_policy.cc:22`

### 5.6 AFFINITY_WITH_BUNDLE

这是 PG 相关的内部特殊路径。

它做的事很直接：

```text
已知 pg_id + bundle_index
  -> 查 BundleLocationIndex
  -> 找到这个 bundle 所在 node
  -> 检查该节点资源是否可用
```

源码：`src/ray/raylet/scheduling/policy/affinity_with_bundle_scheduling_policy.cc:46`

但它不是理解 PG actor 调度的主线。源码里也写了：

```cpp
// This scheduling strategy is only used for gcs scheduling for the time being.
```

源码：`src/ray/raylet/scheduling/cluster_resource_scheduler.cc:192`

## 6. Placement Group 对 Actor 调度的约束方式

### 6.1 PG 创建阶段：预留 bundle 资源

创建 PG 时，Ray 调度的是 bundle：

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}],
    strategy="STRICT_SPREAD",
)
```

假设调度结果：

```text
bundle 0 -> node A
bundle 1 -> node B
```

此时 node A / node B 上会出现 PG 专属资源。例如：

```text
GPU_group_<pg_id>
GPU_group_0_<pg_id>
bundle_group_<pg_id>
bundle_group_0_<pg_id>
```

这些资源名由 Ray 生成。

源码：`src/ray/common/bundle_spec.cc:160`

### 6.2 Actor 使用 PG 阶段：消费 PG 专属资源

创建 actor：

```python
A.options(
    num_gpus=1,
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg,
        placement_group_bundle_index=0,
    ),
).remote()
```

这个 actor 请求的不是普通：

```text
GPU: 1
```

而是类似：

```text
GPU_group_<pg_id>: 1
GPU_group_0_<pg_id>: 1
bundle_group_<pg_id>: 0.001
bundle_group_0_<pg_id>: 0.001
```

所以它只能跑到 bundle 0 所在节点。普通节点没有这些 PG 专属资源，直接 not feasible。

### 6.3 PG 不是额外的负载均衡策略

PG 更像是先缩小可选节点集合：

```text
不用 PG:
  候选节点 = 所有满足 CPU/GPU/custom resource 的节点。

使用 PG + bundle_index=0:
  候选节点 = bundle 0 所在节点。

使用 PG + bundle_index=-1:
  候选节点 = 这个 PG 的任意可用 bundle 所在节点。
```

然后 Ray 再根据内部机制选节点。

但注意：用户层并不是同时设置：

```python
scheduling_strategy=PlacementGroupSchedulingStrategy(...)
scheduling_strategy="SPREAD"
```

这两个不是可叠加参数。PG 的 `strategy="SPREAD"` 是 PG bundle 放置策略，不是 actor/task 的 `"SPREAD"` scheduling_strategy。

## 7. Actor 创建调度全流程

### 7.1 不使用 PG 的 actor

代码：

```python
@ray.remote(num_gpus=1)
class A:
    pass

a = A.remote()
```

流程：

```text
1. Python 层整理 actor options。
2. scheduling_strategy 默认为 "DEFAULT"。
3. CoreWorker 创建 actor creation task spec。
4. Actor 注册到 GCS。
5. GCS 选一个 initial forwarding raylet。
6. forwarding raylet 根据 cluster resource view 做 HYBRID 调度。
7. 如果选中本机，进入本机 WorkerPool lease worker。
8. 如果选中远端，返回 retry_at_raylet_address，spillback 到目标 raylet。
9. 目标 raylet 最终确认本地资源，lease worker。
10. actor 在该 worker 上启动。
```

### 7.2 使用 PG 且指定 bundle 的 actor

代码：

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}],
    strategy="STRICT_SPREAD",
)
ray.get(pg.ready())

@ray.remote(num_gpus=1)
class A:
    pass

a0 = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 0),
).remote()
```

假设：

```text
bundle 0 -> node A
bundle 1 -> node B
```

流程：

```text
1. PG 已经先把 bundle 0 放到 node A。
2. node A 拥有 GPU_group_0_<pg_id>。
3. actor 使用 PlacementGroupSchedulingStrategy(pg, 0)。
4. actor 资源请求被改写成 PG 专属资源。
5. GCS 仍然只选 forwarding raylet。
6. raylet 调度时发现只有 node A feasible。
7. 请求最终 spillback 或直接到 node A 的 raylet。
8. node A raylet lease worker。
9. actor 启动在 node A。
```

这里 DEFAULT/top-k 基本没有发挥空间，因为候选节点通常只剩 bundle 0 所在节点。

### 7.3 使用 PG 但 bundle_index=-1 的 actor

代码：

```python
a = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, -1),
).remote()
```

含义：

```text
actor 可以使用这个 PG 里的任意一个合适 bundle。
```

假设：

```text
bundle 0 -> node A
bundle 1 -> node B
```

那么候选范围大致是：

```text
node A 或 node B
```

Ray 会找一个当前可用且满足资源的 bundle。这个时候比指定 bundle 更有调度空间。

### 7.4 PG 的 SPREAD 与 actor 的 SPREAD 的差异

PG 的 SPREAD：

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}],
    strategy="SPREAD",
)
```

含义：

```text
尽量把 PG bundles 分散到不同节点。
```

actor 的 SPREAD：

```python
A.options(scheduling_strategy="SPREAD").remote()
```

含义：

```text
不使用 PG 的情况下，尽量把 actor 分散到不同节点。
```

现代 API 里，如果你写：

```python
A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 0),
).remote()
```

那么 actor/task 的 scheduling_strategy 是 PG strategy，不是 `"SPREAD"`。

#### 7.4.1 PG SPREAD 与 actor SPREAD 的本质区别

两者表面上都是“尽量分散”，但分散的对象和生效阶段不同。

| 对比项 | PG `strategy="SPREAD"` | Actor `scheduling_strategy="SPREAD"` |
|---|---|---|
| 作用对象 | PG 的 bundles | actor / task |
| 发生时间 | 创建 placement group 时 | 创建 actor/task 时 |
| 是否预留资源 | 是，会预留 bundle 资源 | 否，只是调度当前 actor/task |
| 生命周期 | PG 存活期间资源一直被 reserved | actor/task 本身运行期间占用资源 |
| 后续 actor 是否必须使用它 | 只有显式使用 `PlacementGroupSchedulingStrategy(pg, ...)` 才会用 PG 资源 | actor 自己直接被调度 |
| 典型用途 | gang scheduling、资源池预留、多个 actor 成组启动 | 普通 actor/task 尽量分散 |

例如：

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}],
    strategy="SPREAD",
)
ray.get(pg.ready())
```

这一步只是先分散预留资源：

```text
bundle 0 -> node A
bundle 1 -> node B
```

此时还没有启动 actor。后续 actor 必须显式使用这个 PG，才会消费这些已经预留的 bundle：

```python
a0 = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 0),
).remote()

a1 = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 1),
).remote()
```

而 actor SPREAD 是直接调度 actor：

```python
a0 = A.options(scheduling_strategy="SPREAD").remote()
a1 = A.options(scheduling_strategy="SPREAD").remote()
```

这种方式不会提前预留一组资源。Ray 只是创建每个 actor 时，尽量把它们分散到不同节点。

因此可以概括为：

```text
PG SPREAD:
  先把资源坑位分散占好，再让 actor/task 进入这些坑位。

Actor SPREAD:
  不提前占资源，创建 actor/task 时直接尽量分散放置。
```

### 7.5 actor method 不重新调度

这一点在分析 actor 性能和资源占用时需要单独注意。

```python
a = A.remote()
a.f.remote()
a.g.remote()
```

`A.remote()` 需要集群级调度。`a.f.remote()` 和 `a.g.remote()` 不会再选择节点，因为 actor 已经固定在某个 worker 上。

actor method 只是在 actor worker 内部排队、并发执行或按 concurrency group 执行。

## 8. 典型场景示例

### 8.1 默认 actor

```python
@ray.remote(num_gpus=1)
class A:
    pass

actors = [A.remote() for _ in range(4)]
```

理解：

```text
不用 PG。
每个 actor 请求普通 GPU: 1。
Raylet 用 DEFAULT/HYBRID 从所有 GPU 节点里选。
```

### 8.2 不使用 PG 的 SPREAD actor

```python
actors = [
    A.options(scheduling_strategy="SPREAD").remote()
    for _ in range(4)
]
```

理解：

```text
不用 PG。
候选节点是所有满足 GPU: 1 的节点。
SPREAD 尽量把 actor 分散到不同节点。
```

### 8.3 PG STRICT_SPREAD + 指定 bundle

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}],
    strategy="STRICT_SPREAD",
)
ray.get(pg.ready())

a0 = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 0),
).remote()

a1 = A.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(pg, 1),
).remote()
```

假设：

```text
bundle 0 -> node A
bundle 1 -> node B
```

结果：

```text
a0 只能去 node A。
a1 只能去 node B。
```

这里 actor 本身没有再使用 `"SPREAD"`。分散效果来自 PG 的 `STRICT_SPREAD` bundle 放置。

### 8.4 PG SPREAD + bundle_index=-1

```python
pg = placement_group(
    [{"GPU": 1}, {"GPU": 1}, {"GPU": 1}],
    strategy="SPREAD",
)
ray.get(pg.ready())

actors = [
    A.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(pg, -1),
    ).remote()
    for _ in range(3)
]
```

理解：

```text
PG 先尽量把 3 个 bundles 分散。
actor 创建时不指定具体 bundle。
每个 actor 可以使用这个 PG 中任意可用 bundle。
最终 actor 会落到 PG bundle 所在节点，而不是集群任意节点。
```

### 8.5 NodeAffinity 与 PG 的组合限制

这两个都是 actor/task 的 `scheduling_strategy` 类型：

```python
PlacementGroupSchedulingStrategy(...)
NodeAffinitySchedulingStrategy(...)
```

现代 API 里它们不是两个可以同时填写的字段。

如果你既想要“只用某个资源池”，又想要“固定某个节点”，通常需要重新设计资源约束，例如：

```text
使用 PG bundle 固定资源池；
或使用自定义资源 / node label 表达节点约束；
避免把所有约束都表达为单一的 scheduling_strategy。
```

## 9. 总结模型

### 9.1 不使用 PG

```text
actor 资源请求:
  CPU / GPU / custom resources

候选节点:
  所有满足这些资源的节点

调度策略:
  DEFAULT / SPREAD / NodeAffinity / NodeLabel
```

### 9.2 使用 PG

```text
PG 创建:
  根据 PACK / SPREAD / STRICT_PACK / STRICT_SPREAD 放置 bundles

actor 资源请求:
  被改写成 *_group_<pg_id> 资源

候选节点:
  PG bundle 所在节点

调度结果:
  actor 消费某个 bundle 的预留资源并启动
```

### 9.3 GCS 与 raylet 的角色

```text
GCS:
  管 actor 元数据。
  选 initial forwarding raylet。
  不做 DEFAULT/top-k/SPREAD 的主调度。

forwarding raylet:
  基于 cluster resource view 选择目标节点。

目标 raylet:
  对本机资源做最终确认。
  lease worker。
  启动 actor。
```

### 9.4 核心结论

```text
PG placement strategy 决定 bundle 怎么先占资源；
PlacementGroupSchedulingStrategy 决定 actor/task 使用哪个 PG 资源池；
actor/task scheduling_strategy 是单选概念；
raylet 根据资源约束和调度策略选择节点；
actor 启动后，actor method 不再做节点调度。
```

## 10. XTuner 当前 Ray Actor 分配策略风险分析

本节基于当前 XTuner 代码与配置进行静态分析，重点关注 Ray 启动方式、head 节点压力、actor 放置策略、Placement Group 使用方式，以及对象/控制流是否可能集中经过 driver 或 head。

分析范围：

```text
代码目录:
  /mnt/shared-storage-user/huanghaian/code/xtuner

配置:
  examples/v1/config/rl_grpo_geo3k_judge.py

脚本:
  examples/v1/scripts/run_rl.sh
```

### 10.1 `run_rl.sh` 默认走 Ray Client，训练主进程存在数据通路风险

`xtuner/v1/train/cli/rl.py` 中，如果检测到 `RAY_MASTER_ADDR`，会使用 Ray Client 地址初始化：

```python
ray_head_address = f"ray://{master_addr}:{client_port}"
ray.init(address=ray_head_address)
```

对应脚本 `examples/v1/scripts/run_rl.sh` 会默认设置：

```bash
export RAY_MASTER_ADDR=${MASTER_ADDR:-"127.0.0.1"}
```

因此直接使用 `run_rl.sh` 时，trainer 进程通常会走 Ray Client 模式。对 RL 训练来说，这有两个风险：

```text
1. trainer 与 Ray 集群之间多一层 Ray Client proxy。
2. 大量 ray.get、rollout state、训练 batch、控制请求可能经过 client/server 代理链路。
```

这对高频控制流、大对象传输、同步 rollout 生产都不理想。

改动建议：

```text
1. 集群内训练入口默认使用 ray.init(address="auto")。
2. Ray Client 只作为显式 debug / notebook / 外部交互模式。
3. 在脚本中增加显式开关，例如 XTUNER_RAY_CONNECT_MODE=auto/client。
4. 默认训练脚本不要因为存在 RAY_MASTER_ADDR 就自动使用 ray://。
```

建议方向：

```python
connect_mode = os.getenv("XTUNER_RAY_CONNECT_MODE", "auto")
if connect_mode == "client":
    ray.init(address=f"ray://{master_addr}:{client_port}")
else:
    ray.init(address="auto")
```

### 10.2 head 节点声明全集群 CPU，容易误导 Ray 调度

当前脚本中，head 节点启动时会把 CPU 数量设置成全集群总量：

```bash
node_count=${NODE_COUNT:-1}
total_cpus=$((node_count * 128))

ray start --head \
  ... \
  --num-cpus=$total_cpus
```

这会让 Ray 认为 head 节点拥有全集群 CPU。结合 Ray 默认 `HYBRID` 调度策略，可能导致：

```text
1. CPU actor 更容易落到 head。
2. 无 PG 的 controller / judger / helper task 更容易落到 head。
3. ray.available_resources() 的 CPU 判断被误导。
4. head 既承担 GCS / dashboard / driver / job supervisor，又承担过多业务 actor。
```

这和 Ray 的节点资源模型不一致。Ray 期望每个 raylet 声明本节点真实可用资源，而不是 head 代表全集群资源。

改动建议：

```text
1. 删除 head 上的 --num-cpus=$((node_count * 128))。
2. 让 Ray 自动识别 head 本机 CPU，或只传 head 本机 CPU 数。
3. worker 节点也应各自声明本机 CPU，而不是由 head 汇总声明。
4. 如果希望 head 只做控制节点，可以显式降低 head CPU，例如 --num-cpus=0 或较小值。
```

对于训练集群，更推荐：

```text
head:
  只声明控制面需要的少量 CPU，避免业务 actor 堆到 head。

worker:
  声明本机 CPU/GPU/NPU，承载训练、rollout、judger、agent loop 等业务 actor。
```

### 10.3 GPU train / rollout worker 的 Placement Group 编排整体合理

训练 worker 和 rollout worker 都通过 accelerator Placement Group 创建，整体方向是对的。

相关逻辑：

```text
AutoAcceleratorWorkers.build_placement_group(...)
  创建包含 CPU / memory / GPU 或 NPU 的 bundles。

AutoAcceleratorWorkers.from_placement_group(...)
  按 bundle index 创建 worker actor。
```

训练 worker：

```text
WorkerConfig.build(...)
  -> AutoAcceleratorWorkers.from_placement_group(...)
```

rollout worker：

```text
RolloutController._init_workers(...)
  -> AutoAcceleratorWorkers.from_placement_group(...)
```

这意味着核心 GPU/NPU actor 会被 PG 专属资源限制到对应 bundle 所在节点，不会因为 GCS initial forwarding raylet 在 head 上，就全部跑到 head。

改动建议：

```text
1. 保留训练 worker / rollout worker 使用 accelerator PG 的做法。
2. 多机情况下检查 pg_pack_strategy 是否符合预期。
3. colocate 模式一般用 PACK 可以让 train/rollout 共享同一组资源位。
4. disaggregated 模式应使用独立 train_pg / rollout_pg，并确保 PG name 不复用错误。
```

需要注意：

```text
PG 只能约束使用了 PlacementGroupSchedulingStrategy 或 legacy placement_group options 的 actor/task。
没有使用 PG 的 control actor 不会自动跟随这些 GPU bundles。
```

### 10.4 RolloutController 没有显式资源和放置约束，可能成为 head 热点

`RolloutConfig.build()` 创建 `RolloutController` 时，当前只设置了 `max_concurrency`：

```python
return (
    ray.remote(RolloutController)
    .options(max_concurrency=int(os.environ.get("RAY_MAX_CONCURRENCY", 1000)))
    .remote(self, placement_group)
)
```

这里没有设置：

```text
num_cpus
scheduling_strategy
PlacementGroupSchedulingStrategy
NodeAffinitySchedulingStrategy
NodeLabelSchedulingStrategy
```

因此 `RolloutController` 会走普通 actor 默认调度。结合 head 节点 CPU 被过度声明的问题，它容易落到 head。

这类 controller 虽然不直接占 GPU，但它负责：

```text
1. rollout worker 管理。
2. generate 请求路由。
3. worker health check。
4. server URL / rank metadata 管理。
5. onload / offload / recover 等控制操作。
```

如果 rollout 请求频繁，controller 落到 head 会造成明显控制面压力。

改动建议：

```text
1. 给 RolloutController 显式设置 num_cpus，例如 1~4。
2. 给 RolloutController 明确放置策略，避免默认落到 head。
3. 可以使用 CPU PG 或 NodeLabel，把 controller 放在 rollout worker 附近。
4. 对多机大规模 rollout，可以考虑 controller 分片，减少单 actor max_concurrency 压力。
```

一种更稳的方向：

```python
ray.remote(RolloutController).options(
    num_cpus=2,
    scheduling_strategy=...
)
```

如果希望 controller 跟某个 rollout bundle 同节点，可以考虑使用 PG 里的 CPU 资源，或给 rollout 节点打 label 后使用 NodeLabel。

对于当前共卡模式，更符合需求的最小改法不是把 `RolloutController` 绑定到某个 GPU bundle，而是只保证它不落到 head。因为共卡模式下每张卡已经由 accelerator PG 均匀编排，`RolloutController` 本身属于控制面 actor，不一定需要绑定某张 GPU。

更推荐给 worker 节点声明一个自定义逻辑资源，head 节点不声明这个资源：

```bash
# head:
ray start --head ...

# worker:
ray start \
  --address="$RAY_MASTER_ADDR:$RAY_HEAD_PORT" \
  --resources='{"xtuner_worker_node": 1}' \
  --block \
  --disable-usage-stats
```

然后创建 `RolloutController` 时要求一个很小的该资源：

```python
return (
    ray.remote(RolloutController)
    .options(
        num_cpus=1,
        resources={"xtuner_worker_node": 0.001},
        max_concurrency=int(os.environ.get("RAY_MAX_CONCURRENCY", 1000)),
    )
    .remote(self, placement_group)
)
```

这里 `xtuner_worker_node` 不是实际硬件资源，而是一个节点级调度标签。由于 head 没有这个资源，Ray 不会把 `RolloutController` 调度到 head；由于每个 worker 都有这个资源，controller 可以落到任意 worker 节点。

如果后续有多个 `RolloutController`，可以再叠加：

```python
scheduling_strategy="SPREAD"
```

这样 Ray 会在满足 `xtuner_worker_node` 的节点集合里尽量分散 controller，而不是把 head 纳入候选集合。

### 10.5 AgentLoop 默认本地运行，容易把 rollout 生产压力压到 trainer/driver

当前 `AgentLoopConfig` 默认：

```python
num_ray_actors: int = 0
```

当 `num_ray_actors=0` 时，agent loop 不会创建 Ray actor，而是本地运行在 trainer 进程中。

当前 GEO3K 配置中：

```python
agent_loop_config = SingleTurnAgentLoopConfig(...)
```

没有显式设置 `num_ray_actors`，所以它默认本地执行。`SyncProduceStrategy` 会在本地 event loop 中创建大量 asyncio tasks，然后调用：

```python
rollout_ctl.generate.remote(...)
judger.judge(...)
```

在当前配置下：

```text
train_batch_size = 1024
prompt_repeat_k = 5
RAY_MAX_CONCURRENCY = 1024
```

这会让 trainer/driver 承担大量数据生产调度、rollout 状态管理和回收逻辑。如果 trainer 在 head 或 Ray Client 前端，这会放大 head / client proxy 的压力。

改动建议：

```text
1. 大 batch / 高并发 rollout 场景下，不建议 AgentLoop 长期本地跑在 trainer 进程。
2. 将 AgentLoop 配置为多个 Ray actor，例如 num_ray_actors > 0。
3. 为 AgentLoop actor 创建 CPU PG，并使用 SPREAD 分散到 worker 节点。
4. AgentLoop actor 数量应和 rollout 并发、judger 吞吐、CPU 资源匹配。
5. trainer 只负责训练主循环和聚合，不承担所有 rollout 生产协程。
```

示意配置方向：

```python
agent_loop_config = SingleTurnAgentLoopConfig(
    ...,
    num_ray_actors=8,
    num_cpus=1,
    cpu_memory=1024**3,
)
```

同时需要让 `AgentLoopManagerConfig.build()` 支持传入 CPU PG，否则这些 AgentLoop actors 仍然会走默认调度。

当前代码已经预留了一部分 actor 化能力，但还不能通过配置强制分散。原因是：

```text
AgentLoopConfig.build(...)
  如果 num_ray_actors > 1，会调用 _build_router(...)

_build_router(...)
  内部调用 _build_ray_actors(...)

_build_ray_actors(...)
  支持 pg 参数

但是 AgentLoopManagerConfig.build(...)
  当前调用 task_cfg.agent_loop_config.build(...)
  没有传入 pg
```

因此即使配置 `num_ray_actors > 1`，当前创建出的多个 `AgentLoopActor` 仍然是普通 DEFAULT 调度：

```python
actor_cls.options(num_cpus=..., memory=...).remote(...)
```

这只能说有概率被 Ray 分散，不能保证分散；如果 head 还错误声明了全集群 CPU，这些 actor 仍然可能落到 head。

推荐改造方向是给 AgentLoop 单独创建 CPU Placement Group：

```python
agent_loop_pg = placement_group(
    bundles=[
        {
            "CPU": agent_loop_num_cpus,
            "memory": agent_loop_memory,
            "xtuner_worker_node": 0.001,
        }
        for _ in range(num_agent_loop_actors)
    ],
    strategy="SPREAD",
)
ray.get(agent_loop_pg.ready())
```

然后每个 `AgentLoopActor` 绑定一个 PG bundle：

```python
PlacementGroupSchedulingStrategy(
    placement_group=agent_loop_pg,
    placement_group_bundle_index=i,
    placement_group_capture_child_tasks=True,
)
```

这个方案表达的是：

```text
1. AgentLoopActor 只能调度到带有 xtuner_worker_node 的 worker 节点。
2. PG strategy="SPREAD" 先把 CPU bundles 分散到不同节点。
3. 每个 AgentLoopActor 再绑定一个 bundle，避免多个 actor 都挤到同一资源位。
```

相比简单使用 `scheduling_strategy="SPREAD"`，CPU PG 更适合 AgentLoop：

```text
scheduling_strategy="SPREAD":
  只是让普通 actor 尽量分散，不能表达固定资源位，也不能和后续 CPU 资源规划自然结合。

CPU PG + SPREAD:
  先规划 AgentLoop 的 CPU/memory 资源位，再将 actor 绑定到这些资源位。
  这更接近“强制分散”的语义。
```

对应代码改动建议：

```text
1. 在 AgentLoopManagerConfig.build() 外层创建 agent_loop_pg。
2. 将 agent_loop_pg 传给 task_cfg.agent_loop_config.build(...)。
3. AgentLoopConfig.build() / _build_router() / _build_ray_actors() 沿用已有 pg 参数。
4. CPUActorLauncher 继续使用 PlacementGroupSchedulingStrategy 绑定 bundle。
5. 启动脚本中 worker 节点声明 xtuner_worker_node，head 不声明。
```

### 10.6 Judger 是远程 actor，但没有 PG 且副本数偏少

当前配置：

```python
judger_config = GEO3KJudgerConfig(num_ray_actors=1)
```

JudgerConfig 在 `num_ray_actors > 0` 时会创建远程 Ray actor。但 `AgentLoopManagerConfig.build()` 中调用：

```python
judger=build_judger(task_cfg.judger_config)
```

没有传入 PG，也没有显式调度策略。因此 judger actor 走普通 DEFAULT 调度。

风险：

```text
1. judger 可能落到 head。
2. 单个 judger actor 可能成为 rollout 后处理瓶颈。
3. 如果 reward handler 是 HTTP 或 CPU 密集逻辑，单 actor 会限制吞吐。
4. 如果 head CPU 被过度声明，judger 更容易和 driver/controller 抢 head CPU。
```

改动建议：

```text
1. 为 judger 创建独立 CPU PG。
2. judger PG 使用 SPREAD，避免所有 judger 堆到同一节点。
3. 根据 batch size 和判分耗时增加 num_ray_actors。
4. 修改 AgentLoopManagerConfig.build()，允许传入 judger_pg / agent_loop_pg。
5. 如果 judge 是轻量逻辑，也至少显式 num_cpus，避免资源不透明。
```

示意方向：

```python
judger_config = GEO3KJudgerConfig(
    num_ray_actors=8,
    num_cpus_per_actor=1,
    cpu_memory_per_actor=1024**3,
)
```

并在构建时：

```python
build_judger(task_cfg.judger_config, pg=judger_pg, start_bundle_idx=...)
```

### 10.7 CPU actor / control-plane actor 缺少统一资源规划

当前代码中 GPU/NPU worker 的 PG 规划比较明确，但 CPU/control-plane actor 比较分散：

```text
RolloutController:
  无 PG，无显式 num_cpus。

AgentLoop:
  默认本地，不消耗 Ray 资源，但消耗 driver/head CPU。

Judger:
  可远程，但当前构建路径没有 PG。

find_master_addr_and_port / get_accelerator_ids:
  作为 helper remote task，绑定 accelerator PG bundle，问题较小。
```

这会导致资源模型不完整：

```text
GPU/NPU worker 被 PG 管住了；
CPU 控制面和数据生产面没有被同等严格地管住。
```

改动建议：

```text
1. 将 Ray 资源规划分成 accelerator PG 和 CPU/control PG 两层。
2. 所有长期存在的 CPU actor 都显式声明 num_cpus / memory。
3. 所有关键 CPU actor 都指定 PG / NodeLabel / NodeAffinity 中的一种放置约束。
4. trainer driver 尽量只保留主循环，不承担大规模生产协程。
5. head 节点尽量不承载业务 actor，或者只承载低负载控制 actor。
```

### 10.8 建议改造优先级

P0：启动与 head 资源声明

```text
1. 默认训练模式不要走 Ray Client，改为 ray.init(address="auto")。
2. 删除 head 上的全集群 --num-cpus 声明。
3. head / worker 分别声明各自真实资源。
```

P1：控制面 actor 放置

```text
1. RolloutController 显式 num_cpus 和放置策略。
2. Judger 使用 CPU PG，并增加副本数。
3. AgentLoop 在大 batch 下改为远程 actor，并通过 CPU PG 分散。
```

P2：资源模型收敛

```text
1. 将 CPU PG / accelerator PG 统一纳入 trainer config。
2. AgentLoopManagerConfig.build() 支持接收 agent_loop_pg / judger_pg。
3. 所有长期 actor 均显式声明资源，不依赖 Ray 默认 actor 资源语义。
4. 对 rollout controller / judger / agent loop 增加监控，观察 actor 所在 node、CPU、heap、object store 使用。
```

总体结论：

```text
XTuner 当前 GPU/NPU worker 的 PG 编排基本合理；
主要风险集中在启动方式、head CPU 声明、RolloutController、AgentLoop、Judger 等 CPU/control-plane actor。

如果不处理这些问题，多机或大 batch 场景下容易出现：
  - head 负载过重；
  - trainer/driver 成为 rollout 生产瓶颈；
  - Ray Client 代理链路放大对象传输成本；
  - judger 或 controller 成为单点吞吐瓶颈。
```
