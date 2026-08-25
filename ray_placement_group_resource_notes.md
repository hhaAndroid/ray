# Ray Placement Group 资源语义笔记

## 1. 文档目标与讨论范围

本文用于说明 Ray Placement Group（PG）的基本原理、常见用法，以及 PG bundle 和 actor/task 资源请求之间的关系。

重点讨论：

```text
1. Placement Group 解决什么问题。
2. bundle 是什么，bundle 和 CPU/GPU 资源是什么关系。
3. PG 的 placement strategy 和 actor/task 的 scheduling_strategy 有什么区别。
4. actor/task 绑定到某个 bundle 后，资源如何扣减。
5. 为什么绑定到包含 GPU 的 bundle，不等于 actor 自动获得 GPU。
6. slime 中 InfoActor 为什么需要 @ray.remote(num_gpus=1)。
```

本文主要讨论用户可见的 PG 资源语义和调度约束，不展开 GCS placement group scheduler 的完整源码实现。

## 2. Placement Group 解决什么问题

Ray 默认调度 task/actor 时，会根据集群当前可用资源选择节点。对很多分布式训练或 serving 场景，仅靠单个 task/actor 的资源请求不够，需要先把一组资源一起预留出来。

Placement Group 的作用就是：

```text
把一组 bundle 作为整体进行预留和放置，
后续 task/actor 可以绑定到这个 PG 或某个具体 bundle 上运行。
```

典型场景：

```text
分布式训练：
  需要同时拿到 N 张 GPU，避免部分 worker 启动后剩余 worker 长期 pending。

训练 + 推理 colocate：
  需要稳定知道逻辑 rank 对应哪张物理 GPU。

多 actor gang scheduling：
  需要一组 actor 的资源同时满足，而不是零散地抢资源。
```

一个简单例子：

```python
import ray
from ray.util.placement_group import placement_group

ray.init(num_cpus=4, num_gpus=2)

pg = placement_group(
    [{"CPU": 1, "GPU": 1}, {"CPU": 1, "GPU": 1}],
    strategy="PACK",
)
ray.get(pg.ready())

print("cluster_resources:", ray.cluster_resources())
print("available_resources:", ray.available_resources())
```

这里创建了两个 bundle，每个 bundle 预留：

```text
CPU: 1
GPU: 1
```

注意，这里的“预留”不是说已经有某个 actor 使用了这些资源，而是 Ray 已经在集群里为这个 PG 占住了这些资源，后续可以从这些 bundle 里扣。

运行后，资源里会出现两类信息。

第一类是全局资源：

```text
CPU: 4.0
GPU: 2.0
```

它们表示整个 Ray 节点/集群声明给 Ray 的普通 CPU/GPU 资源总量。

第二类是 Ray 为 placement group 生成的 PG 专属资源，名字里会带一个 PG id：

```text
CPU_group_<pg_id>: 2.0
GPU_group_<pg_id>: 2.0
CPU_group_0_<pg_id>: 1.0
GPU_group_0_<pg_id>: 1.0
CPU_group_1_<pg_id>: 1.0
GPU_group_1_<pg_id>: 1.0
bundle_group_<pg_id>: 2000.0
bundle_group_0_<pg_id>: 1000.0
bundle_group_1_<pg_id>: 1000.0
```

可以这样读：

```text
CPU_group_0_<pg_id> / GPU_group_0_<pg_id>:
  第 0 个 bundle 里的 CPU/GPU 资源。

CPU_group_1_<pg_id> / GPU_group_1_<pg_id>:
  第 1 个 bundle 里的 CPU/GPU 资源。

CPU_group_<pg_id> / GPU_group_<pg_id>:
  这个 placement group 总共预留的 CPU/GPU 资源。

bundle_group_<pg_id> / bundle_group_0_<pg_id> / bundle_group_1_<pg_id>:
  Ray 内部用于表达 PG / bundle 归属约束的 marker resource。
  它们不是 CPU/GPU，也不代表真实硬件数量。
```

这里的 `1000.0` 容易误解，需要单独说明。

Ray 在创建/commit 一个 bundle 时，会为这个 bundle 额外生成两个 marker resource：

```text
bundle_group_<bundle_index>_<pg_id>: 1000.0
bundle_group_<pg_id>: 1000.0
```

第一个是 indexed marker，表示“这个具体 bundle 可被绑定”；第二个是 wildcard marker，表示“这个 placement group 在这个节点上有 bundle 可用”。如果同一个节点上有多个 bundle，wildcard `bundle_group_<pg_id>` 会累加。例如两个 bundle 都在同一节点时，通常会看到：

```text
bundle_group_0_<pg_id>: 1000.0
bundle_group_1_<pg_id>: 1000.0
bundle_group_<pg_id>: 2000.0
```

源码里这个值不是从用户的 `{"CPU": 1, "GPU": 1}` 推导出来的，而是 Ray 写死的内部 marker 容量。对应逻辑在 `src/ray/common/bundle_spec.cc`：

```text
每个 bundle 的 indexed bundle marker = 1000
每个 bundle 对 wildcard bundle marker 的贡献 = 1000
```

当 actor/task 使用 `PlacementGroupSchedulingStrategy` 绑定 PG 时，Ray 会把原始资源请求改写成 PG 专属资源请求，同时额外加一个很小的 bundle marker 请求：

```text
bundle_group_<pg_id>: 0.001
bundle_group_<bundle_index>_<pg_id>: 0.001   # 指定 bundle index 时
```

这就是实验里为什么启动一个 actor 后会看到：

```text
bundle_group_0_<pg_id>: 1000.0 -> 999.999
```

含义不是“这个 actor 消耗了 0.001 个 CPU/GPU”，而是：

```text
这个 actor 被约束到了这个 PG / bundle 上。
Ray 用 0.001 的 marker request 表达这种归属关系。
```

为什么 bundle marker 容量是 `1000`，而每个 PG-bound actor/task 只扣 `0.001`？

可以理解成 Ray 给每个 bundle 放了一个很大的“准入标记容量”。这个 marker 的目的不是限制真实 CPU/GPU，而是让即使 task/actor 本身不请求 CPU/GPU，也能通过资源调度路径被约束到对应 PG/bundle 上。`1000 / 0.001 = 1,000,000`，也就是说这个 marker 设计上非常不容易成为实际调度瓶颈。

因此：

```text
bundle_group_1_<pg_id>: 1000.0
  表示第 1 个 bundle 的 PG 归属 marker 容量。

每个绑定到第 1 个 bundle 的 actor/task:
  会额外扣 bundle_group_1_<pg_id>: 0.001。

这个数值不表示第 1 个 bundle 里有 1000 个资源，
也不表示可以跑 1000 个 actor。
真正能不能跑，仍然取决于 CPU_group/GPU_group 等真实资源请求。
```

为什么 `available_resources` 里普通 `CPU/GPU` 会下降？

假设创建 PG 前：

```text
CPU: 4.0
GPU: 2.0
```

创建上面的 PG 后，PG 总共预留了：

```text
CPU: 2
GPU: 2
```

因此普通可用资源会变成：

```text
CPU: 2.0
GPU: 0.0
```

同时 PG 专属资源会出现：

```text
CPU_group_0_<pg_id>: 1.0
GPU_group_0_<pg_id>: 1.0
CPU_group_1_<pg_id>: 1.0
GPU_group_1_<pg_id>: 1.0
```

后续如果 actor/task 使用 `PlacementGroupSchedulingStrategy` 绑定到某个 bundle，它的资源请求会从这些 `*_group_*` 资源里扣。

## 3. bundle 是什么

bundle 可以理解成 PG 内部的一个资源池。

例如：

```python
{"CPU": 4, "GPU": 1}
```

含义是：

```text
这个 bundle 里预留了 4 个 CPU resource 和 1 个 GPU resource。
```

它不是一个不可拆分的“硬件容器”，也不是“用了 CPU 就自动用了 GPU”的绑定单元。

更准确的模型是：

```text
placement group:
  一组 bundle

bundle:
  一组资源的预留池

actor/task:
  从指定 bundle 里扣自己声明需要的资源
```

因此，一个 bundle 可以被多个 actor/task 共享，只要它们请求的资源总和不超过 bundle 剩余资源。

例如：

```text
bundle = {"CPU": 4, "GPU": 1}

可以同时放：
  actor A: num_gpus=1, num_cpus=0
  actor B: num_cpus=1
  actor C: num_cpus=1
  actor D: num_cpus=0, num_gpus=0
```

只要 GPU 和 CPU 的扣减都没有超过 bundle 资源上限，这些 actor 就可以同 bundle。

所以需要区分两句话：

```text
bundle 是 PG 的最小预留/放置单元。
bundle 不是 actor/task 的最小独占分配单元。
```

## 4. PG placement strategy 与 task/actor scheduling_strategy

Ray 里有两层容易混淆的 strategy。

### 4.1 PG 的 placement strategy

这是创建 PG 时传给 `placement_group(...)` 的 strategy：

```python
pg = placement_group(bundles, strategy="PACK")
```

它决定多个 bundles 如何放到集群节点上：

```text
PACK:
  尽量把 bundles 放到少量节点上。

SPREAD:
  尽量把 bundles 分散。

STRICT_PACK:
  所有 bundles 必须放在同一个节点。

STRICT_SPREAD:
  每个 bundle 必须放在不同节点。
```

这个阶段解决的是：

```text
PG 的 bundles 本身如何预留和放置。
```

### 4.2 actor/task 的 scheduling_strategy

这是创建 actor/task 时传给 `.options(...)` 的 scheduling strategy：

```python
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

strategy = PlacementGroupSchedulingStrategy(
    placement_group=pg,
    placement_group_bundle_index=0,
)

actor = Actor.options(scheduling_strategy=strategy).remote()
```

它决定这个 actor/task 的资源请求从哪里扣。

如果指定了 `placement_group_bundle_index=0`，含义是：

```text
这个 actor/task 的资源需求必须从 PG 的第 0 个 bundle 里满足。
```

它不表示：

```text
这个 actor/task 独占第 0 个 bundle。
```

## 5. 绑定 bundle 后资源如何扣减

假设有一个 bundle：

```python
bundle = {"CPU": 1, "GPU": 1}
```

并把 actor 绑定到这个 bundle：

```python
strategy = PlacementGroupSchedulingStrategy(
    placement_group=pg,
    placement_group_bundle_index=0,
)
```

actor 实际扣哪些资源，取决于 actor 自己的 resource request。

### 5.1 不声明 GPU 的 actor

```python
@ray.remote
class PlainActor:
    def info(self):
        return ray.get_gpu_ids()

actor = PlainActor.options(scheduling_strategy=strategy).remote()
```

这个 actor 绑定到了第 0 个 bundle，但它没有请求 GPU。

因此：

```text
它不会扣这个 bundle 的 GPU。
ray.get_gpu_ids() 通常返回 []。
```

### 5.2 声明 GPU 的 actor

```python
@ray.remote(num_gpus=1)
class GPUActor:
    def info(self):
        return ray.get_gpu_ids()

actor = GPUActor.options(scheduling_strategy=strategy).remote()
```

这个 actor 请求 1 个 GPU，并且绑定到了第 0 个 bundle。

因此：

```text
Ray 必须从第 0 个 bundle 里分配 1 个 GPU 给它。
ray.get_gpu_ids() 才会返回非空 GPU id。
```

### 5.3 0-resource actor

```python
@ray.remote(num_cpus=0, num_gpus=0)
class ZeroResourceActor:
    ...
```

这种 actor 基本不扣 CPU/GPU 资源。即使同一个 bundle 里的 GPU 已经被其他 actor 占用，它仍然可能被调度进去。

## 6. bundle_group_* 是什么

观察 `ray.available_resources()` 时，经常会看到类似资源：

```text
bundle_group_6e8ae141c8c453ee42492fcc3c4e01000000: 1000.0  # 单 bundle 时常见；同节点多 bundle 时会累加
bundle_group_0_6e8ae141c8c453ee42492fcc3c4e01000000: 1000.0
CPU_group_0_6e8ae141c8c453ee42492fcc3c4e01000000: 1.0
GPU_group_0_6e8ae141c8c453ee42492fcc3c4e01000000: 1.0
```

可以先按下面方式理解：

```text
CPU_group_0_<pg_id>:
  第 0 个 bundle 中的 CPU 资源。

GPU_group_0_<pg_id>:
  第 0 个 bundle 中的 GPU 资源。

bundle_group_<pg_id> / bundle_group_0_<pg_id>:
  Ray 内部用于表达 PG / bundle 归属约束的 marker resource。
```

`bundle_group_*` 不是 CPU，也不是 GPU，不代表真实硬件。它是 Ray 调度内部用于把 task/actor 约束到 PG/bundle 的资源标记。

更具体地说，每个 bundle 会生成 `1000.0` 的 indexed marker：

```text
bundle_group_0_<pg_id>: 1000.0
bundle_group_1_<pg_id>: 1000.0
```

同时每个 bundle 也会给 wildcard marker 贡献 `1000.0`：

```text
bundle_group_<pg_id>: 1000.0   # 该节点上只有 1 个 bundle
bundle_group_<pg_id>: 2000.0   # 该节点上有 2 个 bundle
```

actor/task 绑定 PG 时，Ray 只额外请求 `0.001` 的 bundle marker。所以 `1000.0` 不是资源数量，而是一个足够大的内部标记容量，用来承载很多 PG-bound task/actor 的归属约束。

例如某个 actor 绑定到第 0 个 bundle 后，可能看到：

```text
bundle_group_0_<pg_id>: 1000.0 -> 999.999
```

这表示 actor 被放进了这个 bundle，但只扣了很小一部分 marker resource。

判断 actor 是否真的消耗了 bundle 的 GPU，要看：

```text
GPU_group_0_<pg_id>
ray.get_gpu_ids()
```

而不是看 `bundle_group_*`。

## 7. slime 中 InfoActor 为什么需要 num_gpus=1

slime 里有如下代码：

```python
@ray.remote(num_gpus=1)
class InfoActor:
    def get_ip_and_gpu_id(self):
        return ray.util.get_node_ip_address(), ray.get_gpu_ids()[0]
```

创建 placement group 后，slime 会把临时 `InfoActor` 放到每个 bundle 上：

```python
InfoActor.options(
    scheduling_strategy=PlacementGroupSchedulingStrategy(
        placement_group=pg,
        placement_group_bundle_index=i,
    ),
).remote()
```

它的目的不是长期运行，而是探测：

```text
第 i 个 PG bundle 实际落在哪个 node、哪张 GPU。
```

如果 `InfoActor` 不声明 `num_gpus=1`，即使它绑定到了包含 GPU 的 bundle：

```text
ray.get_gpu_ids()
```

仍然会返回空列表。

因此这里的 `num_gpus=1` 是必要的。它强制 Ray 从指定 bundle 中为这个临时 actor 分配 1 个 GPU，然后 `ray.get_gpu_ids()[0]` 才能拿到 GPU id。

slime 随后会 kill 掉这些临时 actor：

```python
for actor in info_actors:
    ray.kill(actor)
```

最终得到两个稳定映射：

```text
pg_reordered_bundle_indices:
  逻辑 GPU 顺序 -> Ray PG bundle index

pg_reordered_gpu_ids:
  逻辑 GPU 顺序 -> physical GPU id
```

这对后续 Megatron actor 和 SGLang engine 的 GPU 排布很重要。

## 8. 推荐心智模型

可以把 Placement Group 理解成：

```text
PG:
  一次性预留一组资源。

bundle:
  PG 内部的资源池。

placement_group_bundle_index:
  指定 actor/task 从哪个资源池里扣资源。

actor/task resource request:
  决定实际扣 CPU、GPU 还是其他 custom resource。
```

最容易犯错的理解是：

```text
错误：actor 绑定到 {"CPU": 1, "GPU": 1} bundle，就自动拥有 CPU 和 GPU。
正确：actor 只拥有它自己请求并被 Ray 分配到的资源。
```

因此：

```text
绑定 bundle 只提供资源扣减范围。
请求 GPU 才会获得 GPU。
ray.get_gpu_ids() 只反映 actor 实际获得的 GPU allocation。
```
