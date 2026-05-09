# Ray Object Spilling 机制讨论记录

## 1. 背景

本记录整理一次关于 Ray Object Spilling 的讨论，重点解释：

1. 单节点多 actor 场景下 object store 如何判断内存压力。
2. spilling 是谁触发的。
3. 什么对象算“可 spill 对象”。
4. 为什么默认启用 spilling 仍然可能 OOM。

主要参考本地文档和源码：

- `doc/source/ray-core/internals/object-spilling.rst`
- `doc/source/ray-core/objects/object-spilling.rst`
- `doc/source/ray-core/objects/serialization.rst`
- `doc/source/ray-core/scheduling/memory-management.rst`
- `doc/source/ray-core/scheduling/ray-oom-prevention.rst`
- `src/ray/raylet/node_manager.cc`
- `src/ray/raylet/local_object_manager.cc`
- `src/ray/raylet/local_object_manager.h`
- `src/ray/object_manager/plasma/store.cc`
- `src/ray/object_manager/plasma/obj_lifecycle_mgr.cc`
- `src/ray/object_manager/plasma/common.h`
- `src/ray/object_manager/plasma/create_request_queue.cc`
- `python/ray/_private/utils.py`
- `python/ray/_private/ray_constants.py`
- `src/ray/common/ray_config_def.h`

## 2. 单节点多 actor 时的 object store 模型

在一个节点上，即使有 100 个 Ray actor，也不是每个 actor 各自有一个 object store。

模型是：

```text
100 个 actor / worker
    |
    | ray.put / task return / ray.get 拉取对象
    v
同一个节点上的 Plasma object store
    |
    v
同一个节点上的 raylet / LocalObjectManager
```

本地文档 `doc/source/ray-core/objects.rst` 和 `serialization.rst` 都说明：每个节点有自己的 shared-memory object store。一个节点上的多个 worker / actor 通过这个 shared memory object store 共享大对象。

因此，spilling 的判断是节点级别的，不是 actor 级别的。

## 3. Object Spilling 解决的是什么问题

Object Spilling 解决的是：

```text
object store shared memory 不够用
```

它不是解决：

```text
Python worker heap 不够用
```

spilling 做的事情是：

```text
object store shared memory
    -> 外部存储，例如本地磁盘或 S3
```

对象的 `ObjectRef` 仍然有效。以后如果有人 `ray.get(ref)`，Ray 会把 spilled object restore 回 object store。

所以 spilling 是“腾 object store shared memory”，不是“释放对象引用”。

## 4. Object store 容量和 threshold 的来源

`object_spilling_threshold` 是基于 object store 容量判断的，不是基于节点总内存判断的。

Raylet 中的判断公式是：

```text
allocated_percentage =
    primary object bytes / object store capacity
```

达到 `object_spilling_threshold` 后，Ray 会主动尝试 spilling。

### 4.1 object_store_memory 是怎么来的

Raylet 本身要求启动时传入一个明确的 `object_store_memory`。这个值有两种来源。

第一种是用户显式设置：

```python
ray.init(object_store_memory=100 * 1024**3)
```

或者：

```bash
ray start --object-store-memory=107374182400
```

这时 object store 容量就是用户设置的值。

第二种是用户不设置，Ray 自动计算。源码在 `python/ray/_private/utils.py` 的 `resolve_object_store_memory()`：

```python
object_store_memory = int(
    available_memory_bytes
    * DEFAULT_OBJECT_STORE_MEMORY_PROPORTION
)
```

默认比例在 `python/ray/_private/ray_constants.py`：

```python
DEFAULT_OBJECT_STORE_MEMORY_PROPORTION = 0.3
```

也就是：

```text
默认 object_store_memory = 当前节点 available memory 的 30%
```

但这个默认值还会被 cap：

- 默认最大值是 200GB。
- Linux 上默认还会受 `/dev/shm` 可用大小限制，取 `/dev/shm` 可用大小约 95% 作为上限之一。
- Mac 上还有额外的较小限制，用来避免性能问题。

所以默认 object store 容量可以粗略理解为：

```text
min(available memory * 30%, /dev/shm 可用大小 * 95%, 200GB)
```

实际源码还包含最小值、平台差异和用户显式配置路径。

### 4.2 object_spilling_threshold 的分母是什么

`object_spilling_threshold` 的分母是：

```text
object_store_memory
```

不是：

```text
节点总内存
```

也不是：

```text
worker heap + object store + raylet 的总和
```

例如：

```text
节点可用内存: 400GB
/dev/shm 足够大
默认 object store: min(400GB * 0.3, 200GB) = 120GB
object_spilling_threshold = 0.8
=> primary object bytes 到约 96GB 时触发主动 spilling
```

如果 `/dev/shm` 只有 64GB：

```text
默认 object store 可能被 /dev/shm cap 住
约 64GB * 0.95 = 60.8GB
object_spilling_threshold = 0.8
=> primary object bytes 到约 48.6GB 时触发主动 spilling
```

### 4.3 默认值通常够用，但原因要理解清楚

一般 workload 下，默认值通常可以先用，因为 Ray 已经按节点可用内存和 `/dev/shm` 情况给 object store 分配了一个相对合理的容量。

但不能理解成：

```text
spilling 条件苛刻，所以默认值就一定安全。
```

更准确的理解是：

```text
默认 object_store_memory 通常够用；
但 spilling 条件苛刻本身是风险点，不是安全点。
```

如果 object store 里很多对象都正在被 actor / worker / driver 映射使用，Ray 可能找不到可 spill 对象：

```text
object store 压力大
但对象都 ref_count > 1
=> 不能 spill
=> 仍可能 ObjectStoreFull / OOM
```

默认值适合的场景：

- 大对象数量不多。
- 对象生命周期短。
- actor 不长期保存大 NumPy / Arrow zero-copy view。
- `ray.get` 是分批处理，不是一次性 get 很多。
- spill 目录磁盘空间足够。

需要关注或调参的场景：

- 许多 actor 并发产生大对象。
- 大量 task return 大对象，driver 长时间保存 refs。
- 大量 NumPy 对象被 actor / driver `ray.get` 后长期保存。
- Ray Data、shuffle、batch inference 等 object store 压力大的 workload。
- Docker 或容器环境中 `/dev/shm` 很小，导致默认 object store 被 cap 得很小。

因此建议是：默认值可以先用，但要配合 `ray memory --stats-only` 和 raylet spill 日志观察真实压力；不要因为 spilling 存在就放松对对象生命周期、并发和 zero-copy view 的控制。

## 5. Spilling 的触发路径

当前源码和 `doc/source/ray-core/internals/object-spilling.rst` 中整理了三条触发路径。

### 5.1 主动触发：超过 object_spilling_threshold

`src/ray/raylet/node_manager.cc` 中 `SpillIfOverPrimaryObjectsThreshold()` 会判断：

```cpp
allocated_percentage =
    local_object_manager_.GetPrimaryBytes() /
    object_manager_.GetMemoryCapacity();

if (allocated_percentage >= object_spilling_threshold) {
    local_object_manager_.SpillObjectUptoMaxThroughput();
}
```

默认阈值在 `src/ray/common/ray_config_def.h`：

```cpp
object_spilling_threshold = 0.8
```

也就是：

```text
primary object bytes / object store capacity >= 80%
```

时，Ray 会主动尝试 spilling。

这里的 primary object bytes 来自 `LocalObjectManager::GetPrimaryBytes()`：

```cpp
pinned_objects_size_ + num_bytes_pending_spill_
```

可以粗略理解为 raylet 当前管理的 primary copy 大小，包括：

- 还在 shared memory 中 pinned 的对象。
- 已经交给 IO worker、正在 spilling 的对象。

### 5.2 主动触发：周期性检查

`node_manager.cc` 会周期性执行：

```text
SpillIfOverPrimaryObjectsThreshold()
```

周期由：

```cpp
free_objects_period_milliseconds = 1000
```

控制，默认约 1 秒。

### 5.3 主动触发：新对象 sealed 后立即检查

每当 Plasma object store 中有新对象 sealed，Raylet 会执行 `HandleObjectLocal()`，最后调用：

```text
SpillIfOverPrimaryObjectsThreshold()
```

所以大对象刚写入完成并让 primary bytes 超过阈值时，不一定要等下一次周期检查，可能马上触发 spilling。

### 5.4 被动触发：Create 对象时已经 OOM

当 actor / worker 要创建新 Plasma 对象时，如果 shared memory 分配失败，会进入 `CreateRequestQueue::ProcessRequests()`。

流程大概是：

```text
Create object
  |
  v
Plasma allocation OutOfMemory
  |
  v
触发 global GC
  |
  v
调用 spill_objects_callback()
  |
  v
raylet 的 LocalObjectManager 尝试 spilling
  |
  v
等待 oom_grace_period_s
  |
  v
仍失败则尝试 fallback allocator
  |
  v
fallback 也失败则报错
```

`oom_grace_period_s` 默认是 2 秒。

## 6. 什么是“可 spill 对象”

这是最容易混淆的点。

源码中的判断在 `src/ray/object_manager/plasma/store.cc`：

```cpp
bool PlasmaStore::IsObjectSpillable(const ObjectID &object_id) {
  auto entry = object_lifecycle_mgr_.GetObject(object_id);
  if (!entry) {
    return false;
  }
  return entry->Sealed() && entry->GetRefCount() == 1;
}
```

也就是说，一个对象可 spill，必须同时满足：

```text
1. 对象已经 sealed
2. Plasma 内部 ref_count == 1
```

### 6.1 sealed 是什么

对象写入 Plasma object store 时，不是一瞬间完成的。

过程是：

```text
Create
  -> worker 往 shared memory 写对象数据
  -> Seal
  -> 对象变成完整、不可变、可被其他 worker 读取
```

`sealed` 就是对象已经完整写入，并且变成不可变对象。

没有 sealed 的对象还在创建中，不能 spill。

### 6.2 Plasma ref_count 是什么

这里的 `GetRefCount()` 不是前一个文档里讨论的 distributed `ObjectRef` 引用计数。

它是 Plasma store 内部的本地使用计数，可以理解为：

```text
这个节点上有多少 Plasma client 正在使用 / 映射这块 shared memory
```

源码中 `ObjectLifecycleManager::AddReference()` 会增加这个计数，`RemoveReference()` 会减少这个计数。

谁可能增加 Plasma ref_count：

- 创建对象的 worker。
- `ray.get` 读取对象的 driver / worker。
- actor task 读取对象参数时的 worker。
- raylet 为了管理 primary copy 而 pin 对象。

### 6.3 为什么 ref_count == 1 才可 spill

`ref_count == 1` 通常可以理解为：

```text
只有 raylet 还 pin 着这份 primary copy，
没有 worker / actor / driver 正在直接映射或使用 object store 中这块 shared memory。
```

此时 Ray 可以安全地把这份 shared memory 数据写到磁盘，并释放 object store 空间。

如果 `ref_count > 1`，说明有其他进程正在直接使用这块 shared memory。Ray 不能一边让进程读这块内存，一边把它搬走，所以不能 spill。

### 6.4 可 spill 不等于没人持有 ObjectRef

这是一个关键结论：

```text
ObjectRef 还活着，仍然可以 spill。
```

因为 `ObjectRef` 保活的是对象身份和元数据，不代表某个进程正在直接映射 object store 里的数据。

可 spill 的意思是：

```text
对象仍然有效，
但当前没人直接用 shared memory 里的这份数据，
所以可以先搬到磁盘。
```

以后有人通过 `ObjectRef` 访问时，再 restore。

## 7. 典型例子

### 7.1 可 spill：driver 只持有 ObjectRef

```python
ref = ray.put(big_obj)
```

如果 driver 只是持有 `ref`，没有 `ray.get(ref)`，也没有 actor 正在使用这个对象，那么对象通常可以 spill。

状态可以理解为：

```text
driver: 持有 ObjectRef
object store: 有对象数据
raylet: pin primary copy
其他 worker/actor: 没有映射对象数据

Plasma ref_count ~= 1
=> 可 spill
```

这里 `ref` 还活着，所以对象不能被删除；但 object store 中的数据可以被 spill 到磁盘。

### 7.2 可 spill：task 返回大对象，caller 暂时不 get

```python
@ray.remote
def make_big():
    return big_obj

ref = make_big.remote()
```

如果 driver 只拿到返回的 `ObjectRef`，但暂时没有 `ray.get(ref)`，并且没有其他 worker 正在读取该对象，那么这个返回对象通常也可以 spill。

```text
ObjectRef 保活对象
但没有进程映射 object store 数据
=> 可 spill
```

### 7.3 不可 spill：对象还在写入中

```python
ref = ray.put(very_big_obj)
```

在 `ray.put` 的写入过程中，对象还没 sealed。

```text
Create
  -> 正在写 bytes
  -> 还没 Seal
```

这个阶段不能 spill。

### 7.4 不可 spill：NumPy zero-copy 结果还活着

```python
ref = ray.put(np.zeros(...))
arr = ray.get(ref)
del ref
```

`arr` 可能直接指向 object store shared memory。即使 `ref` 被删除，只要 `arr` 还活着，这块 shared memory 仍然被 driver 映射。

状态可以理解为：

```text
raylet pin: 1
driver arr 映射 shared memory: +1

Plasma ref_count > 1
=> 不可 spill
```

需要：

```python
del arr
```

之后才可能变成可 spill 或可释放。

### 7.5 不可 spill：actor 正在使用 zero-copy 参数

```python
ref = ray.put(big_np_array)
actor.consume.remote(ref)
```

顶层传 `ObjectRef` 给 actor method 时，actor 收到的是对象值。如果这个对象是 NumPy array，actor worker 可能直接映射 object store shared memory。

actor method 执行期间：

```text
raylet pin: 1
actor worker 映射参数: +1

Plasma ref_count > 1
=> 不可 spill
```

等 actor method 执行结束，并且没有保存这个 zero-copy view 后，才可能重新变成可 spill。

### 7.6 普通 Python 对象 get 后通常不阻止 spill

```python
ref = ray.put(big_custom_obj)
obj = ray.get(ref)
```

如果是普通自定义对象，`ray.get(ref)` 通常会反序列化出 Python heap 副本。反序列化完成后，`obj` 不再依赖 object store shared memory。

状态可能是：

```text
obj: Python heap 副本
object store 原始数据: 没人直接映射
raylet: 仍然 pin primary copy

Plasma ref_count ~= 1
=> 仍可能可 spill
```

这就是普通 Python 对象和 NumPy 对象的重要区别：

```text
普通对象 ray.get 后通常是 heap copy
NumPy ray.get 后可能是 shared memory view
```

## 8. 为什么默认启用 spilling 仍然可能 OOM

spilling 不是无限内存。默认启用 spilling 后，仍然可能 OOM。

### 8.1 没有可 spill 对象

如果 object store 里大对象都正在被 actor / worker / driver 映射使用，就算 object store 满了，Ray 也不能 spill 它们。

典型情况：

- 很多 actor 正在同时消费大 NumPy 参数。
- driver `ray.get` 了很多 NumPy 对象，并长期保存返回值。
- 大对象正在创建中，还没有 sealed。
- 对象正在被任务作为参数读取。

### 8.2 spilling 速度跟不上对象产生速度

100 个 actor 同时产生大对象时，object store 增长速度可能超过 IO worker 写磁盘速度。

默认配置中：

```cpp
max_io_workers = 4
```

所以可能出现：

```text
actor 产生对象速度
  >
IO worker spill 到磁盘速度
  >
object store 仍然被打满
```

### 8.3 磁盘满或 spill 目录不可用

如果 object store 满了，Ray 想 spill 或 fallback 到磁盘，但本地磁盘也满了，就会失败。

Python 侧可能看到：

```text
OutOfDiskError
```

`python/ray/exceptions.py` 中 `OutOfDiskError` 的说明是：object store 满了，并且本地磁盘使用率超过容量阈值，默认约 95%。

### 8.4 fallback allocator 也失败

当 Plasma 创建对象 OOM 后，Ray 会触发 GC、spilling，并等待 `oom_grace_period_s`。

如果仍然没有空间，会尝试 fallback allocator，用磁盘 mmap 分配对象。

如果 fallback 也失败，就会报错。

### 8.5 爆的是 Python heap，不是 object store

spilling 不解决 worker heap 过大。

例如：

```python
objs = ray.get(many_large_refs)
```

普通 Python 对象会反序列化到 driver heap。如果一次 get 太多对象，可能导致 heap OOM。

再比如 100 个 actor 每个都保存一份普通大对象：

```python
self.obj = ray.get(ref)
```

这占用的是 actor Python heap。object spilling 不能把 actor heap 搬到磁盘。

Ray 的 memory monitor 会看节点总内存，包括 worker heap、object store 和 raylet。当超过 `RAY_memory_usage_threshold`，默认 0.95，raylet 可能杀 task / actor，Python 侧可能看到：

```text
OutOfMemoryError
```

## 9. 单节点 100 actor 的具体理解

假设一个节点上有 100 个 actor。

### 9.1 情况一：actor 只是产生对象，暂时没人读取

```text
100 个 actor 产生大对象
driver 只保存 ObjectRef
没有 actor 正在 ray.get 或消费这些对象
```

这时很多对象可能是可 spill 的。

当 primary object bytes 超过阈值时，raylet 会尝试把这些对象 spill 到磁盘。

### 9.2 情况二：actor 同时消费大 NumPy 对象

```text
100 个 actor 同时处理大 NumPy array
每个 actor 都直接映射 object store shared memory
```

这时很多对象可能不可 spill，因为 Plasma ref_count 大于 1。

即使 object store 压力很大，Ray 也不能随便把正在被 actor 读取的 shared memory 搬走。

### 9.3 情况三：actor 处理普通 Python 对象

```text
actor get 普通自定义对象
反序列化成 actor heap 副本
```

普通对象被反序列化成 heap 副本后，actor 对这个 heap 副本的使用通常不会继续占用原始 object store shared memory。

因此，原始 object store 对象可能重新变成可 spill。

但这不代表内存压力消失了，只是压力从 object store 转移到了 actor heap。

### 9.4 情况四：actor 保存 ObjectRef，但不 get

```python
actor.store_ref.remote([ref])
```

actor 保存的是 `ObjectRef`，不是对象数据。

这种情况会保活对象引用链，但不一定阻止 spilling。

原因是：

```text
ObjectRef 保活对象身份
但不等于正在映射 shared memory
```

所以对象仍可能可 spill。

## 10. 如何观察 spilling 发生了多少

Ray 提供几种方式观察 object spilling 的数量、字节数和吞吐。

### 10.1 使用 ray memory --stats-only

用户侧最直接的方式是：

```bash
ray memory --stats-only
```

本地文档 `doc/source/ray-core/objects/object-spilling.rst` 给出的输出类似：

```text
--- Aggregate object store stats across all nodes ---
Plasma memory usage 50 MiB, 1 objects, 50.0% full
Spilled 200 MiB, 4 objects, avg write throughput 570 MiB/s
Restored 150 MiB, 3 objects, avg read throughput 1361 MiB/s
```

这里重点看：

- `Spilled xxx MiB`：累计 spill 出去的字节数。
- `xxx objects`：累计 spill 的对象数。
- `avg write throughput`：平均写 spill 存储的吞吐。
- `Restored xxx MiB`：累计从 spill 存储 restore 回来的字节数。
- `Plasma memory usage`：当前 object store 中仍占用的 shared memory。

注意：`Spilled xxx MiB` 是累计值，不等于当前磁盘上仍然存在多少 spilled object。对象 out of scope 后，spill 文件可能会被删除。

### 10.2 查看 raylet 日志

当 spilling 发生时，raylet 日志会打印类似信息。

日志位置通常是：

```bash
/tmp/ray/session_latest/logs/raylet.out
```

典型日志：

```text
Spilled 50 MiB, 1 objects, write throughput 230 MiB/s
Restored 50 MiB, 1 objects, read throughput 505 MiB/s
```

这些日志来自 `src/ray/raylet/local_object_manager.cc`。当 `OnObjectSpilled()` 更新累计计数后，Ray 会按阈值输出 spill 汇总日志。

### 10.3 源码中的统计字段

`LocalObjectManager::FillObjectStoreStats()` 会把 spilling 统计写入 `GetNodeStatsReply.store_stats`：

```cpp
stats->set_spill_time_total_s(spill_time_total_s_);
stats->set_spilled_bytes_total(spilled_bytes_total_);
stats->set_spilled_objects_total(spilled_objects_total_);
stats->set_restore_time_total_s(restore_time_total_s_);
stats->set_restored_bytes_total(restored_bytes_total_);
stats->set_restored_objects_total(restored_objects_total_);
stats->set_object_store_bytes_primary_copy(pinned_objects_size_);
stats->set_num_object_store_primary_copies(local_objects_.size());
```

`python/ray/_private/internal_api.py` 中 `store_stats_summary()` 会把这些字段格式化成 `ray memory` 看到的输出：

```text
Spilled {spilled_bytes_total} MiB, {spilled_objects_total} objects
Restored {restored_bytes_total} MiB, {restored_objects_total} objects
```

所以 `ray memory --stats-only` 本质上就是读取各节点的 object store stats，再做汇总展示。

### 10.4 Metrics 视角

`LocalObjectManager::RecordMetrics()` 还会记录 spilling 相关 metrics，包括：

- 当前 pinned 对象数量。
- pending spill 对象数量。
- pending restore 对象数量。
- spilled bytes。
- restored bytes。
- spill / restore 请求数量。
- spill / restore 吞吐。

其中 object store memory gauge 会用 `Location=SPILLED` 记录当前仍在 spill 存储中的字节数：

```cpp
object_store_memory_gauge_.Record(
    spilled_bytes_current_,
    {{stats::LocationKey, "SPILLED"}});
```

这类指标通常可以通过 Ray dashboard / metrics 系统查看，适合长期监控。

### 10.5 观察时要区分累计值和当前值

观察 spilling 时要区分两类数：

| 指标 | 含义 |
| --- | --- |
| `spilled_bytes_total` | 从进程启动以来累计 spill 过多少字节 |
| `spilled_objects_total` | 从进程启动以来累计 spill 过多少对象 |
| `restored_bytes_total` | 累计 restore 回来多少字节 |
| `restored_objects_total` | 累计 restore 回来多少对象 |
| `spilled_bytes_current_` | 当前仍在 spill 存储中的对象字节数 |
| `object_store_bytes_primary_copy` | 当前 raylet 管理的 primary copy shared memory 字节数 |

因此，如果看到：

```text
Spilled 200 GiB
```

不代表当前磁盘一定还占着 200 GiB。它可能只是历史累计 spill 量。

如果要看当前 object store 压力，更应该同时看：

- `Plasma memory usage`
- `% full`
- `% needed`
- `Location=SPILLED` 对应的当前 spilled bytes
- raylet 日志中是否持续出现 spilling / restoring

## 11. 实用判断规则

可以用下面规则判断：

| 场景 | 是否可 spill | 原因 |
| --- | --- | --- |
| driver 只持有 `ObjectRef` | 通常可 | 没有直接映射 shared memory |
| actor 只保存嵌套传来的 `ObjectRef` | 通常可 | 保存 ref 不等于读取数据 |
| task 返回大对象，caller 暂时不 get | 通常可 | result ref 保活对象，但数据没人映射 |
| 对象正在 `ray.put` 写入 | 不可 | 还没 sealed |
| actor 正在读取 NumPy 参数 | 不可 | zero-copy 映射 shared memory |
| driver 保存 `ray.get(np_ref)` 返回的 NumPy array | 不可 | NumPy array 可能直接指向 shared memory |
| 普通对象 `obj = ray.get(ref)` | 通常仍可 | `obj` 是 Python heap 副本 |
| object store 满但磁盘也满 | spilling 失败 | 可能 `OutOfDiskError` |
| worker heap 爆了 | spilling 无效 | 可能 `OutOfMemoryError` |

## 12. 直接硬传大 Torch / NumPy 对象时的影响

有时用户不想显式 `ray.put`，而是直接把大对象传给 actor：

```python
actor.method.remote(big_obj)
```

这时要注意：如果参数足够大，Ray 并不是全程只走 Python heap。源码 `python/ray/_raylet.pyx::prepare_args_internal()` 中，非 `ObjectRef` 参数会先被序列化；如果太大，不能 inline 到 task RPC，就会被隐式放入 object store：

```text
put_serialized_object_and_increment_local_ref(...)
```

可以理解为：

```text
actor.method.remote(big_obj)
  ~= actor.method.remote(ray.put(big_obj))
```

区别是这个 `ray.put` 是 Ray 内部临时生成的，用户拿不到这个中间 `ObjectRef`。

### 12.1 大 Torch tensor 默认行为

示例：

```python
actor.method.remote(big_torch_tensor)
```

默认情况下，PyTorch tensor 的 zero-copy serialization 是关闭的。文档 `doc/source/ray-core/objects/serialization.rst` 说明，PyTorch tensor 的 zero-copy 是可选特性，需要设置：

```bash
RAY_ENABLE_ZERO_COPY_TORCH_TENSORS=1
```

因此默认大 Torch tensor 直接传给 actor 时，大致路径是：

```text
driver heap: big_torch_tensor
  |
  | 序列化
  v
object store: 临时参数对象
  |
  | actor 执行时取参数
  v
actor heap: 反序列化后的 torch.Tensor
```

这意味着：

- object store 仍然会参与，作为大参数的临时暂存。
- actor 收到的 tensor 通常主要占 actor heap。
- task / actor method 结束后，如果没有其他引用，临时 object store 参数对象可以释放。
- 如果并发提交很多 actor method，仍可能同时产生多份临时 object store 参数对象。

顺序调用时：

```python
for actor in actors:
    ray.get(actor.method.remote(big_torch_tensor))
```

通常同一时间临时 object store 参数对象较少。

并发调用时：

```python
refs = [actor.method.remote(big_torch_tensor) for actor in actors]
ray.get(refs)
```

可能同时产生 `n` 份隐式 object store 参数对象。Ray 不会因为它们来自同一个 Python tensor 就自动去重。

### 12.2 Torch 开启 zero-copy 后

如果启用：

```bash
RAY_ENABLE_ZERO_COPY_TORCH_TENSORS=1
```

Ray 会尝试把 PyTorch tensor 转成 NumPy 视图，并利用 pickle5 的 out-of-band buffer 做 zero-copy sharing。

这种情况下，行为会更接近 NumPy：

- actor 收到的 tensor 可能关联 object store shared memory。
- 如果 actor 长期保存 tensor，可能长期 pin object store。
- 如果多个同节点 actor 都持有 zero-copy tensor view，对象可能不可 spill。

这个特性适合只读 tensor。因为 PyTorch 本身没有原生 read-only tensor 语义，文档也提示需要谨慎使用。

### 12.3 大 NumPy array 的行为

示例：

```python
actor.method.remote(big_np_array)
```

大 NumPy 参数也会隐式进 object store。但 NumPy 和默认 Torch 的差异是：NumPy 在 Ray 中天然走 pickle5 out-of-band buffer 和 zero-copy 反序列化。

actor method 执行期间，actor 里拿到的 NumPy array 可能直接指向 object store shared memory：

```text
driver heap: big_np_array
  |
  | 序列化 / 隐式 put
  v
object store: 临时参数对象
  |
  | zero-copy 反序列化
  v
actor: NumPy array view 指向 shared memory
```

这时：

```text
raylet pin: 1
actor NumPy view: +1
Plasma ref_count > 1
=> 不可 spill
```

如果 actor 只是临时使用这个 NumPy array，method 结束后没有保存它，那么临时参数对象之后可能释放或重新变成可 spill。

如果 actor 保存：

```python
self.arr = arr
```

那么这个 NumPy view 可能长期 pin object store shared memory。即使用户没有显式 `ray.put`，这个隐式 put 出来的临时对象也可能因为 actor 持有 zero-copy view 而长期不能释放。

### 12.4 直接硬传大对象的风险

直接硬传大对象的问题不是“完全不会进入 object store”，而是：

```text
它会隐式进入 object store，
但用户容易误以为只是普通 RPC 参数或 heap copy。
```

风险包括：

- 并发硬传同一个大对象时，Ray 可能隐式 put 出多份 object store 参数对象。
- Torch 默认主要会转成 actor heap 压力，但 object store 仍会短暂承压。
- NumPy 可能 zero-copy pin object store，导致临时参数对象并不临时。
- 对象产生速度超过 spilling 速度时，仍可能触发 object store full。
- actor 长期保存 NumPy view 或开启 zero-copy 的 Torch tensor，会降低可 spill 对象数量。

更推荐的写法是显式 put 一次：

```python
ref = ray.put(big_tensor_or_array)
refs = [actor.method.remote(ref) for actor in actors]
ray.get(refs)
```

这样可以避免同一个大对象被隐式 put 出 `n` 份。

如果 actor 需要保存或转发引用，而不是立即拿到值，则应该嵌套传递：

```python
actor.method.remote([ref])
```

这样 actor 收到的是 `ObjectRef` 本身，而不是被自动解引用后的对象值。

## 13. 手动 free object store 对象的语义和限制

如果用户非常确定某个对象后续不会再被任何 task / actor / driver 使用，可以使用 Ray 的低层 API 手动释放 object store 对象：

```python
from ray._private.internal_api import free

free([ref], local_only=False)
```

但这个 API 需要谨慎使用：

- 它是 private API。
- 当前源码中标记为 deprecated。
- 如果后续还有 actor / task 使用这个对象，会导致访问失败。

文档 `doc/source/ray-core/objects/serialization.rst` 只在显式序列化 `ObjectRef` 这种特殊场景下提到它，用来避免对象因为手动序列化 `ObjectRef` 而长期 pin 住。

### 13.1 local_only 参数

`free` 有一个参数：

```python
free([ref], local_only=False)
```

含义是：

| 参数 | 含义 |
| --- | --- |
| `local_only=True` | 只尝试删除本节点 object store 中的副本 |
| `local_only=False` | 向集群所有 object store 传播删除请求 |

通常如果你想确认整个集群都不再需要这个对象，应该使用：

```python
free([ref], local_only=False)
```

### 13.2 free 是逻辑删除，不保证物理内存立刻下降

这是最重要的一点：

```text
free 调用成功
不等于 object store 内存一定立刻下降。
```

源码 `CoreWorker::DeleteImpl()` 会把对象标记成：

```cpp
RayObject(rpc::ErrorType::OBJECT_FREED)
```

所以后续再访问这个 `ObjectRef`，通常会报：

```text
ObjectFreedError
```

这说明对象在 Ray 语义上已经被手动 free，后续不应该再访问。

但是 Plasma store 真正删除对象时，还有一个本地使用计数限制。`ObjectLifecycleManager::DeleteObject()` 中，如果对象仍然在被 Plasma client 使用：

```cpp
if (entry->ref_count_ != 0) {
  earger_deletion_objects_.emplace(object_id);
  return PlasmaError::ObjectInUse;
}
```

也就是说：

```text
对象还在被 worker / actor / driver 映射使用
  -> 先标记为待删除
  -> 不能立刻物理删除
  -> 等 Plasma ref_count 降到 0
  -> 再真正删除 shared memory
```

因此要强调：

```text
即使调用 free，也可能一时删不掉 object store 中那块物理内存。
```

### 13.3 NumPy zero-copy 下尤其容易“删不掉”

示例：

```python
ref = ray.put(np.zeros(...))
arr = ray.get(ref)
free([ref], local_only=False)
```

这里 `arr` 可能直接指向 object store shared memory。

调用 `free([ref])` 后：

```text
ray.get(ref) 后续可能会报 ObjectFreedError
```

但只要：

```python
arr
```

还活着，Plasma 侧 ref_count 可能仍然大于 0，shared memory 不一定立刻释放。

需要：

```python
del arr
```

或者让持有这个 view 的 actor / worker 释放对应对象后，物理内存才可能真正下降。

### 13.4 如何确认是否真的 free 了

可以分两层确认。

第一层：确认逻辑上已经 free。

```python
from ray._private.internal_api import free
import ray

free([ref], local_only=False)

try:
    ray.get(ref)
except ray.exceptions.ObjectFreedError:
    print("logically freed")
```

如果 `ray.get(ref)` 报 `ObjectFreedError`，说明这个对象在 Ray 语义上已经被手动释放，后续不应该再访问。

第二层：确认 object store 物理内存是否下降。

```bash
ray memory --stats-only
```

重点看：

```text
Plasma memory usage ...
```

如果 Plasma memory usage 下降，说明 shared memory 中的对象数据确实释放了。

如果没有下降，常见原因包括：

- 还有 NumPy array / zero-copy Torch tensor view 活着。
- actor method 正在使用该对象作为参数。
- 某个 worker / driver 仍然映射着该 Plasma 对象。
- 删除请求是异步传播的，还没有完成。
- 对象已经 spilled，`Spilled xxx MiB` 是累计值，不会因为删除历史记录而下降。

### 13.5 判断表

| 现象 | 说明 |
| --- | --- |
| `ray.get(ref)` 报 `ObjectFreedError` | 逻辑上已经 free |
| `ray memory --stats-only` 中 Plasma memory usage 下降 | object store shared memory 真的释放了 |
| `ray.get(ref)` 报 freed，但 Plasma memory 没下降 | 可能还有进程正在映射对象，或删除仍在等待 |
| NumPy view / zero-copy tensor 还活着 | 物理内存可能删不掉 |
| `local_only=True` | 只影响本节点副本，不代表集群其他节点副本都删除 |

### 13.6 使用建议

`free` 适合非常少数、用户能确定生命周期的场景。例如：

- 你显式序列化了 `ObjectRef`，导致 Ray 无法正常靠引用计数释放。
- 你确定所有下游 actor / task 都不会再访问该对象。
- 你愿意接受后续访问直接失败。

普通情况下，更推荐依靠 Ray 的引用计数自动释放，并通过以下方式减少 object store 压力：

- 删除不再需要的 `ObjectRef`。
- 分批 `ray.get`。
- 避免长期保存 NumPy zero-copy view。
- 显式 `ray.put` 一次后复用，避免并发硬传生成多份临时对象。

## 14. 核心总结

Object Spilling 的核心可以概括为：

```text
Ray 只 spill 那些：
  已经完整写入 object store，
  当前没人直接使用 shared memory，
  但仍被 ObjectRef / owner / borrower 体系保活的 primary copy。
```

它不是：

```text
对象没人引用了才 spill。
```

对象没人引用时应该被释放，而不是 spill。

也不是：

```text
只要 object store 满了就一定能 spill。
```

如果对象都还在被使用、spilling 太慢、磁盘不够、或者真正爆的是 worker heap，仍然可能 OOM。
