# Ray Namespace 与 Named Actor 源码笔记

本文整理 Ray namespace 的内部机制，以及它为什么会影响 named actor 的创建和查找。后半部分再用 `RolloutTraceStore` 的监控访问问题做具体分析。

## 1. Ray namespace 先解决什么问题

Ray namespace 可以先理解成一层“名字解析作用域”。

它主要影响 named actor、named placement group 这类“按名字查找”的对象。以 named actor 为例，Ray 不是只按 actor name 建全局索引，而是按下面这个二元组建索引：

```text
(ray_namespace, actor_name)
```

所以同一个 namespace 内不能有两个同名 actor，但不同 namespace 内可以存在同名 actor。

一个简化模型是：

```text
named_actors_[ray_namespace][actor_name] = actor_id
```

因此：

```python
ray.get_actor("worker")
```

不是在整个 Ray cluster 里全局搜索 `worker`，而是在“当前 job 的默认 namespace”中查找：

```text
(current_job_namespace, worker)
```

如果要查其他 namespace，必须显式传：

```python
ray.get_actor("worker", namespace="other_namespace")
```

## 2. `ray.init(namespace=...)` 设置当前 job 的默认 namespace

`ray.init(namespace="xxx")` 设置的是当前 driver/job 的默认 namespace。

如果用户没有显式传 namespace，Ray 会在 `JobConfig` 中给这个 job 生成一个 UUID 作为匿名 namespace。

源码位置：

- `python/ray/job_config.py:224`

关键代码：

```python
if self.ray_namespace is None:
    pb.ray_namespace = str(uuid.uuid4())
else:
    pb.ray_namespace = self.ray_namespace
```

这意味着两个不同 driver 即使连接到同一个 Ray cluster，如果都没有显式传 `namespace`，它们通常也会处在不同的匿名 namespace 中。

当前 job 的 namespace 可以这样看：

```python
ray.get_runtime_context().namespace
```

源码位置：

- `python/ray/runtime_context.py:452`

## 3. Actor 创建时 namespace 从哪里来

创建 actor 时有两个 namespace 来源：

1. `Actor.options(namespace=...)` 显式指定的 actor namespace。
2. 当前 job config 里的默认 namespace。

优先级是：

```text
Actor.options(namespace=...) > 当前 job namespace
```

Python 侧创建 actor 时，会把 `name` 和 `namespace` 传入 core worker。

源码位置：

- `python/ray/actor.py:2054`

关键参数：

```python
actor_id = worker.core_worker.create_actor(
    ...
    name if name is not None else "",
    namespace if namespace is not None else "",
    ...
)
```

C++ core worker 里，如果 actor option 的 namespace 为空，就回退到当前 job namespace。

源码位置：

- `src/ray/core_worker/core_worker.cc:2105`

关键代码：

```cpp
const auto ray_namespace =
    actor_creation_options.ray_namespace.empty()
        ? worker_context_->GetCurrentJobConfig().ray_namespace()
        : actor_creation_options.ray_namespace;
```

所以这两种写法效果等价：

```python
ray.init(namespace="xtuner_rollout") # 这个意思只是设置默认值，和是否连接到同一集群没有关系
RolloutTraceStore.options(name="rollout_trace_store").remote() # 会通过前面默认值创建 actor
```

```python
ray.init() # 不设置默认值，而是内部自动创建
RolloutTraceStore.options(
    name="rollout_trace_store",
    namespace="xtuner_rollout", # 显著式写
).remote()
```

第二种写法里，actor 自己的 namespace 已经显式指定，因此启动程序的默认 namespace 是什么不重要。

## 4. GCS 怎么保存 named actor

Actor 创建任务到 GCS 后，`GcsActorManager::RegisterActor` 会取出 actor creation task spec 里的 `ray_namespace`。

源码位置：

- `src/ray/gcs/actor/gcs_actor_manager.cc:694`

关键代码：

```cpp
std::string ray_namespace = actor_creation_task_spec.ray_namespace();
RAY_CHECK(!ray_namespace.empty())
    << "`ray_namespace` should be set when creating actor in core worker.";
```

如果 actor 有 name，GCS 会按 namespace 分组注册。

源码位置：

- `src/ray/gcs/actor/gcs_actor_manager.cc:704`

关键代码：

```cpp
auto &actors_in_namespace = named_actors_[actor->GetRayNamespace()];
auto it = actors_in_namespace.find(actor->GetName());
if (it == actors_in_namespace.end()) {
    actors_in_namespace.emplace(actor->GetName(), actor->GetActorID());
} else {
    return Status::AlreadyExists(...);
}
```

这就是 named actor 的核心约束：

```text
同一 namespace 内 name 唯一
不同 namespace 内可以有相同 name
```

## 5. `ray.get_actor(name)` 为什么默认只查当前 namespace

Python 入口：

- `python/ray/_private/worker.py:3219`

如果用户不传 namespace，Python 层会传空字符串给 core worker：

```python
return worker.core_worker.get_named_actor_handle(name, namespace or "")
```

C++ core worker 再把空字符串替换成当前 job namespace。

源码位置：

- `src/ray/core_worker/core_worker.cc:2631`

关键代码：

```cpp
return actor_manager_->GetNamedActorHandle(
    name,
    ray_namespace.empty()
        ? worker_context_->GetCurrentJobConfig().ray_namespace()
        : ray_namespace,
    CurrentCallSite(),
    rpc_address_);
```

因此：

```python
ray.get_actor("x")
```

等价于：

```python
ray.get_actor("x", namespace=ray.get_runtime_context().namespace)
```

而不是跨 namespace 搜索。

## 6. GCS 查找 named actor 的方式

Core worker 会通过 actor manager 去 GCS 查 named actor。

源码位置：

- `src/ray/core_worker/actor_management/actor_manager.cc:63`

先查本地 cache：

```cpp
ActorID actor_id = GetCachedNamedActorID(GenerateCachedActorName(ray_namespace, name));
```

cache 不命中时，向 GCS 查：

```cpp
const auto status = gcs_client_->Actors().SyncGetByName(
    name, ray_namespace, actor_table_data, task_spec);
```

GCS 侧最终是按 namespace 先找一层，再按 name 找。

源码位置：

- `src/ray/gcs/actor/gcs_actor_manager.cc:881`

关键代码：

```cpp
ActorID GcsActorManager::GetActorIDByName(
    const std::string &name,
    const std::string &ray_namespace) const {
  ActorID actor_id = ActorID::Nil();
  auto namespace_it = named_actors_.find(ray_namespace);
  if (namespace_it != named_actors_.end()) {
    auto it = namespace_it->second.find(name);
    if (it != namespace_it->second.end()) {
      actor_id = it->second;
    }
  }
  return actor_id;
}
```

所以 named actor 查找的真实 key 是：

```text
(ray_namespace, name)
```

## 7. namespace 不是访问隔离

`ray.init(address="auto", namespace="A")` 只是把当前监控程序的默认 namespace 设置成 `A`。

它不会限制这个程序只能访问 `A`。只要显式传 namespace，仍然可以查其他 namespace：

```python
ray.get_actor("x", namespace="B")
```

所以 namespace 更像“默认名字查找路径”，不是权限边界，也不是资源隔离机制。

如果调用方已经持有 actor handle，即使当前 job namespace 不同，也可以继续调用该 actor。

## 8. 回到 `RolloutTraceStore` 的现象

当前 store 的创建逻辑大致是：

```python
_STORE_NAME = "rollout_trace_store"

def get_store():
    try:
        return ray.get_actor(_STORE_NAME)
    except ValueError:
        pass

    return RolloutTraceStore.options(name=_STORE_NAME).remote()
```

这里创建 actor 时只传了 `name`，没有传 `namespace`。

根据前面的规则，actor 会注册到“创建它的当前 job namespace”中：

```text
(creator_job_namespace, rollout_trace_store)
```

### 8.1 为什么同一个程序里能拿到

同一个训练程序里再次调用：

```python
ray.get_actor(_STORE_NAME)
```

调用方的当前 job namespace 仍然是创建 actor 时的 namespace，因此查的是：

```text
(creator_job_namespace, rollout_trace_store)
```

这和注册时的 key 一致，所以能拿到。

### 8.2 为什么外部监控程序拿不到

外部监控程序通过：

```python
ray.init(address="auto")
ray.get_actor(_STORE_NAME)
```

连接到同一个 cluster，但这个监控程序是另一个 driver/job。由于没有显式传 namespace，它会拥有自己的默认 namespace，通常是另一个匿名 UUID。

于是监控程序查的是：

```text
(monitor_job_namespace, rollout_trace_store)
```

而真实 actor 在：

```text
(creator_job_namespace, rollout_trace_store)
```

两个 namespace 不同，所以查找失败。

`address="auto"` 只负责连接到已有 Ray cluster，不会自动继承另一个 driver 的 namespace。

## 9. 为什么 `list_actors` 兜底能找到

`ray.util.state.list_actors` 查的是 actor 状态表，不是走 named actor 的“当前 namespace 解析”。

所以它可以返回 actor 的元数据，例如：

```text
name = rollout_trace_store
ray_namespace = creator_job_namespace
state = ALIVE
```

拿到真实 namespace 后，再显式查：

```python
store = ray.get_actor(
    "rollout_trace_store",
    namespace="creator_job_namespace",
)
```

就能命中：

```text
named_actors_[creator_job_namespace][rollout_trace_store]
```

因此下面这种兜底写法是合理的：

```python
def get_store():
    try:
        return ray.get_actor(_STORE_NAME)
    except ValueError:
        pass

    from ray.util.state import list_actors

    actors = list_actors(filters=[("name", "=", _STORE_NAME)], detail=True)
    if not actors:
        raise RuntimeError(f"cannot find ray actor: {_STORE_NAME}")

    actor = actors[0]
    namespace = (
        actor.get("ray_namespace")
        if isinstance(actor, dict)
        else actor.ray_namespace
    )
    return ray.get_actor(_STORE_NAME, namespace=namespace)
```

但这个写法属于“自动发现”。如果同一个 cluster 里有多个 namespace 都有同名 store，它需要额外判断到底要连哪一个。

## 10. 推荐解决方式

### 10.1 固定共享 namespace

如果 `RolloutTraceStore` 是训练程序和外部监控程序共同访问的共享服务，建议创建 actor 时显式指定 namespace：

```python
_STORE_NAME = "rollout_trace_store"
_STORE_NAMESPACE = "xtuner_rollout"

def get_store():
    try:
        return ray.get_actor(_STORE_NAME, namespace=_STORE_NAMESPACE)
    except ValueError:
        return RolloutTraceStore.options(
            name=_STORE_NAME,
            namespace=_STORE_NAMESPACE,
        ).remote()
```

监控程序可以显式查：

```python
ray.init(address="auto")
store = ray.get_actor("rollout_trace_store", namespace="xtuner_rollout")
```

也可以把监控程序默认 namespace 设置成相同值：

```python
ray.init(address="auto", namespace="xtuner_rollout")
store = ray.get_actor("rollout_trace_store")
```

这两种写法等价。更推荐第一种，因为调用点明确写出了要查哪个 namespace。

### 10.2 多训练任务并行时带 run id

如果同一个 Ray cluster 里可能同时运行多个训练任务，不建议所有任务共用：

```text
(xtuner_rollout, rollout_trace_store)
```

否则会发生 named actor 冲突。

可以把 run id 放到 namespace：

```python
_STORE_NAME = "rollout_trace_store"
_STORE_NAMESPACE = f"xtuner_rollout_{run_id}"
```

也可以把 run id 放到 actor name：

```python
_STORE_NAME = f"rollout_trace_store_{run_id}"
_STORE_NAMESPACE = "xtuner_rollout"
```

核心原则是保证 `(namespace, name)` 在同一个 Ray cluster 内唯一。

### 10.3 监控多个 namespace

如果监控程序不知道训练任务 namespace，需要自动发现多个 store，可以先扫 actor 状态表：

```python
from ray.util.state import list_actors

def find_trace_stores():
    actors = list_actors(filters=[("name", "=", "rollout_trace_store")], detail=True)
    stores = []
    for actor in actors:
        namespace = (
            actor.get("ray_namespace")
            if isinstance(actor, dict)
            else actor.ray_namespace
        )
        stores.append(ray.get_actor("rollout_trace_store", namespace=namespace))
    return stores
```

这种写法适合监控工具自动发现，但需要处理多个候选 actor 的选择问题。

## 11. detached actor 与生命周期

当前这类写法：

```python
RolloutTraceStore.options(name=_STORE_NAME).remote()
```

没有设置 `lifetime="detached"`，因此 actor 是非 detached named actor。它虽然有名字，但生命周期仍然受 owner 影响。

如果训练 driver / owner 退出，这个 actor 不保证继续存在。

如果希望训练程序退出后监控程序仍然能访问 store，需要评估是否使用 detached actor：

```python
RolloutTraceStore.options(
    name=_STORE_NAME,
    namespace=_STORE_NAMESPACE,
    lifetime="detached",
).remote()
```

但 detached actor 也需要明确清理策略，否则容易在 Ray cluster 中残留旧的 store。

## 12. 排障清单

外部监控程序 `ray.get_actor(_STORE_NAME)` 失败时，可以按下面顺序查：

1. 确认监控程序连接的是同一个 Ray cluster。
2. 打印 `ray.get_runtime_context().namespace`，确认当前默认 namespace。
3. 用 `ray.util.state.list_actors(filters=[("name", "=", _STORE_NAME)], detail=True)` 查 actor 是否存在。
4. 检查返回结果中的 `ray_namespace`。
5. 用 `ray.get_actor(_STORE_NAME, namespace=ray_namespace)` 精确查找。
6. 如果需要稳定访问，创建 actor 时显式设置 `Actor.options(namespace=...)`。
7. 如果多任务并行，确认 `(namespace, name)` 不冲突。
8. 如果训练进程已退出，确认 actor 是否需要 `lifetime="detached"`。

