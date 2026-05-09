# Ray GCS 和 Raylet 架构笔记

## 1. 先建立两个核心概念

### 1.1 GCS 是什么

GCS 是 `Global Control Service`，可以先理解成 Ray 集群的“全局登记处”或“集群元数据库”。

它主要管理集群级别的控制信息，例如：

- 集群有哪些节点。
- 节点是否存活。
- actor 的全局信息、位置和状态。
- placement group 信息。
- job 信息。
- runtime env 和 internal KV。
- 资源信息和控制面 pubsub。

官方文档里直接说：GCS manages cluster-level metadata，并且提供 actor、placement group 和 node management 等集群级操作。

关键点：

- GCS 只在 head node 上是核心单例服务。
- GCS 是控制面，不是普通 task 的执行中转站。
- GCS 不搬运用户对象数据。
- 默认情况下，GCS 数据存在内存里；GCS 失败会导致整个集群失败，除非启用 GCS fault tolerance。

### 1.2 Raylet 是什么

Raylet 是每个 Ray 节点上的本地管理进程，可以理解成“每台机器的本地管家”。

它主要负责本机上的事情：

- 管理本机 worker 进程。
- 管理本机 CPU/GPU 等资源。
- 接收 worker lease 请求，决定本机能不能执行任务。
- 启动或复用 worker。
- 管理本机 object store。
- 通过 object manager 和其他节点传输对象。
- 向 GCS 注册自己、汇报状态。
- 处理本节点失败、worker 失败、对象本地事件等。

当前源码里 raylet 启动日志明确写：

```text
Raylet consists of node_manager and object_manager.
```

所以 raylet 可以拆成两个核心角色：

```text
raylet
  NodeManager   = 本机调度、资源、worker 生命周期
  ObjectManager = 本机对象管理、跨节点对象传输
```

### 1.3 最简模型

```text
GCS
  管全局事实：集群有什么、actor 在哪、节点是否活着、资源/元数据

每个节点的 raylet
  管本机执行：worker、资源、本机 object store、对象传输
```

一句话：

> GCS 管“全局登记和控制面”，raylet 管“本机执行和对象管理”。

## 2. 启动节点时，GCS 和 raylet 怎么配合

### 2.1 节点启动流程

一个 Ray 节点启动时，本机 raylet 会启动，然后把自己的信息注册到 GCS。

大致流程：

```text
raylet 启动
  -> 初始化 NodeManager 和 ObjectManager
  -> 准备 node_manager_port / object_manager_port 等地址信息
  -> 向 GCS 注册当前节点
  -> GCS 记录这个节点
```

### 2.2 GCS 在这里做什么

GCS 记录集群节点表，例如：

- 这个节点的 node id。
- 节点地址。
- node manager port。
- object manager port。
- runtime env agent port。
- metrics agent port。
- dashboard agent port。
- 节点 alive/dead 状态。

### 2.3 raylet 在这里做什么

raylet 管理本机服务和本机资源。它启动后会对外提供本节点的调度和对象管理能力。

可以理解成：

```text
GCS    = 记录“这台机器加入集群了”
raylet = 让“这台机器真的可以接任务、跑 worker、存对象”
```

## 3. 普通函数 f.remote() 时谁参与

示例：

```python
import ray

@ray.remote
def f(x):
    return x + 1

ref = f.remote(1)
```

### 3.1 主执行路径

普通 task 的主路径主要是：

```text
driver/worker 进程
  -> CoreWorker 构造 task spec
  -> 向某个 raylet 发 RequestWorkerLease
  -> raylet 选择/启动/复用 worker
  -> CoreWorker 向目标 worker 发 PushTask
  -> worker 执行函数
```

### 3.2 GCS 是否参与

普通 `f.remote()` 的每次调度执行主路径通常不经过 GCS。

但 GCS 可能参与一些辅助元数据：

- 第一次使用 remote function 时，函数序列化信息会放到 GCS KV，远端 worker 后续可以取函数定义。
- GCS 维护节点表、资源信息、控制面信息。
- 如果有节点变化、故障、资源状态更新，GCS 会参与控制面处理。

所以更准确地说：

```text
普通 task 执行路径：
  不走 GCS 中转

普通 task 元数据环境：
  可能依赖 GCS
```

### 3.3 raylet 做什么

raylet 是普通 task 调度落地的关键角色：

- 收到 `RequestWorkerLease`。
- 根据本机资源、worker 状态、调度策略决定是否能跑。
- 能跑就分配 worker。
- 不能跑可能拒绝或让请求去别的 raylet。普通 task 的这个路径可以理解成：不是 GCS 说：“去那个 raylet”，而是 raylet 自己根据它掌握的集群资源视图说：“我这里不合适，你去那个 raylet”
- 管理 worker 生命周期。

简化理解：

```text
f.remote() 不是问 GCS “请帮我跑任务”
而是问 raylet “请给我一个能跑这个任务的 worker”
```

## 4. Actor.remote() 创建 actor 时谁参与

示例：

```python
@ray.remote
class A:
    def ping(self):
        return "pong"

a = A.remote()
```

### 4.1 为什么 actor 创建要更依赖 GCS

actor 是长期存在的、有身份、有位置、有状态的实体。Ray 需要全局知道：

- actor id 是什么。
- actor 是否 alive。
- actor 跑在哪个节点。
- actor worker 地址是什么。
- named actor 的名字如何解析。
- actor 是否需要重启、重建。

这些信息是全局信息，所以 actor 创建会经过 GCS actor manager。

### 4.2 GCS 做什么

GCS 负责 actor 的全局登记和控制面调度：

```text
driver CoreWorker
  -> 向 GCS 注册/创建 actor
  -> GCS ActorManager / ActorScheduler 处理 actor creation
  -> 找合适 raylet 创建 actor worker
  -> actor 创建成功后，GCS 记录 actor 地址和状态
```

### 4.3 raylet 做什么

raylet 负责本机 actor worker 的实际落地：

- 接收 actor creation 的 worker lease。
- 启动或复用 worker。
- 给 actor 分配资源。
- 让 worker 执行 actor constructor。
- 后续管理这个 actor worker 的生命周期。

简化理解：

```text
GCS    = 负责“这个 actor 是谁、应该创建、在哪登记”
raylet = 负责“在本机真的拉起 actor worker 并分配资源”
```

## 5. actor.method.remote() 时谁参与

示例：

```python
ref = a.ping.remote()
```

### 5.1 通常不需要每次经过 GCS

actor 创建完成后，调用方通常已经知道 actor worker 地址，后续 actor method 调用会直接发给 actor worker。

大致是：

```text
caller CoreWorker
  -> 直接向 actor worker 发 PushTask
  -> actor worker 执行方法
```

### 5.2 GCS 做什么

GCS 不作为每次 actor method 的中转站。

但 GCS 仍然维护 actor 的全局状态，例如：

- actor 是否 alive。
- actor 地址。
- actor restart/reconstruction 相关状态。
- named actor 查询。

如果 actor 死亡、重启、地址变化，GCS 会重新参与控制面。

### 5.3 raylet 做什么

raylet 不是每个 actor method 的数据中转站，但它仍然管理 actor 所在节点：

- 管理 actor worker 进程。
- 监控 worker 是否异常退出。
- 管理该 actor 占用的资源。
- 参与 actor worker kill/cancel 等本地控制。

## 6. ray.put + ray.get 跨 actor 传对象时谁参与

示例：

```python
@ray.remote
class Producer:
    def make(self):
        obj = b"x" * 1024 * 1024 * 1024
        return ray.put(obj)

@ray.remote
class Consumer:
    def consume(self, ref):
        return len(ray.get(ref))
```

### 6.1 对象数据是否经过 GCS

不经过。

对象数据本体不放 GCS，也不由 GCS 转发。

如果 Producer 和 Consumer 在不同节点：

```text
Producer 所在节点 object store
  -> Producer 所在节点 raylet/ObjectManager
  -> 网络传输 object chunks
  -> Consumer 所在节点 raylet/ObjectManager
  -> Consumer 所在节点 object store
```

### 6.2 GCS 做什么

GCS 在这里最多是间接参与：

- 维护节点表，大家知道节点地址和端口。
- 维护 actor 元信息，知道 actor 在哪里。
- 节点失败时做控制面处理。

但它不做这些事情：

- 不存用户对象数据。
- 不转发对象数据。
- 不在每次 `ray.get` 时搬对象。
- 不作为对象传输代理。

### 6.3 raylet 做什么

raylet 里的 ObjectManager 是对象跨节点传输的主角：

- 本地没有对象时发起 pull。
- 远端有对象时负责 push。
- 按 chunk 传输对象数据。
- 写入本地 object store。
- 和本地 object store / plasma 交互。

简化理解：

```text
GCS    = 告诉大家“集群里有哪些节点”
raylet = 真正把对象从一个节点搬到另一个节点
```

## 7. placement group 时谁参与

示例：

```python
from ray.util.placement_group import placement_group

pg = placement_group([{"GPU": 1}, {"GPU": 1}])
ray.get(pg.ready())
```

### 7.1 GCS 做什么

placement group 是全局资源编排概念，所以 GCS 会深度参与：

- 创建 placement group。
- 记录 placement group 信息。
- 调度 bundle 到不同节点。
- 协调资源预留。
- 查询 placement group 是否 ready。

### 7.2 raylet 做什么

raylet 负责本机 bundle 资源预留和提交：

- Prepare bundle resources。
- Commit bundle resources。
- Cancel resource reserve。
- 在本机把对应资源锁住。

简化理解：

```text
GCS    = 设计和协调全局 placement group 布局
raylet = 在本机真正预留资源
```

## 8. 节点失败时谁参与

### 8.1 raylet 失败

如果某个节点的 raylet 失败，文档说该节点会被标记为 dead，并被视为节点失败。

影响包括：

- 该节点上运行中的 tasks/actors 失败。
- 该节点 worker 进程拥有的对象可能丢失。
- 触发 task/actor/object fault tolerance 机制。

### 8.2 GCS 做什么

GCS 维护节点存活状态：

- 发现或接收节点失败信息。
- 更新节点表。
- 发布控制面事件。
- 让 actor、placement group、resource manager 等模块处理后续恢复。

### 8.3 raylet 做什么

正常情况下，每个 raylet 管理自己的本地 worker 和对象。一旦 raylet 失败，这个节点就等价于从 Ray 集群里消失。

简化理解：

```text
raylet 失败 = 这个 Ray 节点失败
GCS 负责把这个全局事实记录并广播出去
```

## 9. head node / GCS 失败时谁参与

### 9.1 默认行为

默认情况下，GCS 不具备 fault tolerance，因为数据存在内存里。如果 GCS 失败，整个 Ray 集群失败。

文档里明确说：

```text
If it fails, the entire Ray cluster fails.
```

### 9.2 开启 GCS FT 后

如果启用外部 Redis 持久化，GCS 重启后可以从 Redis 恢复数据。

恢复期间不可用的能力包括：

- actor creation/deletion/reconstruction。
- placement group creation/deletion/reconstruction。
- resource management。
- worker node registration。
- worker process creation。

但文档也说明：

```text
running Ray tasks and actors remain alive,
and any existing objects stay available.
```

这说明一个重要事实：

```text
GCS 是控制面核心；
已有运行中的任务、actor、对象不一定每一刻都依赖 GCS 转发。
```

## 10. 总结表

| 场景 | GCS 作用 | raylet 作用 |
| --- | --- | --- |
| 节点启动 | 记录节点信息、端口、alive 状态 | 启动本机 NodeManager/ObjectManager，向 GCS 注册 |
| 普通 `f.remote()` | 维护函数/节点等元数据，不是每次执行中转 | 接收 worker lease，调度/启动/复用 worker |
| 第一次 remote function 使用 | GCS KV 保存函数序列化信息 | worker 执行时配合拉取/运行任务 |
| `Actor.remote()` | actor 全局登记、调度、状态管理 | 在本机创建 actor worker，分配资源 |
| `actor.method.remote()` | 通常不参与每次调用；维护 actor 状态 | 管理 actor worker 生命周期和资源 |
| `ray.put` | 不存对象数据 | 本机 object store/object manager 管对象 |
| 跨节点 `ray.get` | 不搬对象数据 | 两端 ObjectManager 传输对象 chunk |
| placement group | 全局创建、调度、记录 | 本机资源 prepare/commit/cancel |
| 节点失败 | 标记节点 dead，广播/触发恢复 | raylet 失败等价于本节点失败 |
| GCS 失败 | 默认整个集群失败 | 已运行任务/actor 在恢复窗口可能仍活着，但新控制面操作不可用 |

## 11. 一句话记忆

```text
GCS    = 全局登记处，管“集群事实”
raylet = 本地管家，管“本机执行”
```

再精确一点：

```text
GCS 不跑你的函数，不搬你的大对象。
raylet 不保存全局真相，但负责本机调度、worker、object store 和跨节点对象传输。
```

## 12. 源码和文档依据

- `doc/source/ray-core/fault_tolerance/gcs.rst`
  - GCS manages cluster-level metadata。
  - GCS 提供 actor、placement group、node management 等集群级操作。
  - 默认 GCS 不是 fault tolerant，失败会导致整个集群失败。
  - GCS 恢复期间 actor creation、placement group、resource management、node registration、worker process creation 不可用。

- `doc/source/ray-core/fault_tolerance/nodes.rst`
  - Ray cluster 由一个或多个 worker nodes 组成。
  - 每个节点包含 worker processes 和 system processes，例如 raylet。
  - head node 有额外进程，例如 GCS。
  - raylet 失败时，对应节点会被标记 dead。

- `src/ray/gcs/gcs_server.cc`
  - `GcsServer::DoStart` 初始化 `GcsNodeManager`、`GcsResourceManager`、`GcsJobManager`、`GcsPlacementGroupManager`、`GcsActorManager`、`GcsWorkerManager`、`GcsTaskManager` 等模块。

- `src/ray/raylet/node_manager.cc`
  - raylet 启动日志说明：`Raylet consists of node_manager and object_manager.`
  - raylet 启动后调用 `gcs_client_.Nodes().RegisterSelf(...)` 向 GCS 注册自己。

- `src/ray/protobuf/node_manager.proto`
  - `NodeManagerService` 提供 `RequestWorkerLease`、`PrepareBundleResources`、`CommitBundleResources`、`PinObjectIDs`、`GetNodeStats`、`KillLocalActor`、`CancelLocalTask` 等本地节点管理 RPC。

- `src/ray/protobuf/object_manager.proto`
  - `ObjectManagerService` 提供 `Push`、`Pull`、`FreeObjects`。
  - `PushRequest` 里包含 object chunk data，说明对象数据由 ObjectManager 服务传输，不由 GCS 传输。

- `doc/source/ray-core/internals/task-lifecycle.rst`
  - 普通 task 通过 `NormalTaskSubmitter` 向 raylet 发送 `RequestWorkerLease`。
  - 拿到 leased worker 后，通过 `PushTask` 发给 worker 执行。
  - 小对象可以随 RPC 返回，大对象会进入 plasma/object store。

