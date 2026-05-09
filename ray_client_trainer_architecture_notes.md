# Ray Client、启动方式与 RL Trainer 架构记录

本文整理本轮讨论中关于 Ray Client、Ray driver、Ray Client Server、Ray 内部 actor 通信，以及 verl / xtuner RL trainer 架构的结论。

## 1. 基础概念

### 1.1 Ray Driver 是什么

Ray driver 是“发起 Ray 任务的用户程序”。

普通 native 模式下：

```python
import ray

ray.init()
actor = MyActor.remote()
ref = actor.work.remote()
result = ray.get(ref)
```

当前 Python 进程就是 Ray Core 视角下的 driver。它直接向 Ray Core 提交 task、创建 actor、持有 `ObjectRef`、执行 `ray.get()`。

Ray Client 模式下：

```python
ray.init("ray://head-node:10001")
```

当前 Python 进程不是 Ray Core 视角下的 native driver，而是 Ray Client frontend。head 节点上的 Ray Client Server / specific server 会代表它执行 Ray API。

或者简单来说，这种情况下 ray 的 driver 进程并不是当前进程，当前进程是 ray client 进程，Ray Client Server 如果运行在 head 进程，那这个这个 Ray Client Server 进程才是 driver 进程。一定要区分 driver 进程的概念，对于后续非常重要。

### 1.2 Ray Client Server 是什么

Ray Client Server 是 `ray://` 模式的服务端，通常运行在 head 节点。

可以这样理解：

```text
你的 Python 进程
  = client frontend
  = 执行普通 Python 控制流

head 上的 Ray Client Server / specific server
  = server-side proxy driver
  = 持有真实 ObjectRef / ActorHandle
  = 向 Ray Core 提交 task / actor call
```

Ray Client 源码文档中说明：Ray Client 是 gRPC client/server；server 会运行 `ray.init()`，并像普通 Ray driver 一样工作。

在这种模式下，我们启动的 python py 文件就是 ray client 进程，不是 driver 进程，这个 py 进程里面执行的任何和 ray 相关操作都是需要经过 grpc 在 client/server 进行传递数据。也就是这是额外引入的一层 grpc 通信，需要特别重视。

### 1.3 Ray Client gRPC 和 Ray 内部 RPC 的区别

Ray Client gRPC：

```text
用户 Python 进程 <-> Ray Client Server
```

用途是让一个外部 Python 进程通过 `ray://` 控制 Ray 集群。

Ray 内部 RPC：

```text
CoreWorker <-> CoreWorker
CoreWorker <-> raylet
raylet <-> GCS
raylet / object manager <-> object store
```

用途是执行 task、actor method、对象拉取、调度、引用管理等。

这两套东西都可能基于 gRPC，但语义不同。前者是“进集群的门”，后者是“集群内部干活的路”。

### 1.4 trainer 普通代码和 Ray API 的区别

在 Ray Client 模式下：

```python
trainer = Trainer(...)
trainer.fit()
```

`trainer.fit()` 里的普通 Python 控制流仍然在当前 Python 进程中执行。

例如：

```text
for 循环
if 判断
构造 Python list/dict
tokenizer.decode
组 batch
写日志
```

这些都不会自动搬到 Ray Client Server。

但下面这些 Ray API 会通过 Ray Client gRPC 发给 server：

```text
ray.get(...)
ray.put(...)
ray.wait(...)
actor.method.remote(...)
SomeActor.remote(...)
```

因此，Ray Client 模式下最危险的是：client frontend 里的 trainer 主循环频繁 `ray.get` 大对象。

## 2. 三种启动方式

这里讨论三种常见启动方式。

### 2.1 方式一：直接 python，代码内部 ray.init()

示例：

```bash
python train.py
```

代码：

```python
import ray

ray.init()
trainer = Trainer(...)
trainer.fit()
```

如果没有已有 Ray 集群，`ray.init()` 会启动本地 Ray；如果指定 `address="auto"` 或 `RAY_ADDRESS`，则连接已有集群。

通信形态：

```text
train.py Python 进程
  -> native Ray driver
  -> Ray Core
  -> actor / task / object store
```

这种方式没有 Ray Client gRPC 层。

### 2.2 方式二：run_rl.sh，脚本 ray start，然后直接 python

xtuner 的 `run_rl.sh` 大致是：

```bash
ray start --head --port=$RAY_HEAD_PORT --dashboard-port=$RAY_DASHBOARD_PORT ...
python xtuner/v1/train/cli/rl.py --config $CONFIG_PATH
```

但 `xtuner/v1/train/cli/rl.py` 中，如果存在 `RAY_MASTER_ADDR`，会执行：

```python
master_addr = os.getenv("RAY_MASTER_ADDR", "127.0.0.1")
client_port = os.getenv("RAY_CLIENT_PORT", "10001")
ray_head_address = f"ray://{master_addr}:{client_port}"
ray.init(address=ray_head_address)
```

所以这不是普通 native attach，而是 Ray Client 模式。

通信形态：

```text
rl.py Python 进程
  -> Ray Client gRPC
  -> head 上 Ray Client Server / specific server
  -> Ray Core
  -> actor / task / object store
```

如果 `trainer.fit()` 在 `rl.py` 进程中直接执行，那么 trainer 主循环就是跑在 Ray Client frontend 中。

### 2.3 方式三：run_rl_submit.sh，脚本 ray start，然后 ray job submit

xtuner 的 `run_rl_submit.sh` 大致是：

```bash
ray start --head --port=$RAY_HEAD_PORT --dashboard-port=$RAY_DASHBOARD_PORT ...

ray job submit \
  --address="http://127.0.0.1:$RAY_DASHBOARD_PORT" \
  --runtime-env-json="$RUNTIME_ENV_JSON" \
  -- python xtuner/v1/train/cli/rl.py --config $CONFIG_PATH
```

`ray job submit` 走的是 Ray Jobs HTTP API，不是 Ray Client gRPC。

通信形态：

```text
提交端 shell / CLI
  -> Dashboard Jobs HTTP API :8265
  -> JobManager / JobSupervisor
  -> 启动 entrypoint subprocess
  -> entrypoint driver
  -> Ray Core
```

Ray Jobs 的作用是把 entrypoint driver 拉起来、管理日志、状态、停止等。

但要注意：如果 entrypoint 内部的 `rl.py` 又因为 `RAY_MASTER_ADDR` 执行 `ray.init("ray://...")`，那 entrypoint 仍然可能进入 Ray Client 模式。因此 Jobs API 本身不是 Ray Client，但 entrypoint 代码可能再次主动选择 Ray Client。

## 3. 示例代码

### 3.1 不推荐：Ray Client frontend 直接跑 trainer.fit()

```python
import ray

ray.init("ray://head-node:10001")

trainer = Trainer(...)
trainer.fit()
```

如果 `trainer.fit()` 里有：

```python
rollout_batch = ray.get(rollout_ref)
train_info = ray.get(train_ref)
```

大对象会走：

```text
对象所在节点 object store
  -> Ray Client Server
  -> Ray Client gRPC
  -> trainer frontend
```

这会额外增加序列化、拷贝、gRPC chunk、client/server 内存压力。

### 3.2 推荐：Ray Client 只提交 TaskRunner actor

```python
import ray


@ray.remote(num_cpus=1)
class RLTaskRunner:
    def run(self, config_path: str):
        cfg = load_config(config_path)
        trainer = cfg.build()
        trainer.fit()


ray.init("ray://head-node:10001")

runner = RLTaskRunner.remote()
ray.get(runner.run.remote("config.py"))
```

通信形态：

```text
client frontend
  -> Ray Client gRPC
  -> 提交 RLTaskRunner.run

RLTaskRunner actor
  -> 在 Ray 集群内部运行 trainer.fit
  -> 内部 ray.get / actor.method.remote 走 Ray Core
```

这样 Ray Client 只承担入口提交和最终等待，不承担每个训练 step 的大对象传输。

### 3.3 actor 之间传对象不会经过 Ray Client

```python
@ray.remote
class Producer:
    def make(self):
        return big_batch


@ray.remote
class Consumer:
    def consume(self, batch):
        return train(batch)


producer = Producer.remote()
consumer = Consumer.remote()

batch_ref = producer.make.remote()
result_ref = consumer.consume.remote(batch_ref)
```

如果这段提交发生在 client frontend，Ray Client 只传递调用请求和 `ObjectRef` 元信息。大对象本体在 actor / object store 之间流动：

```text
Producer 所在节点
  -> Ray object manager / object store
  -> Consumer 所在节点
```

不会绕回 client frontend。

## 4. 核心结论

### 4.1 Ray Client 只包住 driver/client 层

Ray Client 不会包住集群内部 actor 之间的通信。

```text
client frontend <-> Ray Client Server
```

这层只存在于外部 driver/client 到 Ray 集群入口之间。

actor 内部调用 actor：

```text
Actor A -> Actor B
```

走 Ray Core 内部 actor task，不走 Ray Client gRPC。

### 4.2 大对象 ray.get 本身就有成本

只要调用：

```python
obj = ray.get(big_ref)
```

语义就是把对象取到当前调用方进程可见。

native driver 下也要搬数据：

```text
对象所在节点 object store
  -> driver 所在节点 object store
  -> driver 进程
```

### 4.3 Ray Client 下 ray.get 大对象更糟

Ray Client 下多一层代理：

```text
对象所在节点 object store
  -> Ray Client Server
  -> server 侧 ray.get
  -> gRPC chunk
  -> client frontend
```

所以：

```text
大对象 ray.get 的成本，任何模式都有。
Ray Client 模式更糟，因为多了 server -> client frontend 的 gRPC 传输和序列化。
```

### 4.4 RL trainer 最好不要在 Ray Client frontend 中跑主循环

如果主循环在 Ray Client frontend 中：

```text
produce rollout
get batch
prepare train data
train
sync weights
eval
log
```

这些步骤中的所有 Ray API 都会通过 Ray Client Server 代理。

更合理的是：

```text
外层 client 只提交 runner
runner actor 在集群内部执行 trainer.fit
```

## 5. 三种情况细说

### 5.1 native driver 直接跑 trainer.fit()

```python
ray.init(address="auto")
trainer.fit()
```

优点：

- 没有 Ray Client gRPC 额外代理。
- `ray.get` 直接由 native driver 通过 Ray Core 获取结果。
- 调试简单。

风险：

- driver 仍然可能成为中心化瓶颈。
- 如果 trainer 中心进程频繁 `ray.get` 大 rollout batch，仍然会把大对象拉回 driver。
- driver 挂了，训练主流程也挂。

适合：

- 单机或小规模调试。
- trainer 只拿小 metadata，不拿大 batch。

### 5.2 Ray Client frontend 直接跑 trainer.fit()

```python
ray.init("ray://head:10001")
trainer.fit()
```

优点：

- 可以从外部机器或普通 shell 连接远程 Ray 集群。
- 交互式开发方便。

风险：

- `ray.get` 大对象会经过 Ray Client Server 再回到 frontend。
- `ray.put` 大对象会从 frontend 上传到 Ray Client Server。
- 每个 actor 调用都多一层 client -> server 控制面代理。
- frontend 进程和 Ray Client 连接稳定性会影响训练。

不适合：

- 长时间大规模 RL 训练主循环。
- trainer 每 step 需要处理大 `RolloutState` / tensor / 多模态数据。

### 5.3 Ray Jobs 启动 entrypoint driver

```bash
ray job submit --address=http://head:8265 -- python train.py
```

优点：

- entrypoint 由 Ray Jobs 管理。
- 可以查状态、日志、停止任务。
- runtime env 可以作用于 entrypoint driver。
- 比 Ray Client 更适合长任务。

风险：

- entrypoint 代码如果又主动 `ray.init("ray://...")`，仍会退回 Ray Client 问题。
- 如果 entrypoint native 运行，但 trainer 仍中心化 `ray.get` 大对象，仍有 controller 瓶颈。

建议：

- Jobs entrypoint 中优先使用 native attach，例如 `ray.init(address="auto")`，而不是 `ray://`。
- 或者 Jobs entrypoint 只启动一个 `RLTaskRunner` actor，让 trainer 主循环在 actor 中运行。

### 5.4 Ray Client 只提交 TaskRunner actor

```python
ray.init("ray://head:10001")
runner = RLTaskRunner.remote()
ray.get(runner.run.remote(config_path))
```

优点：

- 外层 Ray Client 只承担提交和等待。
- `trainer.fit()` 在集群内部 actor 中执行。
- trainer 内部的 `ray.get` / actor 调用不再返回 client frontend。
- 大幅降低 Ray Client 代理层对训练 step 的影响。

风险：

- `RLTaskRunner` actor 仍可能成为 single-controller 瓶颈。
- 如果它自己 `ray.get` 大对象，仍会在 runner actor 所在节点汇聚数据。
- 需要避免 runner 被调度到 head 节点，防止 head 过载。

建议：

- 给 runner 使用专门资源或调度约束。
- runner 只处理状态机和小 metadata。
- 大 batch 尽量由 replay buffer actor / train actor 直接消费。

## 6. verl 写法分析

verl 当前 `main_ppo.py` 的关键结构是：

```python
task_runner_class = ray.remote(num_cpus=1)(TaskRunner)
runner = task_runner_class.remote()
ray.get(runner.run.remote(config))
```

真正训练逻辑在 `TaskRunner.run()` 内：

```python
trainer = RayPPOTrainer(...)
trainer.init_workers()
trainer.fit()
```

这说明：

```text
外层 driver/client
  -> 只创建 TaskRunner
  -> 等 TaskRunner.run 完成

TaskRunner actor
  -> 创建 RayPPOTrainer
  -> 执行 trainer.fit
```

从 Ray Client 角度看，这是合理设计。即使外层使用 Ray Client，训练主循环也不在 client frontend 中跑。

但是 verl 仍然有 single-controller 风险：

- `TaskRunner` / `RayPPOTrainer` 可能汇聚 rollout 数据。
- dataloader、validation、metrics、logging 都可能压在一个 actor 上。
- 如果 `RayPPOTrainer.fit()` 中频繁 `ray.get` 大 `DataProto` 到 controller，仍然会有中心化内存和传输瓶颈。

所以对 verl 的判断是：

```text
从 Ray Client 代理成本角度：设计合理。
从超大规模 RL 架构角度：仍需检查 single-controller 是否成为瓶颈。
```

## 7. xtuner 写法分析

xtuner `rl_design` 分支当前入口中，如果存在 `RAY_MASTER_ADDR`：

```python
ray_head_address = f"ray://{master_addr}:{client_port}"
ray.init(address=ray_head_address)
```

这会进入 Ray Client 模式。

当前 trainer 结构是：

```python
trainer = trainer_cfg.build()
trainer.fit()
```

`RLColocateTrainer.fit()` 和 `RLDisaggregatedTrainer.fit()` 的主循环都在 trainer 对象本身中执行。

关键步骤包括：

```text
agent_loop_manager.produce_batch / get_batch
_train_one_batch
_prepare_train_data
train_controller.fit
rollout_controller onload/offload/sync
evaluation
logging / trajectory dump
```

如果入口走 `ray://`，那么这些逻辑跑在 Ray Client frontend 中。

### 7.1 主要问题

第一，Ray Client 代理成本。

如果 `produce_batch()` 或 `get_batch()` 返回大量 `RolloutState`，数据可能经过：

```text
Ray actor / object store
  -> Ray Client Server
  -> Ray Client gRPC
  -> trainer frontend
```

第二，中心化 trainer 成本。

`_prepare_train_data()` 在中心 trainer 中遍历 rollout 数据，构造 tensor、`SequenceContext`、`data_batches`，再分发给 train workers。

这会使 trainer 成为：

```text
rollout 数据汇聚点
训练 batch 构造点
日志和 trajectory dump 点
同步控制点
```

如果 `RolloutState` 里只有 token ids、reward、少量 metadata，问题可能暂时可控。

如果包含大 tensor、图像、logprobs、大量 routed experts、长 response 或多模态字段，风险会明显放大。

### 7.2 更新后的判断

经过进一步分析，当前 xtuner 最应该优先修的不是先引入 `RLTaskRunner` actor，而是先把 Ray 初始化和启动脚本的资源建模修正确。

`RLTaskRunner` actor 的主要价值是控制 trainer/controller 的 placement，避免 trainer 跑在 Ray Client frontend 或 head 节点上。但如果先把 `ray://` 去掉，让 entrypoint 成为 native Ray driver，那么 P1 的收益会明显下降。

更大的风险仍然是数据流中心化：

```text
rollout 数据
  -> trainer / controller
  -> _prepare_train_data
  -> train workers
```

只要 trainer/controller 仍然每 step `ray.get` 或接收完整 rollout batch，它仍然可能成为大对象汇聚点。这个问题不是单纯把 trainer 包成 actor 就能解决的。

## 8. xtuner 全局改进建议

### 8.1 P0：先修 `rl.py` 的 Ray 初始化

当前 `xtuner/v1/train/cli/rl.py` 中，如果存在 `RAY_MASTER_ADDR`，会自动执行：

```python
ray.init(address=f"ray://{master_addr}:{client_port}")
```

这会把入口进程变成 Ray Client frontend。这个行为不应该由 `RAY_MASTER_ADDR` 隐式触发。

建议改成显式配置：

```python
ray_address = os.getenv("XTUNER_RAY_ADDRESS")

if ray_address:
    ray.init(address=ray_address)
else:
    try:
        ray.init(address="auto")
    except Exception:
        ray.init(num_cpus=128)
```

含义是：

```text
XTUNER_RAY_ADDRESS=ray://head:10001  -> 显式使用 Ray Client
XTUNER_RAY_ADDRESS=auto              -> native attach 到已有 Ray 集群
不设置 XTUNER_RAY_ADDRESS           -> 优先 address="auto"，失败后本地 ray.init
```

`RAY_MASTER_ADDR` 只应该表示 head 地址，不应该等价于 Ray Client。

### 8.2 多机正式入口统一使用 Ray Jobs

建议把 `run_rl_submit.sh` 作为 xtuner 多机 production 入口。

`run_rl.sh` 可以保留为单机或调试入口，但不建议作为多机正式入口。因为直接：

```bash
python xtuner/v1/train/cli/rl.py --config ...
```

时，driver/trainer 就跑在执行这个脚本的节点上。Ray 不能把已经启动的普通 Python driver 进程迁移到其他节点。

Ray Jobs 的好处是：

```text
提交端 shell / CLI
  -> Dashboard Jobs HTTP API
  -> JobManager / JobSupervisor
  -> entrypoint subprocess
  -> native Ray driver
```

这样 driver 生命周期、日志、停止、状态查询都交给 Ray Jobs 管理。

注意：Ray Jobs 本身不是 Ray Client；但 entrypoint 代码里如果再次 `ray.init("ray://...")`，仍然会回到 Ray Client 问题。所以 P0 必须先修。

### 8.3 修正 head 的 CPU 声明

当前脚本中有类似逻辑：

```bash
total_cpus=$((node_count * 128))

ray start --head \
  ... \
  --num-cpus=$total_cpus
```

这个写法不合理。`--num-cpus` 声明的是当前 Ray 节点的 logical CPU 数，不是整个集群的 CPU 总数。

Ray 文档中说明，Ray 资源是 logical resources，用于调度准入控制，不等同于物理隔离。`ray start --head --num-cpus=0` 的含义是告诉 scheduler 不要把需要 CPU 的 task/actor 调度到 head 上，从而保留 head 运行 Ray 系统进程。

建议改成：

```bash
# head 默认只做控制面
ray start --head \
  ... \
  --num-cpus="${RAY_HEAD_NUM_CPUS:-0}"

# worker 声明本机 CPU
ray start \
  --address="$RAY_MASTER_ADDR:$RAY_HEAD_PORT" \
  --num-cpus="${RAY_WORKER_NUM_CPUS:-128}" \
  --block
```

如果确实希望 head 也参与训练，再显式设置：

```bash
RAY_HEAD_NUM_CPUS=128
```

但不要把 `node_count * cpus_per_node` 声明到 head 上。

### 8.4 暂时不优先做 P1：RLTaskRunner actor

不建议当前优先做 P1。

原因：

1. P0 去掉隐式 Ray Client 后，trainer 已经可以作为 native Ray driver 运行。
2. P1 主要解决 trainer/controller placement，不解决大对象数据流中心化。
3. 当前更大的性能风险是 trainer/controller 拉取、展开、转换完整 rollout batch。

P1 仍然有价值，但它应该被理解为 placement / lifecycle 优化，而不是 Ray Client 和数据流问题的根治方案。

### 8.5 如果必须保证 trainer 不跑在 head

如果不做 P1，又要求 trainer 不跑 head，最合理的是在 Ray Jobs entrypoint 层做 placement 约束，而不是把 trainer 包成 actor。

worker 启动时加一个自定义资源：

```bash
ray start \
  --address="$RAY_MASTER_ADDR:$RAY_HEAD_PORT" \
  --num-cpus="${RAY_WORKER_NUM_CPUS:-128}" \
  --resources='{"xtuner_driver": 1}' \
  --block
```

提交 job 时要求这个资源：

```bash
ray job submit \
  --entrypoint-num-cpus=1 \
  --entrypoint-resources='{"xtuner_driver": 1}' \
  --runtime-env-json="$RUNTIME_ENV_JSON" \
  -- python xtuner/v1/train/cli/rl.py --config "$CONFIG_PATH"
```

Ray Jobs 默认会把 entrypoint driver 跑在 head 上；如果指定了 entrypoint 资源，entrypoint 会调度到满足资源的节点。这个机制比在应用层额外包一层 runner actor 更直接。

### 8.6 后续真正的性能优化方向

后续重点应该放在数据流，而不是先改 trainer 外壳。

更理想的数据流是：

```text
rollout actor
  -> replay buffer actor / object store
  -> train actor 直接消费
```

trainer/controller 只处理：

```text
小 metadata
状态机
step 指标
checkpoint / sync 指令
```

避免：

```text
trainer/controller 展开完整 rollout batch
trainer/controller 反序列化大量对象
trainer/controller 构造完整训练 tensor
```

一句话总结：

```text
先做 P0 + 启动脚本资源修正；
不急着做 P1；
后续把大数据流从 trainer/driver 中挪出去。
```

## 9. 设计评审检查清单

评审 Ray RL trainer 架构时，先问：

1. `trainer.fit()` 跑在 Ray Client frontend、native driver，还是集群内 actor？
2. 每个 step 是否 `ray.get` 大对象到 trainer/controller？
3. `RolloutState` 是否包含大 tensor、图像、logprobs、routed experts、多模态字段？
4. trainer/controller 是否在 Python heap 中保存完整 rollout batch？
5. actor 之间是否可以直接传 `ObjectRef`，而不是让 trainer 展开对象？
6. Ray Client 是否只用于提交入口，而不是承载训练主循环？
7. head 节点是否承担了 GCS、dashboard、Ray Client Server、driver、controller 等过多角色？
8. 如果使用 Ray Jobs，entrypoint 是否又错误地 `ray.init("ray://...")`？

## 10. 最终一句话

Ray Client 模式下，最危险的不是 actor 之间传大对象，而是 client-side trainer 在训练主循环里频繁 `ray.get` 大对象。

verl 通过 `TaskRunner` actor 把训练主循环放进集群内部，规避了 Ray Client frontend 的主要问题。

xtuner 当前最优先的改法是：去掉 `RAY_MASTER_ADDR -> ray://` 的隐式 Ray Client 初始化，并修正启动脚本中的 head CPU 资源声明。`RLTaskRunner` actor 可以作为后续 placement 优化，但不应该被当作当前第一优先级。

## 11. nested ObjectRef 生命周期复杂度与后续重构方向

### 11.1 Ray 对 nested ObjectRef 的语义

Ray 官方文档里有两个关键语义：

1. `ObjectRef` 可以被传来传去，也可以存放在其他 Python 对象内部；Ray 通过 distributed reference counting 追踪这些引用，对象会在所有引用删除后自动释放。
2. `ObjectRef` 作为 remote task / actor method 的顶层参数传入时，Ray 会自动解引用，任务拿到的是实际对象。
3. `ObjectRef` 如果藏在 list、dict、dataclass、Pydantic model 这类嵌套对象里，Ray 不会自动解引用；接收方拿到的是 `ObjectRef` 本身，需要自己 `ray.get()`。

所以 nested `ObjectRef` 不是不能用，它的价值是：

```text
只把引用传给下游；
下游如果不需要数据，就不触发对象传输；
下游如果需要数据，再显式 ray.get。
```

但它的代价也很明显：

```text
引用藏在业务对象字段里；
对象 store 生命周期不再直观；
owner / borrower 链更难判断；
free / del 是否真正释放变得不确定。
```

### 11.2 xtuner 当前的 nested ObjectRef 位置

xtuner 当前至少有两类 nested `ObjectRef`：

1. `RolloutState.mm_info.pixel_values`

   在 `xtuner/v1/data_proto/rl_data.py` 中，`MultimodalInfo.pixel_values` 允许是 `np.ndarray | RayObjectRef | None`。

   在 `xtuner/v1/rl/agent_loop_manager/sampler.py` 中，`put_to_ray()` 会把 `pixel_values` 通过 `ray.put(pixel_values)` 放进 object store，然后把 ref 塞回 `RolloutState.mm_info`。

   训练 worker 后续在 `xtuner/v1/rl/trainer/worker.py` 中，从 `seq_ctx.pixel_values` 里拿出 ref 并逐个 `ray.get()`。

2. `RolloutState.routed_experts`

   在 `xtuner/v1/data_proto/rl_data.py` 中，`RolloutState.routed_experts` 允许是 `list[int] | RayObjectRef | None`。

   rollout worker / agent loop 会把 routed experts 放入 object store。

   训练 worker 在 `_add_rollout_routed_experts()` 里 `ray.get()`，然后尝试 `ray.internal.free()`。

这两个设计的共同问题是：

```text
RolloutState 表面上是一个普通业务对象；
但它内部可能隐式 pin 住 object store 中的大对象；
这些 ref 会随着 replay buffer、trajectory 保存、Pydantic model copy、checkpoint 等路径传播。
```

### 11.3 当前最危险的一点：序列化 ObjectRef

xtuner 当前 `routed_experts` 的 Pydantic serializer 会对 `ObjectRef` 做 `ray.cloudpickle.dumps()`，再 base64 编码保存。

Ray 文档明确说，显式序列化 `ObjectRef` 应该作为 last resort。因为一旦 out-of-band 序列化，Ray 很难知道这个引用什么时候真正不用了；为了避免对象被提前删掉，底层对象可能会保持 pinned，需要显式 free。

因此，后续应该避免把 `ObjectRef` 写入：

```text
jsonl trajectory
checkpoint
Pydantic dump
外部数据库
日志系统
```

更合理的策略是：

```text
日志只保存摘要信息；
checkpoint 保存已物化的普通数据，或者保存可重新构造的 key；
业务对象 dump 时明确禁止 ObjectRef 序列化。
```

### 11.4 推荐改法一：不要把 ObjectRef 藏进 RolloutState

当前形态：

```python
RolloutState(
    mm_info={"pixel_values": pixel_ref},
    routed_experts=routed_experts_ref,
)
```

推荐改成显式引用结构：

```python
@dataclass
class TrainShardRef:
    token_ref: ray.ObjectRef | None
    pixel_ref: ray.ObjectRef | None
    routed_experts_ref: ray.ObjectRef | None
    meta: dict
```

也就是说：

```text
业务对象负责表达样本语义；
ObjectRef 容器负责表达大对象引用；
二者不要混在同一个 RolloutState 里。
```

这样后续看代码时很容易判断：

```text
哪里创建 object store 对象；
哪里持有 ref；
哪里消费 ref；
哪里释放 ref。
```

### 11.5 推荐改法二：从 sample 级 ref 改成 train-shard 级 ref

比“显式引用结构”更进一步的是参考 slime 的数据流：

```text
rollout / agent loop manager
  -> 按 train DP rank 切 shard
  -> 每个 shard ray.put 一次
  -> trainer 只拿 list[ObjectRef]
  -> train worker 按 rank ray.get 自己的 shard
```

也就是：

```python
shard_refs = [ray.put(train_shard_for_rank_i) for i in range(dp_size)]
worker_i.fit.remote(shard_refs[i])
```

而不是：

```python
RolloutState(
    mm_info={"pixel_values": pixel_ref},
    routed_experts=routed_experts_ref,
)
```

这个改法的好处是：

1. ref 数量从 sample 级下降到 shard 级。
2. trainer/controller 不需要展开完整大 batch。
3. train worker 只拉取自己需要的数据。
4. object store 生命周期更接近“一批训练数据”的生命周期，而不是散落在每个样本字段里。
5. 后续做显式 release、监控 object store、排查 `ray memory` 都更容易。

### 11.6 推荐改法三：必要时引入 ObjectRefRegistry actor

如果异步 replay buffer 确实需要长期持有大对象引用，可以引入一个集中管理引用的 actor：

```python
@ray.remote
class ObjectRefRegistry:
    def __init__(self):
        self.refs = {}

    def put(self, key, obj):
        self.refs[key] = ray.put(obj)
        return key

    def get_ref(self, key):
        return self.refs[key]

    def release(self, key):
        ref = self.refs.pop(key, None)
        if ref is not None:
            ray._private.internal_api.free([ref], local_only=False)
```

业务对象只保存 key，不保存 `ObjectRef`：

```text
RolloutState.routed_experts_key = "step_10_rank_3_sample_7"
```

这样生命周期集中在 registry：

```text
谁创建；
谁还持有；
哪些 key 还活着；
什么时候 release；
release 后是否仍然无法释放。
```

这个方案比在各处散落 `ray.internal.free()` 更容易维护。

### 11.7 不要把 ray.internal.free 当成主内存管理方案

xtuner 当前已经有一些显式 free：

```text
free_object_refs()
ray.internal.free(..., local_only=False)
```

这些可以保留为兜底，但不能作为主要设计。

原因是：

```text
如果还有 pending task 参数引用，删不掉；
如果还有 actor 局部变量引用，删不掉；
如果还有 nested ObjectRef 藏在业务对象里，删不掉；
如果 ObjectRef 被 cloudpickle 序列化过，也可能继续 pin；
如果 ray.get 得到的是 numpy / zero-copy 对象，Python 值本身也可能继续 pin object store。
```

所以更可靠的主策略是：

```text
减少 ObjectRef 数量；
缩短 ObjectRef 生命周期；
让 ObjectRef 显式存在；
避免 ObjectRef 被 Pydantic / json / checkpoint 序列化；
把大对象按训练 shard 管理，而不是按样本字段管理。
```

### 11.8 后续重构优先级

建议后续单独重构时按这个顺序做：

1. 禁止 `RolloutState` 的 Pydantic dump 序列化 `ObjectRef`，尤其是 `routed_experts`。
2. 明确区分 `RolloutState` 和大对象引用结构，避免 `ObjectRef` 藏在 `mm_info` / `routed_experts` 字段里。
3. 把 `pixel_values`、`routed_experts` 从 sample 级 nested refs 改成 train-shard 级 refs。
4. 让 train worker 直接消费自己的 shard ref，trainer/controller 只传小 metadata。
5. 如果 replay buffer 还需要跨 step 持有大对象，再引入 `ObjectRefRegistry` actor 集中管理。

最终目标：

```text
RolloutState 是轻量样本状态；
TrainShardRef 是显式大对象引用；
trainer/controller 不展开大 batch；
train worker 直接消费 shard；
object store 生命周期按 batch/shard 清晰收敛。
```
