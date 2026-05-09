# Ray Actor Method 调用链路源码笔记

本文整理 `actor.method.remote()` 从 Python 入口到目标 actor worker 执行的源码链路。重点只看 actor method，不展开普通 task 的完整调度。

## 1. 总览

Actor 创建完成之后，actor method 不再做节点级调度。`actor.method.remote()` 的主路径是：

```text
caller Python
  ActorMethod.remote()
  ActorHandle._actor_method_call()
  worker.core_worker.submit_actor_task()

caller C++ CoreWorker
  CoreWorker::SubmitActorTask()
  ActorTaskSubmitter::SubmitTask()
  ResolveDependencies()
  ActorTaskSubmitter::SendPendingTasks()
  CoreWorkerClient::PushActorTask()

target actor worker
  CoreWorkerService.PushTask
  CoreWorker::HandlePushTask()
  TaskReceiver::QueueTaskForExecution()
  OrderedActorTaskExecutionQueue / UnorderedActorTaskExecutionQueue
  CoreWorker::ExecuteTask()
  Python task_execution_handler()
  execute_task()
  func(actor, *args, **kwargs)

caller C++ CoreWorker
  ActorTaskSubmitter::HandlePushTaskReply()
  TaskManager::CompletePendingTask()
```

几个关键结论先放前面：

1. `actor.method.remote()` 不会经过 GCS 做每次方法调用的中转。
2. GCS 只通过 actor state pubsub 告诉 caller：actor 当前 worker 地址是什么。
3. actor method RPC 是 caller CoreWorker 直接 `PushTask` 到目标 actor worker 的 CoreWorker gRPC server。
4. caller 侧有一个 per-actor submit queue，负责依赖解析、顺序、重试、backpressure。
5. executor 侧也有 actor task execution queue，负责 actor 内部串行 / 并发执行。
6. 普通 actor 默认 ordered queue 串行执行；AsyncActor / ThreadedActor 默认 out-of-order，并进入 unordered queue + fiber / thread pool。

## 2. Python 入口

用户代码：

```python
ref = actor.method.remote(x)
```

首先进入 `ActorMethod.remote()`：

```python
def remote(self, *args, **kwargs):
    return self._remote(args, kwargs)
```

源码位置：

- `python/ray/actor.py:916`

`ActorMethod._remote()` 做几件事：

1. 填充 `num_returns`、`max_task_retries`、`retry_exceptions`、`concurrency_group` 等 method option。
2. 检查 tensor transport 等特殊选项。
3. 找到绑定的 `ActorHandle`。
4. 调用 `dst_actor._actor_method_call(...)`。

关键代码：

```python
return dst_actor._actor_method_call(
    self._method_name,
    args=args,
    kwargs=kwargs,
    name=name,
    num_returns=num_returns,
    max_task_retries=max_task_retries,
    retry_exceptions=retry_exceptions,
    concurrency_group_name=concurrency_group,
    ...
)
```

源码位置：

- `python/ray/actor.py:1024`
- `python/ray/actor.py:1093`

## 3. ActorHandle 保存了什么

`ActorHandle` 不是 actor 本体。它是 caller 侧用来提交 actor method 的句柄。

关键字段包括：

```text
_ray_actor_id
_ray_actor_language
_ray_method_signatures
_ray_method_num_returns
_ray_method_max_task_retries
_ray_actor_method_cpus
_ray_actor_creation_function_descriptor
_ray_allow_out_of_order_execution
```

源码位置：

- `python/ray/actor.py:2128`
- `python/ray/actor.py:2183`

对 Python actor，`ActorHandle.__getattr__()` 会把 `actor.foo` 解析成一个绑定到该 handle 的 `ActorMethod`。

```python
if item in self._method_shells:
    return self._method_shells[item].bind(self)
```

源码位置：

- `python/ray/actor.py:2442`
- `python/ray/actor.py:2468`

所以：

```text
actor.foo
  不是拿 actor 实例上的 foo 方法
  而是生成一个 caller 侧的 ActorMethod stub
```

## 4. Python 到 C++：submit_actor_task

`ActorHandle._actor_method_call()` 会：

1. 根据 method signature 把 args / kwargs flatten 成 `list_args`。
2. 生成 method 的 `function_descriptor`。
3. 调用 Cython 层 `worker.core_worker.submit_actor_task(...)`。

关键代码：

```python
object_refs = worker.core_worker.submit_actor_task(
    self._ray_actor_language,
    self._ray_actor_id,
    function_descriptor,
    list_args,
    name,
    num_returns,
    max_task_retries,
    retry_exceptions,
    retry_exception_allowlist,
    self._ray_actor_method_cpus,
    concurrency_group_name if concurrency_group_name is not None else b"",
    ...
)
```

源码位置：

- `python/ray/actor.py:2324`
- `python/ray/actor.py:2411`

Cython 层 `submit_actor_task()` 会把 Python 参数转成 C++ `TaskArg`，并把 CPU method resource、concurrency group、num_returns 等封装成 `CTaskOptions`。

```cython
status = CCoreWorkerProcess.GetCoreWorker().SubmitActorTask(
    c_actor_id,
    ray_function,
    args_vector,
    CTaskOptions(...),
    max_retries,
    retry_exceptions,
    serialized_retry_exception_allowlist,
    call_site,
    return_refs,
    current_c_task_id,
)
```

源码位置：

- `python/ray/_raylet.pyx:3780`
- `python/ray/_raylet.pyx:3831`

如果提交成功，Cython 直接把 C++ 返回的 `return_refs` 包成 Python `ObjectRef` 返回给用户。

```python
return VectorToObjectRefs(return_refs, skip_adding_local_ref=True)
```

源码位置：

- `python/ray/_raylet.pyx:3865`

注意：这里返回 `ObjectRef` 时，actor method 通常还没有执行完成。返回 ref 的创建和 method 执行是异步解耦的。

## 5. CoreWorker::SubmitActorTask

C++ 入口是 `CoreWorker::SubmitActorTask()`。

它主要做：

1. 确认本 worker 有对应 actor handle。
2. 检查 `max_pending_calls` backpressure。
3. 订阅 actor state。
4. 构造 actor task spec。
5. 创建 return object refs，并放入 `TaskManager` pending task。
6. 交给 `ActorTaskSubmitter`。

关键代码：

```cpp
if (actor_task_submitter_->PendingTasksFull(actor_id)) {
  return Status::OutOfResource(...);
}

auto actor_handle = actor_manager_->GetActorHandle(actor_id);
actor_manager_->SubscribeActorState(actor_id);

TaskSpecBuilder builder;
const TaskID actor_task_id = TaskID::ForActorTask(...);

BuildCommonTaskSpec(...);
actor_handle->SetActorTaskSpec(...);

TaskSpecification task_spec = std::move(builder).ConsumeAndBuild();
task_returns = task_manager_->AddPendingTask(...);
actor_task_submitter_->SubmitTask(task_spec);
```

源码位置：

- `src/ray/core_worker/core_worker.cc:2374`
- `src/ray/core_worker/core_worker.cc:2388`
- `src/ray/core_worker/core_worker.cc:2395`
- `src/ray/core_worker/core_worker.cc:2405`
- `src/ray/core_worker/core_worker.cc:2426`
- `src/ray/core_worker/core_worker.cc:2457`
- `src/ray/core_worker/core_worker.cc:2467`

这里有一个重要点：`AddPendingTask()` 会让 caller 成为返回 `ObjectRef` 的 owner。actor worker 执行完成后只是把 return object 数据或 metadata 回给 caller，caller 的 `TaskManager` 再完成这些 pending refs。

## 6. actor method 的 sequence number

Actor task spec 里有 per-concurrency-group sequence number。

它在 `ActorHandle::SetActorTaskSpec()` 中生成：

```cpp
builder.SetActorTaskSpec(
    GetActorID(),
    actor_creation_dummy_object_id,
    max_retries,
    retry_exceptions,
    serialized_retry_exception_allowlist,
    concurrency_group_counters_[concurrency_group_name]++,
    tensor_transport);
```

源码位置：

- `src/ray/core_worker/actor_management/actor_handle.cc:142`
- `src/ray/core_worker/actor_management/actor_handle.cc:160`

含义：

```text
同一个 actor handle 内，
每个 concurrency group 维护自己的递增序号。
```

这个序号后面会同时用于 caller 侧发送顺序和 executor 侧执行顺序。

## 7. caller 侧 ActorTaskSubmitter

`ActorTaskSubmitter::SubmitTask()` 是 caller 侧 actor method 提交队列的核心。

它不会马上盲目发 RPC，而是先：

1. 找到 actor 对应的 `ClientQueue`。
2. 如果 actor 不是 DEAD，把 task 放进 `actor_submit_queue_`。
3. 增加 `cur_pending_calls_`。
4. 异步解析参数依赖。
5. 依赖 ready 后再 `SendPendingTasks()`。

关键代码：

```cpp
queue->second.actor_submit_queue_->Emplace(
    concurrency_group, send_pos, task_spec);
queue->second.cur_pending_calls_++;

resolver_.ResolveDependencies(task_spec, ... {
  actor_submit_queue->MarkDependencyResolved(concurrency_group, send_pos);
  SendPendingTasks(actor_id);
});
```

源码位置：

- `src/ray/core_worker/task_submission/actor_task_submitter.cc:168`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:186`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:193`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:212`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:226`

`ClientQueue` 根据 `allow_out_of_order_execution` 选择两种 submit queue：

```cpp
if (allow_out_of_order_execution) {
  actor_submit_queue_ = std::make_unique<OutofOrderActorSubmitQueue>();
} else {
  actor_submit_queue_ = std::make_unique<SequentialActorSubmitQueue>();
}
```

源码位置：

- `src/ray/core_worker/task_submission/actor_task_submitter.h:277`
- `src/ray/core_worker/task_submission/actor_task_submitter.h:285`

### 7.1 SequentialActorSubmitQueue

默认同步 actor 走 `SequentialActorSubmitQueue`。

它的特点：

1. task 先按 sequence number 入队。
2. 依赖 ready 后标记为 resolved。
3. `PopNextTaskToSend()` 只弹出每个 group 队头且依赖 ready 的任务。
4. retry task 优先，且发送时带 `skip_queue=true`。

源码位置：

- `src/ray/core_worker/task_submission/sequential_actor_submit_queue.cc:24`
- `src/ray/core_worker/task_submission/sequential_actor_submit_queue.cc:116`
- `src/ray/core_worker/task_submission/sequential_actor_submit_queue.cc:153`

### 7.2 OutofOrderActorSubmitQueue

AsyncActor / ThreadedActor 默认 `allow_out_of_order_execution=True`，caller 侧走 `OutofOrderActorSubmitQueue`。

它的特点：

1. dependency 未 ready 时在 pending queue。
2. dependency ready 后进入 sending queue。
3. `PopNextTaskToSend()` 可以弹出任意 ready task，不要求前序 sequence number 都 ready。
4. 发送时通常带 `skip_queue=true`。

源码位置：

- `src/ray/core_worker/task_submission/out_of_order_actor_submit_queue.cc:24`
- `src/ray/core_worker/task_submission/out_of_order_actor_submit_queue.cc:110`
- `src/ray/core_worker/task_submission/out_of_order_actor_submit_queue.cc:143`

## 8. actor 地址如何获得

`actor.method.remote()` 每次调用不问 GCS，但 caller 要知道 actor 当前 worker 地址。

机制是：

1. actor handle 注册到 `ActorManager` 时，创建 per-actor submit queue。
2. 第一次提交 actor task 时，`CoreWorker::SubmitActorTask()` 调用 `actor_manager_->SubscribeActorState(actor_id)`。
3. `ActorManager` 通过 GCS actor state pubsub 订阅 actor 状态。
4. 收到 `ALIVE` 状态时，调用 `ActorTaskSubmitter::ConnectActor(actor_id, actor_data.address(), num_restarts)`。
5. `ConnectActor()` 把 actor worker address 写入 `ClientQueue.client_address_`，之后 `SendPendingTasks()` 才能发 RPC。

关键代码：

```cpp
actor_manager_->SubscribeActorState(actor_id);
```

源码位置：

- `src/ray/core_worker/core_worker.cc:2408`

注册 actor handle 时创建 queue：

```cpp
actor_task_submitter_.AddActorQueueIfNotExists(
    actor_id,
    actor_handle->MaxPendingCalls(),
    actor_handle->AllowOutOfOrderExecution(),
    ...);
```

源码位置：

- `src/ray/core_worker/actor_management/actor_manager.cc:167`
- `src/ray/core_worker/actor_management/actor_manager.cc:178`

订阅 actor state：

```cpp
gcs_client_->Actors().AsyncSubscribe(actor_id, actor_notification_callback, ...);
```

源码位置：

- `src/ray/core_worker/actor_management/actor_manager.cc:295`
- `src/ray/core_worker/actor_management/actor_manager.cc:319`

收到 ALIVE 后连接 actor：

```cpp
if (actor_data.state() == rpc::ActorTableData::ALIVE) {
  actor_task_submitter_.ConnectActor(
      actor_id, actor_data.address(), actor_data.num_restarts());
}
```

源码位置：

- `src/ray/core_worker/actor_management/actor_manager.cc:231`
- `src/ray/core_worker/actor_management/actor_manager.cc:262`

`ConnectActor()` 更新地址并发送 pending tasks：

```cpp
queue->second.worker_id_ = address.worker_id();
queue->second.client_address_ = address;
SendPendingTasks(actor_id);
```

源码位置：

- `src/ray/core_worker/task_submission/actor_task_submitter.cc:298`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:341`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:346`

所以 GCS 的角色是：

```text
GCS 告诉 caller：actor 现在活着，地址是哪里。
GCS 不承载每次 actor method 的数据 RPC。
```

## 9. PushActorTask RPC

依赖 resolved 且 actor address 已知后，`SendPendingTasks()` 从 submit queue 弹出可发送任务。

```cpp
auto task = actor_submit_queue->PopNextTaskToSend();
PushActorTask(client_queue, task->first, task->second);
```

源码位置：

- `src/ray/core_worker/task_submission/actor_task_submitter.cc:535`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:573`

`PushActorTask()` 构造 `PushTaskRequest`：

```cpp
request->mutable_task_spec()->CopyFrom(task_spec.GetMessage());
request->set_intended_worker_id(queue.worker_id_);
request->set_sequence_number(task_spec.ConcurrencyGroupSequenceNumber());
```

然后通过目标 actor worker 的 CoreWorker client 发送：

```cpp
core_worker_client_pool_.GetOrConnect(addr)->PushActorTask(
    std::move(request), skip_queue, std::move(wrapped_callback));
```

源码位置：

- `src/ray/core_worker/task_submission/actor_task_submitter.cc:582`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:587`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:593`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:639`

`CoreWorkerService` 的 RPC 定义里，actor task 和 normal task 共用 `PushTask`。

源码位置：

- `src/ray/protobuf/core_worker.proto:472`
- `src/ray/protobuf/core_worker.proto:480`

## 10. executor 侧 CoreWorker::HandlePushTask

目标 actor worker 收到 `PushTask` 后进入 `CoreWorker::HandlePushTask()`。

对 actor task，它会把请求 post 到 task execution service，再交给 `TaskReceiver`：

```cpp
if (request.task_spec().type() == TaskType::ACTOR_TASK) {
  task_execution_service_.post([...] {
    task_receiver_->QueueTaskForExecution(
        std::move(request), reply, send_reply_callback);
  });
}
```

源码位置：

- `src/ray/core_worker/core_worker.cc:3341`
- `src/ray/core_worker/core_worker.cc:3382`
- `src/ray/core_worker/core_worker.cc:3398`

这里还有一个重要保护：

```cpp
HandleWrongRecipient(WorkerID::FromBinary(request.intended_worker_id()), ...)
```

源码位置：

- `src/ray/core_worker/core_worker.cc:3346`

它确保这个 RPC 确实发到了预期 actor worker。actor restart 后，旧 worker / 新 worker 地址可能变化，这个 intended worker id 用于挡住错投递。

## 11. TaskReceiver 选择 execution queue

`TaskReceiver::QueueTaskForExecution()` 对 actor task 的处理：

```cpp
auto it = actor_task_execution_queues_.find(task_spec.CallerWorkerId());
if (it == actor_task_execution_queues_.end()) {
  it = actor_task_execution_queues_.emplace(
    task_spec.CallerWorkerId(),
    allow_out_of_order_execution_
      ? std::make_unique<UnorderedActorTaskExecutionQueue>(...)
      : std::make_unique<OrderedActorTaskExecutionQueue>(...)
  ).first;
}
it->second->EnqueueTask(
    request.sequence_number(),
    request.client_processed_up_to(),
    TaskToExecute(...));
```

源码位置：

- `src/ray/core_worker/task_execution/task_receiver.cc:144`
- `src/ray/core_worker/task_execution/task_receiver.cc:214`
- `src/ray/core_worker/task_execution/task_receiver.cc:220`
- `src/ray/core_worker/task_execution/task_receiver.cc:241`

注意：executor 侧 queue 是按 `CallerWorkerId()` 分开的。

```text
同一个 actor 可能被多个 caller worker 调用；
每个 caller worker 有自己的 actor task execution queue。
```

这和 actor handle sequence number 的语义配套：sequence number 是 caller 侧生成的，因此 executor 侧按 caller worker 分队列处理。

## 12. actor 创建时配置执行模型

actor 创建 task 完成后，`TaskReceiver::HandleTaskExecutionResult()` 会根据 creation task spec 初始化 concurrency group manager。

```cpp
if (task_spec.IsActorCreationTask()) {
  concurrency_groups_ = task_spec.ConcurrencyGroups();
  if (is_asyncio_) {
    fiber_state_manager_ = std::make_shared<ConcurrencyGroupManager<FiberState>>(...);
  } else {
    pool_manager_ = std::make_shared<ConcurrencyGroupManager<BoundedExecutor>>(...);
  }
}
```

源码位置：

- `src/ray/core_worker/task_execution/task_receiver.cc:97`

actor 创建阶段还会调用 `SetupActor()`：

```cpp
SetupActor(task_spec.IsAsyncioActor(),
           task_spec.MaxActorConcurrency(),
           task_spec.AllowOutOfOrderExecution());
```

源码位置：

- `src/ray/core_worker/task_execution/task_receiver.cc:208`
- `src/ray/core_worker/task_execution/task_receiver.cc:275`

Python 创建 actor 时，`allow_out_of_order_execution` 的默认值是：

```python
if allow_out_of_order_execution is None:
    allow_out_of_order_execution = is_asyncio or max_concurrency > 1
```

也就是说：

```text
普通同步 actor:
  allow_out_of_order_execution=False

AsyncActor:
  allow_out_of_order_execution=True

ThreadedActor:
  max_concurrency > 1 -> allow_out_of_order_execution=True
```

源码位置：

- `python/ray/actor.py:2034`
- `python/ray/actor.py:2037`

## 13. OrderedActorTaskExecutionQueue

普通同步 actor 默认走 `OrderedActorTaskExecutionQueue`。

它的核心行为：

1. actor task 入队时保存 `seq_no`。
2. 如果有 pending dependencies，先通过 `ActorTaskExecutionArgWaiter` 等参数 ready。
3. `ExecuteQueuedTasks()` 只执行当前 group 的 `next_seq_no`。
4. 每执行一个任务，`next_seq_no++`。
5. 如果先收到了后面的 seq_no，但前面的 seq_no 迟迟没到，会启动 reorder wait timer。

关键代码：

```cpp
RAY_CHECK(seq_no != -1);
RAY_CHECK(group_state.pending_tasks.emplace(seq_no, std::move(task)).second);

if (!dependencies.empty()) {
  waiter_.AsyncWait(dependencies, ... {
    ready_task->MarkDependenciesResolved();
    ExecuteQueuedTasks();
  });
}

while (!group_state.pending_tasks.empty()) {
  auto &[seq_no, request] = *begin_it;
  if (seq_no == group_state.next_seq_no) {
    if (request.DependenciesResolved()) {
      ExecuteRequest(std::move(request));
      group_state.next_seq_no++;
    }
  }
}
```

源码位置：

- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:67`
- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:93`
- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:110`
- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:174`
- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:210`

执行时：

```cpp
auto pool = pool_manager_->GetExecutor(...);
if (pool == nullptr) {
  AcceptRequestOrRejectIfCanceled(task_id, request);
} else {
  pool->Post(...);
}
```

源码位置：

- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:289`

对默认同步 actor，`pool == nullptr`，因此方法在 actor worker 的 task execution service 主执行路径上串行跑。

## 14. UnorderedActorTaskExecutionQueue

AsyncActor 和 ThreadedActor 默认走 `UnorderedActorTaskExecutionQueue`。

它的核心行为：

1. 不等待前序 sequence number。
2. 依赖 ready 后立即尝试执行。
3. 防止同一个 task id 的多个 attempt 同时执行。
4. AsyncActor 进入 fiber executor。
5. ThreadedActor 进入 bounded thread pool。

关键代码：

```cpp
if (run_task) {
  RunRequest(std::move(task));
}
```

```cpp
if (is_asyncio_) {
  auto fiber = fiber_state_manager_->GetExecutor(...);
  fiber->EnqueueFiber(...);
} else {
  auto pool = pool_manager_->GetExecutor(...);
  if (pool == nullptr) {
    AcceptRequestOrRejectIfCanceled(...);
  } else {
    pool->Post(...);
  }
}
```

源码位置：

- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:80`
- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:122`
- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:144`
- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:148`
- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:156`

这和之前 actor 并发模型笔记可以串起来：

```text
AsyncActor:
  UnorderedActorTaskExecutionQueue
  -> FiberState
  -> Python event loop

ThreadedActor:
  UnorderedActorTaskExecutionQueue
  -> BoundedExecutor
  -> C++ thread pool
  -> Python method still受 GIL 影响

普通同步 actor:
  OrderedActorTaskExecutionQueue
  -> 默认无 executor pool
  -> 串行执行
```

## 15. ConcurrencyGroupManager

`ConcurrencyGroupManager` 根据 group name 或 function descriptor 找 executor。

```cpp
if (!concurrency_group_name.empty()) {
  return name_to_executor_index_[concurrency_group_name];
}

if (functions_to_executor_index_.find(fd->ToString()) !=
    functions_to_executor_index_.end()) {
  return functions_to_executor_index_[fd->ToString()];
}

return default_executor_;
```

源码位置：

- `src/ray/core_worker/task_execution/concurrency_group_manager.cc:28`
- `src/ray/core_worker/task_execution/concurrency_group_manager.cc:58`

默认 executor 是否存在由这里决定：

```cpp
if (ExecutorType::NeedDefaultExecutor(
        max_concurrency_for_default_concurrency_group,
        !concurrency_groups.empty())) {
  default_executor_ = std::make_shared<ExecutorType>(...);
}
```

源码位置：

- `src/ray/core_worker/task_execution/concurrency_group_manager.cc:46`

核心理解：

```text
没有 concurrency group、默认 max_concurrency=1 的同步 actor:
  default_executor_ 通常为空，直接串行执行。

有 concurrency group 或 max_concurrency>1:
  任务会进入对应 executor。
```

## 16. 进入 Python 方法

execution queue 最终调用：

```cpp
request.Execute();
```

源码位置：

- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:318`
- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:226`

`TaskToExecute.Execute()` 触发 `TaskReceiver` 里构造的 `execute_callback`，它调用 `task_handler_`。这个 handler 在 `CoreWorker` 构造时绑定到 `CoreWorker::ExecuteTask()`。

源码位置：

- `src/ray/core_worker/task_execution/task_receiver.cc:173`
- `src/ray/core_worker/core_worker.cc:376`
- `src/ray/core_worker/core_worker.cc:392`

`CoreWorker::ExecuteTask()` 做：

1. `GetAndPinArgsForExecutor()` 获取并 pin 参数。
2. 设置 current task / running task 状态。
3. 判断 task type：actor creation / actor task / normal task。
4. 调用语言层 `options_.task_execution_callback(...)`。
5. 收集 borrowed refs。
6. 清理 running task 状态。

关键代码：

```cpp
Status pin_args_request_status =
    GetAndPinArgsForExecutor(task_spec, &args, &arg_refs, &borrowed_ids);

worker_context_->SetCurrentTask(task_spec);

Status status = options_.task_execution_callback(
    task_spec.CallerAddress(),
    task_type,
    task_spec.GetName(),
    func,
    ...,
    return_objects,
    dynamic_return_objects,
    ...,
    name_of_concurrency_group_to_execute,
    ...);
```

源码位置：

- `src/ray/core_worker/core_worker.cc:2758`
- `src/ray/core_worker/core_worker.cc:2801`
- `src/ray/core_worker/core_worker.cc:2839`
- `src/ray/core_worker/core_worker.cc:2906`

Python worker 的 `task_execution_callback` 是 `_raylet.pyx` 里的 `task_execution_handler()`。

源码位置：

- `python/ray/_raylet.pyx:2270`
- `python/ray/_raylet.pyx:2304`

它调用 `execute_task_with_cancellation_handler()`，再调用 `execute_task()`。

源码位置：

- `python/ray/_raylet.pyx:2058`
- `python/ray/_raylet.pyx:2154`
- `python/ray/_raylet.pyx:1688`

对 actor task，`execute_task()` 会取出当前 actor 实例：

```python
actor_id = core_worker.get_actor_id()
actor = worker.actors[actor_id]
```

然后构造 `function_executor`：

```python
def function_executor(*arguments, **kwarguments):
    func = execution_info.function
    ...
    return func(actor, *arguments, **kwarguments)
```

源码位置：

- `python/ray/_raylet.pyx:1755`
- `python/ray/_raylet.pyx:1761`
- `python/ray/_raylet.pyx:1797`

所以真正的 Python 方法调用是：

```text
func(actor, *arguments, **kwarguments)
```

不是在 caller 进程执行，而是在 actor worker 进程里的 actor 实例上执行。

## 17. AsyncActor 的 Python 执行

如果当前 actor 是 asyncio actor：

```python
if core_worker.current_actor_is_asyncio():
    if is_async_func(func.method):
        async_function = func
    else:
        async_function = sync_to_async(func)

    return core_worker.run_async_func_or_coro_in_event_loop(
        async_function,
        function_descriptor,
        name_of_concurrency_group_to_execute,
        task_id=task_id,
        task_name=task_name,
        func_args=(actor, *arguments),
        func_kwargs=kwarguments)
```

源码位置：

- `python/ray/_raylet.pyx:1764`
- `python/ray/_raylet.pyx:1778`
- `python/ray/_raylet.pyx:1791`

这说明：

```text
C++ unordered queue / FiberState
  负责把 actor task 放入对应 fiber/concurrency group

Python run_async_func_or_coro_in_event_loop
  负责把 coroutine 放到 actor event loop 中执行
```

## 18. 返回值怎么回到 caller

Python 方法执行后，`execute_task()` 调用：

```python
core_worker.store_task_outputs(
    worker,
    outputs,
    caller_address,
    returns,
    None,
    c_tensor_transport,
)
```

源码位置：

- `python/ray/_raylet.pyx:1995`
- `python/ray/_raylet.pyx:2036`

`store_task_outputs()` 会序列化输出，并调用 `store_task_output()`，后者分配 / 写入 / seal return object。

源码位置：

- `python/ray/_raylet.pyx:4203`
- `python/ray/_raylet.pyx:4263`
- `python/ray/_raylet.pyx:4290`
- `python/ray/_raylet.pyx:4329`

小对象可能 inline 到 `PushTaskReply`，大对象会写入 plasma object store。无论哪种，caller 侧最终都通过 `PushTaskReply` 处理返回对象 metadata。

`TaskReceiver::HandleTaskExecutionResult()` 把 return objects 序列化进 reply：

```cpp
for (size_t i = 0; i < result.return_objects.size(); i++) {
  auto return_object_proto = reply->add_return_objects();
  SerializeReturnObject(return_object.first, return_object.second, return_object_proto);
}
send_reply_callback(Status::OK(), nullptr, nullptr);
```

源码位置：

- `src/ray/core_worker/task_execution/task_receiver.cc:90`
- `src/ray/core_worker/task_execution/task_receiver.cc:140`

caller 侧 `ActorTaskSubmitter::HandlePushTaskReply()` 收到 reply 后：

```cpp
task_manager_.CompletePendingTask(
    task_id, reply, addr, reply.is_application_error());
```

源码位置：

- `src/ray/core_worker/task_submission/actor_task_submitter.cc:643`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:696`

`TaskManager::CompletePendingTask()` 再把 reply 中的 return object 写入 caller 本地 memory store，或者记录 plasma object location / metadata，使用户之前拿到的 `ObjectRef` 变为 ready。

源码位置：

- `src/ray/core_worker/task_manager.cc:908`
- `src/ray/core_worker/task_manager.cc:955`
- `src/ray/core_worker/task_manager.cc:1052`

## 19. actor method 和 GCS / raylet 的关系

一次普通 actor method 调用中：

```text
GCS:
  不参与每次 method 的执行中转。
  只在 actor state 变化时通过 pubsub 通知 caller。

caller CoreWorker:
  持有 ActorHandle。
  构造 actor task spec。
  创建返回 ObjectRef。
  维护 per-actor submit queue。
  直接 PushTask 到 actor worker。

target actor CoreWorker:
  接收 PushTask。
  进入 actor execution queue。
  执行 Python actor method。
  返回 PushTaskReply。

raylet:
  不负责每次 actor method 的节点调度。
  但会参与参数依赖等待、对象拉取、worker 进程生命周期、actor worker 与本地资源管理。
```

这里要和 actor 创建阶段区分：

```text
Actor.remote():
  GCS / raylet 参与 actor creation task 调度，决定 actor 放哪。

actor.method.remote():
  actor 已经固定在某个 worker。
  method 直接发到该 worker。
```

## 20. 和并发模型的对应关系

| actor 类型 | Python 默认 | caller submit queue | executor execution queue | 执行器 |
| --- | --- | --- | --- | --- |
| 普通同步 actor | `max_concurrency=1` | `SequentialActorSubmitQueue` | `OrderedActorTaskExecutionQueue` | 默认无 pool，串行 |
| ThreadedActor | `max_concurrency>1` | `OutofOrderActorSubmitQueue` | `UnorderedActorTaskExecutionQueue` | `BoundedExecutor` thread pool |
| AsyncActor | 类里有 `async def` | `OutofOrderActorSubmitQueue` | `UnorderedActorTaskExecutionQueue` | `FiberState` + event loop |

对应源码：

- Python 默认 `allow_out_of_order_execution`：`python/ray/actor.py:2034`
- caller queue 选择：`src/ray/core_worker/task_submission/actor_task_submitter.h:285`
- executor queue 选择：`src/ray/core_worker/task_execution/task_receiver.cc:220`
- executor 获取：`src/ray/core_worker/task_execution/concurrency_group_manager.cc:58`

## 21. 实用判断规则

### 21.1 `actor.method.remote()` 卡住可能卡在哪

可能位置：

1. caller 侧 `max_pending_calls` 满了。
2. caller 侧参数依赖还没 ready。
3. caller 还没收到 actor ALIVE 地址。
4. PushTask RPC 发出后 actor worker 没有及时回复。
5. actor executor queue 中排队等待前序 sequence number。
6. actor executor queue 中等待参数 fetch。
7. ThreadedActor / AsyncActor 达到 concurrency group 限制。
8. Python 方法本身阻塞或 event loop 被阻塞。

### 21.2 为什么普通 actor 默认串行

不是因为 RPC 只能一个一个发，而是因为：

```text
Python 创建 actor 时 allow_out_of_order_execution=False
  -> caller 侧 SequentialActorSubmitQueue
  -> executor 侧 OrderedActorTaskExecutionQueue
  -> 默认 no executor pool
  -> 按 sequence number 串行 Execute()
```

### 21.3 为什么 AsyncActor 不保证提交顺序

因为：

```text
is_asyncio=True
  -> allow_out_of_order_execution=True
  -> caller 侧 ready 的任务可以先发
  -> executor 侧 UnorderedActorTaskExecutionQueue 不等前序 seq_no
  -> coroutine 在 await 点主动让出
```

所以 AsyncActor 的“单线程 event loop”不等于“状态不会乱”。await 点之间仍然有业务状态竞态。

### 21.4 actor method 返回值 owner 是谁

通常是 caller，也就是提交 `actor.method.remote()` 的 worker / driver。

原因：

1. caller 侧 `CoreWorker::SubmitActorTask()` 先创建 return refs。
2. caller 侧 `TaskManager::AddPendingTask()` 持有 pending task。
3. actor worker 执行完成后，把 return object 结果通过 `PushTaskReply` 回给 caller。
4. caller 侧 `TaskManager::CompletePendingTask()` 完成这些 refs。

这点对 ObjectRef 生命周期很重要：actor worker 是执行者和对象数据生产者，但返回 `ObjectRef` 的 owner 通常是 caller。

## 22. 源码索引

Python 入口：

- `python/ray/actor.py:916`
- `python/ray/actor.py:1024`
- `python/ray/actor.py:1093`
- `python/ray/actor.py:2324`
- `python/ray/actor.py:2411`

Cython 提交：

- `python/ray/_raylet.pyx:3780`
- `python/ray/_raylet.pyx:3831`
- `python/ray/_raylet.pyx:3865`

C++ caller 提交：

- `src/ray/core_worker/core_worker.cc:2374`
- `src/ray/core_worker/actor_management/actor_handle.cc:142`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:168`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:535`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:582`

actor state / 地址：

- `src/ray/core_worker/actor_management/actor_manager.cc:167`
- `src/ray/core_worker/actor_management/actor_manager.cc:231`
- `src/ray/core_worker/actor_management/actor_manager.cc:295`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:298`

executor 接收和排队：

- `src/ray/core_worker/core_worker.cc:3341`
- `src/ray/core_worker/task_execution/task_receiver.cc:144`
- `src/ray/core_worker/task_execution/ordered_actor_task_execution_queue.cc:67`
- `src/ray/core_worker/task_execution/unordered_actor_task_execution_queue.cc:80`
- `src/ray/core_worker/task_execution/concurrency_group_manager.cc:28`

Python 执行：

- `src/ray/core_worker/core_worker.cc:2758`
- `python/ray/_raylet.pyx:2270`
- `python/ray/_raylet.pyx:2058`
- `python/ray/_raylet.pyx:1688`
- `python/ray/_raylet.pyx:1761`
- `python/ray/_raylet.pyx:1797`

返回值：

- `python/ray/_raylet.pyx:2036`
- `python/ray/_raylet.pyx:4203`
- `src/ray/core_worker/task_execution/task_receiver.cc:90`
- `src/ray/core_worker/task_submission/actor_task_submitter.cc:696`
- `src/ray/core_worker/task_manager.cc:908`

## 23. 一句话总结

```text
actor.method.remote()
  本质是 caller CoreWorker 按 ActorHandle 构造 actor task，
  通过 actor state 缓存的 worker 地址直接 PushTask 到 actor worker，
  actor worker 再按 ordered / unordered execution queue 执行 Python 方法，
  最后把结果 reply 给 caller，由 caller 完成最初返回给用户的 ObjectRef。
```

