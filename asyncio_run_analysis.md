# XTuner 中 `asyncio_run` 的设计背景与风险说明

## 1. asyncio 里的 event loop 是什么

在 Python `asyncio` 里，event loop 可以理解为异步任务的调度器。

一个 coroutine 只有被 event loop 驱动后才会真正执行。例如：

```python
async def foo():
    ...
```

`foo()` 本身只是创建一个 coroutine object，并不会运行。它必须通过某个 event loop 执行：

```python
asyncio.run(foo())
```

或者在已有 loop 里：

```python
await foo()
```

event loop 负责调度这些异步对象：

- coroutine
- Task
- Future
- callback
- socket / pipe / timer 等异步 IO 事件

在 asyncio 语义里，`Task` 和 `Future` 通常都绑定到某一个具体的 event loop。一个在 loop A 上创建的 `Future`，不能随便拿到 loop B 上 await。否则可能报：

```text
RuntimeError: Task/Future attached to a different loop
```

## 2. event loop 如何创建和获取

常见的 loop 创建或获取方式大致有四类。

### 2.1 `asyncio.run(coro)`

这是最常见的同步入口：

```python
asyncio.run(main())
```

它适合程序最外层入口使用。每次调用都会：

```text
创建一个新的 event loop
把它设置为当前运行中的 loop
运行传入的 coroutine
清理异步生成器和默认 executor
取消或清理剩余任务
关闭 event loop
```

因此 `asyncio.run(...)` 的语义是“一次性运行一个 async 程序”。它不适合在同一个同步流程里反复调用，用来驱动一批会跨调用复用 async 状态的对象。

### 2.2 `asyncio.new_event_loop()` + `loop.run_until_complete(coro)`

这是手动创建 loop 的方式：

```python
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
result = loop.run_until_complete(main())
```

这种方式把 loop 生命周期交给调用方管理。调用方可以选择：

```python
loop.run_until_complete(step1())
loop.run_until_complete(step2())
loop.run_until_complete(step3())
```

这样 `step1`、`step2`、`step3` 都运行在同一个 loop 上。

当前 XTuner 的 `asyncio_run` 本质上就是这种思路：创建一次 loop，缓存起来，多次 `run_until_complete(...)` 复用。

需要注意，`new_event_loop()` 只是创建一个 loop 对象，不代表可以在任意位置运行它。

如果当前线程里已经有一个正在运行的 event loop，例如已经处在 async 函数内部：

```python
async def main():
    loop2 = asyncio.new_event_loop()
```

上面这句通常可以执行，因为它只是创建了一个新 loop。但如果接着在同一个线程里运行它：

```python
async def main():
    loop2 = asyncio.new_event_loop()
    loop2.run_until_complete(foo())
```

通常会报错：

```text
RuntimeError: Cannot run the event loop while another loop is running
```

原因是同一个线程里已经有一个 event loop 正在调度当前 coroutine，asyncio 不允许在这个线程里再嵌套运行另一个 loop。

也不建议在 running loop 内部随意调用：

```python
asyncio.set_event_loop(loop2)
```

因为当前 coroutine 实际仍然运行在原来的 running loop 上。此时再修改当前线程关联的 loop，会让 `get_event_loop()`、`create_task()` 等接口的行为更难判断。

如果确实需要第二个 event loop，通常要把它放到独立线程里运行：

```python
import asyncio
import threading

def run_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()

loop2 = asyncio.new_event_loop()
thread = threading.Thread(target=run_loop, args=(loop2,), daemon=True)
thread.start()
```

然后通过线程安全接口提交 coroutine：

```python
future = asyncio.run_coroutine_threadsafe(foo(), loop2)
result = future.result()
```

这种方式适用于确实需要隔离一套长期运行的异步系统，例如某个后台 client、服务端连接管理器、独立 IO 调度器等。它的代价是引入跨线程通信、关闭顺序、异常传播和资源回收问题，因此一般不作为普通 async 调用链里的默认选择。

### 2.3 `asyncio.get_running_loop()`

这是在 async 代码里获取“当前正在运行的 loop”的方式：

```python
async def main():
    loop = asyncio.get_running_loop()
```

如果当前线程没有正在运行的 event loop，它会抛出 `RuntimeError`。

这个接口适合在 coroutine 内部使用，因为此时一定已经处在某个 event loop 的调度中。它不会创建新 loop。

### 2.4 `asyncio.get_event_loop()`

这是获取“当前线程关联的 loop”的老接口：

```python
loop = asyncio.get_event_loop()
```

它的行为比 `get_running_loop()` 更复杂：在某些 Python 版本或策略下，如果当前线程还没有设置 loop，它可能创建或返回一个默认 loop；在另一些场景下可能抛错或有 deprecation warning。

XTuner 的 `create_task` 里用了：

```python
loop = asyncio.get_event_loop()
```

这意味着它依赖当前线程已经有一个合适的 event loop。通常这个 loop 来自外层：

- `asyncio.run(...)` 创建的临时 loop；
- 手动 `asyncio.new_event_loop()` 后通过 `asyncio.set_event_loop(loop)` 设置的 loop；
- XTuner 自己的 `asyncio_run` 缓存并设置的共享 loop；
- 已经处在 async 调用链中时，当前运行中的 loop。

总结一下：

```text
asyncio.run(...)                  每次创建并关闭一个新 loop
asyncio.new_event_loop()          手动创建 loop，生命周期由调用方管理
asyncio.get_running_loop()        获取当前正在运行的 loop，不创建
asyncio.get_event_loop()          获取当前线程关联的 loop，行为依赖上下文和 Python 版本
loop.run_until_complete(coro)     在指定 loop 上同步跑一个 coroutine
```

## 3. `create_task` 如何和 loop 绑定

XTuner 里的 `create_task` 定义在：

```text
xtuner/v1/rl/utils/async_utils.py
```

核心逻辑是：

```python
def create_task(coro, loop=None, done_callbacks=None):
    if loop is None:
        loop = asyncio.get_event_loop()
    task = loop.create_task(coro)
    ...
    return task
```

这里有两个关键点：

第一，`create_task` 本身不会创建新的 event loop。它只是拿到当前线程关联的 event loop，然后把 coroutine 包成一个 `Task`，注册到这个 loop 上。

第二，这个 `Task` 从创建开始就属于这个 loop。后续它的执行、取消、完成回调、异常处理，都由这个 loop 调度。

所以真正决定“每次是否换 loop”的不是 `create_task`，而是外层如何进入 async 世界。

例如标准库的：

```python
asyncio.run(coro())
```

每调用一次都会：

```text
创建一个新的 event loop
运行传入的 coroutine
清理未完成任务
关闭 event loop
```

如果同步代码反复调用 `asyncio.run(...)`，那每一次调用都会使用一个新的 loop。

## 4. 为什么标准 `asyncio.run` 在这里会有问题

XTuner 的共卡 RL trainer 是同步编排逻辑，但 rollout / agent loop 已经是异步实现。

共卡训练大致是：

```text
RLColocateTrainer.fit()
  -> produce rollout
  -> train one batch
  -> sync weights
  -> evaluate
```

其中 `fit()` 是同步函数，但 `produce_batch()`、`_run_evaluation()` 等接口是 async。因此同步 trainer 必须通过某种方式调用 async 代码。

如果直接用标准库 `asyncio.run(...)`，共卡路径可能变成：

```python
for train_step in range(...):
    produce_result = asyncio.run(
        self.agent_loop_manager.produce_batch(...)
    )

    eval_result = asyncio.run(
        self._run_evaluation(...)
    )
```

这意味着：

```text
train step 1 的 produce_batch 使用 loop A
train step 2 的 produce_batch 使用 loop B
train step 3 的 produce_batch 使用 loop C
```

但 XTuner 的 agent loop、rollout controller、tool loop、Ray actor proxy 等对象可能是跨 step 复用的。

这就容易出现这样的问题：

```text
某个对象在 step 1 内部创建了绑定到 loop A 的 Future/Task
这个对象本身被 trainer 长期持有
step 2 又在 loop B 里继续调用这个对象
对象内部尝试 await 上次保存的 Future/Task
于是触发 Future attached to a different loop
```

## 5. VerlToolAgentLoop 的具体问题场景

代码注释里提到的 `VerlToolAgentLoop` 就是这种场景。

相关代码在：

```text
recipe/verl_agent/common/agent_loop_verl_tool.py
```

`VerlToolAgentLoop.__init__` 中会创建并长期持有一个 `ToolAgentLoop`：

```python
self.verl_tool_agent_loop = ToolAgentLoop(...)
```

每条样本生成时会调用：

```python
output = await self.verl_tool_agent_loop.run(...)
```

完整链路大致是：

```text
RLColocateTrainer.fit()
  -> agent_loop_manager.produce_batch(...)
    -> ProduceStrategy.produce_batch(...)
      -> create_task(ctx.generate_group(...))
        -> AgentLoop.generate_group(...)
          -> create_task(self.generate_sample(...))
            -> VerlToolAgentLoop.generate_sample(...)
              -> await self.verl_tool_agent_loop.run(...)
```

如果外层每个 step 都使用标准 `asyncio.run(...)`，那么：

```text
step 1: ToolAgentLoop.run 在 loop A 中执行
step 2: 同一个 ToolAgentLoop.run 在 loop B 中执行
```

如果 `ToolAgentLoop` 内部缓存了 task、future、client、连接状态、Ray awaitable 或其他绑定 loop 的异步对象，就可能在 step 2 中 await 到 loop A 的对象，从而触发 loop mismatch。

这就是注释里说的：

```text
Future attached to a different loop
```

下面是一个最小化例子，用来说明这种错误如何产生。

```python
import asyncio


class ReusedAsyncObject:
    def __init__(self):
        self.future = None

    async def first_call(self):
        loop = asyncio.get_running_loop()
        self.future = loop.create_future()

    async def second_call(self):
        await self.future


obj = ReusedAsyncObject()

asyncio.run(obj.first_call())
asyncio.run(obj.second_call())
```

第一次 `asyncio.run(obj.first_call())` 会创建一个 event loop，可以称为 loop A。`first_call()` 里创建的 `self.future` 绑定到 loop A。

第二次 `asyncio.run(obj.second_call())` 会创建另一个 event loop，可以称为 loop B。此时 `second_call()` 在 loop B 中执行，但它尝试 await 的 `self.future` 属于 loop A，于是就会触发 loop mismatch。

实际报错形式通常类似：

```text
RuntimeError: Task/Future attached to a different loop
```

这个例子里的 `ReusedAsyncObject` 对应到 XTuner 场景里，就是被 trainer 跨 step 长期持有的对象，例如 `VerlToolAgentLoop` 内部的 `ToolAgentLoop`、底层 client、Ray awaitable 包装对象或其他异步状态。

如果改成复用同一个 loop：

```python
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

obj = ReusedAsyncObject()
loop.run_until_complete(obj.first_call())
loop.run_until_complete(obj.second_call())
```

那么两次调用都发生在同一个 loop 上，就不会因为 `self.future` 归属不同而触发 loop mismatch。

这个例子只用于解释 loop 归属问题。真实训练代码里，future 可能来自 Ray、HTTP client、tool loop、后台 task 或更深层的 async 库，因此错误栈通常不会像这个例子一样直接。

## 6. 当前 `asyncio_run` 的解决办法

XTuner 现在没有直接使用标准库 `asyncio.run`，而是实现了自己的 `asyncio_run`：

```text
xtuner/v1/rl/utils/async_utils.py
```

其核心思想是：

```text
不要每次调用都创建并关闭一个新的 event loop
而是在模块级别缓存一个 event loop
后续同步代码进入 async 世界时复用同一个 loop
```

简化后类似：

```python
_ASYNCIO_RUN_LOOP = None

def _get_default_asyncio_loop():
    global _ASYNCIO_RUN_LOOP
    if _ASYNCIO_RUN_LOOP is not None and not _ASYNCIO_RUN_LOOP.is_closed():
        return _ASYNCIO_RUN_LOOP

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    _ASYNCIO_RUN_LOOP = loop
    return loop

def asyncio_run(coro):
    loop = _get_default_asyncio_loop()
    if loop.is_running():
        raise RuntimeError(...)
    return loop.run_until_complete(coro)
```

这里的：

```python
if loop.is_running():
    raise RuntimeError(...)
```

是一个非常重要的边界检查。

`asyncio_run` 的定位是“同步代码进入 async 世界”。也就是说，调用方当前不应该已经处在某个正在运行的 event loop 里。它内部最终要调用：

```python
loop.run_until_complete(coro)
```

而 `run_until_complete(...)` 的含义是：由当前同步线程接管这个 loop，把传入的 coroutine 跑到结束。

如果这个 loop 已经在运行，说明当前线程已经处在该 loop 的调度过程中。例如：

```python
async def outer():
    asyncio_run(inner())
```

此时 `outer()` 已经被某个 event loop 调度了。如果 `asyncio_run(inner())` 再尝试 `run_until_complete(inner())`，就变成了在一个正在运行的 loop 里面再次启动同一个 loop。asyncio 不支持这种嵌套运行。

这种情况下正确的 async 语义应该是：

```python
async def outer():
    await inner()
```

而不是：

```python
async def outer():
    asyncio_run(inner())
```

因此这个检查的作用是尽早暴露错误用法：`asyncio_run` 只能放在同步入口层，不能放在 async 调用链内部。

如果没有这层检查，常见结果是底层 `run_until_complete(...)` 抛出类似错误：

```text
RuntimeError: This event loop is already running
```

或者在更复杂的场景里造成更难理解的调度问题。主动检查并报错，可以把问题定位到清晰的边界违规：已经在 async 环境里，就应该直接 `await`，不应该再用同步桥。

这样共卡路径中多次调用：

```python
asyncio_run(self.agent_loop_manager.produce_batch(...))
asyncio_run(self._run_evaluation(...))
```

都会复用同一个 event loop。

于是：

```text
step 1: loop A
step 2: loop A
step 3: loop A
```

如果 `VerlToolAgentLoop` 或底层 client 内部有跨 step 复用的异步对象，它们仍然属于同一个 loop，从而避免 `Future attached to a different loop`。

这个设计本质上是在同步 trainer 和异步 rollout 之间建立一个稳定的 sync-to-async 边界。

## 7. 这个设计本身带来的隐性生命周期问题

复用 event loop 能解决 loop mismatch，但也带来另一个问题：event loop 的生命周期变长了。

标准库 `asyncio.run(...)` 每次调用结束时会关闭 loop，并清理未完成 task。当前 XTuner 的 `asyncio_run` 不会关闭 loop，而是复用它。

这意味着：

```text
asyncio_run(coro) 返回
只代表传入的 coro 完成
不代表这个 loop 里的所有 task 都已经完成或清理干净
```

例如：

```python
async def background():
    await asyncio.sleep(1)
    raise RuntimeError("old task failed")

async def step1():
    create_task(background())
    return

async def step2():
    await asyncio.sleep(10)
```

如果使用共享 loop：

```python
asyncio_run(step1())
asyncio_run(step2())
```

`step1()` 返回时，`background()` 可能还没有结束。因为 loop 没有关闭，到了 `step2()` 运行时，旧的 `background()` 会继续被调度。

于是现象可能变成：

```text
step2 正在运行
但 step1 遗留下来的 background task 突然报错
```

从训练日志看，报错发生在 step2 的时间点；但真正的问题可能是 step1 的 rollout task 没有收干净。

这就是“生命周期变隐式”的含义：一次 `asyncio_run(...)` 调用结束后，异步世界里可能还有残留 task、callback、future、连接清理逻辑没有完成。

## 8. `done callback` 为什么会让异常位置更不直观

XTuner 的 `create_task` 默认会给 task 加一个 done callback：

```python
def handle_task_exception(task):
    try:
        exc = task.exception()
        if exc is not None:
            raise exc
    except asyncio.CancelledError:
        pass
```

也就是说，如果 task 结束时带异常，这个 callback 会把异常重新 raise 出来。

如果 task 是被主流程明确 await 的：

```python
result = await task
```

异常会沿着当前调用链清晰地抛出。排查时能看到是当前 step 的当前 await 出了问题。

但如果 task 是残留的后台 task，没有被当前主流程 await，那么异常可能发生在 event loop callback 里。表现可能是：

```text
Exception in callback handle_task_exception(...)
RuntimeError: ...
```

这种异常不一定会成为当前 `asyncio_run(...)` 的直接返回异常。它更像是 event loop 在调度某个旧 callback 时打出来的错误。

因此排查难度会变高：

```text
日志时间点属于当前 step
异常来源可能属于上一个 step 遗留的 task
主训练流程可能还在等待 replay buffer / Ray actor / rollout 状态
```

## 9. 以 XTuner 当前代码为例的风险点

当前代码中，共卡路径多次通过 `asyncio_run` 从同步 trainer 进入异步逻辑：

```text
xtuner/v1/train/rl_trainer.py
```

典型调用包括：

```python
asyncio_run(self._run_initial_evaluate())
```

```python
produce_result = asyncio_run(
    self.agent_loop_manager.produce_batch(...)
)
```

```python
eval_log_info.update(asyncio_run(self._run_evaluation(train_step)))
```

这类调用本身符合 `asyncio_run` 的定位：同步 trainer 顶层进入 async rollout/eval。

但这也要求 `produce_batch()` 内部必须把本轮创建的 task 都明确收口。相关链路中会创建 task：

```text
xtuner/v1/rl/agent_loop_manager/producer.py
```

例如：

```python
return create_task(
    ctx.generate_group(...)
)
```

然后 `AgentLoop.generate_group()` 内部还会继续创建 task：

```text
xtuner/v1/rl/agent_loop/agent_loop.py
```

```python
task = create_task(self.generate_sample(state, **kwargs))
```

正常情况下，这些 task 会被 `await`、`task.result()`、`pause_produce()` 等逻辑收掉。

但是如果异常路径中没有完整执行收口逻辑，就可能出现：

```text
某个 rollout task 仍然挂在共享 event loop 上
下一次 asyncio_run(...) 时继续运行
旧 task 继续写 replay buffer
旧 task 继续调用 rollout controller
旧 task 在 done callback 里报错
```

这类问题不会像 loop mismatch 那样总是稳定复现。它可能表现为：

- 某一步训练日志里出现来自旧 task 的异常；
- replay buffer 状态和当前 step 预期不一致；
- trainer 等待 batch ready 时卡住；
- Ray actor 或 rollout worker 的状态切换变得难以解释；
- 错误栈指向 callback，而不是清晰指向当前训练 step 的主调用链。

另一个当前代码里的具体风险是：`asyncio_run` 也被用在共享底层同步 API 中。

例如：

```text
xtuner/v1/rl/agent_loop_manager/agent_loop_manager.py
```

里面有：

```python
asyncio_run(self.replay_buffer.save(checkpoint_path))
```

以及：

```python
asyncio_run(self.replay_buffer.resume(checkpoint_path))
```

这类代码如果只从同步 trainer 中调用，问题不大；但如果从已经处在 async event loop 内的非共卡 trainer 调用，就会触发：

```text
RuntimeError: asyncio_run does not support being called from a running event loop.
```

非共卡路径中：

```text
RLDisaggregatedTrainer.fit()
  -> asyncio_run(self._fit())
    -> await self._sync_weights_and_save(...)
      -> self._maybe_save_checkpoint(...)
        -> self.agent_loop_manager.save(...)
          -> asyncio_run(self.replay_buffer.save(...))
```

这就是在一个已经运行的 event loop 内部再次调用 `asyncio_run`。当前 helper 明确禁止这种用法，因此只要非共卡路径打开 checkpoint 保存，就可能触发运行时错误。

## 10. 小结

当前 `asyncio_run` 的设计动机是合理的：它解决的是同步共卡 trainer 反复调用 async rollout 时，每次使用标准 `asyncio.run` 都会创建新 loop，从而导致跨 step 复用对象出现 loop mismatch 的问题。

它的核心价值是：

```text
让同步 trainer 多次进入 async rollout 时使用同一个 event loop
避免 Future/Task/client/Ray awaitable 绑定到不同 loop
```

但它也带来两个需要特别注意的约束：

```text
第一，asyncio_run 只能作为同步代码进入 async 世界的边界，不能在已有 event loop 内部调用。

第二，共享 event loop 不会在每次调用结束后自动关闭，因此每个 async 调用必须自己确保本轮 task 被 await、cancel 或 drain 干净。
```

如果这两个边界没有守住，就容易出现两类问题：

```text
在 async 链路里嵌套 asyncio_run，直接触发 RuntimeError。

旧 task 残留在共享 loop 中，导致异常延迟暴露、日志归因困难、状态污染或训练卡住。
```

## 11. 后续修复方向

基于上面的分析，后续修复可以按下面几个方向推进。这里重点描述原则和风险边界，具体代码改动可以按模块逐步落地。

### 11.1 收紧 `asyncio_run` 使用边界

`asyncio_run` 应该只出现在同步入口层，作为 sync-to-async 的桥。

可以接受的位置是：

```python
def fit(self):
    return asyncio_run(self._fit())
```

或者共卡同步 trainer 顶层调用：

```python
produce_result = asyncio_run(self.agent_loop_manager.produce_batch(...))
```

不建议出现在共享底层模块中，例如：

```text
agent_loop_manager
replay_buffer
producer
agent_loop
rollout worker
judger
```

这些模块既可能被同步 trainer 调用，也可能被 async trainer 调用。底层如果直接调用 `asyncio_run`，就容易在 async 链路中触发嵌套 event loop 问题。

当前已经能明确看到一类直接报错问题：非共卡 async 训练链路中可能间接调用到底层同步 API，而底层同步 API 内部又调用 `asyncio_run`。

典型链路是：

```text
RLDisaggregatedTrainer.fit()
  -> asyncio_run(self._fit())
    -> await self._sync_weights_and_save(...)
      -> self._maybe_save_checkpoint(...)
        -> self.agent_loop_manager.save(...)
          -> asyncio_run(self.replay_buffer.save(...))
```

这会在已经运行的 event loop 内部再次调用 `asyncio_run`，触发：

```text
RuntimeError: asyncio_run does not support being called from a running event loop.
```

这类问题的修复方向比较清晰：共享底层模块应该优先提供 async API，例如 `save_async()` / `resume_async()`；同步 API 只作为薄 wrapper 保留给同步入口使用。

也就是说：

```text
async trainer 调用 await save_async(...)
sync trainer 调用 save(...)，由 save(...) 内部 asyncio_run(save_async(...))
```

这样可以把 `asyncio_run` 限制在同步入口层，避免它泄漏到已经处在 async event loop 的调用链里。

### 11.2 所有 `create_task` 都要有明确 owner

共享 event loop 下，`asyncio_run(coro)` 返回只代表传入的 `coro` 完成，不代表这个 loop 上所有 task 都完成。因此每个 `create_task` 都应该有明确 owner。

一个基本原则是：

```text
谁 create_task，谁负责 await / cancel / drain。
```

对于短生命周期并发，例如 `AgentLoop.generate_group()` 中并发生成组内多个 sample，可以考虑使用更结构化的并发方式，例如 `asyncio.TaskGroup`。这样子 task 的异常、取消和收口都能跟随当前调用链，异常也更容易从当前 step 暴露出来。

对于 `AsyncProduceStrategy` 这种需要跨一段时间持有 pending task 的生产策略，不能简单替换成 `TaskGroup`，但应该保证 pending task 集合有明确生命周期：正常完成、暂停生产、异常退出、trainer shutdown 时都必须被收口。

### 11.3 `produce_batch` 异常路径也要收口 pending task

共卡 `produce_batch()` 这类流程尤其要注意异常路径。正常路径里会执行：

```text
_produce_batch_to_buffer()
pause_produce()
_get_batch_from_buffer()
```

但如果 `_produce_batch_to_buffer()` 中途抛异常，仍然应该通过 `finally` 保证本轮已启动的 rollout task 被 pause、cancel 或 drain，避免残留 task 留到下一次 `asyncio_run(...)` 继续运行。

这点和共享 event loop 关系很大：如果 loop 每次关闭，残留 task 更容易被取消；但当前设计是复用 loop，因此异常路径必须由业务代码自己收口。

### 11.4 `handle_task_exception` 不宜在 callback 中裸 raise

`create_task` 的异常处理也可以进一步增强。当前 done callback 会重新 raise task 异常，这可能导致日志表现为：

```text
Exception in callback handle_task_exception(...)
```

这类异常不一定沿当前训练 step 的主调用链抛出，可能只是 event loop 在调度某个旧 task 的 callback 时打出来。因此它不一定能清楚说明异常属于哪个 train step、哪个 model step、哪个 task 或哪个 rollout uid。

更好的方向是：

```text
被 owner await 的 task：异常自然从 await 链路抛出。
无人 await 的后台 task：callback 记录带上下文的错误日志。
```

### 11.5 给 task 增加名称和训练上下文

为了让后台 task 的异常可定位，可以给 task 增加名称和上下文信息，例如：

```text
rollout:<task_name>:train_step-12:model_step-10
```

上下文里可以包含 train step、model step、task name、rollout uid、session uid 等。这样即使异常出现在 callback 日志中，也能判断它属于哪一次 rollout 生产，而不是只看到一段脱离业务语境的 traceback。

### 11.6 shutdown 时统一 cancel/drain

非共卡 shutdown 路径也应该保证彻底收口。不只是让 `produce_loop` 退出，还要确保 producer 下层 strategy 持有的 pending rollout task 被 cancel/drain，避免退出时留下未消费异常或未完成 Ray 调用。

也就是说，shutdown 的语义最好不只是：

```text
停止 producer loop
```

而应该是：

```text
停止 producer loop
停止继续调度新 rollout
取消或等待已有 pending task
消费 task terminal exception
确保 pending task 数量归零
```

### 11.7 补充异常路径测试

建议后续补充一些针对异常路径的测试：

- async trainer 保存 checkpoint 时不触发嵌套 `asyncio_run`；
- `_produce_batch_to_buffer()` 抛异常时仍会执行 pause/drain；
- `generate_group()` 内某个 sample task 抛异常时，其他兄弟 task 会被取消或收口；
- trainer shutdown 后 pending task 数量归零；
- 后台 task 异常日志能带上 train step、model step、task name 等上下文。

总体上，`asyncio_run` 可以继续保留，但它的边界需要收紧：

```text
它应该只存在于同步入口层。
底层共享模块应该优先 async 化。
所有 create_task 都要有明确 owner 和收口路径。
异常日志要能定位到具体训练上下文。
```
