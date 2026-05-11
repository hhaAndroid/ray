# Ray Actor Task 取消机制笔记

## 1. 先说结论

`ray.cancel(actor.method.remote())` 取消的不是 actor 本身，而是这一次 actor method 调用，也就是一个 actor task。

它的本质是：

1. 调用方根据返回的 `ObjectRef` 找到对应 task。
2. Ray 把这个 task 标记为 canceled。
3. 如果 task 还没开始执行，Ray 在队列层直接丢掉或跳过它。
4. 如果 task 已经开始执行，Ray 只能用对应执行模型支持的方式做协作式取消。

所以 actor task 取消不是抢占式中断。

更准确地说：

```text
未执行:
  Ray 可以直接取消，不需要用户代码配合。

已执行:
  async actor 依赖 asyncio cancellation。
  sync / threaded actor 依赖用户代码检查取消标记。
```

## 2. actor task 和 actor 的区别

下面这段代码里：

```python
ref = actor.work.remote()
ray.cancel(ref)
```

`ray.cancel(ref)` 只针对 `work.remote()` 这一次调用。

它不会销毁 actor，也不会清空 actor 内部状态。actor 后续仍然可以继续接收新的 method 调用。

如果目标是杀掉整个 actor，应该使用：

```python
ray.kill(actor)
```

这两个语义不同：


| API               | 目标                   | 结果                     |
| ----------------- | -------------------- | ---------------------- |
| `ray.cancel(ref)` | 一次 task / actor task | 让这个 `ObjectRef` 进入取消状态 |
| `ray.kill(actor)` | actor 进程和 actor 状态   | actor 死亡，后续调用失败        |


## 3. cancel 请求的主流程

actor method 调用会返回一个 `ObjectRef`。Ray 用这个 `ObjectRef` 找到它对应的 task，再找到这个 task 的 owner。

owner 是真正发起这次 actor method 调用的 worker 或 driver。owner 维护这次调用的 task 信息，所以取消请求最终要回到 owner 处理。

主流程可以简化成：

```text
ray.cancel(ref)
  -> 根据 ref 找 task owner
  -> owner 找到 task spec
  -> 判断是 actor task
  -> 标记 task canceled
  -> 按当前状态执行取消
```

actor task 不支持 `force=True`：

```python
ray.cancel(actor.method.remote(), force=True)
```

这会报错。

原因是 actor worker 是一个长期进程，里面保存 actor 状态，还会执行后续 method。Ray 不会为了取消其中一次 method 调用而直接杀掉整个 actor worker。

## 4. 状态一：还没发给 actor worker

这种情况通常发生在：

- actor method 的参数依赖还没 ready。
- 前面还有同一个 actor 的 method 在排队。
- caller 侧 actor submit queue 还没把任务发出去。

此时取消最简单：

```text
caller 侧 actor submit queue
  删除这个 task
  标记 TASK_CANCELLED
  对应 ObjectRef 变成取消错误
```

这个 task 不会到达 actor worker，用户代码完全不会执行。

后续：

```python
ray.get(ref)
```

会抛 `TaskCancelledError`。

## 5. 状态二：已经发给 actor worker，但还没开始执行

这时 owner 不能只删除本地队列，因为 task 已经到 actor worker 侧了。

Ray 会走一条 RPC 路径：

```text
owner
  -> actor 所在节点 raylet
  -> actor worker
```

actor worker 收到取消请求后，会在自己的 actor task execution queue 里查找这个 task。

如果 task 还在队列里，没有开始执行，Ray 会把它标记为 canceled。等队列调度到它时，发现已经取消，就不再执行用户函数，而是直接返回取消结果。

这种情况和“还没发给 actor worker”在用户视角基本一样：

```python
ray.get(ref)
```

通常还是 `TaskCancelledError`。

可以理解为：

```text
已发出，但未执行:
  task 到过 actor worker
  但用户代码没有真正跑
  最终还是 before execution cancellation
```

## 6. 状态三：已经开始执行

已经开始执行时，Ray 不能统一强行终止。

这时要看 actor 类型。

### 6.1 普通同步 actor

普通同步 actor 的 method 一旦进入 Python 用户代码，Ray 不能强行打断它。

取消请求到达后，Ray 会设置一个取消标记。用户代码如果想支持及时退出，需要主动检查：

```python
ray.get_runtime_context().is_canceled()
```

如果不检查，method 会继续跑完。

但要注意：即使 method 正常返回，调用方这边的 `ObjectRef` 仍然可能被视为 canceled，`ray.get(ref)` 看到的是取消错误，而不是正常返回值。

不能打断的核心原因是：普通同步 actor 的 method 是一段正在同步执行的 Python 用户代码，Ray 没有一个安全的“只中断这一次 method、但保留 actor 进程和 actor 状态”的机制。

更具体地说，有几个原因。

第一，同步代码没有调度点。

async actor 可以通过 `asyncio.Task.cancel()` 在下一个 `await` 注入 `CancelledError`。但同步 actor method 一旦进入普通 Python 函数调用，就没有 event loop 调度点，Ray 没法在任意一行安全插入异常。

例如：

```python
def f(self):
    while True:
        do_work()
```

这段代码如果不主动检查取消标记，Ray 不能从外部安全地把它停在某一行。

第二，actor 是有状态的长生命周期进程。

普通 task 被取消时，Ray 可以更激进一点，因为 task worker 可以结束或被回收。但 actor worker 里保存了 `self` 状态，后续还要继续接收 actor method。强行在任意位置打断，可能让 actor 状态处在半更新状态。

例如：

```python
def update(self):
    self.a = 1
    # 如果这里被强行打断
    self.b = 2
```

这时 actor 还活着，但状态可能已经不一致。Ray 不替用户定义这种事务语义。

第三，Python 线程也不能安全强杀。

threaded actor 更明显。Python 没有安全的通用机制去杀掉某个正在运行的线程。强杀线程可能导致锁没释放、对象状态损坏、C 扩展卡死等问题。

所以更准确的说法是：仅靠这次 `ray.cancel(ref)`，运行中的同步 actor method 不会被 Ray 中途打断。它后续可能正常返回、自己抛异常、被 `ray.kill(actor)` 杀掉、因为 worker / 节点失败而退出，或者一直阻塞不结束。但如果没有这些外部因素，也没有主动检查 `is_canceled()`，这段同步 method 会继续执行下去，直到它自己返回或失败。

### 6.2 threaded actor

threaded actor 和普通同步 actor 类似。

Ray 不会强行杀掉某个 Python 线程。取消也是设置标记，用户代码需要自己检查并退出。

因此 threaded actor 里的长 CPU 任务也要写成协作式取消。

### 6.3 async actor

async actor 不一样。

Ray 会找到这次 actor method 对应的 `asyncio.Task`，然后调用 `cancel()`。

这会让协程在下一个可取消点收到 `asyncio.CancelledError`。

但这仍然不是抢占式中断。`asyncio.Task.cancel()` 只是安排取消事件，只有协程重新回到 event loop 调度点时，取消才会被处理。

也就是说：

```text
async actor 正在 await:
  可以比较及时取消。

async actor 正在跑一大段同步 CPU 代码:
  event loop 被占住，取消不会及时生效。
```

## 7. “取消事件必须被消费”怎么理解

可以这样理解，但要区分未执行和已执行。

未执行的 task 不需要用户代码消费取消事件。Ray 在队列层就可以处理掉。

已执行的 task 才需要执行逻辑配合：


| 执行模型           | 取消如何被消费                                              |
| -------------- | ---------------------------------------------------- |
| async actor    | 协程到达下一个 `await` / event loop 调度点，收到 `CancelledError` |
| sync actor     | 用户代码主动调用 `is_canceled()` 检查                          |
| threaded actor | 用户代码主动调用 `is_canceled()` 检查                          |


如果没有可取消点，也没有主动检查，那么运行中的代码不会被及时停下。

所以更精确的结论是：

```text
取消请求一定会被记录到 Ray 的 task 状态里；
运行中的用户代码是否停下，取决于它有没有机会处理这个取消状态。
```

## 8. recursive=True

`ray.cancel()` 默认 `recursive=True`。

含义是：取消当前 task 时，也尝试取消它提交出来的子 task / 子 actor task。

例如 actor method 内部又调用了其他 remote task：

```python
@ray.remote
def child():
    ...

@ray.remote
class A:
    def run(self):
        ref = child.remote()
        return ray.get(ref)
```

如果取消：

```python
ref = a.run.remote()
ray.cancel(ref, recursive=True)
```

Ray 会尝试连 `child` 一起取消。

但 actor task 仍然不支持 force kill。即使外层普通 task 可以 `force=True`，递归取消到 actor task 时，也不会强杀 actor method。

## 9. 示例一：取消还没执行的 actor task

这个例子里 actor 是单并发的，第二个调用会排队。取消第二个调用时，它还没执行。

```python
import time
import ray

ray.init()

@ray.remote
class Worker:
    def slow(self):
        time.sleep(10)
        return "slow done"

    def fast(self):
        return "fast done"

w = Worker.remote()

first = w.slow.remote()
second = w.fast.remote()

ray.cancel(second)

try:
    ray.get(second)
except ray.exceptions.TaskCancelledError:
    print("second was cancelled before execution")

print(ray.get(first))
```

这里 `fast()` 通常不会真正进入 actor 执行。

## 10. 示例二：sync actor 运行中取消

这个例子里 method 已经开始执行。Ray 不能强行打断同步 Python 代码，所以需要主动检查取消标记。

```python
import time
import ray

ray.init()

@ray.remote
class Worker:
    def loop(self):
        for i in range(100):
            if ray.get_runtime_context().is_canceled():
                print("detected cancellation")
                return "stopped early"
            time.sleep(0.1)
        return "done"

w = Worker.remote()
ref = w.loop.remote()

time.sleep(1)
ray.cancel(ref)

try:
    ray.get(ref)
except ray.exceptions.TaskCancelledError:
    print("caller sees TaskCancelledError")
```

注意这里即使 actor method 里 `return "stopped early"`，调用方仍可能看到 `TaskCancelledError`。取消语义优先于这次调用的正常返回。

如果不写 `is_canceled()` 检查，这个 method 会继续跑到结束。

## 11. 示例三：async actor 正常可取消

async actor 在 `await` 处会让出 event loop，因此取消能比较及时生效。

```python
import asyncio
import ray

ray.init()

@ray.remote
class Worker:
    async def wait(self):
        try:
            await asyncio.sleep(100)
        except asyncio.CancelledError:
            print("async task got CancelledError")
            raise

w = Worker.remote()
ref = w.wait.remote()

ray.cancel(ref)

try:
    ray.get(ref)
except ray.exceptions.TaskCancelledError:
    print("caller sees TaskCancelledError")
```

这里取消会注入到 `asyncio.Task`，协程内部能捕获 `asyncio.CancelledError`。

## 12. 示例四：async actor 里跑同步 CPU 代码

这个例子虽然是 async actor，但 method 里面没有 `await`，而是在 event loop 线程里跑同步 CPU 循环。

```python
import ray

ray.init()

@ray.remote
class Worker:
    async def cpu_bound(self):
        total = 0
        for i in range(10**12):
            total += i
        return total

w = Worker.remote()
ref = w.cpu_bound.remote()

ray.cancel(ref)

ray.get(ref)
```

这个取消不会及时生效。

原因是 `asyncio.Task.cancel()` 需要协程回到 event loop 调度点后才会处理。上面的循环一直占住 event loop，没有 `await`，所以取消事件没有机会被消费。

这里不能用同步 actor 的写法：

```python
ray.get_runtime_context().is_canceled()
```

原因是 `is_canceled()` 在 async actor 中不支持。即使 async actor method 内部调用的是一个普通同步 CPU 函数，只要当前 actor 是 async actor，调用 `ray.get_runtime_context().is_canceled()` 就会报错。

async actor 的取消模型是：

```text
ray.cancel(ref)
  -> 找到这次 actor method 对应的 asyncio.Task
  -> 调用 asyncio.Task.cancel()
  -> 等协程在下一个 await 点收到 CancelledError
```

所以 async actor 里的同步 CPU 函数不能靠 `is_canceled()` 感知 Ray 取消。它必须让外层协程有机会回到 event loop，或者自己设计额外的 stop flag。

改法是把 CPU 任务拆成小块，中间主动让出 event loop：

```python
import asyncio
import ray

ray.init()

def cpu_chunk():
    total = 0
    for i in range(10**6):
        total += i
    return total

@ray.remote
class Worker:
    async def cpu_bound(self):
        total = 0
        for _ in range(100000):
            total += cpu_chunk()
            await asyncio.sleep(0)
        return total
```

`await asyncio.sleep(0)` 不是为了睡眠，而是为了给 event loop 一个调度机会，让取消能被处理。

也可以把 CPU 函数丢到 executor：

```python
import asyncio
import ray

ray.init()

def heavy_cpu_work():
    total = 0
    for i in range(10**12):
        total += i
    return total

@ray.remote
class Worker:
    async def run(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, heavy_cpu_work)
```

但这也有一个限制：取消 outer coroutine 只会取消等待，不会强行杀掉 executor 里的线程函数。要真正停掉 CPU 函数，仍然要让 CPU 函数自己检查 stop flag，或者拆成更小的 Ray task。

如果用 executor，并且希望后台 CPU 函数也能停，可以显式传一个 stop flag：

```python
import asyncio
import threading
import ray

ray.init()


def heavy_cpu_work(stop):
    total = 0
    while not stop.is_set():
        for i in range(10**6):
            total += i
    return total

@ray.remote
class Worker:
    async def run(self):
        stop = threading.Event()
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(None, heavy_cpu_work, stop)
        try:
            return await fut
        except asyncio.CancelledError:
            stop.set()
            raise
```

这里 `CancelledError` 只负责通知 outer coroutine 已取消，真正让 CPU 函数停下的是 `stop.set()` 和 `heavy_cpu_work()` 内部的周期性检查。

另一种做法是把重 CPU 工作拆成普通 Ray task，再依赖 `recursive=True` 递归取消子 task。但这更适合任务边界清楚、可以接受拆分调度开销的场景。

## 13. 示例五：recursive cancel

这个例子中 actor method 内部提交了子 task。

```python
import time
import ray

ray.init()

@ray.remote
def child():
    time.sleep(100)
    return "child done"

@ray.remote
class Worker:
    def parent(self):
        ref = child.remote()
        return ray.get(ref)

w = Worker.remote()
ref = w.parent.remote()

ray.cancel(ref, recursive=True)

try:
    ray.get(ref)
except ray.exceptions.TaskCancelledError:
    print("parent was cancelled")
```

`recursive=True` 会让 Ray 尝试取消 `parent()` 里提交出来的 `child()`。

如果写成：

```python
ray.cancel(ref, recursive=False)
```

Ray 只取消 `parent()` 这次 actor task，不主动递归取消它提交出来的子 task。

## 14. 实用判断规则

排查 actor task 取消时，可以按下面顺序判断。

第一，取消的是 actor 还是 actor method？

```text
ray.cancel(ref): 取消一次 method 调用
ray.kill(actor): 杀整个 actor
```

第二，取消时 task 是否已经开始执行？

```text
未开始:
  Ray 队列层可以直接取消。

已开始:
  进入协作式取消。
```

第三，actor 是哪种执行模型？

```text
sync actor:
  需要 is_canceled()。

threaded actor:
  需要 is_canceled()，并注意线程安全。

async actor:
  依赖 await / event loop 调度点。
```

第四，代码里是否有不可中断的长同步段？

```text
长 CPU 循环、长时间 C 扩展调用、阻塞 IO:
  都可能让取消不能及时生效。
```

第五，是否需要递归取消子任务？

```text
recursive=True:
  取消当前 task 以及它提交出来的子 task / actor task。

recursive=False:
  只取消当前 task。
```

## 15. 一句话总结

Ray actor task cancellation 是协作式取消。

未执行的 actor task 可以在队列层被 Ray 直接丢弃；已经执行的 actor task 需要执行模型配合：async actor 等待下一个 event loop 调度点，同步和 threaded actor 需要用户代码检查取消标记。取消请求会让这次 `ObjectRef` 进入取消语义，但不等于强行终止 actor 或抢占正在运行的 Python 代码。

## 16. asyncio wait_for 超时和 Ray task 取消的区别

这一节只保留 Ray 侧结论。Python `asyncio` 的 `Future`、`Task`、`CancelledError`、`wait_for`、`shield`、`gather`、`TaskGroup` 和同步 CPU 段取消延迟等基础语义，统一看 [python_asyncio_cancellation_notes.md](/mnt/shared-storage-user/huanghaian/code/ray/python_asyncio_cancellation_notes.md:1)。

核心区别是：

```text
asyncio.wait_for:
  控制本地 asyncio 等待超时。

ray.cancel(ref):
  请求 Ray runtime 取消 ref 对应的 task / actor task。
```

因此：

```python
ref = actor.long_running.remote()

try:
    result = await asyncio.wait_for(ref, timeout=1)
except asyncio.TimeoutError:
    print("local wait timed out")
```

这里超时只表示本地这次等待结束了，Ray actor method 不会因为 `wait_for` 超时而自动取消。actor method 可能仍然在 actor worker 上排队或执行。

如果业务语义是“超时后取消这次 actor method”，需要显式调用：

```python
ref = actor.long_running.remote()

try:
    result = await asyncio.wait_for(ref, timeout=1)
except asyncio.TimeoutError:
    ray.cancel(ref)
    raise
```

此时仍然要回到 Ray actor task cancellation 的语义：

```text
还没执行:
  Ray 队列层可以直接取消。

async actor 正在 await:
  通过 asyncio.Task.cancel() 协作式取消，具体 Python 语义见 python_asyncio_cancellation_notes.md。

async actor 正在跑同步 CPU:
  event loop 被占住，不会及时取消。

sync / threaded actor 正在运行:
  Ray 只设置取消标记，需要用户代码检查 is_canceled()。
```

如果使用 `ref.as_future()`，`asyncio.wait_for(future, timeout=1)` 超时取消的是本地 Future 包装，不会自动反向调用 `ray.cancel(ref)`。原始 `ObjectRef` 仍然代表 Ray 里的远端结果引用。

## 17. XTuner worker_task 取消代码分析

本节分析 XTuner 中这段代码：

```text
/mnt/shared-storage-user/huanghaian/code/temp/xtuner/xtuner/v1/ray/dataflow/flow.py:261-276
```

### 17.1 原始代码逐行解释

原始代码：

```python
# 发起一次远端 Ray actor method 调用。
# env_run_ref 是 Ray ObjectRef，对应 env_controller.run.remote(...) 这次远端 actor task。
# 注意：它不是本地 asyncio.Task。
env_run_ref = self.env_controller.run.remote(  # type: ignore[attr-defined]
    group_data_items,
    sample_params=self.sample_params,
    extra_params=self.extra_params,
)

try:
    # 等 env_run_ref 对应的远端 Ray 调用完成。
    #
    # shield 的作用是阻断外层 asyncio 取消直接作用到这个 awaitable。
    # 这里的设计意图是：本地 worker_task 被取消时，不让 asyncio 的取消语义
    # 和 Ray 的取消语义混在一起；远端 Ray task 的取消交给下面的 ray.cancel。
    group_data_items = await asyncio.shield(env_run_ref)

except asyncio.CancelledError as exc:
    # 当前 worker_task 这个本地 asyncio task 被取消了。
    #
    # 但本地 asyncio task 被取消，并不等于 env_run_ref 对应的 Ray actor method
    # 已经被取消。所以这里显式调用 ray.cancel。
    #
    # recursive=True 是 Ray 的语义：让 Ray 尝试递归取消 env_controller.run
    # 在运行期间提交出来的子 Ray task / actor task。
    ray.cancel(env_run_ref, recursive=True)

    try:
        # 发出 Ray cancel 后，再给远端 env_run_ref 一点时间进入终态。
        #
        # 如果 env_controller.run 能及时返回 aborted / failed / skipped 等业务结果，
        # 这里可以拿到 group_data_items，后续继续走 replay buffer / group_state 处理。
        #
        # 如果 env_run_ref 变成 TaskCancelledError、RayActorError，或者超时，
        # 会进入下面的 except。
        group_data_items = await asyncio.wait_for(
            asyncio.shield(env_run_ref),
            timeout=self.cancel_response_timeout,
        )
    except BaseException:
        # 如果取消后等待远端响应失败，重新抛出最初的 asyncio CancelledError。
        #
        # 注意：这个写法会丢掉这里捕获到的真实异常上下文。
        # 例如 wait_for 超时、Ray task cancelled、Ray actor died 等信息都会被遮住。
        raise exc
```

这段代码的整体意图是合理的：

```text
本地 worker_task 被取消
  -> 显式 ray.cancel(env_run_ref, recursive=True)
  -> 尝试等待远端 env_run_ref 响应取消
  -> 如果远端及时返回业务结果，则继续处理
  -> 如果远端没及时响应，则向外传播 worker_task 的取消
```

### 17.2 这段代码的关键语义

这里同时存在两层取消：

```text
asyncio cancel:
  取消本地 worker_task 这个 asyncio.Task。

ray.cancel:
  取消 env_run_ref 对应的远端 Ray actor task。
```

这两层不是同一个东西。

外层调用：

```python
task.cancel()
```

只会取消本地 `worker_task()`。它不会自动等价于：

```python
ray.cancel(env_run_ref)
```

所以 `except asyncio.CancelledError` 里显式 `ray.cancel(env_run_ref, recursive=True)` 是必要的。

### 17.3 当前写法的隐含业务语义

当前代码在一种情况下会“吞掉本地取消”：

```text
worker_task 收到 asyncio.CancelledError
  -> ray.cancel(env_run_ref)
  -> env_run_ref 在 cancel_response_timeout 内正常返回 group_data_items
  -> worker_task 继续执行后续 group_state 逻辑
  -> worker_task 最终可能正常 return
```

这是否正确，取决于业务语义。

如果设计目标是：

```text
取消 worker_task 后，如果远端能返回 aborted / skipped / failed 样本，
就继续把这些状态写回 replay buffer / metrics。
```

那么当前行为是合理的。

如果设计目标是：

```text
只要 worker_task 收到取消，无论远端是否返回，都必须最终 cancelled。
```

那么拿到 `group_data_items` 后仍然应该 `raise exc`，而不是继续往下走。

### 17.4 当前写法的问题

第一，`except BaseException: raise exc` 太粗。

它会隐藏取消收尾阶段的真实异常。

更好的最低限度写法是：

```python
except BaseException as wait_exc:
    raise exc from wait_exc
```

这样外层仍然看到原始取消，但异常链里保留了收尾失败的原因。

第二，第二次 `asyncio.shield(env_run_ref)` 不一定有必要。

第二次等待已经处在取消收尾路径：

```text
ray.cancel 已经发出；
最多等 cancel_response_timeout；
等不到就放弃等待并继续取消。
```

这时没有必要保护本地等待继续存在。`wait_for` 超时后取消本地这次等待是合理的。

第三，第一次 `shield(env_run_ref)` 也不是绝对必要。

即使不用 `shield`，本地 `await env_run_ref` 被取消后，原始 `env_run_ref` 仍然是 Ray ObjectRef。代码仍然可以在 `except asyncio.CancelledError` 里显式 `ray.cancel(env_run_ref)`，并再次等待它进入终态。

因此更推荐把逻辑写成“普通 await + 显式 Ray cancel + bounded drain”，语义更直接。

### 17.5 改进写法一：保留当前业务语义

这个版本保留当前行为：如果远端在取消后及时返回 `group_data_items`，就继续处理后续 group state。

```python
env_run_ref = self.env_controller.run.remote(  # type: ignore[attr-defined]
    group_data_items,
    sample_params=self.sample_params,
    extra_params=self.extra_params,
)

try:
    # 正常路径：等待远端 Ray actor method 返回。
    #
    # 这里不使用 shield。worker_task 被取消时，本地 await 会抛 CancelledError；
    # 但这只取消本地等待，不等于取消远端 Ray task。
    # 远端 Ray task 的取消在 except 分支里显式处理。
    group_data_items = await env_run_ref

except asyncio.CancelledError as exc:
    # 本地 worker_task 被取消后，显式取消远端 Ray actor task。
    ray.cancel(env_run_ref, recursive=True)

    try:
        # 给远端 Ray task 一个有限窗口进入终态。
        # 如果它及时返回业务结果，保留当前语义：继续处理 group_data_items。
        group_data_items = await asyncio.wait_for(
            env_run_ref,
            timeout=self.cancel_response_timeout,
        )
    except BaseException as wait_exc:
        # 远端没有及时完成、或者返回 Ray 取消/失败异常。
        # 继续传播本地 worker_task 的取消，同时保留收尾失败的 cause。
        raise exc from wait_exc
```

这个版本的特点：

```text
优点:
  取消链路清楚；
  不混用 shield；
  保留 wait_exc 作为异常 cause；
  保留“取消后远端及时返回则继续处理”的业务语义。

注意:
  worker_task 收到取消后，仍可能最终正常 return。
```

### 17.6 改进写法二：取消后一定向外传播取消

如果业务希望 worker_task 一旦被取消，就必须最终表现为 cancelled，可以写成：

```python
env_run_ref = self.env_controller.run.remote(  # type: ignore[attr-defined]
    group_data_items,
    sample_params=self.sample_params,
    extra_params=self.extra_params,
)

try:
    group_data_items = await env_run_ref

except asyncio.CancelledError as exc:
    ray.cancel(env_run_ref, recursive=True)

    try:
        # 可选：等待远端收尾。
        # 即使拿到结果，也不继续正常返回，只用于让远端有机会清理。
        await asyncio.wait_for(
            env_run_ref,
            timeout=self.cancel_response_timeout,
        )
    except BaseException as wait_exc:
        raise exc from wait_exc

    # 远端及时结束，但本地 worker_task 仍然保持 cancelled 语义。
    raise exc
```

这个版本的特点：

```text
优点:
  worker_task 的取消语义更一致；
  外层 task.cancel() 后，最终一定看到 worker_task 被取消。

代价:
  远端返回的 aborted / failed / skipped 业务结果不会继续进入后续 group_state 处理。
```

### 17.7 和 SingleTurnEnvironment 的关系

`env_run_ref` 实际对应：

```text
SingleTurnEnvironment.run()
  -> generate()
      -> rollout_controller.rollout.remote(...)
  -> judger_controller.run.remote(...)
```

因此 `ray.cancel(env_run_ref, recursive=True)` 会让 Ray 尝试递归取消它能追踪到的子 Ray tasks。

但内层代码仍然应该处理自己的下游 refs。

原因是：

```text
DataFlow 只知道 env_run_ref；
SingleTurnEnvironment.generate 才知道 rollout response refs；
SingleTurnEnvironment.run 才知道 judger_response_ref。
```

内层显式取消这些 refs 是合理的防御式设计，也能做业务级收尾，例如等待 rollout 返回 aborted/skipped/failed 状态。

## 18. 最终记忆

Ray 侧只记住这几件事：

```text
asyncio.wait_for:
  本地等待超时控制，不自动取消 Ray task。

ray.cancel(ref):
  取消 Ray runtime 里的 task / actor task。

ObjectRef:
  Ray 远端结果引用，本身不是 Python asyncio Task。
```

所以，`asyncio.wait_for` 超时不等于 `ray.cancel`。如果要取消 Ray actor method，必须显式调用 `ray.cancel(ref)`。

Python asyncio 自身的取消细节统一参考 [python_asyncio_cancellation_notes.md](/mnt/shared-storage-user/huanghaian/code/ray/python_asyncio_cancellation_notes.md:1)。
