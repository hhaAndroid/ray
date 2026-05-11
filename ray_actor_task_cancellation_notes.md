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

这一节单独解释一个容易混淆的问题：

```python
await asyncio.wait_for(actor.method.remote(), timeout=1)
```

超时后，actor method 会不会自动被 Ray 取消？

答案是：不会。

要理解这个问题，先要区分 Python asyncio 里的 `Future` / `Task`，以及 Ray 里的 `ObjectRef` / actor task。

### 16.1 Python asyncio 里的 Future 和 Task

在 asyncio 中，`Future` 表示一个未来会完成的结果。

`Task` 是 `Future` 的一种特殊形式。它包装了一个 coroutine，并且由当前 event loop 负责调度执行。

例如：

```python
import asyncio


async def work():
    await asyncio.sleep(10)
    return "done"


async def main():
    task = asyncio.create_task(work())
    result = await task
    print(result)


asyncio.run(main())
```

这里 `task` 既是一个可以 `await` 的对象，也是 `work()` 这段 coroutine 的实际执行句柄。

所以：

```python
task.cancel()
```

取消的是这个 asyncio task 本身。下一次 coroutine 被调度时，它会收到 `asyncio.CancelledError`。

这就是为什么下面的代码能取消本地 asyncio task：

```python
import asyncio


async def work():
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError: # 不捕获也可以正常取消
        print("work cancelled")
        raise


async def main():
    task = asyncio.create_task(work())
    try:
        await asyncio.wait_for(task, timeout=1)
    except asyncio.TimeoutError:
        print("timeout")
        print(task.cancelled())


asyncio.run(main())
```

`asyncio.wait_for(task, timeout=1)` 的语义是：

```text
等待 task 完成。
如果超时，取消这个 task。
等待取消过程完成，然后抛 TimeoutError。
```

在这个例子里，`task` 就是执行实体本身，所以 wait_for 超时会真正取消本地 coroutine。

### 16.2 Python asyncio 取消机制注意点

asyncio 的取消也是协作式取消，不是抢占式中断。

调用：

```python
task.cancel()
```

并不是立即把 coroutine 停在当前机器指令上，而是向这个 `Task` 发出取消请求。等 coroutine 下一次恢复执行时，asyncio 会在它当前等待的位置注入 `asyncio.CancelledError`。

所以最典型的取消点是 `await`：

```python
async def work():
    await asyncio.sleep(100)
```

如果外部取消这个 task，`CancelledError` 会在 `await asyncio.sleep(100)` 这里被抛出。

#### 16.2.1 不捕获取消异常也可以正常取消

下面这个函数不捕获 `CancelledError`，也可以正常取消：

```python
async def work():
    await asyncio.sleep(100)
```

取消发生时，`CancelledError` 会自动向外传播，task 最终进入 cancelled 状态。

捕获 `CancelledError` 通常是为了打日志或清理资源：

```python
async def work():
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        print("work cancelled")
        raise
```

这里最后的 `raise` 很重要。它表示清理完以后继续传播取消。

#### 16.2.2 捕获取消异常后不要随便吞掉

如果捕获 `CancelledError` 但不重新抛出，取消就可能被吞掉：

```python
async def work():
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        print("swallow cancellation")
        return "not cancelled"
```

这时外部虽然发起了取消，但 `work()` 自己把取消异常吃掉并正常返回。调用方看到的可能不是 cancelled，而是一个正常结果。

所以常见建议是：

```text
需要清理:
  except CancelledError:
      cleanup()
      raise

不需要清理:
  不捕获 CancelledError，让它自然传播。
```

#### 16.2.3 cancel() 不保证立刻完成

`task.cancel()` 只是请求取消。

如果 coroutine 在收到 `CancelledError` 后还有清理逻辑，取消完成要等清理逻辑跑完：

```python
async def work():
    try:
        await asyncio.sleep(100)
    finally:
        await cleanup()
```

因此 `asyncio.wait_for(task, timeout=1)` 超时后，会取消 task，并等待取消过程完成。实际总耗时可能略超过 `timeout`，因为它要等 coroutine 响应取消和收尾。

#### 16.2.4 没有 await 的长同步代码不会及时取消

asyncio 取消需要 coroutine 回到调度点。

如果 coroutine 里跑一大段同步 CPU 代码，中间没有 `await`，取消请求不会及时被处理：

```python
async def work():
    total = 0
    for i in range(10**12):
        total += i
    return total
```

这段代码会占住 event loop。即使外部调用了 `task.cancel()`，`CancelledError` 也要等 coroutine 重新回到 event loop 调度点后才有机会注入。

改法通常是拆 chunk，并周期性 `await`：

```python
async def work():
    total = 0
    for _ in range(100000):
        total += cpu_chunk()
        await asyncio.sleep(0)
    return total
```

`await asyncio.sleep(0)` 的作用是主动让出 event loop，让其他任务和取消请求有机会被处理。

#### 16.2.5 work 里创建的子 task 不一定自动取消

asyncio 不会因为“某些 task 是在 `work()` 里面创建的”，就自动把它们和 `work()` 绑定成父子生命周期。

例如：

```python
async def child():
    await asyncio.sleep(100)


async def work():
    t = asyncio.create_task(child())
    await asyncio.sleep(100)
```

如果外部取消 `work()` 对应的 task，被取消的是 `work()` 当前正在等待的 `asyncio.sleep(100)`。`t` 这个 child task 不会因为它是在 `work()` 里创建的就自动取消。它可能继续运行。

如果 `work()` 正在 await 这个 child task：

```python
async def work():
    t = asyncio.create_task(child())
    await t
```

那么取消 `work()` 时，取消会沿着当前 await 链路传到 `t`，child 通常也会被取消。

但这个行为来自“当前正在 await 它”，不是来自“它是在 work 里面创建的”。

#### 16.2.6 大量子 task 要显式管理

如果一个 coroutine 创建了大量子 task，最好显式管理它们的取消：

```python
async def work():
    tasks = [asyncio.create_task(child(i)) for i in range(100)]
    try:
        return await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
```

这里的关键点是：

```text
父 coroutine 收到取消
  -> 显式取消所有 child task
  -> 等 child task 完成取消清理
  -> 重新 raise CancelledError
```

如果使用 Python 3.11+，可以考虑 `asyncio.TaskGroup`：

```python
async def work():
    async with asyncio.TaskGroup() as tg:
        for i in range(100):
            tg.create_task(child(i))
```

`TaskGroup` 是结构化并发。退出这个作用域时，它会管理组内任务的完成、异常和取消。父作用域被取消时，组内未完成任务也会被取消并等待收尾。

#### 16.2.7 gather 和取消

`asyncio.gather()` 常用于等待一组 awaitable：

```python
await asyncio.gather(task1, task2, task3)
```

如果等待 `gather()` 的外层 task 被取消，`gather()` 通常会把取消传播给它管理的未完成 awaitable。

但如果你用 `create_task()` 创建了子 task，却没有把它们纳入当前 await 链路、`gather()` 或 `TaskGroup`，它们就可能变成后台任务，继续运行。

所以经验规则是：

```text
创建了 task，就要有明确 owner。

要么 await 它；
要么放进 gather / TaskGroup；
要么在取消路径里显式 cancel 它。
```

#### 16.2.8 shield 会阻断取消传播

`asyncio.shield()` 可以保护里面的 awaitable，不让外层取消直接传进去。

例如：

```python
task = asyncio.create_task(work())

try:
    await asyncio.wait_for(asyncio.shield(task), timeout=1)
except asyncio.TimeoutError:
    print("timeout, but task is still running")
```

这里 `wait_for` 超时后，只取消外层这次等待，不取消 `task` 本身。

所以 `shield()` 的语义是：

```text
取消当前等待者可以；
不要把这个取消传播到底层 awaitable。
```

这在“本地等待设置超时，但底层任务继续跑”时很有用。

#### 16.2.9 asyncio 小结

Python asyncio 的取消可以记成：

```text
Task.cancel():
  请求取消本地 asyncio task。

CancelledError:
  在 coroutine 的调度点注入。

不捕获 CancelledError:
  正常传播取消。

捕获后 raise:
  做清理后继续取消。

捕获后不 raise:
  可能吞掉取消。

create_task():
  只创建后台 task，不自动建立父子取消关系。

gather / TaskGroup:
  更适合管理一组子 task 的生命周期。

shield:
  阻断外层取消向内传播。
```

### 16.3 Ray actor method 的 ObjectRef 是什么

Ray actor method 调用是远端执行：

```python
ref = actor.method.remote()
```

这里发生的是：

```text
actor.method.remote()
  -> 向 Ray runtime 提交 actor task
  -> 返回 ObjectRef
```

`ObjectRef` 是远端结果的引用，不是 Python asyncio task。

如果写：

```python
future = ref.as_future()
```

它表示：

```text
创建一个本地 Python Future
注册回调：等 Ray ObjectRef ready 后，把结果填进这个 Future
```

这个本地 Future 只是等待 Ray 对象 ready 的结果接收器。

取消它：

```python
future.cancel()
```

含义是：

```text
取消这个本地等待器。
```

不是：

```text
取消 Ray 里的 actor method task。
```

Ray actor task 已经提交到 Ray runtime 中。要取消它，必须显式调用：

```python
ray.cancel(ref)
```

### 16.4 wait_for(ref) 超时会发生什么

如果直接 await Ray `ObjectRef`：

```python
ref = actor.long_running.remote()

try:
    result = await asyncio.wait_for(ref, timeout=1)
except asyncio.TimeoutError:
    print("local wait timed out")
```

超时后，影响的是当前这次本地 asyncio 等待。

Ray actor method 不会因为这个 timeout 自动取消。它可能仍然在 actor worker 上排队或执行。

后面仍然可以继续等待原始 `ObjectRef`：

```python
result = await ref
```

或者：

```python
result = ray.get(ref)
```

也就是说：

```text
asyncio.wait_for timeout:
  取消本地等待
  不自动 ray.cancel(ref)
```

### 16.5 wait_for(ref.as_future()) 和 shield

如果使用 `ref.as_future()`：

```python
ref = actor.long_running.remote()
future = ref.as_future()

try:
    await asyncio.wait_for(future, timeout=1)
except asyncio.TimeoutError:
    print("timeout")
```

`wait_for` 超时时，会取消它等待的本地 `future`。

这时通常是：

```text
future:
  被标记为 cancelled

Ray actor task:
  继续运行

ref:
  仍然有效
```

因此后面再等同一个 `future`，可能会直接失败：

```python
await future  # 可能抛 CancelledError
```

但仍然可以等原始 `ref`：

```python
result = await ref
```

如果只是想给这一次等待设置超时，但不想把本地 `future` 标记为 cancelled，可以用 `asyncio.shield()`：

```python
ref = actor.long_running.remote()
future = ref.as_future()

try:
    await asyncio.wait_for(asyncio.shield(future), timeout=1)
except asyncio.TimeoutError:
    print("timeout, but future is still usable")

result = await future
```

`shield()` 的含义是：

```text
外层 wait_for 超时时，只取消这次等待；
不要把取消传递给里面的 future。
```

注意：`shield()` 也不会取消或保护 Ray actor task。它只影响本地 asyncio Future 的取消传播。

### 16.6 超时后如果想取消 Ray actor method

如果业务语义是“等待 1 秒，超时就取消这次 actor method”，需要显式写：

```python
import asyncio
import ray


async def call_with_timeout(actor):
    ref = actor.long_running.remote()
    try:
        return await asyncio.wait_for(ref, timeout=1)
    except asyncio.TimeoutError:
        ray.cancel(ref)
        raise
```

这时有两个动作：

```text
asyncio.wait_for:
  控制本地等待超时。

ray.cancel(ref):
  请求 Ray runtime 取消 ref 对应的 actor task。
```

取消之后，actor method 能不能及时停下，仍然取决于前面讲过的 actor task cancellation 语义：

```text
还没执行:
  Ray 队列层可以直接取消。

async actor 正在 await:
  可以比较及时收到 CancelledError。

async actor 正在跑同步 CPU:
  不会及时取消。

sync / threaded actor 正在运行:
  只设置取消标记，需要用户代码检查 is_canceled()。
```

### 16.7 对比例子

本地 asyncio task 会被 wait_for 超时取消：

```python
import asyncio


async def local_work():
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        print("local work cancelled")
        raise


async def main():
    task = asyncio.create_task(local_work())
    try:
        await asyncio.wait_for(task, timeout=1)
    except asyncio.TimeoutError:
        print("local timeout")


asyncio.run(main())
```

Ray actor method 不会被 wait_for 超时自动取消：

```python
import asyncio
import time
import ray

ray.init()


@ray.remote
class Worker:
    def long_running(self):
        time.sleep(10)
        return "done"


async def main():
    w = Worker.remote()
    ref = w.long_running.remote()

    try:
        await asyncio.wait_for(ref, timeout=1)
    except asyncio.TimeoutError:
        print("local wait timed out")

    # Ray actor method 仍然可能继续执行。
    print(await ref)


asyncio.run(main())
```

超时后显式取消 Ray actor method：

```python
import asyncio
import time
import ray

ray.init()


@ray.remote
class Worker:
    def long_running(self):
        for _ in range(100):
            if ray.get_runtime_context().is_canceled():
                return "stopped"
            time.sleep(0.1)
        return "done"


async def main():
    w = Worker.remote()
    ref = w.long_running.remote()

    try:
        await asyncio.wait_for(ref, timeout=1)
    except asyncio.TimeoutError:
        ray.cancel(ref)
        print("cancel requested")

    try:
        await ref
    except ray.exceptions.TaskCancelledError:
        print("ray task cancelled")


asyncio.run(main())
```

## 17. 最终记忆

把这几个概念分开：

```text
asyncio.wait_for:
  本地等待超时控制。

asyncio.Task.cancel:
  取消本地 event loop 里的 coroutine task。

Future.cancel:
  取消这个 Future；是否取消底层工作取决于 Future 是否连接到底层取消接口。

ray.cancel(ref):
  取消 Ray runtime 里的 task / actor task。

ObjectRef:
  Ray 远端结果引用，不是 asyncio Task。
```

所以，`asyncio.wait_for` 超时不等于 `ray.cancel`。如果要取消 Ray actor method，必须显式调用 `ray.cancel(ref)`。
