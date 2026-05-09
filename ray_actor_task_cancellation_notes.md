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

| API | 目标 | 结果 |
| --- | --- | --- |
| `ray.cancel(ref)` | 一次 task / actor task | 让这个 `ObjectRef` 进入取消状态 |
| `ray.kill(actor)` | actor 进程和 actor 状态 | actor 死亡，后续调用失败 |

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

| 执行模型 | 取消如何被消费 |
| --- | --- |
| async actor | 协程到达下一个 `await` / event loop 调度点，收到 `CancelledError` |
| sync actor | 用户代码主动调用 `is_canceled()` 检查 |
| threaded actor | 用户代码主动调用 `is_canceled()` 检查 |

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
