# Python asyncio 取消机制笔记

## 1. 文档目标

这份笔记只讨论 Python `asyncio` 自身的取消机制，不讨论 Ray。

重点回答几个问题：

1. `asyncio.Task.cancel()` 到底做了什么。
2. `CancelledError` 是怎么传播的。
3. `asyncio.wait_for()` 超时时会取消谁。
4. `asyncio.shield()` 为什么存在。
5. coroutine 内部创建子 task 后，外层取消时子 task 会不会一起取消。
6. `gather()` 和 `TaskGroup` 在取消传播上的差异。
7. async 函数里跑同步 CPU 代码时，取消为什么会延迟。

## 2. 核心概念

### 2.1 event loop

`asyncio` 是基于 event loop 的协作式并发模型。

一个 coroutine 只有在 `await` 某个 awaitable、主动让出控制权时，event loop 才有机会调度其他 task。

这意味着：

```text
asyncio 不是抢占式调度。
```

如果一个 async 函数里长时间执行同步代码，并且中间没有 `await`，event loop 就会被占住。

### 2.2 coroutine

`async def` 调用后返回 coroutine 对象。

```python
async def work():
    return 1


coro = work()
```

这个 `coro` 本身还没有真正运行。它需要被 `await`，或者被包装成 `Task` 后交给 event loop 调度。

### 2.3 Future

`Future` 表示一个未来会完成的结果。

它可以处于几种状态：

```text
pending
done with result
done with exception
cancelled
```

`Future` 不一定代表一个正在执行的 coroutine。它也可能只是某个外部事件的结果接收器即回调

### 2.4 Task

`Task` 是 `Future` 的子类。它包装 coroutine，并由 event loop 调度执行。

```python
task = asyncio.create_task(work())
```

这里 `task` 有两层含义：

```text
它是一个 Future，可以被 await。
它也是 work() 这段 coroutine 的执行句柄。
```

因此：

```python
task.cancel()
```

取消的是这个本地 asyncio task 对应的 coroutine 执行。

## 3. 核心原则

### 3.1 asyncio 取消是协作式取消

`task.cancel()` 不是强行杀线程，也不是立刻把 coroutine 停在当前机器指令上。

它的语义是：

```text
向 task 发出取消请求；
等 coroutine 下一次恢复执行时；
在当前 await 点注入 CancelledError。
```

最典型的取消点是 `await`：

```python
async def work():
    await asyncio.sleep(100)
```

如果外部取消 `work()` 对应的 task，`CancelledError` 通常会在 `await asyncio.sleep(100)` 这里抛出，然后整个当前 task 退出。

### 3.2 取消请求不等于取消已经完成

`task.cancel()` 只是请求取消。

task 什么时候真正进入 cancelled 状态，取决于 coroutine 是否响应取消、是否做清理、是否吞掉了 `CancelledError`。

### 3.3 子 task 不会因为“在某个 coroutine 里创建”就自动归它管理

这点非常重要。

```python
async def work():
    t = asyncio.create_task(child())
    await asyncio.sleep(100)
```

`t` 不会因为它是在 `work()` 里创建的，就自动绑定到 `work()` 的生命周期。

如果取消 `work()`，`t` 不一定被取消。

但如果 `work()` 当前等待的正好是 `t`：

```python
async def work():
    t = asyncio.create_task(child())
    await t
```

取消 `work()` 时，取消会沿当前 await 链路传给 `t`。这不是因为 `t` 是在 `work()` 里创建的，而是因为 `work()` 正在等待它。

### 3.4 谁创建 task，谁负责生命周期

经验规则：

```text
不要裸 create_task 后不管。

创建 task 后，要明确：
  谁 await 它；
  谁 cancel 它；
  谁处理它的异常；
  谁在 shutdown 时等待它收尾。
```

如果确实要后台任务，需要有 supervisor 或明确 owner。

## 4. CancelledError

### 4.1 不捕获也能正常取消

下面这个 coroutine 不捕获 `CancelledError`，也可以正常取消：

```python
import asyncio

async def work():
    await asyncio.sleep(100)
```

取消发生时，`CancelledError` 会自动向外传播，task 最终进入 cancelled 状态。

### 4.2 捕获取消异常是为了清理

如果需要打日志或释放资源，可以捕获 `CancelledError`：

```python
async def work():
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        print("work cancelled")
        raise
```

这里最后的 `raise` 很重要。它表示清理完成后继续传播取消。

### 4.3 不要无意吞掉 CancelledError

如果捕获后不重新抛出，取消可能被吞掉：

```python
async def work():
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        print("swallow cancellation")
        return "not cancelled"
```

这时外部发起了取消，但 `work()` 自己把取消异常转成了正常返回。

调用方可能看到：

```text
task 正常完成
返回值是 "not cancelled"
```

而不是：

```text
task 被取消
```

这里要和另一个情况区分开：`task.cancel()` 只是发出取消请求，取消请求和 task 正常完成之间可能存在竞争。

如果取消请求还没有真正注入到 coroutine，task 就已经正常返回了，那么调用方也可能看到正常完成：

```text
cancel request 发出
CancelledError 还没注入
task 正好 return
调用方看到正常结果
```

如果 `CancelledError` 已经注入，并且 coroutine 没有吞掉它，而是继续向外传播，那么调用方看到的是取消：

```text
cancel request 发出
CancelledError 注入
coroutine 没有捕获，或者捕获后重新 raise
调用方看到 task 被取消
```

如果 `CancelledError` 已经注入，但 coroutine 捕获后没有重新抛出，而是正常 `return`，那么调用方通常看到正常完成：

```text
cancel request 发出
CancelledError 注入
coroutine 捕获并 return
调用方看到正常结果
```

所以准确地说：

```text
task.cancel() 只是请求取消；
最终是正常完成还是 cancelled，
取决于取消是否来得及注入，以及 coroutine 是否传播 CancelledError。
```

### 4.4 推荐写法

不需要清理时：

```python
async def work():
    await do_something()
```

需要清理时：

```python
async def work():
    try:
        await do_something()
    except asyncio.CancelledError:
        await cleanup()
        raise
```

或者：

```python
async def work():
    try:
        await do_something()
    finally:
        await cleanup()
```

需要注意，`finally` 里的异步清理也会影响取消完成时间。

## 5. wait_for

### 5.1 wait_for 的语义

`asyncio.wait_for(aw, timeout)` 的语义是：

```text
等待 aw 完成；
如果 timeout 前完成，返回结果；
如果超时，取消 aw；
等待 aw 响应取消；
然后抛 TimeoutError。
```

例子：

```python
import asyncio


async def work():
    await asyncio.sleep(100)

async def main():
    task = asyncio.create_task(work())
    try:
        await asyncio.wait_for(task, timeout=1)
    except asyncio.TimeoutError:
        print("timeout")
        print(task.cancelled())

asyncio.run(main())
```

这里 `task` 是本地 asyncio task，也是 `work()` 的执行句柄。所以 `wait_for` 超时会取消这个 task。

wait_for 是一个外层等待器；它超时时，会主动对它正在等待的 awaitable 调用取消。asyncio.wait_for() 的设计语义就是：如果超时，就取消正在等待的 awaitable。而你传进去的 awaitable 正好就是：task。

这和 shield 的区别是  await asyncio.wait_for(asyncio.shield(task), timeout=1) 这时 wait_for 等待的是 shield(task) 这个外层保护对象。超时时取消的是外层等待，不会把取消传给里面的 task。因此 task 继续跑。

### 5.2 wait_for 超时不一定严格等于 timeout 时间

因为 `wait_for` 超时后会等待被取消的 awaitable 完成取消处理，所以实际耗时可能超过 `timeout`。

例如：

```python
import asyncio

async def cleanup():
    await asyncio.sleep(2)

async def work():
    try:
        await asyncio.sleep(100)
    finally:
        await cleanup()

async def main():
    task = asyncio.create_task(work())
    try:
        await asyncio.wait_for(task, timeout=1)
    except asyncio.TimeoutError:
        print("timeout after cancellation cleanup")

asyncio.run(main())
```

这里 `wait_for(..., timeout=1)` 可能要等 `cleanup()` 结束后才返回 `TimeoutError`。

### 5.3 wait_for 取消的是它等待的 awaitable

如果传入的是本地 `Task`：

```python
await asyncio.wait_for(task, timeout=1)
```

超时会取消这个 `task`。

如果传入的是被 `shield()` 包住的 awaitable：

```python
await asyncio.wait_for(asyncio.shield(task), timeout=1)
```

超时只取消外层等待，不取消里面的 `task`。

## 6. shield

### 6.1 shield 的作用

`asyncio.shield(aw)` 的作用是阻断外层取消向 `aw` 传播。

它区分两件事：

```text
当前等待者不想等了
不等于
底层任务应该停掉
```

### 6.2 典型场景：等待超时，但任务继续跑

```python
import asyncio

async def warmup_cache():
    await asyncio.sleep(10)
    print("cache warmed")

async def main():
    task = asyncio.create_task(warmup_cache())
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=1)
    except asyncio.TimeoutError:
        print("this request stops waiting, warmup continues")

    await task

asyncio.run(main())
```

不用 `shield` 时，`wait_for` 超时会取消 `task`。

用了 `shield` 后，超时只取消这一次等待，`task` 自己继续运行。

### 6.3 典型场景：共享任务

如果多个调用者都在等待同一个 task，某个调用者超时或取消，不应该顺手取消共享 task。

```python
connect_task = asyncio.create_task(connect_db())

async def handler():
    try:
        await asyncio.wait_for(asyncio.shield(connect_task), timeout=0.5)
    except asyncio.TimeoutError:
        return "try later"
```

这里某个 `handler()` 超时，不应该取消全局连接任务。

### 6.4 shield 不是绝对保护

`shield()` 只阻断当前这条 await 链路上的取消传播。

如果其他地方直接取消底层 task：

```python
task.cancel()
```

它仍然会被取消。

如果 event loop 关闭或进程退出，也无法保证继续执行。

## 7. 子 task 和嵌套 task

### 7.1 子 create_task 的问题

下面这个例子中，`child()` 不会自动跟随 `work()` 取消：

```python
import asyncio

async def child():
    await asyncio.sleep(100)

async def work():
    t = asyncio.create_task(child())
    await asyncio.sleep(100)
```

如果取消 `work()`，取消会打到 `work()` 当前正在 await 的 `asyncio.sleep(100)`。

`t` 是另一个独立 task。它不会因为是在 `work()` 里创建的，就自动取消。

但如果 `work()` 当前等待的正好是 `t`，也就是写成 `await t`，取消就会沿当前 await 链路传给 `t`。这不是因为 `t` 是在 `work()` 里创建的，而是因为 `work()` 正在等待它。下一节单独说明这个情况。

### 7.2 当前 await 链路会传播取消

如果 `work()` 正在 await 子 task：

```python
async def work():
    t = asyncio.create_task(child())
    await t
```

取消 `work()` 时，取消会沿当前 await 链路传到 `t`。

这里 child 被取消的原因是：

```text
work 当前正在 await t
```

而不是：

```text
t 是在 work 里创建的
```

### 7.3 合理处理：显式管理子 task

如果创建了子 task，但当前还要 await 别的东西，需要在取消路径里显式取消子 task。

```python
async def work():
    tasks = [asyncio.create_task(child()) for _ in range(10)]
    try:
        await asyncio.sleep(100)
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
```

关键点：

```text
task.cancel():
  发取消请求。

gather(..., return_exceptions=True):
  等所有子 task 完成取消和清理。

raise:
  继续传播 work 自己的取消。
```

这里不能只调用 `task.cancel()` 后就直接退出，因为 `cancel()` 只是给子 task 发出取消请求，不代表子 task 已经停下来了。

子 task 收到取消后，可能还要：

```text
执行 finally；
执行 except CancelledError 里的 cleanup；
关闭连接、释放锁、flush 缓冲；
把 CancelledError 或其他异常设置到 task 结果里。
```

如果父 coroutine 直接退出，不等待这些子 task 收尾，这些子 task 可能继续在后台跑一段时间。更糟的是，如果子 task 在取消清理过程中抛异常，而没人 await 它，event loop 可能打印类似 “Task exception was never retrieved” 的日志。

所以：

```python
for task in tasks:
    task.cancel()
await asyncio.gather(*tasks, return_exceptions=True)
```

这两步要一起看：

```text
第一步 cancel:
  请求所有子 task 停下。

第二步 gather:
  等它们真的停下，并把取消异常/清理异常都收集回来。
```

`return_exceptions=True` 的目的不是忽略问题，而是避免第一个异常提前打断等待，保证所有子 task 都有机会完成收尾。

### 7.4 嵌套 3、4 层 task 怎么办

不要试图让最外层手动知道所有深层 task。

更合理的规则是：

```text
每一层管理自己创建的 task。
```

但要区分两种情况。

第一种：当前正在等待自己创建的子 tasks。

这种情况下，如果外层取消打到当前 `await asyncio.gather(*tasks)`，`gather()` 会把取消传播给它管理的未完成 tasks。所以不需要在 `except CancelledError` 里重复对每个 task 调 `cancel()`。

最简单写法：

```python
async def level3():
    await asyncio.sleep(100)

async def level2():
    tasks = [asyncio.create_task(level3()) for _ in range(3)]
    await asyncio.gather(*tasks)

async def level1():
    tasks = [asyncio.create_task(level2()) for _ in range(3)]
    await asyncio.gather(*tasks)
```

如果你希望在取消路径里显式等待子 task 收尾并收集异常，可以写：

```python
async def level2():
    tasks = [asyncio.create_task(level3()) for _ in range(3)]
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
```

这里不需要手动 `task.cancel()`，因为第一次 `await asyncio.gather(*tasks)` 被取消时，已经向这些 tasks 传播了取消。第二次 `gather(..., return_exceptions=True)` 的作用是等待它们完成取消清理。

第二种：创建了子 tasks，但当前等待的不是它们。

这种情况下，外层取消只会打到当前 await 的对象，不会自动取消你之前创建的那些子 tasks。此时需要手动 cancel。

```python
async def level2():
    tasks = [asyncio.create_task(level3()) for _ in range(3)]
    try:
        await asyncio.sleep(100)  # 当前等待的不是 tasks
    except asyncio.CancelledError:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise
```

所以嵌套多层时，规则不是“永远手动 cancel”，而是：

```text
当前 await 的正是 gather(*tasks):
  gather 负责把外层取消传播给 tasks；
  如需收尾，可再 gather(..., return_exceptions=True)。

当前 await 的不是这些 tasks:
  需要手动 task.cancel()；
  然后 gather(..., return_exceptions=True) 等收尾。
```

## 8. gather

### 8.1 gather 的作用

`asyncio.gather()` 用来等待一组 awaitable。

```python
results = await asyncio.gather(task1, task2, task3)
```

它会按传入顺序返回结果。

### 8.2 外层取消 gather

如果等待 `gather()` 的外层 task 被取消，`gather()` 会把取消传播给它管理的未完成 awaitable。

```python
async def work():
    tasks = [asyncio.create_task(child(i)) for i in range(10)]
    await asyncio.gather(*tasks)
```

取消 `work()` 时，`gather()` 管理的未完成 child task 通常会一起取消。

### 8.3 子 task 普通异常和取消不同

如果某个 child task 抛普通异常，`gather()` 默认会把第一个异常传播给等待者。如果其中一个 task 抛了普通异常，比如 RuntimeError：

gather 会把这个异常抛给等待 gather 的调用方；但不会自动取消其他还没完成的 tasks。其他 task 可能继续运行。

因此如果业务要求“一子失败，全体取消”，需要显式设计，或者使用 `TaskGroup`。

### 8.4 return_exceptions=True

取消收尾时常用：

```python
await asyncio.gather(*tasks, return_exceptions=True)
```

目的是等待所有 task 完成，不让某个 task 的取消异常或其他异常提前打断收尾流程。

这通常用于：

```text
已经决定 shutdown / cancel；
现在只想等所有子 task 结束；
异常作为结果收集，不再中断等待。
```

## 9. TaskGroup

### 9.1 TaskGroup 是结构化并发

Python 3.11+ 提供 `asyncio.TaskGroup`。

它把一组子 task 的生命周期绑定到一个 `async with` 作用域。

```python
async def work():
    async with asyncio.TaskGroup() as tg:
        for i in range(10):
            tg.create_task(child(i))
```

离开 `async with` 之前，`TaskGroup` 会等待组内 task 完成。

### 9.2 父 task 取消时，TaskGroup 会取消组内 task

```python
async def work():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(child())
        await asyncio.sleep(100)
```

如果外部取消 `work()`，`work` 会退出 `TaskGroup` 作用域。`TaskGroup` 会取消组内未完成 task，并等待它们收尾。

### 9.3 嵌套 TaskGroup

嵌套 `TaskGroup` 不需要是同一个实例。

例子：

```python
async def level3():
    await asyncio.sleep(100)

async def level2():
    async with asyncio.TaskGroup() as tg:
        for _ in range(3):
            tg.create_task(level3())

async def level1():
    async with asyncio.TaskGroup() as tg:
        for _ in range(3):
            tg.create_task(level2())

async def work():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(level1())
        await asyncio.sleep(100)
```

取消传播链路是：

```text
取消 work task
  -> work 的 TaskGroup 取消 level1 task
  -> level1 task 收到 CancelledError
  -> level1 退出自己的 TaskGroup
  -> level1 的 TaskGroup 取消 level2 tasks
  -> level2 同理取消 level3 tasks
```

不是因为所有层级用了同一个 `TaskGroup` 实例。

而是因为每一层被取消后，都会退出自己的 `TaskGroup` 作用域，并取消自己管理的子 task。

### 9.4 TaskGroup 的边界

`TaskGroup` 只管理通过这个 `TaskGroup` 创建的 task。

如果在 `TaskGroup` 作用域里裸写：

```python
asyncio.create_task(other_work())
```

这个 task 不归当前 `TaskGroup` 管。

正确写法是：

```python
tg.create_task(other_work())
```

### 9.5 TaskGroup 和异常

如果组内某个 task 抛出非取消异常，`TaskGroup` 会取消其他未完成 task，并在退出时抛出异常组。

这比 `gather()` 更接近“一子失败，全体收敛”的结构化并发语义。

## 10. CPU 同步段和取消延迟

### 10.1 问题

async 函数里如果执行长时间同步 CPU 代码，中间没有 `await`，取消不会及时生效。

```python
async def work():
    total = 0
    for i in range(10**12):
        total += i
    return total
```

原因是 event loop 被占住了。`CancelledError` 没有机会注入。

### 10.2 取消链会被卡住

如果取消链执行到某个 task，而这个 task 正在跑 CPU 同步段：

```text
取消请求已经到达 task；
但 task 没有到 await 点；
CancelledError 暂时不能注入；
这个 task 内部的 TaskGroup 也暂时不会退出；
它管理的子 task 也可能暂时不会被取消。
```

如果 CPU 段最终结束，并且后续到达 `await` 或返回 event loop，取消会继续被处理。

如果 CPU 段是死循环，取消可能永远无法生效。

### 10.3 推荐做法：分片让出 event loop

```python
async def work():
    total = 0
    for _ in range(100000):
        total += cpu_chunk()
        await asyncio.sleep(0)
    return total
```

`await asyncio.sleep(0)` 是一个显式调度点，让 event loop 有机会处理其他 task 和取消请求。

### 10.4 推荐做法：放到 executor

CPU 或阻塞同步函数也可以放到 executor：

```python
async def work():
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, blocking_func)
```

但注意：取消 outer coroutine 只会取消等待，不会安全杀掉 executor 里的线程函数。

如果要让 executor 里的函数也能停，需要自己设计 stop flag：

```python
import threading

def blocking_func(stop):
    while not stop.is_set():
        do_one_chunk()

async def work():
    stop = threading.Event()
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(None, blocking_func, stop)
    try:
        return await fut
    except asyncio.CancelledError:
        stop.set()
        raise
```

## 11. 实用规则

### 11.1 不要裸 create_task 后不管

除非明确就是后台任务，并且有 supervisor 管理，否则不要这样写：

```python
asyncio.create_task(child())
```

更好的方式是：

```python
task = asyncio.create_task(child())
await task
```

或者：

```python
async with asyncio.TaskGroup() as tg:
    tg.create_task(child())
```

### 11.2 捕获 CancelledError 后通常要 raise

```python
except asyncio.CancelledError:
    await cleanup()
    raise
```

不要无意中把取消变成正常返回。

### 11.3 子 task 要有 owner

每个 task 都应该能回答：

```text
谁负责 await？
谁负责 cancel？
谁负责处理异常？
谁负责 shutdown 时收尾？
```

### 11.4 优先使用 TaskGroup

Python 3.11+ 中，多子任务并发优先考虑 `TaskGroup`。

它比裸 `create_task()` 更容易维持清晰的生命周期。

### 11.5 shield 只用于明确不想传播取消的场景

`shield()` 不是默认选择。

只有当你明确需要：

```text
当前等待者可以取消；
底层任务不应该被当前等待者取消。
```

才使用 `shield()`。

### 11.6 CPU 同步段要主动让出 event loop

长 CPU 循环要么拆 chunk + `await asyncio.sleep(0)`，要么放到 executor，并配合 stop flag。

## 12. 最终总结

`asyncio` 取消机制可以压缩成几句话：

```text
Task.cancel() 是取消请求，不是强杀。

CancelledError 在 await / 调度点注入。

不捕获 CancelledError 可以正常取消。

捕获后不 raise 可能吞掉取消。

create_task 不自动建立父子生命周期。

gather / TaskGroup / 显式 cancel 用来管理子 task。

shield 用来阻断外层取消向内传播。

长同步 CPU 段会延迟甚至阻塞取消。
```

最重要的工程原则是：

```text
协程可以被取消；
取消路径也要被设计。
```

