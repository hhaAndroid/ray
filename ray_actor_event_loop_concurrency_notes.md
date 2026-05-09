# Ray Actor 事件循环、线程池与并发模型笔记

## 1. 背景

这份笔记记录 Ray actor 内部调用并发逻辑，重点包括：

1. 普通同步 actor。
2. 带 `async def` 方法的 AsyncActor。
3. `max_concurrency > 1` 的 ThreadedActor。
4. `max_concurrency`、event loop、线程池、concurrency group 的关系。
5. AsyncActor 中容易误解的状态竞态。

本文基于 Ray 当前源码和文档整理，主要参考：

1. `doc/source/ray-core/actors/async_api.rst`
2. `doc/source/ray-core/actors/concurrency_group_api.rst`
3. `python/ray/actor.py`
4. `python/ray/_raylet.pyx`
5. `python/ray/_private/async_compat.py`
6. `src/ray/core_worker/task_execution/*`

## 2. Ray Actor 的三种执行模型

### 2.1 普通同步 Actor

示例：

```python
@ray.remote
class A:
    def f(self):
        return 1
```

普通同步 actor 的特点：

1. actor 类中没有 `async def`。
2. 默认 `max_concurrency=1`。
3. 同一个 actor 实例一次只执行一个 actor method。
4. 默认按提交顺序执行 actor tasks。

内部上，它主要走 ordered actor task queue。

如果没有额外 concurrency group，并且默认并发是 1，Ray 不会为默认 group 创建额外线程池，任务可以直接在主执行路径串行执行。

### 2.2 ThreadedActor

示例：

```python
@ray.remote
class A:
    def f(self):
        return 1

a = A.options(max_concurrency=4).remote()
```

ThreadedActor 的条件：

1. actor 类里没有任何 `async def`。
2. `max_concurrency > 1`。

特点：

1. Ray 会给该 actor 创建线程池。
2. 线程池大小受 `max_concurrency` 限制。
3. 多个 actor method 可以同时在不同 OS 线程里执行。
4. 执行顺序不再保证。
5. 共享 `self.xxx` 状态需要自己加锁。

注意：ThreadedActor 不能绕过 Python GIL。对纯 Python CPU 逻辑，不一定有真正并行收益。它更适合：

1. I/O 阻塞逻辑。
2. 会释放 GIL 的 NumPy / PyTorch / C++ 扩展。
3. 阻塞系统调用。

### 2.3 AsyncActor

示例：

```python
@ray.remote
class A:
    async def f(self):
        return 1
```

AsyncActor 的条件：

```text
actor 类中只要存在任意 async def 方法，Ray 就会把整个 actor 识别为 AsyncActor。
```

特点：

1. 默认 `max_concurrency=1000`。
2. `max_concurrency` 表示最多多少 coroutine / fiber in-flight，不是 OS 线程数。
3. Ray 内部会为 AsyncActor 创建 Python event loop。
4. actor method 会被提交到 Ray 创建的 event loop 中执行。
5. 并发依赖 `await` 让出控制权。

Ray 文档强调：

```text
AsyncActor 里不要直接调用阻塞式 ray.get / ray.wait。
```

因为它们会阻塞 event loop。

## 3. Actor 类型如何判断

源码中，`python/ray/actor.py` 会通过 `has_async_methods()` 判断 actor 类里是否有 async 方法。

`python/ray/_private/async_compat.py` 中：

```python
def is_async_func(func) -> bool:
    return inspect.iscoroutinefunction(func) or inspect.isasyncgenfunction(func)

def has_async_methods(cls: object) -> bool:
    return len(inspect.getmembers(cls, predicate=is_async_func)) > 0
```

因此：

```python
@ray.remote
class A:
    async def ping(self):
        return "ok"

    def heavy_sync(self):
        time.sleep(10)
```

这个 actor 仍然是 AsyncActor。

`heavy_sync()` 不会自动跑进线程池，而是会被包装成 async wrapper 后在 AsyncActor 的 event loop 里执行。同步阻塞逻辑会卡住 event loop。

## 4. max_concurrency 的含义

### 4.1 普通同步 Actor

默认：

```text
max_concurrency = 1
```

含义：

```text
同一个 actor 同时最多执行一个 actor method。
```

### 4.2 ThreadedActor

示例：

```python
a = A.options(max_concurrency=4).remote()
```

含义：

```text
actor 内部线程池最多 4 个线程并发执行 actor methods。
```

C++ 侧使用 `BoundedExecutor`，其内部会创建 `max_concurrency` 个 `std::thread`。

### 4.3 AsyncActor

示例：

```python
a = A.options(max_concurrency=128).remote()
```

含义：

```text
最多允许 128 个 actor method coroutine / fiber 同时处于执行或等待状态。
```

它不是线程数。

AsyncActor 默认值是：

```text
DEFAULT_MAX_CONCURRENCY_ASYNC = 1000
```

这个默认值很大，实际业务中经常需要调小，否则可能造成：

1. 大量请求同时进入 actor。
2. 大量对象反序列化。
3. 大量下游 RPC。
4. 大量 `run_in_executor()` 排队。
5. 内存和对象 store 压力上升。

## 5. AsyncActor 的 Event Loop

### 5.1 Ray 会为 AsyncActor 创建 event loop

AsyncActor 创建时，Ray 会初始化 event loop。

内部逻辑大致是：

```text
actor creation task
  -> 判断 current_actor_is_asyncio
  -> initialize_eventloops_for_actor_concurrency_group
  -> 创建 default event loop
  -> 创建线程运行 eventloop.run_forever()
```

也就是说，AsyncActor 的 actor methods 默认不是跑在用户显式创建的 loop 上，而是跑在 Ray 为该 actor 创建的 loop 上。

### 5.2 actor method 如何进入 event loop

AsyncActor method 调用路径大致是：

```text
actor.method.remote()
  -> SubmitActorTask
  -> actor task execution queue
  -> FiberState 限流
  -> run_async_func_or_coro_in_event_loop
  -> asyncio.run_coroutine_threadsafe(coro, eventloop)
```

Ray 用 `asyncio.run_coroutine_threadsafe()` 把 coroutine 提交到对应 event loop。

### 5.3 用户自己创建 event loop 会怎样

如果在 AsyncActor 内写：

```python
async def f(self):
    loop = asyncio.new_event_loop()
```

这个 `loop` 和 Ray 创建的 event loop 是两个不同对象。

但它不会自动运行。

如果要让它运行，必须自己开线程：

```python
self.loop = asyncio.new_event_loop()
self.thread = threading.Thread(target=self.loop.run_forever)
self.thread.start()
```

此时 actor 内会有两套调度系统：

```text
Ray actor event loop
用户自建 background event loop
```

两者之间通信要用：

1. `asyncio.run_coroutine_threadsafe()`
2. `loop.call_soon_threadsafe()`
3. thread-safe queue

不要直接跨 loop 操作 Future / Task / Lock 等对象。

### 5.4 不要在 Ray event loop 线程里 run_until_complete

错误示例：

```python
async def f(self):
    loop = asyncio.new_event_loop()
    loop.run_until_complete(some_coro())
```

问题：

1. 它会同步阻塞当前 Ray event loop 线程，直到 `some_coro()` 完成。
2. 当前 actor 的其他 async methods 很难继续调度。
3. 很多情况下 Python 会直接报错，例如当前线程已经有 event loop 正在运行。

推荐：

```python
async def f(self):
    return await some_coro()
```

如果是阻塞同步逻辑：

```python
async def f(self):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, blocking_func)
```

## 6. run_in_executor 的并发限制

`loop.run_in_executor(None, func)` 使用当前 event loop 的默认 executor。

Python 源码逻辑：

```python
if executor is None:
    executor = self._default_executor
    if executor is None:
        executor = concurrent.futures.ThreadPoolExecutor(
            thread_name_prefix="asyncio"
        )
        self._default_executor = executor
```

`ThreadPoolExecutor(max_workers=None)` 默认线程数在当前 Python 3.13 环境中是：

```python
min(32, (os.process_cpu_count() or 1) + 4)
```

因此：

```python
await loop.run_in_executor(None, blocking_func)
```

含义是：

```text
提交到当前 event loop 的默认 ThreadPoolExecutor；
最多默认 max_workers 个线程同时执行；
超出的任务进入 executor 内部队列等待。
```

在 AsyncActor 中要叠加两层并发限制：

```text
AsyncActor max_concurrency
  控制同时进入 actor 的 coroutine/fiber 数量

event loop default executor max_workers
  控制 run_in_executor(None, ...) 中 blocking_func 的线程并发数
```

示例：

```python
@ray.remote
class A:
    async def f(self):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, blocking_func)
```

如果同时提交 1000 个 `f.remote()`：

```text
Ray 可能允许很多 coroutine 进入；
但 blocking_func 同时跑的数量受 Python 默认线程池限制；
剩余任务在 ThreadPoolExecutor 队列里等待。
```

推荐显式控制：

```python
@ray.remote
class A:
    def __init__(self):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=8)
        self.sem = asyncio.Semaphore(8)

    async def f(self):
        loop = asyncio.get_running_loop()
        async with self.sem:
            return await loop.run_in_executor(self.executor, blocking_func)
```

这样避免默认 executor 队列无限堆积。

## 7. Concurrency Group

### 7.1 基本概念

Ray 支持按方法划分 concurrency group。

示例：

```python
@ray.remote(concurrency_groups={"io": 2, "compute": 4})
class A:
    @ray.method(concurrency_group="io")
    async def read(self):
        ...

    @ray.method(concurrency_group="compute")
    async def compute(self):
        ...

    async def default_method(self):
        ...
```

含义：

```text
read() 进入 io group，最多并发 2；
compute() 进入 compute group，最多并发 4；
default_method() 进入 default group。
```

默认 group 的并发由 actor 的 `max_concurrency` 控制。

### 7.2 AsyncActor 中的 concurrency group

在 AsyncActor 中，每个 concurrency group 会有自己的 event loop 和线程。

大致结构：

```text
default group -> event loop A -> thread A
io group      -> event loop B -> thread B
compute group -> event loop C -> thread C
```

因此，concurrency group 不只是逻辑限流，也会影响 event loop 隔离。

### 7.3 ThreadedActor 中的 concurrency group

在 ThreadedActor 中，每个 concurrency group 对应一个 `BoundedExecutor` 线程池。

大致结构：

```text
default group -> thread pool A
io group      -> thread pool B
compute group -> thread pool C
```

适合隔离：

1. 控制面方法。
2. 健康检查方法。
3. 慢 I/O 方法。
4. 大计算方法。

示例：

```python
@ray.remote(concurrency_groups={"control": 1, "work": 8})
class Worker:
    @ray.method(concurrency_group="control")
    def health(self):
        return "ok"

    @ray.method(concurrency_group="work")
    def process(self, x):
        return heavy_work(x)
```

这样 `process()` 堵住时，不至于完全饿死 `health()`。

## 8. AsyncActor 中的状态竞态

### 8.1 为什么单 event loop 也会有竞态

AsyncActor 只有一个 event loop，因此不会在同一瞬间执行两段 Python 字节码。

但是 coroutine 会在 `await` 点让出控制权。

如果一段逻辑：

```text
读取 self 状态
await
基于旧状态写回 self 状态
```

就可能出现竞态。

### 8.2 错误示例

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0

    async def incr(self):
        old = self.value
        await asyncio.sleep(0)
        self.value = old + 1
        return self.value
```

同时调用两次：

```python
c = Counter.remote()
refs = [c.incr.remote(), c.incr.remote()]
print(ray.get(refs))
```

可能执行顺序：

```text
task A: old = self.value  # 0
task A: await sleep(0)    # 让出 event loop

task B: old = self.value  # 0
task B: await sleep(0)    # 让出 event loop

task A: self.value = old + 1  # 写 1
task B: self.value = old + 1  # 也写 1
```

期望最终 `self.value == 2`，实际可能是 `1`。

### 8.3 正确写法

使用 `asyncio.Lock`：

```python
@ray.remote
class Counter:
    def __init__(self):
        self.value = 0
        self.lock = asyncio.Lock()

    async def incr(self):
        async with self.lock:
            old = self.value
            await asyncio.sleep(0)
            self.value = old + 1
            return self.value
```

更好的写法是缩小临界区，不在 lock 内做慢 await：

```python
async def incr(self):
    async with self.lock:
        self.value += 1
        return self.value
```

判断规则：

```text
如果读 self 状态和写 self 状态之间存在 await，就要考虑竞态。
```

## 9. Actor 使用注意事项

### 9.1 AsyncActor 中不要阻塞 event loop

避免：

```python
ray.get(...)
ray.wait(...)
time.sleep(...)
loop.run_until_complete(...)
大量纯 Python CPU 计算
```

推荐：

```python
await ref
await asyncio.sleep(...)
await loop.run_in_executor(...)
```

### 9.2 有一个 async def，整个 actor 就是 AsyncActor

混合同步方法和异步方法时要小心：

```python
@ray.remote
class A:
    async def ping(self):
        return "ok"

    def heavy_sync(self):
        time.sleep(10)
```

`heavy_sync()` 会阻塞 AsyncActor 的 event loop。

### 9.3 max_concurrency 不要盲目调大

`max_concurrency` 太大可能导致：

1. actor 内存膨胀。
2. object store 压力升高。
3. 反序列化并发过高。
4. 下游服务被打爆。
5. executor 队列堆积。
6. 状态竞态概率上升。

### 9.4 ThreadedActor 需要线程安全

ThreadedActor 多个方法会真正运行在不同 OS 线程里。

共享状态必须使用：

1. `threading.Lock`
2. `threading.RLock`
3. thread-safe queue
4. 或者避免共享可变状态

### 9.5 AsyncActor 需要 coroutine 安全

AsyncActor 中共享状态要考虑 `await` 交错。

常用工具：

1. `asyncio.Lock`
2. `asyncio.Semaphore`
3. `asyncio.Queue`
4. 缩小临界区

### 9.6 actor 是进程级隔离单元

一个 Ray actor 通常对应一个 worker process。

actor 不是轻量线程，actor 太多会带来：

1. 进程开销。
2. worker 内存开销。
3. RPC 连接开销。
4. 心跳和调度开销。

## 10. 总结

一句话总结：

```text
普通 actor 是串行队列；
ThreadedActor 是 actor 内线程池；
AsyncActor 是 event loop + coroutine，并发靠 await 让出执行权。
```

`max_concurrency` 在三种模型里的含义不同：

```text
普通 actor:
  同时执行的 actor method 数，默认 1。

ThreadedActor:
  actor 内线程池大小。

AsyncActor:
  同时 in-flight 的 coroutine / fiber 数，不是线程数。
```

最重要的实践原则：

```text
不要在 AsyncActor 中阻塞 event loop；
不要在 ThreadedActor 中忽略线程安全；
不要把 max_concurrency 当成无成本吞吐开关；
不要忘记 async 代码在 await 点也会产生状态竞态。
```
