# Ray Actor 与 ObjectRef 生命周期讨论记录

## 1. 背景

本记录整理一次关于 Ray actor 中对象管理、`ray.put`、`ray.get`、ObjectRef 引用计数、borrower 传播、普通对象和 NumPy 对象生命周期差异的讨论。

主要参考本地文档和源码：

- `doc/source/ray-core/scheduling/memory-management.rst`
- `doc/source/ray-core/objects.rst`
- `doc/source/ray-core/objects/serialization.rst`
- `doc/source/ray-core/internals/object-spilling.rst`
- `src/ray/core_worker/reference_counter.h`
- `src/ray/core_worker/reference_counter.cc`
- `src/ray/core_worker/core_worker.cc`
- `python/ray/_private/serialization.py`
- `python/ray/_raylet.pyx`

## 2. 先理解 owner / borrower 模型

讨论 `ObjectRef` 生命周期时，最重要的两个角色是 owner 和 borrower。

本地文档 `doc/source/ray-core/fault_tolerance/objects.rst` 里的定义是：Ray object 有两部分，数据值存放在 object store，元数据存放在对象的 owner 处。owner 是创建原始 `ObjectRef` 的 worker 进程，例如调用 `ray.put()` 或 `.remote()` 的进程。

### 2.1 Owner 是谁

owner 不是“当前持有对象数据的节点”，而是“负责管理这个 ObjectRef 元数据和引用计数的进程”。

常见例子：

```python
ref = ray.put(obj)
```

这里调用 `ray.put` 的 driver / worker 是 owner。

```python
ref = task.remote()
```

这里提交 task 的 caller 通常是返回值 `ref` 的 owner；真正执行 task 的 worker 可能只是创建了对象的 value，不一定是 owner。

所以要区分：

| 概念 | 含义 |
| --- | --- |
| owner | 管理 `ObjectRef` 元数据、引用计数、对象位置、失败恢复信息的进程 |
| object store location | 对象数据实际存放在哪个节点的 object store |
| value creator | 实际执行任务并产生对象值的 worker |

owner 活着时，Ray 才能继续判断这个对象是否还有引用、是否能释放、是否能重建。owner 死亡时，其他进程后续访问该对象可能遇到 `OwnerDiedError`。

### 2.2 Borrower 是谁

borrower 是“拿到了某个不属于自己的 `ObjectRef`，并且可能继续使用或传播它的 worker / actor / driver”。

典型场景：

```python
ref = ray.put(big_obj)          # driver 是 owner
actor.store.remote([ref])       # actor 收到嵌套 ObjectRef，成为 borrower
```

这里 `[ref]` 是嵌套传递，actor 方法里收到的是 `ObjectRef` 本身。如果 actor 保存它：

```python
self.ref = refs[0]
```

owner 就不能只看 driver 本地是否还有 `ref`。即使 driver `del ref`，对象也不能释放，因为 actor 这个 borrower 仍然可能 `ray.get(self.ref)`。

### 2.3 Borrower 传播为什么重要

borrower 还可以把借来的 `ObjectRef` 再传给第三方：

```python
# actor A 已经保存了 self.ref
actor_b.store.remote([self.ref])
```

这时 actor B 也成为 borrower。owner 必须知道这条传播链，否则 actor A 释放后，owner 可能误以为对象已经没人用了。

源码 `src/ray/core_worker/reference_counter.h` 中 `BorrowInfo` 维护了 borrower 列表；`Reference::OutOfScope()` 也明确把 `has_borrowers`、`stored_in_objects`、nested borrowed refs 等条件纳入判断。也就是说，一个对象是否能释放，不只取决于 owner 本地 `local_ref_count`，还取决于是否还有 borrower 链没有结束。

### 2.4 一个简化心智模型

可以把生命周期理解为：

```text
owner 创建 ObjectRef
  |
  | 本地持有 ref / task 参数引用 / captured ref
  |
  | 把 ref 嵌套传给 actor 或 worker
  v
borrower 出现
  |
  | borrower 保存、ray.get、继续传递给其他 borrower
  v
owner 等所有本地引用和 borrower 链都结束
  |
  v
object store 对象才可能释放
```

如果只是顶层传 `ObjectRef`：

```python
actor.method.remote(ref)
```

actor 收到的是对象值，不是 `ObjectRef` 本身。对于普通对象，这通常不会让 actor 长期成为 borrower。真正容易形成 borrower 链的是嵌套传递、自定义对象字段里包含 `ObjectRef`、或者把 `ObjectRef` 存进另一个 object store 对象。

## 3. 先理解 ray memory 里的几种状态

讨论对象生命周期前，先区分 `ray memory` 中常见的几种引用类型。它们不是同一个层次的概念，但都会影响 object store 中对象能不能释放。

### 3.1 LOCAL_REFERENCE

表示某个 driver、worker 或 actor 进程里还存在 Python 层面的 `ObjectRef`。

常见来源：

```python
ref = ray.put(obj)
self.ref = ref
refs.append(ref)
```

只要 `ObjectRef` 还活着，Ray 就认为这个对象仍被使用，object store 中的数据不能释放。

### 3.2 USED_BY_PENDING_TASK

表示某个对象正在被尚未完成的 task 或 actor method 当作参数使用。

示例：

```python
ref = ray.put(obj)
future = actor.method.remote(ref)
```

在 actor method 还没有执行完之前，即使 driver 里 `del ref`，这个对象也不能释放。因为 pending task 还依赖它。

### 3.3 CAPTURED_IN_OBJECT

表示某个 `ObjectRef` 被序列化进了另一个 object store 对象里。

示例：

```python
inner = ray.put(obj)
outer = ray.put([inner])
del inner
```

此时 `inner` 虽然没有本地 Python 变量引用，但 `outer` 的内容里包含了 `inner` 的 `ObjectRef`。因此 `inner` 仍然被 `outer` 保活，`ray memory` 中会显示为 `CAPTURED_IN_OBJECT`。

### 3.4 PINNED_IN_MEMORY

表示对象值本身被某个 worker / driver 进程中的反序列化结果直接引用，导致 object store 中的数据不能释放。

最典型的是 NumPy zero-copy：

```python
ref = ray.put(np.zeros(...))
arr = ray.get(ref)
del ref
```

这里 `ref` 这个 `ObjectRef` 可能已经没了，但 `arr` 仍然直接指向 object store shared memory，所以对象仍会被 pin。只有 `del arr` 后，object store 中的数据才可能释放。

注意：普通 Python 对象通常不是这个路径。普通对象 `ray.get(ref)` 后一般得到 Python heap 副本，这个副本活着不会继续 pin 原始 object store 对象。

### 3.5 ACTOR_HANDLE

表示 actor handle 本身也是一种 Ray 引用。actor handle 会影响 actor 生命周期，但和普通 object store 数据的释放不是同一类问题。本记录主要关注 `ObjectRef` 指向的数据对象。

### 3.6 这几个状态的关系

可以粗略理解为：

| 状态 | 保活原因 |
| --- | --- |
| `LOCAL_REFERENCE` | 某进程还持有 `ObjectRef` |
| `USED_BY_PENDING_TASK` | 某个未完成任务/actor method 需要这个对象 |
| `CAPTURED_IN_OBJECT` | `ObjectRef` 被包进另一个 object store 对象 |
| `PINNED_IN_MEMORY` | 反序列化结果直接指向 shared memory |
| `ACTOR_HANDLE` | actor handle 还活着 |

对象只有在所有相关引用都消失后，owner 才能判断它 out of scope，并通知 raylet / object store 释放或 unpin。

## 4. 核心结论

Ray 释放 object store 中对象时，关心的不是“某个 Python 值还在不在”，而是：

1. 是否还有 `ObjectRef` 链路存在。
2. 是否还有 pending task / actor method 参数依赖这个对象。
3. 是否有 actor、driver、worker 本地保存了 `ObjectRef`。
4. 是否有 `ObjectRef` 被序列化进其他对象，形成 nested / captured ref。
5. 是否有 `ray.get` 返回值直接指向 object store shared memory，例如 NumPy zero-copy。

可以记成一句话：

> Ray 释放 object store 对象时关心的是“还有没有 ObjectRef 链或 shared-memory view”，不是普通 Python 值副本。

ray.get(ref) 本身不是让 borrower 链变长的关键；持有 ObjectRef 才是。ray.get 返回的普通值通常不影响原始 ref；如果返回的是 NumPy array，那还可能通过 PINNED_IN_MEMORY 保活 object
store 数据。

## 5. 典型生命周期场景

这一节按具体使用场景展开：先看普通对象和 NumPy 的差异，再看 `ObjectRef` 作为参数传递、嵌套引用、captured ref、borrower 传播，以及多 actor 并发时的内存份数。

### 5.1 普通大对象

示例：

```python
ref = ray.put(big_custom_obj)
obj = ray.get(ref)
del ref
```

对于普通自定义对象，`ray.get(ref)` 通常会反序列化出一份 Python heap 副本。这个副本 `obj` 活着，不会继续保活原始 object store 对象。

因此，只要没有其他 Ray 层面的引用，owner 发现引用计数归零后，object store 中那份对象可以自动释放。

但 `obj` 本身仍然占用当前 Python 进程 heap，需要 Python GC 负责释放：

```python
del obj
```

### 5.2 NumPy 对象

示例：

```python
ref = ray.put(np.zeros(...))
arr = ray.get(ref)
del ref
```

NumPy 是特殊情况。Ray 对 NumPy 使用 pickle5 out-of-band buffer 和 zero-copy 反序列化。`ray.get(ref)` 返回的 `arr` 可能直接指向 object store shared memory。

所以即使 `del ref`，只要 `arr` 还活着，object store 中的对象仍可能显示为：

```text
PINNED_IN_MEMORY
```

需要：

```python
del arr
```

之后 object store 中那份对象才可能释放。

### 5.3 顶层 ObjectRef 与嵌套 ObjectRef

Ray 对 `ObjectRef` 参数有两种不同语义。

#### 5.3.1 顶层传递：by-value

```python
actor.method.remote(ref)
```

如果 `ref` 是顶层参数，Ray 会自动解引用。actor 方法里收到的是对象值，不是 `ObjectRef`。

对于普通对象，这通常意味着 actor 收到的是 Python heap 副本。actor 后续处理这个普通值，不会让原始 `ref` 的 object store 对象继续保活。

#### 5.3.2 嵌套传递：by-reference

```python
actor.method.remote([ref])
```

如果 `ref` 被包在 list、dict、自定义对象字段等结构中，Ray 不会自动解引用。actor 收到的是 `ObjectRef`。

actor 如果保存：

```python
self.ref = wrapped[0]
```

那么 actor 就成为 borrower。owner 不能释放原始对象，直到 actor 释放这个 `ObjectRef`。

### 5.4 自定义对象内部包含 ObjectRef

示例：

```python
class Wrapper:
    def __init__(self, ref):
        self.ref = ref

inner = ray.put(big_obj)
wrapper = Wrapper(inner)
actor.method.remote(wrapper)
```

这里 `wrapper` 自身是普通对象，会按值序列化发送给 actor。

但 `wrapper.ref` 是 nested `ObjectRef`，Ray 会在序列化时识别并记录到 `contained_object_refs`，保证 inner 对象继续被引用计数保活。

结果是：

- `wrapper` 外层对象可能有多份 Python heap 副本。
- `inner` 指向的 object store 对象通常不是复制 n 份，而是通过同一个 `ObjectRef` 被多个 borrower 共享。
- actor 只有在执行 `ray.get(wrapper.ref)` 时，才会拉取 inner 的真实数据。

### 5.5 Captured In Object

示例：

```python
inner = ray.put(big_obj)
outer = ray.put([inner])
del inner
```

虽然 `inner` 这个 Python 变量被删除了，但 `outer` 的对象值里包含了 `inner` 的 `ObjectRef`。Ray 会把 inner 标记为：

```text
CAPTURED_IN_OBJECT
```

只要 `outer` 还活着，inner 指向的 object store 数据就不能释放。

### 5.6 Borrower 传播

borrower 传播指的是：一个已经借到 `ObjectRef` 的 worker / actor，又把这个 `ObjectRef` 传给了第三方。

示例：

```python
# driver 是 owner
ref = ray.put(big_obj)

# actor A 成为 borrower
actor_a.store.remote([ref])

# actor A 内部又把 ref 传给 actor B
actor_b.store.remote([self.ref])
```

这时 owner 不只要等 actor A 释放，还要知道 actor B 也在借用。否则 actor A 释放后，owner 可能误删对象，导致 actor B 后续 `ray.get(ref)` 失败。

Ray 的实现会在任务结束时，把“我还在借哪些 ObjectRef，以及我又传给了哪些 worker”的信息返回给 caller / owner，然后 owner 把这些间接 borrower 合并进引用表。

### 5.7 ray.get 与引用计数

`ray.get(ref)` 本身不会给原始 `ObjectRef` 增加一份 `local_ref_count`。

但 `ray.get(ref)` 的返回值可能影响释放：

- 普通对象：返回 Python heap 副本，通常不保活 object store 对象。
- NumPy：返回值可能指向 shared memory，会 `PINNED_IN_MEMORY`。
- 包含 NumPy 的普通对象：外层普通对象不特殊，但里面的 NumPy buffer 仍可能 pin shared memory。

### 5.8 多 actor 处理普通对象时的内存份数

如果直接把同一个普通大对象按值发给多个 actor：

```python
for actor in actors:
    actor.method.remote(big_custom_obj)
```

Ray 可能对每次调用隐式 `ray.put`，因为大对象不适合 inline 到 task RPC 中。Ray 不做去重，因此可能产生多份 object store 输入对象。

更典型的危险写法是并发提交：

```python
refs = [actor.method.remote(big_custom_obj) for actor in actors]
ray.get(refs)
```

这种写法不推荐用于同一个大对象。因为每次 `actor.method.remote(big_custom_obj)` 都是在按值传参，Ray 可能为每一次调用各自生成一份临时 object store 输入对象。

如果有 `n` 个 actor 并发执行，就可能同时存在：

- driver 里的 `big_custom_obj` Python heap 副本。
- 最多 `n` 份隐式 put 出来的 object store 输入副本。
- 每个 actor 执行时各自反序列化出来的 Python heap 副本。
- 如果 actor 还返回这个对象，还会有额外的返回对象副本。

这些临时输入对象会在对应 actor method 完成、任务参数引用计数归零后自动释放。但在并发窗口内，它们可能同时占用大量 object store 内存，因此容易造成 object store 压力、对象 spilling，甚至 OOM。

更好的方式是：

```python
ref = ray.put(big_custom_obj)
for actor in actors:
    actor.method.remote(ref)
```

或者并发提交时写成：

```python
ref = ray.put(big_custom_obj)
refs = [actor.method.remote(ref) for actor in actors]
ray.get(refs)
```

这里先显式 `ray.put` 一次，再复用同一个 `ObjectRef`。顶层 `ref` 会被解引用，actor 收到值；输入 object store 通常只有一份 primary copy。但每个 actor 执行时仍会有自己的 Python heap 反序列化副本。

如果 actor 需要保存或转发引用，应该显式嵌套传递：

```python
actor.method.remote([ref])
```

这会形成 borrower，而不是值副本。

## 6. 实用判断规则

可以按下面规则判断是否会保活原始 object store 对象：

| 情况 | 是否保活原始 object store 对象 |
| --- | --- |
| 普通 Python 值副本 | 通常不保活 |
| 顶层传 `ObjectRef` 给 actor | actor 收到值；值本身通常不保活 |
| 嵌套传 `ObjectRef`，如 `[ref]` | 保活 |
| 自定义对象字段里有 `ObjectRef` | 保活 |
| `ray.put([ref])` 形成 outer object | inner 被 `CAPTURED_IN_OBJECT` 保活 |
| `ray.get` 返回 NumPy array | 可能通过 `PINNED_IN_MEMORY` 保活 |
| borrower 把 `ObjectRef` 再传给别人 | 保活，直到间接 borrower 也释放 |

## 7. 调试方式

使用：

```bash
ray memory --sort-by=OBJECT_SIZE --group-by=STACK_TRACE
```

重点观察：

- `LOCAL_REFERENCE`
- `USED_BY_PENDING_TASK`
- `CAPTURED_IN_OBJECT`
- `PINNED_IN_MEMORY`

这些字段能帮助判断对象为什么还没有释放。
