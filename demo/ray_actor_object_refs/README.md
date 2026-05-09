# Ray Actor ObjectRef 生命周期 demo

这个 demo 用来观察 `ray.put` 对象在 actor 之间传递、`ray.get`、actor 持有 `ObjectRef` 时的引用计数变化。

运行：

```bash
python demo/ray_actor_object_refs/object_lifetime_demo.py
```

如果你想更仔细地看每个阶段，可以在另一个终端运行：

```bash
ray memory --sort-by=OBJECT_SIZE --group-by=STACK_TRACE
```

观察重点：

1. `actor.method.remote(ref)` 这种顶层 `ObjectRef` 参数会被 Ray 自动解引用，actor 方法里收到的是对象值，不是 `ObjectRef`。
2. `actor.method.remote([ref])` 把 `ObjectRef` 包在 list 里传，actor 方法里可以拿到并保存这个 `ObjectRef`。
3. actor 把 `ObjectRef` 存到 `self` 后，即使 driver 删除自己的 `ref`，对象仍然不会释放。
4. `ray.get(ref)` 得到的 NumPy 数组可能直接指向 object store shared memory；即使删掉 `ObjectRef`，只要这个数组还活着，`ray memory` 里仍可能看到 `PINNED_IN_MEMORY`。

