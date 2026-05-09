import gc
import os
import shutil
import subprocess
import sys
import time

import numpy as np
import ray


OBJECT_MIB = 32


def pause(title: str) -> None:
    print("\n" + "=" * 88)
    print(title)
    print("=" * 88)
    input("按 Enter 继续...")


def show_memory(title: str) -> None:
    print("\n" + "-" * 88)
    print(f"ray memory: {title}")
    print("-" * 88)

    ray_cli = shutil.which("ray")
    if ray_cli is None:
        print("没有找到 ray CLI。可以在另一个终端手动运行：")
        print("  ray memory --sort-by=OBJECT_SIZE --group-by=STACK_TRACE")
        return

    cmd = [ray_cli, "memory", "--sort-by=OBJECT_SIZE", "--group-by=STACK_TRACE"]
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=20,
        )
    except subprocess.TimeoutExpired:
        print("ray memory 超时。可以手动运行：", " ".join(cmd))
        return

    print(completed.stdout)


@ray.remote
class RefHolder:
    def __init__(self):
        self.ref = None
        self.value = None

    def receive_top_level(self, obj):
        self.value = obj
        return {
            "received_type": type(obj).__name__,
            "is_object_ref": isinstance(obj, ray.ObjectRef),
            "shape": getattr(obj, "shape", None),
        }

    def store_wrapped_ref(self, wrapped):
        self.ref = wrapped[0]
        return {
            "stored_type": type(self.ref).__name__,
            "is_object_ref": isinstance(self.ref, ray.ObjectRef),
        }

    def get_stored_sum(self):
        return int(ray.get(self.ref).sum())

    def clear_stored_ref(self):
        self.ref = None
        gc.collect()
        return "cleared"

    def clear_value(self):
        self.value = None
        gc.collect()
        return "cleared"


def make_array(fill_value: int) -> np.ndarray:
    num_bytes = OBJECT_MIB * 1024 * 1024
    return np.full(num_bytes, fill_value, dtype=np.uint8)


def main() -> None:
    print(f"driver pid: {os.getpid()}")
    ray.init()

    actor = RefHolder.remote()

    pause("阶段 1：driver ray.put 一个 32MiB NumPy 对象")
    ref = ray.put(make_array(1))
    print("driver 持有 ref:", ref)
    show_memory("driver 刚 ray.put 后，应该能看到 LOCAL_REFERENCE")

    pause("阶段 2：把 ref 作为顶层参数传给 actor")
    info = ray.get(actor.receive_top_level.remote(ref))
    print("actor 收到的参数信息:", info)
    print("结论：顶层 ObjectRef 参数被自动解引用，actor 收到的是 ndarray 值。")
    show_memory("actor 保存了反序列化后的值，可能看到 PINNED_IN_MEMORY")

    pause("阶段 3：把 ref 包在 list 里传给 actor，让 actor 保存 ObjectRef")
    info = ray.get(actor.store_wrapped_ref.remote([ref]))
    print("actor 保存的信息:", info)
    del ref
    gc.collect()
    print("driver 已经 del ref 并 gc.collect()")
    print("actor 仍可通过保存的 ObjectRef ray.get:", ray.get(actor.get_stored_sum.remote()))
    show_memory("driver 删除 ref 后，actor 仍持有 ObjectRef，object 不应释放")

    pause("阶段 4：让 actor 清理保存的 ObjectRef 和上一步保存的 value")
    print(ray.get(actor.clear_stored_ref.remote()))
    print(ray.get(actor.clear_value.remote()))
    gc.collect()
    show_memory("actor 清理后，如果没有其他引用，对象可以 out of scope")

    pause("阶段 5：演示 ray.get 的返回值也可能 pin 住 object store")
    ref2 = ray.put(make_array(2))
    local_value = ray.get(ref2)
    print("local_value sum:", int(local_value.sum()))
    del ref2
    gc.collect()
    show_memory("del ref2 后，local_value 仍在 driver，可能看到 PINNED_IN_MEMORY")

    pause("阶段 6：删除 ray.get 返回的 local_value")
    del local_value
    gc.collect()
    show_memory("删除 local_value 后，对象可以释放")

    ray.shutdown()
    print("demo 完成")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(130)
