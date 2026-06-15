import asyncio
import inspect
import os
import signal
import sys
import traceback


def _frame_location(frame):
    return f"{frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}"


def _awaitable_name(obj):
    if inspect.iscoroutine(obj):
        return obj.cr_code.co_name
    if inspect.isgenerator(obj):
        return obj.gi_code.co_name
    if inspect.isasyncgen(obj):
        return obj.ag_code.co_name
    return type(obj).__name__


def _await_chain(obj, *, depth=0, seen=None):
    if obj is None:
        return []

    if seen is None:
        seen = set()

    obj_id = id(obj)
    if obj_id in seen:
        return [f"{'  ' * depth}<cycle: {obj!r}>"]
    seen.add(obj_id)

    indent = "  " * depth
    lines = []

    if isinstance(obj, asyncio.Task):
        lines.append(f"{indent}Task name={obj.get_name()!r} state={obj._state}")
        waiter = getattr(obj, "_fut_waiter", None)
        lines.extend(_await_chain(obj.get_coro(), depth=depth + 1, seen=seen))
        if waiter is not None:
            lines.append(f"{indent}  waiting on: {waiter!r}")
        return lines

    frame = (
        getattr(obj, "cr_frame", None)
        or getattr(obj, "gi_frame", None)
        or getattr(obj, "ag_frame", None)
    )
    if frame is not None:
        lines.append(f"{indent}{_awaitable_name(obj)} at {_frame_location(frame)}")
    else:
        lines.append(f"{indent}{_awaitable_name(obj)} {obj!r}")

    next_obj = (
        getattr(obj, "cr_await", None)
        or getattr(obj, "gi_yieldfrom", None)
        or getattr(obj, "ag_await", None)
    )
    if next_obj is not None:
        lines.extend(_await_chain(next_obj, depth=depth + 1, seen=seen))

    return lines


def dump_asyncio_tasks(loop):
    print("\n========== asyncio task dump ==========", file=sys.stderr)
    for task in sorted(asyncio.all_tasks(loop), key=lambda item: item.get_name()):
        print(f"\n--- {task.get_name()} ---", file=sys.stderr)
        print(repr(task), file=sys.stderr)

        print("\nawait chain:", file=sys.stderr)
        for line in _await_chain(task):
            print(line, file=sys.stderr)

        stack = task.get_stack()
        if stack:
            print("\nraw task stack:", file=sys.stderr)
            for frame in stack:
                traceback.print_stack(frame, file=sys.stderr)
        else:
            print("\nraw task stack: <empty>", file=sys.stderr)
    print("======== end asyncio task dump ========\n", file=sys.stderr)


async def leaf_wait_forever():
    event = asyncio.Event()
    print("leaf_wait_forever: about to await event.wait(); event is never set", flush=True)
    await event.wait()


async def middle_layer():
    await leaf_wait_forever()


async def stuck_worker():
    await middle_layer()


async def sleepy_worker():
    while True:
        await asyncio.sleep(3600)


async def main():
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGUSR1, dump_asyncio_tasks, loop)

    print(f"pid: {os.getpid()}", flush=True)
    print(f"send task dump: kill -USR1 {os.getpid()}", flush=True)

    tasks = [
        asyncio.create_task(stuck_worker(), name="stuck-worker"),
        asyncio.create_task(sleepy_worker(), name="sleepy-worker"),
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
