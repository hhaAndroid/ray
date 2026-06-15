import asyncio
import os
import time


async def stuck_worker():
    event = asyncio.Event()
    print("stuck_worker: waiting forever on event.wait()", flush=True)
    await event.wait()


async def one_shot_stall_monitor(stall_s=3.0, interval_s=1.0):
    last_change_at = time.perf_counter()
    dumped = False
    progress_signature = "no-progress"

    while True:
        await asyncio.sleep(interval_s)
        now = time.perf_counter()

        current_signature = "no-progress"
        if current_signature != progress_signature:
            progress_signature = current_signature
            last_change_at = now
            dumped = False
            continue

        if dumped or now - last_change_at < stall_s:
            print(
                f"monitor: alive, no dump. dumped={dumped}, "
                f"stall_for={now - last_change_at:.1f}s",
                flush=True,
            )
            continue

        print("monitor: stall detected, dump task stacks once", flush=True)
        for task in asyncio.all_tasks():
            if task is asyncio.current_task():
                continue
            print(f"  task={task!r}", flush=True)
            for frame in task.get_stack():
                print(f"    {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}", flush=True)

        dumped = True


async def main():
    print(f"pid: {os.getpid()}", flush=True)
    asyncio.create_task(one_shot_stall_monitor(), name="one-shot-monitor")
    await stuck_worker()


if __name__ == "__main__":
    asyncio.run(main())
