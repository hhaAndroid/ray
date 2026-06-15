import asyncio
import os
import time


def blocking_sync_call():
    print("blocking_sync_call: enter time.sleep(3600)", flush=True)
    time.sleep(3600)
    print("blocking_sync_call: unreachable until sleep returns", flush=True)


async def monitor():
    while True:
        print("monitor: event loop is alive", flush=True)
        await asyncio.sleep(1)


async def worker_blocks_loop():
    await asyncio.sleep(0.1)
    print("worker_blocks_loop: about to call blocking sync function", flush=True)
    blocking_sync_call()


async def main():
    print(f"pid: {os.getpid()}", flush=True)
    asyncio.create_task(monitor(), name="monitor")
    await worker_blocks_loop()


if __name__ == "__main__":
    asyncio.run(main())
