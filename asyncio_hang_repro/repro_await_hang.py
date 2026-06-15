import asyncio
import os


async def wait_forever():
    event = asyncio.Event()
    print("about to await event.wait(); this event is never set", flush=True)
    await event.wait()
    print("unreachable", flush=True)


async def main():
    print(f"pid: {os.getpid()}", flush=True)
    await wait_forever()


if __name__ == "__main__":
    asyncio.run(main())
