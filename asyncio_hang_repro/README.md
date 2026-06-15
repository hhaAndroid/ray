# asyncio 卡住复现

运行：

```bash
python asyncio_hang_repro/repro_await_hang.py
```

脚本会打印自己的 PID，然后卡在：

```python
await event.wait()
```

这个 `event` 永远不会被 `set()`，所以协程会挂起。主线程会回到 asyncio event loop，继续等待某个事件把它唤醒。

另开一个终端查看：

```bash
pystack remote <PID>
```

根据 Python 和 pystack 版本不同，你通常会看到主线程停在 `asyncio.run`、`run_until_complete`、`_run_once` 或 selector/epoll 相关位置，而不是像同步阻塞那样直接看到栈顶停在 `await event.wait()`。

如果你使用 Python 3.14+，可以对比 asyncio-aware 的查看方式：

```bash
python -m asyncio pstree <PID>
```

这个命令更可能直接展示挂起的 task/coroutine 链路。

## Python 3.12 排查方式

Python 3.12 没有 `python -m asyncio pstree <PID>` 这种外部查看 asyncio task 的标准工具。更实用的做法是在进程内预留一个 dump 入口，比如信号触发。

运行这个版本：

```bash
python asyncio_hang_repro/repro_with_signal_dump.py
```

它会打印 PID 和提示命令：

```bash
kill -USR1 <PID>
```

发信号后，程序会在 stderr 打印所有 asyncio task 的信息，包括：

- task 名称和状态
- coroutine 的 await 链路
- `task.get_stack()` 的原始栈

这个版本能帮助你看到类似 `stuck_worker -> middle_layer -> leaf_wait_forever -> Event.wait` 的逻辑等待链，而不是只看到主线程停在 event loop。

## 对照：event loop 被同步函数卡住

运行：

```bash
python asyncio_hang_repro/repro_loop_blocked_by_sync.py
```

这个脚本会启动一个 monitor task，每秒打印：

```text
monitor: event loop is alive
```

随后另一个 task 会调用同步函数：

```python
time.sleep(3600)
```

一旦进入 `time.sleep()`，event loop 被当前线程占住，monitor task 也不会再被调度，所以 heartbeat 会停止。

此时另开终端执行：

```bash
pystack remote <PID>
```

这类同步阻塞通常能在 pystack 里看到业务调用链，例如 `worker_blocks_loop -> blocking_sync_call -> time.sleep`。这和 `await event.wait()` 那种“协程挂起、主线程回到 event loop”的情况不同。

## 对照：monitor 只 dump 一次，看起来像后来不打印

运行：

```bash
python asyncio_hang_repro/repro_monitor_one_shot.py
```

这个脚本里业务 task 永远卡在：

```python
await event.wait()
```

event loop 没有被同步函数阻塞，所以 pystack 仍然更可能看到 `_run_once`、selector 或 epoll。

但 monitor 使用了和 producer stall monitor 类似的 one-shot 逻辑：

```python
if dumped or now - last_change_at < stall_s:
    continue
...
dumped = True
```

所以同一次 stall 只会 dump 一次。只要进度签名一直不变，`dumped=True` 会一直保持，后续不会反复打印栈。这个现象容易被误解成“卡住后 monitor 不工作了”。
