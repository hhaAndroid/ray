# Ray 启动方式与环境变量传播验证笔记

本文只讨论 Ray 本身的启动方式、`ray.init()` 发生在哪里，以及环境变量如何传播。
这里不讨论任何具体训练框架内部实现。

## 1. 核心概念

### 1.1 `ray start` 不等于 `ray.init`

`ray start --head` 的作用是启动 Ray 集群的 head 节点后台服务，例如：

```text
GCS
raylet
object store
dashboard
dashboard agent
runtime env agent
```

它不会让当前 shell 进程变成 Ray driver，也不会自动在用户 Python 程序里调用
`ray.init()`。

worker 节点上的：

```bash
ray start --address="$HEAD:6379" --block
```

也是启动本机 Ray 后台服务，核心是本机 raylet / object store / agent，并把本节点资源注册到 head 的 GCS。

`--block` 只是让 `ray start` 命令前台不退出，常用于容器或多机脚本里保持 worker 节点进程存活。它不是阻塞 Ray 调度。

真正创建 Ray driver 的，是用户 Python 进程里的：

```python
ray.init()
ray.init(address="auto")
ray.init("ray://...")
```

或者 Ray Jobs 启动 entrypoint 后，由 entrypoint 代码内部调用 `ray.init(...)`。

### 1.2 环境变量不是全局广播

shell 里的：

```bash
export FOO=bar
```

只影响当前 shell 及其子进程。

它不会自动广播到其他机器，也不会自动修改已经启动的 Ray 系统进程。

所以要区分：

```text
shell env
driver env
raylet / system process env
Ray task / actor worker env
Ray job runtime_env env_vars
```

## 2. 三种启动方式

### 2.1 方式一：直接 Python，代码里 `ray.init()`

形式：

```bash
export FOO=bar
python train.py
```

代码：

```python
import ray

ray.init()
...
```

这时：

```text
python train.py 进程
  -> 调用 ray.init()
  -> 如果没有已有集群，启动本地 Ray
  -> 当前 Python 进程成为 Ray driver
  -> driver 创建 Ray task / actor
```

环境变量传播模型：

```text
shell 在 python 启动前 export 的 env
  -> driver 能看到
  -> Ray task / actor 通常也能看到

driver 在 ray.init() 之后才改 os.environ
  -> driver 自己能看到
  -> 已不应期待自动传播给 Ray task / actor
```

这里说的是“不会自动传播”，不是说完全没有办法传给 task / actor。

如果确实需要在 Ray worker 进程里注入环境变量，应该使用 Ray 的 `runtime_env`：

```python
ray.init(runtime_env={"env_vars": {"FOO": "bar"}})
```

这样 `FOO` 会成为这个 driver job 的 runtime env，后续由它创建的 Ray task / actor 通常能看到。

也可以只给某个 task / actor 单独设置：

```python
@ray.remote(runtime_env={"env_vars": {"FOO": "task-level"}})
def f():
    ...

Actor.options(runtime_env={"env_vars": {"FOO": "actor-level"}}).remote()
```

或者在提交 task 时覆盖：

```python
f.options(runtime_env={"env_vars": {"FOO": "call-level"}}).remote()
```

因此更准确的规则是：

```text
driver 修改 os.environ
  -> 不会自动同步给 Ray worker

要让 Ray worker 获得新的 env
  -> 用 ray.init(runtime_env=...)
  -> 或 task / actor 级别 runtime_env
```

### 2.2 方式二：`ray start` 后直接 Python attach

形式：

```bash
export FOO=bar

ray start --head \
  --port=6379 \
  --dashboard-port=8265

python train.py
```

代码：

```python
import ray

ray.init(address="auto")
...
```

多机时通常是：

```text
head 节点:
  export ...
  ray start --head
  python train.py
  train.py 内 ray.init(address="auto")

worker 节点:
  export ...
  ray start --address=head:6379 --block
```

关键点：

```text
ray start 只启动 Ray 系统进程。
python train.py 才是用户 driver。
worker 节点因为 --block，不会继续跑后面的 python train.py。
```

环境变量传播模型：

```text
head shell 里的 env
  -> head ray start 子进程能继承
  -> head 上后续 python driver 也能继承

worker shell 里的 env
  -> worker ray start 子进程能继承
  -> 本节点 Ray worker 进程可能继承对应 raylet / agent 的环境

head shell env
  -> 不会自动广播到 worker 节点 shell
```

所以方式二在多机下如果依赖业务环境变量，通常要求每个节点都以相同脚本和相同参数启动。
如果 Ray 集群是早就启动好的，而不是当前脚本启动的，那么 shell 里的新 env 不一定会进入 Ray worker。

解决办法和方式一相同：不要只依赖 shell env，而是在 driver 连接集群时显式设置 job-level
`runtime_env`：

```python
ray.init(
    address="auto",
    runtime_env={"env_vars": {"FOO": "bar"}},
)
```

这样即使 Ray 集群早已启动，`FOO` 也会跟着当前 driver job 传给它创建的 Ray task / actor。
如果只想影响某个 task / actor，也可以使用 task / actor 级别的 `runtime_env`。

### 2.3 方式三：`ray start` 后 `ray job submit`

形式：

```bash
ray start --head \
  --port=6379 \
  --dashboard-port=8265

ray job submit \
  --address="http://127.0.0.1:8265" \
  --runtime-env-json='{"env_vars": {"FOO": "bar"}}' \
  -- python train.py
```

代码：

```python
import ray

ray.init(address="auto")
...
```

链路是：

```text
ray job submit CLI
  -> Dashboard Jobs HTTP API
  -> JobManager / JobSupervisor
  -> 启动 entrypoint subprocess
  -> entrypoint 运行 python train.py
  -> train.py 内 ray.init(address="auto")
  -> entrypoint 成为 Ray driver
  -> driver 创建 Ray task / actor
```

关键点：

```text
ray job submit CLI 本身不是 driver。
ray job submit 不等于 ray.init。
真正的 ray.init 发生在 job entrypoint 进程里的用户代码。
```

`--runtime-env-json` 注入的是这个 Ray job 的运行环境：

```text
runtime_env env_vars
  -> job entrypoint driver 能看到
  -> 该 job 创建的 Ray task / actor 通常也能看到
```

因此，entrypoint 里的 `ray.init()` 通常不需要重复传同一份 `runtime_env`：

```python
ray.init(address="auto")
```

即可。Ray Jobs 启动 entrypoint 时已经把 `--runtime-env-json` 里的环境变量注入到
entrypoint 进程，并把 job-level runtime env 绑定到这个 Ray job。entrypoint 调用
`ray.init(address="auto")` 后，它创建的 task / actor 会继续继承这份 job-level runtime env。

不推荐常规写成：

```python
ray.init(
    address="auto",
    runtime_env={"env_vars": {"FOO": "bar"}},
)
```

除非确实需要在 entrypoint 代码里额外追加或覆盖 runtime env。常规 Ray Jobs 场景下，业务环境变量应该放在
`ray job submit --runtime-env-json`，entrypoint 内部保持 `ray.init(address="auto")` 即可。

需要特别区分 shell env 和 job runtime env：

```bash
export FOO=bar
ray job submit -- python train.py
```

这种 shell 里的 `FOO` 只保证 `ray job submit` 这个 CLI 子进程能看到，不等价于 Ray Jobs entrypoint
一定能看到，也不等价于 Ray task / actor 一定能看到。

如果 Ray head 正好也是当前 shell 刚刚 `ray start --head` 启动的，并且 job entrypoint 默认调度在 head
上，那么按当前 Ray Jobs 实现，这条路径通常是能工作的：

```text
当前 shell env
  -> ray start --head 启动的 Ray 系统进程
  -> JobSupervisor actor 进程
  -> JobSupervisor 用 os.environ.copy() 启动 entrypoint subprocess
  -> entrypoint driver
```

所以在这个受限条件下，entrypoint 看到 shell env 不是随机事件，而是进程继承链的结果，XTuner 目前代码正好是这样。

但它仍然不是推荐依赖的业务 env 传播机制，原因是这条链路依赖几个前提：

```text
Ray 集群必须是当前 shell 带着这些 env 启动的；
JobSupervisor / entrypoint 必须运行在继承了这些 env 的节点上；
Ray task / actor 如果被调度到其他节点，也要求那些节点的 raylet / worker 进程同样继承了这些 env；
如果使用已有集群、指定 entrypoint 资源、允许 driver 跑到 worker 节点，或者某些节点启动 env 不一致，就可能丢失。
```

也就是说，shell env 继承是“当前启动拓扑下能工作”的进程继承行为；`runtime_env` 才是 Ray job / task /
actor 级别的显式环境声明。

还有一个容易混淆的点：`ray job submit` 这个 CLI 可以在任意能访问 dashboard 的机器上运行，但它所在 shell
里的 env 不会因为提交动作自动变成 job entrypoint 的 env。

例如：

```bash
# 机器 A：启动 Ray head，没有 FOO
ray start --head

# 机器 B：提交 job，shell 里有 FOO
export FOO=bar
ray job submit --address=http://head:8265 -- python train.py
```

这种情况下，`FOO` 只保证机器 B 上的 `ray job submit` CLI 进程能看到。entrypoint 是由 Ray 集群内部的
JobSupervisor 启动的，不是机器 B 的 CLI fork 出来的，所以 entrypoint 不会因为机器 B 的 shell 有 `FOO`
就自动看到 `FOO`。换句话说，`python train.py` 这个 entrypoint driver 进程默认看不到机器 B shell 里的
`FOO`。

如果希望 `train.py` 看到 `FOO`，必须显式传递，例如通过 Ray Jobs runtime env：

```bash
ray job submit \
  --address=http://head:8265 \
  --runtime-env-json='{"env_vars": {"FOO": "bar"}}' \
  -- python train.py
```

或者把值作为普通命令行参数传给 `train.py`，再由 `train.py` 自己解析。

这个结论和 `ray job submit` 是否在 head 节点运行无关。即使是：

```text
head 节点:
  ray start --head 时没有 FOO

提交节点 B:
  ray job submit \
    --address=http://head:8265 \
    --runtime-env-json='{"env_vars": {"FOO": "bar"}}' \
    -- python train.py
```

`FOO` 仍然会作为 job spec 的一部分通过 Jobs API 发送给 Ray 集群。head 上的 JobSupervisor 启动
`python train.py` 时会应用这份 job runtime env，所以 `train.py` 能看到 `FOO=bar`。这和 submit
节点 B 的 shell env 继承无关。

同理，如果只有 head 的 `ray start --head` 带了 `FOO`，而 worker 节点 `ray start --address=...` 没带
`FOO`，那么：

```text
entrypoint 默认跑在 head 上时，可能能看到 FOO；
调度到 head 的 task / actor 可能能看到 FOO；
调度到 worker 节点的 task / actor 不应期待看到 FOO。
```

如果希望与提交机器、entrypoint 位置、task / actor 调度位置无关，应使用 job-level `runtime_env`。

Ray Jobs 模式下要稳定传给 entrypoint / task / actor，应使用：

```bash
ray job submit \
  --runtime-env-json='{"env_vars": {"FOO": "bar"}}' \
  -- python train.py
```

它不是给整个 Ray 集群全局设置环境变量，也不会修改：

```text
raylet
GCS
dashboard
worker 节点 shell
其他 job
```

```bash
submit 节点 shell export FOO

    -> 不会自动传给 train.py

  submit 节点 --runtime-env-json 传 FOO

    -> 会传给 train.py 和该 job 的 task/actor
```

## 3. 验证脚本

验证代码在：

```text
env_probe/env_probe.py
```

它会打印五类进程看到的环境变量：

```text
driver_before_ray_init
driver_after_ray_init
driver_after_env_mutation
ray_task
ray_actor
```

其中关注三个变量：

```text
PROBE_BEFORE   # shell 提前设置
PROBE_AFTER    # driver 在 ray.init() 之后才设置
PROBE_RUNTIME  # Ray Jobs runtime_env 设置
```

## 4. 实验一：直接 Python + `ray.init()`

启动脚本：

```bash
env_probe/run_mode1_direct_python.sh
```

核心逻辑：

```bash
export PROBE_BEFORE="set-in-shell-before-python"

python env_probe/env_probe.py \
  --init local \
  --set-after-init PROBE_AFTER=set-in-driver-after-ray-init
```

观察结果：

```text
driver_before_ray_init:
  PROBE_BEFORE = set-in-shell-before-python
  PROBE_AFTER = null

driver_after_env_mutation:
  PROBE_BEFORE = set-in-shell-before-python
  PROBE_AFTER = set-in-driver-after-ray-init

ray_task:
  PROBE_BEFORE = set-in-shell-before-python
  PROBE_AFTER = null

ray_actor:
  PROBE_BEFORE = set-in-shell-before-python
  PROBE_AFTER = null
```

结论：

```text
shell 在 Python 启动前设置的 env，driver / task / actor 都能看到。
driver 在 ray.init() 后才设置的 env，只有 driver 自己能看到，task / actor 看不到。
```

## 5. 实验二：`ray start --head` + 直接 Python attach

启动脚本：

```bash
env_probe/run_mode2_ray_start_then_python.sh
```

核心逻辑：

```bash
export PROBE_BEFORE="set-in-shell-before-ray-start"

ray start --head ...

python env_probe/env_probe.py \
  --init auto \
  --set-after-init PROBE_AFTER=set-in-driver-after-ray-init
```

观察结果：

```text
driver_before_ray_init:
  PROBE_BEFORE = set-in-shell-before-ray-start

driver_after_ray_init:
  PROBE_BEFORE = set-in-shell-before-ray-start

driver_after_env_mutation:
  PROBE_AFTER = set-in-driver-after-ray-init

ray_task:
  PROBE_BEFORE = set-in-shell-before-ray-start
  PROBE_AFTER = null

ray_actor:
  PROBE_BEFORE = set-in-shell-before-ray-start
  PROBE_AFTER = null
```

结论：

```text
ray start 不会触发 ray.init。
后面的 Python 进程调用 ray.init(address="auto") 后才成为 driver。

ray start / python 之前已经存在的 shell env，driver / task / actor 能看到。
driver 在 ray.init() 之后才设置的 env，task / actor 看不到。
```

## 6. 实验三：`ray job submit --runtime-env-json`

启动脚本：

```bash
env_probe/run_mode3_ray_job_submit.sh
```

核心逻辑：

```bash
unset PROBE_BEFORE
unset PROBE_RUNTIME

ray start --head ...

ray job submit \
  --runtime-env-json='{
    "env_vars": {
      "PROBE_RUNTIME": "set-by-ray-job-runtime-env-json"
    }
  }' \
  -- python env_probe/env_probe.py \
  --init auto \
  --set-after-init PROBE_AFTER=set-in-job-driver-after-ray-init
```

观察结果：

```text
driver_before_ray_init:
  PROBE_BEFORE = null
  PROBE_RUNTIME = set-by-ray-job-runtime-env-json
  RAY_ADDRESS = head:6379

driver_after_ray_init:
  PROBE_RUNTIME = set-by-ray-job-runtime-env-json

driver_after_env_mutation:
  PROBE_AFTER = set-in-job-driver-after-ray-init
  PROBE_RUNTIME = set-by-ray-job-runtime-env-json

ray_task:
  PROBE_BEFORE = null
  PROBE_AFTER = null
  PROBE_RUNTIME = set-by-ray-job-runtime-env-json

ray_actor:
  PROBE_BEFORE = null
  PROBE_AFTER = null
  PROBE_RUNTIME = set-by-ray-job-runtime-env-json
```

结论：

```text
--runtime-env-json 不是给 ray job submit CLI 自己用的。
它会注入到 job entrypoint driver。
entrypoint driver 创建的 Ray task / actor 也能看到它。

即使 shell 没有 export PROBE_RUNTIME，只要 runtime_env env_vars 里设置了，
job driver / task / actor 仍然能看到。
```

## 7. 总结

三种方式最重要的差异：

```text
方式一：直接 python + ray.init()
  ray.init 发生在当前 Python 进程。
  当前 Python 进程就是 driver。

方式二：ray start + python + ray.init(address="auto")
  ray start 只启动集群服务。
  后面的 Python 进程调用 ray.init 后成为 driver。

方式三：ray start + ray job submit
  ray job submit 只提交 job。
  Ray Jobs 启动 entrypoint subprocess。
  entrypoint 里的用户代码调用 ray.init 后成为 driver。
```

环境变量传播建议：

```text
只在本地直接跑:
  shell export 通常够用。

手动 ray start 多机跑:
  每个节点都要在 ray start 之前准备好一致的必要 env。
  不要以为 head shell env 会广播到 worker。

Ray Jobs 跑:
  优先使用 --runtime-env-json / runtime_env={"env_vars": ...} 注入业务 env。
  这比依赖 raylet 启动时继承 shell env 更稳定。
```

