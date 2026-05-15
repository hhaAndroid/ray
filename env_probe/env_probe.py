import argparse
import json
import os
import socket
from typing import Dict, List

import ray


DEFAULT_KEYS = [
    "PROBE_BEFORE",
    "PROBE_AFTER",
    "PROBE_RUNTIME",
    "WORK_DIR",
    "MODEL_PATH",
    "DATA_PATH",
    "EVAL_DATA_PATH",
    "RAY_ADDRESS",
    "RAY_JOB_ID",
]


def parse_key_values(items: List[str]) -> Dict[str, str]:
    result = {}
    for item in items:
        if "=" not in item:
            raise ValueError(f"Expected KEY=VALUE, got {item!r}")
        key, value = item.split("=", 1)
        result[key] = value
    return result


def snapshot_env(keys: List[str]) -> Dict[str, str | None]:
    return {key: os.environ.get(key) for key in keys}


def current_process_info(role: str, keys: List[str]) -> Dict:
    ctx = ray.get_runtime_context() if ray.is_initialized() else None
    return {
        "role": role,
        "pid": os.getpid(),
        "hostname": socket.gethostname(),
        "node_id": ctx.get_node_id() if ctx is not None else None,
        "job_id": ctx.get_job_id() if ctx is not None else None,
        "env": snapshot_env(keys),
    }


@ray.remote
def probe_task(keys: List[str]) -> Dict:
    return current_process_info("ray_task", keys)


@ray.remote
class ProbeActor:
    def read_env(self, keys: List[str]) -> Dict:
        return current_process_info("ray_actor", keys)


def print_record(record: Dict) -> None:
    print(json.dumps(record, indent=2, sort_keys=True), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--init",
        choices=["local", "auto", "address"],
        default="local",
        help="How this process should call ray.init().",
    )
    parser.add_argument("--address", default=None, help="Ray address for --init=address.")
    parser.add_argument(
        "--key",
        action="append",
        default=[],
        help="Extra environment variable name to report. Can be passed multiple times.",
    )
    parser.add_argument(
        "--set-after-init",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Set env var in the driver after ray.init(), before creating task/actor.",
    )
    parser.add_argument(
        "--runtime-env",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Pass env var through ray.init(runtime_env={'env_vars': ...}).",
    )
    args = parser.parse_args()

    keys = list(dict.fromkeys(DEFAULT_KEYS + args.key))
    runtime_env_vars = parse_key_values(args.runtime_env)
    runtime_env = {"env_vars": runtime_env_vars} if runtime_env_vars else None

    print_record(current_process_info("driver_before_ray_init", keys))

    if args.init == "local":
        ray.init(num_cpus=2, include_dashboard=False, runtime_env=runtime_env)
    elif args.init == "auto":
        ray.init(address="auto", runtime_env=runtime_env)
    else:
        if not args.address:
            raise ValueError("--address is required when --init=address")
        ray.init(address=args.address, runtime_env=runtime_env)

    print_record(current_process_info("driver_after_ray_init", keys))

    for key, value in parse_key_values(args.set_after_init).items():
        os.environ[key] = value

    print_record(current_process_info("driver_after_env_mutation", keys))
    print_record(ray.get(probe_task.remote(keys)))

    actor = ProbeActor.remote()
    print_record(ray.get(actor.read_env.remote(keys)))

    ray.shutdown()


if __name__ == "__main__":
    main()
