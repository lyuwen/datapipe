"""Environment detection helpers for distributed launch contexts.

Detection priority (deterministic):
1. explicit arguments (handled by the caller);
2. torchrun-compatible env;
3. Slurm;
4. K8s indexed job;
5. local fallback.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class DetectionResult:
    rank: int
    world_size: int
    local_rank: int | None = None
    node_rank: int | None = None
    job_id: str | None = None
    environment: str = "local"


def _int_env(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def detect_torchrun() -> DetectionResult | None:
    rank = _int_env("RANK")
    world = _int_env("WORLD_SIZE")
    if rank is None or world is None:
        return None
    local_rank = _int_env("LOCAL_RANK")
    local_world = _int_env("LOCAL_WORLD_SIZE")
    node_rank = None
    if local_rank is not None and local_world is not None and local_world > 0:
        # torchrun does not set NODE_RANK, but LOCAL_WORLD_SIZE lets us derive
        # a node index only when LOCAL_RANK is 0-based; keep it None to avoid
        # guessing. We do report LOCAL_WORLD_SIZE via job_id for info.
        node_rank = None
    return DetectionResult(
        rank=rank,
        world_size=world,
        local_rank=local_rank,
        node_rank=node_rank,
        job_id=f"torchrun:{local_world}" if local_world is not None else None,
        environment="torchrun",
    )


def detect_slurm() -> DetectionResult | None:
    rank = _int_env("SLURM_PROCID")
    world = _int_env("SLURM_NTASKS")
    if rank is None or world is None:
        return None
    return DetectionResult(
        rank=rank,
        world_size=world,
        local_rank=_int_env("SLURM_LOCALID"),
        node_rank=_int_env("SLURM_NODEID"),
        job_id=os.environ.get("SLURM_JOB_ID"),
        environment="slurm",
    )


def detect_k8s() -> DetectionResult | None:
    index = _int_env("JOB_COMPLETION_INDEX")
    world = _int_env("WORLD_SIZE")
    if index is None or world is None:
        return None
    return DetectionResult(
        rank=index,
        world_size=world,
        local_rank=None,
        node_rank=None,
        job_id=os.environ.get("JOB_NAME"),
        environment="k8s",
    )


def detect_local() -> DetectionResult:
    return DetectionResult(
        rank=0,
        world_size=1,
        local_rank=0,
        node_rank=0,
        job_id=None,
        environment="local",
    )


def detect() -> DetectionResult:
    """Run deterministic detection priority: torchrun, Slurm, K8s, local."""
    for detector in (detect_torchrun, detect_slurm, detect_k8s):
        result = detector()
        if result is not None:
            return result
    return detect_local()
