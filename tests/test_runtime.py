"""Runtime context and environment detection tests."""

from __future__ import annotations

import pytest

from datapipe import (
    ModuloSharding,
    NoSharding,
    RuntimeContext,
    default_sharding_for,
)
from datapipe.runtime.detect import (
    detect_k8s,
    detect_local,
    detect_slurm,
    detect_torchrun,
)


@pytest.fixture
def clean_env(monkeypatch):
    """Remove all recognized distributed env vars before each test."""
    keys = [
        "RANK",
        "WORLD_SIZE",
        "LOCAL_RANK",
        "LOCAL_WORLD_SIZE",
        "SLURM_PROCID",
        "SLURM_NTASKS",
        "SLURM_LOCALID",
        "SLURM_NODEID",
        "SLURM_JOB_ID",
        "JOB_COMPLETION_INDEX",
        "JOB_NAME",
    ]
    for k in keys:
        monkeypatch.delenv(k, raising=False)
    return monkeypatch


def test_local_detection(clean_env):
    r = detect_local()
    assert r.rank == 0
    assert r.world_size == 1
    assert r.environment == "local"


def test_torchrun_detection(clean_env):
    clean_env.setenv("RANK", "3")
    clean_env.setenv("WORLD_SIZE", "8")
    clean_env.setenv("LOCAL_RANK", "1")
    r = detect_torchrun()
    assert r is not None
    assert r.rank == 3
    assert r.world_size == 8
    assert r.local_rank == 1
    assert r.environment == "torchrun"


def test_slurm_detection(clean_env):
    clean_env.setenv("SLURM_PROCID", "2")
    clean_env.setenv("SLURM_NTASKS", "16")
    clean_env.setenv("SLURM_LOCALID", "0")
    clean_env.setenv("SLURM_NODEID", "1")
    clean_env.setenv("SLURM_JOB_ID", "12345")
    r = detect_slurm()
    assert r is not None
    assert r.rank == 2
    assert r.world_size == 16
    assert r.local_rank == 0
    assert r.node_rank == 1
    assert r.job_id == "12345"
    assert r.environment == "slurm"


def test_k8s_detection(clean_env):
    clean_env.setenv("JOB_COMPLETION_INDEX", "5")
    clean_env.setenv("WORLD_SIZE", "10")
    r = detect_k8s()
    assert r is not None
    assert r.rank == 5
    assert r.world_size == 10
    assert r.environment == "k8s"


def test_detect_priority(clean_env):
    """torchrun beats Slurm beats K8s beats local."""
    clean_env.setenv("RANK", "0")
    clean_env.setenv("WORLD_SIZE", "2")
    clean_env.setenv("SLURM_PROCID", "9")
    clean_env.setenv("SLURM_NTASKS", "20")
    from datapipe.runtime.detect import detect

    r = detect()
    assert r.environment == "torchrun"
    assert r.world_size == 2


def test_runtime_context_auto_local(clean_env):
    rc = RuntimeContext.auto()
    assert rc.rank == 0
    assert rc.world_size == 1
    assert rc.environment == "local"


def test_runtime_context_explicit_override(clean_env):
    rc = RuntimeContext.auto(rank=7, world_size=20)
    assert rc.rank == 7
    assert rc.world_size == 20


def test_runtime_context_validation():
    with pytest.raises(ValueError):
        RuntimeContext(rank=5, world_size=4)
    with pytest.raises(ValueError):
        RuntimeContext(rank=0, world_size=0)


def test_default_sharding():
    assert isinstance(default_sharding_for(RuntimeContext()), NoSharding)
    multi = RuntimeContext(rank=1, world_size=4)
    assert isinstance(default_sharding_for(multi), ModuloSharding)
