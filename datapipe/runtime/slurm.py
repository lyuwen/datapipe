"""Slurm helpers (kept separate for future expansion)."""

from __future__ import annotations

import os


def slurm_env() -> dict[str, str | None]:
    """Return the relevant Slurm environment variables."""
    return {
        "rank": os.environ.get("SLURM_PROCID"),
        "world_size": os.environ.get("SLURM_NTASKS"),
        "local_rank": os.environ.get("SLURM_LOCALID"),
        "node_rank": os.environ.get("SLURM_NODEID"),
        "job_id": os.environ.get("SLURM_JOB_ID"),
    }
