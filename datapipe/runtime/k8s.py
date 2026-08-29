"""Kubernetes indexed-job environment helpers."""

from __future__ import annotations

import os


def k8s_env() -> dict[str, str | None]:
    """Return the relevant K8s indexed-job environment variables."""
    return {
        "job_completion_index": os.environ.get("JOB_COMPLETION_INDEX"),
        "world_size": os.environ.get("WORLD_SIZE"),
        "job_name": os.environ.get("JOB_NAME"),
    }
