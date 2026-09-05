"""Small deterministic utilities shared by simulation and evaluation."""
from __future__ import annotations
import hashlib


def stable_seed(value: str, modulo: int = 2**31 - 1) -> int:
    """Return a process-independent integer seed derived from SHA-256."""
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % modulo
