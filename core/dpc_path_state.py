from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DpcPathState:
    """Separate placeholder for the future independent DPC path."""

    path_ready: bool = False
