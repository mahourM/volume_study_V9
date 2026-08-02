from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AbsorptionPathState:
    """Empty shell for the future independent absorption path."""

    path_ready: bool = False
