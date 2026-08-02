from __future__ import annotations

from .absorption_cluster_model import AbsorptionRuntimeConfig, TimeframeSpec


def build_default_absorption_config() -> AbsorptionRuntimeConfig:
    return AbsorptionRuntimeConfig(
        enabled_timeframes=(
            TimeframeSpec("M1", 60_000),
            TimeframeSpec("M5", 300_000),
            TimeframeSpec("M15", 900_000),
            TimeframeSpec("M30", 1_800_000),
            TimeframeSpec("H1", 3_600_000),
        ),
        rolling_candle_buffer_size=3,
    )
