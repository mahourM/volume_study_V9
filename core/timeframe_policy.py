from __future__ import annotations


STUDY_TIMEFRAMES: tuple[str, ...] = ("M1", "M5", "M15", "M30", "H1")
EXECUTION_TIMEFRAMES: tuple[str, ...] = ("M5",)
DEFAULT_FOOTPRINT_TIMEFRAME = "M1"
DISPLAY_WINDOW_HOURS = 24

TIMEFRAME_MS_BY_NAME: dict[str, int] = {
    "M1": 60_000,
    "M5": 5 * 60_000,
    "M15": 15 * 60_000,
    "M30": 30 * 60_000,
    "H1": 60 * 60_000,
}

STUDY_DISPLAY_CANDLE_LIMITS: dict[str, int] = {
    timeframe: max(1, (DISPLAY_WINDOW_HOURS * 60 * 60 * 1000) // TIMEFRAME_MS_BY_NAME[timeframe])
    for timeframe in STUDY_TIMEFRAMES
}


def normalized_study_timeframes() -> tuple[str, ...]:
    return STUDY_TIMEFRAMES


def normalized_execution_timeframes() -> tuple[str, ...]:
    return EXECUTION_TIMEFRAMES


def primary_execution_timeframe() -> str:
    return EXECUTION_TIMEFRAMES[0] if EXECUTION_TIMEFRAMES else ""


def is_study_timeframe(timeframe: str | None) -> bool:
    return str(timeframe or "").strip().upper() in STUDY_TIMEFRAMES


def study_display_candle_limit(timeframe: str | None, default: int = 150) -> int:
    normalized_timeframe = str(timeframe or "").strip().upper()
    limit = STUDY_DISPLAY_CANDLE_LIMITS.get(normalized_timeframe)
    if limit is not None:
        return limit
    return max(1, int(default))
