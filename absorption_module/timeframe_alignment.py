from __future__ import annotations

from typing import Sequence

from .absorption_cluster_model import BinMarketData, CandleAbsorptionResult, RollingCandleBuffer
from .absorption_memory import AbsorptionMemoryState


def update_rolling_buffer(
    memory: AbsorptionMemoryState,
    symbol: str,
    timeframe_name: str,
    max_candles: int,
    candle_result: CandleAbsorptionResult,
    candle_bins: Sequence[BinMarketData],
) -> None:
    key = (symbol, timeframe_name)
    buffer = memory.rolling_buffers.get(key)
    if buffer is None:
        buffer = RollingCandleBuffer(symbol=symbol, timeframe_name=timeframe_name, max_candles=max_candles)
        memory.rolling_buffers[key] = buffer
    buffer.candle_results.append(candle_result)
    buffer.candle_bins[candle_result.candle_open_time_utc_ms] = tuple(candle_bins)
    while len(buffer.candle_results) > buffer.max_candles:
        removed = buffer.candle_results.pop(0)
        buffer.candle_bins.pop(removed.candle_open_time_utc_ms, None)
