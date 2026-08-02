from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP
from statistics import median

from absorption.models import BinanceCandle


ALPHA_BY_INTERVAL: dict[str, Decimal] = {
    "30s": Decimal("0.24"),
    "1m": Decimal("0.22"),
    "3m": Decimal("0.21"),
    "5m": Decimal("0.20"),
    "15m": Decimal("0.18"),
    "30m": Decimal("0.17"),
    "1h": Decimal("0.16"),
    "2h": Decimal("0.15"),
    "6h": Decimal("0.135"),
    "8h": Decimal("0.13"),
    "12h": Decimal("0.125"),
}


class AdaptiveBinSizer:
    def __init__(self, lookback_candles: int) -> None:
        if lookback_candles <= 0:
            raise ValueError("lookback_candles must be positive")
        self.lookback_candles = lookback_candles

    def calculate(self, interval: str, closed_candles: list[BinanceCandle], price_step: Decimal) -> Decimal:
        alpha = ALPHA_BY_INTERVAL.get(interval)
        if alpha is None:
            raise ValueError(f"Unsupported interval for Formula 1: {interval}")
        if price_step <= 0:
            raise ValueError("price_step must be positive")

        lookback_window = closed_candles[-self.lookback_candles :]
        ranges = [item.range_size for item in lookback_window if item.range_size > 0]
        if not ranges:
            return price_step

        median_range = Decimal(str(median(ranges)))
        raw_bin_size = alpha * median_range
        step_count = (raw_bin_size / price_step).to_integral_value(rounding=ROUND_HALF_UP)
        if step_count < 1:
            step_count = Decimal("1")
        return step_count * price_step
