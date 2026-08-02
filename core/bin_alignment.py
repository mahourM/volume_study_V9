from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from statistics import median
from typing import Iterable, Mapping, Sequence


DecimalLike = Decimal | int | float | str


def to_decimal(value: DecimalLike) -> Decimal:
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def require_positive(value: DecimalLike, name: str) -> Decimal:
    decimal_value = to_decimal(value)
    if decimal_value <= 0:
        raise ValueError(f"{name} must be positive")
    return decimal_value


@dataclass(frozen=True)
class ExchangeMetadata:
    symbol: str
    tick_size: Decimal
    step_size: Decimal

    @classmethod
    def from_values(
        cls,
        *,
        symbol: str,
        tick_size: DecimalLike,
        step_size: DecimalLike,
    ) -> ExchangeMetadata:
        return cls(
            symbol=symbol,
            tick_size=require_positive(tick_size, "tick_size"),
            step_size=require_positive(step_size, "step_size"),
        )


@dataclass(frozen=True)
class FormulaOneSettings:
    alpha_by_timeframe: Mapping[str, Decimal]
    minimum_bin_steps: int
    rounding: str

    def alpha_for(self, timeframe: str) -> Decimal:
        alpha = self.alpha_by_timeframe.get(timeframe)
        if alpha is None:
            raise ValueError(f"Missing Formula 1 alpha for timeframe {timeframe!r}")
        return require_positive(alpha, "Formula 1 alpha")


def compute_fixed_bin_size(
    *,
    timeframe: str,
    candle_ranges: Sequence[DecimalLike],
    tick_size: DecimalLike,
    settings: FormulaOneSettings,
) -> Decimal:
    tick = require_positive(tick_size, "tick_size")
    if settings.minimum_bin_steps <= 0:
        raise ValueError("minimum_bin_steps must be positive")

    ranges = [to_decimal(item) for item in candle_ranges if to_decimal(item) > 0]
    if not ranges:
        return tick * Decimal(settings.minimum_bin_steps)

    median_range = Decimal(str(median(ranges)))
    raw_bin_size = settings.alpha_for(timeframe) * median_range
    step_count = (raw_bin_size / tick).to_integral_value(rounding=settings.rounding)
    minimum_steps = Decimal(settings.minimum_bin_steps)
    if step_count < minimum_steps:
        step_count = minimum_steps
    return step_count * tick


def bin_index(price: DecimalLike, fixed_bin_size: DecimalLike) -> int:
    bin_size = require_positive(fixed_bin_size, "fixed_bin_size")
    return int((to_decimal(price) / bin_size).to_integral_value(rounding=ROUND_FLOOR))


def bin_bounds(index: int, fixed_bin_size: DecimalLike) -> tuple[Decimal, Decimal]:
    bin_size = require_positive(fixed_bin_size, "fixed_bin_size")
    low = Decimal(index) * bin_size
    return low, low + bin_size


def bin_indices_for_candle(
    *,
    body_low: DecimalLike | None,
    body_high: DecimalLike | None,
    extra_prices: Iterable[DecimalLike],
    fixed_bin_size: DecimalLike,
) -> range:
    candidates: list[Decimal] = []
    if body_low is not None:
        candidates.append(to_decimal(body_low))
    if body_high is not None:
        candidates.append(to_decimal(body_high))
    candidates.extend(to_decimal(price) for price in extra_prices)
    if not candidates:
        return range(0, 0)

    start_index = bin_index(min(candidates), fixed_bin_size)
    end_index = bin_index(max(candidates), fixed_bin_size)
    return range(start_index, end_index + 1)


def tick_precision(tick_size: DecimalLike) -> int:
    tick = require_positive(tick_size, "tick_size").normalize()
    return max(0, -tick.as_tuple().exponent)


def format_price_for_tick(price: DecimalLike, tick_size: DecimalLike) -> str:
    precision = tick_precision(tick_size)
    return f"{to_decimal(price):.{precision}f}"
