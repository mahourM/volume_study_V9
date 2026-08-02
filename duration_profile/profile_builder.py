from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


SIGNIFICANT_DURATION_RATIO = Decimal("0.60")


@dataclass(frozen=True)
class DurationProfileLevel:
    price: Decimal
    duration_ms: int
    candle_duration_ms: int

    @property
    def duration_fraction(self) -> float:
        if self.candle_duration_ms <= 0:
            return 0.0
        return self.duration_ms / self.candle_duration_ms

    @property
    def significant(self) -> bool:
        if self.candle_duration_ms <= 0:
            return False
        return Decimal(self.duration_ms) > Decimal(self.candle_duration_ms) * SIGNIFICANT_DURATION_RATIO

    def to_payload(self) -> dict[str, Any]:
        return {
            "price": float(self.price),
            "price_text": format(self.price, "f"),
            "duration_ms": int(self.duration_ms),
            "duration_fraction": self.duration_fraction,
            "significant": self.significant,
        }


@dataclass(frozen=True)
class DurationProfileRange:
    price_low: Decimal
    price_high: Decimal
    total_duration_ms: int
    max_duration_ms: int
    levels_count: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "price_low": float(self.price_low),
            "price_high": float(self.price_high),
            "price_low_text": format(self.price_low, "f"),
            "price_high_text": format(self.price_high, "f"),
            "total_duration_ms": int(self.total_duration_ms),
            "max_duration_ms": int(self.max_duration_ms),
            "levels_count": int(self.levels_count),
        }


@dataclass(frozen=True)
class DurationProfile:
    symbol: str
    timeframe: str
    candle_open_time_utc_ms: int
    candle_close_time_utc_ms: int
    candle_duration_ms: int
    price_step: Decimal
    levels: tuple[DurationProfileLevel, ...]
    significant_ranges: tuple[DurationProfileRange, ...]

    @property
    def max_duration_ms(self) -> int:
        if not self.levels:
            return 0
        return max(level.duration_ms for level in self.levels)

    def to_payload(self) -> dict[str, Any]:
        return {
            "type": "DURATION_PROFILE",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "candle_open_time_utc_ms": int(self.candle_open_time_utc_ms),
            "candle_close_time_utc_ms": int(self.candle_close_time_utc_ms),
            "candle_duration_ms": int(self.candle_duration_ms),
            "price_step": float(self.price_step),
            "price_step_text": format(self.price_step, "f"),
            "max_duration_ms": int(self.max_duration_ms),
            "levels": [level.to_payload() for level in self.levels],
            "significant_ranges": [item.to_payload() for item in self.significant_ranges],
        }


def build_duration_profile(
    *,
    symbol: str,
    timeframe: str,
    candle_open_time_utc_ms: int,
    candle_close_time_utc_ms: int,
    price_step: Decimal,
    price_events: list[tuple[int, Decimal]],
    fallback_open_price: Decimal | None = None,
) -> DurationProfile:
    open_time_ms = int(candle_open_time_utc_ms)
    close_time_ms = int(candle_close_time_utc_ms)
    close_boundary_ms = close_time_ms + 1
    candle_duration_ms = max(0, close_boundary_ms - open_time_ms)
    durations_by_price = _calculate_price_level_durations(
        open_time_ms=open_time_ms,
        close_boundary_ms=close_boundary_ms,
        price_events=price_events,
        fallback_open_price=fallback_open_price,
    )

    levels = tuple(
        DurationProfileLevel(
            price=price,
            duration_ms=duration_ms,
            candle_duration_ms=candle_duration_ms,
        )
        for price, duration_ms in sorted(durations_by_price.items(), key=lambda item: item[0])
        if duration_ms > 0
    )

    return DurationProfile(
        symbol=symbol,
        timeframe=timeframe,
        candle_open_time_utc_ms=open_time_ms,
        candle_close_time_utc_ms=close_time_ms,
        candle_duration_ms=candle_duration_ms,
        price_step=price_step,
        levels=levels,
        significant_ranges=_build_significant_ranges(levels, price_step),
    )


def _calculate_price_level_durations(
    *,
    open_time_ms: int,
    close_boundary_ms: int,
    price_events: list[tuple[int, Decimal]],
    fallback_open_price: Decimal | None,
) -> dict[Decimal, int]:
    durations_by_price: dict[Decimal, int] = {}
    active_price: Decimal | None = None
    active_since_ms: int | None = None

    for event_time_ms, price in sorted(price_events, key=lambda item: item[0]):
        event_time_ms = int(event_time_ms)
        if event_time_ms < open_time_ms:
            active_price = price
            active_since_ms = open_time_ms
            continue

        if event_time_ms >= close_boundary_ms:
            break

        if active_price is None or active_since_ms is None:
            active_price = price
            active_since_ms = event_time_ms
            continue

        if event_time_ms < active_since_ms:
            continue

        if price == active_price:
            continue

        elapsed_ms = event_time_ms - active_since_ms
        if elapsed_ms > 0:
            durations_by_price[active_price] = durations_by_price.get(active_price, 0) + elapsed_ms

        active_price = price
        active_since_ms = event_time_ms

    if active_price is None and fallback_open_price is not None:
        active_price = fallback_open_price
        active_since_ms = open_time_ms

    if active_price is not None and active_since_ms is not None:
        elapsed_ms = close_boundary_ms - active_since_ms
        if elapsed_ms > 0:
            durations_by_price[active_price] = durations_by_price.get(active_price, 0) + elapsed_ms

    return durations_by_price


def _build_significant_ranges(
    levels: tuple[DurationProfileLevel, ...],
    price_step: Decimal,
) -> tuple[DurationProfileRange, ...]:
    significant_levels = [level for level in levels if level.significant]
    if not significant_levels:
        return ()

    ranges: list[DurationProfileRange] = []
    current_low = significant_levels[0].price
    current_high = significant_levels[0].price
    current_total = significant_levels[0].duration_ms
    current_max = significant_levels[0].duration_ms
    current_count = 1

    for level in significant_levels[1:]:
        is_adjacent = price_step > 0 and level.price <= current_high + price_step
        if is_adjacent:
            current_high = level.price
            current_total += level.duration_ms
            current_max = max(current_max, level.duration_ms)
            current_count += 1
            continue

        ranges.append(
            DurationProfileRange(
                price_low=current_low,
                price_high=current_high,
                total_duration_ms=current_total,
                max_duration_ms=current_max,
                levels_count=current_count,
            )
        )
        current_low = level.price
        current_high = level.price
        current_total = level.duration_ms
        current_max = level.duration_ms
        current_count = 1

    ranges.append(
        DurationProfileRange(
            price_low=current_low,
            price_high=current_high,
            total_duration_ms=current_total,
            max_duration_ms=current_max,
            levels_count=current_count,
        )
    )
    return tuple(ranges)
