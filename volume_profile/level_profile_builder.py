from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class LevelVolumeProfileLevel:
    price: Decimal
    agg_buy_volume: Decimal
    agg_sell_volume: Decimal
    max_total_volume: Decimal

    @property
    def total_volume(self) -> Decimal:
        return self.agg_buy_volume + self.agg_sell_volume

    @property
    def delta_volume(self) -> Decimal:
        return self.agg_buy_volume - self.agg_sell_volume

    @property
    def volume_fraction(self) -> float:
        if self.max_total_volume <= 0:
            return 0.0
        return float(self.total_volume / self.max_total_volume)

    def to_payload(self) -> dict[str, Any]:
        return {
            "p": float(self.price),
            "b": float(self.agg_buy_volume),
            "s": float(self.agg_sell_volume),
        }


@dataclass(frozen=True)
class LevelVolumeProfile:
    symbol: str
    timeframe: str
    candle_open_time_utc_ms: int
    candle_close_time_utc_ms: int
    price_step: Decimal
    levels: tuple[LevelVolumeProfileLevel, ...]

    @property
    def max_total_volume(self) -> Decimal:
        if not self.levels:
            return Decimal("0")
        return max(level.total_volume for level in self.levels)

    def to_payload(self) -> dict[str, Any]:
        return {
            "type": "LEVEL_VOLUME_PROFILE",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "candle_open_time_utc_ms": int(self.candle_open_time_utc_ms),
            "candle_close_time_utc_ms": int(self.candle_close_time_utc_ms),
            "price_step": float(self.price_step),
            "max_total_volume": float(self.max_total_volume),
            "levels_count": len(self.levels),
            "levels": [level.to_payload() for level in self.levels],
        }


def build_level_volume_profile(
    *,
    symbol: str,
    timeframe: str,
    candle_open_time_utc_ms: int,
    candle_close_time_utc_ms: int,
    price_step: Decimal,
    agg_trades: list[Any],
) -> LevelVolumeProfile:
    buy_volume_by_price: dict[Decimal, Decimal] = {}
    sell_volume_by_price: dict[Decimal, Decimal] = {}

    for trade in agg_trades:
        price = Decimal(str(trade.price))
        quantity = Decimal(str(trade.quantity))
        if quantity <= 0:
            continue

        if trade.side == "sell":
            sell_volume_by_price[price] = sell_volume_by_price.get(price, Decimal("0")) + quantity
        elif trade.side == "buy":
            buy_volume_by_price[price] = buy_volume_by_price.get(price, Decimal("0")) + quantity

    prices = sorted(set(buy_volume_by_price) | set(sell_volume_by_price))
    max_total_volume = max(
        (
            buy_volume_by_price.get(price, Decimal("0"))
            + sell_volume_by_price.get(price, Decimal("0"))
            for price in prices
        ),
        default=Decimal("0"),
    )

    levels = tuple(
        LevelVolumeProfileLevel(
            price=price,
            agg_buy_volume=buy_volume_by_price.get(price, Decimal("0")),
            agg_sell_volume=sell_volume_by_price.get(price, Decimal("0")),
            max_total_volume=max_total_volume,
        )
        for price in prices
        if buy_volume_by_price.get(price, Decimal("0")) + sell_volume_by_price.get(price, Decimal("0")) > 0
    )

    return LevelVolumeProfile(
        symbol=symbol,
        timeframe=timeframe,
        candle_open_time_utc_ms=int(candle_open_time_utc_ms),
        candle_close_time_utc_ms=int(candle_close_time_utc_ms),
        price_step=price_step,
        levels=levels,
    )
