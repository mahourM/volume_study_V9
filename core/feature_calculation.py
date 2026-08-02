from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from core.bin_alignment import (
    DecimalLike,
    bin_bounds,
    format_price_for_tick,
    to_decimal,
)
from core.trade_mapping import (
    L2BinState,
    dominant_diagonal_side as calculate_dominant_diagonal_side,
    dominant_side_efficiency as calculate_dominant_side_efficiency,
    dominant_side_volume as calculate_dominant_side_volume,
    price_progress_in_bin as calculate_price_progress_in_bin,
)


@dataclass(frozen=True)
class OutputPrecision:
    decimal_places: int
    duration_unit_ms: int

    def format_decimal(self, value: DecimalLike) -> str:
        if self.decimal_places < 0:
            raise ValueError("decimal_places must be non-negative")
        return f"{to_decimal(value):.{self.decimal_places}f}"


@dataclass(frozen=True)
class L2Feature:
    total_volume: Decimal
    delta: Decimal
    horizontal_delta: Decimal
    ask_traded_volume: Decimal
    bid_traded_volume: Decimal
    buy_diagonal_imbalance_ratio: Decimal
    sell_diagonal_imbalance_ratio: Decimal
    duration_ms: int
    min_trade_price_in_bin: Decimal | None = None
    max_trade_price_in_bin: Decimal | None = None
    price_progress_in_bin: Decimal | None = None
    dominant_diagonal_side: str = "NONE"
    dominant_side_volume: Decimal = Decimal("0")
    dominant_side_efficiency: Decimal | None = None

    def to_payload(self, precision: OutputPrecision) -> dict[str, Any]:
        if precision.duration_unit_ms <= 0:
            raise ValueError("duration_unit_ms must be positive")
        duration = Decimal(self.duration_ms) / Decimal(precision.duration_unit_ms)
        return {
            "total_volume": precision.format_decimal(self.total_volume),
            "delta": precision.format_decimal(self.delta),
            "horizontal_delta": precision.format_decimal(self.horizontal_delta),
            "ask_traded_volume": precision.format_decimal(self.ask_traded_volume),
            "bid_traded_volume": precision.format_decimal(self.bid_traded_volume),
            "buy_diagonal_imbalance_ratio": precision.format_decimal(
                self.buy_diagonal_imbalance_ratio
            ),
            "sell_diagonal_imbalance_ratio": precision.format_decimal(
                self.sell_diagonal_imbalance_ratio
            ),
            "duration": precision.format_decimal(duration),
            "min_trade_price_in_bin": _format_optional_decimal(
                self.min_trade_price_in_bin,
                precision,
            ),
            "max_trade_price_in_bin": _format_optional_decimal(
                self.max_trade_price_in_bin,
                precision,
            ),
            "price_progress_in_bin": _format_optional_decimal(
                self.price_progress_in_bin,
                precision,
            ),
            "dominant_diagonal_side": self.dominant_diagonal_side,
            "dominant_side_volume": precision.format_decimal(self.dominant_side_volume),
            "dominant_side_efficiency": _format_optional_decimal(
                self.dominant_side_efficiency,
                precision,
            ),
        }


@dataclass(frozen=True)
class BinFeature:
    bin_id: str
    index: int
    low: Decimal
    high: Decimal
    l2: L2Feature

    def to_payload(self, precision: OutputPrecision) -> dict[str, Any]:
        return {
            "bin_id": self.bin_id,
            "index": self.index,
            "low": precision.format_decimal(self.low),
            "high": precision.format_decimal(self.high),
            "l2": self.l2.to_payload(precision),
        }


def calculate_l2_feature(l2_state: L2BinState | None, duration_ms: int) -> L2Feature:
    if l2_state is None:
        return L2Feature(
            total_volume=Decimal("0"),
            delta=Decimal("0"),
            horizontal_delta=Decimal("0"),
            ask_traded_volume=Decimal("0"),
            bid_traded_volume=Decimal("0"),
            buy_diagonal_imbalance_ratio=Decimal("0"),
            sell_diagonal_imbalance_ratio=Decimal("0"),
            duration_ms=duration_ms,
        )
    return L2Feature(
        total_volume=l2_state.total_volume,
        delta=l2_state.delta,
        horizontal_delta=getattr(l2_state, "horizontal_delta", l2_state.delta),
        ask_traded_volume=getattr(l2_state, "ask_traded_volume", Decimal("0")),
        bid_traded_volume=getattr(l2_state, "bid_traded_volume", Decimal("0")),
        buy_diagonal_imbalance_ratio=getattr(
            l2_state,
            "buy_diagonal_imbalance_ratio",
            Decimal("0"),
        ),
        sell_diagonal_imbalance_ratio=getattr(
            l2_state,
            "sell_diagonal_imbalance_ratio",
            Decimal("0"),
        ),
        duration_ms=duration_ms,
        min_trade_price_in_bin=getattr(l2_state, "min_trade_price_in_bin", None),
        max_trade_price_in_bin=getattr(l2_state, "max_trade_price_in_bin", None),
        price_progress_in_bin=calculate_price_progress_in_bin(l2_state),
        dominant_diagonal_side=calculate_dominant_diagonal_side(l2_state),
        dominant_side_volume=calculate_dominant_side_volume(l2_state),
        dominant_side_efficiency=calculate_dominant_side_efficiency(l2_state),
    )


def _format_optional_decimal(value: Decimal | None, precision: OutputPrecision) -> str | None:
    if value is None:
        return None
    return precision.format_decimal(value)


def build_bin_feature(
    *,
    candle_number: int,
    index: int,
    fixed_bin_size: DecimalLike,
    tick_size: DecimalLike,
    l2_state: L2BinState | None,
    duration_ms: int,
) -> BinFeature:
    low, high = bin_bounds(index, fixed_bin_size)
    formatted_low = format_price_for_tick(low, tick_size)
    return BinFeature(
        bin_id=f"{candle_number}_{formatted_low}",
        index=index,
        low=low,
        high=high,
        l2=calculate_l2_feature(l2_state, duration_ms),
    )
