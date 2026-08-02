from __future__ import annotations

from dataclasses import dataclass, replace
from collections.abc import Mapping
from decimal import Decimal
from typing import Literal

from core.bin_alignment import DecimalLike, bin_index, to_decimal


TradeSide = Literal["buy", "sell"]
DOMINANT_SIDE_BUY = "BUY"
DOMINANT_SIDE_SELL = "SELL"
DOMINANT_SIDE_NONE = "NONE"
PRICE_EFFICIENCY_EPSILON = Decimal("0.000001")


@dataclass(frozen=True)
class TradeEvent:
    symbol: str
    event_time_ms: int
    price: Decimal
    quantity: Decimal
    side: TradeSide

    @classmethod
    def from_values(
        cls,
        *,
        symbol: str,
        event_time_ms: int,
        price: DecimalLike,
        quantity: DecimalLike,
        side: TradeSide,
    ) -> TradeEvent:
        qty = to_decimal(quantity)
        if qty < 0:
            raise ValueError("trade quantity must be non-negative")
        return cls(
            symbol=symbol,
            event_time_ms=int(event_time_ms),
            price=to_decimal(price),
            quantity=qty,
            side=side,
        )


@dataclass
class L2BinState:
    total_volume: Decimal = Decimal("0")
    delta: Decimal = Decimal("0")
    horizontal_delta: Decimal = Decimal("0")
    ask_traded_volume: Decimal = Decimal("0")
    bid_traded_volume: Decimal = Decimal("0")
    buy_diagonal_imbalance_ratio: Decimal = Decimal("0")
    sell_diagonal_imbalance_ratio: Decimal = Decimal("0")
    duration_ms: int = 0
    first_price: Decimal | None = None
    last_price: Decimal | None = None
    min_trade_price_in_bin: Decimal | None = None
    max_trade_price_in_bin: Decimal | None = None

    def apply_trade(self, event: TradeEvent) -> None:
        if self.first_price is None:
            self.first_price = event.price
        self.last_price = event.price
        self.min_trade_price_in_bin = (
            event.price
            if self.min_trade_price_in_bin is None
            else min(self.min_trade_price_in_bin, event.price)
        )
        self.max_trade_price_in_bin = (
            event.price
            if self.max_trade_price_in_bin is None
            else max(self.max_trade_price_in_bin, event.price)
        )
        self.total_volume += event.quantity
        if event.side == "buy":
            self.delta += event.quantity
            self.horizontal_delta += event.quantity
            self.ask_traded_volume += event.quantity
        elif event.side == "sell":
            self.delta -= event.quantity
            self.horizontal_delta -= event.quantity
            self.bid_traded_volume += event.quantity
        else:
            raise ValueError(f"Unsupported trade side {event.side!r}")

    @property
    def price_progress_in_bin(self) -> Decimal | None:
        return price_progress_in_bin(self)

    @property
    def dominant_diagonal_side(self) -> str:
        return dominant_diagonal_side(self)

    @property
    def dominant_side_volume(self) -> Decimal:
        return dominant_side_volume(self)

    @property
    def dominant_side_efficiency(self) -> Decimal | None:
        return dominant_side_efficiency(self)


@dataclass
class PriceDwellTracker:
    active_bin_index: int | None = None
    active_since_ms: int | None = None

    def observe(
        self,
        *,
        price: DecimalLike,
        event_time_ms: int,
        fixed_bin_size: DecimalLike,
        durations_by_index: dict[int, int],
    ) -> int:
        next_index = bin_index(price, fixed_bin_size)
        event_time = int(event_time_ms)
        if self.active_bin_index is None:
            self.active_bin_index = next_index
            self.active_since_ms = event_time
            return next_index

        if self.active_since_ms is None or event_time < self.active_since_ms:
            return self.active_bin_index

        if next_index != self.active_bin_index:
            elapsed_ms = event_time - self.active_since_ms
            if elapsed_ms > 0:
                durations_by_index[self.active_bin_index] = (
                    durations_by_index.get(self.active_bin_index, 0) + elapsed_ms
                )
            self.active_bin_index = next_index
            self.active_since_ms = event_time

        return self.active_bin_index

    def finalize(self, *, close_time_ms: int, durations_by_index: dict[int, int]) -> None:
        if self.active_bin_index is None or self.active_since_ms is None:
            return
        elapsed_ms = int(close_time_ms) - self.active_since_ms
        if elapsed_ms > 0:
            durations_by_index[self.active_bin_index] = (
                durations_by_index.get(self.active_bin_index, 0) + elapsed_ms
            )
        self.active_bin_index = None
        self.active_since_ms = None


def apply_trade(
    bins_by_index: dict[int, L2BinState],
    dwell_tracker: PriceDwellTracker,
    event: TradeEvent,
    *,
    fixed_bin_size: DecimalLike,
    durations_by_index: dict[int, int],
) -> int:
    dwell_tracker.observe(
        price=event.price,
        event_time_ms=event.event_time_ms,
        fixed_bin_size=fixed_bin_size,
        durations_by_index=durations_by_index,
    )
    trade_index = bin_index(event.price, fixed_bin_size)
    bin_state = bins_by_index.setdefault(trade_index, L2BinState())
    bin_state.apply_trade(event)
    return trade_index


def calculate_diagonal_imbalance_ratios(bins_by_index: dict[int, L2BinState]) -> None:
    for index in sorted(bins_by_index):
        current = bins_by_index[index]
        higher = bins_by_index.get(index + 1)
        lower = bins_by_index.get(index - 1)
<<<<<<< HEAD
        lower_bid_volume = lower.bid_traded_volume if lower is not None else Decimal("0")
        higher_ask_volume = higher.ask_traded_volume if higher is not None else Decimal("0")
        current.buy_diagonal_imbalance_ratio = _safe_ratio(
            current.ask_traded_volume,
            lower_bid_volume,
        )
        current.sell_diagonal_imbalance_ratio = _safe_ratio(
            current.bid_traded_volume,
            higher_ask_volume,
=======
        higher_bid_volume = higher.bid_traded_volume if higher is not None else Decimal("0")
        lower_ask_volume = lower.ask_traded_volume if lower is not None else Decimal("0")
        current.buy_diagonal_imbalance_ratio = _safe_ratio(
            current.ask_traded_volume,
            higher_bid_volume,
        )
        current.sell_diagonal_imbalance_ratio = _safe_ratio(
            current.bid_traded_volume,
            lower_ask_volume,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        )


def _safe_ratio(numerator: Decimal, denominator: Decimal) -> Decimal:
<<<<<<< HEAD
    return numerator / max(denominator, Decimal("1"))
=======
    if denominator <= 0:
        return Decimal("0")
    return numerator / denominator
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


def price_progress_in_bin(state: object) -> Decimal | None:
    min_price = getattr(state, "min_trade_price_in_bin", None)
    max_price = getattr(state, "max_trade_price_in_bin", None)
    if min_price is None or max_price is None:
        return None
    return Decimal(str(max_price)) - Decimal(str(min_price))


def dominant_diagonal_side(state: object) -> str:
    buy_ratio = Decimal(str(getattr(state, "buy_diagonal_imbalance_ratio", Decimal("0")) or Decimal("0")))
    sell_ratio = Decimal(str(getattr(state, "sell_diagonal_imbalance_ratio", Decimal("0")) or Decimal("0")))
    if buy_ratio > sell_ratio:
        return DOMINANT_SIDE_BUY
    if sell_ratio > buy_ratio:
        return DOMINANT_SIDE_SELL
    return DOMINANT_SIDE_NONE


def dominant_side_volume(state: object) -> Decimal:
    side = dominant_diagonal_side(state)
    if side == DOMINANT_SIDE_BUY:
        return Decimal(str(getattr(state, "ask_traded_volume", Decimal("0")) or Decimal("0")))
    if side == DOMINANT_SIDE_SELL:
        return Decimal(str(getattr(state, "bid_traded_volume", Decimal("0")) or Decimal("0")))
    return Decimal("0")


def dominant_side_efficiency(state: object, epsilon: Decimal = PRICE_EFFICIENCY_EPSILON) -> Decimal | None:
    progress = price_progress_in_bin(state)
    if progress is None:
        return None
<<<<<<< HEAD
    total_volume = Decimal(str(getattr(state, "total_volume", Decimal("0")) or Decimal("0")))
    if total_volume <= 0:
        return None
    denominator = max(total_volume, epsilon)
=======
    side = dominant_diagonal_side(state)
    if side == DOMINANT_SIDE_NONE:
        return None
    volume = dominant_side_volume(state)
    if volume <= 0:
        return None
    denominator = max(volume, epsilon)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    return progress / denominator


def copy_l2_bins_with_diagonal_imbalance_ratios(
    bins_by_index: Mapping[int, L2BinState],
) -> dict[int, L2BinState]:
    copied = {index: replace(state) for index, state in bins_by_index.items()}
    calculate_diagonal_imbalance_ratios(copied)
    return copied
