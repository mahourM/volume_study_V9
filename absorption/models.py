from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class BinanceCandle:
    symbol: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal

    @property
    def range_size(self) -> Decimal:
        return self.high_price - self.low_price


@dataclass
class FootprintBin:
    bin_low: Decimal
    bin_high: Decimal
<<<<<<< HEAD
    bin_index: int | None = None
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")
    duration_ms: int = 0
    horizontal_delta: Decimal = Decimal("0")
    ask_traded_volume: Decimal = Decimal("0")
    bid_traded_volume: Decimal = Decimal("0")
    buy_diagonal_imbalance_ratio: Decimal = Decimal("0")
    sell_diagonal_imbalance_ratio: Decimal = Decimal("0")
    min_trade_price_in_bin: Decimal | None = None
    max_trade_price_in_bin: Decimal | None = None
    price_progress_in_bin: Decimal | None = None
    dominant_diagonal_side: str = "NONE"
    dominant_side_volume: Decimal = Decimal("0")
    dominant_side_efficiency: Decimal | None = None
<<<<<<< HEAD
    contract_spike_score: Decimal = Decimal("0")
    abnormal_contract: bool = False
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    volume_percentile: Decimal | None = None
    is_volume_valid: bool = True
    efficiency_percentile: Decimal | None = None
    efficiency_zscore: Decimal | None = None
    rejection_reason: str = ""

    @property
    def total_volume(self) -> Decimal:
        return self.buy_volume + self.sell_volume

    @property
    def delta(self) -> Decimal:
        return self.buy_volume - self.sell_volume

    def to_payload(self) -> dict[str, Any]:
<<<<<<< HEAD
        payload = {
            "index": self.bin_index,
            "price": str(self.bin_low),
=======
        return {
            "bin_low": str(self.bin_low),
            "bin_high": str(self.bin_high),
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "buy_volume": str(self.buy_volume),
            "sell_volume": str(self.sell_volume),
            "total_volume": str(self.total_volume),
            "delta": str(self.delta),
            "horizontal_delta": str(self.horizontal_delta),
            "ask_traded_volume": str(self.ask_traded_volume),
            "bid_traded_volume": str(self.bid_traded_volume),
            "buy_diagonal_imbalance_ratio": str(self.buy_diagonal_imbalance_ratio),
            "sell_diagonal_imbalance_ratio": str(self.sell_diagonal_imbalance_ratio),
            "duration_ms": self.duration_ms,
            "min_trade_price_in_bin": (
                str(self.min_trade_price_in_bin)
                if self.min_trade_price_in_bin is not None
                else None
            ),
            "max_trade_price_in_bin": (
                str(self.max_trade_price_in_bin)
                if self.max_trade_price_in_bin is not None
                else None
            ),
            "price_progress_in_bin": (
                str(self.price_progress_in_bin)
                if self.price_progress_in_bin is not None
                else None
            ),
            "dominant_diagonal_side": self.dominant_diagonal_side,
            "dominant_side_volume": str(self.dominant_side_volume),
            "dominant_side_efficiency": (
                str(self.dominant_side_efficiency)
                if self.dominant_side_efficiency is not None
                else None
            ),
<<<<<<< HEAD
            "contract_spike_score": str(self.contract_spike_score),
            "abnormal_contract": self.abnormal_contract,
            "abnormal_volume": self.abnormal_contract,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "volume_percentile": (
                str(self.volume_percentile)
                if self.volume_percentile is not None
                else None
            ),
            "is_volume_valid": self.is_volume_valid,
            "efficiency_percentile": (
                str(self.efficiency_percentile)
                if self.efficiency_percentile is not None
                else None
            ),
            "efficiency_zscore": (
                str(self.efficiency_zscore)
                if self.efficiency_zscore is not None
                else None
            ),
            "rejection_reason": self.rejection_reason,
        }
<<<<<<< HEAD
        if self.bin_index is None:
            payload["bin_low"] = str(self.bin_low)
            payload["bin_high"] = str(self.bin_high)
        return payload
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


@dataclass(frozen=True)
class HvnResult:
    hvn_bins: tuple[FootprintBin, ...]
    core_bin: FootprintBin | None
    side_bins: tuple[FootprintBin, ...]
    full_range: tuple[Decimal, Decimal] | None

    def to_payload(self) -> dict[str, Any]:
        return {
            "hvn_bins": [item.to_payload() for item in self.hvn_bins],
            "core_bin": self.core_bin.to_payload() if self.core_bin else None,
            "side_bins": [item.to_payload() for item in self.side_bins],
            "full_range": (
                {"low": str(self.full_range[0]), "high": str(self.full_range[1])}
                if self.full_range
                else None
            ),
        }


@dataclass(frozen=True)
class CandleFootprint:
    symbol: str
    mt5_symbol: str
    timeframe: str
    interval: str
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    bin_size: Decimal
    bins: tuple[FootprintBin, ...]
    hvn_result: HvnResult
<<<<<<< HEAD
    price_step: Decimal | None = None
    contract_spike_score_deviation: Decimal | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = {
=======

    def to_payload(self) -> dict[str, Any]:
        return {
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "symbol": self.symbol,
            "mt5_symbol": self.mt5_symbol,
            "timeframe": self.timeframe,
            "interval": self.interval,
            "open_time_ms": self.open_time_ms,
            "close_time_ms": self.close_time_ms,
            "open_price": str(self.open_price),
            "high_price": str(self.high_price),
            "low_price": str(self.low_price),
            "close_price": str(self.close_price),
            "bin_size": str(self.bin_size),
            "bins": [item.to_payload() for item in self.bins],
            "hvn": self.hvn_result.to_payload(),
        }
<<<<<<< HEAD
        if self.price_step is not None:
            payload["price_step"] = str(self.price_step)
        if self.contract_spike_score_deviation is not None:
            payload["contract_spike_score_deviation"] = str(
                self.contract_spike_score_deviation
            )
        return payload
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
