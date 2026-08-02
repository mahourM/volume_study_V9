from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping, Sequence

from core.bin_alignment import bin_bounds, bin_index, require_positive, to_decimal


@dataclass(frozen=True)
class BinVolume:
    bin_index: int
    price_low: Decimal
    price_high: Decimal
    buy_volume: Decimal = Decimal("0")
    sell_volume: Decimal = Decimal("0")

    @property
    def total_volume(self) -> Decimal:
        return self.buy_volume + self.sell_volume

    @property
    def delta_volume(self) -> Decimal:
        return self.buy_volume - self.sell_volume


@dataclass(frozen=True)
class CandleBinVolumeProfile:
    symbol: str
    timeframe: str
    candle_open_time_utc_ms: int
    candle_close_time_utc_ms: int
    fixed_bin_size: Decimal
    bins_by_index: Mapping[int, BinVolume]


@dataclass(frozen=True)
class BaselineBinVolume:
    bin_index: int
    price_low: Decimal
    price_high: Decimal
    buy_volume: Decimal
    sell_volume: Decimal

    @property
    def total_volume(self) -> Decimal:
        return self.buy_volume + self.sell_volume

    @property
    def delta_volume(self) -> Decimal:
        return self.buy_volume - self.sell_volume

    def to_payload(self) -> dict[str, Any]:
        return {
            "bin_low": float(self.price_low),
            "bin_high": float(self.price_high),
            "baseline_buy_volume": float(self.buy_volume),
            "baseline_sell_volume": float(self.sell_volume),
            "baseline_total_volume": float(self.total_volume),
            "baseline_delta_volume": float(self.delta_volume),
        }


@dataclass(frozen=True)
class VolumeZScoreProfileBin:
    bin_index: int
    price_low: Decimal
    price_high: Decimal
    current_volume: Decimal
    current_buy_volume: Decimal
    current_sell_volume: Decimal
    current_delta_volume: Decimal
    baseline_count: int
    baseline_median_volume: Decimal
    baseline_mad_volume: Decimal
    effective_mad_volume: Decimal
    volume_z_score: Decimal
    positive_volume_z_score: Decimal
    z_cap: Decimal
    line_width_ratio: float

    def to_payload(self) -> dict[str, Any]:
        return {
            "bin_low": float(self.price_low),
            "bin_high": float(self.price_high),
            "current_volume": float(self.current_volume),
            "current_buy_volume": float(self.current_buy_volume),
            "current_sell_volume": float(self.current_sell_volume),
            "current_delta_volume": float(self.current_delta_volume),
            "baseline_count": int(self.baseline_count),
            "baseline_median_volume": float(self.baseline_median_volume),
            "baseline_mad_volume": float(self.baseline_mad_volume),
            "effective_mad_volume": float(self.effective_mad_volume),
            "volume_z_score": float(self.volume_z_score),
            "positive_volume_z_score": float(self.positive_volume_z_score),
            "z_cap": float(self.z_cap),
            "line_width_ratio": float(self.line_width_ratio),
        }


@dataclass(frozen=True)
class VolumeZScoreProfile:
    symbol: str
    timeframe: str
    candle_open_time_utc_ms: int
    candle_close_time_utc_ms: int
    fixed_bin_size: Decimal
    baseline_count: int
    target_baseline_candles: int
    volume_floor: Decimal
    z_cap: Decimal
    bins: tuple[VolumeZScoreProfileBin, ...]
    baseline_bins: tuple[BaselineBinVolume, ...]

    @property
    def max_positive_volume_z_score(self) -> Decimal:
        if not self.bins:
            return Decimal("0")
        return max(item.positive_volume_z_score for item in self.bins)

    def to_payload(self) -> dict[str, Any]:
        return {
            "type": "VOLUME_ZSCORE_PROFILE",
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "candle_open_time_utc_ms": int(self.candle_open_time_utc_ms),
            "candle_close_time_utc_ms": int(self.candle_close_time_utc_ms),
            "fixed_bin_size": float(self.fixed_bin_size),
            "baseline_count": int(self.baseline_count),
            "target_baseline_candles": int(self.target_baseline_candles),
            "volume_floor": float(self.volume_floor),
            "z_cap": float(self.z_cap),
            "max_positive_volume_z_score": float(self.max_positive_volume_z_score),
            "bins_count": len(self.bins),
            "baseline_bins_count": len(self.baseline_bins),
            "bins": [item.to_payload() for item in self.bins],
        }


def build_candle_bin_volume_profile(
    *,
    symbol: str,
    timeframe: str,
    candle_open_time_utc_ms: int,
    candle_close_time_utc_ms: int,
    fixed_bin_size: Decimal,
    agg_trades: Sequence[Any],
) -> CandleBinVolumeProfile:
    bin_size = require_positive(fixed_bin_size, "fixed_bin_size")
    buy_volume_by_bin: dict[int, Decimal] = {}
    sell_volume_by_bin: dict[int, Decimal] = {}

    for trade in agg_trades:
        quantity = _trade_quantity(trade)
        if quantity <= 0:
            continue
        index = bin_index(_trade_price(trade), bin_size)
        side = _trade_side(trade)
        if side == "sell":
            sell_volume_by_bin[index] = sell_volume_by_bin.get(index, Decimal("0")) + quantity
        elif side == "buy":
            buy_volume_by_bin[index] = buy_volume_by_bin.get(index, Decimal("0")) + quantity

    bins_by_index: dict[int, BinVolume] = {}
    for index in sorted(set(buy_volume_by_bin) | set(sell_volume_by_bin)):
        price_low, price_high = bin_bounds(index, bin_size)
        bins_by_index[index] = BinVolume(
            bin_index=index,
            price_low=price_low,
            price_high=price_high,
            buy_volume=buy_volume_by_bin.get(index, Decimal("0")),
            sell_volume=sell_volume_by_bin.get(index, Decimal("0")),
        )

    return CandleBinVolumeProfile(
        symbol=symbol,
        timeframe=timeframe,
        candle_open_time_utc_ms=int(candle_open_time_utc_ms),
        candle_close_time_utc_ms=int(candle_close_time_utc_ms),
        fixed_bin_size=bin_size,
        bins_by_index=bins_by_index,
    )


def build_volume_zscore_profile(
    *,
    current_profile: CandleBinVolumeProfile,
    baseline_profiles: Sequence[CandleBinVolumeProfile],
    target_baseline_candles: int,
    volume_floor: Decimal,
    z_cap: Decimal,
) -> VolumeZScoreProfile:
    floor = max(to_decimal(volume_floor), Decimal("0"))
    cap = require_positive(z_cap, "z_cap")
    baseline_count = len(baseline_profiles)
    baseline_bins = _aggregate_baseline_bins(
        baseline_profiles=baseline_profiles,
        fixed_bin_size=current_profile.fixed_bin_size,
    )
    zscore_bins: list[VolumeZScoreProfileBin] = []

    for index in sorted(current_profile.bins_by_index):
        current_bin = current_profile.bins_by_index[index]
        if current_bin.total_volume <= 0:
            continue

        baseline_values = [
            profile.bins_by_index.get(index).total_volume
            if profile.bins_by_index.get(index) is not None
            else Decimal("0")
            for profile in baseline_profiles
        ]
        median_volume = _median_decimal(baseline_values)
        mad_volume = _median_decimal([abs(value - median_volume) for value in baseline_values])
        effective_mad = max(mad_volume, floor)
        if effective_mad <= 0:
            volume_z = Decimal("0")
        else:
            volume_z = (current_bin.total_volume - median_volume) / effective_mad
        positive_z = max(volume_z, Decimal("0"))
        if positive_z <= 0:
            continue

        zscore_bins.append(
            VolumeZScoreProfileBin(
                bin_index=index,
                price_low=current_bin.price_low,
                price_high=current_bin.price_high,
                current_volume=current_bin.total_volume,
                current_buy_volume=current_bin.buy_volume,
                current_sell_volume=current_bin.sell_volume,
                current_delta_volume=current_bin.delta_volume,
                baseline_count=baseline_count,
                baseline_median_volume=median_volume,
                baseline_mad_volume=mad_volume,
                effective_mad_volume=effective_mad,
                volume_z_score=volume_z,
                positive_volume_z_score=positive_z,
                z_cap=cap,
                line_width_ratio=_clamp(float(positive_z / cap), 0.0, 1.0),
            )
        )

    return VolumeZScoreProfile(
        symbol=current_profile.symbol,
        timeframe=current_profile.timeframe,
        candle_open_time_utc_ms=current_profile.candle_open_time_utc_ms,
        candle_close_time_utc_ms=current_profile.candle_close_time_utc_ms,
        fixed_bin_size=current_profile.fixed_bin_size,
        baseline_count=baseline_count,
        target_baseline_candles=int(target_baseline_candles),
        volume_floor=floor,
        z_cap=cap,
        bins=tuple(zscore_bins),
        baseline_bins=baseline_bins,
    )


def _aggregate_baseline_bins(
    *,
    baseline_profiles: Sequence[CandleBinVolumeProfile],
    fixed_bin_size: Decimal,
) -> tuple[BaselineBinVolume, ...]:
    buy_volume_by_bin: dict[int, Decimal] = {}
    sell_volume_by_bin: dict[int, Decimal] = {}
    for profile in baseline_profiles:
        for index, bin_volume in profile.bins_by_index.items():
            buy_volume_by_bin[index] = buy_volume_by_bin.get(index, Decimal("0")) + bin_volume.buy_volume
            sell_volume_by_bin[index] = sell_volume_by_bin.get(index, Decimal("0")) + bin_volume.sell_volume

    result: list[BaselineBinVolume] = []
    for index in sorted(set(buy_volume_by_bin) | set(sell_volume_by_bin)):
        price_low, price_high = bin_bounds(index, fixed_bin_size)
        result.append(
            BaselineBinVolume(
                bin_index=index,
                price_low=price_low,
                price_high=price_high,
                buy_volume=buy_volume_by_bin.get(index, Decimal("0")),
                sell_volume=sell_volume_by_bin.get(index, Decimal("0")),
            )
        )
    return tuple(result)


def _median_decimal(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    ordered_values = sorted(values)
    mid = len(ordered_values) // 2
    if len(ordered_values) % 2:
        return ordered_values[mid]
    return (ordered_values[mid - 1] + ordered_values[mid]) / Decimal("2")


def _trade_price(trade: Any) -> Decimal:
    return to_decimal(_trade_value(trade, "price", "p"))


def _trade_quantity(trade: Any) -> Decimal:
    return to_decimal(_trade_value(trade, "quantity", "q"))


def _trade_side(trade: Any) -> str:
    side = _trade_value(trade, "side", default=None)
    if side is not None:
        return str(side).lower()
    maker_flag = _trade_value(trade, "m", default=False)
    return "sell" if bool(maker_flag) else "buy"


def _trade_value(trade: Any, *names: str, default: Any = None) -> Any:
    if isinstance(trade, Mapping):
        for name in names:
            if name in trade:
                return trade[name]
        return default
    for name in names:
        if hasattr(trade, name):
            return getattr(trade, name)
    return default


def _clamp(value: float, low: float, high: float) -> float:
    return min(max(value, low), high)
