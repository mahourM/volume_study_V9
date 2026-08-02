from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Any, Mapping

from core.bin_alignment import (
    ExchangeMetadata,
    bin_index,
    to_decimal,
)
from core.feature_calculation import BinFeature, OutputPrecision, build_bin_feature
from core.trade_mapping import (
    L2BinState,
    dominant_diagonal_side as calculate_dominant_diagonal_side,
    dominant_side_efficiency as calculate_dominant_side_efficiency,
    dominant_side_volume as calculate_dominant_side_volume,
    price_progress_in_bin as calculate_price_progress_in_bin,
)


@dataclass(frozen=True)
class StudySnapshotConfig:
    symbol: str
    timeframe: str
    study_candle_count: int
    fixed_bin_size: Decimal
    exchange_metadata: ExchangeMetadata
    output_precision: OutputPrecision

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.timeframe:
            raise ValueError("timeframe is required")
        if self.study_candle_count <= 0:
            raise ValueError("study_candle_count must be positive")
        if self.fixed_bin_size <= 0:
            raise ValueError("fixed_bin_size must be positive")


@dataclass(frozen=True)
class FrozenL2BinState:
    total_volume: Decimal
    delta: Decimal
    horizontal_delta: Decimal
    ask_traded_volume: Decimal
    bid_traded_volume: Decimal
    buy_diagonal_imbalance_ratio: Decimal
    sell_diagonal_imbalance_ratio: Decimal
    duration_ms: int
    first_price: Decimal | None = None
    last_price: Decimal | None = None
    min_trade_price_in_bin: Decimal | None = None
    max_trade_price_in_bin: Decimal | None = None
    price_progress_in_bin: Decimal | None = None
    dominant_diagonal_side: str = "NONE"
    dominant_side_volume: Decimal = Decimal("0")
    dominant_side_efficiency: Decimal | None = None


@dataclass(frozen=True)
class CandleRecord:
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    close_price: Decimal | None
    l2_bins: Mapping[int, L2BinState | FrozenL2BinState]
    durations_ms_by_index: Mapping[int, int]
    closed: bool

    def body_low_high(self) -> tuple[Decimal | None, Decimal | None]:
        if self.open_price is None or self.close_price is None:
            return None, None
        return min(self.open_price, self.close_price), max(self.open_price, self.close_price)

    def frozen(self) -> CandleRecord:
        l2_items = {
            index: freeze_l2_bin_state(state, self.durations_ms_by_index.get(index, state.duration_ms))
            for index, state in self.l2_bins.items()
        }
        durations = dict(self.durations_ms_by_index)
        return CandleRecord(
            open_time_ms=self.open_time_ms,
            close_time_ms=self.close_time_ms,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            l2_bins=MappingProxyType(l2_items),
            durations_ms_by_index=MappingProxyType(durations),
            closed=True,
        )


@dataclass(frozen=True)
class CandleStudySnapshot:
    symbol: str
    timeframe: str
    study_candle_count: int
    fixed_bin_size: Decimal
    candles: tuple[dict[str, Any], ...]

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "study_candle_count": self.study_candle_count,
            "fixed_bin_size": str(self.fixed_bin_size),
            "candles": list(self.candles),
        }


def freeze_l2_bin_state(state: L2BinState | FrozenL2BinState, duration_ms: int) -> FrozenL2BinState:
    return FrozenL2BinState(
        total_volume=state.total_volume,
        delta=state.delta,
        horizontal_delta=getattr(state, "horizontal_delta", state.delta),
        ask_traded_volume=getattr(state, "ask_traded_volume", Decimal("0")),
        bid_traded_volume=getattr(state, "bid_traded_volume", Decimal("0")),
        buy_diagonal_imbalance_ratio=getattr(
            state,
            "buy_diagonal_imbalance_ratio",
            Decimal("0"),
        ),
        sell_diagonal_imbalance_ratio=getattr(
            state,
            "sell_diagonal_imbalance_ratio",
            Decimal("0"),
        ),
        duration_ms=duration_ms,
        first_price=getattr(state, "first_price", None),
        last_price=getattr(state, "last_price", None),
        min_trade_price_in_bin=getattr(state, "min_trade_price_in_bin", None),
        max_trade_price_in_bin=getattr(state, "max_trade_price_in_bin", None),
        price_progress_in_bin=calculate_price_progress_in_bin(state),
        dominant_diagonal_side=calculate_dominant_diagonal_side(state),
        dominant_side_volume=calculate_dominant_side_volume(state),
        dominant_side_efficiency=calculate_dominant_side_efficiency(state),
    )


def build_candle_study_snapshot(
    *,
    config: StudySnapshotConfig,
    candle_records: list[CandleRecord],
) -> CandleStudySnapshot:
    config.validate()
    visible_records = sorted(candle_records, key=lambda item: item.open_time_ms, reverse=True)[
        : config.study_candle_count
    ]
    candles = tuple(
        build_candle_payload(
            config=config,
            candle_number=position,
            record=record,
        )
        for position, record in enumerate(visible_records, start=1)
    )
    return CandleStudySnapshot(
        symbol=config.symbol,
        timeframe=config.timeframe,
        study_candle_count=config.study_candle_count,
        fixed_bin_size=config.fixed_bin_size,
        candles=candles,
    )


def build_candle_payload(
    *,
    config: StudySnapshotConfig,
    candle_number: int,
    record: CandleRecord,
) -> dict[str, Any]:
    bin_features = build_candle_bin_features(
        config=config,
        candle_number=candle_number,
        record=record,
    )
    precision = config.output_precision
    return {
        "candle_number": candle_number,
        "open_time": record.open_time_ms,
        "close_time": record.close_time_ms,
        "ohlc": format_ohlc(record, precision),
        "bins": [item.to_payload(precision) for item in bin_features],
    }


def build_candle_bin_features(
    *,
    config: StudySnapshotConfig,
    candle_number: int,
    record: CandleRecord,
) -> tuple[BinFeature, ...]:
    output_indices = output_bin_indices(record, config.fixed_bin_size)
    if not output_indices and record.l2_bins:
        output_indices = sorted(record.l2_bins)

    features = [
        build_bin_feature(
            candle_number=candle_number,
            index=index,
            fixed_bin_size=config.fixed_bin_size,
            tick_size=config.exchange_metadata.tick_size,
            l2_state=record.l2_bins.get(index),
            duration_ms=record.durations_ms_by_index.get(
                index,
                getattr(record.l2_bins.get(index), "duration_ms", 0),
            ),
        )
        for index in output_indices
    ]
    return tuple(features)


def output_bin_indices(record: CandleRecord, fixed_bin_size: Decimal) -> list[int]:
    body_low, body_high = record.body_low_high()
    candidates: list[int] = []
    if body_low is not None:
        candidates.append(bin_index(body_low, fixed_bin_size))
    if body_high is not None:
        candidates.append(bin_index(body_high, fixed_bin_size))
    candidates.extend(record.l2_bins.keys())
    if not candidates:
        return []
    return list(range(min(candidates), max(candidates) + 1))


def format_ohlc(record: CandleRecord, precision: OutputPrecision) -> dict[str, str | None]:
    return {
        "open": format_optional_decimal(record.open_price, precision),
        "high": format_optional_decimal(record.high_price, precision),
        "low": format_optional_decimal(record.low_price, precision),
        "close": format_optional_decimal(record.close_price, precision),
    }


def format_optional_decimal(value: Decimal | None, precision: OutputPrecision) -> str | None:
    if value is None:
        return None
    return precision.format_decimal(to_decimal(value))
