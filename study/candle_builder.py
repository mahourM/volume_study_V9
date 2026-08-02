from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_FLOOR
from collections.abc import Iterable
from types import MappingProxyType

from core.bin_alignment import ExchangeMetadata, require_positive, to_decimal
from core.feature_calculation import OutputPrecision
from core.trade_mapping import (
    L2BinState,
    PriceDwellTracker,
    TradeEvent,
    calculate_diagonal_imbalance_ratios,
    apply_trade,
    copy_l2_bins_with_diagonal_imbalance_ratios,
)
from study.study_snapshot import (
    CandleRecord,
    CandleStudySnapshot,
    StudySnapshotConfig,
    build_candle_study_snapshot,
)


@dataclass(frozen=True)
class OrderFlowStudyConfig:
    symbol: str
    timeframe: str
    timeframe_ms: int
    study_candle_count: int
    fixed_bin_size: Decimal
    exchange_metadata: ExchangeMetadata
    output_precision: OutputPrecision

    def validate(self) -> None:
        if not self.symbol:
            raise ValueError("symbol is required")
        if not self.timeframe:
            raise ValueError("timeframe is required")
        if self.timeframe_ms <= 0:
            raise ValueError("timeframe_ms must be positive")
        if self.study_candle_count <= 0:
            raise ValueError("study_candle_count must be positive")
        require_positive(self.fixed_bin_size, "fixed_bin_size")
        require_positive(self.exchange_metadata.tick_size, "tick_size")
        require_positive(self.exchange_metadata.step_size, "step_size")


@dataclass
class ActiveCandle:
    open_time_ms: int
    close_time_ms: int
    fixed_bin_size: Decimal
    open_price: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    close_price: Decimal | None = None
    l2_bins: dict[int, L2BinState] = field(default_factory=dict)
    durations_ms_by_index: dict[int, int] = field(default_factory=dict)
    dwell_tracker: PriceDwellTracker = field(default_factory=PriceDwellTracker)

    def apply_trade(self, event: TradeEvent) -> None:
        self._update_ohlc(event.price)
        apply_trade(
            self.l2_bins,
            self.dwell_tracker,
            event,
            fixed_bin_size=self.fixed_bin_size,
            durations_by_index=self.durations_ms_by_index,
        )

    def to_record(self, *, closed: bool, duration_cutoff_ms: int | None = None) -> CandleRecord:
        durations = dict(self.durations_ms_by_index)
        if duration_cutoff_ms is not None:
            self._copy_active_duration(durations, duration_cutoff_ms)
        l2_bins = dict(self.l2_bins)
        if not closed:
            l2_bins = copy_l2_bins_with_diagonal_imbalance_ratios(l2_bins)
        return CandleRecord(
            open_time_ms=self.open_time_ms,
            close_time_ms=self.close_time_ms,
            open_price=self.open_price,
            high_price=self.high_price,
            low_price=self.low_price,
            close_price=self.close_price,
            l2_bins=MappingProxyType(l2_bins) if closed else l2_bins,
            durations_ms_by_index=MappingProxyType(durations) if closed else durations,
            closed=closed,
        )

    def close(self) -> CandleRecord:
        self.dwell_tracker.finalize(
            close_time_ms=self.close_time_ms,
            durations_by_index=self.durations_ms_by_index,
        )
        calculate_diagonal_imbalance_ratios(self.l2_bins)
        return self.to_record(closed=True).frozen()

    def _update_ohlc(self, price: Decimal) -> None:
        if self.open_price is None:
            self.open_price = price
            self.high_price = price
            self.low_price = price
            self.close_price = price
            return
        self.high_price = max(self.high_price or price, price)
        self.low_price = min(self.low_price or price, price)
        self.close_price = price

    def _copy_active_duration(self, target: dict[int, int], cutoff_ms: int) -> None:
        active_index = self.dwell_tracker.active_bin_index
        active_since = self.dwell_tracker.active_since_ms
        if active_index is None or active_since is None:
            return
        bounded_cutoff = min(int(cutoff_ms), self.close_time_ms)
        elapsed_ms = bounded_cutoff - active_since
        if elapsed_ms > 0:
            target[active_index] = target.get(active_index, 0) + elapsed_ms


class OrderFlowCandleBuilder:
    def __init__(self, config: OrderFlowStudyConfig) -> None:
        config.validate()
        self.config = config
        self._active: ActiveCandle | None = None
        self._closed: list[CandleRecord] = []

    @property
    def active_candle(self) -> ActiveCandle | None:
        return self._active

    @property
    def closed_candles(self) -> tuple[CandleRecord, ...]:
        return tuple(self._closed)

    def on_trade(self, event: TradeEvent) -> bool:
        if event.symbol != self.config.symbol:
            return False
        active = self._ensure_active(event.event_time_ms)
        if active is None:
            return False
        active.apply_trade(event)
        return True

    def append_closed_candle_batch(
        self,
        *,
        open_time_ms: int,
        close_time_ms: int,
        open_price: Decimal | None,
        high_price: Decimal | None,
        low_price: Decimal | None,
        close_price: Decimal | None,
        trades: Iterable[TradeEvent],
    ) -> bool:
        if self._closed and int(self._closed[-1].open_time_ms) >= int(open_time_ms):
            return False
        if self._active is not None:
            if int(self._active.open_time_ms) > int(open_time_ms):
                return False
            if int(self._active.open_time_ms) < int(open_time_ms):
                self._freeze_active()

        active = ActiveCandle(
            open_time_ms=int(open_time_ms),
            close_time_ms=int(close_time_ms),
            fixed_bin_size=self.config.fixed_bin_size,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
        )
        for event in sorted(trades, key=lambda item: int(item.event_time_ms)):
            if event.symbol == self.config.symbol and int(open_time_ms) <= int(event.event_time_ms) < int(close_time_ms):
                active.apply_trade(event)
        if open_price is not None:
            active.open_price = open_price
        if high_price is not None:
            active.high_price = high_price if active.high_price is None else max(active.high_price, high_price)
        if low_price is not None:
            active.low_price = low_price if active.low_price is None else min(active.low_price, low_price)
        if close_price is not None:
            active.close_price = close_price
        self._active = active
        self._freeze_active()
        return True

    def close_active_candle_for_kline(
        self,
        *,
        open_time_ms: int,
        close_time_ms: int,
        open_price: Decimal | None,
        high_price: Decimal | None,
        low_price: Decimal | None,
        close_price: Decimal | None,
    ) -> bool:
        active = self._active
        if active is None:
            return False
        if int(active.open_time_ms) != int(open_time_ms):
            return False
        if int(active.close_time_ms) != int(close_time_ms):
            return False
        active.open_price = open_price
        active.high_price = high_price
        active.low_price = low_price
        active.close_price = close_price
        self._freeze_active()
        return True

    def snapshot(self, *, now_ms: int) -> CandleStudySnapshot:
        records = list(self._closed)
        if self._active is not None:
            records.append(self._active.to_record(closed=False, duration_cutoff_ms=now_ms))
        return build_candle_study_snapshot(
            config=StudySnapshotConfig(
                symbol=self.config.symbol,
                timeframe=self.config.timeframe,
                study_candle_count=self.config.study_candle_count,
                fixed_bin_size=self.config.fixed_bin_size,
                exchange_metadata=self.config.exchange_metadata,
                output_precision=self.config.output_precision,
            ),
            candle_records=records,
        )

    def _ensure_active(self, event_time_ms: int) -> ActiveCandle | None:
        event_open_time = self._candle_open_time(event_time_ms)
        if self._active is not None:
            if event_open_time < self._active.open_time_ms:
                return None
            if event_open_time == self._active.open_time_ms:
                return self._active
            self._freeze_active()

        self._active = ActiveCandle(
            open_time_ms=event_open_time,
            close_time_ms=event_open_time + self.config.timeframe_ms,
            fixed_bin_size=self.config.fixed_bin_size,
        )
        return self._active

    def _freeze_active(self) -> None:
        if self._active is None:
            return
        self._closed.append(self._active.close())
        self._active = None
        self._closed = self._closed[-self.config.study_candle_count :]

    def _candle_open_time(self, event_time_ms: int) -> int:
        timeframe = Decimal(self.config.timeframe_ms)
        event_time = to_decimal(event_time_ms)
        return int((event_time / timeframe).to_integral_value(rounding=ROUND_FLOOR) * timeframe)
