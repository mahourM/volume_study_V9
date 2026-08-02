from __future__ import annotations

import time
import threading
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import median
from typing import Any, Mapping

from absorption.binance_aggtrade_ws_client import AggTradeEvent
from absorption.binance_kline_ws_client import KLINE_INTERVAL_BY_INTERNAL, KlineClosedEvent
from cme_provider.local_data import (
    CmeLocalDataCatalog,
    CmeLocalDbnTradeStore,
    new_york_session_bounds_utc_ms,
    trading_day_bounds_utc_ms,
    trading_day_for_timestamp_ms,
)
from cme_provider.footprint_index import FootprintCandleIndex
from core.bin_alignment import ExchangeMetadata, bin_bounds
from core.contract_spike import calculate_contract_spike_metrics, is_contract_spike
from core.engine_output_bus import (
    DOM_ENGINE_PRODUCER,
    DOM_POSITIVE_REFILL_OUTPUT_TYPE,
    EngineOutputStore,
)
from core.feature_calculation import OutputPrecision
from core.timeframe_policy import TIMEFRAME_MS_BY_NAME
from core.trade_mapping import TradeEvent
from execution.position_close_csv_recorder import get_position_close_csv_recorder
from study.candle_builder import OrderFlowCandleBuilder, OrderFlowStudyConfig
from triggerEngine import TriggerEngine
from volume_profile.zscore_profile_builder import build_candle_bin_volume_profile


CME_BIN_TICK_OPTIONS = (1, 2, 4, 8, 16)
CME_BIN_TICK_COUNT = Decimal("1")


def normalize_cme_bin_tick_count(value: int | Decimal | None) -> Decimal:
    try:
        candidate = int(value) if value is not None else int(CME_BIN_TICK_COUNT)
    except (TypeError, ValueError):
        candidate = int(CME_BIN_TICK_COUNT)
    if candidate not in CME_BIN_TICK_OPTIONS:
        candidate = int(CME_BIN_TICK_COUNT)
    return Decimal(candidate)


@dataclass(frozen=True)
class CmeEngineConfig:
    session_start_hour_chicago: int
    new_york_session_start_hour: int = 9
    new_york_session_start_minute: int = 30
    new_york_session_end_hour: int = 16
    new_york_session_end_minute: int = 0
    output_decimal_places: int = 3
    duration_unit_ms: int = 1000


class CmeCandleEngine:
    def __init__(self, *, trade_store: CmeLocalDbnTradeStore) -> None:
        self.trade_store = trade_store
        self._cache: dict[tuple[str, str], tuple[KlineClosedEvent, ...]] = {}

    def candles(self, *, provider_symbol: str, timeframe: str) -> tuple[KlineClosedEvent, ...]:
        key = (provider_symbol.upper(), timeframe.strip().upper())
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        candles = self.candles_from_trades(
            provider_symbol=key[0],
            timeframe=key[1],
            trades=self.trade_store.trades_for_symbol(key[0]),
        )
        self._cache[key] = candles
        return candles

    @staticmethod
    def candles_from_trades(
        *,
        provider_symbol: str,
        timeframe: str,
        trades: tuple[AggTradeEvent, ...],
    ) -> tuple[KlineClosedEvent, ...]:
        normalized_symbol = provider_symbol.upper()
        normalized_timeframe = timeframe.strip().upper()
        interval_ms = TIMEFRAME_MS_BY_NAME[normalized_timeframe]
        interval = KLINE_INTERVAL_BY_INTERNAL[normalized_timeframe]
        builders: dict[int, _CandleAccumulator] = {}
        for trade in trades:
            open_time_ms = (int(trade.event_time_ms) // interval_ms) * interval_ms
            accumulator = builders.get(open_time_ms)
            if accumulator is None:
                accumulator = _CandleAccumulator(open_time_ms=open_time_ms)
                builders[open_time_ms] = accumulator
            accumulator.apply_trade(trade)

        candles = tuple(
            KlineClosedEvent(
                symbol=normalized_symbol,
                internal_timeframe=normalized_timeframe,
                binance_interval=interval,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + interval_ms - 1,
                open_price=accumulator.open_price or Decimal("0"),
                high_price=accumulator.high_price or Decimal("0"),
                low_price=accumulator.low_price or Decimal("0"),
                close_price=accumulator.close_price or Decimal("0"),
            )
            for open_time_ms, accumulator in sorted(builders.items())
            if accumulator.is_valid
        )
        return candles


class CmeFootprintEngine:
    def __init__(
        self,
        *,
        trade_store: CmeLocalDbnTradeStore,
        candle_engine: CmeCandleEngine,
        config: CmeEngineConfig,
    ) -> None:
        self.trade_store = trade_store
        self.candle_engine = candle_engine
        self.config = config
        self._cache: dict[tuple[str, str, Decimal], tuple[dict[str, Any], ...]] = {}

    def footprint_candles(
        self,
        *,
        provider_symbol: str,
        mt5_symbol: str,
        timeframe: str,
        tick_size: Decimal,
    ) -> tuple[dict[str, Any], ...]:
        normalized_symbol = provider_symbol.upper()
        normalized_timeframe = timeframe.strip().upper()
        fixed_bin_size = tick_size * CME_BIN_TICK_COUNT
        key = (normalized_symbol, normalized_timeframe, fixed_bin_size)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        payload = self.footprint_candles_from_trades(
            provider_symbol=normalized_symbol,
            mt5_symbol=mt5_symbol,
            timeframe=normalized_timeframe,
            tick_size=tick_size,
            trades=self.trade_store.trades_for_symbol(normalized_symbol),
        )
        self._cache[key] = payload
        return payload

    def footprint_candles_from_trades(
        self,
        *,
        provider_symbol: str,
        mt5_symbol: str,
        timeframe: str,
        tick_size: Decimal,
        trades: tuple[AggTradeEvent, ...],
    ) -> tuple[dict[str, Any], ...]:
        normalized_symbol = provider_symbol.upper()
        normalized_timeframe = timeframe.strip().upper()
        fixed_bin_size = tick_size * CME_BIN_TICK_COUNT
        candles = self.candle_engine.candles_from_trades(
            provider_symbol=normalized_symbol,
            timeframe=normalized_timeframe,
            trades=trades,
        )
        trades_by_open = self._trades_by_candle_open_from_trades(
            trades=trades,
            timeframe=normalized_timeframe,
        )
        builder = OrderFlowCandleBuilder(
            OrderFlowStudyConfig(
                symbol=normalized_symbol,
                timeframe=normalized_timeframe,
                timeframe_ms=TIMEFRAME_MS_BY_NAME[normalized_timeframe],
                study_candle_count=max(1, len(candles)),
                fixed_bin_size=fixed_bin_size,
                exchange_metadata=ExchangeMetadata.from_values(
                    symbol=normalized_symbol,
                    tick_size=tick_size,
                    step_size=Decimal("1"),
                ),
                output_precision=OutputPrecision(
                    decimal_places=self.config.output_decimal_places,
                    duration_unit_ms=self.config.duration_unit_ms,
                ),
            )
        )
        for candle in candles:
            builder.append_closed_candle_batch(
                open_time_ms=int(candle.open_time_ms),
                close_time_ms=int(candle.close_time_ms),
                open_price=candle.open_price,
                high_price=candle.high_price,
                low_price=candle.low_price,
                close_price=candle.close_price,
                trades=trades_by_open.get(int(candle.open_time_ms), ()),
            )

        snapshot = builder.snapshot(now_ms=int(time.time() * 1000)).to_payload()
        payload = tuple(
            self._normalize_footprint_candle_payload(
                candle,
                mt5_symbol=mt5_symbol,
                provider_symbol=normalized_symbol,
                timeframe=normalized_timeframe,
                interval=KLINE_INTERVAL_BY_INTERNAL[normalized_timeframe],
                output_decimal_places=self.config.output_decimal_places,
            )
            for candle in sorted(snapshot.get("candles", []), key=lambda item: int(item.get("open_time", 0)))
        )
        return payload

    def _trades_by_candle_open(
        self,
        *,
        provider_symbol: str,
        timeframe: str,
    ) -> dict[int, tuple[TradeEvent, ...]]:
        return self._trades_by_candle_open_from_trades(
            trades=self.trade_store.trades_for_symbol(provider_symbol),
            timeframe=timeframe,
        )

    @staticmethod
    def _trades_by_candle_open_from_trades(
        *,
        trades: tuple[AggTradeEvent, ...],
        timeframe: str,
    ) -> dict[int, tuple[TradeEvent, ...]]:
        interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
        grouped: dict[int, list[TradeEvent]] = defaultdict(list)
        for trade in trades:
            open_time_ms = (int(trade.event_time_ms) // interval_ms) * interval_ms
            grouped[open_time_ms].append(
                TradeEvent.from_values(
                    symbol=trade.symbol,
                    event_time_ms=int(trade.event_time_ms),
                    price=trade.price,
                    quantity=trade.quantity,
                    side=trade.side,  # type: ignore[arg-type]
                )
            )
        return {
            open_time_ms: tuple(sorted(trades, key=lambda item: int(item.event_time_ms)))
            for open_time_ms, trades in grouped.items()
        }

    @staticmethod
    def _normalize_footprint_candle_payload(
        candle: dict[str, Any],
        *,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        interval: str,
        output_decimal_places: int,
    ) -> dict[str, Any]:
        payload = dict(candle)
        ohlc = payload.get("ohlc", {}) if isinstance(payload.get("ohlc"), dict) else {}
        payload["symbol"] = provider_symbol
        payload["mt5_symbol"] = mt5_symbol
        payload["provider_symbol"] = provider_symbol
        payload["market_provider"] = "CME_LOCAL_DBN"
        payload["timeframe"] = timeframe
        payload["interval"] = interval
        payload["open_time_ms"] = int(payload.get("open_time", 0))
        payload["close_time_ms"] = int(payload.get("close_time", 0))
        payload["open_price"] = ohlc.get("open")
        payload["high_price"] = ohlc.get("high")
        payload["low_price"] = ohlc.get("low")
        payload["close_price"] = ohlc.get("close")
        spike_metrics = _add_contract_spike_metrics(
            payload.get("bins", []),
            output_decimal_places=output_decimal_places,
        )
        payload["contract_spike_p75"] = _format_decimal(
            spike_metrics.p75,
            output_decimal_places,
        )
        payload["contract_spike_normal_median"] = _format_decimal(
            spike_metrics.normal_median,
            output_decimal_places,
        )
        payload["contract_spike_normal_mad"] = _format_decimal(
            spike_metrics.normal_mad,
            output_decimal_places,
        )
        payload["contract_spike_score_deviation"] = _format_decimal(
            spike_metrics.score_deviation,
            output_decimal_places,
        )
        return payload


class CmeDailyVolumeProfileEngine:
    def __init__(
        self,
        *,
        trade_store: CmeLocalDbnTradeStore,
        config: CmeEngineConfig,
    ) -> None:
        self.trade_store = trade_store
        self.config = config
        self._cache: dict[tuple[str, Decimal], tuple[dict[str, Any], ...]] = {}

    def daily_profiles(
        self,
        *,
        provider_symbol: str,
        tick_size: Decimal,
    ) -> tuple[dict[str, Any], ...]:
        normalized_symbol = provider_symbol.upper()
        fixed_bin_size = tick_size * CME_BIN_TICK_COUNT
        key = (normalized_symbol, fixed_bin_size)
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        trades_by_day: dict[str, list[AggTradeEvent]] = defaultdict(list)
        for trade in self.trade_store.trades_for_symbol(normalized_symbol):
            trading_day = trading_day_for_timestamp_ms(
                int(trade.event_time_ms),
                session_start_hour_chicago=self.config.session_start_hour_chicago,
            )
            trades_by_day[trading_day].append(trade)

        profiles = tuple(
            self._profile_for_day(
                provider_symbol=normalized_symbol,
                trading_day=trading_day,
                fixed_bin_size=fixed_bin_size,
                trades=trades,
            )
            for trading_day, trades in sorted(trades_by_day.items())
        )
        self._cache[key] = profiles
        return profiles

    def daily_profiles_from_trades(
        self,
        *,
        provider_symbol: str,
        trading_day: str,
        tick_size: Decimal,
        trades: tuple[AggTradeEvent, ...],
    ) -> tuple[dict[str, Any], ...]:
        if not trades:
            return ()
        return (
            self._profile_for_day(
                provider_symbol=provider_symbol.upper(),
                trading_day=trading_day,
                fixed_bin_size=tick_size * CME_BIN_TICK_COUNT,
                trades=list(trades),
            ),
        )

    def _profile_for_day(
        self,
        *,
        provider_symbol: str,
        trading_day: str,
        fixed_bin_size: Decimal,
        trades: list[AggTradeEvent],
    ) -> dict[str, Any]:
        profile = build_candle_bin_volume_profile(
            symbol=provider_symbol,
            timeframe="1D",
            candle_open_time_utc_ms=min(int(item.event_time_ms) for item in trades),
            candle_close_time_utc_ms=max(int(item.event_time_ms) for item in trades),
            fixed_bin_size=fixed_bin_size,
            agg_trades=trades,
        )
        bins = []
        max_total_volume = Decimal("0")
        for bin_volume in profile.bins_by_index.values():
            max_total_volume = max(max_total_volume, bin_volume.total_volume)
        for index in sorted(profile.bins_by_index):
            bin_volume = profile.bins_by_index[index]
            low, high = bin_bounds(index, fixed_bin_size)
            total = bin_volume.total_volume
            bins.append(
                {
                    "bin_index": index,
                    "price_low": str(low),
                    "price_high": str(high),
                    "buy_volume": str(bin_volume.buy_volume),
                    "sell_volume": str(bin_volume.sell_volume),
                    "total_volume": str(total),
                    "delta_volume": str(bin_volume.delta_volume),
                    "normalized_volume": float(total / max_total_volume) if max_total_volume > 0 else 0.0,
                }
            )

        session_start_ms, session_end_ms = trading_day_bounds_utc_ms(
            trading_day,
            session_start_hour_chicago=self.config.session_start_hour_chicago,
        )
        return {
            "trading_day": trading_day,
            "session_start_utc_ms": session_start_ms,
            "session_end_utc_ms": session_end_ms,
            "fixed_bin_size": str(fixed_bin_size),
            "max_total_volume": str(max_total_volume),
            "bins": bins,
        }


class CmeChartPayloadBuilder:
    def __init__(
        self,
        *,
        candle_engine: CmeCandleEngine,
        volume_profile_engine: CmeDailyVolumeProfileEngine,
    ) -> None:
        self.candle_engine = candle_engine
        self.volume_profile_engine = volume_profile_engine

    def chart_session_payload(
        self,
        *,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        tick_size: Decimal,
    ) -> dict[str, Any]:
        candles = self.candle_engine.candles(
            provider_symbol=provider_symbol,
            timeframe=timeframe,
        )
        return {
            "mt5_symbol": mt5_symbol,
            "symbol": provider_symbol,
            "provider_symbol": provider_symbol,
            "market_provider": "CME_LOCAL_DBN",
            "timeframe": timeframe,
            "interval": KLINE_INTERVAL_BY_INTERNAL[timeframe],
            "price_step": str(tick_size),
            "fixed_bin_size": str(tick_size * CME_BIN_TICK_COUNT),
            "candles": [
                {
                    "open_time_ms": int(candle.open_time_ms),
                    "close_time_ms": int(candle.close_time_ms),
                    "open_price": str(candle.open_price),
                    "high_price": str(candle.high_price),
                    "low_price": str(candle.low_price),
                    "close_price": str(candle.close_price),
                }
                for candle in candles
            ],
            "daily_volume_profiles": list(
                self.volume_profile_engine.daily_profiles(
                    provider_symbol=provider_symbol,
                    tick_size=tick_size,
                )
            ),
        }


class CmePagedHistoryEngine:
    MAX_WINDOW_CANDLES = 500

    def __init__(
        self,
        *,
        catalog: CmeLocalDataCatalog,
        trade_store: CmeLocalDbnTradeStore,
        candle_engine: CmeCandleEngine,
        footprint_engine: CmeFootprintEngine,
        volume_profile_engine: CmeDailyVolumeProfileEngine,
        config: CmeEngineConfig,
        trigger_engine: TriggerEngine | None = None,
        engine_output_store: EngineOutputStore | None = None,
        candle_cache_size: int = 20_000,
        footprint_cache_size: int = 10_000,
        footprint_index_path: Path | None = None,
    ) -> None:
        self.catalog = catalog
        self.trade_store = trade_store
        self.candle_engine = candle_engine
        self.footprint_engine = footprint_engine
        self.volume_profile_engine = volume_profile_engine
        self.config = config
        self.trigger_engine = trigger_engine
        self.engine_output_store = engine_output_store
        self._candle_cache_size = max(1, int(candle_cache_size))
        self._footprint_cache_size = max(1, int(footprint_cache_size))
        self._persistent_footprint_index = (
            FootprintCandleIndex(footprint_index_path)
            if footprint_index_path is not None
            else None
        )
        self._chart_candle_cache: OrderedDict[
            tuple[str, str, int],
            dict[str, Any],
        ] = OrderedDict()
        self._footprint_candle_cache: OrderedDict[
            tuple[str, str, str, str, int, int],
            dict[str, Any],
        ] = OrderedDict()
        self._chart_cache_lock = threading.RLock()
        self._footprint_cache_lock = threading.RLock()
        self._new_york_session_profile_cache: dict[
            tuple[str, str, Decimal],
            dict[str, Any] | None,
        ] = {}

    def chart_window(
        self,
        *,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        tick_size: Decimal,
        end_time_ms: int | None = None,
        candle_limit: int = 200,
        include_profiles: bool = True,
        order_books_by_time: Mapping[int, Any] | None = None,
    ) -> dict[str, Any]:
        chart_bin_tick_count = normalize_cme_bin_tick_count(CME_BIN_TICK_COUNT)
        fixed_bin_size = tick_size * chart_bin_tick_count
        window = self._frame_window(
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            end_time_ms=end_time_ms,
            candle_limit=candle_limit,
        )
        frame = window["frame"]
        viewport_metrics = _new_viewport_metrics()
        candles = self._cached_chart_candles(
            frame,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            metrics=viewport_metrics,
        )
        footprint_candles = self._cached_footprint_candles(
            frame,
            provider_symbol=provider_symbol,
            mt5_symbol=mt5_symbol,
            timeframe=timeframe,
            tick_size=tick_size,
            bin_tick_count=chart_bin_tick_count,
            output_decimal_places=self.config.output_decimal_places,
            duration_unit_ms=self.config.duration_unit_ms,
            metrics=viewport_metrics,
        )
        candles = self._merge_chart_candles_with_footprints(
            candles,
            footprint_candles,
        )
        dom_outputs_by_open = self._dom_positive_refill_outputs_by_candle_open(
            target_candles=candles,
            mt5_symbol=mt5_symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
        )
        if dom_outputs_by_open:
            self._attach_dom_positive_refill_markers(
                target_candles=candles,
                outputs_by_open=dom_outputs_by_open,
            )
            order_books_by_time = _merge_order_books_by_time(
                self._dom_positive_refill_order_books_by_time(
                    outputs_by_open=dom_outputs_by_open,
                    mt5_symbol=mt5_symbol,
                    provider_symbol=provider_symbol,
                    timeframe=timeframe,
                ),
                order_books_by_time,
            )
        signals = self._attach_trigger_signals(
            frame,
            target_candles=candles,
            mt5_symbol=mt5_symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            tick_size=tick_size,
            evaluation_time_ms=window["end_ms"],
            viewport_metrics=viewport_metrics,
            order_books_by_time=order_books_by_time,
        )
        self._attach_cumulative_delta_fields(
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            candles=candles,
            viewport_metrics=viewport_metrics,
        )
        contract_symbol = str(frame.attrs.get("contract_symbol", ""))
        trading_days = _trading_days_in_frame(
            frame,
            session_start_hour_chicago=self.config.session_start_hour_chicago,
        )
        return {
            "mt5_symbol": mt5_symbol,
            "symbol": provider_symbol,
            "provider_symbol": provider_symbol,
            "contract_symbol": contract_symbol,
            "contract_symbols": list(frame.attrs.get("contract_symbols", ())),
            "market_provider": "CME_LOCAL_DBN",
            "quantity_unit": "CONTRACTS",
            "timeframe": timeframe,
            "interval": KLINE_INTERVAL_BY_INTERNAL[timeframe],
            "price_step": str(tick_size),
            "bin_tick_count": int(chart_bin_tick_count),
            "fixed_bin_size": str(fixed_bin_size),
            "trading_day": trading_days[-1] if trading_days else "",
            "trading_days": trading_days,
            "viewport_window": True,
            "earliest_window_start_ms": window["earliest_start_ms"],
            "window_start_ms": window["start_ms"],
            "window_end_ms": window["end_ms"],
            "latest_window_end_ms": window["latest_end_ms"],
            "window_candle_limit": window["candle_limit"],
            "window_cursor_ms": window["start_ms"],
            "has_older_data": window["has_older_data"],
            "processed_trades": len(frame),
            "candles": candles,
            "signals": signals,
            "viewport_metrics": viewport_metrics,
            "daily_volume_profiles": (
                self._new_york_session_profiles_for_window(
                    frame,
                    provider_symbol=provider_symbol,
                    tick_size=tick_size,
                )
                if include_profiles
                else []
            ),
        }

    def footprint_window(
        self,
        *,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        tick_size: Decimal,
        bin_tick_count: int | Decimal = CME_BIN_TICK_COUNT,
        end_time_ms: int | None = None,
        candle_limit: int = 20,
    ) -> dict[str, Any]:
        normalized_bin_tick_count = normalize_cme_bin_tick_count(bin_tick_count)
        normalized_limit = max(1, min(self.MAX_WINDOW_CANDLES, int(candle_limit)))
        interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
        latest_available_end = getattr(self.trade_store, "latest_available_end_ms", None)
        latest_end_ms = (
            latest_available_end(provider_symbol)
            if callable(latest_available_end)
            else None
        )
        if latest_end_ms is None:
            latest_event_ms = self.trade_store.latest_event_time_ms(provider_symbol)
            latest_end_ms = (
                ((latest_event_ms // interval_ms) + 1) * interval_ms
                if latest_event_ms is not None
                else None
            )
        if latest_end_ms is not None:
            latest_end_ms = (int(latest_end_ms) // interval_ms) * interval_ms
            requested_end_ms = (
                int(end_time_ms) if end_time_ms is not None else latest_end_ms
            )
            requested_end_ms = max(
                interval_ms,
                min(requested_end_ms, latest_end_ms),
            )
            requested_end_ms = (requested_end_ms // interval_ms) * interval_ms
            requested_start_ms = requested_end_ms - normalized_limit * interval_ms
            cached_window = None
            if self._persistent_footprint_index is not None:
                footprint_signature = self._footprint_index_signature(
                    provider_symbol=provider_symbol,
                    mt5_symbol=mt5_symbol,
                    timeframe=timeframe,
                    tick_size=tick_size,
                    bin_tick_count=normalized_bin_tick_count,
                )
                cached_window = self._persistent_footprint_index.load_window(
                    signature=footprint_signature,
                    requested_start_ms=requested_start_ms,
                    requested_end_ms=requested_end_ms,
                    candle_limit=normalized_limit,
                )
            if cached_window is not None:
                candles = [dict(candle) for candle in cached_window["candles"]]
                signals = [
                    dict(signal)
                    for candle in candles
                    for signal in candle.get("trigger_signals", ()) or ()
                ]
                contract_symbols = list(cached_window["contract_symbols"])
                trading_days = list(cached_window["trading_days"])
                earliest_start_ms = self.trade_store.earliest_partition_time_ms(
                    provider_symbol
                )
                return {
                    "mt5_symbol": mt5_symbol,
                    "symbol": provider_symbol,
                    "provider_symbol": provider_symbol,
                    "contract_symbol": (
                        contract_symbols[0] if len(contract_symbols) == 1 else ""
                    ),
                    "contract_symbols": contract_symbols,
                    "market_provider": "CME_LOCAL_DBN",
                    "quantity_unit": "CONTRACTS",
                    "timeframe": timeframe,
                    "interval": KLINE_INTERVAL_BY_INTERNAL[timeframe],
                    "price_step": str(tick_size),
                    "bin_tick_count": int(normalized_bin_tick_count),
                    "fixed_bin_size": str(tick_size * normalized_bin_tick_count),
                    "trading_day": trading_days[-1] if trading_days else "",
                    "trading_days": trading_days,
                    "viewport_window": True,
                    "earliest_window_start_ms": int(
                        earliest_start_ms
                        if earliest_start_ms is not None
                        else cached_window["window_start_ms"]
                    ),
                    "window_start_ms": int(cached_window["window_start_ms"]),
                    "window_end_ms": requested_end_ms,
                    "latest_window_end_ms": latest_end_ms,
                    "window_candle_limit": normalized_limit,
                    "window_cursor_ms": int(cached_window["window_start_ms"]),
                    "has_older_data": (
                        earliest_start_ms is not None
                        and int(cached_window["window_start_ms"])
                        > int(earliest_start_ms)
                    ),
                    "processed_trades": int(cached_window["processed_trades"]),
                    "candles": candles,
                    "signals": signals,
                    "viewport_metrics": {
                        **_new_viewport_metrics(),
                        "cache_hit_count": len(candles),
                    },
                    "live_candle": None,
                }

        window = self._frame_window(
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            end_time_ms=end_time_ms,
            candle_limit=candle_limit,
        )
        frame = window["frame"]
        contract_symbol = str(frame.attrs.get("contract_symbol", ""))
        trading_days = _trading_days_in_frame(
            frame,
            session_start_hour_chicago=self.config.session_start_hour_chicago,
        )
        viewport_metrics = _new_viewport_metrics()
        candles = self._cached_footprint_candles(
            frame,
            provider_symbol=provider_symbol,
            mt5_symbol=mt5_symbol,
            timeframe=timeframe,
            tick_size=tick_size,
            bin_tick_count=normalized_bin_tick_count,
            output_decimal_places=self.config.output_decimal_places,
            duration_unit_ms=self.config.duration_unit_ms,
            metrics=viewport_metrics,
        )
        self._attach_cumulative_delta_fields(
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            candles=candles,
            viewport_metrics=viewport_metrics,
        )
        signals = self._attach_trigger_signals(
            frame,
            target_candles=candles,
            analysis_candles=(
                candles
                if int(normalized_bin_tick_count) == self.trigger_engine.config.bin_tick_count
                else None
            ) if self.trigger_engine is not None else None,
            mt5_symbol=mt5_symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            tick_size=tick_size,
            evaluation_time_ms=window["end_ms"],
            viewport_metrics=viewport_metrics,
        )
        if self._persistent_footprint_index is not None:
            interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
            footprint_signature = self._footprint_index_signature(
                provider_symbol=provider_symbol,
                mt5_symbol=mt5_symbol,
                timeframe=timeframe,
                tick_size=tick_size,
                bin_tick_count=normalized_bin_tick_count,
            )
            open_times = _frame_candle_open_times(frame, timeframe=timeframe)
            trade_count_by_open = {
                int(open_time_ms): len(
                    _frame_for_candle_opens(
                        frame,
                        timeframe=timeframe,
                        open_times=[int(open_time_ms)],
                    )
                )
                for open_time_ms in open_times
            }
            self._persistent_footprint_index.store_window(
                signature=footprint_signature,
                coverage_start_ms=int(window["end_ms"])
                - int(window["candle_limit"]) * interval_ms,
                coverage_end_ms=int(window["end_ms"]),
                candles=candles,
                contract_symbol=contract_symbol,
                trading_day_by_open={
                    int(candle.get("open_time_ms") or candle.get("open_time") or 0):
                    str(candle.get("trading_day") or "")
                    for candle in candles
                },
                trade_count_by_open=trade_count_by_open,
            )
        return {
            "mt5_symbol": mt5_symbol,
            "symbol": provider_symbol,
            "provider_symbol": provider_symbol,
            "contract_symbol": contract_symbol,
            "contract_symbols": list(frame.attrs.get("contract_symbols", ())),
            "market_provider": "CME_LOCAL_DBN",
            "quantity_unit": "CONTRACTS",
            "timeframe": timeframe,
            "interval": KLINE_INTERVAL_BY_INTERNAL[timeframe],
            "price_step": str(tick_size),
            "bin_tick_count": int(normalized_bin_tick_count),
            "fixed_bin_size": str(tick_size * normalized_bin_tick_count),
            "trading_day": trading_days[-1] if trading_days else "",
            "trading_days": trading_days,
            "viewport_window": True,
            "earliest_window_start_ms": window["earliest_start_ms"],
            "window_start_ms": window["start_ms"],
            "window_end_ms": window["end_ms"],
            "latest_window_end_ms": window["latest_end_ms"],
            "window_candle_limit": window["candle_limit"],
            "window_cursor_ms": window["start_ms"],
            "has_older_data": window["has_older_data"],
            "processed_trades": len(frame),
            "candles": candles,
            "signals": signals,
            "viewport_metrics": viewport_metrics,
            "live_candle": None,
        }

    def _footprint_index_signature(
        self,
        *,
        provider_symbol: str,
        mt5_symbol: str,
        timeframe: str,
        tick_size: Decimal,
        bin_tick_count: int | Decimal,
    ) -> str:
        archive_versions = []
        for archive in self.catalog.archives_for_symbol(provider_symbol):
            try:
                stat = archive.path.stat()
                file_version = (int(stat.st_size), int(stat.st_mtime_ns))
            except OSError:
                file_version = (-1, -1)
            archive_versions.append(
                (
                    str(archive.path.resolve()),
                    file_version,
                    archive.query_start_ns,
                    archive.query_end_ns,
                    tuple(archive.dbn_members),
                )
            )
        trigger_config = (
            repr(self.trigger_engine.config)
            if self.trigger_engine is not None
            else ""
        )
        return FootprintCandleIndex.signature(
            (
                "footprint-candle-index-v1",
                provider_symbol.strip().upper(),
                mt5_symbol.strip().upper(),
                timeframe.strip().upper(),
                str(tick_size.normalize()),
                int(normalize_cme_bin_tick_count(bin_tick_count)),
                int(self.config.output_decimal_places),
                int(self.config.duration_unit_ms),
                int(self.config.session_start_hour_chicago),
                int(self.config.new_york_session_start_hour),
                int(self.config.new_york_session_start_minute),
                int(self.config.new_york_session_end_hour),
                int(self.config.new_york_session_end_minute),
                trigger_config,
                tuple(archive_versions),
            )
        )

    def _attach_cumulative_delta_fields(
        self,
        *,
        provider_symbol: str,
        timeframe: str,
        candles: list[dict[str, Any]],
        viewport_metrics: dict[str, int],
    ) -> None:
        cumulative_delta_reader = getattr(
            self.trade_store,
            "cumulative_contract_deltas",
            None,
        )
        cumulative_deltas = (
            cumulative_delta_reader(
                provider_symbol,
                candle_open_times_ms=[
                    int(candle["open_time_ms"])
                    for candle in candles
                ],
                interval_ms=TIMEFRAME_MS_BY_NAME[timeframe],
                timeframe=timeframe,
                session_start_hour_chicago=self.config.session_start_hour_chicago,
                new_york_session_start_hour=self.config.new_york_session_start_hour,
                new_york_session_start_minute=self.config.new_york_session_start_minute,
                new_york_session_end_hour=self.config.new_york_session_end_hour,
                new_york_session_end_minute=self.config.new_york_session_end_minute,
            )
            if callable(cumulative_delta_reader) and candles
            else {}
        )
        cumulative_cache_metrics = getattr(
            self.trade_store,
            "cumulative_delta_cache_metrics",
            lambda: {"hit_count": 0, "miss_count": 0},
        )()
        viewport_metrics["cumulative_delta_cache_hit_count"] += int(
            cumulative_cache_metrics.get("hit_count", 0)
        )
        viewport_metrics["cumulative_delta_cache_miss_count"] += int(
            cumulative_cache_metrics.get("miss_count", 0)
        )
        visible_running_delta = 0
        for candle in candles:
            visible_running_delta += int(candle.get("delta_contracts") or 0)
            metrics = cumulative_deltas.get(int(candle["open_time_ms"]), {})
            session_cumulative_delta = metrics.get("session_cumulative_delta")
            day_cumulative_delta = metrics.get("day_cumulative_delta")
            candle["session_cumulative_delta"] = (
                _format_number(session_cumulative_delta, 0)
                if session_cumulative_delta is not None
                else None
            )
            candle["day_cumulative_delta"] = _format_number(
                day_cumulative_delta
                if day_cumulative_delta is not None
                else visible_running_delta,
                0,
            )
            candle["trading_day"] = str(metrics.get("trading_day", ""))
            candle["ny_session_date"] = str(metrics.get("ny_session_date", ""))

    def _cached_chart_candles(
        self,
        frame: Any,
        *,
        provider_symbol: str,
        timeframe: str,
        metrics: dict[str, int],
    ) -> list[dict[str, Any]]:
        open_times = _frame_candle_open_times(frame, timeframe=timeframe)
        normalized_symbol = provider_symbol.strip().upper()
        normalized_timeframe = timeframe.strip().upper()
        cached_by_open: dict[int, dict[str, Any]] = {}
        missing_opens: list[int] = []
        with self._chart_cache_lock:
            for open_time_ms in open_times:
                key = (normalized_symbol, normalized_timeframe, open_time_ms)
                cached = self._chart_candle_cache.get(key)
                if cached is None:
                    missing_opens.append(open_time_ms)
                    continue
                self._chart_candle_cache.move_to_end(key)
                cached_by_open[open_time_ms] = cached

            metrics["cache_hit_count"] += len(cached_by_open)
            metrics["cache_miss_count"] += len(missing_opens)
            if missing_opens:
                missing_frame = _frame_for_candle_opens(
                    frame,
                    timeframe=timeframe,
                    open_times=missing_opens,
                )
                rebuilt = _chart_candles_from_frame(
                    missing_frame,
                    timeframe=timeframe,
                )
                metrics["candle_rebuild_count"] += len(rebuilt)
                for candle in rebuilt:
                    open_time_ms = int(candle["open_time_ms"])
                    key = (
                        normalized_symbol,
                        normalized_timeframe,
                        open_time_ms,
                    )
                    self._chart_candle_cache[key] = candle
                    self._chart_candle_cache.move_to_end(key)
                    cached_by_open[open_time_ms] = candle
                while len(self._chart_candle_cache) > self._candle_cache_size:
                    self._chart_candle_cache.popitem(last=False)

        return [
            dict(cached_by_open[open_time_ms])
            for open_time_ms in open_times
            if open_time_ms in cached_by_open
        ]

    @staticmethod
    def _merge_chart_candles_with_footprints(
        chart_candles: list[dict[str, Any]],
        footprint_candles: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        footprints_by_open = {
            int(candle.get("open_time_ms", 0)): candle
            for candle in footprint_candles
        }
        merged: list[dict[str, Any]] = []
        for chart_candle in chart_candles:
            open_time_ms = int(chart_candle.get("open_time_ms", 0))
            footprint_candle = footprints_by_open.get(open_time_ms)
            if footprint_candle is None:
                merged.append(dict(chart_candle))
                continue
            enriched = dict(footprint_candle)
            enriched.update(chart_candle)
            merged.append(enriched)
        return merged

    def _dom_positive_refill_outputs_by_candle_open(
        self,
        *,
        target_candles: list[dict[str, Any]],
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
    ) -> dict[int, list[dict[str, Any]]]:
        if self.engine_output_store is None or not target_candles:
            return {}
        normalized_timeframe = timeframe.strip().upper()
        timeframe_ms = int(TIMEFRAME_MS_BY_NAME[normalized_timeframe])
        open_times = {
            int(candle.get("open_time_ms", candle.get("open_time", 0)) or 0)
            for candle in target_candles
        }
        open_times.discard(0)
        if not open_times:
            return {}
        start_ms = min(open_times)
        end_ms = max(open_times) + timeframe_ms - 1
        outputs = self.engine_output_store.outputs(
            producer=DOM_ENGINE_PRODUCER,
            output_type=DOM_POSITIVE_REFILL_OUTPUT_TYPE,
            symbol=mt5_symbol,
            provider_symbol=provider_symbol,
            timeframe=normalized_timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        grouped: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for output in outputs:
            timestamp_ms = _payload_int(
                output,
                "marker_time_ms",
                "footprint_open_time_ms",
                "timestamp_ms",
                "event_time_ms",
            )
            if timestamp_ms <= 0:
                continue
            open_time_ms = (timestamp_ms // timeframe_ms) * timeframe_ms
            if open_time_ms in open_times:
                grouped[open_time_ms].append(dict(output))
        return dict(grouped)

    @staticmethod
    def _attach_dom_positive_refill_markers(
        *,
        target_candles: list[dict[str, Any]],
        outputs_by_open: Mapping[int, list[dict[str, Any]]],
    ) -> None:
        for candle in target_candles:
            open_time_ms = int(candle.get("open_time_ms", candle.get("open_time", 0)) or 0)
            outputs = outputs_by_open.get(open_time_ms, ())
            if not outputs:
                continue
            markers_by_id: dict[str, dict[str, Any]] = {}
            for existing in candle.get("dom_refill_markers", ()) or ():
                if isinstance(existing, Mapping):
                    marker = dict(existing)
                    marker_id = str(
                        marker.get("id")
                        or marker.get("output_id")
                        or marker.get("event_id")
                        or f"{marker.get('order_id')}|{marker.get('price')}|{marker.get('side')}"
                    )
                    markers_by_id[marker_id] = marker
            for output in outputs:
                marker = _dom_positive_refill_marker(output)
                markers_by_id[str(marker["id"])] = marker
            candle["dom_refill_markers"] = sorted(
                markers_by_id.values(),
                key=lambda item: (
                    _payload_int(item, "timestamp_ms", "event_time_ms"),
                    str(item.get("side") or ""),
                    str(item.get("price") or ""),
                    str(item.get("order_id") or ""),
                ),
            )

    @staticmethod
    def _dom_positive_refill_order_books_by_time(
        *,
        outputs_by_open: Mapping[int, list[dict[str, Any]]],
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
    ) -> dict[int, dict[str, Any]]:
        snapshots: dict[int, dict[str, Any]] = {}
        for open_time_ms, outputs in outputs_by_open.items():
            markers = [_dom_positive_refill_marker(output) for output in outputs]
            levels = [
                _dom_positive_refill_level(marker)
                for marker in markers
                if str(marker.get("price") or "").strip()
            ]
            snapshots[int(open_time_ms)] = {
                "type": "ENGINE_OUTPUT_DOM_POSITIVE_REFILL",
                "mt5_symbol": mt5_symbol,
                "symbol": provider_symbol,
                "provider_symbol": provider_symbol,
                "timeframe": timeframe.strip().upper(),
                "timestamp_ms": int(open_time_ms),
                "dom_refill_points": markers,
                "engine_outputs": {
                    "dom_positive_refills": list(outputs),
                },
                "events": markers,
                "raw_events": markers,
                "order_book_levels": levels,
            }
        return snapshots

    def _cached_footprint_candles(
        self,
        frame: Any,
        *,
        provider_symbol: str,
        mt5_symbol: str,
        timeframe: str,
        tick_size: Decimal,
        bin_tick_count: int | Decimal,
        output_decimal_places: int,
        duration_unit_ms: int,
        metrics: dict[str, int],
    ) -> list[dict[str, Any]]:
        open_times = _frame_candle_open_times(frame, timeframe=timeframe)
        normalized_symbol = provider_symbol.strip().upper()
        normalized_mt5_symbol = mt5_symbol.strip().upper()
        normalized_timeframe = timeframe.strip().upper()
        normalized_bin_ticks = int(normalize_cme_bin_tick_count(bin_tick_count))
        tick_key = str(tick_size.normalize())
        cached_by_open: dict[int, dict[str, Any]] = {}
        missing_opens: list[int] = []

        def cache_key(open_time_ms: int) -> tuple[str, str, str, str, int, int]:
            return (
                normalized_symbol,
                normalized_mt5_symbol,
                normalized_timeframe,
                tick_key,
                normalized_bin_ticks,
                int(open_time_ms),
            )

        with self._footprint_cache_lock:
            for open_time_ms in open_times:
                key = cache_key(open_time_ms)
                cached = self._footprint_candle_cache.get(key)
                if cached is None:
                    missing_opens.append(open_time_ms)
                    continue
                self._footprint_candle_cache.move_to_end(key)
                cached_by_open[open_time_ms] = cached

            metrics["cache_hit_count"] += len(cached_by_open)
            metrics["cache_miss_count"] += len(missing_opens)
            if missing_opens:
                missing_frame = _frame_for_candle_opens(
                    frame,
                    timeframe=timeframe,
                    open_times=missing_opens,
                )
                rebuilt = _footprint_candles_from_frame(
                    missing_frame,
                    provider_symbol=provider_symbol,
                    mt5_symbol=mt5_symbol,
                    timeframe=timeframe,
                    tick_size=tick_size,
                    bin_tick_count=normalized_bin_ticks,
                    output_decimal_places=output_decimal_places,
                    duration_unit_ms=duration_unit_ms,
                )
                metrics["footprint_rebuild_count"] += len(rebuilt)
                for candle in rebuilt:
                    open_time_ms = int(candle["open_time_ms"])
                    key = cache_key(open_time_ms)
                    self._footprint_candle_cache[key] = candle
                    self._footprint_candle_cache.move_to_end(key)
                    cached_by_open[open_time_ms] = candle
                while (
                    len(self._footprint_candle_cache)
                    > self._footprint_cache_size
                ):
                    self._footprint_candle_cache.popitem(last=False)

        return [
            dict(cached_by_open[open_time_ms])
            for open_time_ms in open_times
            if open_time_ms in cached_by_open
        ]

    def _attach_trigger_signals(
        self,
        frame: Any,
        *,
        target_candles: list[dict[str, Any]],
        analysis_candles: list[dict[str, Any]] | None = None,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        tick_size: Decimal,
        evaluation_time_ms: int,
        viewport_metrics: dict[str, int],
        order_books_by_time: Mapping[int, Any] | None = None,
    ) -> list[dict[str, Any]]:
        if self.trigger_engine is None or not self.trigger_engine.supports_timeframe(timeframe):
            return []

        analysis_frame = self._trigger_analysis_frame(
            frame,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            target_candles=target_candles,
        )
        if analysis_candles is None:
            analysis_candles = self._cached_footprint_candles(
                analysis_frame,
                provider_symbol=provider_symbol,
                mt5_symbol=mt5_symbol,
                timeframe=timeframe,
                tick_size=tick_size,
                bin_tick_count=self.trigger_engine.config.bin_tick_count,
                output_decimal_places=self.config.output_decimal_places,
                duration_unit_ms=self.config.duration_unit_ms,
                metrics=viewport_metrics,
            )
        signals = self.trigger_engine.enrich_candles(
            analysis_candles,
            evaluation_time_ms=evaluation_time_ms,
            order_books_by_time=order_books_by_time,
            record_closed_positions=True,
        )
        get_position_close_csv_recorder().record_signal_payloads(signals)
        target_open_times = {
            int(candle.get("open_time_ms", candle.get("open_time", 0)))
            for candle in target_candles
        }
        signals = [
            signal
            for signal in signals
            if int(signal["trigger_candle_time_ms"]) in target_open_times
        ]
        signals_by_open: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for signal in signals:
            signals_by_open[int(signal["trigger_candle_time_ms"])].append(signal)
        for candle in target_candles:
            open_time_ms = int(candle.get("open_time_ms", candle.get("open_time", 0)))
            candle["trigger_signals"] = list(signals_by_open.get(open_time_ms, ()))
        return signals

    def record_closed_position_history(
        self,
        *,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        tick_size: Decimal,
    ) -> int:
        if self.trigger_engine is None or not self.trigger_engine.supports_timeframe(timeframe):
            return 0

        normalized_timeframe = timeframe.strip().upper()
        interval_ms = TIMEFRAME_MS_BY_NAME[normalized_timeframe]
        start_ms = self.trade_store.earliest_partition_time_ms(provider_symbol)
        latest_available_end = getattr(self.trade_store, "latest_available_end_ms", None)
        end_ms = (
            latest_available_end(provider_symbol)
            if callable(latest_available_end)
            else None
        )
        if end_ms is None:
            latest_event_ms = self.trade_store.latest_event_time_ms(provider_symbol)
            end_ms = (
                ((latest_event_ms // interval_ms) + 1) * interval_ms
                if latest_event_ms is not None
                else None
            )
        if start_ms is None or end_ms is None or int(end_ms) <= int(start_ms):
            return 0

        history_trigger_engine = TriggerEngine(self.trigger_engine.config)
        first_day = date.fromisoformat(
            trading_day_for_timestamp_ms(
                int(start_ms),
                session_start_hour_chicago=self.config.session_start_hour_chicago,
            )
        )
        last_day = date.fromisoformat(
            trading_day_for_timestamp_ms(
                max(int(start_ms), int(end_ms) - 1),
                session_start_hour_chicago=self.config.session_start_hour_chicago,
            )
        )
        exit_count = 0
        carry_candle: dict[str, Any] | None = None
        current_day = first_day
        while current_day <= last_day:
            day_start_ms, day_end_ms = trading_day_bounds_utc_ms(
                current_day.isoformat(),
                session_start_hour_chicago=self.config.session_start_hour_chicago,
            )
            chunk_start_ms = max(int(start_ms), int(day_start_ms))
            chunk_end_ms = min(int(end_ms), int(day_end_ms))
            current_day += timedelta(days=1)
            if chunk_end_ms <= chunk_start_ms:
                continue
            frame = self.trade_store.trade_frame_for_time_range(
                provider_symbol,
                start_ms=chunk_start_ms,
                end_ms=chunk_end_ms,
                session_start_hour_chicago=self.config.session_start_hour_chicago,
            )
            if len(frame) == 0:
                continue
            metrics = _new_viewport_metrics()
            candles = self._cached_footprint_candles(
                frame,
                provider_symbol=provider_symbol,
                mt5_symbol=mt5_symbol,
                timeframe=normalized_timeframe,
                tick_size=tick_size,
                bin_tick_count=self.trigger_engine.config.bin_tick_count,
                output_decimal_places=self.config.output_decimal_places,
                duration_unit_ms=self.config.duration_unit_ms,
                metrics=metrics,
            )
            if carry_candle is not None:
                candles = [carry_candle, *candles]
            if not candles:
                carry_candle = None
                continue

            is_final_chunk = current_day > last_day
            evaluation_time_ms = (
                int(end_ms)
                if is_final_chunk
                else int(candles[-1].get("close_time_ms", candles[-1].get("close_time", 0)) or 0)
            )
            signals = history_trigger_engine.enrich_candles(
                candles,
                evaluation_time_ms=evaluation_time_ms,
                persist_state=True,
                record_closed_positions=True,
            )
            exit_count += sum(
                1
                for signal in signals
                if str(signal.get("signal_type") or "").strip().upper()
                in {"EXIT_BUY", "EXIT_SELL"}
            )
            carry_candle = None if is_final_chunk else dict(candles[-1])
        return exit_count

    def _trigger_analysis_frame(
        self,
        frame: Any,
        *,
        provider_symbol: str,
        timeframe: str,
        target_candles: list[dict[str, Any]],
    ) -> Any:
        if not target_candles:
            return frame
        interval_ms = TIMEFRAME_MS_BY_NAME[timeframe.strip().upper()]
        open_times = [
            int(value)
            for candle in target_candles
            if (value := candle.get("open_time_ms", candle.get("open_time"))) is not None
        ]
        close_times = [
            int(value)
            for candle in target_candles
            if (value := candle.get("close_time_ms", candle.get("close_time"))) is not None
        ]
        if not open_times:
            return frame
        start_ms = min(open_times) - 5 * interval_ms
        end_ms = max(close_times) + 1 if close_times else max(open_times) + interval_ms
        earliest_reader = getattr(self.trade_store, "earliest_partition_time_ms", None)
        if callable(earliest_reader):
            earliest_partition_ms = earliest_reader(provider_symbol)
            if earliest_partition_ms is not None:
                start_ms = max(int(earliest_partition_ms), start_ms)
        return self.trade_store.trade_frame_for_time_range(
            provider_symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            session_start_hour_chicago=self.config.session_start_hour_chicago,
        )

    def _new_york_session_profiles_for_window(
        self,
        frame: Any,
        *,
        provider_symbol: str,
        tick_size: Decimal,
    ) -> list[dict[str, Any]]:
        if len(frame) == 0 or self.volume_profile_engine is None:
            return []
        profiles = []
        for session_date in _new_york_session_dates_intersecting_frame(
            frame,
            config=self.config,
        ):
            cache_key = (
                provider_symbol.upper(),
                session_date,
                tick_size * CME_BIN_TICK_COUNT,
            )
            if cache_key not in self._new_york_session_profile_cache:
                self._new_york_session_profile_cache[cache_key] = (
                    self._build_new_york_session_profile(
                        provider_symbol=provider_symbol,
                        session_date=session_date,
                        tick_size=tick_size,
                    )
                )
            profile = self._new_york_session_profile_cache[cache_key]
            if profile is not None:
                profiles.append(profile)
        return profiles

    def _build_new_york_session_profile(
        self,
        *,
        provider_symbol: str,
        session_date: str,
        tick_size: Decimal,
    ) -> dict[str, Any] | None:
        session_start_ms, session_end_ms = new_york_session_bounds_utc_ms(
            session_date,
            start_hour=self.config.new_york_session_start_hour,
            start_minute=self.config.new_york_session_start_minute,
            end_hour=self.config.new_york_session_end_hour,
            end_minute=self.config.new_york_session_end_minute,
        )
        session_frame = self.trade_store.trade_frame_for_time_range(
            provider_symbol,
            start_ms=session_start_ms,
            end_ms=session_end_ms,
            session_start_hour_chicago=self.config.session_start_hour_chicago,
        )
        if len(session_frame) == 0:
            return None
        return _new_york_contract_profile_from_frame(
            session_frame,
            session_date=session_date,
            tick_size=tick_size,
            config=self.config,
        )

    def _frame_window(
        self,
        *,
        provider_symbol: str,
        timeframe: str,
        end_time_ms: int | None,
        candle_limit: int,
    ) -> dict[str, Any]:
        import pandas as pd

        interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
        normalized_limit = max(1, min(self.MAX_WINDOW_CANDLES, int(candle_limit)))
        latest_available_end = getattr(self.trade_store, "latest_available_end_ms", None)
        latest_end_ms = (
            latest_available_end(provider_symbol)
            if callable(latest_available_end)
            else None
        )
        if latest_end_ms is None:
            latest_event_ms = self.trade_store.latest_event_time_ms(provider_symbol)
            latest_end_ms = (
                ((latest_event_ms // interval_ms) + 1) * interval_ms
                if latest_event_ms is not None
                else None
            )
        if latest_end_ms is None:
            empty = pd.DataFrame(columns=["ts_event", "side", "price", "size", "symbol"])
            return {
                "frame": empty,
                "start_ms": 0,
                "end_ms": 0,
                "latest_end_ms": 0,
                "candle_limit": normalized_limit,
                "earliest_start_ms": 0,
                "has_older_data": False,
            }

        latest_end_ms = (int(latest_end_ms) // interval_ms) * interval_ms
        requested_end_ms = int(end_time_ms) if end_time_ms is not None else latest_end_ms
        requested_end_ms = max(interval_ms, min(requested_end_ms, latest_end_ms))
        requested_end_ms = (requested_end_ms // interval_ms) * interval_ms
        if requested_end_ms <= 0:
            requested_end_ms = latest_end_ms
        requested_start_ms = requested_end_ms - normalized_limit * interval_ms
        earliest_partition_ms = self.trade_store.earliest_partition_time_ms(provider_symbol)
        frame = self.trade_store.trade_frame_for_time_range(
            provider_symbol,
            start_ms=requested_start_ms,
            end_ms=requested_end_ms,
            session_start_hour_chicago=self.config.session_start_hour_chicago,
        )

        if len(frame) == 0 and end_time_ms is not None:
            previous_event_ms = self.trade_store.latest_event_time_ms(
                provider_symbol,
                before_ms=requested_end_ms,
            )
            if previous_event_ms is not None:
                requested_end_ms = ((previous_event_ms // interval_ms) + 1) * interval_ms
                requested_start_ms = requested_end_ms - normalized_limit * interval_ms
                frame = self.trade_store.trade_frame_for_time_range(
                    provider_symbol,
                    start_ms=requested_start_ms,
                    end_ms=requested_end_ms,
                    session_start_hour_chicago=self.config.session_start_hour_chicago,
                )

        for _ in range(8):
            candle_count = _frame_candle_count(frame, interval_ms=interval_ms)
            if candle_count >= normalized_limit:
                break
            previous_event_ms = self.trade_store.latest_event_time_ms(
                provider_symbol,
                before_ms=requested_start_ms,
            )
            if previous_event_ms is None:
                break
            missing_candles = normalized_limit - candle_count
            previous_candle_start_ms = (int(previous_event_ms) // interval_ms) * interval_ms
            expanded_start_ms = min(
                requested_start_ms - missing_candles * interval_ms,
                previous_candle_start_ms,
            )
            if earliest_partition_ms is not None:
                expanded_start_ms = max(int(earliest_partition_ms), expanded_start_ms)
            if expanded_start_ms >= requested_start_ms:
                break
            requested_start_ms = expanded_start_ms
            frame = self.trade_store.trade_frame_for_time_range(
                provider_symbol,
                start_ms=requested_start_ms,
                end_ms=requested_end_ms,
                session_start_hour_chicago=self.config.session_start_hour_chicago,
            )

        frame, visible_start_ms = _last_candle_frame(
            frame,
            interval_ms=interval_ms,
            candle_limit=normalized_limit,
        )
        if visible_start_ms is not None:
            requested_start_ms = visible_start_ms
        has_older_data = (
            earliest_partition_ms is not None and requested_start_ms > earliest_partition_ms
        )
        return {
            "frame": frame,
            "earliest_start_ms": int(earliest_partition_ms or requested_start_ms),
            "start_ms": requested_start_ms,
            "end_ms": requested_end_ms,
            "latest_end_ms": latest_end_ms,
            "candle_limit": normalized_limit,
            "has_older_data": has_older_data,
        }

    @staticmethod
    def _empty_page(
        *,
        mt5_symbol: str,
        provider_symbol: str,
        timeframe: str,
        tick_size: Decimal,
    ) -> dict[str, Any]:
        return {
            "mt5_symbol": mt5_symbol,
            "symbol": provider_symbol,
            "provider_symbol": provider_symbol,
            "market_provider": "CME_LOCAL_DBN",
            "quantity_unit": "CONTRACTS",
            "timeframe": timeframe,
            "interval": KLINE_INTERVAL_BY_INTERNAL[timeframe],
            "price_step": str(tick_size),
            "bin_tick_count": int(normalize_cme_bin_tick_count(CME_BIN_TICK_COUNT)),
            "fixed_bin_size": str(tick_size * CME_BIN_TICK_COUNT),
            "trading_day": "",
            "trading_days": [],
            "viewport_window": True,
            "earliest_window_start_ms": 0,
            "window_start_ms": 0,
            "window_end_ms": 0,
            "latest_window_end_ms": 0,
            "window_candle_limit": 0,
            "window_cursor_ms": 0,
            "has_older_data": False,
            "processed_trades": 0,
            "candles": [],
            "daily_volume_profiles": [],
            "live_candle": None,
        }


DBN_FIXED_PRICE_SCALE = 1_000_000_000


def _frame_candle_count(frame: Any, *, interval_ms: int) -> int:
    if len(frame) == 0:
        return 0
    interval_ns = int(interval_ms) * 1_000_000
    return int((frame["ts_event"] // interval_ns).nunique())


def _last_candle_frame(
    frame: Any,
    *,
    interval_ms: int,
    candle_limit: int,
) -> tuple[Any, int | None]:
    if len(frame) == 0:
        return frame, None
    interval_ns = int(interval_ms) * 1_000_000
    candle_buckets = frame["ts_event"] // interval_ns
    ordered_buckets = sorted(int(value) for value in candle_buckets.unique())
    if not ordered_buckets:
        return frame.iloc[0:0].copy(), None
    first_visible_index = max(
        0,
        len(ordered_buckets) - max(1, int(candle_limit)),
    )
    first_visible_bucket = ordered_buckets[first_visible_index]
    attrs = dict(frame.attrs)
    result = frame.loc[candle_buckets >= first_visible_bucket].copy()
    result.attrs.update(attrs)
    return result, first_visible_bucket * int(interval_ms)


def _merge_order_books_by_time(
    generated: Mapping[int, Any],
    explicit: Mapping[int, Any] | None,
) -> dict[int, Any] | None:
    if not generated:
        return dict(explicit) if explicit else None
    merged = {int(key): value for key, value in generated.items()}
    if explicit:
        merged.update({int(key): value for key, value in explicit.items()})
    return merged


def _dom_positive_refill_marker(output: Mapping[str, Any]) -> dict[str, Any]:
    source_event_time_ms = _payload_int(output, "timestamp_ms", "event_time_ms")
    timestamp_ms = _payload_int(output, "marker_time_ms", "footprint_open_time_ms") or source_event_time_ms
    marker_price = str(output.get("marker_price") or output.get("footprint_bin_low") or output.get("price") or "")
    refill_count = _payload_int(
        output,
        "price_base_refill_count",
        "positive_refill_count",
        "refill_count",
    )
    refill_total = _payload_int(
        output,
        "price_base_refill_contracts",
        "positive_refill_total",
        "refill_total",
        "refill_contracts",
    )
    refill_filled_total = _payload_int(
        output,
        "positive_refill_filled_total",
        "refill_filled_contracts",
        "executed_contracts",
    )
    executed_refill = min(refill_total, _payload_int(output, "executed_refill_contracts"))
    execution_rate = round(executed_refill / refill_total * 100.0 if refill_total > 0 else 0.0, 1)
    rate_label = f"{execution_rate:.1f}".rstrip("0").rstrip(".")
    output_id = str(output.get("id") or output.get("output_id") or "").strip()
    if not output_id:
        output_id = "|".join(
            (
                DOM_POSITIVE_REFILL_OUTPUT_TYPE,
                str(output.get("provider_symbol") or output.get("symbol") or ""),
                str(output.get("timeframe") or ""),
                str(timestamp_ms),
                str(output.get("price") or ""),
                str(output.get("side") or ""),
                str(output.get("order_id") or ""),
            )
        )
    return {
        "id": output_id,
        "output_id": output_id,
        "event_id": output_id,
        "type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
        "source": str(output.get("source") or DOM_POSITIVE_REFILL_OUTPUT_TYPE),
        "timestamp_ms": timestamp_ms,
        "event_time_ms": timestamp_ms,
        "source_event_time_ms": source_event_time_ms,
        "marker_time_ms": timestamp_ms,
        "marker_price": marker_price,
        "date": str(output.get("date") or ""),
        "symbol": str(output.get("symbol") or output.get("provider_symbol") or ""),
        "provider_symbol": str(output.get("provider_symbol") or output.get("symbol") or ""),
        "mt5_symbol": str(output.get("mt5_symbol") or ""),
        "timeframe": str(output.get("timeframe") or ""),
        "price": marker_price,
        "source_price": str(output.get("price") or ""),
        "side": _normalized_dom_side(output.get("side")),
        "order_id": str(output.get("order_id") or output.get("venue_order_id") or ""),
        "venue_order_id": str(output.get("venue_order_id") or output.get("order_id") or ""),
        "positive_refill_count": refill_count,
        "positive_refill_total": refill_total,
        "price_base_refill_count": refill_count,
        "price_base_refill_contracts": refill_total,
        "refill_added_contracts": refill_total,
        "executed_refill_contracts": executed_refill,
        "withdrawn_refill_contracts": _payload_int(output, "withdrawn_refill_contracts"),
        "refill_execution_rate": execution_rate,
        "refill_display": str(output.get("refill_display") or (
            f"{refill_count}({refill_total}) E{executed_refill} - {rate_label}%"
        )),
        "refill_method": "price_base_refill",
        "order_ids": tuple(output.get("order_ids") or ()),
        "order_count": _payload_int(output, "order_count"),
        "positive_refill_filled_total": refill_filled_total,
        "refill_count": refill_count,
        "refill_contracts": refill_total,
        "refill_total": refill_total,
        "refill_filled_contracts": refill_filled_total,
        "executed_contracts": _payload_int(output, "executed_contracts"),
        "market_buy": _payload_int(output, "market_buy"),
        "market_sell": _payload_int(output, "market_sell"),
        "market_buy_contracts": _payload_int(output, "market_buy_contracts"),
        "market_sell_contracts": _payload_int(output, "market_sell_contracts"),
        "ask_traded_contracts": _payload_int(output, "ask_traded_contracts"),
        "bid_traded_contracts": _payload_int(output, "bid_traded_contracts"),
        "footprint_open_time_ms": _payload_int(output, "footprint_open_time_ms"),
        "footprint_bin_low": str(output.get("footprint_bin_low") or ""),
        "footprint_bin_high": str(output.get("footprint_bin_high") or ""),
        "span_candles": _payload_int(output, "span_candles"),
    }


def _dom_positive_refill_level(marker: Mapping[str, Any]) -> dict[str, Any]:
    side = _normalized_dom_side(marker.get("side"))
    bid_contracts = 1 if side == "BID" else 0
    ask_contracts = 1 if side == "ASK" else 0
    refill_filled_total = _payload_int(
        marker,
        "positive_refill_filled_total",
        "refill_filled_contracts",
        "executed_contracts",
    )
    if refill_filled_total <= 0:
        refill_filled_total = _payload_int(
            marker,
            "positive_refill_total",
            "refill_total",
            "refill_contracts",
        )
    return {
        "price": str(marker.get("price") or ""),
        "bid_contracts": bid_contracts,
        "ask_contracts": ask_contracts,
        "top_order_id": str(marker.get("order_id") or ""),
        "top_order_side": side,
        "top_order_type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
        "top_order_positive_refill_count": _payload_int(
            marker,
            "price_base_refill_count",
            "positive_refill_count",
            "refill_count",
        ),
        "top_order_positive_refill_total": _payload_int(
            marker,
            "price_base_refill_contracts",
            "positive_refill_total",
            "refill_total",
            "refill_contracts",
        ),
        "price_base_refill_count": _payload_int(
            marker,
            "price_base_refill_count",
        ),
        "price_base_refill_contracts": _payload_int(
            marker,
            "price_base_refill_contracts",
        ),
        "refill_method": "price_base_refill",
        "top_order_positive_refill_filled_total": refill_filled_total,
    }


def _normalized_dom_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"B", "BUY", "BID"}:
        return "BID"
    if side in {"A", "SELL", "ASK"}:
        return "ASK"
    return side


def _payload_int(payload: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        raw_value = payload.get(key)
        if raw_value in {None, ""}:
            continue
        try:
            return int(raw_value or 0)
        except (TypeError, ValueError):
            continue
    return 0


def _new_viewport_metrics() -> dict[str, int]:
    return {
        "cache_hit_count": 0,
        "cache_miss_count": 0,
        "candle_rebuild_count": 0,
        "footprint_rebuild_count": 0,
        "cumulative_delta_cache_hit_count": 0,
        "cumulative_delta_cache_miss_count": 0,
    }


def _frame_candle_open_times(
    frame: Any,
    *,
    timeframe: str,
) -> list[int]:
    if len(frame) == 0:
        return []
    import numpy as np

    interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
    event_time_ms = (
        frame["ts_event"].to_numpy(dtype=np.uint64, copy=False)
        // 1_000_000
    )
    candle_open = (event_time_ms // interval_ms) * interval_ms
    return sorted(int(value) for value in np.unique(candle_open))


def _frame_for_candle_opens(
    frame: Any,
    *,
    timeframe: str,
    open_times: list[int],
) -> Any:
    if len(frame) == 0 or not open_times:
        return frame.iloc[0:0].copy()
    import numpy as np

    interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
    event_time_ms = (
        frame["ts_event"].to_numpy(dtype=np.uint64, copy=False)
        // 1_000_000
    )
    candle_open = (event_time_ms // interval_ms) * interval_ms
    attrs = dict(frame.attrs)
    result = frame.loc[np.isin(candle_open, open_times)].copy()
    result.attrs.update(attrs)
    return result


def _chart_candles_from_frame(frame: Any, *, timeframe: str) -> list[dict[str, Any]]:
    if len(frame) == 0:
        return []
    import numpy as np
    import pandas as pd

    interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
    event_time_ms = frame["ts_event"].to_numpy(dtype=np.uint64, copy=False) // 1_000_000
    price_fixed = frame["price"].to_numpy(dtype=np.int64, copy=False)
    candle_open = (event_time_ms // interval_ms) * interval_ms
    grouped = pd.DataFrame(
        {
            "open_time_ms": candle_open.astype(np.int64, copy=False),
            "price_fixed": price_fixed,
        }
    ).groupby("open_time_ms", sort=True)["price_fixed"].agg(["first", "max", "min", "last"])
    return [
        {
            "open_time_ms": int(open_time_ms),
            "close_time_ms": int(open_time_ms) + interval_ms - 1,
            "open_price": _format_fixed_price(row["first"]),
            "high_price": _format_fixed_price(row["max"]),
            "low_price": _format_fixed_price(row["min"]),
            "close_price": _format_fixed_price(row["last"]),
        }
        for open_time_ms, row in grouped.iterrows()
    ]


def _new_york_contract_profile_from_frame(
    frame: Any,
    *,
    session_date: str,
    tick_size: Decimal,
    config: CmeEngineConfig,
) -> dict[str, Any]:
    import numpy as np
    import pandas as pd

    fixed_bin_size = tick_size * CME_BIN_TICK_COUNT
    bin_size_fixed = _decimal_price_to_fixed(fixed_bin_size)
    price_fixed = frame["price"].to_numpy(dtype=np.int64, copy=False)
    size = frame["size"].to_numpy(dtype=np.int64, copy=False)
    side = frame["side"].astype(str).str.upper().to_numpy(copy=False)
    bins = np.floor_divide(price_fixed, bin_size_fixed)
    grouped = pd.DataFrame(
        {
            "bin_index": bins,
            "buy_contracts": np.where(side == "B", size, 0),
            "sell_contracts": np.where(side == "A", size, 0),
        }
    ).groupby("bin_index", sort=True)[["buy_contracts", "sell_contracts"]].sum()
    max_total_contracts = (
        int((grouped["buy_contracts"] + grouped["sell_contracts"]).max())
        if len(grouped)
        else 0
    )
    bin_payloads = []
    for bin_index, row in grouped.iterrows():
        buy_contracts = int(row["buy_contracts"])
        sell_contracts = int(row["sell_contracts"])
        total_contracts = buy_contracts + sell_contracts
        contract_delta = buy_contracts - sell_contracts
        low_fixed = int(bin_index) * bin_size_fixed
        bin_payloads.append(
            {
                "bin_index": int(bin_index),
                "price_low": _format_fixed_price(low_fixed),
                "price_high": _format_fixed_price(low_fixed + bin_size_fixed),
                "quantity_unit": "CONTRACTS",
                "buy_contracts": _format_number(buy_contracts, 0),
                "sell_contracts": _format_number(sell_contracts, 0),
                "total_contracts": _format_number(total_contracts, 0),
                "contract_delta": _format_number(contract_delta, 0),
                "normalized_contracts": (
                    total_contracts / max_total_contracts
                    if max_total_contracts > 0
                    else 0.0
                ),
                # Compatibility aliases; CME values are contract counts.
                "buy_volume": _format_number(buy_contracts, 0),
                "sell_volume": _format_number(sell_contracts, 0),
                "total_volume": _format_number(total_contracts, 0),
                "delta_volume": _format_number(contract_delta, 0),
                "normalized_volume": (
                    total_contracts / max_total_contracts
                    if max_total_contracts > 0
                    else 0.0
                ),
            }
        )
    session_start_ms, session_end_ms = new_york_session_bounds_utc_ms(
        session_date,
        start_hour=config.new_york_session_start_hour,
        start_minute=config.new_york_session_start_minute,
        end_hour=config.new_york_session_end_hour,
        end_minute=config.new_york_session_end_minute,
    )
    return {
        "trading_day": session_date,
        "display_session_date": session_date,
        "profile_session_date": session_date,
        "source_session_date": session_date,
        "ny_session_key": session_date,
        "session_name": "NEW_YORK_RTH",
        "session_timezone": "America/New_York",
        "session_start_utc_ms": session_start_ms,
        "session_end_utc_ms": session_end_ms,
        "source_session_start_utc_ms": session_start_ms,
        "source_session_end_utc_ms": session_end_ms,
        "fixed_bin_size": str(fixed_bin_size),
        "quantity_unit": "CONTRACTS",
        "max_total_contracts": _format_number(max_total_contracts, 0),
        "max_total_volume": _format_number(max_total_contracts, 0),
        "is_previous_session": False,
        "is_partial": False,
        "bins": bin_payloads,
    }


def _new_york_session_dates_intersecting_frame(
    frame: Any,
    *,
    config: CmeEngineConfig,
) -> list[str]:
    if len(frame) == 0:
        return []
    from datetime import UTC, date, datetime, time, timedelta
    from zoneinfo import ZoneInfo

    new_york = ZoneInfo("America/New_York")
    first_event_ms = int(frame["ts_event"].min()) // 1_000_000
    last_event_ms = int(frame["ts_event"].max()) // 1_000_000
    first_date = datetime.fromtimestamp(first_event_ms / 1000, tz=UTC).astimezone(new_york).date()
    last_date = datetime.fromtimestamp(last_event_ms / 1000, tz=UTC).astimezone(new_york).date()
    result = []
    current_date = first_date
    while current_date <= last_date:
        session_start_ms, session_end_ms = new_york_session_bounds_utc_ms(
            current_date.isoformat(),
            start_hour=config.new_york_session_start_hour,
            start_minute=config.new_york_session_start_minute,
            end_hour=config.new_york_session_end_hour,
            end_minute=config.new_york_session_end_minute,
        )
        if session_end_ms >= first_event_ms and session_start_ms <= last_event_ms:
            result.append(current_date.isoformat())
        current_date += timedelta(days=1)
    return result


def _trading_days_in_frame(
    frame: Any,
    *,
    session_start_hour_chicago: int,
) -> list[str]:
    if len(frame) == 0:
        return []
    return [
        trading_day
        for trading_day, day_frame in _window_frame_by_trading_day(
            frame,
            session_start_hour_chicago=session_start_hour_chicago,
        )
        if len(day_frame)
    ]


def _window_frame_by_trading_day(
    frame: Any,
    *,
    session_start_hour_chicago: int,
) -> list[tuple[str, Any]]:
    if len(frame) == 0:
        return []
    from datetime import date, timedelta

    first_event_ms = int(frame["ts_event"].min()) // 1_000_000
    last_event_ms = int(frame["ts_event"].max()) // 1_000_000
    first_day = date.fromisoformat(
        trading_day_for_timestamp_ms(
            first_event_ms,
            session_start_hour_chicago=session_start_hour_chicago,
        )
    )
    last_day = date.fromisoformat(
        trading_day_for_timestamp_ms(
            last_event_ms,
            session_start_hour_chicago=session_start_hour_chicago,
        )
    )
    result = []
    current_day = first_day
    while current_day <= last_day:
        trading_day = current_day.isoformat()
        start_ms, end_ms = trading_day_bounds_utc_ms(
            trading_day,
            session_start_hour_chicago=session_start_hour_chicago,
        )
        day_frame = frame.loc[
            (frame["ts_event"] >= start_ms * 1_000_000)
            & (frame["ts_event"] < end_ms * 1_000_000)
        ]
        if len(day_frame):
            result.append((trading_day, day_frame))
        current_day += timedelta(days=1)
    return result


def _footprint_candles_from_frame(
    frame: Any,
    *,
    provider_symbol: str,
    mt5_symbol: str,
    timeframe: str,
    tick_size: Decimal,
    bin_tick_count: int | Decimal = CME_BIN_TICK_COUNT,
    output_decimal_places: int,
    duration_unit_ms: int,
) -> list[dict[str, Any]]:
    if len(frame) == 0:
        return []
    import numpy as np
    import pandas as pd

    interval_ms = TIMEFRAME_MS_BY_NAME[timeframe]
    fixed_bin_size = tick_size * normalize_cme_bin_tick_count(bin_tick_count)
    bin_size_fixed = _decimal_price_to_fixed(fixed_bin_size)
    event_time_ms = (frame["ts_event"].to_numpy(dtype=np.uint64, copy=False) // 1_000_000).astype(
        np.int64,
        copy=False,
    )
    price_fixed = frame["price"].to_numpy(dtype=np.int64, copy=False)
    size = frame["size"].to_numpy(dtype=np.int64, copy=False)
    side = frame["side"].astype(str).str.upper().to_numpy(copy=False)
    candle_open = (event_time_ms // interval_ms) * interval_ms
    bin_indices = np.floor_divide(price_fixed, bin_size_fixed)
    work = pd.DataFrame(
        {
            "open_time_ms": candle_open,
            "event_time_ms": event_time_ms,
            "price_fixed": price_fixed,
            "bin_index": bin_indices,
            "contracts": size,
            "buy_contracts": np.where(side == "B", size, 0),
            "sell_contracts": np.where(side == "A", size, 0),
        }
    )
    candle_ohlc = work.groupby("open_time_ms", sort=True)["price_fixed"].agg(
        ["first", "max", "min", "last"]
    )
    bin_groups = work.groupby(["open_time_ms", "bin_index"], sort=True).agg(
        total_contracts=("contracts", "sum"),
        buy_contracts=("buy_contracts", "sum"),
        sell_contracts=("sell_contracts", "sum"),
        first_price=("price_fixed", "first"),
        last_price=("price_fixed", "last"),
        min_price=("price_fixed", "min"),
        max_price=("price_fixed", "max"),
    )
    duration_by_key = _duration_ms_by_candle_bin(
        candle_open=candle_open,
        bin_indices=bin_indices,
        event_time_ms=event_time_ms,
        interval_ms=interval_ms,
    )
    grouped_by_candle = {
        int(open_time): group.droplevel(0)
        for open_time, group in bin_groups.groupby(level=0, sort=True)
    }
    ordered_opens = list(candle_ohlc.index)
    payloads = []
    for ascending_position, open_time_ms in enumerate(ordered_opens):
        candle_number = len(ordered_opens) - ascending_position
        ohlc = candle_ohlc.loc[open_time_ms]
        candle_bins = grouped_by_candle.get(int(open_time_ms))
        if candle_bins is None or candle_bins.empty:
            continue
        states = {
            int(bin_index): {
                "total": int(row["total_contracts"]),
                "buy": int(row["buy_contracts"]),
                "sell": int(row["sell_contracts"]),
                "first_price": int(row["first_price"]),
                "last_price": int(row["last_price"]),
                "min_price": int(row["min_price"]),
                "max_price": int(row["max_price"]),
            }
            for bin_index, row in candle_bins.iterrows()
        }
        buy_contracts = sum(state["buy"] for state in states.values())
        sell_contracts = sum(state["sell"] for state in states.values())
        delta_contracts = buy_contracts - sell_contracts
        traded_bin_totals = [
            state["total"]
            for state in states.values()
            if state["total"] > 0
        ]
        median_bin_contracts = (
            Decimal(str(median(traded_bin_totals)))
            if traded_bin_totals
            else Decimal("0")
        )
        min_index = min(states)
        max_index = max(states)
        bins_payload = [
            _footprint_bin_payload(
                candle_number=candle_number,
                bin_index=bin_index,
                state=states.get(bin_index),
                states=states,
                median_bin_contracts=median_bin_contracts,
                duration_ms=duration_by_key.get((int(open_time_ms), bin_index), 0),
                bin_size_fixed=bin_size_fixed,
                output_decimal_places=output_decimal_places,
                duration_unit_ms=duration_unit_ms,
            )
            for bin_index in range(min_index, max_index + 1)
        ]
        spike_metrics = _add_contract_spike_metrics(
            bins_payload,
            output_decimal_places=output_decimal_places,
        )
        _add_efficiency_percentiles(
            bins_payload,
            output_decimal_places=output_decimal_places,
        )
        close_time_ms = int(open_time_ms) + interval_ms - 1
        payloads.append(
            {
                "candle_number": candle_number,
                "open_time": int(open_time_ms),
                "close_time": close_time_ms,
                "ohlc": {
                    "open": _format_fixed_price(ohlc["first"], output_decimal_places),
                    "high": _format_fixed_price(ohlc["max"], output_decimal_places),
                    "low": _format_fixed_price(ohlc["min"], output_decimal_places),
                    "close": _format_fixed_price(ohlc["last"], output_decimal_places),
                },
                "bins": bins_payload,
                "symbol": provider_symbol,
                "mt5_symbol": mt5_symbol,
                "provider_symbol": provider_symbol,
                "market_provider": "CME_LOCAL_DBN",
                "quantity_unit": "CONTRACTS",
                "timeframe": timeframe,
                "interval": KLINE_INTERVAL_BY_INTERNAL[timeframe],
                "price_step": str(tick_size),
                "bin_tick_count": int(normalize_cme_bin_tick_count(bin_tick_count)),
                "open_time_ms": int(open_time_ms),
                "close_time_ms": close_time_ms,
                "open_price": _format_fixed_price(ohlc["first"], output_decimal_places),
                "high_price": _format_fixed_price(ohlc["max"], output_decimal_places),
                "low_price": _format_fixed_price(ohlc["min"], output_decimal_places),
                "close_price": _format_fixed_price(ohlc["last"], output_decimal_places),
                "buy_contracts": _format_number(buy_contracts, 0),
                "sell_contracts": _format_number(sell_contracts, 0),
                "delta_contracts": _format_number(delta_contracts, 0),
                "median_bin_contracts": _format_decimal(median_bin_contracts, 1),
                "median_bin_volume": _format_decimal(median_bin_contracts, 1),
                "contract_spike_p75": _format_decimal(
                    spike_metrics.p75,
                    output_decimal_places,
                ),
                "contract_spike_normal_median": _format_decimal(
                    spike_metrics.normal_median,
                    output_decimal_places,
                ),
                "contract_spike_normal_mad": _format_decimal(
                    spike_metrics.normal_mad,
                    output_decimal_places,
                ),
                "contract_spike_score_deviation": _format_decimal(
                    spike_metrics.score_deviation,
                    output_decimal_places,
                ),
            }
        )
    return payloads


def _duration_ms_by_candle_bin(
    *,
    candle_open: Any,
    bin_indices: Any,
    event_time_ms: Any,
    interval_ms: int,
) -> dict[tuple[int, int], int]:
    import numpy as np

    if len(event_time_ms) == 0:
        return {}
    segment_start_mask = np.empty(len(event_time_ms), dtype=bool)
    segment_start_mask[0] = True
    segment_start_mask[1:] = (candle_open[1:] != candle_open[:-1]) | (
        bin_indices[1:] != bin_indices[:-1]
    )
    starts = np.flatnonzero(segment_start_mask)
    durations: dict[tuple[int, int], int] = {}
    for position, start_index in enumerate(starts):
        open_time = int(candle_open[start_index])
        bin_index = int(bin_indices[start_index])
        if position + 1 < len(starts) and int(candle_open[starts[position + 1]]) == open_time:
            end_time = int(event_time_ms[starts[position + 1]])
        else:
            end_time = open_time + interval_ms - 1
        duration = max(0, end_time - int(event_time_ms[start_index]))
        key = (open_time, bin_index)
        durations[key] = durations.get(key, 0) + duration
    return durations


def _footprint_bin_payload(
    *,
    candle_number: int,
    bin_index: int,
    state: dict[str, int] | None,
    states: dict[int, dict[str, int]],
    median_bin_contracts: Decimal,
    duration_ms: int,
    bin_size_fixed: int,
    output_decimal_places: int,
    duration_unit_ms: int,
) -> dict[str, Any]:
    state = state or {
        "total": 0,
        "buy": 0,
        "sell": 0,
        "first_price": 0,
        "last_price": 0,
        "min_price": 0,
        "max_price": 0,
    }
    buy_contracts = state["buy"]
    sell_contracts = state["sell"]
    lower_sell = states.get(bin_index - 1, {}).get("sell", 0)
    upper_buy = states.get(bin_index + 1, {}).get("buy", 0)
    buy_ratio = (
        Decimal(buy_contracts) / Decimal(max(lower_sell, 1))
    )
    sell_ratio = (
        Decimal(sell_contracts) / Decimal(max(upper_buy, 1))
    )
    buy_diagonal_contract_delta = buy_contracts - lower_sell
    sell_diagonal_contract_delta = sell_contracts - upper_buy
    total_contracts = state["total"]
    has_median_volume = median_bin_contracts > 0
    abnormal_buy_imbalance = (
        has_median_volume
        and buy_ratio >= Decimal("3")
        and Decimal(total_contracts) >= median_bin_contracts
    )
    abnormal_sell_imbalance = (
        has_median_volume
        and sell_ratio >= Decimal("3")
        and Decimal(total_contracts) >= median_bin_contracts
    )
    if buy_ratio > sell_ratio:
        dominant_side = "BUY"
        dominant_contracts = buy_contracts
    elif sell_ratio > buy_ratio:
        dominant_side = "SELL"
        dominant_contracts = sell_contracts
    else:
        dominant_side = "NONE"
        dominant_contracts = 0
    has_trades = state["total"] > 0
    progress_fixed = state["max_price"] - state["min_price"] if has_trades else 0
    efficiency = (
        (Decimal(progress_fixed) / Decimal(DBN_FIXED_PRICE_SCALE)) / Decimal(total_contracts)
        if total_contracts > 0
        else None
    )
    low_fixed = bin_index * bin_size_fixed
    return {
        "bin_id": f"{candle_number}_{_format_fixed_price(low_fixed)}",
        "index": bin_index,
        "low": _format_fixed_price(low_fixed, output_decimal_places),
        "high": _format_fixed_price(low_fixed + bin_size_fixed, output_decimal_places),
        "quantity_unit": "CONTRACTS",
        "abnormal_buy_imbalance": abnormal_buy_imbalance,
        "abnormal_sell_imbalance": abnormal_sell_imbalance,
        "l2": {
            "quantity_unit": "CONTRACTS",
            "total_contracts": _format_number(state["total"], 0),
            "candle_median_bin_contracts": _format_decimal(median_bin_contracts, 1),
            "candle_median_bin_volume": _format_decimal(median_bin_contracts, 1),
            "abnormal_buy_imbalance": abnormal_buy_imbalance,
            "abnormal_sell_imbalance": abnormal_sell_imbalance,
            "contract_delta": _format_number(
                buy_contracts - sell_contracts,
                0,
            ),
            "horizontal_contract_delta": _format_number(
                buy_contracts - sell_contracts,
                0,
            ),
            "buy_contracts": _format_number(buy_contracts, 0),
            "sell_contracts": _format_number(sell_contracts, 0),
            "ask_traded_contracts": _format_number(
                buy_contracts,
                0,
            ),
            "bid_traded_contracts": _format_number(
                sell_contracts,
                0,
            ),
            "buy_diagonal_contract_delta": _format_number(
                buy_diagonal_contract_delta,
                0,
            ),
            "sell_diagonal_contract_delta": _format_number(
                sell_diagonal_contract_delta,
                0,
            ),
            "buy_diagonal_contract_ratio": _format_decimal(
                buy_ratio,
                output_decimal_places,
            ),
            "sell_diagonal_contract_ratio": _format_decimal(
                sell_ratio,
                output_decimal_places,
            ),
            "dominant_side_contracts": _format_number(
                dominant_contracts,
                0,
            ),
            # Compatibility aliases; CME values are contract counts.
            "total_volume": _format_number(state["total"], 0),
            "delta": _format_number(buy_contracts - sell_contracts, 0),
            "horizontal_delta": _format_number(
                buy_contracts - sell_contracts,
                0,
            ),
            "ask_traded_volume": _format_number(buy_contracts, 0),
            "bid_traded_volume": _format_number(sell_contracts, 0),
            "buy_diagonal_imbalance_ratio": _format_decimal(buy_ratio, output_decimal_places),
            "sell_diagonal_imbalance_ratio": _format_decimal(sell_ratio, output_decimal_places),
            "duration": _format_decimal(
                Decimal(duration_ms) / Decimal(max(1, duration_unit_ms)),
                output_decimal_places,
            ),
            "min_trade_price_in_bin": (
                _format_fixed_price(state["min_price"], output_decimal_places) if has_trades else None
            ),
            "max_trade_price_in_bin": (
                _format_fixed_price(state["max_price"], output_decimal_places) if has_trades else None
            ),
            "price_progress_in_bin": (
                _format_fixed_price(progress_fixed, output_decimal_places) if has_trades else None
            ),
            "dominant_diagonal_side": dominant_side,
            "dominant_side_volume": _format_number(
                dominant_contracts,
                0,
            ),
            "dominant_side_efficiency": (
                _format_decimal(efficiency, output_decimal_places) if efficiency is not None else None
            ),
            "efficiency_percentile": None,
        },
    }


def _add_contract_spike_metrics(
    bins_payload: list[dict[str, Any]],
    *,
    output_decimal_places: int,
):
    totals = [
        Decimal(
            str(
                item.get("l2", {}).get(
                    "total_contracts",
                    item.get("l2", {}).get("total_volume", 0),
                )
            )
        )
        for item in bins_payload
    ]
    metrics = calculate_contract_spike_metrics(totals)
    for item, score in zip(bins_payload, metrics.scores):
        abnormal_contract = is_contract_spike(score)
        formatted_score = _format_decimal(score, output_decimal_places)
        item["contract_spike_score"] = formatted_score
        item["abnormal_contract"] = abnormal_contract
        item["abnormal_volume"] = abnormal_contract
        l2_payload = item.setdefault("l2", {})
        l2_payload["contract_spike_score"] = formatted_score
        l2_payload["abnormal_contract"] = abnormal_contract
        l2_payload["abnormal_volume"] = abnormal_contract
    return metrics


def _add_efficiency_percentiles(
    bins_payload: list[dict[str, Any]],
    *,
    output_decimal_places: int,
) -> None:
    positions = []
    values = []
    for position, item in enumerate(bins_payload):
        raw_efficiency = item.get("l2", {}).get("dominant_side_efficiency")
        if raw_efficiency is None:
            continue
        positions.append(position)
        values.append(Decimal(str(raw_efficiency)))
    for position, percentile in zip(positions, _percentile_ranks(values)):
        bins_payload[position]["l2"]["efficiency_percentile"] = _format_decimal(
            percentile,
            output_decimal_places,
        )


def _percentile_ranks(values: list[Decimal]) -> list[Decimal]:
    if not values:
        return []
    if len(values) == 1:
        return [Decimal("1")]
    sorted_values = sorted(values)
    denominator = Decimal(len(sorted_values) - 1)
    ranks = []
    for value in values:
        lower = sorted_values.index(value)
        upper = len(sorted_values) - 1 - list(reversed(sorted_values)).index(value)
        ranks.append((Decimal(lower + upper) / Decimal("2")) / denominator)
    return ranks


def _decimal_price_to_fixed(value: Decimal) -> int:
    return int(value * Decimal(DBN_FIXED_PRICE_SCALE))


def _format_fixed_price(value: Any, places: int = 3) -> str:
    decimal_value = Decimal(int(value)) / Decimal(DBN_FIXED_PRICE_SCALE)
    return f"{decimal_value:.{places}f}"


def _format_number(value: int, places: int = 3) -> str:
    return f"{Decimal(int(value)):.{places}f}"


def _format_decimal(value: Decimal, places: int) -> str:
    return f"{value:.{places}f}"


@dataclass
class _CandleAccumulator:
    open_time_ms: int
    open_price: Decimal | None = None
    high_price: Decimal | None = None
    low_price: Decimal | None = None
    close_price: Decimal | None = None

    @property
    def is_valid(self) -> bool:
        return (
            self.open_price is not None
            and self.high_price is not None
            and self.low_price is not None
            and self.close_price is not None
        )

    def apply_trade(self, trade: AggTradeEvent) -> None:
        price = Decimal(str(trade.price))
        if self.open_price is None:
            self.open_price = price
            self.high_price = price
            self.low_price = price
            self.close_price = price
            return
        self.high_price = max(self.high_price or price, price)
        self.low_price = min(self.low_price or price, price)
        self.close_price = price
