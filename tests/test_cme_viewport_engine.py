from __future__ import annotations

import unittest
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import numpy as np
import pandas as pd

from cme_provider.engines import (
    CME_BIN_TICK_COUNT,
    CmeEngineConfig,
    CmePagedHistoryEngine,
    _footprint_candles_from_frame,
    _last_candle_frame,
    _new_york_contract_profile_from_frame,
)
from cme_provider.local_data import (
    CmeLocalDbnTradeStore,
    _trade_frame_from_dbn_store,
    new_york_session_bounds_utc_ms,
)
from core.contract_spike import calculate_contract_spike_metrics
from core.engine_output_bus import (
    DOM_ENGINE_PRODUCER,
    DOM_POSITIVE_REFILL_OUTPUT_TYPE,
    EngineOutputStore,
)
from triggerEngine import TriggerConfig, TriggerEngine


class _WindowOnlyTradeStore:
    def __init__(self, frame: pd.DataFrame, earliest_ms: int) -> None:
        self.frame = frame
        self.earliest_ms = earliest_ms
        self.requests: list[tuple[int, int]] = []

    def latest_event_time_ms(
        self,
        provider_symbol: str,
        *,
        before_ms: int | None = None,
    ) -> int | None:
        del provider_symbol
        values = self.frame["ts_event"] // 1_000_000
        if before_ms is not None:
            values = values.loc[values < before_ms]
        return int(values.max()) if len(values) else None

    def earliest_partition_time_ms(self, provider_symbol: str) -> int:
        del provider_symbol
        return self.earliest_ms

    def trade_frame_for_time_range(
        self,
        provider_symbol: str,
        *,
        start_ms: int,
        end_ms: int,
        session_start_hour_chicago: int,
    ) -> pd.DataFrame:
        del provider_symbol, session_start_hour_chicago
        self.requests.append((start_ms, end_ms))
        result = self.frame.loc[
            (self.frame["ts_event"] >= start_ms * 1_000_000)
            & (self.frame["ts_event"] < end_ms * 1_000_000)
        ].copy()
        result.attrs["contract_symbol"] = "NQM6"
        result.attrs["contract_symbols"] = ("NQM6",)
        return result

    def trade_frame_for_trading_day(self, *args, **kwargs):
        raise AssertionError("A viewport request must not load a full trading day")


class CmeViewportEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.base_ms = 1_767_225_600_000
        rows = []
        for minute in range(10):
            for offset_ms, side in ((5_000, "A"), (35_000, "B")):
                rows.append(
                    {
                        "ts_event": (self.base_ms + minute * 60_000 + offset_ms) * 1_000_000,
                        "side": side,
                        "price": (16_000 + minute) * 1_000_000_000,
                        "size": minute + 1,
                        "symbol": "NQM6",
                    }
                )
        frame = pd.DataFrame(rows)
        self.store = _WindowOnlyTradeStore(frame, self.base_ms)
        self.engine = CmePagedHistoryEngine(
            catalog=None,
            trade_store=self.store,
            candle_engine=None,
            footprint_engine=None,
            volume_profile_engine=None,
            config=CmeEngineConfig(session_start_hour_chicago=17),
        )

    def test_chart_engine_processes_only_requested_viewport(self) -> None:
        payload = self.engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=3,
        )

        expected_end = self.base_ms + 10 * 60_000
        self.assertEqual(self.store.requests, [(expected_end - 3 * 60_000, expected_end)])
        self.assertEqual(payload["window_candle_limit"], 3)
        self.assertEqual(payload["earliest_window_start_ms"], self.base_ms)
        self.assertEqual(payload["processed_trades"], 6)
        self.assertEqual(len(payload["candles"]), 3)
        self.assertEqual(payload["quantity_unit"], "CONTRACTS")
        self.assertEqual(payload["daily_volume_profiles"], [])
        self.assertTrue(payload["candles"][0]["bins"])
        self.assertIn("contract_spike_score", payload["candles"][0]["bins"][0])

    def test_chart_engine_can_skip_deferred_profiles(self) -> None:
        self.engine._new_york_session_profiles_for_window = lambda *args, **kwargs: self.fail(
            "Fast candle viewport must not calculate volume profiles"
        )

        payload = self.engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=3,
            include_profiles=False,
        )

        self.assertEqual(payload["daily_volume_profiles"], [])

    def test_chart_window_attaches_cumulative_delta_rows(self) -> None:
        def cumulative_contract_deltas(*args, **kwargs):
            del args
            return {
                int(open_time): {
                    "session_cumulative_delta": 100 + index,
                    "day_cumulative_delta": 200 + index,
                    "trading_day": "2026-01-02",
                    "ny_session_date": "2026-01-02",
                }
                for index, open_time in enumerate(kwargs["candle_open_times_ms"])
            }

        self.store.cumulative_contract_deltas = cumulative_contract_deltas
        self.store.cumulative_delta_cache_metrics = lambda: {
            "hit_count": 2,
            "miss_count": 1,
        }

        payload = self.engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=3,
            include_profiles=False,
        )

        self.assertEqual(payload["candles"][0]["session_cumulative_delta"], "100")
        self.assertEqual(payload["candles"][0]["day_cumulative_delta"], "200")
        self.assertEqual(payload["candles"][2]["session_cumulative_delta"], "102")
        self.assertEqual(payload["candles"][2]["day_cumulative_delta"], "202")
        self.assertEqual(payload["candles"][0]["trading_day"], "2026-01-02")
        self.assertEqual(payload["candles"][0]["ny_session_date"], "2026-01-02")
        self.assertEqual(
            payload["viewport_metrics"]["cumulative_delta_cache_hit_count"],
            2,
        )
        self.assertEqual(
            payload["viewport_metrics"]["cumulative_delta_cache_miss_count"],
            1,
        )

    def test_chart_window_attaches_dom_engine_outputs_to_candles(self) -> None:
        output_store = EngineOutputStore()
        output_store.publish_many(
            (
                {
                    "producer": DOM_ENGINE_PRODUCER,
                    "type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
                    "symbol": "NQ.FUT",
                    "provider_symbol": "NQ.FUT",
                    "mt5_symbol": "NQ",
                    "timeframe": "M1",
                    "timestamp_ms": self.base_ms + 7 * 60_000 + 5_000,
                    "event_time_ms": self.base_ms + 7 * 60_000 + 5_000,
                    "price": "16007",
                    "side": "BID",
                    "order_id": "DOM-REFILL-1",
                    "positive_refill_count": 10,
                    "positive_refill_total": 215,
                    "positive_refill_filled_total": 40,
                    "executed_contracts": 40,
                },
            )
        )
        engine = CmePagedHistoryEngine(
            catalog=None,
            trade_store=self.store,
            candle_engine=None,
            footprint_engine=None,
            volume_profile_engine=None,
            config=CmeEngineConfig(session_start_hour_chicago=17),
            engine_output_store=output_store,
        )

        payload = engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=3,
            include_profiles=False,
        )

        marker = payload["candles"][0]["dom_refill_markers"][0]
        self.assertEqual(marker["type"], DOM_POSITIVE_REFILL_OUTPUT_TYPE)
        self.assertEqual(marker["order_id"], "DOM-REFILL-1")
        self.assertEqual(marker["price"], "16007")
        self.assertEqual(marker["side"], "BID")
        self.assertEqual(marker["positive_refill_count"], 10)
        self.assertEqual(marker["positive_refill_total"], 215)
        self.assertEqual(marker["positive_refill_filled_total"], 40)
        self.assertEqual(marker["refill_filled_contracts"], 40)

    def test_adjacent_chart_viewport_rebuilds_only_new_candle(self) -> None:
        first = self.engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=3,
            include_profiles=False,
        )
        second = self.engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            end_time_ms=self.base_ms + 9 * 60_000,
            candle_limit=3,
            include_profiles=False,
        )

        self.assertEqual(first["viewport_metrics"]["candle_rebuild_count"], 3)
        self.assertEqual(second["viewport_metrics"]["cache_hit_count"], 4)
        self.assertEqual(second["viewport_metrics"]["cache_miss_count"], 2)
        self.assertEqual(second["viewport_metrics"]["candle_rebuild_count"], 1)
        self.assertEqual(second["viewport_metrics"]["footprint_rebuild_count"], 1)

    def test_footprint_engine_uses_independent_window(self) -> None:
        end_ms = self.base_ms + 7 * 60_000
        payload = self.engine.footprint_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            end_time_ms=end_ms,
            candle_limit=2,
        )

        self.assertEqual(self.store.requests, [(end_ms - 2 * 60_000, end_ms)])
        self.assertEqual(payload["processed_trades"], 4)
        self.assertEqual(len(payload["candles"]), 2)
        self.assertEqual(payload["window_start_ms"], end_ms - 2 * 60_000)
        self.assertEqual(payload["window_end_ms"], end_ms)
        self.assertEqual(payload["candles"][0]["delta_contracts"], "0")
        self.assertEqual(payload["candles"][1]["delta_contracts"], "0")
        self.assertIsNone(payload["candles"][0]["session_cumulative_delta"])
        self.assertEqual(payload["candles"][1]["day_cumulative_delta"], "0")

    def test_adjacent_footprint_viewport_rebuilds_only_new_candle(self) -> None:
        first = self.engine.footprint_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=2,
        )
        second = self.engine.footprint_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            end_time_ms=self.base_ms + 9 * 60_000,
            candle_limit=2,
        )

        self.assertEqual(first["viewport_metrics"]["footprint_rebuild_count"], 2)
        self.assertEqual(second["viewport_metrics"]["cache_hit_count"], 1)
        self.assertEqual(second["viewport_metrics"]["cache_miss_count"], 1)
        self.assertEqual(second["viewport_metrics"]["footprint_rebuild_count"], 1)

    def test_footprint_engine_accepts_configurable_bin_ticks(self) -> None:
        payload = self.engine.footprint_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=2,
            bin_tick_count=8,
        )

        self.assertEqual(payload["bin_tick_count"], 8)
        self.assertEqual(payload["fixed_bin_size"], "2.00")

    def test_footprint_engine_accepts_one_tick_bins(self) -> None:
        payload = self.engine.footprint_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            candle_limit=2,
            bin_tick_count=1,
        )

        self.assertEqual(payload["bin_tick_count"], 1)
        self.assertEqual(payload["fixed_bin_size"], "0.25")

    def test_chart_window_fills_visible_candles_across_market_gap(self) -> None:
        rows = []
        for minute in (0, 1, 100, 101):
            for offset_ms, side in ((5_000, "A"), (35_000, "B")):
                rows.append(
                    {
                        "ts_event": (self.base_ms + minute * 60_000 + offset_ms) * 1_000_000,
                        "side": side,
                        "price": (16_000 + minute) * 1_000_000_000,
                        "size": 1,
                        "symbol": "NQM6",
                    }
                )
        store = _WindowOnlyTradeStore(pd.DataFrame(rows), self.base_ms)
        engine = CmePagedHistoryEngine(
            catalog=None,
            trade_store=store,
            candle_engine=None,
            footprint_engine=None,
            volume_profile_engine=None,
            config=CmeEngineConfig(session_start_hour_chicago=17),
        )
        end_ms = self.base_ms + 102 * 60_000

        payload = engine.chart_window(
            mt5_symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            end_time_ms=end_ms,
            candle_limit=3,
        )

        self.assertEqual(
            store.requests,
            [
                (self.base_ms + 99 * 60_000, end_ms),
                (self.base_ms + 1 * 60_000, end_ms),
            ],
        )
        self.assertEqual(len(payload["candles"]), 3)
        self.assertEqual(
            [item["open_time_ms"] for item in payload["candles"]],
            [
                self.base_ms + 1 * 60_000,
                self.base_ms + 100 * 60_000,
                self.base_ms + 101 * 60_000,
            ],
        )
        self.assertEqual(payload["processed_trades"], 6)
        self.assertEqual(payload["window_start_ms"], self.base_ms + 1 * 60_000)

    def test_last_candle_frame_accepts_limit_larger_than_available_buckets(self) -> None:
        frame = self.store.frame.loc[
            self.store.frame["ts_event"]
            < (self.base_ms + 2 * 60_000) * 1_000_000
        ].copy()

        visible, visible_start_ms = _last_candle_frame(
            frame,
            interval_ms=60_000,
            candle_limit=80,
        )

        self.assertEqual(len(visible), 4)
        self.assertEqual(visible_start_ms, self.base_ms)

    def test_concurrent_m5_chart_requests_do_not_rebuild_trigger_footprints_twice(self) -> None:
        engine = CmePagedHistoryEngine(
            catalog=None,
            trade_store=self.store,
            candle_engine=None,
            footprint_engine=None,
            volume_profile_engine=None,
            config=CmeEngineConfig(session_start_hour_chicago=17),
            trigger_engine=TriggerEngine(TriggerConfig()),
        )
        original_builder = _footprint_candles_from_frame

        def slow_builder(*args, **kwargs):
            time.sleep(0.05)
            return original_builder(*args, **kwargs)

        def load():
            return engine.chart_window(
                mt5_symbol="NQ",
                provider_symbol="NQ.FUT",
                timeframe="M5",
                tick_size=Decimal("0.25"),
                candle_limit=3,
                include_profiles=False,
            )

        with patch(
            "cme_provider.engines._footprint_candles_from_frame",
            side_effect=slow_builder,
        ):
            with ThreadPoolExecutor(max_workers=2) as executor:
                payloads = list(executor.map(lambda _: load(), range(2)))

        metrics = [payload["viewport_metrics"] for payload in payloads]
        self.assertEqual(
            sum(item["candle_rebuild_count"] for item in metrics),
            2,
        )
        self.assertEqual(
            sum(item["footprint_rebuild_count"] for item in metrics),
            4,
        )
        self.assertGreaterEqual(
            sum(item["cache_hit_count"] for item in metrics),
            6,
        )

    def test_dbn_reader_maps_only_outright_contracts(self) -> None:
        dtype = np.dtype(
            [
                ("instrument_id", "<u4"),
                ("ts_event", "<u8"),
                ("side", "S1"),
                ("price", "<i8"),
                ("size", "<u4"),
            ]
        )
        records = np.array(
            [
                (101, 1_000_000, b"A", 16_000_000_000_000, 2),
                (202, 2_000_000, b"B", 16_001_000_000_000, 3),
            ],
            dtype=dtype,
        )

        class _DbnStore:
            mappings = {
                "NQM6": [{"symbol": "101"}],
                "NQM6-NQU6": [{"symbol": "202"}],
            }

            def to_ndarray(self, schema: str):
                self.schema = schema
                return records

            def to_df(self, **kwargs):
                raise AssertionError("Fast outright mapping should avoid mapped DataFrame conversion")

        frame = _trade_frame_from_dbn_store(_DbnStore(), provider_symbol="NQ.FUT")

        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["symbol"], "NQM6")
        self.assertEqual(frame.iloc[0]["side"], "A")

    def test_new_york_session_bounds_are_dst_aware(self) -> None:
        start_ms, end_ms = new_york_session_bounds_utc_ms("2026-06-08")

        self.assertEqual(
            datetime.fromtimestamp(start_ms / 1000, tz=UTC).isoformat(),
            "2026-06-08T13:30:00+00:00",
        )
        self.assertEqual(
            datetime.fromtimestamp(end_ms / 1000, tz=UTC).isoformat(),
            "2026-06-08T20:00:00+00:00",
        )

    def test_cumulative_contract_delta_resets_for_each_trading_day_and_ny_session(self) -> None:
        daily_before_1 = int(datetime(2026, 6, 8, 21, 59, tzinfo=UTC).timestamp() * 1000)
        daily_after_1 = int(datetime(2026, 6, 8, 22, 0, tzinfo=UTC).timestamp() * 1000)
        ny_before = int(datetime(2026, 6, 9, 13, 29, tzinfo=UTC).timestamp() * 1000)
        ny_after = int(datetime(2026, 6, 9, 13, 30, tzinfo=UTC).timestamp() * 1000)
        ny_last = int(datetime(2026, 6, 9, 19, 59, tzinfo=UTC).timestamp() * 1000)
        ny_closed = int(datetime(2026, 6, 9, 20, 0, tzinfo=UTC).timestamp() * 1000)
        daily_before_2 = int(datetime(2026, 6, 9, 21, 59, tzinfo=UTC).timestamp() * 1000)
        daily_after_2 = int(datetime(2026, 6, 9, 22, 0, tzinfo=UTC).timestamp() * 1000)
        candle_rows = [
            (daily_before_1, "A", 4),
            (daily_after_1, "B", 3),
            (ny_before, "B", 2),
            (ny_after, "A", 6),
            (ny_last, "A", 2),
            (ny_closed, "B", 4),
            (daily_before_2, "A", 1),
            (daily_after_2, "B", 5),
        ]
        frame = pd.DataFrame(
            [
                {
                    "ts_event": (open_time_ms + 10_000) * 1_000_000,
                    "side": side,
                    "size": size,
                }
                for open_time_ms, side, size in candle_rows
            ]
        )
        store = object.__new__(CmeLocalDbnTradeStore)
        store.trade_frame_for_time_range = lambda *args, **kwargs: frame

        metrics = store.cumulative_contract_deltas(
            "NQ.FUT",
            candle_open_times_ms=[row[0] for row in candle_rows],
            interval_ms=60_000,
            session_start_hour_chicago=17,
        )

        print(
            "CVD_DAILY_RESET | "
            f"before={metrics[daily_before_1]} | after={metrics[daily_after_1]}"
        )
        print(
            "CVD_NY_SESSION_RESET | "
            f"before={metrics[ny_before]} | after={metrics[ny_after]}"
        )
        print(
            "CVD_DAILY_RESET | "
            f"before={metrics[daily_before_2]} | after={metrics[daily_after_2]}"
        )
        self.assertEqual(metrics[daily_after_1]["trading_day"], "2026-06-09")
        self.assertEqual(metrics[daily_after_1]["day_cumulative_delta"], 3)
        self.assertEqual(metrics[daily_after_2]["trading_day"], "2026-06-10")
        self.assertEqual(metrics[daily_after_2]["day_cumulative_delta"], 5)
        self.assertNotEqual(
            metrics[daily_before_1]["trading_day"],
            metrics[daily_after_1]["trading_day"],
        )
        self.assertNotEqual(
            metrics[daily_before_2]["trading_day"],
            metrics[daily_after_2]["trading_day"],
        )
        self.assertEqual(metrics[ny_after]["ny_session_date"], "2026-06-09")
        self.assertEqual(metrics[ny_after]["session_cumulative_delta"], -6)
        self.assertIsNotNone(metrics[ny_last]["session_cumulative_delta"])
        self.assertIsNone(metrics[ny_closed]["session_cumulative_delta"])
        self.assertEqual(metrics[ny_closed]["ny_session_date"], "")
        self.assertNotEqual(
            metrics[ny_before]["ny_session_date"],
            metrics[ny_after]["ny_session_date"],
        )

    def test_cumulative_delta_cache_hits_and_extends_incrementally(self) -> None:
        base_ms = int(datetime(2026, 6, 9, 1, 0, tzinfo=UTC).timestamp() * 1000)
        frame = pd.DataFrame(
            [
                {
                    "ts_event": (base_ms + 10_000) * 1_000_000,
                    "side": "A",
                    "size": 4,
                },
                {
                    "ts_event": (base_ms + 70_000) * 1_000_000,
                    "side": "B",
                    "size": 2,
                },
                {
                    "ts_event": (base_ms + 130_000) * 1_000_000,
                    "side": "A",
                    "size": 3,
                },
            ]
        )
        calls: list[tuple[int, int]] = []
        store = object.__new__(CmeLocalDbnTradeStore)

        def frame_for_range(*args, **kwargs):
            del args
            start_ms = int(kwargs["start_ms"])
            end_ms = int(kwargs["end_ms"])
            calls.append((start_ms, end_ms))
            return frame.loc[
                (frame["ts_event"] >= start_ms * 1_000_000)
                & (frame["ts_event"] < end_ms * 1_000_000)
            ].copy()

        store.trade_frame_for_time_range = frame_for_range
        opens = [base_ms, base_ms + 60_000]

        first = store.cumulative_contract_deltas(
            "NQ.FUT",
            candle_open_times_ms=opens,
            interval_ms=60_000,
            timeframe="M1",
            session_start_hour_chicago=17,
        )
        first_call_count = len(calls)
        second = store.cumulative_contract_deltas(
            "NQ.FUT",
            candle_open_times_ms=opens,
            interval_ms=60_000,
            timeframe="M1",
            session_start_hour_chicago=17,
        )
        second_cache_metrics = store.cumulative_delta_cache_metrics()
        extended = store.cumulative_contract_deltas(
            "NQ.FUT",
            candle_open_times_ms=[base_ms + 120_000],
            interval_ms=60_000,
            timeframe="M1",
            session_start_hour_chicago=17,
        )

        self.assertEqual(first[base_ms + 60_000]["day_cumulative_delta"], -2)
        self.assertEqual(second[base_ms + 60_000]["day_cumulative_delta"], -2)
        self.assertEqual(first_call_count, 1)
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][0], base_ms + 120_000)
        self.assertEqual(extended[base_ms + 120_000]["day_cumulative_delta"], -5)
        self.assertEqual(second_cache_metrics, {"hit_count": 1, "miss_count": 0})
        self.assertEqual(
            store.cumulative_delta_cache_metrics(),
            {"hit_count": 0, "miss_count": 1},
        )

    def test_dbn_cache_sizes_are_configurable(self) -> None:
        store = CmeLocalDbnTradeStore(
            catalog=object(),
            partition_cache_size=12,
            trading_day_cache_size=6,
            cumulative_delta_cache_size=96,
        )

        self.assertEqual(store._partition_cache_size, 12)
        self.assertEqual(store._trading_day_cache_size, 6)
        self.assertEqual(store._cumulative_delta_cache_size, 96)

    def test_four_tick_bins_and_abnormal_flags_are_presentation_only(self) -> None:
        open_time_ms = int(datetime(2026, 6, 9, 14, 0, tzinfo=UTC).timestamp() * 1000)
        rows = []
        trade_sizes = [
            (16_000, "A", 6), (16_000, "B", 2),
            (16_001, "A", 7), (16_001, "B", 2),
            (16_002, "A", 8), (16_002, "B", 2),
            (16_003, "A", 9), (16_003, "B", 2),
            (16_004, "A", 2), (16_004, "B", 38),
        ]
        for offset, (price, side, size) in enumerate(trade_sizes):
            rows.append(
                {
                    "ts_event": (open_time_ms + offset * 1_000) * 1_000_000,
                    "side": side,
                    "price": price * 1_000_000_000,
                    "size": size,
                    "symbol": "NQM6",
                }
            )
        candles = _footprint_candles_from_frame(
            pd.DataFrame(rows),
            provider_symbol="NQ.FUT",
            mt5_symbol="NQ",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            output_decimal_places=3,
            duration_unit_ms=1_000,
        )
        candle = candles[0]
        flagged = [
            item
            for item in candle["bins"]
            if item["abnormal_contract"]
            or item["abnormal_buy_imbalance"]
            or item["abnormal_sell_imbalance"]
        ]
        print(
            "CONTRACT_SPIKE_SAMPLE | "
            f"bin_ticks={CME_BIN_TICK_COUNT} | median={candle['median_bin_volume']} | "
            f"p75={candle['contract_spike_p75']} | normal_median={candle['contract_spike_normal_median']} | "
            f"normal_mad={candle['contract_spike_normal_mad']} | "
            f"bins={[(item['index'], item['l2']['total_contracts'], item['contract_spike_score'], item['l2']['buy_diagonal_imbalance_ratio'], item['l2']['sell_diagonal_imbalance_ratio'], item['abnormal_contract'], item['abnormal_buy_imbalance'], item['abnormal_sell_imbalance']) for item in flagged]}"
        )
        self.assertEqual(CME_BIN_TICK_COUNT, Decimal("1"))
        self.assertEqual(candle["median_bin_volume"], "10.0")
        self.assertEqual(candle["contract_spike_p75"], "11.000")
        self.assertEqual(candle["contract_spike_normal_median"], "9.500")
        self.assertEqual(candle["contract_spike_normal_mad"], "1.000")
        self.assertEqual(candle["contract_spike_score_deviation"], "8.256")
        traded_bins = [
            item
            for item in candle["bins"]
            if int(item["l2"]["total_contracts"]) > 0
        ]
        self.assertTrue(
            all(item["l2"]["efficiency_percentile"] is not None for item in traded_bins)
        )
        spike_bin = next(item for item in flagged if item["abnormal_contract"])
        self.assertEqual(spike_bin["l2"]["total_contracts"], "40")
        self.assertEqual(spike_bin["contract_spike_score"], "20.572")
        self.assertTrue(spike_bin["abnormal_volume"])
        self.assertEqual(
            spike_bin["l2"]["contract_spike_score"],
            spike_bin["contract_spike_score"],
        )
        self.assertTrue(any(item["abnormal_buy_imbalance"] for item in flagged))
        self.assertTrue(any(item["abnormal_sell_imbalance"] for item in flagged))

    def test_contract_spike_score_is_zero_when_normal_core_mad_is_zero(self) -> None:
        metrics = calculate_contract_spike_metrics([0, 10, 10, 100])

        self.assertEqual(metrics.p75, Decimal("55.0"))
        self.assertEqual(metrics.normal_median, Decimal("10"))
        self.assertEqual(metrics.normal_mad, Decimal("0"))
        self.assertEqual(metrics.scores, (Decimal("0"),) * 4)
        self.assertEqual(metrics.score_deviation, Decimal("0"))

    def test_cme_diagonal_ratio_uses_safe_denominator_without_overwriting_zero_contracts(self) -> None:
        open_time_ms = int(datetime(2026, 6, 9, 14, 0, tzinfo=UTC).timestamp() * 1000)
        rows = [
            {
                "ts_event": open_time_ms * 1_000_000,
                "side": "A",
                "price": 16_000_000_000_000,
                "size": 3,
                "symbol": "NQM6",
            },
            {
                "ts_event": (open_time_ms + 1_000) * 1_000_000,
                "side": "A",
                "price": 16_000_500_000_000,
                "size": 4,
                "symbol": "NQM6",
            },
        ]

        candle = _footprint_candles_from_frame(
            pd.DataFrame(rows),
            provider_symbol="NQ.FUT",
            mt5_symbol="NQ",
            timeframe="M1",
            tick_size=Decimal("0.25"),
            output_decimal_places=3,
            duration_unit_ms=1_000,
        )[0]
        first_footprint_bin = candle["bins"][0]
        second_footprint_bin = candle["bins"][2]

        print(
            "SAFE_DIAGONAL_DENOMINATOR_SAMPLE | "
            f"buy={first_footprint_bin['l2']['buy_contracts']} | "
            f"actual_upper_buy=0 | sell_ratio={first_footprint_bin['l2']['sell_diagonal_contract_ratio']} | "
            f"sell={first_footprint_bin['l2']['sell_contracts']} | "
            f"efficiency={first_footprint_bin['l2']['dominant_side_efficiency']}"
        )
        self.assertEqual(first_footprint_bin["l2"]["buy_contracts"], "0")
        self.assertEqual(first_footprint_bin["l2"]["sell_contracts"], "3")
        self.assertEqual(first_footprint_bin["l2"]["ask_traded_contracts"], "0")
        self.assertEqual(first_footprint_bin["l2"]["sell_diagonal_contract_ratio"], "3.000")
        self.assertEqual(first_footprint_bin["l2"]["dominant_diagonal_side"], "SELL")
        self.assertEqual(first_footprint_bin["l2"]["dominant_side_efficiency"], "0.000")
        self.assertEqual(second_footprint_bin["l2"]["buy_contracts"], "0")
        self.assertEqual(second_footprint_bin["l2"]["sell_contracts"], "4")

    def test_contract_profile_is_labeled_for_its_new_york_session(self) -> None:
        source_start_ms, _ = new_york_session_bounds_utc_ms("2026-06-08")
        frame = pd.DataFrame(
            [
                {
                    "ts_event": (source_start_ms + 60_000) * 1_000_000,
                    "side": "A",
                    "price": 16_000_000_000_000,
                    "size": 3,
                },
                {
                    "ts_event": (source_start_ms + 120_000) * 1_000_000,
                    "side": "B",
                    "price": 16_000_000_000_000,
                    "size": 2,
                },
            ]
        )

        profile = _new_york_contract_profile_from_frame(
            frame,
            session_date="2026-06-08",
            tick_size=Decimal("0.25"),
            config=CmeEngineConfig(session_start_hour_chicago=17),
        )

        self.assertIsNotNone(profile)
        assert profile is not None
        self.assertEqual(profile["trading_day"], "2026-06-08")
        self.assertEqual(profile["profile_session_date"], "2026-06-08")
        self.assertEqual(profile["quantity_unit"], "CONTRACTS")
        self.assertFalse(profile["is_previous_session"])
        self.assertEqual(profile["bins"][0]["buy_contracts"], "2")
        self.assertEqual(profile["bins"][0]["sell_contracts"], "3")
        self.assertEqual(profile["bins"][0]["total_contracts"], "5")
        self.assertEqual(profile["bins"][0]["contract_delta"], "-1")

    def test_new_york_profile_uses_only_its_own_session(self) -> None:
        session_start_ms, session_end_ms = new_york_session_bounds_utc_ms("2026-06-08")
        frame = pd.DataFrame(
            [
                {
                    "ts_event": (session_start_ms + 60_000) * 1_000_000,
                    "side": "A",
                    "price": 16_000_000_000_000,
                    "size": 4,
                }
            ]
        )
        store = _WindowOnlyTradeStore(frame, session_start_ms)
        engine = CmePagedHistoryEngine(
            catalog=None,
            trade_store=store,
            candle_engine=None,
            footprint_engine=None,
            volume_profile_engine=object(),
            config=CmeEngineConfig(session_start_hour_chicago=17),
        )

        profile = engine._build_new_york_session_profile(
            provider_symbol="NQ.FUT",
            session_date="2026-06-08",
            tick_size=Decimal("0.25"),
        )

        self.assertIsNotNone(profile)
        assert profile is not None
        self.assertEqual(store.requests, [(session_start_ms, session_end_ms)])
        self.assertEqual(profile["profile_session_date"], "2026-06-08")
        self.assertEqual(profile["display_session_date"], "2026-06-08")
        self.assertEqual(profile["bins"][0]["total_contracts"], "4")


if __name__ == "__main__":
    unittest.main()
