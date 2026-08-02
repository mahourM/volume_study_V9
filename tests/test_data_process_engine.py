from __future__ import annotations

import inspect
import threading
import unittest
from collections import OrderedDict
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping
from unittest.mock import MagicMock, patch

import pandas as pd

from absorption.html_server import (
    _candles_html_page,
    _process_replay_payload_for_timeframe,
    _refill_scan_payload_for_timeframe,
)
from absorption.session_service import _aggregate_refill_scan_payloads, _stable_dom_index_version
from absorption.session_service import AbsorptionFootprintService
from DOM.models import DomRawEvent
from core.engine_output_bus import DOM_POSITIVE_REFILL_OUTPUT_TYPE, EngineOutputStore
from core.timeframe_policy import DEFAULT_FOOTPRINT_TIMEFRAME
from process.dataProcessEngine import DataProcessEngine, _refill_levels_from_payloads, _refill_zones_from_payloads
from process.data_sources import (
    CmeFootprintReplaySource,
    DomDatabentoReplaySource,
    InMemoryProcessEventSource,
    InMemoryProcessFootprintSource,
)
from process.models import (
    DATA_PROCESS_ENTRY_ACTION,
    DATA_PROCESS_ENGINE_PRODUCER,
    DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
    DATA_PROCESS_REFILL_OUTPUT_TYPE,
    DataProcessConfig,
    ProcessReplayRequest,
    ProcessRunResult,
    ProcessSymbol,
)
from process.sinks import CsvProcessLogSink, EngineOutputStoreSink
from process.time_range import parse_vancouver_replay_range


def symbol() -> ProcessSymbol:
    return ProcessSymbol(
        provider_symbol="NQ.FUT",
        mt5_symbol="NQ",
        market_provider="CME_LOCAL_DBN",
        dataset="GLBX.MDP3",
        schema="mbo",
        tick_size=Decimal("0.25"),
        timeframe="M1",
        interval="1m",
    )


def raw(
    ts_event_ms: int,
    *,
    action: str,
    size: int,
    price: str = "29618.25",
    side: str = "BID",
    order_id: str = "6877621592351",
) -> DomRawEvent:
    return DomRawEvent(
        ts_event_ms=ts_event_ms,
        price=Decimal(price),
        size=size,
        side=side,
        action=action,
        order_id=order_id,
        instrument_id=42004058,
        sequence=ts_event_ms,
        source_file="memory",
    )


def fill_refill_events(count: int, *, start_ms: int = 2_000) -> tuple[DomRawEvent, ...]:
    events: list[DomRawEvent] = []
    for index in range(count):
        event_ms = int(start_ms) + index * 100
        events.append(raw(event_ms, action="F", size=2))
        events.append(raw(event_ms + 1, action="M", size=2))
    return tuple(events)


def level_order_events(
    *,
    order_id: str,
    price: str,
    side: str,
    refill_count: int,
    start_ms: int,
) -> tuple[DomRawEvent, ...]:
    events: list[DomRawEvent] = [
        raw(start_ms, action="A", size=2, price=price, side=side, order_id=order_id)
    ]
    for index in range(refill_count):
        event_ms = start_ms + 100 + index * 10
        events.append(raw(event_ms, action="F", size=2, price=price, side=side, order_id=order_id))
        events.append(raw(event_ms + 1, action="M", size=2, price=price, side=side, order_id=order_id))
    events.append(raw(start_ms + 100 + refill_count * 10 + 1, action="C", size=2, price=price, side=side, order_id=order_id))
    return tuple(events)


def footprint_candle(*bins: dict[str, Any]) -> dict[str, Any]:
    return {
        "open_time_ms": 0,
        "close_time_ms": 59_999,
        "provider_symbol": "NQ.FUT",
        "bins": bins,
    }


def footprint_bin(low: str, high: str, *, buy: str, sell: str) -> dict[str, Any]:
    return {
        "low": low,
        "high": high,
        "l2": {
            "buy_contracts": buy,
            "sell_contracts": sell,
            "ask_traded_contracts": buy,
            "bid_traded_contracts": sell,
        },
    }


def footprint_noise_bins(
    start: str,
    count: int,
    *,
    buy: str,
    sell: str,
    tick: str = "0.25",
) -> tuple[dict[str, Any], ...]:
    start_price = Decimal(start)
    tick_size = Decimal(tick)
    return tuple(
        footprint_bin(
            str(start_price + tick_size * index),
            str(start_price + tick_size * (index + 1)),
            buy=buy,
            sell=sell,
        )
        for index in range(count)
    )


def refill_level_payload(
    *,
    price: str,
    side: str,
    buy: int,
    sell: int,
    refill_count: int = 15,
    refill_contracts: int = 20,
    diagonal_ratio_pass: bool = True,
    terminal_z_score: str = "2",
) -> dict[str, Any]:
    aggressive_contracts = int(sell) if side == "BID" else int(buy) if side == "ASK" else max(int(buy), int(sell))
    diagonal_denominator = 1 if diagonal_ratio_pass else max(1, aggressive_contracts)
    diagonal_ratio = Decimal(aggressive_contracts) / Decimal(diagonal_denominator)
    return {
        "payload_id": f"{side}-{price}-{buy}-{sell}-{refill_count}",
        "output_id": f"{side}-{price}-{buy}-{sell}-{refill_count}",
        "id": f"{side}-{price}-{buy}-{sell}-{refill_count}",
        "price": price,
        "side": side,
        "refill_count": refill_count,
        "refill_contracts": refill_contracts,
        "price_base_refill_count": refill_count,
        "price_base_refill_contracts": refill_contracts,
        "refill_method": "price_base_refill",
        "market_buy": buy,
        "market_sell": sell,
        "footprint_aggressive_contracts": aggressive_contracts,
        "footprint_aggressive_z_score": terminal_z_score,
        "footprint_diagonal_numerator_contracts": aggressive_contracts,
        "footprint_diagonal_denominator_contracts": diagonal_denominator,
        "footprint_diagonal_ratio": str(diagonal_ratio),
        "footprint_diagonal_ratio_pass": diagonal_ratio_pass,
        "terminal_aggressive_contracts": aggressive_contracts,
        "terminal_aggressive_z_score": terminal_z_score,
        "terminal_diagonal_numerator_contracts": aggressive_contracts,
        "terminal_diagonal_denominator_contracts": diagonal_denominator,
        "terminal_diagonal_ratio": str(diagonal_ratio),
        "terminal_diagonal_ratio_pass": diagonal_ratio_pass,
        "footprint_bin_low": price,
        "footprint_bin_high": str(Decimal(price) + Decimal("0.25")),
        "timestamp_ms": 1_000,
        "footprint_open_time_ms": 0,
        "provider_symbol": "NQ.FUT",
        "mt5_symbol": "NQ",
        "timeframe": "M1",
    }


class _CollectingSink:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def publish(self, payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> None:
        self.payloads.extend(dict(payload) for payload in payloads)


def test_refill_levels_use_price_base_event_increments() -> None:
    first = refill_level_payload(price="100", side="BID", buy=0, sell=1, refill_count=5, refill_contracts=8)
    second = refill_level_payload(price="100", side="BID", buy=0, sell=1, refill_count=6, refill_contracts=10)
    first.update(price_base_refill_count=1, price_base_refill_contracts=2, refill_method="price_base_refill")
    second.update(price_base_refill_count=1, price_base_refill_contracts=2, refill_method="price_base_refill")

    levels = _refill_levels_from_payloads((first, second))

    assert len(levels) == 1
    assert levels[0].refill_count == 2
    assert levels[0].refill_contracts == 4


class _ReplayProvider:
    def __init__(self) -> None:
        self.calls: list[dict[str, str]] = []

    def data_process_replay_payload(
        self,
        *,
        timeframe: str | None,
        start_vancouver: str,
        end_vancouver: str,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "timeframe": timeframe or "",
                "start_vancouver": start_vancouver,
                "end_vancouver": end_vancouver,
            }
        )
        return {
            "type": "DATA_PROCESS_REPLAY_RESULT",
            "status": "OK",
            "timeframe": timeframe,
            "start_vancouver": start_vancouver,
            "end_vancouver": end_vancouver,
        }


class _RefillScanProvider:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def data_process_refill_scan_payload(
        self,
        *,
        timeframe: str | None,
        start_vancouver: str,
        end_vancouver: str,
        refill_min: int,
        activity_filter: str = "",
        rate_min: float | None = None,
        spike_score_min: Decimal | None = None,
    ) -> dict[str, Any]:
        self.calls.append(
            {
                "timeframe": timeframe or "",
                "start_vancouver": start_vancouver,
                "end_vancouver": end_vancouver,
                "refill_min": refill_min,
                "activity_filter": activity_filter,
                "rate_min": rate_min,
                "spike_score_min": spike_score_min,
            }
        )
        return {
            "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
            "status": "OK",
            "timeframe": timeframe,
            "refill_min": refill_min,
        }


class _FakeCmeCatalog:
    def available_symbols(self) -> tuple[str, ...]:
        return ("NQ.FUT",)

    def tick_size_for(self, provider_symbol: str) -> Decimal:
        del provider_symbol
        return Decimal("0.25")


class _FakeCmeTradeStore:
    def __init__(self, frame: pd.DataFrame) -> None:
        self.frame = frame
        self.requests: list[tuple[str, int, int, int]] = []

    def trade_frame_for_time_range(
        self,
        provider_symbol: str,
        *,
        start_ms: int,
        end_ms: int,
        session_start_hour_chicago: int,
    ) -> pd.DataFrame:
        self.requests.append(
            (
                provider_symbol,
                int(start_ms),
                int(end_ms),
                int(session_start_hour_chicago),
            )
        )
        result = self.frame.loc[
            (self.frame["ts_event"] >= int(start_ms) * 1_000_000)
            & (self.frame["ts_event"] < int(end_ms) * 1_000_000)
        ].copy()
        result.attrs["contract_symbol"] = "NQM6"
        result.attrs["contract_symbols"] = ("NQM6",)
        return result


class DataProcessEngineTests(unittest.TestCase):
    def test_dom_replay_source_reads_indexed_sources_in_global_time_order(self) -> None:
        sources = (
            ("ready_late", "ready.dbn", "READY", 4_000, 1_000, 4_000, "NQM6"),
            ("running_old", "running.dbn", "RUNNING", 3_000, 1_000, 3_000, "NQM6"),
            ("error_partial", "error.dbn", "ERROR", 2_500, 1_000, 2_500, "NQM6"),
        )
        event_rows = {
            "ready_late": (
                ("ready_late", 1, 3_000, "100.00", 1, "BID", "A", "late", 1),
                ("ready_late", 2, 2_000, "100.25", 2, "ASK", "M", "duplicate", 1),
                ("ready_late", 3, 3_500, "100.00", 1, "BID", "F", "same-source", 1),
                ("ready_late", 4, 3_500, "100.00", 1, "BID", "F", "same-source", 1),
            ),
            "running_old": (
                ("running_old", 1, 1_500, "100.00", 1, "BID", "A", "running", 1),
                ("running_old", 2, 2_000, "100.25", 2, "ASK", "M", "duplicate", 1),
            ),
            "error_partial": (
                ("error_partial", 1, 1_200, "100.00", 1, "BID", "A", "error", 1),
            ),
        }

        class _Rows:
            def __init__(self, rows: tuple[tuple[Any, ...], ...]) -> None:
                self._rows = rows

            def fetchall(self) -> list[tuple[Any, ...]]:
                return list(self._rows)

        class _Connection:
            def __enter__(self) -> "_Connection":
                return self

            def __exit__(self, *_args: Any) -> None:
                return None

            def execute(self, query: str, params: tuple[Any, ...] = ()) -> _Rows:
                text = " ".join(query.split())
                if "SELECT provider_symbol, GROUP_CONCAT(contract_symbols)" in text:
                    return _Rows((("NQ.FUT", ",".join(row[6] for row in sources)),))
                if "FROM dom_events" in text:
                    if "CAST(price AS REAL)" in text:
                        source_key, start_ms, end_ms = params[:3]
                        price_params = params[3:-4]
                        last_ts, _same_ts, last_ordinal, limit = params[-4:]
                        price_ranges = tuple(
                            (float(price_params[index]), float(price_params[index + 1]))
                            for index in range(0, len(price_params), 2)
                        )
                    else:
                        source_key, start_ms, end_ms, last_ts, _same_ts, last_ordinal, limit = params
                        price_ranges = ()
                    rows = []
                    for row in event_rows[str(source_key)]:
                        _source_key, ordinal, ts_event_ms, *_rest = row
                        if not int(start_ms) <= int(ts_event_ms) <= int(end_ms):
                            continue
                        if int(ts_event_ms) < int(last_ts) or (
                            int(ts_event_ms) == int(last_ts)
                            and int(ordinal) <= int(last_ordinal)
                        ):
                            continue
                        if price_ranges and not any(
                            price_low <= float(row[3]) < price_high
                            for price_low, price_high in price_ranges
                        ):
                            continue
                        rows.append(row)
                    rows.sort(key=lambda row: (int(row[2]), int(row[1])))
                    return _Rows(tuple(rows[: int(limit)]))
                if "SELECT source_key" in text:
                    provider = str(params[0]).upper()
                    start_ms = int(params[1]) if len(params) > 1 else None
                    end_ms = int(params[2]) if len(params) > 2 else None
                    rows = []
                    for row in sources:
                        source_key, source_file, status, _indexed, earliest, latest, _contracts = row
                        if provider != "NQ.FUT":
                            continue
                        if status != "READY" and latest <= 0:
                            continue
                        if start_ms is not None and latest < start_ms:
                            continue
                        if end_ms is not None and earliest > end_ms:
                            continue
                        rows.append((source_key, source_file, earliest))
                    rows.sort(key=lambda row: (row[2], row[1]))
                    return _Rows(tuple((row[0],) for row in rows))
                return _Rows(())

        class _FakeDomDatabentoReplaySource(DomDatabentoReplaySource):
            def _connection(self) -> _Connection:
                return _Connection()

        source = _FakeDomDatabentoReplaySource(
            index_path=Path("ignored.sqlite3"),
            dataset="GLBX.MDP3",
            default_tick_size="0.25",
            batch_size=2,
        )

        self.assertEqual(source.symbols()[0].provider_symbol, "NQ.FUT")
        events = tuple(source.events(symbol(), start_ms=1_000, end_ms=4_000))

        self.assertEqual(
            [event.order_id for event in events],
            ["error", "running", "duplicate", "late", "same-source", "same-source"],
        )
        self.assertEqual(
            [event.ts_event_ms for event in events],
            [1_200, 1_500, 2_000, 3_000, 3_500, 3_500],
        )

        source.restrict_events_to_footprints((
            SimpleNamespace(
                symbol=symbol(),
                candles=({
                    "open_time_ms": 0,
                    "bins": ({"low": "100.00", "high": "100.25"},),
                },),
            ),
        ))
        filtered = tuple(source.events(symbol(), start_ms=1_000, end_ms=4_000))

        self.assertEqual(
            [event.order_id for event in filtered],
            ["error", "running", "late", "same-source", "same-source"],
        )

        spike_source = _FakeDomDatabentoReplaySource(
            index_path=Path("ignored.sqlite3"),
            dataset="GLBX.MDP3",
            default_tick_size="0.25",
            batch_size=2,
            footprint_score_min="12",
        )
        spike_source.restrict_events_to_footprints((
            SimpleNamespace(
                symbol=symbol(),
                candles=({
                    "open_time_ms": 0,
                    "bins": (
                        {"low": "100.00", "high": "100.25", "contract_spike_score": "12"},
                        {"low": "100.25", "high": "100.50", "contract_spike_score": "12"},
                    ),
                },),
            ),
        ))
        spike_events = tuple(
            spike_source.events(symbol(), start_ms=1_000, end_ms=4_000)
        )

        self.assertEqual(
            [event.order_id for event in spike_events],
            ["error", "running", "duplicate", "late", "same-source", "same-source"],
        )

    def test_vancouver_replay_range_converts_to_utc_milliseconds(self) -> None:
        replay_range = parse_vancouver_replay_range(
            start="2026-06-08T10:30:00",
            end="2026-06-08T10:32:00",
        )

        self.assertEqual(replay_range.start_ms, 1_780_939_800_000)
        self.assertEqual(replay_range.end_ms, 1_780_939_920_000)
        self.assertTrue(replay_range.start_vancouver.startswith("2026-06-08T10:30:00"))
        self.assertTrue(replay_range.start_utc.startswith("2026-06-08T17:30:00"))

    def test_process_replay_endpoint_uses_vancouver_query_fields(self) -> None:
        provider = _ReplayProvider()

        status_line, payload = _process_replay_payload_for_timeframe(
            provider,
            "M1",
            {
                "start_vancouver": ["2026-06-08T10:30:00"],
                "end_vancouver": ["2026-06-08T10:32:00"],
            },
        )

        self.assertEqual(status_line, "HTTP/1.1 200 OK")
        self.assertEqual(payload["status"], "OK")
        self.assertEqual(
            provider.calls,
            [
                {
                    "timeframe": "M1",
                    "start_vancouver": "2026-06-08T10:30:00",
                    "end_vancouver": "2026-06-08T10:32:00",
                }
            ],
        )

    def test_replay_tools_default_to_m1_timeframe(self) -> None:
        self.assertEqual(DEFAULT_FOOTPRINT_TIMEFRAME, "M1")

    def test_process_replay_response_is_complete_and_does_not_pollute_live_output_store(self) -> None:
        source = inspect.getsource(AbsorptionFootprintService.data_process_replay_payload)

        self.assertIn('"payloads": list(result.payloads),', source)
        self.assertNotIn("result.payloads[:100]", source)
        self.assertNotIn("EngineOutputStoreSink(self.engine_output_store)", source)

    def test_refill_scan_endpoint_uses_independent_refill_minimum(self) -> None:
        provider = _RefillScanProvider()

        status_line, payload = _refill_scan_payload_for_timeframe(
            provider,
            "M1",
            {
                "start_vancouver": ["2026-06-08T10:30:00"],
                "end_vancouver": ["2026-06-08T10:32:00"],
                "refill_min": ["7"],
            },
        )

        self.assertEqual(status_line, "HTTP/1.1 200 OK")
        self.assertEqual(payload["status"], "OK")
        self.assertEqual(payload["refill_min"], 7)
        self.assertEqual(
            provider.calls,
            [
                {
                    "timeframe": "M1",
                    "start_vancouver": "2026-06-08T10:30:00",
                    "end_vancouver": "2026-06-08T10:32:00",
                    "refill_min": 7,
                    "activity_filter": "",
                    "rate_min": None,
                    "spike_score_min": None,
                }
            ],
        )

    def test_refill_scan_endpoint_accepts_zero_threshold(self) -> None:
        provider = _RefillScanProvider()

        status_line, payload = _refill_scan_payload_for_timeframe(
            provider,
            "M1",
            {
                "start_vancouver": ["2026-06-08T10:30:00"],
                "end_vancouver": ["2026-06-08T10:32:00"],
                "refill_min": ["0"],
            },
        )

        self.assertEqual(status_line, "HTTP/1.1 200 OK")
        self.assertEqual(payload["refill_min"], 0)
        self.assertEqual(provider.calls[0]["refill_min"], 0)
        self.assertEqual(provider.calls[0]["activity_filter"], "")
        self.assertIsNone(provider.calls[0]["rate_min"])

    def test_refill_scan_endpoint_passes_activity_filter_code(self) -> None:
        provider = _RefillScanProvider()

        status_line, payload = _refill_scan_payload_for_timeframe(
            provider,
            "M1",
            {
                "start_vancouver": ["2026-06-08T10:30:00"],
                "end_vancouver": ["2026-06-08T10:32:00"],
                "refill_min": ["0"],
                "activity_filter": ["a250"],
            },
        )

        self.assertEqual(status_line, "HTTP/1.1 200 OK")
        self.assertEqual(provider.calls[0]["activity_filter"], "A250")

    def test_refill_scan_endpoint_passes_rate_minimum(self) -> None:
        provider = _RefillScanProvider()

        status_line, _payload = _refill_scan_payload_for_timeframe(
            provider,
            "M1",
            {
                "start_vancouver": ["2026-06-08T10:30:00"],
                "end_vancouver": ["2026-06-08T10:32:00"],
                "rate_min": ["63.6"],
            },
        )

        self.assertEqual(status_line, "HTTP/1.1 200 OK")
        self.assertEqual(provider.calls[0]["rate_min"], 63.6)

    def test_refill_scan_endpoint_passes_spike_score_minimum(self) -> None:
        provider = _RefillScanProvider()

        status_line, _payload = _refill_scan_payload_for_timeframe(
            provider,
            "M1",
            {
                "start_vancouver": ["2026-06-08T10:30:00"],
                "end_vancouver": ["2026-06-08T10:32:00"],
                "spike_score_min": ["12.5"],
            },
        )

        self.assertEqual(status_line, "HTTP/1.1 200 OK")
        self.assertEqual(provider.calls[0]["spike_score_min"], Decimal("12.5"))

    def test_refill_scan_uses_complete_candle_activity_snapshots(self) -> None:
        source = inspect.getsource(AbsorptionFootprintService.data_process_refill_scan_payload)

        self.assertIn("max_payloads=2_000_000", source)
        self.assertIn("emit_individual_refill_orders=False", source)
        self.assertIn("emit_price_activity_levels=True", source)
        self.assertIn("collect_event_payloads=False", source)
        self.assertIn("filter_price_activity_to_footprints=True", source)
        self.assertIn("events_are_time_ordered=True", source)
        self.assertIn("deduplicate_events=False", source)

    def test_candle_page_exposes_vancouver_replay_and_refill_scan_controls(self) -> None:
        page = _candles_html_page("M1")

        self.assertIn('id="process-replay-form"', page)
        self.assertIn('id="refill-scan-form"', page)
        self.assertIn('id="refill-scan-min"', page)
        self.assertIn('id="refill-scan-min" type="number" min="0"', page)
        self.assertIn('id="refill-activity-filter"', page)
        self.assertIn('id="refill-rate-min"', page)
        self.assertIn('id="spike-score-min"', page)
        self.assertIn('id="spike-score-submit"', page)
        self.assertIn("function runSpikeScoreScan()", page)
        self.assertIn("Spike score : ${fmtMaybe(marker?.contract_spike_score, 3)} | R", page)
        self.assertIn("const renderedRefillMarkerKeys = new Set();", page)
        self.assertIn("markerKeys.has(logicalMarkerKey)", page)
        self.assertIn("font-size: 20px;", page)
        self.assertIn('/refill-scan/${ACTIVE_TIMEFRAME}', page)
        self.assertIn("const refillScanMarkersByCandleOpen = new Map();", page)
        self.assertIn("function rememberRefillScanPayloadMarkers(scan, minimumRefillCount)", page)
        self.assertIn("function runRefillScan(event)", page)
        self.assertIn("refillScanForm.addEventListener(\"submit\", runRefillScan);", page)
        self.assertIn("domRefillMarkerLabel(marker, includeContracts)", page)
        self.assertIn("return `${refillCount}(${added}) E${executed} - ${rate}%`;", page)
        self.assertIn("marker?.refill_filled_contracts", page)
        self.assertIn("payload?.executed_refill_contracts", page)
        self.assertIn("payload?.marker_time_ms", page)
        self.assertIn("payload?.footprint_open_time_ms", page)
        self.assertIn("payload?.marker_price", page)
        self.assertIn("payload?.footprint_bin_low", page)
        self.assertIn("Vancouver", page)
        self.assertIn("font-size: 20px;", page)
        self.assertIn("font-weight: 150;", page)
        self.assertIn("function replayViewportEndMs(replay)", page)
        self.assertIn("const replayMarkersByCandleOpen = new Map();", page)
        self.assertIn("const replayTriggerSignalsByCandleOpen = new Map();", page)
        self.assertIn("function rememberReplayPayloadMarkers(replay)", page)
        self.assertIn("function rememberReplayTriggerSignals(replay)", page)
        self.assertIn("replayMarkersByCandleOpen.clear();", page)
        self.assertIn("replayTriggerSignalsByCandleOpen.clear();", page)
        self.assertIn('id="clear-markers"', page)
        self.assertIn("const clearMarkersButton = document.getElementById(\"clear-markers\");", page)
        self.assertIn("let markerOverlaysHidden = false;", page)
        self.assertIn("let replayOverlayActive = false;", page)
        self.assertIn("replayOverlayActive = true;", page)
        self.assertIn("if (!markerOverlaysHidden && !replayOverlayActive)", page)
        self.assertIn("replayOverlayActive = false;", page)
        self.assertIn("function clearCandleMarkers()", page)
        self.assertIn("refillScanMarkersByCandleOpen.clear();", page)
        self.assertIn("deleteScanMarkersByCandleOpen.clear();", page)
        self.assertIn("clearMarkersButton.addEventListener(\"click\", clearCandleMarkers);", page)
        self.assertIn("rememberReplayPayloadMarkers(replay);", page)
        self.assertIn("rememberReplayTriggerSignals(replay);", page)
        self.assertIn("function applyReplayMarkersToCharts()", page)
        self.assertIn("const appliedMarkerCount = applyReplayMarkersToCharts();", page)
        self.assertIn("replay?.trigger_signals", page)
        self.assertIn("const replaySignals = markerOverlaysHidden ? [] : replayTriggerSignalsByCandleOpen.get(openTime);", page)
        self.assertIn("mergeDomRefillMarkers(", page)
        self.assertIn("replay?.payload_log_path", page)
        self.assertIn("triggerCount > 0 ? ` | ${triggerCount} triggers` : \"\"", page)
        self.assertIn('appliedMarkerCount > 0 ? " drawn" : ""', page)
        self.assertNotIn("const targetEnd = replayViewportEndMs(replay)", page)
        self.assertNotIn("requestedWindowEndMs = targetEnd;", page)
        self.assertNotIn("refresh(targetEnd, requestedCandleLimit())", page)
        self.assertIn("ctx.lineWidth = 2.5;", page)
        self.assertIn("ctx.setLineDash([8, 5]);", page)
        self.assertIn("function priceDecimalsForStep(step)", page)
        self.assertIn("function fmtPrice(value, step = 0)", page)
        self.assertIn("const rawPriceToY = price => {", page)
        self.assertIn("const priceToY = price => Math.max(0, Math.min(plotH, rawPriceToY(price)));", page)
        self.assertIn("this.drawDomRefillMarkers(ctx, candleItems, layout, plotH, rawPriceToY);", page)
        self.assertIn("drawDomRefillMarkers(ctx, candleItems, layout, plotH, rawPriceToY)", page)
        self.assertIn("function drawTriggerMarkers(ctx, candle, centerX, yHigh, yLow, plotH = Infinity)", page)
        self.assertIn("if (tipY - arrowHeight < plotPad) tipY = arrowHeight + plotPad;", page)
        self.assertIn("drawTriggerMarkers(ctx, item.candle, center, priceToY(high), priceToY(low), plotH);", page)
        self.assertIn("const y = rawPriceToY(price);", page)
        self.assertIn("fmtPrice(price, this.priceStep)", page)
        self.assertNotIn("requestViewportWindow(replayEnd", page)
        self.assertIn("const DOM_REFILL_MARKER_MIN_COUNT = 1;", page)
        self.assertIn("const DOM_REFILL_MARKER_SPAN_CANDLES = 5;", page)
        self.assertIn("refillCount < minimumRefillCount", page)
        self.assertIn("Number(marker?.span_candles) || DOM_REFILL_MARKER_SPAN_CANDLES", page)
        self.assertNotIn("refillCount < 15", page)

    def test_refill_order_without_valid_zone_does_not_emit_payload(self) -> None:
        process_symbol = symbol()
        events = [
            raw(1_000, action="A", size=2),
            *fill_refill_events(5),
            raw(4_000, action="C", size=2),
        ]
        sink = _CollectingSink()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: tuple(events)}),
            config=DataProcessConfig(),
            sinks=(sink,),
        )

        result = engine.run_replay(
            ProcessReplayRequest(start_ms=1_000, end_ms=4_000)
        )

        self.assertEqual(result.processed_event_count, len(events))
        self.assertEqual(result.emitted_payload_count, 0)
        self.assertEqual(sink.payloads, [])

    def test_scan_mode_emits_refill_order_without_zone_requirement(self) -> None:
        process_symbol = symbol()
        events = [
            raw(1_000, action="A", size=2),
            *fill_refill_events(7),
            raw(4_000, action="C", size=2),
        ]
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: tuple(events)}),
            config=DataProcessConfig(
                emit_individual_refill_orders=True,
            ),
        )

        result = engine.run_replay(
            ProcessReplayRequest(start_ms=1_000, end_ms=4_000)
        )

        refill_payloads = [item for item in result.payloads if item["price_base_refill_count"] > 0]
        self.assertEqual(len(refill_payloads), 7)
        payload = refill_payloads[-1]
        self.assertEqual(payload["type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(payload["payload_type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(payload["output_type"], DATA_PROCESS_REFILL_OUTPUT_TYPE)
        self.assertEqual(payload["refill_count"], 7)
        self.assertEqual(payload["refill_contracts"], 14)
        self.assertEqual(payload["refill_filled_contracts"], 14)
        self.assertEqual(payload["positive_refill_filled_total"], 14)
        self.assertEqual(payload["executed_contracts"], 14)
        self.assertEqual(payload["price"], "29618.25")
        self.assertEqual(payload["side"], "BID")
        self.assertEqual(payload["zone_low"], "29618.25")
        self.assertEqual(payload["zone_high"], "29618.25")

    def test_scan_mode_does_not_count_size_reductions_after_fill_as_refill(self) -> None:
        process_symbol = symbol()
        price = "29277.0"
        order_id = "6877553156923"
        events = (
            raw(1_000, action="A", size=40, price=price, side="BID", order_id=order_id),
            raw(1_010, action="F", size=1, price=price, side="BID", order_id=order_id),
            raw(1_011, action="M", size=39, price=price, side="BID", order_id=order_id),
            raw(1_020, action="F", size=1, price=price, side="BID", order_id=order_id),
            raw(1_021, action="M", size=38, price=price, side="BID", order_id=order_id),
            raw(1_030, action="F", size=1, price=price, side="BID", order_id=order_id),
            raw(1_031, action="M", size=37, price=price, side="BID", order_id=order_id),
            raw(1_040, action="F", size=2, price=price, side="BID", order_id=order_id),
            raw(1_041, action="M", size=35, price=price, side="BID", order_id=order_id),
            raw(1_050, action="F", size=1, price=price, side="BID", order_id=order_id),
            raw(1_051, action="M", size=34, price=price, side="BID", order_id=order_id),
            raw(1_060, action="F", size=2, price=price, side="BID", order_id=order_id),
            raw(1_061, action="M", size=32, price=price, side="BID", order_id=order_id),
            raw(1_070, action="F", size=32, price=price, side="BID", order_id=order_id),
            raw(1_071, action="C", size=32, price=price, side="BID", order_id=order_id),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(
                emit_individual_refill_orders=True,
            ),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=1_071))

        self.assertEqual(result.emitted_payload_count, 0)

    def test_scan_mode_can_start_with_fill_modify_sequence_without_add(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="F", size=1, price="29250.75", side="ASK", order_id="MISSING-ADD"),
            raw(1_001, action="M", size=2, price="29250.75", side="ASK", order_id="MISSING-ADD"),
            raw(1_002, action="F", size=1, price="29250.75", side="ASK", order_id="MISSING-ADD"),
            raw(1_003, action="M", size=1, price="29250.75", side="ASK", order_id="MISSING-ADD"),
            raw(1_004, action="C", size=1, price="29250.75", side="ASK", order_id="MISSING-ADD"),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(
                emit_individual_refill_orders=True,
            ),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=1_004))

        refill_payloads = [item for item in result.payloads if item["price_base_refill_count"] > 0]
        self.assertEqual(len(refill_payloads), 1)
        payload = refill_payloads[-1]
        self.assertEqual(payload["refill_count"], 1)
        self.assertEqual(payload["refill_contracts"], 2)
        self.assertEqual(payload["refill_filled_contracts"], 1)
        self.assertEqual(payload["side"], "ASK")

    def test_refill_counts_only_positive_modify_delta_after_fill(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="A", size=5, order_id="NET-REFILL"),
            raw(1_100, action="F", size=2, order_id="NET-REFILL"),
            raw(1_101, action="M", size=6, order_id="NET-REFILL"),
            raw(1_200, action="F", size=1, order_id="NET-REFILL"),
            raw(1_201, action="M", size=4, order_id="NET-REFILL"),
            raw(1_300, action="F", size=3, order_id="NET-REFILL"),
            raw(1_301, action="M", size=5, order_id="NET-REFILL"),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(emit_individual_refill_orders=True),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=1_301))

        refill_payloads = [item for item in result.payloads if item["price_base_refill_count"] > 0]
        self.assertEqual(len(refill_payloads), 2)
        payload = refill_payloads[-1]
        self.assertEqual(payload["refill_count"], 2)
        self.assertEqual(payload["refill_contracts"], 7)
        self.assertEqual(payload["price_base_refill_count"], 1)
        self.assertEqual(payload["price_base_refill_contracts"], 4)

    def test_scan_mode_can_emit_open_refill_order_at_replay_end(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="A", size=2, order_id="OPEN-ORDER"),
            raw(1_100, action="F", size=1, order_id="OPEN-ORDER"),
            raw(1_101, action="M", size=2, order_id="OPEN-ORDER"),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(
                emit_individual_refill_orders=True,
            ),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=2_000))

        refill_payloads = [item for item in result.payloads if item["price_base_refill_count"] > 0]
        self.assertEqual(len(refill_payloads), 1)
        payload = refill_payloads[-1]
        self.assertEqual(payload["close_reason"], "REFILL")
        self.assertEqual(payload["refill_count"], 1)
        self.assertEqual(payload["refill_filled_contracts"], 1)

    def test_emit_start_keeps_warmup_state_but_counts_only_scan_window(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="A", size=2, order_id="WARMUP"),
            raw(1_100, action="F", size=1, order_id="WARMUP"),
            raw(1_101, action="M", size=2, order_id="WARMUP"),
            raw(2_100, action="F", size=1, order_id="WARMUP"),
            raw(2_101, action="M", size=2, order_id="WARMUP"),
            raw(2_200, action="C", size=2, order_id="WARMUP"),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(
                emit_individual_refill_orders=True,
            ),
        )

        result = engine.run_replay(
            ProcessReplayRequest(start_ms=1_000, end_ms=2_200, emit_start_ms=2_000)
        )

        refill_payloads = [item for item in result.payloads if item["price_base_refill_count"] > 0]
        self.assertEqual(len(refill_payloads), 1)
        payload = refill_payloads[-1]
        self.assertEqual(payload["refill_count"], 1)
        self.assertEqual(payload["refill_filled_contracts"], 1)

    def test_refill_scan_aggregation_uses_marker_candle_bin_and_side(self) -> None:
        payloads = (
            {
                "provider_symbol": "NQ.FUT",
                "timeframe": "M1",
                "marker_time_ms": 60_000,
                "marker_price": "29250.000",
                "price": "29250.75",
                "side": "ASK",
                "order_id": "A",
                "refill_count": 1,
                "refill_contracts": 2,
                "price_base_refill_count": 1,
                "price_base_refill_contracts": 2,
                "refill_method": "price_base_refill",
                "refill_filled_contracts": 1,
                "market_buy": 118,
                "market_sell": 0,
            },
            {
                "provider_symbol": "NQ.FUT",
                "timeframe": "M1",
                "marker_time_ms": 60_000,
                "marker_price": "29250.000",
                "price": "29250.50",
                "side": "ASK",
                "order_id": "B",
                "refill_count": 2,
                "refill_contracts": 3,
                "price_base_refill_count": 2,
                "price_base_refill_contracts": 3,
                "refill_method": "price_base_refill",
                "refill_filled_contracts": 2,
                "market_buy": 118,
                "market_sell": 0,
            },
        )

        aggregated = _aggregate_refill_scan_payloads(
            payloads,
            start_ms=60_000,
            end_ms=120_000,
        )

        self.assertEqual(len(aggregated), 1)
        self.assertEqual(aggregated[0]["timestamp_ms"], 60_000)
        self.assertEqual(aggregated[0]["price"], "29250.000")
        self.assertEqual(aggregated[0]["refill_count"], 3)
        self.assertEqual(aggregated[0]["refill_filled_contracts"], 3)
        self.assertEqual(aggregated[0]["order_ids"], ("A", "B"))
        self.assertEqual(aggregated[0]["order_count"], 2)
        self.assertEqual(aggregated[0]["order_id"], "")
        self.assertEqual(aggregated[0]["aggregation_key"], "candle_price_side")

    def test_refill_scan_reuses_unfiltered_range_cache_and_invalidates_on_data_change(self) -> None:
        engine_calls = 0

        class _CachedSource:
            def __init__(self, *, index_path: Path, **_kwargs: Any) -> None:
                self.index_path = index_path

            def symbols(self) -> tuple[ProcessSymbol, ...]:
                return (symbol(),)

        class _UnusedFootprintSource:
            def __init__(self, **_kwargs: Any) -> None:
                pass

        class _CachedEngine:
            def __init__(self, **_kwargs: Any) -> None:
                pass

            def run_replay(self, request: ProcessReplayRequest) -> ProcessRunResult:
                nonlocal engine_calls
                engine_calls += 1
                marker_time_ms = int(request.emit_start_ms or request.start_ms)
                return ProcessRunResult(
                    start_ms=request.start_ms,
                    end_ms=request.end_ms,
                    symbols=request.symbols,
                    processed_event_count=25,
                    emitted_payload_count=1,
                    payloads=(
                        {
                            "provider_symbol": "NQ.FUT",
                            "mt5_symbol": "NQ",
                            "market_provider": "CME_LOCAL_DBN",
                            "timeframe": "M1",
                            "marker_time_ms": marker_time_ms,
                            "marker_price": "29000.00",
                            "price": "29000.00",
                            "side": "BID",
                            "order_id": "CACHE",
                            "has_price_activity": True,
                            "price_base_refill_count": 5,
                            "price_base_refill_contracts": 8,
                            "executed_refill_contracts": 3,
                            "executed_contracts": 12,
                        },
                    ),
                )

        service = object.__new__(AbsorptionFootprintService)
        service.runtime_config = SimpleNamespace(
            project_root=Path("virtual-project"),
            dom_data_dir_name="DOM",
            dom_extracted_cache_dir_name=".cache",
            cme_dataset="GLBX.MDP3",
            cme_default_tick_size=Decimal("0.25"),
            dom_dbn_batch_size=25_000,
            cme_schema="trades",
            cme_trading_day_start_hour_chicago=17,
        )
        service.cme_catalog = object()
        service.cme_trade_store = object()
        service.cme_paged_history_engine = SimpleNamespace(
            config=SimpleNamespace(output_decimal_places=3, duration_unit_ms=1000)
        )
        service._active_cme_process_symbols_for_timeframe = (
            lambda source_symbols, **_kwargs: source_symbols
        )
        service._refill_scan_cache_lock = threading.Lock()
        service._refill_scan_cache = OrderedDict()
        with (
            patch("absorption.session_service.DomDatabentoReplaySource", _CachedSource),
            patch("absorption.session_service.CmeFootprintReplaySource", _UnusedFootprintSource),
            patch("absorption.session_service.DataProcessEngine", _CachedEngine),
            patch("absorption.session_service.RefillScanIndex.load", return_value=None),
            patch("absorption.session_service.RefillScanIndex.store"),
            patch("absorption.session_service._load_refill_scan_disk_cache", return_value=None),
            patch(
                "absorption.session_service._stable_dom_index_version",
                side_effect=(
                    (("source", 1),),
                    (("source", 1),),
                    (("source", 2),),
                ),
            ),
        ):
            first = service.data_process_refill_scan_payload(
                timeframe="M1",
                start_vancouver="2026-06-07T15:17:00",
                end_vancouver="2026-06-07T15:18:00",
                refill_min=10,
            )
            second = service.data_process_refill_scan_payload(
                timeframe="M1",
                start_vancouver="2026-06-07T15:17:00",
                end_vancouver="2026-06-07T15:18:00",
                refill_min=0,
            )
            third = service.data_process_refill_scan_payload(
                timeframe="M1",
                start_vancouver="2026-06-07T15:17:00",
                end_vancouver="2026-06-07T15:18:00",
                refill_min=0,
            )

        self.assertFalse(first["cache_hit"])
        self.assertEqual(first["matched_payload_count"], 0)
        self.assertTrue(second["cache_hit"])
        self.assertEqual(second["matched_payload_count"], 1)
        self.assertFalse(third["cache_hit"])
        self.assertEqual(engine_calls, 2)

    def test_refill_scan_index_version_ignores_wal_file_lifecycle(self) -> None:
        row = ("source", 1, 2, "READY", 30, 10, 30, "NQM6")
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.execute.return_value.fetchall.return_value = [row]
        with (
            patch("absorption.session_service.sqlite3.connect", return_value=connection),
            patch("pathlib.Path.stat", side_effect=AssertionError("filesystem version used")),
        ):
            before = _stable_dom_index_version(Path("dom_timeline_index.sqlite3"))
            after = _stable_dom_index_version(Path("dom_timeline_index.sqlite3"))

        self.assertEqual(before, after)

    def test_single_level_zone_payload_includes_zone_and_market_contracts(self) -> None:
        process_symbol = symbol()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: level_order_events(
                        order_id="SINGLE-BUY-ZONE",
                        price="29618.25",
                        side="BID",
                        refill_count=41,
                        start_ms=1_000,
                    )
                }
            ),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            *footprint_noise_bins("29617.00", 4, buy="1", sell="1"),
                            footprint_bin("29618.25", "29618.50", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(
            ProcessReplayRequest(
                start_ms=0,
                end_ms=60_000,
                symbols=(process_symbol,),
            )
        )

        self.assertEqual(result.emitted_payload_count, 27)
        payload = result.payloads[-1]
        self.assertEqual(payload["type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(payload["payload_type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(payload["output_type"], DATA_PROCESS_REFILL_OUTPUT_TYPE)
        self.assertEqual(payload["action"], DATA_PROCESS_ENTRY_ACTION)
        self.assertEqual(payload["payload_id"], payload["output_id"])
        self.assertEqual(payload["payload_id"], payload["id"])
        self.assertEqual(payload["side"], "BID")
        self.assertEqual(payload["price"], "29618.25")
        self.assertEqual(payload["refill_count"], 41)
        self.assertEqual(payload["refill_contracts"], 82)
        self.assertEqual(payload["refill_filled_contracts"], 82)
        self.assertEqual(payload["positive_refill_filled_total"], 82)
        self.assertEqual(payload["executed_contracts"], 82)
        self.assertEqual(payload["market_buy"], 0)
        self.assertEqual(payload["market_sell"], 80)
        self.assertEqual(payload["terminal_market_buy"], 0)
        self.assertEqual(payload["terminal_market_sell"], 80)
        self.assertEqual(payload["zone_low"], "29618.25")
        self.assertEqual(payload["zone_high"], "29618.50")
        self.assertEqual(payload["zone_level_count"], 1)
        self.assertEqual(payload["footprint_open_time_ms"], 0)
        self.assertEqual(payload["footprint_bin_low"], "29618.25")
        self.assertEqual(payload["footprint_bin_high"], "29618.50")

    def test_multi_level_buy_zone_can_include_three_refill_terminal_level(self) -> None:
        process_symbol = symbol()
        events: list[DomRawEvent] = []
        levels = (
            ("ABOVE-1", "28883.0", "BID", 7),
            ("ABOVE-2", "28880.5", "BID", 7),
            ("RESET-ASK", "28880.0", "ASK", 8),
            ("ZONE-1", "28878.5", "BID", 5),
            ("ZONE-2", "28877.5", "BID", 3),
            ("ZONE-3", "28876.0", "BID", 7),
            ("ZONE-4", "28875.5", "BID", 5),
        )
        for index, (order_id, price, side, refill_count) in enumerate(levels):
            events.extend(
                level_order_events(
                    order_id=order_id,
                    price=price,
                    side=side,
                    refill_count=refill_count,
                    start_ms=1_000 + index * 1_000,
                )
            )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: tuple(events)}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("28884.0", "28884.25", buy="999", sell="1"),
                            footprint_bin("28883.0", "28883.25", buy="11", sell="24"),
                            footprint_bin("28880.5", "28880.75", buy="11", sell="25"),
                            footprint_bin("28880.0", "28880.25", buy="17", sell="20"),
                            footprint_bin("28878.5", "28878.75", buy="3", sell="22"),
                            footprint_bin("28877.5", "28877.75", buy="2", sell="23"),
                            footprint_bin("28876.0", "28876.25", buy="1", sell="22"),
                            footprint_bin("28875.5", "28875.75", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=20_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 5)
        payload = result.payloads[-1]
        self.assertEqual(payload["side"], "BID")
        self.assertEqual(payload["price"], "28875.5")
        self.assertEqual(payload["refill_count"], 20)
        self.assertEqual(payload["zone_low"], "28875.5")
        self.assertEqual(payload["zone_high"], "28878.75")
        self.assertEqual(payload["zone_level_count"], 4)
        self.assertEqual(payload["zone_market_sell"], 147)
        self.assertEqual(payload["terminal_market_buy"], 0)
        self.assertEqual(payload["terminal_market_sell"], 80)
        self.assertEqual(payload["terminal_aggressive_contracts"], 80)
        self.assertEqual(payload["terminal_diagonal_numerator_contracts"], 80)
        self.assertEqual(payload["terminal_diagonal_denominator_contracts"], 1)
        self.assertEqual(payload["terminal_diagonal_ratio"], "80")
        self.assertTrue(payload["terminal_diagonal_ratio_pass"])

    def test_sell_terminal_diagonal_ratio_uses_lower_market_sell(self) -> None:
        process_symbol = symbol()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: level_order_events(
                        order_id="SINGLE-SELL-ZONE",
                        price="28890.0",
                        side="ASK",
                        refill_count=15,
                        start_ms=1_000,
                    )
                }
            ),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("28889.0", "28889.25", buy="1", sell="20"),
                            footprint_bin("28890.0", "28890.25", buy="80", sell="0"),
                            footprint_bin("28891.0", "28891.25", buy="1", sell="999"),
                            *footprint_noise_bins("28891.25", 2, buy="1", sell="1"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 1)
        payload = result.payloads[-1]
        self.assertEqual(payload["side"], "ASK")
        self.assertEqual(payload["entry_direction"], "SHORT")
        self.assertEqual(payload["terminal_market_buy"], 80)
        self.assertEqual(payload["terminal_market_sell"], 0)
        self.assertEqual(payload["terminal_aggressive_contracts"], 80)
        self.assertEqual(payload["terminal_diagonal_numerator_contracts"], 80)
        self.assertEqual(payload["terminal_diagonal_denominator_contracts"], 20)
        self.assertEqual(payload["terminal_diagonal_ratio"], "4")
        self.assertTrue(payload["terminal_diagonal_ratio_pass"])

    def test_bid_and_ask_sequences_accept_equal_aggressive_contract_steps(self) -> None:
        bid_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="102", side="BID", buy=2, sell=30),
                    refill_level_payload(price="101", side="BID", buy=2, sell=25),
                    refill_level_payload(price="100", side="BID", buy=0, sell=20),
                )
            )
            if zone.direction == "LONG"
        ]
        ask_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="100", side="ASK", buy=30, sell=2),
                    refill_level_payload(price="101", side="ASK", buy=25, sell=2),
                    refill_level_payload(price="102", side="ASK", buy=20, sell=0),
                )
            )
            if zone.direction == "SHORT"
        ]

        self.assertEqual(len(bid_zones), 1)
        self.assertEqual([level.price for level in bid_zones[0].levels], [Decimal("102"), Decimal("101"), Decimal("100")])
        self.assertEqual(len(ask_zones), 1)
        self.assertEqual([level.price for level in ask_zones[0].levels], [Decimal("100"), Decimal("101"), Decimal("102")])

    def test_terminal_level_must_pass_diagonal_ratio(self) -> None:
        bid_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="102", side="BID", buy=2, sell=30),
                    refill_level_payload(price="101", side="BID", buy=2, sell=25),
                    refill_level_payload(price="100", side="BID", buy=0, sell=20, diagonal_ratio_pass=False),
                )
            )
            if zone.direction == "LONG"
        ]
        ask_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="100", side="ASK", buy=30, sell=2),
                    refill_level_payload(price="101", side="ASK", buy=25, sell=2),
                    refill_level_payload(price="102", side="ASK", buy=20, sell=0, diagonal_ratio_pass=False),
                )
            )
            if zone.direction == "SHORT"
        ]

        self.assertEqual(bid_zones, [])
        self.assertEqual(ask_zones, [])

    def test_terminal_level_must_be_aggressive_spike(self) -> None:
        bid_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="102", side="BID", buy=2, sell=30),
                    refill_level_payload(price="101", side="BID", buy=2, sell=25),
                    refill_level_payload(price="100", side="BID", buy=0, sell=20, terminal_z_score="1.79"),
                )
            )
            if zone.direction == "LONG"
        ]
        ask_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="100", side="ASK", buy=30, sell=2),
                    refill_level_payload(price="101", side="ASK", buy=25, sell=2),
                    refill_level_payload(price="102", side="ASK", buy=20, sell=0, terminal_z_score="1.79"),
                )
            )
            if zone.direction == "SHORT"
        ]

        self.assertEqual(bid_zones, [])
        self.assertEqual(ask_zones, [])

    def test_zone_requires_at_least_twenty_refill_contracts_across_sequence(self) -> None:
        rejected_bid_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(
                        price="100",
                        side="BID",
                        buy=0,
                        sell=20,
                        refill_count=1,
                        refill_contracts=1,
                    ),
                )
            )
            if zone.direction == "LONG"
        ]
        accepted_bid_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(
                        price="102",
                        side="BID",
                        buy=3,
                        sell=20,
                        refill_contracts=6,
                    ),
                    refill_level_payload(
                        price="101",
                        side="BID",
                        buy=2,
                        sell=20,
                        refill_contracts=7,
                    ),
                    refill_level_payload(
                        price="100",
                        side="BID",
                        buy=0,
                        sell=20,
                        refill_contracts=7,
                    ),
                )
            )
            if zone.direction == "LONG"
        ]
        rejected_ask_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(
                        price="100",
                        side="ASK",
                        buy=20,
                        sell=0,
                        refill_count=1,
                        refill_contracts=1,
                    ),
                )
            )
            if zone.direction == "SHORT"
        ]

        self.assertEqual(rejected_bid_zones, [])
        self.assertEqual(len(accepted_bid_zones), 1)
        self.assertEqual(accepted_bid_zones[0].refill_contracts, 20)
        self.assertEqual(rejected_ask_zones, [])

    def test_zone_requires_at_least_fifteen_refills_across_sequence(self) -> None:
        rejected_bid_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(
                        price="100",
                        side="BID",
                        buy=0,
                        sell=20,
                        refill_count=14,
                        refill_contracts=28,
                    ),
                )
            )
            if zone.direction == "LONG"
        ]
        rejected_ask_zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(
                        price="100",
                        side="ASK",
                        buy=20,
                        sell=0,
                        refill_count=14,
                        refill_contracts=28,
                    ),
                )
            )
            if zone.direction == "SHORT"
        ]

        self.assertEqual(rejected_bid_zones, [])
        self.assertEqual(rejected_ask_zones, [])

    def test_opposite_level_without_refill_or_large_opposite_contracts_does_not_reset_bid_sequence(self) -> None:
        zones = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="103", side="BID", buy=3, sell=30),
                    refill_level_payload(
                        price="102",
                        side="ASK",
                        buy=20,
                        sell=0,
                        refill_count=0,
                        refill_contracts=0,
                    ),
                    refill_level_payload(price="101", side="BID", buy=2, sell=25),
                    refill_level_payload(price="100", side="BID", buy=0, sell=20),
                )
            )
            if zone.direction == "LONG"
        ]

        self.assertEqual(len(zones), 1)
        self.assertEqual([level.price for level in zones[0].levels], [Decimal("103"), Decimal("101"), Decimal("100")])

    def test_opposite_level_resets_bid_sequence_with_refill_or_more_than_twenty_opposite_contracts(self) -> None:
        reset_by_refill = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="103", side="BID", buy=3, sell=30),
                    refill_level_payload(price="102", side="ASK", buy=20, sell=0),
                    refill_level_payload(price="101", side="BID", buy=2, sell=25),
                    refill_level_payload(price="100", side="BID", buy=0, sell=20),
                )
            )
            if zone.direction == "LONG"
        ]
        reset_by_contracts = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="103", side="BID", buy=3, sell=30),
                    refill_level_payload(
                        price="102",
                        side="ASK",
                        buy=21,
                        sell=0,
                        refill_count=0,
                        refill_contracts=0,
                    ),
                    refill_level_payload(price="101", side="BID", buy=2, sell=25),
                    refill_level_payload(price="100", side="BID", buy=0, sell=20),
                )
            )
            if zone.direction == "LONG"
        ]

        self.assertEqual([level.price for level in reset_by_refill[0].levels], [Decimal("101"), Decimal("100")])
        self.assertEqual([level.price for level in reset_by_contracts[0].levels], [Decimal("101"), Decimal("100")])

    def test_opposite_level_reset_rule_is_symmetric_for_ask_sequences(self) -> None:
        kept = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="100", side="ASK", buy=30, sell=3),
                    refill_level_payload(
                        price="101",
                        side="BID",
                        buy=0,
                        sell=20,
                        refill_count=0,
                        refill_contracts=0,
                    ),
                    refill_level_payload(price="102", side="ASK", buy=25, sell=2),
                    refill_level_payload(price="103", side="ASK", buy=20, sell=0),
                )
            )
            if zone.direction == "SHORT"
        ]
        reset = [
            zone
            for zone in _refill_zones_from_payloads(
                (
                    refill_level_payload(price="100", side="ASK", buy=30, sell=3),
                    refill_level_payload(
                        price="101",
                        side="BID",
                        buy=0,
                        sell=21,
                        refill_count=0,
                        refill_contracts=0,
                    ),
                    refill_level_payload(price="102", side="ASK", buy=25, sell=2),
                    refill_level_payload(price="103", side="ASK", buy=20, sell=0),
                )
            )
            if zone.direction == "SHORT"
        ]

        self.assertEqual([level.price for level in kept[0].levels], [Decimal("100"), Decimal("102"), Decimal("103")])
        self.assertEqual([level.price for level in reset[0].levels], [Decimal("102"), Decimal("103")])

    def test_bid_zone_policy_keeps_only_new_lower_bid_zone_active(self) -> None:
        process_symbol = symbol()
        events = (
            *level_order_events(order_id="BID-HIGH", price="101", side="BID", refill_count=15, start_ms=1_000),
            *level_order_events(order_id="BID-LOW", price="99", side="BID", refill_count=15, start_ms=2_000),
            *level_order_events(order_id="BID-HIGHER", price="102", side="BID", refill_count=15, start_ms=3_000),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            *footprint_noise_bins("95", 12, buy="1", sell="0"),
                            footprint_bin("99", "99.25", buy="0", sell="80"),
                            footprint_bin("101", "101.25", buy="0", sell="80"),
                            footprint_bin("102", "102.25", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual([payload["price"] for payload in result.payloads], ["101", "99", "99"])
        self.assertEqual(result.payloads[-1]["zone_low"], "99")
        self.assertEqual(result.payloads[-1]["zone_high"], "99.25")
        self.assertEqual(result.payloads[1]["action"], "CANCEL")
        self.assertEqual(result.payloads[1]["canceled_zone_ids"], (result.payloads[0]["output_id"],))
        self.assertEqual(result.payloads[-1]["active_zone_count"], 1)

    def test_ask_zone_policy_keeps_only_new_higher_ask_zone_active(self) -> None:
        process_symbol = symbol()
        events = (
            *level_order_events(order_id="ASK-LOW", price="101", side="ASK", refill_count=15, start_ms=1_000),
            *level_order_events(order_id="ASK-HIGH", price="103", side="ASK", refill_count=15, start_ms=2_000),
            *level_order_events(order_id="ASK-LOWER", price="100", side="ASK", refill_count=15, start_ms=3_000),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("100", "100.25", buy="80", sell="0"),
                            footprint_bin("101", "101.25", buy="80", sell="0"),
                            footprint_bin("103", "103.25", buy="80", sell="0"),
                            *footprint_noise_bins("104", 12, buy="0", sell="1"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual([payload["price"] for payload in result.payloads], ["101", "103", "103"])
        self.assertEqual(result.payloads[-1]["zone_low"], "103")
        self.assertEqual(result.payloads[-1]["zone_high"], "103.25")
        self.assertEqual(result.payloads[1]["action"], "CANCEL")
        self.assertEqual(result.payloads[1]["canceled_zone_ids"], (result.payloads[0]["output_id"],))
        self.assertEqual(result.payloads[-1]["active_zone_count"], 1)

    def test_higher_ask_terminal_cancels_active_ask_zone(self) -> None:
        process_symbol = symbol()
        events = level_order_events(
            order_id="ASK-ACTIVE",
            price="101",
            side="ASK",
            refill_count=15,
            start_ms=1_000,
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("101", "101.25", buy="40", sell="0"),
                            *footprint_noise_bins("101.25", 8, buy="0", sell="1"),
                            footprint_bin("103.25", "103.50", buy="40", sell="0"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 2)
        self.assertEqual(result.payloads[0]["action"], DATA_PROCESS_ENTRY_ACTION)
        self.assertEqual(result.payloads[1]["action"], "CANCEL")
        self.assertEqual(result.payloads[1]["canceled_zone_ids"], (result.payloads[0]["output_id"],))
        self.assertEqual(result.payloads[1]["active_zone_count"], 0)

    def test_lower_bid_terminal_cancels_active_bid_zone(self) -> None:
        process_symbol = symbol()
        events = level_order_events(
            order_id="BID-ACTIVE",
            price="101",
            side="BID",
            refill_count=15,
            start_ms=1_000,
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("98.75", "99.00", buy="0", sell="40"),
                            *footprint_noise_bins("99.00", 8, buy="1", sell="0"),
                            footprint_bin("101", "101.25", buy="0", sell="40"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 2)
        self.assertEqual(result.payloads[0]["action"], DATA_PROCESS_ENTRY_ACTION)
        self.assertEqual(result.payloads[1]["action"], "CANCEL")
        self.assertEqual(result.payloads[1]["canceled_zone_ids"], (result.payloads[0]["output_id"],))
        self.assertEqual(result.payloads[1]["active_zone_count"], 0)

    def test_opposite_side_zone_policy_cancels_crossed_active_zone(self) -> None:
        process_symbol = symbol()
        events = (
            *level_order_events(order_id="BID-ACTIVE", price="100", side="BID", refill_count=15, start_ms=1_000),
            *level_order_events(order_id="ASK-BELOW", price="98", side="ASK", refill_count=15, start_ms=2_000),
            *level_order_events(order_id="BID-ABOVE", price="101", side="BID", refill_count=15, start_ms=3_000),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("98", "98.25", buy="80", sell="0"),
                            footprint_bin("100", "100.25", buy="0", sell="80"),
                            footprint_bin("101", "101.25", buy="0", sell="80"),
                            *footprint_noise_bins("102", 8, buy="0", sell="0"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual([payload["price"] for payload in result.payloads], ["100", "98", "101", "100"])
        self.assertEqual(result.payloads[1]["canceled_zone_ids"], (result.payloads[0]["output_id"],))
        self.assertEqual(result.payloads[2]["canceled_zone_ids"], (result.payloads[1]["output_id"],))
        self.assertEqual(result.payloads[3]["action"], "CANCEL")
        self.assertEqual(result.payloads[3]["canceled_zone_ids"], (result.payloads[2]["output_id"],))
        self.assertEqual(result.payloads[3]["active_zone_count"], 0)

    def test_zone_buffer_keeps_latest_state_for_same_order_at_same_level(self) -> None:
        process_symbol = symbol()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: level_order_events(
                        order_id="LATEST-SAME",
                        price="29618.25",
                        side="BID",
                        refill_count=16,
                        start_ms=1_000,
                    )
                }
            ),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            *footprint_noise_bins("29617.00", 4, buy="1", sell="1"),
                            footprint_bin("29618.25", "29618.50", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 2)
        payload = result.payloads[-1]
        self.assertEqual(payload["refill_count"], 16)
        self.assertEqual(payload["refill_contracts"], 32)
        self.assertEqual(payload["refill_filled_contracts"], 32)
        self.assertEqual(payload["zone_level_count"], 1)
        self.assertEqual(payload["zone_order_ids"], ("PRICE_LEVEL|0|29618.25|BID",))

    def test_zone_buffer_sums_independent_orders_at_same_level(self) -> None:
        process_symbol = symbol()
        events = (
            *level_order_events(
                order_id="LEVEL-ORDER-1",
                price="29618.25",
                side="BID",
                refill_count=7,
                start_ms=1_000,
            ),
            *level_order_events(
                order_id="LEVEL-ORDER-2",
                price="29618.25",
                side="BID",
                refill_count=8,
                start_ms=2_000,
            ),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            *footprint_noise_bins("29617.00", 4, buy="1", sell="1"),
                            footprint_bin("29618.25", "29618.50", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 1)
        payload = result.payloads[-1]
        self.assertEqual(payload["refill_count"], 15)
        self.assertEqual(payload["refill_contracts"], 30)
        self.assertEqual(payload["zone_level_count"], 1)
        self.assertEqual(payload["zone_order_ids"], ("PRICE_LEVEL|0|29618.25|BID",))

    def test_price_change_does_not_transfer_refill_context_to_new_level(self) -> None:
        process_symbol = symbol()
        events_list = [
            raw(1_000, action="A", size=10, price="29618.25", side="BID", order_id="MOVING-ORDER"),
        ]
        for index in range(2):
            event_ms = 1_100 + index * 100
            events_list.append(raw(event_ms, action="F", size=2, price="29618.25", side="BID", order_id="MOVING-ORDER"))
            events_list.append(raw(event_ms + 1, action="M", size=10, price="29618.25", side="BID", order_id="MOVING-ORDER"))
        for index in range(13):
            event_ms = 1_300 + index * 100
            events_list.append(raw(event_ms, action="F", size=2, price="29617.25", side="BID", order_id="MOVING-ORDER"))
            events_list.append(raw(event_ms + 1, action="M", size=10, price="29617.25", side="BID", order_id="MOVING-ORDER"))
        events = tuple(events_list)
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            footprint_bin("29618.25", "29618.50", buy="1", sell="70"),
                            *footprint_noise_bins("29618.50", 10, buy="1", sell="0"),
                            footprint_bin("29617.25", "29617.50", buy="0", sell="90"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=60_000, symbols=(process_symbol,)))

        self.assertEqual(result.emitted_payload_count, 0)

    def test_empty_footprint_price_bin_keeps_market_buy_sell_zero(self) -> None:
        process_symbol = symbol()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: (
                        raw(1_000, action="A", size=2),
                        *fill_refill_events(5),
                        raw(4_000, action="C", size=2),
                    )
                }
            ),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        {
                            "open_time_ms": 0,
                            "close_time_ms": 59_999,
                            "provider_symbol": "NQ.FUT",
                            "bins": (
                                {
                                    "low": "29618.00",
                                    "high": "29618.25",
                                    "l2": {
                                        "buy_contracts": "4",
                                        "sell_contracts": "1",
                                        "ask_traded_contracts": "4",
                                        "bid_traded_contracts": "1",
                                    },
                                },
                                {
                                    "low": "29618.25",
                                    "high": "29618.50",
                                    "l2": {
                                        "total_contracts": "0",
                                        "buy_contracts": "0",
                                        "sell_contracts": "0",
                                        "ask_traded_contracts": "0",
                                        "bid_traded_contracts": "0",
                                    },
                                },
                                {
                                    "low": "29618.50",
                                    "high": "29618.75",
                                    "l2": {
                                        "buy_contracts": "9",
                                        "sell_contracts": "2",
                                        "ask_traded_contracts": "9",
                                        "bid_traded_contracts": "2",
                                    },
                                },
                            ),
                        },
                    )
                }
            ),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(
            ProcessReplayRequest(
                start_ms=0,
                end_ms=60_000,
                symbols=(process_symbol,),
            )
        )

        self.assertEqual(result.emitted_payload_count, 0)
        self.assertEqual(result.payloads, tuple())

    def test_ignores_refilled_order_cancelled_without_trade(self) -> None:
        process_symbol = symbol()
        events = [
            raw(1_000, action="A", size=2),
            *[
                raw(2_000 + index, action="M", size=3 + index)
                for index in range(10)
            ],
            raw(4_000, action="C", size=12),
        ]
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: tuple(events)}),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(
            ProcessReplayRequest(start_ms=1_000, end_ms=4_000)
        )

        self.assertEqual(result.emitted_payload_count, 0)
        self.assertEqual(result.payloads, tuple())

    def test_ignores_modify_growth_before_fill(self) -> None:
        process_symbol = symbol()
        events = [
            raw(1_000, action="A", size=2),
            *[
                raw(2_000 + index, action="M", size=3 + index)
                for index in range(10)
            ],
            raw(3_000, action="F", size=5),
            raw(4_000, action="C", size=7),
        ]
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: tuple(events)}),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(
            ProcessReplayRequest(start_ms=1_000, end_ms=4_000)
        )

        self.assertEqual(result.emitted_payload_count, 0)
        self.assertEqual(result.payloads, tuple())

    def test_store_sink_publishes_dom_compatible_payload_for_candles(self) -> None:
        process_symbol = symbol()
        output_store = EngineOutputStore()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: level_order_events(
                        order_id="STORE-ZONE",
                        price="29618.25",
                        side="BID",
                        refill_count=41,
                        start_ms=1_000,
                    )
                }
            ),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            *footprint_noise_bins("29617.00", 4, buy="1", sell="1"),
                            footprint_bin("29618.25", "29618.50", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
            sinks=(EngineOutputStoreSink(output_store),),
        )

        engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=3_100))

        native_outputs = output_store.outputs(
            producer=DATA_PROCESS_ENGINE_PRODUCER,
            output_type=DATA_PROCESS_REFILL_OUTPUT_TYPE,
            provider_symbol="NQ.FUT",
            timeframe="M1",
            start_ms=1_000,
            end_ms=3_100,
        )
        self.assertEqual(len(native_outputs), 27)
        latest_native = native_outputs[-1]
        self.assertEqual(latest_native["type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(latest_native["payload_type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(latest_native["output_type"], DATA_PROCESS_REFILL_OUTPUT_TYPE)

        outputs = output_store.outputs(
            producer="dom",
            output_type=DOM_POSITIVE_REFILL_OUTPUT_TYPE,
            provider_symbol="NQ.FUT",
            timeframe="M1",
            start_ms=1_000,
            end_ms=3_100,
        )
        self.assertEqual(len(outputs), 27)
        latest_output = outputs[-1]
        self.assertEqual(latest_output["type"], DOM_POSITIVE_REFILL_OUTPUT_TYPE)
        self.assertEqual(latest_output["output_type"], DOM_POSITIVE_REFILL_OUTPUT_TYPE)
        self.assertEqual(latest_output["payload_type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(latest_output["action"], DATA_PROCESS_ENTRY_ACTION)
        self.assertEqual(latest_output["payload_id"], latest_output["output_id"])
        self.assertEqual(latest_output["payload_id"], latest_output["id"])
        self.assertEqual(latest_output["positive_refill_count"], 41)
        self.assertEqual(latest_output["positive_refill_total"], 82)
        self.assertEqual(latest_output["positive_refill_filled_total"], 82)
        self.assertEqual(latest_output["refill_filled_contracts"], 82)
        self.assertEqual(latest_output["zone_low"], "29618.25")
        self.assertEqual(latest_output["zone_high"], "29618.50")
        self.assertEqual(latest_output["trade_count"], 41)
        self.assertEqual(latest_output["market_buy"], 0)
        self.assertEqual(latest_output["market_sell"], 80)

    def test_csv_sink_logs_dispatched_payloads(self) -> None:
        process_symbol = symbol()
        payload_path = Path("payloads.csv")
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: level_order_events(
                        order_id="CSV-ZONE",
                        price="29618.25",
                        side="BID",
                        refill_count=41,
                        start_ms=1_000,
                    )
                }
            ),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        footprint_candle(
                            *footprint_noise_bins("29617.00", 4, buy="1", sell="1"),
                            footprint_bin("29618.25", "29618.50", buy="0", sell="80"),
                        ),
                    )
                }
            ),
            config=DataProcessConfig(),
            sinks=(CsvProcessLogSink(payload_path),),
        )

        with patch("process.sinks._append_rows") as append_rows:
            engine.run_replay(ProcessReplayRequest(start_ms=0, end_ms=3_100))

        self.assertEqual(append_rows.call_count, 27)
        path_arg, _fields_arg, rows = append_rows.call_args.args
        self.assertEqual(path_arg, payload_path)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["payload_id"], rows[0]["output_id"])
        self.assertEqual(rows[0]["type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(rows[0]["payload_type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(rows[0]["output_type"], DATA_PROCESS_REFILL_OUTPUT_TYPE)
        self.assertEqual(rows[0]["order_id"], "PRICE_LEVEL|0|29618.25|BID")
        self.assertEqual(rows[0]["action"], DATA_PROCESS_ENTRY_ACTION)
        self.assertEqual(rows[0]["refill_count"], 41)
        self.assertEqual(rows[0]["trade_count"], 41)
        self.assertEqual(rows[0]["zone_low"], "29618.25")
        self.assertEqual(rows[0]["zone_high"], "29618.50")
        self.assertIn("market_buy", _fields_arg)
        self.assertIn("market_sell", _fields_arg)
        self.assertIn("payload_id", _fields_arg)
        self.assertIn("payload_type", _fields_arg)
        self.assertIn("output_type", _fields_arg)
        self.assertIn("action", _fields_arg)
        self.assertIn("zone_low", _fields_arg)
        self.assertIn("zone_high", _fields_arg)

    def test_replay_loads_footprint_context_for_same_symbols_and_range(self) -> None:
        process_symbol = symbol()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: tuple()}),
            footprint_source=InMemoryProcessFootprintSource(
                {
                    process_symbol: (
                        {"open_time_ms": 1_000, "provider_symbol": "NQ.FUT"},
                        {"open_time_ms": 10_000, "provider_symbol": "NQ.FUT"},
                    )
                }
            ),
        )

        result = engine.run_replay(
            ProcessReplayRequest(
                start_ms=1_000,
                end_ms=2_000,
                symbols=(process_symbol,),
            )
        )

        self.assertEqual(result.footprint_candle_count, 1)
        self.assertEqual(len(result.footprints), 1)
        self.assertEqual(result.footprints[0].symbol.provider_symbol, "NQ.FUT")
        self.assertEqual(result.footprints[0].candles[0]["open_time_ms"], 1_000)

    def test_cme_footprint_source_preserves_aggressor_side_mapping(self) -> None:
        base_ms = 1_000
        frame = pd.DataFrame(
            [
                {
                    "ts_event": (base_ms + 10) * 1_000_000,
                    "side": "B",
                    "price": 16_000 * 1_000_000_000,
                    "size": 3,
                    "symbol": "NQM6",
                },
                {
                    "ts_event": (base_ms + 20) * 1_000_000,
                    "side": "A",
                    "price": 16_000 * 1_000_000_000,
                    "size": 2,
                    "symbol": "NQM6",
                },
            ]
        )
        trade_store = _FakeCmeTradeStore(frame)
        source = CmeFootprintReplaySource(
            catalog=_FakeCmeCatalog(),
            trade_store=trade_store,
            dataset="GLBX.MDP3",
            schema="trades",
            timeframe="M1",
            interval="1m",
            session_start_hour_chicago=17,
        )
        process_symbol = source.symbols()[0]

        candles = tuple(
            source.candles(
                process_symbol,
                start_ms=base_ms,
                end_ms=base_ms + 60_000,
            )
        )

        self.assertEqual(
            trade_store.requests,
            [("NQ.FUT", base_ms, base_ms + 60_000, 17)],
        )
        self.assertEqual(len(candles), 1)
        candle = candles[0]
        self.assertEqual(source.bin_tick_count, 1)
        self.assertEqual(candle["bin_tick_count"], 1)
        self.assertEqual(candle["buy_contracts"], "3")
        self.assertEqual(candle["sell_contracts"], "2")
        self.assertEqual(candle["delta_contracts"], "1")
        self.assertEqual(candle["bins"][0]["low"], "16000.000")
        self.assertEqual(candle["bins"][0]["high"], "16000.250")
        traded_bin = candle["bins"][0]["l2"]
        self.assertEqual(traded_bin["buy_contracts"], "3")
        self.assertEqual(traded_bin["sell_contracts"], "2")
        self.assertEqual(traded_bin["ask_traded_contracts"], "3")
        self.assertEqual(traded_bin["bid_traded_contracts"], "2")

    def test_databento_fill_modify_pairs_keep_order_alive_until_cancel(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="A", size=2),
            raw(1_100, action="F", size=1),
            raw(1_101, action="M", size=1),
            raw(1_200, action="F", size=1),
            raw(1_201, action="M", size=2),
            raw(1_300, action="F", size=1),
            raw(1_301, action="M", size=1),
            raw(1_400, action="C", size=1),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=1_400))

        self.assertEqual(result.emitted_payload_count, 0)
        self.assertEqual(result.payloads, tuple())

    def test_refill_count_requires_replenishment_after_fill(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="A", size=3, price="28874.5", order_id="6877548059913"),
            raw(1_100, action="M", size=3, price="28875.5", order_id="6877548059913"),
            raw(2_000, action="F", size=3, price="28875.5", order_id="6877548059913"),
            raw(2_001, action="M", size=3, price="28875.5", order_id="6877548059913"),
            raw(2_002, action="F", size=7, price="28875.5", order_id="6877548059913"),
            raw(2_003, action="M", size=2, price="28875.5", order_id="6877548059913"),
            raw(2_004, action="F", size=7, price="28875.5", order_id="6877548059913"),
            raw(2_005, action="M", size=1, price="28875.5", order_id="6877548059913"),
            raw(2_006, action="F", size=1, price="28875.5", order_id="6877548059913"),
            raw(2_007, action="M", size=2, price="28875.5", order_id="6877548059913"),
            raw(2_008, action="F", size=1, price="28875.5", order_id="6877548059913"),
            raw(2_009, action="M", size=1, price="28875.5", order_id="6877548059913"),
            raw(2_010, action="F", size=1, price="28875.5", order_id="6877548059913"),
            raw(2_011, action="C", size=1, price="28875.5", order_id="6877548059913"),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=2_011))

        self.assertEqual(result.emitted_payload_count, 0)
        self.assertEqual(result.payloads, tuple())

    def test_fills_without_replenishment_do_not_emit_refill_payload(self) -> None:
        process_symbol = symbol()
        events = (
            raw(1_000, action="A", size=3),
            raw(1_100, action="F", size=1),
            raw(1_200, action="F", size=1),
            raw(1_300, action="F", size=1),
            raw(1_400, action="C", size=1),
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({process_symbol: events}),
            config=DataProcessConfig(),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=1_400))

        self.assertEqual(result.emitted_payload_count, 0)


if __name__ == "__main__":
    unittest.main()
