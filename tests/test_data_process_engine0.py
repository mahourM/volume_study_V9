from __future__ import annotations

import unittest
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping
from unittest.mock import patch

from absorption.html_server import (
    _candles_html_page,
    _process_replay_payload_for_timeframe,
)
from DOM.models import DomRawEvent
from core.engine_output_bus import DOM_POSITIVE_REFILL_OUTPUT_TYPE, EngineOutputStore
from process.dataProcessEngine import DataProcessEngine
from process.data_sources import InMemoryProcessEventSource
from process.models import (
    DATA_PROCESS_REFILL_OUTPUT_TYPE,
    DataProcessConfig,
    ProcessReplayRequest,
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
        events.append(raw(event_ms, action="F", size=1))
        events.append(raw(event_ms + 1, action="M", size=2))
    return tuple(events)


class _CollectingSink:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def publish(self, payloads: list[Mapping[str, Any]] | tuple[Mapping[str, Any], ...]) -> None:
        self.payloads.extend(dict(payload) for payload in payloads)


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


class DataProcessEngineTests(unittest.TestCase):
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

    def test_candle_page_exposes_vancouver_replay_and_refill_scan_controls(self) -> None:
        page = _candles_html_page("M1")

        self.assertIn('id="process-replay-form"', page)
        self.assertIn("Vancouver", page)
        self.assertIn("font-size: 20px;", page)
        self.assertIn("font-weight: 150;", page)
        self.assertIn("function replayViewportEndMs(replay)", page)
        self.assertIn("const replayMarkersByCandleOpen = new Map();", page)
        self.assertIn("function rememberReplayPayloadMarkers(replay)", page)
        self.assertIn("rememberReplayPayloadMarkers(replay);", page)
        self.assertIn("function applyReplayMarkersToCharts()", page)
        self.assertIn("const appliedMarkerCount = applyReplayMarkersToCharts();", page)
        self.assertIn("mergeDomRefillMarkers(", page)
        self.assertIn("replay?.payload_log_path", page)
        self.assertIn('appliedMarkerCount > 0 ? " drawn" : ""', page)
        self.assertNotIn("const targetEnd = replayViewportEndMs(replay)", page)
        self.assertNotIn("requestedWindowEndMs = targetEnd;", page)
        self.assertNotIn("refresh(targetEnd, requestedCandleLimit())", page)
        self.assertIn("ctx.lineWidth = 2.5;", page)
        self.assertIn("ctx.setLineDash([8, 5]);", page)
        self.assertNotIn("requestViewportWindow(replayEnd", page)
        self.assertIn("const DOM_REFILL_MARKER_MIN_COUNT = 1;", page)
        self.assertIn("refillCount < minimumRefillCount", page)
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

    def test_store_sink_receives_no_payload_without_valid_zone(self) -> None:
        process_symbol = symbol()
        output_store = EngineOutputStore()
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: (
                        raw(1_000, action="A", size=2),
                        *fill_refill_events(5),
                        raw(3_100, action="C", size=2),
                    )
                }
            ),
            config=DataProcessConfig(),
            sinks=(EngineOutputStoreSink(output_store),),
        )

        engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=3_100))

        outputs = output_store.outputs(
            producer="dom",
            output_type=DOM_POSITIVE_REFILL_OUTPUT_TYPE,
            provider_symbol="NQ.FUT",
            timeframe="M1",
            start_ms=1_000,
            end_ms=3_100,
        )
        self.assertEqual(outputs, tuple())

    def test_csv_sink_is_not_called_without_valid_zone_payload(self) -> None:
        process_symbol = symbol()
        payload_path = Path("payloads.csv")
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource(
                {
                    process_symbol: (
                        raw(1_000, action="A", size=2),
                        *fill_refill_events(5),
                        raw(3_100, action="C", size=2),
                    )
                }
            ),
            config=DataProcessConfig(),
            sinks=(CsvProcessLogSink(payload_path),),
        )

        with patch("process.sinks._append_rows") as append_rows:
            engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=3_100))

        append_rows.assert_not_called()

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


if __name__ == "__main__":
    unittest.main()
