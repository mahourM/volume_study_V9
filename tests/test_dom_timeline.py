from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path
from typing import Any

from DOM.data_provider import DomFileDataProvider
from DOM.engine import DomEngineConfig, DomTimelineEngine
from DOM.models import DomContext, DomFileInfo, DomProviderResult, DomRawEvent


def _context() -> DomContext:
    return DomContext(
        mt5_symbol="NQ",
        provider_symbol="NQ.FUT",
        market_provider="CME_LOCAL_DBN",
        timeframe="M5",
        interval="5m",
        dataset="GLBX.MDP3",
        schema="mbo",
        tick_size=Decimal("0.25"),
        session_start_hour_chicago=17,
        timezone="America/Vancouver",
        trigger_timeout_candles=100,
        initial_view_candles=20,
        retention_ms=300_000 * 100,
        data_dir=Path("DOM"),
    )


def _engine(provider: Any) -> DomTimelineEngine:
    return DomTimelineEngine(
        provider=provider,
        config=DomEngineConfig(
            window_cache_size=8,
            max_events_per_window=500,
            max_resting_segments_per_window=500,
            max_line_points_per_window=500,
            max_price_levels=50,
            time_bucket_divisor=60,
            render_overscan_multiplier=4,
            render_overscan_max_ms=60_000,
        ),
    )


def _file_provider(cache_dir: Path) -> DomFileDataProvider:
    return DomFileDataProvider(
        data_dir=cache_dir,
        extracted_cache_dir=cache_dir,
        file_globs=("*.zip",),
        file_cache_size=1,
        dbn_batch_size=100,
        max_events_per_request=100,
        stream_bucket_ms=60_000,
        stream_cache_max_buckets=1,
    )


def test_dom_index_prevents_parallel_builds_for_the_same_source(tmp_path: Path) -> None:
    first = _file_provider(tmp_path)
    second = _file_provider(tmp_path)

    with first._source_index_file_lock("source") as first_acquired:
        assert first_acquired is True
        with second._source_index_file_lock("source") as second_acquired:
            assert second_acquired is False

    with second._source_index_file_lock("source") as acquired_after_release:
        assert acquired_after_release is True


def test_dom_index_enforces_unique_source_ordinals(tmp_path: Path) -> None:
    provider = _file_provider(tmp_path)
    with provider._index_connection() as connection:
        event = ("source", 1, 1000, "100", 1, "BID", "T", "order", 1)
        connection.execute(
            """
            INSERT INTO dom_events(
                source_key, ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            event,
        )
        try:
            connection.execute(
                """
                INSERT INTO dom_events(
                    source_key, ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                event,
            )
        except sqlite3.IntegrityError:
            pass
        else:
            raise AssertionError("duplicate source ordinal was accepted")


class _EmptyProvider:
    def cache_signature(self, context: DomContext) -> tuple[tuple[str, int, int], ...]:
        del context
        return ()

    def events_for_window(self, *args: Any, **kwargs: Any) -> DomProviderResult:
        raise AssertionError("empty DOM folders should not attempt to read events")


class _MemoryProvider:
    def __init__(self, events: tuple[DomRawEvent, ...]) -> None:
        self.events = events
        self.files = (
            DomFileInfo(
                path=Path("DOM") / "sample.dbn",
                name="sample.dbn",
                size_bytes=123,
                modified_ns=456,
            ),
        )

    def cache_signature(self, context: DomContext) -> tuple[tuple[str, int, int], ...]:
        del context
        return (("DOM/sample.dbn", 456, 123),)

    def events_for_window(
        self,
        context: DomContext,
        *,
        start_ms: int,
        end_ms: int,
        read_through: bool = False,
        primary_start_ms: int | None = None,
        primary_end_ms: int | None = None,
    ) -> DomProviderResult:
        del context, read_through, primary_start_ms, primary_end_ms
        events = tuple(
            event
            for event in self.events
            if int(start_ms) <= int(event.ts_event_ms) <= int(end_ms)
        )
        return DomProviderResult(
            files=self.files,
            events=events,
            earliest_event_time_ms=min(event.ts_event_ms for event in self.events),
            latest_event_time_ms=max(event.ts_event_ms for event in self.events),
            contract_symbols=("NQM6",),
            sampled=False,
            status="READY",
            message="",
        )


def _raw(
    ts_event_ms: int,
    *,
    action: str,
    side: str,
    price: str,
    size: int,
    order_id: str,
    instrument_id: int = 1,
) -> DomRawEvent:
    return DomRawEvent(
        ts_event_ms=ts_event_ms,
        price=Decimal(price),
        size=size,
        side=side,
        action=action,
        order_id=order_id,
        instrument_id=instrument_id,
        sequence=ts_event_ms,
        source_file="sample.dbn",
    )


def test_dom_timeline_empty_folder_returns_no_files_payload() -> None:
    payload = _engine(_EmptyProvider()).timeline_window(_context())

    assert payload["status"] == "NO_DOM_FILES"
    assert payload["message"] == "No DOM files found"
    assert payload["events"] == []
    assert payload["order_book_levels"] == []
    assert payload["debug"]["dom_file_count"] == 0


def test_dom_timeline_engine_builds_order_state_events_and_resting_book() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=5, order_id="1"),
        _raw(1200, action="A", side="ASK", price="100.25", size=3, order_id="2"),
        _raw(1500, action="M", side="BID", price="100", size=7, order_id="1"),
        _raw(2000, action="F", side="ASK", price="100.25", size=2, order_id="2"),
        _raw(3000, action="C", side="BID", price="100", size=7, order_id="1"),
    )
    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=4000,
    )

    assert [event["event_type"] for event in payload["events"]] == [
        "ADD",
        "ADD",
        "MODIFY",
        "EXECUTE",
        "CANCEL_DELETE",
    ]
    assert payload["debug"]["add_count"] == 2
    assert payload["debug"]["modify_count"] == 1
    assert payload["debug"]["execute_count"] == 1
    assert payload["debug"]["cancel_delete_count"] == 1
    assert payload["events"][0]["symbol"] == "NQ.FUT"
    assert payload["events"][0]["provider_symbol"] == "NQ.FUT"
    assert payload["events"][0]["mt5_symbol"] == "NQ"
    assert payload["events"][0]["order_id"] == "1"
    assert payload["events"][0]["venue_order_id"] == "1"
    assert payload["events"][0]["event_id"].startswith("NQ.FUT|")
    assert any(
        level["price"] == "100.25"
        and level["bid_contracts"] == 0
        and level["ask_contracts"] == 1
        for level in payload["order_book_levels"]
    )
    assert any(
        segment["event_type"] == "RESTING_LIQUIDITY"
        and segment["side"] == "BID"
        and segment["price"] == "100"
        for segment in payload["resting_segments"]
    )
    assert payload["trigger_timeout_candles"] == 100
    assert payload["initial_view_candles"] == 20
    assert payload["retention_ms"] == 300_000 * 100


def test_dom_timeline_execute_counts_fill_action_not_trade_print() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=5, order_id="bid-1"),
        _raw(1010, action="A", side="ASK", price="100.25", size=4, order_id="ask-1"),
        _raw(1200, action="F", side="BID", price="100", size=2, order_id="bid-1"),
        _raw(1200, action="T", side="ASK", price="100", size=2, order_id="trade-1"),
    )

    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=2000,
    )

    execute_events = [event for event in payload["events"] if event["event_type"] == "EXECUTE"]
    assert len(execute_events) == 1
    assert execute_events[0]["action"] == "F"
    assert execute_events[0]["side"] == "BID"
    assert execute_events[0]["executed_contracts"] == 2
    assert all(event["action"] != "T" for event in payload["events"])
    assert payload["debug"]["execute_count"] == 1


def test_dom_timeline_emits_positive_refill_engine_outputs() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=2, order_id="refill-1"),
        *(
            event
            for index in range(10)
            for event in (
                _raw(
                    1100 + index * 200,
                    action="F",
                    side="BID",
                    price="100",
                    size=1,
                    order_id="refill-1",
                ),
                _raw(
                    1101 + index * 200,
                    action="M",
                    side="BID",
                    price="100",
                    size=2,
                    order_id="refill-1",
                ),
            )
        ),
    )

    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=4000,
    )

    points = payload["dom_refill_points"]
    outputs = payload["engine_outputs"]["dom_positive_refills"]

    assert len(points) == 1
    assert len(outputs) == 1
    assert outputs[0]["type"] == "DOM_POSITIVE_REFILL"
    assert outputs[0]["producer"] == "dom"
    assert outputs[0]["provider_symbol"] == "NQ.FUT"
    assert outputs[0]["mt5_symbol"] == "NQ"
    assert outputs[0]["timeframe"] == "M5"
    assert outputs[0]["price"] == "100"
    assert outputs[0]["side"] == "BID"
    assert outputs[0]["order_id"] == ""
    assert outputs[0]["order_ids"] == ("refill-1",)
    assert outputs[0]["order_count"] == 1
    assert outputs[0]["refill_method"] == "price_base_refill"
    assert outputs[0]["positive_refill_count"] == 10
    assert outputs[0]["positive_refill_total"] == 10
    assert outputs[0]["date"]
    assert outputs[0]["id"].startswith("DOM|DOM_POSITIVE_REFILL|NQ.FUT|M5|")


def test_dom_timeline_aggregates_refills_by_price_across_order_ids() -> None:
    events = []
    for order_index in range(2):
        order_id = f"split-{order_index}"
        events.append(_raw(1000 + order_index, action="A", side="BID", price="100", size=2, order_id=order_id))
        for refill_index in range(5):
            event_ms = 1100 + order_index * 1000 + refill_index * 200
            events.extend(
                (
                    _raw(event_ms, action="F", side="BID", price="100", size=1, order_id=order_id),
                    _raw(event_ms + 1, action="M", side="BID", price="100", size=2, order_id=order_id),
                )
            )

    payload = _engine(_MemoryProvider(tuple(sorted(events, key=lambda event: event.ts_event_ms)))).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=4000,
    )

    outputs = payload["engine_outputs"]["dom_positive_refills"]
    assert len(outputs) == 1
    assert outputs[0]["positive_refill_count"] == 10
    assert outputs[0]["positive_refill_total"] == 10
    assert outputs[0]["order_ids"] == ("split-0", "split-1")
    assert outputs[0]["order_count"] == 2
    assert outputs[0]["refill_method"] == "price_base_refill"


def test_dom_timeline_ignores_modify_without_positive_size_delta_after_fill() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=5, order_id="refill-1"),
        _raw(1100, action="F", side="BID", price="100", size=1, order_id="refill-1"),
        _raw(1101, action="M", side="BID", price="100", size=4, order_id="refill-1"),
        _raw(1200, action="M", side="BID", price="100", size=3, order_id="refill-1"),
    )

    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=4000,
    )

    assert payload["engine_outputs"]["dom_positive_refills"] == []


def test_dom_timeline_ignores_modify_growth_without_fill() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=1, order_id="refill-1"),
        *(
            _raw(
                1100 + index * 100,
                action="M",
                side="BID",
                price="100",
                size=2 + index,
                order_id="refill-1",
            )
            for index in range(10)
        ),
    )

    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=4000,
    )

    assert payload["dom_refill_points"] == []
    assert payload["engine_outputs"]["dom_positive_refills"] == []


def test_dom_timeline_ignores_fills_without_replenishment() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=3, order_id="refill-1"),
        *(
            _raw(
                1100 + index * 100,
                action="F",
                side="BID",
                price="100",
                size=1,
                order_id="refill-1",
            )
            for index in range(10)
        ),
    )

    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=1000,
        end_time_ms=4000,
    )

    assert payload["dom_refill_points"] == []
    assert payload["engine_outputs"]["dom_positive_refills"] == []


def test_dom_timeline_explicit_window_does_not_snap_to_event_time() -> None:
    events = (
        _raw(1000, action="A", side="BID", price="100", size=5, order_id="1"),
    )
    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=2000,
        end_time_ms=3000,
    )

    assert payload["window_start_ms"] == 2000
    assert payload["window_end_ms"] == 3000


def test_dom_timeline_keeps_events_inside_explicit_window() -> None:
    outside_before = tuple(
        _raw(1000 + index * 100, action="A", side="BID", price="99", size=1, order_id=f"b{index}")
        for index in range(6)
    )
    inside = (
        _raw(5000, action="A", side="BID", price="100", size=5, order_id="inside-bid"),
        _raw(5500, action="A", side="ASK", price="100.25", size=4, order_id="inside-ask"),
    )
    outside_after = tuple(
        _raw(9000 + index * 100, action="A", side="ASK", price="101", size=1, order_id=f"a{index}")
        for index in range(6)
    )

    payload = _engine(_MemoryProvider(outside_before + inside + outside_after)).timeline_window(
        _context(),
        start_time_ms=5000,
        end_time_ms=6000,
    )

    inside_order_ids = [
        event["order_id"]
        for event in payload["events"]
        if 5000 <= int(event["timestamp_ms"]) <= 6000
    ]
    assert inside_order_ids == ["inside-bid", "inside-ask"]


def test_dom_timeline_quote_lines_ignore_crossed_outliers() -> None:
    events = (
        _raw(5000, action="A", side="BID", price="105", size=1, order_id="bad-bid"),
        _raw(5010, action="A", side="ASK", price="100", size=1, order_id="bad-ask"),
        _raw(5020, action="A", side="BID", price="99.75", size=1, order_id="good-bid"),
        _raw(5030, action="A", side="ASK", price="100.25", size=1, order_id="good-ask"),
    )
    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=5000,
        end_time_ms=6000,
    )

    bid_prices = [Decimal(point["price"]) for point in payload["best_bid_line"]]
    ask_prices = [Decimal(point["price"]) for point in payload["best_ask_line"]]
    assert bid_prices
    assert ask_prices
    assert max(bid_prices) < min(ask_prices)


def test_dom_timeline_filters_to_primary_instrument_for_viewport() -> None:
    events = (
        _raw(5000, action="A", side="BID", price="29075", size=10, order_id="front-bid", instrument_id=1),
        _raw(5010, action="A", side="ASK", price="29075.25", size=10, order_id="front-ask", instrument_id=1),
        _raw(5020, action="A", side="BID", price="29366", size=1, order_id="next-bid", instrument_id=2),
        _raw(5030, action="A", side="ASK", price="29366.25", size=1, order_id="next-ask", instrument_id=2),
    )
    payload = _engine(_MemoryProvider(events)).timeline_window(
        _context(),
        start_time_ms=5000,
        end_time_ms=6000,
    )

    assert {event["order_id"] for event in payload["events"]} == {"front-bid", "front-ask"}
    assert {point["price"] for point in payload["best_bid_line"]} <= {"29075"}
    assert {point["price"] for point in payload["best_ask_line"]} <= {"29075.25"}


def test_dom_timeline_cache_debug_uses_final_window_fields() -> None:
    events = (
        _raw(5000, action="A", side="BID", price="100", size=5, order_id="1"),
        _raw(5200, action="A", side="ASK", price="100.25", size=3, order_id="2"),
    )
    engine = _engine(_MemoryProvider(events))

    first = engine.timeline_window(_context(), start_time_ms=5000, end_time_ms=6000)
    second = engine.timeline_window(_context(), start_time_ms=5000, end_time_ms=6000)

    assert first["debug"]["requested_start_ms"] == 5000
    assert first["debug"]["requested_end_ms"] == 6000
    assert first["debug"]["plan_start_ms"] == 5000
    assert first["debug"]["plan_end_ms"] == 6000
    assert first["debug"]["render_start_ms"] == first["window_start_ms"]
    assert first["debug"]["render_end_ms"] == first["window_end_ms"]
    assert first["debug"]["buffer_start_ms"] == first["render_start_ms"]
    assert first["debug"]["buffer_end_ms"] == first["render_end_ms"]
    assert first["debug"]["provider_event_count"] >= first["debug"]["visible_event_count"]
    assert second["debug"]["cache_hit_count"] == 1
