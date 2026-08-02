from __future__ import annotations

import unittest
from unittest.mock import patch
from dataclasses import replace
from decimal import Decimal

from DOM.models import DomRawEvent
from absorption.session_service import (
    _aggregate_refill_scan_payloads,
    _displayed_execution_rate,
    _matches_refill_activity_filter,
    _load_refill_scan_disk_cache,
    _spike_score_scan_payloads,
    _store_refill_scan_disk_cache,
)
from process.models import ProcessFootprintSnapshot
from process.dataProcessEngine import DataProcessEngine
from process.data_sources import InMemoryProcessEventSource, InMemoryProcessFootprintSource
from process.models import DataProcessConfig, ProcessReplayRequest, ProcessSymbol


SYMBOL = ProcessSymbol(
    provider_symbol="NQ.FUT", mt5_symbol="NQ", market_provider="CME_LOCAL_DBN",
    dataset="GLBX.MDP3", schema="mbo", tick_size=Decimal("0.25"),
    timeframe="M1", interval="1m",
)


def event(seq: int, action: str, size: int, *, order="A", price="100", side="BID"):
    return DomRawEvent(
        ts_event_ms=60_000 + seq, price=Decimal(price), size=size, side=side,
        action=action, order_id=order, instrument_id=1, sequence=seq,
        source_file="test.dbn",
    )


def run(events):
    source = InMemoryProcessEventSource({SYMBOL: tuple(events)})
    engine = DataProcessEngine(
        event_source=source,
        config=DataProcessConfig(emit_individual_refill_orders=True),
    )
    result = engine.run_replay(ProcessReplayRequest(60_000, 119_999, (SYMBOL,)))
    return result, _aggregate_refill_scan_payloads(result.payloads, start_ms=60_000, end_ms=119_999)


def run_activity(events):
    source = InMemoryProcessEventSource({SYMBOL: tuple(events)})
    engine = DataProcessEngine(
        event_source=source,
        config=DataProcessConfig(
            emit_individual_refill_orders=True,
            emit_price_activity_levels=True,
        ),
    )
    result = engine.run_replay(ProcessReplayRequest(60_000, 119_999, (SYMBOL,)))
    return _aggregate_refill_scan_payloads(result.payloads, start_ms=60_000, end_ms=119_999)


def metric(events, *, price="100", side="BID"):
    _, aggregates = run(events)
    matches = [row for row in aggregates if row["price"] == price and row["side"] == side]
    if not matches:
        return {"refill_count": 0, "refill_added_contracts": 0,
                "executed_refill_contracts": 0, "withdrawn_refill_contracts": 0,
                "refill_execution_rate": 0.0}
    return matches[0]


class RefillExecutionRateTests(unittest.TestCase):
    def test_refill_scan_disk_cache_survives_service_restart(self):
        class MemoryPath:
            def __init__(self):
                self.parent = self
                self.text = ""

            def mkdir(self, **_kwargs):
                return None

            def with_suffix(self, _suffix):
                return self

            def write_text(self, text, **_kwargs):
                self.text = text

            def replace(self, _destination):
                return None

            def read_text(self, **_kwargs):
                return self.text

        cache_path = MemoryPath()
        with patch(
            "absorption.session_service._refill_scan_disk_cache_path",
            return_value=cache_path,
        ):
            cache_key = ("v5", "M1", 60_000, 120_000, ((1, 2),))
            expected = {
                "aggregate_payloads": ({"marker_time_ms": 60_000, "price": "100"},),
                "processed_event_count": 123,
                "emitted_payload_count": 1,
                "footprint_candle_count": 1,
                "footprint_symbols": ({"provider_symbol": "NQ.FUT"},),
                "spike_score_payloads": (),
            }

            _store_refill_scan_disk_cache(None, cache_key, expected)
            loaded = _load_refill_scan_disk_cache(None, cache_key)

            self.assertIsNotNone(loaded)
            self.assertEqual(loaded["aggregate_payloads"][0]["marker_time_ms"], 60_000)
            self.assertEqual(loaded["aggregate_payloads"][0]["price"], "100")
            self.assertEqual(loaded["processed_event_count"], 123)
            self.assertEqual(loaded["footprint_symbols"], expected["footprint_symbols"])

    def test_aggregate_only_scan_matches_price_activity_without_event_payloads(self):
        events = (
            event(1, "A", 2),
            event(2, "F", 1),
            event(3, "M", 4),
            event(4, "F", 3),
        )
        footprints = (
            {
                "open_time_ms": 60_000,
                "close_time_ms": 119_999,
                "bins": ({"low": "99", "high": "101"},),
            },
        )

        def replay(*, collect_event_payloads: bool):
            engine = DataProcessEngine(
                event_source=InMemoryProcessEventSource({SYMBOL: events}),
                footprint_source=InMemoryProcessFootprintSource({SYMBOL: footprints}),
                config=DataProcessConfig(
                    emit_individual_refill_orders=False,
                    emit_price_activity_levels=True,
                    collect_event_payloads=collect_event_payloads,
                    filter_price_activity_to_footprints=True,
                ),
            )
            result = engine.run_replay(
                ProcessReplayRequest(60_000, 119_999, (SYMBOL,))
            )
            return tuple(
                payload
                for payload in result.payloads
                if payload.get("output_type") == "DATA_PROCESS_PRICE_ACTIVITY_LEVEL"
            )

        self.assertEqual(
            replay(collect_event_payloads=False),
            replay(collect_event_payloads=True),
        )

    def test_scan_keeps_all_candle_snapshots_without_event_payloads(self):
        events = (
            event(1, "A", 2, order="A"),
            event(2, "F", 1, order="A"),
            event(3, "M", 4, order="A"),
            replace(event(4, "A", 2, order="B"), ts_event_ms=120_001),
            replace(event(5, "F", 1, order="B"), ts_event_ms=120_002),
            replace(event(6, "M", 4, order="B"), ts_event_ms=120_003),
            replace(event(7, "A", 5, order="OFF", price="200"), ts_event_ms=120_004),
        )
        footprints = (
            {"open_time_ms": 60_000, "close_time_ms": 119_999,
             "bins": ({"low": "99", "high": "101"},)},
            {"open_time_ms": 120_000, "close_time_ms": 179_999,
             "bins": ({"low": "99", "high": "101"},)},
        )
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({SYMBOL: events}),
            footprint_source=InMemoryProcessFootprintSource({SYMBOL: footprints}),
            config=DataProcessConfig(
                max_payloads=3,
                emit_individual_refill_orders=False,
                emit_price_activity_levels=True,
                collect_event_payloads=False,
                filter_price_activity_to_footprints=True,
            ),
        )

        result = engine.run_replay(ProcessReplayRequest(60_000, 179_999, (SYMBOL,)))

        self.assertEqual(len(result.payloads), 2)
        self.assertEqual(
            [payload["marker_time_ms"] for payload in result.payloads],
            [60_000, 120_000],
        )
        self.assertTrue(all(payload["price"] == "100" for payload in result.payloads))
        self.assertTrue(all(
            payload["output_type"] == "DATA_PROCESS_PRICE_ACTIVITY_LEVEL"
            for payload in result.payloads
        ))

    def test_spike_score_scan_filters_bins_and_combines_both_sides(self):
        footprint = ProcessFootprintSnapshot(
            symbol=SYMBOL,
            candles=(
                {
                    "open_time_ms": 60_000,
                    "bins": (
                        {"low": "100.00", "high": "101.00", "contract_spike_score": "12.500"},
                        {"low": "101.00", "high": "102.00", "contract_spike_score": "11.999"},
                    ),
                },
            ),
        )
        activity = (
            {
                "provider_symbol": "NQ.FUT", "timeframe": "M1",
                "marker_time_ms": 60_500, "marker_price": "100.25",
                "side": "ASK", "price_base_refill_count": 4,
                "executed_contracts": 9,
            },
            {
                "provider_symbol": "NQ.FUT", "timeframe": "M1",
                "marker_time_ms": 62_500, "marker_price": "100.50",
                "side": "ASK", "price_base_refill_count": 2,
                "executed_contracts": 4,
            },
            {
                "provider_symbol": "NQ.FUT", "timeframe": "M1",
                "marker_time_ms": 61_500, "marker_price": "100.75",
                "side": "BID", "price_base_refill_count": 3,
                "executed_contracts": 7,
            },
        )

        rows = _spike_score_scan_payloads(
            (footprint,), activity, score_min=Decimal("12.5"),
            start_ms=60_000, end_ms=120_000,
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["contract_spike_score"], "12.500")
        self.assertEqual(
            (
                rows[0]["ask_refill_count"], rows[0]["bid_refill_count"],
                rows[0]["ask_execution_count"], rows[0]["bid_execution_count"],
            ),
            (6, 3, 13, 7),
        )

    def test_increasing_modify_after_fill(self):
        row = metric([event(1, "A", 3), event(2, "F", 1), event(3, "M", 6)])
        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (1, 4))

    def test_decreasing_modify_after_fill_is_not_refill(self):
        row = metric([event(1, "A", 3), event(2, "F", 1), event(3, "M", 1)])
        self.assertEqual(row["refill_count"], 0)

    def test_fill_consumes_only_existing_liquidity(self):
        row = metric([event(1, "A", 3), event(2, "F", 1), event(3, "M", 5), event(4, "F", 2)])
        self.assertEqual(row["executed_refill_contracts"], 0)

    def test_fill_consumes_existing_then_refill(self):
        row = metric([event(1, "A", 4), event(2, "F", 1), event(3, "M", 8), event(4, "F", 4)])
        self.assertEqual(row["executed_refill_contracts"], 1)

    def test_fill_consumes_all_refill(self):
        row = metric([event(1, "A", 2), event(2, "F", 2), event(3, "M", 5), event(4, "F", 5)])
        self.assertEqual(row["executed_refill_contracts"], 5)

    def test_multiple_refills_before_fill(self):
        row = metric([event(1, "A", 2), event(2, "F", 1), event(3, "M", 4), event(4, "M", 7)])
        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (2, 6))

    def test_decreasing_modify_withdraws_open_refill(self):
        row = metric([event(1, "A", 2), event(2, "F", 2), event(3, "M", 5), event(4, "M", 2)])
        self.assertEqual((row["executed_refill_contracts"], row["withdrawn_refill_contracts"]), (0, 3))

    def test_cancel_does_not_execute_open_refill(self):
        row = metric([event(1, "A", 2), event(2, "F", 2), event(3, "M", 5), event(4, "D", 5)])
        self.assertEqual((row["executed_refill_contracts"], row["withdrawn_refill_contracts"]), (0, 5))

    def test_multiple_orders_aggregate_at_price(self):
        row = metric([
            event(1, "A", 1, order="A"), event(2, "F", 1, order="A"), event(3, "M", 2, order="A"),
            event(4, "A", 1, order="B"), event(5, "F", 1, order="B"), event(6, "M", 3, order="B"),
        ])
        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (3, 6))

    def test_buy_and_sell_at_same_price_are_separate(self):
        _, rows = run([
            event(1, "A", 1), event(2, "F", 1), event(3, "M", 3),
            event(4, "A", 1, order="S", side="ASK"), event(5, "F", 1, order="S", side="ASK"),
            event(6, "M", 4, order="S", side="ASK"),
        ])
        self.assertEqual({row["side"]: row["refill_added_contracts"] for row in rows}, {"BID": 3, "ASK": 4})

    def test_price_change_does_not_move_old_refill(self):
        _, rows = run([event(1, "A", 1), event(2, "F", 1), event(3, "M", 3), event(4, "M", 3, price="101")])
        self.assertEqual([(row["price"], row["refill_added_contracts"]) for row in rows], [("100", 3)])

    def test_duplicate_event_is_ignored(self):
        duplicate = event(3, "M", 3)
        row = metric([event(1, "A", 1), event(2, "F", 1), duplicate, duplicate])
        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (1, 3))

    def test_distinct_fill_sequences_in_same_millisecond_are_preserved(self):
        first_fill = replace(event(2, "F", 3), ts_event_ms=60_002)
        second_fill = replace(event(3, "F", 4), ts_event_ms=60_002)

        rows = run_activity([event(1, "A", 10), first_fill, second_fill])

        self.assertEqual(rows[0]["fill_event_count"], 2)
        self.assertEqual(rows[0]["executed_contracts"], 7)

    def test_full_execution_rate_is_100_percent(self):
        row = metric([event(1, "A", 1), event(2, "F", 1), event(3, "M", 4), event(4, "F", 4)])
        self.assertEqual(row["refill_execution_rate"], 100.0)

    def test_refill_without_execution_rate_is_zero(self):
        row = metric([event(1, "A", 1), event(2, "F", 1), event(3, "M", 4)])
        self.assertEqual(row["refill_execution_rate"], 0.0)

    def test_execution_never_exceeds_added_and_aggregate_display(self):
        events = []
        seq = 1
        for order_id, added, executed in (("A", 2, 2), ("B", 2, 2), ("C", 2, 1), ("D", 2, 1), ("E", 2, 1), ("F", 1, 0)):
            events.extend((event(seq, "A", 1, order=order_id), event(seq + 1, "F", 1, order=order_id),
                           event(seq + 2, "M", added, order=order_id)))
            if executed:
                events.append(event(seq + 3, "F", executed, order=order_id))
            seq += 10
        row = metric(events)
        self.assertLessEqual(row["executed_refill_contracts"], row["refill_added_contracts"])
        self.assertEqual((row["refill_count"], row["refill_added_contracts"], row["executed_refill_contracts"]), (11, 16, 12))
        self.assertAlmostEqual(row["refill_execution_rate"], 75.0, places=1)
        self.assertEqual(row["refill_display"], "11(16) E12 - 75%")


class PriceActivityLevelTests(unittest.TestCase):
    def test_order_book_state_continues_while_candle_statistics_reset(self):
        first = event(1, "A", 100, order="A")
        second = replace(event(2, "F", 30, order="A"), ts_event_ms=120_002)
        source = InMemoryProcessEventSource({SYMBOL: (first, second)})
        engine = DataProcessEngine(
            event_source=source,
            config=DataProcessConfig(emit_price_activity_levels=True),
        )
        result = engine.run_replay(ProcessReplayRequest(60_000, 179_999, (SYMBOL,)))
        rows = _aggregate_refill_scan_payloads(
            result.payloads, start_ms=60_000, end_ms=179_999,
        )

        self.assertEqual(len(rows), 2)
        first_candle, second_candle = rows
        self.assertEqual(
            (first_candle["opening_liquidity"], first_candle["gross_added_contracts"], first_candle["closing_liquidity"]),
            (0, 100, 100),
        )
        self.assertEqual(
            (
                second_candle["opening_liquidity"], second_candle["gross_added_contracts"],
                second_candle["executed_contracts"], second_candle["closing_liquidity"],
            ),
            (100, 0, 30, 70),
        )
        self.assertEqual(second_candle["available_liquidity"], 100)
        self.assertEqual(second_candle["level_execution_rate"], 30.0)
        self.assertTrue(second_candle["level_execution_invariant_ok"])

    def test_new_order_id_can_refill_same_price_level(self):
        row = run_activity([
            event(1, "A", 10, order="A"),
            event(2, "F", 4, order="A"),
            event(3, "A", 3, order="B"),
        ])[0]

        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (1, 3))
        self.assertEqual(row["gross_added_contracts"], 13)
        self.assertEqual(row["non_refill_added_contracts"], 10)
        self.assertEqual(
            row["gross_added_contracts"],
            row["non_refill_added_contracts"] + row["refill_added_contracts"],
        )
        self.assertEqual(row["closing_liquidity"], 9)
        self.assertTrue(row["level_execution_invariant_ok"])

    def test_available_liquidity_is_rate_denominator(self):
        first = event(1, "A", 100, order="A")
        fill = replace(event(2, "F", 30, order="A"), ts_event_ms=120_002)
        refill = replace(event(3, "A", 20, order="B"), ts_event_ms=120_003)
        source = InMemoryProcessEventSource({SYMBOL: (first, fill, refill)})
        engine = DataProcessEngine(
            event_source=source,
            config=DataProcessConfig(emit_price_activity_levels=True),
        )
        result = engine.run_replay(ProcessReplayRequest(60_000, 179_999, (SYMBOL,)))
        rows = _aggregate_refill_scan_payloads(
            result.payloads, start_ms=60_000, end_ms=179_999,
        )
        row = [item for item in rows if item["marker_time_ms"] == 120_000][0]

        self.assertEqual((row["opening_liquidity"], row["gross_added_contracts"]), (100, 20))
        self.assertEqual(row["available_liquidity"], 120)
        self.assertEqual(row["level_execution_rate"], 25.0)
        self.assertEqual((row["non_refill_added_contracts"], row["refill_added_contracts"]), (0, 20))
        self.assertEqual(row["closing_liquidity"], 90)
        self.assertTrue(row["level_execution_invariant_ok"])

    def test_activity_is_aggregated_per_candle_not_across_replay_range(self):
        first = event(1, "A", 2, order="first")
        second = replace(event(2, "A", 3, order="second"), ts_event_ms=120_002)
        source = InMemoryProcessEventSource({SYMBOL: (first, second)})
        engine = DataProcessEngine(
            event_source=source,
            config=DataProcessConfig(emit_price_activity_levels=True),
        )

        result = engine.run_replay(ProcessReplayRequest(60_000, 179_999, (SYMBOL,)))
        rows = _aggregate_refill_scan_payloads(
            result.payloads, start_ms=60_000, end_ms=179_999,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual([row["marker_time_ms"] for row in rows], [60_000, 120_000])
        self.assertEqual([row["order_count"] for row in rows], [1, 1])
        self.assertEqual([row["added_contracts"] for row in rows], [2, 3])

    def test_activity_filter_codes(self):
        ask = {"side": "ASK", "order_count": 12, "added_contracts": 250}
        bid = {"side": "BID", "order_count": 3, "added_contracts": 400}
        self.assertTrue(_matches_refill_activity_filter(ask, "O10"))
        self.assertFalse(_matches_refill_activity_filter(bid, "O10"))
        self.assertTrue(_matches_refill_activity_filter(ask, "A250"))
        self.assertFalse(_matches_refill_activity_filter(ask, "A251"))
        self.assertTrue(_matches_refill_activity_filter(bid, "B400"))
        self.assertFalse(_matches_refill_activity_filter(bid, "A1"))

    def test_displayed_rate_uses_refill_rate_for_refill_rows_on_both_sides(self):
        for side in ("ASK", "BID"):
            row = {
                "side": side,
                "has_refill": True,
                "refill_execution_rate": 63.6,
                "level_execution_rate": 90.0,
            }
            self.assertEqual(_displayed_execution_rate(row), 63.6)

    def test_displayed_rate_uses_level_rate_for_non_refill_rows(self):
        row = {
            "side": "BID",
            "has_refill": False,
            "refill_execution_rate": 0.0,
            "level_execution_rate": 52.4,
        }
        self.assertEqual(_displayed_execution_rate(row), 52.4)

    def test_large_add_and_fills_without_refill(self):
        rows = run_activity([
            event(1, "A", 132), event(2, "F", 1), event(3, "M", 131),
            event(4, "F", 10), event(5, "M", 121), event(6, "F", 1),
            event(7, "M", 120), event(8, "F", 120), event(9, "C", 120),
        ])
        row = rows[0]
        self.assertEqual((row["order_count"], row["added_contracts"]), (1, 132))
        self.assertEqual((row["fill_event_count"], row["executed_contracts"]), (4, 132))
        self.assertEqual(row["level_execution_rate"], 100.0)
        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (0, 0))
        self.assertEqual(row["executed_refill_contracts"], 0)
        self.assertEqual(row["refill_execution_rate"], 0.0)
        self.assertTrue(row["has_price_activity"])
        self.assertFalse(row["has_refill"])
        self.assertEqual(row["display_mode"], "level_execution")
        self.assertEqual(row["display_text"], "O1 A132 E132 - 100%")

    def test_remaining_size_modifies_do_not_increase_added_contracts(self):
        row = run_activity([event(1, "A", 10), event(2, "F", 3), event(3, "M", 7)])[0]
        self.assertEqual(row["added_contracts"], 10)

    def test_threshold_zero_includes_active_non_refill_level(self):
        row = run_activity([event(1, "A", 2)])[0]
        self.assertTrue(row["has_price_activity"] and row["refill_count"] >= 0)

    def test_threshold_one_excludes_same_non_refill_level(self):
        row = run_activity([event(1, "A", 2)])[0]
        self.assertFalse(row["refill_count"] >= 1)

    def test_analytical_payload_exists_with_zero_refill(self):
        row = run_activity([event(1, "A", 2), event(2, "F", 1)])[0]
        self.assertEqual(row["refill_count"], 0)
        self.assertEqual(row["executed_contracts"], 1)

    def test_multiple_orders_at_same_price(self):
        row = run_activity([event(1, "A", 2, order="A"), event(2, "A", 3, order="B")])[0]
        self.assertEqual((row["order_count"], row["added_contracts"]), (2, 5))

    def test_only_some_orders_refill(self):
        row = run_activity([
            event(1, "A", 2, order="A"), event(2, "F", 1, order="A"), event(3, "M", 3, order="A"),
            event(4, "A", 4, order="B"), event(5, "F", 1, order="B"), event(6, "M", 3, order="B"),
        ])[0]
        self.assertEqual(row["order_count"], 2)
        self.assertEqual((row["refill_count"], row["refill_added_contracts"]), (2, 6))

    def test_level_and_refill_execution_rates_are_independent(self):
        row = run_activity([
            event(1, "A", 4), event(2, "F", 2), event(3, "M", 4), event(4, "F", 3),
        ])[0]
        self.assertEqual(row["level_execution_rate"], 83.3)
        self.assertEqual(row["refill_execution_rate"], 50.0)

    def test_positive_modify_adds_only_delta(self):
        row = run_activity([event(1, "A", 100), event(2, "M", 115)])[0]
        self.assertEqual(row["added_contracts"], 115)

    def test_bid_and_ask_activity_are_separate(self):
        rows = run_activity([event(1, "A", 2), event(2, "A", 3, order="S", side="ASK")])
        self.assertEqual({row["side"]: row["added_contracts"] for row in rows}, {"BID": 2, "ASK": 3})

    def test_cancel_and_decrease_are_not_execution(self):
        row = run_activity([event(1, "A", 10), event(2, "M", 7), event(3, "C", 2)])[0]
        self.assertEqual(row["executed_contracts"], 0)
        self.assertEqual(row["cancelled_or_withdrawn_contracts"], 5)

    def test_price_change_does_not_transfer_activity(self):
        rows = run_activity([event(1, "A", 10), event(2, "M", 6, price="101")])
        by_price = {row["price"]: row for row in rows}
        self.assertEqual(by_price["100"]["added_contracts"], 10)
        self.assertEqual(by_price["100"]["cancelled_or_withdrawn_contracts"], 10)
        self.assertEqual(by_price["101"]["added_contracts"], 6)


if __name__ == "__main__":
    unittest.main()
