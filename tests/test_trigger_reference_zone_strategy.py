"""Legacy reference-zone tests kept as inert text after the July 2026 trigger rewrite.

from __future__ import annotations

import unittest
from decimal import Decimal

from triggerEngine import ABSORPTION_FOUND, IDLE, TRADING, ZONE_TOUCHED, TriggerConfig, TriggerEngine


M1_MS = 60_000
M5_MS = 300_000
OPEN_TIME_MS = 1_800_000


def footprint_bin(
    *,
    low: str,
    high: str,
    score: str = "14",
    buy_contracts: str = "0",
    sell_contracts: str = "0",
    buy_volume: str = "0",
    sell_volume: str = "0",
) -> dict:
    return {
        "low": low,
        "high": high,
        "l2": {
            "contract_spike_score": score,
            "buy_contracts": buy_contracts,
            "sell_contracts": sell_contracts,
            "buy_volume": buy_volume,
            "sell_volume": sell_volume,
        },
    }


def candle(
    offset: int,
    *,
    timeframe: str = "M5",
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    market_provider: str = "CME_LOCAL_DBN",
    open_price: str = "100",
    high_price: str = "103",
    low_price: str = "95",
    close_price: str = "101",
    bins: list[dict] | None = None,
    deviation: str = "3",
    price_step: str = "0.25",
    closed: bool = True,
) -> dict:
    interval_ms = M1_MS if timeframe.upper() == "M1" else M5_MS
    open_time_ms = OPEN_TIME_MS + offset * interval_ms
    return {
        "mt5_symbol": symbol,
        "provider_symbol": provider_symbol,
        "market_provider": market_provider,
        "quantity_unit": "VOLUME" if market_provider == "BINANCE" else "CONTRACTS",
        "timeframe": timeframe,
        "open_time_ms": open_time_ms,
        "close_time_ms": open_time_ms + interval_ms - 1,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "price_step": price_step,
        "contract_spike_score_deviation": deviation,
        "closed": closed,
        "bins": bins or [],
    }


def dom_snapshot(
    *,
    timeframe: str = "M5",
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    refill_count: int = 40,
) -> dict:
    return {
        "type": "DOM_TIMELINE_SESSION",
        "mt5_symbol": symbol,
        "provider_symbol": provider_symbol,
        "timeframe": timeframe,
        "timestamp_ms": OPEN_TIME_MS,
        "order_book_levels": [
            {
                "price": "96",
                "bid_contracts": 1,
                "top_order_id": "BID-96",
                "top_order_side": "BID",
                "top_order_positive_refill_count": refill_count,
                "price_base_refill_count": refill_count,
                "price_base_refill_contracts": refill_count,
            },
            {
                "price": "102",
                "bid_contracts": 1,
                "top_order_id": "BID-102",
                "top_order_side": "BID",
                "top_order_positive_refill_count": refill_count,
                "price_base_refill_count": refill_count,
                "price_base_refill_contracts": refill_count,
            },
            {
                "price": "103",
                "ask_contracts": 1,
                "top_order_id": "ASK-103",
                "top_order_side": "ASK",
                "top_order_positive_refill_count": refill_count,
                "price_base_refill_count": refill_count,
                "price_base_refill_contracts": refill_count,
            },
            {
                "price": "150",
                "ask_contracts": 1,
                "top_order_id": "ASK-150",
                "top_order_side": "ASK",
                "top_order_positive_refill_count": refill_count,
                "price_base_refill_count": refill_count,
                "price_base_refill_contracts": refill_count,
            },
        ],
    }


def buy_reference(offset: int = 0, *, timeframe: str = "M5", symbol: str = "NQ") -> dict:
    return candle(
        offset,
        timeframe=timeframe,
        symbol=symbol,
        open_price="100",
        high_price="103",
        low_price="95",
        close_price="102",
        bins=[footprint_bin(low="96", high="96.25", sell_contracts="11")],
    )


def sell_reference(offset: int = 0, *, timeframe: str = "M5", symbol: str = "NQ") -> dict:
    return candle(
        offset,
        timeframe=timeframe,
        symbol=symbol,
        open_price="102",
        high_price="105",
        low_price="97",
        close_price="100",
        bins=[footprint_bin(low="103", high="103.25", buy_contracts="11")],
    )


class TriggerReferenceZoneStrategyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = TriggerEngine()
        self.engine.set_dom_output_snapshot(dom_snapshot())
        self.engine.set_dom_output_snapshot(dom_snapshot(timeframe="M1"))
        self.engine.set_dom_output_snapshot(
            dom_snapshot(symbol="BTCUSD", provider_symbol="BTCUSDT")
        )

    def state(self, symbol: str = "NQ", timeframe: str = "M5"):
        return self.engine.state_for(symbol=symbol, timeframe=timeframe)

    def open_buy(self) -> dict:
        self.engine.process_closed_candle(buy_reference())
        for offset in range(1, 5):
            self.engine.process_closed_candle(
                candle(offset, open_price="98", high_price="100", low_price="98", close_price="99")
            )
        self.engine.process_closed_candle(
            candle(5, open_price="98", high_price="100", low_price="97", close_price="99")
        )
        return self.engine.process_closed_candle(
            candle(6, open_price="99", high_price="102", low_price="98", close_price="101"),
            next_candle=candle(7, open_price="100", high_price="102", low_price="99", close_price="101"),
        )[0].to_payload()

    def open_sell(self) -> dict:
        self.engine.process_closed_candle(sell_reference())
        for offset in range(1, 5):
            self.engine.process_closed_candle(
                candle(offset, open_price="102", high_price="102", low_price="99", close_price="100")
            )
        self.engine.process_closed_candle(
            candle(5, open_price="102", high_price="102.25", low_price="99", close_price="100")
        )
        return self.engine.process_closed_candle(
            candle(6, open_price="101", high_price="101", low_price="98", close_price="99"),
            next_candle=candle(7, open_price="100", high_price="101", low_price="98", close_price="99"),
        )[0].to_payload()

    def test_buy_reference_zone_entry_and_stop(self) -> None:
        self.assertEqual(self.engine.process_closed_candle(buy_reference()), tuple())
        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(state.reference_zone_low, Decimal("96"))
        self.assertEqual(state.reference_zone_high, Decimal("97.00"))
        self.assertEqual(state.stop_loss, Decimal("95"))

        self.assertEqual(
            self.engine.process_closed_candle(candle(1, low_price="97.25")),
            tuple(),
        )
        self.assertEqual(
            self.engine.process_closed_candle(candle(2, low_price="97")),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        for offset in range(3, 5):
            self.assertEqual(
                self.engine.process_closed_candle(candle(offset, low_price="98")),
                tuple(),
            )
        self.assertEqual(
            self.engine.process_closed_candle(candle(5, low_price="97")),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)
        signal = self.engine.process_closed_candle(
            candle(6, open_price="98", high_price="103", low_price="98", close_price="101"),
            next_candle=candle(7, open_price="100"),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "100")
        self.assertEqual(signal["stop_loss"], "95")
        self.assertNotIn("take_profit", signal)
        self.assertEqual(signal["confirmation_state"], ZONE_TOUCHED)
        self.assertEqual(signal["marker_color"], "GREEN")
        self.assertEqual(signal["marker_direction"], "UP")
        self.assertEqual(signal["matched_bin_count"], 1)
        self.assertEqual(self.state().state, TRADING)

    def test_sell_reference_zone_entry_and_stop(self) -> None:
        self.assertEqual(self.engine.process_closed_candle(sell_reference()), tuple())
        state = self.state()
        self.assertEqual(state.direction, "SHORT")
        self.assertEqual(state.reference_zone_low, Decimal("102.25"))
        self.assertEqual(state.reference_zone_high, Decimal("103.25"))
        self.assertEqual(state.stop_loss, Decimal("105"))

        self.assertEqual(
            self.engine.process_closed_candle(candle(1, high_price="102.25")),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        for offset in range(2, 5):
            self.assertEqual(
                self.engine.process_closed_candle(candle(offset, high_price="102")),
                tuple(),
            )
        self.assertEqual(
            self.engine.process_closed_candle(candle(5, high_price="102.25")),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)
        signal = self.engine.process_closed_candle(
            candle(6, open_price="102", high_price="102", low_price="99", close_price="100"),
            next_candle=candle(7, open_price="100"),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["entry_price"], "100")
        self.assertEqual(signal["stop_loss"], "105")
        self.assertNotIn("take_profit", signal)
        self.assertEqual(signal["marker_color"], "RED")
        self.assertEqual(signal["marker_direction"], "DOWN")

    def test_buy_retest_rejects_close_below_reference_zone_low(self) -> None:
        self.engine.process_closed_candle(buy_reference())
        for offset in range(1, 5):
            self.engine.process_closed_candle(candle(offset, low_price="98"))

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(5, open_price="97", high_price="98", low_price="95", close_price="95.75")
            ),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.engine.process_closed_candle(
            candle(6, open_price="96", high_price="99", low_price="97", close_price="98")
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

    def test_sell_retest_rejects_close_above_reference_zone_high(self) -> None:
        self.engine.process_closed_candle(sell_reference())
        for offset in range(1, 5):
            self.engine.process_closed_candle(candle(offset, high_price="102"))

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(5, open_price="103", high_price="104", low_price="102", close_price="103.50")
            ),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.engine.process_closed_candle(
            candle(6, open_price="102", high_price="102.25", low_price="100", close_price="101")
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

    def test_reference_requires_candle_direction_score_and_one_sided_flow(self) -> None:
        invalid = (
            buy_reference() | {"close_price": "99"},
            buy_reference() | {"bins": [footprint_bin(low="96", high="96.25", score="13.999", sell_contracts="10")]},
            buy_reference() | {"bins": [footprint_bin(low="96", high="96.25", buy_contracts="1", sell_contracts="10")]},
        )
        for index, item in enumerate(invalid):
            with self.subTest(index=index):
                engine = TriggerEngine()
                self.assertEqual(engine.process_closed_candle(item), tuple())
                self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

    def test_reference_bin_can_be_anywhere_inside_candle(self) -> None:
        self.engine.process_closed_candle(
            buy_reference() | {"bins": [footprint_bin(low="102", high="102.25", sell_contracts="10")]}
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

    def test_thresholds_are_inclusive(self) -> None:
        self.engine.process_closed_candle(buy_reference())
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

    def test_reference_does_not_require_spike_score_deviation(self) -> None:
        self.engine.process_closed_candle(
            buy_reference() | {"contract_spike_score_deviation": "0"}
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

    def test_reference_requires_dom_refill_order(self) -> None:
        engine = TriggerEngine()
        self.assertEqual(engine.process_closed_candle(buy_reference()), tuple())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

        engine.set_dom_output_snapshot(dom_snapshot(refill_count=9))
        self.assertEqual(engine.process_closed_candle(buy_reference()), tuple())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

    def test_reference_accepts_dom_refill_events_inside_bin(self) -> None:
        engine = TriggerEngine()
        engine.set_dom_output_snapshot(
            {
                "type": "DOM_TIMELINE_SESSION",
                "mt5_symbol": "NQ",
                "provider_symbol": "NQ.FUT",
                "timeframe": "M5",
                "events": [
                    {
                        "timestamp_ms": OPEN_TIME_MS,
                        "price": "96",
                        "side": "BID",
                        "order_id": "BID-REFILL",
                        "positive_refill_count": 40,
                    }
                ],
            }
        )

        self.assertEqual(engine.process_closed_candle(buy_reference()), tuple())
        self.assertEqual(
            engine.state_for(symbol="NQ", timeframe="M5").state,
            ABSORPTION_FOUND,
        )

    def test_reference_accepts_dom_refill_engine_outputs_inside_bin(self) -> None:
        engine = TriggerEngine()
        engine.set_dom_output_snapshot(
            {
                "type": "DOM_TIMELINE_SESSION",
                "mt5_symbol": "NQ",
                "provider_symbol": "NQ.FUT",
                "timeframe": "M5",
                "engine_outputs": {
                    "dom_positive_refills": [
                        {
                            "id": "DOM|DOM_POSITIVE_REFILL|NQ.FUT|M5|1800000|96|BID|BID-OUTPUT",
                            "producer": "dom",
                            "type": "DOM_POSITIVE_REFILL",
                            "timestamp_ms": OPEN_TIME_MS,
                            "event_time_ms": OPEN_TIME_MS,
                            "provider_symbol": "NQ.FUT",
                            "mt5_symbol": "NQ",
                            "timeframe": "M5",
                            "price": "96",
                            "side": "BID",
                            "order_id": "BID-OUTPUT",
                            "positive_refill_count": 10,
                            "positive_refill_total": 10,
                        }
                    ],
                },
            }
        )

        self.assertEqual(engine.process_closed_candle(buy_reference()), tuple())
        self.assertEqual(
            engine.state_for(symbol="NQ", timeframe="M5").state,
            ABSORPTION_FOUND,
        )

    def test_reference_counts_dom_refill_by_order_id(self) -> None:
        engine = TriggerEngine()
        engine.set_dom_output_snapshot(
            {
                "type": "DOM_TIMELINE_SESSION",
                "mt5_symbol": "NQ",
                "provider_symbol": "NQ.FUT",
                "timeframe": "M5",
                "events": [
                    {
                        "timestamp_ms": OPEN_TIME_MS,
                        "price": "96",
                        "side": "BID",
                        "order_id": "BID-SPLIT-REFILL",
                    }
                ],
                "raw_events": [
                    {
                        "timestamp_ms": OPEN_TIME_MS + 10,
                        "price": "96",
                        "side": "BID",
                        "order_id": "BID-SPLIT-REFILL",
                        "positive_refill_count": 20,
                    },
                    {
                        "timestamp_ms": OPEN_TIME_MS + 20,
                        "price": "97",
                        "side": "BID",
                        "order_id": "BID-SPLIT-REFILL",
                        "positive_refill_count": 20,
                    },
                ],
            }
        )

        self.assertEqual(engine.process_closed_candle(buy_reference()), tuple())
        self.assertEqual(
            engine.state_for(symbol="NQ", timeframe="M5").state,
            ABSORPTION_FOUND,
        )

    def test_buy_confirmation_can_arrive_after_non_confirming_retest_candle(self) -> None:
        self.engine.process_closed_candle(buy_reference())
        for offset in range(1, 5):
            self.assertEqual(
                self.engine.process_closed_candle(candle(offset, low_price="98")),
                tuple(),
            )
        self.assertEqual(
            self.engine.process_closed_candle(candle(5, low_price="97")),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(6, open_price="100", high_price="101", low_price="95", close_price="96")
            ),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        signals = self.engine.process_closed_candle(
            candle(7, open_price="96", high_price="100", low_price="96", close_price="98"),
            next_candle=candle(8, open_price="99"),
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "BUY_ENTRY")

    def test_sell_confirmation_can_arrive_after_non_confirming_retest_candle(self) -> None:
        self.engine.process_closed_candle(sell_reference())
        for offset in range(1, 5):
            self.assertEqual(
                self.engine.process_closed_candle(candle(offset, high_price="102")),
                tuple(),
            )
        self.assertEqual(
            self.engine.process_closed_candle(candle(5, high_price="102.25")),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(6, open_price="100", high_price="104", low_price="99", close_price="103")
            ),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        signals = self.engine.process_closed_candle(
            candle(7, open_price="103", high_price="104", low_price="100", close_price="101"),
            next_candle=candle(8, open_price="100"),
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "SELL_ENTRY")

    def test_sell_retest_requires_swing_low_after_prior_peak_above_reference(self) -> None:
        self.engine.process_closed_candle(sell_reference())
        setup_candles = (
            candle(1, high_price="101", low_price="99"),
            candle(2, high_price="102", low_price="99"),
            candle(3, high_price="106", low_price="98"),
            candle(4, high_price="102", low_price="99"),
            candle(5, high_price="102", low_price="99"),
        )
        for item in setup_candles:
            self.assertEqual(self.engine.process_closed_candle(item), tuple())
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.assertEqual(
            self.engine.process_closed_candle(candle(6, high_price="102.25", low_price="95")),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.assertEqual(
            self.engine.process_closed_candle(candle(7, high_price="102", low_price="99")),
            tuple(),
        )
        self.assertEqual(
            self.engine.process_closed_candle(candle(8, high_price="102", low_price="99")),
            tuple(),
        )
        self.assertEqual(
            self.engine.process_closed_candle(candle(9, high_price="102.25", low_price="99")),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        signals = self.engine.process_closed_candle(
            candle(10, open_price="102", high_price="102", low_price="99", close_price="100"),
            next_candle=candle(11, open_price="100"),
        )
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "SELL_ENTRY")

    def test_buy_retest_requires_swing_high_after_prior_low_below_reference(self) -> None:
        self.engine.process_closed_candle(buy_reference())
        setup_candles = (
            candle(1, high_price="100", low_price="98"),
            candle(2, high_price="101", low_price="98"),
            candle(3, high_price="100", low_price="94"),
            candle(4, high_price="101", low_price="98"),
            candle(5, high_price="100", low_price="98"),
        )
        for item in setup_candles:
            self.assertEqual(self.engine.process_closed_candle(item), tuple())
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.assertEqual(
            self.engine.process_closed_candle(candle(6, high_price="105", low_price="97")),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.assertEqual(
            self.engine.process_closed_candle(candle(7, high_price="100", low_price="98")),
            tuple(),
        )
        self.assertEqual(
            self.engine.process_closed_candle(candle(8, high_price="101", low_price="98")),
            tuple(),
        )
        self.assertEqual(
            self.engine.process_closed_candle(candle(9, high_price="100", low_price="97")),
            tuple(),
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        signals = self.engine.process_closed_candle(
            candle(10, open_price="98", high_price="103", low_price="98", close_price="101"),
            next_candle=candle(11, open_price="100"),
        )
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "BUY_ENTRY")

    def test_binance_uses_volume_zero_rule_and_its_own_tick_size(self) -> None:
        item = candle(
            0,
            symbol="BTCUSD",
            provider_symbol="BTCUSDT",
            market_provider="BINANCE",
            open_price="100",
            high_price="103",
            low_price="95",
            close_price="102",
            price_step="0.01",
            bins=[footprint_bin(low="96", high="96.50", sell_volume="2.5")],
        )
        self.engine.process_closed_candle(item)
        state = self.state(symbol="BTCUSD")
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(state.reference_zone_high, Decimal("96.53"))

    def test_reference_expires_after_one_hundred_candles(self) -> None:
        self.engine.process_closed_candle(buy_reference())
        original_time = self.state().reference_candle_time_ms
        for offset in range(1, 101):
            self.engine.process_closed_candle(candle(offset, low_price="97.25", bins=[]))
        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        self.assertEqual(self.state().reference_candle_time_ms, original_time)

        self.assertEqual(
            self.engine.process_closed_candle(candle(101, low_price="97.25", bins=[])),
            tuple(),
        )
        self.assertEqual(self.state().state, IDLE)

    def test_touched_reference_expires_before_late_entry_confirmation(self) -> None:
        self.engine.process_closed_candle(buy_reference())
        for offset in range(1, 100):
            self.engine.process_closed_candle(candle(offset, low_price="98", bins=[]))
        self.engine.process_closed_candle(
            candle(100, open_price="98", high_price="100", low_price="97", close_price="98")
        )
        self.assertEqual(self.state().state, ZONE_TOUCHED)

        signals = self.engine.process_closed_candle(
            candle(101, open_price="98", high_price="103", low_price="98", close_price="101"),
            next_candle=candle(102, open_price="100"),
        )
        self.assertEqual(signals, tuple())
        self.assertEqual(self.state().state, IDLE)

    def test_stale_setup_does_not_block_later_reference_entry(self) -> None:
        stale_sell = candle(
            0,
            open_price="160",
            high_price="170",
            low_price="130",
            close_price="140",
            bins=[footprint_bin(low="150", high="150.25", buy_contracts="20")],
        )
        self.engine.process_closed_candle(stale_sell)
        self.engine.process_closed_candle(buy_reference(1))
        for offset in range(2, 6):
            self.engine.process_closed_candle(
                candle(offset, open_price="98", high_price="100", low_price="98", close_price="99")
            )
        self.engine.process_closed_candle(
            candle(6, open_price="98", high_price="100", low_price="97", close_price="99")
        )
        signals = self.engine.process_closed_candle(
            candle(7, open_price="99", high_price="103", low_price="99", close_price="102"),
            next_candle=candle(8, open_price="101"),
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].signal_type, "BUY_ENTRY")

    def test_multi_symbol_and_timeframe_state_is_independent(self) -> None:
        self.engine.process_closed_candle(buy_reference(timeframe="M1"))
        self.engine.process_closed_candle(
            sell_reference(symbol="ES") | {"provider_symbol": "ES.FUT"}
        )
        self.assertEqual(self.state(timeframe="M1").direction, "LONG")
        self.assertEqual(self.state(symbol="ES").direction, "SHORT")
        self.assertEqual(self.state().state, IDLE)

    def test_configured_timeframes_limit_trigger_support(self) -> None:
        engine = TriggerEngine(TriggerConfig(supported_timeframes=("M15",)))
        engine.process_closed_candle(buy_reference())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

    def test_buy_exits_on_contract_spike_score_above_fourteen(self) -> None:
        self.open_buy()
        self.assertEqual(
            self.engine.process_closed_candle(
                candle(
                    8,
                    open_price="100",
                    high_price="199.75",
                    low_price="99",
                    close_price="104",
                    bins=[footprint_bin(low="104", high="104.25", score="14", buy_contracts="1")],
                )
            ),
            tuple(),
        )
        signal = self.engine.process_closed_candle(
            candle(
                9,
                open_price="100",
                high_price="200",
                low_price="99",
                close_price="104",
                bins=[footprint_bin(low="104", high="104.25", score="14.001", buy_contracts="1")],
            ),
            next_candle=candle(10, open_price="105"),
        )[0].to_payload()
        self.assertEqual(signal["exit_price"], "105")
        self.assertEqual(signal["reason"], "BUY_EXIT_CONTRACT_SPIKE_SCORE")
        self.assertEqual(signal["marker_shape"], "SQUARE")
        self.assertEqual(signal["marker_color"], "RED")
        self.assertEqual(self.state().state, IDLE)

    def test_buy_stop_has_priority_when_stop_and_spike_share_a_candle(self) -> None:
        self.open_buy()
        signal = self.engine.process_closed_candle(
            candle(
                8,
                open_price="100",
                high_price="201",
                low_price="94",
                close_price="101",
                bins=[footprint_bin(low="101", high="101.25", score="14.001", buy_contracts="1")],
            )
        )[0].to_payload()
        self.assertEqual(signal["exit_price"], "95")
        self.assertEqual(signal["reason"], "BUY_EXIT_STOP_LOSS")
        self.assertEqual(signal["marker_shape"], "SQUARE")
        self.assertEqual(signal["marker_color"], "RED")

    def test_sell_exits_on_contract_spike_score_above_fourteen_and_stop(self) -> None:
        self.open_sell()
        self.assertEqual(
            self.engine.process_closed_candle(
                candle(
                    8,
                    open_price="100",
                    high_price="101",
                    low_price="0.25",
                    close_price="96",
                    bins=[footprint_bin(low="96", high="96.25", score="14", sell_contracts="1")],
                )
            ),
            tuple(),
        )
        spike_exit = self.engine.process_closed_candle(
            candle(
                9,
                open_price="100",
                high_price="101",
                low_price="0",
                close_price="96",
                bins=[footprint_bin(low="96", high="96.25", score="14.001", sell_contracts="1")],
            ),
            next_candle=candle(10, open_price="95"),
        )[0].to_payload()
        self.assertEqual(spike_exit["exit_price"], "95")
        self.assertEqual(spike_exit["reason"], "SELL_EXIT_CONTRACT_SPIKE_SCORE")
        self.assertEqual(spike_exit["marker_shape"], "SQUARE")
        self.assertEqual(spike_exit["marker_color"], "GREEN")

        engine = TriggerEngine()
        engine.set_dom_output_snapshot(dom_snapshot())
        engine.process_closed_candle(sell_reference())
        for offset in range(1, 5):
            engine.process_closed_candle(candle(offset, high_price="102"))
        engine.process_closed_candle(candle(5, high_price="102.25"))
        engine.process_closed_candle(
            candle(6, open_price="102", high_price="102", low_price="99", close_price="100"),
            next_candle=candle(7, open_price="100"),
        )
        stopped = engine.process_closed_candle(
            candle(8, open_price="100", high_price="105", low_price="98", close_price="104")
        )[0].to_payload()
        self.assertEqual(stopped["exit_price"], "105")
        self.assertEqual(stopped["reason"], "SELL_EXIT_STOP_LOSS")
        self.assertEqual(stopped["marker_shape"], "SQUARE")
        self.assertEqual(stopped["marker_color"], "GREEN")

    def test_open_buy_checks_current_candle_for_spike_exit(self) -> None:
        self.open_buy()
        signal = self.engine.process_closed_candle(
            candle(8, open_price="100", high_price="101", low_price="99", close_price="100"),
            current_candle=candle(
                9,
                open_price="100",
                high_price="111",
                low_price="99",
                close_price="110",
                closed=False,
                bins=[footprint_bin(low="110", high="110.25", score="14.001", buy_contracts="1")],
            ),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "EXIT_BUY")
        self.assertEqual(signal["exit_price"], "110")
        self.assertEqual(signal["reason"], "BUY_EXIT_CONTRACT_SPIKE_SCORE")

    def test_open_sell_checks_current_candle_for_spike_exit(self) -> None:
        self.open_sell()
        signal = self.engine.process_closed_candle(
            candle(8, open_price="100", high_price="101", low_price="99", close_price="100"),
            current_candle=candle(
                9,
                open_price="100",
                high_price="101",
                low_price="89",
                close_price="90",
                closed=False,
                bins=[footprint_bin(low="90", high="90.25", score="14.001", sell_contracts="1")],
            ),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "EXIT_SELL")
        self.assertEqual(signal["exit_price"], "90")
        self.assertEqual(signal["reason"], "SELL_EXIT_CONTRACT_SPIKE_SCORE")

    def test_open_position_checks_only_exit(self) -> None:
        self.open_buy()
        self.assertEqual(
            self.engine.process_closed_candle(sell_reference(2) | {"high_price": "104", "low_price": "96"}),
            tuple(),
        )
        self.assertEqual(self.state().state, TRADING)

    def test_evaluate_latest_only_processes_last_closed_candle(self) -> None:
        items = [buy_reference(0), sell_reference(1), buy_reference(2) | {"closed": False}]
        self.engine.evaluate_latest(items, evaluation_time_ms=items[1]["close_time_ms"] + 1)
        self.assertEqual(self.state().direction, "SHORT")

    def test_enrich_candles_backtests_without_persisting_state(self) -> None:
        items = [
            buy_reference(0),
            candle(1, low_price="98"),
            candle(2, low_price="98"),
            candle(3, low_price="98"),
            candle(4, low_price="98"),
            candle(5, low_price="97"),
            candle(6, open_price="98", high_price="104", low_price="98", close_price="103"),
            candle(7, open_price="100", high_price="104", low_price="99", close_price="103"),
        ]
        signals = self.engine.enrich_candles(items, evaluation_time_ms=items[-1]["close_time_ms"] + 1)
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signal_type"], "BUY_ENTRY")
        self.assertEqual(self.state().state, IDLE)

    def test_runtime_logging_can_be_disabled_or_enabled(self) -> None:
        with self.assertNoLogs("triggerEngine", level="INFO"):
            self.engine.process_closed_candle(buy_reference())
        engine = TriggerEngine(TriggerConfig(runtime_logging_enabled=True))
        engine.set_dom_output_snapshot(dom_snapshot())
        with self.assertLogs("triggerEngine", level="INFO") as records:
            engine.process_closed_candle(buy_reference())
        self.assertIn("TRIGGER_STATE_CHANGE", "\n".join(records.output))


if __name__ == "__main__":
    unittest.main()
"""

from __future__ import annotations

import unittest
from decimal import Decimal

from triggerEngine import ABSORPTION_FOUND, IDLE, TRADING, TriggerEngine


M5_MS = 300_000
OPEN_TIME_MS = 1_800_000


def footprint_bin(
    *,
    low: str,
    high: str,
    score: str = "14",
    market_buy: str = "0",
    market_sell: str = "0",
) -> dict:
    return {
        "low": low,
        "high": high,
        "l2": {
            "contract_spike_score": score,
            "buy_contracts": market_buy,
            "sell_contracts": market_sell,
        },
    }


def candle(
    offset: int,
    *,
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    timeframe: str = "M5",
    open_price: str = "106",
    high_price: str = "112",
    low_price: str = "100",
    close_price: str = "106",
    bins: list[dict] | None = None,
    closed: bool = True,
) -> dict:
    open_time_ms = OPEN_TIME_MS + offset * M5_MS
    return {
        "mt5_symbol": symbol,
        "provider_symbol": provider_symbol,
        "market_provider": "CME_LOCAL_DBN",
        "quantity_unit": "CONTRACTS",
        "timeframe": timeframe,
        "open_time_ms": open_time_ms,
        "close_time_ms": open_time_ms + M5_MS - 1,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "price_step": "0.25",
        "closed": closed,
        "is_live": not closed,
        "bins": bins or [],
    }


def sell_reference(offset: int = 0, *, symbol: str = "NQ") -> dict:
    return candle(
        offset,
        symbol=symbol,
        open_price="108",
        high_price="112",
        low_price="100",
        close_price="106",
        bins=[
            footprint_bin(
                low="109",
                high="109.25",
                market_buy="9",
                market_sell="0",
            )
        ],
    )


def buy_reference(offset: int = 0, *, symbol: str = "NQ") -> dict:
    return candle(
        offset,
        symbol=symbol,
        open_price="104",
        high_price="112",
        low_price="100",
        close_price="106",
        bins=[
            footprint_bin(
                low="102",
                high="102.25",
                market_buy="0",
                market_sell="9",
            )
        ],
    )


def idle_candle(offset: int, *, symbol: str = "NQ") -> dict:
    return candle(offset, symbol=symbol, open_price="106", high_price="108", low_price="104", close_price="106")


def buy_retest(offset: int = 5, *, symbol: str = "NQ") -> dict:
    return candle(
        offset,
        symbol=symbol,
        open_price="102.50",
        high_price="105",
        low_price="102.75",
        close_price="104",
    )


def sell_retest(offset: int = 5, *, symbol: str = "NQ") -> dict:
    return candle(
        offset,
        symbol=symbol,
        open_price="109",
        high_price="110",
        low_price="106",
        close_price="108.75",
    )


def refill_payload(
    offset: int,
    *,
    price: str,
    side: str,
    count: int = 5,
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    timeframe: str = "M5",
    market_buy: int | None = None,
    market_sell: int | None = None,
    footprint_bin_low: str | None = None,
    footprint_bin_high: str | None = None,
    action: str = "ENTRY",
    zone_low: str | None = None,
    zone_high: str | None = None,
    entry_direction: str | None = None,
    reference_side: str | None = None,
    zone_level_count: int = 1,
) -> dict:
    timestamp_ms = OPEN_TIME_MS + offset * M5_MS + 1_000
    normalized_side = str(side or "").strip().upper()
    if market_buy is None:
        market_buy = 9 if normalized_side == "ASK" else 0
    if market_sell is None:
        market_sell = 9 if normalized_side == "BID" else 0
    low = footprint_bin_low or price
    high = footprint_bin_high or str(Decimal(str(low)) + Decimal("0.25"))
    payload_id = f"PROCESS-PAYLOAD-{symbol}-{timeframe}-{side}-{price}-{offset}-{count}"
    payload = {
        "type": "ABSORPTION",
        "payload_type": "ABSORPTION",
        "output_type": "DATA_PROCESS_REFILL_ORDER_CLOSED",
        "payload_id": payload_id,
        "id": payload_id,
        "output_id": payload_id,
        "action": action,
        "source_engine": "dataProcessEngine",
        "timestamp_ms": timestamp_ms,
        "threshold_time_ms": timestamp_ms,
        "close_time_ms": timestamp_ms + 500,
        "mt5_symbol": symbol,
        "provider_symbol": provider_symbol,
        "symbol": provider_symbol,
        "timeframe": timeframe,
        "price": price,
        "side": side,
        "order_id": f"{symbol}-{side}-{price}-{offset}-{count}",
        "refill_count": count,
        "refill_contracts": count,
        "price_base_refill_count": count,
        "price_base_refill_contracts": count,
        "refill_method": "price_base_refill",
        "market_buy": int(market_buy),
        "market_sell": int(market_sell),
        "market_buy_contracts": int(market_buy),
        "market_sell_contracts": int(market_sell),
        "ask_traded_contracts": int(market_buy),
        "bid_traded_contracts": int(market_sell),
        "footprint_open_time_ms": OPEN_TIME_MS + offset * M5_MS,
        "footprint_bin_low": low,
        "footprint_bin_high": high,
    }
    if zone_low is not None and zone_high is not None:
        payload.update(
            {
                "zone_low": zone_low,
                "zone_high": zone_high,
                "reference_zone_low": zone_low,
                "reference_zone_high": zone_high,
                "entry_direction": entry_direction
                or ("LONG" if normalized_side == "BID" else "SHORT"),
                "reference_side": reference_side
                or ("SELL" if normalized_side == "BID" else "BUY"),
                "zone_level_count": int(zone_level_count),
            }
        )
    return payload


def ingest_refill(
    engine: TriggerEngine,
    offset: int,
    *,
    price: str,
    side: str,
    count: int = 5,
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    market_buy: int | None = None,
    market_sell: int | None = None,
    footprint_bin_low: str | None = None,
    footprint_bin_high: str | None = None,
    action: str = "ENTRY",
    zone_low: str | None = None,
    zone_high: str | None = None,
    entry_direction: str | None = None,
    reference_side: str | None = None,
    zone_level_count: int = 1,
) -> None:
    engine.set_dom_output_snapshot(
        refill_payload(
            offset,
            price=price,
            side=side,
            count=count,
            symbol=symbol,
            provider_symbol=provider_symbol,
            market_buy=market_buy,
            market_sell=market_sell,
            footprint_bin_low=footprint_bin_low,
            footprint_bin_high=footprint_bin_high,
            action=action,
            zone_low=zone_low,
            zone_high=zone_high,
            entry_direction=entry_direction,
            reference_side=reference_side,
            zone_level_count=zone_level_count,
        )
    )


def ingest_entry_zone(
    engine: TriggerEngine,
    offset: int,
    *,
    price: str,
    side: str,
    zone_low: str,
    zone_high: str,
    count: int = 5,
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    market_buy: int | None = None,
    market_sell: int | None = None,
    action: str = "ENTRY",
    zone_level_count: int = 1,
) -> None:
    normalized_side = str(side or "").strip().upper()
    ingest_refill(
        engine,
        offset,
        price=price,
        side=side,
        count=count,
        symbol=symbol,
        provider_symbol=provider_symbol,
        market_buy=market_buy,
        market_sell=market_sell,
        action=action,
        zone_low=zone_low,
        zone_high=zone_high,
        entry_direction="LONG" if normalized_side == "BID" else "SHORT",
        reference_side="SELL" if normalized_side == "BID" else "BUY",
        zone_level_count=zone_level_count,
    )


class TriggerAbsorptionRefillStrategyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = TriggerEngine()

    def state(self, symbol: str = "NQ", timeframe: str = "M5"):
        return self.engine.state_for(symbol=symbol, timeframe=timeframe)

    def open_buy(self) -> dict:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
        )
        self.engine.process_closed_candle(buy_reference())
        for offset in range(1, 5):
            self.engine.process_closed_candle(idle_candle(offset))
        signal = self.engine.process_closed_candle(
            buy_retest(5),
            next_candle=candle(6, open_price="104.25"),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        return signal

    def open_sell(self) -> dict:
        ingest_entry_zone(
            self.engine,
            0,
            price="109",
            side="ASK",
            zone_low="106",
            zone_high="109",
            count=41,
            market_buy=80,
            market_sell=0,
        )
        self.engine.process_closed_candle(sell_reference())
        for offset in range(1, 5):
            self.engine.process_closed_candle(idle_candle(offset))
        signal = self.engine.process_closed_candle(
            sell_retest(5),
            next_candle=candle(6, open_price="108.50"),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        return signal

    def test_sell_reference_and_bearish_retest_enter_short(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="109",
            side="ASK",
            zone_low="106",
            zone_high="109",
            count=41,
            market_buy=80,
            market_sell=0,
        )

        self.assertEqual(self.engine.process_closed_candle(sell_reference()), tuple())
        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "SHORT")
        self.assertEqual(str(state.reference_zone_low), "106")
        self.assertEqual(str(state.reference_zone_high), "109")

        for offset in range(1, 5):
            self.assertEqual(self.engine.process_closed_candle(idle_candle(offset)), tuple())

        signal = self.engine.process_closed_candle(
            sell_retest(5),
            next_candle=candle(6, open_price="108.50"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["direction"], "SHORT")
        self.assertEqual(signal["entry_price"], "108.50")
        self.assertEqual(signal["trigger_category"], "ABSORPTION")
        self.assertEqual(signal["reference_bin_side"], "BUY")
        self.assertNotIn("stop_loss", signal)
        self.assertEqual(self.state().state, TRADING)

    def test_buy_reference_and_bullish_retest_enter_long(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
        )

        self.assertEqual(self.engine.process_closed_candle(buy_reference()), tuple())
        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(str(state.reference_zone_low), "102")
        self.assertEqual(str(state.reference_zone_high), "105")

        for offset in range(1, 5):
            self.assertEqual(self.engine.process_closed_candle(idle_candle(offset)), tuple())

        signal = self.engine.process_closed_candle(
            buy_retest(5),
            next_candle=candle(6, open_price="104.25"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["direction"], "LONG")
        self.assertEqual(signal["entry_price"], "104.25")
        self.assertEqual(signal["reference_bin_side"], "SELL")
        self.assertEqual(
            signal["process_payload_id"],
            "PROCESS-PAYLOAD-NQ-M5-BID-102-0-41",
        )
        self.assertEqual(
            signal["reference_payload_id"],
            "PROCESS-PAYLOAD-NQ-M5-BID-102-0-41",
        )
        self.assertEqual(
            signal["reference_bin"]["payload_id"],
            "PROCESS-PAYLOAD-NQ-M5-BID-102-0-41",
        )
        self.assertNotIn("stop_loss", signal)
        self.assertEqual(self.state().state, TRADING)

    def test_entry_reference_ignores_non_entry_action_payload(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
            action="EXIT",
        )

        self.engine.process_closed_candle(buy_reference())

        self.assertEqual(self.state().state, IDLE)

    def test_buy_reference_uses_prebuilt_process_entry_zone(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="28875.5",
            side="BID",
            zone_low="28875.5",
            zone_high="28878.5",
            count=20,
            market_buy=17,
            market_sell=87,
            zone_level_count=4,
        )

        self.engine.process_closed_candle(
            candle(
                0,
                open_price="28879",
                high_price="28884",
                low_price="28875",
                close_price="28882",
            )
        )

        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(str(state.reference_zone_low), "28875.5")
        self.assertEqual(str(state.reference_zone_high), "28878.5")
        self.assertEqual(state.reference_bin["refill_count"], 20)
        self.assertEqual(state.reference_bin["market_sell"], 87)
        self.assertEqual(state.reference_bin["zone_level_count"], 4)
        self.assertEqual(len(state.reference_bins), 1)

    def test_sell_reference_uses_prebuilt_process_entry_zone(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="111.5",
            side="ASK",
            zone_low="109.0",
            zone_high="111.5",
            count=20,
            market_buy=87,
            market_sell=17,
            zone_level_count=4,
        )

        self.engine.process_closed_candle(
            candle(
                0,
                open_price="108",
                high_price="112",
                low_price="106",
                close_price="107",
            )
        )

        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "SHORT")
        self.assertEqual(str(state.reference_zone_low), "109.0")
        self.assertEqual(str(state.reference_zone_high), "111.5")
        self.assertEqual(state.reference_bin["refill_count"], 20)
        self.assertEqual(state.reference_bin["market_buy"], 87)
        self.assertEqual(state.reference_bin["zone_level_count"], 4)
        self.assertEqual(len(state.reference_bins), 1)

    def test_entry_trigger_removes_reference_refill_from_memory(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
        )

        self.engine.process_closed_candle(buy_reference())
        active_before_entry = self.engine._active_refill_records(buy_reference())
        self.assertTrue(
            any(
                record.price == Decimal("102") and record.side == "BID"
                for record in active_before_entry
            )
        )

        for offset in range(1, 5):
            self.engine.process_closed_candle(idle_candle(offset))
        signal = self.engine.process_closed_candle(
            buy_retest(5),
            next_candle=candle(6, open_price="104.25"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertTrue(signal["reference_bin"]["refill_record_id"])
        active_after_entry = self.engine._active_refill_records(idle_candle(6))
        self.assertFalse(
            any(
                record.price == Decimal("102") and record.side == "BID"
                for record in active_after_entry
            )
        )

    def test_refill_count_no_longer_gates_process_zone(self) -> None:
        payload = refill_payload(0, price="102", side="BID", count=3) | {
            "refill_contracts": 20,
            "positive_refill_total": 20,
            "refill_total": 20,
            "zone_low": "102",
            "zone_high": "105",
            "reference_zone_low": "102",
            "reference_zone_high": "105",
            "entry_direction": "LONG",
            "reference_side": "SELL",
        }
        self.assertIsNone(self.engine.set_dom_output_snapshot(payload))

        self.engine.process_closed_candle(buy_reference())

        self.assertEqual(self.state().state, ABSORPTION_FOUND)

    def test_reference_uses_process_zone_without_footprint_third_filter(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="101",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
        )

        self.engine.process_closed_candle(
            buy_reference()
            | {"bins": [footprint_bin(low="110", high="110.25", market_sell="9")]}
        )

        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(str(state.reference_zone_low), "101")
        self.assertEqual(str(state.reference_zone_high), "105")

    def test_payload_without_process_zone_does_not_create_reference(self) -> None:
        engine = TriggerEngine()
        self.assertIsNone(
            engine.set_dom_output_snapshot(refill_payload(0, price="102", side="BID", count=5))
        )
        engine.process_closed_candle(buy_reference())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)
        self.assertEqual(len(engine._active_refill_records(buy_reference())), 1)

        engine = TriggerEngine()
        ingest_refill(engine, 0, price="102", side="BID", count=5)
        engine.process_closed_candle(buy_reference())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

    def test_other_active_refills_do_not_block_process_entry_zone(self) -> None:
        engine = TriggerEngine()
        ingest_refill(engine, -1, price="101", side="BID", count=20)
        ingest_entry_zone(
            engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=20,
            market_buy=0,
            market_sell=80,
        )

        engine.process_closed_candle(buy_reference())

        state = engine.state_for(symbol="NQ", timeframe="M5")
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "LONG")

    def test_entry_zone_requires_matching_reference_candle(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
        )
        reference_candle = buy_reference(20)
        self.engine.process_closed_candle(reference_candle)
        self.assertEqual(self.state().state, IDLE)
        self.assertEqual(len(self.engine._active_refill_records(reference_candle)), 0)

    def test_multi_symbol_state_and_refill_memory_are_independent(self) -> None:
        ingest_entry_zone(
            self.engine,
            0,
            price="102",
            side="BID",
            zone_low="102",
            zone_high="105",
            count=41,
            market_buy=0,
            market_sell=80,
        )
        ingest_entry_zone(
            self.engine,
            0,
            price="109",
            side="ASK",
            zone_low="106",
            zone_high="109",
            count=41,
            market_buy=80,
            market_sell=0,
            symbol="ES",
            provider_symbol="ES.FUT",
        )

        self.engine.process_closed_candle(buy_reference(symbol="NQ"))
        self.engine.process_closed_candle(
            sell_reference(symbol="ES") | {"provider_symbol": "ES.FUT"}
        )

        self.assertEqual(self.state(symbol="NQ").direction, "LONG")
        self.assertEqual(self.state(symbol="ES").direction, "SHORT")
        self.assertEqual(self.state(symbol="NQ", timeframe="M1").state, IDLE)

    def test_sell_exits_on_live_bid_refill_in_lower_wick_or_body_third(self) -> None:
        self.open_sell()
        ingest_refill(self.engine, 7, price="101", side="BID", count=20)
        live = candle(
            7,
            open_price="104",
            high_price="112",
            low_price="100",
            close_price="110",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_SELL")
        self.assertEqual(signal["reason"], "SELL_EXIT_LIVE_BID_REFILL")
        self.assertEqual(signal["exit_price"], "110")
        self.assertEqual(self.state().state, IDLE)

    def test_buy_exits_on_live_ask_refill_in_upper_wick_or_body_third(self) -> None:
        self.open_buy()
        ingest_refill(self.engine, 7, price="111", side="ASK", count=20)
        live = candle(
            7,
            open_price="104",
            high_price="112",
            low_price="100",
            close_price="110",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_BUY")
        self.assertEqual(signal["reason"], "BUY_EXIT_LIVE_ASK_REFILL_FOUND")
        self.assertEqual(signal["exit_price"], "110")

    def test_buy_exits_when_live_price_touches_ask_refill_level(self) -> None:
        self.open_buy()
        ingest_refill(self.engine, 7, price="106", side="ASK", count=5)
        live = candle(
            7,
            open_price="107",
            high_price="112",
            low_price="100",
            close_price="106",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_BUY")
        self.assertEqual(signal["reason"], "BUY_EXIT_OPPOSITE_ASK_STRONG_REFILL_LEVEL")
        self.assertNotIn("refill_kind", signal["reference_bin"])

    def test_sell_exits_when_live_price_touches_bid_refill_level(self) -> None:
        self.open_sell()
        ingest_refill(self.engine, 7, price="108", side="BID", count=5)
        live = candle(
            7,
            open_price="104",
            high_price="112",
            low_price="100",
            close_price="110",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_SELL")
        self.assertEqual(signal["reason"], "SELL_EXIT_OPPOSITE_BID_STRONG_REFILL_LEVEL")
        self.assertNotIn("refill_kind", signal["reference_bin"])

    def test_buy_lower_ask_refill_exits_as_touched_opposite_level(self) -> None:
        self.open_buy()
        ingest_refill(self.engine, 7, price="101", side="ASK", count=20)
        live = candle(
            7,
            open_price="104",
            high_price="112",
            low_price="100",
            close_price="110",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_BUY")
        self.assertEqual(
            signal["reason"],
            "BUY_EXIT_OPPOSITE_ASK_STRONG_REFILL_LEVEL",
        )
        self.assertEqual(signal["reference_bin"]["side"], "ASK")

    def test_sell_upper_bid_refill_exits_as_touched_opposite_level(self) -> None:
        self.open_sell()
        ingest_refill(self.engine, 7, price="111", side="BID", count=20)
        live = candle(
            7,
            open_price="108",
            high_price="112",
            low_price="100",
            close_price="104",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_SELL")
        self.assertEqual(
            signal["reason"],
            "SELL_EXIT_OPPOSITE_BID_STRONG_REFILL_LEVEL",
        )
        self.assertEqual(signal["reference_bin"]["side"], "BID")

    def test_opposite_level_exit_does_not_filter_by_refill_kind(self) -> None:
        self.open_buy()
        ingest_refill(
            self.engine,
            7,
            price="106",
            side="ASK",
            count=20,
            market_buy=1,
            market_sell=1,
        )
        live = candle(
            7,
            open_price="107",
            high_price="112",
            low_price="100",
            close_price="106",
            closed=False,
        )

        signal = self.engine.process_closed_candle(
            idle_candle(6),
            current_candle=live,
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_BUY")
        self.assertEqual(signal["reason"], "BUY_EXIT_OPPOSITE_ASK_STRONG_REFILL_LEVEL")
        self.assertNotIn("refill_kind", signal["reference_bin"])

    def test_closed_candle_spike_no_longer_exits_position(self) -> None:
        self.open_buy()

        signals = self.engine.process_closed_candle(
            candle(
                7,
                open_price="111",
                high_price="120",
                low_price="109",
                close_price="119",
                bins=[footprint_bin(low="118", high="118.25", score="99", market_buy="10")],
            )
        )

        self.assertEqual(signals, tuple())
        self.assertEqual(self.state().state, TRADING)


if __name__ == "__main__":
    unittest.main()
