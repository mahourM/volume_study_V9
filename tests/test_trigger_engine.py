from __future__ import annotations

import unittest
from decimal import Decimal

from absorption.html_server import _candles_html_page, _html_page
from absorption.session_service import _enrich_replay_candles_chronologically
from process.sinks import TriggerEngineSink
from triggerEngine import (
    ABSORPTION_FOUND,
    IDLE,
    PEAK_CONFIRMED,
    TRADING,
    TriggerConfig,
    TriggerEngine,
    ZONE_TOUCHED,
    _reference_payload_id,
    order_book_snapshot_from_payload,
    order_book_snapshots_from_payload,
)


M1_MS = 60_000
M5_MS = 300_000
OPEN_TIME_MS = 1_800_000


def trigger_bin(
    *,
    low: str,
    high: str,
    dominance: str,
    spike_score: str = "6",
    index: int,
    abnormal: bool = True,
    buy_diagonal_ratio: str = "0",
    sell_diagonal_ratio: str = "0",
) -> dict:
    return {
        "index": index,
        "low": low,
        "high": high,
        "abnormal_contract": abnormal,
        "abnormal_volume": abnormal,
        "l2": {
            "dominant_diagonal_side": dominance,
            "contract_spike_score": spike_score,
            "buy_contracts": "10" if dominance == "BUY" else "1",
            "sell_contracts": "10" if dominance == "SELL" else "1",
            "buy_diagonal_contract_ratio": buy_diagonal_ratio,
            "sell_diagonal_contract_ratio": sell_diagonal_ratio,
        },
    }


def candle(
    offset: int,
    *,
    timeframe: str = "M5",
    symbol: str = "NQ",
    provider_symbol: str = "NQ.FUT",
    open_price: str = "100",
    high_price: str = "103",
    low_price: str = "95",
    close_price: str = "101",
    bins: list[dict] | None = None,
) -> dict:
    interval_ms = M1_MS if timeframe.upper() == "M1" else M5_MS
    open_time_ms = OPEN_TIME_MS + offset * interval_ms
    return {
        "mt5_symbol": symbol,
        "provider_symbol": provider_symbol,
        "timeframe": timeframe,
        "open_time_ms": open_time_ms,
        "close_time_ms": open_time_ms + interval_ms - 1,
        "open_price": open_price,
        "high_price": high_price,
        "low_price": low_price,
        "close_price": close_price,
        "bins": bins or [],
    }


def buy_absorption_candle(offset: int = 0, *, timeframe: str = "M5") -> dict:
    return candle(
        offset,
        timeframe=timeframe,
        open_price="100",
        high_price="103",
        low_price="95",
        close_price="102",
        bins=[
            trigger_bin(
                low="96",
                high="97",
                dominance="SELL",
                spike_score="13",
                index=96,
            )
        ],
    )


def sell_absorption_candle(offset: int = 0, *, timeframe: str = "M5") -> dict:
    return candle(
        offset,
        timeframe=timeframe,
        open_price="100",
        high_price="105",
        low_price="97",
        close_price="98",
        bins=[
            trigger_bin(
                low="103",
                high="104",
                dominance="BUY",
                spike_score="13",
                index=103,
            )
        ],
    )


def buy_signal_bin(
    *,
    low: str = "96",
    high: str = "97",
    ratio: str = "4.1",
    index: int = 96,
) -> dict:
    return trigger_bin(
        low=low,
        high=high,
        dominance="BUY",
        spike_score="0",
        index=index,
        abnormal=False,
        buy_diagonal_ratio=ratio,
    )


def sell_signal_bin(
    *,
    low: str = "103",
    high: str = "104",
    ratio: str = "4.1",
    index: int = 103,
) -> dict:
    return trigger_bin(
        low=low,
        high=high,
        dominance="SELL",
        spike_score="0",
        index=index,
        abnormal=False,
        sell_diagonal_ratio=ratio,
    )


@unittest.skip("legacy Peak/Signal Bin strategy replaced by reference-zone strategy")
class TriggerEngineTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = TriggerEngine()

    def state(self, symbol: str = "NQ", timeframe: str = "M5"):
        return self.engine.state_for(symbol=symbol, timeframe=timeframe)

    def test_buy_entry_state_flow_creates_position_with_stop_loss(self) -> None:
        self.assertEqual(
            self.engine.process_closed_candle(buy_absorption_candle()),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
            ),
            tuple(),
        )
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        signal = self.engine.process_closed_candle(
            candle(
                2,
                open_price="95.5",
                high_price="99",
                low_price="94",
                close_price="96",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(3, open_price="98.5", high_price="100", low_price="97", close_price="99"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["direction"], "LONG")
        self.assertEqual(signal["entry_price"], "98.5")
        self.assertEqual(signal["stop_loss"], "94")
        self.assertEqual(signal["reference_bin_low"], "96")
        self.assertEqual(signal["reference_bin_high"], "97")
        self.assertEqual(signal["reference_bin_side"], "SELL")
        self.assertEqual(signal["timeframe"], "M5")
        self.assertEqual(signal["marker_shape"], "ARROW")
        self.assertEqual(signal["marker_direction"], "UP")
        self.assertEqual(signal["reference_candle_time_ms"], OPEN_TIME_MS)
        self.assertEqual(signal["confirmation_candle_time_ms"], OPEN_TIME_MS + 2 * M5_MS)
        self.assertEqual(signal["confirmation_state"], PEAK_CONFIRMED)
        self.assertEqual(signal["matched_bin_count"], 2)
        self.assertTrue(signal["position_id"].startswith("POS-"))
        self.assertEqual(self.state().state, TRADING)
        self.assertIsNotNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

    def test_runtime_trigger_logging_is_disabled_by_default(self) -> None:
        engine = TriggerEngine()

        with self.assertNoLogs("triggerEngine", level="INFO"):
            engine.process_closed_candle(buy_absorption_candle())
            engine.process_closed_candle(
                candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
            )
            engine.process_closed_candle(
                candle(
                    2,
                    open_price="95.5",
                    high_price="99",
                    low_price="94",
                    close_price="96",
                    bins=[buy_signal_bin()],
                ),
                next_candle=candle(3, open_price="98.5", high_price="100", low_price="97", close_price="99"),
            )

    def test_runtime_trigger_logging_can_be_enabled(self) -> None:
        engine = TriggerEngine(TriggerConfig(runtime_logging_enabled=True))

        with self.assertLogs("triggerEngine", level="INFO") as records:
            engine.process_closed_candle(buy_absorption_candle())
            engine.process_closed_candle(
                candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
            )
            engine.process_closed_candle(
                candle(
                    2,
                    open_price="95.5",
                    high_price="99",
                    low_price="94",
                    close_price="96",
                    bins=[buy_signal_bin()],
                ),
                next_candle=candle(3, open_price="98.5", high_price="100", low_price="97", close_price="99"),
            )

        output = "\n".join(records.output)
        self.assertIn("TRIGGER_STATE_CHANGE", output)
        self.assertIn("TRIGGER_SIGNAL", output)

    def test_buy_exit_has_priority_over_new_entry(self) -> None:
        self.test_buy_entry_state_flow_creates_position_with_stop_loss()
        exit_candle = candle(
            3,
            open_price="98.5",
            high_price="104",
            low_price="98",
            close_price="103",
            bins=[
                trigger_bin(
                    low="103",
                    high="104",
                    dominance="BUY",
                    spike_score="8",
                    index=103,
                ),
                trigger_bin(
                    low="98",
                    high="99",
                    dominance="SELL",
                    spike_score="20",
                    index=98,
                ),
            ],
        )

        signal = self.engine.process_closed_candle(
            exit_candle,
            next_candle=candle(4, open_price="102.5", high_price="103", low_price="101", close_price="102"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "EXIT_BUY")
        self.assertEqual(signal["exit_price"], "102.5")
        self.assertEqual(signal["reason"], "BUY_EXIT_UPPER_WICK_BUY_ABNORMAL_VOLUME")
        self.assertEqual(signal["marker_shape"], "SQUARE")
        self.assertEqual(signal["marker_direction"], "NONE")
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))
        self.assertEqual(self.state().state, IDLE)

    def test_buy_entry_requires_signal_bin_after_peak(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        blocked = self.engine.process_closed_candle(
            candle(2, open_price="99", high_price="100", low_price="94", close_price="98")
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="97",
                high_price="100",
                low_price="94",
                close_price="98",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(4, open_price="98.75", high_price="100", low_price="98", close_price="99"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "98.75")
        self.assertEqual(signal["stop_loss"], "94")
        self.assertEqual(self.state().state, TRADING)

    def test_sell_entry_and_exit_flow(self) -> None:
        self.assertEqual(
            self.engine.process_closed_candle(sell_absorption_candle()),
            tuple(),
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
            ),
            tuple(),
        )
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        entry_signal = self.engine.process_closed_candle(
            candle(
                2,
                open_price="104.5",
                high_price="107",
                low_price="101",
                close_price="103.5",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(3, open_price="102", high_price="103", low_price="99", close_price="100"),
        )[0].to_payload()

        self.assertEqual(entry_signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(entry_signal["direction"], "SHORT")
        self.assertEqual(entry_signal["entry_price"], "102")
        self.assertEqual(entry_signal["stop_loss"], "107")
        self.assertEqual(entry_signal["marker_shape"], "ARROW")
        self.assertEqual(entry_signal["marker_direction"], "DOWN")
        self.assertEqual(entry_signal["reference_candle_time_ms"], OPEN_TIME_MS)
        self.assertEqual(entry_signal["confirmation_candle_time_ms"], OPEN_TIME_MS + 2 * M5_MS)
        self.assertEqual(entry_signal["confirmation_state"], PEAK_CONFIRMED)
        self.assertEqual(entry_signal["break_confirmed_candle_time_ms"], 0)
        self.assertEqual(entry_signal["matched_bin_count"], 2)
        self.assertEqual(self.state().state, TRADING)

        exit_signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="102",
                high_price="103",
                low_price="96",
                close_price="97",
                bins=[
                    trigger_bin(
                        low="96",
                        high="97",
                        dominance="SELL",
                        spike_score="9",
                        index=96,
                    )
                ],
            ),
            next_candle=candle(4, open_price="98", high_price="99", low_price="97", close_price="98.5"),
        )[0].to_payload()

        self.assertEqual(exit_signal["signal_type"], "EXIT_SELL")
        self.assertEqual(exit_signal["exit_price"], "98")
        self.assertEqual(exit_signal["reason"], "SELL_EXIT_LOWER_WICK_SELL_ABNORMAL_VOLUME")
        self.assertEqual(exit_signal["marker_shape"], "SQUARE")
        self.assertEqual(exit_signal["marker_direction"], "NONE")
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))
        self.assertEqual(self.state().state, IDLE)

    def test_sell_entry_requires_signal_bin_after_break(self) -> None:
        self.engine.process_closed_candle(sell_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
        )

        blocked = self.engine.process_closed_candle(
            candle(2, open_price="101", high_price="107", low_price="100", close_price="102")
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="103",
                high_price="107",
                low_price="100",
                close_price="102",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(4, open_price="102.25", high_price="103", low_price="100", close_price="101"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["entry_price"], "102.25")
        self.assertEqual(signal["stop_loss"], "107")
        self.assertEqual(self.state().state, TRADING)

    def test_opposite_reference_after_buy_peak_is_ignored_until_timeout(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        blocked = self.engine.process_closed_candle(sell_absorption_candle(2))

        state = self.state()
        self.assertEqual(blocked, tuple())
        self.assertEqual(state.state, PEAK_CONFIRMED)
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(state.reference_candle_time_ms, OPEN_TIME_MS)

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="94.8",
                high_price="98",
                low_price="94",
                close_price="95.2",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(4, open_price="95.75", high_price="98", low_price="95", close_price="97"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "95.75")

    def test_opposite_reference_after_sell_break_is_ignored_until_timeout(self) -> None:
        self.engine.process_closed_candle(sell_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
        )
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        blocked = self.engine.process_closed_candle(buy_absorption_candle(2))

        state = self.state()
        self.assertEqual(blocked, tuple())
        self.assertEqual(state.state, PEAK_CONFIRMED)
        self.assertEqual(state.direction, "SHORT")
        self.assertEqual(state.reference_candle_time_ms, OPEN_TIME_MS)

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="105",
                high_price="106",
                low_price="102",
                close_price="104.5",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(4, open_price="104.75", high_price="105", low_price="101", close_price="102"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["entry_price"], "104.75")

    def test_signal_bin_before_peak_is_ignored(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())

        blocked = self.engine.process_closed_candle(
            candle(
                1,
                open_price="98",
                high_price="100",
                low_price="97",
                close_price="98",
                bins=[buy_signal_bin()],
            )
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

    def test_buy_signal_bin_can_be_on_peak_candle(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())

        signal = self.engine.process_closed_candle(
            candle(
                1,
                open_price="95",
                high_price="99",
                low_price="94.5",
                close_price="98",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(2, open_price="96.25", high_price="99", low_price="95", close_price="98"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "96.25")
        self.assertEqual(signal["stop_loss"], "94.5")
        self.assertEqual(signal["confirmation_state"], PEAK_CONFIRMED)

    def test_sell_signal_bin_can_be_on_break_candle(self) -> None:
        self.engine.process_closed_candle(sell_absorption_candle())

        signal = self.engine.process_closed_candle(
            candle(
                1,
                open_price="104.8",
                high_price="106",
                low_price="102",
                close_price="103.5",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(2, open_price="104.25", high_price="105", low_price="101", close_price="102"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["entry_price"], "104.25")
        self.assertEqual(signal["stop_loss"], "106")
        self.assertEqual(signal["confirmation_state"], PEAK_CONFIRMED)

    def test_buy_signal_bin_requires_bullish_candle(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        blocked = self.engine.process_closed_candle(
            candle(
                2,
                open_price="95.2",
                high_price="98",
                low_price="94",
                close_price="94.8",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(3, open_price="95.75", high_price="98", low_price="95", close_price="97"),
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="94.8",
                high_price="98",
                low_price="94",
                close_price="95.2",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(4, open_price="95.75", high_price="98", low_price="95", close_price="97"),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "95.75")
        self.assertEqual(signal["stop_loss"], "94")

    def test_sell_signal_bin_requires_bearish_candle(self) -> None:
        self.engine.process_closed_candle(sell_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
        )

        blocked = self.engine.process_closed_candle(
            candle(
                2,
                open_price="104.5",
                high_price="106",
                low_price="102",
                close_price="105",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(3, open_price="104.75", high_price="105", low_price="101", close_price="102"),
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="105",
                high_price="106",
                low_price="102",
                close_price="104.5",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(4, open_price="104.75", high_price="105", low_price="101", close_price="102"),
        )[0].to_payload()
        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["entry_price"], "104.75")
        self.assertEqual(signal["stop_loss"], "106")

    def test_signal_bin_does_not_require_matching_cme_candle_delta(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )
        buy_candle = candle(
            2,
            open_price="95.5",
            high_price="99",
            low_price="94",
            close_price="96",
            bins=[buy_signal_bin()],
        )
        buy_candle["buy_contracts"] = "9"
        buy_candle["sell_contracts"] = "10"

        buy_signal = self.engine.process_closed_candle(buy_candle)[0].to_payload()
        self.assertEqual(buy_signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(self.state().state, TRADING)

        engine = TriggerEngine()
        engine.process_closed_candle(sell_absorption_candle())
        engine.process_closed_candle(
            candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
        )
        sell_candle = candle(
            2,
            open_price="105",
            high_price="106",
            low_price="102",
            close_price="104.5",
            bins=[sell_signal_bin()],
        )
        sell_candle["buy_contracts"] = "10"
        sell_candle["sell_contracts"] = "9"

        sell_signal = engine.process_closed_candle(sell_candle)[0].to_payload()
        self.assertEqual(sell_signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(
            engine.state_for(symbol="NQ", timeframe="M5").state,
            TRADING,
        )

    def test_signal_bin_does_not_require_matching_binance_candle_delta(self) -> None:
        def volume_bin(item: dict, *, buy: str, sell: str) -> dict:
            item["l2"].pop("buy_contracts", None)
            item["l2"].pop("sell_contracts", None)
            item["l2"]["buy_volume"] = buy
            item["l2"]["sell_volume"] = sell
            return item

        engine = TriggerEngine()
        engine.process_closed_candle(
            candle(
                0,
                symbol="BTCUSD",
                provider_symbol="BTCUSDT",
                open_price="100",
                high_price="103",
                low_price="95",
                close_price="102",
                bins=[
                    volume_bin(
                        trigger_bin(
                            low="96",
                            high="97",
                            dominance="SELL",
                            spike_score="13",
                            index=96,
                        ),
                        buy="1",
                        sell="10",
                    )
                ],
            )
        )
        engine.process_closed_candle(
            candle(
                1,
                symbol="BTCUSD",
                provider_symbol="BTCUSDT",
                open_price="98",
                high_price="99",
                low_price="94.5",
                close_price="95.5",
            )
        )
        buy_signal = engine.process_closed_candle(
            candle(
                2,
                symbol="BTCUSD",
                provider_symbol="BTCUSDT",
                open_price="95.5",
                high_price="99",
                low_price="94",
                close_price="96",
                bins=[
                    volume_bin(buy_signal_bin(), buy="10", sell="1"),
                    volume_bin(
                        trigger_bin(
                            low="94",
                            high="95",
                            dominance="SELL",
                            spike_score="0",
                            index=94,
                            abnormal=False,
                        ),
                        buy="1",
                        sell="20",
                    ),
                ],
            )
        )

        self.assertEqual(buy_signal[0].signal_type, "BUY_ENTRY")
        self.assertEqual(
            engine.state_for(symbol="BTCUSD", timeframe="M5").state,
            TRADING,
        )

        sell_engine = TriggerEngine()
        sell_engine.process_closed_candle(
            candle(
                0,
                symbol="BTCUSD",
                provider_symbol="BTCUSDT",
                open_price="100",
                high_price="105",
                low_price="97",
                close_price="98",
                bins=[
                    volume_bin(
                        trigger_bin(
                            low="103",
                            high="104",
                            dominance="BUY",
                            spike_score="13",
                            index=103,
                        ),
                        buy="10",
                        sell="1",
                    )
                ],
            )
        )
        sell_engine.process_closed_candle(
            candle(
                1,
                symbol="BTCUSD",
                provider_symbol="BTCUSDT",
                open_price="103",
                high_price="106",
                low_price="102",
                close_price="104.5",
            )
        )
        sell_signal = sell_engine.process_closed_candle(
            candle(
                2,
                symbol="BTCUSD",
                provider_symbol="BTCUSDT",
                open_price="105",
                high_price="106",
                low_price="102",
                close_price="104.5",
                bins=[
                    volume_bin(sell_signal_bin(), buy="1", sell="10"),
                    volume_bin(
                        trigger_bin(
                            low="105",
                            high="106",
                            dominance="BUY",
                            spike_score="0",
                            index=105,
                            abnormal=False,
                        ),
                        buy="20",
                        sell="1",
                    ),
                ],
            )
        )

        self.assertEqual(sell_signal[0].signal_type, "SELL_ENTRY")
        self.assertEqual(
            sell_engine.state_for(symbol="BTCUSD", timeframe="M5").state,
            TRADING,
        )

    def test_signal_bin_ratio_must_be_at_least_threshold(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        blocked = self.engine.process_closed_candle(
            candle(
                2,
                open_price="95.5",
                high_price="99",
                low_price="94",
                close_price="96",
                bins=[buy_signal_bin(ratio="3.499")],
            )
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

    def test_signal_bin_ratio_equal_threshold_is_valid(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        signal = self.engine.process_closed_candle(
            candle(
                2,
                open_price="94.8",
                high_price="99",
                low_price="94",
                close_price="95.5",
                bins=[buy_signal_bin(ratio="3.5")],
            ),
            next_candle=candle(3, open_price="95.75", high_price="98", low_price="95", close_price="97"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "95.75")

    def test_reference_bin_can_be_anywhere_in_candle(self) -> None:
        self.engine.process_closed_candle(
            candle(
                0,
                open_price="100",
                high_price="105",
                low_price="95",
                close_price="102",
                bins=[
                    trigger_bin(
                        low="104",
                        high="105",
                        dominance="SELL",
                        spike_score="13",
                        index=104,
                    )
                ],
            )
        )

        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        self.assertEqual(self.state().direction, "LONG")
        self.assertEqual(self.state().reference_bin["low"], "104")

        engine = TriggerEngine()
        engine.process_closed_candle(
            candle(
                0,
                open_price="104",
                high_price="105",
                low_price="95",
                close_price="96",
                bins=[
                    trigger_bin(
                        low="95",
                        high="96",
                        dominance="BUY",
                        spike_score="13",
                        index=95,
                    )
                ],
            )
        )

        state = engine.state_for(symbol="NQ", timeframe="M5")
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "SHORT")
        self.assertEqual(state.reference_bin["low"], "95")

    def test_reference_bin_outside_candle_is_ignored(self) -> None:
        self.engine.process_closed_candle(
            candle(
                0,
                open_price="100",
                high_price="105",
                low_price="95",
                close_price="104",
                bins=[
                    trigger_bin(
                        low="105",
                        high="106",
                        dominance="SELL",
                        spike_score="13",
                        index=105,
                    )
                ],
            )
        )

        self.assertEqual(self.state().state, IDLE)

        engine = TriggerEngine()
        engine.process_closed_candle(
            candle(
                0,
                open_price="100",
                high_price="105",
                low_price="95",
                close_price="96",
                bins=[
                    trigger_bin(
                        low="94",
                        high="95",
                        dominance="BUY",
                        spike_score="13",
                        index=94,
                    )
                ],
            )
        )

        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

    def test_reference_bin_in_body_is_valid_regardless_of_candle_direction(self) -> None:
        self.engine.process_closed_candle(
            candle(
                0,
                open_price="104",
                high_price="120",
                low_price="90",
                close_price="100",
                bins=[
                    trigger_bin(
                        low="101",
                        high="102",
                        dominance="SELL",
                        spike_score="13",
                        index=101,
                    )
                ],
            )
        )

        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        self.assertEqual(self.state().direction, "LONG")

        engine = TriggerEngine()
        engine.process_closed_candle(
            candle(
                0,
                open_price="100",
                high_price="110",
                low_price="80",
                close_price="104",
                bins=[
                    trigger_bin(
                        low="102",
                        high="103",
                        dominance="BUY",
                        spike_score="13",
                        index=102,
                    )
                ],
            )
        )

        state = engine.state_for(symbol="NQ", timeframe="M5")
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "SHORT")

    def test_reference_contract_spike_score_threshold_is_twelve(self) -> None:
        self.engine.process_closed_candle(
            candle(
                0,
                bins=[
                    trigger_bin(
                        low="100",
                        high="101",
                        dominance="SELL",
                        spike_score="11.999",
                        index=100,
                    )
                ],
            )
        )
        self.assertEqual(self.state().state, IDLE)

        self.engine.process_closed_candle(
            candle(
                1,
                bins=[
                    trigger_bin(
                        low="100",
                        high="101",
                        dominance="SELL",
                        spike_score="12",
                        index=100,
                    )
                ],
            )
        )
        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        self.assertEqual(self.state().direction, "LONG")

    def test_reference_direction_uses_contracts_or_volume_not_dominance(self) -> None:
        cases = (
            ("NQ-LONG", "NQ.FUT", "BUY", "1", "2", True, "LONG", "SELL"),
            ("NQ-SHORT", "NQ.FUT", "SELL", "2", "1", True, "SHORT", "BUY"),
            ("BTC-LONG", "BTCUSDT", "BUY", "1", "2", False, "LONG", "SELL"),
            ("BTC-SHORT", "BTCUSDT", "SELL", "2", "1", False, "SHORT", "BUY"),
        )
        for symbol, provider, dominance, buy, sell, is_cme, direction, side in cases:
            with self.subTest(symbol=symbol):
                item = trigger_bin(
                    low="100",
                    high="101",
                    dominance=dominance,
                    spike_score="13",
                    index=100,
                )
                if is_cme:
                    item["l2"]["buy_contracts"] = buy
                    item["l2"]["sell_contracts"] = sell
                else:
                    item["l2"].pop("buy_contracts", None)
                    item["l2"].pop("sell_contracts", None)
                    item["l2"]["buy_volume"] = buy
                    item["l2"]["sell_volume"] = sell

                engine = TriggerEngine()
                engine.process_closed_candle(
                    candle(
                        0,
                        symbol=symbol,
                        provider_symbol=provider,
                        bins=[item],
                    )
                )
                state = engine.state_for(symbol=symbol, timeframe="M5")
                self.assertEqual(state.state, ABSORPTION_FOUND)
                self.assertEqual(state.direction, direction)
                self.assertEqual(state.reference_bin["side"], side)

    def test_simultaneous_buy_and_sell_references_are_ignored(self) -> None:
        self.engine.process_closed_candle(
            candle(
                0,
                open_price="100",
                high_price="106",
                low_price="94",
                close_price="102",
                bins=[
                    trigger_bin(
                        low="95",
                        high="96",
                        dominance="SELL",
                        spike_score="13",
                        index=95,
                    ),
                    trigger_bin(
                        low="104",
                        high="105",
                        dominance="BUY",
                        spike_score="13",
                        index=104,
                    ),
                ],
            )
        )

        self.assertEqual(self.state().state, IDLE)
        self.assertEqual(len(self.state().setups), 0)

    def test_peak_confirmation_uses_low_for_buy_and_high_for_sell(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="100", low_price="95.5", close_price="98.5")
        )

        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        engine = TriggerEngine()
        engine.process_closed_candle(sell_absorption_candle())
        engine.process_closed_candle(
            candle(1, open_price="103", high_price="104.5", low_price="101", close_price="102.5")
        )

        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, PEAK_CONFIRMED)

    def test_buy_signal_bin_requires_buy_side_and_buy_diagonal_ratio(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        blocked = self.engine.process_closed_candle(
            candle(
                2,
                open_price="95.5",
                high_price="99",
                low_price="94",
                close_price="96",
                bins=[
                    trigger_bin(
                        low="96",
                        high="97",
                        dominance="BUY",
                        spike_score="0",
                        index=96,
                        abnormal=False,
                        sell_diagonal_ratio="9",
                    )
                ],
            )
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="95.5",
                high_price="99",
                low_price="94",
                close_price="96",
                bins=[buy_signal_bin()],
            ),
            next_candle=candle(4, open_price="96.25", high_price="99", low_price="95", close_price="98"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["entry_price"], "96.25")

    def test_signal_bin_price_must_be_on_correct_side_of_reference(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        blocked_buy = self.engine.process_closed_candle(
            candle(
                2,
                open_price="95.5",
                high_price="100",
                low_price="94",
                close_price="96",
                bins=[buy_signal_bin(low="98", high="99", index=98)],
            )
        )

        self.assertEqual(blocked_buy, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        engine = TriggerEngine()
        engine.process_closed_candle(sell_absorption_candle())
        engine.process_closed_candle(
            candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
        )

        blocked_sell = engine.process_closed_candle(
            candle(
                2,
                open_price="104",
                high_price="107",
                low_price="101",
                close_price="104",
                bins=[sell_signal_bin(low="101", high="102", index=101)],
            )
        )

        self.assertEqual(blocked_sell, tuple())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, PEAK_CONFIRMED)

    def test_signal_bin_distance_from_reference_must_not_exceed_three_points(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5")
        )

        blocked_buy = self.engine.process_closed_candle(
            candle(
                2,
                open_price="92.5",
                high_price="97",
                low_price="92",
                close_price="93.5",
                bins=[buy_signal_bin(low="92", high="93", index=92)],
            )
        )

        self.assertEqual(blocked_buy, tuple())
        self.assertEqual(self.state().state, PEAK_CONFIRMED)

        buy_signal = self.engine.process_closed_candle(
            candle(
                3,
                open_price="93",
                high_price="97",
                low_price="92.5",
                close_price="94",
                bins=[buy_signal_bin(low="93", high="94", index=93)],
            ),
            next_candle=candle(4, open_price="94.25", high_price="96", low_price="94", close_price="95"),
        )[0].to_payload()

        self.assertEqual(buy_signal["signal_type"], "BUY_ENTRY")

        engine = TriggerEngine()
        engine.process_closed_candle(sell_absorption_candle())
        engine.process_closed_candle(
            candle(1, open_price="103", high_price="106", low_price="102", close_price="104.5")
        )

        blocked_sell = engine.process_closed_candle(
            candle(
                2,
                open_price="109",
                high_price="109.5",
                low_price="104",
                close_price="108",
                bins=[sell_signal_bin(low="108", high="109", index=108)],
            )
        )

        self.assertEqual(blocked_sell, tuple())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, PEAK_CONFIRMED)

        sell_signal = engine.process_closed_candle(
            candle(
                3,
                open_price="107",
                high_price="107.5",
                low_price="103",
                close_price="106",
                bins=[sell_signal_bin(low="106", high="107", index=106)],
            ),
            next_candle=candle(4, open_price="106.25", high_price="107", low_price="104", close_price="105"),
        )[0].to_payload()

        self.assertEqual(sell_signal["signal_type"], "SELL_ENTRY")

    def test_multiple_reference_bins_allow_signal_within_reference_range(self) -> None:
        self.engine.process_closed_candle(
            candle(
                0,
                open_price="100",
                high_price="103",
                low_price="94",
                close_price="102",
                bins=[
                    trigger_bin(
                        low="95",
                        high="96",
                        dominance="SELL",
                        spike_score="13",
                        index=95,
                    ),
                    trigger_bin(
                        low="97",
                        high="98",
                        dominance="SELL",
                        spike_score="14",
                        index=97,
                    ),
                ],
            )
        )

        signal = self.engine.process_closed_candle(
            candle(
                1,
                open_price="95",
                high_price="99",
                low_price="94.5",
                close_price="98",
                bins=[buy_signal_bin(low="96", high="97", index=96)],
            ),
            next_candle=candle(2, open_price="96.5", high_price="99", low_price="95", close_price="98"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "BUY_ENTRY")
        self.assertEqual(signal["reference_bin_low"], "97")
        self.assertEqual(signal["reference_bin_high"], "98")
        self.assertEqual(signal["entry_price"], "96.5")

    def test_trading_state_blocks_new_reference_search(self) -> None:
        self.test_buy_entry_state_flow_creates_position_with_stop_loss()

        blocked = self.engine.process_closed_candle(
            buy_absorption_candle(4),
            next_candle=candle(5, open_price="101", high_price="102", low_price="99", close_price="100"),
        )

        self.assertEqual(blocked, tuple())
        self.assertEqual(self.state().state, TRADING)
        self.assertIsNotNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

    def test_clear_position_resets_trading_state_after_external_exit(self) -> None:
        self.test_buy_entry_state_flow_creates_position_with_stop_loss()

        self.engine.clear_position(
            symbol="NQ",
            timeframe="M5",
            reason="RESET_AFTER_STOP_LOSS",
        )

        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))
        self.assertEqual(self.state().state, IDLE)

        self.engine.process_closed_candle(buy_absorption_candle(4))
        self.assertEqual(self.state().state, ABSORPTION_FOUND)

    def test_new_reference_adds_independent_entry_state(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.assertEqual(self.state().direction, "LONG")

        self.assertEqual(
            self.engine.process_closed_candle(sell_absorption_candle(1)),
            tuple(),
        )

        state = self.state()
        self.assertEqual(state.state, ABSORPTION_FOUND)
        self.assertEqual(state.direction, "LONG")
        self.assertEqual(state.reference_candle_time_ms, OPEN_TIME_MS)
        self.assertEqual(state.confirmation_candle_time_ms, 0)
        self.assertEqual(state.confirmation_state, "")
        self.assertEqual(state.candles_since_reference, 1)
        self.assertEqual(len(state.setups), 2)
        self.assertEqual(state.setups[0].direction, "LONG")
        self.assertEqual(state.setups[1].direction, "SHORT")
        self.assertEqual(state.setups[1].reference_candle_time_ms, OPEN_TIME_MS + M5_MS)
        self.assertEqual(state.setups[1].candles_since_reference, 0)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

    def test_independent_new_reference_can_trigger_without_resetting_first_setup(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        self.engine.process_closed_candle(sell_absorption_candle(1))
        self.assertEqual(len(self.state().setups), 2)

        signal = self.engine.process_closed_candle(
            candle(
                2,
                open_price="104.8",
                high_price="106",
                low_price="102",
                close_price="103.5",
                bins=[sell_signal_bin()],
            ),
            next_candle=candle(3, open_price="104.25", high_price="105", low_price="101", close_price="102"),
        )[0].to_payload()

        self.assertEqual(signal["signal_type"], "SELL_ENTRY")
        self.assertEqual(signal["reference_candle_time_ms"], OPEN_TIME_MS + M5_MS)
        self.assertEqual(signal["entry_price"], "104.25")

    def test_stale_trading_state_without_position_resets_before_entry_scan(self) -> None:
        self.state().state = TRADING

        self.engine.process_closed_candle(buy_absorption_candle())

        self.assertEqual(self.state().state, ABSORPTION_FOUND)
        self.assertEqual(self.state().direction, "LONG")

    def test_absorption_timeout_resets_after_two_candles_without_peak(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle())
        for index in range(1, 3):
            self.engine.process_closed_candle(
                candle(index, open_price="100", high_price="102", low_price="97", close_price="98")
            )

        self.assertEqual(self.state().state, IDLE)

    def test_absorption_timeout_counts_missing_timeframe_slots_before_buy_peak(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle(timeframe="M1"))

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(
                    3,
                    timeframe="M1",
                    open_price="95.5",
                    high_price="96",
                    low_price="94",
                    close_price="95",
                )
            ),
            tuple(),
        )

        self.assertEqual(self.state(timeframe="M1").state, IDLE)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M1"))

    def test_absorption_timeout_counts_missing_timeframe_slots_before_sell_break(self) -> None:
        self.engine.process_closed_candle(sell_absorption_candle(timeframe="M1"))

        self.assertEqual(
            self.engine.process_closed_candle(
                candle(
                    3,
                    timeframe="M1",
                    open_price="104",
                    high_price="106",
                    low_price="103",
                    close_price="105",
                )
            ),
            tuple(),
        )

        self.assertEqual(self.state(timeframe="M1").state, IDLE)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M1"))

    def test_buy_retest_timeout_resets_after_peak_without_late_entry(self) -> None:
        self.engine.process_closed_candle(buy_absorption_candle(timeframe="M1"))
        self.engine.process_closed_candle(
            candle(
                1,
                timeframe="M1",
                open_price="98",
                high_price="99",
                low_price="94.5",
                close_price="95.5",
            )
        )

        self.assertEqual(self.state(timeframe="M1").state, PEAK_CONFIRMED)
        late_retest = self.engine.process_closed_candle(
            candle(
                4,
                timeframe="M1",
                open_price="97.5",
                high_price="99",
                low_price="97",
                close_price="98.5",
            ),
            next_candle=candle(
                5,
                timeframe="M1",
                open_price="98.75",
                high_price="100",
                low_price="98",
                close_price="99",
            ),
        )

        self.assertEqual(late_retest, tuple())
        self.assertEqual(self.state(timeframe="M1").state, IDLE)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M1"))

    def test_sell_retest_timeout_resets_after_peak_without_late_entry(self) -> None:
        self.engine.process_closed_candle(sell_absorption_candle(timeframe="M1"))
        self.engine.process_closed_candle(
            candle(
                1,
                timeframe="M1",
                open_price="103",
                high_price="106",
                low_price="102",
                close_price="104.5",
            )
        )

        self.assertEqual(self.state(timeframe="M1").state, PEAK_CONFIRMED)
        late_retest = self.engine.process_closed_candle(
            candle(
                4,
                timeframe="M1",
                open_price="103.5",
                high_price="104",
                low_price="101",
                close_price="102.5",
            ),
            next_candle=candle(
                5,
                timeframe="M1",
                open_price="102.25",
                high_price="103",
                low_price="100",
                close_price="101",
            ),
        )

        self.assertEqual(late_retest, tuple())
        self.assertEqual(self.state(timeframe="M1").state, IDLE)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M1"))

    def test_all_timeframes_are_supported_by_default_and_state_is_separate(self) -> None:
        self.assertTrue(self.engine.supports_timeframe("M1"))
        self.assertTrue(self.engine.supports_timeframe("M5"))
        self.assertTrue(self.engine.supports_timeframe("H1"))

        self.engine.process_closed_candle(buy_absorption_candle(timeframe="M1"))
        self.engine.process_closed_candle(sell_absorption_candle(timeframe="M5"))

        self.assertEqual(self.state(timeframe="M1").state, ABSORPTION_FOUND)
        self.assertEqual(self.state(timeframe="M5").state, ABSORPTION_FOUND)
        self.assertEqual(self.state(timeframe="M1").direction, "LONG")
        self.assertEqual(self.state(timeframe="M5").direction, "SHORT")

    def test_configured_timeframes_limit_trigger_support(self) -> None:
        engine = TriggerEngine(TriggerConfig(supported_timeframes=("M15",)))

        self.assertFalse(engine.supports_timeframe("M5"))
        self.assertTrue(engine.supports_timeframe("M15"))

    def test_order_book_snapshot_context_can_be_supplied_without_triggering(self) -> None:
        payload = {
            "type": "DOM_TIMELINE_SESSION",
            "mt5_symbol": "NQ",
            "provider_symbol": "NQ.FUT",
            "timeframe": "M5",
            "window_end_ms": OPEN_TIME_MS + M5_MS - 1,
            "order_book_levels": [
                {"price": "100.00", "bid_contracts": 12, "ask_contracts": 0},
                {"price": "100.25", "bid_contracts": 0, "ask_contracts": 8},
            ],
        }

        snapshot = self.engine.set_order_book_snapshot(payload)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.best_bid, Decimal("100.00"))
        self.assertEqual(snapshot.best_ask, Decimal("100.25"))
        self.assertEqual(snapshot.level_at("100.00").bid_contracts, 12)
        self.assertEqual(
            self.engine.order_book_for(
                symbol="NQ",
                provider_symbol="NQ.FUT",
                timeframe="M5",
            ),
            snapshot,
        )
        self.assertEqual(
            self.engine.process_closed_candle(candle(0, bins=[]), order_book=payload),
            tuple(),
        )

    def test_order_book_snapshot_from_dom_payload_exposes_zone_liquidity(self) -> None:
        snapshot = order_book_snapshot_from_payload(
            {
                "symbol": "NQ",
                "provider_symbol": "NQ.FUT",
                "timeframe": "M5",
                "timestamp_ms": OPEN_TIME_MS,
                "order_book_levels": [
                    {"price": "99.75", "bid_contracts": 4, "ask_contracts": 0},
                    {"price": "100.00", "bid_contracts": 6, "ask_contracts": 2},
                    {"price": "100.25", "bid_contracts": 0, "ask_contracts": 7},
                ],
            }
        )

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(
            snapshot.liquidity_between("99.75", "100.00"),
            {"bid_contracts": 10, "ask_contracts": 2},
        )

    def test_evaluate_latest_only_processes_last_closed_candle(self) -> None:
        signals = self.engine.evaluate_latest(
            [
                buy_absorption_candle(0),
                candle(1, open_price="98", high_price="99", low_price="94", close_price="95.5"),
            ],
            evaluation_time_ms=OPEN_TIME_MS + 2 * M5_MS,
        )

        self.assertEqual(signals, tuple())
        self.assertEqual(self.state().state, IDLE)

    def test_enrich_candles_backtests_sequence_without_persisting_state(self) -> None:
        candles = [
            buy_absorption_candle(0),
            candle(1, open_price="98", high_price="99", low_price="94.5", close_price="95.5"),
            candle(
                2,
                open_price="95.5",
                high_price="99",
                low_price="94",
                close_price="96",
                bins=[buy_signal_bin()],
            ),
            candle(3, open_price="98.5", high_price="100", low_price="97", close_price="99"),
        ]

        signals = self.engine.enrich_candles(
            candles,
            evaluation_time_ms=OPEN_TIME_MS + 4 * M5_MS,
        )

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signal_type"], "BUY_ENTRY")
        self.assertEqual(candles[2]["trigger_signals"], signals)
        self.assertEqual(self.state().state, IDLE)
        self.assertIsNone(self.engine.position_for(symbol="NQ", timeframe="M5"))

    def test_both_chart_pages_render_backend_trigger_markers(self) -> None:
        footprint_page = _html_page("M5")
        candle_page = _candles_html_page("M5")

        for page in (footprint_page, candle_page):
            self.assertIn("function candleTriggerSignals(candle)", page)
            self.assertIn("drawTriggerMarkers(ctx, candle, center", page)
            self.assertIn('markerDirection === "DOWN" && markerColor === "RED"', page)
            self.assertIn('markerDirection === "UP" && markerColor === "GREEN"', page)
            self.assertIn('markerShape === "SQUARE"', page)
            self.assertIn('startsWith("EXIT_") ? "SQUARE" : "ARROW"', page)
            self.assertIn("function triggerMarkerAt(candle, centerX, yHigh, yLow, x, y)", page)
            self.assertIn("Reference candle ${triggerTimeLabel(signal?.reference_candle_time_ms)", page)
            self.assertIn("Contract spike score ${fmtMaybe(signal?.contract_spike_score ?? signal?.spike_score, 3)}", page)

    def test_candle_page_reconciles_session_signals_with_cached_candles(self) -> None:
        candle_page = _candles_html_page("M5")

        self.assertIn("function signalTriggerTime(signal)", candle_page)
        self.assertIn("const signalsByOpen = new Map();", candle_page)
        self.assertIn("let replayOverlayActive = false;", candle_page)
        self.assertIn("if (!markerOverlaysHidden && !replayOverlayActive)", candle_page)
        self.assertIn("for (const signal of safeArray(session?.signals))", candle_page)
        self.assertIn("const replaySignals = markerOverlaysHidden ? [] : replayTriggerSignalsByCandleOpen.get(openTime);", candle_page)
        self.assertIn("...(replaySignals || []),", candle_page)
        self.assertIn("candle.trigger_signals = [...signalsById.values()];", candle_page)


class TriggerOrderBookDomPayloadTests(unittest.TestCase):
    @staticmethod
    def _zone_payload(
        *,
        payload_id: str,
        side: str,
        entry_direction: str,
        price: str,
        zone_low: str,
        zone_high: str,
        offset: int,
    ) -> dict[str, object]:
        timestamp_ms = OPEN_TIME_MS + offset * M5_MS
        return {
            "payload_id": payload_id,
            "id": payload_id,
            "output_id": payload_id,
            "type": "ABSORPTION",
            "payload_type": "ABSORPTION",
            "action": "ENTRY",
            "mt5_symbol": "NQ",
            "provider_symbol": "NQ.FUT",
            "timeframe": "M5",
            "timestamp_ms": timestamp_ms,
            "event_time_ms": timestamp_ms,
            "footprint_open_time_ms": timestamp_ms,
            "price": price,
            "side": side,
            "refill_side": side,
            "entry_direction": entry_direction,
            "reference_side": "SELL" if entry_direction == "LONG" else "BUY",
            "refill_count": 1,
            "refill_contracts": 2,
            "price_base_refill_count": 1,
            "price_base_refill_contracts": 2,
            "refill_method": "price_base_refill",
            "price_base_refill_count": 1,
            "price_base_refill_contracts": 2,
            "refill_method": "price_base_refill",
            "zone_low": zone_low,
            "zone_high": zone_high,
            "reference_zone_low": zone_low,
            "reference_zone_high": zone_high,
            "zone_level_count": 3,
        }

    def test_trading_path_rejects_legacy_refill_without_price_base_contract(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        payload = self._zone_payload(
            payload_id="LEGACY-REFILL",
            side="BID",
            entry_direction="LONG",
            price="96",
            zone_low="95",
            zone_high="97",
            offset=0,
        )
        payload.pop("price_base_refill_count")
        payload.pop("price_base_refill_contracts")
        payload.pop("refill_method")

        engine.set_dom_output_snapshot(payload)

        self.assertFalse(any(engine._refills_by_key.values()))

    def test_trading_path_uses_price_base_contracts_not_executed_contracts(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        payload = self._zone_payload(
            payload_id="PRICE-BASE-REFILL",
            side="BID",
            entry_direction="LONG",
            price="96",
            zone_low="95",
            zone_high="97",
            offset=0,
        )
        payload["executed_contracts"] = 999
        payload["refill_filled_contracts"] = 999

        engine.set_dom_output_snapshot(payload)

        records = tuple(record for bucket in engine._refills_by_key.values() for record in bucket.values())
        self.assertTrue(records)
        self.assertTrue(all(record.refill_count == 1 for record in records))
        self.assertTrue(all(record.refill_total == 2 for record in records))

    def test_opposite_entry_trigger_reverses_existing_position(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="SELL-ZONE-0",
                side="ASK",
                entry_direction="SHORT",
                price="105",
                zone_low="104",
                zone_high="106",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="99", close_price="100.5")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="101", high_price="103", low_price="99", close_price="100")
            )
        sell_entry = engine.process_closed_candle(
            candle(
                5,
                open_price="107",
                high_price="107",
                low_price="100",
                close_price="103",
                bins=[sell_signal_bin(low="103", high="103.25", index=103)],
            ),
            next_candle=candle(6, open_price="103", high_price="104", low_price="101", close_price="102"),
        )

        self.assertEqual(len(sell_entry), 1)
        self.assertEqual(sell_entry[0].signal_type, "SELL_ENTRY")
        self.assertEqual(engine.position_for(symbol="NQ", timeframe="M5").side, "SHORT")

        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="BUY-ZONE-6",
                side="BID",
                entry_direction="LONG",
                price="96",
                zone_low="95",
                zone_high="97",
                offset=6,
            )
        )
        engine.process_closed_candle(
            candle(6, open_price="103", high_price="104", low_price="101", close_price="102")
        )
        for offset in range(7, 11):
            engine.process_closed_candle(
                candle(offset, open_price="102", high_price="104", low_price="101", close_price="102.5")
            )
        reversal = engine.process_closed_candle(
            candle(
                11,
                open_price="94",
                high_price="100",
                low_price="95",
                close_price="99",
                bins=[buy_signal_bin(low="98", high="98.25", index=98)],
            ),
            next_candle=candle(12, open_price="100", high_price="102", low_price="99", close_price="101"),
        )

        self.assertEqual([signal.signal_type for signal in reversal], ["EXIT_SELL", "BUY_ENTRY"])
        self.assertEqual(reversal[0].reason, "SELL_EXIT_REVERSE_TO_BUY")
        self.assertEqual(reversal[0].exit_price, reversal[1].entry_price)
        self.assertEqual(reversal[1].reason, "BUY_ENTRY_EXECUTION_ABOVE_ZONE_HIGH")
        self.assertEqual(reversal[1].stop_loss, Decimal("95"))
        self.assertEqual(engine.position_for(symbol="NQ", timeframe="M5").side, "LONG")
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, TRADING)

    def test_entry_keeps_older_opposite_zone_available_for_later_reversal(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="BUY-ZONE-0",
                side="BID",
                entry_direction="LONG",
                price="96",
                zone_low="95",
                zone_high="97",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="98", close_price="102")
        )
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="SELL-ZONE-1",
                side="ASK",
                entry_direction="SHORT",
                price="105",
                zone_low="104",
                zone_high="106",
                offset=1,
            )
        )
        engine.process_closed_candle(
            candle(1, open_price="101", high_price="103", low_price="99", close_price="100.5")
        )
        for offset in range(2, 6):
            engine.process_closed_candle(
                candle(offset, open_price="101", high_price="103", low_price="99", close_price="100")
            )

        sell_entry = engine.process_closed_candle(
            candle(
                6,
                open_price="107",
                high_price="107",
                low_price="100",
                close_price="103",
                bins=[sell_signal_bin(low="103", high="103.25", index=103)],
            ),
            next_candle=candle(7, open_price="103", high_price="104", low_price="101", close_price="102"),
        )

        self.assertEqual([signal.signal_type for signal in sell_entry], ["SELL_ENTRY"])
        self.assertEqual(engine.position_for(symbol="NQ", timeframe="M5").side, "SHORT")
        self.assertEqual(
            [setup.direction for setup in engine.state_for(symbol="NQ", timeframe="M5").setups],
            ["LONG"],
        )

        for offset in range(7, 11):
            engine.process_closed_candle(
                candle(offset, open_price="102", high_price="104", low_price="98", close_price="102.5")
            )
        reversal = engine.process_closed_candle(
            candle(
                11,
                open_price="94",
                high_price="100",
                low_price="95",
                close_price="99",
                bins=[buy_signal_bin(low="98", high="98.25", index=98)],
            ),
            next_candle=candle(12, open_price="100", high_price="102", low_price="99", close_price="101"),
        )

        self.assertEqual([signal.signal_type for signal in reversal], ["EXIT_SELL", "BUY_ENTRY"])
        self.assertEqual(reversal[1].to_payload()["process_payload_id"], "BUY-ZONE-0")
        self.assertEqual(reversal[1].reason, "BUY_ENTRY_EXECUTION_ABOVE_ZONE_HIGH")
        self.assertEqual(engine.position_for(symbol="NQ", timeframe="M5").side, "LONG")

    def test_buy_entry_requires_execution_above_zone_high_and_uses_zone_low_stop(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="BUY-ZONE-0",
                side="BID",
                entry_direction="LONG",
                price="96",
                zone_low="95",
                zone_high="97",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="95", close_price="102")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="103", high_price="104", low_price="99", close_price="102")
            )

        no_execution = engine.process_closed_candle(
            candle(5, open_price="98", high_price="103", low_price="95", close_price="102"),
            next_candle=candle(6, open_price="102", high_price="104", low_price="101", close_price="103"),
        )
        signal = engine.process_closed_candle(
            candle(
                6,
                open_price="102",
                high_price="104",
                low_price="95",
                close_price="101",
                bins=[buy_signal_bin(low="98", high="98.25", index=98)],
            ),
            next_candle=candle(7, open_price="101", high_price="103", low_price="100", close_price="102"),
        )

        self.assertEqual(no_execution, tuple())
        self.assertEqual(len(signal), 1)
        self.assertEqual(signal[0].signal_type, "BUY_ENTRY")
        self.assertEqual(signal[0].reason, "BUY_ENTRY_EXECUTION_ABOVE_ZONE_HIGH")
        self.assertEqual(signal[0].stop_loss, Decimal("95"))
        self.assertEqual(signal[0].matched_bins[1]["low"], "98")

    def test_sell_entry_requires_execution_below_zone_low_and_uses_zone_high_stop(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="SELL-ZONE-0",
                side="ASK",
                entry_direction="SHORT",
                price="105",
                zone_low="104",
                zone_high="106",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="99", close_price="100.5")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="101", high_price="103", low_price="99", close_price="100")
            )

        no_execution = engine.process_closed_candle(
            candle(5, open_price="107", high_price="107", low_price="100", close_price="103"),
            next_candle=candle(6, open_price="103", high_price="104", low_price="101", close_price="102"),
        )
        signal = engine.process_closed_candle(
            candle(
                6,
                open_price="103",
                high_price="107",
                low_price="100",
                close_price="102",
                bins=[sell_signal_bin(low="103", high="103.25", index=103)],
            ),
            next_candle=candle(7, open_price="102", high_price="103", low_price="100", close_price="101"),
        )

        self.assertEqual(no_execution, tuple())
        self.assertEqual(len(signal), 1)
        self.assertEqual(signal[0].signal_type, "SELL_ENTRY")
        self.assertEqual(signal[0].reason, "SELL_ENTRY_EXECUTION_BELOW_ZONE_LOW")
        self.assertEqual(signal[0].stop_loss, Decimal("106"))
        self.assertEqual(signal[0].matched_bins[1]["high"], "103.25")

    def test_touched_sell_zone_can_trigger_on_later_execution_without_second_touch(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="SELL-ZONE-0",
                side="ASK",
                entry_direction="SHORT",
                price="105",
                zone_low="104",
                zone_high="106",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="99", close_price="101")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="101", high_price="103", low_price="99", close_price="100")
            )

        touched = engine.process_closed_candle(
            candle(5, open_price="107", high_price="107", low_price="100", close_price="103")
        )
        state = engine.state_for(symbol="NQ", timeframe="M5")
        self.assertEqual(touched, tuple())
        self.assertEqual(state.state, ZONE_TOUCHED)
        self.assertEqual(state.confirmation_candle_time_ms, OPEN_TIME_MS + 5 * M5_MS)

        signal = engine.process_closed_candle(
            candle(
                6,
                open_price="103",
                high_price="103.5",
                low_price="100",
                close_price="101",
                bins=[sell_signal_bin(low="103", high="103.25", index=103)],
            ),
            next_candle=candle(7, open_price="101", high_price="102", low_price="100", close_price="100.5"),
        )

        self.assertEqual(len(signal), 1)
        self.assertEqual(signal[0].signal_type, "SELL_ENTRY")
        self.assertEqual(signal[0].trigger_candle_time_ms, OPEN_TIME_MS + 6 * M5_MS)
        self.assertEqual(signal[0].confirmation_candle_time_ms, OPEN_TIME_MS + 5 * M5_MS)
        self.assertEqual(engine.position_for(symbol="NQ", timeframe="M5").side, "SHORT")

    def test_reference_zone_can_wait_more_than_ten_candles_for_first_touch(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            {
                **self._zone_payload(
                    payload_id="SELL-ZONE-0",
                    side="ASK",
                    entry_direction="SHORT",
                    price="105",
                    zone_low="104",
                    zone_high="106",
                    offset=0,
                ),
                "timeframe": "M1",
            }
        )
        engine.process_closed_candle(
            candle(0, timeframe="M1", open_price="100", high_price="103", low_price="99", close_price="101")
        )
        for offset in range(1, 17):
            engine.process_closed_candle(
                candle(offset, timeframe="M1", open_price="101", high_price="103", low_price="99", close_price="100")
            )

        touched = engine.process_closed_candle(
            candle(17, timeframe="M1", open_price="107", high_price="107", low_price="100", close_price="103")
        )
        state = engine.state_for(symbol="NQ", timeframe="M1")
        self.assertEqual(touched, tuple())
        self.assertEqual(state.state, ZONE_TOUCHED)
        self.assertEqual(state.confirmation_candle_time_ms, OPEN_TIME_MS + 17 * M1_MS)

        signal = engine.process_closed_candle(
            candle(
                18,
                timeframe="M1",
                open_price="103",
                high_price="103.5",
                low_price="100",
                close_price="101",
                bins=[sell_signal_bin(low="103", high="103.25", index=103)],
            ),
            next_candle=candle(19, timeframe="M1", open_price="101", high_price="102", low_price="100", close_price="100.5"),
        )

        self.assertEqual(len(signal), 1)
        self.assertEqual(signal[0].signal_type, "SELL_ENTRY")
        self.assertEqual(signal[0].trigger_candle_time_ms, OPEN_TIME_MS + 18 * M1_MS)
        self.assertEqual(signal[0].confirmation_candle_time_ms, OPEN_TIME_MS + 17 * M1_MS)

    def test_touched_buy_zone_can_trigger_on_later_execution_without_second_touch(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="BUY-ZONE-0",
                side="BID",
                entry_direction="LONG",
                price="96",
                zone_low="95",
                zone_high="97",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="98", close_price="102")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="103", high_price="104", low_price="99", close_price="102")
            )

        touched = engine.process_closed_candle(
            candle(5, open_price="98", high_price="103", low_price="96", close_price="102")
        )
        state = engine.state_for(symbol="NQ", timeframe="M5")
        self.assertEqual(touched, tuple())
        self.assertEqual(state.state, ZONE_TOUCHED)
        self.assertEqual(state.confirmation_candle_time_ms, OPEN_TIME_MS + 5 * M5_MS)

        signal = engine.process_closed_candle(
            candle(
                6,
                open_price="102",
                high_price="104",
                low_price="99",
                close_price="103",
                bins=[buy_signal_bin(low="98", high="98.25", index=98)],
            ),
            next_candle=candle(7, open_price="103", high_price="104", low_price="102", close_price="103.5"),
        )

        self.assertEqual(len(signal), 1)
        self.assertEqual(signal[0].signal_type, "BUY_ENTRY")
        self.assertEqual(signal[0].trigger_candle_time_ms, OPEN_TIME_MS + 6 * M5_MS)
        self.assertEqual(signal[0].confirmation_candle_time_ms, OPEN_TIME_MS + 5 * M5_MS)
        self.assertEqual(engine.position_for(symbol="NQ", timeframe="M5").side, "LONG")

    def test_touched_zone_expires_after_ten_touched_candles(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        engine.set_dom_output_snapshot(
            self._zone_payload(
                payload_id="SELL-ZONE-0",
                side="ASK",
                entry_direction="SHORT",
                price="105",
                zone_low="104",
                zone_high="106",
                offset=0,
            )
        )
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="99", close_price="101")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="101", high_price="103", low_price="99", close_price="100")
            )
        engine.process_closed_candle(
            candle(5, open_price="107", high_price="107", low_price="100", close_price="103")
        )
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, ZONE_TOUCHED)
        for offset in range(6, 16):
            engine.process_closed_candle(
                candle(offset, open_price="103", high_price="103.5", low_price="100", close_price="101")
            )

        expired = engine.process_closed_candle(
            candle(
                16,
                open_price="103",
                high_price="103.5",
                low_price="100",
                close_price="101",
                bins=[sell_signal_bin(low="103", high="103.25", index=103)],
            )
        )

        self.assertEqual(expired, tuple())
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)

    def test_canceled_reference_zone_removes_only_matching_symbol_touched_setup(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        nq_payload = self._zone_payload(
            payload_id="NQ-SELL-ZONE-0",
            side="ASK",
            entry_direction="SHORT",
            price="105",
            zone_low="104",
            zone_high="106",
            offset=0,
        )
        es_payload = dict(nq_payload)
        es_payload.update(
            {
                "payload_id": "ES-SELL-ZONE-0",
                "id": "ES-SELL-ZONE-0",
                "output_id": "ES-SELL-ZONE-0",
                "mt5_symbol": "ES",
                "provider_symbol": "ES.FUT",
                "symbol": "ES.FUT",
            }
        )
        engine.set_dom_output_snapshot(nq_payload)
        engine.set_dom_output_snapshot(es_payload)
        engine.process_closed_candle(
            candle(0, symbol="NQ", provider_symbol="NQ.FUT", open_price="100", high_price="103", low_price="99", close_price="101")
        )
        engine.process_closed_candle(
            candle(0, symbol="ES", provider_symbol="ES.FUT", open_price="100", high_price="103", low_price="99", close_price="101")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, symbol="NQ", provider_symbol="NQ.FUT", open_price="101", high_price="103", low_price="99", close_price="100")
            )
            engine.process_closed_candle(
                candle(offset, symbol="ES", provider_symbol="ES.FUT", open_price="101", high_price="103", low_price="99", close_price="100")
            )
        engine.process_closed_candle(
            candle(5, symbol="NQ", provider_symbol="NQ.FUT", open_price="107", high_price="107", low_price="100", close_price="103")
        )
        engine.process_closed_candle(
            candle(5, symbol="ES", provider_symbol="ES.FUT", open_price="107", high_price="107", low_price="100", close_price="103")
        )
        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, ZONE_TOUCHED)
        self.assertEqual(engine.state_for(symbol="ES", timeframe="M5").state, ZONE_TOUCHED)

        replacement = self._zone_payload(
            payload_id="NQ-SELL-ZONE-1",
            side="ASK",
            entry_direction="SHORT",
            price="109",
            zone_low="108",
            zone_high="110",
            offset=6,
        )
        replacement["canceled_zone_ids"] = ("NQ-SELL-ZONE-0",)
        engine.set_dom_output_snapshot(replacement)

        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").state, IDLE)
        self.assertEqual(engine.state_for(symbol="ES", timeframe="M5").state, ZONE_TOUCHED)

    def test_canceled_zone_ids_remove_refill_records_from_trigger_memory(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        base_payload = {
            "payload_id": "PROCESS-ZONE-OLD",
            "id": "PROCESS-ZONE-OLD",
            "output_id": "PROCESS-ZONE-OLD",
            "type": "ABSORPTION",
            "payload_type": "ABSORPTION",
            "action": "ENTRY",
            "mt5_symbol": "NQ",
            "provider_symbol": "NQ.FUT",
            "timeframe": "M5",
            "timestamp_ms": OPEN_TIME_MS,
            "footprint_open_time_ms": OPEN_TIME_MS,
            "price": "96",
            "side": "BID",
            "refill_side": "BID",
            "refill_count": 1,
            "refill_contracts": 2,
            "price_base_refill_count": 1,
            "price_base_refill_contracts": 2,
            "refill_method": "price_base_refill",
            "zone_low": "95",
            "zone_high": "97",
            "reference_zone_low": "95",
            "reference_zone_high": "97",
            "zone_level_count": 1,
        }
        engine.set_dom_output_snapshot(base_payload)
        self.assertTrue(any("PROCESS-ZONE-OLD" in bucket for bucket in engine._refills_by_key.values()))

        replacement = dict(base_payload)
        replacement.update(
            {
                "payload_id": "PROCESS-ZONE-NEW",
                "id": "PROCESS-ZONE-NEW",
                "output_id": "PROCESS-ZONE-NEW",
                "timestamp_ms": OPEN_TIME_MS + 1,
                "price": "94",
                "zone_low": "93",
                "zone_high": "95",
                "reference_zone_low": "93",
                "reference_zone_high": "95",
                "canceled_zone_ids": ("PROCESS-ZONE-OLD",),
            }
        )
        engine.set_dom_output_snapshot(replacement)

        self.assertFalse(any("PROCESS-ZONE-OLD" in bucket for bucket in engine._refills_by_key.values()))
        self.assertTrue(any("PROCESS-ZONE-NEW" in bucket for bucket in engine._refills_by_key.values()))

    def test_trigger_engine_sink_passes_canceled_zone_ids_to_trigger_memory(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        sink = TriggerEngineSink(engine)
        old_payload = self._zone_payload(
            payload_id="PROCESS-ZONE-OLD",
            side="ASK",
            entry_direction="SHORT",
            price="105",
            zone_low="104",
            zone_high="106",
            offset=0,
        )
        sink.publish((old_payload,))
        self.assertTrue(any("PROCESS-ZONE-OLD" in bucket for bucket in engine._refills_by_key.values()))

        replacement = self._zone_payload(
            payload_id="PROCESS-ZONE-NEW",
            side="ASK",
            entry_direction="SHORT",
            price="109",
            zone_low="108",
            zone_high="110",
            offset=1,
        )
        replacement["canceled_zone_ids"] = ("PROCESS-ZONE-OLD",)
        sink.publish((replacement,))

        self.assertFalse(any("PROCESS-ZONE-OLD" in bucket for bucket in engine._refills_by_key.values()))
        self.assertTrue(any("PROCESS-ZONE-NEW" in bucket for bucket in engine._refills_by_key.values()))

    def test_canceled_zone_ids_remove_existing_entry_setups(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        old_payload = self._zone_payload(
            payload_id="PROCESS-ZONE-OLD",
            side="ASK",
            entry_direction="SHORT",
            price="105",
            zone_low="104",
            zone_high="106",
            offset=0,
        )
        engine.set_dom_output_snapshot(old_payload)
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="99", close_price="101")
        )
        self.assertEqual(
            _reference_payload_id(engine.state_for(symbol="NQ", timeframe="M5").setups[0].reference_bin),
            "PROCESS-ZONE-OLD",
        )

        replacement = self._zone_payload(
            payload_id="PROCESS-ZONE-NEW",
            side="ASK",
            entry_direction="SHORT",
            price="109",
            zone_low="108",
            zone_high="110",
            offset=1,
        )
        replacement["canceled_zone_ids"] = ("PROCESS-ZONE-OLD",)
        engine.set_dom_output_snapshot(replacement)

        self.assertEqual(engine.state_for(symbol="NQ", timeframe="M5").setups, tuple())

    def test_replay_trigger_enrichment_feeds_payloads_chronologically(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        old_open_ms = OPEN_TIME_MS
        replacement_open_ms = OPEN_TIME_MS + 12 * M1_MS

        def m1_zone_payload(
            *,
            payload_id: str,
            open_time_ms: int,
            price: str,
            zone_low: str,
            zone_high: str,
            canceled_zone_ids: tuple[str, ...] = (),
        ) -> dict[str, object]:
            event_time_ms = open_time_ms + 30_000
            payload = {
                "payload_id": payload_id,
                "id": payload_id,
                "output_id": payload_id,
                "type": "ABSORPTION",
                "payload_type": "ABSORPTION",
                "action": "ENTRY",
                "mt5_symbol": "NQ",
                "provider_symbol": "NQ.FUT",
                "timeframe": "M1",
                "timestamp_ms": event_time_ms,
                "event_time_ms": event_time_ms,
                "threshold_time_ms": event_time_ms,
                "close_time_ms": event_time_ms,
                "footprint_open_time_ms": open_time_ms,
                "marker_time_ms": open_time_ms,
                "price": price,
                "side": "ASK",
                "refill_side": "ASK",
                "entry_direction": "SHORT",
                "reference_side": "BUY",
                "refill_count": 1,
                "refill_contracts": 2,
                "price_base_refill_count": 1,
                "price_base_refill_contracts": 2,
                "refill_method": "price_base_refill",
                "zone_low": zone_low,
                "zone_high": zone_high,
                "reference_zone_low": zone_low,
                "reference_zone_high": zone_high,
                "zone_level_count": 3,
            }
            if canceled_zone_ids:
                payload["canceled_zone_ids"] = canceled_zone_ids
            return payload

        candles = [
            candle(
                offset,
                timeframe="M1",
                open_price="100",
                high_price="103",
                low_price="99",
                close_price="101",
            )
            for offset in range(19)
        ]
        candles[12].update(
            {
                "open_price": "107",
                "high_price": "107",
                "low_price": "100",
                "close_price": "103",
                "bins": [sell_signal_bin(low="103", high="103.25", index=103)],
            }
        )
        candles[17].update(
            {
                "open_price": "111",
                "high_price": "112",
                "low_price": "105",
                "close_price": "106",
                "bins": [sell_signal_bin(low="108.5", high="108.75", index=108)],
            }
        )

        signals = _enrich_replay_candles_chronologically(
            trigger_engine=engine,
            replay_candles=candles,
            replay_payloads=(
                m1_zone_payload(
                    payload_id="PROCESS-ZONE-OLD",
                    open_time_ms=old_open_ms,
                    price="105",
                    zone_low="104",
                    zone_high="106",
                ),
                m1_zone_payload(
                    payload_id="PROCESS-ZONE-NEW",
                    open_time_ms=replacement_open_ms,
                    price="109",
                    zone_low="109",
                    zone_high="111",
                    canceled_zone_ids=("PROCESS-ZONE-OLD",),
                ),
            ),
            evaluation_time_ms=OPEN_TIME_MS + 19 * M1_MS,
        )

        self.assertEqual(candles[12]["trigger_signals"], [])
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0]["signal_type"], "SELL_ENTRY")
        self.assertEqual(signals[0]["trigger_candle_time_ms"], OPEN_TIME_MS + 17 * M1_MS)
        self.assertEqual(signals[0]["reference_candle_time_ms"], replacement_open_ms)
        self.assertEqual(signals[0]["process_payload_id"], "PROCESS-ZONE-NEW")

    def test_entry_trigger_clears_active_refill_zones_for_symbol_timeframe(self) -> None:
        engine = TriggerEngine(TriggerConfig(refill_lifetime_candles=100))
        for index, price in enumerate(("96", "96.5")):
            engine.set_dom_output_snapshot(
                {
                    "payload_id": f"PROCESS-ZONE-{index}",
                    "id": f"PROCESS-ZONE-{index}",
                    "output_id": f"PROCESS-ZONE-{index}",
                    "type": "ABSORPTION",
                    "payload_type": "ABSORPTION",
                    "action": "ENTRY",
                    "mt5_symbol": "NQ",
                    "provider_symbol": "NQ.FUT",
                    "timeframe": "M5",
                    "timestamp_ms": OPEN_TIME_MS + index,
                    "event_time_ms": OPEN_TIME_MS + index,
                    "footprint_open_time_ms": OPEN_TIME_MS,
                    "price": price,
                    "side": "BID",
                    "refill_side": "BID",
                    "entry_direction": "LONG",
                    "reference_side": "SELL",
                    "refill_count": 1,
                    "refill_contracts": 2,
                    "price_base_refill_count": 1,
                    "price_base_refill_contracts": 2,
                    "refill_method": "price_base_refill",
                    "zone_low": str(Decimal(price) - Decimal("1")),
                    "zone_high": str(Decimal(price) + Decimal("1")),
                    "reference_zone_low": str(Decimal(price) - Decimal("1")),
                    "reference_zone_high": str(Decimal(price) + Decimal("1")),
                    "zone_level_count": 1,
                }
            )

        self.assertTrue(any(engine._refills_by_key.values()))
        engine.process_closed_candle(
            candle(0, open_price="100", high_price="103", low_price="95", close_price="102")
        )
        for offset in range(1, 5):
            engine.process_closed_candle(
                candle(offset, open_price="103", high_price="104", low_price="99", close_price="102")
            )
        signal = engine.process_closed_candle(
            candle(
                5,
                open_price="98",
                high_price="103",
                low_price="95",
                close_price="102",
                bins=[buy_signal_bin(low="98", high="98.25", index=98)],
            ),
            next_candle=candle(6, open_price="102", high_price="104", low_price="101", close_price="103"),
        )

        self.assertEqual(len(signal), 1)
        self.assertEqual(signal[0].signal_type, "BUY_ENTRY")
        self.assertFalse(any(engine._refills_by_key.values()))

    def test_full_dom_payload_is_available_to_trigger_engine(self) -> None:
        payload = {
            "type": "DOM_TIMELINE_SESSION",
            "mt5_symbol": "NQ",
            "provider_symbol": "NQ.FUT",
            "timeframe": "M5",
            "window_end_ms": OPEN_TIME_MS + M5_MS - 1,
            "order_book_levels": [
                {
                    "price": "100.25",
                    "bid_contracts": 0,
                    "ask_contracts": 0,
                    "raw_buy_execute_contracts": 25,
                    "raw_sell_execute_contracts": 7,
                    "top_order_id": "OID-1",
                    "top_order_type": "MODIFY",
                    "top_order_positive_refill_count": 3,
                    "top_order_positive_refill_total": 12,
                }
            ],
            "events": [
                {
                    "event_id": "EVT-1",
                    "timestamp_ms": OPEN_TIME_MS,
                    "order_id": "OID-1",
                    "event_type": "MODIFY",
                    "positive_refill_count": 1,
                    "positive_refill_contracts": 12,
                }
            ],
            "raw_events": [
                {
                    "timestamp_ms": OPEN_TIME_MS,
                    "order_id": "OID-1",
                    "action": "M",
                    "side": "ASK",
                    "size": 12,
                }
            ],
            "resting_segments": [
                {
                    "order_id": "OID-1",
                    "start_ms": OPEN_TIME_MS,
                    "end_ms": OPEN_TIME_MS + 1000,
                    "price": "100.25",
                }
            ],
            "best_bid_line": [{"timestamp_ms": OPEN_TIME_MS, "price": "100.00"}],
            "best_ask_line": [{"timestamp_ms": OPEN_TIME_MS, "price": "100.25"}],
            "iceberg_filter": {
                "active": True,
                "metric": "positive_refill_total",
                "order_ids": ["OID-1"],
            },
            "debug": {"visible_event_count": 1},
            "viewport_metrics": {"mbo_event_count": 1},
        }
        engine = TriggerEngine()

        snapshot = engine.set_dom_output_snapshot(payload)

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.best_bid, Decimal("100.00"))
        self.assertEqual(snapshot.best_ask, Decimal("100.25"))
        self.assertEqual(snapshot.events[0]["positive_refill_contracts"], 12)
        self.assertEqual(snapshot.raw_events[0]["order_id"], "OID-1")
        self.assertEqual(snapshot.resting_segments[0]["order_id"], "OID-1")
        self.assertEqual(snapshot.iceberg_filter["order_ids"], ["OID-1"])
        level = snapshot.level_at("100.25")
        self.assertIsNotNone(level)
        assert level is not None
        self.assertEqual(level.raw_buy_execute_contracts, 25)
        self.assertEqual(level.raw_sell_execute_contracts, 7)
        self.assertEqual(level.top_order_positive_refill_total, 12)
        self.assertEqual(
            snapshot.events_between(OPEN_TIME_MS, OPEN_TIME_MS)[0]["event_id"],
            "EVT-1",
        )
        self.assertEqual(snapshot.events_for_order("OID-1")[0]["event_id"], "EVT-1")
        dom_output = engine.dom_output_for(
            symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M5",
        )
        self.assertIsNotNone(dom_output)
        assert dom_output is not None
        self.assertEqual(dom_output["events"][0]["order_id"], "OID-1")

    def test_dom_timeline_payload_with_sessions_can_store_snapshot(self) -> None:
        payload = {
            "type": "DOM_TIMELINE_SNAPSHOT",
            "timeframe": "M5",
            "sessions": [
                {
                    "type": "DOM_TIMELINE_SESSION",
                    "mt5_symbol": "NQ",
                    "provider_symbol": "NQ.FUT",
                    "timeframe": "M5",
                    "order_book_levels": [
                        {"price": "100.00", "bid_contracts": 4, "ask_contracts": 0}
                    ],
                }
            ],
        }

        snapshots = order_book_snapshots_from_payload(payload)

        self.assertEqual(len(snapshots), 1)
        self.assertEqual(snapshots[0].level_at("100.00").bid_contracts, 4)


if __name__ == "__main__":
    unittest.main()
