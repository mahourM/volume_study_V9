from __future__ import annotations

import unittest
<<<<<<< HEAD
from dataclasses import replace
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from decimal import Decimal

from absorption.absorption_runtime import EntryState, LiveAbsorptionRuntime
from absorption_module.absorption_cluster_builder import build_candle_absorption_results
from absorption_module.absorption_cluster_model import (
    AbsorptionCandidateType,
    AbsorptionRuntimeConfig,
    BinMarketData,
    TimeframeSpec,
    TradeSide,
)
from execution.trading_decision_engine import EntryTrigger, ExitTrigger, OpenPositionState, TradingDecisionEngine
from execution.trading_zone_state import ZoneState, ZoneStateStore
<<<<<<< HEAD
from study.study_snapshot import CandleRecord, FrozenL2BinState
=======
from study.study_snapshot import CandleRecord
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


def config() -> AbsorptionRuntimeConfig:
    return AbsorptionRuntimeConfig(
        enabled_timeframes=(TimeframeSpec("M15", 900_000),),
        rolling_candle_buffer_size=3,
    )


def market_bin(
    index: int,
    *,
    timeframe_name: str = "M5",
    time_ms: int = 1_000,
    buy: float = 10.0,
    sell: float = 10.0,
    buy_diag: float = 10.0,
    sell_diag: float = 10.0,
    delta: float = 1.0,
    min_trade_price: float | None = None,
    max_trade_price: float | None = None,
    dominant_side_efficiency: float | None = None,
    has_trade_prices: bool = True,
) -> BinMarketData:
    if buy_diag > sell_diag:
        dominant_side = "BUY"
        dominant_volume = buy
    elif sell_diag > buy_diag:
        dominant_side = "SELL"
        dominant_volume = sell
    else:
        dominant_side = "NONE"
        dominant_volume = 0.0
    resolved_min = (
        None
        if not has_trade_prices
        else float(index) + 0.1
        if min_trade_price is None
        else min_trade_price
    )
    resolved_max = (
        None
        if not has_trade_prices
        else float(index) + 0.2
        if max_trade_price is None
        else max_trade_price
    )
    if (
        has_trade_prices
        and dominant_side_efficiency is not None
        and dominant_volume > 0
        and min_trade_price is None
        and max_trade_price is None
    ):
        resolved_min = float(index) + 0.1
        resolved_max = resolved_min + (float(dominant_side_efficiency) * float(dominant_volume))
    price_progress = (
        None
        if resolved_min is None or resolved_max is None
        else max(float(resolved_max) - float(resolved_min), 0.0)
    )
    if dominant_side_efficiency is None and price_progress is not None and dominant_volume > 0:
        dominant_side_efficiency = price_progress / dominant_volume
    return BinMarketData(
        symbol="BTCUSD",
        timeframe_name=timeframe_name,
        candle_open_time_utc_ms=0,
        candle_close_time_utc_ms=60_000,
        bin_index=index,
        price_low=float(index),
        price_high=float(index + 1),
        price_progress=float(price_progress or 0.0),
        total_volume=buy + sell,
        delta_volume=delta,
        time_in_bin_ms=time_ms,
        horizontal_delta=delta,
        ask_traded_volume=buy,
        bid_traded_volume=sell,
        buy_diagonal_imbalance_ratio=buy_diag,
        sell_diagonal_imbalance_ratio=sell_diag,
        min_trade_price_in_bin=resolved_min,
        max_trade_price_in_bin=resolved_max,
        price_progress_in_bin=price_progress,
        dominant_diagonal_side=dominant_side,
        dominant_side_volume=dominant_volume,
        dominant_side_efficiency=dominant_side_efficiency,
    )


def dominance_context_bins(side: str, start_index: int = 200) -> tuple[BinMarketData, ...]:
    normalized_side = side.strip().upper()
    efficiencies = (0.010, 0.015, 0.020, 0.025)
    if normalized_side == "BUY":
        return tuple(
            market_bin(
                start_index + offset,
                buy=10,
                sell=10,
                buy_diag=2,
                sell_diag=1,
                dominant_side_efficiency=efficiency,
            )
            for offset, efficiency in enumerate(efficiencies)
        )
    if normalized_side == "SELL":
        return tuple(
            market_bin(
                start_index + offset,
                buy=10,
                sell=10,
                buy_diag=1,
                sell_diag=2,
                dominant_side_efficiency=efficiency,
            )
            for offset, efficiency in enumerate(efficiencies)
        )
    return tuple()


def high_dominance_bin(side: str, index: int, efficiency: float = 0.20) -> BinMarketData:
    normalized_side = side.strip().upper()
    if normalized_side == "BUY":
        return market_bin(
            index,
            buy=50,
            sell=50,
            buy_diag=100,
            sell_diag=1,
            dominant_side_efficiency=efficiency,
        )
    if normalized_side == "SELL":
        return market_bin(
            index,
            buy=50,
            sell=50,
            buy_diag=1,
            sell_diag=100,
            dominant_side_efficiency=efficiency,
        )
    raise ValueError(f"unsupported dominance side: {side}")


def test_single_time_spike_requires_two_sided_volume_and_diagonal_pressure() -> None:
    results = build_candle_absorption_results(
        (
            market_bin(0),
            market_bin(1, time_ms=20_000, buy=30, sell=28, buy_diag=30, sell_diag=29, delta=8),
            market_bin(2),
            market_bin(3),
        ),
        config(),
    )

    assert len(results) == 1
    result = results[0]
    assert result.detected is True
    assert result.candidate_type == AbsorptionCandidateType.SINGLE_TIME_SPIKE
    assert result.setup_side == TradeSide.SELL
    assert result.zone_low == 1.0
    assert result.zone_high == 2.0


def test_adjacent_time_cluster_aggregates_time_and_volume_but_checks_diagonal_per_bin() -> None:
    results = build_candle_absorption_results(
        (
            market_bin(0),
            market_bin(1, time_ms=12_000, buy=16, sell=16, buy_diag=16, sell_diag=15.5, delta=-3),
            market_bin(2, time_ms=12_000, buy=16, sell=16, buy_diag=16, sell_diag=15.5, delta=-4),
            market_bin(3),
        ),
        config(),
    )

    assert len(results) == 1
    result = results[0]
    assert result.candidate_type == AbsorptionCandidateType.ADJACENT_TIME_CLUSTER
    assert result.setup_side == TradeSide.BUY
    assert result.buy_volume == 32
    assert result.sell_volume == 32
    assert result.zone_low == 1.0
    assert result.zone_high == 3.0


def test_candidate_bins_are_excluded_from_reference_median() -> None:
    results = build_candle_absorption_results(
        (
            market_bin(0, buy=20, sell=20, buy_diag=20, sell_diag=20),
            market_bin(1, time_ms=20_000, buy=60, sell=60, buy_diag=60, sell_diag=60, delta=-8),
        ),
        config(),
    )

    assert len(results) == 1
    assert results[0].setup_side == TradeSide.BUY


def setup_runtime() -> LiveAbsorptionRuntime:
    runtime = LiveAbsorptionRuntime()
    runtime._active_mt5_symbols = {"BTCUSD"}
    runtime._active_internal_timeframes_by_symbol = {"BTCUSD": {"M5"}}
    runtime._active_output_timeframes_by_symbol = {"BTCUSD": {"M5"}}
    runtime._mt5_timeframe_by_builder_key = {("BTCUSD", "M5"): "M5"}
    return runtime


def entry_trigger(
    *,
    side: str,
    position_id: str,
    absorption_time: int,
    dominance_time: int,
    stop_reference_price: str = "95",
    trigger_bin_price: str = "101",
) -> EntryTrigger:
    return EntryTrigger(
        symbol="BTCUSD",
        timeframe="M5",
        side=side,
        position_id=position_id,
        signal_time=dominance_time,
        stop_reference_price=Decimal(stop_reference_price),
        absorption_candle_time_utc_ms=absorption_time,
        dominance_candle_time_utc_ms=dominance_time,
        trigger_bin_price=Decimal(trigger_bin_price),
        entry_reason=(
            "DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION"
            if side == "BUY"
            else "DOMINANCE_CONFIRMED_AFTER_BUY_ABSORPTION"
        ),
    )


def entry_state(
    *,
    side: str,
    absorption_time: int,
    started_count: int = 1,
    expires_count: int = 3,
    stop_reference_price: str = "95",
) -> EntryState:
    return EntryState(
        symbol="BTCUSD",
        timeframe="M5",
        side=side,
        absorption_candle_time_utc_ms=absorption_time,
        started_closed_candle_count=started_count,
        expires_closed_candle_count=expires_count,
        stop_reference_price=Decimal(stop_reference_price),
    )


def candle(
    *,
    open_time: int,
    close_time: int,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
) -> CandleRecord:
    return CandleRecord(
        open_time_ms=open_time,
        close_time_ms=close_time,
        open_price=Decimal(open_price),
        high_price=Decimal(high_price),
        low_price=Decimal(low_price),
        close_price=Decimal(close_price),
        l2_bins={},
        durations_ms_by_index={},
        closed=True,
    )


<<<<<<< HEAD
def rule_candle(
    *,
    open_time: int,
    close_time: int,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
    buy: str,
    sell: str,
) -> CandleRecord:
    buy_volume = Decimal(buy)
    sell_volume = Decimal(sell)
    delta = buy_volume - sell_volume
    return CandleRecord(
        open_time_ms=open_time,
        close_time_ms=close_time,
        open_price=Decimal(open_price),
        high_price=Decimal(high_price),
        low_price=Decimal(low_price),
        close_price=Decimal(close_price),
        l2_bins={
            0: FrozenL2BinState(
                total_volume=buy_volume + sell_volume,
                delta=delta,
                horizontal_delta=delta,
                ask_traded_volume=buy_volume,
                bid_traded_volume=sell_volume,
                buy_diagonal_imbalance_ratio=Decimal("0"),
                sell_diagonal_imbalance_ratio=Decimal("0"),
                duration_ms=0,
            )
        },
        durations_ms_by_index={},
        closed=True,
    )


def rule_bin(
    index: int,
    *,
    buy: float,
    sell: float,
    buy_diag: float,
    sell_diag: float,
    efficiency_percentile: float,
) -> BinMarketData:
    return replace(
        market_bin(
            index,
            buy=buy,
            sell=sell,
            buy_diag=buy_diag,
            sell_diag=sell_diag,
            dominant_side_efficiency=0.20,
        ),
        volume_percentile=1.0,
        is_volume_valid=True,
        efficiency_percentile=efficiency_percentile,
    )


def test_reference_rule_buy_entry_uses_ui_rounded_volume_efficiency_percentile_and_action() -> None:
    runtime = setup_runtime()
    records = tuple(
        rule_candle(
            open_time=index * 60_000,
            close_time=(index + 1) * 60_000,
            open_price="104",
            high_price="106",
            low_price=str(105 - index),
            close_price="105",
            buy="1",
            sell="1",
        )
        for index in range(5)
    ) + (
        rule_candle(
            open_time=300_000,
            close_time=360_000,
            open_price="102",
            high_price="104",
            low_price="97",
            close_price="100",
            buy="1",
            sell="2",
        ),
        rule_candle(
            open_time=360_000,
            close_time=420_000,
            open_price="100",
            high_price="103",
            low_price="98",
            close_price="102",
            buy="1.50",
            sell="0.49",
        ),
    )

    trigger = runtime._detect_reference_entry_trigger(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        side="BUY",
        records=records,
        bin_items=(
            rule_bin(98, buy=1.50, sell=0.49, buy_diag=101.0, sell_diag=0.5, efficiency_percentile=0.2),
        ),
    )

    assert trigger is not None
    assert trigger.action == "ENTRY_BUY"
    assert trigger.position_id == "ENTRY-BTCUSD-M5-BUY-420000-420000"
    assert trigger.stop_reference_price == Decimal("97")
    assert trigger.target_entry_open_time_utc_ms == 420_000

    runtime._entry_triggers_by_key[(trigger.symbol, trigger.timeframe, trigger.side)] = trigger
    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert payloads[0]["command_type"] == "OPEN"
    assert payloads[0]["action"] == "ENTRY_BUY"
    assert payloads[0]["request_id"] == "ENTRY-BTCUSD-M5-BUY-420000-420000"
    assert "position_id" not in payloads[0]


def test_reference_rule_rejects_imbalance_when_efficiency_percentile_is_not_above_threshold() -> None:
    runtime = setup_runtime()
    records = tuple(
        rule_candle(
            open_time=index * 60_000,
            close_time=(index + 1) * 60_000,
            open_price="104",
            high_price="106",
            low_price=str(105 - index),
            close_price="105",
            buy="1",
            sell="1",
        )
        for index in range(5)
    ) + (
        rule_candle(
            open_time=300_000,
            close_time=360_000,
            open_price="102",
            high_price="104",
            low_price="97",
            close_price="100",
            buy="1",
            sell="2",
        ),
        rule_candle(
            open_time=360_000,
            close_time=420_000,
            open_price="100",
            high_price="103",
            low_price="98",
            close_price="102",
            buy="2",
            sell="0",
        ),
    )

    trigger = runtime._detect_reference_entry_trigger(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        side="BUY",
        records=records,
        bin_items=(
            rule_bin(98, buy=2, sell=0, buy_diag=101.0, sell_diag=0.5, efficiency_percentile=0.1),
        ),
    )

    assert trigger is None


def test_reference_rule_buy_entry_mode_b_uses_selected_low_stop_loss() -> None:
    runtime = setup_runtime()
    records = tuple(
        rule_candle(
            open_time=index * 60_000,
            close_time=(index + 1) * 60_000,
            open_price="100",
            high_price="103",
            low_price=str(100 - index),
            close_price="101",
            buy="2",
            sell="1",
        )
        for index in range(5)
    ) + (
        rule_candle(
            open_time=300_000,
            close_time=360_000,
            open_price="100",
            high_price="102",
            low_price="94",
            close_price="101",
            buy="2",
            sell="1",
        ),
        rule_candle(
            open_time=360_000,
            close_time=420_000,
            open_price="101",
            high_price="104",
            low_price="95",
            close_price="103",
            buy="3",
            sell="1",
        ),
    )

    trigger = runtime._detect_reference_entry_trigger(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        side="BUY",
        records=records,
        bin_items=(
            rule_bin(95, buy=3, sell=0, buy_diag=101.0, sell_diag=0.5, efficiency_percentile=0.2),
        ),
    )

    assert trigger is not None
    assert trigger.stop_reference_price == Decimal("94")


def test_reference_rule_sell_entry_mode_b_uses_selected_high_stop_loss() -> None:
    runtime = setup_runtime()
    records = tuple(
        rule_candle(
            open_time=index * 60_000,
            close_time=(index + 1) * 60_000,
            open_price="100",
            high_price=str(100 + index),
            low_price="95",
            close_price="99",
            buy="1",
            sell="2",
        )
        for index in range(5)
    ) + (
        rule_candle(
            open_time=300_000,
            close_time=360_000,
            open_price="105",
            high_price="106",
            low_price="100",
            close_price="103",
            buy="1",
            sell="3",
        ),
        rule_candle(
            open_time=360_000,
            close_time=420_000,
            open_price="103",
            high_price="105",
            low_price="99",
            close_price="101",
            buy="0",
            sell="2",
        ),
    )

    trigger = runtime._detect_reference_entry_trigger(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        side="SELL",
        records=records,
        bin_items=(
            rule_bin(105, buy=0, sell=2, buy_diag=0.5, sell_diag=101.0, efficiency_percentile=0.2),
        ),
    )

    assert trigger is not None
    assert trigger.stop_reference_price == Decimal("106")


def test_reference_rule_buy_exit_keeps_profit_gate_and_emits_exit_action() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", profit=Decimal("1"), position_id="ENTRY-buy-rule")
    records = (
        rule_candle(
            open_time=0,
            close_time=60_000,
            open_price="100",
            high_price="104",
            low_price="99",
            close_price="102",
            buy="2",
            sell="1",
        ),
        rule_candle(
            open_time=60_000,
            close_time=120_000,
            open_price="101",
            high_price="105",
            low_price="100",
            close_price="103",
            buy="2",
            sell="1",
        ),
        rule_candle(
            open_time=120_000,
            close_time=180_000,
            open_price="103",
            high_price="104",
            low_price="99",
            close_price="101",
            buy="0",
            sell="2",
        ),
    )

    created = runtime._evaluate_reference_exit_triggers(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        records=records,
        bin_items=(
            rule_bin(104, buy=0, sell=2, buy_diag=0.5, sell_diag=101.0, efficiency_percentile=0.2),
        ),
    )

    assert created is True
    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert payloads[0]["command_type"] == "CLOSE"
    assert payloads[0]["action"] == "EXIT_BUY_POSITION"
    assert payloads[0]["position_id"] == "ENTRY-buy-rule"

    loss_runtime = setup_runtime()
    add_open_position(loss_runtime, side="BUY", profit=Decimal("-1"), position_id="ENTRY-buy-loss")
    created = loss_runtime._evaluate_reference_exit_triggers(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        records=records,
        bin_items=(
            rule_bin(104, buy=0, sell=2, buy_diag=0.5, sell_diag=101.0, efficiency_percentile=0.2),
        ),
    )
    assert created is False


def test_reference_rule_sell_entry_and_sell_exit_emit_approved_actions() -> None:
    runtime = setup_runtime()
    entry_records = tuple(
        rule_candle(
            open_time=index * 60_000,
            close_time=(index + 1) * 60_000,
            open_price="99",
            high_price=str(100 + index),
            low_price="95",
            close_price="100",
            buy="1",
            sell="1",
        )
        for index in range(5)
    ) + (
        rule_candle(
            open_time=300_000,
            close_time=360_000,
            open_price="100",
            high_price="105",
            low_price="99",
            close_price="102",
            buy="2",
            sell="1",
        ),
        rule_candle(
            open_time=360_000,
            close_time=420_000,
            open_price="102",
            high_price="104",
            low_price="99",
            close_price="100",
            buy="0",
            sell="2",
        ),
    )

    trigger = runtime._detect_reference_entry_trigger(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        side="SELL",
        records=entry_records,
        bin_items=(
            rule_bin(104, buy=0, sell=2, buy_diag=0.5, sell_diag=101.0, efficiency_percentile=0.2),
        ),
    )

    assert trigger is not None
    assert trigger.action == "ENTRY_SELL"
    assert trigger.stop_reference_price == Decimal("105")

    add_open_position(runtime, side="SELL", profit=Decimal("1"), position_id="ENTRY-sell-rule")
    exit_records = (
        rule_candle(
            open_time=0,
            close_time=60_000,
            open_price="103",
            high_price="104",
            low_price="99",
            close_price="101",
            buy="1",
            sell="2",
        ),
        rule_candle(
            open_time=60_000,
            close_time=120_000,
            open_price="101",
            high_price="103",
            low_price="98",
            close_price="99",
            buy="1",
            sell="2",
        ),
        rule_candle(
            open_time=120_000,
            close_time=180_000,
            open_price="99",
            high_price="104",
            low_price="99",
            close_price="102",
            buy="2",
            sell="0",
        ),
    )

    created = runtime._evaluate_reference_exit_triggers(
        mt5_symbol="BTCUSD",
        output_timeframe="M5",
        records=exit_records,
        bin_items=(
            rule_bin(99, buy=2, sell=0, buy_diag=101.0, sell_diag=0.5, efficiency_percentile=0.2),
        ),
    )

    assert created is True
    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert payloads[0]["command_type"] == "CLOSE"
    assert payloads[0]["action"] == "EXIT_SELL_POSITION"


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def test_state_machine_builds_buy_open_after_sell_absorption_and_buy_dominance() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(96, sell_diag=20, buy_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="100",
            high_price="106",
            low_price="94",
            close_price="105",
        ),
        (
            market_bin(101, buy_diag=100, sell_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.20),
            *dominance_context_bins("BUY", 102),
        ),
        2,
    )

    trigger = runtime._entry_triggers_by_key[("BTCUSD", "M5", "BUY")]
    assert trigger.stop_reference_price == Decimal("94")
<<<<<<< HEAD
    assert trigger.trigger_bin_price == Decimal("101.0")
=======
    assert trigger.trigger_bin_price == Decimal("101.5")
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    assert trigger.entry_reason == "DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION"
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_states_by_key

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "OPEN"
    assert payloads[0]["side"] == "BUY"
    assert payloads[0]["request_id"].startswith("ENTRY-BTCUSD-M5-BUY-")
    assert "position_id" not in payloads[0]
    assert payloads[0]["stop_reference_price"] == 94.0
    assert "entry_price" not in payloads[0]
    assert "final_stop_loss" not in payloads[0]


<<<<<<< HEAD
def test_buy_dominance_confirmation_accepts_body_bins() -> None:
=======
def test_buy_dominance_confirmation_accepts_upper_wick_bins() -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(96, sell_diag=20, buy_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="100",
            high_price="106",
            low_price="94",
            close_price="105",
        ),
        (
<<<<<<< HEAD
            high_dominance_bin("BUY", 101),
=======
            high_dominance_bin("BUY", 105),
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            *dominance_context_bins("BUY", 101),
        ),
        2,
    )

    trigger = runtime._entry_triggers_by_key[("BTCUSD", "M5", "BUY")]
<<<<<<< HEAD
    assert trigger.trigger_bin_price == Decimal("101.0")
    assert trigger.entry_reason == "DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION"


def test_buy_dominance_confirmation_rejects_lower_wick_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(96, sell_diag=20, buy_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="100",
            high_price="106",
            low_price="94",
            close_price="105",
        ),
        (
            high_dominance_bin("BUY", 95),
            *dominance_context_bins("BUY", 101),
        ),
        2,
    )

    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key
    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key


def test_buy_dominance_confirmation_rejects_upper_wick_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(96, sell_diag=20, buy_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="100",
            high_price="106",
            low_price="94",
            close_price="105",
        ),
        (
            high_dominance_bin("BUY", 106),
            *dominance_context_bins("BUY", 101),
        ),
        2,
    )

    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key
    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key


=======
    assert trigger.trigger_bin_price == Decimal("105.5")
    assert trigger.entry_reason == "DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION"


>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def test_absorption_seed_uses_low_efficiency_instead_of_duration() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(
                96,
                sell_diag=20,
                buy_diag=1,
                buy=50,
                sell=50,
                time_ms=1,
                dominant_side_efficiency=0.001,
            ),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key


def test_absorption_seed_rejects_bins_without_valid_efficiency_inputs() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(
                96,
                sell_diag=20,
                buy_diag=1,
                buy=50,
                sell=50,
                time_ms=30_000,
                dominant_side_efficiency=None,
                has_trade_prices=False,
            ),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key


def test_buy_absorption_seed_accepts_lower_wick_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(96, sell_diag=20, buy_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key


<<<<<<< HEAD
def test_buy_absorption_seed_accepts_lower_body_third_bins() -> None:
=======
def test_buy_absorption_seed_accepts_body_bins() -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(100, sell_diag=20, buy_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key


<<<<<<< HEAD
def test_buy_absorption_seed_rejects_middle_body_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(102, sell_diag=20, buy_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def test_buy_absorption_seed_rejects_upper_wick_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(104, sell_diag=20, buy_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key


def test_sell_absorption_seed_accepts_upper_wick_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(104, buy_diag=20, sell_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 98),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "SELL") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key


<<<<<<< HEAD
def test_sell_absorption_seed_accepts_upper_body_third_bins() -> None:
=======
def test_sell_absorption_seed_accepts_body_bins() -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(99, buy_diag=20, sell_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "SELL") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key


<<<<<<< HEAD
def test_sell_absorption_seed_rejects_middle_body_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(98, buy_diag=20, sell_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 101),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def test_sell_absorption_seed_rejects_lower_wick_bins() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(96, buy_diag=20, sell_diag=1, buy=50, sell=50, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 99),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key


def test_state_machine_builds_sell_open_after_buy_absorption_and_sell_dominance() -> None:
    runtime = setup_runtime()

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(104, buy_diag=20, sell_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 98),
        ),
        1,
    )

    assert ("BTCUSD", "M5", "SELL") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="103",
            high_price="106",
            low_price="98",
            close_price="99",
        ),
        (
<<<<<<< HEAD
            market_bin(102, buy_diag=1, sell_diag=100, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.20),
=======
            market_bin(101, buy_diag=1, sell_diag=100, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.20),
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            *dominance_context_bins("SELL", 102),
        ),
        2,
    )

    trigger = runtime._entry_triggers_by_key[("BTCUSD", "M5", "SELL")]
    assert trigger.stop_reference_price == Decimal("106")
<<<<<<< HEAD
    assert trigger.trigger_bin_price == Decimal("102.0")
=======
    assert trigger.trigger_bin_price == Decimal("101.5")
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    assert trigger.entry_reason == "DOMINANCE_CONFIRMED_AFTER_BUY_ABSORPTION"
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_states_by_key


<<<<<<< HEAD
def test_sell_dominance_confirmation_accepts_body_bins() -> None:
    runtime = setup_runtime()
    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(104, buy_diag=20, sell_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 98),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="103",
            high_price="106",
            low_price="98",
            close_price="99",
        ),
        (
            high_dominance_bin("SELL", 102),
            *dominance_context_bins("SELL", 99),
        ),
        2,
    )

    trigger = runtime._entry_triggers_by_key[("BTCUSD", "M5", "SELL")]
    assert trigger.trigger_bin_price == Decimal("102.0")
    assert trigger.entry_reason == "DOMINANCE_CONFIRMED_AFTER_BUY_ABSORPTION"


def test_sell_dominance_confirmation_rejects_upper_wick_bins() -> None:
    runtime = setup_runtime()
    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(104, buy_diag=20, sell_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 98),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="103",
            high_price="106",
            low_price="98",
            close_price="99",
        ),
        (
            high_dominance_bin("SELL", 105),
            *dominance_context_bins("SELL", 99),
        ),
        2,
    )

    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key
    assert ("BTCUSD", "M5", "SELL") in runtime._entry_states_by_key


def test_sell_dominance_confirmation_rejects_lower_wick_bins() -> None:
=======
def test_sell_dominance_confirmation_accepts_lower_wick_bins() -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    runtime = setup_runtime()
    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="105",
            low_price="96",
            close_price="97",
        ),
        (
            market_bin(104, buy_diag=20, sell_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("SELL", 98),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="103",
            high_price="106",
            low_price="98",
            close_price="99",
        ),
        (
            high_dominance_bin("SELL", 98),
            *dominance_context_bins("SELL", 99),
        ),
        2,
    )

<<<<<<< HEAD
    assert ("BTCUSD", "M5", "SELL") not in runtime._entry_triggers_by_key
    assert ("BTCUSD", "M5", "SELL") in runtime._entry_states_by_key
=======
    trigger = runtime._entry_triggers_by_key[("BTCUSD", "M5", "SELL")]
    assert trigger.trigger_bin_price == Decimal("98.5")
    assert trigger.entry_reason == "DOMINANCE_CONFIRMED_AFTER_BUY_ABSORPTION"
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


def test_entry_state_times_out_without_open() -> None:
    runtime = setup_runtime()
    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=60_000,
            close_time=120_000,
            open_price="100",
            high_price="104",
            low_price="95",
            close_price="103",
        ),
        (
            market_bin(96, sell_diag=20, buy_diag=1, buy=50, sell=50, time_ms=10_000, dominant_side_efficiency=0.001),
            *dominance_context_bins("BUY", 101),
        ),
        1,
    )

    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=120_000,
            close_time=180_000,
            open_price="100",
            high_price="101",
            low_price="99",
            close_price="100",
        ),
        (
            market_bin(100, buy_diag=1, sell_diag=1, buy=5, sell=5, time_ms=1_000),
        ),
        2,
    )

    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key
    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="100",
            high_price="101",
            low_price="99",
            close_price="100",
        ),
        (
            market_bin(100, buy_diag=1, sell_diag=1, buy=5, sell=5, time_ms=1_000),
        ),
        3,
    )

<<<<<<< HEAD
    assert ("BTCUSD", "M5", "BUY") in runtime._entry_states_by_key
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_triggers_by_key
    runtime._evaluate_entry_state_machine(
        "BTCUSD",
        "M5",
        candle(
            open_time=240_000,
            close_time=300_000,
            open_price="100",
            high_price="101",
            low_price="99",
            close_price="100",
        ),
        (
            market_bin(100, buy_diag=1, sell_diag=1, buy=5, sell=5, time_ms=1_000),
        ),
        4,
    )

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    assert ("BTCUSD", "M5", "BUY") not in runtime._entry_states_by_key
    assert not runtime._entry_triggers_by_key


def add_open_position(
    runtime: LiveAbsorptionRuntime,
    *,
    side: str,
    profit: Decimal = Decimal("1"),
    position_id: str = "ENTRY-open",
    client_position_id: str | None = None,
    request_id: str = "",
    client_position_identifier: str = "",
) -> None:
    resolved_client_position_id = client_position_id or f"ticket-{position_id}"
    runtime._open_execution_positions[("metatrader", resolved_client_position_id)] = OpenPositionState(
        client_name="metatrader",
        position_id=position_id,
        client_position_id=resolved_client_position_id,
        symbol="BTCUSD",
        timeframe="M5",
        side=side,
        request_id=request_id,
        client_position_identifier=client_position_identifier,
        profit=profit,
    )


def test_position_opened_clears_entry_context_and_pending_open_command() -> None:
    runtime = setup_runtime()
    trigger = entry_trigger(
        side="BUY",
        position_id="ENTRY-open-clear",
        absorption_time=60_000,
        dominance_time=120_000,
    )
    context_key = ("BTCUSD", "M5", "BUY")
    runtime._entry_states_by_key[context_key] = entry_state(side="BUY", absorption_time=60_000)
    runtime._entry_triggers_by_key[context_key] = trigger

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "OPEN"
    assert payloads[0]["request_id"] == "ENTRY-open-clear"
    assert runtime._pending_execution_commands

    result = runtime.update_execution_position_status(
        {
            "client_name": "metatrader",
            "execution_request_id": "ENTRY-open-clear",
            "position_id": "MT5:1001",
            "client_position_id": "1001",
            "symbol_name": "BTCUSD",
            "timeframe": "M5",
            "side": "BUY",
            "status": "POSITION_OPENED",
            "signal_time": 120_000,
        }
    )

    assert result["lock_state"] == "OPEN"
    assert context_key not in runtime._entry_states_by_key
    assert context_key not in runtime._entry_triggers_by_key
    assert not runtime._pending_execution_commands


def test_position_close_removes_stale_entry_context_but_keeps_fresh_context() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="MT5:1002", client_position_id="1002")
    stale_key = ("BTCUSD", "M5", "BUY")
    fresh_key = ("BTCUSD", "M5", "SELL")
    runtime._entry_states_by_key[stale_key] = entry_state(side="BUY", absorption_time=800)
    runtime._entry_triggers_by_key[stale_key] = entry_trigger(
        side="BUY",
        position_id="ENTRY-stale-before-close",
        absorption_time=800,
        dominance_time=1_000,
    )
    runtime._entry_states_by_key[fresh_key] = entry_state(side="SELL", absorption_time=1_100)
    runtime._entry_triggers_by_key[fresh_key] = entry_trigger(
        side="SELL",
        position_id="ENTRY-fresh-after-close",
        absorption_time=1_100,
        dominance_time=1_200,
        stop_reference_price="105",
        trigger_bin_price="99",
    )

    result = runtime.update_execution_position_status(
        {
            "client_name": "metatrader",
            "position_id": "MT5:1002",
            "client_position_id": "1002",
            "symbol_name": "BTCUSD",
            "timeframe": "M5",
            "side": "BUY",
            "status": "POSITION_CLOSED_BY_STOP_LOSS",
            "signal_time": 1_000,
        }
    )

    assert result["lock_state"] == "CLEARED"
    assert stale_key not in runtime._entry_states_by_key
    assert stale_key not in runtime._entry_triggers_by_key
    assert fresh_key in runtime._entry_states_by_key
    assert fresh_key in runtime._entry_triggers_by_key

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "OPEN"
    assert payloads[0]["request_id"] == "ENTRY-fresh-after-close"


def test_profit_exit_closes_buy_on_sell_dominance_high_efficiency_bearish_candle() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="ENTRY-buy")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="103",
            high_price="105",
            low_price="98",
            close_price="101",
        ),
        (
            high_dominance_bin("SELL", 103),
            *dominance_context_bins("SELL", 99),
        ),
    )

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "CLOSE"
    assert payloads[0]["side"] == "BUY"
    assert payloads[0]["position_id"] == "ENTRY-buy"
    assert payloads[0]["exit_reason"] == "PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK"


def test_profit_exit_closes_sell_on_buy_dominance_high_efficiency_bullish_candle() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="SELL", position_id="ENTRY-sell")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="101",
            high_price="105",
            low_price="96",
            close_price="103",
        ),
        (
            high_dominance_bin("BUY", 98),
            *dominance_context_bins("BUY", 99),
        ),
    )

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "CLOSE"
    assert payloads[0]["side"] == "SELL"
    assert payloads[0]["position_id"] == "ENTRY-sell"
    assert payloads[0]["exit_reason"] == "PROFIT_EXIT_BUY_DOMINANCE_HIGH_EFFICIENCY_LOWER_WICK"


<<<<<<< HEAD
def test_profit_exit_closes_buy_on_sell_dominance_high_efficiency_top_body_third_bin() -> None:
=======
def test_profit_exit_closes_buy_on_sell_dominance_high_efficiency_body_bin() -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="ENTRY-buy-body")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="103",
            high_price="105",
            low_price="98",
            close_price="101",
        ),
        (
<<<<<<< HEAD
            high_dominance_bin("SELL", 103),
            *dominance_context_bins("SELL", 98),
=======
            high_dominance_bin("SELL", 101),
            *dominance_context_bins("SELL", 99),
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        ),
    )

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "CLOSE"
    assert payloads[0]["side"] == "BUY"
    assert payloads[0]["position_id"] == "ENTRY-buy-body"
    assert payloads[0]["exit_reason"] == "PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK"


<<<<<<< HEAD
def test_profit_exit_closes_sell_on_buy_dominance_high_efficiency_bottom_body_third_bin() -> None:
=======
def test_profit_exit_closes_sell_on_buy_dominance_high_efficiency_body_bin() -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    runtime = setup_runtime()
    add_open_position(runtime, side="SELL", position_id="ENTRY-sell-body")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="101",
            high_price="105",
            low_price="96",
            close_price="103",
        ),
        (
<<<<<<< HEAD
            high_dominance_bin("BUY", 101),
            *dominance_context_bins("BUY", 97),
=======
            high_dominance_bin("BUY", 102),
            *dominance_context_bins("BUY", 98),
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        ),
    )

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "CLOSE"
    assert payloads[0]["side"] == "SELL"
    assert payloads[0]["position_id"] == "ENTRY-sell-body"
    assert payloads[0]["exit_reason"] == "PROFIT_EXIT_BUY_DOMINANCE_HIGH_EFFICIENCY_LOWER_WICK"


<<<<<<< HEAD
def test_profit_exit_rejects_buy_when_sell_dominance_is_below_top_body_third() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="ENTRY-buy-lower-body")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="103",
            high_price="105",
            low_price="98",
            close_price="101",
        ),
        (
            high_dominance_bin("SELL", 101),
            *dominance_context_bins("SELL", 98),
        ),
    )

    assert not runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")


def test_profit_exit_rejects_sell_when_buy_dominance_is_above_bottom_body_third() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="SELL", position_id="ENTRY-sell-upper-body")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="101",
            high_price="105",
            low_price="96",
            close_price="103",
        ),
        (
            high_dominance_bin("BUY", 102),
            *dominance_context_bins("BUY", 97),
        ),
    )

    assert not runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def test_profit_exit_keeps_loss_position_for_stop_loss_only() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", profit=Decimal("-1"), position_id="ENTRY-loss")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="103",
            high_price="105",
            low_price="98",
            close_price="101",
        ),
        (
            high_dominance_bin("SELL", 103),
            *dominance_context_bins("SELL", 99),
        ),
    )

    assert not runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")


def test_profit_exit_requires_valid_efficiency_bearish_candle_and_expected_dominant_side() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="ENTRY-buy-invalid")

    invalid_cases = (
        (
            candle(
                open_time=180_000,
                close_time=240_000,
                open_price="103",
                high_price="105",
                low_price="98",
                close_price="101",
            ),
            (
                market_bin(
                    103,
                    buy=50,
                    sell=50,
                    buy_diag=1,
                    sell_diag=100,
                    dominant_side_efficiency=0.20,
                    has_trade_prices=False,
                ),
                *dominance_context_bins("SELL", 99),
            ),
        ),
        (
            candle(
                open_time=180_000,
                close_time=240_000,
                open_price="100",
                high_price="105",
                low_price="98",
                close_price="101",
            ),
            (
                high_dominance_bin("SELL", 103),
                *dominance_context_bins("SELL", 99),
            ),
        ),
        (
            candle(
                open_time=180_000,
                close_time=240_000,
                open_price="103",
                high_price="105",
                low_price="98",
                close_price="101",
            ),
            (
                high_dominance_bin("BUY", 103),
                *dominance_context_bins("BUY", 99),
            ),
        ),
    )
    for record_case, bins in invalid_cases:
        runtime._evaluate_profit_exit_triggers(
            "BTCUSD",
            "M5",
            record_case,
            bins,
        )

    assert not runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")


def test_profit_exit_rejects_low_zscore_opposite_dominance() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="ENTRY-buy-low-zscore")

    runtime._evaluate_profit_exit_triggers(
        "BTCUSD",
        "M5",
        candle(
            open_time=180_000,
            close_time=240_000,
            open_price="103",
            high_price="105",
            low_price="98",
            close_price="101",
        ),
        (
            high_dominance_bin("SELL", 103, efficiency=0.030),
            *dominance_context_bins("SELL", 99),
        ),
    )

    assert not runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")


def test_position_closed_by_signal_without_close_command_keeps_open_position() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="SELL", position_id="ENTRY-unmatched-close")

    result = runtime.update_execution_position_status(
        {
            "client_name": "metatrader",
            "position_id": "ENTRY-unmatched-close",
            "client_position_id": "ticket-ENTRY-unmatched-close",
            "symbol_name": "BTCUSD",
            "timeframe": "M5",
            "side": "SELL",
            "status": "POSITION_CLOSED_BY_SIGNAL",
        }
    )

    assert result["lock_state"] == "IGNORED_UNMATCHED_SIGNAL_CLOSE"
    assert ("metatrader", "ticket-ENTRY-unmatched-close") in runtime._open_execution_positions


def test_position_closed_by_stop_loss_clears_without_close_command() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="SELL", position_id="ENTRY-stop-loss")

    result = runtime.update_execution_position_status(
        {
            "client_name": "metatrader",
            "position_id": "ENTRY-stop-loss",
            "client_position_id": "ticket-ENTRY-stop-loss",
            "symbol_name": "BTCUSD",
            "timeframe": "M5",
            "side": "SELL",
            "status": "POSITION_CLOSED_BY_STOP_LOSS",
        }
    )

    assert result["lock_state"] == "CLEARED"
    assert not runtime._open_execution_positions


def test_position_closed_by_signal_after_close_command_clears_position() -> None:
    runtime = setup_runtime()
    add_open_position(runtime, side="BUY", position_id="ENTRY-matched-close")
    runtime._exit_triggers_by_key[("BTCUSD", "M5", "BUY")] = ExitTrigger(
        symbol="BTCUSD",
        timeframe="M5",
        side="BUY",
        position_id="ENTRY-matched-close",
        signal_time=240_000,
        trigger_bin_price=Decimal("103.5"),
        exit_reason="PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK",
    )

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "CLOSE"

    result = runtime.update_execution_position_status(
        {
            "client_name": "metatrader",
            "position_id": "ENTRY-matched-close",
            "client_position_id": "ticket-ENTRY-matched-close",
            "symbol_name": "BTCUSD",
            "timeframe": "M5",
            "side": "BUY",
            "status": "POSITION_CLOSED_BY_SIGNAL",
        }
    )

    assert result["lock_state"] == "CLEARED"
    assert not runtime._open_execution_positions


def test_rejected_close_still_open_preserves_entry_request_id() -> None:
    runtime = setup_runtime()
    add_open_position(
        runtime,
        side="BUY",
        position_id="MT5:12345",
        client_position_id="12345",
        request_id="ENTRY-original",
        client_position_identifier="777",
    )
    runtime._exit_triggers_by_key[("BTCUSD", "M5", "BUY")] = ExitTrigger(
        symbol="BTCUSD",
        timeframe="M5",
        side="BUY",
        position_id="MT5:12345",
        signal_time=240_000,
        trigger_bin_price=Decimal("103.5"),
        exit_reason="PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK",
    )

    payloads = runtime.execution_command_payloads("BTCUSD", client_name="metatrader", primary_timeframe="M5")
    assert len(payloads) == 1
    assert payloads[0]["command_type"] == "CLOSE"
    close_request_id = payloads[0]["request_id"]

    result = runtime.update_execution_position_status(
        {
            "client_name": "metatrader",
            "execution_request_id": close_request_id,
            "position_id": "MT5:12345",
            "client_position_id": "12345",
            "client_position_identifier": "777",
            "symbol_name": "BTCUSD",
            "timeframe": "M5",
            "side": "BUY",
            "status": "POSITION_STILL_OPEN",
            "rejection_reason": "CLOSE_SKIPPED_POSITION_NOT_PROFITABLE",
        }
    )

    assert result["lock_state"] == "OPEN"
    assert not runtime._pending_execution_commands
    assert runtime._open_execution_positions[("metatrader", "12345")].request_id == "ENTRY-original"


def test_decision_engine_allows_one_open_per_symbol_and_close_only_when_profitable() -> None:
    zone_store = ZoneStateStore()
    engine = TradingDecisionEngine(zone_store)
    buy_trigger = EntryTrigger(
        symbol="BTCUSD",
        timeframe="M5",
        side="BUY",
        position_id="ENTRY-buy",
        signal_time=100,
        stop_reference_price=Decimal("95"),
        absorption_candle_time_utc_ms=60,
        dominance_candle_time_utc_ms=100,
        trigger_bin_price=Decimal("101"),
        entry_reason="DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION",
    )
    sell_trigger = EntryTrigger(
        symbol="BTCUSD",
        timeframe="M5",
        side="SELL",
        position_id="ENTRY-sell",
        signal_time=110,
        stop_reference_price=Decimal("105"),
        absorption_candle_time_utc_ms=70,
        dominance_candle_time_utc_ms=110,
        trigger_bin_price=Decimal("99"),
        entry_reason="DOMINANCE_CONFIRMED_AFTER_BUY_ABSORPTION",
    )

    commands = engine.evaluate_symbol(
        client_name="metatrader",
        symbol="BTCUSD",
        entry_triggers=(buy_trigger, sell_trigger),
        open_positions=tuple(),
        primary_timeframe="M5",
    )

    assert len(commands) == 1
    assert commands[0].request_id == "ENTRY-sell"
    assert commands[0].position_id == ""

    zone_store.update(
        ZoneState(
            symbol="BTCUSD",
            timeframe="M5",
            zone_id="ENTRY-buy",
            side="BUY",
        )
    )
    losing_position = OpenPositionState(
        client_name="metatrader",
        position_id="ENTRY-sell",
        client_position_id="ticket-1",
        symbol="BTCUSD",
        timeframe="M5",
        side="SELL",
        profit=Decimal("-1"),
    )
    assert not engine.evaluate_symbol(
        client_name="metatrader",
        symbol="BTCUSD",
        entry_triggers=(buy_trigger,),
        open_positions=(losing_position,),
        primary_timeframe="M5",
    )

    profitable_position = OpenPositionState(
        client_name="metatrader",
        position_id="ENTRY-sell",
        client_position_id="ticket-1",
        symbol="BTCUSD",
        timeframe="M5",
        side="SELL",
        profit=Decimal("1"),
    )
    close_commands = engine.evaluate_symbol(
        client_name="metatrader",
        symbol="BTCUSD",
        entry_triggers=(buy_trigger,),
        open_positions=(profitable_position,),
        primary_timeframe="M5",
    )
    assert len(close_commands) == 1
    assert close_commands[0].command_type == "CLOSE"


def test_decision_engine_prefers_profit_exit_trigger_reason_for_profitable_close() -> None:
    zone_store = ZoneStateStore()
    engine = TradingDecisionEngine(zone_store)
    zone_store.update(
        ZoneState(
            symbol="BTCUSD",
            timeframe="M5",
            zone_id="opposite",
            side="SELL",
        )
    )
    position = OpenPositionState(
        client_name="metatrader",
        position_id="ENTRY-buy",
        client_position_id="ticket-1",
        symbol="BTCUSD",
        timeframe="M5",
        side="BUY",
        profit=Decimal("1"),
    )
    trigger = ExitTrigger(
        symbol="BTCUSD",
        timeframe="M5",
        side="BUY",
        position_id="ENTRY-buy",
        signal_time=240_000,
        trigger_bin_price=Decimal("103.5"),
        exit_reason="PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK",
    )

    commands = engine.evaluate_symbol(
        client_name="metatrader",
        symbol="BTCUSD",
        entry_triggers=tuple(),
        open_positions=(position,),
        exit_triggers=(trigger,),
        primary_timeframe="M5",
    )

    assert len(commands) == 1
    assert commands[0].command_type == "CLOSE"
    assert commands[0].exit_reason == "PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK"
    assert commands[0].trigger_bin_price == Decimal("103.5")


<<<<<<< HEAD
def test_decision_bins_store_contract_spike_score_without_changing_efficiency_formula() -> None:
    runtime = LiveAbsorptionRuntime()
    bins = (
        market_bin(0, buy=4, sell=4, buy_diag=2, sell_diag=1),
        market_bin(1, buy=5, sell=4, buy_diag=2, sell_diag=1),
        market_bin(2, buy=6, sell=4, buy_diag=2, sell_diag=1),
        market_bin(3, buy=6, sell=5, buy_diag=2, sell_diag=1),
        market_bin(
            4,
            buy=30,
            sell=10,
            buy_diag=8,
            sell_diag=1,
            min_trade_price=4.1,
            max_trade_price=4.5,
        ),
    )

    enriched = runtime._enrich_decision_bins(bins)
    spike_bin = enriched[-1]

    assert spike_bin.abnormal_contract is True
    assert abs(spike_bin.contract_spike_score - 20.57225) < 1e-9
    assert abs(float(spike_bin.dominant_side_efficiency or 0.0) - (0.4 / 40.0)) < 1e-12


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def load_tests(loader: unittest.TestLoader, tests: unittest.TestSuite, pattern: str | None) -> unittest.TestSuite:
    suite = unittest.TestSuite()
    for test_func in (
        test_single_time_spike_requires_two_sided_volume_and_diagonal_pressure,
        test_adjacent_time_cluster_aggregates_time_and_volume_but_checks_diagonal_per_bin,
        test_candidate_bins_are_excluded_from_reference_median,
<<<<<<< HEAD
        test_reference_rule_buy_entry_uses_ui_rounded_volume_efficiency_percentile_and_action,
        test_reference_rule_rejects_imbalance_when_efficiency_percentile_is_not_above_threshold,
        test_reference_rule_buy_entry_mode_b_uses_selected_low_stop_loss,
        test_reference_rule_sell_entry_mode_b_uses_selected_high_stop_loss,
        test_reference_rule_buy_exit_keeps_profit_gate_and_emits_exit_action,
        test_reference_rule_sell_entry_and_sell_exit_emit_approved_actions,
        test_state_machine_builds_buy_open_after_sell_absorption_and_buy_dominance,
        test_buy_dominance_confirmation_accepts_body_bins,
        test_buy_dominance_confirmation_rejects_lower_wick_bins,
        test_buy_dominance_confirmation_rejects_upper_wick_bins,
        test_absorption_seed_uses_low_efficiency_instead_of_duration,
        test_absorption_seed_rejects_bins_without_valid_efficiency_inputs,
        test_buy_absorption_seed_accepts_lower_wick_bins,
        test_buy_absorption_seed_accepts_lower_body_third_bins,
        test_buy_absorption_seed_rejects_middle_body_bins,
        test_buy_absorption_seed_rejects_upper_wick_bins,
        test_sell_absorption_seed_accepts_upper_wick_bins,
        test_sell_absorption_seed_accepts_upper_body_third_bins,
        test_sell_absorption_seed_rejects_middle_body_bins,
        test_sell_absorption_seed_rejects_lower_wick_bins,
        test_state_machine_builds_sell_open_after_buy_absorption_and_sell_dominance,
        test_sell_dominance_confirmation_accepts_body_bins,
        test_sell_dominance_confirmation_rejects_upper_wick_bins,
        test_sell_dominance_confirmation_rejects_lower_wick_bins,
=======
        test_state_machine_builds_buy_open_after_sell_absorption_and_buy_dominance,
        test_buy_dominance_confirmation_accepts_upper_wick_bins,
        test_absorption_seed_uses_low_efficiency_instead_of_duration,
        test_absorption_seed_rejects_bins_without_valid_efficiency_inputs,
        test_buy_absorption_seed_accepts_lower_wick_bins,
        test_buy_absorption_seed_accepts_body_bins,
        test_buy_absorption_seed_rejects_upper_wick_bins,
        test_sell_absorption_seed_accepts_upper_wick_bins,
        test_sell_absorption_seed_accepts_body_bins,
        test_sell_absorption_seed_rejects_lower_wick_bins,
        test_state_machine_builds_sell_open_after_buy_absorption_and_sell_dominance,
        test_sell_dominance_confirmation_accepts_lower_wick_bins,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        test_entry_state_times_out_without_open,
        test_position_opened_clears_entry_context_and_pending_open_command,
        test_position_close_removes_stale_entry_context_but_keeps_fresh_context,
        test_profit_exit_closes_buy_on_sell_dominance_high_efficiency_bearish_candle,
        test_profit_exit_closes_sell_on_buy_dominance_high_efficiency_bullish_candle,
<<<<<<< HEAD
        test_profit_exit_closes_buy_on_sell_dominance_high_efficiency_top_body_third_bin,
        test_profit_exit_closes_sell_on_buy_dominance_high_efficiency_bottom_body_third_bin,
        test_profit_exit_rejects_buy_when_sell_dominance_is_below_top_body_third,
        test_profit_exit_rejects_sell_when_buy_dominance_is_above_bottom_body_third,
=======
        test_profit_exit_closes_buy_on_sell_dominance_high_efficiency_body_bin,
        test_profit_exit_closes_sell_on_buy_dominance_high_efficiency_body_bin,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        test_profit_exit_keeps_loss_position_for_stop_loss_only,
        test_profit_exit_requires_valid_efficiency_bearish_candle_and_expected_dominant_side,
        test_profit_exit_rejects_low_zscore_opposite_dominance,
        test_position_closed_by_signal_without_close_command_keeps_open_position,
        test_position_closed_by_stop_loss_clears_without_close_command,
        test_position_closed_by_signal_after_close_command_clears_position,
        test_rejected_close_still_open_preserves_entry_request_id,
        test_decision_engine_allows_one_open_per_symbol_and_close_only_when_profitable,
        test_decision_engine_prefers_profit_exit_trigger_reason_for_profitable_close,
<<<<<<< HEAD
        test_decision_bins_store_contract_spike_score_without_changing_efficiency_formula,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    ):
        suite.addTest(unittest.FunctionTestCase(test_func))
    return suite
