from __future__ import annotations

import unittest
from decimal import Decimal
from typing import Any

from actor_proxy import ActorProxyConfig, ActorProxyEngine, ActorProxyReplayAdapter, RawDomEvent
from DOM.models import DomRawEvent
from process.dataProcessEngine import DataProcessEngine
from process.data_sources import InMemoryProcessEventSource
from process.models import (
    DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
    DATA_PROCESS_REFILL_OUTPUT_TYPE,
    DataProcessConfig,
    ProcessReplayRequest,
    ProcessSymbol,
)
from triggerEngine import TriggerEngine, TriggerSignal


def process_symbol() -> ProcessSymbol:
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


def dom_raw(
    ts_event_ms: int,
    *,
    action: str,
    size: int,
    order_id: str = "ORDER-1",
    price: str = "100.00",
    side: str = "BID",
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


def raw_event(
    ts_event_ms: int,
    *,
    action: str,
    size: int,
    order_id: str = "ORDER-1",
    price: str = "100.00",
    side: str = "BID",
) -> RawDomEvent:
    return RawDomEvent(
        ts_event_ms=ts_event_ms,
        symbol="NQ.FUT",
        instrument_id=42004058,
        order_id=order_id,
        side=side,
        price=Decimal(price),
        size=size,
        action=action,
        source="memory",
    )


def actor_payload(*orders: dict[str, Any]) -> dict[str, Any]:
    return {
        "payload_id": "AP-1",
        "source_payload_id": "DP-1",
        "symbol": "NQ",
        "provider_symbol": "NQ.FUT",
        "instrument_id": 42004058,
        "timeframe": "M1",
        "raw_data_only": True,
        "orders": list(orders),
    }


def actor_order(
    order_id: str,
    *,
    price: str = "100.00",
    initial_size: int = 100,
    current_size: int = 100,
    refill_count: int = 5,
    refill_contracts: int = 50,
) -> dict[str, Any]:
    return {
        "symbol": "NQ",
        "provider_symbol": "NQ.FUT",
        "instrument_id": 42004058,
        "order_id": order_id,
        "side": "BID",
        "price": price,
        "initial_size": initial_size,
        "current_size": current_size,
        "refill_count": refill_count,
        "refill_contracts": refill_contracts,
        "executed_contracts": refill_count,
        "trade_count": refill_count,
        "first_seen_ts_event_ms": 900,
        "last_seen_ts_event_ms": 1_000,
        "source_file": "memory",
        "reason": "HIGH_REFILL",
    }


def position_context(**overrides: Any) -> dict[str, Any]:
    payload = {
        "position_id": "POS-1",
        "side": "LONG",
        "entry_price": "100.25",
        "entry_time_ms": 1_000,
        "symbol": "NQ",
        "provider_symbol": "NQ.FUT",
        "timeframe": "M1",
        "tracking_start_ms": 1_000,
        "tracking_end_ms": 5_000,
        "tracking_mode": "REPLAY",
    }
    payload.update(overrides)
    return payload


class _FakeActorProxyEngine:
    def __init__(self) -> None:
        self.started: list[tuple[dict[str, Any], dict[str, Any]]] = []
        self.stopped: list[str] = []

    def start_tracking(self, actor_payload: dict[str, Any], position_context: dict[str, Any]) -> None:
        self.started.append((dict(actor_payload), dict(position_context)))

    def stop_tracking(self, position_id: str) -> None:
        self.stopped.append(position_id)


class ActorProxyEngineTests(unittest.TestCase):
    def test_multi_order_payload_prioritizes_larger_actor(self) -> None:
        engine = ActorProxyEngine()

        cluster = engine.start_tracking(
            actor_payload(
                actor_order("SMALL", initial_size=10, current_size=10, refill_count=1, refill_contracts=2),
                actor_order("BIG", initial_size=100, current_size=100, refill_count=8, refill_contracts=80),
            ),
            position_context(),
        )

        self.assertEqual(len(cluster.candidates), 2)
        self.assertEqual(cluster.candidates[0].original_order_id, "BIG")
        self.assertGreater(
            cluster.candidates[0].importance_score,
            cluster.candidates[1].importance_score,
        )

    def test_cancel_without_replacement_waits_for_confirm_window(self) -> None:
        engine = ActorProxyEngine(
            ActorProxyConfig(
                active_contract_ratio_exit_threshold=Decimal("0.80"),
                cluster_exit_score_threshold=Decimal("0.50"),
                confirm_window_ms=1_000,
            )
        )
        engine.start_tracking(actor_payload(actor_order("ORDER-1")), position_context())

        self.assertEqual(
            engine.on_raw_dom_event(raw_event(1_100, action="CANCEL", size=100)),
            tuple(),
        )
        self.assertIsNone(engine.update_cluster_state("POS-1", now_ms=2_099))
        signal = engine.update_cluster_state("POS-1", now_ms=2_100)

        self.assertIsNotNone(signal)
        self.assertEqual(signal.reason, "ACTOR_PROXY_EXIT")
        self.assertEqual(signal.dominant_actor_status, "CANCELLED")

    def test_raw_add_reactivates_candidate_when_initial_payload_was_closed(self) -> None:
        engine = ActorProxyEngine()
        cluster = engine.start_tracking(
            actor_payload(actor_order("ORDER-1", current_size=0)),
            position_context(),
        )

        engine.on_raw_dom_event(raw_event(1_001, action="ADD", size=100))

        self.assertEqual(cluster.candidates[0].status, "ACTIVE")
        self.assertEqual(cluster.candidates[0].current_size, 100)

    def test_cancel_with_valid_replacement_does_not_exit(self) -> None:
        engine = ActorProxyEngine(
            ActorProxyConfig(
                active_contract_ratio_exit_threshold=Decimal("0.80"),
                cluster_exit_score_threshold=Decimal("0.50"),
                confirm_window_ms=500,
                replacement_time_gap_ms=1_000,
                replacement_tick_distance=2,
                tick_size=Decimal("0.25"),
            )
        )
        cluster = engine.start_tracking(actor_payload(actor_order("ORDER-1")), position_context())

        engine.on_raw_dom_event(raw_event(1_100, action="CANCEL", size=100))
        engine.on_raw_dom_event(
            raw_event(1_200, action="ADD", size=90, order_id="ORDER-2", price="100.25")
        )

        self.assertIsNone(engine.update_cluster_state("POS-1", now_ms=3_000))
        self.assertEqual(cluster.candidates[0].status, "REPLACED")
        self.assertEqual(cluster.candidates[0].active_order_id, "ORDER-2")

    def test_severe_active_contract_ratio_drop_raises_exit_score(self) -> None:
        engine = ActorProxyEngine(
            ActorProxyConfig(
                active_contract_ratio_exit_threshold=Decimal("0.80"),
                cluster_exit_score_threshold=Decimal("0.95"),
            )
        )
        engine.start_tracking(
            actor_payload(
                actor_order("ORDER-1", initial_size=100, current_size=100),
                actor_order("ORDER-2", price="99.75", initial_size=100, current_size=100),
            ),
            position_context(),
        )

        engine.on_raw_dom_event(raw_event(1_100, action="CANCEL", size=100, order_id="ORDER-1"))
        metrics = engine.cluster_metrics("POS-1", now_ms=1_100)

        self.assertEqual(metrics.total_active_contracts, 100)
        self.assertLess(metrics.active_contract_ratio, Decimal("0.80"))
        self.assertGreater(metrics.cluster_exit_score, Decimal("0.50"))

    def test_replay_adapter_filters_exact_user_window_and_standardizes_events(self) -> None:
        adapter = ActorProxyReplayAdapter(
            raw_events=(
                raw_event(999, action="A", size=1),
                raw_event(1_000, action="A", size=2),
                raw_event(1_500, action="C", size=2),
                raw_event(2_001, action="A", size=3),
            )
        )

        events = adapter.iter_events(start_ms=1_000, end_ms=2_000, symbol="NQ.FUT")

        self.assertEqual([event.ts_event_ms for event in events], [1_000, 1_500])
        self.assertEqual(events[0].action, "ADD")
        self.assertEqual(events[1].action, "CANCEL")
        self.assertEqual(events[0].price, Decimal("100.00"))

    def test_data_process_payload_keeps_legacy_fields_and_adds_actor_payload(self) -> None:
        symbol = process_symbol()
        events = [
            dom_raw(1_000, action="A", size=2),
            dom_raw(1_100, action="F", size=1),
            dom_raw(1_101, action="M", size=2),
            dom_raw(1_200, action="F", size=1),
            dom_raw(1_201, action="M", size=2),
            dom_raw(1_300, action="C", size=2),
        ]
        engine = DataProcessEngine(
            event_source=InMemoryProcessEventSource({symbol: tuple(events)}),
            config=DataProcessConfig(
                emit_individual_refill_orders=True,
            ),
        )

        result = engine.run_replay(ProcessReplayRequest(start_ms=1_000, end_ms=1_300))

        self.assertEqual(result.emitted_payload_count, 3)
        refill_additions = [
            item for item in result.payloads
            if int(item.get("price_base_refill_count") or 0) > 0
        ]
        self.assertEqual(len(refill_additions), 2)
        payload = refill_additions[-1]
        self.assertEqual(payload["type"], DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION)
        self.assertEqual(payload["output_type"], DATA_PROCESS_REFILL_OUTPUT_TYPE)
        self.assertEqual(payload["order_id"], "")
        self.assertEqual(payload["level_event_order_id"], "ORDER-1")
        self.assertEqual(payload["refill_count"], 2)
        actor = payload["actor_proxy_payload"]
        self.assertEqual(actor["replay_window_policy"], "EXACT_USER_REQUESTED_RANGE")
        self.assertEqual(actor["tracking_start_ms"], 1_000)
        self.assertEqual(actor["tracking_end_ms"], 1_300)
        self.assertTrue(actor["orders"][0]["order_id"].startswith("PRICE_LEVEL|"))
        self.assertEqual(actor["orders"][0]["source_file"], "memory")

    def test_trigger_signal_keeps_legacy_payload_and_embeds_actor_proxy_context(self) -> None:
        proxy_payload = actor_payload(actor_order("ORDER-1"))
        signal = TriggerSignal(
            signal_id="TRG-1",
            signal_type="BUY_ENTRY",
            symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            trigger_candle_time_ms=1_000,
            trigger_candle_close_time_ms=59_999,
            action_candle_time_ms=60_000,
            direction="LONG",
            position_id="POS-1",
            reason="BUY_ENTRY_REFERENCE_ZONE_TOUCHED",
            wick="LOWER_WICK_OR_BODY_THIRD",
            marker_position="BELOW",
            marker_color="GREEN",
            marker_direction="UP",
            marker_shape="ARROW",
            reference_bin={"low": "99.75", "high": "100.00", "side": "SELL"},
            matched_bins=({"low": "99.75", "high": "100.00", "side": "SELL"},),
            entry_price=Decimal("100.25"),
            actor_proxy_payload=proxy_payload,
        )

        payload = signal.to_payload()

        self.assertEqual(payload["signal_type"], "BUY_ENTRY")
        self.assertEqual(payload["position_id"], "POS-1")
        self.assertEqual(payload["entry_price"], "100.25")
        self.assertEqual(payload["actor_proxy_payload"]["orders"][0]["order_id"], "ORDER-1")
        self.assertEqual(
            payload["actor_proxy_payload"]["position_context"]["position_id"],
            "POS-1",
        )

    def test_trigger_engine_starts_actor_proxy_with_entry_context(self) -> None:
        fake = _FakeActorProxyEngine()
        engine = TriggerEngine(actor_proxy_engine=fake)
        proxy_payload = actor_payload(actor_order("ORDER-1"))
        signal = TriggerSignal(
            signal_id="TRG-1",
            signal_type="BUY_ENTRY",
            symbol="NQ",
            provider_symbol="NQ.FUT",
            timeframe="M1",
            trigger_candle_time_ms=1_000,
            trigger_candle_close_time_ms=59_999,
            action_candle_time_ms=60_000,
            direction="LONG",
            position_id="POS-1",
            reason="BUY_ENTRY_REFERENCE_ZONE_TOUCHED",
            wick="LOWER_WICK_OR_BODY_THIRD",
            marker_position="BELOW",
            marker_color="GREEN",
            marker_direction="UP",
            marker_shape="ARROW",
            reference_bin={"low": "99.75", "high": "100.00", "side": "SELL"},
            matched_bins=({"low": "99.75", "high": "100.00", "side": "SELL"},),
            entry_price=Decimal("100.25"),
            actor_proxy_payload=proxy_payload,
        )

        engine._start_actor_proxy_tracking(signal)

        self.assertEqual(len(fake.started), 1)
        self.assertEqual(fake.started[0][1]["position_id"], "POS-1")
        self.assertEqual(fake.started[0][1]["entry_time_ms"], 60_000)
        self.assertEqual(fake.started[0][1]["mode"], "REPLAY")


if __name__ == "__main__":
    unittest.main()
