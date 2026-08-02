from __future__ import annotations

import csv
import json
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any, Protocol

from core.engine_output_bus import (
    DOM_ENGINE_PRODUCER,
    DOM_POSITIVE_REFILL_OUTPUT_TYPE,
    EngineOutputStore,
)
from process.models import (
    DATA_PROCESS_ENTRY_ACTION,
    DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
    DATA_PROCESS_PAYLOAD_TYPES,
    DATA_PROCESS_REFILL_OUTPUT_TYPE,
)
from triggerEngine import TriggerEngine


class ProcessPayloadSink(Protocol):
    def publish(self, payloads: Iterable[Mapping[str, Any]]) -> None:
        ...


class EngineOutputStoreSink:
    def __init__(
        self,
        output_store: EngineOutputStore,
        *,
        publish_native: bool = True,
        publish_dom_compatible: bool = True,
    ) -> None:
        self.output_store = output_store
        self.publish_native = bool(publish_native)
        self.publish_dom_compatible = bool(publish_dom_compatible)

    def publish(self, payloads: Iterable[Mapping[str, Any]]) -> None:
        outputs: list[dict[str, Any]] = []
        for payload in payloads:
            if self.publish_native:
                outputs.append(dict(payload))
            if self.publish_dom_compatible and _is_price_base_refill_payload(payload):
                outputs.append(dom_positive_refill_output(payload))
        if outputs:
            self.output_store.publish_many(outputs)


class TriggerEngineSink:
    def __init__(self, trigger_engine: TriggerEngine) -> None:
        self.trigger_engine = trigger_engine

    def publish(self, payloads: Iterable[Mapping[str, Any]]) -> None:
        for payload in payloads:
            if not _is_price_base_refill_payload(payload):
                continue
            dom_output = dom_positive_refill_output(payload)
            self.trigger_engine.set_dom_output_snapshot(
                {
                    "type": "DATA_PROCESS_DOM_OUTPUT",
                    "mt5_symbol": dom_output.get("mt5_symbol", ""),
                    "symbol": dom_output.get("provider_symbol", ""),
                    "provider_symbol": dom_output.get("provider_symbol", ""),
                    "timeframe": dom_output.get("timeframe", ""),
                    "timestamp_ms": dom_output.get("timestamp_ms", 0),
                    "canceled_zone_ids": dom_output.get("canceled_zone_ids", ()),
                    "engine_outputs": {
                        "dom_positive_refills": [dom_output],
                    },
                    "events": [dom_output],
                    "raw_events": [dom_output],
                    "order_book_levels": [
                        {
                            "price": dom_output.get("price", ""),
                            "bid_contracts": 1 if dom_output.get("side") == "BID" else 0,
                            "ask_contracts": 1 if dom_output.get("side") == "ASK" else 0,
                            "top_order_id": dom_output.get("order_id", ""),
                            "top_order_side": dom_output.get("side", ""),
                            "top_order_type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
                            "top_order_positive_refill_count": dom_output.get("positive_refill_count", 0),
                            "top_order_positive_refill_total": dom_output.get("positive_refill_total", 0),
                            "price_base_refill_count": dom_output.get("price_base_refill_count", 0),
                            "price_base_refill_contracts": dom_output.get("price_base_refill_contracts", 0),
                            "refill_method": "price_base_refill",
                            "top_order_positive_refill_filled_total": dom_output.get("positive_refill_filled_total", 0),
                        }
                    ],
                },
                symbol=str(dom_output.get("mt5_symbol") or ""),
                provider_symbol=str(dom_output.get("provider_symbol") or ""),
                timeframe=str(dom_output.get("timeframe") or ""),
                timestamp_ms=int(dom_output.get("timestamp_ms") or 0),
            )


class CsvProcessLogSink:
    fields = (
        "payload_id",
        "output_id",
        "output_type",
        "type",
        "payload_type",
        "action",
        "timestamp_ms",
        "threshold_time_ms",
        "close_time_ms",
        "provider_symbol",
        "mt5_symbol",
        "market_provider",
        "timeframe",
        "order_id",
        "price",
        "side",
        "refill_count",
        "refill_contracts",
        "refill_filled_contracts",
        "market_buy",
        "market_sell",
        "market_buy_contracts",
        "market_sell_contracts",
        "ask_traded_contracts",
        "bid_traded_contracts",
        "trade_count",
        "executed_contracts",
        "opening_liquidity",
        "available_liquidity",
        "gross_added_contracts",
        "non_refill_added_contracts",
        "refill_added_contracts",
        "withdrawn_contracts",
        "closing_liquidity",
        "level_execution_rate",
        "level_execution_rate_defined",
        "level_execution_invariant_ok",
        "added_breakdown_invariant_ok",
        "initial_order_size",
        "max_order_size",
        "footprint_open_time_ms",
        "footprint_bin_low",
        "footprint_bin_high",
        "zone_low",
        "zone_high",
        "zone_level_count",
        "zone_market_buy",
        "zone_market_sell",
        "close_reason",
        "payload_json",
    )

    trigger_fields = (
        "signal_id",
        "signal_type",
        "process_payload_id",
        "reference_payload_id",
        "symbol",
        "provider_symbol",
        "timeframe",
        "trigger_candle_time_ms",
        "payload_json",
    )

    def __init__(
        self,
        payload_path: Path,
        *,
        trigger_path: Path | None = None,
    ) -> None:
        self.payload_path = payload_path
        self.trigger_path = trigger_path

    def publish(self, payloads: Iterable[Mapping[str, Any]]) -> None:
        rows = []
        for payload in payloads:
            payload_id = str(payload.get("payload_id") or payload.get("output_id") or payload.get("id") or "")
            payload_type = _payload_type(payload)
            rows.append(
                {
                    "payload_id": payload_id,
                    "output_id": str(payload.get("output_id") or payload_id),
                    "output_type": str(payload.get("output_type") or DATA_PROCESS_REFILL_OUTPUT_TYPE),
                    "type": payload_type,
                    "payload_type": payload_type,
                    "action": str(payload.get("action") or DATA_PROCESS_ENTRY_ACTION),
                    "timestamp_ms": int(payload.get("timestamp_ms") or 0),
                    "threshold_time_ms": int(payload.get("threshold_time_ms") or 0),
                    "close_time_ms": int(payload.get("close_time_ms") or 0),
                    "provider_symbol": str(payload.get("provider_symbol") or ""),
                    "mt5_symbol": str(payload.get("mt5_symbol") or ""),
                    "market_provider": str(payload.get("market_provider") or ""),
                    "timeframe": str(payload.get("timeframe") or ""),
                    "order_id": str(payload.get("order_id") or ""),
                    "price": str(payload.get("price") or ""),
                    "side": str(payload.get("side") or ""),
                    "refill_count": int(payload.get("refill_count") or 0),
                    "refill_contracts": int(payload.get("refill_contracts") or 0),
                    "refill_filled_contracts": int(
                        payload.get("refill_filled_contracts")
                        or payload.get("positive_refill_filled_total")
                        or payload.get("executed_contracts")
                        or 0
                    ),
                    "market_buy": int(payload.get("market_buy") or 0),
                    "market_sell": int(payload.get("market_sell") or 0),
                    "market_buy_contracts": int(payload.get("market_buy_contracts") or 0),
                    "market_sell_contracts": int(payload.get("market_sell_contracts") or 0),
                    "ask_traded_contracts": int(payload.get("ask_traded_contracts") or 0),
                    "bid_traded_contracts": int(payload.get("bid_traded_contracts") or 0),
                    "trade_count": int(payload.get("trade_count") or 0),
                    "executed_contracts": int(payload.get("executed_contracts") or 0),
                    "opening_liquidity": int(payload.get("opening_liquidity") or 0),
                    "available_liquidity": int(payload.get("available_liquidity") or 0),
                    "gross_added_contracts": int(
                        payload.get("gross_added_contracts")
                        or payload.get("added_contracts")
                        or 0
                    ),
                    "non_refill_added_contracts": int(payload.get("non_refill_added_contracts") or 0),
                    "refill_added_contracts": int(
                        payload.get("refill_added_contracts")
                        or payload.get("price_base_refill_contracts")
                        or 0
                    ),
                    "withdrawn_contracts": int(
                        payload.get("withdrawn_contracts")
                        or payload.get("cancelled_or_withdrawn_contracts")
                        or 0
                    ),
                    "closing_liquidity": int(payload.get("closing_liquidity") or 0),
                    "level_execution_rate": payload.get("level_execution_rate"),
                    "level_execution_rate_defined": bool(payload.get("level_execution_rate_defined")),
                    "level_execution_invariant_ok": bool(payload.get("level_execution_invariant_ok", True)),
                    "added_breakdown_invariant_ok": bool(payload.get("added_breakdown_invariant_ok", True)),
                    "initial_order_size": int(payload.get("initial_order_size") or 0),
                    "max_order_size": int(payload.get("max_order_size") or 0),
                    "footprint_open_time_ms": int(payload.get("footprint_open_time_ms") or 0),
                    "footprint_bin_low": str(payload.get("footprint_bin_low") or ""),
                    "footprint_bin_high": str(payload.get("footprint_bin_high") or ""),
                    "zone_low": str(payload.get("zone_low") or ""),
                    "zone_high": str(payload.get("zone_high") or ""),
                    "zone_level_count": int(payload.get("zone_level_count") or 0),
                    "zone_market_buy": int(payload.get("zone_market_buy") or payload.get("market_buy") or 0),
                    "zone_market_sell": int(payload.get("zone_market_sell") or payload.get("market_sell") or 0),
                    "close_reason": str(payload.get("close_reason") or ""),
                    "payload_json": json.dumps(dict(payload), sort_keys=True, separators=(",", ":")),
                }
            )
        _append_rows(self.payload_path, self.fields, rows)

    def record_trigger_signals(self, signals: Iterable[Mapping[str, Any]]) -> None:
        if self.trigger_path is None:
            return
        rows = []
        for signal in signals:
            rows.append(
                {
                    "signal_id": str(signal.get("signal_id") or ""),
                    "signal_type": str(signal.get("signal_type") or ""),
                    "process_payload_id": str(signal.get("process_payload_id") or ""),
                    "reference_payload_id": str(signal.get("reference_payload_id") or ""),
                    "symbol": str(signal.get("symbol") or signal.get("mt5_symbol") or ""),
                    "provider_symbol": str(signal.get("provider_symbol") or ""),
                    "timeframe": str(signal.get("timeframe") or ""),
                    "trigger_candle_time_ms": int(signal.get("trigger_candle_time_ms") or 0),
                    "payload_json": json.dumps(dict(signal), sort_keys=True, separators=(",", ":")),
                }
            )
        _append_rows(self.trigger_path, self.trigger_fields, rows)


def dom_positive_refill_output(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not _is_price_base_refill_payload(payload):
        raise ValueError("price_base_refill fields are required")
    source_event_time_ms = _payload_int(payload, "timestamp_ms", "event_time_ms", "threshold_time_ms")
    timestamp_ms = _payload_int(payload, "marker_time_ms", "footprint_open_time_ms") or source_event_time_ms
    payload_id = str(payload.get("payload_id") or payload.get("output_id") or payload.get("id") or "")
    payload_type = _payload_type(payload)
    refill_total = _payload_int(payload, "price_base_refill_contracts")
    executed_refill = min(refill_total, _payload_int(payload, "executed_refill_contracts"))
    execution_rate = round(executed_refill / refill_total * 100.0 if refill_total > 0 else 0.0, 1)
    rate_label = f"{execution_rate:.1f}".rstrip("0").rstrip(".")
    refill_filled_total = _payload_int(
        payload,
        "refill_filled_contracts",
        "positive_refill_filled_total",
        "executed_contracts",
    )
    return {
        **dict(payload),
        "payload_id": payload_id,
        "id": payload_id,
        "output_id": payload_id,
        "producer": DOM_ENGINE_PRODUCER,
        "source_engine": "dataProcessEngine",
        "type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
        "output_type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
        "payload_type": payload_type,
        "process_output_type": str(payload.get("output_type") or DATA_PROCESS_REFILL_OUTPUT_TYPE),
        "action": str(payload.get("action") or DATA_PROCESS_ENTRY_ACTION),
        "timestamp_ms": timestamp_ms,
        "event_time_ms": timestamp_ms,
        "source_event_time_ms": source_event_time_ms,
        "marker_time_ms": timestamp_ms,
        "marker_price": str(payload.get("marker_price") or payload.get("footprint_bin_low") or payload.get("price") or ""),
        "positive_refill_count": _payload_int(
            payload,
            "price_base_refill_count",
        ),
        "positive_refill_total": refill_total,
        "price_base_refill_count": _payload_int(
            payload,
            "price_base_refill_count",
        ),
        "price_base_refill_contracts": refill_total,
        "refill_added_contracts": refill_total,
        "executed_refill_contracts": executed_refill,
        "withdrawn_refill_contracts": _payload_int(payload, "withdrawn_refill_contracts"),
        "refill_execution_rate": execution_rate,
        "refill_display": str(payload.get("refill_display") or (
            f"{_payload_int(payload, 'price_base_refill_count')}({refill_total}) "
            f"E{executed_refill} - {rate_label}%"
        )),
        "refill_method": "price_base_refill",
        "positive_refill_filled_total": refill_filled_total,
        "refill_total": refill_total,
        "refill_filled_contracts": refill_filled_total,
        "source": DATA_PROCESS_REFILL_OUTPUT_TYPE,
    }


def _is_price_base_refill_payload(
    payload: Mapping[str, Any],
) -> bool:
    refill_count = _payload_int(
        payload,
        "price_base_refill_count",
    )

    refill_contracts = _payload_int(
        payload,
        "price_base_refill_contracts",
    )

    return (
        refill_count > 0
        and refill_contracts > 0
        and str(
            payload.get("refill_method") or ""
        ).strip().lower() == "price_base_refill"
    )


def _payload_type(payload: Mapping[str, Any]) -> str:
    raw_value = str(payload.get("payload_type") or payload.get("type") or "").strip().upper()
    if raw_value in DATA_PROCESS_PAYLOAD_TYPES:
        return raw_value
    return DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION


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


def _append_rows(
    path: Path,
    fields: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size <= 0
    if not write_header:
        try:
            existing_header = path.read_text(encoding="utf-8").splitlines()[0].split(",")
            write_header = existing_header != list(fields)
        except (OSError, IndexError):
            write_header = True
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if write_header:
            writer.writeheader()
        writer.writerows(rows)
