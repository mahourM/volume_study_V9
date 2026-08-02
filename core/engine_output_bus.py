from __future__ import annotations

import threading
from collections import OrderedDict
from collections.abc import Iterable, Mapping
from typing import Any


DOM_ENGINE_PRODUCER = "dom"
DOM_POSITIVE_REFILL_OUTPUT_TYPE = "DOM_POSITIVE_REFILL"


class EngineOutputStore:
    def __init__(self, *, max_outputs_per_key: int = 5000) -> None:
        self.max_outputs_per_key = max(1, int(max_outputs_per_key))
        self._outputs_by_key: dict[
            tuple[str, str, str, str],
            OrderedDict[str, dict[str, Any]],
        ] = {}
        self._lock = threading.RLock()

    def publish_many(
        self,
        outputs: Iterable[Mapping[str, Any]],
    ) -> tuple[dict[str, Any], ...]:
        published: list[dict[str, Any]] = []
        with self._lock:
            for raw_output in outputs:
                output = _normalized_output(raw_output)
                if output is None:
                    continue
                key = (
                    str(output["producer"]),
                    str(output["output_type"]),
                    str(output["provider_symbol"] or output["symbol"]),
                    str(output["timeframe"]),
                )
                output_id = str(output["id"])
                bucket = self._outputs_by_key.setdefault(key, OrderedDict())
                bucket[output_id] = output
                bucket.move_to_end(output_id)
                while len(bucket) > self.max_outputs_per_key:
                    bucket.popitem(last=False)
                published.append(dict(output))
        return tuple(published)

    def publish_from_payload(
        self,
        payload: Mapping[str, Any],
    ) -> tuple[dict[str, Any], ...]:
        return self.publish_many(extract_engine_outputs(payload))

    def outputs(
        self,
        *,
        producer: str | None = None,
        output_type: str | None = None,
        symbol: str | None = None,
        provider_symbol: str | None = None,
        timeframe: str | None = None,
        start_ms: int | None = None,
        end_ms: int | None = None,
    ) -> tuple[dict[str, Any], ...]:
        normalized_producer = _normalize_text(producer)
        normalized_type = _normalize_text(output_type)
        normalized_timeframe = _normalize_text(timeframe)
        lookup_symbols = {
            _normalize_text(item)
            for item in (symbol, provider_symbol)
            if _normalize_text(item)
        }
        start = int(start_ms) if start_ms is not None else None
        end = int(end_ms) if end_ms is not None else None

        matches: list[dict[str, Any]] = []
        with self._lock:
            for (key_producer, key_type, key_symbol, key_timeframe), bucket in self._outputs_by_key.items():
                if normalized_producer and key_producer != normalized_producer:
                    continue
                if normalized_type and key_type != normalized_type:
                    continue
                if normalized_timeframe and key_timeframe != normalized_timeframe:
                    continue
                if lookup_symbols and key_symbol not in lookup_symbols:
                    continue
                for output in bucket.values():
                    if lookup_symbols and not (
                        _normalize_text(output.get("symbol")) in lookup_symbols
                        or _normalize_text(output.get("provider_symbol")) in lookup_symbols
                        or _normalize_text(output.get("mt5_symbol")) in lookup_symbols
                    ):
                        continue
                    timestamp_ms = _safe_int(output.get("timestamp_ms"), 0)
                    if start is not None and timestamp_ms < start:
                        continue
                    if end is not None and timestamp_ms > end:
                        continue
                    matches.append(dict(output))
        return tuple(
            sorted(
                matches,
                key=lambda item: (
                    _safe_int(item.get("timestamp_ms"), 0),
                    str(item.get("output_type") or item.get("type") or ""),
                    str(item.get("id") or ""),
                ),
            )
        )


def extract_engine_outputs(payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    raw_outputs = payload.get("engine_outputs", ())
    outputs: list[dict[str, Any]] = []
    if isinstance(raw_outputs, Mapping):
        for value in raw_outputs.values():
            outputs.extend(dict(item) for item in _iter_mappings(value))
    else:
        outputs.extend(dict(item) for item in _iter_mappings(raw_outputs))
    return tuple(outputs)


def _normalized_output(raw_output: Mapping[str, Any]) -> dict[str, Any] | None:
    output = dict(raw_output)
    producer = _normalize_text(output.get("producer") or output.get("source_engine"))
    output_type = _normalize_text(output.get("output_type") or output.get("type"))
    timeframe = _normalize_text(output.get("timeframe"))
    symbol = _normalize_text(output.get("symbol") or output.get("provider_symbol") or output.get("mt5_symbol"))
    provider_symbol = _normalize_text(output.get("provider_symbol") or symbol)
    mt5_symbol = _normalize_text(output.get("mt5_symbol") or symbol)
    timestamp_ms = _safe_int(
        output.get("timestamp_ms", output.get("event_time_ms", output.get("time_ms"))),
        0,
    )
    if not producer or not output_type or not timeframe or not provider_symbol or timestamp_ms <= 0:
        return None

    output["producer"] = producer
    output["source_engine"] = producer
    output["output_type"] = output_type
    if not _normalize_text(output.get("type")):
        output["type"] = output_type
    output["symbol"] = symbol or provider_symbol
    output["provider_symbol"] = provider_symbol
    output["mt5_symbol"] = mt5_symbol
    output["timeframe"] = timeframe
    output["timestamp_ms"] = timestamp_ms
    output.setdefault("event_time_ms", timestamp_ms)
    output_id = str(output.get("id") or output.get("output_id") or "").strip()
    if not output_id:
        output_id = _stable_output_id(output)
    output["id"] = output_id
    output["output_id"] = output_id
    return output


def _stable_output_id(output: Mapping[str, Any]) -> str:
    return "|".join(
        (
            _normalize_text(output.get("producer")),
            _normalize_text(output.get("output_type") or output.get("type")),
            _normalize_text(output.get("provider_symbol") or output.get("symbol")),
            _normalize_text(output.get("timeframe")),
            str(_safe_int(output.get("timestamp_ms"), 0)),
            str(output.get("price") or ""),
            _normalize_text(output.get("side")),
            str(output.get("order_id") or output.get("venue_order_id") or ""),
        )
    )


def _iter_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        return
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for item in value:
            if isinstance(item, Mapping):
                yield item


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(fallback)
