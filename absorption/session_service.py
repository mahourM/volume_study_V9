from __future__ import annotations
<<<<<<< HEAD

import asyncio
import csv
import hashlib
import json
import logging
import sqlite3
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Any, Iterable, Mapping
=======
STUDY_DEFAULT_DISPLAY_TIMEFRAME = "M15"
STUDY_DISPLAY_TIMEFRAMES = ("M1", "M5", "M15", "M30", "H1")
STUDY_DISPLAY_TIMEFRAME_SET = frozenset(STUDY_DISPLAY_TIMEFRAMES)
STUDY_DISPLAY_CANDLE_LIMITS = {
    "M1": 600,
    "M5": 120,
    "M15": 40,
    "M30": 20,
    "H1": 10,
}


def study_display_candle_limit(timeframe: str | None, default: int = 150) -> int:
    normalized_timeframe = str(timeframe or "").strip().upper()
    limit = STUDY_DISPLAY_CANDLE_LIMITS.get(normalized_timeframe)
    if limit is not None:
        return limit
    return max(1, int(default))


import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_FLOOR
from typing import Any
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from absorption.binance_kline_ws_client import (
    BinanceKlineWebSocketManager,
    KlineClosedEvent,
    KLINE_INTERVAL_BY_INTERNAL,
)
from absorption.adaptive_bin_sizer import AdaptiveBinSizer
from absorption.absorption_runtime import LiveAbsorptionRuntime
from absorption.binance_client import BinanceRestClient
from absorption.binance_aggtrade_ws_client import AggTradeEvent, BinanceAggTradeWebSocketManager
from absorption.footprint_memory import LatestCandleFootprintMemory
from absorption.hvn_detection import detect_hvns
from absorption.models import CandleFootprint, FootprintBin
from absorption.raw_event_buffers import RawMarketEventBuffer
from config.config_runtime import RuntimeConfig
<<<<<<< HEAD
from DOM.data_provider import DomFileDataProvider
from DOM.engine import DomEngineConfig, DomTimelineEngine
from DOM.models import DomContext
from process.dataProcessEngine import (
    ADD_ACTIONS,
    CANCEL_ACTIONS,
    CLEAR_ACTIONS,
    FILL_ACTIONS,
    MODIFY_ACTIONS,
    DataProcessEngine,
)
from process.data_sources import CmeFootprintReplaySource, DomDatabentoReplaySource
from process.models import (
    DataProcessConfig,
    ProcessFootprintSnapshot,
    ProcessReplayRequest,
    ProcessSymbol,
)
from process.refill_scan_index import RefillScanIndex
from process.sinks import CsvProcessLogSink, EngineOutputStoreSink, TriggerEngineSink
from process.time_range import VANCOUVER_TIMEZONE, parse_vancouver_replay_range
from process.warmup_historic_data import WarmupHistoricCatalog
from triggerEngine import TriggerEngine
from cme_provider.engines import (
    CME_BIN_TICK_COUNT,
    CmeCandleEngine,
    CmeChartPayloadBuilder,
    CmeDailyVolumeProfileEngine,
    CmeEngineConfig,
    CmeFootprintEngine,
    CmePagedHistoryEngine,
    normalize_cme_bin_tick_count,
)
from cme_provider.local_data import (
    CmeLocalDataCatalog,
    CmeLocalDataError,
    CmeLocalDbnTradeStore,
    trading_day_for_timestamp_ms,
)
from core.contract_spike import calculate_contract_spike_metrics, is_contract_spike
from core.engine_output_bus import EngineOutputStore
from core.performance_metrics import get_performance_metrics_recorder
from core.symbol_resolver import PROVIDER_BINANCE, PROVIDER_CME_LOCAL_DBN
from core.system_models import SymbolSessionState
from core.timeframe_policy import (
    DEFAULT_FOOTPRINT_TIMEFRAME as STUDY_DEFAULT_DISPLAY_TIMEFRAME,
    STUDY_DISPLAY_CANDLE_LIMITS,
    STUDY_TIMEFRAMES as STUDY_DISPLAY_TIMEFRAMES,
    TIMEFRAME_MS_BY_NAME,
    study_display_candle_limit,
)
from absorption_module.absorption_cluster_model import BinMarketData
from actor_proxy import ActorProxyEngine, RawDomEvent
=======
from core.performance_metrics import get_performance_metrics_recorder
from core.system_models import SymbolSessionState
from absorption_module.absorption_cluster_model import BinMarketData
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from duration_profile import build_duration_profile
from volume_profile.level_profile_builder import build_level_volume_profile
from volume_profile.zscore_profile_builder import (
    CandleBinVolumeProfile,
    build_candle_bin_volume_profile,
    build_volume_zscore_profile,
)
<<<<<<< HEAD
from triggerEngine import REFERENCE_TIMEOUT_CANDLES, TriggerConfig, TriggerEngine


def _stable_dom_index_version(index_path: Path) -> tuple[Any, ...]:
    """Return a data-derived version that does not change when an empty WAL is recreated."""
    try:
        with sqlite3.connect(
            f"file:{index_path.resolve().as_posix()}?mode=ro",
            uri=True,
            timeout=2,
        ) as connection:
            rows = connection.execute(
                """
                SELECT source_key, modified_ns, size_bytes, status,
                       indexed_until_ms, earliest_event_time_ms,
                       latest_event_time_ms, contract_symbols
                FROM dom_sources
                ORDER BY source_key
                """
            ).fetchall()
        if rows:
            return tuple(tuple(row) for row in rows)
    except (OSError, sqlite3.Error):
        pass

    try:
        index_stat = index_path.stat()
        return ((int(index_stat.st_size), int(index_stat.st_mtime_ns)),)
    except OSError:
        return ((-1, -1),)
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


SessionKey = tuple[str, str]
LOGGER = logging.getLogger(__name__)
M1_KLINE_RESTART_MISSED_CANDLES = 2
<<<<<<< HEAD
DATA_PROCESS_REFILL_SCAN_WARMUP_MS = 3 * 60_000
DATA_PROCESS_DELETE_SCAN_WARMUP_MS = 15 * 60_000
DATA_PROCESS_REFILL_SCAN_CACHE_SIZE = 16
DATA_PROCESS_REFILL_SCAN_CACHE_VERSION = "price-level-spike-complete-v5"
STUDY_DISPLAY_TIMEFRAME_SET = frozenset(STUDY_DISPLAY_TIMEFRAMES)
REFILL_SCAN_DISK_CACHE_FIELDS = (
    "provider_symbol", "mt5_symbol", "symbol", "timeframe",
    "marker_time_ms", "timestamp_ms", "event_time_ms",
    "marker_price", "price", "side",
    "price_base_refill_count", "refill_count", "positive_refill_count",
    "price_base_refill_contracts", "refill_contracts", "refill_added_contracts",
    "executed_refill_contracts", "withdrawn_refill_contracts",
    "refill_filled_contracts", "positive_refill_filled_total",
    "executed_contracts", "added_contracts", "gross_added_contracts",
    "opening_liquidity", "available_liquidity", "withdrawn_contracts",
    "cancelled_or_withdrawn_contracts", "closing_liquidity",
    "order_count", "has_refill", "has_price_activity",
    "refill_execution_rate", "level_execution_rate",
    "display_text", "refill_display", "source", "output_type",
    "payload_id", "id", "output_id",
)
REFILL_SCAN_RESPONSE_FIELDS = (
    "provider_symbol", "mt5_symbol", "symbol", "timeframe",
    "marker_time_ms", "marker_price", "side",
    "price_base_refill_count", "price_base_refill_contracts",
    "executed_refill_contracts",
    "withdrawn_refill_contracts", "executed_contracts",
    "opening_liquidity", "available_liquidity", "withdrawn_contracts",
    "closing_liquidity", "order_count", "has_price_activity",
    "refill_execution_rate", "display_text", "source",
)


def _refill_scan_response_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        field: payload.get(field)
        for field in REFILL_SCAN_RESPONSE_FIELDS
        if payload.get(field) not in {None, ""}
    }


def _refill_scan_disk_cache_path(root: Any, cache_key: tuple[Any, ...]) -> Any:
    serialized_key = json.dumps(cache_key, ensure_ascii=True, default=str, separators=(",", ":"))
    digest = hashlib.sha256(serialized_key.encode("utf-8")).hexdigest()
    return root / "DOM" / ".cache" / "refill_scan" / f"{digest}.json"


def _load_refill_scan_disk_cache(root: Any, cache_key: tuple[Any, ...]) -> dict[str, Any] | None:
    path = _refill_scan_disk_cache_path(root, cache_key)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return None
    if not isinstance(raw, dict):
        return None
    if isinstance(raw.get("aggregate_rows"), list):
        fields = tuple(raw.get("aggregate_fields") or ())
        if not fields:
            return None
        raw["aggregate_payloads"] = tuple(
            dict(zip(fields, row))
            for row in raw["aggregate_rows"]
            if isinstance(row, list)
        )
    elif isinstance(raw.get("aggregate_payloads"), list):
        raw["aggregate_payloads"] = tuple(raw["aggregate_payloads"])
    else:
        return None
    raw["footprints"] = ()
    raw["footprint_symbols"] = tuple(raw.get("footprint_symbols") or ())
    raw["spike_score_payloads"] = tuple(raw.get("spike_score_payloads") or ())
    return raw


def _store_refill_scan_disk_cache(
    root: Any,
    cache_key: tuple[Any, ...],
    cached_replay: Mapping[str, Any],
) -> None:
    path = _refill_scan_disk_cache_path(root, cache_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        "aggregate_fields": list(REFILL_SCAN_DISK_CACHE_FIELDS),
        "aggregate_rows": [
            [payload.get(field) for field in REFILL_SCAN_DISK_CACHE_FIELDS]
            for payload in (cached_replay.get("aggregate_payloads") or ())
        ],
        "processed_event_count": int(cached_replay.get("processed_event_count") or 0),
        "emitted_payload_count": int(cached_replay.get("emitted_payload_count") or 0),
        "footprint_candle_count": int(cached_replay.get("footprint_candle_count") or 0),
        "footprint_symbols": list(cached_replay.get("footprint_symbols") or ()),
        "spike_score_payloads": list(cached_replay.get("spike_score_payloads") or ()),
    }
    temporary = path.with_suffix(".tmp")
    temporary.write_text(
        json.dumps(serializable, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )
    temporary.replace(path)
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


def make_session_key(mt5_symbol: str, timeframe: str) -> SessionKey:
    return (mt5_symbol, timeframe.strip().upper())


<<<<<<< HEAD
def _process_symbols_for_timeframe(
    symbols: tuple[ProcessSymbol, ...],
    *,
    timeframe: str,
    interval: str,
) -> tuple[ProcessSymbol, ...]:
    normalized_timeframe = timeframe.strip().upper()
    return tuple(
        replace(
            symbol,
            timeframe=normalized_timeframe,
            interval=interval,
        )
        for symbol in symbols
    )


def _payload_refill_count(payload: Mapping[str, Any]) -> int:
    value = payload.get("price_base_refill_count")
    if value in {None, ""}:
        return 0
    try:
        return int(Decimal(str(value)))
    except Exception:
        return 0


def _payload_scan_contract_count(payload: Mapping[str, Any]) -> int:
    if payload.get("price_base_refill_contracts") not in {None, ""}:
        return _payload_int(payload, "price_base_refill_contracts")
    return _payload_int(
        payload,
        "deleted_contracts",
        "refill_filled_contracts",
        "positive_refill_filled_total",
        "executed_contracts",
        "refill_contracts",
        "positive_refill_total",
        "refill_total",
        "top_order_positive_refill_filled_total",
        "top_order_positive_refill_total",
    )


def _payload_int(payload: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        value = payload.get(key)
        if value in {None, ""}:
            continue
        try:
            return int(Decimal(str(value)))
        except Exception:
            continue
    return 0


def _payload_text(payload: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _normalized_dom_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"A", "ASK", "SELL", "OFFER"}:
        return "ASK"
    if side in {"B", "BID", "BUY"}:
        return "BID"
    return side


def _format_scan_price(price: Decimal, places: int) -> str:
    return f"{Decimal(str(price)):.{int(places)}f}"


def _delete_scan_payload(
    *,
    provider_symbol: str,
    mt5_symbol: str,
    market_provider: str,
    timeframe: str,
    marker_time_ms: int,
    marker_price: str,
    side: str,
) -> dict[str, Any]:
    output_id = "|".join(
        (
            "DATA_PROCESS",
            "DELETE_SCAN",
            provider_symbol.upper(),
            timeframe.upper(),
            str(marker_time_ms),
            marker_price,
            side,
        )
    )
    return {
        "payload_id": output_id,
        "id": output_id,
        "output_id": output_id,
        "type": "DATA_PROCESS_DELETE_SCAN_LEVEL",
        "source": "DATA_PROCESS_DELETE_SCAN",
        "timestamp_ms": int(marker_time_ms),
        "event_time_ms": int(marker_time_ms),
        "marker_time_ms": int(marker_time_ms),
        "marker_price": marker_price,
        "price": marker_price,
        "side": side,
        "provider_symbol": provider_symbol,
        "symbol": provider_symbol,
        "mt5_symbol": mt5_symbol,
        "market_provider": market_provider,
        "timeframe": timeframe,
        "delete_count": 0,
        "deleted_contracts": 0,
        "c_delete_count": 0,
        "c_deleted_contracts": 0,
        "m_delete_count": 0,
        "m_deleted_contracts": 0,
        "refill_count": 0,
        "refill_filled_contracts": 0,
        "positive_refill_count": 0,
        "positive_refill_filled_total": 0,
        "span_candles": 5,
    }


@dataclass
class _DeleteScanOrder:
    price: Decimal
    side: str
    size: int


def _chart_aligned_refill_scan_payload(
    payload: Mapping[str, Any],
    *,
    start_ms: int,
    end_ms: int,
) -> dict[str, Any] | None:
    aligned = dict(payload)
    source_event_time_ms = _payload_int(
        aligned,
        "timestamp_ms",
        "event_time_ms",
        "threshold_time_ms",
    )
    marker_time_ms = _payload_int(
        aligned,
        "marker_time_ms",
        "footprint_open_time_ms",
        "timestamp_ms",
        "event_time_ms",
    )
    event_time_for_filter = source_event_time_ms or marker_time_ms
    if event_time_for_filter < int(start_ms) or event_time_for_filter >= int(end_ms):
        return None
    if marker_time_ms <= 0 or marker_time_ms < int(start_ms) or marker_time_ms >= int(end_ms):
        marker_time_ms = event_time_for_filter
    marker_price = _payload_text(
        aligned,
        "marker_price",
        "footprint_bin_low",
        "price",
    )
    side = _payload_text(aligned, "side", "top_order_side").upper()
    provider_symbol = _payload_text(aligned, "provider_symbol", "symbol").upper()
    timeframe = _payload_text(aligned, "timeframe").upper()
    if not marker_price or side not in {"BID", "ASK"} or not provider_symbol or not timeframe:
        return None
    aligned["source_event_time_ms"] = source_event_time_ms
    aligned["timestamp_ms"] = marker_time_ms
    aligned["event_time_ms"] = marker_time_ms
    aligned["marker_time_ms"] = marker_time_ms
    aligned["marker_price"] = marker_price
    aligned["source_price"] = _payload_text(aligned, "price", "level_price", "reference_price")
    aligned["price"] = marker_price
    aligned["side"] = side
    return aligned


def _refill_scan_group_key(payload: Mapping[str, Any]) -> tuple[str, str, int, str, str]:
    return (
        _payload_text(payload, "provider_symbol", "symbol").upper(),
        _payload_text(payload, "timeframe").upper(),
        _payload_int(payload, "marker_time_ms", "timestamp_ms", "event_time_ms"),
        _payload_text(payload, "marker_price", "price"),
        _payload_text(payload, "side", "top_order_side").upper(),
    )


def _matches_refill_activity_filter(payload: Mapping[str, Any], code: str) -> bool:
    normalized = str(code or "").strip().upper()
    if not normalized:
        return True
    value = int(normalized[1:])
    if normalized[0] == "O":
        return _payload_int(payload, "order_count") >= value
    required_side = "ASK" if normalized[0] == "A" else "BID"
    return (
        str(payload.get("side") or "").strip().upper() == required_side
        and _payload_int(payload, "added_contracts") >= value
    )


def _displayed_execution_rate(payload: Mapping[str, Any]) -> float:
    rate_field = (
        "refill_execution_rate"
        if bool(payload.get("has_refill"))
        else "level_execution_rate"
    )
    try:
        return float(payload.get(rate_field) or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _spike_score_scan_payloads(
    footprints: Iterable[Any],
    aggregate_payloads: Iterable[Mapping[str, Any]],
    *,
    score_min: Decimal,
    start_ms: int,
    end_ms: int,
) -> tuple[dict[str, Any], ...]:
    activity_by_candle_side: dict[
        tuple[str, str, int, str],
        list[tuple[Decimal, Mapping[str, Any]]],
    ] = {}
    for payload in aggregate_payloads:
        side = _normalized_dom_side(payload.get("side"))
        if side not in {"ASK", "BID"}:
            continue
        timeframe = _payload_text(payload, "timeframe").upper()
        interval_ms = int(TIMEFRAME_MS_BY_NAME.get(timeframe, 60_000))
        marker_time_ms = _payload_int(payload, "marker_time_ms", "timestamp_ms")
        candle_open_time_ms = (marker_time_ms // interval_ms) * interval_ms
        try:
            price = Decimal(_payload_text(payload, "marker_price", "price"))
        except Exception:
            continue
        key = (
            _payload_text(payload, "provider_symbol", "symbol").upper(),
            timeframe,
            candle_open_time_ms,
            side,
        )
        activity_by_candle_side.setdefault(key, []).append((price, payload))

    results: list[dict[str, Any]] = []
    for snapshot in footprints:
        symbol = getattr(snapshot, "symbol", None)
        provider_symbol = str(getattr(symbol, "provider_symbol", "") or "").strip().upper()
        mt5_symbol = str(getattr(symbol, "mt5_symbol", "") or "").strip().upper()
        timeframe = str(getattr(symbol, "timeframe", "") or "").strip().upper()
        for candle in getattr(snapshot, "candles", ()):
            open_time_ms = _payload_int(candle, "open_time_ms", "open_time")
            if open_time_ms < int(start_ms) or open_time_ms >= int(end_ms):
                continue
            for footprint_bin in candle.get("bins", ()):
                if not isinstance(footprint_bin, Mapping):
                    continue
                raw_score = footprint_bin.get("contract_spike_score")
                if raw_score in {None, ""}:
                    raw_score = footprint_bin.get("l2", {}).get("contract_spike_score")
                try:
                    score = Decimal(str(raw_score))
                    bin_low = Decimal(
                        str(
                            footprint_bin.get("low")
                            or footprint_bin.get("bin_low")
                            or footprint_bin.get("price")
                        )
                    )
                    bin_high = Decimal(
                        str(
                            footprint_bin.get("high")
                            or footprint_bin.get("bin_high")
                            or bin_low
                        )
                    )
                except Exception:
                    continue
                if score < score_min:
                    continue
                side_values: dict[str, tuple[int, int]] = {}
                for side in ("ASK", "BID"):
                    activities = activity_by_candle_side.get(
                        (provider_symbol, timeframe, open_time_ms, side),
                        (),
                    )
                    side_values[side] = (
                        sum(
                            _payload_refill_count(activity)
                            for activity_price, activity in activities
                            if bin_low <= activity_price < bin_high
                        ),
                        sum(
                            _payload_int(activity, "executed_contracts")
                            for activity_price, activity in activities
                            if bin_low <= activity_price < bin_high
                        ),
                    )
                marker_price = format(bin_low, "f")
                marker_id = "|".join(
                    ("SPIKE_SCORE_SCAN", provider_symbol, timeframe, str(open_time_ms), marker_price)
                )
                results.append(
                    {
                        "payload_id": marker_id,
                        "id": marker_id,
                        "output_id": marker_id,
                        "type": "DATA_PROCESS_SPIKE_SCORE_SCAN_BIN",
                        "source": "SPIKE_SCORE_SCAN",
                        "provider_symbol": provider_symbol,
                        "symbol": provider_symbol,
                        "mt5_symbol": mt5_symbol,
                        "timeframe": timeframe,
                        "timestamp_ms": open_time_ms,
                        "event_time_ms": open_time_ms,
                        "marker_time_ms": open_time_ms,
                        "marker_price": marker_price,
                        "price": marker_price,
                        "contract_spike_score": format(score, "f"),
                        "ask_refill_count": side_values["ASK"][0],
                        "bid_refill_count": side_values["BID"][0],
                        "ask_execution_count": side_values["ASK"][1],
                        "bid_execution_count": side_values["BID"][1],
                        "span_candles": 5,
                    }
                )
    return tuple(
        sorted(
            results,
            key=lambda item: (
                item["marker_time_ms"],
                item["provider_symbol"],
                Decimal(item["marker_price"]),
            ),
        )
    )


def _aggregate_refill_scan_payloads(
    payloads: Iterable[Mapping[str, Any]],
    *,
    start_ms: int,
    end_ms: int,
) -> tuple[dict[str, Any], ...]:
    grouped: dict[tuple[str, str, int, str, str], dict[str, Any]] = {}
    order_ids_by_key: dict[tuple[str, str, int, str, str], list[str]] = {}
    for raw_payload in payloads:
        if (
            raw_payload.get("price_base_refill_count") in {None, ""}
            or raw_payload.get("price_base_refill_contracts") in {None, ""}
        ):
            continue
        payload = _chart_aligned_refill_scan_payload(
            raw_payload,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        if payload is None:
            continue
        provider_symbol, timeframe, marker_time_ms, marker_price, side = _refill_scan_group_key(payload)
        key = (provider_symbol, timeframe, marker_time_ms, marker_price, side)
        order_id = _payload_text(payload, "order_id", "venue_order_id")
        if key not in grouped:
            grouped[key] = {
                **payload,
                "payload_id": "",
                "id": "",
                "output_id": "",
                "timestamp_ms": marker_time_ms,
                "event_time_ms": marker_time_ms,
                "marker_time_ms": marker_time_ms,
                "marker_price": marker_price,
                "price": marker_price,
                "side": side,
                "refill_count": 0,
                "refill_contracts": 0,
                "price_base_refill_count": 0,
                "price_base_refill_contracts": 0,
                "refill_added_contracts": 0,
                "executed_refill_contracts": 0,
                "withdrawn_refill_contracts": 0,
                "refill_method": "price_base_refill",
                "refill_filled_contracts": 0,
                "positive_refill_count": 0,
                "positive_refill_total": 0,
                "positive_refill_filled_total": 0,
                "refill_total": 0,
                "trade_count": 0,
                "executed_contracts": 0,
                "add_event_count": 0,
                "fill_event_count": 0,
                "opening_liquidity": 0,
                "available_liquidity": 0,
                "gross_added_contracts": 0,
                "non_refill_added_contracts": 0,
                "added_contracts": 0,
                "withdrawn_contracts": 0,
                "cancelled_or_withdrawn_contracts": 0,
                "closing_liquidity": 0,
                "order_id": "",
                "venue_order_id": "",
                "order_ids": (),
                "order_count": 0,
                "aggregation_key": "candle_price_side",
                "source": "DATA_PROCESS_REFILL_SCAN_AGGREGATE",
            }
            order_ids_by_key[key] = []
        aggregate = grouped[key]
        is_price_activity = str(payload.get("output_type") or "").upper() == "DATA_PROCESS_PRICE_ACTIVITY_LEVEL"
        if is_price_activity:
            for field in (
                "add_event_count", "fill_event_count", "opening_liquidity",
                "available_liquidity", "gross_added_contracts",
                "non_refill_added_contracts", "added_contracts",
                "executed_contracts", "withdrawn_contracts",
                "cancelled_or_withdrawn_contracts", "closing_liquidity",
            ):
                aggregate[field] = _payload_int(payload, field)
            aggregate["refill_count"] = _payload_refill_count(payload)
            aggregate["refill_contracts"] = _payload_int(
                payload, "refill_added_contracts", "price_base_refill_contracts"
            )
            aggregate["executed_refill_contracts"] = _payload_int(
                payload, "executed_refill_contracts"
            )
            aggregate["withdrawn_refill_contracts"] = _payload_int(
                payload, "withdrawn_refill_contracts"
            )
            aggregate["_has_activity_snapshot"] = True
        else:
            if not bool(aggregate.get("_has_activity_snapshot")):
                aggregate["refill_count"] += _payload_refill_count(payload)
                aggregate["refill_contracts"] += _payload_int(
                    payload,
                    "price_base_refill_contracts",
                )
                aggregate["executed_refill_contracts"] += _payload_int(payload, "executed_refill_contracts")
                aggregate["withdrawn_refill_contracts"] += _payload_int(payload, "withdrawn_refill_contracts")
        if not is_price_activity:
            aggregate["refill_filled_contracts"] += _payload_int(
                payload,
                "refill_filled_contracts",
                "positive_refill_filled_total",
            )
            aggregate["trade_count"] += _payload_int(payload, "trade_count")
        for activity_order_id in payload.get("order_ids", ()) or ():
            normalized_order_id = str(activity_order_id or "").strip()
            if normalized_order_id and normalized_order_id not in order_ids_by_key[key]:
                order_ids_by_key[key].append(normalized_order_id)
        for field in (
            "market_buy",
            "market_sell",
            "market_buy_contracts",
            "market_sell_contracts",
            "ask_traded_contracts",
            "bid_traded_contracts",
        ):
            aggregate[field] = max(_payload_int(aggregate, field), _payload_int(payload, field))
        if order_id and order_id not in order_ids_by_key[key]:
            order_ids_by_key[key].append(order_id)

    results: list[dict[str, Any]] = []
    for key, aggregate in grouped.items():
        provider_symbol, timeframe, marker_time_ms, marker_price, side = key
        order_ids = tuple(order_ids_by_key.get(key, ()))
        output_id = "|".join(
            (
                "DATA_PROCESS",
                "REFILL_SCAN",
                provider_symbol,
                timeframe,
                str(marker_time_ms),
                marker_price,
                side,
            )
        )
        aggregate["payload_id"] = output_id
        aggregate["id"] = output_id
        aggregate["output_id"] = output_id
        aggregate["positive_refill_count"] = int(aggregate["refill_count"])
        aggregate["positive_refill_total"] = int(aggregate["refill_contracts"])
        aggregate["price_base_refill_count"] = int(aggregate["refill_count"])
        aggregate["price_base_refill_contracts"] = int(aggregate["refill_contracts"])
        aggregate["refill_added_contracts"] = int(aggregate["refill_contracts"])
        aggregate["executed_refill_contracts"] = min(
            int(aggregate["executed_refill_contracts"]),
            int(aggregate["refill_contracts"]),
        )
        added_contracts = int(aggregate["refill_contracts"])
        executed_refill_contracts = int(aggregate["executed_refill_contracts"])
        execution_rate = round(
            (executed_refill_contracts / added_contracts * 100.0) if added_contracts > 0 else 0.0,
            1,
        )
        rate_label = f"{execution_rate:.1f}".rstrip("0").rstrip(".")
        aggregate["refill_execution_rate"] = execution_rate
        level_added = int(
            aggregate.get("gross_added_contracts")
            or aggregate.get("added_contracts")
            or 0
        )
        opening_liquidity = int(aggregate.get("opening_liquidity") or 0)
        available_liquidity = opening_liquidity + level_added
        level_executed = int(aggregate.get("executed_contracts") or 0)
        level_rate = (
            round(level_executed / available_liquidity * 100.0, 1)
            if available_liquidity > 0 else None
        )
        level_rate_label = (
            f"{level_rate:.1f}".rstrip("0").rstrip(".")
            if level_rate is not None else "N/A"
        )
        has_refill = int(aggregate["refill_count"]) > 0
        has_price_activity = bool(len(order_ids) > 0 or level_executed > 0 or level_added > 0)
        aggregate["level_execution_rate"] = level_rate
        aggregate["level_execution_rate_defined"] = level_rate is not None
        aggregate["available_liquidity"] = available_liquidity
        expected_closing = (
            opening_liquidity + level_added - level_executed
            - int(aggregate.get("withdrawn_contracts") or aggregate.get("cancelled_or_withdrawn_contracts") or 0)
        )
        aggregate["level_execution_invariant_ok"] = (
            int(aggregate.get("closing_liquidity") or 0) == expected_closing
            and expected_closing >= 0
        )
        aggregate["added_breakdown_invariant_ok"] = (
            level_added
            == int(aggregate.get("non_refill_added_contracts") or 0)
            + int(aggregate.get("refill_contracts") or 0)
        )
        aggregate["has_refill"] = has_refill
        aggregate["has_price_activity"] = has_price_activity
        aggregate["display_mode"] = "refill" if has_refill else "level_execution"
        aggregate["refill_display"] = (
            f"{int(aggregate['refill_count'])}({added_contracts}) "
            f"E{executed_refill_contracts} - {rate_label}%"
        )
        aggregate["display_text"] = (
            aggregate["refill_display"]
            if has_refill
            else f"O{len(order_ids)} A{level_added} E{level_executed} - {level_rate_label}%"
        )
        aggregate["refill_method"] = "price_base_refill"
        aggregate["positive_refill_filled_total"] = int(aggregate["refill_filled_contracts"])
        aggregate["refill_total"] = int(aggregate["refill_contracts"])
        aggregate["order_ids"] = order_ids
        aggregate["order_count"] = len(order_ids)
        aggregate["order_id"] = ""
        aggregate["venue_order_id"] = ""
        aggregate.pop("_has_activity_snapshot", None)
        results.append(aggregate)
    return tuple(
        sorted(
            results,
            key=lambda item: (
                _payload_int(item, "marker_time_ms", "timestamp_ms"),
                str(item.get("side") or ""),
                Decimal(str(item.get("marker_price") or item.get("price") or "0")),
            ),
        )
    )


def _replay_payload_available_time_ms(payload: Mapping[str, Any]) -> int:
    source_times = [
        _payload_int(payload, key)
        for key in (
            "close_time_ms",
            "source_event_time_ms",
            "timestamp_ms",
            "event_time_ms",
            "threshold_time_ms",
            "updated_at_ms",
        )
    ]
    source_times = [value for value in source_times if value > 0]
    if source_times:
        return max(source_times)
    return _payload_int(
        payload,
        "marker_time_ms",
        "footprint_open_time_ms",
        "candle_open_time_ms",
    )


def _replay_candle_open_time_ms(candle: Mapping[str, Any]) -> int:
    return _payload_int(candle, "open_time_ms", "open_time")


def _replay_candle_close_time_ms(candle: Mapping[str, Any]) -> int:
    return _payload_int(candle, "close_time_ms", "close_time")


def _replay_payload_sort_key(payload: Mapping[str, Any]) -> tuple[int, int, str]:
    return (
        _replay_payload_available_time_ms(payload),
        _payload_int(payload, "marker_time_ms", "footprint_open_time_ms"),
        _payload_text(payload, "payload_id", "output_id", "id"),
    )


def _replay_payload_canceled_zone_ids(payload: Mapping[str, Any]) -> tuple[str, ...]:
    raw_value = payload.get("canceled_zone_ids")
    if isinstance(raw_value, str):
        items: Iterable[Any] = (raw_value,)
    elif isinstance(raw_value, Iterable) and not isinstance(raw_value, (bytes, Mapping)):
        items = raw_value
    else:
        items = ()
    return tuple(str(item or "").strip() for item in items if str(item or "").strip())


def _replay_reference_payload_id(reference_bin: Any) -> str:
    if not isinstance(reference_bin, Mapping):
        return ""
    return str(
        reference_bin.get("payload_id")
        or reference_bin.get("process_payload_id")
        or reference_bin.get("source_payload_id")
        or reference_bin.get("output_id")
        or reference_bin.get("id")
        or reference_bin.get("refill_record_id")
        or reference_bin.get("record_id")
        or ""
    ).strip()


def _remove_canceled_replay_entry_setups(
    state_store: dict[tuple[str, str], Any],
    position_store: dict[tuple[str, str], Any],
    payload: Mapping[str, Any],
) -> None:
    canceled_ids = set(_replay_payload_canceled_zone_ids(payload))
    if not canceled_ids:
        return
    timeframe = _payload_text(payload, "timeframe").upper()
    lookup_keys = {
        (symbol, timeframe)
        for symbol in (
            _payload_text(payload, "mt5_symbol", "symbol").upper(),
            _payload_text(payload, "provider_symbol", "symbol").upper(),
        )
        if symbol and timeframe
    }
    if not lookup_keys:
        lookup_keys = set(state_store)
    for key in tuple(lookup_keys):
        state = state_store.get(key)
        if state is None or not getattr(state, "setups", ()):
            continue
        kept_setups = tuple(
            setup
            for setup in state.setups
            if _replay_reference_payload_id(getattr(setup, "reference_bin", None)) not in canceled_ids
        )
        if len(kept_setups) == len(state.setups):
            continue
        state.setups = kept_setups
        if position_store.get(key) is not None:
            if not kept_setups:
                state.reference_bin = None
                state.reference_bins = ()
            else:
                TriggerEngine._sync_entry_state(state)
            continue
        TriggerEngine._sync_entry_state(state)


def _enrich_replay_candles_chronologically(
    *,
    trigger_engine: TriggerEngine,
    replay_candles: list[dict[str, Any]],
    replay_payloads: Iterable[Mapping[str, Any]],
    evaluation_time_ms: int,
    record_closed_positions: bool = False,
) -> list[dict[str, Any]]:
    ordered_candles = sorted(
        replay_candles,
        key=lambda item: (
            _replay_candle_open_time_ms(item),
            _payload_text(item, "provider_symbol", "symbol"),
            _payload_text(item, "timeframe"),
        ),
    )
    for candle in ordered_candles:
        candle["trigger_signals"] = []

    payloads = sorted((dict(item) for item in replay_payloads), key=_replay_payload_sort_key)
    payload_sink = TriggerEngineSink(trigger_engine)
    state_store: dict[tuple[str, str], Any] = {}
    position_store: dict[tuple[str, str], Any] = {}
    signals: list[dict[str, Any]] = []
    payload_index = 0

    for index, candle in enumerate(ordered_candles):
        if not trigger_engine._is_closed(candle, evaluation_time_ms=evaluation_time_ms):
            continue
        close_time_ms = _replay_candle_close_time_ms(candle)
        if close_time_ms <= 0:
            close_time_ms = int(evaluation_time_ms)
        while (
            payload_index < len(payloads)
            and _replay_payload_available_time_ms(payloads[payload_index]) <= close_time_ms
        ):
            payload_sink.publish((payloads[payload_index],))
            _remove_canceled_replay_entry_setups(
                state_store,
                position_store,
                payloads[payload_index],
            )
            payload_index += 1

        next_candle = ordered_candles[index + 1] if index + 1 < len(ordered_candles) else None
        current_candle = (
            next_candle
            if next_candle is not None
            and not trigger_engine._is_closed(
                next_candle,
                evaluation_time_ms=evaluation_time_ms,
            )
            else None
        )
        candle_signals = [
            signal.to_payload()
            for signal in trigger_engine.process_closed_candle(
                candle,
                next_candle=next_candle,
                current_candle=current_candle,
                entry_states=state_store,
                positions=position_store,
                record_closed_position=record_closed_positions,
            )
        ]
        candle["trigger_signals"] = candle_signals
        signals.extend(candle_signals)

    return signals


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
@dataclass(frozen=True)
class AbsorptionSessionSpec:
    mt5_symbol: str
    binance_symbol: str
    timeframe: str
    interval: str
<<<<<<< HEAD
    market_provider: str = PROVIDER_BINANCE
    provider_symbol: str = ""
    dataset: str = ""
    schema: str = ""
    tick_size: str = ""
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


class AbsorptionFootprintService:
    def __init__(self, runtime_config: RuntimeConfig) -> None:
        self.runtime_config = runtime_config
        self.client = BinanceRestClient(runtime_config.binance_api_base_url)
        stream_base_url = getattr(runtime_config, "binance_ws_base_url", "wss://stream.binance.com:9443/ws")
        self.agg_trade_stream = BinanceAggTradeWebSocketManager(stream_base_url=stream_base_url)
        self.kline_stream = BinanceKlineWebSocketManager(stream_base_url=stream_base_url)
<<<<<<< HEAD
        self.cme_catalog = CmeLocalDataCatalog(
            data_dir=runtime_config.project_root / runtime_config.cme_local_data_dir_name,
            dataset=runtime_config.cme_dataset,
            schema=runtime_config.cme_schema,
            default_tick_size=runtime_config.cme_default_tick_size,
        )
        self.cme_trade_store = CmeLocalDbnTradeStore(
            self.cme_catalog,
            partition_cache_size=runtime_config.cme_partition_cache_size,
            trading_day_cache_size=runtime_config.cme_trading_day_cache_size,
            cumulative_delta_cache_size=runtime_config.cme_cumulative_delta_cache_size,
        )
        cme_engine_config = CmeEngineConfig(
            session_start_hour_chicago=runtime_config.cme_trading_day_start_hour_chicago,
            new_york_session_start_hour=runtime_config.cme_new_york_session_start_hour,
            new_york_session_start_minute=runtime_config.cme_new_york_session_start_minute,
            new_york_session_end_hour=runtime_config.cme_new_york_session_end_hour,
            new_york_session_end_minute=runtime_config.cme_new_york_session_end_minute,
        )
        self.cme_candle_engine = CmeCandleEngine(trade_store=self.cme_trade_store)
        self.cme_footprint_engine = CmeFootprintEngine(
            trade_store=self.cme_trade_store,
            candle_engine=self.cme_candle_engine,
            config=cme_engine_config,
        )
        self.cme_daily_volume_profile_engine = CmeDailyVolumeProfileEngine(
            trade_store=self.cme_trade_store,
            config=cme_engine_config,
        )
        self.actor_proxy_engine = ActorProxyEngine()
        self.trigger_engine = TriggerEngine(
            TriggerConfig(
                supported_timeframes=runtime_config.trigger_timeframes,
                confirmation_timeframe=runtime_config.trigger_confirmation_timeframe,
                efficiency_max=Decimal(
                    runtime_config.trigger_efficiency_max
                ),
                diagonal_ratio_min=Decimal(
                    runtime_config.trigger_diagonal_ratio_min
                ),
                contract_spike_score_min=Decimal(
                    runtime_config.trigger_contract_spike_score_min
                ),
                reference_contract_spike_score_min=Decimal(
                    runtime_config.trigger_reference_contract_spike_score_min
                ),
                reference_spike_score_deviation_min=Decimal(
                    runtime_config.trigger_reference_spike_score_deviation_min
                ),
                reference_zone_tick_count=(
                    runtime_config.trigger_reference_zone_tick_count
                ),
                point_value_by_symbol={
                    symbol: Decimal(point_value)
                    for symbol, point_value in (
                        runtime_config.trigger_point_value_by_symbol.items()
                    )
                },
                bin_tick_count=runtime_config.trigger_bin_tick_count,
                runtime_logging_enabled=runtime_config.trigger_runtime_logging_enabled,
            ),
            actor_proxy_engine=self.actor_proxy_engine,
        )
        self.engine_output_store = EngineOutputStore()
        self.cme_chart_payload_builder = CmeChartPayloadBuilder(
            candle_engine=self.cme_candle_engine,
            volume_profile_engine=self.cme_daily_volume_profile_engine,
        )
        self.cme_paged_history_engine = CmePagedHistoryEngine(
            catalog=self.cme_catalog,
            trade_store=self.cme_trade_store,
            candle_engine=self.cme_candle_engine,
            footprint_engine=self.cme_footprint_engine,
            volume_profile_engine=self.cme_daily_volume_profile_engine,
            config=cme_engine_config,
            trigger_engine=self.trigger_engine,
            engine_output_store=self.engine_output_store,
            candle_cache_size=runtime_config.cme_candle_cache_size,
            footprint_cache_size=runtime_config.cme_footprint_cache_size,
            footprint_index_path=(
                runtime_config.project_root
                / runtime_config.cme_local_data_dir_name
                / ".cache"
                / "footprint_candles.sqlite3"
            ),
        )
        self.dom_data_provider = DomFileDataProvider(
            data_dir=runtime_config.project_root / runtime_config.dom_data_dir_name,
            extracted_cache_dir=(
                runtime_config.project_root
                / runtime_config.dom_data_dir_name
                / runtime_config.dom_extracted_cache_dir_name
            ),
            file_globs=runtime_config.dom_file_globs,
            file_cache_size=runtime_config.dom_file_cache_size,
            dbn_batch_size=runtime_config.dom_dbn_batch_size,
            max_events_per_request=runtime_config.dom_provider_event_limit,
            stream_bucket_ms=runtime_config.dom_stream_bucket_ms,
            stream_cache_max_buckets=runtime_config.dom_stream_cache_max_buckets,
            prefetch_window_multiplier=runtime_config.dom_prefetch_window_multiplier,
            prefetch_max_ms=runtime_config.dom_prefetch_max_ms,
        )
        self.warmup_historic_catalog = WarmupHistoricCatalog(
            root_dir=(
                runtime_config.project_root
                / runtime_config.warmup_historic_data_dir_name
            ),
            manifest_name=runtime_config.warmup_historic_manifest_name,
            dom_dir_name=runtime_config.warmup_historic_dom_dir_name,
            l2_dir_name=runtime_config.warmup_historic_l2_dir_name,
            cache_dir_name=runtime_config.warmup_historic_cache_dir_name,
            file_globs=runtime_config.warmup_historic_file_globs,
            default_market_provider=PROVIDER_CME_LOCAL_DBN,
            default_dataset=runtime_config.cme_dataset,
            default_dom_schema="mbo",
            default_l2_schema="l2",
            default_timeframe=runtime_config.trigger_confirmation_timeframe or "M1",
            default_tick_size=runtime_config.cme_default_tick_size,
        )
        self.dom_timeline_engine = DomTimelineEngine(
            provider=self.dom_data_provider,
            config=DomEngineConfig(
                window_cache_size=runtime_config.dom_window_cache_size,
                max_events_per_window=runtime_config.dom_max_events_per_window,
                max_resting_segments_per_window=runtime_config.dom_max_resting_segments_per_window,
                max_line_points_per_window=runtime_config.dom_max_line_points_per_window,
                max_price_levels=runtime_config.dom_max_price_levels,
                time_bucket_divisor=runtime_config.dom_time_bucket_divisor,
                render_overscan_multiplier=runtime_config.dom_render_overscan_multiplier,
                render_overscan_max_ms=runtime_config.dom_render_overscan_max_ms,
            ),
        )
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.absorption_runtime = LiveAbsorptionRuntime()
        self.raw_event_buffer = RawMarketEventBuffer()
        self._closed_kline_queues: dict[tuple[str, str], asyncio.PriorityQueue[tuple[int, int, KlineClosedEvent]]] = {}
        self._closed_kline_tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._closed_kline_sequence = 0
<<<<<<< HEAD
        self._snapshot_cache_ttl_seconds = max(
            1.0,
            float(runtime_config.viewport_snapshot_cache_ttl_seconds),
        )
        self._snapshot_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._refill_scan_cache_lock = threading.Lock()
        self._refill_scan_cache: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
=======
        self._snapshot_cache_ttl_seconds = 1.0
        self._snapshot_cache: dict[str, tuple[float, dict[str, Any]]] = {}
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

        self.kline_stream.add_kline_listener(self._enqueue_closed_kline_event)
        self.absorption_runtime.add_closed_record_listener(self._on_runtime_closed_record)
        self.agg_trade_stream.add_trade_listener(self._on_aggtrade_event)
        
        self.bin_sizer = AdaptiveBinSizer(runtime_config.absorption_lookback_candles)
        self._session_specs: dict[SessionKey, AbsorptionSessionSpec] = {}
        self._memories: dict[SessionKey, LatestCandleFootprintMemory] = {}
        self._price_steps: dict[str, Decimal] = {}
        self._latest_candles: dict[SessionKey, list[Any]] = {}
        self._closed_klines_by_session: dict[SessionKey, list[KlineClosedEvent]] = {}
        self._fixed_bin_size_by_symbol_timeframe: dict[tuple[str, str], Decimal] = {}
        self._frozen_bin_durations_ms: dict[SessionKey, dict[int, dict[int, int]]] = {}
        self._duration_profile_payloads: dict[SessionKey, dict[str, Any]] = {}
        self._level_volume_profile_payloads: dict[SessionKey, dict[str, Any]] = {}
        self._volume_zscore_candle_profiles: dict[SessionKey, dict[int, CandleBinVolumeProfile]] = {}
        self._volume_zscore_profile_payloads: dict[SessionKey, dict[str, Any]] = {}
        self._historical_klines_bootstrapped: set[SessionKey] = set()
        self._volume_zscore_rest_bootstrapped: set[SessionKey] = set()
        self._volume_zscore_update_tasks: dict[SessionKey, asyncio.Task] = {}
        self._last_kline_stream_restart_ms_by_symbol: dict[str, int] = {}
        self._footprint_update_seconds = float(getattr(runtime_config, "absorption_poll_seconds", 2.0))
        get_performance_metrics_recorder().set_snapshot_providers(
            active_keys_provider=self._performance_metric_active_keys,
            raw_trade_retained_event_count_provider=self._performance_metric_raw_trade_retained_event_count,
            raw_oldest_retained_age_ms_provider=self._performance_metric_raw_oldest_retained_age_ms,
            raw_retention_blocking_timeframe_provider=self._performance_metric_raw_retention_blocking_timeframe,
            kline_queue_size_provider=self._performance_metric_kline_queue_size,
        )
<<<<<<< HEAD
    
=======

>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def _on_aggtrade_event(self, event: AggTradeEvent) -> None:
        self.raw_event_buffer.append_trade_event(event)
        self.absorption_runtime.on_trade_event(event)

    def _enqueue_closed_kline_event(self, event: KlineClosedEvent) -> None:
        key = self._closed_kline_worker_key(event.symbol, event.internal_timeframe)
        if key not in self._active_closed_kline_worker_keys():
            return
        self._closed_kline_sequence += 1
        self._ensure_closed_kline_worker(key)
        queue = self._closed_kline_queues.setdefault(key, asyncio.PriorityQueue())
        queue.put_nowait(
            (
                int(event.close_time_ms),
                self._closed_kline_sequence,
                event,
            )
        )

    def _on_kline_closed_event(self, event: KlineClosedEvent) -> None:
        for key, spec in self._session_specs.items():
            if spec.binance_symbol.upper() != event.symbol.upper():
                continue
            if spec.timeframe.strip().upper() != event.internal_timeframe.strip().upper():
                continue

            self._merge_closed_candles(key, (event,))

    @staticmethod
    def _closed_kline_worker_key(symbol: str, timeframe: str) -> tuple[str, str]:
        return (symbol.strip().upper(), timeframe.strip().upper())

    def _active_closed_kline_worker_keys(self) -> set[tuple[str, str]]:
        return {
            self._closed_kline_worker_key(spec.binance_symbol, spec.timeframe)
            for spec in self._session_specs.values()
        }

    def _ensure_all_closed_kline_workers(self) -> None:
        for key in self._active_closed_kline_worker_keys():
            self._ensure_closed_kline_worker(key)

    def _ensure_closed_kline_worker(self, key: tuple[str, str]) -> None:
        task = self._closed_kline_tasks.get(key)
        if task is not None and not task.done():
            return
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return
        self._closed_kline_tasks[key] = asyncio.create_task(
            self._closed_kline_processing_loop(key),
            name=f"closed-kline-{key[0]}-{key[1]}",
        )

    def _cancel_stale_closed_kline_workers(self, active_keys: set[tuple[str, str]]) -> None:
        for key, task in list(self._closed_kline_tasks.items()):
            if key in active_keys:
                continue
            task.cancel()
            self._closed_kline_tasks.pop(key, None)
            self._closed_kline_queues.pop(key, None)
        for key in list(self._closed_kline_queues):
            if key not in active_keys:
                self._closed_kline_queues.pop(key, None)

    def configure_sessions(self, sessions: list[SymbolSessionState]) -> None:
        specs: dict[SessionKey, AbsorptionSessionSpec] = {}
        memories: dict[SessionKey, LatestCandleFootprintMemory] = {}
        bin_durations: dict[SessionKey, dict[int, dict[int, int]]] = {}

        for session in sessions:
<<<<<<< HEAD
            if not session.symbol_resolved:
                continue

            provider = session.market_provider or (PROVIDER_BINANCE if session.binance_symbol else "")
            if provider == PROVIDER_BINANCE and not session.binance_symbol:
                continue
            if provider == PROVIDER_CME_LOCAL_DBN:
                if not session.provider_symbol:
                    session.session_ready = False
                    session.status = "CME_SYMBOL_NOT_RESOLVED"
                    session.absorption_path_state.path_ready = False
                    continue
                if not self.cme_catalog.has_symbol(session.provider_symbol):
                    session.session_ready = False
                    session.status = "CME_LOCAL_DATA_NOT_AVAILABLE"
                    session.absorption_path_state.path_ready = False
                    continue

=======
            if not session.symbol_resolved or not session.binance_symbol:
                continue

>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            requested_timeframe = session.timeframe.strip().upper()
            if requested_timeframe not in KLINE_INTERVAL_BY_INTERNAL:
                session.session_ready = False
                session.status = "TIMEFRAME_NOT_SUPPORTED"
                session.absorption_path_state.path_ready = False
                LOGGER.warning(
                    "UNSUPPORTED_TIMEFRAME | mt5_symbol=%s | timeframe=%s",
                    session.mt5_symbol,
                    requested_timeframe,
                )
                continue

            session.session_ready = True
            session.status = "READY"
            session.absorption_path_state.path_ready = True

            interval = KLINE_INTERVAL_BY_INTERNAL[requested_timeframe]
<<<<<<< HEAD
            provider_symbol = session.provider_symbol or session.binance_symbol
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            session_key = make_session_key(
                session.mt5_symbol,
                requested_timeframe,
            )

            specs[session_key] = AbsorptionSessionSpec(
                mt5_symbol=session.mt5_symbol,
                binance_symbol=session.binance_symbol,
                timeframe=requested_timeframe,
                interval=interval,
<<<<<<< HEAD
                market_provider=provider,
                provider_symbol=provider_symbol,
                dataset=session.dataset,
                schema=session.schema,
                tick_size=session.tick_size,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            )

            existing_memory = self._memories.get(session_key)
            memory_candle_limit = self._study_memory_candles(requested_timeframe)
            if existing_memory is not None and existing_memory.max_candles == memory_candle_limit:
                memories[session_key] = existing_memory
            else:
                resized_memory = LatestCandleFootprintMemory(memory_candle_limit)
                if existing_memory is not None:
                    resized_memory.replace_window(existing_memory.snapshot())
                memories[session_key] = resized_memory

            bin_durations[session_key] = (
                self._frozen_bin_durations_ms.get(session_key)
                or {}
            )

        self._session_specs = specs
        self._memories = memories
        self._frozen_bin_durations_ms = bin_durations
        timeframes_by_binance_symbol: dict[str, set[str]] = {}
        for spec in specs.values():
<<<<<<< HEAD
            if spec.market_provider != PROVIDER_BINANCE:
                continue
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            timeframes_by_binance_symbol.setdefault(spec.binance_symbol.upper(), set()).add(
                spec.timeframe.strip().upper()
            )
        for binance_symbol, timeframes in timeframes_by_binance_symbol.items():
            self.raw_event_buffer.configure_symbol_timeframes(binance_symbol, timeframes)

        for key in list(self._latest_candles):
            if key not in specs:
                self._latest_candles.pop(key, None)
        for key in list(self._duration_profile_payloads):
            if key not in specs:
                self._duration_profile_payloads.pop(key, None)
        for key in list(self._level_volume_profile_payloads):
            if key not in specs:
                self._level_volume_profile_payloads.pop(key, None)
        for key in list(self._volume_zscore_profile_payloads):
            if key not in specs:
                self._volume_zscore_profile_payloads.pop(key, None)
        for key in list(self._volume_zscore_candle_profiles):
            if key not in specs:
                self._volume_zscore_candle_profiles.pop(key, None)
        for key, task in list(self._volume_zscore_update_tasks.items()):
            if key not in specs:
                task.cancel()
                self._volume_zscore_update_tasks.pop(key, None)
        self._historical_klines_bootstrapped = {
            key for key in self._historical_klines_bootstrapped if key in specs
        }
        self._volume_zscore_rest_bootstrapped = {
            key for key in self._volume_zscore_rest_bootstrapped if key in specs
        }
<<<<<<< HEAD
        active_binance_symbols = {
            spec.binance_symbol.upper()
            for spec in specs.values()
            if spec.market_provider == PROVIDER_BINANCE and spec.binance_symbol
        }
=======
        active_binance_symbols = {spec.binance_symbol.upper() for spec in specs.values()}
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self._cancel_stale_closed_kline_workers(self._active_closed_kline_worker_keys())
        self._last_kline_stream_restart_ms_by_symbol = {
            symbol: restarted_at
            for symbol, restarted_at in self._last_kline_stream_restart_ms_by_symbol.items()
            if symbol in active_binance_symbols
        }
                

        self.absorption_runtime.configure_sessions(sessions)

    async def run_forever(self) -> None:
<<<<<<< HEAD
=======
        get_performance_metrics_recorder().start()
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self._ensure_all_closed_kline_workers()
        await asyncio.gather(
            self._footprint_update_loop(),
            self._closed_kline_worker_supervisor(),
        )

    def _performance_metric_active_keys(self) -> tuple[tuple[str, str], ...]:
        return tuple(
            sorted(
                {
                    (spec.mt5_symbol, spec.timeframe.strip().upper())
                    for spec in self._session_specs.values()
                }
            )
        )

    def _performance_metric_raw_trade_retained_event_count(self, symbol: str, timeframe: str) -> int | None:
        session_key = make_session_key(symbol, timeframe)
        spec = self._session_specs.get(session_key)
        if spec is None:
            return None
        return self.raw_event_buffer.pending_trade_event_count(spec.binance_symbol, spec.timeframe)

    def _performance_metric_raw_oldest_retained_age_ms(self, symbol: str, timeframe: str) -> int | None:
        session_key = make_session_key(symbol, timeframe)
        spec = self._session_specs.get(session_key)
        if spec is None:
            return None
        oldest_time_ms = self.raw_event_buffer.oldest_retained_event_time_ms(spec.binance_symbol, spec.timeframe)
        if oldest_time_ms is None:
            return None
        return max(0, int(time.time() * 1000) - int(oldest_time_ms))

    def _performance_metric_raw_retention_blocking_timeframe(self, symbol: str, timeframe: str) -> str | None:
        session_key = make_session_key(symbol, timeframe)
        spec = self._session_specs.get(session_key)
        if spec is None:
            return None
        return self.raw_event_buffer.retention_blocking_timeframe(spec.binance_symbol, spec.timeframe)

    def _performance_metric_kline_queue_size(self, symbol: str, timeframe: str) -> int | None:
        pending_records = self.absorption_runtime.pending_closed_record_count(symbol, timeframe)
        session_key = make_session_key(symbol, timeframe)
        spec = self._session_specs.get(session_key)
        if spec is None:
            return None
        queue = self._closed_kline_queues.get(
            self._closed_kline_worker_key(spec.binance_symbol, spec.timeframe)
        )
        return int(queue.qsize() if queue is not None else 0) + int(pending_records or 0)

    async def _footprint_update_loop(self) -> None:
        while True:
            await self.update_once()
            await asyncio.sleep(self._footprint_update_seconds)

    async def _closed_kline_worker_supervisor(self) -> None:
        while True:
            self._ensure_all_closed_kline_workers()
            self._cancel_stale_closed_kline_workers(self._active_closed_kline_worker_keys())
            await asyncio.sleep(1.0)

    async def _closed_kline_processing_loop(self, key: tuple[str, str]) -> None:
        queue = self._closed_kline_queues.setdefault(key, asyncio.PriorityQueue())
        while True:
            _close_time_ms, _sequence, event = await queue.get()
            try:
                await self._process_closed_kline_event(event)
            except Exception:
                LOGGER.exception(
                    "CLOSED_KLINE_BATCH_PROCESSING_ERROR | symbol=%s | timeframe=%s | open_time_utc_ms=%d",
                    event.symbol,
                    event.internal_timeframe,
                    int(event.open_time_ms),
                )
            finally:
                queue.task_done()

    async def _process_closed_kline_event(self, event: KlineClosedEvent) -> None:
        self._on_kline_closed_event(event)
        await self._ensure_runtime_builders_for_event(event)
        self.absorption_runtime.process_closed_kline_batch(
            event=event,
            trade_events=(),
        )
        self._mark_raw_event_buffer_processed(event)
        self._snapshot_cache.clear()

    def _on_runtime_closed_record(
        self,
        *,
        mt5_symbol: str,
        timeframe_name: str,
        fixed_bin_size: Decimal,
        record: Any,
        bin_items: tuple[BinMarketData, ...],
    ) -> None:
        normalized_timeframe = timeframe_name.strip().upper()
        if normalized_timeframe not in STUDY_DISPLAY_TIMEFRAME_SET:
            return

        key = make_session_key(mt5_symbol, normalized_timeframe)
        spec = self._session_specs.get(key)
        memory = self._memories.get(key)
        if spec is None or memory is None:
            return

        footprint = self._canonical_record_to_footprint(
            spec=spec,
            fixed_bin_size=fixed_bin_size,
            record=record,
            bin_items=bin_items,
        )
        memory.replace_window(memory.snapshot() + [footprint])
        self._snapshot_cache.clear()

    def _canonical_record_to_footprint(
        self,
        *,
        spec: AbsorptionSessionSpec,
        fixed_bin_size: Decimal,
        record: Any,
        bin_items: tuple[BinMarketData, ...],
    ) -> CandleFootprint:
        l2_indices = set(record.l2_bins)
        bins = tuple(
            self._canonical_item_to_footprint_bin(item, fixed_bin_size)
            for item in sorted(bin_items, key=lambda value: value.bin_index)
            if item.bin_index in l2_indices
        )
<<<<<<< HEAD
        spike_metrics = calculate_contract_spike_metrics(
            item.total_volume for item in bins
        )
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        return CandleFootprint(
            symbol=spec.binance_symbol,
            mt5_symbol=spec.mt5_symbol,
            timeframe=spec.timeframe,
            interval=spec.interval,
            open_time_ms=int(record.open_time_ms),
            close_time_ms=int(record.close_time_ms),
            open_price=record.open_price or Decimal("0"),
            high_price=record.high_price or Decimal("0"),
            low_price=record.low_price or Decimal("0"),
            close_price=record.close_price or Decimal("0"),
            bin_size=fixed_bin_size,
            bins=bins,
            hvn_result=detect_hvns(bins),
<<<<<<< HEAD
            price_step=self._price_steps.get(spec.binance_symbol.upper()),
            contract_spike_score_deviation=spike_metrics.score_deviation,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        )

    @staticmethod
    def _canonical_item_to_footprint_bin(item: BinMarketData, fixed_bin_size: Decimal) -> FootprintBin:
        buy_volume = Decimal(str(item.ask_traded_volume))
        sell_volume = Decimal(str(item.bid_traded_volume))
        bin_low = Decimal(item.bin_index) * fixed_bin_size
        return FootprintBin(
            bin_low=bin_low,
            bin_high=bin_low + fixed_bin_size,
<<<<<<< HEAD
            bin_index=int(item.bin_index),
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            buy_volume=buy_volume,
            sell_volume=sell_volume,
            duration_ms=int(item.time_in_bin_ms),
            horizontal_delta=Decimal(str(item.horizontal_delta)),
            ask_traded_volume=buy_volume,
            bid_traded_volume=sell_volume,
            buy_diagonal_imbalance_ratio=Decimal(
                str(item.buy_diagonal_imbalance_ratio)
            ),
            sell_diagonal_imbalance_ratio=Decimal(
                str(item.sell_diagonal_imbalance_ratio)
            ),
            min_trade_price_in_bin=(
                Decimal(str(item.min_trade_price_in_bin))
                if item.min_trade_price_in_bin is not None
                else None
            ),
            max_trade_price_in_bin=(
                Decimal(str(item.max_trade_price_in_bin))
                if item.max_trade_price_in_bin is not None
                else None
            ),
            price_progress_in_bin=(
                Decimal(str(item.price_progress_in_bin))
                if item.price_progress_in_bin is not None
                else None
            ),
            dominant_diagonal_side=item.dominant_diagonal_side,
            dominant_side_volume=Decimal(str(item.dominant_side_volume)),
            dominant_side_efficiency=(
                Decimal(str(item.dominant_side_efficiency))
                if item.dominant_side_efficiency is not None
                else None
            ),
<<<<<<< HEAD
            contract_spike_score=Decimal(str(item.contract_spike_score)),
            abnormal_contract=bool(item.abnormal_contract),
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            volume_percentile=(
                Decimal(str(item.volume_percentile))
                if item.volume_percentile is not None
                else None
            ),
            is_volume_valid=bool(item.is_volume_valid),
            efficiency_percentile=(
                Decimal(str(item.efficiency_percentile))
                if item.efficiency_percentile is not None
                else None
            ),
            efficiency_zscore=(
                Decimal(str(item.efficiency_zscore))
                if item.efficiency_zscore is not None
                else None
            ),
            rejection_reason=item.rejection_reason,
        )

    async def _ensure_runtime_builders_for_event(self, event: KlineClosedEvent) -> None:
        matching_specs = [
            spec
            for spec in self._session_specs.values()
            if spec.binance_symbol.upper() == event.symbol.upper()
            and spec.timeframe.strip().upper() == event.internal_timeframe.strip().upper()
        ]
        if not matching_specs:
            return
        price_step = await self._get_price_step(event.symbol)
        for mt5_symbol in sorted({spec.mt5_symbol for spec in matching_specs}):
            fixed_bin_size_by_timeframe: dict[str, Decimal] = {}
            timeframe_name = event.internal_timeframe.strip().upper()
            session_key = make_session_key(mt5_symbol, timeframe_name)
            closed_candles = list(self._closed_klines_by_session.get(session_key, []))
            if not closed_candles:
                continue
            bin_size_key = (mt5_symbol, timeframe_name)
            fixed_bin_size = self._fixed_bin_size_by_symbol_timeframe.get(bin_size_key)
            if fixed_bin_size is None or fixed_bin_size <= 0:
                interval = KLINE_INTERVAL_BY_INTERNAL[timeframe_name]
                fixed_bin_size = self.bin_sizer.calculate(interval, closed_candles, price_step)
                self._fixed_bin_size_by_symbol_timeframe[bin_size_key] = fixed_bin_size
            fixed_bin_size_by_timeframe[timeframe_name] = fixed_bin_size
            if fixed_bin_size_by_timeframe:
                self.absorption_runtime.ensure_symbol_builders(
                    mt5_symbol=mt5_symbol,
                    fixed_bin_size_by_timeframe=fixed_bin_size_by_timeframe,
                    tick_size=price_step,
                )

    def _mark_raw_event_buffer_processed(self, event: KlineClosedEvent) -> None:
        self.raw_event_buffer.mark_timeframe_processed(
            event.symbol,
            event.internal_timeframe,
            int(event.open_time_ms),
            int(event.close_time_ms) + 1,
        )

<<<<<<< HEAD
    def _active_cme_process_symbols_for_timeframe(
        self,
        source_symbols: tuple[ProcessSymbol, ...],
        *,
        timeframe: str,
        interval: str,
    ) -> tuple[ProcessSymbol, ...]:
        normalized_timeframe = timeframe.strip().upper()
        active_provider_symbols: set[str] = set()
        active_mt5_symbols: set[str] = set()
        for spec in self._session_specs.values():
            if spec.market_provider != PROVIDER_CME_LOCAL_DBN:
                continue
            if spec.timeframe.strip().upper() != normalized_timeframe:
                continue
            provider_symbol = str(spec.provider_symbol or spec.binance_symbol or "").strip().upper()
            mt5_symbol = str(spec.mt5_symbol or "").strip().upper()
            if provider_symbol:
                active_provider_symbols.add(provider_symbol)
            if mt5_symbol:
                active_mt5_symbols.add(mt5_symbol)
        if active_provider_symbols or active_mt5_symbols:
            source_symbols = tuple(
                symbol
                for symbol in source_symbols
                if str(symbol.provider_symbol or "").strip().upper() in active_provider_symbols
                or str(symbol.mt5_symbol or "").strip().upper() in active_mt5_symbols
            )
        return _process_symbols_for_timeframe(
            source_symbols,
            timeframe=normalized_timeframe,
            interval=interval,
        )

    async def update_once(self) -> None:
        self._purge_expired_snapshot_cache()
        binance_specs = [
            spec
            for spec in self._session_specs.values()
            if spec.market_provider == PROVIDER_BINANCE and spec.binance_symbol
        ]
        active_symbols = {spec.binance_symbol for spec in binance_specs}
        active_symbol_timeframes = {
            (spec.binance_symbol.upper(), spec.timeframe.strip().upper())
            for spec in binance_specs
=======
    async def update_once(self) -> None:
        active_symbols = {spec.binance_symbol for spec in self._session_specs.values()}
        active_symbol_timeframes = {
            (spec.binance_symbol.upper(), spec.timeframe.strip().upper())
            for spec in self._session_specs.values()
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        }
        await self.agg_trade_stream.remove_inactive_symbols(active_symbols)
        await self.kline_stream.remove_inactive_symbol_timeframes(active_symbol_timeframes)
        self.raw_event_buffer.remove_inactive_symbols(active_symbols)
<<<<<<< HEAD
        for spec in list(binance_specs):
=======
        for spec in list(self._session_specs.values()):
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            try:
                await self._update_session(spec)
            except Exception:
                import logging
                logging.getLogger(__name__).exception(
                    "ABSORPTION_UPDATE_SESSION_ERROR | symbol=%s | timeframe=%s",
                    spec.binance_symbol,
                    spec.timeframe,
                )
        await self._heal_stale_m1_kline_streams(active_symbols, int(time.time() * 1000))

    async def _heal_stale_m1_kline_streams(self, active_symbols: set[str], now_ms: int) -> None:
        m1_interval_ms = self._interval_to_ms(KLINE_INTERVAL_BY_INTERNAL["M1"])
        restart_throttle_ms = m1_interval_ms * M1_KLINE_RESTART_MISSED_CANDLES

        for binance_symbol in sorted({symbol.upper() for symbol in active_symbols}):
            m1_specs = [
                spec
                for spec in self._session_specs.values()
                if spec.binance_symbol.upper() == binance_symbol
                and spec.timeframe.strip().upper() == "M1"
            ]
            latest_m1_close_time = self._latest_closed_kline_close_time_ms(m1_specs)
            if latest_m1_close_time is None:
                continue

            stale_after_ms = int(latest_m1_close_time) + 1 + restart_throttle_ms
            if now_ms <= stale_after_ms:
                continue

            last_restart_ms = self._last_kline_stream_restart_ms_by_symbol.get(binance_symbol, 0)
            if last_restart_ms > 0 and now_ms - last_restart_ms < restart_throttle_ms:
                continue

            active_timeframes = self._active_kline_timeframes_for_binance_symbol(binance_symbol)
            if not active_timeframes:
                continue

            LOGGER.warning(
                "BINANCE_KLINE_M1_STALE_RESTART | symbol=%s | latest_m1_close_time_utc_ms=%d | now_utc_ms=%d | missed_candles=%d | restarted_timeframes=%s",
                binance_symbol,
                int(latest_m1_close_time),
                int(now_ms),
                M1_KLINE_RESTART_MISSED_CANDLES,
                ",".join(active_timeframes),
            )
            await self.kline_stream.restart_symbol_timeframes(binance_symbol, active_timeframes)
            self._last_kline_stream_restart_ms_by_symbol[binance_symbol] = now_ms
            await self._backfill_recent_closed_klines_for_binance_symbol(binance_symbol, now_ms)

    def _latest_closed_kline_close_time_ms(self, specs: list[AbsorptionSessionSpec]) -> int | None:
        latest_close_time: int | None = None
        for spec in specs:
            closed_candles = self._closed_klines_by_session.get(make_session_key(spec.mt5_symbol, spec.timeframe), [])
            if not closed_candles:
                continue
            close_time = int(closed_candles[-1].close_time_ms)
            latest_close_time = close_time if latest_close_time is None else max(latest_close_time, close_time)
        return latest_close_time

    def _active_kline_timeframes_for_binance_symbol(self, binance_symbol: str) -> tuple[str, ...]:
        active_timeframes = {
            spec.timeframe.strip().upper()
            for spec in self._session_specs.values()
            if spec.binance_symbol.upper() == binance_symbol.upper()
        }
        return tuple(
            timeframe
            for timeframe in KLINE_INTERVAL_BY_INTERNAL
            if timeframe in active_timeframes
        )

    async def _backfill_recent_closed_klines_for_binance_symbol(self, binance_symbol: str, now_ms: int) -> None:
        runtime_events: dict[tuple[str, int], KlineClosedEvent] = {}
        for spec in list(self._session_specs.values()):
            if spec.binance_symbol.upper() != binance_symbol.upper():
                continue
            for event in await self._backfill_recent_closed_klines(spec=spec, now_ms=now_ms):
                runtime_events[(event.internal_timeframe, int(event.open_time_ms))] = event

        timeframe_order = list(KLINE_INTERVAL_BY_INTERNAL)
        for event in sorted(
            runtime_events.values(),
            key=lambda item: (
                int(item.open_time_ms),
                timeframe_order.index(item.internal_timeframe) if item.internal_timeframe in timeframe_order else 999,
            ),
        ):
            self._enqueue_closed_kline_event(event)

    async def _backfill_recent_closed_klines(
        self,
        *,
        spec: AbsorptionSessionSpec,
        now_ms: int,
    ) -> tuple[KlineClosedEvent, ...]:
        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        existing_candles = list(self._closed_klines_by_session.get(session_key, []))
        existing_open_times = {int(candle.open_time_ms) for candle in existing_candles}
        limit = self._recent_closed_kline_backfill_limit(spec=spec, now_ms=now_ms, existing_candles=existing_candles)

        try:
            candles = await self.client.get_klines(
                spec.binance_symbol,
                spec.interval,
                limit,
            )
        except Exception:
            LOGGER.exception(
                "KLINE_REST_CATCHUP_ERROR | symbol=%s | timeframe=%s",
                spec.binance_symbol,
                spec.timeframe,
            )
            return ()

        closed_events = [
            KlineClosedEvent(
                symbol=candle.symbol.upper(),
                internal_timeframe=spec.timeframe,
                binance_interval=spec.interval,
                open_time_ms=int(candle.open_time_ms),
                close_time_ms=int(candle.close_time_ms),
                open_price=candle.open_price,
                high_price=candle.high_price,
                low_price=candle.low_price,
                close_price=candle.close_price,
            )
            for candle in candles
            if int(candle.close_time_ms) < int(now_ms)
        ]
        if not closed_events:
            return ()

        self._merge_closed_candles(session_key, closed_events)
        return tuple(
            event
            for event in closed_events
            if int(event.open_time_ms) not in existing_open_times
        )

    def _recent_closed_kline_backfill_limit(
        self,
        *,
        spec: AbsorptionSessionSpec,
        now_ms: int,
        existing_candles: list[KlineClosedEvent],
    ) -> int:
        interval_ms = self._interval_to_ms(spec.interval)
        missing_count = 5
        if existing_candles and interval_ms > 0:
            latest_close_boundary_ms = int(existing_candles[-1].close_time_ms) + 1
            missing_count = max(0, int((int(now_ms) - latest_close_boundary_ms) // interval_ms)) + 3
        return min(
            1000,
            self._closed_candle_keep_count(spec.timeframe),
            max(5, missing_count),
        )

<<<<<<< HEAD
    def snapshot_payload(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        candle_limit: int | None = None,
        bin_tick_count: int | None = None,
    ) -> dict[str, Any]:
        normalized_timeframe = timeframe.strip().upper() if timeframe else None
        normalized_end_time = int(end_time_ms) if end_time_ms is not None else None
        normalized_limit = max(1, min(500, int(candle_limit or 20)))
        normalized_bin_tick_count = normalize_cme_bin_tick_count(bin_tick_count)
        cache_key = self._viewport_snapshot_cache_key(
            view="footprint",
            timeframe=normalized_timeframe,
            end_time_ms=normalized_end_time,
            candle_limit=normalized_limit,
            variant=f"{int(normalized_bin_tick_count)}t",
        )
        cached = self._cached_snapshot(cache_key)
        if cached is not None:
            return self._snapshot_with_cache_result(cached, hit=True)
        sessions = self._build_session_payloads(
            timeframe=normalized_timeframe,
            end_time_ms=normalized_end_time,
            candle_limit=normalized_limit,
            bin_tick_count=normalized_bin_tick_count,
        )
        viewport_window = any(
            bool(session.get("viewport_window"))
            for session in sessions
        )
        display_limits = dict(STUDY_DISPLAY_CANDLE_LIMITS)
        if viewport_window and normalized_timeframe in display_limits:
            display_limits[normalized_timeframe] = normalized_limit
        payload = {
            "type": "ABSORPTION_FOOTPRINT_SNAPSHOT",
            "memory_candles": (
                normalized_limit
                if viewport_window
                else max(STUDY_DISPLAY_CANDLE_LIMITS.values())
            ),
            "display_candles_by_timeframe": display_limits,
            "lookback_candles": self.runtime_config.absorption_lookback_candles,
            "full_history": False,
            "viewport_window": viewport_window,
            "earliest_window_start_ms": self._minimum_numeric_session_field(
                sessions,
                "earliest_window_start_ms",
            ),
            "window_start_ms": self._minimum_numeric_session_field(sessions, "window_start_ms"),
            "window_end_ms": self._maximum_numeric_session_field(sessions, "window_end_ms"),
            "latest_window_end_ms": self._maximum_numeric_session_field(
                sessions,
                "latest_window_end_ms",
            ),
            "window_candle_limit": normalized_limit,
            "bin_tick_count": int(normalized_bin_tick_count),
            "has_older_data": any(bool(session.get("has_older_data")) for session in sessions),
            "processed_trades": sum(int(session.get("processed_trades", 0)) for session in sessions),
            "generated_at_utc": int(time.time() * 1000),
            "signals": self._session_signals(sessions),
            "viewport_metrics": self._session_viewport_metrics(sessions),
            "sessions": sessions,
        }
        self._record_snapshot_cache_result(payload, hit=False)
        self._store_snapshot_cache(cache_key, payload)
        return payload

    def _cached_snapshot(self, cache_key: str) -> dict[str, Any] | None:
        self._purge_expired_snapshot_cache()
=======
    def snapshot_payload(self) -> dict[str, Any]:
        cached = self._cached_snapshot("footprint")
        if cached is not None:
            return cached
        payload = {
            "type": "ABSORPTION_FOOTPRINT_SNAPSHOT",
            "memory_candles": max(STUDY_DISPLAY_CANDLE_LIMITS.values()),
            "display_candles_by_timeframe": dict(STUDY_DISPLAY_CANDLE_LIMITS),
            "lookback_candles": self.runtime_config.absorption_lookback_candles,
            "generated_at_utc": int(time.time() * 1000),
            "sessions": self._build_session_payloads(),
        }
        self._store_snapshot_cache("footprint", payload)
        return payload

    def _cached_snapshot(self, cache_key: str) -> dict[str, Any] | None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        cached = self._snapshot_cache.get(cache_key)
        if cached is None:
            return None
        cached_at, payload = cached
        if time.monotonic() - cached_at > self._snapshot_cache_ttl_seconds:
            self._snapshot_cache.pop(cache_key, None)
            return None
        return payload

    def _store_snapshot_cache(self, cache_key: str, payload: dict[str, Any]) -> None:
<<<<<<< HEAD
        self._purge_expired_snapshot_cache()
        self._snapshot_cache[cache_key] = (time.monotonic(), payload)

    def _purge_expired_snapshot_cache(self) -> None:
        cutoff = time.monotonic() - self._snapshot_cache_ttl_seconds
        for key, (cached_at, _payload) in list(self._snapshot_cache.items()):
            if cached_at < cutoff:
                self._snapshot_cache.pop(key, None)

    def _viewport_snapshot_cache_key(
        self,
        *,
        view: str,
        timeframe: str | None,
        end_time_ms: int | None,
        candle_limit: int,
        variant: str,
    ) -> str:
        normalized_timeframe = (
            timeframe or STUDY_DEFAULT_DISPLAY_TIMEFRAME
        ).strip().upper()
        interval_ms = TIMEFRAME_MS_BY_NAME.get(normalized_timeframe, 1)
        provider_symbols = sorted(
            {
                spec.provider_symbol or spec.binance_symbol or spec.mt5_symbol
                for spec in self._session_specs.values()
                if spec.timeframe.strip().upper() == normalized_timeframe
            }
        )
        symbol_key = "+".join(provider_symbols) or "UNKNOWN"
        if end_time_ms is None:
            session_key = "LATEST"
            range_key = f"latest-{int(candle_limit)}"
        else:
            normalized_end = (int(end_time_ms) // interval_ms) * interval_ms
            end_candle = normalized_end // interval_ms
            start_candle = end_candle - int(candle_limit)
            session_key = trading_day_for_timestamp_ms(
                max(1, normalized_end - 1),
                session_start_hour_chicago=(
                    self.runtime_config.cme_trading_day_start_hour_chicago
                ),
            )
            range_key = f"{start_candle}-{end_candle}"
        return (
            f"{view}:{symbol_key}:{normalized_timeframe}:"
            f"{session_key}:{range_key}:{variant}"
        )

    @staticmethod
    def _record_snapshot_cache_result(
        payload: dict[str, Any],
        *,
        hit: bool,
    ) -> None:
        metrics = payload.setdefault("viewport_metrics", {})
        field = "cache_hit_count" if hit else "cache_miss_count"
        metrics[field] = int(metrics.get(field, 0)) + 1

    @staticmethod
    def _session_viewport_metrics(
        sessions: list[dict[str, Any]],
    ) -> dict[str, int]:
        fields = (
            "cache_hit_count",
            "cache_miss_count",
            "candle_rebuild_count",
            "footprint_rebuild_count",
            "cumulative_delta_cache_hit_count",
            "cumulative_delta_cache_miss_count",
        )
        return {
            field: sum(
                int(session.get("viewport_metrics", {}).get(field, 0))
                for session in sessions
            )
            for field in fields
        }

    @classmethod
    def _snapshot_with_cache_result(
        cls,
        payload: dict[str, Any],
        *,
        hit: bool,
    ) -> dict[str, Any]:
        result = dict(payload)
        result["viewport_metrics"] = {
            "cache_hit_count": 0,
            "cache_miss_count": 0,
            "candle_rebuild_count": 0,
            "footprint_rebuild_count": 0,
            "cumulative_delta_cache_hit_count": 0,
            "cumulative_delta_cache_miss_count": 0,
        }
        cls._record_snapshot_cache_result(result, hit=hit)
        return result

=======
        self._snapshot_cache[cache_key] = (time.monotonic(), payload)

>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    async def _update_session(self, spec: AbsorptionSessionSpec) -> None:
        price_step = await self._get_price_step(
            spec.binance_symbol,
        )

        session_key = make_session_key(
            spec.mt5_symbol,
            spec.timeframe,
        )

        now_ms = int(time.time() * 1000)
        await self._ensure_historical_closed_klines(
            spec=spec,
            now_ms=now_ms,
        )

        closed_candles = list(
            self._closed_klines_by_session.get(
                session_key,
                [],
            )
        )

        if not closed_candles:
            await self.kline_stream.ensure_symbol_timeframe(
                spec.binance_symbol,
                spec.timeframe,
            )
            await self.agg_trade_stream.ensure_symbol(
                spec.binance_symbol,
            )
            return

        bin_size_key = (
            spec.mt5_symbol,
            spec.timeframe.strip().upper(),
        )

        fixed_bin_size = self._fixed_bin_size_by_symbol_timeframe.get(
            bin_size_key,
        )

        if fixed_bin_size is None or fixed_bin_size <= 0:
            fixed_bin_size = self.bin_sizer.calculate(
                spec.interval,
                closed_candles,
                price_step,
            )

            self._fixed_bin_size_by_symbol_timeframe[
                bin_size_key
            ] = fixed_bin_size

        bin_size = fixed_bin_size

        fixed_bin_size_by_timeframe: dict[str, Decimal] = {
            spec.timeframe.strip().upper(): fixed_bin_size
        }

        if not self.absorption_runtime.has_symbol_builders(
            spec.mt5_symbol,
        ):
            self.absorption_runtime.ensure_symbol_builders(
                mt5_symbol=spec.mt5_symbol,
                fixed_bin_size_by_timeframe=fixed_bin_size_by_timeframe,
                tick_size=price_step,
            )

        self._latest_candles[session_key] = self._prune_latest_candles(
            timeframe=spec.timeframe,
            candles=closed_candles,
            closed_candles=closed_candles,
            now_ms=now_ms,
        )
        await self.kline_stream.ensure_symbol_timeframe(
            spec.binance_symbol,
            spec.timeframe,
        )
        await self.agg_trade_stream.ensure_symbol(
            spec.binance_symbol,
        )

    async def _get_price_step(self, symbol: str) -> Decimal:
        price_step = self._price_steps.get(symbol)
        if price_step is None:
            price_step = await self.client.get_price_step(symbol)
            self._price_steps[symbol] = price_step
        return price_step

    async def _update_duration_profile(
        self,
        *,
        spec: AbsorptionSessionSpec,
        closed_candles: list[Any],
        price_step: Decimal,
        now_ms: int,
    ) -> None:
        if not closed_candles:
            return

        candle = closed_candles[-1]
        close_boundary_ms = int(candle.close_time_ms) + 1
        freeze_delay_ms = int(getattr(self.runtime_config, "absorption_duration_profile_freeze_delay_ms", 700))
        if now_ms < close_boundary_ms + freeze_delay_ms:
            return

        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        existing_payload = self._duration_profile_payloads.get(session_key)
        if (
            existing_payload is not None
            and int(existing_payload.get("candle_open_time_utc_ms", -1)) == int(candle.open_time_ms)
        ):
            return

        price_events = await self.agg_trade_stream.snapshot_price_events_with_previous_for_time_range(
            spec.binance_symbol,
            int(candle.open_time_ms),
            close_boundary_ms,
        )
        if price_events is None:
            return

        profile = build_duration_profile(
            symbol=spec.mt5_symbol,
            timeframe=spec.timeframe,
            candle_open_time_utc_ms=int(candle.open_time_ms),
            candle_close_time_utc_ms=int(candle.close_time_ms),
            price_step=price_step,
            price_events=price_events,
            fallback_open_price=candle.open_price,
        )
        self._duration_profile_payloads[session_key] = profile.to_payload()

    async def _update_level_volume_profile(
        self,
        *,
        spec: AbsorptionSessionSpec,
        closed_candles: list[Any],
        price_step: Decimal,
        now_ms: int,
    ) -> None:
        if not closed_candles:
            return

        candle = closed_candles[-1]
        close_boundary_ms = int(candle.close_time_ms) + 1
        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        existing_payload = self._level_volume_profile_payloads.get(session_key)
        duration_payload = self._duration_profile_payloads.get(session_key)
        if (
            duration_payload is None
            or int(duration_payload.get("candle_open_time_utc_ms", -1)) != int(candle.open_time_ms)
        ):
            if (
                existing_payload is not None
                and int(existing_payload.get("candle_open_time_utc_ms", -1)) != int(candle.open_time_ms)
            ):
                self._level_volume_profile_payloads.pop(session_key, None)
            return

        if (
            existing_payload is not None
            and int(existing_payload.get("candle_open_time_utc_ms", -1)) == int(candle.open_time_ms)
        ):
            return

        trades = await self.agg_trade_stream.websocket_trades_for_time_range(
            spec.binance_symbol,
            int(candle.open_time_ms),
            close_boundary_ms,
        )
        if trades is None:
            return

        profile = build_level_volume_profile(
            symbol=spec.mt5_symbol,
            timeframe=spec.timeframe,
            candle_open_time_utc_ms=int(candle.open_time_ms),
            candle_close_time_utc_ms=int(candle.close_time_ms),
            price_step=price_step,
            agg_trades=trades,
        )
        self._level_volume_profile_payloads[session_key] = profile.to_payload()


    async def _update_volume_zscore_profile(
        self,
        *,
        spec: AbsorptionSessionSpec,
        closed_candles: list[Any],
        fixed_bin_size: Decimal,
        now_ms: int,
    ) -> None:
        del now_ms
        baseline_count = self._volume_zscore_baseline_candles()
        required_count = baseline_count + 1
        if len(closed_candles) < required_count:
            return

        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        window = list(closed_candles[-required_count:])
        current_candle = window[-1]
        existing_payload = self._volume_zscore_profile_payloads.get(session_key)
        if (
            existing_payload is not None
            and int(existing_payload.get("candle_open_time_utc_ms", -1)) == int(current_candle.open_time_ms)
        ):
            return

        profiles = await self._ensure_volume_zscore_profiles_for_window(
            spec=spec,
            candles=window,
            fixed_bin_size=fixed_bin_size,
        )
        if profiles is None:
            return

        try:
            current_profile = profiles[int(current_candle.open_time_ms)]
            baseline_profiles = [
                profiles[int(candle.open_time_ms)]
                for candle in window[:-1]
            ]
        except KeyError:
            return

        profile = build_volume_zscore_profile(
            current_profile=current_profile,
            baseline_profiles=baseline_profiles,
            target_baseline_candles=baseline_count,
            volume_floor=Decimal(str(getattr(self.runtime_config, "volume_zscore_volume_floor", 0.01))),
            z_cap=Decimal(str(getattr(self.runtime_config, "volume_zscore_z_cap", 5.0))),
        )
        self._volume_zscore_profile_payloads[session_key] = profile.to_payload()
        self._prune_volume_zscore_candle_profiles(session_key, closed_candles)

    def _schedule_volume_zscore_profile_update(
        self,
        *,
        spec: AbsorptionSessionSpec,
        closed_candles: list[Any] | None = None,
        fixed_bin_size: Decimal | None = None,
    ) -> None:
        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        task = self._volume_zscore_update_tasks.get(session_key)
        if task is not None and not task.done():
            return

        self._volume_zscore_update_tasks[session_key] = asyncio.create_task(
            self._run_volume_zscore_profile_update(
                spec=spec,
                closed_candles=closed_candles,
                fixed_bin_size=fixed_bin_size,
            ),
            name=f"volume-zscore-profile-{spec.binance_symbol}-{spec.timeframe}",
        )

    async def _run_volume_zscore_profile_update(
        self,
        *,
        spec: AbsorptionSessionSpec,
        closed_candles: list[Any] | None,
        fixed_bin_size: Decimal | None,
    ) -> None:
        try:
            now_ms = int(time.time() * 1000)
            price_step = await self._get_price_step(spec.binance_symbol)
            await self._ensure_historical_closed_klines(
                spec=spec,
                now_ms=now_ms,
            )

            session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
            candles = list(closed_candles or self._closed_klines_by_session.get(session_key, []))
            if len(candles) < self._volume_zscore_required_candles():
                return

            bin_size = fixed_bin_size
            bin_size_key = (spec.mt5_symbol, spec.timeframe.strip().upper())
            if bin_size is None or bin_size <= 0:
                bin_size = self._fixed_bin_size_by_symbol_timeframe.get(bin_size_key)
            if bin_size is None or bin_size <= 0:
                bin_size = self.bin_sizer.calculate(
                    spec.interval,
                    candles,
                    price_step,
                )
                self._fixed_bin_size_by_symbol_timeframe[bin_size_key] = bin_size

            await self._update_volume_zscore_profile(
                spec=spec,
                closed_candles=candles,
                fixed_bin_size=bin_size,
                now_ms=now_ms,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception(
                "VOLUME_ZSCORE_PROFILE_UPDATE_ERROR | symbol=%s | timeframe=%s",
                spec.binance_symbol,
                spec.timeframe,
            )

    async def _ensure_volume_zscore_profiles_for_window(
        self,
        *,
        spec: AbsorptionSessionSpec,
        candles: list[Any],
        fixed_bin_size: Decimal,
    ) -> dict[int, CandleBinVolumeProfile] | None:
        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        profiles = self._volume_zscore_candle_profiles.setdefault(session_key, {})
        missing_candles = [
            candle
            for candle in candles
            if int(candle.open_time_ms) not in profiles
        ]
        if not missing_candles:
            return profiles

        current_candle = candles[-1]
        baseline_ready = all(int(candle.open_time_ms) in profiles for candle in candles[:-1])
        if (
            session_key in self._volume_zscore_rest_bootstrapped
            and baseline_ready
            and len(missing_candles) == 1
            and int(missing_candles[0].open_time_ms) == int(current_candle.open_time_ms)
        ):
            close_boundary_ms = int(current_candle.close_time_ms) + 1
            trades = await self.agg_trade_stream.websocket_trades_for_time_range(
                spec.binance_symbol,
                int(current_candle.open_time_ms),
                close_boundary_ms,
            )
            if trades is not None:
                profiles[int(current_candle.open_time_ms)] = self._build_volume_zscore_candle_profile(
                    spec=spec,
                    candle=current_candle,
                    fixed_bin_size=fixed_bin_size,
                    agg_trades=trades,
                )
                return profiles

            await self._backfill_volume_zscore_profiles_from_rest(
                spec=spec,
                candles=[current_candle],
                fixed_bin_size=fixed_bin_size,
            )
            return profiles if int(current_candle.open_time_ms) in profiles else None

        await self._backfill_volume_zscore_profiles_from_rest(
            spec=spec,
            candles=candles,
            fixed_bin_size=fixed_bin_size,
        )
        return profiles if all(int(candle.open_time_ms) in profiles for candle in candles) else None

    async def _backfill_volume_zscore_profiles_from_rest(
        self,
        *,
        spec: AbsorptionSessionSpec,
        candles: list[Any],
        fixed_bin_size: Decimal,
    ) -> None:
        if not candles:
            return

        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        start_time_ms = int(candles[0].open_time_ms)
        end_time_ms = int(candles[-1].close_time_ms)
        try:
            raw_trades = await self.client.get_agg_trades(
                spec.binance_symbol,
                start_time_ms,
                end_time_ms,
            )
        except Exception:
            LOGGER.exception(
                "VOLUME_ZSCORE_AGGTRADE_BACKFILL_ERROR | symbol=%s | timeframe=%s",
                spec.binance_symbol,
                spec.timeframe,
            )
            return

        trades = [
            self._agg_trade_event_from_rest_payload(spec.binance_symbol, item)
            for item in raw_trades
        ]
        profiles = self._volume_zscore_candle_profiles.setdefault(session_key, {})
        for candle in candles:
            candle_open_time_ms = int(candle.open_time_ms)
            close_boundary_ms = int(candle.close_time_ms) + 1
            candle_trades = [
                trade
                for trade in trades
                if candle_open_time_ms <= int(trade.event_time_ms) < close_boundary_ms
            ]
            profiles[candle_open_time_ms] = self._build_volume_zscore_candle_profile(
                spec=spec,
                candle=candle,
                fixed_bin_size=fixed_bin_size,
                agg_trades=candle_trades,
            )
        self._volume_zscore_rest_bootstrapped.add(session_key)

    def _build_volume_zscore_candle_profile(
        self,
        *,
        spec: AbsorptionSessionSpec,
        candle: Any,
        fixed_bin_size: Decimal,
        agg_trades: list[Any],
    ) -> CandleBinVolumeProfile:
        return build_candle_bin_volume_profile(
            symbol=spec.mt5_symbol,
            timeframe=spec.timeframe,
            candle_open_time_utc_ms=int(candle.open_time_ms),
            candle_close_time_utc_ms=int(candle.close_time_ms),
            fixed_bin_size=fixed_bin_size,
            agg_trades=agg_trades,
        )

    @staticmethod
    def _agg_trade_event_from_rest_payload(symbol: str, payload: dict[str, Any]) -> AggTradeEvent:
        return AggTradeEvent(
            symbol=symbol.upper(),
            event_time_ms=int(payload.get("T") or payload.get("E") or 0),
            price=Decimal(str(payload["p"])),
            quantity=Decimal(str(payload["q"])),
            side="sell" if bool(payload.get("m")) else "buy",
        )

    async def _ensure_historical_closed_klines(
        self,
        *,
        spec: AbsorptionSessionSpec,
        now_ms: int,
    ) -> None:
        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        if session_key in self._historical_klines_bootstrapped:
            return

        limit = max(
            int(getattr(self.runtime_config, "absorption_lookback_candles", 150)),
            self._study_memory_candles(spec.timeframe),
            self._volume_zscore_required_candles(),
        ) + 2
        try:
            candles = await self.client.get_klines(
                spec.binance_symbol,
                spec.interval,
                limit,
            )
        except Exception:
            LOGGER.exception(
                "HISTORICAL_KLINE_BACKFILL_ERROR | symbol=%s | timeframe=%s",
                spec.binance_symbol,
                spec.timeframe,
            )
            return

        closed_events = [
            KlineClosedEvent(
                symbol=candle.symbol.upper(),
                internal_timeframe=spec.timeframe,
                binance_interval=spec.interval,
                open_time_ms=int(candle.open_time_ms),
                close_time_ms=int(candle.close_time_ms),
                open_price=candle.open_price,
                high_price=candle.high_price,
                low_price=candle.low_price,
                close_price=candle.close_price,
            )
            for candle in candles
            if int(candle.close_time_ms) < int(now_ms)
        ]
        self._merge_closed_candles(session_key, closed_events)
        self._historical_klines_bootstrapped.add(session_key)

    def _merge_closed_candles(
        self,
        session_key: SessionKey,
        candles: tuple[Any, ...] | list[Any],
    ) -> None:
        if not candles:
            return

        merged = {
            int(candle.open_time_ms): candle
            for candle in self._closed_klines_by_session.get(session_key, [])
        }
        for candle in candles:
            merged[int(candle.open_time_ms)] = candle
        ordered_candles = [
            item
            for _, item in sorted(merged.items(), key=lambda pair: pair[0])
        ]
        keep_count = self._closed_candle_keep_count(session_key[1])
        if len(ordered_candles) > keep_count:
            ordered_candles = ordered_candles[-keep_count:]
        self._closed_klines_by_session[session_key] = ordered_candles

    def _prune_volume_zscore_candle_profiles(
        self,
        session_key: SessionKey,
        closed_candles: list[Any],
    ) -> None:
        profiles = self._volume_zscore_candle_profiles.get(session_key)
        if not profiles:
            return
        keep_open_times = {
            int(candle.open_time_ms)
            for candle in closed_candles[-self._closed_candle_keep_count(session_key[1]) :]
        }
        for open_time_ms in list(profiles):
            if open_time_ms not in keep_open_times:
                profiles.pop(open_time_ms, None)

    def _volume_zscore_baseline_candles(self) -> int:
        return max(1, int(getattr(self.runtime_config, "volume_zscore_baseline_candles", 15)))

    def _volume_zscore_required_candles(self) -> int:
        return self._volume_zscore_baseline_candles() + 1

    def _closed_candle_keep_count(self, timeframe: str | None = None) -> int:
        return max(
            self._study_memory_candles(timeframe) + int(self.runtime_config.absorption_lookback_candles) + 5,
            self._volume_zscore_required_candles() + 5,
        )

    async def _freeze_closed_bin_duration_snapshots(
        self,
        spec: AbsorptionSessionSpec,
        closed_candles: list[Any],
        bin_size: Decimal,
    ) -> None:
        session_key = make_session_key(spec.mt5_symbol, spec.timeframe)
        duration_snapshots = self._frozen_bin_durations_ms.setdefault(session_key, {})
        visible_open_times = {item.open_time_ms for item in self._study_closed_window(closed_candles, spec.timeframe)}
        stale_keys = [key for key in duration_snapshots if key not in visible_open_times]
        for key in stale_keys:
            duration_snapshots.pop(key, None)

        for candle in self._study_closed_window(closed_candles, spec.timeframe):
            if candle.open_time_ms in duration_snapshots:
                continue
            price_events = await self.agg_trade_stream.snapshot_price_events_for_time_range(
                spec.binance_symbol,
                candle.open_time_ms,
                candle.close_time_ms,
            )
            if price_events is None:
                continue
            duration_snapshots[candle.open_time_ms] = self._calculate_bin_durations_ms(
                candle=candle,
                bin_size=bin_size,
                price_events=price_events,
            )

    @staticmethod
    def _calculate_bin_durations_ms(
        *,
        candle: Any,
        bin_size: Decimal,
        price_events: list[tuple[int, Decimal]],
    ) -> dict[int, int]:
        if bin_size <= 0:
            return {}

        candle_open_time_ms = int(candle.open_time_ms)
        candle_close_time_ms = int(candle.close_time_ms)
        durations_ms: dict[int, int] = {}
        active_bin_index: int | None = None
        active_since_ms = 0

        for event_time_ms, price in sorted(price_events, key=lambda item: item[0]):
            event_time_ms = int(event_time_ms)
            if event_time_ms < candle_open_time_ms:
                event_time_ms = candle_open_time_ms
            if event_time_ms > candle_close_time_ms:
                break

            bin_index = int(((price - candle.low_price) / bin_size).to_integral_value(rounding=ROUND_FLOOR))
            if bin_index < 0:
                bin_index = 0

            if active_bin_index is None:
                active_bin_index = bin_index
                active_since_ms = event_time_ms
                continue

            if event_time_ms < active_since_ms:
                continue

            if bin_index != active_bin_index:
                elapsed_ms = event_time_ms - active_since_ms
                if elapsed_ms > 0:
                    durations_ms[active_bin_index] = durations_ms.get(active_bin_index, 0) + elapsed_ms
                active_bin_index = bin_index
                active_since_ms = event_time_ms

        if active_bin_index is not None:
            elapsed_ms = candle_close_time_ms - active_since_ms
            if elapsed_ms > 0:
                durations_ms[active_bin_index] = durations_ms.get(active_bin_index, 0) + elapsed_ms

        return durations_ms

<<<<<<< HEAD
    def _build_session_payloads(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        candle_limit: int = 20,
        bin_tick_count: Decimal = CME_BIN_TICK_COUNT,
    ) -> list[dict[str, Any]]:
        sessions = []
        now_ms = int(time.time() * 1000)
        normalized_timeframe = timeframe.strip().upper() if timeframe else None
=======
    def _build_session_payloads(self) -> list[dict[str, Any]]:
        sessions = []
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

        for key, spec in self._session_specs.items():
            if spec.timeframe.strip().upper() not in STUDY_DISPLAY_TIMEFRAME_SET:
                continue
<<<<<<< HEAD
            if normalized_timeframe is not None and spec.timeframe.strip().upper() != normalized_timeframe:
                continue

            if spec.market_provider == PROVIDER_CME_LOCAL_DBN:
                if normalized_timeframe is None:
                    continue
                sessions.append(
                    self._build_cme_footprint_session_payload(
                        spec,
                        end_time_ms=end_time_ms,
                        candle_limit=candle_limit,
                        bin_tick_count=bin_tick_count,
                    )
                )
                continue
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

            memory = self._memories.get(key)
            fixed_bin_size = self._fixed_bin_size_by_symbol_timeframe.get(
                (spec.mt5_symbol, spec.timeframe.strip().upper())
            )

            sessions.append(
                {
                    "mt5_symbol": spec.mt5_symbol,
                    "binance_symbol": spec.binance_symbol,
<<<<<<< HEAD
                    "provider_symbol": spec.provider_symbol or spec.binance_symbol,
                    "market_provider": spec.market_provider,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                    "timeframe": spec.timeframe,
                    "interval": spec.interval,
                    "fixed_bin_size": str(fixed_bin_size) if fixed_bin_size is not None else None,
                    "candles": memory.to_payload() if memory else [],
<<<<<<< HEAD
                    "live_candle": self._live_candle_payload(
                        spec=spec,
                        fixed_bin_size=fixed_bin_size,
                        now_ms=now_ms,
                    ),
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                }
            )

        return sessions

<<<<<<< HEAD
    def _build_cme_footprint_session_payload(
        self,
        spec: AbsorptionSessionSpec,
        *,
        end_time_ms: int | None = None,
        candle_limit: int = 20,
        bin_tick_count: Decimal = CME_BIN_TICK_COUNT,
    ) -> dict[str, Any]:
        tick_size = self._cme_tick_size(spec)
        normalized_bin_tick_count = normalize_cme_bin_tick_count(bin_tick_count)
        try:
            payload = self.cme_paged_history_engine.footprint_window(
                provider_symbol=spec.provider_symbol,
                mt5_symbol=spec.mt5_symbol,
                timeframe=spec.timeframe,
                tick_size=tick_size,
                bin_tick_count=normalized_bin_tick_count,
                end_time_ms=end_time_ms,
                candle_limit=candle_limit,
            )
            payload.update(
                {
                    "binance_symbol": "",
                    "dataset": spec.dataset,
                    "schema": spec.schema,
                    "status": "READY",
                    "error": "",
                }
            )
            return payload
        except CmeLocalDataError as exc:
            return {
                "mt5_symbol": spec.mt5_symbol,
                "binance_symbol": "",
                "provider_symbol": spec.provider_symbol,
                "market_provider": PROVIDER_CME_LOCAL_DBN,
                "quantity_unit": "CONTRACTS",
                "dataset": spec.dataset,
                "schema": spec.schema,
                "timeframe": spec.timeframe,
                "interval": spec.interval,
                "price_step": str(tick_size),
                "bin_tick_count": int(normalized_bin_tick_count),
                "fixed_bin_size": str(tick_size * normalized_bin_tick_count),
                "status": "CME_LOCAL_DATA_READER_NOT_READY",
                "error": str(exc),
                "viewport_window": True,
                "earliest_window_start_ms": 0,
                "window_start_ms": 0,
                "window_end_ms": 0,
                "latest_window_end_ms": 0,
                "window_candle_limit": candle_limit,
                "has_older_data": False,
                "processed_trades": 0,
                "candles": [],
                "live_candle": None,
            }

    def dom_timeline_payload(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        start_time_ms: int | None = None,
        price_min: Decimal | None = None,
        price_max: Decimal | None = None,
        selected_date: str | None = None,
        iceberg_min_contracts: int | None = None,
        iceberg_order_ids: tuple[str, ...] = (),
        iceberg_path_start_ms: int | None = None,
        iceberg_path_end_ms: int | None = None,
    ) -> dict[str, Any]:
        normalized_timeframe = (timeframe or STUDY_DEFAULT_DISPLAY_TIMEFRAME).strip().upper()
        sessions = [
            self._build_dom_timeline_session_payload(
                spec,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                price_min=price_min,
                price_max=price_max,
                selected_date=selected_date,
                iceberg_min_contracts=iceberg_min_contracts,
                iceberg_order_ids=iceberg_order_ids,
                iceberg_path_start_ms=iceberg_path_start_ms,
                iceberg_path_end_ms=iceberg_path_end_ms,
            )
            for spec in self._session_specs.values()
            if spec.market_provider == PROVIDER_CME_LOCAL_DBN
            and spec.timeframe.strip().upper() == normalized_timeframe
        ]
        payload = {
            "type": "DOM_TIMELINE_SNAPSHOT",
            "timeframe": normalized_timeframe,
            "full_history": False,
            "viewport_window": True,
            "earliest_window_start_ms": self._minimum_numeric_session_field(
                sessions,
                "earliest_window_start_ms",
            ),
            "window_start_ms": self._minimum_numeric_session_field(sessions, "window_start_ms"),
            "window_end_ms": self._maximum_numeric_session_field(sessions, "window_end_ms"),
            "latest_window_end_ms": self._maximum_numeric_session_field(
                sessions,
                "latest_window_end_ms",
            ),
            "has_older_data": any(bool(session.get("has_older_data")) for session in sessions),
            "processed_events": sum(
                int(session.get("debug", {}).get("mbo_event_count", 0))
                for session in sessions
            ),
            "generated_at_utc": int(time.time() * 1000),
            "viewport_metrics": self._dom_session_viewport_metrics(sessions),
            "sessions": sessions,
        }
        return payload

    def _build_dom_timeline_session_payload(
        self,
        spec: AbsorptionSessionSpec,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        price_min: Decimal | None = None,
        price_max: Decimal | None = None,
        selected_date: str | None = None,
        iceberg_min_contracts: int | None = None,
        iceberg_order_ids: tuple[str, ...] = (),
        iceberg_path_start_ms: int | None = None,
        iceberg_path_end_ms: int | None = None,
    ) -> dict[str, Any]:
        tick_size = self._cme_tick_size(spec)
        trigger_timeout_candles = self._dom_trigger_timeout_candles()
        timeframe_ms = int(TIMEFRAME_MS_BY_NAME[spec.timeframe.strip().upper()])
        context = DomContext(
            mt5_symbol=spec.mt5_symbol,
            provider_symbol=spec.provider_symbol,
            market_provider=spec.market_provider,
            timeframe=spec.timeframe,
            interval=spec.interval,
            dataset=spec.dataset,
            schema="mbo",
            tick_size=tick_size,
            session_start_hour_chicago=self.runtime_config.cme_trading_day_start_hour_chicago,
            timezone=self.runtime_config.dom_timezone,
            trigger_timeout_candles=trigger_timeout_candles,
            initial_view_candles=self.runtime_config.dom_initial_view_candles,
            retention_ms=timeframe_ms * trigger_timeout_candles,
            data_dir=self.runtime_config.project_root / self.runtime_config.dom_data_dir_name,
        )
        try:
            payload = self.dom_timeline_engine.timeline_window(
                context,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                price_min=price_min,
                price_max=price_max,
                selected_date=selected_date,
                iceberg_min_contracts=iceberg_min_contracts,
                iceberg_order_ids=iceberg_order_ids,
                iceberg_path_start_ms=iceberg_path_start_ms,
                iceberg_path_end_ms=iceberg_path_end_ms,
            )
            payload.update(
                {
                    "new_york_session_timezone": "America/New_York",
                    "new_york_session_start_hour": self.runtime_config.cme_new_york_session_start_hour,
                    "new_york_session_start_minute": self.runtime_config.cme_new_york_session_start_minute,
                    "new_york_session_end_hour": self.runtime_config.cme_new_york_session_end_hour,
                    "new_york_session_end_minute": self.runtime_config.cme_new_york_session_end_minute,
                }
            )
            self._publish_dom_timeline_payload_to_trigger_engine(payload, spec)
            self._publish_dom_timeline_payload_to_actor_proxy_engine(payload)
            return payload
        except CmeLocalDataError as exc:
            payload = {
                "type": "DOM_TIMELINE_SESSION",
                "mt5_symbol": spec.mt5_symbol,
                "symbol": spec.provider_symbol,
                "provider_symbol": spec.provider_symbol,
                "market_provider": PROVIDER_CME_LOCAL_DBN,
                "dataset": spec.dataset,
                "schema": "mbo",
                "timeframe": spec.timeframe,
                "interval": spec.interval,
                "timezone": self.runtime_config.dom_timezone,
                "session_start_hour_chicago": self.runtime_config.cme_trading_day_start_hour_chicago,
                "new_york_session_timezone": "America/New_York",
                "new_york_session_start_hour": self.runtime_config.cme_new_york_session_start_hour,
                "new_york_session_start_minute": self.runtime_config.cme_new_york_session_start_minute,
                "new_york_session_end_hour": self.runtime_config.cme_new_york_session_end_hour,
                "new_york_session_end_minute": self.runtime_config.cme_new_york_session_end_minute,
                "price_step": str(tick_size),
                "tick_size": str(tick_size),
                "quantity_unit": "CONTRACTS",
                "trigger_timeout_candles": trigger_timeout_candles,
                "retention_ms": timeframe_ms * trigger_timeout_candles,
                "data_dir": str(self.runtime_config.project_root / self.runtime_config.dom_data_dir_name),
                "dom_files": [],
                "dom_file_count": 0,
                "contract_symbols": [],
                "viewport_window": True,
                "window_start_ms": 0,
                "window_end_ms": 0,
                "render_start_ms": 0,
                "render_end_ms": 0,
                "navigation_start_ms": 0,
                "navigation_end_ms": 0,
                "selected_date": selected_date or "",
                "earliest_window_start_ms": 0,
                "latest_window_end_ms": 0,
                "has_older_data": False,
                "status": "DOM_LOCAL_DATA_READER_NOT_READY",
                "message": str(exc),
                "events": [],
                "resting_segments": [],
                "best_bid_line": [],
                "best_ask_line": [],
                "order_book_levels": [],
                "time_bucket_ms": max(1, timeframe_ms // max(1, int(self.runtime_config.dom_time_bucket_divisor))),
                "viewport_metrics": {
                    "cache_hit_count": 0,
                    "cache_miss_count": 1,
                    "mbo_event_count": 0,
                },
                "debug": {
                    "requested_start_ms": int(start_time_ms) if start_time_ms is not None else 0,
                    "requested_end_ms": int(end_time_ms) if end_time_ms is not None else 0,
                    "plan_start_ms": 0,
                    "plan_end_ms": 0,
                    "render_start_ms": 0,
                    "render_end_ms": 0,
                    "buffer_start_ms": 0,
                    "buffer_end_ms": 0,
                    "cache_hit_count": 0,
                    "provider_event_count": 0,
                    "visible_event_count": 0,
                    "dom_file_count": 0,
                    "mbo_event_count": 0,
                    "symbol": spec.provider_symbol,
                    "timeframe": spec.timeframe,
                    "tick_size": str(tick_size),
                    "price_level_count": 0,
                    "time_bucket_count": 0,
                    "add_count": 0,
                    "cancel_delete_count": 0,
                    "modify_count": 0,
                    "execute_count": 0,
                    "last_bid": "",
                    "last_ask": "",
                },
            }
            self._publish_dom_timeline_payload_to_trigger_engine(payload, spec)
            self._publish_dom_timeline_payload_to_actor_proxy_engine(payload)
            return payload

    def _publish_dom_timeline_payload_to_trigger_engine(
        self,
        payload: Mapping[str, Any],
        spec: AbsorptionSessionSpec,
    ) -> None:
        published_outputs = self.engine_output_store.publish_from_payload(payload)
        if published_outputs:
            self._snapshot_cache.clear()
        snapshot = self.trigger_engine.set_dom_output_snapshot(
            payload,
            symbol=spec.mt5_symbol,
            provider_symbol=spec.provider_symbol,
            timeframe=spec.timeframe,
        )
        if snapshot is None:
            self.trigger_engine.clear_order_book_snapshot(
                symbol=spec.mt5_symbol,
                provider_symbol=spec.provider_symbol,
                timeframe=spec.timeframe,
            )

    def _publish_dom_timeline_payload_to_actor_proxy_engine(
        self,
        payload: Mapping[str, Any],
    ) -> None:
        raw_events = payload.get("raw_events")
        if not isinstance(raw_events, Iterable) or isinstance(raw_events, (str, bytes, Mapping)):
            return
        for raw_event in raw_events:
            if not isinstance(raw_event, Mapping):
                continue
            if not str(raw_event.get("order_id") or raw_event.get("venue_order_id") or "").strip():
                continue
            if not str(raw_event.get("action") or raw_event.get("event_type") or "").strip():
                continue
            self.actor_proxy_engine.on_raw_dom_event(RawDomEvent.from_mapping(raw_event))

    @staticmethod
    def _dom_session_viewport_metrics(
        sessions: list[dict[str, Any]],
    ) -> dict[str, int]:
        fields = (
            "cache_hit_count",
            "cache_miss_count",
            "mbo_event_count",
        )
        return {
            field: sum(
                int(session.get("viewport_metrics", {}).get(field, 0))
                for session in sessions
            )
            for field in fields
        }

    @staticmethod
    def _dom_trigger_timeout_candles() -> int:
        return max(1, int(REFERENCE_TIMEOUT_CANDLES))

    def warmup_historic_status_payload(self) -> dict[str, Any]:
        payload = self.warmup_historic_catalog.status_payload()
        payload["enabled"] = self.runtime_config.warmup_historic_enabled
        payload["default_days"] = self.runtime_config.warmup_historic_default_days
        return payload

    def warmup_historic_files_for_symbol(
        self,
        *,
        provider_symbol: str = "",
        mt5_symbol: str = "",
        market_provider: str = PROVIDER_CME_LOCAL_DBN,
    ) -> dict[str, Any]:
        files = self.warmup_historic_catalog.files_for_symbol(
            provider_symbol=provider_symbol,
            mt5_symbol=mt5_symbol,
            market_provider=market_provider,
        )
        return {
            "type": "WARMUP_HISTORIC_FILES_FOR_SYMBOL",
            "provider_symbol": provider_symbol,
            "mt5_symbol": mt5_symbol,
            "market_provider": market_provider,
            "file_count": len(files),
            "dom_files": [
                file.to_payload(root_dir=self.warmup_historic_catalog.root_dir)
                for file in files
                if file.data_type == "dom"
            ],
            "l2_files": [
                file.to_payload(root_dir=self.warmup_historic_catalog.root_dir)
                for file in files
                if file.data_type == "l2"
            ],
        }

    def data_process_replay_payload(
        self,
        *,
        timeframe: str | None = None,
        start_vancouver: str,
        end_vancouver: str,
    ) -> dict[str, Any]:
        normalized_timeframe = (
            timeframe or STUDY_DEFAULT_DISPLAY_TIMEFRAME
        ).strip().upper()
        if normalized_timeframe not in STUDY_DISPLAY_TIMEFRAME_SET:
            raise ValueError(f"unsupported timeframe: {normalized_timeframe}")

        replay_range = parse_vancouver_replay_range(
            start=start_vancouver,
            end=end_vancouver,
        )
        interval = KLINE_INTERVAL_BY_INTERNAL[normalized_timeframe]
        source = DomDatabentoReplaySource(
            index_path=(
                self.runtime_config.project_root
                / self.runtime_config.dom_data_dir_name
                / self.runtime_config.dom_extracted_cache_dir_name
                / "dom_timeline_index.sqlite3"
            ),
            dataset=self.runtime_config.cme_dataset,
            schema="mbo",
            market_provider=PROVIDER_CME_LOCAL_DBN,
            default_tick_size=self.runtime_config.cme_default_tick_size,
            timeframe=normalized_timeframe,
            interval=interval,
            batch_size=self.runtime_config.dom_dbn_batch_size,
        )
        footprint_source = CmeFootprintReplaySource(
            catalog=self.cme_catalog,
            trade_store=self.cme_trade_store,
            dataset=self.runtime_config.cme_dataset,
            schema=self.runtime_config.cme_schema,
            market_provider=PROVIDER_CME_LOCAL_DBN,
            timeframe=normalized_timeframe,
            interval=interval,
            bin_tick_count=CME_BIN_TICK_COUNT,
            output_decimal_places=self.cme_paged_history_engine.config.output_decimal_places,
            duration_unit_ms=self.cme_paged_history_engine.config.duration_unit_ms,
            session_start_hour_chicago=(
                self.runtime_config.cme_trading_day_start_hour_chicago
            ),
        )
        symbols = self._active_cme_process_symbols_for_timeframe(
            source.symbols(),
            timeframe=normalized_timeframe,
            interval=interval,
        )
        log_dir = self.runtime_config.project_root / "runtime_metrics"
        payload_log_path = log_dir / "data_process_payloads.csv"
        trigger_log_path = log_dir / "data_process_trigger_signals.csv"
        run_log_path = log_dir / "data_process_replay_runs.csv"
        csv_sink = CsvProcessLogSink(
            payload_log_path,
            trigger_path=trigger_log_path,
        )
        replay_trigger_engine = TriggerEngine(self.trigger_engine.config)
        engine = DataProcessEngine(
            event_source=source,
            footprint_source=footprint_source,
            config=DataProcessConfig(emit_price_activity_levels=True),
            sinks=(
                csv_sink,
            ),
        )
        result = engine.run_replay(
            ProcessReplayRequest(
                start_ms=replay_range.start_ms,
                end_ms=replay_range.end_ms,
                symbols=symbols,
            )
        )
        replay_candles: list[dict[str, Any]] = []
        for snapshot in result.footprints:
            for candle in snapshot.candles:
                replay_candle = dict(candle)
                replay_candle.setdefault("mt5_symbol", snapshot.symbol.mt5_symbol)
                replay_candle.setdefault(
                    "provider_symbol",
                    snapshot.symbol.provider_symbol,
                )
                replay_candle.setdefault(
                    "market_provider",
                    snapshot.symbol.market_provider,
                )
                replay_candle.setdefault("timeframe", snapshot.symbol.timeframe)
                replay_candles.append(replay_candle)
        trigger_signals = _enrich_replay_candles_chronologically(
            trigger_engine=replay_trigger_engine,
            replay_candles=replay_candles,
            replay_payloads=result.payloads,
            evaluation_time_ms=replay_range.end_ms,
            record_closed_positions=False,
        )
        csv_sink.record_trigger_signals(trigger_signals)
        if result.emitted_payload_count > 0:
            self._snapshot_cache.clear()
        self._record_data_process_replay_run(
            run_log_path=run_log_path,
            timeframe=normalized_timeframe,
            replay_range=replay_range,
            processed_event_count=result.processed_event_count,
            emitted_payload_count=result.emitted_payload_count,
            footprint_candle_count=result.footprint_candle_count,
            symbols=result.symbols,
        )
        return {
            "type": "DATA_PROCESS_REPLAY_RESULT",
            "status": "OK",
            "timezone": VANCOUVER_TIMEZONE,
            "timeframe": normalized_timeframe,
            "start_vancouver": replay_range.start_vancouver,
            "end_vancouver": replay_range.end_vancouver,
            "start_utc": replay_range.start_utc,
            "end_utc": replay_range.end_utc,
            "start_ms": replay_range.start_ms,
            "end_ms": replay_range.end_ms,
            "symbols": [
                {
                    "provider_symbol": symbol.provider_symbol,
                    "mt5_symbol": symbol.mt5_symbol,
                    "market_provider": symbol.market_provider,
                    "timeframe": symbol.timeframe,
                }
                for symbol in result.symbols
            ],
            "processed_event_count": result.processed_event_count,
            "emitted_payload_count": result.emitted_payload_count,
            "footprint_candle_count": result.footprint_candle_count,
            "footprint_symbols": [
                {
                    "provider_symbol": snapshot.symbol.provider_symbol,
                    "mt5_symbol": snapshot.symbol.mt5_symbol,
                    "market_provider": snapshot.symbol.market_provider,
                    "timeframe": snapshot.symbol.timeframe,
                    "candle_count": snapshot.candle_count,
                }
                for snapshot in result.footprints
            ],
            "payload_log_path": str(payload_log_path),
            "trigger_log_path": str(trigger_log_path),
            "run_log_path": str(run_log_path),
            "payloads": list(result.payloads),
            "trigger_signal_count": len(trigger_signals),
            "trigger_signals": trigger_signals,
        }

    def data_process_refill_scan_payload(
        self,
        *,
        timeframe: str | None = None,
        start_vancouver: str,
        end_vancouver: str,
        refill_min: int,
        contracts_min: int = 0,
        activity_filter: str = "",
        rate_min: float | None = None,
        spike_score_min: Decimal | str | float | None = None,
    ) -> dict[str, Any]:
        normalized_timeframe = (
            timeframe or STUDY_DEFAULT_DISPLAY_TIMEFRAME
        ).strip().upper()
        if normalized_timeframe not in STUDY_DISPLAY_TIMEFRAME_SET:
            raise ValueError(f"unsupported timeframe: {normalized_timeframe}")

        minimum_refill_count = max(0, int(refill_min))
        normalized_activity_filter = str(activity_filter or "").strip().upper()
        if normalized_activity_filter and (
            len(normalized_activity_filter) < 2
            or normalized_activity_filter[0] not in {"O", "A", "B"}
            or not normalized_activity_filter[1:].isdigit()
        ):
            raise ValueError("activity_filter must use O, A, or B followed by a non-negative integer")
        minimum_execution_rate = None if rate_min is None else float(rate_min)
        if minimum_execution_rate is not None and not 0.0 <= minimum_execution_rate <= 100.0:
            raise ValueError("rate_min must be between 0 and 100")
        minimum_spike_score = (
            None if spike_score_min is None else Decimal(str(spike_score_min))
        )
        if minimum_spike_score is not None and not minimum_spike_score.is_finite():
            raise ValueError("spike_score_min must be a finite number")
        replay_range = parse_vancouver_replay_range(
            start=start_vancouver,
            end=end_vancouver,
        )
        interval = KLINE_INTERVAL_BY_INTERNAL[normalized_timeframe]
        source = DomDatabentoReplaySource(
            index_path=(
                self.runtime_config.project_root
                / self.runtime_config.dom_data_dir_name
                / self.runtime_config.dom_extracted_cache_dir_name
                / "dom_timeline_index.sqlite3"
            ),
            dataset=self.runtime_config.cme_dataset,
            schema="mbo",
            market_provider=PROVIDER_CME_LOCAL_DBN,
            default_tick_size=self.runtime_config.cme_default_tick_size,
            timeframe=normalized_timeframe,
            interval=interval,
            batch_size=max(100_000, int(self.runtime_config.dom_dbn_batch_size)),
            footprint_score_min=None,
        )
        footprint_source = CmeFootprintReplaySource(
            catalog=self.cme_catalog,
            trade_store=self.cme_trade_store,
            dataset=self.runtime_config.cme_dataset,
            schema=self.runtime_config.cme_schema,
            market_provider=PROVIDER_CME_LOCAL_DBN,
            timeframe=normalized_timeframe,
            interval=interval,
            bin_tick_count=CME_BIN_TICK_COUNT,
            output_decimal_places=self.cme_paged_history_engine.config.output_decimal_places,
            duration_unit_ms=self.cme_paged_history_engine.config.duration_unit_ms,
            session_start_hour_chicago=(
                self.runtime_config.cme_trading_day_start_hour_chicago
            ),
        )
        symbols = self._active_cme_process_symbols_for_timeframe(
            source.symbols(),
            timeframe=normalized_timeframe,
            interval=interval,
        )
        warmup_start_ms = max(
            0,
            replay_range.start_ms - DATA_PROCESS_REFILL_SCAN_WARMUP_MS,
        )
        index_version = _stable_dom_index_version(source.index_path)
        cache_key = (
            DATA_PROCESS_REFILL_SCAN_CACHE_VERSION,
            normalized_timeframe,
            int(replay_range.start_ms),
            int(replay_range.end_ms),
            int(warmup_start_ms),
            (
                format(minimum_spike_score, "f")
                if minimum_spike_score is not None
                else None
            ),
            index_version,
            tuple(
                (
                    symbol.provider_symbol,
                    symbol.mt5_symbol,
                    symbol.market_provider,
                    symbol.dataset,
                    symbol.schema,
                    str(symbol.tick_size),
                    tuple(symbol.contract_symbols),
                )
                for symbol in symbols
            ),
        )
        analytics_signature = RefillScanIndex.signature(
            (
                DATA_PROCESS_REFILL_SCAN_CACHE_VERSION,
                normalized_timeframe,
                index_version,
                cache_key[-1],
            )
        )
        analytics_index = RefillScanIndex(
            self.runtime_config.project_root
            / self.runtime_config.dom_data_dir_name
            / self.runtime_config.dom_extracted_cache_dir_name
            / "refill_scan_index_v2.sqlite3"
        )
        with self._refill_scan_cache_lock:
            cached_replay = self._refill_scan_cache.get(cache_key)
            if cached_replay is not None:
                self._refill_scan_cache.move_to_end(cache_key)
        analytics_cache_hit = False
        if cached_replay is None:
            cached_replay = analytics_index.load(
                signature=analytics_signature,
                start_ms=replay_range.start_ms,
                end_ms=replay_range.end_ms,
            )
            analytics_cache_hit = cached_replay is not None
        if cached_replay is None:
            cached_replay = _load_refill_scan_disk_cache(
                self.runtime_config.project_root,
                cache_key,
            )
        cache_hit = cached_replay is not None
        if (
            cached_replay is not None
            and not analytics_cache_hit
            and minimum_spike_score is None
        ):
            analytics_index.store(
                signature=analytics_signature,
                start_ms=replay_range.start_ms,
                end_ms=replay_range.end_ms,
                cached_replay=cached_replay,
                fields=REFILL_SCAN_DISK_CACHE_FIELDS,
            )
        if cached_replay is not None:
            with self._refill_scan_cache_lock:
                self._refill_scan_cache[cache_key] = cached_replay
                self._refill_scan_cache.move_to_end(cache_key)
                while len(self._refill_scan_cache) > DATA_PROCESS_REFILL_SCAN_CACHE_SIZE:
                    self._refill_scan_cache.popitem(last=False)
        if cached_replay is None:
            engine = DataProcessEngine(
                event_source=source,
                footprint_source=footprint_source,
                config=DataProcessConfig(
                    max_payloads=2_000_000,
                    emit_individual_refill_orders=False,
                    emit_price_activity_levels=True,
                    collect_event_payloads=False,
                    filter_price_activity_to_footprints=True,
                    events_are_time_ordered=True,
                    deduplicate_events=False,
                ),
                sinks=(),
            )
            result = engine.run_replay(
                ProcessReplayRequest(
                    start_ms=warmup_start_ms,
                    end_ms=replay_range.end_ms,
                    emit_start_ms=replay_range.start_ms,
                    symbols=symbols,
                )
            )
            aggregate_payloads = _aggregate_refill_scan_payloads(
                result.payloads,
                start_ms=replay_range.start_ms,
                end_ms=replay_range.end_ms,
            )
            cached_replay = {
                "aggregate_payloads": aggregate_payloads,
                "footprints": result.footprints,
                "processed_event_count": result.processed_event_count,
                "emitted_payload_count": result.emitted_payload_count,
                "footprint_candle_count": result.footprint_candle_count,
                "footprint_symbols": tuple(
                    {
                        "provider_symbol": snapshot.symbol.provider_symbol,
                        "mt5_symbol": snapshot.symbol.mt5_symbol,
                        "market_provider": snapshot.symbol.market_provider,
                        "timeframe": snapshot.symbol.timeframe,
                        "candle_count": snapshot.candle_count,
                    }
                    for snapshot in result.footprints
                ),
            }
            cached_replay["spike_score_payloads"] = (
                _spike_score_scan_payloads(
                    result.footprints,
                    aggregate_payloads,
                    score_min=minimum_spike_score,
                    start_ms=replay_range.start_ms,
                    end_ms=replay_range.end_ms,
                )
                if minimum_spike_score is not None
                else ()
            )
            analytics_index.store(
                signature=analytics_signature,
                start_ms=replay_range.start_ms,
                end_ms=replay_range.end_ms,
                cached_replay=cached_replay,
                fields=REFILL_SCAN_DISK_CACHE_FIELDS,
            )
            with self._refill_scan_cache_lock:
                self._refill_scan_cache[cache_key] = cached_replay
                self._refill_scan_cache.move_to_end(cache_key)
                while len(self._refill_scan_cache) > DATA_PROCESS_REFILL_SCAN_CACHE_SIZE:
                    self._refill_scan_cache.popitem(last=False)
        aggregate_payloads = tuple(cached_replay["aggregate_payloads"])
        aggregate_matches = [
            payload
            for payload in aggregate_payloads
            if (
                _payload_refill_count(payload) >= minimum_refill_count
                and (
                    minimum_refill_count > 0
                    or bool(payload.get("has_price_activity"))
                )
            )
        ]
        if normalized_activity_filter:
            aggregate_matches = [
                payload for payload in aggregate_matches
                if _matches_refill_activity_filter(payload, normalized_activity_filter)
            ]
        if minimum_execution_rate is not None:
            aggregate_matches = [
                payload for payload in aggregate_matches
                if _displayed_execution_rate(payload) >= minimum_execution_rate
            ]
        matched_payloads = sorted(
            aggregate_matches,
            key=lambda item: (
                _payload_int(item, "marker_time_ms", "timestamp_ms"),
                str(item.get("side") or ""),
                Decimal(str(item.get("marker_price") or item.get("price") or "0")),
                str(item.get("order_id") or ""),
            ),
        )
        matched_level_keys = {
            (
                _payload_text(payload, "provider_symbol", "symbol").upper(),
                _payload_text(payload, "timeframe").upper(),
                _payload_int(payload, "marker_time_ms", "timestamp_ms"),
                _payload_text(payload, "marker_price", "price"),
            )
            for payload in matched_payloads
        }
        matched_side_keys = {
            _refill_scan_group_key(payload)
            for payload in matched_payloads
        }
        summary_payloads = sorted(
            (
                payload
                for payload in aggregate_payloads
                if (
                    _payload_text(payload, "provider_symbol", "symbol").upper(),
                    _payload_text(payload, "timeframe").upper(),
                    _payload_int(payload, "marker_time_ms", "timestamp_ms"),
                    _payload_text(payload, "marker_price", "price"),
                ) in matched_level_keys
                and _refill_scan_group_key(payload) not in matched_side_keys
            ),
            key=lambda item: (
                _payload_int(item, "marker_time_ms", "timestamp_ms"),
                str(item.get("side") or ""),
                Decimal(str(item.get("marker_price") or item.get("price") or "0")),
                str(item.get("order_id") or ""),
            ),
        )
        spike_score_payloads = ()
        if minimum_spike_score is not None:
            spike_score_payloads = tuple(cached_replay.get("spike_score_payloads") or ())
            if not spike_score_payloads:
                cached_footprints = tuple(cached_replay.get("footprints") or ())
                if not cached_footprints:
                    cached_footprints = tuple(
                        ProcessFootprintSnapshot(
                            symbol=symbol,
                            candles=tuple(
                                footprint_source.candles(
                                    symbol,
                                    start_ms=warmup_start_ms,
                                    end_ms=replay_range.end_ms,
                                )
                            ),
                        )
                        for symbol in symbols
                    )
                spike_score_payloads = _spike_score_scan_payloads(
                    cached_footprints,
                    aggregate_payloads,
                    score_min=minimum_spike_score,
                    start_ms=replay_range.start_ms,
                    end_ms=replay_range.end_ms,
                )
        return {
            "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
            "status": "OK",
            "timezone": VANCOUVER_TIMEZONE,
            "timeframe": normalized_timeframe,
            "refill_min": minimum_refill_count,
            "activity_filter": normalized_activity_filter,
            "rate_min": minimum_execution_rate,
            "spike_score_min": (
                format(minimum_spike_score, "f")
                if minimum_spike_score is not None
                else None
            ),
            "start_vancouver": replay_range.start_vancouver,
            "end_vancouver": replay_range.end_vancouver,
            "start_utc": replay_range.start_utc,
            "end_utc": replay_range.end_utc,
            "start_ms": replay_range.start_ms,
            "end_ms": replay_range.end_ms,
            "warmup_start_ms": warmup_start_ms,
            "cache_hit": cache_hit,
            "symbols": [
                {
                    "provider_symbol": symbol.provider_symbol,
                    "mt5_symbol": symbol.mt5_symbol,
                    "market_provider": symbol.market_provider,
                    "timeframe": symbol.timeframe,
                }
                for symbol in symbols
            ],
            "processed_event_count": int(cached_replay["processed_event_count"]),
            "emitted_payload_count": int(cached_replay["emitted_payload_count"]),
            "matched_payload_count": len(matched_payloads),
            "footprint_candle_count": int(cached_replay["footprint_candle_count"]),
            "footprint_symbols": list(cached_replay["footprint_symbols"]),
            "payloads": [
                _refill_scan_response_payload(payload)
                for payload in matched_payloads
            ],
            "summary_payloads": [
                _refill_scan_response_payload(payload)
                for payload in summary_payloads
            ],
            "spike_score_payload_count": len(spike_score_payloads),
            "spike_score_payloads": list(spike_score_payloads),
        }

    def data_process_delete_scan_payload(
        self,
        *,
        timeframe: str | None = None,
        start_vancouver: str,
        end_vancouver: str,
        side: str,
        delete_min: int = 1,
        contracts_min: int = 0,
    ) -> dict[str, Any]:
        normalized_timeframe = (timeframe or STUDY_DEFAULT_DISPLAY_TIMEFRAME).strip().upper()
        if normalized_timeframe not in STUDY_DISPLAY_TIMEFRAME_SET:
            raise ValueError(f"unsupported timeframe: {normalized_timeframe}")
        normalized_side = _normalized_dom_side(side)
        if normalized_side not in {"ASK", "BID"}:
            raise ValueError(f"unsupported delete side: {side}")
        minimum_delete_count = max(1, int(delete_min))
        minimum_contract_count = max(0, int(contracts_min))

        replay_range = parse_vancouver_replay_range(start=start_vancouver, end=end_vancouver)
        interval = KLINE_INTERVAL_BY_INTERNAL[normalized_timeframe]
        interval_ms = int(TIMEFRAME_MS_BY_NAME[normalized_timeframe])
        source = DomDatabentoReplaySource(
            index_path=(
                self.runtime_config.project_root
                / self.runtime_config.dom_data_dir_name
                / self.runtime_config.dom_extracted_cache_dir_name
                / "dom_timeline_index.sqlite3"
            ),
            dataset=self.runtime_config.cme_dataset,
            schema="mbo",
            market_provider=PROVIDER_CME_LOCAL_DBN,
            default_tick_size=self.runtime_config.cme_default_tick_size,
            timeframe=normalized_timeframe,
            interval=interval,
            batch_size=self.runtime_config.dom_dbn_batch_size,
        )
        symbols = self._active_cme_process_symbols_for_timeframe(
            source.symbols(),
            timeframe=normalized_timeframe,
            interval=interval,
        )
        warmup_start_ms = max(0, replay_range.start_ms - DATA_PROCESS_DELETE_SCAN_WARMUP_MS)
        output_decimal_places = self.cme_paged_history_engine.config.output_decimal_places
        groups: dict[tuple[str, str, int, str, str], dict[str, Any]] = {}
        processed_event_count = 0

        def record_delete(
            *,
            symbol: ProcessSymbol,
            timestamp_ms: int,
            price: Decimal,
            order_side: str,
            contracts: int,
            delete_kind: str,
        ) -> None:
            if contracts <= 0 or order_side != normalized_side:
                return
            normalized_delete_kind = str(delete_kind or "").strip().upper()
            if normalized_delete_kind not in {"C", "M"}:
                normalized_delete_kind = "C"
            if timestamp_ms < replay_range.start_ms or timestamp_ms >= replay_range.end_ms:
                return
            marker_time_ms = (int(timestamp_ms) // interval_ms) * interval_ms
            marker_price = _format_scan_price(price, output_decimal_places)
            key = (
                symbol.provider_symbol.strip().upper(),
                symbol.timeframe.strip().upper(),
                marker_time_ms,
                marker_price,
                order_side,
            )
            payload = groups.get(key)
            if payload is None:
                payload = _delete_scan_payload(
                    provider_symbol=symbol.provider_symbol,
                    mt5_symbol=symbol.mt5_symbol,
                    market_provider=symbol.market_provider,
                    timeframe=symbol.timeframe,
                    marker_time_ms=marker_time_ms,
                    marker_price=marker_price,
                    side=order_side,
                )
                groups[key] = payload
            if normalized_delete_kind == "M":
                payload["m_delete_count"] = int(payload["m_delete_count"]) + 1
                payload["m_deleted_contracts"] = int(payload["m_deleted_contracts"]) + int(contracts)
            else:
                payload["c_delete_count"] = int(payload["c_delete_count"]) + 1
                payload["c_deleted_contracts"] = int(payload["c_deleted_contracts"]) + int(contracts)
            payload["delete_count"] = (
                int(payload["c_delete_count"])
                + int(payload["m_delete_count"])
            )
            payload["deleted_contracts"] = (
                int(payload["c_deleted_contracts"])
                + int(payload["m_deleted_contracts"])
            )
            payload["refill_count"] = int(payload["delete_count"])
            payload["refill_filled_contracts"] = int(payload["deleted_contracts"])
            payload["positive_refill_count"] = int(payload["delete_count"])
            payload["positive_refill_filled_total"] = int(payload["deleted_contracts"])

        for symbol in symbols:
            orders: dict[str, _DeleteScanOrder] = {}
            for event in source.events(symbol, start_ms=warmup_start_ms, end_ms=replay_range.end_ms):
                processed_event_count += 1
                action = str(event.action or "").strip().upper()
                order_id = str(event.order_id or "").strip()
                event_side = _normalized_dom_side(event.side)
                event_size = max(0, int(event.size or 0))
                if action in CLEAR_ACTIONS:
                    orders.clear()
                    continue
                if not order_id:
                    continue
                order = orders.get(order_id)
                if action in ADD_ACTIONS:
                    if event.price is not None:
                        orders[order_id] = _DeleteScanOrder(price=event.price, side=event_side, size=event_size)
                    continue
                if action in FILL_ACTIONS:
                    if order is not None:
                        order.size = max(0, int(order.size) - min(event_size, int(order.size)))
                        if event.price is not None:
                            order.price = event.price
                        if event_side in {"ASK", "BID"}:
                            order.side = event_side
                        if order.size <= 0:
                            orders.pop(order_id, None)
                    continue
                if action in MODIFY_ACTIONS:
                    if event.price is None and order is None:
                        continue
                    if order is None:
                        orders[order_id] = _DeleteScanOrder(
                            price=event.price or Decimal("0"),
                            side=event_side,
                            size=event_size,
                        )
                        continue
                    old_price = order.price
                    old_side = order.side
                    old_size = max(0, int(order.size))
                    new_price = event.price if event.price is not None else old_price
                    new_side = event_side if event_side in {"ASK", "BID"} else old_side
                    new_size = event_size
                    moved_level = new_price != old_price or new_side != old_side
                    deleted_contracts = old_size if moved_level else max(0, old_size - new_size)
                    record_delete(
                        symbol=symbol,
                        timestamp_ms=int(event.ts_event_ms),
                        price=old_price,
                        order_side=old_side,
                        contracts=deleted_contracts,
                        delete_kind="M",
                    )
                    if new_size > 0:
                        order.price = new_price
                        order.side = new_side
                        order.size = new_size
                    else:
                        orders.pop(order_id, None)
                    continue
                if action in CANCEL_ACTIONS:
                    if order is None:
                        if event.price is not None and event_size > 0 and event_side in {"ASK", "BID"}:
                            record_delete(
                                symbol=symbol,
                                timestamp_ms=int(event.ts_event_ms),
                                price=event.price,
                                order_side=event_side,
                                contracts=event_size,
                                delete_kind="C",
                            )
                        continue
                    old_size = max(0, int(order.size))
                    deleted_contracts = event_size if event_size > 0 else old_size
                    record_delete(
                        symbol=symbol,
                        timestamp_ms=int(event.ts_event_ms),
                        price=order.price,
                        order_side=order.side,
                        contracts=deleted_contracts,
                        delete_kind="C",
                    )
                    remaining = old_size - deleted_contracts
                    if remaining > 0 and action == "C":
                        order.size = remaining
                    else:
                        orders.pop(order_id, None)

        matched_payloads = [
            payload
            for payload in groups.values()
            if int(payload.get("delete_count") or 0) >= minimum_delete_count
            and int(payload.get("deleted_contracts") or 0) >= minimum_contract_count
        ]
        payloads = sorted(
            matched_payloads,
            key=lambda item: (
                _payload_int(item, "marker_time_ms", "timestamp_ms"),
                Decimal(str(item.get("marker_price") or item.get("price") or "0")),
                str(item.get("side") or ""),
            ),
        )
        return {
            "type": "DATA_PROCESS_DELETE_SCAN_RESULT",
            "status": "OK",
            "timezone": VANCOUVER_TIMEZONE,
            "timeframe": normalized_timeframe,
            "side": normalized_side,
            "delete_min": minimum_delete_count,
            "contracts_min": minimum_contract_count,
            "start_vancouver": replay_range.start_vancouver,
            "end_vancouver": replay_range.end_vancouver,
            "start_utc": replay_range.start_utc,
            "end_utc": replay_range.end_utc,
            "start_ms": replay_range.start_ms,
            "end_ms": replay_range.end_ms,
            "warmup_start_ms": warmup_start_ms,
            "symbols": [
                {
                    "provider_symbol": symbol.provider_symbol,
                    "mt5_symbol": symbol.mt5_symbol,
                    "market_provider": symbol.market_provider,
                    "timeframe": symbol.timeframe,
                }
                for symbol in symbols
            ],
            "processed_event_count": processed_event_count,
            "matched_payload_count": len(payloads),
            "payloads": payloads,
        }

    @staticmethod
    def _record_data_process_replay_run(
        *,
        run_log_path: Any,
        timeframe: str,
        replay_range: Any,
        processed_event_count: int,
        emitted_payload_count: int,
        footprint_candle_count: int,
        symbols: tuple[ProcessSymbol, ...],
    ) -> None:
        run_log_path.parent.mkdir(parents=True, exist_ok=True)
        fields = (
            "started_at_ms",
            "timeframe",
            "start_vancouver",
            "end_vancouver",
            "start_utc",
            "end_utc",
            "start_ms",
            "end_ms",
            "processed_event_count",
            "emitted_payload_count",
            "footprint_candle_count",
            "symbols",
        )
        write_header = not run_log_path.exists() or run_log_path.stat().st_size <= 0
        if not write_header:
            try:
                existing_header = run_log_path.read_text(
                    encoding="utf-8",
                ).splitlines()[0].split(",")
                write_header = existing_header != list(fields)
            except (OSError, IndexError):
                write_header = True
        with run_log_path.open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            if write_header:
                writer.writeheader()
            writer.writerow(
                {
                    "started_at_ms": int(time.time() * 1000),
                    "timeframe": timeframe,
                    "start_vancouver": replay_range.start_vancouver,
                    "end_vancouver": replay_range.end_vancouver,
                    "start_utc": replay_range.start_utc,
                    "end_utc": replay_range.end_utc,
                    "start_ms": replay_range.start_ms,
                    "end_ms": replay_range.end_ms,
                    "processed_event_count": int(processed_event_count),
                    "emitted_payload_count": int(emitted_payload_count),
                    "footprint_candle_count": int(footprint_candle_count),
                    "symbols": ",".join(symbol.provider_symbol for symbol in symbols),
                }
            )

    def candle_chart_payload(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        candle_limit: int | None = None,
        include_profiles: bool = True,
    ) -> dict[str, Any]:
        normalized_timeframe = (timeframe or STUDY_DEFAULT_DISPLAY_TIMEFRAME).strip().upper()
        normalized_end_time = int(end_time_ms) if end_time_ms is not None else None
        normalized_limit = max(1, min(500, int(candle_limit or 200)))
        normalized_bin_tick_count = normalize_cme_bin_tick_count(CME_BIN_TICK_COUNT)
        cache_key = self._viewport_snapshot_cache_key(
            view="candle",
            timeframe=normalized_timeframe,
            end_time_ms=normalized_end_time,
            candle_limit=normalized_limit,
            variant=(
                f"{'profiles' if include_profiles else 'candles-only'}:"
                f"bin-{int(normalized_bin_tick_count)}t:"
                "engine-output-dom-positive-refill-v1"
            ),
        )
        cached = self._cached_snapshot(cache_key)
        if cached is not None:
            return self._snapshot_with_cache_result(cached, hit=True)

        sessions = [
            self._build_cme_chart_session_payload(
                spec,
                end_time_ms=normalized_end_time,
                candle_limit=normalized_limit,
                include_profiles=include_profiles,
            )
            for spec in self._session_specs.values()
            if spec.market_provider == PROVIDER_CME_LOCAL_DBN
            and spec.timeframe.strip().upper() == normalized_timeframe
        ]
        payload = {
            "type": "CME_CANDLE_CHART_SNAPSHOT",
            "timeframe": normalized_timeframe,
            "full_history": False,
            "viewport_window": True,
            "earliest_window_start_ms": self._minimum_numeric_session_field(
                sessions,
                "earliest_window_start_ms",
            ),
            "window_start_ms": self._minimum_numeric_session_field(sessions, "window_start_ms"),
            "window_end_ms": self._maximum_numeric_session_field(sessions, "window_end_ms"),
            "latest_window_end_ms": self._maximum_numeric_session_field(
                sessions,
                "latest_window_end_ms",
            ),
            "window_candle_limit": normalized_limit,
            "bin_tick_count": int(normalized_bin_tick_count),
            "has_older_data": any(bool(session.get("has_older_data")) for session in sessions),
            "processed_trades": sum(int(session.get("processed_trades", 0)) for session in sessions),
            "generated_at_utc": int(time.time() * 1000),
            "signals": self._session_signals(sessions),
            "viewport_metrics": self._session_viewport_metrics(sessions),
            "sessions": sessions,
        }
        self._record_snapshot_cache_result(payload, hit=False)
        self._store_snapshot_cache(cache_key, payload)
        return payload
    def _build_cme_chart_session_payload(
        self,
        spec: AbsorptionSessionSpec,
        *,
        end_time_ms: int | None = None,
        candle_limit: int = 200,
        include_profiles: bool = True,
    ) -> dict[str, Any]:
        tick_size = self._cme_tick_size(spec)
        try:
            payload = self.cme_paged_history_engine.chart_window(
                mt5_symbol=spec.mt5_symbol,
                provider_symbol=spec.provider_symbol,
                timeframe=spec.timeframe,
                tick_size=tick_size,
                end_time_ms=end_time_ms,
                candle_limit=candle_limit,
                include_profiles=include_profiles,
            )
            payload.update(
                {
                    "dataset": spec.dataset,
                    "schema": spec.schema,
                    "status": "READY",
                    "error": "",
                }
            )
            return payload
        except CmeLocalDataError as exc:
            return {
                "mt5_symbol": spec.mt5_symbol,
                "symbol": spec.provider_symbol,
                "provider_symbol": spec.provider_symbol,
                "market_provider": PROVIDER_CME_LOCAL_DBN,
                "quantity_unit": "CONTRACTS",
                "dataset": spec.dataset,
                "schema": spec.schema,
                "timeframe": spec.timeframe,
                "interval": spec.interval,
                "price_step": str(tick_size),
                "bin_tick_count": int(normalize_cme_bin_tick_count(CME_BIN_TICK_COUNT)),
                "fixed_bin_size": str(tick_size * CME_BIN_TICK_COUNT),
                "status": "CME_LOCAL_DATA_READER_NOT_READY",
                "error": str(exc),
                "viewport_window": True,
                "earliest_window_start_ms": 0,
                "window_start_ms": 0,
                "window_end_ms": 0,
                "latest_window_end_ms": 0,
                "window_candle_limit": candle_limit,
                "has_older_data": False,
                "processed_trades": 0,
                "candles": [],
                "daily_volume_profiles": [],
            }

    @staticmethod
    def _minimum_numeric_session_field(
        sessions: list[dict[str, Any]],
        field: str,
    ) -> int:
        values = [int(session.get(field, 0)) for session in sessions if int(session.get(field, 0)) > 0]
        return min(values) if values else 0

    @staticmethod
    def _maximum_numeric_session_field(
        sessions: list[dict[str, Any]],
        field: str,
    ) -> int:
        values = [int(session.get(field, 0)) for session in sessions if int(session.get(field, 0)) > 0]
        return max(values) if values else 0

    @staticmethod
    def _session_signals(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
        signals_by_id = {}
        for session in sessions:
            for signal in session.get("signals", []):
                signal_id = str(signal.get("signal_id", "")).strip()
                if signal_id:
                    signals_by_id[signal_id] = signal
        return list(signals_by_id.values())

    @staticmethod
    def _history_cursor(sessions: list[dict[str, Any]]) -> str:
        cursors = [
            str(session.get("history_cursor", "")).strip()
            for session in sessions
            if str(session.get("history_cursor", "")).strip()
        ]
        return max(cursors) if cursors else ""

    @staticmethod
    def _history_complete(sessions: list[dict[str, Any]]) -> bool:
        cme_sessions = [
            session
            for session in sessions
            if session.get("market_provider") == PROVIDER_CME_LOCAL_DBN
        ]
        return bool(cme_sessions) and all(bool(session.get("history_complete")) for session in cme_sessions)

    @staticmethod
    def _history_loaded_days(sessions: list[dict[str, Any]]) -> int:
        values = [
            int(session.get("history_loaded_days", 0))
            for session in sessions
            if session.get("market_provider") == PROVIDER_CME_LOCAL_DBN
        ]
        return min(values) if values else 0

    @staticmethod
    def _history_total_days(sessions: list[dict[str, Any]]) -> int:
        values = [
            int(session.get("history_total_days", 0))
            for session in sessions
            if session.get("market_provider") == PROVIDER_CME_LOCAL_DBN
        ]
        return max(values) if values else 0

    def _cme_tick_size(self, spec: AbsorptionSessionSpec) -> Decimal:
        if spec.tick_size:
            return Decimal(str(spec.tick_size))
        return self.cme_catalog.tick_size_for(spec.provider_symbol)

    def _live_candle_payload(
        self,
        *,
        spec: AbsorptionSessionSpec,
        fixed_bin_size: Decimal | None,
        now_ms: int,
    ) -> dict[str, Any] | None:
        if fixed_bin_size is None or fixed_bin_size <= 0:
            return None

        open_time_ms = self._get_live_candle_open_time_ms(spec, now_ms)
        interval_ms = self._interval_to_ms(spec.interval)
        close_time_ms = open_time_ms + interval_ms - 1
        trades = self.raw_event_buffer.snapshot_trade_events(
            spec.binance_symbol,
            spec.timeframe,
            open_time_ms,
            now_ms + 1,
        )
        if not trades:
            return None

        ordered_trades = sorted(trades, key=lambda item: int(item.event_time_ms))
        prices = [Decimal(str(item.price)) for item in ordered_trades]
        bins_by_index: dict[int, FootprintBin] = {}
        for trade in ordered_trades:
            price = Decimal(str(trade.price))
            quantity = Decimal(str(trade.quantity))
            bin_index = int((price / fixed_bin_size).to_integral_value(rounding=ROUND_FLOOR))
            bin_low = Decimal(bin_index) * fixed_bin_size
            footprint_bin = bins_by_index.get(bin_index)
            if footprint_bin is None:
                footprint_bin = FootprintBin(
                    bin_low=bin_low,
                    bin_high=bin_low + fixed_bin_size,
                    bin_index=bin_index,
                )
                bins_by_index[bin_index] = footprint_bin
            if trade.side == "sell":
                footprint_bin.sell_volume += quantity
                footprint_bin.bid_traded_volume += quantity
            else:
                footprint_bin.buy_volume += quantity
                footprint_bin.ask_traded_volume += quantity
            footprint_bin.horizontal_delta = footprint_bin.buy_volume - footprint_bin.sell_volume
            footprint_bin.min_trade_price_in_bin = (
                price
                if footprint_bin.min_trade_price_in_bin is None
                else min(footprint_bin.min_trade_price_in_bin, price)
            )
            footprint_bin.max_trade_price_in_bin = (
                price
                if footprint_bin.max_trade_price_in_bin is None
                else max(footprint_bin.max_trade_price_in_bin, price)
            )
            if footprint_bin.min_trade_price_in_bin is not None and footprint_bin.max_trade_price_in_bin is not None:
                footprint_bin.price_progress_in_bin = (
                    footprint_bin.max_trade_price_in_bin - footprint_bin.min_trade_price_in_bin
                )

        bins = tuple(item for _, item in sorted(bins_by_index.items()))
        contract_spike_metrics = calculate_contract_spike_metrics(
            item.total_volume for item in bins
        )
        for footprint_bin, score in zip(bins, contract_spike_metrics.scores):
            footprint_bin.contract_spike_score = score
            footprint_bin.abnormal_contract = is_contract_spike(score)
        footprint = CandleFootprint(
            symbol=spec.binance_symbol,
            mt5_symbol=spec.mt5_symbol,
            timeframe=spec.timeframe,
            interval=spec.interval,
            open_time_ms=open_time_ms,
            close_time_ms=close_time_ms,
            open_price=prices[0],
            high_price=max(prices),
            low_price=min(prices),
            close_price=prices[-1],
            bin_size=fixed_bin_size,
            bins=bins,
            hvn_result=detect_hvns(bins),
            price_step=self._price_steps.get(spec.binance_symbol.upper()),
            contract_spike_score_deviation=contract_spike_metrics.score_deviation,
        )
        payload = footprint.to_payload()
        payload["is_live"] = True
        return payload

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def execution_signal_payloads(
        self,
        mt5_symbol: str | None = None,
        client_name: str = "metatrader",
        primary_timeframe: str = "",
    ) -> list[dict[str, Any]]:
        return self.absorption_runtime.execution_signal_payloads(
            mt5_symbol=mt5_symbol,
            client_name=client_name,
            primary_timeframe=primary_timeframe,
        )

    def execution_command_payloads(
        self,
        mt5_symbol: str | None = None,
        client_name: str = "metatrader",
        primary_timeframe: str = "",
    ) -> list[dict[str, Any]]:
        return self.absorption_runtime.execution_command_payloads(
            mt5_symbol=mt5_symbol,
            client_name=client_name,
            primary_timeframe=primary_timeframe,
        )

    def update_execution_position_status(self, status_payload: dict[str, Any]) -> dict[str, Any]:
        return self.absorption_runtime.update_execution_position_status(status_payload)

    def latest_duration_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        del mt5_symbol, timeframe
        return None

    def latest_level_volume_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        del mt5_symbol, timeframe
        return None

    def latest_volume_zscore_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        del mt5_symbol, timeframe
        return None

    async def volume_zscore_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        del mt5_symbol, timeframe
        return None

    async def level_volume_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        del mt5_symbol, timeframe
        return None

    def _get_live_candle_open_time_ms(self, spec: AbsorptionSessionSpec, now_ms: int) -> int:
        candles = self._latest_candles.get(make_session_key(spec.mt5_symbol, spec.timeframe)) or []
        for candle in reversed(candles):
            if candle.open_time_ms <= now_ms <= candle.close_time_ms:
                return int(candle.open_time_ms)
        interval_ms = self._interval_to_ms(spec.interval)
        return (now_ms // interval_ms) * interval_ms

    def _study_memory_candles(self, timeframe: str | None = None) -> int:
        return study_display_candle_limit(
            timeframe,
            default=int(self.runtime_config.absorption_memory_candles),
        )

    def _study_keep_recent_count(self, timeframe: str | None = None) -> int:
        return self._study_memory_candles(timeframe) + 2

    def _study_closed_window(self, closed_candles: list[Any], timeframe: str | None = None) -> list[Any]:
        return list(closed_candles[-self._study_memory_candles(timeframe) :])

    def _prune_latest_candles(
        self,
        *,
        timeframe: str,
        candles: list[Any],
        closed_candles: list[Any],
        now_ms: int,
    ) -> list[Any]:
        visible_closed_open_times = {
            item.open_time_ms
            for item in self._study_closed_window(closed_candles, timeframe)
        }
        retained = [
            item
            for item in candles
            if item.open_time_ms in visible_closed_open_times or item.close_time_ms >= now_ms
        ]
        return retained[-self._study_keep_recent_count(timeframe) :]

    @staticmethod
    def _interval_to_ms(interval: str) -> int:
        unit = interval[-1]
        value = int(interval[:-1]) if interval[:-1].isdigit() else 1
        if unit == "s":
            return value * 1_000
        if unit == "m":
            return value * 60_000
        if unit == "h":
            return value * 3_600_000
        if unit == "d":
            return value * 86_400_000
        return 60_000
