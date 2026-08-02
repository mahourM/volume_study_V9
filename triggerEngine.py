from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable, Mapping

from core.engine_output_bus import DOM_POSITIVE_REFILL_OUTPUT_TYPE, extract_engine_outputs
from execution.position_close_csv_recorder import get_position_close_csv_recorder


LOGGER = logging.getLogger(__name__)
IDLE = "IDLE"
ABSORPTION_FOUND = "ABSORPTION_FOUND"
PEAK_CONFIRMED = "PEAK_CONFIRMED"
BREAK_CONFIRMED = "BREAK_CONFIRMED"
TRADING = "TRADING"
TIMEFRAME_INTERVAL_MS = {
    "S30": 30_000,
    "30S": 30_000,
    "M1": 60_000,
    "M3": 3 * 60_000,
    "M5": 5 * 60_000,
    "M15": 15 * 60_000,
    "M30": 30 * 60_000,
    "H1": 60 * 60_000,
    "H2": 2 * 60 * 60_000,
    "H6": 6 * 60 * 60_000,
    "H8": 8 * 60 * 60_000,
    "H12": 12 * 60 * 60_000,
}
SIGNAL_BIN_MAX_DISTANCE_POINTS = Decimal("3")
ZONE_TOUCHED = "ZONE_TOUCHED"
MIN_RETEST_CANDLES_SINCE_REFERENCE = 5
REFERENCE_TIMEOUT_CANDLES = 100
TOUCHED_ZONE_TIMEOUT_CANDLES = 10
EXIT_CONTRACT_SPIKE_SCORE_MIN = Decimal("14")
REFERENCE_DOM_REFILL_COUNT_MIN = 10
DOM_REFILL_POINT_MIN_COUNT = 10
TRIGGER_CATEGORY_ABSORPTION = "ABSORPTION"
TRIGGER_CATEGORY_ICEBURG = "ICEBURG"

@dataclass(frozen=True)
class TriggerConfig:
    supported_timeframes: tuple[str, ...] = ()
    trading_timeframe: str = ""
    confirmation_timeframe: str = ""
    runtime_logging_enabled: bool = False
    efficiency_max: Decimal = Decimal("0.005")
    diagonal_ratio_min: Decimal = Decimal("3.5")
    contract_spike_score_min: Decimal = Decimal("5.0")
    reference_contract_spike_score_min: Decimal = Decimal("14")
    reference_spike_score_deviation_min: Decimal = Decimal("3.0")
    reference_zone_tick_count: int = 3
    refill_lifetime_candles: int = 1
    point_value_by_symbol: Mapping[str, Decimal] = field(
        default_factory=lambda: {
            "NQ": Decimal("20"),
            "NQ.FUT": Decimal("20"),
        }
    )
    absorption_timeout_candles: int = 2
    bin_tick_count: int = 2

    def __post_init__(self) -> None:
        try:
            normalized_efficiency_max = Decimal(str(self.efficiency_max))
            normalized_diagonal_ratio_min = Decimal(str(self.diagonal_ratio_min))
            normalized_contract_spike_score_min = Decimal(
                str(self.contract_spike_score_min)
            )
            normalized_reference_contract_spike_score_min = Decimal(
                str(self.reference_contract_spike_score_min)
            )
            normalized_reference_spike_score_deviation_min = Decimal(
                str(self.reference_spike_score_deviation_min)
            )
            normalized_reference_zone_tick_count = int(
                self.reference_zone_tick_count
            )
            normalized_refill_lifetime_candles = int(self.refill_lifetime_candles)
            normalized_point_values = {
                str(symbol).strip().upper(): Decimal(str(point_value))
                for symbol, point_value in self.point_value_by_symbol.items()
                if str(symbol).strip()
            }
            normalized_timeout = int(self.absorption_timeout_candles)
            normalized_bin_tick_count = int(self.bin_tick_count)
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("invalid trigger configuration") from exc
        if normalized_efficiency_max < 0:
            raise ValueError("efficiency maximum must be non-negative")
        if normalized_diagonal_ratio_min < 0:
            raise ValueError("diagonal ratio minimum must be non-negative")
        if normalized_contract_spike_score_min < 0:
            raise ValueError("contract spike score minimum must be non-negative")
        if normalized_reference_contract_spike_score_min < 0:
            raise ValueError(
                "reference contract spike score minimum must be non-negative"
            )
        if normalized_reference_spike_score_deviation_min < 0:
            raise ValueError(
                "reference spike score deviation minimum must be non-negative"
            )
        if normalized_reference_zone_tick_count <= 0:
            raise ValueError("reference zone tick count must be positive")
        if normalized_refill_lifetime_candles < 0:
            raise ValueError("refill lifetime must be non-negative")
        if any(value <= 0 for value in normalized_point_values.values()):
            raise ValueError("point values must be positive")
        if normalized_timeout <= 0:
            raise ValueError("absorption timeout must be positive")
        if normalized_bin_tick_count <= 0:
            raise ValueError("trigger bin tick count must be positive")

        raw_timeframes = (
            (self.supported_timeframes,)
            if isinstance(self.supported_timeframes, str)
            else self.supported_timeframes
        )
        normalized_timeframes = tuple(
            dict.fromkeys(
                str(timeframe).strip().upper()
                for timeframe in raw_timeframes
                if str(timeframe).strip()
            )
        )
        object.__setattr__(self, "supported_timeframes", normalized_timeframes)
        object.__setattr__(
            self,
            "trading_timeframe",
            str(self.trading_timeframe or "").strip().upper(),
        )
        object.__setattr__(
            self,
            "confirmation_timeframe",
            str(self.confirmation_timeframe or "").strip().upper(),
        )
        object.__setattr__(
            self,
            "runtime_logging_enabled",
            _bool_config_value(self.runtime_logging_enabled),
        )
        object.__setattr__(self, "efficiency_max", normalized_efficiency_max)
        object.__setattr__(self, "diagonal_ratio_min", normalized_diagonal_ratio_min)
        object.__setattr__(
            self,
            "contract_spike_score_min",
            normalized_contract_spike_score_min,
        )
        object.__setattr__(
            self,
            "reference_contract_spike_score_min",
            normalized_reference_contract_spike_score_min,
        )
        object.__setattr__(
            self,
            "reference_spike_score_deviation_min",
            normalized_reference_spike_score_deviation_min,
        )
        object.__setattr__(
            self,
            "reference_zone_tick_count",
            normalized_reference_zone_tick_count,
        )
        object.__setattr__(
            self,
            "refill_lifetime_candles",
            normalized_refill_lifetime_candles,
        )
        object.__setattr__(self, "point_value_by_symbol", normalized_point_values)
        object.__setattr__(self, "absorption_timeout_candles", normalized_timeout)
        object.__setattr__(self, "bin_tick_count", normalized_bin_tick_count)


@dataclass(frozen=True)
class TriggerSignal:
    signal_id: str
    signal_type: str
    symbol: str
    provider_symbol: str
    timeframe: str
    trigger_candle_time_ms: int
    trigger_candle_close_time_ms: int
    action_candle_time_ms: int
    direction: str
    position_id: str
    reason: str
    wick: str
    marker_position: str
    marker_color: str
    marker_direction: str
    marker_shape: str
    reference_bin: dict[str, Any]
    matched_bins: tuple[dict[str, Any], ...]
    reference_candle_time_ms: int = 0
    confirmation_candle_time_ms: int = 0
    confirmation_state: str = ""
    entry_price: Decimal | None = None
    exit_price: Decimal | None = None
    stop_loss: Decimal | None = None
    trigger_category: str = TRIGGER_CATEGORY_ABSORPTION
    actor_proxy_payload: Mapping[str, Any] | None = None

    def to_payload(self) -> dict[str, Any]:
        reference_payload_id = _reference_payload_id(self.reference_bin)
        payload = {
            "signal_id": self.signal_id,
            "signal_type": self.signal_type,
            "trigger_category": self.trigger_category,
            "trigger_family": self.trigger_category,
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "timeframe": self.timeframe,
            "trigger_candle_time_ms": self.trigger_candle_time_ms,
            "trigger_candle_close_time_ms": self.trigger_candle_close_time_ms,
            "candle_time": self.trigger_candle_time_ms,
            "action_candle_time_ms": self.action_candle_time_ms,
            "entry_candle_time_ms": self.action_candle_time_ms,
            "direction": self.direction,
            "position_id": self.position_id,
            "reason": self.reason,
            "wick": self.wick,
            "marker_position": self.marker_position,
            "marker_color": self.marker_color,
            "marker_direction": self.marker_direction,
            "marker_shape": self.marker_shape,
            "reference_candle_time_ms": self.reference_candle_time_ms,
            "confirmation_candle_time_ms": self.confirmation_candle_time_ms,
            "confirmation_state": self.confirmation_state,
            "reference_payload_id": reference_payload_id,
            "process_payload_id": reference_payload_id,
            "source_payload_id": reference_payload_id,
            "break_confirmed_candle_time_ms": (
                self.confirmation_candle_time_ms
                if self.confirmation_state == BREAK_CONFIRMED
                else 0
            ),
            "reference_bin": dict(self.reference_bin),
            "reference_bin_low": self.reference_bin.get("low"),
            "reference_bin_high": self.reference_bin.get("high"),
            "reference_bin_side": self.reference_bin.get("side"),
            "abnormal_volume_score": self.reference_bin.get("abnormal_volume_score"),
            "spike_score": self.reference_bin.get("spike_score"),
            "spike_score_deviation": self.reference_bin.get(
                "spike_score_deviation"
            ),
            "reference_zone_low": self.reference_bin.get("reference_zone_low"),
            "reference_zone_high": self.reference_bin.get("reference_zone_high"),
            "matched_bin_count": len(self.matched_bins),
            "matched_bins": [dict(item) for item in self.matched_bins],
            "execution_mode": "STUDY_ONLY",
        }
        if self.entry_price is not None:
            payload["entry_price"] = str(self.entry_price)
        if self.exit_price is not None:
            payload["exit_price"] = str(self.exit_price)
            payload["exit_candle_time_ms"] = self.action_candle_time_ms
        if self.stop_loss is not None:
            payload["stop_loss"] = str(self.stop_loss)
            payload["stop_reference_price"] = str(self.stop_loss)
        if self.actor_proxy_payload:
            actor_payload = dict(self.actor_proxy_payload)
            actor_payload["position_context"] = {
                "position_id": self.position_id,
                "side": self.direction,
                "entry_price": str(self.entry_price) if self.entry_price is not None else "",
                "entry_time_ms": int(self.action_candle_time_ms),
                "symbol": self.symbol,
                "provider_symbol": self.provider_symbol,
                "timeframe": self.timeframe,
            }
            payload["actor_proxy_payload"] = actor_payload
        return payload


@dataclass
class ReferenceSetup:
    state: str = ABSORPTION_FOUND
    direction: str = ""
    reference_bin: dict[str, Any] | None = None
    reference_bins: tuple[dict[str, Any], ...] = ()
    reference_candle_time_ms: int = 0
    confirmation_candle_time_ms: int = 0
    confirmation_state: str = ""
    candles_since_reference: int = 0
    lowest_price_since_reference: Decimal | None = None
    highest_price_since_reference: Decimal | None = None
    reference_zone_low: Decimal | None = None
    reference_zone_high: Decimal | None = None
    stop_loss: Decimal | None = None
    tick_size: Decimal | None = None
    retest_direction_candles: tuple[dict[str, Decimal | int], ...] = ()
    retest_requires_turn: bool = False


@dataclass
class EntryState:
    state: str = IDLE
    direction: str = ""
    reference_bin: dict[str, Any] | None = None
    reference_bins: tuple[dict[str, Any], ...] = ()
    reference_candle_time_ms: int = 0
    confirmation_candle_time_ms: int = 0
    confirmation_state: str = ""
    candles_since_reference: int = 0
    lowest_price_since_reference: Decimal | None = None
    highest_price_since_reference: Decimal | None = None
    reference_zone_low: Decimal | None = None
    reference_zone_high: Decimal | None = None
    stop_loss: Decimal | None = None
    setups: tuple[ReferenceSetup, ...] = ()


@dataclass
class OpenPosition:
    position_id: str
    symbol: str
    provider_symbol: str
    timeframe: str
    side: str
    entry_candle_time_ms: int
    entry_price: Decimal | None
    stop_loss: Decimal | None
    reference_candle_time_ms: int = 0


@dataclass(frozen=True)
class RefillRecord:
    record_id: str
    symbol: str
    provider_symbol: str
    timeframe: str
    timestamp_ms: int
    price: Decimal
    side: str
    refill_count: int
    refill_total: int
    executed_refill_contracts: int = 0
    withdrawn_refill_contracts: int = 0
    has_refill: bool = True
    has_price_activity: bool = False
    order_count: int = 0
    added_contracts: int = 0
    opening_liquidity: int = 0
    available_liquidity: int = 0
    gross_added_contracts: int = 0
    non_refill_added_contracts: int = 0
    fill_event_count: int = 0
    executed_contracts: int = 0
    withdrawn_contracts: int = 0
    closing_liquidity: int = 0
    level_execution_rate: float = 0.0
    level_execution_rate_defined: bool = False
    level_execution_invariant_ok: bool = True
    added_breakdown_invariant_ok: bool = True
    action: str = "ENTRY"
    payload_type: str = TRIGGER_CATEGORY_ABSORPTION
    source: str = ""
    market_buy: int = 0
    market_sell: int = 0
    footprint_open_time_ms: int = 0
    footprint_bin_low: Decimal | None = None
    footprint_bin_high: Decimal | None = None
    zone_low: Decimal | None = None
    zone_high: Decimal | None = None
    zone_level_count: int = 0
    terminal_market_buy: int = 0
    terminal_market_sell: int = 0
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def refill_amount(self) -> int:
        return int(self.refill_count)

    def to_bin_payload(self) -> dict[str, Any]:
        return {
            "payload_id": self.record_id,
            "id": self.record_id,
            "output_id": self.record_id,
            "source_payload_id": self.record_id,
            "refill_record_id": self.record_id,
            "type": self.payload_type,
            "payload_type": self.payload_type,
            "index": None,
            "low": str(self.footprint_bin_low or self.price),
            "high": str(self.footprint_bin_high or self.price),
            "side": self.side,
            "dominance_side": self.side,
            "wick": "",
            "entry_direction": "",
            "abnormal_volume_score": "0",
            "spike_score": "0",
            "contract_spike_score": "0",
            "buy_contracts": str(int(self.market_buy)),
            "sell_contracts": str(int(self.market_sell)),
            "market_buy": int(self.market_buy),
            "market_sell": int(self.market_sell),
            "refill_count": int(self.refill_count),
            "refill_total": int(self.refill_total),
            "price_base_refill_count": int(self.refill_count),
            "price_base_refill_contracts": int(self.refill_total),
            "refill_added_contracts": int(self.refill_total),
            "executed_refill_contracts": min(
                int(self.refill_total), int(self.executed_refill_contracts)
            ),
            "withdrawn_refill_contracts": int(self.withdrawn_refill_contracts),
            "has_refill": bool(self.has_refill),
            "has_price_activity": bool(self.has_price_activity),
            "order_count": int(self.order_count),
            "added_contracts": int(self.added_contracts),
            "opening_liquidity": int(self.opening_liquidity),
            "available_liquidity": int(self.available_liquidity),
            "gross_added_contracts": int(self.gross_added_contracts),
            "non_refill_added_contracts": int(self.non_refill_added_contracts),
            "fill_event_count": int(self.fill_event_count),
            "executed_contracts": int(self.executed_contracts),
            "withdrawn_contracts": int(self.withdrawn_contracts),
            "closing_liquidity": int(self.closing_liquidity),
            "level_execution_rate": float(self.level_execution_rate),
            "level_execution_rate_defined": bool(self.level_execution_rate_defined),
            "level_execution_invariant_ok": bool(self.level_execution_invariant_ok),
            "added_breakdown_invariant_ok": bool(self.added_breakdown_invariant_ok),
            "refill_execution_rate": (
                min(int(self.refill_total), int(self.executed_refill_contracts))
                / int(self.refill_total) * 100.0
                if int(self.refill_total) > 0 else 0.0
            ),
            "refill_method": "price_base_refill",
            "action": self.action,
            "trigger_category": self.payload_type,
            "footprint_open_time_ms": int(self.footprint_open_time_ms),
            "source": self.source,
        }


@dataclass(frozen=True)
class TriggerOrderBookLevel:
    price: Decimal
    bid_contracts: int = 0
    ask_contracts: int = 0
    raw_buy_execute_contracts: int = 0
    raw_sell_execute_contracts: int = 0
    top_order_id: str = ""
    top_order_side: str = ""
    top_order_type: str = ""
    top_order_size: int = 0
    top_order_count: int = 0
    top_order_rank: int = 0
    top_order_last_size: int = 0
    top_order_current_contracts: int = 0
    top_order_positive_refill_count: int = 0
    top_order_positive_refill_total: int = 0

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "price": str(self.price),
            "bid_contracts": int(self.bid_contracts),
            "ask_contracts": int(self.ask_contracts),
        }
        optional_int_fields = {
            "raw_buy_execute_contracts": self.raw_buy_execute_contracts,
            "raw_sell_execute_contracts": self.raw_sell_execute_contracts,
            "top_order_size": self.top_order_size,
            "top_order_count": self.top_order_count,
            "top_order_rank": self.top_order_rank,
            "top_order_last_size": self.top_order_last_size,
            "top_order_current_contracts": self.top_order_current_contracts,
            "top_order_positive_refill_count": self.top_order_positive_refill_count,
            "top_order_positive_refill_total": self.top_order_positive_refill_total,
        }
        payload.update(
            {
                key: int(value)
                for key, value in optional_int_fields.items()
                if int(value) > 0
            }
        )
        if self.top_order_id:
            payload["top_order_id"] = self.top_order_id
        if self.top_order_side:
            payload["top_order_side"] = self.top_order_side
        if self.top_order_type:
            payload["top_order_type"] = self.top_order_type
        return payload


@dataclass(frozen=True)
class TriggerOrderBookSnapshot:
    symbol: str
    provider_symbol: str
    timeframe: str
    timestamp_ms: int
    levels: tuple[TriggerOrderBookLevel, ...] = ()
    best_bid: Decimal | None = None
    best_ask: Decimal | None = None
    source: str = "DOM"
    events: tuple[dict[str, Any], ...] = ()
    raw_events: tuple[dict[str, Any], ...] = ()
    resting_segments: tuple[dict[str, Any], ...] = ()
    best_bid_line: tuple[dict[str, Any], ...] = ()
    best_ask_line: tuple[dict[str, Any], ...] = ()
    iceberg_filter: Mapping[str, Any] = field(default_factory=dict)
    debug: Mapping[str, Any] = field(default_factory=dict)
    viewport_metrics: Mapping[str, Any] = field(default_factory=dict)
    dom_payload: Mapping[str, Any] = field(default_factory=dict, repr=False, compare=False)

    def level_at(self, price: Decimal | str | int | float) -> TriggerOrderBookLevel | None:
        target = _decimal_from_payload(price)
        if target is None:
            return None
        for level in self.levels:
            if level.price == target:
                return level
        return None

    def levels_between(
        self,
        low: Decimal | str | int | float,
        high: Decimal | str | int | float,
    ) -> tuple[TriggerOrderBookLevel, ...]:
        low_price = _decimal_from_payload(low)
        high_price = _decimal_from_payload(high)
        if low_price is None or high_price is None:
            return tuple()
        lower = min(low_price, high_price)
        upper = max(low_price, high_price)
        return tuple(
            level
            for level in self.levels
            if lower <= level.price <= upper
        )

    def liquidity_between(
        self,
        low: Decimal | str | int | float,
        high: Decimal | str | int | float,
    ) -> dict[str, int]:
        levels = self.levels_between(low, high)
        return {
            "bid_contracts": sum(level.bid_contracts for level in levels),
            "ask_contracts": sum(level.ask_contracts for level in levels),
        }

    def events_between(
        self,
        start_ms: int,
        end_ms: int,
    ) -> tuple[dict[str, Any], ...]:
        lower = min(int(start_ms), int(end_ms))
        upper = max(int(start_ms), int(end_ms))
        return tuple(
            event
            for event in self.events
            if lower <= _event_timestamp_ms(event) <= upper
        )

    def raw_events_between(
        self,
        start_ms: int,
        end_ms: int,
    ) -> tuple[dict[str, Any], ...]:
        lower = min(int(start_ms), int(end_ms))
        upper = max(int(start_ms), int(end_ms))
        return tuple(
            event
            for event in self.raw_events
            if lower <= _event_timestamp_ms(event) <= upper
        )

    def events_for_order(self, order_id: str) -> tuple[dict[str, Any], ...]:
        target = str(order_id or "").strip()
        if not target:
            return tuple()
        events_by_identity = {
            _dom_event_identity(event): event
            for event in (*self.events, *self.raw_events)
            if str(event.get("order_id") or event.get("venue_order_id") or "").strip()
            == target
        }
        return tuple(
            sorted(
                events_by_identity.values(),
                key=lambda item: _event_timestamp_ms(item),
            )
        )

    def to_payload(self) -> dict[str, Any]:
        payload = {
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "timeframe": self.timeframe,
            "timestamp_ms": int(self.timestamp_ms),
            "levels": [level.to_payload() for level in self.levels],
            "source": self.source,
        }
        if self.best_bid is not None:
            payload["best_bid"] = str(self.best_bid)
        if self.best_ask is not None:
            payload["best_ask"] = str(self.best_ask)
        if self.events:
            payload["events"] = [dict(item) for item in self.events]
        if self.raw_events:
            payload["raw_events"] = [dict(item) for item in self.raw_events]
        if self.resting_segments:
            payload["resting_segments"] = [dict(item) for item in self.resting_segments]
        if self.best_bid_line:
            payload["best_bid_line"] = [dict(item) for item in self.best_bid_line]
        if self.best_ask_line:
            payload["best_ask_line"] = [dict(item) for item in self.best_ask_line]
        if self.iceberg_filter:
            payload["iceberg_filter"] = dict(self.iceberg_filter)
        if self.debug:
            payload["debug"] = dict(self.debug)
        if self.viewport_metrics:
            payload["viewport_metrics"] = dict(self.viewport_metrics)
        return payload


class TriggerEngine:
    def __init__(
        self,
        config: TriggerConfig | None = None,
        *,
        actor_proxy_engine: Any | None = None,
    ) -> None:
        self.config = config or TriggerConfig()
        self.actor_proxy_engine = actor_proxy_engine
        self._entry_states: dict[tuple[str, str], EntryState] = {}
        self._positions: dict[tuple[str, str], OpenPosition] = {}
        self._order_books_by_key: dict[tuple[str, str], TriggerOrderBookSnapshot] = {}
        self._refills_by_key: dict[tuple[str, str], dict[str, RefillRecord]] = {}

    def supports_timeframe(self, timeframe: str | None) -> bool:
        normalized_timeframe = str(timeframe or "").strip().upper()
        if not normalized_timeframe:
            return False
        return (
            not self.config.supported_timeframes
            or normalized_timeframe in self.config.supported_timeframes
        )

    def state_for(
        self,
        *,
        symbol: str,
        timeframe: str,
    ) -> EntryState:
        return self._entry_states.setdefault(
            (symbol.strip().upper(), timeframe.strip().upper()),
            EntryState(),
        )

    def position_for(
        self,
        *,
        symbol: str,
        timeframe: str,
    ) -> OpenPosition | None:
        return self._positions.get((symbol.strip().upper(), timeframe.strip().upper()))

    def set_order_book_snapshot(
        self,
        snapshot: TriggerOrderBookSnapshot | Mapping[str, Any],
        *,
        symbol: str | None = None,
        provider_symbol: str | None = None,
        timeframe: str | None = None,
        timestamp_ms: int | None = None,
    ) -> TriggerOrderBookSnapshot | None:
        normalized_snapshots = order_book_snapshots_from_payload(
            snapshot,
            symbol=symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            timestamp_ms=timestamp_ms,
        )
        if not normalized_snapshots:
            if isinstance(snapshot, Mapping):
                self._remove_canceled_refill_records_from_payload(
                    snapshot,
                    symbol=symbol,
                    provider_symbol=provider_symbol,
                    timeframe=timeframe,
                )
                self._store_refill_records(
                    _refill_records_from_payload(
                        snapshot,
                        self.config,
                        symbol=symbol,
                        provider_symbol=provider_symbol,
                        timeframe=timeframe,
                        timestamp_ms=timestamp_ms,
                    )
                )
            return None
        for normalized in normalized_snapshots:
            self._store_order_book_snapshot(normalized)
            payload = normalized.dom_payload or normalized.to_payload()
            self._remove_canceled_refill_records_from_payload(
                payload,
                symbol=normalized.symbol,
                provider_symbol=normalized.provider_symbol,
                timeframe=normalized.timeframe,
            )
            self._store_refill_records(
                _refill_records_from_payload(
                    payload,
                    self.config,
                    symbol=normalized.symbol,
                    provider_symbol=normalized.provider_symbol,
                    timeframe=normalized.timeframe,
                    timestamp_ms=normalized.timestamp_ms,
                )
            )
        return normalized_snapshots[0]

    def set_dom_output_snapshot(
        self,
        payload: TriggerOrderBookSnapshot | Mapping[str, Any],
        *,
        symbol: str | None = None,
        provider_symbol: str | None = None,
        timeframe: str | None = None,
        timestamp_ms: int | None = None,
    ) -> TriggerOrderBookSnapshot | None:
        return self.set_order_book_snapshot(
            payload,
            symbol=symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            timestamp_ms=timestamp_ms,
        )

    def order_book_for(
        self,
        *,
        symbol: str,
        timeframe: str,
        provider_symbol: str | None = None,
    ) -> TriggerOrderBookSnapshot | None:
        normalized_timeframe = str(timeframe or "").strip().upper()
        lookup_symbols = tuple(
            dict.fromkeys(
                item
                for item in (
                    str(symbol or "").strip().upper(),
                    str(provider_symbol or "").strip().upper(),
                )
                if item
            )
        )
        for lookup_symbol in lookup_symbols:
            snapshot = self._order_books_by_key.get(
                (lookup_symbol, normalized_timeframe)
            )
            if snapshot is not None:
                return snapshot
        return None

    def dom_output_for(
        self,
        *,
        symbol: str,
        timeframe: str,
        provider_symbol: str | None = None,
    ) -> Mapping[str, Any] | None:
        snapshot = self.order_book_for(
            symbol=symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
        )
        if snapshot is None:
            return None
        return snapshot.dom_payload or snapshot.to_payload()

    def price_activity_records_for(
        self,
        *,
        symbol: str,
        timeframe: str,
        provider_symbol: str | None = None,
    ) -> tuple[RefillRecord, ...]:
        normalized_timeframe = str(timeframe or "").strip().upper()
        records: dict[str, RefillRecord] = {}
        for item in (symbol, provider_symbol):
            normalized_symbol = str(item or "").strip().upper()
            if normalized_symbol:
                records.update(self._refills_by_key.get((normalized_symbol, normalized_timeframe), {}))
        return tuple(
            sorted(
                (record for record in records.values() if record.has_price_activity),
                key=lambda record: (record.timestamp_ms, record.price, record.side, record.record_id),
            )
        )

    def clear_order_book_snapshot(
        self,
        *,
        symbol: str,
        timeframe: str,
        provider_symbol: str | None = None,
    ) -> None:
        normalized_timeframe = str(timeframe or "").strip().upper()
        for item in (symbol, provider_symbol):
            normalized_symbol = str(item or "").strip().upper()
            if normalized_symbol:
                self._order_books_by_key.pop((normalized_symbol, normalized_timeframe), None)

    def _store_order_book_snapshot(
        self,
        snapshot: TriggerOrderBookSnapshot,
    ) -> None:
        for item in (snapshot.symbol, snapshot.provider_symbol):
            normalized_symbol = str(item or "").strip().upper()
            if normalized_symbol:
                self._order_books_by_key[
                    (normalized_symbol, snapshot.timeframe)
                ] = snapshot

    def _store_refill_records(
        self,
        records: Iterable[RefillRecord],
    ) -> None:
        for record in records:
            for item in (record.symbol, record.provider_symbol):
                normalized_symbol = str(item or "").strip().upper()
                normalized_timeframe = str(record.timeframe or "").strip().upper()
                if not normalized_symbol or not normalized_timeframe:
                    continue
                bucket = self._refills_by_key.setdefault(
                    (normalized_symbol, normalized_timeframe),
                    {},
                )
                bucket[record.record_id] = record

    def _remove_refill_record_from_memory(
        self,
        record_id: str,
        *,
        symbol: str = "",
        provider_symbol: str = "",
        timeframe: str = "",
    ) -> None:
        normalized_record_id = str(record_id or "").strip()
        if not normalized_record_id:
            return
        normalized_timeframe = str(timeframe or "").strip().upper()
        lookup_symbols = tuple(
            dict.fromkeys(
                item
                for item in (
                    str(symbol or "").strip().upper(),
                    str(provider_symbol or "").strip().upper(),
                )
                if item
            )
        )
        removed = False
        if normalized_timeframe and lookup_symbols:
            for lookup_symbol in lookup_symbols:
                bucket = self._refills_by_key.get((lookup_symbol, normalized_timeframe))
                if bucket is not None:
                    removed = bucket.pop(normalized_record_id, None) is not None or removed
        if removed:
            return
        for bucket in self._refills_by_key.values():
            bucket.pop(normalized_record_id, None)

    def _remove_signal_reference_refill(self, signal: TriggerSignal) -> None:
        lookup_symbols = tuple(
            dict.fromkeys(
                item
                for item in (
                    str(signal.symbol or "").strip().upper(),
                    str(signal.provider_symbol or "").strip().upper(),
                )
                if item
            )
        )
        normalized_timeframe = str(signal.timeframe or "").strip().upper()
        if lookup_symbols and normalized_timeframe:
            for lookup_symbol in lookup_symbols:
                bucket = self._refills_by_key.get((lookup_symbol, normalized_timeframe))
                if bucket is not None:
                    bucket.clear()
            return

        reference_bin = signal.reference_bin or {}
        record_ids = reference_bin.get("refill_record_ids")
        if isinstance(record_ids, (list, tuple)):
            for item in record_ids:
                self._remove_refill_record_from_memory(
                    str(item or ""),
                    symbol=signal.symbol,
                    provider_symbol=signal.provider_symbol,
                    timeframe=signal.timeframe,
                )
            return
        record_id = str(
            reference_bin.get("refill_record_id") or reference_bin.get("record_id") or ""
        ).strip()
        self._remove_refill_record_from_memory(
            record_id,
            symbol=signal.symbol,
            provider_symbol=signal.provider_symbol,
            timeframe=signal.timeframe,
        )

    def _remove_canceled_refill_records_from_payload(
        self,
        payload: Mapping[str, Any],
        *,
        symbol: str | None = None,
        provider_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> None:
        normalized_symbol = str(
            symbol
            or payload.get("mt5_symbol")
            or payload.get("symbol")
            or ""
        ).strip().upper()
        normalized_provider_symbol = str(
            provider_symbol
            or payload.get("provider_symbol")
            or payload.get("symbol")
            or ""
        ).strip().upper()
        normalized_timeframe = str(timeframe or payload.get("timeframe") or "").strip().upper()
        canceled_ids = tuple(
            str(item or "").strip()
            for item in _iterable_payload_values(payload.get("canceled_zone_ids"))
            if str(item or "").strip()
        )
        for item in canceled_ids:
            self._remove_refill_record_from_memory(
                item,
                symbol=normalized_symbol,
                provider_symbol=normalized_provider_symbol,
                timeframe=normalized_timeframe,
            )
        if canceled_ids:
            self._remove_canceled_entry_setups(
                canceled_ids,
                symbol=normalized_symbol,
                provider_symbol=normalized_provider_symbol,
                timeframe=normalized_timeframe,
            )

    def _remove_canceled_entry_setups(
        self,
        canceled_ids: Iterable[str],
        *,
        symbol: str = "",
        provider_symbol: str = "",
        timeframe: str = "",
    ) -> None:
        canceled_id_set = {str(item or "").strip() for item in canceled_ids if str(item or "").strip()}
        if not canceled_id_set:
            return
        normalized_timeframe = str(timeframe or "").strip().upper()
        lookup_keys = {
            (str(item or "").strip().upper(), normalized_timeframe)
            for item in (symbol, provider_symbol)
            if str(item or "").strip() and normalized_timeframe
        }
        if not lookup_keys:
            lookup_keys = set(self._entry_states)

        for key in tuple(lookup_keys):
            state = self._entry_states.get(key)
            if state is None or not state.setups:
                continue
            kept_setups = tuple(
                setup
                for setup in state.setups
                if _reference_payload_id(setup.reference_bin) not in canceled_id_set
            )
            if len(kept_setups) == len(state.setups):
                continue
            state.setups = kept_setups
            if self._positions.get(key) is not None:
                if kept_setups:
                    self._sync_entry_state(state)
                else:
                    state.reference_bin = None
                    state.reference_bins = ()
                continue
            self._sync_entry_state(state)

    def _active_refill_records(
        self,
        candle: Mapping[str, Any],
        *,
        side: str | None = None,
        live_only: bool = False,
    ) -> tuple[RefillRecord, ...]:
        symbol = _symbol(candle)
        provider_symbol = _provider_symbol(candle)
        timeframe = _timeframe(candle)
        lookup_symbols = tuple(
            dict.fromkeys(item for item in (symbol, provider_symbol) if item)
        )
        records_by_id: dict[str, RefillRecord] = {}
        for lookup_symbol in lookup_symbols:
            bucket = self._refills_by_key.get((lookup_symbol, timeframe), {})
            records_by_id.update(bucket)

        normalized_side = _normalized_dom_side(side)
        active: list[RefillRecord] = []
        expired_ids: set[str] = set()
        for record in records_by_id.values():
            age = _refill_age_candles(candle, record)
            if age is None:
                continue
            if not _refill_record_is_active(record, age=age, config=self.config):
                expired_ids.add(record.record_id)
                continue
            if live_only and age != 0:
                continue
            if not record.has_refill:
                continue
            if normalized_side and record.side != normalized_side:
                continue
            active.append(record)

        if expired_ids:
            for bucket in self._refills_by_key.values():
                for record_id in expired_ids:
                    bucket.pop(record_id, None)
        return tuple(
            sorted(
                active,
                key=lambda item: (item.timestamp_ms, item.price, item.side, item.record_id),
            )
        )

    def _order_book_context(
        self,
        candle: Mapping[str, Any],
        order_book: TriggerOrderBookSnapshot | Mapping[str, Any] | None = None,
    ) -> TriggerOrderBookSnapshot | None:
        if order_book is not None:
            snapshot = self.set_order_book_snapshot(
                order_book,
                symbol=_symbol(candle),
                provider_symbol=_provider_symbol(candle),
                timeframe=_timeframe(candle),
                timestamp_ms=_integer_field(candle, "close_time_ms", "close_time"),
            )
            if snapshot is not None:
                return snapshot
        return self.order_book_for(
            symbol=_symbol(candle),
            provider_symbol=_provider_symbol(candle),
            timeframe=_timeframe(candle),
        )

    def set_open_position(self, position: OpenPosition) -> None:
        key = (position.symbol.strip().upper(), position.timeframe.strip().upper())
        self._positions[key] = position
        self._entry_states[key] = EntryState(state=TRADING)
        self._log_state_change(
            symbol=key[0],
            timeframe=key[1],
            transition=f"{IDLE} -> {TRADING}",
        )

    def clear_position(
        self,
        *,
        symbol: str,
        timeframe: str,
        reason: str = "RESET_AFTER_POSITION_CLOSED",
    ) -> None:
        key = (symbol.strip().upper(), timeframe.strip().upper())
        position = self._positions.pop(key, None)
        if position is not None:
            self._stop_actor_proxy_tracking(position.position_id)
        self._entry_states[key] = EntryState()
        self._log_state_change(
            symbol=key[0],
            timeframe=key[1],
            transition=reason,
        )

    def reset_state(self, *, symbol: str, timeframe: str, reason: str) -> None:
        key = (symbol.strip().upper(), timeframe.strip().upper())
        self._entry_states[key] = EntryState()
        self._log_state_change(
            symbol=key[0],
            timeframe=key[1],
            transition=reason,
        )

    def evaluate_candle(
        self,
        candle: Mapping[str, Any],
        *,
        evaluation_time_ms: int,
        order_book: TriggerOrderBookSnapshot | Mapping[str, Any] | None = None,
    ) -> tuple[TriggerSignal, ...]:
        if not self._is_closed(candle, evaluation_time_ms=evaluation_time_ms):
            return tuple()
        return self.process_closed_candle(candle, order_book=order_book)

    def evaluate_latest(
        self,
        candles: Iterable[Mapping[str, Any]],
        *,
        evaluation_time_ms: int,
        order_book: TriggerOrderBookSnapshot | Mapping[str, Any] | None = None,
    ) -> tuple[TriggerSignal, ...]:
        ordered = _sorted_candles(candles)
        closed = [
            candle
            for candle in ordered
            if self._is_closed(candle, evaluation_time_ms=evaluation_time_ms)
        ]
        if not closed:
            return tuple()
        latest = closed[-1]
        latest_open = _integer_field(latest, "open_time_ms", "open_time")
        next_candle = next(
            (
                candle
                for candle in ordered
                if latest_open is not None
                and (_integer_field(candle, "open_time_ms", "open_time") or 0)
                > latest_open
            ),
            None,
        )
        current_candle = (
            next_candle
            if next_candle is not None
            and not self._is_closed(next_candle, evaluation_time_ms=evaluation_time_ms)
            else None
        )
        return self.process_closed_candle(
            latest,
            next_candle=next_candle,
            current_candle=current_candle,
            order_book=order_book,
        )

    def process_closed_candle(
        self,
        candle: Mapping[str, Any],
        *,
        next_candle: Mapping[str, Any] | None = None,
        current_candle: Mapping[str, Any] | None = None,
        order_book: TriggerOrderBookSnapshot | Mapping[str, Any] | None = None,
        entry_states: dict[tuple[str, str], EntryState] | None = None,
        positions: dict[tuple[str, str], OpenPosition] | None = None,
        record_closed_position: bool = False,
    ) -> tuple[TriggerSignal, ...]:
        timeframe = _timeframe(candle)
        if not self.supports_timeframe(timeframe):
            return tuple()
        symbol = _symbol(candle)
        provider_symbol = _provider_symbol(candle)
        if not symbol or not timeframe:
            return tuple()

        state_store = entry_states if entry_states is not None else self._entry_states
        position_store = positions if positions is not None else self._positions
        key = (symbol, timeframe)
        state = state_store.setdefault(key, EntryState())
        position = position_store.get(key)
        order_book_context = self._order_book_context(candle, order_book)
        self._store_refill_records(
            _refill_records_from_payload(
                candle,
                self.config,
                symbol=symbol,
                provider_symbol=provider_symbol,
                timeframe=timeframe,
            )
        )
        if current_candle is not None:
            self._store_refill_records(
                _refill_records_from_payload(
                    current_candle,
                    self.config,
                    symbol=_symbol(current_candle) or symbol,
                    provider_symbol=_provider_symbol(current_candle) or provider_symbol,
                    timeframe=_timeframe(current_candle) or timeframe,
                )
            )

        if position is not None and position.side == "LONG":
            self._initialize_position_prices(candle, position)
            if state.state != TRADING:
                state.state = TRADING
            state_store[key] = state
            reversal_entry = self._opposite_entry_signal(
                candle,
                next_candle,
                state,
                desired_direction="SHORT",
                order_book=order_book_context,
            )
            if reversal_entry is not None:
                return self._reverse_position(
                    candle,
                    next_candle,
                    position,
                    reversal_entry,
                    state_store=state_store,
                    position_store=position_store,
                    key=key,
                    record_closed_position=record_closed_position,
                    external_state=entry_states is not None or positions is not None,
                )
            self._remember_opposite_entry_setups(
                candle,
                state,
                desired_direction="SHORT",
                order_book=order_book_context,
            )
            signal = self._exit_buy_signal(
                candle,
                next_candle,
                position,
                order_book=order_book_context,
            )
            if signal is None and current_candle is not None:
                signal = self._exit_buy_signal(
                    current_candle,
                    None,
                    position,
                    order_book=order_book_context,
                )
            if signal is not None:
                if record_closed_position or (entry_states is None and positions is None):
                    get_position_close_csv_recorder().record(
                        position=position,
                        exit_signal=signal,
                    )
                position_store.pop(key, None)
                self._stop_actor_proxy_tracking(position.position_id)
                self._remove_signal_reference_refill(signal)
                state_store[key] = EntryState()
                self._log_signal(signal)
                self._log_state_change(
                    symbol=symbol,
                    timeframe=timeframe,
                    transition="RESET_AFTER_EXIT",
                )
                return (signal,)
            return tuple()

        if position is not None and position.side == "SHORT":
            self._initialize_position_prices(candle, position)
            if state.state != TRADING:
                state.state = TRADING
            state_store[key] = state
            reversal_entry = self._opposite_entry_signal(
                candle,
                next_candle,
                state,
                desired_direction="LONG",
                order_book=order_book_context,
            )
            if reversal_entry is not None:
                return self._reverse_position(
                    candle,
                    next_candle,
                    position,
                    reversal_entry,
                    state_store=state_store,
                    position_store=position_store,
                    key=key,
                    record_closed_position=record_closed_position,
                    external_state=entry_states is not None or positions is not None,
                )
            self._remember_opposite_entry_setups(
                candle,
                state,
                desired_direction="LONG",
                order_book=order_book_context,
            )
            signal = self._exit_sell_signal(
                candle,
                next_candle,
                position,
                order_book=order_book_context,
            )
            if signal is None and current_candle is not None:
                signal = self._exit_sell_signal(
                    current_candle,
                    None,
                    position,
                    order_book=order_book_context,
                )
            if signal is not None:
                if record_closed_position or (entry_states is None and positions is None):
                    get_position_close_csv_recorder().record(
                        position=position,
                        exit_signal=signal,
                    )
                position_store.pop(key, None)
                self._stop_actor_proxy_tracking(position.position_id)
                self._remove_signal_reference_refill(signal)
                state_store[key] = EntryState()
                self._log_signal(signal)
                self._log_state_change(
                    symbol=symbol,
                    timeframe=timeframe,
                    transition="RESET_AFTER_EXIT",
                )
                return (signal,)
            return tuple()

        if position is None and state.state == TRADING:
            state = EntryState()
            state_store[key] = state
            self._log_state_change(
                symbol=symbol,
                timeframe=timeframe,
                transition="RESET_AFTER_POSITION_CLOSED",
            )

        signal = self._entry_signal(
            candle,
            next_candle,
            state,
            order_book=order_book_context,
        )
        if signal is None:
            return tuple()

        self._remove_signal_reference_refill(signal)
        position_store[key] = OpenPosition(
            position_id=signal.position_id,
            symbol=symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            side="LONG" if signal.signal_type == "BUY_ENTRY" else "SHORT",
            entry_candle_time_ms=signal.action_candle_time_ms,
            entry_price=signal.entry_price,
            stop_loss=signal.stop_loss,
            reference_candle_time_ms=signal.reference_candle_time_ms,
        )
        self._start_actor_proxy_tracking(signal)
        state_store[key] = self._trading_state_after_entry(state, signal)
        self._log_signal(signal)
        self._log_state_change(
            symbol=symbol,
            timeframe=timeframe,
            transition="RESET_AFTER_ENTRY",
        )
        return (signal,)

    def enrich_candles(
        self,
        candles: Iterable[dict[str, Any]],
        *,
        evaluation_time_ms: int,
        confirmation_candles: Iterable[Mapping[str, Any]] | None = None,
        order_books_by_time: Mapping[int, TriggerOrderBookSnapshot | Mapping[str, Any]] | None = None,
        persist_state: bool = False,
        record_closed_positions: bool = False,
    ) -> list[dict[str, Any]]:
        del confirmation_candles
        ordered = _sorted_mutable_candles(candles)
        for candle in ordered:
            candle["trigger_signals"] = []

        state_store = self._entry_states if persist_state else {}
        position_store = self._positions if persist_state else {}
        signals = []
        for index, candle in enumerate(ordered):
            if not self._is_closed(candle, evaluation_time_ms=evaluation_time_ms):
                continue
            next_candle = ordered[index + 1] if index + 1 < len(ordered) else None
            current_candle = (
                next_candle
                if next_candle is not None
                and not self._is_closed(
                    next_candle,
                    evaluation_time_ms=evaluation_time_ms,
                )
                else None
            )
            candle_signals = [
                signal.to_payload()
                for signal in self.process_closed_candle(
                    candle,
                    next_candle=next_candle,
                    current_candle=current_candle,
                    order_book=(
                        _order_book_for_candle_time(candle, order_books_by_time)
                        or self.order_book_for(
                            symbol=_symbol(candle),
                            provider_symbol=_provider_symbol(candle),
                            timeframe=_timeframe(candle),
                        )
                    ),
                    entry_states=state_store,
                    positions=position_store,
                    record_closed_position=record_closed_positions,
                )
            ]
            candle["trigger_signals"] = candle_signals
            signals.extend(candle_signals)
        return signals

    def _entry_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        state: EntryState,
        *,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> TriggerSignal | None:
        kept_setups: list[ReferenceSetup] = []
        if state.setups:
            for setup in state.setups:
                signal, keep_setup = self._process_entry_setup(
                    candle,
                    next_candle,
                    setup,
                    order_book=order_book,
                )
                if signal is not None:
                    return signal
                if keep_setup:
                    kept_setups.append(setup)
            state.setups = tuple(kept_setups)
            self._sync_entry_state(state)

        absorption = self._entry_absorption_candidate(
            candle,
            order_book=order_book,
        )
        if absorption is not None:
            previous_state = state.state
            setup = self._setup_from_absorption(candle, absorption)
            state.setups = (*state.setups, setup)
            self._sync_entry_state(state)
            self._log_state_change(
                symbol=_symbol(candle),
                timeframe=_timeframe(candle),
                transition=f"{previous_state} -> {ABSORPTION_FOUND}",
                reference_bin=setup.reference_bin,
            )

        iceburg = self._iceburg_entry_signal(candle, next_candle, state)
        if iceburg is not None:
            return iceburg
        return None

    def _opposite_entry_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        state: EntryState,
        *,
        desired_direction: str,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> TriggerSignal | None:
        kept_setups: list[ReferenceSetup] = []
        normalized_direction = str(desired_direction or "").strip().upper()
        for setup in state.setups:
            if setup.direction != normalized_direction:
                kept_setups.append(setup)
                continue
            signal, keep_setup = self._process_entry_setup(
                candle,
                next_candle,
                setup,
                order_book=order_book,
            )
            if signal is not None:
                state.setups = tuple(kept_setups)
                return signal
            if keep_setup:
                kept_setups.append(setup)
        state.setups = tuple(kept_setups)
        return None

    def _remember_opposite_entry_setups(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
        *,
        desired_direction: str,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> None:
        normalized_direction = str(desired_direction or "").strip().upper()
        existing_ids = {
            _reference_payload_id(setup.reference_bin)
            for setup in state.setups
            if setup.direction == normalized_direction
        }
        next_setups = list(state.setups)
        for absorption in self._entry_absorption_candidates(candle, order_book=order_book):
            direction = _entry_direction_from_payload(
                absorption,
                str(absorption.get("refill_side") or absorption.get("side") or ""),
            )
            if direction != normalized_direction:
                continue
            reference_id = _reference_payload_id(absorption)
            if reference_id and reference_id in existing_ids:
                continue
            setup = self._setup_from_absorption(candle, absorption)
            next_setups.append(setup)
            if reference_id:
                existing_ids.add(reference_id)
        state.setups = tuple(next_setups)

    def _reverse_position(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        position: OpenPosition,
        entry_signal: TriggerSignal,
        *,
        state_store: dict[tuple[str, str], EntryState],
        position_store: dict[tuple[str, str], OpenPosition],
        key: tuple[str, str],
        record_closed_position: bool,
        external_state: bool,
    ) -> tuple[TriggerSignal, ...]:
        exit_signal = self._reversal_exit_signal(
            candle,
            next_candle,
            position,
            entry_signal,
        )
        if record_closed_position or not external_state:
            get_position_close_csv_recorder().record(
                position=position,
                exit_signal=exit_signal,
            )
        position_store.pop(key, None)
        self._stop_actor_proxy_tracking(position.position_id)
        self._log_signal(exit_signal)

        position_store[key] = OpenPosition(
            position_id=entry_signal.position_id,
            symbol=entry_signal.symbol,
            provider_symbol=entry_signal.provider_symbol,
            timeframe=entry_signal.timeframe,
            side="LONG" if entry_signal.signal_type == "BUY_ENTRY" else "SHORT",
            entry_candle_time_ms=entry_signal.action_candle_time_ms,
            entry_price=entry_signal.entry_price,
            stop_loss=entry_signal.stop_loss,
            reference_candle_time_ms=entry_signal.reference_candle_time_ms,
        )
        self._start_actor_proxy_tracking(entry_signal)
        self._remove_signal_reference_refill(entry_signal)
        state_store[key] = self._trading_state_after_entry(state_store.get(key), entry_signal)
        self._log_signal(entry_signal)
        self._log_state_change(
            symbol=entry_signal.symbol,
            timeframe=entry_signal.timeframe,
            transition=f"REVERSE_{position.side}_TO_{position_store[key].side}",
            reference_bin=entry_signal.reference_bin,
        )
        return (exit_signal, entry_signal)

    def _reversal_exit_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        position: OpenPosition,
        entry_signal: TriggerSignal,
    ) -> TriggerSignal:
        if position.side == "LONG":
            return self._exit_signal_payload(
                candle,
                next_candle,
                position,
                signal_type="EXIT_BUY",
                direction="EXIT_BUY",
                marker_position="ABOVE",
                marker_color="RED",
                marker_direction="NONE",
                reason="BUY_EXIT_REVERSE_TO_SELL",
                reference_bin=dict(entry_signal.reference_bin),
                exit_price=entry_signal.entry_price,
            )
        return self._exit_signal_payload(
            candle,
            next_candle,
            position,
            signal_type="EXIT_SELL",
            direction="EXIT_SELL",
            marker_position="BELOW",
            marker_color="GREEN",
            marker_direction="NONE",
            reason="SELL_EXIT_REVERSE_TO_BUY",
            reference_bin=dict(entry_signal.reference_bin),
            exit_price=entry_signal.entry_price,
        )

    @staticmethod
    def _trading_state_after_entry(
        state: EntryState | None,
        entry_signal: TriggerSignal,
    ) -> EntryState:
        entered_direction = "LONG" if entry_signal.signal_type == "BUY_ENTRY" else "SHORT"
        preserved_setups = tuple(
            setup
            for setup in getattr(state, "setups", ())
            if setup.direction and setup.direction != entered_direction
        )
        return EntryState(state=TRADING, setups=preserved_setups)

    def _process_entry_setup(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        setup: ReferenceSetup,
        *,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> tuple[TriggerSignal | None, bool]:
        del order_book
        setup.candles_since_reference += 1
        if setup.state == ZONE_TOUCHED and self._touched_zone_timeout_expired(candle, setup):
            self._log_state_change(
                symbol=_symbol(candle),
                timeframe=_timeframe(candle),
                transition="RESET_TOUCHED_ZONE_BY_TIMEOUT",
                reference_bin=setup.reference_bin,
            )
            return None, False
        if setup.state != ZONE_TOUCHED and setup.candles_since_reference > REFERENCE_TIMEOUT_CANDLES:
            self._log_state_change(
                symbol=_symbol(candle),
                timeframe=_timeframe(candle),
                transition="RESET_BY_TIMEOUT",
                reference_bin=setup.reference_bin,
            )
            return None, False
        low_price = _decimal_field(candle, "low_price", nested=("ohlc", "low"))
        high_price = _decimal_field(candle, "high_price", nested=("ohlc", "high"))

        if setup.direction == "LONG" and setup.state in {ABSORPTION_FOUND, ZONE_TOUCHED}:
            signal_bin = self._entry_execution_bin(candle, setup, direction="LONG")
            if setup.state == ZONE_TOUCHED and signal_bin is not None:
                return self._buy_entry_payload(
                    candle,
                    next_candle,
                    setup,
                    signal_bin=signal_bin,
                ), True
            if not self._setup_touched_zone(setup, low_price=low_price, high_price=high_price):
                return None, True
            if setup.state == ABSORPTION_FOUND and self._retest_has_minimum_distance(setup):
                if signal_bin is not None:
                    return self._buy_entry_payload(
                        candle,
                        next_candle,
                        setup,
                        signal_bin=signal_bin,
                    ), True
                self._mark_retest(candle, setup)
            return None, True

        if setup.direction == "SHORT" and setup.state in {ABSORPTION_FOUND, ZONE_TOUCHED}:
            signal_bin = self._entry_execution_bin(candle, setup, direction="SHORT")
            if setup.state == ZONE_TOUCHED and signal_bin is not None:
                return self._sell_entry_payload(
                    candle,
                    next_candle,
                    setup,
                    signal_bin=signal_bin,
                ), True
            if not self._setup_touched_zone(setup, low_price=low_price, high_price=high_price):
                return None, True
            if setup.state == ABSORPTION_FOUND and self._retest_has_minimum_distance(setup):
                if signal_bin is not None:
                    return self._sell_entry_payload(
                        candle,
                        next_candle,
                        setup,
                        signal_bin=signal_bin,
                    ), True
                self._mark_retest(candle, setup)
            return None, True
        return None, True

    def _entry_execution_bin(
        self,
        candle: Mapping[str, Any],
        setup: ReferenceSetup,
        *,
        direction: str,
    ) -> dict[str, Any] | None:
        if setup.reference_zone_low is None or setup.reference_zone_high is None:
            return None
        zone_low = min(setup.reference_zone_low, setup.reference_zone_high)
        zone_high = max(setup.reference_zone_low, setup.reference_zone_high)
        normalized_direction = str(direction or "").strip().upper()
        matches: list[tuple[Mapping[str, Any], Decimal, Decimal]] = []
        for item in _normalized_bins(candle):
            center = _bin_center(item)
            if center is None:
                continue
            if normalized_direction == "LONG":
                executed_contracts = _bin_buy_contracts(item)
                if executed_contracts <= 0 or center <= zone_high:
                    continue
                matches.append((item, center, executed_contracts))
                continue
            if normalized_direction == "SHORT":
                executed_contracts = _bin_sell_contracts(item)
                if executed_contracts <= 0 or center >= zone_low:
                    continue
                matches.append((item, center, executed_contracts))
        if not matches:
            return None
        if normalized_direction == "LONG":
            item, _center, _contracts = min(
                matches,
                key=lambda candidate: (candidate[1], -candidate[2]),
            )
            return self._bin_payload(
                item,
                entry_direction="BUY",
                wick="ABOVE_ZONE",
                side="BUY",
            )
        if normalized_direction == "SHORT":
            item, _center, _contracts = max(
                matches,
                key=lambda candidate: (candidate[1], candidate[2]),
            )
            return self._bin_payload(
                item,
                entry_direction="SELL",
                wick="BELOW_ZONE",
                side="SELL",
            )
        return None

    def _iceburg_entry_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        state: EntryState,
    ) -> TriggerSignal | None:
        del candle, next_candle, state
        return None

    def _iceburg_exit_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        position: OpenPosition,
    ) -> TriggerSignal | None:
        del candle, next_candle, position
        return None

    def _mark_retest(
        self,
        candle: Mapping[str, Any],
        setup: ReferenceSetup,
    ) -> None:
        retest_time_ms = _integer_field(candle, "open_time_ms", "open_time") or 0
        setup.state = ZONE_TOUCHED
        setup.confirmation_state = ZONE_TOUCHED
        setup.confirmation_candle_time_ms = retest_time_ms
        if setup.reference_bin is not None:
            setup.reference_bin["retest_candle_time_ms"] = retest_time_ms
        self._log_state_change(
            symbol=_symbol(candle),
            timeframe=_timeframe(candle),
            transition=f"{ABSORPTION_FOUND} -> {ZONE_TOUCHED}",
            reference_bin=setup.reference_bin,
        )

    @staticmethod
    def _setup_touched_zone(
        setup: ReferenceSetup,
        *,
        low_price: Decimal | None,
        high_price: Decimal | None,
    ) -> bool:
        if setup.reference_zone_low is None or setup.reference_zone_high is None:
            return False
        if setup.direction == "LONG":
            return low_price is not None and low_price <= setup.reference_zone_high
        if setup.direction == "SHORT":
            return high_price is not None and high_price >= setup.reference_zone_low
        return False

    @staticmethod
    def _touched_zone_timeout_expired(
        candle: Mapping[str, Any],
        setup: ReferenceSetup,
    ) -> bool:
        elapsed_candles = _elapsed_candles_since_time(
            candle,
            start_time_ms=int(setup.confirmation_candle_time_ms or 0),
        )
        return (
            elapsed_candles is not None
            and elapsed_candles > TOUCHED_ZONE_TIMEOUT_CANDLES
        )

    @staticmethod
    def _retest_has_minimum_distance(setup: ReferenceSetup) -> bool:
        return setup.candles_since_reference >= MIN_RETEST_CANDLES_SINCE_REFERENCE

    @staticmethod
    def _retest_has_required_direction(setup: ReferenceSetup) -> bool:
        return not setup.retest_requires_turn

    def _update_retest_direction_state(
        self,
        candle: Mapping[str, Any],
        setup: ReferenceSetup,
    ) -> None:
        if setup.state != ABSORPTION_FOUND:
            return
        summary = _candle_pivot_summary(candle)
        if summary is None:
            return
        setup.retest_direction_candles = (
            *setup.retest_direction_candles,
            summary,
        )[-5:]
        if len(setup.retest_direction_candles) < 5:
            return
        center = setup.retest_direction_candles[2]
        reference_low, reference_high = _reference_extreme_prices(setup)

        if setup.direction == "SHORT":
            if (
                reference_high is not None
                and center["high"] > reference_high
                and _is_swing_high(setup.retest_direction_candles)
            ):
                setup.retest_requires_turn = True
                return
            if setup.retest_requires_turn and _is_swing_low(
                setup.retest_direction_candles
            ):
                setup.retest_requires_turn = False
            return

        if setup.direction == "LONG":
            if (
                reference_low is not None
                and center["low"] < reference_low
                and _is_swing_low(setup.retest_direction_candles)
            ):
                setup.retest_requires_turn = True
                return
            if setup.retest_requires_turn and _is_swing_high(
                setup.retest_direction_candles
            ):
                setup.retest_requires_turn = False

    def _buy_entry_payload(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        setup: ReferenceSetup,
        *,
        signal_bin: dict[str, Any] | None = None,
    ) -> TriggerSignal:
        if setup.confirmation_candle_time_ms <= 0:
            setup.confirmation_candle_time_ms = (
                _integer_field(candle, "open_time_ms", "open_time") or 0
            )
        setup.confirmation_state = ZONE_TOUCHED
        self._log_state_change(
            symbol=_symbol(candle),
            timeframe=_timeframe(candle),
            transition=f"{ZONE_TOUCHED} -> BUY_ENTRY",
            reference_bin=setup.reference_bin,
        )
        return self._entry_signal_payload(
            candle,
            next_candle,
            setup,
            signal_type="BUY_ENTRY",
            direction="LONG",
            marker_position="BELOW",
            marker_color="GREEN",
            marker_direction="UP",
            reason="BUY_ENTRY_EXECUTION_ABOVE_ZONE_HIGH",
            stop_loss=setup.reference_zone_low,
            signal_bin=signal_bin,
        )

    def _sell_entry_payload(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        setup: ReferenceSetup,
        *,
        signal_bin: dict[str, Any] | None = None,
    ) -> TriggerSignal:
        if setup.confirmation_candle_time_ms <= 0:
            setup.confirmation_candle_time_ms = (
                _integer_field(candle, "open_time_ms", "open_time") or 0
            )
        setup.confirmation_state = ZONE_TOUCHED
        self._log_state_change(
            symbol=_symbol(candle),
            timeframe=_timeframe(candle),
            transition=f"{ZONE_TOUCHED} -> SELL_ENTRY",
            reference_bin=setup.reference_bin,
        )
        return self._entry_signal_payload(
            candle,
            next_candle,
            setup,
            signal_type="SELL_ENTRY",
            direction="SHORT",
            marker_position="ABOVE",
            marker_color="RED",
            marker_direction="DOWN",
            reason="SELL_ENTRY_EXECUTION_BELOW_ZONE_LOW",
            stop_loss=setup.reference_zone_high,
            signal_bin=signal_bin,
        )

    def _start_absorption_state(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
        absorption: Mapping[str, Any],
        *,
        transition: str,
    ) -> None:
        setup = self._setup_from_absorption(candle, absorption)
        state.setups = (setup,)
        self._sync_entry_state(state)
        self._log_state_change(
            symbol=_symbol(candle),
            timeframe=_timeframe(candle),
            transition=transition,
            reference_bin=setup.reference_bin,
        )

    def _setup_from_absorption(
        self,
        candle: Mapping[str, Any],
        absorption: Mapping[str, Any],
    ) -> ReferenceSetup:
        reference_bins_payload = absorption.get("reference_bins")
        reference_bins = tuple(dict(item) for item in _iter_mappings(reference_bins_payload))
        reference_bin = {
            key: value for key, value in absorption.items() if key != "reference_bins"
        }
        low_price = _decimal_field(candle, "low_price", nested=("ohlc", "low"))
        high_price = _decimal_field(candle, "high_price", nested=("ohlc", "high"))
        direction = _entry_direction_from_payload(
            absorption,
            str(absorption.get("refill_side") or absorption.get("side") or ""),
        )
        reference_zone_low = _decimal_from_payload(absorption.get("reference_zone_low"))
        reference_zone_high = _decimal_from_payload(absorption.get("reference_zone_high"))
        stop_loss = _decimal_from_payload(absorption.get("stop_loss"))
        if stop_loss is None:
            if direction == "LONG":
                stop_loss = reference_zone_low
            elif direction == "SHORT":
                stop_loss = reference_zone_high
        return ReferenceSetup(
            state=ABSORPTION_FOUND,
            direction=direction,
            reference_bin=reference_bin,
            reference_bins=reference_bins or (dict(reference_bin),),
            reference_candle_time_ms=(
                _integer_field(absorption, "reference_candle_time_ms")
                or _integer_field(candle, "open_time_ms", "open_time")
                or 0
            ),
            confirmation_candle_time_ms=0,
            confirmation_state="",
            candles_since_reference=0,
            lowest_price_since_reference=low_price,
            highest_price_since_reference=high_price,
            reference_zone_low=reference_zone_low,
            reference_zone_high=reference_zone_high,
            stop_loss=stop_loss,
            tick_size=_decimal_from_payload(absorption.get("tick_size")),
        )

    def _entry_absorption_candidate(
        self,
        candle: Mapping[str, Any],
        *,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> dict[str, Any] | None:
        candidates = self._entry_absorption_candidates(
            candle,
            order_book=order_book,
        )
        if not candidates:
            return None
        return candidates[0]

    def _entry_absorption_candidates(
        self,
        candle: Mapping[str, Any],
        *,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> tuple[dict[str, Any], ...]:
        del order_book
        candidates = [
            payload
            for record in self._active_refill_records(candle)
            if _refill_record_matches_reference_candle(candle, record)
            and str(record.action or "").strip().upper() == "ENTRY"
            if (payload := _entry_reference_payload_from_record(record, candle)) is not None
        ]
        return tuple(
            sorted(
                candidates,
                key=lambda item: (
                    _integer_field(
                        item,
                        "timestamp_ms",
                        "event_time_ms",
                        "threshold_time_ms",
                    )
                    or 0,
                    _non_negative_int(item.get("refill_count")),
                    _non_negative_int(item.get("zone_level_count")),
                ),
                reverse=True,
            )
        )

    def _reference_blocked_by_same_side_refill(
        self,
        candle: Mapping[str, Any],
        reference_bin: Mapping[str, Any],
        *,
        direction: str,
    ) -> bool:
        reference_low = _decimal_field(reference_bin, "low", "bin_low")
        reference_high = _decimal_field(reference_bin, "high", "bin_high")
        if reference_low is None or reference_high is None:
            return True
        if str(direction).strip().upper() == "SHORT":
            for record in self._active_refill_records(candle, side="ASK"):
                if record.price > reference_high:
                    return True
            return False
        if str(direction).strip().upper() == "LONG":
            for record in self._active_refill_records(candle, side="BID"):
                if record.price < reference_low:
                    return True
            return False
        return True

    def _reference_bin_has_required_dom_refill(
        self,
        candle: Mapping[str, Any],
        reference_bin: Mapping[str, Any],
        *,
        direction: str,
        order_book: TriggerOrderBookSnapshot | None,
    ) -> bool:
        del order_book
        low = _decimal_field(reference_bin, "low", "bin_low")
        high = _decimal_field(reference_bin, "high", "bin_high")
        if low is None or high is None:
            return False
        lower = min(low, high)
        upper = max(low, high)
        required_side = "BID" if str(direction).strip().upper() == "LONG" else "ASK"
        for record in self._active_refill_records(candle, side=required_side):
            if lower <= record.price <= upper:
                return True
        return False

    @staticmethod
    def _sync_entry_state(state: EntryState) -> None:
        if not state.setups:
            state.state = IDLE
            state.direction = ""
            state.reference_bin = None
            state.reference_bins = ()
            state.reference_candle_time_ms = 0
            state.confirmation_candle_time_ms = 0
            state.confirmation_state = ""
            state.candles_since_reference = 0
            state.lowest_price_since_reference = None
            state.highest_price_since_reference = None
            state.reference_zone_low = None
            state.reference_zone_high = None
            state.stop_loss = None
            return
        setup = state.setups[0]
        state.state = setup.state
        state.direction = setup.direction
        state.reference_bin = setup.reference_bin
        state.reference_bins = setup.reference_bins
        state.reference_candle_time_ms = setup.reference_candle_time_ms
        state.confirmation_candle_time_ms = setup.confirmation_candle_time_ms
        state.confirmation_state = setup.confirmation_state
        state.candles_since_reference = setup.candles_since_reference
        state.lowest_price_since_reference = setup.lowest_price_since_reference
        state.highest_price_since_reference = setup.highest_price_since_reference
        state.reference_zone_low = setup.reference_zone_low
        state.reference_zone_high = setup.reference_zone_high
        state.stop_loss = setup.stop_loss

    def _entry_signal_bin(
        self,
        candle: Mapping[str, Any],
        setup: ReferenceSetup,
        *,
        side: str,
    ) -> dict[str, Any] | None:
        bounds = _reference_bin_bounds(setup)
        if bounds is None:
            return None
        reference_low, reference_high = bounds
        normalized_side = str(side or "").strip().upper()
        if normalized_side == "BUY" and not _is_bullish_candle(candle):
            return None
        if normalized_side == "SELL" and not _is_bearish_candle(candle):
            return None
        matches = []
        for item in _bins(candle):
            center = _bin_center(item)
            if center is None:
                continue
            if normalized_side == "SELL" and center < reference_low:
                continue
            if normalized_side == "BUY" and center > reference_high:
                continue
            if _signal_bin_reference_distance(
                center,
                reference_low=reference_low,
                reference_high=reference_high,
                side=normalized_side,
            ) > SIGNAL_BIN_MAX_DISTANCE_POINTS:
                continue
            if _traded_side(item) != normalized_side:
                continue
            ratio = _diagonal_ratio(item, side=normalized_side)
            if ratio is None or ratio < self.config.diagonal_ratio_min:
                continue
            matches.append((item, ratio))
        if not matches:
            return None
        item, _ratio = max(
            matches,
            key=lambda candidate: (
                candidate[1],
                _bin_center(candidate[0]) or Decimal("0"),
            ),
        )
        return self._bin_payload(
            item,
            entry_direction=setup.direction,
            wick=str(setup.reference_bin.get("wick") if setup.reference_bin else ""),
            side=normalized_side,
        )

    def _exit_buy_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        position: OpenPosition,
        *,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> TriggerSignal | None:
        del order_book
        if not _is_live_candle(candle):
            return None
        refill = self._live_refill_in_wick_or_body_third(
            candle,
            side="ASK",
            wick="UPPER_WICK",
            body_third="UPPER",
        )
        reason = "BUY_EXIT_LIVE_ASK_REFILL_FOUND"
        reference_bin = refill.to_bin_payload() if refill is not None else None
        if refill is None:
            refill = self._touched_strong_refill_level(
                candle,
                side="ASK",
            )
            reason = "BUY_EXIT_OPPOSITE_ASK_STRONG_REFILL_LEVEL"
            reference_bin = refill.to_bin_payload() if refill is not None else None
        if reference_bin is None:
            reference_bin = self._exit_reference_refill_bin(
                candle,
                side="ASK",
                reference_side="BUY",
                wick="LOWER_WICK",
                body_third="LOWER",
                entry_direction="EXIT_BUY",
            )
            reason = "BUY_EXIT_ASK_REFERENCE_BIN_LOWER_WICK_OR_BODY_THIRD"
        if reference_bin is None:
            return None
        exit_price = _decimal_field(candle, "close_price", nested=("ohlc", "close"))
        return self._exit_signal_payload(
            candle,
            next_candle,
            position,
            signal_type="EXIT_BUY",
            direction="EXIT_BUY",
            marker_position="ABOVE",
            marker_color="RED",
            marker_direction="NONE",
            reason=reason,
            reference_bin=reference_bin,
            exit_price=exit_price,
        )

    def _exit_sell_signal(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        position: OpenPosition,
        *,
        order_book: TriggerOrderBookSnapshot | None = None,
    ) -> TriggerSignal | None:
        del order_book
        if not _is_live_candle(candle):
            return None
        refill = self._live_refill_in_wick_or_body_third(
            candle,
            side="BID",
            wick="LOWER_WICK",
            body_third="LOWER",
        )
        reason = "SELL_EXIT_LIVE_BID_REFILL"
        reference_bin = refill.to_bin_payload() if refill is not None else None
        if refill is None:
            refill = self._touched_strong_refill_level(
                candle,
                side="BID",
            )
            reason = "SELL_EXIT_OPPOSITE_BID_STRONG_REFILL_LEVEL"
            reference_bin = refill.to_bin_payload() if refill is not None else None
        if reference_bin is None:
            reference_bin = self._exit_reference_refill_bin(
                candle,
                side="BID",
                reference_side="SELL",
                wick="UPPER_WICK",
                body_third="UPPER",
                entry_direction="EXIT_SELL",
            )
            reason = "SELL_EXIT_BID_REFERENCE_BIN_UPPER_WICK_OR_BODY_THIRD"
        if reference_bin is None:
            return None
        exit_price = _decimal_field(candle, "close_price", nested=("ohlc", "close"))
        return self._exit_signal_payload(
            candle,
            next_candle,
            position,
            signal_type="EXIT_SELL",
            direction="EXIT_SELL",
            marker_position="BELOW",
            marker_color="GREEN",
            marker_direction="NONE",
            reason=reason,
            reference_bin=reference_bin,
            exit_price=exit_price,
        )

    def _live_refill_in_wick_or_body_third(
        self,
        candle: Mapping[str, Any],
        *,
        side: str,
        wick: str,
        body_third: str,
    ) -> RefillRecord | None:
        matches = [
            record
            for record in self._active_refill_records(candle, side=side, live_only=True)
            if _price_in_wick_or_body_third(
                candle,
                record.price,
                wick=wick,
                body_third=body_third,
            )
        ]
        if not matches:
            return None
        return max(matches, key=lambda item: (item.refill_amount, item.timestamp_ms))

    def _touched_strong_refill_level(
        self,
        candle: Mapping[str, Any],
        *,
        side: str,
    ) -> RefillRecord | None:
        bounds = _candle_bounds(candle)
        if bounds is None:
            return None
        low_price, high_price = bounds
        matches = [
            record
            for record in self._active_refill_records(candle, side=side)
            if low_price <= record.price <= high_price
        ]
        if not matches:
            return None
        return max(matches, key=lambda item: (item.refill_amount, item.timestamp_ms))

    def _exit_reference_refill_bin(
        self,
        candle: Mapping[str, Any],
        *,
        side: str,
        reference_side: str,
        wick: str,
        body_third: str,
        entry_direction: str,
    ) -> dict[str, Any] | None:
        matches: list[tuple[Mapping[str, Any], RefillRecord]] = []
        for record in self._active_refill_records(candle, side=side, live_only=True):
            item = _reference_item_from_refill_record(record)
            if item is None:
                continue
            center = _bin_center(item)
            if center is None:
                continue
            if not _is_one_sided_reference_bin(
                candle,
                item,
                reference_side=reference_side,
            ):
                continue
            if not _price_in_wick_or_body_third(
                candle,
                center,
                wick=wick,
                body_third=body_third,
            ):
                continue
            matches.append((item, record))
        if not matches:
            return None
        item, _record = max(
            matches,
            key=lambda candidate: (candidate[1].refill_amount, candidate[1].timestamp_ms),
        )
        return self._bin_payload(
            item,
            entry_direction=entry_direction,
            wick=f"{wick}_OR_BODY_{body_third}_THIRD",
            side=reference_side,
        )

    def _initialize_position_prices(
        self,
        candle: Mapping[str, Any],
        position: OpenPosition,
    ) -> None:
        if position.entry_price is None:
            position.entry_price = _decimal_field(
                candle,
                "open_price",
                nested=("ohlc", "open"),
            )

    def _exit_contract_spike_bin(
        self,
        candle: Mapping[str, Any],
    ) -> Mapping[str, Any] | None:
        matches = [
            item
            for item in _normalized_bins(candle)
            if _contract_spike_score(item) > EXIT_CONTRACT_SPIKE_SCORE_MIN
        ]
        if not matches:
            return None
        return max(
            matches,
            key=lambda item: (
                _contract_spike_score(item),
                _bin_center(item) or Decimal("0"),
            ),
        )

    @staticmethod
    def _exit_spike_price(
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
    ) -> Decimal | None:
        if candle.get("is_live") is True or candle.get("closed") is False:
            return _decimal_field(candle, "close_price", nested=("ohlc", "close"))
        next_open = (
            _decimal_field(next_candle, "open_price", nested=("ohlc", "open"))
            if next_candle is not None
            else None
        )
        if next_open is not None:
            return next_open
        return _decimal_field(candle, "close_price", nested=("ohlc", "close"))

    def _point_value(self, candle: Mapping[str, Any]) -> Decimal:
        payload_value = _decimal_field(
            candle,
            "dollars_per_point",
            "point_value",
        )
        if payload_value is not None and payload_value > 0:
            return payload_value
        candidates = (
            _symbol(candle),
            _provider_symbol(candle),
            _provider_symbol(candle).split(".", 1)[0],
        )
        for candidate in candidates:
            configured = self.config.point_value_by_symbol.get(candidate)
            if configured is not None:
                return configured
        return Decimal("1")

    def _entry_signal_payload(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        state: EntryState | ReferenceSetup,
        *,
        signal_type: str,
        direction: str,
        marker_position: str,
        marker_color: str,
        marker_direction: str,
        reason: str,
        stop_loss: Decimal | None,
        signal_bin: dict[str, Any] | None = None,
    ) -> TriggerSignal:
        assert state.reference_bin is not None
        symbol = _symbol(candle)
        provider_symbol = _provider_symbol(candle)
        timeframe = _timeframe(candle)
        open_time_ms = _integer_field(candle, "open_time_ms", "open_time") or 0
        close_time_ms = _integer_field(candle, "close_time_ms", "close_time") or 0
        action_time_ms = (
            _integer_field(next_candle, "open_time_ms", "open_time")
            if next_candle is not None
            else None
        ) or (close_time_ms + 1)
        entry_price = (
            _decimal_field(next_candle, "open_price", nested=("ohlc", "open"))
            if next_candle is not None
            else None
        )
        position_id = _stable_id(
            "POS",
            symbol,
            timeframe,
            signal_type,
            str(state.reference_candle_time_ms),
            str(open_time_ms),
        )
        signal_id = _stable_id("TRG", position_id, signal_type, str(open_time_ms))
        reference_bin = dict(state.reference_bin)
        return TriggerSignal(
            signal_id=signal_id,
            signal_type=signal_type,
            symbol=symbol,
            provider_symbol=provider_symbol,
            timeframe=timeframe,
            trigger_candle_time_ms=open_time_ms,
            trigger_candle_close_time_ms=close_time_ms,
            action_candle_time_ms=action_time_ms,
            direction=direction,
            position_id=position_id,
            reason=reason,
            wick=str(reference_bin.get("wick") or ""),
            marker_position=marker_position,
            marker_color=marker_color,
            marker_direction=marker_direction,
            marker_shape="ARROW",
            reference_bin=reference_bin,
            matched_bins=(
                (reference_bin, signal_bin)
                if signal_bin is not None
                else (reference_bin,)
            ),
            reference_candle_time_ms=state.reference_candle_time_ms,
            confirmation_candle_time_ms=state.confirmation_candle_time_ms,
            confirmation_state=state.confirmation_state,
            entry_price=entry_price,
            stop_loss=stop_loss,
            trigger_category=str(
                reference_bin.get("trigger_category") or TRIGGER_CATEGORY_ABSORPTION
            ),
            actor_proxy_payload=_actor_proxy_payload_from_reference_bin(reference_bin),
        )

    def _exit_signal_payload(
        self,
        candle: Mapping[str, Any],
        next_candle: Mapping[str, Any] | None,
        position: OpenPosition,
        *,
        signal_type: str,
        direction: str,
        marker_position: str,
        marker_color: str,
        marker_direction: str,
        reason: str,
        reference_bin: dict[str, Any],
        exit_price: Decimal | None = None,
    ) -> TriggerSignal:
        open_time_ms = _integer_field(candle, "open_time_ms", "open_time") or 0
        close_time_ms = _integer_field(candle, "close_time_ms", "close_time") or 0
        next_open_time_ms = (
            _integer_field(next_candle, "open_time_ms", "open_time")
            if next_candle is not None
            else None
        )
        action_time_ms = (
            open_time_ms
            if candle.get("is_live") is True or candle.get("closed") is False
            else (next_open_time_ms or close_time_ms + 1)
        )
        resolved_exit_price = exit_price
        if resolved_exit_price is None:
            resolved_exit_price = (
                _decimal_field(next_candle, "open_price", nested=("ohlc", "open"))
                if next_candle is not None
                else None
            )
        signal_id = _stable_id(
            "TRG",
            position.position_id,
            signal_type,
            str(open_time_ms),
        )
        return TriggerSignal(
            signal_id=signal_id,
            signal_type=signal_type,
            symbol=position.symbol,
            provider_symbol=position.provider_symbol,
            timeframe=position.timeframe,
            trigger_candle_time_ms=open_time_ms,
            trigger_candle_close_time_ms=close_time_ms,
            action_candle_time_ms=action_time_ms,
            direction=direction,
            position_id=position.position_id,
            reason=reason,
            wick=str(reference_bin.get("wick") or ""),
            marker_position=marker_position,
            marker_color=marker_color,
            marker_direction=marker_direction,
            marker_shape="SQUARE",
            reference_bin=reference_bin,
            matched_bins=(reference_bin,),
            exit_price=resolved_exit_price,
        )

    def _reference_bins(
        self,
        candle: Mapping[str, Any],
        *,
        dominance_side: str,
        side_reader: Callable[[Mapping[str, Any]], str] | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        bounds = _candle_bounds(candle)
        if bounds is None:
            return tuple()
        lower_bound, upper_bound = bounds
        matches = []
        read_side = side_reader or _dominance_side
        for item in _bins(candle):
            center = _bin_center(item)
            if center is None or center < lower_bound or center > upper_bound:
                continue
            if (
                read_side(item) == dominance_side
                and _abnormal_score(item)
                >= self.config.reference_contract_spike_score_min
            ):
                matches.append(item)
        return tuple(matches)

    def _abnormal_bins(
        self,
        candle: Mapping[str, Any],
        *,
        wick: str,
        dominance_side: str,
        side_reader: Callable[[Mapping[str, Any]], str] | None = None,
    ) -> tuple[Mapping[str, Any], ...]:
        bounds = _wick_bounds(candle, wick=wick)
        if bounds is None:
            return tuple()
        lower_bound, upper_bound = bounds
        matches = []
        read_side = side_reader or _dominance_side
        for item in _bins(candle):
            center = _bin_center(item)
            if center is None or center < lower_bound or center > upper_bound:
                continue
            if read_side(item) == dominance_side and self._is_abnormal_volume(item):
                matches.append(item)
        return tuple(matches)

    def _is_abnormal_volume(self, item: Mapping[str, Any]) -> bool:
        if _bool_bin_field(item, "abnormal_contract") or _bool_bin_field(
            item,
            "abnormal_volume",
        ):
            return True
        return _abnormal_score(item) >= self.config.contract_spike_score_min

    def _bin_payload(
        self,
        item: Mapping[str, Any],
        *,
        entry_direction: str,
        wick: str,
        side: str | None = None,
    ) -> dict[str, Any]:
        low = _decimal_field(item, "low", "bin_low")
        high = _decimal_field(item, "high", "bin_high")
        score = _abnormal_score(item)
        normalized_side = str(side or _dominance_side(item)).strip().upper()
        return {
            "refill_record_id": str(
                item.get("refill_record_id") or item.get("record_id") or ""
            ),
            "index": item.get("index", item.get("bin_index")),
            "low": str(low) if low is not None else None,
            "high": str(high) if high is not None else None,
            "side": normalized_side,
            "dominance_side": normalized_side,
            "wick": wick,
            "entry_direction": entry_direction,
            "abnormal_volume_score": str(score),
            "spike_score": str(score),
            "contract_spike_score": str(score),
            "buy_diagonal_ratio": str(_buy_diagonal_ratio(item) or Decimal("0")),
            "sell_diagonal_ratio": str(_sell_diagonal_ratio(item) or Decimal("0")),
            "buy_contracts": str(
                _first_bin_decimal(
                    item,
                    ("buy_contracts", "ask_traded_contracts", "ask_traded_volume"),
                )
            ),
            "sell_contracts": str(
                _first_bin_decimal(
                    item,
                    ("sell_contracts", "bid_traded_contracts", "bid_traded_volume"),
                )
            ),
            "buy_volume": str(
                _first_bin_decimal(
                    item,
                    ("buy_volume", "ask_traded_volume"),
                )
            ),
            "sell_volume": str(
                _first_bin_decimal(
                    item,
                    ("sell_volume", "bid_traded_volume"),
                )
            ),
        }

    @staticmethod
    def _update_extreme_since_reference(
        candle: Mapping[str, Any],
        state: EntryState,
    ) -> None:
        low_price = _decimal_field(candle, "low_price", nested=("ohlc", "low"))
        high_price = _decimal_field(candle, "high_price", nested=("ohlc", "high"))
        if low_price is not None:
            state.lowest_price_since_reference = (
                low_price
                if state.lowest_price_since_reference is None
                else min(state.lowest_price_since_reference, low_price)
            )
        if high_price is not None:
            state.highest_price_since_reference = (
                high_price
                if state.highest_price_since_reference is None
                else max(state.highest_price_since_reference, high_price)
            )

    def _reset_entry_state(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
        *,
        reason: str,
    ) -> None:
        state.state = IDLE
        state.direction = ""
        state.reference_bin = None
        state.reference_bins = ()
        state.reference_candle_time_ms = 0
        state.confirmation_candle_time_ms = 0
        state.confirmation_state = ""
        state.candles_since_reference = 0
        state.lowest_price_since_reference = None
        state.highest_price_since_reference = None
        state.reference_zone_low = None
        state.reference_zone_high = None
        state.stop_loss = None
        state.setups = ()
        self._log_state_change(
            symbol=_symbol(candle),
            timeframe=_timeframe(candle),
            transition=reason,
        )

    def _absorption_timeout_expired_before_candle(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
    ) -> bool:
        elapsed_candles = _elapsed_candles_since_reference(candle, state)
        return (
            elapsed_candles is not None
            and elapsed_candles > self.config.absorption_timeout_candles
        )

    def _absorption_timeout_reached(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
    ) -> bool:
        elapsed_candles = _elapsed_candles_since_reference(candle, state)
        if elapsed_candles is not None:
            return elapsed_candles >= self.config.absorption_timeout_candles
        return state.candles_since_reference >= self.config.absorption_timeout_candles

    def _confirmation_timeout_expired_before_candle(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
    ) -> bool:
        elapsed_candles = _elapsed_candles_since_confirmation(candle, state)
        return (
            elapsed_candles is not None
            and elapsed_candles > self.config.absorption_timeout_candles
        )

    def _confirmation_timeout_reached(
        self,
        candle: Mapping[str, Any],
        state: EntryState,
    ) -> bool:
        elapsed_candles = _elapsed_candles_since_confirmation(candle, state)
        return (
            elapsed_candles is not None
            and elapsed_candles >= self.config.absorption_timeout_candles
        )

    @staticmethod
    def _is_closed(
        candle: Mapping[str, Any],
        *,
        evaluation_time_ms: int,
    ) -> bool:
        if candle.get("is_live") is True or candle.get("closed") is False:
            return False
        open_time_ms = _integer_field(candle, "open_time_ms", "open_time")
        close_time_ms = _integer_field(candle, "close_time_ms", "close_time")
        if open_time_ms is None or close_time_ms is None or close_time_ms < open_time_ms:
            return False
        return int(evaluation_time_ms) > close_time_ms

    def _log_state_change(
        self,
        *,
        symbol: str,
        timeframe: str,
        transition: str,
        reference_bin: Mapping[str, Any] | None = None,
    ) -> None:
        if not self.config.runtime_logging_enabled:
            return
        LOGGER.info(
            "TRIGGER_STATE_CHANGE | symbol=%s | timeframe=%s | transition=%s | reference_bin_low=%s | reference_bin_high=%s | reference_bin_side=%s | spike_score=%s",
            symbol,
            timeframe,
            transition,
            (reference_bin or {}).get("low"),
            (reference_bin or {}).get("high"),
            (reference_bin or {}).get("side"),
            (reference_bin or {}).get("spike_score"),
        )

    def _start_actor_proxy_tracking(self, signal: TriggerSignal) -> None:
        actor_engine = self.actor_proxy_engine
        actor_payload = signal.actor_proxy_payload
        if actor_engine is None or not isinstance(actor_payload, Mapping):
            return
        start_tracking = getattr(actor_engine, "start_tracking", None)
        if not callable(start_tracking):
            return
        position_context = {
            "position_id": signal.position_id,
            "side": signal.direction,
            "entry_price": str(signal.entry_price) if signal.entry_price is not None else "",
            "entry_time_ms": int(signal.action_candle_time_ms),
            "symbol": signal.symbol,
            "provider_symbol": signal.provider_symbol,
            "timeframe": signal.timeframe,
            "mode": _actor_proxy_tracking_mode(actor_payload),
            "tracking_mode": _actor_proxy_tracking_mode(actor_payload),
            "tracking_start_ms": (
                actor_payload.get("tracking_start_ms")
                or actor_payload.get("replay_start_ms")
                or signal.action_candle_time_ms
            ),
            "tracking_end_ms": (
                actor_payload.get("tracking_end_ms")
                or actor_payload.get("replay_end_ms")
            ),
        }
        start_tracking(actor_payload, position_context)

    def _stop_actor_proxy_tracking(self, position_id: str) -> None:
        actor_engine = self.actor_proxy_engine
        if actor_engine is None:
            return
        stop_tracking = getattr(actor_engine, "stop_tracking", None)
        if callable(stop_tracking):
            stop_tracking(position_id)

    def _log_signal(self, signal: TriggerSignal) -> None:
        if not self.config.runtime_logging_enabled:
            return
        LOGGER.info(
            "TRIGGER_SIGNAL | signal_type=%s | symbol=%s | timeframe=%s | candle_time=%s | reference_bin_low=%s | reference_bin_high=%s | reference_bin_side=%s | spike_score=%s | entry_price=%s | exit_price=%s | stop_loss=%s | reason=%s | position_id=%s",
            signal.signal_type,
            signal.symbol,
            signal.timeframe,
            signal.trigger_candle_time_ms,
            signal.reference_bin.get("low"),
            signal.reference_bin.get("high"),
            signal.reference_bin.get("side"),
            signal.reference_bin.get("spike_score"),
            signal.entry_price,
            signal.exit_price,
            signal.stop_loss,
            signal.reason,
            signal.position_id,
        )


def _bool_config_value(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _reference_payload_id(reference_bin: Mapping[str, Any] | None) -> str:
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


def _actor_proxy_payload_from_reference_bin(
    reference_bin: Mapping[str, Any] | None,
) -> Mapping[str, Any] | None:
    if not isinstance(reference_bin, Mapping):
        return None
    actor_payload = reference_bin.get("actor_proxy_payload")
    if isinstance(actor_payload, Mapping):
        return dict(actor_payload)
    return None


def _actor_proxy_tracking_mode(actor_payload: Mapping[str, Any]) -> str:
    raw_mode = str(
        actor_payload.get("tracking_mode")
        or actor_payload.get("mode")
        or "REPLAY"
    ).strip().upper()
    if raw_mode == "REPLAY_STUDY":
        return "REPLAY"
    if raw_mode in {"LIVE", "REPLAY"}:
        return raw_mode
    return "REPLAY"


def _refill_records_from_payload(
    payload: Mapping[str, Any] | None,
    config: TriggerConfig,
    *,
    symbol: str | None = None,
    provider_symbol: str | None = None,
    timeframe: str | None = None,
    timestamp_ms: int | None = None,
) -> tuple[RefillRecord, ...]:
    if payload is None or not isinstance(payload, Mapping):
        return tuple()
    context_symbol = str(
        symbol
        or payload.get("mt5_symbol")
        or payload.get("symbol")
        or payload.get("provider_symbol")
        or ""
    ).strip().upper()
    context_provider_symbol = str(
        provider_symbol
        or payload.get("provider_symbol")
        or payload.get("symbol")
        or context_symbol
    ).strip().upper()
    context_timeframe = str(timeframe or payload.get("timeframe") or "").strip().upper()
    context_timestamp = (
        int(timestamp_ms)
        if timestamp_ms is not None
        else (
            _integer_field(
                payload,
                "timestamp_ms",
                "event_time_ms",
                "threshold_time_ms",
                "window_end_ms",
                "end_time_ms",
                "close_time_ms",
                "close_time",
            )
            or 0
        )
    )

    records: dict[str, RefillRecord] = {}

    def add_from_mapping(
        item: Mapping[str, Any],
        *,
        fallback_timestamp_ms: int = context_timestamp,
    ) -> None:
        record = _refill_record_from_mapping(
            item,
            config,
            symbol=context_symbol,
            provider_symbol=context_provider_symbol,
            timeframe=context_timeframe,
            timestamp_ms=fallback_timestamp_ms,
        )
        if record is not None:
            records[record.record_id] = record

    add_from_mapping(payload)

    for session_payload in _iter_mappings(payload.get("sessions")):
        for record in _refill_records_from_payload(
            session_payload,
            config,
            symbol=context_symbol,
            provider_symbol=context_provider_symbol,
            timeframe=context_timeframe,
            timestamp_ms=context_timestamp,
        ):
            records[record.record_id] = record

    for key in ("dom_refill_points", "dom_refill_markers"):
        for item in _iter_mappings(payload.get(key)):
            add_from_mapping(item)

    for item in extract_engine_outputs(payload):
        add_from_mapping(item)

    for key in ("events", "raw_events", "resting_segments"):
        for item in _iter_mappings(payload.get(key)):
            add_from_mapping(item)

    raw_levels = payload.get("order_book_levels")
    if raw_levels is None:
        raw_levels = payload.get("levels")
    for item in _iter_mappings(raw_levels):
        add_from_mapping(item)

    return tuple(records.values())


def _entry_reference_payload_from_record(
    record: RefillRecord,
    candle: Mapping[str, Any],
) -> dict[str, Any] | None:
    if record.zone_low is None or record.zone_high is None:
        return None
    zone_low = min(record.zone_low, record.zone_high)
    zone_high = max(record.zone_low, record.zone_high)
    direction = _entry_direction_from_payload(record.payload, record.side)
    if direction not in {"LONG", "SHORT"}:
        return None
    reference_side = str(
        record.payload.get("reference_side")
        or record.payload.get("reference_bin_side")
        or ("SELL" if direction == "LONG" else "BUY")
    ).strip().upper()
    if reference_side not in {"BUY", "SELL"}:
        reference_side = "SELL" if direction == "LONG" else "BUY"
    wick = str(
        record.payload.get("wick")
        or (
            "LOWER_WICK_OR_BODY_THIRD"
            if direction == "LONG"
            else "UPPER_WICK_OR_BODY_THIRD"
        )
    ).strip().upper()
    payload = dict(record.payload)
    payload.pop("refill_kind", None)
    payload.update(
        {
            "payload_id": str(
                record.payload.get("payload_id")
                or record.payload.get("output_id")
                or record.payload.get("id")
                or record.record_id
            ),
            "id": str(record.payload.get("id") or record.record_id),
            "output_id": str(record.payload.get("output_id") or record.record_id),
            "source_payload_id": str(
                record.payload.get("payload_id")
                or record.payload.get("output_id")
                or record.payload.get("id")
                or record.record_id
            ),
            "process_payload_id": str(
                record.payload.get("payload_id")
                or record.payload.get("output_id")
                or record.payload.get("id")
                or record.record_id
            ),
            "refill_record_id": record.record_id,
            "record_id": record.record_id,
            "low": str(zone_low),
            "high": str(zone_high),
            "zone_low": str(zone_low),
            "zone_high": str(zone_high),
            "reference_zone_low": str(zone_low),
            "reference_zone_high": str(zone_high),
            "side": reference_side,
            "dominance_side": reference_side,
            "reference_side": reference_side,
            "entry_direction": direction,
            "wick": wick,
            "type": record.payload_type,
            "payload_type": record.payload_type,
            "action": "ENTRY",
            "refill_side": record.side,
            "refill_count": int(record.refill_count),
            "refill_total": int(record.refill_total),
            "price_base_refill_count": int(record.refill_count),
            "price_base_refill_contracts": int(record.refill_total),
            "refill_added_contracts": int(record.refill_total),
            "executed_refill_contracts": int(record.executed_refill_contracts),
            "withdrawn_refill_contracts": int(record.withdrawn_refill_contracts),
            "refill_method": "price_base_refill",
            "market_buy": int(record.market_buy),
            "market_sell": int(record.market_sell),
            "market_buy_contracts": int(record.market_buy),
            "market_sell_contracts": int(record.market_sell),
            "reference_candle_time_ms": int(
                record.footprint_open_time_ms
                or _integer_field(candle, "open_time_ms", "open_time")
                or 0
            ),
            "trigger_category": str(
                record.payload.get("trigger_category") or TRIGGER_CATEGORY_ABSORPTION
            ),
        }
    )
    if not str(payload.get("source") or "").strip():
        payload["source"] = record.source or "DATA_PROCESS_REFILL_ZONE"
    if not payload.get("refill_record_ids"):
        payload["refill_record_ids"] = (record.record_id,)
    return payload


def _entry_direction_from_payload(
    payload: Mapping[str, Any],
    refill_side: str,
) -> str:
    raw_direction = str(
        payload.get("entry_direction")
        or payload.get("direction")
        or payload.get("trade_direction")
        or ""
    ).strip().upper()
    aliases = {
        "BUY": "LONG",
        "LONG": "LONG",
        "BUY_ENTRY": "LONG",
        "SELL": "SHORT",
        "SHORT": "SHORT",
        "SELL_ENTRY": "SHORT",
    }
    if raw_direction in aliases:
        return aliases[raw_direction]
    normalized_side = _normalized_dom_side(refill_side or payload.get("refill_side"))
    if normalized_side == "BID":
        return "LONG"
    if normalized_side == "ASK":
        return "SHORT"
    return ""


def _reference_item_from_refill_record(record: RefillRecord) -> dict[str, Any] | None:
    if int(record.market_buy) <= 0 and int(record.market_sell) <= 0:
        return None
    low = record.footprint_bin_low or record.price
    high = record.footprint_bin_high or record.price
    if high < low:
        low, high = high, low
    return {
        "payload_id": record.record_id,
        "id": record.record_id,
        "output_id": record.record_id,
        "source_payload_id": record.record_id,
        "process_payload_id": record.record_id,
        "refill_record_id": record.record_id,
        "index": None,
        "low": str(low),
        "high": str(high),
        "l2": {
            "quantity_unit": "CONTRACTS",
            "buy_contracts": str(int(record.market_buy)),
            "sell_contracts": str(int(record.market_sell)),
            "ask_traded_contracts": str(int(record.market_buy)),
            "bid_traded_contracts": str(int(record.market_sell)),
            "contract_spike_score": "0",
            "abnormal_volume_score": "0",
            "spike_score": "0",
        },
        "refill_count": int(record.refill_count),
        "refill_total": int(record.refill_total),
        "price_base_refill_count": int(record.refill_count),
        "price_base_refill_contracts": int(record.refill_total),
        "refill_added_contracts": int(record.refill_total),
        "executed_refill_contracts": int(record.executed_refill_contracts),
        "withdrawn_refill_contracts": int(record.withdrawn_refill_contracts),
        "refill_method": "price_base_refill",
        "refill_side": record.side,
        "action": record.action,
        "source": record.source,
        "footprint_open_time_ms": int(record.footprint_open_time_ms),
    }


def _refill_record_from_mapping(
    payload: Mapping[str, Any],
    config: TriggerConfig,
    *,
    symbol: str,
    provider_symbol: str,
    timeframe: str,
    timestamp_ms: int,
    ) -> RefillRecord | None:
    price = _decimal_from_payload(
        payload.get("price")
        or payload.get("threshold_price")
        or payload.get("level_price")
    )
    side = _normalized_dom_side(
        payload.get("side")
        or payload.get("top_order_side")
        or payload.get("book_side")
    )
    if price is None or side not in {"BID", "ASK"}:
        return None

    if (
        payload.get("price_base_refill_count") in {None, ""}
        or payload.get("price_base_refill_contracts") in {None, ""}
    ):
        return None
    refill_count = _non_negative_int(payload.get("price_base_refill_count"))
    refill_total = _non_negative_int(payload.get("price_base_refill_contracts"))
    executed_refill_contracts = min(
        refill_total, _non_negative_int(payload.get("executed_refill_contracts"))
    )
    has_price_activity = bool(payload.get("has_price_activity"))
    if refill_count <= 0 and not has_price_activity:
        return None

    normalized_symbol = str(
        payload.get("mt5_symbol")
        or payload.get("symbol")
        or symbol
        or payload.get("provider_symbol")
        or ""
    ).strip().upper()
    normalized_provider_symbol = str(
        payload.get("provider_symbol")
        or payload.get("symbol")
        or provider_symbol
        or normalized_symbol
    ).strip().upper()
    normalized_timeframe = str(payload.get("timeframe") or timeframe or "").strip().upper()
    if not normalized_symbol or not normalized_timeframe:
        return None
    normalized_timestamp = (
        _integer_field(
            payload,
            "timestamp_ms",
            "event_time_ms",
            "threshold_time_ms",
            "time_ms",
            "ts_event_ms",
            "close_time_ms",
            "close_time",
        )
        or int(timestamp_ms or 0)
    )
    record_id = str(payload.get("payload_id") or payload.get("id") or payload.get("output_id") or "").strip()
    if not record_id:
        record_id = _stable_id(
            "RFL",
            normalized_symbol,
            normalized_provider_symbol,
            normalized_timeframe,
            str(normalized_timestamp),
            str(price),
            side,
            str(payload.get("order_id") or payload.get("venue_order_id") or ""),
            str(refill_count),
            str(refill_total),
        )
    market_buy = max(
        _non_negative_int(payload.get("market_buy")),
        _non_negative_int(payload.get("market_buy_contracts")),
        _non_negative_int(payload.get("ask_traded_contracts")),
        _non_negative_int(payload.get("ask_traded_volume")),
        _non_negative_int(payload.get("buy_contracts")),
        _non_negative_int(payload.get("buy_volume")),
    )
    market_sell = max(
        _non_negative_int(payload.get("market_sell")),
        _non_negative_int(payload.get("market_sell_contracts")),
        _non_negative_int(payload.get("bid_traded_contracts")),
        _non_negative_int(payload.get("bid_traded_volume")),
        _non_negative_int(payload.get("sell_contracts")),
        _non_negative_int(payload.get("sell_volume")),
    )
    footprint_bin_low = _decimal_from_payload(
        payload.get("footprint_bin_low")
        or payload.get("bin_low")
        or payload.get("price_low")
        or payload.get("low")
    )
    footprint_bin_high = _decimal_from_payload(
        payload.get("footprint_bin_high")
        or payload.get("bin_high")
        or payload.get("price_high")
        or payload.get("high")
    )
    zone_low = _decimal_from_payload(
        payload.get("zone_low")
        or payload.get("reference_zone_low")
    )
    zone_high = _decimal_from_payload(
        payload.get("zone_high")
        or payload.get("reference_zone_high")
    )
    return RefillRecord(
        record_id=record_id,
        symbol=normalized_symbol,
        provider_symbol=normalized_provider_symbol,
        timeframe=normalized_timeframe,
        timestamp_ms=normalized_timestamp,
        price=price,
        side=side,
        refill_count=refill_count,
        refill_total=refill_total,
        executed_refill_contracts=executed_refill_contracts,
        withdrawn_refill_contracts=_non_negative_int(payload.get("withdrawn_refill_contracts")),
        has_refill=refill_count > 0,
        has_price_activity=has_price_activity,
        order_count=_non_negative_int(payload.get("order_count")),
        added_contracts=_non_negative_int(payload.get("added_contracts")),
        opening_liquidity=_non_negative_int(payload.get("opening_liquidity")),
        available_liquidity=_non_negative_int(payload.get("available_liquidity")),
        gross_added_contracts=_non_negative_int(
            payload.get("gross_added_contracts") or payload.get("added_contracts")
        ),
        non_refill_added_contracts=_non_negative_int(payload.get("non_refill_added_contracts")),
        fill_event_count=_non_negative_int(payload.get("fill_event_count")),
        executed_contracts=_non_negative_int(payload.get("executed_contracts")),
        withdrawn_contracts=_non_negative_int(
            payload.get("withdrawn_contracts")
            or payload.get("cancelled_or_withdrawn_contracts")
        ),
        closing_liquidity=_non_negative_int(payload.get("closing_liquidity")),
        level_execution_rate=float(payload.get("level_execution_rate") or 0.0),
        level_execution_rate_defined=bool(payload.get("level_execution_rate_defined")),
        level_execution_invariant_ok=bool(payload.get("level_execution_invariant_ok", True)),
        added_breakdown_invariant_ok=bool(payload.get("added_breakdown_invariant_ok", True)),
        action=str(payload.get("action") or "ENTRY").strip().upper(),
        payload_type=_payload_type_from_mapping(payload),
        source=str(payload.get("source") or payload.get("source_engine") or payload.get("output_type") or ""),
        market_buy=market_buy,
        market_sell=market_sell,
        footprint_open_time_ms=_integer_field(
            payload,
            "footprint_open_time_ms",
            "footprint_open_time",
        )
        or 0,
        footprint_bin_low=footprint_bin_low,
        footprint_bin_high=footprint_bin_high,
        zone_low=zone_low,
        zone_high=zone_high,
        zone_level_count=_non_negative_int(payload.get("zone_level_count")),
        terminal_market_buy=max(
            _non_negative_int(payload.get("terminal_market_buy")),
            _non_negative_int(payload.get("terminal_market_buy_contracts")),
        ),
        terminal_market_sell=max(
            _non_negative_int(payload.get("terminal_market_sell")),
            _non_negative_int(payload.get("terminal_market_sell_contracts")),
        ),
        payload=dict(payload),
    )


def _payload_type_from_mapping(payload: Mapping[str, Any]) -> str:
    raw_value = str(
        payload.get("payload_type")
        or payload.get("type")
        or payload.get("trigger_category")
        or ""
    ).strip().upper()
    if raw_value in {TRIGGER_CATEGORY_ABSORPTION, TRIGGER_CATEGORY_ICEBURG}:
        return raw_value
    return TRIGGER_CATEGORY_ABSORPTION


def _refill_age_candles(
    candle: Mapping[str, Any],
    record: RefillRecord,
) -> int | None:
    if record.timestamp_ms <= 0:
        return 0
    open_time_ms = _integer_field(candle, "open_time_ms", "open_time")
    close_time_ms = _integer_field(candle, "close_time_ms", "close_time")
    if close_time_ms is not None and record.timestamp_ms > close_time_ms:
        return None
    if open_time_ms is None:
        return 0
    if record.timestamp_ms >= open_time_ms:
        return 0
    interval_ms = TIMEFRAME_INTERVAL_MS.get(_timeframe(candle))
    if interval_ms is None or interval_ms <= 0:
        return 0
    return max(
        0,
        (int(open_time_ms) - int(record.timestamp_ms) + int(interval_ms) - 1)
        // int(interval_ms),
    )


def _refill_record_matches_reference_candle(
    candle: Mapping[str, Any],
    record: RefillRecord,
) -> bool:
    open_time_ms = _integer_field(candle, "open_time_ms", "open_time")
    if open_time_ms is None:
        return False
    if int(record.footprint_open_time_ms) > 0:
        return int(record.footprint_open_time_ms) == int(open_time_ms)

    close_time_ms = _integer_field(candle, "close_time_ms", "close_time")
    if int(record.timestamp_ms) <= 0 or close_time_ms is None:
        return False
    return int(open_time_ms) <= int(record.timestamp_ms) <= int(close_time_ms)


def _refill_record_is_active(
    record: RefillRecord,
    *,
    age: int,
    config: TriggerConfig,
) -> bool:
    del record
    return int(age) <= int(config.refill_lifetime_candles)


def _normalized_dom_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    aliases = {
        "B": "BID",
        "BUY": "BID",
        "BID": "BID",
        "A": "ASK",
        "SELL": "ASK",
        "ASK": "ASK",
    }
    return aliases.get(side, side)


def order_book_snapshot_from_payload(
    payload: TriggerOrderBookSnapshot | Mapping[str, Any] | None,
    *,
    symbol: str | None = None,
    provider_symbol: str | None = None,
    timeframe: str | None = None,
    timestamp_ms: int | None = None,
) -> TriggerOrderBookSnapshot | None:
    snapshots = order_book_snapshots_from_payload(
        payload,
        symbol=symbol,
        provider_symbol=provider_symbol,
        timeframe=timeframe,
        timestamp_ms=timestamp_ms,
    )
    return snapshots[0] if snapshots else None


def order_book_snapshots_from_payload(
    payload: TriggerOrderBookSnapshot | Mapping[str, Any] | None,
    *,
    symbol: str | None = None,
    provider_symbol: str | None = None,
    timeframe: str | None = None,
    timestamp_ms: int | None = None,
) -> tuple[TriggerOrderBookSnapshot, ...]:
    if payload is None:
        return tuple()
    if isinstance(payload, TriggerOrderBookSnapshot):
        return (payload,)
    if not isinstance(payload, Mapping):
        return tuple()
    raw_levels = payload.get("levels")
    if raw_levels is None:
        raw_levels = payload.get("order_book_levels")
    raw_sessions = payload.get("sessions")
    if raw_levels is None and raw_sessions is not None:
        snapshots: list[TriggerOrderBookSnapshot] = []
        for session_payload in _iter_mappings(raw_sessions):
            snapshot = _single_order_book_snapshot_from_payload(
                session_payload,
                symbol=symbol,
                provider_symbol=provider_symbol,
                timeframe=timeframe or str(payload.get("timeframe") or ""),
                timestamp_ms=timestamp_ms,
            )
            if snapshot is not None:
                snapshots.append(snapshot)
        return tuple(snapshots)
    snapshot = _single_order_book_snapshot_from_payload(
        payload,
        symbol=symbol,
        provider_symbol=provider_symbol,
        timeframe=timeframe,
        timestamp_ms=timestamp_ms,
    )
    return (snapshot,) if snapshot is not None else tuple()


def _single_order_book_snapshot_from_payload(
    payload: Mapping[str, Any],
    *,
    symbol: str | None = None,
    provider_symbol: str | None = None,
    timeframe: str | None = None,
    timestamp_ms: int | None = None,
) -> TriggerOrderBookSnapshot | None:
    raw_levels = payload.get("levels")
    if raw_levels is None:
        raw_levels = payload.get("order_book_levels")
    levels = tuple(
        sorted(
            (
                level
                for item in _iter_mappings(raw_levels)
                if (level := _order_book_level_from_payload(item)) is not None
            ),
            key=lambda item: item.price,
        )
    )
    events = _mapping_tuple(payload.get("events"))
    engine_output_events = extract_engine_outputs(payload)
    if engine_output_events:
        events = (*events, *engine_output_events)
    raw_events = _mapping_tuple(payload.get("raw_events"))
    resting_segments = _mapping_tuple(payload.get("resting_segments"))
    best_bid_line = _mapping_tuple(payload.get("best_bid_line"))
    best_ask_line = _mapping_tuple(payload.get("best_ask_line"))
    if not (
        levels
        or events
        or raw_events
        or resting_segments
        or best_bid_line
        or best_ask_line
    ):
        return None

    normalized_symbol = str(
        symbol
        or payload.get("mt5_symbol")
        or payload.get("symbol")
        or payload.get("provider_symbol")
        or ""
    ).strip().upper()
    normalized_provider_symbol = str(
        provider_symbol
        or payload.get("provider_symbol")
        or payload.get("symbol")
        or normalized_symbol
    ).strip().upper()
    normalized_timeframe = str(
        timeframe
        or payload.get("timeframe")
        or ""
    ).strip().upper()
    if not normalized_symbol or not normalized_timeframe:
        return None

    normalized_timestamp = (
        int(timestamp_ms)
        if timestamp_ms is not None
        else (
            _integer_field(payload, "timestamp_ms", "window_end_ms", "end_time_ms")
            or 0
        )
    )
    best_bid = (
        _decimal_from_payload(payload.get("best_bid"))
        or _best_bid(levels)
        or _last_line_price(best_bid_line)
    )
    best_ask = (
        _decimal_from_payload(payload.get("best_ask"))
        or _best_ask(levels)
        or _last_line_price(best_ask_line)
    )
    return TriggerOrderBookSnapshot(
        symbol=normalized_symbol,
        provider_symbol=normalized_provider_symbol,
        timeframe=normalized_timeframe,
        timestamp_ms=normalized_timestamp,
        levels=levels,
        best_bid=best_bid,
        best_ask=best_ask,
        source=str(payload.get("source") or payload.get("output_type") or payload.get("type") or "DOM"),
        events=events,
        raw_events=raw_events,
        resting_segments=resting_segments,
        best_bid_line=best_bid_line,
        best_ask_line=best_ask_line,
        iceberg_filter=_mapping(payload.get("iceberg_filter")),
        debug=_mapping(payload.get("debug")),
        viewport_metrics=_mapping(payload.get("viewport_metrics")),
        dom_payload=dict(payload),
    )


def _order_book_level_from_payload(
    payload: Mapping[str, Any],
) -> TriggerOrderBookLevel | None:
    price = _decimal_from_payload(payload.get("price"))
    if price is None:
        return None
    bid_contracts = _non_negative_int(
        payload.get("bid_contracts", payload.get("bid", 0))
    )
    ask_contracts = _non_negative_int(
        payload.get("ask_contracts", payload.get("ask", 0))
    )
    raw_buy_execute_contracts = _non_negative_int(
        payload.get("raw_buy_execute_contracts", payload.get("buy_execute_contracts", 0))
    )
    raw_sell_execute_contracts = _non_negative_int(
        payload.get("raw_sell_execute_contracts", payload.get("sell_execute_contracts", 0))
    )
    top_order_id = str(payload.get("top_order_id") or payload.get("order_id") or "")
    top_order_side = str(payload.get("top_order_side") or payload.get("side") or "")
    top_order_type = str(payload.get("top_order_type") or payload.get("event_type") or "")
    top_order_size = _non_negative_int(payload.get("top_order_size", 0))
    top_order_count = _non_negative_int(payload.get("top_order_count", 0))
    top_order_rank = _non_negative_int(payload.get("top_order_rank", 0))
    top_order_last_size = _non_negative_int(payload.get("top_order_last_size", 0))
    top_order_current_contracts = _non_negative_int(
        payload.get("top_order_current_contracts", payload.get("order_current_contracts", 0))
    )
    top_order_positive_refill_count = _non_negative_int(
        payload.get("top_order_positive_refill_count", 0)
    )
    top_order_positive_refill_total = _non_negative_int(
        payload.get("top_order_positive_refill_total", 0)
    )
    if not any(
        (
            bid_contracts,
            ask_contracts,
            raw_buy_execute_contracts,
            raw_sell_execute_contracts,
            top_order_id,
            top_order_size,
            top_order_count,
            top_order_rank,
            top_order_last_size,
            top_order_current_contracts,
            top_order_positive_refill_count,
            top_order_positive_refill_total,
        )
    ):
        return None
    return TriggerOrderBookLevel(
        price=price,
        bid_contracts=bid_contracts,
        ask_contracts=ask_contracts,
        raw_buy_execute_contracts=raw_buy_execute_contracts,
        raw_sell_execute_contracts=raw_sell_execute_contracts,
        top_order_id=top_order_id,
        top_order_side=top_order_side,
        top_order_type=top_order_type,
        top_order_size=top_order_size,
        top_order_count=top_order_count,
        top_order_rank=top_order_rank,
        top_order_last_size=top_order_last_size,
        top_order_current_contracts=top_order_current_contracts,
        top_order_positive_refill_count=top_order_positive_refill_count,
        top_order_positive_refill_total=top_order_positive_refill_total,
    )


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _mapping_tuple(value: Any) -> tuple[dict[str, Any], ...]:
    return tuple(dict(item) for item in _iter_mappings(value))


def _iter_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        return
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for item in value:
            if isinstance(item, Mapping):
                yield item


def _iterable_payload_values(value: Any) -> Iterable[Any]:
    if value is None or value == "":
        return tuple()
    if isinstance(value, (str, bytes)):
        return (value,)
    if isinstance(value, Iterable):
        return tuple(value)
    return (value,)


def _non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _best_bid(levels: Iterable[TriggerOrderBookLevel]) -> Decimal | None:
    prices = [level.price for level in levels if level.bid_contracts > 0]
    return max(prices) if prices else None


def _best_ask(levels: Iterable[TriggerOrderBookLevel]) -> Decimal | None:
    prices = [level.price for level in levels if level.ask_contracts > 0]
    return min(prices) if prices else None


def _last_line_price(items: Iterable[Mapping[str, Any]]) -> Decimal | None:
    last_price: Decimal | None = None
    for item in items:
        price = _decimal_from_payload(item.get("price"))
        if price is not None:
            last_price = price
    return last_price


def _event_timestamp_ms(payload: Mapping[str, Any]) -> int:
    return (
        _integer_field(payload, "timestamp_ms", "ts_event_ms", "time_ms", "timestamp")
        or 0
    )


def _dom_event_order_id(payload: Mapping[str, Any]) -> str:
    return str(payload.get("order_id") or payload.get("venue_order_id") or "").strip()


def _dom_event_identity(payload: Mapping[str, Any]) -> tuple[str, str, str, str, str, str]:
    event_id = str(payload.get("event_id") or "").strip()
    if event_id:
        return ("event_id", event_id, "", "", "", "")
    return (
        str(_event_timestamp_ms(payload)),
        _dom_event_order_id(payload),
        str(payload.get("action") or payload.get("event_type") or "").strip().upper(),
        str(payload.get("price") or "").strip(),
        str(payload.get("side") or "").strip().upper(),
        str(payload.get("order_size") or payload.get("raw_event_size") or "").strip(),
    )


def _dom_event_refill_count(payload: Mapping[str, Any]) -> int:
    return _non_negative_int(payload.get("price_base_refill_count"))


def _dom_refill_points(order_book: TriggerOrderBookSnapshot) -> tuple[Mapping[str, Any], ...]:
    payload = order_book.dom_payload if isinstance(order_book.dom_payload, Mapping) else {}
    points = payload.get("dom_refill_points", ())
    dom_points = (
        tuple(item for item in points if isinstance(item, Mapping))
        if isinstance(points, Iterable) and not isinstance(points, (str, bytes, Mapping))
        else tuple()
    )
    engine_points = tuple(
        item
        for item in extract_engine_outputs(payload)
        if str(item.get("output_type") or item.get("type") or "").strip().upper()
        == DOM_POSITIVE_REFILL_OUTPUT_TYPE
    )
    return (*dom_points, *engine_points)


def _dom_side_matches(value: Any, required_side: str) -> bool:
    return _normalized_dom_side(value) == _normalized_dom_side(required_side)


def _dom_event_matches_reference_bin(
    event: Mapping[str, Any],
    *,
    lower: Decimal,
    upper: Decimal,
    required_side: str,
) -> bool:
    price = _decimal_from_payload(event.get("price"))
    return (
        price is not None
        and lower <= price <= upper
        and _dom_side_matches(event.get("side"), required_side)
    )


def _dom_resting_segment_matches_reference_bin(
    segment: Mapping[str, Any],
    *,
    lower: Decimal,
    upper: Decimal,
    required_side: str,
    start_ms: int,
    end_ms: int,
) -> bool:
    if not _dom_event_matches_reference_bin(
        segment,
        lower=lower,
        upper=upper,
        required_side=required_side,
    ):
        return False
    segment_start = _integer_field(segment, "start_ms", "timestamp_ms", "ts_event_ms") or int(start_ms)
    segment_end = _integer_field(segment, "end_ms", "timestamp_ms", "ts_event_ms") or int(end_ms)
    return int(segment_start) <= int(end_ms) and int(segment_end) >= int(start_ms)


def _order_book_for_candle_time(
    candle: Mapping[str, Any],
    order_books_by_time: Mapping[int, TriggerOrderBookSnapshot | Mapping[str, Any]] | None,
) -> TriggerOrderBookSnapshot | Mapping[str, Any] | None:
    if not order_books_by_time:
        return None
    for value in (
        _integer_field(candle, "close_time_ms", "close_time"),
        _integer_field(candle, "open_time_ms", "open_time"),
    ):
        if value is not None and value in order_books_by_time:
            return order_books_by_time[value]
    return None


def _sorted_candles(
    candles: Iterable[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    return sorted(
        list(candles),
        key=lambda item: _integer_field(item, "open_time_ms", "open_time") or 0,
    )


def _sorted_mutable_candles(candles: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        list(candles),
        key=lambda item: _integer_field(item, "open_time_ms", "open_time") or 0,
    )


def _stable_id(prefix: str, *parts: str) -> str:
    identity = "|".join(parts)
    return f"{prefix}-{hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20].upper()}"


def _integer_field(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _timeframe(candle: Mapping[str, Any]) -> str:
    return str(candle.get("timeframe", "")).strip().upper()


def _elapsed_candles_since_reference(
    candle: Mapping[str, Any],
    state: EntryState,
) -> int | None:
    return _elapsed_candles_since_time(
        candle,
        start_time_ms=state.reference_candle_time_ms,
    )


def _elapsed_candles_since_confirmation(
    candle: Mapping[str, Any],
    state: EntryState,
) -> int | None:
    return _elapsed_candles_since_time(
        candle,
        start_time_ms=state.confirmation_candle_time_ms,
    )


def _elapsed_candles_since_time(
    candle: Mapping[str, Any],
    *,
    start_time_ms: int,
) -> int | None:
    current_open_time_ms = _integer_field(candle, "open_time_ms", "open_time")
    interval_ms = TIMEFRAME_INTERVAL_MS.get(_timeframe(candle))
    if (
        current_open_time_ms is None
        or start_time_ms <= 0
        or interval_ms is None
        or interval_ms <= 0
        or current_open_time_ms < start_time_ms
    ):
        return None
    return (current_open_time_ms - start_time_ms) // interval_ms


def _wick_bounds(
    candle: Mapping[str, Any],
    *,
    wick: str,
) -> tuple[Decimal, Decimal] | None:
    open_price = _decimal_field(candle, "open_price", nested=("ohlc", "open"))
    high_price = _decimal_field(candle, "high_price", nested=("ohlc", "high"))
    low_price = _decimal_field(candle, "low_price", nested=("ohlc", "low"))
    close_price = _decimal_field(candle, "close_price", nested=("ohlc", "close"))
    if None in (open_price, high_price, low_price, close_price):
        return None
    assert open_price is not None
    assert high_price is not None
    assert low_price is not None
    assert close_price is not None
    body_low = min(open_price, close_price)
    body_high = max(open_price, close_price)
    if wick == "LOWER_WICK":
        return low_price, body_low
    if wick == "UPPER_WICK":
        return body_high, high_price
    return None


def _candle_bounds(candle: Mapping[str, Any]) -> tuple[Decimal, Decimal] | None:
    low_price = _decimal_field(candle, "low_price", nested=("ohlc", "low"))
    high_price = _decimal_field(candle, "high_price", nested=("ohlc", "high"))
    if low_price is None or high_price is None:
        return None
    return low_price, high_price


def _reference_bin_in_required_third(
    candle: Mapping[str, Any],
    reference_bin: Mapping[str, Any],
    *,
    direction: str,
) -> bool:
    center = _bin_center(reference_bin)
    if center is None:
        return False
    normalized_direction = str(direction or "").strip().upper()
    if normalized_direction == "LONG":
        return _price_in_wick_or_body_third(
            candle,
            center,
            wick="LOWER_WICK",
            body_third="LOWER",
        )
    if normalized_direction == "SHORT":
        return _price_in_wick_or_body_third(
            candle,
            center,
            wick="UPPER_WICK",
            body_third="UPPER",
        )
    return False


def _price_in_wick_or_body_third(
    candle: Mapping[str, Any],
    price: Decimal,
    *,
    wick: str,
    body_third: str,
) -> bool:
    return _price_in_wick(candle, price, wick=wick) or _price_in_body_third(
        candle,
        price,
        third=body_third,
    )


def _price_in_wick(
    candle: Mapping[str, Any],
    price: Decimal,
    *,
    wick: str,
) -> bool:
    bounds = _wick_bounds(candle, wick=wick)
    if bounds is None:
        return False
    low_price, high_price = bounds
    if high_price < low_price:
        low_price, high_price = high_price, low_price
    return low_price <= price <= high_price


def _price_in_body_third(
    candle: Mapping[str, Any],
    price: Decimal,
    *,
    third: str,
) -> bool:
    open_price = _decimal_field(candle, "open_price", nested=("ohlc", "open"))
    close_price = _decimal_field(candle, "close_price", nested=("ohlc", "close"))
    if open_price is None or close_price is None:
        return False
    body_low = min(open_price, close_price)
    body_high = max(open_price, close_price)
    if price < body_low or price > body_high:
        return False
    body_size = body_high - body_low
    if body_size <= 0:
        return price == body_low
    one_third = body_size / Decimal("3")
    normalized_third = str(third or "").strip().upper()
    if normalized_third == "LOWER":
        return body_low <= price <= body_low + one_third
    if normalized_third == "UPPER":
        return body_high - one_third <= price <= body_high
    return False


def _price_in_candle_third(
    candle: Mapping[str, Any],
    price: Decimal,
    *,
    third: str,
) -> bool:
    bounds = _candle_bounds(candle)
    if bounds is None:
        return False
    low_price, high_price = bounds
    if high_price < low_price:
        low_price, high_price = high_price, low_price
    if price < low_price or price > high_price:
        return False
    spread = high_price - low_price
    if spread <= 0:
        return price == low_price
    one_third = spread / Decimal("3")
    normalized_third = str(third or "").strip().upper()
    if normalized_third == "LOWER":
        return low_price <= price <= low_price + one_third
    if normalized_third == "UPPER":
        return high_price - one_third <= price <= high_price
    return False


def _is_live_candle(candle: Mapping[str, Any]) -> bool:
    return candle.get("is_live") is True or candle.get("closed") is False


def _is_one_sided_reference_bin(
    candle: Mapping[str, Any],
    payload: Mapping[str, Any],
    *,
    reference_side: str,
) -> bool:
    provider = str(candle.get("market_provider") or "").strip().upper()
    quantity_unit = str(candle.get("quantity_unit") or "").strip().upper()
    use_contracts = provider.startswith("CME") or quantity_unit == "CONTRACTS"
    use_volume = provider == "BINANCE" or quantity_unit == "VOLUME"
    if not use_contracts and not use_volume:
        use_contracts = any(
            _bin_field(payload, key) is not None
            for key in ("buy_contracts", "sell_contracts")
        )
        use_volume = not use_contracts

    if use_contracts:
        buy_value = _first_bin_decimal(
            payload,
            ("buy_contracts", "ask_traded_contracts"),
        )
        sell_value = _first_bin_decimal(
            payload,
            ("sell_contracts", "bid_traded_contracts"),
        )
    else:
        buy_value = _first_bin_decimal(
            payload,
            ("buy_volume", "ask_traded_volume"),
        )
        sell_value = _first_bin_decimal(
            payload,
            ("sell_volume", "bid_traded_volume"),
        )
    buy = buy_value if buy_value is not None else Decimal("0")
    sell = sell_value if sell_value is not None else Decimal("0")
    if reference_side == "BUY":
        return buy > 0 and sell == 0
    if reference_side == "SELL":
        return buy == 0 and sell > 0
    return False


def _spike_score_deviation(candle: Mapping[str, Any]) -> Decimal:
    explicit = _decimal_field(candle, "contract_spike_score_deviation")
    if explicit is not None:
        return explicit
    scores = tuple(
        _abnormal_score(item)
        for item in _bins(candle)
        if _bin_total_traded(item) > 0
    )
    if not scores:
        return Decimal("0")
    mean = sum(scores, Decimal("0")) / Decimal(len(scores))
    variance = sum(
        ((score - mean) ** 2 for score in scores),
        Decimal("0"),
    ) / Decimal(len(scores))
    return variance.sqrt()


def _bin_total_traded(payload: Mapping[str, Any]) -> Decimal:
    return _bin_buy_contracts(payload) + _bin_sell_contracts(payload)


def _bin_buy_contracts(payload: Mapping[str, Any]) -> Decimal:
    return _first_bin_decimal(
        payload,
        ("buy_contracts", "ask_traded_contracts", "buy_volume", "ask_traded_volume"),
    ) or Decimal("0")


def _bin_sell_contracts(payload: Mapping[str, Any]) -> Decimal:
    return _first_bin_decimal(
        payload,
        ("sell_contracts", "bid_traded_contracts", "sell_volume", "bid_traded_volume"),
    ) or Decimal("0")


def _candle_tick_size(
    candle: Mapping[str, Any],
    *,
    reference_bin: Mapping[str, Any],
    bin_tick_count: int,
) -> Decimal | None:
    explicit = _decimal_field(candle, "price_step", "tick_size")
    if explicit is not None and explicit > 0:
        return explicit
    low = _decimal_field(reference_bin, "low", "bin_low")
    high = _decimal_field(reference_bin, "high", "bin_high")
    if low is None or high is None or high <= low or bin_tick_count <= 0:
        return None
    return (high - low) / Decimal(bin_tick_count)


def _price_level_payload(price: Decimal, *, side: str) -> dict[str, Any]:
    return {
        "index": None,
        "low": str(price),
        "high": str(price),
        "side": side,
        "dominance_side": side,
        "wick": "",
        "entry_direction": "",
        "abnormal_volume_score": "0",
        "spike_score": "0",
        "contract_spike_score": "0",
    }


def _is_bullish_candle(candle: Mapping[str, Any]) -> bool:
    open_price = _decimal_field(candle, "open_price", nested=("ohlc", "open"))
    close_price = _decimal_field(candle, "close_price", nested=("ohlc", "close"))
    return open_price is not None and close_price is not None and close_price > open_price


def _is_bearish_candle(candle: Mapping[str, Any]) -> bool:
    open_price = _decimal_field(candle, "open_price", nested=("ohlc", "open"))
    close_price = _decimal_field(candle, "close_price", nested=("ohlc", "close"))
    return open_price is not None and close_price is not None and close_price < open_price


def _bins(candle: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    return tuple(item for item in candle.get("bins", ()) if isinstance(item, Mapping))


def _normalized_bins(candle: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    bin_size = _decimal_field(candle, "bin_size", "fixed_bin_size")
    result = []
    for item in _bins(candle):
        if _decimal_field(item, "low", "bin_low") is not None:
            result.append(item)
            continue
        price = _decimal_field(item, "price")
        if price is None or bin_size is None or bin_size <= 0:
            result.append(item)
            continue
        normalized = dict(item)
        normalized["low"] = str(price)
        normalized["high"] = str(price + bin_size)
        result.append(normalized)
    return tuple(result)


def _bin_center(payload: Mapping[str, Any]) -> Decimal | None:
    low = _decimal_field(payload, "low", "bin_low")
    high = _decimal_field(payload, "high", "bin_high")
    if low is None or high is None:
        return None
    return (low + high) / Decimal("2")


def _reference_bin_bounds(
    state: EntryState | ReferenceSetup,
) -> tuple[Decimal, Decimal] | None:
    reference_bins = state.reference_bins or (
        (state.reference_bin,) if state.reference_bin is not None else tuple()
    )
    lows = []
    highs = []
    for item in reference_bins:
        low = _decimal_from_payload(item.get("low"))
        high = _decimal_from_payload(item.get("high"))
        if low is None or high is None:
            continue
        lows.append(low)
        highs.append(high)
    if not lows or not highs:
        return None
    return min(lows), max(highs)


def _reference_extreme_prices(
    setup: ReferenceSetup,
) -> tuple[Decimal | None, Decimal | None]:
    bounds = _reference_bin_bounds(setup)
    if bounds is None:
        return None, None
    return bounds


def _candle_pivot_summary(
    candle: Mapping[str, Any],
) -> dict[str, Decimal | int] | None:
    low_price = _decimal_field(candle, "low_price", nested=("ohlc", "low"))
    high_price = _decimal_field(candle, "high_price", nested=("ohlc", "high"))
    open_time_ms = _integer_field(candle, "open_time_ms", "open_time") or 0
    if low_price is None or high_price is None:
        return None
    return {
        "open_time_ms": open_time_ms,
        "low": low_price,
        "high": high_price,
    }


def _is_swing_low(
    candles: tuple[dict[str, Decimal | int], ...],
) -> bool:
    if len(candles) < 5:
        return False
    center_low = candles[2]["low"]
    return all(center_low < candles[index]["low"] for index in (0, 1, 3, 4))


def _is_swing_high(
    candles: tuple[dict[str, Decimal | int], ...],
) -> bool:
    if len(candles) < 5:
        return False
    center_high = candles[2]["high"]
    return all(center_high > candles[index]["high"] for index in (0, 1, 3, 4))


def _signal_bin_reference_distance(
    signal_center: Decimal,
    *,
    reference_low: Decimal,
    reference_high: Decimal,
    side: str,
) -> Decimal:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY" and signal_center < reference_low:
        return reference_low - signal_center
    if normalized_side == "SELL" and signal_center > reference_high:
        return signal_center - reference_high
    return Decimal("0")


def _is_blocked_by_opposite_confirmed_setup(
    setups: tuple[ReferenceSetup, ...],
    absorption: Mapping[str, Any],
) -> bool:
    new_direction = str(absorption.get("entry_direction") or "").strip().upper()
    for setup in setups:
        if setup.state not in (PEAK_CONFIRMED, BREAK_CONFIRMED):
            continue
        current_direction = str(setup.direction or "").strip().upper()
        if (
            (current_direction == "LONG" and new_direction == "SHORT")
            or (current_direction == "SHORT" and new_direction == "LONG")
        ):
            return True
    return False


def _decimal_field(
    payload: Mapping[str, Any] | None,
    *keys: str,
    nested: tuple[str, str] | None = None,
) -> Decimal | None:
    if payload is None:
        return None
    values = [payload.get(key) for key in keys]
    if nested is not None:
        container = payload.get(nested[0])
        if isinstance(container, Mapping):
            values.append(container.get(nested[1]))
    for value in values:
        try:
            return Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            continue
    return None


def _decimal_from_payload(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _bin_field(payload: Mapping[str, Any], key: str) -> Any:
    l2 = payload.get("l2")
    if isinstance(l2, Mapping) and key in l2:
        return l2.get(key)
    return payload.get(key)


def _bin_decimal(payload: Mapping[str, Any], key: str) -> Decimal | None:
    try:
        return Decimal(str(_bin_field(payload, key)))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _first_bin_decimal(
    payload: Mapping[str, Any],
    keys: tuple[str, ...],
) -> Decimal | None:
    for key in keys:
        value = _bin_decimal(payload, key)
        if value is not None:
            return value
    return None


def _bool_bin_field(payload: Mapping[str, Any], key: str) -> bool:
    value = _bin_field(payload, key)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _abnormal_score(payload: Mapping[str, Any]) -> Decimal:
    return (
        _bin_decimal(payload, "contract_spike_score")
        or _bin_decimal(payload, "abnormal_volume_score")
        or _bin_decimal(payload, "spike_score")
        or Decimal("0")
    )


def _contract_spike_score(payload: Mapping[str, Any]) -> Decimal:
    return (
        _bin_decimal(payload, "contract_spike_score")
        or _bin_decimal(payload, "spike_score")
        or Decimal("0")
    )


def _diagonal_ratio(payload: Mapping[str, Any], *, side: str) -> Decimal | None:
    normalized_side = str(side or "").strip().upper()
    if normalized_side == "BUY":
        return _buy_diagonal_ratio(payload)
    if normalized_side == "SELL":
        return _sell_diagonal_ratio(payload)
    return None


def _buy_diagonal_ratio(payload: Mapping[str, Any]) -> Decimal | None:
    return _first_bin_decimal(
        payload,
        (
            "buy_diagonal_contract_ratio",
            "buy_diagonal_imbalance_ratio",
            "buy_diagonal_ratio",
        ),
    )


def _sell_diagonal_ratio(payload: Mapping[str, Any]) -> Decimal | None:
    return _first_bin_decimal(
        payload,
        (
            "sell_diagonal_contract_ratio",
            "sell_diagonal_imbalance_ratio",
            "sell_diagonal_ratio",
        ),
    )


def _dominance_side(payload: Mapping[str, Any]) -> str:
    return str(_bin_field(payload, "dominant_diagonal_side") or "").strip().upper()


def _traded_side(payload: Mapping[str, Any]) -> str:
    buy_contracts = _first_bin_decimal(
        payload,
        ("buy_contracts", "ask_traded_contracts"),
    )
    sell_contracts = _first_bin_decimal(
        payload,
        ("sell_contracts", "bid_traded_contracts"),
    )
    if buy_contracts is not None or sell_contracts is not None:
        return _side_from_buy_sell(buy_contracts, sell_contracts)

    buy_volume = _first_bin_decimal(payload, ("buy_volume", "ask_traded_volume"))
    sell_volume = _first_bin_decimal(payload, ("sell_volume", "bid_traded_volume"))
    return _side_from_buy_sell(buy_volume, sell_volume)


def _side_from_buy_sell(
    buy_value: Decimal | None,
    sell_value: Decimal | None,
) -> str:
    buy = buy_value if buy_value is not None else Decimal("0")
    sell = sell_value if sell_value is not None else Decimal("0")
    if buy > sell:
        return "BUY"
    if sell > buy:
        return "SELL"
    return "NONE"


def _symbol(candle: Mapping[str, Any]) -> str:
    return str(
        candle.get("mt5_symbol")
        or candle.get("symbol")
        or candle.get("provider_symbol")
        or ""
    ).strip().upper()


def _provider_symbol(candle: Mapping[str, Any]) -> str:
    symbol = _symbol(candle)
    return str(
        candle.get("provider_symbol")
        or candle.get("symbol")
        or symbol
    ).strip().upper()
