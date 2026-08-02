from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Iterable, Mapping


ACTOR_PROXY_EXIT_REASON = "ACTOR_PROXY_EXIT"

ACTION_ADD = "ADD"
ACTION_MODIFY = "MODIFY"
ACTION_CANCEL = "CANCEL"
ACTION_TRADE = "TRADE"
ACTION_FILL = "FILL"
ACTION_CLEAR = "CLEAR"

CANDIDATE_ACTIVE = "ACTIVE"
CANDIDATE_REDUCED = "REDUCED"
CANDIDATE_CANCELLED = "CANCELLED"
CANDIDATE_FILLED = "FILLED"
CANDIDATE_REPLACED = "REPLACED"
CANDIDATE_LOST = "LOST"
CANDIDATE_EXITED = "EXITED"
ACTIVE_CANDIDATE_STATUSES = frozenset(
    {CANDIDATE_ACTIVE, CANDIDATE_REDUCED, CANDIDATE_REPLACED}
)

_ACTION_ALIASES = {
    "A": ACTION_ADD,
    "ADD": ACTION_ADD,
    "M": ACTION_MODIFY,
    "MODIFY": ACTION_MODIFY,
    "UPDATE": ACTION_MODIFY,
    "C": ACTION_CANCEL,
    "D": ACTION_CANCEL,
    "CANCEL": ACTION_CANCEL,
    "DELETE": ACTION_CANCEL,
    "T": ACTION_TRADE,
    "TRADE": ACTION_TRADE,
    "F": ACTION_FILL,
    "FILL": ACTION_FILL,
    "EXECUTE": ACTION_FILL,
    "R": ACTION_CLEAR,
    "CLEAR": ACTION_CLEAR,
}


@dataclass(frozen=True)
class RawDomEvent:
    ts_event_ms: int
    symbol: str
    instrument_id: int | str | None = None
    order_id: str = ""
    side: str = ""
    price: Decimal | None = None
    size: int = 0
    action: str = ""
    ts_recv_ms: int | None = None
    flags: Any = None
    sequence: int | None = None
    ordinal: int | None = None
    source: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "ts_event_ms", int(self.ts_event_ms))
        object.__setattr__(self, "symbol", _normalize_text(self.symbol))
        object.__setattr__(self, "order_id", str(self.order_id or "").strip())
        object.__setattr__(self, "side", normalize_side(self.side))
        object.__setattr__(self, "price", _decimal_or_none(self.price))
        object.__setattr__(self, "size", max(0, _int_value(self.size, 0)))
        object.__setattr__(self, "action", normalize_action(self.action))
        if self.ts_recv_ms is not None:
            object.__setattr__(self, "ts_recv_ms", int(self.ts_recv_ms))
        if self.sequence is not None:
            object.__setattr__(self, "sequence", int(self.sequence))
        if self.ordinal is not None:
            object.__setattr__(self, "ordinal", int(self.ordinal))
        object.__setattr__(self, "source", str(self.source or "").strip())

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> RawDomEvent:
        return cls(
            ts_event_ms=_int_from_mapping(payload, "ts_event_ms", "timestamp_ms", "event_time_ms", "time_ms"),
            ts_recv_ms=_optional_int_from_mapping(payload, "ts_recv_ms", "receive_time_ms"),
            symbol=str(
                payload.get("symbol")
                or payload.get("provider_symbol")
                or payload.get("mt5_symbol")
                or ""
            ),
            instrument_id=payload.get("instrument_id"),
            order_id=str(payload.get("order_id") or payload.get("venue_order_id") or ""),
            side=str(payload.get("side") or payload.get("order_side") or ""),
            price=_decimal_or_none(payload.get("price")),
            size=_int_from_mapping(payload, "size", "order_size", "raw_event_size", "remaining_size"),
            action=str(payload.get("action") or payload.get("event_type") or ""),
            flags=payload.get("flags"),
            sequence=_optional_int_from_mapping(payload, "sequence", "seq"),
            ordinal=_optional_int_from_mapping(payload, "ordinal"),
            source=str(payload.get("source") or payload.get("source_file") or payload.get("source_id") or ""),
        )

    @classmethod
    def from_dom_raw_event(
        cls,
        event: Any,
        *,
        symbol: str = "",
        provider_symbol: str = "",
    ) -> RawDomEvent:
        return cls(
            ts_event_ms=int(getattr(event, "ts_event_ms")),
            symbol=symbol or provider_symbol,
            instrument_id=getattr(event, "instrument_id", None),
            order_id=str(getattr(event, "order_id", "") or ""),
            side=str(getattr(event, "side", "") or ""),
            price=_decimal_or_none(getattr(event, "price", None)),
            size=_int_value(getattr(event, "size", 0), 0),
            action=str(getattr(event, "action", "") or ""),
            sequence=getattr(event, "sequence", None),
            source=str(getattr(event, "source_file", "") or ""),
        )

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "ts_event_ms": int(self.ts_event_ms),
            "symbol": self.symbol,
            "instrument_id": self.instrument_id,
            "order_id": self.order_id,
            "side": self.side,
            "price": str(self.price) if self.price is not None else "",
            "size": int(self.size),
            "action": self.action,
            "source": self.source,
        }
        if self.ts_recv_ms is not None:
            payload["ts_recv_ms"] = int(self.ts_recv_ms)
        if self.flags is not None:
            payload["flags"] = self.flags
        if self.sequence is not None:
            payload["sequence"] = int(self.sequence)
        if self.ordinal is not None:
            payload["ordinal"] = int(self.ordinal)
        return payload


@dataclass(frozen=True)
class ActorProxyOrder:
    symbol: str
    provider_symbol: str
    order_id: str
    side: str
    price: Decimal
    initial_size: int
    current_size: int
    refill_count: int = 0
    refill_contracts: int = 0
    executed_contracts: int = 0
    trade_count: int = 0
    first_seen_ts_event_ms: int = 0
    last_seen_ts_event_ms: int = 0
    instrument_id: int | str | None = None
    source_file: str = ""
    source_id: str = ""
    candle_id: str = ""
    candle_open_time_ms: int = 0
    reason: str = "HIGH_REFILL"
    raw_event_refs: tuple[Mapping[str, Any], ...] = ()
    importance_score: Decimal = Decimal("0")

    def __post_init__(self) -> None:
        object.__setattr__(self, "symbol", _normalize_text(self.symbol))
        object.__setattr__(self, "provider_symbol", _normalize_text(self.provider_symbol))
        object.__setattr__(self, "order_id", str(self.order_id or "").strip())
        object.__setattr__(self, "side", normalize_side(self.side))
        price = _decimal_or_none(self.price)
        if price is None:
            raise ValueError("actor proxy order price is required")
        object.__setattr__(self, "price", price)
        object.__setattr__(self, "initial_size", max(0, int(self.initial_size)))
        object.__setattr__(self, "current_size", max(0, int(self.current_size)))
        object.__setattr__(self, "refill_count", max(0, int(self.refill_count)))
        object.__setattr__(self, "refill_contracts", max(0, int(self.refill_contracts)))
        object.__setattr__(self, "executed_contracts", max(0, int(self.executed_contracts)))
        object.__setattr__(self, "trade_count", max(0, int(self.trade_count)))
        object.__setattr__(self, "first_seen_ts_event_ms", max(0, int(self.first_seen_ts_event_ms)))
        object.__setattr__(self, "last_seen_ts_event_ms", max(0, int(self.last_seen_ts_event_ms)))
        object.__setattr__(self, "source_file", str(self.source_file or "").strip())
        object.__setattr__(self, "source_id", str(self.source_id or "").strip())
        object.__setattr__(self, "candle_id", str(self.candle_id or "").strip())
        object.__setattr__(self, "candle_open_time_ms", max(0, int(self.candle_open_time_ms)))
        object.__setattr__(self, "reason", str(self.reason or "").strip().upper() or "HIGH_REFILL")
        object.__setattr__(
            self,
            "raw_event_refs",
            tuple(dict(item) for item in _iter_mappings(self.raw_event_refs)),
        )
        object.__setattr__(self, "importance_score", _decimal_or_none(self.importance_score) or Decimal("0"))

    @property
    def persistence_ms(self) -> int:
        if self.first_seen_ts_event_ms <= 0 or self.last_seen_ts_event_ms <= 0:
            return 0
        return max(0, self.last_seen_ts_event_ms - self.first_seen_ts_event_ms)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ActorProxyOrder:
        initial_size = _int_from_mapping(
            payload,
            "initial_size",
            "initial_order_size",
            "max_order_size",
            "top_order_size",
        )
        current_size = _int_from_mapping(
            payload,
            "current_size",
            "remaining_size",
            "current_order_size",
            "top_order_current_contracts",
            fallback=initial_size,
        )
        return cls(
            symbol=str(payload.get("symbol") or payload.get("mt5_symbol") or payload.get("provider_symbol") or ""),
            provider_symbol=str(payload.get("provider_symbol") or payload.get("symbol") or ""),
            instrument_id=payload.get("instrument_id"),
            order_id=str(payload.get("order_id") or payload.get("venue_order_id") or payload.get("top_order_id") or ""),
            side=str(payload.get("side") or payload.get("top_order_side") or ""),
            price=_decimal_required(payload, "price", "level_price", "marker_price"),
            initial_size=initial_size,
            current_size=current_size,
            refill_count=_int_from_mapping(payload, "refill_count", "positive_refill_count", "top_order_positive_refill_count"),
            refill_contracts=_int_from_mapping(payload, "refill_contracts", "positive_refill_total", "refill_total", "top_order_positive_refill_total"),
            executed_contracts=_int_from_mapping(payload, "executed_contracts", "refill_filled_contracts", "positive_refill_filled_total"),
            trade_count=_int_from_mapping(payload, "trade_count"),
            first_seen_ts_event_ms=_int_from_mapping(payload, "first_seen_ts_event_ms", "opened_at_ms", "timestamp_ms"),
            last_seen_ts_event_ms=_int_from_mapping(payload, "last_seen_ts_event_ms", "updated_at_ms", "close_time_ms", "timestamp_ms"),
            source_file=str(payload.get("source_file") or ""),
            source_id=str(payload.get("source_id") or payload.get("source") or ""),
            candle_id=str(payload.get("candle_id") or ""),
            candle_open_time_ms=_int_from_mapping(payload, "candle_open_time_ms", "footprint_open_time_ms", "marker_time_ms"),
            reason=str(payload.get("reason") or payload.get("importance_reason") or "HIGH_REFILL"),
            raw_event_refs=tuple(_iter_mappings(payload.get("raw_event_refs", ()))),
            importance_score=_decimal_or_none(payload.get("importance_score")) or Decimal("0"),
        )

    def with_importance_score(self, score: Decimal) -> ActorProxyOrder:
        return ActorProxyOrder(
            symbol=self.symbol,
            provider_symbol=self.provider_symbol,
            instrument_id=self.instrument_id,
            order_id=self.order_id,
            side=self.side,
            price=self.price,
            initial_size=self.initial_size,
            current_size=self.current_size,
            refill_count=self.refill_count,
            refill_contracts=self.refill_contracts,
            executed_contracts=self.executed_contracts,
            trade_count=self.trade_count,
            first_seen_ts_event_ms=self.first_seen_ts_event_ms,
            last_seen_ts_event_ms=self.last_seen_ts_event_ms,
            source_file=self.source_file,
            source_id=self.source_id,
            candle_id=self.candle_id,
            candle_open_time_ms=self.candle_open_time_ms,
            reason=self.reason,
            raw_event_refs=self.raw_event_refs,
            importance_score=score,
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "instrument_id": self.instrument_id,
            "order_id": self.order_id,
            "side": self.side,
            "price": str(self.price),
            "initial_size": int(self.initial_size),
            "current_size": int(self.current_size),
            "remaining_size": int(self.current_size),
            "refill_count": int(self.refill_count),
            "refill_contracts": int(self.refill_contracts),
            "executed_contracts": int(self.executed_contracts),
            "trade_count": int(self.trade_count),
            "first_seen_ts_event_ms": int(self.first_seen_ts_event_ms),
            "last_seen_ts_event_ms": int(self.last_seen_ts_event_ms),
            "source_file": self.source_file,
            "source_id": self.source_id,
            "candle_id": self.candle_id,
            "candle_open_time_ms": int(self.candle_open_time_ms),
            "reason": self.reason,
            "raw_event_refs": [dict(item) for item in self.raw_event_refs],
            "importance_score": str(self.importance_score),
        }


@dataclass(frozen=True)
class ActorProxyPayload:
    payload_id: str
    symbol: str
    provider_symbol: str
    timeframe: str
    orders: tuple[ActorProxyOrder, ...]
    source_payload_id: str = ""
    source_engine: str = "dataProcessEngine"
    source: str = ""
    instrument_id: int | str | None = None
    mode: str = "REPLAY_STUDY"
    raw_data_only: bool = True
    candle_id: str = ""
    candle_open_time_ms: int = 0
    tracking_start_ms: int | None = None
    tracking_end_ms: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "payload_id", str(self.payload_id or "").strip())
        object.__setattr__(self, "source_payload_id", str(self.source_payload_id or self.payload_id).strip())
        object.__setattr__(self, "symbol", _normalize_text(self.symbol))
        object.__setattr__(self, "provider_symbol", _normalize_text(self.provider_symbol))
        object.__setattr__(self, "timeframe", _normalize_text(self.timeframe))
        object.__setattr__(self, "source_engine", str(self.source_engine or "").strip())
        object.__setattr__(self, "source", str(self.source or "").strip())
        object.__setattr__(self, "mode", str(self.mode or "REPLAY_STUDY").strip().upper())
        object.__setattr__(self, "raw_data_only", bool(self.raw_data_only))
        object.__setattr__(self, "candle_id", str(self.candle_id or "").strip())
        object.__setattr__(self, "candle_open_time_ms", max(0, int(self.candle_open_time_ms)))
        object.__setattr__(self, "orders", tuple(self.orders))
        if self.tracking_start_ms is not None:
            object.__setattr__(self, "tracking_start_ms", int(self.tracking_start_ms))
        if self.tracking_end_ms is not None:
            object.__setattr__(self, "tracking_end_ms", int(self.tracking_end_ms))

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> ActorProxyPayload:
        raw_orders = payload.get("orders") or payload.get("actor_orders") or ()
        orders = tuple(ActorProxyOrder.from_mapping(item) for item in _iter_mappings(raw_orders))
        return cls(
            payload_id=str(payload.get("payload_id") or payload.get("id") or payload.get("output_id") or ""),
            source_payload_id=str(payload.get("source_payload_id") or payload.get("process_payload_id") or ""),
            symbol=str(payload.get("symbol") or payload.get("mt5_symbol") or payload.get("provider_symbol") or ""),
            provider_symbol=str(payload.get("provider_symbol") or payload.get("symbol") or ""),
            instrument_id=payload.get("instrument_id"),
            timeframe=str(payload.get("timeframe") or ""),
            source_engine=str(payload.get("source_engine") or "dataProcessEngine"),
            source=str(payload.get("source") or ""),
            mode=str(payload.get("mode") or payload.get("tracking_mode") or "REPLAY_STUDY"),
            raw_data_only=bool(payload.get("raw_data_only", True)),
            candle_id=str(payload.get("candle_id") or ""),
            candle_open_time_ms=_int_from_mapping(payload, "candle_open_time_ms", "footprint_open_time_ms"),
            tracking_start_ms=_optional_int_from_mapping(payload, "tracking_start_ms", "replay_start_ms"),
            tracking_end_ms=_optional_int_from_mapping(payload, "tracking_end_ms", "replay_end_ms"),
            orders=orders,
        )

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": "actor_proxy_payload.v1",
            "payload_id": self.payload_id,
            "source_payload_id": self.source_payload_id,
            "source_engine": self.source_engine,
            "source": self.source,
            "mode": self.mode,
            "raw_data_only": bool(self.raw_data_only),
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "instrument_id": self.instrument_id,
            "timeframe": self.timeframe,
            "candle_id": self.candle_id,
            "candle_open_time_ms": int(self.candle_open_time_ms),
            "orders": [order.to_payload() for order in self.orders],
        }
        if self.tracking_start_ms is not None:
            payload["tracking_start_ms"] = int(self.tracking_start_ms)
            payload["replay_start_ms"] = int(self.tracking_start_ms)
        if self.tracking_end_ms is not None:
            payload["tracking_end_ms"] = int(self.tracking_end_ms)
            payload["replay_end_ms"] = int(self.tracking_end_ms)
        return payload


@dataclass(frozen=True)
class PositionContext:
    position_id: str
    side: str
    entry_price: Decimal | None
    entry_time_ms: int
    symbol: str
    timeframe: str
    provider_symbol: str = ""
    tracking_start_ms: int | None = None
    tracking_end_ms: int | None = None
    mode: str = "REPLAY"

    def __post_init__(self) -> None:
        object.__setattr__(self, "position_id", str(self.position_id or "").strip())
        object.__setattr__(self, "side", _normalize_text(self.side))
        object.__setattr__(self, "entry_price", _decimal_or_none(self.entry_price))
        object.__setattr__(self, "entry_time_ms", max(0, int(self.entry_time_ms)))
        object.__setattr__(self, "symbol", _normalize_text(self.symbol))
        object.__setattr__(self, "provider_symbol", _normalize_text(self.provider_symbol))
        object.__setattr__(self, "timeframe", _normalize_text(self.timeframe))
        object.__setattr__(self, "mode", str(self.mode or "").strip().upper() or "REPLAY")
        if self.tracking_start_ms is not None:
            object.__setattr__(self, "tracking_start_ms", int(self.tracking_start_ms))
        if self.tracking_end_ms is not None:
            object.__setattr__(self, "tracking_end_ms", int(self.tracking_end_ms))

    @property
    def effective_tracking_start_ms(self) -> int:
        if self.tracking_start_ms is not None:
            return int(self.tracking_start_ms)
        return int(self.entry_time_ms)

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> PositionContext:
        return cls(
            position_id=str(payload.get("position_id") or ""),
            side=str(payload.get("side") or payload.get("direction") or ""),
            entry_price=_decimal_or_none(payload.get("entry_price")),
            entry_time_ms=_int_from_mapping(payload, "entry_time_ms", "entry_candle_time_ms", "action_candle_time_ms"),
            symbol=str(payload.get("symbol") or payload.get("mt5_symbol") or payload.get("provider_symbol") or ""),
            provider_symbol=str(payload.get("provider_symbol") or payload.get("symbol") or ""),
            timeframe=str(payload.get("timeframe") or ""),
            tracking_start_ms=_optional_int_from_mapping(payload, "tracking_start_ms", "replay_start_ms"),
            tracking_end_ms=_optional_int_from_mapping(payload, "tracking_end_ms", "replay_end_ms"),
            mode=str(payload.get("mode") or payload.get("tracking_mode") or "REPLAY"),
        )

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "position_id": self.position_id,
            "side": self.side,
            "entry_price": str(self.entry_price) if self.entry_price is not None else "",
            "entry_time_ms": int(self.entry_time_ms),
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "timeframe": self.timeframe,
            "mode": self.mode,
        }
        if self.tracking_start_ms is not None:
            payload["tracking_start_ms"] = int(self.tracking_start_ms)
            payload["replay_start_ms"] = int(self.tracking_start_ms)
        if self.tracking_end_ms is not None:
            payload["tracking_end_ms"] = int(self.tracking_end_ms)
            payload["replay_end_ms"] = int(self.tracking_end_ms)
        return payload


@dataclass
class ActorCandidate:
    candidate_id: str
    original_order_id: str
    active_order_id: str
    symbol: str
    provider_symbol: str
    instrument_id: int | str | None
    side: str
    price: Decimal
    initial_size: int
    current_size: int
    initial_refill_count: int
    initial_refill_contracts: int
    initial_executed_contracts: int
    trade_count: int
    importance_score: Decimal
    first_seen_ts_event_ms: int
    last_seen_ts_event_ms: int
    source_file: str = ""
    source_id: str = ""
    reason: str = "HIGH_REFILL"
    status: str = CANDIDATE_ACTIVE
    cancelled_contracts: int = 0
    filled_contracts: int = 0
    replaced_contracts: int = 0
    pending_refill_contracts: int = 0
    last_refill_ts_event_ms: int = 0
    last_replacement_ts_event_ms: int = 0
    lost_since_ts_event_ms: int = 0
    raw_event_refs: tuple[Mapping[str, Any], ...] = ()

    @property
    def is_active(self) -> bool:
        return self.status in ACTIVE_CANDIDATE_STATUSES and self.current_size > 0

    def to_payload(self) -> dict[str, Any]:
        return {
            "candidate_id": self.candidate_id,
            "original_order_id": self.original_order_id,
            "active_order_id": self.active_order_id,
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "instrument_id": self.instrument_id,
            "side": self.side,
            "price": str(self.price),
            "initial_size": int(self.initial_size),
            "current_size": int(self.current_size),
            "status": self.status,
            "importance_score": str(self.importance_score),
            "cancelled_contracts": int(self.cancelled_contracts),
            "filled_contracts": int(self.filled_contracts),
            "replaced_contracts": int(self.replaced_contracts),
            "last_refill_ts_event_ms": int(self.last_refill_ts_event_ms),
            "last_replacement_ts_event_ms": int(self.last_replacement_ts_event_ms),
            "lost_since_ts_event_ms": int(self.lost_since_ts_event_ms),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ActorClusterMetrics:
    total_tracked_initial_contracts: int
    total_active_contracts: int
    active_contract_ratio: Decimal
    cancelled_contracts: int
    filled_contracts: int
    replaced_contracts: int
    refill_rate_recent: Decimal
    cancel_rate_recent: Decimal
    add_rate_recent: Decimal
    time_since_last_refill_ms: int
    time_since_last_replacement_ms: int
    dominant_actor_still_active: bool
    cluster_exit_score: Decimal

    def to_payload(self) -> dict[str, Any]:
        return {
            "total_tracked_initial_contracts": int(self.total_tracked_initial_contracts),
            "total_active_contracts": int(self.total_active_contracts),
            "active_contract_ratio": str(self.active_contract_ratio),
            "cancelled_contracts": int(self.cancelled_contracts),
            "filled_contracts": int(self.filled_contracts),
            "replaced_contracts": int(self.replaced_contracts),
            "refill_rate_recent": str(self.refill_rate_recent),
            "cancel_rate_recent": str(self.cancel_rate_recent),
            "add_rate_recent": str(self.add_rate_recent),
            "time_since_last_refill_ms": int(self.time_since_last_refill_ms),
            "time_since_last_replacement_ms": int(self.time_since_last_replacement_ms),
            "dominant_actor_still_active": bool(self.dominant_actor_still_active),
            "cluster_exit_score": str(self.cluster_exit_score),
        }


@dataclass
class ActorCluster:
    cluster_id: str
    payload: ActorProxyPayload
    position_context: PositionContext
    candidates: list[ActorCandidate]
    started_ts_event_ms: int
    last_event_ts_event_ms: int = 0
    weakened_since_ts_event_ms: int = 0
    exit_signal: ExitSignal | None = None
    activity_events: list[tuple[int, str, int]] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "cluster_id": self.cluster_id,
            "payload_id": self.payload.payload_id,
            "position_context": self.position_context.to_payload(),
            "started_ts_event_ms": int(self.started_ts_event_ms),
            "last_event_ts_event_ms": int(self.last_event_ts_event_ms),
            "weakened_since_ts_event_ms": int(self.weakened_since_ts_event_ms),
            "candidates": [candidate.to_payload() for candidate in self.candidates],
            "exit_signal": self.exit_signal.to_payload() if self.exit_signal else None,
        }


@dataclass(frozen=True)
class ExitSignal:
    position_id: str
    symbol: str
    side: str
    confidence_score: Decimal
    cluster_exit_score: Decimal
    active_contract_ratio: Decimal
    dominant_actor_status: str
    explanation: str
    ts_event_ms: int
    reason: str = ACTOR_PROXY_EXIT_REASON

    def __post_init__(self) -> None:
        object.__setattr__(self, "confidence_score", _decimal_or_none(self.confidence_score) or Decimal("0"))
        object.__setattr__(self, "cluster_exit_score", _decimal_or_none(self.cluster_exit_score) or Decimal("0"))
        object.__setattr__(self, "active_contract_ratio", _decimal_or_none(self.active_contract_ratio) or Decimal("0"))
        object.__setattr__(self, "ts_event_ms", int(self.ts_event_ms))

    def to_payload(self) -> dict[str, Any]:
        return {
            "position_id": self.position_id,
            "symbol": self.symbol,
            "side": self.side,
            "reason": self.reason,
            "confidence_score": str(self.confidence_score),
            "cluster_exit_score": str(self.cluster_exit_score),
            "active_contract_ratio": str(self.active_contract_ratio),
            "dominant_actor_status": self.dominant_actor_status,
            "explanation": self.explanation,
            "ts_event_ms": int(self.ts_event_ms),
        }


def normalize_action(value: Any) -> str:
    normalized = str(value or "").strip().upper()
    return _ACTION_ALIASES.get(normalized, normalized)


def normalize_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"B", "BID", "BUY"}:
        return "BID"
    if side in {"A", "ASK", "SELL"}:
        return "ASK"
    return side


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _decimal_required(payload: Mapping[str, Any], *keys: str) -> Decimal:
    value = _decimal_from_mapping(payload, *keys)
    if value is None:
        raise ValueError(f"missing decimal field: {', '.join(keys)}")
    return value


def _decimal_from_mapping(payload: Mapping[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = _decimal_or_none(payload.get(key))
        if value is not None:
            return value
    return None


def _decimal_or_none(value: Any) -> Decimal | None:
    if value in {None, ""}:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _int_from_mapping(
    payload: Mapping[str, Any],
    *keys: str,
    fallback: int = 0,
) -> int:
    for key in keys:
        value = payload.get(key)
        if value in {None, ""}:
            continue
        return _int_value(value, fallback)
    return int(fallback)


def _optional_int_from_mapping(payload: Mapping[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = payload.get(key)
        if value in {None, ""}:
            continue
        return _int_value(value, 0)
    return None


def _int_value(value: Any, fallback: int) -> int:
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, TypeError, ValueError):
        return int(fallback)


def _iter_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        return
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for item in value:
            if isinstance(item, Mapping):
                yield item
