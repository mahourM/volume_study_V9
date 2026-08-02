from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from DOM.models import DomRawEvent
from core.timeframe_policy import TIMEFRAME_MS_BY_NAME
from process.data_sources import ProcessEventSource, ProcessFootprintSource
from process.models import (
    DATA_PROCESS_ENTRY_ACTION,
    DATA_PROCESS_ENGINE_PRODUCER,
    DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
    DATA_PROCESS_REFILL_OUTPUT_TYPE,
    DataProcessConfig,
    ProcessFootprintSnapshot,
    ProcessReplayRequest,
    ProcessRunResult,
    ProcessSymbol,
)
from process.sinks import ProcessPayloadSink


ADD_ACTIONS = frozenset({"A", "ADD"})
MODIFY_ACTIONS = frozenset({"M", "MODIFY"})
FILL_ACTIONS = frozenset({"F", "FILL", "EXECUTE"})
CANCEL_ACTIONS = frozenset({"C", "D", "CANCEL", "DELETE"})
CLEAR_ACTIONS = frozenset({"R", "CLEAR"})
OPPOSITE_SEQUENCE_RESET_CONTRACTS_MIN = 20
REFILL_ZONE_COUNT_MIN = 15
REFILL_ZONE_CONTRACTS_MIN = 20
TERMINAL_DIAGONAL_RATIO_MIN = 4
TERMINAL_Z_SCORE_MIN = Decimal("1.8")
DATA_PROCESS_TERMINAL_CANCEL_OUTPUT_TYPE = "DATA_PROCESS_TERMINAL_ZONE_CANCEL"


@dataclass
class _RefillLot:
    candle_open_time_ms: int
    price: Decimal
    side: str
    remaining_qty: int


@dataclass
class _PriceActivityLevel:
    provider_symbol: str
    mt5_symbol: str
    timeframe: str
    price: Decimal
    side: str
    candle_open_time_ms: int
    opening_liquidity: int = 0
    opening_liquidity_inferred: bool = False
    gross_added_contracts: int = 0
    non_refill_added_contracts: int = 0
    refill_added_contracts: int = 0
    withdrawn_contracts: int = 0
    closing_liquidity: int = 0
    refill_count: int = 0
    executed_refill_contracts: int = 0
    withdrawn_refill_contracts: int = 0
    order_ids: set[str] = field(default_factory=set)
    add_event_count: int = 0
    fill_event_count: int = 0
    added_contracts: int = 0
    executed_contracts: int = 0
    cancelled_or_withdrawn_contracts: int = 0
    last_event_time_ms: int = 0


@dataclass
class _PriceLevelState:
    non_refill_liquidity: int = 0
    refill_lots: list[_RefillLot] = field(default_factory=list)

    # Executed liquidity that may still be replenished.
    pending_replenishment_qty: int = 0

    # Candle in which the pending execution occurred.
    # Prevents an old Fill from classifying an unrelated future Add as refill.
    pending_replenishment_candle_open_time_ms: int = 0

    @property
    def total_liquidity(self) -> int:
        return max(0, int(self.non_refill_liquidity)) + sum(
            max(0, int(lot.remaining_qty))
            for lot in self.refill_lots
        )


@dataclass(frozen=True)
class _LevelRefillChange:
    candle_open_time_ms: int
    price: Decimal
    side: str
    refill_count_delta: int = 0
    refill_added_delta: int = 0
    refill_executed_delta: int = 0
    refill_withdrawn_delta: int = 0


@dataclass
class _TrackedOrder:
    order_id: str
    provider_symbol: str
    mt5_symbol: str
    market_provider: str
    timeframe: str
    price: Decimal
    side: str
    current_size: int
    initial_order_size: int
    max_order_size: int
    opened_at_ms: int
    updated_at_ms: int
    instrument_id: int = 0
    source_file: str = ""
    refill_count: int = 0
    refill_contracts: int = 0
    trade_count: int = 0
    executed_contracts: int = 0
    pending_refill_contracts: int = 0
    latest_refill_contracts: int = 0
    threshold_time_ms: int = 0
    threshold_price: Decimal | None = None
    threshold_side: str = ""
    existing_qty: int = 0
    refill_lots: list[_RefillLot] = field(default_factory=list)

    @property
    def has_trade(self) -> bool:
        return self.trade_count > 0 or self.executed_contracts > 0

    def apply_refill(self, *, event: DomRawEvent, contracts: int) -> None:
        if contracts <= 0:
            return
        self.refill_count += 1
        self.refill_contracts += int(contracts)
        self.latest_refill_contracts = int(contracts)
        self.threshold_time_ms = int(event.ts_event_ms)
        self.threshold_price = event.price or self.price
        self.threshold_side = _normalized_side(event.side or self.side)

    def to_payload(
        self,
        *,
        close_event: DomRawEvent,
        close_reason: str,
        footprint_metrics: Mapping[str, Any] | None = None,
        metric_price: Decimal | None = None,
        metric_side: str = "",
        metric_candle_open_time_ms: int = 0,
        refill_count_delta: int = 0,
        refill_added_delta: int = 0,
        refill_executed_delta: int = 0,
        refill_withdrawn_delta: int = 0,
    ) -> dict[str, Any]:
        timestamp_ms = int(close_event.ts_event_ms)
        price = metric_price or self.threshold_price or close_event.price or self.price
        side = _normalized_side(metric_side or self.threshold_side or close_event.side or self.side)
        footprint = dict(footprint_metrics or _empty_footprint_metrics())
        output_id = "|".join(
            (
                DATA_PROCESS_ENGINE_PRODUCER.upper(),
                DATA_PROCESS_REFILL_OUTPUT_TYPE,
                self.provider_symbol.upper(),
                self.timeframe.upper(),
                str(timestamp_ms),
                str(price),
                side,
                self.order_id,
            )
        )
        footprint_open_time_ms = int(footprint.get("footprint_open_time_ms") or 0)
        footprint_bin_low = str(footprint.get("footprint_bin_low") or "")
        footprint_bin_high = str(footprint.get("footprint_bin_high") or "")
        zone_low = footprint_bin_low or str(price)
        zone_high = footprint_bin_high or str(price)
        marker_time_ms = int(metric_candle_open_time_ms or footprint_open_time_ms or timestamp_ms)
        marker_price = footprint_bin_low or str(price)
        entry_direction = "LONG" if side == "BID" else "SHORT"
        reference_side = "SELL" if entry_direction == "LONG" else "BUY"
        added_contracts = max(0, int(refill_added_delta))
        executed_refill = min(added_contracts, max(0, int(refill_executed_delta)))
        execution_rate = round(
            executed_refill / added_contracts * 100.0 if added_contracts > 0 else 0.0,
            1,
        )
        rate_label = f"{execution_rate:.1f}".rstrip("0").rstrip(".")
        payload = {
            "payload_id": output_id,
            "id": output_id,
            "output_id": output_id,
            "producer": DATA_PROCESS_ENGINE_PRODUCER,
            "source_engine": "dataProcessEngine",
            "type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
            "payload_type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
            "output_type": DATA_PROCESS_REFILL_OUTPUT_TYPE,
            "action": DATA_PROCESS_ENTRY_ACTION,
            "timestamp_ms": timestamp_ms,
            "event_time_ms": timestamp_ms,
            "marker_time_ms": marker_time_ms,
            "marker_price": marker_price,
            "threshold_time_ms": int(self.threshold_time_ms or timestamp_ms),
            "close_time_ms": int(close_event.ts_event_ms),
            "symbol": self.provider_symbol,
            "provider_symbol": self.provider_symbol,
            "mt5_symbol": self.mt5_symbol,
            "market_provider": self.market_provider,
            "timeframe": self.timeframe,
            "price": str(price),
            "side": side,
            "order_id": self.order_id,
            "venue_order_id": self.order_id,
            "refill_count": int(self.refill_count),
            "refill_contracts": int(self.refill_contracts),
            "price_base_refill_count": int(refill_count_delta),
            "price_base_refill_contracts": int(refill_added_delta),
            "refill_added_contracts": int(refill_added_delta),
            "executed_refill_contracts": int(refill_executed_delta),
            "withdrawn_refill_contracts": int(refill_withdrawn_delta),
            "refill_execution_rate": execution_rate,
            "refill_display": (
                f"{int(refill_count_delta)}({added_contracts}) "
                f"E{executed_refill} - {rate_label}%"
            ),
            "refill_method": "price_base_refill",
            "refill_filled_contracts": int(self.executed_contracts),
            "positive_refill_filled_total": int(self.executed_contracts),
            "trade_count": int(self.trade_count),
            "executed_contracts": int(self.executed_contracts),
            "initial_order_size": int(self.initial_order_size),
            "max_order_size": int(self.max_order_size),
            "current_order_size": int(self.current_size),
            "initial_size": int(self.initial_order_size),
            "current_size": int(self.current_size),
            "remaining_size": int(self.current_size),
            "instrument_id": int(self.instrument_id or 0),
            "close_event_size": int(close_event.size or 0),
            "close_reason": close_reason,
            "opened_at_ms": int(self.opened_at_ms),
            "updated_at_ms": int(self.updated_at_ms),
            "first_seen_ts_event_ms": int(self.opened_at_ms),
            "last_seen_ts_event_ms": int(self.updated_at_ms),
            "source_file": self.source_file,
            "source_id": self.source_file,
            "candle_open_time_ms": footprint_open_time_ms,
            "reason": "HIGH_REFILL",
            "market_buy": int(footprint.get("market_buy") or 0),
            "market_sell": int(footprint.get("market_sell") or 0),
            "market_buy_contracts": int(footprint.get("market_buy_contracts") or 0),
            "market_sell_contracts": int(footprint.get("market_sell_contracts") or 0),
            "ask_traded_contracts": int(footprint.get("ask_traded_contracts") or 0),
            "bid_traded_contracts": int(footprint.get("bid_traded_contracts") or 0),
            "footprint_aggressive_contracts": int(footprint.get("footprint_aggressive_contracts") or 0),
            "footprint_aggressive_z_score": str(footprint.get("footprint_aggressive_z_score") or "0"),
            "footprint_diagonal_numerator_contracts": int(
                footprint.get("footprint_diagonal_numerator_contracts") or 0
            ),
            "footprint_diagonal_denominator_contracts": int(
                footprint.get("footprint_diagonal_denominator_contracts") or 0
            ),
            "footprint_diagonal_ratio": str(footprint.get("footprint_diagonal_ratio") or "0"),
            "footprint_diagonal_ratio_pass": bool(
                footprint.get("footprint_diagonal_ratio_pass") or False
            ),
            "footprint_open_time_ms": footprint_open_time_ms,
            "footprint_bin_low": footprint_bin_low,
            "footprint_bin_high": footprint_bin_high,
            "zone_low": zone_low,
            "zone_high": zone_high,
            "reference_zone_low": zone_low,
            "reference_zone_high": zone_high,
            "entry_direction": entry_direction,
            "reference_side": reference_side,
            "refill_side": side,
            "zone_level_count": 1,
            "single_level_zone": True,
            "terminal_market_buy": int(footprint.get("market_buy") or 0),
            "terminal_market_sell": int(footprint.get("market_sell") or 0),
            "terminal_market_buy_contracts": int(footprint.get("market_buy") or 0),
            "terminal_market_sell_contracts": int(footprint.get("market_sell") or 0),
            "terminal_aggressive_contracts": int(footprint.get("footprint_aggressive_contracts") or 0),
            "terminal_aggressive_z_score": str(footprint.get("footprint_aggressive_z_score") or "0"),
            "terminal_diagonal_numerator_contracts": int(
                footprint.get("footprint_diagonal_numerator_contracts") or 0
            ),
            "terminal_diagonal_denominator_contracts": int(
                footprint.get("footprint_diagonal_denominator_contracts") or 0
            ),
            "terminal_diagonal_ratio": str(footprint.get("footprint_diagonal_ratio") or "0"),
            "terminal_diagonal_ratio_pass": bool(
                footprint.get("footprint_diagonal_ratio_pass") or False
            ),
            "zone_market_buy": int(footprint.get("market_buy") or 0),
            "zone_market_sell": int(footprint.get("market_sell") or 0),
            "source": "DATA_PROCESS_ENGINE",
        }
        payload["actor_proxy_payload"] = _actor_proxy_payload_from_orders(
            payload,
            (payload,),
            source="DATA_PROCESS_REFILL_ORDER",
        )
        return payload


@dataclass(frozen=True)
class _RefillLevel:
    price: Decimal
    side: str
    zone_low: Decimal
    zone_high: Decimal
    refill_count: int
    refill_contracts: int
    executed_refill_contracts: int
    withdrawn_refill_contracts: int
    market_buy: int
    market_sell: int
    payloads: tuple[dict[str, Any], ...]

    @property
    def representative(self) -> dict[str, Any]:
        refill_events = tuple(
            item
            for item in self.payloads
            if _payload_int(item, "price_base_refill_count") > 0
            or _payload_int(item, "price_base_refill_contracts") > 0
        )
        return max(
            refill_events or self.payloads,
            key=lambda item: (
                _payload_int(item, "timestamp_ms", "threshold_time_ms", "close_time_ms"),
                _payload_int(item, "refill_count"),
            ),
        )


@dataclass(frozen=True)
class _RefillZone:
    direction: str
    reference_side: str
    refill_side: str
    levels: tuple[_RefillLevel, ...]
    terminal_level: _RefillLevel
    zone_low: Decimal
    zone_high: Decimal
    refill_count: int
    refill_contracts: int
    executed_refill_contracts: int
    withdrawn_refill_contracts: int
    market_buy: int
    market_sell: int
    single_level: bool


@dataclass(frozen=True)
class _ActiveZone:
    output_id: str
    side: str
    zone_low: Decimal
    zone_high: Decimal


class DataProcessEngine:
    def __init__(
        self,
        *,
        event_source: ProcessEventSource,
        footprint_source: ProcessFootprintSource | None = None,
        config: DataProcessConfig | None = None,
        sinks: Iterable[ProcessPayloadSink] = (),
    ) -> None:
        self.event_source = event_source
        self.footprint_source = footprint_source
        self.config = config or DataProcessConfig()
        self.sinks = tuple(sinks)
        self._orders: dict[tuple[str, str, str], _TrackedOrder] = {}
        self._footprint_candles_by_key: dict[tuple[str, str], tuple[dict[str, Any], ...]] = {}
        self._refill_payloads_by_key: dict[tuple[str, str, int], dict[str, dict[str, Any]]] = {}
        self._emitted_zone_ids: set[str] = set()
        self._active_zones_by_key: dict[tuple[str, str], tuple[_ActiveZone, ...]] = {}
        self._last_footprint_candle_by_key: dict[tuple[str, str], Mapping[str, Any]] = {}
        self._terminal_zones_by_candle_key: dict[tuple[str, str, int], tuple[_ActiveZone, ...]] = {}
        self._terminal_cancel_checked_by_candle_key: dict[tuple[str, str, int], tuple[str, ...]] = {}
        self._processed_event_ids: set[tuple[Any, ...]] = set()
        self._price_activity_levels: dict[tuple[str, str, int, Decimal, str], _PriceActivityLevel] = {}
        self._price_activity_touched: set[tuple[str, str, int, Decimal, str]] = set()
        self._price_level_states: dict[tuple[str, str, Decimal, str], _PriceLevelState] = {}
        self._price_activity_emit_enabled = True

    def run_replay(self, request: ProcessReplayRequest) -> ProcessRunResult:
        symbols = request.symbols or self.event_source.symbols()
        emit_start_ms = (
            int(request.emit_start_ms)
            if request.emit_start_ms is not None
            else int(request.start_ms)
        )
        emit_window_started = int(request.start_ms) >= emit_start_ms
        footprints = self._load_footprints(
            symbols=symbols,
            start_ms=int(request.start_ms),
            end_ms=int(request.end_ms),
        )
        if self.config.filter_price_activity_to_footprints:
            restrict_events = getattr(
                self.event_source,
                "restrict_events_to_footprints",
                None,
            )
            if callable(restrict_events):
                restrict_events(
                    footprints,
                    full_range_before_ms=emit_start_ms,
                )
        previous_footprints = self._footprint_candles_by_key
        previous_refills = self._refill_payloads_by_key
        previous_emitted_zones = self._emitted_zone_ids
        previous_active_zones = self._active_zones_by_key
        previous_last_footprint_candle = self._last_footprint_candle_by_key
        previous_terminal_zones = self._terminal_zones_by_candle_key
        previous_terminal_cancel_checked = self._terminal_cancel_checked_by_candle_key
        previous_orders = self._orders
        previous_processed_event_ids = self._processed_event_ids
        previous_price_activity_levels = self._price_activity_levels
        previous_price_activity_touched = self._price_activity_touched
        previous_price_level_states = self._price_level_states
        previous_price_activity_emit_enabled = self._price_activity_emit_enabled
        self._orders = {}
        self._processed_event_ids = set()
        self._price_activity_levels = {}
        self._price_activity_touched = set()
        self._price_level_states = {}
        self._price_activity_emit_enabled = emit_window_started
        self._footprint_candles_by_key = {
            snapshot.symbol.key: snapshot.candles
            for snapshot in footprints
        }
        self._refill_payloads_by_key = {}
        self._emitted_zone_ids = set()
        self._active_zones_by_key = {}
        self._last_footprint_candle_by_key = {}
        self._terminal_zones_by_candle_key = {}
        self._terminal_cancel_checked_by_candle_key = {}
        payloads: list[dict[str, Any]] = []
        processed_event_count = 0
        limit_reached = False
        try:
            for symbol in symbols:
                if limit_reached:
                    break
                source_events = self.event_source.events(
                    symbol,
                    start_ms=int(request.start_ms),
                    end_ms=int(request.end_ms),
                )
                ordered_events = (
                    source_events
                    if self.config.events_are_time_ordered
                    else sorted(
                        source_events,
                        key=lambda item: (
                            int(item.ts_event_ms),
                            int(item.sequence or 0),
                            str(item.source_file or ""),
                            str(item.order_id or ""),
                            str(item.action or ""),
                        ),
                    )
                )
                for event in ordered_events:
                    processed_event_count += 1
                    if not emit_window_started and int(event.ts_event_ms) >= emit_start_ms:
                        self._reset_refill_tracking_for_emit_window()
                        self._price_activity_levels.clear()
                        self._price_activity_touched.clear()
                        self._price_activity_emit_enabled = True
                        emit_window_started = True
                    event_payloads = self.process_event_payloads(symbol, event)
                    if (
                        event_payloads
                        and int(event.ts_event_ms) >= emit_start_ms
                        and self.config.collect_event_payloads
                    ):
                        event_payloads = tuple(
                            _payload_with_actor_proxy_replay_window(
                                payload,
                                start_ms=int(request.start_ms),
                                end_ms=int(request.end_ms),
                            )
                            for payload in event_payloads
                        )
                        remaining = int(self.config.max_payloads) - len(payloads)
                        publish_payloads = event_payloads[:remaining]
                        payloads.extend(publish_payloads)
                        self._publish(publish_payloads)
                    if (
                        self.config.collect_event_payloads
                        and len(payloads) >= self.config.max_payloads
                    ):
                        limit_reached = True
                        break
            if self.config.emit_price_activity_levels and not limit_reached:
                activity_payloads = self._price_activity_payloads()
                remaining = int(self.config.max_payloads) - len(payloads)
                publish_payloads = activity_payloads[:remaining]
                payloads.extend(publish_payloads)
                self._publish(publish_payloads)
        finally:
            self._footprint_candles_by_key = previous_footprints
            self._refill_payloads_by_key = previous_refills
            self._emitted_zone_ids = previous_emitted_zones
            self._active_zones_by_key = previous_active_zones
            self._last_footprint_candle_by_key = previous_last_footprint_candle
            self._terminal_zones_by_candle_key = previous_terminal_zones
            self._terminal_cancel_checked_by_candle_key = previous_terminal_cancel_checked
            self._orders = previous_orders
            self._processed_event_ids = previous_processed_event_ids
            self._price_activity_levels = previous_price_activity_levels
            self._price_activity_touched = previous_price_activity_touched
            self._price_level_states = previous_price_level_states
            self._price_activity_emit_enabled = previous_price_activity_emit_enabled
        return ProcessRunResult(
            start_ms=int(request.start_ms),
            end_ms=int(request.end_ms),
            symbols=tuple(symbols),
            processed_event_count=processed_event_count,
            emitted_payload_count=len(payloads),
            payloads=tuple(payloads),
            footprints=footprints,
            footprint_candle_count=sum(item.candle_count for item in footprints),
        )

    def _reset_refill_tracking_for_emit_window(self) -> None:
        for order in self._orders.values():
            order.refill_count = 0
            order.refill_contracts = 0
            order.latest_refill_contracts = 0
            order.existing_qty = int(order.current_size)
            order.refill_lots.clear()
            order.trade_count = 0
            order.executed_contracts = 0
            order.threshold_time_ms = 0
            order.threshold_price = None
            order.threshold_side = ""

    def _activity_level(
        self,
        symbol: ProcessSymbol,
        *,
        price: Decimal,
        side: str,
        event_time_ms: int,
        opening_liquidity: int,
    ) -> tuple[tuple[str, str, int, Decimal, str], _PriceActivityLevel]:
        normalized_side = _normalized_side(side)
        candle_open_time_ms = _event_candle_open_time_ms(symbol.timeframe, event_time_ms)
        key = (*symbol.key, candle_open_time_ms, price, normalized_side)
        level = self._price_activity_levels.get(key)
        if level is None:
            level = _PriceActivityLevel(
                provider_symbol=symbol.provider_symbol,
                mt5_symbol=symbol.mt5_symbol,
                timeframe=symbol.timeframe,
                price=price,
                side=normalized_side,
                candle_open_time_ms=candle_open_time_ms,
                opening_liquidity=max(0, int(opening_liquidity)),
                closing_liquidity=max(0, int(opening_liquidity)),
            )
            self._price_activity_levels[key] = level
        if self._price_activity_emit_enabled:
            self._price_activity_touched.add(key)
        return key, level

    def _price_level_state(
        self,
        symbol: ProcessSymbol,
        *,
        price: Decimal,
        side: str,
    ) -> _PriceLevelState:
        key = (*symbol.key, price, _normalized_side(side))
        return self._price_level_states.setdefault(key, _PriceLevelState())

    @staticmethod
    def _consume_level_liquidity(
        state: _PriceLevelState,
        quantity: int,
    ) -> tuple[int, tuple[_RefillLot, ...]]:
        remaining = min(max(0, int(quantity)), state.total_liquidity)
        consumed_total = remaining
        non_refill = min(remaining, max(0, int(state.non_refill_liquidity)))
        state.non_refill_liquidity -= non_refill
        remaining -= non_refill
        consumed_refill: list[_RefillLot] = []
        for lot in state.refill_lots:
            if remaining <= 0:
                break
            amount = min(remaining, max(0, int(lot.remaining_qty)))
            if amount <= 0:
                continue
            lot.remaining_qty -= amount
            remaining -= amount
            consumed_refill.append(
                _RefillLot(lot.candle_open_time_ms, lot.price, lot.side, amount)
            )
        state.refill_lots = [lot for lot in state.refill_lots if int(lot.remaining_qty) > 0]
        return consumed_total, tuple(consumed_refill)

    def _add_level_liquidity(
        self,
        state: _PriceLevelState,
        level: _PriceActivityLevel,
        *,
        quantity: int,
    ) -> tuple[_LevelRefillChange, ...]:
        added = max(0, int(quantity))
        if added <= 0:
            return ()

        # Pending execution from an older candle must not classify a new Add
        # in the current candle as a refill.
        if (
            int(state.pending_replenishment_candle_open_time_ms) > 0
            and int(state.pending_replenishment_candle_open_time_ms)
            != int(level.candle_open_time_ms)
        ):
            state.pending_replenishment_qty = 0
            state.pending_replenishment_candle_open_time_ms = 0

        pending_replenishment = max(
            0,
            int(state.pending_replenishment_qty),
        )

        # Only the portion that replaces previously executed liquidity
        # is classified as refill.
        refill_added = min(
            added,
            pending_replenishment,
        )

        non_refill_added = added - refill_added

        state.pending_replenishment_qty = (
            pending_replenishment - refill_added
        )

        if state.pending_replenishment_qty <= 0:
            state.pending_replenishment_qty = 0
            state.pending_replenishment_candle_open_time_ms = 0

        # Ordinary added liquidity remains in the normal liquidity pool.
        state.non_refill_liquidity += non_refill_added

        # Refill liquidity is separately tracked only so its later
        # execution or withdrawal can be measured.
        if refill_added > 0:
            state.refill_lots.append(
                _RefillLot(
                    candle_open_time_ms=level.candle_open_time_ms,
                    price=level.price,
                    side=level.side,
                    remaining_qty=refill_added,
                )
            )

            level.refill_count += 1
            level.refill_added_contracts += refill_added

        # Gross added includes both ordinary Add and refill,
        # but each contract is counted only once.
        level.gross_added_contracts += added
        level.non_refill_added_contracts += non_refill_added
        level.added_contracts = level.gross_added_contracts
        level.add_event_count += 1

        if refill_added <= 0:
            return ()

        return (
            _LevelRefillChange(
                candle_open_time_ms=level.candle_open_time_ms,
                price=level.price,
                side=level.side,
                refill_count_delta=1,
                refill_added_delta=refill_added,
            ),
        )

    def _record_price_activity(
        self,
        symbol: ProcessSymbol,
        event: DomRawEvent,
    ) -> tuple[_LevelRefillChange, ...]:
        action = str(event.action or "").strip().upper()
        if (
            action in FILL_ACTIONS
            and int(event.ts_event_ms) >= 1780870080000
            and int(event.ts_event_ms) < 1780870140000
            and event.price == Decimal("28875.5")
            and _normalized_side(event.side) == "BID"
        ):
            print(
                "TARGET_RAW_FILL",
                "ts=", event.ts_event_ms,
                "sequence=", event.sequence,
                "order_id=", event.order_id,
                "size=", event.size,
                "price=", event.price,
                "side=", event.side,
                "source=", event.source_file,
                flush=True,
            )
        order = self._orders.get(_order_key(symbol, event.order_id)) if event.order_id else None
        event_side = _normalized_side(event.side or (order.side if order is not None else ""))
        event_price = event.price or (order.price if order is not None else None)
        changes: list[_LevelRefillChange] = []

        def activity(price: Decimal, side: str) -> tuple[_PriceLevelState, _PriceActivityLevel]:
            state = self._price_level_state(symbol, price=price, side=side)
            _, level = self._activity_level(
                symbol,
                price=price,
                side=side,
                event_time_ms=event.ts_event_ms,
                opening_liquidity=state.total_liquidity,
            )
            return state, level

        def finish(state: _PriceLevelState, level: _PriceActivityLevel) -> None:
            level.closing_liquidity = state.total_liquidity
            level.last_event_time_ms = int(event.ts_event_ms)

        def consume(
            state: _PriceLevelState,
            level: _PriceActivityLevel,
            quantity: int,
            *,
            executed: bool,
        ) -> None:
            requested_qty = max(0, int(quantity))

            if requested_qty <= 0:
                finish(state, level)
                return

            if executed:
                known_liquidity_before_fill = max(
                    0,
                    int(state.total_liquidity),
                )

                missing_liquidity = max(
                    0,
                    requested_qty - known_liquidity_before_fill,
                )

                if missing_liquidity > 0:
                    # A raw Fill proves that this liquidity existed, even when
                    # the replay state did not reconstruct it beforehand.
                    #
                    # Treat the missing amount as inferred opening liquidity
                    # for this candle/price/side so the execution is not lost
                    # and the liquidity-conservation equation remains valid.
                    state.non_refill_liquidity += missing_liquidity
                    level.opening_liquidity += missing_liquidity
                    level.opening_liquidity_inferred = True

            consumed, refill_lots = self._consume_level_liquidity(
                state,
                requested_qty,
            )

            if executed:
                # After inferring any missing liquidity above, consumed should
                # equal the raw Fill quantity.
                level.executed_contracts += consumed
                level.fill_event_count += int(consumed > 0)

                if consumed > 0:
                    if (
                        int(state.pending_replenishment_candle_open_time_ms) > 0
                        and int(state.pending_replenishment_candle_open_time_ms)
                        != int(level.candle_open_time_ms)
                    ):
                        state.pending_replenishment_qty = 0

                    state.pending_replenishment_candle_open_time_ms = int(
                        level.candle_open_time_ms
                    )
                    state.pending_replenishment_qty += consumed

            else:
                level.withdrawn_contracts += consumed
                level.cancelled_or_withdrawn_contracts = (
                    level.withdrawn_contracts
                )

            for lot in refill_lots:
                origin_key = (
                    *symbol.key,
                    lot.candle_open_time_ms,
                    lot.price,
                    lot.side,
                )
                origin = self._price_activity_levels.get(origin_key)

                if origin is not None:
                    if executed:
                        origin.executed_refill_contracts += lot.remaining_qty
                    else:
                        origin.withdrawn_refill_contracts += lot.remaining_qty

                changes.append(
                    _LevelRefillChange(
                        candle_open_time_ms=lot.candle_open_time_ms,
                        price=lot.price,
                        side=lot.side,
                        refill_executed_delta=(
                            lot.remaining_qty if executed else 0
                        ),
                        refill_withdrawn_delta=(
                            lot.remaining_qty if not executed else 0
                        ),
                    )
                )

            finish(state, level)

        if action in CLEAR_ACTIONS:
            for tracked in tuple(self._orders.values()):
                if (
                    tracked.provider_symbol.strip().upper(),
                    tracked.timeframe.strip().upper(),
                ) != symbol.key:
                    continue

                state, level = activity(
                    tracked.price,
                    tracked.side,
                )
                level.order_ids.add(str(tracked.order_id))
                consume(
                    state,
                    level,
                    tracked.current_size,
                    executed=False,
                )

            for state_key, price_state in tuple(
                self._price_level_states.items()
            ):
                if state_key[:2] != symbol.key:
                    continue

                price_state.non_refill_liquidity = 0
                price_state.refill_lots.clear()
                price_state.pending_replenishment_qty = 0
                price_state.pending_replenishment_candle_open_time_ms = 0

            return tuple(changes)
        if event_price is None or event_side not in {"BID", "ASK"}:
            return ()
        if action in ADD_ACTIONS:
            if order is not None and int(order.current_size) > 0:
                old_state, old_level = activity(order.price, order.side)
                old_level.order_ids.add(str(event.order_id))
                consume(old_state, old_level, order.current_size, executed=False)
            state, level = activity(event_price, event_side)
            level.order_ids.add(str(event.order_id))
            changes.extend(self._add_level_liquidity(state, level, quantity=event.size))
            finish(state, level)
            return tuple(changes)
        if order is None:
            if action in MODIFY_ACTIONS:
                state, level = activity(event_price, event_side)
                level.order_ids.add(str(event.order_id))

                inferred_size = max(0, int(event.size or 0))

                if inferred_size > 0:
                    # First observed state of an already-existing order.
                    # It is inferred opening liquidity, not a new Add or refill.
                    state.non_refill_liquidity += inferred_size
                    level.opening_liquidity += inferred_size
                    level.opening_liquidity_inferred = True

                finish(state, level)
                return tuple(changes)

            if action in FILL_ACTIONS:
                state, level = activity(event_price, event_side)
                level.order_ids.add(str(event.order_id))

                fill_size = max(
                    0,
                    int(event.size or 0),
                )

                consume(
                    state,
                    level,
                    fill_size,
                    executed=True,
                )

                return tuple(changes)

            # Unknown Cancel/Delete or unsupported action:
            # there is no reliable state to update.
            return tuple(changes)
        if action in MODIFY_ACTIONS:
            new_size = max(0, int(event.size or 0))
            if event_price != order.price or event_side != order.side:
                old_state, old_level = activity(order.price, order.side)
                old_level.order_ids.add(str(event.order_id))
                consume(old_state, old_level, order.current_size, executed=False)
                new_state, new_level = activity(event_price, event_side)
                new_level.order_ids.add(str(event.order_id))
                changes.extend(self._add_level_liquidity(new_state, new_level, quantity=new_size))
                finish(new_state, new_level)
                return tuple(changes)
            delta = new_size - int(order.current_size)
            state, level = activity(order.price, order.side)
            level.order_ids.add(str(event.order_id))
            if delta > 0:
                changes.extend(self._add_level_liquidity(state, level, quantity=delta))
            elif delta < 0:
                consume(state, level, -delta, executed=False)
            finish(state, level)
            return tuple(changes)
            
        if action in FILL_ACTIONS:
            fill_price = event.price or order.price
            fill_side = _normalized_side(
                event.side or order.side
            )

            state, level = activity(
                fill_price,
                fill_side,
            )
            level.order_ids.add(str(event.order_id))

            reported_fill = max(
                0,
                int(event.size or 0),
            )

            consume(
                state,
                level,
                reported_fill,
                executed=True,
            )

            return tuple(changes)
        if action in CANCEL_ACTIONS:
            state, level = activity(order.price, order.side)
            level.order_ids.add(str(event.order_id))

            canceled = max(0, int(event.size or 0))
            reduction = (
                min(canceled, max(0, int(order.current_size)))
                if action == "C" and canceled > 0
                else max(0, int(order.current_size))
            )

            consume(
                state,
                level,
                reduction,
                executed=False,
            )

            return tuple(changes)

        # Known order, but the event action is unsupported.
        return tuple(changes)

    def _price_activity_payloads(self) -> tuple[dict[str, Any], ...]:
        payloads: list[dict[str, Any]] = []
        for key in sorted(self._price_activity_touched, key=lambda item: (item[0], item[1], item[2], item[3], item[4])):
            level = self._price_activity_levels.get(key)
            if level is None:
                continue
            if self.config.filter_price_activity_to_footprints:
                candles = self._footprint_candles_by_key.get(
                    (level.provider_symbol.strip().upper(), level.timeframe.strip().upper()),
                    (),
                )
                footprint_candle = next(
                    (
                        candle
                        for candle in candles
                        if _footprint_candle_contains_time(
                            candle,
                            timestamp_ms=int(level.candle_open_time_ms),
                        )
                    ),
                    None,
                )
                if (
                    footprint_candle is None
                    or _footprint_bin_for_price(footprint_candle, level.price) is None
                ):
                    continue
            opening = max(0, int(level.opening_liquidity))
            added = max(0, int(level.gross_added_contracts))
            executed = max(0, int(level.executed_contracts))
            withdrawn = max(0, int(level.withdrawn_contracts))
            closing = max(0, int(level.closing_liquidity))
            available = opening + added
            expected_closing = opening + added - executed - withdrawn
            execution_exceeds_available = executed > available
            invariant_ok = closing == expected_closing and expected_closing >= 0
            rate = round(executed / available * 100.0, 1) if available > 0 else None
            rate_label = (
                f"{rate:.1f}".rstrip("0").rstrip(".") if rate is not None else "N/A"
            )
            refill_added = max(0, int(level.refill_added_contracts))
            refill_executed = min(refill_added, max(0, int(level.executed_refill_contracts)))
            refill_rate = round(refill_executed / refill_added * 100.0, 1) if refill_added > 0 else 0.0
            refill_rate_label = f"{refill_rate:.1f}".rstrip("0").rstrip(".")
            candle_open_time_ms = int(level.candle_open_time_ms)
            output_id = "|".join((
                "DATA_PROCESS", "PRICE_ACTIVITY", level.provider_symbol.upper(),
                level.timeframe.upper(), str(candle_open_time_ms), str(level.price), level.side,
            ))
            order_count = len(level.order_ids)
            has_activity = bool(order_count > 0 or executed > 0 or added > 0)
            payloads.append({
                "payload_id": output_id,
                "id": output_id,
                "output_id": output_id,
                "producer": DATA_PROCESS_ENGINE_PRODUCER,
                "source_engine": "dataProcessEngine",
                "type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
                "payload_type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
                "output_type": "DATA_PROCESS_PRICE_ACTIVITY_LEVEL",
                "action": "ANALYZE",
                "timestamp_ms": candle_open_time_ms,
                "event_time_ms": candle_open_time_ms,
                "marker_time_ms": candle_open_time_ms,
                "marker_price": str(level.price),
                "provider_symbol": level.provider_symbol,
                "symbol": level.provider_symbol,
                "mt5_symbol": level.mt5_symbol,
                "timeframe": level.timeframe,
                "price": str(level.price),
                "side": level.side,
                "order_count": order_count,
                "order_ids": tuple(sorted(level.order_ids)),
                "add_event_count": int(level.add_event_count),
                "fill_event_count": int(level.fill_event_count),
                "opening_liquidity": opening,
                "opening_liquidity_inferred": bool(level.opening_liquidity_inferred),
                "available_liquidity": available,
                "gross_added_contracts": added,
                "non_refill_added_contracts": int(level.non_refill_added_contracts),
                "added_contracts": added,
                "executed_contracts": executed,
                "withdrawn_contracts": withdrawn,
                "cancelled_or_withdrawn_contracts": withdrawn,
                "closing_liquidity": closing,
                "level_execution_rate": rate,
                "level_execution_rate_defined": rate is not None,
                "execution_exceeds_available": execution_exceeds_available,
                "level_execution_invariant_ok": invariant_ok,
                "added_breakdown_invariant_ok": (
                    added
                    == int(level.non_refill_added_contracts) + refill_added
                ),
                "liquidity_conservation_delta": closing - expected_closing,
                "refill_count": int(level.refill_count),
                "refill_contracts": refill_added,
                "price_base_refill_count": int(level.refill_count),
                "price_base_refill_contracts": refill_added,
                "refill_added_contracts": refill_added,
                "executed_refill_contracts": refill_executed,
                "withdrawn_refill_contracts": min(
                    refill_added, max(0, int(level.withdrawn_refill_contracts))
                ),
                "refill_execution_rate": refill_rate,
                "refill_display": (
                    f"{int(level.refill_count)}({refill_added}) "
                    f"E{refill_executed} - {refill_rate_label}%"
                ),
                "refill_method": "price_base_refill",
                "has_refill": (
                    int(level.refill_count) > 0
                    and refill_added > 0
                ),
                "has_price_activity": has_activity,
                "display_mode": (
                    "refill"
                    if int(level.refill_count) > 0 and refill_added > 0
                    else "level_execution"
                ),
                "display_text": (
                    f"{int(level.refill_count)}({refill_added}) "
                    f"E{refill_executed} - {refill_rate_label}%"
                    if int(level.refill_count) > 0 and refill_added > 0
                    else f"O{order_count} A{added} E{executed} - {rate_label}%"
                ),
                "source": "DATA_PROCESS_PRICE_ACTIVITY_LEVEL",
            })
        return tuple(payloads)

    def _open_refill_payloads_at_replay_end(
        self,
        *,
        symbols: Iterable[ProcessSymbol],
        end_ms: int,
    ) -> tuple[dict[str, Any], ...]:
        symbol_keys = {symbol.key for symbol in symbols}
        payloads: list[dict[str, Any]] = []
        for key, order in list(self._orders.items()):
            if key[:2] not in symbol_keys:
                continue
            if int(order.refill_count) <= 0:
                continue
            close_event = DomRawEvent(
                ts_event_ms=int(end_ms),
                price=order.price,
                size=int(order.current_size),
                side=order.side,
                action="E",
                order_id=order.order_id,
                instrument_id=0,
                sequence=int(end_ms),
                source_file="DATA_PROCESS_REPLAY_END",
            )
            payloads.extend(
                self._payloads_from_refilled_order(
                    order,
                    event=close_event,
                    reason="REPLAY_END",
                )
            )
        return tuple(payloads)

    def _load_footprints(
        self,
        *,
        symbols: Iterable[ProcessSymbol],
        start_ms: int,
        end_ms: int,
    ) -> tuple[ProcessFootprintSnapshot, ...]:
        if self.footprint_source is None:
            return ()
        snapshots: list[ProcessFootprintSnapshot] = []
        for symbol in symbols:
            candles = tuple(
                dict(candle)
                for candle in self.footprint_source.candles(
                    symbol,
                    start_ms=int(start_ms),
                    end_ms=int(end_ms),
                )
            )
            snapshots.append(
                ProcessFootprintSnapshot(
                    symbol=symbol,
                    candles=candles,
                )
            )
        return tuple(snapshots)

    def process_event(
        self,
        symbol: ProcessSymbol,
        event: DomRawEvent,
    ) -> dict[str, Any] | None:
        payloads = self.process_event_payloads(symbol, event)
        return payloads[0] if payloads else None

    def process_event_payloads(
        self,
        symbol: ProcessSymbol,
        event: DomRawEvent,
    ) -> tuple[dict[str, Any], ...]:
        event_identity = (
            *symbol.key,
            str(event.source_file or ""),
            int(event.instrument_id or 0),
            int(event.sequence or 0),
            int(event.ts_event_ms),
            str(event.order_id or ""),
            str(event.action or "").strip().upper(),
            str(event.price or ""),
            int(event.size or 0),
            _normalized_side(event.side),
        )
        if self.config.deduplicate_events:
            if event_identity in self._processed_event_ids:
                return ()
            self._processed_event_ids.add(event_identity)
        level_changes = self._record_price_activity(symbol, event)
        terminal_cancel_payloads = (
            self._terminal_cancel_payloads(symbol, event)
            if self.config.collect_event_payloads
            else ()
        )
        def output(*payloads: dict[str, Any]) -> tuple[dict[str, Any], ...]:
            if not self.config.collect_event_payloads:
                return ()
            return (*payloads, *self._payloads_for_level_changes(symbol, event, level_changes))
        action = str(event.action or "").strip().upper()
        if action in CLEAR_ACTIONS:
            return output(*terminal_cancel_payloads, *self._clear_symbol_orders(symbol, event))
        if not str(event.order_id or "").strip():
            return output(*terminal_cancel_payloads)
        if event.price is None and action not in CANCEL_ACTIONS:
            return output(*terminal_cancel_payloads)
        if action in ADD_ACTIONS:
            self._add_order(symbol, event)
            return output(*terminal_cancel_payloads)
        if action in MODIFY_ACTIONS:
            return output(*terminal_cancel_payloads, *self._modify_order(symbol, event))
        if action in FILL_ACTIONS:
            return output(*terminal_cancel_payloads, *self._fill_order(symbol, event))
        if action in CANCEL_ACTIONS:
            return output(*terminal_cancel_payloads, *self._cancel_order(symbol, event))
        return output(*terminal_cancel_payloads)

    def _add_order(self, symbol: ProcessSymbol, event: DomRawEvent) -> None:
        if event.price is None:
            return
        size = max(0, int(event.size or 0))
        self._orders[_order_key(symbol, event.order_id)] = _TrackedOrder(
            order_id=str(event.order_id),
            provider_symbol=symbol.provider_symbol,
            mt5_symbol=symbol.mt5_symbol,
            market_provider=symbol.market_provider,
            timeframe=symbol.timeframe,
            price=event.price,
            side=_normalized_side(event.side),
            current_size=size,
            initial_order_size=size,
            max_order_size=size,
            opened_at_ms=int(event.ts_event_ms),
            updated_at_ms=int(event.ts_event_ms),
            instrument_id=int(event.instrument_id or 0),
            source_file=str(event.source_file or ""),
            existing_qty=size,
        )

    def _modify_order(self, symbol: ProcessSymbol, event: DomRawEvent) -> tuple[dict[str, Any], ...]:
        if event.price is None:
            return ()
        key = _order_key(symbol, event.order_id)
        order = self._orders.get(key)
        if order is None:
            size = max(0, int(event.size or 0))
            self._orders[key] = _TrackedOrder(
                order_id=str(event.order_id),
                provider_symbol=symbol.provider_symbol,
                mt5_symbol=symbol.mt5_symbol,
                market_provider=symbol.market_provider,
                timeframe=symbol.timeframe,
                price=event.price,
                side=_normalized_side(event.side),
                current_size=size,
                initial_order_size=size,
                max_order_size=size,
                opened_at_ms=int(event.ts_event_ms),
                updated_at_ms=int(event.ts_event_ms),
                instrument_id=int(event.instrument_id or 0),
                source_file=str(event.source_file or ""),
                existing_qty=size,
            )
            return ()
        new_size = max(0, int(event.size or 0))
        old_price = order.price
        old_side = order.side
        new_side = _normalized_side(event.side or order.side)
        if event.price != old_price or new_side != old_side:
            order.price = event.price
            order.side = new_side
            order.current_size = new_size
            order.existing_qty = new_size
            order.refill_lots.clear()
            order.pending_refill_contracts = 0
            order.updated_at_ms = int(event.ts_event_ms)
            return ()
        order.price = event.price
        order.side = new_side
        order.current_size = new_size
        order.existing_qty = new_size
        order.pending_refill_contracts = 0
        order.refill_lots.clear()
        order.max_order_size = max(order.max_order_size, new_size)
        order.updated_at_ms = int(event.ts_event_ms)
        if int(event.instrument_id or 0) > 0:
            order.instrument_id = int(event.instrument_id or 0)
        if str(event.source_file or "").strip():
            order.source_file = str(event.source_file or "")
        return ()

    def _fill_order(self, symbol: ProcessSymbol, event: DomRawEvent) -> tuple[dict[str, Any], ...]:
        key = _order_key(symbol, event.order_id)
        order = self._orders.get(key)
        if order is None:
            if event.price is None:
                return ()
            order = _TrackedOrder(
                order_id=str(event.order_id),
                provider_symbol=symbol.provider_symbol,
                mt5_symbol=symbol.mt5_symbol,
                market_provider=symbol.market_provider,
                timeframe=symbol.timeframe,
                price=event.price,
                side=_normalized_side(event.side),
                current_size=0,
                initial_order_size=0,
                max_order_size=0,
                opened_at_ms=int(event.ts_event_ms),
                updated_at_ms=int(event.ts_event_ms),
                instrument_id=int(event.instrument_id or 0),
                source_file=str(event.source_file or ""),
                existing_qty=0,
            )
            self._orders[key] = order
        executed = max(0, int(event.size or 0))
        order.trade_count += 1
        order.executed_contracts += executed
        order.updated_at_ms = int(event.ts_event_ms)
        if int(event.instrument_id or 0) > 0:
            order.instrument_id = int(event.instrument_id or 0)
        if str(event.source_file or "").strip():
            order.source_file = str(event.source_file or "")
        if event.price is not None:
            order.price = event.price
        order.side = _normalized_side(event.side or order.side)
        # Raw Fill is authoritative. The tracked order state may be
        # incomplete, so never reduce the reported execution quantity.
        order.current_size = max(
            0,
            int(order.current_size) - executed,
        )
        
        order.existing_qty = order.current_size
        order.pending_refill_contracts = 0
        order.refill_lots.clear()
        return ()

    def _cancel_order(self, symbol: ProcessSymbol, event: DomRawEvent) -> tuple[dict[str, Any], ...]:
        key = _order_key(symbol, event.order_id)
        order = self._orders.get(key)
        if order is None:
            return ()
        canceled = max(0, int(event.size or 0))
        action = str(event.action or "").strip().upper()
        reduction = min(canceled, int(order.current_size)) if action == "C" and canceled > 0 else int(order.current_size)
        order.current_size = max(0, int(order.current_size) - reduction)
        order.existing_qty = order.current_size
        order.updated_at_ms = int(event.ts_event_ms)
        if int(event.instrument_id or 0) > 0:
            order.instrument_id = int(event.instrument_id or 0)
        if str(event.source_file or "").strip():
            order.source_file = str(event.source_file or "")
        if event.price is not None:
            order.price = event.price
        order.side = _normalized_side(event.side or order.side)
        if order.current_size <= 0:
            self._close_order(key, order, close_event=event, close_reason="CANCEL")
        return ()

    def _clear_symbol_orders(
        self,
        symbol: ProcessSymbol,
        event: DomRawEvent,
    ) -> tuple[dict[str, Any], ...]:
        payloads: list[dict[str, Any]] = []
        prefix = (symbol.provider_symbol.strip().upper(), symbol.timeframe.strip().upper())
        for key, order in list(self._orders.items()):
            if key[:2] != prefix:
                continue
            closed_payloads = self._close_order(key, order, close_event=event, close_reason="CLEAR")
            payloads.extend(closed_payloads)
        return tuple(payloads)

    @staticmethod
    def _consume_refill_lots(order: _TrackedOrder, quantity: int) -> tuple[_RefillLot, ...]:
        remaining = max(0, int(quantity))
        consumed: list[_RefillLot] = []
        for lot in order.refill_lots:
            if remaining <= 0:
                break
            amount = min(remaining, max(0, int(lot.remaining_qty)))
            if amount <= 0:
                continue
            lot.remaining_qty -= amount
            remaining -= amount
            consumed.append(
                _RefillLot(
                    candle_open_time_ms=lot.candle_open_time_ms,
                    price=lot.price,
                    side=lot.side,
                    remaining_qty=amount,
                )
            )
        order.refill_lots = [lot for lot in order.refill_lots if int(lot.remaining_qty) > 0]
        return tuple(consumed)

    def _withdraw_order_liquidity(
        self,
        order: _TrackedOrder,
        quantity: int,
    ) -> tuple[_RefillLot, ...]:
        remaining = max(0, int(quantity))
        existing_withdrawn = min(remaining, max(0, int(order.existing_qty)))
        order.existing_qty -= existing_withdrawn
        remaining -= existing_withdrawn
        return self._consume_refill_lots(order, remaining)

    def _payloads_for_level_changes(
        self,
        symbol: ProcessSymbol,
        event: DomRawEvent,
        changes: Iterable[_LevelRefillChange],
    ) -> tuple[dict[str, Any], ...]:
        grouped: dict[tuple[int, Decimal, str], _LevelRefillChange] = {}
        for change in changes:
            key = (change.candle_open_time_ms, change.price, change.side)
            previous = grouped.get(key)
            grouped[key] = _LevelRefillChange(
                candle_open_time_ms=change.candle_open_time_ms,
                price=change.price,
                side=change.side,
                refill_count_delta=(previous.refill_count_delta if previous else 0) + change.refill_count_delta,
                refill_added_delta=(previous.refill_added_delta if previous else 0) + change.refill_added_delta,
                refill_executed_delta=(previous.refill_executed_delta if previous else 0) + change.refill_executed_delta,
                refill_withdrawn_delta=(previous.refill_withdrawn_delta if previous else 0) + change.refill_withdrawn_delta,
            )
        payloads: list[dict[str, Any]] = []
        for index, change in enumerate(grouped.values()):
            stats = self._price_activity_levels.get(
                (*symbol.key, change.candle_open_time_ms, change.price, change.side)
            )
            state = self._price_level_states.get((*symbol.key, change.price, change.side))
            emit_individual = bool(self.config.emit_individual_refill_orders)
            proxy = _TrackedOrder(
                order_id=(
                    f"PRICE_LEVEL|{change.candle_open_time_ms}|{change.price}|{change.side}"
                    + (f"|{event.ts_event_ms}|{event.sequence}|{index}" if emit_individual else "")
                ),
                provider_symbol=symbol.provider_symbol,
                mt5_symbol=symbol.mt5_symbol,
                market_provider=symbol.market_provider,
                timeframe=symbol.timeframe,
                price=change.price,
                side=change.side,
                current_size=state.total_liquidity if state is not None else 0,
                initial_order_size=stats.opening_liquidity if stats is not None else 0,
                max_order_size=(
                    stats.opening_liquidity + stats.gross_added_contracts
                    if stats is not None else 0
                ),
                opened_at_ms=change.candle_open_time_ms,
                updated_at_ms=int(event.ts_event_ms),
                instrument_id=int(event.instrument_id or 0),
                source_file=str(event.source_file or ""),
                refill_count=stats.refill_count if stats is not None else change.refill_count_delta,
                refill_contracts=(
                    stats.refill_added_contracts if stats is not None else change.refill_added_delta
                ),
                trade_count=stats.fill_event_count if stats is not None else 0,
                executed_contracts=stats.executed_contracts if stats is not None else 0,
                threshold_time_ms=change.candle_open_time_ms,
                threshold_price=change.price,
                threshold_side=change.side,
            )
            reason = (
                "REFILL"
                if change.refill_added_delta > 0
                else "REFILL_EXECUTED"
                if change.refill_executed_delta > 0
                else "REFILL_WITHDRAWN"
            )
            count_delta = change.refill_count_delta if emit_individual else (stats.refill_count if stats else 0)
            added_delta = change.refill_added_delta if emit_individual else (stats.refill_added_contracts if stats else 0)
            executed_delta = change.refill_executed_delta if emit_individual else (stats.executed_refill_contracts if stats else 0)
            withdrawn_delta = change.refill_withdrawn_delta if emit_individual else (stats.withdrawn_refill_contracts if stats else 0)
            generated = self._payloads_from_refilled_order(
                    proxy,
                    event=event,
                    reason=reason,
                    metric_price=change.price,
                    metric_side=change.side,
                    metric_candle_open_time_ms=change.candle_open_time_ms,
                    refill_count_delta=count_delta,
                    refill_added_delta=added_delta,
                    refill_executed_delta=executed_delta,
                    refill_withdrawn_delta=withdrawn_delta,
                    emit_candidate=change.refill_added_delta > 0,
                )
            for payload in generated:
                normalized = dict(payload)
                normalized["level_event_order_id"] = str(event.order_id or "")
                if emit_individual:
                    normalized["order_id"] = ""
                    normalized["venue_order_id"] = ""
                payloads.append(normalized)
        return tuple(payloads)

    def _payloads_for_refill_lot_changes(
        self,
        order: _TrackedOrder,
        *,
        event: DomRawEvent,
        changes: Iterable[_RefillLot],
        reason: str,
        change_kind: str,
    ) -> tuple[dict[str, Any], ...]:
        payloads: list[dict[str, Any]] = []
        grouped: dict[tuple[int, Decimal, str], int] = {}
        for lot in changes:
            key = (int(lot.candle_open_time_ms), lot.price, lot.side)
            grouped[key] = grouped.get(key, 0) + max(0, int(lot.remaining_qty))
        for (candle_open_time_ms, price, side), amount in grouped.items():
            if amount <= 0:
                continue
            payloads.extend(
                self._payloads_from_refilled_order(
                    order,
                    event=event,
                    reason=reason,
                    metric_price=price,
                    metric_side=side,
                    metric_candle_open_time_ms=candle_open_time_ms,
                    refill_executed_delta=amount if change_kind == "EXECUTED" else 0,
                    refill_withdrawn_delta=amount if change_kind == "WITHDRAWN" else 0,
                )
            )
        return tuple(payloads)

    def _close_order(
        self,
        key: tuple[str, str, str],
        order: _TrackedOrder,
        *,
        close_event: DomRawEvent,
        close_reason: str,
    ) -> tuple[dict[str, Any], ...]:
        self._orders.pop(key, None)
        return ()

    def _payloads_from_refilled_order(
        self,
        order: _TrackedOrder,
        *,
        event: DomRawEvent,
        reason: str,
        metric_price: Decimal | None = None,
        metric_side: str = "",
        metric_candle_open_time_ms: int = 0,
        refill_count_delta: int = 0,
        refill_added_delta: int = 0,
        refill_executed_delta: int = 0,
        refill_withdrawn_delta: int = 0,
        emit_candidate: bool | None = None,
    ) -> tuple[dict[str, Any], ...]:
        # Aggregate-only scans derive their final rows from
        # ``_price_activity_levels``. Building per-event refill/zone payloads in
        # that mode is both unused and extremely expensive because each payload
        # computes footprint statistics and z-scores.
        if not self.config.collect_event_payloads:
            return ()
        payload = order.to_payload(
            close_event=event,
            close_reason=reason,
            footprint_metrics=self._footprint_metrics_for_order(order),
            metric_price=metric_price,
            metric_side=metric_side,
            metric_candle_open_time_ms=metric_candle_open_time_ms,
            refill_count_delta=refill_count_delta,
            refill_added_delta=refill_added_delta,
            refill_executed_delta=refill_executed_delta,
            refill_withdrawn_delta=refill_withdrawn_delta,
        )
        if self.config.emit_individual_refill_orders:
            return (payload,)
        zone_payload = self._zone_payload_from_refill(
            payload,
            emit_candidate=(
                bool(refill_count_delta > 0 or refill_added_delta > 0)
                if emit_candidate is None else bool(emit_candidate)
            ),
        )
        return (zone_payload,) if zone_payload is not None else ()

    def _zone_payload_from_refill(
        self,
        payload: dict[str, Any],
        *,
        emit_candidate: bool = True,
    ) -> dict[str, Any] | None:
        key = _refill_zone_key(payload)
        if key is None:
            return None
        bucket = self._refill_payloads_by_key.setdefault(key, {})
        order_key = _refill_order_state_key(payload)
        if not order_key:
            return None
        bucket[order_key] = dict(payload)
        if not emit_candidate:
            return None
        candidates = _refill_zones_from_payloads(bucket.values())
        changed_price = _payload_decimal(payload, "price", "threshold_price", "level_price")
        changed_side = _normalized_side(payload.get("side"))
        for zone in sorted(
            candidates,
            key=lambda item: (
                item.refill_count,
                item.market_buy if item.direction == "SHORT" else item.market_sell,
                item.terminal_level.price if item.direction == "SHORT" else -item.terminal_level.price,
            ),
            reverse=True,
        ):
            if changed_price is not None and not any(
                level.price == changed_price and level.side == changed_side
                for level in zone.levels
            ):
                continue
            zone_payload = _payload_from_refill_zone(zone)
            output_id = str(zone_payload.get("output_id") or zone_payload.get("id") or "")
            if output_id in self._emitted_zone_ids:
                continue
            zone_payload = self._active_zone_payload(zone_payload)
            if zone_payload is None:
                continue
            self._emitted_zone_ids.add(output_id)
            return zone_payload
        return None

    def _active_zone_payload(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        key = _active_zone_key(payload)
        new_zone = _active_zone_from_payload(payload)
        if key is None or new_zone is None:
            return None

        active_zones = list(self._active_zones_by_key.get(key, ()))
        canceled_zone_ids: list[str] = []
        kept_zones: list[_ActiveZone] = []
        same_zone_updates: list[_ActiveZone] = []
        same_side_zones: list[_ActiveZone] = []

        for active in active_zones:
            if _same_active_zone(active, new_zone):
                same_zone_updates.append(active)
            elif active.side == new_zone.side:
                same_side_zones.append(active)
            else:
                kept_zones.append(active)

        if new_zone.side == "BID":
            if same_side_zones and not all(_zone_below(new_zone, active) for active in same_side_zones):
                return None
            cross_canceled = [
                active
                for active in kept_zones
                if active.side == "ASK" and _zone_above(new_zone, active)
            ]
        elif new_zone.side == "ASK":
            if same_side_zones and not all(_zone_above(new_zone, active) for active in same_side_zones):
                return None
            cross_canceled = [
                active
                for active in kept_zones
                if active.side == "BID" and _zone_below(new_zone, active)
            ]
        else:
            return None

        canceled_zone_ids.extend(active.output_id for active in same_zone_updates)
        canceled_zone_ids.extend(active.output_id for active in same_side_zones)
        canceled_zone_ids.extend(active.output_id for active in cross_canceled)
        canceled_id_set = set(canceled_zone_ids)
        kept_zones = [
            active
            for active in kept_zones
            if active.output_id not in canceled_id_set
        ]
        self._active_zones_by_key[key] = (*kept_zones, new_zone)

        updated = dict(payload)
        updated["zone_status"] = "ACTIVE"
        updated["canceled_zone_ids"] = tuple(canceled_zone_ids)
        updated["active_zone_count"] = len(self._active_zones_by_key[key])
        return updated

    def _terminal_cancel_payloads(
        self,
        symbol: ProcessSymbol,
        event: DomRawEvent,
    ) -> tuple[dict[str, Any], ...]:
        key = symbol.key
        active_zones = tuple(self._active_zones_by_key.get(key, ()))
        if not active_zones:
            return ()
        candle = self._footprint_candle_for_time(
            symbol,
            timestamp_ms=int(event.ts_event_ms),
        )
        if candle is None:
            return ()
        candle_key = (*symbol.key, _payload_int(candle, "open_time_ms", "open_time"))
        active_zone_ids = tuple(sorted(active.output_id for active in active_zones))
        if self._terminal_cancel_checked_by_candle_key.get(candle_key) == active_zone_ids:
            return ()

        terminal_zones = self._terminal_zones_for_candle(symbol, candle)
        if not terminal_zones:
            self._terminal_cancel_checked_by_candle_key[candle_key] = active_zone_ids
            return ()

        canceled_ids: list[str] = []
        matched_terminals: list[_ActiveZone] = []
        for active in active_zones:
            for terminal in terminal_zones:
                if (
                    active.side == "ASK"
                    and terminal.side == "ASK"
                    and _zone_above(terminal, active)
                ):
                    canceled_ids.append(active.output_id)
                    matched_terminals.append(terminal)
                    break
                if (
                    active.side == "BID"
                    and terminal.side == "BID"
                    and _zone_below(terminal, active)
                ):
                    canceled_ids.append(active.output_id)
                    matched_terminals.append(terminal)
                    break
        if not canceled_ids:
            self._terminal_cancel_checked_by_candle_key[candle_key] = active_zone_ids
            return ()

        canceled_id_set = set(canceled_ids)
        self._active_zones_by_key[key] = tuple(
            active
            for active in active_zones
            if active.output_id not in canceled_id_set
        )
        self._terminal_cancel_checked_by_candle_key[candle_key] = tuple(
            sorted(active.output_id for active in self._active_zones_by_key[key])
        )
        return (
            _payload_from_terminal_zone_cancel(
                symbol,
                event=event,
                candle=candle,
                terminal=matched_terminals[0],
                canceled_zone_ids=tuple(dict.fromkeys(canceled_ids)),
                active_zone_count=len(self._active_zones_by_key[key]),
            ),
        )

    def _footprint_candle_for_time(
        self,
        symbol: ProcessSymbol,
        *,
        timestamp_ms: int,
    ) -> Mapping[str, Any] | None:
        cached = self._last_footprint_candle_by_key.get(symbol.key)
        if cached is not None and _footprint_candle_contains_time(
            cached,
            timestamp_ms=int(timestamp_ms),
        ):
            return cached
        candles = self._footprint_candles_by_key.get(symbol.key, ())
        for candle in candles:
            if _footprint_candle_contains_time(candle, timestamp_ms=int(timestamp_ms)):
                self._last_footprint_candle_by_key[symbol.key] = candle
                return candle
        return None

    def _terminal_zones_for_candle(
        self,
        symbol: ProcessSymbol,
        candle: Mapping[str, Any],
    ) -> tuple[_ActiveZone, ...]:
        candle_open_time_ms = _payload_int(candle, "open_time_ms", "open_time")
        key = (*symbol.key, candle_open_time_ms)
        cached = self._terminal_zones_by_candle_key.get(key)
        if cached is not None:
            return cached
        terminal_zones = _terminal_zones_from_footprint_candle(candle)
        self._terminal_zones_by_candle_key[key] = terminal_zones
        return terminal_zones

    def _footprint_metrics_for_order(self, order: _TrackedOrder) -> dict[str, Any]:
        price = order.threshold_price or order.price
        timestamp_ms = int(order.threshold_time_ms or order.updated_at_ms or 0)
        candles = self._footprint_candles_by_key.get(
            (
                order.provider_symbol.strip().upper(),
                order.timeframe.strip().upper(),
            ),
            (),
        )
        for candle in candles:
            open_time_ms = _payload_int(candle, "open_time_ms", "open_time")
            close_time_ms = _payload_int(candle, "close_time_ms", "close_time")
            if close_time_ms <= 0:
                close_time_ms = open_time_ms
            if not (open_time_ms <= timestamp_ms <= close_time_ms):
                continue
            matched_bin = _footprint_bin_for_price(candle, price)
            if matched_bin is None:
                continue
            l2 = matched_bin.get("l2") if isinstance(matched_bin, Mapping) else {}
            if not isinstance(l2, Mapping):
                l2 = {}
            market_buy = _payload_int(
                l2,
                "market_buy",
                "market_buy_contracts",
                "buy_contracts",
                "ask_traded_contracts",
                "ask_traded_volume",
                "buy_volume",
            )
            market_sell = _payload_int(
                l2,
                "market_sell",
                "market_sell_contracts",
                "sell_contracts",
                "bid_traded_contracts",
                "bid_traded_volume",
                "sell_volume",
            )
            side = _normalized_side(order.threshold_side or order.side)
            if side == "BID":
                aggressive_contracts = market_sell
                diagonal_denominator = max(
                    1,
                    _footprint_adjacent_side_contracts(
                        candle,
                        matched_bin,
                        direction="ABOVE",
                        side="BUY",
                    ),
                )
                diagonal_ratio_pass = (
                    market_buy == 0
                    and market_sell > 0
                    and market_sell >= TERMINAL_DIAGONAL_RATIO_MIN * diagonal_denominator
                )
            elif side == "ASK":
                aggressive_contracts = market_buy
                diagonal_denominator = max(
                    1,
                    _footprint_adjacent_side_contracts(
                        candle,
                        matched_bin,
                        direction="BELOW",
                        side="SELL",
                    ),
                )
                diagonal_ratio_pass = (
                    market_sell == 0
                    and market_buy > 0
                    and market_buy >= TERMINAL_DIAGONAL_RATIO_MIN * diagonal_denominator
                )
            else:
                aggressive_contracts = max(market_buy, market_sell)
                diagonal_denominator = 1
                diagonal_ratio_pass = False
            diagonal_ratio = Decimal(aggressive_contracts) / Decimal(diagonal_denominator)
            aggressive_z_score = _footprint_terminal_z_score(
                candle,
                matched_bin,
                side=side,
            )
            return {
                "market_buy": market_buy,
                "market_sell": market_sell,
                "market_buy_contracts": market_buy,
                "market_sell_contracts": market_sell,
                "ask_traded_contracts": _payload_int(
                    l2,
                    "ask_traded_contracts",
                    "buy_contracts",
                    "ask_traded_volume",
                ),
                "bid_traded_contracts": _payload_int(
                    l2,
                    "bid_traded_contracts",
                    "sell_contracts",
                    "bid_traded_volume",
                ),
                "footprint_aggressive_contracts": aggressive_contracts,
                "footprint_aggressive_z_score": str(aggressive_z_score),
                "footprint_diagonal_numerator_contracts": aggressive_contracts,
                "footprint_diagonal_denominator_contracts": diagonal_denominator,
                "footprint_diagonal_ratio": str(diagonal_ratio),
                "footprint_diagonal_ratio_pass": bool(diagonal_ratio_pass),
                "footprint_open_time_ms": open_time_ms,
                "footprint_bin_low": str(
                    matched_bin.get("low")
                    or matched_bin.get("bin_low")
                    or matched_bin.get("price_low")
                    or ""
                ),
                "footprint_bin_high": str(
                    matched_bin.get("high")
                    or matched_bin.get("bin_high")
                    or matched_bin.get("price_high")
                    or ""
                ),
            }
        return _empty_footprint_metrics()

    def _publish(self, payloads: Iterable[Mapping[str, Any]]) -> None:
        payload_tuple = tuple(payloads)
        if not payload_tuple:
            return
        for sink in self.sinks:
            sink.publish(payload_tuple)


def _order_key(symbol: ProcessSymbol, order_id: str) -> tuple[str, str, str]:
    return (
        symbol.provider_symbol.strip().upper(),
        symbol.timeframe.strip().upper(),
        str(order_id or "").strip(),
    )


def _event_candle_open_time_ms(timeframe: str, timestamp_ms: int) -> int:
    timeframe_ms = int(TIMEFRAME_MS_BY_NAME.get(str(timeframe or "").strip().upper(), 0) or 0)
    if timeframe_ms <= 0:
        return int(timestamp_ms)
    return (int(timestamp_ms) // timeframe_ms) * timeframe_ms


def _normalized_side(value: Any) -> str:
    side = str(value or "").strip().upper()
    if side in {"B", "BID", "BUY"}:
        return "BID"
    if side in {"A", "ASK", "SELL"}:
        return "ASK"
    return side


def _footprint_bin_for_price(
    candle: Mapping[str, Any],
    price: Decimal,
) -> Mapping[str, Any] | None:
    fallback: Mapping[str, Any] | None = None
    for raw_bin in candle.get("bins", ()) or ():
        if not isinstance(raw_bin, Mapping):
            continue
        low = _payload_decimal(raw_bin, "low", "bin_low", "price_low")
        high = _payload_decimal(raw_bin, "high", "bin_high", "price_high")
        if low is None:
            continue
        if high is None:
            if low == price:
                return raw_bin
            continue
        if low <= price < high:
            return raw_bin
        if low <= price <= high:
            fallback = raw_bin
    return fallback


def _footprint_adjacent_side_contracts(
    candle: Mapping[str, Any],
    matched_bin: Mapping[str, Any],
    *,
    direction: str,
    side: str,
) -> int:
    adjacent = _footprint_adjacent_bin(candle, matched_bin, direction=direction)
    if adjacent is None:
        return 0
    return _footprint_bin_side_contracts(adjacent, side)


def _footprint_adjacent_bin(
    candle: Mapping[str, Any],
    matched_bin: Mapping[str, Any],
    *,
    direction: str,
) -> Mapping[str, Any] | None:
    matched_low = _payload_decimal(matched_bin, "low", "bin_low", "price_low")
    matched_high = _payload_decimal(matched_bin, "high", "bin_high", "price_high")
    ordered_bins: list[tuple[Decimal, Decimal, Mapping[str, Any]]] = []
    for raw_bin in candle.get("bins", ()) or ():
        if not isinstance(raw_bin, Mapping):
            continue
        low = _payload_decimal(raw_bin, "low", "bin_low", "price_low")
        high = _payload_decimal(raw_bin, "high", "bin_high", "price_high")
        if low is None:
            continue
        if high is None:
            high = low
        ordered_bins.append((low, high, raw_bin))
    ordered_bins.sort(key=lambda item: (item[0], item[1]))

    matched_index: int | None = None
    for index, (low, high, raw_bin) in enumerate(ordered_bins):
        if raw_bin is matched_bin or (low == matched_low and high == matched_high):
            matched_index = index
            break
    if matched_index is None:
        return None

    normalized = str(direction or "").strip().upper()
    if normalized == "ABOVE" and matched_index + 1 < len(ordered_bins):
        return ordered_bins[matched_index + 1][2]
    if normalized == "BELOW" and matched_index - 1 >= 0:
        return ordered_bins[matched_index - 1][2]
    return None


def _footprint_bin_side_contracts(raw_bin: Mapping[str, Any], side: str) -> int:
    l2 = raw_bin.get("l2") if isinstance(raw_bin, Mapping) else {}
    if not isinstance(l2, Mapping):
        return 0
    normalized = str(side or "").strip().upper()
    if normalized == "BUY":
        return _payload_int(
            l2,
            "market_buy",
            "market_buy_contracts",
            "buy_contracts",
            "ask_traded_contracts",
            "ask_traded_volume",
            "buy_volume",
        )
    if normalized == "SELL":
        return _payload_int(
            l2,
            "market_sell",
            "market_sell_contracts",
            "sell_contracts",
            "bid_traded_contracts",
            "bid_traded_volume",
            "sell_volume",
        )
    return 0


def _footprint_candle_contains_time(
    candle: Mapping[str, Any],
    *,
    timestamp_ms: int,
) -> bool:
    open_time_ms = _payload_int(candle, "open_time_ms", "open_time")
    close_time_ms = _payload_int(candle, "close_time_ms", "close_time")
    if close_time_ms <= 0:
        close_time_ms = open_time_ms
    return open_time_ms <= int(timestamp_ms) <= close_time_ms


def _refill_zone_key(payload: Mapping[str, Any]) -> tuple[str, str, int] | None:
    provider_symbol = str(payload.get("provider_symbol") or payload.get("symbol") or "").strip().upper()
    timeframe = str(payload.get("timeframe") or "").strip().upper()
    footprint_open_time_ms = _payload_int(payload, "footprint_open_time_ms")
    if not provider_symbol or not timeframe:
        return None
    if footprint_open_time_ms <= 0:
        timeframe_ms = int(TIMEFRAME_MS_BY_NAME.get(timeframe, 0) or 0)
        timestamp_ms = _payload_int(payload, "timestamp_ms", "event_time_ms", "threshold_time_ms")
        if timeframe_ms > 0 and timestamp_ms > 0:
            footprint_open_time_ms = (timestamp_ms // timeframe_ms) * timeframe_ms
    return provider_symbol, timeframe, footprint_open_time_ms


def _refill_order_state_key(payload: Mapping[str, Any]) -> str:
    order_id = str(payload.get("order_id") or payload.get("venue_order_id") or "").strip()
    if order_id.startswith("PRICE_LEVEL|"):
        return order_id
    timestamp_ms = _payload_int(payload, "timestamp_ms", "event_time_ms", "threshold_time_ms")
    price = str(payload.get("price") or payload.get("threshold_price") or "").strip()
    side = _normalized_side(payload.get("side"))
    if order_id and timestamp_ms > 0 and price and side:
        return "|".join(
            (
                order_id,
                str(timestamp_ms),
                str(_payload_int(payload, "marker_time_ms", "candle_open_time_ms")),
                price,
                side,
                str(payload.get("close_reason") or ""),
            )
        )
    return str(payload.get("payload_id") or payload.get("output_id") or payload.get("id") or "").strip()


def _active_zone_key(payload: Mapping[str, Any]) -> tuple[str, str] | None:
    provider_symbol = str(payload.get("provider_symbol") or payload.get("symbol") or "").strip().upper()
    timeframe = str(payload.get("timeframe") or "").strip().upper()
    if not provider_symbol or not timeframe:
        return None
    return provider_symbol, timeframe


def _active_zone_from_payload(payload: Mapping[str, Any]) -> _ActiveZone | None:
    output_id = str(payload.get("output_id") or payload.get("payload_id") or payload.get("id") or "").strip()
    side = _normalized_side(payload.get("refill_side") or payload.get("side"))
    zone_low = _payload_decimal(payload, "zone_low", "reference_zone_low", "footprint_bin_low")
    zone_high = _payload_decimal(payload, "zone_high", "reference_zone_high", "footprint_bin_high")
    if not output_id or side not in {"BID", "ASK"} or zone_low is None or zone_high is None:
        return None
    return _ActiveZone(
        output_id=output_id,
        side=side,
        zone_low=min(zone_low, zone_high),
        zone_high=max(zone_low, zone_high),
    )


def _same_active_zone(left: _ActiveZone, right: _ActiveZone) -> bool:
    return (
        left.side == right.side
        and left.zone_low == right.zone_low
        and left.zone_high == right.zone_high
    )


def _zone_below(left: _ActiveZone, right: _ActiveZone) -> bool:
    return left.zone_high < right.zone_low


def _zone_above(left: _ActiveZone, right: _ActiveZone) -> bool:
    return left.zone_low > right.zone_high


def _terminal_zones_from_footprint_candle(
    candle: Mapping[str, Any],
) -> tuple[_ActiveZone, ...]:
    terminals: list[_ActiveZone] = []
    buy_values = _footprint_side_contract_values(candle, "BUY")
    sell_values = _footprint_side_contract_values(candle, "SELL")
    for raw_bin in candle.get("bins", ()) or ():
        if not isinstance(raw_bin, Mapping):
            continue
        low = _payload_decimal(raw_bin, "low", "bin_low", "price_low")
        high = _payload_decimal(raw_bin, "high", "bin_high", "price_high")
        if low is None:
            continue
        if high is None:
            high = low
        market_buy = _footprint_bin_side_contracts(raw_bin, "BUY")
        market_sell = _footprint_bin_side_contracts(raw_bin, "SELL")
        if (
            market_sell == 0
            and market_buy > 0
            and _footprint_terminal_diagonal_pass(candle, raw_bin, side="ASK")
            and _z_score(market_buy, buy_values) >= TERMINAL_Z_SCORE_MIN
        ):
            terminals.append(
                _ActiveZone(
                    output_id=_terminal_zone_id(candle, low, high, "ASK"),
                    side="ASK",
                    zone_low=min(low, high),
                    zone_high=max(low, high),
                )
            )
            continue
        if (
            market_buy == 0
            and market_sell > 0
            and _footprint_terminal_diagonal_pass(candle, raw_bin, side="BID")
            and _z_score(market_sell, sell_values) >= TERMINAL_Z_SCORE_MIN
        ):
            terminals.append(
                _ActiveZone(
                    output_id=_terminal_zone_id(candle, low, high, "BID"),
                    side="BID",
                    zone_low=min(low, high),
                    zone_high=max(low, high),
                )
            )
    return tuple(terminals)


def _footprint_terminal_diagonal_pass(
    candle: Mapping[str, Any],
    raw_bin: Mapping[str, Any],
    *,
    side: str,
) -> bool:
    normalized = _normalized_side(side)
    market_buy = _footprint_bin_side_contracts(raw_bin, "BUY")
    market_sell = _footprint_bin_side_contracts(raw_bin, "SELL")
    if normalized == "ASK":
        denominator = max(
            1,
            _footprint_adjacent_side_contracts(
                candle,
                raw_bin,
                direction="BELOW",
                side="SELL",
            ),
        )
        return (
            market_sell == 0
            and market_buy > 0
            and market_buy >= TERMINAL_DIAGONAL_RATIO_MIN * denominator
        )
    if normalized == "BID":
        denominator = max(
            1,
            _footprint_adjacent_side_contracts(
                candle,
                raw_bin,
                direction="ABOVE",
                side="BUY",
            ),
        )
        return (
            market_buy == 0
            and market_sell > 0
            and market_sell >= TERMINAL_DIAGONAL_RATIO_MIN * denominator
        )
    return False


def _footprint_terminal_z_score(
    candle: Mapping[str, Any],
    raw_bin: Mapping[str, Any],
    *,
    side: str,
) -> Decimal:
    normalized = _normalized_side(side)
    if normalized == "ASK":
        return _z_score(
            _footprint_bin_side_contracts(raw_bin, "BUY"),
            _footprint_side_contract_values(candle, "BUY"),
        )
    if normalized == "BID":
        return _z_score(
            _footprint_bin_side_contracts(raw_bin, "SELL"),
            _footprint_side_contract_values(candle, "SELL"),
        )
    return Decimal("0")


def _footprint_side_contract_values(
    candle: Mapping[str, Any],
    side: str,
) -> tuple[int, ...]:
    return tuple(
        _footprint_bin_side_contracts(raw_bin, side)
        for raw_bin in candle.get("bins", ()) or ()
        if isinstance(raw_bin, Mapping)
    )


def _z_score(current: int | Decimal, values: Iterable[int | Decimal]) -> Decimal:
    decimal_values = tuple(Decimal(str(value)) for value in values)
    if not decimal_values:
        return Decimal("0")
    count = Decimal(len(decimal_values))
    mean = sum(decimal_values, Decimal("0")) / count
    variance = sum((value - mean) ** 2 for value in decimal_values) / count
    if variance <= 0:
        return Decimal("0")
    return (Decimal(str(current)) - mean) / variance.sqrt()


def _terminal_zone_id(
    candle: Mapping[str, Any],
    low: Decimal,
    high: Decimal,
    side: str,
) -> str:
    return "|".join(
        (
            "TERMINAL",
            str(_payload_int(candle, "open_time_ms", "open_time")),
            str(low),
            str(high),
            side,
        )
    )


def _refill_zones_from_payloads(payloads: Iterable[Mapping[str, Any]]) -> tuple[_RefillZone, ...]:
    levels = _refill_levels_from_payloads(payloads)
    if not levels:
        return tuple()
    return (
        *_buy_refill_zones(levels),
        *_sell_refill_zones(levels),
    )


def _refill_levels_from_payloads(payloads: Iterable[Mapping[str, Any]]) -> tuple[_RefillLevel, ...]:
    buckets: dict[tuple[Decimal, str], list[dict[str, Any]]] = {}
    for payload in payloads:
        price = _payload_decimal(payload, "price", "threshold_price", "level_price")
        side = _normalized_side(payload.get("side"))
        if (
            price is None
            or side not in {"BID", "ASK"}
            or payload.get("price_base_refill_count") in {None, ""}
            or payload.get("price_base_refill_contracts") in {None, ""}
        ):
            continue
        buckets.setdefault((price, side), []).append(dict(payload))
    levels = []
    for (price, side), items in buckets.items():
        lows = tuple(
            value
            for item in items
            for value in (
                _payload_decimal(item, "zone_low", "reference_zone_low", "footprint_bin_low", "bin_low", "price_low"),
            )
            if value is not None
        )
        highs = tuple(
            value
            for item in items
            for value in (
                _payload_decimal(item, "zone_high", "reference_zone_high", "footprint_bin_high", "bin_high", "price_high"),
            )
            if value is not None
        )
        zone_low = min((*lows, price))
        zone_high = max((*highs, price))
        levels.append(
            _RefillLevel(
                price=price,
                side=side,
                zone_low=zone_low,
                zone_high=zone_high,
                refill_count=sum(_payload_int(item, "price_base_refill_count") for item in items),
                refill_contracts=sum(
                    _payload_int(item, "price_base_refill_contracts")
                    for item in items
                ),
                executed_refill_contracts=sum(
                    _payload_int(item, "executed_refill_contracts")
                    for item in items
                ),
                withdrawn_refill_contracts=sum(
                    _payload_int(item, "withdrawn_refill_contracts")
                    for item in items
                ),
                market_buy=max(_payload_int(item, "market_buy", "market_buy_contracts", "ask_traded_contracts") for item in items),
                market_sell=max(_payload_int(item, "market_sell", "market_sell_contracts", "bid_traded_contracts") for item in items),
                payloads=tuple(items),
            )
        )
    return tuple(sorted(levels, key=lambda item: (item.price, item.side)))


def _buy_refill_zones(levels: tuple[_RefillLevel, ...]) -> tuple[_RefillZone, ...]:
    candidates: list[_RefillZone] = []
    sequence: list[_RefillLevel] = []
    for level in sorted(levels, key=lambda item: item.price, reverse=True):
        if level.side == "ASK":
            if _opposite_level_resets_sequence(level):
                sequence = []
            continue
        if level.side != "BID" or level.market_buy >= level.market_sell:
            sequence = []
            continue
        if sequence and level.market_buy > sequence[-1].market_buy:
            sequence = [level]
        else:
            sequence.append(level)
        if level.market_buy != 0:
            continue
        candidate = _buy_refill_zone(tuple(sequence), levels)
        if candidate is not None:
            candidates.append(candidate)
        sequence = []
    return tuple(candidates)


def _sell_refill_zones(levels: tuple[_RefillLevel, ...]) -> tuple[_RefillZone, ...]:
    candidates: list[_RefillZone] = []
    sequence: list[_RefillLevel] = []
    for level in sorted(levels, key=lambda item: item.price):
        if level.side == "BID":
            if _opposite_level_resets_sequence(level):
                sequence = []
            continue
        if level.side != "ASK" or level.market_sell >= level.market_buy:
            sequence = []
            continue
        if sequence and level.market_sell > sequence[-1].market_sell:
            sequence = [level]
        else:
            sequence.append(level)
        if level.market_sell != 0:
            continue
        candidate = _sell_refill_zone(tuple(sequence), levels)
        if candidate is not None:
            candidates.append(candidate)
        sequence = []
    return tuple(candidates)


def _opposite_level_resets_sequence(level: _RefillLevel) -> bool:
    if int(level.refill_count) > 0 or int(level.refill_contracts) > 0:
        return True
    if level.side == "ASK":
        return int(level.market_buy) > OPPOSITE_SEQUENCE_RESET_CONTRACTS_MIN
    if level.side == "BID":
        return int(level.market_sell) > OPPOSITE_SEQUENCE_RESET_CONTRACTS_MIN
    return False


def _buy_refill_zone(
    sequence: tuple[_RefillLevel, ...],
    all_levels: tuple[_RefillLevel, ...],
) -> _RefillZone | None:
    if not sequence:
        return None
    terminal = sequence[-1]
    if (
        terminal.side != "BID"
        or terminal.market_buy != 0
        or terminal.market_sell <= 0
        or not _terminal_level_passes_diagonal_ratio(terminal)
        or not _terminal_level_is_spike(terminal)
    ):
        return None
    del all_levels
    refill_count = sum(level.refill_count for level in sequence)
    refill_contracts = sum(level.refill_contracts for level in sequence)
    executed_refill_contracts = min(
        refill_contracts,
        sum(level.executed_refill_contracts for level in sequence),
    )
    withdrawn_refill_contracts = sum(level.withdrawn_refill_contracts for level in sequence)
    if refill_count < REFILL_ZONE_COUNT_MIN:
        return None
    if refill_contracts < REFILL_ZONE_CONTRACTS_MIN:
        return None
    market_buy = sum(level.market_buy for level in sequence)
    market_sell = sum(level.market_sell for level in sequence)
    single_level = len(sequence) == 1
    zone_low = min(level.zone_low for level in sequence)
    zone_high = max(level.zone_high for level in sequence)
    return _RefillZone(
        direction="LONG",
        reference_side="SELL",
        refill_side="BID",
        levels=sequence,
        terminal_level=terminal,
        zone_low=zone_low,
        zone_high=zone_high,
        refill_count=refill_count,
        refill_contracts=refill_contracts,
        executed_refill_contracts=executed_refill_contracts,
        withdrawn_refill_contracts=withdrawn_refill_contracts,
        market_buy=market_buy,
        market_sell=market_sell,
        single_level=single_level,
    )


def _sell_refill_zone(
    sequence: tuple[_RefillLevel, ...],
    all_levels: tuple[_RefillLevel, ...],
) -> _RefillZone | None:
    if not sequence:
        return None
    terminal = sequence[-1]
    if (
        terminal.side != "ASK"
        or terminal.market_sell != 0
        or terminal.market_buy <= 0
        or not _terminal_level_passes_diagonal_ratio(terminal)
        or not _terminal_level_is_spike(terminal)
    ):
        return None
    del all_levels
    refill_count = sum(level.refill_count for level in sequence)
    refill_contracts = sum(level.refill_contracts for level in sequence)
    executed_refill_contracts = min(
        refill_contracts,
        sum(level.executed_refill_contracts for level in sequence),
    )
    withdrawn_refill_contracts = sum(level.withdrawn_refill_contracts for level in sequence)
    if refill_count < REFILL_ZONE_COUNT_MIN:
        return None
    if refill_contracts < REFILL_ZONE_CONTRACTS_MIN:
        return None
    market_buy = sum(level.market_buy for level in sequence)
    market_sell = sum(level.market_sell for level in sequence)
    single_level = len(sequence) == 1
    zone_low = min(level.zone_low for level in sequence)
    zone_high = max(level.zone_high for level in sequence)
    return _RefillZone(
        direction="SHORT",
        reference_side="BUY",
        refill_side="ASK",
        levels=sequence,
        terminal_level=terminal,
        zone_low=zone_low,
        zone_high=zone_high,
        refill_count=refill_count,
        refill_contracts=refill_contracts,
        executed_refill_contracts=executed_refill_contracts,
        withdrawn_refill_contracts=withdrawn_refill_contracts,
        market_buy=market_buy,
        market_sell=market_sell,
        single_level=single_level,
    )


def _payload_from_refill_zone(zone: _RefillZone) -> dict[str, Any]:
    terminal_payload = dict(zone.terminal_level.representative)
    level_payloads = [dict(level.representative) for level in zone.levels]
    all_level_payloads = [
        dict(item)
        for level in zone.levels
        for item in level.payloads
    ]
    latest_order_payloads = _latest_refill_payloads_by_order(all_level_payloads)
    timestamp_ms = max(_payload_int(item, "timestamp_ms", "threshold_time_ms") for item in level_payloads)
    close_time_ms = max(_payload_int(item, "close_time_ms") for item in level_payloads)
    filled_contracts = sum(
        _payload_int(
            item,
            "refill_filled_contracts",
            "positive_refill_filled_total",
            "executed_contracts",
        )
        for item in latest_order_payloads
    )
    opened_at_ms_values = [_payload_int(item, "opened_at_ms") for item in level_payloads if _payload_int(item, "opened_at_ms") > 0]
    order_ids = tuple(
        dict.fromkeys(
            str(item.get("order_id") or item.get("venue_order_id") or "")
            for item in all_level_payloads
            if str(item.get("order_id") or item.get("venue_order_id") or "").strip()
        )
    )
    output_id = "|".join(
        (
            DATA_PROCESS_ENGINE_PRODUCER.upper(),
            DATA_PROCESS_REFILL_OUTPUT_TYPE,
            str(terminal_payload.get("provider_symbol") or "").upper(),
            str(terminal_payload.get("timeframe") or "").upper(),
            str(_payload_int(terminal_payload, "footprint_open_time_ms")),
            str(timestamp_ms),
            str(zone.refill_count),
            str(zone.zone_low),
            str(zone.zone_high),
            zone.refill_side,
            "ZONE",
        )
    )
    terminal_market_buy = int(zone.terminal_level.market_buy)
    terminal_market_sell = int(zone.terminal_level.market_sell)
    terminal_aggressive_contracts = max(terminal_market_buy, terminal_market_sell)
    terminal_diagonal_numerator = _payload_int(
        terminal_payload,
        "terminal_diagonal_numerator_contracts",
        "footprint_diagonal_numerator_contracts",
    )
    terminal_diagonal_denominator = max(
        1,
        _payload_int(
            terminal_payload,
            "terminal_diagonal_denominator_contracts",
            "footprint_diagonal_denominator_contracts",
        ),
    )
    terminal_diagonal_ratio = _payload_decimal(
        terminal_payload,
        "terminal_diagonal_ratio",
        "footprint_diagonal_ratio",
    )
    if terminal_diagonal_ratio is None:
        terminal_diagonal_ratio = Decimal(terminal_diagonal_numerator) / Decimal(terminal_diagonal_denominator)
    terminal_diagonal_ratio_pass = _payload_bool(
        terminal_payload,
        "terminal_diagonal_ratio_pass",
        "footprint_diagonal_ratio_pass",
    )
    terminal_aggressive_z_score = _payload_decimal(
        terminal_payload,
        "terminal_aggressive_z_score",
        "footprint_aggressive_z_score",
    )
    if terminal_aggressive_z_score is None:
        terminal_aggressive_z_score = Decimal("0")
    marker_time_ms = _payload_int(terminal_payload, "footprint_open_time_ms") or timestamp_ms
    marker_price = str(terminal_payload.get("footprint_bin_low") or zone.terminal_level.price)
    executed_refill_contracts = min(
        int(zone.executed_refill_contracts),
        int(zone.refill_contracts),
    )
    refill_execution_rate = round(
        (
            executed_refill_contracts / int(zone.refill_contracts) * 100.0
            if int(zone.refill_contracts) > 0
            else 0.0
        ),
        1,
    )
    rate_label = f"{refill_execution_rate:.1f}".rstrip("0").rstrip(".")
    payload = {
        **terminal_payload,
        "payload_id": output_id,
        "id": output_id,
        "output_id": output_id,
        "type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
        "payload_type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
        "output_type": DATA_PROCESS_REFILL_OUTPUT_TYPE,
        "action": DATA_PROCESS_ENTRY_ACTION,
        "timestamp_ms": timestamp_ms,
        "event_time_ms": timestamp_ms,
        "marker_time_ms": marker_time_ms,
        "marker_price": marker_price,
        "threshold_time_ms": timestamp_ms,
        "close_time_ms": close_time_ms,
        "side": zone.refill_side,
        "price": str(zone.terminal_level.price),
        "order_id": str(terminal_payload.get("order_id") or output_id),
        "venue_order_id": str(terminal_payload.get("venue_order_id") or terminal_payload.get("order_id") or output_id),
        "zone_low": str(zone.zone_low),
        "zone_high": str(zone.zone_high),
        "reference_zone_low": str(zone.zone_low),
        "reference_zone_high": str(zone.zone_high),
        "entry_direction": zone.direction,
        "reference_side": zone.reference_side,
        "refill_side": zone.refill_side,
        "zone_level_count": len(zone.levels),
        "single_level_zone": bool(zone.single_level),
        "stop_loss": str(zone.zone_low if zone.direction == "LONG" else zone.zone_high),
        "refill_count": int(zone.refill_count),
        "refill_contracts": int(zone.refill_contracts),
        "price_base_refill_count": int(zone.refill_count),
        "price_base_refill_contracts": int(zone.refill_contracts),
        "refill_added_contracts": int(zone.refill_contracts),
        "executed_refill_contracts": executed_refill_contracts,
        "withdrawn_refill_contracts": int(zone.withdrawn_refill_contracts),
        "refill_execution_rate": refill_execution_rate,
        "refill_display": (
            f"{int(zone.refill_count)}({int(zone.refill_contracts)}) "
            f"E{executed_refill_contracts} - {rate_label}%"
        ),
        "refill_method": "price_base_refill",
        "refill_filled_contracts": int(filled_contracts),
        "positive_refill_count": int(zone.refill_count),
        "positive_refill_total": int(zone.refill_contracts),
        "positive_refill_filled_total": int(filled_contracts),
        "refill_total": int(zone.refill_contracts),
        "market_buy": int(zone.market_buy),
        "market_sell": int(zone.market_sell),
        "market_buy_contracts": int(zone.market_buy),
        "market_sell_contracts": int(zone.market_sell),
        "ask_traded_contracts": int(zone.market_buy),
        "bid_traded_contracts": int(zone.market_sell),
        "terminal_market_buy": terminal_market_buy,
        "terminal_market_sell": terminal_market_sell,
        "terminal_market_buy_contracts": terminal_market_buy,
        "terminal_market_sell_contracts": terminal_market_sell,
        "terminal_aggressive_contracts": terminal_aggressive_contracts,
        "terminal_aggressive_z_score": str(terminal_aggressive_z_score),
        "terminal_diagonal_numerator_contracts": terminal_diagonal_numerator,
        "terminal_diagonal_denominator_contracts": terminal_diagonal_denominator,
        "terminal_diagonal_ratio": str(terminal_diagonal_ratio),
        "terminal_diagonal_ratio_pass": terminal_diagonal_ratio_pass,
        "terminal_one_sided": bool(
            (terminal_market_buy > 0 and terminal_market_sell == 0)
            or (terminal_market_sell > 0 and terminal_market_buy == 0)
        ),
        "zone_market_buy": int(zone.market_buy),
        "zone_market_sell": int(zone.market_sell),
        "zone_buy_contracts": int(zone.market_buy),
        "zone_sell_contracts": int(zone.market_sell),
        "zone_order_ids": order_ids,
        "zone_levels": [
            {
                "price": str(level.price),
                "side": level.side,
                "refill_count": int(level.refill_count),
                "refill_contracts": int(level.refill_contracts),
                "refill_added_contracts": int(level.refill_contracts),
                "executed_refill_contracts": min(
                    int(level.executed_refill_contracts),
                    int(level.refill_contracts),
                ),
                "withdrawn_refill_contracts": int(level.withdrawn_refill_contracts),
                "refill_filled_contracts": sum(
                    _payload_int(
                        item,
                        "refill_filled_contracts",
                        "positive_refill_filled_total",
                        "executed_contracts",
                    )
                    for item in _latest_refill_payloads_by_order(level.payloads)
                ),
                "market_buy": int(level.market_buy),
                "market_sell": int(level.market_sell),
            }
            for level in zone.levels
        ],
        "trade_count": sum(_payload_int(item, "trade_count") for item in latest_order_payloads),
        "executed_contracts": sum(_payload_int(item, "executed_contracts") for item in latest_order_payloads),
        "opened_at_ms": min(opened_at_ms_values) if opened_at_ms_values else _payload_int(terminal_payload, "opened_at_ms"),
        "updated_at_ms": max(_payload_int(item, "updated_at_ms") for item in level_payloads),
        "source": "DATA_PROCESS_REFILL_ZONE",
    }
    payload["actor_proxy_payload"] = _actor_proxy_payload_from_orders(
        payload,
        latest_order_payloads,
        source="DATA_PROCESS_REFILL_ZONE",
    )
    return payload


def _latest_refill_payloads_by_order(
    payloads: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], ...]:
    latest: dict[str, dict[str, Any]] = {}
    for index, raw_payload in enumerate(payloads):
        payload = dict(raw_payload)
        order_id = str(payload.get("order_id") or payload.get("venue_order_id") or "").strip()
        key = order_id or str(payload.get("payload_id") or payload.get("id") or index)
        existing = latest.get(key)
        if existing is None or _payload_int(
            payload,
            "updated_at_ms",
            "timestamp_ms",
            "event_time_ms",
        ) >= _payload_int(
            existing,
            "updated_at_ms",
            "timestamp_ms",
            "event_time_ms",
        ):
            latest[key] = payload
    return tuple(latest.values())


def _payload_from_terminal_zone_cancel(
    symbol: ProcessSymbol,
    *,
    event: DomRawEvent,
    candle: Mapping[str, Any],
    terminal: _ActiveZone,
    canceled_zone_ids: tuple[str, ...],
    active_zone_count: int,
) -> dict[str, Any]:
    timestamp_ms = int(event.ts_event_ms)
    footprint_open_time_ms = _payload_int(candle, "open_time_ms", "open_time")
    close_time_ms = _payload_int(candle, "close_time_ms", "close_time") or timestamp_ms
    price = terminal.zone_low
    raw_bin = _footprint_bin_for_price(candle, price)
    market_buy = _footprint_bin_side_contracts(raw_bin or {}, "BUY")
    market_sell = _footprint_bin_side_contracts(raw_bin or {}, "SELL")
    terminal_aggressive_contracts = market_buy if terminal.side == "ASK" else market_sell
    terminal_aggressive_z_score = (
        _footprint_terminal_z_score(candle, raw_bin, side=terminal.side)
        if raw_bin is not None
        else Decimal("0")
    )
    output_id = "|".join(
        (
            DATA_PROCESS_ENGINE_PRODUCER.upper(),
            DATA_PROCESS_TERMINAL_CANCEL_OUTPUT_TYPE,
            symbol.provider_symbol.upper(),
            symbol.timeframe.upper(),
            str(footprint_open_time_ms),
            str(timestamp_ms),
            terminal.side,
            str(terminal.zone_low),
            str(terminal.zone_high),
        )
    )
    return {
        "payload_id": output_id,
        "id": output_id,
        "output_id": output_id,
        "producer": DATA_PROCESS_ENGINE_PRODUCER,
        "source_engine": "dataProcessEngine",
        "type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
        "payload_type": DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
        "output_type": DATA_PROCESS_TERMINAL_CANCEL_OUTPUT_TYPE,
        "action": "CANCEL",
        "timestamp_ms": timestamp_ms,
        "event_time_ms": timestamp_ms,
        "marker_time_ms": footprint_open_time_ms or timestamp_ms,
        "marker_price": str(price),
        "threshold_time_ms": timestamp_ms,
        "close_time_ms": close_time_ms,
        "symbol": symbol.provider_symbol,
        "provider_symbol": symbol.provider_symbol,
        "mt5_symbol": symbol.mt5_symbol,
        "market_provider": symbol.market_provider,
        "timeframe": symbol.timeframe,
        "price": str(price),
        "side": terminal.side,
        "order_id": output_id,
        "venue_order_id": output_id,
        "refill_count": 0,
        "refill_contracts": 0,
        "refill_filled_contracts": 0,
        "positive_refill_count": 0,
        "positive_refill_total": 0,
        "positive_refill_filled_total": 0,
        "refill_total": 0,
        "market_buy": market_buy,
        "market_sell": market_sell,
        "market_buy_contracts": market_buy,
        "market_sell_contracts": market_sell,
        "ask_traded_contracts": market_buy,
        "bid_traded_contracts": market_sell,
        "footprint_aggressive_contracts": terminal_aggressive_contracts,
        "footprint_aggressive_z_score": str(terminal_aggressive_z_score),
        "footprint_open_time_ms": footprint_open_time_ms,
        "footprint_bin_low": str(terminal.zone_low),
        "footprint_bin_high": str(terminal.zone_high),
        "zone_low": str(terminal.zone_low),
        "zone_high": str(terminal.zone_high),
        "reference_zone_low": str(terminal.zone_low),
        "reference_zone_high": str(terminal.zone_high),
        "zone_level_count": 1,
        "single_level_zone": True,
        "zone_status": "CANCELED_BY_TERMINAL",
        "canceled_zone_ids": canceled_zone_ids,
        "active_zone_count": int(active_zone_count),
        "terminal_market_buy": market_buy,
        "terminal_market_sell": market_sell,
        "terminal_market_buy_contracts": market_buy,
        "terminal_market_sell_contracts": market_sell,
        "terminal_aggressive_contracts": terminal_aggressive_contracts,
        "terminal_aggressive_z_score": str(terminal_aggressive_z_score),
        "terminal_diagonal_ratio_pass": True,
        "source": DATA_PROCESS_TERMINAL_CANCEL_OUTPUT_TYPE,
    }


def _terminal_level_passes_diagonal_ratio(level: _RefillLevel) -> bool:
    payload = level.representative
    return _payload_bool(
        payload,
        "terminal_diagonal_ratio_pass",
        "footprint_diagonal_ratio_pass",
    )


def _terminal_level_is_spike(level: _RefillLevel) -> bool:
    z_score = _payload_decimal(
        level.representative,
        "terminal_aggressive_z_score",
        "footprint_aggressive_z_score",
    )
    return z_score is not None and z_score >= TERMINAL_Z_SCORE_MIN


def _payload_with_actor_proxy_replay_window(
    payload: dict[str, Any],
    *,
    start_ms: int,
    end_ms: int,
) -> dict[str, Any]:
    actor_payload = payload.get("actor_proxy_payload")
    if not isinstance(actor_payload, Mapping):
        return payload
    updated = dict(payload)
    updated_actor_payload = dict(actor_payload)
    updated_actor_payload.update(
        {
            "tracking_mode": "REPLAY",
            "mode": "REPLAY_STUDY",
            "tracking_start_ms": int(start_ms),
            "tracking_end_ms": int(end_ms),
            "replay_start_ms": int(start_ms),
            "replay_end_ms": int(end_ms),
            "replay_window_policy": "EXACT_USER_REQUESTED_RANGE",
        }
    )
    updated["actor_proxy_payload"] = updated_actor_payload
    return updated


def _actor_proxy_payload_from_orders(
    base_payload: Mapping[str, Any],
    order_payloads: Iterable[Mapping[str, Any]],
    *,
    source: str,
) -> dict[str, Any]:
    orders: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for raw_order in order_payloads:
        order = _actor_proxy_order_from_payload(raw_order, base_payload)
        if order is None:
            continue
        key = (
            str(order.get("order_id") or ""),
            str(order.get("price") or ""),
            str(order.get("side") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        orders.append(order)
    if not orders:
        fallback_order = _actor_proxy_order_from_payload(base_payload, base_payload)
        if fallback_order is not None:
            orders.append(fallback_order)
    orders.sort(
        key=lambda item: (
            int(item.get("refill_contracts") or 0),
            int(item.get("refill_count") or 0),
            int(item.get("initial_size") or 0),
            int(item.get("executed_contracts") or 0),
        ),
        reverse=True,
    )
    payload_id = str(
        base_payload.get("payload_id")
        or base_payload.get("output_id")
        or base_payload.get("id")
        or ""
    )
    candle_open_time_ms = _payload_int(
        base_payload,
        "candle_open_time_ms",
        "footprint_open_time_ms",
        "marker_time_ms",
    )
    return {
        "schema_version": "actor_proxy_payload.v1",
        "payload_id": payload_id,
        "source_payload_id": payload_id,
        "source_engine": "dataProcessEngine",
        "source": source,
        "mode": "REPLAY_STUDY",
        "raw_data_only": True,
        "replay_window_policy": "EXACT_USER_REQUESTED_RANGE",
        "symbol": str(
            base_payload.get("mt5_symbol")
            or base_payload.get("symbol")
            or base_payload.get("provider_symbol")
            or ""
        ),
        "provider_symbol": str(
            base_payload.get("provider_symbol")
            or base_payload.get("symbol")
            or ""
        ),
        "instrument_id": _payload_int(base_payload, "instrument_id"),
        "timeframe": str(base_payload.get("timeframe") or ""),
        "candle_id": str(base_payload.get("candle_id") or candle_open_time_ms or ""),
        "candle_open_time_ms": candle_open_time_ms,
        "orders": orders,
    }


def _actor_proxy_order_from_payload(
    payload: Mapping[str, Any],
    base_payload: Mapping[str, Any],
) -> dict[str, Any] | None:
    price = _payload_decimal(payload, "price", "level_price", "marker_price")
    if price is None:
        price = _payload_decimal(base_payload, "price", "level_price", "marker_price")
    if price is None:
        return None
    order_id = str(
        payload.get("order_id")
        or payload.get("venue_order_id")
        or payload.get("top_order_id")
        or ""
    ).strip()
    side = _normalized_side(payload.get("side") or payload.get("top_order_side") or base_payload.get("side"))
    if not order_id or side not in {"BID", "ASK"}:
        return None
    current_size_keys = (
        "current_size",
        "remaining_size",
        "current_order_size",
        "top_order_current_contracts",
    )
    has_current_size = any(payload.get(key) not in {None, ""} for key in current_size_keys)
    current_size = (
        _payload_int(payload, *current_size_keys)
        if has_current_size
        else _payload_int(payload, "max_order_size", "initial_order_size", "initial_size", "top_order_size")
    )
    initial_size = max(
        _payload_int(payload, "initial_size", "initial_order_size"),
        _payload_int(payload, "max_order_size", "top_order_size"),
        current_size,
    )
    refill_count = _payload_int(payload, "price_base_refill_count")
    refill_contracts = _payload_int(
        payload,
        "price_base_refill_contracts",
    )
    executed_contracts = _payload_int(
        payload,
        "executed_contracts",
        "refill_filled_contracts",
        "positive_refill_filled_total",
    )
    if initial_size <= 0:
        initial_size = max(current_size, refill_contracts, executed_contracts)
    first_seen_ts_event_ms = _payload_int(
        payload,
        "first_seen_ts_event_ms",
        "opened_at_ms",
        "timestamp_ms",
        "threshold_time_ms",
    )
    last_seen_ts_event_ms = _payload_int(
        payload,
        "last_seen_ts_event_ms",
        "updated_at_ms",
        "close_time_ms",
        "timestamp_ms",
    )
    candle_open_time_ms = _payload_int(
        base_payload,
        "candle_open_time_ms",
        "footprint_open_time_ms",
        "marker_time_ms",
    )
    large_size_threshold = _payload_int(base_payload, "large_order_size_threshold")
    high_refill = refill_count > 0 or refill_contracts > 0
    large_size = large_size_threshold > 0 and initial_size >= large_size_threshold
    if high_refill and large_size:
        reason = "BOTH"
    elif large_size:
        reason = "LARGE_SIZE"
    else:
        reason = "HIGH_REFILL"
    raw_event_refs = [dict(item) for item in _iter_mappings(payload.get("raw_event_refs"))]
    if not raw_event_refs:
        raw_event_refs = [
            {
                "order_id": order_id,
                "first_seen_ts_event_ms": first_seen_ts_event_ms,
                "last_seen_ts_event_ms": last_seen_ts_event_ms,
                "source": str(payload.get("source_file") or payload.get("source") or ""),
            }
        ]
    return {
        "symbol": str(
            payload.get("mt5_symbol")
            or payload.get("symbol")
            or base_payload.get("mt5_symbol")
            or base_payload.get("symbol")
            or base_payload.get("provider_symbol")
            or ""
        ),
        "provider_symbol": str(
            payload.get("provider_symbol")
            or base_payload.get("provider_symbol")
            or payload.get("symbol")
            or ""
        ),
        "instrument_id": _payload_int(payload, "instrument_id") or _payload_int(base_payload, "instrument_id"),
        "order_id": order_id,
        "side": side,
        "price": str(price),
        "initial_size": int(initial_size),
        "current_size": int(current_size),
        "remaining_size": int(current_size),
        "refill_count": int(refill_count),
        "refill_contracts": int(refill_contracts),
        "price_base_refill_count": int(refill_count),
        "price_base_refill_contracts": int(refill_contracts),
        "refill_method": "price_base_refill",
        "executed_contracts": int(executed_contracts),
        "trade_count": _payload_int(payload, "trade_count"),
        "first_seen_ts_event_ms": int(first_seen_ts_event_ms),
        "last_seen_ts_event_ms": int(last_seen_ts_event_ms),
        "source_file": str(payload.get("source_file") or ""),
        "source_id": str(payload.get("source_id") or payload.get("source") or ""),
        "candle_id": str(base_payload.get("candle_id") or candle_open_time_ms or ""),
        "candle_open_time_ms": int(candle_open_time_ms),
        "reason": reason,
        "raw_event_refs": raw_event_refs,
    }


def _payload_decimal(payload: Mapping[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        raw_value = payload.get(key)
        if raw_value in {None, ""}:
            continue
        try:
            return Decimal(str(raw_value))
        except Exception:
            continue
    return None


def _payload_int(payload: Mapping[str, Any], *keys: str) -> int:
    for key in keys:
        raw_value = payload.get(key)
        if raw_value in {None, ""}:
            continue
        try:
            return int(Decimal(str(raw_value)))
        except Exception:
            continue
    return 0


def _payload_bool(payload: Mapping[str, Any], *keys: str) -> bool:
    for key in keys:
        raw_value = payload.get(key)
        if raw_value in {None, ""}:
            continue
        if isinstance(raw_value, bool):
            return raw_value
        if isinstance(raw_value, (int, float, Decimal)):
            return bool(raw_value)
        normalized = str(raw_value).strip().lower()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return False


def _iter_mappings(value: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(value, Mapping):
        yield value
        return
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        for item in value:
            if isinstance(item, Mapping):
                yield item


def _empty_footprint_metrics() -> dict[str, Any]:
    return {
        "market_buy": 0,
        "market_sell": 0,
        "market_buy_contracts": 0,
        "market_sell_contracts": 0,
        "ask_traded_contracts": 0,
        "bid_traded_contracts": 0,
        "footprint_aggressive_contracts": 0,
        "footprint_diagonal_numerator_contracts": 0,
        "footprint_diagonal_denominator_contracts": 1,
        "footprint_diagonal_ratio": "0",
        "footprint_diagonal_ratio_pass": False,
        "footprint_open_time_ms": 0,
        "footprint_bin_low": "",
        "footprint_bin_high": "",
    }
