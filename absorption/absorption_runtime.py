from __future__ import annotations
import csv
import logging
import math
import time
from dataclasses import dataclass, replace
<<<<<<< HEAD
from decimal import Decimal, ROUND_HALF_UP
=======
from decimal import Decimal
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from pathlib import Path
from typing import Any, Callable
from absorption_module.timeframe_alignment import update_rolling_buffer
from absorption.binance_kline_ws_client import (
    KlineClosedEvent,
    KLINE_INTERVAL_BY_INTERNAL,
)
from absorption.binance_aggtrade_ws_client import AggTradeEvent
from absorption_module.absorption_cluster_builder import (
    build_candle_absorption_results,
    build_empty_candle_absorption_result,
)
from absorption_module.absorption_cluster_model import (
    BinMarketData,
    TradeSide,
)
from absorption_module.absorption_memory import (
    AbsorptionMemoryState,
    increment_closed_candle_count,
)
from absorption_module.default_absorption_config import build_default_absorption_config
from core.bin_alignment import ExchangeMetadata, bin_bounds
<<<<<<< HEAD
from core.contract_spike import calculate_contract_spike_metrics, is_contract_spike
from core.feature_calculation import OutputPrecision
from core.performance_metrics import elapsed_ms, get_performance_metrics_recorder, perf_counter_ms
from core.system_models import SymbolSessionState
from core.timeframe_policy import STUDY_TIMEFRAMES, normalized_execution_timeframes
=======
from core.feature_calculation import OutputPrecision
from core.performance_metrics import elapsed_ms, get_performance_metrics_recorder, perf_counter_ms
from core.system_models import SymbolSessionState
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from core.trade_mapping import TradeEvent
from execution.trading_decision_engine import (
    EntryTrigger,
    ExitTrigger,
    OpenPositionState,
    TradingCommand,
    TradingDecisionEngine,
)
from execution.trading_zone_state import NEUTRAL_SIDE, ZoneState, ZoneStateStore
from study.candle_builder import OrderFlowCandleBuilder, OrderFlowStudyConfig
from study.study_snapshot import CandleRecord
LOGGER = logging.getLogger(__name__)
MT5_OUTPUT_TIMEFRAME_BY_INTERNAL: dict[str, str] = {
    "M1": "M1",
    "M5": "M5",
    "M15": "M15",
    "M30": "M30",
    "H1": "H1",
}



<<<<<<< HEAD
MT5_ABSORPTION_TIMEFRAME_ORDER = STUDY_TIMEFRAMES
MT5_ABSORPTION_TIMEFRAMES = frozenset(MT5_ABSORPTION_TIMEFRAME_ORDER)
FOOTPRINT_STUDY_TIMEFRAMES = STUDY_TIMEFRAMES

REFERENCE_RULE_LEFT_CANDLE_COUNT = 5
REFERENCE_RULE_MIN_CANDLE_WINDOW = REFERENCE_RULE_LEFT_CANDLE_COUNT + 2
REFERENCE_RULE_DOMINANT_PRESSURE_THRESHOLD = 100.0
REFERENCE_RULE_OPPOSITE_PRESSURE_THRESHOLD = 1.0
REFERENCE_RULE_EFFICIENCY_PERCENT_THRESHOLD = 0.1
EXECUTION_CLOSED_CANDLE_WINDOW = REFERENCE_RULE_MIN_CANDLE_WINDOW
=======
MT5_ABSORPTION_TIMEFRAME_ORDER = ("M1", "M5", "M15", "M30", "H1")
MT5_ABSORPTION_TIMEFRAMES = frozenset(MT5_ABSORPTION_TIMEFRAME_ORDER)
FOOTPRINT_STUDY_TIMEFRAMES = ("M1", "M5", "M15", "M30", "H1")

EXECUTION_CLOSED_CANDLE_WINDOW = 2
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
EXECUTION_COMMAND_LIFECYCLE_CSV_FIELDS = (
    "timestamp_utc_ms",
    "event_type",
    "client_name",
    "symbol_name",
    "timeframe",
    "command_type",
<<<<<<< HEAD
    "action",
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    "request_id",
    "position_id",
    "client_position_id",
    "client_position_identifier",
    "signal_time_utc_ms",
<<<<<<< HEAD
    "target_entry_open_time_utc_ms",
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    "trigger_bin_price",
    "entry_reason",
    "exit_reason",
    "status",
    "rejection_reason",
    "reason",
)

RETRYABLE_EXECUTION_REJECTION_REASONS = frozenset(
    {
        "ENTRY_PRICE_UNAVAILABLE",
        "MARGIN_SAFETY_FAILED",
        "ORDER_CHECK_FAILED",
        "ORDER_SEND_FAILED",
        "VOLUME_REDUCED_BELOW_MINIMUM",
        "MINIMUM_VOLUME_MARGIN_OR_ORDER_CHECK_FAILED",
    }
)


@dataclass
class EntryState:
    symbol: str
    timeframe: str
    side: str
    absorption_candle_time_utc_ms: int
    started_closed_candle_count: int
    expires_closed_candle_count: int
    stop_reference_price: Decimal


def _record_execution_command_lifecycle_event(
    event_type: str,
    command: TradingCommand,
    *,
    status: str = "",
    rejection_reason: str = "",
    reason: str = "",
) -> None:
    output_path = Path.cwd() / "runtime_metrics" / "execution_command_lifecycle_events.csv"
    row = {
        "timestamp_utc_ms": int(time.time() * 1000),
        "event_type": event_type,
        "client_name": command.client_name,
        "symbol_name": command.symbol_name,
        "timeframe": command.timeframe,
        "command_type": command.command_type,
<<<<<<< HEAD
        "action": command.action,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        "request_id": command.request_id,
        "position_id": command.position_id,
        "client_position_id": command.client_position_id,
        "client_position_identifier": command.client_position_identifier,
        "signal_time_utc_ms": command.signal_time,
<<<<<<< HEAD
        "target_entry_open_time_utc_ms": command.target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        "trigger_bin_price": (
            str(command.trigger_bin_price)
            if command.trigger_bin_price is not None
            else ""
        ),
        "entry_reason": command.entry_reason,
        "exit_reason": command.exit_reason,
        "status": status,
        "rejection_reason": rejection_reason,
        "reason": reason,
    }
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not output_path.exists() or output_path.stat().st_size <= 0
        existing_rows: list[dict[str, str]] = []
        rewrite_file = False
        if not write_header:
            with output_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                if tuple(reader.fieldnames or ()) != EXECUTION_COMMAND_LIFECYCLE_CSV_FIELDS:
                    existing_rows = [
                        {field: str(existing_row.get(field, "") or "") for field in EXECUTION_COMMAND_LIFECYCLE_CSV_FIELDS}
                        for existing_row in reader
                    ]
                    rewrite_file = True
        with output_path.open("w" if rewrite_file else "a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=EXECUTION_COMMAND_LIFECYCLE_CSV_FIELDS)
            if write_header or rewrite_file:
                writer.writeheader()
            if rewrite_file:
                writer.writerows(existing_rows)
            writer.writerow(row)
    except Exception:
        LOGGER.exception(
<<<<<<< HEAD
            "EXECUTION_COMMAND_LIFECYCLE_EVENT_WRITE_FAILED | event_type=%s | command_type=%s | action=%s | "
            "symbol=%s | timeframe=%s | position_id=%s",
            event_type,
            command.command_type,
            command.action,
=======
            "EXECUTION_COMMAND_LIFECYCLE_EVENT_WRITE_FAILED | event_type=%s | command_type=%s | "
            "symbol=%s | timeframe=%s | position_id=%s",
            event_type,
            command.command_type,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            command.symbol_name,
            command.timeframe,
            command.position_id,
        )


class LiveAbsorptionRuntime:
    
    def __init__(self) -> None:
        self.config = build_default_absorption_config()
        self.timeframe_specs = {item.name: item for item in self.config.enabled_timeframes}
<<<<<<< HEAD
        self.execution_timeframes = set(normalized_execution_timeframes())
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.memory = AbsorptionMemoryState()
        self._active_mt5_symbols: set[str] = set()
        self._mt5_symbols_by_binance: dict[str, set[str]] = {}
        self._active_internal_timeframes_by_symbol: dict[str, set[str]] = {}
        self._active_output_timeframes_by_symbol: dict[str, set[str]] = {}
        self._mt5_timeframe_by_builder_key: dict[tuple[str, str], str] = {}
        self._builders: dict[tuple[str, str], OrderFlowCandleBuilder] = {}
        self._last_processed_open_time: dict[tuple[str, str], int] = {}
        self._latest_price_by_symbol: dict[str, Decimal] = {}
        self._closed_kline_by_symbol_timeframe_open: dict[tuple[str, str, int], KlineClosedEvent] = {}
        self._closed_record_listeners: list[Callable[..., None]] = []
        self._execution_position_locks: dict[tuple[str, str, str], dict[str, Any]] = {}
        self.zone_state_store = ZoneStateStore()
        self.trading_decision_engine = TradingDecisionEngine(self.zone_state_store)
        self._entry_states_by_key: dict[tuple[str, str, str], EntryState] = {}
        self._entry_triggers_by_key: dict[tuple[str, str, str], EntryTrigger] = {}
        self._exit_triggers_by_key: dict[tuple[str, str, str], ExitTrigger] = {}
        self._known_execution_clients: set[str] = set()
        self._open_execution_positions: dict[tuple[str, str], OpenPositionState] = {}
        self._pending_execution_commands: dict[tuple[str, str, str, str, str, str, str], TradingCommand] = {}
        self._sent_execution_command_keys: set[tuple[str, str, str, str, str, str, str]] = set()
        self._completed_execution_command_keys: set[tuple[str, str, str, str, str, str, str]] = set()
        self._initialized_execution_evaluations: set[tuple[str, str, str]] = set()
        self.timeframe_specs = {
            spec.name: spec
            for spec in self.config.enabled_timeframes
        }

    def add_closed_record_listener(self, listener: Callable[..., None]) -> None:
        self._closed_record_listeners.append(listener)

    def _notify_closed_record_listeners(
        self,
        *,
        mt5_symbol: str,
        timeframe_name: str,
        fixed_bin_size: Decimal,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> None:
        for listener in tuple(self._closed_record_listeners):
            try:
                listener(
                    mt5_symbol=mt5_symbol,
                    timeframe_name=timeframe_name,
                    fixed_bin_size=fixed_bin_size,
                    record=record,
                    bin_items=bin_items,
                )
            except Exception:
                LOGGER.exception(
                    "ABSORPTION_CLOSED_RECORD_LISTENER_ERROR | symbol=%s | timeframe=%s | candle_open_time_utc_ms=%d",
                    mt5_symbol,
                    timeframe_name,
                    int(record.open_time_ms),
                )

    def _get_closed_kline_event(
        self,
        *,
        symbol: str,
        timeframe: str,
        candle_open_time_utc_ms: int,
    ) -> KlineClosedEvent | None:
        return self._closed_kline_by_symbol_timeframe_open.get(
            (
                symbol.upper(),
                timeframe.strip().upper(),
                int(candle_open_time_utc_ms),
            )
        )
    
    def on_kline_closed_event(self, event: KlineClosedEvent) -> None:
        self.process_closed_kline_batch(
            event=event,
            trade_events=(),
        )

    def process_closed_kline_batch(
        self,
        *,
        event: KlineClosedEvent,
        trade_events: tuple[AggTradeEvent, ...] | list[AggTradeEvent],
    ) -> None:
        metrics_start_ms = perf_counter_ms()
        mt5_symbols = self._record_closed_kline_event(event)
        close_time_ms = int(event.close_time_ms) + 1
        for mt5_symbol in mt5_symbols:
            builder = self._builders.get((mt5_symbol, event.internal_timeframe.strip().upper()))
            if builder is None:
                continue
            self._latest_price_by_symbol[mt5_symbol] = event.close_price
            closed_incremental = False
            if not trade_events:
                closed_incremental = builder.close_active_candle_for_kline(
                    open_time_ms=int(event.open_time_ms),
                    close_time_ms=close_time_ms,
                    open_price=event.open_price,
                    high_price=event.high_price,
                    low_price=event.low_price,
                    close_price=event.close_price,
                )
            if not closed_incremental:
                mt5_trades = tuple(
                    TradeEvent.from_values(
                        symbol=mt5_symbol,
                        event_time_ms=trade.event_time_ms,
                        price=trade.price,
                        quantity=trade.quantity,
                        side=trade.side,  # type: ignore[arg-type]
                    )
                    for trade in trade_events
                )
                builder.append_closed_candle_batch(
                    open_time_ms=int(event.open_time_ms),
                    close_time_ms=close_time_ms,
                    open_price=event.open_price,
                    high_price=event.high_price,
                    low_price=event.low_price,
                    close_price=event.close_price,
                    trades=mt5_trades,
                )
            self._process_new_closed_records(mt5_symbol, event.internal_timeframe.strip().upper(), builder)
        duration_ms = elapsed_ms(metrics_start_ms)
        recorder = get_performance_metrics_recorder()
        for mt5_symbol in mt5_symbols:
            recorder.record_kline_closed_event(
                symbol=mt5_symbol,
                timeframe=event.internal_timeframe,
                event_time_utc_ms=int(event.close_time_ms),
                duration_ms=duration_ms,
            )

    def _record_closed_kline_event(self, event: KlineClosedEvent) -> tuple[str, ...]:
        mt5_symbols = self._mt5_symbols_for_binance(event.symbol)
        keys = [
            (
                event.symbol.upper(),
                event.internal_timeframe.strip().upper(),
                int(event.open_time_ms),
            )
        ]

        for mt5_symbol in mt5_symbols:
            keys.append(
                (
                    mt5_symbol.upper(),
                    event.internal_timeframe.strip().upper(),
                    int(event.open_time_ms),
                )
            )

        for key in keys:
            self._closed_kline_by_symbol_timeframe_open[key] = event

        for symbol_key, timeframe_key, _open_time in keys:
            stale_keys = [
                item_key
                for item_key in self._closed_kline_by_symbol_timeframe_open
                if item_key[0] == symbol_key
                and item_key[1] == timeframe_key
            ]

            stale_keys.sort(
                key=lambda item: item[2],
                reverse=True,
            )

            for stale_key in stale_keys[200:]:
                self._closed_kline_by_symbol_timeframe_open.pop(stale_key, None)
        return mt5_symbols

    def configure_sessions(self, sessions: list[SymbolSessionState]) -> None:
        active_symbols = {
            session.mt5_symbol
            for session in sessions
            if session.symbol_resolved and session.session_ready
        }
        self._active_mt5_symbols = active_symbols
        self._mt5_symbols_by_binance = {}
        active_internal_timeframes: dict[str, set[str]] = {}
        active_output_timeframes: dict[str, set[str]] = {}
        output_timeframe_by_builder_key: dict[tuple[str, str], str] = {}
        for session in sessions:
            if not session.symbol_resolved or not session.binance_symbol or not session.session_ready:
                continue
            self._mt5_symbols_by_binance.setdefault(session.binance_symbol.upper(), set()).add(session.mt5_symbol)
            
            requested_timeframe = session.timeframe.strip().upper()
            if requested_timeframe not in KLINE_INTERVAL_BY_INTERNAL:
                LOGGER.warning(
                    "UNSUPPORTED_TIMEFRAME | mt5_symbol=%s | timeframe=%s",
                    session.mt5_symbol,
                    requested_timeframe,
                )
                continue
            builder_timeframes = (requested_timeframe,)
            for internal_timeframe in builder_timeframes:
                if internal_timeframe not in self.timeframe_specs:
                    continue
                if internal_timeframe not in KLINE_INTERVAL_BY_INTERNAL:
                    continue
                
                output_timeframe = MT5_OUTPUT_TIMEFRAME_BY_INTERNAL[internal_timeframe]

                active_internal_timeframes.setdefault(session.mt5_symbol, set()).add(internal_timeframe)

                output_timeframe_by_builder_key[(session.mt5_symbol, internal_timeframe)] = output_timeframe

            if (
<<<<<<< HEAD
                requested_timeframe in self.execution_timeframes
                and requested_timeframe in self.timeframe_specs
=======
                requested_timeframe in self.timeframe_specs
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                and requested_timeframe in KLINE_INTERVAL_BY_INTERNAL
                and requested_timeframe in MT5_OUTPUT_TIMEFRAME_BY_INTERNAL
            ):
                active_output_timeframes.setdefault(session.mt5_symbol, set()).add(
                    MT5_OUTPUT_TIMEFRAME_BY_INTERNAL[requested_timeframe]
                )

        self._active_internal_timeframes_by_symbol = active_internal_timeframes
        self._active_output_timeframes_by_symbol = active_output_timeframes
        self._mt5_timeframe_by_builder_key = output_timeframe_by_builder_key
        active_builder_keys = set(output_timeframe_by_builder_key)

        stale_builder_keys = [key for key in self._builders if key not in active_builder_keys]
        for key in stale_builder_keys:
            self._builders.pop(key, None)
            self._last_processed_open_time.pop(key, None)

        stale_buffer_keys = [key for key in self.memory.rolling_buffers if key not in active_builder_keys]
        for key in stale_buffer_keys:
            self.memory.rolling_buffers.pop(key, None)
        stale_count_keys = [key for key in self.memory.closed_candle_counts if key not in active_builder_keys]
        for key in stale_count_keys:
            self.memory.closed_candle_counts.pop(key, None)

        for symbol in list(self._latest_price_by_symbol):
            if symbol not in active_symbols:
                self._latest_price_by_symbol.pop(symbol, None)
        for key in list(self._entry_states_by_key):
            symbol_name, timeframe_name, _side = key
            if symbol_name not in active_symbols or timeframe_name not in active_output_timeframes.get(symbol_name, set()):
                self._entry_states_by_key.pop(key, None)
        for key in list(self._entry_triggers_by_key):
            symbol_name, timeframe_name, _side = key
            if symbol_name not in active_symbols or timeframe_name not in active_output_timeframes.get(symbol_name, set()):
                self._entry_triggers_by_key.pop(key, None)
        for key in list(self._exit_triggers_by_key):
            symbol_name, timeframe_name, _side = key
            if symbol_name not in active_symbols or timeframe_name not in active_output_timeframes.get(symbol_name, set()):
                self._exit_triggers_by_key.pop(key, None)
        for lock_key in list(self._execution_position_locks):
            _client_name, symbol_name, _side = lock_key
            if symbol_name not in active_symbols:
                self._execution_position_locks.pop(lock_key, None)
        for position_key, position in list(self._open_execution_positions.items()):
            if position.symbol not in active_symbols:
                self._open_execution_positions.pop(position_key, None)
        for command_key in list(self._pending_execution_commands):
            _client_name, symbol_name, _timeframe, _command_type, _request_id, _position_id, _client_position_id = command_key
            if symbol_name not in active_symbols:
                self._pending_execution_commands.pop(command_key, None)
                self._sent_execution_command_keys.discard(command_key)
                self._completed_execution_command_keys.discard(command_key)
        for command_key in list(self._sent_execution_command_keys):
            _client_name, symbol_name, _timeframe, _command_type, _request_id, _position_id, _client_position_id = command_key
            if symbol_name not in active_symbols:
                self._sent_execution_command_keys.discard(command_key)
        for command_key in list(self._completed_execution_command_keys):
            _client_name, symbol_name, _timeframe, _command_type, _request_id, _position_id, _client_position_id = command_key
            if symbol_name not in active_symbols:
                self._completed_execution_command_keys.discard(command_key)
        for evaluation_key in list(self._initialized_execution_evaluations):
            _client_name, symbol_name, _timeframe = evaluation_key
            if symbol_name not in active_symbols:
                self._initialized_execution_evaluations.discard(evaluation_key)
        for symbol in self.zone_state_store.symbols():
            if symbol and symbol not in active_symbols:
                self.zone_state_store.remove_symbol(symbol)

    def ensure_symbol_builders(
        self,
        *,
        mt5_symbol: str,
        fixed_bin_size_by_timeframe: dict[str, Decimal],
        tick_size: Decimal,
    ) -> None:
        if mt5_symbol not in self._active_mt5_symbols:
            return
        

        metadata = ExchangeMetadata.from_values(
            symbol=mt5_symbol,
            tick_size=tick_size,
            step_size=tick_size,
        )
        for timeframe_name in self._active_internal_timeframes(mt5_symbol):
            timeframe_spec = self.timeframe_specs[timeframe_name]
            fixed_bin_size = fixed_bin_size_by_timeframe.get(
                timeframe_name,
            )

            if fixed_bin_size is None or fixed_bin_size <= 0:
                continue
            key = (mt5_symbol, timeframe_name)
            if key in self._builders:
                continue
            self._builders[key] = OrderFlowCandleBuilder(
                OrderFlowStudyConfig(
                    symbol=mt5_symbol,
                    timeframe=timeframe_name,
                    timeframe_ms=timeframe_spec.duration_ms,
                    study_candle_count=EXECUTION_CLOSED_CANDLE_WINDOW,
                    fixed_bin_size=fixed_bin_size,
                    exchange_metadata=metadata,
                    output_precision=OutputPrecision(decimal_places=8, duration_unit_ms=1000),
                )
            )

    def has_symbol_builders(self, mt5_symbol: str) -> bool:
        return all((mt5_symbol, timeframe_name) in self._builders for timeframe_name in self._active_internal_timeframes(mt5_symbol))

    def on_trade_event(self, event: AggTradeEvent) -> None:
        metrics_start_ms = perf_counter_ms()
        touched_keys: set[tuple[str, str]] = set()
        for mt5_symbol in self._mt5_symbols_for_binance(event.symbol):
            self._latest_price_by_symbol[mt5_symbol] = event.price
            trade_event = TradeEvent.from_values(
                symbol=mt5_symbol,
                event_time_ms=event.event_time_ms,
                price=event.price,
                quantity=event.quantity,
                side=event.side,  # type: ignore[arg-type]
            )
            for timeframe_name, builder in self._active_builders(mt5_symbol):
                touched_keys.add((mt5_symbol, timeframe_name))
                builder.on_trade(trade_event)
        duration_ms = elapsed_ms(metrics_start_ms)
        recorder = get_performance_metrics_recorder()
        for mt5_symbol, timeframe_name in touched_keys:
            recorder.record_aggtrade_event(
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                duration_ms=duration_ms,
            )

    def execution_signal_payloads(
        self,
        mt5_symbol: str | None = None,
        client_name: str = "metatrader",
        primary_timeframe: str = "",
    ) -> list[dict[str, Any]]:
        return self.execution_command_payloads(
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
        requested_primary_timeframe = primary_timeframe.strip().upper()
        normalized_client = client_name.strip().lower() or "metatrader"
        self._known_execution_clients.add(normalized_client)

        symbols = [mt5_symbol] if mt5_symbol else sorted(self._active_mt5_symbols)
        payloads: list[dict[str, Any]] = []

        for symbol_name in symbols:
            if not symbol_name:
                continue
            active_timeframes = self._active_output_timeframes(symbol_name)
            normalized_primary_timeframe = requested_primary_timeframe or (active_timeframes[0] if active_timeframes else "")
<<<<<<< HEAD
            if not normalized_primary_timeframe or normalized_primary_timeframe not in active_timeframes:
=======
            if not normalized_primary_timeframe:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                continue
            self._ensure_execution_evaluation_initialized(
                client_name=normalized_client,
                symbol_name=symbol_name,
                primary_timeframe=normalized_primary_timeframe,
            )
            self._expire_or_invalidate_undispatched_execution_commands(
                client_name=normalized_client,
                symbol_name=symbol_name,
                primary_timeframe=normalized_primary_timeframe,
            )
            commands = self._undispatched_execution_commands(
                client_name=normalized_client,
                symbol_name=symbol_name,
                primary_timeframe=normalized_primary_timeframe,
            )
            for command in commands:
                command_key = self._execution_command_key(command)
                self._sent_execution_command_keys.add(command_key)
                _record_execution_command_lifecycle_event(
                    "EXECUTION_COMMAND_DISPATCHED",
                    command,
                )
            payloads.extend(command.to_payload() for command in commands)

        return payloads

    def update_execution_position_status(self, status_payload: dict[str, Any]) -> dict[str, Any]:
        client_name = str(status_payload.get("client_name") or "metatrader").strip().lower()
        symbol_name = str(status_payload.get("symbol_name") or status_payload.get("symbol") or "").strip()
        side = str(status_payload.get("side") or "").strip().upper()
        status = str(status_payload.get("status") or "").strip().upper()
        execution_request_id = str(
            status_payload.get("execution_request_id")
            or status_payload.get("trading_request_id")
            or status_payload.get("position_request_id")
            or ""
        ).strip()
        position_request_id = execution_request_id
        position_id = str(status_payload.get("position_id") or "").strip()
        cluster_id = str(status_payload.get("cluster_id") or execution_request_id or position_id).strip()
        timeframe = str(status_payload.get("timeframe") or "").strip().upper()
        signal_time = int(status_payload.get("signal_time") or 0)
        client_position_id = str(
            status_payload.get("client_position_id")
            or status_payload.get("ticket_number")
            or status_payload.get("ticket")
            or ""
        ).strip()
        client_position_identifier = str(
            status_payload.get("client_position_identifier")
            or status_payload.get("position_identifier")
            or status_payload.get("identifier")
            or ""
        ).strip()
        if not position_id and client_position_id:
            position_id = f"{client_name.upper()}:{client_position_id}"
        profit = self._decimal_from_payload(status_payload.get("profit"))
        entry_price = self._decimal_from_payload(status_payload.get("entry_price"))
        opened_at_utc_ms = int(status_payload.get("opened_at_utc_ms") or 0)
        rejection_reason = str(status_payload.get("rejection_reason") or "").strip()
        self._known_execution_clients.add(client_name)

        if not symbol_name or side not in {"BUY", "SELL"} or not (position_id or client_position_id or execution_request_id):
            return {
                "status": "ERROR",
                "code": "INVALID_POSITION_STATUS",
                "lock_updated": False,
            }

        if status in {"POSITION_OPENED", "POSITION_STILL_OPEN"}:
            existing_key = self._find_position_key(
                client_name=client_name,
                client_position_id=client_position_id,
                position_id=position_id,
                cluster_id=cluster_id,
            )
            previous_signature: tuple[object, ...] | None = None
            if existing_key is not None:
                existing_position = self._open_execution_positions[existing_key]
                previous_signature = self._position_evaluation_signature(existing_position)
                if not client_position_id:
                    client_position_id = existing_position.client_position_id
                if not client_position_identifier:
                    client_position_identifier = existing_position.client_position_identifier
                if status == "POSITION_STILL_OPEN" and rejection_reason and existing_position.request_id:
                    position_request_id = existing_position.request_id
                elif not position_request_id:
                    position_request_id = existing_position.request_id
                if signal_time <= 0:
                    signal_time = existing_position.opened_at_utc_ms
                if profit is None:
                    profit = existing_position.profit
                if entry_price is None:
                    entry_price = existing_position.entry_price
            if not client_position_id:
                client_position_id = position_id
            position = OpenPositionState(
                client_name=client_name,
                position_id=position_id,
                client_position_id=client_position_id,
                symbol=symbol_name,
                timeframe=timeframe,
                side=side,
                request_id=position_request_id,
                client_position_identifier=client_position_identifier,
                profit=profit,
                entry_price=entry_price,
                opened_at_utc_ms=opened_at_utc_ms if opened_at_utc_ms > 0 else signal_time,
            )
            position_key = self._position_store_key(client_name, client_position_id)
            if existing_key is not None and existing_key != position_key:
                self._open_execution_positions.pop(existing_key, None)
            self._open_execution_positions[position_key] = position
            self._complete_matching_execution_commands(
                client_name=client_name,
                symbol_name=symbol_name,
                timeframe=timeframe,
                request_id=execution_request_id,
                position_id=position_id,
                client_position_id=client_position_id,
                command_type="OPEN",
                status=status,
                rejection_reason=rejection_reason,
            )
            if status == "POSITION_STILL_OPEN" and rejection_reason:
                self._complete_matching_execution_commands(
                    client_name=client_name,
                    symbol_name=symbol_name,
                    timeframe=timeframe,
                    request_id=execution_request_id,
                    position_id=position_id,
                    client_position_id=client_position_id,
                    command_type="CLOSE",
                    status=status,
                    rejection_reason=rejection_reason,
                )
            self._clear_entry_context_for_open_position(
                symbol_name=symbol_name,
                timeframe=timeframe,
                side=side,
            )
            current_signature = self._position_evaluation_signature(position)
            if existing_key is None or previous_signature != current_signature:
                self._refresh_all_execution_commands_for_symbol(symbol_name)
            return {
                "status": "OK",
                "lock_updated": True,
                "lock_state": "OPEN",
            }

        if status in {"POSITION_CLOSED_BY_STOP_LOSS", "POSITION_CLOSED_BY_SIGNAL", "POSITION_REJECTED"}:
            retryable_rejection = (
                status == "POSITION_REJECTED"
                and self._is_retryable_execution_rejection(rejection_reason)
            )
            if status == "POSITION_CLOSED_BY_SIGNAL":
                close_command_matched = self._complete_matching_execution_commands(
                    client_name=client_name,
                    symbol_name=symbol_name,
                    timeframe=timeframe,
                    request_id=execution_request_id,
                    position_id=position_id,
                    client_position_id=client_position_id,
                    command_type="CLOSE",
                    status=status,
                    rejection_reason=rejection_reason,
                )
                if not close_command_matched:
                    LOGGER.warning(
                        "POSITION_CLOSED_BY_SIGNAL_IGNORED_WITHOUT_CLOSE_COMMAND | "
                        "client=%s | symbol=%s | timeframe=%s | side=%s | "
                        "position_id=%s | client_position_id=%s",
                        client_name,
                        symbol_name,
                        timeframe,
                        side,
                        position_id,
                        client_position_id,
                    )
                    return {
                        "status": "OK",
                        "lock_updated": False,
                        "lock_state": "IGNORED_UNMATCHED_SIGNAL_CLOSE",
                    }
            else:
                self._complete_matching_execution_commands(
                    client_name=client_name,
                    symbol_name=symbol_name,
                    timeframe=timeframe,
                    request_id=execution_request_id,
                    position_id=position_id,
                    client_position_id=client_position_id,
                    command_type="OPEN",
                    status=status,
                    rejection_reason=rejection_reason,
                    complete_command=not retryable_rejection,
                )
            if status == "POSITION_CLOSED_BY_STOP_LOSS":
                self._complete_matching_execution_commands(
                    client_name=client_name,
                    symbol_name=symbol_name,
                    timeframe=timeframe,
                    request_id=execution_request_id,
                    position_id=position_id,
                    client_position_id=client_position_id,
                    command_type="CLOSE",
                    status=status,
                    rejection_reason=rejection_reason,
                )
            existing_key = self._find_position_key(
                client_name=client_name,
                client_position_id=client_position_id,
                position_id=position_id,
                cluster_id=cluster_id,
            )
            if existing_key is None:
                if status == "POSITION_REJECTED":
                    self._refresh_all_execution_commands_for_symbol(symbol_name)
                return {
                    "status": "OK",
                    "lock_updated": False,
                    "lock_state": "NOT_FOUND",
                }
            if status != "POSITION_REJECTED":
                exit_time_utc_ms = self._position_close_time_utc_ms(signal_time)
                self._open_execution_positions.pop(existing_key, None)
                self._clear_entry_context_before_or_at(
                    symbol_name=symbol_name,
                    timeframe=timeframe,
                    cutoff_time_utc_ms=exit_time_utc_ms,
                )
                self._refresh_all_execution_commands_for_symbol(symbol_name)
                return {
                    "status": "OK",
                    "lock_updated": True,
                    "lock_state": "CLEARED",
                }

        return {
            "status": "OK",
            "lock_updated": False,
            "lock_state": "UNCHANGED",
        }

    def _refresh_all_execution_commands_for_symbol(self, symbol_name: str) -> None:
        for client_name in self._known_execution_clients:
            for timeframe in self._active_output_timeframes(symbol_name):
                self._refresh_execution_commands(
                    client_name=client_name,
                    symbol_name=symbol_name,
                    primary_timeframe=timeframe,
                )

    def _clear_entry_context_for_open_position(
        self,
        *,
        symbol_name: str,
        timeframe: str,
        side: str,
    ) -> None:
        normalized_symbol = symbol_name.strip()
        normalized_timeframe = timeframe.strip().upper()
        normalized_side = side.strip().upper()
        if not normalized_symbol or not normalized_timeframe or normalized_side not in {"BUY", "SELL"}:
            return

        context_key = (normalized_symbol, normalized_timeframe, normalized_side)
        self._entry_states_by_key.pop(context_key, None)
        self._entry_triggers_by_key.pop(context_key, None)
        self._remove_pending_open_commands(
            symbol_name=normalized_symbol,
            timeframe=normalized_timeframe,
            side=normalized_side,
        )
        self._refresh_trading_zone_state(
            normalized_symbol,
            normalized_timeframe,
            trigger_evaluation=False,
        )

    def _clear_entry_context_before_or_at(
        self,
        *,
        symbol_name: str,
        timeframe: str,
        cutoff_time_utc_ms: int,
    ) -> None:
        normalized_symbol = symbol_name.strip()
        normalized_timeframe = timeframe.strip().upper()
        if not normalized_symbol or not normalized_timeframe or cutoff_time_utc_ms <= 0:
            return

        for key, state in list(self._entry_states_by_key.items()):
            state_symbol, state_timeframe, _state_side = key
            if state_symbol != normalized_symbol or state_timeframe != normalized_timeframe:
                continue
            if state.absorption_candle_time_utc_ms <= cutoff_time_utc_ms:
                self._entry_states_by_key.pop(key, None)

        for key, trigger in list(self._entry_triggers_by_key.items()):
            trigger_symbol, trigger_timeframe, _trigger_side = key
            if trigger_symbol != normalized_symbol or trigger_timeframe != normalized_timeframe:
                continue
            if trigger.dominance_candle_time_utc_ms <= cutoff_time_utc_ms:
                self._entry_triggers_by_key.pop(key, None)

        self._remove_pending_open_commands(
            symbol_name=normalized_symbol,
            timeframe=normalized_timeframe,
            max_signal_time_utc_ms=cutoff_time_utc_ms,
        )
        self._refresh_trading_zone_state(
            normalized_symbol,
            normalized_timeframe,
            trigger_evaluation=False,
        )

    def _remove_pending_open_commands(
        self,
        *,
        symbol_name: str,
        timeframe: str,
        side: str | None = None,
        max_signal_time_utc_ms: int | None = None,
    ) -> None:
        normalized_symbol = symbol_name.strip()
        normalized_timeframe = timeframe.strip().upper()
        normalized_side = side.strip().upper() if side is not None else ""
        for command_key, command in list(self._pending_execution_commands.items()):
            if command.command_type.strip().upper() != "OPEN":
                continue
            if command.symbol_name.strip() != normalized_symbol:
                continue
            if command.timeframe.strip().upper() != normalized_timeframe:
                continue
            if normalized_side and command.side.strip().upper() != normalized_side:
                continue
            command_signal_time = (
                command.dominance_candle_time_utc_ms
                or command.signal_time
                or command.source_candle_close_time_utc_ms
            )
            if max_signal_time_utc_ms is not None and command_signal_time > max_signal_time_utc_ms:
                continue
            self._pending_execution_commands.pop(command_key, None)
            self._sent_execution_command_keys.discard(command_key)

    @staticmethod
    def _position_close_time_utc_ms(signal_time_utc_ms: int) -> int:
        if signal_time_utc_ms > 0:
            return signal_time_utc_ms
        return int(time.time() * 1000)

    def _ensure_execution_evaluation_initialized(
        self,
        *,
        client_name: str,
        symbol_name: str,
        primary_timeframe: str,
    ) -> None:
        evaluation_key = self._execution_evaluation_key(client_name, symbol_name, primary_timeframe)
        if evaluation_key in self._initialized_execution_evaluations:
            return
        self._initialized_execution_evaluations.add(evaluation_key)
        self._refresh_execution_commands(
            client_name=client_name,
            symbol_name=symbol_name,
            primary_timeframe=primary_timeframe,
        )

    def _refresh_execution_commands(
        self,
        *,
        client_name: str,
        symbol_name: str,
        primary_timeframe: str,
        trigger_source_timeframe: str | None = None,
        trigger_side: str | None = None,
    ) -> tuple[TradingCommand, ...]:
        evaluation_key = self._execution_evaluation_key(client_name, symbol_name, primary_timeframe)
        existing_sent_commands = tuple(
            command
            for command_key, command in self._pending_execution_commands.items()
            if command_key in self._sent_execution_command_keys
            and self._execution_evaluation_key(command.client_name, command.symbol_name, command.timeframe) == evaluation_key
        )
        if existing_sent_commands:
            return existing_sent_commands

        commands = self.trading_decision_engine.evaluate_symbol(
            client_name=client_name,
            symbol=symbol_name,
            entry_triggers=self._entry_triggers_for_symbol(symbol_name),
            open_positions=self._open_positions_for_client_symbol(client_name, symbol_name),
            exit_triggers=self._exit_triggers_for_symbol(symbol_name),
            primary_timeframe=primary_timeframe,
        )
        existing_unsent_commands = tuple(
            command
            for command_key, command in self._pending_execution_commands.items()
            if command_key not in self._sent_execution_command_keys
            and self._execution_evaluation_key(command.client_name, command.symbol_name, command.timeframe) == evaluation_key
        )
        if not commands:
            for command in existing_unsent_commands:
                _record_execution_command_lifecycle_event(
                    "EXECUTION_COMMAND_PRESERVED_PENDING",
                    command,
                    reason="REEVALUATION_RETURNED_NO_COMMAND",
                )

        for command in commands:
            command_key = self._execution_command_key(command)
            if command_key in self._completed_execution_command_keys:
                continue
            if command_key in self._pending_execution_commands:
                continue
            self._pending_execution_commands[command_key] = command
            _record_execution_command_lifecycle_event(
                "EXECUTION_COMMAND_QUEUED",
                command,
            )
        return commands

    def _undispatched_execution_commands(
        self,
        *,
        client_name: str,
        symbol_name: str,
        primary_timeframe: str,
    ) -> tuple[TradingCommand, ...]:
        evaluation_key = self._execution_evaluation_key(client_name, symbol_name, primary_timeframe)
        commands = [
            command
            for command_key, command in self._pending_execution_commands.items()
            if command_key not in self._sent_execution_command_keys
            and self._execution_evaluation_key(command.client_name, command.symbol_name, command.timeframe) == evaluation_key
        ]
        return tuple(commands)

    def _expire_or_invalidate_undispatched_execution_commands(
        self,
        *,
        client_name: str,
        symbol_name: str,
        primary_timeframe: str,
    ) -> None:
        del client_name, symbol_name, primary_timeframe

    @staticmethod
    def _execution_evaluation_key(
        client_name: str,
        symbol_name: str,
        primary_timeframe: str,
    ) -> tuple[str, str, str]:
        return (
            client_name.strip().lower() or "metatrader",
            symbol_name.strip(),
            primary_timeframe.strip().upper(),
        )

    @staticmethod
    def _execution_command_key(command: TradingCommand) -> tuple[str, str, str, str, str, str, str]:
        return (
            command.client_name.strip().lower() or "metatrader",
            command.symbol_name.strip(),
            command.timeframe.strip().upper(),
            command.command_type.strip().upper(),
            command.request_id.strip(),
            command.position_id.strip(),
            command.client_position_id.strip(),
        )

    def _complete_matching_execution_commands(
        self,
        *,
        client_name: str,
        symbol_name: str,
        timeframe: str,
        request_id: str,
        position_id: str,
        client_position_id: str,
        command_type: str,
        status: str = "",
        rejection_reason: str = "",
        complete_command: bool = True,
    ) -> bool:
        normalized_client = client_name.strip().lower() or "metatrader"
        normalized_symbol = symbol_name.strip()
        normalized_timeframe = timeframe.strip().upper()
        normalized_command_type = command_type.strip().upper()
        normalized_request_id = request_id.strip()
        matched = False
        for command_key, command in list(self._pending_execution_commands.items()):
            if command_key[0] != normalized_client:
                continue
            if command_key[1] != normalized_symbol or command_key[2] != normalized_timeframe:
                continue
            if command_key[3] != normalized_command_type:
                continue
            if normalized_request_id and command.request_id and command.request_id != normalized_request_id:
                continue
            if not normalized_request_id and position_id and command.position_id and command.position_id != position_id:
                continue
            if normalized_request_id and command.request_id == normalized_request_id:
                pass
            elif position_id and command.position_id and command.position_id != position_id:
                continue
            if client_position_id and command.client_position_id and command.client_position_id != client_position_id:
                continue
            matched = True
            self._sent_execution_command_keys.discard(command_key)
            if complete_command:
                self._pending_execution_commands.pop(command_key, None)
                self._completed_execution_command_keys.add(command_key)
                event_type = "EXECUTION_COMMAND_COMPLETED"
                event_reason = "MATCHING_STATUS_UPDATE"
            else:
                event_type = "EXECUTION_COMMAND_RETRY_PENDING"
                event_reason = "RETRYABLE_REJECTION_STATUS_UPDATE"
            _record_execution_command_lifecycle_event(
                event_type,
                command,
                status=status,
                rejection_reason=rejection_reason,
                reason=event_reason,
            )
        return matched

    @staticmethod
    def _is_retryable_execution_rejection(rejection_reason: str) -> bool:
        return rejection_reason.strip().upper() in RETRYABLE_EXECUTION_REJECTION_REASONS

    @staticmethod
    def _position_evaluation_signature(position: OpenPositionState) -> tuple[object, ...]:
        return (
            position.client_name,
            position.request_id,
            position.position_id,
            position.client_position_id,
            position.client_position_identifier,
            position.symbol,
            position.timeframe,
            position.side,
            position.profit is not None and position.profit > 0,
        )

    def _entry_triggers_for_symbol(self, symbol_name: str) -> tuple[EntryTrigger, ...]:
        return tuple(
            trigger
            for trigger in self._entry_triggers_by_key.values()
            if trigger.symbol == symbol_name
        )

    def _exit_triggers_for_symbol(self, symbol_name: str) -> tuple[ExitTrigger, ...]:
        return tuple(
            trigger
            for trigger in self._exit_triggers_by_key.values()
            if trigger.symbol == symbol_name
        )

    def _open_positions_for_client_symbol(
        self,
        client_name: str,
        symbol_name: str,
    ) -> tuple[OpenPositionState, ...]:
        return tuple(
            position
            for position in self._open_execution_positions.values()
            if position.client_name == client_name and position.symbol == symbol_name
        )

    @staticmethod
    def _position_store_key(client_name: str, client_position_id: str) -> tuple[str, str]:
        return (
            client_name.strip().lower() or "metatrader",
            client_position_id.strip(),
        )

    def _find_position_key(
        self,
        *,
        client_name: str,
        client_position_id: str,
        position_id: str,
        cluster_id: str,
    ) -> tuple[str, str] | None:
        normalized_client = client_name.strip().lower() or "metatrader"
        if client_position_id:
            key = self._position_store_key(normalized_client, client_position_id)
            if key in self._open_execution_positions:
                return key
        for key, position in self._open_execution_positions.items():
            if key[0] != normalized_client:
                continue
            if position.position_id in {position_id, cluster_id} or position.request_id in {position_id, cluster_id}:
                return key
        return None

    @staticmethod
    def _decimal_from_payload(value: object) -> Decimal | None:
        if value is None or value == "":
            return None
        try:
            return Decimal(str(value))
        except Exception:
            return None

    def _mt5_symbols_for_binance(self, binance_symbol: str) -> tuple[str, ...]:
        return tuple(self._mt5_symbols_by_binance.get(binance_symbol.upper(), set()))


    def _active_internal_timeframes(self, mt5_symbol: str) -> tuple[str, ...]:
        active_timeframes = self._active_internal_timeframes_by_symbol.get(mt5_symbol, set())
        return tuple(timeframe for timeframe in self.timeframe_specs if timeframe in active_timeframes)

    def _active_output_timeframes(self, mt5_symbol: str) -> tuple[str, ...]:
        active_timeframes = self._active_output_timeframes_by_symbol.get(mt5_symbol, set())
        return tuple(
            timeframe
            for timeframe in MT5_ABSORPTION_TIMEFRAME_ORDER
            if timeframe in active_timeframes
        )

    def _active_builders(self, mt5_symbol: str) -> tuple[tuple[str, OrderFlowCandleBuilder], ...]:
        builders: list[tuple[str, OrderFlowCandleBuilder]] = []
        for timeframe_name in self._active_internal_timeframes(mt5_symbol):
            builder = self._builders.get((mt5_symbol, timeframe_name))
            if builder is not None:
                builders.append((timeframe_name, builder))
        return tuple(builders)

    def pending_closed_record_count(self, mt5_symbol: str, timeframe_name: str) -> int | None:
        normalized_timeframe = timeframe_name.strip().upper()
        builder = self._builders.get((mt5_symbol, normalized_timeframe))
        if builder is None:
            return None
        last_open_time = self._last_processed_open_time.get((mt5_symbol, normalized_timeframe), -1)
        return sum(1 for record in builder.closed_candles if int(record.open_time_ms) > int(last_open_time))

    def _process_kline_closed_builders(self, event: KlineClosedEvent) -> None:
        timeframe_name = event.internal_timeframe.strip().upper()
        close_time_ms = int(event.close_time_ms) + 1
        for mt5_symbol in self._mt5_symbols_for_binance(event.symbol):
            builder = self._builders.get((mt5_symbol, timeframe_name))
            if builder is None:
                continue
            self._close_order_flow_builder_for_kline(
                builder=builder,
                open_time_ms=int(event.open_time_ms),
                close_time_ms=close_time_ms,
                open_price=event.open_price,
                high_price=event.high_price,
                low_price=event.low_price,
                close_price=event.close_price,
            )
            self._process_new_closed_records(mt5_symbol, timeframe_name, builder)

    @staticmethod
    def _close_order_flow_builder_for_kline(
        *,
        builder: OrderFlowCandleBuilder,
        open_time_ms: int,
        close_time_ms: int,
        open_price: Decimal | None,
        high_price: Decimal | None,
        low_price: Decimal | None,
        close_price: Decimal | None,
    ) -> bool:
        return builder.close_active_candle_for_kline(
            open_time_ms=open_time_ms,
            close_time_ms=close_time_ms,
            open_price=open_price,
            high_price=high_price,
            low_price=low_price,
            close_price=close_price,
        )

    def _record_performance_metric(
        self,
        *,
        start_ms: float,
        symbol: str,
        timeframe: str,
        component: str,
        closed_record_level_count: int | None = None,
    ) -> None:
        duration_ms = elapsed_ms(start_ms)
        recorder = get_performance_metrics_recorder()
        recorder.record(
            symbol=symbol,
            timeframe=timeframe,
            component=component,
            duration_ms=duration_ms,
            closed_record_level_count=closed_record_level_count,
        )

    @staticmethod
    def _record_closed_record_processed_metric(
        *,
        symbol: str,
        timeframe: str,
        close_time_ms: int,
    ) -> None:
        get_performance_metrics_recorder().record_closed_record_processed(
            symbol=symbol,
            timeframe=timeframe,
            event_time_utc_ms=int(close_time_ms),
        )

    @staticmethod
    def _closed_record_level_count(record: CandleRecord) -> int:
        return len(record.l2_bins)

    def _output_timeframe_for_builder(self, mt5_symbol: str, timeframe_name: str) -> str:
        return self._mt5_timeframe_by_builder_key.get(
            (mt5_symbol, timeframe_name),
            MT5_OUTPUT_TIMEFRAME_BY_INTERNAL[timeframe_name],
        )

    def _is_execution_timeframe(self, mt5_symbol: str, timeframe_name: str) -> bool:
        normalized_timeframe = timeframe_name.strip().upper()
        if normalized_timeframe not in MT5_OUTPUT_TIMEFRAME_BY_INTERNAL:
            return False
        output_timeframe = self._output_timeframe_for_builder(mt5_symbol, normalized_timeframe)
        return output_timeframe in self._active_output_timeframes(mt5_symbol)

    def _process_new_closed_records(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        builder: OrderFlowCandleBuilder,
    ) -> None:
        metrics_start_ms = perf_counter_ms()
        key = (mt5_symbol, timeframe_name)
        last_open_time = self._last_processed_open_time.get(key, -1)
        try:
            for record in sorted(builder.closed_candles, key=lambda item: item.open_time_ms):
                if record.open_time_ms <= last_open_time:
                    continue
                processed = self._process_closed_record(mt5_symbol, timeframe_name, builder, record)
                if not processed:
                    break
                self._last_processed_open_time[key] = record.open_time_ms
                last_open_time = record.open_time_ms
        finally:
            self._record_performance_metric(
                start_ms=metrics_start_ms,
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                component="_process_new_closed_records",
            )

    def _process_closed_record(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        builder: OrderFlowCandleBuilder,
        record: CandleRecord,
    ) -> bool:
        metrics_start_ms = perf_counter_ms()
        closed_record_level_count = self._closed_record_level_count(record)
        is_execution_timeframe = self._is_execution_timeframe(mt5_symbol, timeframe_name)
        if not record.closed:
            LOGGER.debug(
                "ABSORPTION_EVALUATION_DEFERRED | reason=record_not_closed | symbol=%s | timeframe=%s | candle_open_time_utc_ms=%d | candle_close_time_utc_ms=%d",
                mt5_symbol,
                timeframe_name,
                int(record.open_time_ms),
                int(record.close_time_ms),
            )
            self._record_performance_metric(
                start_ms=metrics_start_ms,
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                component="_process_closed_record",
                closed_record_level_count=closed_record_level_count,
            )
            return False

        bin_items = self._record_to_bin_market_data(
            mt5_symbol=mt5_symbol,
            timeframe_name=timeframe_name,
            fixed_bin_size=builder.config.fixed_bin_size,
            record=record,
        )
        bin_items = self._enrich_decision_bins(bin_items)

        if not bin_items:
            increment_closed_candle_count(self.memory, mt5_symbol, timeframe_name)
            if is_execution_timeframe:
                self._refresh_trading_zone_state(mt5_symbol, timeframe_name)
            self._record_closed_record_processed_metric(
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                close_time_ms=int(record.close_time_ms),
            )
            self._record_performance_metric(
                start_ms=metrics_start_ms,
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                component="_process_closed_record",
                closed_record_level_count=closed_record_level_count,
            )
            return True

        candle_open_time = min(item.candle_open_time_utc_ms for item in bin_items)
        candle_close_time = max(item.candle_close_time_utc_ms for item in bin_items)
        kline_event = self._get_closed_kline_event(
            symbol=mt5_symbol,
            timeframe=timeframe_name,
            candle_open_time_utc_ms=candle_open_time,
        )

        if kline_event is None:
            LOGGER.debug(
                "ABSORPTION_EVALUATION_DEFERRED | reason=missing_closed_kline | symbol=%s | timeframe=%s | candle_open_time_utc_ms=%d | candle_close_time_utc_ms=%d",
                mt5_symbol,
                timeframe_name,
                int(candle_open_time),
                int(candle_close_time),
            )
            self._record_performance_metric(
                start_ms=metrics_start_ms,
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                component="_process_closed_record",
                closed_record_level_count=closed_record_level_count,
            )
            return False

        if int(kline_event.open_time_ms) != int(candle_open_time):
            LOGGER.debug(
                "ABSORPTION_EVALUATION_DEFERRED | reason=closed_kline_open_mismatch | symbol=%s | timeframe=%s | candle_open_time_utc_ms=%d | kline_open_time_utc_ms=%d",
                mt5_symbol,
                timeframe_name,
                int(candle_open_time),
                int(kline_event.open_time_ms),
            )
            self._record_performance_metric(
                start_ms=metrics_start_ms,
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                component="_process_closed_record",
                closed_record_level_count=closed_record_level_count,
            )
            return False

        expected_kline_close_time = int(candle_close_time) - 1
        if int(kline_event.close_time_ms) != expected_kline_close_time:
            LOGGER.debug(
                "ABSORPTION_EVALUATION_DEFERRED | reason=closed_kline_close_mismatch | symbol=%s | timeframe=%s | expected_kline_close_time_utc_ms=%d | kline_close_time_utc_ms=%d",
                mt5_symbol,
                timeframe_name,
                expected_kline_close_time,
                int(kline_event.close_time_ms),
            )
            self._record_performance_metric(
                start_ms=metrics_start_ms,
                symbol=mt5_symbol,
                timeframe=timeframe_name,
                component="_process_closed_record",
                closed_record_level_count=closed_record_level_count,
            )
            return False

        current_candle_count = increment_closed_candle_count(self.memory, mt5_symbol, timeframe_name)
        valid_bin_items = self._volume_valid_bins(bin_items)
        candle_results = (
            build_candle_absorption_results(
                valid_bin_items,
                self.config,
                self._log_absorption_candidate_rejection,
            )
            if valid_bin_items
            else tuple()
        )
        candle_result = (
            max(
                candle_results,
                key=lambda item: (
                    item.score,
                    item.time_share,
                    item.buy_volume + item.sell_volume,
                ),
            )
            if candle_results
            else build_empty_candle_absorption_result(bin_items)
        )

        update_rolling_buffer(
            memory=self.memory,
            symbol=mt5_symbol,
            timeframe_name=timeframe_name,
            max_candles=self.config.rolling_candle_buffer_size,
            candle_result=candle_result,
            candle_bins=bin_items,
        )

        if is_execution_timeframe:
<<<<<<< HEAD
            self._evaluate_reference_candle_rule_triggers(
                mt5_symbol,
                timeframe_name,
                builder,
                record,
                tuple(bin_items),
            )
=======
            self._evaluate_profit_exit_triggers(
                mt5_symbol,
                timeframe_name,
                record,
                tuple(bin_items),
            )
            self._evaluate_entry_state_machine(
                mt5_symbol,
                timeframe_name,
                record,
                tuple(bin_items),
                current_candle_count,
            )
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            self._refresh_trading_zone_state(mt5_symbol, timeframe_name)
        self._record_closed_record_processed_metric(
            symbol=mt5_symbol,
            timeframe=timeframe_name,
            close_time_ms=int(record.close_time_ms),
        )
        self._notify_closed_record_listeners(
            mt5_symbol=mt5_symbol,
            timeframe_name=timeframe_name,
            fixed_bin_size=builder.config.fixed_bin_size,
            record=record,
            bin_items=tuple(bin_items),
        )
        self._record_performance_metric(
            start_ms=metrics_start_ms,
            symbol=mt5_symbol,
            timeframe=timeframe_name,
            component="_process_closed_record",
            closed_record_level_count=closed_record_level_count,
        )
        return True

      

            
    def _record_to_bin_market_data(
        self,
        *,
        mt5_symbol: str,
        timeframe_name: str,
        fixed_bin_size: Decimal,
        record: CandleRecord,
    ) -> tuple[BinMarketData, ...]:
        indices = sorted(record.l2_bins.keys())
        items: list[BinMarketData] = []
        for index in indices:
            l2_state = record.l2_bins.get(index)
            bin_low, bin_high = bin_bounds(index, fixed_bin_size)
            price_progress = self._bin_price_progress(record, l2_state)
            min_trade_price = getattr(l2_state, "min_trade_price_in_bin", None)
            max_trade_price = getattr(l2_state, "max_trade_price_in_bin", None)
            price_progress_in_bin = getattr(l2_state, "price_progress_in_bin", None)
            dominant_side_efficiency = getattr(l2_state, "dominant_side_efficiency", None)
            items.append(
                BinMarketData(
                    symbol=mt5_symbol,
                    timeframe_name=timeframe_name,
                    candle_open_time_utc_ms=int(record.open_time_ms),
                    candle_close_time_utc_ms=int(record.close_time_ms),
                    bin_index=int(index),
                    price_low=float(bin_low),
                    price_high=float(bin_high),
                    price_progress=float(price_progress),
                    total_volume=float(getattr(l2_state, "total_volume", Decimal("0"))),
                    delta_volume=float(getattr(l2_state, "delta", Decimal("0"))),
                    time_in_bin_ms=int(
                        record.durations_ms_by_index.get(
                            index,
                            int(getattr(l2_state, "duration_ms", 0)),
                        )
                    ),
                    horizontal_delta=float(
                        getattr(l2_state, "horizontal_delta", getattr(l2_state, "delta", Decimal("0")))
                    ),
                    ask_traded_volume=float(getattr(l2_state, "ask_traded_volume", Decimal("0"))),
                    bid_traded_volume=float(getattr(l2_state, "bid_traded_volume", Decimal("0"))),
                    buy_diagonal_imbalance_ratio=float(
                        getattr(l2_state, "buy_diagonal_imbalance_ratio", Decimal("0"))
                    ),
                    sell_diagonal_imbalance_ratio=float(
                        getattr(l2_state, "sell_diagonal_imbalance_ratio", Decimal("0"))
                    ),
                    min_trade_price_in_bin=(
                        float(min_trade_price)
                        if min_trade_price is not None
                        else None
                    ),
                    max_trade_price_in_bin=(
                        float(max_trade_price)
                        if max_trade_price is not None
                        else None
                    ),
                    price_progress_in_bin=(
                        float(price_progress_in_bin)
                        if price_progress_in_bin is not None
                        else None
                    ),
                    dominant_diagonal_side=str(
                        getattr(l2_state, "dominant_diagonal_side", "NONE") or "NONE"
                    ),
                    dominant_side_volume=float(
                        getattr(l2_state, "dominant_side_volume", Decimal("0"))
                        or Decimal("0")
                    ),
                    dominant_side_efficiency=(
                        float(dominant_side_efficiency)
                        if dominant_side_efficiency is not None
                        else None
                    ),
                )
            )
        return tuple(items)

    def _enrich_decision_bins(self, bin_items: tuple[BinMarketData, ...]) -> tuple[BinMarketData, ...]:
        if not bin_items:
            return tuple()

<<<<<<< HEAD
        contract_spike_metrics = calculate_contract_spike_metrics(
            item.total_volume for item in bin_items
        )
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        volume_ranks = self._percentile_ranks([max(float(item.total_volume), 0.0) for item in bin_items])
        enriched: list[BinMarketData] = []
        epsilon = float(self.config.entry_ratio_epsilon)
        min_volume_percentile = float(self.config.min_valid_bin_volume_percentile)
        min_opposite_ratio = float(self.config.dominant_side_min_opposite_volume_ratio)

<<<<<<< HEAD
        for item, volume_percentile, contract_spike_score in zip(
            bin_items,
            volume_ranks,
            contract_spike_metrics.scores,
        ):
=======
        for item, volume_percentile in zip(bin_items, volume_ranks):
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            total_volume = max(float(item.total_volume), 0.0)
            is_volume_valid = total_volume > epsilon and volume_percentile >= min_volume_percentile
            rejection_reason = ""
            dominant_side = "NONE"
            dominant_volume = 0.0
            price_progress = self._price_progress_from_trade_prices(item)
            efficiency: float | None = None

            if not is_volume_valid:
                rejection_reason = "LOW_VOLUME_BIN_REJECTED"
                LOGGER.debug(
                    "LOW_VOLUME_BIN_REJECTED | symbol=%s | timeframe=%s | bin_index=%d | "
                    "total_volume=%.12f | volume_percentile=%.12f | threshold=%.12f",
                    item.symbol,
                    item.timeframe_name,
                    int(item.bin_index),
                    total_volume,
                    volume_percentile,
                    min_volume_percentile,
                )
            else:
                buy_volume = max(float(item.ask_traded_volume), 0.0)
                sell_volume = max(float(item.bid_traded_volume), 0.0)
                buy_diagonal = float(item.buy_diagonal_imbalance_ratio)
                sell_diagonal = float(item.sell_diagonal_imbalance_ratio)
                if (
                    buy_volume > epsilon
                    and buy_diagonal > sell_diagonal
                    and buy_volume >= min_opposite_ratio * sell_volume
                ):
                    dominant_side = "BUY"
                    dominant_volume = buy_volume
                elif (
                    sell_volume > epsilon
                    and sell_diagonal > buy_diagonal
                    and sell_volume >= min_opposite_ratio * buy_volume
                ):
                    dominant_side = "SELL"
                    dominant_volume = sell_volume
                else:
                    rejection_reason = "INVALID_DOMINANT_SIDE"
                    LOGGER.debug(
                        "INVALID_DOMINANT_SIDE | symbol=%s | timeframe=%s | bin_index=%d | "
                        "buy_volume=%.12f | sell_volume=%.12f | buy_diagonal=%.12f | sell_diagonal=%.12f",
                        item.symbol,
                        item.timeframe_name,
                        int(item.bin_index),
                        buy_volume,
                        sell_volume,
                        buy_diagonal,
                        sell_diagonal,
                    )

                if dominant_side != "NONE":
<<<<<<< HEAD
                    if price_progress is None or total_volume <= epsilon:
                        rejection_reason = "INVALID_EFFICIENCY_INPUT"
                        LOGGER.debug(
                            "INVALID_EFFICIENCY_INPUT | symbol=%s | timeframe=%s | bin_index=%d | "
                            "dominant_side=%s | total_volume=%.12f | price_progress=%s",
=======
                    if price_progress is None or dominant_volume <= epsilon:
                        rejection_reason = "INVALID_EFFICIENCY_INPUT"
                        LOGGER.debug(
                            "INVALID_EFFICIENCY_INPUT | symbol=%s | timeframe=%s | bin_index=%d | "
                            "dominant_side=%s | dominant_volume=%.12f | price_progress=%s",
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                            item.symbol,
                            item.timeframe_name,
                            int(item.bin_index),
                            dominant_side,
<<<<<<< HEAD
                            total_volume,
                            price_progress,
                        )
                    else:
                        efficiency = price_progress / max(total_volume, epsilon)
=======
                            dominant_volume,
                            price_progress,
                        )
                    else:
                        efficiency = price_progress / max(dominant_volume, epsilon)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

            enriched.append(
                replace(
                    item,
                    price_progress_in_bin=price_progress,
                    dominant_diagonal_side=dominant_side,
                    dominant_side_volume=dominant_volume,
                    dominant_side_efficiency=efficiency,
<<<<<<< HEAD
                    contract_spike_score=float(contract_spike_score),
                    abnormal_contract=is_contract_spike(contract_spike_score),
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                    volume_percentile=volume_percentile,
                    is_volume_valid=is_volume_valid,
                    efficiency_percentile=None,
                    efficiency_zscore=None,
                    rejection_reason=rejection_reason,
                )
            )

        efficiency_values = [
            item.dominant_side_efficiency
            for item in enriched
            if item.is_volume_valid and item.dominant_side_efficiency is not None
        ]
        if efficiency_values:
            efficiency_ranks = self._percentile_ranks([float(value) for value in efficiency_values])
            rank_by_efficiency_position: dict[int, float] = {}
            efficiency_position = 0
            for index, item in enumerate(enriched):
                if item.is_volume_valid and item.dominant_side_efficiency is not None:
                    rank_by_efficiency_position[index] = efficiency_ranks[efficiency_position]
                    efficiency_position += 1
            enriched = [
                replace(item, efficiency_percentile=rank_by_efficiency_position.get(index))
                for index, item in enumerate(enriched)
            ]

        zscores_by_index: dict[int, float] = {}
        if len(efficiency_values) < int(self.config.minimum_required_bins_for_zscore):
            first = bin_items[0]
            LOGGER.debug(
                "INSUFFICIENT_VALID_BINS_FOR_ZSCORE | symbol=%s | timeframe=%s | "
                "valid_efficiency_bins_count=%d | required=%d",
                first.symbol,
                first.timeframe_name,
                len(efficiency_values),
                int(self.config.minimum_required_bins_for_zscore),
            )
        elif efficiency_values:
            mean = sum(float(value) for value in efficiency_values) / len(efficiency_values)
            variance = sum((float(value) - mean) ** 2 for value in efficiency_values) / len(efficiency_values)
            std = math.sqrt(variance)
            if std <= epsilon:
                first = bin_items[0]
                LOGGER.debug(
                    "EFFICIENCY_ZSCORE_UNAVAILABLE | symbol=%s | timeframe=%s | "
                    "valid_efficiency_bins_count=%d | efficiency_std=%.12f",
                    first.symbol,
                    first.timeframe_name,
                    len(efficiency_values),
                    std,
                )
            else:
                for index, item in enumerate(enriched):
                    if item.is_volume_valid and item.dominant_side_efficiency is not None:
                        zscores_by_index[index] = (float(item.dominant_side_efficiency) - mean) / std

        if zscores_by_index:
            enriched = [
                replace(item, efficiency_zscore=zscores_by_index.get(index))
                for index, item in enumerate(enriched)
            ]

        return tuple(enriched)

    @staticmethod
    def _percentile_ranks(values: list[float]) -> list[float]:
        if not values:
            return []
        values = [float(value) if math.isfinite(float(value)) else 0.0 for value in values]
        if len(values) == 1:
            return [1.0]
        sorted_values = sorted(float(value) for value in values)
        denominator = len(sorted_values) - 1
        ranks: list[float] = []
        for value in values:
            lower = next(index for index, item in enumerate(sorted_values) if item >= value)
            upper = len(sorted_values) - 1 - next(
                index for index, item in enumerate(reversed(sorted_values)) if item <= value
            )
            ranks.append(((lower + upper) / 2.0) / denominator)
        return ranks

    @staticmethod
    def _price_progress_from_trade_prices(item: BinMarketData) -> float | None:
        if item.min_trade_price_in_bin is None or item.max_trade_price_in_bin is None:
            return None
        progress = float(item.max_trade_price_in_bin) - float(item.min_trade_price_in_bin)
        if not math.isfinite(progress) or progress < 0.0:
            return None
        return progress

    @staticmethod
    def _volume_valid_bins(bin_items: tuple[BinMarketData, ...]) -> tuple[BinMarketData, ...]:
        return tuple(item for item in bin_items if item.is_volume_valid)

    def _ensure_enriched_decision_bins(self, bin_items: tuple[BinMarketData, ...]) -> tuple[BinMarketData, ...]:
        if not bin_items:
            return tuple()
        if any(item.volume_percentile is None for item in bin_items):
            return self._enrich_decision_bins(bin_items)
        return bin_items

<<<<<<< HEAD
    def _evaluate_reference_candle_rule_triggers(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        builder: OrderFlowCandleBuilder,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> None:
        if not bin_items:
            return

        records = self._closed_records_up_to_reference(builder, record)
        if len(records) < 2:
            return

        output_timeframe = self._output_timeframe_for_builder(mt5_symbol, timeframe_name)
        enriched_bins = self._ensure_enriched_decision_bins(bin_items)
        trigger_created = False

        for side in ("BUY", "SELL"):
            trigger = self._detect_reference_entry_trigger(
                mt5_symbol=mt5_symbol,
                output_timeframe=output_timeframe,
                side=side,
                records=records,
                bin_items=enriched_bins,
            )
            if trigger is None:
                continue
            self._entry_triggers_by_key[(trigger.symbol, trigger.timeframe, trigger.side)] = trigger
            self.zone_state_store.update(
                ZoneState(
                    symbol=trigger.symbol,
                    timeframe=trigger.timeframe,
                    zone_id=trigger.position_id,
                    side=trigger.side,
                    suggested_stop_loss=trigger.stop_reference_price,
                )
            )
            trigger_created = True
            LOGGER.debug(
                "REFERENCE_ENTRY_TRIGGER_DETECTED | symbol=%s | timeframe=%s | side=%s | "
                "action=%s | position_id=%s | target_entry_open_time_utc_ms=%s | "
                "stop_reference_price=%s | entry_reason=%s",
                trigger.symbol,
                trigger.timeframe,
                trigger.side,
                trigger.action,
                trigger.position_id,
                trigger.target_entry_open_time_utc_ms,
                trigger.stop_reference_price,
                trigger.entry_reason,
            )

        if self._evaluate_reference_exit_triggers(
            mt5_symbol=mt5_symbol,
            output_timeframe=output_timeframe,
            records=records,
            bin_items=enriched_bins,
        ):
            trigger_created = True

        if trigger_created:
            self._refresh_all_execution_commands_for_symbol(mt5_symbol)

    @staticmethod
    def _closed_records_up_to_reference(
        builder: OrderFlowCandleBuilder,
        reference_record: CandleRecord,
    ) -> tuple[CandleRecord, ...]:
        records = [
            item
            for item in sorted(builder.closed_candles, key=lambda value: int(value.open_time_ms))
            if int(item.open_time_ms) <= int(reference_record.open_time_ms)
        ]
        return tuple(records)

    def _detect_reference_entry_trigger(
        self,
        *,
        mt5_symbol: str,
        output_timeframe: str,
        side: str,
        records: tuple[CandleRecord, ...],
        bin_items: tuple[BinMarketData, ...],
    ) -> EntryTrigger | None:
        if len(records) < 2:
            return None

        reference = records[-1]
        previous = records[-2]
        if side == "BUY":
            if not self._is_bullish_candle(reference):
                return None
            if self._record_rounded_cumulative_delta(reference) <= 0:
                return None
            if not self._lowest_bin_sell_volume_is_zero(bin_items):
                return None
            trigger_bin = self._buy_imbalance_bin(bin_items)
            if trigger_bin is None:
                return None
            stop_reference = self._buy_entry_stop_reference(records)
            if stop_reference is None:
                return None
            return self._build_reference_entry_trigger(
                symbol=mt5_symbol,
                timeframe=output_timeframe,
                side="BUY",
                reference=reference,
                stop_reference=stop_reference,
                trigger_bin=trigger_bin,
                entry_reason="REFERENCE_RULE_ENTRY_BUY",
            )

        if side == "SELL":
            if not self._is_bearish_candle(reference):
                return None
            if self._record_rounded_cumulative_delta(reference) >= 0:
                return None
            if not self._highest_bin_buy_volume_is_zero(bin_items):
                return None
            trigger_bin = self._sell_imbalance_bin(bin_items)
            if trigger_bin is None:
                return None
            stop_reference = self._sell_entry_stop_reference(records)
            if stop_reference is None:
                return None
            return self._build_reference_entry_trigger(
                symbol=mt5_symbol,
                timeframe=output_timeframe,
                side="SELL",
                reference=reference,
                stop_reference=stop_reference,
                trigger_bin=trigger_bin,
                entry_reason="REFERENCE_RULE_ENTRY_SELL",
            )

        return None

    def _build_reference_entry_trigger(
        self,
        *,
        symbol: str,
        timeframe: str,
        side: str,
        reference: CandleRecord,
        stop_reference: Decimal,
        trigger_bin: BinMarketData,
        entry_reason: str,
    ) -> EntryTrigger:
        reference_time = int(reference.close_time_ms)
        return EntryTrigger(
            symbol=symbol,
            timeframe=timeframe,
            side=side,
            position_id=f"ENTRY-{symbol}-{timeframe}-{side}-{reference_time}-{reference_time}",
            signal_time=reference_time,
            stop_reference_price=stop_reference,
            absorption_candle_time_utc_ms=reference_time,
            dominance_candle_time_utc_ms=reference_time,
            trigger_bin_price=Decimal(str(self._bin_price(trigger_bin))),
            entry_reason=entry_reason,
            action=f"ENTRY_{side}",
            target_entry_open_time_utc_ms=reference_time,
        )

    def _evaluate_reference_exit_triggers(
        self,
        *,
        mt5_symbol: str,
        output_timeframe: str,
        records: tuple[CandleRecord, ...],
        bin_items: tuple[BinMarketData, ...],
    ) -> bool:
        if len(records) < 2:
            return False

        positions = [
            position
            for position in self._open_execution_positions.values()
            if position.symbol == mt5_symbol
            and position.timeframe == output_timeframe
            and position.side in {"BUY", "SELL"}
            and position.profit is not None
            and position.profit > 0
        ]
        if not positions:
            return False

        trigger_created = False
        for position in positions:
            trigger = self._detect_reference_exit_trigger(
                position=position,
                records=records,
                bin_items=bin_items,
            )
            if trigger is None:
                continue
            self._exit_triggers_by_key[(trigger.symbol, trigger.timeframe, trigger.side)] = trigger
            trigger_created = True
            LOGGER.debug(
                "REFERENCE_EXIT_TRIGGER_DETECTED | symbol=%s | timeframe=%s | side=%s | "
                "action=%s | position_id=%s | trigger_bin_price=%s | exit_reason=%s",
                trigger.symbol,
                trigger.timeframe,
                trigger.side,
                trigger.action,
                trigger.position_id,
                trigger.trigger_bin_price,
                trigger.exit_reason,
            )
        return trigger_created

    def _detect_reference_exit_trigger(
        self,
        *,
        position: OpenPositionState,
        records: tuple[CandleRecord, ...],
        bin_items: tuple[BinMarketData, ...],
    ) -> ExitTrigger | None:
        reference = records[-1]
        if position.side == "BUY":
            if not self._is_bearish_candle(reference):
                return None
            if self._record_rounded_cumulative_delta(reference) >= 0:
                return None
            if not self._highest_bin_buy_volume_is_zero(bin_items):
                return None
            trigger_bin = self._sell_imbalance_bin(bin_items)
            if trigger_bin is None or not self._buy_exit_previous_condition(records):
                return None
            exit_reason = "REFERENCE_RULE_EXIT_BUY_POSITION"
        elif position.side == "SELL":
            if not self._is_bullish_candle(reference):
                return None
            if self._record_rounded_cumulative_delta(reference) <= 0:
                return None
            if not self._lowest_bin_sell_volume_is_zero(bin_items):
                return None
            trigger_bin = self._buy_imbalance_bin(bin_items)
            if trigger_bin is None or not self._sell_exit_previous_condition(records):
                return None
            exit_reason = "REFERENCE_RULE_EXIT_SELL_POSITION"
        else:
            return None

        return ExitTrigger(
            symbol=position.symbol,
            timeframe=position.timeframe,
            side=position.side,
            position_id=position.position_id,
            signal_time=int(reference.close_time_ms),
            trigger_bin_price=Decimal(str(self._bin_price(trigger_bin))),
            exit_reason=exit_reason,
            action=f"EXIT_{position.side}_POSITION",
        )

    def _buy_entry_stop_reference(self, records: tuple[CandleRecord, ...]) -> Decimal | None:
        reference = records[-1]
        previous = records[-2]
        previous_delta = self._record_rounded_cumulative_delta(previous)
        reference_low = self._decimal_from_payload(reference.low_price)
        previous_low = self._decimal_from_payload(previous.low_price)
        if reference_low is None or previous_low is None:
            return None

        selected_index, selected_low = (
            (-2, previous_low)
            if previous_low <= reference_low
            else (-1, reference_low)
        )
        if not self._selected_low_breaks_left_lows(records, selected_index, selected_low):
            return None

        if self._is_bearish_candle(previous) and previous_delta < 0:
            return selected_low

        if previous_delta <= 0:
            return None
        return selected_low

    def _sell_entry_stop_reference(self, records: tuple[CandleRecord, ...]) -> Decimal | None:
        reference = records[-1]
        previous = records[-2]
        previous_delta = self._record_rounded_cumulative_delta(previous)
        reference_high = self._decimal_from_payload(reference.high_price)
        previous_high = self._decimal_from_payload(previous.high_price)
        if reference_high is None or previous_high is None:
            return None

        selected_index, selected_high = (
            (-2, previous_high)
            if previous_high >= reference_high
            else (-1, reference_high)
        )
        if not self._selected_high_breaks_left_highs(records, selected_index, selected_high):
            return None

        if self._is_bullish_candle(previous) and previous_delta > 0:
            return selected_high

        if previous_delta >= 0:
            return None
        return selected_high

    def _buy_exit_previous_condition(self, records: tuple[CandleRecord, ...]) -> bool:
        if len(records) < 2:
            return False
        reference = records[-1]
        previous = records[-2]
        previous_delta = self._record_rounded_cumulative_delta(previous)
        if self._is_bullish_candle(previous) and previous_delta > 0:
            return True
        if len(records) < 3 or previous_delta >= 0:
            return False
        previous_high = self._decimal_from_payload(previous.high_price)
        reference_high = self._decimal_from_payload(reference.high_price)
        two_before = records[-3]
        return (
            previous_high is not None
            and reference_high is not None
            and previous_high > reference_high
            and self._is_bullish_candle(two_before)
            and self._record_rounded_cumulative_delta(two_before) > 0
        )

    def _sell_exit_previous_condition(self, records: tuple[CandleRecord, ...]) -> bool:
        if len(records) < 2:
            return False
        reference = records[-1]
        previous = records[-2]
        previous_delta = self._record_rounded_cumulative_delta(previous)
        if self._is_bearish_candle(previous) and previous_delta < 0:
            return True
        if len(records) < 3 or previous_delta <= 0:
            return False
        previous_low = self._decimal_from_payload(previous.low_price)
        reference_low = self._decimal_from_payload(reference.low_price)
        two_before = records[-3]
        return (
            previous_low is not None
            and reference_low is not None
            and previous_low < reference_low
            and self._is_bearish_candle(two_before)
            and self._record_rounded_cumulative_delta(two_before) < 0
        )

    def _selected_low_breaks_left_lows(
        self,
        records: tuple[CandleRecord, ...],
        selected_index: int,
        selected_low: Decimal,
    ) -> bool:
        selected_absolute_index = len(records) + selected_index
        left_records = records[
            max(0, selected_absolute_index - REFERENCE_RULE_LEFT_CANDLE_COUNT) : selected_absolute_index
        ]
        if len(left_records) < REFERENCE_RULE_LEFT_CANDLE_COUNT:
            return False
        left_lows = [self._decimal_from_payload(item.low_price) for item in left_records]
        return all(item is not None and selected_low < item for item in left_lows)

    def _selected_high_breaks_left_highs(
        self,
        records: tuple[CandleRecord, ...],
        selected_index: int,
        selected_high: Decimal,
    ) -> bool:
        selected_absolute_index = len(records) + selected_index
        left_records = records[
            max(0, selected_absolute_index - REFERENCE_RULE_LEFT_CANDLE_COUNT) : selected_absolute_index
        ]
        if len(left_records) < REFERENCE_RULE_LEFT_CANDLE_COUNT:
            return False
        left_highs = [self._decimal_from_payload(item.high_price) for item in left_records]
        return all(item is not None and selected_high > item for item in left_highs)

    @classmethod
    def _lowest_bin_sell_volume_is_zero(cls, bin_items: tuple[BinMarketData, ...]) -> bool:
        if not bin_items:
            return False
        lowest = min(bin_items, key=lambda item: int(item.bin_index))
        return cls._rounded_ui_volume(lowest.bid_traded_volume) == Decimal("0")

    @classmethod
    def _highest_bin_buy_volume_is_zero(cls, bin_items: tuple[BinMarketData, ...]) -> bool:
        if not bin_items:
            return False
        highest = max(bin_items, key=lambda item: int(item.bin_index))
        return cls._rounded_ui_volume(highest.ask_traded_volume) == Decimal("0")

    @classmethod
    def _buy_imbalance_bin(cls, bin_items: tuple[BinMarketData, ...]) -> BinMarketData | None:
        eligible = [
            item
            for item in bin_items
            if float(item.buy_diagonal_imbalance_ratio) > REFERENCE_RULE_DOMINANT_PRESSURE_THRESHOLD
            and float(item.sell_diagonal_imbalance_ratio) < REFERENCE_RULE_OPPOSITE_PRESSURE_THRESHOLD
            and item.efficiency_percentile is not None
            and float(item.efficiency_percentile) > REFERENCE_RULE_EFFICIENCY_PERCENT_THRESHOLD
        ]
        if not eligible:
            return None
        return max(
            eligible,
            key=lambda item: (
                float(item.buy_diagonal_imbalance_ratio),
                float(item.efficiency_percentile or 0.0),
                int(item.bin_index),
            ),
        )

    @classmethod
    def _sell_imbalance_bin(cls, bin_items: tuple[BinMarketData, ...]) -> BinMarketData | None:
        eligible = [
            item
            for item in bin_items
            if float(item.sell_diagonal_imbalance_ratio) > REFERENCE_RULE_DOMINANT_PRESSURE_THRESHOLD
            and float(item.buy_diagonal_imbalance_ratio) < REFERENCE_RULE_OPPOSITE_PRESSURE_THRESHOLD
            and item.efficiency_percentile is not None
            and float(item.efficiency_percentile) > REFERENCE_RULE_EFFICIENCY_PERCENT_THRESHOLD
        ]
        if not eligible:
            return None
        return max(
            eligible,
            key=lambda item: (
                float(item.sell_diagonal_imbalance_ratio),
                float(item.efficiency_percentile or 0.0),
                int(item.bin_index),
            ),
        )

    @classmethod
    def _record_rounded_cumulative_delta(cls, record: CandleRecord) -> Decimal:
        return sum(
            (
                cls._rounded_ui_volume(getattr(state, "ask_traded_volume", Decimal("0")))
                - cls._rounded_ui_volume(getattr(state, "bid_traded_volume", Decimal("0")))
            )
            for state in record.l2_bins.values()
        )

    @staticmethod
    def _is_bullish_candle(record: CandleRecord) -> bool:
        return (
            record.open_price is not None
            and record.close_price is not None
            and record.close_price > record.open_price
        )

    @staticmethod
    def _is_bearish_candle(record: CandleRecord) -> bool:
        return (
            record.open_price is not None
            and record.close_price is not None
            and record.close_price < record.open_price
        )

    @staticmethod
    def _rounded_ui_volume(value: object) -> Decimal:
        if value is None:
            return Decimal("0")
        return Decimal(str(value)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def _evaluate_profit_exit_triggers(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> None:
        if not bin_items:
            return
        bin_items = self._ensure_enriched_decision_bins(bin_items)
        output_timeframe = self._output_timeframe_for_builder(mt5_symbol, timeframe_name)
        positions = [
            position
            for position in self._open_execution_positions.values()
            if position.symbol == mt5_symbol
            and position.timeframe == output_timeframe
            and position.side in {"BUY", "SELL"}
            and position.profit is not None
            and position.profit > 0
        ]
        if not positions:
            return
        trigger_created = False
        for position in positions:
            detected = self._detect_profit_exit_trigger(position, record, bin_items)
            if detected is None:
                continue
            trigger, item, efficiency, efficiency_percentile, efficiency_zscore = detected
            self._exit_triggers_by_key[(trigger.symbol, trigger.timeframe, trigger.side)] = trigger
            trigger_created = True
            LOGGER.debug(
                "PROFIT_EXIT_TRIGGER_DETECTED | symbol=%s | timeframe=%s | side=%s | "
                "position_id=%s | bin_index=%d | trigger_bin_price=%s | "
                "efficiency=%.12f | efficiency_percentile=%.12f | efficiency_zscore=%.12f | "
                "price_progress_in_bin=%s | dominant_diagonal_side=%s | "
                "dominant_side_volume=%s | exit_reason=%s",
                trigger.symbol,
                trigger.timeframe,
                trigger.side,
                trigger.position_id,
                int(item.bin_index),
                trigger.trigger_bin_price,
                float(efficiency),
                float(efficiency_percentile),
                float(efficiency_zscore),
                item.price_progress_in_bin,
                item.dominant_diagonal_side,
                item.dominant_side_volume,
                trigger.exit_reason,
            )
        if trigger_created:
            self._refresh_all_execution_commands_for_symbol(mt5_symbol)

    def _detect_profit_exit_trigger(
        self,
        position: OpenPositionState,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[ExitTrigger, BinMarketData, float, float, float] | None:
        open_price = self._decimal_from_payload(record.open_price)
        close_price = self._decimal_from_payload(record.close_price)
        if open_price is None or close_price is None:
            return None
        if position.side == "BUY" and close_price >= open_price:
            return None
        if position.side == "SELL" and close_price <= open_price:
            return None

        expected_dominant_side = "SELL" if position.side == "BUY" else "BUY"
<<<<<<< HEAD
        candidates = self._candidate_profit_exit_bins(position.side, record, self._volume_valid_bins(bin_items))
=======
        candidates = self._candidate_full_candle_bins(record, self._volume_valid_bins(bin_items))
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        eligible: list[tuple[float, float, float, BinMarketData]] = []
        for item in candidates:
            dominance = self._dominance_candidate(
                side=expected_dominant_side,
                item=item,
                efficiency_percentile_threshold=self.config.exit_dominance_efficiency_percentile,
                efficiency_zscore_threshold=self.config.exit_dominance_efficiency_zscore,
            )
            if dominance is None:
                continue
            efficiency, efficiency_percentile, efficiency_zscore = dominance
            eligible.append((efficiency_zscore, efficiency_percentile, efficiency, item))
        if not eligible:
            return None
        _zscore, _percentile, _efficiency, item = max(
            eligible,
            key=lambda value: (
                value[0],
                value[1],
                value[2],
                value[3].dominant_side_volume,
                value[3].bin_index,
            ),
        )
        exit_reason = (
            "PROFIT_EXIT_SELL_DOMINANCE_HIGH_EFFICIENCY_UPPER_WICK"
            if position.side == "BUY"
            else "PROFIT_EXIT_BUY_DOMINANCE_HIGH_EFFICIENCY_LOWER_WICK"
        )
        return (
            ExitTrigger(
                symbol=position.symbol,
                timeframe=position.timeframe,
                side=position.side,
                position_id=position.position_id,
                signal_time=int(record.close_time_ms),
                trigger_bin_price=Decimal(str(self._bin_price(item))),
                exit_reason=exit_reason,
            ),
            item,
            _efficiency,
            _percentile,
            _zscore,
        )

    def _evaluate_entry_state_machine(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
        current_closed_candle_count: int,
    ) -> None:
        if not bin_items:
            return
        bin_items = self._ensure_enriched_decision_bins(bin_items)
        for side in ("BUY", "SELL"):
            key = (mt5_symbol, timeframe_name, side)
            state = self._entry_states_by_key.get(key)
            if state is not None:
                self._update_entry_stop_tracking(state, record)
                if current_closed_candle_count >= state.expires_closed_candle_count:
                    self._entry_states_by_key.pop(key, None)
                    LOGGER.debug(
                        "ENTRY_STATE_TIMEOUT | symbol=%s | timeframe=%s | side=%s | absorption_candle_time_utc_ms=%d",
                        mt5_symbol,
                        timeframe_name,
                        side,
                        state.absorption_candle_time_utc_ms,
                    )
                    continue
                trigger = self._detect_dominance_confirmation(state, record, bin_items)
                if trigger is not None:
                    self._entry_states_by_key.pop(key, None)
                    self._entry_triggers_by_key[(trigger.symbol, trigger.timeframe, trigger.side)] = trigger
                    self.zone_state_store.update(
                        ZoneState(
                            symbol=trigger.symbol,
                            timeframe=trigger.timeframe,
                            zone_id=trigger.position_id,
                            side=trigger.side,
                            suggested_stop_loss=trigger.stop_reference_price,
                        )
                    )
                    LOGGER.debug(
                        "DOMINANCE_CONFIRMED | symbol=%s | timeframe=%s | side=%s | position_id=%s | entry_reason=%s",
                        trigger.symbol,
                        trigger.timeframe,
                        trigger.side,
                        trigger.position_id,
                        trigger.entry_reason,
                    )
                    self._refresh_all_execution_commands_for_symbol(mt5_symbol)

            new_state = self._detect_absorption_seed(
                mt5_symbol,
                timeframe_name,
                side,
                record,
                bin_items,
                current_closed_candle_count,
            )
            if new_state is None:
                continue
            previous_state = self._entry_states_by_key.get(key)
            self._entry_states_by_key[key] = new_state
            LOGGER.debug(
                "%s | symbol=%s | timeframe=%s | side=%s | absorption_candle_time_utc_ms=%d | stop_reference_price=%s",
                "ABSORPTION_STATE_OVERRIDDEN" if previous_state is not None else "ABSORPTION_SEEKING_STARTED",
                mt5_symbol,
                timeframe_name,
                side,
                new_state.absorption_candle_time_utc_ms,
                new_state.stop_reference_price,
            )

    def _detect_absorption_seed(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        side: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
        current_closed_candle_count: int,
    ) -> EntryState | None:
        open_price = self._decimal_from_payload(record.open_price)
        close_price = self._decimal_from_payload(record.close_price)
        high_price = self._decimal_from_payload(record.high_price)
        low_price = self._decimal_from_payload(record.low_price)
        if open_price is None or close_price is None or high_price is None or low_price is None:
            self._log_entry_state_rejection(mt5_symbol, timeframe_name, side, "INVALID_ENTRY_STATE_PRICE")
            return None

        valid_bins = self._volume_valid_bins(bin_items)
        volume_threshold = self._percentile(
            [item.total_volume for item in valid_bins],
            self.config.entry_absorption_volume_percentile,
        )
        if side == "BUY":
            diagonal_values = [item.sell_diagonal_imbalance_ratio for item in valid_bins]
            diagonal_median = self._median(diagonal_values)
<<<<<<< HEAD
            candidates = self._candidate_entry_zone_bins("BUY", record, valid_bins)
=======
            candidates = self._candidate_absorption_seed_bins("BUY", record, valid_bins)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            expected_dominant_side = "SELL"
            absorption_percentile = self.config.entry_absorption_efficiency_percentile
        else:
            diagonal_values = [item.buy_diagonal_imbalance_ratio for item in valid_bins]
            diagonal_median = self._median(diagonal_values)
<<<<<<< HEAD
            candidates = self._candidate_entry_zone_bins("SELL", record, valid_bins)
=======
            candidates = self._candidate_absorption_seed_bins("SELL", record, valid_bins)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            expected_dominant_side = "BUY"
            absorption_percentile = self.config.entry_absorption_efficiency_percentile

        if volume_threshold is None or diagonal_median is None:
            self._log_entry_state_rejection(mt5_symbol, timeframe_name, side, "INVALID_ENTRY_STATE_STATISTICS")
            return None

        for item in candidates:
            efficiency = self._candidate_efficiency(item)
            if efficiency is None:
                continue
            if str(item.dominant_diagonal_side).strip().upper() != expected_dominant_side:
                continue
            if item.efficiency_percentile is None or item.efficiency_percentile > absorption_percentile:
                continue
            diagonal_pressure = (
                item.sell_diagonal_imbalance_ratio
                if side == "BUY"
                else item.buy_diagonal_imbalance_ratio
            )
            if item.total_volume < volume_threshold:
                continue
            if diagonal_pressure <= self.config.entry_absorption_diagonal_multiplier * diagonal_median:
                continue
            stop_reference = low_price if side == "BUY" else high_price
            LOGGER.debug(
                "ABSORPTION_EFFICIENCY_SEED_ACCEPTED | symbol=%s | timeframe=%s | side=%s | "
                "bin_index=%d | efficiency=%.12f | efficiency_percentile=%.12f | "
                "price_progress_in_bin=%s | dominant_diagonal_side=%s | dominant_side_volume=%s",
                mt5_symbol,
                timeframe_name,
                side,
                int(item.bin_index),
                float(efficiency),
                float(item.efficiency_percentile),
                item.price_progress_in_bin,
                item.dominant_diagonal_side,
                item.dominant_side_volume,
            )
            return EntryState(
                symbol=mt5_symbol,
                timeframe=self._output_timeframe_for_builder(mt5_symbol, timeframe_name),
                side=side,
                absorption_candle_time_utc_ms=int(record.close_time_ms),
                started_closed_candle_count=current_closed_candle_count,
                expires_closed_candle_count=current_closed_candle_count
                + int(self.config.entry_state_timeout_closed_candles),
                stop_reference_price=stop_reference,
            )
        return None

    def _detect_dominance_confirmation(
        self,
        state: EntryState,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> EntryTrigger | None:
        open_price = self._decimal_from_payload(record.open_price)
        close_price = self._decimal_from_payload(record.close_price)
        if open_price is None or close_price is None:
            self._log_entry_state_rejection(state.symbol, state.timeframe, state.side, "INVALID_ENTRY_STATE_PRICE")
            return None
        if state.side == "BUY" and close_price <= open_price:
            return None
        if state.side == "SELL" and close_price >= open_price:
            return None

<<<<<<< HEAD
        candidates = self._candidate_body_bins(state.side, record, self._volume_valid_bins(bin_items))
=======
        candidates = self._candidate_full_candle_bins(record, self._volume_valid_bins(bin_items))
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        for item in candidates:
            dominance = self._dominance_candidate(
                side=state.side,
                item=item,
                efficiency_percentile_threshold=self.config.entry_dominance_efficiency_percentile,
                efficiency_zscore_threshold=self.config.entry_dominance_efficiency_zscore,
            )
            if dominance is None:
                continue
            efficiency, efficiency_percentile, efficiency_zscore = dominance
            if state.side == "BUY":
                entry_reason = "DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION"
            else:
                entry_reason = "DOMINANCE_CONFIRMED_AFTER_BUY_ABSORPTION"
            dominance_time = int(record.close_time_ms)
            LOGGER.debug(
                "DOMINANCE_EFFICIENCY_CONFIRMED | symbol=%s | timeframe=%s | side=%s | "
                "bin_index=%d | efficiency=%.12f | efficiency_percentile=%.12f | efficiency_zscore=%.12f | "
                "price_progress_in_bin=%s | dominant_diagonal_side=%s | dominant_side_volume=%s",
                state.symbol,
                state.timeframe,
                state.side,
                int(item.bin_index),
                float(efficiency),
                float(efficiency_percentile),
                float(efficiency_zscore),
                item.price_progress_in_bin,
                item.dominant_diagonal_side,
                item.dominant_side_volume,
            )
            return EntryTrigger(
                symbol=state.symbol,
                timeframe=state.timeframe,
                side=state.side,
                position_id=(
                    f"ENTRY-{state.symbol}-{state.timeframe}-{state.side}-"
                    f"{state.absorption_candle_time_utc_ms}-{dominance_time}"
                ),
                signal_time=dominance_time,
                stop_reference_price=state.stop_reference_price,
                absorption_candle_time_utc_ms=state.absorption_candle_time_utc_ms,
                dominance_candle_time_utc_ms=dominance_time,
                trigger_bin_price=Decimal(str(self._bin_price(item))),
                entry_reason=entry_reason,
            )
        return None

    def _dominance_candidate(
        self,
        *,
        side: str,
        item: BinMarketData,
        efficiency_percentile_threshold: float,
        efficiency_zscore_threshold: float,
    ) -> tuple[float, float, float] | None:
        normalized_side = side.strip().upper()
        if not item.is_volume_valid:
            return None
        if str(item.dominant_diagonal_side).strip().upper() != normalized_side:
            return None
        efficiency = self._candidate_efficiency(item)
        if efficiency is None:
            return None
        if item.efficiency_percentile is None or item.efficiency_percentile < efficiency_percentile_threshold:
            return None
        if item.efficiency_zscore is None:
            LOGGER.debug(
                "EFFICIENCY_ZSCORE_UNAVAILABLE | symbol=%s | timeframe=%s | bin_index=%d | side=%s",
                item.symbol,
                item.timeframe_name,
                int(item.bin_index),
                normalized_side,
            )
            return None
        if item.efficiency_zscore < efficiency_zscore_threshold:
            return None
        if normalized_side == "BUY":
            ratio = item.buy_diagonal_imbalance_ratio / (
                item.sell_diagonal_imbalance_ratio + self.config.entry_ratio_epsilon
            )
        elif normalized_side == "SELL":
            ratio = item.sell_diagonal_imbalance_ratio / (
                item.buy_diagonal_imbalance_ratio + self.config.entry_ratio_epsilon
            )
        else:
            return None
        if ratio <= self.config.entry_dominance_ratio:
            return None
        return efficiency, float(item.efficiency_percentile), float(item.efficiency_zscore)

    @staticmethod
    def _candidate_wick_bins(
        side: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[BinMarketData, ...]:
        if record.open_price is None or record.close_price is None or record.high_price is None or record.low_price is None:
            return tuple()
        open_price = float(record.open_price)
        close_price = float(record.close_price)
        high_price = float(record.high_price)
        low_price = float(record.low_price)
        if side == "BUY":
            upper_bound = min(open_price, close_price)
            return tuple(
                item
                for item in bin_items
                if low_price <= LiveAbsorptionRuntime._bin_price(item) < upper_bound
            )
        lower_bound = max(open_price, close_price)
        return tuple(
            item
            for item in bin_items
            if lower_bound < LiveAbsorptionRuntime._bin_price(item) <= high_price
        )

    @staticmethod
    def _candidate_full_candle_bins(
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[BinMarketData, ...]:
        if record.high_price is None or record.low_price is None:
            return tuple()
        high_price = float(record.high_price)
        low_price = float(record.low_price)
        return tuple(
            item
            for item in bin_items
            if low_price <= LiveAbsorptionRuntime._bin_price(item) <= high_price
        )

    @staticmethod
    def _candidate_absorption_seed_bins(
        side: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[BinMarketData, ...]:
        if record.open_price is None or record.close_price is None or record.high_price is None or record.low_price is None:
            return tuple()
        open_price = float(record.open_price)
        close_price = float(record.close_price)
        high_price = float(record.high_price)
        low_price = float(record.low_price)
        body_low = min(open_price, close_price)
        body_high = max(open_price, close_price)
        if side == "BUY":
            return tuple(
                item
                for item in bin_items
                if low_price <= LiveAbsorptionRuntime._bin_price(item) <= body_high
            )
        if side == "SELL":
            return tuple(
                item
                for item in bin_items
                if body_low <= LiveAbsorptionRuntime._bin_price(item) <= high_price
            )
        return tuple()

    @staticmethod
<<<<<<< HEAD
    def _candidate_entry_zone_bins(
        side: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[BinMarketData, ...]:
        if record.open_price is None or record.close_price is None or record.high_price is None or record.low_price is None:
            return tuple()
        open_price = float(record.open_price)
        close_price = float(record.close_price)
        high_price = float(record.high_price)
        low_price = float(record.low_price)
        body_low = min(open_price, close_price)
        body_high = max(open_price, close_price)
        body_third = (body_high - body_low) / 3.0
        if side == "BUY":
            upper_bound = body_low + body_third
            return tuple(
                item
                for item in bin_items
                if low_price <= LiveAbsorptionRuntime._bin_price(item) <= upper_bound
            )
        if side == "SELL":
            lower_bound = body_high - body_third
            return tuple(
                item
                for item in bin_items
                if lower_bound <= LiveAbsorptionRuntime._bin_price(item) <= high_price
            )
        return tuple()

    @staticmethod
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def _candidate_profit_exit_bins(
        position_side: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[BinMarketData, ...]:
        if position_side == "BUY":
<<<<<<< HEAD
            return LiveAbsorptionRuntime._candidate_entry_zone_bins("SELL", record, bin_items)
        if position_side == "SELL":
            return LiveAbsorptionRuntime._candidate_entry_zone_bins("BUY", record, bin_items)
=======
            return LiveAbsorptionRuntime._candidate_absorption_seed_bins("SELL", record, bin_items)
        if position_side == "SELL":
            return LiveAbsorptionRuntime._candidate_absorption_seed_bins("BUY", record, bin_items)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        return tuple()

    @staticmethod
    def _candidate_body_bins(
        side: str,
        record: CandleRecord,
        bin_items: tuple[BinMarketData, ...],
    ) -> tuple[BinMarketData, ...]:
        if record.open_price is None or record.close_price is None:
            return tuple()
        open_price = float(record.open_price)
        close_price = float(record.close_price)
        lower_bound = min(open_price, close_price)
        upper_bound = max(open_price, close_price)
        if side == "BUY" and close_price <= open_price:
            return tuple()
        if side == "SELL" and close_price >= open_price:
            return tuple()
        return tuple(
            item
            for item in bin_items
            if lower_bound <= LiveAbsorptionRuntime._bin_price(item) <= upper_bound
        )

    @staticmethod
    def _percentile(values: list[float | int], percentile: float) -> float | None:
        numeric_values = sorted(float(value) for value in values if float(value) >= 0.0)
        if not numeric_values:
            return None
        if len(numeric_values) == 1:
            return numeric_values[0]
        bounded = min(max(float(percentile), 0.0), 1.0)
        rank = bounded * (len(numeric_values) - 1)
        lower_index = int(rank)
        upper_index = min(lower_index + 1, len(numeric_values) - 1)
        fraction = rank - lower_index
        return numeric_values[lower_index] + (
            (numeric_values[upper_index] - numeric_values[lower_index]) * fraction
        )

    @staticmethod
    def _median(values: list[float | int]) -> float | None:
        numeric_values = sorted(float(value) for value in values if float(value) >= 0.0)
        if not numeric_values:
            return None
        middle = len(numeric_values) // 2
        if len(numeric_values) % 2 == 1:
            return numeric_values[middle]
        return (numeric_values[middle - 1] + numeric_values[middle]) / 2.0

    @staticmethod
    def _bin_price(item: BinMarketData) -> float:
<<<<<<< HEAD
        return float(item.price_low)
=======
        return (float(item.price_low) + float(item.price_high)) / 2.0
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    @staticmethod
    def _candidate_efficiency(item: BinMarketData) -> float | None:
        if not item.is_volume_valid:
            return None
        if str(item.dominant_diagonal_side).strip().upper() == "NONE":
            return None
        if item.min_trade_price_in_bin is None or item.max_trade_price_in_bin is None:
            return None
        if item.dominant_side_volume is None or float(item.dominant_side_volume) <= 0.0:
            return None
        value = item.dominant_side_efficiency
        if value is None:
            return None
        numeric = float(value)
        if not math.isfinite(numeric) or numeric < 0.0:
            return None
        return numeric

    @classmethod
    def _valid_efficiency_values(cls, bin_items: tuple[BinMarketData, ...]) -> list[float]:
        return [
            efficiency
            for item in bin_items
            if (efficiency := cls._candidate_efficiency(item)) is not None
        ]

    def _update_entry_stop_tracking(self, state: EntryState, record: CandleRecord) -> None:
        if state.side == "BUY":
            low_price = self._decimal_from_payload(record.low_price)
            if low_price is not None and low_price < state.stop_reference_price:
                state.stop_reference_price = low_price
        else:
            high_price = self._decimal_from_payload(record.high_price)
            if high_price is not None and high_price > state.stop_reference_price:
                state.stop_reference_price = high_price

    @staticmethod
    def _log_entry_state_rejection(symbol: str, timeframe: str, side: str, reason: str) -> None:
        LOGGER.debug(
            "ENTRY_STATE_REJECTED | symbol=%s | timeframe=%s | side=%s | reason=%s",
            symbol,
            timeframe,
            side,
            reason,
        )

    @staticmethod
    def _bin_price_progress(record: CandleRecord, l2_state: Any) -> Decimal:
        price_progress = getattr(l2_state, "price_progress_in_bin", None)
        if price_progress is not None:
            return Decimal(str(price_progress))
        return Decimal("0")

    @staticmethod
    def _log_absorption_candidate_rejection(
        symbol: str,
        timeframe_name: str,
        candle_open_time_utc_ms: int,
        candle_close_time_utc_ms: int,
        candidate_type: str,
        bin_indices: tuple[int, ...],
        reason: str,
    ) -> None:
        LOGGER.debug(
            "ABSORPTION_CANDIDATE_REJECTED | symbol=%s | timeframe=%s | "
            "candle_open_time_utc_ms=%d | candle_close_time_utc_ms=%d | "
            "candidate_type=%s | bin_indices=%s | reason=%s",
            symbol,
            timeframe_name,
            int(candle_open_time_utc_ms),
            int(candle_close_time_utc_ms),
            candidate_type,
            ",".join(str(item) for item in bin_indices),
            reason,
        )

    def _refresh_trading_zone_state(
        self,
        mt5_symbol: str,
        timeframe_name: str,
        *,
        trigger_evaluation: bool = True,
    ) -> None:
        normalized_timeframe = timeframe_name.strip().upper()
        if normalized_timeframe not in self._active_output_timeframes(mt5_symbol):
            return

        trigger_candidates = [
            trigger
            for trigger in self._entry_triggers_by_key.values()
            if trigger.symbol == mt5_symbol and trigger.timeframe == normalized_timeframe
        ]
        if trigger_candidates:
            trigger = max(
                trigger_candidates,
                key=lambda item: item.dominance_candle_time_utc_ms,
            )
            state = ZoneState(
                symbol=mt5_symbol,
                timeframe=normalized_timeframe,
                zone_id=trigger.position_id,
                side=trigger.side,
                suggested_stop_loss=trigger.stop_reference_price,
            )
        else:
            state = ZoneState(
                symbol=mt5_symbol,
                timeframe=normalized_timeframe,
                zone_id="",
                side=NEUTRAL_SIDE,
                suggested_stop_loss=None,
            )

        changed = self.zone_state_store.update(state)
        if not changed:
            return

        LOGGER.debug(
            "TRADING_ZONE_STATE_UPDATED | symbol=%s | timeframe=%s | zone_id=%s | side=%s | suggested_stop_loss=%s",
            state.symbol,
            state.timeframe,
            state.zone_id,
            state.side,
            state.suggested_stop_loss,
        )
        if not trigger_evaluation:
            return

        for client_name in self._known_execution_clients:
            for trading_timeframe in self._active_output_timeframes(mt5_symbol):
                self._refresh_execution_commands(
                    client_name=client_name,
                    symbol_name=mt5_symbol,
                    primary_timeframe=trading_timeframe,
                    trigger_source_timeframe=state.timeframe,
                    trigger_side=state.side,
                )

    @staticmethod
    def _execution_lock_key(client_name: str, symbol_name: str, side: object) -> tuple[str, str, str]:
        return (
            str(client_name).strip().lower() or "metatrader",
            str(symbol_name).strip(),
            str(side).strip().upper(),
        )
