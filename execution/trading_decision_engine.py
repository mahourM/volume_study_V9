from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from execution.trading_decision_event_recorder import (
    TradingDecisionEvent,
    get_trading_decision_event_recorder,
)
from execution.trading_zone_state import ZoneStateStore


@dataclass(frozen=True)
class OpenPositionState:
    client_name: str
    position_id: str
    client_position_id: str
    symbol: str
    timeframe: str
    side: str
    request_id: str = ""
    client_position_identifier: str = ""
    profit: Decimal | None = None
    entry_price: Decimal | None = None
    opened_at_utc_ms: int = 0


@dataclass(frozen=True)
class EntryTrigger:
    symbol: str
    timeframe: str
    side: str
    position_id: str
    signal_time: int
    stop_reference_price: Decimal
    absorption_candle_time_utc_ms: int
    dominance_candle_time_utc_ms: int
    trigger_bin_price: Decimal
    entry_reason: str
<<<<<<< HEAD
    action: str = ""
    target_entry_open_time_utc_ms: int = 0
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


@dataclass(frozen=True)
class ExitTrigger:
    symbol: str
    timeframe: str
    side: str
    position_id: str
    signal_time: int
    trigger_bin_price: Decimal
    exit_reason: str
<<<<<<< HEAD
    action: str = ""
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


@dataclass(frozen=True)
class TradingCommand:
    command_type: str
    client_name: str
    symbol_name: str
    timeframe: str
    side: str
    position_id: str
    request_id: str = ""
    cluster_id: str = ""
    client_position_id: str = ""
    client_position_identifier: str = ""
    signal_time: int = 0
    source_candle_open_time_utc_ms: int = 0
    source_candle_close_time_utc_ms: int = 0
    zone_low: Decimal | None = None
    zone_high: Decimal | None = None
    stop_reference_price: Decimal | None = None
    absorption_candle_time_utc_ms: int = 0
    dominance_candle_time_utc_ms: int = 0
    trigger_bin_price: Decimal | None = None
    entry_reason: str = ""
    exit_reason: str = ""
<<<<<<< HEAD
    action: str = ""
    target_entry_open_time_utc_ms: int = 0
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "command_type": self.command_type,
            "client_name": self.client_name,
            "symbol_name": self.symbol_name,
            "timeframe": self.timeframe,
            "side": self.side,
        }
<<<<<<< HEAD
        if self.action:
            payload["action"] = self.action
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        if self.request_id:
            payload["request_id"] = self.request_id
        if self.position_id:
            payload["position_id"] = self.position_id
        if self.cluster_id:
            payload["cluster_id"] = self.cluster_id
        if self.client_position_id:
            payload["client_position_id"] = self.client_position_id
        if self.client_position_identifier:
            payload["client_position_identifier"] = self.client_position_identifier
        if self.signal_time:
            payload["signal_time"] = self.signal_time
        if self.source_candle_open_time_utc_ms:
            payload["source_candle_open_time_utc_ms"] = self.source_candle_open_time_utc_ms
        if self.source_candle_close_time_utc_ms:
            payload["source_candle_close_time_utc_ms"] = self.source_candle_close_time_utc_ms
        if self.zone_low is not None:
            payload["zone_low"] = float(self.zone_low)
        if self.zone_high is not None:
            payload["zone_high"] = float(self.zone_high)
        if self.stop_reference_price is not None:
            payload["stop_reference_price"] = float(self.stop_reference_price)
        if self.absorption_candle_time_utc_ms:
            payload["absorption_candle_time_utc_ms"] = self.absorption_candle_time_utc_ms
        if self.dominance_candle_time_utc_ms:
            payload["dominance_candle_time_utc_ms"] = self.dominance_candle_time_utc_ms
        if self.trigger_bin_price is not None:
            payload["trigger_bin_price"] = float(self.trigger_bin_price)
        if self.entry_reason:
            payload["entry_reason"] = self.entry_reason
        if self.exit_reason:
            payload["exit_reason"] = self.exit_reason
<<<<<<< HEAD
        if self.target_entry_open_time_utc_ms:
            payload["target_entry_open_time_utc_ms"] = self.target_entry_open_time_utc_ms
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        return payload


class TradingDecisionEngine:
    def __init__(self, zone_state_store: ZoneStateStore) -> None:
        self.zone_state_store = zone_state_store

    def evaluate_symbol(
        self,
        *,
        client_name: str,
        symbol: str,
        entry_triggers: tuple[EntryTrigger, ...],
        open_positions: tuple[OpenPositionState, ...],
        exit_triggers: tuple[ExitTrigger, ...] = tuple(),
        primary_timeframe: str | None = None,
    ) -> tuple[TradingCommand, ...]:
        normalized_symbol = symbol.strip()
        normalized_primary = primary_timeframe.strip().upper() if primary_timeframe else None

        close_commands = self._build_close_commands(
            client_name=client_name,
            symbol=normalized_symbol,
            exit_triggers=exit_triggers,
            open_positions=open_positions,
        )
        if close_commands:
            return tuple(close_commands)

        symbol_positions = [
            position
            for position in open_positions
            if position.symbol == normalized_symbol
        ]
        if symbol_positions:
            return tuple()

        eligible_triggers: list[EntryTrigger] = []
        for trigger in entry_triggers:
            if trigger.symbol != normalized_symbol:
                continue
            if normalized_primary is not None and trigger.timeframe != normalized_primary:
                continue
            eligible_triggers.append(trigger)
        if not eligible_triggers:
            return tuple()

        trigger = max(
            eligible_triggers,
            key=lambda item: (
                item.dominance_candle_time_utc_ms,
                item.absorption_candle_time_utc_ms,
                item.side,
            ),
        )
        command = self._build_open_command(
            client_name=client_name,
            trigger=trigger,
        )
        return tuple() if command is None else (command,)

    def _build_open_command(
        self,
        *,
        client_name: str,
        trigger: EntryTrigger,
    ) -> TradingCommand | None:
        side = trigger.side.strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
<<<<<<< HEAD
        action = trigger.action or f"ENTRY_{side}"
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        get_trading_decision_event_recorder().record(
            TradingDecisionEvent(
                event_type="TRADING_DECISION_EVALUATED",
                symbol=trigger.symbol,
                trading_timeframe=trigger.timeframe,
                command_type="OPEN",
<<<<<<< HEAD
                action=action,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                entry_side=side,
                stop_reference_price=trigger.stop_reference_price,
                request_id=trigger.position_id,
                signal_time_utc_ms=trigger.signal_time,
<<<<<<< HEAD
                target_entry_open_time_utc_ms=trigger.target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                trigger_bin_price=trigger.trigger_bin_price,
                entry_reason=trigger.entry_reason,
                reason=trigger.entry_reason,
            )
        )
        return TradingCommand(
            command_type="OPEN",
            client_name=client_name,
            symbol_name=trigger.symbol,
            timeframe=trigger.timeframe,
            side=side,
            position_id="",
            request_id=trigger.position_id,
            cluster_id=trigger.position_id,
            signal_time=trigger.signal_time,
            source_candle_open_time_utc_ms=trigger.absorption_candle_time_utc_ms,
            source_candle_close_time_utc_ms=trigger.dominance_candle_time_utc_ms,
            stop_reference_price=trigger.stop_reference_price,
            absorption_candle_time_utc_ms=trigger.absorption_candle_time_utc_ms,
            dominance_candle_time_utc_ms=trigger.dominance_candle_time_utc_ms,
            trigger_bin_price=trigger.trigger_bin_price,
            entry_reason=trigger.entry_reason,
<<<<<<< HEAD
            action=action,
            target_entry_open_time_utc_ms=trigger.target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        )

    def _build_close_commands(
        self,
        *,
        client_name: str,
        symbol: str,
        exit_triggers: tuple[ExitTrigger, ...],
        open_positions: tuple[OpenPositionState, ...],
    ) -> list[TradingCommand]:
        commands: list[TradingCommand] = []
        for position in open_positions:
            if position.client_name != client_name or position.symbol != symbol:
                continue
            if position.profit is None or position.profit <= 0:
                continue
            exit_trigger = self._exit_trigger_for_position(position, exit_triggers)
            exit_reason = (
                exit_trigger.exit_reason
                if exit_trigger is not None
                else self._exit_reason(position)
            )
            if not exit_reason:
                continue
<<<<<<< HEAD
            action = (
                exit_trigger.action
                if exit_trigger is not None and exit_trigger.action
                else f"EXIT_{position.side}_POSITION"
            )
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            get_trading_decision_event_recorder().record(
                TradingDecisionEvent(
                    event_type="TRADING_DECISION_EVALUATED",
                    symbol=position.symbol,
                    trading_timeframe=position.timeframe,
                    command_type="CLOSE",
<<<<<<< HEAD
                    action=action,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                    entry_side=position.side,
                    request_id=position.request_id,
                    position_id=position.position_id,
                    client_position_id=position.client_position_id,
                    client_position_identifier=position.client_position_identifier,
                    signal_time_utc_ms=exit_trigger.signal_time if exit_trigger is not None else 0,
                    trigger_bin_price=(
                        exit_trigger.trigger_bin_price
                        if exit_trigger is not None
                        else None
                    ),
                    exit_reason=exit_reason,
                    reason=exit_reason,
                )
            )
            commands.append(
                TradingCommand(
                    command_type="CLOSE",
                    client_name=client_name,
                    symbol_name=position.symbol,
                    timeframe=position.timeframe,
                    side=position.side,
                    position_id=position.position_id,
                    request_id=(
                        f"CLOSE-{position.client_name}-{position.client_position_id}-"
                        f"{exit_trigger.signal_time if exit_trigger is not None else 0}"
                    ),
                    client_position_id=position.client_position_id,
                    client_position_identifier=position.client_position_identifier,
                    signal_time=exit_trigger.signal_time if exit_trigger is not None else 0,
                    trigger_bin_price=(
                        exit_trigger.trigger_bin_price
                        if exit_trigger is not None
                        else None
                    ),
                    exit_reason=exit_reason,
<<<<<<< HEAD
                    action=action,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                )
            )
        return commands

    @staticmethod
    def _exit_trigger_for_position(
        position: OpenPositionState,
        exit_triggers: tuple[ExitTrigger, ...],
    ) -> ExitTrigger | None:
        matching = [
            trigger
            for trigger in exit_triggers
            if trigger.symbol == position.symbol
            and trigger.timeframe == position.timeframe
            and trigger.side == position.side
            and (not trigger.position_id or trigger.position_id == position.position_id)
        ]
        if not matching:
            return None
        return max(
            matching,
            key=lambda item: (
                item.signal_time,
                item.trigger_bin_price,
                item.exit_reason,
            ),
        )

    def _exit_reason(self, position: OpenPositionState) -> str:
        opposite_side = self._opposite_side(position.side)
        trading_state = self.zone_state_store.get(position.symbol, position.timeframe)
        if trading_state.is_active and trading_state.side == opposite_side:
            return "TRADING_TIMEFRAME_OPPOSITE_ENTRY_STATE"
        return ""

    @staticmethod
    def _opposite_side(side: str) -> str:
        if side == "BUY":
            return "SELL"
        if side == "SELL":
            return "BUY"
        return ""
