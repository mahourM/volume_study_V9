from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Protocol


DEFAULT_RISK_PERCENT = Decimal("3")
DEFAULT_INITIAL_DEPOSIT_USD = Decimal("2000")
PREFERRED_LEVERAGE = Decimal("50")


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PositionLifecycleStatus(str, Enum):
    OPENED = "POSITION_OPENED"
    STILL_OPEN = "POSITION_STILL_OPEN"
    CLOSED_BY_STOP_LOSS = "POSITION_CLOSED_BY_STOP_LOSS"
    CLOSED_BY_SIGNAL = "POSITION_CLOSED_BY_SIGNAL"
    REJECTED = "POSITION_REJECTED"


class TradingCommandType(str, Enum):
    OPEN = "OPEN"
    CLOSE = "CLOSE"


@dataclass(frozen=True)
class ExecutionSignal:
    position_id: str
    symbol_name: str
    timeframe: str
    side: TradeSide
    signal_time: int
    cluster_id: str
    source_candle_open_time_utc_ms: int
    source_candle_close_time_utc_ms: int
    request_id: str = ""
    stop_reference_price: Decimal | None = None
    absorption_candle_time_utc_ms: int = 0
    dominance_candle_time_utc_ms: int = 0
    trigger_bin_price: Decimal | None = None
    entry_reason: str = ""
<<<<<<< HEAD
    action: str = ""
    target_entry_open_time_utc_ms: int = 0
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def to_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "symbol_name": self.symbol_name,
            "timeframe": self.timeframe,
            "side": self.side.value,
            "signal_time": self.signal_time,
            "cluster_id": self.cluster_id,
            "source_candle_open_time_utc_ms": self.source_candle_open_time_utc_ms,
            "source_candle_close_time_utc_ms": self.source_candle_close_time_utc_ms,
        }
        if self.request_id:
            payload["request_id"] = self.request_id
        if self.position_id:
            payload["position_id"] = self.position_id
        if self.stop_reference_price is not None:
            payload["stop_reference_price"] = str(self.stop_reference_price)
        if self.absorption_candle_time_utc_ms:
            payload["absorption_candle_time_utc_ms"] = self.absorption_candle_time_utc_ms
        if self.dominance_candle_time_utc_ms:
            payload["dominance_candle_time_utc_ms"] = self.dominance_candle_time_utc_ms
        if self.trigger_bin_price is not None:
            payload["trigger_bin_price"] = str(self.trigger_bin_price)
        if self.entry_reason:
            payload["entry_reason"] = self.entry_reason
<<<<<<< HEAD
        if self.action:
            payload["action"] = self.action
        if self.target_entry_open_time_utc_ms:
            payload["target_entry_open_time_utc_ms"] = self.target_entry_open_time_utc_ms
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        return payload


@dataclass(frozen=True)
class TradingCommand:
    command_type: TradingCommandType
    position_id: str
    symbol_name: str
    timeframe: str
    side: TradeSide
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
            "command_type": self.command_type.value,
            "symbol_name": self.symbol_name,
            "timeframe": self.timeframe,
            "side": self.side.value,
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
            payload["zone_low"] = str(self.zone_low)
        if self.zone_high is not None:
            payload["zone_high"] = str(self.zone_high)
        if self.stop_reference_price is not None:
            payload["stop_reference_price"] = str(self.stop_reference_price)
        if self.absorption_candle_time_utc_ms:
            payload["absorption_candle_time_utc_ms"] = self.absorption_candle_time_utc_ms
        if self.dominance_candle_time_utc_ms:
            payload["dominance_candle_time_utc_ms"] = self.dominance_candle_time_utc_ms
        if self.trigger_bin_price is not None:
            payload["trigger_bin_price"] = str(self.trigger_bin_price)
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


@dataclass(frozen=True)
class PositionStatusEvent:
    client_name: str
    position_id: str
    symbol_name: str
    timeframe: str
    side: TradeSide
    status: PositionLifecycleStatus
    request_id: str = ""
    signal_time: int = 0
    cluster_id: str = ""
    client_position_id: str = ""
    client_position_identifier: str = ""
    profit: Decimal | None = None
    entry_price: Decimal | None = None
    opened_at_utc_ms: int = 0
    rejection_reason: str = ""


@dataclass(frozen=True)
class AccountSnapshot:
    account_balance: Decimal
    account_equity: Decimal
    profit_position_count: int
    loss_position_count: int
    average_profit_amount: Decimal
    average_loss_amount: Decimal

    @property
    def total_position_count(self) -> int:
        return self.profit_position_count + self.loss_position_count

    @property
    def edge(self) -> Decimal:
        if self.total_position_count <= 0:
            return Decimal("0")
        total_count = Decimal(self.total_position_count)
        profit_weight = Decimal(self.profit_position_count) / total_count
        loss_weight = Decimal(self.loss_position_count) / total_count
        return (profit_weight * self.average_profit_amount) - (loss_weight * abs(self.average_loss_amount))

    @property
    def edge_percent(self) -> Decimal:
        if self.account_balance == 0:
            return Decimal("0")
        return (self.edge / self.account_balance) * Decimal("100")


@dataclass(frozen=True)
class ExecutionDecision:
    client_name: str
    symbol_name: str
    timeframe: str
    side: TradeSide
    position_id: str
    decision_type: str
    decision_result: str
    rejection_reason: str = ""
    request_id: str = ""
    client_position_id: str = ""
    client_position_identifier: str = ""


class ExecutionClient(Protocol):
    client_name: str

    async def process_signal(self, signal: ExecutionSignal) -> ExecutionDecision:
        ...

    async def process_command(self, command: TradingCommand) -> ExecutionDecision:
        ...
