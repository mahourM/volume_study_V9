from __future__ import annotations

import time
from dataclasses import dataclass
from decimal import Decimal

from client.execution_contracts import ExecutionDecision, ExecutionSignal, TradeSide, TradingCommand, TradingCommandType
from client.kraken_account_data import KrakenAccountDataClient
from client.kraken_config import KrakenClientConfig
from client.kraken_market_data import KrakenMarketDataClient
from client.kraken_order_gateway import KrakenOrderGateway


@dataclass
class KrakenPostStopRecord:
    stop_hit_price: Decimal
    stopped_at: float


class KrakenExecutionClient:
    client_name = "kraken"

    def __init__(self, config: KrakenClientConfig | None = None) -> None:
        self.config = config or KrakenClientConfig.from_environment()
        self.order_gateway = KrakenOrderGateway(self.config)
        self.market_data = KrakenMarketDataClient(self.config)
        self.account_data = KrakenAccountDataClient(self.order_gateway)
        self._post_stop_records: dict[tuple[str, TradeSide], KrakenPostStopRecord] = {}

    async def process_signal(self, signal: ExecutionSignal) -> ExecutionDecision:
        if self.is_post_stop_delay_active(signal):
            return ExecutionDecision(
                client_name=self.client_name,
                symbol_name=signal.symbol_name,
                timeframe=signal.timeframe,
                side=signal.side,
                position_id=signal.position_id,
                decision_type="entry",
                decision_result="rejected",
                rejection_reason="POST_STOP_REENTRY_DELAY_ACTIVE",
                request_id=signal.request_id,
            )

        if signal.stop_reference_price is None or signal.stop_reference_price <= 0:
            return ExecutionDecision(
                client_name=self.client_name,
                symbol_name=signal.symbol_name,
                timeframe=signal.timeframe,
                side=signal.side,
                position_id=signal.position_id,
                decision_type="entry",
                decision_result="rejected",
                rejection_reason="INVALID_STOP_REFERENCE_PRICE",
                request_id=signal.request_id,
            )

        if not self.config.live_execution_enabled:
            return ExecutionDecision(
                client_name=self.client_name,
                symbol_name=signal.symbol_name,
                timeframe=signal.timeframe,
                side=signal.side,
                position_id=signal.position_id,
                decision_type="entry",
                decision_result="rejected",
                rejection_reason="KRAKEN_EXECUTION_NOT_ENABLED",
                request_id=signal.request_id,
            )

        return ExecutionDecision(
            client_name=self.client_name,
            symbol_name=signal.symbol_name,
            timeframe=signal.timeframe,
            side=signal.side,
            position_id=signal.position_id,
            decision_type="entry",
            decision_result="rejected",
            rejection_reason="KRAKEN_LIVE_EXECUTION_GATEWAY_NOT_WIRED",
            request_id=signal.request_id,
        )

    async def process_command(self, command: TradingCommand) -> ExecutionDecision:
        if command.command_type == TradingCommandType.CLOSE:
            return ExecutionDecision(
                client_name=self.client_name,
                symbol_name=command.symbol_name,
                timeframe=command.timeframe,
                side=command.side,
                position_id=command.position_id,
                decision_type="exit",
                decision_result="rejected",
                rejection_reason="KRAKEN_LIVE_EXECUTION_GATEWAY_NOT_WIRED",
                request_id=command.request_id,
                client_position_id=command.client_position_id,
            )
        if command.stop_reference_price is None or command.stop_reference_price <= 0:
            return ExecutionDecision(
                client_name=self.client_name,
                symbol_name=command.symbol_name,
                timeframe=command.timeframe,
                side=command.side,
                position_id=command.position_id,
                decision_type="entry",
                decision_result="rejected",
                rejection_reason="INVALID_STOP_REFERENCE_PRICE",
                request_id=command.request_id,
            )
        signal = ExecutionSignal(
            position_id=command.position_id,
            symbol_name=command.symbol_name,
            timeframe=command.timeframe,
            side=command.side,
            signal_time=command.signal_time,
            cluster_id=command.cluster_id or command.request_id or command.position_id,
            source_candle_open_time_utc_ms=command.source_candle_open_time_utc_ms,
            source_candle_close_time_utc_ms=command.source_candle_close_time_utc_ms,
            request_id=command.request_id,
            stop_reference_price=command.stop_reference_price,
            absorption_candle_time_utc_ms=command.absorption_candle_time_utc_ms,
            dominance_candle_time_utc_ms=command.dominance_candle_time_utc_ms,
            trigger_bin_price=command.trigger_bin_price,
            entry_reason=command.entry_reason,
<<<<<<< HEAD
            action=command.action,
            target_entry_open_time_utc_ms=command.target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        )
        return await self.process_signal(signal)

    def initial_risk_amount(self) -> Decimal:
        return self.config.initial_deposit_usd * (self.config.risk_percent / Decimal("100"))

    async def resolve_stop_loss(self, signal: ExecutionSignal, entry_price: Decimal) -> Decimal | None:
        if self.is_post_stop_delay_active(signal):
            return None

        if (
            signal.stop_reference_price is not None
            and self.stop_loss_behind_entry(signal.side, entry_price, signal.stop_reference_price)
        ):
            return signal.stop_reference_price
        return None

    @staticmethod
    def stop_loss_behind_entry(side: TradeSide, entry_price: Decimal, stop_loss: Decimal) -> bool:
        if entry_price <= 0 or stop_loss <= 0:
            return False
        if side == TradeSide.BUY:
            return stop_loss < entry_price
        return stop_loss > entry_price

    def record_stop_loss_close(
        self,
        symbol_name: str,
        side: TradeSide,
        stop_hit_price: Decimal,
        stopped_at: float | None = None,
    ) -> None:
        self._post_stop_records[(symbol_name, side)] = KrakenPostStopRecord(
            stop_hit_price=stop_hit_price,
            stopped_at=time.time() if stopped_at is None else stopped_at,
        )

    def is_post_stop_delay_active(self, signal: ExecutionSignal, now: float | None = None) -> bool:
        record = self._post_stop_records.get((signal.symbol_name, signal.side))
        if record is None:
            return False
        current_time = time.time() if now is None else now
        return current_time - record.stopped_at < self.config.post_stop_reentry_delay_seconds

    @staticmethod
    def normalize_side(side: str) -> TradeSide:
        normalized = side.strip().upper()
        if normalized == TradeSide.BUY.value:
            return TradeSide.BUY
        if normalized == TradeSide.SELL.value:
            return TradeSide.SELL
        raise ValueError(f"Unsupported Kraken trade side: {side}")
