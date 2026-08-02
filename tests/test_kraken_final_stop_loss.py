from __future__ import annotations

import unittest
from decimal import Decimal

from client.execution_contracts import ExecutionSignal, TradeSide, TradingCommand, TradingCommandType
from client.kraken_config import KrakenClientConfig
from client.kraken_execution_client import KrakenExecutionClient


def signal(*, side: TradeSide = TradeSide.BUY, stop_reference_price: Decimal | None = Decimal("99")) -> ExecutionSignal:
    return ExecutionSignal(
        position_id="ABS-test",
        symbol_name="BTCUSD",
        timeframe="M5",
        side=side,
        signal_time=180_000,
        cluster_id="ABS-test",
        source_candle_open_time_utc_ms=120_000,
        source_candle_close_time_utc_ms=180_000,
        stop_reference_price=stop_reference_price,
        absorption_candle_time_utc_ms=120_000,
        dominance_candle_time_utc_ms=180_000,
        trigger_bin_price=Decimal("100"),
        entry_reason="DOMINANCE_CONFIRMED_AFTER_SELL_ABSORPTION",
    )


class KrakenStopReferenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.client = KrakenExecutionClient(KrakenClientConfig())

    async def test_resolve_stop_loss_uses_stop_reference_price(self) -> None:
        stop_loss = await self.client.resolve_stop_loss(
            signal(stop_reference_price=Decimal("99")),
            Decimal("100"),
        )

        self.assertEqual(stop_loss, Decimal("99"))

    async def test_resolve_stop_loss_rejects_missing_or_wrong_side_stop_reference(self) -> None:
        self.assertIsNone(
            await self.client.resolve_stop_loss(
                signal(stop_reference_price=None),
                Decimal("100"),
            )
        )
        self.assertIsNone(
            await self.client.resolve_stop_loss(
                signal(stop_reference_price=Decimal("101")),
                Decimal("100"),
            )
        )

    async def test_process_command_rejects_missing_stop_reference_price(self) -> None:
        command = TradingCommand(
            command_type=TradingCommandType.OPEN,
            position_id="ABS-test",
            symbol_name="BTCUSD",
            timeframe="M5",
            side=TradeSide.BUY,
            cluster_id="ABS-test",
            stop_reference_price=None,
        )

        decision = await self.client.process_command(command)

        self.assertEqual(decision.decision_result, "rejected")
        self.assertEqual(decision.rejection_reason, "INVALID_STOP_REFERENCE_PRICE")


if __name__ == "__main__":
    unittest.main()
