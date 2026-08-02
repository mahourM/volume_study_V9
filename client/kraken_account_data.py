from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from client.execution_contracts import AccountSnapshot
from client.kraken_order_gateway import KrakenOrderGateway


@dataclass(frozen=True)
class TradingPerformanceStats:
    profit_position_count: int = 0
    loss_position_count: int = 0
    average_profit_amount: Decimal = Decimal("0")
    average_loss_amount: Decimal = Decimal("0")


class KrakenAccountDataClient:
    def __init__(self, order_gateway: KrakenOrderGateway) -> None:
        self.order_gateway = order_gateway

    def build_account_snapshot(
        self,
        *,
        account_balance: Decimal,
        account_equity: Decimal,
        performance_stats: TradingPerformanceStats,
    ) -> AccountSnapshot:
        return AccountSnapshot(
            account_balance=account_balance,
            account_equity=account_equity,
            profit_position_count=performance_stats.profit_position_count,
            loss_position_count=performance_stats.loss_position_count,
            average_profit_amount=performance_stats.average_profit_amount,
            average_loss_amount=abs(performance_stats.average_loss_amount),
        )

    def fetch_private_balance_payload(self) -> dict[str, object]:
        return self.order_gateway.private_request("/0/private/Balance", {})
