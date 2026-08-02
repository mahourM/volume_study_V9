from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from client.execution_contracts import DEFAULT_INITIAL_DEPOSIT_USD, DEFAULT_RISK_PERCENT, PREFERRED_LEVERAGE


@dataclass(frozen=True)
class KrakenClientConfig:
    api_key: str = ""
    private_key: str = ""
    rest_base_url: str = "https://api.kraken.com"
    public_ws_url: str = "wss://ws.kraken.com"
    risk_percent: Decimal = DEFAULT_RISK_PERCENT
    initial_deposit_usd: Decimal = DEFAULT_INITIAL_DEPOSIT_USD
    preferred_leverage: Decimal = PREFERRED_LEVERAGE
    live_execution_enabled: bool = False
    post_stop_reentry_delay_seconds: int = 30
    reentry_stop_buffer_atr_period: int = 14
    reentry_stop_buffer_atr_multiplier: Decimal = Decimal("0.10")
    symbol_pair_overrides: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_environment(cls, local_config_path: Path | None = None) -> "KrakenClientConfig":
        file_values: dict[str, object] = {}
        if local_config_path is not None and local_config_path.exists():
            file_values = json.loads(local_config_path.read_text(encoding="utf-8"))

        def value(name: str, default: object = "") -> object:
            return os.environ.get(name, file_values.get(name, default))

        return cls(
            api_key=str(value("KRAKEN_API_KEY", "")),
            private_key=str(value("KRAKEN_PRIVATE_KEY", "")),
            rest_base_url=str(value("KRAKEN_REST_BASE_URL", "https://api.kraken.com")),
            public_ws_url=str(value("KRAKEN_PUBLIC_WS_URL", "wss://ws.kraken.com")),
            risk_percent=Decimal(str(value("KRAKEN_RISK_PERCENT", DEFAULT_RISK_PERCENT))),
            initial_deposit_usd=Decimal(str(value("KRAKEN_INITIAL_DEPOSIT_USD", DEFAULT_INITIAL_DEPOSIT_USD))),
            preferred_leverage=Decimal(str(value("KRAKEN_PREFERRED_LEVERAGE", PREFERRED_LEVERAGE))),
            live_execution_enabled=str(value("KRAKEN_LIVE_EXECUTION_ENABLED", "false")).strip().lower() == "true",
            symbol_pair_overrides=dict(file_values.get("KRAKEN_SYMBOL_PAIR_OVERRIDES", {}) or {}),
        )
