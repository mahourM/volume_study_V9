from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from core.absorption_path_state import AbsorptionPathState
from core.dpc_path_state import DpcPathState


class SystemInitValidationError(ValueError):
    """Raised when the MT5 startup payload is malformed."""


@dataclass(frozen=True)
class SystemInitSymbolSpec:
    mt5_symbol: str
<<<<<<< HEAD
    timeframe: str | None = None
=======
    timeframe: str
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "SystemInitSymbolSpec":
        raw_mt5_symbol = payload.get("mt5_symbol", "")
        mt5_symbol = raw_mt5_symbol if isinstance(raw_mt5_symbol, str) else str(raw_mt5_symbol)
<<<<<<< HEAD
        raw_timeframe = payload.get("timeframe")
        timeframe = str(raw_timeframe).strip().upper() if raw_timeframe is not None else None
        if not mt5_symbol:
            raise SystemInitValidationError("SYSTEM_INIT symbols require mt5_symbol")
        return cls(mt5_symbol=mt5_symbol, timeframe=timeframe)

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "mt5_symbol": self.mt5_symbol,
        }
        if self.timeframe:
            payload["timeframe"] = self.timeframe
        return payload
=======
        timeframe = str(payload.get("timeframe", "")).strip().upper()
        if not mt5_symbol:
            raise SystemInitValidationError("SYSTEM_INIT symbols require mt5_symbol")
        if not timeframe:
            raise SystemInitValidationError("SYSTEM_INIT symbols require timeframe")
        return cls(mt5_symbol=mt5_symbol, timeframe=timeframe)

    def to_payload(self) -> dict[str, Any]:
        return {
            "mt5_symbol": self.mt5_symbol,
            "timeframe": self.timeframe,
        }
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


@dataclass(frozen=True)
class SystemInitRequest:
    symbols_count: int
    symbols: tuple[SystemInitSymbolSpec, ...]
    schema_version: str = "7.0"

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "SystemInitRequest":
        request_type = str(payload.get("type", "")).strip().upper()
        if request_type != "SYSTEM_INIT":
            raise SystemInitValidationError("Unsupported request type for system initialization")

        raw_symbols = payload.get("symbols", [])
        if not isinstance(raw_symbols, list) or not raw_symbols:
            raise SystemInitValidationError("SYSTEM_INIT requires a non-empty symbols list")

        symbols = tuple(SystemInitSymbolSpec.from_payload(item) for item in raw_symbols)
        symbols_count = int(payload.get("symbols_count", len(symbols)))
        if symbols_count < 0:
            raise SystemInitValidationError("SYSTEM_INIT symbols_count must be non-negative")

        schema_version = str(payload.get("schema_version", "7.0")).strip() or "7.0"
        return cls(
            symbols_count=symbols_count,
            symbols=symbols,
            schema_version=schema_version,
        )


@dataclass
class SymbolSessionState:
    mt5_symbol: str
    timeframe: str
    binance_symbol: str = ""
<<<<<<< HEAD
    market_provider: str = ""
    provider_symbol: str = ""
    dataset: str = ""
    schema: str = ""
    tick_size: str = ""
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    symbol_resolved: bool = False
    session_ready: bool = False
    status: str = "PENDING"
    dpc_path_state: DpcPathState = field(default_factory=DpcPathState)
    absorption_path_state: AbsorptionPathState = field(default_factory=AbsorptionPathState)

    def to_ack_payload(self) -> dict[str, Any]:
        return {
            "mt5_symbol": self.mt5_symbol,
            "timeframe": self.timeframe,
            "status": self.status,
            "symbol_resolved": self.symbol_resolved,
            "session_ready": self.session_ready,
            "dpc_path_ready": self.dpc_path_state.path_ready,
            "absorption_path_ready": self.absorption_path_state.path_ready,
            "binance_symbol": self.binance_symbol,
<<<<<<< HEAD
            "market_provider": self.market_provider,
            "provider_symbol": self.provider_symbol,
            "dataset": self.dataset,
            "schema": self.schema,
            "tick_size": self.tick_size,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        }
