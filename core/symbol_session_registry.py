from __future__ import annotations

import time
from typing import Iterable

<<<<<<< HEAD
from core.symbol_resolver import BinanceSymbolResolver, PROVIDER_BINANCE, SymbolResolution
from core.system_models import SymbolSessionState, SystemInitRequest
from core.timeframe_policy import (
    normalized_execution_timeframes,
    normalized_study_timeframes,
    primary_execution_timeframe,
)
=======
from core.symbol_resolver import BinanceSymbolResolver
from core.system_models import SymbolSessionState, SystemInitRequest
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


SessionKey = tuple[str, str]


def make_session_key(mt5_symbol: str, timeframe: str) -> SessionKey:
    return (mt5_symbol, timeframe.strip().upper())


class SymbolSessionRegistry:
    def __init__(self, resolver: BinanceSymbolResolver) -> None:
        self.resolver = resolver
        self._sessions_by_key: dict[SessionKey, SymbolSessionState] = {}
        self._last_init_request: SystemInitRequest | None = None

    @property
    def is_initialized(self) -> bool:
        return self._last_init_request is not None

    def initialize(self, request: SystemInitRequest) -> list[SymbolSessionState]:
        for symbol_spec in request.symbols:
<<<<<<< HEAD
            resolution = self._resolution_for_symbol(symbol_spec.mt5_symbol)
            for timeframe in normalized_study_timeframes():
                if resolution is not None:
                    session = SymbolSessionState(
                        mt5_symbol=symbol_spec.mt5_symbol,
                        binance_symbol=resolution.binance_symbol,
                        market_provider=resolution.market_provider,
                        provider_symbol=resolution.provider_symbol,
                        dataset=resolution.dataset,
                        schema=resolution.schema,
                        tick_size=resolution.tick_size,
                        timeframe=timeframe,
                        symbol_resolved=True,
                        session_ready=True,
                        status="READY",
                    )
                else:
                    session = SymbolSessionState(
                        mt5_symbol=symbol_spec.mt5_symbol,
                        timeframe=timeframe,
                        symbol_resolved=False,
                        session_ready=False,
                        status="SYMBOL_NOT_SUPPORTED",
                    )
                self._sessions_by_key[make_session_key(session.mt5_symbol, session.timeframe)] = session
=======
            binance_symbol = self.resolver.resolve(symbol_spec.mt5_symbol)
            if binance_symbol:
                session = SymbolSessionState(
                    mt5_symbol=symbol_spec.mt5_symbol,
                    binance_symbol=binance_symbol,
                    timeframe=symbol_spec.timeframe,
                    symbol_resolved=True,
                    session_ready=True,
                    status="READY",
                )
            else:
                session = SymbolSessionState(
                    mt5_symbol=symbol_spec.mt5_symbol,
                    timeframe=symbol_spec.timeframe,
                    symbol_resolved=False,
                    session_ready=False,
                    status="SYMBOL_NOT_SUPPORTED",
                )
            self._sessions_by_key[make_session_key(session.mt5_symbol, session.timeframe)] = session
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

        self._last_init_request = request
        return self.get_all_sessions()

<<<<<<< HEAD
    def _resolution_for_symbol(self, mt5_symbol: str) -> SymbolResolution | None:
        raw_resolution = self.resolver.resolve(mt5_symbol)
        if raw_resolution is None:
            return None
        if isinstance(raw_resolution, SymbolResolution):
            return raw_resolution
        return SymbolResolution(
            market_provider=PROVIDER_BINANCE,
            provider_symbol=str(raw_resolution),
            binance_symbol=str(raw_resolution),
        )

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def get_all_sessions(self) -> list[SymbolSessionState]:
        return list(self._sessions_by_key.values())

    def get_sessions_for_symbol(self, mt5_symbol: str) -> list[SymbolSessionState]:
        return [
            session
            for (symbol, _timeframe), session in self._sessions_by_key.items()
            if symbol == mt5_symbol
        ]

    def build_system_init_ack(self, sessions: Iterable[SymbolSessionState]) -> dict[str, object]:
        session_list = list(sessions)
        resolved_count = sum(1 for session in session_list if session.symbol_resolved)
        ready_count = sum(1 for session in session_list if session.status == "READY")
        if session_list and ready_count == len(session_list):
            overall_status = "OK"
        elif ready_count > 0:
            overall_status = "PARTIAL_OK"
        else:
            overall_status = "ERROR"

        return {
            "type": "SYSTEM_INIT_ACK",
            "schema_version": "7.0",
            "status": overall_status,
            "symbols_count": len(session_list),
<<<<<<< HEAD
            "sessions_count": len(session_list),
            "resolved_symbols_count": resolved_count,
            "ready_symbols_count": ready_count,
            "study_timeframes": list(normalized_study_timeframes()),
            "execution_timeframes": list(normalized_execution_timeframes()),
            "primary_execution_timeframe": primary_execution_timeframe(),
=======
            "resolved_symbols_count": resolved_count,
            "ready_symbols_count": ready_count,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "symbols": [session.to_ack_payload() for session in session_list],
            "generated_at_utc": int(time.time() * 1000),
        }
