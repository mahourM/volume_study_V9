from __future__ import annotations

import asyncio
import json
from typing import Any

<<<<<<< HEAD
from core.symbol_resolver import (
    BinanceSymbolResolver,
    CmeSymbolResolver,
    MarketSymbolResolver,
    PROVIDER_CME_LOCAL_DBN,
)
from core.symbol_session_registry import SymbolSessionRegistry
from core.timeframe_policy import normalized_execution_timeframes, normalized_study_timeframes
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from core.system_startup_router import SystemStartupRouter


class _InitializedRegistry:
    is_initialized = True


class _ExecutionCommandProvider:
    def execution_command_payloads(
        self,
        mt5_symbol: str | None = None,
        client_name: str = "metatrader",
        primary_timeframe: str = "M15",
    ) -> list[dict[str, Any]]:
        return [
            {
                "command_type": "OPEN",
                "request_id": "ENTRY-BTCUSD-M5-BUY-1780620900000-1780620900000",
                "client_name": client_name,
                "symbol_name": mt5_symbol,
                "timeframe": primary_timeframe,
                "side": "BUY",
            }
        ]


<<<<<<< HEAD
class _RecordingConfigurator:
    def __init__(self) -> None:
        self.sessions = []

    def configure_sessions(self, sessions: list[Any]) -> None:
        self.sessions = list(sessions)


class _FakeCmeCatalog:
    def __init__(self, symbols: tuple[str, ...]) -> None:
        self._symbols = symbols

    def available_symbols(self) -> tuple[str, ...]:
        return self._symbols


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
def test_execution_command_list_envelope_identity_precedes_nested_command_ids() -> None:
    request_id = "GET_TRADING_EXECUTION_SIGNALS:BTCUSD:M5:1780631700:1520592390"
    router = SystemStartupRouter(
        _InitializedRegistry(),  # type: ignore[arg-type]
        _ExecutionCommandProvider(),  # type: ignore[arg-type]
    )

    response = asyncio.run(
        router.handle_request(
            {
                "type": "GET_TRADING_EXECUTION_SIGNALS",
                "request_id": request_id,
                "client_name": "metatrader",
                "symbol": "BTCUSD",
                "primary_timeframe": "M5",
            }
        )
    )

    json_text = json.dumps(response, separators=(",", ":"))

    assert response["type"] == "TRADING_EXECUTION_COMMAND_LIST"
    assert list(response).index("request_id") < list(response).index("commands")
    assert list(response).index("symbol") < list(response).index("commands")
    assert list(response).index("primary_timeframe") < list(response).index("commands")
    assert json_text.find(f'"request_id":"{request_id}"') < json_text.find('"commands":')
    assert json_text.find('"request_id":"ENTRY-BTCUSD-M5-BUY-') > json_text.find('"commands":')
<<<<<<< HEAD


def test_system_init_without_timeframe_expands_all_study_timeframes() -> None:
    registry = SymbolSessionRegistry(BinanceSymbolResolver())
    configurator = _RecordingConfigurator()
    router = SystemStartupRouter(
        registry,
        configurator,  # type: ignore[arg-type]
    )

    response = asyncio.run(
        router.handle_request(
            {
                "type": "SYSTEM_INIT",
                "schema_version": "7.0",
                "symbols_count": 1,
                "symbols": [{"mt5_symbol": "BTCUSD"}],
            }
        )
    )

    study_timeframes = normalized_study_timeframes()

    assert response["type"] == "SYSTEM_INIT_ACK"
    assert response["status"] == "OK"
    assert response["symbols_count"] == len(study_timeframes)
    assert response["sessions_count"] == len(study_timeframes)
    assert response["study_timeframes"] == list(study_timeframes)
    assert response["execution_timeframes"] == list(normalized_execution_timeframes())
    assert response["primary_execution_timeframe"] == "M5"
    assert [item["timeframe"] for item in response["symbols"]] == list(study_timeframes)
    assert {session.timeframe for session in configurator.sessions} == set(study_timeframes)
    assert {session.binance_symbol for session in configurator.sessions} == {"BTCUSDT"}


def test_system_init_routes_cme_symbol_without_binance_symbol() -> None:
    registry = SymbolSessionRegistry(
        MarketSymbolResolver(
            cme_resolver=CmeSymbolResolver(
                available_symbols=("NQ.FUT",),
                tick_sizes={"NQ.FUT": "0.25"},
            )
        )
    )
    configurator = _RecordingConfigurator()
    router = SystemStartupRouter(
        registry,
        configurator,  # type: ignore[arg-type]
    )

    response = asyncio.run(
        router.handle_request(
            {
                "type": "SYSTEM_INIT",
                "schema_version": "7.0",
                "symbols_count": 1,
                "symbols": [{"mt5_symbol": "NQ"}],
            }
        )
    )

    assert response["type"] == "SYSTEM_INIT_ACK"
    assert response["status"] == "OK"
    assert {session.market_provider for session in configurator.sessions} == {PROVIDER_CME_LOCAL_DBN}
    assert {session.provider_symbol for session in configurator.sessions} == {"NQ.FUT"}
    assert {session.binance_symbol for session in configurator.sessions} == {""}
    assert all(item["provider_symbol"] == "NQ.FUT" for item in response["symbols"])


def test_cme_study_bootstrap_initializes_local_catalog_symbols() -> None:
    from main import _bootstrap_cme_study_sessions

    registry = SymbolSessionRegistry(
        MarketSymbolResolver(
            cme_resolver=CmeSymbolResolver(
                available_symbols=("NQ.FUT",),
                tick_sizes={"NQ.FUT": "0.25"},
            )
        )
    )
    configurator = _RecordingConfigurator()

    sessions = _bootstrap_cme_study_sessions(
        cme_catalog=_FakeCmeCatalog(("NQ.FUT",)),  # type: ignore[arg-type]
        session_registry=registry,
        absorption_service=configurator,  # type: ignore[arg-type]
    )

    assert registry.is_initialized is True
    assert len(sessions) == len(normalized_study_timeframes())
    assert {session.market_provider for session in configurator.sessions} == {PROVIDER_CME_LOCAL_DBN}
    assert {session.provider_symbol for session in configurator.sessions} == {"NQ.FUT"}
    assert {session.binance_symbol for session in configurator.sessions} == {""}
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
