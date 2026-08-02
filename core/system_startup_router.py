from __future__ import annotations

import logging
import time
from typing import Any, Protocol

from core.symbol_session_registry import SymbolSessionRegistry
from core.system_models import SymbolSessionState
from core.system_models import SystemInitRequest, SystemInitValidationError

LOGGER = logging.getLogger(__name__)


class AbsorptionSessionConfigurator(Protocol):
    def configure_sessions(self, sessions: list[SymbolSessionState]) -> None:
        ...

    def execution_signal_payloads(
        self,
        mt5_symbol: str | None = None,
        client_name: str = "metatrader",
<<<<<<< HEAD
        primary_timeframe: str = "",
=======
        primary_timeframe: str = "M15",
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    ) -> list[dict[str, Any]]:
        ...

    def execution_command_payloads(
        self,
        mt5_symbol: str | None = None,
        client_name: str = "metatrader",
<<<<<<< HEAD
        primary_timeframe: str = "",
=======
        primary_timeframe: str = "M15",
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    ) -> list[dict[str, Any]]:
        ...

    def update_execution_position_status(self, status_payload: dict[str, Any]) -> dict[str, Any]:
        ...

    def latest_duration_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        ...

    def latest_level_volume_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        ...

    async def level_volume_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        ...

    async def volume_zscore_profile_payload(
        self,
        mt5_symbol: str | None = None,
        timeframe: str | None = None,
    ) -> dict[str, Any] | None:
        ...


class SystemStartupRouter:
    def __init__(
        self,
        session_registry: SymbolSessionRegistry,
        absorption_session_configurator: AbsorptionSessionConfigurator | None = None,
    ) -> None:
        self.session_registry = session_registry
        self.absorption_session_configurator = absorption_session_configurator

    async def handle_request(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request_type = str(request_payload.get("type", "")).strip().upper()
        request_id = self._request_id_from_payload(request_payload)

        if request_type in {"PING", "HEARTBEAT"}:
            return self._build_heartbeat_response()
        if request_type == "SYSTEM_INIT":
            return await self._handle_system_init(request_payload)
        if not self.session_registry.is_initialized:
            return self._build_error_response(
                code="SYSTEM_NOT_INITIALIZED",
                message="Send SYSTEM_INIT from MT5 before requesting market features.",
                request_id=request_id,
            )
        if request_type == "GET_TRADING_EXECUTION_SIGNALS":
            return self._handle_trading_execution_signals(request_payload)
        if request_type == "UPDATE_TRADING_POSITION_STATUS":
            return self._handle_trading_position_status_update(request_payload)
        if request_type == "GET_DURATION_PROFILE":
            return self._handle_duration_profile(request_payload)
        if request_type == "GET_LEVEL_VOLUME_PROFILE":
            return await self._handle_level_volume_profile(request_payload)
        if request_type == "GET_VOLUME_ZSCORE_PROFILE":
            return await self._handle_volume_zscore_profile(request_payload)
        return self._build_error_response(
            code="PHASE_NOT_IMPLEMENTED",
            message="DPC and Absorption paths are initialized as empty shells only.",
            request_id=request_id,
        )

    def _build_heartbeat_response(self) -> dict[str, Any]:
        sessions = self.session_registry.get_all_sessions()
        return {
            "type": "HEARTBEAT",
            "schema_version": "7.0",
            "status": "OK",
            "system_initialized": self.session_registry.is_initialized,
            "prepared_symbols_count": len(sessions),
            "generated_at_utc": int(time.time() * 1000),
        }

    async def _handle_system_init(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request_id = self._request_id_from_payload(request_payload)
        try:
            init_request = SystemInitRequest.from_payload(request_payload)
        except SystemInitValidationError as exc:
            return self._build_error_response(code="INVALID_SYSTEM_INIT", message=str(exc), request_id=request_id)

        sessions = self.session_registry.initialize(init_request)
        if self.absorption_session_configurator is not None:
            self.absorption_session_configurator.configure_sessions(sessions)
        LOGGER.info(
            "MT5_SYSTEM_INIT_RECEIVED | symbols_count=%d | schema_version=%s",
            init_request.symbols_count,
            init_request.schema_version,
        )
        for session in sessions:
            LOGGER.info(
<<<<<<< HEAD
                "MT5_SYMBOL_SESSION | mt5_symbol=%s | timeframe=%s | provider=%s | provider_symbol=%s | binance_symbol=%s | status=%s",
                session.mt5_symbol,
                session.timeframe,
                session.market_provider or "UNRESOLVED",
                session.provider_symbol or "UNRESOLVED",
=======
                "MT5_SYMBOL_SESSION | mt5_symbol=%s | timeframe=%s | binance_symbol=%s | status=%s",
                session.mt5_symbol,
                session.timeframe,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                session.binance_symbol or "UNRESOLVED",
                session.status,
            )
        return self.session_registry.build_system_init_ack(sessions)

    def _handle_trading_execution_signals(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request_id = self._request_id_from_payload(request_payload)
        provider = self.absorption_session_configurator
        if provider is None:
            return self._build_error_response(
                code="SIGNAL_PROVIDER_NOT_READY",
                message="Execution signal provider is not configured.",
                request_id=request_id,
            )

        mt5_symbol_value = request_payload.get("symbol")
        mt5_symbol = mt5_symbol_value if isinstance(mt5_symbol_value, str) and mt5_symbol_value else None
        client_name = str(request_payload.get("client_name", "metatrader")).strip().lower() or "metatrader"
        primary_timeframe = str(request_payload.get("primary_timeframe", "")).strip().upper()
        commands = provider.execution_command_payloads(
            mt5_symbol=mt5_symbol,
            client_name=client_name,
            primary_timeframe=primary_timeframe,
        )
        if commands:
            response: dict[str, Any] = {
                "type": "TRADING_EXECUTION_COMMAND_LIST",
                "client_name": client_name,
            }
            if request_id is not None:
                response["request_id"] = request_id
            if mt5_symbol is not None:
                response["symbol"] = mt5_symbol
            response.update(
                {
                    "primary_timeframe": primary_timeframe,
                    "commands": commands,
                    "generated_at_utc": int(time.time() * 1000),
                }
            )
            return response

        response = {
            "type": "NO_TRADING_EXECUTION_COMMAND",
            "client_name": client_name,
            "primary_timeframe": primary_timeframe,
            "generated_at_utc": int(time.time() * 1000),
        }
        if mt5_symbol is not None:
            response["symbol"] = mt5_symbol
        return self._with_request_identity(response, request_id)

    def _handle_trading_position_status_update(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        request_id = self._request_id_from_payload(request_payload)
        provider = self.absorption_session_configurator
        if provider is None:
            return self._build_error_response(
                code="SIGNAL_PROVIDER_NOT_READY",
                message="Execution position status provider is not configured.",
                request_id=request_id,
            )

        result = provider.update_execution_position_status(request_payload)
        response: dict[str, Any] = {
            "type": "TRADING_POSITION_STATUS_ACK",
            "status": result.get("status", "OK"),
            "lock_updated": bool(result.get("lock_updated", False)),
            "lock_state": result.get("lock_state", ""),
            "generated_at_utc": int(time.time() * 1000),
        }
        if "code" in result:
            response["code"] = result["code"]
        return self._with_request_identity(response, request_id)

    def _handle_duration_profile(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        mt5_symbol_value = request_payload.get("symbol")
        mt5_symbol = mt5_symbol_value if isinstance(mt5_symbol_value, str) and mt5_symbol_value else None
        request_id = self._request_id_from_payload(request_payload)
        timeframe = self._timeframe_from_payload(request_payload)
        provider = self.absorption_session_configurator
        payload = provider.latest_duration_profile_payload(mt5_symbol, timeframe) if provider is not None else None
        if payload is not None:
            return self._with_request_identity(payload, request_id)
        response: dict[str, Any] = {
            "type": "NO_DURATION_PROFILE",
            "generated_at_utc": int(time.time() * 1000),
        }
        if request_id is not None:
            response["request_id"] = request_id
        if mt5_symbol is not None:
            response["symbol"] = mt5_symbol
        if timeframe is not None:
            response["timeframe"] = timeframe
        return response

    async def _handle_level_volume_profile(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        mt5_symbol_value = request_payload.get("symbol")
        mt5_symbol = mt5_symbol_value if isinstance(mt5_symbol_value, str) and mt5_symbol_value else None
        request_id = self._request_id_from_payload(request_payload)
        timeframe = self._timeframe_from_payload(request_payload)
        provider = self.absorption_session_configurator
        payload = await provider.level_volume_profile_payload(mt5_symbol, timeframe) if provider is not None else None
        if payload is not None:
            return self._with_request_identity(payload, request_id)
        response: dict[str, Any] = {
            "type": "NO_LEVEL_VOLUME_PROFILE",
            "generated_at_utc": int(time.time() * 1000),
        }
        if request_id is not None:
            response["request_id"] = request_id
        if mt5_symbol is not None:
            response["symbol"] = mt5_symbol
        if timeframe is not None:
            response["timeframe"] = timeframe
        return response

    async def _handle_volume_zscore_profile(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        mt5_symbol_value = request_payload.get("symbol")
        mt5_symbol = mt5_symbol_value if isinstance(mt5_symbol_value, str) and mt5_symbol_value else None
        request_id = self._request_id_from_payload(request_payload)
        timeframe = self._timeframe_from_payload(request_payload)
        provider = self.absorption_session_configurator
        payload = await provider.volume_zscore_profile_payload(mt5_symbol, timeframe) if provider is not None else None
        if payload is not None:
            return self._with_request_identity(payload, request_id)
        response: dict[str, Any] = {
            "type": "NO_VOLUME_ZSCORE_PROFILE",
            "generated_at_utc": int(time.time() * 1000),
        }
        if request_id is not None:
            response["request_id"] = request_id
        if mt5_symbol is not None:
            response["symbol"] = mt5_symbol
        if timeframe is not None:
            response["timeframe"] = timeframe
        return response

    @staticmethod
    def _build_error_response(code: str, message: str, request_id: str | None = None) -> dict[str, Any]:
        response: dict[str, Any] = {
            "type": "ERROR",
            "schema_version": "7.0",
            "code": code,
            "message": message,
            "generated_at_utc": int(time.time() * 1000),
        }
        if request_id is not None:
            response["request_id"] = request_id
        return response

    @staticmethod
    def _request_id_from_payload(request_payload: dict[str, Any]) -> str | None:
        request_id = request_payload.get("request_id")
        return request_id if isinstance(request_id, str) and request_id else None

    @staticmethod
    def _timeframe_from_payload(request_payload: dict[str, Any]) -> str | None:
        timeframe = request_payload.get("timeframe")
        return timeframe if isinstance(timeframe, str) and timeframe else None

    @staticmethod
    def _with_request_identity(payload: dict[str, Any], request_id: str | None) -> dict[str, Any]:
        response = dict(payload)
        if request_id is not None:
            response["request_id"] = request_id
        return response
