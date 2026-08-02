from __future__ import annotations

from typing import Any, Protocol
import asyncio
import logging

from core.message_protocol import dumps_message, loads_message
<<<<<<< HEAD
from core.startup_guard import is_address_in_use_error
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

LOGGER = logging.getLogger(__name__)

QUIET_REQUEST_TYPES = {
    "GET_TRADING_EXECUTION_SIGNALS",
    "UPDATE_TRADING_POSITION_STATUS",
    "GET_DURATION_PROFILE",
    "GET_LEVEL_VOLUME_PROFILE",
    "GET_VOLUME_ZSCORE_PROFILE",
}


class RequestHandler(Protocol):
    async def handle_request(self, request_payload: dict[str, Any]) -> dict[str, Any]:
        ...


class TcpJsonServer:
    def __init__(self, host: str, port: int, request_router: RequestHandler) -> None:
        self.host = host
        self.port = port
        self.request_router = request_router

    async def run_forever(self) -> None:
        while True:
            try:
                server = await asyncio.start_server(self._handle_client_connection, host=self.host, port=self.port)
                sockets = server.sockets or []
                for socket_item in sockets:
                    LOGGER.info("PYTHON_TCP_SERVER_LISTENING | address=%s", socket_item.getsockname())
                async with server:
                    await server.serve_forever()
<<<<<<< HEAD
            except OSError as exc:
                if is_address_in_use_error(exc):
                    raise
                LOGGER.exception("PYTHON_TCP_SERVER_RESTARTING | error=%s", exc)
                await asyncio.sleep(5)
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            except Exception as exc:
                LOGGER.exception("PYTHON_TCP_SERVER_RESTARTING | error=%s", exc)
                await asyncio.sleep(5)

    async def _handle_client_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer_name = writer.get_extra_info("peername")
        LOGGER.info("MT5_TCP_CONNECTED | peer=%s", peer_name)
        try:
            while True:
                request_line = await reader.readline()
                if not request_line:
                    break
                request_payload = loads_message(request_line)
                request_type = str(request_payload.get("type", "")).strip().upper()
                log_poll = request_type not in QUIET_REQUEST_TYPES
                if log_poll:
                    LOGGER.info("MT5_REQUEST_RECEIVED | peer=%s | type=%s", peer_name, request_type)
                response_payload = await self.request_router.handle_request(request_payload)
                writer.write(dumps_message(response_payload))
                await writer.drain()
                if log_poll:
                    LOGGER.info("PYTHON_RESPONSE_SENT | peer=%s | type=%s", peer_name, response_payload.get("type", ""))
        except Exception as exc:
            LOGGER.exception("MT5_TCP_CONNECTION_ERROR | peer=%s | error=%s", peer_name, exc)
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            LOGGER.info("MT5_TCP_DISCONNECTED | peer=%s", peer_name)
