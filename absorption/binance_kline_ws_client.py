from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

try:
    import websockets
except ImportError as exc:
    websockets = None
    WEBSOCKETS_IMPORT_ERROR = exc
else:
    WEBSOCKETS_IMPORT_ERROR = None


LOGGER = logging.getLogger(__name__)

DIRECT_KLINE_TIMEFRAME_BY_INTERNAL = {
    "M1": "1m",
    "M5": "5m",
    "M15": "15m",
    "M30": "30m",
    "H1": "1h",
}

KLINE_INTERVAL_BY_INTERNAL = DIRECT_KLINE_TIMEFRAME_BY_INTERNAL


@dataclass(frozen=True)
class KlineClosedEvent:
    symbol: str
    internal_timeframe: str
    binance_interval: str
    open_time_ms: int
    close_time_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal

    @property
    def range_size(self) -> Decimal:
        return self.high_price - self.low_price
    @property
    def interval(self) -> str:
        return self.binance_interval

class BinanceKlineWebSocketManager:
    def __init__(self, stream_base_url: str = "wss://stream.binance.com:9443/ws") -> None:
        self.stream_base_url = stream_base_url.rstrip("/")
        self._tasks: dict[tuple[str, str], asyncio.Task] = {}
        self._listeners: list[Callable[[KlineClosedEvent], None]] = []

    def add_kline_listener(self, listener: Callable[[KlineClosedEvent], None]) -> None:
        self._listeners.append(listener)

    async def ensure_symbol_timeframe(self, symbol: str, internal_timeframe: str) -> None:
        if internal_timeframe not in KLINE_INTERVAL_BY_INTERNAL:
            return

        normalized_symbol = symbol.upper()
        binance_interval = DIRECT_KLINE_TIMEFRAME_BY_INTERNAL[internal_timeframe]
        key = (normalized_symbol, internal_timeframe)

        if key in self._tasks and not self._tasks[key].done():
            return

        if websockets is None:
            raise RuntimeError("The 'websockets' package is required for Binance kline streaming.") from WEBSOCKETS_IMPORT_ERROR

        self._tasks[key] = asyncio.create_task(
            self._run_symbol_timeframe(
                symbol=normalized_symbol,
                internal_timeframe=internal_timeframe,
                binance_interval=binance_interval,
            ),
            name=f"binance-kline-{normalized_symbol}-{internal_timeframe}",
        )

    async def restart_symbol_timeframes(
        self,
        symbol: str,
        internal_timeframes: set[str] | tuple[str, ...] | list[str],
    ) -> None:
        normalized_symbol = symbol.upper()
        timeframes = [
            timeframe.strip().upper()
            for timeframe in internal_timeframes
            if timeframe.strip().upper() in KLINE_INTERVAL_BY_INTERNAL
        ]
        timeframe_order = list(KLINE_INTERVAL_BY_INTERNAL)

        for internal_timeframe in sorted(set(timeframes), key=lambda item: timeframe_order.index(item)):
            key = (normalized_symbol, internal_timeframe)
            task = self._tasks.pop(key, None)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    LOGGER.exception(
                        "BINANCE_KLINE_WS_RESTART_CANCEL_ERROR | symbol=%s | timeframe=%s",
                        normalized_symbol,
                        internal_timeframe,
                    )

            await self.ensure_symbol_timeframe(normalized_symbol, internal_timeframe)

    async def remove_inactive_symbols(self, active_symbols: set[str]) -> None:
        normalized_active = {item.upper() for item in active_symbols}

        stale_keys = [
            key
            for key in self._tasks
            if key[0] not in normalized_active
        ]

        for key in stale_keys:
            task = self._tasks.pop(key)

            task.cancel()

            try:
                await task
            except asyncio.CancelledError:
                pass

    async def remove_inactive_symbol_timeframes(self, active_symbol_timeframes: set[tuple[str, str]]) -> None:
        normalized_active = {
            (symbol.strip().upper(), timeframe.strip().upper())
            for symbol, timeframe in active_symbol_timeframes
        }

        stale_keys = [
            key
            for key in self._tasks
            if key not in normalized_active
        ]

        for key in stale_keys:
            task = self._tasks.pop(key)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    async def _run_symbol_timeframe(
        self,
        *,
        symbol: str,
        internal_timeframe: str,
        binance_interval: str,
    ) -> None:
        stream_symbol = symbol.lower()
        url = f"{self.stream_base_url}/{stream_symbol}@kline_{binance_interval}"

        while True:
            try:
                await self._stream_symbol_timeframe(
                    url=url,
                    symbol=symbol,
                    internal_timeframe=internal_timeframe,
                    binance_interval=binance_interval,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOGGER.warning(
                    "BINANCE_KLINE_WS_RECONNECT | symbol=%s | timeframe=%s | interval=%s | error=%s",
                    symbol,
                    internal_timeframe,
                    binance_interval,
                    exc,
                )
                await asyncio.sleep(2)

    async def _stream_symbol_timeframe(
        self,
        *,
        url: str,
        symbol: str,
        internal_timeframe: str,
        binance_interval: str,
    ) -> None:
        assert websockets is not None

        async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as websocket:
            LOGGER.info(
                "BINANCE_KLINE_WS_READY | symbol=%s | timeframe=%s | interval=%s",
                symbol,
                internal_timeframe,
                binance_interval,
            )

            async for raw_message in websocket:
                message = json.loads(raw_message)
                event = self._parse_closed_kline(
                    message=message,
                    internal_timeframe=internal_timeframe,
                    binance_interval=binance_interval,
                )

                if event is None:
                    continue

                self._notify_kline_listeners(event)

    def _notify_kline_listeners(self, event: KlineClosedEvent) -> None:
        for listener in tuple(self._listeners):
            try:
                listener(event)
            except Exception:
                LOGGER.exception(
                    "BINANCE_KLINE_LISTENER_ERROR | symbol=%s | timeframe=%s",
                    event.symbol,
                    event.internal_timeframe,
                )

    @staticmethod
    def _parse_closed_kline(
        *,
        message: dict[str, Any],
        internal_timeframe: str,
        binance_interval: str,
    ) -> KlineClosedEvent | None:
        kline = message.get("k")

        if not isinstance(kline, dict):
            return None

        if not bool(kline.get("x", False)):
            return None

        return KlineClosedEvent(
            symbol=str(kline["s"]).upper(),
            internal_timeframe=internal_timeframe,
            binance_interval=binance_interval,
            open_time_ms=int(kline["t"]),
            close_time_ms=int(kline["T"]),
            open_price=Decimal(str(kline["o"])),
            high_price=Decimal(str(kline["h"])),
            low_price=Decimal(str(kline["l"])),
            close_price=Decimal(str(kline["c"])),
        )
