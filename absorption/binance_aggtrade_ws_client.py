from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable

try:
    import websockets
except ImportError as exc:  # pragma: no cover
    websockets = None
    _WEBSOCKETS_IMPORT_ERROR = exc
else:
    _WEBSOCKETS_IMPORT_ERROR = None

LOGGER = logging.getLogger(__name__)


@dataclass
class SymbolAggTradeState:
    symbol: str
    recent_trades: list[AggTradeEvent] = field(default_factory=list)
    recent_price_events: list[tuple[int, Decimal]] = field(default_factory=list)
    last_event_time_ms: int = 0
    ready_event: asyncio.Event = field(default_factory=asyncio.Event)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    task: asyncio.Task | None = None


@dataclass(frozen=True)
class AggTradeEvent:
    symbol: str
    event_time_ms: int
    price: Decimal
    quantity: Decimal
    side: str
    aggregate_trade_id: int = 0


class BinanceAggTradeWebSocketManager:
    def __init__(self, stream_base_url: str = "wss://stream.binance.com:9443/ws") -> None:
        self.stream_base_url = stream_base_url.rstrip("/")
        self._states: dict[str, SymbolAggTradeState] = {}
        self._trade_listeners: list[Callable[[AggTradeEvent], None]] = []
        

    def add_trade_listener(self, listener: Callable[[AggTradeEvent], None]) -> None:
        self._trade_listeners.append(listener)

    

    async def remove_inactive_symbols(self, active_symbols: set[str]) -> None:
        normalized_active = {item.upper() for item in active_symbols}
        stale_symbols = [symbol for symbol in self._states if symbol not in normalized_active]
        for symbol in stale_symbols:
            state = self._states.pop(symbol)
            if state.task is not None:
                state.task.cancel()
                try:
                    await state.task
                except asyncio.CancelledError:
                    pass


    async def ensure_symbol(
        self,
        symbol: str,
    ) -> None:
        normalized_symbol = symbol.upper()
        state = self._states.get(normalized_symbol)

        if state is None:
            state = SymbolAggTradeState(symbol=normalized_symbol)
            self._states[normalized_symbol] = state

        if state.task is None or state.task.done():
            if websockets is None:
                raise RuntimeError(
                    "The 'websockets' package is required for Binance aggTrade streaming."
                ) from _WEBSOCKETS_IMPORT_ERROR

            state.task = asyncio.create_task(
                self._run_symbol(state),
                name=f"binance-aggtrade-{normalized_symbol}",
            )

    async def snapshot_price_events_for_time_range(
        self,
        symbol: str,
        open_time_ms: int,
        close_time_ms: int,
    ) -> list[tuple[int, Decimal]] | None:

        state = self._states.get(symbol.upper())

        if state is None:
            return []

        if not state.ready_event.is_set():
            return None

        async with state.lock:
            return [
                (event_time_ms, price)
                for event_time_ms, price in state.recent_price_events
                if open_time_ms <= event_time_ms < close_time_ms
            ]

    async def snapshot_price_events_with_previous_for_time_range(
        self,
        symbol: str,
        open_time_ms: int,
        close_time_ms: int,
    ) -> list[tuple[int, Decimal]] | None:

        state = self._states.get(symbol.upper())

        if state is None:
            return []

        if not state.ready_event.is_set():
            return None

        previous_event: tuple[int, Decimal] | None = None
        range_events: list[tuple[int, Decimal]] = []

        async with state.lock:
            for event_time_ms, price in state.recent_price_events:
                if event_time_ms < open_time_ms:
                    if previous_event is None or event_time_ms > previous_event[0]:
                        previous_event = (event_time_ms, price)
                    continue
                if open_time_ms <= event_time_ms < close_time_ms:
                    range_events.append((event_time_ms, price))

        if previous_event is not None:
            range_events.append(previous_event)

        range_events.sort(key=lambda item: item[0])
        return range_events

    async def _run_symbol(self, state: SymbolAggTradeState) -> None:
        stream_symbol = state.symbol.lower()
        url = f"{self.stream_base_url}/{stream_symbol}@aggTrade"

        while True:
            try:
                await self._stream_symbol(state, url)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                state.ready_event.clear()
                LOGGER.warning("BINANCE_AGGTRADE_WS_RECONNECT | symbol=%s | error=%s", state.symbol, exc)
                await asyncio.sleep(2)

    async def _stream_symbol(self, state: SymbolAggTradeState, url: str) -> None:
        assert websockets is not None
        async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as websocket:
            state.ready_event.set()
            LOGGER.info("BINANCE_AGGTRADE_WS_READY | symbol=%s", state.symbol)
            async for raw_message in websocket:
                event = json.loads(raw_message)
                await self._apply_agg_trade_event(state, event)

    async def _apply_agg_trade_event(
        self,
        state: SymbolAggTradeState,
        event: dict[str, Any],
    ) -> None:
        try:
            price = Decimal(str(event["p"]))
            quantity = Decimal(str(event["q"]))
        except (KeyError, ValueError):
            return

        event_time_ms = int(event.get("T") or event.get("E") or 0)

        trade_side = "sell" if bool(event.get("m")) else "buy"

        emitted_event = AggTradeEvent(
            symbol=state.symbol,
            event_time_ms=event_time_ms,
            price=price,
            quantity=quantity,
            side=trade_side,
            aggregate_trade_id=int(event.get("a", 0) or 0),
        )

        async with state.lock:
            state.recent_trades.append(emitted_event)

            state.recent_price_events.append(
                (
                    event_time_ms,
                    price,
                )
            )

            max_trade_memory = 200_000

            if len(state.recent_trades) > max_trade_memory:
                del state.recent_trades[:50_000]

            if len(state.recent_price_events) > max_trade_memory:
                del state.recent_price_events[:50_000]

            state.last_event_time_ms = event_time_ms

        self._notify_trade_listeners(emitted_event)

    async def snapshot_trades_for_time_range(
        self,
        symbol: str,
        open_time_ms: int,
        close_time_ms: int,
    ) -> list[AggTradeEvent]:

        state = self._states.get(symbol.upper())

        if state is None:
            return []

        if not state.ready_event.is_set():
            return []

        async with state.lock:
            return [
                trade
                for trade in state.recent_trades
                if open_time_ms <= trade.event_time_ms < close_time_ms
            ]

    async def websocket_trades_for_time_range(
        self,
        symbol: str,
        open_time_ms: int,
        close_time_ms: int,
    ) -> list[AggTradeEvent] | None:
        state = self._states.get(symbol.upper())

        if state is None:
            return []

        if not state.ready_event.is_set():
            return None

        async with state.lock:
            return [
                trade
                for trade in state.recent_trades
                if open_time_ms <= trade.event_time_ms < close_time_ms
            ]

    def _notify_trade_listeners(self, event: AggTradeEvent) -> None:
        for listener in tuple(self._trade_listeners):
            try:
                listener(event)
            except Exception:
                LOGGER.exception(
                    "BINANCE_AGGTRADE_LISTENER_ERROR | symbol=%s",
                    event.symbol,
                )
