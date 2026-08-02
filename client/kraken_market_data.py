from __future__ import annotations

import asyncio
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Awaitable, Callable

from client.kraken_config import KrakenClientConfig

try:
    import websockets
except Exception as exc:
    websockets = None
    WEBSOCKETS_IMPORT_ERROR = exc
else:
    WEBSOCKETS_IMPORT_ERROR = None


@dataclass(frozen=True)
class KrakenMarketEvent:
    symbol_name: str
    event_type: str
    event_time: float
    payload: dict[str, Decimal | str]


@dataclass(frozen=True)
class KrakenClosedCandle:
    open_time_utc_ms: int
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal


KRAKEN_INTERVAL_MINUTES_BY_TIMEFRAME = {
    "M1": 1,
    "M5": 5,
    "M15": 15,
    "M30": 30,
    "H1": 60,
}


def calculate_manual_atr(candles: list[KrakenClosedCandle], period: int) -> Decimal:
    if period <= 0 or len(candles) < period + 1:
        return Decimal("0")

    true_ranges: list[Decimal] = []
    recent_candles = candles[-period:]
    previous_candles = candles[-period - 1 : -1]
    for candle, previous_candle in zip(recent_candles, previous_candles):
        high_low = candle.high_price - candle.low_price
        high_previous_close = abs(candle.high_price - previous_candle.close_price)
        low_previous_close = abs(candle.low_price - previous_candle.close_price)
        true_ranges.append(max(high_low, high_previous_close, low_previous_close))

    if len(true_ranges) != period:
        return Decimal("0")
    return sum(true_ranges, Decimal("0")) / Decimal(period)


class KrakenMarketDataClient:
    def __init__(self, config: KrakenClientConfig) -> None:
        self.config = config

    def kraken_pair_for_symbol(self, symbol_name: str) -> str:
        override = self.config.symbol_pair_overrides.get(symbol_name)
        if override:
            return override
        return symbol_name

    async def get_source_closed_candle(
        self,
        symbol_name: str,
        timeframe: str,
        source_candle_open_time_utc_ms: int,
    ) -> KrakenClosedCandle | None:
        candles = await self.get_closed_candles(
            symbol_name=symbol_name,
            timeframe=timeframe,
            since_utc_ms=int(source_candle_open_time_utc_ms),
            limit=4,
        )
        for candle in candles:
            if int(candle.open_time_utc_ms) == int(source_candle_open_time_utc_ms):
                return candle
        return None

    async def get_recent_closed_candles(
        self,
        symbol_name: str,
        timeframe: str,
        limit: int,
    ) -> list[KrakenClosedCandle]:
        return await self.get_closed_candles(
            symbol_name=symbol_name,
            timeframe=timeframe,
            since_utc_ms=None,
            limit=limit,
        )

    async def get_closed_candles(
        self,
        *,
        symbol_name: str,
        timeframe: str,
        since_utc_ms: int | None,
        limit: int,
    ) -> list[KrakenClosedCandle]:
        return await asyncio.to_thread(
            self._blocking_get_closed_candles,
            symbol_name,
            timeframe,
            since_utc_ms,
            limit,
        )

    def _blocking_get_closed_candles(
        self,
        symbol_name: str,
        timeframe: str,
        since_utc_ms: int | None,
        limit: int,
    ) -> list[KrakenClosedCandle]:
        interval = self._kraken_interval_for_timeframe(timeframe)
        pair = self.kraken_pair_for_symbol(symbol_name)
        query: dict[str, str] = {
            "pair": pair,
            "interval": str(interval),
        }
        if since_utc_ms is not None:
            query["since"] = str(max(0, int(since_utc_ms) // 1000 - (interval * 60)))
        url = f"{self.config.rest_base_url.rstrip('/')}/0/public/OHLC?{urllib.parse.urlencode(query)}"
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        result = payload.get("result", {})
        if not isinstance(result, dict):
            return []
        rows: list[Any] = []
        for key, value in result.items():
            if key == "last":
                continue
            if isinstance(value, list):
                rows = value
                break
        candles = [self._parse_ohlc_row(row) for row in rows]
        now_ms = int(time.time() * 1000)
        interval_ms = interval * 60 * 1000
        parsed = [
            candle
            for candle in candles
            if candle is not None and candle.open_time_utc_ms + interval_ms <= now_ms
        ]
        return parsed[-max(1, int(limit)) :]

    @staticmethod
    def _kraken_interval_for_timeframe(timeframe: str) -> int:
        normalized = timeframe.strip().upper()
        if normalized not in KRAKEN_INTERVAL_MINUTES_BY_TIMEFRAME:
            raise ValueError(f"Unsupported Kraken timeframe: {timeframe}")
        return KRAKEN_INTERVAL_MINUTES_BY_TIMEFRAME[normalized]

    @staticmethod
    def _parse_ohlc_row(row: object) -> KrakenClosedCandle | None:
        if not isinstance(row, list) or len(row) < 5:
            return None
        try:
            return KrakenClosedCandle(
                open_time_utc_ms=int(Decimal(str(row[0])) * Decimal("1000")),
                open_price=Decimal(str(row[1])),
                high_price=Decimal(str(row[2])),
                low_price=Decimal(str(row[3])),
                close_price=Decimal(str(row[4])),
            )
        except Exception:
            return None

    async def run_symbol(
        self,
        symbol_name: str,
        on_event: Callable[[KrakenMarketEvent], Awaitable[None]],
        stop_flag: Callable[[], bool],
    ) -> None:
        if websockets is None:
            raise RuntimeError("The websockets package is required for Kraken WebSocket data") from WEBSOCKETS_IMPORT_ERROR

        pair = self.kraken_pair_for_symbol(symbol_name)
        subscriptions = (
            {"event": "subscribe", "pair": [pair], "subscription": {"name": "trade"}},
        )

        backoff_seconds = 0.5
        while not stop_flag():
            try:
                async with websockets.connect(self.config.public_ws_url, ping_interval=20, ping_timeout=20) as ws:
                    for subscription in subscriptions:
                        await ws.send(json.dumps(subscription))
                    backoff_seconds = 0.5

                    while not stop_flag():
                        raw_message = await ws.recv()
                        data = json.loads(raw_message)
                        if isinstance(data, dict):
                            continue
                        if not isinstance(data, list) or len(data) < 4:
                            continue

                        channel = data[-2]
                        if not isinstance(channel, str):
                            continue

                        event_time = time.time()
                        if channel == "trade":
                            await self._emit_trade_events(symbol_name, event_time, data[1], on_event)
            except Exception:
                await asyncio.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 8)

    async def _emit_trade_events(
        self,
        symbol_name: str,
        event_time: float,
        trades: object,
        on_event: Callable[[KrakenMarketEvent], Awaitable[None]],
    ) -> None:
        if not isinstance(trades, list):
            return
        for trade in trades:
            if not isinstance(trade, list) or len(trade) < 4:
                continue
            await on_event(
                KrakenMarketEvent(
                    symbol_name=symbol_name,
                    event_type="TRADE",
                    event_time=event_time,
                    payload={
                        "price": Decimal(str(trade[0])),
                        "quantity": Decimal(str(trade[1])),
                        "side": "BUY" if trade[3] == "b" else "SELL",
                    },
                )
            )
