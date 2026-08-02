from __future__ import annotations

import asyncio
import json
from decimal import Decimal
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

from absorption.models import BinanceCandle


class BinanceRestClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url.rstrip("/")

    async def get_price_step(self, symbol: str) -> Decimal:
        payload = await self._get_json("/api/v3/exchangeInfo", {"symbol": symbol})
        symbols = payload.get("symbols", [])
        if not symbols:
            raise ValueError(f"Symbol not found on Binance: {symbol}")

        for filter_item in symbols[0].get("filters", []):
            if filter_item.get("filterType") == "PRICE_FILTER":
                return Decimal(str(filter_item["tickSize"]))
        raise ValueError(f"PRICE_FILTER not found for Binance symbol: {symbol}")

    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[BinanceCandle]:
        payload = await self._get_json(
            "/api/v3/klines",
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
            },
        )
        candles: list[BinanceCandle] = []
        for item in payload:
            candles.append(
                BinanceCandle(
                    symbol=symbol,
                    interval=interval,
                    open_time_ms=int(item[0]),
                    open_price=Decimal(str(item[1])),
                    high_price=Decimal(str(item[2])),
                    low_price=Decimal(str(item[3])),
                    close_price=Decimal(str(item[4])),
                    close_time_ms=int(item[6]),
                )
            )
        return candles

    async def get_agg_trades(self, symbol: str, start_time_ms: int, end_time_ms: int) -> list[dict[str, Any]]:
        all_trades: list[dict[str, Any]] = []
        max_window_ms = 60 * 60 * 1000 - 1
        cursor_ms = int(start_time_ms)
        while cursor_ms <= end_time_ms:
            window_end_ms = min(int(end_time_ms), cursor_ms + max_window_ms)
            batch_trades = await self._get_agg_trades_window(symbol, cursor_ms, window_end_ms)
            all_trades.extend(batch_trades)
            cursor_ms = window_end_ms + 1
        return all_trades

    async def _get_agg_trades_window(self, symbol: str, start_time_ms: int, end_time_ms: int) -> list[dict[str, Any]]:
        all_trades: list[dict[str, Any]] = []
        cursor_ms = int(start_time_ms)
        end_ms = int(end_time_ms)
        while cursor_ms <= end_ms:
            batch = await self._get_json(
                "/api/v3/aggTrades",
                {
                    "symbol": symbol,
                    "startTime": cursor_ms,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            if not batch:
                break

            all_trades.extend(batch)
            if len(batch) < 1000:
                break

            next_cursor_ms = int(batch[-1]["T"]) + 1
            if next_cursor_ms <= cursor_ms:
                break
            cursor_ms = next_cursor_ms
        return all_trades

    async def _get_json(self, path: str, params: dict[str, Any]) -> Any:
        url = f"{self.base_url}{path}?{urlencode(params)}"
        return await asyncio.to_thread(self._blocking_get_json, url)

    @staticmethod
    def _blocking_get_json(url: str) -> Any:
        with urlopen(url, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
