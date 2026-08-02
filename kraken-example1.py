from __future__ import annotations

import asyncio
import json
import time
import traceback
from typing import Awaitable, Callable

try:
    import websockets
except Exception:
    websockets = None

from .base import ExchangeClient, MarketEvent


class KrakenClient(ExchangeClient):
    name = "kraken"

    async def run_symbol(
        self,
        symbol: str,
        on_event: Callable[[MarketEvent], Awaitable[None]],
        stop_flag,
    ) -> None:
        if websockets is None:
            raise RuntimeError("Missing dependency: websockets (pip install websockets)")

        url = "wss://ws.kraken.com"

        mapped_symbol = symbol
        if mapped_symbol.upper() == "BTCUSD":
            mapped_symbol = "XBT/USD"

        sub_trade = {"event": "subscribe", "pair": [mapped_symbol], "subscription": {"name": "trade"}}
        sub_book = {"event": "subscribe", "pair": [mapped_symbol], "subscription": {"name": "book", "depth": 10}}

        backoff = 0.5
        while not stop_flag():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_trade))
                    await ws.send(json.dumps(sub_book))
                    backoff = 0.5

                    printed_first_dict = False
                    printed_first_list = False

                    while not stop_flag():
                        msg = await ws.recv()
                        data = json.loads(msg)

                        if isinstance(data, dict):
                            if not printed_first_dict:
                                printed_first_dict = True
                                print(f"[KRAKEN_FIRST_DICT] {data}")

                            if data.get("event") == "subscriptionStatus":
                                print(
                                    f"[KRAKEN_SUB] status={data.get('status')} pair={data.get('pair')} sub={data.get('subscription')}"
                                )
                            now = time.time()
                            await on_event(MarketEvent(self.name, symbol, "PING", now, {"event": data.get("event")}))
                            continue

                        if not isinstance(data, list):
                            continue

                        if (not printed_first_list) and len(data) >= 4:
                            printed_first_list = True
                            print(f"[KRAKEN_FIRST_LIST] {data[:4]}")

                        now = time.time()

                        if len(data) < 3:
                            continue

                        channel = data[-2]
                        if not isinstance(channel, str):
                            continue

                        if channel == "trade":
                            trades = data[1]
                            if isinstance(trades, list):
                                for t in trades:
                                    if not isinstance(t, list) or len(t) < 4:
                                        continue
                                    await on_event(
                                        MarketEvent(
                                            self.name,
                                            symbol,
                                            "TRADE",
                                            now,
                                            {
                                                "price": float(t[0]),
                                                "qty": float(t[1]),
                                                "side": "BUY" if t[3] == "b" else "SELL",
                                            },
                                        )
                                    )

                        elif channel.startswith("book"):
                            book = data[1]
                            if not isinstance(book, dict):
                                continue
                            # Kraken snapshot uses "as"/"bs", updates often use "a"/"b"
                            asks = book.get("a") or book.get("as") or []
                            bids = book.get("b") or book.get("bs") or []
                            if not bids or not asks:
                                continue
                                

                            if not isinstance(bids[0], list) or not isinstance(asks[0], list):
                                continue
                            if not hasattr(self, "_printed_first_book_top"):
                                self._printed_first_book_top = True
                                print(f"[KRAKEN_FIRST_BOOK_TOP] bid={bids[0][0]} ask={asks[0][0]}")
                            await on_event(
                                MarketEvent(
                                    self.name,
                                    symbol,
                                    "BOOK_TOP",
                                    now,
                                    {
                                        "bid": float(bids[0][0]),
                                        "bid_qty": float(bids[0][1]),
                                        "ask": float(asks[0][0]),
                                        "ask_qty": float(asks[0][1]),
                                    },
                                )
                            )

            except Exception as e:
                print(f"[WS_ERROR] ex=kraken err={type(e).__name__} detail={e}")
                print(traceback.format_exc())
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, 8.0)