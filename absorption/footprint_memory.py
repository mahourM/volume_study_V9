from __future__ import annotations

from collections import OrderedDict
from threading import Lock
from typing import Any

from absorption.models import CandleFootprint


class LatestCandleFootprintMemory:
    def __init__(self, max_candles: int) -> None:
        if max_candles <= 0:
            raise ValueError("max_candles must be positive")
        self.max_candles = max_candles
        self._items: OrderedDict[int, CandleFootprint] = OrderedDict()
        self._lock = Lock()

    def replace_window(self, footprints: list[CandleFootprint]) -> None:
        latest_items = sorted(footprints, key=lambda item: item.open_time_ms)[-self.max_candles :]
        with self._lock:
            self._items = OrderedDict((item.open_time_ms, item) for item in latest_items)

    def snapshot(self) -> list[CandleFootprint]:
        with self._lock:
            return list(self._items.values())

    def to_payload(self) -> list[dict[str, Any]]:
        return [item.to_payload() for item in self.snapshot()]
