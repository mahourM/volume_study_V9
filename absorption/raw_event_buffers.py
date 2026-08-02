from __future__ import annotations

from dataclasses import dataclass, field

from absorption.binance_aggtrade_ws_client import AggTradeEvent

BASE_BUCKET_MS = 60_000
TIMEFRAME_ORDER = ("M1", "M5", "M15", "M30", "H1")


@dataclass
class _RawEventBucket:
    trade_events: list[AggTradeEvent] = field(default_factory=list)


@dataclass
class _TimeframeRawEventStore:
    buckets: dict[int, _RawEventBucket] = field(default_factory=dict)
    trade_event_count: int = 0


@dataclass
class _SymbolRawEventStore:
    timeframe_stores: dict[str, _TimeframeRawEventStore] = field(default_factory=dict)
    active_timeframes: set[str] = field(default_factory=set)


class RawMarketEventBuffer:
    def __init__(self) -> None:
        self._stores: dict[str, _SymbolRawEventStore] = {}

    def configure_symbol_timeframes(self, symbol: str, timeframes: set[str]) -> None:
        store = self._store_for(symbol)
        normalized_timeframes = {timeframe.strip().upper() for timeframe in timeframes if timeframe.strip()}
        store.active_timeframes = normalized_timeframes
        for timeframe in list(store.timeframe_stores):
            if timeframe not in normalized_timeframes:
                store.timeframe_stores.pop(timeframe, None)
        for timeframe in normalized_timeframes:
            store.timeframe_stores.setdefault(timeframe, _TimeframeRawEventStore())

    def append_trade_event(self, event: AggTradeEvent) -> None:
        store = self._store_for(event.symbol)
        for timeframe in tuple(store.active_timeframes):
            timeframe_store = store.timeframe_stores.setdefault(timeframe, _TimeframeRawEventStore())
            bucket = self._bucket_for_timeframe_store(timeframe_store, event.event_time_ms)
            bucket.trade_events.append(event)
            timeframe_store.trade_event_count += 1

    def snapshot_trade_events(self, symbol: str, timeframe: str, open_time_ms: int, close_time_ms: int) -> list[AggTradeEvent]:
        store = self._stores.get(symbol.upper())
        if store is None:
            return []
        timeframe_store = store.timeframe_stores.get(timeframe.strip().upper())
        if timeframe_store is None:
            return []
        events: list[AggTradeEvent] = []
        for bucket in self._bucket_range(open_time_ms, close_time_ms):
            raw_bucket = timeframe_store.buckets.get(bucket)
            if raw_bucket is not None:
                events.extend(raw_bucket.trade_events)
        return sorted(
            (
                event
                for event in events
                if int(open_time_ms) <= int(event.event_time_ms) < int(close_time_ms)
            ),
            key=lambda event: (int(event.event_time_ms), int(getattr(event, "aggregate_trade_id", 0))),
        )

    def pending_trade_event_count(self, symbol: str | None = None, timeframe: str | None = None) -> int:
        normalized_timeframe = timeframe.strip().upper() if timeframe is not None else None
        if symbol is not None:
            store = self._stores.get(symbol.upper())
            if store is None:
                return 0
            if normalized_timeframe is not None:
                timeframe_store = store.timeframe_stores.get(normalized_timeframe)
                return timeframe_store.trade_event_count if timeframe_store is not None else 0
            return sum(timeframe_store.trade_event_count for timeframe_store in store.timeframe_stores.values())
        if normalized_timeframe is not None:
            return sum(
                timeframe_store.trade_event_count
                for store in self._stores.values()
                for store_timeframe, timeframe_store in store.timeframe_stores.items()
                if store_timeframe == normalized_timeframe
            )
        return sum(
            timeframe_store.trade_event_count
            for store in self._stores.values()
            for timeframe_store in store.timeframe_stores.values()
        )

    def oldest_retained_event_time_ms(self, symbol: str | None = None, timeframe: str | None = None) -> int | None:
        normalized_timeframe = timeframe.strip().upper() if timeframe is not None else None
        stores = [self._stores[symbol.upper()]] if symbol is not None and symbol.upper() in self._stores else []
        if symbol is None:
            stores = list(self._stores.values())
        oldest: int | None = None
        for store in stores:
            for store_timeframe, timeframe_store in store.timeframe_stores.items():
                if normalized_timeframe is not None and store_timeframe != normalized_timeframe:
                    continue
                for bucket_time, bucket in timeframe_store.buckets.items():
                    if not bucket.trade_events:
                        continue
                    oldest = bucket_time if oldest is None else min(oldest, bucket_time)
        return oldest

    def retention_blocking_timeframe(self, symbol: str | None = None, timeframe: str | None = None) -> str | None:
        normalized_timeframe = timeframe.strip().upper() if timeframe is not None else None
        stores = [self._stores[symbol.upper()]] if symbol is not None and symbol.upper() in self._stores else []
        if symbol is None:
            stores = list(self._stores.values())
        if not stores:
            return None
        blocking_timeframe: str | None = None
        blocking_bucket_time: int | None = None
        for store in stores:
            for store_timeframe, timeframe_store in store.timeframe_stores.items():
                if normalized_timeframe is not None and store_timeframe != normalized_timeframe:
                    continue
                for bucket_time, bucket in timeframe_store.buckets.items():
                    if not bucket.trade_events:
                        continue
                    if blocking_bucket_time is None or bucket_time < blocking_bucket_time:
                        blocking_bucket_time = bucket_time
                        blocking_timeframe = store_timeframe
                    elif bucket_time == blocking_bucket_time and blocking_timeframe is not None:
                        blocking_timeframe = max((blocking_timeframe, store_timeframe), key=self._timeframe_rank)
        return blocking_timeframe

    def mark_timeframe_processed(
        self,
        symbol: str,
        timeframe: str,
        open_time_ms: int,
        close_time_ms: int,
    ) -> None:
        store = self._stores.get(symbol.upper())
        if store is None:
            return
        normalized_timeframe = timeframe.strip().upper()
        timeframe_store = store.timeframe_stores.get(normalized_timeframe)
        if timeframe_store is None:
            return
        for bucket_time in self._bucket_range(open_time_ms, close_time_ms):
            bucket = timeframe_store.buckets.pop(bucket_time, None)
            if bucket is None:
                continue
            timeframe_store.trade_event_count -= len(bucket.trade_events)
        if timeframe_store.trade_event_count < 0:
            timeframe_store.trade_event_count = 0

    def remove_inactive_symbols(self, active_symbols: set[str]) -> None:
        normalized_active = {symbol.upper() for symbol in active_symbols}
        for symbol in list(self._stores):
            if symbol not in normalized_active:
                self._stores.pop(symbol, None)

    def _store_for(self, symbol: str) -> _SymbolRawEventStore:
        return self._stores.setdefault(symbol.upper(), _SymbolRawEventStore())

    def _bucket_for_timeframe_store(self, store: _TimeframeRawEventStore, event_time_ms: int) -> _RawEventBucket:
        bucket_time = self._bucket_open_time_ms(event_time_ms)
        return store.buckets.setdefault(bucket_time, _RawEventBucket())

    @staticmethod
    def _bucket_open_time_ms(event_time_ms: int) -> int:
        return (int(event_time_ms) // BASE_BUCKET_MS) * BASE_BUCKET_MS

    @classmethod
    def _bucket_range(cls, open_time_ms: int, close_time_ms: int) -> range:
        start_bucket = cls._bucket_open_time_ms(open_time_ms)
        end_bucket = cls._bucket_open_time_ms(max(int(open_time_ms), int(close_time_ms) - 1))
        return range(start_bucket, end_bucket + BASE_BUCKET_MS, BASE_BUCKET_MS)

    @staticmethod
    def _timeframe_rank(timeframe: str) -> int:
        normalized_timeframe = timeframe.strip().upper()
        if normalized_timeframe in TIMEFRAME_ORDER:
            return TIMEFRAME_ORDER.index(normalized_timeframe)
        return len(TIMEFRAME_ORDER)
