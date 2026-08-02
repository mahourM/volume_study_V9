from __future__ import annotations

import heapq
import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from DOM.models import DomRawEvent
from cme_provider.engines import _footprint_candles_from_frame
from cme_provider.local_data import CmeLocalDataCatalog, CmeLocalDbnTradeStore
from core.timeframe_policy import TIMEFRAME_MS_BY_NAME
from process.models import ProcessSymbol


class ProcessEventSource(Protocol):
    def symbols(self) -> tuple[ProcessSymbol, ...]:
        ...

    def events(
        self,
        symbol: ProcessSymbol,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterable[DomRawEvent]:
        ...


class ProcessFootprintSource(Protocol):
    def symbols(self) -> tuple[ProcessSymbol, ...]:
        ...

    def candles(
        self,
        symbol: ProcessSymbol,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterable[dict[str, Any]]:
        ...


@dataclass(frozen=True)
class InMemoryProcessEventSource:
    symbol_events: dict[ProcessSymbol, tuple[DomRawEvent, ...]]

    def symbols(self) -> tuple[ProcessSymbol, ...]:
        return tuple(self.symbol_events)

    def events(
        self,
        symbol: ProcessSymbol,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterable[DomRawEvent]:
        return tuple(
            event
            for event in self.symbol_events.get(symbol, ())
            if int(start_ms) <= int(event.ts_event_ms) <= int(end_ms)
        )


@dataclass(frozen=True)
class InMemoryProcessFootprintSource:
    symbol_candles: dict[ProcessSymbol, tuple[dict[str, Any], ...]]

    def symbols(self) -> tuple[ProcessSymbol, ...]:
        return tuple(self.symbol_candles)

    def candles(
        self,
        symbol: ProcessSymbol,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterable[dict[str, Any]]:
        return tuple(
            candle
            for candle in self.symbol_candles.get(symbol, ())
            if int(start_ms) <= int(candle.get("open_time_ms", 0)) < int(end_ms)
        )


class DomDatabentoReplaySource:
    def __init__(
        self,
        *,
        index_path: Path,
        dataset: str,
        schema: str = "mbo",
        market_provider: str = "CME_LOCAL_DBN",
        default_tick_size: Decimal | str = Decimal("0.25"),
        timeframe: str = "M1",
        interval: str = "1m",
        batch_size: int = 25_000,
        footprint_score_min: Decimal | str | None = None,
    ) -> None:
        self.index_path = index_path
        self.dataset = dataset
        self.schema = schema
        self.market_provider = market_provider
        self.default_tick_size = Decimal(str(default_tick_size))
        self.timeframe = timeframe.strip().upper()
        self.interval = interval
        self.batch_size = max(1, int(batch_size))
        self.footprint_score_min = (
            None
            if footprint_score_min is None
            else Decimal(str(footprint_score_min))
        )
        self._footprint_price_ranges: dict[
            tuple[str, int], tuple[tuple[Decimal, Decimal], ...]
        ] | None = None

    def restrict_events_to_footprints(
        self,
        snapshots: Iterable[Any],
        *,
        full_range_before_ms: int | None = None,
    ) -> None:
        ranges: dict[
            tuple[str, int], tuple[tuple[Decimal, Decimal], ...]
        ] = {}
        for snapshot in snapshots:
            symbol = getattr(snapshot, "symbol", None)
            provider_symbol = str(
                getattr(symbol, "provider_symbol", "") or ""
            ).strip().upper()
            if not provider_symbol:
                continue
            for candle in getattr(snapshot, "candles", ()):
                try:
                    open_time_ms = int(
                        candle.get("open_time_ms")
                        or candle.get("open_time")
                        or 0
                    )
                except Exception:
                    continue
                apply_score_min = (
                    self.footprint_score_min is not None
                    and (
                        full_range_before_ms is None
                        or open_time_ms >= int(full_range_before_ms)
                    )
                )
                lows: list[Decimal] = []
                highs: list[Decimal] = []
                selected_ranges: list[tuple[Decimal, Decimal]] = []
                for footprint_bin in candle.get("bins", ()):
                    try:
                        raw_score = footprint_bin.get("contract_spike_score")
                        if raw_score in {None, ""}:
                            raw_score = footprint_bin.get("l2", {}).get(
                                "contract_spike_score"
                            )
                        score = Decimal(str(raw_score or 0))
                        low = Decimal(str(
                            footprint_bin.get("low")
                            or footprint_bin.get("bin_low")
                            or footprint_bin.get("price")
                        ))
                        high = Decimal(str(
                            footprint_bin.get("high")
                            or footprint_bin.get("bin_high")
                            or low
                        ))
                    except Exception:
                        continue
                    if apply_score_min and score < self.footprint_score_min:
                        continue
                    lows.append(low)
                    highs.append(high)
                    selected_ranges.append((low, high))
                if lows and highs:
                    ranges[(provider_symbol, open_time_ms)] = (
                        tuple(selected_ranges)
                        if apply_score_min
                        else ((min(lows), max(highs)),)
                    )
        self._footprint_price_ranges = ranges

    def symbols(self) -> tuple[ProcessSymbol, ...]:
        with self._connection() as connection:
            rows = connection.execute(
                """
                SELECT provider_symbol, GROUP_CONCAT(contract_symbols)
                FROM dom_sources
                WHERE provider_symbol != ''
                  AND (status = 'READY' OR latest_event_time_ms > 0)
                GROUP BY provider_symbol
                ORDER BY provider_symbol
                """
            ).fetchall()
        symbols: list[ProcessSymbol] = []
        for provider_symbol, contract_symbols_text in rows:
            normalized_provider = str(provider_symbol or "").strip().upper()
            if not normalized_provider:
                continue
            contract_symbols = tuple(
                dict.fromkeys(
                    item.strip()
                    for chunk in str(contract_symbols_text or "").split(",")
                    for item in chunk.split(",")
                    if item.strip()
                )
            )
            symbols.append(
                ProcessSymbol(
                    provider_symbol=normalized_provider,
                    mt5_symbol=_mt5_symbol_from_provider(normalized_provider),
                    market_provider=self.market_provider,
                    dataset=self.dataset,
                    schema=self.schema,
                    tick_size=self.default_tick_size,
                    timeframe=self.timeframe,
                    interval=self.interval,
                    contract_symbols=contract_symbols,
                )
            )
        return tuple(symbols)

    def events(
        self,
        symbol: ProcessSymbol,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterable[DomRawEvent]:
        normalized_provider = str(symbol.provider_symbol or "").strip().upper()
        if not normalized_provider:
            return tuple()

        def iterator() -> Iterable[DomRawEvent]:
            with self._connection() as connection:
                interval_ms = int(
                    TIMEFRAME_MS_BY_NAME.get(self.timeframe, 60_000)
                )
                filtered_windows = (
                    tuple(
                        sorted(
                            (
                                max(int(start_ms), int(open_time_ms)),
                                min(
                                    int(end_ms),
                                    int(open_time_ms) + interval_ms - 1,
                                ),
                                price_ranges,
                            )
                            for (provider_symbol, open_time_ms), price_ranges
                            in self._footprint_price_ranges.items()
                            if provider_symbol == normalized_provider
                            and int(open_time_ms) <= int(end_ms)
                            and int(open_time_ms) + interval_ms > int(start_ms)
                        )
                    )
                    if self._footprint_price_ranges is not None
                    else ()
                )
                source_keys = tuple(
                    str(row[0])
                    for row in connection.execute(
                        """
                        SELECT source_key
                        FROM dom_sources
                        WHERE UPPER(provider_symbol) = ?
                          AND (status = 'READY' OR latest_event_time_ms > 0)
                          AND latest_event_time_ms >= ?
                          AND earliest_event_time_ms <= ?
                        ORDER BY earliest_event_time_ms, source_file
                        """,
                        (normalized_provider, int(start_ms), int(end_ms)),
                    ).fetchall()
                )
                if not source_keys:
                    return
                source_buffers: dict[str, list[tuple[Any, ...]]] = {}
                source_positions: dict[str, int] = {}
                source_cursors: dict[str, tuple[int, int]] = {
                    source_key: (int(start_ms) - 1, -1)
                    for source_key in source_keys
                }
                source_window_positions: dict[str, int] = {
                    source_key: 0 for source_key in source_keys
                }
                heap: list[tuple[int, str, int, tuple[Any, ...]]] = []
                seen_timestamp: int | None = None
                signature_sources: dict[
                    tuple[int, str, int, str, str, str, int], str
                ] = {}

                def fetch_batch(source_key: str) -> bool:
                    last_ts_event_ms, last_ordinal = source_cursors[source_key]
                    if self._footprint_price_ranges is None:
                        rows = connection.execute(
                            """
                            SELECT source_key, ordinal, ts_event_ms, price, size,
                                   side, action, order_id, instrument_id
                            FROM dom_events
                            WHERE source_key = ?
                              AND ts_event_ms >= ?
                              AND ts_event_ms <= ?
                              AND (
                                ts_event_ms > ?
                                OR (
                                  ts_event_ms = ?
                                  AND ordinal > ?
                                )
                              )
                            ORDER BY ts_event_ms, ordinal
                            LIMIT ?
                            """,
                            (
                                source_key,
                                int(start_ms),
                                int(end_ms),
                                last_ts_event_ms,
                                last_ts_event_ms,
                                last_ordinal,
                                self.batch_size,
                            ),
                        ).fetchall()
                    else:
                        rows = ()
                        while source_window_positions[source_key] < len(filtered_windows):
                            window_start_ms, window_end_ms, price_ranges = (
                                filtered_windows[source_window_positions[source_key]]
                            )
                            if last_ts_event_ms > window_end_ms:
                                source_window_positions[source_key] += 1
                                continue
                            price_conditions = " OR ".join(
                                "(CAST(price AS REAL) >= ? AND CAST(price AS REAL) < ?)"
                                for _price_range in price_ranges
                            )
                            rows = connection.execute(
                                f"""
                                SELECT source_key, ordinal, ts_event_ms, price, size,
                                       side, action, order_id, instrument_id
                                FROM dom_events
                                WHERE source_key = ?
                                  AND ts_event_ms >= ?
                                  AND ts_event_ms <= ?
                                  AND (
                                    action IN ('R', 'CLEAR')
                                    OR ({price_conditions})
                                  )
                                  AND (
                                    ts_event_ms > ?
                                    OR (
                                      ts_event_ms = ?
                                      AND ordinal > ?
                                    )
                                  )
                                ORDER BY ts_event_ms, ordinal
                                LIMIT ?
                                """,
                                (
                                    source_key,
                                    window_start_ms,
                                    window_end_ms,
                                    *(
                                        bound
                                        for price_low, price_high in price_ranges
                                        for bound in (float(price_low), float(price_high))
                                    ),
                                    last_ts_event_ms,
                                    last_ts_event_ms,
                                    last_ordinal,
                                    self.batch_size,
                                ),
                            ).fetchall()
                            if rows:
                                break
                            source_window_positions[source_key] += 1
                    source_buffers[source_key] = list(rows)
                    source_positions[source_key] = 0
                    return bool(rows)

                def push_next(source_key: str) -> None:
                    rows = source_buffers.get(source_key) or []
                    position = int(source_positions.get(source_key, 0))
                    if position >= len(rows):
                        if not fetch_batch(source_key):
                            return
                        rows = source_buffers[source_key]
                        position = 0
                    row = rows[position]
                    source_positions[source_key] = position + 1
                    row_source_key, ordinal, ts_event_ms, *_rest = row
                    heapq.heappush(
                        heap,
                        (
                            int(ts_event_ms),
                            str(row_source_key or source_key),
                            int(ordinal or 0),
                            row,
                        ),
                    )

                for source_key in source_keys:
                    push_next(source_key)

                while heap:
                    _heap_ts_event_ms, heap_source_key, heap_ordinal, row = heapq.heappop(heap)
                    source_key, ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id = row
                    source_key = str(source_key or heap_source_key)
                    ts_event_ms = int(ts_event_ms)
                    source_cursors[source_key] = (ts_event_ms, int(ordinal or heap_ordinal))
                    push_next(source_key)
                    if self._footprint_price_ranges is not None:
                        interval_ms = int(
                            TIMEFRAME_MS_BY_NAME.get(self.timeframe, 60_000)
                        )
                        candle_open_time_ms = (
                            ts_event_ms // interval_ms
                        ) * interval_ms
                        price_ranges = self._footprint_price_ranges.get(
                            (normalized_provider, candle_open_time_ms)
                        )
                        if str(action or "").strip().upper() in {"R", "CLEAR"}:
                            pass
                        elif price_ranges is None or price in {None, ""}:
                            continue
                        else:
                            try:
                                event_price = Decimal(str(price))
                            except Exception:
                                continue
                            if not any(
                                price_low <= event_price < price_high
                                for price_low, price_high in price_ranges
                            ):
                                continue
                    if seen_timestamp != ts_event_ms:
                        seen_timestamp = ts_event_ms
                        signature_sources.clear()
                    event_signature = (
                        ts_event_ms,
                        str(price or ""),
                        int(size or 0),
                        str(side or ""),
                        str(action or ""),
                        str(order_id or ""),
                        int(instrument_id or 0),
                    )
                    first_source_key = signature_sources.get(event_signature)
                    if (
                        first_source_key is not None
                        and first_source_key != source_key
                    ):
                        continue
                    signature_sources.setdefault(event_signature, source_key)
                    yield DomRawEvent(
                        ts_event_ms=ts_event_ms,
                        price=Decimal(str(price)) if price not in {None, ""} else None,
                        size=int(size or 0),
                        side=str(side or "NONE"),
                        action=str(action or ""),
                        order_id=str(order_id or ""),
                        instrument_id=int(instrument_id or 0),
                        sequence=int(ordinal or 0),
                        source_file=source_key,
                    )

        return iterator()

    def _source_keys_for_symbol(self, provider_symbol: str) -> tuple[str, ...]:
        normalized = str(provider_symbol or "").strip().upper()
        if not normalized:
            return ()
        with self._connection() as connection:
            return tuple(
                str(row[0])
                for row in connection.execute(
                    """
                    SELECT source_key
                    FROM dom_sources
                    WHERE UPPER(provider_symbol) = ?
                      AND (status = 'READY' OR latest_event_time_ms > 0)
                    ORDER BY earliest_event_time_ms, source_file
                    """,
                    (normalized,),
                ).fetchall()
            )

    def _connection(self) -> sqlite3.Connection:
        return sqlite3.connect(f"file:{self.index_path}?mode=ro", uri=True)


class CmeFootprintReplaySource:
    def __init__(
        self,
        *,
        catalog: CmeLocalDataCatalog,
        trade_store: CmeLocalDbnTradeStore,
        dataset: str,
        schema: str,
        market_provider: str = "CME_LOCAL_DBN",
        timeframe: str = "M1",
        interval: str = "1m",
        bin_tick_count: int | Decimal = 1,
        output_decimal_places: int = 3,
        duration_unit_ms: int = 1000,
        session_start_hour_chicago: int = 17,
    ) -> None:
        self.catalog = catalog
        self.trade_store = trade_store
        self.dataset = dataset
        self.schema = schema
        self.market_provider = market_provider
        self.timeframe = timeframe.strip().upper()
        self.interval = interval
        self.bin_tick_count = bin_tick_count
        self.output_decimal_places = int(output_decimal_places)
        self.duration_unit_ms = int(duration_unit_ms)
        self.session_start_hour_chicago = int(session_start_hour_chicago)

    def symbols(self) -> tuple[ProcessSymbol, ...]:
        return tuple(
            ProcessSymbol(
                provider_symbol=provider_symbol,
                mt5_symbol=_mt5_symbol_from_provider(provider_symbol),
                market_provider=self.market_provider,
                dataset=self.dataset,
                schema=self.schema,
                tick_size=self.catalog.tick_size_for(provider_symbol),
                timeframe=self.timeframe,
                interval=self.interval,
                contract_symbols=(),
            )
            for provider_symbol in self.catalog.available_symbols()
        )

    def candles(
        self,
        symbol: ProcessSymbol,
        *,
        start_ms: int,
        end_ms: int,
    ) -> Iterable[dict[str, Any]]:
        frame = self.trade_store.trade_frame_for_time_range(
            symbol.provider_symbol,
            start_ms=int(start_ms),
            end_ms=int(end_ms),
            session_start_hour_chicago=self.session_start_hour_chicago,
        )
        if len(frame) == 0:
            return tuple()
        return tuple(
            _footprint_candles_from_frame(
                frame,
                provider_symbol=symbol.provider_symbol,
                mt5_symbol=symbol.mt5_symbol,
                timeframe=self.timeframe,
                tick_size=symbol.tick_size,
                bin_tick_count=self.bin_tick_count,
                output_decimal_places=self.output_decimal_places,
                duration_unit_ms=self.duration_unit_ms,
            )
        )


def _mt5_symbol_from_provider(provider_symbol: str) -> str:
    normalized = str(provider_symbol or "").strip().upper()
    return normalized.split(".", 1)[0] if "." in normalized else normalized
