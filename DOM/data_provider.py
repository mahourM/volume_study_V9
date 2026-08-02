from __future__ import annotations

import json
import hashlib
import msvcrt
import sqlite3
import shutil
import time
import threading
import zipfile
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import numpy as np

from DOM.models import DomContext, DomFileInfo, DomProviderResult, DomRawEvent
from cme_provider.engines import DBN_FIXED_PRICE_SCALE
from cme_provider.local_data import (
    CmeLocalDataError,
    _import_databento,
    _optional_int,
    _outright_instrument_symbols,
    _timestamp_to_ms,
    _utc_date_from_member_name,
)


class DomDataProvider(Protocol):
    def cache_signature(self, context: DomContext) -> tuple[tuple[str, int, int], ...]:
        ...

    def latest_available_end_ms(self, context: DomContext) -> int | None:
        ...

    def available_time_bounds_ms(self, context: DomContext) -> tuple[int, int] | None:
        ...

    def available_dates(self, context: DomContext) -> tuple[str, ...]:
        ...

    def initial_window_end_ms(self, context: DomContext, *, initial_span_ms: int) -> int | None:
        ...

    def events_for_window(
        self,
        context: DomContext,
        *,
        start_ms: int,
        end_ms: int,
        read_through: bool = False,
        primary_start_ms: int | None = None,
        primary_end_ms: int | None = None,
    ) -> DomProviderResult:
        ...

    def order_events_for_window(
        self,
        context: DomContext,
        *,
        order_ids: tuple[str, ...],
        start_ms: int,
        end_ms: int,
    ) -> tuple[DomRawEvent, ...]:
        ...

    def refill_point_order_ids_for_window(
        self,
        context: DomContext,
        *,
        start_ms: int,
        end_ms: int,
        threshold: int,
        instrument_ids: tuple[int, ...] = (),
    ) -> tuple[str, ...]:
        ...


@dataclass(frozen=True)
class _LoadedDomFile:
    info: DomFileInfo
    events: tuple[DomRawEvent, ...]
    earliest_event_time_ms: int
    latest_event_time_ms: int
    contract_symbols: tuple[str, ...]
    cache_group: tuple[Any, ...]
    loaded_start_ms: int
    loaded_end_ms: int
    sampled: bool


@dataclass
class _DomStreamState:
    dbn_store: object
    batches: object
    bucket_events: OrderedDict[int, list[DomRawEvent]]
    scanned_until_ms: int
    exhausted: bool
    contract_symbols: tuple[str, ...]
    instrument_symbols: dict[int, str] | None


class DomFileDataProvider:
    def __init__(
        self,
        *,
        data_dir: Path,
        extracted_cache_dir: Path,
        file_globs: tuple[str, ...],
        file_cache_size: int,
        dbn_batch_size: int,
        max_events_per_request: int,
        stream_bucket_ms: int,
        stream_cache_max_buckets: int,
        prefetch_window_multiplier: int = 1,
        prefetch_max_ms: int = 0,
    ) -> None:
        self.data_dir = data_dir
        self.extracted_cache_dir = extracted_cache_dir
        self.file_globs = tuple(item for item in file_globs if str(item).strip())
        self._file_cache_size = max(1, int(file_cache_size))
        self._dbn_batch_size = max(1, int(dbn_batch_size))
        self._max_events_per_request = max(1, int(max_events_per_request))
        self._stream_bucket_ms = max(1, int(stream_bucket_ms))
        self._stream_cache_max_buckets = max(1, int(stream_cache_max_buckets))
        self._prefetch_window_multiplier = max(1, int(prefetch_window_multiplier))
        self._prefetch_max_ms = max(0, int(prefetch_max_ms))
        self._file_cache: OrderedDict[tuple[Any, ...], _LoadedDomFile] = OrderedDict()
        self._stream_cache: OrderedDict[tuple[Any, ...], _DomStreamState] = OrderedDict()
        self._index_path = self.extracted_cache_dir / "dom_timeline_index.sqlite3"
        self._index_lock = threading.Lock()
        self._index_write_lock = threading.Lock()
        self._index_threads: dict[str, threading.Thread] = {}

    def cache_signature(self, context: DomContext) -> tuple[tuple[str, int, int], ...]:
        return tuple(
            (str(info.path), int(info.modified_ns), int(info.size_bytes))
            for info in self._discover_files(context)
        )

    def latest_available_end_ms(self, context: DomContext) -> int | None:
        values = []
        for info in self._discover_files(context):
            if info.path.suffix.lower() != ".zip":
                continue
            member_bounds = self._zip_latest_member_bounds_ms(info.path)
            if member_bounds is not None:
                values.append(member_bounds[1])
                continue
            metadata_end = self._zip_metadata_end_ms(info.path, context=context)
            if metadata_end is not None:
                values.append(metadata_end)
        return max(values) if values else None

    def available_time_bounds_ms(self, context: DomContext) -> tuple[int, int] | None:
        ranges: list[tuple[int, int]] = []
        for info in self._discover_files(context):
            if info.path.suffix.lower() == ".zip":
                member_bounds = self._zip_available_member_bounds_ms(info.path)
                if member_bounds is not None:
                    ranges.append(member_bounds)
                continue
            utc_date = _utc_date_from_member_name(info.path.name)
            if utc_date is None:
                continue
            start_dt = datetime.fromisoformat(utc_date).replace(tzinfo=UTC)
            start_ms = int(start_dt.timestamp() * 1000)
            ranges.append((start_ms, int((start_dt + timedelta(days=1)).timestamp() * 1000)))
        if ranges:
            return min(start for start, _end in ranges), max(end for _start, end in ranges)
        latest_end = self.latest_available_end_ms(context)
        if latest_end is None:
            return None
        return max(0, int(latest_end) - int(context.retention_ms)), int(latest_end)

    def available_dates(self, context: DomContext) -> tuple[str, ...]:
        dates: set[str] = set()
        for info in self._discover_files(context):
            if info.path.suffix.lower() == ".zip":
                dates.update(self._zip_available_member_dates(info.path))
                continue
            utc_date = _utc_date_from_member_name(info.path.name)
            if utc_date is not None:
                dates.add(utc_date)
        return tuple(sorted(dates))

    def initial_window_end_ms(self, context: DomContext, *, initial_span_ms: int) -> int | None:
        values = []
        for info in self._discover_files(context):
            if info.path.suffix.lower() != ".zip":
                continue
            member_bounds = self._zip_latest_member_bounds_ms(info.path)
            if member_bounds is None:
                continue
            start_ms, end_ms = member_bounds
            values.append(min(int(end_ms), int(start_ms) + max(1, int(initial_span_ms))))
        return max(values) if values else None

    def events_for_window(
        self,
        context: DomContext,
        *,
        start_ms: int,
        end_ms: int,
        read_through: bool = False,
        primary_start_ms: int | None = None,
        primary_end_ms: int | None = None,
    ) -> DomProviderResult:
        files = self._discover_files(context)
        if not files:
            return DomProviderResult(
                files=(),
                events=(),
                earliest_event_time_ms=0,
                latest_event_time_ms=0,
                contract_symbols=(),
                sampled=False,
                status="NO_DOM_FILES",
                message="No DOM files found",
            )

        loaded_files = [
            self._load_file(
                info,
                context=context,
                start_ms=start_ms,
                end_ms=end_ms,
                read_through=read_through,
                primary_start_ms=primary_start_ms,
                primary_end_ms=primary_end_ms,
            )
            for info in files
        ]
        events = [
            event
            for loaded in loaded_files
            for event in loaded.events
            if int(start_ms) <= int(event.ts_event_ms) <= int(end_ms)
        ]
        events.sort(key=lambda item: (int(item.ts_event_ms), int(item.sequence)))
        earliest_values = [
            loaded.earliest_event_time_ms
            for loaded in loaded_files
            if loaded.earliest_event_time_ms > 0
        ]
        latest_values = [
            loaded.latest_event_time_ms
            for loaded in loaded_files
            if loaded.latest_event_time_ms > 0
        ]
        contract_symbols = tuple(
            dict.fromkeys(
                symbol
                for loaded in loaded_files
                for symbol in loaded.contract_symbols
                if symbol
            )
        )
        return DomProviderResult(
            files=files,
            events=tuple(events),
            earliest_event_time_ms=min(earliest_values) if earliest_values else 0,
            latest_event_time_ms=max(latest_values) if latest_values else 0,
            contract_symbols=contract_symbols,
            sampled=any(loaded.sampled for loaded in loaded_files),
            status="READY",
            message="",
        )

    def order_events_for_window(
        self,
        context: DomContext,
        *,
        order_ids: tuple[str, ...],
        start_ms: int,
        end_ms: int,
    ) -> tuple[DomRawEvent, ...]:
        normalized_order_ids = tuple(
            dict.fromkeys(str(order_id).strip() for order_id in order_ids if str(order_id).strip())
        )
        if not normalized_order_ids or int(end_ms) < int(start_ms):
            return ()
        files = self._discover_files(context)
        if not files:
            return ()
        events: list[DomRawEvent] = []
        contract_symbols: list[str] = []
        for info in files:
            member_names = (
                self._zip_member_names_for_window(
                    info.path,
                    start_ms=int(start_ms),
                    end_ms=int(end_ms),
                )
                if info.path.suffix.lower() == ".zip"
                else ()
            )
            if info.path.suffix.lower() == ".zip":
                try:
                    with zipfile.ZipFile(info.path) as archive:
                        for member_name in member_names:
                            extracted_path = self._ensure_extracted_member(
                                archive,
                                zip_info=info,
                                member_name=member_name,
                            )
                            member_events, member_contracts = self._order_events_from_index(
                                extracted_path,
                                context=context,
                                source_file=f"{info.name}/{Path(member_name).name}",
                                order_ids=normalized_order_ids,
                                start_ms=start_ms,
                                end_ms=end_ms,
                            )
                            events.extend(member_events)
                            contract_symbols.extend(member_contracts)
                except Exception:
                    continue
            else:
                member_events, member_contracts = self._order_events_from_index(
                    info.path,
                    context=context,
                    source_file=info.name,
                    order_ids=normalized_order_ids,
                    start_ms=start_ms,
                    end_ms=end_ms,
                )
                events.extend(member_events)
                contract_symbols.extend(member_contracts)
        del contract_symbols
        events.sort(key=lambda item: (int(item.ts_event_ms), int(item.sequence)))
        return tuple(events)

    def refill_point_order_ids_for_window(
        self,
        context: DomContext,
        *,
        start_ms: int,
        end_ms: int,
        threshold: int,
        instrument_ids: tuple[int, ...] = (),
    ) -> tuple[str, ...]:
        if int(end_ms) < int(start_ms) or int(threshold) <= 0:
            return ()
        files = self._discover_files(context)
        if not files:
            return ()
        candidates: dict[str, int] = {}
        for info in files:
            member_names = (
                self._zip_member_names_for_window(
                    info.path,
                    start_ms=int(start_ms),
                    end_ms=int(end_ms),
                )
                if info.path.suffix.lower() == ".zip"
                else ()
            )
            if info.path.suffix.lower() == ".zip":
                try:
                    with zipfile.ZipFile(info.path) as archive:
                        for member_name in member_names:
                            extracted_path = self._ensure_extracted_member(
                                archive,
                                zip_info=info,
                                member_name=member_name,
                            )
                            for order_id, modify_count in self._refill_point_order_ids_from_index(
                                extracted_path,
                                context=context,
                                start_ms=start_ms,
                                end_ms=end_ms,
                                threshold=threshold,
                                instrument_ids=instrument_ids,
                            ):
                                candidates[order_id] = max(candidates.get(order_id, 0), modify_count)
                except Exception:
                    continue
            else:
                for order_id, modify_count in self._refill_point_order_ids_from_index(
                    info.path,
                    context=context,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    threshold=threshold,
                    instrument_ids=instrument_ids,
                ):
                    candidates[order_id] = max(candidates.get(order_id, 0), modify_count)
        return tuple(
            order_id
            for order_id, _count in sorted(
                candidates.items(),
                key=lambda item: (-int(item[1]), item[0]),
            )[:200]
        )

    def _discover_files(self, context: DomContext) -> tuple[DomFileInfo, ...]:
        del context
        self.data_dir.mkdir(parents=True, exist_ok=True)
        paths: list[Path] = []
        for file_glob in self.file_globs:
            paths.extend(self.data_dir.glob(file_glob))
        unique_paths = sorted({path.resolve() for path in paths if path.is_file()})
        result = []
        for path in unique_paths:
            stat = path.stat()
            result.append(
                DomFileInfo(
                    path=path,
                    name=path.name,
                    size_bytes=int(stat.st_size),
                    modified_ns=int(stat.st_mtime_ns),
                )
            )
        return tuple(result)

    def _load_file(
        self,
        info: DomFileInfo,
        *,
        context: DomContext,
        start_ms: int,
        end_ms: int,
        read_through: bool,
        primary_start_ms: int | None,
        primary_end_ms: int | None,
    ) -> _LoadedDomFile:
        load_start_ms, load_end_ms = self._expanded_window(start_ms=start_ms, end_ms=end_ms)
        primary_window = _primary_window(
            load_start_ms,
            load_end_ms,
            primary_start_ms=primary_start_ms,
            primary_end_ms=primary_end_ms,
        )
        member_names = (
            self._zip_member_names_for_window(info.path, start_ms=int(start_ms), end_ms=int(end_ms))
            if info.path.suffix.lower() == ".zip"
            else ()
        )
        cache_group = (
            str(info.path),
            int(info.modified_ns),
            int(info.size_bytes),
            context.provider_symbol.upper(),
            member_names,
            primary_window or (),
        )
        cache_key = (
            *cache_group,
            int(load_start_ms),
            int(load_end_ms),
        )
        cached = self._file_cache.get(cache_key)
        if cached is not None:
            self._file_cache.move_to_end(cache_key)
            return cached
        covering = self._covering_cached_file(
            cache_group,
            start_ms=start_ms,
            end_ms=end_ms,
        )
        if covering is not None:
            return covering

        events, contract_symbols, sampled = self._load_events_from_file(
            info,
            context=context,
            member_names=member_names,
            start_ms=load_start_ms,
            end_ms=load_end_ms,
            read_through=read_through,
            primary_window=primary_window,
        )
        if events:
            earliest = min(int(event.ts_event_ms) for event in events)
            latest = max(int(event.ts_event_ms) for event in events)
        else:
            earliest = 0
            latest = 0
        loaded = _LoadedDomFile(
            info=info,
            events=tuple(sorted(events, key=lambda item: (int(item.ts_event_ms), int(item.sequence)))),
            earliest_event_time_ms=earliest,
            latest_event_time_ms=latest,
            contract_symbols=contract_symbols,
            cache_group=cache_group,
            loaded_start_ms=int(load_start_ms),
            loaded_end_ms=int(load_end_ms),
            sampled=sampled,
        )
        self._file_cache[cache_key] = loaded
        self._file_cache.move_to_end(cache_key)
        while len(self._file_cache) > self._file_cache_size:
            self._file_cache.popitem(last=False)
        return loaded

    def _expanded_window(self, *, start_ms: int, end_ms: int) -> tuple[int, int]:
        start = int(start_ms)
        end = max(start, int(end_ms))
        span = max(1, end - start)
        padding = min(
            self._prefetch_max_ms,
            span * max(0, self._prefetch_window_multiplier - 1),
        )
        return max(0, start - padding), end + padding

    def _covering_cached_file(
        self,
        cache_group: tuple[Any, ...],
        *,
        start_ms: int,
        end_ms: int,
    ) -> _LoadedDomFile | None:
        found_key = None
        found_loaded = None
        for key, loaded in reversed(self._file_cache.items()):
            if loaded.cache_group != cache_group:
                continue
            if loaded.sampled:
                continue
            if int(loaded.loaded_start_ms) <= int(start_ms) and int(loaded.loaded_end_ms) >= int(end_ms):
                found_key = key
                found_loaded = loaded
                break
        if found_key is not None and found_loaded is not None:
            self._file_cache.move_to_end(found_key)
            return found_loaded
        return None

    def _load_events_from_file(
        self,
        info: DomFileInfo,
        *,
        context: DomContext,
        member_names: tuple[str, ...] = (),
        start_ms: int,
        end_ms: int,
        read_through: bool,
        primary_window: tuple[int, int] | None,
    ) -> tuple[list[DomRawEvent], tuple[str, ...], bool]:
        if info.path.suffix.lower() == ".zip":
            return self._load_events_from_zip(
                info,
                context=context,
                member_names=member_names,
                start_ms=start_ms,
                end_ms=end_ms,
                read_through=read_through,
                primary_window=primary_window,
            )
        indexed = self._events_from_index(
            info.path,
            context=context,
            source_file=info.name,
            start_ms=start_ms,
            end_ms=end_ms,
            primary_window=primary_window,
        )
        if indexed is not None:
            return indexed
        result = self._events_from_path_once(
            info.path,
            context=context,
            source_file=info.name,
            start_ms=start_ms,
            end_ms=end_ms,
            read_through=read_through,
            primary_window=primary_window,
        )
        if read_through:
            self._start_indexing(
                info.path,
                context=context,
                source_file=info.name,
            )
        return result

    def _load_events_from_zip(
        self,
        info: DomFileInfo,
        *,
        context: DomContext,
        member_names: tuple[str, ...],
        start_ms: int,
        end_ms: int,
        read_through: bool,
        primary_window: tuple[int, int] | None,
    ) -> tuple[list[DomRawEvent], tuple[str, ...], bool]:
        databento = _import_databento()
        events: list[DomRawEvent] = []
        contract_symbols: list[str] = []
        sampled = False
        try:
            with zipfile.ZipFile(info.path) as archive:
                for member_name in member_names:
                    extracted_path = self._ensure_extracted_member(
                        archive,
                        zip_info=info,
                        member_name=member_name,
                    )
                    indexed = self._events_from_index(
                        extracted_path,
                        context=context,
                        source_file=f"{info.name}/{Path(member_name).name}",
                        start_ms=start_ms,
                        end_ms=end_ms,
                        primary_window=primary_window,
                    )
                    if indexed is not None:
                        member_events, member_contracts, member_sampled = indexed
                    else:
                        member_events, member_contracts, member_sampled = self._events_from_path_once(
                            extracted_path,
                            context=context,
                            source_file=f"{info.name}/{Path(member_name).name}",
                            start_ms=start_ms,
                            end_ms=end_ms,
                            read_through=read_through,
                            primary_window=primary_window,
                        )
                        if read_through:
                            self._start_indexing(
                                extracted_path,
                                context=context,
                                source_file=f"{info.name}/{Path(member_name).name}",
                            )
                    events.extend(member_events)
                    contract_symbols.extend(member_contracts)
                    sampled = sampled or member_sampled
                    if len(events) > self._max_events_per_request:
                        events = _limit_raw_events(
                            events,
                            self._max_events_per_request,
                            primary_window=primary_window,
                        )
                        sampled = True
                        if primary_window is None:
                            break
        except Exception as exc:
            raise CmeLocalDataError(f"Unable to read DOM MBO DBN archive: {info.name}") from exc
        return events, tuple(dict.fromkeys(contract_symbols)), sampled

    def _events_from_index(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
        start_ms: int,
        end_ms: int,
        primary_window: tuple[int, int] | None = None,
    ) -> tuple[list[DomRawEvent], tuple[str, ...], bool] | None:
        if not self._index_path.exists() or self._index_path.stat().st_size <= 0:
            return None
        source_key = self._index_source_key(path, context=context)
        try:
            with self._readonly_index_connection() as connection:
                source = connection.execute(
                    "SELECT status, indexed_until_ms, contract_symbols FROM dom_sources WHERE source_key = ?",
                    (source_key,),
                ).fetchone()
                if source is None:
                    return None
                status, indexed_until_ms, contract_symbols_text = source
                if status != "READY" and int(indexed_until_ms or 0) < int(end_ms):
                    return None
                rows = self._index_rows_for_window(
                    connection,
                    source_key=source_key,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    primary_window=primary_window,
                )
        except sqlite3.Error:
            return None
        sampled = False
        events = [
            DomRawEvent(
                ts_event_ms=int(ts_event_ms),
                price=Decimal(str(price)) if price not in {None, ""} else None,
                size=int(size or 0),
                side=str(side or "NONE"),
                action=str(action or ""),
                order_id=str(order_id or ""),
                instrument_id=int(instrument_id or 0),
                sequence=int(ordinal or 0),
                source_file=source_file,
            )
            for ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id in rows
        ]
        contract_symbols = tuple(
            item for item in str(contract_symbols_text or "").split(",") if item
        )
        return events, contract_symbols, sampled

    def _order_events_from_index(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
        order_ids: tuple[str, ...],
        start_ms: int,
        end_ms: int,
    ) -> tuple[list[DomRawEvent], tuple[str, ...]]:
        if not order_ids or not self._index_path.exists() or self._index_path.stat().st_size <= 0:
            return [], ()
        source_key = self._index_source_key(path, context=context)
        try:
            with self._readonly_index_connection() as connection:
                source = connection.execute(
                    "SELECT status, contract_symbols FROM dom_sources WHERE source_key = ?",
                    (source_key,),
                ).fetchone()
                if source is None:
                    return [], ()
                status, contract_symbols_text = source
                if status != "READY":
                    return [], ()
                placeholders = ",".join("?" for _ in order_ids)
                started_at = time.monotonic()

                def progress() -> int:
                    return 1 if (time.monotonic() - started_at) > 6.0 else 0

                connection.set_progress_handler(progress, 10_000)
                rows = connection.execute(
                    f"""
                    SELECT ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
                    FROM dom_events
                    WHERE source_key = ?
                      AND ts_event_ms >= ?
                      AND ts_event_ms <= ?
                      AND order_id IN ({placeholders})
                    ORDER BY ts_event_ms, ordinal
                    LIMIT ?
                    """,
                    (
                        source_key,
                        int(start_ms),
                        int(end_ms),
                        *order_ids,
                        max(self._max_events_per_request * 8, 10_000),
                    ),
                ).fetchall()
        except sqlite3.Error:
            return [], ()
        events = [
            DomRawEvent(
                ts_event_ms=int(ts_event_ms),
                price=Decimal(str(price)) if price not in {None, ""} else None,
                size=int(size or 0),
                side=str(side or "NONE"),
                action=str(action or ""),
                order_id=str(order_id or ""),
                instrument_id=int(instrument_id or 0),
                sequence=int(ordinal or 0),
                source_file=source_file,
            )
            for ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id in rows
        ]
        contract_symbols = tuple(
            item for item in str(contract_symbols_text or "").split(",") if item
        )
        return events, contract_symbols

    def _refill_point_order_ids_from_index(
        self,
        path: Path,
        *,
        context: DomContext,
        start_ms: int,
        end_ms: int,
        threshold: int,
        instrument_ids: tuple[int, ...],
    ) -> tuple[tuple[str, int], ...]:
        if int(threshold) <= 0 or not self._index_path.exists() or self._index_path.stat().st_size <= 0:
            return ()
        source_key = self._index_source_key(path, context=context)
        try:
            with self._readonly_index_connection() as connection:
                source = connection.execute(
                    "SELECT status FROM dom_sources WHERE source_key = ?",
                    (source_key,),
                ).fetchone()
                if source is None or source[0] != "READY":
                    return ()
                started_at = time.monotonic()

                def progress() -> int:
                    return 1 if (time.monotonic() - started_at) > 3.0 else 0

                connection.set_progress_handler(progress, 10_000)
                params: list[Any] = [
                    source_key,
                    int(start_ms),
                    int(end_ms),
                    "M",
                    "MODIFY",
                ]
                instrument_clause = ""
                normalized_instruments = tuple(
                    int(item) for item in instrument_ids if int(item) > 0
                )
                if normalized_instruments:
                    instrument_clause = (
                        " AND instrument_id IN ("
                        + ",".join("?" for _ in normalized_instruments)
                        + ")"
                    )
                    params.extend(normalized_instruments)
                params.extend([int(threshold), 200])
                rows = connection.execute(
                    f"""
                    SELECT order_id, COUNT(*) AS modify_count
                    FROM dom_events
                    WHERE source_key = ?
                      AND ts_event_ms >= ?
                      AND ts_event_ms <= ?
                      AND action IN (?,?)
                      AND order_id != ''
                      {instrument_clause}
                    GROUP BY order_id
                    HAVING modify_count >= ?
                    ORDER BY modify_count DESC, order_id
                    LIMIT ?
                    """,
                    tuple(params),
                ).fetchall()
        except sqlite3.Error:
            return ()
        return tuple((str(order_id), int(modify_count or 0)) for order_id, modify_count in rows)

    def _index_rows_for_window(
        self,
        connection: sqlite3.Connection,
        *,
        source_key: str,
        start_ms: int,
        end_ms: int,
        primary_window: tuple[int, int] | None,
    ) -> list[tuple[Any, ...]]:
        if primary_window is None:
            return connection.execute(
                """
                SELECT ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
                FROM dom_events
                WHERE source_key = ? AND ts_event_ms >= ? AND ts_event_ms <= ?
                ORDER BY ts_event_ms, ordinal
                LIMIT ?
                """,
                (source_key, int(start_ms), int(end_ms), self._max_events_per_request + 1),
            ).fetchall()
        primary_start_ms, primary_end_ms = primary_window
        primary_rows = connection.execute(
            """
            SELECT ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
            FROM dom_events
            WHERE source_key = ? AND ts_event_ms >= ? AND ts_event_ms <= ?
            ORDER BY ts_event_ms, ordinal
            LIMIT ?
            """,
            (
                source_key,
                int(primary_start_ms),
                int(primary_end_ms),
                self._max_events_per_request + 1,
            ),
        ).fetchall()
        if len(primary_rows) >= self._max_events_per_request:
            return primary_rows
        remaining = max(1, self._max_events_per_request - len(primary_rows) + 1)
        before_limit = max(1, remaining // 2)
        after_limit = max(1, remaining - before_limit)
        before_rows = connection.execute(
            """
            SELECT ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
            FROM dom_events
            WHERE source_key = ? AND ts_event_ms >= ? AND ts_event_ms < ?
            ORDER BY ts_event_ms DESC, ordinal DESC
            LIMIT ?
            """,
            (source_key, int(start_ms), int(primary_start_ms), before_limit),
        ).fetchall()
        after_rows = connection.execute(
            """
            SELECT ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
            FROM dom_events
            WHERE source_key = ? AND ts_event_ms > ? AND ts_event_ms <= ?
            ORDER BY ts_event_ms, ordinal
            LIMIT ?
            """,
            (source_key, int(primary_end_ms), int(end_ms), after_limit),
        ).fetchall()
        rows = list(reversed(before_rows)) + list(primary_rows) + list(after_rows)
        rows.sort(key=lambda item: (int(item[1]), int(item[0])))
        return rows

    def _start_indexing(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
    ) -> None:
        source_key = self._index_source_key(path, context=context)
        with self._index_lock:
            active = self._index_threads.get(source_key)
            if active is not None and active.is_alive():
                return
            if self._index_status(path, context=context) == "READY":
                return
            thread = threading.Timer(
                3.0,
                self._index_path_worker,
                kwargs={
                    "path": path,
                    "context": context,
                    "source_file": source_file,
                    "source_key": source_key,
                },
            )
            thread.daemon = True
            self._index_threads[source_key] = thread
            thread.start()

    def _index_path_worker(
        self,
        *,
        path: Path,
        context: DomContext,
        source_file: str,
        source_key: str,
    ) -> None:
        try:
            self._build_index_for_path(
                path,
                context=context,
                source_file=source_file,
                source_key=source_key,
            )
        except Exception as exc:
            try:
                with self._index_connection() as connection:
                    connection.execute(
                        """
                        UPDATE dom_sources
                        SET status = ?, error = ?
                        WHERE source_key = ?
                        """,
                        ("ERROR", str(exc)[:500], source_key),
                    )
            except sqlite3.Error:
                return

    def _build_index_for_path(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
        source_key: str,
    ) -> None:
        with self._source_index_file_lock(source_key) as acquired:
            if not acquired:
                return
            self._build_index_for_path_locked(
                path,
                context=context,
                source_file=source_file,
                source_key=source_key,
            )

    def _build_index_for_path_locked(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
        source_key: str,
    ) -> None:
        databento = _import_databento()
        stat = path.stat()
        ordinal = 0
        earliest = 0
        latest = 0
        contract_symbols: tuple[str, ...] = ()
        with self._index_connection() as connection:
            # A RUNNING/ERROR source is deliberately rebuilt from the beginning.
            # Resuming by timestamp is unsafe because an MBO file can contain many
            # legitimate events with the same millisecond timestamp.  Starting over
            # under the per-source process lock makes interrupted builds lossless.
            connection.execute("DELETE FROM dom_events WHERE source_key = ?", (source_key,))
            connection.execute(
                """
                INSERT INTO dom_sources(
                    source_key, path, modified_ns, size_bytes, provider_symbol, source_file,
                    status, indexed_until_ms, earliest_event_time_ms, latest_event_time_ms,
                    contract_symbols, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_key) DO UPDATE SET
                    path = excluded.path,
                    modified_ns = excluded.modified_ns,
                    size_bytes = excluded.size_bytes,
                    provider_symbol = excluded.provider_symbol,
                    source_file = excluded.source_file,
                    status = excluded.status,
                    indexed_until_ms = 0,
                    earliest_event_time_ms = 0,
                    latest_event_time_ms = 0,
                    contract_symbols = '',
                    error = ''
                """,
                (
                    source_key,
                    str(path),
                    int(stat.st_mtime_ns),
                    int(stat.st_size),
                    context.provider_symbol.upper(),
                    source_file,
                    "RUNNING",
                    0,
                    0,
                    0,
                    "",
                    "",
                ),
            )

        dbn_store = databento.DBNStore.from_file(str(path))
        instrument_symbols = _outright_instrument_symbols(
            dbn_store,
            provider_symbol=context.provider_symbol,
        )
        contract_symbols = tuple(
            dict.fromkeys(contract_symbols + tuple(instrument_symbols.values()))
        )
        batches = dbn_store.to_ndarray(schema="mbo", count=self._dbn_batch_size)
        for records in _iter_record_batches(batches):
            if len(records) == 0 or (records.dtype.names or ()) is None or "ts_event" not in (records.dtype.names or ()):
                continue
            event_time_ms = (records["ts_event"] // 1_000_000).astype(np.int64, copy=False)
            if not len(event_time_ms):
                continue
            batch_start = int(event_time_ms[0])
            batch_end = int(event_time_ms[-1])
            batch_events, batch_contracts = self._events_from_records(
                records,
                dbn_store=dbn_store,
                context=context,
                source_file=source_file,
                start_ms=batch_start,
                end_ms=batch_end,
                instrument_symbols=instrument_symbols,
                primary_window=None,
            )
            if batch_contracts:
                contract_symbols = tuple(dict.fromkeys(contract_symbols + batch_contracts))
            rows = []
            for event in batch_events:
                ordinal += 1
                if earliest <= 0:
                    earliest = int(event.ts_event_ms)
                latest = max(latest, int(event.ts_event_ms))
                rows.append(
                    (
                        source_key,
                        ordinal,
                        int(event.ts_event_ms),
                        _format_index_price(event.price),
                        int(event.size),
                        event.side,
                        event.action,
                        event.order_id,
                        int(event.instrument_id),
                    )
                )
            with self._index_connection() as connection:
                if rows:
                    connection.executemany(
                        """
                        INSERT INTO dom_events(
                            source_key, ordinal, ts_event_ms, price, size, side, action, order_id, instrument_id
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
                connection.execute(
                    """
                    UPDATE dom_sources
                    SET indexed_until_ms = ?, earliest_event_time_ms = ?, latest_event_time_ms = ?,
                        contract_symbols = ?
                    WHERE source_key = ?
                    """,
                    (
                        batch_end,
                        earliest,
                        latest,
                        ",".join(contract_symbols),
                        source_key,
                    ),
                )
        with self._index_connection() as connection:
            connection.execute(
                """
                UPDATE dom_sources
                SET status = ?, indexed_until_ms = ?, earliest_event_time_ms = ?,
                    latest_event_time_ms = ?, contract_symbols = ?, error = ?
                WHERE source_key = ?
                """,
                ("READY", latest, earliest, latest, ",".join(contract_symbols), "", source_key),
            )

    def _index_status(self, path: Path, *, context: DomContext) -> str:
        if not self._index_path.exists() or self._index_path.stat().st_size <= 0:
            return ""
        source_key = self._index_source_key(path, context=context)
        try:
            with self._readonly_index_connection() as connection:
                row = connection.execute(
                    "SELECT status FROM dom_sources WHERE source_key = ?",
                    (source_key,),
                ).fetchone()
        except sqlite3.Error:
            return ""
        return str(row[0]) if row else ""

    @contextmanager
    def _readonly_index_connection(self) -> Any:
        uri_path = self._index_path.resolve().as_posix()
        connection = sqlite3.connect(f"file:{uri_path}?mode=ro", uri=True, timeout=0.05)
        try:
            connection.execute("PRAGMA query_only = ON")
            yield connection
        finally:
            connection.close()

    @contextmanager
    def _index_connection(self) -> Any:
        self.extracted_cache_dir.mkdir(parents=True, exist_ok=True)
        self._discard_empty_index_artifacts()
        with self._index_write_lock:
            connection = sqlite3.connect(str(self._index_path), timeout=30.0)
            try:
                connection.execute("PRAGMA busy_timeout=30000")
                connection.execute("PRAGMA journal_mode=WAL")
                connection.execute("PRAGMA synchronous=NORMAL")
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dom_sources(
                        source_key TEXT PRIMARY KEY,
                        path TEXT NOT NULL,
                        modified_ns INTEGER NOT NULL,
                        size_bytes INTEGER NOT NULL,
                        provider_symbol TEXT NOT NULL,
                        source_file TEXT NOT NULL,
                        status TEXT NOT NULL,
                        indexed_until_ms INTEGER NOT NULL DEFAULT 0,
                        earliest_event_time_ms INTEGER NOT NULL DEFAULT 0,
                        latest_event_time_ms INTEGER NOT NULL DEFAULT 0,
                        contract_symbols TEXT NOT NULL DEFAULT '',
                        error TEXT NOT NULL DEFAULT ''
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dom_events(
                        source_key TEXT NOT NULL,
                        ordinal INTEGER NOT NULL,
                        ts_event_ms INTEGER NOT NULL,
                        price TEXT,
                        size INTEGER NOT NULL,
                        side TEXT NOT NULL,
                        action TEXT NOT NULL,
                        order_id TEXT NOT NULL,
                        instrument_id INTEGER NOT NULL
                    )
                    """
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dom_events_source_time ON dom_events(source_key, ts_event_ms)"
                )
                connection.execute(
                    "CREATE INDEX IF NOT EXISTS idx_dom_events_source_time_ordinal "
                    "ON dom_events(source_key, ts_event_ms, ordinal)"
                )
                connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_dom_events_source_ordinal ON dom_events(source_key, ordinal)"
                )
                yield connection
                connection.commit()
            finally:
                connection.close()

    @contextmanager
    def _source_index_file_lock(self, source_key: str) -> Any:
        lock_dir = self.extracted_cache_dir / ".index-locks"
        lock_dir.mkdir(parents=True, exist_ok=True)
        lock_path = lock_dir / f"{source_key}.lock"
        handle = lock_path.open("a+b")
        acquired = False
        try:
            handle.seek(0, 2)
            if handle.tell() == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                acquired = True
            except OSError:
                acquired = False
            yield acquired
        finally:
            if acquired:
                handle.seek(0)
                try:
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                except OSError:
                    pass
            handle.close()

    def _discard_empty_index_artifacts(self) -> None:
        if not self._index_path.exists() or self._index_path.stat().st_size > 0:
            return
        for path in (
            self._index_path,
            self._index_path.with_name(f"{self._index_path.name}-journal"),
            self._index_path.with_name(f"{self._index_path.name}-wal"),
            self._index_path.with_name(f"{self._index_path.name}-shm"),
        ):
            try:
                if path.exists():
                    path.unlink()
            except OSError:
                pass

    @staticmethod
    def _index_source_key(path: Path, *, context: DomContext) -> str:
        stat = path.stat()
        raw_key = "|".join(
            (
                str(path.resolve()),
                str(int(stat.st_mtime_ns)),
                str(int(stat.st_size)),
                context.provider_symbol.upper(),
            )
        )
        return hashlib.sha1(raw_key.encode("utf-8")).hexdigest()

    def _events_from_path_stream_cached(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
        start_ms: int,
        end_ms: int,
        read_through: bool,
    ) -> tuple[list[DomRawEvent], tuple[str, ...], bool]:
        stat = path.stat()
        cache_key = (
            str(path),
            int(stat.st_mtime_ns),
            int(stat.st_size),
            context.provider_symbol.upper(),
        )
        state = self._stream_cache.get(cache_key)
        if state is not None:
            self._stream_cache.move_to_end(cache_key)
        else:
            state = self._open_stream_state(path)
            self._stream_cache[cache_key] = state
            self._stream_cache.move_to_end(cache_key)
            while len(self._stream_cache) > self._file_cache_size:
                self._stream_cache.popitem(last=False)

        if int(end_ms) > int(state.scanned_until_ms) and not state.exhausted:
            self._scan_stream_state(
                state,
                context=context,
                source_file=source_file,
                cache_from_ms=int(start_ms),
                target_ms=int(end_ms),
            )

        if self._stream_range_available(state, start_ms=int(start_ms), end_ms=int(end_ms)):
            events = self._events_from_stream_buckets(
                state,
                start_ms=int(start_ms),
                end_ms=int(end_ms),
            )
            sampled = False
            if len(events) > self._max_events_per_request:
                events = events[: self._max_events_per_request]
                sampled = True
            return events, state.contract_symbols, sampled

        return self._events_from_path_once(
            path,
            context=context,
            source_file=source_file,
            start_ms=start_ms,
            end_ms=end_ms,
            read_through=read_through,
            primary_window=None,
        )

    def _open_stream_state(self, path: Path) -> _DomStreamState:
        databento = _import_databento()
        try:
            dbn_store = databento.DBNStore.from_file(str(path))
        except Exception as exc:
            raise CmeLocalDataError(f"Unable to read DOM MBO DBN file: {path.name}") from exc
        return _DomStreamState(
            dbn_store=dbn_store,
            batches=dbn_store.to_ndarray(schema="mbo", count=self._dbn_batch_size),
            bucket_events=OrderedDict(),
            scanned_until_ms=0,
            exhausted=False,
            contract_symbols=(),
            instrument_symbols=None,
        )

    def _events_from_path_once(
        self,
        path: Path,
        *,
        context: DomContext,
        source_file: str,
        start_ms: int,
        end_ms: int,
        read_through: bool,
        primary_window: tuple[int, int] | None,
    ) -> tuple[list[DomRawEvent], tuple[str, ...], bool]:
        databento = _import_databento()
        try:
            dbn_store = databento.DBNStore.from_file(str(path))
        except Exception as exc:
            raise CmeLocalDataError(f"Unable to read DOM MBO DBN file: {path.name}") from exc
        return self._events_from_store(
            dbn_store,
            dbn_store=dbn_store,
            context=context,
            source_file=source_file,
            start_ms=start_ms,
            end_ms=end_ms,
            read_through=read_through,
            primary_window=primary_window,
        )

    def _scan_stream_state(
        self,
        state: _DomStreamState,
        *,
        context: DomContext,
        source_file: str,
        cache_from_ms: int,
        target_ms: int,
    ) -> None:
        for records in _iter_record_batches(state.batches):
            if len(records) == 0 or (records.dtype.names or ()) is None or "ts_event" not in (records.dtype.names or ()):
                continue
            batch_event_time_ms = (records["ts_event"] // 1_000_000).astype(np.int64, copy=False)
            if not len(batch_event_time_ms):
                continue
            batch_start_ms = int(batch_event_time_ms[0])
            batch_end_ms = int(batch_event_time_ms[-1])
            scan_start_ms = max(int(cache_from_ms), int(state.scanned_until_ms) + 1)
            if batch_end_ms >= scan_start_ms:
                self._mark_stream_buckets(state, start_ms=scan_start_ms, end_ms=batch_end_ms)
                if state.instrument_symbols is None:
                    state.instrument_symbols = _outright_instrument_symbols(
                        state.dbn_store,
                        provider_symbol=context.provider_symbol,
                    )
                batch_events, batch_contracts = self._events_from_records(
                    records,
                    dbn_store=state.dbn_store,
                    context=context,
                    source_file=source_file,
                    start_ms=scan_start_ms,
                    end_ms=batch_end_ms,
                    instrument_symbols=state.instrument_symbols,
                    primary_window=None,
                )
                if batch_contracts:
                    state.contract_symbols = tuple(dict.fromkeys(state.contract_symbols + batch_contracts))
                for event in batch_events:
                    bucket_key = self._stream_bucket_key(int(event.ts_event_ms))
                    state.bucket_events.setdefault(bucket_key, []).append(event)
                self._trim_stream_buckets(state)
            state.scanned_until_ms = max(int(state.scanned_until_ms), batch_end_ms)
            if batch_start_ms > int(target_ms) or state.scanned_until_ms >= int(target_ms):
                return
        state.exhausted = True

    def _stream_range_available(
        self,
        state: _DomStreamState,
        *,
        start_ms: int,
        end_ms: int,
    ) -> bool:
        if int(end_ms) > int(state.scanned_until_ms) and not state.exhausted:
            return False
        start_bucket = self._stream_bucket_key(start_ms)
        end_bucket = self._stream_bucket_key(end_ms)
        bucket = start_bucket
        while bucket <= end_bucket:
            if bucket not in state.bucket_events:
                return False
            bucket += self._stream_bucket_ms
        return True

    def _events_from_stream_buckets(
        self,
        state: _DomStreamState,
        *,
        start_ms: int,
        end_ms: int,
    ) -> list[DomRawEvent]:
        events: list[DomRawEvent] = []
        bucket = self._stream_bucket_key(start_ms)
        end_bucket = self._stream_bucket_key(end_ms)
        while bucket <= end_bucket:
            events.extend(
                event
                for event in state.bucket_events.get(bucket, [])
                if int(start_ms) <= int(event.ts_event_ms) <= int(end_ms)
            )
            bucket += self._stream_bucket_ms
        events.sort(key=lambda item: (int(item.ts_event_ms), int(item.sequence)))
        return events

    def _mark_stream_buckets(
        self,
        state: _DomStreamState,
        *,
        start_ms: int,
        end_ms: int,
    ) -> None:
        bucket = self._stream_bucket_key(start_ms)
        end_bucket = self._stream_bucket_key(end_ms)
        while bucket <= end_bucket:
            state.bucket_events.setdefault(bucket, [])
            bucket += self._stream_bucket_ms

    def _trim_stream_buckets(self, state: _DomStreamState) -> None:
        while len(state.bucket_events) > self._stream_cache_max_buckets:
            state.bucket_events.popitem(last=False)

    def _stream_bucket_key(self, value_ms: int) -> int:
        return (max(0, int(value_ms)) // self._stream_bucket_ms) * self._stream_bucket_ms

    def _events_from_store(
        self,
        store: object,
        *,
        dbn_store: object,
        context: DomContext,
        source_file: str,
        start_ms: int,
        end_ms: int,
        read_through: bool,
        primary_window: tuple[int, int] | None,
    ) -> tuple[list[DomRawEvent], tuple[str, ...], bool]:
        events: list[DomRawEvent] = []
        contract_symbols: tuple[str, ...] = ()
        sampled = False
        instrument_symbols = _outright_instrument_symbols(
            dbn_store,
            provider_symbol=context.provider_symbol,
        )
        batches = store.to_ndarray(schema="mbo", count=self._dbn_batch_size)
        for records in _iter_record_batches(batches):
            if len(records) == 0 or (records.dtype.names or ()) is None or "ts_event" not in (records.dtype.names or ()):
                continue
            batch_event_time_ms = (records["ts_event"] // 1_000_000).astype(np.int64, copy=False)
            if len(batch_event_time_ms) and int(batch_event_time_ms[0]) > int(end_ms):
                break
            if len(batch_event_time_ms) and int(batch_event_time_ms[-1]) < int(start_ms):
                continue
            batch_events, batch_contracts = self._events_from_records(
                    records,
                    dbn_store=dbn_store,
                    context=context,
                    source_file=source_file,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    instrument_symbols=instrument_symbols,
                    primary_window=primary_window,
            )
            events.extend(batch_events)
            if batch_contracts:
                contract_symbols = tuple(dict.fromkeys(contract_symbols + batch_contracts))
            
            
        
        return events, contract_symbols, sampled

    def _ensure_extracted_member(
        self,
        archive: zipfile.ZipFile,
        *,
        zip_info: DomFileInfo,
        member_name: str,
    ) -> Path:
        member = archive.getinfo(member_name)
        target_dir = self.extracted_cache_dir / zip_info.path.stem
        safe_name = Path(member_name).name
        target_path = target_dir / safe_name
        expected_size = int(member.file_size)
        if target_path.exists() and target_path.stat().st_size == expected_size:
            return target_path
        target_dir.mkdir(parents=True, exist_ok=True)

        write_path = target_path
        if write_path.exists():
            try:
                write_path.unlink()
            except OSError:
                write_path = target_dir / f"{int(member.CRC):08x}-{safe_name}"
                if write_path.exists() and write_path.stat().st_size == expected_size:
                    return write_path
                if write_path.exists():
                    write_path.unlink()

        with archive.open(member, "r") as source, write_path.open("wb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
        if write_path.stat().st_size != expected_size:
            raise CmeLocalDataError(f"Incomplete DOM MBO DBN extraction: {safe_name}")
        return write_path

    @staticmethod
    def _zip_metadata_end_ms(path: Path, *, context: DomContext) -> int | None:
        try:
            with zipfile.ZipFile(path) as archive:
                if "metadata.json" not in archive.namelist():
                    return None
                metadata = json.loads(archive.read("metadata.json").decode("utf-8"))
        except Exception:
            return None
        query = metadata.get("query") if isinstance(metadata, dict) else {}
        if not isinstance(query, dict):
            return None
        schema = str(query.get("schema", "")).strip().lower()
        if schema and schema != "mbo":
            return None
        dataset = str(query.get("dataset", "")).strip()
        if dataset and context.dataset and dataset != context.dataset:
            return None
        symbols = tuple(str(item).strip().upper() for item in query.get("symbols", []) if str(item).strip())
        if symbols and context.provider_symbol.upper() not in symbols:
            return None
        end_ns = _optional_int(query.get("end"))
        return int(end_ns) // 1_000_000 if end_ns is not None and end_ns > 0 else None

    @staticmethod
    def _zip_latest_member_bounds_ms(path: Path) -> tuple[int, int] | None:
        try:
            with zipfile.ZipFile(path) as archive:
                dated_members = [
                    utc_date
                    for name in archive.namelist()
                    if name.lower().endswith((".dbn", ".dbn.zst"))
                    if (utc_date := _utc_date_from_member_name(name)) is not None
                ]
        except Exception:
            return None
        if not dated_members:
            return None
        latest_date = max(dated_members)
        start_dt = datetime.fromisoformat(latest_date).replace(tzinfo=UTC)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int((start_dt + timedelta(days=1)).timestamp() * 1000)
        return start_ms, end_ms

    @staticmethod
    def _zip_available_member_bounds_ms(path: Path) -> tuple[int, int] | None:
        try:
            with zipfile.ZipFile(path) as archive:
                dated_members = [
                    utc_date
                    for name in archive.namelist()
                    if name.lower().endswith((".dbn", ".dbn.zst"))
                    if (utc_date := _utc_date_from_member_name(name)) is not None
                ]
        except Exception:
            return None
        if not dated_members:
            return None
        first_dt = datetime.fromisoformat(min(dated_members)).replace(tzinfo=UTC)
        last_dt = datetime.fromisoformat(max(dated_members)).replace(tzinfo=UTC)
        start_ms = int(first_dt.timestamp() * 1000)
        end_ms = int((last_dt + timedelta(days=1)).timestamp() * 1000)
        return start_ms, end_ms

    @staticmethod
    def _zip_available_member_dates(path: Path) -> tuple[str, ...]:
        try:
            with zipfile.ZipFile(path) as archive:
                dated_members = {
                    utc_date
                    for name in archive.namelist()
                    if name.lower().endswith((".dbn", ".dbn.zst"))
                    if (utc_date := _utc_date_from_member_name(name)) is not None
                }
        except Exception:
            return ()
        return tuple(sorted(dated_members))

    @staticmethod
    def _zip_member_names_for_window(
        path: Path,
        *,
        start_ms: int,
        end_ms: int,
    ) -> tuple[str, ...]:
        with zipfile.ZipFile(path) as archive:
            member_names = tuple(
                sorted(
                    name
                    for name in archive.namelist()
                    if name.lower().endswith((".dbn", ".dbn.zst"))
                )
            )
        if not member_names:
            return ()

        dated_members = [
            (utc_date, member_name)
            for member_name in member_names
            if (utc_date := _utc_date_from_member_name(member_name)) is not None
        ]
        if not dated_members:
            return member_names

        if int(start_ms) <= 0 and int(end_ms) >= 2**62:
            latest_date = max(utc_date for utc_date, _member_name in dated_members)
            return tuple(
                member_name
                for utc_date, member_name in dated_members
                if utc_date == latest_date
            )

        start_date = datetime.fromtimestamp(max(0, int(start_ms)) / 1000, tz=UTC).date().isoformat()
        if int(end_ms) >= 2**62:
            end_date = max(utc_date for utc_date, _member_name in dated_members)
        else:
            end_date = datetime.fromtimestamp(max(0, int(end_ms)) / 1000, tz=UTC).date().isoformat()
        return tuple(
            member_name
            for utc_date, member_name in dated_members
            if start_date <= utc_date <= end_date
        )

    def _events_from_records(
        self,
        records: np.ndarray,
        *,
        dbn_store: object,
        context: DomContext,
        source_file: str,
        start_ms: int,
        end_ms: int,
        instrument_symbols: dict[int, str] | None = None,
        primary_window: tuple[int, int] | None = None,
    ) -> tuple[list[DomRawEvent], tuple[str, ...]]:
        if len(records) == 0:
            return [], ()
        field_names = records.dtype.names or ()
        if "ts_event" not in field_names:
            return [], ()

        event_time_ms = (records["ts_event"] // 1_000_000).astype(np.int64, copy=False)
        time_mask = (event_time_ms >= int(start_ms)) & (event_time_ms <= int(end_ms))
        if not bool(np.any(time_mask)):
            return [], ()
        records = records[time_mask]

        resolved_symbols = instrument_symbols
        if resolved_symbols is None:
            resolved_symbols = _outright_instrument_symbols(
                dbn_store,
                provider_symbol=context.provider_symbol,
            )
        contract_symbols = tuple(dict.fromkeys(resolved_symbols.values()))
        if resolved_symbols and "instrument_id" in field_names:
            outright_ids = np.fromiter(resolved_symbols, dtype=np.uint32)
            records = records[np.isin(records["instrument_id"], outright_ids)]
            active_instrument_id = _active_instrument_id_from_records(
                records,
                field_names,
                primary_window=primary_window,
            )
            if active_instrument_id is not None:
                records = records[records["instrument_id"] == active_instrument_id]
                active_symbol = resolved_symbols.get(active_instrument_id)
                contract_symbols = (active_symbol,) if active_symbol else ()
        if len(records) == 0:
            return [], contract_symbols

        order = np.argsort(records["ts_event"], kind="stable")
        ordered = records[order]
        events: list[DomRawEvent] = []
        sequence = 0
        for record in ordered:
            price = _price_from_record(record, field_names)
            size = _positive_int(_field_value(record, field_names, "size"))
            action = _decode_ascii(_field_value(record, field_names, "action")).upper()
            side = _normalize_side(_decode_ascii(_field_value(record, field_names, "side")))
            order_id = _order_id_from_record(record, field_names)
            instrument_id = _positive_int(_field_value(record, field_names, "instrument_id"))
            sequence += 1
            events.append(
                DomRawEvent(
                    ts_event_ms=_timestamp_to_ms(_field_value(record, field_names, "ts_event")),
                    price=price,
                    size=size,
                    side=side,
                    action=action,
                    order_id=order_id,
                    instrument_id=instrument_id,
                    sequence=sequence,
                    source_file=source_file,
                )
            )
        return events, contract_symbols


def _primary_window(
    start_ms: int,
    end_ms: int,
    *,
    primary_start_ms: int | None,
    primary_end_ms: int | None,
) -> tuple[int, int] | None:
    if primary_start_ms is None or primary_end_ms is None:
        return None
    primary_start = max(int(start_ms), int(primary_start_ms))
    primary_end = min(int(end_ms), int(primary_end_ms))
    if primary_end < primary_start:
        return None
    return primary_start, primary_end


def _sample_dom_events(events: list[DomRawEvent], max_events: int) -> list[DomRawEvent]:
    limit = max(1, int(max_events))
    if len(events) <= limit:
        return events
    ordered = sorted(events, key=lambda item: (int(item.ts_event_ms), int(item.sequence)))
    step = (len(ordered) - 1) / max(1, limit - 1)
    return [ordered[round(index * step)] for index in range(limit)]


def _limit_raw_events(
    events: list[DomRawEvent],
    max_events: int,
    *,
    primary_window: tuple[int, int] | None,
) -> list[DomRawEvent]:
    limit = max(1, int(max_events))
    ordered = sorted(events, key=lambda item: (int(item.ts_event_ms), int(item.sequence)))
    if len(ordered) <= limit:
        return ordered
    if primary_window is None:
        return ordered[:limit]
    primary_start_ms, primary_end_ms = primary_window
    primary = [
        event
        for event in ordered
        if int(primary_start_ms) <= int(event.ts_event_ms) <= int(primary_end_ms)
    ]
    if len(primary) >= limit:
        return _sample_dom_events(primary, limit)
    margin = [
        event
        for event in ordered
        if int(event.ts_event_ms) < int(primary_start_ms)
        or int(event.ts_event_ms) > int(primary_end_ms)
    ]
    sampled_margin = _sample_dom_events(margin, limit - len(primary)) if margin else []
    return sorted(primary + sampled_margin, key=lambda item: (int(item.ts_event_ms), int(item.sequence)))


def _format_index_price(value: Decimal | None) -> str:
    return "" if value is None else format(value.normalize(), "f")


def _field_value(record: object, field_names: tuple[str, ...], field: str) -> object:
    if field not in field_names:
        return None
    return record[field]


def _decode_ascii(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("ascii", errors="ignore")
    try:
        if hasattr(value, "tobytes"):
            return value.tobytes().decode("ascii", errors="ignore").strip("\x00")
    except Exception:
        return str(value)
    return str(value)


def _positive_int(value: object) -> int:
    parsed = _optional_int(value)
    return max(0, int(parsed or 0))


def _price_from_record(record: object, field_names: tuple[str, ...]) -> Decimal | None:
    value = _field_value(record, field_names, "price")
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    parsed = _optional_int(value)
    if parsed is None:
        try:
            return Decimal(str(value))
        except Exception:
            return None
    if abs(parsed) >= 9_000_000_000_000_000_000:
        return None
    return Decimal(parsed) / Decimal(DBN_FIXED_PRICE_SCALE)


def _active_instrument_id_from_records(
    records: np.ndarray,
    field_names: tuple[str, ...],
    *,
    primary_window: tuple[int, int] | None,
) -> int | None:
    if len(records) == 0 or "instrument_id" not in field_names:
        return None
    candidates = records
    if primary_window is not None and "ts_event" in field_names:
        event_time_ms = (records["ts_event"] // 1_000_000).astype(np.int64, copy=False)
        primary_start_ms, primary_end_ms = primary_window
        primary_mask = (event_time_ms >= int(primary_start_ms)) & (event_time_ms <= int(primary_end_ms))
        if bool(np.any(primary_mask)):
            candidates = records[primary_mask]
    if len(candidates) == 0:
        return None
    instrument_ids = candidates["instrument_id"].astype(np.uint64, copy=False)
    sizes = (
        candidates["size"].astype(np.int64, copy=False)
        if "size" in field_names
        else np.ones(len(candidates), dtype=np.int64)
    )
    unique_ids, inverse = np.unique(instrument_ids, return_inverse=True)
    totals = np.zeros(len(unique_ids), dtype=np.int64)
    np.add.at(totals, inverse, sizes)
    if len(unique_ids) == 0:
        return None
    return int(unique_ids[int(np.argmax(totals))])


def _normalize_side(value: str) -> str:
    normalized = value.strip().upper()
    if normalized in {"B", "BID", "BUY"}:
        return "BID"
    if normalized in {"A", "ASK", "SELL"}:
        return "ASK"
    return "NONE"


def _order_id_from_record(record: object, field_names: tuple[str, ...]) -> str:
    value = _field_value(record, field_names, "order_id")
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text in {"0", "None"} else text


def _iter_record_batches(records: object) -> object:
    if isinstance(records, np.ndarray):
        return (records,)
    return records
