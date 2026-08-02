from __future__ import annotations

import json
import logging
import re
import threading
import zipfile
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from absorption.binance_aggtrade_ws_client import AggTradeEvent

LOGGER = logging.getLogger(__name__)


class CmeLocalDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class CmeArchiveInfo:
    path: Path
    dataset: str
    schema: str
    symbols: tuple[str, ...]
    dbn_members: tuple[str, ...]
    condition_by_date: dict[str, str]
    query_start_ns: int | None = None
    query_end_ns: int | None = None


@dataclass(frozen=True)
class CmeDbnPartition:
    archive_path: Path
    member_name: str
    utc_date: str


@dataclass
class _CumulativeDeltaCacheEntry:
    start_ms: int
    loaded_end_ms: int
    event_time_ms: Any
    prefix_delta: Any


class CmeLocalDataCatalog:
    def __init__(
        self,
        *,
        data_dir: Path,
        dataset: str,
        schema: str,
        default_tick_size: str,
    ) -> None:
        self.data_dir = data_dir
        self.dataset = dataset
        self.schema = schema
        self.default_tick_size = Decimal(str(default_tick_size))
        self._archives = self._discover_archives()

    @property
    def archives(self) -> tuple[CmeArchiveInfo, ...]:
        return self._archives

    def available_symbols(self) -> tuple[str, ...]:
        symbols = {
            symbol
            for archive in self._archives
            for symbol in archive.symbols
            if archive.dataset == self.dataset and archive.schema == self.schema
        }
        return tuple(sorted(symbols))

    def has_symbol(self, provider_symbol: str) -> bool:
        return provider_symbol.upper() in self.available_symbols()

    def tick_size_for(self, provider_symbol: str) -> Decimal:
        del provider_symbol
        return self.default_tick_size

    def archives_for_symbol(self, provider_symbol: str) -> tuple[CmeArchiveInfo, ...]:
        normalized_symbol = provider_symbol.upper()
        return tuple(
            archive
            for archive in self._archives
            if normalized_symbol in archive.symbols
            and archive.dataset == self.dataset
            and archive.schema == self.schema
        )

    def partitions_for_symbol(self, provider_symbol: str) -> tuple[CmeDbnPartition, ...]:
        partitions = []
        for archive in self.archives_for_symbol(provider_symbol):
            for member_name in archive.dbn_members:
                utc_date = _utc_date_from_member_name(member_name)
                if utc_date is None:
                    continue
                partitions.append(
                    CmeDbnPartition(
                        archive_path=archive.path,
                        member_name=member_name,
                        utc_date=utc_date,
                    )
                )
        return tuple(
            sorted(
                partitions,
                key=lambda item: (item.utc_date, str(item.archive_path), item.member_name),
            )
        )

    def trading_days_for_symbol(self, provider_symbol: str) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    partition.utc_date
                    for partition in self.partitions_for_symbol(provider_symbol)
                    if date.fromisoformat(partition.utc_date).weekday() < 5
                }
            )
        )

    def latest_available_end_ms(self, provider_symbol: str) -> int | None:
        end_values = [
            int(archive.query_end_ns) // 1_000_000
            for archive in self.archives_for_symbol(provider_symbol)
            if archive.query_end_ns is not None
        ]
        return max(end_values) if end_values else None

    def _discover_archives(self) -> tuple[CmeArchiveInfo, ...]:
        if not self.data_dir.exists():
            return ()

        archives: list[CmeArchiveInfo] = []
        for zip_path in sorted(self.data_dir.glob("*.zip")):
            try:
                archive = self._read_archive_info(zip_path)
            except Exception:
                LOGGER.exception("CME_LOCAL_ARCHIVE_DISCOVERY_ERROR | path=%s", zip_path)
                continue
            if archive is not None:
                archives.append(archive)
        return tuple(archives)

    def _read_archive_info(self, zip_path: Path) -> CmeArchiveInfo | None:
        with zipfile.ZipFile(zip_path) as archive:
            names = set(archive.namelist())
            if "metadata.json" not in names:
                return None
            metadata = json.loads(archive.read("metadata.json").decode("utf-8"))
            query = metadata.get("query") if isinstance(metadata, dict) else {}
            dataset = str(query.get("dataset", "")).strip()
            schema = str(query.get("schema", "")).strip()
            symbols = tuple(str(item).upper() for item in query.get("symbols", []) if str(item).strip())
            query_start_ns = _optional_int(query.get("start"))
            query_end_ns = _optional_int(query.get("end"))
            dbn_members = tuple(
                sorted(name for name in names if name.lower().endswith(".dbn.zst"))
            )
            condition_by_date: dict[str, str] = {}
            if "condition.json" in names:
                condition_rows = json.loads(archive.read("condition.json").decode("utf-8"))
                if isinstance(condition_rows, list):
                    condition_by_date = {
                        str(item.get("date")): str(item.get("condition", ""))
                        for item in condition_rows
                        if isinstance(item, dict) and item.get("date")
                    }
        return CmeArchiveInfo(
            path=zip_path,
            dataset=dataset,
            schema=schema,
            symbols=symbols,
            dbn_members=dbn_members,
            condition_by_date=condition_by_date,
            query_start_ns=query_start_ns,
            query_end_ns=query_end_ns,
        )


class CmeLocalDbnTradeStore:
    def __init__(
        self,
        catalog: CmeLocalDataCatalog,
        *,
        partition_cache_size: int = 8,
        trading_day_cache_size: int = 4,
        cumulative_delta_cache_size: int = 64,
    ) -> None:
        self.catalog = catalog
        self._trade_cache_by_symbol: dict[str, tuple[AggTradeEvent, ...]] = {}
        self._latest_event_time_cache: dict[str, int | None] = {}
        self._partition_cache_size = max(1, int(partition_cache_size))
        self._trading_day_cache_size = max(
            1,
            int(trading_day_cache_size),
        )
        self._cumulative_delta_cache_size = max(
            1,
            int(cumulative_delta_cache_size),
        )
        self._partition_trade_cache: OrderedDict[
            tuple[str, str, str],
            tuple[AggTradeEvent, ...],
        ] = OrderedDict()
        self._partition_frame_cache: OrderedDict[tuple[str, str, str], Any] = OrderedDict()
        self._trading_day_frame_cache: OrderedDict[
            tuple[str, str, int],
            Any,
        ] = OrderedDict()
        self._cumulative_delta_cache: OrderedDict[
            tuple[str, str, str, str],
            _CumulativeDeltaCacheEntry,
        ] = OrderedDict()
        self._partition_cache_lock = threading.RLock()
        self._cumulative_delta_cache_lock = threading.RLock()
        self._cumulative_delta_metrics = threading.local()

    def trades_for_symbol(self, provider_symbol: str) -> tuple[AggTradeEvent, ...]:
        normalized_symbol = provider_symbol.upper()
        cached = self._trade_cache_by_symbol.get(normalized_symbol)
        if cached is not None:
            return cached

        trades = tuple(self._load_trades(normalized_symbol))
        self._trade_cache_by_symbol[normalized_symbol] = trades
        return trades

    def trades_for_trading_day(
        self,
        provider_symbol: str,
        trading_day: str,
        *,
        session_start_hour_chicago: int,
    ) -> tuple[AggTradeEvent, ...]:
        start_ms, end_ms = trading_day_bounds_utc_ms(
            trading_day,
            session_start_hour_chicago=session_start_hour_chicago,
        )
        start_date = datetime.fromtimestamp(start_ms / 1000, tz=UTC).date()
        end_date = datetime.fromtimestamp((end_ms - 1) / 1000, tz=UTC).date()
        required_dates = {
            (start_date + timedelta(days=offset)).isoformat()
            for offset in range((end_date - start_date).days + 1)
        }
        events = [
            event
            for partition in self.catalog.partitions_for_symbol(provider_symbol)
            if partition.utc_date in required_dates
            for event in self.trades_for_partition(partition, provider_symbol=provider_symbol)
            if start_ms <= int(event.event_time_ms) < end_ms
        ]
        events.sort(key=lambda item: (int(item.event_time_ms), int(item.aggregate_trade_id)))
        return tuple(events)

    def trades_for_partition(
        self,
        partition: CmeDbnPartition,
        *,
        provider_symbol: str,
    ) -> tuple[AggTradeEvent, ...]:
        cache_key = (
            str(partition.archive_path),
            partition.member_name,
            provider_symbol.upper(),
        )
        with self._partition_cache_lock:
            cached = self._partition_trade_cache.get(cache_key)
            if cached is not None:
                self._partition_trade_cache.move_to_end(cache_key)
                return cached

            frame = self.trade_frame_for_partition(
                partition,
                provider_symbol=provider_symbol,
            )
            events: list[AggTradeEvent] = []
            self._extend_events_from_frame(
                events=events,
                frame=frame,
                provider_symbol=provider_symbol.upper(),
                aggregate_trade_id=0,
            )
            result = tuple(events)
            self._partition_trade_cache[cache_key] = result
            self._partition_trade_cache.move_to_end(cache_key)
            while len(self._partition_trade_cache) > self._partition_cache_size:
                self._partition_trade_cache.popitem(last=False)
            return result

    def trade_frame_for_trading_day(
        self,
        provider_symbol: str,
        trading_day: str,
        *,
        session_start_hour_chicago: int,
    ) -> Any:
        import pandas as pd

        self._ensure_trading_day_cache_state()
        cache_key = (
            provider_symbol.strip().upper(),
            str(trading_day),
            int(session_start_hour_chicago),
        )
        with self._partition_cache_lock:
            cached = self._trading_day_frame_cache.get(cache_key)
            if cached is not None:
                self._trading_day_frame_cache.move_to_end(cache_key)
                return cached

        start_ms, end_ms = trading_day_bounds_utc_ms(
            trading_day,
            session_start_hour_chicago=session_start_hour_chicago,
        )
        start_date = datetime.fromtimestamp(start_ms / 1000, tz=UTC).date()
        end_date = datetime.fromtimestamp((end_ms - 1) / 1000, tz=UTC).date()
        required_dates = {
            (start_date + timedelta(days=offset)).isoformat()
            for offset in range((end_date - start_date).days + 1)
        }
        frames = [
            self.trade_frame_for_partition(
                partition,
                provider_symbol=provider_symbol,
            )
            for partition in self.catalog.partitions_for_symbol(provider_symbol)
            if partition.utc_date in required_dates
        ]
        if not frames:
            result = pd.DataFrame(
                columns=["ts_event", "side", "price", "size", "symbol"]
            )
            self._store_trading_day_frame(cache_key, result)
            return result

        combined = pd.concat(frames, ignore_index=True)
        start_ns = int(start_ms) * 1_000_000
        end_ns = int(end_ms) * 1_000_000
        filtered = combined.loc[
            (combined["ts_event"] >= start_ns) & (combined["ts_event"] < end_ns),
            ["ts_event", "side", "price", "size", "symbol"],
        ]
        active_contract = _active_outright_contract(filtered, provider_symbol)
        if active_contract:
            filtered = filtered.loc[filtered["symbol"].astype(str).str.upper() == active_contract]
        result = filtered.sort_values("ts_event", kind="stable", ignore_index=True)
        result.attrs["contract_symbol"] = active_contract
        result.attrs["contract_symbols"] = (
            (active_contract,) if active_contract else ()
        )
        self._store_trading_day_frame(cache_key, result)
        return result

    def trade_frame_for_time_range(
        self,
        provider_symbol: str,
        *,
        start_ms: int,
        end_ms: int,
        session_start_hour_chicago: int,
    ) -> Any:
        import numpy as np
        import pandas as pd

        if end_ms <= start_ms:
            return pd.DataFrame(columns=["ts_event", "side", "price", "size", "symbol"])

        start_ns = int(start_ms) * 1_000_000
        end_ns = int(end_ms) * 1_000_000
        frames = []
        contract_symbols = []
        first_day = date.fromisoformat(
            trading_day_for_timestamp_ms(
                start_ms,
                session_start_hour_chicago=session_start_hour_chicago,
            )
        )
        last_day = date.fromisoformat(
            trading_day_for_timestamp_ms(
                max(start_ms, end_ms - 1),
                session_start_hour_chicago=session_start_hour_chicago,
            )
        )
        current_day = first_day
        while current_day <= last_day:
            day_frame = self.trade_frame_for_trading_day(
                provider_symbol,
                current_day.isoformat(),
                session_start_hour_chicago=session_start_hour_chicago,
            )
            current_day += timedelta(days=1)
            if day_frame.empty:
                continue
            event_times = day_frame["ts_event"].to_numpy(
                dtype=np.uint64,
                copy=False,
            )
            start_index = int(
                np.searchsorted(event_times, start_ns, side="left")
            )
            end_index = int(
                np.searchsorted(event_times, end_ns, side="left")
            )
            viewport_frame = day_frame.iloc[
                start_index:end_index
            ][["ts_event", "side", "price", "size", "symbol"]]
            viewport_frame.attrs.update(day_frame.attrs)
            if not viewport_frame.empty:
                frames.append(viewport_frame)
                contract_symbol = str(
                    day_frame.attrs.get("contract_symbol", "")
                )
                if contract_symbol:
                    contract_symbols.append(contract_symbol)
        if not frames:
            return pd.DataFrame(columns=["ts_event", "side", "price", "size", "symbol"])

        result = pd.concat(frames, ignore_index=True)
        unique_contracts = tuple(dict.fromkeys(contract_symbols))
        result.attrs["contract_symbols"] = unique_contracts
        result.attrs["contract_symbol"] = unique_contracts[0] if len(unique_contracts) == 1 else ""
        return result

    def _ensure_trading_day_cache_state(self) -> None:
        if not hasattr(self, "_trading_day_frame_cache"):
            self._trading_day_frame_cache = OrderedDict()
        if not hasattr(self, "_trading_day_cache_size"):
            self._trading_day_cache_size = 4
        if not hasattr(self, "_partition_cache_lock"):
            self._partition_cache_lock = threading.RLock()

    def _store_trading_day_frame(
        self,
        cache_key: tuple[str, str, int],
        frame: Any,
    ) -> None:
        self._ensure_trading_day_cache_state()
        with self._partition_cache_lock:
            self._trading_day_frame_cache[cache_key] = frame
            self._trading_day_frame_cache.move_to_end(cache_key)
            while (
                len(self._trading_day_frame_cache)
                > self._trading_day_cache_size
            ):
                self._trading_day_frame_cache.popitem(last=False)

    def cumulative_contract_deltas(
        self,
        provider_symbol: str,
        *,
        candle_open_times_ms: Iterable[int],
        interval_ms: int,
        timeframe: str | None = None,
        session_start_hour_chicago: int,
        new_york_session_start_hour: int = 9,
        new_york_session_start_minute: int = 30,
        new_york_session_end_hour: int = 16,
        new_york_session_end_minute: int = 0,
    ) -> dict[int, dict[str, int | str | None]]:
        import numpy as np

        candle_opens = sorted({int(value) for value in candle_open_times_ms if int(value) > 0})
        if not candle_opens:
            return {}

        reset_contexts = {
            candle_open: cumulative_delta_reset_context(
                candle_open,
                session_start_hour_chicago=session_start_hour_chicago,
                new_york_session_start_hour=new_york_session_start_hour,
                new_york_session_start_minute=new_york_session_start_minute,
                new_york_session_end_hour=new_york_session_end_hour,
                new_york_session_end_minute=new_york_session_end_minute,
            )
            for candle_open in candle_opens
        }
        normalized_symbol = provider_symbol.strip().upper()
        normalized_timeframe = (
            str(timeframe).strip().upper()
            if timeframe
            else f"{int(interval_ms)}MS"
        )
        hit_count = 0
        miss_count = 0
        day_entries: dict[str, _CumulativeDeltaCacheEntry] = {}
        session_entries: dict[str, _CumulativeDeltaCacheEntry] = {}

        candles_by_trading_day: dict[str, list[int]] = {}
        candles_by_ny_session: dict[str, list[int]] = {}
        for candle_open in candle_opens:
            context = reset_contexts[candle_open]
            candles_by_trading_day.setdefault(
                str(context["trading_day"]),
                [],
            ).append(candle_open)
            if context["ny_session_active"]:
                candles_by_ny_session.setdefault(
                    str(context["ny_session_date"]),
                    [],
                ).append(candle_open)

        for trading_day, opens in candles_by_trading_day.items():
            context = reset_contexts[opens[0]]
            entry, cache_hit = self._cumulative_delta_series(
                provider_symbol=normalized_symbol,
                timeframe=normalized_timeframe,
                session_kind="TRADING_DAY",
                session_id=trading_day,
                start_ms=int(context["trading_day_start_ms"]),
                required_end_ms=max(opens) + int(interval_ms),
                session_start_hour_chicago=session_start_hour_chicago,
            )
            day_entries[trading_day] = entry
            hit_count += int(cache_hit)
            miss_count += int(not cache_hit)

        for session_date, opens in candles_by_ny_session.items():
            context = reset_contexts[opens[0]]
            entry, cache_hit = self._cumulative_delta_series(
                provider_symbol=normalized_symbol,
                timeframe=normalized_timeframe,
                session_kind="NEW_YORK",
                session_id=session_date,
                start_ms=int(context["ny_session_start_ms"]),
                required_end_ms=max(opens) + int(interval_ms),
                session_start_hour_chicago=session_start_hour_chicago,
            )
            session_entries[session_date] = entry
            hit_count += int(cache_hit)
            miss_count += int(not cache_hit)

        self._set_cumulative_delta_cache_metrics(
            hit_count=hit_count,
            miss_count=miss_count,
        )
        return {
            candle_open: {
                "session_cumulative_delta": (
                    self._cumulative_delta_at(
                        session_entries[str(reset_contexts[candle_open]["ny_session_date"])],
                        candle_open + interval_ms,
                    )
                    if reset_contexts[candle_open]["ny_session_active"]
                    else None
                ),
                "day_cumulative_delta": self._cumulative_delta_at(
                    day_entries[str(reset_contexts[candle_open]["trading_day"])],
                    candle_open + interval_ms,
                ),
                "trading_day": reset_contexts[candle_open]["trading_day"],
                "ny_session_date": reset_contexts[candle_open]["ny_session_date"],
            }
            for candle_open in candle_opens
        }

    def _cumulative_delta_series(
        self,
        *,
        provider_symbol: str,
        timeframe: str,
        session_kind: str,
        session_id: str,
        start_ms: int,
        required_end_ms: int,
        session_start_hour_chicago: int,
    ) -> tuple[_CumulativeDeltaCacheEntry, bool]:
        import numpy as np

        self._ensure_cumulative_delta_cache_state()
        key = (
            provider_symbol.strip().upper(),
            timeframe.strip().upper(),
            session_kind,
            session_id,
        )
        with self._cumulative_delta_cache_lock:
            cached = self._cumulative_delta_cache.get(key)
            if cached is not None and cached.loaded_end_ms >= required_end_ms:
                self._cumulative_delta_cache.move_to_end(key)
                return cached, True

            load_start_ms = (
                cached.loaded_end_ms
                if cached is not None
                else int(start_ms)
            )
            frame = self.trade_frame_for_time_range(
                provider_symbol,
                start_ms=load_start_ms,
                end_ms=required_end_ms,
                session_start_hour_chicago=session_start_hour_chicago,
            )
            if len(frame):
                frame = frame.loc[
                    (frame["ts_event"] >= int(load_start_ms) * 1_000_000)
                    & (frame["ts_event"] < int(required_end_ms) * 1_000_000)
                ]
            event_time_ms, signed_contracts = self._signed_contract_delta_arrays(frame)

            if cached is None:
                prefix_delta = np.concatenate(
                    (
                        np.array([0], dtype=np.int64),
                        np.cumsum(signed_contracts, dtype=np.int64),
                    )
                )
                entry = _CumulativeDeltaCacheEntry(
                    start_ms=int(start_ms),
                    loaded_end_ms=int(required_end_ms),
                    event_time_ms=event_time_ms,
                    prefix_delta=prefix_delta,
                )
            else:
                next_prefix = np.cumsum(signed_contracts, dtype=np.int64)
                if len(next_prefix):
                    next_prefix = next_prefix + int(cached.prefix_delta[-1])
                entry = _CumulativeDeltaCacheEntry(
                    start_ms=cached.start_ms,
                    loaded_end_ms=int(required_end_ms),
                    event_time_ms=np.concatenate(
                        (cached.event_time_ms, event_time_ms)
                    ),
                    prefix_delta=np.concatenate(
                        (cached.prefix_delta, next_prefix)
                    ),
                )

            self._cumulative_delta_cache[key] = entry
            self._cumulative_delta_cache.move_to_end(key)
            while (
                len(self._cumulative_delta_cache)
                > self._cumulative_delta_cache_size
            ):
                self._cumulative_delta_cache.popitem(last=False)
            return entry, False

    @staticmethod
    def _signed_contract_delta_arrays(frame: Any) -> tuple[Any, Any]:
        import numpy as np

        if len(frame) == 0:
            return (
                np.array([], dtype=np.int64),
                np.array([], dtype=np.int64),
            )
        event_time_ms = (
            frame["ts_event"].to_numpy(dtype=np.uint64, copy=False)
            // 1_000_000
        ).astype(np.int64, copy=False)
        size = frame["size"].to_numpy(dtype=np.int64, copy=False)
        side = frame["side"].astype(str).str.upper().to_numpy(copy=False)
        signed_contracts = np.where(
            side == "B",
            size,
            np.where(side == "A", -size, 0),
        ).astype(np.int64, copy=False)
        order = np.argsort(event_time_ms, kind="stable")
        return event_time_ms[order], signed_contracts[order]

    @staticmethod
    def _cumulative_delta_at(
        entry: _CumulativeDeltaCacheEntry,
        end_ms: int,
    ) -> int:
        import numpy as np

        end_index = int(
            np.searchsorted(
                entry.event_time_ms,
                int(end_ms),
                side="left",
            )
        )
        return int(entry.prefix_delta[end_index])

    def _ensure_cumulative_delta_cache_state(self) -> None:
        if not hasattr(self, "_cumulative_delta_cache"):
            self._cumulative_delta_cache = OrderedDict()
        if not hasattr(self, "_cumulative_delta_cache_size"):
            self._cumulative_delta_cache_size = 64
        if not hasattr(self, "_cumulative_delta_cache_lock"):
            self._cumulative_delta_cache_lock = threading.RLock()
        if not hasattr(self, "_cumulative_delta_metrics"):
            self._cumulative_delta_metrics = threading.local()

    def _set_cumulative_delta_cache_metrics(
        self,
        *,
        hit_count: int,
        miss_count: int,
    ) -> None:
        self._ensure_cumulative_delta_cache_state()
        self._cumulative_delta_metrics.value = {
            "hit_count": int(hit_count),
            "miss_count": int(miss_count),
        }

    def cumulative_delta_cache_metrics(self) -> dict[str, int]:
        self._ensure_cumulative_delta_cache_state()
        value = getattr(self._cumulative_delta_metrics, "value", None)
        if not isinstance(value, dict):
            return {"hit_count": 0, "miss_count": 0}
        return {
            "hit_count": int(value.get("hit_count", 0)),
            "miss_count": int(value.get("miss_count", 0)),
        }

    def latest_event_time_ms(
        self,
        provider_symbol: str,
        *,
        before_ms: int | None = None,
    ) -> int | None:
        normalized_symbol = provider_symbol.upper()
        if before_ms is None and normalized_symbol in self._latest_event_time_cache:
            return self._latest_event_time_cache[normalized_symbol]

        before_ns = int(before_ms) * 1_000_000 if before_ms is not None else None
        for partition in reversed(self.catalog.partitions_for_symbol(normalized_symbol)):
            partition_start_ms = int(
                datetime.fromisoformat(partition.utc_date).replace(tzinfo=UTC).timestamp() * 1000
            )
            if before_ms is not None and partition_start_ms >= before_ms:
                continue
            frame = self.trade_frame_for_partition(
                partition,
                provider_symbol=normalized_symbol,
            )
            candidates = _outright_contract_rows(frame, normalized_symbol)
            if before_ns is not None:
                candidates = candidates.loc[candidates["ts_event"] < before_ns]
            if candidates.empty:
                continue
            result = int(candidates["ts_event"].max()) // 1_000_000
            if before_ms is None:
                self._latest_event_time_cache[normalized_symbol] = result
            return result

        if before_ms is None:
            self._latest_event_time_cache[normalized_symbol] = None
        return None

    def earliest_partition_time_ms(self, provider_symbol: str) -> int | None:
        partitions = self.catalog.partitions_for_symbol(provider_symbol)
        if not partitions:
            return None
        return int(
            datetime.fromisoformat(partitions[0].utc_date).replace(tzinfo=UTC).timestamp() * 1000
        )

    def latest_available_end_ms(self, provider_symbol: str) -> int | None:
        return self.catalog.latest_available_end_ms(provider_symbol)

    def trade_frame_for_partition(
        self,
        partition: CmeDbnPartition,
        *,
        provider_symbol: str,
    ) -> Any:
        cache_key = (
            str(partition.archive_path),
            partition.member_name,
            provider_symbol.upper(),
        )
        with self._partition_cache_lock:
            cached = self._partition_frame_cache.get(cache_key)
            if cached is not None:
                self._partition_frame_cache.move_to_end(cache_key)
                return cached

            databento = _import_databento()
            with zipfile.ZipFile(partition.archive_path) as archive:
                raw_bytes = archive.read(partition.member_name)
            try:
                dbn_store = databento.DBNStore.from_bytes(raw_bytes)
            except TypeError:
                dbn_store = databento.DBNStore.from_bytes(BytesIO(raw_bytes))
            frame = _trade_frame_from_dbn_store(
                dbn_store,
                provider_symbol=provider_symbol,
            )
            self._partition_frame_cache[cache_key] = frame
            self._partition_frame_cache.move_to_end(cache_key)
            while len(self._partition_frame_cache) > self._partition_cache_size:
                self._partition_frame_cache.popitem(last=False)
            return frame

    def _load_trades(self, provider_symbol: str) -> Iterable[AggTradeEvent]:
        archives = self.catalog.archives_for_symbol(provider_symbol)
        if not archives:
            return ()

        databento = _import_databento()
        events: list[AggTradeEvent] = []
        aggregate_trade_id = 0
        for archive_info in archives:
            with zipfile.ZipFile(archive_info.path) as archive:
                for member in archive_info.dbn_members:
                    raw_bytes = archive.read(member)
                    try:
                        dbn_store = databento.DBNStore.from_bytes(raw_bytes)
                    except TypeError:
                        dbn_store = databento.DBNStore.from_bytes(BytesIO(raw_bytes))
                    aggregate_trade_id = self._extend_events_from_store(
                        events=events,
                        dbn_store=dbn_store,
                        provider_symbol=provider_symbol,
                        aggregate_trade_id=aggregate_trade_id,
                    )

        events.sort(key=lambda item: (int(item.event_time_ms), int(item.aggregate_trade_id)))
        return events

    @staticmethod
    def _extend_events_from_store(
        *,
        events: list[AggTradeEvent],
        dbn_store: Any,
        provider_symbol: str,
        aggregate_trade_id: int,
    ) -> int:
        frames = dbn_store.to_df(
            price_type="decimal",
            pretty_ts=False,
            map_symbols=False,
            schema="trades",
            count=250_000,
        )
        for frame in _iter_dataframes(frames):
            aggregate_trade_id = CmeLocalDbnTradeStore._extend_events_from_frame(
                events=events,
                frame=frame,
                provider_symbol=provider_symbol,
                aggregate_trade_id=aggregate_trade_id,
            )
        return aggregate_trade_id

    @staticmethod
    def _extend_events_from_frame(
        *,
        events: list[AggTradeEvent],
        frame: Any,
        provider_symbol: str,
        aggregate_trade_id: int,
    ) -> int:
        previous_price: Decimal | None = None
        fixed_price_scale = Decimal("1000000000")
        for row in frame.itertuples(index=False):
            raw_price = getattr(row, "price")
            price = (
                Decimal(int(raw_price)) / fixed_price_scale
                if not isinstance(raw_price, Decimal)
                else raw_price
            )
            quantity = _to_decimal(getattr(row, "size", getattr(row, "quantity", 0)))
            if quantity <= 0:
                continue
            side = _trade_side(getattr(row, "side", ""), price, previous_price)
            event_time_ms = _timestamp_to_ms(getattr(row, "ts_event"))
            aggregate_trade_id += 1
            events.append(
                AggTradeEvent(
                    symbol=provider_symbol.upper(),
                    event_time_ms=event_time_ms,
                    price=price,
                    quantity=quantity,
                    side=side,
                    aggregate_trade_id=aggregate_trade_id,
                )
            )
            previous_price = price
        return aggregate_trade_id


class CmeHistoricalClient:
    """Scaffold for future CME API access; intentionally unused in local DBN mode."""

    def __init__(self, *, api_key: str = "", dataset: str = "GLBX.MDP3", schema: str = "trades") -> None:
        self.api_key = api_key
        self.dataset = dataset
        self.schema = schema

    def is_configured(self) -> bool:
        return bool(self.api_key.strip())


def trading_day_for_timestamp_ms(
    timestamp_ms: int,
    *,
    session_start_hour_chicago: int,
) -> str:
    chicago = ZoneInfo("America/Chicago")
    local_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).astimezone(chicago)
    trading_date = local_dt.date()
    if local_dt.hour >= session_start_hour_chicago:
        trading_date += timedelta(days=1)
    return trading_date.isoformat()


def trading_day_bounds_utc_ms(
    trading_day: str,
    *,
    session_start_hour_chicago: int,
) -> tuple[int, int]:
    chicago = ZoneInfo("America/Chicago")
    session_end_date = date.fromisoformat(trading_day)
    session_start_date = session_end_date - timedelta(days=1)
    start_local = datetime.combine(
        session_start_date,
        time(hour=session_start_hour_chicago),
        tzinfo=chicago,
    )
    end_local = datetime.combine(
        session_end_date,
        time(hour=session_start_hour_chicago),
        tzinfo=chicago,
    )
    return int(start_local.astimezone(UTC).timestamp() * 1000), int(end_local.astimezone(UTC).timestamp() * 1000)


def new_york_session_bounds_utc_ms(
    session_date: str,
    *,
    start_hour: int = 9,
    start_minute: int = 30,
    end_hour: int = 16,
    end_minute: int = 0,
) -> tuple[int, int]:
    new_york = ZoneInfo("America/New_York")
    local_date = date.fromisoformat(session_date)
    start_local = datetime.combine(
        local_date,
        time(hour=start_hour, minute=start_minute),
        tzinfo=new_york,
    )
    end_local = datetime.combine(
        local_date,
        time(hour=end_hour, minute=end_minute),
        tzinfo=new_york,
    )
    if end_local <= start_local:
        end_local += timedelta(days=1)
    return (
        int(start_local.astimezone(UTC).timestamp() * 1000),
        int(end_local.astimezone(UTC).timestamp() * 1000),
    )


def cumulative_delta_reset_context(
    timestamp_ms: int,
    *,
    session_start_hour_chicago: int,
    new_york_session_start_hour: int = 9,
    new_york_session_start_minute: int = 30,
    new_york_session_end_hour: int = 16,
    new_york_session_end_minute: int = 0,
) -> dict[str, int | str | bool]:
    new_york = ZoneInfo("America/New_York")
    local_dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).astimezone(new_york)
    session_start_local = datetime.combine(
        local_dt.date(),
        time(
            hour=new_york_session_start_hour,
            minute=new_york_session_start_minute,
        ),
        tzinfo=new_york,
    )
    session_end_local = datetime.combine(
        local_dt.date(),
        time(
            hour=new_york_session_end_hour,
            minute=new_york_session_end_minute,
        ),
        tzinfo=new_york,
    )
    session_active = session_start_local <= local_dt < session_end_local
    trading_day = trading_day_for_timestamp_ms(
        timestamp_ms,
        session_start_hour_chicago=session_start_hour_chicago,
    )
    trading_day_start_ms, _ = trading_day_bounds_utc_ms(
        trading_day,
        session_start_hour_chicago=session_start_hour_chicago,
    )
    return {
        "trading_day": trading_day,
        "trading_day_start_ms": trading_day_start_ms,
        "ny_session_date": local_dt.date().isoformat() if session_active else "",
        "ny_session_start_ms": int(session_start_local.astimezone(UTC).timestamp() * 1000),
        "ny_session_end_ms": int(session_end_local.astimezone(UTC).timestamp() * 1000),
        "ny_session_active": session_active,
    }


def normalize_cme_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Z0-9.]", "", symbol.upper())


def _utc_date_from_member_name(member_name: str) -> str | None:
    match = re.search(r"(?<!\d)(\d{8})(?!\d)", Path(member_name).name)
    if match is None:
        return None
    try:
        return datetime.strptime(match.group(1), "%Y%m%d").date().isoformat()
    except ValueError:
        return None


def _active_outright_contract(frame: Any, provider_symbol: str) -> str:
    candidates = _outright_contract_rows(frame, provider_symbol)
    if candidates.empty:
        return ""
    candidates = candidates.assign(_symbol=candidates["symbol"].astype(str).str.upper())
    volume_by_symbol = candidates.groupby("_symbol", sort=True)["size"].sum()
    return str(volume_by_symbol.idxmax())


def _outright_contract_rows(frame: Any, provider_symbol: str) -> Any:
    if len(frame) == 0 or "symbol" not in frame:
        return frame.iloc[0:0]
    if bool(frame.attrs.get("outright_only")):
        return frame
    base_symbol = provider_symbol.upper().removesuffix(".FUT")
    symbols = frame["symbol"].astype(str).str.upper()
    outright_pattern = rf"^{re.escape(base_symbol)}[FGHJKMNQUVXZ]\d{{1,2}}$"
    outright_mask = symbols.str.match(outright_pattern)
    candidates = frame.loc[outright_mask]
    if candidates.empty:
        candidates = frame.loc[~symbols.str.contains("-", regex=False)]
    return candidates


def _trade_frame_from_dbn_store(dbn_store: Any, *, provider_symbol: str) -> Any:
    import numpy as np
    import pandas as pd

    instrument_symbols = _outright_instrument_symbols(
        dbn_store,
        provider_symbol=provider_symbol,
    )
    if not instrument_symbols:
        return dbn_store.to_df(
            price_type="fixed",
            pretty_ts=False,
            map_symbols=True,
            schema="trades",
        )[["ts_event", "side", "price", "size", "symbol"]].copy()

    records = dbn_store.to_ndarray(schema="trades")
    if len(records) == 0:
        return pd.DataFrame(columns=["ts_event", "side", "price", "size", "symbol"])
    outright_ids = np.fromiter(instrument_symbols, dtype=np.uint32)
    records = records[np.isin(records["instrument_id"], outright_ids)]
    if len(records) == 0:
        return pd.DataFrame(columns=["ts_event", "side", "price", "size", "symbol"])

    instrument_ids = records["instrument_id"]
    symbol_values = pd.Series(instrument_ids, copy=False).map(instrument_symbols).to_numpy(copy=False)
    result = pd.DataFrame(
        {
            "ts_event": records["ts_event"],
            "side": np.char.decode(records["side"], "ascii"),
            "price": records["price"],
            "size": records["size"],
            "symbol": symbol_values,
        }
    )
    result.attrs["outright_only"] = True
    return result


def _outright_instrument_symbols(
    dbn_store: Any,
    *,
    provider_symbol: str,
) -> dict[int, str]:
    base_symbol = provider_symbol.upper().removesuffix(".FUT")
    outright_pattern = re.compile(rf"^{re.escape(base_symbol)}[FGHJKMNQUVXZ]\d{{1,2}}$")
    mappings = getattr(dbn_store, "mappings", {})
    if not isinstance(mappings, dict):
        return {}
    result: dict[int, str] = {}
    for raw_symbol, intervals in mappings.items():
        normalized_symbol = str(raw_symbol).upper()
        if outright_pattern.fullmatch(normalized_symbol) is None:
            continue
        for interval in intervals if isinstance(intervals, list) else ():
            if not isinstance(interval, dict):
                continue
            instrument_id = _optional_int(interval.get("symbol"))
            if instrument_id is not None:
                result[instrument_id] = normalized_symbol
    return result


def _import_databento() -> Any:
    try:
        import databento as db
    except ImportError as exc:
        raise CmeLocalDataError(
            "The 'databento' package is required to read CME DBN files."
        ) from exc
    return db


def _iter_dataframes(frames: Any) -> Iterable[Any]:
    if hasattr(frames, "iterrows"):
        return (frames,)
    return frames


def _timestamp_to_ms(value: Any) -> int:
    if hasattr(value, "timestamp"):
        return int(value.timestamp() * 1000)
    integer = int(value)
    if abs(integer) > 10_000_000_000_000:
        return integer // 1_000_000
    return integer


def _to_decimal(value: Any) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _trade_side(raw_side: Any, price: Decimal, previous_price: Decimal | None) -> str:
    side = str(raw_side or "").strip().upper()
    if side in {"B", "BID", "BUY"}:
        return "buy"
    if side in {"A", "ASK", "SELL"}:
        return "sell"
    if previous_price is None:
        return "buy"
    return "buy" if price >= previous_price else "sell"
