from __future__ import annotations

import asyncio
import csv
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

try:
    import psutil
except ImportError:  # pragma: no cover - optional runtime dependency
    psutil = None


CSV_FIELDS = (
    "timestamp_utc",
    "symbol",
    "timeframe",
    "event_loop_lag_ms",
    "on_aggtrade_duration_ms",
    "on_kline_closed_duration_ms",
    "process_closed_record_duration_ms",
    "raw_trade_retained_event_count",
    "raw_oldest_retained_age_ms",
    "raw_retention_blocking_timeframe",
    "kline_queue_size",
    "cpu_percent",
    "memory_mb",
    "latest_kline_closed_time_utc",
    "latest_closed_record_processed_time_utc",
    "latest_kline_closed_age_ms",
    "latest_closed_record_processed_age_ms",
    "stale_threshold_seconds",
    "stale_m1",
    "stale_m5",
    "stale_m15",
    "stale_m30",
)

TIMEFRAME_SECONDS_BY_TIMEFRAME = {
    "M1": 60,
    "M5": 300,
    "M15": 900,
    "M30": 1_800,
}

STALE_ALLOWED_DELAY_SECONDS_BY_TIMEFRAME = {
    "M1": 30,
    "M5": 60,
    "M15": 90,
    "M30": 120,
}

MetricKey = tuple[str, str]
ActiveKeysProvider = Callable[[], Iterable[MetricKey]]
KlineQueueSizeProvider = Callable[[str, str], int | None]
RawTradeRetainedEventCountProvider = Callable[[str, str], int | None]
RawOldestRetainedAgeMsProvider = Callable[[str, str], int | None]
RawRetentionBlockingTimeframeProvider = Callable[[str, str], str | None]


@dataclass
class _MetricState:
    on_aggtrade_duration_ms: float | None = None
    on_kline_closed_duration_ms: float | None = None
    process_closed_record_duration_ms: float | None = None
    latest_kline_closed_time_utc_ms: int | None = None
    latest_closed_record_processed_time_utc_ms: int | None = None

    def reset_durations(self) -> None:
        self.on_aggtrade_duration_ms = None
        self.on_kline_closed_duration_ms = None
        self.process_closed_record_duration_ms = None


@dataclass(frozen=True)
class PerformanceMetricRow:
    timestamp_utc_ms: int
    symbol: str
    timeframe: str
    event_loop_lag_ms: float
    on_aggtrade_duration_ms: float | None
    on_kline_closed_duration_ms: float | None
    process_closed_record_duration_ms: float | None
    raw_trade_retained_event_count: int | None
    raw_oldest_retained_age_ms: int | None
    raw_retention_blocking_timeframe: str | None
    kline_queue_size: int | None
    cpu_percent: float | None
    memory_mb: float | None
    latest_kline_closed_time_utc_ms: int | None
    latest_closed_record_processed_time_utc_ms: int | None
    stale_flags: dict[str, bool | None] = field(default_factory=dict)

    def to_csv_row(self) -> dict[str, object]:
        return {
            "timestamp_utc": _format_utc_ms(self.timestamp_utc_ms),
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "event_loop_lag_ms": _round_float(self.event_loop_lag_ms),
            "on_aggtrade_duration_ms": _round_float(self.on_aggtrade_duration_ms),
            "on_kline_closed_duration_ms": _round_float(self.on_kline_closed_duration_ms),
            "process_closed_record_duration_ms": _round_float(self.process_closed_record_duration_ms),
            "raw_trade_retained_event_count": _blank_if_none(self.raw_trade_retained_event_count),
            "raw_oldest_retained_age_ms": _blank_if_none(self.raw_oldest_retained_age_ms),
            "raw_retention_blocking_timeframe": self.raw_retention_blocking_timeframe or "",
            "kline_queue_size": _blank_if_none(self.kline_queue_size),
            "cpu_percent": _round_float(self.cpu_percent),
            "memory_mb": _round_float(self.memory_mb),
            "latest_kline_closed_time_utc": _format_utc_ms(self.latest_kline_closed_time_utc_ms),
            "latest_closed_record_processed_time_utc": _format_utc_ms(
                self.latest_closed_record_processed_time_utc_ms
            ),
            "latest_kline_closed_age_ms": _age_ms(
                self.timestamp_utc_ms,
                self.latest_kline_closed_time_utc_ms,
            ),
            "latest_closed_record_processed_age_ms": _age_ms(
                self.timestamp_utc_ms,
                self.latest_closed_record_processed_time_utc_ms,
            ),
            "stale_threshold_seconds": _blank_if_none(_stale_threshold_seconds(self.timeframe)),
            "stale_m1": _bool_to_csv(self.stale_flags.get("M1")),
            "stale_m5": _bool_to_csv(self.stale_flags.get("M5")),
            "stale_m15": _bool_to_csv(self.stale_flags.get("M15")),
            "stale_m30": _bool_to_csv(self.stale_flags.get("M30")),
        }


class PerformanceMetricsRecorder:
    def __init__(
        self,
        *,
        output_path: Path | None = None,
        snapshot_interval_seconds: float = 5.0,
        lag_sample_interval_seconds: float = 1.0,
    ) -> None:
        self.output_path = output_path or Path.cwd() / "runtime_metrics" / "ws_performance_metrics.csv"
        self.snapshot_interval_seconds = float(snapshot_interval_seconds)
        self.lag_sample_interval_seconds = float(lag_sample_interval_seconds)
        self._states: dict[MetricKey, _MetricState] = {}
        self._latest_event_loop_lag_ms = 0.0
        self._max_event_loop_lag_ms = 0.0
        self._task: asyncio.Task | None = None
        self._active_keys_provider: ActiveKeysProvider | None = None
        self._raw_trade_retained_event_count_provider: RawTradeRetainedEventCountProvider | None = None
        self._raw_oldest_retained_age_ms_provider: RawOldestRetainedAgeMsProvider | None = None
        self._raw_retention_blocking_timeframe_provider: RawRetentionBlockingTimeframeProvider | None = None
        self._kline_queue_size_provider: KlineQueueSizeProvider | None = None
        self._process = psutil.Process() if psutil is not None else None

    def configure(self, *, output_path: Path) -> None:
        self.output_path = output_path

    def set_snapshot_providers(
        self,
        *,
        active_keys_provider: ActiveKeysProvider | None = None,
        raw_trade_retained_event_count_provider: RawTradeRetainedEventCountProvider | None = None,
        raw_oldest_retained_age_ms_provider: RawOldestRetainedAgeMsProvider | None = None,
        raw_retention_blocking_timeframe_provider: RawRetentionBlockingTimeframeProvider | None = None,
        kline_queue_size_provider: KlineQueueSizeProvider | None = None,
    ) -> None:
        self._active_keys_provider = active_keys_provider
        self._raw_trade_retained_event_count_provider = raw_trade_retained_event_count_provider
        self._raw_oldest_retained_age_ms_provider = raw_oldest_retained_age_ms_provider
        self._raw_retention_blocking_timeframe_provider = raw_retention_blocking_timeframe_provider
        self._kline_queue_size_provider = kline_queue_size_provider

    def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        loop = asyncio.get_running_loop()
        self._task = loop.create_task(self._run_forever(), name="performance-metrics-snapshot-writer")

    def should_record(self, duration_ms: float) -> bool:
        del duration_ms
        return False

    def should_record_event(
        self,
        *,
        symbol: str,
        timeframe: str = "",
        component: str,
        duration_ms: float,
    ) -> bool:
        del symbol, timeframe, component, duration_ms
        return False

    def record(
        self,
        *,
        symbol: str,
        timeframe: str = "",
        component: str,
        duration_ms: float,
        active_level_count: int | None = None,
        pending_trade_events: int | None = None,
        closed_record_level_count: int | None = None,
        sample_checked: bool = False,
    ) -> None:
        del active_level_count, pending_trade_events, closed_record_level_count, sample_checked
        normalized_component = component.strip()
        if normalized_component == "on_aggtrade_event":
            self.record_aggtrade_duration(symbol=symbol, timeframe=timeframe, duration_ms=duration_ms)
        elif normalized_component == "on_kline_closed_event":
            self.record_kline_closed_duration(symbol=symbol, timeframe=timeframe, duration_ms=duration_ms)
        elif normalized_component == "_process_closed_record":
            self.record_closed_record_duration(symbol=symbol, timeframe=timeframe, duration_ms=duration_ms)

    def record_aggtrade_event(self, *, symbol: str, timeframe: str, duration_ms: float) -> None:
        self.record_aggtrade_duration(symbol=symbol, timeframe=timeframe, duration_ms=duration_ms)

    def record_aggtrade_duration(self, *, symbol: str, timeframe: str, duration_ms: float) -> None:
        self._set_max_duration(self._state_for(symbol, timeframe), "on_aggtrade_duration_ms", duration_ms)

    def record_kline_closed_event(
        self,
        *,
        symbol: str,
        timeframe: str,
        event_time_utc_ms: int,
        duration_ms: float,
    ) -> None:
        state = self._state_for(symbol, timeframe)
        state.latest_kline_closed_time_utc_ms = int(event_time_utc_ms)
        self._set_max_duration(state, "on_kline_closed_duration_ms", duration_ms)

    def record_kline_closed_duration(self, *, symbol: str, timeframe: str, duration_ms: float) -> None:
        self._set_max_duration(self._state_for(symbol, timeframe), "on_kline_closed_duration_ms", duration_ms)

    def record_closed_record_processed(
        self,
        *,
        symbol: str,
        timeframe: str,
        event_time_utc_ms: int,
    ) -> None:
        self._state_for(symbol, timeframe).latest_closed_record_processed_time_utc_ms = int(event_time_utc_ms)

    def record_closed_record_duration(self, *, symbol: str, timeframe: str, duration_ms: float) -> None:
        self._set_max_duration(self._state_for(symbol, timeframe), "process_closed_record_duration_ms", duration_ms)

    def _state_for(self, symbol: str, timeframe: str) -> _MetricState:
        key = (symbol.strip().upper(), timeframe.strip().upper())
        return self._states.setdefault(key, _MetricState())

    @staticmethod
    def _set_max_duration(state: _MetricState, field_name: str, duration_ms: float) -> None:
        current_value = getattr(state, field_name)
        next_value = float(duration_ms)
        if current_value is None or next_value > current_value:
            setattr(state, field_name, next_value)

    async def _run_forever(self) -> None:
        loop = asyncio.get_running_loop()
        lag_interval = max(0.1, self.lag_sample_interval_seconds)
        snapshot_interval = max(1.0, self.snapshot_interval_seconds)
        next_lag_sample_time = loop.time() + lag_interval
        next_snapshot_time = loop.time() + snapshot_interval
        while True:
            next_wake_time = min(next_lag_sample_time, next_snapshot_time)
            await asyncio.sleep(max(0.0, next_wake_time - loop.time()))
            now = loop.time()
            if now >= next_lag_sample_time:
                self._latest_event_loop_lag_ms = max(0.0, (now - next_lag_sample_time) * 1000.0)
                self._max_event_loop_lag_ms = max(self._max_event_loop_lag_ms, self._latest_event_loop_lag_ms)
                next_lag_sample_time = now + lag_interval
            if now >= next_snapshot_time:
                await self._write_snapshot()
                self._max_event_loop_lag_ms = self._latest_event_loop_lag_ms
                next_snapshot_time = now + snapshot_interval

    async def _write_snapshot(self) -> None:
        rows = self._build_snapshot_rows()
        self._reset_window_durations()
        if not rows:
            return
        try:
            await asyncio.to_thread(_append_rows_to_csv, self.output_path, rows)
        except Exception:
            return

    def _build_snapshot_rows(self) -> list[PerformanceMetricRow]:
        now_ms = int(time.time() * 1000)
        cpu_percent, memory_mb = self._process_snapshot()
        keys = self._snapshot_keys()
        return [
            PerformanceMetricRow(
                timestamp_utc_ms=now_ms,
                symbol=symbol,
                timeframe=timeframe,
                event_loop_lag_ms=self._max_event_loop_lag_ms,
                on_aggtrade_duration_ms=state.on_aggtrade_duration_ms,
                on_kline_closed_duration_ms=state.on_kline_closed_duration_ms,
                process_closed_record_duration_ms=state.process_closed_record_duration_ms,
                raw_trade_retained_event_count=self._raw_trade_retained_event_count(symbol, timeframe),
                raw_oldest_retained_age_ms=self._raw_oldest_retained_age_ms(symbol, timeframe),
                raw_retention_blocking_timeframe=self._raw_retention_blocking_timeframe(symbol, timeframe),
                kline_queue_size=self._kline_queue_size(symbol, timeframe),
                cpu_percent=cpu_percent,
                memory_mb=memory_mb,
                latest_kline_closed_time_utc_ms=state.latest_kline_closed_time_utc_ms,
                latest_closed_record_processed_time_utc_ms=state.latest_closed_record_processed_time_utc_ms,
                stale_flags=self._stale_flags_for_symbol(symbol, now_ms),
            )
            for symbol, timeframe, state in keys
        ]

    def _snapshot_keys(self) -> list[tuple[str, str, _MetricState]]:
        active_keys = set(self._states)
        if self._active_keys_provider is not None:
            try:
                active_keys.update(
                    (symbol.strip().upper(), timeframe.strip().upper())
                    for symbol, timeframe in self._active_keys_provider()
                )
            except Exception:
                pass
        return [
            (symbol, timeframe, self._states.setdefault((symbol, timeframe), _MetricState()))
            for symbol, timeframe in sorted(active_keys)
        ]

    def _raw_trade_retained_event_count(self, symbol: str, timeframe: str) -> int | None:
        if self._raw_trade_retained_event_count_provider is None:
            return None
        try:
            return self._raw_trade_retained_event_count_provider(symbol, timeframe)
        except Exception:
            return None

    def _raw_oldest_retained_age_ms(self, symbol: str, timeframe: str) -> int | None:
        if self._raw_oldest_retained_age_ms_provider is None:
            return None
        try:
            return self._raw_oldest_retained_age_ms_provider(symbol, timeframe)
        except Exception:
            return None

    def _raw_retention_blocking_timeframe(self, symbol: str, timeframe: str) -> str | None:
        if self._raw_retention_blocking_timeframe_provider is None:
            return None
        try:
            return self._raw_retention_blocking_timeframe_provider(symbol, timeframe)
        except Exception:
            return None

    def _kline_queue_size(self, symbol: str, timeframe: str) -> int | None:
        if self._kline_queue_size_provider is None:
            return None
        try:
            return self._kline_queue_size_provider(symbol, timeframe)
        except Exception:
            return None

    def _stale_flags_for_symbol(self, symbol: str, now_ms: int) -> dict[str, bool | None]:
        return {
            timeframe: self._is_timeframe_stale(symbol, timeframe, now_ms)
            for timeframe in TIMEFRAME_SECONDS_BY_TIMEFRAME
        }

    def _is_timeframe_stale(self, symbol: str, timeframe: str, now_ms: int) -> bool | None:
        state = self._states.get((symbol.strip().upper(), timeframe.strip().upper()))
        if state is None:
            return None
        timestamps = [
            value
            for value in (
                state.latest_kline_closed_time_utc_ms,
                state.latest_closed_record_processed_time_utc_ms,
            )
            if value is not None
        ]
        if not timestamps:
            return None
        threshold_seconds = _stale_threshold_seconds(timeframe)
        if threshold_seconds is None:
            return None
        threshold_ms = threshold_seconds * 1000
        return any(now_ms - int(timestamp) > threshold_ms for timestamp in timestamps)

    def _process_snapshot(self) -> tuple[float | None, float | None]:
        if self._process is None:
            return None, None
        try:
            cpu_percent = float(self._process.cpu_percent(interval=None))
            memory_mb = float(self._process.memory_info().rss) / (1024.0 * 1024.0)
            return cpu_percent, memory_mb
        except Exception:
            return None, None

    def _reset_window_durations(self) -> None:
        for state in self._states.values():
            state.reset_durations()


def perf_counter_ms() -> float:
    return time.perf_counter() * 1000.0


def elapsed_ms(start_ms: float) -> float:
    return (time.perf_counter() * 1000.0) - start_ms


def get_performance_metrics_recorder() -> PerformanceMetricsRecorder:
    return _RECORDER


def _append_rows_to_csv(output_path: Path, rows: list[PerformanceMetricRow]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not _preserve_legacy_file_if_needed(output_path):
        return
    write_header = not output_path.exists() or output_path.stat().st_size <= 0
    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if write_header:
            writer.writeheader()
        for row in rows:
            writer.writerow(row.to_csv_row())


def _preserve_legacy_file_if_needed(output_path: Path) -> bool:
    if not output_path.exists() or output_path.stat().st_size <= 0:
        return True
    try:
        with output_path.open("r", encoding="utf-8", newline="") as handle:
            first_line = handle.readline().strip()
    except OSError:
        return False
    expected_header = ",".join(CSV_FIELDS)
    if first_line == expected_header:
        return True
    suffix = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    legacy_path = output_path.with_name(f"{output_path.stem}_legacy_{suffix}{output_path.suffix}")
    try:
        output_path.replace(legacy_path)
    except OSError:
        return False
    return True


def _format_utc_ms(value_ms: int | None) -> str:
    if value_ms is None:
        return ""
    return datetime.fromtimestamp(int(value_ms) / 1000.0, tz=timezone.utc).isoformat(
        timespec="milliseconds"
    ).replace("+00:00", "Z")


def _age_ms(now_ms: int, value_ms: int | None) -> object:
    if value_ms is None:
        return ""
    return max(0, int(now_ms) - int(value_ms))


def _round_float(value: float | None) -> object:
    if value is None:
        return ""
    return round(float(value), 3)


def _blank_if_none(value: int | None) -> object:
    return "" if value is None else value


def _stale_threshold_seconds(timeframe: str) -> int | None:
    normalized_timeframe = timeframe.strip().upper()
    timeframe_seconds = TIMEFRAME_SECONDS_BY_TIMEFRAME.get(normalized_timeframe)
    allowed_delay_seconds = STALE_ALLOWED_DELAY_SECONDS_BY_TIMEFRAME.get(normalized_timeframe)
    if timeframe_seconds is None or allowed_delay_seconds is None:
        return None
    return timeframe_seconds + allowed_delay_seconds


def _bool_to_csv(value: bool | None) -> str:
    if value is None:
        return ""
    return "true" if value else "false"


_RECORDER = PerformanceMetricsRecorder()


__all__ = [
    "PerformanceMetricsRecorder",
    "elapsed_ms",
    "get_performance_metrics_recorder",
    "perf_counter_ms",
]
