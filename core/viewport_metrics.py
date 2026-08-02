from __future__ import annotations

import csv
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)

CSV_FIELDS = (
    "timestamp_utc",
    "event",
    "view",
    "symbol",
    "timeframe",
    "request_id",
    "window_start_ms",
    "window_end_ms",
    "viewport_fetch_duration_ms",
    "cache_hit_count",
    "cache_miss_count",
    "candle_rebuild_count",
    "footprint_rebuild_count",
    "cumulative_delta_cache_hit_count",
    "cumulative_delta_cache_miss_count",
    "cancelled_requests",
    "ignored_obsolete_requests",
)


class ViewportMetricsRecorder:
    def __init__(self, output_path: Path | None = None) -> None:
        self.output_path = (
            output_path
            or Path.cwd() / "runtime_metrics" / "viewport_performance_metrics.csv"
        )
        self._lock = threading.Lock()

    def configure(self, *, output_path: Path) -> None:
        self.output_path = output_path

    def record(
        self,
        *,
        event: str,
        view: str,
        symbol: str = "",
        timeframe: str = "",
        request_id: int | str | None = None,
        window_start_ms: int | None = None,
        window_end_ms: int | None = None,
        viewport_fetch_duration_ms: float | None = None,
        cache_hit_count: int = 0,
        cache_miss_count: int = 0,
        candle_rebuild_count: int = 0,
        footprint_rebuild_count: int = 0,
        cumulative_delta_cache_hit_count: int = 0,
        cumulative_delta_cache_miss_count: int = 0,
        cancelled_requests: int = 0,
        ignored_obsolete_requests: int = 0,
    ) -> None:
        row: dict[str, Any] = {
            "timestamp_utc": datetime.fromtimestamp(
                time.time(),
                tz=timezone.utc,
            ).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "event": event,
            "view": view,
            "symbol": symbol.strip().upper(),
            "timeframe": timeframe.strip().upper(),
            "request_id": "" if request_id is None else request_id,
            "window_start_ms": "" if window_start_ms is None else int(window_start_ms),
            "window_end_ms": "" if window_end_ms is None else int(window_end_ms),
            "viewport_fetch_duration_ms": (
                ""
                if viewport_fetch_duration_ms is None
                else round(float(viewport_fetch_duration_ms), 3)
            ),
            "cache_hit_count": int(cache_hit_count),
            "cache_miss_count": int(cache_miss_count),
            "candle_rebuild_count": int(candle_rebuild_count),
            "footprint_rebuild_count": int(footprint_rebuild_count),
            "cumulative_delta_cache_hit_count": int(
                cumulative_delta_cache_hit_count
            ),
            "cumulative_delta_cache_miss_count": int(
                cumulative_delta_cache_miss_count
            ),
            "cancelled_requests": int(cancelled_requests),
            "ignored_obsolete_requests": int(ignored_obsolete_requests),
        }
        try:
            with self._lock:
                self.output_path.parent.mkdir(parents=True, exist_ok=True)
                write_header = (
                    not self.output_path.exists()
                    or self.output_path.stat().st_size <= 0
                )
                with self.output_path.open(
                    "a",
                    newline="",
                    encoding="utf-8",
                ) as handle:
                    writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                    if write_header:
                        writer.writeheader()
                    writer.writerow(row)
        except OSError:
            LOGGER.exception("VIEWPORT_METRIC_WRITE_FAILED")
            return

        LOGGER.debug(
            "VIEWPORT_METRIC | event=%s | view=%s | symbol=%s | timeframe=%s "
            "| duration_ms=%s | cache_hit=%d | cache_miss=%d "
            "| candle_rebuild=%d | footprint_rebuild=%d | cvd_hit=%d "
            "| cvd_miss=%d | cancelled=%d "
            "| ignored=%d",
            event,
            view,
            row["symbol"],
            row["timeframe"],
            row["viewport_fetch_duration_ms"],
            row["cache_hit_count"],
            row["cache_miss_count"],
            row["candle_rebuild_count"],
            row["footprint_rebuild_count"],
            row["cumulative_delta_cache_hit_count"],
            row["cumulative_delta_cache_miss_count"],
            row["cancelled_requests"],
            row["ignored_obsolete_requests"],
        )


def get_viewport_metrics_recorder() -> ViewportMetricsRecorder:
    return _RECORDER


_RECORDER = ViewportMetricsRecorder()


__all__ = ["ViewportMetricsRecorder", "get_viewport_metrics_recorder"]
