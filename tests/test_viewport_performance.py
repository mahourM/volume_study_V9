from __future__ import annotations

import csv
import io
import unittest
from unittest.mock import patch

from absorption.session_service import (
    AbsorptionFootprintService,
    AbsorptionSessionSpec,
)
from config.config_runtime import RuntimeConfig
from core.viewport_metrics import ViewportMetricsRecorder


class _NonClosingStringIO(io.StringIO):
    def close(self) -> None:
        pass


class _MemoryOutputPath:
    def __init__(self) -> None:
        self.parent = self
        self.stream = _NonClosingStringIO()

    def mkdir(self, **kwargs) -> None:
        del kwargs

    def exists(self) -> bool:
        return False

    def stat(self):
        return type("_Stat", (), {"st_size": 0})()

    def open(self, *args, **kwargs):
        del args, kwargs
        return self.stream


class ViewportPerformanceTests(unittest.TestCase):
    def test_viewport_cache_key_uses_candle_bucket_not_exact_timestamp(self) -> None:
        service = object.__new__(AbsorptionFootprintService)
        service.runtime_config = RuntimeConfig()
        service._session_specs = {
            ("NQ", "M1"): AbsorptionSessionSpec(
                mt5_symbol="NQ",
                binance_symbol="",
                timeframe="M1",
                interval="1m",
                provider_symbol="NQ.FUT",
            )
        }
        end_time_ms = 1_780_963_200_000

        first = service._viewport_snapshot_cache_key(
            view="candle",
            timeframe="M1",
            end_time_ms=end_time_ms,
            candle_limit=80,
            variant="profiles=0",
        )
        same_bucket = service._viewport_snapshot_cache_key(
            view="candle",
            timeframe="M1",
            end_time_ms=end_time_ms + 30_000,
            candle_limit=80,
            variant="profiles=0",
        )

        self.assertEqual(first, same_bucket)
        self.assertIn("candle:NQ.FUT:M1:", first)
        self.assertNotIn(str(end_time_ms), first)

    def test_viewport_metrics_recorder_writes_required_fields(self) -> None:
        output_path = _MemoryOutputPath()
        recorder = ViewportMetricsRecorder(output_path)

        with (
            patch("core.viewport_metrics.LOGGER.info") as info_log,
            patch("core.viewport_metrics.LOGGER.debug") as debug_log,
        ):
            recorder.record(
                event="viewport_fetch",
                view="footprint",
                symbol="nq.fut",
                timeframe="m1",
                request_id=7,
                viewport_fetch_duration_ms=12.3456,
                cache_hit_count=9,
                cache_miss_count=1,
                candle_rebuild_count=0,
                footprint_rebuild_count=1,
                cumulative_delta_cache_hit_count=1,
                cumulative_delta_cache_miss_count=0,
                cancelled_requests=2,
                ignored_obsolete_requests=3,
            )
            info_log.assert_not_called()
            debug_log.assert_called_once()

        output_path.stream.seek(0)
        row = next(csv.DictReader(output_path.stream))

        self.assertEqual(row["symbol"], "NQ.FUT")
        self.assertEqual(row["timeframe"], "M1")
        self.assertEqual(row["viewport_fetch_duration_ms"], "12.346")
        self.assertEqual(row["cache_hit_count"], "9")
        self.assertEqual(row["footprint_rebuild_count"], "1")
        self.assertEqual(row["cumulative_delta_cache_hit_count"], "1")
        self.assertEqual(row["cancelled_requests"], "2")
        self.assertEqual(row["ignored_obsolete_requests"], "3")


if __name__ == "__main__":
    unittest.main()
