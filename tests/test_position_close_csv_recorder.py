from __future__ import annotations

import csv
import io
import unittest
from decimal import Decimal
from types import SimpleNamespace

from execution.position_close_csv_recorder import PositionCloseCsvRecorder


class _MemoryStat:
    st_size = 0


class _MemoryFile:
    def __init__(self, name: str, files: dict[str, io.StringIO]) -> None:
        self.name = name
        self.parent = self
        self._files = files

    def __truediv__(self, name: str) -> "_MemoryFile":
        return _MemoryFile(name, self._files)

    def mkdir(self, **kwargs) -> None:
        del kwargs

    def exists(self) -> bool:
        stream = self._files.get(self.name)
        return stream is not None and len(stream.getvalue()) > 0

    def stat(self) -> _MemoryStat:
        stream = self._files.get(self.name)
        return type("_MemoryStat", (), {"st_size": len(stream.getvalue()) if stream else 0})()

    def open(self, mode: str, *args, **kwargs):
        del args, kwargs
        if "w" in mode:
            stream = io.StringIO()
            self._files[self.name] = stream
        else:
            stream = self._files.setdefault(self.name, io.StringIO())
            if "a" in mode:
                stream.seek(0, io.SEEK_END)
            else:
                stream.seek(0)
        return _NonClosingStream(stream)


class _NonClosingStream:
    def __init__(self, stream: io.StringIO) -> None:
        self._stream = stream

    def __enter__(self) -> io.StringIO:
        return self._stream

    def __exit__(self, *args) -> None:
        del args


class PositionCloseCsvRecorderTests(unittest.TestCase):
    def test_configure_initializes_timeframe_files(self) -> None:
        files: dict[str, io.StringIO] = {}
        output_dir = _MemoryFile("", files)
        recorder = PositionCloseCsvRecorder()

        recorder.configure(
            output_dir=output_dir,
            enabled=True,
            timeframes=("M1", "M5"),
        )

        self.assertIn("closed_positions_M1.csv", files)
        self.assertIn("closed_positions_M5.csv", files)
        self.assertEqual(_read_rows(files["closed_positions_M1.csv"]), [])

    def test_writes_one_csv_per_timeframe_and_dedupes_position(self) -> None:
        files: dict[str, io.StringIO] = {}
        output_dir = _MemoryFile("", files)
        recorder = PositionCloseCsvRecorder(
            output_dir=output_dir,
            enabled=True,
        )
        long_position = SimpleNamespace(
            position_id="POS-LONG",
            symbol="NQ.FUT",
            timeframe="M1",
            side="LONG",
            entry_candle_time_ms=1_780_958_460_000,
            reference_candle_time_ms=1_780_957_560_000,
            entry_price=Decimal("29350.000"),
        )
        long_exit = SimpleNamespace(
            trigger_candle_time_ms=1_780_960_920_000,
            exit_price=Decimal("29450.000"),
            reason="BUY_EXIT_100_USD_PROFIT_TARGET",
        )
        short_position = SimpleNamespace(
            position_id="POS-SHORT",
            symbol="NQ.FUT",
            timeframe="M5",
            side="SHORT",
            entry_candle_time_ms=1_780_963_300_000,
            reference_candle_time_ms=1_780_956_300_000,
            entry_price=Decimal("29400.500"),
        )
        short_exit = SimpleNamespace(
            trigger_candle_time_ms=1_780_963_300_000,
            exit_price=Decimal("29421.250"),
            reason="SELL_EXIT_STOP_LOSS",
        )

        recorder.record(position=long_position, exit_signal=long_exit)
        recorder.record(position=long_position, exit_signal=long_exit)
        recorder.record(position=short_position, exit_signal=short_exit)

        m1_rows = _read_rows(files["closed_positions_M1.csv"])
        m5_rows = _read_rows(files["closed_positions_M5.csv"])

        self.assertEqual(len(m1_rows), 1)
        self.assertEqual(m1_rows[0]["symbol"], "NQ.FUT")
        self.assertEqual(m1_rows[0]["timeframe"], "M1")
        self.assertEqual(m1_rows[0]["side"], "LONG")
        self.assertEqual(m1_rows[0]["price_move"], "100.000")
        self.assertEqual(m1_rows[0]["point_value"], "20")
        self.assertEqual(m1_rows[0]["profit_loss_usd"], "2000.000")
        self.assertEqual(
            m1_rows[0]["exit_reason"],
            "BUY_EXIT_100_USD_PROFIT_TARGET",
        )
        self.assertEqual(len(m5_rows), 1)
        self.assertEqual(m5_rows[0]["side"], "SHORT")
        self.assertEqual(m5_rows[0]["price_move"], "-20.750")
        self.assertEqual(m5_rows[0]["point_value"], "20")
        self.assertEqual(m5_rows[0]["profit_loss_usd"], "-415.000")

    def test_records_closed_position_from_signal_payloads(self) -> None:
        files: dict[str, io.StringIO] = {}
        recorder = PositionCloseCsvRecorder(
            output_dir=_MemoryFile("", files),
            enabled=True,
        )
        signals = [
            {
                "signal_type": "BUY_ENTRY",
                "symbol": "NQ.FUT",
                "timeframe": "M1",
                "position_id": "POS-PAYLOAD",
                "trigger_candle_time_ms": 1_780_958_400_000,
                "action_candle_time_ms": 1_780_958_460_000,
                "reference_candle_time_ms": 1_780_957_560_000,
                "entry_price": "29350.000",
            },
            {
                "signal_type": "EXIT_BUY",
                "position_id": "POS-PAYLOAD",
                "trigger_candle_time_ms": 1_780_960_920_000,
                "exit_price": "29450.000",
                "reason": "BUY_EXIT_100_USD_PROFIT_TARGET",
            },
        ]

        recorder.record_signal_payloads(signals)
        recorder.record_signal_payloads(signals)

        rows = _read_rows(files["closed_positions_M1.csv"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["symbol"], "NQ.FUT")
        self.assertEqual(rows[0]["side"], "LONG")
        self.assertEqual(rows[0]["entry_price"], "29350.000")
        self.assertEqual(rows[0]["exit_price"], "29450.000")
        self.assertEqual(rows[0]["price_move"], "100.000")
        self.assertEqual(rows[0]["point_value"], "20")
        self.assertEqual(rows[0]["profit_loss_usd"], "2000.000")

    def test_records_signal_payloads_across_batches(self) -> None:
        files: dict[str, io.StringIO] = {}
        recorder = PositionCloseCsvRecorder(
            output_dir=_MemoryFile("", files),
            enabled=True,
        )
        entry_signal = {
            "signal_type": "SELL_ENTRY",
            "symbol": "NQ.FUT",
            "timeframe": "M1",
            "position_id": "POS-CROSS-BATCH",
            "action_candle_time_ms": 1_780_958_460_000,
            "reference_candle_time_ms": 1_780_957_560_000,
            "entry_price": "29103.250",
        }
        exit_signal = {
            "signal_type": "EXIT_SELL",
            "position_id": "POS-CROSS-BATCH",
            "trigger_candle_time_ms": 1_780_960_920_000,
            "exit_price": "29003.250",
            "reason": "SELL_EXIT_100_USD_PROFIT_TARGET",
        }

        recorder.record_signal_payloads([entry_signal])
        self.assertNotIn("closed_positions_M1.csv", files)

        recorder.record_signal_payloads([exit_signal])

        rows = _read_rows(files["closed_positions_M1.csv"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["side"], "SHORT")
        self.assertEqual(rows[0]["price_move"], "100.000")
        self.assertEqual(rows[0]["profit_loss_usd"], "2000.000")

    def test_records_signal_payloads_when_exit_arrives_before_entry(self) -> None:
        files: dict[str, io.StringIO] = {}
        recorder = PositionCloseCsvRecorder(
            output_dir=_MemoryFile("", files),
            enabled=True,
        )
        entry_signal = {
            "signal_type": "BUY_ENTRY",
            "symbol": "NQ.FUT",
            "timeframe": "M1",
            "position_id": "POS-EXIT-FIRST",
            "action_candle_time_ms": 1_780_958_460_000,
            "reference_candle_time_ms": 1_780_957_560_000,
            "entry_price": "29350.000",
        }
        exit_signal = {
            "signal_type": "EXIT_BUY",
            "position_id": "POS-EXIT-FIRST",
            "trigger_candle_time_ms": 1_780_960_920_000,
            "exit_price": "29450.000",
            "reason": "BUY_EXIT_100_USD_PROFIT_TARGET",
        }

        recorder.record_signal_payloads([exit_signal])
        self.assertNotIn("closed_positions_M1.csv", files)

        recorder.record_signal_payloads([entry_signal])

        rows = _read_rows(files["closed_positions_M1.csv"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["side"], "LONG")
        self.assertEqual(rows[0]["price_move"], "100.000")
        self.assertEqual(rows[0]["profit_loss_usd"], "2000.000")

    def test_migrates_old_profit_loss_header(self) -> None:
        files: dict[str, io.StringIO] = {}
        output_dir = _MemoryFile("", files)
        files["closed_positions_M1.csv"] = io.StringIO(
            "symbol,timeframe,time_vancouver,entry_time_vancouver,"
            "exit_time_vancouver,reference_candle_time_vancouver,side,"
            "entry_price,exit_price,profit_loss,exit_reason,position_id\n"
            "NQ.FUT,M1,2026-06-08 16:12:00,2026-06-08 15:31:00,"
            "2026-06-08 16:12:00,2026-06-08 15:16:00,LONG,"
            "29350.000,29450.000,100.000,BUY_EXIT_100_USD_PROFIT_TARGET,"
            "POS-F2C083B21DDE964EBF1D\n"
        )
        recorder = PositionCloseCsvRecorder()

        recorder.configure(
            output_dir=output_dir,
            enabled=True,
            timeframes=("M1",),
        )

        rows = _read_rows(files["closed_positions_M1.csv"])
        self.assertEqual(len(rows), 1)
        self.assertNotIn("profit_loss", rows[0])
        self.assertEqual(rows[0]["price_move"], "100.000")
        self.assertEqual(rows[0]["point_value"], "20")
        self.assertEqual(rows[0]["profit_loss_usd"], "2000.000")


def _read_rows(stream: io.StringIO) -> list[dict[str, str]]:
    stream.seek(0)
    return list(csv.DictReader(stream))


if __name__ == "__main__":
    unittest.main()
