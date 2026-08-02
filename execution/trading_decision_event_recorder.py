from __future__ import annotations

import csv
import json
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path


CSV_FIELDS = (
    "timestamp_utc_ms",
    "event_type",
    "symbol",
    "trading_timeframe",
    "command_type",
<<<<<<< HEAD
    "action",
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    "entry_side",
    "trigger_source_timeframe",
    "trigger_side",
    "trading_timeframe_stop_loss",
    "lower_timeframe_suggested_stop_losses",
    "stop_reference_price",
    "selected_stop_loss_source",
    "request_id",
    "position_id",
    "client_position_id",
    "client_position_identifier",
    "signal_time_utc_ms",
<<<<<<< HEAD
    "target_entry_open_time_utc_ms",
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    "trigger_bin_price",
    "entry_reason",
    "exit_reason",
    "reason",
)


@dataclass(frozen=True)
class TradingDecisionEvent:
    event_type: str
    symbol: str
    trading_timeframe: str
    command_type: str
    entry_side: str
<<<<<<< HEAD
    action: str = ""
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    trigger_source_timeframe: str = ""
    trigger_side: str = ""
    trading_timeframe_stop_loss: Decimal | None = None
    lower_timeframe_suggested_stop_losses: dict[str, str] | None = None
    stop_reference_price: Decimal | None = None
    selected_stop_loss_source: str = ""
    request_id: str = ""
    position_id: str = ""
    client_position_id: str = ""
    client_position_identifier: str = ""
    signal_time_utc_ms: int = 0
<<<<<<< HEAD
    target_entry_open_time_utc_ms: int = 0
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    trigger_bin_price: Decimal | None = None
    entry_reason: str = ""
    exit_reason: str = ""
    reason: str = ""

    def dedupe_key(self) -> tuple[object, ...]:
        return (
            self.event_type,
            self.symbol,
            self.trading_timeframe,
            self.command_type,
<<<<<<< HEAD
            self.action,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            self.entry_side,
            self.trigger_source_timeframe,
            self.trigger_side,
            self.trading_timeframe_stop_loss,
            tuple(sorted((self.lower_timeframe_suggested_stop_losses or {}).items())),
            self.stop_reference_price,
            self.selected_stop_loss_source,
            self.request_id,
            self.position_id,
            self.client_position_id,
            self.client_position_identifier,
            self.signal_time_utc_ms,
<<<<<<< HEAD
            self.target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            self.trigger_bin_price,
            self.entry_reason,
            self.exit_reason,
            self.reason,
        )

    def to_csv_row(self) -> dict[str, object]:
        return {
            "timestamp_utc_ms": int(time.time() * 1000),
            "event_type": self.event_type,
            "symbol": self.symbol,
            "trading_timeframe": self.trading_timeframe,
            "command_type": self.command_type,
<<<<<<< HEAD
            "action": self.action,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "entry_side": self.entry_side,
            "trigger_source_timeframe": self.trigger_source_timeframe,
            "trigger_side": self.trigger_side,
            "trading_timeframe_stop_loss": _decimal_to_csv(self.trading_timeframe_stop_loss),
            "lower_timeframe_suggested_stop_losses": json.dumps(
                self.lower_timeframe_suggested_stop_losses or {},
                sort_keys=True,
                separators=(",", ":"),
            ),
            "stop_reference_price": _decimal_to_csv(self.stop_reference_price),
            "selected_stop_loss_source": self.selected_stop_loss_source,
            "request_id": self.request_id,
            "position_id": self.position_id,
            "client_position_id": self.client_position_id,
            "client_position_identifier": self.client_position_identifier,
            "signal_time_utc_ms": self.signal_time_utc_ms,
<<<<<<< HEAD
            "target_entry_open_time_utc_ms": self.target_entry_open_time_utc_ms,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "trigger_bin_price": _decimal_to_csv(self.trigger_bin_price),
            "entry_reason": self.entry_reason,
            "exit_reason": self.exit_reason,
            "reason": self.reason,
        }


class TradingDecisionEventRecorder:
    def __init__(self) -> None:
        self.output_path = Path.cwd() / "runtime_metrics" / "trading_decision_events.csv"
        self._seen_event_keys: set[tuple[object, ...]] = set()

    def configure(self, *, output_path: Path) -> None:
        self.output_path = output_path

    def record(self, event: TradingDecisionEvent) -> None:
        event_key = event.dedupe_key()
        if event_key in self._seen_event_keys:
            return
        self._seen_event_keys.add(event_key)
        _append_event_to_csv(self.output_path, event)


_RECORDER = TradingDecisionEventRecorder()


def get_trading_decision_event_recorder() -> TradingDecisionEventRecorder:
    return _RECORDER


def _append_event_to_csv(output_path: Path, event: TradingDecisionEvent) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not output_path.exists() or output_path.stat().st_size <= 0
    existing_rows: list[dict[str, str]] = []
    rewrite_file = False
    if not write_header:
        with output_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != CSV_FIELDS:
                existing_rows = [
                    {field: str(existing_row.get(field, "") or "") for field in CSV_FIELDS}
                    for existing_row in reader
                ]
                rewrite_file = True
    with output_path.open("w" if rewrite_file else "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if write_header or rewrite_file:
            writer.writeheader()
        if rewrite_file:
            writer.writerows(existing_rows)
        writer.writerow(event.to_csv_row())


def _decimal_to_csv(value: Decimal | None) -> str:
    return "" if value is None else str(value)
