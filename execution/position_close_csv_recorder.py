from __future__ import annotations

import csv
import logging
import threading
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable, Mapping
from zoneinfo import ZoneInfo


LOGGER = logging.getLogger(__name__)
VANCOUVER_TZ = ZoneInfo("America/Vancouver")

CSV_FIELDS = (
    "symbol",
    "timeframe",
    "time_vancouver",
    "entry_time_vancouver",
    "exit_time_vancouver",
    "reference_candle_time_vancouver",
    "side",
    "entry_price",
    "exit_price",
    "price_move",
    "point_value",
    "profit_loss_usd",
    "exit_reason",
    "position_id",
)


class PositionCloseCsvRecorder:
    def __init__(
        self,
        *,
        output_dir: Path | None = None,
        enabled: bool = False,
        point_value_by_symbol: Mapping[str, Any] | None = None,
    ) -> None:
        self.output_dir = output_dir or Path.cwd() / "runtime_metrics"
        self.enabled = bool(enabled)
        self.point_value_by_symbol = _normalized_point_values(point_value_by_symbol)
        self._lock = threading.Lock()
        self._seen_keys: set[tuple[str, str]] = set()
        self._pending_entries: dict[str, Mapping[str, Any]] = {}
        self._pending_exits: dict[str, Mapping[str, Any]] = {}

    def configure(
        self,
        *,
        output_dir: Path,
        enabled: bool = True,
        timeframes: tuple[str, ...] = (),
        point_value_by_symbol: Mapping[str, Any] | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.enabled = bool(enabled)
        if point_value_by_symbol is not None:
            self.point_value_by_symbol = _normalized_point_values(point_value_by_symbol)
        self._seen_keys.clear()
        self._pending_entries.clear()
        self._pending_exits.clear()
        if self.enabled:
            for timeframe in timeframes:
                normalized = str(timeframe or "").strip().upper()
                if normalized:
                    self.ensure_timeframe_file(normalized)

    def ensure_timeframe_file(self, timeframe: str) -> None:
        if not self.enabled:
            return
        normalized = str(timeframe or "").strip().upper()
        if not normalized:
            return
        output_path = self.output_dir / f"closed_positions_{_safe_filename_part(normalized)}.csv"
        try:
            _ensure_file(output_path)
        except OSError:
            LOGGER.exception(
                "POSITION_CLOSE_CSV_INIT_FAILED | timeframe=%s",
                normalized,
            )

    def reset_timeframe_file(self, timeframe: str) -> None:
        normalized = str(timeframe or "").strip().upper()
        if not self.enabled or not normalized:
            return
        output_path = self.output_dir / f"closed_positions_{_safe_filename_part(normalized)}.csv"
        try:
            _write_header(output_path)
        except OSError:
            LOGGER.exception(
                "POSITION_CLOSE_CSV_RESET_FAILED | timeframe=%s",
                normalized,
            )

    def record(self, *, position: Any, exit_signal: Any) -> None:
        if not self.enabled:
            return
        with self._lock:
            try:
                _record_row(
                    output_dir=self.output_dir,
                    seen_keys=self._seen_keys,
                    row=_row_for_position(
                        position,
                        exit_signal,
                        point_value_by_symbol=self.point_value_by_symbol,
                    ),
                )
            except OSError:
                LOGGER.exception(
                    "POSITION_CLOSE_CSV_WRITE_FAILED | source=position_object",
                )

    def record_payload_pair(
        self,
        *,
        entry_signal: Mapping[str, Any],
        exit_signal: Mapping[str, Any],
    ) -> None:
        if not self.enabled:
            return
        with self._lock:
            try:
                _record_row(
                    output_dir=self.output_dir,
                    seen_keys=self._seen_keys,
                    row=_row_for_payload_pair(
                        entry_signal=entry_signal,
                        exit_signal=exit_signal,
                        point_value_by_symbol=self.point_value_by_symbol,
                    ),
                )
            except OSError:
                LOGGER.exception(
                    "POSITION_CLOSE_CSV_WRITE_FAILED | source=signal_payload",
                )

    def record_signal_payloads(self, signals: Iterable[Mapping[str, Any]]) -> None:
        if not self.enabled:
            return
        with self._lock:
            for signal in signals:
                signal_type = str(signal.get("signal_type") or "").strip().upper()
                position_id = str(signal.get("position_id") or "").strip()
                if not position_id:
                    continue
                if signal_type in {"BUY_ENTRY", "SELL_ENTRY"}:
                    exit_signal = self._pending_exits.pop(position_id, None)
                    if exit_signal is None:
                        self._pending_entries[position_id] = signal
                        continue
                    self._record_payload_pair_unlocked(
                        entry_signal=signal,
                        exit_signal=exit_signal,
                    )
                    self._pending_entries.pop(position_id, None)
                    continue
                if signal_type not in {"EXIT_BUY", "EXIT_SELL"}:
                    continue
                entry_signal = self._pending_entries.pop(position_id, None)
                if entry_signal is None:
                    self._pending_exits[position_id] = signal
                    continue
                self._record_payload_pair_unlocked(
                    entry_signal=entry_signal,
                    exit_signal=signal,
                )
                self._pending_exits.pop(position_id, None)

    def _record_payload_pair_unlocked(
        self,
        *,
        entry_signal: Mapping[str, Any],
        exit_signal: Mapping[str, Any],
    ) -> None:
        try:
            _record_row(
                output_dir=self.output_dir,
                seen_keys=self._seen_keys,
                row=_row_for_payload_pair(
                    entry_signal=entry_signal,
                    exit_signal=exit_signal,
                    point_value_by_symbol=self.point_value_by_symbol,
                ),
            )
        except OSError:
            LOGGER.exception(
                "POSITION_CLOSE_CSV_WRITE_FAILED | source=signal_payload",
            )


def get_position_close_csv_recorder() -> PositionCloseCsvRecorder:
    return _RECORDER


def _row_for_position(
    position: Any,
    exit_signal: Any,
    *,
    point_value_by_symbol: Mapping[str, Decimal],
) -> dict[str, str]:
    entry_price = _decimal_value(getattr(position, "entry_price", None))
    exit_price = _decimal_value(getattr(exit_signal, "exit_price", None))
    side = str(getattr(position, "side", "") or "").strip().upper()
    symbol = str(getattr(position, "symbol", "") or "").strip().upper()
    price_move, point_value, profit_loss_usd = _profit_fields(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        exit_price=exit_price,
        point_value_by_symbol=point_value_by_symbol,
    )

    exit_time_ms = int(getattr(exit_signal, "trigger_candle_time_ms", 0) or 0)
    return {
        "symbol": symbol,
        "timeframe": str(getattr(position, "timeframe", "") or "").strip().upper(),
        "time_vancouver": _vancouver_time(exit_time_ms),
        "entry_time_vancouver": _vancouver_time(
            int(getattr(position, "entry_candle_time_ms", 0) or 0)
        ),
        "exit_time_vancouver": _vancouver_time(exit_time_ms),
        "reference_candle_time_vancouver": _vancouver_time(
            int(getattr(position, "reference_candle_time_ms", 0) or 0)
        ),
        "side": side,
        "entry_price": "" if entry_price is None else str(entry_price),
        "exit_price": "" if exit_price is None else str(exit_price),
        "price_move": "" if price_move is None else str(price_move),
        "point_value": str(point_value),
        "profit_loss_usd": "" if profit_loss_usd is None else str(profit_loss_usd),
        "exit_reason": str(getattr(exit_signal, "reason", "") or ""),
        "position_id": str(getattr(position, "position_id", "") or ""),
    }


def _row_for_payload_pair(
    *,
    entry_signal: Mapping[str, Any],
    exit_signal: Mapping[str, Any],
    point_value_by_symbol: Mapping[str, Decimal],
) -> dict[str, str]:
    entry_price = _decimal_value(entry_signal.get("entry_price"))
    exit_price = _decimal_value(exit_signal.get("exit_price"))
    entry_type = str(entry_signal.get("signal_type") or "").strip().upper()
    side = "LONG" if entry_type == "BUY_ENTRY" else "SHORT"
    symbol = str(entry_signal.get("symbol") or "").strip().upper()
    price_move, point_value, profit_loss_usd = _profit_fields(
        symbol=symbol,
        side=side,
        entry_price=entry_price,
        exit_price=exit_price,
        point_value_by_symbol=point_value_by_symbol,
    )

    exit_time_ms = _integer_value(exit_signal.get("trigger_candle_time_ms"))
    return {
        "symbol": symbol,
        "timeframe": str(entry_signal.get("timeframe") or "").strip().upper(),
        "time_vancouver": _vancouver_time(exit_time_ms),
        "entry_time_vancouver": _vancouver_time(
            _integer_value(entry_signal.get("action_candle_time_ms"))
        ),
        "exit_time_vancouver": _vancouver_time(exit_time_ms),
        "reference_candle_time_vancouver": _vancouver_time(
            _integer_value(entry_signal.get("reference_candle_time_ms"))
        ),
        "side": side,
        "entry_price": "" if entry_price is None else str(entry_price),
        "exit_price": "" if exit_price is None else str(exit_price),
        "price_move": "" if price_move is None else str(price_move),
        "point_value": str(point_value),
        "profit_loss_usd": "" if profit_loss_usd is None else str(profit_loss_usd),
        "exit_reason": str(exit_signal.get("reason") or ""),
        "position_id": str(entry_signal.get("position_id") or ""),
    }


def _record_row(
    *,
    output_dir: Path,
    seen_keys: set[tuple[str, str]],
    row: dict[str, str],
) -> None:
    timeframe = str(row.get("timeframe", "") or "").strip().upper()
    position_id = str(row.get("position_id", "") or "").strip()
    if not timeframe or not position_id:
        return
    key = (timeframe, position_id)
    if key in seen_keys:
        return
    output_path = output_dir / f"closed_positions_{_safe_filename_part(timeframe)}.csv"
    if _position_id_exists(output_path, position_id):
        seen_keys.add(key)
        return
    _append_row(output_path, row)
    seen_keys.add(key)


def _append_row(output_path: Path, row: dict[str, str]) -> None:
    _ensure_file(output_path)
    with output_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writerow(row)


def _write_header(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()


def _ensure_file(output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not output_path.exists() or output_path.stat().st_size <= 0:
        _write_header(output_path)
        return

    with output_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) == CSV_FIELDS:
            return
        rows = [_normalize_existing_row(row) for row in reader]
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _position_id_exists(output_path: Path, position_id: str) -> bool:
    if not output_path.exists() or output_path.stat().st_size <= 0:
        return False
    with output_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return any(str(row.get("position_id", "")).strip() == position_id for row in reader)


def _vancouver_time(timestamp_ms: int) -> str:
    if timestamp_ms <= 0:
        return ""
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=VANCOUVER_TZ).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _decimal_value(value: Any) -> Decimal | None:
    try:
        return Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None


def _profit_fields(
    *,
    symbol: str,
    side: str,
    entry_price: Decimal | None,
    exit_price: Decimal | None,
    point_value_by_symbol: Mapping[str, Decimal],
) -> tuple[Decimal | None, Decimal, Decimal | None]:
    point_value = _point_value_for_symbol(symbol, point_value_by_symbol)
    if entry_price is None or exit_price is None:
        return None, point_value, None
    price_move = (
        exit_price - entry_price
        if side == "LONG"
        else entry_price - exit_price
    )
    return price_move, point_value, price_move * point_value


def _point_value_for_symbol(
    symbol: str,
    point_value_by_symbol: Mapping[str, Decimal],
) -> Decimal:
    normalized = str(symbol or "").strip().upper()
    candidates = (normalized, normalized.split(".", 1)[0])
    for candidate in candidates:
        point_value = point_value_by_symbol.get(candidate)
        if point_value is not None:
            return point_value
    return Decimal("1")


def _normalized_point_values(
    values: Mapping[str, Any] | None,
) -> dict[str, Decimal]:
    defaults: dict[str, Any] = {"NQ": "20", "NQ.FUT": "20"}
    source = values or defaults
    result: dict[str, Decimal] = {}
    for symbol, value in source.items():
        normalized_symbol = str(symbol or "").strip().upper()
        point_value = _decimal_value(value)
        if normalized_symbol and point_value is not None and point_value > 0:
            result[normalized_symbol] = point_value
    return result or {"NQ": Decimal("20"), "NQ.FUT": Decimal("20")}


def _normalize_existing_row(row: Mapping[str, Any]) -> dict[str, str]:
    normalized = {field: str(row.get(field, "") or "") for field in CSV_FIELDS}
    if normalized["price_move"]:
        return normalized
    entry_price = _decimal_value(normalized["entry_price"])
    exit_price = _decimal_value(normalized["exit_price"])
    side = normalized["side"].strip().upper()
    price_move, point_value, profit_loss_usd = _profit_fields(
        symbol=normalized["symbol"],
        side=side,
        entry_price=entry_price,
        exit_price=exit_price,
        point_value_by_symbol=_normalized_point_values(None),
    )
    normalized["price_move"] = "" if price_move is None else str(price_move)
    normalized["point_value"] = str(point_value)
    normalized["profit_loss_usd"] = (
        "" if profit_loss_usd is None else str(profit_loss_usd)
    )
    return normalized


def _integer_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _safe_filename_part(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value)


_RECORDER = PositionCloseCsvRecorder()


__all__ = [
    "PositionCloseCsvRecorder",
    "get_position_close_csv_recorder",
]
