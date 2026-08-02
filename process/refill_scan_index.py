from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


class RefillScanIndex:
    def __init__(self, path: Path) -> None:
        self.path = path

    @staticmethod
    def signature(parts: tuple[Any, ...]) -> str:
        serialized = json.dumps(parts, ensure_ascii=True, default=str, separators=(",", ":"))
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def load(
        self,
        *,
        signature: str,
        start_ms: int,
        end_ms: int,
    ) -> dict[str, Any] | None:
        if not self.path.exists() or int(end_ms) <= int(start_ms):
            return None
        with self._connection(readonly=True) as connection:
            blocks = connection.execute(
                """
                SELECT start_ms, end_ms, processed_event_count,
                       emitted_payload_count, footprint_candle_count,
                       footprint_symbols_json
                FROM refill_scan_blocks
                WHERE signature = ? AND status = 'READY'
                  AND end_ms > ? AND start_ms < ?
                ORDER BY start_ms, end_ms
                """,
                (signature, int(start_ms), int(end_ms)),
            ).fetchall()
            if not self._covers(blocks, start_ms=int(start_ms), end_ms=int(end_ms)):
                return None
            rows = connection.execute(
                """
                SELECT payload_json
                FROM refill_scan_rows
                WHERE signature = ?
                  AND marker_time_ms >= ? AND marker_time_ms < ?
                ORDER BY marker_time_ms, marker_price, side
                """,
                (signature, int(start_ms), int(end_ms)),
            ).fetchall()
        payloads = tuple(json.loads(str(row[0])) for row in rows)
        footprint_symbols: list[dict[str, Any]] = []
        for block in blocks:
            try:
                candidates = json.loads(str(block[5] or "[]"))
            except ValueError:
                candidates = []
            for candidate in candidates:
                if candidate not in footprint_symbols:
                    footprint_symbols.append(candidate)
        return {
            "aggregate_payloads": payloads,
            "footprints": (),
            "processed_event_count": sum(int(block[2] or 0) for block in blocks),
            "emitted_payload_count": len(payloads),
            "footprint_candle_count": sum(int(block[4] or 0) for block in blocks),
            "footprint_symbols": tuple(footprint_symbols),
            "spike_score_payloads": (),
        }

    def store(
        self,
        *,
        signature: str,
        start_ms: int,
        end_ms: int,
        cached_replay: Mapping[str, Any],
        fields: tuple[str, ...],
    ) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payloads = tuple(cached_replay.get("aggregate_payloads") or ())
        with self._connection(readonly=False) as connection:
            connection.execute(
                """
                DELETE FROM refill_scan_rows
                WHERE signature = ? AND marker_time_ms >= ? AND marker_time_ms < ?
                """,
                (signature, int(start_ms), int(end_ms)),
            )
            connection.executemany(
                """
                INSERT OR REPLACE INTO refill_scan_rows(
                    signature, block_start_ms, marker_time_ms,
                    marker_price, side, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        signature,
                        int(start_ms),
                        int(payload.get("marker_time_ms") or payload.get("timestamp_ms") or 0),
                        str(payload.get("marker_price") or payload.get("price") or ""),
                        str(payload.get("side") or ""),
                        json.dumps(
                            {field: payload.get(field) for field in fields},
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                    for payload in payloads
                ),
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO refill_scan_blocks(
                    signature, start_ms, end_ms, status,
                    processed_event_count, emitted_payload_count,
                    footprint_candle_count, footprint_symbols_json
                ) VALUES (?, ?, ?, 'READY', ?, ?, ?, ?)
                """,
                (
                    signature,
                    int(start_ms),
                    int(end_ms),
                    int(cached_replay.get("processed_event_count") or 0),
                    int(cached_replay.get("emitted_payload_count") or 0),
                    int(cached_replay.get("footprint_candle_count") or 0),
                    json.dumps(
                        list(cached_replay.get("footprint_symbols") or ()),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                ),
            )
            connection.commit()

    @staticmethod
    def _covers(blocks: Iterable[tuple[Any, ...]], *, start_ms: int, end_ms: int) -> bool:
        covered_until = int(start_ms)
        for block in blocks:
            block_start = int(block[0])
            block_end = int(block[1])
            if block_end <= covered_until:
                continue
            if block_start > covered_until:
                return False
            covered_until = max(covered_until, block_end)
            if covered_until >= int(end_ms):
                return True
        return covered_until >= int(end_ms)

    def _connection(self, *, readonly: bool) -> sqlite3.Connection:
        if readonly:
            connection = sqlite3.connect(
                f"file:{self.path.resolve().as_posix()}?mode=ro",
                uri=True,
                timeout=2,
            )
        else:
            connection = sqlite3.connect(str(self.path), timeout=30)
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=NORMAL")
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS refill_scan_blocks(
                    signature TEXT NOT NULL,
                    start_ms INTEGER NOT NULL,
                    end_ms INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    processed_event_count INTEGER NOT NULL DEFAULT 0,
                    emitted_payload_count INTEGER NOT NULL DEFAULT 0,
                    footprint_candle_count INTEGER NOT NULL DEFAULT 0,
                    footprint_symbols_json TEXT NOT NULL DEFAULT '[]',
                    PRIMARY KEY(signature, start_ms, end_ms)
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS refill_scan_rows(
                    signature TEXT NOT NULL,
                    block_start_ms INTEGER NOT NULL,
                    marker_time_ms INTEGER NOT NULL,
                    marker_price TEXT NOT NULL,
                    side TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    PRIMARY KEY(signature, marker_time_ms, marker_price, side)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_refill_scan_rows_signature_time
                ON refill_scan_rows(signature, marker_time_ms)
                """
            )
        return connection
