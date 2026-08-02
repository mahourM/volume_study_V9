from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


class FootprintCandleIndex:
    def __init__(self, path: Path) -> None:
        self.path = path

    @staticmethod
    def signature(parts: tuple[Any, ...]) -> str:
        serialized = json.dumps(
            parts,
            ensure_ascii=True,
            default=str,
            separators=(",", ":"),
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def load_window(
        self,
        *,
        signature: str,
        requested_start_ms: int,
        requested_end_ms: int,
        candle_limit: int,
    ) -> dict[str, Any] | None:
        if (
            not self.path.exists()
            or int(requested_end_ms) <= int(requested_start_ms)
            or int(candle_limit) <= 0
        ):
            return None
        with self._connection(readonly=True) as connection:
            blocks = connection.execute(
                """
                SELECT start_ms, end_ms
                FROM footprint_blocks
                WHERE signature = ? AND status = 'READY'
                  AND end_ms > ? AND start_ms < ?
                ORDER BY start_ms, end_ms
                """,
                (
                    signature,
                    int(requested_start_ms),
                    int(requested_end_ms),
                ),
            ).fetchall()
            if not self._covers(
                blocks,
                start_ms=int(requested_start_ms),
                end_ms=int(requested_end_ms),
            ):
                return None
            rows = connection.execute(
                """
                SELECT open_time_ms, candle_json, contract_symbol,
                       trading_day, trade_count
                FROM footprint_candles
                WHERE signature = ? AND open_time_ms < ?
                ORDER BY open_time_ms DESC
                LIMIT ?
                """,
                (signature, int(requested_end_ms), int(candle_limit)),
            ).fetchall()
            if not rows:
                return None
            rows.reverse()
            first_open_ms = int(rows[0][0])
            has_older_data = (
                connection.execute(
                    """
                    SELECT 1 FROM footprint_candles
                    WHERE signature = ? AND open_time_ms < ?
                    LIMIT 1
                    """,
                    (signature, first_open_ms),
                ).fetchone()
                is not None
            )
        candles = [
            json.loads(gzip.decompress(bytes(row[1])).decode("utf-8"))
            for row in rows
        ]
        contract_symbols = tuple(
            dict.fromkeys(str(row[2]) for row in rows if str(row[2]).strip())
        )
        trading_days = tuple(
            dict.fromkeys(str(row[3]) for row in rows if str(row[3]).strip())
        )
        return {
            "candles": candles,
            "contract_symbols": contract_symbols,
            "trading_days": trading_days,
            "processed_trades": sum(int(row[4] or 0) for row in rows),
            "window_start_ms": first_open_ms,
            "has_older_data": has_older_data,
        }

    def store_window(
        self,
        *,
        signature: str,
        coverage_start_ms: int,
        coverage_end_ms: int,
        candles: Iterable[Mapping[str, Any]],
        contract_symbol: str,
        trading_day_by_open: Mapping[int, str],
        trade_count_by_open: Mapping[int, int],
    ) -> None:
        normalized_candles = tuple(dict(candle) for candle in candles)
        if int(coverage_end_ms) <= int(coverage_start_ms):
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection(readonly=False) as connection:
            connection.executemany(
                """
                INSERT OR REPLACE INTO footprint_candles(
                    signature, open_time_ms, candle_json, contract_symbol,
                    trading_day, trade_count
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        signature,
                        int(candle.get("open_time_ms") or candle.get("open_time") or 0),
                        sqlite3.Binary(
                            gzip.compress(
                                json.dumps(
                                    candle,
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                ).encode("utf-8"),
                                compresslevel=1,
                            )
                        ),
                        str(contract_symbol or ""),
                        str(
                            trading_day_by_open.get(
                                int(
                                    candle.get("open_time_ms")
                                    or candle.get("open_time")
                                    or 0
                                ),
                                candle.get("trading_day") or "",
                            )
                        ),
                        int(
                            trade_count_by_open.get(
                                int(
                                    candle.get("open_time_ms")
                                    or candle.get("open_time")
                                    or 0
                                ),
                                0,
                            )
                        ),
                    )
                    for candle in normalized_candles
                    if int(candle.get("open_time_ms") or candle.get("open_time") or 0) > 0
                ),
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO footprint_blocks(
                    signature, start_ms, end_ms, status
                ) VALUES (?, ?, ?, 'READY')
                """,
                (
                    signature,
                    int(coverage_start_ms),
                    int(coverage_end_ms),
                ),
            )
            connection.commit()

    @staticmethod
    def _covers(
        blocks: Iterable[tuple[Any, ...]],
        *,
        start_ms: int,
        end_ms: int,
    ) -> bool:
        covered_until = int(start_ms)
        for block_start, block_end in blocks:
            block_start = int(block_start)
            block_end = int(block_end)
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
            return sqlite3.connect(
                f"file:{self.path.resolve().as_posix()}?mode=ro",
                uri=True,
                timeout=2,
            )
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS footprint_blocks(
                signature TEXT NOT NULL,
                start_ms INTEGER NOT NULL,
                end_ms INTEGER NOT NULL,
                status TEXT NOT NULL,
                PRIMARY KEY(signature, start_ms, end_ms)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS footprint_candles(
                signature TEXT NOT NULL,
                open_time_ms INTEGER NOT NULL,
                candle_json BLOB NOT NULL,
                contract_symbol TEXT NOT NULL DEFAULT '',
                trading_day TEXT NOT NULL DEFAULT '',
                trade_count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(signature, open_time_ms)
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_footprint_candles_signature_time
            ON footprint_candles(signature, open_time_ms)
            """
        )
        return connection
