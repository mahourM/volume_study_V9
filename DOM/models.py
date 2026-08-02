from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DomContext:
    mt5_symbol: str
    provider_symbol: str
    market_provider: str
    timeframe: str
    interval: str
    dataset: str
    schema: str
    tick_size: Decimal
    session_start_hour_chicago: int
    timezone: str
    trigger_timeout_candles: int
    initial_view_candles: int
    retention_ms: int
    data_dir: Path


@dataclass(frozen=True)
class DomFileInfo:
    path: Path
    name: str
    size_bytes: int
    modified_ns: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "size_bytes": int(self.size_bytes),
            "modified_ns": int(self.modified_ns),
        }


@dataclass(frozen=True)
class DomRawEvent:
    ts_event_ms: int
    price: Decimal | None
    size: int
    side: str
    action: str
    order_id: str
    instrument_id: int
    sequence: int
    source_file: str


@dataclass(frozen=True)
class DomProviderResult:
    files: tuple[DomFileInfo, ...]
    events: tuple[DomRawEvent, ...]
    earliest_event_time_ms: int
    latest_event_time_ms: int
    contract_symbols: tuple[str, ...]
    sampled: bool
    status: str
    message: str


@dataclass
class DomRefillLot:
    candle_open_time_ms: int
    price: Decimal
    side: str
    remaining_qty: int


@dataclass
class DomOrderState:
    order_id: str
    side: str
    price: Decimal
    size: int
    started_at_ms: int
    updated_at_ms: int
    pending_refill_contracts: int = 0
    existing_qty: int = 0
    refill_lots: list[DomRefillLot] = field(default_factory=list)
