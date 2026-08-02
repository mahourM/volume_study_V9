from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any


DATA_PROCESS_ENGINE_PRODUCER = "data_process"
DATA_PROCESS_REFILL_OUTPUT_TYPE = "DATA_PROCESS_REFILL_ORDER_CLOSED"
DATA_PROCESS_ENTRY_ACTION = "ENTRY"
DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION = "ABSORPTION"
DATA_PROCESS_PAYLOAD_TYPE_ICEBURG = "ICEBURG"
DATA_PROCESS_PAYLOAD_TYPES = (
    DATA_PROCESS_PAYLOAD_TYPE_ABSORPTION,
    DATA_PROCESS_PAYLOAD_TYPE_ICEBURG,
)


@dataclass(frozen=True)
class ProcessSymbol:
    provider_symbol: str
    mt5_symbol: str
    market_provider: str
    dataset: str
    schema: str
    tick_size: Decimal
    timeframe: str
    interval: str
    contract_symbols: tuple[str, ...] = ()

    @property
    def key(self) -> tuple[str, str]:
        return (self.provider_symbol.strip().upper(), self.timeframe.strip().upper())


@dataclass(frozen=True)
class ProcessReplayRequest:
    start_ms: int
    end_ms: int
    symbols: tuple[ProcessSymbol, ...] = ()
    emit_start_ms: int | None = None


@dataclass(frozen=True)
class ProcessFootprintSnapshot:
    symbol: ProcessSymbol
    candles: tuple[dict[str, Any], ...]

    @property
    def candle_count(self) -> int:
        return len(self.candles)


@dataclass(frozen=True)
class DataProcessConfig:
    max_payloads: int = 100_000
    emit_individual_refill_orders: bool = False
    emit_price_activity_levels: bool = False
    collect_event_payloads: bool = True
    filter_price_activity_to_footprints: bool = False
    events_are_time_ordered: bool = False
    deduplicate_events: bool = True


@dataclass(frozen=True)
class ProcessRunResult:
    start_ms: int
    end_ms: int
    symbols: tuple[ProcessSymbol, ...]
    processed_event_count: int
    emitted_payload_count: int
    payloads: tuple[dict[str, Any], ...]
    footprints: tuple[ProcessFootprintSnapshot, ...] = ()
    footprint_candle_count: int = 0
