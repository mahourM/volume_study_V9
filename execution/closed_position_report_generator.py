from __future__ import annotations

import logging
from dataclasses import dataclass
from decimal import Decimal

from config.config_runtime import RuntimeConfig
from cme_provider.engines import (
    CmeCandleEngine,
    CmeDailyVolumeProfileEngine,
    CmeEngineConfig,
    CmeFootprintEngine,
    CmePagedHistoryEngine,
)
from cme_provider.local_data import CmeLocalDataCatalog, CmeLocalDbnTradeStore
from execution.position_close_csv_recorder import get_position_close_csv_recorder
from triggerEngine import TriggerConfig, TriggerEngine


LOGGER = logging.getLogger(__name__)
REPORT_TIMEFRAME = "M1"


@dataclass(frozen=True)
class ClosedPositionReportResult:
    timeframe: str
    symbols: tuple[str, ...]
    closed_positions: int


def generate_closed_position_report(
    runtime_config: RuntimeConfig | None = None,
) -> ClosedPositionReportResult:
    runtime_config = runtime_config or RuntimeConfig()
    catalog = CmeLocalDataCatalog(
        data_dir=runtime_config.project_root / runtime_config.cme_local_data_dir_name,
        dataset=runtime_config.cme_dataset,
        schema=runtime_config.cme_schema,
        default_tick_size=runtime_config.cme_default_tick_size,
    )
    symbols = catalog.available_symbols()
    recorder = get_position_close_csv_recorder()
    recorder.configure(
        output_dir=runtime_config.project_root / "runtime_metrics",
        timeframes=(REPORT_TIMEFRAME,),
        point_value_by_symbol=runtime_config.trigger_point_value_by_symbol,
    )
    recorder.reset_timeframe_file(REPORT_TIMEFRAME)

    if not symbols:
        return ClosedPositionReportResult(
            timeframe=REPORT_TIMEFRAME,
            symbols=tuple(),
            closed_positions=0,
        )

    trade_store = CmeLocalDbnTradeStore(
        catalog,
        partition_cache_size=runtime_config.cme_partition_cache_size,
        trading_day_cache_size=runtime_config.cme_trading_day_cache_size,
        cumulative_delta_cache_size=runtime_config.cme_cumulative_delta_cache_size,
    )
    engine_config = CmeEngineConfig(
        session_start_hour_chicago=runtime_config.cme_trading_day_start_hour_chicago,
        new_york_session_start_hour=runtime_config.cme_new_york_session_start_hour,
        new_york_session_start_minute=runtime_config.cme_new_york_session_start_minute,
        new_york_session_end_hour=runtime_config.cme_new_york_session_end_hour,
        new_york_session_end_minute=runtime_config.cme_new_york_session_end_minute,
    )
    candle_engine = CmeCandleEngine(trade_store=trade_store)
    volume_profile_engine = CmeDailyVolumeProfileEngine(
        trade_store=trade_store,
        config=engine_config,
    )
    trigger_engine = TriggerEngine(_trigger_config(runtime_config))
    history_engine = CmePagedHistoryEngine(
        catalog=catalog,
        trade_store=trade_store,
        candle_engine=candle_engine,
        footprint_engine=CmeFootprintEngine(
            trade_store=trade_store,
            candle_engine=candle_engine,
            config=engine_config,
        ),
        volume_profile_engine=volume_profile_engine,
        config=engine_config,
        trigger_engine=trigger_engine,
        candle_cache_size=runtime_config.cme_candle_cache_size,
        footprint_cache_size=runtime_config.cme_footprint_cache_size,
    )

    closed_positions = 0
    for symbol in symbols:
        count = history_engine.record_closed_position_history(
            mt5_symbol=symbol,
            provider_symbol=symbol,
            timeframe=REPORT_TIMEFRAME,
            tick_size=catalog.tick_size_for(symbol),
        )
        closed_positions += count
        LOGGER.info(
            "CLOSED_POSITION_REPORT_SYMBOL_DONE | symbol=%s | timeframe=%s | closed_positions=%d",
            symbol,
            REPORT_TIMEFRAME,
            count,
        )

    return ClosedPositionReportResult(
        timeframe=REPORT_TIMEFRAME,
        symbols=symbols,
        closed_positions=closed_positions,
    )


def _trigger_config(runtime_config: RuntimeConfig) -> TriggerConfig:
    return TriggerConfig(
        supported_timeframes=(REPORT_TIMEFRAME,),
        confirmation_timeframe=runtime_config.trigger_confirmation_timeframe,
        efficiency_max=Decimal(runtime_config.trigger_efficiency_max),
        diagonal_ratio_min=Decimal(runtime_config.trigger_diagonal_ratio_min),
        contract_spike_score_min=Decimal(
            runtime_config.trigger_contract_spike_score_min
        ),
        reference_contract_spike_score_min=Decimal(
            runtime_config.trigger_reference_contract_spike_score_min
        ),
        reference_spike_score_deviation_min=Decimal(
            runtime_config.trigger_reference_spike_score_deviation_min
        ),
        reference_zone_tick_count=runtime_config.trigger_reference_zone_tick_count,
        point_value_by_symbol={
            symbol: Decimal(point_value)
            for symbol, point_value in runtime_config.trigger_point_value_by_symbol.items()
        },
        bin_tick_count=runtime_config.trigger_bin_tick_count,
        runtime_logging_enabled=runtime_config.trigger_runtime_logging_enabled,
    )


__all__ = [
    "ClosedPositionReportResult",
    "REPORT_TIMEFRAME",
    "generate_closed_position_report",
]
