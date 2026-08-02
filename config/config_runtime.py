from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class RuntimeConfig:
    tcp_host: str = "127.0.0.1"
    tcp_port: int = 5557
    absorption_http_host: str = "127.0.0.1"
    absorption_http_port: int = 8088
    absorption_poll_seconds: float = 10.0
    absorption_lookback_candles: int = 150
    absorption_memory_candles: int = 150
    absorption_duration_profile_freeze_delay_ms: int = 700
    volume_zscore_baseline_candles: int = 15
    volume_zscore_volume_floor: float = 0.01
    volume_zscore_z_cap: float = 5.0
    binance_api_base_url: str = "https://api.binance.com"
<<<<<<< HEAD
    cme_local_data_dir_name: str = "CME"
    cme_dataset: str = "GLBX.MDP3"
    cme_schema: str = "trades"
    cme_default_tick_size: str = "0.25"
    cme_trading_day_start_hour_chicago: int = 17
    cme_new_york_session_start_hour: int = 9
    cme_new_york_session_start_minute: int = 30
    cme_new_york_session_end_hour: int = 16
    cme_new_york_session_end_minute: int = 0
    cme_partition_cache_size: int = 8
    cme_trading_day_cache_size: int = 4
    cme_cumulative_delta_cache_size: int = 64
    cme_candle_cache_size: int = 20_000
    cme_footprint_cache_size: int = 10_000
    dom_data_dir_name: str = "DOM"
    dom_file_globs: tuple[str, ...] = ("*.dbn", "*.dbn.zst", "*.zip")
    dom_file_cache_size: int = 8
    dom_extracted_cache_dir_name: str = ".cache"
    dom_dbn_batch_size: int = 25_000
    warmup_historic_enabled: bool = True
    warmup_historic_default_days: int = 3
    warmup_historic_data_dir_name: str = "warmupHistoricData"
    warmup_historic_manifest_name: str = "manifest.json"
    warmup_historic_dom_dir_name: str = "dom"
    warmup_historic_l2_dir_name: str = "l2"
    warmup_historic_cache_dir_name: str = ".cache"
    warmup_historic_file_globs: tuple[str, ...] = (
        "*.dbn",
        "*.dbn.zst",
        "*.zip",
        "*.json",
        "*.jsonl",
        "*.csv",
    )
    dom_stream_bucket_ms: int = 1_000
    dom_stream_cache_max_buckets: int = 7_200
    dom_prefetch_window_multiplier: int = 4
    dom_prefetch_max_ms: int = 60_000
    dom_window_cache_size: int = 64
    dom_initial_view_candles: int = 3
    dom_provider_event_limit: int = 8_000
    dom_max_events_per_window: int = 3_000
    dom_max_resting_segments_per_window: int = 3_000
    dom_max_line_points_per_window: int = 2_000
    dom_max_price_levels: int = 500
    dom_time_bucket_divisor: int = 60
    dom_render_overscan_multiplier: int = 4
    dom_render_overscan_max_ms: int = 60_000
    dom_timezone: str = "America/Vancouver"
    viewport_snapshot_cache_ttl_seconds: float = 30.0
    trigger_timeframes: tuple[str, ...] = ()
    trigger_confirmation_timeframe: str = "M1"
    trigger_efficiency_max: str = "0.005"
    trigger_diagonal_ratio_min: str = "3.5"
    trigger_contract_spike_score_min: str = "5.0"
    trigger_reference_contract_spike_score_min: str = "14"
    trigger_reference_spike_score_deviation_min: str = "3.0"
    trigger_reference_zone_tick_count: int = 3
    trigger_point_value_by_symbol: dict[str, str] = field(
        default_factory=lambda: {
            "NQ": "20",
            "NQ.FUT": "20",
        }
    )
    trigger_bin_tick_count: int = 1
    trigger_runtime_logging_enabled: bool = False
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    project_root: Path = field(init=False)

    def __post_init__(self) -> None:
        project_root = Path(__file__).resolve().parent.parent
        object.__setattr__(self, "project_root", project_root)
