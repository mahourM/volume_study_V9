from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Tuple


class TradeSide(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    NONE = "NONE"


class ClusterStatus(str, Enum):
    ACTIVE = "ACTIVE"
    INVALIDATED = "INVALIDATED"
    EXPIRED = "EXPIRED"


class MoveClassification(str, Enum):
    UNCERTAIN = "UNCERTAIN"


class AbsorptionCandidateType(str, Enum):
    SINGLE_TIME_SPIKE = "SINGLE_TIME_SPIKE"
    ADJACENT_TIME_CLUSTER = "ADJACENT_TIME_CLUSTER"


@dataclass(frozen=True)
class TimeframeSpec:
    name: str
    duration_ms: int


@dataclass(frozen=True)
class AbsorptionRuntimeConfig:
    enabled_timeframes: Tuple[TimeframeSpec, ...]
    rolling_candle_buffer_size: int
    single_time_share_threshold: float = 0.25
    adjacent_time_cluster_share_threshold: float = 0.35
    volume_spike_multiplier: float = 2.5
    volume_balance_limit: float = 2.0
    single_diagonal_spike_multiplier: float = 2.5
    cluster_diagonal_spike_multiplier: float = 1.5
    diagonal_balance_limit: float = 2.0
    zone_expiry_closed_candles: int = 100
    entry_absorption_volume_percentile: float = 0.75
    entry_absorption_diagonal_multiplier: float = 2.5
    entry_absorption_duration_percentile: float = 0.75
    entry_absorption_efficiency_percentile: float = 0.20
    entry_dominance_ratio: float = 6.5
    entry_dominance_duration_percentile: float = 0.10
    entry_dominance_efficiency_percentile: float = 0.80
    min_valid_bin_volume_percentile: float = 0.15
    dominant_side_min_opposite_volume_ratio: float = 0.20
    minimum_required_bins_for_zscore: int = 5
    entry_dominance_efficiency_zscore: float = 1.50
    exit_absorption_efficiency_percentile: float = 0.20
    exit_dominance_efficiency_percentile: float = 0.80
    exit_dominance_efficiency_zscore: float = 1.50
<<<<<<< HEAD
    entry_state_timeout_closed_candles: int = 3
=======
    entry_state_timeout_closed_candles: int = 2
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    entry_ratio_epsilon: float = 0.000001


@dataclass(frozen=True)
class BinMarketData:
    symbol: str
    timeframe_name: str
    candle_open_time_utc_ms: int
    candle_close_time_utc_ms: int
    bin_index: int
    price_low: float
    price_high: float
    price_progress: float
    total_volume: float
    delta_volume: float
    time_in_bin_ms: int
    horizontal_delta: float = 0.0
    ask_traded_volume: float = 0.0
    bid_traded_volume: float = 0.0
    buy_diagonal_imbalance_ratio: float = 0.0
    sell_diagonal_imbalance_ratio: float = 0.0
    min_trade_price_in_bin: float | None = None
    max_trade_price_in_bin: float | None = None
    price_progress_in_bin: float | None = None
    dominant_diagonal_side: str = "NONE"
    dominant_side_volume: float = 0.0
    dominant_side_efficiency: float | None = None
<<<<<<< HEAD
    contract_spike_score: float = 0.0
    abnormal_contract: bool = False
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    volume_percentile: float | None = None
    is_volume_valid: bool = True
    efficiency_percentile: float | None = None
    efficiency_zscore: float | None = None
    rejection_reason: str = ""


@dataclass(frozen=True)
class CandleAbsorptionResult:
    symbol: str
    timeframe_name: str
    candle_open_time_utc_ms: int
    candle_close_time_utc_ms: int
    detected: bool
    setup_side: TradeSide
    score: float
    zone_low: Optional[float]
    zone_high: Optional[float]
    dominant_bins: Tuple[BinMarketData, ...]
    opposite_side_score: float
    dominance_score: float
    candidate_type: AbsorptionCandidateType | None = None
    delta_volume: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_diagonal_pressure: float = 0.0
    sell_diagonal_pressure: float = 0.0
    time_share: float = 0.0


@dataclass
class AbsorptionCluster:
    cluster_id: str
    symbol: str
    timeframe_name: str
    timeframe_duration_ms: int
    created_time_utc_ms: int
    expire_time_utc_ms: int
    zone_low: float
    zone_high: float
    setup_side: TradeSide
    absorbed_aggression_side: TradeSide
    cluster_score: float
    dominance_score: float
    dominant_bins: Tuple[BinMarketData, ...]
    candidate_type: AbsorptionCandidateType | None = None
    delta_volume: float = 0.0
    buy_volume: float = 0.0
    sell_volume: float = 0.0
    buy_diagonal_pressure: float = 0.0
    sell_diagonal_pressure: float = 0.0
    zone_height: float = 0.0
    effective_low: float = 0.0
    effective_high: float = 0.0
    expires_after_candles: int = 100
    created_candle_count: int = 0
    entry_signal: bool = False
    entry_side: TradeSide = TradeSide.NONE
    stop_loss: float | None = None
    entry_candle_open_time_utc_ms: int = 0
    entry_candle_close_time_utc_ms: int = 0
    status: ClusterStatus = ClusterStatus.ACTIVE
    metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class RollingCandleBuffer:
    symbol: str
    timeframe_name: str
    max_candles: int
    candle_results: List[CandleAbsorptionResult] = field(default_factory=list)
    candle_bins: Dict[int, Tuple[BinMarketData, ...]] = field(default_factory=dict)
