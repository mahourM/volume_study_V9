from __future__ import annotations

import logging
import math
import statistics
import uuid
from collections.abc import Callable
from typing import Dict, Iterable, List, Sequence, Tuple

from .absorption_cluster_model import (
    AbsorptionCandidateType,
    AbsorptionCluster,
    AbsorptionRuntimeConfig,
    BinMarketData,
    CandleAbsorptionResult,
    TimeframeSpec,
    TradeSide,
)

LOGGER = logging.getLogger(__name__)

RejectionLogger = Callable[[str, str, int, int, str, tuple[int, ...], str], None]


def build_candle_absorption_results(
    bin_items: Sequence[BinMarketData],
    config: AbsorptionRuntimeConfig,
    rejection_logger: RejectionLogger | None = None,
) -> Tuple[CandleAbsorptionResult, ...]:
    if not bin_items:
        raise ValueError("bin_items cannot be empty")

    ordered_bins = tuple(sorted(bin_items, key=lambda item: item.bin_index))
    candle_duration_ms = _candle_duration_ms(ordered_bins)
    time_share_by_index = {
        item.bin_index: max(float(item.time_in_bin_ms), 0.0) / candle_duration_ms
        for item in ordered_bins
    }

    candidates = _build_time_candidates(
        ordered_bins,
        time_share_by_index,
        config,
        rejection_logger,
    )
    results: list[CandleAbsorptionResult] = []
    for candidate_type, candidate_bins in candidates:
        result = _evaluate_absorption_candidate(
            ordered_bins,
            candidate_type,
            candidate_bins,
            time_share_by_index,
            config,
            rejection_logger,
        )
        if result is not None:
            results.append(result)
    return tuple(results)


def build_candle_absorption_result(
    bin_items: Sequence[BinMarketData],
    config: AbsorptionRuntimeConfig,
    rejection_logger: RejectionLogger | None = None,
) -> CandleAbsorptionResult:
    results = build_candle_absorption_results(bin_items, config, rejection_logger)
    if results:
        return max(
            results,
            key=lambda item: (
                item.score,
                item.time_share,
                item.buy_volume + item.sell_volume,
            ),
        )
    return build_empty_candle_absorption_result(bin_items)


def build_empty_candle_absorption_result(bin_items: Sequence[BinMarketData]) -> CandleAbsorptionResult:
    if not bin_items:
        raise ValueError("bin_items cannot be empty")
    first_source = bin_items[0]
    return CandleAbsorptionResult(
        symbol=first_source.symbol,
        timeframe_name=first_source.timeframe_name,
        candle_open_time_utc_ms=first_source.candle_open_time_utc_ms,
        candle_close_time_utc_ms=first_source.candle_close_time_utc_ms,
        detected=False,
        setup_side=TradeSide.NONE,
        score=0.0,
        zone_low=None,
        zone_high=None,
        dominant_bins=tuple(),
        opposite_side_score=0.0,
        dominance_score=0.0,
    )


def _build_time_candidates(
    ordered_bins: Sequence[BinMarketData],
    time_share_by_index: dict[int, float],
    config: AbsorptionRuntimeConfig,
    rejection_logger: RejectionLogger | None,
) -> list[tuple[AbsorptionCandidateType, tuple[BinMarketData, ...]]]:
    candidates: list[tuple[AbsorptionCandidateType, tuple[BinMarketData, ...]]] = []
    single_spike_indexes: set[int] = set()
    for item in ordered_bins:
        if time_share_by_index[item.bin_index] >= config.single_time_share_threshold:
            candidates.append((AbsorptionCandidateType.SINGLE_TIME_SPIKE, (item,)))
            single_spike_indexes.add(item.bin_index)

    non_single_bins = [
        item for item in ordered_bins
        if item.bin_index not in single_spike_indexes
    ]
    for run in _adjacent_runs(non_single_bins):
        if len(run) < 2:
            continue
        windows = _adjacent_time_cluster_windows(
            run,
            time_share_by_index,
            config.adjacent_time_cluster_share_threshold,
        )
        if windows:
            for window in windows:
                candidates.append((AbsorptionCandidateType.ADJACENT_TIME_CLUSTER, window))
        else:
            first = ordered_bins[0]
            _reject(
                rejection_logger,
                first.symbol,
                first.timeframe_name,
                first.candle_open_time_utc_ms,
                first.candle_close_time_utc_ms,
                AbsorptionCandidateType.ADJACENT_TIME_CLUSTER.value,
                tuple(item.bin_index for item in run),
                "TIME_CONDITION_FAILED",
            )

    if not candidates:
        first = ordered_bins[0]
        _reject(
            rejection_logger,
            first.symbol,
            first.timeframe_name,
            first.candle_open_time_utc_ms,
            first.candle_close_time_utc_ms,
            "NONE",
            tuple(),
            "TIME_CONDITION_FAILED",
        )
    return candidates


def _evaluate_absorption_candidate(
    ordered_bins: Sequence[BinMarketData],
    candidate_type: AbsorptionCandidateType,
    candidate_bins: Sequence[BinMarketData],
    time_share_by_index: dict[int, float],
    config: AbsorptionRuntimeConfig,
    rejection_logger: RejectionLogger | None,
) -> CandleAbsorptionResult | None:
    first = ordered_bins[0]
    candidate_indexes = {item.bin_index for item in candidate_bins}
    reference_bins = [
        item for item in ordered_bins
        if item.bin_index not in candidate_indexes
    ]
    medians = _reference_medians(reference_bins)
    if medians is None:
        _reject_candidate(rejection_logger, first, candidate_type, candidate_bins, "REFERENCE_MEDIAN_UNAVAILABLE")
        return None

    median_buy_volume, median_sell_volume, median_buy_diagonal, median_sell_diagonal = medians
    buy_volume = sum(_buy_volume(item) for item in candidate_bins)
    sell_volume = sum(_sell_volume(item) for item in candidate_bins)
    delta_volume = sum(_horizontal_delta(item) for item in candidate_bins)
    time_share = sum(time_share_by_index[item.bin_index] for item in candidate_bins)

    if candidate_type == AbsorptionCandidateType.SINGLE_TIME_SPIKE:
        core_bin = candidate_bins[0]
        buy_diagonal_pressure = _buy_diagonal_pressure(core_bin)
        sell_diagonal_pressure = _sell_diagonal_pressure(core_bin)
        volume_result = _volume_rule_passes(
            buy_volume,
            sell_volume,
            median_buy_volume,
            median_sell_volume,
            config.volume_spike_multiplier,
            config.volume_balance_limit,
        )
        if volume_result is not None:
            _reject_candidate(rejection_logger, first, candidate_type, candidate_bins, volume_result)
            return None
        diagonal_result = _single_diagonal_rule_passes(
            buy_diagonal_pressure,
            sell_diagonal_pressure,
            median_buy_diagonal,
            median_sell_diagonal,
            config,
        )
        if diagonal_result is not None:
            _reject_candidate(rejection_logger, first, candidate_type, candidate_bins, diagonal_result)
            return None
    else:
        buy_diagonal_values = [_buy_diagonal_pressure(item) for item in candidate_bins]
        sell_diagonal_values = [_sell_diagonal_pressure(item) for item in candidate_bins]
        buy_diagonal_pressure = statistics.fmean(buy_diagonal_values)
        sell_diagonal_pressure = statistics.fmean(sell_diagonal_values)
        volume_result = _volume_rule_passes(
            buy_volume,
            sell_volume,
            median_buy_volume,
            median_sell_volume,
            config.volume_spike_multiplier,
            config.volume_balance_limit,
        )
        if volume_result is not None:
            _reject_candidate(rejection_logger, first, candidate_type, candidate_bins, volume_result)
            return None
        diagonal_result = _cluster_diagonal_rule_passes(
            candidate_bins,
            median_buy_diagonal,
            median_sell_diagonal,
            config,
        )
        if diagonal_result is not None:
            _reject_candidate(rejection_logger, first, candidate_type, candidate_bins, diagonal_result)
            return None

    if delta_volume > 0:
        setup_side = TradeSide.SELL
    elif delta_volume < 0:
        setup_side = TradeSide.BUY
    else:
        _reject_candidate(rejection_logger, first, candidate_type, candidate_bins, "ZERO_DELTA")
        return None

    zone_low = min(item.price_low for item in candidate_bins)
    zone_high = max(item.price_high for item in candidate_bins)
    volume_ratio = min(
        buy_volume / median_buy_volume,
        sell_volume / median_sell_volume,
    )
    diagonal_ratio = min(
        buy_diagonal_pressure / median_buy_diagonal,
        sell_diagonal_pressure / median_sell_diagonal,
    )
    score = time_share + volume_ratio + diagonal_ratio
    return CandleAbsorptionResult(
        symbol=first.symbol,
        timeframe_name=first.timeframe_name,
        candle_open_time_utc_ms=first.candle_open_time_utc_ms,
        candle_close_time_utc_ms=first.candle_close_time_utc_ms,
        detected=True,
        setup_side=setup_side,
        score=score,
        zone_low=zone_low,
        zone_high=zone_high,
        dominant_bins=tuple(candidate_bins),
        opposite_side_score=0.0,
        dominance_score=1.0,
        candidate_type=candidate_type,
        delta_volume=delta_volume,
        buy_volume=buy_volume,
        sell_volume=sell_volume,
        buy_diagonal_pressure=buy_diagonal_pressure,
        sell_diagonal_pressure=sell_diagonal_pressure,
        time_share=time_share,
    )


def _volume_rule_passes(
    buy_volume: float,
    sell_volume: float,
    median_buy_volume: float,
    median_sell_volume: float,
    multiplier: float,
    balance_limit: float,
) -> str | None:
    if buy_volume < multiplier * median_buy_volume:
        return "BUY_VOLUME_NOT_SIGNIFICANT"
    if sell_volume < multiplier * median_sell_volume:
        return "SELL_VOLUME_NOT_SIGNIFICANT"
    if not _balanced(buy_volume, sell_volume, balance_limit):
        return "VOLUME_BALANCE_FAILED"
    return None


def _single_diagonal_rule_passes(
    buy_diagonal_pressure: float,
    sell_diagonal_pressure: float,
    median_buy_diagonal: float,
    median_sell_diagonal: float,
    config: AbsorptionRuntimeConfig,
) -> str | None:
    if buy_diagonal_pressure < config.single_diagonal_spike_multiplier * median_buy_diagonal:
        return "BUY_DIAGONAL_NOT_SIGNIFICANT"
    if sell_diagonal_pressure < config.single_diagonal_spike_multiplier * median_sell_diagonal:
        return "SELL_DIAGONAL_NOT_SIGNIFICANT"
    if not _balanced(buy_diagonal_pressure, sell_diagonal_pressure, config.diagonal_balance_limit):
        return "DIAGONAL_BALANCE_FAILED"
    return None


def _cluster_diagonal_rule_passes(
    candidate_bins: Sequence[BinMarketData],
    median_buy_diagonal: float,
    median_sell_diagonal: float,
    config: AbsorptionRuntimeConfig,
) -> str | None:
    for item in candidate_bins:
        buy_diagonal_pressure = _buy_diagonal_pressure(item)
        sell_diagonal_pressure = _sell_diagonal_pressure(item)
        if buy_diagonal_pressure < config.cluster_diagonal_spike_multiplier * median_buy_diagonal:
            return "BUY_DIAGONAL_NOT_SIGNIFICANT"
        if sell_diagonal_pressure < config.cluster_diagonal_spike_multiplier * median_sell_diagonal:
            return "SELL_DIAGONAL_NOT_SIGNIFICANT"
        if not _balanced(buy_diagonal_pressure, sell_diagonal_pressure, config.diagonal_balance_limit):
            return "DIAGONAL_BALANCE_FAILED"
    return None


def _reference_medians(reference_bins: Sequence[BinMarketData]) -> tuple[float, float, float, float] | None:
    buy_volume = _positive_median(_buy_volume(item) for item in reference_bins)
    sell_volume = _positive_median(_sell_volume(item) for item in reference_bins)
    buy_diagonal = _positive_median(_buy_diagonal_pressure(item) for item in reference_bins)
    sell_diagonal = _positive_median(_sell_diagonal_pressure(item) for item in reference_bins)
    if (
        buy_volume is None
        or sell_volume is None
        or buy_diagonal is None
        or sell_diagonal is None
    ):
        return None
    return buy_volume, sell_volume, buy_diagonal, sell_diagonal


def _positive_median(values: Iterable[float]) -> float | None:
    positive_values = [
        float(value) for value in values
        if math.isfinite(float(value)) and float(value) > 0.0
    ]
    if not positive_values:
        return None
    return float(statistics.median(positive_values))


def _adjacent_runs(items: Sequence[BinMarketData]) -> list[tuple[BinMarketData, ...]]:
    if not items:
        return []
    runs: list[tuple[BinMarketData, ...]] = []
    current: list[BinMarketData] = []
    previous_index: int | None = None
    for item in sorted(items, key=lambda value: value.bin_index):
        if previous_index is None or item.bin_index == previous_index + 1:
            current.append(item)
        else:
            runs.append(tuple(current))
            current = [item]
        previous_index = item.bin_index
    if current:
        runs.append(tuple(current))
    return runs


def _adjacent_time_cluster_windows(
    run: Sequence[BinMarketData],
    time_share_by_index: dict[int, float],
    threshold: float,
) -> tuple[tuple[BinMarketData, ...], ...]:
    windows: list[tuple[BinMarketData, ...]] = []
    for start in range(len(run)):
        time_share = 0.0
        for end in range(start, len(run)):
            time_share += time_share_by_index[run[end].bin_index]
            if end == start:
                continue
            if time_share >= threshold:
                windows.append(tuple(run[start : end + 1]))
    return tuple(windows)


def _candle_duration_ms(ordered_bins: Sequence[BinMarketData]) -> float:
    first = ordered_bins[0]
    duration = int(first.candle_close_time_utc_ms) - int(first.candle_open_time_utc_ms)
    return float(max(duration, 1))


def _balanced(first_value: float, second_value: float, limit: float) -> bool:
    smaller = min(first_value, second_value)
    larger = max(first_value, second_value)
    if smaller <= 0.0:
        return False
    return larger / smaller <= limit


def _buy_volume(item: BinMarketData) -> float:
    return max(float(item.ask_traded_volume), 0.0)


def _sell_volume(item: BinMarketData) -> float:
    return max(float(item.bid_traded_volume), 0.0)


def _buy_diagonal_pressure(item: BinMarketData) -> float:
    return max(float(item.buy_diagonal_imbalance_ratio), 0.0)


def _sell_diagonal_pressure(item: BinMarketData) -> float:
    return max(float(item.sell_diagonal_imbalance_ratio), 0.0)


def _horizontal_delta(item: BinMarketData) -> float:
    return float(item.horizontal_delta)


def _reject_candidate(
    rejection_logger: RejectionLogger | None,
    first: BinMarketData,
    candidate_type: AbsorptionCandidateType,
    candidate_bins: Sequence[BinMarketData],
    reason: str,
) -> None:
    _reject(
        rejection_logger,
        first.symbol,
        first.timeframe_name,
        first.candle_open_time_utc_ms,
        first.candle_close_time_utc_ms,
        candidate_type.value,
        tuple(item.bin_index for item in candidate_bins),
        reason,
    )


def _reject(
    rejection_logger: RejectionLogger | None,
    symbol: str,
    timeframe_name: str,
    candle_open_time_utc_ms: int,
    candle_close_time_utc_ms: int,
    candidate_type: str,
    bin_indices: tuple[int, ...],
    reason: str,
) -> None:
    if rejection_logger is not None:
        rejection_logger(
            symbol,
            timeframe_name,
            candle_open_time_utc_ms,
            candle_close_time_utc_ms,
            candidate_type,
            bin_indices,
            reason,
        )
        return
    LOGGER.debug(
        "ABSORPTION_CANDIDATE_REJECTED | symbol=%s | timeframe=%s | "
        "candle_open_time_utc_ms=%d | candle_close_time_utc_ms=%d | "
        "candidate_type=%s | bin_indices=%s | reason=%s",
        symbol,
        timeframe_name,
        int(candle_open_time_utc_ms),
        int(candle_close_time_utc_ms),
        candidate_type,
        ",".join(str(item) for item in bin_indices),
        reason,
    )


def build_absorption_cluster(
    candle_result: CandleAbsorptionResult,
    timeframe_spec: TimeframeSpec,
    config: AbsorptionRuntimeConfig,
    *,
    created_candle_count: int = 0,
) -> AbsorptionCluster:
    if not candle_result.detected or candle_result.zone_low is None or candle_result.zone_high is None:
        raise ValueError("Cannot build an absorption cluster from an undetected candle result")
    created_time = candle_result.candle_close_time_utc_ms
    expire_time = created_time + (config.zone_expiry_closed_candles * timeframe_spec.duration_ms)
    absorbed_side = TradeSide.SELL if candle_result.setup_side == TradeSide.BUY else TradeSide.BUY
    zone_height = max(float(candle_result.zone_high) - float(candle_result.zone_low), 0.0)
    cluster = AbsorptionCluster(
        cluster_id=f"ABS-{candle_result.symbol}-{candle_result.timeframe_name}-{created_time}-{uuid.uuid4().hex[:8]}",
        symbol=candle_result.symbol,
        timeframe_name=candle_result.timeframe_name,
        timeframe_duration_ms=timeframe_spec.duration_ms,
        created_time_utc_ms=created_time,
        expire_time_utc_ms=expire_time,
        zone_low=candle_result.zone_low,
        zone_high=candle_result.zone_high,
        setup_side=candle_result.setup_side,
        absorbed_aggression_side=absorbed_side,
        cluster_score=candle_result.score,
        dominance_score=candle_result.dominance_score,
        dominant_bins=candle_result.dominant_bins,
        candidate_type=candle_result.candidate_type,
        delta_volume=candle_result.delta_volume,
        buy_volume=candle_result.buy_volume,
        sell_volume=candle_result.sell_volume,
        buy_diagonal_pressure=candle_result.buy_diagonal_pressure,
        sell_diagonal_pressure=candle_result.sell_diagonal_pressure,
        zone_height=zone_height,
        effective_low=float(candle_result.zone_low),
        effective_high=float(candle_result.zone_high),
        expires_after_candles=config.zone_expiry_closed_candles,
        created_candle_count=created_candle_count,
    )
    cluster.metadata["time_share"] = candle_result.time_share
    cluster.metadata["diagonal_pressure_by_bin"] = [
        {
            "bin_index": item.bin_index,
            "buy_diagonal_pressure": _buy_diagonal_pressure(item),
            "sell_diagonal_pressure": _sell_diagonal_pressure(item),
        }
        for item in candle_result.dominant_bins
    ]
    cluster.metadata["price_efficiency_by_bin"] = [
        {
            "bin_index": item.bin_index,
            "min_trade_price_in_bin": item.min_trade_price_in_bin,
            "max_trade_price_in_bin": item.max_trade_price_in_bin,
            "price_progress_in_bin": item.price_progress_in_bin,
            "dominant_diagonal_side": item.dominant_diagonal_side,
            "dominant_side_volume": item.dominant_side_volume,
            "dominant_side_efficiency": item.dominant_side_efficiency,
<<<<<<< HEAD
            "contract_spike_score": item.contract_spike_score,
            "abnormal_contract": item.abnormal_contract,
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        }
        for item in candle_result.dominant_bins
    ]
    return cluster


def build_clusters_from_candle_results(
    candle_results: Sequence[CandleAbsorptionResult],
    timeframe_specs: Dict[str, TimeframeSpec],
    config: AbsorptionRuntimeConfig,
) -> Tuple[AbsorptionCluster, ...]:
    clusters: List[AbsorptionCluster] = []
    for result in candle_results:
        if not result.detected:
            continue
        timeframe_spec = timeframe_specs.get(result.timeframe_name)
        if timeframe_spec is None:
            raise KeyError(f"Missing timeframe spec for {result.timeframe_name}")
        clusters.append(build_absorption_cluster(result, timeframe_spec, config))
    return tuple(clusters)
