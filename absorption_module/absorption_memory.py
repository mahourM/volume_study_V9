from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .absorption_cluster_model import AbsorptionCluster, ClusterStatus, RollingCandleBuffer, TradeSide


ClusterKey = Tuple[str, str]


def make_cluster_key(symbol: str, timeframe_name: str) -> ClusterKey:
    return (symbol, timeframe_name)


@dataclass
class AbsorptionMemoryState:
    clusters_by_symbol: Dict[ClusterKey, List[AbsorptionCluster]] = field(default_factory=dict)
    rolling_buffers: Dict[Tuple[str, str], RollingCandleBuffer] = field(default_factory=dict)
    closed_candle_counts: Dict[ClusterKey, int] = field(default_factory=dict)


def increment_closed_candle_count(memory: AbsorptionMemoryState, symbol: str, timeframe_name: str) -> int:
    key = make_cluster_key(symbol, timeframe_name)
    next_count = memory.closed_candle_counts.get(key, 0) + 1
    memory.closed_candle_counts[key] = next_count
    return next_count


def closed_candle_count(memory: AbsorptionMemoryState, symbol: str, timeframe_name: str) -> int:
    return memory.closed_candle_counts.get(make_cluster_key(symbol, timeframe_name), 0)


def upsert_cluster(memory: AbsorptionMemoryState, cluster: AbsorptionCluster) -> AbsorptionCluster:
    symbol_clusters = memory.clusters_by_symbol.setdefault(
        make_cluster_key(cluster.symbol, cluster.timeframe_name),
        [],
    )

    for index, existing_cluster in enumerate(symbol_clusters):
        if existing_cluster.status != ClusterStatus.ACTIVE:
            continue

        if existing_cluster.setup_side != cluster.setup_side:
            continue

        zones_overlap = (
            cluster.zone_low <= existing_cluster.zone_high
            and cluster.zone_high >= existing_cluster.zone_low
        )

        if not zones_overlap:
            continue

        existing_cluster.metadata.update(cluster.metadata)
        existing_cluster.metadata["last_update_time_utc_ms"] = cluster.created_time_utc_ms
        existing_cluster.metadata["last_update_cluster_id"] = cluster.cluster_id
        existing_cluster.created_time_utc_ms = cluster.created_time_utc_ms
        existing_cluster.expire_time_utc_ms = cluster.expire_time_utc_ms
        existing_cluster.expires_after_candles = cluster.expires_after_candles
        existing_cluster.created_candle_count = cluster.created_candle_count
        existing_cluster.entry_signal = False
        existing_cluster.entry_side = TradeSide.NONE
        existing_cluster.stop_loss = None
        existing_cluster.entry_candle_open_time_utc_ms = 0
        existing_cluster.entry_candle_close_time_utc_ms = 0

        if cluster.cluster_score > existing_cluster.cluster_score:
            existing_cluster.zone_low = cluster.zone_low
            existing_cluster.zone_high = cluster.zone_high
            existing_cluster.cluster_score = cluster.cluster_score
            existing_cluster.dominance_score = cluster.dominance_score
            existing_cluster.dominant_bins = cluster.dominant_bins
            existing_cluster.candidate_type = cluster.candidate_type
            existing_cluster.delta_volume = cluster.delta_volume
            existing_cluster.buy_volume = cluster.buy_volume
            existing_cluster.sell_volume = cluster.sell_volume
            existing_cluster.buy_diagonal_pressure = cluster.buy_diagonal_pressure
            existing_cluster.sell_diagonal_pressure = cluster.sell_diagonal_pressure
            existing_cluster.zone_height = cluster.zone_height
            existing_cluster.effective_low = cluster.effective_low
            existing_cluster.effective_high = cluster.effective_high

        symbol_clusters[index] = existing_cluster
        return existing_cluster

    symbol_clusters.append(cluster)
    return cluster


def get_clusters(
    memory: AbsorptionMemoryState,
    symbol: Optional[str] = None,
    timeframe_name: Optional[str] = None,
) -> Tuple[AbsorptionCluster, ...]:
    selected_clusters: List[AbsorptionCluster] = []
    for (item_symbol, item_timeframe), clusters in memory.clusters_by_symbol.items():
        if symbol is not None and item_symbol != symbol:
            continue
        if timeframe_name is not None and item_timeframe != timeframe_name:
            continue
        selected_clusters.extend(clusters)
    return tuple(selected_clusters)


def update_cluster_statuses(memory: AbsorptionMemoryState, symbol: str, current_time_utc_ms: int, current_price: float) -> None:
    for cluster in get_clusters(memory, symbol):
        if cluster.status in {ClusterStatus.INVALIDATED, ClusterStatus.EXPIRED}:
            continue
        if cluster.created_candle_count > 0:
            continue
        if current_time_utc_ms >= cluster.expire_time_utc_ms:
            cluster.status = ClusterStatus.EXPIRED
            continue


def expire_clusters_by_candle_count(memory: AbsorptionMemoryState, symbol: str, timeframe_name: str) -> None:
    current_count = closed_candle_count(memory, symbol, timeframe_name)
    for cluster in get_clusters(memory, symbol, timeframe_name):
        if cluster.status in {ClusterStatus.INVALIDATED, ClusterStatus.EXPIRED}:
            continue
        created_count = int(cluster.created_candle_count or 0)
        expires_after = int(cluster.expires_after_candles or 0)
        if created_count <= 0 or expires_after <= 0:
            continue
        if current_count - created_count >= expires_after:
            cluster.status = ClusterStatus.EXPIRED
        





def get_active_clusters(memory: AbsorptionMemoryState, symbol: Optional[str] = None) -> Tuple[AbsorptionCluster, ...]:
    selected_clusters: List[AbsorptionCluster] = []
    for cluster in get_clusters(memory, symbol):
        if cluster.status == ClusterStatus.ACTIVE:
            selected_clusters.append(cluster)
    return tuple(selected_clusters)


def prune_inactive_clusters(
    memory: AbsorptionMemoryState,
    symbol: Optional[str] = None,
    max_active_per_key: Optional[int] = None,
) -> None:
    for key in list(memory.clusters_by_symbol):
        if symbol is not None and key[0] != symbol:
            continue
        active_clusters = [
            cluster for cluster in memory.clusters_by_symbol.get(key, [])
            if cluster.status == ClusterStatus.ACTIVE
        ]
        if max_active_per_key is not None and max_active_per_key > 0 and len(active_clusters) > max_active_per_key:
            active_clusters = sorted(
            active_clusters,
            key=lambda cluster: (
                cluster.cluster_score,
                cluster.dominance_score,
                cluster.created_time_utc_ms,
                cluster.cluster_id,
            ),
        )[-max_active_per_key:]
        memory.clusters_by_symbol[key] = active_clusters
        if not memory.clusters_by_symbol[key]:
            memory.clusters_by_symbol.pop(key, None)
