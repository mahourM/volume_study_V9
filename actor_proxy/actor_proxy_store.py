from __future__ import annotations

from threading import RLock

from actor_proxy.actor_proxy_models import ActorCluster, ExitSignal


class ActorProxyStore:
    def __init__(self) -> None:
        self._clusters_by_position_id: dict[str, ActorCluster] = {}
        self._exit_signals_by_position_id: dict[str, ExitSignal] = {}
        self._lock = RLock()

    def set_cluster(self, cluster: ActorCluster) -> None:
        with self._lock:
            self._clusters_by_position_id[cluster.position_context.position_id] = cluster

    def cluster_for_position(self, position_id: str) -> ActorCluster | None:
        with self._lock:
            return self._clusters_by_position_id.get(str(position_id or "").strip())

    def active_clusters(self) -> tuple[ActorCluster, ...]:
        with self._lock:
            return tuple(self._clusters_by_position_id.values())

    def remove_cluster(self, position_id: str) -> ActorCluster | None:
        with self._lock:
            return self._clusters_by_position_id.pop(str(position_id or "").strip(), None)

    def record_exit_signal(self, signal: ExitSignal) -> None:
        with self._lock:
            self._exit_signals_by_position_id[signal.position_id] = signal

    def exit_signal_for_position(self, position_id: str) -> ExitSignal | None:
        with self._lock:
            return self._exit_signals_by_position_id.get(str(position_id or "").strip())
