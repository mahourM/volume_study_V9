from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Mapping

from actor_proxy.actor_proxy_config import ActorProxyConfig
from actor_proxy.actor_proxy_models import (
    ACTION_ADD,
    ACTION_CANCEL,
    ACTION_CLEAR,
    ACTION_FILL,
    ACTION_MODIFY,
    ACTION_TRADE,
    CANDIDATE_ACTIVE,
    CANDIDATE_CANCELLED,
    CANDIDATE_EXITED,
    CANDIDATE_FILLED,
    CANDIDATE_LOST,
    CANDIDATE_REDUCED,
    CANDIDATE_REPLACED,
    ActorCandidate,
    ActorCluster,
    ActorClusterMetrics,
    ActorProxyOrder,
    ActorProxyPayload,
    ExitSignal,
    PositionContext,
    RawDomEvent,
)
from actor_proxy.actor_proxy_store import ActorProxyStore


LOGGER = logging.getLogger(__name__)


class ActorProxyEngine:
    def __init__(
        self,
        config: ActorProxyConfig | None = None,
        store: ActorProxyStore | None = None,
    ) -> None:
        self.config = config or ActorProxyConfig()
        self.store = store or ActorProxyStore()
        self._recent_add_events: list[RawDomEvent] = []

    def start_tracking(
        self,
        actor_payload: ActorProxyPayload | Mapping[str, Any],
        position_context: PositionContext | Mapping[str, Any],
    ) -> ActorCluster:
        payload = (
            actor_payload
            if isinstance(actor_payload, ActorProxyPayload)
            else ActorProxyPayload.from_mapping(actor_payload)
        )
        context = (
            position_context
            if isinstance(position_context, PositionContext)
            else PositionContext.from_mapping(position_context)
        )
        orders = self._orders_with_importance(payload.orders)
        candidates = [
            self._candidate_from_order(order, index=index)
            for index, order in enumerate(orders[: self.config.max_tracked_candidates])
        ]
        cluster = ActorCluster(
            cluster_id="|".join(("ACTOR_PROXY", context.position_id, payload.payload_id)),
            payload=payload,
            position_context=context,
            candidates=candidates,
            started_ts_event_ms=context.effective_tracking_start_ms,
            last_event_ts_event_ms=context.effective_tracking_start_ms,
        )
        self.store.set_cluster(cluster)
        LOGGER.info(
            "ACTOR_PROXY_TRACKING_STARTED | position_id=%s | payload_id=%s | symbol=%s | timeframe=%s | candidates=%d | mode=%s | tracking_start_ms=%s | tracking_end_ms=%s",
            context.position_id,
            payload.payload_id,
            payload.provider_symbol or payload.symbol,
            payload.timeframe,
            len(candidates),
            context.mode,
            context.effective_tracking_start_ms,
            context.tracking_end_ms,
        )
        for candidate in candidates:
            LOGGER.info(
                "ACTOR_PROXY_CANDIDATE_ADDED | position_id=%s | order_id=%s | side=%s | price=%s | initial_size=%d | importance_score=%s",
                context.position_id,
                candidate.original_order_id,
                candidate.side,
                candidate.price,
                candidate.initial_size,
                candidate.importance_score,
            )
        return cluster

    def on_raw_dom_event(
        self,
        event: RawDomEvent | Mapping[str, Any],
    ) -> tuple[ExitSignal, ...]:
        raw_event = event if isinstance(event, RawDomEvent) else RawDomEvent.from_mapping(event)
        signals: list[ExitSignal] = []
        for cluster in self.store.active_clusters():
            if not self._event_is_in_tracking_window(raw_event, cluster):
                continue
            if not self._event_matches_cluster(raw_event, cluster):
                continue
            self._apply_event(cluster, raw_event)
            signal = self.update_cluster_state(
                position_id=cluster.position_context.position_id,
                now_ms=raw_event.ts_event_ms,
            )
            if signal is not None:
                signals.append(signal)
        return tuple(signals)

    def update_cluster_state(
        self,
        position_id: str | None = None,
        *,
        now_ms: int | None = None,
    ) -> ExitSignal | None:
        clusters = (
            (self.store.cluster_for_position(position_id),)
            if position_id is not None
            else self.store.active_clusters()
        )
        for cluster in clusters:
            if cluster is None:
                continue
            now = int(now_ms if now_ms is not None else cluster.last_event_ts_event_ms)
            metrics = self.cluster_metrics(cluster.position_context.position_id, now_ms=now)
            if cluster.exit_signal is not None:
                return cluster.exit_signal
            if not self._exit_conditions_met(cluster, metrics, now_ms=now):
                cluster.weakened_since_ts_event_ms = 0
                continue
            if cluster.weakened_since_ts_event_ms <= 0:
                cluster.weakened_since_ts_event_ms = now
                LOGGER.info(
                    "ACTOR_PROXY_CLUSTER_WEAKENED | position_id=%s | active_contract_ratio=%s | cluster_exit_score=%s | ts_event_ms=%s",
                    cluster.position_context.position_id,
                    metrics.active_contract_ratio,
                    metrics.cluster_exit_score,
                    now,
                )
                continue
            if now - cluster.weakened_since_ts_event_ms < self.config.confirm_window_ms:
                continue
            signal = self._exit_signal(cluster, metrics, now_ms=now)
            cluster.exit_signal = signal
            self.store.record_exit_signal(signal)
            LOGGER.info(
                "ACTOR_PROXY_EXIT_SIGNAL | position_id=%s | symbol=%s | active_contract_ratio=%s | cluster_exit_score=%s | confidence_score=%s | ts_event_ms=%s",
                signal.position_id,
                signal.symbol,
                signal.active_contract_ratio,
                signal.cluster_exit_score,
                signal.confidence_score,
                signal.ts_event_ms,
            )
            return signal
        return None

    def cluster_metrics(
        self,
        position_id: str,
        *,
        now_ms: int | None = None,
    ) -> ActorClusterMetrics:
        cluster = self.store.cluster_for_position(position_id)
        if cluster is None:
            raise KeyError(f"actor proxy cluster not found for position {position_id!r}")
        now = int(now_ms if now_ms is not None else cluster.last_event_ts_event_ms)
        self._prune_activity_events(cluster, now_ms=now)
        total_initial = sum(max(0, candidate.initial_size) for candidate in cluster.candidates)
        total_active = sum(max(0, candidate.current_size) for candidate in cluster.candidates if candidate.is_active)
        cancelled = sum(max(0, candidate.cancelled_contracts) for candidate in cluster.candidates)
        filled = sum(max(0, candidate.filled_contracts) for candidate in cluster.candidates)
        replaced = sum(max(0, candidate.replaced_contracts) for candidate in cluster.candidates)
        active_ratio = _ratio(total_active, total_initial)
        refill_rate = self._activity_rate(cluster, "REFILL")
        cancel_rate = self._activity_rate(cluster, "CANCEL")
        add_rate = self._activity_rate(cluster, "ADD")
        dominant = self._dominant_candidate(cluster)
        dominant_active = bool(dominant and dominant.is_active)
        last_refill = max((candidate.last_refill_ts_event_ms for candidate in cluster.candidates), default=0)
        last_replacement = max((candidate.last_replacement_ts_event_ms for candidate in cluster.candidates), default=0)
        time_since_refill = max(0, now - last_refill) if last_refill > 0 else max(0, now - cluster.started_ts_event_ms)
        time_since_replacement = (
            max(0, now - last_replacement)
            if last_replacement > 0
            else max(0, now - cluster.started_ts_event_ms)
        )
        refill_dropped = self._refill_rate_dropped(cluster, refill_rate)
        score = self._cluster_exit_score(
            active_ratio=active_ratio,
            cancelled_contracts=cancelled,
            filled_contracts=filled,
            total_initial_contracts=total_initial,
            dominant_actor_still_active=dominant_active,
            refill_dropped=refill_dropped,
            recent_replacement=last_replacement > 0
            and now - last_replacement <= self.config.replacement_time_gap_ms,
        )
        return ActorClusterMetrics(
            total_tracked_initial_contracts=total_initial,
            total_active_contracts=total_active,
            active_contract_ratio=active_ratio,
            cancelled_contracts=cancelled,
            filled_contracts=filled,
            replaced_contracts=replaced,
            refill_rate_recent=refill_rate,
            cancel_rate_recent=cancel_rate,
            add_rate_recent=add_rate,
            time_since_last_refill_ms=time_since_refill,
            time_since_last_replacement_ms=time_since_replacement,
            dominant_actor_still_active=dominant_active,
            cluster_exit_score=score,
        )

    def get_exit_signal(self, position_id: str) -> ExitSignal | None:
        cluster = self.store.cluster_for_position(position_id)
        if cluster is not None and cluster.exit_signal is not None:
            return cluster.exit_signal
        return self.store.exit_signal_for_position(position_id)

    def stop_tracking(self, position_id: str) -> None:
        cluster = self.store.remove_cluster(position_id)
        if cluster is None:
            return
        for candidate in cluster.candidates:
            candidate.status = CANDIDATE_EXITED

    def _orders_with_importance(
        self,
        orders: tuple[ActorProxyOrder, ...],
    ) -> tuple[ActorProxyOrder, ...]:
        if not orders:
            return tuple()
        max_initial = max(max(0, order.initial_size) for order in orders) or 1
        max_refill_contracts = max(max(0, order.refill_contracts) for order in orders) or 1
        max_refill_count = max(max(0, order.refill_count) for order in orders) or 1
        max_executed = max(max(0, order.executed_contracts) for order in orders) or 1
        max_persistence = max(max(0, order.persistence_ms) for order in orders) or 1
        weights = self.config.importance_weights
        scored = []
        for order in orders:
            score = (
                weights.initial_size * _ratio(order.initial_size, max_initial)
                + weights.refill_contracts * _ratio(order.refill_contracts, max_refill_contracts)
                + weights.refill_count * _ratio(order.refill_count, max_refill_count)
                + weights.executed_contracts * _ratio(order.executed_contracts, max_executed)
                + weights.persistence * _ratio(order.persistence_ms, max_persistence)
            )
            scored.append(order.with_importance_score(score))
        return tuple(
            sorted(
                scored,
                key=lambda item: (
                    item.importance_score,
                    item.initial_size,
                    item.refill_contracts,
                    item.refill_count,
                ),
                reverse=True,
            )
        )

    def _candidate_from_order(self, order: ActorProxyOrder, *, index: int) -> ActorCandidate:
        current_size = max(0, int(order.current_size))
        status = CANDIDATE_ACTIVE if current_size > 0 else CANDIDATE_LOST
        lost_since = 0 if current_size > 0 else order.last_seen_ts_event_ms
        return ActorCandidate(
            candidate_id=f"{index + 1}:{order.order_id}",
            original_order_id=order.order_id,
            active_order_id=order.order_id,
            symbol=order.symbol,
            provider_symbol=order.provider_symbol,
            instrument_id=order.instrument_id,
            side=order.side,
            price=order.price,
            initial_size=max(0, int(order.initial_size)),
            current_size=current_size,
            initial_refill_count=max(0, int(order.refill_count)),
            initial_refill_contracts=max(0, int(order.refill_contracts)),
            initial_executed_contracts=max(0, int(order.executed_contracts)),
            trade_count=max(0, int(order.trade_count)),
            importance_score=order.importance_score,
            first_seen_ts_event_ms=int(order.first_seen_ts_event_ms),
            last_seen_ts_event_ms=int(order.last_seen_ts_event_ms),
            source_file=order.source_file,
            source_id=order.source_id,
            reason=order.reason,
            status=status,
            lost_since_ts_event_ms=lost_since,
            raw_event_refs=order.raw_event_refs,
        )

    def _apply_event(self, cluster: ActorCluster, event: RawDomEvent) -> None:
        cluster.last_event_ts_event_ms = max(cluster.last_event_ts_event_ms, event.ts_event_ms)
        if event.action == ACTION_ADD:
            self._remember_add_event(event)
            self._record_activity(cluster, event.ts_event_ms, "ADD", event.size)
            existing_candidate = self._candidate_for_order_id(cluster, event.order_id)
            if existing_candidate is not None:
                if event.price is not None:
                    existing_candidate.price = event.price
                existing_candidate.current_size = max(0, event.size)
                existing_candidate.status = (
                    CANDIDATE_ACTIVE
                    if existing_candidate.status != CANDIDATE_REPLACED
                    else CANDIDATE_REPLACED
                )
                existing_candidate.last_seen_ts_event_ms = event.ts_event_ms
                existing_candidate.lost_since_ts_event_ms = 0
                return
            self._match_pending_replacements(cluster, event)
            return
        if event.action == ACTION_CLEAR:
            for candidate in cluster.candidates:
                if candidate.is_active:
                    self._mark_candidate_lost(cluster, candidate, event, CANDIDATE_LOST)
            return
        candidate = self._candidate_for_order_id(cluster, event.order_id)
        if candidate is None:
            return
        if event.action == ACTION_MODIFY:
            self._apply_modify(cluster, candidate, event)
        elif event.action in {ACTION_FILL, ACTION_TRADE}:
            self._apply_fill(cluster, candidate, event)
        elif event.action == ACTION_CANCEL:
            self._apply_cancel(cluster, candidate, event)

    def _apply_modify(
        self,
        cluster: ActorCluster,
        candidate: ActorCandidate,
        event: RawDomEvent,
    ) -> None:
        if event.price is not None:
            candidate.price = event.price
        new_size = event.size if event.size > 0 else candidate.current_size
        if new_size > candidate.current_size:
            refill_contracts = max(0, new_size - candidate.current_size)
            candidate.last_refill_ts_event_ms = event.ts_event_ms
            candidate.status = CANDIDATE_ACTIVE if candidate.status != CANDIDATE_REPLACED else CANDIDATE_REPLACED
            self._record_activity(cluster, event.ts_event_ms, "REFILL", refill_contracts)
        elif new_size < candidate.current_size:
            reduced_contracts = candidate.current_size - new_size
            candidate.cancelled_contracts += reduced_contracts
            candidate.status = CANDIDATE_REDUCED if new_size > 0 else CANDIDATE_CANCELLED
            self._record_activity(cluster, event.ts_event_ms, "CANCEL", reduced_contracts)
            LOGGER.info(
                "ACTOR_PROXY_ORDER_REDUCED | position_id=%s | order_id=%s | old_size=%d | new_size=%d | ts_event_ms=%s",
                cluster.position_context.position_id,
                candidate.active_order_id,
                candidate.current_size,
                new_size,
                event.ts_event_ms,
            )
        candidate.current_size = max(0, new_size)
        candidate.last_seen_ts_event_ms = event.ts_event_ms
        if candidate.current_size <= 0:
            self._mark_candidate_lost(cluster, candidate, event, candidate.status)

    def _apply_fill(
        self,
        cluster: ActorCluster,
        candidate: ActorCandidate,
        event: RawDomEvent,
    ) -> None:
        executed = event.size if event.size > 0 else candidate.current_size
        displayed_execution = min(executed, candidate.current_size)
        candidate.filled_contracts += executed
        candidate.current_size = max(0, candidate.current_size - displayed_execution)
        candidate.pending_refill_contracts += executed
        candidate.last_seen_ts_event_ms = event.ts_event_ms
        self._record_activity(cluster, event.ts_event_ms, "FILL", executed)
        if event.price is not None:
            candidate.price = event.price
        if candidate.current_size <= 0:
            self._mark_candidate_lost(cluster, candidate, event, CANDIDATE_FILLED)

    def _apply_cancel(
        self,
        cluster: ActorCluster,
        candidate: ActorCandidate,
        event: RawDomEvent,
    ) -> None:
        cancelled = event.size if event.size > 0 else candidate.current_size
        cancelled = min(cancelled, candidate.current_size) if candidate.current_size > 0 else cancelled
        candidate.cancelled_contracts += cancelled
        candidate.current_size = max(0, candidate.current_size - cancelled)
        candidate.last_seen_ts_event_ms = event.ts_event_ms
        self._record_activity(cluster, event.ts_event_ms, "CANCEL", cancelled)
        if event.price is not None:
            candidate.price = event.price
        if candidate.current_size > 0:
            candidate.status = CANDIDATE_REDUCED
            LOGGER.info(
                "ACTOR_PROXY_ORDER_REDUCED | position_id=%s | order_id=%s | remaining_size=%d | ts_event_ms=%s",
                cluster.position_context.position_id,
                candidate.active_order_id,
                candidate.current_size,
                event.ts_event_ms,
            )
            return
        LOGGER.info(
            "ACTOR_PROXY_ORDER_CANCELLED | position_id=%s | order_id=%s | ts_event_ms=%s",
            cluster.position_context.position_id,
            candidate.active_order_id,
            event.ts_event_ms,
        )
        self._mark_candidate_lost(cluster, candidate, event, CANDIDATE_CANCELLED)

    def _mark_candidate_lost(
        self,
        cluster: ActorCluster,
        candidate: ActorCandidate,
        event: RawDomEvent,
        status: str,
    ) -> None:
        candidate.status = status
        candidate.current_size = 0
        candidate.lost_since_ts_event_ms = event.ts_event_ms
        replacement = self._replacement_from_recent_adds(candidate, event.ts_event_ms)
        if replacement is not None:
            self._apply_replacement(cluster, candidate, replacement)
            return
        LOGGER.info(
            "ACTOR_PROXY_REPLACEMENT_NOT_FOUND | position_id=%s | order_id=%s | ts_event_ms=%s",
            cluster.position_context.position_id,
            candidate.active_order_id,
            event.ts_event_ms,
        )

    def _match_pending_replacements(self, cluster: ActorCluster, add_event: RawDomEvent) -> None:
        for candidate in cluster.candidates:
            if candidate.is_active:
                continue
            if candidate.lost_since_ts_event_ms <= 0:
                continue
            if self._replacement_matches(candidate, add_event, candidate.lost_since_ts_event_ms):
                self._apply_replacement(cluster, candidate, add_event)
                return

    def _apply_replacement(
        self,
        cluster: ActorCluster,
        candidate: ActorCandidate,
        event: RawDomEvent,
    ) -> None:
        old_order_id = candidate.active_order_id
        candidate.active_order_id = event.order_id
        candidate.status = CANDIDATE_REPLACED
        candidate.current_size = max(0, event.size)
        candidate.price = event.price or candidate.price
        candidate.replaced_contracts += max(0, event.size)
        candidate.last_replacement_ts_event_ms = event.ts_event_ms
        candidate.last_seen_ts_event_ms = event.ts_event_ms
        candidate.lost_since_ts_event_ms = 0
        self._record_activity(cluster, event.ts_event_ms, "REPLACE", event.size)
        LOGGER.info(
            "ACTOR_PROXY_REPLACEMENT_FOUND | position_id=%s | old_order_id=%s | new_order_id=%s | price=%s | size=%d | ts_event_ms=%s",
            cluster.position_context.position_id,
            old_order_id,
            event.order_id,
            event.price,
            event.size,
            event.ts_event_ms,
        )

    def _candidate_for_order_id(
        self,
        cluster: ActorCluster,
        order_id: str,
    ) -> ActorCandidate | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None
        for candidate in cluster.candidates:
            if candidate.active_order_id == normalized:
                return candidate
        return None

    def _dominant_candidate(self, cluster: ActorCluster) -> ActorCandidate | None:
        if not cluster.candidates:
            return None
        return max(cluster.candidates, key=lambda item: item.importance_score)

    def _event_is_in_tracking_window(self, event: RawDomEvent, cluster: ActorCluster) -> bool:
        context = cluster.position_context
        if event.ts_event_ms < context.effective_tracking_start_ms:
            return False
        if context.tracking_end_ms is not None and event.ts_event_ms > context.tracking_end_ms:
            return False
        return True

    def _event_matches_cluster(self, event: RawDomEvent, cluster: ActorCluster) -> bool:
        symbols = {
            item
            for item in (
                cluster.payload.symbol,
                cluster.payload.provider_symbol,
                cluster.position_context.symbol,
                cluster.position_context.provider_symbol,
            )
            if item
        }
        if event.symbol and symbols and event.symbol not in symbols:
            return False
        if event.instrument_id not in {None, "", 0}:
            instrument_ids = {
                str(item.instrument_id)
                for item in cluster.candidates
                if item.instrument_id not in {None, "", 0}
            }
            if instrument_ids and str(event.instrument_id) not in instrument_ids:
                return False
        return True

    def _remember_add_event(self, event: RawDomEvent) -> None:
        self._recent_add_events.append(event)
        cutoff = event.ts_event_ms - self.config.replacement_time_gap_ms
        self._recent_add_events = [
            item for item in self._recent_add_events if item.ts_event_ms >= cutoff
        ]

    def _replacement_from_recent_adds(
        self,
        candidate: ActorCandidate,
        lost_ts_event_ms: int,
    ) -> RawDomEvent | None:
        matches = [
            event
            for event in self._recent_add_events
            if self._replacement_matches(candidate, event, lost_ts_event_ms)
        ]
        if not matches:
            return None
        return min(matches, key=lambda item: abs(item.ts_event_ms - lost_ts_event_ms))

    def _replacement_matches(
        self,
        candidate: ActorCandidate,
        event: RawDomEvent,
        lost_ts_event_ms: int,
    ) -> bool:
        if event.action != ACTION_ADD:
            return False
        if not event.order_id or event.order_id == candidate.active_order_id:
            return False
        if event.side != candidate.side:
            return False
        if event.price is None:
            return False
        if candidate.instrument_id not in {None, "", 0} and event.instrument_id not in {None, "", 0}:
            if str(candidate.instrument_id) != str(event.instrument_id):
                return False
        price_distance = abs(event.price - candidate.price)
        max_distance = self.config.tick_size * Decimal(self.config.replacement_tick_distance)
        if price_distance > max_distance:
            return False
        if abs(event.ts_event_ms - int(lost_ts_event_ms)) > self.config.replacement_time_gap_ms:
            return False
        reference_size = candidate.initial_size or candidate.cancelled_contracts or event.size or 1
        size_ratio = _ratio(event.size, reference_size)
        return self.config.size_similarity_min <= size_ratio <= self.config.size_similarity_max

    def _record_activity(
        self,
        cluster: ActorCluster,
        ts_event_ms: int,
        activity_type: str,
        contracts: int,
    ) -> None:
        cluster.activity_events.append((int(ts_event_ms), str(activity_type), max(0, int(contracts))))
        self._prune_activity_events(cluster, now_ms=ts_event_ms)

    def _prune_activity_events(self, cluster: ActorCluster, *, now_ms: int) -> None:
        cutoff = int(now_ms) - self.config.recent_window_ms
        cluster.activity_events = [
            item for item in cluster.activity_events if item[0] >= cutoff
        ]

    def _activity_rate(self, cluster: ActorCluster, activity_type: str) -> Decimal:
        contracts = sum(
            item[2]
            for item in cluster.activity_events
            if item[1] == activity_type
        )
        seconds = Decimal(self.config.recent_window_ms) / Decimal(1000)
        return Decimal(contracts) / seconds if seconds > 0 else Decimal("0")

    def _refill_rate_dropped(self, cluster: ActorCluster, recent_refill_rate: Decimal) -> bool:
        baseline_contracts = sum(
            max(candidate.initial_refill_contracts, candidate.initial_refill_count)
            for candidate in cluster.candidates
        )
        baseline_rate = Decimal(max(1, baseline_contracts)) / (
            Decimal(self.config.recent_window_ms) / Decimal(1000)
        )
        return recent_refill_rate <= baseline_rate * self.config.refill_rate_drop_ratio

    def _cluster_exit_score(
        self,
        *,
        active_ratio: Decimal,
        cancelled_contracts: int,
        filled_contracts: int,
        total_initial_contracts: int,
        dominant_actor_still_active: bool,
        refill_dropped: bool,
        recent_replacement: bool,
    ) -> Decimal:
        threshold = self.config.active_contract_ratio_exit_threshold
        ratio_component = Decimal("0")
        if threshold > 0 and active_ratio < threshold:
            ratio_component = _clamp_ratio((threshold - active_ratio) / threshold)
        cancel_fill_component = _clamp_ratio(
            _ratio(cancelled_contracts + filled_contracts, total_initial_contracts)
        )
        score = (
            Decimal("0.35") * ratio_component
            + Decimal("0.20") * (Decimal("1") if refill_dropped else Decimal("0"))
            + Decimal("0.20") * cancel_fill_component
            + Decimal("0.15") * (Decimal("0") if dominant_actor_still_active else Decimal("1"))
            + Decimal("0.10") * (Decimal("0") if recent_replacement else Decimal("1"))
        )
        return _clamp_ratio(score)

    def _exit_conditions_met(
        self,
        cluster: ActorCluster,
        metrics: ActorClusterMetrics,
        *,
        now_ms: int,
    ) -> bool:
        if metrics.total_tracked_initial_contracts <= 0:
            return False
        active_ratio_low = (
            metrics.active_contract_ratio
            < self.config.active_contract_ratio_exit_threshold
        )
        refill_dropped = self._refill_rate_dropped(cluster, metrics.refill_rate_recent)
        primary_weak = any(
            candidate.status in {CANDIDATE_CANCELLED, CANDIDATE_FILLED, CANDIDATE_LOST}
            or (candidate.status == CANDIDATE_REDUCED and candidate.current_size < candidate.initial_size)
            for candidate in cluster.candidates
        )
        recent_replacement = any(
            candidate.last_replacement_ts_event_ms > 0
            and now_ms - candidate.last_replacement_ts_event_ms <= self.config.confirm_window_ms
            for candidate in cluster.candidates
        )
        score_ok = metrics.cluster_exit_score >= self.config.cluster_exit_score_threshold
        return active_ratio_low and refill_dropped and primary_weak and not recent_replacement and score_ok

    def _exit_signal(
        self,
        cluster: ActorCluster,
        metrics: ActorClusterMetrics,
        *,
        now_ms: int,
    ) -> ExitSignal:
        dominant = self._dominant_candidate(cluster)
        dominant_status = dominant.status if dominant is not None else ""
        confidence = _clamp_ratio(metrics.cluster_exit_score)
        explanation = (
            "ActorProxy cluster weakened: "
            f"active_contract_ratio={metrics.active_contract_ratio}, "
            f"dominant_actor_status={dominant_status}, "
            f"cancelled_contracts={metrics.cancelled_contracts}, "
            f"filled_contracts={metrics.filled_contracts}, "
            f"replaced_contracts={metrics.replaced_contracts}"
        )
        return ExitSignal(
            position_id=cluster.position_context.position_id,
            symbol=cluster.position_context.symbol or cluster.payload.symbol,
            side=cluster.position_context.side,
            confidence_score=confidence,
            cluster_exit_score=metrics.cluster_exit_score,
            active_contract_ratio=metrics.active_contract_ratio,
            dominant_actor_status=dominant_status,
            explanation=explanation,
            ts_event_ms=now_ms,
        )


def _ratio(numerator: int | Decimal, denominator: int | Decimal) -> Decimal:
    try:
        denominator_decimal = Decimal(str(denominator))
        if denominator_decimal <= 0:
            return Decimal("0")
        return Decimal(str(numerator)) / denominator_decimal
    except Exception:
        return Decimal("0")


def _clamp_ratio(value: Decimal) -> Decimal:
    if value < 0:
        return Decimal("0")
    if value > 1:
        return Decimal("1")
    return value
