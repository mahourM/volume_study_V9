from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from typing import Any

from DOM.data_provider import DomDataProvider
from DOM.models import DomContext, DomOrderState, DomProviderResult, DomRawEvent, DomRefillLot
from core.engine_output_bus import DOM_ENGINE_PRODUCER, DOM_POSITIVE_REFILL_OUTPUT_TYPE
from core.timeframe_policy import TIMEFRAME_MS_BY_NAME


ADD_ACTIONS = frozenset({"A", "ADD"})
CANCEL_ACTIONS = frozenset({"C", "D", "R", "CANCEL", "DELETE", "CLEAR"})
MODIFY_ACTIONS = frozenset({"M", "MODIFY"})
EXECUTE_ACTIONS = frozenset({"F", "FILL"})
DOM_LOW_CONTRACT_THRESHOLD = 2
DOM_REFILL_POINT_MIN_COUNT = 10
DOM_REFILL_POINT_CANDLE_SPAN = 5


@dataclass(frozen=True)
class DomEngineConfig:
    window_cache_size: int
    max_events_per_window: int
    max_resting_segments_per_window: int
    max_line_points_per_window: int
    max_price_levels: int
    time_bucket_divisor: int
    render_overscan_multiplier: int
    render_overscan_max_ms: int


@dataclass(frozen=True)
class DomWindowPlan:
    start_ms: int
    end_ms: int
    load_start_ms: int
    navigation_start_ms: int
    navigation_end_ms: int
    selected_date: str


class DomWindowPlanningEngine:
    def resolve(
        self,
        *,
        provider: DomDataProvider,
        context: DomContext,
        start_time_ms: int | None,
        end_time_ms: int | None,
        selected_date: str | None,
    ) -> DomWindowPlan | None:
        date_bounds = _date_bounds_utc_ms(selected_date)
        normalized_end = int(end_time_ms) if end_time_ms and end_time_ms > 0 else 0
        normalized_start = int(start_time_ms) if start_time_ms and start_time_ms > 0 else 0

        if date_bounds is not None and normalized_end <= 0:
            date_start, date_end = date_bounds
            normalized_start = date_start if normalized_start <= 0 else normalized_start
            normalized_end = min(date_end, normalized_start + _initial_view_span_ms(context))

        if normalized_end <= 0:
            initial_reader = getattr(provider, "initial_window_end_ms", None)
            initial_end_ms = (
                initial_reader(context, initial_span_ms=_initial_view_span_ms(context))
                if callable(initial_reader)
                else None
            )
            if initial_end_ms is not None and initial_end_ms > 0:
                normalized_end = int(initial_end_ms)

        if normalized_end <= 0:
            latest_reader = getattr(provider, "latest_available_end_ms", None)
            latest_available_end_ms = (
                latest_reader(context)
                if callable(latest_reader)
                else None
            )
            if latest_available_end_ms is not None and latest_available_end_ms > 0:
                normalized_end = int(latest_available_end_ms)
            else:
                return None

        if normalized_start <= 0 or normalized_start >= normalized_end:
            normalized_start = max(0, normalized_end - _initial_view_span_ms(context))

        navigation_start_ms, navigation_end_ms = (
            date_bounds if date_bounds is not None else _day_bounds_for_ms(normalized_start)
        )
        normalized_start = max(navigation_start_ms, normalized_start)
        normalized_end = min(navigation_end_ms, max(normalized_start + 1, normalized_end))
        return DomWindowPlan(
            start_ms=int(normalized_start),
            end_ms=int(normalized_end),
            load_start_ms=max(
                int(navigation_start_ms),
                int(normalized_start) - int(context.retention_ms),
            ),
            navigation_start_ms=int(navigation_start_ms),
            navigation_end_ms=int(navigation_end_ms),
            selected_date=_date_label_for_ms(navigation_start_ms),
        )


class DomWindowPayloadCacheEngine:
    def __init__(self, max_size: int) -> None:
        self.max_size = max(1, int(max_size))
        self._cache: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()

    def get(self, key: tuple[Any, ...]) -> dict[str, Any] | None:
        cached = self._cache.get(key)
        if cached is None:
            return None
        self._cache.move_to_end(key)
        result = dict(cached)
        metrics = dict(result.get("viewport_metrics", {}))
        metrics["cache_hit_count"] = int(metrics.get("cache_hit_count", 0)) + 1
        result["viewport_metrics"] = metrics
        debug = dict(result.get("debug", {}))
        debug["cache_hit_count"] = metrics["cache_hit_count"]
        result["debug"] = debug
        return result

    def put(self, key: tuple[Any, ...], payload: dict[str, Any]) -> None:
        self._cache[key] = payload
        self._cache.move_to_end(key)
        while len(self._cache) > self.max_size:
            self._cache.popitem(last=False)


class DomTimelineEngine:
    def __init__(
        self,
        *,
        provider: DomDataProvider,
        config: DomEngineConfig,
    ) -> None:
        self.provider = provider
        self.config = config
        self.window_planner = DomWindowPlanningEngine()
        self.payload_cache = DomWindowPayloadCacheEngine(config.window_cache_size)

    def timeline_window(
        self,
        context: DomContext,
        *,
        start_time_ms: int | None = None,
        end_time_ms: int | None = None,
        price_min: Decimal | None = None,
        price_max: Decimal | None = None,
        selected_date: str | None = None,
        iceberg_min_contracts: int | None = None,
        iceberg_order_ids: tuple[str, ...] = (),
        iceberg_path_start_ms: int | None = None,
        iceberg_path_end_ms: int | None = None,
    ) -> dict[str, Any]:
        data_signature = self.provider.cache_signature(context)
        if not data_signature:
            return self._empty_payload(
                context,
                files=(),
                status="NO_DOM_FILES",
                message="No DOM files found",
            )

        plan = self.window_planner.resolve(
            provider=self.provider,
            context=context,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            selected_date=selected_date,
        )
        if plan is None:
            latest_result = self.provider.events_for_window(
                context,
                start_ms=0,
                end_ms=2**63 - 1,
            )
            if latest_result.latest_event_time_ms <= 0:
                return self._empty_payload(
                    context,
                    files=latest_result.files,
                    status=latest_result.status,
                    message=latest_result.message or "No DOM events found",
                    provider_result=latest_result,
                )
            fallback_end = latest_result.latest_event_time_ms
            plan = DomWindowPlan(
                start_ms=max(0, int(fallback_end) - int(context.retention_ms)),
                end_ms=int(fallback_end),
                load_start_ms=0,
                navigation_start_ms=max(0, int(fallback_end) - int(context.retention_ms)),
                navigation_end_ms=int(fallback_end),
                selected_date=_date_label_for_ms(fallback_end),
            )

        buffer_plan = self._render_buffer_plan(context, plan)
        provider_result = self._provider_result_for_plan(
            context,
            buffer_plan,
            primary_start_ms=plan.start_ms,
            primary_end_ms=plan.end_ms,
        )
        if start_time_ms is None and end_time_ms is None:
            snap_plan = self._snap_empty_window_to_next_event(
                context,
                plan=plan,
                provider_result=provider_result,
            )
            if snap_plan is not None:
                plan = snap_plan
                buffer_plan = self._render_buffer_plan(context, plan)
                provider_result = self._provider_result_for_plan(
                    context,
                    buffer_plan,
                    primary_start_ms=plan.start_ms,
                    primary_end_ms=plan.end_ms,
                )
        render_plan = self._render_plan_for_loaded_events(
            context,
            plan=plan,
            provider_result=provider_result,
            explicit_end_time=end_time_ms,
        )
        plan_start_ms = int(plan.start_ms)
        plan_end_ms = int(plan.end_ms)
        next_buffer_plan = self._render_buffer_plan(context, render_plan)
        if (
            next_buffer_plan.start_ms != buffer_plan.start_ms
            or next_buffer_plan.end_ms != buffer_plan.end_ms
        ):
            buffer_plan = next_buffer_plan
            provider_result = self._provider_result_for_plan(
                context,
                buffer_plan,
                primary_start_ms=render_plan.start_ms,
                primary_end_ms=render_plan.end_ms,
            )
        else:
            buffer_plan = next_buffer_plan
        cache_key = (
            context.provider_symbol.upper(),
            context.timeframe,
            int(render_plan.start_ms),
            int(render_plan.end_ms),
            int(buffer_plan.start_ms),
            int(buffer_plan.end_ms),
            str(price_min) if price_min is not None else "",
            str(price_max) if price_max is not None else "",
            render_plan.selected_date,
            int(iceberg_min_contracts or 0),
            ",".join(str(order_id).strip() for order_id in iceberg_order_ids if str(order_id).strip()),
            int(iceberg_path_start_ms or 0),
            int(iceberg_path_end_ms or 0),
            DOM_REFILL_POINT_MIN_COUNT,
            data_signature,
        )
        cached = self.payload_cache.get(cache_key)
        if cached is not None:
            return cached
        provider_result = self._filter_provider_result_to_primary_instrument(
            provider_result,
            start_ms=render_plan.start_ms,
            end_ms=render_plan.end_ms,
        )
        payload = self._build_payload(
            context,
            provider_result=provider_result,
            plan=render_plan,
            render_start_ms=buffer_plan.start_ms,
            render_end_ms=buffer_plan.end_ms,
            requested_start_ms=int(start_time_ms) if start_time_ms is not None else 0,
            requested_end_ms=int(end_time_ms) if end_time_ms is not None else 0,
            plan_start_ms=plan_start_ms,
            plan_end_ms=plan_end_ms,
            buffer_start_ms=int(buffer_plan.start_ms),
            buffer_end_ms=int(buffer_plan.end_ms),
            price_min=price_min,
            price_max=price_max,
            iceberg_min_contracts=iceberg_min_contracts,
            iceberg_order_ids=iceberg_order_ids,
            iceberg_path_start_ms=iceberg_path_start_ms,
            iceberg_path_end_ms=iceberg_path_end_ms,
        )
        self.payload_cache.put(cache_key, payload)
        return payload

    def _snap_empty_window_to_next_event(
        self,
        context: DomContext,
        *,
        plan: DomWindowPlan,
        provider_result: DomProviderResult,
    ) -> DomWindowPlan | None:
        if any(plan.start_ms <= int(event.ts_event_ms) <= plan.end_ms for event in provider_result.events):
            return None
        if plan.end_ms >= plan.navigation_end_ms:
            return None
        search_result = self.provider.events_for_window(
            context,
            start_ms=min(plan.navigation_end_ms, plan.end_ms + 1),
            end_ms=plan.navigation_end_ms,
        )
        if not search_result.events:
            span = max(1, int(plan.end_ms) - int(plan.start_ms))
            backward_window_ms = max(_initial_view_span_ms(context), span * 10)
            search_start = max(
                int(plan.navigation_start_ms),
                int(plan.start_ms) - int(backward_window_ms),
            )
            search_end = max(search_start, int(plan.start_ms) - 1)
            search_result = self.provider.events_for_window(
                context,
                start_ms=search_start,
                end_ms=search_end,
            )
            if not search_result.events:
                return None
            previous_event_ms = max(int(event.ts_event_ms) for event in search_result.events)
            next_start = max(plan.navigation_start_ms, previous_event_ms)
            next_end = min(plan.navigation_end_ms, next_start + span)
            if next_end <= next_start:
                return None
            return DomWindowPlan(
                start_ms=int(next_start),
                end_ms=int(next_end),
                load_start_ms=max(
                    int(plan.navigation_start_ms),
                    int(next_start) - int(context.retention_ms),
                ),
                navigation_start_ms=plan.navigation_start_ms,
                navigation_end_ms=plan.navigation_end_ms,
                selected_date=plan.selected_date,
            )
        next_event_ms = min(int(event.ts_event_ms) for event in search_result.events)
        span = max(1, int(plan.end_ms) - int(plan.start_ms))
        next_start = max(plan.navigation_start_ms, next_event_ms)
        next_end = min(plan.navigation_end_ms, next_start + span)
        if next_end <= next_start:
            return None
        return DomWindowPlan(
            start_ms=int(next_start),
            end_ms=int(next_end),
            load_start_ms=max(
                int(plan.navigation_start_ms),
                int(next_start) - int(context.retention_ms),
            ),
            navigation_start_ms=plan.navigation_start_ms,
            navigation_end_ms=plan.navigation_end_ms,
            selected_date=plan.selected_date,
        )

    def _provider_result_for_plan(
        self,
        context: DomContext,
        plan: DomWindowPlan,
        *,
        primary_start_ms: int | None = None,
        primary_end_ms: int | None = None,
    ) -> DomProviderResult:
        if plan.load_start_ms >= plan.start_ms:
            return self.provider.events_for_window(
                context,
                start_ms=plan.start_ms,
                end_ms=plan.end_ms,
                read_through=True,
                primary_start_ms=primary_start_ms,
                primary_end_ms=primary_end_ms,
            )

        state_result = self.provider.events_for_window(
            context,
            start_ms=plan.load_start_ms,
            end_ms=max(plan.load_start_ms, plan.start_ms - 1),
        )
        visible_result = self.provider.events_for_window(
            context,
            start_ms=plan.start_ms,
            end_ms=plan.end_ms,
            read_through=True,
            primary_start_ms=primary_start_ms,
            primary_end_ms=primary_end_ms,
        )
        order_context_events = self._visible_order_context_events(
            context,
            plan=plan,
            visible_events=visible_result.events,
        )
        merged_events_by_key = {
            _raw_event_merge_key(event): event
            for event in state_result.events + order_context_events + visible_result.events
        }
        events = tuple(
            sorted(
                merged_events_by_key.values(),
                key=lambda item: (int(item.ts_event_ms), int(item.sequence)),
            )
        )
        files = tuple(
            {
                (str(item.path), int(item.modified_ns), int(item.size_bytes)): item
                for item in state_result.files + visible_result.files
            }.values()
        )
        earliest_values = [
            value
            for value in (
                state_result.earliest_event_time_ms,
                visible_result.earliest_event_time_ms,
            )
            if value > 0
        ]
        latest_values = [
            value
            for value in (
                state_result.latest_event_time_ms,
                visible_result.latest_event_time_ms,
            )
            if value > 0
        ]
        return DomProviderResult(
            files=files,
            events=events,
            earliest_event_time_ms=min(earliest_values) if earliest_values else 0,
            latest_event_time_ms=max(latest_values) if latest_values else 0,
            contract_symbols=tuple(
                dict.fromkeys(state_result.contract_symbols + visible_result.contract_symbols)
            ),
            sampled=bool(state_result.sampled or visible_result.sampled),
            status=visible_result.status or state_result.status,
            message=visible_result.message or state_result.message,
        )

    def _visible_order_context_events(
        self,
        context: DomContext,
        *,
        plan: DomWindowPlan,
        visible_events: tuple[DomRawEvent, ...],
    ) -> tuple[DomRawEvent, ...]:
        if plan.load_start_ms >= plan.start_ms:
            return ()
        reader = getattr(self.provider, "order_events_for_window", None)
        if not callable(reader):
            return ()
        order_ids = tuple(
            dict.fromkeys(
                str(event.order_id).strip()
                for event in visible_events
                if str(event.order_id).strip()
            )
        )
        if not order_ids:
            return ()
        events: list[DomRawEvent] = []
        max_order_ids = 1_200
        chunk_size = 300
        limited_order_ids = order_ids[:max_order_ids]
        for index in range(0, len(limited_order_ids), chunk_size):
            chunk = limited_order_ids[index : index + chunk_size]
            try:
                events.extend(
                    reader(
                        context,
                        order_ids=chunk,
                        start_ms=plan.load_start_ms,
                        end_ms=max(plan.load_start_ms, plan.start_ms - 1),
                    )
                )
            except Exception:
                continue
        return tuple(events)

    def _filter_provider_result_to_primary_instrument(
        self,
        provider_result: DomProviderResult,
        *,
        start_ms: int,
        end_ms: int,
    ) -> DomProviderResult:
        primary_events = [
            event
            for event in provider_result.events
            if int(start_ms) <= int(event.ts_event_ms) <= int(end_ms)
            and int(event.instrument_id) > 0
        ]
        if not primary_events:
            return provider_result
        size_by_instrument: defaultdict[int, int] = defaultdict(int)
        for event in primary_events:
            size_by_instrument[int(event.instrument_id)] += max(1, int(event.size))
        active_instrument_id = max(size_by_instrument, key=size_by_instrument.get)
        filtered_events = tuple(
            event
            for event in provider_result.events
            if int(event.instrument_id) == int(active_instrument_id)
        )
        if len(filtered_events) == len(provider_result.events):
            return provider_result
        earliest = min((int(event.ts_event_ms) for event in filtered_events), default=0)
        latest = max((int(event.ts_event_ms) for event in filtered_events), default=0)
        return DomProviderResult(
            files=provider_result.files,
            events=filtered_events,
            earliest_event_time_ms=earliest,
            latest_event_time_ms=latest,
            contract_symbols=provider_result.contract_symbols,
            sampled=provider_result.sampled,
            status=provider_result.status,
            message=provider_result.message,
        )

    def _render_plan_for_loaded_events(
        self,
        context: DomContext,
        *,
        plan: DomWindowPlan,
        provider_result: DomProviderResult,
        explicit_end_time: int | None,
    ) -> DomWindowPlan:
        if explicit_end_time is not None and explicit_end_time > 0:
            return plan
        if not provider_result.events:
            return plan
        loaded_start = min(int(event.ts_event_ms) for event in provider_result.events)
        loaded_end = max(int(event.ts_event_ms) for event in provider_result.events)
        if loaded_end <= loaded_start:
            loaded_end = loaded_start + self._time_bucket_ms(context)
        if loaded_end >= plan.end_ms:
            return plan
        render_start = max(plan.start_ms, loaded_start)
        render_end = min(plan.end_ms, max(loaded_end, render_start + self._time_bucket_ms(context)))
        return DomWindowPlan(
            start_ms=int(render_start),
            end_ms=int(render_end),
            load_start_ms=plan.load_start_ms,
            navigation_start_ms=plan.navigation_start_ms,
            navigation_end_ms=plan.navigation_end_ms,
            selected_date=plan.selected_date,
        )

    def _render_buffer_plan(self, context: DomContext, plan: DomWindowPlan) -> DomWindowPlan:
        span = max(1, int(plan.end_ms) - int(plan.start_ms))
        multiplier = max(1, int(self.config.render_overscan_multiplier))
        max_padding = max(0, int(self.config.render_overscan_max_ms))
        padding = min(max_padding, span * max(0, multiplier - 1))
        start_ms = max(int(plan.navigation_start_ms), int(plan.start_ms) - padding)
        end_ms = min(int(plan.navigation_end_ms), int(plan.end_ms) + padding)
        return DomWindowPlan(
            start_ms=int(start_ms),
            end_ms=int(max(start_ms + 1, end_ms)),
            load_start_ms=max(
                int(plan.navigation_start_ms),
                int(start_ms) - int(context.retention_ms),
            ),
            navigation_start_ms=plan.navigation_start_ms,
            navigation_end_ms=plan.navigation_end_ms,
            selected_date=plan.selected_date,
        )

    def _build_payload(
        self,
        context: DomContext,
        *,
        provider_result: DomProviderResult,
        plan: DomWindowPlan,
        render_start_ms: int,
        render_end_ms: int,
        requested_start_ms: int,
        requested_end_ms: int,
        plan_start_ms: int,
        plan_end_ms: int,
        buffer_start_ms: int,
        buffer_end_ms: int,
        price_min: Decimal | None,
        price_max: Decimal | None,
        iceberg_min_contracts: int | None,
        iceberg_order_ids: tuple[str, ...],
        iceberg_path_start_ms: int | None,
        iceberg_path_end_ms: int | None,
    ) -> dict[str, Any]:
        orders: dict[str, DomOrderState] = {}
        bid_levels: defaultdict[Decimal, int] = defaultdict(int)
        ask_levels: defaultdict[Decimal, int] = defaultdict(int)
        book_bid_levels: dict[Decimal, int] = {}
        book_ask_levels: dict[Decimal, int] = {}
        book_orders: dict[str, DomOrderState] = {}
        book_activity_by_price: dict[Decimal, dict[str, Any]] = {}
        activity_stats: dict[tuple[Decimal, str, str, str], dict[str, Any]] = {}
        raw_execute_by_price: dict[Decimal, dict[str, int]] = {}
        visible_events: list[dict[str, Any]] = []
        resting_segments: list[dict[str, Any]] = []
        best_bid_line: list[dict[str, Any]] = []
        best_ask_line: list[dict[str, Any]] = []
        counts = {"add": 0, "cancel_delete": 0, "modify": 0, "execute": 0}
        synthetic_order_index = 0
        visible_start_ms = int(render_start_ms)
        visible_end_ms = int(render_end_ms)
        book_snapshot_seen = False
        iceberg_threshold = int(iceberg_min_contracts or 0)
        iceberg_active = iceberg_threshold > 0
        pinned_iceberg_order_ids = {
            str(order_id).strip()
            for order_id in iceberg_order_ids
            if str(order_id).strip()
        }
        requested_iceberg_path_start_ms = int(iceberg_path_start_ms or 0)
        requested_iceberg_path_end_ms = int(iceberg_path_end_ms or 0)
        iceberg_path_events_by_order: dict[str, list[dict[str, Any]]] = defaultdict(list)
        iceberg_refill_stats_by_order: dict[str, dict[str, int]] = defaultdict(
            lambda: {"positive_refill_count": 0, "positive_refill_total": 0}
        )
        refill_bucket_ms = self._time_bucket_ms(context)
        price_base_refill_states: dict[tuple[int, str, str], dict[str, Any]] = {}
        processed_event_ids: set[tuple[Any, ...]] = set()

        for raw_event in sorted(
            provider_result.events,
            key=lambda item: (
                int(item.ts_event_ms), int(item.sequence or 0), str(item.source_file or ""),
                str(item.order_id or ""), str(item.action or ""),
            ),
        ):
            event_identity = (
                str(raw_event.source_file or ""), int(raw_event.instrument_id or 0),
                int(raw_event.sequence or 0), int(raw_event.ts_event_ms),
                str(raw_event.order_id or ""), str(raw_event.action or "").strip().upper(),
            )
            
            if event_identity in processed_event_ids:
                continue
            processed_event_ids.add(event_identity)
            event_type = _event_type(raw_event.action)
            if event_type == "OTHER":
                continue
            if raw_event.price is None and event_type != "CANCEL_DELETE":
                continue
            if event_type == "CLEAR":
                self._close_all_segments(
                    orders,
                    resting_segments,
                    end_ms=raw_event.ts_event_ms,
                    visible_start_ms=visible_start_ms,
                    visible_end_ms=visible_end_ms,
                    price_min=price_min,
                    price_max=price_max,
                )
                orders.clear()
                bid_levels.clear()
                ask_levels.clear()
                if int(raw_event.ts_event_ms) <= int(plan.end_ms):
                    book_bid_levels = {}
                    book_ask_levels = {}
                    book_orders = {}
                    book_snapshot_seen = True
                continue

            key = raw_event.order_id
            if not key:
                synthetic_order_index += 1
                key = f"synthetic:{raw_event.side}:{raw_event.price}:{synthetic_order_index}"

            event_payload = self._apply_event(
                context,
                raw_event,
                event_type=event_type,
                order_key=key,
                orders=orders,
                bid_levels=bid_levels,
                ask_levels=ask_levels,
                resting_segments=resting_segments,
                visible_start_ms=visible_start_ms,
                visible_end_ms=visible_end_ms,
                price_min=price_min,
                price_max=price_max,
            )
            count_key = {
                "ADD": "add",
                "CANCEL_DELETE": "cancel_delete",
                "MODIFY": "modify",
                "EXECUTE": "execute",
            }.get(event_type)
            if count_key is not None:
                counts[count_key] += 1
            if iceberg_active and event_payload is not None:
                iceberg_path_events_by_order[key].append(event_payload)
                refill_count = int(event_payload.get("positive_refill_count", 0) or 0)
                refill_amount = int(event_payload.get("positive_refill_contracts", 0))
                if refill_count > 0:
                    iceberg_refill_stats_by_order[key]["positive_refill_count"] += refill_count
                    iceberg_refill_stats_by_order[key]["positive_refill_total"] += refill_amount
            if event_payload is not None:
                refill_count = max(0, int(event_payload.get("positive_refill_count", 0) or 0))
                refill_amount = int(event_payload.get("positive_refill_contracts", 0))
                refill_executed = max(0, int(event_payload.get("executed_refill_contracts", 0) or 0))
                refill_withdrawn = max(0, int(event_payload.get("withdrawn_refill_contracts", 0) or 0))
                if refill_count > 0 or refill_executed > 0 or refill_withdrawn > 0:
                    event_time_ms = int(event_payload.get("timestamp_ms", 0) or 0)
                    refill_price = str(event_payload.get("price", "") or "")
                    refill_side = str(event_payload.get("side", "") or "").strip().upper()
                    candle_open_time_ms = (event_time_ms // refill_bucket_ms) * refill_bucket_ms
                    price_key = (candle_open_time_ms, refill_price, refill_side)
                    point_state = price_base_refill_states.setdefault(
                        price_key,
                        {
                            "candle_open_time_ms": candle_open_time_ms,
                            "price": refill_price,
                            "side": refill_side,
                            "order_ids": set(),
                            "positive_refill_count": 0,
                            "positive_refill_total": 0,
                            "executed_refill_contracts": 0,
                            "withdrawn_refill_contracts": 0,
                            "threshold_time_ms": 0,
                            "last_refill_time_ms": 0,
                        },
                    )
                    point_state["order_ids"].add(key)
                    point_state["positive_refill_count"] = (
                        int(point_state["positive_refill_count"]) + refill_count
                    )
                    point_state["positive_refill_total"] = (
                        int(point_state["positive_refill_total"]) + refill_amount
                    )
                    point_state["executed_refill_contracts"] = min(
                        int(point_state["positive_refill_total"]),
                        int(point_state["executed_refill_contracts"]) + refill_executed,
                    )
                    point_state["withdrawn_refill_contracts"] = (
                        int(point_state["withdrawn_refill_contracts"]) + refill_withdrawn
                    )
                    if int(point_state["threshold_time_ms"]) <= 0 and (
                        int(point_state["positive_refill_count"]) >= DOM_REFILL_POINT_MIN_COUNT
                    ):
                        point_state["threshold_time_ms"] = event_time_ms
                    point_state["last_refill_time_ms"] = event_time_ms
            if visible_start_ms <= int(raw_event.ts_event_ms) <= visible_end_ms:
                self._append_best_lines(
                    best_bid_line,
                    best_ask_line,
                    timestamp_ms=raw_event.ts_event_ms,
                    bid_levels=bid_levels,
                    ask_levels=ask_levels,
                )
            if (
                event_payload is not None
                and visible_start_ms <= int(raw_event.ts_event_ms) <= visible_end_ms
                and _price_in_range(raw_event.price, price_min=price_min, price_max=price_max)
            ):
                visible_events.append(event_payload)
                self._record_book_activity(
                    activity_stats,
                    raw_execute_by_price,
                    price=raw_event.price,
                    order_id=key,
                    event_type=event_type,
                    side=event_payload.get("side", ""),
                    raw_size=int(raw_event.size),
                    timestamp_ms=int(raw_event.ts_event_ms),
                    order_current_contracts=int(event_payload.get("order_current_contracts", 0)),
                    positive_refill_count=int(event_payload.get("positive_refill_count", 0) or 0),
                    positive_refill_contracts=int(event_payload.get("positive_refill_contracts", 0)),
                )
            if int(raw_event.ts_event_ms) <= int(plan.end_ms):
                book_bid_levels = dict(bid_levels)
                book_ask_levels = dict(ask_levels)
                book_orders = dict(orders)
                book_snapshot_seen = True

        if not best_bid_line and bid_levels:
            best_bid_line.append(
                {"timestamp_ms": int(visible_start_ms), "price": _format_decimal(max(bid_levels))}
            )
        if not best_ask_line and ask_levels:
            best_ask_line.append(
                {"timestamp_ms": int(visible_start_ms), "price": _format_decimal(min(ask_levels))}
            )
        self._close_all_segments(
            orders,
            resting_segments,
            end_ms=visible_end_ms,
            visible_start_ms=visible_start_ms,
            visible_end_ms=visible_end_ms,
            price_min=price_min,
            price_max=price_max,
        )
        if not book_snapshot_seen:
            book_bid_levels = dict(bid_levels)
            book_ask_levels = dict(ask_levels)
            book_orders = dict(orders)
        book_activity_by_price = self._top_book_activity_by_price(activity_stats, raw_execute_by_price)
        iceberg_order_ids: list[str] = []
        iceberg_order_summaries: list[dict[str, Any]] = []
        iceberg_path_start_ms = 0
        iceberg_path_end_ms = 0
        if iceberg_active:
            selected_order_ids = set(pinned_iceberg_order_ids)
            selected_order_ids.update({
                order_id
                for order_id, refill_stats in iceberg_refill_stats_by_order.items()
                if int(refill_stats.get("positive_refill_total", 0)) >= iceberg_threshold
            })
            iceberg_order_ids = sorted(selected_order_ids)
            if selected_order_ids:
                selected_events = [
                    event
                    for order_id in iceberg_order_ids
                    for event in iceberg_path_events_by_order.get(order_id, [])
                ]
                selected_events.sort(
                    key=lambda item: (
                        int(item.get("timestamp_ms", 0)),
                        str(item.get("event_id", "")),
                    )
                )
                path_reader = getattr(self.provider, "order_events_for_window", None)
                if callable(path_reader):
                    path_end_ms = max(
                        visible_end_ms,
                        requested_iceberg_path_end_ms,
                        max((int(event.get("timestamp_ms", 0)) for event in selected_events), default=0),
                    )
                    path_start_ms = (
                        requested_iceberg_path_start_ms
                        if requested_iceberg_path_start_ms > 0 and requested_iceberg_path_start_ms < path_end_ms
                        else int(plan.navigation_start_ms)
                    )
                    try:
                        raw_path_events = path_reader(
                            context,
                            order_ids=tuple(iceberg_order_ids),
                            start_ms=int(path_start_ms),
                            end_ms=int(path_end_ms),
                        )
                    except Exception:
                        raw_path_events = ()
                    path_payloads = self._order_path_payloads_from_raw_events(
                        context,
                        [
                            event
                            for event in raw_path_events
                            if str(event.order_id or "").strip() in selected_order_ids
                        ],
                    )
                    if path_payloads:
                        selected_events = path_payloads
                selected_refill_stats: dict[str, dict[str, int]] = defaultdict(
                    lambda: {"positive_refill_count": 0, "positive_refill_total": 0}
                )
                for event in selected_events:
                    order_id = str(event.get("order_id") or event.get("venue_order_id") or "").strip()
                    if not order_id:
                        continue
                    refill_count = int(event.get("positive_refill_count", 0) or 0)
                    refill_amount = int(event.get("positive_refill_contracts", 0))
                    if refill_count > 0:
                        selected_refill_stats[order_id]["positive_refill_count"] += refill_count
                        selected_refill_stats[order_id]["positive_refill_total"] += refill_amount
                final_selected_order_ids = {
                    order_id
                    for order_id in selected_order_ids
                    if (
                        order_id in pinned_iceberg_order_ids
                        or int(selected_refill_stats.get(order_id, {}).get("positive_refill_total", 0))
                        >= iceberg_threshold
                    )
                }
                if final_selected_order_ids != selected_order_ids:
                    selected_order_ids = final_selected_order_ids
                    iceberg_order_ids = sorted(selected_order_ids)
                    selected_events = [
                        event
                        for event in selected_events
                        if str(event.get("order_id") or event.get("venue_order_id") or "").strip()
                        in selected_order_ids
                    ]
                iceberg_order_summaries = [
                    {
                        "order_id": order_id,
                        "positive_refill_count": int(
                            selected_refill_stats.get(order_id, {}).get("positive_refill_count", 0)
                        ),
                        "positive_refill_total": int(
                            selected_refill_stats.get(order_id, {}).get("positive_refill_total", 0)
                        ),
                    }
                    for order_id in iceberg_order_ids
                ]
                visible_events = selected_events
                resting_segments = [
                    segment
                    for segment in resting_segments
                    if str(segment.get("order_id", "")) in selected_order_ids
                ]
                iceberg_path_start_ms = min(
                    (int(event.get("timestamp_ms", 0)) for event in selected_events),
                    default=0,
                )
                iceberg_path_end_ms = max(
                    (int(event.get("timestamp_ms", 0)) for event in selected_events),
                    default=0,
                )
            else:
                visible_events = []
                resting_segments = []

        dom_refill_points = [
            {
                "price": str(state["price"]),
                "side": str(state["side"]),
                "positive_refill_count": int(state["positive_refill_count"]),
                "positive_refill_total": int(state["positive_refill_total"]),
                "price_base_refill_count": int(state["positive_refill_count"]),
                "price_base_refill_contracts": int(state["positive_refill_total"]),
                "refill_added_contracts": int(state["positive_refill_total"]),
                "executed_refill_contracts": int(state["executed_refill_contracts"]),
                "withdrawn_refill_contracts": int(state["withdrawn_refill_contracts"]),
                "refill_execution_rate": round(
                    int(state["executed_refill_contracts"])
                    / int(state["positive_refill_total"]) * 100.0
                    if int(state["positive_refill_total"]) > 0 else 0.0,
                    1,
                ),
                "event_time_ms": int(state["threshold_time_ms"]),
                "timestamp_ms": int(state["threshold_time_ms"]),
                "candle_open_time_ms": int(state["candle_open_time_ms"]),
                "symbol": context.provider_symbol,
                "provider_symbol": context.provider_symbol,
                "mt5_symbol": context.mt5_symbol,
                "order_id": "",
                "order_ids": tuple(sorted(state["order_ids"])),
                "order_count": len(state["order_ids"]),
                "refill_method": "price_base_refill",
                "span_candles": DOM_REFILL_POINT_CANDLE_SPAN,
                "source": "PRICE_BASE_REFILL",
            }
            for state in price_base_refill_states.values()
            if (
                int(state["positive_refill_count"]) >= DOM_REFILL_POINT_MIN_COUNT
                and int(state["threshold_time_ms"]) > 0
                and int(plan.start_ms) <= int(state["threshold_time_ms"]) <= int(plan.end_ms)
                and str(state["price"])
                and str(state["side"])
            )
        ]

        dom_positive_refill_outputs = _dom_positive_refill_outputs(
            dom_refill_points,
            context=context,
        )
        raw_visible_events = list(visible_events)
        total_visible_events = len(visible_events)
        total_resting_segments = len(resting_segments)
        visible_events = _sample_points_with_primary_window(
            visible_events,
            self.config.max_events_per_window,
            primary_start_ms=plan.start_ms,
            primary_end_ms=plan.end_ms,
            start_key="timestamp_ms",
        )
        resting_segments = _sample_points_with_primary_window(
            resting_segments,
            self.config.max_resting_segments_per_window,
            primary_start_ms=plan.start_ms,
            primary_end_ms=plan.end_ms,
            start_key="start_ms",
            end_key="end_ms",
        )
        
        
        quote_bid_line, quote_ask_line = self._quote_lines_from_events(
            provider_result.events,
            context=context,
            start_ms=visible_start_ms,
            end_ms=visible_end_ms,
        )
        if quote_bid_line:
            best_bid_line = quote_bid_line
        if quote_ask_line:
            best_ask_line = quote_ask_line
        best_bid_line = _sample_points(
            _line_with_window_edges(
                best_bid_line,
                start_ms=visible_start_ms,
                end_ms=visible_end_ms,
            ),
            self.config.max_line_points_per_window,
        )
        best_ask_line = _sample_points(
            _line_with_window_edges(
                best_ask_line,
                start_ms=visible_start_ms,
                end_ms=visible_end_ms,
            ),
            self.config.max_line_points_per_window,
        )
        order_book_levels = self._order_book_levels(
            defaultdict(int, book_bid_levels),
            defaultdict(int, book_ask_levels),
            orders=book_orders,
            activity_by_price=book_activity_by_price,
            price_min=price_min,
            price_max=price_max,
        )
        if len(order_book_levels) > self.config.max_price_levels:
            order_book_levels = order_book_levels[: self.config.max_price_levels]
        for item in visible_events + resting_segments:
            item.setdefault("symbol", context.provider_symbol)
            item.setdefault("provider_symbol", context.provider_symbol)
            item.setdefault("mt5_symbol", context.mt5_symbol)
        price_levels_count = len(
            {
                str(item.get("price", ""))
                for item in visible_events + resting_segments + order_book_levels
                if item.get("price") not in {None, ""}
            }
        )
        time_bucket_ms = self._time_bucket_ms(context)
        time_bucket_count = len(
            {
                int(event["timestamp_ms"]) // time_bucket_ms
                for event in visible_events
                if int(event.get("timestamp_ms", 0)) > 0
            }
        )
        global_navigation_start_ms, global_navigation_end_ms = self._global_navigation_bounds(
            context,
            plan=plan,
        )
        available_dates = self._available_dates(context)
        last_bid = best_bid_line[-1]["price"] if best_bid_line else ""
        last_ask = best_ask_line[-1]["price"] if best_ask_line else ""
        debug = {
            "requested_start_ms": int(requested_start_ms),
            "requested_end_ms": int(requested_end_ms),
            "plan_start_ms": int(plan_start_ms),
            "plan_end_ms": int(plan_end_ms),
            "render_start_ms": int(plan.start_ms),
            "render_end_ms": int(plan.end_ms),
            "buffer_start_ms": int(buffer_start_ms),
            "buffer_end_ms": int(buffer_end_ms),
            "cache_hit_count": 0,
            "provider_event_count": len(provider_result.events),
            "visible_event_count": total_visible_events,
            "dom_file_count": len(provider_result.files),
            "mbo_event_count": len(provider_result.events),
            "symbol": context.provider_symbol,
            "timeframe": context.timeframe,
            "tick_size": str(context.tick_size),
            "price_level_count": price_levels_count,
            "time_bucket_count": time_bucket_count,
            "add_count": counts["add"],
            "cancel_delete_count": counts["cancel_delete"],
            "modify_count": counts["modify"],
            "execute_count": counts["execute"],
            "last_bid": last_bid,
            "last_ask": last_ask,
            "rendered_event_count": len(visible_events),
            "total_visible_event_count": total_visible_events,
            "rendered_resting_segment_count": len(resting_segments),
            "total_resting_segment_count": total_resting_segments,
            "iceberg_filter_threshold": iceberg_threshold,
            "iceberg_filter_order_count": len(iceberg_order_ids),
        }
        payload = {
            "type": "DOM_TIMELINE_SESSION",
            "mt5_symbol": context.mt5_symbol,
            "symbol": context.provider_symbol,
            "provider_symbol": context.provider_symbol,
            "market_provider": context.market_provider,
            "dataset": context.dataset,
            "schema": "mbo",
            "timeframe": context.timeframe,
            "interval": context.interval,
            "timezone": context.timezone,
            "session_start_hour_chicago": context.session_start_hour_chicago,
            "price_step": str(context.tick_size),
            "tick_size": str(context.tick_size),
            "quantity_unit": "CONTRACTS",
            "trigger_timeout_candles": context.trigger_timeout_candles,
            "initial_view_candles": context.initial_view_candles,
            "retention_ms": context.retention_ms,
            "data_dir": str(context.data_dir),
            "dom_files": [item.to_payload() for item in provider_result.files],
            "dom_file_count": len(provider_result.files),
            "contract_symbols": list(provider_result.contract_symbols),
            "viewport_window": True,
            "window_start_ms": int(plan.start_ms),
            "window_end_ms": int(plan.end_ms),
            "render_start_ms": int(visible_start_ms),
            "render_end_ms": int(visible_end_ms),
            "navigation_start_ms": int(plan.navigation_start_ms),
            "navigation_end_ms": int(plan.navigation_end_ms),
            "global_navigation_start_ms": int(global_navigation_start_ms),
            "global_navigation_end_ms": int(global_navigation_end_ms),
            "available_dates": list(available_dates),
            "selected_date": plan.selected_date,
            "earliest_window_start_ms": int(provider_result.earliest_event_time_ms),
            "latest_window_end_ms": int(provider_result.latest_event_time_ms),
            "has_older_data": (
                provider_result.earliest_event_time_ms > 0
                and int(plan.start_ms) > int(provider_result.earliest_event_time_ms)
            ),
            "status": provider_result.status,
            "message": provider_result.message,
            "events": visible_events,
            "raw_events": raw_visible_events,
            "dom_refill_points": dom_refill_points,
            "engine_outputs": {
                "dom_positive_refills": dom_positive_refill_outputs,
            },
            "iceberg_filter": {
                "active": bool(iceberg_active),
                "threshold": int(iceberg_threshold),
                "metric": "positive_refill_total",
                "order_ids": iceberg_order_ids,
                "orders": iceberg_order_summaries,
                "path_start_ms": int(iceberg_path_start_ms),
                "path_end_ms": int(iceberg_path_end_ms),
            },
            "resting_segments": resting_segments,
            "best_bid_line": best_bid_line,
            "best_ask_line": best_ask_line,
            "order_book_levels": order_book_levels,
            "time_bucket_ms": time_bucket_ms,
            "viewport_metrics": {
                "cache_hit_count": 0,
                "cache_miss_count": 1,
                "mbo_event_count": len(provider_result.events),
            },
            "debug": debug,
        }
        if (
            not visible_events
            and not resting_segments
            and not best_bid_line
            and not best_ask_line
            and provider_result.status == "READY"
        ):
            payload["message"] = "No DOM events found"
        return payload

    def _global_navigation_bounds(
        self,
        context: DomContext,
        *,
        plan: DomWindowPlan,
    ) -> tuple[int, int]:
        bounds_reader = getattr(self.provider, "available_time_bounds_ms", None)
        if callable(bounds_reader):
            bounds = bounds_reader(context)
            if bounds is not None:
                start_ms, end_ms = bounds
                if int(end_ms) > int(start_ms):
                    return int(start_ms), int(end_ms)
        return int(plan.navigation_start_ms), int(plan.navigation_end_ms)

    def _available_dates(self, context: DomContext) -> tuple[str, ...]:
        dates_reader = getattr(self.provider, "available_dates", None)
        if not callable(dates_reader):
            return ()
        return tuple(str(item) for item in dates_reader(context) if str(item).strip())

    def _order_path_payloads_from_raw_events(
        self,
        context: DomContext,
        raw_events: tuple[DomRawEvent, ...] | list[DomRawEvent],
    ) -> list[dict[str, Any]]:
        orders: dict[str, DomOrderState] = {}
        bid_levels: defaultdict[Decimal, int] = defaultdict(int)
        ask_levels: defaultdict[Decimal, int] = defaultdict(int)
        resting_segments: list[dict[str, Any]] = []
        payloads: list[dict[str, Any]] = []
        for raw_event in sorted(raw_events, key=lambda item: (int(item.ts_event_ms), int(item.sequence))):
            event_type = _event_type(raw_event.action)
            if event_type in {"OTHER", "CLEAR"}:
                continue
            order_key = str(raw_event.order_id or "").strip()
            if not order_key:
                continue
            event_payload = self._apply_event(
                context,
                raw_event,
                event_type=event_type,
                order_key=order_key,
                orders=orders,
                bid_levels=bid_levels,
                ask_levels=ask_levels,
                resting_segments=resting_segments,
                visible_start_ms=0,
                visible_end_ms=2**63 - 1,
                price_min=None,
                price_max=None,
            )
            if event_payload is not None:
                payloads.append(event_payload)
        return payloads

    @staticmethod
    def _consume_order_liquidity(
        order: DomOrderState,
        quantity: int,
        *,
        consume_refill: bool,
    ) -> tuple[int, int]:
        remaining = max(0, int(quantity))
        existing = min(remaining, max(0, int(order.existing_qty)))
        order.existing_qty -= existing
        remaining -= existing
        refill = 0
        if consume_refill:
            for lot in order.refill_lots:
                if remaining <= 0:
                    break
                amount = min(remaining, max(0, int(lot.remaining_qty)))
                lot.remaining_qty -= amount
                remaining -= amount
                refill += amount
            order.refill_lots = [lot for lot in order.refill_lots if lot.remaining_qty > 0]
        return existing, refill

    def _apply_event(
        self,
        context: DomContext,
        raw_event: DomRawEvent,
        *,
        event_type: str,
        order_key: str,
        orders: dict[str, DomOrderState],
        bid_levels: defaultdict[Decimal, int],
        ask_levels: defaultdict[Decimal, int],
        resting_segments: list[dict[str, Any]],
        visible_start_ms: int,
        visible_end_ms: int,
        price_min: Decimal | None,
        price_max: Decimal | None,
    ) -> dict[str, Any] | None:
        added = 0
        canceled = 0
        # Raw executed quantity reported by the MBO event.
        executed = 0

        # Quantity that can safely be removed from the reconstructed book state.
        book_executed = 0
        modified_delta = 0
        positive_refill_count = 0
        positive_refill_contracts = 0
        executed_refill_contracts = 0
        withdrawn_refill_contracts = 0
        previous = orders.get(order_key)
        price = raw_event.price or (previous.price if previous is not None else None)
        side = raw_event.side if raw_event.side != "NONE" else (previous.side if previous is not None else "NONE")
        if price is None or side not in {"BID", "ASK"}:
            return None
        before_resting_bid = int(bid_levels.get(price, 0))
        before_resting_ask = int(ask_levels.get(price, 0))
        before_best_bid = max(bid_levels) if bid_levels else None
        before_best_ask = min(ask_levels) if ask_levels else None
        previous_contracts = int(previous.size) if previous is not None else 0
        previous_pending_refill = int(previous.pending_refill_contracts) if previous is not None else 0

        if event_type == "ADD":
            added = max(0, raw_event.size)
            if previous is not None:
                self._close_segment(
                    previous,
                    resting_segments,
                    end_ms=raw_event.ts_event_ms,
                    visible_start_ms=visible_start_ms,
                    visible_end_ms=visible_end_ms,
                    price_min=price_min,
                    price_max=price_max,
                )
                self._adjust_level(
                    bid_levels,
                    ask_levels,
                    side=previous.side,
                    price=previous.price,
                    delta=-previous.size,
                )
            orders[order_key] = DomOrderState(
                order_id=order_key,
                side=side,
                price=price,
                size=added,
                started_at_ms=raw_event.ts_event_ms,
                updated_at_ms=raw_event.ts_event_ms,
                existing_qty=added,
            )
            self._adjust_level(bid_levels, ask_levels, side=side, price=price, delta=added)
        elif event_type == "MODIFY":
            if previous is None:
                added = max(0, raw_event.size)
                orders[order_key] = DomOrderState(
                    order_id=order_key,
                    side=side,
                    price=price,
                    size=added,
                    started_at_ms=raw_event.ts_event_ms,
                    updated_at_ms=raw_event.ts_event_ms,
                    existing_qty=added,
                )
                self._adjust_level(bid_levels, ask_levels, side=side, price=price, delta=added)
                modified_delta = added
            else:
                new_size = max(0, raw_event.size)
                modified_delta = new_size - previous.size
                price_or_side_changed = price != previous.price or side != previous.side
                if price_or_side_changed:
                    _, withdrawn_refill_contracts = self._consume_order_liquidity(
                        previous, previous.size, consume_refill=True
                    )
                    previous.existing_qty = new_size
                    previous.refill_lots = []
                    previous.pending_refill_contracts = 0
                replenished_contracts = (
                    max(0, int(new_size) - int(previous.size))
                    if not price_or_side_changed
                    and (int(previous.pending_refill_contracts) > 0 or bool(previous.refill_lots))
                    else 0
                )
                if replenished_contracts > 0:
                    positive_refill_count = 1
                    positive_refill_contracts = replenished_contracts
                pending_refill_contracts = max(
                    0,
                    0
                    if replenished_contracts > 0
                    else int(previous.pending_refill_contracts),
                )
                if not price_or_side_changed and modified_delta < 0:
                    _, withdrawn_refill_contracts = self._consume_order_liquidity(
                        previous, -modified_delta, consume_refill=True
                    )
                elif not price_or_side_changed and replenished_contracts > 0:
                    previous.refill_lots.append(
                        DomRefillLot(
                            candle_open_time_ms=(raw_event.ts_event_ms // self._time_bucket_ms(context))
                            * self._time_bucket_ms(context),
                            price=price,
                            side=side,
                            remaining_qty=replenished_contracts,
                        )
                    )
                elif not price_or_side_changed and modified_delta > 0:
                    previous.existing_qty += modified_delta
                self._close_segment(
                    previous,
                    resting_segments,
                    end_ms=raw_event.ts_event_ms,
                    visible_start_ms=visible_start_ms,
                    visible_end_ms=visible_end_ms,
                    price_min=price_min,
                    price_max=price_max,
                )
                self._adjust_level(
                    bid_levels,
                    ask_levels,
                    side=previous.side,
                    price=previous.price,
                    delta=-previous.size,
                )
                if new_size > 0:
                    orders[order_key] = DomOrderState(
                        order_id=order_key,
                        side=side,
                        price=price,
                        size=new_size,
                        started_at_ms=raw_event.ts_event_ms,
                        updated_at_ms=raw_event.ts_event_ms,
                        pending_refill_contracts=pending_refill_contracts,
                        existing_qty=previous.existing_qty,
                        refill_lots=previous.refill_lots,
                    )
                    self._adjust_level(bid_levels, ask_levels, side=side, price=price, delta=new_size)
                else:
                    if pending_refill_contracts > 0:
                        orders[order_key] = DomOrderState(
                            order_id=order_key,
                            side=side,
                            price=price,
                            size=0,
                            started_at_ms=raw_event.ts_event_ms,
                            updated_at_ms=raw_event.ts_event_ms,
                            pending_refill_contracts=pending_refill_contracts,
                            existing_qty=previous.existing_qty,
                            refill_lots=previous.refill_lots,
                        )
                    else:
                        orders.pop(order_key, None)
        elif event_type == "EXECUTE":
            # The raw MBO Fill quantity is authoritative for execution statistics.
            executed = max(
                0,
                int(raw_event.size or 0),
            )

            if previous is not None:
                # Only book-state reduction is limited by the reconstructed
                # remaining order size. Do not cap the raw executed quantity.
                book_executed = min(
                    executed,
                    max(0, int(previous.size)),
                )

                remaining = max(
                    0,
                    int(previous.size) - book_executed,
                )

                # Refill opportunity is created by the complete raw Fill,
                # not merely by the portion matched to reconstructed state.
                pending_refill_contracts = max(
                    0,
                    int(previous.pending_refill_contracts) + executed,
                )

                _, executed_refill_contracts = (
                    self._consume_order_liquidity(
                        previous,
                        book_executed,
                        consume_refill=True,
                    )
                )

                self._close_segment(
                    previous,
                    resting_segments,
                    end_ms=raw_event.ts_event_ms,
                    visible_start_ms=visible_start_ms,
                    visible_end_ms=visible_end_ms,
                    price_min=price_min,
                    price_max=price_max,
                )

                # Remove only the known quantity from the reconstructed book.
                self._adjust_level(
                    bid_levels,
                    ask_levels,
                    side=previous.side,
                    price=previous.price,
                    delta=-book_executed,
                )

                if remaining > 0:
                    orders[order_key] = DomOrderState(
                        order_id=order_key,
                        side=previous.side,
                        price=previous.price,
                        size=remaining,
                        started_at_ms=raw_event.ts_event_ms,
                        updated_at_ms=raw_event.ts_event_ms,
                        pending_refill_contracts=pending_refill_contracts,
                        existing_qty=previous.existing_qty,
                        refill_lots=previous.refill_lots,
                    )
                elif pending_refill_contracts > 0:
                    orders[order_key] = DomOrderState(
                        order_id=order_key,
                        side=previous.side,
                        price=previous.price,
                        size=0,
                        started_at_ms=raw_event.ts_event_ms,
                        updated_at_ms=raw_event.ts_event_ms,
                        pending_refill_contracts=pending_refill_contracts,
                        existing_qty=previous.existing_qty,
                        refill_lots=previous.refill_lots,
                    )
                else:
                    orders.pop(order_key, None)
        elif event_type == "CANCEL_DELETE":
            canceled = max(0, raw_event.size)
            if previous is not None:
                canceled = min(canceled or previous.size, previous.size)
                remaining = max(0, previous.size - canceled)
                _, withdrawn_refill_contracts = self._consume_order_liquidity(
                    previous, canceled, consume_refill=True
                )
                self._close_segment(
                    previous,
                    resting_segments,
                    end_ms=raw_event.ts_event_ms,
                    visible_start_ms=visible_start_ms,
                    visible_end_ms=visible_end_ms,
                    price_min=price_min,
                    price_max=price_max,
                )
                self._adjust_level(
                    bid_levels,
                    ask_levels,
                    side=previous.side,
                    price=previous.price,
                    delta=-canceled,
                )
                if remaining > 0 and raw_event.action.upper() == "C":
                    orders[order_key] = DomOrderState(
                        order_id=order_key,
                        side=previous.side,
                        price=previous.price,
                        size=remaining,
                        started_at_ms=raw_event.ts_event_ms,
                        updated_at_ms=raw_event.ts_event_ms,
                        pending_refill_contracts=previous.pending_refill_contracts,
                        existing_qty=previous.existing_qty,
                        refill_lots=previous.refill_lots,
                    )
                else:
                    orders.pop(order_key, None)

        resting_bid = int(bid_levels.get(price, 0))
        resting_ask = int(ask_levels.get(price, 0))
        net_liquidity_change = int(
            added
            - canceled
            - book_executed
            + modified_delta
        )
        current_order = orders.get(order_key)
        after_best_bid = max(bid_levels) if bid_levels else None
        after_best_ask = min(ask_levels) if ask_levels else None
        return {
            "event_id": _dom_event_id(context, raw_event, order_key),
            "timestamp_ms": int(raw_event.ts_event_ms),
            "symbol": context.provider_symbol,
            "provider_symbol": context.provider_symbol,
            "mt5_symbol": context.mt5_symbol,
            "price": _format_decimal(price),
            "side": side,
            "event_type": event_type,
            "action": raw_event.action,
            "order_id": order_key,
            "venue_order_id": raw_event.order_id,
            "order_size": int(raw_event.size),
            "raw_event_size": int(raw_event.size),
            "order_previous_contracts": previous_contracts,
            "order_current_contracts": int(current_order.size) if current_order is not None else 0,
            "order_previous_pending_refill_contracts": previous_pending_refill,
            "order_pending_refill_contracts": (
                int(current_order.pending_refill_contracts) if current_order is not None else 0
            ),
            "before_resting_bid_contracts": before_resting_bid,
            "before_resting_ask_contracts": before_resting_ask,
            "resting_bid_contracts": resting_bid,
            "resting_ask_contracts": resting_ask,
            "before_best_bid": _format_decimal(before_best_bid) if before_best_bid is not None else "",
            "before_best_ask": _format_decimal(before_best_ask) if before_best_ask is not None else "",
            "after_best_bid": _format_decimal(after_best_bid) if after_best_bid is not None else "",
            "after_best_ask": _format_decimal(after_best_ask) if after_best_ask is not None else "",
            "added_contracts": int(added),
            "canceled_contracts": int(canceled),
            "executed_contracts": int(executed),
            "modified_delta": int(modified_delta),
            "positive_refill_count": int(positive_refill_count),
            "positive_refill_contracts": int(positive_refill_contracts),
            "executed_refill_contracts": int(executed_refill_contracts),
            "withdrawn_refill_contracts": int(withdrawn_refill_contracts),
            "net_liquidity_change": net_liquidity_change,
        }

    @staticmethod
    def _adjust_level(
        bid_levels: defaultdict[Decimal, int],
        ask_levels: defaultdict[Decimal, int],
        *,
        side: str,
        price: Decimal,
        delta: int,
    ) -> None:
        levels = bid_levels if side == "BID" else ask_levels
        next_value = int(levels.get(price, 0)) + int(delta)
        if next_value > 0:
            levels[price] = next_value
        else:
            levels.pop(price, None)

    def _close_all_segments(
        self,
        orders: dict[str, DomOrderState],
        resting_segments: list[dict[str, Any]],
        *,
        end_ms: int,
        visible_start_ms: int,
        visible_end_ms: int,
        price_min: Decimal | None,
        price_max: Decimal | None,
    ) -> None:
        for order in list(orders.values()):
            self._close_segment(
                order,
                resting_segments,
                end_ms=end_ms,
                visible_start_ms=visible_start_ms,
                visible_end_ms=visible_end_ms,
                price_min=price_min,
                price_max=price_max,
            )

    @staticmethod
    def _close_segment(
        order: DomOrderState,
        resting_segments: list[dict[str, Any]],
        *,
        end_ms: int,
        visible_start_ms: int,
        visible_end_ms: int,
        price_min: Decimal | None,
        price_max: Decimal | None,
    ) -> None:
        if int(order.size) <= 0:
            return
        if end_ms <= order.started_at_ms:
            return
        if end_ms < visible_start_ms or order.started_at_ms > visible_end_ms:
            return
        if not _price_in_range(order.price, price_min=price_min, price_max=price_max):
            return
        resting_segments.append(
            {
                "start_ms": max(int(order.started_at_ms), int(visible_start_ms)),
                "end_ms": min(int(end_ms), int(visible_end_ms)),
                "price": _format_decimal(order.price),
                "side": order.side,
                "order_id": order.order_id,
                "order_size": int(order.size),
                "event_type": "RESTING_LIQUIDITY",
            }
        )

    @staticmethod
    def _append_best_lines(
        best_bid_line: list[dict[str, Any]],
        best_ask_line: list[dict[str, Any]],
        *,
        timestamp_ms: int,
        bid_levels: defaultdict[Decimal, int],
        ask_levels: defaultdict[Decimal, int],
    ) -> None:
        if bid_levels:
            best_bid = max(bid_levels)
            if not best_bid_line or best_bid_line[-1]["price"] != _format_decimal(best_bid):
                best_bid_line.append({"timestamp_ms": int(timestamp_ms), "price": _format_decimal(best_bid)})
        if ask_levels:
            best_ask = min(ask_levels)
            if not best_ask_line or best_ask_line[-1]["price"] != _format_decimal(best_ask):
                best_ask_line.append({"timestamp_ms": int(timestamp_ms), "price": _format_decimal(best_ask)})

    @staticmethod
    def _record_book_activity(
        activity_stats: dict[tuple[Decimal, str, str, str], dict[str, Any]],
        raw_execute_by_price: dict[Decimal, dict[str, int]],
        *,
        price: Decimal | None,
        order_id: str,
        event_type: str,
        side: str,
        raw_size: int,
        timestamp_ms: int,
        order_current_contracts: int,
        positive_refill_count: int,
        positive_refill_contracts: int,
    ) -> None:
        if price is None:
            return
        amount = max(0, int(raw_size))
        if amount <= 0:
            return
        normalized_type = str(event_type or "").upper()
        normalized_side = str(side or "").upper()
        if normalized_type == "EXECUTE":
            execute_totals = raw_execute_by_price.setdefault(price, {"buy": 0, "sell": 0})
            if normalized_side == "ASK":
                execute_totals["buy"] += amount
            elif normalized_side == "BID":
                execute_totals["sell"] += amount
        key = (price, str(order_id or ""), normalized_type, normalized_side)
        stat = activity_stats.get(key)
        if stat is None:
            stat = {
                "price": price,
                "order_id": str(order_id or ""),
                "event_type": normalized_type,
                "side": normalized_side,
                "raw_total": 0,
                "event_count": 0,
                "max_event_size": 0,
                "last_event_size": 0,
                "last_timestamp_ms": 0,
                "last_order_current_contracts": 0,
                "positive_refill_count": 0,
                "positive_refill_total": 0,
            }
            activity_stats[key] = stat
        stat["raw_total"] = int(stat["raw_total"]) + amount
        stat["event_count"] = int(stat["event_count"]) + 1
        refill_count = max(0, int(positive_refill_count))
        refill_amount = max(0, int(positive_refill_contracts))
        if refill_count > 0:
            stat["positive_refill_count"] = int(stat["positive_refill_count"]) + refill_count
            stat["positive_refill_total"] = int(stat["positive_refill_total"]) + refill_amount
        stat["max_event_size"] = max(int(stat["max_event_size"]), amount)
        if int(timestamp_ms) >= int(stat["last_timestamp_ms"]):
            stat["last_timestamp_ms"] = int(timestamp_ms)
            stat["last_event_size"] = amount
            stat["last_order_current_contracts"] = max(0, int(order_current_contracts))

    @staticmethod
    def _top_book_activity_by_price(
        activity_stats: dict[tuple[Decimal, str, str, str], dict[str, Any]],
        raw_execute_by_price: dict[Decimal, dict[str, int]],
    ) -> dict[Decimal, dict[str, Any]]:
        best_by_price: dict[Decimal, dict[str, Any]] = {}
        for stat in activity_stats.values():
            price = stat["price"]
            current = best_by_price.get(price)
            if (
                current is None
                or int(stat["raw_total"]) > int(current["raw_total"])
                or (
                    int(stat["raw_total"]) == int(current["raw_total"])
                    and int(stat["event_count"]) > int(current["event_count"])
                )
            ):
                best_by_price[price] = stat
        top_prices = sorted(
            best_by_price,
            key=lambda price: (
                int(best_by_price[price]["raw_total"]),
                int(best_by_price[price]["event_count"]),
                str(best_by_price[price]["order_id"]),
            ),
            reverse=True,
        )[:5]
        result: dict[Decimal, dict[str, Any]] = {}
        for rank, price in enumerate(top_prices, start=1):
            stat = dict(best_by_price[price])
            totals = raw_execute_by_price.get(price, {})
            stat["top_rank"] = rank
            stat["raw_buy_execute_contracts"] = int(totals.get("buy", 0))
            stat["raw_sell_execute_contracts"] = int(totals.get("sell", 0))
            result[price] = stat
        for price, totals in raw_execute_by_price.items():
            stat = result.setdefault(price, {"price": price})
            stat["raw_buy_execute_contracts"] = int(totals.get("buy", 0))
            stat["raw_sell_execute_contracts"] = int(totals.get("sell", 0))
        return result

    def _order_book_levels(
        self,
        bid_levels: defaultdict[Decimal, int],
        ask_levels: defaultdict[Decimal, int],
        *,
        orders: dict[str, DomOrderState] | None = None,
        activity_by_price: dict[Decimal, dict[str, Any]] | None = None,
        price_min: Decimal | None,
        price_max: Decimal | None,
    ) -> list[dict[str, Any]]:
        del orders
        activity_by_price = activity_by_price or {}
        prices = sorted(
            {
                price
                for price in set(bid_levels) | set(ask_levels) | set(activity_by_price)
                if _price_in_range(price, price_min=price_min, price_max=price_max)
            },
            reverse=True,
        )
        return [
            {
                "price": _format_decimal(price),
                "bid_contracts": int(bid_levels.get(price, 0)),
                "ask_contracts": int(ask_levels.get(price, 0)),
                "raw_buy_execute_contracts": int(
                    activity_by_price.get(price, {}).get("raw_buy_execute_contracts", 0)
                ),
                "raw_sell_execute_contracts": int(
                    activity_by_price.get(price, {}).get("raw_sell_execute_contracts", 0)
                ),
                "top_order_id": str(activity_by_price.get(price, {}).get("order_id", "")),
                "top_order_side": str(activity_by_price.get(price, {}).get("side", "")),
                "top_order_type": str(activity_by_price.get(price, {}).get("event_type", "")),
                "top_order_size": int(activity_by_price.get(price, {}).get("raw_total", 0)),
                "top_order_count": int(activity_by_price.get(price, {}).get("event_count", 0)),
                "top_order_rank": int(activity_by_price.get(price, {}).get("top_rank", 0)),
                "top_order_last_size": int(activity_by_price.get(price, {}).get("last_event_size", 0)),
                "top_order_current_contracts": int(
                    activity_by_price.get(price, {}).get("last_order_current_contracts", 0)
                ),
                "top_order_positive_refill_count": int(
                    activity_by_price.get(price, {}).get("positive_refill_count", 0)
                ),
                "top_order_positive_refill_total": int(
                    activity_by_price.get(price, {}).get("positive_refill_total", 0)
                ),
            }
            for price in prices
        ]

    def _time_bucket_ms(self, context: DomContext) -> int:
        timeframe_ms = int(TIMEFRAME_MS_BY_NAME.get(context.timeframe, 1))
        divisor = max(1, int(self.config.time_bucket_divisor))
        return max(1, timeframe_ms // divisor)

    def _quote_lines_from_events(
        self,
        events: tuple[DomRawEvent, ...],
        *,
        context: DomContext,
        start_ms: int,
        end_ms: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        bucket_ms = self._time_bucket_ms(context)
        buckets: dict[int, dict[str, set[Decimal]]] = {}
        for event in events:
            event_ms = int(event.ts_event_ms)
            if event.price is None or event_ms < int(start_ms) or event_ms > int(end_ms):
                continue
            if event.side not in {"BID", "ASK"}:
                continue
            bucket_key = (event_ms // bucket_ms) * bucket_ms
            side_key = "bid" if event.side == "BID" else "ask"
            buckets.setdefault(bucket_key, {"bid": set(), "ask": set()})[side_key].add(event.price)
        bid_line: list[dict[str, Any]] = []
        ask_line: list[dict[str, Any]] = []
        last_bid: Decimal | None = None
        last_ask: Decimal | None = None
        for bucket_key in sorted(buckets):
            bid, ask = _non_crossed_quote(
                buckets[bucket_key]["bid"],
                buckets[bucket_key]["ask"],
                previous_bid=last_bid,
                previous_ask=last_ask,
            )
            if bid is not None:
                last_bid = bid
                if not bid_line or bid_line[-1]["price"] != _format_decimal(bid):
                    bid_line.append({"timestamp_ms": int(bucket_key), "price": _format_decimal(bid)})
            if ask is not None:
                last_ask = ask
                if not ask_line or ask_line[-1]["price"] != _format_decimal(ask):
                    ask_line.append({"timestamp_ms": int(bucket_key), "price": _format_decimal(ask)})
        return bid_line, ask_line

    def _empty_payload(
        self,
        context: DomContext,
        *,
        files: tuple[Any, ...],
        status: str,
        message: str,
        provider_result: DomProviderResult | None = None,
    ) -> dict[str, Any]:
        debug = {
            "requested_start_ms": 0,
            "requested_end_ms": 0,
            "plan_start_ms": 0,
            "plan_end_ms": 0,
            "render_start_ms": 0,
            "render_end_ms": 0,
            "buffer_start_ms": 0,
            "buffer_end_ms": 0,
            "cache_hit_count": 0,
            "provider_event_count": len(provider_result.events) if provider_result else 0,
            "visible_event_count": 0,
            "dom_file_count": len(files),
            "mbo_event_count": len(provider_result.events) if provider_result else 0,
            "symbol": context.provider_symbol,
            "timeframe": context.timeframe,
            "tick_size": str(context.tick_size),
            "price_level_count": 0,
            "time_bucket_count": 0,
            "add_count": 0,
            "cancel_delete_count": 0,
            "modify_count": 0,
            "execute_count": 0,
            "last_bid": "",
            "last_ask": "",
        }
        return {
            "type": "DOM_TIMELINE_SESSION",
            "mt5_symbol": context.mt5_symbol,
            "symbol": context.provider_symbol,
            "provider_symbol": context.provider_symbol,
            "market_provider": context.market_provider,
            "dataset": context.dataset,
            "schema": "mbo",
            "timeframe": context.timeframe,
            "interval": context.interval,
            "timezone": context.timezone,
            "session_start_hour_chicago": context.session_start_hour_chicago,
            "price_step": str(context.tick_size),
            "tick_size": str(context.tick_size),
            "quantity_unit": "CONTRACTS",
            "trigger_timeout_candles": context.trigger_timeout_candles,
            "initial_view_candles": context.initial_view_candles,
            "retention_ms": context.retention_ms,
            "data_dir": str(context.data_dir),
            "dom_files": [item.to_payload() for item in files],
            "dom_file_count": len(files),
            "contract_symbols": [],
            "viewport_window": True,
            "window_start_ms": 0,
            "window_end_ms": 0,
            "render_start_ms": 0,
            "render_end_ms": 0,
            "navigation_start_ms": 0,
            "navigation_end_ms": 0,
            "global_navigation_start_ms": 0,
            "global_navigation_end_ms": 0,
            "available_dates": [],
            "selected_date": "",
            "earliest_window_start_ms": 0,
            "latest_window_end_ms": 0,
            "has_older_data": False,
            "status": status,
            "message": message,
            "events": [],
            "raw_events": [],
            "dom_refill_points": [],
            "engine_outputs": {
                "dom_positive_refills": [],
            },
            "resting_segments": [],
            "best_bid_line": [],
            "best_ask_line": [],
            "order_book_levels": [],
            "time_bucket_ms": self._time_bucket_ms(context),
            "viewport_metrics": {
                "cache_hit_count": 0,
                "cache_miss_count": 1,
                "mbo_event_count": 0,
            },
            "debug": debug,
        }


def _dom_positive_refill_outputs(
    points: list[dict[str, Any]],
    *,
    context: DomContext,
) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    for point in points:
        timestamp_ms = _safe_int(
            point.get("timestamp_ms", point.get("event_time_ms")),
            0,
        )
        if timestamp_ms <= 0:
            continue
        price = str(point.get("price") or "").strip()
        side = str(point.get("side") or "").strip().upper()
        order_id = str(point.get("order_id") or point.get("venue_order_id") or "").strip()
        candle_open_time_ms = _safe_int(point.get("candle_open_time_ms"), timestamp_ms)
        refill_count = _safe_int(point.get("positive_refill_count"), 0)
        refill_total = _safe_int(point.get("positive_refill_total"), 0)
        executed_refill = min(refill_total, _safe_int(point.get("executed_refill_contracts"), 0))
        execution_rate = round(executed_refill / refill_total * 100.0 if refill_total > 0 else 0.0, 1)
        rate_label = f"{execution_rate:.1f}".rstrip("0").rstrip(".")
        if not price or not side or refill_count < DOM_REFILL_POINT_MIN_COUNT:
            continue
        output_id = "|".join(
            (
                DOM_ENGINE_PRODUCER.upper(),
                DOM_POSITIVE_REFILL_OUTPUT_TYPE,
                context.provider_symbol.upper(),
                context.timeframe.strip().upper(),
                str(candle_open_time_ms),
                price,
                side,
                "PRICE",
            )
        )
        outputs.append(
            {
                "id": output_id,
                "output_id": output_id,
                "producer": DOM_ENGINE_PRODUCER,
                "source_engine": DOM_ENGINE_PRODUCER,
                "type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
                "output_type": DOM_POSITIVE_REFILL_OUTPUT_TYPE,
                "name": "DOM price-base refill",
                "schema_version": "2.0",
                "timestamp_ms": timestamp_ms,
                "event_time_ms": timestamp_ms,
                "candle_open_time_ms": candle_open_time_ms,
                "date": _date_label_for_ms(timestamp_ms),
                "symbol": context.provider_symbol,
                "provider_symbol": context.provider_symbol,
                "mt5_symbol": context.mt5_symbol,
                "market_provider": context.market_provider,
                "timeframe": context.timeframe,
                "interval": context.interval,
                "price": price,
                "side": side,
                "order_id": order_id,
                "venue_order_id": order_id,
                "order_ids": tuple(point.get("order_ids") or ()),
                "order_count": _safe_int(point.get("order_count"), 0),
                "refill_method": "price_base_refill",
                "positive_refill_count": refill_count,
                "positive_refill_total": refill_total,
                "refill_count": refill_count,
                "refill_total": refill_total,
                "price_base_refill_count": refill_count,
                "price_base_refill_contracts": refill_total,
                "refill_added_contracts": refill_total,
                "executed_refill_contracts": executed_refill,
                "withdrawn_refill_contracts": _safe_int(point.get("withdrawn_refill_contracts"), 0),
                "refill_execution_rate": execution_rate,
                "refill_display": f"{refill_count}({refill_total}) E{executed_refill} - {rate_label}%",
                "span_candles": DOM_REFILL_POINT_CANDLE_SPAN,
                "source": str(point.get("source") or "PRICE_BASE_REFILL"),
            }
        )
    return outputs


def _event_type(action: str) -> str:
    normalized = str(action or "").strip().upper()
    if normalized in ADD_ACTIONS:
        return "ADD"
    if normalized == "R":
        return "CLEAR"
    if normalized in CANCEL_ACTIONS:
        return "CANCEL_DELETE"
    if normalized in MODIFY_ACTIONS:
        return "MODIFY"
    if normalized in EXECUTE_ACTIONS:
        return "EXECUTE"
    return "OTHER"


def _price_in_range(
    price: Decimal | None,
    *,
    price_min: Decimal | None,
    price_max: Decimal | None,
) -> bool:
    if price is None:
        return True
    if price_min is not None and price < price_min:
        return False
    if price_max is not None and price > price_max:
        return False
    return True


def _non_crossed_quote(
    bid_prices: set[Decimal],
    ask_prices: set[Decimal],
    *,
    previous_bid: Decimal | None,
    previous_ask: Decimal | None,
) -> tuple[Decimal | None, Decimal | None]:
    bids = sorted(bid_prices, reverse=True)
    asks = sorted(ask_prices)
    best_pair: tuple[Decimal, Decimal] | None = None
    best_spread: Decimal | None = None
    for bid in bids:
        for ask in asks:
            spread = ask - bid
            if spread <= 0:
                continue
            if best_spread is None or spread < best_spread:
                best_pair = (bid, ask)
                best_spread = spread
            break
    if best_pair is not None:
        return best_pair
    bid = None
    ask = None
    if previous_ask is not None:
        valid_bids = [item for item in bids if item < previous_ask]
        if valid_bids:
            bid = valid_bids[0]
    if previous_bid is not None:
        valid_asks = [item for item in asks if item > previous_bid]
        if valid_asks:
            ask = valid_asks[0]
    if bid is None and previous_bid is not None:
        bid = previous_bid
    if ask is None and previous_ask is not None:
        ask = previous_ask
    return bid, ask


def _dom_event_id(context: DomContext, event: DomRawEvent, order_key: str) -> str:
    return "|".join(
        (
            context.provider_symbol,
            str(event.instrument_id),
            str(int(event.ts_event_ms)),
            str(int(event.sequence)),
            str(order_key),
            str(event.action),
        )
    )


def _raw_event_merge_key(event: DomRawEvent) -> tuple[Any, ...]:
    return (
        int(event.ts_event_ms),
        int(event.sequence),
        str(event.order_id),
        str(event.action),
        str(event.side),
        str(event.price or ""),
        int(event.size),
        int(event.instrument_id),
    )


def _sample_points(points: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    limit = max(1, int(max_points))
    if len(points) <= limit:
        return points
    step = (len(points) - 1) / max(1, limit - 1)
    return [points[round(index * step)] for index in range(limit)]


def _render_contract_amount(point: dict[str, Any]) -> int:
    for key in ("raw_event_size", "order_size", "size"):
        value = point.get(key)
        parsed = _safe_int(value, 0)
        if parsed > 0:
            return parsed
    event_type = str(point.get("event_type", "")).upper()
    if event_type == "RESTING_LIQUIDITY":
        value = point.get("order_size")
    elif event_type == "CANCEL_DELETE":
        value = point.get("canceled_contracts", point.get("order_size"))
    elif event_type == "MODIFY":
        value = abs(_safe_int(point.get("modified_delta"), _safe_int(point.get("order_size"), 0)))
    elif event_type == "ADD":
        value = point.get("added_contracts", point.get("order_size"))
    elif event_type == "EXECUTE":
        value = point.get("executed_contracts", point.get("order_size"))
    else:
        value = point.get("order_size", 0)
    return max(0, _safe_int(value, 0))


def _render_priority_score(point: dict[str, Any]) -> tuple[int, int, int]:
    event_type = str(point.get("event_type", "")).upper()
    amount = _render_contract_amount(point)
    family_rank = {
        "RESTING_LIQUIDITY": 0,
        "CANCEL_DELETE": 1,
        "MODIFY": 2,
        "ADD": 3,
        "EXECUTE": 4,
    }.get(event_type, 2)
    if amount <= DOM_LOW_CONTRACT_THRESHOLD:
        return (0, family_rank, amount)
    return (1, amount, family_rank)


def _sample_render_points_by_priority(
    points: list[dict[str, Any]],
    max_points: int,
    *,
    start_key: str,
    end_key: str | None = None,
) -> list[dict[str, Any]]:
    limit = max(1, int(max_points))
    if len(points) <= limit:
        return points
    groups: dict[tuple[int, int, int], list[dict[str, Any]]] = defaultdict(list)
    for point in points:
        groups[_render_priority_score(point)].append(point)

    selected: list[dict[str, Any]] = []
    for score in sorted(groups, reverse=True):
        remaining = limit - len(selected)
        if remaining <= 0:
            break
        group = groups[score]
        selected.extend(group if len(group) <= remaining else _sample_points(group, remaining))

    return sorted(
        selected,
        key=lambda item: (
            _safe_int(item.get(start_key), 0),
            _safe_int(item.get(end_key), 0) if end_key else 0,
            str(item.get("price", "")),
        ),
    )


def _sample_points_with_primary_window(
    points: list[dict[str, Any]],
    max_points: int,
    *,
    primary_start_ms: int,
    primary_end_ms: int,
    start_key: str,
    end_key: str | None = None,
) -> list[dict[str, Any]]:
    limit = max(1, int(max_points))
    if len(points) <= limit:
        return points
    primary: list[dict[str, Any]] = []
    margin: list[dict[str, Any]] = []
    for point in points:
        start_ms = _safe_int(point.get(start_key), 0)
        end_ms = _safe_int(point.get(end_key), start_ms) if end_key else start_ms
        if start_ms <= int(primary_end_ms) and end_ms >= int(primary_start_ms):
            primary.append(point)
        else:
            margin.append(point)
    if len(primary) >= limit:
        return _sample_render_points_by_priority(
            primary,
            limit,
            start_key=start_key,
            end_key=end_key,
        )
    sampled_margin = (
        _sample_render_points_by_priority(
            margin,
            limit - len(primary),
            start_key=start_key,
            end_key=end_key,
        )
        if margin
        else []
    )
    combined = primary + sampled_margin
    return sorted(
        combined,
        key=lambda item: (
            _safe_int(item.get(start_key), 0),
            _safe_int(item.get(end_key), 0) if end_key else 0,
            str(item.get("price", "")),
        ),
    )


def _safe_int(value: Any, fallback: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(fallback)


def _initial_view_span_ms(context: DomContext) -> int:
    timeframe_ms = int(TIMEFRAME_MS_BY_NAME.get(context.timeframe.strip().upper(), 0))
    if timeframe_ms <= 0:
        timeout_candles = max(1, int(context.trigger_timeout_candles))
        timeframe_ms = max(1, int(context.retention_ms) // timeout_candles)
    return max(1, int(context.initial_view_candles)) * max(1, timeframe_ms)


def _line_with_window_edges(
    points: list[dict[str, Any]],
    *,
    start_ms: int,
    end_ms: int,
) -> list[dict[str, Any]]:
    if not points:
        return []
    ordered = sorted(points, key=lambda item: int(item.get("timestamp_ms", 0)))
    first = ordered[0]
    last = ordered[-1]
    result = list(ordered)
    if int(first.get("timestamp_ms", 0)) > int(start_ms):
        result.insert(0, {"timestamp_ms": int(start_ms), "price": first.get("price", "")})
    if int(last.get("timestamp_ms", 0)) < int(end_ms):
        result.append({"timestamp_ms": int(end_ms), "price": last.get("price", "")})
    if len(result) == 1:
        result = [
            {"timestamp_ms": int(start_ms), "price": result[0].get("price", "")},
            {"timestamp_ms": int(end_ms), "price": result[0].get("price", "")},
        ]
    return result


def _date_bounds_utc_ms(value: str | None) -> tuple[int, int] | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        day = datetime.strptime(text[:10], "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None
    start_ms = int(day.timestamp() * 1000)
    return start_ms, int((day + timedelta(days=1)).timestamp() * 1000)


def _day_bounds_for_ms(value_ms: int) -> tuple[int, int]:
    day = datetime.fromtimestamp(max(0, int(value_ms)) / 1000, tz=UTC)
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    start_ms = int(start.timestamp() * 1000)
    return start_ms, int((start + timedelta(days=1)).timestamp() * 1000)


def _date_label_for_ms(value_ms: int) -> str:
    return datetime.fromtimestamp(max(0, int(value_ms)) / 1000, tz=UTC).date().isoformat()


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral_value(rounding=ROUND_FLOOR):
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")
