from __future__ import annotations

from queue import Queue
from typing import Any, Mapping

from actor_proxy.actor_proxy_config import ActorProxyConfig
from actor_proxy.actor_proxy_engine import ActorProxyEngine
from actor_proxy.actor_proxy_models import ExitSignal, PositionContext, RawDomEvent


class ActorProxyLiveAdapter:
    """Live API boundary placeholder.

    The real broker/API connector can call handle_event() with raw DOM/MBO events.
    Tracking starts only after TriggerEngine opens a position and calls start().
    """

    def __init__(
        self,
        *,
        engine: ActorProxyEngine,
        config: ActorProxyConfig | None = None,
    ) -> None:
        self.engine = engine
        self.config = config or engine.config
        self.queue: Queue[RawDomEvent] = Queue(maxsize=self.config.live_queue_maxsize)
        self.active_context: PositionContext | None = None

    def start(self, position_context: PositionContext | Mapping[str, Any]) -> None:
        context = (
            position_context
            if isinstance(position_context, PositionContext)
            else PositionContext.from_mapping(position_context)
        )
        self.active_context = PositionContext(
            position_id=context.position_id,
            side=context.side,
            entry_price=context.entry_price,
            entry_time_ms=context.entry_time_ms,
            symbol=context.symbol,
            provider_symbol=context.provider_symbol,
            timeframe=context.timeframe,
            tracking_start_ms=context.entry_time_ms,
            tracking_end_ms=None,
            mode="LIVE",
        )

    def stop(self) -> None:
        self.active_context = None

    def handle_event(self, event: RawDomEvent | Mapping[str, Any]) -> None:
        if self.active_context is None:
            return
        raw_event = event if isinstance(event, RawDomEvent) else RawDomEvent.from_mapping(event)
        self.queue.put_nowait(raw_event)

    def drain(self, *, max_events: int | None = None) -> tuple[ExitSignal, ...]:
        if self.active_context is None:
            return tuple()
        signals: list[ExitSignal] = []
        drained = 0
        while not self.queue.empty():
            if max_events is not None and drained >= max_events:
                break
            event = self.queue.get_nowait()
            drained += 1
            signals.extend(self.engine.on_raw_dom_event(event))
        return tuple(signals)
