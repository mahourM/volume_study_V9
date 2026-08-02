from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Mapping

from actor_proxy.actor_proxy_engine import ActorProxyEngine
from actor_proxy.actor_proxy_models import (
    ActorProxyPayload,
    ExitSignal,
    PositionContext,
    RawDomEvent,
)


class ActorProxyReplayAdapter:
    """Feeds exact user-requested replay windows into ActorProxyEngine."""

    def __init__(
        self,
        *,
        raw_events: Iterable[RawDomEvent | Mapping[str, Any]] = (),
        event_source: Any = None,
    ) -> None:
        self.raw_events = tuple(raw_events)
        self.event_source = event_source

    def iter_events(
        self,
        *,
        start_ms: int,
        end_ms: int,
        symbol: str = "",
        process_symbol: Any = None,
    ) -> tuple[RawDomEvent, ...]:
        if int(end_ms) < int(start_ms):
            raise ValueError("replay end_ms must be greater than or equal to start_ms")
        if self.event_source is not None:
            if process_symbol is None:
                raise ValueError("process_symbol is required when event_source is configured")
            events = (
                RawDomEvent.from_dom_raw_event(
                    event,
                    symbol=symbol or getattr(process_symbol, "provider_symbol", ""),
                    provider_symbol=getattr(process_symbol, "provider_symbol", ""),
                )
                for event in self.event_source.events(
                    process_symbol,
                    start_ms=int(start_ms),
                    end_ms=int(end_ms),
                )
            )
        else:
            events = (
                event if isinstance(event, RawDomEvent) else RawDomEvent.from_mapping(event)
                for event in self.raw_events
            )
        normalized_symbol = str(symbol or "").strip().upper()
        return tuple(
            sorted(
                (
                    event
                    for event in events
                    if int(start_ms) <= int(event.ts_event_ms) <= int(end_ms)
                    and (not normalized_symbol or event.symbol in {"", normalized_symbol})
                ),
                key=lambda item: (
                    item.ts_event_ms,
                    item.sequence if item.sequence is not None else 0,
                    item.ordinal if item.ordinal is not None else 0,
                ),
            )
        )

    def run_tracking(
        self,
        *,
        engine: ActorProxyEngine,
        actor_payload: ActorProxyPayload | Mapping[str, Any],
        position_context: PositionContext | Mapping[str, Any],
        start_ms: int,
        end_ms: int,
        symbol: str = "",
        process_symbol: Any = None,
    ) -> tuple[ExitSignal, ...]:
        context_payload = (
            position_context.to_payload()
            if isinstance(position_context, PositionContext)
            else dict(position_context)
        )
        context_payload.update(
            {
                "mode": "REPLAY",
                "tracking_mode": "REPLAY",
                "tracking_start_ms": int(start_ms),
                "tracking_end_ms": int(end_ms),
                "replay_start_ms": int(start_ms),
                "replay_end_ms": int(end_ms),
            }
        )
        engine.start_tracking(actor_payload, context_payload)
        signals: list[ExitSignal] = []
        for event in self.iter_events(
            start_ms=int(start_ms),
            end_ms=int(end_ms),
            symbol=symbol,
            process_symbol=process_symbol,
        ):
            signals.extend(engine.on_raw_dom_event(event))
        return tuple(signals)
