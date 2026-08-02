from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal


NEUTRAL_SIDE = "NEUTRAL"
TRADE_SIDES = {"BUY", "SELL"}
ZONE_STATE_TIMEFRAMES = ("M1", "M5", "M15", "M30")


@dataclass(frozen=True)
class ZoneState:
    symbol: str
    timeframe: str
    zone_id: str
    side: str
    suggested_stop_loss: Decimal | None = None

    @property
    def is_active(self) -> bool:
        return bool(self.zone_id) and self.side in TRADE_SIDES


class ZoneStateStore:
    def __init__(self) -> None:
        self._states: dict[tuple[str, str], ZoneState] = {}

    def get(self, symbol: str, timeframe: str) -> ZoneState:
        normalized_symbol = symbol.strip()
        normalized_timeframe = timeframe.strip().upper()
        return self._states.get(
            (normalized_symbol, normalized_timeframe),
            ZoneState(
                symbol=normalized_symbol,
                timeframe=normalized_timeframe,
                zone_id="",
                side=NEUTRAL_SIDE,
                suggested_stop_loss=None,
            ),
        )

    def update(self, state: ZoneState) -> bool:
        normalized_state = ZoneState(
            symbol=state.symbol.strip(),
            timeframe=state.timeframe.strip().upper(),
            zone_id=state.zone_id,
            side=state.side.strip().upper() if state.side else NEUTRAL_SIDE,
            suggested_stop_loss=state.suggested_stop_loss,
        )
        key = (normalized_state.symbol, normalized_state.timeframe)
        previous = self._states.get(key)
        if previous == normalized_state:
            return False
        self._states[key] = normalized_state
        return True

    def remove_symbol(self, symbol: str) -> None:
        normalized_symbol = symbol.strip()
        for key in list(self._states):
            if key[0] == normalized_symbol:
                self._states.pop(key, None)

    def symbols(self) -> tuple[str, ...]:
        return tuple(sorted({key[0] for key in self._states}))

    def states_for_symbol(self, symbol: str) -> tuple[ZoneState, ...]:
        normalized_symbol = symbol.strip()
        return tuple(
            self.get(normalized_symbol, timeframe)
            for timeframe in ZONE_STATE_TIMEFRAMES
        )
