from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from zoneinfo import ZoneInfo


VANCOUVER_TIMEZONE = "America/Vancouver"
VANCOUVER_ZONE = ZoneInfo(VANCOUVER_TIMEZONE)


@dataclass(frozen=True)
class VancouverReplayRange:
    start_ms: int
    end_ms: int
    start_vancouver: str
    end_vancouver: str
    start_utc: str
    end_utc: str


def parse_vancouver_replay_range(
    *,
    start: str,
    end: str,
) -> VancouverReplayRange:
    start_dt = parse_vancouver_datetime(start)
    end_dt = parse_vancouver_datetime(end)
    start_ms = _utc_ms(start_dt)
    end_ms = _utc_ms(end_dt)
    if end_ms <= start_ms:
        raise ValueError("end time must be after start time")
    return VancouverReplayRange(
        start_ms=start_ms,
        end_ms=end_ms,
        start_vancouver=_format_vancouver(start_dt),
        end_vancouver=_format_vancouver(end_dt),
        start_utc=_format_utc(start_dt),
        end_utc=_format_utc(end_dt),
    )


def parse_vancouver_datetime(value: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise ValueError("time value is required")
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                parsed = datetime.strptime(text, "%Y-%m-%d %H:%M")
            except ValueError as exc:
                raise ValueError(
                    "time must use YYYY-MM-DD HH:MM or YYYY-MM-DDTHH:MM"
                ) from exc
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=VANCOUVER_ZONE)
    return parsed.astimezone(VANCOUVER_ZONE)


def _utc_ms(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1000)


def _format_vancouver(value: datetime) -> str:
    return value.astimezone(VANCOUVER_ZONE).isoformat(timespec="seconds")


def _format_utc(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds")
