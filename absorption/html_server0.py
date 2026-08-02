from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal, InvalidOperation
from typing import Protocol
from urllib.parse import parse_qs

from core.contract_spike import CONTRACT_SPIKE_THRESHOLD
from core.startup_guard import is_address_in_use_error
from core.timeframe_policy import (
    DEFAULT_FOOTPRINT_TIMEFRAME,
    STUDY_TIMEFRAMES,
)
from DOM.html import (
    DOM_HTML_PAGE,
    dom_html_page,
    dom_timeframe_for_data_path,
    dom_timeframe_for_path,
)

FOOTPRINT_TIMEFRAMES = STUDY_TIMEFRAMES
LOGGER = logging.getLogger(__name__)


def _timeframe_for_path(path: str) -> str | None:
    if path in {"/", "/footprint"}:
        return DEFAULT_FOOTPRINT_TIMEFRAME
    prefix = "/footprint/"
    if path.startswith(prefix):
        timeframe = path[len(prefix) :].strip().upper()
        return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None
    return None


def _timeframe_for_candles_path(path: str) -> str | None:
    if path == "/candles":
        return DEFAULT_FOOTPRINT_TIMEFRAME
    prefix = "/candles/"
    if path.startswith(prefix):
        timeframe = path[len(prefix) :].strip().upper()
        return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None
    return None


class SnapshotProvider(Protocol):
    def snapshot_payload(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        candle_limit: int | None = None,
        bin_tick_count: int | None = None,
    ) -> dict:
        ...

    def candle_chart_payload(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        candle_limit: int | None = None,
        include_profiles: bool = True,
    ) -> dict:
        ...

    def dom_timeline_payload(
        self,
        timeframe: str | None = None,
        end_time_ms: int | None = None,
        start_time_ms: int | None = None,
        price_min: Decimal | None = None,
        price_max: Decimal | None = None,
        selected_date: str | None = None,
        iceberg_min_contracts: int | None = None,
        iceberg_order_ids: tuple[str, ...] = (),
        iceberg_path_start_ms: int | None = None,
        iceberg_path_end_ms: int | None = None,
    ) -> dict:
        ...

    def data_process_replay_payload(
        self,
        *,
        timeframe: str | None = None,
        start_vancouver: str,
        end_vancouver: str,
    ) -> dict:
        ...


class AbsorptionHtmlServer:
    def __init__(self, host: str, port: int, snapshot_provider: SnapshotProvider) -> None:
        self.host = host
        self.port = port
        self.snapshot_provider = snapshot_provider

    async def run_forever(self) -> None:
        while True:
            try:
                server = await asyncio.start_server(self._handle_request, host=self.host, port=self.port)
                async with server:
                    await server.serve_forever()
            except OSError as exc:
                if is_address_in_use_error(exc):
                    raise
                LOGGER.exception("ABSORPTION_HTTP_SERVER_RESTARTING | error=%s", exc)
                await asyncio.sleep(5)
            except Exception as exc:
                LOGGER.exception("ABSORPTION_HTTP_SERVER_RESTARTING | error=%s", exc)
                await asyncio.sleep(5)

    async def _handle_request(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            request_text = request_line.decode("utf-8", errors="ignore")
            raw_target = request_text.split(" ")[1] if " " in request_text else "/"
            raw_path, _, raw_query = raw_target.partition("?")
            path = raw_path
            query_params = parse_qs(raw_query)
            if path != "/" and path.endswith("/"):
                path = path.rstrip("/")

            while True:
                header_line = await reader.readline()
                if not header_line or header_line in {b"\r\n", b"\n"}:
                    break

            status_line = "HTTP/1.1 200 OK"

            if path == "/viewport-client-metric":
                _record_client_viewport_metric(query_params)
                body = b""
                content_type = "text/plain; charset=utf-8"
            elif path == "/process-replay" or path.startswith("/process-replay/"):
                timeframe = (
                    _timeframe_for_process_replay_path(path)
                    or _timeframe_from_query(query_params)
                    or DEFAULT_FOOTPRINT_TIMEFRAME
                )
                status_line, replay_payload = await asyncio.to_thread(
                    _process_replay_payload_for_timeframe,
                    self.snapshot_provider,
                    timeframe,
                    query_params,
                )
                body = json.dumps(replay_payload).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            elif path == "/data":
                timeframe = _timeframe_from_query(query_params)
                snapshot_payload = await asyncio.to_thread(
                    _snapshot_payload_for_timeframe,
                    self.snapshot_provider,
                    timeframe,
                    None,
                    None,
                )
                if timeframe is not None:
                    snapshot_payload = _filter_snapshot_payload_timeframe(snapshot_payload, timeframe)
                body = json.dumps(snapshot_payload).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            elif path.startswith("/footprint-data/"):
                timeframe = _timeframe_for_data_path(path)
                if timeframe is None:
                    status_line = "HTTP/1.1 404 Not Found"
                    body = b"Not Found"
                    content_type = "text/plain; charset=utf-8"
                else:
                    fetch_started = time.perf_counter()
                    end_time_ms = _window_end_time_from_query(query_params)
                    candle_limit = _candle_limit_from_query(query_params)
                    bin_tick_count = _bin_tick_count_from_query(query_params)
                    snapshot_payload = _filter_snapshot_payload_timeframe(
                        await asyncio.to_thread(
                            _snapshot_payload_for_timeframe,
                            self.snapshot_provider,
                            timeframe,
                            end_time_ms,
                            candle_limit,
                            bin_tick_count,
                        ),
                        timeframe,
                    )
                    after_open_time_ms = _after_open_time_from_query(query_params)
                    if after_open_time_ms is not None and end_time_ms is None:
                        snapshot_payload = _filter_snapshot_payload_after_open_time(
                            snapshot_payload,
                            after_open_time_ms,
                        )
                    snapshot_payload = _filter_snapshot_payload_known_candles(
                        snapshot_payload,
                        _known_open_times_from_query(query_params),
                    )
                    body = json.dumps(snapshot_payload).encode("utf-8")
                    _record_viewport_fetch_metric(
                        snapshot_payload,
                        view="footprint",
                        timeframe=timeframe,
                        request_id=_request_id_from_query(query_params),
                        duration_ms=(
                            time.perf_counter() - fetch_started
                        ) * 1000.0,
                    )
                    content_type = "application/json; charset=utf-8"
            elif path.startswith("/dom-data/"):
                timeframe = dom_timeframe_for_data_path(path)
                if timeframe is None:
                    status_line = "HTTP/1.1 404 Not Found"
                    body = b"Not Found"
                    content_type = "text/plain; charset=utf-8"
                else:
                    fetch_started = time.perf_counter()
                    snapshot_payload = await asyncio.to_thread(
                        _dom_timeline_payload_for_timeframe,
                        self.snapshot_provider,
                        timeframe,
                        _window_end_time_from_query(query_params),
                        _window_start_time_from_query(query_params),
                        _decimal_from_query(query_params, "price_min"),
                        _decimal_from_query(query_params, "price_max"),
                        _date_from_query(query_params),
                        _iceberg_min_contracts_from_query(query_params),
                        _iceberg_order_ids_from_query(query_params),
                        _positive_int_from_query(query_params, "iceberg_path_start_ms"),
                        _positive_int_from_query(query_params, "iceberg_path_end_ms"),
                    )
                    body = json.dumps(snapshot_payload).encode("utf-8")
                    _record_viewport_fetch_metric(
                        snapshot_payload,
                        view="dom",
                        timeframe=timeframe,
                        request_id=_request_id_from_query(query_params),
                        duration_ms=(
                            time.perf_counter() - fetch_started
                        ) * 1000.0,
                    )
                    content_type = "application/json; charset=utf-8"
            elif path.startswith("/candles-data/"):
                timeframe = _timeframe_for_candles_data_path(path)
                if timeframe is None:
                    status_line = "HTTP/1.1 404 Not Found"
                    body = b"Not Found"
                    content_type = "text/plain; charset=utf-8"
                else:
                    fetch_started = time.perf_counter()
                    end_time_ms = _window_end_time_from_query(query_params)
                    candle_limit = _candle_limit_from_query(query_params)
                    include_profiles = _include_profiles_from_query(query_params)
                    snapshot_payload = await asyncio.to_thread(
                        _candle_chart_payload_for_timeframe,
                        self.snapshot_provider,
                        timeframe,
                        end_time_ms,
                        candle_limit,
                        include_profiles,
                    )
                    snapshot_payload = _filter_snapshot_payload_known_candles(
                        snapshot_payload,
                        _known_open_times_from_query(query_params),
                    )
                    body = json.dumps(snapshot_payload).encode("utf-8")
                    _record_viewport_fetch_metric(
                        snapshot_payload,
                        view="candle",
                        timeframe=timeframe,
                        request_id=_request_id_from_query(query_params),
                        duration_ms=(
                            time.perf_counter() - fetch_started
                        ) * 1000.0,
                    )
                    content_type = "application/json; charset=utf-8"
            else:
                timeframe = _timeframe_for_path(path)
                candles_timeframe = _timeframe_for_candles_path(path)
                dom_timeframe = dom_timeframe_for_path(path)
                if timeframe is None and candles_timeframe is None and dom_timeframe is None:
                    status_line = "HTTP/1.1 404 Not Found"
                    body = b"Not Found"
                    content_type = "text/plain; charset=utf-8"
                elif dom_timeframe is not None:
                    body = dom_html_page(dom_timeframe).encode("utf-8")
                    content_type = "text/html; charset=utf-8"
                elif candles_timeframe is not None:
                    body = _candles_html_page(candles_timeframe).encode("utf-8")
                    content_type = "text/html; charset=utf-8"
                else:
                    body = _html_page(timeframe).encode("utf-8")
                    content_type = "text/html; charset=utf-8"

            writer.write(
                status_line.encode("utf-8")
                + b"\r\n"
                + f"Content-Type: {content_type}\r\n".encode("utf-8")
                + f"Content-Length: {len(body)}\r\n".encode("utf-8")
                + b"Cache-Control: no-store\r\n"
                + b"Connection: close\r\n\r\n"
                + body
            )
            try:
                await writer.drain()
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionResetError, BrokenPipeError, OSError):
                pass


def _timeframe_from_query(query_params: dict[str, list[str]]) -> str | None:
    values = query_params.get("timeframe") or query_params.get("tf") or []
    timeframe = str(values[0]).strip().upper() if values else ""
    return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None


def _timeframe_for_data_path(path: str) -> str | None:
    prefix = "/footprint-data/"
    if not path.startswith(prefix):
        return None
    timeframe = path[len(prefix) :].strip().upper()
    return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None


def _timeframe_for_candles_data_path(path: str) -> str | None:
    prefix = "/candles-data/"
    if not path.startswith(prefix):
        return None
    timeframe = path[len(prefix) :].strip().upper()
    return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None


def _timeframe_for_process_replay_path(path: str) -> str | None:
    prefix = "/process-replay/"
    if not path.startswith(prefix):
        return None
    timeframe = path[len(prefix) :].strip().upper()
    return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None


def _snapshot_payload_for_timeframe(
    provider: SnapshotProvider,
    timeframe: str | None,
    end_time_ms: int | None = None,
    candle_limit: int | None = None,
    bin_tick_count: int | None = None,
) -> dict:
    try:
        return provider.snapshot_payload(
            timeframe=timeframe,
            end_time_ms=end_time_ms,
            candle_limit=candle_limit,
            bin_tick_count=bin_tick_count,
        )
    except TypeError:
        try:
            return provider.snapshot_payload(timeframe=timeframe)
        except TypeError:
            return provider.snapshot_payload()


def _candle_chart_payload_for_timeframe(
    provider: SnapshotProvider,
    timeframe: str | None,
    end_time_ms: int | None = None,
    candle_limit: int | None = None,
    include_profiles: bool = True,
) -> dict:
    chart_payload = getattr(provider, "candle_chart_payload", None)
    if chart_payload is None:
        return {"type": "CME_CANDLE_CHART_SNAPSHOT", "timeframe": timeframe, "sessions": []}
    try:
        return chart_payload(
            timeframe=timeframe,
            end_time_ms=end_time_ms,
            candle_limit=candle_limit,
            include_profiles=include_profiles,
        )
    except TypeError:
        try:
            return chart_payload(timeframe=timeframe)
        except TypeError:
            return chart_payload()


def _dom_timeline_payload_for_timeframe(
    provider: SnapshotProvider,
    timeframe: str | None,
    end_time_ms: int | None = None,
    start_time_ms: int | None = None,
    price_min: Decimal | None = None,
    price_max: Decimal | None = None,
    selected_date: str | None = None,
    iceberg_min_contracts: int | None = None,
    iceberg_order_ids: tuple[str, ...] = (),
    iceberg_path_start_ms: int | None = None,
    iceberg_path_end_ms: int | None = None,
) -> dict:
    dom_payload = getattr(provider, "dom_timeline_payload", None)
    if dom_payload is None:
        return {"type": "DOM_TIMELINE_SNAPSHOT", "timeframe": timeframe, "sessions": []}
    try:
        return dom_payload(
            timeframe=timeframe,
            end_time_ms=end_time_ms,
            start_time_ms=start_time_ms,
            price_min=price_min,
            price_max=price_max,
            selected_date=selected_date,
            iceberg_min_contracts=iceberg_min_contracts,
            iceberg_order_ids=iceberg_order_ids,
            iceberg_path_start_ms=iceberg_path_start_ms,
            iceberg_path_end_ms=iceberg_path_end_ms,
        )
    except TypeError:
        try:
            return dom_payload(timeframe=timeframe)
        except TypeError:
            return dom_payload()


def _process_replay_payload_for_timeframe(
    provider: SnapshotProvider,
    timeframe: str | None,
    query_params: dict[str, list[str]],
) -> tuple[str, dict]:
    replay_payload = getattr(provider, "data_process_replay_payload", None)
    if replay_payload is None:
        return (
            "HTTP/1.1 501 Not Implemented",
            {
                "type": "DATA_PROCESS_REPLAY_RESULT",
                "status": "ERROR",
                "message": "data process replay is not available",
            },
        )
    start_vancouver = _text_from_query(
        query_params,
        "start_vancouver",
        "start",
        "from",
        "start_time",
    )
    end_vancouver = _text_from_query(
        query_params,
        "end_vancouver",
        "end",
        "to",
        "end_time",
    )
    try:
        return (
            "HTTP/1.1 200 OK",
            replay_payload(
                timeframe=timeframe,
                start_vancouver=start_vancouver,
                end_vancouver=end_vancouver,
            ),
        )
    except ValueError as exc:
        return (
            "HTTP/1.1 400 Bad Request",
            {
                "type": "DATA_PROCESS_REPLAY_RESULT",
                "status": "ERROR",
                "message": str(exc),
                "timeframe": timeframe,
            },
        )
    except Exception as exc:
        LOGGER.exception("DATA_PROCESS_REPLAY_ERROR | timeframe=%s", timeframe)
        return (
            "HTTP/1.1 500 Internal Server Error",
            {
                "type": "DATA_PROCESS_REPLAY_RESULT",
                "status": "ERROR",
                "message": str(exc),
                "timeframe": timeframe,
            },
        )


def _after_open_time_from_query(query_params: dict[str, list[str]]) -> int | None:
    values = query_params.get("after_open_time_ms") or query_params.get("after") or []
    if not values:
        return None
    try:
        return int(str(values[0]).strip())
    except ValueError:
        return None


def _window_end_time_from_query(query_params: dict[str, list[str]]) -> int | None:
    values = query_params.get("end_time_ms") or query_params.get("before_open_time_ms") or []
    if not values:
        return None
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def _window_start_time_from_query(query_params: dict[str, list[str]]) -> int | None:
    values = query_params.get("start_time_ms") or query_params.get("from_time_ms") or []
    if not values:
        return None
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def _positive_int_from_query(query_params: dict[str, list[str]], field: str) -> int | None:
    values = query_params.get(field) or []
    if not values:
        return None
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return None
    return value if value > 0 else None


def _iceberg_min_contracts_from_query(query_params: dict[str, list[str]]) -> int | None:
    values = query_params.get("iceberg_min") or query_params.get("iceberg_min_contracts") or []
    if not values:
        return None
    try:
        value = int(float(str(values[0]).strip()))
    except ValueError:
        return None
    return value if value > 0 else None


def _iceberg_order_ids_from_query(query_params: dict[str, list[str]]) -> tuple[str, ...]:
    values = query_params.get("iceberg_order_ids") or query_params.get("iceberg_orders") or []
    order_ids: list[str] = []
    for raw_value in values:
        for item in str(raw_value or "").replace(";", ",").split(","):
            value = item.strip()
            if value:
                order_ids.append(value)
    return tuple(dict.fromkeys(order_ids))


def _decimal_from_query(query_params: dict[str, list[str]], field: str) -> Decimal | None:
    values = query_params.get(field) or []
    if not values:
        return None
    try:
        return Decimal(str(values[0]).strip())
    except (InvalidOperation, ValueError):
        return None


def _date_from_query(query_params: dict[str, list[str]]) -> str | None:
    values = query_params.get("date") or query_params.get("selected_date") or []
    if not values:
        return None
    value = str(values[0]).strip()
    if len(value) < 10:
        return None
    value = value[:10]
    try:
        time.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None
    return value


def _candle_limit_from_query(query_params: dict[str, list[str]]) -> int | None:
    values = query_params.get("candle_limit") or query_params.get("limit") or []
    if not values:
        return None
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return None
    return max(1, min(500, value))


def _bin_tick_count_from_query(query_params: dict[str, list[str]]) -> int:
    values = query_params.get("bin_ticks") or query_params.get("bin_tick_count") or []
    if not values:
        return 4
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return 4
    return value if value in {1, 2, 4, 8, 16} else 4


def _include_profiles_from_query(query_params: dict[str, list[str]]) -> bool:
    values = query_params.get("include_profiles") or []
    if not values:
        return True
    return str(values[0]).strip().lower() not in {"0", "false", "no", "off"}


def _request_id_from_query(query_params: dict[str, list[str]]) -> int | None:
    values = query_params.get("request_id") or []
    if not values:
        return None
    try:
        return int(str(values[0]).strip())
    except ValueError:
        return None


def _text_from_query(query_params: dict[str, list[str]], *fields: str) -> str:
    for field in fields:
        values = query_params.get(field) or []
        if values:
            return str(values[0]).strip()
    return ""


def _known_open_times_from_query(
    query_params: dict[str, list[str]],
) -> set[int]:
    values = query_params.get("known_open_times_ms") or []
    if not values:
        return set()
    result: set[int] = set()
    for raw_value in str(values[0]).split(","):
        try:
            value = int(raw_value.strip())
        except ValueError:
            continue
        if value > 0:
            result.add(value)
        if len(result) >= 500:
            break
    return result


def _filter_snapshot_payload_known_candles(
    snapshot_payload: dict,
    known_open_times: set[int],
) -> dict:
    if not known_open_times:
        return snapshot_payload
    filtered_payload = dict(snapshot_payload)
    filtered_sessions = []
    omitted_count = 0
    for session in snapshot_payload.get("sessions", []):
        next_session = dict(session)
        candles = list(session.get("candles", []))
        next_session["candles"] = [
            candle
            for candle in candles
            if int(candle.get("open_time_ms") or candle.get("open_time") or 0)
            not in known_open_times
            or bool(candle.get("dom_refill_markers"))
        ]
        omitted_count += len(candles) - len(next_session["candles"])
        filtered_sessions.append(next_session)
    filtered_payload["sessions"] = filtered_sessions
    filtered_payload["client_cached_candle_count"] = omitted_count
    return filtered_payload


def _record_viewport_fetch_metric(
    snapshot_payload: dict,
    *,
    view: str,
    timeframe: str,
    request_id: int | None,
    duration_ms: float,
) -> None:
    del snapshot_payload, view, timeframe, request_id, duration_ms


def _record_client_viewport_metric(
    query_params: dict[str, list[str]],
) -> None:
    del query_params


def _optional_positive_int(value: object) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _filter_snapshot_payload_timeframe(snapshot_payload: dict, timeframe: str) -> dict:
    normalized_timeframe = timeframe.strip().upper()
    if normalized_timeframe not in FOOTPRINT_TIMEFRAMES:
        return dict(snapshot_payload)
    filtered_payload = dict(snapshot_payload)
    filtered_payload["sessions"] = [
        session
        for session in snapshot_payload.get("sessions", [])
        if str(session.get("timeframe", "")).strip().upper() == normalized_timeframe
    ]
    display_limits = snapshot_payload.get("display_candles_by_timeframe", {})
    if isinstance(display_limits, dict) and normalized_timeframe in display_limits:
        filtered_payload["memory_candles"] = display_limits[normalized_timeframe]
    return filtered_payload


def _filter_snapshot_payload_after_open_time(snapshot_payload: dict, after_open_time_ms: int) -> dict:
    filtered_payload = dict(snapshot_payload)
    filtered_sessions = []
    for session in snapshot_payload.get("sessions", []):
        next_session = dict(session)
        next_session["candles"] = [
            candle
            for candle in session.get("candles", [])
            if int(candle.get("open_time_ms") or candle.get("open_time") or 0) > after_open_time_ms
        ]
        filtered_sessions.append(next_session)
    filtered_payload["sessions"] = filtered_sessions
    filtered_payload["delta_after_open_time_ms"] = after_open_time_ms
    return filtered_payload


_HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__ACTIVE_TIMEFRAME__</title>
  <style>
    :root {
      --bg: #0d1117;
      --panel: #111820;
      --line: #2b333d;
      --text: #e6edf3;
      --muted: #8b949e;
      --blue: #58a6ff;
      --green: #3fb950;
      --red: #f85149;
      --gold: #d29922;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 12px;
      overflow: hidden;
    }
    header {
      height: 78px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: #0f141b;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      margin: 0 0 4px;
      font-size: 20px;
      line-height: 1.1;
    }
    .meta, .note {
      color: var(--muted);
    }
    .timeframe-links {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .header-controls {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .bin-control {
      display: flex;
      align-items: center;
      gap: 6px;
      color: var(--muted);
      white-space: nowrap;
    }
    .bin-control select {
      height: 30px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #111820;
      color: var(--text);
      padding: 0 24px 0 8px;
    }
    .timeframe-link {
      border: 1px solid var(--line);
      color: var(--muted);
      background: #111820;
      border-radius: 6px;
      padding: 6px 9px;
      text-decoration: none;
      font-weight: 700;
    }
    .timeframe-link.active {
      color: var(--text);
      border-color: var(--blue);
      background: rgba(88,166,255,.16);
    }
    main {
      height: calc(100vh - 78px);
      padding: 12px;
      overflow: hidden;
    }
    .session {
      height: 100%;
      min-height: 420px;
      border: 1px solid var(--line);
      background: var(--panel);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) 24px;
      overflow: hidden;
    }
    .session-title {
      min-height: 40px;
      padding: 9px 12px;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      color: var(--muted);
    }
    .chart-host {
      position: relative;
      min-height: 0;
      background: #0b0f14;
      overflow: hidden;
      cursor: crosshair;
    }
    .chart-scrollbar {
      display: flex;
      align-items: center;
      min-width: 0;
      padding: 2px 88px 2px 6px;
      border-top: 1px solid var(--line);
      background: #0f141b;
    }
    .history-scrollbar {
      width: 100%;
      min-width: 0;
      height: 18px;
      overflow-x: scroll;
      overflow-y: hidden;
      scrollbar-color: var(--blue) #1b2430;
      scrollbar-width: auto;
    }
    .history-scrollbar.disabled {
      overflow-x: hidden;
      opacity: .45;
    }
    .history-scrollbar-content {
      height: 1px;
    }
    canvas {
      display: block;
      width: 100%;
      height: 100%;
    }
    .tooltip {
      position: absolute;
      left: 0;
      top: 0;
      max-width: 460px;
      padding: 7px 9px;
      border: 1px solid var(--line);
      background: rgba(15,20,27,.94);
      color: var(--text);
      font-size: 20px;
      pointer-events: none;
      white-space: pre-line;
      opacity: 0;
      transition: opacity .08s ease;
      line-height: 1.35;
    }
    .empty {
      height: 100%;
      display: grid;
      place-items: center;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>__ACTIVE_TIMEFRAME__</h1>
      <div class="meta" id="status">Waiting for data...</div>
    </div>
    <div class="header-controls">
      <label class="bin-control" for="bin-tick-select">
        <span>Bin</span>
        <select id="bin-tick-select" aria-label="Footprint bin size">
          <option value="1">1 tick</option>
          <option value="2">2 ticks</option>
          <option value="4">4 ticks</option>
          <option value="8">8 ticks</option>
          <option value="16">16 ticks</option>
        </select>
      </label>
      <nav class="timeframe-links" id="timeframe-links" aria-label="Footprint timeframes"></nav>
      <a class="timeframe-link" href="/dom/__ACTIVE_TIMEFRAME__">DOM Timeline</a>
    </div>
  </header>
  <main id="app"></main>
  <script>
    const app = document.getElementById("app");
    const statusEl = document.getElementById("status");
    const linksEl = document.getElementById("timeframe-links");
    const binTickSelect = document.getElementById("bin-tick-select");
    const ACTIVE_TIMEFRAME = "__ACTIVE_TIMEFRAME__";
    const FOOTPRINT_TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"];
    const BIN_TICK_OPTIONS = [1, 2, 4, 8, 16];
    const DEFAULT_BIN_TICK_COUNT = 4;
    const FOOTPRINT_VISUAL_CONFIG = {
      verticalPaddingPercent: 0.08,
      minVerticalPaddingTicks: 10,
      minBinPixelHeight: 18,
      profileReservedWidthPx: 0,
      profileReservedRatio: 0,
      defaultVisibleCandles: 10,
      autoScaleEnabled: false,
      ...(() => {
        try {
          return JSON.parse(localStorage.getItem("footprint.visualConfig") || "{}");
        } catch {
          return {};
        }
      })(),
      ...(window.FOOTPRINT_VISUAL_CONFIG || {}),
    };
    const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };
    const charts = new Map();
    const refreshDelayMs = 2500;
    const VIEWPORT_REQUEST_DEBOUNCE_MS = 80;
    const FOOTPRINT_FETCH_OVERSCAN = 4;
    const ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN = __CONTRACT_SPIKE_THRESHOLD__;
    const FOOTPRINT_HIGH_SPIKE_SCORE_THRESHOLD = 12;
    const FOOTPRINT_SINGLE_SIDE_SPIKE_SCORE_THRESHOLD = 14;
    const FOOTPRINT_SINGLE_SIDE_SPIKE_STYLES = {
      SELL_ONLY: { fill: "#f2cc60" },
      BUY_ONLY: { fill: "#3fb950" },
    };
    const ABSORPTION_HIGHLIGHT_STYLES = {
      BUY: {
        fill: "rgba(255,45,149,.78)",
        zoneFill: "rgba(255,45,149,.18)",
        stroke: "rgba(255,45,149,.95)",
      },
      SELL: {
        fill: "rgba(63,185,80,.78)",
        zoneFill: "rgba(63,185,80,.18)",
        stroke: "rgba(63,185,80,.95)",
      },
    };
    let viewportMode = false;
    let earliestWindowStartMs = 0;
    let windowStartMs = 0;
    let windowEndMs = 0;
    let latestWindowEndMs = 0;
    let hasOlderData = false;
    let activeBinTickCount = DEFAULT_BIN_TICK_COUNT;

    function normalizedWheelDelta(event, pageSize) {
      const raw = Math.abs(event.deltaX) > Math.abs(event.deltaY)
        ? event.deltaX
        : event.deltaY;
      if (event.deltaMode === WheelEvent.DOM_DELTA_LINE) return raw * 16;
      if (event.deltaMode === WheelEvent.DOM_DELTA_PAGE) return raw * Math.max(1, pageSize);
      return raw;
    }

    try {
      const storedBinTicks = Number.parseInt(localStorage.getItem("footprint.binTicks") || "", 10);
      if (BIN_TICK_OPTIONS.includes(storedBinTicks)) activeBinTickCount = storedBinTicks;
    } catch {}

    function safeArray(value) { return Array.isArray(value) ? value : []; }
    function num(value) {
      const parsed = Number.parseFloat(value ?? "0");
      return Number.isFinite(parsed) ? parsed : 0;
    }
    function maybeNum(value) {
      const parsed = Number.parseFloat(value ?? "");
      return Number.isFinite(parsed) ? parsed : NaN;
    }
    function candleOpen(candle) { return Number(candle?.open_time_ms ?? candle?.open_time ?? 0); }
    function candleClose(candle) { return Number(candle?.close_time_ms ?? candle?.close_time ?? 0); }
    function ohlc(candle, key) { return candle?.ohlc?.[key] ?? candle?.[`${key}_price`] ?? ""; }
    function candleTriggerSignals(candle) { return safeArray(candle?.trigger_signals); }
    function signalMarkerShape(signal) {
      return String(signal?.marker_shape || (String(signal?.signal_type || "").startsWith("EXIT_") ? "SQUARE" : "ARROW")).trim().toUpperCase();
    }
    function triggerMarkerBounds(signal, centerX, yHigh, yLow) {
      const shape = signalMarkerShape(signal);
      const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
      const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
      const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
      if (shape === "SQUARE") {
        const size = 10;
        const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
        return { left: centerX - size / 2, right: centerX + size / 2, top: centerY - size / 2, bottom: centerY + size / 2 };
      }
      if (markerDirection === "DOWN" && markerColor === "RED") {
        const tipY = Math.max(3, yHigh - 4);
        const baseY = Math.max(1, tipY - 12);
        return { left: centerX - 8, right: centerX + 8, top: baseY - 3, bottom: tipY + 4 };
      }
      if (markerDirection === "UP" && markerColor === "GREEN") {
        const tipY = yLow + 4;
        const baseY = tipY + 12;
        return { left: centerX - 8, right: centerX + 8, top: tipY - 4, bottom: baseY + 3 };
      }
      return null;
    }
    function triggerMarkerAt(candle, centerX, yHigh, yLow, x, y) {
      for (const signal of [...candleTriggerSignals(candle)].reverse()) {
        if (signalMarkerShape(signal) !== "ARROW") continue;
        const bounds = triggerMarkerBounds(signal, centerX, yHigh, yLow);
        if (!bounds) continue;
        const pad = 4;
        if (x >= bounds.left - pad && x <= bounds.right + pad && y >= bounds.top - pad && y <= bounds.bottom + pad) {
          return signal;
        }
      }
      return null;
    }
    function triggerTimeLabel(ms) { return dateTimeLabel(Number(ms)); }
    function triggerTooltipText(signal) {
      const confirmationState = String(signal?.confirmation_state || "").trim() || "CONFIRMED";
      return `${signal?.signal_type || "TRIGGER"}
Reference candle ${triggerTimeLabel(signal?.reference_candle_time_ms) || "N/A"}
${confirmationState} ${triggerTimeLabel(signal?.confirmation_candle_time_ms || signal?.break_confirmed_candle_time_ms) || "N/A"}
Contract spike score ${fmtMaybe(signal?.contract_spike_score ?? signal?.spike_score, 3)}`;
    }
    function placeTooltip(tooltip, hover, plotW, plotH, fallbackWidth = 320, fallbackHeight = 100) {
      tooltip.style.opacity = "1";
      const tooltipWidth = tooltip.offsetWidth || fallbackWidth;
      const tooltipHeight = tooltip.offsetHeight || fallbackHeight;
      let left = hover.x + 12;
      if (left + tooltipWidth + 8 > plotW) left = hover.x - tooltipWidth - 12;
      left = Math.max(0, Math.min(left, Math.max(0, plotW - tooltipWidth - 8)));
      let top = hover.y + 12;
      if (top + tooltipHeight + 8 > plotH) top = hover.y - tooltipHeight - 12;
      top = Math.max(0, Math.min(top, Math.max(0, plotH - tooltipHeight - 8)));
      tooltip.style.left = `${left}px`;
      tooltip.style.top = `${top}px`;
    }
    function drawTriggerMarkers(ctx, candle, centerX, yHigh, yLow) {
      for (const signal of candleTriggerSignals(candle)) {
        const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
        const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
        const markerShape = signalMarkerShape(signal);
        const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
        const fillColor = markerColor === "GREEN" ? "#3fb950" : "#f85149";
        if (markerShape === "SQUARE") {
          const size = 10;
          const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
          ctx.fillStyle = fillColor;
          ctx.fillRect(centerX - size / 2, centerY - size / 2, size, size);
          ctx.strokeStyle = "#0d1117";
          ctx.lineWidth = 1.5;
          ctx.strokeRect(centerX - size / 2, centerY - size / 2, size, size);
        } else if (markerDirection === "DOWN" && markerColor === "RED") {
          const tipY = Math.max(3, yHigh - 4);
          const baseY = Math.max(1, tipY - 12);
          ctx.fillStyle = "#f85149";
          ctx.beginPath();
          ctx.moveTo(centerX, tipY);
          ctx.lineTo(centerX - 7, baseY);
          ctx.lineTo(centerX + 7, baseY);
          ctx.closePath();
          ctx.fill();
        } else if (markerDirection === "UP" && markerColor === "GREEN") {
          const tipY = yLow + 4;
          const baseY = tipY + 12;
          ctx.fillStyle = "#3fb950";
          ctx.beginPath();
          ctx.moveTo(centerX, tipY);
          ctx.lineTo(centerX - 7, baseY);
          ctx.lineTo(centerX + 7, baseY);
          ctx.closePath();
          ctx.fill();
        }
      }
    }
    function candleKey(candle) { return String(candleOpen(candle)); }
    function sessionTimeframe(session) { return String(session?.timeframe || ACTIVE_TIMEFRAME).trim().toUpperCase(); }
    function sessionKey(session) {
      const symbol = session.mt5_symbol || session.binance_symbol || session.symbol || "UNKNOWN";
      return `${symbol}|${sessionTimeframe(session)}`;
    }
    function binIndex(bin, size) {
      const explicit = Number(bin?.index ?? bin?.bin_index);
      if (Number.isFinite(explicit)) return explicit;
      const low = maybeNum(bin?.bin_low ?? bin?.low);
      return size > 0 && Number.isFinite(low) ? Math.floor((low + size * 1e-9) / size) : NaN;
    }
    function priceForBinIndex(index, size) { return index * size; }
    function rowCenterPriceForBinIndex(index, size) { return (index + 0.5) * size; }
    function binPayloadField(bin, key) { return bin?.l2?.[key] ?? bin?.[key]; }
    function binTotal(bin) { return num(binPayloadField(bin, "total_contracts") ?? binPayloadField(bin, "total_volume")); }
    function binBuy(bin) { return num(binPayloadField(bin, "buy_contracts") ?? binPayloadField(bin, "ask_traded_contracts") ?? binPayloadField(bin, "ask_traded_volume") ?? binPayloadField(bin, "buy_volume")); }
    function binSell(bin) { return num(binPayloadField(bin, "sell_contracts") ?? binPayloadField(bin, "bid_traded_contracts") ?? binPayloadField(bin, "bid_traded_volume") ?? binPayloadField(bin, "sell_volume")); }
    function binDelta(bin) { return num(binPayloadField(bin, "horizontal_contract_delta") ?? binPayloadField(bin, "contract_delta") ?? binPayloadField(bin, "horizontal_delta") ?? binPayloadField(bin, "delta")); }
    function binDuration(bin) { return num(bin?.l2?.duration ?? (bin?.duration_ms === undefined ? 0 : num(bin?.duration_ms) / 1000)); }
    function binBuyPressure(bin) { return num(binPayloadField(bin, "buy_diagonal_contract_ratio") ?? binPayloadField(bin, "buy_diagonal_imbalance_ratio")); }
    function binSellPressure(bin) { return num(binPayloadField(bin, "sell_diagonal_contract_ratio") ?? binPayloadField(bin, "sell_diagonal_imbalance_ratio")); }
    function binBuyDiagonalDelta(bin) { return maybeNum(binPayloadField(bin, "buy_diagonal_contract_delta")); }
    function binSellDiagonalDelta(bin) { return maybeNum(binPayloadField(bin, "sell_diagonal_contract_delta")); }
    function binFlag(bin, key) { return binPayloadField(bin, key) === true; }
    function binAbnormalContract(bin) { return binFlag(bin, "abnormal_contract") || binFlag(bin, "abnormal_volume"); }
    function binAbnormalVolume(bin) { return binAbnormalContract(bin); }
    function binAbnormalBuyImbalance(bin) { return binFlag(bin, "abnormal_buy_imbalance"); }
    function binAbnormalSellImbalance(bin) { return binFlag(bin, "abnormal_sell_imbalance"); }
    function binContractSpikeScore(bin) { return maybeNum(binPayloadField(bin, "contract_spike_score")); }
    function binHasHighContractSpikeScore(bin) {
      const score = binContractSpikeScore(bin);
      const buyContracts = binBuy(bin);
      const sellContracts = binSell(bin);
      const hasOneSidedContracts = (
        (buyContracts === 0 && sellContracts > 0)
        || (sellContracts === 0 && buyContracts > 0)
      );
      return Number.isFinite(score) && score > FOOTPRINT_HIGH_SPIKE_SCORE_THRESHOLD && hasOneSidedContracts;
    }
    function binDominantSide(bin) {
      const buyContracts = binBuy(bin);
      const sellContracts = binSell(bin);
      if (buyContracts > sellContracts) return "BUY";
      if (sellContracts > buyContracts) return "SELL";
      return "NONE";
    }
    function binDominantQuantity(bin) { return maybeNum(binPayloadField(bin, "dominant_side_contracts") ?? binPayloadField(bin, "dominant_side_volume")); }
    function binEfficiency(bin) { return maybeNum(binPayloadField(bin, "dominant_side_efficiency")); }
    function binEfficiencyPercentile(bin) { return maybeNum(binPayloadField(bin, "efficiency_percentile")); }
    function binEfficiencyZscore(bin) { return maybeNum(binPayloadField(bin, "efficiency_zscore")); }
    function normalizedTickIndex(price, tickSize) {
      if (!Number.isFinite(price) || !Number.isFinite(tickSize) || tickSize <= 0) return NaN;
      return Math.round(price / tickSize);
    }
    function binPriceLevel(bin, size) {
      const explicit = maybeNum(binPayloadField(bin, "price") ?? bin?.bin_low ?? bin?.low);
      return Number.isFinite(explicit) ? explicit : binPrice(bin, size);
    }
    function binTickIndex(bin, size, tickSize) {
      return normalizedTickIndex(binPriceLevel(bin, size), tickSize);
    }
    function binDirectionalEfficiency(candle, bin, size, tickSize, side) {
      const currentContracts = side === "BUY" ? binBuy(bin) : binSell(bin);
      if (!(currentContracts > 0)) return NaN;
      const currentTick = binTickIndex(bin, size, tickSize);
      if (!Number.isFinite(currentTick)) return NaN;
      let referenceTick = NaN;
      for (const candidate of safeArray(candle?.bins)) {
        const candidateTick = binTickIndex(candidate, size, tickSize);
        if (!Number.isFinite(candidateTick)) continue;
        if (side === "BUY") {
          if (
            candidateTick < currentTick
            && binSell(candidate) > 0
            && (!Number.isFinite(referenceTick) || candidateTick > referenceTick)
          ) {
            referenceTick = candidateTick;
          }
        } else if (
          candidateTick > currentTick
          && binBuy(candidate) > 0
          && (!Number.isFinite(referenceTick) || candidateTick < referenceTick)
        ) {
          referenceTick = candidateTick;
        }
      }
      if (!Number.isFinite(referenceTick)) {
        const fallbackPrice = side === "BUY"
          ? maybeNum(ohlc(candle, "low"))
          : maybeNum(ohlc(candle, "high"));
        referenceTick = normalizedTickIndex(fallbackPrice, tickSize);
      }
      if (!Number.isFinite(referenceTick)) return NaN;
      const distanceTicks = Math.abs(currentTick - referenceTick);
      return distanceTicks / currentContracts;
    }
    function binBuyDirectionalEfficiency(candle, bin, size, tickSize) {
      return binDirectionalEfficiency(candle, bin, size, tickSize, "BUY");
    }
    function binSellDirectionalEfficiency(candle, bin, size, tickSize) {
      return binDirectionalEfficiency(candle, bin, size, tickSize, "SELL");
    }
    function binPrice(bin, size) {
      const index = binIndex(bin, size);
      return Number.isFinite(index) ? priceForBinIndex(index, size) : NaN;
    }
    function binRowPrice(bin, size) {
      const index = binIndex(bin, size);
      return Number.isFinite(index) ? rowCenterPriceForBinIndex(index, size) : NaN;
    }
    function candleRegionDelta(candle, region, size) {
      const open = maybeNum(ohlc(candle, "open"));
      const high = maybeNum(ohlc(candle, "high"));
      const low = maybeNum(ohlc(candle, "low"));
      const close = maybeNum(ohlc(candle, "close"));
      if (![open, high, low, close].every(Number.isFinite)) return NaN;
      const bodyLow = Math.min(open, close);
      const bodyHigh = Math.max(open, close);
      return safeArray(candle?.bins).reduce((sum, bin) => {
        const price = binPrice(bin, size);
        if (!Number.isFinite(price)) return sum;
        const inRegion = (
          (region === "BODY" && bodyLow <= price && price <= bodyHigh)
          || (region === "UPPER_WICK" && bodyHigh < price && price <= high)
          || (region === "LOWER_WICK" && low <= price && price < bodyLow)
        );
        return inRegion ? sum + binDelta(bin) : sum;
      }, 0);
    }
    function candleBinMetric(candle, field, size, tickSize) {
      const tradedBins = safeArray(candle?.bins).filter(bin => binTotal(bin) > 0);
      let valueForBin = null;
      if (field === "max_buy_diagonal_ratio" || field === "sum_buy_diagonal_ratio") {
        valueForBin = binBuyPressure;
      } else if (field === "max_sell_diagonal_ratio" || field === "sum_sell_diagonal_ratio") {
        valueForBin = binSellPressure;
      } else if (field === "max_contract_spike_score") {
        valueForBin = binContractSpikeScore;
      } else if (field === "max_buy_directional_efficiency") {
        valueForBin = bin => binBuyDirectionalEfficiency(candle, bin, size, tickSize);
      } else if (field === "max_sell_directional_efficiency") {
        valueForBin = bin => binSellDirectionalEfficiency(candle, bin, size, tickSize);
      }
      if (!valueForBin) return NaN;
      const values = tradedBins.map(valueForBin).filter(Number.isFinite);
      if (!values.length) return NaN;
      if (field.startsWith("max_")) return Math.max(...values);
      if (field.startsWith("sum_")) return values.reduce((sum, value) => sum + value, 0);
      return NaN;
    }
    function footprintDeltaTableValue(candle, field, size, tickSize) {
      if (field === "body_delta") return candleRegionDelta(candle, "BODY", size);
      if (field === "upper_wick_delta") return candleRegionDelta(candle, "UPPER_WICK", size);
      if (field === "lower_wick_delta") return candleRegionDelta(candle, "LOWER_WICK", size);
      if (
        field === "max_buy_diagonal_ratio"
        || field === "max_sell_diagonal_ratio"
        || field === "sum_buy_diagonal_ratio"
        || field === "sum_sell_diagonal_ratio"
        || field === "max_contract_spike_score"
        || field === "max_buy_directional_efficiency"
        || field === "max_sell_directional_efficiency"
      ) {
        return candleBinMetric(candle, field, size, tickSize);
      }
      const value = candle?.[field];
      return value === null || value === undefined ? NaN : num(value);
    }
    function binIsInRequiredWick(candle, bin, size, side) {
      const open = maybeNum(ohlc(candle, "open"));
      const high = maybeNum(ohlc(candle, "high"));
      const low = maybeNum(ohlc(candle, "low"));
      const close = maybeNum(ohlc(candle, "close"));
      const price = binPrice(bin, size);
      if (![open, high, low, close, price].every(Number.isFinite)) return false;
      const bodyLow = Math.min(open, close);
      const bodyHigh = Math.max(open, close);
      if (side === "BUY") return bodyHigh <= price && price <= high;
      if (side === "SELL") return low <= price && price <= bodyLow;
      return false;
    }
    function binAbsorptionHighlightStyle(candle, bin, size) {
      const spikeScore = binContractSpikeScore(bin);
      const side = binDominantSide(bin);
      if (!Number.isFinite(spikeScore) || spikeScore < ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN) return null;
      if (!binIsInRequiredWick(candle, bin, size, side)) return null;
      return ABSORPTION_HIGHLIGHT_STYLES[side] || null;
    }
    function binInSingleSideSpikeRegion(candle, bin, size, kind) {
      const open = maybeNum(ohlc(candle, "open"));
      const high = maybeNum(ohlc(candle, "high"));
      const low = maybeNum(ohlc(candle, "low"));
      const close = maybeNum(ohlc(candle, "close"));
      const price = binRowPrice(bin, size);
      if (![open, high, low, close, price].every(Number.isFinite) || high <= low) return false;
      const bullish = close > open;
      const bearish = close < open;
      if (!bullish && !bearish) return false;
      const range = high - low;
      const lowerThirdEnd = low + range / 3;
      const upperThirdStart = high - range / 3;
      if (kind === "SELL_ONLY") {
        if (bearish) return upperThirdStart <= price && price <= high;
        if (bullish) return low <= price && price <= lowerThirdEnd;
      }
      if (kind === "BUY_ONLY") {
        if (bullish) return lowerThirdEnd <= price && price <= high;
        if (bearish) return low <= price && price <= upperThirdStart;
      }
      return false;
    }
    function binSingleSideSpikeStyle(candle, bin, size) {
      const spikeScore = binContractSpikeScore(bin);
      if (!Number.isFinite(spikeScore) || spikeScore <= FOOTPRINT_SINGLE_SIDE_SPIKE_SCORE_THRESHOLD) return null;
      const buyContracts = binBuy(bin);
      const sellContracts = binSell(bin);
      if (
        buyContracts === 0
        && sellContracts > 0
        && binInSingleSideSpikeRegion(candle, bin, size, "SELL_ONLY")
      ) {
        return FOOTPRINT_SINGLE_SIDE_SPIKE_STYLES.SELL_ONLY;
      }
      if (
        buyContracts > 0
        && sellContracts === 0
        && binInSingleSideSpikeRegion(candle, bin, size, "BUY_ONLY")
      ) {
        return FOOTPRINT_SINGLE_SIDE_SPIKE_STYLES.BUY_ONLY;
      }
      return null;
    }
    function candleHasContractAtBinIndex(candle, targetIndex, size) {
      return safeArray(candle?.bins).some(bin => (
        binTotal(bin) > 0
        && binIndex(bin, size) === targetIndex
      ));
    }
    function binAtPrice(candle, price, size) {
      if (!Number.isFinite(price) || size <= 0) return null;
      const targetIndex = Math.floor(price / size);
      return safeArray(candle?.bins).find(bin => binIndex(bin, size) === targetIndex) || null;
    }
    function footprintFontSize(rowHeight) {
      if (rowHeight < 12) return 0;
      if (rowHeight < 16) return 12;
      if (rowHeight <= 22) return 14;
      return 16;
    }
    function drawBinQuantityPair(ctx, bin, leftX, rightX, y, rowHeight = 12) {
      const width = rightX - leftX;
      const buyColumnX = leftX + width * 0.27;
      const sellColumnX = leftX + width * 0.73;
      const fontSize = footprintFontSize(rowHeight);
      if (fontSize <= 0) return;
      ctx.textBaseline = "middle";
      ctx.textAlign = "center";
      ctx.font = `150 ${fontSize}px Arial Narrow, Segoe UI, Arial`;
      const textColor = binHasHighContractSpikeScore(bin) ? "#000000" : null;
      ctx.fillStyle = textColor || "#9be9a8";
      ctx.fillText(fmt(binBuy(bin), 0), buyColumnX, y);
      ctx.font = `150 ${fontSize}px Arial Narrow, Segoe UI, Arial`;
      ctx.fillStyle = textColor || "#ffb3ad";
      ctx.fillText(fmt(binSell(bin), 0), sellColumnX, y);
    }
    function fmt(value, places = 2) { return num(value).toFixed(places); }
    function fmtMaybe(value, places = 2) {
      const parsed = Number.parseFloat(value ?? "");
      return Number.isFinite(parsed) ? parsed.toFixed(places) : "N/A";
    }
    function fmtPrice(value) {
      const abs = Math.abs(num(value));
      if (abs >= 1000) return num(value).toFixed(1);
      if (abs >= 1) return num(value).toFixed(3);
      return num(value).toFixed(6);
    }
    function signed(value, places = 2) {
      const n = num(value);
      return `${n >= 0 ? "+" : ""}${n.toFixed(places)}`;
    }
    function timeLabel(ms) {
      if (!ms) return "";
      return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }
    function dateTimeLabel(ms) {
      if (!ms) return "";
      return new Date(ms).toLocaleString([], {
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
    function timeframeLinks() {
      binTickSelect.value = String(activeBinTickCount);
      linksEl.innerHTML = FOOTPRINT_TIMEFRAMES.map(timeframe => {
        const active = timeframe === ACTIVE_TIMEFRAME ? " active" : "";
        return `<a class="timeframe-link${active}" href="/footprint/${timeframe}">${timeframe}</a>`;
      }).join("");
    }
    function clearAllFootprintChartCaches() {
      for (const chart of charts.values()) {
        chart.candleMap.clear();
        chart.candles = [];
        chart.cachedWindows = [];
        chart.liveCandle = null;
        chart.viewportInitialized = false;
        chart.pinnedViewportEndMs = 0;
        chart.horizontalOffsetPx = 0;
        chart.lastClosedOpenTime = 0;
        chart.verticalPinned = true;
        chart.priceCenterIndex = NaN;
      }
    }
    function footprintCandleWidth() {
      const availableWidth = Math.max(1, window.innerWidth - 112);
      const targetVisible = Math.max(1, Number(FOOTPRINT_VISUAL_CONFIG.defaultVisibleCandles) || 10);
      return Math.max(88, Math.min(180, availableWidth / targetVisible));
    }

    class CanvasFootprintChart {
      constructor(section, session) {
        this.section = section;
        this.host = section.querySelector(".chart-host");
        this.canvas = section.querySelector("canvas");
        this.tooltip = section.querySelector(".tooltip");
        this.scrollbar = section.querySelector(".history-scrollbar");
        this.scrollbarContent = section.querySelector(".history-scrollbar-content");
        this.ctx = this.canvas.getContext("2d");
        this.candles = [];
        this.candleMap = new Map();
        this.cachedWindows = [];
        this.liveCandle = null;
        this.fixedSize = 0;
        this.priceStep = 0;
        this.displayLimit = 0;
        this.viewportInitialized = false;
        this.pinnedViewportEndMs = 0;
        this.candleWidth = footprintCandleWidth();
        this.autoScaleCandleWidth = true;
        this.minBinPixelHeight = Math.max(1, Number(FOOTPRINT_VISUAL_CONFIG.minBinPixelHeight) || 18);
        this.binPixelHeight = this.minBinPixelHeight;
        this.effectiveBinPixelHeight = this.minBinPixelHeight;
        this.binTickCount = activeBinTickCount;
        this.priceCenterIndex = NaN;
        this.verticalPinned = true;
        this.scaleMetrics = null;
        this.currentRange = null;
        this.quantityUnit = "";
        this.hover = null;
        this.horizontalOffsetPx = 0;
        this.syncingScrollbar = false;
        this.programmaticScrollbarLeft = NaN;
        this.programmaticScrollbarUntil = 0;
        this.scrollbarInteracting = false;
        this.scrollbarInteractionTimer = null;
        this.scrollbarPointerActive = false;
        this.scrollbarUserInputUntil = 0;
        this.lastClosedOpenTime = 0;
        this.lastLayoutSignature = "";
        this.attachEvents();
        this.updateSessionHeader(session);
      }
      updateSessionHeader(session) {
        const title = this.section.querySelector(".session-title strong");
        const meta = this.section.querySelector(".session-title span");
        const providerSymbol = session.provider_symbol || session.binance_symbol || session.symbol || "";
        const provider = session.market_provider || (session.binance_symbol ? "BINANCE" : "");
        this.quantityUnit = String(session.quantity_unit || "").trim().toUpperCase();
        if (title) title.textContent = `${session.mt5_symbol || session.symbol || ""} -> ${providerSymbol}`;
        const unit = this.quantityUnit === "CONTRACTS" ? " / contracts" : "";
        if (meta) meta.textContent = `timeframe ${session.timeframe || ACTIVE_TIMEFRAME} / ${provider} ${session.interval || ""}${unit}`;
      }
      attachEvents() {
        const armScrollbarInput = (durationMs = 500) => {
          this.scrollbarUserInputUntil = Date.now() + durationMs;
        };
        this.scrollbar.addEventListener("pointerdown", () => {
          this.scrollbarPointerActive = true;
          armScrollbarInput(2_000);
        });
        window.addEventListener("pointerup", () => {
          if (!this.scrollbarPointerActive) return;
          this.scrollbarPointerActive = false;
          armScrollbarInput(250);
        });
        this.scrollbar.addEventListener("wheel", () => {
          armScrollbarInput(500);
        }, { passive: true });
        this.scrollbar.addEventListener("keydown", event => {
          if (
            event.key === "ArrowLeft"
            || event.key === "ArrowRight"
            || event.key === "PageUp"
            || event.key === "PageDown"
            || event.key === "Home"
            || event.key === "End"
          ) {
            armScrollbarInput(500);
          }
        });
        this.scrollbar.addEventListener("scroll", () => {
          const programmaticScroll = (
            Number.isFinite(this.programmaticScrollbarLeft)
            && Date.now() <= this.programmaticScrollbarUntil
            && Math.abs(this.scrollbar.scrollLeft - this.programmaticScrollbarLeft) <= 2
          );
          if (
            this.syncingScrollbar
            || programmaticScroll
            || this.scrollbar.classList.contains("disabled")
          ) {
            return;
          }
          if (
            !this.scrollbarPointerActive
            && Date.now() > this.scrollbarUserInputUntil
          ) {
            return;
          }
          const maxScroll = Math.max(0, this.scrollbar.scrollWidth - this.scrollbar.clientWidth);
          if (maxScroll <= 0) return;
          this.scrollbarInteracting = true;
          if (this.scrollbarInteractionTimer) clearTimeout(this.scrollbarInteractionTimer);
          this.scrollbarInteractionTimer = setTimeout(() => {
            this.scrollbarInteracting = false;
            this.syncScrollbar();
          }, 140);
          const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
          const limit = this.visibleCapacity();
          const earliest = earliestWindowStartMs || windowStartMs || 0;
          const latest = latestWindowEndMs || windowEndMs || 0;
          const maxStart = Math.max(earliest, latest - limit * intervalMs);
          const ratio = this.scrollbar.scrollLeft / maxScroll;
          const selectedStartMs = Math.round(
            (earliest + ratio * (maxStart - earliest)) / intervalMs,
          ) * intervalMs;
          cancelLiveRefresh();
          requestViewportWindow(selectedStartMs + limit * intervalMs, limit);
        });
        this.host.addEventListener("wheel", event => {
          event.preventDefault();
          const all = this.renderCandles();
          if (!all.length) return;
          const rect = this.canvas.getBoundingClientRect();
          const pointerX = event.clientX - rect.left;
          const overPriceAxis = pointerX >= rect.width - 88;
          if (event.shiftKey) {
            const visibleRows = Math.max(8, Math.floor(Math.max(1, rect.height - 34) / this.binPixelHeight));
            const direction = Math.sign(event.deltaY || event.deltaX);
            const fallbackCenter = num(ohlc(all[all.length - 1], "close")) / Math.max(this.fixedSize, 1e-9);
            this.priceCenterIndex = (
              Number.isFinite(this.priceCenterIndex) ? this.priceCenterIndex : fallbackCenter
            ) - direction * Math.max(1, Math.round(visibleRows * 0.12));
            this.verticalPinned = false;
          } else if (event.altKey || overPriceAxis) {
            const next = this.binPixelHeight * (event.deltaY > 0 ? 0.9 : 1.1);
            this.binPixelHeight = Math.max(this.minBinPixelHeight, Math.min(64, next));
            this.verticalPinned = true;
          } else if (event.ctrlKey) {
            const next = this.candleWidth * (event.deltaY > 0 ? 0.9 : 1.1);
            this.candleWidth = Math.max(88, Math.min(180, next));
            this.autoScaleCandleWidth = false;
            scheduleViewportResize(
              this.currentViewportEndTimeMs() || windowEndMs || null,
              this.visibleCapacity(),
            );
          } else {
            cancelLiveRefresh();
            const primaryDelta = normalizedWheelDelta(event, rect.width);
            const candleSteps = this.scrollByPixels(primaryDelta);
            if (candleSteps !== 0) {
              
              requestViewportWindow(
                this.currentViewportEndTimeMs(),
                this.visibleCapacity(),
                false,
              );
            }
            this.syncScrollbar();
            return;
          }
          this.draw();
        }, { passive: false });
        this.host.addEventListener("mousemove", event => {
          const rect = this.canvas.getBoundingClientRect();
          this.hover = { x: event.clientX - rect.left, y: event.clientY - rect.top };
          this.draw();
        });
        this.host.addEventListener("dblclick", event => {
          const rect = this.canvas.getBoundingClientRect();
          const pointerX = event.clientX - rect.left;
          if (pointerX < rect.width - 88) return;
          this.verticalPinned = true;
          this.priceCenterIndex = NaN;
          this.binPixelHeight = this.minBinPixelHeight;
          this.draw();
        });
        this.host.addEventListener("mouseleave", () => {
          this.hover = null;
          this.tooltip.style.opacity = "0";
          this.draw();
        });
        window.addEventListener("resize", () => {
          if (this.autoScaleCandleWidth) this.candleWidth = footprintCandleWidth();
          this.draw();
          this.syncScrollbar();
        });
      }
      currentViewportEndTimeMs() {
        if (this.viewportInitialized && this.pinnedViewportEndMs > 0) {
          return this.pinnedViewportEndMs;
        }
        const all = this.renderCandles();
        return all.length
          ? candleOpen(all[all.length - 1]) + TIMEFRAME_MS[ACTIVE_TIMEFRAME]
          : 0;
      }
      viewportEndTimeForIndex(all, viewEnd) {
        if (!all.length) return 0;
        const endIndex = Math.max(0, Math.min(viewEnd, all.length));
        if (endIndex < all.length) return candleOpen(all[endIndex]);
        return candleOpen(all[all.length - 1]) + TIMEFRAME_MS[ACTIVE_TIMEFRAME];
      }
      positionViewport(endTimeMs, candleLimit = this.visibleCapacity()) {
        const targetEnd = Number(endTimeMs);
        if (!Number.isFinite(targetEnd) || targetEnd <= 0) return;
        this.viewportInitialized = true;
        this.pinnedViewportEndMs = targetEnd;
      }
      canPreviewViewport(endTimeMs, candleLimit = this.visibleCapacity()) {
        const all = this.renderCandles();
        if (!all.length) return false;
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        const targetEnd = Number(endTimeMs);
        const coverage = this.cachedWindows.find(window => (
          targetEnd > window.startMs
          && targetEnd <= window.endMs + intervalMs
        ));
        if (!coverage) return false;
        const available = all.filter(candle => {
          const openTime = candleOpen(candle);
          return openTime >= coverage.startMs && openTime < targetEnd;
        }).length;
        return available >= Math.max(1, candleLimit);
      }
      rememberCachedWindow(session) {
        const candles = safeArray(session?.candles);
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        let startMs = Number(session?.window_start_ms);
        let endMs = Number(session?.window_end_ms);
        if ((!Number.isFinite(startMs) || startMs <= 0) && candles.length) {
          startMs = candleOpen(candles[0]);
        }
        if ((!Number.isFinite(endMs) || endMs <= startMs) && candles.length) {
          endMs = candleOpen(candles[candles.length - 1]) + intervalMs;
        }
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
          return;
        }
        const windows = [
          ...this.cachedWindows,
          { startMs, endMs },
        ].sort((left, right) => left.startMs - right.startMs);
        const merged = [];
        for (const window of windows) {
          const previous = merged[merged.length - 1];
          if (previous && window.startMs <= previous.endMs + intervalMs) {
            previous.endMs = Math.max(previous.endMs, window.endMs);
          } else {
            merged.push({ ...window });
          }
        }
        this.cachedWindows = merged;
      }
      cacheSession(session, displayLimit = this.displayLimit) {
        this.rememberCachedWindow(session);
        this.updateSessionHeader(session);
        const parsedDisplayLimit = Number.parseInt(displayLimit, 10);
        this.displayLimit = Number.isFinite(parsedDisplayLimit) ? parsedDisplayLimit : this.displayLimit || 0;
        const size = num(session?.fixed_bin_size);
        if (size > 0) this.fixedSize = size;
        const priceStep = num(session?.price_step);
        if (priceStep > 0) this.priceStep = priceStep;
        const binTickCount = Number.parseInt(session?.bin_tick_count, 10);
        if (BIN_TICK_OPTIONS.includes(binTickCount)) this.binTickCount = binTickCount;
        for (const candle of safeArray(session?.candles)) {
          const key = candleKey(candle);
          if (!key || key === "0") continue;
          this.candleMap.set(key, candle);
        }
        this.candles = [...this.candleMap.values()].sort((a, b) => candleOpen(a) - candleOpen(b));
        const clientCacheLimit = Math.max(500, this.displayLimit * 20);
        if (this.candles.length > clientCacheLimit) {
          const targetEnd = (
            this.pinnedViewportEndMs
            || requestedWindowEndMs
            || windowEndMs
            || Infinity
          );
          const targetIndex = this.candles.findIndex(candle => candleOpen(candle) >= targetEnd);
          const center = targetIndex < 0 ? this.candles.length : targetIndex;
          const start = Math.max(0, Math.min(
            this.candles.length - clientCacheLimit,
            center - Math.floor(clientCacheLimit / 2),
          ));
          this.candles = this.candles.slice(start, start + clientCacheLimit);
          this.candleMap = new Map(this.candles.map(item => [candleKey(item), item]));
        }
        this.lastClosedOpenTime = this.candles.reduce((max, candle) => Math.max(max, candleOpen(candle)), this.lastClosedOpenTime || 0);
      }
      mergeSession(session, displayLimit) {
        this.cacheSession(session, displayLimit);
        this.liveCandle = session?.live_candle || null;
        const all = this.renderCandles();
        if (!this.viewportInitialized && all.length) {
          this.viewportInitialized = true;
          this.pinnedViewportEndMs = (
            candleOpen(all[all.length - 1])
            + TIMEFRAME_MS[ACTIVE_TIMEFRAME]
          );
        }
        this.draw();
        this.syncScrollbar();
      }
      renderCandles() {
        if (!this.liveCandle) return this.candles;
        const liveOpen = candleOpen(this.liveCandle);
        const closedWithoutLive = this.candles.filter(candle => candleOpen(candle) !== liveOpen);
        return [...closedWithoutLive, this.liveCandle].sort((a, b) => candleOpen(a) - candleOpen(b));
      }
      visibleCapacity() {
        const rect = this.host.getBoundingClientRect();
        const axisWidth = 88;
        return Math.max(1, Math.floor(Math.max(1, rect.width - axisWidth) / this.candleWidth));
      }
      previewViewport(endTimeMs, candleLimit = this.visibleCapacity(), redraw = true) {
        this.positionViewport(endTimeMs, candleLimit);
        if (redraw) this.draw();
      }
      scrollByPixels(deltaPixels) {
        if (!Number.isFinite(deltaPixels) || deltaPixels === 0) return 0;
        const visible = this.visibleCapacity();
        const scaledDelta = Math.max(
          -48,
          Math.min(48, deltaPixels * 0.7),
        );
        this.horizontalOffsetPx -= scaledDelta;
        let candleDelta = 0;
        while (this.horizontalOffsetPx >= this.candleWidth) {
          this.horizontalOffsetPx -= this.candleWidth;
          candleDelta -= 1;
        }
        while (this.horizontalOffsetPx <= -this.candleWidth) {
          this.horizontalOffsetPx += this.candleWidth;
          candleDelta += 1;
        }
        if (candleDelta !== 0 || !this.viewportInitialized) {
          const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
          const earliest = earliestWindowStartMs || windowStartMs || 0;
          const latest = latestWindowEndMs || windowEndMs || 0;
          const earliestEnd = earliest > 0 ? earliest + visible * intervalMs : 0;
          let nextEnd = this.currentViewportEndTimeMs() + candleDelta * intervalMs;
          if (earliestEnd > 0) nextEnd = Math.max(earliestEnd, nextEnd);
          if (latest > 0) nextEnd = Math.min(latest, nextEnd);
          this.positionViewport(nextEnd, visible);
        }
        if (this.canPreviewViewport(this.currentViewportEndTimeMs(), visible)) {
          this.draw();
        }
        return candleDelta;
      }
      cachedOpenTimes(endTimeMs, candleLimit = this.visibleCapacity()) {
        const targetEnd = Number(endTimeMs);
        return this.renderCandles()
          .filter(candle => candleOpen(candle) < targetEnd)
          .slice(-Math.max(1, candleLimit))
          .map(candle => candleOpen(candle));
      }
      resizeCanvas() {
        const rect = this.host.getBoundingClientRect();
        const ratio = window.devicePixelRatio || 1;
        const width = Math.max(1, Math.floor(rect.width));
        const height = Math.max(320, Math.floor(rect.height));
        if (this.canvas.width !== Math.floor(width * ratio) || this.canvas.height !== Math.floor(height * ratio)) {
          this.canvas.width = Math.floor(width * ratio);
          this.canvas.height = Math.floor(height * ratio);
          this.canvas.style.width = `${width}px`;
          this.canvas.style.height = `${height}px`;
        }
        this.ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
        return { width, height };
      }
      syncScrollbar() {
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        const limit = this.visibleCapacity();
        const earliest = earliestWindowStartMs || windowStartMs || 0;
        const latest = latestWindowEndMs || windowEndMs || 0;
        const maxStart = Math.max(earliest, latest - limit * intervalMs);
        const targetEnd = (
          requestedWindowEndMs
          || this.currentViewportEndTimeMs()
          || windowEndMs
          || latest
        );
        const currentStart = Math.max(
          earliest,
          Math.min(maxStart, targetEnd - limit * intervalMs),
        );
        const disabled = earliest <= 0 || maxStart <= earliest;
        this.scrollbar.classList.toggle("disabled", disabled);
        this.scrollbar.setAttribute("aria-disabled", disabled ? "true" : "false");
        const totalCandles = Math.max(limit, Math.ceil((latest - earliest) / intervalMs));
        const contentWidth = disabled
          ? this.scrollbar.clientWidth
          : Math.min(16_000_000, Math.max(this.scrollbar.clientWidth + 1, totalCandles * this.candleWidth));
        this.scrollbarContent.style.width = `${Math.ceil(contentWidth)}px`;
        if (disabled || this.scrollbarInteracting) return;
        const maxScroll = Math.max(0, this.scrollbar.scrollWidth - this.scrollbar.clientWidth);
        const ratio = maxStart > earliest ? (currentStart - earliest) / (maxStart - earliest) : 0;
        const nextScrollLeft = ratio * maxScroll;
        this.syncingScrollbar = true;
        this.programmaticScrollbarLeft = nextScrollLeft;
        this.programmaticScrollbarUntil = Date.now() + 250;
        this.scrollbar.scrollLeft = nextScrollLeft;
        requestAnimationFrame(() => { this.syncingScrollbar = false; });
      }
      visibleCandleItems(all, plotW) {
        const visible = this.visibleCapacity();
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        const targetEnd = this.currentViewportEndTimeMs();
        const coverage = this.cachedWindows.find(window => (
          targetEnd > window.startMs
          && targetEnd <= window.endMs + intervalMs
        ));
        if (!coverage) return [];
        const visibleCandles = all
          .filter(candle => {
            const openTime = candleOpen(candle);
            return openTime >= coverage.startMs && openTime < targetEnd;
          })
          .slice(-visible);
        if (!visibleCandles.length) return [];
        const byOpenTime = new Map(all.map(candle => [candleOpen(candle), candle]));
        const firstOpen = candleOpen(visibleCandles[0]);
        const lastOpen = candleOpen(visibleCandles[visibleCandles.length - 1]);
        const previous = byOpenTime.get(firstOpen - intervalMs);
        const next = byOpenTime.get(lastOpen + intervalMs);
        const renderItems = [];
        if (
          previous
          && candleOpen(previous) >= coverage.startMs
        ) {
          renderItems.push({ candle: previous, slot: -1 });
        }
        visibleCandles.forEach((candle, slot) => {
          renderItems.push({ candle, slot });
        });
        if (
          next
          && candleOpen(next) < coverage.endMs
          && candleOpen(next) < targetEnd + intervalMs
        ) {
          renderItems.push({ candle: next, slot: visibleCandles.length });
        }
        return renderItems
          .map(item => ({
            candle: item.candle,
            x: item.slot * this.candleWidth + this.horizontalOffsetPx,
          }))
          .filter(item => item.x + this.candleWidth > 0 && item.x < plotW);
      }
      computePriceRange(candles, size, plotH) {
        let dataMinPrice = Infinity;
        let dataMaxPrice = -Infinity;
        for (const candle of candles) {
          for (const key of ["open", "high", "low", "close"]) {
            const value = maybeNum(ohlc(candle, key));
            if (Number.isFinite(value)) {
              dataMinPrice = Math.min(dataMinPrice, value);
              dataMaxPrice = Math.max(dataMaxPrice, value);
            }
          }
          for (const bin of safeArray(candle?.bins)) {
            const index = binIndex(bin, size);
            if (!Number.isFinite(index)) continue;
            dataMinPrice = Math.min(dataMinPrice, index * size);
            dataMaxPrice = Math.max(dataMaxPrice, (index + 1) * size);
          }
        }
        if (!Number.isFinite(dataMinPrice) || !Number.isFinite(dataMaxPrice) || dataMinPrice === dataMaxPrice) {
          dataMinPrice = 0;
          dataMaxPrice = size > 0 ? size * 20 : 1;
        }
        const tickSize = this.priceStep > 0 ? this.priceStep : size;
        const dataSpan = Math.max(tickSize, dataMaxPrice - dataMinPrice);
        const paddingPrice = Math.max(
          tickSize * Math.max(0, Number(FOOTPRINT_VISUAL_CONFIG.minVerticalPaddingTicks) || 0),
          dataSpan * Math.max(0, Number(FOOTPRINT_VISUAL_CONFIG.verticalPaddingPercent) || 0),
        );
        const paddingTicks = tickSize > 0 ? paddingPrice / tickSize : 0;
        const naturalMinIndex = Math.floor((dataMinPrice - paddingPrice) / size);
        const naturalMaxIndex = Math.ceil((dataMaxPrice + paddingPrice) / size);
        const naturalRows = Math.max(1, naturalMaxIndex - naturalMinIndex);
        const requestedBinPixelHeight = Math.max(
          this.minBinPixelHeight,
          this.binPixelHeight,
        );
        const visibleRows = Math.max(8, plotH / requestedBinPixelHeight);
        const halfRows = visibleRows / 2;
        const naturalCenterIndex = (naturalMinIndex + naturalMaxIndex) / 2;

        if (this.verticalPinned) {
          this.priceCenterIndex = naturalCenterIndex;
        } else if (!Number.isFinite(this.priceCenterIndex)) {
          this.priceCenterIndex = naturalCenterIndex;
        }

        if (naturalRows > visibleRows) {
          this.priceCenterIndex = Math.max(
            naturalMinIndex + halfRows,
            Math.min(naturalMaxIndex - halfRows, this.priceCenterIndex),
          );
        } else {
          this.priceCenterIndex = naturalCenterIndex;
        }
        this.effectiveBinPixelHeight = requestedBinPixelHeight;
        const range = {
          min: (this.priceCenterIndex - halfRows) * size,
          max: (this.priceCenterIndex + halfRows) * size,
        };
        this.scaleMetrics = {
          dataMinPrice,
          dataMaxPrice,
          visualMinPrice: range.min,
          visualMaxPrice: range.max,
          verticalPaddingTicks: paddingTicks,
        };
        return range;
      }
      draw() {
        const { width, height } = this.resizeCanvas();
        const ctx = this.ctx;
        ctx.clearRect(0, 0, width, height);
        ctx.fillStyle = "#0b0f14";
        ctx.fillRect(0, 0, width, height);
        const all = this.renderCandles();
        if (!all.length || this.fixedSize <= 0) {
          ctx.fillStyle = "#8b949e";
          ctx.fillText(`No ${ACTIVE_TIMEFRAME} candles yet`, 18, 28);
          return;
        }
        const axisWidth = 88;
        const bottomAxis = 34;
        const deltaTableHeight = 236;
        const plotW = width - axisWidth;
        const plotH = Math.max(160, height - bottomAxis - deltaTableHeight);
        const candleItems = this.visibleCandleItems(all, plotW);
        const candles = candleItems.map(item => item.candle);
        const range = this.computePriceRange(candles, this.fixedSize, plotH);
        this.currentRange = range;
        const layoutMetrics = {
          dataMinPrice: Number(this.scaleMetrics?.dataMinPrice?.toFixed(6) || 0),
          dataMaxPrice: Number(this.scaleMetrics?.dataMaxPrice?.toFixed(6) || 0),
          visualMinPrice: Number(this.scaleMetrics?.visualMinPrice?.toFixed(6) || 0),
          visualMaxPrice: Number(this.scaleMetrics?.visualMaxPrice?.toFixed(6) || 0),
          verticalPaddingTicks: Number(this.scaleMetrics?.verticalPaddingTicks?.toFixed(2) || 0),
          binTickCount: this.binTickCount,
          minBinPixelHeight: this.minBinPixelHeight,
          actualBinPixelHeight: Number(this.effectiveBinPixelHeight.toFixed(2)),
          fontSize: footprintFontSize(this.effectiveBinPixelHeight),
          visibleCandles: candles.length,
          profileReservedArea: 0,
        };
        for (const [key, value] of Object.entries(layoutMetrics)) {
          this.section.dataset[key] = String(value);
        }
        this.section.dataset.horizontalOffsetPx = String(
          Number(this.horizontalOffsetPx.toFixed(2)),
        );
        this.section.dataset.candleWidthPx = String(
          Number(this.candleWidth.toFixed(2)),
        );
        this.section.dataset.viewportEndTimeMs = String(
          this.currentViewportEndTimeMs(),
        );
        this.section.dataset.firstVisibleOpenTimeMs = String(
          candles.length ? candleOpen(candles[0]) : 0,
        );
        this.section.dataset.lastVisibleOpenTimeMs = String(
          candles.length ? candleOpen(candles[candles.length - 1]) : 0,
        );
        const layoutSignature = JSON.stringify(layoutMetrics);
        if (layoutSignature !== this.lastLayoutSignature) {
          console.info(
            `FOOTPRINT_LAYOUT | data_min=${layoutMetrics.dataMinPrice}`
            + ` | data_max=${layoutMetrics.dataMaxPrice}`
            + ` | visual_min=${layoutMetrics.visualMinPrice}`
            + ` | visual_max=${layoutMetrics.visualMaxPrice}`
            + ` | padding_ticks=${layoutMetrics.verticalPaddingTicks}`
            + ` | bin_ticks=${layoutMetrics.binTickCount}`
            + ` | min_bin_px=${layoutMetrics.minBinPixelHeight}`
            + ` | actual_bin_px=${layoutMetrics.actualBinPixelHeight}`
            + ` | font_px=${layoutMetrics.fontSize}`
            + ` | visible_candles=${layoutMetrics.visibleCandles}`
            + ` | profile_reserved_px=${layoutMetrics.profileReservedArea}`,
          );
          this.lastLayoutSignature = layoutSignature;
        }
        const priceToY = price => {
          const ratio = (range.max - price) / Math.max(1e-9, range.max - range.min);
          return Math.max(0, Math.min(plotH, ratio * plotH));
        };
        const minIndex = Math.floor(range.min / this.fixedSize);
        const maxIndex = Math.ceil(range.max / this.fixedSize);
        this.drawGrid(ctx, plotW, plotH, axisWidth, minIndex, maxIndex, priceToY);
        this.drawAbsorptionHighlightExtensions(ctx, candleItems, plotW, plotH, priceToY);
        candleItems.forEach(item => {
          this.drawCandle(ctx, item.candle, item.x, plotH, priceToY);
        });
        this.drawDeltaTable(ctx, candleItems, plotW, plotH, axisWidth, deltaTableHeight);
        this.drawTimeAxis(ctx, candleItems, plotW, plotH + deltaTableHeight);
        this.drawPriceAxis(ctx, plotW, plotH, axisWidth, minIndex, maxIndex, priceToY);
        this.drawHover(ctx, candleItems, plotW, plotH, axisWidth);
      }
      drawGrid(ctx, plotW, plotH, axisWidth, minIndex, maxIndex, priceToY) {
        ctx.strokeStyle = "rgba(48,54,61,.55)";
        ctx.lineWidth = 1;
        const rowHeight = Math.abs(priceToY(minIndex * this.fixedSize) - priceToY((minIndex + 1) * this.fixedSize));
        const labelStep = Math.max(1, Math.ceil(18 / Math.max(1, rowHeight)));
        for (let index = minIndex; index <= maxIndex; index += labelStep) {
          const y = priceToY(rowCenterPriceForBinIndex(index, this.fixedSize));
          ctx.beginPath();
          ctx.moveTo(0, y);
          ctx.lineTo(plotW + axisWidth, y);
          ctx.stroke();
        }
      }
      drawCandle(ctx, candle, x, plotH, priceToY) {
        const innerW = Math.max(72, Math.min(88, this.candleWidth - 10));
        const innerX = x + (this.candleWidth - innerW) / 2;
        const open = maybeNum(ohlc(candle, "open"));
        const high = maybeNum(ohlc(candle, "high"));
        const low = maybeNum(ohlc(candle, "low"));
        const close = maybeNum(ohlc(candle, "close"));
        for (const bin of safeArray(candle?.bins)) {
          const total = binTotal(bin);
          if (total <= 0) continue;
          const index = binIndex(bin, this.fixedSize);
          if (!Number.isFinite(index)) continue;
          const top = priceToY((index + 1) * this.fixedSize);
          const bottom = priceToY(index * this.fixedSize);
          if (bottom <= 0 || top >= plotH) continue;
          const h = Math.max(1, bottom - top);
          const delta = binDelta(bin);
          const highlightStyle = binAbsorptionHighlightStyle(candle, bin, this.fixedSize);
          const singleSideSpikeStyle = binSingleSideSpikeStyle(candle, bin, this.fixedSize);
          const hasHighContractSpikeScore = binHasHighContractSpikeScore(bin);
          const usesHighContrastBin = Boolean(singleSideSpikeStyle) || hasHighContractSpikeScore;
          ctx.fillStyle = singleSideSpikeStyle?.fill || (hasHighContractSpikeScore
            ? "#ffffff"
            : highlightStyle?.fill || (delta > 0 ? "rgba(31,111,235,.62)" : delta < 0 ? "rgba(248,81,73,.52)" : "rgba(210,153,34,.42)"));
          ctx.fillRect(innerX, top, innerW, h);
          if (!usesHighContrastBin) {
            ctx.fillStyle = "rgba(11,15,20,.34)";
            ctx.fillRect(innerX + innerW / 2 - 5, top, 10, h);
          }
          if (highlightStyle && !usesHighContrastBin) {
            ctx.fillStyle = highlightStyle.zoneFill;
            ctx.fillRect(innerX, top, innerW, h);
            ctx.strokeStyle = highlightStyle.stroke;
            ctx.lineWidth = 2;
            ctx.strokeRect(innerX + 1, top + 1, innerW - 2, Math.max(1, h - 2));
          }
          if (binAbnormalBuyImbalance(bin)) {
            ctx.strokeStyle = "#3fb950";
            ctx.lineWidth = 3;
            ctx.beginPath();
            ctx.moveTo(innerX + 1.5, top);
            ctx.lineTo(innerX + 1.5, bottom);
            ctx.stroke();
          }
          if (binAbnormalSellImbalance(bin)) {
            ctx.strokeStyle = "#f85149";
            ctx.lineWidth = 3;
            ctx.beginPath();
            ctx.moveTo(innerX + innerW - 1.5, top);
            ctx.lineTo(innerX + innerW - 1.5, bottom);
            ctx.stroke();
          }
          if (h >= 12 && innerW >= 42) {
            const centerY = top + h / 2;
            drawBinQuantityPair(ctx, bin, innerX, innerX + innerW, centerY, h);
          }
        }
        if ([open, high, low, close].every(Number.isFinite)) {
          const center = x + this.candleWidth / 2;
          const yHigh = priceToY(high);
          const yLow = priceToY(low);
          const yOpen = priceToY(open);
          const yClose = priceToY(close);
          const bull = close >= open;
          ctx.strokeStyle = bull ? "rgba(63,185,80,.9)" : "rgba(248,81,73,.9)";
          ctx.fillStyle = bull ? "#238636" : "#da3633";
          ctx.lineWidth = 1;
          ctx.strokeRect(
            innerX - 0.5,
            Math.min(yHigh, yLow),
            innerW + 1,
            Math.max(1, Math.abs(yLow - yHigh)),
          );
          ctx.beginPath();
          ctx.moveTo(center, yHigh);
          ctx.lineTo(center, yLow);
          ctx.stroke();
          const bodyTop = Math.min(yOpen, yClose);
          const bodyH = Math.max(2, Math.abs(yOpen - yClose));
          ctx.fillRect(center - 5, bodyTop, 10, bodyH);
          ctx.strokeRect(center - 5, bodyTop, 10, bodyH);
          drawTriggerMarkers(ctx, candle, center, yHigh, yLow);
        }
      }
      footprintInnerGeometry(x) {
        const innerW = Math.max(72, Math.min(88, this.candleWidth - 10));
        const innerX = x + (this.candleWidth - innerW) / 2;
        return { innerX, innerW };
      }
      drawAbsorptionHighlightExtensions(ctx, candleItems, plotW, plotH, priceToY) {
        for (let itemIndex = 0; itemIndex < candleItems.length; itemIndex++) {
          const item = candleItems[itemIndex];
          const sourceGeometry = this.footprintInnerGeometry(item.x);
          for (const bin of safeArray(item.candle?.bins)) {
            if (binTotal(bin) <= 0) continue;
            const style = binAbsorptionHighlightStyle(item.candle, bin, this.fixedSize);
            if (!style) continue;
            const index = binIndex(bin, this.fixedSize);
            if (!Number.isFinite(index)) continue;
            const top = priceToY((index + 1) * this.fixedSize);
            const bottom = priceToY(index * this.fixedSize);
            if (bottom <= 0 || top >= plotH) continue;
            let rightX = plotW;
            for (let blockerIndex = itemIndex + 1; blockerIndex < candleItems.length; blockerIndex++) {
              if (candleHasContractAtBinIndex(candleItems[blockerIndex].candle, index, this.fixedSize)) {
                rightX = this.footprintInnerGeometry(candleItems[blockerIndex].x).innerX;
                break;
              }
            }
            const leftX = sourceGeometry.innerX + sourceGeometry.innerW;
            if (rightX <= leftX) continue;
            ctx.fillStyle = style.zoneFill;
            ctx.fillRect(
              Math.max(0, leftX),
              top,
              Math.min(plotW, rightX) - Math.max(0, leftX),
              Math.max(1, bottom - top),
            );
          }
        }
      }
      drawTimeAxis(ctx, candleItems, plotW, plotH) {
        ctx.fillStyle = "#8b949e";
        ctx.strokeStyle = "#30363d";
        ctx.beginPath();
        ctx.moveTo(0, plotH);
        ctx.lineTo(plotW, plotH);
        ctx.stroke();
        ctx.font = "15px Segoe UI, Arial";
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        candleItems.forEach((item, index) => {
          if (index % 10 !== 0) return;
          ctx.fillText(
            dateTimeLabel(candleOpen(item.candle)),
            item.x + this.candleWidth / 2,
            plotH + 9,
          );
        });
      }
      drawDeltaTable(ctx, candleItems, plotW, top, axisWidth, tableHeight) {
        const rows = [
          ["Delta", "delta_contracts", 0, "SIGNED"],
          ["Body Delta", "body_delta", 0, "SIGNED"],
          ["Upper Wick Delta", "upper_wick_delta", 0, "SIGNED"],
          ["Lower Wick Delta", "lower_wick_delta", 0, "SIGNED"],
          ["Max Buy Ratio", "max_buy_diagonal_ratio", 3, "BUY"],
          ["Max Sell Ratio", "max_sell_diagonal_ratio", 3, "SELL"],
          ["Sum Buy Ratio", "sum_buy_diagonal_ratio", 3, "BUY"],
          ["Sum Sell Ratio", "sum_sell_diagonal_ratio", 3, "SELL"],
          ["Max Buy Dir Eff", "max_buy_directional_efficiency", 2, "BUY"],
          ["Max Sell Dir Eff", "max_sell_directional_efficiency", 2, "SELL"],
          ["Max Spike Score", "max_contract_spike_score", 3, "NEUTRAL"],
          ["Spike Score SD", "contract_spike_score_deviation", 3, "NEUTRAL"],
          ["NY Session Cum", "session_cumulative_delta", 0, "SIGNED"],
          ["Day Cum", "day_cumulative_delta", 0, "SIGNED"],
        ];
        const rowHeight = tableHeight / rows.length;
        const hoveredRowIndex = (
          this.hover
          && this.hover.x >= 0
          && this.hover.x <= plotW + axisWidth
          && this.hover.y >= top
          && this.hover.y < top + tableHeight
        ) ? Math.floor((this.hover.y - top) / rowHeight) : -1;
        ctx.fillStyle = "#0f141b";
        ctx.fillRect(0, top, plotW + axisWidth, tableHeight);
        ctx.strokeStyle = "#30363d";
        ctx.lineWidth = 1;
        ctx.textBaseline = "middle";
        rows.forEach(([label, field, decimalPlaces, colorMode], rowIndex) => {
          const y = top + rowIndex * rowHeight;
          if (rowIndex === hoveredRowIndex) {
            ctx.fillStyle = "rgba(88,166,255,.16)";
            ctx.fillRect(0, y, plotW + axisWidth, rowHeight);
          }
          ctx.beginPath();
          ctx.moveTo(0, y);
          ctx.lineTo(plotW + axisWidth, y);
          ctx.stroke();
          candleItems.forEach(item => {
            const candle = item.candle;
            const centerX = item.x + this.candleWidth / 2;
            if (centerX >= plotW) return;
            const rawValue = footprintDeltaTableValue(candle, field, this.fixedSize, this.priceStep);
            if (!Number.isFinite(rawValue)) return;
            const text = decimalPlaces > 0
              ? rawValue.toFixed(decimalPlaces)
              : String(Math.trunc(rawValue));
            ctx.font = "600 22px Arial Narrow, Segoe UI, Arial";
            ctx.fillStyle = colorMode === "BUY"
              ? "#3fb950"
              : colorMode === "SELL"
                ? "#f85149"
                : colorMode === "NEUTRAL"
                  ? "#d29922"
                  : rawValue > 0
                    ? "#3fb950"
                    : rawValue < 0
                      ? "#f85149"
                      : "#8b949e";
            ctx.textAlign = "center";
            ctx.fillText(text, centerX, y + rowHeight / 2);
          });
          ctx.font = "600 20px Arial Narrow, Segoe UI, Arial";
          ctx.fillStyle = "#c9d1d9";
          ctx.textAlign = "right";
          ctx.fillText(label, plotW + axisWidth - 6, y + rowHeight / 2);
        });
        ctx.beginPath();
        ctx.moveTo(plotW, top);
        ctx.lineTo(plotW, top + tableHeight);
        ctx.stroke();
      }
      drawPriceAxis(ctx, plotW, plotH, axisWidth, minIndex, maxIndex, priceToY) {
        ctx.fillStyle = "#101720";
        ctx.fillRect(plotW, 0, axisWidth, plotH);
        ctx.strokeStyle = "#30363d";
        ctx.beginPath();
        ctx.moveTo(plotW, 0);
        ctx.lineTo(plotW, plotH);
        ctx.stroke();
        const tickSize = this.priceStep > 0 ? this.priceStep : this.fixedSize;
        const priceLabelStep = tickSize * 20;
        const firstLabelIndex = Math.ceil((minIndex * this.fixedSize) / priceLabelStep);
        const lastLabelIndex = Math.floor((maxIndex * this.fixedSize) / priceLabelStep);
        ctx.fillStyle = "#8b949e";
        ctx.font = "600 15px Segoe UI, Arial";
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        for (let labelIndex = firstLabelIndex; labelIndex <= lastLabelIndex; labelIndex += 1) {
          const price = labelIndex * priceLabelStep;
          const y = priceToY(price);
          ctx.fillText(fmtPrice(price), plotW + axisWidth - 8, y);
        }
      }
      drawHover(ctx, candleItems, plotW, plotH, axisWidth) {
        if (!this.hover || this.hover.x < 0 || this.hover.x > plotW || this.hover.y < 0 || this.hover.y > plotH) {
          this.tooltip.style.opacity = "0";
          return;
        }
        const hoveredItem = candleItems.find(item => (
          this.hover.x >= item.x
          && this.hover.x < item.x + this.candleWidth
        ));
        if (!hoveredItem) {
          this.tooltip.style.opacity = "0";
          return;
        }
        const candle = hoveredItem.candle;
        const triggerHigh = maybeNum(ohlc(candle, "high"));
        const triggerLow = maybeNum(ohlc(candle, "low"));
        if ([triggerHigh, triggerLow].every(Number.isFinite)) {
          const triggerSignal = triggerMarkerAt(
            candle,
            hoveredItem.x + this.candleWidth / 2,
            this.yForPrice(triggerHigh, plotH),
            this.yForPrice(triggerLow, plotH),
            this.hover.x,
            this.hover.y,
          );
          if (triggerSignal) {
            this.tooltip.textContent = triggerTooltipText(triggerSignal);
            placeTooltip(this.tooltip, this.hover, plotW, plotH, 320, 100);
            return;
          }
        }
        const rawPrice = this.priceAtY(this.hover.y, plotH);
        const price = this.priceStep > 0
          ? Math.round(rawPrice / this.priceStep) * this.priceStep
          : rawPrice;
        const markerY = this.yForPrice(price, plotH);
        ctx.strokeStyle = "rgba(88,166,255,.75)";
        ctx.beginPath();
        ctx.moveTo(hoveredItem.x, 0);
        ctx.lineTo(hoveredItem.x, plotH);
        ctx.moveTo(0, markerY);
        ctx.lineTo(plotW, markerY);
        ctx.stroke();
        this.drawPriceMarker(ctx, plotW, axisWidth, plotH, markerY, price);
        const total = safeArray(candle.bins).reduce((sum, bin) => sum + binTotal(bin), 0);
        const delta = safeArray(candle.bins).reduce((sum, bin) => sum + binDelta(bin), 0);
        const usesContracts = this.quantityUnit === "CONTRACTS";
        const hoveredBin = binAtPrice(candle, price, this.fixedSize);
        let binDetails = "";
        if (hoveredBin) {
          const index = binIndex(hoveredBin, this.fixedSize);
          binDetails = usesContracts ? `
Bin ${index} price ${fmtPrice(priceForBinIndex(index, this.fixedSize))}
Total contracts ${fmt(binTotal(hoveredBin), 0)} | Contract delta ${signed(binDelta(hoveredBin), 0)}
Diagonal contracts B delta ${fmtMaybe(binBuyDiagonalDelta(hoveredBin), 0)} / S delta ${fmtMaybe(binSellDiagonalDelta(hoveredBin), 0)}
Diagonal ratios B ${fmtMaybe(binBuyPressure(hoveredBin), 3)} / S ${fmtMaybe(binSellPressure(hoveredBin), 3)}
Contract spike score ${fmtMaybe(binContractSpikeScore(hoveredBin), 3)}
Dominance ${binDominantSide(hoveredBin)} | Dom contracts ${fmtMaybe(binDominantQuantity(hoveredBin), 0)}
Buy Dir Eff: ${fmtMaybe(binBuyDirectionalEfficiency(candle, hoveredBin, this.fixedSize, this.priceStep), 2)} | Sell Dir Eff: ${fmtMaybe(binSellDirectionalEfficiency(candle, hoveredBin, this.fixedSize, this.priceStep), 2)}` : `
Bin ${index} price ${fmtPrice(priceForBinIndex(index, this.fixedSize))}
TotalVolume ${fmt(binTotal(hoveredBin), 0)} | Delta ${signed(binDelta(hoveredBin), 0)}
Diagonal pressure B ${fmtMaybe(binBuyPressure(hoveredBin), 3)} / S ${fmtMaybe(binSellPressure(hoveredBin), 3)}
Dominance ${binDominantSide(hoveredBin)} | DomVol ${fmtMaybe(binDominantQuantity(hoveredBin), 2)}
Buy Dir Eff: ${fmtMaybe(binBuyDirectionalEfficiency(candle, hoveredBin, this.fixedSize, this.priceStep), 2)} | Sell Dir Eff: ${fmtMaybe(binSellDirectionalEfficiency(candle, hoveredBin, this.fixedSize, this.priceStep), 2)}`;
        }
        const quantitySummary = usesContracts
          ? `Contracts ${fmt(total, 0)} | Contract delta ${signed(delta, 0)}`
          : `Volume ${fmt(total, 2)} | Delta ${signed(delta, 2)}`;
        this.tooltip.textContent = `${candle.is_live ? "LIVE " : ""}${ACTIVE_TIMEFRAME} ${timeLabel(candleOpen(candle))}
O ${ohlc(candle, "open")} H ${ohlc(candle, "high")} L ${ohlc(candle, "low")} C ${ohlc(candle, "close")}
${quantitySummary}
Price ${fmtPrice(price)}${binDetails}`;
        this.tooltip.style.opacity = "1";
        const tooltipWidth = this.tooltip.offsetWidth || 360;
        const tooltipHeight = this.tooltip.offsetHeight || 120;
        let left = this.hover.x + 12;
        if (left + tooltipWidth + 8 > plotW) left = this.hover.x - tooltipWidth - 12;
        left = Math.max(0, Math.min(left, Math.max(0, plotW - tooltipWidth - 8)));
        let top = this.hover.y + 12;
        if (top + tooltipHeight + 8 > plotH) top = this.hover.y - tooltipHeight - 12;
        top = Math.max(0, Math.min(top, Math.max(0, plotH - tooltipHeight - 8)));
        this.tooltip.style.left = `${left}px`;
        this.tooltip.style.top = `${top}px`;
      }
      priceAtY(y, plotH) {
        const range = this.currentRange;
        if (!range) return 0;
        const ratio = y / Math.max(1, plotH);
        return range.max - ratio * (range.max - range.min);
      }
      yForPrice(price, plotH) {
        const range = this.currentRange;
        if (!range) return 0;
        const ratio = (range.max - price) / Math.max(1e-9, range.max - range.min);
        return Math.max(0, Math.min(plotH, ratio * plotH));
      }
      drawPriceMarker(ctx, plotW, axisWidth, plotH, y, price) {
        const boxHeight = 30;
        const boxY = Math.max(0, Math.min(plotH - boxHeight, y - boxHeight / 2));
        ctx.fillStyle = "#1f6feb";
        ctx.fillRect(plotW + 2, boxY, axisWidth - 4, boxHeight);
        ctx.strokeStyle = "#58a6ff";
        ctx.strokeRect(plotW + 2.5, boxY + 0.5, axisWidth - 5, boxHeight - 1);
        ctx.fillStyle = "#ffffff";
        ctx.font = "150 20px Segoe UI, Arial";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(fmtPrice(price), plotW + axisWidth / 2, boxY + boxHeight / 2);
      }
    }

    function ensureSessionSection(session) {
      const key = sessionKey(session);
      let chart = charts.get(key);
      if (chart) return chart;
      const section = document.createElement("section");
      section.className = "session";
      section.dataset.session = key;
      section.innerHTML = `<div class="session-title"><strong></strong><span></span></div>
        <div class="chart-host"><canvas></canvas><div class="tooltip"></div></div>`;
      app.appendChild(section);
      section.querySelector(".chart-host").insertAdjacentHTML(
        "afterend",
        `<div class="chart-scrollbar"><div class="history-scrollbar" role="scrollbar" aria-label="Horizontal chart position" aria-orientation="horizontal"><div class="history-scrollbar-content"></div></div></div>`,
      );
      chart = new CanvasFootprintChart(section, session);
      charts.set(key, chart);
      return chart;
    }
    function activeTimeframeSessions(snapshot) {
      return safeArray(snapshot?.sessions).filter(session => sessionTimeframe(session) === ACTIVE_TIMEFRAME);
    }
    function render(snapshot) {
      if (!snapshot) return;
      const sessions = activeTimeframeSessions(snapshot);
      viewportMode = Boolean(snapshot?.viewport_window);
      if (Number(snapshot?.earliest_window_start_ms) > 0) {
        earliestWindowStartMs = Number(snapshot.earliest_window_start_ms);
      }
      if (Number(snapshot?.window_start_ms) > 0) windowStartMs = Number(snapshot.window_start_ms);
      if (Number(snapshot?.window_end_ms) > 0) windowEndMs = Number(snapshot.window_end_ms);
      if (Number(snapshot?.latest_window_end_ms) > 0) {
        latestWindowEndMs = Math.max(latestWindowEndMs, Number(snapshot.latest_window_end_ms));
      }
      hasOlderData = Boolean(snapshot?.has_older_data);
      const displayLimit = snapshot?.display_candles_by_timeframe?.[ACTIVE_TIMEFRAME] ?? snapshot?.memory_candles ?? "";
      const generated = snapshot?.generated_at_utc ? new Date(snapshot.generated_at_utc).toLocaleTimeString() : new Date().toLocaleTimeString();
      const processedTrades = Number(snapshot?.processed_trades || 0);
      statusEl.textContent = `Updated ${generated} | timeframe ${ACTIVE_TIMEFRAME} | bin ${activeBinTickCount} ticks | viewport candles ${displayLimit} | processed trades ${processedTrades} | Canvas engine`;
      if (!sessions.length && charts.size === 0) {
        app.innerHTML = `<div class="empty">No ${ACTIVE_TIMEFRAME} sessions yet.</div>`;
        return;
      }
      if (app.querySelector(".empty")) app.replaceChildren();
      const activeKeys = new Set(sessions.map(sessionKey));
      for (const [key, chart] of charts) {
        if (!activeKeys.has(key) && snapshot.delta_after_open_time_ms === undefined) {
          chart.section.remove();
          charts.delete(key);
        }
      }
      sessions.forEach(session => ensureSessionSection(session).mergeSession(session, displayLimit));
    }
    function cacheSnapshot(snapshot) {
      if (!snapshot) return;
      const displayLimit = (
        snapshot?.display_candles_by_timeframe?.[ACTIVE_TIMEFRAME]
        ?? snapshot?.memory_candles
        ?? ""
      );
      for (const session of activeTimeframeSessions(snapshot)) {
        const chart = charts.get(sessionKey(session));
        if (chart) chart.cacheSession(session, displayLimit);
      }
    }
    function deltaAfterOpenTime() {
      if (!charts.size) return null;
      let value = Infinity;
      for (const chart of charts.values()) {
        if (!chart.lastClosedOpenTime) return null;
        value = Math.min(value, chart.lastClosedOpenTime);
      }
      return Number.isFinite(value) ? value : null;
    }
    let refreshTimer = null;
    let viewportResizeTimer = null;
    let viewportRequestTimer = null;
    let scheduledViewportRequest = null;
    let activeViewportRequest = null;
    let latestViewportRequestId = 0;
    let requestedWindowEndMs = 0;
    const replayMarkersByCandleOpen = new Map();
    function requestedFootprintCandleLimit() {
      return Math.max(
        1,
        Math.floor(Math.max(1, window.innerWidth - 112) / footprintCandleWidth()),
      );
    }
    function scheduleViewportResize(endTimeMs, candleLimit) {
      if (!viewportMode) return;
      if (viewportResizeTimer) clearTimeout(viewportResizeTimer);
      viewportResizeTimer = setTimeout(
        () => requestViewportWindow(endTimeMs, candleLimit),
        120,
      );
    }
    function requestViewportWindow(
      endTimeMs,
      candleLimit = requestedFootprintCandleLimit(),
      preview = true,
    ) {
      const normalizedEndTimeMs = Number(endTimeMs);
      if (!Number.isFinite(normalizedEndTimeMs) || normalizedEndTimeMs <= 0) return;
      const normalizedLimit = Math.max(1, Number(candleLimit) || requestedFootprintCandleLimit());
      cancelLiveRefresh();
      requestedWindowEndMs = normalizedEndTimeMs;
      for (const chart of charts.values()) {
        chart.positionViewport(normalizedEndTimeMs, normalizedLimit);
        if (
          preview
          && chart.canPreviewViewport(normalizedEndTimeMs, normalizedLimit)
        ) {
          chart.draw();
        }
        chart.syncScrollbar();
      }
      const requestId = ++latestViewportRequestId;
      
      scheduledViewportRequest = {
        id: requestId,
        endTimeMs: normalizedEndTimeMs,
        candleLimit: normalizedLimit,
      };
      if (viewportRequestTimer) clearTimeout(viewportRequestTimer);
      viewportRequestTimer = setTimeout(() => {
        const request = scheduledViewportRequest;
        scheduledViewportRequest = null;
        viewportRequestTimer = null;
        if (request) refresh(request.endTimeMs, request.candleLimit, request.id);
      }, VIEWPORT_REQUEST_DEBOUNCE_MS);
    }
    function navigateFootprintByCandles(
      candleDelta,
      candleLimit = requestedFootprintCandleLimit(),
      preview = true,
    ) {
      if (!viewportMode || !windowEndMs || candleDelta === 0) return;
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
      const latestEnd = latestWindowEndMs || windowEndMs;
      const earliestEnd = (earliestWindowStartMs || windowStartMs) + candleLimit * intervalMs;
      const chartEnd = charts.size
        ? [...charts.values()][0].currentViewportEndTimeMs()
        : 0;
      const navigationEnd = requestedWindowEndMs || chartEnd || windowEndMs;
      const desiredEnd = Math.max(
        earliestEnd,
        Math.min(latestEnd, navigationEnd + candleDelta * intervalMs),
      );
      if (desiredEnd === navigationEnd) return;
      for (const chart of charts.values()) chart.resetAutoScale();
      requestViewportWindow(desiredEnd, candleLimit, preview);
    }
    function cancelLiveRefresh() {
      if (refreshTimer) clearTimeout(refreshTimer);
      refreshTimer = null;
    }
    function scheduleNextRefresh(delayMs = refreshDelayMs) {
      cancelLiveRefresh();
      refreshTimer = setTimeout(() => {
        refreshTimer = null;
        if (
          viewportMode
          || requestedWindowEndMs
          || scheduledViewportRequest
          || activeViewportRequest
        ) {
          return;
        }
        refresh();
      }, delayMs);
    }
    function reportClientViewportMetric(eventName, requestId) {
      const params = new URLSearchParams({
        event: eventName,
        view: "footprint",
        timeframe: ACTIVE_TIMEFRAME,
        request_id: String(requestId),
      });
      fetch(`/viewport-client-metric?${params.toString()}`, {
        cache: "no-store",
        keepalive: true,
      }).catch(() => {});
    }
    function cancelActiveViewportRequest() {
      if (!activeViewportRequest) return;
      const request = activeViewportRequest;
      activeViewportRequest = null;
      request.controller.abort();
      reportClientViewportMetric("cancelled", request.id);
    }
    function knownOpenTimes(endTimeMs, candleLimit) {
      const values = new Set();
      for (const chart of charts.values()) {
        for (const openTime of chart.cachedOpenTimes(endTimeMs, candleLimit)) {
          values.add(openTime);
        }
      }
      return [...values].sort((a, b) => a - b);
    }
    async function refresh(
      endTimeMs = null,
      candleLimit = requestedFootprintCandleLimit(),
      scheduledRequestId = null,
    ) {
      if (
        endTimeMs === null
        && scheduledRequestId === null
        && (
          viewportMode
          || requestedWindowEndMs
          || scheduledViewportRequest
        )
      ) {
        return;
      }
      const requestId = scheduledRequestId ?? ++latestViewportRequestId;
      if (requestId !== latestViewportRequestId) return;
      cancelActiveViewportRequest();
      const controller = new AbortController();
      activeViewportRequest = { id: requestId, controller };
      try {
        const params = new URLSearchParams();
        const fetchCandleLimit = Math.max(
          1,
          candleLimit + FOOTPRINT_FETCH_OVERSCAN,
        );
        params.set("candle_limit", String(fetchCandleLimit));
        params.set("bin_ticks", String(activeBinTickCount));
        params.set("request_id", String(requestId));
        if (endTimeMs) {
          params.set("end_time_ms", String(endTimeMs));
          const known = knownOpenTimes(endTimeMs, fetchCandleLimit);
          if (known.length) params.set("known_open_times_ms", known.join(","));
        } else if (!viewportMode) {
          const after = deltaAfterOpenTime();
          if (after !== null) params.set("after_open_time_ms", String(after));
        }
        const suffix = `?${params.toString()}`;
        const response = await fetch(
          `/footprint-data/${ACTIVE_TIMEFRAME}${suffix}`,
          { cache: "no-store", signal: controller.signal },
        );
        const snapshot = response.ok ? await response.json() : null;
        if (requestId !== latestViewportRequestId) {
          cacheSnapshot(snapshot);
          reportClientViewportMetric("ignored_obsolete", requestId);
          return;
        }
        render(snapshot);
        if (endTimeMs) requestedWindowEndMs = 0;
        
      } catch (error) {
        if (error?.name !== "AbortError" && requestId === latestViewportRequestId) {
          render(null);
        }
      } finally {
        if (activeViewportRequest?.id === requestId) {
          activeViewportRequest = null;
        }
        if (!viewportMode && requestId === latestViewportRequestId) {
          scheduleNextRefresh(refreshDelayMs);
        }
      }
    }
    window.addEventListener("keydown", event => {
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
      const target = event.target;
      if (
        target instanceof HTMLInputElement
        && !target.classList.contains("history-scrollbar")
      ) return;
      event.preventDefault();
      navigateFootprintByCandles(
        event.key === "ArrowLeft" ? -3 : 3,
        requestedFootprintCandleLimit(),
      );
    });
    binTickSelect.addEventListener("change", () => {
      const preservedViewportEndMs = charts.size
        ? [...charts.values()][0].currentViewportEndTimeMs()
        : 0;
      const selected = Number.parseInt(binTickSelect.value, 10);
      activeBinTickCount = BIN_TICK_OPTIONS.includes(selected)
        ? selected
        : DEFAULT_BIN_TICK_COUNT;
      binTickSelect.value = String(activeBinTickCount);
      try {
        localStorage.setItem("footprint.binTicks", String(activeBinTickCount));
      } catch {}
      clearAllFootprintChartCaches();
      for (const chart of charts.values()) {
        chart.binTickCount = activeBinTickCount;
      }
      const targetEnd = (
        requestedWindowEndMs
        || preservedViewportEndMs
        || windowEndMs
        || latestWindowEndMs
      );
      if (targetEnd > 0) {
        requestViewportWindow(targetEnd, requestedFootprintCandleLimit());
      } else {
        refresh();
      }
    });
    timeframeLinks();
    refresh();
  </script>
</body>
</html>"""


def _html_page(timeframe: str = DEFAULT_FOOTPRINT_TIMEFRAME) -> str:
    normalized_timeframe = timeframe.strip().upper()
    if normalized_timeframe not in FOOTPRINT_TIMEFRAMES:
        normalized_timeframe = DEFAULT_FOOTPRINT_TIMEFRAME
    return (
        _HTML_TEMPLATE
        .replace("__ACTIVE_TIMEFRAME__", normalized_timeframe)
        .replace("__CONTRACT_SPIKE_THRESHOLD__", str(CONTRACT_SPIKE_THRESHOLD))
    )


_HTML_PAGE = _html_page(DEFAULT_FOOTPRINT_TIMEFRAME)


_CANDLES_HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Candles __ACTIVE_TIMEFRAME__</title>
  <style>
    :root {
      --bg: #0d1117;
      --panel: #111820;
      --line: #2b333d;
      --text: #e6edf3;
      --muted: #8b949e;
      --blue: #58a6ff;
      --green: #3fb950;
      --red: #f85149;
      --gold: #d29922;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 12px;
      overflow: hidden;
    }
    header {
      height: 112px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: #0f141b;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      margin: 0 0 4px;
      font-size: 20px;
      line-height: 1.1;
    }
    .meta { color: var(--muted); }
    .timeframe-links {
      display: flex;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .candle-header-controls {
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .process-replay {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .process-replay input {
      width: 270px;
      min-width: 0;
      border: 1px solid var(--line);
      background: #0d1117;
      color: var(--text);
      border-radius: 6px;
      padding: 8px 10px;
      font-size: 20px;
      font-weight: 150;
      line-height: 1.2;
    }
    .process-replay button {
      border: 1px solid var(--blue);
      background: rgba(88,166,255,.16);
      color: var(--text);
      border-radius: 6px;
      padding: 8px 14px;
      font-size: 20px;
      font-weight: 150;
      line-height: 1.2;
      cursor: pointer;
    }
    .process-replay button:disabled {
      opacity: .55;
      cursor: wait;
    }
    .process-timezone {
      color: var(--muted);
      font-size: 20px;
      font-weight: 150;
      white-space: nowrap;
    }
    .process-replay-status {
      min-width: 72px;
      color: var(--muted);
      font-size: 20px;
      font-weight: 150;
      white-space: nowrap;
    }
    .timeframe-link {
      border: 1px solid var(--line);
      color: var(--muted);
      background: #111820;
      border-radius: 6px;
      padding: 6px 9px;
      text-decoration: none;
      font-weight: 700;
    }
    .timeframe-link.active {
      color: var(--text);
      border-color: var(--blue);
      background: rgba(88,166,255,.16);
    }
    main {
      height: calc(100vh - 112px);
      padding: 12px;
      overflow: hidden;
    }
    .session {
      height: 100%;
      min-height: 420px;
      border: 1px solid var(--line);
      background: var(--panel);
      display: grid;
      grid-template-rows: auto minmax(0, 1fr) 24px;
      overflow: hidden;
    }
    .session-title {
      min-height: 40px;
      padding: 9px 12px;
      border-bottom: 1px solid var(--line);
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 16px;
      color: var(--muted);
    }
    .chart-host {
      position: relative;
      min-height: 0;
      background: #0b0f14;
      overflow: hidden;
      cursor: crosshair;
    }
    .chart-scrollbar {
      display: flex;
      align-items: center;
      min-width: 0;
      padding: 2px 82px 2px 6px;
      border-top: 1px solid var(--line);
      background: #0f141b;
    }
    .history-scrollbar {
      width: 100%;
      min-width: 0;
      height: 18px;
      overflow-x: scroll;
      overflow-y: hidden;
      scrollbar-color: var(--blue) #1b2430;
      scrollbar-width: auto;
    }
    .history-scrollbar.disabled {
      overflow-x: hidden;
      opacity: .45;
    }
    .history-scrollbar-content {
      height: 1px;
    }
    canvas {
      display: block;
      width: 100%;
      height: 100%;
    }
    .tooltip {
      position: absolute;
      left: 0;
      top: 0;
      max-width: 360px;
      padding: 7px 9px;
      border: 1px solid var(--line);
      background: rgba(15,20,27,.94);
      color: var(--text);
      font-size: 15px;
      pointer-events: none;
      white-space: pre-line;
      opacity: 0;
      line-height: 1.35;
    }
    .empty {
      height: 100%;
      display: grid;
      place-items: center;
      color: var(--muted);
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Candles __ACTIVE_TIMEFRAME__</h1>
      <div class="meta" id="status">Waiting for data...</div>
    </div>
    <div class="candle-header-controls">
      <form class="process-replay" id="process-replay-form">
        <span class="process-timezone">Vancouver</span>
        <input id="process-start" type="datetime-local" step="1" aria-label="Replay start time Vancouver">
        <input id="process-end" type="datetime-local" step="1" aria-label="Replay end time Vancouver">
        <button id="process-submit" type="submit">Replay</button>
        <span class="process-replay-status" id="process-replay-status"></span>
      </form>
      <nav class="timeframe-links" id="timeframe-links" aria-label="Candle chart timeframes"></nav>
      <a class="timeframe-link" href="/dom/__ACTIVE_TIMEFRAME__">DOM Timeline</a>
    </div>
  </header>
  <main id="app"></main>
  <script>
    const app = document.getElementById("app");
    const statusEl = document.getElementById("status");
    const linksEl = document.getElementById("timeframe-links");
    const processReplayForm = document.getElementById("process-replay-form");
    const processStartInput = document.getElementById("process-start");
    const processEndInput = document.getElementById("process-end");
    const processSubmitButton = document.getElementById("process-submit");
    const processReplayStatusEl = document.getElementById("process-replay-status");
    const ACTIVE_TIMEFRAME = "__ACTIVE_TIMEFRAME__";
    const TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"];
    const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };
    const DOM_REFILL_MARKER_MIN_COUNT = 1;
    const CANDLE_VISUAL_CONFIG = {
      verticalPaddingPercent: 0.08,
      minVerticalPaddingTicks: 10,
      minBinPixelHeight: 18,
      defaultVisibleCandles: 80,
      autoScaleEnabled: true,
      ...(() => {
        try {
          return JSON.parse(localStorage.getItem("candles.visualConfig") || "{}");
        } catch {
          return {};
        }
      })(),
      ...(window.CANDLE_VISUAL_CONFIG || {}),
    };
    const charts = new Map();
    const ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN = __CONTRACT_SPIKE_THRESHOLD__;
    const ABSORPTION_HIGHLIGHT_STYLES = {
      BUY: {
        fill: "rgba(255,45,149,.78)",
        zoneFill: "rgba(255,45,149,.18)",
        stroke: "rgba(255,45,149,.95)",
      },
      SELL: {
        fill: "rgba(63,185,80,.78)",
        zoneFill: "rgba(63,185,80,.18)",
        stroke: "rgba(63,185,80,.95)",
      },
    };
    const VIEWPORT_REQUEST_DEBOUNCE_MS = 80;
    const CANDLE_FETCH_OVERSCAN = 96;
    let earliestWindowStartMs = 0;
    let windowStartMs = 0;
    let windowEndMs = 0;
    let latestWindowEndMs = 0;
    let hasOlderData = false;
    let viewportResizeTimer = null;
    let viewportRequestTimer = null;
    let scheduledViewportRequest = null;
    let activeViewportRequest = null;
    let latestViewportRequestId = 0;
    let requestedWindowEndMs = 0;
    const replayMarkersByCandleOpen = new Map();

    function normalizedWheelDelta(event, pageSize) {
      const raw = Math.abs(event.deltaX) > Math.abs(event.deltaY)
        ? event.deltaX
        : event.deltaY;
      if (event.deltaMode === WheelEvent.DOM_DELTA_LINE) return raw * 16;
      if (event.deltaMode === WheelEvent.DOM_DELTA_PAGE) return raw * Math.max(1, pageSize);
      return raw;
    }

    function safeArray(value) { return Array.isArray(value) ? value : []; }
    function num(value) {
      const parsed = Number.parseFloat(value ?? "0");
      return Number.isFinite(parsed) ? parsed : 0;
    }
    function maybeNum(value) {
      const parsed = Number.parseFloat(value ?? "");
      return Number.isFinite(parsed) ? parsed : NaN;
    }
    function fmtMaybe(value, places = 2) {
      const parsed = Number.parseFloat(value ?? "");
      return Number.isFinite(parsed) ? parsed.toFixed(places) : "N/A";
    }
    function candleOpen(candle) { return Number(candle?.open_time_ms ?? 0); }
    function ohlc(candle, key) { return maybeNum(candle?.ohlc?.[key] ?? candle?.[`${key}_price`]); }
    function candleTriggerSignals(candle) { return safeArray(candle?.trigger_signals); }
    function candleDomRefillMarkers(candle) { return safeArray(candle?.dom_refill_markers); }
    function markerStableId(marker) {
      return String(
        marker?.id
        || marker?.output_id
        || marker?.event_id
        || [
          marker?.order_id ?? marker?.venue_order_id ?? "",
          marker?.price ?? marker?.level_price ?? marker?.reference_price ?? "",
          marker?.side ?? marker?.top_order_side ?? "",
          marker?.timestamp_ms ?? marker?.event_time_ms ?? "",
        ].join("|"),
      );
    }
    function mergeDomRefillMarkers(existingMarkers, nextMarkers) {
      const markersById = new Map();
      for (const marker of [...safeArray(existingMarkers), ...safeArray(nextMarkers)]) {
        if (!marker || typeof marker !== "object") continue;
        markersById.set(markerStableId(marker), marker);
      }
      return [...markersById.values()].sort((left, right) => (
        Number(left?.timestamp_ms ?? left?.event_time_ms ?? 0)
        - Number(right?.timestamp_ms ?? right?.event_time_ms ?? 0)
      ));
    }
    function domRefillMarkerSide(marker) {
      const directSide = String(marker?.side || marker?.top_order_side || "").trim().toUpperCase();
      if (["A", "ASK", "SELL", "SHORT"].includes(directSide)) return "ASK";
      if (["B", "BID", "BUY", "LONG"].includes(directSide)) return "BID";
      const triggerSide = String(marker?.trigger_side || marker?.direction || "").trim().toUpperCase();
      if (["SELL", "SHORT", "ASK"].includes(triggerSide)) return "ASK";
      if (["BUY", "LONG", "BID"].includes(triggerSide)) return "BID";
      return "";
    }
    function domRefillMarkerColor(marker) {
      const side = domRefillMarkerSide(marker);
      return side === "ASK" ? "#f85149" : "#3fb950";
    }
    function domRefillMarkerCount(marker) {
      return Math.trunc(num(
        marker?.refill_count
        ?? marker?.positive_refill_count
        ?? marker?.top_order_positive_refill_count,
      ));
    }
    function replayPayloadToDomMarker(payload) {
      const timestampMs = replayPayloadTimestampMs(payload);
      const refillCount = Math.trunc(num(payload?.positive_refill_count ?? payload?.refill_count));
      if (!Number.isFinite(timestampMs) || timestampMs <= 0 || refillCount <= 0) return null;
      return {
        ...payload,
        id: markerStableId(payload),
        output_id: markerStableId(payload),
        event_id: markerStableId(payload),
        type: "DOM_POSITIVE_REFILL",
        source: payload?.source || "DATA_PROCESS_REFILL_ORDER_CLOSED",
        timestamp_ms: timestampMs,
        event_time_ms: timestampMs,
        price: String(payload?.price ?? payload?.level_price ?? payload?.reference_price ?? ""),
        side: domRefillMarkerSide(payload),
        order_id: String(payload?.order_id ?? payload?.venue_order_id ?? ""),
        venue_order_id: String(payload?.venue_order_id ?? payload?.order_id ?? ""),
        positive_refill_count: refillCount,
        refill_count: refillCount,
        positive_refill_total: Math.trunc(num(payload?.positive_refill_total ?? payload?.refill_contracts ?? payload?.refill_total)),
        refill_total: Math.trunc(num(payload?.positive_refill_total ?? payload?.refill_contracts ?? payload?.refill_total)),
      };
    }
    function rememberReplayPayloadMarkers(replay) {
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60_000;
      for (const payload of safeArray(replay?.payloads)) {
        const marker = replayPayloadToDomMarker(payload);
        if (!marker) continue;
        const openTimeMs = Math.floor(Number(marker.timestamp_ms) / intervalMs) * intervalMs;
        const markers = replayMarkersByCandleOpen.get(openTimeMs) || [];
        replayMarkersByCandleOpen.set(
          openTimeMs,
          mergeDomRefillMarkers(markers, [marker]),
        );
      }
    }
    function vancouverInputValue(timestampMs) {
      const value = Number(timestampMs);
      if (!Number.isFinite(value) || value <= 0) return "";
      const parts = new Intl.DateTimeFormat("en-CA", {
        timeZone: "America/Vancouver",
        year: "numeric",
        month: "2-digit",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
        second: "2-digit",
        hourCycle: "h23",
      }).formatToParts(new Date(value)).reduce((acc, part) => {
        if (part.type !== "literal") acc[part.type] = part.value;
        return acc;
      }, {});
      return `${parts.year}-${parts.month}-${parts.day}T${parts.hour}:${parts.minute}:${parts.second}`;
    }
    function syncReplayInputsFromViewport() {
      if (!processStartInput || !processEndInput) return;
      if (!processStartInput.value && windowStartMs > 0) {
        processStartInput.value = vancouverInputValue(windowStartMs);
      }
      if (!processEndInput.value && windowEndMs > 0) {
        processEndInput.value = vancouverInputValue(windowEndMs);
      }
    }
    function signalTriggerTime(signal) {
      return Number(signal?.trigger_candle_time_ms ?? 0);
    }
    function signalMarkerShape(signal) {
      return String(signal?.marker_shape || (String(signal?.signal_type || "").startsWith("EXIT_") ? "SQUARE" : "ARROW")).trim().toUpperCase();
    }
    function triggerMarkerBounds(signal, centerX, yHigh, yLow) {
      const shape = signalMarkerShape(signal);
      const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
      const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
      const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
      if (shape === "SQUARE") {
        const size = 10;
        const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
        return { left: centerX - size / 2, right: centerX + size / 2, top: centerY - size / 2, bottom: centerY + size / 2 };
      }
      if (markerDirection === "DOWN" && markerColor === "RED") {
        const tipY = Math.max(3, yHigh - 4);
        const baseY = Math.max(1, tipY - 12);
        return { left: centerX - 8, right: centerX + 8, top: baseY - 3, bottom: tipY + 4 };
      }
      if (markerDirection === "UP" && markerColor === "GREEN") {
        const tipY = yLow + 4;
        const baseY = tipY + 12;
        return { left: centerX - 8, right: centerX + 8, top: tipY - 4, bottom: baseY + 3 };
      }
      return null;
    }
    function triggerMarkerAt(candle, centerX, yHigh, yLow, x, y) {
      for (const signal of [...candleTriggerSignals(candle)].reverse()) {
        if (signalMarkerShape(signal) !== "ARROW") continue;
        const bounds = triggerMarkerBounds(signal, centerX, yHigh, yLow);
        if (!bounds) continue;
        const pad = 4;
        if (x >= bounds.left - pad && x <= bounds.right + pad && y >= bounds.top - pad && y <= bounds.bottom + pad) {
          return signal;
        }
      }
      return null;
    }
    function triggerTimeLabel(ms) { return timeLabel(Number(ms)); }
    function triggerTooltipText(signal) {
      const confirmationState = String(signal?.confirmation_state || "").trim() || "CONFIRMED";
      return `${signal?.signal_type || "TRIGGER"}
Reference candle ${triggerTimeLabel(signal?.reference_candle_time_ms) || "N/A"}
${confirmationState} ${triggerTimeLabel(signal?.confirmation_candle_time_ms || signal?.break_confirmed_candle_time_ms) || "N/A"}
Contract spike score ${fmtMaybe(signal?.contract_spike_score ?? signal?.spike_score, 3)}`;
    }
    function placeTooltip(tooltip, hover, plotW, plotH, fallbackWidth = 300, fallbackHeight = 100) {
      tooltip.style.opacity = "1";
      const tooltipWidth = tooltip.offsetWidth || fallbackWidth;
      const tooltipHeight = tooltip.offsetHeight || fallbackHeight;
      let left = hover.x + 12;
      if (left + tooltipWidth + 8 > plotW) left = hover.x - tooltipWidth - 12;
      left = Math.max(0, Math.min(left, Math.max(0, plotW - tooltipWidth - 8)));
      let top = hover.y + 12;
      if (top + tooltipHeight + 8 > plotH) top = hover.y - tooltipHeight - 12;
      top = Math.max(0, Math.min(top, Math.max(0, plotH - tooltipHeight - 8)));
      tooltip.style.left = `${left}px`;
      tooltip.style.top = `${top}px`;
    }
    function drawTriggerMarkers(ctx, candle, centerX, yHigh, yLow) {
      for (const signal of candleTriggerSignals(candle)) {
        const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
        const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
        const markerShape = signalMarkerShape(signal);
        const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
        const fillColor = markerColor === "GREEN" ? "#3fb950" : "#f85149";
        if (markerShape === "SQUARE") {
          const size = 10;
          const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
          ctx.fillStyle = fillColor;
          ctx.fillRect(centerX - size / 2, centerY - size / 2, size, size);
          ctx.strokeStyle = "#0d1117";
          ctx.lineWidth = 1.5;
          ctx.strokeRect(centerX - size / 2, centerY - size / 2, size, size);
        } else if (markerDirection === "DOWN" && markerColor === "RED") {
          const tipY = Math.max(3, yHigh - 4);
          const baseY = Math.max(1, tipY - 12);
          ctx.fillStyle = "#f85149";
          ctx.beginPath();
          ctx.moveTo(centerX, tipY);
          ctx.lineTo(centerX - 7, baseY);
          ctx.lineTo(centerX + 7, baseY);
          ctx.closePath();
          ctx.fill();
        } else if (markerDirection === "UP" && markerColor === "GREEN") {
          const tipY = yLow + 4;
          const baseY = tipY + 12;
          ctx.fillStyle = "#3fb950";
          ctx.beginPath();
          ctx.moveTo(centerX, tipY);
          ctx.lineTo(centerX - 7, baseY);
          ctx.lineTo(centerX + 7, baseY);
          ctx.closePath();
          ctx.fill();
        }
      }
    }
    function binIndex(bin, size) {
      const explicit = Number(bin?.index ?? bin?.bin_index);
      if (Number.isFinite(explicit)) return explicit;
      const low = maybeNum(bin?.bin_low ?? bin?.low);
      return size > 0 && Number.isFinite(low) ? Math.floor((low + size * 1e-9) / size) : NaN;
    }
    function priceForBinIndex(index, size) { return index * size; }
    function binPayloadField(bin, key) { return bin?.l2?.[key] ?? bin?.[key]; }
    function binTotal(bin) { return num(binPayloadField(bin, "total_contracts") ?? binPayloadField(bin, "total_volume")); }
    function binBuy(bin) { return num(binPayloadField(bin, "buy_contracts") ?? binPayloadField(bin, "ask_traded_contracts") ?? binPayloadField(bin, "ask_traded_volume") ?? binPayloadField(bin, "buy_volume")); }
    function binSell(bin) { return num(binPayloadField(bin, "sell_contracts") ?? binPayloadField(bin, "bid_traded_contracts") ?? binPayloadField(bin, "bid_traded_volume") ?? binPayloadField(bin, "sell_volume")); }
    function binDelta(bin) { return num(binPayloadField(bin, "horizontal_contract_delta") ?? binPayloadField(bin, "contract_delta") ?? binPayloadField(bin, "horizontal_delta") ?? binPayloadField(bin, "delta")); }
    function candleDeltaValue(candle, field) {
      const raw = candle?.[field];
      if (raw !== null && raw !== undefined) return num(raw);
      if (field !== "delta_contracts") return NaN;
      const bins = safeArray(candle?.bins);
      return bins.length ? bins.reduce((sum, bin) => sum + binDelta(bin), 0) : NaN;
    }
    function binContractSpikeScore(bin) { return maybeNum(binPayloadField(bin, "contract_spike_score")); }
    function binDominantSide(bin) {
      const buyContracts = binBuy(bin);
      const sellContracts = binSell(bin);
      if (buyContracts > sellContracts) return "BUY";
      if (sellContracts > buyContracts) return "SELL";
      return "NONE";
    }
    function binEfficiency(bin) { return maybeNum(binPayloadField(bin, "dominant_side_efficiency")); }
    function binPrice(bin, size) {
      const index = binIndex(bin, size);
      return Number.isFinite(index) ? priceForBinIndex(index, size) : NaN;
    }
    function binIsInRequiredWick(candle, bin, size, side) {
      const open = ohlc(candle, "open");
      const high = ohlc(candle, "high");
      const low = ohlc(candle, "low");
      const close = ohlc(candle, "close");
      const price = binPrice(bin, size);
      if (![open, high, low, close, price].every(Number.isFinite)) return false;
      const bodyLow = Math.min(open, close);
      const bodyHigh = Math.max(open, close);
      if (side === "BUY") return bodyHigh <= price && price <= high;
      if (side === "SELL") return low <= price && price <= bodyLow;
      return false;
    }
    function candleWickBandBounds(candle, side, priceToY) {
      const open = ohlc(candle, "open");
      const high = ohlc(candle, "high");
      const low = ohlc(candle, "low");
      const close = ohlc(candle, "close");
      if (![open, high, low, close].every(Number.isFinite)) return null;
      const bodyLow = Math.min(open, close);
      const bodyHigh = Math.max(open, close);
      if (side === "BUY") {
        return { top: priceToY(high), bottom: priceToY(bodyHigh) };
      }
      if (side === "SELL") {
        return { top: priceToY(bodyLow), bottom: priceToY(low) };
      }
      return null;
    }
    function binAbsorptionHighlightStyle(candle, bin, size) {
      const spikeScore = binContractSpikeScore(bin);
      const side = binDominantSide(bin);
      if (!Number.isFinite(spikeScore) || spikeScore < ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN) return null;
      if (!binIsInRequiredWick(candle, bin, size, side)) return null;
      return ABSORPTION_HIGHLIGHT_STYLES[side] || null;
    }
    function binCandleAbsorptionHighlightStyle(bin) {
      const spikeScore = binContractSpikeScore(bin);
      const side = binDominantSide(bin);
      if (!Number.isFinite(spikeScore) || spikeScore < ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN) return null;
      return ABSORPTION_HIGHLIGHT_STYLES[side] || null;
    }
    function candleHasContractAtBinIndex(candle, targetIndex, size) {
      return safeArray(candle?.bins).some(bin => (
        binTotal(bin) > 0
        && binIndex(bin, size) === targetIndex
      ));
    }
    function fmtPrice(value) {
      const abs = Math.abs(num(value));
      if (abs >= 1000) return num(value).toFixed(1);
      if (abs >= 1) return num(value).toFixed(3);
      return num(value).toFixed(6);
    }
    function timeLabel(ms) {
      if (!ms) return "";
      return new Date(ms).toLocaleString([], {
        timeZone: "America/Vancouver",
        month: "short",
        day: "2-digit",
        hour: "2-digit",
        minute: "2-digit",
      });
    }
    function sessionKey(session) {
      return `${session.mt5_symbol || session.provider_symbol || "UNKNOWN"}|${session.timeframe || ACTIVE_TIMEFRAME}`;
    }
    function timeframeLinks() {
      linksEl.innerHTML = TIMEFRAMES.map(timeframe => {
        const active = timeframe === ACTIVE_TIMEFRAME ? " active" : "";
        return `<a class="timeframe-link${active}" href="/candles/${timeframe}">${timeframe}</a>`;
      }).join("");
    }
    class CandleChart {
      constructor(section, session) {
        this.section = section;
        this.host = section.querySelector(".chart-host");
        this.canvas = section.querySelector("canvas");
        this.tooltip = section.querySelector(".tooltip");
        this.scrollbar = section.querySelector(".history-scrollbar");
        this.scrollbarContent = section.querySelector(".history-scrollbar-content");
        this.ctx = this.canvas.getContext("2d");
        this.candles = [];
        this.candleMap = new Map();
        this.cachedWindows = [];
        this.candleWidth = 10;
        this.autoScaleCandleWidth = true;
        this.fixedSize = num(session?.fixed_bin_size);
        this.priceStep = num(session?.price_step);
        this.autoScaleEnabled = true;
        this.verticalScaleFactor = 1;
        this.verticalCenterPrice = NaN;
        this.manualVisualSpan = NaN;
        this.lastVisualRange = null;
        this.lastLayoutSignature = "";
        this.viewEnd = 0;
        this.followRight = true;
        this.hover = null;
        this.horizontalOffsetPx = 0;
        this.syncingScrollbar = false;
        this.programmaticScrollbarLeft = NaN;
        this.programmaticScrollbarUntil = 0;
        this.scrollbarInteracting = false;
        this.scrollbarInteractionTimer = null;
        this.candleWidth = this.defaultCandleWidth();
        this.updateHeader(session);
        this.attachEvents();
      }
      updateHeader(session) {
        const title = this.section.querySelector(".session-title strong");
        const meta = this.section.querySelector(".session-title span");
        const providerSymbol = session.provider_symbol || session.symbol || "";
        if (title) title.textContent = `${session.mt5_symbol || providerSymbol} -> ${providerSymbol}`;
        const unit = String(session.quantity_unit || "").toUpperCase() === "CONTRACTS"
          ? " / contracts"
          : "";
        if (meta) meta.textContent = `${session.market_provider || ""} / ${session.timeframe || ACTIVE_TIMEFRAME}${unit}`;
      }
      clearCachedData() {
        this.candles = [];
        this.candleMap.clear();
        this.cachedWindows = [];
        this.viewEnd = 0;
        this.followRight = true;
        this.lastVisualRange = null;
        this.draw();
        this.syncScrollbar();
      }
      resetAutoScale() {
        this.autoScaleEnabled = true;
        this.verticalScaleFactor = 1;
        this.verticalCenterPrice = NaN;
        this.manualVisualSpan = NaN;
      }
      attachEvents() {
        this.scrollbar.addEventListener("scroll", () => {
          const programmaticScroll = (
            Number.isFinite(this.programmaticScrollbarLeft)
            && Date.now() <= this.programmaticScrollbarUntil
            && Math.abs(this.scrollbar.scrollLeft - this.programmaticScrollbarLeft) <= 2
          );
          if (
            this.syncingScrollbar
            || programmaticScroll
            || this.scrollbar.classList.contains("disabled")
          ) {
            return;
          }
          const maxScroll = Math.max(0, this.scrollbar.scrollWidth - this.scrollbar.clientWidth);
          if (maxScroll <= 0) return;
          this.scrollbarInteracting = true;
          if (this.scrollbarInteractionTimer) clearTimeout(this.scrollbarInteractionTimer);
          this.scrollbarInteractionTimer = setTimeout(() => {
            this.scrollbarInteracting = false;
            this.syncScrollbar();
          }, 140);
          const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
          const limit = this.visibleCapacity();
          const earliest = earliestWindowStartMs || windowStartMs || 0;
          const latest = latestWindowEndMs || windowEndMs || 0;
          const maxStart = Math.max(earliest, latest - limit * intervalMs);
          const ratio = this.scrollbar.scrollLeft / maxScroll;
          const selectedStartMs = Math.round(
            (earliest + ratio * (maxStart - earliest)) / intervalMs,
          ) * intervalMs;
          this.resetAutoScale();
          requestViewportWindow(selectedStartMs + limit * intervalMs, limit);
        });
        this.host.addEventListener("wheel", event => {
          event.preventDefault();
          if (!this.candles.length) return;
          const rect = this.canvas.getBoundingClientRect();
          if (event.shiftKey && this.lastVisualRange) {
            const direction = Math.sign(event.deltaY || event.deltaX);
            const span = this.lastVisualRange.max - this.lastVisualRange.min;
            const currentCenter = Number.isFinite(this.verticalCenterPrice)
              ? this.verticalCenterPrice
              : (this.lastVisualRange.min + this.lastVisualRange.max) / 2;
            this.verticalCenterPrice = currentCenter + direction * span * 0.08;
            if (!Number.isFinite(this.manualVisualSpan)) {
              this.manualVisualSpan = span;
            }
            this.autoScaleEnabled = false;
          } else if (event.altKey && this.lastVisualRange) {
            this.verticalCenterPrice = Number.isFinite(this.verticalCenterPrice)
              ? this.verticalCenterPrice
              : (this.lastVisualRange.min + this.lastVisualRange.max) / 2;
            const currentSpan = Number.isFinite(this.manualVisualSpan)
              ? this.manualVisualSpan
              : this.lastVisualRange.max - this.lastVisualRange.min;
            this.manualVisualSpan = Math.max(
              this.priceStep > 0 ? this.priceStep * 2 : 2,
              currentSpan * (event.deltaY > 0 ? 1.1 : 0.9),
            );
            this.verticalScaleFactor = Math.max(
              0.2,
              Math.min(5, this.verticalScaleFactor * (event.deltaY > 0 ? 1.1 : 0.9)),
            );
            this.autoScaleEnabled = false;
          } else if (event.ctrlKey) {
            const next = this.candleWidth * (event.deltaY > 0 ? 0.9 : 1.1);
            this.candleWidth = Math.max(2, Math.min(36, next));
            this.autoScaleCandleWidth = false;
            scheduleViewportResize(windowEndMs || null, this.visibleCapacity());
          } else {
            this.resetAutoScale();
            const primaryDelta = normalizedWheelDelta(event, rect.width);
            const candleSteps = this.scrollByPixels(primaryDelta);
            if (candleSteps !== 0) {
              requestViewportWindow(
                this.currentViewportEndTimeMs(),
                this.visibleCapacity(),
                false,
              );
            }
            this.syncScrollbar();
            return;
          }
          this.draw();
        }, { passive: false });
        this.host.addEventListener("mousemove", event => {
          const rect = this.canvas.getBoundingClientRect();
          this.hover = { x: event.clientX - rect.left, y: event.clientY - rect.top };
          this.draw();
        });
        this.host.addEventListener("mouseleave", () => {
          this.hover = null;
          this.tooltip.style.opacity = "0";
          this.draw();
        });
        this.host.addEventListener("dblclick", event => {
          const rect = this.canvas.getBoundingClientRect();
          if (event.clientX - rect.left < rect.width - 82) return;
          this.autoScaleEnabled = true;
          this.verticalScaleFactor = 1;
          this.verticalCenterPrice = NaN;
          this.manualVisualSpan = NaN;
          this.draw();
        });
        window.addEventListener("resize", () => {
          if (this.autoScaleCandleWidth) this.candleWidth = this.defaultCandleWidth();
          this.draw();
          this.syncScrollbar();
        });
      }
      currentViewportEndTimeMs() {
        if (!this.candles.length) return 0;
        const endIndex = Math.max(0, Math.min(this.viewEnd, this.candles.length));
        if (endIndex < this.candles.length) {
          return candleOpen(this.candles[endIndex]);
        }
        return (
          candleOpen(this.candles[this.candles.length - 1])
          + TIMEFRAME_MS[ACTIVE_TIMEFRAME]
        );
      }
      positionViewport(endTimeMs, candleLimit = this.visibleCapacity()) {
        if (!this.candles.length) return;
        const targetEnd = Number(endTimeMs);
        const firstAfter = this.candles.findIndex(
          candle => candleOpen(candle) >= targetEnd,
        );
        this.followRight = false;
        this.viewEnd = firstAfter < 0 ? this.candles.length : firstAfter;
        this.viewEnd = Math.max(
          Math.min(this.viewEnd, this.candles.length),
          Math.min(Math.max(1, candleLimit), this.candles.length),
        );
      }
      canPreviewViewport(endTimeMs, candleLimit = this.visibleCapacity()) {
        if (!this.candles.length) return false;
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        const targetEnd = Number(endTimeMs);
        const coverage = this.cachedWindows.find(window => (
          targetEnd > window.startMs
          && targetEnd <= window.endMs + intervalMs
        ));
        if (!coverage) return false;
        const available = this.candles.filter(candle => {
          const openTime = candleOpen(candle);
          return openTime >= coverage.startMs && openTime < targetEnd;
        }).length;
        return available >= Math.max(1, candleLimit);
      }
      rememberCachedWindow(session) {
        const candles = safeArray(session?.candles);
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        let startMs = Number(session?.window_start_ms);
        let endMs = Number(session?.window_end_ms);
        if ((!Number.isFinite(startMs) || startMs <= 0) && candles.length) {
          startMs = candleOpen(candles[0]);
        }
        if ((!Number.isFinite(endMs) || endMs <= startMs) && candles.length) {
          endMs = candleOpen(candles[candles.length - 1]) + intervalMs;
        }
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
          return;
        }
        const windows = [
          ...this.cachedWindows,
          { startMs, endMs },
        ].sort((left, right) => left.startMs - right.startMs);
        const merged = [];
        for (const window of windows) {
          const previous = merged[merged.length - 1];
          if (previous && window.startMs <= previous.endMs + intervalMs) {
            previous.endMs = Math.max(previous.endMs, window.endMs);
          } else {
            merged.push({ ...window });
          }
        }
        this.cachedWindows = merged;
      }
      cacheSession(session) {
        const anchorEndMs = this.followRight ? 0 : this.currentViewportEndTimeMs();
        this.rememberCachedWindow(session);
        this.updateHeader(session);
        const priceStep = num(session?.price_step);
        const size = num(session?.fixed_bin_size);
        if (size > 0) this.fixedSize = size;
        if (priceStep > 0) this.priceStep = priceStep;
        for (const candle of safeArray(session.candles)) {
          const openTime = candleOpen(candle);
          const replayMarkers = replayMarkersByCandleOpen.get(openTime);
          if (replayMarkers?.length) {
            candle.dom_refill_markers = mergeDomRefillMarkers(
              candle.dom_refill_markers,
              replayMarkers,
            );
          }
          const key = String(openTime);
          if (key !== "0") this.candleMap.set(key, candle);
        }
        this.candles = [...this.candleMap.values()].sort((a, b) => candleOpen(a) - candleOpen(b));
        const signalsByOpen = new Map();
        for (const signal of safeArray(session?.signals)) {
          const openTime = signalTriggerTime(signal);
          if (openTime <= 0) continue;
          const signals = signalsByOpen.get(openTime) || [];
          signals.push(signal);
          signalsByOpen.set(openTime, signals);
        }
        for (const candle of this.candles) {
          const signalsById = new Map();
          for (const signal of [
            ...candleTriggerSignals(candle),
            ...(signalsByOpen.get(candleOpen(candle)) || []),
          ]) {
            const signalId = String(signal?.signal_id || "").trim();
            const key = signalId || `${signal?.direction || ""}|${signalTriggerTime(signal)}`;
            signalsById.set(key, signal);
          }
          candle.trigger_signals = [...signalsById.values()];
        }
        const clientCacheLimit = 5000;
        if (this.candles.length > clientCacheLimit) {
          const targetEnd = anchorEndMs || requestedWindowEndMs || windowEndMs || Infinity;
          const targetIndex = this.candles.findIndex(candle => candleOpen(candle) >= targetEnd);
          const center = targetIndex < 0 ? this.candles.length : targetIndex;
          const start = Math.max(0, Math.min(
            this.candles.length - clientCacheLimit,
            center - Math.floor(clientCacheLimit / 2),
          ));
          this.candles = this.candles.slice(start, start + clientCacheLimit);
          this.candleMap = new Map(
            this.candles.map(candle => [String(candleOpen(candle)), candle]),
          );
        }
        if (anchorEndMs > 0) {
          this.positionViewport(anchorEndMs, this.visibleCapacity());
        }
      }
      mergeSession(session) {
        this.cacheSession(session);
        if (requestedWindowEndMs) {
          this.previewViewport(requestedWindowEndMs, this.visibleCapacity(), false);
        } else if (this.followRight || this.viewEnd <= 0) {
          this.viewEnd = this.candles.length;
        }
        this.draw();
        this.syncScrollbar();
      }
      visibleCapacity() {
        const rect = this.host.getBoundingClientRect();
        const layout = this.layoutWidths(rect.width);
        return Math.max(20, Math.floor(Math.max(1, layout.candlePlotW) / this.candleWidth));
      }
      previewViewport(endTimeMs, candleLimit = this.visibleCapacity(), redraw = true) {
        this.positionViewport(endTimeMs, candleLimit);
        if (redraw) this.draw();
      }
      scrollByPixels(deltaPixels) {
        if (!Number.isFinite(deltaPixels) || deltaPixels === 0) return 0;
        const visible = this.visibleCapacity();
        if (this.followRight || this.viewEnd <= 0) {
          this.viewEnd = this.candles.length;
        }
        this.followRight = false;
        const scaledDelta = Math.max(
          -48,
          Math.min(48, deltaPixels * 0.7),
        );
        this.horizontalOffsetPx -= scaledDelta;
        let candleDelta = 0;
        while (this.horizontalOffsetPx >= this.candleWidth) {
          this.horizontalOffsetPx -= this.candleWidth;
          candleDelta -= 1;
          if (this.viewEnd > Math.min(visible, this.candles.length)) {
            this.viewEnd -= 1;
          }
        }
        while (this.horizontalOffsetPx <= -this.candleWidth) {
          this.horizontalOffsetPx += this.candleWidth;
          candleDelta += 1;
          if (this.viewEnd < this.candles.length) {
            this.viewEnd += 1;
          }
        }
        this.draw();
        return candleDelta;
      }
      cachedOpenTimes(endTimeMs, candleLimit = this.visibleCapacity()) {
        const targetEnd = Number(endTimeMs);
        return this.candles
          .filter(candle => candleOpen(candle) < targetEnd)
          .slice(-Math.max(1, candleLimit))
          .map(candle => candleOpen(candle));
      }
      layoutWidths(width) {
        const axisWidth = 82;
        const contentW = Math.max(1, width - axisWidth);
        return {
          axisWidth,
          contentW,
          candlePlotX: 0,
          candlePlotW: contentW,
        };
      }
      defaultCandleWidth() {
        const rect = this.host.getBoundingClientRect();
        const layout = this.layoutWidths(rect.width || window.innerWidth);
        const targetVisible = Math.max(20, Number(CANDLE_VISUAL_CONFIG.defaultVisibleCandles) || 80);
        return Math.max(2, Math.min(36, layout.candlePlotW / targetVisible));
      }
      visibleCandleItems(layout) {
        const visible = this.visibleCapacity();
        if (this.followRight || this.viewEnd <= 0) this.viewEnd = this.candles.length;
        this.viewEnd = Math.max(Math.min(this.viewEnd, this.candles.length), Math.min(visible, this.candles.length));
        const start = Math.max(0, this.viewEnd - visible);
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        const previousIsAdjacent = (
          start > 0
          && candleOpen(this.candles[start]) - candleOpen(this.candles[start - 1]) === intervalMs
        );
        const nextIsAdjacent = (
          this.viewEnd > 0
          && this.viewEnd < this.candles.length
          && candleOpen(this.candles[this.viewEnd])
            - candleOpen(this.candles[this.viewEnd - 1]) === intervalMs
        );
        const renderStart = previousIsAdjacent ? start - 1 : start;
        const renderEnd = nextIsAdjacent ? this.viewEnd + 1 : this.viewEnd;
        return this.candles
          .slice(renderStart, renderEnd)
          .map((candle, index) => ({
            candle,
            x: (
              layout.candlePlotX
              + (renderStart + index - start) * this.candleWidth
              + this.horizontalOffsetPx
            ),
          }))
          .filter(item => (
            item.x + this.candleWidth > layout.candlePlotX
            && item.x < layout.contentW
          ));
      }
      visibleScaleCandles() {
        const visible = this.visibleCapacity();
        if (this.followRight || this.viewEnd <= 0) this.viewEnd = this.candles.length;
        this.viewEnd = Math.max(Math.min(this.viewEnd, this.candles.length), Math.min(visible, this.candles.length));
        const start = Math.max(0, this.viewEnd - visible);
        return this.candles.slice(start, this.viewEnd);
      }
      resizeCanvas() {
        const rect = this.host.getBoundingClientRect();
        const ratio = window.devicePixelRatio || 1;
        const width = Math.max(1, Math.floor(rect.width));
        const height = Math.max(320, Math.floor(rect.height));
        if (this.canvas.width !== Math.floor(width * ratio) || this.canvas.height !== Math.floor(height * ratio)) {
          this.canvas.width = Math.floor(width * ratio);
          this.canvas.height = Math.floor(height * ratio);
          this.canvas.style.width = `${width}px`;
          this.canvas.style.height = `${height}px`;
        }
        this.ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
        return { width, height };
      }
      syncScrollbar() {
        const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
        const limit = this.visibleCapacity();
        const earliest = earliestWindowStartMs || windowStartMs || 0;
        const latest = latestWindowEndMs || windowEndMs || 0;
        const maxStart = Math.max(earliest, latest - limit * intervalMs);
        const targetEnd = requestedWindowEndMs || windowEndMs || latest;
        const currentStart = Math.max(
          earliest,
          Math.min(maxStart, targetEnd - limit * intervalMs),
        );
        const disabled = earliest <= 0 || maxStart <= earliest;
        this.scrollbar.classList.toggle("disabled", disabled);
        this.scrollbar.setAttribute("aria-disabled", disabled ? "true" : "false");
        const totalCandles = Math.max(limit, Math.ceil((latest - earliest) / intervalMs));
        const contentWidth = disabled
          ? this.scrollbar.clientWidth
          : Math.min(16_000_000, Math.max(this.scrollbar.clientWidth + 1, totalCandles * this.candleWidth));
        this.scrollbarContent.style.width = `${Math.ceil(contentWidth)}px`;
        if (disabled || this.scrollbarInteracting) return;
        const maxScroll = Math.max(0, this.scrollbar.scrollWidth - this.scrollbar.clientWidth);
        const ratio = maxStart > earliest ? (currentStart - earliest) / (maxStart - earliest) : 0;
        const nextScrollLeft = ratio * maxScroll;
        this.syncingScrollbar = true;
        this.programmaticScrollbarLeft = nextScrollLeft;
        this.programmaticScrollbarUntil = Date.now() + 250;
        this.scrollbar.scrollLeft = nextScrollLeft;
        requestAnimationFrame(() => { this.syncingScrollbar = false; });
      }
      computeCandleDataPriceRange(candles) {
        let minPrice = Infinity;
        let maxPrice = -Infinity;
        const includePrice = value => {
          const price = maybeNum(value);
          if (!Number.isFinite(price)) return;
          minPrice = Math.min(minPrice, price);
          maxPrice = Math.max(maxPrice, price);
        };
        for (const candle of candles) {
          for (const key of ["open", "high", "low", "close"]) {
            includePrice(ohlc(candle, key));
          }
          for (const marker of candleDomRefillMarkers(candle)) {
            includePrice(marker?.price ?? marker?.level_price ?? marker?.reference_price);
          }
          for (const marker of safeArray(replayMarkersByCandleOpen.get(candleOpen(candle)))) {
            includePrice(marker?.price ?? marker?.level_price ?? marker?.reference_price);
          }
        }
        if (!Number.isFinite(minPrice) || !Number.isFinite(maxPrice) || minPrice === maxPrice) {
          minPrice = 0;
          maxPrice = 1;
        }
        return { min: minPrice, max: maxPrice };
      }
      computeVisualRenderRange(candleRange) {
        const dataRange = candleRange;
        const tickSize = this.priceStep > 0 ? this.priceStep : 1;
        const dataSpan = Math.max(tickSize, dataRange.max - dataRange.min);
        const paddingPrice = Math.max(
          tickSize * Math.max(0, Number(CANDLE_VISUAL_CONFIG.minVerticalPaddingTicks) || 0),
          dataSpan * Math.max(0, Number(CANDLE_VISUAL_CONFIG.verticalPaddingPercent) || 0),
        );
        const paddedMin = dataRange.min - paddingPrice;
        const paddedMax = dataRange.max + paddingPrice;
        if (this.autoScaleEnabled) {
          this.verticalCenterPrice = (paddedMin + paddedMax) / 2;
          this.verticalScaleFactor = 1;
          return {
            min: paddedMin,
            max: paddedMax,
            paddingTicks: paddingPrice / tickSize,
          };
        }
        const center = Number.isFinite(this.verticalCenterPrice)
          ? this.verticalCenterPrice
          : (paddedMin + paddedMax) / 2;
        const manualSpan = Number.isFinite(this.manualVisualSpan)
          ? this.manualVisualSpan
          : paddedMax - paddedMin;
        const halfSpan = Math.max(tickSize, manualSpan / 2);
        return {
          min: center - halfSpan,
          max: center + halfSpan,
          paddingTicks: paddingPrice / tickSize,
        };
      }
      draw() {
        const { width, height } = this.resizeCanvas();
        const ctx = this.ctx;
        ctx.clearRect(0, 0, width, height);
        ctx.fillStyle = "#0b0f14";
        ctx.fillRect(0, 0, width, height);
        if (!this.candles.length) {
          ctx.fillStyle = "#8b949e";
          ctx.fillText(`No ${ACTIVE_TIMEFRAME} candles yet`, 18, 28);
          return;
        }
        const bottomAxis = 34;
        const deltaTableHeight = 66;
        const plotH = Math.max(160, height - bottomAxis - deltaTableHeight);
        const layout = this.layoutWidths(width);
        const candleItems = this.visibleCandleItems(layout);
        const scaleCandles = this.visibleScaleCandles();
        const candleRange = this.computeCandleDataPriceRange(scaleCandles);
        const range = this.computeVisualRenderRange(candleRange);
        this.lastVisualRange = range;
        const priceToY = price => {
          const ratio = (range.max - price) / Math.max(1e-9, range.max - range.min);
          return Math.max(0, Math.min(plotH, ratio * plotH));
        };
        this.drawGrid(ctx, layout, plotH);
        const layoutMetrics = {
          candleDataMinPrice: Number(candleRange.min.toFixed(6)),
          candleDataMaxPrice: Number(candleRange.max.toFixed(6)),
          visualMinPrice: Number(range.min.toFixed(6)),
          visualMaxPrice: Number(range.max.toFixed(6)),
          verticalPaddingTicks: Number(range.paddingTicks.toFixed(2)),
          visibleCandles: scaleCandles.length,
          actualBinPixelHeight: 0,
        };
        for (const [key, value] of Object.entries(layoutMetrics)) {
          this.section.dataset[key] = String(value);
        }
        this.section.dataset.horizontalOffsetPx = String(
          Number(this.horizontalOffsetPx.toFixed(2)),
        );
        this.section.dataset.candleWidthPx = String(
          Number(this.candleWidth.toFixed(2)),
        );
        this.section.dataset.firstVisibleOpenTimeMs = String(
          scaleCandles.length ? candleOpen(scaleCandles[0]) : 0,
        );
        this.section.dataset.lastVisibleOpenTimeMs = String(
          scaleCandles.length ? candleOpen(scaleCandles[scaleCandles.length - 1]) : 0,
        );
        const layoutSignature = JSON.stringify(layoutMetrics);
        if (layoutSignature !== this.lastLayoutSignature) {
          console.info(
            `CANDLE_LAYOUT | candle_data_min=${layoutMetrics.candleDataMinPrice}`
            + ` | candle_data_max=${layoutMetrics.candleDataMaxPrice}`
            + ` | visual_min=${layoutMetrics.visualMinPrice}`
            + ` | visual_max=${layoutMetrics.visualMaxPrice}`
            + ` | padding_ticks=${layoutMetrics.verticalPaddingTicks}`
            + ` | visible_candles=${layoutMetrics.visibleCandles}`
            + ` | actual_bin_px=${layoutMetrics.actualBinPixelHeight}`,
          );
          this.lastLayoutSignature = layoutSignature;
        }
        candleItems.forEach(item => (
          this.drawCandle(ctx, item.candle, item.x, plotH, priceToY)
        ));
        this.drawDomRefillMarkers(ctx, candleItems, layout, plotH, priceToY);
        this.drawDeltaTable(ctx, candleItems, layout, plotH, deltaTableHeight);
        this.drawAxes(ctx, candleItems, layout, plotH, range, priceToY, plotH + deltaTableHeight);
        this.drawHover(ctx, candleItems, layout, plotH, range);
      }
      drawDeltaTable(ctx, candleItems, layout, top, tableHeight) {
        const rows = [
          ["Delta", "delta_contracts"],
          ["NY Session Cum", "session_cumulative_delta"],
          ["Day Cum", "day_cumulative_delta"],
        ];
        const rowHeight = tableHeight / rows.length;
        ctx.fillStyle = "#0f141b";
        ctx.fillRect(0, top, layout.contentW + layout.axisWidth, tableHeight);
        ctx.strokeStyle = "#30363d";
        ctx.lineWidth = 1;
        ctx.textBaseline = "middle";
        rows.forEach(([label, field], rowIndex) => {
          const y = top + rowIndex * rowHeight;
          ctx.beginPath();
          ctx.moveTo(0, y);
          ctx.lineTo(layout.contentW + layout.axisWidth, y);
          ctx.stroke();
          candleItems.forEach(item => {
            const centerX = item.x + this.candleWidth / 2;
            if (centerX >= layout.contentW) return;
            const value = Math.trunc(candleDeltaValue(item.candle, field));
            if (!Number.isFinite(value)) return;
            ctx.font = "600 14px Arial Narrow, Segoe UI, Arial";
            ctx.fillStyle = value > 0 ? "#3fb950" : value < 0 ? "#f85149" : "#8b949e";
            ctx.textAlign = "center";
            ctx.fillText(String(value), centerX, y + rowHeight / 2);
          });
          ctx.font = "600 11px Arial Narrow, Segoe UI, Arial";
          ctx.fillStyle = "#c9d1d9";
          ctx.textAlign = "right";
          ctx.fillText(label, layout.contentW + layout.axisWidth - 6, y + rowHeight / 2);
        });
        ctx.beginPath();
        ctx.moveTo(layout.contentW, top);
        ctx.lineTo(layout.contentW, top + tableHeight);
        ctx.stroke();
      }
      drawGrid(ctx, layout, plotH) {
        ctx.strokeStyle = "rgba(48,54,61,.55)";
        ctx.lineWidth = 1;
        for (let y = 0; y <= plotH; y += 44) {
          ctx.beginPath();
          ctx.moveTo(0, y);
          ctx.lineTo(layout.contentW + layout.axisWidth, y);
          ctx.stroke();
        }
      }
      drawDomRefillMarkers(ctx, candleItems, layout, plotH, priceToY) {
        ctx.save();
        ctx.lineWidth = 2.5;
        ctx.font = "800 13px Segoe UI, Arial";
        ctx.textAlign = "left";
        ctx.textBaseline = "bottom";
        for (const item of candleItems) {
          const markers = mergeDomRefillMarkers(
            candleDomRefillMarkers(item.candle),
            replayMarkersByCandleOpen.get(candleOpen(item.candle)),
          );
          for (const marker of markers) {
            const price = maybeNum(marker?.price ?? marker?.level_price ?? marker?.reference_price);
            const refillCount = domRefillMarkerCount(marker);
            if (!Number.isFinite(price) || refillCount < DOM_REFILL_MARKER_MIN_COUNT) continue;
            const y = priceToY(price);
            if (!Number.isFinite(y) || y < -6 || y > plotH + 6) continue;
            const span = Math.max(1, Number(marker?.span_candles) || 5);
            const startX = Math.max(layout.candlePlotX, item.x);
            const endX = Math.min(layout.contentW, item.x + span * this.candleWidth);
            if (endX <= layout.candlePlotX || startX >= layout.contentW || endX <= startX) {
              continue;
            }
            const color = domRefillMarkerColor(marker);
            ctx.strokeStyle = color;
            ctx.fillStyle = color;
            ctx.shadowColor = "rgba(255,255,255,.55)";
            ctx.shadowBlur = 3;
            ctx.setLineDash([8, 5]);
            ctx.beginPath();
            ctx.moveTo(startX, y);
            ctx.lineTo(endX, y);
            ctx.stroke();
            ctx.shadowBlur = 0;
            ctx.setLineDash([]);
            ctx.fillText(String(refillCount), Math.min(endX + 4, layout.contentW - 28), y - 3);
          }
        }
        ctx.restore();
      }
      drawCandle(ctx, candle, x, plotH, priceToY) {
        const center = x + this.candleWidth / 2;
        const open = ohlc(candle, "open");
        const high = ohlc(candle, "high");
        const low = ohlc(candle, "low");
        const close = ohlc(candle, "close");
        if (![open, high, low, close].every(Number.isFinite)) return;
        const yHigh = priceToY(high);
        const yLow = priceToY(low);
        const yOpen = priceToY(open);
        const yClose = priceToY(close);
        const bull = close >= open;
        ctx.strokeStyle = bull ? "rgba(63,185,80,.95)" : "rgba(248,81,73,.95)";
        ctx.fillStyle = bull ? "#238636" : "#da3633";
        ctx.beginPath();
        ctx.moveTo(center, yHigh);
        ctx.lineTo(center, yLow);
        ctx.stroke();
        const bodyTop = Math.min(yOpen, yClose);
        const bodyH = Math.max(1, Math.abs(yOpen - yClose));
        const bodyW = Math.max(2, Math.min(this.candleWidth - 1, 9));
        ctx.fillRect(center - bodyW / 2, bodyTop, bodyW, bodyH);
        ctx.strokeRect(center - bodyW / 2, bodyTop, bodyW, bodyH);
        drawTriggerMarkers(ctx, candle, center, yHigh, yLow);
      }
      drawAxes(ctx, candleItems, layout, plotH, range, priceToY, timeAxisY = plotH) {
        ctx.fillStyle = "#101720";
        ctx.fillRect(layout.contentW, 0, layout.axisWidth, plotH);
        ctx.strokeStyle = "#30363d";
        ctx.beginPath();
        ctx.moveTo(layout.contentW, 0);
        ctx.lineTo(layout.contentW, plotH);
        ctx.moveTo(layout.candlePlotX, timeAxisY);
        ctx.lineTo(layout.contentW, timeAxisY);
        ctx.stroke();
        ctx.fillStyle = "#8b949e";
        ctx.font = "10px Segoe UI, Arial";
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        for (let i = 0; i <= 6; i += 1) {
          const price = range.min + (range.max - range.min) * (i / 6);
          ctx.fillText(fmtPrice(price), layout.contentW + layout.axisWidth - 8, priceToY(price));
        }
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        const step = Math.max(1, Math.ceil(80 / Math.max(1, this.candleWidth)));
        candleItems.forEach((item, index) => {
          if (index % step !== 0 && index !== candleItems.length - 1) return;
          ctx.fillText(
            timeLabel(candleOpen(item.candle)),
            item.x + this.candleWidth / 2,
            timeAxisY + 9,
          );
        });
      }
      drawHover(ctx, candleItems, layout, plotH, range) {
        if (
          !this.hover
          || this.hover.x < layout.candlePlotX
          || this.hover.x > layout.contentW
          || this.hover.y < 0
          || this.hover.y > plotH
        ) {
          this.tooltip.style.opacity = "0";
          return;
        }
        const hoveredItem = candleItems.find(item => (
          this.hover.x >= item.x
          && this.hover.x < item.x + this.candleWidth
        ));
        if (!hoveredItem) {
          this.tooltip.style.opacity = "0";
          return;
        }
        const candle = hoveredItem.candle;
        const triggerHigh = ohlc(candle, "high");
        const triggerLow = ohlc(candle, "low");
        if ([triggerHigh, triggerLow].every(Number.isFinite)) {
          const priceToY = value => Math.max(
            0,
            Math.min(
              plotH,
              ((range.max - value) / Math.max(1e-9, range.max - range.min)) * plotH,
            ),
          );
          const triggerSignal = triggerMarkerAt(
            candle,
            hoveredItem.x + this.candleWidth / 2,
            priceToY(triggerHigh),
            priceToY(triggerLow),
            this.hover.x,
            this.hover.y,
          );
          if (triggerSignal) {
            this.tooltip.textContent = triggerTooltipText(triggerSignal);
            placeTooltip(this.tooltip, this.hover, layout.contentW, plotH, 300, 100);
            return;
          }
        }
        const rawPrice = range.max - (this.hover.y / Math.max(1, plotH)) * (range.max - range.min);
        const price = this.priceStep > 0
          ? Math.round(rawPrice / this.priceStep) * this.priceStep
          : rawPrice;
        const markerY = Math.max(
          0,
          Math.min(
            plotH,
            ((range.max - price) / Math.max(1e-9, range.max - range.min)) * plotH,
          ),
        );
        ctx.strokeStyle = "rgba(88,166,255,.75)";
        ctx.beginPath();
        const markerX = hoveredItem.x;
        ctx.moveTo(markerX, 0);
        ctx.lineTo(markerX, plotH);
        ctx.moveTo(0, markerY);
        ctx.lineTo(layout.contentW, markerY);
        ctx.stroke();
        this.drawPriceMarker(
          ctx,
          layout.contentW,
          layout.axisWidth,
          plotH,
          markerY,
          price,
        );
        this.tooltip.textContent = `${ACTIVE_TIMEFRAME} ${new Date(candleOpen(candle)).toLocaleString([], { timeZone: "America/Vancouver" })}
O ${candle.open_price} H ${candle.high_price}
L ${candle.low_price} C ${candle.close_price}`;
        this.tooltip.style.opacity = "1";
        const tooltipWidth = this.tooltip.offsetWidth || 260;
        const tooltipHeight = this.tooltip.offsetHeight || 90;
        let left = this.hover.x + 12;
        if (left + tooltipWidth + 8 > layout.contentW) left = this.hover.x - tooltipWidth - 12;
        left = Math.max(0, Math.min(left, Math.max(0, layout.contentW - tooltipWidth - 8)));
        let top = this.hover.y + 12;
        if (top + tooltipHeight + 8 > plotH) top = this.hover.y - tooltipHeight - 12;
        top = Math.max(0, Math.min(top, Math.max(0, plotH - tooltipHeight - 8)));
        this.tooltip.style.left = `${left}px`;
        this.tooltip.style.top = `${top}px`;
      }
      drawPriceMarker(ctx, plotW, axisWidth, plotH, y, price) {
        const boxHeight = 20;
        const boxY = Math.max(0, Math.min(plotH - boxHeight, y - boxHeight / 2));
        ctx.fillStyle = "#1f6feb";
        ctx.fillRect(plotW + 2, boxY, axisWidth - 4, boxHeight);
        ctx.strokeStyle = "#58a6ff";
        ctx.strokeRect(plotW + 2.5, boxY + 0.5, axisWidth - 5, boxHeight - 1);
        ctx.fillStyle = "#ffffff";
        ctx.font = "150 20px Segoe UI, Arial";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(fmtPrice(price), plotW + axisWidth / 2, boxY + boxHeight / 2);
      }
    }

    function ensureSessionSection(session) {
      const key = sessionKey(session);
      let chart = charts.get(key);
      if (chart) return chart;
      const section = document.createElement("section");
      section.className = "session";
      section.dataset.session = key;
      section.innerHTML = `<div class="session-title"><strong></strong><span></span></div>
        <div class="chart-host"><canvas></canvas><div class="tooltip"></div></div>
        <div class="chart-scrollbar"><div class="history-scrollbar" role="scrollbar" aria-label="Horizontal chart position" aria-orientation="horizontal"><div class="history-scrollbar-content"></div></div></div>`;
      app.appendChild(section);
      chart = new CandleChart(section, session);
      charts.set(key, chart);
      return chart;
    }
    function render(snapshot) {
      const sessions = safeArray(snapshot?.sessions);
      const generated = snapshot?.generated_at_utc ? new Date(snapshot.generated_at_utc).toLocaleTimeString() : new Date().toLocaleTimeString();
      if (Number(snapshot?.earliest_window_start_ms) > 0) {
        earliestWindowStartMs = Number(snapshot.earliest_window_start_ms);
      }
      if (Number(snapshot?.window_start_ms) > 0) windowStartMs = Number(snapshot.window_start_ms);
      if (Number(snapshot?.window_end_ms) > 0) windowEndMs = Number(snapshot.window_end_ms);
      if (Number(snapshot?.latest_window_end_ms) > 0) {
        latestWindowEndMs = Math.max(latestWindowEndMs, Number(snapshot.latest_window_end_ms));
      }
      syncReplayInputsFromViewport();
      hasOlderData = Boolean(snapshot?.has_older_data);
      if (!sessions.length) {
        statusEl.textContent = `Updated ${generated} | timeframe ${ACTIVE_TIMEFRAME} | no sessions`;
        app.innerHTML = `<div class="empty">No ${ACTIVE_TIMEFRAME} candle sessions yet.</div>`;
        return;
      }
      if (app.querySelector(".empty")) app.replaceChildren();
      const activeKeys = new Set(sessions.map(sessionKey));
      for (const [key, chart] of charts) {
        if (!activeKeys.has(key)) {
          chart.section.remove();
          charts.delete(key);
        }
      }
      sessions.forEach(session => ensureSessionSection(session).mergeSession(session));
      const candlesCount = [...charts.values()].reduce((sum, chart) => sum + chart.candles.length, 0);
      const processedTrades = Number(snapshot?.processed_trades || 0);
      statusEl.textContent = `Updated ${generated} | timeframe ${ACTIVE_TIMEFRAME} | viewport candles ${candlesCount} | processed trades ${processedTrades}`;
    }
    function applyReplayMarkersToCharts() {
      let appliedCount = 0;
      for (const chart of charts.values()) {
        for (const [openTimeMs, markers] of replayMarkersByCandleOpen.entries()) {
          const key = String(openTimeMs);
          const candle = chart.candleMap.get(key);
          if (!candle) continue;
          candle.dom_refill_markers = mergeDomRefillMarkers(
            candle.dom_refill_markers,
            markers,
          );
          chart.candleMap.set(key, candle);
          appliedCount += safeArray(markers).length;
        }
        chart.draw();
        chart.syncScrollbar();
      }
      return appliedCount;
    }
    function cacheSnapshot(snapshot) {
      if (!snapshot) return;
      for (const session of safeArray(snapshot?.sessions)) {
        const chart = charts.get(sessionKey(session));
        if (chart) chart.cacheSession(session);
      }
    }
    function requestedCandleLimit() {
      if (charts.size) {
        return Math.max(20, ...[...charts.values()].map(chart => chart.visibleCapacity()));
      }
      return Math.max(20, Number(CANDLE_VISUAL_CONFIG.defaultVisibleCandles) || 80);
    }
    function scheduleViewportResize(endTimeMs, candleLimit) {
      if (viewportResizeTimer) clearTimeout(viewportResizeTimer);
      viewportResizeTimer = setTimeout(
        () => requestViewportWindow(endTimeMs, candleLimit),
        120,
      );
    }
    function schedulePendingViewportRequest(
      delayMs = VIEWPORT_REQUEST_DEBOUNCE_MS,
    ) {
      if (
        viewportRequestTimer
        || activeViewportRequest
        || !scheduledViewportRequest
      ) {
        return;
      }
      viewportRequestTimer = setTimeout(() => {
        const request = scheduledViewportRequest;
        scheduledViewportRequest = null;
        viewportRequestTimer = null;
        if (request) {
          refresh(request.endTimeMs, request.candleLimit, request.id);
        }
      }, delayMs);
    }
    function requestViewportWindow(
      endTimeMs,
      candleLimit = requestedCandleLimit(),
      preview = true,
    ) {
      const normalizedEndTimeMs = Number(endTimeMs);
      if (!Number.isFinite(normalizedEndTimeMs) || normalizedEndTimeMs <= 0) return;
      const normalizedLimit = Math.max(1, Number(candleLimit) || requestedCandleLimit());
      requestedWindowEndMs = normalizedEndTimeMs;
      if (preview) {
        for (const chart of charts.values()) {
          if (chart.canPreviewViewport(normalizedEndTimeMs, normalizedLimit)) {
            chart.previewViewport(normalizedEndTimeMs, normalizedLimit);
          }
        }
      }
      const requestId = ++latestViewportRequestId;
      scheduledViewportRequest = {
        id: requestId,
        endTimeMs: normalizedEndTimeMs,
        candleLimit: normalizedLimit,
      };
      schedulePendingViewportRequest();
    }
    function navigateCandlesByCount(
      candleDelta,
      candleLimit = requestedCandleLimit(),
      preview = true,
    ) {
      if (!windowEndMs || candleDelta === 0) return;
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME];
      const latestEnd = latestWindowEndMs || windowEndMs;
      const earliestEnd = (earliestWindowStartMs || windowStartMs) + candleLimit * intervalMs;
      const chartEnd = charts.size
        ? [...charts.values()][0].currentViewportEndTimeMs()
        : 0;
      const navigationEnd = requestedWindowEndMs || chartEnd || windowEndMs;
      const desiredEnd = Math.max(
        earliestEnd,
        Math.min(latestEnd, navigationEnd + candleDelta * intervalMs),
      );
      if (desiredEnd === navigationEnd) return;
      requestViewportWindow(desiredEnd, candleLimit, preview);
    }
    function reportClientViewportMetric(eventName, requestId) {
      const params = new URLSearchParams({
        event: eventName,
        view: "candle",
        timeframe: ACTIVE_TIMEFRAME,
        request_id: String(requestId),
      });
      fetch(`/viewport-client-metric?${params.toString()}`, {
        cache: "no-store",
        keepalive: true,
      }).catch(() => {});
    }
    function cancelActiveViewportRequest() {
      if (!activeViewportRequest) return;
      const request = activeViewportRequest;
      activeViewportRequest = null;
      request.controller.abort();
      reportClientViewportMetric("cancelled", request.id);
    }
    function knownOpenTimes(endTimeMs, candleLimit) {
      const values = new Set();
      for (const chart of charts.values()) {
        for (const openTime of chart.cachedOpenTimes(endTimeMs, candleLimit)) {
          values.add(openTime);
        }
      }
      return [...values].sort((a, b) => a - b);
    }
    function replayPayloadTimestampMs(payload) {
      return Number(
        payload?.timestamp_ms
        ?? payload?.event_time_ms
        ?? payload?.threshold_time_ms
        ?? 0,
      );
    }
    function replayViewportEndMs(replay) {
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60_000;
      let latestPayloadTimeMs = 0;
      for (const payload of safeArray(replay?.payloads)) {
        const timestampMs = replayPayloadTimestampMs(payload);
        if (Number.isFinite(timestampMs) && timestampMs > latestPayloadTimeMs) {
          latestPayloadTimeMs = timestampMs;
        }
      }
      if (latestPayloadTimeMs > 0) {
        return Math.floor(latestPayloadTimeMs / intervalMs) * intervalMs + intervalMs;
      }
      const replayEndMs = Number(replay?.end_ms || 0);
      return Number.isFinite(replayEndMs) && replayEndMs > 0 ? replayEndMs : 0;
    }
    async function runProcessReplay(event) {
      event.preventDefault();
      const startValue = String(processStartInput?.value || "").trim();
      const endValue = String(processEndInput?.value || "").trim();
      if (!startValue || !endValue) {
        processReplayStatusEl.textContent = "Set range";
        return;
      }
      processSubmitButton.disabled = true;
      processReplayStatusEl.textContent = "Running";
      try {
        const params = new URLSearchParams({
          start_vancouver: startValue,
          end_vancouver: endValue,
        });
        const response = await fetch(
          `/process-replay/${ACTIVE_TIMEFRAME}?${params.toString()}`,
          { cache: "no-store" },
        );
        const replay = response.ok ? await response.json() : null;
        if (!response.ok || replay?.status !== "OK") {
          throw new Error(replay?.message || "Replay failed");
        }
        const payloadCount = Number(replay?.emitted_payload_count || 0);
        const eventCount = Number(replay?.processed_event_count || 0);
        rememberReplayPayloadMarkers(replay);
        const appliedMarkerCount = applyReplayMarkersToCharts();
        processReplayStatusEl.textContent = (
          `${payloadCount}/${eventCount} CSV`
          + (appliedMarkerCount > 0 ? " drawn" : "")
        );
        processReplayStatusEl.title = [
          replay?.payload_log_path ? `Payloads: ${replay.payload_log_path}` : "",
          replay?.run_log_path ? `Runs: ${replay.run_log_path}` : "",
        ].filter(Boolean).join("\n");
      } catch (error) {
        processReplayStatusEl.textContent = error?.message || "Replay failed";
      } finally {
        processSubmitButton.disabled = false;
      }
    }
    async function refresh(
      endTimeMs = null,
      candleLimit = requestedCandleLimit(),
      scheduledRequestId = null,
    ) {
      const requestId = scheduledRequestId ?? ++latestViewportRequestId;
      if (requestId !== latestViewportRequestId) return;
      const controller = new AbortController();
      activeViewportRequest = { id: requestId, controller };
      try {
        const params = new URLSearchParams();
        const fetchCandleLimit = Math.min(
          500,
          Math.max(1, candleLimit + CANDLE_FETCH_OVERSCAN),
        );
        params.set("candle_limit", String(fetchCandleLimit));
        params.set("include_profiles", "0");
        params.set("request_id", String(requestId));
        if (endTimeMs) {
          params.set("end_time_ms", String(endTimeMs));
          const known = knownOpenTimes(endTimeMs, fetchCandleLimit);
          if (known.length) params.set("known_open_times_ms", known.join(","));
        }
        const suffix = `?${params.toString()}`;
        const response = await fetch(
          `/candles-data/${ACTIVE_TIMEFRAME}${suffix}`,
          { cache: "no-store", signal: controller.signal },
        );
        const snapshot = response.ok ? await response.json() : null;
        if (requestId !== latestViewportRequestId) {
          cacheSnapshot(snapshot);
          reportClientViewportMetric("ignored_obsolete", requestId);
          return;
        }
        render(snapshot);
        if (endTimeMs) requestedWindowEndMs = 0;
        
      } catch (error) {
        if (error?.name !== "AbortError" && requestId === latestViewportRequestId) {
          render(null);
        }
      } finally {
        if (activeViewportRequest?.id === requestId) {
          activeViewportRequest = null;
        }
        if (scheduledViewportRequest) {
          schedulePendingViewportRequest(0);
        }
      }
    }
    window.addEventListener("keydown", event => {
      if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
      const target = event.target;
      if (
        target instanceof HTMLInputElement
        && !target.classList.contains("history-scrollbar")
      ) return;
      event.preventDefault();
      navigateCandlesByCount(
        event.key === "ArrowLeft" ? -3 : 3,
        requestedCandleLimit(),
      );
    });
    processReplayForm.addEventListener("submit", runProcessReplay);
    timeframeLinks();
    refresh();
  </script>
</body>
</html>"""


def _candles_html_page(timeframe: str = DEFAULT_FOOTPRINT_TIMEFRAME) -> str:
    normalized_timeframe = timeframe.strip().upper()
    if normalized_timeframe not in FOOTPRINT_TIMEFRAMES:
        normalized_timeframe = DEFAULT_FOOTPRINT_TIMEFRAME
    return (
        _CANDLES_HTML_TEMPLATE
        .replace("__ACTIVE_TIMEFRAME__", normalized_timeframe)
        .replace("__CONTRACT_SPIKE_THRESHOLD__", str(CONTRACT_SPIKE_THRESHOLD))
    )
