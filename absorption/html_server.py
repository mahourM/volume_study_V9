from __future__ import annotations

import asyncio
<<<<<<< HEAD
import gzip
import json
import logging
import re
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
=======
import json
from typing import Protocol
from urllib.parse import parse_qs


FOOTPRINT_TIMEFRAMES = ("M1", "M5", "M15", "M30", "H1")
DEFAULT_FOOTPRINT_TIMEFRAME = "M15"
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


def _timeframe_for_path(path: str) -> str | None:
    if path in {"/", "/footprint"}:
        return DEFAULT_FOOTPRINT_TIMEFRAME
    prefix = "/footprint/"
    if path.startswith(prefix):
        timeframe = path[len(prefix) :].strip().upper()
        return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None
    return None


<<<<<<< HEAD
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

    def data_process_refill_scan_payload(
        self,
        *,
        timeframe: str | None = None,
        start_vancouver: str,
        end_vancouver: str,
        refill_min: int,
        contracts_min: int = 0,
        activity_filter: str = "",
        rate_min: float | None = None,
        spike_score_min: Decimal | str | float | None = None,
    ) -> dict:
        ...

    def data_process_delete_scan_payload(
        self,
        *,
        timeframe: str | None = None,
        start_vancouver: str,
        end_vancouver: str,
        side: str,
        delete_min: int = 1,
        contracts_min: int = 0,
    ) -> dict:
=======
class SnapshotProvider(Protocol):
    def snapshot_payload(self) -> dict:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
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
<<<<<<< HEAD
            except OSError as exc:
                if is_address_in_use_error(exc):
                    raise
                LOGGER.exception("ABSORPTION_HTTP_SERVER_RESTARTING | error=%s", exc)
                await asyncio.sleep(5)
            except Exception as exc:
                LOGGER.exception("ABSORPTION_HTTP_SERVER_RESTARTING | error=%s", exc)
=======
            except Exception:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
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

<<<<<<< HEAD
            request_headers: dict[str, str] = {}
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            while True:
                header_line = await reader.readline()
                if not header_line or header_line in {b"\r\n", b"\n"}:
                    break
<<<<<<< HEAD
                header_text = header_line.decode("latin-1", errors="ignore")
                header_name, separator, header_value = header_text.partition(":")
                if separator:
                    request_headers[header_name.strip().lower()] = header_value.strip()

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
            elif path == "/refill-scan" or path.startswith("/refill-scan/"):
                timeframe = (
                    _timeframe_for_refill_scan_path(path)
                    or _timeframe_from_query(query_params)
                    or DEFAULT_FOOTPRINT_TIMEFRAME
                )
                status_line, scan_payload = await asyncio.to_thread(
                    _refill_scan_payload_for_timeframe,
                    self.snapshot_provider,
                    timeframe,
                    query_params,
                )
                body = json.dumps(scan_payload, separators=(",", ":")).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            elif path == "/delete-scan" or path.startswith("/delete-scan/"):
                timeframe = (
                    _timeframe_for_delete_scan_path(path)
                    or _timeframe_from_query(query_params)
                    or DEFAULT_FOOTPRINT_TIMEFRAME
                )
                status_line, scan_payload = await asyncio.to_thread(
                    _delete_scan_payload_for_timeframe,
                    self.snapshot_provider,
                    timeframe,
                    query_params,
                )
                body = json.dumps(scan_payload).encode("utf-8")
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
=======

            status_line = "HTTP/1.1 200 OK"

            if path == "/data":
                timeframe = _timeframe_from_query(query_params)
                snapshot_payload = self.snapshot_provider.snapshot_payload()
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
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
<<<<<<< HEAD
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
                    body = json.dumps(
                        snapshot_payload,
                        separators=(",", ":"),
                    ).encode("utf-8")
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
                        _optional_positive_int(
                            (query_params.get("client_bin_tick_count") or [None])[0]
                        ),
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
=======
                    snapshot_payload = _filter_snapshot_payload_timeframe(
                        self.snapshot_provider.snapshot_payload(),
                        timeframe,
                    )
                    body = json.dumps(snapshot_payload).encode("utf-8")
                    content_type = "application/json; charset=utf-8"
            else:
                timeframe = _timeframe_for_path(path)
                if timeframe is None:
                    status_line = "HTTP/1.1 404 Not Found"
                    body = b"Not Found"
                    content_type = "text/plain; charset=utf-8"
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                else:
                    body = _html_page(timeframe).encode("utf-8")
                    content_type = "text/html; charset=utf-8"

<<<<<<< HEAD
            content_encoding_header = b""
            if (
                (
                    path == "/refill-scan"
                    or path.startswith("/refill-scan/")
                    or path.startswith("/footprint-data/")
                )
                and len(body) >= 32_768
                and "gzip" in request_headers.get("accept-encoding", "").lower()
            ):
                body = gzip.compress(body, compresslevel=1)
                content_encoding_header = b"Content-Encoding: gzip\r\nVary: Accept-Encoding\r\n"

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            writer.write(
                status_line.encode("utf-8")
                + b"\r\n"
                + f"Content-Type: {content_type}\r\n".encode("utf-8")
                + f"Content-Length: {len(body)}\r\n".encode("utf-8")
<<<<<<< HEAD
                + content_encoding_header
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
                + b"Cache-Control: no-store\r\n"
                + b"Connection: close\r\n\r\n"
                + body
            )
<<<<<<< HEAD
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
=======
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744


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


<<<<<<< HEAD
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


def _timeframe_for_refill_scan_path(path: str) -> str | None:
    prefix = "/refill-scan/"
    if not path.startswith(prefix):
        return None
    timeframe = path[len(prefix) :].strip().upper()
    return timeframe if timeframe in FOOTPRINT_TIMEFRAMES else None


def _timeframe_for_delete_scan_path(path: str) -> str | None:
    prefix = "/delete-scan/"
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


def _refill_scan_payload_for_timeframe(
    provider: SnapshotProvider,
    timeframe: str | None,
    query_params: dict[str, list[str]],
) -> tuple[str, dict]:
    scan_payload = getattr(provider, "data_process_refill_scan_payload", None)
    if scan_payload is None:
        return (
            "HTTP/1.1 501 Not Implemented",
            {
                "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
                "status": "ERROR",
                "message": "data process refill scan is not available",
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
    refill_min = _non_negative_int_from_query(query_params, "refill_min")
    if refill_min is None:
        refill_min = _non_negative_int_from_query(query_params, "min_refill")
    if refill_min is None:
        refill_min = _non_negative_int_from_query(query_params, "refill")
    if refill_min is None:
        refill_min = 1
    activity_filter = _text_from_query(
        query_params,
        "activity_filter",
        "level_filter",
        "filter_code",
    ).strip().upper()
    if activity_filter and re.fullmatch(r"[OAB]\d+", activity_filter) is None:
        return (
            "HTTP/1.1 400 Bad Request",
            {
                "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
                "status": "ERROR",
                "message": "activity_filter must use O, A, or B followed by a non-negative integer",
                "timeframe": timeframe,
            },
        )
    rate_min_text = _text_from_query(query_params, "rate_min", "min_rate").strip()
    rate_min: float | None = None
    if rate_min_text:
        try:
            rate_min = float(rate_min_text)
        except ValueError:
            rate_min = None
        if rate_min is None or not 0.0 <= rate_min <= 100.0:
            return (
                "HTTP/1.1 400 Bad Request",
                {
                    "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
                    "status": "ERROR",
                    "message": "rate_min must be between 0 and 100",
                    "timeframe": timeframe,
                },
            )
    spike_score_min_text = _text_from_query(
        query_params,
        "spike_score_min",
        "min_spike_score",
    ).strip()
    spike_score_min: Decimal | None = None
    if spike_score_min_text:
        try:
            spike_score_min = Decimal(spike_score_min_text)
        except Exception:
            spike_score_min = None
        if spike_score_min is None or not spike_score_min.is_finite():
            return (
                "HTTP/1.1 400 Bad Request",
                {
                    "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
                    "status": "ERROR",
                    "message": "spike_score_min must be a finite number",
                    "timeframe": timeframe,
                },
            )
    try:
        return (
            "HTTP/1.1 200 OK",
            scan_payload(
                timeframe=timeframe,
                start_vancouver=start_vancouver,
                end_vancouver=end_vancouver,
                refill_min=refill_min,
                activity_filter=activity_filter,
                rate_min=rate_min,
                **(
                    {"spike_score_min": spike_score_min}
                    if spike_score_min is not None
                    else {}
                ),
            ),
        )
    except ValueError as exc:
        return (
            "HTTP/1.1 400 Bad Request",
            {
                "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
                "status": "ERROR",
                "message": str(exc),
                "timeframe": timeframe,
            },
        )
    except Exception as exc:
        LOGGER.exception("DATA_PROCESS_REFILL_SCAN_ERROR | timeframe=%s", timeframe)
        return (
            "HTTP/1.1 500 Internal Server Error",
            {
                "type": "DATA_PROCESS_REFILL_SCAN_RESULT",
                "status": "ERROR",
                "message": str(exc),
                "timeframe": timeframe,
            },
        )


def _delete_scan_payload_for_timeframe(
    provider: SnapshotProvider,
    timeframe: str | None,
    query_params: dict[str, list[str]],
) -> tuple[str, dict]:
    scan_payload = getattr(provider, "data_process_delete_scan_payload", None)
    if scan_payload is None:
        return (
            "HTTP/1.1 501 Not Implemented",
            {
                "type": "DATA_PROCESS_DELETE_SCAN_RESULT",
                "status": "ERROR",
                "message": "data process delete scan is not available",
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
    side = _text_from_query(query_params, "side", "delete_side") or "ASK"
    delete_min = (
        _positive_int_from_query(query_params, "delete_min")
        or _positive_int_from_query(query_params, "min_delete")
        or _positive_int_from_query(query_params, "refill_min")
        or _positive_int_from_query(query_params, "refill")
        or 1
    )
    contracts_min = (
        _positive_int_from_query(query_params, "delete_contracts_min")
        or _positive_int_from_query(query_params, "contracts_min")
        or _positive_int_from_query(query_params, "min_contracts")
        or _positive_int_from_query(query_params, "contract_min")
        or 0
    )
    try:
        return (
            "HTTP/1.1 200 OK",
            scan_payload(
                timeframe=timeframe,
                start_vancouver=start_vancouver,
                end_vancouver=end_vancouver,
                side=side,
                delete_min=delete_min,
                contracts_min=contracts_min,
            ),
        )
    except ValueError as exc:
        return (
            "HTTP/1.1 400 Bad Request",
            {
                "type": "DATA_PROCESS_DELETE_SCAN_RESULT",
                "status": "ERROR",
                "message": str(exc),
                "timeframe": timeframe,
            },
        )
    except Exception as exc:
        LOGGER.exception("DATA_PROCESS_DELETE_SCAN_ERROR | timeframe=%s", timeframe)
        return (
            "HTTP/1.1 500 Internal Server Error",
            {
                "type": "DATA_PROCESS_DELETE_SCAN_RESULT",
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


def _non_negative_int_from_query(query_params: dict[str, list[str]], field: str) -> int | None:
    values = query_params.get(field) or []
    if not values:
        return None
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return None
    return value if value >= 0 else None


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
        return 1
    try:
        value = int(str(values[0]).strip())
    except ValueError:
        return 1
    return value if value in {1, 2, 4, 8, 16} else 1


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
    client_bin_tick_count: int | None = None,
) -> dict:
    if not known_open_times:
        return snapshot_payload
    filtered_payload = dict(snapshot_payload)
    filtered_sessions = []
    omitted_count = 0
    for session in snapshot_payload.get("sessions", []):
        next_session = dict(session)
        candles = list(session.get("candles", []))
        session_bin_tick_count = _optional_positive_int(session.get("bin_tick_count"))
        if (
            client_bin_tick_count is not None
            and session_bin_tick_count is not None
            and session_bin_tick_count != client_bin_tick_count
        ):
            next_session["candles"] = candles
        else:
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


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
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


<<<<<<< HEAD
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


=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
_HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>__ACTIVE_TIMEFRAME__</title>
  <style>
    :root {
      --bg: #0d1117;
<<<<<<< HEAD
      --panel: #111820;
      --line: #2b333d;
      --text: #e6edf3;
      --muted: #8b949e;
      --blue: #58a6ff;
      --green: #3fb950;
      --red: #f85149;
      --gold: #d29922;
=======
      --panel: #161b22;
      --line: #30363d;
      --text: #e6edf3;
      --muted: #8b949e;
      --bid: #58a6ff;
      --ask: #d29922;
      --pos: #3fb950;
      --neg: #f85149;
      --core-bg: rgba(31,111,235,.42);
      --side-bg: rgba(236,171,0,.30);
      --hvn-bg: rgba(236,171,0,.18);
      --center-col: 520px;
      --range-col: 70px;
      --candle-width: calc(var(--center-col) + var(--range-col));
      --row-height: 36px;
      --candle-gap: 14px;
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
<<<<<<< HEAD
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
=======
      background: #0d1117;
      color: var(--text);
      font-family: "Segoe UI", Arial, sans-serif;
      font-size: 12px;
    }
    header {
      padding: 16px 22px;
      border-bottom: 1px solid var(--line);
      background: #0f141b;
      position: sticky;
      top: 0;
      z-index: 10;
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    }
    h1 {
      margin: 0 0 4px;
      font-size: 20px;
<<<<<<< HEAD
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
=======
    }
    .meta, .note, .range {
      color: var(--muted);
    }
    main {
      padding: 16px;
    }
    .session {
      border: 1px solid var(--line);
      background: var(--panel);
      border-radius: 8px;
      padding: 12px;
      margin-bottom: 16px;
    }
    .session-title {
      display: flex;
      justify-content: space-between;
      gap: 16px;
      margin-bottom: 8px;
      color: var(--muted);
    }
    .timeframe-header {
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 16px;
      margin-bottom: 12px;
    }
    .timeframe-title {
      margin: 0;
      font-size: 16px;
      color: var(--text);
    }
    .timeframe-links {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      justify-content: flex-end;
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    }
    .timeframe-link {
      border: 1px solid var(--line);
      color: var(--muted);
<<<<<<< HEAD
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
=======
      background: #0f141b;
      border-radius: 6px;
      padding: 5px 8px;
      text-decoration: none;
      font-weight: 600;
    }
    .timeframe-link.active {
      color: var(--text);
      border-color: #58a6ff;
      background: rgba(88,166,255,.16);
    }
    .chart-shell {
      height: calc(100vh - 172px);
      min-height: 560px;
      border: 1px solid var(--line);
      background: #0b0f14;
      display: grid;
      grid-template-rows: minmax(0, 1fr) auto;
      overflow: hidden;
    }
    .chart-wrap {
      min-height: 0;
      overflow: auto;
      position: relative;
      overflow-anchor: none;
    }
    .chart-content {
      min-width: 100%;
      width: max-content;
      padding-bottom: 12px;
    }
    .candle-strip {
      display: flex;
      align-items: flex-start;
      gap: var(--candle-gap);
      padding: 12px 12px 8px;
      min-width: 100%;
      position: relative;
      width: max-content;
    }
    .candle {
      width: var(--candle-width);
      border: 1px solid #26303b;
      background: #0f141b;
      flex: 0 0 auto;
    }
    .candle-head {
      position: sticky;
      top: 0;
      z-index: 2;
      background: #161b22;
      border-bottom: 1px solid var(--line);
      padding: 8px 10px;
      display: flex;
      justify-content: space-between;
      gap: 10px;
      white-space: nowrap;
    }
    .candle-grid {
      position: relative;
      overflow: hidden;
    }
    .candle-shape {
      position: absolute;
      top: 0;
      bottom: 0;
      left: 0;
      width: var(--center-col);
      z-index: 0;
      pointer-events: none;
    }
    .candle-wick {
      position: absolute;
      left: 50%;
      width: 2px;
      transform: translateX(-50%);
      border-radius: 999px;
      opacity: .72;
    }
    .candle-body {
      position: absolute;
      left: 18px;
      right: 18px;
      border: 1px solid;
      border-radius: 2px;
      opacity: .64;
    }
    .candle-shape.bull .candle-wick,
    .candle-shape.bull .candle-body {
      background: rgba(63,185,80,.24);
      border-color: rgba(63,185,80,.70);
    }
    .candle-shape.bear .candle-wick,
    .candle-shape.bear .candle-body {
      background: rgba(248,81,73,.24);
      border-color: rgba(248,81,73,.70);
    }
    .bin-row {
      position: relative;
      z-index: 1;
      display: grid;
      grid-template-columns: var(--center-col) var(--range-col);
      height: var(--row-height);
      border-bottom: 1px solid rgba(48,54,61,.55);
      align-items: stretch;
    }
    .bin-row.core { background: var(--core-bg); }
    .bin-row.side { background: var(--side-bg); }
    .bin-row.hvn { background: var(--hvn-bg); }
    .cell {
      padding: 4px 6px;
      overflow: hidden;
      white-space: nowrap;
      text-overflow: ellipsis;
      display: flex;
      flex-direction: column;
      justify-content: center;
      border-right: 1px solid rgba(48,54,61,.35);
      line-height: 1.15;
    }
    .cell-inner {
      width: 100%;
      overflow: hidden;
      white-space: nowrap;
      text-overflow: ellipsis;
    }
    .center .cell-inner {
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 2px;
      white-space: normal;
    }
    .footprint-main,
    .footprint-extra {
      display: block;
      max-width: 100%;
      overflow: hidden;
      white-space: nowrap;
      text-overflow: ellipsis;
    }
    .footprint-extra {
      color: var(--muted);
      font-size: 10px;
      font-weight: 500;
    }
    .bid { text-align: right; color: var(--bid); }
    .ask { color: var(--ask); }
    .center {
      text-align: center;
      color: var(--text);
      font-weight: 500;
    }
    .range {
      font-size: 10px;
      text-align: right;
      border-right: 0;
    }
    .delta-pos { color: var(--pos); }
    .delta-neg { color: var(--neg); }
    .delta-zero { color: var(--text); }
    .summary-rows {
      display: grid;
      gap: 4px;
      padding: 8px 12px;
      width: max-content;
      min-width: 100%;
    }
    .summary-lock {
      border-top: 1px solid var(--line);
      background: #101720;
      overflow: hidden;
      box-shadow: 0 -10px 18px rgba(0,0,0,.28);
      z-index: 7;
    }
    .summary-track {
      width: max-content;
      will-change: transform;
    }
    .summary-row {
      display: flex;
      gap: var(--candle-gap);
      min-width: 100%;
    }
    .summary-cell {
      width: var(--candle-width);
      min-height: 30px;
      flex: 0 0 auto;
      border: 1px solid #26303b;
      background: #0f141b;
      display: grid;
      grid-template-columns: 76px 1fr;
      align-items: center;
      overflow: hidden;
    }
    .summary-label {
      height: 100%;
      display: flex;
      align-items: center;
      padding: 0 8px;
      color: var(--muted);
      border-right: 1px solid rgba(48,54,61,.55);
      font-size: 10px;
      text-transform: uppercase;
      letter-spacing: 0;
    }
    .summary-value {
      padding: 0 8px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      text-align: right;
      font-weight: 700;
    }
    .empty { padding: 18px; color: var(--muted); }
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
  </style>
</head>
<body>
  <header>
<<<<<<< HEAD
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
=======
    <h1>__ACTIVE_TIMEFRAME__</h1>
    <div class="meta" id="status">Waiting for data...</div>
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
  </header>
  <main id="app"></main>
  <script>
    const app = document.getElementById("app");
    const statusEl = document.getElementById("status");
<<<<<<< HEAD
    const linksEl = document.getElementById("timeframe-links");
    const binTickSelect = document.getElementById("bin-tick-select");
    const ACTIVE_TIMEFRAME = "__ACTIVE_TIMEFRAME__";
    const FOOTPRINT_TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"];
    const BIN_TICK_OPTIONS = [1, 2, 4, 8, 16];
    const DEFAULT_BIN_TICK_COUNT = 1;
    const BIN_TICK_STORAGE_KEY = "footprint.binTicks.v2";
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
      const storedBinTicks = Number.parseInt(localStorage.getItem(BIN_TICK_STORAGE_KEY) || "", 10);
      if (BIN_TICK_OPTIONS.includes(storedBinTicks)) activeBinTickCount = storedBinTicks;
    } catch {}
=======
    const ACTIVE_TIMEFRAME = "__ACTIVE_TIMEFRAME__";
    const FOOTPRINT_TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"];
    const scrollState = new Map();
    const restoringScrollKeys = new Set();
    const scrollEdgeThreshold = 32;
    const candleRangePaddingBins = 10;
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    function safeArray(value) { return Array.isArray(value) ? value : []; }
    function num(value) {
      const parsed = Number.parseFloat(value ?? "0");
      return Number.isFinite(parsed) ? parsed : 0;
    }
    function maybeNum(value) {
      const parsed = Number.parseFloat(value ?? "");
      return Number.isFinite(parsed) ? parsed : NaN;
    }
<<<<<<< HEAD
    function candleOpen(candle) { return Number(candle?.open_time_ms ?? candle?.open_time ?? 0); }
    function candleClose(candle) { return Number(candle?.close_time_ms ?? candle?.close_time ?? 0); }
    function ohlc(candle, key) { return candle?.ohlc?.[key] ?? candle?.[`${key}_price`] ?? ""; }
    function candleTriggerSignals(candle) { return safeArray(candle?.trigger_signals); }
    function signalMarkerShape(signal) {
      return String(signal?.marker_shape || (String(signal?.signal_type || "").startsWith("EXIT_") ? "SQUARE" : "ARROW")).trim().toUpperCase();
    }
    function triggerMarkerBounds(signal, centerX, yHigh, yLow, plotH = Infinity) {
      const shape = signalMarkerShape(signal);
      const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
      const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
      const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
      const arrowHeight = 12;
      const plotPad = 4;
      if (shape === "SQUARE") {
        const size = 10;
        const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
        return { left: centerX - size / 2, right: centerX + size / 2, top: centerY - size / 2, bottom: centerY + size / 2 };
      }
      if (markerDirection === "DOWN" && markerColor === "RED") {
        let tipY = yHigh - 4;
        if (tipY - arrowHeight < plotPad) tipY = arrowHeight + plotPad;
        if (Number.isFinite(plotH)) tipY = Math.min(tipY, Math.max(plotPad, plotH - plotPad));
        const baseY = tipY - arrowHeight;
        return { left: centerX - 8, right: centerX + 8, top: baseY - 3, bottom: tipY + 4 };
      }
      if (markerDirection === "UP" && markerColor === "GREEN") {
        let tipY = yLow + 4;
        if (Number.isFinite(plotH) && tipY + arrowHeight > plotH - plotPad) {
          tipY = Math.max(plotPad, plotH - arrowHeight - plotPad);
        }
        const baseY = tipY + arrowHeight;
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
    function drawTriggerMarkers(ctx, candle, centerX, yHigh, yLow, plotH = Infinity) {
      for (const signal of candleTriggerSignals(candle)) {
        const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
        const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
        const markerShape = signalMarkerShape(signal);
        const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
        const fillColor = markerColor === "GREEN" ? "#3fb950" : "#f85149";
        const arrowHeight = 12;
        const plotPad = 4;
        if (markerShape === "SQUARE") {
          const size = 10;
          const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
          ctx.fillStyle = fillColor;
          ctx.fillRect(centerX - size / 2, centerY - size / 2, size, size);
          ctx.strokeStyle = "#0d1117";
          ctx.lineWidth = 1.5;
          ctx.strokeRect(centerX - size / 2, centerY - size / 2, size, size);
        } else if (markerDirection === "DOWN" && markerColor === "RED") {
          let tipY = yHigh - 4;
          if (tipY - arrowHeight < plotPad) tipY = arrowHeight + plotPad;
          if (Number.isFinite(plotH)) tipY = Math.min(tipY, Math.max(plotPad, plotH - plotPad));
          const baseY = tipY - arrowHeight;
          ctx.fillStyle = "#f85149";
          ctx.beginPath();
          ctx.moveTo(centerX, tipY);
          ctx.lineTo(centerX - 7, baseY);
          ctx.lineTo(centerX + 7, baseY);
          ctx.closePath();
          ctx.fill();
        } else if (markerDirection === "UP" && markerColor === "GREEN") {
          let tipY = yLow + 4;
          if (Number.isFinite(plotH) && tipY + arrowHeight > plotH - plotPad) {
            tipY = Math.max(plotPad, plotH - arrowHeight - plotPad);
          }
          const baseY = tipY + arrowHeight;
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
=======
    function visible(value) { return Math.abs(num(value)) >= 0.0005; }
    function fmt(value) { return num(value).toFixed(3); }
    function fmtMaybe(value) {
      const parsed = maybeNum(value);
      return Number.isFinite(parsed) ? parsed.toFixed(3) : "n/a";
    }
    function fmtEfficiency(value) {
      const parsed = maybeNum(value);
      return Number.isFinite(parsed) ? parsed.toFixed(6) : "n/a";
    }
    function dominantSide(value) {
      const side = String(value || "NONE").trim().toUpperCase();
      return ["BUY", "SELL"].includes(side) ? side : "NONE";
    }
    function signed(value) {
      const n = num(value);
      return `${n >= 0 ? "+" : ""}${n.toFixed(3)}`;
    }
    function fmtDuration(bin) {
      const value = bin?.l2?.duration ?? (num(bin?.duration_ms) / 1000);
      return `${num(value).toFixed(3)}s`;
    }
    function compact(value) {
      const n = num(value);
      if (Math.abs(n) >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, "") + "K";
      if (Math.abs(n) >= 10) return n.toFixed(0);
      return n.toFixed(0);
    }
    function binLow(bin) { return num(bin?.bin_low ?? bin?.low); }
    function binHigh(bin) { return num(bin?.bin_high ?? bin?.high); }
    function isValidBin(bin) { return binHigh(bin) > binLow(bin); }
    function binKey(bin) { return `${binLow(bin)}|${binHigh(bin)}`; }
    function fixedBinSizeFromCandles(candles) {
      for (const candle of safeArray(candles)) {
        const size = num(candle?.fixed_bin_size ?? candle?.bin_size);
        if (size > 0) return size;
      }
      for (const candle of safeArray(candles)) {
        for (const bin of safeArray(candle?.bins)) {
          const size = binHigh(bin) - binLow(bin);
          if (size > 0) return size;
        }
      }
      return 0;
    }
    function fixedBinSize(session, candles, fallbackCandles) {
      const sessionSize = num(session?.fixed_bin_size ?? session?.bin_size);
      if (sessionSize > 0) return sessionSize;
      return fixedBinSizeFromCandles(candles) || fixedBinSizeFromCandles(fallbackCandles);
    }
    function priceBinIndex(price, size) {
      const value = maybeNum(price);
      return size > 0 && Number.isFinite(value) ? Math.floor((value + size * 1e-9) / size) : NaN;
    }
    function binIndex(bin, size) {
      const explicit = Number(bin?.index);
      if (Number.isFinite(explicit)) return explicit;
      return priceBinIndex(binLow(bin), size);
    }
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    function sessionTimeframe(session) { return String(session?.timeframe || ACTIVE_TIMEFRAME).trim().toUpperCase(); }
    function sessionKey(session) {
      const symbol = session.mt5_symbol || session.binance_symbol || session.symbol || "UNKNOWN";
      return `${symbol}|${sessionTimeframe(session)}`;
    }
<<<<<<< HEAD
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
    function priceDecimalsForStep(step) {
      const parsedStep = Math.abs(num(step));
      if (!Number.isFinite(parsedStep) || parsedStep <= 0) return null;
      for (let decimals = 0; decimals <= 8; decimals += 1) {
        const scaled = parsedStep * (10 ** decimals);
        if (Math.abs(scaled - Math.round(scaled)) < 1e-8) return decimals;
      }
      return 8;
    }
    function fmtPrice(value, step = 0) {
      const decimals = priceDecimalsForStep(step);
      if (decimals !== null) return num(value).toFixed(decimals);
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
        candleItems.forEach(item => {
          const center = item.x + this.candleWidth / 2;
          const high = maybeNum(ohlc(item.candle, "high"));
          const low = maybeNum(ohlc(item.candle, "low"));
          if (Number.isFinite(high) && Number.isFinite(low)) {
            drawTriggerMarkers(ctx, item.candle, center, priceToY(high), priceToY(low), plotH);
          }
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
=======
    function candleKey(candle) { return String(candle.open_time_ms ?? candle.open_time ?? ""); }
    function candleOpen(candle) { return Number(candle.open_time_ms ?? candle.open_time ?? 0); }
    function candleTime(candle) {
      const value = candleOpen(candle);
      return value ? new Date(value).toLocaleTimeString() : "";
    }
    function ohlc(candle, key) {
      return candle?.ohlc?.[key] ?? candle?.[`${key}_price`] ?? "";
    }
    function maxScrollLeft(wrap) {
      return Math.max(0, wrap.scrollWidth - wrap.clientWidth);
    }
    function maxScrollTop(wrap) {
      return Math.max(0, wrap.scrollHeight - wrap.clientHeight);
    }
    function isNearRightEdge(wrap) {
      return maxScrollLeft(wrap) - wrap.scrollLeft <= scrollEdgeThreshold;
    }
    function isNearBottomEdge(wrap) {
      return maxScrollTop(wrap) - wrap.scrollTop <= scrollEdgeThreshold;
    }
    function topVisibleRowAnchor(section, wrap) {
      const wrapRect = wrap.getBoundingClientRect();
      const grid = section.querySelector(".candle-grid");
      if (!grid) return null;
      for (const row of grid.querySelectorAll(".bin-row[data-bin-index]")) {
        const rect = row.getBoundingClientRect();
        if (rect.bottom > wrapRect.top) {
          return {
            binIndex: row.getAttribute("data-bin-index"),
            offset: rect.top - wrapRect.top,
          };
        }
      }
      return null;
    }
    function leftVisibleCandleAnchor(section, wrap) {
      const wrapRect = wrap.getBoundingClientRect();
      for (const candle of section.querySelectorAll(".candle[data-candle-key]")) {
        const rect = candle.getBoundingClientRect();
        if (rect.right > wrapRect.left) {
          return {
            candleKey: candle.getAttribute("data-candle-key"),
            offset: rect.left - wrapRect.left,
          };
        }
      }
      return null;
    }
    function userScrollingState(previous, nearRight, nearBottom, manual) {
      if (nearRight && nearBottom) return false;
      return Boolean(manual || previous?.userIsScrolling);
    }
    function captureScrollState() {
      document.querySelectorAll(".session[data-session]").forEach(section => {
        const key = section.getAttribute("data-session");
        const wrap = section.querySelector(".chart-wrap");
        if (!key || !wrap) return;
        const previous = scrollState.get(key) || {};
        const nearRight = isNearRightEdge(wrap);
        const nearBottom = isNearBottomEdge(wrap);
        const rowAnchor = topVisibleRowAnchor(section, wrap);
        const candleAnchor = leftVisibleCandleAnchor(section, wrap);
        scrollState.set(key, {
          ...previous,
          left: wrap.scrollLeft,
          top: wrap.scrollTop,
          nearRight,
          nearBottom,
          userIsScrolling: userScrollingState(previous, nearRight, nearBottom, false),
          anchorBinIndex: rowAnchor?.binIndex ?? previous.anchorBinIndex,
          anchorBinOffset: rowAnchor?.offset ?? previous.anchorBinOffset,
          anchorCandleKey: candleAnchor?.candleKey ?? previous.anchorCandleKey,
          anchorCandleOffset: candleAnchor?.offset ?? previous.anchorCandleOffset,
        });
      });
    }
    function syncSummaryPosition(wrap) {
      const track = wrap?.closest(".chart-shell")?.querySelector(".summary-track");
      if (!track) return;
      track.style.transform = `translateX(${-wrap.scrollLeft}px)`;
    }
    function updateStoredScrollPosition(key, wrap) {
      syncSummaryPosition(wrap);
      const previous = scrollState.get(key) || {};
      const nearRight = isNearRightEdge(wrap);
      const nearBottom = isNearBottomEdge(wrap);
      scrollState.set(key, {
        ...previous,
        left: wrap.scrollLeft,
        top: wrap.scrollTop,
        nearRight,
        nearBottom,
        userIsScrolling: userScrollingState(previous, nearRight, nearBottom, false),
      });
    }
    function markUserScrolling(key, wrap) {
      syncSummaryPosition(wrap);
      const previous = scrollState.get(key) || {};
      const nearRight = isNearRightEdge(wrap);
      const nearBottom = isNearBottomEdge(wrap);
      scrollState.set(key, {
        ...previous,
        left: wrap.scrollLeft,
        top: wrap.scrollTop,
        nearRight,
        nearBottom,
        userIsScrolling: userScrollingState(previous, nearRight, nearBottom, true),
      });
      clearTimeout(wrap.__scrollStopTimer);

      wrap.__scrollStopTimer = setTimeout(() => {

        const previous = scrollState.get(key) || {};

        scrollState.set(key, {
          ...previous,
          userIsScrolling: false,
        });

      }, 400);
    }
    function handleScroll(key, wrap) {
      syncSummaryPosition(wrap);
      if (restoringScrollKeys.has(key)) {
        updateStoredScrollPosition(key, wrap);
      } else {
        markUserScrolling(key, wrap);
      }
    }
    function attachScrollGuard(key, wrap) {
      if (wrap.__scrollGuardKey === key) return;
      wrap.__scrollGuardKey = key;
      const mark = () => markUserScrolling(key, wrap);
      wrap.addEventListener("wheel", mark, { passive: true });
      wrap.addEventListener("touchstart", mark, { passive: true });
      wrap.addEventListener("pointerdown", mark, { passive: true });
      wrap.addEventListener("mousedown", mark, { passive: true });
      wrap.onscroll = () => handleScroll(key, wrap);
    }
    function clamp(value, min, max) {
      return Math.max(min, Math.min(value, max));
    }
    function restoreRowAnchor(wrap, previous) {
      if (previous?.anchorBinIndex == null) return false;
      const selector = `.bin-row[data-bin-index="${CSS.escape(String(previous.anchorBinIndex))}"]`;
      const row = wrap.querySelector(selector);
      if (!row) return false;
      const wrapRect = wrap.getBoundingClientRect();
      const rowRect = row.getBoundingClientRect();
      const delta = rowRect.top - wrapRect.top - num(previous.anchorBinOffset);
      wrap.scrollTop = clamp(wrap.scrollTop + delta, 0, maxScrollTop(wrap));
      return true;
    }
    function restoreCandleAnchor(wrap, previous) {
      if (previous?.anchorCandleKey == null) return false;
      const selector = `.candle[data-candle-key="${CSS.escape(String(previous.anchorCandleKey))}"]`;
      const candle = wrap.querySelector(selector);
      if (!candle) return false;
      const wrapRect = wrap.getBoundingClientRect();
      const candleRect = candle.getBoundingClientRect();
      const delta = candleRect.left - wrapRect.left - num(previous.anchorCandleOffset);
      wrap.scrollLeft = clamp(wrap.scrollLeft + delta, 0, maxScrollLeft(wrap));
      return true;
    }
    function restoreSessionScroll(key) {
      const wrap = document.querySelector(`[data-session="${CSS.escape(key)}"] .chart-wrap`);
      if (!wrap) return;
      const previous = scrollState.get(key);
      restoringScrollKeys.add(key);
      if (previous) {
        const followHorizontal = previous.nearRight && !previous.userIsScrolling;
        const followVertical = previous.nearBottom && !previous.userIsScrolling;
        wrap.scrollLeft = followHorizontal
          ? maxScrollLeft(wrap)
          : Math.min(previous.left ?? 0, maxScrollLeft(wrap));
        wrap.scrollTop = followVertical
          ? maxScrollTop(wrap)
          : Math.min(previous.top ?? 0, maxScrollTop(wrap));
        if (!followHorizontal) restoreCandleAnchor(wrap, previous);
        if (!followVertical) restoreRowAnchor(wrap, previous);
      } else {
        wrap.scrollLeft = maxScrollLeft(wrap);
        wrap.scrollTop = 0;
      }
      updateStoredScrollPosition(key, wrap);
      attachScrollGuard(key, wrap);
      syncSummaryPosition(wrap);
      requestAnimationFrame(() => restoringScrollKeys.delete(key));
    }
    function binsOverlap(a, b) {
      if (!a || !b) return false;
      return Math.min(binHigh(a), binHigh(b)) > Math.max(binLow(a), binLow(b));
    }
    function binRole(bin, hvn) {
      if (binsOverlap(bin, hvn?.core_bin)) return "core";
      if (safeArray(hvn?.side_bins).some(item => binsOverlap(bin, item))) return "side";
      if (safeArray(hvn?.hvn_bins).some(item => binsOverlap(bin, item))) return "hvn";
      return "normal";
    }

    function findMappedVisualIndex(source, visual, size) {
      const index = binIndex(source, size);
      if (Number.isFinite(index)) {
        const directIndex = visual.findIndex(bin => bin.index === index);
        if (directIndex >= 0) return directIndex;
      }
      let bestIndex = -1;
      let bestOverlap = 0;
      const low = binLow(source);
      const high = binHigh(source);
      const eps = Math.max((high - low) * 1e-9, 1e-12);
      visual.forEach((bin, index) => {
        const overlap = Math.min(high, binHigh(bin)) - Math.max(low, binLow(bin));
        if (overlap > bestOverlap + eps) {
          bestOverlap = overlap;
          bestIndex = index;
        }
      });
      if (bestIndex >= 0) return bestIndex;
      const mid = (low + high) / 2;
      return visual.findIndex(bin => mid >= binLow(bin) - eps && mid < binHigh(bin) + eps);
    }

    function latestCandlePrice(candles) {
      const ordered = safeArray(candles).slice().sort((a, b) => candleOpen(a) - candleOpen(b));
      for (let index = ordered.length - 1; index >= 0; index -= 1) {
        for (const key of ["close", "open", "high", "low"]) {
          const value = maybeNum(ohlc(ordered[index], key));
          if (Number.isFinite(value)) return value;
        }
      }
      return NaN;
    }
    function computeSharedVisualRange(candles, size, referencePrice) {
      if (size <= 0) return null;
      let minIndex = Infinity;
      let maxIndex = -Infinity;
      function addIndex(index) {
        if (!Number.isFinite(index)) return;
        minIndex = Math.min(minIndex, index);
        maxIndex = Math.max(maxIndex, index);
      }
      for (const candle of safeArray(candles)) {
        const highIndex = priceBinIndex(ohlc(candle, "high"), size);
        const lowIndex = priceBinIndex(ohlc(candle, "low"), size);
        if (!Number.isFinite(highIndex) || !Number.isFinite(lowIndex)) continue;
        addIndex(Math.min(highIndex, lowIndex) - candleRangePaddingBins);
        addIndex(Math.max(highIndex, lowIndex) + candleRangePaddingBins);
      }
      const referenceIndex = priceBinIndex(referencePrice, size);
      if (Number.isFinite(referenceIndex)) addIndex(referenceIndex);
      if (!Number.isFinite(minIndex) || !Number.isFinite(maxIndex)) return null;
      return { minIndex, maxIndex, size };
    }
    function visualRangeKey(range) {
      if (!range) return "none";
      return `${range.minIndex}|${range.maxIndex}|${range.size}`;
    }
    function buildSharedVisualBins(range) {
      if (!range || range.size <= 0) return [];
      const visual = [];
      for (let index = range.maxIndex; index >= range.minIndex; index -= 1) {
        visual.push({
          index,
          bin_low: index * range.size,
          bin_high: (index + 1) * range.size,
          source: "shared",
        });
      }
      return visual;
    }

    function emptyL2() {
      return {
        total: 0,
        delta: 0,
        horizontalDelta: 0,
        buyVolume: 0,
        sellVolume: 0,
        buyRatio: 0,
        sellRatio: 0,
        durationSeconds: 0,
        priceProgressInBin: null,
        dominantSide: "NONE",
        dominantSideVolume: 0,
        dominantSideEfficiency: null,
        volumePercentile: null,
        isVolumeValid: true,
        efficiencyPercentile: null,
        efficiencyZscore: null,
        rejectionReason: "",
        dominantSideRankVolume: 0,
        has: false,
      };
    }
    function addL2(target, bin) {
      const total = bin?.l2?.total_volume ?? bin?.total_volume;
      const delta = bin?.l2?.delta ?? bin?.delta;
      const horizontalDelta = bin?.l2?.horizontal_delta ?? bin?.horizontal_delta ?? delta;
      const buyVolume = bin?.l2?.ask_traded_volume ?? bin?.ask_traded_volume ?? bin?.buy_volume;
      const sellVolume = bin?.l2?.bid_traded_volume ?? bin?.bid_traded_volume ?? bin?.sell_volume;
      const buyRatio = bin?.l2?.buy_diagonal_imbalance_ratio ?? bin?.buy_diagonal_imbalance_ratio;
      const sellRatio = bin?.l2?.sell_diagonal_imbalance_ratio ?? bin?.sell_diagonal_imbalance_ratio;
      const duration = bin?.l2?.duration ?? (bin?.duration_ms === undefined ? undefined : num(bin?.duration_ms) / 1000);
      const priceProgress = bin?.l2?.price_progress_in_bin ?? bin?.price_progress_in_bin ?? bin?.price_progress;
      const domSide = bin?.l2?.dominant_diagonal_side ?? bin?.dominant_diagonal_side;
      const domVolume = bin?.l2?.dominant_side_volume ?? bin?.dominant_side_volume;
      const domEfficiency = bin?.l2?.dominant_side_efficiency ?? bin?.dominant_side_efficiency;
      const volumePercentile = bin?.l2?.volume_percentile ?? bin?.volume_percentile;
      const isVolumeValid = bin?.l2?.is_volume_valid ?? bin?.is_volume_valid;
      const efficiencyPercentile = bin?.l2?.efficiency_percentile ?? bin?.efficiency_percentile;
      const efficiencyZscore = bin?.l2?.efficiency_zscore ?? bin?.efficiency_zscore;
      const rejectionReason = bin?.l2?.rejection_reason ?? bin?.rejection_reason;
      if (
        total === undefined &&
        delta === undefined &&
        buyVolume === undefined &&
        sellVolume === undefined &&
        duration === undefined &&
        buyRatio === undefined &&
        sellRatio === undefined &&
        priceProgress === undefined &&
        domSide === undefined &&
        domVolume === undefined &&
        domEfficiency === undefined &&
        volumePercentile === undefined &&
        isVolumeValid === undefined &&
        efficiencyPercentile === undefined &&
        efficiencyZscore === undefined &&
        rejectionReason === undefined
      ) return;
      target.total += num(total);
      target.delta += num(delta);
      target.horizontalDelta += num(horizontalDelta);
      target.buyVolume += num(buyVolume);
      target.sellVolume += num(sellVolume);
      // Merged visual bins display the dominant precomputed real-bin ratios; ratios are never summed or recalculated here.
      if (buyRatio !== undefined) target.buyRatio = Math.max(target.buyRatio, num(buyRatio));
      if (sellRatio !== undefined) target.sellRatio = Math.max(target.sellRatio, num(sellRatio));
      target.durationSeconds += num(duration);
      const sideVolume = num(domVolume);
      if (sideVolume >= target.dominantSideRankVolume) {
        target.dominantSideRankVolume = sideVolume;
        target.dominantSide = dominantSide(domSide);
        target.dominantSideVolume = sideVolume;
        target.dominantSideEfficiency = domEfficiency ?? null;
        target.priceProgressInBin = priceProgress ?? null;
        target.volumePercentile = volumePercentile ?? null;
        target.isVolumeValid = isVolumeValid === undefined ? true : Boolean(isVolumeValid);
        target.efficiencyPercentile = efficiencyPercentile ?? null;
        target.efficiencyZscore = efficiencyZscore ?? null;
        target.rejectionReason = rejectionReason ?? "";
      }
      target.has = true;
    }
    function alignBinData(visual, candle, size) {
      const aligned = visual.map(bin => ({
        bin,
        l2: emptyL2(),
      }));
      for (const footprintBin of safeArray(candle?.bins).filter(isValidBin)) {
        const index = findMappedVisualIndex(footprintBin, visual, size);
        if (index < 0) continue;
        addL2(aligned[index].l2, footprintBin);
      }
      return aligned;
    }

    function footprintLabel(l2) {
      if (!l2?.has) return "";
      const hDelta = num(l2.horizontalDelta);
      const totalVolume = num(l2.buyVolume) + num(l2.sellVolume);
      const deltaClass = hDelta > 0 ? "delta-pos" : hDelta < 0 ? "delta-neg" : "";
      const main = `${fmt(totalVolume)} - B:${fmt(l2.buyVolume)} / S:${fmt(l2.sellVolume)} | <span class="${deltaClass}">${signed(hDelta)}</span> | B:${num(l2.buyRatio).toFixed(2)}x / S:${num(l2.sellRatio).toFixed(2)}x | ${num(l2.durationSeconds).toFixed(3)}s`;
      const extra = `Dom: ${dominantSide(l2.dominantSide)} (${fmt(l2.dominantSideVolume)}) | Eff: ${fmtEfficiency(l2.dominantSideEfficiency)} | PP: ${fmtMaybe(l2.priceProgressInBin)} | Vol%: ${fmtMaybe(l2.volumePercentile)} | Valid: ${l2.isVolumeValid ? "Y" : "N"} | Eff%: ${fmtMaybe(l2.efficiencyPercentile)} | Z: ${fmtMaybe(l2.efficiencyZscore)}${l2.rejectionReason ? ` | ${l2.rejectionReason}` : ""}`;
      return `<span class="footprint-main">${main}</span><span class="footprint-extra">${extra}</span>`;
    }
    function rangeLabel(bin) {
      return `${binLow(bin).toFixed(1)}<br>${binHigh(bin).toFixed(1)}`;
    }
    function cell(content, className) {
      return `<div class="cell ${className}"><span class="cell-inner">${content || ""}</span></div>`;
    }
    function candleSpanStyle(highIndex, lowIndex, visual) {
      if (!visual.length || !Number.isFinite(highIndex) || !Number.isFinite(lowIndex)) return "";
      const maxIndex = visual[0].index;
      const rowCount = visual.length;
      const topRow = Math.min(rowCount - 1, Math.max(0, maxIndex - Math.max(highIndex, lowIndex)));
      const bottomRow = Math.max(topRow + 1, Math.min(rowCount, maxIndex - Math.min(highIndex, lowIndex) + 1));
      const span = Math.max(1, bottomRow - topRow);
      return `top: calc(${topRow} * var(--row-height)); height: calc(${span} * var(--row-height));`;
    }
    function candleShape(candle, visual, size) {
      if (!visual.length || size <= 0) return "";
      const open = maybeNum(ohlc(candle, "open"));
      const high = maybeNum(ohlc(candle, "high"));
      const low = maybeNum(ohlc(candle, "low"));
      const close = maybeNum(ohlc(candle, "close"));
      if (![open, high, low, close].every(Number.isFinite)) return "";
      const highIndex = priceBinIndex(high, size);
      const lowIndex = priceBinIndex(low, size);
      const openIndex = priceBinIndex(open, size);
      const closeIndex = priceBinIndex(close, size);
      const wickStyle = candleSpanStyle(highIndex, lowIndex, visual);
      const bodyStyle = candleSpanStyle(Math.max(openIndex, closeIndex), Math.min(openIndex, closeIndex), visual);
      if (!wickStyle || !bodyStyle) return "";
      const direction = close >= open ? "bull" : "bear";
      return `<div class="candle-shape ${direction}">
        <div class="candle-wick" style="${wickStyle}"></div>
        <div class="candle-body" style="${bodyStyle}"></div>
      </div>`;
    }
    function renderCandle(candle, visual, size) {
      const aligned = alignBinData(visual, candle, size);
      const rows = aligned.map(item => {
        const role = binRole(item.bin, candle.hvn || {});
        return `<div class="bin-row ${role}" data-bin-index="${item.bin.index}">
          ${cell(footprintLabel(item.l2), "center")}
          ${cell(rangeLabel(item.bin), "range")}
        </div>`;
      }).join("");
      const open = ohlc(candle, "open");
      const close = ohlc(candle, "close");
      return `<article class="candle" data-candle-key="${candleKey(candle)}">
        <div class="candle-head">
          <strong>${candleTime(candle)}</strong>
          <span>O ${open} C ${close}</span>
        </div>
        <div class="candle-grid">
          ${candleShape(candle, visual, size)}
          ${rows || '<div class="empty">No bins</div>'}
        </div>
      </article>`;
    }
    function binPayloadField(bin, fieldName) {
      return bin?.l2?.[fieldName] ?? bin?.[fieldName];
    }
    function candleSummary(candle) {
      return safeArray(candle?.bins).reduce((summary, bin) => {
        summary.delta += num(binPayloadField(bin, "horizontal_delta"));
        summary.volume += num(binPayloadField(bin, "total_volume"));
        return summary;
      }, { delta: 0, volume: 0 });
    }
    function summaryDeltaClass(value) {
      const delta = num(value);
      if (delta > 0) return "delta-pos";
      if (delta < 0) return "delta-neg";
      return "delta-zero";
    }
    function renderSummaryCell(candle, kind) {
      const summary = candleSummary(candle);
      if (kind === "delta") {
        return `<div class="summary-cell" data-candle-key="${candleKey(candle)}">
          <span class="summary-label">Delta</span>
          <span class="summary-value ${summaryDeltaClass(summary.delta)}">${signed(summary.delta)}</span>
        </div>`;
      }
      return `<div class="summary-cell" data-candle-key="${candleKey(candle)}">
        <span class="summary-label">Volume</span>
        <span class="summary-value">${fmt(summary.volume)}</span>
      </div>`;
    }
    function renderSummaryRows(candles) {
      return `<div class="summary-rows">
        <div class="summary-row summary-delta-row">${candles.map(candle => renderSummaryCell(candle, "delta")).join("")}</div>
        <div class="summary-row summary-volume-row">${candles.map(candle => renderSummaryCell(candle, "volume")).join("")}</div>
      </div>`;
    }

    function visibleCandlesForSession(session, displayLimit) {
      const candles = safeArray(session?.candles).slice().sort((a, b) => candleOpen(a) - candleOpen(b));
      const limit = Number.parseInt(displayLimit, 10);
      return limit > 0 ? candles.slice(-limit) : candles;
    }
    function rememberRenderedCandles(key, candles, visualRange, fixedSize) {
      const previous = scrollState.get(key) || {};
      const candleCache = {};
      const renderedCandleKeys = [];
      safeArray(candles).forEach(candle => {
        const key = candleKey(candle);
        if (!key) return;
        renderedCandleKeys.push(key);
        candleCache[key] = candle;
      });
      scrollState.set(key, {
        ...previous,
        renderedCandleKeys,
        candleCache,
        visualRange,
        visualRangeKey: visualRangeKey(visualRange),
        fixedSize,
      });
    }
    function renderedCandleKeys(section) {
      return new Set([...section.querySelectorAll(".candle[data-candle-key]")]
        .map(candle => candle.getAttribute("data-candle-key"))
        .filter(Boolean));
    }
    function cachedCandleChanged(previous, candle) {
      const key = candleKey(candle);
      if (!key || !previous?.candleCache?.[key]) return false;
      return JSON.stringify(previous.candleCache[key]) !== JSON.stringify(candle);
    }
    function appendSummaryCells(section, candle) {
      const deltaRow = section.querySelector(".summary-delta-row");
      const volumeRow = section.querySelector(".summary-volume-row");
      if (deltaRow) deltaRow.insertAdjacentHTML("beforeend", renderSummaryCell(candle, "delta"));
      if (volumeRow) volumeRow.insertAdjacentHTML("beforeend", renderSummaryCell(candle, "volume"));
    }
    function appendCandle(section, candle, visual, size) {
      const strip = section.querySelector(".candle-strip");
      if (!strip) return;
      strip.insertAdjacentHTML("beforeend", renderCandle(candle, visual, size));
      appendSummaryCells(section, candle);
    }
    function pruneStaleCandles(section, visibleKeys) {
      const keep = new Set(visibleKeys);
      section.querySelectorAll(".candle[data-candle-key], .summary-cell[data-candle-key]").forEach(element => {
        if (!keep.has(element.getAttribute("data-candle-key"))) element.remove();
      });
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    }
    function activeTimeframeSessions(snapshot) {
      return safeArray(snapshot?.sessions).filter(session => sessionTimeframe(session) === ACTIVE_TIMEFRAME);
    }
<<<<<<< HEAD
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
        localStorage.setItem(BIN_TICK_STORAGE_KEY, String(activeBinTickCount));
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
=======
    function timeframeLinks() {
      return FOOTPRINT_TIMEFRAMES.map(timeframe => {
        const activeClass = timeframe === ACTIVE_TIMEFRAME ? " active" : "";
        return `<a class="timeframe-link${activeClass}" href="/footprint/${timeframe}">${timeframe}</a>`;
      }).join("");
    }
    function ensureAppShell() {
      if (app.dataset.timeframe === ACTIVE_TIMEFRAME) return;
      app.dataset.timeframe = ACTIVE_TIMEFRAME;
      app.innerHTML = `<section class="timeframe-shell" data-timeframe="${ACTIVE_TIMEFRAME}">
        <div class="timeframe-header">
          <div>
            <h2 class="timeframe-title">Footprint ${ACTIVE_TIMEFRAME}</h2>
            <div class="note">Direct timeframe page: /footprint/${ACTIVE_TIMEFRAME}</div>
          </div>
          <nav class="timeframe-links" aria-label="Footprint timeframes">${timeframeLinks()}</nav>
        </div>
        <div class="timeframe-body" id="timeframe-body"></div>
      </section>`;
    }
    function renderSessionView(session, displayLimit) {
      const key = sessionKey(session);
      const incomingCandles = safeArray(session?.candles);
      const candles = visibleCandlesForSession(session, displayLimit);
      const size = fixedBinSize(session, candles, incomingCandles);
      const range = computeSharedVisualRange(candles, size, latestCandlePrice(candles));
      const visual = buildSharedVisualBins(range);
      rememberRenderedCandles(key, candles, range, size);
      return `<section class="session" data-session="${key}">
        <div class="session-title">
          <strong>${session.mt5_symbol || session.symbol || ""} -> ${session.binance_symbol || ""}</strong>
          <span>timeframe ${session.timeframe || ACTIVE_TIMEFRAME} / Binance ${session.interval || ""}</span>
        </div>
        <div class="note">Each price row is one shared visual bin. The bottom summary rows are calculated per candle from raw candle bins.</div>
        <div class="chart-shell">
          <div class="chart-wrap">
            <div class="chart-content">
              <div class="candle-strip">${candles.map(candle => renderCandle(candle, visual, size)).join("")}</div>
            </div>
          </div>
          <div class="summary-lock">
            <div class="summary-track">${renderSummaryRows(candles)}</div>
          </div>
        </div>
      </section>`;
    }
    function timeframeBody() {
      const body = document.getElementById("timeframe-body");
      return body || null;
    }
    function replaceSessionSection(body, section, session, displayLimit) {
      const html = renderSessionView(session, displayLimit);
      if (section) {
        section.outerHTML = html;
      } else {
        body.insertAdjacentHTML("beforeend", html);
      }
    }
    function showEmptyState(body) {
      if (body.dataset.empty === "true") return;
      body.replaceChildren();
      body.insertAdjacentHTML("beforeend", `<section class="session empty-session"><div class="empty">No ${ACTIVE_TIMEFRAME} sessions yet.</div></section>`);
      body.dataset.empty = "true";
    }
    function clearEmptyState(body) {
      if (body.dataset.empty !== "true") return;
      body.replaceChildren();
      body.dataset.empty = "false";
    }
    function removeMissingSessions(body, activeKeys) {
      body.querySelectorAll(".session[data-session]").forEach(section => {
        const key = section.getAttribute("data-session");
        if (!activeKeys.has(key)) {
          section.remove();
          scrollState.delete(key);
        }
      });
    }
    function updateSessionView(body, session, displayLimit) {
      const key = sessionKey(session);
      const section = body.querySelector(`.session[data-session="${CSS.escape(key)}"]`);
      const incomingCandles = safeArray(session?.candles);
      const candles = visibleCandlesForSession(session, displayLimit);
      const size = fixedBinSize(session, candles, incomingCandles);
      const range = computeSharedVisualRange(candles, size, latestCandlePrice(candles));
      const rangeKey = visualRangeKey(range);
      const previous = scrollState.get(key) || {};
      const previousRangeKey = previous.visualRangeKey ?? visualRangeKey(previous.visualRange);
      if (!section || previousRangeKey !== rangeKey || previous.fixedSize !== size) {
        replaceSessionSection(body, section, session, displayLimit);
        restoreSessionScroll(key);
        return;
      }

      const visual = buildSharedVisualBins(range);
      const existingKeys = renderedCandleKeys(section);
      if (candles.some(candle => existingKeys.has(candleKey(candle)) && cachedCandleChanged(previous, candle))) {
        replaceSessionSection(body, section, session, displayLimit);
        restoreSessionScroll(key);
        return;
      }

      candles.forEach(candle => {
        const key = candleKey(candle);
        if (key && !existingKeys.has(key)) appendCandle(section, candle, visual, size);
      });
      pruneStaleCandles(section, candles.map(candleKey).filter(Boolean));
      rememberRenderedCandles(key, candles, range, size);
      restoreSessionScroll(key);
    }

    function render(snapshot) {
      captureScrollState();
      ensureAppShell();
      const sessions = activeTimeframeSessions(snapshot);
      const generated = snapshot?.generated_at_utc ? new Date(snapshot.generated_at_utc).toLocaleTimeString() : new Date().toLocaleTimeString();
      const displayLimit = snapshot?.display_candles_by_timeframe?.[ACTIVE_TIMEFRAME] ?? snapshot?.memory_candles ?? "";
      statusEl.textContent = `Updated ${generated} | timeframe ${ACTIVE_TIMEFRAME} | UI candles ${displayLimit}`;
      const body = timeframeBody();
      if (!body) return;
      if (!sessions.length) {
        showEmptyState(body);
        return;
      }
      clearEmptyState(body);
      const activeKeys = new Set(sessions.map(sessionKey));
      removeMissingSessions(body, activeKeys);
      sessions.forEach(session => updateSessionView(body, session, displayLimit));
    }

    async function fetchJson(url) {
      try {
        const response = await fetch(url, { cache: "no-store" });
        return response.ok ? await response.json() : null;
      } catch {
        return null;
      }
    }
    
    const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };
    const refreshDelayMs = 2500;
    let refreshInFlight = false;
    let refreshTimer = null;
    function timeframeMs() {
      return TIMEFRAME_MS[ACTIVE_TIMEFRAME] || TIMEFRAME_MS.M15;
    }
    function nextCandleRefreshDelayMs(now = Date.now()) {
      const interval = timeframeMs();
      const remainder = now % interval;
      return Math.max(1000, interval - remainder + refreshDelayMs);
    }
    function scheduleNextRefresh() {
      if (refreshTimer) clearTimeout(refreshTimer);
      refreshTimer = setTimeout(refresh, nextCandleRefreshDelayMs());
    }
    async function refresh() {
      if (refreshInFlight) return;
      refreshInFlight = true;
      try {
        render(await fetchJson(`/footprint-data/${ACTIVE_TIMEFRAME}`));
      } finally {
        refreshInFlight = false;
        scheduleNextRefresh();
      }
    }
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    refresh();
  </script>
</body>
</html>"""


def _html_page(timeframe: str = DEFAULT_FOOTPRINT_TIMEFRAME) -> str:
    normalized_timeframe = timeframe.strip().upper()
    if normalized_timeframe not in FOOTPRINT_TIMEFRAMES:
        normalized_timeframe = DEFAULT_FOOTPRINT_TIMEFRAME
<<<<<<< HEAD
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
      height: 100vh;
      min-height: 0;
      display: grid;
      grid-template-rows: auto minmax(0, 1fr);
    }
    header {
      min-height: 112px;
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
    .refill-scan {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
      justify-content: flex-end;
      padding: 4px 6px;
      border: 1px solid rgba(210,153,34,.55);
      border-radius: 6px;
      background: rgba(210,153,34,.08);
    }
    .process-replay input,
    .refill-scan input {
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
    .refill-scan input.refill-scan-time {
      width: 220px;
    }
    .refill-scan input.refill-scan-min {
      width: 92px;
      text-align: center;
    }
    .refill-scan input.refill-scan-contracts-min {
      width: 118px;
      text-align: center;
    }
    .refill-scan input.refill-activity-filter {
      width: 92px;
      text-transform: uppercase;
    }
    .refill-scan input.refill-rate-min {
      width: 92px;
      text-align: center;
    }
    .refill-scan input.spike-score-min {
      width: 112px;
      text-align: center;
    }
    .process-replay button,
    .refill-scan button {
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
    .refill-scan button {
      border-color: var(--gold);
      background: rgba(210,153,34,.16);
    }
    .refill-scan button.ask-del {
      border-color: var(--red);
      background: rgba(248,81,73,.16);
    }
    .refill-scan button.bid-del {
      border-color: var(--green);
      background: rgba(63,185,80,.16);
    }
    .refill-scan button.spike-score {
      border-color: #a371f7;
      background: rgba(163,113,247,.16);
    }
    .clear-markers-button {
      border: 1px solid var(--line);
      background: rgba(139,148,158,.14);
      color: var(--text);
      border-radius: 6px;
      padding: 8px 14px;
      font-size: 20px;
      font-weight: 150;
      line-height: 1.2;
      cursor: pointer;
    }
    .process-replay button:disabled,
    .refill-scan button:disabled {
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
    .refill-scan-status {
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
      min-height: 0;
      padding: 12px;
      overflow: hidden;
    }
    .session {
      height: 100%;
      min-height: 0;
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
      max-width: min(560px, calc(100% - 16px));
      padding: 10px 12px;
      border: 1px solid var(--line);
      background: rgba(15,20,27,.94);
      color: var(--text);
      font-size: 20px;
      pointer-events: none;
      white-space: pre-line;
      opacity: 0;
      line-height: 1.25;
      overflow-wrap: anywhere;
      z-index: 20;
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
      <form class="refill-scan" id="refill-scan-form">
        <span class="process-timezone">Vancouver</span>
        <input class="refill-scan-time" id="refill-scan-start" type="datetime-local" step="1" aria-label="Refill scan start time Vancouver">
        <input class="refill-scan-time" id="refill-scan-end" type="datetime-local" step="1" aria-label="Refill scan end time Vancouver">
        <input class="refill-scan-min" id="refill-scan-min" type="number" min="0" step="1" value="7" aria-label="Minimum refill count">
        <input class="refill-activity-filter" id="refill-activity-filter" type="text" inputmode="text" placeholder="O10" aria-label="Activity filter code">
        <input class="refill-rate-min" id="refill-rate-min" type="number" min="0" max="100" step="0.1" placeholder="rate %" aria-label="Minimum execution rate percent">
        <input class="refill-scan-contracts-min" id="refill-scan-contracts-min" type="number" min="0" step="1" placeholder="contracts" aria-label="Minimum contracts">
        <input class="spike-score-min" id="spike-score-min" type="number" step="0.001" value="12" aria-label="Minimum spike score">
        <button id="refill-scan-submit" type="submit">Refill</button>
        <button class="spike-score" id="spike-score-submit" type="button">Spike Score</button>
        <button class="ask-del" id="ask-del-submit" type="button">Ask Del</button>
        <button class="bid-del" id="bid-del-submit" type="button">Bid Del</button>
        <span class="refill-scan-status" id="refill-scan-status"></span>
      </form>
      <button class="clear-markers-button" id="clear-markers" type="button">Clear</button>
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
    const refillScanForm = document.getElementById("refill-scan-form");
    const refillScanStartInput = document.getElementById("refill-scan-start");
    const refillScanEndInput = document.getElementById("refill-scan-end");
    const refillScanMinInput = document.getElementById("refill-scan-min");
    const refillActivityFilterInput = document.getElementById("refill-activity-filter");
    const refillRateMinInput = document.getElementById("refill-rate-min");
    const refillScanContractsMinInput = document.getElementById("refill-scan-contracts-min");
    const spikeScoreMinInput = document.getElementById("spike-score-min");
    const refillScanSubmitButton = document.getElementById("refill-scan-submit");
    const spikeScoreSubmitButton = document.getElementById("spike-score-submit");
    const askDeleteSubmitButton = document.getElementById("ask-del-submit");
    const bidDeleteSubmitButton = document.getElementById("bid-del-submit");
    const clearMarkersButton = document.getElementById("clear-markers");
    const refillScanStatusEl = document.getElementById("refill-scan-status");
    const ACTIVE_TIMEFRAME = "__ACTIVE_TIMEFRAME__";
    const TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"];
    const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };
    const DOM_REFILL_MARKER_MIN_COUNT = 1;
    const DOM_REFILL_MARKER_SPAN_CANDLES = 5;
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
    const replayTriggerSignalsByCandleOpen = new Map();
    const refillScanMarkersByCandleOpen = new Map();
    const spikeScoreMarkersByCandleOpen = new Map();
    const deleteScanMarkersByCandleOpen = new Map();
    let activeRefillScanMinCount = 7;
    let deleteScanRangeKey = "";
    let markerOverlaysHidden = false;
    let replayOverlayActive = false;

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
          marker?.marker_price ?? marker?.price ?? marker?.level_price ?? marker?.reference_price ?? "",
          marker?.side ?? marker?.top_order_side ?? "",
          marker?.marker_time_ms ?? marker?.timestamp_ms ?? marker?.event_time_ms ?? "",
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
      if (side === "ASK") return "#f85149";
      if (side === "BID") return "#3fb950";
      return "#8b949e";
    }
    function domRefillMarkerPrice(marker) {
      return maybeNum(
        marker?.marker_price
        ?? marker?.price
        ?? marker?.level_price
        ?? marker?.reference_price,
      );
    }
    function domRefillMarkerCount(marker) {
      return Math.trunc(num(
        marker?.delete_count
        ?? marker?.price_base_refill_count
        ?? marker?.refill_count
        ?? marker?.positive_refill_count
        ?? marker?.top_order_positive_refill_count,
      ));
    }
    function domRefillMarkerContracts(marker) {
      const priceBaseContracts = Math.trunc(num(marker?.price_base_refill_contracts));
      if (marker?.price_base_refill_contracts !== undefined && marker?.price_base_refill_contracts !== null) {
        return priceBaseContracts;
      }
      const filledContracts = Math.trunc(num(
        marker?.deleted_contracts
        ?? marker?.refill_filled_contracts
        ?? marker?.positive_refill_filled_total
        ?? marker?.top_order_positive_refill_filled_total,
      ));
      if (filledContracts > 0) return filledContracts;
      const executedContracts = Math.trunc(num(marker?.executed_contracts));
      if (executedContracts > 0) return executedContracts;
      return Math.trunc(num(
        marker?.refill_contracts
        ?? marker?.positive_refill_total
        ?? marker?.refill_total
        ?? marker?.top_order_positive_refill_total,
      ));
    }
    function domRefillExecutedContracts(marker) {
      return Math.max(0, Math.trunc(num(marker?.executed_refill_contracts)));
    }
    function domRefillExecutionRate(marker) {
      const added = domRefillMarkerContracts(marker);
      const executed = Math.min(added, domRefillExecutedContracts(marker));
      return added > 0 ? (executed / added) * 100 : 0;
    }
    function deleteScanMarkerLabel(marker) {
      const cCount = Math.trunc(num(marker?.c_delete_count));
      const cContracts = Math.trunc(num(marker?.c_deleted_contracts));
      const mCount = Math.trunc(num(marker?.m_delete_count));
      const mContracts = Math.trunc(num(marker?.m_deleted_contracts));
      const parts = [
        `C: ${cCount}(${cContracts})`,
      ];
      if (mCount > 0 || mContracts > 0) {
        parts.push(`M: ${mCount}(${mContracts})`);
      }
      return parts.join(" | ");
    }
    function domRefillMarkerLabel(marker, includeContracts = false) {
      if (String(marker?.source || "").toUpperCase() === "DELETE_SCAN" || marker?.type === "DOM_DELETE_SCAN") {
        return deleteScanMarkerLabel(marker);
      }
      const refillCount = domRefillMarkerCount(marker);
      if (!includeContracts) return String(refillCount);
      if (String(marker?.display_text || "").trim()) return String(marker.display_text);
      const added = domRefillMarkerContracts(marker);
      const executed = Math.min(added, domRefillExecutedContracts(marker));
      const rate = domRefillExecutionRate(marker).toFixed(1).replace(/\.0$/, "");
      return `${refillCount}(${added}) E${executed} - ${rate}%`;
    }
    function replayPayloadToDomMarker(payload) {
      const timestampMs = replayPayloadTimestampMs(payload);
      const refillCount = Math.trunc(num(
        payload?.price_base_refill_count
        ?? payload?.positive_refill_count
        ?? payload?.refill_count,
      ));
      if (
        !Number.isFinite(timestampMs)
        || timestampMs <= 0
        || (refillCount <= 0 && !Boolean(payload?.has_price_activity))
      ) return null;
      const priceBaseContracts = Math.trunc(num(payload?.price_base_refill_contracts));
      const explicitFilledContracts = Math.trunc(num(
        payload?.refill_filled_contracts
        ?? payload?.positive_refill_filled_total,
      ));
      const executedContracts = Math.trunc(num(payload?.executed_refill_contracts));
      const legacyRefillContracts = Math.trunc(num(
        payload?.positive_refill_total
        ?? payload?.refill_contracts
        ?? payload?.refill_total,
      ));
      const hasPriceBaseContracts = payload?.price_base_refill_contracts !== undefined
        && payload?.price_base_refill_contracts !== null;
      const refillContracts = hasPriceBaseContracts
        ? priceBaseContracts
        : legacyRefillContracts;
      const markerPrice = String(
        payload?.marker_price
        ?? payload?.footprint_bin_low
        ?? payload?.price
        ?? payload?.level_price
        ?? payload?.reference_price
        ?? "",
      );
      return {
        ...payload,
        id: markerStableId(payload),
        output_id: markerStableId(payload),
        event_id: markerStableId(payload),
        type: "DOM_POSITIVE_REFILL",
        source: payload?.source || "DATA_PROCESS_REFILL_ORDER_CLOSED",
        timestamp_ms: timestampMs,
        event_time_ms: timestampMs,
        marker_time_ms: timestampMs,
        marker_price: markerPrice,
        price: markerPrice,
        source_price: String(payload?.price ?? payload?.level_price ?? payload?.reference_price ?? ""),
        side: domRefillMarkerSide(payload),
        order_id: String(payload?.order_id ?? payload?.venue_order_id ?? ""),
        venue_order_id: String(payload?.venue_order_id ?? payload?.order_id ?? ""),
        positive_refill_count: refillCount,
        refill_count: refillCount,
        price_base_refill_count: refillCount,
        price_base_refill_contracts: refillContracts,
        refill_added_contracts: refillContracts,
        executed_refill_contracts: Math.min(
          refillContracts,
          Math.max(0, Math.trunc(num(payload?.executed_refill_contracts))),
        ),
        withdrawn_refill_contracts: Math.max(0, Math.trunc(num(payload?.withdrawn_refill_contracts))),
        refill_execution_rate: num(payload?.refill_execution_rate),
        refill_display: String(payload?.refill_display ?? ""),
        refill_method: "price_base_refill",
        refill_contracts: refillContracts,
        positive_refill_total: refillContracts,
        refill_total: refillContracts,
        refill_filled_contracts: explicitFilledContracts > 0 ? explicitFilledContracts : executedContracts,
        positive_refill_filled_total: explicitFilledContracts > 0 ? explicitFilledContracts : executedContracts,
      };
    }
    function deletePayloadToDomMarker(payload) {
      const timestampMs = replayPayloadTimestampMs(payload);
      const deleteCount = Math.trunc(num(payload?.delete_count ?? payload?.refill_count));
      const deletedContracts = Math.trunc(num(
        payload?.deleted_contracts
        ?? payload?.refill_filled_contracts
        ?? payload?.positive_refill_filled_total,
      ));
      if (!Number.isFinite(timestampMs) || timestampMs <= 0 || deleteCount <= 0) return null;
      const markerPrice = String(
        payload?.marker_price
        ?? payload?.price
        ?? payload?.level_price
        ?? payload?.reference_price
        ?? "",
      );
      return {
        ...payload,
        id: markerStableId(payload),
        output_id: markerStableId(payload),
        event_id: markerStableId(payload),
        type: "DOM_DELETE_SCAN",
        source: "DELETE_SCAN",
        timestamp_ms: timestampMs,
        event_time_ms: timestampMs,
        marker_time_ms: timestampMs,
        marker_price: markerPrice,
        price: markerPrice,
        side: domRefillMarkerSide(payload),
        delete_count: deleteCount,
        deleted_contracts: deletedContracts,
        c_delete_count: Math.trunc(num(payload?.c_delete_count)),
        c_deleted_contracts: Math.trunc(num(payload?.c_deleted_contracts)),
        m_delete_count: Math.trunc(num(payload?.m_delete_count)),
        m_deleted_contracts: Math.trunc(num(payload?.m_deleted_contracts)),
        refill_count: deleteCount,
        refill_filled_contracts: deletedContracts,
        positive_refill_count: deleteCount,
        positive_refill_filled_total: deletedContracts,
      };
    }
    function rememberReplayPayloadMarkers(replay) {
      markerOverlaysHidden = false;
      replayOverlayActive = true;
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60_000;
      replayMarkersByCandleOpen.clear();
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
    function rememberReplayTriggerSignals(replay) {
      markerOverlaysHidden = false;
      replayOverlayActive = true;
      replayTriggerSignalsByCandleOpen.clear();
      for (const signal of safeArray(replay?.trigger_signals)) {
        const openTimeMs = signalTriggerTime(signal);
        if (!Number.isFinite(openTimeMs) || openTimeMs <= 0) continue;
        const signals = replayTriggerSignalsByCandleOpen.get(openTimeMs) || [];
        signals.push(signal);
        replayTriggerSignalsByCandleOpen.set(openTimeMs, signals);
      }
    }
    function rememberRefillScanPayloadMarkers(scan, minimumRefillCount) {
      markerOverlaysHidden = false;
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60_000;
      const nextMarkersByOpen = new Map();
      let markerCount = 0;
      for (const payload of safeArray(scan?.payloads)) {
        const marker = replayPayloadToDomMarker(payload);
        if (!marker || domRefillMarkerCount(marker) < minimumRefillCount) continue;
        const sourceId = markerStableId(marker);
        marker.id = `REFILL_SCAN|${minimumRefillCount}|${sourceId}`;
        marker.output_id = marker.id;
        marker.event_id = marker.id;
        marker.source = "REFILL_SCAN";
        marker.scan_min_refill_count = minimumRefillCount;
        const openTimeMs = Math.floor(Number(marker.timestamp_ms) / intervalMs) * intervalMs;
        const markers = nextMarkersByOpen.get(openTimeMs) || [];
        nextMarkersByOpen.set(
          openTimeMs,
          mergeDomRefillMarkers(markers, [marker]),
        );
        markerCount += 1;
      }
      for (const payload of safeArray(scan?.summary_payloads)) {
        const marker = replayPayloadToDomMarker(payload);
        if (!marker) continue;
        const sourceId = markerStableId(marker);
        marker.id = `refill-scan:${sourceId}`;
        marker.output_id = marker.id;
        marker.event_id = marker.id;
        marker.source = "REFILL_SCAN";
        marker.scan_min_refill_count = minimumRefillCount;
        const openTimeMs = Math.floor(Number(marker.timestamp_ms) / intervalMs) * intervalMs;
        const markers = nextMarkersByOpen.get(openTimeMs) || [];
        nextMarkersByOpen.set(
          openTimeMs,
          mergeDomRefillMarkers(markers, [marker]),
        );
      }
      refillScanMarkersByCandleOpen.clear();
      for (const [openTimeMs, markers] of nextMarkersByOpen.entries()) {
        refillScanMarkersByCandleOpen.set(openTimeMs, markers);
      }
      activeRefillScanMinCount = minimumRefillCount;
      return markerCount;
    }
    function spikeScorePayloadToMarker(payload) {
      const timestampMs = replayPayloadTimestampMs(payload);
      const price = String(payload?.marker_price ?? payload?.price ?? "");
      const score = maybeNum(payload?.contract_spike_score ?? payload?.spike_score);
      if (!Number.isFinite(timestampMs) || timestampMs <= 0 || !price || !Number.isFinite(score)) {
        return null;
      }
      return {
        ...payload,
        id: markerStableId(payload),
        output_id: markerStableId(payload),
        event_id: markerStableId(payload),
        type: "DOM_SPIKE_SCORE_SCAN",
        source: "SPIKE_SCORE_SCAN",
        timestamp_ms: timestampMs,
        event_time_ms: timestampMs,
        marker_time_ms: timestampMs,
        marker_price: price,
        price,
        contract_spike_score: score,
        ask_refill_count: Math.max(0, Math.trunc(num(payload?.ask_refill_count))),
        bid_refill_count: Math.max(0, Math.trunc(num(payload?.bid_refill_count))),
        ask_execution_count: Math.max(0, Math.trunc(num(payload?.ask_execution_count))),
        bid_execution_count: Math.max(0, Math.trunc(num(payload?.bid_execution_count))),
      };
    }
    function rememberSpikeScoreMarkers(scan) {
      markerOverlaysHidden = false;
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60_000;
      spikeScoreMarkersByCandleOpen.clear();
      let markerCount = 0;
      for (const payload of safeArray(scan?.spike_score_payloads)) {
        const marker = spikeScorePayloadToMarker(payload);
        if (!marker) continue;
        const openTimeMs = Math.floor(Number(marker.timestamp_ms) / intervalMs) * intervalMs;
        const markers = spikeScoreMarkersByCandleOpen.get(openTimeMs) || [];
        spikeScoreMarkersByCandleOpen.set(
          openTimeMs,
          mergeDomRefillMarkers(markers, [marker]),
        );
        markerCount += 1;
      }
      return markerCount;
    }
    function rememberDeleteScanPayloadMarkers(scan, side, minimumDeleteCount = 1, minimumContracts = 0) {
      markerOverlaysHidden = false;
      const intervalMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60_000;
      const normalizedSide = String(side || "").trim().toUpperCase();
      const normalizedMinimumDeleteCount = Math.max(1, Math.trunc(num(minimumDeleteCount || 1)));
      const normalizedMinimumContracts = Math.max(0, Math.trunc(num(minimumContracts || 0)));
      const rangeKey = `${Number(scan?.start_ms || 0)}|${Number(scan?.end_ms || 0)}`;
      if (rangeKey !== deleteScanRangeKey) {
        deleteScanMarkersByCandleOpen.clear();
        deleteScanRangeKey = rangeKey;
      }
      for (const [openTimeMs, markers] of [...deleteScanMarkersByCandleOpen.entries()]) {
        const kept = safeArray(markers).filter(marker => marker?.delete_scan_side !== normalizedSide);
        if (kept.length) {
          deleteScanMarkersByCandleOpen.set(openTimeMs, kept);
        } else {
          deleteScanMarkersByCandleOpen.delete(openTimeMs);
        }
      }
      let markerCount = 0;
      for (const payload of safeArray(scan?.payloads)) {
        const marker = deletePayloadToDomMarker(payload);
        if (!marker || domRefillMarkerCount(marker) < normalizedMinimumDeleteCount) continue;
        if (domRefillMarkerContracts(marker) < normalizedMinimumContracts) continue;
        const sourceId = markerStableId(marker);
        marker.id = `DELETE_SCAN|${normalizedSide}|${normalizedMinimumDeleteCount}|${normalizedMinimumContracts}|${sourceId}`;
        marker.output_id = marker.id;
        marker.event_id = marker.id;
        marker.source = "DELETE_SCAN";
        marker.delete_scan_side = normalizedSide;
        marker.delete_scan_min_count = normalizedMinimumDeleteCount;
        marker.delete_scan_min_contracts = normalizedMinimumContracts;
        const openTimeMs = Math.floor(Number(marker.timestamp_ms) / intervalMs) * intervalMs;
        const markers = deleteScanMarkersByCandleOpen.get(openTimeMs) || [];
        deleteScanMarkersByCandleOpen.set(
          openTimeMs,
          mergeDomRefillMarkers(markers, [marker]),
        );
        markerCount += 1;
      }
      return markerCount;
    }
    function redrawRefillScanMarkers() {
      for (const chart of charts.values()) {
        chart.draw();
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
      if (!processStartInput.value && windowStartMs > 0) {
        processStartInput.value = vancouverInputValue(windowStartMs);
      }
      if (!processEndInput.value && windowEndMs > 0) {
        processEndInput.value = vancouverInputValue(windowEndMs);
      }
      if (refillScanStartInput && !refillScanStartInput.value && windowStartMs > 0) {
        refillScanStartInput.value = vancouverInputValue(windowStartMs);
      }
      if (refillScanEndInput && !refillScanEndInput.value && windowEndMs > 0) {
        refillScanEndInput.value = vancouverInputValue(windowEndMs);
      }
    }
    function signalTriggerTime(signal) {
      return Number(signal?.trigger_candle_time_ms ?? 0);
    }
    function signalMarkerShape(signal) {
      return String(signal?.marker_shape || (String(signal?.signal_type || "").startsWith("EXIT_") ? "SQUARE" : "ARROW")).trim().toUpperCase();
    }
    function triggerMarkerBounds(signal, centerX, yHigh, yLow, plotH = Infinity) {
      const shape = signalMarkerShape(signal);
      const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
      const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
      const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
      const arrowHeight = 12;
      const plotPad = 4;
      if (shape === "SQUARE") {
        const size = 10;
        const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
        return { left: centerX - size / 2, right: centerX + size / 2, top: centerY - size / 2, bottom: centerY + size / 2 };
      }
      if (markerDirection === "DOWN" && markerColor === "RED") {
        let tipY = yHigh - 4;
        if (tipY - arrowHeight < plotPad) tipY = arrowHeight + plotPad;
        if (Number.isFinite(plotH)) tipY = Math.min(tipY, Math.max(plotPad, plotH - plotPad));
        const baseY = tipY - arrowHeight;
        return { left: centerX - 8, right: centerX + 8, top: baseY - 3, bottom: tipY + 4 };
      }
      if (markerDirection === "UP" && markerColor === "GREEN") {
        let tipY = yLow + 4;
        if (Number.isFinite(plotH) && tipY + arrowHeight > plotH - plotPad) {
          tipY = Math.max(plotPad, plotH - arrowHeight - plotPad);
        }
        const baseY = tipY + arrowHeight;
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
    function drawTriggerMarkers(ctx, candle, centerX, yHigh, yLow, plotH = Infinity) {
      for (const signal of candleTriggerSignals(candle)) {
        const markerDirection = String(signal?.marker_direction || "").trim().toUpperCase();
        const markerColor = String(signal?.marker_color || "").trim().toUpperCase();
        const markerShape = signalMarkerShape(signal);
        const markerPosition = String(signal?.marker_position || "").trim().toUpperCase();
        const fillColor = markerColor === "GREEN" ? "#3fb950" : "#f85149";
        const arrowHeight = 12;
        const plotPad = 4;
        if (markerShape === "SQUARE") {
          const size = 10;
          const centerY = markerPosition === "BELOW" ? yLow + 10 : Math.max(6, yHigh - 10);
          ctx.fillStyle = fillColor;
          ctx.fillRect(centerX - size / 2, centerY - size / 2, size, size);
          ctx.strokeStyle = "#0d1117";
          ctx.lineWidth = 1.5;
          ctx.strokeRect(centerX - size / 2, centerY - size / 2, size, size);
        } else if (markerDirection === "DOWN" && markerColor === "RED") {
          let tipY = yHigh - 4;
          if (tipY - arrowHeight < plotPad) tipY = arrowHeight + plotPad;
          if (Number.isFinite(plotH)) tipY = Math.min(tipY, Math.max(plotPad, plotH - plotPad));
          const baseY = tipY - arrowHeight;
          ctx.fillStyle = "#f85149";
          ctx.beginPath();
          ctx.moveTo(centerX, tipY);
          ctx.lineTo(centerX - 7, baseY);
          ctx.lineTo(centerX + 7, baseY);
          ctx.closePath();
          ctx.fill();
        } else if (markerDirection === "UP" && markerColor === "GREEN") {
          let tipY = yLow + 4;
          if (Number.isFinite(plotH) && tipY + arrowHeight > plotH - plotPad) {
            tipY = Math.max(plotPad, plotH - arrowHeight - plotPad);
          }
          const baseY = tipY + arrowHeight;
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
    function priceDecimalsForStep(step) {
      const parsedStep = Math.abs(num(step));
      if (!Number.isFinite(parsedStep) || parsedStep <= 0) return null;
      for (let decimals = 0; decimals <= 8; decimals += 1) {
        const scaled = parsedStep * (10 ** decimals);
        if (Math.abs(scaled - Math.round(scaled)) < 1e-8) return decimals;
      }
      return 8;
    }
    function fmtPrice(value, step = 0) {
      const decimals = priceDecimalsForStep(step);
      if (decimals !== null) return num(value).toFixed(decimals);
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
        this.binTickCount = Number.parseInt(session?.bin_tick_count, 10) || 0;
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
          this.resetAutoScale();
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
        this.updateHeader(session);
        const priceStep = num(session?.price_step);
        const size = num(session?.fixed_bin_size);
        const binTickCount = Number.parseInt(session?.bin_tick_count, 10);
        const sizeChanged = (
          size > 0
          && this.fixedSize > 0
          && Math.abs(size - this.fixedSize) > 1e-9
        );
        const binTickChanged = (
          Number.isFinite(binTickCount)
          && binTickCount > 0
          && this.binTickCount > 0
          && binTickCount !== this.binTickCount
        );
        if (sizeChanged || binTickChanged) {
          this.candleMap.clear();
          this.candles = [];
          this.cachedWindows = [];
        }
        this.rememberCachedWindow(session);
        if (size > 0) this.fixedSize = size;
        if (priceStep > 0) this.priceStep = priceStep;
        if (Number.isFinite(binTickCount) && binTickCount > 0) {
          this.binTickCount = binTickCount;
        }
        for (const candle of safeArray(session.candles)) {
          const openTime = candleOpen(candle);
          const replayMarkers = replayMarkersByCandleOpen.get(openTime);
          if (markerOverlaysHidden) {
            candle.dom_refill_markers = [];
            candle.trigger_signals = [];
          } else if (replayOverlayActive) {
            candle.dom_refill_markers = [];
            candle.trigger_signals = [];
            if (replayMarkers?.length) {
              candle.dom_refill_markers = mergeDomRefillMarkers([], replayMarkers);
            }
          } else if (replayMarkers?.length) {
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
        if (!markerOverlaysHidden && !replayOverlayActive) {
          for (const signal of safeArray(session?.signals)) {
            const openTime = signalTriggerTime(signal);
            if (openTime <= 0) continue;
            const signals = signalsByOpen.get(openTime) || [];
            signals.push(signal);
            signalsByOpen.set(openTime, signals);
          }
        }
        for (const candle of this.candles) {
          const openTime = candleOpen(candle);
          const replaySignals = markerOverlaysHidden ? [] : replayTriggerSignalsByCandleOpen.get(openTime);
          const signalsById = new Map();
          for (const signal of [
            ...candleTriggerSignals(candle),
            ...(signalsByOpen.get(openTime) || []),
            ...(replaySignals || []),
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
            includePrice(domRefillMarkerPrice(marker));
          }
          for (const marker of safeArray(replayMarkersByCandleOpen.get(candleOpen(candle)))) {
            includePrice(domRefillMarkerPrice(marker));
          }
          for (const marker of safeArray(refillScanMarkersByCandleOpen.get(candleOpen(candle)))) {
            includePrice(domRefillMarkerPrice(marker));
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
        const rawPriceToY = price => {
          const ratio = (range.max - price) / Math.max(1e-9, range.max - range.min);
          return ratio * plotH;
        };
        const priceToY = price => Math.max(0, Math.min(plotH, rawPriceToY(price)));
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
        this.drawDomRefillMarkers(ctx, candleItems, layout, plotH, rawPriceToY);
        candleItems.forEach(item => {
          const center = item.x + this.candleWidth / 2;
          const high = maybeNum(ohlc(item.candle, "high"));
          const low = maybeNum(ohlc(item.candle, "low"));
          if (Number.isFinite(high) && Number.isFinite(low)) {
            drawTriggerMarkers(ctx, item.candle, center, priceToY(high), priceToY(low), plotH);
          }
        });
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
      drawDomRefillMarkers(ctx, candleItems, layout, plotH, rawPriceToY) {
        ctx.save();
        ctx.lineWidth = 2.5;
        ctx.font = "800 13px Segoe UI, Arial";
        ctx.textAlign = "left";
        ctx.textBaseline = "bottom";
        const occupiedLabelBoxes = [];
        const renderedRefillMarkerKeys = new Set();
        this.drawRefillMarkerSet(
          ctx,
          candleItems,
          layout,
          plotH,
          rawPriceToY,
          item => safeArray(refillScanMarkersByCandleOpen.get(candleOpen(item.candle))),
          activeRefillScanMinCount,
          true,
          occupiedLabelBoxes,
          renderedRefillMarkerKeys,
          item => mergeDomRefillMarkers(
            refillScanMarkersByCandleOpen.get(candleOpen(item.candle)),
            mergeDomRefillMarkers(
              candleDomRefillMarkers(item.candle),
              replayMarkersByCandleOpen.get(candleOpen(item.candle)),
            ),
          ),
        );
        this.drawRefillMarkerSet(
          ctx,
          candleItems,
          layout,
          plotH,
          rawPriceToY,
          item => mergeDomRefillMarkers(
            candleDomRefillMarkers(item.candle),
            replayMarkersByCandleOpen.get(candleOpen(item.candle)),
          ),
          DOM_REFILL_MARKER_MIN_COUNT,
          false,
          occupiedLabelBoxes,
          renderedRefillMarkerKeys,
        );
        this.drawRefillMarkerSet(
          ctx,
          candleItems,
          layout,
          plotH,
          rawPriceToY,
          item => safeArray(deleteScanMarkersByCandleOpen.get(candleOpen(item.candle))),
          1,
          true,
          occupiedLabelBoxes,
          renderedRefillMarkerKeys,
        );
        this.drawRefillMarkerSet(
          ctx,
          candleItems,
          layout,
          plotH,
          rawPriceToY,
          item => safeArray(spikeScoreMarkersByCandleOpen.get(candleOpen(item.candle))),
          0,
          true,
          occupiedLabelBoxes,
          renderedRefillMarkerKeys,
        );
        ctx.restore();
      }
      drawRefillMarkerSet(
        ctx,
        candleItems,
        layout,
        plotH,
        rawPriceToY,
        markersForItem,
        minimumRefillCount,
        includeContracts,
        occupiedLabelBoxes,
        renderedMarkerKeys,
        summaryMarkersForItem = markersForItem,
      ) {
        const labelBoxes = Array.isArray(occupiedLabelBoxes) ? occupiedLabelBoxes : [];
        const markerKeys = renderedMarkerKeys instanceof Set ? renderedMarkerKeys : new Set();
        const boxesOverlap = (left, right) => !(
          left.right < right.left
          || left.left > right.right
          || left.bottom < right.top
          || left.top > right.bottom
        );
        for (const item of candleItems) {
          const markers = safeArray(markersForItem(item));
          const summaryMarkers = safeArray(summaryMarkersForItem(item));
          for (const marker of markers) {
            const price = domRefillMarkerPrice(marker);
            const refillCount = domRefillMarkerCount(marker);
            if (!Number.isFinite(price) || refillCount < minimumRefillCount) continue;
            const markerSource = String(marker?.source || "").toUpperCase();
            const markerKind = markerSource === "DELETE_SCAN"
              ? "DELETE"
              : markerSource === "SPIKE_SCORE_SCAN" ? "SPIKE" : "REFILL";
            const refillPeers = markerKind === "REFILL"
              ? summaryMarkers.filter(candidate => (
                String(candidate?.source || "").toUpperCase() !== "DELETE_SCAN"
                && domRefillMarkerPrice(candidate) === price
              ))
              : [];
            const markerForSide = side => refillPeers
              .filter(candidate => domRefillMarkerSide(candidate) === side)
              .sort((left, right) => (
                domRefillMarkerCount(right) - domRefillMarkerCount(left)
                || Math.trunc(num(right?.executed_contracts))
                  - Math.trunc(num(left?.executed_contracts))
              ))[0];
            const bidMarker = markerForSide("BID");
            const askMarker = markerForSide("ASK");
            const bidRefillCount = bidMarker ? domRefillMarkerCount(bidMarker) : 0;
            const askRefillCount = askMarker ? domRefillMarkerCount(askMarker) : 0;
            const bidExecuted = bidMarker
              ? Math.max(0, Math.trunc(num(bidMarker?.executed_contracts)))
              : 0;
            const askExecuted = askMarker
              ? Math.max(0, Math.trunc(num(askMarker?.executed_contracts)))
              : 0;
            const logicalMarkerKey = markerKind === "REFILL"
              ? [
                markerKind,
                candleOpen(item.candle),
                price,
                bidRefillCount,
                askRefillCount,
                bidExecuted,
                askExecuted,
              ].join("|")
              : markerKind === "SPIKE"
              ? [
                markerKind,
                candleOpen(item.candle),
                price,
                marker?.contract_spike_score,
                marker?.ask_refill_count,
                marker?.bid_refill_count,
                marker?.ask_execution_count,
                marker?.bid_execution_count,
              ].join("|")
              : [
                markerKind,
                candleOpen(item.candle),
                price,
                domRefillMarkerSide(marker),
                refillCount,
                domRefillMarkerContracts(marker),
                domRefillExecutedContracts(marker),
              ].join("|");
            if (markerKeys.has(logicalMarkerKey)) continue;
            const y = rawPriceToY(price);
            if (!Number.isFinite(y) || y < -6 || y > plotH + 6) continue;
            const span = Math.max(1, Number(marker?.span_candles) || DOM_REFILL_MARKER_SPAN_CANDLES);
            const startX = Math.max(layout.candlePlotX, item.x);
            let endX = Math.min(layout.contentW, item.x + span * this.candleWidth);
            if (endX <= layout.candlePlotX || startX >= layout.contentW || endX <= startX) {
              continue;
            }
            markerKeys.add(logicalMarkerKey);
            const neutralColor = "#c9d1d9";
            const bidColor = "#3fb950";
            const askColor = "#f85149";
            const color = markerKind !== "REFILL"
              ? domRefillMarkerColor(marker)
              : bidExecuted > askExecuted
              ? bidColor
              : askExecuted > bidExecuted
              ? askColor
              : bidRefillCount > askRefillCount
              ? bidColor
              : askRefillCount > bidRefillCount
              ? askColor
              : "#8b949e";
            const labelParts = markerKind === "SPIKE"
              ? [
                { text: `Spike score : ${fmtMaybe(marker?.contract_spike_score, 3)} | R `, color: neutralColor },
                { text: String(Math.trunc(num(marker?.bid_refill_count))), color: bidColor },
                { text: " - ", color: neutralColor },
                { text: String(Math.trunc(num(marker?.ask_refill_count))), color: askColor },
                { text: " | E ", color: neutralColor },
                { text: String(Math.trunc(num(marker?.bid_execution_count))), color: bidColor },
                { text: " - ", color: neutralColor },
                { text: String(Math.trunc(num(marker?.ask_execution_count))), color: askColor },
              ]
              : markerKind === "REFILL"
              ? [
                { text: "R: ", color: neutralColor },
                { text: String(bidRefillCount), color: bidColor },
                { text: " | ", color: neutralColor },
                { text: String(askRefillCount), color: askColor },
                { text: "   E: ", color: neutralColor },
                { text: String(bidExecuted), color: bidColor },
                { text: " | ", color: neutralColor },
                { text: String(askExecuted), color: askColor },
              ]
              : [{ text: domRefillMarkerLabel(marker, includeContracts), color }];
            const labelPaddingX = 4;
            const labelPaddingY = 2;
            const labelWidth = labelParts.reduce(
              (width, part) => width + ctx.measureText(part.text).width,
              0,
            );
            const labelHeight = 16;
            const labelRightLimit = layout.contentW - labelWidth - (labelPaddingX * 2) - 4;
            const labelLeftLimit = layout.candlePlotX + 2;
            let effectiveSpan = span;
            let labelX = Math.max(labelLeftLimit, Math.min(endX + 4, labelRightLimit));
            const labelY = y - 3;
            let labelBox = {
              left: labelX - labelPaddingX,
              right: labelX + labelWidth + labelPaddingX,
              top: labelY - labelHeight - labelPaddingY,
              bottom: labelY + labelPaddingY,
            };
            const overlapsAnyLabel = box => labelBoxes.some(existing => boxesOverlap(box, existing));
            while (overlapsAnyLabel(labelBox) && endX < labelRightLimit - 4) {
              effectiveSpan += 1;
              endX = Math.min(layout.contentW, item.x + effectiveSpan * this.candleWidth);
              labelX = Math.max(labelLeftLimit, Math.min(endX + 4, labelRightLimit));
              labelBox = {
                left: labelX - labelPaddingX,
                right: labelX + labelWidth + labelPaddingX,
                top: labelY - labelHeight - labelPaddingY,
                bottom: labelY + labelPaddingY,
              };
            }
            const canDrawLabel = (
              labelRightLimit > labelLeftLimit
              && !overlapsAnyLabel(labelBox)
            );
            ctx.strokeStyle = color;
            ctx.shadowColor = "rgba(255,255,255,.55)";
            ctx.shadowBlur = 3;
            ctx.setLineDash([8, 5]);
            ctx.beginPath();
            ctx.moveTo(startX, y);
            ctx.lineTo(endX, y);
            ctx.stroke();
            ctx.shadowBlur = 0;
            ctx.setLineDash([]);
            if (canDrawLabel) {
              ctx.fillStyle = "rgba(0,0,0,.86)";
              ctx.fillRect(
                labelBox.left,
                labelBox.top,
                labelBox.right - labelBox.left,
                labelBox.bottom - labelBox.top,
              );
              let partX = labelX;
              for (const part of labelParts) {
                ctx.fillStyle = part.color;
                ctx.fillText(part.text, partX, labelY);
                partX += ctx.measureText(part.text).width;
              }
              labelBoxes.push(labelBox);
            }
          }
        }
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
          ctx.fillText(
            fmtPrice(price, this.priceStep),
            layout.contentW + layout.axisWidth - 8,
            priceToY(price),
          );
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
            placeTooltip(
              this.tooltip,
              this.hover,
              this.canvas.clientWidth || layout.contentW,
              this.canvas.clientHeight || plotH,
              520,
              180,
            );
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
        const hoverMarkersByLevelSide = new Map();
        for (const marker of [
          ...safeArray(replayMarkersByCandleOpen.get(candleOpen(candle))),
          ...safeArray(refillScanMarkersByCandleOpen.get(candleOpen(candle))),
        ]) {
          const markerPrice = domRefillMarkerPrice(marker);
          const markerSide = domRefillMarkerSide(marker);
          if (!Number.isFinite(markerPrice) || !markerSide) continue;
          hoverMarkersByLevelSide.set(`${markerPrice}|${markerSide}`, marker);
        }
        const hoverMarkers = [...hoverMarkersByLevelSide.values()];
        const closestMarker = hoverMarkers.reduce((closest, marker) => {
          const markerPrice = domRefillMarkerPrice(marker);

          if (!Number.isFinite(markerPrice)) {
            return closest;
          }

          if (!closest) {
            return marker;
          }

          return (
            Math.abs(markerPrice - price)
            < Math.abs(domRefillMarkerPrice(closest) - price)
          )
            ? marker
            : closest;
        }, null);

        const closestPrice = closestMarker
          ? domRefillMarkerPrice(closestMarker)
          : NaN;

        const activeMarkers = Number.isFinite(closestPrice)
          && Math.abs(closestPrice - price) <= Math.max(this.priceStep, 1e-9)
          ? hoverMarkers.filter((marker) => {
              const markerPrice = domRefillMarkerPrice(marker);

              return (
                Number.isFinite(markerPrice)
                && Math.abs(markerPrice - closestPrice) <= 1e-9
              );
            })
          : [];

        activeMarkers.sort((left, right) => {
          const sidePriority = {
            BID: 0,
            ASK: 1,
          };

          const leftSide = String(left?.side || "").trim().toUpperCase();
          const rightSide = String(right?.side || "").trim().toUpperCase();

          return (
            (sidePriority[leftSide] ?? 9)
            - (sidePriority[rightSide] ?? 9)
          );
        });

        const markerDetails = activeMarkers.length > 0
          ? `

        ${activeMarkers.map((marker) => {
          const side = String(marker?.side || "").trim().toUpperCase() || "UNKNOWN";

          const opening = Math.trunc(
            num(marker?.opening_liquidity)
          );

          const openingSuffix = marker?.opening_liquidity_inferred
            ? " (inferred)"
            : "";

          const rateText = marker?.level_execution_rate_defined === false
            ? "N/A"
            : `${num(marker?.level_execution_rate)
                .toFixed(1)
                .replace(/\.0$/, "")}%`;

          const invariantText = marker?.level_execution_invariant_ok === false
            ? "FAILED"
            : "OK";

          return `${side}
Orders: ${Math.trunc(num(marker?.order_count))} | Opening: ${opening}${openingSuffix}
Gross Added: ${Math.trunc(num(marker?.gross_added_contracts ?? marker?.added_contracts))} | Non-refill: ${Math.trunc(num(marker?.non_refill_added_contracts))}
Available: ${Math.trunc(num(marker?.available_liquidity))} | Fill Events: ${Math.trunc(num(marker?.fill_event_count))}
Executed: ${Math.trunc(num(marker?.executed_contracts))} | Withdrawn: ${Math.trunc(num(marker?.withdrawn_contracts ?? marker?.cancelled_or_withdrawn_contracts))} | Closing: ${Math.trunc(num(marker?.closing_liquidity))}
Level Rate: ${rateText} | Invariant: ${invariantText}

Refills: ${domRefillMarkerCount(marker)} | Refill Added: ${domRefillMarkerContracts(marker)}
Refill Executed: ${domRefillExecutedContracts(marker)} | Refill Withdrawn: ${Math.trunc(num(marker?.withdrawn_refill_contracts))}
Refill Remaining: ${Math.max(
  0,
  domRefillMarkerContracts(marker)
  - domRefillExecutedContracts(marker)
  - Math.trunc(num(marker?.withdrawn_refill_contracts))
)}
Refill Rate: ${domRefillExecutionRate(marker).toFixed(1).replace(/\.0$/, "")}%`;
}).join("\n\n")}`
  : "";
        this.tooltip.textContent = `${ACTIVE_TIMEFRAME} ${new Date(candleOpen(candle)).toLocaleString([], { timeZone: "America/Vancouver" })}
O ${candle.open_price} H ${candle.high_price}
L ${candle.low_price} C ${candle.close_price}${markerDetails}`;
        this.tooltip.style.opacity = "1";
        const tooltipWidth = this.tooltip.offsetWidth || 520;
        const tooltipHeight = this.tooltip.offsetHeight || 180;
        const tooltipBoundaryWidth = this.canvas.clientWidth || layout.contentW;
        const tooltipBoundaryHeight = this.canvas.clientHeight || plotH;
        let left = this.hover.x + 12;
        if (left + tooltipWidth + 8 > tooltipBoundaryWidth) left = this.hover.x - tooltipWidth - 12;
        left = Math.max(8, Math.min(left, Math.max(8, tooltipBoundaryWidth - tooltipWidth - 8)));
        let top = this.hover.y + 12;
        if (top + tooltipHeight + 8 > tooltipBoundaryHeight) top = this.hover.y - tooltipHeight - 12;
        top = Math.max(8, Math.min(top, Math.max(8, tooltipBoundaryHeight - tooltipHeight - 8)));
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
        ctx.fillText(fmtPrice(price, this.priceStep), plotW + axisWidth / 2, boxY + boxHeight / 2);
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
        for (const [key, candle] of chart.candleMap.entries()) {
          candle.dom_refill_markers = [];
          candle.trigger_signals = [];
          chart.candleMap.set(key, candle);
        }
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
        for (const [openTimeMs, signals] of replayTriggerSignalsByCandleOpen.entries()) {
          const key = String(openTimeMs);
          const candle = chart.candleMap.get(key);
          if (!candle) continue;
          const signalsById = new Map();
          for (const signal of [
            ...candleTriggerSignals(candle),
            ...safeArray(signals),
          ]) {
            const signalId = String(signal?.signal_id || "").trim();
            const signalKey = signalId || `${signal?.direction || ""}|${signalTriggerTime(signal)}`;
            signalsById.set(signalKey, signal);
          }
          candle.trigger_signals = [...signalsById.values()];
          chart.candleMap.set(key, candle);
          appliedCount += safeArray(signals).length;
        }
        chart.draw();
        chart.syncScrollbar();
      }
      return appliedCount;
    }
    function clearCandleMarkers() {
      markerOverlaysHidden = true;
      replayOverlayActive = false;
      replayMarkersByCandleOpen.clear();
      replayTriggerSignalsByCandleOpen.clear();
      refillScanMarkersByCandleOpen.clear();
      spikeScoreMarkersByCandleOpen.clear();
      deleteScanMarkersByCandleOpen.clear();
      deleteScanRangeKey = "";
      for (const chart of charts.values()) {
        for (const [key, candle] of chart.candleMap.entries()) {
          candle.dom_refill_markers = [];
          candle.trigger_signals = [];
          chart.candleMap.set(key, candle);
        }
        chart.candles = [...chart.candleMap.values()].sort((a, b) => candleOpen(a) - candleOpen(b));
        chart.draw();
        chart.syncScrollbar();
      }
      processReplayStatusEl.textContent = "Markers cleared";
      processReplayStatusEl.title = "";
      refillScanStatusEl.textContent = "";
      refillScanStatusEl.title = "";
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
        payload?.marker_time_ms
        ?? payload?.footprint_open_time_ms
        ?? payload?.timestamp_ms
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
        const triggerCount = Number(
          replay?.trigger_signal_count ?? safeArray(replay?.trigger_signals).length,
        );
        rememberReplayPayloadMarkers(replay);
        rememberReplayTriggerSignals(replay);
        const appliedMarkerCount = applyReplayMarkersToCharts();
        processReplayStatusEl.textContent = (
          `${payloadCount}/${eventCount} CSV`
          + (triggerCount > 0 ? ` | ${triggerCount} triggers` : "")
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
    async function runRefillScan(event) {
      event.preventDefault();
      const startValue = String(refillScanStartInput?.value || "").trim();
      const endValue = String(refillScanEndInput?.value || "").trim();
      const minimumRefillCount = Math.max(
        0,
        Math.trunc(num(refillScanMinInput?.value ?? 1)),
      );
      const activityFilter = String(refillActivityFilterInput?.value || "").trim().toUpperCase();
      if (activityFilter && !/^[OAB]\d+$/.test(activityFilter)) {
        refillScanStatusEl.textContent = "Invalid filter";
        return;
      }
      const rateMinText = String(refillRateMinInput?.value || "").trim();
      const rateMin = rateMinText === "" ? null : Number(rateMinText);
      if (rateMin !== null && (!Number.isFinite(rateMin) || rateMin < 0 || rateMin > 100)) {
        refillScanStatusEl.textContent = "Invalid rate";
        return;
      }
      if (!startValue || !endValue) {
        refillScanStatusEl.textContent = "Set range";
        return;
      }
      refillScanSubmitButton.disabled = true;
      refillScanStatusEl.textContent = "Scanning";
      try {
        const params = new URLSearchParams({
          start_vancouver: startValue,
          end_vancouver: endValue,
          refill_min: String(minimumRefillCount),
          activity_filter: activityFilter,
        });
        if (rateMin !== null) params.set("rate_min", String(rateMin));
        const response = await fetch(
          `/refill-scan/${ACTIVE_TIMEFRAME}?${params.toString()}`,
          { cache: "no-store" },
        );
        const scan = response.ok ? await response.json() : null;
        if (!response.ok || scan?.status !== "OK") {
          throw new Error(scan?.message || "Refill scan failed");
        }
        const markerCount = rememberRefillScanPayloadMarkers(scan, minimumRefillCount);
        redrawRefillScanMarkers();
        const matchedPayloadCount = Number(scan?.matched_payload_count ?? markerCount);
        const eventCount = Number(scan?.processed_event_count || 0);
        refillScanStatusEl.textContent = `${markerCount}/${matchedPayloadCount}`;
        refillScanStatusEl.title = `Processed events: ${eventCount}`;
      } catch (error) {
        refillScanStatusEl.textContent = error?.message || "Scan failed";
      } finally {
        refillScanSubmitButton.disabled = false;
      }
    }
    async function runDeleteScan(side) {
      const startValue = String(refillScanStartInput?.value || "").trim();
      const endValue = String(refillScanEndInput?.value || "").trim();
      const normalizedSide = String(side || "").trim().toUpperCase();
      const minimumDeleteCount = Math.max(
        1,
        Math.trunc(num(refillScanMinInput?.value || 1)),
      );
      const minimumContracts = Math.max(
        0,
        Math.trunc(num(refillScanContractsMinInput?.value || 0)),
      );
      const button = normalizedSide === "ASK" ? askDeleteSubmitButton : bidDeleteSubmitButton;
      if (!startValue || !endValue) {
        refillScanStatusEl.textContent = "Set range";
        return;
      }
      button.disabled = true;
      refillScanStatusEl.textContent = `${normalizedSide} del`;
      try {
        const params = new URLSearchParams({
          start_vancouver: startValue,
          end_vancouver: endValue,
          side: normalizedSide,
          delete_min: String(minimumDeleteCount),
          delete_contracts_min: String(minimumContracts),
        });
        const response = await fetch(
          `/delete-scan/${ACTIVE_TIMEFRAME}?${params.toString()}`,
          { cache: "no-store" },
        );
        const scan = response.ok ? await response.json() : null;
        if (!response.ok || scan?.status !== "OK") {
          throw new Error(scan?.message || "Delete scan failed");
        }
        const markerCount = rememberDeleteScanPayloadMarkers(
          scan,
          normalizedSide,
          minimumDeleteCount,
          minimumContracts,
        );
        redrawRefillScanMarkers();
        const matchedPayloadCount = Number(scan?.matched_payload_count ?? markerCount);
        const eventCount = Number(scan?.processed_event_count || 0);
        refillScanStatusEl.textContent = `${normalizedSide} ${markerCount}/${matchedPayloadCount}`;
        refillScanStatusEl.title = `Processed events: ${eventCount}`;
      } catch (error) {
        refillScanStatusEl.textContent = error?.message || "Delete scan failed";
      } finally {
        button.disabled = false;
      }
    }
    async function runSpikeScoreScan() {
      const startValue = String(refillScanStartInput?.value || "").trim();
      const endValue = String(refillScanEndInput?.value || "").trim();
      const minimumScoreText = String(spikeScoreMinInput?.value || "").trim();
      const minimumScore = Number(minimumScoreText);
      if (!startValue || !endValue) {
        refillScanStatusEl.textContent = "Set range";
        return;
      }
      if (!minimumScoreText || !Number.isFinite(minimumScore)) {
        refillScanStatusEl.textContent = "Invalid spike score";
        return;
      }
      spikeScoreSubmitButton.disabled = true;
      refillScanStatusEl.textContent = "Spike scanning";
      try {
        const params = new URLSearchParams({
          start_vancouver: startValue,
          end_vancouver: endValue,
          refill_min: "0",
          spike_score_min: String(minimumScore),
        });
        const response = await fetch(
          `/refill-scan/${ACTIVE_TIMEFRAME}?${params.toString()}`,
          { cache: "no-store" },
        );
        const scan = response.ok ? await response.json() : null;
        if (!response.ok || scan?.status !== "OK") {
          throw new Error(scan?.message || "Spike score scan failed");
        }
        const markerCount = rememberSpikeScoreMarkers(scan);
        redrawRefillScanMarkers();
        refillScanStatusEl.textContent = `Spike ${markerCount}`;
        refillScanStatusEl.title = `Processed events: ${Number(scan?.processed_event_count || 0)}`;
      } catch (error) {
        refillScanStatusEl.textContent = error?.message || "Spike scan failed";
      } finally {
        spikeScoreSubmitButton.disabled = false;
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
        const clientBinTickCount = charts.size
          ? [...charts.values()].find(chart => Number(chart.binTickCount) > 0)?.binTickCount
          : 0;
        if (Number(clientBinTickCount) > 0) {
          params.set("client_bin_tick_count", String(clientBinTickCount));
        }
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
    refillScanForm.addEventListener("submit", runRefillScan);
    spikeScoreSubmitButton.addEventListener("click", runSpikeScoreScan);
    askDeleteSubmitButton.addEventListener("click", () => runDeleteScan("ASK"));
    bidDeleteSubmitButton.addEventListener("click", () => runDeleteScan("BID"));
    clearMarkersButton.addEventListener("click", clearCandleMarkers);
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
=======
    return _HTML_TEMPLATE.replace("__ACTIVE_TIMEFRAME__", normalized_timeframe)


_HTML_PAGE = _html_page(DEFAULT_FOOTPRINT_TIMEFRAME)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
