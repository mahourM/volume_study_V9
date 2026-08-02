from __future__ import annotations

from typing import Any

import orjson


def loads_message(data: bytes | str) -> dict[str, Any]:
    if isinstance(data, bytes):
        return orjson.loads(data)
    return orjson.loads(data.encode("utf-8"))


def dumps_message(payload: dict[str, Any]) -> bytes:
    return orjson.dumps(payload) + b"\n"
