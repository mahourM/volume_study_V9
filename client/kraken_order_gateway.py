from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

from client.kraken_config import KrakenClientConfig


@dataclass(frozen=True)
class KrakenOrderRequest:
    pair: str
    side: str
    volume: Decimal
    order_type: str = "market"
    client_order_id: str = ""


class KrakenOrderGateway:
    def __init__(self, config: KrakenClientConfig) -> None:
        self.config = config

    def create_signature(self, api_path: str, data: dict[str, str]) -> str:
        encoded_payload = urllib.parse.urlencode(data)
        encoded = (str(data["nonce"]) + encoded_payload).encode("utf-8")
        message = api_path.encode("utf-8") + hashlib.sha256(encoded).digest()
        signature = hmac.new(base64.b64decode(self.config.private_key), message, hashlib.sha512)
        return base64.b64encode(signature.digest()).decode("utf-8")

    def private_request(self, api_path: str, data: dict[str, str]) -> dict[str, Any]:
        if not self.config.live_execution_enabled:
            raise RuntimeError("Kraken live execution is disabled")
        if not self.config.api_key or not self.config.private_key:
            raise RuntimeError("Kraken API credentials are not configured")

        request_data = dict(data)
        request_data.setdefault("nonce", str(int(time.time() * 1000)))
        body = urllib.parse.urlencode(request_data).encode("utf-8")
        request = urllib.request.Request(
            self.config.rest_base_url.rstrip("/") + api_path,
            data=body,
            headers={
                "API-Key": self.config.api_key,
                "API-Sign": self.create_signature(api_path, request_data),
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    def place_market_order(self, order: KrakenOrderRequest) -> dict[str, Any]:
        payload = {
            "ordertype": order.order_type,
            "type": order.side.lower(),
            "volume": str(order.volume),
            "pair": order.pair,
        }
        if order.client_order_id:
            payload["cl_ord_id"] = order.client_order_id
        return self.private_request("/0/private/AddOrder", payload)
