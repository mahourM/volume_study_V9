import time
import json
import hmac
import hashlib
import base64
import urllib.parse
import requests

API_KEY = "YOUR_API_KEY"
API_SECRET = "YOUR_PRIVATE_KEY"

BASE_URL = "https://api.kraken.com"
API_PATH = "/0/private/AddOrder"

def create_signature(urlpath, data, secret):
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode()

    message = urlpath.encode() + hashlib.sha256(encoded).digest()

    mac = hmac.new(
        base64.b64decode(secret),
        message,
        hashlib.sha512
    )

    sigdigest = base64.b64encode(mac.digest())
    return sigdigest.decode()

def place_market_buy():
    nonce = str(int(time.time() * 1000))

    data = {
        "nonce": nonce,
        "ordertype": "market",
        "type": "buy",
        "volume": "0.001",
        "pair": "XBTUSD"
    }

    headers = {
        "API-Key": API_KEY,
        "API-Sign": create_signature(API_PATH, data, API_SECRET)
    }

    response = requests.post(
        BASE_URL + API_PATH,
        headers=headers,
        data=data,
        timeout=10
    )

    print(json.dumps(response.json(), indent=2))

place_market_buy()