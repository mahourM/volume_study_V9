from __future__ import annotations

<<<<<<< HEAD
from dataclasses import dataclass
import re


PROVIDER_BINANCE = "BINANCE"
PROVIDER_CME_LOCAL_DBN = "CME_LOCAL_DBN"


@dataclass(frozen=True)
class SymbolResolution:
    market_provider: str
    provider_symbol: str
    binance_symbol: str = ""
    dataset: str = ""
    schema: str = ""
    tick_size: str = ""


=======
import re


>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
class BinanceSymbolResolver:
    """Resolves MT5 symbols to Binance spot symbols for V7 startup."""

    _KNOWN_BASE_ASSETS: tuple[str, ...] = (
        "BTC",
        "ETH",
        "BNB",
        "SOL",
        "XRP",
        "DOGE",
        "ADA",
        "AVAX",
        "LINK",
        "LTC",
        "TRX",
        "DOT",
        "MATIC",
        "BCH",
        "ETC",
        "ATOM",
        "XLM",
        "SUI",
        "TON",
        "NEAR",
        "APT",
        "ARB",
        "OP",
        "UNI",
        "PEPE",
        "SHIB",
    )

    _DIRECT_SYMBOL_ALIASES: dict[str, str] = {
        "BTCUSD": "BTCUSDT",
        "BTCUSDT": "BTCUSDT",
        "XBTUSD": "BTCUSDT",
        "XBTUSDT": "BTCUSDT",
        "ETHUSD": "ETHUSDT",
        "ETHUSDT": "ETHUSDT",
    }

    def resolve(self, mt5_symbol: str) -> str | None:
        normalized_symbol = self._normalize_mt5_symbol(mt5_symbol)
        if not normalized_symbol:
            return None

        direct_symbol = self._DIRECT_SYMBOL_ALIASES.get(normalized_symbol)
        if direct_symbol:
            return direct_symbol

        for base_asset in self._KNOWN_BASE_ASSETS:
            if normalized_symbol == base_asset:
                return f"{base_asset}USDT"

            if normalized_symbol.endswith(f"{base_asset}USDT"):
                return f"{base_asset}USDT"

            if normalized_symbol.endswith(f"{base_asset}USD"):
                return f"{base_asset}USDT"

            if f"{base_asset}USDT" in normalized_symbol:
                return f"{base_asset}USDT"

            if f"{base_asset}USD" in normalized_symbol:
                return f"{base_asset}USDT"

        return None

    @staticmethod
    def _normalize_mt5_symbol(mt5_symbol: str) -> str:
        return re.sub(r"[^A-Z0-9]", "", mt5_symbol.upper())
<<<<<<< HEAD


class CmeSymbolResolver:
    """Resolves client symbols to locally available CME DBN symbols."""

    _ALIASES: dict[str, str] = {
        "NQ": "NQ.FUT",
        "NQFUT": "NQ.FUT",
        "NQ.FUT": "NQ.FUT",
    }

    def __init__(
        self,
        *,
        available_symbols: tuple[str, ...] = (),
        dataset: str = "GLBX.MDP3",
        schema: str = "trades",
        tick_sizes: dict[str, str] | None = None,
    ) -> None:
        self.available_symbols = tuple(symbol.upper() for symbol in available_symbols)
        self.dataset = dataset
        self.schema = schema
        self.tick_sizes = {key.upper(): value for key, value in (tick_sizes or {}).items()}

    def resolve(self, mt5_symbol: str) -> SymbolResolution | None:
        normalized_symbol = self._normalize_symbol(mt5_symbol)
        provider_symbol = self._ALIASES.get(normalized_symbol, "")
        if not provider_symbol and normalized_symbol.endswith("FUT"):
            provider_symbol = f"{normalized_symbol[:-3]}.FUT"
        if not provider_symbol:
            return None

        provider_symbol = provider_symbol.upper()
        if self.available_symbols and provider_symbol not in self.available_symbols:
            return None

        return SymbolResolution(
            market_provider=PROVIDER_CME_LOCAL_DBN,
            provider_symbol=provider_symbol,
            dataset=self.dataset,
            schema=self.schema,
            tick_size=self.tick_sizes.get(provider_symbol, ""),
        )

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return re.sub(r"[^A-Z0-9.]", "", symbol.upper())


class MarketSymbolResolver:
    """Routes symbols to the correct market provider without probing unused providers."""

    def __init__(
        self,
        *,
        binance_resolver: BinanceSymbolResolver | None = None,
        cme_resolver: CmeSymbolResolver | None = None,
    ) -> None:
        self.binance_resolver = binance_resolver or BinanceSymbolResolver()
        self.cme_resolver = cme_resolver

    def resolve(self, mt5_symbol: str) -> SymbolResolution | None:
        binance_symbol = self.binance_resolver.resolve(mt5_symbol)
        if binance_symbol:
            return SymbolResolution(
                market_provider=PROVIDER_BINANCE,
                provider_symbol=binance_symbol,
                binance_symbol=binance_symbol,
            )

        if self.cme_resolver is not None:
            cme_resolution = self.cme_resolver.resolve(mt5_symbol)
            if cme_resolution is not None:
                return cme_resolution

        return None
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
