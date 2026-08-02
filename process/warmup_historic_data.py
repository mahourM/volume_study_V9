from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


WARMUP_DATA_TYPES = frozenset(("dom", "l2"))


@dataclass(frozen=True)
class WarmupHistoricFile:
    path: Path
    data_type: str
    provider_symbol: str
    mt5_symbol: str
    market_provider: str = ""
    dataset: str = ""
    schema: str = ""
    timeframe: str = ""
    tick_size: str = ""
    start_utc: str = ""
    end_utc: str = ""
    file_format: str = ""
    source: str = "scan"

    @property
    def symbol_key(self) -> tuple[str, str]:
        return (
            self.provider_symbol.strip().upper(),
            self.mt5_symbol.strip().upper(),
        )

    def matches_symbol(
        self,
        *,
        provider_symbol: str = "",
        mt5_symbol: str = "",
        market_provider: str = "",
    ) -> bool:
        if market_provider and self.market_provider:
            if self.market_provider.strip().upper() != market_provider.strip().upper():
                return False

        provider = provider_symbol.strip().upper()
        mt5 = mt5_symbol.strip().upper()
        own_provider = self.provider_symbol.strip().upper()
        own_mt5 = self.mt5_symbol.strip().upper()
        provider_root = _provider_root(provider)
        own_provider_root = _provider_root(own_provider)

        if provider and (
            provider in {own_provider, own_mt5, own_provider_root}
            or (provider_root and provider_root in {own_provider, own_mt5, own_provider_root})
        ):
            return True
        if mt5 and mt5 in {own_provider, own_mt5, own_provider_root}:
            return True
        return not provider and not mt5

    def to_payload(self, *, root_dir: Path | None = None) -> dict[str, Any]:
        path_text = str(self.path)
        if root_dir is not None:
            try:
                path_text = self.path.resolve().relative_to(root_dir.resolve()).as_posix()
            except ValueError:
                path_text = str(self.path)
        return {
            "path": path_text,
            "data_type": self.data_type,
            "provider_symbol": self.provider_symbol,
            "mt5_symbol": self.mt5_symbol,
            "market_provider": self.market_provider,
            "dataset": self.dataset,
            "schema": self.schema,
            "timeframe": self.timeframe,
            "tick_size": self.tick_size,
            "start_utc": self.start_utc,
            "end_utc": self.end_utc,
            "file_format": self.file_format,
            "source": self.source,
            "exists": self.path.exists(),
        }


class WarmupHistoricCatalog:
    def __init__(
        self,
        *,
        root_dir: Path,
        manifest_name: str = "manifest.json",
        dom_dir_name: str = "dom",
        l2_dir_name: str = "l2",
        cache_dir_name: str = ".cache",
        file_globs: tuple[str, ...] = ("*.dbn", "*.dbn.zst", "*.zip", "*.json", "*.jsonl", "*.csv"),
        default_market_provider: str = "",
        default_dataset: str = "",
        default_dom_schema: str = "mbo",
        default_l2_schema: str = "l2",
        default_timeframe: str = "M1",
        default_tick_size: str = "0.25",
    ) -> None:
        self.root_dir = root_dir
        self.manifest_path = root_dir / manifest_name
        self.dom_dir_name = dom_dir_name
        self.l2_dir_name = l2_dir_name
        self.cache_dir_name = cache_dir_name
        self.file_globs = file_globs
        self.default_market_provider = default_market_provider
        self.default_dataset = default_dataset
        self.default_dom_schema = default_dom_schema
        self.default_l2_schema = default_l2_schema
        self.default_timeframe = default_timeframe
        self.default_tick_size = default_tick_size

    def files(self) -> tuple[WarmupHistoricFile, ...]:
        manifest_files = self._manifest_files()
        manifest_relative_paths = {
            _relative_path(file.path, self.root_dir).lower()
            for file in manifest_files
        }
        scanned_files = tuple(
            file
            for file in self._scanned_files()
            if _relative_path(file.path, self.root_dir).lower()
            not in manifest_relative_paths
        )
        return tuple(sorted((*manifest_files, *scanned_files), key=_file_sort_key))

    def files_for_symbol(
        self,
        *,
        provider_symbol: str = "",
        mt5_symbol: str = "",
        market_provider: str = "",
        data_type: str = "",
    ) -> tuple[WarmupHistoricFile, ...]:
        normalized_type = data_type.strip().lower()
        return tuple(
            file
            for file in self.files()
            if (not normalized_type or file.data_type == normalized_type)
            and file.matches_symbol(
                provider_symbol=provider_symbol,
                mt5_symbol=mt5_symbol,
                market_provider=market_provider,
            )
        )

    def grouped_by_symbol(
        self,
        *,
        data_type: str = "",
    ) -> dict[str, dict[str, list[dict[str, Any]]]]:
        grouped: dict[str, dict[str, list[dict[str, Any]]]] = {}
        normalized_type = data_type.strip().lower()
        for file in self.files():
            if normalized_type and file.data_type != normalized_type:
                continue
            key = file.provider_symbol.strip().upper() or file.mt5_symbol.strip().upper()
            if not key:
                key = "UNKNOWN"
            grouped.setdefault(key, {"dom": [], "l2": []})
            grouped[key].setdefault(file.data_type, []).append(
                file.to_payload(root_dir=self.root_dir)
            )
        return grouped

    def status_payload(self) -> dict[str, Any]:
        files = self.files()
        return {
            "type": "WARMUP_HISTORIC_DATA_STATUS",
            "root_dir": str(self.root_dir),
            "manifest_path": str(self.manifest_path),
            "manifest_exists": self.manifest_path.exists(),
            "file_count": len(files),
            "dom_file_count": sum(1 for file in files if file.data_type == "dom"),
            "l2_file_count": sum(1 for file in files if file.data_type == "l2"),
            "symbols": self.grouped_by_symbol(),
        }

    def _manifest_files(self) -> tuple[WarmupHistoricFile, ...]:
        if not self.manifest_path.exists():
            return ()
        with self.manifest_path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if not isinstance(payload, dict):
            raise ValueError("warmup manifest must be a JSON object")

        symbol_defaults = _manifest_symbol_defaults(payload.get("symbols", {}))
        entries = payload.get("files", ())
        if not isinstance(entries, list):
            raise ValueError("warmup manifest files must be a list")

        files: list[WarmupHistoricFile] = []
        for raw_entry in entries:
            if not isinstance(raw_entry, dict):
                raise ValueError("warmup manifest file entries must be objects")
            files.append(self._manifest_file(raw_entry, symbol_defaults))
        return tuple(files)

    def _manifest_file(
        self,
        entry: dict[str, Any],
        symbol_defaults: dict[str, dict[str, str]],
    ) -> WarmupHistoricFile:
        relative_path = str(entry.get("path", "")).strip()
        if not relative_path:
            raise ValueError("warmup manifest file entry is missing path")
        path = _safe_child_path(self.root_dir, relative_path)

        data_type = str(entry.get("data_type") or _data_type_from_path(path, self)).strip().lower()
        if data_type not in WARMUP_DATA_TYPES:
            raise ValueError(f"unsupported warmup data_type: {data_type}")

        provider_symbol = str(
            entry.get("provider_symbol")
            or _provider_from_path(path, self.root_dir, self.dom_dir_name, self.l2_dir_name)
        ).strip().upper()
        defaults = symbol_defaults.get(provider_symbol, {})
        mt5_symbol = str(entry.get("mt5_symbol") or defaults.get("mt5_symbol") or _provider_root(provider_symbol)).strip().upper()

        return WarmupHistoricFile(
            path=path,
            data_type=data_type,
            provider_symbol=provider_symbol,
            mt5_symbol=mt5_symbol,
            market_provider=str(entry.get("market_provider") or defaults.get("market_provider") or self.default_market_provider),
            dataset=str(entry.get("dataset") or defaults.get("dataset") or self.default_dataset),
            schema=str(
                entry.get("schema")
                or defaults.get("schema")
                or (self.default_dom_schema if data_type == "dom" else self.default_l2_schema)
            ),
            timeframe=str(entry.get("timeframe") or defaults.get("timeframe") or self.default_timeframe).strip().upper(),
            tick_size=str(entry.get("tick_size") or defaults.get("tick_size") or self.default_tick_size),
            start_utc=str(entry.get("start_utc") or ""),
            end_utc=str(entry.get("end_utc") or ""),
            file_format=str(entry.get("file_format") or _file_format(path)),
            source="manifest",
        )

    def _scanned_files(self) -> tuple[WarmupHistoricFile, ...]:
        files: list[WarmupHistoricFile] = []
        for data_type, dir_name in (("dom", self.dom_dir_name), ("l2", self.l2_dir_name)):
            base_dir = self.root_dir / dir_name
            if not base_dir.exists():
                continue
            for path in _iter_matching_files(base_dir, self.file_globs):
                provider_symbol = _provider_from_path(path, self.root_dir, self.dom_dir_name, self.l2_dir_name)
                files.append(
                    WarmupHistoricFile(
                        path=path.resolve(),
                        data_type=data_type,
                        provider_symbol=provider_symbol,
                        mt5_symbol=_provider_root(provider_symbol),
                        market_provider=self.default_market_provider,
                        dataset=self.default_dataset,
                        schema=self.default_dom_schema if data_type == "dom" else self.default_l2_schema,
                        timeframe=self.default_timeframe,
                        tick_size=self.default_tick_size,
                        file_format=_file_format(path),
                        source="scan",
                    )
                )
        return tuple(files)


def _iter_matching_files(base_dir: Path, globs: Iterable[str]) -> tuple[Path, ...]:
    paths: dict[str, Path] = {}
    for pattern in globs:
        for path in base_dir.rglob(pattern):
            if path.is_file():
                paths[str(path.resolve()).lower()] = path
    return tuple(sorted(paths.values(), key=lambda path: str(path).lower()))


def _manifest_symbol_defaults(raw_symbols: Any) -> dict[str, dict[str, str]]:
    if not isinstance(raw_symbols, dict):
        return {}
    defaults: dict[str, dict[str, str]] = {}
    for raw_key, raw_value in raw_symbols.items():
        provider_symbol = str(raw_key or "").strip().upper()
        if not provider_symbol or not isinstance(raw_value, dict):
            continue
        defaults[provider_symbol] = {
            str(key): str(value)
            for key, value in raw_value.items()
            if value is not None
        }
    return defaults


def _safe_child_path(root_dir: Path, relative_path: str) -> Path:
    root = root_dir.resolve()
    path = (root / relative_path).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ValueError(f"warmup path escapes root: {relative_path}") from exc
    return path


def _data_type_from_path(path: Path, catalog: WarmupHistoricCatalog) -> str:
    parts = {part.strip().lower() for part in path.parts}
    if catalog.dom_dir_name.strip().lower() in parts:
        return "dom"
    if catalog.l2_dir_name.strip().lower() in parts:
        return "l2"
    return ""


def _provider_from_path(
    path: Path,
    root_dir: Path,
    dom_dir_name: str,
    l2_dir_name: str,
) -> str:
    try:
        relative_parts = path.resolve().relative_to(root_dir.resolve()).parts
    except ValueError:
        relative_parts = path.parts

    ignored = {
        dom_dir_name.strip().lower(),
        l2_dir_name.strip().lower(),
        ".cache",
    }
    for part in relative_parts[:-1]:
        cleaned = _clean_symbol_token(part)
        if cleaned and part.strip().lower() not in ignored:
            return cleaned

    stem = path.name.upper()
    for separator in ("_", "-", " ", "."):
        stem = stem.replace(separator, " ")
    for token in stem.split():
        cleaned = _clean_symbol_token(token)
        if cleaned:
            return cleaned
    return ""


def _clean_symbol_token(value: str) -> str:
    token = value.strip().upper()
    if not token or token.startswith("."):
        return ""
    if token in {"DOM", "L2", "CACHE", "DATA", "WARMUP", "HISTORIC"}:
        return ""
    allowed = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789.")
    cleaned = "".join(ch for ch in token if ch in allowed)
    if not cleaned or not any(ch.isalpha() for ch in cleaned):
        return ""
    if cleaned in {"DBN", "ZST", "ZIP", "JSON", "JSONL", "CSV"}:
        return ""
    return cleaned


def _provider_root(provider_symbol: str) -> str:
    symbol = provider_symbol.strip().upper()
    if not symbol:
        return ""
    return symbol.split(".", 1)[0]


def _relative_path(path: Path, root_dir: Path) -> str:
    try:
        return path.resolve().relative_to(root_dir.resolve()).as_posix()
    except ValueError:
        return str(path.resolve())


def _file_format(path: Path) -> str:
    name = path.name.lower()
    if name.endswith(".dbn.zst"):
        return "dbn.zst"
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "unknown"


def _file_sort_key(file: WarmupHistoricFile) -> tuple[str, str, str]:
    return (
        file.provider_symbol.strip().upper(),
        file.data_type,
        str(file.path).lower(),
    )
