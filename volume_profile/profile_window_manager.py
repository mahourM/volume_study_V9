from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass

from volume_profile.profile_bin_builder import VolumeProfileBinBuilder
from volume_profile.poc_value_area_calculator import calculate_hvn_lvn_levels, calculate_poc_vah_val
from backfill.binance_aggtrades_backfill_loader import BinanceAggTradesBackfillLoader

INTERVAL_SECONDS_BY_KEY = {
    "5M": 5 * 60,
    "15M": 15 * 60,
    "30M": 30 * 60,
    "1H": 60 * 60,
    "2H": 2 * 60 * 60,
    "3H": 3 * 60 * 60,
    "4H": 4 * 60 * 60,
    "8H": 8 * 60 * 60,
    "12H": 12 * 60 * 60,
    "24H": 24 * 60 * 60,
}


@dataclass
class ProfileState:
    profile_id: str
    start_time_utc_ms: int
    end_time_utc_ms: int
    is_live: bool
    bin_builder: VolumeProfileBinBuilder


class VolumeProfileWindowManager:

    def __init__(
        self,
        symbol: str,
        profile_interval_key: str,
        profiles_count: int,
        include_live_profile: bool,
        max_bins_per_profile: int,
        fixed_bin_size: float,
    ) -> None:
        self.symbol = symbol.upper()
        self.profile_interval_key = profile_interval_key.upper()
        self.profiles_count = profiles_count
        self.include_live_profile = include_live_profile
        self.max_bins_per_profile = max_bins_per_profile
        self.fixed_bin_size = fixed_bin_size
        self.interval_seconds = INTERVAL_SECONDS_BY_KEY[self.profile_interval_key]

        self.completed_profiles = deque(maxlen=profiles_count)
        self.live_profile: ProfileState | None = None

        self.backfill_lock = asyncio.Lock()
        self.backfill_in_progress = False
        self.pending_live_trades: list[tuple[float, float, int]] = []

        self.backfill_loaded = False
        self.last_backfill_signature = ""

    def get_now_unix_ms_utc(self) -> int:
        return int(time.time() * 1000)

    def on_trade(
        self,
        trade_price: float,
        trade_quantity: float,
        trade_timestamp_utc: int,
    ) -> None:
        if self.backfill_in_progress:
            self.pending_live_trades.append((trade_price, trade_quantity, trade_timestamp_utc))
            return

        self._roll_profiles_if_needed(trade_timestamp_utc)
        if self.live_profile is None:
            return
        self.live_profile.bin_builder.add_trade(trade_price=trade_price, trade_quantity=trade_quantity)

    async def ensure_backfill_ready(
        self,
        requested_profiles_count: int,
        requested_profile_interval_key: str,
        include_live_profile: bool,
        backfill_loader: BinanceAggTradesBackfillLoader,
    ) -> None:
        requested_profile_interval_key = requested_profile_interval_key.upper()
        if requested_profile_interval_key not in INTERVAL_SECONDS_BY_KEY:
            requested_profile_interval_key = "12H"

        signature = f"{requested_profiles_count}|{requested_profile_interval_key}|{int(include_live_profile)}"
        if self.backfill_loaded and self.last_backfill_signature == signature:
            return

        async with self.backfill_lock:
            if self.backfill_loaded and self.last_backfill_signature == signature:
                return

            self.backfill_in_progress = True
            self.pending_live_trades = []

            self.profiles_count = max(1, requested_profiles_count)
            self.include_live_profile = include_live_profile
            self.profile_interval_key = requested_profile_interval_key
            self.interval_seconds = INTERVAL_SECONDS_BY_KEY[self.profile_interval_key]
            self.completed_profiles = deque(maxlen=self.profiles_count)
            self.live_profile = None

            backfill_snapshot_now_ms = self.get_now_unix_ms_utc()
            interval_ms = self.interval_seconds * 1000
            current_interval_start_ms = (backfill_snapshot_now_ms // interval_ms) * interval_ms
            current_interval_end_ms = current_interval_start_ms + interval_ms

            historical_profile_count = self.profiles_count #- 1 if self.include_live_profile else self.profiles_count
            historical_ranges: list[tuple[int, int]] = []

            historical_end_ms = current_interval_start_ms
            for _ in range(historical_profile_count):
                historical_start_ms = historical_end_ms - interval_ms
                historical_ranges.append((historical_start_ms, historical_end_ms - 1))
                historical_end_ms = historical_start_ms

            historical_ranges.reverse()

            for range_start_ms, range_end_ms in historical_ranges:
                profile_state = self._create_profile(
                    start_time_utc_ms=range_start_ms,
                    end_time_utc_ms=range_end_ms + 1,
                    is_live=False,
                )
                historical_trades = await backfill_loader.load_trades_for_window(
                    start_time_utc_ms=range_start_ms,
                    end_time_utc_ms=range_end_ms,
                )
                for trade_item in historical_trades:
                    profile_state.bin_builder.add_trade(
                        trade_price=trade_item["price"],
                        trade_quantity=trade_item["quantity"],
                    )
                self.completed_profiles.append(profile_state)

            self.live_profile = self._create_profile(
                start_time_utc_ms=current_interval_start_ms,
                end_time_utc_ms=current_interval_end_ms,
                is_live=True,
            )

            if self.include_live_profile:
                live_trades = await backfill_loader.load_trades_for_window(
                    start_time_utc_ms=current_interval_start_ms,
                    end_time_utc_ms=backfill_snapshot_now_ms,
                )
                for trade_item in live_trades:
                    self.live_profile.bin_builder.add_trade(
                        trade_price=trade_item["price"],
                        trade_quantity=trade_item["quantity"],
                    )

            buffered_trades = list(self.pending_live_trades)
            self.pending_live_trades = []
            self.backfill_in_progress = False

            for trade_price, trade_quantity, trade_timestamp_utc in buffered_trades:
                if trade_timestamp_utc > backfill_snapshot_now_ms:
                    self.on_trade(
                        trade_price=trade_price,
                        trade_quantity=trade_quantity,
                        trade_timestamp_utc=trade_timestamp_utc,
                    )

            self.backfill_loaded = True
            self.last_backfill_signature = signature

    def _roll_profiles_if_needed(self, trade_timestamp_utc: int) -> None:
        interval_ms = self.interval_seconds * 1000
        aligned_start = (trade_timestamp_utc // interval_ms) * interval_ms
        aligned_end = aligned_start + interval_ms

        if self.live_profile is None:
            self.live_profile = self._create_profile(aligned_start, aligned_end, True)
            return

        if trade_timestamp_utc < self.live_profile.end_time_utc_ms:
            return

        self.live_profile.is_live = False
        self.completed_profiles.append(self.live_profile)
        self.live_profile = self._create_profile(aligned_start, aligned_end, True)

    def _create_profile(self, start_time_utc_ms: int, end_time_utc_ms: int, is_live: bool) -> ProfileState:
        profile_id = f"{self.symbol}_{self.profile_interval_key}_{start_time_utc_ms}_{end_time_utc_ms}"
        return ProfileState(
            profile_id=profile_id,
            start_time_utc_ms=start_time_utc_ms,
            end_time_utc_ms=end_time_utc_ms,
            is_live=is_live,
            bin_builder=VolumeProfileBinBuilder(fixed_bin_size=self.fixed_bin_size),
        )

    def export_profiles_payload(self) -> list[dict]:
        payload = []

        for profile_state in list(self.completed_profiles):
            payload.append(self._export_single_profile_payload(profile_state))

        if self.include_live_profile and self.live_profile is not None:
            payload.append(self._export_single_profile_payload(self.live_profile))

        max_profiles = self.profiles_count + (1 if self.include_live_profile else 0)

        if len(payload) > max_profiles:
            payload = payload[-max_profiles:]

        return payload

    def _export_single_profile_payload(self, profile_state: ProfileState) -> dict:
        volume_by_bin_index = dict(profile_state.bin_builder.volume_by_bin_index)
        poc_price, vah_price, val_price = calculate_poc_vah_val(
            volume_by_bin_index=volume_by_bin_index,
            fixed_bin_size=self.fixed_bin_size,
        )
        hvn_levels, lvn_levels = calculate_hvn_lvn_levels(
            volume_by_bin_index=volume_by_bin_index,
            fixed_bin_size=self.fixed_bin_size,
        )
        bins_payload = profile_state.bin_builder.export_bins_payload()

        profile_low = min((item["price_low"] for item in bins_payload), default=0.0)
        profile_high = max((item["price_high"] for item in bins_payload), default=0.0)

        return {
            "profile_id": profile_state.profile_id,
            "is_live": profile_state.is_live,
            "start_time_utc": profile_state.start_time_utc_ms,
            "end_time_utc": profile_state.end_time_utc_ms,
            "profile_low": profile_low,
            "profile_high": profile_high,
            "poc": poc_price,
            "vah": vah_price,
            "val": val_price,
            "hvn_levels": hvn_levels,
            "lvn_levels": lvn_levels,
            "bins": bins_payload,
        }