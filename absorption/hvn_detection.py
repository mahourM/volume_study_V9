from __future__ import annotations

from statistics import median

from absorption.models import FootprintBin, HvnResult


def detect_hvns(bins: tuple[FootprintBin, ...]) -> HvnResult:
    non_zero_bins = tuple(item for item in bins if item.total_volume > 0)
    if not non_zero_bins:
        return HvnResult(hvn_bins=(), core_bin=None, side_bins=(), full_range=None)

    median_vol = median(item.total_volume for item in non_zero_bins)
    hvn_bins = tuple(item for item in non_zero_bins if item.total_volume >= median_vol * 2)
    if not hvn_bins:
        return HvnResult(hvn_bins=(), core_bin=None, side_bins=(), full_range=None)

    core_bin = max(hvn_bins, key=lambda item: item.total_volume)
    side_bins = tuple(item for item in hvn_bins if item is not core_bin)
    full_range = (
        min(item.bin_low for item in hvn_bins),
        max(item.bin_high for item in hvn_bins),
    )
    return HvnResult(
        hvn_bins=hvn_bins,
        core_bin=core_bin,
        side_bins=side_bins,
        full_range=full_range,
    )
