from __future__ import annotations


def calculate_poc_vah_val(volume_by_bin_index: dict[int, float], fixed_bin_size: float) -> tuple[float, float, float]:
    if not volume_by_bin_index:
        return 0.0, 0.0, 0.0

    total_volume = sum(volume_by_bin_index.values())
    poc_bin_index, _ = max(volume_by_bin_index.items(), key=lambda item: item[1])
    poc_price = poc_bin_index * fixed_bin_size

    target_value_area_volume = total_volume * 0.90
    accumulated_volume = 0.0
    selected_bin_indices: list[int] = []

    sorted_by_volume_desc = sorted(volume_by_bin_index.items(), key=lambda item: item[1], reverse=True)
    for bin_index, volume in sorted_by_volume_desc:
        selected_bin_indices.append(bin_index)
        accumulated_volume += volume
        if accumulated_volume >= target_value_area_volume:
            break

    value_area_low = min(selected_bin_indices) * fixed_bin_size
    value_area_high = (max(selected_bin_indices) * fixed_bin_size) + fixed_bin_size

    return round(poc_price, 8), round(value_area_high, 8), round(value_area_low, 8)


def calculate_hvn_lvn_levels(volume_by_bin_index: dict[int, float], fixed_bin_size: float) -> tuple[list[float], list[float]]:
    if len(volume_by_bin_index) < 3:
        return [], []

    volumes = sorted(volume_by_bin_index.values())
    n = len(volumes)
    hvn_threshold = volumes[int(n * 0.85)]
    lvn_threshold = volumes[int(n * 0.10)]

    if hvn_threshold <= 0.0:
        return [], []

    sorted_items = sorted(volume_by_bin_index.items(), key=lambda item: item[0])
    hvn_levels: list[float] = []
    lvn_levels: list[float] = []

    # HVN: contiguous groups above 70th percentile → peak bin of each group
    hvn_group: list[tuple[int, float]] = []
    for bin_index, volume in sorted_items:
        if volume >= hvn_threshold:
            hvn_group.append((bin_index, volume))
        else:
            if hvn_group:
                peak_index = max(hvn_group, key=lambda x: x[1])[0]
                hvn_levels.append(round(peak_index * fixed_bin_size, 8))
                hvn_group = []
    if hvn_group:
        peak_index = max(hvn_group, key=lambda x: x[1])[0]
        hvn_levels.append(round(peak_index * fixed_bin_size, 8))

    # LVN: contiguous groups below 30th percentile → trough bin of each group
    lvn_group: list[tuple[int, float]] = []
    for bin_index, volume in sorted_items:
        if volume <= lvn_threshold:
            lvn_group.append((bin_index, volume))
        else:
            if lvn_group:
                trough_index = min(lvn_group, key=lambda x: x[1])[0]
                lvn_levels.append(round(trough_index * fixed_bin_size, 8))
                lvn_group = []
    if lvn_group:
        trough_index = min(lvn_group, key=lambda x: x[1])[0]
        lvn_levels.append(round(trough_index * fixed_bin_size, 8))

    return hvn_levels, lvn_levels
