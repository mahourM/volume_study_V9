from __future__ import annotations


class VolumeProfileBinBuilder:

    def __init__(self, fixed_bin_size: float) -> None:
        self.fixed_bin_size = fixed_bin_size
        self.volume_by_bin_index: dict[int, float] = {}

    def add_trade(self, trade_price: float, trade_quantity: float) -> None:
        bin_index = int(trade_price // self.fixed_bin_size)
        self.volume_by_bin_index[bin_index] = self.volume_by_bin_index.get(bin_index, 0.0) + trade_quantity

    def export_bins_payload(self) -> list[dict]:
        if not self.volume_by_bin_index:
            return []

        maximum_volume = max(self.volume_by_bin_index.values())
        payload = []

        for bin_index in sorted(self.volume_by_bin_index.keys()):
            volume = self.volume_by_bin_index[bin_index]
            price_low = bin_index * self.fixed_bin_size
            price_high = price_low + self.fixed_bin_size

            payload.append(
                {
                    "price_low": round(price_low, 8),
                    "price_high": round(price_high, 8),
                    "volume": round(volume, 8),
                    "normalized_volume": round(volume / maximum_volume, 8) if maximum_volume > 0.0 else 0.0,
                }
            )

        return payload
