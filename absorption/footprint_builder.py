from __future__ import annotations

from decimal import Decimal, ROUND_FLOOR
from typing import Any

from absorption.hvn_detection import detect_hvns
from absorption.models import BinanceCandle, CandleFootprint, FootprintBin


def build_candle_footprint(
    *,
    candle: BinanceCandle,
    mt5_symbol: str,
    timeframe: str,
    bin_size: Decimal,
    agg_trades: list[dict[str, Any]],
    bin_duration_ms_by_index: dict[int, int] | None = None,
) -> CandleFootprint:
    bins_by_index: dict[int, FootprintBin] = {}

    for trade in agg_trades:
        price = Decimal(str(trade.price))
        quantity = Decimal(str(trade.quantity))
        is_sell = trade.side == "sell"
        bin_index = int(((price - candle.low_price) / bin_size).to_integral_value(rounding=ROUND_FLOOR))
        if bin_index < 0:
            bin_index = 0

        footprint_bin = bins_by_index.get(bin_index)
        if footprint_bin is None:
            bin_low = candle.low_price + (bin_size * bin_index)
<<<<<<< HEAD
            footprint_bin = FootprintBin(bin_low=bin_low, bin_high=bin_low + bin_size, bin_index=bin_index)
=======
            footprint_bin = FootprintBin(bin_low=bin_low, bin_high=bin_low + bin_size)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            bins_by_index[bin_index] = footprint_bin

        footprint_bin.min_trade_price_in_bin = (
            price
            if footprint_bin.min_trade_price_in_bin is None
            else min(footprint_bin.min_trade_price_in_bin, price)
        )
        footprint_bin.max_trade_price_in_bin = (
            price
            if footprint_bin.max_trade_price_in_bin is None
            else max(footprint_bin.max_trade_price_in_bin, price)
        )
        if (
            footprint_bin.min_trade_price_in_bin is not None
            and footprint_bin.max_trade_price_in_bin is not None
        ):
            footprint_bin.price_progress_in_bin = (
                footprint_bin.max_trade_price_in_bin
                - footprint_bin.min_trade_price_in_bin
            )

        if is_sell:
            footprint_bin.sell_volume += quantity
        else:
            footprint_bin.buy_volume += quantity

    for bin_index, duration_ms in (bin_duration_ms_by_index or {}).items():
        footprint_bin = bins_by_index.get(bin_index)
        if footprint_bin is not None:
            footprint_bin.duration_ms = int(duration_ms)

    bins = tuple(item for _, item in sorted(bins_by_index.items()))
    hvn_result = detect_hvns(bins)
    return CandleFootprint(
        symbol=candle.symbol,
        mt5_symbol=mt5_symbol,
        timeframe=timeframe,
        interval=candle.interval,
        open_time_ms=candle.open_time_ms,
        close_time_ms=candle.close_time_ms,
        open_price=candle.open_price,
        high_price=candle.high_price,
        low_price=candle.low_price,
        close_price=candle.close_price,
        bin_size=bin_size,
        bins=bins,
        hvn_result=hvn_result,
    )
