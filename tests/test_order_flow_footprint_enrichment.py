from __future__ import annotations

import unittest
<<<<<<< HEAD
import asyncio
import time
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from decimal import Decimal

from absorption.binance_aggtrade_ws_client import AggTradeEvent
from absorption.binance_kline_ws_client import KLINE_INTERVAL_BY_INTERNAL
from absorption.html_server import (
<<<<<<< HEAD
    _candles_html_page,
    _HTML_PAGE,
    FOOTPRINT_TIMEFRAMES,
    _filter_snapshot_payload_after_open_time,
    _filter_snapshot_payload_known_candles,
    _filter_snapshot_payload_timeframe,
    _html_page,
    _bin_tick_count_from_query,
    _candle_limit_from_query,
    _include_profiles_from_query,
    _timeframe_for_candles_data_path,
    _timeframe_for_candles_path,
    _timeframe_for_data_path,
    _timeframe_for_path,
    _window_end_time_from_query,
)
from DOM.html import (
    DOM_HTML_PAGE,
    dom_html_page,
    dom_timeframe_for_data_path,
    dom_timeframe_for_path,
=======
    _HTML_PAGE,
    FOOTPRINT_TIMEFRAMES,
    _filter_snapshot_payload_timeframe,
    _html_page,
    _timeframe_for_data_path,
    _timeframe_for_path,
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
)
from absorption.session_service import (
    AbsorptionFootprintService,
    STUDY_DISPLAY_CANDLE_LIMITS,
    STUDY_DISPLAY_TIMEFRAMES,
    study_display_candle_limit,
)
from absorption.raw_event_buffers import RawMarketEventBuffer
from absorption_module.absorption_cluster_model import BinMarketData
from config.config_runtime import RuntimeConfig
from core.bin_alignment import ExchangeMetadata
<<<<<<< HEAD
from core.contract_spike import CONTRACT_SPIKE_THRESHOLD
from core.feature_calculation import OutputPrecision
from core.symbol_resolver import PROVIDER_CME_LOCAL_DBN
from core.system_models import SymbolSessionState
from core.trade_mapping import L2BinState, TradeEvent, calculate_diagonal_imbalance_ratios
from cme_provider.engines import _dom_positive_refill_marker
=======
from core.feature_calculation import OutputPrecision
from core.system_models import SymbolSessionState
from core.trade_mapping import L2BinState, TradeEvent, calculate_diagonal_imbalance_ratios
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
from study.candle_builder import OrderFlowCandleBuilder, OrderFlowStudyConfig


def trade(*, price: str, quantity: str, side: str, event_time_ms: int = 1) -> TradeEvent:
    return TradeEvent.from_values(
        symbol="BTCUSDT",
        event_time_ms=event_time_ms,
        price=price,
        quantity=quantity,
        side=side,  # type: ignore[arg-type]
    )


class FootprintEnrichmentTests(unittest.TestCase):
<<<<<<< HEAD
    def test_dom_positive_refill_marker_preserves_data_process_l2_fields(self) -> None:
        marker = _dom_positive_refill_marker(
            {
                "output_id": "OUT-1",
                "timestamp_ms": 1_000,
                "provider_symbol": "NQ.FUT",
                "mt5_symbol": "NQ",
                "timeframe": "M1",
                "price": "28969.5",
                "side": "BID",
                "refill_count": 7,
                "refill_contracts": 9,
                "market_buy": 0,
                "market_sell": 20,
                "market_buy_contracts": 0,
                "market_sell_contracts": 20,
                "ask_traded_contracts": 0,
                "bid_traded_contracts": 20,
                "footprint_open_time_ms": 900,
                "footprint_bin_low": "28969.500",
                "footprint_bin_high": "28969.750",
            }
        )

        self.assertEqual(marker["market_buy"], 0)
        self.assertEqual(marker["market_sell"], 20)
        self.assertEqual(marker["market_sell_contracts"], 20)
        self.assertEqual(marker["bid_traded_contracts"], 20)
        self.assertEqual(marker["positive_refill_total"], 9)
        self.assertEqual(marker["positive_refill_filled_total"], 9)
        self.assertEqual(marker["refill_filled_contracts"], 9)
        self.assertEqual(marker["footprint_open_time_ms"], 900)
        self.assertEqual(marker["footprint_bin_low"], "28969.500")
        self.assertEqual(marker["footprint_bin_high"], "28969.750")

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def test_apply_trade_accumulates_direct_buy_sell_volume(self) -> None:
        state = L2BinState()

        state.apply_trade(trade(price="1.20", quantity="12.5", side="buy"))
        state.apply_trade(trade(price="1.80", quantity="4.25", side="sell"))
        state.buy_diagonal_imbalance_ratio = Decimal("0.5")
        state.sell_diagonal_imbalance_ratio = Decimal("4")

        self.assertEqual(state.ask_traded_volume, Decimal("12.5"))
        self.assertEqual(state.bid_traded_volume, Decimal("4.25"))
        self.assertEqual(state.total_volume, Decimal("16.75"))
        self.assertEqual(state.delta, Decimal("8.25"))
        self.assertEqual(state.horizontal_delta, Decimal("8.25"))
        self.assertEqual(state.min_trade_price_in_bin, Decimal("1.20"))
        self.assertEqual(state.max_trade_price_in_bin, Decimal("1.80"))
        self.assertEqual(state.price_progress_in_bin, Decimal("0.60"))
        self.assertEqual(state.dominant_diagonal_side, "SELL")
        self.assertEqual(state.dominant_side_volume, Decimal("4.25"))
<<<<<<< HEAD
        self.assertEqual(state.dominant_side_efficiency, Decimal("0.60") / Decimal("16.75"))

    def test_diagonal_ratios_use_adjacent_price_bins(self) -> None:
        bins = {0: L2BinState(), 1: L2BinState(), 2: L2BinState()}
        bins[0].apply_trade(trade(price="0.5", quantity="4", side="sell"))
        bins[1].apply_trade(trade(price="1.5", quantity="12", side="buy"))
        bins[1].apply_trade(trade(price="1.5", quantity="3", side="sell"))
        bins[2].apply_trade(trade(price="2.5", quantity="5", side="buy"))
=======
        self.assertEqual(state.dominant_side_efficiency, Decimal("0.60") / Decimal("4.25"))

    def test_diagonal_ratios_use_adjacent_price_bins(self) -> None:
        bins = {0: L2BinState(), 1: L2BinState(), 2: L2BinState()}
        bins[0].apply_trade(trade(price="0.5", quantity="5", side="buy"))
        bins[1].apply_trade(trade(price="1.5", quantity="12", side="buy"))
        bins[1].apply_trade(trade(price="1.5", quantity="3", side="sell"))
        bins[2].apply_trade(trade(price="2.5", quantity="4", side="sell"))
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

        calculate_diagonal_imbalance_ratios(bins)

        self.assertEqual(bins[1].buy_diagonal_imbalance_ratio, Decimal("3"))
        self.assertEqual(bins[1].sell_diagonal_imbalance_ratio, Decimal("0.6"))

<<<<<<< HEAD
    def test_diagonal_ratios_use_one_for_missing_or_zero_denominator(self) -> None:
=======
    def test_diagonal_ratios_are_zero_for_missing_or_zero_denominator(self) -> None:
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        bins = {0: L2BinState(), 1: L2BinState(), 2: L2BinState()}
        bins[1].apply_trade(trade(price="1.5", quantity="7", side="buy"))

        calculate_diagonal_imbalance_ratios(bins)

<<<<<<< HEAD
        self.assertEqual(bins[1].buy_diagonal_imbalance_ratio, Decimal("7"))
        self.assertEqual(bins[1].sell_diagonal_imbalance_ratio, Decimal("0"))
        self.assertEqual(bins[0].bid_traded_volume, Decimal("0"))
        self.assertEqual(bins[2].ask_traded_volume, Decimal("0"))
=======
        self.assertEqual(bins[1].buy_diagonal_imbalance_ratio, Decimal("0"))
        self.assertEqual(bins[1].sell_diagonal_imbalance_ratio, Decimal("0"))
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def test_active_snapshot_payload_contains_precomputed_ratios(self) -> None:
        builder = OrderFlowCandleBuilder(
            OrderFlowStudyConfig(
                symbol="BTCUSDT",
                timeframe="M1",
                timeframe_ms=60_000,
                study_candle_count=5,
                fixed_bin_size=Decimal("1"),
                exchange_metadata=ExchangeMetadata.from_values(
                    symbol="BTCUSDT",
                    tick_size="0.01",
                    step_size="0.001",
                ),
                output_precision=OutputPrecision(decimal_places=3, duration_unit_ms=1000),
            )
        )
        for item in (
            trade(price="0.5", quantity="5", side="buy", event_time_ms=1_000),
            trade(price="1.5", quantity="12", side="buy", event_time_ms=2_000),
            trade(price="1.5", quantity="3", side="sell", event_time_ms=3_000),
            trade(price="2.5", quantity="4", side="sell", event_time_ms=4_000),
        ):
            builder.on_trade(item)

        payload = builder.snapshot(now_ms=5_000).to_payload()
        middle_bin = next(item for item in payload["candles"][0]["bins"] if item["index"] == 1)

        self.assertEqual(middle_bin["l2"]["ask_traded_volume"], "12.000")
        self.assertEqual(middle_bin["l2"]["bid_traded_volume"], "3.000")
<<<<<<< HEAD
        self.assertEqual(middle_bin["l2"]["buy_diagonal_imbalance_ratio"], "12.000")
        self.assertEqual(middle_bin["l2"]["sell_diagonal_imbalance_ratio"], "3.000")
=======
        self.assertEqual(middle_bin["l2"]["buy_diagonal_imbalance_ratio"], "3.000")
        self.assertEqual(middle_bin["l2"]["sell_diagonal_imbalance_ratio"], "0.600")
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.assertEqual(middle_bin["l2"]["min_trade_price_in_bin"], "1.500")
        self.assertEqual(middle_bin["l2"]["max_trade_price_in_bin"], "1.500")
        self.assertEqual(middle_bin["l2"]["price_progress_in_bin"], "0.000")
        self.assertEqual(middle_bin["l2"]["dominant_diagonal_side"], "BUY")
        self.assertEqual(middle_bin["l2"]["dominant_side_volume"], "12.000")
        self.assertEqual(middle_bin["l2"]["dominant_side_efficiency"], "0.000")

    def test_footprint_payload_propagates_direct_buy_sell_volume(self) -> None:
        item = BinMarketData(
            symbol="BTCUSD",
            timeframe_name="M15",
            candle_open_time_utc_ms=1,
            candle_close_time_utc_ms=2,
            bin_index=42,
            price_low=42.0,
            price_high=43.0,
            price_progress=0.0,
            total_volume=79.0,
            delta_volume=6.432,
            time_in_bin_ms=23_261,
            horizontal_delta=6.432,
            ask_traded_volume=42.715,
            bid_traded_volume=36.283,
            buy_diagonal_imbalance_ratio=3.6,
            sell_diagonal_imbalance_ratio=0.85,
            min_trade_price_in_bin=42.125,
            max_trade_price_in_bin=42.875,
            price_progress_in_bin=0.75,
            dominant_diagonal_side="BUY",
            dominant_side_volume=42.715,
            dominant_side_efficiency=0.017558234812127,
        )

        payload = AbsorptionFootprintService._canonical_item_to_footprint_bin(
            item,
            Decimal("1"),
        ).to_payload()

<<<<<<< HEAD
        self.assertEqual(payload["index"], 42)
        self.assertEqual(payload["price"], "42")
        self.assertNotIn("mid_price", payload)
        self.assertNotIn("bin_low", payload)
        self.assertNotIn("bin_high", payload)
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.assertEqual(payload["ask_traded_volume"], "42.715")
        self.assertEqual(payload["bid_traded_volume"], "36.283")
        self.assertEqual(payload["buy_diagonal_imbalance_ratio"], "3.6")
        self.assertEqual(payload["sell_diagonal_imbalance_ratio"], "0.85")
        self.assertEqual(payload["min_trade_price_in_bin"], "42.125")
        self.assertEqual(payload["max_trade_price_in_bin"], "42.875")
        self.assertEqual(payload["price_progress_in_bin"], "0.75")
        self.assertEqual(payload["dominant_diagonal_side"], "BUY")
        self.assertEqual(payload["dominant_side_volume"], "42.715")
        self.assertEqual(payload["dominant_side_efficiency"], "0.017558234812127")

<<<<<<< HEAD
    def test_ui_uses_contract_quantities_for_cme_and_volume_for_binance(self) -> None:
        self.assertIn("class CanvasFootprintChart", _HTML_PAGE)
        self.assertIn('class="history-scrollbar"', _HTML_PAGE)
        self.assertIn('role="scrollbar"', _HTML_PAGE)
        self.assertIn('class="history-scrollbar-content"', _HTML_PAGE)
        self.assertIn('this.scrollbar.addEventListener("scroll"', _HTML_PAGE)
        self.assertNotIn('type="range"', _HTML_PAGE)
        self.assertIn('event.key === "ArrowLeft" ? -3 : 3', _HTML_PAGE)
        self.assertIn("const candleSteps = this.scrollByPixels(primaryDelta);", _HTML_PAGE)
        self.assertIn("this.horizontalOffsetPx -= scaledDelta;", _HTML_PAGE)
        self.assertIn("FOOTPRINT_FETCH_OVERSCAN = 4", _HTML_PAGE)
        self.assertIn("const width = Math.max(1, Math.floor(rect.width));", _HTML_PAGE)
        self.assertIn("function binBuy(bin)", _HTML_PAGE)
        self.assertIn("function binSell(bin)", _HTML_PAGE)
        self.assertIn("function binTotal(bin)", _HTML_PAGE)
        self.assertIn("function binDelta(bin)", _HTML_PAGE)
        self.assertIn("function binBuyPressure(bin)", _HTML_PAGE)
        self.assertIn("function binSellPressure(bin)", _HTML_PAGE)
        self.assertIn("function binContractSpikeScore(bin)", _HTML_PAGE)
        self.assertIn("function binAbnormalContract(bin)", _HTML_PAGE)
        self.assertIn("function binDominantSide(bin)", _HTML_PAGE)
        self.assertIn("function binEfficiency(bin)", _HTML_PAGE)
        self.assertIn("function binBuyDirectionalEfficiency(candle, bin, size, tickSize)", _HTML_PAGE)
        self.assertIn("function binSellDirectionalEfficiency(candle, bin, size, tickSize)", _HTML_PAGE)
        self.assertIn("function normalizedTickIndex(price, tickSize)", _HTML_PAGE)
        self.assertIn("return Math.round(price / tickSize);", _HTML_PAGE)
        self.assertIn("function binAtPrice(candle, price, size)", _HTML_PAGE)
        self.assertIn("function drawBinQuantityPair(ctx, bin, leftX, rightX, y, rowHeight = 12)", _HTML_PAGE)
        self.assertIn('binPayloadField(bin, "total_contracts")', _HTML_PAGE)
        self.assertIn('binPayloadField(bin, "buy_contracts")', _HTML_PAGE)
        self.assertIn('binPayloadField(bin, "sell_contracts")', _HTML_PAGE)
        self.assertIn('binPayloadField(bin, "contract_delta")', _HTML_PAGE)
        self.assertIn("const total = binTotal(bin);", _HTML_PAGE)
        self.assertIn("const delta = binDelta(bin);", _HTML_PAGE)
        self.assertIn("const buyColumnX = leftX + width * 0.27;", _HTML_PAGE)
        self.assertIn("const sellColumnX = leftX + width * 0.73;", _HTML_PAGE)
        self.assertIn('ctx.textAlign = "center";', _HTML_PAGE)
        self.assertIn("ctx.fillText(fmt(binBuy(bin), 0), buyColumnX, y);", _HTML_PAGE)
        self.assertIn("ctx.fillText(fmt(binSell(bin), 0), sellColumnX, y);", _HTML_PAGE)
        self.assertIn("ctx.fillRect(innerX + innerW / 2 - 5, top, 10, h);", _HTML_PAGE)
        self.assertIn("drawBinQuantityPair(ctx, bin, innerX, innerX + innerW, centerY, h);", _HTML_PAGE)
        self.assertNotIn("ctx.fillText(`${fmt(total, 0)} ${signed(delta, 0)}`", _HTML_PAGE)
        self.assertNotIn("ctx.fillText(`${dominantSide} E:${fmtMaybe(efficiency, 3)}`", _HTML_PAGE)
        self.assertNotIn("ctx.fillText(`D ${fmtMaybe(binBuyPressure(bin), 1)}/${fmtMaybe(binSellPressure(bin), 1)}`", _HTML_PAGE)
        self.assertIn('const usesContracts = this.quantityUnit === "CONTRACTS";', _HTML_PAGE)
        self.assertIn("Contracts ${fmt(total, 0)} | Contract delta ${signed(delta, 0)}", _HTML_PAGE)
        self.assertIn("Total contracts ${fmt(binTotal(hoveredBin), 0)} | Contract delta ${signed(binDelta(hoveredBin), 0)}", _HTML_PAGE)
        self.assertIn("Diagonal contracts B delta ${fmtMaybe(binBuyDiagonalDelta(hoveredBin), 0)} / S delta ${fmtMaybe(binSellDiagonalDelta(hoveredBin), 0)}", _HTML_PAGE)
        self.assertIn("Dom contracts ${fmtMaybe(binDominantQuantity(hoveredBin), 0)}", _HTML_PAGE)
        self.assertIn("Volume ${fmt(total, 2)} | Delta ${signed(delta, 2)}", _HTML_PAGE)
        self.assertIn("TotalVolume ${fmt(binTotal(hoveredBin), 0)} | Delta ${signed(binDelta(hoveredBin), 0)}", _HTML_PAGE)
        self.assertIn("Diagonal pressure B ${fmtMaybe(binBuyPressure(hoveredBin), 3)} / S ${fmtMaybe(binSellPressure(hoveredBin), 3)}", _HTML_PAGE)
        self.assertIn("Dominance ${binDominantSide(hoveredBin)} | DomVol ${fmtMaybe(binDominantQuantity(hoveredBin), 2)}", _HTML_PAGE)
        self.assertIn("Buy Dir Eff: ${fmtMaybe(binBuyDirectionalEfficiency(candle, hoveredBin, this.fixedSize, this.priceStep), 2)}", _HTML_PAGE)
        self.assertIn("Sell Dir Eff: ${fmtMaybe(binSellDirectionalEfficiency(candle, hoveredBin, this.fixedSize, this.priceStep), 2)}", _HTML_PAGE)
        self.assertIn(
            "Contract spike score ${fmtMaybe(binContractSpikeScore(hoveredBin), 3)}",
            _HTML_PAGE,
        )
        self.assertNotIn("Abnormal contract ${binAbnormalContract(hoveredBin)}", _HTML_PAGE)
        self.assertNotIn("Eff pct ${fmtMaybe(binEfficiencyPercentile(hoveredBin), 2)}", _HTML_PAGE)
        self.assertNotIn("| Eff ${fmtMaybe(binEfficiency(hoveredBin), 6)}", _HTML_PAGE)
        self.assertNotIn("Median bin contracts ${fmt(binMedianVolume(hoveredBin), 1)}", _HTML_PAGE)
        self.assertNotIn('class="footprint-main"', _HTML_PAGE)
        self.assertNotIn('class="footprint-extra"', _HTML_PAGE)

    def test_footprint_directional_efficiency_tooltip_uses_nearest_opposite_contract_tick(self) -> None:
        self.assertIn('if (!(currentContracts > 0)) return NaN;', _HTML_PAGE)
        self.assertIn("const currentTick = binTickIndex(bin, size, tickSize);", _HTML_PAGE)
        self.assertIn("candidateTick < currentTick", _HTML_PAGE)
        self.assertIn("&& binSell(candidate) > 0", _HTML_PAGE)
        self.assertIn("candidateTick > currentTick", _HTML_PAGE)
        self.assertIn("&& binBuy(candidate) > 0", _HTML_PAGE)
        self.assertIn('const fallbackPrice = side === "BUY"', _HTML_PAGE)
        self.assertIn('? maybeNum(ohlc(candle, "low"))', _HTML_PAGE)
        self.assertIn(': maybeNum(ohlc(candle, "high"));', _HTML_PAGE)
        self.assertIn("referenceTick = normalizedTickIndex(fallbackPrice, tickSize);", _HTML_PAGE)
        self.assertIn("const distanceTicks = Math.abs(currentTick - referenceTick);", _HTML_PAGE)
        self.assertIn("return distanceTicks / currentContracts;", _HTML_PAGE)
        self.assertNotIn("binIsEmpty(candidate)", _HTML_PAGE)
        self.assertNotIn('binPayloadField(bin, "directional_efficiency")', _HTML_PAGE)

    def test_candle_page_has_history_scrollbar_and_three_candle_keyboard_navigation(self) -> None:
        page = _candles_html_page("M15")

        self.assertIn('class="history-scrollbar"', page)
        self.assertIn('role="scrollbar"', page)
        self.assertIn('class="history-scrollbar-content"', page)
        self.assertIn('this.scrollbar.addEventListener("scroll"', page)
        self.assertNotIn('type="range"', page)
        self.assertIn("scheduledViewportRequest", page)
        self.assertIn("activeViewportRequest", page)
        self.assertIn("new AbortController()", page)
        self.assertIn("requestId !== latestViewportRequestId", page)
        self.assertIn("function navigateCandlesByCount(", page)
        self.assertIn('event.key === "ArrowLeft" ? -3 : 3', page)
        self.assertIn("navigateCandlesByCount(", page)
        self.assertIn("this.visibleCapacity(),\n                false,", page)
        self.assertIn("this.horizontalOffsetPx = 0;", page)
        self.assertIn("const candleSteps = this.scrollByPixels(primaryDelta);", page)
        self.assertIn("this.horizontalOffsetPx -= scaledDelta;", page)
        self.assertIn("resetAutoScale()", page)
        self.assertIn("this.resetAutoScale();", page)
        self.assertNotIn("this.resetAutoScale();\n          requestViewportWindow(selectedStartMs", page)
        self.assertNotIn("this.resetAutoScale();\n            const primaryDelta = normalizedWheelDelta(event, rect.width);", page)
        self.assertIn("if (event.clientX - rect.left < rect.width - 82) return;\n          this.resetAutoScale();", page)
        self.assertNotIn("lockCurrentVisualRange()", page)
        self.assertNotIn("this.lockCurrentVisualRange();", page)
        self.assertIn("event.altKey && this.lastVisualRange", page)
        self.assertNotIn("event.altKey || overPriceAxis", page)
        self.assertIn("visibleCandleItems(layout)", page)
        self.assertIn("const nextIsAdjacent = (", page)
        self.assertIn("CANDLE_FETCH_OVERSCAN = 96", page)
        self.assertIn("function domRefillMarkerPrice(marker)", page)
        self.assertIn("includePrice(domRefillMarkerPrice(marker));", page)
        self.assertIn("const price = domRefillMarkerPrice(marker);", page)
        self.assertIn('params.set("client_bin_tick_count", String(clientBinTickCount));', page)
        self.assertIn("function schedulePendingViewportRequest(", page)
        self.assertIn("if (scheduledViewportRequest)", page)
        self.assertIn("function cacheSnapshot(snapshot)", page)
        self.assertIn("cacheSnapshot(snapshot);", page)
        self.assertIn("currentViewportEndTimeMs()", page)
        self.assertIn("canPreviewViewport(endTimeMs", page)
        self.assertIn("this.cachedWindows = [];", page)
        self.assertIn("rememberCachedWindow(session)", page)
        self.assertIn(
            "requestViewportWindow(\n"
            "                this.currentViewportEndTimeMs(),",
            page,
        )
        self.assertIn("if (endTimeMs) requestedWindowEndMs = 0;", page)
        self.assertIn("programmaticScrollbarUntil", page)
        self.assertNotIn(
            "if (viewportRequestTimer) clearTimeout(viewportRequestTimer);",
            page,
        )
        self.assertIn("this.autoScaleEnabled = true;", page)
        self.assertNotIn("this.autoScaleEnabled = Boolean(CANDLE_VISUAL_CONFIG.autoScaleEnabled);", page)
        self.assertIn("this.manualVisualSpan = NaN;", page)
        self.assertNotIn("this.viewportScaleRange", page)

    def test_footprint_scroll_queue_has_no_candle_profile_dependency(self) -> None:
        self.assertNotIn("cancelProfileRefresh();", _HTML_PAGE)
        self.assertNotIn("pendingViewportRequest", _HTML_PAGE)
        self.assertIn("cancelActiveViewportRequest();", _HTML_PAGE)
        self.assertIn("clearTimeout(viewportRequestTimer);", _HTML_PAGE)
        self.assertIn("viewportRequestTimer = setTimeout", _HTML_PAGE)
        self.assertIn("const VIEWPORT_REQUEST_DEBOUNCE_MS = 80;", _HTML_PAGE)
        self.assertIn("}, VIEWPORT_REQUEST_DEBOUNCE_MS);", _HTML_PAGE)
        self.assertIn("requestId !== latestViewportRequestId", _HTML_PAGE)
        self.assertIn("function cacheSnapshot(snapshot)", _HTML_PAGE)
        self.assertIn("cacheSnapshot(snapshot);", _HTML_PAGE)
        self.assertIn("currentViewportEndTimeMs()", _HTML_PAGE)
        self.assertIn("canPreviewViewport(endTimeMs", _HTML_PAGE)
        self.assertIn("this.cachedWindows = [];", _HTML_PAGE)
        self.assertIn("rememberCachedWindow(session)", _HTML_PAGE)
        self.assertIn("this.viewportInitialized = false;", _HTML_PAGE)
        self.assertIn("this.pinnedViewportEndMs = 0;", _HTML_PAGE)
        merge_session = _HTML_PAGE.split(
            "mergeSession(session, displayLimit) {",
            1,
        )[1].split("renderCandles() {", 1)[0]
        self.assertNotIn("this.positionViewport(", merge_session)
        self.assertNotIn("requestedWindowEndMs", merge_session)
        self.assertIn("if (!this.viewportInitialized && all.length) {", merge_session)
        cache_session = _HTML_PAGE.split(
            "cacheSession(session, displayLimit = this.displayLimit) {",
            1,
        )[1].split("mergeSession(session, displayLimit) {", 1)[0]
        self.assertNotIn("this.positionViewport(", cache_session)
        self.assertIn("this.scrollbarPointerActive = false;", _HTML_PAGE)
        self.assertIn("const armScrollbarInput =", _HTML_PAGE)
        self.assertIn(
            "!this.scrollbarPointerActive\n"
            "            && Date.now() > this.scrollbarUserInputUntil",
            _HTML_PAGE,
        )
        self.assertIn(
            "this.pinnedViewportEndMs = (\n"
            "            candleOpen(all[all.length - 1])",
            _HTML_PAGE,
        )
        self.assertIn(
            "requestedWindowEndMs\n"
            "          || this.currentViewportEndTimeMs()",
            _HTML_PAGE,
        )
        self.assertIn(
            "requestViewportWindow(\n"
            "                this.currentViewportEndTimeMs(),",
            _HTML_PAGE,
        )
        self.assertIn("if (endTimeMs) requestedWindowEndMs = 0;", _HTML_PAGE)
        self.assertIn("programmaticScrollbarUntil", _HTML_PAGE)
        visible_items = _HTML_PAGE.split(
            "visibleCandleItems(all, plotW) {",
            1,
        )[1].split("computePriceRange(", 1)[0]
        self.assertNotIn("this.viewEnd =", visible_items)
        self.assertNotIn("this.pinnedViewportEndMs =", visible_items)
        self.assertIn("const coverage = this.cachedWindows.find(", visible_items)
        self.assertIn("chart.positionViewport(normalizedEndTimeMs, normalizedLimit);", _HTML_PAGE)
        self.assertIn("this.section.dataset.viewportEndTimeMs = String(", _HTML_PAGE)

    def test_footprint_delta_table_and_price_crosshair_are_rendered(self) -> None:
        self.assertIn('["Delta", "delta_contracts", 0, "SIGNED"]', _HTML_PAGE)
        self.assertIn('["Body Delta", "body_delta", 0, "SIGNED"]', _HTML_PAGE)
        self.assertIn('["Upper Wick Delta", "upper_wick_delta", 0, "SIGNED"]', _HTML_PAGE)
        self.assertIn('["Lower Wick Delta", "lower_wick_delta", 0, "SIGNED"]', _HTML_PAGE)
        self.assertIn('["Max Buy Ratio", "max_buy_diagonal_ratio", 3, "BUY"]', _HTML_PAGE)
        self.assertIn('["Max Sell Ratio", "max_sell_diagonal_ratio", 3, "SELL"]', _HTML_PAGE)
        self.assertIn('["Sum Buy Ratio", "sum_buy_diagonal_ratio", 3, "BUY"]', _HTML_PAGE)
        self.assertIn('["Sum Sell Ratio", "sum_sell_diagonal_ratio", 3, "SELL"]', _HTML_PAGE)
        self.assertIn('["Max Buy Dir Eff", "max_buy_directional_efficiency", 2, "BUY"]', _HTML_PAGE)
        self.assertIn('["Max Sell Dir Eff", "max_sell_directional_efficiency", 2, "SELL"]', _HTML_PAGE)
        self.assertIn('["Max Spike Score", "max_contract_spike_score", 3, "NEUTRAL"]', _HTML_PAGE)
        self.assertIn('["Spike Score SD", "contract_spike_score_deviation", 3, "NEUTRAL"]', _HTML_PAGE)
        self.assertIn('["NY Session Cum", "session_cumulative_delta", 0, "SIGNED"]', _HTML_PAGE)
        self.assertIn('["Day Cum", "day_cumulative_delta", 0, "SIGNED"]', _HTML_PAGE)
        self.assertNotIn('["NY Session Cum", "cumulative_delta"]', _HTML_PAGE)
        self.assertNotIn('["Day Cum", "cumulative_delta"]', _HTML_PAGE)
        self.assertIn(
            "`150 ${fontSize}px Arial Narrow, Segoe UI, Arial`",
            _HTML_PAGE,
        )
        self.assertEqual(_HTML_PAGE.count("`150 ${fontSize}px Arial Narrow, Segoe UI, Arial`"), 2)
        self.assertIn('const deltaTableHeight = 236;', _HTML_PAGE)
        self.assertIn('"600 22px Arial Narrow, Segoe UI, Arial"', _HTML_PAGE)
        self.assertIn('"600 20px Arial Narrow, Segoe UI, Arial"', _HTML_PAGE)
        self.assertIn('function candleRegionDelta(candle, region, size)', _HTML_PAGE)
        self.assertIn('function candleBinMetric(candle, field, size, tickSize)', _HTML_PAGE)
        self.assertIn('function footprintDeltaTableValue(candle, field, size, tickSize)', _HTML_PAGE)
        self.assertIn('(region === "BODY" && bodyLow <= price && price <= bodyHigh)', _HTML_PAGE)
        self.assertIn('(region === "UPPER_WICK" && bodyHigh < price && price <= high)', _HTML_PAGE)
        self.assertIn('(region === "LOWER_WICK" && low <= price && price < bodyLow)', _HTML_PAGE)
        self.assertNotIn('} else if (field === "contract_spike_score_deviation") {', _HTML_PAGE)
        self.assertIn('const value = candle?.[field];', _HTML_PAGE)
        self.assertIn('} else if (field === "max_contract_spike_score") {', _HTML_PAGE)
        self.assertIn('} else if (field === "max_buy_directional_efficiency") {', _HTML_PAGE)
        self.assertIn('valueForBin = bin => binBuyDirectionalEfficiency(candle, bin, size, tickSize);', _HTML_PAGE)
        self.assertIn('valueForBin = bin => binSellDirectionalEfficiency(candle, bin, size, tickSize);', _HTML_PAGE)
        self.assertIn('|| field === "max_contract_spike_score"', _HTML_PAGE)
        self.assertIn('|| field === "max_buy_directional_efficiency"', _HTML_PAGE)
        self.assertIn('const rawValue = footprintDeltaTableValue(candle, field, this.fixedSize, this.priceStep);', _HTML_PAGE)
        self.assertIn("const hoveredRowIndex = (", _HTML_PAGE)
        self.assertIn('ctx.fillStyle = "rgba(88,166,255,.16)";', _HTML_PAGE)
        self.assertIn("const priceLabelStep = tickSize * 20;", _HTML_PAGE)
        self.assertIn('ctx.font = "600 15px Segoe UI, Arial";', _HTML_PAGE)
        self.assertIn("const boxHeight = 30;", _HTML_PAGE)
        self.assertIn('ctx.font = "150 20px Segoe UI, Arial";', _HTML_PAGE)
        self.assertIn("drawPriceMarker(ctx, plotW, axisWidth", _HTML_PAGE)
        self.assertIn("ctx.lineTo(plotW, markerY);", _HTML_PAGE)
        self.assertIn("binAbnormalContract(bin)", _HTML_PAGE)
        self.assertIn("binAbnormalBuyImbalance(bin)", _HTML_PAGE)
        self.assertIn("binAbnormalSellImbalance(bin)", _HTML_PAGE)
        self.assertIn("rgba(255,45,149,.95)", _HTML_PAGE)
        self.assertIn("rgba(63,185,80,.95)", _HTML_PAGE)
        self.assertIn('ctx.strokeStyle = "#3fb950";', _HTML_PAGE)
        self.assertIn('ctx.strokeStyle = "#f85149";', _HTML_PAGE)
        candle_page = _candles_html_page("M1")
        self.assertIn("function priceForBinIndex(index, size) { return index * size; }", candle_page)
        self.assertIn("const price = binPrice(bin, size);", candle_page)
        self.assertNotIn("binCenterPrice", candle_page)
        self.assertIn('["Delta", "delta_contracts"]', candle_page)
        self.assertIn('["NY Session Cum", "session_cumulative_delta"]', candle_page)
        self.assertIn('["Day Cum", "day_cumulative_delta"]', candle_page)
        self.assertNotIn('["Body Delta", "body_delta"]', candle_page)
        self.assertNotIn('["Upper Wick Delta", "upper_wick_delta"]', candle_page)
        self.assertNotIn('["Lower Wick Delta", "lower_wick_delta"]', candle_page)
        self.assertNotIn('max_buy_diagonal_ratio', candle_page)
        self.assertNotIn('max_sell_diagonal_ratio', candle_page)
        self.assertNotIn('sum_buy_diagonal_ratio', candle_page)
        self.assertNotIn('sum_sell_diagonal_ratio', candle_page)
        self.assertNotIn('contract_spike_score_deviation', candle_page)
        self.assertIn("function candleDeltaValue(candle, field)", candle_page)
        self.assertIn("this.drawDeltaTable(ctx, candleItems, layout, plotH, deltaTableHeight);", candle_page)
        self.assertIn("drawPriceMarker(ctx, plotW, axisWidth", candle_page)
        self.assertIn("ctx.lineTo(layout.contentW, markerY);", candle_page)
        self.assertIn('ctx.font = "150 20px Segoe UI, Arial";', candle_page)
=======
    def test_ui_uses_direct_volumes_and_max_ratios_for_visual_merge(self) -> None:
        self.assertIn("const totalVolume = num(l2.buyVolume) + num(l2.sellVolume);", _HTML_PAGE)
        self.assertIn("${fmt(totalVolume)} - B:${fmt(l2.buyVolume)} / S:${fmt(l2.sellVolume)}", _HTML_PAGE)
        self.assertIn("target.buyVolume += num(buyVolume);", _HTML_PAGE)
        self.assertIn("target.sellVolume += num(sellVolume);", _HTML_PAGE)
        self.assertIn("Math.max(target.buyRatio", _HTML_PAGE)
        self.assertIn("Math.max(target.sellRatio", _HTML_PAGE)
        self.assertIn("target.dominantSideEfficiency = domEfficiency ?? null;", _HTML_PAGE)
        self.assertIn('class="footprint-main"', _HTML_PAGE)
        self.assertIn('class="footprint-extra"', _HTML_PAGE)
        self.assertIn("const extra = `Dom: ${dominantSide(l2.dominantSide)}", _HTML_PAGE)
        self.assertIn("Eff: ${fmtEfficiency(l2.dominantSideEfficiency)}", _HTML_PAGE)
        self.assertIn("PP: ${fmtMaybe(l2.priceProgressInBin)}", _HTML_PAGE)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def test_ui_uses_route_driven_timeframe_pages(self) -> None:
        self.assertEqual(FOOTPRINT_TIMEFRAMES, ("M1", "M5", "M15", "M30", "H1"))
        self.assertEqual(STUDY_DISPLAY_TIMEFRAMES, ("M1", "M5", "M15", "M30", "H1"))
<<<<<<< HEAD
        self.assertEqual(_timeframe_for_path("/"), "M5")
        self.assertEqual(_timeframe_for_path("/footprint/M30"), "M30")
        self.assertEqual(_timeframe_for_path("/footprint/h1"), "H1")
        self.assertIsNone(_timeframe_for_path("/footprint/H4"))
        self.assertEqual(dom_timeframe_for_path("/dom"), "M5")
        self.assertEqual(dom_timeframe_for_path("/dom/M30"), "M30")
        self.assertEqual(dom_timeframe_for_path("/dom/h1"), "H1")
        self.assertIsNone(dom_timeframe_for_path("/dom/H4"))
        self.assertEqual(dom_timeframe_for_data_path("/dom-data/M5"), "M5")
        self.assertIsNone(dom_timeframe_for_data_path("/dom-data/H4"))
        self.assertIn('const ACTIVE_TIMEFRAME = "M5";', _HTML_PAGE)
        self.assertIn("<title>M5</title>", _HTML_PAGE)
        self.assertIn("<h1>M5</h1>", _HTML_PAGE)
        self.assertIn('href="/footprint/${timeframe}"', _HTML_PAGE)
        self.assertIn('href="/dom/M5">DOM Timeline</a>', _html_page("M5"))
        self.assertIn("class CanvasFootprintChart", _HTML_PAGE)
        self.assertIn("function activeTimeframeSessions(snapshot)", _HTML_PAGE)
        self.assertIn("sessionTimeframe(session) === ACTIVE_TIMEFRAME", _HTML_PAGE)
        self.assertIn("`${symbol}|${sessionTimeframe(session)}`", _HTML_PAGE)
        self.assertIn("`/footprint-data/${ACTIVE_TIMEFRAME}${suffix}`", _HTML_PAGE)
        self.assertIn("{ cache: \"no-store\", signal: controller.signal }", _HTML_PAGE)
        self.assertIn("visibleCandleItems(all, plotW)", _HTML_PAGE)
=======
        self.assertEqual(_timeframe_for_path("/"), "M15")
        self.assertEqual(_timeframe_for_path("/footprint/M30"), "M30")
        self.assertEqual(_timeframe_for_path("/footprint/h1"), "H1")
        self.assertIsNone(_timeframe_for_path("/footprint/H4"))
        self.assertIsNone(_timeframe_for_path("/dom"))
        self.assertIsNone(_timeframe_for_path("/dom-data"))
        self.assertIn('const ACTIVE_TIMEFRAME = "M15";', _HTML_PAGE)
        self.assertIn("<title>M15</title>", _HTML_PAGE)
        self.assertIn("<h1>M15</h1>", _HTML_PAGE)
        self.assertIn('href="/footprint/${timeframe}"', _HTML_PAGE)
        self.assertIn("Footprint ${ACTIVE_TIMEFRAME}", _HTML_PAGE)
        self.assertIn("function renderSessionView(session, displayLimit)", _HTML_PAGE)
        self.assertIn("sessionTimeframe(session) === ACTIVE_TIMEFRAME", _HTML_PAGE)
        self.assertIn("`${symbol}|${sessionTimeframe(session)}`", _HTML_PAGE)
        self.assertIn("fetchJson(`/footprint-data/${ACTIVE_TIMEFRAME}`)", _HTML_PAGE)
        self.assertIn("function visibleCandlesForSession(session, displayLimit)", _HTML_PAGE)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.assertIn('const ACTIVE_TIMEFRAME = "M5";', _html_page("M5"))
        self.assertIn("<title>M30</title>", _html_page("M30"))
        self.assertIn("<h1>M30</h1>", _html_page("M30"))
        self.assertNotIn("Unified Order Flow Footprint", _html_page("M30"))
<<<<<<< HEAD
        self.assertIn('const ACTIVE_TIMEFRAME = "M5";', DOM_HTML_PAGE)
        self.assertIn("<title>DOM Timeline M30</title>", dom_html_page("M30"))
        self.assertIn("class CanvasDomTimelineChart", DOM_HTML_PAGE)
        self.assertIn("`/dom-data/${ACTIVE_TIMEFRAME}${suffix}`", DOM_HTML_PAGE)
        self.assertIn('href="/dom/${timeframe}"', DOM_HTML_PAGE)
=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def test_ui_removes_dom_payload_dependency(self) -> None:
        self.assertNotIn("live-dom-panel", _HTML_PAGE)
        self.assertNotIn("renderLiveDomPanel", _HTML_PAGE)
        self.assertNotIn("/dom-data", _HTML_PAGE)
        self.assertNotIn("mergeSnapshots", _HTML_PAGE)
        self.assertNotIn("sessionMergeKeys", _HTML_PAGE)
        self.assertNotIn("live_dom", _HTML_PAGE)
        self.assertNotIn("currentReferencePrice", _HTML_PAGE)
<<<<<<< HEAD
        self.assertNotIn("computeSharedVisualRange", _HTML_PAGE)
        self.assertNotIn("renderSessionView", _HTML_PAGE)
        self.assertNotIn("bin-row", _HTML_PAGE)
        self.assertIn("Canvas engine", _HTML_PAGE)
=======
        self.assertIn("computeSharedVisualRange(candles, size, latestCandlePrice(candles))", _HTML_PAGE)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def test_service_does_not_wire_depth_stream(self) -> None:
        service = AbsorptionFootprintService(RuntimeConfig())

        self.assertFalse(hasattr(service, "depth_stream"))
        self.assertFalse(hasattr(service, "dom_snapshot_payload"))
        self.assertNotIn("dom", service._snapshot_cache)

<<<<<<< HEAD
    def test_ui_axes_are_calculated_once_for_visible_chart(self) -> None:
        self.assertIn("drawTimeAxis(ctx, candleItems, plotW, plotH)", _HTML_PAGE)
        self.assertIn("drawPriceAxis(ctx, plotW, plotH, axisWidth, minIndex, maxIndex, priceToY)", _HTML_PAGE)
        self.assertIn("function priceForBinIndex(index, size) { return index * size; }", _HTML_PAGE)
        self.assertIn("function rowCenterPriceForBinIndex(index, size) { return (index + 0.5) * size; }", _HTML_PAGE)
        self.assertIn("const y = priceToY(rowCenterPriceForBinIndex(index, this.fixedSize));", _HTML_PAGE)
        self.assertIn("if (index % 10 !== 0) return;", _HTML_PAGE)
        self.assertIn("dateTimeLabel(candleOpen(item.candle))", _HTML_PAGE)
        self.assertIn("ctx.fillText(fmtPrice(price)", _HTML_PAGE)
        self.assertNotIn("summary-lock", _HTML_PAGE)
        self.assertNotIn("summary-track", _HTML_PAGE)

    def test_ui_visual_range_uses_visible_candle_ohlc_with_padding(self) -> None:
        self.assertIn("computePriceRange(candles, size, plotH)", _HTML_PAGE)
        self.assertIn('for (const key of ["open", "high", "low", "close"])', _HTML_PAGE)
        self.assertIn("dataMinPrice = Math.min(dataMinPrice, value);", _HTML_PAGE)
        self.assertIn("dataMaxPrice = Math.max(dataMaxPrice, value);", _HTML_PAGE)
        self.assertIn("dataMinPrice = Math.min(dataMinPrice, index * size);", _HTML_PAGE)
        self.assertIn("dataMaxPrice = Math.max(dataMaxPrice, (index + 1) * size);", _HTML_PAGE)
        self.assertIn("minBinPixelHeight: 18,", _HTML_PAGE)
        self.assertIn("this.binPixelHeight = this.minBinPixelHeight;", _HTML_PAGE)
        self.assertIn("autoScaleEnabled: false,", _HTML_PAGE)
        self.assertIn("this.verticalPinned = true;", _HTML_PAGE)
        self.assertIn("const requestedBinPixelHeight = Math.max(", _HTML_PAGE)
        self.assertNotIn("if (!this.verticalPinned)", _HTML_PAGE)
        self.assertNotIn("const naturalBinPixelHeight = plotH / naturalRows;", _HTML_PAGE)
        self.assertNotIn("drawClosePath", _HTML_PAGE)
        self.assertIn("function footprintCandleWidth() {", _HTML_PAGE)
        self.assertIn("FOOTPRINT_VISUAL_CONFIG.defaultVisibleCandles", _HTML_PAGE)
        self.assertIn("function footprintFontSize(rowHeight)", _HTML_PAGE)
        self.assertIn("if (rowHeight < 12) return 0;", _HTML_PAGE)
        self.assertIn("if (rowHeight < 16) return 12;", _HTML_PAGE)
        self.assertIn("if (rowHeight <= 22) return 14;", _HTML_PAGE)
        self.assertIn("return 16;", _HTML_PAGE)
        self.assertNotIn("binAbnormalBuyImbalance(bin) ? 700 : 500", _HTML_PAGE)
        self.assertIn("const innerW = Math.max(72, Math.min(88, this.candleWidth - 10));", _HTML_PAGE)
        self.assertIn("if (h >= 12 && innerW >= 42)", _HTML_PAGE)
        self.assertIn("this.candleWidth = Math.max(88, Math.min(180, next));", _HTML_PAGE)
        self.assertIn("if (event.shiftKey)", _HTML_PAGE)
        self.assertIn("event.altKey || overPriceAxis", _HTML_PAGE)
        self.assertIn("const overPriceAxis", _HTML_PAGE)
        self.assertIn('params.set("bin_ticks", String(activeBinTickCount));', _HTML_PAGE)
        self.assertIn('id="bin-tick-select"', _HTML_PAGE)
        self.assertIn('<option value="1">1 tick</option>', _HTML_PAGE)
        self.assertIn("const BIN_TICK_OPTIONS = [1, 2, 4, 8, 16];", _HTML_PAGE)
        self.assertIn("FOOTPRINT_LAYOUT | data_min=", _HTML_PAGE)
        self.assertIn('localStorage.getItem("footprint.visualConfig")', _HTML_PAGE)
        self.assertNotIn("function expandedVisualRange(previousRange, incomingRange)", _HTML_PAGE)
        self.assertNotIn("function stableVisualRange", _HTML_PAGE)
        self.assertNotIn("function visualRangeKey(range)", _HTML_PAGE)
        self.assertNotIn("priceContextBins", _HTML_PAGE)
        self.assertNotIn("contextRangeForPrice", _HTML_PAGE)

    def test_absorption_bin_highlight_uses_spike_efficiency_and_side(self) -> None:
        self.assertIn(f"const ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN = {CONTRACT_SPIKE_THRESHOLD};", _HTML_PAGE)
        self.assertNotIn("__CONTRACT_SPIKE_THRESHOLD__", _HTML_PAGE)
        self.assertNotIn("const ABSORPTION_HIGHLIGHT_EFFICIENCY_MAX", _HTML_PAGE)
        self.assertIn("function binAbsorptionHighlightStyle(candle, bin, size)", _HTML_PAGE)
        self.assertIn("const FOOTPRINT_HIGH_SPIKE_SCORE_THRESHOLD = 12;", _HTML_PAGE)
        self.assertIn("function binHasHighContractSpikeScore(bin)", _HTML_PAGE)
        self.assertIn("score > FOOTPRINT_HIGH_SPIKE_SCORE_THRESHOLD", _HTML_PAGE)
        self.assertIn("const hasOneSidedContracts = (", _HTML_PAGE)
        self.assertIn("(buyContracts === 0 && sellContracts > 0)", _HTML_PAGE)
        self.assertIn("(sellContracts === 0 && buyContracts > 0)", _HTML_PAGE)
        self.assertIn("score > FOOTPRINT_HIGH_SPIKE_SCORE_THRESHOLD && hasOneSidedContracts", _HTML_PAGE)
        self.assertIn("const FOOTPRINT_SINGLE_SIDE_SPIKE_SCORE_THRESHOLD = 14;", _HTML_PAGE)
        self.assertIn("function binInSingleSideSpikeRegion(candle, bin, size, kind)", _HTML_PAGE)
        self.assertIn("function binSingleSideSpikeStyle(candle, bin, size)", _HTML_PAGE)
        self.assertIn('if (kind === "SELL_ONLY")', _HTML_PAGE)
        self.assertIn('if (kind === "BUY_ONLY")', _HTML_PAGE)
        self.assertIn("buyContracts === 0", _HTML_PAGE)
        self.assertIn("sellContracts === 0", _HTML_PAGE)
        self.assertIn("return FOOTPRINT_SINGLE_SIDE_SPIKE_STYLES.SELL_ONLY;", _HTML_PAGE)
        self.assertIn("return FOOTPRINT_SINGLE_SIDE_SPIKE_STYLES.BUY_ONLY;", _HTML_PAGE)
        self.assertIn('const textColor = binHasHighContractSpikeScore(bin) ? "#000000" : null;', _HTML_PAGE)
        self.assertIn("const singleSideSpikeStyle = binSingleSideSpikeStyle(candle, bin, this.fixedSize);", _HTML_PAGE)
        self.assertIn('const hasHighContractSpikeScore = binHasHighContractSpikeScore(bin);', _HTML_PAGE)
        self.assertIn("const usesHighContrastBin = Boolean(singleSideSpikeStyle) || hasHighContractSpikeScore;", _HTML_PAGE)
        self.assertIn("singleSideSpikeStyle?.fill", _HTML_PAGE)
        self.assertIn('? "#ffffff"', _HTML_PAGE)
        self.assertIn("const buyContracts = binBuy(bin);", _HTML_PAGE)
        self.assertIn("const sellContracts = binSell(bin);", _HTML_PAGE)
        self.assertIn('if (buyContracts > sellContracts) return "BUY";', _HTML_PAGE)
        self.assertIn('if (sellContracts > buyContracts) return "SELL";', _HTML_PAGE)
        self.assertIn("spikeScore < ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN", _HTML_PAGE)
        self.assertNotIn("efficiency > ABSORPTION_HIGHLIGHT_EFFICIENCY_MAX", _HTML_PAGE)
        self.assertIn('if (side === "BUY") return bodyHigh <= price && price <= high;', _HTML_PAGE)
        self.assertIn('if (side === "SELL") return low <= price && price <= bodyLow;', _HTML_PAGE)
        self.assertIn("ABSORPTION_HIGHLIGHT_STYLES[side]", _HTML_PAGE)
        self.assertIn("rgba(255,45,149,.78)", _HTML_PAGE)
        self.assertIn("rgba(63,185,80,.78)", _HTML_PAGE)
        self.assertIn("drawAbsorptionHighlightExtensions(ctx, candleItems, plotW, plotH, priceToY)", _HTML_PAGE)
        self.assertIn("binAbsorptionHighlightStyle(candle, bin, this.fixedSize)", _HTML_PAGE)
        self.assertIn("binAbsorptionHighlightStyle(item.candle, bin, this.fixedSize)", _HTML_PAGE)
        self.assertIn("candleHasContractAtBinIndex(candleItems[blockerIndex].candle, index, this.fixedSize)", _HTML_PAGE)
        self.assertNotIn('ctx.strokeStyle = "rgba(210,153,34,.95)";', _HTML_PAGE)
        self.assertNotIn("FOOTPRINT_SINGLE_SIDE_SPIKE_SCORE_THRESHOLD", _candles_html_page("M1"))

    def test_ui_refreshes_on_candle_close_without_dom_polling(self) -> None:
        self.assertIn("const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };", _HTML_PAGE)
        self.assertIn("const refreshDelayMs = 2500;", _HTML_PAGE)
        self.assertIn('params.set("candle_limit"', _HTML_PAGE)
        self.assertIn('params.set("end_time_ms"', _HTML_PAGE)
        self.assertNotIn("before_trading_day", _HTML_PAGE)
        self.assertIn("function cancelLiveRefresh()", _HTML_PAGE)
        self.assertIn("refreshTimer = setTimeout(() => {", _HTML_PAGE)
        self.assertIn(
            "viewportMode\n"
            "          || requestedWindowEndMs\n"
            "          || scheduledViewportRequest\n"
            "          || activeViewportRequest",
            _HTML_PAGE,
        )
        self.assertIn(
            "endTimeMs === null\n"
            "        && scheduledRequestId === null",
            _HTML_PAGE,
        )
        self.assertIn("cancelLiveRefresh();\n      requestedWindowEndMs", _HTML_PAGE)
        self.assertIn(
            "if (!viewportMode && requestId === latestViewportRequestId)",
            _HTML_PAGE,
        )
        self.assertIn("scheduleNextRefresh(refreshDelayMs);", _HTML_PAGE)
=======
    def test_ui_summary_rows_are_calculated_per_candle(self) -> None:
        self.assertIn('summary.delta += num(binPayloadField(bin, "horizontal_delta"));', _HTML_PAGE)
        self.assertIn('summary.volume += num(binPayloadField(bin, "total_volume"));', _HTML_PAGE)
        self.assertIn("function candleSummary(candle)", _HTML_PAGE)
        self.assertIn("function renderSummaryRows(candles)", _HTML_PAGE)
        self.assertIn("summary-lock", _HTML_PAGE)
        self.assertIn("summary-track", _HTML_PAGE)
        self.assertIn("function syncSummaryPosition(wrap)", _HTML_PAGE)
        self.assertIn("summary-delta-row", _HTML_PAGE)
        self.assertIn("summary-volume-row", _HTML_PAGE)

    def test_ui_visual_range_uses_visible_candle_ohlc_with_padding(self) -> None:
        self.assertIn("const candleRangePaddingBins = 10;", _HTML_PAGE)
        self.assertIn('priceBinIndex(ohlc(candle, "high"), size)', _HTML_PAGE)
        self.assertIn('priceBinIndex(ohlc(candle, "low"), size)', _HTML_PAGE)
        self.assertIn("Math.min(highIndex, lowIndex) - candleRangePaddingBins", _HTML_PAGE)
        self.assertIn("Math.max(highIndex, lowIndex) + candleRangePaddingBins", _HTML_PAGE)
        self.assertIn("const referenceIndex = priceBinIndex(referencePrice, size);", _HTML_PAGE)
        self.assertNotIn("function expandedVisualRange(previousRange, incomingRange)", _HTML_PAGE)
        self.assertNotIn("function stableVisualRange", _HTML_PAGE)
        self.assertIn("function visualRangeKey(range)", _HTML_PAGE)
        self.assertNotIn("priceContextBins", _HTML_PAGE)
        self.assertNotIn("contextRangeForPrice", _HTML_PAGE)

    def test_ui_refreshes_on_candle_close_without_dom_polling(self) -> None:
        self.assertIn("const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };", _HTML_PAGE)
        self.assertIn("function nextCandleRefreshDelayMs(now = Date.now())", _HTML_PAGE)
        self.assertIn("setTimeout(refresh, nextCandleRefreshDelayMs())", _HTML_PAGE)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.assertNotIn("setInterval(refresh, 750)", _HTML_PAGE)
        self.assertNotIn('fetchJson("/data")', _HTML_PAGE)
        self.assertNotIn('fetchJson("/dom-data")', _HTML_PAGE)

    def test_footprint_data_path_filters_to_one_timeframe(self) -> None:
        self.assertEqual(_timeframe_for_data_path("/footprint-data/M1"), "M1")
        self.assertEqual(_timeframe_for_data_path("/footprint-data/h1"), "H1")
        self.assertIsNone(_timeframe_for_data_path("/footprint-data/H4"))
<<<<<<< HEAD
        self.assertEqual(_bin_tick_count_from_query({"bin_ticks": ["1"]}), 1)
        self.assertEqual(_bin_tick_count_from_query({"bin_ticks": ["2"]}), 2)
        self.assertEqual(_bin_tick_count_from_query({"bin_ticks": ["16"]}), 16)
        self.assertEqual(_bin_tick_count_from_query({"bin_ticks": ["3"]}), 1)

    def test_known_browser_candles_are_omitted_from_viewport_response(self) -> None:
        payload = {
            "sessions": [
                {
                    "mt5_symbol": "NQ",
                    "candles": [
                        {"open_time_ms": 1000},
                        {"open_time_ms": 2000},
                        {"open_time_ms": 3000},
                    ],
                }
            ]
        }

        filtered = _filter_snapshot_payload_known_candles(
            payload,
            {1000, 3000},
        )

        self.assertEqual(
            filtered["sessions"][0]["candles"],
            [{"open_time_ms": 2000}],
        )
        self.assertEqual(filtered["client_cached_candle_count"], 2)
        self.assertEqual(len(payload["sessions"][0]["candles"]), 3)

        payload = {
            "sessions": [
                {
                    "mt5_symbol": "NQ",
                    "bin_tick_count": 1,
                    "candles": [{"open_time_ms": 1000}],
                }
            ]
        }

        filtered = _filter_snapshot_payload_known_candles(
            payload,
            {1000},
            client_bin_tick_count=4,
        )

        self.assertEqual(filtered["sessions"][0]["candles"], [{"open_time_ms": 1000}])
        self.assertEqual(filtered["client_cached_candle_count"], 0)

        payload = {
            "memory_candles": 40,
            "display_candles_by_timeframe": {"M1": 1440, "M5": 288},
=======

        payload = {
            "memory_candles": 40,
            "display_candles_by_timeframe": {"M1": 600, "M5": 120},
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
            "sessions": [
                {"timeframe": "M1", "symbol": "BTCUSD"},
                {"timeframe": "M5", "symbol": "ETHUSD"},
            ],
        }

        filtered = _filter_snapshot_payload_timeframe(payload, "m1")

<<<<<<< HEAD
        self.assertEqual(filtered["memory_candles"], 1440)
        self.assertEqual(filtered["sessions"], [{"timeframe": "M1", "symbol": "BTCUSD"}])

    def test_footprint_data_path_supports_delta_after_open_time(self) -> None:
        payload = {
            "sessions": [
                {
                    "timeframe": "M5",
                    "candles": [
                        {"open_time_ms": 1000},
                        {"open_time_ms": 2000},
                        {"open_time_ms": 3000},
                    ],
                    "live_candle": {"open_time_ms": 4000, "is_live": True},
                }
            ]
        }

        filtered = _filter_snapshot_payload_after_open_time(payload, 2000)

        self.assertEqual(filtered["delta_after_open_time_ms"], 2000)
        self.assertEqual(filtered["sessions"][0]["candles"], [{"open_time_ms": 3000}])
        self.assertEqual(filtered["sessions"][0]["live_candle"], {"open_time_ms": 4000, "is_live": True})

    def test_ui_candle_limits_cover_twenty_four_hours_per_timeframe(self) -> None:
        self.assertEqual(
            STUDY_DISPLAY_CANDLE_LIMITS,
            {
                "M1": 1440,
                "M5": 288,
                "M15": 96,
                "M30": 48,
                "H1": 24,
            },
        )
        self.assertEqual(study_display_candle_limit("M1"), 1440)
        self.assertEqual(study_display_candle_limit("M5"), 288)
        self.assertEqual(study_display_candle_limit("M15"), 96)
        self.assertEqual(study_display_candle_limit("M30"), 48)
        self.assertEqual(study_display_candle_limit("H1"), 24)
        self.assertEqual(study_display_candle_limit("H4", default=150), 150)
        self.assertIn("display_candles_by_timeframe", _HTML_PAGE)
        self.assertIn("viewport candles ${displayLimit}", _HTML_PAGE)
        self.assertNotIn("H4", KLINE_INTERVAL_BY_INTERNAL)

    def test_candle_chart_routes_are_timeframe_specific(self) -> None:
        self.assertEqual(_timeframe_for_candles_path("/candles"), "M5")
        self.assertEqual(_timeframe_for_candles_path("/candles/M1"), "M1")
        self.assertEqual(_timeframe_for_candles_path("/candles/h1"), "H1")
        self.assertIsNone(_timeframe_for_candles_path("/candles/H4"))
        self.assertEqual(_timeframe_for_candles_data_path("/candles-data/M30"), "M30")
        self.assertIsNone(_timeframe_for_candles_data_path("/candles-data/H4"))
        self.assertIn('href="/candles/${timeframe}"', _candles_html_page("M1"))
        self.assertIn("computeCandleDataPriceRange(candles)", _candles_html_page("M1"))
        self.assertIn("computeVisualRenderRange(candleRange)", _candles_html_page("M1"))
        self.assertIn("visibleScaleCandles()", _candles_html_page("M1"))
        self.assertIn("const scaleCandles = this.visibleScaleCandles();", _candles_html_page("M1"))
        self.assertIn("const candleRange = this.computeCandleDataPriceRange(scaleCandles);", _candles_html_page("M1"))
        self.assertNotIn("includePrice(index * size);", _candles_html_page("M1"))
        self.assertNotIn("includePrice((index + 1) * size);", _candles_html_page("M1"))
        self.assertIn("visibleCandleItems(layout)", _candles_html_page("M1"))
        self.assertIn("item.x + this.candleWidth > layout.candlePlotX", _candles_html_page("M1"))
        self.assertIn("CANDLE_LAYOUT | candle_data_min=", _candles_html_page("M1"))
        self.assertIn('localStorage.getItem("candles.visualConfig")', _candles_html_page("M1"))
        self.assertIn("`/candles-data/${ACTIVE_TIMEFRAME}${suffix}`", _candles_html_page("M1"))
        self.assertIn('params.set("request_id", String(requestId))', _candles_html_page("M1"))
        self.assertIn('params.set("known_open_times_ms", known.join(","))', _candles_html_page("M1"))
        self.assertIn('params.set("include_profiles", "0")', _candles_html_page("M1"))
        self.assertIn("this.candleMap = new Map();", _candles_html_page("M1"))
        self.assertIn(
            f"const ABSORPTION_HIGHLIGHT_SPIKE_SCORE_MIN = {CONTRACT_SPIKE_THRESHOLD};",
            _candles_html_page("M1"),
        )
        self.assertNotIn("__CONTRACT_SPIKE_THRESHOLD__", _candles_html_page("M1"))
        self.assertNotIn("const ABSORPTION_HIGHLIGHT_EFFICIENCY_MAX", _candles_html_page("M1"))
        self.assertIn("this.fixedSize = num(session?.fixed_bin_size);", _candles_html_page("M1"))
        self.assertIn("if (size > 0) this.fixedSize = size;", _candles_html_page("M1"))
        self.assertIn("function binBuy(bin)", _candles_html_page("M1"))
        self.assertIn("function binSell(bin)", _candles_html_page("M1"))
        self.assertIn("const buyContracts = binBuy(bin);", _candles_html_page("M1"))
        self.assertIn("const sellContracts = binSell(bin);", _candles_html_page("M1"))
        self.assertIn('if (buyContracts > sellContracts) return "BUY";', _candles_html_page("M1"))
        self.assertIn('if (sellContracts > buyContracts) return "SELL";', _candles_html_page("M1"))
        self.assertIn('if (side === "BUY") return bodyHigh <= price && price <= high;', _candles_html_page("M1"))
        self.assertIn('if (side === "SELL") return low <= price && price <= bodyLow;', _candles_html_page("M1"))
        self.assertNotIn("this.drawAbsorptionHighlightExtensions(ctx, candleItems, layout, plotH, priceToY);", _candles_html_page("M1"))
        self.assertIn("function binCandleAbsorptionHighlightStyle(bin)", _candles_html_page("M1"))
        self.assertNotIn("const style = binAbsorptionHighlightStyle(item.candle, bin, size);", _candles_html_page("M1"))
        self.assertNotIn("const style = binCandleAbsorptionHighlightStyle(bin);", _candles_html_page("M1"))
        self.assertNotIn("const drawnBinIndexes = new Set();", _candles_html_page("M1"))
        self.assertNotIn("if (drawnBinIndexes.has(index)) continue;", _candles_html_page("M1"))
        self.assertNotIn("drawnBinIndexes.add(index);", _candles_html_page("M1"))
        self.assertNotIn("const CANDLE_ABSORPTION_BAND_MIN_PX", _candles_html_page("M1"))
        self.assertNotIn("function visiblePixelBand", _candles_html_page("M1"))
        self.assertNotIn("const bounds = candleWickBandBounds(item.candle, side, priceToY);", _candles_html_page("M1"))
        self.assertNotIn("const price = binCenterPrice(bin, size);", _candles_html_page("M1"))
        self.assertNotIn("ctx.moveTo(sourceX, y);", _candles_html_page("M1"))
        self.assertNotIn("ctx.setLineDash([6, 5]);", _candles_html_page("M1"))
        self.assertNotIn("ctx.fillStyle = style.fill;", _candles_html_page("M1"))
        self.assertNotIn("ctx.globalAlpha = 0.45;", _candles_html_page("M1"))
        self.assertNotIn("ctx.strokeStyle = style.stroke;", _candles_html_page("M1"))
        self.assertNotIn("const sourceLeft = Math.max(layout.candlePlotX, item.x);", _candles_html_page("M1"))
        self.assertNotIn("candleHasContractAtBinIndex(candleItems[blockerIndex].candle, index, size)", _candles_html_page("M1"))
        for timeframe in ("M1", "M5", "M15", "M30", "H1"):
            candle_page = _candles_html_page(timeframe)
            self.assertNotIn("daily_volume_profiles", candle_page)
            self.assertNotIn("profile-scale-mode", candle_page)
            self.assertNotIn("drawProfiles(", candle_page)
            self.assertNotIn("scheduleProfileRefresh(", candle_page)
            self.assertNotIn('params.set("include_profiles", "1")', candle_page)
        self.assertEqual(
            _window_end_time_from_query({"end_time_ms": ["1781136000000"]}),
            1781136000000,
        )
        self.assertEqual(_candle_limit_from_query({"candle_limit": ["240"]}), 240)
        self.assertEqual(_candle_limit_from_query({"candle_limit": ["900"]}), 500)
        self.assertFalse(_include_profiles_from_query({"include_profiles": ["0"]}))
        self.assertFalse(_include_profiles_from_query({"include_profiles": ["false"]}))
        self.assertTrue(_include_profiles_from_query({"include_profiles": ["1"]}))
        self.assertTrue(_include_profiles_from_query({}))
        self.assertNotIn("before_trading_day", _candles_html_page("M1"))
        self.assertNotIn("scheduleNextRefresh", _candles_html_page("M1"))

    def test_footprint_tooltip_is_clamped_by_measured_size(self) -> None:
        self.assertIn("font-size: 20px;", _HTML_PAGE)
        self.assertIn("const tooltipHeight = this.tooltip.offsetHeight || 120;", _HTML_PAGE)
        self.assertIn("if (top + tooltipHeight + 8 > plotH)", _HTML_PAGE)
        self.assertIn("top = Math.max(0, Math.min(top, Math.max(0, plotH - tooltipHeight - 8)))", _HTML_PAGE)

=======
        self.assertEqual(filtered["memory_candles"], 600)
        self.assertEqual(filtered["sessions"], [{"timeframe": "M1", "symbol": "BTCUSD"}])

    def test_ui_candle_limits_cover_ten_hours_per_timeframe(self) -> None:
        self.assertEqual(
            STUDY_DISPLAY_CANDLE_LIMITS,
            {
                "M1": 600,
                "M5": 120,
                "M15": 40,
                "M30": 20,
                "H1": 10,
            },
        )
        self.assertEqual(study_display_candle_limit("M1"), 600)
        self.assertEqual(study_display_candle_limit("M5"), 120)
        self.assertEqual(study_display_candle_limit("M15"), 40)
        self.assertEqual(study_display_candle_limit("M30"), 20)
        self.assertEqual(study_display_candle_limit("H1"), 10)
        self.assertEqual(study_display_candle_limit("H4", default=150), 150)
        self.assertIn("display_candles_by_timeframe", _HTML_PAGE)
        self.assertIn("UI candles ${displayLimit}", _HTML_PAGE)
        self.assertNotIn("H4", KLINE_INTERVAL_BY_INTERNAL)

>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def test_service_allocates_timeframe_specific_ui_memory(self) -> None:
        service = AbsorptionFootprintService(RuntimeConfig())
        service.configure_sessions(
            [
                SymbolSessionState(
                    mt5_symbol="BTCUSD",
                    timeframe="M15",
                    binance_symbol="BTCUSDT",
                    symbol_resolved=True,
                )
            ]
        )

<<<<<<< HEAD
        self.assertEqual(service._memories[("BTCUSD", "M15")].max_candles, 96)
=======
        self.assertEqual(service._memories[("BTCUSD", "M15")].max_candles, 40)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.assertNotIn(("BTCUSD", "M1"), service._memories)
        self.assertNotIn(("BTCUSD", "M5"), service._memories)
        self.assertNotIn(("BTCUSD", "M30"), service._memories)
        self.assertNotIn(("BTCUSD", "H1"), service._memories)
<<<<<<< HEAD
        self.assertEqual(service._study_memory_candles("M1"), 1440)
        self.assertEqual(service._study_memory_candles("H1"), 24)
        self.assertEqual(service._closed_candle_keep_count("M1"), 1595)
        self.assertEqual(service._closed_candle_keep_count("H1"), 179)
=======
        self.assertEqual(service._study_memory_candles("M1"), 600)
        self.assertEqual(service._study_memory_candles("H1"), 10)
        self.assertEqual(service._closed_candle_keep_count("M1"), 755)
        self.assertEqual(service._closed_candle_keep_count("H1"), 165)
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
        self.assertEqual(
            service.snapshot_payload()["display_candles_by_timeframe"],
            STUDY_DISPLAY_CANDLE_LIMITS,
        )

        runtime = service.absorption_runtime
        self.assertEqual(runtime._active_internal_timeframes("BTCUSD"), ("M15",))
<<<<<<< HEAD
        self.assertEqual(runtime._active_output_timeframes("BTCUSD"), ())
        self.assertFalse(runtime._is_execution_timeframe("BTCUSD", "M1"))
        self.assertFalse(runtime._is_execution_timeframe("BTCUSD", "M15"))
=======
        self.assertEqual(runtime._active_output_timeframes("BTCUSD"), ("M15",))
        self.assertFalse(runtime._is_execution_timeframe("BTCUSD", "M1"))
        self.assertTrue(runtime._is_execution_timeframe("BTCUSD", "M15"))
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

        runtime.ensure_symbol_builders(
            mt5_symbol="BTCUSD",
            fixed_bin_size_by_timeframe={
                timeframe_name: Decimal("1")
                for timeframe_name in STUDY_DISPLAY_TIMEFRAMES
            },
            tick_size=Decimal("0.01"),
        )
        self.assertIn(("BTCUSD", "M15"), runtime._builders)
        self.assertNotIn(("BTCUSD", "M1"), runtime._builders)
        self.assertNotIn(("BTCUSD", "M5"), runtime._builders)
        self.assertNotIn(("BTCUSD", "M30"), runtime._builders)
        self.assertNotIn(("BTCUSD", "H1"), runtime._builders)
        self.assertNotIn(("BTCUSD", "H4"), runtime._builders)

<<<<<<< HEAD
    def test_snapshot_cache_purges_expired_cursor_pages(self) -> None:
        service = AbsorptionFootprintService(RuntimeConfig())
        service._snapshot_cache["footprint:M5:old"] = (
            time.monotonic() - service._snapshot_cache_ttl_seconds - 1,
            {"sessions": []},
        )

        service._store_snapshot_cache("footprint:M5:new", {"sessions": []})

        self.assertNotIn("footprint:M5:old", service._snapshot_cache)
        self.assertIn("footprint:M5:new", service._snapshot_cache)

    def test_cme_sessions_do_not_start_binance_streams(self) -> None:
        service = AbsorptionFootprintService(RuntimeConfig())
        service.configure_sessions(
            [
                SymbolSessionState(
                    mt5_symbol="NQ",
                    timeframe="M30",
                    market_provider=PROVIDER_CME_LOCAL_DBN,
                    provider_symbol="NQ.FUT",
                    dataset="GLBX.MDP3",
                    schema="trades",
                    tick_size="0.25",
                    symbol_resolved=True,
                    session_ready=True,
                    status="READY",
                )
            ]
        )

        asyncio.run(service.update_once())

        self.assertIn(("NQ", "M30"), service._session_specs)
        self.assertEqual(service.agg_trade_stream._states, {})
        self.assertEqual(service.kline_stream._tasks, {})
        self.assertEqual(service.raw_event_buffer.pending_trade_event_count(), 0)

=======
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744
    def test_runtime_rejects_unsupported_h4_timeframe(self) -> None:
        service = AbsorptionFootprintService(RuntimeConfig())
        session = SymbolSessionState(
            mt5_symbol="ETHUSD",
            timeframe="H4",
            binance_symbol="ETHUSDT",
            symbol_resolved=True,
            session_ready=True,
            status="READY",
        )
        service.configure_sessions([session])

        runtime = service.absorption_runtime
        self.assertEqual(session.status, "TIMEFRAME_NOT_SUPPORTED")
        self.assertFalse(session.session_ready)
        self.assertFalse(session.absorption_path_state.path_ready)
        self.assertEqual(service._session_specs, {})
        self.assertEqual(runtime._active_internal_timeframes("ETHUSD"), ())
        self.assertEqual(runtime._active_output_timeframes("ETHUSD"), ())
        self.assertFalse(runtime._is_execution_timeframe("ETHUSD", "M1"))
        self.assertFalse(runtime._is_execution_timeframe("ETHUSD", "H4"))

    def test_service_keeps_multi_symbol_multi_timeframe_sessions_isolated(self) -> None:
        service = AbsorptionFootprintService(RuntimeConfig())
        service.configure_sessions(
            [
                SymbolSessionState(
                    mt5_symbol="BTCUSD",
                    timeframe="M1",
                    binance_symbol="BTCUSDT",
                    symbol_resolved=True,
                ),
                SymbolSessionState(
                    mt5_symbol="BTCUSD",
                    timeframe="M5",
                    binance_symbol="BTCUSDT",
                    symbol_resolved=True,
                ),
                SymbolSessionState(
                    mt5_symbol="ETHUSD",
                    timeframe="H1",
                    binance_symbol="ETHUSDT",
                    symbol_resolved=True,
                ),
            ]
        )

        self.assertIn(("BTCUSD", "M1"), service._session_specs)
        self.assertIn(("BTCUSD", "M5"), service._session_specs)
        self.assertIn(("ETHUSD", "H1"), service._session_specs)
        self.assertNotIn(("BTCUSD", "H1"), service._session_specs)

        runtime = service.absorption_runtime
        self.assertEqual(runtime._active_internal_timeframes("BTCUSD"), ("M1", "M5"))
<<<<<<< HEAD
        self.assertEqual(runtime._active_output_timeframes("BTCUSD"), ("M5",))
        self.assertEqual(runtime._active_internal_timeframes("ETHUSD"), ("H1",))
        self.assertEqual(runtime._active_output_timeframes("ETHUSD"), ())
=======
        self.assertEqual(runtime._active_output_timeframes("BTCUSD"), ("M1", "M5"))
        self.assertEqual(runtime._active_internal_timeframes("ETHUSD"), ("H1",))
        self.assertEqual(runtime._active_output_timeframes("ETHUSD"), ("H1",))
>>>>>>> e43d73ff590f4332fffa9a8782f68c807c5f6744

    def test_raw_event_buffer_timeframes_release_independently(self) -> None:
        buffer = RawMarketEventBuffer()
        buffer.configure_symbol_timeframes("BTCUSDT", {"M1", "H1"})
        buffer.append_trade_event(
            AggTradeEvent(
                symbol="BTCUSDT",
                event_time_ms=1,
                price=Decimal("100"),
                quantity=Decimal("1"),
                side="buy",
            )
        )

        self.assertEqual(buffer.pending_trade_event_count("BTCUSDT", "M1"), 1)
        self.assertEqual(buffer.pending_trade_event_count("BTCUSDT", "H1"), 1)
        self.assertEqual(buffer.retention_blocking_timeframe("BTCUSDT", "M1"), "M1")
        self.assertEqual(buffer.retention_blocking_timeframe("BTCUSDT"), "H1")

        buffer.mark_timeframe_processed("BTCUSDT", "M1", 0, 60_000)
        self.assertEqual(buffer.pending_trade_event_count("BTCUSDT", "M1"), 0)
        self.assertEqual(buffer.pending_trade_event_count("BTCUSDT", "H1"), 1)
        self.assertIsNone(buffer.retention_blocking_timeframe("BTCUSDT", "M1"))
        self.assertEqual(buffer.retention_blocking_timeframe("BTCUSDT", "H1"), "H1")

        buffer.mark_timeframe_processed("BTCUSDT", "H1", 0, 3_600_000)
        self.assertEqual(buffer.pending_trade_event_count("BTCUSDT"), 0)


if __name__ == "__main__":
    unittest.main()
