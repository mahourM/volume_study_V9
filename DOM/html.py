from __future__ import annotations

from core.timeframe_policy import DEFAULT_FOOTPRINT_TIMEFRAME, STUDY_TIMEFRAMES

DOM_TIMEFRAMES = STUDY_TIMEFRAMES


def dom_timeframe_for_path(path: str) -> str | None:
    if path == "/dom":
        return DEFAULT_FOOTPRINT_TIMEFRAME
    prefix = "/dom/"
    if path.startswith(prefix):
        timeframe = path[len(prefix) :].strip().upper()
        if timeframe in DOM_TIMEFRAMES:
            return timeframe
    return None


def dom_timeframe_for_data_path(path: str) -> str | None:
    prefix = "/dom-data/"
    if not path.startswith(prefix):
        return None
    timeframe = path[len(prefix) :].strip().upper()
    if timeframe in DOM_TIMEFRAMES:
        return timeframe
    return None


def dom_html_page(timeframe: str = DEFAULT_FOOTPRINT_TIMEFRAME) -> str:
    active_timeframe = timeframe.strip().upper()
    if active_timeframe not in DOM_TIMEFRAMES:
        active_timeframe = DEFAULT_FOOTPRINT_TIMEFRAME
    return _DOM_HTML_TEMPLATE.replace("__ACTIVE_TIMEFRAME__", active_timeframe)


_DOM_HTML_TEMPLATE = r'''<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>DOM Timeline __ACTIVE_TIMEFRAME__</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #0b0f14;
      --panel: #10161d;
      --panel-2: #151b22;
      --line: #27313d;
      --text: #e7edf4;
      --muted: #8e9aaa;
      --bid: #37c987;
      --ask: #ff6b6b;
      --modify: #f2c94c;
      --execute: #7aa7ff;
      --cancel: #b98cff;
      --resting: rgba(152, 176, 204, .34);
      --blue: #58a6ff;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--text);
      font: 13px/1.35 Segoe UI, Arial, sans-serif;
      overflow: hidden;
    }
    header {
      height: 78px;
      padding: 14px 18px;
      border-bottom: 1px solid var(--line);
      background: #0f141b;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 {
      margin: 0 0 4px;
      font-size: 20px;
      line-height: 1.1;
      letter-spacing: 0;
    }
    .meta { color: var(--muted); }
    .header-controls {
      display: flex;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
      justify-content: flex-end;
    }
    .marker-filter {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-wrap: wrap;
      padding: 5px 8px;
      border: 1px solid #58a6ff;
      border-radius: 6px;
      background: #071625;
    }
    .marker-filter-item {
      --marker-color: #58a6ff;
      display: flex;
      align-items: center;
      gap: 5px;
      color: #fff;
      font-size: 15px;
      font-weight: 150;
      line-height: 1;
      white-space: nowrap;
    }
    .marker-filter-item.add { --marker-color: var(--bid); }
    .marker-filter-item.cancel { --marker-color: var(--cancel); }
    .marker-filter-item.modify { --marker-color: var(--modify); }
    .marker-filter-item.execute { --marker-color: var(--execute); }
    .marker-filter-item.resting { --marker-color: #44d7ff; }
    .marker-filter-item.price { --marker-color: #f2c94c; }
    .marker-filter-item.order-id { --marker-color: #58a6ff; }
    .marker-filter-item.refill { --marker-color: #2dd4bf; }
    .marker-filter-item.iceberg { --marker-color: #ff9f43; }
    .marker-filter-item input[type="checkbox"] {
      width: 16px;
      height: 16px;
      margin: 0;
      accent-color: var(--marker-color);
      cursor: pointer;
    }
    .marker-filter-text {
      width: 48px;
      height: 26px;
      border: 1px solid var(--marker-color);
      border-radius: 5px;
      background: #06111d;
      color: #fff;
      padding: 3px 5px;
      font: inherit;
      font-size: 15px;
      font-weight: 150;
      text-align: center;
      text-transform: lowercase;
    }
    .marker-filter-text:focus {
      outline: 2px solid var(--marker-color);
      outline-offset: 1px;
    }
    .marker-filter-text.price-filter {
      width: 82px;
    }
    .marker-filter-text.id-filter {
      width: 118px;
      text-transform: none;
    }
    .marker-filter-text.refill-filter {
      width: 74px;
    }
    .marker-filter-text.iceberg-filter {
      width: 74px;
    }
    .slice-window-control {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 5px 8px;
      border: 1px solid #f2c94c;
      border-radius: 6px;
      background: #171203;
      color: #fff;
      font-size: 15px;
      font-weight: 150;
      line-height: 1;
      white-space: nowrap;
    }
    .slice-window-control input[type="checkbox"] {
      width: 16px;
      height: 16px;
      margin: 0;
      accent-color: #f2c94c;
      cursor: pointer;
    }
    .slice-window-input {
      width: 190px;
      height: 26px;
      border: 1px solid #f2c94c;
      border-radius: 5px;
      background: #0f0b02;
      color: #fff;
      padding: 3px 5px;
      font: inherit;
      font-size: 15px;
      font-weight: 150;
      text-align: left;
      color-scheme: dark;
    }
    .slice-window-input:focus {
      outline: 2px solid #f2c94c;
      outline-offset: 1px;
    }
    .slice-window-input:disabled {
      opacity: .55;
      border-color: #a87900;
      color: #fff;
      cursor: not-allowed;
    }
    .ny-session-control {
      display: flex;
      align-items: center;
      gap: 6px;
      padding: 5px 8px;
      border: 1px solid #37c987;
      border-radius: 6px;
      background: #061a13;
      color: #fff;
      font-size: 15px;
      font-weight: 150;
      line-height: 1;
      white-space: nowrap;
    }
    .ny-session-control input[type="checkbox"] {
      width: 16px;
      height: 16px;
      margin: 0;
      accent-color: #37c987;
      cursor: pointer;
    }
    .timeframe-links {
      display: flex;
      align-items: center;
      gap: 6px;
      flex-wrap: wrap;
    }
    .timeframe-link, .tool-button, .date-input {
      border: 1px solid var(--line);
      color: #fff;
      background: #111820;
      border-radius: 6px;
      padding: 6px 9px;
      text-decoration: none;
      font: inherit;
      font-weight: 700;
      cursor: pointer;
    }
    .date-input {
      color: var(--text);
      height: 31px;
      min-width: 138px;
      color-scheme: dark;
    }
    .timeframe-link.active, .tool-button.active {
      color: var(--text);
      border-color: var(--blue);
      background: rgba(88,166,255,.16);
    }
    main {
      height: calc(100vh - 78px);
      display: grid;
      grid-template-columns: minmax(0, 1fr) 520px 18px;
      min-width: 0;
    }
    .dom-stage {
      position: relative;
      min-width: 0;
      min-height: 0;
      background: #0b0f14;
      overflow: hidden;
      cursor: crosshair;
    }
    .timeline-scrollbar {
      position: absolute;
      left: 88px;
      right: 18px;
      bottom: 12px;
      z-index: 3;
      height: 18px;
      overflow-x: scroll;
      overflow-y: hidden;
      scrollbar-color: var(--blue) #1b2430;
      scrollbar-width: auto;
    }
    .timeline-scrollbar.disabled {
      opacity: .35;
    }
    .timeline-scrollbar.day-scrollbar {
      bottom: 64px;
      scrollbar-color: #f2c94c #1b2430;
    }
    .time-scrollbar-content {
      height: 1px;
      width: 100%;
    }
    .day-scrollbar-content {
      height: 1px;
      width: 100%;
    }
    .ny-session-strip {
      position: absolute;
      left: 88px;
      right: 18px;
      bottom: 36px;
      height: 22px;
      z-index: 3;
      pointer-events: none;
      overflow: visible;
    }
    .ny-session-segment {
      position: absolute;
      top: 7px;
      height: 9px;
      border-radius: 5px;
      background: rgba(55, 201, 135, .78);
      box-shadow: 0 0 0 1px rgba(55, 201, 135, .95), 0 0 10px rgba(55, 201, 135, .45);
    }
    .ny-session-boundary {
      position: absolute;
      top: 1px;
      width: 2px;
      height: 20px;
      background: #f2c94c;
      box-shadow: 0 0 8px rgba(242, 201, 76, .7);
    }
    .ny-session-label {
      position: absolute;
      top: -12px;
      transform: translateX(-50%);
      color: #fff;
      font-size: 13px;
      font-weight: 150;
      white-space: nowrap;
      text-shadow: 0 1px 4px #000;
    }
    canvas {
      display: block;
      width: 100%;
      height: 100%;
    }
    .book-panel {
      border-left: 1px solid var(--line);
      background: var(--panel);
      min-width: 0;
      overflow: hidden;
      position: relative;
    }
    .book-title {
      padding: 12px 12px 6px;
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 8px;
      border-bottom: 1px solid var(--line);
      position: relative;
      z-index: 2;
      background: var(--panel);
    }
    .book-title strong { font-size: 14px; }
    .legend {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 7px 10px;
      padding: 10px 12px;
      color: var(--muted);
      border-bottom: 1px solid var(--line);
      position: relative;
      z-index: 2;
      background: var(--panel);
    }
    .legend-item {
      display: flex;
      align-items: center;
      gap: 6px;
      min-width: 0;
    }
    .swatch {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex: 0 0 auto;
    }
    .swatch.bid { background: var(--bid); }
    .swatch.ask { background: var(--ask); }
    .swatch.cancel { background: var(--cancel); }
    .swatch.modify { background: var(--modify); }
    .swatch.execute { background: var(--execute); }
    .swatch.resting { background: var(--resting); border-radius: 2px; }
    .book-levels {
      position: absolute;
      inset: 0;
      overflow: hidden;
      z-index: 1;
    }
    .book-row {
      position: absolute;
      left: 0;
      right: 0;
      height: 24px;
      display: grid;
      grid-template-columns: .55fr .72fr .55fr .9fr 1.35fr .72fr .58fr;
      align-items: center;
      gap: 6px;
      padding: 0 8px;
      border-bottom: 1px solid rgba(39,49,61,.55);
      font-variant-numeric: tabular-nums;
      font-size: 15px;
      font-weight: 150;
      line-height: 1;
    }
    .book-row .bid { color: var(--bid); text-align: left; font-weight: 150; }
    .book-row .ask { color: var(--ask); text-align: right; font-weight: 150; }
    .book-row .price { color: var(--text); text-align: center; font-weight: 150; }
    .book-row .exec {
      display: flex;
      align-items: center;
      justify-content: center;
      gap: 3px;
      color: var(--muted);
      min-width: 0;
    }
    .book-row .exec-buy,
    .book-row .exec-sell {
      min-width: 22px;
      padding: 3px 4px;
      border-radius: 4px;
      text-align: center;
      font-weight: 150;
    }
    .book-row .exec-buy { color: var(--bid); }
    .book-row .exec-sell { color: var(--ask); }
    .book-row .exec-hot {
      color: #fff4b8;
      background: rgba(242,201,76,.28);
      box-shadow: inset 0 0 0 1px rgba(242,201,76,.65);
    }
    .book-row .book-top-id,
    .book-row .book-top-type,
    .book-row .book-top-qty {
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      color: #e7edf4;
      font-weight: 150;
    }
    .book-row .book-top-id { text-align: left; }
    .book-row .book-top-type { text-align: left; color: var(--muted); }
    .book-row .book-top-qty { text-align: right; color: #f2c94c; }
    .price-scrollbar {
      height: 100%;
      overflow-x: hidden;
      overflow-y: scroll;
      border-left: 1px solid var(--line);
      background: #0f141b;
      scrollbar-color: var(--blue) #1b2430;
      scrollbar-width: auto;
    }
    .price-scrollbar.disabled {
      opacity: .35;
    }
    .price-scrollbar-content {
      width: 1px;
      height: 100%;
    }
    .tooltip {
      position: absolute;
      left: 0;
      top: 0;
      max-width: min(680px, calc(100% - 24px));
      max-height: calc(100% - 24px);
      padding: 12px 14px;
      border: 1px solid var(--line);
      background: rgba(15,20,27,.95);
      color: var(--text);
      font-size: 20px;
      font-weight: 100;
      pointer-events: none;
      white-space: pre-wrap;
      overflow-wrap: anywhere;
      overflow: auto;
      opacity: 0;
      line-height: 1.32;
      z-index: 4;
      box-shadow: 0 10px 28px rgba(0,0,0,.35);
      font-variant-numeric: tabular-nums;
    }
    .time-slice-panel {
      position: fixed;
      left: 0;
      top: 0;
      width: min(1900px, calc(100vw - 24px));
      overflow: hidden;
      border: 1px solid var(--blue);
      background: rgba(13,18,25,.97);
      color: var(--text);
      z-index: 8;
      opacity: 0;
      pointer-events: none;
      box-shadow: 0 16px 42px rgba(0,0,0,.45);
    }
    .time-slice-panel.open {
      opacity: 1;
      pointer-events: auto;
    }
    .time-slice-head {
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 14px;
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      background: rgba(21,27,34,.95);
      position: absolute;
      left: 0;
      right: 0;
      top: 0;
      z-index: 2;
    }
    .time-slice-head strong {
      font-size: 18px;
      font-weight: 700;
    }
    .time-slice-head span {
      color: var(--muted);
      font-size: 15px;
      font-variant-numeric: tabular-nums;
    }
    .time-slice-columns {
      position: absolute;
      left: 0;
      right: 0;
      top: 43px;
      height: 30px;
      display: grid;
      grid-template-columns: minmax(64px, .66fr) minmax(116px, 1fr) minmax(56px, .52fr) minmax(56px, .52fr) minmax(96px, .92fr) minmax(104px, 1fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(132px, 1.08fr) minmax(82px, .76fr) minmax(78px, .7fr);
      align-items: center;
      gap: 5px;
      padding: 0 8px;
      color: var(--muted);
      background: rgba(21,27,34,.95);
      border-bottom: 1px solid var(--line);
      font-size: 16px;
      font-weight: 150;
      font-variant-numeric: tabular-nums;
      z-index: 2;
    }
    .time-slice-columns span {
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
    }
    .time-slice-columns .column-title {
      color: #fff;
      font-size: 16px;
      font-weight: 150;
    }
    .time-slice-columns .price,
    .time-slice-columns .ids,
    .time-slice-columns .top-id,
    .time-slice-columns .top-type { text-align: left; }
    .time-slice-body {
      position: absolute;
      inset: 0;
      overflow: hidden;
    }
    .time-slice-grid {
      position: relative;
      width: 100%;
      height: 100%;
      font-variant-numeric: tabular-nums;
      font-size: 16px;
    }
    .time-slice-row {
      position: absolute;
      left: 0;
      right: 0;
      display: grid;
      grid-template-columns: minmax(64px, .66fr) minmax(116px, 1fr) minmax(56px, .52fr) minmax(56px, .52fr) minmax(96px, .92fr) minmax(104px, 1fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(96px, .92fr) minmax(132px, 1.08fr) minmax(82px, .76fr) minmax(78px, .7fr);
      align-items: center;
      gap: 5px;
      padding: 0 8px;
      border-bottom: 1px solid rgba(39,49,61,.75);
      font-weight: 150;
      line-height: 1;
    }
    .time-slice-row span {
      text-align: right;
      white-space: nowrap;
      overflow: hidden;
      min-width: 0;
    }
    .time-slice-row .price,
    .time-slice-row .ids,
    .time-slice-row .top-id,
    .time-slice-row .top-type { color: var(--text); text-align: left; }
    .time-slice-row .ids {
      display: flex;
      align-items: center;
      gap: 7px;
      overflow: visible;
      min-width: 0;
    }
    .time-slice-row .top-id,
    .time-slice-row .top-type,
    .time-slice-row .top-qty {
      color: #e7edf4;
      text-overflow: ellipsis;
    }
    .time-slice-row .top-qty { color: #f2c94c; }
    .order-id-summary {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
    .order-id-toggle {
      flex: 0 0 auto;
      width: 22px;
      height: 22px;
      border: 1px solid var(--blue);
      background: rgba(28, 129, 232, .18);
      color: #fff;
      font-size: 16px;
      font-weight: 150;
      line-height: 18px;
      cursor: pointer;
      padding: 0;
    }
    .order-id-toggle:hover,
    .order-id-toggle.active {
      border-color: #f2c94c;
      color: #f2c94c;
    }
    .order-id-popup {
      position: absolute;
      z-index: 9;
      max-width: 420px;
      max-height: 260px;
      overflow: auto;
      border: 1px solid var(--blue);
      background: rgba(8, 13, 20, .99);
      box-shadow: 0 14px 34px rgba(0, 0, 0, .48);
      color: #fff;
      font-size: 17px;
      font-weight: 150;
      font-variant-numeric: tabular-nums;
      line-height: 1.45;
      padding: 8px 10px;
    }
    .order-id-popup[hidden] { display: none; }
    .order-id-popup div {
      white-space: nowrap;
      padding: 2px 0;
      border-bottom: 1px solid rgba(28, 129, 232, .22);
    }
    .order-id-popup div:last-child { border-bottom: 0; }
    .time-slice-row .bid { color: var(--bid); }
    .time-slice-row .ask { color: var(--ask); }
    .time-slice-row .add { color: #44d7ff; }
    .time-slice-row .mod-plus { color: #f2c94c; }
    .time-slice-row .mod-minus { color: #ff9f43; }
    .time-slice-empty {
      padding: 18px;
      color: var(--muted);
      font-weight: 700;
      font-size: 18px;
      text-align: center;
    }
    .loading-indicator {
      position: absolute;
      right: 18px;
      top: 16px;
      z-index: 3;
      padding: 7px 10px;
      border: 1px solid var(--blue);
      background: rgba(20, 72, 125, .72);
      color: #fff;
      font-weight: 800;
      opacity: 0;
      pointer-events: none;
    }
    .empty {
      height: 100%;
      display: grid;
      place-items: center;
      color: var(--muted);
      font-weight: 700;
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>DOM Timeline __ACTIVE_TIMEFRAME__</h1>
      <div class="meta" id="status">Waiting for DOM data...</div>
    </div>
    <div class="header-controls">
      <div class="marker-filter" id="marker-filter" aria-label="DOM marker filters">
        <label class="marker-filter-item add"><input type="checkbox" data-marker-filter="ADD" checked><span>ADD</span><input class="marker-filter-text" data-marker-threshold="ADD" type="text" value="all" aria-label="ADD minimum contracts"></label>
        <label class="marker-filter-item cancel"><input type="checkbox" data-marker-filter="CANCEL_DELETE" checked><span>DEL</span><input class="marker-filter-text" data-marker-threshold="CANCEL_DELETE" type="text" value="all" aria-label="Cancel minimum contracts"></label>
        <label class="marker-filter-item modify"><input type="checkbox" data-marker-filter="MODIFY" checked><span>MOD</span><input class="marker-filter-text" data-marker-threshold="MODIFY" type="text" value="all" aria-label="Modify minimum contracts"></label>
        <label class="marker-filter-item execute"><input type="checkbox" data-marker-filter="EXECUTE" checked><span>FILL</span><input class="marker-filter-text" data-marker-threshold="EXECUTE" type="text" value="all" aria-label="Fill minimum contracts"></label>
        <label class="marker-filter-item resting"><input type="checkbox" data-marker-filter="RESTING_LIQUIDITY" checked><span>REST</span><input class="marker-filter-text" data-marker-threshold="RESTING_LIQUIDITY" type="text" value="all" aria-label="Resting minimum contracts"></label>
        <label class="marker-filter-item price"><span>PRICE</span><input class="marker-filter-text price-filter" data-marker-price-filter type="text" value="all" aria-label="Marker price filter"></label>
        <label class="marker-filter-item order-id"><span>ID</span><input class="marker-filter-text id-filter" data-marker-id-filter type="text" value="all" aria-label="Marker order id filter"></label>
        <label class="marker-filter-item refill"><span>REFILL</span><input class="marker-filter-text refill-filter" data-marker-refill-filter type="text" value="all" aria-label="Refill minimum count"></label>
        <label class="marker-filter-item iceberg"><span>ICEBERG</span><input class="marker-filter-text iceberg-filter" data-marker-iceberg-filter type="text" value="all" aria-label="Iceberg minimum contracts"></label>
      </div>
      <label class="slice-window-control"><input id="slice-range-enabled" type="checkbox" aria-label="Enable DOM Slice time range">Slice from <input class="slice-window-input" id="slice-start-datetime" type="datetime-local" step="1" aria-label="DOM Slice start date and time"></label>
      <label class="ny-session-control"><input id="ny-session-toggle" type="checkbox" aria-label="Show only New York session">NY Session</label>
      <nav class="timeframe-links" id="timeframe-links" aria-label="DOM Timeline timeframes"></nav>
      <input class="date-input" id="dom-date" type="date" title="DOM date">
      <a class="timeframe-link" id="footprint-link" href="/footprint/__ACTIVE_TIMEFRAME__">Footprint</a>
      <button class="tool-button active" id="auto-scale" type="button">Auto</button>
    </div>
  </header>
  <main id="dom-main">
    <section class="dom-stage" id="stage">
      <canvas id="chart"></canvas>
      <div class="ny-session-strip" id="ny-session-strip"></div>
      <div class="timeline-scrollbar day-scrollbar disabled" id="day-scrollbar" tabindex="0" aria-label="DOM day scroll">
        <div class="day-scrollbar-content" id="day-scrollbar-content"></div>
      </div>
      <div class="timeline-scrollbar disabled" id="time-scrollbar" tabindex="0" aria-label="DOM time scroll">
        <div class="time-scrollbar-content" id="time-scrollbar-content"></div>
      </div>
      <div class="loading-indicator" id="loading-indicator">Loading DOM...</div>
      <div class="tooltip" id="tooltip"></div>
    </section>
    <aside class="book-panel" id="book-panel">
      <div class="book-title">
        <strong>Order Book</strong>
        <span class="meta" id="book-time">Latest visible</span>
      </div>
      <div class="legend">
        <div class="legend-item"><span class="swatch bid"></span><span>Add Bid</span></div>
        <div class="legend-item"><span class="swatch ask"></span><span>Add Ask</span></div>
        <div class="legend-item"><span class="swatch cancel"></span><span>Cancel/Delete</span></div>
        <div class="legend-item"><span class="swatch modify"></span><span>Modify</span></div>
        <div class="legend-item"><span class="swatch execute"></span><span>Execute</span></div>
        <div class="legend-item"><span class="swatch resting"></span><span>Resting</span></div>
      </div>
      <div class="book-levels" id="book-levels"></div>
    </aside>
    <div class="price-scrollbar disabled" id="price-scrollbar" tabindex="0" aria-label="DOM price scroll">
      <div class="price-scrollbar-content" id="price-scrollbar-content"></div>
    </div>
  </main>
  <div class="time-slice-panel" id="time-slice-panel"></div>
  <script>
    const ACTIVE_TIMEFRAME = "__ACTIVE_TIMEFRAME__";
    const TIMEFRAMES = ["M1", "M5", "M15", "M30", "H1"];
    const TIMEFRAME_MS = { M1: 60000, M5: 300000, M15: 900000, M30: 1800000, H1: 3600000 };
    const statusEl = document.getElementById("status");
    const mainEl = document.getElementById("dom-main");
    const linksEl = document.getElementById("timeframe-links");
    const autoScaleEl = document.getElementById("auto-scale");
    const dateInputEl = document.getElementById("dom-date");
    const markerFilterEl = document.getElementById("marker-filter");
    const sliceRangeEnabledEl = document.getElementById("slice-range-enabled");
    const sliceStartInputEl = document.getElementById("slice-start-datetime");
    const nySessionToggleEl = document.getElementById("ny-session-toggle");
    const nySessionStripEl = document.getElementById("ny-session-strip");
    const dayScrollbarEl = document.getElementById("day-scrollbar");
    const dayScrollbarContentEl = document.getElementById("day-scrollbar-content");
    const timeScrollbarEl = document.getElementById("time-scrollbar");
    const timeScrollbarContentEl = document.getElementById("time-scrollbar-content");
    const priceScrollbarEl = document.getElementById("price-scrollbar");
    const priceScrollbarContentEl = document.getElementById("price-scrollbar-content");
    const canvas = document.getElementById("chart");
    const stage = document.getElementById("stage");
    const tooltip = document.getElementById("tooltip");
    const timeSlicePanelEl = document.getElementById("time-slice-panel");
    const loadingIndicatorEl = document.getElementById("loading-indicator");
    const bookLevelsEl = document.getElementById("book-levels");
    const bookTimeEl = document.getElementById("book-time");
    const DAY_MS = 86400000;
    const BOOK_ROW_MIN_HEIGHT = 24;
    let lastBookRenderKey = "";

    function safeArray(value) { return Array.isArray(value) ? value : []; }
    function hasRawEventsPayload(session) {
      return Object.prototype.hasOwnProperty.call(Object(session || {}), "raw_events");
    }
    function rawSessionEvents(session, options = {}) {
      const rawEvents = safeArray(session?.raw_events);
      if (options.rawOnly || hasRawEventsPayload(session)) return rawEvents;
      return safeArray(session?.events);
    }
    function num(value, fallback = 0) {
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : fallback;
    }
    function fmt(value, digits = 2) {
      const parsed = Number(value);
      if (!Number.isFinite(parsed)) return "";
      const fixed = parsed.toFixed(digits);
      if (!fixed.includes(".")) return fixed;
      return fixed.replace(/(\.\d*?)0+$/, "$1").replace(/\.$/, "");
    }
    function escapeHtml(value) {
      return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }
    function contractLabel(value) {
      const parsed = Number(value);
      if (!Number.isFinite(parsed) || Math.abs(parsed) < 0.00000001) return "";
      return fmt(parsed, 0);
    }
    function metricLabel(total, count) {
      const parsedTotal = Number(total);
      const parsedCount = Number(count);
      if (!Number.isFinite(parsedTotal) || Math.abs(parsedTotal) < 0.00000001) return "";
      const safeCount = Number.isFinite(parsedCount) ? Math.max(0, Math.round(parsedCount)) : 0;
      return safeCount ? `${fmt(parsedTotal, 0)} (${safeCount})` : fmt(parsedTotal, 0);
    }
    function metricCellHtml(className, total, count) {
      const label = metricLabel(total, count);
      const parsedTotal = Number(total);
      const parsedCount = Number(count);
      const title = label
        ? [
            `Raw contracts: ${fmt(parsedTotal, 0)}`,
            `Events: ${Number.isFinite(parsedCount) ? fmt(Math.max(0, Math.round(parsedCount)), 0) : "0"}`,
          ].join("\n")
        : "";
      return `<span class="${className}"${title ? ` title="${escapeHtml(title)}"` : ""}>${escapeHtml(label)}</span>`;
    }
    function rowFontSize(rowHeight, maxSize = 28) {
      const parsed = Number(rowHeight);
      if (!Number.isFinite(parsed)) return 16;
      return Math.max(16, Math.min(maxSize, Math.round(parsed * 0.62)));
    }
    function orderIdsArray(orderIds) {
      if (!orderIds) return [];
      const values = Array.isArray(orderIds) ? orderIds : [...orderIds];
      return values.map(item => String(item || "").trim()).filter(Boolean);
    }
    function orderIdLabel(orderIds) {
      const values = orderIdsArray(orderIds);
      if (!values.length) return "";
      if (values.length === 1) return values[0];
      return `${values[0]} +${values.length - 1}`;
    }
    function orderIdTitle(orderIds) {
      return orderIdsArray(orderIds).join("\n");
    }
    function orderIdCellHtml(orderIds, menuIndex) {
      const values = orderIdsArray(orderIds);
      const toggle = values.length > 1
        ? `<button class="order-id-toggle" type="button" data-order-id-menu="${menuIndex}" aria-label="Show all order ids" title="Show all order ids">&#9662;</button>`
        : "";
      return (
        `<span class="ids" title="${escapeHtml(orderIdTitle(values))}">` +
        `<span class="order-id-summary">${escapeHtml(orderIdLabel(values))}</span>` +
        toggle +
        `</span>`
      );
    }
    function sliceContractTypeLabel(type, side) {
      const normalizedType = String(type || "").toUpperCase();
      const normalizedSide = String(side || "").toUpperCase();
      const sideLabel = normalizedSide === "ASK" ? "A" : (normalizedSide === "BID" ? "B" : "");
      const base = {
        ADD: "ADD",
        EXECUTE: "FILL",
        CANCEL_DELETE: "DEL",
        MODIFY: "MOD",
      }[normalizedType] || normalizedType;
      return [sideLabel, base].filter(Boolean).join(" ");
    }
    function rawEventAmount(event) {
      for (const key of ["raw_total_contracts", "raw_event_size", "order_size", "size"]) {
        if (!Object.prototype.hasOwnProperty.call(event || {}, key)) continue;
        const parsed = Number(event[key]);
        if (Number.isFinite(parsed) && parsed > 0) return parsed;
      }
      return NaN;
    }
    function eventIdentity(event) {
      return String(event?.event_id || event?.ordinal || event?.sequence || event?.order_id || event?.venue_order_id || "");
    }
    function rawEventKey(event) {
      const eventId = String(event?.event_id || "").trim();
      if (eventId) return eventId;
      return [
        event?.timestamp_ms ?? "",
        event?.price ?? "",
        event?.side ?? "",
        event?.event_type ?? "",
        event?.action ?? "",
        event?.order_id || event?.venue_order_id || "",
        event?.sequence ?? event?.ordinal ?? "",
        num(rawEventAmount(event), 0),
      ].join("|");
    }
    function mergeRawEvents(...eventLists) {
      const byKey = new Map();
      for (const events of eventLists) {
        for (const event of safeArray(events)) {
          const key = rawEventKey(event);
          if (!key) continue;
          const existing = byKey.get(key);
          if (!existing || !Number.isFinite(rawEventAmount(existing))) {
            byKey.set(key, event);
          }
        }
      }
      return orderedDomEvents([...byKey.values()]);
    }
    function compareDomEvents(a, b) {
      const timeDiff = num(a?.timestamp_ms, 0) - num(b?.timestamp_ms, 0);
      if (timeDiff !== 0) return timeDiff;
      return eventIdentity(a).localeCompare(eventIdentity(b));
    }
    function orderedDomEvents(events) {
      return safeArray(events).slice().sort(compareDomEvents);
    }
    function latestDomEvent(events) {
      const ordered = orderedDomEvents(events);
      return ordered[ordered.length - 1] || {};
    }
    function sliceContractRealAmount(event, type) {
      void type;
      const rawSize = rawEventAmount(event);
      if (Number.isFinite(rawSize)) return Math.max(0, rawSize);
      return 0;
    }
    function sliceTopContractTitle(item) {
      if (!item) return "";
      const lines = [
        `Order ID: ${item.orderId}`,
        `Price: ${item.price}`,
        `Type: ${item.typeLabel}`,
        `Real total: ${fmt(item.amount, 0)}`,
        `Events: ${fmt(item.count, 0)}`,
        `Max event size: ${fmt(item.maxSize, 0)}`,
        `Last event size: ${fmt(item.lastSize, 0)}`,
      ];
      if (item.breakdown) lines.push("", item.breakdown);
      return lines.join("\n");
    }
    function topContractCellHtml(item, field, className) {
      const title = item ? ` title="${escapeHtml(sliceTopContractTitle(item))}"` : "";
      const value = item ? item[field] : "";
      return `<span class="${className}"${title}>${escapeHtml(value)}</span>`;
    }
    function wrappedValueLines(value, indent = "  ", chunkSize = 26) {
      const text = String(value || "").trim();
      if (!text) return [`${indent}N/A`];
      if (text.length <= chunkSize) return [`${indent}${text}`];
      const lines = [];
      for (let index = 0; index < text.length; index += chunkSize) {
        lines.push(`${indent}${text.slice(index, index + chunkSize)}`);
      }
      return lines;
    }
    function numericMinMax(values) {
      let min = Infinity;
      let max = -Infinity;
      let count = 0;
      for (const value of values) {
        const parsed = Number(value);
        if (!Number.isFinite(parsed)) continue;
        if (parsed < min) min = parsed;
        if (parsed > max) max = parsed;
        count += 1;
      }
      return count ? { min, max } : null;
    }
    function decimalPlaces(value) {
      const text = String(value);
      if (text.includes("e-")) {
        return Math.min(10, Math.max(0, Number(text.split("e-")[1]) || 0));
      }
      const dot = text.indexOf(".");
      return dot >= 0 ? Math.min(10, text.length - dot - 1) : 0;
    }
    function priceTickIndex(price, tick) {
      return Math.round(num(price, NaN) / Math.max(tick, 0.00000001));
    }
    function priceLabelForTickIndex(index, tick) {
      return fmt(index * tick, decimalPlaces(tick));
    }
    function dateTimeLabel(ms) {
      if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) return "N/A";
      return new Date(Number(ms)).toLocaleString();
    }
    function isoDateForMs(ms) {
      if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) return "";
      return new Date(Number(ms)).toISOString().slice(0, 10);
    }
    function utcDayBoundsForMs(ms) {
      if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) return { startMs: 0, endMs: 0 };
      const date = new Date(Number(ms));
      const startMs = Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate());
      return { startMs, endMs: startMs + DAY_MS };
    }
    function axisTimeLabel(ms) {
      if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) return "";
      return new Date(Number(ms)).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
    }
    function axisDateLabel(ms) {
      if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) return "";
      return new Date(Number(ms)).toLocaleDateString([], { month: "2-digit", day: "2-digit" });
    }
    function dayKey(ms) {
      if (!Number.isFinite(Number(ms)) || Number(ms) <= 0) return "";
      return new Date(Number(ms)).toDateString();
    }
    const timeZoneFormatters = new Map();
    function formatterForTimeZone(timeZone) {
      const key = String(timeZone || "America/New_York");
      let formatter = timeZoneFormatters.get(key);
      if (!formatter) {
        formatter = new Intl.DateTimeFormat("en-US", {
          timeZone: key,
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          hourCycle: "h23",
        });
        timeZoneFormatters.set(key, formatter);
      }
      return formatter;
    }
    function timeZoneParts(ms, timeZone) {
      const parts = {};
      for (const item of formatterForTimeZone(timeZone).formatToParts(new Date(Number(ms)))) {
        if (item.type !== "literal") parts[item.type] = Number(item.value);
      }
      return {
        year: parts.year,
        month: parts.month,
        day: parts.day,
        hour: parts.hour,
        minute: parts.minute,
        second: parts.second,
      };
    }
    function zonedTimeToUtcMs(parts, timeZone) {
      const target = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second || 0);
      let guess = target;
      for (let i = 0; i < 4; i += 1) {
        const current = timeZoneParts(guess, timeZone);
        const currentAsUtc = Date.UTC(
          current.year,
          current.month - 1,
          current.day,
          current.hour,
          current.minute,
          current.second || 0,
        );
        const diff = target - currentAsUtc;
        if (Math.abs(diff) < 1) break;
        guess += diff;
      }
      return guess;
    }
    function addDaysToParts(parts, days) {
      const date = new Date(Date.UTC(parts.year, parts.month - 1, parts.day + days));
      return {
        year: date.getUTCFullYear(),
        month: date.getUTCMonth() + 1,
        day: date.getUTCDate(),
      };
    }
    function timeLabelForSession(hour, minute) {
      return `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")} NY`;
    }
    function renderLinks() {
      linksEl.innerHTML = TIMEFRAMES.map(timeframe => {
        const active = timeframe === ACTIVE_TIMEFRAME ? " active" : "";
        return `<a class="timeframe-link${active}" href="/dom/${timeframe}">${timeframe}</a>`;
      }).join("");
    }

    class CanvasDomTimelineChart {
      constructor() {
        this.ctx = canvas.getContext("2d");
        this.snapshot = null;
        this.session = null;
        this.view = { startMs: 0, endMs: 0, priceMin: NaN, priceMax: NaN };
        this.navigation = { startMs: 0, endMs: 0 };
        this.fullNavigation = { startMs: 0, endMs: 0 };
        this.globalNavigation = { startMs: 0, endMs: 0 };
        this.availableDates = [];
        this.selectedDateOverride = "";
        this.renderRange = { startMs: 0, endMs: 0 };
        this.autoScale = true;
        this.hover = null;
        this.mouse = null;
        this.visibleEvents = [];
        this.abortController = null;
        this.fetchTimer = 0;
        this.fetchGeneration = 0;
        this.syncingTimeScrollbar = false;
        this.timeScrollbarSyncTimer = 0;
        this.syncingDayScrollbar = false;
        this.syncingPriceScrollbar = false;
        this.inspectingTimeSlice = false;
        this.nySessionOnly = false;
        this.savedNonNyView = null;
        this.userTimeZoomed = false;
        this.manualTimeZoom = null;
        this.markerFilters = this.defaultMarkerFilters();
        this.markerPriceFilter = NaN;
        this.markerIdFilter = "";
        this.refillMinCount = null;
        this.refillOrderIdsCacheSession = null;
        this.refillOrderIdsCacheThreshold = null;
        this.refillOrderIdsCache = new Set();
        this.icebergMinContracts = null;
        this.icebergFetchTimer = 0;
        this.pendingIcebergPathFocus = false;
        this.timeSliceOrderIdMenus = [];
        this.sliceSessionCache = new Map();
        this.timeSliceRequestId = 0;
        this.dayPrefetchKeys = new Set();
        this.dayPrefetchTimer = 0;
        this.dayPrefetchControllers = [];
        this.resizeObserver = new ResizeObserver(() => this.resize());
        this.resizeObserver.observe(stage);
        canvas.addEventListener("mousemove", event => this.onMouseMove(event));
        canvas.addEventListener("mouseleave", () => this.clearHover());
        canvas.addEventListener("wheel", event => this.onWheel(event), { passive: false });
        window.addEventListener("keydown", event => this.onKeyDown(event));
        mainEl.addEventListener("pointerdown", event => this.onTimeSlicePointerDown(event));
        timeSlicePanelEl.addEventListener("click", event => this.onTimeSlicePanelClick(event));
        window.addEventListener("blur", () => this.closeTimeSlicePanel());
        dayScrollbarEl.addEventListener("scroll", () => this.onDayScrollbarScroll(), { passive: true });
        timeScrollbarEl.addEventListener("scroll", () => this.onTimeScrollbarScroll(), { passive: true });
        priceScrollbarEl.addEventListener("scroll", () => this.onPriceScrollbarScroll(), { passive: true });
        autoScaleEl.addEventListener("click", () => {
          this.autoScale = true;
          autoScaleEl.classList.add("active");
          this.applyAutoScale();
          this.syncPriceScrollbar();
          this.draw();
        });
        this.bindMarkerFilters();
        this.bindNySessionToggle();
        this.resize();
      }
      defaultMarkerFilters() {
        return {
          ADD: { enabled: true, min: null },
          CANCEL_DELETE: { enabled: true, min: null },
          MODIFY: { enabled: true, min: null },
          EXECUTE: { enabled: true, min: null },
          RESTING_LIQUIDITY: { enabled: true, min: null },
        };
      }
      bindMarkerFilters() {
        if (!markerFilterEl) return;
        markerFilterEl.querySelectorAll("[data-marker-filter]").forEach(input => {
          input.addEventListener("change", () => {
            const type = String(input.dataset.markerFilter || "");
            if (!this.markerFilters[type]) return;
            this.markerFilters[type].enabled = Boolean(input.checked);
            this.clearHover();
          });
        });
        markerFilterEl.querySelectorAll("[data-marker-threshold]").forEach(input => {
          input.addEventListener("input", () => {
            const type = String(input.dataset.markerThreshold || "");
            if (!this.markerFilters[type]) return;
            this.markerFilters[type].min = this.parseMarkerThreshold(input.value);
            this.clearHover();
          });
          input.addEventListener("blur", () => {
            if (!String(input.value || "").trim()) input.value = "all";
          });
        });
        const priceFilterInput = markerFilterEl.querySelector("[data-marker-price-filter]");
        if (priceFilterInput) {
          priceFilterInput.addEventListener("input", () => {
            this.markerPriceFilter = this.parseMarkerPriceFilter(priceFilterInput.value);
            this.clearHover();
          });
          priceFilterInput.addEventListener("blur", () => {
            if (!String(priceFilterInput.value || "").trim()) priceFilterInput.value = "all";
          });
        }
        const idFilterInput = markerFilterEl.querySelector("[data-marker-id-filter]");
        if (idFilterInput) {
          idFilterInput.addEventListener("input", () => {
            this.markerIdFilter = this.parseMarkerIdFilter(idFilterInput.value);
            this.clearHover();
          });
          idFilterInput.addEventListener("blur", () => {
            if (!String(idFilterInput.value || "").trim()) idFilterInput.value = "all";
          });
        }
        const refillFilterInput = markerFilterEl.querySelector("[data-marker-refill-filter]");
        if (refillFilterInput) {
          refillFilterInput.addEventListener("input", () => {
            this.refillMinCount = this.parseRefillThreshold(refillFilterInput.value);
            this.clearRefillFilterCache();
            this.clearHover();
          });
          refillFilterInput.addEventListener("blur", () => {
            if (!String(refillFilterInput.value || "").trim()) refillFilterInput.value = "all";
          });
        }
        const icebergFilterInput = markerFilterEl.querySelector("[data-marker-iceberg-filter]");
        if (icebergFilterInput) {
          icebergFilterInput.addEventListener("input", () => {
            this.icebergMinContracts = this.parseIcebergThreshold(icebergFilterInput.value);
            this.pendingIcebergPathFocus = this.hasActiveIcebergFilter();
            this.clearHover();
            this.scheduleIcebergRefetch();
          });
          icebergFilterInput.addEventListener("blur", () => {
            if (!String(icebergFilterInput.value || "").trim()) icebergFilterInput.value = "all";
          });
        }
        if (sliceRangeEnabledEl && sliceStartInputEl) {
          const syncSliceRangeInput = () => {
            sliceStartInputEl.disabled = !sliceRangeEnabledEl.checked;
          };
          sliceRangeEnabledEl.addEventListener("change", syncSliceRangeInput);
          syncSliceRangeInput();
        }
      }
      bindNySessionToggle() {
        if (!nySessionToggleEl) return;
        nySessionToggleEl.addEventListener("change", () => {
          this.setNySessionOnly(Boolean(nySessionToggleEl.checked));
        });
      }
      parseMarkerThreshold(value) {
        const raw = String(value || "").trim().toLowerCase();
        if (!raw || raw === "all") return null;
        const parsed = Number(raw);
        return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
      }
      parseMarkerPriceFilter(value) {
        const raw = String(value || "").trim().toLowerCase();
        if (!raw || raw === "all") return NaN;
        const parsed = Number(raw);
        return Number.isFinite(parsed) ? parsed : NaN;
      }
      parseMarkerIdFilter(value) {
        const raw = String(value || "").trim();
        return !raw || raw.toLowerCase() === "all" ? "" : raw;
      }
      parseIcebergThreshold(value) {
        const parsed = this.parseMarkerThreshold(value);
        return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
      }
      parseRefillThreshold(value) {
        const parsed = this.parseMarkerThreshold(value);
        return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
      }
      hasActiveRefillFilter() {
        return Number.isFinite(Number(this.refillMinCount)) && Number(this.refillMinCount) > 0;
      }
      clearRefillFilterCache() {
        this.refillOrderIdsCacheSession = null;
        this.refillOrderIdsCacheThreshold = null;
        this.refillOrderIdsCache = new Set();
      }
      refillFilterOrderIds() {
        const threshold = Number(this.refillMinCount);
        if (!this.hasActiveRefillFilter() || !this.session) return new Set();
        if (
          this.refillOrderIdsCacheSession === this.session
          && this.refillOrderIdsCacheThreshold === threshold
        ) {
          return this.refillOrderIdsCache;
        }
        const orderIds = new Set();
        for (const event of rawSessionEvents(this.session)) {
          const refillCount = Math.trunc(num(event?.positive_refill_count ?? event?.refill_count, 0));
          if (refillCount < threshold) continue;
          for (const value of [event?.order_id, event?.venue_order_id]) {
            const orderId = String(value || "").trim();
            if (orderId) orderIds.add(orderId);
          }
        }
        this.refillOrderIdsCacheSession = this.session;
        this.refillOrderIdsCacheThreshold = threshold;
        this.refillOrderIdsCache = orderIds;
        return orderIds;
      }
      matchesRefillOrderFilter(item) {
        if (!this.hasActiveRefillFilter()) return true;
        const orderIds = this.refillFilterOrderIds();
        if (!orderIds.size) return false;
        return [item?.order_id, item?.venue_order_id]
          .map(value => String(value || "").trim())
          .some(value => orderIds.has(value));
      }
      hasActiveIcebergFilter() {
        return Number.isFinite(Number(this.icebergMinContracts)) && Number(this.icebergMinContracts) > 0;
      }
      icebergFilterOrderIds() {
        return new Set(
          safeArray(this.session?.iceberg_filter?.order_ids)
            .map(value => String(value || "").trim())
            .filter(Boolean)
        );
      }
      icebergOrderIdsForRequest() {
        if (!this.hasActiveIcebergFilter() || this.pendingIcebergPathFocus) return [];
        return [...this.icebergFilterOrderIds()];
      }
      icebergPathBoundsForRequest() {
        if (!this.hasActiveIcebergFilter() || this.pendingIcebergPathFocus) return null;
        const startMs = num(this.session?.iceberg_filter?.path_start_ms, NaN);
        const endMs = num(this.session?.iceberg_filter?.path_end_ms, NaN);
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return null;
        return { startMs, endMs };
      }
      matchesIcebergOrderFilter(item) {
        if (!this.hasActiveIcebergFilter()) return true;
        const orderIds = this.icebergFilterOrderIds();
        if (!orderIds.size) return false;
        return [item?.order_id, item?.venue_order_id]
          .map(value => String(value || "").trim())
          .some(value => orderIds.has(value));
      }
      scheduleIcebergRefetch() {
        window.clearTimeout(this.icebergFetchTimer);
        this.icebergFetchTimer = window.setTimeout(() => {
          fetchDomData(this.view, { showLoading: true, includePriceRange: false });
        }, 250);
      }
      resize() {
        const rect = stage.getBoundingClientRect();
        const dpr = window.devicePixelRatio || 1;
        canvas.width = Math.max(1, Math.floor(rect.width * dpr));
        canvas.height = Math.max(1, Math.floor(rect.height * dpr));
        canvas.style.width = `${rect.width}px`;
        canvas.style.height = `${rect.height}px`;
        this.ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        this.syncDayScrollbar();
        this.syncPriceScrollbar();
        this.draw();
      }
      setSnapshot(snapshot) {
        this.snapshot = snapshot;
        this.session = safeArray(snapshot?.sessions)[0] || null;
        if (!this.session) {
          this.draw();
          this.disableTimeScrollbars();
          this.disableDayScrollbar();
          this.disablePriceScrollbar();
          return;
        }
        this.globalNavigation = this.globalNavigationForSession(this.session);
        this.availableDates = safeArray(this.session.available_dates)
          .map(value => String(value || "").trim())
          .filter(Boolean)
          .sort();
        this.fullNavigation = this.fullNavigationForSession(this.session);
        this.navigation = this.fullNavigation;
        if (this.session.selected_date && dateInputEl.value !== this.session.selected_date) {
          dateInputEl.value = this.session.selected_date;
        }
        const serverStart = num(this.session.window_start_ms);
        const serverEnd = num(this.session.window_end_ms);
        const initialSpan = this.initialViewSpanMs();

        const hasExistingView =
          Number.isFinite(this.view.startMs) &&
          Number.isFinite(this.view.endMs) &&
          this.view.endMs > this.view.startMs;
        const serverSpan = Math.max(1, serverEnd - serverStart);
        const targetInitialSpan = Math.min(serverSpan, initialSpan);

        if (!hasExistingView || this.view.startMs <= 0 || this.view.endMs <= 0) {
          this.view.startMs = serverStart;
          this.view.endMs = Math.min(serverEnd, serverStart + targetInitialSpan);
        } else if (!this.userTimeZoomed && (this.view.endMs - this.view.startMs) < targetInitialSpan * 0.5) {
          const center = (this.view.startMs + this.view.endMs) / 2;
          this.view.startMs = Math.max(serverStart, Math.min(serverEnd - targetInitialSpan, center - targetInitialSpan / 2));
          this.view.endMs = this.view.startMs + targetInitialSpan;
        }
        if (this.userTimeZoomed) {
          this.applyManualTimeZoom();
        }
        this.renderRange = {
          startMs: num(this.session.render_start_ms, this.view.startMs),
          endMs: num(this.session.render_end_ms, this.view.endMs),
        };

        if (this.pendingIcebergPathFocus) {
          this.applyIcebergPathView();
          this.pendingIcebergPathFocus = false;
        }
        if (this.renderRange.startMs > this.view.startMs || this.renderRange.endMs < this.view.endMs) {
          this.renderRange = {
            startMs: this.view.startMs,
            endMs: this.view.endMs,
          };
        }
        this.clampTimeView({ allowGlobal: true });
        this.applyVerticalPriceScale();
        this.syncDayScrollbar();
        this.syncTimeScrollbar();
        this.syncPriceScrollbar();
        this.draw();
      }
      applyIcebergPathView() {
        if (!this.hasActiveIcebergFilter() || !this.session?.iceberg_filter?.active) return false;
        const startMs = num(this.session.iceberg_filter.path_start_ms, NaN);
        const endMs = num(this.session.iceberg_filter.path_end_ms, NaN);
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) return false;
        const span = Math.max(1, endMs - startMs);
        const pad = Math.max(1000, Math.min(60000, span * 0.06));
        const navStart = num(this.session.navigation_start_ms, startMs);
        const navEnd = num(this.session.navigation_end_ms, endMs);
        this.view.startMs = Math.max(navStart, startMs - pad);
        this.view.endMs = Math.min(navEnd, endMs + pad);
        this.userTimeZoomed = true;
        return true;
      }
      initialViewSpanMs() {
        const tfMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60000;
        const candles = Math.max(1, Math.round(num(this.session?.initial_view_candles, 3)));
        return tfMs * candles;
      }
      rememberManualTimeZoom() {
        if (!Number.isFinite(this.view.startMs) || !Number.isFinite(this.view.endMs) || this.view.endMs <= this.view.startMs) return;
        this.manualTimeZoom = {
          centerMs: (this.view.startMs + this.view.endMs) / 2,
          spanMs: Math.max(1, this.view.endMs - this.view.startMs),
        };
      }
      rememberUserTimeView() {
        this.userTimeZoomed = true;
        this.rememberManualTimeZoom();
      }
      applyManualTimeZoom() {
        if (!this.manualTimeZoom) return false;
        const span = Math.max(1, num(this.manualTimeZoom.spanMs, 0));
        const center = num(this.manualTimeZoom.centerMs, NaN);
        if (!Number.isFinite(center) || !Number.isFinite(span)) return false;
        this.view.startMs = center - span / 2;
        this.view.endMs = center + span / 2;
        return true;
      }
      fullNavigationForSession(session) {
        const viewStart = num(session.window_start_ms);
        const viewEnd = num(session.window_end_ms);
        const startMs = num(session.navigation_start_ms, viewStart);
        const endMs = num(session.navigation_end_ms, viewEnd);
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
          return { startMs: viewStart, endMs: viewEnd };
        }
        return { startMs, endMs };
      }
      globalNavigationForSession(session) {
        const viewStart = num(session.window_start_ms);
        const viewEnd = num(session.window_end_ms);
        const startMs = num(session.global_navigation_start_ms, num(session.navigation_start_ms, viewStart));
        const endMs = num(session.global_navigation_end_ms, num(session.navigation_end_ms, viewEnd));
        if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
          return this.fullNavigationForSession(session);
        }
        return { startMs, endMs };
      }
      navigationForSession(session) {
        const fullStart = num(session.navigation_start_ms, num(session.window_start_ms));
        const fullEnd = num(session.navigation_end_ms, num(session.window_end_ms));
        const viewStart = num(session.window_start_ms);
        const viewEnd = num(session.window_end_ms);
        const span = Math.max(1, viewEnd - viewStart);
        const rawDataStart = num(session.earliest_window_start_ms, NaN);
        const rawDataEnd = num(session.latest_window_end_ms, NaN);
        const hasDataBounds = (
          Number.isFinite(rawDataStart)
          && Number.isFinite(rawDataEnd)
          && rawDataStart > 0
          && rawDataEnd > 0
          && rawDataEnd >= rawDataStart
        );
        if (!Number.isFinite(fullStart) || !Number.isFinite(fullEnd) || fullEnd <= fullStart) {
          return { startMs: viewStart, endMs: viewEnd };
        }
        if (!hasDataBounds) {
          const currentStart = num(this.navigation.startMs, NaN);
          const currentEnd = num(this.navigation.endMs, NaN);
          if (Number.isFinite(currentStart) && Number.isFinite(currentEnd) && currentEnd > currentStart) {
            return {
              startMs: Math.max(fullStart, Math.min(currentStart, viewStart - span * 2)),
              endMs: Math.min(fullEnd, Math.max(currentEnd, viewEnd + span * 2)),
            };
          }
          return {
            startMs: Math.max(fullStart, viewStart - span * 2),
            endMs: Math.min(fullEnd, viewEnd + Math.max(span * 4, 60000)),
          };
        }
        const dataStart = rawDataStart;
        const dataEnd = rawDataEnd;
        const leftPad = span * 2;
        const rightPad = Math.max(span * 18, 60000);
        const startMs = Math.max(fullStart, Math.min(viewStart, dataStart) - leftPad);
        let endMs = Math.min(fullEnd, Math.max(viewEnd, dataEnd) + rightPad);
        if (endMs - startMs <= span) {
          endMs = Math.min(fullEnd, startMs + span * 4);
        }
        return { startMs, endMs };
      }
      applyAutoScale() {
        if (!this.autoScale || !this.session) return;
        this.applyVerticalPriceScale();
      }
      applyVerticalPriceScale() {
        if (!this.session) return;
        const tick = Math.max(num(this.session.tick_size, 0.25), 0.00000001);
        const quotePrices = [];
        for (const point of this.bestLineForView(safeArray(this.session.best_bid_line))) {
          quotePrices.push(num(point.price, NaN));
        }
        for (const point of this.bestLineForView(safeArray(this.session.best_ask_line))) {
          quotePrices.push(num(point.price, NaN));
        }
        const quoteBounds = numericMinMax(quotePrices);
        if (quoteBounds) {
          const quoteSpan = Math.max(tick, quoteBounds.max - quoteBounds.min);
          const pad = Math.max(tick * 20, quoteSpan * 2);
          this.view.priceMin = quoteBounds.min - pad;
          this.view.priceMax = quoteBounds.max + pad;
          return;
        }
        const prices = [];
        for (const event of rawSessionEvents(this.session)) {
          const ms = num(event.timestamp_ms);
          if (ms >= this.view.startMs && ms <= this.view.endMs) prices.push(num(event.price, NaN));
        }
        for (const segment of safeArray(this.session.resting_segments)) {
          const start = num(segment.start_ms);
          const end = num(segment.end_ms);
          if (end >= this.view.startMs && start <= this.view.endMs) prices.push(num(segment.price, NaN));
        }
        for (const point of this.bestLineForView(safeArray(this.session.best_bid_line))) {
          prices.push(num(point.price, NaN));
        }
        for (const point of this.bestLineForView(safeArray(this.session.best_ask_line))) {
          prices.push(num(point.price, NaN));
        }
        const finite = prices.filter(Number.isFinite);
        if (!finite.length) {
          const fallbackMin = Number.isFinite(this.view.priceMin) ? this.view.priceMin : 0;
          const fallbackMax = Number.isFinite(this.view.priceMax) ? this.view.priceMax : tick * 20;
          this.view.priceMin = Math.min(fallbackMin, fallbackMax);
          this.view.priceMax = Math.max(fallbackMin, fallbackMax);
          return;
        }
        const bounds = numericMinMax(finite);
        if (!bounds) return;
        const min = bounds.min;
        const max = bounds.max;
        const pad = Math.max(tick * 4, (max - min) * 0.08);
        this.view.priceMin = min - pad;
        this.view.priceMax = max + pad;
      }
      layout() {
        const rect = stage.getBoundingClientRect();
        return {
          width: rect.width,
          height: rect.height,
          left: 88,
          right: 18,
          top: 16,
          bottom: 190,
        };
      }
      plot(l) {
        return {
          x: l.left,
          y: l.top,
          w: Math.max(1, l.width - l.left - l.right),
          h: Math.max(1, l.height - l.top - l.bottom),
        };
      }
      xForTime(ms, plot) {
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        return plot.x + ((Number(ms) - this.view.startMs) / span) * plot.w;
      }
      timeForX(x, plot) {
        const ratio = (Number(x) - plot.x) / Math.max(1, plot.w);
        return this.view.startMs + Math.max(0, Math.min(1, ratio)) * (this.view.endMs - this.view.startMs);
      }
      yForPrice(price, plot) {
        const span = Math.max(0.00000001, this.view.priceMax - this.view.priceMin);
        return plot.y + plot.h - ((Number(price) - this.view.priceMin) / span) * plot.h;
      }
      priceForY(y, plot) {
        const ratio = 1 - ((Number(y) - plot.y) / Math.max(1, plot.h));
        return this.view.priceMin + ratio * (this.view.priceMax - this.view.priceMin);
      }
      nySessionConfig() {
        const session = this.session || {};
        return {
          timeZone: String(session.new_york_session_timezone || "America/New_York"),
          startHour: Math.max(0, Math.min(23, Math.trunc(num(session.new_york_session_start_hour, 9)))),
          startMinute: Math.max(0, Math.min(59, Math.trunc(num(session.new_york_session_start_minute, 30)))),
          endHour: Math.max(0, Math.min(23, Math.trunc(num(session.new_york_session_end_hour, 16)))),
          endMinute: Math.max(0, Math.min(59, Math.trunc(num(session.new_york_session_end_minute, 0)))),
        };
      }
      nySessionBoundsForParts(parts) {
        const config = this.nySessionConfig();
        const startMs = zonedTimeToUtcMs({
          ...parts,
          hour: config.startHour,
          minute: config.startMinute,
          second: 0,
        }, config.timeZone);
        let endParts = {
          ...parts,
          hour: config.endHour,
          minute: config.endMinute,
          second: 0,
        };
        let endMs = zonedTimeToUtcMs(endParts, config.timeZone);
        if (endMs <= startMs) {
          endParts = { ...addDaysToParts(parts, 1), hour: config.endHour, minute: config.endMinute, second: 0 };
          endMs = zonedTimeToUtcMs(endParts, config.timeZone);
        }
        return { startMs, endMs };
      }
      nySessionBoundsForMs(ms) {
        const config = this.nySessionConfig();
        const parts = timeZoneParts(ms, config.timeZone);
        return this.nySessionBoundsForParts(parts);
      }
      currentNySessionBounds() {
        const reference = Number.isFinite(this.view.startMs) && this.view.startMs > 0
          ? this.view.startMs
          : num(this.session?.window_start_ms, Date.now());
        const config = this.nySessionConfig();
        const referenceParts = timeZoneParts(reference, config.timeZone);
        const fullStart = num(this.fullNavigation.startMs, this.view.startMs);
        const fullEnd = num(this.fullNavigation.endMs, this.view.endMs);
        let firstOverlapping = null;
        for (let offset = -1; offset <= 2; offset += 1) {
          const bounds = this.nySessionBoundsForParts(addDaysToParts(referenceParts, offset));
          const overlapsFullNavigation = bounds.endMs >= fullStart && bounds.startMs <= fullEnd;
          if (reference >= bounds.startMs && reference <= bounds.endMs && overlapsFullNavigation) return bounds;
          if (!firstOverlapping && overlapsFullNavigation) firstOverlapping = bounds;
        }
        return firstOverlapping || this.nySessionBoundsForMs(reference);
      }
      effectiveTimeNavigation() {
        const fullStart = num(this.fullNavigation.startMs, this.view.startMs);
        const fullEnd = num(this.fullNavigation.endMs, this.view.endMs);
        if (!this.nySessionOnly) return { startMs: fullStart, endMs: fullEnd };
        const bounds = this.currentNySessionBounds();
        const startMs = Math.max(fullStart, bounds.startMs);
        const endMs = Math.min(fullEnd, bounds.endMs);
        if (Number.isFinite(startMs) && Number.isFinite(endMs) && endMs > startMs) {
          return { startMs, endMs };
        }
        return bounds;
      }
      setNySessionOnly(enabled) {
        if (!this.session) {
          this.nySessionOnly = enabled;
          return;
        }
        if (enabled && !this.nySessionOnly) {
          this.savedNonNyView = { ...this.view };
        }
        this.nySessionOnly = enabled;
        if (!enabled && this.savedNonNyView) {
          this.view = { ...this.savedNonNyView };
          this.savedNonNyView = null;
        } else if (enabled) {
          const bounds = this.effectiveTimeNavigation();
          const sessionSpan = Math.max(1, bounds.endMs - bounds.startMs);
          const currentSpan = Math.max(1, this.view.endMs - this.view.startMs);
          const span = Math.min(currentSpan, sessionSpan);
          const overlaps = this.view.endMs > bounds.startMs && this.view.startMs < bounds.endMs;
          if (!overlaps || this.view.startMs < bounds.startMs || this.view.endMs > bounds.endMs) {
            this.view.startMs = Math.max(bounds.startMs, Math.min(bounds.endMs - span, this.view.startMs));
            this.view.endMs = this.view.startMs + span;
          }
        }
        this.clampTimeView({ allowGlobal: true });
        this.syncDateToViewStart();
        this.applyVerticalPriceScale();
        this.syncDayScrollbar();
        this.syncTimeScrollbar();
        this.syncPriceScrollbar();
        this.draw();
        this.requestTimeFetchIfNeeded(0);
      }
      nySessionSegmentsForRange(startMs, endMs) {
        const config = this.nySessionConfig();
        const startParts = timeZoneParts(startMs, config.timeZone);
        const segments = [];
        for (let offset = -1; offset <= 2; offset += 1) {
          const parts = addDaysToParts(startParts, offset);
          const bounds = this.nySessionBoundsForParts(parts);
          if (bounds.endMs >= startMs && bounds.startMs <= endMs) segments.push(bounds);
        }
        return segments;
      }
      renderNySessionStrip() {
        if (!nySessionStripEl || !this.session || !Number.isFinite(this.view.startMs) || !Number.isFinite(this.view.endMs)) {
          if (nySessionStripEl) nySessionStripEl.innerHTML = "";
          return;
        }
        const rangeStart = num(this.view.startMs, NaN);
        const rangeEnd = num(this.view.endMs, NaN);
        if (!Number.isFinite(rangeStart) || !Number.isFinite(rangeEnd) || rangeEnd <= rangeStart) {
          nySessionStripEl.innerHTML = "";
          return;
        }
        const span = Math.max(1, rangeEnd - rangeStart);
        const config = this.nySessionConfig();
        const startLabel = timeLabelForSession(config.startHour, config.startMinute);
        const endLabel = timeLabelForSession(config.endHour, config.endMinute);
        const html = [];
        for (const segment of this.nySessionSegmentsForRange(rangeStart, rangeEnd)) {
          const left = Math.max(0, Math.min(100, ((segment.startMs - rangeStart) / span) * 100));
          const right = Math.max(0, Math.min(100, ((segment.endMs - rangeStart) / span) * 100));
          const width = Math.max(0, right - left);
          if (width > 0) {
            html.push(`<div class="ny-session-segment" style="left:${left.toFixed(4)}%;width:${width.toFixed(4)}%"></div>`);
          }
          if (segment.startMs >= rangeStart && segment.startMs <= rangeEnd) {
            html.push(`<div class="ny-session-boundary" style="left:${left.toFixed(4)}%"></div>`);
            html.push(`<div class="ny-session-label" style="left:${left.toFixed(4)}%">${escapeHtml(startLabel)}</div>`);
          }
          if (segment.endMs >= rangeStart && segment.endMs <= rangeEnd) {
            html.push(`<div class="ny-session-boundary" style="left:${right.toFixed(4)}%"></div>`);
            html.push(`<div class="ny-session-label" style="left:${right.toFixed(4)}%">${escapeHtml(endLabel)}</div>`);
          }
        }
        nySessionStripEl.innerHTML = html.join("");
      }
      markerFilterAmountForEvent(event) {
        const type = String(event?.event_type || "");
        const rawAmount = rawEventAmount(event);
        if (Number.isFinite(rawAmount)) return Math.max(0, rawAmount);
        if (type === "ADD") return Math.max(0, num(event.added_contracts, 0));
        if (type === "CANCEL_DELETE") return Math.max(0, num(event.canceled_contracts, 0));
        if (type === "MODIFY") return Math.abs(num(event.modified_delta, 0));
        if (type === "EXECUTE") return Math.max(0, num(event.executed_contracts, 0));
        return 0;
      }
      markerFilterAmountForSegment(segment) {
        return Math.max(0, num(segment?.order_size, 0));
      }
      passesMarkerFilter(type, amount) {
        const filter = this.markerFilters[String(type || "")];
        if (!filter) return true;
        if (!filter.enabled) return false;
        if (!Number.isFinite(filter.min)) return true;
        return num(amount, 0) >= filter.min;
      }
      matchesMarkerPriceFilter(item) {
        if (!Number.isFinite(this.markerPriceFilter)) return true;
        const price = num(item?.price, NaN);
        if (!Number.isFinite(price)) return false;
        const tick = Math.max(num(this.session?.tick_size, 0.25), 0.00000001);
        return priceTickIndex(price, tick) === priceTickIndex(this.markerPriceFilter, tick);
      }
      matchesMarkerIdFilter(item) {
        const expected = String(this.markerIdFilter || "").trim();
        if (!expected) return true;
        return [item?.order_id, item?.venue_order_id]
          .map(value => String(value || "").trim())
          .some(value => value === expected);
      }
      shouldShowEvent(event) {
        if (this.hasActiveIcebergFilter()) {
          return this.matchesIcebergOrderFilter(event);
        }
        const type = String(event?.event_type || "");
        return this.passesMarkerFilter(type, this.markerFilterAmountForEvent(event))
          && this.matchesMarkerPriceFilter(event)
          && this.matchesMarkerIdFilter(event)
          && this.matchesRefillOrderFilter(event);
      }
      shouldShowRestingSegment(segment) {
        if (this.hasActiveIcebergFilter()) {
          return this.matchesIcebergOrderFilter(segment);
        }
        return this.passesMarkerFilter("RESTING_LIQUIDITY", this.markerFilterAmountForSegment(segment))
          && this.matchesMarkerPriceFilter(segment)
          && this.matchesMarkerIdFilter(segment)
          && this.matchesRefillOrderFilter(segment);
      }
      draw() {
        const ctx = this.ctx;
        const l = this.layout();
        const p = this.plot(l);
        ctx.clearRect(0, 0, l.width, l.height);
        ctx.fillStyle = "#0b0f14";
        ctx.fillRect(0, 0, l.width, l.height);
        if (!this.session) {
          this.renderNySessionStrip();
          renderBook(null);
          this.drawEmpty("No DOM files found");
          return;
        }
        const message = this.session.message || "";
        if (
          !rawSessionEvents(this.session).length
          && !safeArray(this.session.resting_segments).length
          && !safeArray(this.session.best_bid_line).length
          && !safeArray(this.session.best_ask_line).length
        ) {
          this.drawGrid(ctx, p);
          this.drawAxes(ctx, p);
          this.drawMouseGuides(ctx, p);
          renderBook(this.session, this);
          this.renderNySessionStrip();
          this.drawEmpty(message || "No DOM events found");
          return;
        }
        this.drawGrid(ctx, p);
        this.drawRestingSegments(ctx, p);
        this.drawBestLine(ctx, p, safeArray(this.session.best_bid_line), "#138a5e", "BID");
        this.drawBestLine(ctx, p, safeArray(this.session.best_ask_line), "#b63f3f", "ASK");
        this.drawEvents(ctx, p);
        this.drawAxes(ctx, p);
        this.drawMouseGuides(ctx, p);
        renderBook(this.session, this);
        this.renderNySessionStrip();
      }
      drawEmpty(text) {
        const l = this.layout();
        this.ctx.fillStyle = "#8e9aaa";
        this.ctx.font = "700 14px Segoe UI, Arial";
        this.ctx.textAlign = "center";
        this.ctx.fillText(text, l.width / 2, l.height / 2);
      }
      drawGrid(ctx, p) {
        ctx.strokeStyle = "rgba(39,49,61,.7)";
        ctx.lineWidth = 1;
        ctx.beginPath();
        for (let i = 0; i <= 8; i += 1) {
          const x = p.x + (p.w * i / 8);
          ctx.moveTo(x, p.y);
          ctx.lineTo(x, p.y + p.h);
        }
        for (let i = 0; i <= 10; i += 1) {
          const y = p.y + (p.h * i / 10);
          ctx.moveTo(p.x, y);
          ctx.lineTo(p.x + p.w, y);
        }
        ctx.stroke();
      }
      drawAxes(ctx, p) {
        ctx.fillStyle = "#d8e4f2";
        ctx.font = "150 18px Segoe UI, Arial";
        ctx.textAlign = "right";
        ctx.textBaseline = "middle";
        for (let i = 0; i <= 10; i += 1) {
          const price = this.view.priceMax - ((this.view.priceMax - this.view.priceMin) * i / 10);
          ctx.fillText(fmt(price, 2), p.x - 8, p.y + (p.h * i / 10));
        }
        ctx.fillStyle = "#ffffff";
        ctx.font = "700 15px Segoe UI, Arial";
        ctx.textAlign = "center";
        ctx.textBaseline = "top";
        let previousDay = "";
        for (let i = 0; i <= 5; i += 1) {
          const ms = this.view.startMs + ((this.view.endMs - this.view.startMs) * i / 5);
          const x = p.x + (p.w * i / 5);
          const currentDay = dayKey(ms);
          ctx.fillText(axisTimeLabel(ms), x, p.y + p.h + 6);
          if (i === 0 || currentDay !== previousDay) {
            ctx.fillStyle = "#b9c7d8";
            ctx.fillText(axisDateLabel(ms), x, p.y + p.h + 24);
            ctx.fillStyle = "#ffffff";
          }
          previousDay = currentDay;
        }
      }
      drawRestingSegments(ctx, p) {
        ctx.lineCap = "round";
        for (const segment of safeArray(this.session.resting_segments)) {
          const start = num(segment.start_ms);
          const end = num(segment.end_ms);
          const price = num(segment.price, NaN);
          if (!this.shouldShowRestingSegment(segment)) continue;
          if (!Number.isFinite(price) || end < this.view.startMs || start > this.view.endMs) continue;
          const x1 = this.xForTime(Math.max(start, this.view.startMs), p);
          const x2 = this.xForTime(Math.min(end, this.view.endMs), p);
          const y = this.yForPrice(price, p);
          const size = Math.max(1, Math.min(9, Math.sqrt(num(segment.order_size, 1))));
          ctx.strokeStyle = segment.side === "BID" ? "rgba(55,201,135,.36)" : "rgba(255,107,107,.36)";
          ctx.lineWidth = size;
          ctx.beginPath();
          ctx.moveTo(x1, y);
          ctx.lineTo(x2, y);
          ctx.stroke();
        }
      }
      drawBestLine(ctx, p, points, color, label) {
        const drawable = this.bestLineForView(points);
        if (!drawable.length) return;
        ctx.strokeStyle = color;
        ctx.lineWidth = 4;
        ctx.shadowColor = color;
        ctx.shadowBlur = 4;
        ctx.beginPath();
        drawable.forEach((point, index) => {
          const x = this.xForTime(num(point.timestamp_ms), p);
          const y = this.yForPrice(num(point.price), p);
          if (index === 0) {
            ctx.moveTo(x, y);
          } else {
            const previous = drawable[index - 1];
            const previousY = this.yForPrice(num(previous.price), p);
            ctx.lineTo(x, previousY);
            ctx.lineTo(x, y);
          }
        });
        ctx.stroke();
        ctx.shadowBlur = 0;
        const last = drawable[drawable.length - 1];
        const y = this.yForPrice(num(last.price), p);
        if (Number.isFinite(y) && y >= p.y && y <= p.y + p.h) {
          ctx.fillStyle = color;
          ctx.font = "700 12px Segoe UI, Arial";
          ctx.textAlign = "right";
          ctx.textBaseline = "middle";
          ctx.fillText(`${label} ${last.price}`, p.x + p.w - 8, y - 8);
        }
      }
      bestLineForView(points) {
        const ordered = safeArray(points)
          .map(point => ({ timestamp_ms: num(point.timestamp_ms, NaN), price: point.price }))
          .filter(point => Number.isFinite(point.timestamp_ms) && Number.isFinite(num(point.price, NaN)))
          .sort((a, b) => a.timestamp_ms - b.timestamp_ms);
        if (!ordered.length) return [];
        const result = [];
        let previous = null;
        for (const point of ordered) {
          if (point.timestamp_ms <= this.view.startMs) previous = point;
          if (point.timestamp_ms > this.view.startMs) break;
        }
        if (previous) {
          result.push({ timestamp_ms: this.view.startMs, price: previous.price });
        }
        for (const point of ordered) {
          if (point.timestamp_ms >= this.view.startMs && point.timestamp_ms <= this.view.endMs) {
            result.push(point);
          }
        }
        if (!result.length) {
          const next = ordered.find(point => point.timestamp_ms > this.view.startMs);
          if (!next || next.timestamp_ms > this.view.endMs) return [];
          result.push(next);
        }
        const last = result[result.length - 1];
        if (last.timestamp_ms < this.view.endMs) {
          result.push({ timestamp_ms: this.view.endMs, price: last.price });
        }
        if (result.length === 1) {
          result.unshift({ timestamp_ms: this.view.startMs, price: result[0].price });
          result.push({ timestamp_ms: this.view.endMs, price: result[0].price });
        }
        return result;
      }
      eventPointGroups(events, p) {
        const groups = new Map();
        for (const event of events) {
          const ms = num(event.timestamp_ms);
          const price = num(event.price, NaN);
          const x = this.xForTime(ms, p);
          const y = this.yForPrice(price, p);
          const type = String(event.event_type || "");
          const side = String(event.side || "");
          const key = `${type}|${side}|${Math.round(x)}|${Math.round(y)}`;
          let group = groups.get(key);
          if (!group) {
            group = { x, y, events: [], rawTotal: 0 };
            groups.set(key, group);
          }
          group.events.push(event);
          group.rawTotal += Math.max(0, num(rawEventAmount(event), 0));
        }
        return [...groups.values()].map(group => {
          const events = orderedDomEvents(group.events);
          const last = latestDomEvent(events);
          const lastX = this.xForTime(num(last.timestamp_ms, NaN), p);
          const lastY = this.yForPrice(num(last.price, NaN), p);
          return {
            x: Number.isFinite(lastX) ? lastX : group.x,
            y: Number.isFinite(lastY) ? lastY : group.y,
            rawTotal: group.rawTotal,
            event: {
              ...last,
              aggregate_events: events,
              order_size: group.rawTotal,
              raw_total_contracts: group.rawTotal,
              event_count: events.length,
              last_order_size: num(rawEventAmount(last), 0),
            },
          };
        });
      }
      drawEvents(ctx, p) {
        this.visibleEvents = [];
        const visible = rawSessionEvents(this.session).filter(event => {
          const ms = num(event.timestamp_ms);
          const price = num(event.price, NaN);
          return Number.isFinite(price)
            && ms >= this.view.startMs
            && ms <= this.view.endMs
            && price >= this.view.priceMin
            && price <= this.view.priceMax
            && this.shouldShowEvent(event);
        });
        const points = this.eventPointGroups(visible, p);
        let maxOrderSize = 1;
        for (const point of points) {
          maxOrderSize = Math.max(maxOrderSize, Math.max(1, num(point.rawTotal, 1)));
        }
        for (const point of points) {
          const event = point.event;
          const x = point.x;
          const y = point.y;
          const relativeSize = Math.sqrt(Math.max(1, num(point.rawTotal, 1)) / maxOrderSize);
          const radius = Math.max(2.5, Math.min(13, 2.5 + relativeSize * 10.5));
          this.visibleEvents.push({ event, x, y, radius });
          this.drawEventMarker(ctx, event, x, y, radius);
        }
      }
      drawEventMarker(ctx, event, x, y, radius) {
        const type = String(event.event_type || "");
        const side = String(event.side || "");
        ctx.lineWidth = 1.5;
        if (type === "ADD") {
          ctx.fillStyle = side === "BID" ? "rgba(55,201,135,.78)" : "rgba(255,107,107,.78)";
          ctx.beginPath();
          ctx.arc(x, y, radius, 0, Math.PI * 2);
          ctx.fill();
          return;
        }
        if (type === "CANCEL_DELETE") {
          ctx.strokeStyle = "#b98cff";
          ctx.beginPath();
          ctx.moveTo(x - radius, y - radius);
          ctx.lineTo(x + radius, y + radius);
          ctx.moveTo(x + radius, y - radius);
          ctx.lineTo(x - radius, y + radius);
          ctx.stroke();
          return;
        }
        if (type === "MODIFY") {
          ctx.fillStyle = "#f2c94c";
          ctx.beginPath();
          ctx.moveTo(x, y - radius);
          ctx.lineTo(x + radius, y);
          ctx.lineTo(x, y + radius);
          ctx.lineTo(x - radius, y);
          ctx.closePath();
          ctx.fill();
          return;
        }
        if (type === "EXECUTE") {
          this.drawExecuteArrow(ctx, x, y, radius, side);
        }
      }
      drawExecuteArrow(ctx, x, y, radius, side) {
        const isAsk = side === "ASK";
        const isBid = side === "BID";
        const direction = isAsk ? 1 : -1;
        const color = isAsk ? "rgba(55,201,135,.94)" : (isBid ? "rgba(255,107,107,.94)" : "rgba(122,167,255,.94)");
        const head = Math.max(4, radius * 1.15);
        const half = Math.max(3, radius * 0.72);
        const stem = Math.max(5, radius * 1.2);
        const stemHalf = Math.max(1.3, radius * 0.22);
        const baseY = y + (head * direction);
        const tailY = baseY + (stem * direction);
        ctx.fillStyle = color;
        ctx.beginPath();
        ctx.moveTo(x, y);
        ctx.lineTo(x - half, baseY);
        ctx.lineTo(x - stemHalf, baseY);
        ctx.lineTo(x - stemHalf, tailY);
        ctx.lineTo(x + stemHalf, tailY);
        ctx.lineTo(x + stemHalf, baseY);
        ctx.lineTo(x + half, baseY);
        ctx.closePath();
        ctx.fill();
      }
      drawMouseGuides(ctx, p) {
        this.drawMousePriceLabel(ctx, p);
        this.drawMouseTimeLabel(ctx, p);
      }
      drawMousePriceLabel(ctx, p) {
        if (!this.mouse) return;
        const x = this.mouse.x;
        const y = this.mouse.y;
        if (x < p.x || x > p.x + p.w || y < p.y || y > p.y + p.h) return;
        const price = this.priceForY(y, p);
        const label = fmt(price, 2);
        ctx.save();
        ctx.strokeStyle = "rgba(88,166,255,.55)";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(p.x, y);
        ctx.lineTo(p.x + p.w, y);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.font = "150 20px Segoe UI, Arial";
        const boxW = Math.max(78, ctx.measureText(label).width + 18);
        const boxH = 30;
        const boxX = p.x + 4;
        const boxY = Math.max(p.y + 2, Math.min(p.y + p.h - boxH - 2, y - boxH / 2));
        ctx.fillStyle = "#1f6feb";
        ctx.fillRect(boxX, boxY, boxW, boxH);
        ctx.strokeStyle = "#8cc8ff";
        ctx.strokeRect(boxX, boxY, boxW, boxH);
        ctx.fillStyle = "#ffffff";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(label, boxX + boxW / 2, boxY + boxH / 2);
        ctx.restore();
      }
      drawMouseTimeLabel(ctx, p) {
        if (!this.mouse) return;
        const x = this.mouse.x;
        const y = this.mouse.y;
        if (x < p.x || x > p.x + p.w || y < p.y || y > p.y + p.h) return;
        const timestamp = this.timeForX(x, p);
        const label = `${axisDateLabel(timestamp)} ${axisTimeLabel(timestamp)}`;
        ctx.save();
        ctx.strokeStyle = "rgba(88,166,255,.55)";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(x, p.y);
        ctx.lineTo(x, p.y + p.h);
        ctx.stroke();
        ctx.setLineDash([]);
        ctx.font = "150 20px Segoe UI, Arial";
        const boxW = Math.max(150, ctx.measureText(label).width + 18);
        const boxH = 30;
        const boxX = Math.max(p.x + 2, Math.min(p.x + p.w - boxW - 2, x - boxW / 2));
        const boxY = p.y + p.h + 44;
        ctx.fillStyle = "#1f6feb";
        ctx.fillRect(boxX, boxY, boxW, boxH);
        ctx.strokeStyle = "#8cc8ff";
        ctx.strokeRect(boxX, boxY, boxW, boxH);
        ctx.fillStyle = "#ffffff";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";
        ctx.fillText(label, boxX + boxW / 2, boxY + boxH / 2);
        ctx.restore();
      }
      onMouseMove(event) {
        const rect = canvas.getBoundingClientRect();
        const x = event.clientX - rect.left;
        const y = event.clientY - rect.top;
        this.mouse = { x, y };
        if (this.inspectingTimeSlice) {
          tooltip.style.opacity = "0";
          this.hover = null;
          this.draw();
          return;
        }
        let nearest = null;
        let nearestDistance = Infinity;
        const nearby = [];
        for (const item of this.visibleEvents) {
          const distance = Math.hypot(item.x - x, item.y - y);
          if (distance <= Math.max(16, item.radius + 8)) {
            nearby.push(item);
            if (distance < nearestDistance) {
              nearest = item;
              nearestDistance = distance;
            }
          }
        }
        if (!nearest) {
          tooltip.style.opacity = "0";
          this.hover = null;
          this.draw();
          return;
        }
        const samePoint = this.samePointVisibleEvents(nearest, nearby);
        const hoverEvent = this.combinedHoverEvent(samePoint.length ? samePoint : [nearest], nearest.event);
        this.hover = hoverEvent;
        tooltip.textContent = tooltipText(hoverEvent);
        tooltip.style.opacity = "1";
        this.placeTooltip(x, y);
        this.draw();
      }
      combinedHoverEvent(items, fallbackEvent) {
        const events = [];
        for (const item of items) {
          const aggregate = safeArray(item.event?.aggregate_events);
          if (aggregate.length) {
            events.push(...aggregate);
          } else if (item.event) {
            events.push(item.event);
          }
        }
        const orderedEvents = orderedDomEvents(events);
        if (orderedEvents.length <= 1) return fallbackEvent;
        const last = latestDomEvent(orderedEvents);
        return {
          ...last,
          aggregate_events: orderedEvents,
          order_size: orderedEvents.reduce((total, item) => total + Math.max(0, num(rawEventAmount(item), 0)), 0),
          raw_total_contracts: orderedEvents.reduce((total, item) => total + Math.max(0, num(rawEventAmount(item), 0)), 0),
          event_count: orderedEvents.length,
          last_order_size: num(rawEventAmount(last), 0),
        };
      }
      placeTooltip(x, y) {
        const margin = 12;
        const gap = 18;
        const stageW = stage.clientWidth;
        const stageH = stage.clientHeight;
        const maxW = Math.max(260, stageW - margin * 2);
        const maxH = Math.max(120, stageH - margin * 2);
        tooltip.style.maxWidth = `${Math.min(680, maxW)}px`;
        tooltip.style.maxHeight = `${maxH}px`;
        const tooltipW = Math.min(tooltip.offsetWidth || 680, maxW);
        const tooltipH = Math.min(tooltip.offsetHeight || 320, maxH);
        const candidates = [
          { left: x + gap, top: y + gap },
          { left: x - tooltipW - gap, top: y + gap },
          { left: x + gap, top: y - tooltipH - gap },
          { left: x - tooltipW - gap, top: y - tooltipH - gap },
          { left: x - tooltipW / 2, top: y - tooltipH - gap },
          { left: x - tooltipW / 2, top: y + gap },
        ];
        const fits = point => (
          point.left >= margin &&
          point.top >= margin &&
          point.left + tooltipW <= stageW - margin &&
          point.top + tooltipH <= stageH - margin
        );
        const selected = candidates.find(fits) || candidates[0];
        const maxLeft = Math.max(margin, stageW - tooltipW - margin);
        const maxTop = Math.max(margin, stageH - tooltipH - margin);
        const left = Math.max(margin, Math.min(maxLeft, selected.left));
        const top = Math.max(margin, Math.min(maxTop, selected.top));
        tooltip.style.left = `${left}px`;
        tooltip.style.top = `${top}px`;
      }
      clearHover() {
        tooltip.style.opacity = "0";
        this.hover = null;
        this.mouse = null;
        this.draw();
      }
      onTimeSlicePointerDown(event) {
        if (event.button !== 0 || !this.session) return;
        if (event.target?.closest?.("a,button,input,.timeline-scrollbar,.price-scrollbar,.time-slice-panel")) return;
        event.preventDefault();
        if (timeSlicePanelEl.classList.contains("open")) {
          this.closeTimeSlicePanel();
          return;
        }
        this.inspectingTimeSlice = true;
        this.timeSliceRequestId += 1;
        tooltip.style.opacity = "0";
        void this.renderTimeSlicePanel(event, this.timeSliceRequestId);
      }
      closeTimeSlicePanel() {
        if (!this.inspectingTimeSlice && !timeSlicePanelEl.classList.contains("open")) return;
        this.inspectingTimeSlice = false;
        this.timeSliceRequestId += 1;
        timeSlicePanelEl.classList.remove("open");
        timeSlicePanelEl.innerHTML = "";
        this.timeSliceOrderIdMenus = [];
      }
      onTimeSlicePanelClick(event) {
        const button = event.target?.closest?.("[data-order-id-menu]");
        if (button) {
          event.preventDefault();
          event.stopPropagation();
          this.toggleOrderIdPopup(button);
          return;
        }
        if (event.target?.closest?.(".order-id-popup")) {
          event.stopPropagation();
          return;
        }
        this.hideOrderIdPopup();
      }
      hideOrderIdPopup() {
        const popup = timeSlicePanelEl.querySelector(".order-id-popup");
        if (popup) {
          popup.hidden = true;
          popup.innerHTML = "";
          delete popup.dataset.menuIndex;
        }
        timeSlicePanelEl.querySelectorAll(".order-id-toggle.active").forEach(button => {
          button.classList.remove("active");
        });
      }
      toggleOrderIdPopup(button) {
        const menuIndex = Number(button?.dataset?.orderIdMenu);
        const popup = timeSlicePanelEl.querySelector(".order-id-popup");
        if (!popup || !Number.isInteger(menuIndex)) return;
        if (!popup.hidden && popup.dataset.menuIndex === String(menuIndex)) {
          this.hideOrderIdPopup();
          return;
        }
        const ids = safeArray(this.timeSliceOrderIdMenus[menuIndex]);
        if (!ids.length) {
          this.hideOrderIdPopup();
          return;
        }
        timeSlicePanelEl.querySelectorAll(".order-id-toggle.active").forEach(item => {
          item.classList.toggle("active", item === button);
        });
        button.classList.add("active");
        popup.dataset.menuIndex = String(menuIndex);
        popup.innerHTML = ids.map(id => `<div>${escapeHtml(id)}</div>`).join("");
        popup.hidden = false;
        popup.style.width = `${Math.min(420, Math.max(220, timeSlicePanelEl.clientWidth - 24))}px`;
        const panelRect = timeSlicePanelEl.getBoundingClientRect();
        const buttonRect = button.getBoundingClientRect();
        let left = buttonRect.left - panelRect.left;
        let top = buttonRect.bottom - panelRect.top + 5;
        const popupWidth = popup.offsetWidth || 320;
        const popupHeight = popup.offsetHeight || 180;
        left = Math.max(8, Math.min(timeSlicePanelEl.clientWidth - popupWidth - 8, left));
        if (top + popupHeight > timeSlicePanelEl.clientHeight - 8) {
          top = buttonRect.top - panelRect.top - popupHeight - 5;
        }
        top = Math.max(8, Math.min(timeSlicePanelEl.clientHeight - popupHeight - 8, top));
        popup.style.left = `${left}px`;
        popup.style.top = `${top}px`;
      }
      timeForClientX(clientX) {
        const layoutBox = this.layout();
        const plotBox = this.plot(layoutBox);
        const stageRect = stage.getBoundingClientRect();
        const x = Math.max(plotBox.x, Math.min(plotBox.x + plotBox.w, Number(clientX) - stageRect.left));
        return this.timeForX(x, plotBox);
      }
      priceForClientY(clientY) {
        const layoutBox = this.layout();
        const plotBox = this.plot(layoutBox);
        const stageRect = stage.getBoundingClientRect();
        const y = Math.max(plotBox.y, Math.min(plotBox.y + plotBox.h, Number(clientY) - stageRect.top));
        return this.priceForY(y, plotBox);
      }
      timeSliceBucketMs() {
        const layoutBox = this.layout();
        const plotBox = this.plot(layoutBox);
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const msPerPixel = span / Math.max(1, plotBox.w);
        return Math.max(1, Math.round(Math.max(num(this.session?.time_bucket_ms, 0), msPerPixel)));
      }
      timeSliceStartMs(fallbackMs) {
        if (!sliceRangeEnabledEl?.checked) return Math.round(fallbackMs);
        const raw = String(sliceStartInputEl?.value || "").trim();
        if (!raw) return Math.round(fallbackMs);
        const parsed = new Date(raw).getTime();
        return Number.isFinite(parsed) ? Math.floor(parsed / 1000) * 1000 : Math.round(fallbackMs);
      }
      async sliceSessionForRange(sliceStart, sliceEnd) {
        if (!sliceRangeEnabledEl?.checked) return this.session;
        const start = Math.floor(Number(sliceStart));
        const end = Math.floor(Number(sliceEnd));
        if (!Number.isFinite(start) || !Number.isFinite(end) || end < start) return this.session;
        const key = `${ACTIVE_TIMEFRAME}|${dateInputEl.value || ""}|${start}|${end}`;
        if (this.sliceSessionCache.has(key)) return this.sliceSessionCache.get(key);
        try {
          const response = await fetch(
            domDataUrl({ startMs: start, endMs: end }, { includePriceRange: false }),
            { cache: "no-store" },
          );
          const snapshot = await response.json();
          const session = safeArray(snapshot?.sessions)[0] || this.session;
          this.sliceSessionCache.set(key, session);
          while (this.sliceSessionCache.size > 8) {
            const firstKey = this.sliceSessionCache.keys().next().value;
            this.sliceSessionCache.delete(firstKey);
          }
          return session;
        } catch (_error) {
          return this.session;
        }
      }
      async renderTimeSlicePanel(event, requestId = this.timeSliceRequestId) {
        if (!this.session || !Number.isFinite(this.view.startMs) || !Number.isFinite(this.view.endMs)) {
          this.closeTimeSlicePanel();
          return;
        }
        const nearestEvent = this.nearestVisibleEventForClient(event.clientX, event.clientY);
        const selectedMs = Math.round(nearestEvent ? num(nearestEvent.event.timestamp_ms, this.timeForClientX(event.clientX)) : this.timeForClientX(event.clientX));
        const selectedPrice = nearestEvent ? num(nearestEvent.event.price, this.priceForClientY(event.clientY)) : this.priceForClientY(event.clientY);
        const sliceEnd = selectedMs;
        const sliceStart = Math.min(this.timeSliceStartMs(selectedMs), sliceEnd);
        const bookRect = bookLevelsEl.getBoundingClientRect();
        const panelHeight = Math.max(260, Math.min(window.innerHeight - 24, bookRect.height || window.innerHeight - 120));
        const bookPriceRows = this.visibleBookPriceRows();
        const summarySession = await this.sliceSessionForRange(sliceStart, sliceEnd);
        if (!this.inspectingTimeSlice || requestId !== this.timeSliceRequestId) return;
        const mergedSummarySession = {
          ...(summarySession || this.session || {}),
          raw_events: mergeRawEvents(
            rawSessionEvents(summarySession, { rawOnly: true }),
            rawSessionEvents(this.session, { rawOnly: true }),
          ),
        };
        const summary = this.timeSliceSummary(
          mergedSummarySession,
          selectedMs,
          sliceStart,
          sliceEnd,
          selectedPrice,
          bookPriceRows,
          nearestEvent?.event || null,
        );
        const rawStatus = summary.hasRawPayload
          ? `raw events ${fmt(num(summary.rawEventCount, 0), 0)}`
          : "raw payload missing";
        const orderIdMenus = [];
        const rows = summary.rows.map(row => {
          const orderIds = orderIdsArray(row.orderIds);
          const menuIndex = orderIdMenus.length;
          orderIdMenus.push(orderIds);
          const fontSize = rowFontSize(row.height, 22);
          return (
            `<div class="time-slice-row" style="top:${num(row.top, 0).toFixed(1)}px;height:${Math.max(1, num(row.height, BOOK_ROW_MIN_HEIGHT)).toFixed(1)}px;font-size:${fontSize}px">` +
            `<span class="price">${escapeHtml(row.price)}</span>` +
            orderIdCellHtml(orderIds, menuIndex) +
            `<span class="bid">${contractLabel(row.restBid)}</span>` +
            `<span class="ask">${contractLabel(row.restAsk)}</span>` +
            metricCellHtml("add", row.addTotal, row.addCount) +
            metricCellHtml("ask", row.fillAsk, row.fillAskCount) +
            metricCellHtml("bid", row.fillBid, row.fillBidCount) +
            metricCellHtml("mod-plus", row.modPlus, row.modPlusCount) +
            metricCellHtml("mod-minus", row.modMinus, row.modMinusCount) +
            metricCellHtml("ask", row.cancelAsk, row.cancelAskCount) +
            metricCellHtml("bid", row.cancelBid, row.cancelBidCount) +
            topContractCellHtml(row.topContract, "orderId", "top-id") +
            topContractCellHtml(row.topContract, "typeLabel", "top-type") +
            topContractCellHtml(row.topContract, "amountLabel", "top-qty") +
            `</div>`
          );
        });
        this.timeSliceOrderIdMenus = orderIdMenus;
        timeSlicePanelEl.innerHTML = (
          `<div class="time-slice-head">` +
          `<strong>DOM Slice ${escapeHtml(this.session.provider_symbol || this.session.symbol || "")}</strong>` +
          `<span>${escapeHtml(dateTimeLabel(sliceStart))} -> ${escapeHtml(dateTimeLabel(sliceEnd))} | ${escapeHtml(rawStatus)}</span>` +
          `</div>` +
          `<div class="time-slice-columns">` +
          `<span class="price column-title">Price</span>` +
          `<span class="ids column-title">Order ID</span>` +
          `<span class="column-title">Rest B</span><span class="column-title">Rest A</span>` +
          `<span class="column-title">ADD</span>` +
          `<span class="column-title">Fill A</span><span class="column-title">Fill B</span>` +
          `<span class="column-title">Mod+</span><span class="column-title">Mod-</span>` +
          `<span class="column-title">Del A</span><span class="column-title">Del B</span>` +
          `<span class="top-id column-title">Top ID</span><span class="top-type column-title">Type</span><span class="column-title">Total Modify</span>` +
          `</div>` +
          `<div class="time-slice-body">` +
          (rows.length
            ? `<div class="time-slice-grid">${rows.join("")}</div>`
            : `<div class="time-slice-empty">${summary.hasRawPayload ? "No DOM activity at this time" : "Raw DOM event payload is unavailable for this slice"}</div>`) +
          `</div>` +
          `<div class="order-id-popup" hidden></div>`
        );
        timeSlicePanelEl.style.height = `${panelHeight}px`;
        this.placeTimeSlicePanel(event.clientX, bookRect.top, panelHeight);
        timeSlicePanelEl.classList.add("open");
      }
      nearestVisibleEventForClient(clientX, clientY) {
        const rect = canvas.getBoundingClientRect();
        const x = Number(clientX) - rect.left;
        const y = Number(clientY) - rect.top;
        let nearest = null;
        let nearestDistance = Infinity;
        for (const item of this.visibleEvents) {
          const distance = Math.hypot(item.x - x, item.y - y);
          if (distance <= Math.max(14, item.radius + 8) && distance < nearestDistance) {
            nearest = item;
            nearestDistance = distance;
          }
        }
        if (!nearest) return null;
        const samePoint = this.samePointVisibleEvents(nearest, this.visibleEvents);
        if (samePoint.length <= 1) return nearest;
        return {
          ...nearest,
          event: this.combinedHoverEvent(samePoint, nearest.event),
          radius: Math.max(...samePoint.map(item => num(item.radius, nearest.radius))),
        };
      }
      samePointVisibleEvents(nearest, items) {
        if (!nearest) return [];
        return safeArray(items).filter(item => (
          Math.round(item.x) === Math.round(nearest.x) &&
          Math.round(item.y) === Math.round(nearest.y)
        ));
      }
      visibleBookPriceRows() {
        const tick = Math.max(num(this.session?.tick_size, 0.25), 0.00000001);
        const bookRect = bookLevelsEl.getBoundingClientRect();
        return [...bookLevelsEl.querySelectorAll(".book-row[data-price]")]
          .map(row => {
            const price = num(row.dataset.price, NaN);
            if (!Number.isFinite(price)) return null;
            const rect = row.getBoundingClientRect();
            return {
              price,
              label: row.dataset.price || priceLabelForTickIndex(priceTickIndex(price, tick), tick),
              top: rect.top - bookRect.top,
              height: rect.height,
            };
          })
          .filter(Boolean);
      }
      timeSliceSummary(session, selectedMs, sliceStart, sliceEnd, selectedPrice = NaN, bookPriceRows = [], selectedEvent = null) {
        const tick = Math.max(num(session?.tick_size, this.session?.tick_size || 0), 0.00000001);
        const hasRawPayload = hasRawEventsPayload(session);
        const sourceRawEvents = rawSessionEvents(session, { rawOnly: true });
        const rowsByTick = new Map();
        const ensureRow = (price, label = "") => {
          const index = priceTickIndex(price, tick);
          if (!Number.isFinite(index)) return null;
          let row = rowsByTick.get(index);
          if (!row) {
            row = {
              index,
              price: label || priceLabelForTickIndex(index, tick),
              orderIds: new Set(),
              top: 0,
              height: BOOK_ROW_MIN_HEIGHT,
              restBid: 0,
              restAsk: 0,
              addTotal: 0,
              addCount: 0,
              fillBid: 0,
              fillBidCount: 0,
              fillAsk: 0,
              fillAskCount: 0,
              cancelBid: 0,
              cancelBidCount: 0,
              cancelAsk: 0,
              cancelAskCount: 0,
              modPlus: 0,
              modPlusCount: 0,
              modMinus: 0,
              modMinusCount: 0,
              topContract: null,
            };
            rowsByTick.set(index, row);
          } else if (label) {
            row.price = label;
          }
          return row;
        };
        const addSideValue = (row, side, bidKey, askKey, value) => {
          const amount = Math.max(0, num(value, 0));
          if (!row || amount <= 0) return;
          if (side === "BID") row[bidKey] += amount;
          if (side === "ASK") row[askKey] += amount;
        };
        const addMetric = (row, totalKey, countKey, value) => {
          const amount = Math.max(0, num(value, 0));
          if (!row || amount <= 0) return;
          row[totalKey] += amount;
          row[countKey] += 1;
        };
        const addOrderId = (row, value) => {
          if (!row) return;
          const text = String(value || "").trim();
          if (text) row.orderIds.add(text);
        };
        const addSideMetric = (row, side, bidTotalKey, bidCountKey, askTotalKey, askCountKey, value) => {
          if (side === "BID") addMetric(row, bidTotalKey, bidCountKey, value);
          if (side === "ASK") addMetric(row, askTotalKey, askCountKey, value);
        };
        const topContractStats = new Map();
        const processedAmountForEvent = (event, type) => {
          if (type === "ADD") return Math.max(0, num(event.added_contracts, 0));
          if (type === "EXECUTE") return Math.max(0, num(event.executed_contracts, 0));
          if (type === "CANCEL_DELETE") return Math.max(0, num(event.canceled_contracts, 0));
          if (type === "MODIFY") return Math.abs(num(event.modified_delta, 0));
          return 0;
        };
        const recordTopContract = (row, event, type, side, amount) => {
          const orderId = String(event.order_id || event.venue_order_id || "").trim();
          if (!row || !orderId || amount <= 0) return;
          const key = `${row.index}|${orderId}|${type}|${side}`;
          let stat = topContractStats.get(key);
          if (!stat) {
            stat = {
              index: row.index,
              price: row.price,
              orderId,
              type,
              side,
              typeLabel: sliceContractTypeLabel(type, side),
              amount: 0,
              processedAmount: 0,
              count: 0,
              maxSize: 0,
              lastSize: 0,
              deltaPlus: 0,
              deltaMinus: 0,
              positiveRefillCount: 0,
              positiveRefillTotal: 0,
            };
            topContractStats.set(key, stat);
          }
          stat.price = row.price;
          stat.amount += amount;
          stat.processedAmount += processedAmountForEvent(event, type);
          stat.count += 1;
          stat.maxSize = Math.max(stat.maxSize, amount);
          stat.lastSize = amount;
          const modifiedDelta = num(event.modified_delta, 0);
          if (type === "MODIFY" && modifiedDelta > 0) stat.deltaPlus += modifiedDelta;
          if (type === "MODIFY" && modifiedDelta < 0) stat.deltaMinus += Math.abs(modifiedDelta);
          const positiveRefillCount = Math.max(0, Math.round(num(event.positive_refill_count ?? event.refill_count, 0)));
          const positiveRefill = Math.max(0, num(event.positive_refill_contracts, 0));
          if (positiveRefillCount > 0) {
            stat.positiveRefillCount += positiveRefillCount;
            stat.positiveRefillTotal += positiveRefill;
          }
        };
        const applyTopContracts = () => {
          const bestByRow = new Map();
          for (const stat of topContractStats.values()) {
            const current = bestByRow.get(stat.index);
            if (
              !current ||
              stat.amount > current.amount ||
              (stat.amount === current.amount && stat.count > current.count)
            ) {
              bestByRow.set(stat.index, stat);
            }
          }
          const topStats = [...bestByRow.values()]
            .sort((a, b) => (b.amount - a.amount) || (b.count - a.count))
            .slice(0, 5);
          for (const stat of topStats) {
            const row = rowsByTick.get(stat.index);
            if (!row) continue;
            const details = [
              `Raw total: ${fmt(stat.amount, 0)} (${fmt(stat.count, 0)} events)`,
            ];
            if (Math.round(stat.processedAmount) !== Math.round(stat.amount)) {
              details.push(`Engine total: ${fmt(stat.processedAmount, 0)}`);
            }
            if (stat.type === "MODIFY") {
              details.push(`Refill count: ${fmt(stat.positiveRefillCount, 0)}`);
              details.push(`Replaced contracts: ${fmt(stat.positiveRefillTotal, 0)}`);
              details.push(`Delta +: ${fmt(stat.deltaPlus, 0)}`);
              details.push(`Delta -: ${fmt(stat.deltaMinus, 0)}`);
            }
            row.topContract = {
              orderId: stat.orderId,
              typeLabel: stat.typeLabel,
              amount: stat.amount,
              amountLabel: contractLabel(stat.amount),
              count: stat.count,
              maxSize: stat.maxSize,
              lastSize: stat.lastSize,
              price: stat.price,
              breakdown: details.join("\n"),
            };
          }
        };

        for (const segment of safeArray(session?.resting_segments)) {
          const start = num(segment.start_ms, NaN);
          const end = num(segment.end_ms, NaN);
          const price = num(segment.price, NaN);
          if (!Number.isFinite(start) || !Number.isFinite(end) || !Number.isFinite(price)) continue;
          if (start <= selectedMs && selectedMs <= end) {
            const row = ensureRow(price);
            addOrderId(row, segment.order_id);
            addSideValue(row, String(segment.side || ""), "restBid", "restAsk", segment.order_size);
          }
        }

        const restingStateByTick = new Map();
        for (const event of sourceRawEvents) {
          const ms = num(event.timestamp_ms, NaN);
          const price = num(event.price, NaN);
          if (!Number.isFinite(ms) || ms > selectedMs || !Number.isFinite(price)) continue;
          const index = priceTickIndex(price, tick);
          if (!Number.isFinite(index)) continue;
          restingStateByTick.set(index, {
            bid: Math.max(0, num(event.resting_bid_contracts, 0)),
            ask: Math.max(0, num(event.resting_ask_contracts, 0)),
          });
        }
        for (const [index, state] of restingStateByTick.entries()) {
          const row = ensureRow(index * tick);
          if (!row) continue;
          row.restBid = state.bid;
          row.restAsk = state.ask;
        }
        if (selectedEvent) {
          const selectedEventPrice = num(selectedEvent.price, NaN);
          if (Number.isFinite(selectedEventPrice)) {
            const row = ensureRow(selectedEventPrice);
            if (row) {
              row.restBid = Math.max(0, num(selectedEvent.resting_bid_contracts, row.restBid));
              row.restAsk = Math.max(0, num(selectedEvent.resting_ask_contracts, row.restAsk));
            }
          }
        }

        for (const event of sourceRawEvents) {
          const ms = num(event.timestamp_ms, NaN);
          const price = num(event.price, NaN);
          if (!Number.isFinite(ms) || ms < sliceStart || ms > sliceEnd || !Number.isFinite(price)) continue;
          const row = ensureRow(price);
          const side = String(event.side || "");
          const type = String(event.event_type || "");
          const realAmount = sliceContractRealAmount(event, type);
          addOrderId(row, event.order_id || event.venue_order_id);
          recordTopContract(row, event, type, side, realAmount);
          if (type === "ADD") {
            addMetric(row, "addTotal", "addCount", realAmount);
          } else if (type === "EXECUTE") {
            addSideMetric(row, side, "fillBid", "fillBidCount", "fillAsk", "fillAskCount", realAmount);
          } else if (type === "CANCEL_DELETE") {
            addSideMetric(row, side, "cancelBid", "cancelBidCount", "cancelAsk", "cancelAskCount", realAmount);
          } else if (type === "MODIFY") {
            const modifiedDelta = num(event.modified_delta, 0);
            if (modifiedDelta < 0) {
              addMetric(row, "modMinus", "modMinusCount", realAmount);
            } else {
              addMetric(row, "modPlus", "modPlusCount", realAmount);
            }
          }
        }
        applyTopContracts();

        if (bookPriceRows.length) {
          const rows = bookPriceRows
            .map(item => {
              const row = ensureRow(item.price, item.label);
              if (!row) return null;
              row.top = item.top;
              row.height = item.height;
              return row;
            })
            .filter(Boolean);
          return { rows, hasRawPayload, rawEventCount: sourceRawEvents.length };
        }

        const rows = [...rowsByTick.values()]
          .filter(row => (
            row.restBid || row.restAsk ||
            row.addTotal ||
            row.fillBid || row.fillAsk ||
            row.cancelBid || row.cancelAsk ||
            row.modPlus || row.modMinus ||
            row.topContract
          ))
          .sort((a, b) => b.index - a.index);
        void selectedPrice;
        return { rows, hasRawPayload, rawEventCount: sourceRawEvents.length };
      }
      placeTimeSlicePanel(clientX, panelTop, panelHeight) {
        const margin = 12;
        const panelW = timeSlicePanelEl.offsetWidth || 940;
        const panelH = Number(panelHeight) || timeSlicePanelEl.offsetHeight || 420;
        let left = Number(clientX) + 16;
        let top = Number(panelTop);
        if (left + panelW + margin > window.innerWidth) left = Number(clientX) - panelW - 16;
        if (!Number.isFinite(top)) top = 0;
        if (top + panelH > window.innerHeight) top = window.innerHeight - panelH;
        timeSlicePanelEl.style.left = `${Math.max(margin, Math.min(window.innerWidth - panelW - margin, left))}px`;
        timeSlicePanelEl.style.top = `${Math.max(0, Math.min(window.innerHeight - panelH, top))}px`;
      }
      clampTimeView(options = {}) {
        const tfMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60000;
        const maxSpan = tfMs * 5;

        let span = Math.max(1, this.view.endMs - this.view.startMs);

        if (span > maxSpan) {
          const center = (this.view.startMs + this.view.endMs) / 2;
          this.view.startMs = center - maxSpan / 2;
          this.view.endMs = center + maxSpan / 2;
          span = maxSpan;
        }

        const useGlobalNavigation = options.allowGlobal === true && !this.nySessionOnly;
        const navigation = useGlobalNavigation
          ? {
              startMs: num(this.globalNavigation.startMs, this.fullNavigation.startMs),
              endMs: num(this.globalNavigation.endMs, this.fullNavigation.endMs),
            }
          : this.effectiveTimeNavigation();
        const navStart = num(navigation.startMs, this.view.startMs);
        const navEnd = num(navigation.endMs, this.view.endMs);

        if (!Number.isFinite(navStart) || !Number.isFinite(navEnd) || navEnd <= navStart) return;

        const maxStart = Math.max(navStart, navEnd - span);

        this.view.startMs = Math.max(navStart, Math.min(maxStart, this.view.startMs));
        this.view.endMs = this.view.startMs + span;
      }
      disableDayScrollbar() {
        dayScrollbarEl.classList.add("disabled");
        dayScrollbarContentEl.style.width = "100%";
        this.syncingDayScrollbar = true;
        dayScrollbarEl.scrollLeft = 0;
        window.setTimeout(() => { this.syncingDayScrollbar = false; }, 0);
      }
      availableDateMs(dateValue) {
        const parsed = Date.parse(`${String(dateValue || "").slice(0, 10)}T00:00:00Z`);
        return Number.isFinite(parsed) ? parsed : NaN;
      }
      availableDateIndexForMs(ms) {
        if (!this.availableDates.length) return -1;
        const currentDate = isoDateForMs(ms);
        const exactIndex = this.availableDates.indexOf(currentDate);
        if (exactIndex >= 0) return exactIndex;
        let nearestIndex = 0;
        let nearestDistance = Infinity;
        for (let index = 0; index < this.availableDates.length; index += 1) {
          const dateMs = this.availableDateMs(this.availableDates[index]);
          const distance = Math.abs(dateMs - Number(ms));
          if (Number.isFinite(distance) && distance < nearestDistance) {
            nearestDistance = distance;
            nearestIndex = index;
          }
        }
        return nearestIndex;
      }
      syncDayScrollbar() {
        if (this.availableDates.length > 1) {
          dayScrollbarEl.classList.remove("disabled");
          const scrollRangePx = Math.max(1000, Math.min(160000, (this.availableDates.length - 1) * 360));
          dayScrollbarContentEl.style.width = `${dayScrollbarEl.clientWidth + scrollRangePx}px`;
          const maxScrollLeft = Math.max(1, dayScrollbarEl.scrollWidth - dayScrollbarEl.clientWidth);
          const index = Math.max(0, Math.min(this.availableDates.length - 1, this.availableDateIndexForMs(this.view.startMs)));
          const ratio = index / Math.max(1, this.availableDates.length - 1);
          this.syncingDayScrollbar = true;
          dayScrollbarEl.scrollLeft = Math.round(ratio * maxScrollLeft);
          window.setTimeout(() => { this.syncingDayScrollbar = false; }, 0);
          return;
        }
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const navStart = num(this.globalNavigation.startMs, 0);
        const navEnd = num(this.globalNavigation.endMs, 0);
        const maxStart = navEnd - span;
        if (!Number.isFinite(navStart) || !Number.isFinite(navEnd) || maxStart <= navStart) {
          this.disableDayScrollbar();
          return;
        }
        dayScrollbarEl.classList.remove("disabled");
        const dayCount = Math.max(1, Math.ceil((maxStart - navStart) / DAY_MS));
        const scrollRangePx = Math.max(1000, Math.min(160000, dayCount * 320));
        dayScrollbarContentEl.style.width = `${dayScrollbarEl.clientWidth + scrollRangePx}px`;
        const maxScrollLeft = Math.max(1, dayScrollbarEl.scrollWidth - dayScrollbarEl.clientWidth);
        const ratio = (this.view.startMs - navStart) / (maxStart - navStart);
        this.syncingDayScrollbar = true;
        dayScrollbarEl.scrollLeft = Math.round(Math.max(0, Math.min(1, ratio)) * maxScrollLeft);
        window.setTimeout(() => { this.syncingDayScrollbar = false; }, 0);
      }
      onDayScrollbarScroll() {
        if (!this.session || this.syncingDayScrollbar || dayScrollbarEl.classList.contains("disabled")) return;
        const maxScrollLeft = Math.max(1, dayScrollbarEl.scrollWidth - dayScrollbarEl.clientWidth);
        const ratio = Math.max(0, Math.min(1, dayScrollbarEl.scrollLeft / maxScrollLeft));
        let selectedDate = "";
        let targetStartMs = 0;
        if (this.availableDates.length > 1) {
          const index = Math.max(
            0,
            Math.min(this.availableDates.length - 1, Math.round(ratio * (this.availableDates.length - 1))),
          );
          selectedDate = this.availableDates[index] || "";
          targetStartMs = this.availableDateMs(selectedDate);
        } else {
          const span = Math.max(1, this.view.endMs - this.view.startMs);
          const navStart = num(this.globalNavigation.startMs, this.view.startMs);
          const maxStart = num(this.globalNavigation.endMs, this.view.endMs) - span;
          if (!Number.isFinite(navStart) || !Number.isFinite(maxStart) || maxStart <= navStart) return;
          targetStartMs = navStart + (maxStart - navStart) * ratio;
          selectedDate = isoDateForMs(targetStartMs);
        }
        if (!selectedDate) return;
        const dayBounds = utcDayBoundsForMs(targetStartMs);
        const navStart = num(this.globalNavigation.startMs, dayBounds.startMs);
        if (dayBounds.endMs > dayBounds.startMs) {
          this.fullNavigation = {
            startMs: Math.max(navStart, dayBounds.startMs),
            endMs: Math.min(num(this.globalNavigation.endMs, dayBounds.endMs), dayBounds.endMs),
          };
          this.navigation = this.fullNavigation;
        }
        dateInputEl.value = selectedDate;
        this.view = { startMs: 0, endMs: 0, priceMin: NaN, priceMax: NaN };
        this.renderRange = { startMs: 0, endMs: 0 };
        this.userTimeZoomed = false;
        this.manualTimeZoom = null;
        this.scheduleFetch(180, {
          showLoading: false,
          selectedDate,
          includePriceRange: false,
          requestView: null,
        });
      }
      disableTimeScrollbars() {
        timeScrollbarEl.classList.add("disabled");
        timeScrollbarContentEl.style.width = "100%";
        window.clearTimeout(this.timeScrollbarSyncTimer);
        this.syncingTimeScrollbar = true;
        timeScrollbarEl.scrollLeft = 0;
        this.timeScrollbarSyncTimer = window.setTimeout(() => {
          this.syncingTimeScrollbar = false;
        }, 180);
      }
      timeNavigationForScrollbar() {
        const localNavigation = this.effectiveTimeNavigation();
        if (this.nySessionOnly) return localNavigation;
        const localStart = num(localNavigation.startMs, NaN);
        const localEnd = num(localNavigation.endMs, NaN);
        if (
          Number.isFinite(localStart)
          && Number.isFinite(localEnd)
          && localEnd > localStart
          && this.view.startMs >= localStart
          && this.view.endMs <= localEnd
        ) {
          return localNavigation;
        }
        const globalStart = num(this.globalNavigation.startMs, NaN);
        const globalEnd = num(this.globalNavigation.endMs, NaN);
        if (Number.isFinite(globalStart) && Number.isFinite(globalEnd) && globalEnd > globalStart) {
          return { startMs: globalStart, endMs: globalEnd };
        }
        return localNavigation;
      }
      syncTimeScrollbar() {
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const navigation = this.timeNavigationForScrollbar();
        const navStart = num(navigation.startMs, 0);
        const navEnd = num(navigation.endMs, 0);
        const maxStart = navEnd - span;
        if (!Number.isFinite(navStart) || !Number.isFinite(navEnd) || maxStart <= navStart) {
          this.disableTimeScrollbars();
          return;
        }
        timeScrollbarEl.classList.remove("disabled");
        const scrollRangePx = Math.max(1000, Math.min(160000, Math.round((maxStart - navStart) / 1000)));
        timeScrollbarContentEl.style.width = `${timeScrollbarEl.clientWidth + scrollRangePx}px`;
        const maxScrollLeft = Math.max(1, timeScrollbarEl.scrollWidth - timeScrollbarEl.clientWidth);
        const ratio = (this.view.startMs - navStart) / (maxStart - navStart);
        window.clearTimeout(this.timeScrollbarSyncTimer);
        this.syncingTimeScrollbar = true;
        timeScrollbarEl.scrollLeft = Math.round(Math.max(0, Math.min(1, ratio)) * maxScrollLeft);
        this.timeScrollbarSyncTimer = window.setTimeout(() => {
          this.syncingTimeScrollbar = false;
        }, 180);
      }
      onTimeScrollbarScroll() {
        if (!this.session || this.syncingTimeScrollbar || timeScrollbarEl.classList.contains("disabled")) return;
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const navigation = this.timeNavigationForScrollbar();
        const navStart = num(navigation.startMs, this.view.startMs);
        const maxStart = num(navigation.endMs, this.view.endMs) - span;
        if (!Number.isFinite(navStart) || !Number.isFinite(maxStart) || maxStart <= navStart) return;
        const maxScrollLeft = Math.max(1, timeScrollbarEl.scrollWidth - timeScrollbarEl.clientWidth);
        const ratio = Math.max(0, Math.min(1, timeScrollbarEl.scrollLeft / maxScrollLeft));
        this.view.startMs = navStart + (maxStart - navStart) * ratio;
        this.view.endMs = this.view.startMs + span;

        this.renderRange = {
          startMs: this.view.startMs,
          endMs: this.view.endMs,
        };

        this.syncDateToViewStart();
        this.rememberUserTimeView();
        this.applyVerticalPriceScale();
        this.syncDayScrollbar();
        this.syncPriceScrollbar();
        this.draw();
        this.scheduleFetch(720, { showLoading: false });
      }
      disablePriceScrollbar() {
        priceScrollbarEl.classList.add("disabled");
        priceScrollbarContentEl.style.height = "100%";
        this.syncingPriceScrollbar = true;
        priceScrollbarEl.scrollTop = 0;
        window.setTimeout(() => { this.syncingPriceScrollbar = false; }, 0);
      }
      priceBoundsForSession() {
        if (!this.session) {
          return { min: this.view.priceMin, max: this.view.priceMax };
        }
        const prices = [];
        for (const event of rawSessionEvents(this.session)) prices.push(num(event.price, NaN));
        for (const segment of safeArray(this.session.resting_segments)) prices.push(num(segment.price, NaN));
        for (const point of safeArray(this.session.best_bid_line)) prices.push(num(point.price, NaN));
        for (const point of safeArray(this.session.best_ask_line)) prices.push(num(point.price, NaN));
        for (const level of safeArray(this.session.order_book_levels)) prices.push(num(level.price, NaN));
        const finite = prices.filter(Number.isFinite);
        const tick = Math.max(num(this.session.tick_size, 0.25), 0.00000001);
        if (!finite.length) {
          const fallbackMin = Number.isFinite(this.view.priceMin) ? this.view.priceMin : 0;
          const fallbackMax = Number.isFinite(this.view.priceMax) ? this.view.priceMax : tick * 20;
          return { min: Math.min(fallbackMin, fallbackMax), max: Math.max(fallbackMin, fallbackMax) };
        }
        const bounds = numericMinMax(finite);
        if (!bounds) {
          const fallbackMin = Number.isFinite(this.view.priceMin) ? this.view.priceMin : 0;
          const fallbackMax = Number.isFinite(this.view.priceMax) ? this.view.priceMax : tick * 20;
          return { min: Math.min(fallbackMin, fallbackMax), max: Math.max(fallbackMin, fallbackMax) };
        }
        const min = bounds.min;
        const max = bounds.max;
        const viewSpan = Math.max(tick * 4, this.view.priceMax - this.view.priceMin);
        const pad = Math.max(tick * 8, (max - min) * 0.08, viewSpan * 0.35);
        return { min: min - pad, max: max + pad };
      }
      syncPriceScrollbar() {
        if (!this.session || !Number.isFinite(this.view.priceMin) || !Number.isFinite(this.view.priceMax)) {
          this.disablePriceScrollbar();
          return;
        }
        const span = Math.max(0.00000001, this.view.priceMax - this.view.priceMin);
        const bounds = this.priceBoundsForSession();
        const minStart = bounds.min;
        const maxStart = bounds.max - span;
        if (!Number.isFinite(minStart) || !Number.isFinite(maxStart) || maxStart <= minStart) {
          this.disablePriceScrollbar();
          return;
        }
        priceScrollbarEl.classList.remove("disabled");
        const tick = Math.max(num(this.session.tick_size, 0.25), 0.00000001);
        const scrollRangePx = Math.max(1000, Math.min(160000, Math.round((maxStart - minStart) / tick) * 12));
        priceScrollbarContentEl.style.height = `${priceScrollbarEl.clientHeight + scrollRangePx}px`;
        const maxScrollTop = Math.max(1, priceScrollbarEl.scrollHeight - priceScrollbarEl.clientHeight);
        const ratio = (maxStart - this.view.priceMin) / (maxStart - minStart);
        this.syncingPriceScrollbar = true;
        priceScrollbarEl.scrollTop = Math.round(Math.max(0, Math.min(1, ratio)) * maxScrollTop);
        window.setTimeout(() => { this.syncingPriceScrollbar = false; }, 0);
      }
      onPriceScrollbarScroll() {
        if (!this.session || this.syncingPriceScrollbar || priceScrollbarEl.classList.contains("disabled")) return;
        const span = Math.max(0.00000001, this.view.priceMax - this.view.priceMin);
        const bounds = this.priceBoundsForSession();
        const minStart = bounds.min;
        const maxStart = bounds.max - span;
        if (!Number.isFinite(minStart) || !Number.isFinite(maxStart) || maxStart <= minStart) return;
        const maxScrollTop = Math.max(1, priceScrollbarEl.scrollHeight - priceScrollbarEl.clientHeight);
        const ratio = Math.max(0, Math.min(1, priceScrollbarEl.scrollTop / maxScrollTop));
        this.view.priceMin = maxStart - (maxStart - minStart) * ratio;
        this.view.priceMax = this.view.priceMin + span;
        this.autoScale = false;
        autoScaleEl.classList.remove("active");
        this.draw();
      }
      syncDateToViewStart() {
        const nextDate = isoDateForMs(this.view.startMs);
        if (nextDate && dateInputEl.value !== nextDate) dateInputEl.value = nextDate;
      }
      panTime(direction, largeStep = false) {
        if (!this.session || !this.view.startMs || !this.view.endMs) return;
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const deltaMs = span * (largeStep ? 0.8 : 0.28) * direction;
        this.view.startMs += deltaMs;
        this.view.endMs += deltaMs;
        this.clampTimeView({ allowGlobal: true });

        this.renderRange = {
          startMs: this.view.startMs,
          endMs: this.view.endMs,
        };

        this.syncDateToViewStart();
        this.syncTimeScrollbar();
        this.syncDayScrollbar();
        this.rememberUserTimeView();
        this.applyVerticalPriceScale();
        this.syncPriceScrollbar();
        this.draw();
        this.scheduleFetch(620, { showLoading: false });
      }
      onKeyDown(event) {
        const target = event.target;
        const tagName = String(target?.tagName || "").toUpperCase();
        if (tagName === "INPUT" || tagName === "SELECT" || tagName === "TEXTAREA" || target?.isContentEditable) {
          return;
        }
        if (event.key === "ArrowLeft") {
          event.preventDefault();
          this.panTime(-1, event.shiftKey);
        } else if (event.key === "ArrowRight") {
          event.preventDefault();
          this.panTime(1, event.shiftKey);
        }
      }
      onWheel(event) {
        if (!this.session || !this.view.startMs || !this.view.endMs) return;
        event.preventDefault();
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const priceSpan = Math.max(0.00000001, this.view.priceMax - this.view.priceMin);
        const horizontalDelta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
        if (event.ctrlKey) {
          const plotBox = this.plot(this.layout());

          const mouseTime = this.timeForX(event.offsetX, plotBox);
          const mousePrice = this.priceForY(event.offsetY, plotBox);

          const currentTimeSpan = Math.max(1, this.view.endMs - this.view.startMs);
          const currentPriceSpan = Math.max(0.00000001, this.view.priceMax - this.view.priceMin);

          const tfMs = TIMEFRAME_MS[ACTIVE_TIMEFRAME] || 60000;
          const navigation = this.timeNavigationForScrollbar();
          const navigationSpan = Math.max(
            tfMs,
            num(navigation.endMs, this.view.endMs) - num(navigation.startMs, this.view.startMs),
          );
          const minTimeSpan = Math.max(50, Math.min(1000, tfMs * 0.005));
          const maxTimeSpan = Math.max(tfMs * 20, navigationSpan);

          const tick = Math.max(num(this.session.tick_size, 0.25), 0.00000001);
          const minPriceSpan = tick * 2;
          const priceBounds = this.priceBoundsForSession();
          const boundsSpan = Math.max(
            tick * 40,
            num(priceBounds.max, this.view.priceMax) - num(priceBounds.min, this.view.priceMin),
          );
          const maxPriceSpan = Math.max(currentPriceSpan, boundsSpan * 1.6);

          const factor = event.deltaY > 0 ? 1.18 : 0.82;

          const nextTimeSpan = Math.max(minTimeSpan, Math.min(maxTimeSpan, currentTimeSpan * factor));
          const nextPriceSpan = Math.max(minPriceSpan, Math.min(maxPriceSpan, currentPriceSpan * factor));

          const timeRatio = (mouseTime - this.view.startMs) / currentTimeSpan;
          const priceRatio = (mousePrice - this.view.priceMin) / currentPriceSpan;

          this.view.startMs = mouseTime - nextTimeSpan * timeRatio;
          this.view.endMs = this.view.startMs + nextTimeSpan;
          this.view.priceMin = mousePrice - nextPriceSpan * priceRatio;
          this.view.priceMax = this.view.priceMin + nextPriceSpan;

          this.clampTimeView({ allowGlobal: true });
          this.rememberUserTimeView();
          this.autoScale = false;
          autoScaleEl.classList.remove("active");
          this.renderRange = {
              startMs: this.view.startMs,
              endMs: this.view.endMs,
          };
          this.syncDateToViewStart();
          this.syncTimeScrollbar();
          this.syncDayScrollbar();
          this.syncPriceScrollbar();
          
          this.scheduleFetch(420, { showLoading: false });
        }else if (event.altKey) {
          const factor = event.deltaY > 0 ? 1.12 : 0.88;
          const mid = (this.view.priceMin + this.view.priceMax) / 2;
          const nextSpan = Math.max(num(this.session.tick_size, 0.25) * 4, priceSpan * factor);
          this.view.priceMin = mid - nextSpan / 2;
          this.view.priceMax = mid + nextSpan / 2;
          this.autoScale = false;
          autoScaleEl.classList.remove("active");
          this.syncPriceScrollbar();
        }else if (event.shiftKey) {
          const tick = Math.max(num(this.session.tick_size, 0.25), 0.00000001);
          const wheelUnits = Math.max(-6, Math.min(6, event.deltaY / 100));
          const deltaPrice = wheelUnits * tick * 3;
          this.view.priceMin += deltaPrice;
          this.view.priceMax += deltaPrice;
          this.autoScale = false;
          autoScaleEl.classList.remove("active");
          this.syncPriceScrollbar();
        } else {
          const deltaMs = (horizontalDelta / 820) * span;
          this.view.startMs += deltaMs;
          this.view.endMs += deltaMs;
          this.clampTimeView({ allowGlobal: true });
          this.rememberUserTimeView();

          this.renderRange = {
            startMs: this.view.startMs,
            endMs: this.view.endMs,
          };

          this.syncDateToViewStart();
          this.syncTimeScrollbar();
          this.syncDayScrollbar();
          this.applyVerticalPriceScale();
          this.syncPriceScrollbar();
          this.scheduleFetch(620, { showLoading: false });
        }
        this.draw();
      }
      requestTimeFetchIfNeeded(delayMs = 620) {
        if (!this.needsTimeFetch()) return;
        this.scheduleFetch(delayMs, { showLoading: false });
      }
      needsTimeFetch() {
        if (!this.session) return true;
        const start = num(this.renderRange.startMs, this.view.startMs);
        const end = num(this.renderRange.endMs, this.view.endMs);
        if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return true;
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const guard = span * 0.35;
        return this.view.startMs < start + guard || this.view.endMs > end - guard;
      }
      abortDayPrefetches() {
        window.clearTimeout(this.dayPrefetchTimer);
        for (const controller of this.dayPrefetchControllers) {
          try { controller.abort(); } catch (_error) {}
        }
        this.dayPrefetchControllers = [];
      }
      scheduleAdjacentDayPrefetch(delayMs = 1200) {
        window.clearTimeout(this.dayPrefetchTimer);
        this.dayPrefetchTimer = window.setTimeout(() => {
          this.prefetchAdjacentDayWindows();
        }, Math.max(0, delayMs));
      }
      rememberDayPrefetchKey(key) {
        if (!key) return;
        if (this.dayPrefetchKeys.size > 48) {
          this.dayPrefetchKeys = new Set([...this.dayPrefetchKeys].slice(-24));
        }
        this.dayPrefetchKeys.add(key);
      }
      prefetchAdjacentDayWindows() {
        if (!this.session) return;
        const span = Math.max(1, this.view.endMs - this.view.startMs);
        const globalStart = num(this.globalNavigation.startMs, 0);
        const globalEnd = num(this.globalNavigation.endMs, 0);
        if (!Number.isFinite(globalStart) || !Number.isFinite(globalEnd) || globalEnd <= globalStart) return;
        this.abortDayPrefetches();
        for (const offset of [-DAY_MS, DAY_MS]) {
          const startMs = this.view.startMs + offset;
          const endMs = startMs + span;
          if (endMs <= globalStart || startMs >= globalEnd) continue;
          const boundedStart = Math.max(globalStart, startMs);
          const boundedEnd = Math.min(globalEnd, endMs);
          if (boundedEnd <= boundedStart) continue;
          const selectedDate = isoDateForMs(boundedStart);
          if (!selectedDate) continue;
          const key = `${ACTIVE_TIMEFRAME}|${selectedDate}|${Math.floor(boundedStart)}|${Math.floor(boundedEnd)}`;
          if (this.dayPrefetchKeys.has(key)) continue;
          this.rememberDayPrefetchKey(key);
          const controller = new AbortController();
          this.dayPrefetchControllers.push(controller);
          fetch(
            domDataUrl(
              { startMs: boundedStart, endMs: boundedEnd },
              { selectedDate, includePriceRange: false },
            ),
            { cache: "no-store", signal: controller.signal },
          )
            .then(response => (response.ok ? response.json() : null))
            .catch(() => null)
            .finally(() => {
              this.dayPrefetchControllers = this.dayPrefetchControllers.filter(item => item !== controller);
            });
        }
      }
      scheduleFetch(delayMs = 160, options = {}) {
        window.clearTimeout(this.fetchTimer);
        if (this.abortController) {
          this.abortController.abort();
          this.abortController = null;
        }
        if (options.showLoading !== false) {
          loadingIndicatorEl.style.opacity = "1";
          statusEl.textContent = "Loading DOM...";
        }
        const requestView = Object.prototype.hasOwnProperty.call(options, "requestView")
          ? options.requestView
          : this.view;
        const requestSnapshot = requestView
          ? {
              startMs: num(requestView.startMs, 0),
              endMs: num(requestView.endMs, 0),
              priceMin: num(requestView.priceMin, NaN),
              priceMax: num(requestView.priceMax, NaN),
            }
          : null;
        const generation = ++this.fetchGeneration;
        this.fetchTimer = window.setTimeout(
          () => fetchDomData(requestSnapshot, { ...options, generation }),
          Math.max(0, delayMs),
        );
      }
    }

    function tooltipContract(value) {
      return fmt(Math.max(0, num(value, 0)), 0);
    }

    function compactOrderId(value) {
      const text = String(value || "").trim();
      if (!text) return "N/A";
      if (text.length <= 24) return text;
      return `${text.slice(0, 12)}...${text.slice(-8)}`;
    }

    function currentBeforeEvent(item) {
      if (Object.prototype.hasOwnProperty.call(Object(item || {}), "order_previous_contracts")) {
        return Math.max(0, num(item.order_previous_contracts, 0));
      }
      const type = String(item?.event_type || "");
      const current = Math.max(0, num(item?.order_current_contracts, 0));
      if (type === "ADD") return Math.max(0, current - Math.max(0, num(item?.added_contracts, rawEventAmount(item))));
      if (type === "EXECUTE") return current + Math.max(0, num(item?.executed_contracts, rawEventAmount(item)));
      if (type === "CANCEL_DELETE") return current + Math.max(0, num(item?.canceled_contracts, rawEventAmount(item)));
      if (type === "MODIFY") return Math.max(0, current - num(item?.modified_delta, 0));
      return current;
    }

    function tooltipEventLabel(type, side) {
      const normalizedType = String(type || "").toUpperCase();
      const normalizedSide = String(side || "").toUpperCase();
      if (normalizedType === "EXECUTE" && normalizedSide === "BID") return "SELL hit BID";
      if (normalizedType === "EXECUTE" && normalizedSide === "ASK") return "BUY lifted ASK";
      if (normalizedType === "MODIFY") return "MODIFY / REFILL";
      if (normalizedType === "CANCEL_DELETE") return "CANCEL / DELETE";
      return sliceContractTypeLabel(normalizedType, normalizedSide);
    }

    function tooltipText(event) {
      const events = orderedDomEvents(safeArray(event?.aggregate_events).length ? event.aggregate_events : [event]);
      const last = latestDomEvent(events);
      const first = events[0] || last || event || {};
      const byOrder = new Map();
      const totals = {
        rawBuyFill: 0,
        rawSellFill: 0,
        visibleBuyFill: 0,
        visibleSellFill: 0,
        added: 0,
        canceled: 0,
        modPlus: 0,
        modMinus: 0,
        refillCount: 0,
        refillTotal: 0,
        refillBidCount: 0,
        refillAskCount: 0,
        refillBidTotal: 0,
        refillAskTotal: 0,
      };

      for (const item of events) {
        const type = String(item.event_type || "").toUpperCase();
        const side = String(item.side || "").toUpperCase();
        const rawSize = Math.max(0, num(rawEventAmount(item), 0));
        const orderId = String(item.order_id || item.venue_order_id || "").trim() || "N/A";
        let stat = byOrder.get(orderId);
        if (!stat) {
          stat = {
            orderId,
            side,
            price: String(item.price || ""),
            count: 0,
            startCurrent: currentBeforeEvent(item),
            endCurrent: Math.max(0, num(item.order_current_contracts, 0)),
            maxCurrent: Math.max(0, currentBeforeEvent(item), num(item.order_current_contracts, 0)),
            startPending: Math.max(0, num(item.order_previous_pending_refill_contracts, 0)),
            endPending: Math.max(0, num(item.order_pending_refill_contracts, 0)),
            rawBuyFill: 0,
            rawSellFill: 0,
            visibleBuyFill: 0,
            visibleSellFill: 0,
            added: 0,
            canceled: 0,
            modPlus: 0,
            modMinus: 0,
            refillCount: 0,
            refillTotal: 0,
            first: item,
            last: item,
          };
          byOrder.set(orderId, stat);
        }
        stat.count += 1;
        stat.endCurrent = Math.max(0, num(item.order_current_contracts, 0));
        stat.endPending = Math.max(0, num(item.order_pending_refill_contracts, 0));
        stat.maxCurrent = Math.max(stat.maxCurrent, currentBeforeEvent(item), stat.endCurrent);
        if (type === "EXECUTE" && side === "ASK") {
          stat.rawBuyFill += rawSize;
          stat.visibleBuyFill += Math.max(0, num(item.executed_contracts, 0));
          totals.rawBuyFill += rawSize;
          totals.visibleBuyFill += Math.max(0, num(item.executed_contracts, 0));
        }
        if (type === "EXECUTE" && side === "BID") {
          stat.rawSellFill += rawSize;
          stat.visibleSellFill += Math.max(0, num(item.executed_contracts, 0));
          totals.rawSellFill += rawSize;
          totals.visibleSellFill += Math.max(0, num(item.executed_contracts, 0));
        }
        if (type === "ADD") {
          const amount = Math.max(0, num(item.added_contracts, rawSize));
          stat.added += amount;
          totals.added += amount;
        }
        if (type === "CANCEL_DELETE") {
          const amount = Math.max(0, num(item.canceled_contracts, rawSize));
          stat.canceled += amount;
          totals.canceled += amount;
        }
        if (type === "MODIFY") {
          const modifiedDelta = num(item.modified_delta, 0);
          if (modifiedDelta > 0) {
            stat.modPlus += modifiedDelta;
            totals.modPlus += modifiedDelta;
          }
          if (modifiedDelta < 0) {
            stat.modMinus += Math.abs(modifiedDelta);
            totals.modMinus += Math.abs(modifiedDelta);
          }
        }
        const refillCount = Math.max(0, Math.round(num(item.positive_refill_count ?? item.refill_count, 0)));
        const positiveRefill = Math.max(0, num(item.positive_refill_contracts, 0));
        if (refillCount > 0) {
          stat.refillCount += refillCount;
          stat.refillTotal += positiveRefill;
          totals.refillCount += refillCount;
          totals.refillTotal += positiveRefill;
          if (side === "BID") {
            totals.refillBidCount += refillCount;
            totals.refillBidTotal += positiveRefill;
          }
          if (side === "ASK") {
            totals.refillAskCount += refillCount;
            totals.refillAskTotal += positiveRefill;
          }
        }
        if (compareDomEvents(item, stat.last) >= 0) stat.last = item;
      }

      const beforeBid = Object.prototype.hasOwnProperty.call(Object(first || {}), "before_resting_bid_contracts")
        ? num(first.before_resting_bid_contracts, 0)
        : num(first.resting_bid_contracts, 0);
      const beforeAsk = Object.prototype.hasOwnProperty.call(Object(first || {}), "before_resting_ask_contracts")
        ? num(first.before_resting_ask_contracts, 0)
        : num(first.resting_ask_contracts, 0);
      const afterBid = num(last.resting_bid_contracts, 0);
      const afterAsk = num(last.resting_ask_contracts, 0);
      const bookBefore = `${first.before_best_bid || "?"}/${first.before_best_ask || "?"}`;
      const bookAfter = `${last.after_best_bid || "?"}/${last.after_best_ask || "?"}`;
      const flowLines = [];
      if (totals.rawSellFill > 0) flowLines.push(`sell hit BID ${tooltipContract(totals.rawSellFill)} raw`);
      if (totals.rawBuyFill > 0) flowLines.push(`buy lifted ASK ${tooltipContract(totals.rawBuyFill)} raw`);
      if (!flowLines.length) flowLines.push(tooltipEventLabel(last.event_type, last.side));
      const signalLines = [];
      if (totals.rawSellFill > 0 && totals.refillBidCount > 0) {
        signalLines.push("Signal: bid refill after sell pressure, possible absorption");
      } else if (totals.rawBuyFill > 0 && totals.refillAskCount > 0) {
        signalLines.push("Signal: ask refill after buy pressure, possible absorption");
      } else if (totals.refillCount > 0) {
        signalLines.push("Signal: refill behavior present");
      }
      if (
        (totals.rawSellFill > 0 && Math.round(totals.rawSellFill) !== Math.round(totals.visibleSellFill)) ||
        (totals.rawBuyFill > 0 && Math.round(totals.rawBuyFill) !== Math.round(totals.visibleBuyFill))
      ) {
        signalLines.push(
          `Raw/visible fill: buy ${tooltipContract(totals.rawBuyFill)}/${tooltipContract(totals.visibleBuyFill)} | ` +
          `sell ${tooltipContract(totals.rawSellFill)}/${tooltipContract(totals.visibleSellFill)}`
        );
      }
      const orderLines = [...byOrder.values()]
        .sort((a, b) => (
          (b.rawBuyFill + b.rawSellFill + b.refillCount + b.added + b.canceled + b.modPlus + b.modMinus) -
          (a.rawBuyFill + a.rawSellFill + a.refillCount + a.added + a.canceled + a.modPlus + a.modMinus)
        ) || b.count - a.count)
        .slice(0, 3)
        .flatMap(stat => {
          const fillLabel = `raw buy/sell ${tooltipContract(stat.rawBuyFill)}/${tooltipContract(stat.rawSellFill)}`;
          const visibleLabel = `visible buy/sell ${tooltipContract(stat.visibleBuyFill)}/${tooltipContract(stat.visibleSellFill)}`;
          const beforeLevel = String(stat.side).toUpperCase() === "ASK"
            ? num(stat.first.before_resting_ask_contracts, stat.first.resting_ask_contracts)
            : num(stat.first.before_resting_bid_contracts, stat.first.resting_bid_contracts);
          const afterLevel = String(stat.side).toUpperCase() === "ASK"
            ? num(stat.last.resting_ask_contracts, 0)
            : num(stat.last.resting_bid_contracts, 0);
          return [
            "",
            `Order ${compactOrderId(stat.orderId)} ${stat.side || ""} ${stat.price}`,
            `  ${fillLabel} | ${visibleLabel}`,
            `  refill count ${tooltipContract(stat.refillCount)} | replaced contracts ${tooltipContract(stat.refillTotal)} | mod +${tooltipContract(stat.modPlus)}/-${tooltipContract(stat.modMinus)}`,
            `  current ${tooltipContract(stat.startCurrent)} -> ${tooltipContract(stat.endCurrent)} | pending ${tooltipContract(stat.startPending)} -> ${tooltipContract(stat.endPending)}`,
            `  level ${tooltipContract(beforeLevel)} -> ${tooltipContract(afterLevel)} | events ${tooltipContract(stat.count)}`,
          ];
        });

      return [
        `${last.symbol || last.provider_symbol || ""}  ${dateTimeLabel(last.timestamp_ms)}`,
        `Price ${last.price} | ${flowLines.join(" | ")} | events ${tooltipContract(events.length)} | orders ${tooltipContract(byOrder.size)}`,
        ...signalLines,
        "",
        `Level visible: bid ${tooltipContract(beforeBid)} -> ${tooltipContract(afterBid)} | ask ${tooltipContract(beforeAsk)} -> ${tooltipContract(afterAsk)}`,
        `Best bid/ask: ${bookBefore} -> ${bookAfter}`,
        `Point totals: add ${tooltipContract(totals.added)} | del ${tooltipContract(totals.canceled)} | mod +${tooltipContract(totals.modPlus)}/-${tooltipContract(totals.modMinus)} | refill count ${tooltipContract(totals.refillCount)} | replaced contracts ${tooltipContract(totals.refillTotal)}`,
        ...orderLines,
      ].filter(line => line !== null && line !== undefined).join("\n");
    }

    function bookTopOrderTypeLabel(level) {
      const side = String(level?.top_order_side || "").toUpperCase();
      const type = String(level?.top_order_type || "").toUpperCase();
      const sideLabel = side === "ASK" ? "A" : (side === "BID" ? "B" : "");
      const typeLabel = {
        ADD: "ADD",
        EXECUTE: "FILL",
        CANCEL_DELETE: "DEL",
        MODIFY: "MOD",
      }[type] || type;
      return [sideLabel, typeLabel].filter(Boolean).join(" ");
    }

    function bookTopOrderTitle(level) {
      if (!level?.top_order_id) return "";
      return [
        `Top active order rank: ${level.top_order_rank || ""}`,
        `Price: ${level.price || ""}`,
        `Order ID: ${level.top_order_id || ""}`,
        `Type: ${bookTopOrderTypeLabel(level)}`,
        `Raw total: ${level.top_order_size || 0}`,
        `Events: ${level.top_order_count || 0}`,
        `Last raw size: ${level.top_order_last_size || 0}`,
        `Last order current: ${level.top_order_current_contracts || 0}`,
        `Refill count: ${level.top_order_positive_refill_count || 0}`,
        `Replaced contracts: ${level.top_order_positive_refill_total || 0}`,
        `Bid level contracts: ${level.bid_contracts || 0}`,
        `Ask level contracts: ${level.ask_contracts || 0}`,
      ].join("\n");
    }

    function bookTopOrderCells(level) {
      const title = level?.top_order_id ? ` title="${escapeHtml(bookTopOrderTitle(level))}"` : "";
      const typeLabel = bookTopOrderTypeLabel(level);
      return (
        `<span class="book-top-id"${title}>${escapeHtml(level?.top_order_id || "")}</span>` +
        `<span class="book-top-type"${title}>${escapeHtml(typeLabel)}</span>` +
        `<span class="book-top-qty"${title}>${contractLabel(level?.top_order_size || 0)}</span>`
      );
    }

    function renderBook(session, chart = null) {
      if (!session) {
        lastBookRenderKey = "";
        bookLevelsEl.innerHTML = `<div class="empty">No DOM files found</div>`;
        bookTimeEl.textContent = "Latest visible";
        return;
      }
      bookTimeEl.textContent = dateTimeLabel(session.window_end_ms);
      const levels = safeArray(session.order_book_levels);
      if (!levels.length) {
        lastBookRenderKey = "";
        bookLevelsEl.innerHTML = `<div class="empty">${session.message || "No active DOM levels"}</div>`;
        return;
      }
      if (!chart || !Number.isFinite(chart.view.priceMin) || !Number.isFinite(chart.view.priceMax)) {
        lastBookRenderKey = "";
        bookLevelsEl.innerHTML = "";
        return;
      }

      const layoutBox = chart.layout();
      const plotBox = chart.plot(layoutBox);
      const stageRect = stage.getBoundingClientRect();
      const bookRect = bookLevelsEl.getBoundingClientRect();
      const priceMin = Math.min(chart.view.priceMin, chart.view.priceMax);
      const priceMax = Math.max(chart.view.priceMin, chart.view.priceMax);
      const tick = Math.max(num(session.tick_size, 0), 0.00000001);
      const highTick = Math.floor((priceMax / tick) + 0.000001);
      const lowTick = Math.ceil((priceMin / tick) - 0.000001);
      const visibleTickCount = Math.max(0, highTick - lowTick + 1);
      const tickPixel = Math.abs(
        chart.yForPrice((highTick * tick) + tick, plotBox) - chart.yForPrice(highTick * tick, plotBox)
      );
      const maxBookRows = Math.max(1, Math.floor(bookRect.height / BOOK_ROW_MIN_HEIGHT));
      const textStepTicks = tickPixel > 0 ? Math.max(1, Math.ceil(BOOK_ROW_MIN_HEIGHT / tickPixel)) : 1;
      const stepTicks = Math.max(textStepTicks, Math.ceil(visibleTickCount / maxBookRows));
      const rowHeight = Math.max(BOOK_ROW_MIN_HEIGHT, tickPixel * stepTicks);
      const levelByTick = new Map();
      for (const level of levels) {
        const index = priceTickIndex(level.price, tick);
        if (Number.isFinite(index)) levelByTick.set(index, level);
      }
      const executeByTick = new Map();
      const activityStats = new Map();
      const sourceEvents = rawSessionEvents(session, { rawOnly: true });
      for (const event of sourceEvents) {
        const ms = num(event.timestamp_ms, NaN);
        const price = num(event.price, NaN);
        if (!Number.isFinite(ms) || ms < chart.view.startMs || ms > chart.view.endMs) continue;
        if (!Number.isFinite(price) || price < priceMin || price > priceMax) continue;
        if (!chart.matchesMarkerIdFilter(event) || !chart.matchesMarkerPriceFilter(event)) continue;
        const index = priceTickIndex(price, tick);
        if (!Number.isFinite(index)) continue;
        const type = String(event.event_type || "");
        const side = String(event.side || "");
        const rawSize = Math.max(0, num(rawEventAmount(event), 0));
        if (rawSize <= 0) continue;
        if (type === "EXECUTE") {
          const aggregate = executeByTick.get(index) || { buy: 0, sell: 0 };
          if (side === "ASK") aggregate.buy += rawSize;
          if (side === "BID") aggregate.sell += rawSize;
          executeByTick.set(index, aggregate);
        }
        const orderId = String(event.order_id || event.venue_order_id || "").trim();
        if (!orderId) continue;
        const key = `${index}|${orderId}|${type}|${side}`;
        let stat = activityStats.get(key);
        if (!stat) {
          stat = {
            index,
            price: priceLabelForTickIndex(index, tick),
            orderId,
            type,
            side,
            rawTotal: 0,
            count: 0,
            lastSize: 0,
            lastCurrent: 0,
            positiveRefillCount: 0,
            positiveRefillTotal: 0,
          };
          activityStats.set(key, stat);
        }
        stat.rawTotal += rawSize;
        stat.count += 1;
        stat.lastSize = rawSize;
        stat.lastCurrent = Math.max(0, num(event.order_current_contracts, 0));
        const positiveRefillCount = Math.max(0, Math.round(num(event.positive_refill_count ?? event.refill_count, 0)));
        const positiveRefill = Math.max(0, num(event.positive_refill_contracts, 0));
        if (positiveRefillCount > 0) {
          stat.positiveRefillCount += positiveRefillCount;
          stat.positiveRefillTotal += positiveRefill;
        }
      }
      if (!sourceEvents.length) {
        for (const level of levels) {
          const index = priceTickIndex(level.price, tick);
          if (!Number.isFinite(index)) continue;
          const buy = Math.max(0, num(level.raw_buy_execute_contracts, 0));
          const sell = Math.max(0, num(level.raw_sell_execute_contracts, 0));
          if (buy || sell) executeByTick.set(index, { buy, sell });
        }
      }
      const bestActivityByTick = new Map();
      for (const stat of activityStats.values()) {
        const current = bestActivityByTick.get(stat.index);
        if (!current || stat.rawTotal > current.rawTotal || (stat.rawTotal === current.rawTotal && stat.count > current.count)) {
          bestActivityByTick.set(stat.index, stat);
        }
      }
      const topActivityByTick = new Map(
        [...bestActivityByTick.values()]
          .sort((a, b) => (b.rawTotal - a.rawTotal) || (b.count - a.count))
          .slice(0, 5)
          .map((stat, index) => [stat.index, { ...stat, rank: index + 1 }])
      );
      const levelSignature = levels
        .map(level => `${level.price}:${level.bid_contracts || 0}:${level.ask_contracts || 0}:${level.raw_buy_execute_contracts || 0}:${level.raw_sell_execute_contracts || 0}:${level.top_order_id || ""}:${level.top_order_side || ""}:${level.top_order_type || ""}:${level.top_order_size || 0}:${level.top_order_rank || 0}:${level.top_order_positive_refill_count || 0}:${level.top_order_positive_refill_total || 0}`)
        .join(";");
      const executeSignature = [...executeByTick.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([index, aggregate]) => `${index}:${aggregate.buy}:${aggregate.sell}`)
        .join(";");
      const activitySignature = [...topActivityByTick.entries()]
        .sort((a, b) => a[0] - b[0])
        .map(([index, stat]) => `${index}:${stat.orderId}:${stat.type}:${stat.side}:${stat.rawTotal}:${stat.count}:${stat.positiveRefillCount}:${stat.positiveRefillTotal}`)
        .join(";");
      const renderKey = [
        session.window_start_ms,
        session.window_end_ms,
        Math.round(chart.view.startMs),
        Math.round(chart.view.endMs),
        priceMin.toFixed(8),
        priceMax.toFixed(8),
        tick,
        stepTicks,
        Math.round(stageRect.top),
        Math.round(stageRect.height),
        Math.round(bookRect.top),
        Math.round(bookRect.height),
        levels.length,
        levelSignature,
        executeSignature,
        activitySignature,
      ].join("|");
      if (renderKey === lastBookRenderKey) return;
      lastBookRenderKey = renderKey;

      const visibleTickIndexes = new Set();
      for (let index = highTick; index >= lowTick; index -= stepTicks) {
        visibleTickIndexes.add(index);
      }
      for (const index of levelByTick.keys()) {
        if (index >= lowTick && index <= highTick) visibleTickIndexes.add(index);
      }
      for (const index of executeByTick.keys()) {
        if (index >= lowTick && index <= highTick) visibleTickIndexes.add(index);
      }
      for (const index of topActivityByTick.keys()) {
        if (index >= lowTick && index <= highTick) visibleTickIndexes.add(index);
      }
      const rows = [];
      const sortedTickIndexes = [...visibleTickIndexes].sort((a, b) => b - a);
      for (const index of sortedTickIndexes) {
        const price = index * tick;
        const level = levelByTick.get(index) || null;
        const execute = executeByTick.get(index) || { buy: 0, sell: 0 };
        const topActivity = topActivityByTick.get(index) || null;
        const displayLevel = topActivity
          ? {
              ...(level || { price: priceLabelForTickIndex(index, tick), bid_contracts: 0, ask_contracts: 0 }),
              top_order_id: topActivity.orderId,
              top_order_side: topActivity.side,
              top_order_type: topActivity.type,
              top_order_size: topActivity.rawTotal,
              top_order_count: topActivity.count,
              top_order_rank: topActivity.rank,
              top_order_last_size: topActivity.lastSize,
              top_order_current_contracts: topActivity.lastCurrent,
              top_order_positive_refill_count: topActivity.positiveRefillCount,
              top_order_positive_refill_total: topActivity.positiveRefillTotal,
            }
          : level;
        const buyExec = Math.round(execute.buy);
        const sellExec = Math.round(execute.sell);
        const buyClass = buyExec > 10 ? " exec-hot" : "";
        const sellClass = sellExec > 10 ? " exec-hot" : "";
        const execHtml = (buyExec || sellExec)
          ? `<span class="exec-buy${buyClass}">${buyExec || ""}</span><span>/</span><span class="exec-sell${sellClass}">${sellExec || ""}</span>`
          : "";
        const y = chart.yForPrice(price, plotBox);
        const screenY = stageRect.top + y;
        const top = screenY - bookRect.top - (rowHeight / 2);
        if (top > bookRect.height || top + rowHeight < 0) continue;
        const fontSize = rowFontSize(rowHeight);
        rows.push(
          `<div class="book-row" data-price="${priceLabelForTickIndex(index, tick)}" style="top:${top.toFixed(1)}px;height:${rowHeight.toFixed(1)}px;font-size:${fontSize}px">` +
          `<span class="bid">${level?.bid_contracts || ""}</span>` +
          `<span class="price">${level?.price || priceLabelForTickIndex(index, tick)}</span>` +
          `<span class="ask">${level?.ask_contracts || ""}</span>` +
          `<span class="exec" title="Market Buy / Market Sell">${execHtml}</span>` +
          bookTopOrderCells(displayLevel) +
          `</div>`
        );
      }
      bookLevelsEl.innerHTML = rows.length
        ? rows.join("")
        : `<div class="empty">${session.message || "No active DOM levels in view"}</div>`;
    }

    const chart = new CanvasDomTimelineChart();
    dateInputEl.addEventListener("change", () => {
      chart.selectedDateOverride = dateInputEl.value;
      chart.view = { startMs: 0, endMs: 0, priceMin: NaN, priceMax: NaN };
      chart.navigation = { startMs: 0, endMs: 0 };
      chart.fullNavigation = { startMs: 0, endMs: 0 };
      chart.globalNavigation = { startMs: 0, endMs: 0 };
      chart.renderRange = { startMs: 0, endMs: 0 };
      chart.autoScale = true;
      chart.userTimeZoomed = false;
      chart.manualTimeZoom = null;
      chart.dayPrefetchKeys.clear();
      chart.abortDayPrefetches();
      chart.pendingIcebergPathFocus = chart.hasActiveIcebergFilter();
      autoScaleEl.classList.add("active");
      loadingIndicatorEl.style.opacity = "1";
      statusEl.textContent = "Loading DOM...";
      fetchDomData(null, { selectedDate: chart.selectedDateOverride });
    });

    function domDataUrl(view = null, options = {}) {
      const params = new URLSearchParams();
      if (view?.startMs) params.set("start_time_ms", String(Math.floor(view.startMs)));
      if (view?.endMs) params.set("end_time_ms", String(Math.floor(view.endMs)));
      const includePriceRange = options.includePriceRange === true
        || (options.includePriceRange !== false && chart?.autoScale === false);
      if (includePriceRange && Number.isFinite(Number(view?.priceMin))) {
        params.set("price_min", String(Number(view.priceMin)));
      }
      if (includePriceRange && Number.isFinite(Number(view?.priceMax))) {
        params.set("price_max", String(Number(view.priceMax)));
      }
      const selectedDate = Object.prototype.hasOwnProperty.call(options, "selectedDate")
        ? options.selectedDate
        : "";
      if (selectedDate) params.set("date", selectedDate);
      if (chart?.hasActiveIcebergFilter?.()) {
        params.set("iceberg_min", String(Math.floor(Number(chart.icebergMinContracts))));
        const icebergOrderIds = chart.icebergOrderIdsForRequest?.() || [];
        if (icebergOrderIds.length) params.set("iceberg_order_ids", icebergOrderIds.join(","));
        const icebergPathBounds = chart.icebergPathBoundsForRequest?.() || null;
        if (icebergPathBounds) {
          params.set("iceberg_path_start_ms", String(Math.floor(icebergPathBounds.startMs)));
          params.set("iceberg_path_end_ms", String(Math.floor(icebergPathBounds.endMs)));
        }
      }
      const suffix = params.toString() ? `?${params}` : "";
      return `/dom-data/${ACTIVE_TIMEFRAME}${suffix}`;
    }

    async function fetchDomData(view = null, options = {}) {
      const generation = Number.isFinite(Number(options.generation))
        ? Number(options.generation)
        : ++chart.fetchGeneration;
      if (chart.abortController) chart.abortController.abort();
      const controller = new AbortController();
      chart.abortController = controller;
      const showLoading = options.showLoading !== false;
      if (showLoading) {
        loadingIndicatorEl.style.opacity = "1";
      }
      const url = domDataUrl(view, options);
      try {
        const response = await fetch(url, { cache: "no-store", signal: controller.signal });
        const snapshot = await response.json();
        if (generation !== chart.fetchGeneration) return;
        statusEl.textContent = statusText(snapshot);
        chart.setSnapshot(snapshot);
        chart.scheduleAdjacentDayPrefetch(1200);
      } catch (error) {
        if (error?.name === "AbortError") return;
        statusEl.textContent = `DOM data error: ${error}`;
      } finally {
        if (chart.abortController === controller) {
          loadingIndicatorEl.style.opacity = "0";
        }
      }
    }

    function statusText(snapshot) {
      const session = safeArray(snapshot?.sessions)[0] || null;
      if (!session) return `Updated ${dateTimeLabel(snapshot?.generated_at_utc)} | no sessions`;
      const debug = session.debug || {};
      const message = session.message ? ` | ${session.message}` : "";
      return `Updated ${dateTimeLabel(snapshot.generated_at_utc)} | ${session.provider_symbol} ${session.timeframe} | files ${debug.dom_file_count || 0} | MBO ${debug.mbo_event_count || 0} | levels ${debug.price_level_count || 0}${message}`;
    }

    renderLinks();
    fetchDomData();
  </script>
</body>
</html>
'''

DOM_HTML_PAGE = dom_html_page(DEFAULT_FOOTPRINT_TIMEFRAME)
