# Task: Produce an implementation plan — Active Contract Observability

**Mode: PLAN ONLY. Do not modify any file. Output a plan document.**

---

## 1. Objective

Make the currently-selected futures contract **visible** everywhere it is used, and make contract switches **detectable**. This is a pure observability change. We are not changing selection behaviour in this phase.

## 2. Background — why this matters

This repo ingests Databento MBO data for CME futures using a **parent symbol** query (`NQ.FUT`, `stype_in: parent`). The archive therefore contains multiple outright expiries (NQM6, NQU6, NQZ6, NQH7, ...) plus calendar spreads (NQM6-NQU6, ...).

The pipeline currently:
1. Filters to outrights only, via a regex on resolved DBN symbols.
2. Picks a single **dominant** `instrument_id` by highest summed `size`.
3. Discards everything else.
4. Applies a second defensive filter in the DOM viewport layer.

Critically, step 2 is decided **per batch/window**, not once per session. There is no persistent "current contract" state and no calendar-based rollover.

**The failure mode this causes:** a user watches order flow, sees large participants entering, and waits for signs of their exit. Meanwhile the pipeline silently switches to a different expiry. The user keeps watching a full, healthy-looking ladder — but it is now a *different instrument*. The exit happens in the contract they can no longer see, and unrelated flow in the new contract may be misread as that exit.

This is a **silent** error. Nothing in the UI indicates the subject changed. That is what this task fixes.

There is a second, related risk: the footprint path and the DOM path appear to select the active contract **independently and at different granularities** (footprint at trading-day level, DOM at batch level). They may therefore disagree with each other at the same wall-clock moment. Confirm or refute this.

## 3. Scope of this phase

In scope:
- Propagate the selected `instrument_id` **and** its human-readable contract symbol (e.g. `NQM6`) from wherever selection happens, through to every payload consumed by the UI.
- Render it in the UI for both the footprint view and the DOM view.
- Detect and surface a switch: when the active contract for a session changes between renders/windows, make it visibly obvious.
- Surface the disagreement case if footprint and DOM resolve to different contracts simultaneously.

Explicitly **out of scope** for this phase — do not implement, but do note in the plan where they would go:
- Changing the selection criterion (e.g. traded-volume-only instead of summed size across all MBO actions).
- Session-level stabilisation / hysteresis.
- Retaining non-dominant contracts or spreads in storage.
- Multi-contract simultaneous display.
- Anything related to open interest modelling.

## 4. Investigation checklist

Answer each of these with concrete file paths and line numbers **verified against the current tree**:

1. Where is the active instrument selected? Identify every independent selection site (there is more than one — DOM decode path, DOM viewport path, and the trades/footprint path at minimum).
2. At what granularity does each selection site operate — per batch, per window, per trading day, per session?
3. Where does the `instrument_id -> contract symbol` mapping exist, and how far does it currently travel? Establish whether the mapping is available at each point where we need to display a symbol, or whether it must be threaded through additional layers.
4. Trace `contract_symbols` end to end. It is persisted in the `dom_sources` table and appears in at least one payload. Determine: what exactly does it contain (all discovered outrights, or the selected one?), which payloads carry it, and whether the UI consumes it anywhere.
5. Identify every payload construction site that reaches the UI, including **error/fallback payloads** — these must carry the same keys with safe defaults so the frontend never sees `undefined`.
6. Identify the UI render sites for session titles/headers in both views.
7. Determine whether the replay path (reading back from the `dom_events` index) preserves enough information to know which contract a replayed event belongs to, or whether the answer is only knowable at decode time.
8. Check whether any caching layer (snapshot cache, viewport cache, partition cache) would serve stale contract identity after a switch, and whether cache keys need to include the contract.

## 5. Starting points — verify, do not trust

These references come from a prior analysis and may be stale or wrong. Confirm each against the actual code before relying on it. If a reference is wrong, say so explicitly in the plan.

| Concern | Reported location |
|---|---|
| Outright regex / symbol resolution | `cme_provider/local_data.py` ~1081 (`_outright_instrument_symbols`) |
| MBO decode + outright filter + dominant pick | `DOM/data_provider.py` ~1663 (`_events_from_records`) |
| Dominant ID selection | `DOM/data_provider.py` ~1837 (`_active_instrument_id_from_records`) |
| Second filter in DOM viewport | `DOM/engine.py` ~477 (`_filter_provider_result_to_primary_instrument`), called from ~264 |
| Footprint active contract (day-level) | `cme_provider/local_data.py` ~1018 (`_active_outright_contract`), applied ~352 |
| Replay source, no instrument filter | `process/data_sources.py` ~299 |
| DOM payload construction | `session_service.py` `_build_dom_timeline_session_payload` / `dom_timeline_payload` |
| Footprint payload construction | `session_service.py` `_build_cme_footprint_session_payload` |
| UI title render sites | `html_server.py` ~1816 (footprint) and ~4170 (DOM) |

## 6. Required plan output

Produce a document containing:

1. **Findings** — answers to §4, with verified paths and line numbers. Call out anything that contradicts §5.
2. **Data flow diagram** — where contract identity is created, where it is lost today, and where it must be threaded through. Cover both the DOM path and the footprint path.
3. **Design decision: naming.** Propose the payload key names and stick to them consistently across every site. Note the existing `contract_symbols` key and decide whether to reuse, rename, or add alongside it — and justify. Beware the existing naming collision: throughout this codebase `contracts` usually means *quantity in lots*, not *futures contract identity*. Choose names that cannot be confused with it.
4. **Change list** — ordered, file by file, with a description of each edit and its dependencies on other edits. Include type/dataclass changes, not just call sites.
5. **Switch detection design** — where switch state is held (server or client), how a switch is represented in the payload, and how it is surfaced in the UI. Consider that the switch may need to be attributable to a point in time on the chart, not just "it changed since last poll".
6. **Backward compatibility** — what happens to already-indexed data and existing cache entries that lack the new fields.
7. **Verification plan** — how to prove this works. Include at minimum: a normal mid-cycle day (expect a single stable contract), and a roll-window day (expect switches, possibly oscillating). Name specific dates or data files present in the repo if you can identify them.
8. **Open questions** — anything you could not determine from the code and need a human decision on.

## 7. Constraints

- Do not change filtering or selection behaviour. If you find a place where adding observability is impossible without altering behaviour, flag it as an open question rather than designing around it.
- Prefer threading real values through existing structures over recomputing selection in a second place. Two independent computations of "which contract" is exactly the class of bug we are trying to expose.
- The plan will be executed by a different agent in a fresh session with no memory of this conversation. It must be self-contained: any reference to a file, function, or symbol must include enough context to be located without prior knowledge.
- Where you are uncertain, say so in the plan rather than guessing.
