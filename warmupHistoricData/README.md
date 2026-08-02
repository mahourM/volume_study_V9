# Warmup Historic Data

Place pre-live historical DOM and L2 files here so the live robot can warm up
order state before consuming live API events.

Recommended multi-symbol layout:

```text
warmupHistoricData/
  manifest.json
  dom/
    NQ.FUT/
      2026-07-01.dbn.zst
      2026-07-02.dbn.zst
  l2/
    NQ.FUT/
      2026-07-01.jsonl
      2026-07-02.jsonl
```

Use one subfolder per provider symbol, for example `NQ.FUT`, `ES.FUT`,
or `MNQ.FUT`. The catalog also matches the MT5 root symbol, so a live session
for `NQ` can find files stored under `NQ.FUT`.

`manifest.json` is optional for discovery, but it is the preferred place for
explicit symbol metadata when multiple symbols or providers are present.
