# Valhalla Fjorge — Architecture

Target architecture **post-refactor** (after design docs 014–022 are implemented).
For the current state during implementation, run `git log --oneline` to see how far the refactor has progressed.

---

## 1. End-to-end Pipeline

How data flows from Discord through the parser to final reports.

### Mermaid

```mermaid
flowchart LR
    subgraph EXT[External sources]
        DC[Discord DMs<br/>Valhalla bot]
        LP[LpAgent API]
        ME[Meteora DLMM API]
        SR[Solana RPC]
    end

    subgraph INGEST[Ingest]
        DCE[dce_pull.py<br/>DiscordChatExporter]
        CLIP[save_clipboard.ps1]
    end

    INDIR[/input/*.txt/]
    ARC[/archive/*.txt/]

    subgraph CORE[valhalla parser core]
        READ[readers]
        EVP[event_parser]
        MAT[matcher]
        MTC[meteora]
        CSW[csv_writer]
    end

    subgraph CHK[Cross-checks &amp; recovery]
        LPP[lpagent_pipeline]
        CCC[cross_check]
        CGP[coverage_gaps]
        BRR[balance_recovery]
        DGP[discord_gaps]
    end

    subgraph REPORT[Reports]
        LR[loss_report/]
        REC[recommendations/]
        WTR[wallet_trend_report]
        CHT[charts]
        UTL[utilization]
    end

    OUT[/output/positions.csv<br/>loss_analysis.md<br/>wallet_trend.md<br/>summary.csv<br/>*.png/]

    DC --> DCE --> INDIR
    DC --> CLIP --> INDIR
    INDIR --> READ --> EVP --> MAT --> MTC --> CSW --> OUT
    SR --> MAT
    ME --> MTC
    LP --> LPP --> CCC --> OUT
    INDIR -.archived.-> ARC
    OUT --> LR --> OUT
    OUT --> REC --> OUT
    OUT --> WTR --> OUT
    OUT --> CHT --> OUT
    OUT --> UTL --> LR
    OUT --> CGP
    OUT --> BRR
    OUT --> DGP
```

### ASCII

```
                                External
   +-------------+         +-----------------+         +------------+
   | Discord DMs |         |  LpAgent API    |         |  Meteora   |
   |  (Valhalla) |         | (cross-check)   |         |  DLMM API  |
   +------+------+         +--------+--------+         +-----+------+
          |                         |                        |
          v                         |                        |
   +------+-----------+             |                        |
   | dce_pull.py      |             |                        |
   | save_clipboard   |             |                        |
   +------+-----------+             |                        |
          |                         |                        |
          v                         |                        |
   +------+----------+              |                        |
   |   input/*.txt   |              |                        |
   +------+----------+              |                        |
          |                         |                        |
          v                         v                        |
   +------+--------------------------------------------------+--------+
   |                       valhalla parser                            |
   |                                                                  |
   |   readers --> event_parser --> matcher --> meteora --> csv_writer|
   |                                   ^                              |
   |                                   |                              |
   |                              Solana RPC                          |
   |                                                                  |
   |   lpagent_pipeline --> cross_check                               |
   |   coverage_gaps     |  balance_recovery  |  discord_gaps         |
   +------+-----------------------------------------------------------+
          |
          v
   +------+--------------------------------------------------------+
   |                        output/                                 |
   |   positions.csv  | summary.csv  | skip_events.csv               |
   |   insufficient_balance.csv | address_cache.json                 |
   +------+-----------------------------------------------------+--+
          |                                                     |
          v                                                     v
   +------+--------------------+                  +-------------+--+
   |  Reports (post-process)   |                  |   Charts       |
   |                           |                  |                |
   |  loss_report/             |                  | daily_pnl.png  |
   |  recommendations/         |                  | rolling_3d.png |
   |  wallet_trend_report      |                  | utilization    |
   |  utilization              |                  | filter_impact  |
   +------+--------------------+                  +-------------+--+
          |
          v
   +------+----------------------+
   |  output/loss_analysis.md    |
   |  output/wallet_trend.md     |
   +-----------------------------+

  archive/ <-- input/*.txt are moved here after successful parse
```

---

## 2. CLI Dispatch

Two entry-point shims, one orchestrator each.

### Mermaid

```mermaid
flowchart TB
    USR[User]
    M1[main.py shim]
    M2[valhalla_parser_v2.py shim]

    USR -->|python main.py| M1
    USR -->|python valhalla_parser_v2.py| M2
    USR -->|track_recommendations.bat| M2

    M1 --> PIPE[valhalla.pipeline.main]
    M2 --> CLI[valhalla.cli.main]

    subgraph PKG_PIPE[valhalla/pipeline/]
        PMENU[menu.py<br/>Case A/B/C]
        PSTATE[state.py<br/>.dce_state.json]
        PCHUNK[chunking.py]
        PRUN[runner.py<br/>subprocess wrappers]
        PSUM[summary.py]
    end

    PIPE --> PMENU
    PIPE --> PSTATE
    PIPE --> PCHUNK
    PIPE --> PRUN
    PIPE --> PSUM

    PRUN -.spawns.-> DCEPULL[dce_pull.py]
    PRUN -.spawns.-> M2
    PRUN -.spawns.-> RECALC[tools/recalc_pending.py]

    CLI --> ARG{argparse mode}
    ARG -->|--report loss| LR[loss_report]
    ARG -->|--report wallet-trend| WTR[wallet_trend_report]
    ARG -->|--track| TRK[track_mode]
    ARG -->|--backtest| BT[backtest]
    ARG -->|--cross-check| LPP[lpagent_pipeline]
    ARG -->|no args| MENU[_interactive_menu]
    ARG -->|default| FULL[full parse + all reports]
```

### ASCII

```
  User
   |
   +---> python main.py
   |        |
   |        v
   |   main.py shim (UTF-8 setup, ~30 lines)
   |        |
   |        v
   |   valhalla.pipeline.main()
   |        |
   |        +-- pipeline/menu.py    (Case A: first run | B: recent | C: stale)
   |        +-- pipeline/state.py   (.dce_state.json)
   |        +-- pipeline/chunking.py (day-by-day)
   |        +-- pipeline/runner.py  --[subprocess]--> dce_pull.py
   |        |                       --[subprocess]--> valhalla_parser_v2.py
   |        |                       --[subprocess]--> tools/recalc_pending.py
   |        +-- pipeline/summary.py
   |
   +---> python valhalla_parser_v2.py [args]
   |       (or track_recommendations.bat)
            |
            v
       valhalla_parser_v2.py shim (~30 lines)
            |
            v
       valhalla.cli.main()
            |
            +-- argparse dispatch:
            |     --report loss          --> loss_report/
            |     --report wallet-trend  --> wallet_trend_report
            |     --report recommendations --> recommendations/
            |     --track                --> track_mode
            |     --backtest             --> backtest
            |     --cross-check DATE     --> lpagent_pipeline
            |     (no args)              --> _interactive_menu()
            |     (default)              --> full parse + all reports
```

---

## 3. Module Map (post-refactor)

The `valhalla/` package after docs 014–022 are implemented.

### Mermaid

```mermaid
flowchart LR
    subgraph ENTRY[Entry shims]
        E1[main.py]
        E2[valhalla_parser_v2.py]
    end

    subgraph PIPE_PKG[valhalla/pipeline/]
        direction TB
        P1[__init__: main]
        P2[state]
        P3[chunking]
        P4[menu]
        P5[runner]
        P6[summary]
    end

    subgraph CLI_MOD[valhalla/cli.py]
        C1[main + argparse + _interactive_menu]
    end

    subgraph CORE_MODS[Core parse]
        direction TB
        K1[readers]
        K2[event_parser]
        K3[matcher]
        K4[meteora]
        K5[csv_writer]
        K6[merge]
        K7[json_io]
        K8[models]
        K9[alias_resolver]
        K10[solana_rpc]
        K11[analysis_config]
    end

    subgraph CROSS[Cross-checks &amp; diagnostics]
        direction TB
        X1[lpagent_pipeline]
        X2[lpagent_client]
        X3[cross_check]
        X4[coverage_gaps]
        X5[balance_recovery]
        X6[discord_gaps]
    end

    subgraph REPORTS_PKG[Reports]
        direction TB
        R1[loss_report/<br/>__init__, formatters,<br/>tables, action_items,<br/>report_builder]
        R2[recommendations/<br/>__init__, wallet_rules,<br/>filter_rules,<br/>position_guard]
        R3[wallet_trend_report]
        R4[charts]
        R5[utilization]
        R6[loss_analyzer]
        R7[source_wallet_analyzer]
        R8[recommendations_tracker]
    end

    subgraph MODES[Interactive modes]
        M1[backtest]
        M2[track_mode]
    end

    E1 --> PIPE_PKG
    E2 --> CLI_MOD
    CLI_MOD --> CORE_MODS
    CLI_MOD --> CROSS
    CLI_MOD --> REPORTS_PKG
    CLI_MOD --> MODES
    R1 --> R6
    R1 --> R2
    R1 --> R5
    R2 --> R6
    M1 --> R6
    M2 --> R8
```

### ASCII

```
  ENTRY SHIMS                           Lines
  -----------                           -----
  main.py                                ~30
  valhalla_parser_v2.py                  ~30

  valhalla/                              Lines
  ---------                              -----
  __init__.py                              5
  cli.py                                 ~700  (was main() in parser, doc 020)
  analysis_config.py                     143

  valhalla/pipeline/   (doc 021)
    __init__.py            (main)        ~30
    state.py                              ~50
    chunking.py                           ~50
    menu.py                              ~200
    runner.py                            ~250
    summary.py                            ~50

  valhalla/loss_report/   (doc 018)
    __init__.py                           ~10
    formatters.py                         ~50
    tables.py                            ~150
    action_items.py                      ~200
    report_builder.py                    ~250

  valhalla/recommendations/   (doc 017)
    __init__.py                           ~10
    wallet_rules.py                      ~250  (Rules A, B, C, F)
    filter_rules.py                      ~150  (Rule D)
    position_guard.py                     ~50

  Cross-checks & diagnostics                   Source
  --------------------------                   ------
  lpagent_pipeline.py     (doc 015)            extracted from parser
  lpagent_client.py                            existing
  cross_check.py                               existing
  coverage_gaps.py        (doc 016)            extracted from parser
  balance_recovery.py     (doc 016)            extracted from parser
  discord_gaps.py                              existing

  Core parse                                   Status
  ----------                                   ------
  readers.py                                   existing, OK as-is
  event_parser.py            584 lines         keep (per doc 022)
  matcher.py                 757 lines         keep (per doc 022)
  meteora.py                 260 lines         existing
  merge.py                   633 lines         keep (per doc 022)
  csv_writer.py              428 lines         existing
  json_io.py                 262 lines         existing
  models.py                  297 lines         existing
  alias_resolver.py           87 lines         existing
  solana_rpc.py              197 lines         existing

  Reports / analysis                           Status
  ------------------                           ------
  charts.py                 1500 lines         keep (per doc 022)
  loss_analyzer.py          1096 lines         keep (per doc 022)
  utilization.py             231 lines         existing
  source_wallet_analyzer.py  337 lines         existing
  recommendations_tracker.py 268 lines         existing
  wallet_trend_report.py     240 lines         existing

  Interactive modes                            Source
  -----------------                            ------
  backtest.py             (doc 019)            extracted from parser
  track_mode.py           (doc 019)            extracted from parser

  Verification (not in valhalla/)              Source
  -------------------------------              ------
  tests/verify_baseline.py  (doc 014)          new — diffs vs _baseline_pre_refactor/
```

---

## Notes

- **Doc 022 explicitly decided NOT to split** `charts.py`, `loss_analyzer.py`, `matcher.py`, `merge.py`, `event_parser.py` despite their size — each is a coherent single-domain module. See `docs/022-valhalla-package-review.md` for the rationale.
- **`tools/`** scripts (`recalc_pending.py`, `compare_positions.py`, `cross_reference.py`, `discord_gaps.py`, etc.) are standalone utilities invoked manually or by `pipeline/runner.py`. They import from `valhalla/` but are not part of the package.
- **`web/`** (Next.js TypeScript port) is gitignored and out of scope for the Python refactor.
