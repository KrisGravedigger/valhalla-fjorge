---
critical: true
execution: afk
---

# [033] Discord embed formats — restore event ingestion (gen1 + gen2 + gen3)

## Intent

On **2026-08-23T06:19** the Valhalla Bot stopped posting plain-text Discord messages and
switched to **Discord embeds**. `dce_to_input.py` deliberately skips embeds (comment:
*"Embeds contain metlex performance charts (images) — irrelevant to the text parser"*).
That assumption is now false: `content` is empty on every message and the entire event
body lives in `embeds[0]`. The converter writes nothing, the parser has no input, and the
pipeline reports `All steps complete`.

Result: **~1720 bot events since 2026-08-23T06:19 are invisible**, including **437 position
closes** carrying real PnL. Nothing is lost upstream — the messages are still on Discord and
can be re-pulled once this lands.

The bot changed format **twice in nine hours**, so this doc must support three dialects:

| Gen | Window | Shape |
|---|---|---|
| **gen1** | until `2026-08-23T06:19` | plain-text `content`, legacy emoji headers |
| **gen2** | `2026-08-23T06:24` → `15:25` | embed, body close to legacy but bold-labelled |
| **gen3** | from `2026-08-23T15:31` | embed, redesigned labels, position ID moved to footer |

gen1 must keep working: `archive/*.txt` is re-parsed by the verification harness.

**Design principle — the archived `.txt` is a financial audit trail.** The converter must
render what Discord actually said. It must **not** synthesise legacy strings (e.g. writing
`🏁 Starting SOL balance: 0.26 SOL ($25.01 USD)` from `**Balance** 0.26 SOL → 1.33 SOL`).
Transport work belongs in `dce_to_input.py` / `readers.py`; semantics stay in
`event_parser.py`. If an implementation step requires the converter to know what an event
*means*, stop and re-read this Intent.

## Context

- `dce_to_input.py` — DCE JSON → `input/dce_*.txt`. Currently drops `embeds` entirely
  (`# --- Embeds: skip entirely ---`) and `sys.exit(0)` on empty output.
- `dce_pull.py` — runs DCE, calls the converter, then **deletes the JSON**. On an empty
  conversion the evidence is destroyed (same failure shape as the CP437/Unicode crash fixed
  2026-07-22).
- `run_pipeline.py` — treats a zero-message pull as success and continues to NAV/charts.
- `valhalla/readers.py` — `PlainTextReader` splits on `[YYYY-MM-DDTHH:MM] Author:`, extracts
  Solscan signatures via `LABELED_SOLSCAN_PATTERN` (label = text immediately before the link;
  `'target' in label` ⇒ target tx, else bot tx), strips `[url]` brackets, returns
  `ParsedMessage`.
- `valhalla/event_parser.py` — `_classify_and_parse_message` dispatches on substring markers,
  then per-event regex. Unparsed messages increment `unparsed_counts`.
- `tests/fixtures/discord_embeds/valhalla_format_variants.json` — **44 curated real messages,
  one per format variant**, covering all three generations. `_variants` records how often each
  variant occurred over the 3-day window.
- `tests/fixtures/discord_embeds/VARIANTS.md` — the same 44 rendered as readable
  author/title/footer/description blocks. **Read this before writing any regex.**

### Known holes in HEAD that this doc closes

1. Embeds dropped ⇒ zero input since 2026-08-23T06:19.
2. Empty conversion is not an error ⇒ silent multi-day data loss.
3. `TARGET_PATTERN = r'Target:\s*(\S+)'` requires a colon. gen2/gen3 write `**Target** 20260618_gh7gW8Ft`.
   This one pattern breaks open / close / skip / already-closed simultaneously.
4. Tx attribution: gen3 replaced `Your Solscan 1` / `Target Tx 1` labels with section lines
   `**Yours** … [Orb](…) · [Solscan](…)` and `**Target tx** … [Orb](…) · [Solscan](…)`.
   The label captured before each link is `Solscan` in both cases ⇒ **target transactions
   would be silently recorded as the user's own.** This is the highest-risk item in the doc:
   it corrupts matching without raising an error.
5. Open events: gen3 moved the position ID out of the header into
   `embeds[0].footer.text` = `"Valhalla · CV1C...BoBG"`.

## Goals

- Every event type the parser handles today parses identically from gen1, gen2 and gen3.
- Tx attribution (yours vs target) is correct in all three dialects.
- A pull that yields zero parseable messages fails loudly and keeps its evidence.
- An unrecognised embed variant produces a visible warning, never a silent drop.

## Non-Goals

- **No backfill run.** Re-ingesting 2026-08-23…now into `output/*.csv` is a separate manual
  step performed by the operator after review. Do not run the pipeline against production
  output.
- **Partial-removal events stay unparsed.** `event_parser` has no branch for
  `Partially Removed DLMM Liquidity` today; `Removed liquidity · DLMM` likewise gets none.
  Rendering it into the `.txt` is enough. No regression, no new event type.
- **Do not fix `target_wallet_address` extraction.** `LPAGENT_PATTERN` matches
  `app.lpagent.io/portfolio?address=`, which the bot abandoned long before this change (0 hits
  across archives since at least 2026-07-29; it now writes
  `valhalla-bot.app/dashboard/wallet/<ADDRESS>`). That field has been `None` for a month and is
  consumed by 12 modules — populating it again could retroactively change matcher behaviour.
  Separate doc, separate decision.
- No changes to `matcher.py`, `merge.py`, `csv_writer.py` or any output schema.

## Technical Design

### 1. `dce_to_input.py` — render embeds, mechanically

Replace the "skip entirely" block. For each embed, in order, emit these lines (omit any part
that is empty):

```
<author.name>                          # e.g. "Closed DLMM Position! (6XpA...fxW6)"
<title> [<url>]                        # e.g. "LOOKSMAX-SOL [https://dexscreener.com/...]"
<description, run through _process_content()>
<footer.text>                          # e.g. "Valhalla · CV1C...BoBG"
```

Rules:

- `title`/`url`: emit `title [url]` when both present, bare `title` when only the title is,
  nothing when the title is empty.
- `description` goes through the existing `_process_content()` — it already turns
  `[Orb](<url>)` into `Orb [url]` and `<https://…>` into `url [url]`.
- **Keep `**` bold markers.** They are what Discord sent; gen1 already contains them and
  existing patterns tolerate them via `\*?\*?`. Do not strip them in the converter.
- Multiple embeds on one message: render each as its own block separated by a blank line.
  (Not observed in the fixture — all messages carry exactly one — but do not assume.)
- A message with an empty `content` and a non-empty embed is **no longer** skipped. The
  existing "no content and no attachments" skip stays for genuinely empty messages.
- Existing attachment handling is unchanged and still appended after the body.

### 2. `dce_to_input.py` / `dce_pull.py` / `run_pipeline.py` — fail loudly

- `dce_to_input.py`: when the export has messages but none produced output, print the count
  and the distinct `embeds[0].author.name` values seen, then `sys.exit(3)` (not 0).
- `dce_pull.py`: on converter exit ≠ 0, **keep the JSON** regardless of `--keep-json` and print
  its path, then propagate the exit code.
- `run_pipeline.py`: a non-zero Discord-pull step aborts the pipeline instead of continuing to
  recalc/NAV/charts.

### 3. `valhalla/readers.py` — line-scoped tx attribution

Current attribution reads the label immediately before each link. Add a **line-scoped** rule
that runs first, and keep the existing per-label rule as the fallback so gen1 is untouched:

For each line of the raw message, after markdown expansion:

| Line starts with (after optional `**`, emoji, whitespace) | All solscan links on that line go to |
|---|---|
| `Target tx`, `Target Tx`, `Target:`, `🎯` | `target_tx_signatures` |
| `Yours`, `Your Solscan`, `Your ` | `bot_tx_signatures` |
| anything else | fall through to the existing per-label rule |

The existing "fallback only when neither list got anything" behaviour stays.

Note the gen1 already-closed shape carries **only** `Target Tx N` links and no bot links —
that must keep producing an empty `bot_tx_signatures`, not a fallback-populated one.

Non-solscan links (`orbmarkets.io`, `metlex.io`, `dexscreener.com`, `jup.ag`,
`valhalla-bot.app`) must never enter either signature list.

### 4. `valhalla/event_parser.py` — dialect-tolerant patterns

Prefer **widening existing patterns** over adding parallel gen2/gen3 pattern sets. Only add a
separate pattern where the shape genuinely diverges.

**Label separator.** Introduce one shared fragment and use it everywhere a `Label:` prefix is
matched today, so `Target:`, `**Target**`, and `Target` all work:

```python
LABEL_SEP = r'\*{0,2}\s*:?\s*'   # after the label word(s), before the value
```

Affected: `TARGET_PATTERN`, `MARKET_CAP_PATTERN`, `JUP_SCORE_PATTERN`, `TOKEN_AGE_PATTERN`,
`YOUR_POS_PATTERN`, `TARGET_POS_PATTERN`, `RUG_TARGET_PATTERN`, `TAKE_PROFIT_TARGET_PATTERN`,
`ALREADY_CLOSED_TARGET_PATTERN`, `INSUF_*` (note gen2/gen3 wrap values in backticks, which
`_strip_inline_code` already removes: `**Your SOL balance** \`1.06 SOL\`` → `Your SOL balance 1.06 SOL`).

**Classification markers.** `_classify_and_parse_message` must additionally recognise:

| New marker | Routes to |
|---|---|
| `Opened · DLMM` (gen3 author line) | open |
| `Skipped - low market cap restriction`, `Skipped - token age restriction`, `Skipped - low Jupiter organic score restriction` | skip |
| `Skipping position due to` (unchanged, still used by gen2/gen3 Entry Mode) | skip |
| `Skipped · add liquidity` | add-liquidity skip (same handling as `Skipping Add Liquidity`) |
| `Skipped · nothing to remove` | same handling as `No liquidity was removed` |

Order matters: `Closed DLMM Position!` must still be tested before generic markers, and
`Rug Check Stop Loss Executed` before `Stop Loss Executed (DLMM)`.

**Open events.**

gen2 keeps the legacy body (`🆕 BidAsk 1-Sided Position | EYE-SOL [url]`, `**MC** $831,819.62 | Age: 5d ago`,
`**Jup Score** 73`, `**Target Pos** BULLS'S EYE: 0.0000 | SOL: 7.3140`, `**Your Pos** …`) and the
legacy header `Opened New DLMM Position! (4jbU...hYhE)` — the widened separator above should be
sufficient. Verify against the fixture; do not assume.

gen3 needs new extraction:

| `OpenEvent` field | gen3 source |
|---|---|
| `position_type` | author `Opened · DLMM · BidAsk 1-Sided` → `BidAsk` |
| `token_pair` | embed `title`, e.g. `LOOKSMAX-SOL` |
| `token_name` | `token_pair` up to the last `-` |
| `your_sol` | `### 0.50 SOL` (first description line) |
| `target_sol` | `7% of target's 7.14` → `7.14` |
| `market_cap` | `**MC** $1,574,702.576` |
| `token_age` | `**Age** 1w` |
| `jup_score` | `**Jup** 75` |
| `position_id` | **footer** `Valhalla · CV1C...BoBG` → `CV1CBoBG` via `_normalize_position_id` |
| `target` | `**Target** [20260713_5iB13i7i]` |

The footer may carry extra segments (`Valhalla · HNTn...qEUY · No action needed`) and may
contain no ID at all (`Valhalla`, `Valhalla · No action needed`). Match the
`xxxx...yyyy` shape, not "the second segment".

**Close events.** `CloseEvent` fields from gen2/gen3:

| Field | gen2 / gen3 source |
|---|---|
| `position_id` | author `Closed DLMM Position! (E1hG...hBX7)` (unchanged shape) |
| `target` | `**Target** [20260821_9JHGdZj4]` |
| `starting_sol` / `ending_sol` | `**Balance** 2.80 SOL → 4.09 SOL` |
| `starting_usd` / `ending_usd` | `**In USD** $261.13 → $381.43` |
| `total_sol` / `active_positions` | `**Total** 68.33 SOL ($6372.24) across 63 open positions` |

The arrow is U+2192 `→`. gen3 prefixes the PnL line with 🟢/🔴; gen2 does not; **two messages in
the fixture window have no PnL line and no `**Position**` line at all** — those must still parse
(PnL is not a `CloseEvent` field). Missing `**Balance**`/`**In USD**`/`**Total**` should
degrade to the existing defaults rather than dropping the event.

**Swap events.** gen1/gen2 keep `Swapped 3809 BULLS'S EYE (<mint>)`. gen3 restructured:
author `Swapped`, description `### 1127 creator capital` then ``**Token** `<mint>` ``. Add a
gen3 branch: amount and token name from the `###` line, mint from the `**Token**` line.

**Already-closed / rug / take-profit / stop-loss / failsafe / insufficient-balance.** These kept
their legacy wording inside the embed; the widened label separator plus the new line-scoped tx
attribution should cover them. Confirm each against the fixture.

### 5. Unknown-variant guardrail

When a Valhalla message reaches the end of `_classify_and_parse_message` without matching any
marker, count it under a new `unparsed_counts` key that includes the embed author line (first
line of the message, truncated), and make sure the existing end-of-parse warning surfaces it.
A future gen4 must be visible on the first run, not after two days.

## Touchable Files

```
dce_to_input.py
dce_pull.py
run_pipeline.py
valhalla/readers.py
valhalla/event_parser.py
tests/test_dce_to_input_embeds.py          (new)
tests/test_event_parser_embed_formats.py   (new)
tests/fixtures/discord_embeds/*            (read-only — do not regenerate)
docs/CHANGELOG.md
```

Do not edit anything else. In particular: no changes to `matcher.py`, `merge.py`,
`csv_writer.py`, `models.py`, or any file under `output/` or `archive/`.

## Acceptance Criteria

### AC-1: Every fixture variant converts
`dce_to_input.py` run against `valhalla_format_variants.json` writes a `.txt` containing all
44 messages. No message is dropped.

### AC-2: Every fixture variant parses
Feeding that `.txt` through `PlainTextReader` + `EventParser` yields, for the 44 curated
messages, **zero** entries in `unparsed_counts`, and at least one event of each type the
parser supports: open, close, rug, take-profit, stop-loss, failsafe, already-closed, skip,
add-liquidity-skip, insufficient-balance, swap.

### AC-3: Same event, three dialects, same values
For each of open, close, swap, already-closed, skip: assert on concrete field values extracted
from the gen1, gen2 and gen3 fixture samples. Values are hard-coded in the test from
`VARIANTS.md` — no round-tripping the implementation against itself.

### AC-4: Tx attribution is not crossed
For the gen3 close sample, `bot_tx_signatures` contains exactly the two signatures under
`**Yours**` and `target_tx_signatures` exactly the two under `**Target tx**`, with no overlap.
Same assertion for the gen2 close (legacy `Your Solscan N` / `Target Tx N` labels) and the
gen1 close. `orbmarkets.io` and `metlex.io` links appear in neither list.

### AC-5: gen1 regression
Parsing `archive/20260819T2255-20260823T0611_dce_20260819_213443_discord.txt` before and after
this change produces **identical** event counts and identical values for open and close events.
Capture the baseline on `HEAD~` and assert equality; a bare "it still runs" is not sufficient.

### AC-6: Empty conversion is an error
A DCE JSON whose messages produce no output makes `dce_to_input.py` exit 3, makes `dce_pull.py`
retain the JSON and exit non-zero, and makes `run_pipeline.py` stop before the recalc step.

### AC-7: Unknown variant is loud
A synthetic embed with `author.name = "Teleported · DLMM"` and an unrecognised body is counted
in `unparsed_counts` under a key containing `Teleported`, and the run prints a warning.

### AC-8: Position ID from footer
The gen3 open sample yields `position_id == "CV1CBoBG"` (dots normalised away). A footer of
`"Valhalla"` or `"Valhalla · No action needed"` yields no ID and does not raise.

## Verification Contract

Run from the repo root and paste the output:

```bash
python -m pytest tests/test_dce_to_input_embeds.py tests/test_event_parser_embed_formats.py -v
python -m pytest tests/test_event_parser_dotted_position_ids.py tests/test_event_parser_markdown_close_headers.py -v
python -m pytest tests/test_matcher_contract.py tests/test_merge_contract.py -q
python dce_to_input.py tests/fixtures/discord_embeds/valhalla_format_variants.json --out _temp/ac_variants.txt
python -c "import sys; sys.argv=['x']; from valhalla.readers import PlainTextReader; from valhalla.event_parser import EventParser; m=PlainTextReader('_temp/ac_variants.txt').read(); p=EventParser(); p.parse_messages(m); print('messages',len(m)); print('unparsed',dict(p.unparsed_counts)); print('open',len(p.open_events),'close',len(p.close_events),'swap',len(p.swap_events),'rug',len(p.rug_events),'skip',len(p.skip_events),'already_closed',len(p.already_closed_events),'insuf',len(p.insufficient_balance_events),'failsafe',len(p.failsafe_events))"
```

Expected: `messages 44`, `unparsed {}`, every event bucket non-zero.

Report: changed files, full test output, and the AC-5 baseline comparison (before/after counts).

## Review guidance (for adversarial review — read this)

The dangerous failure mode here is **silent and financial**, not a crash:

1. **Crossed tx attribution** (AC-4). Target transactions recorded as the user's own would
   corrupt matching for every position opened since 2026-08-23 and would not raise anything.
2. **Numbers read from the wrong side of the arrow.** `**Balance** 2.80 SOL → 4.09 SOL` —
   confirm `starting_sol=2.80`, `ending_sol=4.09`, not swapped.
3. **`**Total** 68.33 SOL ($6372.24) across 63 open positions`** — confirm `total_sol` takes the
   SOL figure and not the USD figure, and `active_positions=63`.
4. **Invented text in the archive.** Grep the generated `.txt` for legacy strings that the
   source embed never contained (`Starting SOL balance`, `Ending SOL balance`, `🎯 Target:`
   on a gen3 message). Any hit is a contract violation of this doc's Intent.
5. **gen1 drift** (AC-5). The widened `LABEL_SEP` makes every legacy pattern looser; check it
   cannot now match across a line boundary or grab a neighbouring label's value.

## Alternatives Considered

**Normalise embeds into legacy text inside the converter**, leaving `event_parser.py`
untouched. Smaller diff and zero blast radius downstream. Rejected: the archived `.txt` is the
audit trail for real money, and this design would fill it with strings the bot never sent,
making a converter mapping bug undetectable after the fact. It also puts event semantics in the
transport layer, against the `valhalla/` package split.

**Support gen3 only**, treating the 9-hour gen2 window as acceptable loss. Rejected by the
operator: that window contains 78 opens and 65 closes with real PnL, and the opens would sit in
the CSV forever without a matching close.

---

## Addendum A — stable-pair opens (SOL-USDC), added 2026-08-26

Found during full-scale validation, not by the curated fixture: 4 open events on 2026-08-25
from target `20260618_gh7gW8Ft` for the pair **SOL-USDC**. The bot omits `**MC**`, `**Age**`
and `**Jup**` for a stable/blue-chip pair and appends a spoiler block:

```
Opened · DLMM · Spot 1-Sided
SOL-USDC
### 1.00 SOL
2% of target's 60.00
**Target** 20260618_gh7gW8Ft  · Orb  · Solscan
**Token** DexScreener  · Orb  · Solscan
**Yours** 3 transactions · …
**Target tx** 3 transactions · …
||**Calculation Details**
Target: SOL 60.000000 tokens ($5910.00) | USDC 0.000000 tokens ($0.00) | Total: $5910.00
Prices: SOL $98.50 | USDC $0.00
Total Deposit: Target 60.0000 SOL | User 1.0000 SOL
Split: SOL 100.0% | USDC 0.0%||
Valhalla · GcWN...hNc8
```

`_parse_open_event` requires market cap, token age and Jup score to all be present, so it
returns `None` and the event is dropped. `USDC` and `Calculation Details` appear nowhere in the
gen1 archives — this is a **new position type, not a regression**.

**Operator decision (2026-08-26): parse them with empty metrics.**

- `market_cap → 0.0`, `token_age → ""`, `jup_score → 0` when the lines are absent.
  This is already the codebase's "no data" convention: `valhalla/loss_analyzer.py:165-172`
  maps `jup_score == 0` and `mc_at_open == 0.0` to `None` and excludes them from threshold
  analysis, so MC/Jup/age statistics are not distorted.
- Everything else is extracted normally: `position_type` from the author line, `token_pair`
  from the title, `your_sol` from `Total Deposit: … User 1.0000 SOL` (falling back to the
  `### 1.00 SOL` line), `target_sol` from `Total Deposit: Target 60.0000 SOL` (falling back to
  `2% of target's 60.00`), `position_id` from the footer.
- Rationale for not dropping them: close events carry no metrics and parse fine, so dropping
  only the opens would produce orphaned closes (`already_closed_unknown_open` noise) for real
  1 SOL positions.

Relaxing the required-field set applies to gen3 opens only. A gen1/gen2 open that is missing
MC/Age/Jup is still a parse failure and must keep incrementing `unparsed_counts` — those
dialects always carried the metrics, so their absence signals a real problem.

### AC-13: stable-pair open parses
The SOL-USDC fixture sample yields an `OpenEvent` with `token_pair == "SOL-USDC"`,
`position_type == "Spot"`, `your_sol == 1.0`, `target_sol == 60.0`,
`position_id == "GcWNhNc8"`, `target == "20260618_gh7gW8Ft"`, and
`market_cap == 0.0`, `token_age == ""`, `jup_score == 0`.

### AC-14: metrics still required for gen1/gen2
A gen1 open sample with its `MC:` line removed must still fail to parse and increment
`unparsed_counts`.
