# [001] Phase A — Extend Data Parsing (target wallet + tx signatures)

## Overview

Extend the message-parsing pipeline to extract two new data points from Discord messages: the target wallet's full Solana address and the target wallet's transaction signature. These values are embedded in URLs inside the messages and must be captured before the URL-stripping step. They flow from readers.py through models.py and matcher.py into two new columns at the end of positions.csv.

This phase is a prerequisite for Phase C (Source Wallet Analyzer). Phase B (Loss Analyzer) and Phase D (CLI Integration) can start without it, but Phase D will include these columns in the report if available.

## Context

Today, `PlainTextReader.read()` returns `List[Tuple[str, str, List[str]]]` — timestamp, clean text, and a list of bot tx signatures. The bot tx signatures are extracted from all `[https://solscan.io/tx/SIG]` URLs before the text is cleaned.

The messages also contain target wallet URLs (lpagent.io) and labeled target tx URLs ("Target Tx 1", "Target Tx 2"). Neither is currently captured.

Example message fragment (raw, before URL stripping):
```
Target: 20260125_2eWqo [https://app.lpagent.io/portfolio?address=2eWqouq9...]
View Tx - Your Solscan 1 [https://solscan.io/tx/BOT_SIG1] | Your Solscan 2 [...]
         | Target Tx 1 [https://solscan.io/tx/TARGET_SIG1] | Target Tx 2 [...]
```

The existing `SOLSCAN_TX_PATTERN` extracts all Solscan signatures indiscriminately — it cannot distinguish "Your" from "Target" tx. Both the new patterns must run on the raw message text (before URL stripping).

## Goals

- Add `ParsedMessage` dataclass to `readers.py` replacing the bare tuple return type
- Extract `target_wallet_address` from lpagent.io URLs using a new regex
- Extract `target_tx_signatures` (labeled "Target Tx N") from Solscan URLs using a labeled regex
- Preserve backward compatibility: existing `bot_tx_signatures` stays the same
- Add `target_wallet_address` and `target_tx_signatures` as `Optional` fields on `OpenEvent` and `MatchedPosition`
- Write two new columns to the end of positions.csv: `target_wallet_address`, `target_tx_signature` (only first target tx)
- Update `json_io.py` to persist and restore the two new fields
- All existing positions without these values remain valid (columns are empty strings)

## Non-Goals

- No changes to close/rug/failsafe event parsing (target wallet info only appears in open messages)
- No Solana RPC calls using the target tx signatures (that is Phase C)
- No analysis logic (that is Phases B and C)

## Design

### ParsedMessage Dataclass

Replace the bare 3-tuple returned by readers with a named dataclass. This makes the interface explicit and easier to extend in the future.

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/readers.py`

```python
from dataclasses import dataclass, field

@dataclass
class ParsedMessage:
    timestamp: str
    clean_text: str
    bot_tx_signatures: List[str]
    target_tx_signatures: List[str] = field(default_factory=list)   # NEW
    target_wallet_address: Optional[str] = None                      # NEW
```

The `read()` method on both `PlainTextReader` and `HtmlReader` changes its return type from `List[Tuple[str, str, List[str]]]` to `List[ParsedMessage]`.

### New Regex Patterns in readers.py

```python
# Labeled Solscan URLs: captures label and signature
# Matches: "Target Tx 1 [https://solscan.io/tx/SIG]"
# Also matches: "Your Solscan 1 [https://solscan.io/tx/SIG]"
LABELED_SOLSCAN_PATTERN = re.compile(
    r'([\w\s]+?)\s*\[https://solscan\.io/tx/([A-Za-z0-9]+)\]'
)

# lpagent.io portfolio URL: captures wallet address
LPAGENT_PATTERN = re.compile(
    r'\[https://app\.lpagent\.io/portfolio\?address=([A-Za-z0-9]+)\]'
)
```

Extraction logic runs on raw message text BEFORE URL stripping:

```python
# Extract target wallet address
lpagent_match = self.LPAGENT_PATTERN.search(raw_msg)
target_wallet_address = lpagent_match.group(1) if lpagent_match else None

# Extract labeled tx signatures
target_tx_signatures = []
bot_tx_signatures = []
for label, sig in self.LABELED_SOLSCAN_PATTERN.findall(raw_msg):
    label_lower = label.lower().strip()
    if 'target' in label_lower:
        target_tx_signatures.append(sig)
    else:
        bot_tx_signatures.append(sig)
```

Note: `bot_tx_signatures` must be re-derived using label filtering — the old `SOLSCAN_TX_PATTERN` (which captured ALL signatures) is no longer the primary extraction. Keep the old pattern in place as a fallback for messages where labeled extraction finds nothing in the bot category (edge case: very old message formats that lack labels).

### Fallback for unlabeled messages

If `LABELED_SOLSCAN_PATTERN` finds no bot signatures but the old `SOLSCAN_TX_PATTERN` does, use the old pattern's results as `bot_tx_signatures` and leave `target_tx_signatures` empty. This preserves backward compatibility with older message formats.

```python
if not bot_tx_signatures:
    # Fallback: collect all signatures (old behavior)
    bot_tx_signatures = self.SOLSCAN_TX_PATTERN.findall(raw_msg)
```

### HtmlReader

`HtmlReader.read()` duplicates the parsing logic from `PlainTextReader`. After Phase A it must also run the two new patterns on the raw text (pre-stripped), in the same way.

The `html_to_text()` method already preserves `TEXT [URL]` format in its output, so the new patterns will work the same way on the converted plain text. The extraction logic runs after `html_to_text()` but before `URL_BRACKET_PATTERN.sub()`.

### OpenEvent — new Optional fields

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/models.py`

Add to the `OpenEvent` dataclass (at the end, with defaults):

```python
@dataclass
class OpenEvent:
    # ... existing fields unchanged ...
    target_wallet_address: Optional[str] = field(default=None)   # NEW
    target_tx_signatures: List[str] = field(default_factory=list) # NEW
```

### MatchedPosition — new Optional fields

Add to the `MatchedPosition` dataclass (at the end, with defaults):

```python
@dataclass
class MatchedPosition:
    # ... existing fields unchanged ...
    target_wallet_address: Optional[str] = field(default=None)   # NEW
    target_tx_signature: Optional[str] = field(default=None)     # NEW — only first
```

### event_parser.py — pass new fields through

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/event_parser.py`

`parse_messages()` currently accepts `List[Tuple[str, str, List[str]]]`. Change signature to accept `List[ParsedMessage]` and unpack accordingly.

In `_parse_open_event()`, accept and pass through `target_wallet_address` and `target_tx_signatures`:

```python
def _parse_open_event(self, timestamp: str, message: str, tx_signatures: List[str],
                      target_wallet_address: Optional[str] = None,
                      target_tx_signatures: Optional[List[str]] = None) -> Optional[OpenEvent]:
```

Populate the new fields on the returned `OpenEvent`.

### matcher.py — propagate to MatchedPosition

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/matcher.py`

When constructing a `MatchedPosition` from an `OpenEvent`, propagate:

```python
target_wallet_address=open_event.target_wallet_address,
target_tx_signature=open_event.target_tx_signatures[0] if open_event.target_tx_signatures else None,
```

This applies only to the "matched with open" branch. For unknown_open / rug_unknown_open / failsafe_unknown_open, leave both fields as `None`.

### csv_writer.py — two new columns at the end

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/csv_writer.py`

In `generate_positions_csv()`, add to the header row (after `meteora_pnl`):

```python
'target_wallet_address', 'target_tx_signature'
```

Add to each data row:

```python
pos.target_wallet_address if pos.target_wallet_address else "",
pos.target_tx_signature if pos.target_tx_signature else "",
```

Do the same for the still-open rows (both fields will be empty strings for unmatched opens, since they come from `OpenEvent` not `MatchedPosition`; OpenEvent has the raw data but CsvWriter currently converts it to a synthetic row without a full MatchedPosition — access `open_event.target_wallet_address` directly).

### json_io.py — persist and restore new fields

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/json_io.py`

In `export_to_json()`, add to `pos_dict`:

```python
"target_wallet_address": pos.target_wallet_address,
"target_tx_signature": pos.target_tx_signature,
```

In `import_from_json()`, restore:

```python
target_wallet_address=pos_dict.get('target_wallet_address'),
target_tx_signature=pos_dict.get('target_tx_signature'),
```

### valhalla_parser_v2.py — update parse_messages call

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla_parser_v2.py`

`file_parser.parse_messages(messages)` — the call site passes `messages` directly. After Phase A, `messages` is a `List[ParsedMessage]` instead of `List[Tuple]`. The call site itself does not change; `parse_messages()` signature adapts to accept `List[ParsedMessage]`.

The dedup loop in `valhalla_parser_v2.py` that iterates over `messages` to extract timestamps for archive naming works on `(ts, _, _)` unpacking — update to use `msg.timestamp` attribute syntax instead:

```python
# Before
for ts, _, _ in messages:
    ...

# After
for msg in messages:
    ts = msg.timestamp
```

## Implementation Plan

1. **`valhalla/readers.py`** — add `ParsedMessage` dataclass; add `LABELED_SOLSCAN_PATTERN` and `LPAGENT_PATTERN`; rewrite extraction block in `PlainTextReader.read()` to use labeled extraction with fallback; update `HtmlReader.read()` identically; change return type to `List[ParsedMessage]`

2. **`valhalla/models.py`** — add `target_wallet_address: Optional[str]` and `target_tx_signatures: List[str]` to `OpenEvent`; add `target_wallet_address: Optional[str]` and `target_tx_signature: Optional[str]` to `MatchedPosition`

3. **`valhalla/event_parser.py`** — update `parse_messages()` to accept `List[ParsedMessage]`; update `_parse_open_event()` signature and OpenEvent construction

4. **`valhalla/matcher.py`** — propagate new fields from `open_event` into `MatchedPosition` constructor calls (only matched-with-open branches)

5. **`valhalla/csv_writer.py`** — add two new columns to header and rows in `generate_positions_csv()`; handle both matched-position rows and still-open rows

6. **`valhalla/json_io.py`** — add new fields to `export_to_json()` serialization and `import_from_json()` deserialization

7. **`valhalla_parser_v2.py`** — update archive-naming loop and any other tuple-unpacking of `messages` to use `ParsedMessage` attribute access

## Dependencies

- Independent. No other phase is required before this one.
- External: no new libraries — all regex is stdlib.

## Testing

1. Run `python valhalla_parser_v2.py` with a log file that contains lpagent.io URLs
2. Open `output/positions.csv` — columns `target_wallet_address` and `target_tx_signature` should appear at the end
3. For positions parsed from messages with the Target URLs, values should be non-empty Solana addresses/signatures
4. For positions from old log files without these URLs, both columns should be empty (no crash)
5. Regression: all other columns should be identical to before the change
6. Run with `--export-json` and then `--import-json` — new fields should survive the round-trip
7. Verify `bot_tx_signatures` are not contaminated with target signatures (spot-check against a known message)

## Alternatives Considered

- **Keep tuple return type, add index 3 and 4**: Rejected — named fields are safer and self-documenting. The tuple approach would require updating every caller by positional index.
- **Extract target_wallet from the `Target:` label line instead of lpagent URL**: The `Target:` field contains a short alias (e.g., `20260125_2eWqo`), not the full address. The full address is only in the lpagent URL.

## Open Questions

- Are there any message formats where `Target Tx 1` label has different capitalization or spacing (e.g., `Target TX 1`)? The regex `r'([\w\s]+?)\s*\['` is flexible on whitespace. If capitalization varies, the `'target' in label_lower` check handles it.
- Does the lpagent URL appear in messages other than open events? If yes, the extraction still works correctly — it will just return a value that gets discarded at the parser level for non-open events.
