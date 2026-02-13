# Valhalla Fjorge

Analyze your [Valhalla Bot](https://valhalla.bot) DLMM position performance with accurate Meteora PnL data.

Parses Discord DM logs from Valhalla Bot, resolves on-chain position addresses via Solana RPC, and fetches precise PnL from the Meteora DLMM API.

## Features

- **Accurate Meteora PnL** - per-position profit/loss from on-chain data (deposits, withdrawals, fees)
- **Multi-wallet support** - tracks multiple target wallets from a single log file
- **Rug detection** - identifies rug pulls and price-drop stop losses
- **Token metrics** - captures MC, Jup Score, token age at entry
- **Time-windowed stats** - 24h, 72h, 7d performance breakdowns
- **Charts** - cumulative PnL, win rate trend, strategy comparison (matplotlib)
- **HTML input** - paste Discord DMs directly from browser (auto-detects format)
- **Incremental merging** - build history over time without re-processing old logs
- **JSON export/import** - portable `.valhalla.json` for cross-session persistence

## Requirements

- Python 3.10+
- No external dependencies (stdlib only for core functionality)
- `matplotlib` (optional, for chart generation)

## Quick Start

### 1. Copy your Discord DMs

Open your DM conversation with Valhalla Bot in Discord. Select messages, Ctrl+C. Paste into a text file.

Add the date at the top of the file in `YYYYMMDD` format:

```
20260212
[03:23] APL. Valhalla Bot:
Opened New DLMM Position! (6w6YidRv)
...
```

### 2. Run the parser

```bash
# Basic run (resolves addresses + fetches Meteora PnL)
python valhalla_parser_v2.py logs.txt --output-dir results/

# With Helius RPC (faster, avoids public RPC rate limits)
python valhalla_parser_v2.py logs.txt --output-dir results/ \
  --rpc-url "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"

# Quick run (cached addresses only, no API calls)
python valhalla_parser_v2.py logs.txt --output-dir results/ --skip-rpc --skip-meteora
```

### 3. View results

Output files in your `--output-dir`:
- `positions.csv` - per-position details with PnL
- `summary.csv` - per-wallet aggregate statistics
- `pnl_cumulative.png` - cumulative PnL chart
- `win_rate_trend.png` - rolling win rate
- `pnl_by_strategy.png` - Spot vs BidAsk performance

## CLI Reference

```
python valhalla_parser_v2.py [input_files...] [options]

Positional:
  input_files              Log file(s) to parse (.txt or .html)

Options:
  --output-dir DIR         Output directory (default: current)
  --rpc-url URL            Solana RPC endpoint (default: public mainnet)
  --skip-rpc               Skip RPC resolution, use address cache only
  --skip-meteora           Skip Meteora API, use Discord PnL as fallback
  --skip-charts            Skip chart generation
  --cache-file FILE        Address cache JSON path
  --date YYYY-MM-DD        Override date for all input files
  --input-format FMT       Force input format: auto, text, html (default: auto)
  --export-json FILE       Export results as .valhalla.json
  --import-json FILE       Import previous .valhalla.json and merge
  --merge CSV [CSV ...]    Merge multiple positions.csv files
```

## Date Handling

Discord logs don't contain dates. Three ways to provide them (in priority order):

1. **In-file header** (recommended): Type `YYYYMMDD` on the first line of your log file
2. **CLI flag**: `--date 2026-02-12`
3. **Filename**: Include date in filename, e.g. `logs_20260212.txt`

The parser detects midnight rollover automatically when the date is set.

## Incremental Workflow

Build up history day by day without re-processing old data:

```bash
# Day 1
python valhalla_parser_v2.py day1.txt --output-dir results/ \
  --export-json history.valhalla.json

# Day 2 (merge with previous)
python valhalla_parser_v2.py day2.txt --output-dir results/ \
  --import-json history.valhalla.json \
  --export-json history.valhalla.json

# Or merge existing CSV outputs
python valhalla_parser_v2.py --merge results/day1/positions.csv results/day2/positions.csv \
  --output-dir results/merged/
```

Deduplication by `position_id` ensures no double-counting. Previously open positions are automatically updated when closed.

## HTML Input

You can paste Discord DMs as HTML directly. The parser auto-detects the format:

```bash
# Auto-detect (works for both .txt and .html)
python valhalla_parser_v2.py discord_feed.html --output-dir results/

# Force HTML mode
python valhalla_parser_v2.py clipboard.html --input-format html --output-dir results/
```

HTML processing extracts links, strips formatting, and decodes entities before parsing.

## Output Format

### positions.csv

| Column | Description |
|--------|-------------|
| timestamp_open/close | `[HH:MM]` from Discord |
| datetime_open/close | ISO 8601 (when date available) |
| target_wallet | Valhalla target wallet ID |
| token | Token name |
| position_type | Spot or BidAsk |
| sol_deployed | SOL deposited into position |
| pnl_sol | Profit/loss in SOL |
| pnl_pct | Profit/loss percentage |
| close_reason | normal, failsafe, rug, still_open |
| mc_at_open | Market cap at position open |
| jup_score | Jupiter score (0-100) |
| token_age | Token age string (e.g., "4h ago") |
| token_age_days/hours | Normalized age values |
| pnl_source | "meteora" or "discord" |
| meteora_* | Detailed Meteora breakdown |

### summary.csv

Per-wallet statistics: positions, wins, losses, rugs, PnL, win rate, plus time-windowed breakdowns (24h, 72h, 7d).

## How PnL is Calculated

1. **Address resolution**: Extract Solscan TX signatures from logs, query Solana RPC to find the full DLMM position address
2. **Meteora API**: Fetch deposits, withdrawals, and claimed fees for each position
3. **Per-transaction SOL equivalent**: For each operation, derive SOL price from the SOL portion, convert token value to SOL at that price
4. **PnL = withdrawals + fees - deposits** (all in SOL equivalent)

Fallback to Discord balance-diff PnL when Meteora data is unavailable.

## Troubleshooting

**RPC rate limits**: Public Solana RPC limits to ~10 req/s. Use `--rpc-url` with a Helius or other RPC provider for faster resolution. The parser uses exponential backoff automatically.

**Meteora API timeouts**: Some positions may fail to fetch. Re-run the parser - resolved addresses are cached, so only failed Meteora calls are retried.

**Missing dates**: Charts and time-windowed stats require dates. Add `YYYYMMDD` at the top of your log files.

**No matplotlib**: Charts are skipped gracefully. Install with `pip install matplotlib` if you want them.

## Project Structure

```
valhalla_parser_v2.py    # Main parser (single-file, ~2000 lines)
schowek_html.ps1         # PowerShell clipboard helper (optional)
sample_logs/             # Example log files
VERCEL_APP_SPEC.md       # Web app specification (future)
```

## License

MIT - see [LICENSE](LICENSE)

## Credits

- [Valhalla Bot](https://valhalla.bot) - the DLMM copy-trading bot
- [Meteora DLMM](https://dlmm.meteora.ag) - the DEX protocol
- [lpagent.io](https://lpagent.io) - used for PnL cross-reference validation
