# [011] LpAgent API Client

## Overview
This doc covers building `valhalla/lpagent_client.py` — a self-contained HTTP client that fetches closed Meteora positions for a single wallet from the lpagent API, handles pagination, rate limiting, and daily file-based caching. It is the data-access layer for the entire cross-check system.

## Context
The project already has a Meteora API client (`valhalla/meteora.py`) that uses `urllib.request` directly without external HTTP libraries. The same pattern should be followed here. The lpagent API is a third-party service with a Free tier limit of 5 requests per minute, requiring 12 seconds between calls. The wallet address is fixed: `J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF`.

Cache files land in `output/lpagent_cache/YYYY-MM-DD.json`. The format stores the raw list of lpagent position objects exactly as returned by the API (all pages merged).

## Goals
- Fetch all closed positions for a wallet within a `from_date`/`to_date` range (dates as `YYYY-MM-DD` strings)
- Paginate transparently (pageSize=10, fixed)
- Respect 5 RPM rate limit (12s sleep between requests)
- Return cached results when `output/lpagent_cache/YYYY-MM-DD.json` exists (one file per day)
- Never cache today's date (data may still be incomplete)
- Accept API key from environment variable `LPAGENT_API_KEY`
- Accept wallet address from environment variable `LPAGENT_WALLET` with fallback to the hardcoded default

## Non-Goals
- Fetching open positions
- Supporting multiple wallets dynamically
- Premium tier / dynamic rate limiting
- Retry logic on HTTP errors (fail fast, log the error)

## Design

### LpAgentClient Class

File: `valhalla/lpagent_client.py`

```python
class LpAgentClient:
    def __init__(self, api_key: str, wallet: str, cache_dir: str = "output/lpagent_cache"):
        ...

    def fetch_day(self, date: str) -> List[dict]:
        """Fetch all closed positions opened on `date` (YYYY-MM-DD).
        Uses cache if available. Returns list of raw lpagent position dicts."""
        ...

    def fetch_range(self, from_date: str, to_date: str) -> List[dict]:
        """Fetch positions for a date range (inclusive). Iterates day by day,
        calling fetch_day() for each. Returns combined list."""
        ...

    def _fetch_from_api(self, date: str) -> List[dict]:
        """Internal: fetch one day from API with pagination and rate limiting."""
        ...

    def _load_cache(self, date: str) -> Optional[List[dict]]:
        ...

    def _save_cache(self, date: str, positions: List[dict]) -> None:
        ...
```

### API Endpoint

The lpagent API endpoint to use:

```
GET https://app.lpagent.io/api/positions
```

Query parameters:
- `wallet` — wallet address
- `status` — always `"Close"`
- `from_date` — `YYYY-MM-DD`
- `to_date` — `YYYY-MM-DD` (same as from_date for day-by-day fetching)
- `page` — 1-indexed page number
- `pageSize` — always `10`

Request headers:
- `Authorization: Bearer {api_key}`
- `Content-Type: application/json`

The API filters by `createdAt` (position open date), not close date. This is intentional — we query by opening date per day.

Response JSON structure (relevant fields only):
```json
{
  "data": [...],
  "total": 47,
  "page": 1,
  "pageSize": 10
}
```

Pagination: keep fetching pages while `(page - 1) * pageSize < total`. The total comes from the first response.

### Rate Limiting

Sleep 12 seconds before each API request (not before cache reads). This ensures we stay within 5 RPM. Apply the sleep unconditionally inside `_fetch_from_api`, even for the first page of the first day — the caller should not need to manage timing.

Exception: no sleep before the very first request of a session (optional optimization — acceptable to always sleep for simplicity).

### Cache Strategy

Cache path: `{cache_dir}/{date}.json`

- Cache stores the full merged list (all pages) as a JSON array
- On read: if file exists and date is not today → return cached data, no API call
- On write: only after all pages for a day are fetched successfully
- Today's date: never cached (data may still be incomplete as positions close throughout the day)
- Cache directory is created automatically if missing

### Error Handling

- HTTP non-200 response: raise `RuntimeError` with status code and body snippet
- JSON decode error: raise `RuntimeError` with raw response preview
- Missing `LPAGENT_API_KEY` env var: raise `ValueError` at construction time
- Empty result from API (total=0): return `[]` and do NOT write cache (avoid caching "no data" for days that might have future data — wait until the date is in the past)

Actually: empty result for a past date IS valid (no positions that day). Cache it to avoid re-querying. Only skip caching for today.

### Field Reference (from lpagent API)

Key fields in each position object returned by the API:
- `tokenId` — full Solana position address (maps to `full_address` in CSV)
- `createdAt` — ISO timestamp of position open
- `updatedAt` — ISO timestamp of last update (used as close time)
- `status` — `"Close"` (always, since we filter)
- `token0Info.token_symbol` — token symbol
- `inputNative` — SOL deposited
- `outputNative` — SOL received
- `pnlNative` — PnL in SOL
- `pnl.percentNative` — PnL percentage
- `collectedFeeNative` — fees collected in SOL

## Implementation Plan

1. Create `output/lpagent_cache/` directory structure (exists check, `mkdir -p`)
2. Create `valhalla/lpagent_client.py` with the `LpAgentClient` class
3. Implement `_load_cache` and `_save_cache` using `json` stdlib
4. Implement `_fetch_from_api` with `urllib.request`, pagination loop, 12s sleep between pages
5. Implement `fetch_day` — check cache first, fall back to `_fetch_from_api`, save cache
6. Implement `fetch_range` — iterate dates with `timedelta(days=1)`, collect results
7. Add `__init__.py` export if needed (check `valhalla/__init__.py` — likely not required since direct imports are used)
8. Update `.env.example` with `LPAGENT_API_KEY` and `LPAGENT_WALLET` entries

## Dependencies
- Independent: no other docs required
- External: Python stdlib only (`urllib.request`, `json`, `os`, `time`, `datetime`, `pathlib`)
- `.env` must have `LPAGENT_API_KEY` set before running

## Testing
- Manual test: run `python -c "from valhalla.lpagent_client import LpAgentClient; ..."` with a real API key for one specific date
- Verify cache file is created after first run: `output/lpagent_cache/2026-03-31.json`
- Verify second run for same date returns instantly (no sleep, no API call)
- Verify today's date is never cached
- Test with an invalid API key: should raise `RuntimeError` with HTTP 401 info

## Alternatives Considered
- **requests library**: Rejected — project uses only stdlib HTTP, no external dependencies
- **Cache by date range instead of per-day**: Rejected — per-day files allow partial cache hits and are simpler to invalidate

## Open Questions
- None — API behavior confirmed empirically per PLAN.md risk section
