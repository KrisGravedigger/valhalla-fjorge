"""
LpAgent API client for fetching closed Meteora positions.

Replaces the per-day JSON cache with a single flat JSONL file keyed on
tokenId. Uses a sliding-window refresh to catch positions opened before
the watermark and closed after it. Asserts totalCount on every fetch to
detect silent truncation. Writes are atomic via os.replace.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

DEFAULT_WALLET = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"
API_BASE = "https://api.lpagent.io/open-api/v1"
PAGE_SIZE = 10
RATE_LIMIT_SLEEP = 12           # seconds between API requests (5 RPM free tier)
REFRESH_WINDOW_HOURS = 120      # 5 days; max observed hold time ~87h + 33h buffer
REFRESH_THRESHOLD_HOURS = 24    # skip network fetch if last refresh was this recent
WATERMARK_DEFAULT_DATE = "2026-02-11"


class LpAgentClient:
    """
    Client for the LpAgent API.

    Fetches closed Meteora positions for a single wallet and stores them
    in a single JSONL file: output/lpagent_cache/positions_{wallet_prefix}.jsonl.
    Deduplicates by tokenId (newer updatedAt wins). Every paginated fetch
    asserts len(retrieved) == totalCount to catch silent truncation.
    """

    def __init__(
        self,
        api_key: str,
        wallet: str = DEFAULT_WALLET,
        cache_dir: str = "output/lpagent_cache",
    ) -> None:
        if not api_key:
            raise ValueError(
                "LPAGENT_API_KEY is required but was not provided. "
                "Set the LPAGENT_API_KEY environment variable."
            )
        self._api_key = api_key
        self._wallet = wallet
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._needs_refresh: bool = False

        # Clean up stale .tmp file from a previous crashed write
        tmp = self._jsonl_path().with_suffix(".tmp")
        if tmp.exists():
            try:
                tmp.unlink()
                logger.info("Removed stale .tmp file: %s", tmp)
            except OSError as exc:
                logger.warning("Could not remove stale .tmp file %s: %s", tmp, exc)

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_since(self, from_date_utc: str) -> List[dict]:
        """Fetch all positions with createdAt >= from_date_utc, paginating fully.

        Asserts len(retrieved) == totalCount. Merges results into the JSONL
        cache (newer updatedAt wins). Returns the merged in-memory list.
        """
        now_utc = datetime.now(timezone.utc)
        now_utc_iso = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Check watermark for skip threshold
        output_dir = str(self._cache_dir.parent)
        from valhalla.lpagent_pipeline import read_watermark, write_watermark  # noqa: PLC0415
        watermark = read_watermark(output_dir)
        last_refresh_str: Optional[str] = watermark.get("last_full_refresh_at")
        if last_refresh_str:
            try:
                last_refresh = datetime.fromisoformat(
                    last_refresh_str.replace("Z", "+00:00")
                )
                hours_ago = (now_utc - last_refresh).total_seconds() / 3600
                if hours_ago < REFRESH_THRESHOLD_HOURS:
                    logger.info(
                        "Refresh skipped: last full refresh was %.1f hours ago (threshold: %dh)",
                        hours_ago,
                        REFRESH_THRESHOLD_HOURS,
                    )
                    return list(self.load_cache().values())
            except ValueError:
                pass  # malformed timestamp — proceed with refresh

        # Fetch from API
        new_records = self._fetch_all_pages(from_date_utc)

        # Merge with existing cache
        existing = self.load_cache()
        merged: Dict[str, dict] = dict(existing)
        for rec in new_records:
            tid = rec["tokenId"]
            if tid not in merged or rec["updatedAt"] > merged[tid]["updatedAt"]:
                merged[tid] = {**rec, "fetched_at_utc": now_utc_iso}

        self._write_jsonl_atomic(merged)

        # Update watermark
        min_safe_open_date = watermark.get("min_safe_open_date", WATERMARK_DEFAULT_DATE)
        new_watermark = {
            "wallet": self._wallet,
            "min_safe_open_date": min(min_safe_open_date, from_date_utc),
            "last_full_refresh_at": now_utc_iso,
            "refresh_window_hours": REFRESH_WINDOW_HOURS,
        }
        write_watermark(output_dir, new_watermark)

        self._needs_refresh = False
        return list(merged.values())

    def fetch_range(self, from_date: str, to_date: str) -> List[dict]:
        """Back-compat shim.

        Triggers fetch_since(from_date) if cache is empty or stale, then
        returns records with createdAt in [from_date, to_date]. Existing
        callers (run_cross_check, CrossChecker) need no modification.
        """
        jsonl = self._jsonl_path()
        cache_empty = not jsonl.exists() or jsonl.stat().st_size == 0

        # Prime _needs_refresh: load_cache detects truncation and sets the flag.
        # Must happen before the refresh check so a truncated cache triggers re-fetch.
        if not cache_empty:
            self.load_cache()

        if cache_empty or self._needs_refresh:
            self.fetch_since(from_date)

        records = self.load_cache()
        result = [
            rec for rec in records.values()
            if _date_in_range(rec.get("createdAt", ""), from_date, to_date)
        ]
        logger.info(
            "fetch_range %s..%s → %d positions (from %d cached)",
            from_date, to_date, len(result), len(records),
        )
        return result

    def load_cache(self) -> Dict[str, dict]:
        """Load all records from the JSONL into a dict keyed by tokenId.

        Detects truncated last line and recovers: truncates the file to the
        last valid line, sets self._needs_refresh = True.
        Always checks wallet match against the watermark (even if JSONL is absent).
        """
        # Wallet check happens regardless of JSONL existence so a changed wallet
        # is caught as soon as the client is used.
        self._check_wallet_match({})

        path = self._jsonl_path()
        if not path.exists():
            return {}

        raw = path.read_bytes()
        if not raw.strip():
            return {}

        lines = raw.splitlines()
        valid_lines: List[bytes] = []
        records: Dict[str, dict] = {}

        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            try:
                obj = json.loads(line_stripped)
                valid_lines.append(line_stripped)
                tid = obj.get("tokenId")
                if tid:
                    records[tid] = obj
            except json.JSONDecodeError:
                logger.warning(
                    "Truncated line detected at record %d — truncating file and scheduling re-fetch",
                    len(valid_lines),
                )
                self._needs_refresh = True
                break  # stop at first bad line

        if self._needs_refresh:
            # Rewrite the file with only valid lines
            path.write_bytes(b"\n".join(valid_lines) + b"\n")
            logger.info("File repaired: %d valid records retained", len(valid_lines))

        return records

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _jsonl_path(self) -> Path:
        return self._cache_dir / f"positions_{self._wallet_prefix()}.jsonl"

    def _wallet_prefix(self) -> str:
        return self._wallet[:5]

    def _check_wallet_match(self, loaded_records: Dict[str, dict]) -> None:
        """Raise ValueError if the watermark wallet does not match self._wallet.

        Called even when loaded_records is empty, so a wallet change is detected
        as soon as the client is used — regardless of whether the JSONL exists.
        """
        output_dir = str(self._cache_dir.parent)
        try:
            from valhalla.lpagent_pipeline import read_watermark  # noqa: PLC0415
            watermark = read_watermark(output_dir)
            cached_wallet: Optional[str] = watermark.get("wallet")
        except Exception:
            return  # can't read watermark — skip check

        if cached_wallet and cached_wallet != self._wallet:
            prefix_cached = cached_wallet[:5]
            prefix_current = self._wallet[:5]
            raise ValueError(
                f"Wallet mismatch: cache was built for {prefix_cached}, "
                f"current wallet is {prefix_current}. "
                "Clear the cache or use a separate cache_dir."
            )

    def _write_jsonl_atomic(self, records: Dict[str, dict]) -> None:
        """Write records dict to JSONL via atomic tmp → os.replace."""
        path = self._jsonl_path()
        tmp = path.with_suffix(".tmp")

        lines = [json.dumps(rec, ensure_ascii=False) for rec in records.values()]
        content = "\n".join(lines)
        if content:
            content += "\n"

        tmp.write_text(content, encoding="utf-8")
        os.replace(str(tmp), str(path))
        logger.info("JSONL written: %d records → %s", len(records), path)

    def _fetch_all_pages(self, from_date_utc: str) -> List[dict]:
        """Paginate from page 1 until totalPages, assert totalCount.

        Raises AssertionError with diagnostic context if the total count
        does not match the number of retrieved records. Does NOT write to
        the JSONL file — caller is responsible for persistence.
        """
        all_positions: List[dict] = []
        page = 1
        total_count: Optional[int] = None
        total_pages = 1
        first_request = True

        while page <= total_pages:
            if first_request:
                first_request = False
            else:
                logger.debug("Rate limit sleep: %ds before page %d", RATE_LIMIT_SLEEP, page)
                time.sleep(RATE_LIMIT_SLEEP)

            params = {
                "owner": self._wallet,
                "from_date": from_date_utc,
                "page": str(page),
            }
            query_string = "&".join(
                f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items()
            )
            url = f"{API_BASE}/lp-positions/historical?{query_string}"

            logger.debug("GET %s (page %d)", url, page)
            raw = self._lpagent_get(url)

            inner = raw.get("data", {})
            page_data = inner.get("data", [])
            pagination = inner.get("pagination") or {}

            if total_count is None:
                total_count = pagination.get("totalCount") or 0
                total_pages = pagination.get("totalPages") or 1
                logger.info(
                    "API: from_date=%s total=%d pages=%d",
                    from_date_utc,
                    total_count,
                    total_pages,
                )

            all_positions.extend(page_data)

            if not page_data:
                break  # no more data

            page += 1

        if total_count is not None and len(all_positions) != total_count:
            raise AssertionError(
                f"lpagent API totalCount mismatch: "
                f"totalCount={total_count}, retrieved={len(all_positions)} "
                f"(from_date={from_date_utc}, pages={total_pages})"
            )

        logger.info(
            "Fetched %d positions from API (from_date=%s)",
            len(all_positions),
            from_date_utc,
        )
        return all_positions

    def _lpagent_get(self, url: str) -> dict:
        """Make a single GET request to the LpAgent API.

        Raises RuntimeError on non-200 responses or JSON decode failures.
        """
        req = urllib.request.Request(
            url,
            headers={
                "x-api-key": self._api_key,
                "Content-Type": "application/json",
                "Accept": "application/json",
                # Mimic a browser to avoid Cloudflare bot detection on the API domain
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                raw = resp.read()
        except urllib.error.HTTPError as e:
            body_snippet = e.read(200).decode("utf-8", errors="replace")
            raise RuntimeError(
                f"LpAgent API returned HTTP {e.code}: {body_snippet}"
            ) from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"LpAgent API request failed: {e.reason}") from e

        try:
            return json.loads(raw)
        except json.JSONDecodeError as e:
            preview = raw[:200].decode("utf-8", errors="replace")
            raise RuntimeError(
                f"LpAgent API returned invalid JSON: {preview}"
            ) from e


# ------------------------------------------------------------------
# Private date-range helper
# ------------------------------------------------------------------

def _date_in_range(created_at: str, from_date: str, to_date: str) -> bool:
    """Return True if created_at (ISO datetime string) falls on a date in [from_date, to_date].

    Comparison is done on the date prefix (first 10 chars) of created_at.
    """
    if len(created_at) < 10:
        return False
    date_part = created_at[:10]
    return from_date <= date_part <= to_date


# ------------------------------------------------------------------
# Module-level factory using environment variables
# ------------------------------------------------------------------

def get_client() -> LpAgentClient:
    """Create an LpAgentClient from environment variables.

    Required: LPAGENT_API_KEY
    Optional: LPAGENT_WALLET (falls back to hardcoded default)

    Raises ValueError if LPAGENT_API_KEY is not set.
    """
    api_key = os.environ.get("LPAGENT_API_KEY", "")
    wallet = os.environ.get("LPAGENT_WALLET", DEFAULT_WALLET)
    return LpAgentClient(api_key=api_key, wallet=wallet)
