"""
LpAgent API client for fetching closed Meteora positions.

Fetches all closed positions for a wallet within a date range.
Handles pagination, rate limiting (5 RPM = 12s between requests),
and daily file-based caching in output/lpagent_cache/YYYY-MM-DD.json.
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

DEFAULT_WALLET = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"
API_BASE = "https://api.lpagent.io/open-api/v1"
PAGE_SIZE = 10
RATE_LIMIT_SLEEP = 12  # seconds between API requests (5 RPM free tier)


class LpAgentClient:
    """
    Client for the LpAgent API.

    Fetches closed Meteora positions for a single wallet.
    Results are cached daily in {cache_dir}/YYYY-MM-DD.json.
    Today's date is never cached (positions may still be closing).
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
        self._first_request = True  # Track whether we've made any API call this session

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def fetch_day(self, date_str: str) -> List[dict]:
        """
        Fetch all closed positions opened on a given date (YYYY-MM-DD).

        Returns cached data if available (and date is not today).
        Otherwise fetches from API, caches the result (unless today), and returns it.
        """
        cached = self._load_cache(date_str)
        if cached is not None:
            logger.info("Cache hit for %s (%d positions)", date_str, len(cached))
            return cached

        logger.info("Cache miss for %s — fetching from API", date_str)
        positions = self._fetch_from_api(date_str)

        today = date.today().strftime("%Y-%m-%d")
        if date_str != today:
            self._save_cache(date_str, positions)
        else:
            logger.debug("Skipping cache write for today (%s)", date_str)

        return positions

    def fetch_range(self, from_date: str, to_date: str) -> List[dict]:
        """
        Fetch positions for a date range (inclusive), day by day.

        Args:
            from_date: Start date as YYYY-MM-DD (inclusive).
            to_date: End date as YYYY-MM-DD (inclusive).

        Returns:
            Combined list of position dicts from all days in range.
        """
        start = datetime.strptime(from_date, "%Y-%m-%d").date()
        end = datetime.strptime(to_date, "%Y-%m-%d").date()

        if start > end:
            raise ValueError(
                f"from_date ({from_date}) must not be after to_date ({to_date})"
            )

        all_positions: List[dict] = []
        current = start
        while current <= end:
            day_str = current.strftime("%Y-%m-%d")
            positions = self.fetch_day(day_str)
            all_positions.extend(positions)
            current += timedelta(days=1)

        logger.info(
            "fetch_range %s..%s → %d positions total",
            from_date,
            to_date,
            len(all_positions),
        )
        return all_positions

    # ------------------------------------------------------------------
    # Internal: API fetching
    # ------------------------------------------------------------------

    def _fetch_from_api(self, date_str: str) -> List[dict]:
        """
        Fetch all closed positions opened on date_str from the API.

        Paginates until all pages are retrieved. Sleeps RATE_LIMIT_SLEEP
        seconds before each request to respect the 5 RPM free-tier limit.
        """
        all_positions: List[dict] = []
        page = 1
        total = None  # filled after first response

        while True:
            # Rate limit: sleep before every request.
            # Skip the very first request of the session to avoid an unnecessary wait
            # when the user only needs one page.
            if self._first_request:
                self._first_request = False
                logger.debug("First request of session — skipping initial sleep")
            else:
                logger.debug("Rate limit sleep: %ds before page %d", RATE_LIMIT_SLEEP, page)
                time.sleep(RATE_LIMIT_SLEEP)

            params = {
                "wallet": self._wallet,
                "status": "Close",
                "from_date": date_str,
                "to_date": date_str,
                "page": str(page),
                "pageSize": str(PAGE_SIZE),
            }
            query_string = "&".join(f"{k}={urllib.parse.quote(v)}" for k, v in params.items())
            url = f"{API_BASE}/positions?{query_string}"

            logger.debug("GET %s (page %d)", url, page)
            data = self._lpagent_get(url)

            page_data = data.get("data", [])
            if total is None:
                total = data.get("total", 0)
                logger.info(
                    "API: date=%s total=%d, fetching page %d/%d",
                    date_str,
                    total,
                    page,
                    max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE),
                )

            all_positions.extend(page_data)

            # Pagination check: stop when we've fetched all pages
            if (page - 1) * PAGE_SIZE + len(page_data) >= total:
                break

            page += 1

        logger.info(
            "Fetched %d positions for %s (API reported total=%d)",
            len(all_positions),
            date_str,
            total or 0,
        )
        return all_positions

    def _lpagent_get(self, url: str) -> dict:
        """
        Make a single GET request to the LpAgent API.

        Raises RuntimeError on non-200 responses or JSON decode failures.
        """
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self._api_key}",
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
    # Internal: cache
    # ------------------------------------------------------------------

    def _cache_path(self, date_str: str) -> Path:
        return self._cache_dir / f"{date_str}.json"

    def _load_cache(self, date_str: str) -> Optional[List[dict]]:
        """
        Load cached positions for date_str.

        Returns the cached list if the file exists and date is not today.
        Returns None if cache miss or date is today.
        """
        today = date.today().strftime("%Y-%m-%d")
        if date_str == today:
            logger.debug("Not reading cache for today (%s)", date_str)
            return None

        path = self._cache_path(date_str)
        if not path.exists():
            return None

        try:
            with path.open("r", encoding="utf-8") as fh:
                return json.load(fh)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to read cache file %s: %s", path, e)
            return None

    def _save_cache(self, date_str: str, positions: List[dict]) -> None:
        """Write positions list to the daily cache file."""
        path = self._cache_path(date_str)
        try:
            with path.open("w", encoding="utf-8") as fh:
                json.dump(positions, fh, indent=2, ensure_ascii=False)
            logger.info("Cached %d positions to %s", len(positions), path)
        except OSError as e:
            logger.warning("Failed to write cache file %s: %s", path, e)


# ------------------------------------------------------------------
# Module-level factory using environment variables
# ------------------------------------------------------------------

def get_client() -> LpAgentClient:
    """
    Create an LpAgentClient from environment variables.

    Required: LPAGENT_API_KEY
    Optional: LPAGENT_WALLET (falls back to hardcoded default)

    Raises ValueError if LPAGENT_API_KEY is not set.
    """
    api_key = os.environ.get("LPAGENT_API_KEY", "")
    wallet = os.environ.get("LPAGENT_WALLET", DEFAULT_WALLET)
    return LpAgentClient(api_key=api_key, wallet=wallet)
