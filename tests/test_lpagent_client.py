"""
Tests for LpAgentClient (doc 026: JSONL rewrite + watermark redesign).

All tests are unit tests — no network access, no LPAGENT_API_KEY required.
time.sleep is monkeypatched to a no-op throughout.
"""

import json
import os
import shutil
from pathlib import Path
from uuid import uuid4
from unittest.mock import MagicMock, patch

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "lpagent_client"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _case_dir(name: str) -> Path:
    """Create an isolated temp dir under _temp/test_lpagent_client/ for each test."""
    path = PROJECT_ROOT / "_temp" / "test_lpagent_client" / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _make_client(tmp_path: Path, wallet: str = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF") -> object:
    """Create an LpAgentClient rooted in tmp_path with a fake API key."""
    from valhalla.lpagent_client import LpAgentClient
    cache_dir = tmp_path / "lpagent_cache"
    return LpAgentClient(api_key="fake-key", wallet=wallet, cache_dir=str(cache_dir))


def _mock_urlopen_with_pages(pages: list):
    """Return a mock for urllib.request.urlopen that cycles through pages."""
    call_count = [0]

    def _urlopen(req, timeout=30):
        idx = call_count[0]
        call_count[0] += 1
        if idx >= len(pages):
            # Return empty last page
            payload = {
                "status": "success",
                "data": {
                    "data": [],
                    "pagination": {"totalCount": 0, "totalPages": 1, "currentPage": idx + 1},
                },
            }
        else:
            payload = pages[idx]
        raw = json.dumps(payload).encode()
        mock_resp = MagicMock()
        mock_resp.read.return_value = raw
        mock_resp.__enter__ = lambda s: s
        mock_resp.__exit__ = MagicMock(return_value=False)
        return mock_resp

    return _urlopen


def _load_fixture_page(filename: str) -> dict:
    return json.loads((FIXTURE_DIR / filename).read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# AC-1: JSONL cache file per wallet
# ---------------------------------------------------------------------------

class TestAC1JsonlCachePerWallet:
    def test_jsonl_written_with_wallet_prefix(self, monkeypatch):
        """Happy path: successful fetch writes to positions_J4tkG.jsonl."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac1-wallet-prefix")
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client.fetch_since("2026-05-01")

        jsonl = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        assert jsonl.exists(), "positions_J4tkG.jsonl should be created"
        lines = [line for line in jsonl.read_text(encoding="utf-8").splitlines() if line.strip()]
        assert len(lines) == 20
        first = json.loads(lines[0])
        required_keys = {"tokenId", "createdAt", "updatedAt", "fetched_at_utc"}
        assert required_keys.issubset(first.keys()), f"Missing keys: {required_keys - first.keys()}"

    def test_wallet_mismatch_raises_value_error(self, monkeypatch):
        """Adversarial: second run with different wallet raises ValueError."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac1-wallet-mismatch")
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        # First run: wallet J4tkG...
        wallet_a = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"
        client_a = _make_client(tmp_path, wallet=wallet_a)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client_a.fetch_since("2026-05-01")

        # Now check watermark has wallet set
        watermark_path = tmp_path / "lpagent_sync.json"
        assert watermark_path.exists()
        wm = json.loads(watermark_path.read_text())
        assert wm["wallet"] == wallet_a

        # Second run: different wallet — load_cache should raise because watermark says J4tkG
        wallet_b = "XXXXX_different_wallet_address_suffix"
        from valhalla.lpagent_client import LpAgentClient
        client_b = LpAgentClient(
            api_key="fake-key",
            wallet=wallet_b,
            cache_dir=str(tmp_path / "lpagent_cache"),
        )
        with pytest.raises(ValueError, match="Wallet mismatch"):
            client_b.load_cache()


# ---------------------------------------------------------------------------
# AC-2: totalCount assertion
# ---------------------------------------------------------------------------

class TestAC2TotalCountAssertion:
    def test_two_pages_asserts_totalcount_success(self, monkeypatch):
        """Happy path: 2 pages of 10 = 20 total, assertion passes."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac2-totalcount-ok")
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            records = client._fetch_all_pages("2026-05-01")
        assert len(records) == 20

    def test_totalcount_mismatch_raises_assertion_error(self, monkeypatch):
        """Adversarial: API reports totalCount=50 but only returns 10 rows → AssertionError."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac2-totalcount-mismatch")
        page1 = _load_fixture_page("page1_truncated.json")
        # page1_truncated has totalCount=50, totalPages=5, data=[10]
        # We only provide one page so pagination ends after page 1 with 10 items != 50

        client = _make_client(tmp_path)

        # Simulate: after page 1, subsequent pages return empty
        empty_page = {
            "status": "success",
            "data": {
                "data": [],
                "pagination": {"totalCount": 50, "totalPages": 5, "currentPage": 2},
            },
        }
        with patch(
            "urllib.request.urlopen",
            side_effect=_mock_urlopen_with_pages([page1, empty_page]),
        ):
            with pytest.raises(AssertionError) as exc_info:
                client._fetch_all_pages("2026-05-01")

        msg = str(exc_info.value)
        assert "totalCount=50" in msg
        assert "retrieved=10" in msg
        assert "from_date=2026-05-01" in msg

    def test_totalcount_mismatch_does_not_write_jsonl(self, monkeypatch):
        """Adversarial: totalCount mismatch — no partial results written to JSONL."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac2-no-partial-write")
        page1 = _load_fixture_page("page1_truncated.json")
        empty_page = {
            "status": "success",
            "data": {
                "data": [],
                "pagination": {"totalCount": 50, "totalPages": 5, "currentPage": 2},
            },
        }
        client = _make_client(tmp_path)
        jsonl = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        assert not jsonl.exists()

        with patch(
            "urllib.request.urlopen",
            side_effect=_mock_urlopen_with_pages([page1, empty_page]),
        ):
            with pytest.raises(AssertionError):
                # fetch_since calls _fetch_all_pages then writes — but AssertionError aborts
                client._fetch_all_pages("2026-05-01")

        # File should not exist (fetch_all_pages raised before any write)
        assert not jsonl.exists(), "Partial results must not be written on totalCount mismatch"


# ---------------------------------------------------------------------------
# AC-3: Sliding-window refresh
# ---------------------------------------------------------------------------

class TestAC3SlidingWindowRefresh:
    def test_refresh_skipped_within_threshold(self, monkeypatch):
        """Adversarial: last_full_refresh_at 10h ago → skip, no urlopen called."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac3-refresh-skipped")

        from datetime import datetime, timezone, timedelta
        recent_ts = (datetime.now(timezone.utc) - timedelta(hours=10)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        watermark = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-04-01",
            "last_full_refresh_at": recent_ts,
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(watermark), encoding="utf-8")

        # Pre-populate JSONL
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)

        client = _make_client(tmp_path)
        url_open_called = []

        def _fail_urlopen(*args, **kwargs):
            url_open_called.append(True)
            raise AssertionError("urlopen should not be called when refresh is skipped")

        with patch("urllib.request.urlopen", side_effect=_fail_urlopen):
            result = client.fetch_since("2026-04-01")

        assert not url_open_called, "urlopen was called but should have been skipped"
        assert len(result) == 5  # 5 records from sample_positions.jsonl

    def test_refresh_executes_when_stale(self, monkeypatch):
        """Happy path: last_full_refresh_at 48h ago with window=120h → refresh runs."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac3-refresh-stale")

        from datetime import datetime, timezone, timedelta
        stale_ts = (datetime.now(timezone.utc) - timedelta(hours=48)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        watermark = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-04-01",
            "last_full_refresh_at": stale_ts,
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(watermark), encoding="utf-8")

        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            result = client.fetch_since("2026-04-01")

        assert len(result) == 20  # 20 records from the two-page fixture

    def test_existing_records_outside_window_preserved(self, monkeypatch):
        """Happy path: records outside refresh window are preserved after merge."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac3-window-preserved")

        # Pre-populate JSONL with 5 records from sample_positions.jsonl
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)

        # Fetch 20 new records — they have different tokenIds (tok01..tok20)
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            result = client.fetch_since("2026-04-01")

        # 5 existing (tokA1..5) + 20 new (tok01..20) = 25 total
        assert len(result) == 25


# ---------------------------------------------------------------------------
# AC-4: Partial JSONL recovery
# ---------------------------------------------------------------------------

class TestAC4PartialJsonlRecovery:
    def test_clean_jsonl_loads_all_records(self):
        """Happy path: clean JSONL with 5 lines loads all 5 records."""
        tmp_path = _case_dir("ac4-clean-jsonl")
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)

        client = _make_client(tmp_path)
        records = client.load_cache()
        assert len(records) == 5
        assert not client._needs_refresh

    def test_truncated_jsonl_recovery(self):
        """Adversarial: truncated last line → 4 records loaded, file repaired, _needs_refresh=True."""
        tmp_path = _case_dir("ac4-truncated-jsonl")
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "truncated_positions.jsonl", jsonl_path)

        client = _make_client(tmp_path)
        records = client.load_cache()

        assert len(records) == 4, f"Expected 4, got {len(records)}"
        assert client._needs_refresh, "_needs_refresh should be True after truncation"

        # File on disk should no longer contain the truncated line
        content = jsonl_path.read_text(encoding="utf-8")
        lines = [line for line in content.splitlines() if line.strip()]
        assert len(lines) == 4, "Repaired file should have exactly 4 lines"
        # All remaining lines should be valid JSON
        for line in lines:
            json.loads(line)  # should not raise


# ---------------------------------------------------------------------------
# AC-5: Atomic writes
# ---------------------------------------------------------------------------

class TestAC5AtomicWrites:
    def test_write_uses_os_replace(self, monkeypatch):
        """Happy path: write creates .tmp then calls os.replace."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac5-os-replace")
        replace_calls = []
        original_replace = os.replace

        def _spy_replace(src, dst):
            replace_calls.append((src, dst))
            return original_replace(src, dst)

        monkeypatch.setattr("os.replace", _spy_replace)

        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client.fetch_since("2026-05-01")

        assert len(replace_calls) >= 1, "os.replace should have been called"
        src, dst = replace_calls[0]
        assert src.endswith(".tmp"), f"Source should be .tmp file, got {src}"
        assert dst.endswith(".jsonl"), f"Destination should be .jsonl file, got {dst}"

    def test_crash_before_replace_leaves_live_file_intact(self, monkeypatch):
        """Adversarial: crash after .tmp write but before os.replace — live file unchanged."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac5-crash-before-replace")

        # Pre-populate live JSONL
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)
        original_content = jsonl_path.read_bytes()

        # Make os.replace raise to simulate crash
        def _crash_replace(src, dst):
            raise RuntimeError("Simulated crash before os.replace")

        monkeypatch.setattr("os.replace", _crash_replace)

        from valhalla.lpagent_client import LpAgentClient
        client = LpAgentClient(
            api_key="fake-key",
            wallet="J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            cache_dir=str(tmp_path / "lpagent_cache"),
        )
        records = {
            "tokX": {
                "tokenId": "tokX",
                "createdAt": "2026-05-01T00:00:00Z",
                "updatedAt": "2026-05-01T00:00:00Z",
                "fetched_at_utc": "2026-05-01T00:00:00Z",
            }
        }
        with pytest.raises(RuntimeError, match="Simulated crash"):
            client._write_jsonl_atomic(records)

        # Live file should be unchanged
        assert jsonl_path.read_bytes() == original_content, "Live file should be unchanged after crash"

    def test_stale_tmp_removed_on_init(self):
        """Stale .tmp file from previous crash is removed on client construction."""
        tmp_path = _case_dir("ac5-stale-tmp")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        tmp_file = cache_dir / "positions_J4tkG.tmp"
        tmp_file.write_text("stale", encoding="utf-8")
        assert tmp_file.exists()

        from valhalla.lpagent_client import LpAgentClient
        LpAgentClient(
            api_key="fake-key",
            wallet="J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            cache_dir=str(cache_dir),
        )
        assert not tmp_file.exists(), "Stale .tmp should be removed on init"


# ---------------------------------------------------------------------------
# AC-6: Back-compat fetch_range shim
# ---------------------------------------------------------------------------

class TestAC6FetchRangeShim:
    def test_fetch_range_filters_by_date(self):
        """Happy path: pre-loaded JSONL, fetch_range returns only records in range."""
        tmp_path = _case_dir("ac6-filter-by-date")
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)

        client = _make_client(tmp_path)
        # sample_positions has createdAt: 2026-04-01, 04-05, 04-10, 04-15, 04-20
        result = client.fetch_range("2026-04-04", "2026-04-12")
        token_ids = {r["tokenId"] for r in result}
        assert token_ids == {"tokA2", "tokA3"}, f"Expected tokA2+tokA3, got {token_ids}"

    def test_fetch_range_triggers_fetch_when_cache_empty(self, monkeypatch):
        """Adversarial: empty cache + fetch_range → triggers fetch_since, returns data."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac6-fetch-on-empty")
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            result = client.fetch_range("2026-05-01", "2026-05-02")

        # Records from fixture: tok01-tok10 have createdAt 2026-05-01, tok11-20 have 2026-05-02
        assert len(result) == 20, f"Expected 20, got {len(result)}"

    def test_fetch_range_returns_list_of_dicts(self):
        """fetch_range returns List[dict] for CrossChecker compatibility."""
        tmp_path = _case_dir("ac6-returns-list")
        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)

        client = _make_client(tmp_path)
        result = client.fetch_range("2026-04-01", "2026-04-30")
        assert isinstance(result, list)
        assert all(isinstance(r, dict) for r in result)


# ---------------------------------------------------------------------------
# AC-7: Watermark format
# ---------------------------------------------------------------------------

class TestAC7WatermarkFormat:
    def test_read_watermark_new_format(self):
        """Happy path: new format watermark read back as dict."""
        tmp_path = _case_dir("ac7-new-format")
        from valhalla.lpagent_pipeline import read_watermark
        wm = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-02-11",
            "last_full_refresh_at": "2026-05-09T14:22:00Z",
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(wm), encoding="utf-8")
        result = read_watermark(str(tmp_path))
        assert result["wallet"] == wm["wallet"]
        assert result["min_safe_open_date"] == "2026-02-11"
        assert result["last_full_refresh_at"] == "2026-05-09T14:22:00Z"
        assert result["refresh_window_hours"] == 120

    def test_read_watermark_legacy_format_promoted(self):
        """Adversarial: old format auto-promoted; new format written back."""
        tmp_path = _case_dir("ac7-legacy-promote")
        from valhalla.lpagent_pipeline import read_watermark
        legacy = {"last_synced_date": "2026-04-30"}
        sync_path = tmp_path / "lpagent_sync.json"
        sync_path.write_text(json.dumps(legacy), encoding="utf-8")

        result = read_watermark(str(tmp_path))

        assert result["min_safe_open_date"] == "2026-04-30"
        assert result["last_full_refresh_at"] is None
        assert "wallet" in result

        # Verify new format was written back
        written = json.loads(sync_path.read_text(encoding="utf-8"))
        assert "min_safe_open_date" in written
        assert written["min_safe_open_date"] == "2026-04-30"
        assert "last_synced_date" not in written

    def test_read_watermark_missing_file_returns_defaults(self):
        """Missing file returns defaults."""
        tmp_path = _case_dir("ac7-missing-file")
        from valhalla.lpagent_pipeline import read_watermark, _default_watermark
        result = read_watermark(str(tmp_path))
        defaults = _default_watermark()
        assert result["min_safe_open_date"] == defaults["min_safe_open_date"]
        assert result["last_full_refresh_at"] is None

    def test_write_watermark_accepts_dict(self):
        """write_watermark accepts a dict and writes it."""
        tmp_path = _case_dir("ac7-write-dict")
        from valhalla.lpagent_pipeline import write_watermark, read_watermark
        wm = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-02-11",
            "last_full_refresh_at": "2026-05-09T14:22:00Z",
            "refresh_window_hours": 120,
        }
        write_watermark(str(tmp_path), wm)
        result = read_watermark(str(tmp_path))
        assert result["wallet"] == wm["wallet"]
        assert result["min_safe_open_date"] == "2026-02-11"

    def test_write_watermark_raises_type_error_for_string(self):
        """write_watermark raises TypeError when called with a string (old signature)."""
        tmp_path = _case_dir("ac7-type-error")
        from valhalla.lpagent_pipeline import write_watermark
        with pytest.raises(TypeError, match="no longer accepts a date string"):
            write_watermark(str(tmp_path), "2026-04-30")

    def test_watermark_written_after_fetch(self, monkeypatch):
        """After a successful fetch_since, lpagent_sync.json has new-format watermark."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("ac7-watermark-after-fetch")
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client.fetch_since("2026-05-01")

        from valhalla.lpagent_pipeline import read_watermark
        wm = read_watermark(str(tmp_path))
        assert wm["wallet"] == "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"
        assert wm["last_full_refresh_at"] is not None
        assert wm["refresh_window_hours"] == 120


# ---------------------------------------------------------------------------
# AC: No sleep during tests
# ---------------------------------------------------------------------------

class TestNoSleepDuringTests:
    def test_no_sleep_called_during_single_page_fetch(self, monkeypatch):
        """time.sleep with value > 0 is not called on the first (only) request."""
        sleep_calls = []
        monkeypatch.setattr("time.sleep", lambda n: sleep_calls.append(n))
        tmp_path = _case_dir("nosleep-single-page")

        # Single page response
        single_page = {
            "status": "success",
            "data": {
                "data": [
                    {"tokenId": "t1", "createdAt": "2026-05-01T10:00:00Z",
                     "updatedAt": "2026-05-01T12:00:00Z"},
                ],
                "pagination": {"totalCount": 1, "totalPages": 1, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([single_page])):
            client._fetch_all_pages("2026-05-01")

        long_sleeps = [s for s in sleep_calls if s > 0]
        assert len(long_sleeps) == 0, (
            f"sleep() called with {long_sleeps} — should not sleep before the first request"
        )

    def test_sleep_called_between_pages(self, monkeypatch):
        """time.sleep IS called between page fetches (page 2 onward)."""
        sleep_calls = []
        monkeypatch.setattr("time.sleep", lambda n: sleep_calls.append(n))
        tmp_path = _case_dir("nosleep-between-pages")

        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client._fetch_all_pages("2026-05-01")

        # Should have slept once (before page 2)
        assert len(sleep_calls) == 1
        assert sleep_calls[0] == 12  # RATE_LIMIT_SLEEP


# ---------------------------------------------------------------------------
# Back-compat smoke: fetch_range still works as run_cross_check primitive
# ---------------------------------------------------------------------------

class TestRunCrossCheckBackCompat:
    def test_fetch_range_returns_data_for_cross_check(self, monkeypatch):
        """fetch_range returns List[dict] that CrossChecker can consume."""
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("backcompat-cross-check")

        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        from valhalla.lpagent_client import LpAgentClient
        client = LpAgentClient(
            api_key="fake-key",
            wallet="J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            cache_dir=str(tmp_path / "lpagent_cache"),
        )
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            raw = client.fetch_range("2026-05-01", "2026-05-02")

        assert isinstance(raw, list)
        assert len(raw) == 20
        for item in raw:
            assert "tokenId" in item
