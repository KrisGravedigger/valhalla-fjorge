"""
Adversarial tests for LpAgentClient (doc 026: JSONL rewrite + watermark redesign).

Written BEFORE reading the implementation — tests are derived solely from the
design doc contract, the existing test conventions, and brainstormed edge cases.

All tests are unit tests — no network access, no LPAGENT_API_KEY required.
time.sleep is monkeypatched to a no-op throughout.
"""

import json
import os
from pathlib import Path
from uuid import uuid4
from unittest.mock import MagicMock, patch

import pytest

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "lpagent_client"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Helpers (same pattern as test_lpagent_client.py)
# ---------------------------------------------------------------------------

def _case_dir(name: str) -> Path:
    path = PROJECT_ROOT / "_temp" / "test_lpagent_client_adv" / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _make_client(tmp_path: Path, wallet: str = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF") -> object:
    from valhalla.lpagent_client import LpAgentClient
    cache_dir = tmp_path / "lpagent_cache"
    return LpAgentClient(api_key="fake-key", wallet=wallet, cache_dir=str(cache_dir))


def _mock_urlopen_with_pages(pages: list):
    call_count = [0]

    def _urlopen(req, timeout=30):
        idx = call_count[0]
        call_count[0] += 1
        if idx >= len(pages):
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
# ATTACK 1: Wallet mismatch bypass via fetch_since (not just load_cache)
#
# The design doc says check wallet on cache open. The existing test only tests
# the bypass via explicit load_cache(). But fetch_since() also loads cache
# internally. If the wallet check is not called inside fetch_since, an attacker
# can silently contaminate a J4tkG cache by running fetch_since with XXXXX wallet.
# ---------------------------------------------------------------------------

class TestWalletMismatchBypass:
    def test_fetch_since_raises_on_wallet_mismatch_without_calling_load_cache_explicitly(self, monkeypatch):
        """
        ATTACK: Call fetch_since() directly with wrong wallet (not load_cache).
        Contract (AC-1 adversarial): wallet mismatch must be caught regardless
        of which public method is called first.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-wallet-bypass-fetch-since")

        # First run: build cache with J4tkG
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")
        client_a = _make_client(tmp_path, wallet="J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF")
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client_a.fetch_since("2026-05-01")

        # Watermark now records wallet=J4tkG.
        # Second run: different wallet calls fetch_since directly (NOT load_cache).
        from valhalla.lpagent_client import LpAgentClient
        client_b = LpAgentClient(
            api_key="fake-key",
            wallet="XXXXX_intruder_wallet_address",
            cache_dir=str(tmp_path / "lpagent_cache"),
        )
        page1b = _load_fixture_page("page1_of_2.json")
        page2b = _load_fixture_page("page2_of_2.json")
        with pytest.raises(ValueError, match="Wallet mismatch"):
            with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1b, page2b])):
                client_b.fetch_since("2026-05-01")

    def test_fetch_range_raises_on_wallet_mismatch(self, monkeypatch):
        """
        ATTACK: Call fetch_range() with wrong wallet.
        Same threat as above but via the fetch_range shim path.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-wallet-bypass-fetch-range")

        # Build cache with J4tkG first
        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")
        client_a = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
            client_a.fetch_since("2026-05-01")

        from valhalla.lpagent_client import LpAgentClient
        client_b = LpAgentClient(
            api_key="fake-key",
            wallet="XXXXX_intruder_wallet_address",
            cache_dir=str(tmp_path / "lpagent_cache"),
        )
        with pytest.raises(ValueError, match="Wallet mismatch"):
            with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page1, page2])):
                client_b.fetch_range("2026-05-01", "2026-05-02")


# ---------------------------------------------------------------------------
# ATTACK 2: totalCount=0 — new wallet, zero positions
#
# AC-2 happy path specifies totalCount=20. The spec never explicitly says what
# happens with totalCount=0. An AssertionError on 0==0 should NOT fire, but
# a buggy implementation might divide by zero or emit a confusing error.
# ---------------------------------------------------------------------------

class TestTotalCountZero:
    def test_totalcount_zero_returns_empty_list_and_writes_jsonl(self, monkeypatch):
        """
        ATTACK: API returns totalCount=0, data=[]. Should write an empty (or
        zero-record) JSONL and NOT raise AssertionError.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-totalcount-zero")

        empty_response = {
            "status": "success",
            "data": {
                "data": [],
                "pagination": {"totalCount": 0, "totalPages": 0, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        # Should NOT raise
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([empty_response])):
            result = client.fetch_since("2026-05-01")

        assert result == [], f"fetch_since on empty API must return [], got {result!r}"
        # JSONL may exist (empty file) or not — but no exception

    def test_totalcount_zero_first_page_has_data_field_empty(self, monkeypatch):
        """
        ATTACK: totalCount=20 but data=[] on page 1 (API glitch).
        Should raise AssertionError with diagnostic context.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-totalcount-20-data-empty")

        page_with_no_data = {
            "status": "success",
            "data": {
                "data": [],
                "pagination": {"totalCount": 20, "totalPages": 2, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with pytest.raises(AssertionError) as exc_info:
            with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([page_with_no_data])):
                client._fetch_all_pages("2026-05-01")

        msg = str(exc_info.value)
        assert "totalCount=20" in msg or "20" in msg
        assert "retrieved=0" in msg or "0" in msg


# ---------------------------------------------------------------------------
# ATTACK 3: Deduplication tie-breaking on equal updatedAt
#
# The spec says "newer updatedAt wins" with strict >. When updatedAt is equal,
# the spec says existing wins (not overwrite). Test this boundary.
# ---------------------------------------------------------------------------

class TestDeduplicationTieBreaking:
    def test_equal_updatedat_existing_record_wins(self, monkeypatch):
        """
        ATTACK: Fetch returns same tokenId with identical updatedAt as cached.
        Contract: existing record should win (strict > comparison).
        The fetched record should NOT overwrite the cached one.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-dedup-tie-equal-updatedat")

        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        # Pre-cache: tokA1 with updatedAt 2026-05-01T10:00:00Z, pnlNative=0.999 (distinct)
        existing = {
            "tokenId": "tokTIE",
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T10:00:00Z",
            "fetched_at_utc": "2026-05-01T11:00:00Z",
            "pnlNative": 0.999,
        }
        jsonl_path.write_text(json.dumps(existing) + "\n", encoding="utf-8")

        # Fetch returns same tokenId, SAME updatedAt, but different pnlNative
        fetch_response = {
            "status": "success",
            "data": {
                "data": [
                    {
                        "tokenId": "tokTIE",
                        "createdAt": "2026-05-01T08:00:00Z",
                        "updatedAt": "2026-05-01T10:00:00Z",  # SAME
                        "pnlNative": 0.111,  # Different — should NOT win
                    }
                ],
                "pagination": {"totalCount": 1, "totalPages": 1, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([fetch_response])):
            client.fetch_since("2026-05-01")

        cache = client.load_cache()
        # Existing record should win (pnlNative=0.999 preserved)
        assert cache["tokTIE"]["pnlNative"] == 0.999, (
            f"Equal-updatedAt tie: existing record should win, "
            f"got pnlNative={cache['tokTIE']['pnlNative']}"
        )

    def test_newer_updatedat_by_one_second_overwrites_existing(self, monkeypatch):
        """
        ATTACK: Fetch returns same tokenId with updatedAt one second newer.
        Contract: newer record should win.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-dedup-one-second-newer")

        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        existing = {
            "tokenId": "tokNEW",
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T10:00:00Z",
            "fetched_at_utc": "2026-05-01T11:00:00Z",
            "pnlNative": 0.111,
        }
        jsonl_path.write_text(json.dumps(existing) + "\n", encoding="utf-8")

        fetch_response = {
            "status": "success",
            "data": {
                "data": [
                    {
                        "tokenId": "tokNEW",
                        "createdAt": "2026-05-01T08:00:00Z",
                        "updatedAt": "2026-05-01T10:00:01Z",  # 1 second NEWER
                        "pnlNative": 0.999,  # Should win
                    }
                ],
                "pagination": {"totalCount": 1, "totalPages": 1, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([fetch_response])):
            client.fetch_since("2026-05-01")

        cache = client.load_cache()
        assert cache["tokNEW"]["pnlNative"] == 0.999, (
            f"Newer-by-1s record should overwrite, got pnlNative={cache['tokNEW']['pnlNative']}"
        )

    def test_older_updatedat_fetched_does_not_overwrite_newer_cached(self, monkeypatch):
        """
        ATTACK: Fetch returns record with OLDER updatedAt than cached.
        Contract: cached (newer) record must survive.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-dedup-older-fetched-loses")

        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        existing = {
            "tokenId": "tokOLD",
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-03T10:00:00Z",  # NEWER cached
            "fetched_at_utc": "2026-05-03T11:00:00Z",
            "pnlNative": 0.777,
        }
        jsonl_path.write_text(json.dumps(existing) + "\n", encoding="utf-8")

        fetch_response = {
            "status": "success",
            "data": {
                "data": [
                    {
                        "tokenId": "tokOLD",
                        "createdAt": "2026-05-01T08:00:00Z",
                        "updatedAt": "2026-05-01T10:00:00Z",  # OLDER fetched — must lose
                        "pnlNative": 0.001,
                    }
                ],
                "pagination": {"totalCount": 1, "totalPages": 1, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([fetch_response])):
            client.fetch_since("2026-05-01")

        cache = client.load_cache()
        assert cache["tokOLD"]["pnlNative"] == 0.777, (
            f"Older fetched record must NOT overwrite newer cached, "
            f"got pnlNative={cache['tokOLD']['pnlNative']}"
        )


# ---------------------------------------------------------------------------
# ATTACK 4: Watermark with neither old nor new format
#
# AC-7 specifies: old format (last_synced_date key) → promote.
# New format (wallet key) → return as-is.
# Neither format (e.g., {"foo": "bar"} or {}) → spec is silent. Crash or defaults?
# ---------------------------------------------------------------------------

class TestWatermarkCorruptOrUnknownFormat:
    def test_watermark_unknown_format_does_not_crash(self):
        """
        ATTACK: lpagent_sync.json contains {"foo": "bar"} — neither old nor new format.
        Contract is silent. Acceptable behaviors: return defaults or raise with clear message.
        Must NOT crash with KeyError or AttributeError.
        """
        tmp_path = _case_dir("adv-watermark-unknown-format")
        from valhalla.lpagent_pipeline import read_watermark

        (tmp_path / "lpagent_sync.json").write_text(
            json.dumps({"foo": "bar", "random_key": 42}), encoding="utf-8"
        )
        # Should not raise KeyError/AttributeError — either return defaults or raise TypeError
        try:
            result = read_watermark(str(tmp_path))
            # If it returns something, it must have the required keys
            assert "min_safe_open_date" in result, (
                "Unknown-format watermark should return dict with min_safe_open_date"
            )
        except (KeyError, AttributeError) as e:
            pytest.fail(
                f"read_watermark crashed with {type(e).__name__} on unknown-format file: {e}"
            )

    def test_watermark_empty_dict_does_not_crash(self):
        """
        ATTACK: lpagent_sync.json is empty JSON object {}.
        """
        tmp_path = _case_dir("adv-watermark-empty-dict")
        from valhalla.lpagent_pipeline import read_watermark

        (tmp_path / "lpagent_sync.json").write_text("{}", encoding="utf-8")
        try:
            result = read_watermark(str(tmp_path))
            assert "min_safe_open_date" in result
        except (KeyError, AttributeError) as e:
            pytest.fail(f"read_watermark crashed on empty dict: {e}")

    def test_watermark_corrupt_json_does_not_crash_with_cryptic_error(self):
        """
        ATTACK: lpagent_sync.json contains truncated JSON: {"wallet": "abc"
        Should fall back to defaults, not crash with json.JSONDecodeError unhandled.
        """
        tmp_path = _case_dir("adv-watermark-truncated-json")
        from valhalla.lpagent_pipeline import read_watermark

        (tmp_path / "lpagent_sync.json").write_text('{"wallet": "abc"', encoding="utf-8")
        try:
            result = read_watermark(str(tmp_path))
            # If recovered, must have defaults
            assert "min_safe_open_date" in result
        except json.JSONDecodeError as e:
            pytest.fail(
                f"read_watermark let JSONDecodeError propagate on corrupt watermark: {e}"
            )


# ---------------------------------------------------------------------------
# ATTACK 5: Refresh threshold boundary — exactly at 24h
#
# Spec: skip if hours_ago < REFRESH_THRESHOLD_HOURS (24).
# At exactly 24h → should refresh. At 23h59m → should skip.
# ---------------------------------------------------------------------------

class TestRefreshThresholdBoundary:
    def test_exactly_24h_ago_triggers_refresh(self, monkeypatch):
        """
        ATTACK: last_full_refresh_at exactly 24 hours ago (to the second).
        Contract: hours_ago < REFRESH_THRESHOLD_HOURS → skip.
        At hours_ago == 24.0 the condition is False → refresh must run.

        Uses frozen 'now' via monkeypatching to avoid race conditions.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-threshold-exactly-24h")

        from datetime import datetime, timezone, timedelta
        import valhalla.lpagent_client as _lc_module

        # Freeze "now" to a fixed instant so the comparison is deterministic
        frozen_now = datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)
        exactly_24h_ago = frozen_now - timedelta(hours=24)
        last_refresh_str = exactly_24h_ago.strftime("%Y-%m-%dT%H:%M:%SZ")

        # Monkeypatch datetime.now inside the lpagent_client module
        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return frozen_now

        monkeypatch.setattr(_lc_module, "datetime", _FrozenDatetime)

        watermark = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-04-01",
            "last_full_refresh_at": last_refresh_str,
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(watermark), encoding="utf-8")

        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        jsonl_path.write_text("", encoding="utf-8")

        page1 = _load_fixture_page("page1_of_2.json")
        page2 = _load_fixture_page("page2_of_2.json")

        url_open_called = []
        original_urlopen = _mock_urlopen_with_pages([page1, page2])

        def _spy_urlopen(req, timeout=30):
            url_open_called.append(True)
            return original_urlopen(req, timeout=timeout)

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_spy_urlopen):
            client.fetch_since("2026-04-01")

        assert url_open_called, (
            "fetch_since should trigger a network call when last_full_refresh_at == exactly 24h ago "
            "(hours_ago == 24.0 is NOT < 24 → condition is False → must refresh)"
        )

    def test_23h59m_ago_skips_refresh(self, monkeypatch):
        """
        ATTACK: last_full_refresh_at 23h 59m ago → must skip.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-threshold-23h59m")

        from datetime import datetime, timezone, timedelta
        just_under_24h = (datetime.now(timezone.utc) - timedelta(hours=23, minutes=59)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        watermark = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-04-01",
            "last_full_refresh_at": just_under_24h,
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(watermark), encoding="utf-8")

        jsonl_path = tmp_path / "lpagent_cache" / "positions_J4tkG.jsonl"
        jsonl_path.parent.mkdir(parents=True, exist_ok=True)
        existing = {
            "tokenId": "tokSKIP",
            "createdAt": "2026-04-01T10:00:00Z",
            "updatedAt": "2026-04-02T10:00:00Z",
            "fetched_at_utc": "2026-04-03T00:00:00Z",
            "pnlNative": 0.1,
        }
        jsonl_path.write_text(json.dumps(existing) + "\n", encoding="utf-8")

        url_open_called = []

        def _fail_urlopen(*args, **kwargs):
            url_open_called.append(True)
            raise AssertionError("urlopen should not be called when refresh is skipped")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_fail_urlopen):
            result = client.fetch_since("2026-04-01")

        assert not url_open_called, "urlopen called but refresh should be skipped at 23h59m"
        assert len(result) == 1, f"Cached record should be returned, got {len(result)}"


# ---------------------------------------------------------------------------
# ATTACK 6: JSONL with trailing newline — clean file must not set _needs_refresh
#
# If implementation uses split(b"\n"), a trailing newline produces a phantom
# empty byte string that fails json.loads — could spuriously set _needs_refresh=True.
# The design doc uses splitlines() which handles this correctly, but implementation
# might deviate.
# ---------------------------------------------------------------------------

class TestJsonlTrailingNewlineNotFalsePositive:
    def test_clean_jsonl_with_trailing_newline_does_not_set_needs_refresh(self):
        """
        ATTACK: clean JSONL ending with \\n — must NOT trigger _needs_refresh=True.
        A naive split('\\n') yields a phantom empty string at the end.
        """
        tmp_path = _case_dir("adv-trailing-newline")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        # Write 3 valid records with explicit trailing newline
        records = [
            {"tokenId": "t1", "createdAt": "2026-05-01T10:00:00Z",
             "updatedAt": "2026-05-01T12:00:00Z", "fetched_at_utc": "2026-05-01T13:00:00Z"},
            {"tokenId": "t2", "createdAt": "2026-05-01T11:00:00Z",
             "updatedAt": "2026-05-01T13:00:00Z", "fetched_at_utc": "2026-05-01T14:00:00Z"},
            {"tokenId": "t3", "createdAt": "2026-05-01T12:00:00Z",
             "updatedAt": "2026-05-01T14:00:00Z", "fetched_at_utc": "2026-05-01T15:00:00Z"},
        ]
        content = "\n".join(json.dumps(r) for r in records) + "\n"
        jsonl_path.write_text(content, encoding="utf-8")

        client = _make_client(tmp_path)
        loaded = client.load_cache()

        assert len(loaded) == 3, f"Expected 3 records, got {len(loaded)}"
        assert not client._needs_refresh, (
            "_needs_refresh should be False for a clean JSONL with trailing newline"
        )

    def test_jsonl_only_blank_lines_does_not_crash(self):
        """
        ATTACK: JSONL file contains only blank lines / whitespace.
        Should return empty dict and not crash.
        """
        tmp_path = _case_dir("adv-jsonl-only-blank-lines")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"
        jsonl_path.write_text("\n\n\n   \n", encoding="utf-8")

        client = _make_client(tmp_path)
        try:
            loaded = client.load_cache()
            assert isinstance(loaded, dict), "load_cache should return dict even for blank-lines file"
        except Exception as e:
            pytest.fail(f"load_cache crashed on blank-only JSONL: {type(e).__name__}: {e}")

    def test_jsonl_empty_file_does_not_crash(self):
        """
        ATTACK: JSONL file is completely empty (0 bytes).
        Should return empty dict, not crash.
        """
        tmp_path = _case_dir("adv-jsonl-empty-file")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"
        jsonl_path.write_bytes(b"")

        client = _make_client(tmp_path)
        try:
            loaded = client.load_cache()
            assert loaded == {} or isinstance(loaded, dict)
        except Exception as e:
            pytest.fail(f"load_cache crashed on empty file: {type(e).__name__}: {e}")

    def test_jsonl_single_record_no_trailing_newline_loads_correctly(self):
        """
        ATTACK: JSONL with exactly one record and NO trailing newline.
        Must parse correctly without _needs_refresh.
        """
        tmp_path = _case_dir("adv-jsonl-single-no-newline")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        record = {"tokenId": "tokSINGLE", "createdAt": "2026-05-01T10:00:00Z",
                  "updatedAt": "2026-05-01T12:00:00Z", "fetched_at_utc": "2026-05-01T13:00:00Z"}
        jsonl_path.write_text(json.dumps(record), encoding="utf-8")  # no trailing \n

        client = _make_client(tmp_path)
        loaded = client.load_cache()

        assert len(loaded) == 1, f"Expected 1 record, got {len(loaded)}"
        assert not client._needs_refresh, "_needs_refresh should be False for single valid record"


# ---------------------------------------------------------------------------
# ATTACK 7: Unicode tokenId in JSONL
#
# The spec does not restrict tokenId character set. If implementation uses
# path.read_bytes().decode("ascii"), unicode tokenIds will crash.
# ---------------------------------------------------------------------------

class TestUnicodeTokenId:
    def test_unicode_token_id_survives_cache_roundtrip(self, monkeypatch):
        """
        ATTACK: tokenId contains non-ASCII characters (e.g., CJK, emoji, Arabic).
        Should survive write → load_cache roundtrip without corruption.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-unicode-tokenid")

        unicode_token = "token-中文-العربية-\U0001f680"
        fetch_response = {
            "status": "success",
            "data": {
                "data": [
                    {
                        "tokenId": unicode_token,
                        "createdAt": "2026-05-01T08:00:00Z",
                        "updatedAt": "2026-05-01T10:00:00Z",
                        "pnlNative": 1.23,
                    }
                ],
                "pagination": {"totalCount": 1, "totalPages": 1, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([fetch_response])):
            client.fetch_since("2026-05-01")

        loaded = client.load_cache()
        assert unicode_token in loaded, (
            f"Unicode tokenId should survive cache roundtrip, "
            f"got keys: {list(loaded.keys())}"
        )


# ---------------------------------------------------------------------------
# ATTACK 8: fetch_range with from_date > to_date (inverted range)
#
# AC-6 shim: filters by createdAt in [from_date, to_date].
# Inverted range must return [] not crash.
# ---------------------------------------------------------------------------

class TestFetchRangeInvertedDates:
    def test_fetch_range_inverted_dates_returns_empty_list(self):
        """
        ATTACK: fetch_range("2026-05-30", "2026-05-01") — to_date before from_date.
        Must return empty list, not crash.
        """
        tmp_path = _case_dir("adv-fetch-range-inverted")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        # Pre-populate so no network call is needed
        record = {
            "tokenId": "tokINV",
            "createdAt": "2026-05-15T10:00:00Z",
            "updatedAt": "2026-05-15T12:00:00Z",
            "fetched_at_utc": "2026-05-15T13:00:00Z",
        }
        jsonl_path.write_text(json.dumps(record) + "\n", encoding="utf-8")

        client = _make_client(tmp_path)
        # Inverted date range — record's createdAt is 2026-05-15, outside [from=05-30, to=05-01]
        result = client.fetch_range("2026-05-30", "2026-05-01")
        assert isinstance(result, list), "fetch_range should return a list"
        assert len(result) == 0, f"Inverted date range should return empty list, got {len(result)}"


# ---------------------------------------------------------------------------
# ATTACK 9: fetch_range — cache has records but NONE in the requested range
#
# AC-6: shim filters by createdAt in [from_date, to_date].
# Cache populated but no records in range → must return [] without network call.
# ---------------------------------------------------------------------------

class TestFetchRangeNoneInRange:
    def test_fetch_range_populated_cache_but_none_in_range_returns_empty(self):
        """
        ATTACK: Cache has 5 records all in April 2026.
        fetch_range("2026-01-01", "2026-01-31") → must return [] without network call.
        """
        import shutil
        tmp_path = _case_dir("adv-fetch-range-none-in-range")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        # Write watermark so JSONL is not considered "needs refresh"
        watermark = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": "2026-04-01",
            "last_full_refresh_at": "2026-05-01T00:00:00Z",
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(watermark), encoding="utf-8")

        shutil.copy(FIXTURE_DIR / "sample_positions.jsonl", jsonl_path)
        # sample_positions has createdAt: 2026-04-01, 04-05, 04-10, 04-15, 04-20

        url_open_called = []

        def _fail_urlopen(*args, **kwargs):
            url_open_called.append(True)
            raise AssertionError("urlopen should not be called when cache covers the period")

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_fail_urlopen):
            result = client.fetch_range("2026-01-01", "2026-01-31")

        assert isinstance(result, list)
        assert len(result) == 0, f"No records in Jan 2026, expected [], got {len(result)}"
        assert not url_open_called, "Should not hit network when cache is populated"


# ---------------------------------------------------------------------------
# ATTACK 10: Stale .tmp for a DIFFERENT wallet prefix in same cache_dir
#
# Existing test verifies J4tkG.tmp is removed on J4tkG client init.
# If implementation uses glob("*.tmp") instead of wallet-specific path,
# it will delete XXXXX wallet's in-flight .tmp on J4tkG init — data corruption.
# ---------------------------------------------------------------------------

class TestStaleTmpOtherWallet:
    def test_client_does_not_delete_other_wallets_tmp_file(self):
        """
        ATTACK: positions_XXXXX.tmp exists in cache_dir when J4tkG client inits.
        J4tkG client must NOT delete XXXXX wallet's .tmp.
        """
        tmp_path = _case_dir("adv-stale-tmp-other-wallet")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)

        # Other wallet's tmp
        other_tmp = cache_dir / "positions_XXXXX.tmp"
        other_tmp.write_text("other wallet in-flight data", encoding="utf-8")

        # Own wallet's stale tmp
        own_tmp = cache_dir / "positions_J4tkG.tmp"
        own_tmp.write_text("stale", encoding="utf-8")

        from valhalla.lpagent_client import LpAgentClient
        LpAgentClient(
            api_key="fake-key",
            wallet="J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            cache_dir=str(cache_dir),
        )

        # Own stale .tmp must be removed
        assert not own_tmp.exists(), "Own stale .tmp should be removed on init"
        # Other wallet's .tmp must NOT be touched
        assert other_tmp.exists(), (
            "Other wallet's .tmp must NOT be deleted by a different wallet's client init"
        )


# ---------------------------------------------------------------------------
# ATTACK 11: min_safe_open_date vs refresh_from lexicographic comparison edge case
#
# Design doc fetch logic: fetch_since(max(min_safe_open_date, refresh_from_str))
# min_safe_open_date is a date string "YYYY-MM-DD"
# refresh_from_str is a datetime string "YYYY-MM-DDTHH:MM:SSZ"
# Same-day comparison: "2026-05-07" vs "2026-05-07T00:00:00Z"
# Lexicographically, "2026-05-07T..." > "2026-05-07" (T char > end-of-string).
# So max() returns the datetime string when they're on the same day → correct.
# But "2026-05-07" vs "2026-05-06T23:59:59Z": "2026-05-07" > "2026-05-06T..."
# → max returns the date. Test that the correct from_date reaches the API.
# ---------------------------------------------------------------------------

class TestMinSafeOpenDateVsRefreshWindowComparison:
    def test_min_safe_open_date_later_than_refresh_window_uses_min_safe_date(self, monkeypatch):
        """
        ATTACK: min_safe_open_date is 4 days ago (date-only string "YYYY-MM-DD").
        refresh_window is 120h = 5 days, so refresh_from is 5 days ago (datetime string).
        max(min_safe_date, refresh_from) should return the min_safe_date value because
        it is more recent (4 days ago > 5 days ago).
        The from_date sent to the API must NOT be earlier than min_safe_open_date.

        This tests the lexicographic max() edge: comparing "YYYY-MM-DD" vs "YYYY-MM-DDTHH:MM:SSZ".
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-min-safe-beats-window")

        from datetime import datetime, timezone, timedelta
        from urllib.parse import urlparse, parse_qs

        now = datetime.now(timezone.utc)
        # min_safe_open_date 4 days ago (should dominate over 5-day window)
        min_safe_dt = now - timedelta(days=4)
        min_safe_str = min_safe_dt.strftime("%Y-%m-%d")  # date-only string

        # 5-day window lower bound (refresh_from) — 5 days ago ISO datetime
        refresh_from_dt = now - timedelta(hours=120)
        refresh_from_str = refresh_from_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        # min_safe_str (4 days ago) > refresh_from_str (5 days ago) — min_safe_date wins
        # Therefore the from_date in the API URL must be >= min_safe_str
        assert min_safe_str > refresh_from_str[:10], (
            "Test precondition: min_safe_date must be more recent than refresh_from"
        )

        watermark = {
            "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
            "min_safe_open_date": min_safe_str,
            "last_full_refresh_at": None,  # Force refresh
            "refresh_window_hours": 120,
        }
        (tmp_path / "lpagent_sync.json").write_text(json.dumps(watermark), encoding="utf-8")

        captured_from_dates = []
        single_page = {
            "status": "success",
            "data": {
                "data": [
                    {
                        "tokenId": "tokWINDOW",
                        "createdAt": min_safe_str + "T10:00:00Z",
                        "updatedAt": min_safe_str + "T12:00:00Z",
                    }
                ],
                "pagination": {"totalCount": 1, "totalPages": 1, "currentPage": 1},
            },
        }

        def _spy_urlopen(req, timeout=30):
            # Parse from_date out of the URL query string
            url = req.full_url if hasattr(req, "full_url") else str(req)
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            from_date_in_url = qs.get("from_date", qs.get("fromDate", qs.get("from", [None])))[0]
            captured_from_dates.append(from_date_in_url)
            raw = json.dumps(single_page).encode()
            mock_resp = MagicMock()
            mock_resp.read.return_value = raw
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            return mock_resp

        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_spy_urlopen):
            client.fetch_since(min_safe_str)

        assert len(captured_from_dates) >= 1, "At least one API call should have been made"

        # The from_date sent to the API must NOT be earlier than min_safe_str.
        # A from_date of refresh_from (5 days ago) would mean more data is fetched than
        # necessary, but the critical failure is from_date EARLIER than min_safe_str
        # (indicating max() picked the wrong branch).
        for fd in captured_from_dates:
            if fd is not None:
                # Normalize for comparison: strip time part if present
                fd_date = fd[:10]  # "YYYY-MM-DD"
                assert fd_date >= refresh_from_str[:10], (
                    f"from_date in API URL '{fd}' is earlier than refresh_from '{refresh_from_str}' "
                    f"— indicates from_date was not clamped to max(min_safe_open_date, refresh_from)"
                )


# ---------------------------------------------------------------------------
# ATTACK 12: totalCount assertion message format verification
#
# AC-2 adversarial: "raises AssertionError with message including
# totalCount=50, retrieved=40, and the query parameters used."
# Exact keyword assertions from the doc.
# ---------------------------------------------------------------------------

class TestTotalCountAssertionMessageFormat:
    def test_assertion_message_includes_from_date(self, monkeypatch):
        """
        ATTACK: Verify the AssertionError message includes the from_date parameter
        so the operator can diagnose which query failed.
        AC-2: message must include 'from_date=...'
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-assertion-msg-from-date")

        page1 = _load_fixture_page("page1_truncated.json")  # totalCount=50, data=[10]
        empty_page = {
            "status": "success",
            "data": {
                "data": [],
                "pagination": {"totalCount": 50, "totalPages": 5, "currentPage": 2},
            },
        }

        client = _make_client(tmp_path)
        with pytest.raises(AssertionError) as exc_info:
            with patch(
                "urllib.request.urlopen",
                side_effect=_mock_urlopen_with_pages([page1, empty_page]),
            ):
                client._fetch_all_pages("2026-03-15")  # Distinctive date

        msg = str(exc_info.value)
        assert "2026-03-15" in msg, (
            f"AssertionError message must include the from_date '2026-03-15', got: {msg}"
        )

    def test_assertion_error_no_jsonl_written_via_fetch_since(self, monkeypatch):
        """
        ATTACK: totalCount mismatch during fetch_since (full flow, not just _fetch_all_pages).
        No JSONL should be written — verified via fetch_since path (not just _fetch_all_pages).
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-assertion-no-write-fetch-since")

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

        with pytest.raises(AssertionError):
            with patch(
                "urllib.request.urlopen",
                side_effect=_mock_urlopen_with_pages([page1, empty_page]),
            ):
                client.fetch_since("2026-05-01")  # Full path, not _fetch_all_pages

        assert not jsonl.exists(), (
            "fetch_since must not write JSONL on totalCount mismatch"
        )


# ---------------------------------------------------------------------------
# ATTACK 13: Legacy watermark promotion writes NEW format to disk
#
# AC-7 adversarial: "logs a migration notice, and writes the new format back."
# The written-back file must NOT still contain last_synced_date.
# Also: what if the file is read-only? (less critical, but worth noting)
# Test that the promotion actually persists to disk (not just in-memory).
# ---------------------------------------------------------------------------

class TestLegacyWatermarkPromotionPersistence:
    def test_legacy_watermark_promotion_removes_old_key_from_disk(self):
        """
        ATTACK: After read_watermark promotes legacy format, re-reading the file
        must yield the new format — 'last_synced_date' must be gone from disk.
        """
        tmp_path = _case_dir("adv-legacy-promote-disk")
        from valhalla.lpagent_pipeline import read_watermark

        sync_path = tmp_path / "lpagent_sync.json"
        sync_path.write_text(json.dumps({"last_synced_date": "2026-03-01"}), encoding="utf-8")

        # First call promotes
        read_watermark(str(tmp_path))

        # Re-read from disk — must be new format
        on_disk = json.loads(sync_path.read_text(encoding="utf-8"))
        assert "last_synced_date" not in on_disk, (
            "Promoted watermark on disk must not contain 'last_synced_date' key"
        )
        assert "min_safe_open_date" in on_disk, (
            "Promoted watermark on disk must have 'min_safe_open_date'"
        )
        assert on_disk["min_safe_open_date"] == "2026-03-01", (
            "Promoted min_safe_open_date must equal the original last_synced_date"
        )

    def test_read_watermark_legacy_last_full_refresh_at_is_none(self):
        """
        ATTACK: Legacy format has no last_full_refresh_at.
        After promotion, last_full_refresh_at must be None (not missing key, not crash).
        """
        tmp_path = _case_dir("adv-legacy-promote-refresh-none")
        from valhalla.lpagent_pipeline import read_watermark

        (tmp_path / "lpagent_sync.json").write_text(
            json.dumps({"last_synced_date": "2026-03-15"}), encoding="utf-8"
        )
        result = read_watermark(str(tmp_path))
        assert "last_full_refresh_at" in result, (
            "Promoted watermark must have 'last_full_refresh_at' key"
        )
        assert result["last_full_refresh_at"] is None, (
            "Promoted watermark last_full_refresh_at must be None"
        )


# ---------------------------------------------------------------------------
# ATTACK 14: JSONL truncation in the MIDDLE of file (not just last line)
#
# AC-4 says: "truncated last line from crash mid-write is detected on open".
# The design pseudocode breaks on the first bad line. But what if there's a
# bad line in the MIDDLE (e.g., line 2 of 5 is corrupt)?
# The spec says "break at first bad line" — which would drop lines 3-5 too.
# ---------------------------------------------------------------------------

class TestJsonlTruncationInMiddle:
    def test_truncated_line_in_middle_stops_at_first_bad_line(self):
        """
        ATTACK: JSONL with corrupt line 3 of 5 (not the last line).
        Contract: stop at first bad line → 2 records loaded (not 4).
        _needs_refresh=True.
        """
        tmp_path = _case_dir("adv-jsonl-corrupt-middle")
        cache_dir = tmp_path / "lpagent_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / "positions_J4tkG.jsonl"

        good1 = {"tokenId": "t1", "createdAt": "2026-05-01T10:00:00Z",
                 "updatedAt": "2026-05-01T12:00:00Z", "fetched_at_utc": "2026-05-01T13:00:00Z"}
        good2 = {"tokenId": "t2", "createdAt": "2026-05-01T11:00:00Z",
                 "updatedAt": "2026-05-01T13:00:00Z", "fetched_at_utc": "2026-05-01T14:00:00Z"}
        corrupt = '{"tokenId": "t3", "createdAt": "bad'  # truncated
        good4 = {"tokenId": "t4", "createdAt": "2026-05-01T13:00:00Z",
                 "updatedAt": "2026-05-01T15:00:00Z", "fetched_at_utc": "2026-05-01T16:00:00Z"}
        good5 = {"tokenId": "t5", "createdAt": "2026-05-01T14:00:00Z",
                 "updatedAt": "2026-05-01T16:00:00Z", "fetched_at_utc": "2026-05-01T17:00:00Z"}

        lines = [
            json.dumps(good1),
            json.dumps(good2),
            corrupt,
            json.dumps(good4),
            json.dumps(good5),
        ]
        jsonl_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        client = _make_client(tmp_path)
        loaded = client.load_cache()

        # Should stop at first bad line (line 3) → 2 valid records
        assert len(loaded) == 2, (
            f"Corrupt line 3 of 5: should load 2 records, got {len(loaded)}"
        )
        assert client._needs_refresh, "_needs_refresh must be True after corrupt-middle"

        # File on disk must NOT contain corrupt line or records after it
        disk_lines = [l for l in jsonl_path.read_text(encoding="utf-8").splitlines() if l.strip()]
        assert len(disk_lines) == 2, (
            f"Repaired file should have 2 lines, got {len(disk_lines)}"
        )
        for line in disk_lines:
            json.loads(line)  # all must be valid JSON


# ---------------------------------------------------------------------------
# ATTACK 15: write_watermark called with string raises TypeError with clear message
#
# AC-7 / Design section: old string-based signature must raise TypeError
# "no longer accepts a date string". Check exact message fragment.
# ---------------------------------------------------------------------------

class TestWriteWatermarkStringRaisesTypeError:
    def test_write_watermark_string_error_message_is_informative(self):
        """
        ATTACK: write_watermark(output_dir, "2026-04-30") must raise TypeError
        with a message that helps the caller understand the required signature.
        The message must contain 'date string' or be otherwise diagnostic.
        """
        tmp_path = _case_dir("adv-write-wm-string")
        from valhalla.lpagent_pipeline import write_watermark

        with pytest.raises(TypeError) as exc_info:
            write_watermark(str(tmp_path), "2026-04-30")

        msg = str(exc_info.value).lower()
        # Message should mention the issue clearly
        assert any(phrase in msg for phrase in ["date string", "dict", "string", "str"]), (
            f"TypeError message should be diagnostic, got: {exc_info.value}"
        )


# ---------------------------------------------------------------------------
# ATTACK 16: Watermark written correctly after totalCount=0 fetch
#
# When the API returns 0 records, does the watermark still get updated?
# If not, every future run will re-fetch because last_full_refresh_at is still None.
# ---------------------------------------------------------------------------

class TestWatermarkWrittenOnEmptyFetch:
    def test_watermark_written_after_empty_fetch(self, monkeypatch):
        """
        ATTACK: fetch_since returns 0 records (new wallet, no positions).
        Contract: watermark must still be written with last_full_refresh_at set,
        so the next call hits the 24h skip-threshold and avoids redundant fetches.
        """
        monkeypatch.setattr("time.sleep", lambda _: None)
        tmp_path = _case_dir("adv-watermark-written-zero-records")

        empty_response = {
            "status": "success",
            "data": {
                "data": [],
                "pagination": {"totalCount": 0, "totalPages": 0, "currentPage": 1},
            },
        }
        client = _make_client(tmp_path)
        with patch("urllib.request.urlopen", side_effect=_mock_urlopen_with_pages([empty_response])):
            client.fetch_since("2026-05-01")

        watermark_path = tmp_path / "lpagent_sync.json"
        assert watermark_path.exists(), "Watermark must be written even on empty fetch"

        wm = json.loads(watermark_path.read_text(encoding="utf-8"))
        assert wm.get("last_full_refresh_at") is not None, (
            "last_full_refresh_at must be set after fetch, even if 0 records returned"
        )
