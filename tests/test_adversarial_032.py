from __future__ import annotations

import io
import urllib.error
from decimal import Decimal

import pytest

from valhalla import internal_nav


def _http_error(code: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://api.jup.ag/swap/v1/quote",
        code=code,
        msg="error",
        hdrs={},
        fp=io.BytesIO(body),
    )


def test_429_with_no_route_body_is_still_transient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_skip_cache", set())
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(
            _http_error(429, b'{"error":"NO_ROUTES_FOUND"}')
        ),
    )

    with pytest.raises(internal_nav.TransientPricingError):
        internal_nav._value_mint_amount(
            "A" * 32,
            Decimal("1000"),
            [],
            [],
            suppress_immaterial_warning=False,
        )


def test_lbpair_rpc_transient_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(TimeoutError("meteora unavailable")),
    )
    monkeypatch.setattr(
        internal_nav,
        "_rpc_call",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            internal_nav.TransientPricingError("RPC getAccountInfo failed")
        ),
    )

    with pytest.raises(internal_nav.TransientPricingError):
        internal_nav._get_pool_mints("RPC", "PAIR")


def test_direct_price_cache_must_not_cross_nav_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_skip_cache", set())
    calls: list[str] = []

    def rpc_call(_rpc: str, method: str, _params: object) -> dict[str, object]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "mint": "B" * 32,
                                        "tokenAmount": {"amount": "1000"},
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        raise AssertionError(method)

    def http_get(url: str) -> dict[str, str]:
        calls.append(url)
        if len(calls) == 1:
            return {"outAmount": "1000"}
        return {"outAmount": "2000"}

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [])
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)
    monkeypatch.setattr(internal_nav, "_http_get", http_get)

    first = internal_nav.compute_nav("RPC", "WALLET")
    second = internal_nav.compute_nav("RPC", "WALLET")

    assert first.idle_spl_sol == Decimal("0.000001")
    assert second.idle_spl_sol == Decimal("0.000002")
