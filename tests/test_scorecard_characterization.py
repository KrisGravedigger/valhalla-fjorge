"""
Characterization net for WalletScorecardAnalyzer.analyze() (S8, refactor phase 8).

Freezes the COMPLETE WalletScorecard output (every field via dataclasses.asdict)
for synthetic, self-contained scenarios. Authored by Claude Code as the verification
contract BEFORE the in-file extraction of analyze()'s per-wallet computation, so the
extraction implementer cannot author its own grader.

WHY this net exists (S8 != S7): the byte-exact loss_analysis.md baseline only renders
~10 of WalletScorecard's 29 fields in Section 4. Many fields (win_rate_24h/72h_pct,
capital_efficiency, consistency_score, median_pnl_sol, the pnl_*d / rug_rate_*d windows,
current_exposure_sol) are NOT in loss_analysis.md AND some feed only wallet_trend.md,
which is EXCLUDED from the baseline diff. They are dark to the gate. A mis-wired variable
feeding such a field would pass check.ps1 green. This net closes that gap.

Fixtures are 100% synthetic (no dependency on the gitignored _baseline_pre_refactor/),
so the committed test is self-contained on a fresh checkout.

EXPECTED was generated against the PRE-extraction code via:
    python tests/test_scorecard_characterization.py
Regenerating it AFTER the extraction would be a tautology (forbidden). If a scenario is
added, regenerate EXPECTED against known-good behavior only.

Scenario coverage (sized empirically against real baseline data):
  - All 5 status branches: inactive, insufficient_data, increase_capital,
    consider_replacing (both the pnl_7d<0 and wr_7d<threshold sub-branches), monitor.
  - Edge paths: avg_hold None (missing open), capital_efficiency None (no deployed),
    median odd vs even count, current_exposure_sol != 0 (still_open with deployed),
    zero closed positions (days_since_last None), rug-rate windows populated,
    win_rate / rug_rate windows None (no recent closes).
"""

from dataclasses import asdict
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from valhalla.loss_analyzer import WalletScorecardAnalyzer
from valhalla.models import MatchedPosition

# Fixed reference point so every recency window is deterministic (no utcnow()).
REF = datetime(2026, 2, 15, 12, 0, 0)


def _dt(days_ago):
    """ISO string for a timestamp `days_ago` before REF."""
    return (REF - timedelta(days=days_ago)).isoformat()


def _pos(
    wallet,
    pnl_sol,
    close_reason,
    close_days_ago=None,
    open_days_ago=None,
    sol_deployed="1.0",
    token="TKN",
):
    return MatchedPosition(
        target_wallet=wallet,
        token=token,
        position_type="long",
        sol_deployed=(Decimal(sol_deployed) if sol_deployed is not None else None),
        sol_received=None,
        pnl_sol=(Decimal(pnl_sol) if pnl_sol is not None else None),
        pnl_pct=None,
        close_reason=close_reason,
        mc_at_open=0.0,
        jup_score=0,
        token_age="",
        datetime_open=("" if open_days_ago is None else _dt(open_days_ago)),
        datetime_close=("" if close_days_ago is None else _dt(close_days_ago)),
    )


def _wallet_with_winrate(wallet, n, win_count, win_pnl, loss_pnl, rug_count=0):
    """
    Build `n` closed positions for `wallet`, spread across recency windows so the
    24h/72h/7d window metrics are populated. `win_count` take_profit wins, `rug_count`
    rugs (counted as losses), remainder stop_loss losses.
    Spread: close_days_ago cycles through 0.5, 1.5, 2.5, 4.0, 6.0 (all within 7d).
    """
    spread = [0.5, 1.5, 2.5, 4.0, 6.0]
    out = []
    for i in range(n):
        cda = spread[i % len(spread)]
        if i < win_count:
            out.append(_pos(wallet, win_pnl, "take_profit", cda, cda + 0.01))
        elif i < win_count + rug_count:
            out.append(_pos(wallet, loss_pnl, "rug", cda, cda + 0.01))
        else:
            out.append(_pos(wallet, loss_pnl, "stop_loss", cda, cda + 0.01))
    return out


def _scenarios():
    """name -> (positions, reference_date)."""
    s = {}

    # 1. inactive: last close 10 days ago (>= INACTIVE_DAYS=3). Also exercises
    #    win_rate_7d/rug_rate_7d None (no recent closes) and odd median (5 values).
    s["inactive"] = (
        [_pos("W_inact", "0.5", "take_profit", 10 + i, 10 + i + 0.01) for i in range(5)],
        REF,
    )

    # 2. insufficient_data: < MIN_POSITIONS (30) closed, recent. Even count (6) -> median averages.
    s["insufficient_recent"] = (
        _wallet_with_winrate("W_insuf", 6, 4, "0.5", "-0.2"),
        REF,
    )

    # 3. increase_capital: 30 closed recent, 70% win, 0 rug, pnl_7d > 0.
    s["increase_capital"] = (
        _wallet_with_winrate("W_inc", 30, 21, "0.5", "-0.2"),
        REF,
    )

    # 4. consider_replacing via low 7d win rate: 30 closed recent, 30% win (wr_7d < 45).
    s["replacing_low_wr"] = (
        _wallet_with_winrate("W_repl_wr", 30, 9, "0.5", "-0.2"),
        REF,
    )

    # 5. consider_replacing via negative pnl: 30 closed recent, 50% win (wr_7d >= 45)
    #    but loss size dominates so pnl_7d < 0.
    s["replacing_neg_pnl"] = (
        _wallet_with_winrate("W_repl_pnl", 30, 15, "0.1", "-0.9"),
        REF,
    )

    # 6. monitor: 30 closed recent, ~53% win (>=45, <60), small positive pnl, low rug.
    s["monitor"] = (
        _wallet_with_winrate("W_mon", 30, 16, "0.5", "-0.2"),
        REF,
    )

    # 7. edge: no sol_deployed -> capital_efficiency None; odd count (5, distinct pnl)
    #    -> odd median picks the middle element.
    s["edge_no_deployed"] = (
        [
            _pos("W_nodep", pnl, "take_profit", 1.0 + i, 1.0 + i + 0.01, sol_deployed=None)
            for i, pnl in enumerate(["0.5", "0.1", "0.9", "0.3", "0.7"])
        ],
        REF,
    )

    # 8. edge: missing datetime_open -> avg_hold None; even count (4, distinct pnl)
    #    -> even median averages the two middle elements.
    s["edge_no_hold"] = (
        [
            _pos("W_nohold", pnl, "take_profit", 1.0 + i, None)
            for i, pnl in enumerate(["0.1", "0.2", "0.4", "0.8"])
        ],
        REF,
    )

    # 9. edge: only still_open positions with deployed -> current_exposure_sol != 0,
    #    closed_positions == 0, days_since_last_position None.
    s["edge_only_open"] = (
        [_pos("W_open", None, "still_open", None, 0.5 + i, sol_deployed="2.0") for i in range(3)],
        REF,
    )

    # 10. edge: rugs present -> rug_rate_pct and rug_rate_*d windows populated.
    s["edge_rugs"] = (
        _wallet_with_winrate("W_rug", 10, 5, "0.5", "-0.3", rug_count=3),
        REF,
    )

    # 11. multi-wallet: exercises the pnl_per_day_sol descending sort across wallets
    #     plus mixed still_open + closed in one analyze() call.
    multi = []
    multi += _wallet_with_winrate("W_multi_a", 30, 24, "0.6", "-0.1")
    multi += _wallet_with_winrate("W_multi_b", 30, 6, "0.2", "-0.5")
    multi += [_pos("W_multi_a", None, "still_open", None, 0.2, sol_deployed="3.0")]
    s["multi_wallet_sort"] = (multi, REF)

    return s


def _run(positions, reference_date):
    cards = WalletScorecardAnalyzer().analyze(positions, reference_date)
    return [asdict(c) for c in cards]


@pytest.mark.parametrize("name", sorted(_scenarios().keys()))
def test_scorecard_characterization(name):
    positions, ref = _scenarios()[name]
    result = _run(positions, ref)
    assert result == EXPECTED[name], f"scorecard output drift in scenario '{name}'"


def test_every_scenario_has_golden():
    """Guard: a scenario added without regenerating EXPECTED fails loudly."""
    assert sorted(_scenarios().keys()) == sorted(EXPECTED.keys())


# ---------------------------------------------------------------------------
# GOLDEN (generated against pre-extraction code — do NOT regenerate post-extract)
# ---------------------------------------------------------------------------

EXPECTED = {'inactive': [{'wallet': 'W_inact',
               'total_positions': 5,
               'closed_positions': 5,
               'win_rate_pct': 100.0,
               'win_rate_7d_pct': None,
               'win_rate_24h_pct': None,
               'win_rate_72h_pct': None,
               'total_pnl_sol': Decimal('2.5'),
               'pnl_7d_sol': Decimal('0'),
               'pnl_per_day_sol': Decimal('0'),
               'rug_rate_pct': 0.0,
               'avg_hold_minutes': 14.4,
               'capital_efficiency': 0.5,
               'consistency_score': None,
               'win_rate_trend_pp': None,
               'status': 'inactive',
               'days_since_last_position': 10,
               'positions_7d': 0,
               'positions_3d': 0,
               'positions_1d': 0,
               'opened_1d': 0,
               'opened_7d': 0,
               'pnl_3d_sol': Decimal('0'),
               'pnl_1d_sol': Decimal('0'),
               'rug_rate_7d_pct': None,
               'rug_rate_3d_pct': None,
               'rug_rate_1d_pct': None,
               'median_pnl_sol': Decimal('0.5'),
               'current_exposure_sol': Decimal('0')}],
 'insufficient_recent': [{'wallet': 'W_insuf',
                          'total_positions': 6,
                          'closed_positions': 6,
                          'win_rate_pct': 66.66666666666666,
                          'win_rate_7d_pct': 66.66666666666666,
                          'win_rate_24h_pct': 50.0,
                          'win_rate_72h_pct': 75.0,
                          'total_pnl_sol': Decimal('1.6'),
                          'pnl_7d_sol': Decimal('1.6'),
                          'pnl_per_day_sol': Decimal('0.2285714285714285714285714286'),
                          'rug_rate_pct': 0.0,
                          'avg_hold_minutes': 14.4,
                          'capital_efficiency': 0.26666666666666666,
                          'consistency_score': 16.666666666666657,
                          'win_rate_trend_pp': 0.0,
                          'status': 'insufficient_data',
                          'days_since_last_position': 0,
                          'positions_7d': 6,
                          'positions_3d': 4,
                          'positions_1d': 2,
                          'opened_1d': 2,
                          'opened_7d': 6,
                          'pnl_3d_sol': Decimal('1.3'),
                          'pnl_1d_sol': Decimal('0.3'),
                          'rug_rate_7d_pct': 0.0,
                          'rug_rate_3d_pct': 0.0,
                          'rug_rate_1d_pct': 0.0,
                          'median_pnl_sol': Decimal('0.5'),
                          'current_exposure_sol': Decimal('0')}],
 'increase_capital': [{'wallet': 'W_inc',
                       'total_positions': 30,
                       'closed_positions': 30,
                       'win_rate_pct': 70.0,
                       'win_rate_7d_pct': 70.0,
                       'win_rate_24h_pct': 83.33333333333334,
                       'win_rate_72h_pct': 72.22222222222221,
                       'total_pnl_sol': Decimal('8.7'),
                       'pnl_7d_sol': Decimal('8.7'),
                       'pnl_per_day_sol': Decimal('1.242857142857142857142857143'),
                       'rug_rate_pct': 0.0,
                       'avg_hold_minutes': 14.4,
                       'capital_efficiency': 0.29,
                       'consistency_score': 13.333333333333343,
                       'win_rate_trend_pp': 0.0,
                       'status': 'increase_capital',
                       'days_since_last_position': 0,
                       'positions_7d': 30,
                       'positions_3d': 18,
                       'positions_1d': 6,
                       'opened_1d': 6,
                       'opened_7d': 30,
                       'pnl_3d_sol': Decimal('5.5'),
                       'pnl_1d_sol': Decimal('2.3'),
                       'rug_rate_7d_pct': 0.0,
                       'rug_rate_3d_pct': 0.0,
                       'rug_rate_1d_pct': 0.0,
                       'median_pnl_sol': Decimal('0.5'),
                       'current_exposure_sol': Decimal('0')}],
 'replacing_low_wr': [{'wallet': 'W_repl_wr',
                       'total_positions': 30,
                       'closed_positions': 30,
                       'win_rate_pct': 30.0,
                       'win_rate_7d_pct': 30.0,
                       'win_rate_24h_pct': 33.33333333333333,
                       'win_rate_72h_pct': 33.33333333333333,
                       'total_pnl_sol': Decimal('0.3'),
                       'pnl_7d_sol': Decimal('0.3'),
                       'pnl_per_day_sol': Decimal('0.04285714285714285714285714286'),
                       'rug_rate_pct': 0.0,
                       'avg_hold_minutes': 14.4,
                       'capital_efficiency': 0.01,
                       'consistency_score': 3.3333333333333286,
                       'win_rate_trend_pp': 0.0,
                       'status': 'consider_replacing',
                       'days_since_last_position': 0,
                       'positions_7d': 30,
                       'positions_3d': 18,
                       'positions_1d': 6,
                       'opened_1d': 6,
                       'opened_7d': 30,
                       'pnl_3d_sol': Decimal('0.6'),
                       'pnl_1d_sol': Decimal('0.2'),
                       'rug_rate_7d_pct': 0.0,
                       'rug_rate_3d_pct': 0.0,
                       'rug_rate_1d_pct': 0.0,
                       'median_pnl_sol': Decimal('-0.2'),
                       'current_exposure_sol': Decimal('0')}],
 'replacing_neg_pnl': [{'wallet': 'W_repl_pnl',
                        'total_positions': 30,
                        'closed_positions': 30,
                        'win_rate_pct': 50.0,
                        'win_rate_7d_pct': 50.0,
                        'win_rate_24h_pct': 50.0,
                        'win_rate_72h_pct': 50.0,
                        'total_pnl_sol': Decimal('-12.0'),
                        'pnl_7d_sol': Decimal('-12.0'),
                        'pnl_per_day_sol': Decimal('-1.714285714285714285714285714'),
                        'rug_rate_pct': 0.0,
                        'avg_hold_minutes': 14.4,
                        'capital_efficiency': -0.4,
                        'consistency_score': 0.0,
                        'win_rate_trend_pp': 0.0,
                        'status': 'consider_replacing',
                        'days_since_last_position': 0,
                        'positions_7d': 30,
                        'positions_3d': 18,
                        'positions_1d': 6,
                        'opened_1d': 6,
                        'opened_7d': 30,
                        'pnl_3d_sol': Decimal('-7.2'),
                        'pnl_1d_sol': Decimal('-2.4'),
                        'rug_rate_7d_pct': 0.0,
                        'rug_rate_3d_pct': 0.0,
                        'rug_rate_1d_pct': 0.0,
                        'median_pnl_sol': Decimal('-0.4'),
                        'current_exposure_sol': Decimal('0')}],
 'monitor': [{'wallet': 'W_mon',
              'total_positions': 30,
              'closed_positions': 30,
              'win_rate_pct': 53.333333333333336,
              'win_rate_7d_pct': 53.333333333333336,
              'win_rate_24h_pct': 66.66666666666666,
              'win_rate_72h_pct': 55.55555555555556,
              'total_pnl_sol': Decimal('5.2'),
              'pnl_7d_sol': Decimal('5.2'),
              'pnl_per_day_sol': Decimal('0.7428571428571428571428571429'),
              'rug_rate_pct': 0.0,
              'avg_hold_minutes': 14.4,
              'capital_efficiency': 0.17333333333333334,
              'consistency_score': 13.333333333333321,
              'win_rate_trend_pp': 0.0,
              'status': 'monitor',
              'days_since_last_position': 0,
              'positions_7d': 30,
              'positions_3d': 18,
              'positions_1d': 6,
              'opened_1d': 6,
              'opened_7d': 30,
              'pnl_3d_sol': Decimal('3.4'),
              'pnl_1d_sol': Decimal('1.6'),
              'rug_rate_7d_pct': 0.0,
              'rug_rate_3d_pct': 0.0,
              'rug_rate_1d_pct': 0.0,
              'median_pnl_sol': Decimal('0.5'),
              'current_exposure_sol': Decimal('0')}],
 'edge_no_deployed': [{'wallet': 'W_nodep',
                       'total_positions': 5,
                       'closed_positions': 5,
                       'win_rate_pct': 100.0,
                       'win_rate_7d_pct': 100.0,
                       'win_rate_24h_pct': 100.0,
                       'win_rate_72h_pct': 100.0,
                       'total_pnl_sol': Decimal('2.5'),
                       'pnl_7d_sol': Decimal('2.5'),
                       'pnl_per_day_sol': Decimal('0.3571428571428571428571428571'),
                       'rug_rate_pct': 0.0,
                       'avg_hold_minutes': 14.4,
                       'capital_efficiency': None,
                       'consistency_score': 0.0,
                       'win_rate_trend_pp': 0.0,
                       'status': 'insufficient_data',
                       'days_since_last_position': 1,
                       'positions_7d': 5,
                       'positions_3d': 3,
                       'positions_1d': 1,
                       'opened_1d': 0,
                       'opened_7d': 5,
                       'pnl_3d_sol': Decimal('1.5'),
                       'pnl_1d_sol': Decimal('0.5'),
                       'rug_rate_7d_pct': 0.0,
                       'rug_rate_3d_pct': 0.0,
                       'rug_rate_1d_pct': 0.0,
                       'median_pnl_sol': Decimal('0.5'),
                       'current_exposure_sol': Decimal('0')}],
 'edge_no_hold': [{'wallet': 'W_nohold',
                   'total_positions': 4,
                   'closed_positions': 4,
                   'win_rate_pct': 100.0,
                   'win_rate_7d_pct': 100.0,
                   'win_rate_24h_pct': 100.0,
                   'win_rate_72h_pct': 100.0,
                   'total_pnl_sol': Decimal('1.5'),
                   'pnl_7d_sol': Decimal('1.5'),
                   'pnl_per_day_sol': Decimal('0.2142857142857142857142857143'),
                   'rug_rate_pct': 0.0,
                   'avg_hold_minutes': None,
                   'capital_efficiency': 0.375,
                   'consistency_score': 0.0,
                   'win_rate_trend_pp': 0.0,
                   'status': 'insufficient_data',
                   'days_since_last_position': 1,
                   'positions_7d': 4,
                   'positions_3d': 3,
                   'positions_1d': 1,
                   'opened_1d': 0,
                   'opened_7d': 0,
                   'pnl_3d_sol': Decimal('0.7'),
                   'pnl_1d_sol': Decimal('0.1'),
                   'rug_rate_7d_pct': 0.0,
                   'rug_rate_3d_pct': 0.0,
                   'rug_rate_1d_pct': 0.0,
                   'median_pnl_sol': Decimal('0.3'),
                   'current_exposure_sol': Decimal('0')}],
 'edge_only_open': [{'wallet': 'W_open',
                     'total_positions': 3,
                     'closed_positions': 0,
                     'win_rate_pct': 0.0,
                     'win_rate_7d_pct': None,
                     'win_rate_24h_pct': None,
                     'win_rate_72h_pct': None,
                     'total_pnl_sol': Decimal('0'),
                     'pnl_7d_sol': Decimal('0'),
                     'pnl_per_day_sol': Decimal('0'),
                     'rug_rate_pct': 0.0,
                     'avg_hold_minutes': None,
                     'capital_efficiency': None,
                     'consistency_score': None,
                     'win_rate_trend_pp': None,
                     'status': 'insufficient_data',
                     'days_since_last_position': None,
                     'positions_7d': 0,
                     'positions_3d': 0,
                     'positions_1d': 0,
                     'opened_1d': 1,
                     'opened_7d': 3,
                     'pnl_3d_sol': Decimal('0'),
                     'pnl_1d_sol': Decimal('0'),
                     'rug_rate_7d_pct': None,
                     'rug_rate_3d_pct': None,
                     'rug_rate_1d_pct': None,
                     'median_pnl_sol': None,
                     'current_exposure_sol': Decimal('6.0')}],
 'edge_rugs': [{'wallet': 'W_rug',
                'total_positions': 10,
                'closed_positions': 10,
                'win_rate_pct': 50.0,
                'win_rate_7d_pct': 50.0,
                'win_rate_24h_pct': 50.0,
                'win_rate_72h_pct': 50.0,
                'total_pnl_sol': Decimal('1.0'),
                'pnl_7d_sol': Decimal('1.0'),
                'pnl_per_day_sol': Decimal('0.1428571428571428571428571429'),
                'rug_rate_pct': 30.0,
                'avg_hold_minutes': 14.4,
                'capital_efficiency': 0.1,
                'consistency_score': 0.0,
                'win_rate_trend_pp': 0.0,
                'status': 'insufficient_data',
                'days_since_last_position': 0,
                'positions_7d': 10,
                'positions_3d': 6,
                'positions_1d': 2,
                'opened_1d': 2,
                'opened_7d': 10,
                'pnl_3d_sol': Decimal('0.6'),
                'pnl_1d_sol': Decimal('0.2'),
                'rug_rate_7d_pct': 30.0,
                'rug_rate_3d_pct': 50.0,
                'rug_rate_1d_pct': 50.0,
                'median_pnl_sol': Decimal('0.1'),
                'current_exposure_sol': Decimal('0')}],
 'multi_wallet_sort': [{'wallet': 'W_multi_a',
                        'total_positions': 31,
                        'closed_positions': 30,
                        'win_rate_pct': 80.0,
                        'win_rate_7d_pct': 80.0,
                        'win_rate_24h_pct': 83.33333333333334,
                        'win_rate_72h_pct': 83.33333333333334,
                        'total_pnl_sol': Decimal('13.8'),
                        'pnl_7d_sol': Decimal('13.8'),
                        'pnl_per_day_sol': Decimal('1.971428571428571428571428571'),
                        'rug_rate_pct': 0.0,
                        'avg_hold_minutes': 14.4,
                        'capital_efficiency': 0.46,
                        'consistency_score': 3.333333333333343,
                        'win_rate_trend_pp': 0.0,
                        'status': 'increase_capital',
                        'days_since_last_position': 0,
                        'positions_7d': 30,
                        'positions_3d': 18,
                        'positions_1d': 6,
                        'opened_1d': 7,
                        'opened_7d': 31,
                        'pnl_3d_sol': Decimal('8.7'),
                        'pnl_1d_sol': Decimal('2.9'),
                        'rug_rate_7d_pct': 0.0,
                        'rug_rate_3d_pct': 0.0,
                        'rug_rate_1d_pct': 0.0,
                        'median_pnl_sol': Decimal('0.6'),
                        'current_exposure_sol': Decimal('3.0')},
                       {'wallet': 'W_multi_b',
                        'total_positions': 30,
                        'closed_positions': 30,
                        'win_rate_pct': 20.0,
                        'win_rate_7d_pct': 20.0,
                        'win_rate_24h_pct': 33.33333333333333,
                        'win_rate_72h_pct': 22.22222222222222,
                        'total_pnl_sol': Decimal('-10.8'),
                        'pnl_7d_sol': Decimal('-10.8'),
                        'pnl_per_day_sol': Decimal('-1.542857142857142857142857143'),
                        'rug_rate_pct': 0.0,
                        'avg_hold_minutes': 14.4,
                        'capital_efficiency': -0.36,
                        'consistency_score': 13.333333333333329,
                        'win_rate_trend_pp': 0.0,
                        'status': 'consider_replacing',
                        'days_since_last_position': 0,
                        'positions_7d': 30,
                        'positions_3d': 18,
                        'positions_1d': 6,
                        'opened_1d': 6,
                        'opened_7d': 30,
                        'pnl_3d_sol': Decimal('-6.2'),
                        'pnl_1d_sol': Decimal('-1.6'),
                        'rug_rate_7d_pct': 0.0,
                        'rug_rate_3d_pct': 0.0,
                        'rug_rate_1d_pct': 0.0,
                        'median_pnl_sol': Decimal('-0.5'),
                        'current_exposure_sol': Decimal('0')}]}


if __name__ == "__main__":
    import pprint

    out = {name: _run(pos, ref) for name, (pos, ref) in _scenarios().items()}
    print("EXPECTED = " + pprint.pformat(out, width=100, sort_dicts=False))
