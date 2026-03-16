"""
Chart generation module for Valhalla parser.
"""

from typing import List, Dict, Optional, Tuple
from decimal import Decimal
from datetime import date, timedelta
from collections import defaultdict

from .models import MatchedPosition, SkipEvent, parse_iso_datetime
from .analysis_config import SCORECARD_INACTIVE_DAYS, PORTFOLIO_TOTAL_SOL, PNL_BREAKDOWN_LOOKBACK_DAYS, FILTER_IMPACT_LOOKBACK_DAYS

# Optional matplotlib for chart generation
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import AutoMinorLocator
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def _short_wallet(wallet: str) -> str:
    """
    Extract short wallet label from target_wallet.

    Args:
        wallet: e.g. "20251126_6ypMuzoZ2"

    Returns:
        Short label: "6ypMuzoZ2" (last part after underscore, or last 9 chars)
    """
    if '_' in wallet:
        return wallet.split('_')[-1]
    return wallet[-9:] if len(wallet) > 9 else wallet


def _get_wallet_colors(wallets: List[str]) -> Dict[str, str]:
    """
    Assign consistent colors to wallets using matplotlib colormap.

    Args:
        wallets: List of wallet identifiers

    Returns:
        Dict mapping wallet -> color hex string
    """
    # Use tab10 colormap (10 distinct colors)
    colors = plt.cm.tab10.colors
    wallet_colors = {}

    for i, wallet in enumerate(wallets):
        # Cycle through colors if more wallets than palette size
        color_idx = i % len(colors)
        # Convert RGB tuple to hex
        rgb = colors[color_idx]
        hex_color = '#{:02x}{:02x}{:02x}'.format(
            int(rgb[0] * 255),
            int(rgb[1] * 255),
            int(rgb[2] * 255)
        )
        wallet_colors[wallet] = hex_color

    return wallet_colors


def _aggregate_daily_data(dated_positions: List[Tuple[MatchedPosition, date]]) -> Tuple[
    Dict[Tuple[str, date], float],  # pnl_data
    Dict[Tuple[str, date], int],    # entries_data
    Dict[Tuple[str, date], float],  # winrate_data
    Dict[Tuple[str, date], int],    # rugs_data
    Dict[Tuple[str, date], float],  # pnl_pct_data
    List[date],                      # sorted_dates
    List[str],                       # sorted_wallets
    Dict[Tuple[str, date], int],    # sl_data
]:
    """
    Aggregate position data by (wallet, date).

    Args:
        dated_positions: List of (MatchedPosition, date) tuples

    Returns:
        Tuple containing:
        - pnl_data: {(wallet, date): total_pnl_sol}
        - entries_data: {(wallet, date): count}
        - winrate_data: {(wallet, date): win_rate_pct}
        - rugs_data: {(wallet, date): rug_count}
        - pnl_pct_data: {(wallet, date): pnl_pct (ROI)}
        - sorted_dates: List of unique dates sorted
        - sorted_wallets: List of unique wallets sorted
        - sl_data: {(wallet, date): stop_loss_count}
    """
    # Group positions by (wallet, date)
    grouped = defaultdict(list)

    for pos, dt in dated_positions:
        key = (pos.target_wallet, dt)
        grouped[key].append(pos)

    # Aggregate metrics for each group
    pnl_data = {}
    entries_data = {}
    winrate_data = {}
    rugs_data = {}
    pnl_pct_data = {}
    sl_data = {}

    _RUG_REASONS = {"rug", "rug_unknown_open"}
    _SL_REASONS = {"stop_loss", "stop_loss_unknown_open"}

    for (wallet, dt), positions in grouped.items():
        # PnL sum
        total_pnl = sum(float(p.pnl_sol) for p in positions)
        pnl_data[(wallet, dt)] = total_pnl

        # PnL % (ROI) = total_pnl / total_deployed * 100
        total_deployed = sum(float(p.sol_deployed) for p in positions if p.sol_deployed is not None and p.sol_deployed > 0)
        if total_deployed > 0:
            pnl_pct_data[(wallet, dt)] = total_pnl / total_deployed * 100

        # Entries count
        entries_data[(wallet, dt)] = len(positions)

        # Win rate calculation
        # Win = pnl_sol > 0 AND close_reason not in ("rug", "rug_unknown_open", "unknown_open")
        wins = sum(
            1 for p in positions
            if p.pnl_sol > 0 and p.close_reason not in ("rug", "rug_unknown_open", "unknown_open")
        )
        total = len(positions)
        win_rate = (wins / total * 100) if total > 0 else 0.0
        winrate_data[(wallet, dt)] = win_rate

        # Rug count
        rug_count = sum(
            1 for p in positions
            if p.close_reason in _RUG_REASONS
        )
        rugs_data[(wallet, dt)] = rug_count

        # Stop-loss count
        sl_count = sum(
            1 for p in positions
            if p.close_reason in _SL_REASONS
        )
        sl_data[(wallet, dt)] = sl_count

    # Extract unique dates and wallets
    all_dates = sorted(set(dt for _, dt in grouped.keys()))
    all_wallets = sorted(set(wallet for wallet, _ in grouped.keys()))

    return pnl_data, entries_data, winrate_data, rugs_data, pnl_pct_data, all_dates, all_wallets, sl_data


def _fill_zeros_for_active_range(
    data_dict: Dict[Tuple[str, date], any],
    dates: List[date],
    wallets: List[str]
) -> None:
    """
    Fill 0 for active wallets on days with no trades.

    For each wallet that has any data, fills 0 for all dates from the wallet's
    first active date up to its fill boundary:
    - Active wallets (gap <= 7 days from global_last): fill to global_last so
      single-day wallets still show 0 on subsequent days.
    - Retired wallets (gap > 7 days from global_last): fill only to their last
      active date to avoid re-adding flat 0 lines after _apply_wallet_retirement
      has removed the post-retirement data.
    """
    if not dates:
        return

    global_last = max(dates)

    for wallet in wallets:
        wallet_dates = [d for (w, d) in data_dict if w == wallet]
        if not wallet_dates:
            continue
        first = min(wallet_dates)
        wallet_last = max(wallet_dates)
        # For retired wallets (gap > 7 days), stop filling at their last active date.
        # For active wallets, fill to global_last so single-day wallets still show zero.
        gap = (global_last - wallet_last).days
        fill_to = global_last if gap <= 7 else wallet_last
        for d in dates:
            if first <= d <= fill_to and (wallet, d) not in data_dict:
                data_dict[(wallet, d)] = 0


def _chart_daily_pnl(
    pnl_data: Dict[Tuple[str, date], float],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str
) -> None:
    """
    Generate daily PnL line chart.

    Each wallet is represented by a colored line.
    Includes mean and total portfolio lines.
    """
    fig, ax = plt.subplots(figsize=(12, 10))

    # Plot each wallet as a line
    for wallet in wallets:
        values = [pnl_data.get((wallet, d)) for d in dates]

        ax.plot(
            dates,
            values,
            marker='o',
            markersize=4,
            linewidth=2,
            linestyle='-',
            label=_short_wallet(wallet),
            color=wallet_colors[wallet]
        )

    # Mean line - compute mean only across wallets with data for each day
    means = []
    for d in dates:
        day_values = [pnl_data.get((w, d)) for w in wallets if (w, d) in pnl_data]
        if day_values:
            means.append(sum(day_values) / len(day_values))
        else:
            means.append(None)
    ax.plot(dates, means, 'k--', linewidth=1.5, label='Mean')

    # Total portfolio line - sum of all wallets per day
    totals = []
    for d in dates:
        day_values = [pnl_data.get((w, d)) for w in wallets if (w, d) in pnl_data]
        if day_values:
            totals.append(sum(day_values))
        else:
            totals.append(None)
    ax.plot(dates, totals, color='black', linewidth=2.5, linestyle='-', label='Total', alpha=0.7)

    # Formatting
    ax.set_title('Daily PnL per Wallet (SOL)', fontsize=14, fontweight='bold')
    ax.set_ylabel('SOL', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.axhline(y=0, color='black', linewidth=0.8, linestyle='-', alpha=0.3)
    # Improved Y-axis: more ticks and finer grid for better resolution near zero
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=20))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(True, alpha=0.3, axis='y', which='major')
    ax.grid(True, alpha=0.15, axis='y', which='minor')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_pnl.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_pnl.png")


def _chart_daily_pnl_pct(
    pnl_pct_data: Dict[Tuple[str, date], float],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str
) -> None:
    """
    Generate daily PnL % (ROI) line chart.

    Each wallet is represented by a colored line.
    Includes mean line. No total (aggregating percentages is meaningless).
    """
    fig, ax = plt.subplots(figsize=(12, 10))

    # Plot each wallet as a line
    for wallet in wallets:
        values = [pnl_pct_data.get((wallet, d)) for d in dates]

        ax.plot(
            dates,
            values,
            marker='o',
            markersize=4,
            linewidth=2,
            linestyle='-',
            label=_short_wallet(wallet),
            color=wallet_colors[wallet]
        )

    # Mean line
    means = []
    for d in dates:
        day_values = [pnl_pct_data.get((w, d)) for w in wallets if (w, d) in pnl_pct_data]
        if day_values:
            means.append(sum(day_values) / len(day_values))
        else:
            means.append(None)
    ax.plot(dates, means, 'k--', linewidth=1.5, label='Mean')

    # Formatting
    ax.set_title('Daily PnL % per Wallet (ROI)', fontsize=14, fontweight='bold')
    ax.set_ylabel('%', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.axhline(y=0, color='black', linewidth=0.8, linestyle='-', alpha=0.3)
    # Improved Y-axis: more ticks and finer grid for better resolution near zero
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=20))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(True, alpha=0.3, axis='y', which='major')
    ax.grid(True, alpha=0.15, axis='y', which='minor')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_pnl_pct.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_pnl_pct.png")


def _chart_daily_entries(
    entries_data: Dict[Tuple[str, date], int],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str
) -> None:
    """
    Generate daily positions opened line chart.

    Each wallet is represented by a colored line.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot each wallet as a line
    for wallet in wallets:
        values = [entries_data.get((wallet, d)) for d in dates]

        ax.plot(
            dates,
            values,
            marker='o',
            markersize=4,
            linewidth=2,
            linestyle='-',
            label=_short_wallet(wallet),
            color=wallet_colors[wallet]
        )

    # Mean line - compute mean only across wallets with data for each day
    means = []
    for d in dates:
        day_values = [entries_data.get((w, d)) for w in wallets if (w, d) in entries_data]
        if day_values:
            means.append(sum(day_values) / len(day_values))
        else:
            means.append(None)
    ax.plot(dates, means, 'k--', linewidth=1.5, label='Mean')

    # Formatting
    ax.set_title('Daily Positions Opened per Wallet', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_entries.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_entries.png")


def _chart_daily_winrate(
    winrate_data: Dict[Tuple[str, date], float],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str
) -> None:
    """
    Generate daily win rate line chart.

    Each wallet is represented by a colored line.
    Includes 50% threshold line.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot each wallet as a line
    for wallet in wallets:
        values = [winrate_data.get((wallet, d)) for d in dates]

        ax.plot(
            dates,
            values,
            marker='o',
            markersize=4,
            linewidth=2,
            linestyle='-',
            label=_short_wallet(wallet),
            color=wallet_colors[wallet]
        )

    # Mean line - compute mean only across wallets with data for each day
    means = []
    for d in dates:
        day_values = [winrate_data.get((w, d)) for w in wallets if (w, d) in winrate_data]
        if day_values:
            means.append(sum(day_values) / len(day_values))
        else:
            means.append(None)
    ax.plot(dates, means, 'k--', linewidth=1.5, label='Mean')

    # 50% threshold line
    ax.axhline(y=50, color='gray', linewidth=1, linestyle='--', alpha=0.6)

    # Formatting
    ax.set_title('Daily Win Rate per Wallet (%)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Win Rate (%)', fontsize=11)
    ax.set_ylim(0, 100)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_winrate.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_winrate.png")


def _chart_daily_losses(
    rugs_data: Dict[Tuple[str, date], int],
    sl_data: Dict[Tuple[str, date], int],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str
) -> None:
    """
    Generate daily rug + stop-loss count line chart.

    Each wallet is represented by a colored line.
    Only includes wallets with at least one rug or stop-loss in the last 7 days.
    """
    # Combine rugs and stop-losses per (wallet, date)
    total_losses_data: Dict[Tuple[str, date], int] = {}
    for key in set(list(rugs_data.keys()) + list(sl_data.keys())):
        total_losses_data[key] = rugs_data.get(key, 0) + sl_data.get(key, 0)

    # Filter wallets: only include those with any loss > 0 in the last 7 days
    cutoff_date = max(dates) - timedelta(days=7)
    active_wallets = []
    for wallet in wallets:
        has_recent_loss = any(
            total_losses_data.get((wallet, d), 0) > 0
            for d in dates
            if d >= cutoff_date
        )
        if has_recent_loss:
            active_wallets.append(wallet)

    # Skip chart generation if no recent losses
    if not active_wallets:
        print("  Skipping daily_rugs.png (no rugs or stop-losses in last 7 days)")
        return

    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot each active wallet as a line
    for wallet in active_wallets:
        values = [total_losses_data.get((wallet, d)) for d in dates]

        ax.plot(
            dates,
            values,
            marker='o',
            markersize=4,
            linewidth=2,
            linestyle='-',
            label=_short_wallet(wallet),
            color=wallet_colors[wallet]
        )

    # Formatting
    ax.set_title('Daily Rug + Stop-Loss Count per Wallet', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.yaxis.get_major_locator().set_params(integer=True)
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_rugs.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_rugs.png")


def _apply_wallet_retirement(
    data_dicts: List[Dict[Tuple[str, date], any]],
    dates: List[date],
    wallets: List[str],
    gap_days: int = 7
) -> set:
    """
    Apply wallet retirement filter: completely remove wallets that have been
    inactive for more than gap_days since the timeline end.

    Args:
        data_dicts: List of data dictionaries to modify in place
        dates: List of all dates in timeline
        wallets: List of all wallets
        gap_days: Minimum gap (in days) to trigger retirement
    Returns:
        Set of retired wallet names (to be filtered from wallets list by caller)
    """
    if not dates:
        return set()

    timeline_end = max(dates)

    # Find last active date for each wallet across all data dicts
    last_active = {}
    for wallet in wallets:
        wallet_dates = []
        for data_dict in data_dicts:
            for (w, d) in data_dict.keys():
                if w == wallet:
                    wallet_dates.append(d)
        if wallet_dates:
            last_active[wallet] = max(wallet_dates)

    # Retire wallets with gap > gap_days: remove ALL their data
    retired = set()
    for wallet, last_date in last_active.items():
        gap = (timeline_end - last_date).days
        if gap > gap_days:
            retired.add(wallet)
            for data_dict in data_dicts:
                keys_to_remove = [(w, d) for (w, d) in data_dict.keys() if w == wallet]
                for key in keys_to_remove:
                    del data_dict[key]

    return retired


def _chart_portfolio_cumulative(
    pnl_data: Dict[Tuple[str, date], float],
    dates: List[date],
    wallets: List[str],
    output_dir: str
) -> None:
    """
    Generate portfolio daily PnL bar chart with cumulative PnL line overlay.

    Bars are green for positive daily PnL and red for negative.
    An orange line shows running cumulative PnL.

    Args:
        pnl_data: {(wallet, date): pnl_sol}
        dates: Sorted list of dates
        wallets: List of active wallet identifiers
        output_dir: Directory to save the chart
    """
    from pathlib import Path

    if not dates:
        return

    # Compute daily totals across all wallets
    daily_totals = []
    for d in dates:
        total = sum(pnl_data[(w, d)] for w in wallets if (w, d) in pnl_data)
        daily_totals.append(total)

    # Compute running cumulative
    cumulative = []
    running = 0.0
    for v in daily_totals:
        running += v
        cumulative.append(running)

    # Build bar colors
    bar_colors = ['#26a69a' if v >= 0 else '#ef5350' for v in daily_totals]

    fig, ax = plt.subplots(figsize=(14, 7))
    fig.patch.set_facecolor('#1a1a1a')
    ax.set_facecolor('#1a1a1a')

    # Bar chart for daily PnL
    ax.bar(dates, daily_totals, color=bar_colors, width=0.8, alpha=0.85, zorder=2)

    # Cumulative line on the same axis
    ax.plot(
        dates,
        cumulative,
        color='#ff9800',
        linewidth=2,
        linestyle='-',
        label='Cumulative PnL',
        zorder=3
    )

    # Formatting
    ax.set_title('Portfolio Daily PnL & Cumulative', fontsize=14, fontweight='bold', color='white')
    ax.set_ylabel('SOL', fontsize=11, color='white')
    ax.tick_params(colors='white')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', color='white')
    ax.axhline(y=0, color='white', linewidth=0.6, linestyle='-', alpha=0.3)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, alpha=0.15, axis='y', color='white')
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=12))
    ax.legend(fontsize='small', facecolor='#2a2a2a', labelcolor='white', framealpha=0.8)

    fig.tight_layout()

    fig.savefig(Path(output_dir) / 'portfolio_cumulative.png', dpi=120)
    plt.close(fig)
    print("  Generated: portfolio_cumulative.png")


_RUG_REASONS = {"rug", "rug_unknown_open"}
_SL_REASONS = {"stop_loss", "stop_loss_unknown_open"}

# Loss categories: (name, color) in priority order (first match wins)
_LOSS_CATEGORIES = [
    ("Rug",             "#8B0000"),
    ("Stop-loss",       "#CC0000"),
    ("Large loss >1%",  "#FF3333"),
    ("Medium loss",     "#FF6666"),
    ("Small loss",      "#FF9999"),
]

# Gain categories: (name, color) in priority order (first match wins)
_GAIN_CATEGORIES = [
    ("Take-profit",     "#006400"),
    ("Large gain >1%",  "#00AA00"),
    ("Medium gain",     "#44CC44"),
    ("Small gain",      "#88EE88"),
]


def _aggregate_pnl_breakdown(
    dated_positions: List[Tuple[MatchedPosition, date]],
    portfolio_sol: float,
    lookback_days: int,
) -> Tuple[
    Dict[Tuple[str, date], Dict[str, float]],  # breakdown_data
    List[date],                                 # filtered dates
    List[str],                                  # active wallets
]:
    """
    Aggregate positions into per-wallet per-day PnL category buckets.

    Filters to the last `lookback_days` days (from max close date in data).
    Only wallets active (with any closing position) in that window are included.

    Loss categories (checked in order, first match wins):
      Rug, Stop-loss, Large loss >1% portfolio, Medium loss, Small loss

    Gain categories (checked in order, first match wins):
      Take-profit, Large gain >1% portfolio, Medium gain, Small gain

    Positions with pnl_sol == 0 are excluded.

    Args:
        dated_positions: List of (MatchedPosition, close_date) tuples
        portfolio_sol: Total portfolio in SOL (for percentage thresholds)
        lookback_days: Number of days to include (counting back from max date)

    Returns:
        Tuple of (breakdown_data, dates, wallets)
        breakdown_data: {(wallet, date): {category_name: sum_pnl_sol}}
    """
    if not dated_positions:
        return {}, [], []

    max_date = max(dt for _, dt in dated_positions)
    cutoff_date = max_date - timedelta(days=lookback_days - 1)

    # Fallback thresholds when portfolio_sol is 0
    if portfolio_sol > 0:
        large_threshold = portfolio_sol * 0.01
        medium_threshold = portfolio_sol * 0.001
    else:
        large_threshold = 0.5
        medium_threshold = 0.05

    breakdown_data: Dict[Tuple[str, date], Dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for pos, dt in dated_positions:
        if dt < cutoff_date:
            continue
        if pos.pnl_sol is None:
            continue

        pnl = float(pos.pnl_sol)
        if pnl == 0.0:
            continue

        key = (pos.target_wallet, dt)
        reason = pos.close_reason or ""

        if pnl < 0:
            # Classify loss
            abs_pnl = abs(pnl)
            if reason in _RUG_REASONS:
                category = "Rug"
            elif reason in _SL_REASONS:
                category = "Stop-loss"
            elif abs_pnl > large_threshold:
                category = "Large loss >1%"
            elif abs_pnl > medium_threshold:
                category = "Medium loss"
            else:
                category = "Small loss"
        else:
            # Classify gain
            if reason == "take_profit":
                category = "Take-profit"
            elif pnl > large_threshold:
                category = "Large gain >1%"
            elif pnl > medium_threshold:
                category = "Medium gain"
            else:
                category = "Small gain"

        breakdown_data[key][category] += pnl

    if not breakdown_data:
        return {}, [], []

    # Convert nested defaultdicts to plain dicts
    plain_breakdown: Dict[Tuple[str, date], Dict[str, float]] = {
        k: dict(v) for k, v in breakdown_data.items()
    }

    dates = sorted(set(dt for _, dt in plain_breakdown.keys()))
    wallets = sorted(set(w for w, _ in plain_breakdown.keys()))

    return plain_breakdown, dates, wallets


def _chart_daily_pnl_breakdown(
    breakdown_data: Dict[Tuple[str, date], Dict[str, float]],
    dates: List[date],
    wallets: List[str],
    output_dir: str,
) -> None:
    """
    Generate stacked bar chart of per-wallet per-day PnL breakdown.

    For each date, wallets are plotted side by side.
    Each wallet has two stacked bars: losses (downward, reds) and gains (upward, greens).
    Uses a dark background style.

    Args:
        breakdown_data: {(wallet, date): {category_name: sum_pnl_sol}}
        dates: Sorted list of dates to plot
        wallets: List of active wallets to plot
        output_dir: Directory to save the chart
    """
    from pathlib import Path

    if not dates or not wallets:
        return

    n_wallets = len(wallets)
    n_dates = len(dates)

    # Bar layout: each date occupies 1 unit; wallets are side by side within that unit
    bar_width = 0.8 / n_wallets

    # Map wallet -> index for positioning
    wallet_index = {w: i for i, w in enumerate(wallets)}

    # Category color lookups
    loss_colors = {name: color for name, color in _LOSS_CATEGORIES}
    gain_colors = {name: color for name, color in _GAIN_CATEGORIES}

    # Determine which categories actually have data (for legend)
    used_loss_cats = []
    used_gain_cats = []
    for cat_name, _ in _LOSS_CATEGORIES:
        if any(cat_name in breakdown_data.get((w, d), {}) for w in wallets for d in dates):
            used_loss_cats.append(cat_name)
    for cat_name, _ in _GAIN_CATEGORIES:
        if any(cat_name in breakdown_data.get((w, d), {}) for w in wallets for d in dates):
            used_gain_cats.append(cat_name)

    fig, ax = plt.subplots(figsize=(max(10, n_dates * n_wallets * 1.2 + 4), 8))
    fig.patch.set_facecolor('#1a1a1a')
    ax.set_facecolor('#1a1a1a')

    # Use numeric x positions: date index + wallet offset
    date_index = {d: i for i, d in enumerate(dates)}

    for wallet in wallets:
        w_idx = wallet_index[wallet]
        x_offset = (w_idx - (n_wallets - 1) / 2.0) * bar_width

        for dt in dates:
            d_idx = date_index[dt]
            x = d_idx + x_offset
            cats = breakdown_data.get((wallet, dt), {})

            # --- Loss stack (downward) ---
            bottom = 0.0
            for cat_name, _ in _LOSS_CATEGORIES:
                val = cats.get(cat_name, 0.0)
                if val < 0:
                    ax.bar(
                        x, val, width=bar_width * 0.9,
                        bottom=bottom,
                        color=loss_colors[cat_name],
                        align='center',
                        zorder=2,
                    )
                    bottom += val  # bottom moves further negative

            # --- Gain stack (upward) ---
            bottom = 0.0
            for cat_name, _ in _GAIN_CATEGORIES:
                val = cats.get(cat_name, 0.0)
                if val > 0:
                    ax.bar(
                        x, val, width=bar_width * 0.9,
                        bottom=bottom,
                        color=gain_colors[cat_name],
                        align='center',
                        zorder=2,
                    )
                    bottom += val

    # X-axis: date labels, one tick per date, wallet names as sub-labels
    ax.set_xticks(range(n_dates))
    date_labels = [d.strftime('%m-%d') for d in dates]
    ax.set_xticklabels(date_labels, color='white', fontsize=9)

    # Add wallet short names as minor tick labels below the date labels
    minor_ticks = []
    minor_labels = []
    for d_idx, dt in enumerate(dates):
        for w_idx, wallet in enumerate(wallets):
            x = d_idx + (w_idx - (n_wallets - 1) / 2.0) * bar_width
            minor_ticks.append(x)
            minor_labels.append(_short_wallet(wallet))

    ax.set_xticks(minor_ticks, minor=True)
    ax.set_xticklabels(minor_labels, minor=True, fontsize=6, color='#aaaaaa', rotation=45, ha='right')
    ax.tick_params(axis='x', which='minor', length=0, pad=12)

    # Formatting
    ax.set_title(
        'Daily PnL Breakdown per Wallet (SOL) — Last 3 Days',
        fontsize=13, fontweight='bold', color='white'
    )
    ax.set_ylabel('SOL', fontsize=11, color='white')
    ax.tick_params(colors='white')
    ax.axhline(y=0, color='white', linewidth=0.8, linestyle='-', alpha=0.4)
    ax.spines['bottom'].set_color('#555555')
    ax.spines['left'].set_color('#555555')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(True, alpha=0.15, axis='y', color='white')
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=12))

    # Padding on x-axis
    ax.set_xlim(-0.6, n_dates - 0.4)

    # Legend: categories (only those with data) + wallet colors via color patches
    from matplotlib.patches import Patch
    legend_entries = []

    for cat_name in used_loss_cats:
        legend_entries.append(Patch(facecolor=loss_colors[cat_name], label=cat_name))
    for cat_name in used_gain_cats:
        legend_entries.append(Patch(facecolor=gain_colors[cat_name], label=cat_name))

    if legend_entries:
        ax.legend(
            handles=legend_entries,
            loc='upper right',
            fontsize='small',
            facecolor='#2a2a2a',
            labelcolor='white',
            framealpha=0.85,
        )

    fig.tight_layout()

    fig.savefig(Path(output_dir) / 'daily_pnl_breakdown.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_pnl_breakdown.png")


def _aggregate_filter_impact(
    positions: List[MatchedPosition],
    skip_events: List[SkipEvent],
    lookback_days: int
) -> Dict[str, Dict[str, dict]]:
    """
    Aggregate filter impact data per wallet per filter reason.

    Returns:
        {wallet: {reason: {passed, skip_buckets: [(label, count, lower_bound)],
                           total, current_threshold}}}
    Buckets are ordered near-threshold-first (highest value bucket first).
    """
    from datetime import datetime as _dt
    from collections import Counter

    cutoff = date.today() - timedelta(days=lookback_days)

    # Define buckets per reason (ordered low→high; reversed later for display)
    AGE_BUCKETS = [
        ("<1h", 0, 1), ("1-2h", 1, 2), ("2-3h", 2, 3), ("3-4h", 3, 4), ("4h+", 4, float('inf'))
    ]
    JUP_BUCKETS = [
        ("0-5", 0, 5), ("5-10", 5, 10), ("10-15", 10, 15), ("15-20", 15, 20),
        ("20-25", 20, 25), ("25-30", 25, 30), ("30-35", 30, 35), ("35-40", 35, 40),
        ("40-45", 40, 45), ("45-50", 45, 50), ("50-55", 50, 55), ("55-60", 55, 60),
        ("60-65", 60, 65), ("65-70", 65, 70), ("70-75", 70, 75), ("75-80", 75, 80),
        ("80-85", 80, 85), ("85-90", 85, 90), ("90-95", 90, 95), ("95+", 95, float('inf'))
    ]
    MC_BUCKETS = [
        ("<100K", 0, 100_000), ("100-200K", 100_000, 200_000),
        ("200-300K", 200_000, 300_000), ("300-400K", 300_000, 400_000),
        ("400-500K", 400_000, 500_000), ("500K-1M", 500_000, 1_000_000),
        ("1-1.5M", 1_000_000, 1_500_000), ("1.5-2M", 1_500_000, 2_000_000),
        ("2-2.5M", 2_000_000, 2_500_000), ("2.5-3M", 2_500_000, 3_000_000),
        ("3M+", 3_000_000, float('inf'))
    ]

    BUCKET_MAP = {
        "token age restriction": AGE_BUCKETS,
        "low Jupiter organic score": JUP_BUCKETS,
        "low market cap": MC_BUCKETS,
    }

    # Count passed positions per wallet (closed positions in lookback window)
    pass_counts: Dict[str, int] = defaultdict(int)
    for pos in positions:
        if pos.datetime_close:
            close_dt = parse_iso_datetime(pos.datetime_close)
            if close_dt and close_dt.date() >= cutoff:
                pass_counts[pos.target_wallet] += 1

    # Group skip events per wallet per reason, bucket by metric_value
    skip_data: Dict[str, Dict[str, Dict[str, int]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(int))
    )
    skip_none: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    threshold_vals: Dict[str, Dict[str, list]] = defaultdict(lambda: defaultdict(list))

    for ev in skip_events:
        if ev.date:
            try:
                ev_date = _dt.strptime(ev.date, "%Y-%m-%d").date()
            except ValueError:
                continue
            if ev_date < cutoff:
                continue

        reason = ev.reason
        if reason not in BUCKET_MAP:
            continue

        wallet = ev.target

        if ev.threshold_value is not None:
            threshold_vals[wallet][reason].append(ev.threshold_value)

        if ev.metric_value is not None:
            for label, lo, hi in BUCKET_MAP[reason]:
                if lo <= ev.metric_value < hi:
                    skip_data[wallet][reason][label] += 1
                    break
        else:
            skip_none[wallet][reason] += 1

    # Build result
    all_wallets = set(pass_counts.keys()) | set(skip_data.keys()) | set(skip_none.keys())

    result: Dict[str, Dict[str, dict]] = {}
    for wallet in all_wallets:
        result[wallet] = {}
        passed = pass_counts.get(wallet, 0)

        for reason, buckets in BUCKET_MAP.items():
            # Reversed: near-threshold (highest value) first
            bucket_counts = []
            total_skipped = 0
            for label, lo, _ in reversed(buckets):
                count = skip_data.get(wallet, {}).get(reason, {}).get(label, 0)
                bucket_counts.append((label, count, lo))
                total_skipped += count

            none_count = skip_none.get(wallet, {}).get(reason, 0)
            total_skipped += none_count
            if none_count > 0:
                bucket_counts.append(("unknown", none_count, None))

            # Most common threshold for this wallet+reason
            thresh_list = threshold_vals.get(wallet, {}).get(reason, [])
            current_threshold = Counter(thresh_list).most_common(1)[0][0] if thresh_list else None

            result[wallet][reason] = {
                'passed': passed,
                'skip_buckets': bucket_counts,
                'total': passed + total_skipped,
                'current_threshold': current_threshold,
            }

    return result


def _format_filter_threshold(reason: str, value) -> str:
    """Format a filter threshold value for human-readable display."""
    if value is None:
        return "?"
    if reason == "token age restriction":
        if value < 1:
            return f"{int(value * 60)}min"
        return f"{int(value)}h"
    elif reason == "low Jupiter organic score":
        return str(int(value))
    elif reason == "low market cap":
        if value >= 1_000_000:
            v = value / 1_000_000
            return f"${v:.0f}M" if v == int(v) else f"${v:.1f}M"
        elif value >= 1_000:
            v = value / 1_000
            return f"${v:.0f}K" if v == int(v) else f"${v:.1f}K"
        return f"${int(value)}"
    return str(value)


def _chart_filter_impact(
    impact_data: Dict[str, Dict[str, dict]],
    wallets: List[str],
    output_dir: str
) -> None:
    """
    Generate filter impact horizontal stacked bar charts.

    Layout per wallet:
    - Bold wallet name header above its bars
    - 3 horizontal bars (Token Age, Jup Score, Market Cap)
    - Green = passed, YlOrRd gradient = skip buckets (yellow=near threshold, red=far)
    - Annotation below each bar: cumulative threshold recommendations
    Paginated: max 4 wallets per PNG.
    """
    from pathlib import Path

    if not wallets:
        return

    FILTERS = ["token age restriction", "low Jupiter organic score", "low market cap"]
    FILTER_LABELS = {
        "token age restriction": "Token Age",
        "low Jupiter organic score": "Jup Score",
        "low market cap": "Market Cap",
    }
    PASS_COLOR = '#2ecc71'
    UNKNOWN_COLOR = '#7f8c8d'
    PAGE_SIZE = 4

    from matplotlib.cm import get_cmap
    warm_cmap = get_cmap('YlOrRd')

    pages = [wallets[i:i + PAGE_SIZE] for i in range(0, len(wallets), PAGE_SIZE)]

    for page_idx, page_wallets in enumerate(pages):
        # Layout constants
        BAR_HEIGHT = 0.85    # thicker bars for readability
        BAR_STEP = 2.0       # vertical space per bar (bar + 2 annotation lines)
        WALLET_GAP = 1.5     # gap between wallet groups
        HEADER_H = 0.8       # height for wallet header

        total_y = len(page_wallets) * (HEADER_H + len(FILTERS) * BAR_STEP + WALLET_GAP)
        fig_height = max(10, total_y * 0.45 + 2)
        fig, ax = plt.subplots(figsize=(16, fig_height))

        fig.patch.set_facecolor('#1a1a2e')
        ax.set_facecolor('#16213e')

        tick_positions = []
        tick_labels = []
        x_max = 1  # track max bar width for xlim

        y = 0
        for wallet in page_wallets:
            wallet_data = impact_data.get(wallet, {})
            short = _short_wallet(wallet)

            # Wallet header above its bars
            header_y = y
            ax.axhline(y=header_y - 0.3, color='#333355', linewidth=0.5, linestyle='--')
            ax.text(0, header_y, f'\u25a0  {short}', ha='left', va='center',
                    color='#e0e0e0', fontsize=12, fontweight='bold', clip_on=False)
            y += HEADER_H

            for filt in FILTERS:
                data = wallet_data.get(filt, {
                    'passed': 0, 'skip_buckets': [], 'total': 0, 'current_threshold': None
                })
                passed = data['passed']
                skip_buckets = data['skip_buckets']
                total = data['total']
                current_threshold = data.get('current_threshold')

                bar_y = y
                tick_positions.append(bar_y)
                tick_labels.append(FILTER_LABELS[filt])

                if total == 0:
                    ax.text(0.5, bar_y, 'no data', ha='center', va='center',
                            color='#666666', fontsize=9, style='italic')
                    y += BAR_STEP
                    continue

                if total > x_max:
                    x_max = total

                # Draw passed segment
                left = 0
                if passed > 0:
                    ax.barh(bar_y, passed, left=left, height=BAR_HEIGHT,
                            color=PASS_COLOR, edgecolor='none')
                    if passed / total > 0.04:
                        ax.text(left + passed / 2, bar_y, str(passed),
                                ha='center', va='center', color='white',
                                fontsize=9, fontweight='bold')
                    left += passed

                # Draw skip bucket segments (near-threshold first = yellow)
                non_unknown = [b for b in skip_buckets if b[0] != "unknown"]
                n_skip = len(non_unknown)
                skip_idx = 0

                for label, count, lo_bound in skip_buckets:
                    if count == 0:
                        if label != "unknown":
                            skip_idx += 1
                        continue
                    if label == "unknown":
                        color = UNKNOWN_COLOR
                    else:
                        if n_skip > 1:
                            t = 0.2 + 0.7 * skip_idx / (n_skip - 1)
                        else:
                            t = 0.5
                        color = warm_cmap(t)
                        skip_idx += 1

                    ax.barh(bar_y, count, left=left, height=BAR_HEIGHT,
                            color=color, edgecolor='none')
                    # Show bucket label + count inside segment if wide enough
                    if count / total > 0.06:
                        ax.text(left + count / 2, bar_y, f'{label}\n({count})',
                                ha='center', va='center', color='white',
                                fontsize=7, linespacing=0.85)
                    elif count / total > 0.03:
                        ax.text(left + count / 2, bar_y, str(count),
                                ha='center', va='center', color='white',
                                fontsize=7)
                    left += count

                # Line 1 below bar: pass/skip summary
                total_skipped = sum(c for _, c, _ in skip_buckets)
                pass_pct = passed / total * 100 if total > 0 else 0
                skip_pct = total_skipped / total * 100 if total > 0 else 0
                ax.text(0, bar_y + 0.58,
                        f'pass: {passed} ({pass_pct:.0f}%)  |  skip: {total_skipped} ({skip_pct:.0f}%)',
                        ha='left', va='center', color='#bbbbbb', fontsize=8,
                        clip_on=False)

                # Line 2 below bar: cumulative threshold recommendations
                rec_parts = []
                if current_threshold is not None:
                    rec_parts.append(
                        f"Now: {_format_filter_threshold(filt, current_threshold)}"
                    )
                cumulative = 0
                for label, count, lo_bound in skip_buckets:
                    if count == 0 or label == "unknown" or lo_bound is None:
                        continue
                    cumulative += count
                    set_to = _format_filter_threshold(filt, lo_bound)
                    rec_parts.append(f"\u2192 {set_to}: +{cumulative}")
                if rec_parts:
                    ax.text(0, bar_y + 0.82, "  ".join(rec_parts),
                            ha='left', va='center', color='#999999',
                            fontsize=7, style='italic', clip_on=False)

                y += BAR_STEP

            y += WALLET_GAP

        # Y-axis
        ax.set_yticks(tick_positions)
        ax.set_yticklabels(tick_labels, color='#cccccc', fontsize=10)
        ax.invert_yaxis()

        # Give right margin for annotations
        ax.set_xlim(right=x_max * 1.15)

        ax.set_xlabel('Position Count', color='#cccccc', fontsize=11)
        ax.tick_params(axis='x', colors='#cccccc')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color('#444444')
        ax.spines['left'].set_color('#444444')

        title = 'Filter Impact Analysis \u2014 Skip vs Pass per Wallet'
        if len(pages) > 1:
            title += f' ({page_idx + 1}/{len(pages)})'
        ax.set_title(title, color='white', fontsize=14, fontweight='bold', pad=15)

        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, facecolor=PASS_COLOR, label='Passed'),
            plt.Rectangle((0, 0), 1, 1, facecolor=warm_cmap(0.2), label='Skip (near threshold)'),
            plt.Rectangle((0, 0), 1, 1, facecolor=warm_cmap(0.9), label='Skip (far from threshold)'),
            plt.Rectangle((0, 0), 1, 1, facecolor=UNKNOWN_COLOR, label='Skip (unknown value)'),
        ]
        ax.legend(handles=legend_elements, loc='lower right',
                  facecolor='#1a1a2e', edgecolor='#444444',
                  labelcolor='#cccccc', fontsize=9)

        fig.tight_layout()
        suffix = f'_p{page_idx + 1}' if len(pages) > 1 else ''
        filename = f'filter_impact{suffix}.png'
        fig.savefig(Path(output_dir) / filename, dpi=120)
        plt.close(fig)
        print(f"  Generated: {filename}")


def generate_charts(positions: List[MatchedPosition], output_dir: str, skip_events: Optional[List[SkipEvent]] = None) -> None:
    """
    Generate PNG chart files from position data.

    Creates charts:
    - daily_pnl.png: Daily PnL per wallet
    - daily_pnl_pct.png: Daily PnL % per wallet (ROI)
    - daily_entries.png: Daily positions opened per wallet
    - daily_winrate.png: Daily win rate per wallet
    - daily_rugs.png: Daily rug + stop-loss count per wallet
    - daily_pnl_rolling_3d.png: 3-day rolling average PnL
    - daily_pnl_rolling_7d.png: 7-day rolling average PnL
    - portfolio_cumulative.png: Portfolio daily PnL bars + cumulative line
    - filter_impact.png: Filter impact analysis (skip vs pass per wallet)

    Args:
        positions: List of MatchedPosition objects
        output_dir: Directory to save chart files
        skip_events: Optional list of SkipEvent objects for filter impact chart
    """
    if not HAS_MATPLOTLIB:
        print("  matplotlib not installed, skipping charts")
        return

    # Filter positions: must have valid datetime_close and non-None pnl_sol
    dated = []
    for p in positions:
        if p.pnl_sol is None:
            continue
        dt = parse_iso_datetime(p.datetime_close)
        if dt is None:
            continue
        dated.append((p, dt.date()))

    if len(dated) < 1:
        print("  Not enough dated positions for charts (need 1+)")
        return

    # Aggregate data (uses close dates for PnL, winrate, rugs, stop-losses)
    pnl_data, entries_data_close, winrate_data, rugs_data, pnl_pct_data, dates, wallets, sl_data = _aggregate_daily_data(dated)

    # Build separate entries data based on OPEN dates (not close dates)
    entries_data = {}
    all_open_dates = set()
    for p in positions:
        if p.pnl_sol is None:
            continue
        dt_open = parse_iso_datetime(p.datetime_open)
        if dt_open is None:
            continue
        open_date = dt_open.date()
        all_open_dates.add(open_date)
        key = (p.target_wallet, open_date)
        entries_data[key] = entries_data.get(key, 0) + 1

    # Merge open dates into the global dates list
    dates = sorted(set(dates) | all_open_dates)

    if not dates or not wallets:
        print("  No date/wallet data for charts")
        return

    # Apply wallet retirement filter: completely hide wallets inactive > SCORECARD_INACTIVE_DAYS
    retired = _apply_wallet_retirement(
        [pnl_data, entries_data, winrate_data, rugs_data, pnl_pct_data, sl_data],
        dates,
        wallets,
        gap_days=SCORECARD_INACTIVE_DAYS
    )
    if retired:
        print(f"  Hiding {len(retired)} inactive wallet(s) from charts: {', '.join(sorted(retired))}")
    wallets = [w for w in wallets if w not in retired]
    if not wallets:
        print("  No active wallets remaining after retirement filter, skipping charts")
        return

    # Fill zeros for active wallet ranges on PnL and entries charts
    _fill_zeros_for_active_range(pnl_data, dates, wallets)
    _fill_zeros_for_active_range(entries_data, dates, wallets)
    _fill_zeros_for_active_range(pnl_pct_data, dates, wallets)

    # Assign consistent wallet colors
    wallet_colors = _get_wallet_colors(wallets)

    # Generate all charts
    _chart_daily_pnl(pnl_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_pnl_pct(pnl_pct_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_entries(entries_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_winrate(winrate_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_losses(rugs_data, sl_data, dates, wallets, wallet_colors, output_dir)
    _chart_rolling_avg_pnl(pnl_data, dates, wallets, wallet_colors, output_dir, window=3)
    _chart_rolling_avg_pnl(pnl_data, dates, wallets, wallet_colors, output_dir, window=7)
    _chart_portfolio_cumulative(pnl_data, dates, wallets, output_dir)

    # Daily PnL breakdown: stacked bar per wallet per day (last N days)
    breakdown_data, bd_dates, bd_wallets = _aggregate_pnl_breakdown(
        dated, PORTFOLIO_TOTAL_SOL, PNL_BREAKDOWN_LOOKBACK_DAYS
    )
    if not bd_dates:
        print("  Skipping daily_pnl_breakdown.png (no positions in lookback window)")
    else:
        _chart_daily_pnl_breakdown(breakdown_data, bd_dates, bd_wallets, output_dir)

    # Filter Impact Analysis chart
    if skip_events:
        impact_data = _aggregate_filter_impact(positions, skip_events, FILTER_IMPACT_LOOKBACK_DAYS)
        active_impact = {w: d for w, d in impact_data.items() if w not in retired}
        if active_impact:
            _chart_filter_impact(active_impact, sorted(active_impact.keys()), output_dir)


def _chart_rolling_avg_pnl(
    pnl_data: Dict[Tuple[str, date], float],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str,
    window: int
) -> None:
    """
    Generate rolling average PnL line chart.

    For each wallet with >= window days of history, computes a rolling
    window-day average of daily PnL and plots it as a line.
    """
    from pathlib import Path

    eligible_wallets = []
    wallet_series = {}

    for wallet in wallets:
        # Collect sorted daily PnL for this wallet (only days with data)
        series = []
        for d in dates:
            val = pnl_data.get((wallet, d))
            if val is not None:
                series.append((d, val))

        if len(series) >= window:
            eligible_wallets.append(wallet)
            wallet_series[wallet] = series

    if not eligible_wallets:
        print(f"  Skipping daily_pnl_rolling_{window}d.png (no wallets with {window}+ days)")
        return

    fig, ax = plt.subplots(figsize=(12, 10))

    for wallet in eligible_wallets:
        series = wallet_series[wallet]
        roll_dates = []
        roll_values = []
        for i in range(window - 1, len(series)):
            avg = sum(v for _, v in series[i - window + 1:i + 1]) / window
            roll_dates.append(series[i][0])
            roll_values.append(avg)

        ax.plot(
            roll_dates,
            roll_values,
            marker='o',
            markersize=3,
            linewidth=2,
            linestyle='-',
            label=_short_wallet(wallet),
            color=wallet_colors[wallet]
        )

    # Formatting
    ax.set_title(f'Daily PnL {window}-Day Rolling Average (SOL)', fontsize=14, fontweight='bold')
    ax.set_ylabel('SOL', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.axhline(y=0, color='black', linewidth=0.8, linestyle='-', alpha=0.3)
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=20))
    ax.yaxis.set_minor_locator(AutoMinorLocator(2))
    ax.grid(True, alpha=0.3, axis='y', which='major')
    ax.grid(True, alpha=0.15, axis='y', which='minor')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    fig.savefig(Path(output_dir) / f'daily_pnl_rolling_{window}d.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: daily_pnl_rolling_{window}d.png")


def generate_insufficient_balance_chart(csv_path: str, output_dir: str) -> None:
    """Generate a daily line chart of insufficient balance event counts from CSV."""
    if not HAS_MATPLOTLIB:
        print("  matplotlib not installed, skipping insufficient balance chart")
        return

    from pathlib import Path
    import csv as csv_module

    csv_file = Path(csv_path)
    if not csv_file.exists():
        return

    # Read daily counts from CSV
    daily_counts: Dict[date, int] = defaultdict(int)
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv_module.DictReader(f)
        for row in reader:
            dt_str = row.get('datetime', '').strip()
            if dt_str:
                dt = parse_iso_datetime(dt_str)
                if dt:
                    daily_counts[dt.date()] += 1

    if not daily_counts:
        return

    sorted_dates = sorted(daily_counts.keys())
    counts = [daily_counts[d] for d in sorted_dates]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(sorted_dates, counts, marker='o', markersize=5, linewidth=2,
            linestyle='-', color='#e74c3c')

    ax.set_title('Daily Insufficient Balance Events', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.yaxis.get_major_locator().set_params(integer=True)

    fig.tight_layout()

    fig.savefig(Path(output_dir) / 'daily_insufficient_balance.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_insufficient_balance.png")
