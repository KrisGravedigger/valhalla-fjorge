"""
Chart generation module for Valhalla parser.
"""

from typing import List, Dict, Tuple
from decimal import Decimal
from datetime import date
from collections import defaultdict

from .models import MatchedPosition, parse_iso_datetime

# Optional matplotlib for chart generation
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
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
    List[date],                      # sorted_dates
    List[str]                        # sorted_wallets
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
        - sorted_dates: List of unique dates sorted
        - sorted_wallets: List of unique wallets sorted
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

    for (wallet, dt), positions in grouped.items():
        # PnL sum
        total_pnl = sum(float(p.pnl_sol) for p in positions)
        pnl_data[(wallet, dt)] = total_pnl

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
            if p.close_reason in ("rug", "rug_unknown_open")
        )
        rugs_data[(wallet, dt)] = rug_count

    # Extract unique dates and wallets
    all_dates = sorted(set(dt for _, dt in grouped.keys()))
    all_wallets = sorted(set(wallet for wallet, _ in grouped.keys()))

    return pnl_data, entries_data, winrate_data, rugs_data, all_dates, all_wallets


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
    """
    fig, ax = plt.subplots(figsize=(12, 6))

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

    # Formatting
    ax.set_title('Daily PnL per Wallet (SOL)', fontsize=14, fontweight='bold')
    ax.set_ylabel('SOL', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.axhline(y=0, color='black', linewidth=0.8, linestyle='-', alpha=0.3)
    ax.grid(True, alpha=0.3, axis='y')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_pnl.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_pnl.png")


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
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
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
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_winrate.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_winrate.png")


def _chart_daily_rugs(
    rugs_data: Dict[Tuple[str, date], int],
    dates: List[date],
    wallets: List[str],
    wallet_colors: Dict[str, str],
    output_dir: str
) -> None:
    """
    Generate daily rug count line chart.

    Each wallet is represented by a colored line.
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot each wallet as a line
    for wallet in wallets:
        values = [rugs_data.get((wallet, d)) for d in dates]

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
        day_values = [rugs_data.get((w, d)) for w in wallets if (w, d) in rugs_data]
        if day_values:
            means.append(sum(day_values) / len(day_values))
        else:
            means.append(None)
    ax.plot(dates, means, 'k--', linewidth=1.5, label='Mean')

    # Formatting
    ax.set_title('Daily Rug Count per Wallet', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count', fontsize=11)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')
    ax.grid(True, alpha=0.3, axis='y')
    ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='small')

    fig.tight_layout()

    from pathlib import Path
    fig.savefig(Path(output_dir) / 'daily_rugs.png', dpi=120)
    plt.close(fig)
    print("  Generated: daily_rugs.png")


def generate_charts(positions: List[MatchedPosition], output_dir: str) -> None:
    """
    Generate PNG chart files from position data.

    Creates 4 line charts:
    - daily_pnl.png: Daily PnL per wallet
    - daily_entries.png: Daily positions opened per wallet
    - daily_winrate.png: Daily win rate per wallet
    - daily_rugs.png: Daily rug count per wallet

    Args:
        positions: List of MatchedPosition objects
        output_dir: Directory to save chart files
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

    # Aggregate data
    pnl_data, entries_data, winrate_data, rugs_data, dates, wallets = _aggregate_daily_data(dated)

    if not dates or not wallets:
        print("  No date/wallet data for charts")
        return

    # Assign consistent wallet colors
    wallet_colors = _get_wallet_colors(wallets)

    # Generate all 4 charts
    _chart_daily_pnl(pnl_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_entries(entries_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_winrate(winrate_data, dates, wallets, wallet_colors, output_dir)
    _chart_daily_rugs(rugs_data, dates, wallets, wallet_colors, output_dir)
