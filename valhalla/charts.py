"""
Chart generation module for Valhalla parser.
"""

from typing import List, Tuple
from decimal import Decimal

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


def _chart_cumulative_pnl(dated_positions: List[Tuple[MatchedPosition, 'datetime']], output_dir: str) -> None:
    """Generate cumulative PnL chart over time."""
    times = [dt for _, dt in dated_positions]
    cumulative = []
    running = Decimal('0')
    for pos, _ in dated_positions:
        running += pos.pnl_sol
        cumulative.append(float(running))

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(times, cumulative, linewidth=1.5, color='#2196F3')
    ax.axhline(y=0, color='gray', linewidth=0.5, linestyle='--')
    ax.fill_between(times, cumulative, 0,
                    where=[c >= 0 for c in cumulative], alpha=0.15, color='green')
    ax.fill_between(times, cumulative, 0,
                    where=[c < 0 for c in cumulative], alpha=0.15, color='red')
    ax.set_title('Cumulative PnL (SOL)')
    ax.set_ylabel('SOL')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    fig.autofmt_xdate()
    fig.tight_layout()
    from pathlib import Path
    fig.savefig(Path(output_dir) / 'pnl_cumulative.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: pnl_cumulative.png")


def _chart_win_rate_trend(dated_positions: List[Tuple[MatchedPosition, 'datetime']], output_dir: str) -> None:
    """Generate rolling win rate trend chart."""
    if len(dated_positions) < 2:
        return

    window = min(10, len(dated_positions))

    # Calculate rolling win rate
    position_numbers = []
    win_rates = []

    for i in range(len(dated_positions)):
        start_idx = max(0, i - window + 1)
        window_positions = dated_positions[start_idx:i + 1]

        wins = sum(1 for pos, _ in window_positions if pos.pnl_sol > 0)
        win_rate = (wins / len(window_positions)) * 100

        position_numbers.append(i + 1)
        win_rates.append(win_rate)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(position_numbers, win_rates, linewidth=1.5, color='#FF9800', marker='o', markersize=3)
    ax.axhline(y=50, color='gray', linewidth=0.5, linestyle='--', label='50% threshold')
    ax.set_title(f'Win Rate Trend (Rolling {window} positions)')
    ax.set_xlabel('Position Number')
    ax.set_ylabel('Win Rate (%)')
    ax.set_ylim(0, 100)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    from pathlib import Path
    fig.savefig(Path(output_dir) / 'win_rate_trend.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: win_rate_trend.png")


def _chart_pnl_by_strategy(dated_positions: List[Tuple[MatchedPosition, 'datetime']], output_dir: str) -> None:
    """Generate PnL by strategy chart (Spot vs BidAsk)."""
    # Separate by strategy
    spot_positions = [(p, dt) for p, dt in dated_positions if p.position_type == 'Spot']
    bidask_positions = [(p, dt) for p, dt in dated_positions if p.position_type == 'BidAsk']

    if not spot_positions and not bidask_positions:
        print("  No strategy data for PnL chart")
        return

    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot Spot cumulative
    if spot_positions:
        spot_positions.sort(key=lambda x: x[1])
        times = [dt for _, dt in spot_positions]
        cumulative = []
        running = Decimal('0')
        for pos, _ in spot_positions:
            running += pos.pnl_sol
            cumulative.append(float(running))
        ax.plot(times, cumulative, linewidth=1.5, color='#4CAF50', label='Spot', marker='o', markersize=3)

    # Plot BidAsk cumulative
    if bidask_positions:
        bidask_positions.sort(key=lambda x: x[1])
        times = [dt for _, dt in bidask_positions]
        cumulative = []
        running = Decimal('0')
        for pos, _ in bidask_positions:
            running += pos.pnl_sol
            cumulative.append(float(running))
        ax.plot(times, cumulative, linewidth=1.5, color='#9C27B0', label='BidAsk', marker='s', markersize=3)

    ax.axhline(y=0, color='gray', linewidth=0.5, linestyle='--')
    ax.set_title('Cumulative PnL by Strategy')
    ax.set_ylabel('SOL')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    from pathlib import Path
    fig.savefig(Path(output_dir) / 'pnl_by_strategy.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: pnl_by_strategy.png")


def generate_charts(positions: List[MatchedPosition], output_dir: str) -> None:
    """Generate PNG chart files from position data."""
    if not HAS_MATPLOTLIB:
        print("  matplotlib not installed, skipping charts")
        return

    # Filter positions with valid datetime_close and non-None pnl_sol
    dated = [(p, parse_iso_datetime(p.datetime_close)) for p in positions]
    dated = [(p, dt) for p, dt in dated if dt is not None and p.pnl_sol is not None]

    if len(dated) < 2:
        print("  Not enough dated positions for charts (need 2+)")
        return

    # Sort by close time
    dated.sort(key=lambda x: x[1])

    _chart_cumulative_pnl(dated, output_dir)
    _chart_win_rate_trend(dated, output_dir)
    _chart_pnl_by_strategy(dated, output_dir)
