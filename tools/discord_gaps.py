"""
CLI wrapper for the Discord gap reporter.

Usage:
    python tools/discord_gaps.py
    python tools/discord_gaps.py output/positions.csv
"""
import sys
import os

# Allow running from project root or tools/
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from valhalla.discord_gaps import report_discord_gaps

if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "output/positions.csv"
    report_discord_gaps(path, silent_if_none=False)
