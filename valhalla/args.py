import argparse


def build_parser():
    parser = argparse.ArgumentParser(
        description='Parse Valhalla Bot Discord DM logs and generate PnL analysis with Meteora API.'
    )
    parser.add_argument('input_files', nargs='*', help='Path(s) to Discord DM log file(s) (default: all files in input/ folder)')
    parser.add_argument('--output-dir', default='output', help='Output directory for CSV files (default: output/)')
    parser.add_argument('--rpc-url', default='https://api.mainnet-beta.solana.com',
                       help='Solana RPC URL (default: public mainnet)')
    parser.add_argument('--skip-rpc', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--skip-meteora', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--use-discord-pnl', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--no-archive', action='store_true', help='Skip moving processed files to archive/')
    parser.add_argument('--cache-file', help='Address cache JSON file (default: address_cache.json in output-dir)')
    parser.add_argument('--date', help='Date for logs in YYYY-MM-DD format (optional, will try to detect from filename)')
    parser.add_argument('--input-format', choices=['auto', 'text', 'html'], default='auto',
                       help='Input format: auto (detect), text (plain text), html (HTML from browser)')
    parser.add_argument('--merge', nargs='+', metavar='CSV_FILE',
                       help='Merge multiple positions.csv files (use instead of input_files)')
    parser.add_argument('--export-json', metavar='FILE',
                       help='Export results as .valhalla.json for incremental workflows')
    parser.add_argument('--import-json', metavar='FILE',
                       help='Import previous .valhalla.json to merge with new data')
    parser.add_argument('--skip-charts', action='store_true', help='Skip chart generation')
    parser.add_argument('--no-clipboard', action='store_true', help='Skip auto-running save_clipboard.ps1')
    parser.add_argument('--recover-insuf', action='store_true',
                       help='Recover insufficient balance history from archive files')
    parser.add_argument('--backtest', nargs='+', metavar='PARAM=VALUE',
                       help='Run filter backtest with custom thresholds. '
                            'E.g.: --backtest jup_score=80 mc=5000000 age=1')
    parser.add_argument('--wallet', metavar='WALLET_ID',
                       help='Filter --backtest to a specific wallet alias')
    parser.add_argument('--no-loss-analysis', action='store_true',
                       help='Skip loss analysis report generation')
    parser.add_argument('--no-wallet-trend', action='store_true',
                       help='Skip wallet trend report generation (output/wallet_trend.md)')
    parser.add_argument('--no-input', action='store_true',
                       help='Skip input file processing and load from existing positions.csv. '
                            'Useful to re-run analysis without processing new logs.')
    parser.add_argument('--report', default='all',
                       help='Comma-separated list of report modules to generate. '
                            'Options: loss,per-wallet,source,charts,recommendations,all (default: all)')
    parser.add_argument('--track', action='store_true',
                       help='Interactively mark recommendation statuses (done/ignored/new). '
                            'Loads positions from output/positions.csv and shows current items.')
    parser.add_argument('--lpagent', action='store_true',
                       help='Enable auto lpagent cross-check after parsing (off by default)')
    parser.add_argument('--cross-check', nargs='*', metavar='DATE',
                       help='Run lpagent cross-check. Optional: FROM_DATE [TO_DATE] in YYYY-MM-DD format. '
                            'If no dates given, uses watermark to yesterday. '
                            'Skips normal log processing.')
    return parser


def parse_args(argv=None):
    return build_parser().parse_args(argv)
