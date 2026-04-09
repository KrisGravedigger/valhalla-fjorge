"""
Fetch Meteora DLMM API data for specific positions:
deposits, withdraws, claim_fees - with TX hashes and SOL equiv calculations.
"""

import json
import time
import urllib.request
from decimal import Decimal
from typing import Optional, Tuple

SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = Decimal('1000000000')
BASE_URL = "https://dlmm-api.meteora.ag"

# Target positions: short_id -> full_address, label
POSITIONS = [
    ("3KSCw7vD", "3KSCokqBk2i7vYAptipff8CYYDJJmqMHhTNM9Pz4w7vD",  "automaton (biggest outlier)"),
    ("5etffR8f", "5etf1aFGbPNgyBfMbMDgKfr9H6a7wxpt1WQV3JRgfR8f",  "automaton (outlier 2)"),
    ("ABZp12yw", "ABZpmF4PbuGbfmj5u7FMoEjEjGcFvA9uVrKQQdgt12yw",  "TOTO outlier"),
    ("BvapUjjU", "BvapBmYPupRW4vKx8PEYYu2oWMjbMJWzw4AeNVwEUjjU",  "Lobstar diff=+0.0972 (we HIGHER)"),
    ("CL6N3Ve3", "CL6NtppymER62r5VMJD47AYdJPDZne1ZTvrYT4v73Ve3",  "Tastecoin diff=+0.2041 (we HIGHER)"),
    ("9Q3Tk7PW", "9Q3TALQbXzeAQ2iuMcNL4sh4No9nsFRF2Q9PXb1qk7PW",  "MUSHU diff=-0.0913 (we LOWER)"),
    ("Azsxa6HU", "Azsx7gvth6GWcTBKASYGCLViiParJnVgDf1MLpRda6HU",  "Jellycat diff=-0.0787 (we LOWER)"),
]


def meteora_get(path: str):
    """Make GET request to Meteora API, return parsed JSON or None."""
    url = f"{BASE_URL}{path}"
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=20) as resp:
            return json.loads(resp.read())
    except Exception as e:
        print(f"  ERROR fetching {path}: {e}")
        return None


def get_sol_side(pair_address: str) -> Optional[Tuple[bool, bool]]:
    """Check /pair/{pair_address} to determine which token is SOL.
    Returns (sol_is_x, sol_is_y) or None if no SOL side found."""
    pair_info = meteora_get(f"/pair/{pair_address}")
    if not pair_info:
        return None
    mint_x = pair_info.get('mint_x', '')
    mint_y = pair_info.get('mint_y', '')
    sol_is_x = (mint_x == SOL_MINT)
    sol_is_y = (mint_y == SOL_MINT)
    print(f"    Pair {pair_address[:8]}... mint_x={mint_x[:8]}... mint_y={mint_y[:8]}... sol_is_x={sol_is_x} sol_is_y={sol_is_y}")
    if not (sol_is_x or sol_is_y):
        return None
    return (sol_is_x, sol_is_y)


def tx_sol_equiv(
    entry: dict,
    sol_is_x: bool,
    fallback_sol_price: Decimal,
) -> Tuple[Decimal, Decimal, Decimal, Decimal, Decimal, Decimal]:
    """Compute SOL equivalent for a single transaction entry.
    Returns (sol_amt, token_sol_equiv, total_sol_equiv, sol_usd, tok_usd, sol_price_used)
    """
    sol_key = 'token_x_amount' if sol_is_x else 'token_y_amount'
    tok_key = 'token_y_amount' if sol_is_x else 'token_x_amount'
    sol_usd_key = 'token_x_usd_amount' if sol_is_x else 'token_y_usd_amount'
    tok_usd_key = 'token_y_usd_amount' if sol_is_x else 'token_x_usd_amount'

    sol_raw = Decimal(str(entry.get(sol_key, 0)))
    sol_amt = sol_raw / LAMPORTS
    sol_usd = Decimal(str(entry.get(sol_usd_key, 0)))
    tok_usd = Decimal(str(entry.get(tok_usd_key, 0)))

    # Derive per-tx SOL price from the SOL portion
    if sol_amt > 0 and sol_usd > 0:
        sol_price = sol_usd / sol_amt
    else:
        sol_price = fallback_sol_price

    # Convert token USD to SOL using this tx's SOL price
    if tok_usd > 0 and sol_price > 0:
        token_sol_equiv = tok_usd / sol_price
    else:
        token_sol_equiv = Decimal('0')

    total_sol_equiv = sol_amt + token_sol_equiv
    return sol_amt, token_sol_equiv, total_sol_equiv, sol_usd, tok_usd, sol_price


def fetch_position(short_id: str, address: str, label: str) -> dict:
    """Fetch all data for one position. Returns dict with all results."""
    print(f"\n{'='*60}")
    print(f"Fetching {short_id} ({label})")
    print(f"Address: {address}")

    # 1. Get position info -> pair_address
    pos_info = meteora_get(f"/position/{address}")
    if not pos_info:
        return {"error": "Could not fetch /position/"}

    pair_address = pos_info.get('pair_address', '')
    if not pair_address:
        return {"error": "No pair_address in position info"}
    print(f"  pair_address: {pair_address}")
    time.sleep(0.5)

    # 2. Determine which side is SOL
    sol_side = get_sol_side(pair_address)
    if not sol_side:
        return {"error": f"No SOL token in pair {pair_address}"}
    sol_is_x, sol_is_y = sol_side
    print(f"  SOL is {'X' if sol_is_x else 'Y'} side")
    time.sleep(0.5)

    # 3. Fetch deposits
    deposits = meteora_get(f"/position/{address}/deposits")
    if deposits is None:
        return {"error": "Could not fetch deposits"}
    print(f"  deposits: {len(deposits)} transactions")
    time.sleep(0.5)

    # 4. Fetch withdraws
    withdraws = meteora_get(f"/position/{address}/withdraws")
    if withdraws is None:
        return {"error": "Could not fetch withdraws"}
    print(f"  withdraws: {len(withdraws)} transactions")
    time.sleep(0.5)

    # 5. Fetch claim_fees
    fees_list = meteora_get(f"/position/{address}/claim_fees")
    if fees_list is None:
        return {"error": "Could not fetch claim_fees"}
    print(f"  claim_fees: {len(fees_list)} transactions")
    time.sleep(0.5)

    # 6. Pre-scan to find initial SOL price
    sol_amount_key = 'token_x_amount' if sol_is_x else 'token_y_amount'
    sol_usd_key = 'token_x_usd_amount' if sol_is_x else 'token_y_usd_amount'
    initial_sol_price = Decimal('0')
    for entry in (deposits or []) + (withdraws or []) + (fees_list or []):
        raw = Decimal(str(entry.get(sol_amount_key, 0)))
        amt = raw / LAMPORTS
        usd = Decimal(str(entry.get(sol_usd_key, 0)))
        if amt > 0 and usd > 0:
            initial_sol_price = usd / amt
            break
    print(f"  Initial SOL price: ${initial_sol_price:.2f}")

    running_sol_price = initial_sol_price

    # 7. Process each transaction type
    def process_txs(tx_list):
        nonlocal running_sol_price
        results = []
        total_sol_amt = Decimal('0')
        total_sol_equiv = Decimal('0')
        total_usd = Decimal('0')

        for entry in tx_list:
            tx_id = entry.get('tx_id', 'UNKNOWN')
            sol_amt, tok_equiv, total_equiv, sol_usd, tok_usd, price = tx_sol_equiv(
                entry, sol_is_x, running_sol_price
            )
            if price > 0:
                running_sol_price = price

            # Extract raw token amounts for display
            sol_raw_key = 'token_x_amount' if sol_is_x else 'token_y_amount'
            tok_raw_key = 'token_y_amount' if sol_is_x else 'token_x_amount'
            sol_raw = entry.get(sol_raw_key, 0)
            tok_raw = entry.get(tok_raw_key, 0)

            total_sol_amt += sol_amt
            total_sol_equiv += total_equiv
            total_usd += sol_usd + tok_usd

            results.append({
                'tx_id': tx_id,
                'sol_raw_lamports': sol_raw,
                'token_raw': tok_raw,
                'sol_amt': sol_amt,
                'tok_equiv': tok_equiv,
                'total_equiv': total_equiv,
                'sol_usd': sol_usd,
                'tok_usd': tok_usd,
                'sol_price': price,
                'entry': entry,
            })

        return results, total_sol_amt, total_sol_equiv, total_usd

    dep_results, dep_sol_amt, dep_sol_equiv, dep_usd = process_txs(deposits)
    wdr_results, wdr_sol_amt, wdr_sol_equiv, wdr_usd = process_txs(withdraws)
    fee_results, fee_sol_amt, fee_sol_equiv, fee_usd = process_txs(fees_list)

    pnl_sol = wdr_sol_equiv + fee_sol_equiv - dep_sol_equiv

    return {
        'short_id': short_id,
        'address': address,
        'label': label,
        'pair_address': pair_address,
        'sol_is_x': sol_is_x,
        'initial_sol_price': initial_sol_price,
        'deposits': dep_results,
        'withdraws': wdr_results,
        'fees': fee_results,
        'dep_sol_equiv': dep_sol_equiv,
        'wdr_sol_equiv': wdr_sol_equiv,
        'fee_sol_equiv': fee_sol_equiv,
        'pnl_sol': pnl_sol,
        'dep_usd': dep_usd,
        'wdr_usd': wdr_usd,
        'fee_usd': fee_usd,
    }


def format_tx(tx: dict, sol_is_x: bool) -> str:
    """Format a single transaction for output."""
    sol_side_label = 'X (SOL)' if sol_is_x else 'Y (SOL)'
    tok_side_label = 'Y (token)' if sol_is_x else 'X (token)'
    lines = [
        f"    TX: {tx['tx_id']}",
        f"    SOL ({sol_side_label}): {tx['sol_amt']:.6f} SOL (raw: {tx['sol_raw_lamports']} lamports) = ${tx['sol_usd']:.4f}",
        f"    Token ({tok_side_label}): raw={tx['token_raw']} = ${tx['tok_usd']:.4f} => {tx['tok_equiv']:.6f} SOL equiv",
        f"    Total SOL equiv: {tx['total_equiv']:.6f} SOL | SOL price used: ${tx['sol_price']:.2f}",
    ]
    return '\n'.join(lines)


def generate_report(all_results: list) -> str:
    """Generate the full markdown report."""
    lines = [
        "# Transaction Detail for Developers",
        "",
        "Fetched from Meteora DLMM API: deposits, withdraws, claim_fees.",
        "SOL equivalents computed per transaction using SOL-side USD to derive SOL price,",
        "then converting token USD to SOL at that price (same logic as valhalla/meteora.py).",
        "",
        "SOL_MINT = `So11111111111111111111111111111111111111112`",
        "",
    ]

    for r in all_results:
        if 'error' in r:
            lines += [
                f"## {r.get('short_id', 'UNKNOWN')} - ERROR",
                f"Error: {r['error']}",
                "",
            ]
            continue

        lines += [
            f"## {r['short_id']} - {r['label']}",
            f"",
            f"**Full address**: `{r['address']}`  ",
            f"**Pair address**: `{r['pair_address']}`  ",
            f"**SOL side**: {'X' if r['sol_is_x'] else 'Y'}  ",
            f"**Initial SOL price**: ${r['initial_sol_price']:.2f}",
            "",
        ]

        # Summary table
        lines += [
            "### Summary",
            "",
            "| | SOL equiv | USD |",
            "|---|---|---|",
            f"| Deposited | {r['dep_sol_equiv']:.6f} | ${r['dep_usd']:.4f} |",
            f"| Withdrawn | {r['wdr_sol_equiv']:.6f} | ${r['wdr_usd']:.4f} |",
            f"| Fees | {r['fee_sol_equiv']:.6f} | ${r['fee_usd']:.4f} |",
            f"| **PnL** | **{r['pnl_sol']:.6f}** | |",
            "",
        ]

        # Deposits
        lines.append(f"### Deposits ({len(r['deposits'])} transactions)")
        lines.append("")
        if r['deposits']:
            lines.append("**TX hashes:**")
            for tx in r['deposits']:
                lines.append(f"- `{tx['tx_id']}`")
            lines.append("")
            lines.append("**Transaction details:**")
            lines.append("")
            for i, tx in enumerate(r['deposits'], 1):
                lines.append(f"**Deposit {i}:**")
                lines.append(f"- TX: `{tx['tx_id']}`")
                lines.append(f"- SOL amount: {tx['sol_amt']:.6f} SOL (raw lamports: {tx['sol_raw_lamports']}) = ${tx['sol_usd']:.4f}")
                lines.append(f"- Token amount: raw={tx['token_raw']} = ${tx['tok_usd']:.4f} => {tx['tok_equiv']:.6f} SOL equiv")
                lines.append(f"- Total SOL equiv: {tx['total_equiv']:.6f} SOL")
                lines.append(f"- SOL price used: ${tx['sol_price']:.2f}")
                lines.append("")
        else:
            lines.append("_No deposits found._")
            lines.append("")

        # Withdraws
        lines.append(f"### Withdrawals ({len(r['withdraws'])} transactions)")
        lines.append("")
        if r['withdraws']:
            lines.append("**TX hashes:**")
            for tx in r['withdraws']:
                lines.append(f"- `{tx['tx_id']}`")
            lines.append("")
            lines.append("**Transaction details:**")
            lines.append("")
            for i, tx in enumerate(r['withdraws'], 1):
                lines.append(f"**Withdrawal {i}:**")
                lines.append(f"- TX: `{tx['tx_id']}`")
                lines.append(f"- SOL amount: {tx['sol_amt']:.6f} SOL (raw lamports: {tx['sol_raw_lamports']}) = ${tx['sol_usd']:.4f}")
                lines.append(f"- Token amount: raw={tx['token_raw']} = ${tx['tok_usd']:.4f} => {tx['tok_equiv']:.6f} SOL equiv")
                lines.append(f"- Total SOL equiv: {tx['total_equiv']:.6f} SOL")
                lines.append(f"- SOL price used: ${tx['sol_price']:.2f}")
                lines.append("")
        else:
            lines.append("_No withdrawals found._")
            lines.append("")

        # Fees
        lines.append(f"### Claimed Fees ({len(r['fees'])} transactions)")
        lines.append("")
        if r['fees']:
            lines.append("**TX hashes:**")
            for tx in r['fees']:
                lines.append(f"- `{tx['tx_id']}`")
            lines.append("")
            lines.append("**Transaction details:**")
            lines.append("")
            for i, tx in enumerate(r['fees'], 1):
                lines.append(f"**Fee claim {i}:**")
                lines.append(f"- TX: `{tx['tx_id']}`")
                lines.append(f"- SOL amount: {tx['sol_amt']:.6f} SOL (raw lamports: {tx['sol_raw_lamports']}) = ${tx['sol_usd']:.4f}")
                lines.append(f"- Token amount: raw={tx['token_raw']} = ${tx['tok_usd']:.4f} => {tx['tok_equiv']:.6f} SOL equiv")
                lines.append(f"- Total SOL equiv: {tx['total_equiv']:.6f} SOL")
                lines.append(f"- SOL price used: ${tx['sol_price']:.2f}")
                lines.append("")
        else:
            lines.append("_No fee claims found._")
            lines.append("")

        lines.append("---")
        lines.append("")

    return '\n'.join(lines)


def main():
    all_results = []

    for short_id, address, label in POSITIONS:
        result = fetch_position(short_id, address, label)
        result['short_id'] = short_id  # ensure it's set even on error
        all_results.append(result)
        time.sleep(0.5)

    report = generate_report(all_results)

    output_path = r"C:\nju\ai\claude\projects\IaaS\valhalla-fjorge\notes\tx_detail_for_devs.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(report)

    print(f"\n\nReport written to: {output_path}")

    # Also print summary to console
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    for r in all_results:
        if 'error' in r:
            print(f"{r['short_id']}: ERROR - {r['error']}")
        else:
            print(f"{r['short_id']}: dep={r['dep_sol_equiv']:.4f} wdr={r['wdr_sol_equiv']:.4f} fees={r['fee_sol_equiv']:.4f} PnL={r['pnl_sol']:.4f}")


if __name__ == '__main__':
    main()
