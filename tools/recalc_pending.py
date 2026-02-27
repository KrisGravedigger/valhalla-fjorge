"""
Recalculate PnL for pending positions and fix Bug #1 positions.
Directly calls Meteora API and updates output/positions.csv.
"""
import csv
import sys
import time
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from valhalla.meteora import MeteoraPnlCalculator

CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output', 'positions.csv')

def main():
    # Read all positions
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    # Find positions to recalculate:
    # 1. pnl_source == "pending" with full_address
    # 2. pnl_source == "meteora" with meteora_deposited == "0.0000" (Bug #1)
    to_recalc = []
    for i, r in enumerate(rows):
        full_addr = r.get('full_address', '')
        if not full_addr:
            continue

        src = r.get('pnl_source', '')
        dep = r.get('meteora_deposited', '')

        if src == 'pending':
            to_recalc.append((i, full_addr, 'pending'))
        elif src == 'meteora' and dep in ('0.0000', '0', ''):
            # Check if lpagent shows non-zero deposit (Bug #1 candidates)
            to_recalc.append((i, full_addr, 'bug1'))

    print(f"Found {len(to_recalc)} positions to recalculate")
    for idx, addr, reason in to_recalc:
        r = rows[idx]
        print(f"  {r['position_id']:>10s} {r.get('token','?'):>12s} src={r.get('pnl_source','?'):>8s} "
              f"dep={r.get('meteora_deposited',''):>8s} reason={reason} "
              f"close={r.get('datetime_close','')[:10]}")

    print()
    calc = MeteoraPnlCalculator()

    success = 0
    failed = 0
    for idx, full_addr, reason in to_recalc:
        r = rows[idx]
        pid = r['position_id']
        print(f"Calculating {pid} ({r.get('token','?')})...", end=' ')

        result = calc.calculate_pnl(full_addr)
        if result:
            # Update the row
            rows[idx]['pnl_source'] = 'meteora'
            rows[idx]['meteora_deposited'] = f"{result.deposited_sol:.4f}"
            rows[idx]['meteora_withdrawn'] = f"{result.withdrawn_sol:.4f}"
            rows[idx]['meteora_fees'] = f"{result.fees_sol:.4f}"
            rows[idx]['meteora_pnl'] = f"{result.pnl_sol:.4f}"
            rows[idx]['pnl_sol'] = f"{result.pnl_sol:.4f}"
            rows[idx]['pnl_pct'] = f"{(result.pnl_sol / result.deposited_sol * 100):.2f}" if result.deposited_sol > 0 else "0.00"
            rows[idx]['sol_deployed'] = f"{result.deposited_sol:.4f}"

            print(f"OK dep={result.deposited_sol:.4f} pnl={result.pnl_sol:+.4f}")
            success += 1
        else:
            print("FAILED (API error or 404)")
            failed += 1

        time.sleep(1)  # rate limit

    print(f"\nResults: {success} success, {failed} failed")

    if success > 0:
        # Write updated CSV
        with open(CSV_PATH, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"Updated {CSV_PATH}")
    else:
        print("No changes to write.")

if __name__ == '__main__':
    main()
