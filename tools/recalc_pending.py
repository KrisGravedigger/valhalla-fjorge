"""
Recalculate PnL for pending positions and fix Bug #1 positions.
Directly calls Meteora API and updates output/positions.csv.

Cases handled per position:
  A  deposited_sol > 0        -> pnl_source="meteora", all fields filled
  B  deposited_sol == 0       -> pnl_source="token_only_deposit", sol_deployed
                                 kept if already set, PnL fields cleared
  C  reason_code="non_sol_pair" -> pnl_source="non_sol_pair", other fields unchanged
  D  other API failure        -> fallback: price_drop_pct -> "discord_estimate",
                                 or remain "pending" if fallback unavailable
"""
import csv
import os
import sys
import time

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from valhalla.meteora import MeteoraPnlCalculator

CSV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output', 'positions.csv')

# Fields to clear for token-only / non-calculable positions
_PNL_FIELDS = ['sol_received', 'pnl_sol', 'pnl_pct',
                'meteora_deposited', 'meteora_withdrawn', 'meteora_fees', 'meteora_pnl']


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

        result, reason_code = calc.calculate_pnl_with_reason(full_addr)

        if result is not None and result.deposited_sol > 0:
            # Case A: normal result with actual SOL amounts
            rows[idx]['pnl_source'] = 'meteora'
            rows[idx]['meteora_deposited'] = f"{result.deposited_sol:.4f}"
            rows[idx]['meteora_withdrawn'] = f"{result.withdrawn_sol:.4f}"
            rows[idx]['meteora_fees'] = f"{result.fees_sol:.4f}"
            rows[idx]['meteora_pnl'] = f"{result.pnl_sol:.4f}"
            rows[idx]['pnl_sol'] = f"{result.pnl_sol:.4f}"
            rows[idx]['pnl_pct'] = f"{(result.pnl_sol / result.deposited_sol * 100):.2f}"
            rows[idx]['sol_deployed'] = f"{result.deposited_sol:.4f}"
            print(f"OK dep={result.deposited_sol:.4f} pnl={result.pnl_sol:+.4f}")
            success += 1

        elif result is not None and result.deposited_sol == 0:
            # Case B: token-only deposit — Meteora correctly returns 0 for SOL flow
            existing_sol_deployed = rows[idx].get('sol_deployed', '')
            rows[idx]['pnl_source'] = 'token_only_deposit'
            # Preserve existing sol_deployed if already set by patch script
            # (do NOT overwrite with 0)
            for field in _PNL_FIELDS:
                rows[idx][field] = ''
            print(f"OK (token-only deposit detected, keeping sol_deployed={existing_sol_deployed!r})")
            success += 1

        elif reason_code == 'non_sol_pair':
            # Case C: pool has no SOL token — mark and leave other fields unchanged
            rows[idx]['pnl_source'] = 'non_sol_pair'
            print("OK (non-SOL pair detected)")
            success += 1

        else:
            # Case D: genuine API error / not_found — try price_drop_pct fallback
            drop_pct_str = r.get('price_drop_pct', '')
            sol_dep_str = r.get('sol_deployed', '')
            if drop_pct_str and sol_dep_str:
                try:
                    drop_pct = float(drop_pct_str)
                    sol_dep = float(sol_dep_str)
                    estimated_loss = sol_dep * drop_pct / 100
                    pnl_sol = -estimated_loss
                    sol_received = sol_dep + pnl_sol
                    rows[idx]['pnl_sol'] = f"{pnl_sol:.4f}"
                    rows[idx]['pnl_pct'] = f"{-drop_pct:.2f}"
                    rows[idx]['sol_received'] = f"{sol_received:.4f}"
                    rows[idx]['pnl_source'] = 'discord_estimate'
                    print(f"OK (price_drop estimate: -{drop_pct:.2f}%)")
                    success += 1
                except (ValueError, ZeroDivisionError):
                    print(f"FAILED ({reason_code})")
                    failed += 1
            else:
                print(f"FAILED ({reason_code})")
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
