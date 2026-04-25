"""
One-time patch for positions that cannot be handled by recalc_pending.py:

  - 7 "token-only deposit" positions: Meteora returned deposited_sol=0 because
    the user deposited 100% in the meme token (no SOL). sol_deployed is sourced
    manually from Discord archive ("Total Deposit: Target X SOL | User Y SOL").

  - 3 "non-SOL pair" positions: pool quote token is not SOL (ASTEROID/cbBTC pool),
    so Meteora API has no SOL side to report.

After running this script:
  - 7 rows get pnl_source="token_only_deposit" + correct sol_deployed, all PnL
    fields cleared (cannot be calculated automatically).
  - 3 rows get pnl_source="non_sol_pair", other fields unchanged.

Usage:
    python tools/patch_token_only_fix.py
"""

import csv
import os
import shutil

CSV_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    'output', 'positions.csv'
)
BACKUP_PATH = CSV_PATH + '.bak_token_only_fix'

# ------------------------------------------------------------------
# Patch data
# ------------------------------------------------------------------

# position_id -> sol_deployed value (as string, 4 decimal places)
TOKEN_ONLY = {
    'BwmBKvwe': '0.0117',  # ASTEROID
    '2WvRM9jc': '0.2444',  # Neukgu
    '8DgUy8qi': '0.2275',  # Neukgu
    'qBtS7VZZ': '0.9359',  # Tortellini
    'J9PtDAcK': '0.1902',  # stonks
    '8SFNwyAJ': '0.9832',  # 49
    'C59rftHX': '0.9702',  # 49
}

# position_ids for non-SOL-quote pools
NON_SOL_PAIR = {'3TuxPFF1', 'A2zvvLFj', 'CjD9FozB'}

# Fields to clear for token-only positions (cannot be auto-calculated)
_CLEAR_FIELDS = [
    'sol_received', 'pnl_sol', 'pnl_pct',
    'meteora_deposited', 'meteora_withdrawn', 'meteora_fees', 'meteora_pnl',
]


def main():
    # --- Read ---
    with open(CSV_PATH, encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Read {len(rows)} rows from {CSV_PATH}")

    # --- Backup ---
    shutil.copy2(CSV_PATH, BACKUP_PATH)
    print(f"Backup written to {BACKUP_PATH}")

    # --- Patch ---
    token_only_changed = 0
    non_sol_changed = 0

    for row in rows:
        pid = row['position_id']

        if pid in TOKEN_ONLY:
            sol_dep = TOKEN_ONLY[pid]
            old_src = row.get('pnl_source', '')
            row['pnl_source'] = 'token_only_deposit'
            row['sol_deployed'] = sol_dep
            for field in _CLEAR_FIELDS:
                row[field] = ''
            token_only_changed += 1
            print(
                f"  [token_only] {pid:>10s}  sol_deployed={sol_dep}"
                f"  (was pnl_source={old_src!r})"
            )

        elif pid in NON_SOL_PAIR:
            old_src = row.get('pnl_source', '')
            row['pnl_source'] = 'non_sol_pair'
            non_sol_changed += 1
            print(
                f"  [non_sol   ] {pid:>10s}  token={row.get('token', '?')}"
                f"  (was pnl_source={old_src!r})"
            )

    # --- Write ---
    with open(CSV_PATH, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    total_changed = token_only_changed + non_sol_changed
    print()
    print(f"Diff summary:")
    print(f"  token_only_deposit : {token_only_changed} rows patched")
    print(f"  non_sol_pair       : {non_sol_changed} rows patched")
    print(f"  Total              : {total_changed} rows changed")
    print(f"  Total rows in CSV  : {len(rows)} (unchanged)")
    print(f"Written: {CSV_PATH}")


if __name__ == '__main__':
    main()
