"""Compare specific positions between our data and lpagent."""
import csv
from decimal import Decimal

targets = ['3KSCw7vD', '5etffR8f', 'ABZp12yw', 'BvapUjjU', 'CL6N3Ve3', '9Q3Tk7PW', 'Azsxa6HU']

our = {}
with open('output/positions.csv', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        if r['position_id'] in targets:
            our[r['position_id']] = r

our_by_full = {}
for pid, r in our.items():
    our_by_full[r['full_address']] = pid

lp = {}
with open('lpagent_closed_positions.csv', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        parts = r['position_id'].split('...')
        if len(parts) != 2:
            continue
        prefix, suffix = parts
        for fa, pid in our_by_full.items():
            if fa.startswith(prefix) and fa.endswith(suffix):
                lp[pid] = r
                break

for pid in targets:
    o = our[pid]
    l = lp.get(pid)
    our_dep = Decimal(o.get('meteora_deposited', '0') or '0')
    our_fee = Decimal(o.get('meteora_fees', '0') or '0')
    our_pnl = Decimal(o.get('pnl_sol', '0') or '0')
    our_wdr = Decimal(o.get('meteora_withdrawn', '0') or '0')

    print(f"=== {pid} ({o.get('token', '?')}) ===")
    print(f"  Our:    dep={our_dep:.4f}  wdr={our_wdr:.4f}  fee={our_fee:.4f}  pnl={our_pnl:+.4f}")

    if l:
        lp_dep = Decimal(l['sol_deployed'])
        lp_fee_pct = Decimal(l['fee_pct'])
        lp_fee = lp_fee_pct / 100 * lp_dep
        lp_sign = Decimal('-1') if l['pnl_sol'].startswith('-') else Decimal('1')
        lp_pnl = lp_sign * Decimal(l['pnl_pct']) / 100 * lp_dep
        diff = our_pnl - lp_pnl
        fee_diff = our_fee - lp_fee
        print(f"  lpagent: dep={lp_dep:.4f}  fee={lp_fee:.4f} ({l['fee_pct']}%)  pnl={lp_pnl:+.4f} ({l['pnl_pct']}%)")
        print(f"  Diff:   pnl={diff:+.4f}  fee={fee_diff:+.4f}")
        print(f"  lpagent raw: pnl_sol={l['pnl_sol']} apr={l['apr']} strategy={l['strategy']} duration={l['duration']}")
    print()
