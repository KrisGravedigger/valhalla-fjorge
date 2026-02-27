"""Cross-reference lpagent CSV vs our positions.csv"""
import csv
from decimal import Decimal

# Load data
lp_rows = []
with open('lpagent_closed_positions.csv', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        lp_rows.append(r)

our_rows = []
with open('output/positions.csv', encoding='utf-8') as f:
    for r in csv.DictReader(f):
        our_rows.append(r)

# Build our index by full_address
our_by_full = {}
for r in our_rows:
    fa = r.get('full_address', '')
    if fa:
        our_by_full[fa] = r

# Match and compare
matched_pairs = []
unmatched_lp = []
matched_our_fas = set()

for lp in lp_rows:
    parts = lp['position_id'].split('...')
    if len(parts) != 2:
        unmatched_lp.append(lp)
        continue
    prefix, suffix = parts
    found_fa = None
    for fa in our_by_full:
        if fa.startswith(prefix) and fa.endswith(suffix):
            found_fa = fa
            break
    if found_fa:
        matched_pairs.append((lp, our_by_full[found_fa]))
        matched_our_fas.add(found_fa)
    else:
        unmatched_lp.append(lp)

# Positions in OUR data but NOT in lpagent
phantom_ours = []
for fa, r in our_by_full.items():
    if fa not in matched_our_fas:
        phantom_ours.append(r)

print(f'=== MATCHING SUMMARY ===')
print(f'Matched:           {len(matched_pairs)}')
print(f'In lpagent only:   {len(unmatched_lp)}')
print(f'In our data only:  {len(phantom_ours)}')
print()

# Analyze matched pairs
total_lp_pnl_pct = Decimal('0')
total_lp_pnl_sol = Decimal('0')
total_lp_fee_pct = Decimal('0')
total_our_pnl = Decimal('0')
total_our_fee = Decimal('0')
total_our_dep = Decimal('0')

diffs = []

for lp, our in matched_pairs:
    lp_dep = Decimal(lp['sol_deployed'])
    lp_pnl_pct_val = Decimal(lp['pnl_pct'])
    lp_fee_pct_val = Decimal(lp['fee_pct'])

    # Sign from pnl_sol field (has +/- prefix)
    pnl_sol_str = lp['pnl_sol']
    lp_sign = Decimal('-1') if pnl_sol_str.startswith('-') else Decimal('1')

    fee_sol_str = lp['fee_sol']
    lp_fee_sign = Decimal('-1') if fee_sol_str.startswith('-') else Decimal('1')

    # Calculate precise PnL from percentage
    lp_pnl_precise = lp_sign * lp_pnl_pct_val / 100 * lp_dep
    lp_fee_precise = lp_fee_sign * lp_fee_pct_val / 100 * lp_dep

    # Also get raw SOL value
    lp_pnl_raw = Decimal(pnl_sol_str)

    # Our values
    our_pnl = Decimal(our.get('pnl_sol', '0') or '0')
    our_fee = Decimal(our.get('meteora_fees', '0') or '0')
    our_dep = Decimal(our.get('meteora_deposited', our.get('sol_deployed', '0')) or '0')

    total_lp_pnl_pct += lp_pnl_precise
    total_lp_pnl_sol += lp_pnl_raw
    total_lp_fee_pct += lp_fee_precise
    total_our_pnl += our_pnl
    total_our_fee += our_fee
    total_our_dep += our_dep

    diff = our_pnl - lp_pnl_precise
    diffs.append((our['position_id'], our.get('token', '?'), our_pnl, lp_pnl_precise, diff, our_dep, lp_dep))

print(f'=== AGGREGATE TOTALS (matched positions only) ===')
print(f'lpagent PnL (from %):   {total_lp_pnl_pct:.4f} SOL')
print(f'lpagent PnL (from SOL): {total_lp_pnl_sol:.4f} SOL')
print(f'Our PnL:                {total_our_pnl:.4f} SOL')
print(f'Difference (ours-lp%):  {(total_our_pnl - total_lp_pnl_pct):.4f} SOL')
print()
print(f'lpagent fees (from %):  {total_lp_fee_pct:.4f} SOL')
print(f'Our fees:               {total_our_fee:.4f} SOL')
print(f'Fee difference:         {(total_our_fee - total_lp_fee_pct):.4f} SOL')
print()

# Sort by biggest diff
diffs.sort(key=lambda x: abs(x[4]), reverse=True)
print(f'=== TOP 30 BIGGEST PNL DIFFERENCES ===')
print(f'{"pos_id":>10s} {"token":>15s} {"our_pnl":>10s} {"lp_pnl":>10s} {"diff":>10s} {"our_dep":>8s} {"lp_dep":>8s}')
for pid, tok, our_p, lp_p, d, dep, lp_d in diffs[:30]:
    print(f'{pid:>10s} {tok:>15s} {our_p:>10.4f} {lp_p:>10.4f} {d:>10.4f} {dep:>8.4f} {lp_d:>8.4f}')
print()

# Distribution of differences
import collections
buckets = collections.Counter()
for _, _, _, _, d, _, _ in diffs:
    ad = abs(d)
    if ad < Decimal('0.001'):
        buckets['< 0.001'] += 1
    elif ad < Decimal('0.01'):
        buckets['0.001-0.01'] += 1
    elif ad < Decimal('0.05'):
        buckets['0.01-0.05'] += 1
    elif ad < Decimal('0.1'):
        buckets['0.05-0.1'] += 1
    elif ad < Decimal('0.5'):
        buckets['0.1-0.5'] += 1
    else:
        buckets['>= 0.5'] += 1

print(f'=== DIFFERENCE DISTRIBUTION ===')
for bucket in ['< 0.001', '0.001-0.01', '0.01-0.05', '0.05-0.1', '0.1-0.5', '>= 0.5']:
    print(f'  {bucket:>15s}: {buckets[bucket]:>5d} positions')
print()

# Direction analysis
our_higher = sum(1 for _, _, _, _, d, _, _ in diffs if d > Decimal('0.001'))
our_lower = sum(1 for _, _, _, _, d, _, _ in diffs if d < Decimal('-0.001'))
close = sum(1 for _, _, _, _, d, _, _ in diffs if abs(d) <= Decimal('0.001'))
print(f'=== DIRECTION ===')
print(f'Our PnL higher:  {our_higher}')
print(f'Our PnL lower:   {our_lower}')
print(f'Close (< 0.001): {close}')
print()

# Sum of positive diffs vs negative diffs
pos_sum = sum(d for _, _, _, _, d, _, _ in diffs if d > 0)
neg_sum = sum(d for _, _, _, _, d, _, _ in diffs if d < 0)
print(f'Sum of positive diffs (we higher): {pos_sum:.4f} SOL')
print(f'Sum of negative diffs (we lower):  {neg_sum:.4f} SOL')
print(f'Net difference:                    {(pos_sum + neg_sum):.4f} SOL')
print()

# Phantom positions analysis
phantom_pnl = Decimal('0')
phantom_meteora = 0
phantom_non_meteora = 0
for r in phantom_ours:
    src = r.get('pnl_source', '')
    pnl = Decimal(r.get('pnl_sol', '0') or '0')
    phantom_pnl += pnl
    if src == 'meteora':
        phantom_meteora += 1
    else:
        phantom_non_meteora += 1

print(f'=== PHANTOM POSITIONS (in our data, NOT in lpagent) ===')
print(f'Count:       {len(phantom_ours)}')
print(f'  meteora:   {phantom_meteora}')
print(f'  other:     {phantom_non_meteora}')
print(f'Total PnL:   {phantom_pnl:.4f} SOL')
print()

# Show phantom positions with biggest PnL
phantoms_sorted = sorted(phantom_ours, key=lambda r: abs(Decimal(r.get('pnl_sol', '0') or '0')), reverse=True)
print(f'Top 20 phantom positions by |PnL|:')
print(f'{"pos_id":>10s} {"token":>15s} {"pnl":>10s} {"dep":>8s} {"source":>10s} {"close":>20s}')
for r in phantoms_sorted[:20]:
    pnl_val = r.get('pnl_sol', '0') or '0'
    dep_val = r.get('sol_deployed', '0') or '0'
    print(f'{r["position_id"]:>10s} {r.get("token","?"):>15s} {Decimal(pnl_val):>10.4f} {Decimal(dep_val):>8.4f} {r.get("pnl_source","?"):>10s} {r.get("datetime_close","?"):>20s}')
