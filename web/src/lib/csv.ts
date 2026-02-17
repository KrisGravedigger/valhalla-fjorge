/**
 * CSV generation for positions and summary.
 * Browser-only: generates CSV strings, no file I/O.
 */

import type { MatchedPosition, OpenEvent } from './types';
import { makeIsoDatetime, normalizeTokenAge } from './types';

const CSV_HEADERS = [
  'datetime_open', 'datetime_close',
  'target_wallet', 'token', 'position_type',
  'sol_deployed', 'sol_received', 'pnl_sol', 'pnl_pct', 'close_reason',
  'mc_at_open', 'jup_score', 'token_age', 'token_age_days', 'token_age_hours',
  'price_drop_pct', 'position_id',
  'full_address', 'pnl_source'
];

function escCsv(val: string): string {
  if (val.includes(',') || val.includes('"') || val.includes('\n')) {
    return `"${val.replace(/"/g, '""')}"`;
  }
  return val;
}

function fmtNum(val: number | null, decimals: number): string {
  if (val === null || val === undefined) return '';
  return val.toFixed(decimals);
}

/**
 * Generate positions CSV string from matched positions and unmatched opens.
 */
export function generatePositionsCsv(
  positions: MatchedPosition[],
  unmatchedOpens: OpenEvent[]
): string {
  const rows: string[] = [CSV_HEADERS.join(',')];

  // Sort: newest datetime_open first
  const sorted = [...positions].sort((a, b) => {
    const aKey = a.datetimeOpen && a.datetimeOpen[0] >= '0' && a.datetimeOpen[0] <= '9' ? a.datetimeOpen : '';
    const bKey = b.datetimeOpen && b.datetimeOpen[0] >= '0' && b.datetimeOpen[0] <= '9' ? b.datetimeOpen : '';
    return bKey.localeCompare(aKey);
  });

  for (const pos of sorted) {
    rows.push([
      escCsv(pos.datetimeOpen),
      escCsv(pos.datetimeClose),
      escCsv(pos.targetWallet),
      escCsv(pos.token),
      escCsv(pos.positionType),
      fmtNum(pos.solDeployed, 4),
      fmtNum(pos.solReceived, 4),
      fmtNum(pos.pnlSol, 4),
      fmtNum(pos.pnlPct, 2),
      escCsv(pos.closeReason),
      fmtNum(pos.mcAtOpen, 2),
      pos.jupScore.toString(),
      escCsv(pos.tokenAge),
      pos.tokenAgeDays !== null ? pos.tokenAgeDays.toString() : '',
      pos.tokenAgeHours !== null ? pos.tokenAgeHours.toString() : '',
      pos.priceDropPct !== null ? fmtNum(pos.priceDropPct, 2) : '',
      escCsv(pos.positionId),
      escCsv(pos.fullAddress),
      escCsv(pos.pnlSource),
    ].join(','));
  }

  // Add still-open positions
  const sortedOpens = [...unmatchedOpens].sort((a, b) => {
    const aDt = makeIsoDatetime(a.date, a.timestamp);
    const bDt = makeIsoDatetime(b.date, b.timestamp);
    const aKey = aDt && aDt[0] >= '0' && aDt[0] <= '9' ? aDt : '';
    const bKey = bDt && bDt[0] >= '0' && bDt[0] <= '9' ? bDt : '';
    return bKey.localeCompare(aKey);
  });

  for (const open of sortedOpens) {
    const datetimeOpen = makeIsoDatetime(open.date, open.timestamp);
    const age = normalizeTokenAge(open.tokenAge);

    rows.push([
      escCsv(datetimeOpen),
      '', // no close
      escCsv(open.target),
      escCsv(open.tokenName),
      escCsv(open.positionType),
      fmtNum(open.yourSol, 4),
      '', // no received
      '', // no pnl
      '', // no pnl %
      'still_open',
      fmtNum(open.marketCap, 2),
      open.jupScore.toString(),
      escCsv(open.tokenAge),
      age.days !== null ? age.days.toString() : '',
      age.hours !== null ? age.hours.toString() : '',
      '', // no price drop
      escCsv(open.positionId),
      '', // no address
      '', // no pnl source
    ].join(','));
  }

  return rows.join('\n');
}

/**
 * Trigger browser download of a CSV string.
 */
export function downloadCsv(csvString: string, filename: string): void {
  const blob = new Blob([csvString], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  URL.revokeObjectURL(url);
}
