/**
 * CSV merge and deduplication.
 * Browser-only: processes CSV strings using Papa Parse.
 */

import Papa from 'papaparse';
import type { MatchedPosition } from './types';

export interface CsvRow {
  [key: string]: string;
}

/**
 * Parse CSV string into rows using Papa Parse.
 */
export function parseCsvString(csvString: string): CsvRow[] {
  const result = Papa.parse<CsvRow>(csvString, {
    header: true,
    skipEmptyLines: true,
  });
  return result.data;
}

/**
 * Merge multiple CSV row arrays, deduplicating by position_id.
 * Later files take priority (overwrite earlier duplicates).
 */
export function mergeCsvRows(rowArrays: CsvRow[][]): CsvRow[] {
  const seenIds = new Map<string, CsvRow>();
  const noIdRows: CsvRow[] = [];

  for (const rows of rowArrays) {
    for (const row of rows) {
      const positionId = (row.position_id || '').trim();
      if (!positionId) {
        noIdRows.push(row);
      } else {
        seenIds.set(positionId, row);
      }
    }
  }

  return [...noIdRows, ...Array.from(seenIds.values())];
}

/**
 * Convert merged CSV rows back to CSV string.
 */
export function rowsToCsv(rows: CsvRow[]): string {
  if (rows.length === 0) return '';

  // Use the headers from the first row
  const headers = Object.keys(rows[0]);
  return Papa.unparse(rows, { columns: headers });
}

/**
 * Convert CSV rows to MatchedPosition objects for chart rendering.
 * Skips still_open positions (no PnL data).
 */
export function rowsToPositions(rows: CsvRow[]): MatchedPosition[] {
  const positions: MatchedPosition[] = [];

  for (const row of rows) {
    if (row.close_reason === 'still_open') continue;

    const parseNum = (val: string | undefined): number | null => {
      if (!val || val.trim() === '') return null;
      const n = parseFloat(val);
      return isNaN(n) ? null : n;
    };

    const parseInt_ = (val: string | undefined): number => {
      if (!val || val.trim() === '') return 0;
      const n = parseInt(val);
      return isNaN(n) ? 0 : n;
    };

    const parseOptInt = (val: string | undefined): number | null => {
      if (!val || val.trim() === '') return null;
      const n = parseInt(val);
      return isNaN(n) ? null : n;
    };

    positions.push({
      targetWallet: row.target_wallet || '',
      token: row.token || '',
      positionType: row.position_type || '',
      solDeployed: parseNum(row.sol_deployed),
      solReceived: parseNum(row.sol_received),
      pnlSol: parseNum(row.pnl_sol),
      pnlPct: parseNum(row.pnl_pct),
      closeReason: row.close_reason || '',
      mcAtOpen: parseFloat(row.mc_at_open || '0') || 0,
      jupScore: parseInt_(row.jup_score),
      tokenAge: row.token_age || '',
      tokenAgeDays: parseOptInt(row.token_age_days),
      tokenAgeHours: parseOptInt(row.token_age_hours),
      priceDropPct: parseNum(row.price_drop_pct),
      positionId: row.position_id || '',
      fullAddress: row.full_address || '',
      pnlSource: row.pnl_source || 'pending',
      datetimeOpen: row.datetime_open || '',
      datetimeClose: row.datetime_close || '',
    });
  }

  return positions;
}
