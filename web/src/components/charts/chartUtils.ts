/**
 * Shared chart utilities for wallet colors and data aggregation.
 */

import type { MatchedPosition } from '@/lib/types';

// Tab10-like color palette (10 distinct colors)
const COLORS = [
  '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
  '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
];

export function getWalletColors(wallets: string[]): Record<string, string> {
  const colors: Record<string, string> = {};
  wallets.forEach((w, i) => {
    colors[w] = COLORS[i % COLORS.length];
  });
  return colors;
}

export function shortWallet(wallet: string): string {
  if (wallet.includes('_')) return wallet.split('_').pop() || wallet;
  return wallet.length > 9 ? wallet.slice(-9) : wallet;
}

export interface DailyDataPoint {
  date: string; // YYYY-MM-DD
  [wallet: string]: number | string | null;
}

/**
 * Aggregate positions into daily data by wallet.
 * Uses datetime_close for PnL/winrate/rugs, datetime_open for entries.
 */
export function aggregateDailyData(
  positions: MatchedPosition[],
  metric: 'pnl' | 'pnlPct' | 'entries' | 'winRate' | 'rugs'
): { data: DailyDataPoint[]; wallets: string[] } {
  // Filter positions with valid data
  const validPositions = positions.filter(p => {
    if (p.pnlSol === null) return false;
    const dateField = metric === 'entries' ? p.datetimeOpen : p.datetimeClose;
    return dateField && dateField.includes('T');
  });

  // Group by (wallet, date)
  const grouped = new Map<string, Map<string, MatchedPosition[]>>();
  const allDates = new Set<string>();
  const allWallets = new Set<string>();

  for (const pos of validPositions) {
    const dateField = metric === 'entries' ? pos.datetimeOpen : pos.datetimeClose;
    const dateStr = dateField.split('T')[0];
    allDates.add(dateStr);
    allWallets.add(pos.targetWallet);

    if (!grouped.has(pos.targetWallet)) grouped.set(pos.targetWallet, new Map());
    const walletMap = grouped.get(pos.targetWallet)!;
    if (!walletMap.has(dateStr)) walletMap.set(dateStr, []);
    walletMap.get(dateStr)!.push(pos);
  }

  const sortedDates = Array.from(allDates).sort();
  const sortedWallets = Array.from(allWallets).sort();

  // Build data points
  const data: DailyDataPoint[] = sortedDates.map(date => {
    const point: DailyDataPoint = { date };

    for (const wallet of sortedWallets) {
      const positions = grouped.get(wallet)?.get(date);
      if (!positions) {
        point[wallet] = null;
        continue;
      }

      switch (metric) {
        case 'pnl':
          point[wallet] = positions.reduce((sum, p) => sum + (p.pnlSol || 0), 0);
          break;
        case 'pnlPct': {
          const totalPnl = positions.reduce((sum, p) => sum + (p.pnlSol || 0), 0);
          const totalDeployed = positions.reduce((sum, p) => sum + (p.solDeployed || 0), 0);
          point[wallet] = totalDeployed > 0 ? (totalPnl / totalDeployed) * 100 : null;
          break;
        }
        case 'entries':
          point[wallet] = positions.length;
          break;
        case 'winRate': {
          const wins = positions.filter(p =>
            p.pnlSol !== null && p.pnlSol > 0 &&
            !['rug', 'rug_unknown_open', 'unknown_open'].includes(p.closeReason)
          ).length;
          point[wallet] = (wins / positions.length) * 100;
          break;
        }
        case 'rugs':
          point[wallet] = positions.filter(p =>
            ['rug', 'rug_unknown_open'].includes(p.closeReason)
          ).length;
          break;
      }
    }

    return point;
  });

  return { data, wallets: sortedWallets };
}

/**
 * Compute rolling average for daily data.
 */
export function computeRollingAvg(
  data: DailyDataPoint[],
  wallets: string[],
  window: number
): DailyDataPoint[] {
  if (data.length < window) return [];

  const result: DailyDataPoint[] = [];

  for (let i = window - 1; i < data.length; i++) {
    const point: DailyDataPoint = { date: data[i].date };

    for (const wallet of wallets) {
      // Collect values in window
      const values: number[] = [];
      for (let j = i - window + 1; j <= i; j++) {
        const val = data[j][wallet];
        if (typeof val === 'number') values.push(val);
      }

      if (values.length >= window) {
        point[wallet] = values.reduce((s, v) => s + v, 0) / values.length;
      } else {
        point[wallet] = null;
      }
    }

    result.push(point);
  }

  return result;
}
