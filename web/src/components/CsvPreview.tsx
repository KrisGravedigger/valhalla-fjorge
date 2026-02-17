'use client';

import { useState, useMemo, useEffect } from 'react';

interface CsvPreviewProps {
  data: Record<string, string>[];
  maxRows?: number;
}

// Key columns to display (subset for readability)
const DISPLAY_COLUMNS = [
  'datetime_open',
  'datetime_close',
  'target_wallet',
  'token',
  'position_type',
  'sol_deployed',
  'pnl_sol',
  'pnl_pct',
  'close_reason',
  'position_id',
  'pnl_source',
];

// Short labels for narrow columns
const COLUMN_LABELS: Record<string, string> = {
  datetime_open: 'Opened',
  datetime_close: 'Closed',
  target_wallet: 'Wallet',
  token: 'Token',
  position_type: 'Type',
  sol_deployed: 'Deployed',
  pnl_sol: 'PnL (SOL)',
  pnl_pct: 'PnL %',
  close_reason: 'Reason',
  position_id: 'ID',
  pnl_source: 'Source',
};

function shortWallet(wallet: string): string {
  if (wallet.includes('_')) return wallet.split('_').pop() || wallet;
  return wallet.length > 9 ? wallet.slice(-9) : wallet;
}

function formatCell(col: string, val: string): string {
  if (!val) return '-';
  if (col === 'target_wallet') return shortWallet(val);
  if (col === 'datetime_open' || col === 'datetime_close') {
    // Show just date + time, drop trailing :00 seconds
    return val.replace(/:00$/, '').replace('T', ' ');
  }
  if (col === 'pnl_pct') return `${val}%`;
  return val;
}

function pnlColor(val: string): string {
  if (!val) return '';
  const n = parseFloat(val);
  if (isNaN(n)) return '';
  if (n > 0) return 'text-green-600 dark:text-green-400';
  if (n < 0) return 'text-red-600 dark:text-red-400';
  return '';
}

export default function CsvPreview({ data, maxRows = 50 }: CsvPreviewProps) {
  const [sortCol, setSortCol] = useState<string>('');
  const [sortAsc, setSortAsc] = useState(false);
  const [page, setPage] = useState(0);

  // Reset pagination when data changes
  useEffect(() => { setPage(0); }, [data]);

  // Determine which columns exist in the data
  const columns = useMemo(() => {
    if (data.length === 0) return [];
    const dataKeys = new Set(Object.keys(data[0]));
    return DISPLAY_COLUMNS.filter(c => dataKeys.has(c));
  }, [data]);

  // Sort
  const sorted = useMemo(() => {
    if (!sortCol) return data;
    return [...data].sort((a, b) => {
      const aVal = a[sortCol] || '';
      const bVal = b[sortCol] || '';
      // Try numeric sort
      const aNum = parseFloat(aVal);
      const bNum = parseFloat(bVal);
      if (!isNaN(aNum) && !isNaN(bNum)) {
        return sortAsc ? aNum - bNum : bNum - aNum;
      }
      return sortAsc ? aVal.localeCompare(bVal) : bVal.localeCompare(aVal);
    });
  }, [data, sortCol, sortAsc]);

  const totalPages = Math.ceil(sorted.length / maxRows);
  const pageData = sorted.slice(page * maxRows, (page + 1) * maxRows);

  const handleSort = (col: string) => {
    if (sortCol === col) {
      setSortAsc(!sortAsc);
    } else {
      setSortCol(col);
      setSortAsc(false);
    }
  };

  if (data.length === 0) {
    return <p className="text-gray-500 dark:text-gray-400 text-center py-4">No data to display</p>;
  }

  return (
    <div>
      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="border-b border-gray-200 dark:border-gray-700">
              {columns.map(col => (
                <th
                  key={col}
                  onClick={() => handleSort(col)}
                  className="px-3 py-2 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer hover:text-gray-700 dark:hover:text-gray-200 whitespace-nowrap"
                >
                  {COLUMN_LABELS[col] || col}
                  {sortCol === col && (sortAsc ? ' ↑' : ' ↓')}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
            {pageData.map((row, i) => (
              <tr key={i} className="hover:bg-gray-50 dark:hover:bg-gray-900">
                {columns.map(col => (
                  <td
                    key={col}
                    className={`px-3 py-2 whitespace-nowrap ${
                      (col === 'pnl_sol' || col === 'pnl_pct') ? pnlColor(row[col]) : ''
                    }`}
                  >
                    {formatCell(col, row[col])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-4 text-sm text-gray-500 dark:text-gray-400">
          <span>{sorted.length} positions</span>
          <div className="flex gap-2">
            <button
              onClick={() => setPage(Math.max(0, page - 1))}
              disabled={page === 0}
              className="px-3 py-1 border border-gray-300 dark:border-gray-600 rounded disabled:opacity-30 hover:bg-gray-100 dark:hover:bg-gray-800"
            >
              Prev
            </button>
            <span className="px-3 py-1">
              {page + 1} / {totalPages}
            </span>
            <button
              onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
              disabled={page >= totalPages - 1}
              className="px-3 py-1 border border-gray-300 dark:border-gray-600 rounded disabled:opacity-30 hover:bg-gray-100 dark:hover:bg-gray-800"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
