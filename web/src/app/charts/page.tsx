'use client';

import { useState, useCallback } from 'react';
import FileUpload from '@/components/FileUpload';
import PageLayout from '@/components/PageLayout';
import { parseCsvString, rowsToPositions } from '@/lib/merger';
import type { MatchedPosition } from '@/lib/types';
import DailyPnlChart from '@/components/charts/DailyPnlChart';
import WinRateChart from '@/components/charts/WinRateChart';
import EntryChart from '@/components/charts/EntryChart';
import RugChart from '@/components/charts/RugChart';
import RollingAvgChart from '@/components/charts/RollingAvgChart';

export default function ChartsPage() {
  const [positions, setPositions] = useState<MatchedPosition[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [fileName, setFileName] = useState<string>('');
  const [hideLegend, setHideLegend] = useState(false);

  const handleFiles = useCallback((files: File[]) => {
    const file = files[0];
    if (!file) return;
    setError(null);

    const reader = new FileReader();
    reader.onload = (e) => {
      const content = e.target?.result as string;
      if (!content) return;

      try {
        const rows = parseCsvString(content);
        const pos = rowsToPositions(rows);
        if (pos.length === 0) {
          setError('No closed positions found in the CSV file.');
          return;
        }
        setPositions(pos);
        setFileName(file.name);
      } catch (err) {
        setError(`Failed to parse CSV: ${err instanceof Error ? err.message : String(err)}`);
      }
    };
    reader.readAsText(file);
  }, []);

  const handleClear = useCallback(() => {
    setPositions(null);
    setError(null);
    setFileName('');
  }, []);

  return (
    <PageLayout
      title="Charts"
      heroImage="/images/hero-charts.jpg"
      navLinks={[
        { label: 'Parse Logs', href: '/parse' },
        { label: 'Merge CSVs', href: '/merge' },
      ]}
    >
      {/* Accuracy warning */}
      <div className="p-4 bg-amber-50 dark:bg-amber-950 border border-amber-200 dark:border-amber-800 rounded-lg text-sm text-amber-800 dark:text-amber-200">
        <strong>Approximate data.</strong> Charts are based on estimated wallet balance changes
        before and after each transaction, which can differ from the actual on-chain PnL.
        For accurate results, use the{' '}
        <a
          href="https://github.com/KrisGravedigger/valhalla-fjorge"
          className="underline font-medium"
        >
          CLI tool
        </a>{' '}
        which fetches real data from the Meteora API.
      </div>

      {!positions ? (
        <>
          <FileUpload
            onFiles={handleFiles}
            accept=".csv"
            label="Drop positions.csv here"
            description="Upload a positions.csv file to generate charts"
          />
          {error && (
            <div className="p-4 bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-300">
              {error}
            </div>
          )}
        </>
      ) : (
        <>
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div>
              <p className="text-sm text-gray-500 dark:text-gray-400">
                Loaded <strong>{positions.length}</strong> positions from <strong>{fileName}</strong>
              </p>
            </div>
            <div className="flex items-center gap-4">
              <label className="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-400 cursor-pointer select-none">
                <input
                  type="checkbox"
                  checked={hideLegend}
                  onChange={(e) => setHideLegend(e.target.checked)}
                  className="w-4 h-4 rounded border-gray-300 dark:border-gray-600"
                />
                Hide followed wallets
              </label>
              <button
                onClick={handleClear}
                className="text-sm text-red-600 dark:text-red-400 hover:underline"
              >
                Clear & upload new file
              </button>
            </div>
          </div>

          <DailyPnlChart positions={positions} hideLegend={hideLegend} />
          <WinRateChart positions={positions} hideLegend={hideLegend} />
          <EntryChart positions={positions} hideLegend={hideLegend} />
          <RugChart positions={positions} hideLegend={hideLegend} />
          <RollingAvgChart positions={positions} window={3} hideLegend={hideLegend} />
          <RollingAvgChart positions={positions} window={7} hideLegend={hideLegend} />
        </>
      )}
    </PageLayout>
  );
}
