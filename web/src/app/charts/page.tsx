'use client';

import { useState, useCallback } from 'react';
import Image from 'next/image';
import FileUpload from '@/components/FileUpload';
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
    <div>
      {/* Hero */}
      <div className="relative h-48 sm:h-64 overflow-hidden">
        <Image
          src="/images/hero-charts.jpg"
          alt="Charts"
          fill
          className="object-cover"
          priority
        />
        <div className="absolute inset-0 bg-gradient-to-b from-black/50 to-black/20 flex items-center justify-center">
          <h1 className="text-4xl font-bold text-white">Charts</h1>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8">
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
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Loaded <strong>{positions.length}</strong> positions from <strong>{fileName}</strong>
                </p>
              </div>
              <button
                onClick={handleClear}
                className="text-sm text-red-600 dark:text-red-400 hover:underline"
              >
                Clear & upload new file
              </button>
            </div>

            <DailyPnlChart positions={positions} />
            <WinRateChart positions={positions} />
            <EntryChart positions={positions} />
            <RugChart positions={positions} />
            <RollingAvgChart positions={positions} window={3} />
            <RollingAvgChart positions={positions} window={7} />
          </>
        )}
      </div>
    </div>
  );
}
