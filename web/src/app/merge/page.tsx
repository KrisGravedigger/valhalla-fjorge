'use client';

import { useState, useCallback } from 'react';
import FileUpload from '@/components/FileUpload';
import CsvPreview from '@/components/CsvPreview';
import DownloadButton from '@/components/DownloadButton';
import PageLayout from '@/components/PageLayout';
import { parseCsvString, mergeCsvRows, rowsToCsv } from '@/lib/merger';
import type { CsvRow } from '@/lib/merger';
import { downloadCsv } from '@/lib/csv';

interface MergeState {
  files: { name: string; rowCount: number }[];
  mergedRows: CsvRow[];
  csvString: string;
  stats: {
    totalBefore: number;
    totalAfter: number;
    duplicatesRemoved: number;
  };
}

export default function MergePage() {
  const [uploadedFiles, setUploadedFiles] = useState<{ name: string; rows: CsvRow[] }[]>([]);
  const [mergeState, setMergeState] = useState<MergeState | null>(null);
  const [error, setError] = useState<string | null>(null);

  const handleFiles = useCallback((files: File[]) => {
    setError(null);

    const newFiles: { name: string; rows: CsvRow[] }[] = [];
    let loaded = 0;

    for (const file of files) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const content = e.target?.result as string;
        if (content) {
          const rows = parseCsvString(content);
          newFiles.push({ name: file.name, rows });
        }
        loaded++;
        if (loaded === files.length) {
          setUploadedFiles(prev => [...prev, ...newFiles]);
        }
      };
      reader.readAsText(file);
    }
  }, []);

  const handleMerge = useCallback(() => {
    if (uploadedFiles.length === 0) return;
    setError(null);

    try {
      const rowArrays = uploadedFiles.map(f => f.rows);
      const totalBefore = rowArrays.reduce((sum, arr) => sum + arr.length, 0);
      const mergedRows = mergeCsvRows(rowArrays);
      const csvString = rowsToCsv(mergedRows);
      const totalAfter = mergedRows.length;

      setMergeState({
        files: uploadedFiles.map(f => ({ name: f.name, rowCount: f.rows.length })),
        mergedRows,
        csvString,
        stats: {
          totalBefore,
          totalAfter,
          duplicatesRemoved: totalBefore - totalAfter,
        },
      });
    } catch (err) {
      setError(`Merge error: ${err instanceof Error ? err.message : String(err)}`);
    }
  }, [uploadedFiles]);

  const handleDownload = useCallback(() => {
    if (mergeState?.csvString) {
      downloadCsv(mergeState.csvString, 'positions_merged.csv');
    }
  }, [mergeState]);

  const handleClear = useCallback(() => {
    setUploadedFiles([]);
    setMergeState(null);
    setError(null);
  }, []);

  return (
    <PageLayout
      title="Merge CSVs"
      heroImage="/images/hero-merge.jpg"
      navLinks={[
        { label: 'Parse Logs', href: '/parse' },
        { label: 'Charts', href: '/charts' },
      ]}
    >
      {/* Upload */}
      <FileUpload
        onFiles={handleFiles}
        accept=".csv"
        multiple
        label="Drop positions.csv files here"
        description="Upload multiple CSV files to merge and deduplicate"
      />

      {/* Uploaded files list */}
      {uploadedFiles.length > 0 && (
        <div className="space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="text-lg font-semibold">Uploaded Files</h2>
            <button
              onClick={handleClear}
              className="text-sm text-red-600 dark:text-red-400 hover:underline"
            >
              Clear all
            </button>
          </div>

          <div className="space-y-2">
            {uploadedFiles.map((file, i) => (
              <div
                key={i}
                className="flex items-center justify-between p-3 bg-gray-50 dark:bg-gray-900 rounded-lg"
              >
                <span className="font-mono text-sm">{file.name}</span>
                <span className="text-sm text-gray-500 dark:text-gray-400">
                  {file.rows.length} positions
                </span>
              </div>
            ))}
          </div>

          <button
            onClick={handleMerge}
            className="px-6 py-2.5 bg-green-600 hover:bg-green-700 text-white font-medium rounded-lg transition-colors"
          >
            Merge {uploadedFiles.length} file{uploadedFiles.length > 1 ? 's' : ''}
          </button>
        </div>
      )}

      {/* Error */}
      {error && (
        <div className="p-4 bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-300">
          {error}
        </div>
      )}

      {/* Results */}
      {mergeState && (
        <div className="space-y-6">
          {/* Stats */}
          <div className="grid grid-cols-3 gap-4">
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-gray-900 dark:text-white">
                {mergeState.stats.totalBefore}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Total Before</div>
            </div>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-green-600 dark:text-green-400">
                {mergeState.stats.totalAfter}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">After Merge</div>
            </div>
            <div className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
              <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">
                {mergeState.stats.duplicatesRemoved}
              </div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Duplicates Removed</div>
            </div>
          </div>

          {/* Download */}
          <DownloadButton onClick={handleDownload} label="Download positions_merged.csv" />

          {/* Preview */}
          <div>
            <h2 className="text-lg font-semibold mb-2">Preview</h2>
            <CsvPreview data={mergeState.mergedRows as Record<string, string>[]} />
          </div>
        </div>
      )}
    </PageLayout>
  );
}
