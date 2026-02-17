'use client';

import { useState, useCallback } from 'react';
import Image from 'next/image';
import FileUpload from '@/components/FileUpload';
import CsvPreview from '@/components/CsvPreview';
import DownloadButton from '@/components/DownloadButton';
import { parseContent } from '@/lib/readers';
import { parseEvents } from '@/lib/eventParser';
import { matchPositions } from '@/lib/matcher';
import { generatePositionsCsv, downloadCsv } from '@/lib/csv';
import type { MatchedPosition, OpenEvent } from '@/lib/types';

interface ParseState {
  positions: MatchedPosition[];
  unmatchedOpens: OpenEvent[];
  csvString: string;
  previewRows: Record<string, string>[];
  stats: {
    totalMessages: number;
    openEvents: number;
    closeEvents: number;
    rugEvents: number;
    matchedPositions: number;
    stillOpen: number;
  };
}

export default function ParsePage() {
  const [parseState, setParseState] = useState<ParseState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState(false);

  const processContent = useCallback((rawContent: string, filename?: string) => {
    setIsProcessing(true);
    setError(null);

    try {
      // Step 1: Parse messages from content
      const { messages, headerDate } = parseContent(rawContent);

      if (messages.length === 0) {
        setError('No Valhalla bot messages found. Make sure you copied from the correct Discord DM.');
        setIsProcessing(false);
        return;
      }

      // Try to extract date from filename if no header date
      let baseDate = headerDate;
      if (!baseDate && filename) {
        // Try YYYYMMDD pattern in filename
        const dateMatches = filename.match(/(\d{4})(\d{2})(\d{2})/g);
        if (dateMatches && dateMatches.length > 0) {
          const last = dateMatches[dateMatches.length - 1];
          const y = last.slice(0, 4);
          const m = last.slice(4, 6);
          const d = last.slice(6, 8);
          const testDate = new Date(parseInt(y), parseInt(m) - 1, parseInt(d));
          if (testDate.getFullYear() === parseInt(y)) {
            baseDate = `${y}-${m}-${d}`;
          }
        }
      }

      // Step 2: Parse events
      const parseResult = parseEvents(messages, baseDate);

      // Step 3: Match positions
      const { positions, unmatchedOpens } = matchPositions(parseResult);

      // Step 4: Generate CSV
      const csvString = generatePositionsCsv(positions, unmatchedOpens);

      // Step 5: Build preview rows from CSV
      const lines = csvString.split('\n');
      const headers = lines[0].split(',');
      const previewRows = lines.slice(1).filter(l => l.trim()).map(line => {
        // Simple CSV parse (good enough for our generated CSV)
        const values: string[] = [];
        let current = '';
        let inQuotes = false;
        for (const ch of line) {
          if (ch === '"') { inQuotes = !inQuotes; }
          else if (ch === ',' && !inQuotes) { values.push(current); current = ''; }
          else { current += ch; }
        }
        values.push(current);

        const row: Record<string, string> = {};
        headers.forEach((h, i) => { row[h] = values[i] || ''; });
        return row;
      });

      setParseState({
        positions,
        unmatchedOpens,
        csvString,
        previewRows,
        stats: {
          totalMessages: messages.length,
          openEvents: parseResult.openEvents.length,
          closeEvents: parseResult.closeEvents.length,
          rugEvents: parseResult.rugEvents.length,
          matchedPositions: positions.length,
          stillOpen: unmatchedOpens.length,
        },
      });
    } catch (err) {
      setError(`Parse error: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      setIsProcessing(false);
    }
  }, []);

  const handleFiles = useCallback((files: File[]) => {
    const file = files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      const content = e.target?.result as string;
      if (content) processContent(content, file.name);
    };
    reader.readAsText(file);
  }, [processContent]);

  const handlePaste = useCallback((html: string, text: string) => {
    // Prefer HTML (has datetime info from Discord)
    if (html) {
      processContent(html);
    } else if (text) {
      processContent(text);
    }
  }, [processContent]);

  const handleDownload = useCallback(() => {
    if (parseState?.csvString) {
      downloadCsv(parseState.csvString, 'positions.csv');
    }
  }, [parseState]);

  return (
    <div>
      {/* Hero */}
      <div className="relative h-48 sm:h-64 overflow-hidden">
        <Image
          src="/images/hero-parse.jpg"
          alt="Parse logs"
          fill
          className="object-cover"
          priority
        />
        <div className="absolute inset-0 bg-gradient-to-b from-black/50 to-black/20 flex items-center justify-center">
          <h1 className="text-4xl font-bold text-white">Parse Logs</h1>
        </div>
      </div>

      <div className="max-w-6xl mx-auto px-4 sm:px-6 lg:px-8 py-8 space-y-8">
        {/* Input */}
        <FileUpload
          onFiles={handleFiles}
          onPaste={handlePaste}
          showPaste
          accept=".txt,.html,.htm"
          label="Drop log files here"
          description=".txt or .html files from Discord DMs"
        />

        {/* Processing indicator */}
        {isProcessing && (
          <div className="text-center py-4">
            <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-500"></div>
            <p className="mt-2 text-gray-600 dark:text-gray-400">Parsing messages...</p>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="p-4 bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-300">
            {error}
          </div>
        )}

        {/* Results */}
        {parseState && (
          <div className="space-y-6">
            {/* Stats */}
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-6 gap-4">
              {[
                { label: 'Messages', value: parseState.stats.totalMessages },
                { label: 'Opens', value: parseState.stats.openEvents },
                { label: 'Closes', value: parseState.stats.closeEvents },
                { label: 'Rugs', value: parseState.stats.rugEvents },
                { label: 'Matched', value: parseState.stats.matchedPositions },
                { label: 'Still Open', value: parseState.stats.stillOpen },
              ].map(stat => (
                <div key={stat.label} className="bg-gray-50 dark:bg-gray-900 rounded-lg p-3 text-center">
                  <div className="text-2xl font-bold text-gray-900 dark:text-white">{stat.value}</div>
                  <div className="text-xs text-gray-500 dark:text-gray-400">{stat.label}</div>
                </div>
              ))}
            </div>

            {/* PnL source notice */}
            <div className="p-3 bg-yellow-50 dark:bg-yellow-950 border border-yellow-200 dark:border-yellow-800 rounded-lg text-sm text-yellow-700 dark:text-yellow-300">
              PnL source: <strong>discord</strong> (balance diff). For Meteora-enriched PnL, use the CLI tool.
            </div>

            {/* Download */}
            <DownloadButton onClick={handleDownload} label="Download positions.csv" />

            {/* Preview table */}
            <div>
              <h2 className="text-lg font-semibold mb-2">Preview</h2>
              <CsvPreview data={parseState.previewRows} />
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
