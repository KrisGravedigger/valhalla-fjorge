'use client';

import { useCallback, useState, useRef, DragEvent, ClipboardEvent } from 'react';

interface FileUploadProps {
  onFiles: (files: File[]) => void;
  onPaste?: (html: string, text: string) => void;
  accept?: string;
  multiple?: boolean;
  showPaste?: boolean;
  label?: string;
  description?: string;
}

export default function FileUpload({
  onFiles,
  onPaste,
  accept,
  multiple = false,
  showPaste = false,
  label = 'Drop files here',
  description = 'or click to browse',
}: FileUploadProps) {
  const [isDragOver, setIsDragOver] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  const handleDrop = useCallback((e: DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    const files = Array.from(e.dataTransfer.files);
    if (files.length > 0) onFiles(files);
  }, [onFiles]);

  const handleDragOver = useCallback((e: DragEvent) => {
    e.preventDefault();
    setIsDragOver(true);
  }, []);

  const handleDragLeave = useCallback(() => {
    setIsDragOver(false);
  }, []);

  const handleFileInput = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const files = Array.from(e.target.files || []);
    if (files.length > 0) onFiles(files);
    // Reset input so same file can be selected again
    e.target.value = '';
  }, [onFiles]);

  const handlePaste = useCallback((e: ClipboardEvent<HTMLTextAreaElement>) => {
    if (!onPaste) return;
    const html = e.clipboardData.getData('text/html');
    const text = e.clipboardData.getData('text/plain');
    if (html || text) {
      e.preventDefault();
      onPaste(html, text);
    }
  }, [onPaste]);

  return (
    <div className="space-y-4">
      {/* File drop zone */}
      <div
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        onDragLeave={handleDragLeave}
        onClick={() => inputRef.current?.click()}
        className={`
          border-2 border-dashed rounded-xl p-8 text-center cursor-pointer transition-all
          ${isDragOver
            ? 'border-blue-500 bg-blue-50 dark:bg-blue-950'
            : 'border-gray-300 dark:border-gray-700 hover:border-gray-400 dark:hover:border-gray-600'
          }
        `}
      >
        <div className="text-4xl mb-2">📁</div>
        <p className="text-lg font-medium text-gray-700 dark:text-gray-300">{label}</p>
        <p className="text-sm text-gray-500 dark:text-gray-400">{description}</p>
        <input
          ref={inputRef}
          type="file"
          accept={accept}
          multiple={multiple}
          onChange={handleFileInput}
          className="hidden"
        />
      </div>

      {/* Paste area */}
      {showPaste && (
        <div>
          <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
            Or paste Discord DMs here (Ctrl+V)
          </label>
          <textarea
            onPaste={handlePaste}
            placeholder="Select messages in Discord, copy (Ctrl+C), then paste here (Ctrl+V)..."
            className="w-full h-40 p-4 border border-gray-300 dark:border-gray-700 rounded-lg bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 placeholder-gray-400 dark:placeholder-gray-500 focus:ring-2 focus:ring-blue-500 focus:border-transparent resize-y font-mono text-sm"
          />
          <p className="text-xs text-gray-500 dark:text-gray-400 mt-1">
            Tip: Copy from Discord preserves HTML formatting for better datetime extraction.
          </p>
        </div>
      )}
    </div>
  );
}
