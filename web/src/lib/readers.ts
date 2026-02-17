/**
 * Log readers for plain text and HTML Discord DM exports.
 * Browser-only: takes string content, no file I/O.
 */

export interface ParsedMessage {
  timestamp: string;     // "[HH:MM]" or "[YYYY-MM-DDTHH:MM]"
  text: string;          // cleaned message body
  txSignatures: string[];
}

// Regex patterns
const MESSAGE_SPLIT = /^(?=\[(?:\d{4}-\d{2}-\d{2}T)?\d{2}:\d{2}\])/gm;
const AUTHOR_PATTERN = /^\[((?:\d{4}-\d{2}-\d{2}T)?\d{2}:\d{2})\]\s*(.+?):\s*\n/m;
const URL_BRACKET_PATTERN = /\[https?:\/\/[^\]]+\]/g;

/**
 * Convert HTML to clean plain text, extracting links as TEXT [URL].
 * Handles clipboard HTML format with StartFragment/EndFragment markers.
 */
export function htmlToText(rawHtml: string): string {
  let content = rawHtml;

  // Extract CF_HTML fragment if present
  const startIdx = content.indexOf('<!--StartFragment-->');
  const endIdx = content.indexOf('<!--EndFragment-->');
  if (startIdx >= 0 && endIdx > startIdx) {
    content = content.slice(startIdx + '<!--StartFragment-->'.length, endIdx);
  }

  // Replace links: <a href="URL">TEXT</a> -> TEXT [URL]
  content = content.replace(
    /<a\b[^>]*href\s*=\s*["']([^"']+)["'][^>]*>(.*?)<\/a>/gi,
    (_, url, inner) => {
      const text = inner.replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
      return text ? `${text} [${url}]` : `[${url}]`;
    }
  );

  // Convert <time datetime="2026-02-14T20:02:53.534Z">21:02</time> to [YYYY-MM-DDTHH:MM]
  // Must happen BEFORE stripping HTML tags. Converts UTC to local timezone.
  content = content.replace(
    /<time[^>]*datetime="([^"]+)"[^>]*>[\s\S]*?<\/time>/g,
    (_, utcStr) => {
      try {
        const dt = new Date(utcStr);
        if (isNaN(dt.getTime())) return '';
        const y = dt.getFullYear();
        const mo = String(dt.getMonth() + 1).padStart(2, '0');
        const d = String(dt.getDate()).padStart(2, '0');
        const h = String(dt.getHours()).padStart(2, '0');
        const m = String(dt.getMinutes()).padStart(2, '0');
        return `[${y}-${mo}-${d}T${h}:${m}]`;
      } catch {
        return '';
      }
    }
  );

  // Fallback: if no full datetime timestamps were found, try Discord snowflake IDs
  // Snowflake format in HTML: chat-messages-CHANNELID-MESSAGEID or data-list-item-id="chat-messages__SNOWFLAKE"
  if (!content.includes('T') || !/\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}\]/.test(content)) {
    const snowflakeMatch = content.match(/chat-messages-\d+-(\d{17,20})/);
    if (snowflakeMatch) {
      const snowflakeId = BigInt(snowflakeMatch[1]);
      const discordEpoch = BigInt(1420070400000);
      const unixMs = Number((snowflakeId >> BigInt(22)) + discordEpoch);
      const dt = new Date(unixMs);
      const y = dt.getFullYear();
      const mo = String(dt.getMonth() + 1).padStart(2, '0');
      const d = String(dt.getDate()).padStart(2, '0');
      const baseDate = `${y}-${mo}-${d}`;
      // Inject full datetime into time-only markers [HH:MM] -> [YYYY-MM-DDTHH:MM]
      content = content.replace(/\[(\d{2}:\d{2})\]/g, `[${baseDate}T$1]`);
    }
  }

  // HTML decode using browser's built-in decoder
  const textarea = typeof document !== 'undefined' ? document.createElement('textarea') : null;
  if (textarea) {
    textarea.innerHTML = content;
    content = textarea.value;
  } else {
    // Fallback for SSR/testing
    content = content
      .replace(/&amp;/g, '&')
      .replace(/&lt;/g, '<')
      .replace(/&gt;/g, '>')
      .replace(/&quot;/g, '"')
      .replace(/&#39;/g, "'")
      .replace(/&nbsp;/g, ' ');
  }

  // Block elements -> newlines
  content = content.replace(/<\s*br\s*\/?>/gi, '\n');
  content = content.replace(/<\/\s*(div|p|li|tr|h[1-6])\s*>/gi, '\n');

  // Strip remaining HTML tags
  content = content.replace(/<[^>]+>/g, '');

  // Clean whitespace
  content = content.replace(/\n[ \t]+/g, '\n');
  content = content.replace(/[ \t]{2,}/g, ' ');
  content = content.replace(/\n{3,}/g, '\n\n');

  return content.trim();
}

/**
 * Detect if content is HTML or plain text.
 */
export function detectFormat(content: string): 'html' | 'text' {
  const first4k = content.slice(0, 4096);
  if (/<html|<!DOCTYPE|<!--StartFragment|<div|<span|<a\s+href/i.test(first4k)) {
    return 'html';
  }
  return 'text';
}

/**
 * Check for YYYYMMDD date header at the start of content.
 * Returns { date, content } where date is "YYYY-MM-DD" or null.
 */
function extractDateHeader(content: string): { date: string | null; content: string } {
  const lines = content.split('\n');
  if (lines.length > 0 && lines[0].trim()) {
    const firstLine = lines[0].trim();
    if (/^\d{8}$/.test(firstLine)) {
      const year = parseInt(firstLine.slice(0, 4));
      const month = parseInt(firstLine.slice(4, 6));
      const day = parseInt(firstLine.slice(6, 8));
      // Validate date
      const testDate = new Date(year, month - 1, day);
      if (testDate.getFullYear() === year && testDate.getMonth() === month - 1 && testDate.getDate() === day) {
        return {
          date: `${year.toString().padStart(4, '0')}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`,
          content: lines.slice(1).join('\n')
        };
      }
    }
  }
  return { date: null, content };
}

/**
 * Parse messages from text content (plain text format).
 * Filters to only Valhalla bot messages.
 * Returns array of ParsedMessage and optional header date.
 */
export function parseMessages(rawContent: string): { messages: ParsedMessage[]; headerDate: string | null } {
  // Check for date header
  const { date: headerDate, content } = extractDateHeader(rawContent);

  // Split into raw messages
  const rawMessages = content.split(MESSAGE_SPLIT);
  const results: ParsedMessage[] = [];

  for (const rawMsg of rawMessages) {
    if (!rawMsg.trim()) continue;

    // Extract author from first line
    const authorMatch = rawMsg.match(AUTHOR_PATTERN);
    if (!authorMatch) continue;

    const timestampStr = authorMatch[1]; // "15:08" or "2026-02-12T15:08"
    const author = authorMatch[2];

    // Filter: only Valhalla messages
    if (!author.toLowerCase().includes('valhalla')) continue;

    const timestamp = `[${timestampStr}]`;

    // Extract Solscan signatures before stripping URLs
    const txSignatures: string[] = [];
    let txMatch;
    const txRegex = /\[https:\/\/solscan\.io\/tx\/([A-Za-z0-9]+)\]/g;
    while ((txMatch = txRegex.exec(rawMsg)) !== null) {
      txSignatures.push(txMatch[1]);
    }

    // Strip URLs in brackets and the author line prefix
    const text = rawMsg.slice(authorMatch[0].length);
    const cleanText = text.replace(URL_BRACKET_PATTERN, '');

    results.push({ timestamp, text: cleanText, txSignatures });
  }

  return { messages: results, headerDate };
}

/**
 * Parse content that may be HTML or plain text.
 * Auto-detects format and processes accordingly.
 */
export function parseContent(rawContent: string): { messages: ParsedMessage[]; headerDate: string | null } {
  const format = detectFormat(rawContent);

  if (format === 'html') {
    // Check for date header before HTML conversion
    const { date: headerDate, content: htmlContent } = extractDateHeader(rawContent);
    const plainText = htmlToText(htmlContent);
    const result = parseMessages(plainText);
    // Use the header date from the original HTML if found
    return {
      messages: result.messages,
      headerDate: headerDate || result.headerDate
    };
  }

  return parseMessages(rawContent);
}
