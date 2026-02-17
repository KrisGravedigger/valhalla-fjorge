// TypeScript interfaces ported from Python models.py

export interface OpenEvent {
  timestamp: string;       // "[HH:MM]" or "[YYYY-MM-DDTHH:MM]"
  positionType: string;    // "Spot" / "BidAsk" / "Curve"
  tokenName: string;
  tokenPair: string;       // "TokenName-SOL"
  target: string;          // wallet identifier
  marketCap: number;
  tokenAge: string;        // "5h ago"
  jupScore: number;
  targetSol: number;
  yourSol: number;
  positionId: string;      // "BUeWH73d" (first4+last4 of address)
  txSignatures: string[];
  date: string;            // "YYYY-MM-DD"
}

export interface CloseEvent {
  timestamp: string;
  target: string;
  startingSol: number;
  startingUsd: number;
  endingSol: number;
  endingUsd: number;
  positionId: string;
  txSignatures: string[];
  totalSol: number;
  activePositions: number;
  date: string;
}

export interface RugEvent {
  timestamp: string;
  target: string;
  tokenPair: string;
  positionAddress: string;
  priceDrop: number;
  threshold: number;
  positionId: string | null;
  date: string;
}

export interface FailsafeEvent {
  timestamp: string;
  positionId: string;
  txSignatures: string[];
  date: string;
}

export interface AddLiquidityEvent {
  timestamp: string;
  positionId: string;
  target: string;
  amountSol: number;
  date: string;
}

export interface InsufficientBalanceEvent {
  timestamp: string;
  target: string;
  solBalance: number;
  effectiveBalance: number;
  requiredAmount: number;
  date: string;
}

export interface MatchedPosition {
  targetWallet: string;
  token: string;
  positionType: string;
  solDeployed: number | null;
  solReceived: number | null;
  pnlSol: number | null;
  pnlPct: number | null;
  closeReason: string;
  mcAtOpen: number;
  jupScore: number;
  tokenAge: string;
  tokenAgeDays: number | null;
  tokenAgeHours: number | null;
  priceDropPct: number | null;
  positionId: string;
  fullAddress: string;
  pnlSource: string;        // "discord" for web app
  datetimeOpen: string;      // ISO 8601
  datetimeClose: string;     // ISO 8601
}

// Utility functions

export function shortId(addr: string): string {
  return addr.slice(0, 4) + addr.slice(-4);
}

export function makeIsoDatetime(dateStr: string, timeStr: string): string {
  // Check for full datetime format [YYYY-MM-DDTHH:MM] first
  const fullMatch = timeStr.match(/\[(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})\]/);
  if (fullMatch) {
    return `${fullMatch[1]}T${fullMatch[2]}:${fullMatch[3]}:00`;
  }

  // Fall back to [HH:MM] format
  const timeMatch = timeStr.match(/\[(\d{2}):(\d{2})\]/);
  if (!timeMatch) return '';

  const timePart = `${timeMatch[1]}:${timeMatch[2]}:00`;

  if (dateStr) {
    const dateOnly = dateStr.includes('T') ? dateStr.split('T')[0] : dateStr;
    return `${dateOnly}T${timePart}`;
  }
  return `T${timePart}`;
}

export function normalizeTokenAge(ageStr: string): { days: number | null; hours: number | null } {
  if (!ageStr) return { days: null, hours: null };

  const match = ageStr.trim().match(/^(\d+)(h|d|w|mo|yr)\s*ago$/);
  if (!match) return { days: null, hours: null };

  const value = parseInt(match[1]);
  const unit = match[2];

  switch (unit) {
    case 'h': return { days: value < 24 ? 0 : Math.floor(value / 24), hours: value };
    case 'd': return { days: value, hours: value * 24 };
    case 'w': return { days: value * 7, hours: value * 7 * 24 };
    case 'mo': return { days: value * 30, hours: value * 30 * 24 };
    case 'yr': return { days: value * 365, hours: value * 365 * 24 };
    default: return { days: null, hours: null };
  }
}

export function parseIsoDatetime(dtStr: string): Date | null {
  if (!dtStr || !dtStr.includes('T')) return null;
  const d = new Date(dtStr);
  return isNaN(d.getTime()) ? null : d;
}
