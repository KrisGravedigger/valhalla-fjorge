/**
 * Event parser for Discord bot messages.
 * Ported from Python event_parser.py
 */

import type {
  OpenEvent, CloseEvent, RugEvent, FailsafeEvent,
  AddLiquidityEvent, InsufficientBalanceEvent
} from './types';

import type { ParsedMessage } from './readers';

// Regex patterns
const TARGET_PATTERN = /Target:\s*(\S+)/;
const POSITION_TYPE_PATTERN = /(Spot|BidAsk|Curve)\s+1-Sided Position\s*\|\s*(\S+)-SOL/;
const MARKET_CAP_PATTERN = /MC:\s*\$([\d,]+\.?\d*)/;
const TOKEN_AGE_PATTERN = /Age:\s*(.+?)(?:\n|$)/;
const JUP_SCORE_PATTERN = /Jup Score:\s*(\d+)/;
const YOUR_POS_PATTERN = /Your Pos:.*?SOL:\s*([\d.]+)/;
const TARGET_POS_PATTERN = /Target Pos:.*?SOL:\s*([\d.]+)/;

const OPEN_POSITION_ID_PATTERN = /Opened New DLMM Position!\s*\((\w+)\)/;
const CLOSE_POSITION_ID_PATTERN = /Closed DLMM Position!\s*\((\w+)\)/;
const FAILSAFE_POSITION_ID_PATTERN = /Failsafe Activated \(DLMM\)\s*\((\w+)\)/;
const ADD_LIQUIDITY_POSITION_ID_PATTERN = /Added DLMM Liquidity\s*\((\w+)\)/;
const LIQUIDITY_AMOUNT_PATTERN = /Amount:\s*([\d.]+)\s*SOL/;

const STARTING_SOL_PATTERN = /Starting SOL balance:\s*([\d.]+)\s*SOL\s*\(\$([\d,.]+)\s*USD\)/;
const ENDING_SOL_PATTERN = /Ending SOL balance:\s*([\d.]+)\s*SOL\s*\(\$([\d,.]+)\s*USD\)/;
const TOTAL_SOL_PATTERN = /Total SOL balance:\s*([\d.]+)\s*SOL.*?\((\d+)\s*Active/;

const RUG_TARGET_PATTERN = /Copied From:\s*(\S+)\)/;
const RUG_POSITION_ID_PATTERN = /Rug Check Stop Loss Executed\s*\(DLMM\)\s*\((\w+)\)/;
const PRICE_DROP_PATTERN = /Price Drop:\s*([\d.]+)%/;
const RUG_THRESHOLD_PATTERN = /Rug Check Threshold:\s*([\d.]+)%/;
const POSITION_ADDRESS_PATTERN = /Position:\s*(\S+)/;
const PAIR_PATTERN = /Pair:\s*(\S+)/;

const INSUF_TARGET_PATTERN = /Trade copied from:\s*(\S+)/;
const INSUF_SOL_BALANCE_PATTERN = /Your SOL balance:\s*([\d.]+)\s*SOL/;
const INSUF_EFFECTIVE_PATTERN = /Total effective balance:\s*([\d.]+)\s*SOL/;
const INSUF_REQUIRED_PATTERN = /Required amount for this trade:\s*([\d.]+)\s*SOL/;

export interface ParseResult {
  openEvents: OpenEvent[];
  closeEvents: CloseEvent[];
  rugEvents: RugEvent[];
  failsafeEvents: FailsafeEvent[];
  addLiquidityEvents: AddLiquidityEvent[];
  insufficientBalanceEvents: InsufficientBalanceEvent[];
}

/**
 * Parse all messages into typed events.
 * Handles midnight rollover detection and full datetime timestamps.
 */
export function parseEvents(messages: ParsedMessage[], baseDate: string | null): ParseResult {
  const result: ParseResult = {
    openEvents: [],
    closeEvents: [],
    rugEvents: [],
    failsafeEvents: [],
    addLiquidityEvents: [],
    insufficientBalanceEvents: [],
  };

  let currentDate = baseDate;
  let prevHour: number | null = null;

  for (const { timestamp, text, txSignatures } of messages) {
    // Check if timestamp contains full date [YYYY-MM-DDTHH:MM]
    const fullDtMatch = timestamp.match(/\[(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})\]/);
    if (fullDtMatch) {
      currentDate = fullDtMatch[1];
    } else if (baseDate) {
      // Old [HH:MM] format — midnight rollover detection
      const timeMatch = timestamp.match(/\[(\d{2}):(\d{2})\]/);
      if (timeMatch) {
        const hour = parseInt(timeMatch[1]);
        if (prevHour !== null && (prevHour - hour) > 6) {
          // Crossed midnight
          if (currentDate) {
            const dt = new Date(currentDate + 'T00:00:00');
            dt.setDate(dt.getDate() + 1);
            currentDate = dt.toISOString().split('T')[0];
          }
        }
        prevHour = hour;
      }
    }

    const eventDate = currentDate || '';

    // Skip "already closed" messages
    if (text.includes('was already closed')) continue;

    // Classify and parse
    if (text.includes('Opened New DLMM Position!')) {
      const event = parseOpenEvent(timestamp, text, txSignatures);
      if (event) { event.date = eventDate; result.openEvents.push(event); }
    } else if (text.includes('Closed DLMM Position!')) {
      const event = parseCloseEvent(timestamp, text, txSignatures);
      if (event) { event.date = eventDate; result.closeEvents.push(event); }
    } else if (text.includes('Failsafe Activated (DLMM)')) {
      const event = parseFailsafeEvent(timestamp, text, txSignatures);
      if (event) { event.date = eventDate; result.failsafeEvents.push(event); }
    } else if (text.includes('Added DLMM Liquidity')) {
      const event = parseAddLiquidityEvent(timestamp, text);
      if (event) { event.date = eventDate; result.addLiquidityEvents.push(event); }
    } else if (text.includes('Rug Check Stop Loss Executed')) {
      const event = parseRugEvent(timestamp, text);
      if (event) { event.date = eventDate; result.rugEvents.push(event); }
    } else if (text.includes('Insufficient Effective Balance')) {
      const event = parseInsufficientBalanceEvent(timestamp, text);
      if (event) { event.date = eventDate; result.insufficientBalanceEvents.push(event); }
    }
  }

  return result;
}

function parseOpenEvent(timestamp: string, message: string, txSignatures: string[]): OpenEvent | null {
  const targetMatch = message.match(TARGET_PATTERN);
  const posTypeMatch = message.match(POSITION_TYPE_PATTERN);
  const mcMatch = message.match(MARKET_CAP_PATTERN);
  const ageMatch = message.match(TOKEN_AGE_PATTERN);
  const jupMatch = message.match(JUP_SCORE_PATTERN);
  const yourSolMatch = message.match(YOUR_POS_PATTERN);
  const targetSolMatch = message.match(TARGET_POS_PATTERN);
  const posIdMatch = message.match(OPEN_POSITION_ID_PATTERN);

  if (!targetMatch || !posTypeMatch || !mcMatch || !ageMatch ||
      !jupMatch || !yourSolMatch || !targetSolMatch || !posIdMatch) {
    return null;
  }

  return {
    timestamp,
    positionType: posTypeMatch[1],
    tokenName: posTypeMatch[2],
    tokenPair: `${posTypeMatch[2]}-SOL`,
    target: targetMatch[1],
    marketCap: parseFloat(mcMatch[1].replace(/,/g, '')),
    tokenAge: ageMatch[1].trim(),
    jupScore: parseInt(jupMatch[1]),
    targetSol: parseFloat(targetSolMatch[1]),
    yourSol: parseFloat(yourSolMatch[1]),
    positionId: posIdMatch[1],
    txSignatures,
    date: '',
  };
}

function parseCloseEvent(timestamp: string, message: string, txSignatures: string[]): CloseEvent | null {
  const targetMatch = message.match(TARGET_PATTERN);
  const startMatch = message.match(STARTING_SOL_PATTERN);
  const endMatch = message.match(ENDING_SOL_PATTERN);
  const totalMatch = message.match(TOTAL_SOL_PATTERN);
  const posIdMatch = message.match(CLOSE_POSITION_ID_PATTERN);

  if (!targetMatch || !startMatch || !endMatch || !posIdMatch) {
    return null;
  }

  return {
    timestamp,
    target: targetMatch[1],
    startingSol: parseFloat(startMatch[1]),
    startingUsd: parseFloat(startMatch[2].replace(/,/g, '')),
    endingSol: parseFloat(endMatch[1]),
    endingUsd: parseFloat(endMatch[2].replace(/,/g, '')),
    positionId: posIdMatch[1],
    txSignatures,
    totalSol: totalMatch ? parseFloat(totalMatch[1]) : 0,
    activePositions: totalMatch ? parseInt(totalMatch[2]) : 0,
    date: '',
  };
}

function parseFailsafeEvent(timestamp: string, message: string, txSignatures: string[]): FailsafeEvent | null {
  const posIdMatch = message.match(FAILSAFE_POSITION_ID_PATTERN);
  if (!posIdMatch) return null;

  return {
    timestamp,
    positionId: posIdMatch[1],
    txSignatures,
    date: '',
  };
}

function parseAddLiquidityEvent(timestamp: string, message: string): AddLiquidityEvent | null {
  const posIdMatch = message.match(ADD_LIQUIDITY_POSITION_ID_PATTERN);
  const targetMatch = message.match(TARGET_PATTERN);
  const amountMatch = message.match(LIQUIDITY_AMOUNT_PATTERN);

  if (!posIdMatch || !targetMatch || !amountMatch) return null;

  return {
    timestamp,
    positionId: posIdMatch[1],
    target: targetMatch[1],
    amountSol: parseFloat(amountMatch[1]),
    date: '',
  };
}

function parseRugEvent(timestamp: string, message: string): RugEvent | null {
  const targetMatch = message.match(RUG_TARGET_PATTERN);
  const pairMatch = message.match(PAIR_PATTERN);
  const posMatch = message.match(POSITION_ADDRESS_PATTERN);
  const dropMatch = message.match(PRICE_DROP_PATTERN);
  const thresholdMatch = message.match(RUG_THRESHOLD_PATTERN);
  const posIdMatch = message.match(RUG_POSITION_ID_PATTERN);

  if (!targetMatch || !pairMatch || !posMatch || !dropMatch || !thresholdMatch) {
    return null;
  }

  const positionId = posIdMatch ? posIdMatch[1] : null;

  return {
    timestamp,
    target: targetMatch[1],
    tokenPair: pairMatch[1],
    positionAddress: posMatch[1],
    priceDrop: parseFloat(dropMatch[1]),
    threshold: parseFloat(thresholdMatch[1]),
    positionId,
    date: '',
  };
}

function parseInsufficientBalanceEvent(timestamp: string, message: string): InsufficientBalanceEvent | null {
  const targetMatch = message.match(INSUF_TARGET_PATTERN);
  const solMatch = message.match(INSUF_SOL_BALANCE_PATTERN);
  const effectiveMatch = message.match(INSUF_EFFECTIVE_PATTERN);
  const requiredMatch = message.match(INSUF_REQUIRED_PATTERN);

  if (!targetMatch || !solMatch || !effectiveMatch || !requiredMatch) return null;

  return {
    timestamp,
    target: targetMatch[1],
    solBalance: parseFloat(solMatch[1]),
    effectiveBalance: parseFloat(effectiveMatch[1]),
    requiredAmount: parseFloat(requiredMatch[1]),
    date: '',
  };
}
