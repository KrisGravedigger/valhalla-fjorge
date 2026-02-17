/**
 * Position matcher - matches open/close events and calculates Discord PnL.
 * Web version: always uses Discord PnL (no Meteora).
 */

import type { MatchedPosition, OpenEvent, AddLiquidityEvent } from './types';
import { makeIsoDatetime, normalizeTokenAge } from './types';
import type { ParseResult } from './eventParser';

export interface MatchResult {
  positions: MatchedPosition[];
  unmatchedOpens: OpenEvent[];
}

export function matchPositions(parseResult: ParseResult): MatchResult {
  const {
    openEvents, closeEvents, rugEvents,
    failsafeEvents, addLiquidityEvents
  } = parseResult;

  const positions: MatchedPosition[] = [];
  const matchedIds = new Set<string>();

  // Index opens by position_id
  const openById = new Map<string, OpenEvent>();
  for (const event of openEvents) {
    openById.set(event.positionId, event);
  }

  // Index failsafe events by position_id
  const failsafeIds = new Set(failsafeEvents.map(e => e.positionId));

  // Index add_liquidity events by position_id
  const liquidityById = new Map<string, AddLiquidityEvent[]>();
  for (const event of addLiquidityEvents) {
    const existing = liquidityById.get(event.positionId) || [];
    existing.push(event);
    liquidityById.set(event.positionId, existing);
  }

  // Match closes to opens
  for (const closeEvent of closeEvents) {
    const pid = closeEvent.positionId;
    if (matchedIds.has(pid)) continue;
    matchedIds.add(pid);

    const openEvent = openById.get(pid);

    if (openEvent) {
      let solDeployed = openEvent.yourSol;

      // Add extra liquidity
      for (const liq of (liquidityById.get(pid) || [])) {
        solDeployed += liq.amountSol;
      }

      const solReceived = closeEvent.endingSol - closeEvent.startingSol;
      const pnlSol = solReceived - solDeployed;
      const pnlPct = solDeployed > 0 ? (pnlSol / solDeployed) * 100 : 0;
      const closeReason = failsafeIds.has(pid) ? 'failsafe' : 'normal';
      const age = normalizeTokenAge(openEvent.tokenAge);

      positions.push({
        targetWallet: closeEvent.target,
        token: openEvent.tokenName,
        positionType: openEvent.positionType,
        solDeployed,
        solReceived,
        pnlSol,
        pnlPct,
        closeReason,
        mcAtOpen: openEvent.marketCap,
        jupScore: openEvent.jupScore,
        tokenAge: openEvent.tokenAge,
        tokenAgeDays: age.days,
        tokenAgeHours: age.hours,
        priceDropPct: null,
        positionId: pid,
        fullAddress: '',
        pnlSource: 'discord',
        datetimeOpen: makeIsoDatetime(openEvent.date, openEvent.timestamp),
        datetimeClose: makeIsoDatetime(closeEvent.date, closeEvent.timestamp),
      });
    } else {
      // Close without matching open
      const solReceived = closeEvent.endingSol - closeEvent.startingSol;

      positions.push({
        targetWallet: closeEvent.target,
        token: 'unknown',
        positionType: 'unknown',
        solDeployed: 0,
        solReceived,
        pnlSol: solReceived,
        pnlPct: 0,
        closeReason: 'unknown_open',
        mcAtOpen: 0,
        jupScore: 0,
        tokenAge: '',
        tokenAgeDays: null,
        tokenAgeHours: null,
        priceDropPct: null,
        positionId: pid,
        fullAddress: '',
        pnlSource: 'discord',
        datetimeOpen: '',
        datetimeClose: makeIsoDatetime(closeEvent.date, closeEvent.timestamp),
      });
    }
  }

  // Handle rug events
  for (const rugEvent of rugEvents) {
    const pid = rugEvent.positionId;
    if (!pid || matchedIds.has(pid)) continue;
    matchedIds.add(pid);

    const openEvent = openById.get(pid);

    if (openEvent) {
      let solDeployed = openEvent.yourSol;
      for (const liq of (liquidityById.get(pid) || [])) {
        solDeployed += liq.amountSol;
      }

      const estimatedLoss = solDeployed * rugEvent.priceDrop / 100;
      const pnlSol = -estimatedLoss;
      const pnlPct = -rugEvent.priceDrop;
      const age = normalizeTokenAge(openEvent.tokenAge);

      positions.push({
        targetWallet: rugEvent.target,
        token: openEvent.tokenName,
        positionType: openEvent.positionType,
        solDeployed,
        solReceived: 0,
        pnlSol,
        pnlPct,
        closeReason: 'rug',
        mcAtOpen: openEvent.marketCap,
        jupScore: openEvent.jupScore,
        tokenAge: openEvent.tokenAge,
        tokenAgeDays: age.days,
        tokenAgeHours: age.hours,
        priceDropPct: rugEvent.priceDrop,
        positionId: pid,
        fullAddress: rugEvent.positionAddress || '',
        pnlSource: 'discord',
        datetimeOpen: makeIsoDatetime(openEvent.date, openEvent.timestamp),
        datetimeClose: makeIsoDatetime(rugEvent.date, rugEvent.timestamp),
      });
    } else {
      // Rug without matching open
      positions.push({
        targetWallet: rugEvent.target,
        token: 'unknown',
        positionType: 'unknown',
        solDeployed: 0,
        solReceived: 0,
        pnlSol: 0,
        pnlPct: 0,
        closeReason: 'rug_unknown_open',
        mcAtOpen: 0,
        jupScore: 0,
        tokenAge: '',
        tokenAgeDays: null,
        tokenAgeHours: null,
        priceDropPct: rugEvent.priceDrop,
        positionId: pid,
        fullAddress: rugEvent.positionAddress || '',
        pnlSource: 'discord',
        datetimeOpen: '',
        datetimeClose: makeIsoDatetime(rugEvent.date, rugEvent.timestamp),
      });
    }
  }

  // Handle standalone failsafe events (not already matched via close)
  for (const failsafeEvent of failsafeEvents) {
    const pid = failsafeEvent.positionId;
    if (matchedIds.has(pid)) continue;
    matchedIds.add(pid);

    const openEvent = openById.get(pid);

    if (openEvent) {
      const age = normalizeTokenAge(openEvent.tokenAge);

      positions.push({
        targetWallet: openEvent.target,
        token: openEvent.tokenName,
        positionType: openEvent.positionType,
        solDeployed: null,
        solReceived: null,
        pnlSol: null,
        pnlPct: null,
        closeReason: 'failsafe',
        mcAtOpen: openEvent.marketCap,
        jupScore: openEvent.jupScore,
        tokenAge: openEvent.tokenAge,
        tokenAgeDays: age.days,
        tokenAgeHours: age.hours,
        priceDropPct: null,
        positionId: pid,
        fullAddress: '',
        pnlSource: 'pending',
        datetimeOpen: makeIsoDatetime(openEvent.date, openEvent.timestamp),
        datetimeClose: makeIsoDatetime(failsafeEvent.date, failsafeEvent.timestamp),
      });
    } else {
      positions.push({
        targetWallet: 'unknown',
        token: 'unknown',
        positionType: 'unknown',
        solDeployed: null,
        solReceived: null,
        pnlSol: null,
        pnlPct: null,
        closeReason: 'failsafe_unknown_open',
        mcAtOpen: 0,
        jupScore: 0,
        tokenAge: '',
        tokenAgeDays: null,
        tokenAgeHours: null,
        priceDropPct: null,
        positionId: pid,
        fullAddress: '',
        pnlSource: 'pending',
        datetimeOpen: '',
        datetimeClose: makeIsoDatetime(failsafeEvent.date, failsafeEvent.timestamp),
      });
    }
  }

  // Unmatched opens
  const unmatchedOpens = openEvents.filter(o => !matchedIds.has(o.positionId));

  return { positions, unmatchedOpens };
}
