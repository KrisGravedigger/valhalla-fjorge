# Valhalla Parser - Vercel Web App Specification

## Overview
Self-service web application for Valhalla Bot users to analyze their position history without installing Python.

**Tech Stack**: Next.js 14 (App Router), Tailwind CSS, Recharts/Chart.js, Vercel hosting

---

## Core Features

### 1. Log Upload & Processing

**Input Methods**:
- **Drag & drop** area for text/HTML files
- **Paste from clipboard** (auto-detects HTML vs plain text)
- **Multi-file upload** for aggregating multiple days

**Format Support**:
- Plain text (`.txt`) - copy-paste from Discord
- HTML (`.html`) - direct Discord clipboard export
- Auto-detection: check for `<html>` or `<!DOCTYPE` tags

**Validation**:
- Max file size: 10MB per file (configurable)
- Max total: 50MB per session
- File type whitelist: `.txt`, `.html`
- Preview first 1000 chars before processing

**Processing Flow**:
```
Upload → Auto-detect format → HTML cleanup (if needed) → Parse → Display results
```

---

### 2. Results Display

**Summary Dashboard**:
- Cards showing key metrics:
  - Total positions | Win rate | Total PnL (SOL)
  - Avg position size | Avg hold time | Rug rate
- Time-windowed stats (tabs):
  - Last 24h | Last 72h | Last 7 days | All time
- Per-target wallet breakdown (if multiple wallets in logs)

**Position Table**:
- Sortable/filterable DataTable (TanStack Table or similar)
- Columns: Token, Type, Entry/Exit time, SOL deployed, PnL SOL, PnL %, Status
- Color-coded: Green (profit), Red (loss), Gray (open)
- Click row → expand with full details (MC, Jup Score, Meteora breakdown)

**Charts** (embedded Recharts components):
1. **Cumulative PnL over time** - line chart
2. **Position volume by strategy** - stacked bar (Spot vs BidAsk)
3. **Win rate trend** - line with 7-day moving average
4. **Rug rate over time** - bar chart

---

### 3. Export/Import Persistence

**Export Format** (`.valhalla.json`):
```json
{
  "version": "1.0",
  "export_timestamp": "2026-02-13T10:30:00Z",
  "positions": [
    {
      "position_id": "6w6YidRv",
      "token": "RATHBUN",
      "open_time": "2026-02-12T03:23:00Z",
      "close_time": "2026-02-12T04:01:00Z",
      "sol_deployed": 2.8,
      "pnl_sol": 0.0547,
      "pnl_pct": 1.96,
      "mc_at_open": 520873.16,
      "jup_score": 75,
      "token_age": "4h ago",
      "position_type": "BidAsk",
      "close_reason": "normal",
      "meteora_pnl": 0.0547
      // ... all fields
    }
  ],
  "summary": {
    "total_positions": 23,
    "win_rate": 0.652,
    "total_pnl_sol": 0.478
    // ... aggregated stats
  },
  "metadata": {
    "date_range": ["2026-02-12", "2026-02-13"],
    "target_wallets": ["20260121_7tB8WHYK", "20260212_BPnaTk"]
  }
}
```

**Import/Merge Workflow**:
1. User uploads new logs
2. Parser detects new positions
3. Before showing results, check if user has saved export
4. UI shows: "Load previous data?" button
5. User uploads `.valhalla.json` → merge with new positions
6. Deduplicate by `position_id`
7. Show combined results with date range indicator

**UI Components**:
- **Export button** (top right): Downloads `.valhalla.json`
- **Import button** (appears when uploading new logs): "Merge with previous data"
- **Clear data** button: Reset session (with confirmation)

**Storage**:
- No server-side persistence (privacy)
- Option to use localStorage for auto-save (opt-in, 5MB limit)
- Clear message: "Data never leaves your device"

---

### 4. Incremental Merging (Advanced)

**Scenario**: User has 7 days of history, wants to add day 8 without re-uploading day 1-7.

**UI Flow**:
```
1. Open app → "Import existing data" section visible
2. Upload previous .valhalla.json
3. Upload new logs (day 8 txt/html)
4. Click "Merge and analyze"
5. Results show:
   - New positions from day 8: +5
   - Updated cumulative stats
   - Charts extend with new data
6. Export updated .valhalla.json
```

**Merge Logic** (client-side JS or API route):
- Parse new logs → generate positions array
- Load previous JSON → extract positions
- Combine arrays
- Deduplicate by `position_id` (keep entry with latest `last_updated` timestamp)
- Recalculate all summary statistics
- Return merged dataset

**Edge Cases**:
- Position appears in both: Use latest data (handle re-runs)
- Date overlap: Merge chronologically
- Different parser versions: Show warning if version mismatch

---

### 5. User Experience Details

**Landing Page**:
- Hero: "Analyze Your Valhalla Bot Performance"
- CTA: "Upload Logs" button
- Features list:
  - ✓ Accurate Meteora PnL
  - ✓ Time-based analytics
  - ✓ Export/import your data
  - ✓ Privacy-first (no data stored)
- Quick start guide (3 steps)

**Privacy Notice** (prominent):
```
🔒 Your data stays private
- All processing happens in your browser (or transiently on server)
- No logs stored on our servers
- No tracking, no cookies beyond essential session
- Open source - verify the code yourself
```

**Loading States**:
- Upload: Progress bar with file name
- Parsing: Spinner with step indicator ("Parsing events... 45/90 messages")
- Meteora API: "Fetching position data... 12/26"

**Error Handling**:
- Invalid file format → Show expected format with example
- Parse errors → Highlight problematic line if possible
- Meteora timeouts → Retry button, fallback to Discord PnL
- Network errors → "Working offline - some features unavailable"

**Responsive Design**:
- Mobile-friendly (charts adapt to small screens)
- Touch-friendly controls
- Progressive Web App (optional): installable, offline-capable

---

### 6. API Routes (Next.js /api/)

**POST /api/parse**
```typescript
// Request
{
  files: File[],           // Uploaded log files
  format: 'auto' | 'txt' | 'html',
  merge_with?: object      // Previous export JSON (optional)
}

// Response
{
  positions: Position[],
  summary: SummaryStats,
  charts_data: ChartsData,
  warnings: string[]       // e.g., "3 positions missing Meteora PnL"
}
```

**Rate Limiting**:
- 10 requests per IP per minute
- Cloudflare rate limiting as backup
- Large file? Queue job and return job_id (polling for status)

**Stateless Design**:
- No database
- Process in-memory, return results immediately
- Optional: Use Vercel Edge Functions for faster cold starts

---

### 7. Technical Implementation Notes

**Parser Execution**:
- **Option A**: Run Python parser via child_process (Vercel supports Python runtime)
- **Option B**: Port parser to TypeScript (more work, but faster, no subprocess overhead)
- **Recommendation**: Start with Option A (Python subprocess), migrate to TS if needed

**Meteora API Calls**:
- Client-side fetch? No - CORS issues
- Server-side (API route)? Yes
- Caching: Cache Meteora responses for 24h (position data is immutable once closed)

**Charts Rendering**:
- Server-side: Generate static PNGs with Puppeteer (slow, overkill)
- Client-side: Recharts/Chart.js with JSON data (recommended)
- Send chart data JSON, render in browser

**Bundle Size Optimization**:
- Code-split: Parse logic separate from landing page
- Lazy-load charts library
- Use dynamic imports for heavy components

---

### 8. Deployment Checklist

**Pre-launch**:
- [ ] Test with 10+ different log formats
- [ ] Verify Meteora API rate limits don't break UX
- [ ] Security audit: XSS from user-uploaded HTML
- [ ] Performance: Test with 1000+ position dataset
- [ ] Mobile testing (iOS Safari, Android Chrome)

**Launch**:
- [ ] Deploy to Vercel (connect to GitHub repo)
- [ ] Custom domain: valhalla-parser.vercel.app (or user's domain)
- [ ] Analytics: Vercel Analytics (privacy-friendly) or none
- [ ] Error monitoring: Sentry or Vercel logs

**Post-launch**:
- [ ] Gather user feedback from Valhalla community
- [ ] Monitor Vercel usage/costs (should be within free tier for moderate traffic)
- [ ] Iterate on UX based on real usage patterns

---

## Future Enhancements (Post-MVP)

- **Multi-wallet comparison**: Upload logs from multiple wallets, compare side-by-side
- **Custom filters**: "Show only BidAsk positions with Jup Score >70"
- **Backtest simulator**: "What if I skipped all tokens <24h old?"
- **API access**: Programmatic access for power users
- **Telegram bot**: Upload logs via Telegram, get analysis
- **Integration with Valhalla Bot**: Direct export button from bot → seamless import

---

## Open Questions

1. **Parser as subprocess vs TS port**: Start with Python subprocess, evaluate performance?
2. **Meteora API proxy**: Need server-side proxy to avoid rate limits hitting users?
3. **Caching strategy**: Cache Meteora position data for closed positions (immutable)?
4. **Monetization**: Free tier with limits, paid for unlimited? (Discuss with Valhalla team)

---

## Success Metrics

- **Adoption**: 100+ active users in first month
- **Retention**: 30% weekly return rate
- **Performance**: P95 parse time <10s for 1 day of logs
- **Reliability**: <1% error rate on valid log files
- **Community**: Mentioned in Valhalla Discord, endorsed by bot creators
