// LPAGENT TABLE EXTRACTOR
// Step 1: In Chrome console type: allow pasting  (then Enter)
// Step 2: Paste this entire script and press Enter
// Step 3: It will auto-paginate and download all closed positions as CSV
//
// NOTE: Run this on the lpagent portfolio page with "Closed Positions" tab visible

(async function() {
    const allRows = [];
    let pageNum = 0;

    function extractCurrentPage() {
        // Find the closed positions table (2nd table on page, or largest)
        const tables = document.querySelectorAll('table');
        // Closed positions is usually the last/bottom table
        const table = tables[tables.length - 1];
        if (!table) return [];

        const rows = [];
        for (const tr of table.querySelectorAll('tbody tr')) {
            const cells = tr.querySelectorAll('td');
            if (cells.length < 7) continue;

            // Cell 0: Token pair, pool type, position ID, strategy
            const c0 = cells[0];
            const pairLink = c0.querySelector('a');
            const pair = pairLink ? pairLink.textContent.trim() : '';
            const badges = c0.querySelectorAll('.text-xs');
            let poolType = '', posId = '', strategy = '';
            badges.forEach(b => {
                const t = b.textContent.trim();
                if (t === 'DLMM') poolType = t;
                else if (t.includes('...')) posId = t;
                else if (['Spot', 'BidAsk'].includes(t)) strategy = t;
            });

            // Cell 1: Duration
            const duration = cells[1].textContent.trim();

            // Cell 2: SOL deployed
            const deployed = cells[2].textContent.trim().replace(/[^\d.]/g, '');

            // Cell 3: Fee earned - extract SOL amount and %
            const feeSpans = cells[3].querySelectorAll('span');
            let feeSol = '', feePct = '';
            for (const s of feeSpans) {
                const t = s.textContent.trim();
                if (t.match(/^\d+\.\d+$/) && !feeSol) feeSol = t;
                else if (t.match(/^\d+\.\d+$/) && feeSol && !feePct) feePct = t;
                else if (t === 'SOL') continue;
                else if (t.endsWith('%')) feePct = t.replace('%', '');
            }
            // Simpler: just grab all numbers from fee cell
            const feeNums = cells[3].textContent.match(/[\d.]+/g) || [];
            feeSol = feeNums[0] || '0';
            feePct = feeNums[1] || '0';

            // Cell 4: Total PnL - same structure
            const pnlNums = cells[4].textContent.match(/[\d.]+/g) || [];
            const pnlSol = pnlNums[0] || '0';
            const pnlPct = pnlNums[1] || '0';
            // Check if negative (red color)
            const pnlColor = cells[4].querySelector('[class*="red"]') ? '-' : '+';
            const feeColor = cells[3].querySelector('[class*="red"]') ? '-' : '+';

            // Cell 5: APR
            const aprNums = cells[5].textContent.match(/[\d.]+/g) || [];
            const apr = aprNums[0] || '0';
            const aprSign = cells[5].querySelector('[class*="red"]') ? '-' : '+';

            // Cell 6: Time/date
            const timeAgo = cells[6].textContent.trim();

            rows.push([
                pair, poolType, posId, strategy, duration, deployed,
                feeColor + feeSol, feePct,
                pnlColor + pnlSol, pnlPct,
                aprSign + apr, timeAgo
            ].join(','));
        }
        return rows;
    }

    function clickNextPage() {
        // Find pagination "next" button
        const buttons = document.querySelectorAll('button');
        for (const btn of buttons) {
            // Look for next page button (usually has > or arrow icon, or aria-label)
            if (btn.getAttribute('aria-label') === 'Go to next page' ||
                btn.textContent.trim() === '>' ||
                btn.textContent.trim() === 'Next' ||
                btn.querySelector('svg[class*="chevron-right"]') ||
                btn.querySelector('svg[class*="arrow-right"]')) {
                if (!btn.disabled) {
                    btn.click();
                    return true;
                }
            }
        }
        // Try finding by position (last button in pagination group)
        const nav = document.querySelector('nav[aria-label*="pagination"], [class*="pagination"]');
        if (nav) {
            const navBtns = nav.querySelectorAll('button');
            const lastBtn = navBtns[navBtns.length - 1];
            if (lastBtn && !lastBtn.disabled) {
                lastBtn.click();
                return true;
            }
        }
        return false;
    }

    // Extract first page
    let rows = extractCurrentPage();
    allRows.push(...rows);
    pageNum++;
    console.log(`Page ${pageNum}: ${rows.length} positions`);

    // Auto-paginate
    while (true) {
        if (!clickNextPage()) break;
        await new Promise(r => setTimeout(r, 2000)); // wait for page load
        rows = extractCurrentPage();
        if (rows.length === 0) break;

        // Check for duplicates (same data as last page = no more pages)
        if (rows[0] === allRows[allRows.length - rows.length]) break;

        allRows.push(...rows);
        pageNum++;
        console.log(`Page ${pageNum}: ${rows.length} positions (total: ${allRows.length})`);

        if (pageNum > 100) break; // safety limit
    }

    // Create CSV with header
    const header = 'pair,pool_type,position_id,strategy,duration,sol_deployed,fee_sol,fee_pct,pnl_sol,pnl_pct,apr,time_ago';
    const csv = header + '\n' + allRows.join('\n');

    // Download
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'lpagent_closed_positions.csv';
    a.click();
    URL.revokeObjectURL(url);

    console.log(`Done! Extracted ${allRows.length} positions across ${pageNum} pages`);
    console.log('Downloaded as lpagent_closed_positions.csv');
})();
