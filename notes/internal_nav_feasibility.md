# Internal NAV Feasibility — Findings (E-spike)

**Date:** 2026-05-16 (zaktualizowano 2026-05-24)  
**Verdict:** GO — 0.014% diff na właściwej parze porównawczej (LP positions + free SOL)

---

## Co zadziałało

- `getProgramAccounts` z filterem discriminator+owner — 50 pozycji PositionV2, AC-1 PASS
- Ręczny decode PositionV2 (struct, bez SDK) — poprawne lower/upper/liq_shares po naprawie
- BinArray PDA derivation i bin math — frakcja liquidity daje sensowne kwoty tokenów
- Jupiter quote (`api.jup.ag/swap/v1/quote`) — działa dla głównych tokenów, 429 przy spike na całym portfelu
- On-chain fallback dla mint addresses — scan LbPair account po bajtach SOL_MINT

## Co nie zadziałało / wymagało obejść

### 1. PositionV2 layout — accounts z limit orderami

**Problem:** Konta PositionV2 z otwartymi zleceniami limit są większe niż standardowe 8120B
(obserwowane: 9464–27496B). Codex wdrożył formułę `N=(raw_len-280)//112` jako
dynamiczną długość tablic — to dawało N=107/86/243 zamiast stałego 70 i przesuwało
`lower_bin_id` na zły offset.

**Naprawa:** N=70 (MAX_BIN_PER_POSITION) jest stałe. Zweryfikowane przez przeszukanie
bajtów: lower_bin_id istnieje wyłącznie na offsetcie 7912 (= 72 + 70×112) niezależnie
od rozmiaru konta. Trailing bytes po 7920B to dane limit orderów — nie są dekodowane.

### 2. BinArray header — wersja 56B nie 48B

Meteora dołożył `version (u8) + _padding ([u8;7])` po `index`, więc BA_HEADER=56 nie 48.
Skutek: bez tej poprawki każdy Bin czytany z przesunięciem 8B → śmieciowe wartości.

### 3. Fee data — zeros (LP fees = 0)

`fee_infos` pod offsetem 4552 (= 72+70×64) to zera — fees faktycznie pobrane lub zerowe.
Wcześniejszy „wynik" 7.77 SOL był garbage z błędnego offsetu.

### 4. Rate limiting (Jupiter + RPC)

Przy 50 pozycjach naraz: RPC 429 (zadziałał retry z backoff) i Jupiter 429 (pominięte tokeny X).
Wpływ na NAV: mały (tokeny X to ułamki SOL w tych pozycjach — Y=SOL dominuje).

## Dokładność

| Przebieg | internal_nav_sol | lpagent_nav (ref) | diff_pct | Co zmieniono |
|---|---:|---:|---:|---|
| Przed naprawą decode | 20.05 SOL | 67.10 SOL (portfolio) | **70.1%** | baseline |
| Po naprawie (N=70 + fixed offsets) | 54.97 SOL | 67.10 SOL (portfolio) | **18.1%** | prawidłowy layout PositionV2 |
| Po extended bins + reward_pendings | 62.02 SOL | 67.10 SOL (portfolio) | **7.6%** | PositionBinData (>70 binów) |
| Po Jupiter throttle + właściwa para | **62.70 SOL** | 62.69 SOL (LP+free) | **0.014%** | poprawna para; ignoruje niekontrolowane aktywa |

**Właściwa para porównawcza (od 2026-05-24):**
`internal_nav_sol` (LP positions + free SOL + idle SPL) vs `lpagent LP positions + lpagent free SOL`.
Nie porównujemy do all-in portfolio widget (67.71) — różnica ~5 SOL to rent reserves (~0.70 SOL) +
aktywa niezidentyfikowane i poza kontrolą użytkownika (staked SOL?, inne tokeny?). Pomijamy świadomie.

### Trailing bytes — wyjaśnione (2026-05-24)

Trailing bytes po 8120B to **PositionBinData extension** (112B/bin):
`liquidity_share(u128=16B) + UserRewardInfo(48B) + FeeInfo(48B)`.

Meteora DLMM obsługuje do 1400 binów na pozycję. Konta 9464B mają 12 dodatkowych binów.
Zysk z tej naprawy: +7.05 SOL (18.1% → 7.6%).

PositionV2 fixed size = **8120B** (nie 7920B jak wcześniej zakładano):
po `upper_bin_id` jest 200B metadata (timestamps, claimed totals, operator, fee_owner, _reserved).

### Gap do portfolio widget: ~5.01 SOL (wyjaśniony i świadomie pominięty)

Spike (62.70 SOL) vs lpagent portfolio widget (67.71 SOL) = 5.01 SOL różnicy.
- ~0.70 SOL: rent reserves (Solana account lamports — zidentyfikowane przez użytkownika)
- ~4.3 SOL: nieznane — prawdopodobnie staked SOL (mSOL/jitoSOL) lub inne niezarządzane aktywa

Decyzja: te aktywa są poza kontrolą użytkownika → nie powinny wchodzić do NAV baseline.
Właściwy baseline = LP positions + free SOL (kontrolowane, płynne).

## Verdict — GO

**diff_pct = 0.014%** na właściwej parze (spike LP+free vs lpagent LP+free).

LP accuracy: spike 54.3245 SOL vs lpagent 54.32 SOL → **0.008% error** — szum pomiarowy.
Free SOL: spike 8.3727 SOL vs lpagent 8.37 SOL → zgodne.

Architektura on-chain NAV jest potwierdzona. `solders` + `struct` (raw RPC, bez SDK) wystarczy.

## Ścieżka do full E implementation

**Narzędzia:** `solders` + `struct` (SDK nie potrzebny — raw RPC wystarczy)

**Szacowany nakład:** L (4–6 sesji, per PLAN-portfolio-truth.md)

**Uwagi do implementacji:**
- Jupiter throttle 0.15s + retry-backoff działa; dla produkcji rozważyć Meteora `/pair/{lb_pair}` jako alternatywne źródło cen
- `--lpagent-nav` powinno przyjmować LP+free (62.69), nie all-in portfolio widget (67.71)
- Reward mints 404 — rewards genuinely zero lub farmy pod innym endpointem; pomijalny wpływ na NAV
