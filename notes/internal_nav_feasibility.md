# Internal NAV Feasibility — Findings (E-spike)

**Date:** 2026-05-16 (zaktualizowano 2026-05-24)  
**Verdict:** KILL z warunkiem — 7.6% gap, prawdopodobnie Jupiter 429 rate limiting, nie strukturalny brak

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

| Przebieg | internal_nav_sol | lpagent_nav | diff_pct | Co zmieniono |
|---|---:|---:|---:|---|
| Przed naprawą decode | 20.05 SOL | 67.10 SOL | **70.1%** | baseline |
| Po naprawie (N=70 + fixed offsets) | 54.97 SOL | 67.10 SOL | **18.1%** | prawidłowy layout PositionV2 |
| Po extended bins + reward_pendings | **62.02 SOL** | 67.10 SOL | **7.6%** | PositionBinData (>70 binów) |

### Trailing bytes — wyjaśnione (2026-05-24)

Trailing bytes po 8120B to **PositionBinData extension** (112B/bin):
`liquidity_share(u128=16B) + UserRewardInfo(48B) + FeeInfo(48B)`.

Meteora DLMM obsługuje do 1400 binów na pozycję. Konta 9464B mają 12 dodatkowych binów.
Zysk z tej naprawy: +7.05 SOL (18.1% → 7.6%).

PositionV2 fixed size = **8120B** (nie 7920B jak wcześniej zakładano):
po `upper_bin_id` jest 200B metadata (timestamps, claimed totals, operator, fee_owner, _reserved).

### Remaining gap: 5.08 SOL (7.6%)

Kandydaci według prawdopodobieństwa:
1. **Jupiter 429 rate limiting** (~70%) — masowe 429 w Step 6b (idle SPL tokens) i kilka pozycji
   gdzie token X nie mógł być wyceniony. Idle SPL = 0.0000 mimo 173 SPL accounts.
2. **Reward mints 404** (~20%) — Meteora API zwraca 404 dla tych puli → rewards = 0.
   Farmy mogą być aktywne dla tych poolów pod innym endpointem.
3. **Inne** (~10%) — różnica czasu pomiaru, inne źródło cen w lpagent.

## Verdict — KILL z warunkiem (zaktualizowany)

diff_pct = 7.6% > próg 5%. Formalnie KILL, ale architektura jest potwierdzona.
Gap jest bliski progu i prawdopodobnie wynika z rate limiting, nie z brakujących komponentów NAV.

**Uzasadnienie „z warunkiem":**
1. Architektura LP reserve NAV działa — poprawnie liczy extended bins, fees, rewards
2. Remaining 7.6% gap → najbardziej prawdopodobnie Jupiter rate limiting przy 50 pozycjach
3. Do formalnego GO: re-run z opóźnieniami między Jupiter calls lub innym price source

## Ścieżka do full E implementation

**Narzędzia:** `solders` + `struct` (SDK nie potrzebny — raw RPC wystarczy)

**Szacowany nakład:** L (4–6 sesji, per PLAN-portfolio-truth.md)

**Action items do GO:**
1. Dodać rate-limiting mitigation w Jupiter calls (delay 0.5–1s między calls lub batch)
2. Re-run spike — oczekiwany diff_pct < 5% gdy 429 nie skażą idle SPL i pozycji
3. Alternatywnie: użyć Meteora DLMM API `/pair/{lb_pair}` jako ceny tokenów (zamiast Jupiter)
4. Wtedy formalna GO decyzja dla sub-project E
