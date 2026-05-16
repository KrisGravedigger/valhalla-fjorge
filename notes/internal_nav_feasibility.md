# Internal NAV Feasibility — Findings (E-spike)

**Date:** 2026-05-16  
**Verdict:** KILL z warunkiem — architektura działa, ale brakuje dekodowania limit orderów

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

| Przebieg | internal_nav_sol | lpagent_nav | diff_pct |
|---|---:|---:|---:|
| Przed naprawą decode | 20.05 SOL | 67.10 SOL | **70.1%** |
| Po naprawie (N=70 + fixed offsets) | 54.97 SOL | 67.10 SOL | **18.1%** |

Pozostały gap 12.13 SOL (18.1%) — najsilniejszy kandydat: **wartość limit orderów**
zakodowana w trailing bytes (raw_len - 7920) kont z otwartymi zleceniami.
Nie jest wykluczone że to definicja lpagent_nav (incl. pending limit orders).

## Verdict — KILL z warunkiem

diff_pct = 18.1% > próg 5%. Formalnie KILL, ale z jednoznaczną ścieżką naprawy.

**Uzasadnienie „z warunkiem":**
1. Architektura jest potwierdzona — LP reserve NAV działa, każda pozycja ma sensowną wartość
2. Jedynym brakującym komponentem jest dekodowanie limit orderów (trailing bytes)
3. Trailing bytes mają deterministyczny rozmiar (raw_len - 7920 per konto)
4. Brak limit order decode ≠ brak feasibility — to dodatkowy krok w pełnej implementacji

## Ścieżka do full E implementation

**Narzędzia:** `solders` + `struct` (SDK nie potrzebny — raw RPC wystarczy)

**Kluczowe ryzyko:** Limit order struct w trailing bytes nie jest zdokumentowany publicznie.
Będzie wymagał reverse-engineering z IDL lub zdekodowania jednego konta przez `anchorpy`.

**Szacowany nakład:** L (4–6 sesji, per PLAN-portfolio-truth.md) + 0.5–1 sesja extra
na limit order decode.

**Action items do wdrożenia:**
1. Zdekodować struct limit orderu — pobrać aktualny `idls/dlmm.json` z Meteora SDK repo
   i znaleźć typ odpowiadający trailing bytes (prawdopodobnie `OpenOrder` lub `LimitOrder`)
2. Zaimplementować `decode_open_orders(data, offset=7920)` → suma value open orders w SOL
3. Re-run spike z nowym komponentem — oczekiwany diff_pct < 5%
4. Wtedy formalna GO decyzja dla sub-project E
