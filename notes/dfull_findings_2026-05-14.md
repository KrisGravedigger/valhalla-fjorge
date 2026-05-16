# D-full Reconciliation — pierwsze wyniki i kwestie do zbadania

**Data raportu:** 2026-05-14  
**Okno:** 2026-04-01 → 2026-04-30  
**Plik raportu:** `output/reconciliation_2026-04-01_2026-04-30.md`  
**Polecenie:** `python -m valhalla.reconcile --from 2026-04-01 --to 2026-04-30`

---

## Wyniki na poziomie nagłówka

| Kategoria        | Liczba   |
|------------------|----------|
| Matched          |  3 525   |
| lpagent-only     |     55   |
| ours-only        |  8 060   |

---

## Wniosek 1 — Drift PnL na matchowanych pozycjach: +1.86 SOL

| Miara              | Wartość        |
|--------------------|----------------|
| PnL suma (nasz)    | +6.832 500 SOL |
| PnL suma (lpagent) | +4.975 489 SOL |
| **Drift**          | **+1.857 011 SOL** (+37%) |

Drift jest konsekwentny — prawie każdy dzień nasz PnL > lpagenta.
Wyjątki (lpagent wyższy): 2026-04-25 (−0.022 SOL), drobne różnice kilku innych dni.

**Hipotezy:**
- Różnica metodologiczna w liczeniu fees (collected vs uncollected)
- Różnica w uwzględnianiu IL (impermanent loss)
- lpagent używa `pnlNative` = only realized, my CSV może zawierać unrealized component
- Różnica w zaokrąglaniu / precyzji

**Do zbadania:** porównać kilka konkretnych pozycji z dużym driftem (np. `14JMM85awY75`: nasz +0.0022, lpagent −0.000447, drift +591%) — sprawdzić co składa się na każdą z wartości.

---

## Wniosek 2 — Ours-only: 8 060 pozycji

| Kategoria              | Liczba | Opis                                                         |
|------------------------|--------|--------------------------------------------------------------|
| `older_than_retention` |  6 225 | `datetime_close < 2026-04-01` — stare dane w positions.csv  |
| `not_in_lpagent`       |  1 273 | brak w JSONL mimo że powinny tam być — **niewyjaśnione**     |
| `lpagent_dropped`      |    562 | `pnl_sol == 0` lub puste — nie przyniosły zysku              |

**6 225 `older_than_retention`** — to oczekiwane; positions.csv zawiera całą historię, JSONL zaczyna się od daty retencji lpagenta.

**1 273 `not_in_lpagent`** — to jest realna rozbieżność. Pozycje które:
- były zamknięte w oknie kwiecień 2026
- mają niezerowy pnl_sol
- należą do śledzonego walletu
- ale nie istnieją w JSONL

Możliwe powody: lpagent nie śledził ich w tym czasie, pozycje zamknięte przez inny mechanizm, błąd synchronizacji JSONL.

---

## Wniosek 3 — Per-wallet agregacja: bezużyteczna

Wszystkie 3 525 matched pozycji mają `source_wallet = "unknown"` w agregacji per-wallet. Pole `source_wallet` jest puste w `output/positions.csv`.

**Konsekwencja:** per-wallet breakdown nie działa dopóki positions.csv nie zostanie uzupełniony.

**Do zbadania:** skąd powinno pochodzić `source_wallet`? Czy istnieje w jakimś innym pliku? Czy jest to pole generowane przez konkretny krok pipeline'u?

---

## lpagent-only: 55 pozycji (wszystkie `in_archive`)

Wszystkie 55 pozycji lpagent-only są w archiwum — lpagent je historycznie widział, ale nie mamy ich w positions.csv. Żadnych `truly_missing`. To dobry znak — nie ma "nowych" pozycji o których nie wiemy.

---

## Kwestie do zbadania — gotowy prompt

Poniższy prompt jest gotowy do wklejenia jako zlecenie dla agenta badawczego.

---

### PROMPT DO DELEGACJI

```
Zbadaj trzy kwestie wynikające z pierwszego uruchomienia D-full reconciliation.
Kontekst: plik output/positions.csv zawiera nasze pozycje, 
output/lpagent_cache/positions_J4tkG.jsonl zawiera dane z lpagenta.
Narzędzia: czytaj pliki bezpośrednio, możesz uruchamiać Python.

## Kwestia 1 — Skąd drift PnL +1.86 SOL na 3525 matchowanych pozycjach?

Nasz PnL (suma): +6.832 SOL
lpagent PnL (suma): +4.975 SOL  
Drift: +1.857 SOL (+37%)

Wybierz 5 pozycji z największym bezwzględnym driftem z pliku:
  output/reconciliation_2026-04-01_2026-04-30_matched.csv
Dla każdej z nich:
1. Znajdź rekord w positions.csv (kolumna full_address) i sprawdź skąd pochodzi pnl_sol
   (kolumny: pnl_source, pnl_sol, ewentualnie inne)
2. Znajdź rekord w JSONL (tokenId == full_address) i sprawdź:
   pnlNative, collectedFeeNative, uncollectedFee, impermanentLoss
3. Sformułuj hipotezę co powoduje różnicę dla tych konkretnych pozycji

Wynik: lista hipotez uszeregowana od najbardziej prawdopodobnej, z przykładami liczbowymi.

## Kwestia 2 — Co to są te 1273 pozycji "not_in_lpagent"?

Plik: output/reconciliation_2026-04-01_2026-04-30_ours_only.csv  
Filtr: reason == "not_in_lpagent"

Przeanalizuj te 1273 pozycji:
1. Jaki jest rozkład dat zamknięcia (datetime_close)?
   Czy skupiają się w konkretnych dniach/tygodniach?
2. Jakie tokeny dominują (kolumna token)?
3. Jaka jest łączna wartość pnl_sol tych pozycji?
4. Sprawdź czy tokenId tych pozycji istnieje gdziekolwiek w JSONL
   (może pod innym polem niż tokenId? lub w archiwum?)
5. Sprawdź czy te pozycje mają jakąś wspólną cechę odróżniającą je
   od pozycji które zostały matchowane (np. source, protokół, wartość pnl)

Wynik: zwięzła charakterystyka tej grupy + hipoteza dlaczego ich nie ma w JSONL.

## Kwestia 3 — Skąd powinno pochodzić pole source_wallet?

W output/positions.csv kolumna source_wallet jest pusta dla wszystkich rekordów
(stąd per-wallet agregacja w raporcie D-full pokazuje tylko "unknown").

1. Sprawdź schemat positions.csv — jakie są wszystkie kolumny?
2. Sprawdź inne pliki w output/ które mogą zawierać informacje o walletach
   (np. summary.csv, capital_flows.csv, address_cache.json)
3. Sprawdź kod w valhalla/ który generuje/aktualizuje positions.csv —
   czy jest logika która powinna wypełniać source_wallet?
4. Sprawdź dokumentację: docs/ — czy któryś dokument opisuje to pole?

Wynik: odpowiedź na pytanie "czy source_wallet jest błędem pipeline'u czy cechą projektu"
oraz co trzeba zrobić żeby per-wallet agregacja działała.

## Format odpowiedzi

Dla każdej kwestii: TL;DR (2 zdania) + szczegółowe ustalenia.
Na końcu: lista konkretnych action items jeśli coś wymaga naprawy.
```

---

## Status

- [x] Kwestia 1: drift PnL — zbadana (2026-05-14)
- [x] Kwestia 2: 1273 not_in_lpagent — zbadana (2026-05-14)
- [x] Kwestia 3: source_wallet puste — zbadana (2026-05-14)

---

## Wyniki badania (2026-05-14)

### Kwestia 1 — Drift PnL: różnica formuł Meteora vs LPAgent

Źródłem driftu jest różnica w danych wejściowych, nie `uncollectedFee` (wszędzie `"0"` w top 5).

- Nasze `pnl_sol` = `withdrawn + fees − deposited` (dane Meteora)
- LPAgent `pnlNative` = `outputNative + collectedFeeNative − inputNative`
- Te same fee, inne kwoty inputu/outputu → systematyczna różnica

**Top 5 pozycji z największym driftem:**

| Token | Nasz PnL | LPAgent | Drift |
|---|---|---|---|
| Neukgu | +0.4251 | +0.3364 | +0.0887 |
| 49 | −0.2946 | −0.3721 | +0.0775 |
| WIZ | −0.3767 | −0.4484 | +0.0717 |
| ASTEROID | −0.2647 | −0.3274 | +0.0627 |
| DJT | −0.3667 | −0.4290 | +0.0623 |

Przykład Neukgu: `2.042 + 0.0107 − 1.6276 = 0.4251` (nasze) vs `1.9899 + 0.0107 − 1.6642 = 0.3364` (LPAgent).

**Action item:** Ujednolicić definicję PnL — porównywać `pnlNative` LPAgent do tej samej formuły, lub jawnie oznaczyć drift jako różnicę źródeł.

---

### Kwestia 2 — 1273 not_in_lpagent: zanieczyszczenie danymi z maja

**1162 z 1273 pozycji ma `datetime_close` w maju 2026** — plik `2026-04-01_2026-04-30` zawiera dane spoza okna. To błąd filtra dat w generowaniu reconciliation CSV.

Rozkład `datetime_close`:

| Tydzień | Zakres | Liczba |
|---|---|---:|
| W14 | 2026-04-01..2026-04-05 | 51 |
| W15 | 2026-04-06..2026-04-12 | 11 |
| W16 | 2026-04-13..2026-04-19 | 20 |
| W17 | 2026-04-20..2026-04-26 | 23 |
| W18 | 2026-04-27..2026-05-03 | 193 |
| W19 | 2026-05-04..2026-05-10 | 668 |
| W20 | 2026-05-11..2026-05-17 | 307 |

Prawdziwie brakujące (kwiecień): **105 pozycji** (W14–W17).  
Zanieczyszczenie (maj): **1168 pozycji** (W18–W20).

Top tokeny w grupie: HANTA 175, Goblin 166, ASTEROID 96, BURNIE 63, unc 51, TripleT 49, Apple 48, TROLL 41, maxxing 41, BULL 38.

**Action item:** Naprawić filtr dat w generowaniu CSV — wykluczyć rekordy z `datetime_close` poza zadanym oknem.

---

### Kwestia 3 — source_wallet: kolumna nie istnieje w positions.csv

Nagłówek `positions.csv` **nie zawiera** kolumny `source_wallet`. Dostępne są `source_wallet_hold_min`, `source_wallet_pnl_pct`, `source_wallet_scenario` — ale nie samo `source_wallet`.

- `valhalla/reconcile.py:484` oczekuje kolumny `source_wallet`
- `docs/029-reconciliation-full.md:41` zakłada że pole istnieje
- Pipeline nigdy go nie wypełnia → per-wallet agregacja pokazuje tylko `unknown | 3525`

To **bug pipeline'u**, nie celowa decyzja projektowa.

**Action item:** Dodać `source_wallet` w positions.csv lub zaktualizować `reconcile.py:484` żeby używał istniejącego pola (`original_wallet` / `target_wallet_address`).

---

## Skonsolidowane Action Items

1. **[PILNE] Naprawić filtr dat** w generowaniu reconciliation CSV — wykluczyć `datetime_close` poza oknem
2. **Ujednolicić definicję PnL** — uzgodnić formułę między Meteora a LPAgent lub jawnie oznaczyć drift jako różnicę źródeł
3. **Naprawić `source_wallet`** w pipeline — dodać kolumnę lub zaktualizować `reconcile.py:484`
4. **Doliczyć sumę pnl_sol** dla 105 prawdziwie brakujących pozycji z kwietnia (pandas)

---

## E-Spike — internal NAV computation (2026-05-16)

### Cel

Sprawdzić czy można zbudować internal_nav_sol niezależnie od lpagenta:
odczyt on-chain PositionV2 → BinArray → Jupiter → suma w SOL.

### Wyniki

| Przebieg | internal_nav_sol | lpagent_nav | diff_pct | Problem |
|---|---:|---:|---:|---|
| Przed fix | 20.05 SOL | 67.10 SOL | 70.1% | PositionV2 decode bug |
| Po fix | 54.97 SOL | 67.10 SOL | 18.1% | Limit orders nie uwzględnione |

### Decode bug — przyczyna i naprawa

**Problem:** Konta PositionV2 z otwartymi limit orders są większe niż standardowe 8120B.
Dynamiczna formuła `N=(raw_len-280)//112` dawała za duże N, przesuwając odczyt
`lower_bin_id` i `liq_shares` na złe offsety → pozycje dekodowane jako lower=0/upper=0
→ BinArray[0] nie istnieje → „KILL candidate" dla 40+ z 50 pozycji.

**Naprawa:** N=70 (MAX_BIN_PER_POSITION) jest stałe dla tablic LP. Zweryfikowane
przez przeszukanie bajtów: lower_bin_id (-988) dla 12264-bajtowego konta istnieje
wyłącznie na offsetcie 7912 = 72+70×112. Trailing data (4344B) po offsetcie 7920
to dane limit orderów — poza zakresem spike'a.

### Pozostałe 18% gap

Możliwe wyjaśnienia:
1. **Limit order value** — pozycje z `raw_len > 8120` mają trailing data z wartością
   otwartych zleceń (nie dekodowaną przez spike). Najsilniejszy kandydat.
2. **Jupiter 429** — kilka tokenów X pominięto przy wycenie (małe, ~0.1 SOL łącznie).
3. **Fees = 0** — potwierdzone na poziomie bajtowym (LP fees_infos offset 4552 = zera).

### Decyzja

Architektura działa (70% → 18%) — subprojekt E jest **wykonywalny**. Pełna
implementacja musi obsłużyć limit order value (trailing bytes per PositionV2 account)
żeby zejść poniżej progu 5%.
