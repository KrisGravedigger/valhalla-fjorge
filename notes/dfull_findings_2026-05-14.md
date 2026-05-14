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

- [ ] Kwestia 1: drift PnL — do zbadania
- [ ] Kwestia 2: 1273 not_in_lpagent — do zbadania  
- [ ] Kwestia 3: source_wallet puste — do zbadania
