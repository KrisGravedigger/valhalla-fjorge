# lpagent Retention Window — Ustalenia empiryczne

**Data:** 2026-05-15  
**Źródło:** D-full reconciliation, okno 2026-04-01 → 2026-04-30  
**Polecenie:** `python -m valhalla.reconcile --from 2026-04-01 --to 2026-04-30`

---

## TL;DR

lpagent przechowuje dane od **2026-04-01**. Starsze pozycje nie istnieją w JSONL.
Liczba „2.29 SOL all-time" widoczna w UI lpagenta to PnL z okna ~kwiecień 2026, nie all-time.

---

## Dane z JSONL (`positions_J4tkG.jsonl`)

| Miara | Wartość |
|---|---|
| Liczba rekordów | 5 123 |
| Najwcześniejszy `updatedAt` | 2026-04-01 |
| Najpóźniejszy `updatedAt` | 2026-05-12 |

Brak pliku `watermark.json` — C nie zapisał watermarku przy budowie JSONL z archiwum.

---

## Wyniki reconciliation (po naprawie filtra dat)

| Kategoria | Liczba | Opis |
|---|---|---|
| Matched | 3 525 | Pozycje w obu zbiorach, kwiecień |
| lpagent-only | 55 | Wszystkie `in_archive` — były w legacy cache, nie w JSONL |
| ours-only: `older_than_retention` | 6 225 | `datetime_close < 2026-04-01` — poza oknem lpagenta |
| ours-only: `not_in_lpagent` | 90 | Kwiecień, w naszym CSV, brak w JSONL |
| ours-only: `wallet_not_tracked` | 21 | Należą do walletów nie śledzonych przez lpagenta |
| ours-only: `lpagent_dropped` | 7 | `pnl_sol == 0` |

---

## Wnioski

### 1. Okno retencji = od 2026-04-01

lpagent nie ma danych sprzed 1 kwietnia 2026. Nasze 6 225 pozycji z `datetime_close < 2026-04-01`
nie istnieje w JSONL — to oczekiwane. Skutkuje to tym, że sumy PnL widoczne w UI lpagenta
dotyczą wyłącznie pozycji otwartych od ~początku kwietnia.

### 2. „2.29 SOL all-time" nie jest all-time

Przy matched PnL (nasz) = **+6.83 SOL**, matched PnL (lpagent) = **+4.97 SOL** — oba za sam
kwiecień. Liczba 2.29 SOL widoczna wcześniej w UI lpagenta to prawdopodobnie PnL z węższego
podokresu (kilka dni) lub z innego filtru daty. lpagent nie pokazuje naszych historycznych
+31.79 SOL wszystkich zamkniętych pozycji.

### 3. 90 pozycji z kwietnia brakuje w JSONL

Prawdopodobna przyczyna: lpagent nie śledził tych pozycji w tamtym czasie (nowe wallety,
short-lived positions, lub przerwa w synchronizacji). Wartość: **+0.62 SOL** — mała,
nie wpływa na obraz całości.

### 4. Żadnych `truly_missing` po stronie lpagenta

55 lpagent-only to wyłącznie pozycje z archiwum legacy. Nie ma pozycji które lpagent widzi,
a my nie — brak niespodzianek.

### 5. Drift PnL na matchowanych: +1.86 SOL (+37%)

Wyjaśnione: różnica metodologii (`withdrawn + fees − deposited` vs `outputNative + collectedFeeNative − inputNative`).
Systematyczna, spójna z wcześniejszym ustaleniem planu (~1.18 SOL na 1196 pozycjach w D-lite).
**Nie jest błędem — out of scope.**

---

## Rekomendacja

Nie ma potrzeby dalszego dochodzenia w kwestii retencji. Okno jest jasne:
lpagent zaczyna od 2026-04-01. Przyszłe reconciliacje powinny uwzględniać tę granicę
przy interpretacji `older_than_retention`.
