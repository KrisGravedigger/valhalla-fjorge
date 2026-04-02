# lpagent Cross-Check System

**Date:** 2026-04-02
**Status:** Ready for design doc split

---

## Problem

Lokalny `positions.csv` jest zasilany wyłącznie przez logi Discord (Valhalla Bot).
Logi mają luki — pozycje, które bot otworzył i zamknął, ale Discord nie dostarczył wiadomości.
Brakujące pozycje wypaczają analizę PnL, statystyki walletów i rekomendacje.

lpagent API ma dostęp do **wszystkich** historycznych pozycji Meteora dla danego walletu,
niezależnie od źródła otwarcia. Porównanie dało ~0.5% luki w oknie od luty 2026,
ale pełna historia sięga września 2025 (ok. 7,200 pozycji przed naszym trackingiem).

## MVP Scope

### Funkcjonalności

1. **Klient lpagent API** (`valhalla/lpagent_client.py`)
   - Pobieranie zamkniętych pozycji dla walletu z filtrowaniem `from_date`/`to_date`
   - Paginacja (pageSize=10, stała — limit param ignorowany przez API)
   - Rate limiting: 5 RPM (Free tier) → 12s między requestami
   - Cache dzienny: `output/lpagent_cache/YYYY-MM-DD.json` — jeśli plik istnieje, skip API

2. **Logika cross-checku** (`valhalla/cross_check.py`)
   - Porównanie lpagent `tokenId` vs lokalny `full_address` (klucz unikatowy pozycji)
   - Identyfikacja missing positions (są w lpagent, nie ma w CSV)
   - Generowanie wierszy backfill z `pnl_source="lpagent"`, `target_wallet="lpagent_backfill"`
   - Raport: ile pozycji brak, szacowany PnL który umknął

3. **Mechanizm zastępowania** (rozszerzenie `valhalla/merge.py`)
   - Przy normalnym mergu: jeśli pozycja z `pnl_source=lpagent` ma ten sam `full_address`
     co nowo parsowana pozycja z Discord → zastąp lpagent-row pełnymi danymi z Discord
   - Gwarantuje że backfill nie koliduje z późniejszym uzupełnieniem logów

4. **Integracja z pipeline'm** (`valhalla_parser_v2.py`)
   - Nowy flag `--cross-check [from_date [to_date]]` — ręczne odpytanie zakresu dat
   - Auto-run na końcu normalnego przebiegu: fetch za wczoraj, cichy jeśli 0 luk
   - Watermark `output/lpagent_sync.json` — śledzi ostatni zsynchronizowany dzień,
     auto-run odpytuje od watermark do wczoraj żeby nie powtarzać

### Mapowanie pól lpagent → positions.csv

| positions.csv | lpagent field | Uwagi |
|---|---|---|
| `datetime_open` | `createdAt` | ISO → YYYY-MM-DDTHH:MM |
| `datetime_close` | `updatedAt` | tylko dla status=Close |
| `target_wallet` | — | stałe `"lpagent_backfill"` |
| `token` | `token0Info.token_symbol` | |
| `position_type` | — | stałe `"Spot"` |
| `sol_deployed` | `inputNative` | |
| `sol_received` | `outputNative` | |
| `pnl_sol` | `pnlNative` | |
| `pnl_pct` | `pnl.percentNative` | |
| `position_id` | `tokenId[:8]` | |
| `full_address` | `tokenId` | klucz dedup |
| `pnl_source` | — | stałe `"lpagent"` |
| `meteora_deposited` | `inputNative` | |
| `meteora_withdrawn` | `outputNative` | |
| `meteora_fees` | `collectedFeeNative` | |
| `meteora_pnl` | `pnlNative` | |
| pozostałe | — | puste string |

### Explicitly Out of Scope

- Pobieranie otwartych pozycji (tylko `status=Close`)
- Uzupełnianie `target_wallet_address`, `close_reason`, `mc_at_open` itp. (ręczne)
- Obsługa wielu walletów (tylko `J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF`)
- Premium tier / wyższe rate limity
- Backfill pozycji sprzed 2026-02-11 (osobna decyzja, ta sama logika wystarczy)

## Technical Approach

### Nowe pliki
- `valhalla/lpagent_client.py` — LpAgentClient class: fetch, paginate, cache
- `valhalla/cross_check.py` — CrossChecker class: compare, diff, generate backfill rows

### Zmieniane pliki
- `valhalla/merge.py` — dodanie logiki zastępowania lpagent-rows przy merge
- `valhalla_parser_v2.py` — `--cross-check` flag + auto-run hook + watermark
- `.env` — nowa zmienna `LPAGENT_API_KEY`
- `.env.example` — dokumentacja nowej zmiennej

### Konfiguracja
```
LPAGENT_API_KEY=lpagent_xxx
LPAGENT_WALLET=J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF
```
Wallet można też hardcode'ować jako domyślny — jedno konto, jeden wallet.

### Cache strategy
Plik `output/lpagent_cache/YYYY-MM-DD.json` zawiera listę pozycji z API dla danego dnia.
Format: surowy JSON z lpagent (lista obiektów). Plik tworzony po pełnym pobraniu dnia
(wszystkie strony). Nigdy nie nadpisywany — dzień historyczny jest niezmienny.

Wyjątek: dzień dzisiejszy i wczorajszy mogą mieć pozycje, które jeszcze nie są zamknięte
lub API jeszcze nie zaindeksował — te dni nie są cache'owane (albo cache TTL=1h).

### Watermark
`output/lpagent_sync.json`:
```json
{ "last_synced_date": "2026-04-01" }
```
Auto-run: fetch od `last_synced_date + 1 dzień` do `wczoraj`.
Po sukcesie: aktualizuj `last_synced_date` do wczoraj.

## Szacunek kosztu API

| Tryb | Zapytania | Czas (5 RPM) |
|---|---|---|
| Auto (wczoraj, ~24 poz) | ~3 | <1 min |
| Ręczny tydzień (~170 poz) | ~21 | ~4 min |
| Backfill od 11 luty (~50 dni) | ~150 | ~30 min |

## Success Criteria

- `python valhalla_parser_v2.py --cross-check 2026-04-01` pobiera pozycje za jeden dzień,
  raportuje brakujące i dodaje je do positions.csv z `pnl_source=lpagent`
- Ponowne uruchomienie tego samego dnia używa cache (0 requestów API)
- Po wklejeniu logów Discord zawierających pozycję z backfill — row jest zastępowany
  pełnymi danymi (bez duplikatu)
- Auto-run na końcu normalnego pipeline'u działa cicho gdy 0 luk
- Klucz API w `.env`, nie hardcode'owany

## Risks

- **updatedAt ≠ dokładny czas zamknięcia** — updatedAt to czas ostatniej aktualizacji rekordu
  w lpagent, może być opóźniony o kilka minut do godziny. Wystarczający do audytu.
- **tokenId kolizja z position_id** — lpagent `tokenId` to pełny adres pozycji Solana,
  identyczny z `full_address` w naszym CSV. Weryfikacja potwierdzona empirycznie.
- **from_date/to_date semantyka** — API filtruje po `createdAt` (data otwarcia), nie zamknięcia.
  Pozycja otwarta 31 marca, zamknięta 1 kwietnia — pojawi się przy `from_date=2026-03-31`.
  Cross-check per dzień powinien używać from_date=to_date=data_otwarcia, nie zamknięcia.
- **Rate limit 5 RPM** — przy backfillu dłuższych zakresów wymagany throttling 12s/request.
