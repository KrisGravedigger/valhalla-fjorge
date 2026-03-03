# PLAN: Loss Analysis Report — Restructure & Wallet Scorecard

**Data:** 2026-02-26
**Status:** Ready for design docs

---

## Problem

Raport `output/loss_analysis.md` zawiera wartościowe dane, ale jest trudny w użyciu:

1. **Kolejność sekcji jest analityczna, nie decyzyjna** — wnioski i rekomendacje są na samym
   dole, a liczby surowe na górze. Użytkownik musi przeczytać cały raport żeby dowiedzieć się
   co robić.

2. **Brak priorytetyzacji walletów** — nie ma jednego miejsca, które odpowiada na pytania:
   "któremu wallet zwiększyć kapitał?", "który wymienić?", "który obserwować?".

3. **Rekomendacje to lista zdań** — nie ma tabelarycznego widoku z rankingiem walletów
   i ich statusem.

4. **Brakuje kilku użytecznych metryk** — m.in. efektywność kapitału (PnL na wdrożony SOL),
   spójność wyników (wariancja win rate w czasie), rug exposure, średni czas trzymania.

---

## Cel

Przebudować `loss_analysis.md` tak, by był raportem decyzyjnym:
- Wnioski i pilne działania na górze
- Wallet Scorecard jako centralny element — ranked table z klasyfikacją
- Nowe metryki wyliczane z istniejących danych (`positions.csv` / `MatchedPosition`)
- Bogatsze triggery rekomendacji

---

## Kontekst techniczny

### Pliki do modyfikacji

| Plik | Rola |
|---|---|
| `valhalla/loss_analyzer.py` | Logika analityczna — tu dodajemy nowe klasy/metryki |
| `valhalla_parser_v2.py` | Formatowanie raportu — funkcja `_generate_loss_report()` (~linia 571) |

### Istniejące dane

**Z `MatchedPosition` (positions.csv):**
- `pnl_sol`, `pnl_pct` — wynik pozycji
- `close_reason` — stop_loss / rug / failsafe / normal / still_open
- `datetime_open`, `datetime_close` — timestamps ISO 8601
- `jup_score`, `mc_at_open`, `token_age_hours` — metryki tokenów
- `sol_deployed` — kapitał wdrożony
- `target_wallet` — identyfikator portfela

**Z `summary.csv` (PositionStats):** — NIE używamy bezpośrednio w loss_analyzer.
Wszystkie nowe metryki liczymy z listy `MatchedPosition`, żeby zachować spójność architektury.

### Istniejące klasy w loss_analyzer.py

- `LossAnalyzer` — risk profile, wywołuje sub-analizy
- `FilterBacktester` — sweep progów filtrów
- `StopLossLevelAnalyzer` — dystrybucja głębokości strat
- `WalletTrendAnalyzer` — trend stop-loss per wallet (flagi deteriorating)
- `LossAnalysisResult` — top-level dataclass z wynikami

---

## Nowe metryki do implementacji

### Metryki per wallet (nowa klasa `WalletScorecardAnalyzer`)

| Metryka | Definicja | Źródło |
|---|---|---|
| `win_rate_pct` | wins / closed_positions | MatchedPosition.close_reason / pnl_sol |
| `win_rate_7d_pct` | wins / closed_positions za ostatnie 7 dni | datetime_close |
| `total_pnl_sol` | suma pnl_sol closed positions | pnl_sol |
| `pnl_per_day_sol` | pnl_7d / 7 | datetime_close + pnl_sol |
| `rug_rate_pct` | rugs / total_positions | close_reason |
| `win_rate_trend` | win_rate_7d - win_rate_all (w pp, z progiem ±5pp) | pochodna |
| `avg_hold_minutes` | średnia czasu (datetime_close - datetime_open) | datetime_open + datetime_close |
| `capital_efficiency` | total_pnl_sol / suma sol_deployed (closed) | pnl_sol / sol_deployed |
| `consistency_score` | max(|WR_24h - WR_all|, |WR_72h - WR_all|, |WR_7d - WR_all|) | pochodna |

**Definicja "win" dla Scorecarda:** pnl_sol > 0 AND close_reason NOT IN LOSS_REASONS
**Definicja "rug":** close_reason IN {rug, rug_unknown_open}
**Minimum danych do klasyfikacji:** ≥ 30 zamkniętych pozycji ogółem

### Klasyfikacja walletów (`status`)

| Status | Kod | Warunki |
|---|---|---|
| Zwiększ kapitał | `increase_capital` | total_pos ≥ 30 AND win_rate_7d ≥ 65% AND win_rate_7d ≥ 60% AND total_pnl_sol > 0 AND rug_rate < 8% |
| Do wymiany | `consider_replacing` | total_pos ≥ 30 AND (total_pnl_sol < 0 OR win_rate_7d < 45%) |
| Za mało danych | `insufficient_data` | total_pos < 30 |
| Nieaktywny | `inactive` | brak pozycji w ciągu ostatnich 3 dni (wg datetime_close) |
| Obserwuj | `monitor` | wszystkie pozostałe przypadki |

**Priorytet reguł:** `inactive` > `insufficient_data` > `increase_capital` > `consider_replacing` > `monitor`

### Nowe triggery rekomendacji

Oprócz obecnych (sweet spot filter, deteriorating flag), dodajemy:

| Trigger | Komunikat |
|---|---|
| status == `increase_capital` | "Wallet X: win rate {WR_7d}% przez 7 dni przy {N} pozycjach — rozważ zwiększenie kapitału" |
| status == `consider_replacing` AND total_pnl_sol < 0 | "Wallet X: ujemny PnL łącznie ({pnl} SOL) na {N} pozycjach — kandydat do wymiany" |
| status == `consider_replacing` AND win_rate_7d < 45% | "Wallet X: win rate 7d spada do {WR_7d}% (całość: {WR_all}%) — rozważ wymianę" |
| win_rate_trend < -15pp | "Wallet X: win rate spada — {WR_7d}% (7d) vs {WR_all}% (całość)" |
| rug_rate > 15% | "Wallet X: wysoki rug rate ({rug_rate}%) — wallet handluje ryzykowniejszymi tokenami" |
| status == `inactive` | "Wallet X: brak aktywności od ponad 3 dni — sprawdź czy jest nadal aktywny" |

---

## Nowa struktura raportu

```
# Loss Analysis Report — [data]

## Spis treści

## 1. Podsumowanie wykonawcze (Executive Summary)
   - 4-5 zdań: stan portfela, najlepszy wallet, główna okazja do poprawy

## 2. Pilne działania (Action Items)
   - Bullet lista rekomendacji z triggerami (priorytet: replacing > increasing capital > filter > deteriorating)

## 3. Wallet Scorecard
   - Tabela rankingowa: wszystkie wallety posortowane wg pnl_per_day_sol (malejąco)
   - Kolumny: Wallet | Pozycje | WR% | WR 7d% | PnL (SOL) | SOL/dzień | Rug Rate | Śr. czas | Trend | Status

## 4. Rekomendacje filtrów
   - Tylko wallety z actionable sweet spot (nie minimum), skondensowane

## 5. Analiza strat
   ### 5a. Risk Profile
   ### 5b. Stop-Loss Level Distribution
   ### 5c. Source Wallet Comparison (jeśli dane dostępne)

## 6. Filter Backtest (globalny)
   - Tabele jup_score / mc_at_open / token_age_hours jak dotychczas

## 7. Szczegóły per wallet (Filter Backtest per wallet)
   - Jak dotychczas, na końcu
```

---

## Fazy implementacji

### Faza A — WalletScorecardAnalyzer (nowe metryki + klasyfikacja)

**Plik:** `valhalla/loss_analyzer.py`

Nowy dataclass `WalletScorecard`:
```python
@dataclass
class WalletScorecard:
    wallet: str
    total_positions: int
    closed_positions: int
    win_rate_pct: float
    win_rate_7d_pct: Optional[float]   # None jeśli brak danych 7d
    win_rate_24h_pct: Optional[float]
    win_rate_72h_pct: Optional[float]
    total_pnl_sol: Decimal
    pnl_7d_sol: Decimal
    pnl_per_day_sol: Decimal           # pnl_7d / 7
    rug_rate_pct: float
    avg_hold_minutes: Optional[float]  # None jeśli brak datetime
    capital_efficiency: Optional[float]  # total_pnl / sum_deployed; None jeśli sol_deployed brak
    consistency_score: Optional[float]  # max odchylenie WR od średniej (pp)
    win_rate_trend_pp: Optional[float]  # win_rate_7d - win_rate_all (pp)
    status: str                        # increase_capital / monitor / consider_replacing / ...
    days_since_last_position: Optional[int]
```

Nowa klasa `WalletScorecardAnalyzer`:
- `analyze(positions, reference_date=None) -> List[WalletScorecard]`
- Sortowanie wyjścia: malejąco wg `pnl_per_day_sol`
- Obsługa braku datetime (graceful None)
- "inactive": dni bez aktywności liczone od `reference_date` (domyślnie max datetime_close)

Rozszerzyć `LossAnalysisResult` o pole `wallet_scorecards: List[WalletScorecard]`.
`LossAnalyzer.analyze()` wywołuje `WalletScorecardAnalyzer().analyze(positions)`.

### Faza B — Nowe triggery rekomendacji

**Plik:** `valhalla_parser_v2.py`

Nowa pomocnicza funkcja `_build_action_items(result, per_wallet_backtest) -> List[str]`:
- Przyjmuje `LossAnalysisResult` i per-wallet backtest dict
- Zwraca posortowaną listę stringów rekomendacji (priorytet: replacing > increasing > filter > deteriorating)
- Deduplikacja: jeśli wallet ma wiele triggerów, zbiera je w jeden blok

### Faza C — Przebudowa formatowania raportu

**Plik:** `valhalla_parser_v2.py`, funkcja `_generate_loss_report()`

Przepisanie kolejności sekcji zgodnie z nową strukturą:
1. Executive Summary (algorytmicznie generowany z `LossAnalysisResult`)
2. Pilne działania (wynik `_build_action_items()`)
3. Wallet Scorecard (tabela z `WalletScorecard`)
4. Rekomendacje filtrów (skondensowane, tylko actionable)
5. Analiza strat (Risk Profile + SL Distribution + Source Wallet)
6. Filter Backtest globalny
7. Szczegóły per wallet

Dodać spis treści (sekcje 1-7 z anchorami markdown).

---

## Kolejność implementacji

1. **Faza A** — nowe metryki i klasyfikacja (tylko `loss_analyzer.py`)
2. **Faza B** — nowe triggery (tylko `valhalla_parser_v2.py`)
3. **Faza C** — nowe formatowanie raportu (tylko `valhalla_parser_v2.py`)

Faza A jest prerequesite dla B i C. Fazy B i C mogą być zrobione równolegle po A.

---

## Conventions (z istniejącego kodu)

- Decimal dla SOL: ZAWSZE, nigdy float
- `parse_iso_datetime` z `models.py` do parsowania dat
- Nowe pola w dataclasach: `field(default=None)` dla Optional
- Nowe klasy w `loss_analyzer.py` bez import zewnętrznych (stdlib only)
- Wszystkie klasy analityczne: czyste funkcje bez I/O, bez side effects
- Formatowanie w `valhalla_parser_v2.py` używa `_md_table()`, `_fmt_sol()`, `_fmt_mc()`, `_fmt_pct()`
- Rate limiting / RPC / Meteora: nie dotyczy — operujemy tylko na `MatchedPosition`

---

## Commit strategy

- Branch: `claude/report-scorecard`
- Commit po każdej Fazie osobno: `feat: add WalletScorecardAnalyzer`, `feat: action items`, `feat: restructure loss report`
- NIE pushować automatycznie

---

## Weryfikacja

1. `python valhalla_parser_v2.py --no-clipboard` → sprawdź że `output/loss_analysis.md` istnieje
2. Otwórz raport — Executive Summary powinno być ≤ 6 linii
3. Wallet Scorecard: sprawdź że liczba wierszy = liczba unikalnych walletów w positions.csv
4. Status walletów: 20260127_3WrPRi (ujemny PnL) → `consider_replacing`; 20260121_7tB8WHYK (WR 68%) → `increase_capital`
5. Sekcja pilnych działań powinna pojawić się przed sekcją analizy
6. Regresja: stare sekcje (Filter Backtest, Risk Profile, SL Distribution) muszą nadal być obecne
