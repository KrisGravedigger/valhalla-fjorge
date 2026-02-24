# PLAN: Loss Autopsy & Filter Backtester

**Data:** 2026-02-23
**Status:** Ready for design docs

## Problem

Pozycje kończące się stop-lossem (-5% do -20%) nie są automatycznie analizowane. Brakuje odpowiedzi na:
- Czy stracone pozycje były z góry bardziej ryzykowne (niższy jup_score / mc / age)?
- Jak poradził sobie wallet źródłowy (trzymał dłużej, wyszedł wcześniej)?
- Jak by wyglądały historyczne wyniki przy zmienionych filtrach / stop-lossie?
- Które wallety warto dalej śledzić?

## Cel

Automatyczny raport `output/loss_analysis.md` generowany przy każdym uruchomieniu parsera. Raport odpowiada na powyższe pytania bez ręcznej analizy.

## Istniejąca infrastruktura (reużywana)

- `valhalla/readers.py` — parsowanie wiadomości Discord
- `valhalla/models.py` — dataclassy OpenEvent, MatchedPosition
- `valhalla/csv_writer.py` — zapis positions.csv
- `valhalla/solana_rpc.py` — SolanaRpcClient, PositionResolver, AddressCache
- `valhalla/meteora.py` — MeteoraPnlCalculator
- `positions.csv` — kolumny: close_reason, pnl_pct, jup_score, mc_at_open, token_age_days, target_wallet, datetime_open, datetime_close

## Format wiadomości Discord (kluczowy kontekst)

```
Target: 20260125_2eWqo [https://app.lpagent.io/portfolio?address=2eWqouq9...]
Starting SOL balance: 2.37 SOL ($194.36 USD) on your wallet
View Tx - Your Solscan 1 [https://solscan.io/tx/BOT_SIG1] | Your Solscan 2 [...]
         | Target Tx 1 [https://solscan.io/tx/TARGET_SIG1] | Target Tx 2 [...]
View Position History - Your Metlex [...] | Target Metlex [...]
Ending SOL balance: 5.50 SOL ($451.06 USD) on your wallet
```

Parsowane wzorce:
- `target_wallet_address` — z `lpagent.io?address=...`
- `target_tx_signatures` — URL-e oznaczone "Target Tx N"

## Fazy implementacji

### Faza A — Parsowanie nowych danych (prerequisite)

**Pliki:** `readers.py`, `models.py`, `event_parser.py`, `matcher.py`, `csv_writer.py`, `json_io.py`

Rozszerzyć readers.py o ekstrakcję (PRZED czyszczeniem URL-i):
1. `target_wallet_address` — z lpagent.io URL
2. `target_tx_signatures` — URL-e oznaczone "Target Tx N"

Zmiana API readers.py — nowy dataclass zamiast tuple:
```python
@dataclass
class ParsedMessage:
    timestamp: str
    clean_text: str
    bot_tx_signatures: List[str]
    target_tx_signatures: List[str]   # NOWE
    target_wallet_address: Optional[str]  # NOWE
```

Nowe pola w OpenEvent i MatchedPosition jako `Optional` z `default=None`.

Nowe kolumny w positions.csv (NA KOŃCU — backwards compatibility):
- `target_wallet_address`
- `target_tx_signature` (tylko pierwsza, Target Tx 1)

**Wzorce regex:**
```python
LABELED_SOLSCAN_PATTERN = re.compile(
    r'([\w\s]+?)\s*\[https://solscan\.io/tx/([A-Za-z0-9]+)\]'
)
LPAGENT_PATTERN = re.compile(
    r'\[https://app\.lpagent\.io/portfolio\?address=([A-Za-z0-9]+)\]'
)
```

### Faza B — Loss Analyzer (działa na istniejących danych)

**Plik:** `valhalla/loss_analyzer.py` (nowy)

Klasy:
1. **LossAnalyzer** — risk profile: porównanie metryk stop-loss vs wszystkich pozycji
2. **FilterBacktester** — sweep progów filtrów, obliczanie net_sol_impact
3. **StopLossLevelAnalyzer** — rozkład pnl_pct w bucketach straty
4. **WalletTrendAnalyzer** — per-wallet stop_loss_rate, trend 7d, flagi

**LossAnalyzer — Risk Profile:**

| Metric | Stop-Loss Avg | All Trades Avg | Difference |
|--------|---------------|----------------|------------|
| jup_score | 72 | 84 | -14.3% |
| mc_at_open | 6.5M | 18.2M | -64.3% |
| token_age_d | 1.2 | 5.8 | -79.3% |

**FilterBacktester.sweep():**
- Parametry: `positions`, `param`, `thresholds`, `direction`, `wallet=None`
- Dla każdego progu oblicz: wins_kept, wins_excluded, losses_avoided, losses_kept
- Metryki: `net_sol_impact = losses_avoided_sol - wins_missed_sol`, `trade_off_ratio`
- Wynik: tabela z wyróżnionym "sweet spot"
- Parametry do sweepowania: jup_score min, mc_at_open min, token_age_days min

**StopLossLevelAnalyzer:**
- Buckety: -3%, -5%, -8%, -10%, -12%, -15%, -20%
- "Gdyby stop-loss był na -8%: uratowałbyś X SOL na Y pozycjach"

**WalletTrendAnalyzer:**
- Per wallet: stop_loss_rate overall vs 7d trend
- Flag: "wallet X: stop-loss rate 7d = 45% vs avg 18% — rozważ wyłączenie"

**Edge cases:**
- `close_reason == "still_open"`: wyklucz z backtestu
- `close_reason == "rug"` lub `"failsafe"`: traktuj jako loss
- `jup_score == 0` lub `None`: obsłuż osobno, nie zaliczaj

### Faza C — Source Wallet Timeline Analysis (wymaga Fazy A)

**Plik:** `valhalla/source_wallet_analyzer.py` (nowy)

Flow dla pozycji z `target_tx_signature`:
1. Użyj PositionResolver → resolwuj Target Tx → adres DLMM source walleta
2. Użyj MeteoraPnlCalculator → PnL source walleta
3. Wyznacz: open time, close time, hold duration, PnL%
4. Porównaj i sklasyfikuj scenariusz

Auto-klasyfikowane scenariusze:
- `held_longer` — bot zamknął na SL, source kontynuował i wyszedł lepiej
- `exited_first` — source wyszedł chwilę przed botem, delay dał gorszy kurs
- `both_loss` — token zawalił się dla wszystkich

Nowe kolumny w positions.csv (po Fazie A):
- `source_wallet_hold_min`
- `source_wallet_pnl_pct`
- `source_wallet_scenario`

Gdy dane niedostępne (stare logi, Meteora API nie zwraca danych): gracefully skip, log warning.

### Faza D — CLI Integration & Output (po Fazach A i B)

**Plik:** `valhalla_parser_v2.py` (modyfikacja)

Auto-generowany raport: `output/loss_analysis.md`
- Generowany przy każdym normalnym uruchomieniu
- Sekcje: Przegląd strat | Risk Profile | Filter Backtest | SL Distribution | Wallet Rankings | Source Wallet Comparisons (jeśli dane)
- Format: Markdown czytelny w terminalu, tabele z |---|---| separatorami
- Generuj ZAWSZE, nawet jeśli 0 stop-loss

Nowa flaga CLI:
```
python valhalla_parser_v2.py --backtest jup_score=80 mc=5000000 age=1
python valhalla_parser_v2.py --backtest jup_score=80 --wallet 20260125_C5JXfmK
```

Opcja: `--no-loss-analysis` (pomiń raport dla szybkości)

## Kolejność implementacji

1. Faza A (readers + models + csv_writer + json_io) — prerequisite dla C
2. Faza B (loss_analyzer.py) — niezależna, działa na istniejących danych
3. Faza D (CLI integration) — po A i B
4. Faza C (source_wallet_analyzer.py) — opcjonalna, osobna iteracja

## Conventions (z istniejącego kodu)

- Rate limiting: 0.3s sleep między API calls (jak w meteora.py)
- Retry logic: exponential backoff, 5 prób (jak w solana_rpc.py)
- Decimal dla SOL: ZAWSZE, nigdy float dla wartości finansowych
- Cache: AddressCache z solana_rpc.py
- API calls: urllib.request (stdlib, bez zewnętrznych deps)
- Nowe kolumny w positions.csv ZAWSZE na końcu (backwards compat)
- Nowe pola w modelach: Optional z field(default=None)

## Commit strategy

- Branch: `claude/loss-autopsy-backtester`
- Commit po każdej Fazie osobno
- Conventional commits: feat:, fix:

## Weryfikacja

1. `python valhalla_parser_v2.py` → sprawdź nowe kolumny w positions.csv
2. `cat output/loss_analysis.md` → czy tabele risk profile mają sensowne liczby
3. `python valhalla_parser_v2.py --backtest jup_score=85` → sprawdź: wins_excluded + wins_kept == total_wins
4. Dla jednej pozycji stop-loss z target_tx_signature: zweryfikuj daty/PnL vs Solscan/Metlex ręcznie
5. Regresja: stare pozycje bez zmian, nowe kolumny mogą być puste
