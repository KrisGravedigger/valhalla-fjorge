# PLAN: Refaktoryzacja `valhalla-fjorge`

> Status: **KONSENSUS Claude ↔ Codex** (iteracja 1, uzgodniony). Żadne zmiany w kodzie jeszcze nie wykonane — czeka na akceptację użytkownika.
> Autor orkiestracji: Claude Code. Głos doradczy: Codex (advisory input + runda debaty z 2026-05-29).
> Data: 2026-05-29.

---

## TL;DR

1. **Diagnoza strukturalna Codexa jest trafna i empirycznie potwierdzona** — `match_positions()` to praktycznie jednofunkcyjny plik (~735 linii), `main()` w `cli.py` ~930 linii, `merge_with_existing_csv()` ~470 linii. Mieszanie warstw (CLI / I/O / domena / render / terminal) jest realne.
2. **Codex niezależnie odtworzył istniejący backlog projektu** (`TODO.md` #137–#146). To dobra wiadomość: dwa niezależne przeglądy się zbiegły. Plan poniżej mapuje się 1:1 na te tickety.
3. **Pozorna sprzeczność z `docs/022` w większości się rozpływa.** Doc 022 wetował split **na pliki/pakiety**, nie ekstrakcję metod **wewnątrz pliku**. Dla `matcher`/`merge`/`loss_analyzer` proponujemy in-file extraction — to jest zgodne z 022. Jedyny realny spór dotyczy `charts.py` (i tak odkładany przez wszystkich).
4. **Fundament weryfikacji wymaga naprawy, ale jest w lepszym stanie niż sugeruje Codex.** Testy przechodzą (231 passed, 1 skipped) po `-p no:anchorpy` i ograniczeniu do `tests/`. Istnieje też pełny baseline harness (`tests/verify_baseline.py`). Problem: snapshot baseline jest z **25 kwietnia** (nieaktualny), brak setup-files, `pytest.ini` skasowany.
5. **#146 (sprzeczny PnL z 4 źródeł) zostaje POZA refaktorem.** Baseline z założenia zamraża obecne — być może błędne — liczby. Refaktor zachowuje zachowanie; korekta PnL to osobne śledztwo.

Największy praktyczny zwrot, kolejno: **fundament weryfikacji → `cli.py` → `matcher.py` → `merge.py`**.

---

## 1. Stan faktyczny (zweryfikowany, nie z pamięci)

### 1a. Rozmiary (potwierdzone `wc -l` + `grep`)

| Plik | Linie | Największa jednostka | Uwaga |
|---|---|---|---|
| `charts.py` | 1528 | `generate_charts()` ~500 | kohezyjny per-wykres |
| `reconcile.py` | 1355 | — | dataclasses + loaders + renderers + CLI |
| `loss_analyzer.py` | 1113 | `WalletScorecardAnalyzer.analyze()` ~290 | 6 niezależnych klas |
| `cli.py` | 1058 | **`main()` ~930** | jedna funkcja = cały workflow |
| `matcher.py` | 757 | **`match_positions()` ~735** | jedna metoda = cały plik |
| `internal_nav.py` | 649 | — | powstał po doc 022 |
| `merge.py` | 644 | **`merge_with_existing_csv()` ~470** | I/O + dedupe + upgrade + terminal |
| `loss_report/report_builder.py` | 620 | `generate_loss_report()` ~597 | analiza + markdown + format |
| `event_parser.py` | 588 | router + ~12 `_parse_*` | jedna odpowiedzialność |

### 1b. Stan test harness

- ✅ `python -m pytest tests/ -p no:anchorpy` → **231 passed, 1 skipped** (~5 s).
- ✅ Istnieje `tests/verify_baseline.py` — golden-diff vs `_baseline_pre_refactor/`, czyste CLI (`--parse`, `--report`, `--include-charts`), sensowne kody wyjścia.
- ⚠️ `pytest.ini` skasowany w worktree (`D pytest.ini`), istnieje tylko `pytest [conflicted].ini` z `addopts = -p no:anchorpy --basetemp=pytest_basetemp` — **bez `testpaths = tests`**.
- ⚠️ Bare `pytest` zbiera `tools/test_meteora.py`, które robi `exit(1)` bez `WALLET_ADDRESS` → kolekcja pada.
- ⚠️ Brak `pyproject.toml` / `requirements*.txt` / lockfile → środowisko nieodtwarzalne.
- 🔴 **Baseline jest nieaktualny**: `_baseline_pre_refactor/positions.csv` z 25 kwietnia (3.2 MB) vs `output/positions.csv` z 29 maja (4.7 MB). `verify_baseline.py` w obecnym stanie diffowałby brudno albo maskował regresje.

**Wniosek:** krok 1 to nie „wepnij zielony check", tylko „napraw konfigurację + **zrób świeży baseline z aktualnego HEAD** i udowodnij, że jest zielony".

### 1b-bis. 🔴 BLOKER ŚRODOWISKOWY: narzędzie synchronizacji tworzy pliki „[conflicted]" (odkryty w S1)

Podczas realizacji S1 wykryto, że na `C:\nju` działa narzędzie synchronizacji/backupu, które przy **nadpisywaniu istniejącego pliku** (gdy ma własną rozbieżną kopię) tworzy kopię z sufiksem `[conflicted]` zamiast czysto nadpisać. Dowody:
- `pytest [conflicted].ini` + `D pytest.ini` (kanoniczny plik **zniknął** z worktree — to groźny kierunek rozwiązania konfliktu).
- `_test_output/loss_analysis [conflicted].md` powstał **na żywo** podczas `verify_baseline.py --report` (timestamp z przebiegu).
- `archive/messages_for_cli (10) [conflicted].txt`.
- Kod NIE produkuje tego sufiksu (grep czysty); nowy, nieistniejący wcześniej plik zapisuje się czysto (test `__synctest.md`). Więc konfliktowane są tylko pliki z rozbieżną kopią po stronie narzędzia.
- Żaden znany proces (Dropbox/OneDrive/Syncthing/Resilio/pCloud/MEGA/Nextcloud/GDrive) nie pasował do `Get-Process` — nazwa narzędzia jest wiedzą użytkownika.

**Dlaczego to blokuje refaktor:** edycja in-place śledzonego źródła (`cli.py`, `matcher.py`, `merge.py`) może wylądować w `cli [conflicted].py`, a kanoniczny plik wrócić do wersji narzędzia → **cicha korupcja refaktoru**. Przypadek `pytest.ini` dowodzi, że kanoniczna nazwa potrafi zostać utracona. Redirect temp-dirów NIE wystarcza — ryzyko dotyczy źródeł, nie tylko artefaktów. **S1 wstrzymana do rozwiązania (exclude/pause sync na `C:\nju` albo przeniesienie repo poza synchronizację).**

### 1b-ter. Realia harnessu (do zapamiętania przy wznowieniu)

- **`input/` jest puste** (pliki archiwizowane po przetworzeniu) → tryb `--parse` zwraca exit(4), **nie da się go zwalidować bez stagingu** slice'a wejścia już reprezentowanego w baseline. **`--report` jest codziennym checkiem**; `--parse` wymaga ręcznego podstawienia inputu.
- Świeży baseline `loss_analysis.md` brać z **regeneracji harnessu** (czysty `_test_output` po `--report`), NIE z kopii `output/` — wtedy `--report` jest zielony z konstrukcji. Potem potwierdzić drugim przebiegiem (determinizm).
- ✅ **ROZWIĄZANE (S1.5, 2026-05-30):** „delta ~131 B / drift" NIE był resztkową tajemnicą ani driftem recommendations-state (rec-state stabilny od 9 kwietnia). Root cause: rekomendacja **utilization** liczyła okno 72h względem wall-clock `now()` (`utilization.py`), a drugi seam — okno 24h insuf-events w `action_items.py`. Wszystkie pozostałe rekomendacje kotwiczą do danych. Z dnia na dzień okno zsuwało się → znikały 3 itemy „capital utilization" + adnotacje „↑ capital (util)". Naprawione w S1.5 (kotwica do `max(datetime_close)` przez `models.latest_position_datetime`). Po fixie loss_analysis.md jest deterministyczny (podwójny przebieg identyczny). Baseline odtworzony na poprawionym zachowaniu.

### 1c. Higiena repo

- Worktree jest w praktyce czysty poza `pytest.ini` (reszta śmieci — `*.pytest-basetemp*`, `_temp/`, `_baseline_pre_refactor/` — już w `.gitignore`).
- `.gitignore` ma bardzo szerokie reguły (`docs/`, `*.txt`, `*.csv`, `*.json`) — chronią prywatne dane, ale ryzyko ukrycia ważnych plików (= TODO #142).
- Entry-pointy to cienkie shimy: `valhalla_parser_v2.py` → `valhalla.cli.main`, `main.py` → `valhalla.pipeline`. Kompatybilność łatwa do utrzymania.

---

## 2. Rozstrzygnięcie sporu z `docs/022` (sedno orkiestracji)

`docs/022-valhalla-package-review.md` orzekł **DO NOT SPLIT** dla `charts`, `loss_analyzer`, `matcher`, `merge`, `event_parser`. Codex i `TODO.md` #144 mówią „rozbić". Pozorny konflikt. Rozróżnienie, które go rozwiązuje:

> **Czy propozycja zmienia powierzchnię plików/importów, czy tylko wewnętrzną strukturę jednego pliku?**

Doc 022 wetował explicite **split na osobne pliki** (cyt. dla matchera: „splitting a single method's helpers **across files** would be anti-cohesive"). Nie wypowiedział się przeciw ekstrakcji 735-liniowej metody na nazwane metody prywatne w tej samej klasie.

### Bucketing modułów

**Bucket A — objęte werdyktem 022 → tylko in-file extraction (zero zmian importów):**
- `matcher.py` — ekstrakcja helperów *wewnątrz* `PositionMatcher`. Zgodne z 022.
- `merge.py` — ekstrakcja helperów *wewnątrz* modułu. Zgodne z 022.
- `loss_analyzer.py` — punktowy refaktor `WalletScorecardAnalyzer.analyze()`. Codex sam mówi „nie rozbijać na siłę" = zgodne z 022.
- `event_parser.py` — **nie ruszać** (022: „not now", Codex: „not first" — zgoda).
- `charts.py` — **odłożone**. Jedyny realny head-to-head: 022 wetuje pakiet `charts/` z konkretnym argumentem technicznym (`_draw_*` sprzężone ze stanem figure/axes z `generate_charts()`). Jeśli kiedyś wracamy — plan MUSI obalić ten argument, nie tylko powiedzieć „za duży".

**Bucket B — powstałe/rozrośnięte PO doc 022 → brak sprzeczności, swoboda projektowa:**
- `cli.py` (tworzony przez doc 020, 022 go nie analizował)
- `reconcile.py` (doc 029)
- `loss_report/report_builder.py` (doc 018)
- `internal_nav.py` (doc 030)

**Loose end:** po refaktorze dopisać do `docs/022` (lub krótki nowy doc) notkę: „022 dotyczy split-na-pliki; ten plan robi in-file extraction" — żeby kolejny czytelnik nie trafił na ten sam pozorny konflikt.

---

## 3. Sieć bezpieczeństwa (warunek pod warunkiem)

1. **Świeży baseline z HEAD.** Przed jakimkolwiek refaktorem semantycznie-ryzykownym: uruchomić `verify_baseline.py --parse --report` na obecnym kodzie, zregenerować `_baseline_pre_refactor/` z aktualnego wyjścia, potwierdzić zielony przebieg. Bramka: **żaden refaktor Bucket A / matcher / merge nie startuje, dopóki świeży baseline nie jest zielony.**
   - ⚠️ **Baseline pokrywa WĘŻSZY zakres niż „całe zachowanie" (flag Codexa).** `verify_baseline.py` ma świadome wykluczenia: `wallet_trend.md` wykluczony z diffa, porównanie wykresów (`--include-charts`) opt-in, `loss_analysis.md` ma stripowane generowane nagłówki. „Baseline zielony" ≠ „zachowanie w pełni zachowane". Dla matcher/merge to luka — dlatego krok 3/5 (testy kontraktowe) jest obowiązkowy, nie opcjonalny.
1-bis. 🔴 **Luka harnessu — bramka `--report` NIE porównuje CSV (odkryte w S1.5, 2026-05-30).** `REPORT_FILES = ["loss_analysis.md"]` — `--report` regeneruje `positions.csv`/`summary.csv` do `_test_output`, ale ich nie diffuje. Gdy je porównać do baseline (kod z `main`, więc problem pre-istniejący):
   - `positions.csv` — różni się TYLKO formatowaniem floatów (`1.0000`→`1.0`); wartości identyczne, deterministyczne. PnL nietknięte.
   - `summary.csv` — różnice z 3 przyczyn: (a) `skips` 23→0 = **artefakt trybu** (ścieżka merge przekazuje pustą listę skip_events, `merge.py:628`); (b) kolumny `*_24h/72h/7d` recency-windowed (ta sama klasa „daty" co utilization, niezakotwiczone); (c) drobny shift wins/losses (np. 569→565) = ścieżka merge przelicza staty inaczej niż capture. Deterministyczne w obrębie dnia.
   - **Znaczenie:** S5/S6 (matcher/merge) PRZEPISUJĄ `positions.csv`, a bramka ich nie chroni bezpośrednio. Pokrycie pośrednie (przez loss_analysis.md) istnieje, ale słabe. **→ krok S1.6 (przed S5):** re-capture deterministycznego CSV baseline + dodać `positions.csv` do `REPORT_FILES` + rozstrzygnąć okna summary. NIE blokuje S2.

2. **Luka harnessu — diff plików, nie stdout.** `verify_baseline.py` porównuje pliki wyjściowe, nie terminal. Codex wskazał „komunikaty terminalowe" jako jeden z mieszanych concernów, a #143 to wprost relokacja `print()`/`input()`. **Decyzja do podjęcia (punkt sporny D):** czy stdout jest częścią zachowania chronionego? Jeśli tak — albo łapać konsolę w baseline, albo trzymać całą relokację print poza krokami strukturalnymi (osobny późniejszy pass #143).
3. **Testy kontraktowe przed `matcher`/`merge`.** Oprócz baseline (output-level) dodać testy na konkretne ścieżki: close_reason (open/close/rug/failsafe/TP/SL), źródła PnL (meteora/discord/pending), unknown_open; dla merge — wszystkie upgrade paths.
4. **Kolejność, nie wielkie bang.** Każdy etap kończy się zielonym checkiem + krótką notką o zachowanej kompatybilności.
5. **Lazy imports w `cli.py`** — `main()` używa wielu importów lokalnych; to często ukrywa unikanie cykli. Split na `commands/*` może te cykle ujawnić. Zachować strukturę leniwych importów; nie „porządkować" ich przy okazji.

---

## 4. Sekwencja prac (mapa na TODO #137–#146)

| Faza | Zakres | TODO | Ryzyko | Bramka wyjścia |
|---|---|---|---|---|
| **0. Fundament** | `pyproject.toml`/`requirements*.txt`; przywrócić `pytest.ini` z `testpaths=tests` + `-p no:anchorpy`; usunąć `pytest [conflicted].ini`; gate `tools/test_meteora.py`; `scripts/check.ps1` (compile+pytest+baseline, wszystkie wywołania z `-NoProfile` — TODO #145); **świeży baseline z HEAD** | #137, #141, #145 | niskie | jedna komenda = zielono, świeży baseline zielony |
| **0.5. Pin domenowych seamów** ⟵ *reorder (Codex)* | **minimalne** testy kontraktowe matcher + merge PRZED tknięciem cli.py — bo CLI woła w te seamy i może je zmienić zanim zostaną przypięte | #141 | niskie | testy zielone |
| **1. cli.py — szkielet** | wydzielić `args.py` (argparse) + cienki dispatch w `main()` | #140 | średnie | CLI dział, baseline zielony |
| **2. cli.py — per command** | `commands/parse.py`, `report.py`, `cross_check.py`, `recalc.py`, `backtest.py`, `track.py`; shim `valhalla_parser_v2.py` bez zmian | #140 | średnie | każda komenda osobno + baseline |
| **3. matcher — kontrakty** | testy kontraktowe na obecne zachowanie | #141 | niskie | testy zielone (baseline behavior) |
| **4. matcher — in-file extract** | helpery: indeksowanie eventów, close_reason, builder `MatchedPosition`, strategie PnL, unknown_open. **Bez zmiany semantyki, bez zmiany importów.** | #144 | **wysokie** | baseline + kontrakty zielone |
| **5. merge — kontrakty** | testy na wszystkie upgrade paths | #141 | niskie | testy zielone |
| **6. merge — in-file extract** | CSV read/write, row→model, merge policy, upgrade decision, reporting. **In-file.** | #144 | **wysokie (dane)** | baseline + kontrakty zielone |
| **7. report_builder** | renderery sekcji (exec summary, action items, recent losses, scorecard, filter recs, per-wallet); snapshot markdown | #144 | średni | markdown snapshot zielony |
| **8. loss_analyzer punktowo** | rozbić `WalletScorecardAnalyzer.analyze()` (~290) | #144 | średni | testy zielone |
| **9. odłożone** | `charts.py` (obalić arg. 022 najpierw), `reconcile.py`, `event_parser.py` | #144 | — | dopiero po decyzji |
| **op. równoległe** | #138 config poza kodem, #142 gitignore, #139 daily run | #138/#142/#139 | niskie | wg potrzeby |

> 🔒 **#143 (relokacja `print()`/`input()`) jest ZAKAZANE w fazach 1–8.** Baseline diffuje pliki, nie stdout — przesunięcie wyjścia konsolowego przeszłoby niezauważone jako „refaktor bez zmiany zachowania". #143 idzie w osobny, późniejszy pass, ALBO musi być poprzedzone dodaniem testu transkryptu CLI / smoke. (Konsensus pkt D.)

---

## 4b. Mapa sesji (dzień-po-dniu, każda zostawia projekt w pełni używalny)

> **Inwariant bezpieczeństwa budżetu:** każda sesja kończy się na `scripts/check.ps1` zielonym ORAZ działającym pipeline (`python main.py` / `valhalla_parser_v2.py`). Między sesjami użytkownik robi normalny przebieg pipeline'a. Jeśli budżet padnie w środku sesji — ostatni *commit* na branchu jest zielony, a niedokończona sesja jest porzucana (`git`), nigdy nie zostawiając połowicznego stanu na używanej ścieżce. Refaktor jest „no-semantic-change", więc nawet po połowie sesji pipeline daje te same wyniki.

| Sesja | Fazy | Co | Ryzyko | Stan na końcu |
|---|---|---|---|---|
| **S1** ✅ | 0 | fundament: setup-files, `pytest.ini`, `check.ps1`, gate test_meteora, **świeży baseline z HEAD** | niskie | tylko DODANO infrastrukturę; pipeline bez zmian; check zielony |
| **S1.5** ✅ *(correctness pre-step, 2026-05-30)* | — | kotwica okna utilization + insuf-24h do daty danych (`models.latest_position_datetime`); re-baseline; rozwiązuje nieodtwarzalność loss_analysis.md (§1b-ter) | niskie (izolowane, świadoma zmiana zachowania, osobny commit) | check zielony i deterministyczny; TODO #147 |
| **S2** ✅ *(2026-05-31)* | 0.5 | minimalne testy kontraktowe matcher (S2a, `test_matcher_contract.py`, 21) + merge (S2b, `test_merge_contract.py`, 21) — czyste dodatki, zero zmian produkcyjnych. **S2b odkrył pinowany quirk:** lpagent old-id-format fallback (addr[:8] vs addr[:4]+addr[-4:]) podwójnie dolicza wiersz Discord (replace + pętla "new") → kandydat #146/cleanup, NIE naprawiać w refaktorze. | niskie | testy przypinają seamy; full suite 272 passed, 1 skipped; pipeline bez zmian |
| **S1.6** ✅ *(2026-06-01, harden CSV gate)* | — | domknięto lukę bramki CSV (§3.1-bis). **Empirycznie udowodnione:** `--report` deterministyczny across-runs ORAZ fixed-point re-merge dla `positions.csv` i `summary.csv`. Re-capture baseline z czystego `_test_output` (nie `output/`); dodano **oba** CSV do `REPORT_FILES` (decyzja użytkownika). **Diff vs stary baseline w pełni skategoryzowany:** positions.csv = 10 float-format + 33 `still_open` gubi `target_wallet_address`; summary.csv = `skips=0` (artefakt trybu) + realne shifty wins/losses/rugs/windows (ścieżka merge liczy agregaty inaczej). Baseline (gitignored) zamraża obecne zachowanie — **2 pinowane quirki** (still_open addr, skips=0) udokumentowane w `verify_baseline.py`, NIE naprawiane (decyzja: pin). Bramka zielona z konstrukcji + `check.ps1` zielony. | średnie | bramka chroni CSV; S5/S6 mają realną siatkę |
| **S3** ✅ *(2026-06-01)* | 1 | `cli.py`: wydzielono `valhalla/args.py` (`build_parser()` + `parse_args(argv=None)`); `main()` woła jeden `_parse_cli_args()`. Argumenty 1:1 (nazwy/defaulty/choices/help/SUPPRESS/nargs), `print/input` nietknięte (#143), lazy-importy zachowane. cli.py 1058→1008. Delegowane do Codexa, zweryfikowane przez Claude Code (check.ps1 zielony, PnL 36.7321 SOL bez zmian). Branch `claude/s3-cli-args`. | średnie | CLI działa identycznie; baseline zielony |
| **S4a** ✅ *(2026-06-01)* | 2 | `cli.py`: utworzono pakiet `valhalla/commands/` + ekstrakcja **cross_check** (verbatim, importy z `lpagent_pipeline`/`lpagent_client`, zero z cli.py) i **track** (wrapper na `track_mode`). Dispatch przez lazy local imports w `main()`. Ryzyko cyklu wykluczone empirycznie przed pracą. Delegowane do Codexa (po resecie limitu), zweryfikowane przez Claude Code: check.ps1 zielony, PnL 36.7321 SOL bez zmian, smoke `--cross-check`. cli.py 1008→961. Branch `claude/s4a-cli-commands`. | średnie | CLI działa identycznie; baseline zielony |
| **S4b-0** ✅ *(2026-06-01)* | 2 | **Bramka charakteryzacyjna parse** (warunek konieczny S4b). Odkryto z advisorem: faza parse (Steps 1-5) jest **niewidoczna dla check.ps1** — bramka odpalała tylko `--report` z `--no-input`. Aktywowano uśpiony tryb `--parse`: stabilny gitignored `_parse_fixture/` (slice 05-28 z archive, już complete w baseline → merge idempotentny), `summary.csv` usunięte z `PARSE_FILES` (kolumna `skips` to artefakt trybu, positions.csv pokrywa parse), `check.ps1` dostał krok `--parse`. Empirycznie: positions/skip_events/insuf byte-identyczne (fixed point, bez re-capture). Zero zmian produkcyjnych. Delegowane do Codexa, zweryfikowane przez Claude Code. Branch `claude/s4b0-parse-gate`. | niskie | check.ps1 zielony z --parse + --report |
| **S4b-1** ✅ *(2026-06-02)* | 2 | `cli.py`: wydzielono Step 1 (read/parse/dedup) z `main()` do helpera `_read_and_parse_input_files(input_files, args)` → `(event_parser, processed_files)`. Verbatim move (tylko dedent), `input()` w 293 nietknięty (#143), helper in-file (zero nowych importów/cykli). Inicjalizatory pre-branch (~165/167) nietknięte, bug `parser.error` zamrożony. Pokryty przez --parse gate (S4b-0). Delegowane do Codexa przez `codex exec` (świeża instancja CLI — app-server zawieszony, kredyty wyczerpane i odnowione w trakcie), diff zweryfikowany linia-po-linii przez Claude Code, check.ps1 zielony (272 passed, PnL 36.7321 SOL na --report + --parse). Branch `claude/s4b1-parse-step1`. | **wysokie** | CLI działa identycznie; baseline zielony |
| **S4b-2** ✅ *(2026-06-02)* | 2 | `cli.py`: wydzielono Step 3 (resolve addresses) z `main()` do helpera `_resolve_addresses(event_parser, args, cache_file, already_complete_ids, positions_csv)` → `(resolved_addresses, cache)`. Verbatim move (dedent only): gałąź RPC + gałąź cache-only + lpagent seeding znak-w-znak (`end=''`/`flush=True`, `cache.save()`). **`cache` zwracany** bo Step 5.7 (source wallet) go konsumuje/zapisuje — gate-covered (5.7 wykonuje się w --parse). Blok `already_*_ids` został w main(). **Gałąź RPC verification-dark** (bramka `--skip-rpc`) → zweryfikowana przeglądem diffa linia-po-linii, nie bramką; NIE dodano mockowanych testów RPC (osobny scope, nie no-semantic-change). Delegowane przez `codex exec`, diff zweryfikowany przez Claude Code, check.ps1 zielony (272 passed, cache-only+seeding exercised, PnL 36.7321 SOL). Branch `claude/s4b1-parse-step1` (wspólny z S4b-1). | **wysokie** | CLI działa identycznie; baseline zielony |
| **S4b-3** ⏳ | 2 | `cli.py`: ostatni kawałek. **WYSOKIE ryzyko.** S4b-3 = Step 4 meteora + **duplikat w Step 8** (re-odpala match/merge/csv; unifikować w helper TYLKO jeśli byte-identyczny diff vs Step 4, inaczej = zmiana semantyki). Steps 4/8 verification-dark (sieć, `--skip-meteora` w bramce) → mogą wymagać mockowanych testów albo samego przeglądu diffa (jak gałąź RPC w S4b-2). `--merge`/`--recover-insuf` = churn, zostawić. `recalc` NIE istnieje w cli.py. wallet_trend (6.5c) wykluczony z baseline → dark bez własnego snapshotu. Thin wrappery (loss/charts/export/archive) zostawić. | **wysokie** | jw.; shim `valhalla_parser_v2.py` bez zmian |
| **S5** | 4 | `matcher.py`: in-file extraction (helpery wewnątrz `PositionMatcher`) | **wysokie** | baseline + kontrakty zielone |
| **S6** | 6 | `merge.py`: in-file extraction | **wysokie (dane)** | jw. |
| **S7** | 7 | `report_builder.py`: renderery sekcji | średni | markdown snapshot zielony |
| **S8** | 8 | `loss_analyzer.py`: rozbić `WalletScorecardAnalyzer.analyze()` | średni | testy zielone |

Sesje operacyjne (#138/#142/#139) — wstawiane elastycznie między powyższe, bo są niezależne i niskiego ryzyka. `charts`/`reconcile`/`event_parser` (#144 reszta) — dopiero po decyzji, poza tą sekwencją.

**Reguła rozbicia sesji:** jeśli S3 albo S5/S6 okaże się za duże na jeden budżet, dzielimy je po jednym wydzielonym helperze/komendzie — każdy mikro-krok kończy się zielonym baseline i jest osobnym commitem. Nigdy nie zostawiamy modułu w stanie „pół-wyekstrahowanym" bez zielonego checka.

---

## 5. Punkty sporne do debaty z Codexem

> **ROZSTRZYGNIĘTE w rundzie debaty z Codexem (2026-05-29). Konsensus we wszystkich czterech.**

- **(A) In-file extraction vs split-na-pliki — ✅ ZGODA.** Codex potwierdza: in-file extraction spełnia jego intencję strukturalną bez łamania doc 022 (kryterium 022 to „dwa+ niezależnie użyteczne podsystemy"; matcher i merge są single-domain). Brak konkretnego powodu na split-na-pliki teraz. **Zastrzeżenie Codexa:** ekstrakcja musi być realną strukturą (nazwane fazy, małe helpery, testy wokół close_reason / upgrade decision), nie arbitralnym cięciem na kawałki.
- **(B) Stan harnessu — ✅ ZGODA.** Codex wycofuje „pytest nie startuje" jako nagłówek. Przyjęta re-diagnoza: zweryfikowane 231/1, realny problem to krucha konfiguracja, brak setup-files, narzędzia spoza testów uciekające do discovery.
- **(C) Strategia baseline — ✅ ZGODA.** Świeży baseline z HEAD przed jakąkolwiek pracą nad matcher/merge jest obowiązkowy. Stary albo blokuje legalne obecne zachowanie, albo uczy ignorować czerwień.
- **(D) stdout w kontrakcie — ✅ ZGODA (kwarantanna).** Relokacja `print()`/`input()` idzie do osobnego passu #143, NIE do kroków strukturalnych. Harness porównuje pliki (`compare_outputs()`), nie transkrypt. Rozszerzanie kontraktu matcher/merge o stdout nieuzasadnione teraz — chyba że ktoś wcześniej doda osobny smoke test transkryptu CLI.

---

## 6. Poza zakresem (świadomie)

- **#146 — „jaki w końcu jest PnL"** (loss ~20 SOL vs wallet_trend ~5 vs własne ~15 vs LpAgent). To problem **poprawności**, nie struktury. Baseline z definicji zamraża obecne liczby (być może błędne). Refaktor zachowuje zachowanie; #146 to osobne śledztwo. **Zakaz „naprawiania PnL przy okazji".**

> **Doprecyzowanie zasady „brak zmian semantyki" (po S1.5):** zakaz dotyczy mieszania korekt poprawności w **kroki strukturalne** (fazy 1–8). Świadome, izolowane korekty *między* etapami — osobny branch/commit + re-baseline + nota w TODO — są dozwolone i są wręcz **celem** rozbicia refaktoru na sesje (decyzja użytkownika, 2026-05-30). S1.5 (kotwica utilization) to wzorcowy przykład: nie był to fix PnL, nie tknął matcher/merge/positions.csv, a po nim baseline jest deterministyczny.
- Zmiany semantyki klasyfikacji pozycji lub reguł PnL.
- Naprawianie niezwiązanego brudnego worktree bez decyzji użytkownika.

---

## 7. Zasady bezpieczeństwa (z advisory Codexa, przyjęte)

- Nie robić szerokiego refaktoru bez zielonego, powtarzalnego checka.
- Nie mieszać refaktoru strukturalnego ze zmianą semantyki PnL.
- Przy `matcher`/`merge` zachować baseline output.
- Każdy etap kończy się testami i krótką notką o kompatybilności.
- Nie naprawiać unrelated dirty worktree bez decyzji użytkownika.
