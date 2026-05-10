# Szukam partnera technicznego — autonomiczny copy-trading na Meteora DLMM
### [Partnerstwo] Projekt zarobkowy z potencjałem komercjalizacji

---

## Co to jest

Platforma do autonomicznego copy-tradingu — dziś na Meteora DLMM, docelowo
agnostyczna względem rynku.

**Faza 1 (aktualna):** Meteora DLMM na Solanie — śledzę portfele najlepszych traderów
liquidity, mierzę rzeczywisty PnL on-chain, automatycznie generuję rekomendacje zmian strategii.

**Faza 2 (docelowa):** ten sam model przeniesiony na inne DEXy o wysokiej płynności —
podpinamy inne API, rdzeń analityczny i logika decyzyjna zostają bez zmian.
Agent zamiast Human in the Loop.

---

## Jak to działa — mechanizm

### Czym jest Meteora DLMM

Meteora DLMM (Dynamic Liquidity Market Maker) to protokół DEX na Solanie oparty na
**skoncentrowanej płynności** — LP (dostawca płynności) nie rozlewa kapitału po całym
rynku, tylko ustawia go w konkretnym przedziale cenowym ("binach"). Mechanika
nawiązująca do Uniswap v3 z nieco większymi możliwościami.

Gdy cena tokenu jest w tym przedziale → LP zarabia fee z transakcji.  
Gdy cena wychodzi poza przedział → LP przestaje zarabiać, narasta impermanent loss.

Dlatego dobry LP **aktywnie zarządza pozycjami**: zamyka gdy cena wychodzi poza zakres,
otwiera nową w nowym przedziale, wybiera strategię dystrybucji (Spot / Bid-Ask / Curve).
Najlepsi LP na Meteorze robią to systematycznie i generują powtarzalne zyski.

### Copy-trading w tym kontekście

Obecnie wykorzystywany przeze mnie Valhalla Bot śledzi wybrane portfele (source wallets) 
i kopiuje ich ruchy. W uproszczeniu:

```
Source wallet otwiera pozycję DLMM
    → Valhalla Bot wykrywa zdarzenie on-chain
    → Otwiera identyczną pozycję na twoim portfelu
    → Gdy source zamyka → twój bot zamyka
```

Efekt: zarabiasz tyle co śledzony LP, proporcjonalnie do twojego kapitału - spread i opłaty.

### Gdzie jest wartość — i problem

**Wartość:** nie musisz sam analizować rynku. Kopiujesz kogoś, kto to robi lepiej.

**Problem:** sam bot jest mechaniczny. Nie ocenia czy:
- śledzony portfel nadal przynosi wyniki (może mu się pogorszyło)
- parametry bota są właściwe dla danego portfela (min MC, Jup score, stop-loss)
- są lepsze portfele do śledzenia niż te aktualnie ustawione
- nie mamy wpływu na kształt inputu i outputu - konfiguracja bota to ręczne klikanie,
powiadomienia są niekonfigurowalne, parsowanie ich jest kwadratowe

To robię ręcznie lub półautomatycznie — i to właśnie chcę zautomatyzować.

---

## Problem rynkowy

Copy-trading na płynnych DEXach działa, ale jest ślepy.

Istniejące boty kopiują portfele mechanicznie — bez oceny, czy dany portfel nadal ma sens,
jakie parametry śledzenia przynoszą wynik, kiedy zamienić portfel na lepszy.
Trader albo robi to ręcznie (czasochłonne, wolne), albo nie robi wcale (kapitał na złych portfelach).

**Luka:** nikt nie zamknął pętli między danymi a konfiguracją bota.

Ten problem istnieje na każdym DEXie z aktywnymi traderami liquidity — Meteora to punkt wejścia,
bo rozumiem go najlepiej, ale architektura jest zaprojektowana pod rozszerzalność.

---

## Co mam dziś — warstwa analityczna (gotowa)

### Pipeline danych

```
Valhalla Bot (Discord DM)
    ↓
Parser logów (HTML/text) — auto-detekcja formatu
    ↓
Solana RPC — rozwiązanie adresów pozycji on-chain
    ↓
Meteora DLMM API — dokładny PnL per pozycja
    ↓ (cross-check)
LpAgent API — wykrywanie luk w logach Discord
    ↓
positions.csv / summary.csv / loss_analysis.md / wallet_trend.md
    ↓
8 wykresów (daily PnL, win rate, rugi, rolling avg...)
```

### Co system mierzy

| Metryka | Co to daje |
|---|---|
| PnL per pozycja (SOL + %) | źródło prawdy — nie Discord-diff, tylko on-chain |
| Win rate 24h / 72h / 7d | trend portfela, nie snapshot |
| Rug rate | selekcja portfeli ze złą oceną ryzyka |
| SOL/day per wallet | ranking efektywności |
| % portfela w otwartych pozycjach | monitoring ekspozycji |
| Insufficient balance events | detekcja problemów z kapitałem |
| Coverage gaps | luki w logach (kiedy bot milczał) |

### Rekomendacje (wygenerowane automatycznie)

System analizuje dane i produkuje gotowe wskazania:
- Który portfel zastąpić
- Jakie parametry filtrowania zmieniać (MC cap, Jup score, token age)
- Kiedy portfel "umiera" (brak aktywności, pogorszone wyniki)
- Które pozycje to statystyczne anomalie, a które trend

### Wyniki — PoC w liczbach

System działa od ~5 miesięcy. Portfel urósł z sukcesywnie dokładanego kapitału
(łącznie 44.6 SOL) do obecnych **60.2 SOL — +15.6 SOL zysku (+35% na wniesionym kapitale)**.

Annualizowany zwrot orientacyjnie ~84% — zastrzeżenie: kapitał był dokładany stopniowo,
więc dokładne TWRR wymaga dat wpłat, ale rząd wielkości jest taki.

Zarządzanie ryzykiem: w danej chwili śledzę 8 aktywnych portfeli z ekspozycją
rozłożoną na max 50 otwartych pozycji jednocześnie. Strata na pojedynczej pozycji
nigdy nie jest dotkliwa dla całości.

### Architektura techniczna

Czysty Python, modularny pakiet, ~6,000 linii — **kod jest publiczny na GitHubie**,
każdy może sprawdzić działanie modułu analityki na własnych danych przed podjęciem decyzji.

- `valhalla/` — parser, RPC client, Meteora API, raportowanie
- `valhalla/pipeline/` — orkiestracja
- `valhalla/recommendations/` — logika rekomendacji
- `valhalla/loss_report/` — analiza strat
- Zero zewnętrznych zależności (stdlib-only rdzeń, `matplotlib` opcjonalnie)

Kod powstawał iteracyjnie (nie zawsze "po bożemu"), ale refactoring jest w toku —
planuję go domknąć przed onboardingiem partnera, żeby wchodził w czysty stan.

---

## Czego NIE mam — domknięcie pętli

Tu jest miejsce dla partnera.

### Problem 1: Rekonfiguracja bota

Valhalla Bot to zewnętrzny produkt — nie mój. Nie mogę automatycznie:
- zmienić listy śledzonych portfeli
- dostosować parametrów (stop-loss, kapitał per pozycja, filtry)

**Teraz:** analizuję dane → decyduję ręcznie → ręcznie zmieniam konfigurację bota.  
**Cel:** skrypt odbiera rekomendacje → zmienia konfigurację automatycznie.

To wymaga **własnego bota kopiującego** — budowanego od zera. Jestem w stanie
dostarczyć pełną specyfikację logiki obecnego bota (jak działa, co kopiuje, jakie parametry),
plus mam własne przemyślenia co można zrobić lepiej niż robi to Valhalla.

### Problem 2: Rotacja portfeli i pipeline odkrywania

To jest równoległa, krytyczna część systemu.

Najlepszy bot jest bezużyteczny jeśli śledzi złe portfele. Portfele "umierają" —
Wallet zmienia styl, przenosi kapitał, staje się nieaktywny. Trzeba go zastąpić.

**Dziś mam:**
- Ranking aktywności portfeli (wallet_trend_report) — wiem, który portfel "umiera"
- Zapytanie Dune SQL, które przeszukuje on-chain historię i wyłania kandydatów
  do śledzenia (portfele z dobrym PnL, wysoką aktywnością, niskim rug rate)
- Dune ma serwer MCP — jest przestrzeń do automatyzacji tego zapytania
- Inne źródła kandydatów (własne obserwacje, sieci społecznościowe DeFi)

**Czego brakuje — pipeline walidacji:**

```
Źródła kandydatów
    ├── Dune SQL (on-chain analytics) — zautomatyzować
    ├── ...
    └── Inne źródła (ręczne obserwacje, alpha leaks)
              ↓
    Walidacja kandydata
    ├── Min. N pozycji w ostatnich X dniach
    ├── Win rate > próg
    ├── Rug rate < próg
    ├── ...
    └── Aktywność regularna (nie flash-trader)
              ↓
    Lista rezerwowa (ranked bench)
    — zawsze gotowe portfele do podstawienia
              ↓
    Auto-rotacja
    — portfel aktywny spada poniżej progu → zamień na #1 z bench
```

**Cel:** nigdy nie być w sytuacji gdzie bot stoi, bo wszystkie śledzone portfele
są nieaktywne, a nie ma co podstawić. Bench zawsze pełny.

### Problem 3: Zbieranie logów

Dziś: albo kopiuję DM z Discorda ręcznie (Ctrl+C, paste, parser) albo korzystam
z ryzykownego rozwiązania do automatycznego przechwytywania DMów z Discorda  
Cel: webhook → dane wpływają automatycznie.


---

## Docelowa architektura — zamknięta pętla

```
┌─────────────────────────────────────────────────────────────┐
│                    Autonomiczny system                       │
│                                                             │
│  Bot on-chain ──→ Logi (auto) ──→ Analityka ──→ Rekomendacje│
│       ↑                                              │      │
│       │                                              │      │
│       └─────── Rekonfiguracja ←── [HITL lub AI] ←───┘      │
│                                                             │
│  Równolegle:                                                │
│  Ranking portfeli ──→ Lista rezerwowa ──→ Auto-rotacja      │
└─────────────────────────────────────────────────────────────┘
```

**HITL (Human in the Loop)** — faza przejściowa: człowiek zatwierdza rekomendacje,
skrypt je wdraża.

**AI Agent** — faza docelowa: agent skonfigurowany z góry (cel, limity ryzyka,
priorytety) podejmuje decyzje autonomicznie. Dostaje narzędzia: `swap_wallet()`,
`set_param()`, `approve_recommendation()`.

---

## Co wnoszę / czego szukam

### Wnoszę

- Kompletna warstwa analityczna (kod, logika, dane historyczne)
- Koncepcja i logika produktu
- Domain knowledge: Meteora DLMM, Valhalla Bot, selekcja portfeli, ocena ryzyka
- Dane: historia pozycji, testy, cross-check z LpAgent API
- Dune SQL — działające zapytanie do odkrywania nowych portfeli on-chain
- Własne źródła kandydatów i metody walidacji (wypracowane empirycznie)

### Szukam

**Partnera technicznego — programisty on-chain z doświadczeniem na Solanie.**

Nie szukam pracownika ani inwestora — szukam kogoś, kto chce razem zbudować narzędzie
zarabiające dla nas obojga, być może docelowo do skomercjalizowania.

Kluczowe kompetencje:
- Budowanie własnego copy-trading bota (Meteora DLMM SDK, `@meteora-ag/dlmm`)
- Obsługa transakcji na Solanie
- Swap layer (Jupiter API) — zakup tokenu przed wejściem w pozycję
- Bezpieczna obsługa klucza prywatnego na serwerze
- Doświadczenie z edge case'ami bota on-chain
- Umiejętność code review i integracji z istniejącym kodem (warstwa analityczna jest gotowa,
  wymaga przejrzenia i spinania z nowym botem)

Sam pisałem bota (off-chain, jednorynkowy, API) — wiedza analityczna jest, ale
raz po raz zderzałem się z edge case'ami, które nieoczekiwanie zużywały kapitał.
Do bota on-chain potrzebuję kogoś, kto te pułapki zna z praktyki.

**Kapitał na start nie jest wymagany.** Infrastruktura (VPS + RPC) to na początkowym etapie
są miedziaki.

### Model współpracy

**Faza 1 — każdy zarabia na swoim:**

Każdy z nas uruchamia własną instancję bota na własnej infrastrukturze, ze swoim portfelem
i swoim kapitałem. Ile kto w to wkłada, tyle wyciąga. Bez dzielenia się zyskami, bez
wspólnego konta, bez sporów.

Wspólny jest kod i wiedza — każde usprawnienie, które jeden z nas wdroży, wchodzi do
wspólnej bazy i oboje na tym korzystamy. Możemy dzielić koszty RPC (wspólny plan,
podzielona faktura).

To co szukam to **partner, nie pracownik.** Ktoś, kto chce zarobić na własnym narzędziu —
nie ktoś, kto dostanie za to zapłatę.

**Faza 2 — komercjalizacja (jak już zażre):**

Kiedy system działa generując powtarzalne wyniki, pojawia się pytanie o produkt dla innych.
Wtedy razem decydujemy jak to skomercjalizować — z zewnętrznym finansowaniem jeśli trzeba.
PoC już jest: system analityczny działa, rekomendacje są trafne, decyzje egzekwuję ręcznie
na ich podstawie i to przynosi wynik. Brakuje tylko domknięcia pętli automatyzacji.

---

## Stan projektu / gdzie jesteśmy

| Element | Status |
|---|---|
| Parser logów Discord | ✅ gotowy |
| Solana RPC + Meteora API client | ✅ gotowy |
| Analiza PnL per pozycja | ✅ gotowy |
| Raporty (loss, wallet trend, charts) | ✅ gotowy |
| Silnik rekomendacji | ✅ gotowy |
| LpAgent cross-check (luki w danych) | ✅ gotowy |
| Modularny pakiet Python | ✅ gotowy (refactor zakończony) |
| Auto-rekonfiguracja bota | ❌ brak bota |
| Rotacja portfeli (logika) | ✅ gotowa (wallet_trend_report) |
| Auto-rotacja portfeli (wykonanie) | ❌ brak połączenia z botem |
| Dune SQL — odkrywanie kandydatów | 🔶 zapytanie gotowe, brak automatyzacji |
| Pipeline walidacji kandydatów | ❌ do zaprojektowania |
| Lista rezerwowa (bench) | ❌ brak struktury |
| Pełna automatyzacja zbierania logów | 🔶 częściowo (DCE pull) |
| AI Agent zamiast HITL | ❌ faza docelowa |

---

## Roadmap rozszerzenia — agnostyczność rynkowa

Warstwa analityczna jest dziś napisana pod Meteora DLMM, ale logika jest uniwersalna:
**obserwuj portfele → mierz PnL → rankinguj → rekomenduj → zamknij pętlę.**

```
Faza 1 — Meteora DLMM (Solana)          ← tutaj jesteśmy
    Warstwa analityczna: gotowa
    Bot on-chain: do zbudowania
    Pętla zwrotna: do domknięcia

Faza 2 — Inne DEXy wysokiej płynności
    Przykłady: Orca (Solana), Uniswap v3/v4 (Ethereum/Base), Raydium CLMM
    Co się zmienia: adapter danych + executor on-chain (nowe API)
    Co zostaje bez zmian: analityka, silnik rekomendacji, AI agent, logika HITL
```

**Dlaczego to działa architektonicznie:**

Rdzeń systemu operuje na abstrakcjach (`pozycja`, `PnL`, `portfel`, `rekomendacja`).
Każdy rynek wymaga jednego adaptera wejścia (dane on-chain → nasze modele)
i jednego adaptera wyjścia (rekomendacja → tx na danym protokole).
Logika decyzyjna i warstwa AI pozostają wspólne.

To oznacza, że każdy kolejny rynek to incremental cost — nie budowanie od zera.

---

## Dlaczego teraz

- Meteora DLMM to jeden z najbardziej aktywnych protokołów DeFi na Solanie (TVL >$1B)
- Copy-trading na DEXach koncentrowanej płynności (DLMM/CLMM) jest niszą z wysoką barierą wejścia
- Warstwa analityczna jest gotowa — jest co podłączyć, nie zaczynamy od zera
- Model agnostyczny: każdy kolejny rynek to adapter, nie nowy produkt
- Fala DEXów koncentrowanej płynności na EVM (Uniswap v4, Ambient) otwiera kolejne rynki

---

*Kontakt: [john.spamowy+claude@gmail.com]*  
*Data: 2026-04-30*
