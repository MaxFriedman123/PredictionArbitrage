# Cross-Platform Sports Arbitrage Scanner

A continuous arbitrage detection system that scans moneyline betting markets across **Kalshi** and **Polymarket** in real time. When a game's combined ask prices across both platforms total less than $0.99, a guaranteed profit opportunity exists -- the scanner finds these and sends a system-wide Windows notification.

---

## How It Works

Prediction markets price outcomes as contracts between $0.00 and $1.00 that pay out $1.00 if correct. If Platform A sells "Team X wins" for $0.45 and Platform B sells "Team Y wins" (the opposite outcome) for $0.50, buying both costs $0.95. One contract always pays out $1.00, netting $0.05 profit regardless of who wins. This is a **risk-free arbitrage**.

The scanner automates this discovery:

1. **Fetch** all open sports events from Kalshi (29 series) and Polymarket (18 sources) in parallel
2. **Filter** to moneyline markets only (straight win/loss, no spreads, over/under, or props)
3. **Match** identical games across platforms using league bucketing, date tolerance (+/- 1 day), and fuzzy team name matching
4. **Price** each match using actual ask prices from the Polymarket CLOB order book (not mid-prices)
5. **Alert** when any match has a combined cost under $0.99 via sound + system tray notification

The scanner runs in a continuous loop, repeating the full process as fast as possible. Press `Ctrl+C` to stop.

---

## Project Structure

```
HFT/
  cross_platform_matcher.py   Main scanner -- continuous arbitrage detection
  kalshi_buy.py               Kalshi market data fetcher and order placement
  polymarket_buy.py           Polymarket market data fetcher and order placement
  .env                        API keys and credentials
  README.md                   This file
```

---

## Files

### cross_platform_matcher.py

The core scanner. Contains all logic for fetching, matching, and alerting.

**Key components:**

| Component | Description |
|---|---|
| `GameEvent` dataclass | Standardized representation of a game across platforms (platform, league, date, teams, prices, volume, URL) |
| `normalize_team()` | Resolves hundreds of team name aliases (abbreviations, city names, nicknames) to canonical forms |
| `fetch_kalshi_games()` | Parallel fetching across 29 Kalshi series with authenticated API calls (RSA-signed requests) |
| `fetch_polymarket_games()` | Parallel fetching across 18 Polymarket sources using both `tag_slug` and `series_id` approaches |
| `match_games()` | Matching engine using league bucketing, +/- 1 day date tolerance, and fuzzy string matching with configurable threshold |
| `update_poly_prices_from_clob()` | Replaces Polymarket mid-prices with actual CLOB ask prices (what you would really pay) |
| `print_matches()` | Filters to profitable matches (combined cost < $0.99) and prints detailed comparison tables |
| `send_notification()` | System-wide Windows notification via `winsound` beeps + `System.Windows.Forms.NotifyIcon` balloon tip |
| `main()` | Continuous loop with scan numbering, timestamps, and automatic retry on errors |

**Matching logic:**

The matcher uses a multi-strategy approach to handle naming differences between platforms:

1. **Exact match** on normalized team names after alias resolution
2. **Fuzzy matching** via `difflib.SequenceMatcher` with a substring bonus
3. **Cross-team comparison** to handle cases where platforms list teams in different order (e.g., "Team A at Team B" vs "Team B vs Team A")
4. **Confidence threshold** of >67% required for a match to be displayed

**Supported leagues:**

NBA, NFL, NHL, MLB, WNBA, NCAA Men's/Women's Basketball, NCAA Football, NCAA Hockey, NCAA Lacrosse, UFC, Boxing, Unrivaled, Euroleague, Eurocup, KBL, CBA, J.B. League, NBL, AHL, KHL, SHL, ATP, WTA, League of Legends, CS2, Valorant, Dota 2

### kalshi_buy.py

Standalone script for fetching and displaying Kalshi sports market data. Handles:

- RSA private key authentication (`KALSHI-ACCESS-KEY`, `KALSHI-ACCESS-SIGNATURE`, `KALSHI-ACCESS-TIMESTAMP` headers)
- Paginated event fetching with `with_nested_markets=true` for efficient single-call retrieval
- Parallel fetching across all sports series using `ThreadPoolExecutor`
- Displays YES ask prices grouped by event

### polymarket_buy.py

Standalone script for fetching and displaying Polymarket sports market data. Handles:

- Gamma API queries by series ID
- Moneyline market filtering (excludes spreads, over/under, props, futures)
- Slug-based fallback fetching for additional market discovery
- Displays outcome prices grouped by event

---

## Setup

### Prerequisites

- Python 3.10+
- Windows (required for notification system -- uses `winsound` and `System.Windows.Forms`)

### Dependencies

```
pip install requests cryptography
```

No additional packages are needed. The notification system uses only Python builtins (`winsound`, `subprocess`) and Windows system libraries.

### Configuration

1. **Kalshi API Key**: Set `KALSHI_API_KEY` environment variable or edit the default in `cross_platform_matcher.py`

2. **Kalshi Private Key**: Set `KALSHI_PRIVATE_KEY_PATH` environment variable or edit the default path. The key file must be a PEM-encoded RSA private key.

3. **Polymarket Credentials** (for order placement only, not needed for scanning): Set in `.env`:
   ```
   POLY_PRIVATE_KEY=<your_private_key>
   POLY_API_KEY=<your_api_key>
   POLY_API_SECRET=<your_api_secret>
   POLY_API_PASSPHRASE=<your_api_passphrase>
   ```

---

## Usage

### Run the continuous scanner

```
python cross_platform_matcher.py
```

The scanner will:
- Run scan after scan in a loop, printing results each time
- Show a summary header with total matches, high-confidence matches, and profitable matches
- Print detailed info for any game where the combined ask cost is under $0.99
- Play an audible alert (system beeps) and show a Windows balloon notification when profitable matches are found
- If no profitable matches are found, immediately scan again
- If profitable matches are found, wait 10 seconds before the next scan
- Automatically retry after 5 seconds if an error occurs

Stop the scanner with `Ctrl+C`.

### Run standalone market viewers

```
python kalshi_buy.py         # View Kalshi sports markets
python polymarket_buy.py     # View Polymarket sports markets
```

---

## Output Example

```
##############################################################################################################
  SCAN #4  |  2026-02-14 21:57:34
##############################################################################################################

Fetching Kalshi markets (29 series)...
  KXNCAAMBGAME: 24 games
  KXUFCFIGHT: 9 games
  Total: 111 Kalshi moneyline games loaded.

Fetching Polymarket markets (18 sources: tag_slug + series_id)...
  tag:NBA: 24 new games
  tag:NCAAMB: 275 new games
  Total: 592 Polymarket moneyline games loaded.

Matching games across platforms...
  NCAAMB: 24 Kalshi x 275 Polymarket
  UFC: 9 Kalshi x 31 Polymarket
  Matched 30 games.

Fetching actual ask prices from Polymarket CLOB (60 tokens)...
  Updated 60/60 Polymarket prices with actual ask prices.

==============================================================================================================
  MONEYLINE BETS ON BOTH KALSHI & POLYMARKET
  Total matching games found:     30
  High-confidence (>67%):         28
  Profitable (cost < $0.99):      1
==============================================================================================================

  ---- NCAAMB ----

  1. Virginia at Ohio St.  (2026-02-14)
     Match confidence: 71%
                                         Team 1                Team 2
             Kalshi (ask)                 $0.71                 $0.32
         Polymarket (ask)                 $0.74                 $0.27
     Kalshi teams:     virginia / ohio st.
     Poly teams:       virginia cavaliers / ohio state buckeyes
     Kalshi volume:    4,726,244    | https://kalshi.com/markets/KXNCAAMBGAME-26FEB14UVAOSU
     Poly volume:      110,718    | https://polymarket.com/event/cbb-ohiost-vir-2026-02-14
       *** ARBITRAGE: combined $0.98 ***

==============================================================================================================

  >>> 1 profitable arbitrage opportunities found!
  >> Alert sound played!
  >> Balloon notification sent!

  Scan #4 completed in 62.8s
```

---

## Key Parameters

| Parameter | Value | Location | Description |
|---|---|---|---|
| `FUZZY_MATCH_THRESHOLD` | 0.50 | `cross_platform_matcher.py` | Minimum similarity score for two games to be considered a match |
| Confidence filter | >67% | `print_matches()` | Matches below this confidence are excluded from output |
| Profit threshold | <$0.99 | `print_matches()` | Combined ask cost must be below this to qualify as profitable |
| Date tolerance | +/- 1 day | `match_games()` | Accounts for timezone differences in game dates across platforms |
| CLOB workers | 10 | `update_poly_prices_from_clob()` | Max concurrent threads for fetching Polymarket order book prices |
| Kalshi workers | 5 | `fetch_kalshi_games()` | Max concurrent threads for fetching Kalshi series |
| Polymarket workers | 8 | `fetch_polymarket_games()` | Max concurrent threads for fetching Polymarket sources |

---

## Architecture

```
main() loop
  |
  |-- fetch_kalshi_games()           Parallel fetch across 29 series
  |     |-- fetch_kalshi_series()    Per-series paginated fetch with RSA auth
  |     |-- parse_kalshi_date()      Extract date from event ticker
  |     |-- parse_teams_from_title() Extract team names from event title
  |     |-- match_market_to_team()   Map sub-markets to teams + extract ask prices
  |
  |-- fetch_polymarket_games()       Parallel fetch across 18 sources
  |     |-- fetch_polymarket_by_tag()    Primary: query by tag_slug
  |     |-- fetch_polymarket_by_series() Fallback: query by series_id
  |     |-- _parse_polymarket_event()    Parse event, filter moneyline, extract prices
  |     |-- is_moneyline_market()        Filter out spreads, O/U, props, futures
  |
  |-- match_games()                  League-bucketed matching with fuzzy names
  |     |-- _score_team_pair()       Similarity scoring with substring boost
  |
  |-- update_poly_prices_from_clob() Replace mid-prices with actual ask prices
  |     |-- _fetch_clob_ask()        Per-token CLOB price lookup
  |
  |-- print_matches()                Filter to profitable, display results
  |     |-- _arb_cost()              Compute minimum combined cost
  |
  |-- send_notification()            System-wide alert on profitable match
        |-- winsound.Beep()          Audible alert
        |-- NotifyIcon.ShowBalloonTip()  Visual balloon notification
```
