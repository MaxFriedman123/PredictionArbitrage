"""
polymarket_buy.py - Polymarket Sports Market Data Fetcher

Fetches sports market data from the Polymarket Gamma API.
Shows Feb 2026 games with YES prices for both teams.
No authentication required for read-only operations.
"""

import requests
import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# API Endpoints
GAMMA_ENDPOINT = "https://gamma-api.polymarket.com"


# Patterns that indicate NON-game markets (spreads, O/U, props, etc.)
EXCLUDE_PATTERNS = [
    'o/u ', 'over/under', 'spread:', 'spread ', 
    'total:', 'team total', '1h ', '1h:', 'first half',
    'anytime touchdown', 'rushing yards', 'receiving yards', 'passing yards',
    'will trump', 'will biden', 'will elon', 'will musk',
    'moneyline', 'mvp', 'champion', 'championship', 'playoffs',
    'super bowl', 'finals', 'world series', 'stanley cup',
    '+', '-',  # Spread indicators like (-3.5) or (+3.5)
]


def is_straight_game_matchup(question, outcomes):
    """
    Check if this is a straight Team A vs Team B win/loss game.
    Returns True only for simple "who wins" matchups.
    """
    q_lower = question.lower()
    
    # Exclude non-game markets (spreads, O/U, props, political, etc.)
    for pattern in EXCLUDE_PATTERNS:
        if pattern in q_lower:
            return False
    
    # Check if outcomes look like team names (not Over/Under, Yes/No for props)
    outcome_lower = [o.lower() for o in outcomes]
    
    # Exclude O/U markets
    if 'over' in outcome_lower or 'under' in outcome_lower:
        return False
    
    # Exclude Yes/No prop markets (unless it's a fight)
    if 'yes' in outcome_lower or 'no' in outcome_lower:
        return False
    
    # Must have "vs" or "vs." or "at" or teams in question for game matchups
    game_indicators = [' vs ', ' vs. ', ' at ', 'winner']
    if not any(ind in q_lower for ind in game_indicators):
        return False
    
    return True


def is_feb_2026_game(event):
    """
    Check if event is a Feb 2026 game that hasn't happened yet.
    """
    # Get end date from event
    end_date_str = event.get('endDate') or event.get('end_date_iso')
    
    if end_date_str:
        try:
            # Parse ISO date (e.g., "2026-02-15T00:00:00Z")
            if 'T' in end_date_str:
                end_date = datetime.datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
            else:
                end_date = datetime.datetime.fromisoformat(end_date_str)
            
            # Check if it's Feb 2026
            if end_date.year == 2026 and end_date.month == 2:
                # Check if it hasn't happened yet
                now = datetime.datetime.now(datetime.timezone.utc)
                if end_date > now:
                    return True
        except (ValueError, TypeError):
            pass
    
    # Fallback: check slug for date patterns (e.g., nba-bos-mia-2026-02-15)
    slug = event.get('slug', '').lower()
    if '2026-02' in slug or '-26-02-' in slug:
        return True
    
    return False


def fetch_sports_events(series_name, series_id):
    """Fetch events for a specific sports series."""
    events = []
    offset = 0
    page_size = 100
    
    while True:
        params = {
            'series_id': series_id,
            'active': 'true',
            'closed': 'false',
            'limit': page_size,
            'offset': offset
        }
        
        try:
            resp = requests.get(f"{GAMMA_ENDPOINT}/events", params=params, timeout=30)
            if resp.status_code != 200:
                break
            
            data = resp.json()
            if not data:
                break
            
            for event in data:
                event['series_name'] = series_name
            
            events.extend(data)
            offset += len(data)
            
            if len(data) < page_size:
                break
                
        except Exception:
            break
    
    return events


def get_sports_markets():
    """
    Fetch sports game markets from Polymarket using series IDs.
    
    Fetches actual game matchups (Team A vs Team B) for:
    - Feb 2026 games only
    - Two-team win/loss outcomes (no spreads, O/U, props)
    - Games that haven't happened yet
    
    Returns list of markets with outcomes and prices.
    """
    all_events = []
    
    print(f"Fetching {len(SPORTS_SERIES)} sports series from Polymarket...")
    
    # Fetch all series in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(fetch_sports_events, name, sid): name 
            for name, sid in SPORTS_SERIES.items()
        }
        for future in as_completed(futures):
            try:
                events = future.result()
                all_events.extend(events)
            except Exception:
                pass
    
    # Filter and process events
    sports_markets = []
    
    for event in all_events:
        # Filter for Feb 2026 games only
        if not is_feb_2026_game(event):
            continue
        
        # Get markets from event
        markets = event.get('markets', [])
        
        for m in markets:
            # Parse outcomes and prices
            outcomes_raw = m.get('outcomes', '[]')
            prices_raw = m.get('outcomePrices', '[]')
            
            try:
                outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
                prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
            except (json.JSONDecodeError, TypeError):
                continue
            
            # Only include markets with exactly 2 outcomes (two teams, win/loss)
            if not outcomes or not prices or len(outcomes) != 2:
                continue
            
            question = m.get('question', '') or event.get('title', '')
            
            # Only include straight game matchups (no spreads, O/U, props)
            if not is_straight_game_matchup(question, outcomes):
                continue
            
            # Build teams dict with prices
            teams = {}
            total_volume = int(float(m.get('volume', 0) or 0))
            
            for i, outcome in enumerate(outcomes):
                if i < len(prices):
                    try:
                        price = float(prices[i])
                        if 0 < price < 1:
                            teams[outcome] = price
                    except (ValueError, TypeError):
                        pass
            
            # Only include if we have prices for exactly 2 teams
            if len(teams) == 2:
                sports_markets.append({
                    'question': question,
                    'slug': m.get('slug', '') or event.get('slug', ''),
                    'teams': teams,
                    'volume': total_volume,
                    'outcomes': outcomes,
                    'series': event.get('series_name', 'SPORTS'),
                    'platform': 'polymarket'
                })
    
    # If no events found via series, fall back to slug-based filtering
    if not sports_markets:
        print("No events from series API, falling back to market search...")
        sports_markets = get_sports_markets_fallback()
    
    print(f"Found {len(sports_markets)} Feb 2026 sports markets")
    return sports_markets


# Sports series IDs for Polymarket (Team vs Team game markets)
SPORTS_SERIES = {
    'NBA': '10345',
    'NFL': '10187',
    'NHL': '10346',
    'MLB': '10347',
    'UFC': '10348',
    'NCAAMB': '10349',
    'NCAAWB': '10350',
    'NCAAF': '10351',
    'BOXING': '10352',
    'TENNIS': '10353',
}


def get_sports_markets_fallback():
    """
    Fallback: Fetch markets and filter by slug pattern for game matchups.
    Looks for slugs like: nba-bos-mia-2026-02-15, nfl-buf-kc-2026-02-01
    """
    all_markets = []
    offset = 0
    page_size = 100
    
    # Game slug patterns: sport-team1-team2-date
    game_patterns = [
        'nba-', 'nfl-', 'nhl-', 'mlb-', 'ufc-', 'ncaa-', 'boxing-', 'tennis-'
    ]
    
    while len(all_markets) < 2000:
        params = {
            'active': 'true',
            'closed': 'false',
            'limit': page_size,
            'offset': offset
        }
        
        try:
            resp = requests.get(f"{GAMMA_ENDPOINT}/markets", params=params, timeout=30)
            if resp.status_code != 200:
                break
            
            markets = resp.json()
            if not markets:
                break
            
            all_markets.extend(markets)
            offset += len(markets)
            
            if len(markets) < page_size:
                break
                
        except Exception:
            break
    
    sports_markets = []
    
    for m in all_markets:
        slug = m.get('slug', '').lower()
        question = m.get('question', '')
        
        # Must start with a sports prefix
        if not any(slug.startswith(p) for p in game_patterns):
            continue
        
        # Must have date pattern in slug (indicates actual game, not futures)
        if '2026-02' not in slug and '-26-02-' not in slug:
            continue
        
        # Parse outcomes and prices
        outcomes_raw = m.get('outcomes', '[]')
        prices_raw = m.get('outcomePrices', '[]')
        
        try:
            outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw
            prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw
        except (json.JSONDecodeError, TypeError):
            continue
        
        # Only include markets with exactly 2 outcomes
        if not outcomes or not prices or len(outcomes) != 2:
            continue
        
        # Only include straight game matchups
        if not is_straight_game_matchup(question, outcomes):
            continue
        
        # Build teams dict
        teams = {}
        total_volume = int(float(m.get('volume', 0) or 0))
        
        for i, outcome in enumerate(outcomes):
            if i < len(prices):
                try:
                    price = float(prices[i])
                    if 0 < price < 1:
                        teams[outcome] = price
                except (ValueError, TypeError):
                    pass
        
        if len(teams) == 2:
            # Extract series from slug
            series = slug.split('-')[0].upper()
            
            sports_markets.append({
                'question': question,
                'slug': slug,
                'teams': teams,
                'volume': total_volume,
                'outcomes': outcomes,
                'series': series,
                'platform': 'polymarket'
            })
    
    return sports_markets


def main():
    """Print single-game sports markets for arbitrage analysis."""
    print("=" * 60)
    print("POLYMARKET SINGLE-GAME SPORTS MARKETS")
    print("=" * 60)
    
    try:
        markets = get_sports_markets()
    except Exception as e:
        print(f"Error: {e}")
        return
    
    if not markets:
        print("No sports markets found.")
        return
    
    # Filter for markets with volume > 0
    active = [m for m in markets if m.get('volume', 0) > 0]
    
    # Sort by volume ascending (like kalshi)
    active.sort(key=lambda x: x.get('volume', 0))
    
    print(f"\nTotal markets: {len(markets)}")
    print(f"Active markets (volume > 0): {len(active)}")
    print(f"Feb 2026 events: {len(active)}")
    print("-" * 80)
    
    for i, m in enumerate(active, 1):
        question = m.get('question', '')[:55]
        series = m.get('series', 'SPORTS')
        teams = m.get('teams', {})
        volume = m.get('volume', 0)
        
        # Build team prices string
        team_prices = []
        for team, price in sorted(teams.items()):
            team_prices.append(f"{team}: ${price:.2f}")
        
        prices_str = " | ".join(team_prices)
        
        # Calculate combined cost
        asks = list(teams.values())
        combined_str = ""
        if len(asks) >= 2:
            combined = sum(asks)
            combined_str = f"Combined: ${combined:.2f}"
            if combined < 1.0:
                combined_str += " *** ARBITRAGE ***"
        
        print(f"{i}. [{series}] {question}")
        print(f"   {prices_str}")
        if combined_str:
            print(f"   {combined_str} | Volume: {volume:,}")
        else:
            print(f"   Volume: {volume:,}")
        print()


if __name__ == '__main__':
    main()
