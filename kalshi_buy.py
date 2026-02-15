"""
kalshi_buy.py - Kalshi Sports Market Data Fetcher

Fetches sports market data from the Kalshi API.
Shows Feb 2026 games with YES ask prices for both teams.
"""

import requests
import datetime
import base64
import os
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding

# Configuration
load_dotenv()

API_KEY_ID = os.getenv('KALSHI_API_KEY', '')
PRIVATE_KEY_PATH = os.getenv('KALSHI_PRIVATE_KEY_PATH', '')
BASE_URL = os.getenv('KALSHI_BASE_URL', 'https://api.elections.kalshi.com/')
if not API_KEY_ID or not PRIVATE_KEY_PATH:
    raise RuntimeError('Missing KALSHI_API_KEY or KALSHI_PRIVATE_KEY_PATH in .env')


def load_private_key(key_path=None):
    """Load the private key from file."""
    path = key_path or PRIVATE_KEY_PATH
    with open(path, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())


def create_signature(private_key, timestamp, method, path):
    """Create the request signature for Kalshi API authentication."""
    path_without_query = path.split('?')[0]
    message = f"{timestamp}{method}{path_without_query}".encode('utf-8')
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')


def api_get(path, private_key=None, api_key_id=None):
    """Make an authenticated GET request to the Kalshi API with retry logic."""
    if private_key is None:
        private_key = load_private_key()
    if api_key_id is None:
        api_key_id = API_KEY_ID
    
    timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
    signature = create_signature(private_key, timestamp, "GET", path)
    
    headers = {
        'KALSHI-ACCESS-KEY': api_key_id,
        'KALSHI-ACCESS-SIGNATURE': signature,
        'KALSHI-ACCESS-TIMESTAMP': timestamp
    }
    
    # Retry logic for SSL errors
    for attempt in range(3):
        try:
            resp = requests.get(BASE_URL + path, headers=headers, timeout=30)
            return resp
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError):
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
            else:
                return None
    return None


# All sports game series from Kalshi
SPORTS_SERIES = [
    # Major US Sports
    'KXNBAGAME', 'KXNFLGAME', 'KXNHLGAME', 'KXMLBGAME', 'KXWNBAGAME',
    # College Sports - Basketball
    'KXNCAAMBGAME', 'KXNCAAWBGAME', 'KXNCAABGAME',
    # College Sports - Other
    'KXNCAAFGAME', 'KXNCAAHOCKEYGAME', 'KXNCAALAXGAME',
    # Fighting
    'KXUFCFIGHT', 'KXBOXINGFIGHT',
    # International Basketball
    'KXUNRIVALEDGAME', 'KXEUROLEAGUEGAME', 'KXEUROCUPGAME',
    'KXKBLGAME', 'KXCBAGAME', 'KXJBLEAGUEGAME', 'KXNBLGAME',
    # Hockey
    'KXAHLGAME', 'KXKHLGAME', 'KXSHLGAME',
    # Tennis
    'KXATPGAME', 'KXWTAGAME',
    # Esports
    'KXLOLGAME', 'KXCS2GAME', 'KXVALORANTGAME', 'KXDOTA2GAME',
]


def get_sports_markets(series_list=None, limit=200):
    """
    Fetch sports markets from Kalshi using with_nested_markets=true.
    
    This fetches events WITH markets in a single API call per series,
    eliminating the need for separate market API calls.
    
    Returns list of market dicts with ticker, title, yes_ask, volume, etc.
    """
    if series_list is None:
        series_list = SPORTS_SERIES
    
    def fetch_series(series):
        """Fetch events + markets for a series in ONE API call."""
        path = f"/trade-api/v2/events?series_ticker={series}&limit={limit}&status=open&with_nested_markets=true"
        resp = api_get(path)
        if resp is None or resp.status_code != 200:
            return []
        
        events = resp.json().get('events', [])
        results = []
        
        for event in events:
            event_ticker = event.get('event_ticker', '')
            
            # Filter for Feb 2026 only
            if '26FEB' not in event_ticker:
                continue
            
            for m in event.get('markets', []):
                ticker = m.get('ticker', '')
                
                # Get yes_ask price
                yes_ask = None
                yes_ask_str = m.get('yes_ask_dollars')
                if yes_ask_str:
                    try:
                        yes_ask = float(yes_ask_str)
                        if yes_ask >= 1.0:
                            yes_ask = None
                    except (ValueError, TypeError):
                        pass
                
                # Fall back to last_price
                if yes_ask is None:
                    lp_str = m.get('last_price_dollars')
                    if lp_str:
                        try:
                            lp = float(lp_str)
                            if 0 < lp < 1:
                                yes_ask = lp
                        except (ValueError, TypeError):
                            pass
                
                results.append({
                    'ticker': ticker,
                    'title': m.get('title', ''),
                    'event_ticker': event_ticker,
                    'yes_ask': yes_ask,
                    'volume': m.get('volume', 0),
                    'series': series
                })
        
        return results
    
    all_results = []
    
    # Fetch all series in parallel
    print(f"Fetching {len(series_list)} sports series with nested markets...")
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_series, s): s for s in series_list}
        for future in as_completed(futures):
            try:
                results = future.result()
                all_results.extend(results)
            except Exception:
                pass
    
    print(f"Found {len(all_results)} Feb 2026 sports markets")
    return all_results


def main():
    """Print single-game sports markets grouped by event."""
    print("=" * 60)
    print("KALSHI SINGLE-GAME SPORTS MARKETS")
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
    active = [m for m in markets if int(m.get('volume', 0)) > 0]
    
    # Group by event (e.g., KXNBAGAME-26FEB01CHIMIA has CHI and MIA)
    events = {}
    for m in active:
        ticker = m.get('ticker', '')
        parts = ticker.rsplit('-', 1)
        event_base = parts[0] if len(parts) == 2 else ticker
        team_code = parts[1] if len(parts) == 2 else ''
        
        if event_base not in events:
            events[event_base] = {
                'title': m.get('title', ''),
                'series': m.get('series', ''),
                'teams': {},
                'total_volume': 0
            }
        
        events[event_base]['teams'][team_code] = m.get('yes_ask')
        events[event_base]['total_volume'] += int(m.get('volume', 0))
    
    # Sort by total volume ascending
    sorted_events = sorted(events.items(), key=lambda x: x[1]['total_volume'])
    
    print(f"\nTotal markets: {len(markets)}")
    print(f"Active markets (volume > 0): {len(active)}")
    print(f"Feb 2026 events: {len(events)}")
    print("-" * 80)
    
    for i, (event_base, data) in enumerate(sorted_events, 1):
        title = data['title'][:55]
        series = data['series']
        teams = data['teams']
        total_vol = data['total_volume']
        
        # Build team prices string
        team_prices = []
        for team, ask in sorted(teams.items()):
            if ask is not None:
                team_prices.append(f"{team}: ${ask:.2f}")
            else:
                team_prices.append(f"{team}: N/A")
        
        prices_str = " | ".join(team_prices)
        
        # Calculate combined cost
        asks = [ask for ask in teams.values() if ask is not None]
        combined_str = ""
        if len(asks) >= 2:
            combined = sum(asks)
            combined_str = f"Combined: ${combined:.2f}"
            if combined < 1.0:
                combined_str += " *** ARBITRAGE ***"
        
        print(f"{i}. [{series}] {title}")
        print(f"   {prices_str}")
        if combined_str:
            print(f"   {combined_str} | Volume: {total_vol:,}")
        else:
            print(f"   Volume: {total_vol:,}")
        print()


if __name__ == '__main__':
    main()