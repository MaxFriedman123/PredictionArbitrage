"""
cross_platform_matcher.py - Cross-Platform Sports Moneyline Matcher

Finds all sports moneyline bets available on BOTH Kalshi and Polymarket.
Prints detailed comparison info for each matched game.

Steps:
1. Fetch all open sports events from Kalshi (27 series)
2. Fetch all open sports events from Polymarket (10 series)
3. Filter to moneyline (straight win/loss) markets only
4. Match identical games across platforms using league+date bucketing + fuzzy names
5. Print matched bets with prices, volumes, URLs, and arbitrage detection
"""

import requests
import json
import datetime
import re
import base64
import os
import difflib
import time
import subprocess
import sys
from dotenv import load_dotenv
import winsound
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding

# ============================================================================
# CONFIGURATION
# ============================================================================

load_dotenv()

API_KEY_ID = os.getenv('KALSHI_API_KEY', '')
PRIVATE_KEY_PATH = os.getenv('KALSHI_PRIVATE_KEY_PATH', '')
if not API_KEY_ID or not PRIVATE_KEY_PATH:
    raise RuntimeError('Missing KALSHI_API_KEY or KALSHI_PRIVATE_KEY_PATH in .env')
KALSHI_BASE_URL = 'https://api.elections.kalshi.com'
GAMMA_ENDPOINT = "https://gamma-api.polymarket.com"
CLOB_ENDPOINT = "https://clob.polymarket.com"

FUZZY_MATCH_THRESHOLD = 0.50

# ============================================================================
# DATA STRUCTURES
# ============================================================================

@dataclass
class GameEvent:
    platform: str
    league: str
    date: str          # YYYY-MM-DD
    team1: str         # normalized
    team2: str         # normalized
    price_team1: float
    price_team2: float
    title: str
    url: str
    volume: float
    raw_data: dict = field(default_factory=dict)

    @property
    def id(self):
        return f"{self.platform}:{self.league}:{self.date}:{self.team1}vs{self.team2}"

# ============================================================================
# NORMALIZATION
# ============================================================================

TEAM_ALIASES = {
    # College abbreviations
    'conn': 'connecticut', 'uconn': 'connecticut',
    'miss': 'mississippi', 'ole miss': 'mississippi',
    'nc state': 'north carolina state', 'ncst': 'north carolina state',
    'unc': 'north carolina',
    'uva': 'virginia', 'vtech': 'virginia tech',
    'lsu': 'louisiana state',
    'usc': 'southern california',
    'ucla': 'university of california los angeles',
    'smu': 'southern methodist', 'tcu': 'texas christian',
    'byu': 'brigham young', 'ucf': 'central florida',
    'fsu': 'florida state',
    'psu': 'penn state', 'penn st': 'penn state',
    'mich st': 'michigan state', 'msu': 'michigan state',
    'ohio st': 'ohio state', 'osu': 'ohio state',
    'wash st': 'washington state', 'wazu': 'washington state',
    'okla st': 'oklahoma state', 'okst': 'oklahoma state',
    'kansas st': 'kansas state', 'ksu': 'kansas state',
    'iowa st': 'iowa state', 'isu': 'iowa state',
    'oregon st': 'oregon state', 'orst': 'oregon state',
    'arizona st': 'arizona state', 'asu': 'arizona state',
    'st johns': "st. john's", 'st marys': "st. mary's",
    'miami fl': 'miami', 'miami (fl)': 'miami',
    'ri': 'rhode island', 'uri': 'rhode island',
    'mass': 'massachusetts', 'umass': 'massachusetts',
    'quin': 'quinnipiac', 'duq': 'duquesne',
    'niag': 'niagara', 'fair': 'fairfield',
    # NBA
    'atl': 'hawks', 'bos': 'celtics', 'bkn': 'nets', 'cha': 'hornets',
    'chi': 'bulls', 'cle': 'cavaliers', 'dal': 'mavericks', 'den': 'nuggets',
    'det': 'pistons', 'gsw': 'warriors', 'hou': 'rockets', 'ind': 'pacers',
    'lac': 'clippers', 'lal': 'lakers', 'mem': 'grizzlies', 'mia': 'heat',
    'mil': 'bucks', 'min': 'timberwolves', 'nop': 'pelicans', 'nyk': 'knicks',
    'okc': 'thunder', 'orl': 'magic', 'phi': '76ers', 'phx': 'suns',
    'por': 'trail blazers', 'sac': 'kings', 'sas': 'spurs', 'tor': 'raptors',
    'uta': 'jazz', 'was': 'wizards',
    # NHL
    'ana': 'ducks', 'ari': 'coyotes', 'buf': 'sabres',
    'cgy': 'flames', 'car': 'hurricanes', 'col': 'avalanche',
    'cbj': 'blue jackets', 'edm': 'oilers',
    'fla': 'panthers', 'lak': 'kings', 'mtl': 'canadiens',
    'nsh': 'predators', 'njd': 'devils', 'nyi': 'islanders', 'nyr': 'rangers',
    'ott': 'senators', 'pit': 'penguins', 'sjs': 'sharks',
    'sea': 'kraken', 'stl': 'blues', 'tbl': 'lightning', 'van': 'canucks',
    'vgk': 'golden knights', 'wpg': 'jets', 'wsh': 'capitals',
    # NFL
    'bal': 'ravens', 'cin': 'bengals', 'gb': 'packers',
    'jax': 'jaguars', 'kc': 'chiefs',
    'lar': 'rams', 'lv': 'raiders',
    'ne': 'patriots', 'no': 'saints', 'nyg': 'giants',
    'nyj': 'jets', 'sf': '49ers', 'tb': 'buccaneers', 'ten': 'titans',
    # UFC/Boxing
    'ko': 'knockout', 'tko': 'technical knockout',

    # ── City Name → Nickname (for Kalshi titles like "Dallas at Los Angeles L") ──
    # NBA
    'atlanta': 'hawks', 'boston': 'celtics', 'brooklyn': 'nets',
    'charlotte': 'hornets', 'chicago': 'bulls', 'cleveland': 'cavaliers',
    'dallas': 'mavericks', 'denver': 'nuggets', 'detroit': 'pistons',
    'golden state': 'warriors', 'houston': 'rockets', 'indiana': 'pacers',
    'los angeles c': 'clippers', 'los angeles l': 'lakers',
    'la clippers': 'clippers', 'la lakers': 'lakers',
    'memphis': 'grizzlies', 'miami': 'heat',
    'milwaukee': 'bucks', 'minnesota': 'timberwolves',
    'new orleans': 'pelicans', 'new york': 'knicks',
    'oklahoma city': 'thunder', 'orlando': 'magic',
    'philadelphia': '76ers', 'phoenix': 'suns',
    'portland': 'trail blazers', 'sacramento': 'kings',
    'san antonio': 'spurs', 'toronto': 'raptors',
    'utah': 'jazz', 'washington': 'wizards',
    # NHL
    'anaheim': 'ducks', 'arizona': 'coyotes', 'buffalo': 'sabres',
    'calgary': 'flames', 'carolina': 'hurricanes', 'colorado': 'avalanche',
    'columbus': 'blue jackets', 'edmonton': 'oilers',
    'florida': 'panthers', 'los angeles k': 'kings',
    'la kings': 'kings', 'montreal': 'canadiens',
    'nashville': 'predators', 'new jersey': 'devils',
    'ny islanders': 'islanders', 'ny rangers': 'rangers',
    'ottawa': 'senators', 'pittsburgh': 'penguins',
    'san jose': 'sharks', 'seattle': 'kraken',
    'st. louis': 'blues', 'st louis': 'blues',
    'tampa bay': 'lightning', 'vancouver': 'canucks',
    'vegas': 'golden knights', 'las vegas': 'golden knights',
    'winnipeg': 'jets',
    # NFL
    'baltimore': 'ravens', 'cincinnati': 'bengals',
    'green bay': 'packers', 'jacksonville': 'jaguars',
    'kansas city': 'chiefs', 'los angeles r': 'rams',
    'la rams': 'rams', 'la chargers': 'chargers',
    'las vegas raiders': 'raiders', 'los angeles ch': 'chargers',
    'new england': 'patriots', 'new york g': 'giants',
    'new york j': 'jets', 'ny giants': 'giants', 'ny jets': 'jets',
    'san francisco': '49ers', 'tennessee': 'titans',
    # MLB
    'los angeles a': 'angels', 'la angels': 'angels',
    'los angeles d': 'dodgers', 'la dodgers': 'dodgers',
    'ny mets': 'mets', 'ny yankees': 'yankees',
    'new york m': 'mets', 'new york y': 'yankees',
    'st. louis cardinals': 'cardinals',
    'texas': 'rangers', 'oakland': 'athletics',
    'tampa bay rays': 'rays',
}

LEAGUE_MAPPING = {
    # Kalshi series -> standard league name
    'KXNBAGAME': 'NBA', 'KXNHLGAME': 'NHL', 'KXNFLGAME': 'NFL',
    'KXMLBGAME': 'MLB', 'KXWNBAGAME': 'WNBA',
    'KXNCAAMBGAME': 'NCAAMB', 'KXNCAAWBGAME': 'NCAAWB',
    'KXNCAAFGAME': 'NCAAF', 'KXNCAABGAME': 'NCAAMB',
    'KXNCAAHOCKEYGAME': 'NCAAHOCKEY', 'KXNCAALAXGAME': 'NCAALAX',
    'KXUFCFIGHT': 'UFC', 'KXBOXINGFIGHT': 'BOXING',
    'KXUNRIVALEDGAME': 'UNRIVALED', 'KXEUROLEAGUEGAME': 'EUROLEAGUE',
    'KXEUROCUPGAME': 'EUROCUP', 'KXKBLGAME': 'KBL',
    'KXCBAGAME': 'CBA', 'KXJBLEAGUEGAME': 'JBLEAGUE',
    'KXNBLGAME': 'NBL',
    'KXAHLGAME': 'AHL', 'KXKHLGAME': 'KHL', 'KXSHLGAME': 'SHL',
    'KXATPGAME': 'ATP', 'KXWTAGAME': 'WTA',
    'KXLOLGAME': 'LOL', 'KXCS2GAME': 'CS2',
    'KXVALORANTGAME': 'VALORANT', 'KXDOTA2GAME': 'DOTA2',
    # Polymarket series -> standard league name
    'NBA': 'NBA', 'NHL': 'NHL', 'NFL': 'NFL', 'MLB': 'MLB',
    'UFC': 'UFC', 'BOXING': 'BOXING', 'TENNIS': 'TENNIS',
    'CBB': 'NCAAMB', 'NCAAMB': 'NCAAMB',
    'CWBB': 'NCAAWB', 'NCAAWB': 'NCAAWB',
    'CFB': 'NCAAF', 'NCAAF': 'NCAAF',
}

# All Kalshi sports series
KALSHI_SERIES = [
    'KXNBAGAME', 'KXNFLGAME', 'KXNHLGAME', 'KXMLBGAME', 'KXWNBAGAME',
    'KXNCAAMBGAME', 'KXNCAAWBGAME', 'KXNCAABGAME',
    'KXNCAAFGAME', 'KXNCAAHOCKEYGAME', 'KXNCAALAXGAME',
    'KXUFCFIGHT', 'KXBOXINGFIGHT',
    'KXUNRIVALEDGAME', 'KXEUROLEAGUEGAME', 'KXEUROCUPGAME',
    'KXKBLGAME', 'KXCBAGAME', 'KXJBLEAGUEGAME', 'KXNBLGAME',
    'KXAHLGAME', 'KXKHLGAME', 'KXSHLGAME',
    'KXATPGAME', 'KXWTAGAME',
    'KXLOLGAME', 'KXCS2GAME', 'KXVALORANTGAME', 'KXDOTA2GAME',
]

# All Polymarket sports tag_slugs (primary) + series_ids (fallback)
POLYMARKET_TAG_SLUGS = {
    'NBA': 'nba',
    'NHL': 'nhl',
    'NFL': 'nfl',
    'MLB': 'mlb',
    'UFC': 'ufc',
    'NCAAMB': 'ncaa-basketball',
    'BOXING': 'boxing',
    'TENNIS': 'tennis',
}

POLYMARKET_SERIES = {
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


def normalize_team(name: str) -> str:
    """Strip junk, lowercase, resolve aliases."""
    n = name.lower().strip()
    # Remove ranking numbers like (1), #25
    n = re.sub(r'\(\d+\)', '', n)
    n = re.sub(r'#\d+', '', n)
    n = n.strip()
    if n in TEAM_ALIASES:
        return TEAM_ALIASES[n]
    return n


def normalize_league(league_raw: str) -> str:
    """Map platform-specific league codes to standard name."""
    return LEAGUE_MAPPING.get(league_raw.upper().strip(), league_raw.upper().strip())


def normalize_date(date_obj_or_str) -> Optional[str]:
    """Return YYYY-MM-DD string."""
    if not date_obj_or_str:
        return None
    if isinstance(date_obj_or_str, str):
        try:
            if 'T' in date_obj_or_str:
                dt = datetime.datetime.fromisoformat(date_obj_or_str.replace('Z', '+00:00'))
                return dt.strftime('%Y-%m-%d')
            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_obj_or_str):
                return date_obj_or_str
        except Exception:
            pass
    return None


def calculate_similarity(name1: str, name2: str) -> float:
    """Return similarity 0.0-1.0 between two normalized team names."""
    if name1 == name2:
        return 1.0
    return difflib.SequenceMatcher(None, name1, name2).ratio()


# ============================================================================
# KALSHI FETCHING
# ============================================================================

def load_private_key():
    with open(PRIVATE_KEY_PATH, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None, backend=default_backend())


def create_signature(private_key, timestamp, method, path):
    path_without_query = path.split('?')[0]
    message = f"{timestamp}{method}{path_without_query}".encode('utf-8')
    signature = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode('utf-8')


# Cache the private key so we only load it once
_cached_private_key = None

def get_private_key():
    global _cached_private_key
    if _cached_private_key is None:
        _cached_private_key = load_private_key()
    return _cached_private_key


def kalshi_api_get(path):
    """Authenticated GET request to Kalshi with retry."""
    for attempt in range(3):
        try:
            private_key = get_private_key()
            timestamp = str(int(datetime.datetime.now().timestamp() * 1000))
            signature = create_signature(private_key, timestamp, "GET", path)
            headers = {
                'KALSHI-ACCESS-KEY': API_KEY_ID,
                'KALSHI-ACCESS-SIGNATURE': signature,
                'KALSHI-ACCESS-TIMESTAMP': timestamp
            }
            return requests.get(KALSHI_BASE_URL + path, headers=headers, timeout=15)
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError):
            if attempt < 2:
                time.sleep(0.5 * (attempt + 1))
            else:
                return None
        except Exception as e:
            print(f"  Kalshi API error: {e}")
            return None
    return None


def parse_kalshi_date(ticker: str) -> Optional[str]:
    """Extract YYYY-MM-DD from a Kalshi event ticker like KXNBAGAME-26FEB01..."""
    parts = ticker.split('-')
    if len(parts) < 2:
        return None
    date_part = parts[1][:7]  # e.g. 26FEB01
    if not re.match(r'\d{2}[A-Z]{3}\d{2}', date_part):
        return None
    mon_map = {
        'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
        'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12,
    }
    yy = "20" + date_part[:2]
    mm = mon_map.get(date_part[2:5], 0)
    dd = date_part[5:7]
    if mm == 0:
        return None
    return f"{yy}-{mm:02d}-{dd}"


def parse_teams_from_title(title: str) -> Optional[Tuple[str, str]]:
    """Extract two team names from 'Team A at Team B' or 'Team A vs Team B'."""
    for sep in [' at ', ' vs. ', ' vs ']:
        if sep in title:
            parts = title.split(sep)
            if len(parts) == 2:
                return parts[0].strip(), parts[1].strip()
    return None


def match_market_to_team(market, t1_norm, t2_norm):
    """Match a Kalshi market sub-ticker to one of the two teams. Returns ('t1', price), ('t2', price), or None."""
    # Extract price — ONLY use yes_ask (the actual price you'd pay to buy YES)
    price = None
    yes_ask = market.get('yes_ask_dollars')
    if yes_ask:
        try:
            price = float(yes_ask)
            if price >= 1.0:
                price = None
        except (ValueError, TypeError):
            pass
    # Fallback to yes_ask (integer cents) if yes_ask_dollars not present
    if price is None:
        yes_ask_cents = market.get('yes_ask')
        if yes_ask_cents:
            try:
                price = int(yes_ask_cents) / 100.0
                if price >= 1.0:
                    price = None
            except (ValueError, TypeError):
                pass
    if price is None:
        return None

    # Extract team code from ticker suffix
    m_ticker = market.get('ticker', '')
    code_raw = m_ticker.split('-')[-1].lower() if '-' in m_ticker else ''
    code_norm = normalize_team(code_raw) if code_raw else ''

    # Strategy 1: Exact match on normalized code
    if code_norm:
        if code_norm == t1_norm:
            return 't1', price
        if code_norm == t2_norm:
            return 't2', price

        # Strategy 2: Similarity + substring
        s1 = calculate_similarity(code_norm, t1_norm)
        s2 = calculate_similarity(code_norm, t2_norm)

        # Substring bonus
        if len(code_norm) >= 2:
            if code_norm in t1_norm:
                s1 = max(s1, 0.9)
            if code_norm in t2_norm:
                s2 = max(s2, 0.9)

        threshold = 0.4
        if s1 > threshold and s1 > s2 + 0.1:
            return 't1', price
        if s2 > threshold and s2 > s1 + 0.1:
            return 't2', price

    # Strategy 3: Title containment fallback
    m_title = market.get('title', '').lower()
    if ' vs ' not in m_title and ' at ' not in m_title:
        if t1_norm in m_title:
            return 't1', price
        if t2_norm in m_title:
            return 't2', price

    return None


def fetch_kalshi_series(series: str) -> List[GameEvent]:
    """Fetch all open events for a single Kalshi series."""
    games = []
    cursor = None
    league = normalize_league(series)

    while True:
        path = f"/trade-api/v2/events?series_ticker={series}&limit=200&status=open&with_nested_markets=true"
        if cursor:
            path += f"&cursor={cursor}"

        resp = kalshi_api_get(path)
        if not resp or resp.status_code != 200:
            break

        body = resp.json()
        events = body.get('events', [])
        cursor = body.get('cursor')

        if not events:
            break

        for event in events:
            ticker = event.get('event_ticker', '')
            game_date = parse_kalshi_date(ticker)
            if not game_date:
                continue

            title = event.get('title', '')
            teams = parse_teams_from_title(title)
            if not teams:
                continue

            t1_raw, t2_raw = teams
            t1_norm = normalize_team(t1_raw)
            t2_norm = normalize_team(t2_raw)

            markets = event.get('markets', [])
            volume = sum(m.get('volume', 0) for m in markets)

            t1_price = None
            t2_price = None

            for m in markets:
                result = match_market_to_team(m, t1_norm, t2_norm)
                if result is None:
                    continue
                side, price = result
                if side == 't1':
                    t1_price = price
                else:
                    t2_price = price

            if t1_price is not None and t2_price is not None:
                games.append(GameEvent(
                    platform='kalshi',
                    league=league,
                    date=game_date,
                    team1=t1_norm,
                    team2=t2_norm,
                    price_team1=t1_price,
                    price_team2=t2_price,
                    title=title,
                    url=f"https://kalshi.com/markets/{ticker}",
                    volume=volume,
                    raw_data=event,
                ))

        if not cursor:
            break

    return games


def fetch_kalshi_games() -> List[GameEvent]:
    """Fetch all open sports moneyline games from Kalshi in parallel."""
    print(f"Fetching Kalshi markets ({len(KALSHI_SERIES)} series)...")
    all_games = []

    # Pre-load the private key before spawning threads
    get_private_key()

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(fetch_kalshi_series, s): s for s in KALSHI_SERIES}
        for f in as_completed(futures):
            series = futures[f]
            try:
                result = f.result()
                if result:
                    all_games.extend(result)
                    print(f"  {series}: {len(result)} games")
            except Exception as e:
                print(f"  {series}: error - {e}")

    print(f"  Total: {len(all_games)} Kalshi moneyline games loaded.\n")
    return all_games


# ============================================================================
# POLYMARKET FETCHING
# ============================================================================

# Patterns that indicate NON-moneyline markets
POLY_EXCLUDE_PATTERNS = [
    'o/u ', 'over/under', 'spread:', 'spread ',
    'total:', 'team total', '1h ', '1h:', 'first half',
    'anytime touchdown', 'rushing yards', 'receiving yards', 'passing yards',
    'passing touchdowns', 'points over', 'points under',
    'rebounds over', 'assists over', 'steals over', 'blocks over',
    'mvp', 'champion', 'championship', 'playoffs',
    'super bowl', 'finals', 'world series', 'stanley cup',
    'regular season', 'division winner', 'conference winner',
]


def is_moneyline_market(question: str, outcomes: list) -> bool:
    """Check if this is a straight Team A vs Team B win/loss market."""
    q_lower = question.lower()

    for pattern in POLY_EXCLUDE_PATTERNS:
        if pattern in q_lower:
            return False

    outcome_lower = [o.lower() for o in outcomes]
    if 'over' in outcome_lower or 'under' in outcome_lower:
        return False
    if 'yes' in outcome_lower or 'no' in outcome_lower:
        return False

    # Accept if it looks like "Team A vs Team B" (any separator)
    game_indicators = [' vs ', ' vs. ', ' at ']
    if any(ind in q_lower for ind in game_indicators):
        return True

    # Also accept if outcomes are two different team-like names (not generic)
    # and the question doesn't match any exclude pattern above
    if len(outcomes) == 2 and outcomes[0] != outcomes[1]:
        skip_words = {'yes','no','over','under','draw'}
        if outcome_lower[0] not in skip_words and outcome_lower[1] not in skip_words:
            # Looks like two team names — likely moneyline
            return True

    return False


def _parse_game_date_eastern(end_date_str: str) -> Optional[str]:
    """Convert a UTC endDate to US Eastern date string (YYYY-MM-DD)."""
    if not end_date_str:
        return None
    try:
        dt = datetime.datetime.fromisoformat(end_date_str.replace('Z', '+00:00'))
        # Subtract 5 hours to approximate Eastern time
        eastern = dt - datetime.timedelta(hours=5)
        return eastern.strftime('%Y-%m-%d')
    except Exception:
        return normalize_date(end_date_str)


def _detect_league_from_slug(slug: str, tag_league: str) -> str:
    """Detect the league from a Polymarket event slug like 'nba-phi-nyk-2025-10-02'."""
    slug_lower = slug.lower()
    league_prefixes = [
        ('nba-', 'NBA'), ('nhl-', 'NHL'), ('nfl-', 'NFL'), ('mlb-', 'MLB'),
        ('ufc-', 'UFC'), ('wnba-', 'WNBA'), ('ncaab-', 'NCAAMB'),
        ('ncaaf-', 'NCAAF'), ('ncaaw-', 'NCAAWB'),
    ]
    for prefix, league in league_prefixes:
        if slug_lower.startswith(prefix):
            return league
    return normalize_league(tag_league)


def _fetch_clob_ask(token_id: str) -> Optional[float]:
    """Fetch the best ask price for a single Polymarket CLOB token."""
    try:
        r = requests.get(
            f"{CLOB_ENDPOINT}/price",
            params={"token_id": token_id, "side": "sell"},
            timeout=5,
        )
        if r.status_code == 200:
            data = r.json()
            price = float(data.get('price', 0))
            if 0 < price < 1:
                return price
    except Exception:
        pass
    return None


def _parse_polymarket_event(ev: dict, league_hint: str, today_str: str) -> Optional[GameEvent]:
    """Parse a single Polymarket event into a GameEvent if it's a valid moneyline.
    Uses outcomePrices (mid-prices) initially — actual ask prices are fetched later via CLOB."""
    end_date = ev.get('endDate', '')
    game_date = _parse_game_date_eastern(end_date)
    if not game_date or game_date < today_str:
        return None

    slug = ev.get('slug', '')
    league = _detect_league_from_slug(slug, league_hint)

    for m in ev.get('markets', []):
        outcomes_raw = m.get('outcomes', '[]')
        outcomes = json.loads(outcomes_raw) if isinstance(outcomes_raw, str) else outcomes_raw

        if len(outcomes) != 2:
            continue

        question = m.get('question', '') or ev.get('title', '')
        if not is_moneyline_market(question, outcomes):
            continue

        prices_raw = m.get('outcomePrices', '[]')
        prices = json.loads(prices_raw) if isinstance(prices_raw, str) else prices_raw

        p1, p2 = None, None
        if prices:
            try:
                p1 = float(prices[0])
            except (ValueError, TypeError):
                pass
        if prices and len(prices) > 1:
            try:
                p2 = float(prices[1])
            except (ValueError, TypeError):
                pass

        # Store clobTokenIds for later CLOB ask price lookup
        clob_ids_raw = m.get('clobTokenIds', '[]')
        clob_ids = json.loads(clob_ids_raw) if isinstance(clob_ids_raw, str) else clob_ids_raw

        if p1 is not None and p2 is not None and 0 < p1 < 1 and 0 < p2 < 1:
            game = GameEvent(
                platform='polymarket',
                league=league,
                date=game_date,
                team1=normalize_team(outcomes[0]),
                team2=normalize_team(outcomes[1]),
                price_team1=p1,
                price_team2=p2,
                title=ev.get('title', ''),
                url=f"https://polymarket.com/event/{slug}",
                volume=float(m.get('volume') or 0),
                raw_data=ev,
            )
            # Attach CLOB token IDs for later price update
            game.raw_data['_clob_token_ids'] = clob_ids if len(clob_ids) == 2 else []
            return game

    return None


def fetch_polymarket_by_tag(league_name: str, tag_slug: str) -> List[GameEvent]:
    """Fetch all open events for a Polymarket tag_slug (e.g. 'nba', 'ncaa-basketball')."""
    games = []
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    offset = 0

    while True:
        try:
            params = {
                "tag_slug": tag_slug,
                "active": "true",
                "closed": "false",
                "limit": 100,
                "offset": offset,
            }
            r = requests.get(f"{GAMMA_ENDPOINT}/events", params=params, timeout=15)
            if r.status_code != 200:
                break

            chunk = r.json()
            if not chunk:
                break

            for ev in chunk:
                game = _parse_polymarket_event(ev, league_name, today_str)
                if game:
                    games.append(game)

            if len(chunk) < 100:
                break
            offset += 100

        except Exception:
            break

    return games


def fetch_polymarket_by_series(league_name: str, series_id: str) -> List[GameEvent]:
    """Fetch all open events for a Polymarket series_id (fallback)."""
    games = []
    today_str = datetime.datetime.now().strftime('%Y-%m-%d')
    offset = 0

    while True:
        try:
            params = {
                "series_id": series_id,
                "active": "true",
                "closed": "false",
                "limit": 100,
                "offset": offset,
            }
            r = requests.get(f"{GAMMA_ENDPOINT}/events", params=params, timeout=15)
            if r.status_code != 200:
                break

            chunk = r.json()
            if not chunk:
                break

            for ev in chunk:
                game = _parse_polymarket_event(ev, league_name, today_str)
                if game:
                    games.append(game)

            if len(chunk) < 100:
                break
            offset += 100

        except Exception:
            break

    return games


def fetch_polymarket_games() -> List[GameEvent]:
    """Fetch all open sports moneyline games from Polymarket using tag_slug + series_id."""
    total_sources = len(POLYMARKET_TAG_SLUGS) + len(POLYMARKET_SERIES)
    print(f"Fetching Polymarket markets ({total_sources} sources: tag_slug + series_id)...")
    all_games = []
    seen_slugs = set()  # deduplicate across tag_slug and series_id results

    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {}
        # Primary: tag_slug approach (much more comprehensive)
        for name, tag in POLYMARKET_TAG_SLUGS.items():
            futures[ex.submit(fetch_polymarket_by_tag, name, tag)] = f"tag:{name}"
        # Fallback: series_id approach
        for name, sid in POLYMARKET_SERIES.items():
            futures[ex.submit(fetch_polymarket_by_series, name, sid)] = f"series:{name}"

        for f in as_completed(futures):
            source = futures[f]
            try:
                result = f.result()
                new_count = 0
                for g in result:
                    # Deduplicate by event slug (from URL)
                    slug = g.url.split('/event/')[-1] if '/event/' in g.url else g.id
                    if slug not in seen_slugs:
                        seen_slugs.add(slug)
                        all_games.append(g)
                        new_count += 1
                if new_count > 0:
                    print(f"  {source}: {new_count} new games")
            except Exception as e:
                print(f"  {source}: error - {e}")

    print(f"  Total: {len(all_games)} Polymarket moneyline games loaded.\n")
    return all_games


# ============================================================================
# MATCHING ENGINE
# ============================================================================

def _score_team_pair(name_a: str, name_b: str) -> float:
    """Score similarity between two team names with substring boost."""
    if name_a == name_b:
        return 1.0
    score = calculate_similarity(name_a, name_b)
    if len(name_a) >= 3 and name_a in name_b:
        score = max(score, 0.9)
    if len(name_b) >= 3 and name_b in name_a:
        score = max(score, 0.9)
    return score


def match_games(kalshi_games: List[GameEvent], poly_games: List[GameEvent]) -> list:
    """Match identical games across Kalshi and Polymarket using league bucketing with ±1 day tolerance."""
    # Build league-based buckets (date is checked with tolerance)
    league_buckets: Dict[str, Dict[str, List[GameEvent]]] = {}

    for g in kalshi_games:
        league_buckets.setdefault(g.league, {'kalshi': [], 'poly': []})['kalshi'].append(g)

    for g in poly_games:
        league_buckets.setdefault(g.league, {'kalshi': [], 'poly': []})['poly'].append(g)

    matches = []
    used_poly = set()  # prevent duplicate poly matches

    print("Matching games across platforms...")
    for league, groups in sorted(league_buckets.items()):
        k_list = groups['kalshi']
        p_list = groups['poly']

        if not k_list or not p_list:
            continue

        print(f"  {league}: {len(k_list)} Kalshi × {len(p_list)} Polymarket")

        for k_game in k_list:
            best_match = None
            best_score = 0.0
            swap_teams = False

            # Parse Kalshi date for ±1 day comparison
            try:
                k_date = datetime.datetime.strptime(k_game.date, '%Y-%m-%d')
            except (ValueError, TypeError):
                continue

            for p_game in p_list:
                if id(p_game) in used_poly:
                    continue

                # ±1 day tolerance on dates
                try:
                    p_date = datetime.datetime.strptime(p_game.date, '%Y-%m-%d')
                except (ValueError, TypeError):
                    continue
                if abs((k_date - p_date).days) > 1:
                    continue

                # Straight comparison: K.t1↔P.t1 and K.t2↔P.t2
                s1_str = _score_team_pair(k_game.team1, p_game.team1)
                s2_str = _score_team_pair(k_game.team2, p_game.team2)
                avg_straight = (s1_str + s2_str) / 2

                # Cross comparison: K.t1↔P.t2 and K.t2↔P.t1
                s1_cross = _score_team_pair(k_game.team1, p_game.team2)
                s2_cross = _score_team_pair(k_game.team2, p_game.team1)
                avg_cross = (s1_cross + s2_cross) / 2

                current_score = max(avg_straight, avg_cross)

                if current_score > best_score and current_score > FUZZY_MATCH_THRESHOLD:
                    best_score = current_score
                    best_match = p_game
                    swap_teams = (avg_cross > avg_straight)

            if best_match:
                used_poly.add(id(best_match))
                matches.append({
                    'kalshi': k_game,
                    'poly': best_match,
                    'score': best_score,
                    'swap': swap_teams,
                })

    print(f"  Matched {len(matches)} games.\n")
    return matches


# ============================================================================
# OUTPUT
# ============================================================================

def fmt(price) -> str:
    """Format price as $0.XX or N/A."""
    if price is None:
        return "  N/A "
    return f"${price:.2f}"


def _arb_cost(m):
    """Compute the minimum combined ask cost for a matched game."""
    k = m['kalshi']
    swap = m['swap']
    p_t1_price = m['poly'].price_team2 if swap else m['poly'].price_team1
    p_t2_price = m['poly'].price_team1 if swap else m['poly'].price_team2
    cost1 = (k.price_team1 or 1.0) + (p_t2_price or 1.0)
    cost2 = (k.price_team2 or 1.0) + (p_t1_price or 1.0)
    return min(cost1, cost2)


def print_matches(matches):
    """Print detailed info for profitable matched moneyline bets (min_cost < $0.99 only).
    Returns list of profitable matches for notification purposes."""
    total_matches = len(matches)

    # Filter to only matches with confidence strictly > 67%
    matches = [m for m in matches if m['score'] > 0.67]

    # Only keep profitable matches (min_cost < $0.99)
    profitable = [m for m in matches if _arb_cost(m) < 0.99]

    separator = "=" * 110

    print(separator)
    print(f"  MONEYLINE BETS ON BOTH KALSHI & POLYMARKET")
    print(f"  Total matching games found:     {total_matches}")
    print(f"  High-confidence (>67%):         {len(matches)}")
    print(f"  Profitable (cost < $0.99):      {len(profitable)}")
    print(separator)

    if not profitable:
        print("\n  No profitable arbitrage opportunities found (all combined ask costs >= $0.99).")
        print(f"\n{separator}")
        return profitable

    # Sort profitable by league then date
    profitable.sort(key=lambda m: (m['kalshi'].league, m['kalshi'].date, m['kalshi'].team1))

    current_league = None

    for i, m in enumerate(profitable, 1):
        k = m['kalshi']
        p = m['poly']
        swap = m['swap']

        # Align teams: K.team1 should correspond to P.team1
        if not swap:
            p_t1_name = p.team1
            p_t2_name = p.team2
            p_t1_price = p.price_team1
            p_t2_price = p.price_team2
        else:
            p_t1_name = p.team2
            p_t2_name = p.team1
            p_t1_price = p.price_team2
            p_t2_price = p.price_team1

        # League header
        if k.league != current_league:
            current_league = k.league
            print(f"\n  {'----'} {current_league} {'----'}")

        min_cost = _arb_cost(m)

        # Print game
        print(f"\n  {i}. {k.title}  ({k.date})")
        print(f"     Match confidence: {m['score']:.0%}")
        print(f"     {'':>20}  {'Team 1':>20}  {'Team 2':>20}")
        print(f"     {'Kalshi (ask)':>20}  {fmt(k.price_team1):>20}  {fmt(k.price_team2):>20}")
        print(f"     {'Polymarket (ask)':>20}  {fmt(p_t1_price):>20}  {fmt(p_t2_price):>20}")
        print(f"     Kalshi teams:     {k.team1} / {k.team2}")
        print(f"     Poly teams:       {p_t1_name} / {p_t2_name}")
        print(f"     Kalshi volume:    {int(k.volume):,}    | {k.url}")
        print(f"     Poly volume:      {int(p.volume):,}    | {p.url}")
        print(f"       *** ARBITRAGE: combined ${min_cost:.2f} ***")

    print(f"\n{separator}")
    print(f"\n  >>> {len(profitable)} profitable arbitrage opportunities found!")
    return profitable


def send_notification(profitable_matches):
    """Send system-wide Windows notification for profitable arbitrage matches.
    Uses multiple methods to ensure the user is alerted even when in other apps:
    1. System beep sounds (always audible)
    2. Windows balloon tip notification (visible in system tray)
    """
    count = len(profitable_matches)

    # Build notification text
    lines = []
    for m in profitable_matches[:3]:
        k = m['kalshi']
        cost = _arb_cost(m)
        profit_cents = int((1.0 - cost) * 100)
        lines.append(f"{k.title} - ${cost:.2f} cost (+{profit_cents}c profit)")
    body = "\r\n".join(lines)
    if count > 3:
        body += f"\r\n...and {count - 3} more"

    title = f"ARBITRAGE ALERT: {count} profitable game{'s' if count != 1 else ''} found!"

    # --- Method 1: Audible alert (always works, system-wide) ---
    try:
        winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
        # Play a distinctive beep pattern so it's unmistakable
        for freq in [800, 1000, 1200]:
            winsound.Beep(freq, 200)
        print("  >> Alert sound played!")
    except Exception as e:
        print(f"  >> Sound alert failed: {e}")

    # --- Method 2: System tray balloon notification (visible system-wide) ---
    ps_title = title.replace('"', '`"').replace("'", "''")
    ps_body = body.replace('"', '`"').replace("'", "''")

    ps_script = f"""Add-Type -AssemblyName System.Windows.Forms
$notify = New-Object System.Windows.Forms.NotifyIcon
$notify.Icon = [System.Drawing.SystemIcons]::Warning
$notify.Visible = $true
$notify.BalloonTipTitle = "{ps_title}"
$notify.BalloonTipText = "{ps_body}"
$notify.BalloonTipIcon = [System.Windows.Forms.ToolTipIcon]::Warning
$notify.ShowBalloonTip(10000)
Start-Sleep -Seconds 5
$notify.Dispose()
"""
    try:
        subprocess.Popen(
            ['powershell', '-NoProfile', '-Command', ps_script],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        print("  >> Balloon notification sent!")
    except Exception as e:
        print(f"  >> Balloon notification failed: {e}")


# ============================================================================
# MAIN
# ============================================================================

def update_poly_prices_from_clob(matches: list):
    """Update Polymarket prices in matched games with actual CLOB ask prices (what you'd really pay)."""
    # Collect all CLOB token lookups needed
    lookups = []  # (match_idx, token_id, team_num)
    for i, m in enumerate(matches):
        poly = m['poly']
        clob_ids = poly.raw_data.get('_clob_token_ids', [])
        if len(clob_ids) == 2:
            lookups.append((i, clob_ids[0], 1))
            lookups.append((i, clob_ids[1], 2))

    if not lookups:
        print("  No CLOB token IDs available — using mid-prices for Polymarket.")
        return

    print(f"Fetching actual ask prices from Polymarket CLOB ({len(lookups)} tokens)...")

    results = {}  # (match_idx, team_num) -> price
    with ThreadPoolExecutor(max_workers=10) as ex:
        future_map = {}
        for match_idx, token_id, team_num in lookups:
            fut = ex.submit(_fetch_clob_ask, token_id)
            future_map[fut] = (match_idx, team_num)
        for fut in as_completed(future_map):
            key = future_map[fut]
            try:
                price = fut.result()
                if price is not None:
                    results[key] = price
            except Exception:
                pass

    updated = 0
    for (match_idx, team_num), price in results.items():
        poly = matches[match_idx]['poly']
        if team_num == 1:
            poly.price_team1 = price
        else:
            poly.price_team2 = price
        updated += 1

    print(f"  Updated {updated}/{len(lookups)} Polymarket prices with actual ask prices.\n")


def main():
    iteration = 0
    print("Starting continuous arbitrage scanner (Ctrl+C to stop)...")
    print("Will send a Windows notification when profitable matches (cost < $0.99) are found.\n")

    while True:
        iteration += 1
        start = time.time()
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"\n{'#'*110}")
        print(f"  SCAN #{iteration}  |  {timestamp}")
        print(f"{'#'*110}\n")

        try:
            kalshi_games = fetch_kalshi_games()
            poly_games = fetch_polymarket_games()

            matches = match_games(kalshi_games, poly_games)

            # Update Polymarket prices with actual CLOB ask prices
            update_poly_prices_from_clob(matches)

            profitable = print_matches(matches)

            # Send notification if profitable matches found
            if profitable:
                send_notification(profitable)

            elapsed = time.time() - start
            print(f"\n  Scan #{iteration} completed in {elapsed:.1f}s")

            if not profitable:
                print(f"  No profitable opportunities this scan. Scanning again immediately...\n")
            else:
                print(f"  Waiting 10s before next scan...\n")
                time.sleep(10)

        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"\n  Error during scan #{iteration}: {e}")
            print(f"  Retrying in 5s...\n")
            time.sleep(5)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Scanner stopped by user. Goodbye!")
        sys.exit(0)
