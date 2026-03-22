#!/usr/bin/env python3
"""
Fetch Google Trends data for UK food bank searches.

This script collects regional, city-level, and national trend data for food bank
searches across the UK using direct HTTP requests to Google Trends internal API,
with exponential backoff retry logic and comprehensive error handling.
"""

import json
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys
import random

try:
    import requests
except ImportError:
    print("Error: requests library not installed. Run: pip install requests>=2.28.0", file=sys.stderr)
    sys.exit(1)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# City geocoding lookup dictionary
CITY_GEOCODES = {
    # England
    "Pudsey": {"nation": "England", "lat": 53.797, "lng": -1.662},
    "Desborough": {"nation": "England", "lat": 52.441, "lng": -0.818},
    "Dinnington": {"nation": "England", "lat": 53.368, "lng": -1.209},
    "Middleton": {"nation": "England", "lat": 53.555, "lng": -2.187},
    "Wick": {"nation": "England", "lat": 50.609, "lng": -2.995},
    "Westoning": {"nation": "England", "lat": 52.009, "lng": -0.486},
    "Dudley": {"nation": "England", "lat": 52.512, "lng": -2.087},
    "Hastings": {"nation": "England", "lat": 50.854, "lng": 0.573},
    "Staindrop": {"nation": "England", "lat": 54.583, "lng": -1.800},
    "Worcester": {"nation": "England", "lat": 52.192, "lng": -2.220},
    "Shrewsbury": {"nation": "England", "lat": 52.708, "lng": -2.754},
    "Bushey": {"nation": "England", "lat": 51.642, "lng": -0.360},
    "Oldham": {"nation": "England", "lat": 53.541, "lng": -2.117},
    "Aldershot": {"nation": "England", "lat": 51.249, "lng": -0.763},
    "Cleobury Mortimer": {"nation": "England", "lat": 52.378, "lng": -2.477},
    "Canterbury": {"nation": "England", "lat": 51.280, "lng": 1.080},
    "Wythenshawe": {"nation": "England", "lat": 53.389, "lng": -2.263},
    "Williton": {"nation": "England", "lat": 51.159, "lng": -3.344},
    "Luton": {"nation": "England", "lat": 51.879, "lng": -0.417},
    "London": {"nation": "England", "lat": 51.507, "lng": -0.128},
    "Manchester": {"nation": "England", "lat": 53.483, "lng": -2.244},
    "Birmingham": {"nation": "England", "lat": 52.486, "lng": -1.890},
    "Leeds": {"nation": "England", "lat": 53.801, "lng": -1.549},
    "Liverpool": {"nation": "England", "lat": 53.408, "lng": -2.992},
    "Sheffield": {"nation": "England", "lat": 53.383, "lng": -1.468},
    "Bristol": {"nation": "England", "lat": 51.455, "lng": -2.587},
    "Newcastle": {"nation": "England", "lat": 54.978, "lng": -1.618},
    "Nottingham": {"nation": "England", "lat": 52.954, "lng": -1.158},
    "Southampton": {"nation": "England", "lat": 50.905, "lng": -1.404},
    "Brighton": {"nation": "England", "lat": 50.823, "lng": -0.137},
    "Plymouth": {"nation": "England", "lat": 50.376, "lng": -4.143},
    "Stoke-on-Trent": {"nation": "England", "lat": 53.003, "lng": -2.180},
    "Coventry": {"nation": "England", "lat": 52.407, "lng": -1.510},
    "Leicester": {"nation": "England", "lat": 52.637, "lng": -1.133},
    "Sunderland": {"nation": "England", "lat": 54.906, "lng": -1.381},
    "Hull": {"nation": "England", "lat": 53.745, "lng": -0.335},
    "Bradford": {"nation": "England", "lat": 53.795, "lng": -1.752},
    "Wolverhampton": {"nation": "England", "lat": 52.587, "lng": -2.129},
    "Reading": {"nation": "England", "lat": 51.455, "lng": -0.978},
    "Derby": {"nation": "England", "lat": 52.922, "lng": -1.476},
    "Peterborough": {"nation": "England", "lat": 52.573, "lng": -0.240},
    "Ipswich": {"nation": "England", "lat": 52.059, "lng": 1.155},
    "Oxford": {"nation": "England", "lat": 51.752, "lng": -1.258},
    "Exeter": {"nation": "England", "lat": 50.726, "lng": -3.527},
    "York": {"nation": "England", "lat": 53.958, "lng": -1.080},
    "Swindon": {"nation": "England", "lat": 51.568, "lng": -1.772},
    "Lincoln": {"nation": "England", "lat": 53.234, "lng": -0.538},
    "Gloucester": {"nation": "England", "lat": 51.864, "lng": -2.245},
    "Colchester": {"nation": "England", "lat": 51.889, "lng": 0.903},
    "Blackpool": {"nation": "England", "lat": 53.814, "lng": -3.055},
    "Bolton": {"nation": "England", "lat": 53.577, "lng": -2.429},
    "Burnley": {"nation": "England", "lat": 53.789, "lng": -2.248},
    "Preston": {"nation": "England", "lat": 53.763, "lng": -2.703},
    "Warrington": {"nation": "England", "lat": 53.390, "lng": -2.597},
    "Wigan": {"nation": "England", "lat": 53.546, "lng": -2.632},
    "Stockport": {"nation": "England", "lat": 53.406, "lng": -2.159},
    "Rochdale": {"nation": "England", "lat": 53.616, "lng": -2.155},
    "Halifax": {"nation": "England", "lat": 53.725, "lng": -1.863},
    "Huddersfield": {"nation": "England", "lat": 53.645, "lng": -1.785},
    "Wakefield": {"nation": "England", "lat": 53.683, "lng": -1.498},
    "Doncaster": {"nation": "England", "lat": 53.523, "lng": -1.134},
    "Barnsley": {"nation": "England", "lat": 53.553, "lng": -1.479},
    "Rotherham": {"nation": "England", "lat": 53.430, "lng": -1.357},
    "Grimsby": {"nation": "England", "lat": 53.568, "lng": -0.076},
    "Scunthorpe": {"nation": "England", "lat": 53.588, "lng": -0.651},
    "Northampton": {"nation": "England", "lat": 52.241, "lng": -0.902},
    "Milton Keynes": {"nation": "England", "lat": 52.040, "lng": -0.759},
    "Bedford": {"nation": "England", "lat": 52.136, "lng": -0.460},
    "Basildon": {"nation": "England", "lat": 51.576, "lng": 0.489},
    "Southend": {"nation": "England", "lat": 51.539, "lng": 0.707},
    "Chelmsford": {"nation": "England", "lat": 51.736, "lng": 0.480},
    "Norwich": {"nation": "England", "lat": 52.629, "lng": 1.299},
    "Cambridge": {"nation": "England", "lat": 52.205, "lng": 0.122},
    "Medway": {"nation": "England", "lat": 51.393, "lng": 0.539},
    "Maidstone": {"nation": "England", "lat": 51.272, "lng": 0.522},
    "Crawley": {"nation": "England", "lat": 51.109, "lng": -0.187},
    "Slough": {"nation": "England", "lat": 51.511, "lng": -0.595},
    "Portsmouth": {"nation": "England", "lat": 50.806, "lng": -1.087},
    "Bournemouth": {"nation": "England", "lat": 50.720, "lng": -1.879},
    "Poole": {"nation": "England", "lat": 50.715, "lng": -1.987},
    "Bath": {"nation": "England", "lat": 51.383, "lng": -2.359},
    "Taunton": {"nation": "England", "lat": 51.015, "lng": -3.107},
    "Cheltenham": {"nation": "England", "lat": 51.901, "lng": -2.074},
    "Hereford": {"nation": "England", "lat": 52.056, "lng": -2.716},
    "Telford": {"nation": "England", "lat": 52.677, "lng": -2.449},
    "Salisbury": {"nation": "England", "lat": 51.069, "lng": -1.795},

    # Scotland
    "Glasgow": {"nation": "Scotland", "lat": 55.864, "lng": -4.252},
    "Edinburgh": {"nation": "Scotland", "lat": 55.953, "lng": -3.189},
    "Aberdeen": {"nation": "Scotland", "lat": 57.150, "lng": -2.094},
    "Dundee": {"nation": "Scotland", "lat": 56.462, "lng": -2.970},
    "Inverness": {"nation": "Scotland", "lat": 57.478, "lng": -4.224},
    "Stirling": {"nation": "Scotland", "lat": 56.120, "lng": -3.937},
    "Perth": {"nation": "Scotland", "lat": 56.395, "lng": -3.437},
    "Paisley": {"nation": "Scotland", "lat": 55.846, "lng": -4.424},
    "Kilmarnock": {"nation": "Scotland", "lat": 55.611, "lng": -4.495},
    "Ayr": {"nation": "Scotland", "lat": 55.459, "lng": -4.629},
    "Falkirk": {"nation": "Scotland", "lat": 56.001, "lng": -3.784},
    "Livingston": {"nation": "Scotland", "lat": 55.883, "lng": -3.516},

    # Wales
    "Cardiff": {"nation": "Wales", "lat": 51.481, "lng": -3.179},
    "Swansea": {"nation": "Wales", "lat": 51.622, "lng": -3.943},
    "Newport": {"nation": "Wales", "lat": 51.589, "lng": -2.998},
    "Wrexham": {"nation": "Wales", "lat": 53.046, "lng": -2.993},
    "Bangor": {"nation": "Wales", "lat": 53.228, "lng": -4.129},
    "Aberystwyth": {"nation": "Wales", "lat": 52.416, "lng": -4.082},
    "Carmarthen": {"nation": "Wales", "lat": 51.857, "lng": -4.311},
    "Llanelli": {"nation": "Wales", "lat": 51.684, "lng": -4.163},
    "Bridgend": {"nation": "Wales", "lat": 51.504, "lng": -3.577},
    "Pontypridd": {"nation": "Wales", "lat": 51.602, "lng": -3.342},
    "Merthyr Tydfil": {"nation": "Wales", "lat": 51.748, "lng": -3.378},
    "Barry": {"nation": "Wales", "lat": 51.400, "lng": -3.268},

    # Northern Ireland
    "Belfast": {"nation": "Northern Ireland", "lat": 54.597, "lng": -5.930},
    "Derry": {"nation": "Northern Ireland", "lat": 54.997, "lng": -7.319},
    "Newry": {"nation": "Northern Ireland", "lat": 54.176, "lng": -6.337},
    "Lisburn": {"nation": "Northern Ireland", "lat": 54.510, "lng": -6.038},
    "Bangor NI": {"nation": "Northern Ireland", "lat": 54.654, "lng": -5.670},
    "Craigavon": {"nation": "Northern Ireland", "lat": 54.447, "lng": -6.389},
    "Ballymena": {"nation": "Northern Ireland", "lat": 54.864, "lng": -6.276},
    "Newtownabbey": {"nation": "Northern Ireland", "lat": 54.660, "lng": -5.906},
    "Omagh": {"nation": "Northern Ireland", "lat": 54.600, "lng": -7.300},
    "Enniskillen": {"nation": "Northern Ireland", "lat": 54.344, "lng": -7.639},
}


class GoogleTrendsFetcher:
    """Fetches and processes Google Trends data for UK food bank searches using direct API calls."""

    PRIMARY_TERM = "food bank"
    NATIONS = {
        "GB-ENG": "England",
        "GB-SCT": "Scotland",
        "GB-WLS": "Wales",
        "GB-NIR": "Northern Ireland",
    }
    REQUEST_DELAY_MIN = 8  # minimum seconds between requests
    REQUEST_DELAY_MAX = 15  # maximum seconds between requests
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF_BASE = 30  # seconds base for exponential backoff

    API_EXPLORE_URL = "https://trends.google.com/trends/api/explore"
    API_GEO_MAP_URL = "https://trends.google.com/trends/api/widgetdata/comparedgeo"
    API_TIMESERIES_URL = "https://trends.google.com/trends/api/widgetdata/multiline"
    TRENDS_BASE_URL = "https://trends.google.com/"

    def __init__(self, dry_run: bool = False):
        """
        Initialize the fetcher.

        Args:
            dry_run: If True, return sample data without hitting Google Trends
        """
        self.dry_run = dry_run
        self.session = None

        if not dry_run:
            # Initialize session with browser-like headers
            self.session = requests.Session()
            self._init_session()
            # Initial delay to avoid immediate rate-limiting
            logger.info("Warming up connection (5s delay)...")
            time.sleep(5)

        self.data = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "timeframe_1d": "now 1-d",
            "timeframe_short": "now 7-d",
            "timeframe_long": "today 3-m",
            "regions": [],
            "national_trend": [],
            "cities": [],
        }

    def _init_session(self):
        """Initialize the session with browser-like headers and cookies."""
        if not self.session:
            self.session = requests.Session()

        # Set browser-like headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-GB,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

        # Visit trends.google.com to establish session cookies
        try:
            logger.info("Initializing session with trends.google.com...")
            self.session.get(self.TRENDS_BASE_URL, timeout=10)
        except Exception as e:
            logger.warning(f"Failed to initialize session: {e}")

    def _strip_json_prefix(self, response_text: str) -> str:
        """Strip the ')]}'' prefix from Google Trends API responses."""
        if response_text.startswith(")]}\'"):
            return response_text[5:]
        return response_text

    def _make_api_request(self, url: str, params: Dict[str, Any], operation_name: str) -> Optional[Dict]:
        """
        Make an API request with proper headers and error handling.

        Args:
            url: API endpoint URL
            params: Request parameters
            operation_name: Name for logging

        Returns:
            Parsed JSON response or None on failure
        """
        try:
            logger.info(f"Making API request: {operation_name}")
            response = self.session.get(url, params=params, timeout=15)

            if response.status_code == 429:
                raise Exception(f"429 Rate Limit - {operation_name}")

            response.raise_for_status()

            # Strip JSON prefix and parse
            clean_text = self._strip_json_prefix(response.text)
            return json.loads(clean_text)

        except Exception as e:
            logger.error(f"API request failed ({operation_name}): {e}")
            raise

    def _get_explore_widgets(self, keyword: str, geo: str, timeframe: str) -> Optional[List[Dict]]:
        """
        Get widget tokens from the explore endpoint.

        Args:
            keyword: Search term
            geo: Geographic code (e.g., 'GB', 'GB-ENG')
            timeframe: Time range (e.g., 'now 7-d', 'today 3-m')

        Returns:
            List of widget objects or None on failure
        """
        req_json = {
            "comparisonItem": [
                {
                    "keyword": keyword,
                    "geo": geo,
                    "time": timeframe
                }
            ],
            "category": 0,
            "property": ""
        }

        params = {
            'hl': 'en-GB',
            'tz': '0',
            'req': json.dumps(req_json),
            'token': ''
        }

        try:
            result = self._make_api_request(
                self.API_EXPLORE_URL,
                params,
                f"explore({keyword}, {geo}, {timeframe})"
            )
            return result.get('widgets', [])
        except Exception:
            return None

    def _get_widget_data(self, widget_id: str, api_url: str, req_obj: Dict, widget_token: str, operation_name: str) -> Optional[Dict]:
        """
        Get data from a specific widget using its token.

        Args:
            widget_id: Widget type (e.g., 'GEO_MAP', 'TIMESERIES')
            api_url: API endpoint URL
            req_obj: Request object from widget
            widget_token: Token from widget
            operation_name: Name for logging

        Returns:
            Parsed widget data or None on failure
        """
        params = {
            'hl': 'en-GB',
            'tz': '0',
            'req': json.dumps(req_obj),
            'token': widget_token
        }

        try:
            result = self._make_api_request(api_url, params, operation_name)
            return result
        except Exception:
            return None

    def _request_with_backoff(self, operation_name: str, operation_func, *args, **kwargs) -> Optional[Any]:
        """
        Execute an operation with exponential backoff retry logic.

        Args:
            operation_name: Name of operation for logging
            operation_func: Function to call
            *args: Positional arguments for operation_func
            **kwargs: Keyword arguments for operation_func

        Returns:
            Result of operation_func, or None if all retries exhausted
        """
        for attempt in range(1, self.RETRY_ATTEMPTS + 1):
            try:
                logger.info(f"Attempt {attempt}/{self.RETRY_ATTEMPTS}: {operation_name}")
                result = operation_func(*args, **kwargs)
                logger.info(f"Success: {operation_name}")
                return result
            except Exception as e:
                error_str = str(e)
                is_rate_limit = "429" in error_str or "Rate Limit" in error_str
                logger.warning(f"{operation_name} error: {type(e).__name__}: {error_str}")

                if attempt < self.RETRY_ATTEMPTS:
                    # Exponential backoff with jitter: 30s, 60s, 120s + random 0-30s
                    wait_time = self.RETRY_BACKOFF_BASE * (2 ** (attempt - 1)) + random.uniform(0, 30)
                    logger.warning(f"Retrying in {wait_time:.0f}s (attempt {attempt}/{self.RETRY_ATTEMPTS})")
                    time.sleep(wait_time)

                    # Reinitialize session on rate-limit errors
                    if is_rate_limit:
                        logger.info("Reinitializing session after rate limit...")
                        self._init_session()
                    continue
                else:
                    logger.error(f"{operation_name} failed after {self.RETRY_ATTEMPTS} attempts: {error_str}")
                    return None
        return None

    def fetch_regional_data(self) -> bool:
        """
        Fetch regional data for primary search term across GB.
        Fetches for 1-day, 7-day, and 90-day timeframes.

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info("DRY RUN: Skipping regional data fetch")
            return True

        regional_data = {}

        # Fetch 1-day data
        logger.info(f"Fetching regional data for 1-day timeframe ('{self.PRIMARY_TERM}')...")

        def fetch_1d():
            return self._fetch_regional_for_timeframe(self.PRIMARY_TERM, 'GB', 'now 1-d')

        result_1d = self._request_with_backoff(
            f"Fetch regional data (1d): '{self.PRIMARY_TERM}'",
            fetch_1d
        )

        if result_1d is None:
            logger.error("Failed to fetch 1-day regional data")
            return False

        regional_data['1d'] = result_1d

        # Delay before switching timeframe
        delay = random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX)
        logger.info(f"Waiting {delay:.1f}s before 7-day fetch...")
        time.sleep(delay)

        # Fetch 7-day data
        logger.info(f"Fetching regional data for 7-day timeframe ('{self.PRIMARY_TERM}')...")

        def fetch_7d():
            return self._fetch_regional_for_timeframe(self.PRIMARY_TERM, 'GB', 'now 7-d')

        result_7d = self._request_with_backoff(
            f"Fetch regional data (7d): '{self.PRIMARY_TERM}'",
            fetch_7d
        )

        if result_7d is None:
            logger.error("Failed to fetch 7-day regional data")
            return False

        regional_data['7d'] = result_7d

        # Delay before switching timeframe
        delay = random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX)
        logger.info(f"Waiting {delay:.1f}s before 90-day fetch...")
        time.sleep(delay)

        # Fetch 90-day data
        logger.info(f"Fetching regional data for 90-day timeframe ('{self.PRIMARY_TERM}')...")

        def fetch_90d():
            return self._fetch_regional_for_timeframe(self.PRIMARY_TERM, 'GB', 'today 3-m')

        result_90d = self._request_with_backoff(
            f"Fetch regional data (90d): '{self.PRIMARY_TERM}'",
            fetch_90d
        )

        if result_90d is None:
            logger.error("Failed to fetch 90-day regional data")
            return False

        regional_data['90d'] = result_90d

        # Process and combine data
        all_regions = set(regional_data['1d'].keys()) | set(regional_data['7d'].keys()) | set(regional_data['90d'].keys())

        for region in sorted(all_regions):
            score_1d = regional_data['1d'].get(region, 0)
            score_7d = regional_data['7d'].get(region, 0)
            score_90d = regional_data['90d'].get(region, 0)

            # Only include if there's data for at least one timeframe
            if score_1d or score_7d or score_90d:
                self.data["regions"].append({
                    "name": region,
                    "score_1d": score_1d,
                    "score_7d": score_7d,
                    "score_90d": score_90d,
                    "terms": {
                        self.PRIMARY_TERM: score_90d  # Use 90d score for terms
                    }
                })

        logger.info(f"Successfully fetched regional data for {len(self.data['regions'])} regions")
        return True

    def _fetch_regional_for_timeframe(self, keyword: str, geo: str, timeframe: str) -> Optional[Dict[str, int]]:
        """
        Fetch regional data for a specific timeframe.

        Args:
            keyword: Search term
            geo: Geographic code
            timeframe: Time range

        Returns:
            Dictionary of region names to scores
        """
        widgets = self._get_explore_widgets(keyword, geo, timeframe)
        if not widgets:
            logger.error("Failed to get explore widgets")
            return None

        # Find GEO_MAP widget
        geo_widget = None
        for widget in widgets:
            if widget.get('id') == 'GEO_MAP':
                geo_widget = widget
                break

        if not geo_widget:
            logger.error("GEO_MAP widget not found")
            return None

        # Get widget data
        widget_request = geo_widget.get('request', {})
        widget_token = geo_widget.get('token', '')

        data = self._get_widget_data(
            'GEO_MAP',
            self.API_GEO_MAP_URL,
            widget_request,
            widget_token,
            f"regional data ({keyword}, {geo}, {timeframe})"
        )

        if not data:
            return None

        # Parse regional data
        regional_dict = {}
        try:
            lines = data.get('default', {}).get('timelineData', [])
            keys = data.get('default', {}).get('geoMapData', [])

            # Map keys to regions and scores
            for idx, key_data in enumerate(keys):
                region_name = key_data.get('geoName', '')
                if idx < len(lines):
                    score = int(lines[idx].get('value', [0])[0])
                    regional_dict[region_name] = score

        except Exception as e:
            logger.error(f"Error parsing regional data: {e}")
            return None

        return regional_dict

    def fetch_national_trend(self) -> bool:
        """
        Fetch national trend data for the primary search term.

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info("DRY RUN: Skipping national trend fetch")
            return True

        logger.info(f"Fetching national trend data ('{self.PRIMARY_TERM}')...")

        def fetch_trend():
            return self._fetch_timeseries(self.PRIMARY_TERM, 'GB', 'today 3-m')

        result = self._request_with_backoff(
            f"Fetch national trend: '{self.PRIMARY_TERM}'",
            fetch_trend
        )

        if result is None:
            logger.error("Failed to fetch national trend data")
            return False

        # Convert to list of dicts
        for entry in result:
            self.data["national_trend"].append({
                "date": entry['date'],
                "value": entry['value'],
            })

        logger.info(f"Successfully fetched national trend with {len(self.data['national_trend'])} data points")
        return True

    def _fetch_timeseries(self, keyword: str, geo: str, timeframe: str) -> Optional[List[Dict]]:
        """
        Fetch timeseries data for a keyword.

        Args:
            keyword: Search term
            geo: Geographic code
            timeframe: Time range

        Returns:
            List of dicts with 'date' and 'value' keys
        """
        widgets = self._get_explore_widgets(keyword, geo, timeframe)
        if not widgets:
            logger.error("Failed to get explore widgets for timeseries")
            return None

        # Find TIMESERIES widget
        ts_widget = None
        for widget in widgets:
            if widget.get('id') == 'TIMESERIES':
                ts_widget = widget
                break

        if not ts_widget:
            logger.error("TIMESERIES widget not found")
            return None

        # Get widget data
        widget_request = ts_widget.get('request', {})
        widget_token = ts_widget.get('token', '')

        data = self._get_widget_data(
            'TIMESERIES',
            self.API_TIMESERIES_URL,
            widget_request,
            widget_token,
            f"timeseries ({keyword}, {geo}, {timeframe})"
        )

        if not data:
            return None

        # Parse timeseries data
        result = []
        try:
            timeline = data.get('default', {}).get('timelineData', [])

            for entry in timeline:
                timestamp = int(entry.get('time', 0))
                value = int(entry.get('value', [0])[0])

                # Convert timestamp to date string
                from datetime import datetime as dt
                date_str = dt.utcfromtimestamp(timestamp).strftime('%Y-%m-%d')

                result.append({
                    'date': date_str,
                    'value': value
                })

        except Exception as e:
            logger.error(f"Error parsing timeseries data: {e}")
            return None

        return result

    def fetch_city_data(self) -> bool:
        """
        Fetch city-level data for each nation.

        Returns:
            True if at least one nation succeeds, False if all fail
        """
        if self.dry_run:
            logger.info("DRY RUN: Skipping city data fetch")
            return True

        any_success = False

        for nation_code, nation_name in self.NATIONS.items():
            logger.info(f"Fetching city-level data for {nation_name}...")

            # Delay between nation requests
            delay = random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX)
            logger.info(f"Waiting {delay:.1f}s before {nation_name} fetch...")
            time.sleep(delay)

            def fetch_cities():
                return self._fetch_city_data_for_nation(self.PRIMARY_TERM, nation_code, 'today 3-m')

            result = self._request_with_backoff(
                f"Fetch city data: {nation_name}",
                fetch_cities
            )

            if result is None:
                logger.warning(f"Failed to fetch city data for {nation_name}, continuing with other nations...")
                continue

            any_success = True

            # Process city data
            for city_name, score in result.items():
                if city_name in CITY_GEOCODES:
                    geo_data = CITY_GEOCODES[city_name]
                    self.data["cities"].append({
                        "name": city_name,
                        "score": score,
                        "nation": geo_data["nation"],
                        "lat": geo_data["lat"],
                        "lng": geo_data["lng"],
                    })
                else:
                    logger.warning(f"City '{city_name}' not in geocoding lookup, skipping")

            city_count = len([c for c in self.data['cities'] if c['nation'] == nation_name])
            logger.info(f"Successfully fetched city data for {nation_name}: {city_count} cities")

        if not any_success:
            logger.error("Failed to fetch city data for any nation")
            return False

        return True

    def _fetch_city_data_for_nation(self, keyword: str, nation_code: str, timeframe: str) -> Optional[Dict[str, int]]:
        """
        Fetch city-level data for a specific nation.

        Args:
            keyword: Search term
            nation_code: Nation geographic code (e.g., 'GB-ENG')
            timeframe: Time range

        Returns:
            Dictionary of city names to scores
        """
        widgets = self._get_explore_widgets(keyword, nation_code, timeframe)
        if not widgets:
            logger.error(f"Failed to get explore widgets for {nation_code}")
            return None

        # Find GEO_MAP widget
        geo_widget = None
        for widget in widgets:
            if widget.get('id') == 'GEO_MAP':
                geo_widget = widget
                break

        if not geo_widget:
            logger.error(f"GEO_MAP widget not found for {nation_code}")
            return None

        # Modify request to set resolution to CITY
        widget_request = geo_widget.get('request', {})
        widget_request['resolution'] = 'CITY'
        widget_token = geo_widget.get('token', '')

        data = self._get_widget_data(
            'GEO_MAP',
            self.API_GEO_MAP_URL,
            widget_request,
            widget_token,
            f"city data ({keyword}, {nation_code}, {timeframe})"
        )

        if not data:
            return None

        # Parse city data
        city_dict = {}
        try:
            lines = data.get('default', {}).get('timelineData', [])
            keys = data.get('default', {}).get('geoMapData', [])

            # Map keys to cities and scores
            for idx, key_data in enumerate(keys):
                city_name = key_data.get('geoName', '')
                if idx < len(lines):
                    score = int(lines[idx].get('value', [0])[0])
                    city_dict[city_name] = score

        except Exception as e:
            logger.error(f"Error parsing city data: {e}")
            return None

        return city_dict

    def generate_sample_data(self):
        """Generate realistic sample data for dry-run mode."""
        logger.info("Generating sample data for dry-run mode...")

        # Sample regions
        self.data["regions"] = [
            {
                "name": "London",
                "score_1d": 90,
                "score_7d": 85,
                "score_90d": 72,
                "terms": {
                    "food bank": 90,
                    "food bank near me": 82,
                    "food bank help": 83,
                }
            },
            {
                "name": "Manchester",
                "score_1d": 82,
                "score_7d": 78,
                "score_90d": 68,
                "terms": {
                    "food bank": 88,
                    "food bank near me": 75,
                    "food bank help": 71,
                }
            },
            {
                "name": "Birmingham",
                "score_1d": 75,
                "score_7d": 71,
                "score_90d": 65,
                "terms": {
                    "food bank": 82,
                    "food bank near me": 68,
                    "food bank help": 65,
                }
            },
            {
                "name": "Glasgow",
                "score_1d": 71,
                "score_7d": 68,
                "score_90d": 62,
                "terms": {
                    "food bank": 75,
                    "food bank near me": 65,
                    "food bank help": 58,
                }
            },
        ]

        # Sample national trend
        from datetime import timedelta, datetime as dt
        base_date = dt.utcnow() - timedelta(days=90)
        for i in range(90):
            current_date = base_date + timedelta(days=i)
            self.data["national_trend"].append({
                "date": current_date.strftime('%Y-%m-%d'),
                "value": 60 + int(10 * (i / 90)),
            })

        # Sample cities
        sample_cities = [
            ("Pudsey", "England", 53.797, -1.662, 92),
            ("London", "England", 51.507, -0.128, 88),
            ("Manchester", "England", 53.483, -2.244, 85),
            ("Glasgow", "Scotland", 55.864, -4.252, 82),
            ("Edinburgh", "Scotland", 55.953, -3.189, 78),
            ("Cardiff", "Wales", 51.481, -3.179, 75),
            ("Belfast", "Northern Ireland", 54.597, -5.930, 72),
        ]

        for city_name, nation, lat, lng, score in sample_cities:
            self.data["cities"].append({
                "name": city_name,
                "score": score,
                "nation": nation,
                "lat": lat,
                "lng": lng,
            })

        logger.info("Sample data generated successfully")

    def fetch_all(self) -> bool:
        """
        Fetch all data: regional, national trend, and city-level.

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            self.generate_sample_data()
            return True

        logger.info("Starting Google Trends data fetch (using direct API)...")

        # Fetch regional data (critical)
        if not self.fetch_regional_data():
            logger.critical("Regional data fetch failed - exiting")
            return False

        # Delay before national trend
        delay = random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX)
        logger.info(f"Waiting {delay:.1f}s before national trend fetch...")
        time.sleep(delay)

        # Fetch national trend
        if not self.fetch_national_trend():
            logger.warning("National trend fetch failed - continuing")
        else:
            # Delay before city data
            delay = random.uniform(self.REQUEST_DELAY_MIN, self.REQUEST_DELAY_MAX)
            logger.info(f"Waiting {delay:.1f}s before city data fetch...")
            time.sleep(delay)

        # Fetch city data (non-critical, but log if all nations fail)
        if not self.fetch_city_data():
            logger.warning("City data fetch failed for all nations")

        logger.info("All data fetching complete")
        return True

    def save(self, output_path: str):
        """
        Save data to JSON file.

        Args:
            output_path: Path to output JSON file
        """
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        with open(output_file, 'w') as f:
            json.dump(self.data, f, indent=2)

        logger.info(f"Data saved to {output_path}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch Google Trends data for UK food bank searches'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Output sample data without hitting Google Trends'
    )
    parser.add_argument(
        '--output',
        default='data/trends_data.json',
        help='Output path for JSON data (default: data/trends_data.json)'
    )

    args = parser.parse_args()

    fetcher = GoogleTrendsFetcher(dry_run=args.dry_run)

    if not fetcher.fetch_all():
        logger.critical("Failed to fetch required data")
        sys.exit(1)

    fetcher.save(args.output)
    logger.info("Successfully completed data fetch and save")


if __name__ == '__main__':
    main()
