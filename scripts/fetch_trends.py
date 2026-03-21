#!/usr/bin/env python3
"""
Fetch Google Trends data for UK food bank searches.

This script collects regional, city-level, and national trend data for food bank
searches across the UK, with exponential backoff retry logic and comprehensive error handling.
"""

import json
import logging
import argparse
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional
import sys

try:
    from pytrends.request import TrendReq
except ImportError:
    print("Error: pytrends library not installed. Run: pip install pytrends>=4.9.0", file=sys.stderr)
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
    """Fetches and processes Google Trends data for UK food bank searches."""

    SEARCH_TERMS = ["food bank", "food bank near me", "food bank help"]
    NATIONS = {
        "GB-ENG": "England",
        "GB-SCT": "Scotland",
        "GB-WLS": "Wales",
        "GB-NIR": "Northern Ireland",
    }
    REQUEST_DELAY = 5  # seconds
    RETRY_ATTEMPTS = 3
    RETRY_BACKOFF_BASE = 2  # exponential backoff multiplier

    def __init__(self, dry_run: bool = False):
        """
        Initialize the fetcher.

        Args:
            dry_run: If True, return sample data without hitting Google Trends
        """
        self.dry_run = dry_run
        self.pytrends = None if dry_run else TrendReq(hl='en-US', tz=0)
        self.data = {
            "last_updated": datetime.utcnow().isoformat() + "Z",
            "timeframe_short": "now 7-d",
            "timeframe_long": "today 3-m",
            "regions": [],
            "national_trend": [],
            "cities": [],
        }

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
                return operation_func(*args, **kwargs)
            except Exception as e:
                if "429" in str(e) or "Too Many Requests" in str(e):
                    if attempt < self.RETRY_ATTEMPTS:
                        wait_time = self.RETRY_BACKOFF_BASE ** (attempt - 1)
                        logger.warning(
                            f"{operation_name} received 429 error. "
                            f"Retrying in {wait_time}s (attempt {attempt}/{self.RETRY_ATTEMPTS})"
                        )
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"{operation_name} failed after {self.RETRY_ATTEMPTS} attempts due to 429 error"
                        )
                        return None
                else:
                    logger.error(f"{operation_name} failed: {str(e)}")
                    return None
        return None

    def fetch_regional_data(self) -> bool:
        """
        Fetch regional data for all search terms across GB.

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info("DRY RUN: Skipping regional data fetch")
            return True

        logger.info("Fetching regional data for 7-day timeframe...")
        regional_7d = {}

        for idx, term in enumerate(self.SEARCH_TERMS):
            if idx > 0:
                time.sleep(self.REQUEST_DELAY)

            def fetch_7d():
                self.pytrends.build_payload([term], timeframe='now 7-d', geo='GB')
                return self.pytrends.interest_by_region()

            result = self._request_with_backoff(
                f"Fetch regional data (7d): '{term}'",
                fetch_7d
            )

            if result is None:
                logger.error("Failed to fetch 7-day regional data")
                return False

            regional_7d[term] = result

        logger.info("Fetching regional data for 90-day timeframe...")
        regional_90d = {}

        for idx, term in enumerate(self.SEARCH_TERMS):
            if idx > 0:
                time.sleep(self.REQUEST_DELAY)

            def fetch_90d():
                self.pytrends.build_payload([term], timeframe='today 3-m', geo='GB')
                return self.pytrends.interest_by_region()

            result = self._request_with_backoff(
                f"Fetch regional data (90d): '{term}'",
                fetch_90d
            )

            if result is None:
                logger.error("Failed to fetch 90-day regional data")
                return False

            regional_90d[term] = result

        # Combine scores across all 3 terms as simple mean per region
        all_regions = set()
        for term_data in regional_7d.values():
            all_regions.update(term_data.index)
        for term_data in regional_90d.values():
            all_regions.update(term_data.index)

        for region in sorted(all_regions):
            scores_7d = []
            scores_90d = []
            term_scores = {}

            for term in self.SEARCH_TERMS:
                if region in regional_7d[term].index:
                    score = regional_7d[term].loc[region, term]
                    scores_7d.append(score)

                if region in regional_90d[term].index:
                    score = regional_90d[term].loc[region, term]
                    scores_90d.append(score)
                    term_scores[term] = int(score)

            # Only include regions with data
            if scores_7d or scores_90d:
                self.data["regions"].append({
                    "name": region,
                    "score_7d": int(sum(scores_7d) / len(scores_7d)) if scores_7d else 0,
                    "score_90d": int(sum(scores_90d) / len(scores_90d)) if scores_90d else 0,
                    "terms": term_scores,
                })

        logger.info(f"Successfully fetched regional data for {len(self.data['regions'])} regions")
        return True

    def fetch_national_trend(self) -> bool:
        """
        Fetch national trend data for the primary search term.

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info("DRY RUN: Skipping national trend fetch")
            return True

        logger.info("Fetching national trend data...")

        def fetch_trend():
            self.pytrends.build_payload(["food bank"], timeframe='today 3-m', geo='GB')
            return self.pytrends.interest_over_time()

        result = self._request_with_backoff(
            "Fetch national trend: 'food bank'",
            fetch_trend
        )

        if result is None:
            logger.error("Failed to fetch national trend data")
            return False

        # Convert to list of dicts
        for date_idx in result.index:
            self.data["national_trend"].append({
                "date": date_idx.strftime('%Y-%m-%d'),
                "value": int(result.loc[date_idx, 'food bank']),
            })

        logger.info(f"Successfully fetched national trend with {len(self.data['national_trend'])} data points")
        return True

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
            time.sleep(3)

            def fetch_cities():
                self.pytrends.build_payload(
                    ["food bank"],
                    timeframe='today 3-m',
                    geo=nation_code
                )
                return self.pytrends.interest_by_region(resolution='CITY')

            result = self._request_with_backoff(
                f"Fetch city data: {nation_name}",
                fetch_cities
            )

            if result is None:
                logger.warning(f"Failed to fetch city data for {nation_name}, continuing with other nations...")
                continue

            any_success = True

            # Process city data
            for city_name in result.index:
                score = int(result.loc[city_name, 'food bank'])

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

            logger.info(f"Successfully fetched city data for {nation_name}: {len([c for c in self.data['cities'] if c['nation'] == nation_name])} cities")

        if not any_success:
            logger.error("Failed to fetch city data for any nation")
            return False

        return True

    def generate_sample_data(self):
        """Generate realistic sample data for dry-run mode."""
        logger.info("Generating sample data for dry-run mode...")

        # Sample regions
        self.data["regions"] = [
            {
                "name": "London",
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

        logger.info("Starting Google Trends data fetch...")

        # Fetch regional data (critical)
        if not self.fetch_regional_data():
            logger.critical("Regional data fetch failed - exiting")
            return False

        # Fetch national trend
        if not self.fetch_national_trend():
            logger.warning("National trend fetch failed - continuing")

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
