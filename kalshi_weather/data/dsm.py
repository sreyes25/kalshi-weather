"""
NWS ASOS Daily Summary (DSM) Parser.

Fetches and parses the Daily Summary Message (DSM) from NWS.
This provides the official daily high/low temperatures for settlement verification.
"""

import logging
import re
from datetime import datetime
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from kalshi_weather.core import DailyObservation, StationReading, StationType
from kalshi_weather.config import (
    DEFAULT_CITY,
    CityConfig,
    NWS_USER_AGENT,
    API_TIMEOUT,
)

logger = logging.getLogger(__name__)

# DSM Product URL Template
# Expects keys: site, issuedby
DSM_URL_TEMPLATE = (
    "https://forecast.weather.gov/product.php?"
    "site={site}&issuedby={issuedby}&product=DSM&format=txt&version=1&glossary=0"
)

# Regex to find the data line: "KNYC DS 1600 02/02 351559/ 140159// ..."
# Capture groups:
# 1. Station ID (e.g. KNYC)
# 2. Time (e.g. 1600)
# 3. Date (e.g. 02/02)
# 4. Max Temp Group (e.g. 351559/)
# 5. Min Temp Group (e.g. 140159//)
DSM_LINE_REGEX = re.compile(
    r"^(?P<station>[A-Z]{4})\s+DS\s+(?P<time>\d{4})\s+(?P<date>\d{2}/\d{2})\s+"
    r"(?P<max_group>[\dM-]+)/+\s+(?P<min_group>[\dM-]+)/+"
)

# Regex to parse temp/time group: "351559" -> 35 F at 15:59
# Handles negative values if they use 'M' or '-' prefix, though typically DSM might use specific coding.
# For now assuming standard integer format, potentially signed.
TEMP_TIME_REGEX = re.compile(r"^(?P<temp>-?\d+|M\d+)(?P<time>\d{4})$")


def parse_dsm_temp(group_str: str) -> Optional[tuple[float, str]]:
    """
    Parse a DSM temperature/time group (e.g., '351559').
    Returns (temp_f, time_str) or None.
    """
    # Remove trailing slashes
    clean_str = group_str.rstrip("/")
    
    match = TEMP_TIME_REGEX.match(clean_str)
    if not match:
        return None
    
    temp_str = match.group("temp")
    time_str = match.group("time")
    
    # Handle 'M' for minus if applicable (standard in some NWS formats)
    if temp_str.startswith("M"):
        temp_val = -float(temp_str[1:])
    else:
        try:
            temp_val = float(temp_str)
        except ValueError:
            return None
            
    return temp_val, time_str


class DSMParser:
    """Fetches and parses NWS Daily Summary Messages."""

    def __init__(self, city: CityConfig = None):
        """
        Initialize with city configuration.
        """
        self.city = city or DEFAULT_CITY
        # Determine NWS site params. 
        # For NYC (KNYC), the WFO is typically OKX, but the URL often works with site=DMX/NYC?
        # The user's link: site=DMX&issuedby=NYC.
        # We need to be careful here. DMX is Des Moines! 
        # But the *text* contains KNYC.
        # `issuedby` is the WFO. For NYC it is `OKX` or `NYC` alias.
        # We will expose these as optional config overrides or infer from city.
        self.site = "OKX" # Default to Upton/NYC for NYC
        self.issued_by = "NYC"
        self.station_id = self.city.station_id # e.g. KNYC

    def _get_url(self, version: int = 1) -> str:
        """Construct the DSM URL with specific version."""
        # For now, hardcode to the user's working link or a generic one if we can.
        # User link: site=DMX&issuedby=NYC. 
        # The 'issuedby=NYC' seems to be the key for identifying the product source for KNYC.
        return DSM_URL_TEMPLATE.format(site="NWS", issuedby="NYC").replace("version=1", f"version={version}")

    def fetch_dsm(self, version: int = 1) -> Optional[DailyObservation]:
        """
        Fetch and parse a specific version of the DSM.
        Returns a DailyObservation if successful.
        """
        url = self._get_url(version=version)
        try:
            response = requests.get(
                url, 
                headers={"User-Agent": NWS_USER_AGENT},
                timeout=API_TIMEOUT
            )
            response.raise_for_status()
            text = response.text
            return self._parse_dsm_text(text)
        except Exception as e:
            logger.error(f"Error fetching DSM version {version}: {e}")
            return None

    def fetch_dsms_for_date(self, target_date_str: str) -> list[DailyObservation]:
        """
        Fetch all DSMs matching the given date string (YYYY-MM-DD).
        Iterates backwards through versions until an older date is found.
        """
        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        found_obs = []
        
        # Limit iteration to avoid infinite loops if something is weird
        max_versions = 30 
        
        for v in range(1, max_versions + 1):
            obs = self.fetch_dsm(version=v)
            
            if not obs:
                # If we fail to fetch a version, we might have hit the end or a glitch.
                # If it is version 1, it's an error. If later, maybe end of list.
                if v == 1:
                    logger.warning("Could not fetch latest DSM.")
                else:
                    logger.info(f"Stopped fetching at version {v} (no data).")
                break
                
            obs_date = datetime.strptime(obs.date, "%Y-%m-%d").date()
            
            if obs_date == target_date:
                found_obs.append(obs)
            elif obs_date < target_date:
                # We reached older data, stop.
                break
            # If obs_date > target_date, it's newer data, so just continue to next version (older).
            
        return found_obs

    def _parse_dsm_text(self, text: str) -> Optional[DailyObservation]:
        """Parse the raw text of the DSM."""
        # Use regex search on the full text to find the data line.
        # This handles cases where the line might be embedded in HTML or formatted differently.
        # We construct a regex that explicitly looks for the station ID provided.
        
        # Pattern: STATION DS TIME DATE MAX MIN
        # e.g. KNYC DS 1600 02/02 351559/ 140159//
        pattern = re.compile(
            rf"(?P<station>{self.station_id})\s+DS\s+(?P<time>\d{{4}})\s+(?P<date>\d{{2}}/\d{{2}})\s+"
            r"(?P<max_group>[\dM-]+)/+\s+(?P<min_group>[\dM-]+)/+"
        )
        
        match = pattern.search(text)
        if not match:
            logger.warning(f"DSM pattern not found for {self.station_id} in response")
            return None

        data = match.groupdict()
        
        # Parse Max
        max_data = parse_dsm_temp(data["max_group"])
        if not max_data:
            logger.warning(f"Failed to parse max group: {data['max_group']}")
            return None
        max_temp, max_time = max_data

        # Parse Min
        min_data = parse_dsm_temp(data["min_group"])
        if not min_data:
            logger.warning(f"Failed to parse min group: {data['min_group']}")
            return None
        min_temp, min_time = min_data

        # Determine Date
        # DSM date is DD/MM. We need to attach the correct year.
        # Usually it's current year, but near Jan 1st be careful.
        # We'll use current UTC date to infer year.
        now = datetime.now(ZoneInfo("UTC"))
        try:
            dsm_month, dsm_day = map(int, data["date"].split("/"))
            
            # Simple year inference
            year = now.year
            # If today is Jan and DSM is Dec -> last year
            if now.month == 1 and dsm_month == 12:
                year -= 1
            # If today is Dec and DSM is Jan -> next year (typically implies issue, but assume current)
            
            obs_date_str = f"{year}-{dsm_month:02d}-{dsm_day:02d}"
            
        except ValueError:
            logger.warning(f"Failed to parse date: {data['date']}")
            return None

        # Create DailyObservation
        # Note: DSM is "Summary", so readings list is empty, but we provide the high/low.
        # We set 'possible_actual' bounds to exact values as DSM is the authority.
        
        return DailyObservation(
            station_id=self.station_id,
            date=obs_date_str,
            observed_high_f=max_temp,
            possible_actual_high_low=max_temp,
            possible_actual_high_high=max_temp,
            readings=[],
            last_updated=datetime.now(ZoneInfo(self.city.timezone)),
        )


def get_dsm_observation(city: CityConfig = None) -> Optional[DailyObservation]:
    """Convenience function to fetch DSM observation."""
    parser = DSMParser(city)
    return parser.fetch_dsm()

