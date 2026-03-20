"""
Historical weather data fetcher for settlement verification.

Fetches past daily high temperatures from NWS Daily Climate Reports (CLI)
via the Iowa Environmental Mesonet (IEM) archive. This is the same data source
Kalshi uses for market settlement.

Falls back to Open-Meteo Archive API if NWS data is unavailable.
"""

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import requests

from kalshi_weather.config import CityConfig, DEFAULT_CITY, API_TIMEOUT, NWS_USER_AGENT

logger = logging.getLogger(__name__)

# Iowa Environmental Mesonet - archives NWS text products including CLI
IEM_AFOS_URL = "https://mesonet.agron.iastate.edu/cgi-bin/afos/retrieve.py"

# Open-Meteo Archive API (fallback)
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# CLI Product Identifiers by city (NWS AFOS PIL codes)
# Format: CLI + station identifier
CLI_PRODUCT_IDS = {
    "NYC": "CLINYC",  # Central Park, NY
    "CHI": "CLIORD",  # Chicago O'Hare
    "LAX": "CLILAX",  # Los Angeles
    "MIA": "CLIMIA",  # Miami
    "AUS": "CLIAUS",  # Austin
}


@dataclass
class SettlementRecord:
    """
    Historical settlement data for a past date.

    Contains the official high temperature used for Kalshi settlement.
    """
    date: str                      # YYYY-MM-DD format
    city_code: str                 # e.g., "NYC"
    settlement_high_f: float       # Official high temperature in Fahrenheit
    settlement_low_f: float        # Official low temperature in Fahrenheit
    source: str                    # Data source (e.g., "NWS Daily Climate Report")
    station_name: str              # Station name (e.g., "CENTRAL PARK NY")
    fetched_at: datetime           # When we fetched this data


def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return celsius * 9.0 / 5.0 + 32.0


def _parse_cli_date(text: str) -> Optional[str]:
    """
    Parse the date from a CLI product header.

    Looks for: "...THE CENTRAL PARK NY CLIMATE SUMMARY FOR JANUARY 26 2026..."
    Returns: "2026-01-26" or None
    """
    match = re.search(
        r"CLIMATE SUMMARY FOR\s+(\w+)\s+(\d{1,2})\s+(\d{4})",
        text,
        re.IGNORECASE
    )
    if not match:
        return None

    month_name, day, year = match.groups()
    try:
        dt = datetime.strptime(f"{month_name} {day} {year}", "%B %d %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _parse_cli_station(text: str) -> str:
    """
    Parse the station name from CLI product.

    Looks for: "...THE CENTRAL PARK NY CLIMATE SUMMARY..."
    Returns: "CENTRAL PARK NY" or "Unknown"
    """
    match = re.search(
        r"\.\.\.THE\s+(.+?)\s+CLIMATE SUMMARY",
        text,
        re.IGNORECASE
    )
    if match:
        return match.group(1).strip()
    return "Unknown"


def _is_preliminary_report(text: str) -> bool:
    """Check if this is a preliminary (mid-day) report vs final."""
    return "VALID TODAY AS OF" in text.upper()


def _parse_cli_temperatures(text: str) -> tuple[Optional[int], Optional[int]]:
    """
    Parse MAX and MIN temperatures from CLI product.

    The CLI format has a TEMPERATURE section like:
    TEMPERATURE (F)
     YESTERDAY
      MAXIMUM         27    316 PM  72    1950  39    -12       43
      MINIMUM         17   1159 PM   2    1871  27    -10       31

    Returns: (max_temp, min_temp) or (None, None) if parsing fails
    """
    # Find the TEMPERATURE section and look for MAXIMUM/MINIMUM in YESTERDAY block
    # The observed value is the first number after MAXIMUM/MINIMUM

    max_match = re.search(
        r"MAXIMUM\s+(-?\d+)\s+",
        text
    )
    min_match = re.search(
        r"MINIMUM\s+(-?\d+)\s+",
        text
    )

    max_temp = int(max_match.group(1)) if max_match else None
    min_temp = int(min_match.group(1)) if min_match else None

    return max_temp, min_temp


def _fetch_cli_products(city: CityConfig, limit: int = 20) -> list[str]:
    """
    Fetch recent CLI products from IEM archive.

    Args:
        city: CityConfig with city code
        limit: Number of products to fetch

    Returns:
        List of CLI product text strings
    """
    pil = CLI_PRODUCT_IDS.get(city.code)
    if not pil:
        logger.warning(f"No CLI product ID configured for {city.code}")
        return []

    try:
        response = requests.get(
            IEM_AFOS_URL,
            params={"pil": pil, "limit": limit},
            timeout=API_TIMEOUT,
            headers={"User-Agent": NWS_USER_AGENT},
        )
        response.raise_for_status()

        # IEM returns products separated by specific markers
        # Split on the product header pattern
        raw_text = response.text

        # Split products - each starts with a line like "571" followed by the WMO header
        products = re.split(r'\n\d{3}\s*\n', raw_text)

        return [p.strip() for p in products if "CLIMATE SUMMARY FOR" in p.upper()]

    except requests.exceptions.RequestException as e:
        logger.warning(f"Failed to fetch CLI products from IEM: {e}")
        return []


def _fetch_settlement_from_nws(
    date: str,
    city: CityConfig,
) -> Optional[SettlementRecord]:
    """
    Fetch settlement data from NWS Daily Climate Report.

    Args:
        date: Target date in YYYY-MM-DD format
        city: CityConfig object

    Returns:
        SettlementRecord if found, None otherwise
    """
    products = _fetch_cli_products(city)

    if not products:
        return None

    # Find the final (non-preliminary) report for the target date
    for product in products:
        # Skip preliminary reports
        if _is_preliminary_report(product):
            continue

        product_date = _parse_cli_date(product)
        if product_date != date:
            continue

        # Found the right product - parse temperatures
        max_temp, min_temp = _parse_cli_temperatures(product)

        if max_temp is None:
            logger.warning(f"Could not parse temperatures from CLI for {date}")
            continue

        station_name = _parse_cli_station(product)

        return SettlementRecord(
            date=date,
            city_code=city.code,
            settlement_high_f=float(max_temp),
            settlement_low_f=float(min_temp) if min_temp is not None else 0.0,
            source="NWS Daily Climate Report",
            station_name=station_name,
            fetched_at=datetime.now(),
        )

    return None


def _fetch_settlement_from_openmeteo(
    date: str,
    city: CityConfig,
) -> Optional[SettlementRecord]:
    """
    Fallback: Fetch settlement data from Open-Meteo Archive.

    Note: This uses model/interpolated data, not official station observations.
    The values may differ from Kalshi's settlement source.

    Args:
        date: Target date in YYYY-MM-DD format
        city: CityConfig object

    Returns:
        SettlementRecord if successful, None otherwise
    """
    try:
        params = {
            "latitude": city.lat,
            "longitude": city.lon,
            "start_date": date,
            "end_date": date,
            "daily": "temperature_2m_max,temperature_2m_min",
            "temperature_unit": "fahrenheit",
            "timezone": city.timezone,
        }

        response = requests.get(
            OPEN_METEO_ARCHIVE_URL,
            params=params,
            timeout=API_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()

        daily = data.get("daily", {})
        temps_max = daily.get("temperature_2m_max", [])
        temps_min = daily.get("temperature_2m_min", [])

        if not temps_max or temps_max[0] is None:
            logger.warning(f"No temperature data available for {date}")
            return None

        return SettlementRecord(
            date=date,
            city_code=city.code,
            settlement_high_f=round(temps_max[0], 1),
            settlement_low_f=round(temps_min[0], 1) if temps_min and temps_min[0] else 0.0,
            source="Open-Meteo Archive (fallback - may differ from settlement)",
            station_name=f"Grid point ({city.lat}, {city.lon})",
            fetched_at=datetime.now(),
        )

    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch historical data from Open-Meteo: {e}")
        return None
    except (ValueError, KeyError) as e:
        logger.error(f"Failed to parse Open-Meteo response: {e}")
        return None


def fetch_settlement(
    date: str,
    city: CityConfig = None,
    use_fallback: bool = True,
) -> Optional[SettlementRecord]:
    """
    Fetch historical settlement temperature for a given date.

    Primary source: NWS Daily Climate Report (same as Kalshi settlement)
    Fallback: Open-Meteo Archive (model data, may differ from actual settlement)

    Args:
        date: Target date in YYYY-MM-DD format
        city: CityConfig object (default: NYC)
        use_fallback: Whether to use Open-Meteo if NWS unavailable (default: True)

    Returns:
        SettlementRecord with the settlement high temperature, or None if unavailable
    """
    city = city or DEFAULT_CITY

    # Validate date is in the past
    target_date = datetime.strptime(date, "%Y-%m-%d").date()
    today = datetime.now().date()

    if target_date >= today:
        logger.warning(f"Date {date} is not in the past. Cannot fetch settlement.")
        return None

    # Try NWS Daily Climate Report first (authoritative source)
    record = _fetch_settlement_from_nws(date, city)

    if record:
        logger.info(f"Got settlement from NWS CLI: {record.settlement_high_f}°F for {date}")
        return record

    # Fallback to Open-Meteo if configured
    if use_fallback:
        logger.info(f"NWS CLI not available for {date}, falling back to Open-Meteo")
        return _fetch_settlement_from_openmeteo(date, city)

    return None


def fetch_settlement_range(
    start_date: str,
    end_date: str,
    city: CityConfig = None,
    use_fallback: bool = True,
) -> list[SettlementRecord]:
    """
    Fetch historical settlement temperatures for a date range.

    Primary source: NWS Daily Climate Report (same as Kalshi settlement)
    Fallback: Open-Meteo Archive for dates not found in NWS

    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        city: CityConfig object (default: NYC)
        use_fallback: Whether to use Open-Meteo for missing dates (default: True)

    Returns:
        List of SettlementRecords for each date in the range
    """
    city = city or DEFAULT_CITY

    # Validate dates
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    today = datetime.now().date()

    if end >= today:
        end = today - timedelta(days=1)

    if start >= today:
        logger.warning("Start date must be in the past")
        return []

    # Calculate how many days we need
    num_days = (end - start).days + 1
    # Fetch extra products to ensure we have enough
    limit = min(num_days * 2 + 10, 100)

    # Build set of needed dates
    needed_dates = set()
    current = start
    while current <= end:
        needed_dates.add(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    records = []
    found_dates = set()

    # Try NWS CLI first
    products = _fetch_cli_products(city, limit=limit)
    fetched_at = datetime.now()

    for product in products:
        # Skip preliminary reports
        if _is_preliminary_report(product):
            continue

        product_date = _parse_cli_date(product)
        if not product_date or product_date not in needed_dates:
            continue

        if product_date in found_dates:
            continue  # Already have this date

        max_temp, min_temp = _parse_cli_temperatures(product)
        if max_temp is None:
            continue

        station_name = _parse_cli_station(product)
        records.append(SettlementRecord(
            date=product_date,
            city_code=city.code,
            settlement_high_f=float(max_temp),
            settlement_low_f=float(min_temp) if min_temp is not None else 0.0,
            source="NWS Daily Climate Report",
            station_name=station_name,
            fetched_at=fetched_at,
        ))
        found_dates.add(product_date)

    # Fallback to Open-Meteo for missing dates
    missing_dates = needed_dates - found_dates
    if missing_dates and use_fallback:
        logger.info(f"Falling back to Open-Meteo for {len(missing_dates)} missing dates")
        for date in missing_dates:
            fallback_record = _fetch_settlement_from_openmeteo(date, city)
            if fallback_record:
                records.append(fallback_record)

    return records


def get_yesterday_settlement(city: CityConfig = None) -> Optional[SettlementRecord]:
    """
    Convenience function to get yesterday's settlement.

    Args:
        city: CityConfig object (default: NYC)

    Returns:
        SettlementRecord for yesterday, or None if unavailable
    """
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return fetch_settlement(yesterday, city)
