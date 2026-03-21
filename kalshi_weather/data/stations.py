"""
NWS Station Observation Parser for Kalshi Weather Bot.

Parses real-time NWS observations with conversion handling for
temperature uncertainty bounds.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Optional
from zoneinfo import ZoneInfo

import requests

from kalshi_weather.core import StationReading, DailyObservation, StationDataSource, StationType
from kalshi_weather.config import (
    CityConfig,
    DEFAULT_CITY,
    NWS_STATIONS_URL,
    NWS_USER_AGENT,
    API_TIMEOUT,
)

logger = logging.getLogger(__name__)

# Uncertainty constants
FIVE_MINUTE_C_PRECISION = 0.1
FIVE_MINUTE_F_UNCERTAINTY = 0.1
HOURLY_F_UNCERTAINTY = 0.5
INTER_READING_UNCERTAINTY = 1.0


def celsius_to_fahrenheit(celsius: float) -> float:
    """Convert Celsius to Fahrenheit."""
    return celsius * 9.0 / 5.0 + 32.0


def calculate_temp_bounds(
    temp_c: Optional[float],
    temp_f: float,
    station_type: StationType,
) -> tuple[float, float]:
    """Calculate possible actual temperature bounds given conversion uncertainty."""
    if station_type == StationType.FIVE_MINUTE:
        uncertainty = FIVE_MINUTE_F_UNCERTAINTY + 0.5
    elif station_type == StationType.HOURLY:
        uncertainty = HOURLY_F_UNCERTAINTY
    else:
        uncertainty = 1.0

    return (temp_f - uncertainty, temp_f + uncertainty)


def determine_station_type(observations: List[dict]) -> StationType:
    """Determine station type based on observation frequency."""
    if len(observations) < 2:
        return StationType.UNKNOWN

    intervals = []
    for i in range(len(observations) - 1):
        try:
            time1 = observations[i].get("properties", {}).get("timestamp")
            time2 = observations[i + 1].get("properties", {}).get("timestamp")

            if time1 and time2:
                dt1 = datetime.fromisoformat(time1.replace("Z", "+00:00"))
                dt2 = datetime.fromisoformat(time2.replace("Z", "+00:00"))
                interval_minutes = abs((dt1 - dt2).total_seconds() / 60)
                intervals.append(interval_minutes)
        except (ValueError, TypeError):
            continue

    if not intervals:
        return StationType.UNKNOWN

    avg_interval = sum(intervals) / len(intervals)

    if avg_interval < 15:
        return StationType.FIVE_MINUTE
    elif avg_interval >= 45:
        return StationType.HOURLY
    else:
        return StationType.UNKNOWN


def parse_observation(obs: dict, station_type: StationType, station_id: str) -> Optional[StationReading]:
    """Parse a single observation from NWS API response."""
    try:
        properties = obs.get("properties", {})

        timestamp_str = properties.get("timestamp")
        if not timestamp_str:
            return None
        timestamp = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))

        temp_data = properties.get("temperature", {})
        temp_value = temp_data.get("value")
        unit_code = temp_data.get("unitCode", "")
        dewpoint_data = properties.get("dewpoint", {})
        humidity_data = properties.get("relativeHumidity", {})

        if temp_value is None:
            return None

        if "degC" in unit_code or "celsius" in unit_code.lower():
            temp_c = float(temp_value)
            temp_f = celsius_to_fahrenheit(temp_c)
        elif "degF" in unit_code or "fahrenheit" in unit_code.lower():
            temp_f = float(temp_value)
            temp_c = None
        else:
            temp_c = float(temp_value)
            temp_f = celsius_to_fahrenheit(temp_c)

        low_f, high_f = calculate_temp_bounds(temp_c, temp_f, station_type)

        dewpoint_f: Optional[float] = None
        dewpoint_value = dewpoint_data.get("value")
        dewpoint_unit = str(dewpoint_data.get("unitCode", "")).lower()
        if dewpoint_value is not None:
            try:
                dew_raw = float(dewpoint_value)
                if "degf" in dewpoint_unit or "fahrenheit" in dewpoint_unit:
                    dewpoint_f = round(dew_raw, 1)
                else:
                    dewpoint_f = round(celsius_to_fahrenheit(dew_raw), 1)
            except (TypeError, ValueError):
                dewpoint_f = None

        relative_humidity_pct: Optional[float] = None
        humidity_value = humidity_data.get("value")
        if humidity_value is not None:
            try:
                relative_humidity_pct = round(float(humidity_value), 1)
            except (TypeError, ValueError):
                relative_humidity_pct = None

        return StationReading(
            station_id=station_id,
            timestamp=timestamp,
            station_type=station_type,
            reported_temp_f=round(temp_f, 1),
            reported_temp_c=round(temp_c, 1) if temp_c is not None else None,
            possible_actual_f_low=round(low_f, 1),
            possible_actual_f_high=round(high_f, 1),
            dewpoint_f=dewpoint_f,
            relative_humidity_pct=relative_humidity_pct,
        )
    except (ValueError, TypeError, KeyError) as e:
        logger.warning(f"Failed to parse observation: {e}")
        return None


def get_recent_observation_history(
    readings: List[StationReading],
    timezone: ZoneInfo,
    current_time: Optional[datetime] = None,
    window_size: int = 6,
    max_age_hours: float = 4.0,
) -> List[StationReading]:
    """
    Extract a recent in-day observation window for trajectory trend analysis.

    Handles sparse histories by returning whatever valid points are available.
    """
    if not readings:
        return []

    now_local = current_time.astimezone(timezone) if current_time else datetime.now(timezone)
    today = now_local.date()

    # Keep only today's observations up to current time.
    same_day = [
        r for r in readings
        if r.timestamp.astimezone(timezone).date() == today
        and r.timestamp.astimezone(timezone) <= now_local
    ]

    if not same_day:
        # Fallback to latest available day when today's readings are missing.
        sorted_all = sorted(readings, key=lambda r: r.timestamp)
        latest_day = sorted_all[-1].timestamp.astimezone(timezone).date()
        same_day = [r for r in sorted_all if r.timestamp.astimezone(timezone).date() == latest_day]

    if not same_day:
        return []

    same_day.sort(key=lambda r: r.timestamp)
    age_cutoff = now_local.timestamp() - max_age_hours * 3600.0
    fresh = [r for r in same_day if r.timestamp.astimezone(timezone).timestamp() >= age_cutoff]

    series = fresh if fresh else same_day
    return series[-max(2, window_size):]


class NWSStationParser(StationDataSource):
    """Fetches and parses NWS station observations."""

    def __init__(self, city: CityConfig = None):
        """
        Initialize with city configuration.

        Args:
            city: CityConfig object (default: NYC)
        """
        city = city or DEFAULT_CITY
        self.station_id = city.station_id
        self.timezone = ZoneInfo(city.timezone)
        self._cached_observations: List[dict] = []
        self._station_type: Optional[StationType] = None
        self._last_fetch: Optional[datetime] = None

    def _get_headers(self) -> dict:
        """Return headers for NWS API requests."""
        return {"User-Agent": NWS_USER_AGENT}

    def _to_nws_rfc3339(self, dt: datetime) -> str:
        """
        Format datetimes for api.weather.gov query params.

        NWS is strict here; avoid fractional seconds to prevent 400 responses.
        """
        return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

    def _parse_obs_timestamp(self, obs: dict) -> Optional[datetime]:
        timestamp = obs.get("properties", {}).get("timestamp")
        if not timestamp:
            return None
        try:
            return datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        except ValueError:
            return None

    def _fetch_latest_observation(self) -> Optional[dict]:
        """Fetch latest single observation and return as feature-like dict."""
        try:
            url = f"{NWS_STATIONS_URL.format(station_id=self.station_id)}/latest"
            response = requests.get(
                url,
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, dict) and "properties" in data:
                return data
            return None
        except requests.exceptions.RequestException:
            return None
        except (ValueError, KeyError):
            return None

    def _fetch_raw_observations(
        self,
        limit: int = 500,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
    ) -> List[dict]:
        """Fetch raw observations from NWS API."""
        try:
            url = NWS_STATIONS_URL.format(station_id=self.station_id)
            params = {"limit": limit}
            if start is not None:
                params["start"] = self._to_nws_rfc3339(start)
            if end is not None:
                params["end"] = self._to_nws_rfc3339(end)

            response = requests.get(
                url,
                params=params,
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            features = data.get("features", [])
            # If day-window query returns empty, retry once without window
            # so we still have usable observations for intraday display.
            if not features and (start is not None or end is not None):
                retry_response = requests.get(
                    url,
                    params={"limit": limit},
                    headers=self._get_headers(),
                    timeout=API_TIMEOUT,
                )
                retry_response.raise_for_status()
                retry_data = retry_response.json()
                features = retry_data.get("features", [])
            latest = self._fetch_latest_observation()
            if latest:
                latest_ts = self._parse_obs_timestamp(latest)
                existing_timestamps = {
                    self._parse_obs_timestamp(f) for f in features if isinstance(f, dict)
                }
                if latest_ts is not None and latest_ts not in existing_timestamps:
                    features.append(latest)

            features = [f for f in features if isinstance(f, dict)]
            features.sort(
                key=lambda f: self._parse_obs_timestamp(f) or datetime.min.replace(tzinfo=ZoneInfo("UTC")),
                reverse=True,
            )
            self._cached_observations = features
            self._last_fetch = datetime.now(self.timezone)

            if features:
                self._station_type = determine_station_type(features)

            return features
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch NWS observations: {e}")
            if self._cached_observations:
                logger.warning(
                    "Using cached NWS observations (%d rows) due to fetch failure.",
                    len(self._cached_observations),
                )
                return list(self._cached_observations)
            return []
        except (ValueError, KeyError) as e:
            logger.warning(f"Failed to parse NWS observations response: {e}")
            if self._cached_observations:
                logger.warning(
                    "Using cached NWS observations (%d rows) due to parse failure.",
                    len(self._cached_observations),
                )
                return list(self._cached_observations)
            return []

    def fetch_current_observations(self) -> List[StationReading]:
        """Fetch recent observations from the station."""
        raw_observations = self._fetch_raw_observations()

        if not raw_observations:
            return []

        station_type = self._station_type or StationType.UNKNOWN
        readings = []

        for obs in raw_observations:
            reading = parse_observation(obs, station_type, self.station_id)
            if reading:
                readings.append(reading)

        return readings

    def get_daily_summary(self, date: str) -> Optional[DailyObservation]:
        """Get aggregated observation data for a specific date."""
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        day_start_local = datetime.combine(target_date, datetime.min.time(), self.timezone)
        day_end_local = day_start_local + timedelta(days=1)
        now_local = datetime.now(self.timezone)
        fetch_end = min(day_end_local, now_local + timedelta(minutes=5))

        raw_observations = self._fetch_raw_observations(
            limit=500,
            start=day_start_local,
            end=fetch_end,
        )
        if not raw_observations:
            return None
        station_type = self._station_type or StationType.UNKNOWN
        readings: List[StationReading] = []
        for obs in raw_observations:
            reading = parse_observation(obs, station_type, self.station_id)
            if reading:
                readings.append(reading)

        if not readings:
            return None

        daily_readings = [
            r for r in readings
            if r.timestamp.astimezone(self.timezone).date() == target_date
        ]
        daily_readings.sort(key=lambda r: r.timestamp)

        if not daily_readings:
            return None

        max_reading = max(daily_readings, key=lambda r: r.reported_temp_f)
        observed_high_f = max_reading.reported_temp_f

        max_possible_high = max(r.possible_actual_f_high for r in daily_readings)
        possible_actual_high_high = max_possible_high + INTER_READING_UNCERTAINTY
        possible_actual_high_low = observed_high_f - HOURLY_F_UNCERTAINTY

        return DailyObservation(
            station_id=self.station_id,
            date=date,
            observed_high_f=observed_high_f,
            possible_actual_high_low=round(possible_actual_high_low, 1),
            possible_actual_high_high=round(possible_actual_high_high, 1),
            readings=daily_readings,
            last_updated=datetime.now(self.timezone),
        )

    def get_station_type(self) -> StationType:
        """Get the determined station type."""
        return self._station_type or StationType.UNKNOWN

    def get_recent_history(
        self,
        current_time: Optional[datetime] = None,
        window_size: int = 6,
        max_age_hours: float = 4.0,
    ) -> List[StationReading]:
        """Fetch and return recent observations for intraday trend analysis."""
        readings = self.fetch_current_observations()
        return get_recent_observation_history(
            readings=readings,
            timezone=self.timezone,
            current_time=current_time,
            window_size=window_size,
            max_age_hours=max_age_hours,
        )


def get_station_observations(city: CityConfig = None) -> List[StationReading]:
    """Convenience function to fetch current observations for a city."""
    parser = NWSStationParser(city)
    return parser.fetch_current_observations()


def get_daily_observation(date: str, city: CityConfig = None) -> Optional[DailyObservation]:
    """Convenience function to get daily observation summary."""
    parser = NWSStationParser(city)
    return parser.get_daily_summary(date)
