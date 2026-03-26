"""
Weather forecast fetcher for Kalshi Weather Bot.

Fetches temperature forecasts from multiple sources:
- Open-Meteo best match model
- Open-Meteo GFS+HRRR blend
- Open-Meteo ensemble (for uncertainty estimates)
- NWS API point forecast
- AccuWeather daily forecast (optional, API key required)
"""

import logging
from datetime import datetime
from typing import List, Optional
import xml.etree.ElementTree as ET

import numpy as np
import requests

from kalshi_weather.core import TemperatureForecast, WeatherModelSource
from kalshi_weather.config import (
    CityConfig,
    DEFAULT_CITY,
    OPEN_METEO_FORECAST_URL,
    OPEN_METEO_GFS_URL,
    OPEN_METEO_ENSEMBLE_URL,
    NWS_API_BASE,
    API_TIMEOUT,
    NWS_USER_AGENT,
    DEFAULT_STD_DEV,
    MIN_STD_DEV,
    OPEN_METEO_EXTRA_MODELS,
    OPEN_METEO_EXTRA_MODEL_STD_DEV,
    ACCUWEATHER_API_BASE,
    ACCUWEATHER_API_KEY,
    ACCUWEATHER_LOCATION_KEY,
    ACCUWEATHER_ENABLED,
    ACCUWEATHER_FORECAST_DAYS,
    ACCUWEATHER_LANGUAGE,
    ACCUWEATHER_STD_DEV,
)

logger = logging.getLogger(__name__)


class OpenMeteoSource(WeatherModelSource):
    """Fetches forecasts from 3 Open-Meteo endpoints."""

    def __init__(self, city: CityConfig = None):
        """
        Initialize with city configuration.

        Args:
            city: CityConfig object (default: NYC)
        """
        city = city or DEFAULT_CITY
        self.lat = city.lat
        self.lon = city.lon
        self.timezone = city.timezone
        self._latest_model_run_time: Optional[datetime] = None
        self._extra_models = OPEN_METEO_EXTRA_MODELS

    @staticmethod
    def _extra_model_source_name(model_id: str) -> str:
        normalized = model_id.strip().lower()
        mapping = {
            "ecmwf_ifs025": "ECMWF",
            "icon_seamless": "ICON",
            "ukmo_global_deterministic_10km": "UKMO",
            "gfs_graphcast025": "GraphCast",
            "gem_global": "GEM",
        }
        return mapping.get(normalized, f"Open-Meteo {model_id}")

    def _base_params(self) -> dict:
        """Return base parameters for Open-Meteo requests."""
        return {
            "latitude": self.lat,
            "longitude": self.lon,
            "daily": "temperature_2m_max",
            "temperature_unit": "fahrenheit",
            "timezone": self.timezone,
            "forecast_days": 14,
        }

    def _fetch_best_match(self, target_date: str) -> Optional[TemperatureForecast]:
        """Fetch from the best match endpoint."""
        try:
            response = requests.get(
                OPEN_METEO_FORECAST_URL,
                params=self._base_params(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})
            times = daily.get("time", [])
            temps = daily.get("temperature_2m_max", [])

            if target_date not in times:
                logger.warning(f"Target date {target_date} not in Open-Meteo best match response")
                return None

            idx = times.index(target_date)
            temp = temps[idx]

            if temp is None:
                return None

            return TemperatureForecast(
                source="Open-Meteo Best Match",
                target_date=target_date,
                forecast_temp_f=temp,
                low_f=temp - DEFAULT_STD_DEV,
                high_f=temp + DEFAULT_STD_DEV,
                std_dev=DEFAULT_STD_DEV,
                model_run_time=None,
                fetched_at=datetime.now(),
                ensemble_members=[],
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch Open-Meteo best match: {e}")
            return None
        except (KeyError, ValueError, IndexError) as e:
            logger.warning(f"Failed to parse Open-Meteo best match response: {e}")
            return None

    def _fetch_gfs(self, target_date: str) -> Optional[TemperatureForecast]:
        """Fetch from the GFS endpoint."""
        try:
            params = self._base_params()
            params["models"] = "gfs_seamless"

            response = requests.get(
                OPEN_METEO_GFS_URL,
                params=params,
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})
            times = daily.get("time", [])
            temps = daily.get("temperature_2m_max", [])

            if target_date not in times:
                logger.warning(f"Target date {target_date} not in Open-Meteo GFS response")
                return None

            idx = times.index(target_date)
            temp = temps[idx]

            if temp is None:
                return None

            return TemperatureForecast(
                source="GFS+HRRR",
                target_date=target_date,
                forecast_temp_f=temp,
                low_f=temp - DEFAULT_STD_DEV,
                high_f=temp + DEFAULT_STD_DEV,
                std_dev=DEFAULT_STD_DEV,
                model_run_time=None,
                fetched_at=datetime.now(),
                ensemble_members=[],
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch Open-Meteo GFS: {e}")
            return None
        except (KeyError, ValueError, IndexError) as e:
            logger.warning(f"Failed to parse Open-Meteo GFS response: {e}")
            return None

    def _fetch_ensemble(self, target_date: str) -> Optional[TemperatureForecast]:
        """Fetch from the ensemble endpoint and calculate statistics."""
        try:
            params = self._base_params()
            # Open-Meteo ensemble API now expects standard daily variables and
            # a selected ensemble model; member-specific fields are returned in
            # the response (temperature_2m_max_memberXX).
            params["models"] = "ecmwf_ifs025_ensemble"
            params["daily"] = "temperature_2m_max"

            response = requests.get(
                OPEN_METEO_ENSEMBLE_URL,
                params=params,
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})
            times = daily.get("time", [])

            if target_date not in times:
                logger.warning(f"Target date {target_date} not in Open-Meteo ensemble response")
                return None

            idx = times.index(target_date)

            ensemble_temps = []
            for key, values in daily.items():
                if key.startswith("temperature_2m_max_member") and values:
                    if idx < len(values) and values[idx] is not None:
                        ensemble_temps.append(float(values[idx]))

            # Fallback for responses where only aggregate daily max is present.
            if not ensemble_temps:
                daily_max = daily.get("temperature_2m_max", [])
                if idx < len(daily_max) and daily_max[idx] is not None:
                    ensemble_temps.append(float(daily_max[idx]))

            if not ensemble_temps:
                logger.warning("No ensemble members found in response")
                return None

            temps_array = np.array(ensemble_temps, dtype=float)
            mean_temp = float(np.mean(temps_array))
            std_dev = float(np.std(temps_array))
            low_f = float(np.percentile(temps_array, 10))
            high_f = float(np.percentile(temps_array, 90))

            std_dev = max(std_dev, MIN_STD_DEV)

            return TemperatureForecast(
                source="Open-Meteo Ensemble",
                target_date=target_date,
                forecast_temp_f=mean_temp,
                low_f=low_f,
                high_f=high_f,
                std_dev=std_dev,
                model_run_time=None,
                fetched_at=datetime.now(),
                ensemble_members=[float(x) for x in ensemble_temps],
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch Open-Meteo ensemble: {e}")
            return None
        except (KeyError, ValueError, IndexError, TypeError) as e:
            logger.warning(f"Failed to parse Open-Meteo ensemble response: {e}")
            return None

    def _fetch_extra_model(self, target_date: str, model_id: str) -> Optional[TemperatureForecast]:
        """Fetch a specific Open-Meteo model id from forecast endpoint."""
        try:
            params = self._base_params()
            params["models"] = model_id
            response = requests.get(
                OPEN_METEO_FORECAST_URL,
                params=params,
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            daily = data.get("daily", {})
            times = daily.get("time", [])
            temps = daily.get("temperature_2m_max", [])
            if target_date not in times:
                return None
            idx = times.index(target_date)
            temp = temps[idx]
            if temp is None:
                return None
            std_dev = max(MIN_STD_DEV, OPEN_METEO_EXTRA_MODEL_STD_DEV)
            source_name = self._extra_model_source_name(model_id)
            return TemperatureForecast(
                source=source_name,
                target_date=target_date,
                forecast_temp_f=float(temp),
                low_f=float(temp) - std_dev,
                high_f=float(temp) + std_dev,
                std_dev=std_dev,
                model_run_time=None,
                fetched_at=datetime.now(),
                ensemble_members=[],
            )
        except requests.exceptions.RequestException:
            return None
        except (KeyError, ValueError, IndexError, TypeError):
            return None

    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """Fetch all available forecasts for a target date."""
        forecasts = []

        best_match = self._fetch_best_match(target_date)
        if best_match:
            forecasts.append(best_match)

        gfs = self._fetch_gfs(target_date)
        if gfs:
            forecasts.append(gfs)

        ensemble = self._fetch_ensemble(target_date)
        if ensemble:
            forecasts.append(ensemble)

        for model_id in self._extra_models:
            extra = self._fetch_extra_model(target_date, model_id)
            if extra:
                forecasts.append(extra)

        return forecasts

    def get_latest_model_run_time(self) -> Optional[datetime]:
        """Get timestamp of most recent model run fetched."""
        return self._latest_model_run_time


class NWSForecastSource(WeatherModelSource):
    """Fetches forecasts from NWS API."""

    def __init__(self, city: CityConfig = None):
        """
        Initialize with city configuration.

        Args:
            city: CityConfig object (default: NYC)
        """
        city = city or DEFAULT_CITY
        self.lat = city.lat
        self.lon = city.lon
        self._latest_model_run_time: Optional[datetime] = None
        self._forecast_url: Optional[str] = None

    def _get_headers(self) -> dict:
        """Return headers for NWS API requests."""
        return {"User-Agent": NWS_USER_AGENT}

    def _mapclick_dwml_url(self) -> str:
        """
        Return weather.gov MapClick DWML URL for this point.

        This feed currently reflects the same "High near X" values rendered on
        the public MapClick page and can update ahead of api.weather.gov
        gridpoint forecast periods.
        """
        return (
            "https://forecast.weather.gov/MapClick.php"
            f"?lat={self.lat}&lon={self.lon}&unit=0&lg=english&FcstType=dwml"
        )

    def _fetch_mapclick_daily_high(self, target_date: str) -> Optional[float]:
        """
        Fetch daily high from MapClick DWML for a YYYY-MM-DD date.
        """
        try:
            response = requests.get(
                self._mapclick_dwml_url(),
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            root = ET.fromstring(response.text)

            time_layouts: dict[str, list[str]] = {}
            for layout in root.findall(".//time-layout"):
                key = (layout.findtext("layout-key") or "").strip()
                if not key:
                    continue
                starts = []
                for node in layout.findall("start-valid-time"):
                    value = (node.text or "").strip()
                    if value:
                        starts.append(value)
                if starts:
                    time_layouts[key] = starts

            for temp_node in root.findall(".//temperature"):
                if (temp_node.get("type") or "").lower() != "maximum":
                    continue
                layout_key = (temp_node.get("time-layout") or "").strip()
                starts = time_layouts.get(layout_key, [])
                if not starts:
                    continue

                values = temp_node.findall("value")
                for start, value_node in zip(starts, values):
                    if not start.startswith(target_date):
                        continue
                    nil_attr = value_node.attrib.get(
                        "{http://www.w3.org/2001/XMLSchema-instance}nil",
                        "",
                    ).strip().lower()
                    text = (value_node.text or "").strip()
                    if nil_attr == "true" or not text:
                        continue
                    try:
                        return float(text)
                    except ValueError:
                        continue
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch NWS MapClick DWML forecast: {e}")
        except ET.ParseError as e:
            logger.warning(f"Failed to parse NWS MapClick DWML forecast: {e}")
        return None

    def _get_forecast_url(self) -> Optional[str]:
        """Get the forecast URL from the points endpoint."""
        if self._forecast_url:
            return self._forecast_url

        try:
            points_url = f"{NWS_API_BASE}/points/{self.lat},{self.lon}"
            response = requests.get(
                points_url,
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            self._forecast_url = data.get("properties", {}).get("forecast")
            return self._forecast_url
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to get NWS forecast URL: {e}")
            return None
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse NWS points response: {e}")
            return None

    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """Fetch all available forecasts for a target date."""
        mapclick_high = self._fetch_mapclick_daily_high(target_date)
        if mapclick_high is not None:
            return [
                TemperatureForecast(
                    source="NWS",
                    target_date=target_date,
                    forecast_temp_f=float(mapclick_high),
                    low_f=float(mapclick_high) - DEFAULT_STD_DEV,
                    high_f=float(mapclick_high) + DEFAULT_STD_DEV,
                    std_dev=DEFAULT_STD_DEV,
                    model_run_time=None,
                    fetched_at=datetime.now(),
                    ensemble_members=[],
                )
            ]

        forecast_url = self._get_forecast_url()
        if not forecast_url:
            return []

        try:
            response = requests.get(
                forecast_url,
                headers=self._get_headers(),
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            data = response.json()

            periods = data.get("properties", {}).get("periods", [])

            for period in periods:
                start_time = period.get("startTime", "")
                is_daytime = period.get("isDaytime", False)

                if start_time.startswith(target_date) and is_daytime:
                    temp = period.get("temperature")
                    if temp is None:
                        continue

                    return [
                        TemperatureForecast(
                            source="NWS",
                            target_date=target_date,
                            forecast_temp_f=float(temp),
                            low_f=float(temp) - DEFAULT_STD_DEV,
                            high_f=float(temp) + DEFAULT_STD_DEV,
                            std_dev=DEFAULT_STD_DEV,
                            model_run_time=None,
                            fetched_at=datetime.now(),
                            ensemble_members=[],
                        )
                    ]

            logger.warning(f"Target date {target_date} not found in NWS forecast")
            return []
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch NWS forecast: {e}")
            return []
        except (KeyError, ValueError) as e:
            logger.warning(f"Failed to parse NWS forecast response: {e}")
            return []

    def get_latest_model_run_time(self) -> Optional[datetime]:
        """Get timestamp of most recent model run fetched."""
        return self._latest_model_run_time


class AccuWeatherSource(WeatherModelSource):
    """Fetches forecasts from official AccuWeather API (optional)."""

    def __init__(self, city: CityConfig = None):
        city = city or DEFAULT_CITY
        self.lat = city.lat
        self.lon = city.lon
        self._latest_model_run_time: Optional[datetime] = None
        self._api_key = ACCUWEATHER_API_KEY
        self._base_url = ACCUWEATHER_API_BASE.rstrip("/")
        self._location_key = ACCUWEATHER_LOCATION_KEY
        self._enabled = bool(ACCUWEATHER_ENABLED and self._api_key)

    def _resolve_location_key(self) -> Optional[str]:
        if self._location_key:
            return self._location_key
        if not self._enabled:
            return None
        try:
            response = requests.get(
                f"{self._base_url}/locations/v1/cities/geoposition/search",
                params={
                    "apikey": self._api_key,
                    "q": f"{self.lat},{self.lon}",
                    "language": ACCUWEATHER_LANGUAGE,
                },
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            key = str(payload.get("Key") or "").strip()
            if key:
                self._location_key = key
                return key
        except requests.exceptions.RequestException as exc:
            logger.warning("Failed to resolve AccuWeather location key: %s", exc)
        except (TypeError, ValueError, KeyError) as exc:
            logger.warning("Failed to parse AccuWeather location key response: %s", exc)
        return None

    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """Fetch the daily high forecast for target date."""
        if not self._enabled:
            return []
        location_key = self._resolve_location_key()
        if not location_key:
            return []

        days = ACCUWEATHER_FORECAST_DAYS
        if days not in {1, 5, 10, 15}:
            days = 5

        try:
            response = requests.get(
                f"{self._base_url}/forecasts/v1/daily/{days}day/{location_key}",
                params={
                    "apikey": self._api_key,
                    "language": ACCUWEATHER_LANGUAGE,
                    "metric": "false",
                    "details": "true",
                },
                timeout=API_TIMEOUT,
            )
            response.raise_for_status()
            payload = response.json()
            daily_items = payload.get("DailyForecasts", []) or []
            for item in daily_items:
                date_iso = str(item.get("Date") or "")
                if not date_iso.startswith(target_date):
                    continue
                maximum = (
                    item.get("Temperature", {})
                    .get("Maximum", {})
                    .get("Value")
                )
                if maximum is None:
                    continue
                temp_f = float(maximum)
                std_dev = max(MIN_STD_DEV, float(ACCUWEATHER_STD_DEV))
                return [
                    TemperatureForecast(
                        source="AccuWeather",
                        target_date=target_date,
                        forecast_temp_f=temp_f,
                        low_f=temp_f - std_dev,
                        high_f=temp_f + std_dev,
                        std_dev=std_dev,
                        model_run_time=None,
                        fetched_at=datetime.now(),
                        ensemble_members=[],
                    )
                ]
        except requests.exceptions.RequestException as exc:
            logger.warning("Failed to fetch AccuWeather forecast: %s", exc)
        except (TypeError, ValueError, KeyError) as exc:
            logger.warning("Failed to parse AccuWeather forecast response: %s", exc)
        return []

    def get_latest_model_run_time(self) -> Optional[datetime]:
        return self._latest_model_run_time


class CombinedWeatherSource(WeatherModelSource):
    """Combines all weather sources into a single interface."""

    def __init__(self, city: CityConfig = None):
        """
        Initialize with city configuration.

        Args:
            city: CityConfig object (default: NYC)
        """
        self.city = city or DEFAULT_CITY
        self.open_meteo = OpenMeteoSource(self.city)
        self.nws = NWSForecastSource(self.city)
        self.accuweather = AccuWeatherSource(self.city)
        self._latest_model_run_time: Optional[datetime] = None

    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """Fetch all available forecasts from all sources for a target date."""
        forecasts = []
        forecasts.extend(self.open_meteo.fetch_forecasts(target_date))
        forecasts.extend(self.nws.fetch_forecasts(target_date))
        forecasts.extend(self.accuweather.fetch_forecasts(target_date))
        return forecasts

    def get_latest_model_run_time(self) -> Optional[datetime]:
        """Get timestamp of most recent model run fetched."""
        return self._latest_model_run_time


def fetch_all_forecasts(target_date: str, city: CityConfig = None) -> List[TemperatureForecast]:
    """
    Convenience function to fetch all forecasts for a target date.

    Args:
        target_date: Date in YYYY-MM-DD format
        city: CityConfig object (default: NYC)

    Returns:
        List of TemperatureForecast objects from all sources
    """
    source = CombinedWeatherSource(city)
    return source.fetch_forecasts(target_date)
