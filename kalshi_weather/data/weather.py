"""
Weather forecast fetcher for Kalshi Weather Bot.

Fetches temperature forecasts from multiple sources:
- Open-Meteo best match model
- Open-Meteo GFS+HRRR blend
- Open-Meteo ensemble (for uncertainty estimates)
- NWS API point forecast
"""

import logging
from datetime import datetime
from typing import List, Optional

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
            params["daily"] = ",".join([f"temperature_2m_max_member{i:02d}" for i in range(51)])

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
                        ensemble_temps.append(values[idx])

            if not ensemble_temps:
                logger.warning("No ensemble members found in response")
                return None

            temps_array = np.array(ensemble_temps)
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
                ensemble_members=ensemble_temps,
            )
        except requests.exceptions.RequestException as e:
            logger.warning(f"Failed to fetch Open-Meteo ensemble: {e}")
            return None
        except (KeyError, ValueError, IndexError) as e:
            logger.warning(f"Failed to parse Open-Meteo ensemble response: {e}")
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
        self._latest_model_run_time: Optional[datetime] = None

    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """Fetch all available forecasts from all sources for a target date."""
        forecasts = []
        forecasts.extend(self.open_meteo.fetch_forecasts(target_date))
        forecasts.extend(self.nws.fetch_forecasts(target_date))
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
