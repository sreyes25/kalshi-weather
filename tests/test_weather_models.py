"""
Tests for weather forecast fetcher module.

Uses the `responses` library to mock HTTP requests.
"""

import pytest
import responses
import numpy as np
from datetime import datetime

from kalshi_weather.data.weather import (
    OpenMeteoSource,
    NWSForecastSource,
    CombinedWeatherSource,
    fetch_all_forecasts,
)
from kalshi_weather.config import (
    NYC,
    OPEN_METEO_FORECAST_URL,
    OPEN_METEO_GFS_URL,
    OPEN_METEO_ENSEMBLE_URL,
    NWS_API_BASE,
)


# =============================================================================
# TEST DATA
# =============================================================================

TARGET_DATE = "2026-01-20"

OPEN_METEO_BEST_MATCH_RESPONSE = {
    "daily": {
        "time": ["2026-01-19", "2026-01-20", "2026-01-21"],
        "temperature_2m_max": [48.0, 52.0, 55.0],
    }
}

OPEN_METEO_GFS_RESPONSE = {
    "daily": {
        "time": ["2026-01-19", "2026-01-20", "2026-01-21"],
        "temperature_2m_max": [47.0, 51.0, 54.0],
    }
}

ENSEMBLE_TEMPS = [50.0, 51.0, 52.0, 53.0, 54.0, 55.0, 56.0, 57.0, 58.0, 59.0]


def make_ensemble_response(target_date: str, temps: list) -> dict:
    """Create an ensemble response with multiple members."""
    daily = {"time": ["2026-01-19", target_date, "2026-01-21"]}
    for i, temp in enumerate(temps):
        daily[f"temperature_2m_max_member{i:02d}"] = [temp - 2, temp, temp + 2]
    return {"daily": daily}


NWS_POINTS_RESPONSE = {
    "properties": {
        "forecast": "https://api.weather.gov/gridpoints/OKX/33,37/forecast"
    }
}

NWS_FORECAST_RESPONSE = {
    "properties": {
        "periods": [
            {"startTime": "2026-01-19T06:00:00-05:00", "isDaytime": True, "temperature": 45},
            {"startTime": "2026-01-19T18:00:00-05:00", "isDaytime": False, "temperature": 35},
            {"startTime": "2026-01-20T06:00:00-05:00", "isDaytime": True, "temperature": 50},
            {"startTime": "2026-01-20T18:00:00-05:00", "isDaytime": False, "temperature": 40},
        ]
    }
}


# =============================================================================
# OPEN-METEO SOURCE TESTS
# =============================================================================


class TestOpenMeteoSource:
    """Tests for OpenMeteoSource class."""

    @responses.activate
    def test_fetch_best_match_success(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=OPEN_METEO_BEST_MATCH_RESPONSE, status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_best_match(TARGET_DATE)
        assert forecast is not None
        assert forecast.source == "Open-Meteo Best Match"
        assert forecast.target_date == TARGET_DATE
        assert forecast.forecast_temp_f == 52.0

    @responses.activate
    def test_fetch_gfs_success(self):
        responses.add(responses.GET, OPEN_METEO_GFS_URL, json=OPEN_METEO_GFS_RESPONSE, status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_gfs(TARGET_DATE)
        assert forecast is not None
        assert forecast.source == "GFS+HRRR"
        assert forecast.forecast_temp_f == 51.0

    @responses.activate
    def test_fetch_ensemble_success(self):
        ensemble_response = make_ensemble_response(TARGET_DATE, ENSEMBLE_TEMPS)
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=ensemble_response, status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_ensemble(TARGET_DATE)
        assert forecast is not None
        assert forecast.source == "Open-Meteo Ensemble"
        expected_mean = np.mean(ENSEMBLE_TEMPS)
        assert abs(forecast.forecast_temp_f - expected_mean) < 0.01
        assert forecast.std_dev >= 1.5
        assert len(forecast.ensemble_members) == len(ENSEMBLE_TEMPS)

    @responses.activate
    def test_fetch_forecasts_all_sources(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=OPEN_METEO_BEST_MATCH_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_GFS_URL, json=OPEN_METEO_GFS_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, ENSEMBLE_TEMPS), status=200)
        source = OpenMeteoSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert len(forecasts) == 3

    @responses.activate
    def test_missing_target_date(self):
        response_without_date = {"daily": {"time": ["2026-01-19", "2026-01-21"], "temperature_2m_max": [48.0, 55.0]}}
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=response_without_date, status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_best_match(TARGET_DATE)
        assert forecast is None

    @responses.activate
    def test_api_error_500(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, status=500)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_best_match(TARGET_DATE)
        assert forecast is None

    @responses.activate
    def test_api_timeout(self):
        from requests.exceptions import Timeout
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, body=Timeout("Connection timed out"))
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_best_match(TARGET_DATE)
        assert forecast is None

    @responses.activate
    def test_null_temperature_value(self):
        response_with_null = {"daily": {"time": ["2026-01-19", "2026-01-20", "2026-01-21"], "temperature_2m_max": [48.0, None, 55.0]}}
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=response_with_null, status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_best_match(TARGET_DATE)
        assert forecast is None


# =============================================================================
# NWS FORECAST SOURCE TESTS
# =============================================================================


class TestNWSForecastSource:
    """Tests for NWSForecastSource class."""

    @responses.activate
    def test_fetch_forecast_success(self):
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], json=NWS_FORECAST_RESPONSE, status=200)
        source = NWSForecastSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert len(forecasts) == 1
        assert forecasts[0].source == "NWS"
        assert forecasts[0].forecast_temp_f == 50.0

    @responses.activate
    def test_missing_target_date(self):
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        forecast_without_date = {"properties": {"periods": [{"startTime": "2026-01-19T06:00:00-05:00", "isDaytime": True, "temperature": 45}]}}
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], json=forecast_without_date, status=200)
        source = NWSForecastSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert forecasts == []

    @responses.activate
    def test_points_api_error(self):
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", status=500)
        source = NWSForecastSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert forecasts == []

    @responses.activate
    def test_forecast_api_error(self):
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], status=500)
        source = NWSForecastSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert forecasts == []

    @responses.activate
    def test_api_timeout(self):
        from requests.exceptions import Timeout
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", body=Timeout("Connection timed out"))
        source = NWSForecastSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert forecasts == []

    @responses.activate
    def test_only_returns_daytime_forecast(self):
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        nighttime_only = {"properties": {"periods": [{"startTime": "2026-01-20T18:00:00-05:00", "isDaytime": False, "temperature": 40}]}}
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], json=nighttime_only, status=200)
        source = NWSForecastSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert forecasts == []


# =============================================================================
# COMBINED WEATHER SOURCE TESTS
# =============================================================================


class TestCombinedWeatherSource:
    """Tests for CombinedWeatherSource class."""

    @responses.activate
    def test_fetch_from_all_sources(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=OPEN_METEO_BEST_MATCH_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_GFS_URL, json=OPEN_METEO_GFS_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, ENSEMBLE_TEMPS), status=200)
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], json=NWS_FORECAST_RESPONSE, status=200)
        source = CombinedWeatherSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert len(forecasts) == 4

    @responses.activate
    def test_partial_failure_still_returns_results(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=OPEN_METEO_BEST_MATCH_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_GFS_URL, status=500)
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, status=500)
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", status=500)
        source = CombinedWeatherSource(NYC)
        forecasts = source.fetch_forecasts(TARGET_DATE)
        assert len(forecasts) >= 1


# =============================================================================
# CONVENIENCE FUNCTION TESTS
# =============================================================================


class TestFetchAllForecasts:
    """Tests for fetch_all_forecasts convenience function."""

    @responses.activate
    def test_fetch_all_forecasts_nyc(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=OPEN_METEO_BEST_MATCH_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_GFS_URL, json=OPEN_METEO_GFS_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, ENSEMBLE_TEMPS), status=200)
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], json=NWS_FORECAST_RESPONSE, status=200)
        forecasts = fetch_all_forecasts(TARGET_DATE, NYC)
        assert len(forecasts) == 4

    @responses.activate
    def test_fetch_all_forecasts_default_city(self):
        responses.add(responses.GET, OPEN_METEO_FORECAST_URL, json=OPEN_METEO_BEST_MATCH_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_GFS_URL, json=OPEN_METEO_GFS_RESPONSE, status=200)
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, ENSEMBLE_TEMPS), status=200)
        responses.add(responses.GET, f"{NWS_API_BASE}/points/{NYC.lat},{NYC.lon}", json=NWS_POINTS_RESPONSE, status=200)
        responses.add(responses.GET, NWS_POINTS_RESPONSE["properties"]["forecast"], json=NWS_FORECAST_RESPONSE, status=200)
        forecasts = fetch_all_forecasts(TARGET_DATE)
        assert len(forecasts) == 4


# =============================================================================
# ENSEMBLE STATISTICS TESTS
# =============================================================================


class TestEnsembleStatistics:
    """Tests for ensemble statistics calculation."""

    @responses.activate
    def test_ensemble_mean_calculation(self):
        temps = [50.0, 52.0, 54.0, 56.0, 58.0]
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, temps), status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_ensemble(TARGET_DATE)
        assert abs(forecast.forecast_temp_f - np.mean(temps)) < 0.01

    @responses.activate
    def test_ensemble_percentiles(self):
        temps = list(range(45, 65))
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, temps), status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_ensemble(TARGET_DATE)
        assert abs(forecast.low_f - np.percentile(temps, 10)) < 0.01
        assert abs(forecast.high_f - np.percentile(temps, 90)) < 0.01

    @responses.activate
    def test_min_std_dev_floor(self):
        temps = [55.0, 55.1, 55.2, 55.0, 55.1]
        responses.add(responses.GET, OPEN_METEO_ENSEMBLE_URL, json=make_ensemble_response(TARGET_DATE, temps), status=200)
        source = OpenMeteoSource(NYC)
        forecast = source._fetch_ensemble(TARGET_DATE)
        assert forecast.std_dev >= 1.5
