"""
Tests for NWS station observation parser module.

Uses the `responses` library to mock HTTP requests.
"""

import pytest
import responses
from datetime import datetime
from zoneinfo import ZoneInfo

from kalshi_weather.data.stations import (
    NWSStationParser,
    get_station_observations,
    get_daily_observation,
    celsius_to_fahrenheit,
    calculate_temp_bounds,
    determine_station_type,
    parse_observation,
    get_recent_observation_history,
    FIVE_MINUTE_F_UNCERTAINTY,
    HOURLY_F_UNCERTAINTY,
    INTER_READING_UNCERTAINTY,
)
from kalshi_weather.core import StationType
from kalshi_weather.config import NWS_STATIONS_URL, NYC


# =============================================================================
# TEST DATA
# =============================================================================

STATION_ID = "KNYC"
TARGET_DATE = "2026-01-20"
BASE_URL = NWS_STATIONS_URL.format(station_id=STATION_ID)
NYC_TZ = ZoneInfo("America/New_York")


def make_observation(timestamp: str, temp_c: float, unit_code: str = "wmoUnit:degC") -> dict:
    return {"type": "Feature", "properties": {"timestamp": timestamp, "temperature": {"value": temp_c, "unitCode": unit_code}}}


def make_api_response(observations: list) -> dict:
    return {"@context": [], "type": "FeatureCollection", "features": observations}


FIVE_MINUTE_OBSERVATIONS = [
    make_observation("2026-01-20T17:00:00+00:00", 11.1),
    make_observation("2026-01-20T16:55:00+00:00", 11.7),
    make_observation("2026-01-20T16:50:00+00:00", 12.2),
    make_observation("2026-01-20T16:45:00+00:00", 11.9),
    make_observation("2026-01-20T16:40:00+00:00", 11.5),
]

HOURLY_OBSERVATIONS = [
    make_observation("2026-01-20T17:00:00+00:00", 11.1),
    make_observation("2026-01-20T16:00:00+00:00", 12.2),
    make_observation("2026-01-20T15:00:00+00:00", 10.0),
    make_observation("2026-01-20T14:00:00+00:00", 8.9),
]


# =============================================================================
# UNIT CONVERSION TESTS
# =============================================================================


class TestCelsiusToFahrenheit:
    def test_freezing_point(self):
        assert celsius_to_fahrenheit(0) == 32.0

    def test_boiling_point(self):
        assert celsius_to_fahrenheit(100) == 212.0

    def test_typical_temperature(self):
        assert abs(celsius_to_fahrenheit(20) - 68.0) < 0.01

    def test_negative_temperature(self):
        assert abs(celsius_to_fahrenheit(-10) - 14.0) < 0.01

    def test_decimal_precision(self):
        result = celsius_to_fahrenheit(12.5)
        expected = 12.5 * 9 / 5 + 32
        assert abs(result - expected) < 0.001


# =============================================================================
# TEMPERATURE BOUNDS TESTS
# =============================================================================


class TestCalculateTempBounds:
    def test_five_minute_station_bounds(self):
        low, high = calculate_temp_bounds(12.0, 53.6, StationType.FIVE_MINUTE)
        uncertainty = FIVE_MINUTE_F_UNCERTAINTY + 0.5
        assert abs(low - (53.6 - uncertainty)) < 0.01
        assert abs(high - (53.6 + uncertainty)) < 0.01

    def test_hourly_station_bounds(self):
        low, high = calculate_temp_bounds(12.0, 53.6, StationType.HOURLY)
        assert abs(low - (53.6 - HOURLY_F_UNCERTAINTY)) < 0.01
        assert abs(high - (53.6 + HOURLY_F_UNCERTAINTY)) < 0.01

    def test_unknown_station_conservative(self):
        low, high = calculate_temp_bounds(12.0, 53.6, StationType.UNKNOWN)
        assert abs(low - 52.6) < 0.01
        assert abs(high - 54.6) < 0.01

    def test_bounds_without_celsius(self):
        low, high = calculate_temp_bounds(None, 54.0, StationType.HOURLY)
        assert low < 54.0
        assert high > 54.0


# =============================================================================
# STATION TYPE DETECTION TESTS
# =============================================================================


class TestDetermineStationType:
    def test_five_minute_detection(self):
        assert determine_station_type(FIVE_MINUTE_OBSERVATIONS) == StationType.FIVE_MINUTE

    def test_hourly_detection(self):
        assert determine_station_type(HOURLY_OBSERVATIONS) == StationType.HOURLY

    def test_single_observation(self):
        assert determine_station_type([FIVE_MINUTE_OBSERVATIONS[0]]) == StationType.UNKNOWN

    def test_empty_observations(self):
        assert determine_station_type([]) == StationType.UNKNOWN

    def test_mixed_intervals(self):
        mixed = [
            make_observation("2026-01-20T17:00:00+00:00", 11.0),
            make_observation("2026-01-20T16:30:00+00:00", 12.0),
            make_observation("2026-01-20T16:00:00+00:00", 11.5),
        ]
        assert determine_station_type(mixed) == StationType.UNKNOWN


# =============================================================================
# OBSERVATION PARSING TESTS
# =============================================================================


class TestParseObservation:
    def test_parse_celsius_observation(self):
        obs = make_observation("2026-01-20T15:00:00+00:00", 12.5)
        reading = parse_observation(obs, StationType.HOURLY, STATION_ID)
        assert reading is not None
        assert reading.station_id == STATION_ID
        assert reading.reported_temp_c == 12.5
        assert abs(reading.reported_temp_f - 54.5) < 0.1

    def test_parse_fahrenheit_observation(self):
        obs = make_observation("2026-01-20T15:00:00+00:00", 54.5, "wmoUnit:degF")
        reading = parse_observation(obs, StationType.HOURLY, STATION_ID)
        assert reading is not None
        assert reading.reported_temp_f == 54.5
        assert reading.reported_temp_c is None

    def test_parse_missing_temperature(self):
        obs = {"type": "Feature", "properties": {"timestamp": "2026-01-20T15:00:00+00:00", "temperature": {"value": None, "unitCode": "wmoUnit:degC"}}}
        assert parse_observation(obs, StationType.HOURLY, STATION_ID) is None

    def test_parse_missing_timestamp(self):
        obs = {"type": "Feature", "properties": {"temperature": {"value": 12.0, "unitCode": "wmoUnit:degC"}}}
        assert parse_observation(obs, StationType.HOURLY, STATION_ID) is None

    def test_parse_includes_bounds(self):
        obs = make_observation("2026-01-20T15:00:00+00:00", 12.5)
        reading = parse_observation(obs, StationType.FIVE_MINUTE, STATION_ID)
        assert reading is not None
        assert reading.possible_actual_f_low < reading.reported_temp_f
        assert reading.possible_actual_f_high > reading.reported_temp_f

    def test_parse_includes_wind_fields(self):
        obs = {
            "type": "Feature",
            "properties": {
                "timestamp": "2026-01-20T15:00:00+00:00",
                "temperature": {"value": 12.5, "unitCode": "wmoUnit:degC"},
                "windDirection": {"value": 250.0, "unitCode": "wmoUnit:degree_(angle)"},
                "windSpeed": {"value": 5.0, "unitCode": "wmoUnit:m_s-1"},
            },
        }
        reading = parse_observation(obs, StationType.HOURLY, STATION_ID)
        assert reading is not None
        assert reading.wind_direction_deg == 250.0
        assert abs((reading.wind_speed_mph or 0.0) - 11.2) < 0.2


# =============================================================================
# NWS STATION PARSER TESTS
# =============================================================================


class TestNWSStationParser:
    @responses.activate
    def test_fetch_current_observations_success(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        readings = parser.fetch_current_observations()
        assert len(readings) == 5

    @responses.activate
    def test_fetch_detects_station_type(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        parser.fetch_current_observations()
        assert parser.get_station_type() == StationType.FIVE_MINUTE

    @responses.activate
    def test_fetch_api_error(self):
        responses.add(responses.GET, BASE_URL, status=500)
        parser = NWSStationParser(NYC)
        assert parser.fetch_current_observations() == []

    @responses.activate
    def test_fetch_timeout(self):
        from requests.exceptions import Timeout
        responses.add(responses.GET, BASE_URL, body=Timeout("Connection timed out"))
        parser = NWSStationParser(NYC)
        assert parser.fetch_current_observations() == []

    @responses.activate
    def test_get_daily_summary_success(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        summary = parser.get_daily_summary(TARGET_DATE)
        assert summary is not None
        assert summary.station_id == STATION_ID
        assert abs(summary.observed_high_f - 54.0) < 0.5

    @responses.activate
    def test_get_daily_summary_no_data(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response([]), status=200)
        parser = NWSStationParser(NYC)
        assert parser.get_daily_summary(TARGET_DATE) is None

    @responses.activate
    def test_get_daily_summary_wrong_date(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        assert parser.get_daily_summary("2026-01-21") is None

    @responses.activate
    def test_daily_summary_includes_uncertainty_bounds(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        summary = parser.get_daily_summary(TARGET_DATE)
        assert summary is not None
        assert summary.possible_actual_high_high > summary.observed_high_f
        assert summary.possible_actual_high_low < summary.observed_high_f


# =============================================================================
# CONVENIENCE FUNCTION TESTS
# =============================================================================


class TestConvenienceFunctions:
    @responses.activate
    def test_get_station_observations(self):
        url = NWS_STATIONS_URL.format(station_id="KNYC")
        responses.add(responses.GET, url, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        readings = get_station_observations(NYC)
        assert len(readings) == 5

    @responses.activate
    def test_get_daily_observation(self):
        url = NWS_STATIONS_URL.format(station_id="KNYC")
        responses.add(responses.GET, url, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        # Use a custom parser with UTC timezone for testing
        from kalshi_weather.data.stations import NWSStationParser
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        summary = parser.get_daily_summary(TARGET_DATE)
        assert summary is not None


# =============================================================================
# EDGE CASE TESTS
# =============================================================================


class TestEdgeCases:
    @responses.activate
    def test_malformed_observation_skipped(self):
        observations = [
            FIVE_MINUTE_OBSERVATIONS[0],
            {"type": "Feature", "properties": {}},
            FIVE_MINUTE_OBSERVATIONS[1],
        ]
        responses.add(responses.GET, BASE_URL, json=make_api_response(observations), status=200)
        parser = NWSStationParser(NYC)
        readings = parser.fetch_current_observations()
        assert len(readings) == 2

    @responses.activate
    def test_negative_temperatures(self):
        observations = [
            make_observation("2026-01-20T17:00:00+00:00", -5.0),
            make_observation("2026-01-20T16:55:00+00:00", -10.0),
        ]
        responses.add(responses.GET, BASE_URL, json=make_api_response(observations), status=200)
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        summary = parser.get_daily_summary(TARGET_DATE)
        assert summary is not None
        assert abs(summary.observed_high_f - 23.0) < 0.5

    @responses.activate
    def test_readings_list_populated(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        summary = parser.get_daily_summary(TARGET_DATE)
        assert summary is not None
        assert len(summary.readings) == 5

    def test_invalid_json_response(self):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET, BASE_URL, body="not valid json", status=200)
            parser = NWSStationParser(NYC)
            assert parser.fetch_current_observations() == []


# =============================================================================
# UNCERTAINTY CALCULATION TESTS
# =============================================================================


class TestUncertaintyCalculations:
    @responses.activate
    def test_inter_reading_uncertainty_applied(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(FIVE_MINUTE_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        parser.timezone = ZoneInfo("UTC")
        summary = parser.get_daily_summary(TARGET_DATE)
        assert summary is not None
        assert summary.possible_actual_high_high >= summary.observed_high_f + 0.5

    @responses.activate
    def test_hourly_station_tighter_bounds(self):
        responses.add(responses.GET, BASE_URL, json=make_api_response(HOURLY_OBSERVATIONS), status=200)
        parser = NWSStationParser(NYC)
        readings = parser.fetch_current_observations()
        assert parser.get_station_type() == StationType.HOURLY
        for reading in readings:
            spread = reading.possible_actual_f_high - reading.possible_actual_f_low
            assert spread <= 2 * (HOURLY_F_UNCERTAINTY + 0.1)


class TestRecentHistoryExtraction:
    def test_recent_history_uses_last_window(self):
        readings = []
        for i, temp in enumerate([50.0, 51.0, 51.5, 52.0, 52.3, 52.6, 52.9]):
            readings.append(
                parse_observation(
                    make_observation(f"2026-01-20T1{i}:00:00+00:00", (temp - 32) * 5 / 9),
                    StationType.HOURLY,
                    STATION_ID,
                )
            )
        valid = [r for r in readings if r is not None]
        recent = get_recent_observation_history(
            valid,
            timezone=ZoneInfo("UTC"),
            current_time=datetime(2026, 1, 20, 16, 0, tzinfo=ZoneInfo("UTC")),
            window_size=4,
            max_age_hours=8,
        )
        assert len(recent) == 4
        assert recent[-1].reported_temp_f >= recent[0].reported_temp_f

    def test_recent_history_graceful_when_sparse(self):
        sparse = [
            parse_observation(make_observation("2026-01-20T12:00:00+00:00", 10.0), StationType.HOURLY, STATION_ID),
            parse_observation(make_observation("2026-01-20T16:00:00+00:00", 11.0), StationType.HOURLY, STATION_ID),
        ]
        valid = [r for r in sparse if r is not None]
        recent = get_recent_observation_history(
            valid,
            timezone=ZoneInfo("UTC"),
            current_time=datetime(2026, 1, 20, 16, 10, tzinfo=ZoneInfo("UTC")),
            window_size=6,
            max_age_hours=1,
        )
        assert len(recent) == 2
