"""High temperature contract implementation."""

from typing import List, Optional

from kalshi_weather.contracts.base import BaseContract
from kalshi_weather.core import (
    ContractType,
    TemperatureForecast,
    DailyObservation,
    MarketBracket,
    OpenPosition,
)
from kalshi_weather.config import CityConfig, DEFAULT_CITY
from kalshi_weather.data import (
    CombinedWeatherSource,
    NWSStationParser,
    KalshiMarketClient,
)


class HighTempContract(BaseContract):
    """
    High temperature contract for Kalshi weather markets.

    Settles on the daily high temperature from NWS Daily Climate Report.
    """

    def __init__(self, city: CityConfig = None):
        """
        Initialize the high temperature contract.

        Args:
            city: City configuration (default: NYC)
        """
        super().__init__(city or DEFAULT_CITY)
        self._weather_source = CombinedWeatherSource(self.city)
        self._station_parser = NWSStationParser(self.city)
        self._market_client = KalshiMarketClient(self.city, ContractType.HIGH_TEMP)

    @property
    def contract_type(self) -> ContractType:
        """Return the contract type."""
        return ContractType.HIGH_TEMP

    @property
    def series_ticker(self) -> str:
        """Return the Kalshi series ticker for this contract."""
        return self.city.high_temp_ticker

    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """
        Fetch high temperature forecasts.

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            List of temperature forecasts
        """
        return self._weather_source.fetch_forecasts(target_date)

    def fetch_observations(self, target_date: str) -> Optional[DailyObservation]:
        """
        Fetch high temperature observations.

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            Daily observation summary
        """
        return self._station_parser.get_daily_summary(target_date)

    def fetch_brackets(self, target_date: str) -> List[MarketBracket]:
        """
        Fetch market brackets for this contract.

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            List of market brackets
        """
        return self._market_client.fetch_brackets(target_date)

    def get_market_status(self) -> dict:
        """Get current market status."""
        return self._market_client.get_market_status()

    def get_available_dates(self) -> List[str]:
        """Get list of dates with open markets."""
        return self._market_client.get_available_dates()

    def fetch_open_positions(self) -> List[OpenPosition]:
        """Fetch current account open positions (if authenticated)."""
        positions: List[OpenPosition] = []
        for row in self._market_client.fetch_open_positions():
            positions.append(
                OpenPosition(
                    ticker=row["ticker"],
                    side=row["side"],
                    contracts=row["contracts"],
                    average_entry_price_cents=row["average_entry_price_cents"],
                    event_ticker=row.get("event_ticker"),
                    subtitle=row.get("subtitle"),
                    yes_bid=row.get("yes_bid"),
                    yes_ask=row.get("yes_ask"),
                    last_price=row.get("last_price"),
                )
            )
        return positions
