"""Base contract class for Kalshi weather markets."""

from abc import ABC, abstractmethod
from typing import List, Optional

from kalshi_weather.core import (
    ContractType,
    TemperatureForecast,
    DailyObservation,
    MarketBracket,
)
from kalshi_weather.config import CityConfig


class BaseContract(ABC):
    """
    Abstract base class for Kalshi weather contracts.

    Extend this class to support new contract types (e.g., low temp, snowfall).
    """

    def __init__(self, city: CityConfig):
        """
        Initialize the contract.

        Args:
            city: City configuration
        """
        self.city = city

    @property
    @abstractmethod
    def contract_type(self) -> ContractType:
        """Return the contract type."""
        pass

    @property
    @abstractmethod
    def series_ticker(self) -> str:
        """Return the Kalshi series ticker for this contract."""
        pass

    @abstractmethod
    def fetch_forecasts(self, target_date: str) -> List[TemperatureForecast]:
        """
        Fetch relevant forecasts for this contract type.

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            List of forecasts
        """
        pass

    @abstractmethod
    def fetch_observations(self, target_date: str) -> Optional[DailyObservation]:
        """
        Fetch relevant observations for this contract type.

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            Daily observation summary or None
        """
        pass

    @abstractmethod
    def fetch_brackets(self, target_date: str) -> List[MarketBracket]:
        """
        Fetch market brackets for this contract.

        Args:
            target_date: Date in YYYY-MM-DD format

        Returns:
            List of market brackets
        """
        pass

    def get_settlement_source(self) -> str:
        """Return the URL or description of the settlement source."""
        return f"https://www.weather.gov/wrh/climate?wfo={self.city.wfo}"
