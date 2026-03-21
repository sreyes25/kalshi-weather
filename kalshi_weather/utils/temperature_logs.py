"""Daily CSV logging for intraday temperature progression."""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from kalshi_weather.core.models import MarketAnalysis

logger = logging.getLogger(__name__)


@dataclass
class DailyTemperatureLogger:
    """Append intraday analysis snapshots to one CSV file per local day."""

    city_code: str
    base_dir: Path = Path("logs/temperature_progression")

    def _resolve_file_path(self, analyzed_at: datetime) -> Path:
        day = analyzed_at.date().isoformat()
        city_dir = self.base_dir / self.city_code.upper()
        city_dir.mkdir(parents=True, exist_ok=True)
        return city_dir / f"{day}.csv"

    @staticmethod
    def _fmt_float(value: Optional[float]) -> str:
        return "" if value is None else f"{value:.3f}"

    def append_snapshot(self, analysis: MarketAnalysis) -> Path:
        """Write one row for the current analysis cycle."""
        output_path = self._resolve_file_path(analysis.analyzed_at)
        file_exists = output_path.exists()

        row = {
            "timestamp": analysis.analyzed_at.isoformat(),
            "city": analysis.city,
            "target_date": analysis.target_date,
            "raw_forecast_mean": self._fmt_float(analysis.raw_forecast_mean),
            "final_model_mean": self._fmt_float(analysis.adjusted_forecast_mean or analysis.forecast_mean),
            "observed_high_f": self._fmt_float(
                analysis.observation.observed_high_f if analysis.observation else None
            ),
            "forecast_std": self._fmt_float(analysis.forecast_std),
            "final_model_std": self._fmt_float(analysis.adjusted_forecast_std or analysis.forecast_std),
        }
        fieldnames = list(row.keys())

        with output_path.open("a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        logger.debug("Appended temperature progression row to %s", output_path)
        return output_path
