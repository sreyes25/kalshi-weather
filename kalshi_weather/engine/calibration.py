"""Forecast calibration utilities.

Tracks historical per-source forecast errors against settled highs and produces
rolling bias/MAE stats for automatic forecast correction.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from kalshi_weather.config import CityConfig, DEFAULT_CITY
from kalshi_weather.core import TemperatureForecast
from kalshi_weather.data.historical import fetch_settlement

logger = logging.getLogger(__name__)

DEFAULT_CALIBRATION_PATH = Path(
    os.getenv("KALSHI_CALIBRATION_FILE", "/tmp/kalshi_weather_forecast_calibration.json")
)


@dataclass
class SourceCalibrationStats:
    source: str
    sample_count: int
    bias_f: float
    mae_f: float
    rmse_f: float


class ForecastCalibrator:
    """Persistent forecast-error tracker and rolling calibration stats provider."""

    def __init__(
        self,
        city: Optional[CityConfig] = None,
        path: Optional[Path] = None,
        lookback_days: int = 45,
        min_samples_for_correction: int = 3,
    ):
        self.city = city or DEFAULT_CITY
        self.path = path or DEFAULT_CALIBRATION_PATH
        self.lookback_days = lookback_days
        self.min_samples_for_correction = min_samples_for_correction

    def record_forecast_snapshot(
        self,
        forecasts: List[TemperatureForecast],
        target_date: str,
        as_of: Optional[datetime] = None,
    ) -> None:
        if not forecasts:
            return

        data = self._load_store()
        as_of_dt = as_of or datetime.now()
        as_of_date = as_of_dt.strftime("%Y-%m-%d")

        for forecast in forecasts:
            record = {
                "city_code": self.city.code,
                "source": forecast.source,
                "target_date": target_date,
                "as_of_date": as_of_date,
                "forecast_temp_f": float(forecast.forecast_temp_f),
                "fetched_at": forecast.fetched_at.isoformat(),
            }
            # Avoid duplicate snapshots for the same source/as_of/target tuple.
            if any(
                r.get("city_code") == record["city_code"]
                and r.get("source") == record["source"]
                and r.get("target_date") == record["target_date"]
                and r.get("as_of_date") == record["as_of_date"]
                for r in data["forecast_snapshots"]
            ):
                continue
            data["forecast_snapshots"].append(record)

        self._prune_store(data)
        self._save_store(data)

    def refresh_settlement_errors(self, min_refresh_minutes: int = 60) -> None:
        """Reconcile unresolved past forecast snapshots with settled highs."""
        data = self._load_store()

        last_refresh_str = data["meta"].get("last_refresh")
        if last_refresh_str:
            try:
                last_refresh = datetime.fromisoformat(last_refresh_str)
                if datetime.now() - last_refresh < timedelta(minutes=min_refresh_minutes):
                    return
            except ValueError:
                pass

        error_keys = {
            (e.get("city_code"), e.get("source"), e.get("target_date"), e.get("as_of_date"))
            for e in data["errors"]
        }

        unresolved_dates = sorted({
            r.get("target_date")
            for r in data["forecast_snapshots"]
            if r.get("city_code") == self.city.code
            and (r.get("city_code"), r.get("source"), r.get("target_date"), r.get("as_of_date")) not in error_keys
        })

        today = datetime.now().date()
        unresolved_dates = [d for d in unresolved_dates if d and datetime.strptime(d, "%Y-%m-%d").date() < today]

        for target_date in unresolved_dates:
            settlement = fetch_settlement(target_date, city=self.city, use_fallback=True)
            if not settlement:
                continue

            # Use the latest available snapshot for each source prior to settlement.
            by_source: Dict[str, dict] = {}
            for snap in data["forecast_snapshots"]:
                if snap.get("city_code") != self.city.code or snap.get("target_date") != target_date:
                    continue
                source = snap.get("source")
                if source is None:
                    continue
                current = by_source.get(source)
                if current is None or snap.get("as_of_date", "") > current.get("as_of_date", ""):
                    by_source[source] = snap

            for source, snap in by_source.items():
                key = (self.city.code, source, target_date, snap.get("as_of_date"))
                if key in error_keys:
                    continue

                forecast_temp = float(snap.get("forecast_temp_f", 0.0))
                error = forecast_temp - settlement.settlement_high_f
                data["errors"].append({
                    "city_code": self.city.code,
                    "source": source,
                    "target_date": target_date,
                    "as_of_date": snap.get("as_of_date"),
                    "forecast_temp_f": forecast_temp,
                    "settlement_high_f": float(settlement.settlement_high_f),
                    "error_f": float(error),
                    "settlement_source": settlement.source,
                })
                error_keys.add(key)

        data["meta"]["last_refresh"] = datetime.now().isoformat()
        self._prune_store(data)
        self._save_store(data)

    def get_source_stats(self) -> Dict[str, SourceCalibrationStats]:
        data = self._load_store()
        cutoff = datetime.now().date() - timedelta(days=self.lookback_days)

        per_source: Dict[str, List[float]] = {}
        for err in data["errors"]:
            if err.get("city_code") != self.city.code:
                continue
            date_str = err.get("target_date")
            source = err.get("source")
            if not source or not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            if dt < cutoff:
                continue
            per_source.setdefault(source, []).append(float(err.get("error_f", 0.0)))

        stats: Dict[str, SourceCalibrationStats] = {}
        for source, errors in per_source.items():
            if len(errors) < self.min_samples_for_correction:
                continue
            n = len(errors)
            bias = sum(errors) / n
            mae = sum(abs(e) for e in errors) / n
            rmse = (sum(e * e for e in errors) / n) ** 0.5
            stats[source] = SourceCalibrationStats(
                source=source,
                sample_count=n,
                bias_f=bias,
                mae_f=mae,
                rmse_f=rmse,
            )
        return stats

    def get_correction_payload(self) -> Dict[str, Dict[str, float]]:
        """Return lightweight dict payload for runtime combiner usage."""
        stats = self.get_source_stats()
        return {
            src: {
                "sample_count": float(s.sample_count),
                "bias_f": s.bias_f,
                "mae_f": s.mae_f,
                "rmse_f": s.rmse_f,
            }
            for src, s in stats.items()
        }

    def _load_store(self) -> dict:
        if not self.path.exists():
            return {
                "meta": {},
                "forecast_snapshots": [],
                "errors": [],
            }
        try:
            return json.loads(self.path.read_text())
        except (json.JSONDecodeError, OSError):
            logger.warning("Failed to read calibration store; resetting: %s", self.path)
            return {
                "meta": {},
                "forecast_snapshots": [],
                "errors": [],
            }

    def _save_store(self, data: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(data, indent=2, sort_keys=True))
        tmp_path.replace(self.path)

    def _prune_store(self, data: dict) -> None:
        cutoff = datetime.now().date() - timedelta(days=180)

        def _keep(date_str: str) -> bool:
            try:
                return datetime.strptime(date_str, "%Y-%m-%d").date() >= cutoff
            except Exception:
                return False

        data["forecast_snapshots"] = [
            s for s in data["forecast_snapshots"]
            if _keep(str(s.get("target_date", "")))
        ]
        data["errors"] = [
            e for e in data["errors"]
            if _keep(str(e.get("target_date", "")))
        ]


def get_runtime_source_corrections(city: Optional[CityConfig] = None) -> Dict[str, Dict[str, float]]:
    """
    Load per-source calibration corrections for runtime forecast combining.

    Best-effort: returns empty dict on failures.
    """
    try:
        calibrator = ForecastCalibrator(city=city)
        return calibrator.get_correction_payload()
    except Exception as exc:
        logger.warning("Failed to load runtime source corrections: %s", exc)
        return {}
