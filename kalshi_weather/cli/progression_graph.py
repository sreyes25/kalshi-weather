"""Live terminal graph for intraday temperature progression logs."""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from rich.live import Live
from rich.panel import Panel
from rich.table import Table


@dataclass
class ProgressPoint:
    timestamp: datetime
    observed_high_f: Optional[float]
    final_model_mean: Optional[float]


class TemperatureProgressionViewer:
    """Render daily CSV logs as a live-updating terminal line graph."""

    def __init__(
        self,
        city_code: str,
        target_date: Optional[str] = None,
        base_dir: Path = Path("logs/temperature_progression"),
        chart_width: int = 72,
        chart_height: int = 14,
    ):
        self.city_code = city_code.upper()
        self.target_date = target_date or datetime.now().strftime("%Y-%m-%d")
        self.base_dir = base_dir
        self.chart_width = chart_width
        self.chart_height = chart_height

    @property
    def csv_path(self) -> Path:
        return self.base_dir / self.city_code / f"{self.target_date}.csv"

    def load_points(self) -> list[ProgressPoint]:
        path = self.csv_path
        if not path.exists():
            return []

        points: list[ProgressPoint] = []
        with path.open("r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    timestamp = datetime.fromisoformat(row.get("timestamp", ""))
                except ValueError:
                    continue

                def parse_float(key: str) -> Optional[float]:
                    value = (row.get(key) or "").strip()
                    if not value:
                        return None
                    try:
                        return float(value)
                    except ValueError:
                        return None

                points.append(
                    ProgressPoint(
                        timestamp=timestamp,
                        observed_high_f=parse_float("observed_high_f"),
                        final_model_mean=parse_float("final_model_mean"),
                    )
                )
        return points

    def _downsample(self, points: list[ProgressPoint], max_points: int) -> list[ProgressPoint]:
        if len(points) <= max_points:
            return points
        step = (len(points) - 1) / (max_points - 1)
        sampled = [points[round(i * step)] for i in range(max_points)]
        return sampled

    def _build_chart_body(self, points: list[ProgressPoint]) -> str:
        values = [
            v
            for p in points
            for v in (p.observed_high_f, p.final_model_mean)
            if v is not None
        ]
        if not values:
            return "No numeric points yet."

        min_v = min(values)
        max_v = max(values)
        if min_v == max_v:
            min_v -= 1.0
            max_v += 1.0

        points = self._downsample(points, self.chart_width)
        width = len(points)
        height = self.chart_height
        grid = [[" " for _ in range(width)] for _ in range(height)]

        def to_row(value: float) -> int:
            pct = (value - min_v) / (max_v - min_v)
            return height - 1 - int(round(pct * (height - 1)))

        for x, point in enumerate(points):
            obs_row = to_row(point.observed_high_f) if point.observed_high_f is not None else None
            model_row = to_row(point.final_model_mean) if point.final_model_mean is not None else None

            if obs_row is not None:
                grid[obs_row][x] = "o"
            if model_row is not None:
                grid[model_row][x] = "x" if grid[model_row][x] == " " else "*"

        lines: list[str] = []
        for i, row in enumerate(grid):
            y_value = max_v - ((max_v - min_v) * i / (height - 1))
            lines.append(f"{y_value:6.1f} |{''.join(row)}")

        first_ts = points[0].timestamp.strftime("%H:%M:%S")
        last_ts = points[-1].timestamp.strftime("%H:%M:%S")
        axis = " " * 8 + "-" * width
        labels = f"{' ' * 8}{first_ts}{' ' * max(1, width - len(first_ts) - len(last_ts))}{last_ts}"
        legend = "Legend: observed_high_f=o  final_model_mean=x  overlap=*"
        return "\n".join(lines + [axis, labels, legend])

    def build_panel(self) -> Panel:
        points = self.load_points()
        table = Table.grid(expand=True)
        table.add_column()
        table.add_row(f"File: {self.csv_path}")

        if not points:
            table.add_row("Waiting for log points. Run `python -m kalshi_weather run` in another terminal.")
            return Panel(table, title=f"Temperature Progression - {self.city_code} {self.target_date}")

        table.add_row(f"Points: {len(points)}")
        table.add_row(self._build_chart_body(points))
        return Panel(table, title=f"Temperature Progression - {self.city_code} {self.target_date}")

    def run(self, refresh_seconds: int = 30, once: bool = False) -> None:
        if once:
            from rich.console import Console

            Console().print(self.build_panel())
            return

        with Live(self.build_panel(), refresh_per_second=4, screen=True) as live:
            while True:
                live.update(self.build_panel())
                time.sleep(refresh_seconds)


def run_temperature_progression_viewer(
    city_code: str,
    target_date: Optional[str],
    refresh_seconds: int,
    once: bool,
) -> None:
    viewer = TemperatureProgressionViewer(city_code=city_code, target_date=target_date)
    viewer.run(refresh_seconds=refresh_seconds, once=once)
