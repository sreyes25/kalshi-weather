"""
Terminal Dashboard for Kalshi Weather Bot.

Uses Rich to display real-time analysis, forecasts, and trading signals.
"""

import math
from datetime import datetime
from typing import Optional, List, Tuple

from rich.console import Console, Group
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich import box

from kalshi_weather.core.models import (
    MarketAnalysis,
    TradingSignal,
    MarketBracket,
    PositionRecommendation,
    OpenPosition,
    BracketType,
)
from kalshi_weather.config.settings import (
    AUTO_SELL_ENABLED,
    AUTO_SELL_DRY_RUN,
    AUTO_SELL_START_HOUR_LOCAL,
    AUTO_SELL_START_MINUTE_LOCAL,
    AUTO_SELL_MIN_PRIMARY_GAP_PP,
    AUTO_SELL_MIN_PROFIT_CENTS,
    POSITION_EXIT_FEE_RATE,
)
from kalshi_weather.engine.probability import DEFAULT_WEIGHTS

class Dashboard:
    """
    Terminal User Interface for the weather bot.
    """

    def __init__(self):
        self.console = Console()
        self.layout = Layout()
        self._setup_layout()

    def _setup_layout(self):
        """Define the grid layout."""
        self.layout.split(
            Layout(name="header", size=3),
            Layout(name="main", ratio=1),
            Layout(name="footer", size=2),
        )

        self.layout["main"].split_row(
            Layout(name="left", ratio=7),
            Layout(name="right", ratio=9),
        )

        self.layout["left"].split(
            Layout(name="forecasts", size=12),
            Layout(name="outcome", size=11),
            Layout(name="observations", ratio=1),
        )

        self.layout["right"].split(
            Layout(name="brackets", size=10),
            Layout(name="signals", ratio=1),
            Layout(name="paper", size=6),
            Layout(name="positions", ratio=1),
        )

    def generate_header(self, analysis: Optional[MarketAnalysis] = None) -> Panel:
        """Create header panel."""
        if analysis:
            title = f"Kalshi Weather Bot - {analysis.city} - Target: {analysis.target_date}"
            sub_text = f"Last Updated: {datetime.now().strftime('%H:%M:%S')}"
        else:
            title = "Kalshi Weather Bot"
            sub_text = "Initializing..."
        
        grid = Table.grid(expand=True)
        grid.add_column(justify="center", ratio=1)
        grid.add_row(f"[b]{title}[/b]")
        grid.add_row(f"[dim]{sub_text}[/dim]")
        
        return Panel(grid, style="bold white on blue")

    def generate_forecast_table(self, analysis: MarketAnalysis) -> Panel:
        """Create forecast table."""
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Source")
        table.add_column("Temp", justify="right")
        table.add_column("StdDev", justify="right")
        table.add_column("Delta", justify="right")
        table.add_column("Changed", justify="right")

        raw_mean = analysis.raw_forecast_mean if analysis.raw_forecast_mean is not None else analysis.forecast_mean
        raw_std = analysis.raw_forecast_std if analysis.raw_forecast_std is not None else analysis.forecast_std
        adjusted_mean = (
            analysis.adjusted_forecast_mean
            if analysis.adjusted_forecast_mean is not None
            else analysis.forecast_mean
        )
        adjusted_std = (
            analysis.adjusted_forecast_std
            if analysis.adjusted_forecast_std is not None
            else analysis.forecast_std
        )

        # Forecast-only summary (kept de-emphasized)
        table.add_row(
            "[b]Forecast-Only Mean[/b]",
            f"{raw_mean:.1f}°F",
            f"{raw_std:.1f}°F",
            "",
            "",
            style="dim",
        )
        table.add_section()

        # Individual Forecasts
        for f in analysis.forecasts:
            changed_at = analysis.source_last_changed_at.get(f.source)
            source_delta = analysis.source_last_change_delta.get(f.source)
            table.add_row(
                f.source,
                f"{f.forecast_temp_f:.1f}°F",
                f"{f.std_dev:.1f}°F",
                self._format_source_delta(source_delta),
                self._format_source_change_time(changed_at, analysis.analyzed_at),
                style="dim",
            )
        table.add_section()

        # Final model callout (placed below sources)
        table.add_row(
            "[b]Final Model Mean[/b]",
            f"[b]{adjusted_mean:.1f}°F[/b]",
            f"[b]{adjusted_std:.1f}°F[/b]",
            "",
            "",
            style="bold black on bright_yellow",
        )
        delta = adjusted_mean - raw_mean
        table.add_row(
            "Adjustment",
            f"{delta:+.1f}°F",
            "",
            "",
            "",
            style="dim",
        )

        return Panel(table, title="Weather Forecasts", border_style="cyan")

    def _format_source_change_time(
        self,
        changed_at: Optional[datetime],
        analyzed_at: datetime,
    ) -> str:
        if changed_at is None:
            return "-"
        if analyzed_at.tzinfo is not None and changed_at.tzinfo is not None:
            changed_local = changed_at.astimezone(analyzed_at.tzinfo)
        else:
            changed_local = changed_at
        hour = changed_local.strftime("%I").lstrip("0") or "12"
        suffix = "a.m." if changed_local.hour < 12 else "p.m."
        return f"{changed_local.strftime('%Y-%m-%d')} {hour}:{changed_local.strftime('%M:%S')} {suffix}"

    def _format_source_delta(self, delta: Optional[float]) -> str:
        if delta is None:
            return "-"
        if abs(float(delta)) < 0.05:
            return "0.0"
        return f"{float(delta):+.1f}"

    def _wind_compass(self, degrees: float) -> str:
        directions = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        idx = int(((degrees % 360.0) + 22.5) // 45.0) % 8
        return directions[idx]

    def generate_observation_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create observation summary."""
        if not analysis.observation:
            return Panel("No observation data available", title="Live Observations", border_style="white")
            
        obs = analysis.observation
        
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        
        grid.add_row("Station:", obs.station_id)
        series_high = obs.reported_series_high_f if obs.reported_series_high_f is not None else obs.observed_high_f
        grid.add_row("Reported High (obs):", f"{series_high:.1f}°F")
        if obs.reported_max_6h_f is not None:
            grid.add_row("Reported High (6h max):", f"{obs.reported_max_6h_f:.1f}°F")
        else:
            grid.add_row("Reported High (6h max):", "-")
        effective_high = self._effective_observation_floor(analysis)
        if effective_high is None:
            effective_high = obs.observed_high_f
        grid.add_row("Effective High (model/risk):", f"[b]{effective_high:.1f}°F[/b]")
        grid.add_row("Actual High (Est):", f"{obs.possible_actual_high_low:.1f}° - {obs.possible_actual_high_high:.1f}°")
        grid.add_row("Readings:", str(len(obs.readings)))
        
        # Show last reading time and value if available
        if obs.readings:
            last = obs.readings[-1]
            grid.add_row("Last Reading:", f"{last.timestamp.astimezone().strftime('%H:%M')} ({last.reported_temp_f}°F)")
            if last.wind_direction_deg is not None or last.wind_speed_mph is not None:
                wind_parts: List[str] = []
                if last.wind_direction_deg is not None:
                    compass = self._wind_compass(float(last.wind_direction_deg))
                    wind_parts.append(f"{compass} ({last.wind_direction_deg:.0f}°)")
                if last.wind_speed_mph is not None:
                    wind_parts.append(f"{last.wind_speed_mph:.1f} mph")
                grid.add_row("Wind:", " @ ".join(wind_parts))
            if last.dewpoint_f is not None:
                grid.add_row("Dew Point:", f"{last.dewpoint_f:.1f}°F")
            if last.relative_humidity_pct is not None:
                grid.add_row("Rel Humidity:", f"{last.relative_humidity_pct:.0f}%")
            temp_trend, dew_trend, rh_trend = self._observation_trends(obs.readings)
            if temp_trend is not None:
                grid.add_row("Temp Trend (60m):", f"{temp_trend:+.2f}°F/hr")
            if dew_trend is not None:
                grid.add_row("Dew Trend (60m):", f"{dew_trend:+.2f}°F/hr")
            if rh_trend is not None:
                grid.add_row("RH Trend (60m):", f"{rh_trend:+.1f}%/hr")
            nowcast_score = self._nowcast_score(temp_trend, dew_trend, rh_trend)
            if nowcast_score is not None:
                label = "Warming bias" if nowcast_score >= 0 else "Cooling bias"
                grid.add_row("Nowcast Score:", f"{nowcast_score:+.0f} ({label})")

        if analysis.trajectory_assessment:
            traj = analysis.trajectory_assessment
            grid.add_row("High Already Set:", f"{traj.prob_high_already_set:.0%}")
            grid.add_row("Exceed Current High:", f"{traj.prob_exceed_observed_high:.0%}")
            grid.add_row("Trend:", f"{traj.trend_f_per_hour:+.2f}°F/hr")
            grid.add_row("Lock Confidence:", f"{traj.lock_confidence:.0%}")
        
        return Panel(grid, title="Live Observations (KNYC)", border_style="green")

    def _linear_trend(self, points: List[Tuple[float, float]]) -> Optional[float]:
        if len(points) < 2:
            return None
        xs = [p[0] for p in points]
        ys = [p[1] for p in points]
        if xs[-1] - xs[0] <= 0.0:
            return None
        x_mean = sum(xs) / len(xs)
        y_mean = sum(ys) / len(ys)
        denom = sum((x - x_mean) ** 2 for x in xs)
        if denom <= 1e-9:
            return None
        slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys)) / denom
        return slope

    def _observation_trends(self, readings) -> tuple[Optional[float], Optional[float], Optional[float]]:
        if len(readings) < 2:
            return (None, None, None)
        recent = readings[-12:]
        t0 = recent[0].timestamp
        temp_points: List[Tuple[float, float]] = []
        dew_points: List[Tuple[float, float]] = []
        rh_points: List[Tuple[float, float]] = []
        for row in recent:
            x = (row.timestamp - t0).total_seconds() / 3600.0
            temp_points.append((x, row.reported_temp_f))
            if row.dewpoint_f is not None:
                dew_points.append((x, row.dewpoint_f))
            if row.relative_humidity_pct is not None:
                rh_points.append((x, row.relative_humidity_pct))
        return (
            self._linear_trend(temp_points),
            self._linear_trend(dew_points),
            self._linear_trend(rh_points),
        )

    def _nowcast_score(
        self,
        temp_trend: Optional[float],
        dew_trend: Optional[float],
        rh_trend: Optional[float],
    ) -> Optional[float]:
        if temp_trend is None and dew_trend is None and rh_trend is None:
            return None
        t = temp_trend or 0.0
        d = dew_trend or 0.0
        r = rh_trend or 0.0
        # Positive score => warming persistence, negative => cooling/lock bias.
        score = (12.0 * t) + (8.0 * d) + (0.6 * r)
        return max(-100.0, min(100.0, score))

    def _get_expected_range(self, mean_f: float, std_f: float) -> tuple[int, int]:
        """
        Build a user-friendly range around the current modeled high.

        This is intentionally simple and narrow for readability.
        """
        band = max(1.0, min(3.0, std_f * 0.9))
        low = int(round(mean_f - band))
        high = int(round(mean_f + band))
        if high < low:
            high = low
        return low, high

    def _effective_observation_floor(self, analysis: MarketAnalysis) -> Optional[float]:
        obs = analysis.observation
        if obs is None:
            return None
        values = [
            obs.observed_high_f,
            obs.reported_series_high_f,
            obs.reported_max_6h_f,
            obs.possible_actual_high_high,
        ]
        candidates = []
        for v in values:
            if v is None:
                continue
            try:
                candidates.append(float(v))
            except (TypeError, ValueError):
                continue
        return max(candidates) if candidates else None

    def _normal_cdf(self, x: float, mean: float, std_dev: float) -> float:
        if std_dev <= 0:
            return 1.0 if x >= mean else 0.0
        z = (x - mean) / (std_dev * math.sqrt(2.0))
        return 0.5 * (1.0 + math.erf(z))

    def _source_weight(self, source: str) -> float:
        if source in DEFAULT_WEIGHTS:
            return float(DEFAULT_WEIGHTS[source])
        source_lower = source.lower()
        for key, weight in DEFAULT_WEIGHTS.items():
            if key.lower() in source_lower or source_lower in key.lower():
                return float(weight)
        return float(DEFAULT_WEIGHTS.get("default", 1.0))

    def _most_likely_discrete_high(self, analysis: MarketAnalysis, mean_f: float, std_f: float) -> int:
        """
        Return integer temperature with highest probability mass under modeled distribution.
        """
        observed_floor = (
            int(math.ceil(analysis.observation.observed_high_f))
            if analysis.observation is not None
            else int(math.floor(mean_f - 5.0 * max(1.0, std_f)))
        )
        low = max(observed_floor, int(math.floor(mean_f - 5.0 * max(1.0, std_f))))
        high = int(math.ceil(mean_f + 5.0 * max(1.0, std_f)))
        if high < low:
            high = low

        best_temp = low
        best_prob = -1.0
        for temp in range(low, high + 1):
            prob = self._normal_cdf(temp + 0.5, mean_f, std_f) - self._normal_cdf(temp - 0.5, mean_f, std_f)
            if prob > best_prob:
                best_prob = prob
                best_temp = temp
        return best_temp

    def _most_likely_from_source_consensus(self, analysis: MarketAnalysis) -> Optional[int]:
        if not analysis.forecasts:
            return None
        weighted_rows: List[Tuple[float, float, float]] = []
        for f in analysis.forecasts:
            std = max(0.8, float(f.std_dev or 0.0))
            weight = self._source_weight(f.source)
            weighted_rows.append((float(f.forecast_temp_f), std, weight))
        total_w = sum(w for _, _, w in weighted_rows)
        if total_w <= 0:
            return None
        weighted_rows = [(m, s, w / total_w) for m, s, w in weighted_rows]

        low = int(math.floor(min(m - 5.0 * s for m, s, _ in weighted_rows)))
        high = int(math.ceil(max(m + 5.0 * s for m, s, _ in weighted_rows)))
        if analysis.observation is not None:
            low = max(low, int(math.ceil(analysis.observation.observed_high_f)))
        if high < low:
            high = low

        best_temp = low
        best_prob = -1.0
        for temp in range(low, high + 1):
            p = 0.0
            for mean_f, std_f, w in weighted_rows:
                p += w * (
                    self._normal_cdf(temp + 0.5, mean_f, std_f)
                    - self._normal_cdf(temp - 0.5, mean_f, std_f)
                )
            if p > best_prob:
                best_prob = p
                best_temp = temp
        return best_temp

    def _representative_temp_for_bracket(self, bracket: MarketBracket) -> Optional[int]:
        """
        Choose a representative integer high for a modeled bracket.
        """
        if bracket.bracket_type == BracketType.BETWEEN:
            if bracket.lower_bound is None or bracket.upper_bound is None:
                return None
            return int(round((float(bracket.lower_bound) + float(bracket.upper_bound)) / 2.0))
        if bracket.bracket_type == BracketType.LESS_THAN:
            if bracket.upper_bound is None:
                return None
            return int(bracket.upper_bound) - 1
        if bracket.bracket_type == BracketType.GREATER_THAN:
            if bracket.lower_bound is None:
                return None
            return int(bracket.lower_bound) + 1
        return None

    def _find_bracket_for_discrete_high(
        self,
        analysis: MarketAnalysis,
        high_temp_f: int,
    ) -> Optional[MarketBracket]:
        if not analysis.brackets:
            return None
        candidates = [b for b in analysis.brackets if b.contains_temp(float(high_temp_f))]
        if not candidates:
            return None
        # Prefer the candidate with strongest modeled probability.
        if analysis.model_probabilities:
            candidates.sort(
                key=lambda b: analysis.model_probabilities.get(b.ticker, 0.0),
                reverse=True,
            )
        return candidates[0]

    def _build_model_read(
        self,
        most_likely_high: int,
        most_likely_bracket: Optional[str],
        bracket_prob: Optional[float],
        analysis: MarketAnalysis,
    ) -> str:
        traj = analysis.trajectory_assessment
        observed = analysis.observation.observed_high_f if analysis.observation else None
        std = analysis.adjusted_forecast_std or analysis.forecast_std
        high_uncertainty = std >= 3.0

        if traj and traj.lock_confidence >= 0.8 and observed is not None:
            return (
                f"Model expects a high near {most_likely_high}°F; "
                f"late-day observations suggest the high may already be set near {observed:.0f}°F."
            )

        if most_likely_bracket and bracket_prob is not None:
            if high_uncertainty:
                return (
                    f"Model leans near {most_likely_high}°F with {most_likely_bracket} as the mode bracket "
                    f"({bracket_prob:.0%}), but uncertainty remains elevated."
                )
            return (
                f"Model leans toward a high around {most_likely_high}°F, "
                f"with {most_likely_bracket} as the mode bracket ({bracket_prob:.0%})."
            )

        if high_uncertainty:
            return (
                f"Forecasts currently center near {most_likely_high}°F, "
                "though uncertainty remains elevated."
            )
        return f"Forecasts and observations point to a likely high around {most_likely_high}°F."

    def generate_outcome_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create advisory summary of most likely final daily high outcome."""
        mean_f = analysis.adjusted_forecast_mean or analysis.forecast_mean
        std_f = analysis.adjusted_forecast_std or analysis.forecast_std
        # Keep this aligned with the value rendered in the forecast panel.
        displayed_final_mean = round(mean_f, 1)

        best_bracket = None
        best_prob = None
        if analysis.brackets and analysis.model_probabilities:
            scored = [
                (b, analysis.model_probabilities.get(b.ticker))
                for b in analysis.brackets
                if analysis.model_probabilities.get(b.ticker) is not None
            ]
            if scored:
                best_bracket, best_prob = max(scored, key=lambda item: item[1])

        # Always derive "Most likely high" from the modeled temperature distribution.
        # Using bracket representatives (especially LESS_THAN/GREATER_THAN) can look
        # inconsistent with mean/range when tails span multiple brackets.
        most_likely_high = self._most_likely_discrete_high(analysis, mean_f, std_f)
        focus_bracket = self._find_bracket_for_discrete_high(analysis, most_likely_high)
        focus_prob = (
            analysis.model_probabilities.get(focus_bracket.ticker)
            if (focus_bracket and analysis.model_probabilities)
            else None
        )
        range_low, range_high = self._get_expected_range(mean_f, std_f)
        effective_floor = self._effective_observation_floor(analysis)
        if effective_floor is not None:
            floor_i = int(math.floor(effective_floor))
            range_low = max(range_low, floor_i)
            if range_high < range_low:
                range_high = range_low

        model_read = self._build_model_read(
            most_likely_high=most_likely_high,
            most_likely_bracket=focus_bracket.subtitle if focus_bracket else (best_bracket.subtitle if best_bracket else None),
            bracket_prob=focus_prob if focus_prob is not None else best_prob,
            analysis=analysis,
        )

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")

        grid.add_row("Most likely exact high (mode):", f"[b]{most_likely_high}°F[/b]")
        grid.add_row("Final model mean:", f"{displayed_final_mean:.1f}°F")
        grid.add_row("Based on:", "Observation-adjusted modeled bracket probabilities")
        if analysis.raw_forecast_mean is not None:
            grid.add_row("Forecast-only mean:", f"{analysis.raw_forecast_mean:.1f}°F")
        grid.add_row("Expected high range:", f"{range_low}° to {range_high}°")
        grid.add_row(
            "Mode bracket:",
            focus_bracket.subtitle if focus_bracket else "N/A",
        )
        grid.add_row(
            "Mode bracket probability:",
            f"{focus_prob:.1%}" if focus_prob is not None else "N/A",
        )
        if best_bracket is not None:
            grid.add_row(
                "Highest-probability bracket:",
                f"{best_bracket.subtitle} ({best_prob:.1%})",
            )

        tomorrow_prediction = "N/A"
        if analysis.tomorrow_date and analysis.tomorrow_forecast_mean is not None:
            tomorrow_prediction = f"{analysis.tomorrow_date}: {analysis.tomorrow_forecast_mean:.1f}°F"
        elif analysis.tomorrow_date:
            tomorrow_prediction = f"{analysis.tomorrow_date}: N/A"
        grid.add_row(
            "[bold bright_magenta]Tomorrow prediction:[/bold bright_magenta]",
            f"[bold bright_magenta]{tomorrow_prediction}[/bold bright_magenta]",
        )

        notes = [f"Model read: {model_read}"]
        if best_bracket is not None and focus_bracket is not None and best_bracket.ticker != focus_bracket.ticker:
            notes.append(
                "Mode vs bracket note: a wide tail bucket like '73° or above' can have higher total probability "
                "than any single exact degree."
            )
        body = Group(grid, Text("\n" + "\n".join(notes), style="dim"))
        return Panel(body, title="Most Likely Outcome", border_style="yellow")

    def generate_bracket_table(self, analysis: MarketAnalysis) -> Panel:
        """Create market brackets table."""
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Bracket")
        table.add_column("Bid/Ask", justify="right")
        table.add_column("Mkt %", justify="right")
        table.add_column("Model Prob %", justify="right")
        
        signal_map = {s.bracket.ticker: s for s in analysis.signals}

        if not analysis.brackets:
            table.add_row(
                "[dim]No market brackets available[/dim]",
                "-",
                "-",
                "-",
                style="dim",
            )
            return Panel(table, title="Market Brackets (0)", border_style="blue")

        for b in analysis.brackets:
            pricing = f"{b.yes_bid}¢ / {b.yes_ask}¢"
            mkt_prob = f"{b.implied_prob:.1%}"
            model_prob = analysis.model_probabilities.get(b.ticker)

            style = "white"
            model_prob_display = f"{model_prob:.1%}" if model_prob is not None else "-"

            if b.ticker in signal_map:
                sig = signal_map[b.ticker]
                style = "bold green" if sig.direction == "BUY" else "bold red"

            table.add_row(
                b.subtitle,
                pricing,
                mkt_prob,
                model_prob_display,
                style=style
            )

        return Panel(table, title=f"Market Brackets ({len(analysis.brackets)})", border_style="blue")

    def _adjust_layout_for_bracket_rows(self, analysis: MarketAnalysis) -> None:
        """
        Expand/shrink bracket panel to fit available rows so all market brackets are
        visible when terminal height permits.
        """
        # Add extra headroom for table header/separator so the last bracket row
        # doesn't get clipped on tighter terminal dimensions.
        bracket_rows_needed = len(analysis.brackets) + 5
        terminal_height = max(20, int(self.console.size.height))
        # Main area excludes header (3) + footer (2)
        available_main = max(8, terminal_height - 5)
        paper_size = 6
        min_signals_rows = 8
        max_bracket_rows = max(6, available_main - paper_size - min_signals_rows)

        desired_rows = max(8, bracket_rows_needed)
        self.layout["brackets"].size = min(desired_rows, max_bracket_rows)

    def generate_signals_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create signals list."""
        if not analysis.signals:
            return Panel(
                Align.center("[dim]No significant trading edges detected[/dim]"),
                title="Trading Signals",
                border_style="white"
            )
            
        table = Table(box=box.ROUNDED, expand=True, show_header=False)
        table.add_column(ratio=1)

        ranked = sorted(analysis.signals, key=lambda s: abs(s.edge), reverse=True)
        shown = ranked[:3]
        hidden_count = max(0, len(ranked) - len(shown))

        for sig in shown:
            direction_color = "green" if sig.direction == "BUY" else "red"
            content = Text()
            content.append(f"{sig.direction} ", style=f"bold {direction_color}")
            content.append(f"{sig.bracket.subtitle}", style="bold white")
            content.append(f"\nEdge: {sig.edge * 100:+.1f}% | Score: {sig.confidence:.0%}")
            reason = sig.reasoning.strip()
            if len(reason) > 92:
                reason = f"{reason[:89]}..."
            content.append(f"\n{reason}", style="dim")
            table.add_row(content)

        body = (
            Group(table, Text(f"+ {hidden_count} more signals hidden for readability", style="dim"))
            if hidden_count > 0
            else table
        )
        return Panel(body, title=f"Signals (Top {len(shown)} / {len(analysis.signals)})", border_style="magenta")

    def generate_final_outcome_focus_box(self, analysis: MarketAnalysis) -> Panel:
        """
        Show decision-engine summary for the current cycle.
        """
        decision = analysis.decision_snapshot
        peak = analysis.peak_prediction
        if decision is None and peak is None:
            return Panel(
                Align.center("[dim]Decision engine has no output yet[/dim]"),
                title="Decision Engine",
                border_style="yellow",
            )

        current_temp = None
        if analysis.observation is not None:
            if analysis.observation.readings:
                current_temp = analysis.observation.readings[-1].reported_temp_f
            else:
                current_temp = analysis.observation.observed_high_f

        predicted_high = peak.predicted_high_f if peak is not None else None
        predicted_conf = peak.confidence if peak is not None else None

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")

        grid.add_row(
            "Current Temp:",
            f"{current_temp:.1f}°F" if current_temp is not None else "N/A",
        )
        if analysis.forecast_mean is not None:
            grid.add_row("Model Mean:", f"{analysis.forecast_mean:.1f}°F")
        else:
            grid.add_row("Model Mean:", "N/A")
        if predicted_high is not None:
            conf_display = f" (Confidence: {predicted_conf:.0%})" if predicted_conf is not None else ""
            grid.add_row(
                "Peak Nowcast:",
                f"{predicted_high:.1f}°F{conf_display}",
            )
        else:
            grid.add_row("Peak Nowcast:", "N/A")

        if decision is not None and decision.ticker is not None:
            grid.add_row(
                "Best Trade:",
                f"{decision.bracket_subtitle or decision.ticker} YES @ {decision.yes_price_cents}c",
            )
            if decision.model_prob is not None:
                grid.add_row("Model Prob:", f"{decision.model_prob:.1%}")
            if decision.edge is not None:
                grid.add_row("Edge:", f"{decision.edge:+.1%}")
            if decision.timing_lag_pp is not None:
                grid.add_row("Market Lag:", f"{decision.timing_lag_pp:+.1f}pp")

        status = "SKIP"
        if decision is not None:
            status = decision.status.upper()
        status_style = "[bold green]TRADE[/bold green]" if status == "TRADE" else "[bold yellow]SKIP[/bold yellow]"
        grid.add_row("Status:", status_style)

        details: List[str] = []
        if decision is not None and decision.reason:
            details.append(f"Reason: {decision.reason}")
        if peak is not None and peak.reasoning:
            details.append(f"Peak model: {peak.reasoning}")
            details.append("Bracket probs use Model Mean/StdDev; Peak Nowcast is a feasibility overlay.")
        body = Group(grid, Text("\n".join(details), style="dim")) if details else grid
        return Panel(body, title="Decision Engine", border_style="yellow")

    def _format_position_price(self, cents: Optional[int]) -> str:
        return f"{cents}c" if cents is not None else "-"

    def _format_entry_display(self, rec: PositionRecommendation) -> str:
        entry = rec.position.average_entry_price_cents
        if entry is None:
            return "-"
        return f"{entry}c avg"

    def _estimate_cost_basis(self, rec: PositionRecommendation) -> Optional[float]:
        entry = rec.position.average_entry_price_cents
        if entry is None:
            return None
        return (float(entry) * rec.position.contracts) / 100.0

    def _format_position_action(self, rec: PositionRecommendation) -> str:
        mapping = {
            "SELL_NOW": "[bold red]SELL NOW[/bold red]",
            "HOLD": "[bold green]HOLD[/bold green]",
            "HOLD_PRIMARY": "[bold cyan]HOLD TO CLOSE[/bold cyan]",
            "HOLD_FOR_TARGET": "[bold yellow]WAIT[/bold yellow]",
            "NO_MODEL": "[dim]NO MODEL[/dim]",
            "NO_QUOTE": "[dim]NO QUOTE[/dim]",
        }
        return mapping.get(rec.action, rec.action)

    def _estimate_pnl_now(self, rec: PositionRecommendation) -> Optional[float]:
        entry = rec.position.average_entry_price_cents
        if entry is None or rec.liquidation_net_cents is None:
            return None
        per_contract = rec.liquidation_net_cents - float(entry)
        return (per_contract * rec.position.contracts) / 100.0

    def _estimate_pnl_settle(self, rec: PositionRecommendation) -> Optional[float]:
        entry = rec.position.average_entry_price_cents
        if entry is None:
            return None
        side = rec.position.side.upper()
        if side == "YES":
            per_contract = 100.0 - float(entry)
        elif side == "NO":
            per_contract = 100.0 - float(entry)
        else:
            return None
        return (per_contract * rec.position.contracts) / 100.0

    def _format_dollar(self, value: Optional[float]) -> str:
        if value is None:
            return "-"
        sign = "+" if value >= 0 else "-"
        return f"{sign}${abs(value):.2f}"

    def _format_money(self, value: Optional[float]) -> str:
        """Format account/notional amounts without P&L sign semantics."""
        if value is None:
            return "-"
        return f"${float(value):.2f}"

    def generate_paper_account_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create a simple paper-account scoreboard panel."""
        paper = analysis.paper_trading_totals or {}
        if not paper:
            return Panel(
                "No paper trades yet",
                title="Paper Account",
                border_style="bright_white",
            )

        pnl = paper.get("total_pnl_dollars")
        return_pct = paper.get("return_pct")
        pnl_text = self._format_dollar(pnl)
        pct_text = "-"
        if return_pct is not None:
            pct_sign = "+" if float(return_pct) >= 0 else "-"
            pct_text = f"{pct_sign}{abs(float(return_pct)) * 100.0:.1f}%"

        status = "FLAT"
        if pnl is not None:
            if float(pnl) > 0:
                status = "WINNING"
            elif float(pnl) < 0:
                status = "LOSING"
        status_text = (
            "[bold green]WINNING[/bold green]"
            if status == "WINNING"
            else ("[bold red]LOSING[/bold red]" if status == "LOSING" else "[bold yellow]FLAT[/bold yellow]")
        )
        border = "green" if status == "WINNING" else ("red" if status == "LOSING" else "yellow")

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        grid.add_row("Start:", self._format_money(paper.get("starting_balance_dollars")))
        grid.add_row("Current Balance:", f"[b]{self._format_money(paper.get('equity_dollars'))}[/b]")
        grid.add_row("P/L:", f"{pnl_text} ({pct_text})")
        grid.add_row("Status:", status_text)
        return Panel(grid, title="Paper Account", border_style=border)

    def generate_positions_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create a simple Kalshi-style open positions panel."""
        rec_by_key = {
            f"{rec.position.ticker}|{rec.position.side.upper()}": rec
            for rec in (analysis.open_positions or [])
        }
        combined_rows: list[tuple[OpenPosition, Optional[PositionRecommendation]]] = []
        seen: set[str] = set()

        def _looks_like_weather_position(pos: OpenPosition, rec: Optional[PositionRecommendation]) -> bool:
            if rec is not None:
                return True
            subtitle = str(pos.subtitle or "").lower()
            ticker = str(pos.ticker or "").upper()
            if "°" in subtitle or " or below" in subtitle or "above " in subtitle:
                return True
            return ticker.startswith("KXHIGH") or ticker.startswith("KXLOW")

        for rec in analysis.open_positions or []:
            key = f"{rec.position.ticker}|{rec.position.side.upper()}"
            if key in seen:
                continue
            combined_rows.append((rec.position, rec))
            seen.add(key)

        for pos in analysis.account_open_positions or []:
            key = f"{pos.ticker}|{pos.side.upper()}"
            if key in seen:
                continue
            rec = rec_by_key.get(key)
            if not _looks_like_weather_position(pos, rec):
                continue
            combined_rows.append((pos, rec))
            seen.add(key)

        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Bracket")
        table.add_column("Side", justify="center")
        table.add_column("Qty", justify="right")
        table.add_column("Entry", justify="right")
        table.add_column("Now", justify="right")
        table.add_column("Target", justify="right")
        table.add_column("Sell", justify="right")

        def _sell_status(rec: Optional[PositionRecommendation]) -> str:
            if rec is None:
                return "WATCH"
            if rec.action == "SELL_NOW":
                return "SELL NOW"
            if rec.action in {"HOLD_FOR_TARGET", "LOCK_PROFIT_PRIMARY"}:
                return "TARGET"
            if rec.action in {"HOLD", "HOLD_PRIMARY"}:
                return "HOLD"
            if rec.action == "NO_MODEL":
                return "NO MODEL"
            if rec.action == "NO_QUOTE":
                return "NO QUOTE"
            return rec.action

        def _fallback_target_cents(
            pos: OpenPosition,
            rec: Optional[PositionRecommendation],
        ) -> Optional[int]:
            if rec and rec.target_exit_price_cents is not None:
                return int(rec.target_exit_price_cents)
            entry = pos.average_entry_price_cents
            if entry is None:
                if rec and rec.liquidation_price_cents is not None:
                    return max(1, min(99, int(rec.liquidation_price_cents) + 1))
                if pos.yes_ask is not None:
                    return max(1, min(99, int(pos.yes_ask) + 1))
                if pos.yes_bid is not None:
                    return max(1, min(99, int(pos.yes_bid) + 1))
                return None
            denom = max(0.01, 1.0 - float(POSITION_EXIT_FEE_RATE))
            required_net = float(entry) + float(AUTO_SELL_MIN_PROFIT_CENTS)
            gross = int(math.ceil(required_net / denom))
            if rec and rec.liquidation_price_cents is not None:
                gross = max(gross, int(rec.liquidation_price_cents))
            return max(1, min(99, gross))

        for pos, rec in combined_rows[:8]:
            label = pos.subtitle or pos.ticker
            entry = f"{int(pos.average_entry_price_cents)}c" if pos.average_entry_price_cents is not None else "-"
            if rec and rec.liquidation_price_cents is not None:
                now = f"{int(rec.liquidation_price_cents)}c"
            else:
                bid = f"{int(pos.yes_bid)}c" if pos.yes_bid is not None else "-"
                ask = f"{int(pos.yes_ask)}c" if pos.yes_ask is not None else "-"
                now = f"{bid}/{ask}"
            target_c = _fallback_target_cents(pos, rec)
            target = f"{int(target_c)}c" if target_c is not None else "-"
            table.add_row(
                label,
                pos.side.upper(),
                str(int(pos.contracts)),
                entry,
                now,
                target,
                _sell_status(rec),
            )

        notes = []
        totals = analysis.portfolio_totals or {}
        account_parts = []
        if totals.get("account_balance_dollars") is not None:
            account_parts.append(f"Balance {self._format_money(totals.get('account_balance_dollars'))}")
        if totals.get("available_to_trade_dollars") is not None:
            account_parts.append(f"Available {self._format_money(totals.get('available_to_trade_dollars'))}")
        if totals.get("portfolio_value_dollars") is not None:
            account_parts.append(f"Portfolio {self._format_money(totals.get('portfolio_value_dollars'))}")
        if account_parts:
            notes.append(" | ".join(account_parts))

        if len(combined_rows) > 8:
            notes.append(f"+{len(combined_rows) - 8} more positions hidden")

        if analysis.auto_trader_events:
            event = analysis.auto_trader_events[0]
            if len(event) > 108:
                event = f"{event[:105]}..."
            notes.append(f"Auto: {event}")

        if not combined_rows:
            notes.append("No open positions")

        body = Group(table, Text("\n".join(notes), style="dim")) if combined_rows else Text("\n".join(notes), style="dim")
        return Panel(body, title="Open Positions", border_style="bright_cyan")

    def update(self, analysis: MarketAnalysis):
        """Update the dashboard with new analysis."""

        self._adjust_layout_for_bracket_rows(analysis)
        self.layout["header"].update(self.generate_header(analysis))
        self.layout["forecasts"].update(self.generate_forecast_table(analysis))
        self.layout["outcome"].update(self.generate_outcome_panel(analysis))
        self.layout["observations"].update(self.generate_observation_panel(analysis))
        self.layout["brackets"].update(self.generate_bracket_table(analysis))
        self.layout["signals"].update(
            Group(
                self.generate_signals_panel(analysis),
                self.generate_final_outcome_focus_box(analysis),
            )
        )
        self.layout["paper"].update(self.generate_paper_account_panel(analysis))
        self.layout["positions"].update(self.generate_positions_panel(analysis))
        
        # Simple footer
        if AUTO_SELL_ENABLED:
            mode = "DRY-RUN auto-sell" if AUTO_SELL_DRY_RUN else "LIVE auto-sell"
            status_text = (
                f"{mode} enabled | lock until {AUTO_SELL_START_HOUR_LOCAL:02d}:{AUTO_SELL_START_MINUTE_LOCAL:02d} "
                f"local | non-primary ambiguity guard {AUTO_SELL_MIN_PRIMARY_GAP_PP:.1f}pp; "
                "primary positions can lock/scalp-exit per risk rules. Press Ctrl+C to exit."
            )
        else:
            status_text = "Advisory only (no auto-trading). Running... Press Ctrl+C to exit."
        self.layout["footer"].update(Panel(Align.center(status_text), style="dim"))
