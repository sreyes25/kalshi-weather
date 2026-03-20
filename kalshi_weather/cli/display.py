"""
Terminal Dashboard for Kalshi Weather Bot.

Uses Rich to display real-time analysis, forecasts, and trading signals.
"""

from datetime import datetime
from typing import Optional

from rich.console import Console, Group
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.align import Align
from rich import box

from kalshi_weather.core.models import MarketAnalysis, TradingSignal, MarketBracket

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
            Layout(name="footer", size=3),
        )

        self.layout["main"].split_row(
            Layout(name="left", ratio=1),
            Layout(name="right", ratio=1),
        )

        self.layout["left"].split(
            Layout(name="forecasts", ratio=1),
            Layout(name="outcome", size=9),
            Layout(name="observations", size=14),
        )

        self.layout["right"].split(
            Layout(name="brackets", ratio=1),
            Layout(name="signals", ratio=1),
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
        
        # Individual Forecasts
        for f in analysis.forecasts:
            table.add_row(
                f.source,
                f"{f.forecast_temp_f:.1f}°F",
                f"{f.std_dev:.1f}°F"
            )
            
        table.add_section()
        
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

        # Raw and adjusted model summary
        table.add_row(
            "[b]Forecast-Only Mean[/b]",
            f"{raw_mean:.1f}°F",
            f"{raw_std:.1f}°F",
            style="dim",
        )
        table.add_row(
            "[b]Final Model Mean[/b]",
            f"[b]{adjusted_mean:.1f}°F[/b]",
            f"{adjusted_std:.1f}°F",
            style="yellow"
        )
        delta = adjusted_mean - raw_mean
        table.add_row(
            "Adjustment",
            f"{delta:+.1f}°F",
            "",
            style="magenta" if abs(delta) >= 2.0 else "dim",
        )

        summary_note = None
        mean_gap = abs(adjusted_mean - raw_mean)
        if mean_gap >= 2.0:
            obs = analysis.observation.observed_high_f if analysis.observation else None
            lock = analysis.trajectory_assessment.lock_confidence if analysis.trajectory_assessment else None
            if obs is not None and lock is not None:
                summary_note = (
                    f"Shift driver: observed high so far {obs:.1f}°F with lock confidence "
                    f"{lock:.0%}. Final model favors settlement near observed high."
                )
            else:
                summary_note = (
                    "Final model shifted from forecast-only using live observations/"
                    "trajectory logic. Use Final Model Mean for decision support."
                )

        body = Group(table) if summary_note is None else Group(
            table,
            Text(f"\n{summary_note}", style="dim"),
        )
        return Panel(body, title="Weather Forecasts", border_style="cyan")

    def generate_observation_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create observation summary."""
        if not analysis.observation:
            return Panel("No observation data available", title="Live Observations", border_style="white")
            
        obs = analysis.observation
        
        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        
        grid.add_row("Station:", obs.station_id)
        grid.add_row("Observed High:", f"[b]{obs.observed_high_f:.1f}°F[/b]")
        grid.add_row("Actual High (Est):", f"{obs.possible_actual_high_low:.1f}° - {obs.possible_actual_high_high:.1f}°")
        grid.add_row("Readings:", str(len(obs.readings)))
        
        # Show last reading time and value if available
        if obs.readings:
            last = obs.readings[-1]
            grid.add_row("Last Reading:", f"{last.timestamp.astimezone().strftime('%H:%M')} ({last.reported_temp_f}°F)")

        if analysis.trajectory_assessment:
            traj = analysis.trajectory_assessment
            grid.add_row("High Already Set:", f"{traj.prob_high_already_set:.0%}")
            grid.add_row("Exceed Current High:", f"{traj.prob_exceed_observed_high:.0%}")
            grid.add_row("Trend:", f"{traj.trend_f_per_hour:+.2f}°F/hr")
            grid.add_row("Lock Confidence:", f"{traj.lock_confidence:.0%}")
        
        return Panel(grid, title="Live Observations (KNYC)", border_style="green")

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
                    f"Model leans near {most_likely_high}°F with {most_likely_bracket} most likely "
                    f"({bracket_prob:.0%}), but uncertainty remains elevated."
                )
            return (
                f"Model leans toward a high around {most_likely_high}°F, "
                f"with {most_likely_bracket} currently the most likely bracket ({bracket_prob:.0%})."
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

        most_likely_high = int(round(mean_f))
        range_low, range_high = self._get_expected_range(mean_f, std_f)

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

        model_read = self._build_model_read(
            most_likely_high=most_likely_high,
            most_likely_bracket=best_bracket.subtitle if best_bracket else None,
            bracket_prob=best_prob,
            analysis=analysis,
        )

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")

        grid.add_row("Most likely high:", f"[b]{most_likely_high}°F[/b]")
        grid.add_row("Based on:", "Final model mean")
        if analysis.raw_forecast_mean is not None:
            grid.add_row("Forecast-only mean:", f"{analysis.raw_forecast_mean:.1f}°F")
        grid.add_row("Expected high range:", f"{range_low}° to {range_high}°")
        grid.add_row(
            "Most likely bracket:",
            best_bracket.subtitle if best_bracket else "N/A",
        )
        grid.add_row(
            "Bracket probability:",
            f"{best_prob:.1%}" if best_prob is not None else "N/A",
        )

        tomorrow_prediction = "N/A"
        if analysis.tomorrow_date and analysis.tomorrow_forecast_mean is not None:
            tomorrow_prediction = f"{analysis.tomorrow_date}: {analysis.tomorrow_forecast_mean:.1f}°F"
        elif analysis.tomorrow_date:
            tomorrow_prediction = f"{analysis.tomorrow_date}: N/A"
        grid.add_row(
            "[magenta]Tomorrow prediction:[/magenta]",
            f"[magenta]{tomorrow_prediction}[/magenta]",
        )

        body = Group(
            grid,
            Text(f"\nModel read: {model_read}", style="dim"),
        )
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
            return Panel(table, title="Market Brackets", border_style="blue")

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
            
        return Panel(table, title="Market Brackets", border_style="blue")

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
        
        for sig in analysis.signals:
            direction_color = "green" if sig.direction == "BUY" else "red"
            
            # Construct a rich text summary
            content = Text()
            content.append(f"{sig.direction} ", style=f"bold {direction_color}")
            content.append(f"{sig.bracket.subtitle}", style="bold white")
            content.append(f"\nEdge: {sig.edge * 100:+.1f}% | Score: {sig.confidence:.0%}")
            content.append(f"\n{sig.reasoning}", style="dim")
            
            table.add_row(content)
            
        return Panel(table, title=f"Signals ({len(analysis.signals)})", border_style="magenta")

    def update(self, analysis: MarketAnalysis):
        """Update the dashboard with new analysis."""
        
        self.layout["header"].update(self.generate_header(analysis))
        self.layout["forecasts"].update(self.generate_forecast_table(analysis))
        self.layout["outcome"].update(self.generate_outcome_panel(analysis))
        self.layout["observations"].update(self.generate_observation_panel(analysis))
        self.layout["brackets"].update(self.generate_bracket_table(analysis))
        self.layout["signals"].update(self.generate_signals_panel(analysis))
        
        # Simple footer
        status_text = "Advisory only (no auto-trading). Running... Press Ctrl+C to exit."
        self.layout["footer"].update(Panel(Align.center(status_text), style="dim"))
