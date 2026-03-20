"""
Terminal Dashboard for Kalshi Weather Bot.

Uses Rich to display real-time analysis, forecasts, and trading signals.
"""

from datetime import datetime
from typing import Optional

from rich.console import Console
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
        
        # Combined
        table.add_row(
            "[b]Combined Mean[/b]",
            f"[b]{analysis.forecast_mean:.1f}°F[/b]",
            f"{analysis.forecast_std:.1f}°F",
            style="yellow"
        )
        
        return Panel(table, title="Weather Forecasts", border_style="cyan")

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

    def generate_bracket_table(self, analysis: MarketAnalysis) -> Panel:
        """Create market brackets table."""
        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Bracket")
        table.add_column("Bid/Ask", justify="right")
        table.add_column("Mkt %", justify="right")
        table.add_column("Model %", justify="right")
        
        signal_map = {s.bracket.ticker: s for s in analysis.signals}

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
            content.append(f"\nEdge: {sig.edge * 100:+.1f}% | Conf: {sig.confidence:.0%}")
            content.append(f"\n{sig.reasoning}", style="dim")
            
            table.add_row(content)
            
        return Panel(table, title=f"Signals ({len(analysis.signals)})", border_style="magenta")

    def update(self, analysis: MarketAnalysis):
        """Update the dashboard with new analysis."""
        
        self.layout["header"].update(self.generate_header(analysis))
        self.layout["forecasts"].update(self.generate_forecast_table(analysis))
        self.layout["observations"].update(self.generate_observation_panel(analysis))
        self.layout["brackets"].update(self.generate_bracket_table(analysis))
        self.layout["signals"].update(self.generate_signals_panel(analysis))
        
        # Simple footer
        status_text = "Advisory only (no auto-trading). Running... Press Ctrl+C to exit."
        self.layout["footer"].update(Panel(Align.center(status_text), style="dim"))
