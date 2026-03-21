"""
Terminal Dashboard for Kalshi Weather Bot.

Uses Rich to display real-time analysis, forecasts, and trading signals.
"""

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
)
from kalshi_weather.config.settings import (
    AUTO_SELL_ENABLED,
    AUTO_SELL_DRY_RUN,
    AUTO_SELL_START_HOUR_LOCAL,
    AUTO_SELL_START_MINUTE_LOCAL,
    AUTO_SELL_MIN_PRIMARY_GAP_PP,
)

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
            Layout(name="outcome", size=9),
            Layout(name="observations", ratio=1),
        )

        self.layout["right"].split(
            Layout(name="brackets", size=10),
            Layout(name="signals", ratio=1),
            Layout(name="positions", size=12),
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
            style="dim",
        )
        table.add_section()

        # Individual Forecasts
        for f in analysis.forecasts:
            table.add_row(
                f.source,
                f"{f.forecast_temp_f:.1f}°F",
                f"{f.std_dev:.1f}°F",
                style="dim",
            )
        table.add_section()

        # Final model callout (placed below sources)
        table.add_row(
            "[b]Final Model Mean[/b]",
            f"[b]{adjusted_mean:.1f}°F[/b]",
            f"[b]{adjusted_std:.1f}°F[/b]",
            style="bold black on bright_yellow",
        )
        delta = adjusted_mean - raw_mean
        table.add_row(
            "Adjustment",
            f"{delta:+.1f}°F",
            "",
            style="dim",
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
        # Keep this aligned with the value rendered in the forecast panel.
        displayed_final_mean = round(mean_f, 1)

        most_likely_high = int(round(displayed_final_mean))
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
        grid.add_row("Final model mean:", f"{displayed_final_mean:.1f}°F")
        grid.add_row("Based on:", "Final model mean (same display value)")
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
        Show the bracket most aligned with final expected outcome.
        """
        if not analysis.brackets or not analysis.model_probabilities:
            return Panel(
                Align.center("[dim]No final-outcome focus available right now[/dim]"),
                title="Final Outcome Focus",
                border_style="yellow",
            )

        scored = [
            (b, analysis.model_probabilities.get(b.ticker))
            for b in analysis.brackets
            if analysis.model_probabilities.get(b.ticker) is not None
        ]
        if not scored:
            return Panel(
                Align.center("[dim]No modeled bracket probabilities available[/dim]"),
                title="Final Outcome Focus",
                border_style="yellow",
            )
        scored.sort(key=lambda item: item[1], reverse=True)
        top_bracket, top_prob = scored[0]
        top_signal = next((s for s in analysis.signals if s.bracket.ticker == top_bracket.ticker), None)
        mkt_prob_c = top_bracket.implied_prob * 100.0
        model_prob_c = (top_prob or 0.0) * 100.0
        edge_c = ((top_prob or 0.0) - top_bracket.implied_prob) * 100.0
        entry_price = top_bracket.yes_ask

        grid = Table.grid(expand=True)
        grid.add_column()
        grid.add_column(justify="right")
        grid.add_row("Most likely bracket:", f"[b]{top_bracket.subtitle}[/b]")
        grid.add_row("Entry ask:", f"{entry_price}c")
        grid.add_row(
            "Model / Market / Edge:",
            f"{model_prob_c:.1f}% / {mkt_prob_c:.1f}% / {edge_c:+.1f}%",
        )
        if top_signal:
            grid.add_row("Signal:", f"{top_signal.direction} ({top_signal.confidence:.0%})")

        extra = []
        for candidate, prob in scored[1:3]:
            extra.append(
                f"• {candidate.subtitle}: ask {candidate.yes_ask}c | "
                f"m {((prob or 0.0) * 100):.1f}% vs mkt {(candidate.implied_prob * 100):.1f}%"
            )

        body = (
            Group(grid, Text("\n" + "\n".join(extra), style="dim"))
            if extra
            else grid
        )
        return Panel(body, title="Final Outcome Focus", border_style="yellow")

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

    def generate_positions_panel(self, analysis: MarketAnalysis) -> Panel:
        """Create open positions panel with deterministic sell guidance."""
        totals = analysis.portfolio_totals or {}
        totals_lines = []
        if totals:
            realized = totals.get("realized_pnl_dollars")
            fees = totals.get("fees_paid_dollars")
            net = totals.get("net_realized_after_fees_dollars")
            traded = totals.get("total_traded_dollars")
            markets = totals.get("markets_count")
            totals_lines.append("P&L Summary:")
            totals_lines.append(f"• Realized: {self._format_dollar(realized)}")
            totals_lines.append(f"• Fees: {self._format_dollar(-abs(fees) if fees is not None else None)}")
            totals_lines.append(f"• Net After Fees: {self._format_dollar(net)}")
            totals_lines.append(f"• Traded: {self._format_dollar(traded)}")
            if markets is not None:
                totals_lines.append(f"• Markets: {int(markets)}")

        if not analysis.open_positions:
            if totals_lines:
                return Panel(
                    Text("\n".join(totals_lines), style="dim"),
                    title="Open Positions",
                    border_style="bright_cyan",
                )
            return Panel(
                Align.center("[dim]No open positions found (or auth unavailable)[/dim]"),
                title="Open Positions",
                border_style="white",
            )

        table = Table(box=box.SIMPLE, expand=True)
        table.add_column("Market")
        table.add_column("Side", justify="center")
        table.add_column("Qty", justify="right")
        table.add_column("Avg Entry", justify="right")
        table.add_column("Basis", justify="right")
        table.add_column("Now", justify="right")
        table.add_column("Model", justify="right")
        table.add_column("Trend", justify="right")
        table.add_column("P&L Now", justify="right")
        table.add_column("P&L If Settle", justify="right")
        table.add_column("Target", justify="right")
        table.add_column("Action", justify="center")

        for rec in analysis.open_positions:
            market_label = rec.position.subtitle or rec.position.ticker
            now_px = self._format_position_price(rec.liquidation_price_cents)
            if rec.liquidation_net_cents is not None and rec.liquidation_price_cents is not None:
                now_px = f"{rec.liquidation_price_cents}c ({rec.liquidation_net_cents:.1f}c net)"
            model_fair = (
                f"{rec.fair_value_cents:.1f}c" if rec.fair_value_cents is not None else "-"
            )
            trend_pp = (
                f"{rec.side_probability_change_pp:+.1f}pp"
                if rec.side_probability_change_pp is not None
                else "-"
            )
            target = self._format_position_price(rec.target_exit_price_cents)
            pnl_now = self._format_dollar(self._estimate_pnl_now(rec))
            pnl_settle = self._format_dollar(self._estimate_pnl_settle(rec))
            cost_basis = self._format_dollar(self._estimate_cost_basis(rec))
            table.add_row(
                market_label,
                rec.position.side,
                str(rec.position.contracts),
                self._format_entry_display(rec),
                cost_basis,
                now_px,
                model_fair,
                trend_pp,
                pnl_now,
                pnl_settle,
                target,
                self._format_position_action(rec),
            )

        notes = []
        if totals_lines:
            notes.extend(totals_lines)
            notes.append("")
        notes.append("• Entry shown as weighted average for each ticker+side (multiple buys are blended).")
        for rec in analysis.open_positions[:2]:
            if rec.rationale:
                notes.append(f"• {rec.position.ticker}: {rec.rationale}")
        if analysis.auto_trader_events:
            notes.append("")
            notes.append("Auto-trader:")
            for event in analysis.auto_trader_events[:3]:
                notes.append(f"• {event}")
        if analysis.position_lifecycle_events:
            notes.append("")
            notes.append("Position updates:")
            for event in analysis.position_lifecycle_events[:3]:
                notes.append(f"• {event}")

        body = Group(table, Text("\n".join(notes), style="dim")) if notes else table
        return Panel(body, title="Open Positions", border_style="bright_cyan")

    def update(self, analysis: MarketAnalysis):
        """Update the dashboard with new analysis."""
        
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
        self.layout["positions"].update(self.generate_positions_panel(analysis))
        
        # Simple footer
        if AUTO_SELL_ENABLED:
            mode = "DRY-RUN auto-sell" if AUTO_SELL_DRY_RUN else "LIVE auto-sell"
            status_text = (
                f"{mode} enabled | lock until {AUTO_SELL_START_HOUR_LOCAL:02d}:{AUTO_SELL_START_MINUTE_LOCAL:02d} "
                f"local | sell only non-primary brackets; ambiguity guard {AUTO_SELL_MIN_PRIMARY_GAP_PP:.1f}pp; "
                "hold primary to settlement. Press Ctrl+C to exit."
            )
        else:
            status_text = "Advisory only (no auto-trading). Running... Press Ctrl+C to exit."
        self.layout["footer"].update(Panel(Align.center(status_text), style="dim"))
