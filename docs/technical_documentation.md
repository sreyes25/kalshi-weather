# Kalshi Weather Bot - Technical Reference

This document details the complete implementation of the Kalshi Weather Bot (v0.1.0).

## 1. Architecture Overview

The system follows a modular pipeline architecture designed for clarity and testability:

1.  **Data Layer**: Fetches raw data from external APIs (Weather, Stations, Kalshi).
2.  **Core Core**: Normalizes data into shared domain models.
3.  **Probability Engine**: Transforms raw data into probability distributions.
4.  **Edge Detector**: Compares models against market prices to find value.
5.  **CLI**: Visualizes the system state and signals.

### Directory Structure

*   `kalshi_weather/core`: Data models and abstract base classes.
*   `kalshi_weather/data`: API clients (Weather, Stations, Markets).
*   `kalshi_weather/engine`: Business logic (Probability, Edge Detection).
*   `kalshi_weather/cli`: Terminal interface and runner.

## 2. Core Data Models (`core/models.py`)

*   **`TemperatureForecast`**: Standardized forecast object (source, temp, std_dev).
*   **`StationReading`**: A single observation point. Contains both reported °F/°C and calculated uncertainty bounds.
*   **`DailyObservation`**: Aggregated daily stats (Observed High so far).
*   **`MarketBracket`**: Represents a betting option (e.g., "54° to 56°").
*   **`TradingSignal`**: A detected opportunity with direction, edge, and confidence.

## 3. Data Layer

### Weather (`data/weather.py`)
Fetches forecasts from multiple sources using **Open-Meteo**:
1.  **Best Match**: Uses Open-Meteo's intelligent model selection.
2.  **GFS**: Global Forecast System (US).
3.  **Ensemble**: 51-member ensemble used to calculate baseline uncertainty (`std_dev`).
4.  **NWS**: National Weather Service Point Forecast (Official source).

### Stations (`data/stations.py`)
Parses real-time data from NWS API for **KNYC (Central Park)**.
*   **Challenge**: 5-minute stations often report rounded Celsius.
*   **Solution**: Reverse-engineers the possible Fahrenheit range.
    *   Example: 20.0°C -> 68.0°F (Exact)
    *   Uncertainty: If reported as 20°C, it could be 19.5°C to 20.5°C.
    *   We track `possible_actual_f_low` and `possible_actual_f_high`.

## 4. Probability Engine (`engine/probability.py`)

### Module 2A: Forecast Combiner
Combines multiple forecasts into a single Gaussian distribution (`CombinedForecast`).
*   **Math**: Weighted Mean.
*   **Variance**: `Combined Var = Pooled Var (uncertainty) + Disagreement Var (spread)`.
*   Effect: If models disagree (e.g., GFS says 50, ECMWF says 60), uncertainty increases significantly.

### Module 2B: Observation Adjuster
Adjusts the forecast based on time-of-day and live observations.
*   **Before 2 PM**: Relies mostly on Forecast.
*   **After 4 PM**: Relies mostly on Observed High.
*   **Logic**:
    *   Blends `Forecast Mean` and `Observed High` weights based on time.
    *   **Constraints**: The daily high *cannot* be lower than the current observed high. The distribution is truncated/shifted.
    *   Uncertainty shrinks as the day progresses (we know more).

### Module 2C: Bracket Calculator
Calculates `P(Bracket Win)` using the Cumulative Distribution Function (CDF) of the Normal Distribution.
*   **Between [A, B]**: `CDF(B + 0.5) - CDF(A - 0.5)`
*   **Greater Than [A]**: `1 - CDF(A + 0.5)`
*   **Less Than [B]**: `CDF(B - 0.5)`
*   *Note: +/- 0.5 adjustment handles integer settlement vs continuous distribution.*

## 5. Edge Detection (`engine/edge_detector.py`)

Identifies trading value by comparing our Model Probability vs Market Implied Probability.

### Algorithm
1.  **Calculate Edge**: `Edge = Model_Prob - Effective_Price`
    *   `Effective_Price` accounts for the `KALSHI_FEE_RATE` (default 10%).
    *   Consider this a "Break-even Probability".
2.  **Filter**:
    *   Must be > `MIN_EDGE_THRESHOLD` (8%).
3.  **Confidence Score**:
    *   Higher Edge = Higher Score.
    *   Lower Uncertainty (StdDev) = Higher Score.

## 6. CLI Dashboard (`cli/display.py`)

Built with `rich` library.
*   **Runner (`bot.py`)**: Executes the loop every ~60s.
*   **Refresh**: Fetches new data -> Runs Analysis -> Updates UI.
