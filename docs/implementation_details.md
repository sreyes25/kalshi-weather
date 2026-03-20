# Kalshi Weather Bot - Implementation Details

## Project Overview

This project is a high-frequency trading bot designed to predict the daily high temperature in New York City (Central Park - KNYC) and identify trading edge in Kalshi's weather markets. It combines multiple weather forecasts, adjusts for real-time station observations, and calculates the probability of settlement falling into specific Kalshi brackets.

## Architecture

The system is divided into three main layers:
1.  **Core Data Layer**: Fetches and normalizes data from external APIs (Weather Models, NWS Stations, Kalshi Markets).
2.  **Probability Engine**: Processes raw data into probability distributions and calculates trading edge.
3.  **CLI/Interface** (Pending): Visualizes data and signals for the user.

## 1. Core Data Layer (`kalshi_weather/data`)

### Weather Forecasts (`weather.py`)
Responsible for fetching and normalizing forecast data from multiple models.
-   **Sources**:
    -   **Open-Meteo**: Provides "Best Match", "GFS", and "Ensemble" models.
    -   **NWS**: Digital Forecast Database (NDFD) point forecast.
-   **Normalization**: All forecasts are converted to `TemperatureForecast` objects containing the predicted high, standard deviation (uncertainty), and source metadata.
-   **Ensemble Logic**: For ensemble sources, it calculates the mean and standard deviation across all member runs to better estimate uncertainty.

### Station Observations (`stations.py`)
Parses real-time observation data from the NWS API (KNYC Station).
-   **Station Types**:
    -   **Hourly**: Standard METAR stations (low frequency).
    -   **5-Minute**: High-frequency stations.
-   **Uncertainty Handling**:
    -   **°C/°F Conversion**: 5-minute stations often report rounded Celsius values. The system reverse-engineers the possible Fahrenheit range (e.g., 20.0°C could be 68.0°F +/- 0.1°F error margin).
    -   `calculate_temp_bounds`: precise bounds are calculated to account for this quantization noise.
-   **Daily Summary**: Aggregates high temperatures observed so far today (`observed_high_f`).

### Market Data (`markets.py`)
Interacts with the Kalshi API to fetch market state.
-   **Bracket Parsing**: Converts human-readable subtitles into structured logic:
    -   `BETWEEN`: "54° to 56°" -> `lower=54, upper=56`
    -   `GREATER_THAN`: "Above 56°" -> `lower=56` (strictly >)
    -   `LESS_THAN`: "Below 50°" -> `upper=50` (strictly <)
-   **Implied Probability**: Calculates probability from the midpoint of Bid/Ask prices (`(bid + ask) / 200`).

## 2. Probability Engine (`kalshi_weather/engine`)

### Forecast Combiner (`probability.py` - Module 2A)
Combines multiple individual forecasts into a single "Combined Forecast" distribution.
-   **Algorithm**: Weighted Mean approximation of a Gaussian Mixture.
-   **Variance Calculation**:
    -   **Pooled Variance**: Weighted average of individual model uncertainties.
    -   **Disagreement Variance**: Variance arising from the spread of the different model means.
    -   `Combined Variance = Pooled Variance + Disagreement Variance`
-   **Safety**: Applies a minimum standard deviation floor (`MIN_STD_DEV`) to prevent overconfidence.

### Observation Adjuster (`probability.py` - Module 2B)
Refines the forecast based on live temperature readings throughout the day.
-   **Time-Based Weighting**:
    -   **Before 2 PM**: Relies almost exclusively on the Forecast.
    -   **2 PM - 4 PM**: Linearly blends Forecast and Observation.
    -   **After 4 PM**: Relies heavily on the Observed High (settlement is imminent).
-   **Adjustment Logic**:
    -   The distribution is shifted towards the `observed_high_f`.
    -   The uncertainty (`std_dev`) shrinks as the day progresses.
    -   **Constraints**: The predicted daily high cannot be lower than the `observed_high_f`.

### Bracket Probability Calculator (`probability.py` - Module 2C)
Maps the final temperature distribution (Normal Distribution) to specific Kalshi brackets.
-   **Math**: Uses the Error Function (`erf`) to calculate the Cumulative Distribution Function (CDF).
-   **Formulas**:
    -   `BETWEEN`: `CDF(upper + 0.5) - CDF(lower - 0.5)`
    -   `GREATER_THAN`: `1.0 - CDF(threshold + 0.5)`
    -   `LESS_THAN`: `CDF(threshold - 0.5)`
    -   *Note: +/- 0.5 adjustment handles the discrete nature of integer settlement temperatures.*

## 3. Configuration & Core Models

-   **`core/models.py`**: Defines the shared data structures (`TemperatureForecast`, `StationReading`, `MarketBracket`, `TradingSignal`).
-   **`config/settings.py`**: Central configuration for API keys (e.g., Kalshi), URLs, and logic constants (e.g., `MIN_EDGE_THRESHOLD`).

## Current Implementation Status
-   [x] **Phase 1 (Data Layer)**: Complete. Can fetch Weather, Station, and Market data.
-   [x] **Phase 2 (Probability Engine)**: Complete. Can combine forecasts, adjust for observations, and calculate bracket odds.
-   [ ] **Phase 3 (Edge Detection)**: Pending. Needs logic to compare Model Prob vs Market Prob and filter for value.
-   [ ] **Phase 4 (CLI)**: Pending. Needs a TUI/Dashboard to run the loop.
