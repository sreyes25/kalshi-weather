# Implementation Plan - Kalshi Weather Bot

To build a bot for predicting and trading on Kalshi's NYC High Temperature (NHIGH) contract.

## User Review Required

> [!IMPORTANT]
> **Station Logic**: The reverse-engineering of °C to °F for 5-minute stations is critical and relies on specific NWS reporting behaviors. This needs careful testing against known data.

> [!WARNING]
> **Settlement Source**: We must strictly adhere to the NWS Daily Climate Report (CLI) as the source of truth, not the real-time observations, although real-time observations are used for intraday prediction.

## Proposed Changes

### Phase 1: Core Data Layer

#### [MODIFY] [src/data/station_parser.py](file:///Users/srinathsrinivasan/Desktop/repos/kalshi-weather-bot/src/data/station_parser.py)
**Module 1B: Station Observation Parser**
- **Purpose**: Parse real-time NWS observations with conversion handling.
- **Data Source**: NWS API (`api.weather.gov/stations/KNYC/observations`)
- **Key Logic**:
    - Identify station type (hourly vs 5-minute).
    - Reverse-engineer °C -> °F for 5-minute stations to find possible actual Fahrenheit values.
    - Track observed high so far.
    - Calculate uncertainty bounds (possible actual high/low).

#### [MODIFY] [src/data/kalshi_client.py](file:///Users/srinathsrinivasan/Desktop/repos/kalshi-weather-bot/src/data/kalshi_client.py)
**Module 1C: Kalshi Market Client**
- **Purpose**: Fetch and parse Kalshi market data.
- **Data Source**: Kalshi API (`api.elections.kalshi.com/trade-api/v2/markets`)
- **Key Logic**:
    - Fetch markets for series `KXHIGHNY` focused on the target date.
    - Parse bracket subtitles into structured data:
        - "54° to 56°" -> `BETWEEN`
        - "Above 56°" -> `GREATER_THAN`
        - "Below 50°" -> `LESS_THAN`
    - Calculate implied probability from bid/ask mid.

### Phase 2: Probability Engine

#### [NEW] [src/engine/probability.py](file:///Users/srinathsrinivasan/Desktop/repos/kalshi-weather-bot/src/engine/probability.py)
**Module 2A: Forecast Combiner**
- **Logic**:
    - Weight forecasts (NWS > ECMWF > GFS/HRRR > Ensemble).
    - Calculate weighted mean.
    - Calculate combined uncertainty (std dev) including both individual variance and disagreement variance.
    - Enforce a minimum standard deviation floor.

**Module 2B: Observation Adjuster**
- **Logic**:
    - Adjust probability based on time of day.
    - Before 2 PM: Rely mostly on forecast.
    - After 4 PM: Rely mostly on observed high.
    - Blend based on `hours_since_noon`.
    - Expand uncertainty bounds based on station data precision.

**Module 2C: Bracket Probability Calculator**
- **Logic**:
    - specific formulas for each bracket type using the calculated normal distribution (CDF).
    - `BETWEEN`: `CDF(upper + 0.5) - CDF(lower - 0.5)`
    - `GREATER_THAN`: `1 - CDF(threshold + 0.5)`
    - `LESS_THAN`: `CDF(threshold - 0.5)`

### Phase 3: Edge Detection

#### [NEW] [src/engine/edge_detector.py](file:///Users/srinathsrinivasan/Desktop/repos/kalshi-weather-bot/src/engine/edge_detector.py)
**Module 3A & 3B: Edge Calculator & Signal Generator**
- **Logic**:
    - effective_price = price + fees (approximate).
    - Compare `model_prob` vs `market_prob`.
    - Threshold: > 8% edge.
    - Generate human-readable reasoning (e.g., "Model mean 53F is in 52-54 bucket, market underpricing").

### Phase 4: CLI Display

#### [NEW] [src/cli/display.py](file:///Users/srinathsrinivasan/Desktop/repos/kalshi-weather-bot/src/cli/display.py)
**Module 4A: Terminal Dashboard**
- **Layout**: Rich TUI or standard text loop.
- Sections for Forecasts, Observations, Market Analysis, and Signals.

#### [NEW] [src/cli/main.py](file:///Users/srinathsrinivasan/Desktop/repos/kalshi-weather-bot/src/cli/main.py)
- **Logic**: Main loop running every ~60 seconds.

## Verification Plan

### Automated Tests
- `pytest` for all modules.
- Specific tests for:
    - Temperature conversion logic (Module 1B).
    - Bracket parsing (Module 1C).
    - Probability math (Module 2).
    - Edge calculation (Module 3).

### Manual Verification
- Run the CLI and verify against:
    - Live NWS website data.
    - Live Kalshi website prices.
