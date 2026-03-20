# Tasks

- [x] **Phase 1: Core Data Layer**
    - [x] Module 1A: Weather Forecast Fetcher
        - [x] Define interfaces (`TemperatureForecast`)
        - [x] Implement Open-Meteo source
        - [x] Implement NWS source
        - [x] Implement Combined source
    - [ ] Module 1B: Station Observation Parser <!-- id: 1 -->
        - [x] Define interfaces (`StationReading`, `DailyObservation`)
        - [x] Implement NWS Observations API client
        - [x] Implement °C/°F reverse engineering logic
        - [x] Implement daily high tracking
    - [ ] Module 1C: Kalshi Market Client <!-- id: 2 -->
        - [x] Define interfaces (`MarketBracket`)
        - [x] Implement Kalshi Markets API client
        - [x] Implement bracket parsing logic
        - [x] Implement implied probability calculation

- [x] **Phase 2: Probability Engine** <!-- id: 3 -->
    - [x] Module 2A: Forecast Combiner
        - [x] Implement weighted mean calculation
        - [x] Implement combined uncertainty calculation
    - [x] Module 2B: Observation Adjuster
        - [x] Implement time-based weighting
        - [x] Implement station uncertainty adjustment
    - [x] Module 2C: Bracket Probability Calculator
        - [x] Implement probability logic for "between" brackets
        - [x] Implement probability logic for "greater_than" brackets
        - [x] Implement probability logic for "less_than" brackets

- [x] **Phase 3: Edge Detection** <!-- id: 4 -->
    - [x] Module 3A: Edge Calculator
        - [x] Implement edge calculation with fee adjustment
        - [x] Implement signal generation logic
    - [x] Module 3B: Signal Generator
        - [x] Implement filtering and ranking
        - [x] Generate reasoning strings

- [x] **Phase 4: CLI Display** <!-- id: 5 -->
    - [x] Module 4A: Terminal Dashboard
        - [x] Create TUI layout
        - [x] Implement real-time refresh loop
    - [x] Module 4B: Signal Alerts
        - [x] Design alert format
        - [x] Integrate into dashboard

- [x] **Integration & Verification** <!-- id: 6 -->
    - [x] Wire everything together in `main.py`
    - [x] End-to-end testing
