# Kalshi Weather Bot - Usage & Signals Guide

This guide explains how to use the `kalshi_weather` bot to find trading opportunities in Kalshi's NYC Daily High Temperature markets.

## How to Run

To start the interactive dashboard:

```bash
python -m kalshi_weather run
```

This launches a terminal dashboard that refreshes every minute.

## Dashboard Overview

The dashboard is split into four panels:

1.  **Forecasts**: Shows raw predictions from weather models (Open-Meteo, NWS, etc.) and a "Combined Mean" which is the weighted consensus.
2.  **Observations**: Live temperature data from the KNYC (Central Park) station.
    *   **Observed High**: The highest temp recorded so far today.
    *   **Actual High (Est)**: The estimated true temperature range, accounting for NWS reporting quirks.
3.  **Market Brackets**: Live prices from Kalshi.
    *   **Green/Red Highlights**: Brackets where the bot detects an edge.
    *   **Model %**: The bot's calculated probability for that bracket.
4.  **Signals**: A filtered list of the best trading opportunities.

## Understanding Signals

The bot compares its internal probability model against the market's implied probability (based on price) to find "Edge".

### The Formula
`Edge = Model_Probability - Effective_Cost`

*   **Model_Probability**: Our calculated odds of the bracket winning.
*   **Effective_Cost**: The price you pay (Ask) adjusted for fees.

### Signal Types

#### 1. "YES" Signal (Buy YES)
*   **Meaning**: The market is underpricing this outcome. We think it's more likely to happen than the price suggests.
*   **Action**: Buy "Yes" contracts at the current Ask price.
*   **Example**: 
    > YES 54° to 56°
    > Edge: +15.2% | Conf: 85%
    > Reason: Model (45.0%) > Market Ask (25.0%) + Fees.

#### 2. "NO" Signal (Buy NO / Sell YES)
*   **Meaning**: The market is overpricing this outcome. We think it's unlikely to happen.
*   **Action**: Buy "No" contracts (or sell "Yes" if you hold them).
*   **Example**:
    > NO Above 60°
    > Edge: +10.5% | Conf: 70%
    > Reason: Model NO (99.0%) > Implied Market NO (85.0%) + Fees.

## How to Trade

1.  **Look for High Confirmation**: Focus on signals with:
    *   **Positive Edge (> 8%)**: Enough margin of safety.
    *   **High Confidence (> 70%)**: Means the edge is large and our forecast uncertainty is low.
2.  **Check Time of Day**:
    *   **Before 2 PM**: Signals rely heavily on forecasts. Good for positioning early if models agree.
    *   **After 4 PM**: Signals rely heavily on observations. Very accurate, but prices often move fast.
3.  **Verify Spreads**: The bot uses the "Ask" price for calculations, but always check liquidity on Kalshi before placing large orders.

## Configuration

You can adjust trading thresholds in `.env`:
*   `MIN_EDGE_THRESHOLD`: Minimum edge to trigger a signal (default 0.08 aka 8%).
*   `KALSHI_FEE_RATE`: Fee buffer to account for (default 0.10 aka 10%).
