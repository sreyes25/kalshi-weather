#!/usr/bin/env python3
"""
Live test script for Kalshi API client.

Run: python scripts/test_kalshi_live.py
"""

import sys
sys.path.insert(0, '.')

from src.data.kalshi_client import KalshiMarketClient, get_market_summary


def main():
    print("=" * 60)
    print("KALSHI API CLIENT - LIVE TEST")
    print("=" * 60)

    client = KalshiMarketClient()

    # Test 1: API Status
    print("\n[1] API Status")
    status = client.get_market_status()
    print(f"    API Available: {status.get('api_available')}")
    print(f"    Markets Found: {status.get('markets_found')}")

    if not status.get('api_available'):
        print(f"    Error: {status.get('error')}")
        return 1

    # Test 2: Available Dates
    print("\n[2] Available Dates")
    dates = client.get_available_dates()
    if not dates:
        print("    No open markets found")
        return 1
    print(f"    Found {len(dates)} dates: {', '.join(dates[:5])}")

    # Test 3: Fetch Brackets
    target_date = dates[0]
    print(f"\n[3] Brackets for {target_date}")
    brackets = client.fetch_brackets(target_date)
    print(f"    Found {len(brackets)} brackets\n")

    print("    Bracket              Type           Bounds       Bid   Ask   Prob    Volume")
    print("    " + "-" * 75)
    for b in brackets:
        bounds = f"{b.lower_bound or ''}-{b.upper_bound or ''}".strip('-')
        print(f"    {b.subtitle:<20} {b.bracket_type.value:<14} {bounds:<12} "
              f"{b.yes_bid:>3}¢  {b.yes_ask:>3}¢  {b.implied_prob:>5.1%}  {b.volume:>6}")

    # Test 4: Probability Sum
    print("\n[4] Probability Check")
    total_prob = sum(b.implied_prob for b in brackets)
    print(f"    Sum of probabilities: {total_prob:.1%} (should be ~100%)")

    # Test 5: Boundary Logic
    print("\n[5] Boundary Logic Test")
    # Get temp range from actual brackets
    all_bounds = []
    for b in brackets:
        if b.lower_bound is not None:
            all_bounds.append(int(b.lower_bound))
        if b.upper_bound is not None:
            all_bounds.append(int(b.upper_bound))
    if all_bounds:
        min_t, max_t = min(all_bounds) - 1, max(all_bounds) + 1
        test_temps = [min_t, min_t + 1, (min_t + max_t) // 2, max_t - 1, max_t]
    else:
        test_temps = [50, 55, 60]
    for temp in test_temps:
        winners = [b.subtitle for b in brackets if b.contains_temp(temp)]
        print(f"    {temp}°F → {winners[0] if winners else 'None'}")

    # Test 6: Market Summary
    print(f"\n[6] Market Summary")
    summary = get_market_summary(target_date)
    print(f"    Total Volume: {summary['total_volume']:,} contracts")
    print(f"    Avg Spread: {summary['avg_spread_cents']:.1f}¢")

    print("\n" + "=" * 60)
    print("ALL TESTS PASSED")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
