"""AC-W-03: every fill-rule id in docs/fill_rules.md has a test of the same name.

Also checks the reverse (every test in fill_rules.rs is listed) and that at
least 15 hand-computed scenario files exist. Exits non-zero on any gap.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]

# The 28 names fixed by spec section 11.
SPEC_RULES = {
    "crossing_limit_fills_at_best_and_walks_levels",
    "crossing_partial_then_rests_remainder",
    "ioc_remainder_cancelled",
    "fok_rejected_if_not_fully_fillable",
    "market_order_walks_book_taker_fee",
    "post_only_rejected_when_crossing",
    "consumed_liquidity_not_refilled_until_level_update",
    "order_not_fillable_before_arrival",
    "passive_fill_requires_queue_ahead_exhausted",
    "passive_partial_fill_when_trade_smaller_than_leaves",
    "trade_through_fills_resting_order",
    "book_cross_fills_resting_order_at_own_price",
    "unknown_side_trade_does_not_advance_queue",
    "level_increase_goes_behind",
    "l2_risk_averse_cancel_does_not_advance",
    "l2_prob_power_cancel_advances_expected_fraction",
    "l2_prob_log_cancel_advances_expected_fraction",
    "l3_cancel_ahead_advances_queue",
    "l3_cancel_behind_does_not_advance",
    "l3_replace_loses_priority",
    "l3_partial_cancel_keeps_priority",
    "modify_is_cancel_replace_and_loses_priority",
    "cancel_in_flight_can_still_fill",
    "maker_rebate_is_negative_fee",
    "taker_fee_on_aggressive_fill",
    "fees_use_point_in_time_schedule",
    "self_trade_prevention_cancel_newest",
    "no_fill_while_book_invalid",
}


def main() -> int:
    """Compare the rule ids in the docs, the spec list and the test names."""
    doc = (REPO / "docs" / "fill_rules.md").read_text(encoding="utf-8")
    documented = set(re.findall(r"^\| `([a-z0-9_]+)` \|", doc, flags=re.M))
    tests_src = (REPO / "crates" / "edbt-exchange" / "tests" / "fill_rules.rs").read_text(encoding="utf-8")
    tests = set(re.findall(r"#\[test\]\s*\n\s*fn ([a-z0-9_]+)\(", tests_src))
    scenarios = sorted((REPO / "tests" / "fixtures" / "scenarios").glob("*.toml"))
    problems = []
    for name in sorted(SPEC_RULES - documented):
        problems.append(f"spec rule not in docs/fill_rules.md: {name}")
    for name in sorted(documented - tests):
        problems.append(f"documented rule without a test: {name}")
    for name in sorted(tests - documented):
        problems.append(f"test not listed in docs/fill_rules.md: {name}")
    if len(scenarios) < 15:
        problems.append(f"only {len(scenarios)} scenario files (need >= 15)")
    for p in problems:
        print(p)
    print(f"rules documented={len(documented)} tests={len(tests)} spec={len(SPEC_RULES)}")
    print(f"scenarios={len(scenarios)}")
    print("AC-W-03 coverage: " + ("PASS" if not problems else "FAIL"))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
