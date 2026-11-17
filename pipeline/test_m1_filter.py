"""
End-to-End Verification Suite for Module M1 (`pipeline/test_m1_filter.py`).
"""
from __future__ import annotations

import sys

from pipeline.m1_filter import INPUT_M0_POOL_PATH, M1SmartFilterEngine, OUTPUT_M1_PATH
from pipeline.schemas_m1 import M1QualifiedSKUsOutput


def run_verification() -> None:
    print("=== [TEST M1.1] Running M1 Smart Filter & Multi-Brand Ranker ===", flush=True)
    assert INPUT_M0_POOL_PATH.exists(), f"Missing M0 input file: {INPUT_M0_POOL_PATH}"

    engine = M1SmartFilterEngine()
    engine.run(input_path=INPUT_M0_POOL_PATH, output_path=OUTPUT_M1_PATH)

    print("\n=== [TEST M1.2] Validating Output Data Contract (data/m1_qualified_skus.json) ===", flush=True)
    assert OUTPUT_M1_PATH.exists(), f"Missing M1 output file: {OUTPUT_M1_PATH}"
    reloaded = M1QualifiedSKUsOutput.model_validate_json(
        OUTPUT_M1_PATH.read_text(encoding="utf-8")
    )

    print(f"  M1 Run ID        : {reloaded.m1_run_id}", flush=True)
    print(f"  Source M0 Run ID : {reloaded.source_m0_run_id}", flush=True)
    print(f"  Sub-Niche        : {reloaded.sub_niche_id} — {reloaded.sub_niche_name_vi}", flush=True)
    print(f"  Models Used      : {reloaded.llm_models_used}", flush=True)
    print(f"  Scoring Policy   : {reloaded.scoring_weights_policy}", flush=True)

    assert len(reloaded.qualified_categories) >= 1, "Must have at least 1 qualified category"

    for cat in reloaded.qualified_categories:
        print(f"\n  ================================================================", flush=True)
        print(f"  [Category] {cat.category_id} — {cat.category_name_vi}", flush=True)
        print(f"  Confidence Score : {cat.category_confidence_score}/100", flush=True)

        w = cat.primary_winner
        print(
            f"  * PRIMARY WINNER : {w.brand} ({w.model_code}) — {w.product_name_vi} "
            f"| ~{w.estimated_market_price_vnd:,}đ ({w.price_vs_median_ratio}x median) "
            f"| Score: {w.score_breakdown.total_score_100}/100 | Affiliate: {w.affiliate_status}",
            flush=True,
        )
        print(f"    + Breakdown    : {w.score_breakdown.score_explanation_vi}", flush=True)
        if w.best_affiliate_offer:
            print(
                f"    + Best Offer   : #{w.best_affiliate_offer.item_id} ({w.best_affiliate_offer.shop_name}) "
                f"| {w.best_affiliate_offer.price_vnd:,}đ | Sold={w.best_affiliate_offer.sales_count} "
                f"| Comm={w.best_affiliate_offer.commission_rate_pct}% | Link={w.best_affiliate_offer.short_link}",
                flush=True,
            )

        if cat.runner_up:
            ru = cat.runner_up
            print(
                f"  * RUNNER-UP      : {ru.brand} ({ru.model_code}) — {ru.product_name_vi} "
                f"| ~{ru.estimated_market_price_vnd:,}đ | Score: {ru.score_breakdown.total_score_100}/100 "
                f"| Affiliate: {ru.affiliate_status}",
                flush=True,
            )
            print(f"    + Breakdown    : {ru.score_breakdown.score_explanation_vi}", flush=True)

        print(f"  * REJECTED FOILS ({len(cat.rejected_foils)}):", flush=True)
        assert len(cat.rejected_foils) >= 1, f"Category {cat.category_id} should preserve at least 1 rejected foil"
        for f in cat.rejected_foils:
            print(
                f"    - [{f.rejection_gate}] {f.brand} ({f.model_code or 'OEM'}) ~{f.estimated_market_price_vnd:,}đ: "
                f"{f.rejection_reason_vi}",
                flush=True,
            )

        v = cat.head_to_head_verdict
        print("  * HEAD-TO-HEAD VERDICT:", flush=True)
        print(f"    - Title            : {v.winner_summary_title_vi}", flush=True)
        print(f"    - Why Chosen       : {v.why_winner_chosen_vi}", flush=True)
        print(f"    - Beats Runner-Up  : {v.why_winner_beats_runner_up_vi}", flush=True)
        print(f"    - Avoid Foils      : {v.why_avoid_rejected_foils_vi}", flush=True)
        print(f"    - Caveat Framing   : {v.honest_caveat_framing_vi}", flush=True)

        # Invariant assertions
        assert w.score_breakdown.affiliate_readiness_bonus <= 15.0, "Affiliate bonus must not exceed 15 pts"
        assert w.score_breakdown.total_score_100 <= 100.0, "Total score must not exceed 100 pts"
        if cat.runner_up:
            assert w.score_breakdown.total_score_100 >= cat.runner_up.score_breakdown.total_score_100

    print("\n=== ALL M1 VERIFICATION CHECKS PASSED (EXIT CODE 0) ===", flush=True)


if __name__ == "__main__":
    try:
        run_verification()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
