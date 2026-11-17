"""
End-to-End Verification Suite for Module M0 (`pipeline/test_m0_discovery.py`).
"""
from __future__ import annotations

import json
import sys

from pipeline.llm_client import FreeTierGeminiClient
from pipeline.m0_discovery import HISTORY_PATH, M0AutonomousDiscoveryEngine, OUTPUT_POOL_PATH
from pipeline.schemas_m0 import M0CandidatePoolOutput


def run_verification() -> None:
    print("=== [TEST M0.1] Checking Free-Tier Gemini Model Availability ===", flush=True)
    llm = FreeTierGeminiClient()
    model_status = llm.detect_available_free_models()
    for m, available in model_status.items():
        print(f"  - {m}: {'AVAILABLE' if available else 'NOT LISTED'}", flush=True)
    assert any(model_status.values()), "At least one Free-Tier model must be available!"

    print("\n=== [TEST M0.2] Running 100% Autonomous M0 Discovery Engine ===", flush=True)
    engine = M0AutonomousDiscoveryEngine(llm_client=llm)
    engine.run(max_categories=2)

    print("\n=== [TEST M0.3] Validating Output Data Contract (data/m0_candidate_pool.json) ===", flush=True)
    assert OUTPUT_POOL_PATH.exists(), f"Missing output file {OUTPUT_POOL_PATH}"
    reloaded = M0CandidatePoolOutput.model_validate_json(
        OUTPUT_POOL_PATH.read_text(encoding="utf-8")
    )

    print(f"  Run ID        : {reloaded.run_id}", flush=True)
    print(f"  Domain        : {reloaded.domain} ({reloaded.domain_name_vi})", flush=True)
    print(f"  Sub-Niche     : {reloaded.sub_niche_id} — {reloaded.sub_niche_name_vi}", flush=True)
    print(f"  Models Used   : {reloaded.llm_models_used}", flush=True)
    print(f"  CSVs Scanned  : {len(reloaded.csv_files_scanned)} file(s)", flush=True)
    print(f"  Categories    : {len(reloaded.functional_categories)}", flush=True)

    assert len(reloaded.functional_categories) >= 1, "Must discover at least 1 functional category"

    for cat in reloaded.functional_categories:
        print(f"\n  [Category] {cat.category_id} — {cat.category_name_vi}", flush=True)
        print(
            f"    Market Price Range: Min={cat.observed_market_price_range_vnd.min_vnd:,}đ | "
            f"Median={cat.observed_market_price_range_vnd.median_vnd:,}đ | "
            f"Max={cat.observed_market_price_range_vnd.max_vnd:,}đ",
            flush=True,
        )
        print(f"    Generic Queries   : {cat.generic_search_queries_used}", flush=True)
        brands = {c.brand.strip().lower() for c in cat.candidates}
        archetypes = {c.archetype for c in cat.candidates}
        print(
            f"    Candidates ({len(cat.candidates)})   : Brands={sorted(brands)} | Archetypes={sorted(archetypes)}",
            flush=True,
        )

        assert len(cat.candidates) >= 3, f"Category {cat.category_id} must have >=3 candidates"
        assert len(brands) >= 2, f"Category {cat.category_id} must have multiple competing brands"
        for c in cat.candidates:
            assert c.honest_caveat_vi.strip(), f"Candidate {c.candidate_id} missing honest_caveat_vi"
            print(
                f"      * [{c.archetype}] {c.brand} ({c.model_code or 'No-Model'}) — {c.product_name_vi} "
                f"| ~{c.estimated_market_price_vnd:,}đ | YT Clips={len(c.youtube_evidence)} "
                f"| Affiliate CSV Matches={len(c.affiliate_matches)}",
                flush=True,
            )
            print(f"        + Caveat ({c.caveat_severity}): {c.honest_caveat_vi}", flush=True)

    assert HISTORY_PATH.exists(), "m0_history.json must be created/updated"
    history_data = json.loads(HISTORY_PATH.read_text(encoding="utf-8"))
    assert len(history_data.get("runs", [])) >= 1, "m0_history.json must record the completed run"

    print("\n=== ALL M0 VERIFICATION CHECKS PASSED (EXIT CODE 0) ===", flush=True)


if __name__ == "__main__":
    try:
        run_verification()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
