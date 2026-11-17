"""
End-to-End Verification Suite for Module M2 (`pipeline/test_m2_script_gen.py`).
"""
from __future__ import annotations

import sys

from pipeline.m2_script_gen import INPUT_M1_PATH, M2ScriptGeneratorEngine, OUTPUT_M2_PATH
from pipeline.schemas_m1 import M1QualifiedSKUsOutput
from pipeline.schemas_m2 import M2ScriptDraftsOutput


def run_verification() -> None:
    print("=== [TEST M2.1] Running M2 Script Classifier & Generator ===", flush=True)
    assert INPUT_M1_PATH.exists(), f"Missing M1 input file: {INPUT_M1_PATH}"
    m1_input = M1QualifiedSKUsOutput.model_validate_json(
        INPUT_M1_PATH.read_text(encoding="utf-8")
    )

    engine = M2ScriptGeneratorEngine()
    engine.run(input_path=INPUT_M1_PATH, output_path=OUTPUT_M2_PATH)

    print("\n=== [TEST M2.2] Validating Output Data Contract (data/m2_script_drafts.json) ===", flush=True)
    assert OUTPUT_M2_PATH.exists(), f"Missing M2 output file: {OUTPUT_M2_PATH}"
    reloaded = M2ScriptDraftsOutput.model_validate_json(
        OUTPUT_M2_PATH.read_text(encoding="utf-8")
    )

    print(f"  M2 Run ID        : {reloaded.m2_run_id}", flush=True)
    print(f"  Source M1 Run ID : {reloaded.source_m1_run_id}", flush=True)
    print(f"  Sub-Niche        : {reloaded.sub_niche_id} — {reloaded.sub_niche_name_vi}", flush=True)
    print(f"  Synergy Type     : {reloaded.multi_category_synergy_type}", flush=True)
    print(f"  Reasoning        : {reloaded.synergy_classification_reasoning_vi}", flush=True)
    print(f"  Models Used      : {reloaded.llm_models_used}", flush=True)
    print(f"  Scripts Generated: {len(reloaded.script_drafts)}", flush=True)

    num_cats = len(m1_input.qualified_categories)
    if num_cats >= 2:
        assert len(reloaded.script_drafts) == num_cats + 1, (
            f"Expected {num_cats + 1} scripts for {num_cats} categories, got {len(reloaded.script_drafts)}"
        )
        single_scripts = [s for s in reloaded.script_drafts if s.script_format == "SINGLE_SKU_DEEP_DIVE"]
        multi_scripts = [
            s
            for s in reloaded.script_drafts
            if s.script_format in ("DUAL_TIER_COMPARISON", "COMBO_WORKFLOW_SYNERGY")
        ]
        assert len(single_scripts) == num_cats, (
            f"Expected {num_cats} SINGLE_SKU_DEEP_DIVE scripts, got {len(single_scripts)}"
        )
        assert len(multi_scripts) == 1, f"Expected 1 multi-SKU script, got {len(multi_scripts)}"

    for draft in reloaded.script_drafts:
        print("\n  ================================================================", flush=True)
        print(f"  [{draft.script_format}] {draft.script_id}", flush=True)
        print(f"    Title          : {draft.title_vi}", flush=True)
        print(f"    Rationale      : {draft.selection_rationale_vi}", flush=True)
        print(
            f"    SKUs           : Primary={draft.primary_sku_ids} | Runner-Ups={draft.runner_up_sku_ids_referenced} | "
            f"Foils={draft.foil_sku_ids_referenced}",
            flush=True,
        )
        print(
            f"    Timing & Pacing: {draft.total_duration_sec}s | {draft.total_word_count} words | "
            f"{draft.average_speaking_rate_wps} wps",
            flush=True,
        )
        print(f"    M3 Claims Anchors: {len(draft.factual_claims_for_m3_audit)} SKU(s)", flush=True)

        assert len(draft.beats) == 5, f"Script {draft.script_id} must have 5 beats, got {len(draft.beats)}"
        assert 28.0 <= draft.total_duration_sec <= 58.0, (
            f"Script {draft.script_id} duration {draft.total_duration_sec}s out of [28, 58]s"
        )
        assert 115 <= draft.total_word_count <= 225, (
            f"Script {draft.script_id} word count {draft.total_word_count} out of [115, 225]"
        )

        has_honest_caveat_beat = False
        prev_end = 0.0
        for beat in draft.beats:
            assert beat.start_sec == prev_end, (
                f"Beat {beat.beat_index} non-contiguous start_sec {beat.start_sec} != {prev_end}"
            )
            assert round(beat.end_sec - beat.start_sec, 1) == beat.duration_sec
            prev_end = beat.end_sec

            onscreen_words = beat.on_screen_text_vi.split()
            assert len(onscreen_words) <= 8, (
                f"Beat {beat.beat_index} on_screen_text_vi exceeds 8 words: {beat.on_screen_text_vi}"
            )
            assert beat.source_youtube_clip is not None, (
                f"Beat {beat.beat_index} missing source_youtube_clip"
            )
            if beat.beat_type == "HONEST_CAVEAT":
                has_honest_caveat_beat = True

            print(
                f"      #{beat.beat_index} [{beat.beat_type}] ({beat.start_sec:.1f}s -> {beat.end_sec:.1f}s | "
                f"{beat.word_count}w) target={beat.target_candidate_id}",
                flush=True,
            )
            print(f"         VO      : {beat.voiceover_vi}", flush=True)
            print(f"         Screen  : {beat.on_screen_text_vi}", flush=True)
            print(
                f"         Clip    : {beat.source_youtube_clip.video_id} — {beat.source_youtube_clip.title[:60]}",
                flush=True,
            )

        assert has_honest_caveat_beat, f"Script {draft.script_id} must include a HONEST_CAVEAT beat"
        assert len(draft.factual_claims_for_m3_audit) == len(draft.primary_sku_ids)

    print("\n=== ALL M2 VERIFICATION CHECKS PASSED (EXIT CODE 0) ===", flush=True)


if __name__ == "__main__":
    try:
        run_verification()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
