"""
End-to-End Verification & Adversarial Fault-Injection Suite for Module M3 (`pipeline/test_m3_qaqc.py`).
"""
from __future__ import annotations

import copy
import re
import sys

from pipeline.m3_qaqc import INPUT_M1_PATH, INPUT_M2_PATH, M3QAQCEngine, OUTPUT_M3_PATH
from pipeline.schemas_m1 import M1QualifiedSKUsOutput
from pipeline.schemas_m2 import M2ScriptDraftsOutput
from pipeline.schemas_m3 import M3ApprovedScriptsOutput


def run_verification() -> None:
    print("=== [TEST M3.1] Real Pipeline QA/QC Audit (M2 -> M3) ===", flush=True)
    assert INPUT_M2_PATH.exists(), f"Missing M2 input file: {INPUT_M2_PATH}"
    assert INPUT_M1_PATH.exists(), f"Missing M1 input file: {INPUT_M1_PATH}"

    engine = M3QAQCEngine()
    engine.run(
        input_m2_path=INPUT_M2_PATH,
        input_m1_path=INPUT_M1_PATH,
        output_path=OUTPUT_M3_PATH,
    )

    assert OUTPUT_M3_PATH.exists(), f"Missing M3 output file: {OUTPUT_M3_PATH}"
    reloaded = M3ApprovedScriptsOutput.model_validate_json(
        OUTPUT_M3_PATH.read_text(encoding="utf-8")
    )

    print(f"\n  M3 Run ID        : {reloaded.m3_run_id}", flush=True)
    print(f"  Source M2 Run ID : {reloaded.source_m2_run_id}", flush=True)
    print(f"  Source M1 Run ID : {reloaded.source_m1_run_id}", flush=True)
    print(f"  Models Used      : {reloaded.llm_models_used}", flush=True)
    print(
        f"  Audit Summary    : APPROVED={reloaded.approved_count} | "
        f"AUTO_PATCHED={reloaded.auto_patched_count} | REJECTED={reloaded.rejected_count}",
        flush=True,
    )

    assert reloaded.rejected_count == 0, f"Expected 0 rejected scripts, got {reloaded.rejected_count}"
    assert len(reloaded.audited_scripts) >= 1

    for s in reloaded.audited_scripts:
        assert s.qaqc_status in ("APPROVED", "APPROVED_WITH_AUTO_PATCH"), (
            f"Unexpected status {s.qaqc_status} for {s.script_id}"
        )
        print("\n  ================================================================", flush=True)
        print(
            f"  [{s.qaqc_status}] {s.script_id} ({s.script_format}) — "
            f"QAQC Score: {s.qaqc_score_breakdown.total_qaqc_score_100}/100 "
            f"| {s.total_duration_sec}s ({s.total_word_count}w)",
            flush=True,
        )
        print(f"    Verdict : {s.auditor_verdict_vi}", flush=True)
        print(f"    Issues  : {len(s.issues_found)}", flush=True)

        compact_token_hits = 0
        for b in s.beats:
            assert b.tts_voiceover_vi.strip(), f"Empty tts_voiceover_vi at beat {b.beat_index}"
            assert b.subtitle_text_vi.strip(), f"Empty subtitle_text_vi at beat {b.beat_index}"
            assert "mi-li-lít" not in b.subtitle_text_vi.lower(), (
                f"Long spelled-out 'mi-li-lít' found in subtitle_text_vi: {b.subtitle_text_vi}"
            )
            assert "mi li lít" not in b.subtitle_text_vi.lower(), (
                f"Long spelled-out 'mi li lít' found in subtitle_text_vi: {b.subtitle_text_vi}"
            )
            if re.search(
                r"\b\d+(?:[.,]\d+)?\s*(?:k|tr|đ|ml|mm|g|kg|w|mah|độ|x\d+mm)\b",
                b.subtitle_text_vi,
                flags=re.IGNORECASE,
            ):
                compact_token_hits += 1

            print(
                f"      #{b.beat_index} [{b.beat_type}] patched={b.was_auto_patched} "
                f"({b.start_sec:.1f}s->{b.end_sec:.1f}s)",
                flush=True,
            )
            print(f"         TTS : {b.tts_voiceover_vi}", flush=True)
            print(f"         Sub : {b.subtitle_text_vi}", flush=True)

        assert compact_token_hits >= 2, (
            f"Expected compact numeric tokens (180ml/89k/900ml/195k) in subtitles of {s.script_id}"
        )

    print("\n=== [TEST M3.2] Adversarial Fault-Injection Audit (Catching Hallucinations & Missing Caveat) ===", flush=True)
    m2_raw = M2ScriptDraftsOutput.model_validate_json(INPUT_M2_PATH.read_text(encoding="utf-8"))
    m1_raw = M1QualifiedSKUsOutput.model_validate_json(INPUT_M1_PATH.read_text(encoding="utf-8"))

    sabotaged_script = copy.deepcopy(m2_raw.script_drafts[0])
    sabotaged_script.script_id = "sabotaged_fault_test_script"
    for b in sabotaged_script.beats:
        if b.beat_index == 2:
            b.voiceover_vi = (
                "Đây là máy xay điện Lock&Lock CKS321GRN cối thủy tinh 500ml "
                "công suất 300W giá 250 nghìn bảo hành 5 năm."
            )
        elif b.beat_index == 4:
            b.voiceover_vi = (
                "Sản phẩm hoàn hảo tuyệt đối không có bất kỳ điểm trừ nào, "
                "xay tảng thịt bò đông lạnh cực kỳ dễ dàng."
            )

    sabotaged_m2 = copy.deepcopy(m2_raw)
    sabotaged_m2.script_drafts = [sabotaged_script]

    fault_result = engine.audit_drafts(sabotaged_m2, m1_raw)
    audited_fault = fault_result.audited_scripts[0]

    detected_cats = [i.issue_category for i in audited_fault.issues_found]
    print(f"  Fault Test Status  : {audited_fault.qaqc_status}", flush=True)
    print(f"  QAQC Score (Before): {audited_fault.qaqc_score_breakdown}", flush=True)
    print(f"  Detected Issues    : {detected_cats}", flush=True)
    for iss in audited_fault.issues_found:
        print(f"    - [{iss.severity}] ({iss.issue_category} @ Beat {iss.beat_index}): {iss.description_vi}", flush=True)
        print(f"      Resolution: {iss.resolution_applied_vi}", flush=True)

    beat2 = next(b for b in audited_fault.beats if b.beat_index == 2)
    beat4 = next(b for b in audited_fault.beats if b.beat_index == 4)
    print(f"  Patched Beat 2 Sub : {beat2.subtitle_text_vi} (was_auto_patched={beat2.was_auto_patched})", flush=True)
    print(f"  Patched Beat 4 Sub : {beat4.subtitle_text_vi} (was_auto_patched={beat4.was_auto_patched})", flush=True)

    assert audited_fault.qaqc_status == "APPROVED_WITH_AUTO_PATCH", (
        f"Expected APPROVED_WITH_AUTO_PATCH, got {audited_fault.qaqc_status}"
    )
    assert "FACT_HALLUCINATION" in detected_cats, f"Failed to detect FACT_HALLUCINATION in {detected_cats}"
    assert "MISSING_HONEST_CAVEAT" in detected_cats, f"Failed to detect MISSING_HONEST_CAVEAT in {detected_cats}"
    assert beat2.was_auto_patched is True, "Beat 2 should be marked was_auto_patched=True"
    assert beat4.was_auto_patched is True, "Beat 4 should be marked was_auto_patched=True"

    for forbidden in ("300w", "500ml", "không có bất kỳ điểm trừ nào"):
        assert forbidden not in beat2.tts_voiceover_vi.lower()
        assert forbidden not in beat4.tts_voiceover_vi.lower()

    print("\n=== ALL M3 VERIFICATION & ADVERSARIAL CHECKS PASSED (EXIT CODE 0) ===", flush=True)


if __name__ == "__main__":
    try:
        run_verification()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
