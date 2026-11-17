"""
Module M2: Script Classifier & Generator (`pipeline/m2_script_gen.py`).

Responsibilities (Black-Box Contract):
  1. Reads ONLY `data/m1_qualified_skus.json` (zero code coupling to M0 or M1).
  2. Never invents specs or prices outside M1's verified output.
  3. Step M2.1 + M2.2 (Synergy Classification & Structured Script Drafting via Gemini):
     - Classifies multi-category relationship as `SIZE_OR_TIER_PAIR`, `COMPLEMENTARY_WORKFLOW`,
       or `SINGLE_CATEGORY_ONLY` so combos are never forced unnaturally.
     - Drafts 1 `SINGLE_SKU_DEEP_DIVE` script per category winner plus 1 multi-SKU script
       (`DUAL_TIER_COMPARISON` or `COMBO_WORKFLOW_SYNERGY`) when >= 2 categories exist.
  4. Deterministic Python Post-Processing:
     - Computes `word_count`, `duration_sec` (~3.1 wps floor 4.5s), and contiguous `start_sec`/`end_sec`.
     - Caps `on_screen_text_vi` cleanly to <= 8 words.
     - Binds `source_youtube_clip` directly from M1's `youtube_evidence` list.
     - Attaches `factual_claims_for_m3_audit` and `affiliate_offers_attached` directly from M1's
       ground truth so M3 can run objective QA/QC audits.
  5. Outputs `data/m2_script_drafts.json` for Module M3.
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.core_resilience import atomic_write_text
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.schemas_m0 import AffiliateListingMatch, DomainId, YouTubeEvidenceClip
from pipeline.schemas_m1 import EvaluatedCandidateSKU, M1QualifiedSKUsOutput, QualifiedCategoryResult, ScoreBreakdown100
from pipeline.schemas_m1_5 import M15ComboCuratorOutput
from pipeline.shared_utils import enforce_short_hook_title
from pipeline.sku_registry import SKURegistryManager

OUTPUT_M1_5_PATH = Path(__file__).resolve().parent.parent / "data" / "m1_5_combo_result.json"
from pipeline.schemas_m2 import (
    FactualClaimReference,
    LLMGeneratedScriptDraft,
    LLMM2BatchGenerationResponse,
    M2ScriptDraftsOutput,
    NarrativeAngle,
    ScriptBeatSegment,
    ScriptDraftItem,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_M1_PATH = DATA_DIR / "m1_qualified_skus.json"
OUTPUT_M2_PATH = DATA_DIR / "m2_script_drafts.json"

DEFAULT_SPEAKING_RATE_WPS = 3.1
MIN_BEAT_DURATION_SEC = 4.5
MAX_ONSCREEN_WORDS = 8


def _trim_onscreen_text(text: str, max_words: int = MAX_ONSCREEN_WORDS) -> str:
    words = text.strip().split()
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words])


def _cap_beat_voiceover(text: str, max_words: int = 31) -> str:
    """Keep each beat's voiceover concise (<= 31 words) so 5-beat scripts stay within 80..165 words."""
    clean = " ".join(text.strip().split())
    words = clean.split()
    if len(words) <= max_words:
        return clean
    prefix = " ".join(words[:max_words])
    for sep in (".", "!", "?", ";", ","):
        idx = prefix.rfind(sep)
        if idx > 0 and len(prefix[:idx].split()) >= 18:
            out = prefix[:idx].strip().rstrip(",;:")
            return out + ("." if out and out[-1] not in ".!?" else "")
    out = prefix.rstrip(",;:")
    return out + ("." if out and out[-1] not in ".!?" else "")


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower()).strip("_")
    return s or "script"


class M2ScriptGeneratorEngine:
    """Executes M2 Synergy Classification, Timed Script Generation, and Ground-Truth Binding."""

    def __init__(
        self,
        llm_client: Optional[FreeTierGeminiClient] = None,
        registry_mgr: Optional[SKURegistryManager] = None,
    ) -> None:
        self.llm = llm_client or FreeTierGeminiClient()
        self.registry_mgr = registry_mgr or SKURegistryManager()

    def _build_prompt(
        self,
        m1_data: M1QualifiedSKUsOutput,
        m1_5_data: Optional[M15ComboCuratorOutput] = None,
    ) -> str:
        category_blocks: List[str] = []
        for idx, cat in enumerate(m1_data.qualified_categories, start=1):
            w = cat.primary_winner
            ru = cat.runner_up
            v = cat.head_to_head_verdict

            runner_line = (
                f"  - Á QUÂN (`{ru.candidate_id}`): {ru.brand} ({ru.model_code or 'N/A'}) — {ru.product_name_vi} | "
                f"Giá ~{ru.estimated_market_price_vnd:,}đ | Điểm {ru.score_breakdown.total_score_100}/100 | "
                f"Nhược điểm: {ru.honest_caveat_vi}"
                if ru
                else "  - Á QUÂN: (Không có)"
            )
            foil_lines = "; ".join(
                f"`{f.candidate_id}` ({f.brand}, ~{f.estimated_market_price_vnd:,}đ, lý do loại: {f.rejection_reason_vi})"
                for f in cat.rejected_foils
            ) or "(Không có)"

            specs_str = ", ".join(f"{k}: {val}" for k, val in w.key_specs.items())

            category_blocks.append(
                f"""
=== NHÓM CÔNG NĂNG {idx}: `{cat.category_id}` — {cat.category_name_vi} ===
- Chủ đề gốc (Domain): `{cat.domain}`
- Vấn đề giải quyết: {cat.problem_solved_vi}
- QUÁN QUÂN (`{w.candidate_id}`): {w.brand} | Model: {w.model_code or 'N/A'} | Tên: {w.product_name_vi}
  + Giá thị trường chuẩn: {w.estimated_market_price_vnd:,}đ ({w.price_vs_median_ratio}x trung vị) | Điểm M1: {w.score_breakdown.total_score_100}/100
  + Thông số chuẩn: {specs_str}
  + Ưu điểm nổi bật: {"; ".join(w.highlight_pros_vi)}
  + Nhược điểm thực tế BẮT BUỘC PHẢI NÓI (`{w.caveat_severity}`): {w.honest_caveat_vi}
{runner_line}
- MẪU ĐỐI CHỨNG NÊN TRÁNH (`rejected_foils`): {foil_lines}
- BIÊN BẢN ĐỐI ĐẦU TỪ M1:
  + Tại sao chọn Quán quân: {v.why_winner_chosen_vi}
  + Tại sao thắng Á quân: {v.why_winner_beats_runner_up_vi}
  + Tại sao tránh hàng lỗi/OEM: {v.why_avoid_rejected_foils_vi}
  + Cách nói nhược điểm trung thực: {v.honest_caveat_framing_vi}
""".strip()
            )

        num_cats = len(m1_data.qualified_categories)
        combo_section_str = ""
        if m1_5_data and m1_5_data.status == "COMBO_APPROVED" and m1_5_data.selected_combo:
            cb = m1_5_data.selected_combo
            member_lines = "\n".join(
                f"   + Bước {m.step_order}: `{m.candidate_id}` ({m.brand} {m.model_code or ''} — {m.product_name_vi}, ~{m.estimated_market_price_vnd:,}đ) -> Vai trò: {m.role_in_combo_vi} | Lưu ý thật: {m.honest_caveat_vi}"
                for m in cb.members
            )
            combo_section_str = f"""
=== KẾT QUẢ THẨM ĐỊNH COMBO TỪ MODULE ĐỘC LẬP M1.5 (`COMBO_APPROVED`) ===
- Combo ID: `{cb.combo_id}` | Domain: `{cb.domain}` | Micro-Scene: `{cb.micro_scene_id}`
- Tiêu đề gợi ý: {cb.combo_title_vi} (Hook <=30 ký tự: "{cb.hook_title_short_vi}")
- Tình huống phối hợp thực tế (`joint_workflow_scenario_vi`): {cb.joint_workflow_scenario_vi}
- Lý giải tính tự nhiên: {cb.why_not_forced_vi}
- Các SKU thành viên (tổng ~{cb.total_combo_price_vnd:,}đ):
{member_lines}
"""
            multi_rule = (
                f"1. Module độc lập M1.5 đã duyệt bộ Combo tự nhiên (`COMBO_APPROVED`) gồm các SKU `{[m.candidate_id for m in cb.members]}`.\n"
                "   -> BẮT BUỘC đặt `multi_category_synergy_type = \"COMPLEMENTARY_WORKFLOW\"`.\n"
                f"2. SỐ LƯỢNG KỊCH BẢN PHẢI TẠO TRONG `scripts`: ĐÚNG {num_cats + 1} KỊCH BẢN gồm:\n"
                f"   - {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE` (mỗi Quán quân 1 kịch bản riêng).\n"
                f"   - 1 kịch bản `COMBO_WORKFLOW_SYNERGY` ghép đúng các SKU `{[m.candidate_id for m in cb.members]}` theo sát tình huống `joint_workflow_scenario_vi` của M1.5 ở trên."
            )
        elif m1_5_data and m1_5_data.status == "BYPASS":
            # Check if categories are a size/tier pair of the same product family
            cat_names_norm = " ".join(c.category_id for c in m1_data.qualified_categories).lower()
            is_size_pair = num_cats >= 2 and ("chopper" in cat_names_norm and len(set(c.domain for c in m1_data.qualified_categories)) == 1)
            if is_size_pair:
                multi_rule = (
                    "1. Module M1.5 trả về `BYPASS` cho Combo nhưng 2 nhóm là CÙNG MỘT DÒNG CÔNG DỤNG khác dung tích/tầm giá "
                    "-> chọn `multi_category_synergy_type = \"SIZE_OR_TIER_PAIR\"` và tạo kịch bản ghép dạng **`DUAL_TIER_COMPARISON`** "
                    f"cùng {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE`."
                )
            else:
                multi_rule = (
                    f"1. Module độc lập M1.5 trả về `status = \"BYPASS\"` ({m1_5_data.bypass_reason_vi}).\n"
                    "   -> TUYỆT ĐỐI KHÔNG ÉP VIẾT KỊCH BẢN COMBO! Hãy đặt `multi_category_synergy_type = \"SINGLE_CATEGORY_ONLY\"` "
                    f"và CHỈ TẠO ĐÚNG {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE` (mỗi Quán quân 1 kịch bản riêng, 0 kịch bản combo)."
                )
        elif num_cats == 1:
            multi_rule = (
                "1. Chỉ có 1 nhóm công năng -> đặt `multi_category_synergy_type = \"SINGLE_CATEGORY_ONLY\"` và chỉ tạo 1 kịch bản `SINGLE_SKU_DEEP_DIVE`."
            )
        else:
            multi_rule = (
                "1. Có từ 2 nhóm công năng trở lên -> PHÂN LOẠI MỐI QUAN HỆ TRƯỚC:\n"
                "   - Nếu 2 Quán quân thực chất là CÙNG MỘT DÒNG CÔNG DỤNG nhưng khác kích cỡ/dung tích/tầm giá (VD: cối 180ml ~89k vs cối 900ml ~195k) "
                "-> KHÔNG ĐƯỢC ÉP THÀNH COMBO MUA CẢ HAI! Hãy chọn `multi_category_synergy_type = \"SIZE_OR_TIER_PAIR\"` và tạo kịch bản ghép dạng **`DUAL_TIER_COMPARISON`** "
                "(đặt lên bàn cân để người xem tự chọn đúng 1 mẫu hợp nhu cầu gia đình).\n"
                "   - Nếu 2 Quán quân thuộc 2 CÔNG ĐOẠN KHÁC NHAU bổ trợ liền mạch trong 1 quy trình (VD: Cối xay tỏi + Bình xịt dầu) "
                "-> chọn `multi_category_synergy_type = \"COMPLEMENTARY_WORKFLOW\"` và tạo kịch bản ghép dạng **`COMBO_WORKFLOW_SYNERGY`**.\n"
                f"2. SỐ LƯỢNG KỊCH BẢN PHẢI TẠO TRONG `scripts`: ĐÚNG {num_cats + 1} KỊCH BẢN gồm:\n"
                f"   - {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE` (mỗi Quán quân 1 kịch bản riêng).\n"
                "   - 1 kịch bản ghép đa SKU (`DUAL_TIER_COMPARISON` hoặc `COMBO_WORKFLOW_SYNERGY` tương ứng với phân loại ở trên)."
            )

        return f"""
Bối cảnh ngách từ M1:
- Lĩnh vực: `{m1_data.domain}` — {m1_data.sub_niche_name_vi}
- Nỗi đau người dùng: {m1_data.problem_context_vi}

Dữ liệu các Quán quân, Á quân và Đối chứng đã được M1 thẩm định:

{chr(10).join(category_blocks)}
{combo_section_str}
YÊU CẦU TẠO KỊCH BẢN (`LLMM2BatchGenerationResponse`):
{multi_rule}
- MỌI KỊCH BẢN BẮT BUỘC ĐIỀN `hook_title_short_vi` (tiêu đề mở bài hiển thị 2s đầu video, NGẮN GỌN TỐI ĐA 30 KÝ TỰ, có dấu tiếng Việt chuẩn, không dài dòng che nội dung).

PHÂN BỔ GÓC KỂ CHUYỆN KHÁC BIỆT (`narrative_angle`) & CHỦ ĐỀ (`domain`, `category_id`):
Mỗi kịch bản trong `scripts` BẮT BUỘC phải điền đúng `domain` (`TECH_DESK` hoặc `KITCHEN_TECH`), đúng `category_id` của Quán quân tương ứng, và PHẢI gán MỘT `narrative_angle` KHÁC NHAU (KHÔNG ĐƯỢC HAI KỊCH BẢN TRÙNG `narrative_angle`):
1. `PAIN_POINT_REALITY_CHECK` (Bóc tách nỗi đau thực tế & kiểm chứng thông số vật lý): Mở bài bằng tình huống gây ức chế đời thường -> đối chiếu cơ chế kỹ thuật giải quyết -> điểm trừ thật -> chốt.
2. `MYTH_VS_FACT_TEARDOWN` (Giải mã hiểu lầm phổ biến / Tránh mua nhầm): Mở bài bằng hiểu lầm nhiều người mắc khi mua dòng sản phẩm này -> lật ngược bằng thông số thực tế -> cảnh báo giới hạn -> chốt.
3. `TIME_AND_EFFORT_MATH` (Bài toán hiệu suất & chi phí sử dụng mỗi ngày): Mở bài bằng con số tính toán thời gian/công sức tiết kiệm hoặc chi phí/lần dùng -> phân tích thiết kế cốt lõi -> điểm trừ -> chốt.
4. `DECISION_GUIDE_BY_USER_TYPE` (Phân loại người dùng: Ai nên mua mã nào / khi nào không nên mua): Phù hợp cho `DUAL_TIER_COMPARISON` — mở bài bằng câu hỏi chọn phân khúc theo nhu cầu thực -> đặt 2 SKU lên bàn cân thông số -> chỉ rõ điểm đánh đổi -> chốt.
5. `WORKFLOW_UPGRADE_JOURNEY` (Nâng cấp quy trình thao tác liên hoàn): Phù hợp cho `COMBO_WORKFLOW_SYNERGY` — ghép chuỗi thao tác mượt mà giữa các món trong 1 kịch bản.

QUY TẮC CẤM LẶP MỞ BÀI & CẤM CÔNG THỨC RẬP KHUÔN:
- Các kịch bản trong cùng đợt chạy TUYỆT ĐỐI KHÔNG được mở đầu giống nhau, không dùng chung một kiểu câu hỏi tu từ hay cụm chữ mở bài na ná nhau, và phải đổi hẳn nhịp điệu/giọng điệu dẫn dắt theo đúng `narrative_angle` đã chọn.

QUY CHUẨN CẤU TRÚC TỪNG KỊCH BẢN (BẮT BUỘC ĐÚNG 5 PHÂN CẢNH / `beats` TỪ `beat_index = 1` ĐẾN `5`):
- Với `SINGLE_SKU_DEEP_DIVE` (tổng ~95–125 từ tiếng Việt, trung bình ~19–25 từ mỗi beat):
  + Beat 1 (`HOOK_FOIL_WARNING`): Mở đầu chạm nỗi đau + cảnh báo hàng rẻ lỗi trong `rejected_foils` (ghi rõ giá ~30k/39k từ dữ liệu).
  + Beat 2 (`WINNER_REVEAL_SPECS`): Giới thiệu Quán quân (đúng hãng, đúng `model_code`, đúng dung tích/chất liệu trong M1).
  + Beat 3 (`HEAD_TO_HEAD_PROOF`): Chứng minh điểm hơn Á quân dựa trên biên bản `why_winner_beats_runner_up_vi`.
  + Beat 4 (`HONEST_CAVEAT`): Nói thẳng nhược điểm thực tế từ `honest_caveat_vi` một cách tự nhiên, chân thật.
  + Beat 5 (`VALUE_VERDICT_CTA`): Chốt mức giá chuẩn (~`estimated_market_price_vnd`) và lời kêu gọi xem link ở giỏ hàng/bình luận.
- Với `DUAL_TIER_COMPARISON` (tổng ~110–140 từ tiếng Việt, trung bình ~22–28 từ mỗi beat):
  + Beat 1 (`HOOK_FOIL_WARNING`): Đặt câu hỏi chọn cỡ nhỏ hay cỡ lớn + né bẫy hàng OEM rẻ.
  + Beat 2 (`DUAL_TIER_BREAKDOWN`): Đặt 2 Quán quân cạnh nhau (nêu đúng model, dung tích, chất liệu và giá chuẩn của CẢ HAI).
  + Beat 3 (`HEAD_TO_HEAD_PROOF`): Phân tích tình huống sử dụng thực tế: ai nên lấy bản nhỏ, ai nên lấy bản lớn.
  + Beat 4 (`HONEST_CAVEAT`): Nêu rõ giới hạn/nhược điểm thật của CẢ HAI mẫu (bên nhỏ chật khi xay nhiều, bên lớn cồng kềnh tốn tủ & nặng tay hơn).
  + Beat 5 (`VALUE_VERDICT_CTA`): Chốt gợi ý chọn 1 trong 2 theo nhu cầu bếp + CTA xem link tham khảo bên dưới.
- Với `COMBO_WORKFLOW_SYNERGY` (tổng ~110–145 từ tiếng Việt, trung bình ~22–28 từ mỗi beat):
  + Beat 1 (`HOOK_FOIL_WARNING`), Beat 2 (`COMBO_STEP_1`), Beat 3 (`COMBO_STEP_2_TRANSITION`), Beat 4 (`HONEST_CAVEAT`), Beat 5 (`VALUE_VERDICT_CTA`).

RÀNG BUỘC KỸ THUẬT & NGÔN NGỮ TUYỆT ĐỐI:
- NGÔN NGỮ VOICEOVER & TIÊU ĐỀ: BẮT BUỘC viết bằng **tiếng Việt có dấu đầy đủ (UTF-8 chuẩn)**, đúng chính tả, văn phong nói tự nhiên mượt mà như reviewer thật (phục vụ đọc Voiceover TTS). TUYỆT ĐỐI KHÔNG viết tiếng Việt không dấu hoặc sai dấu chính tả.
- `target_candidate_id` ở mỗi beat phải dùng đúng chuỗi `candidate_id` có thật trong dữ liệu M1 (VD: `locknlock_cks321`).
- `primary_sku_ids`, `runner_up_sku_ids_referenced`, `foil_sku_ids_referenced` phải dùng đúng các `candidate_id` từ M1.
- `on_screen_text_vi`: Ngắn gọn, nổi bật, tiếng Việt có dấu, TUYỆT ĐỐI KHÔNG QUÁ 8 TỪ.
- KHÔNG ĐƯỢC BỊA THÊM thông số, công suất, thời gian bảo hành hay giá bán khác với dữ liệu M1 đã cho.
""".strip()

    def _index_m1_entities(
        self, m1_data: M1QualifiedSKUsOutput
    ) -> Tuple[
        Dict[str, EvaluatedCandidateSKU],
        Dict[str, QualifiedCategoryResult],
        set[str],
        set[str],
    ]:
        sku_by_id: Dict[str, EvaluatedCandidateSKU] = {}
        cat_by_winner_id: Dict[str, QualifiedCategoryResult] = {}
        valid_runner_ids: set[str] = set()
        valid_foil_ids: set[str] = set()

        for cat in m1_data.qualified_categories:
            cat_by_winner_id[cat.primary_winner.candidate_id] = cat
            for cand in cat.all_qualified_ranked:
                sku_by_id[cand.candidate_id] = cand
            if cat.runner_up:
                valid_runner_ids.add(cat.runner_up.candidate_id)
            for f in cat.rejected_foils:
                valid_foil_ids.add(f.candidate_id)

        return sku_by_id, cat_by_winner_id, valid_runner_ids, valid_foil_ids

    def _pick_clip_for_beat(
        self,
        beat_index: int,
        target_candidate_id: str,
        primary_sku_ids: List[str],
        sku_by_id: Dict[str, EvaluatedCandidateSKU],
        fallback_winner: EvaluatedCandidateSKU,
    ) -> Optional[YouTubeEvidenceClip]:
        # Prefer clips from the target primary winner if it is one of primary_sku_ids
        if target_candidate_id in primary_sku_ids:
            cand = sku_by_id.get(target_candidate_id)
            if cand and cand.youtube_evidence:
                clips = cand.youtube_evidence
                return clips[(beat_index - 1) % len(clips)]

        # Otherwise gather all clips from primary_sku_ids
        combined: List[YouTubeEvidenceClip] = []
        for pid in primary_sku_ids:
            cand = sku_by_id.get(pid)
            if cand and cand.youtube_evidence:
                combined.extend(cand.youtube_evidence)

        if combined:
            return combined[(beat_index - 1) % len(combined)]

        if fallback_winner.youtube_evidence:
            clips = fallback_winner.youtube_evidence
            return clips[(beat_index - 1) % len(clips)]
        return None

    def _postprocess_script(
        self,
        idx: int,
        raw_script: LLMGeneratedScriptDraft,
        m1_data: M1QualifiedSKUsOutput,
        sku_by_id: Dict[str, EvaluatedCandidateSKU],
        valid_runner_ids: set[str],
        valid_foil_ids: set[str],
    ) -> ScriptDraftItem:
        all_winner_ids = [c.primary_winner.candidate_id for c in m1_data.qualified_categories]
        fallback_winner = m1_data.qualified_categories[0].primary_winner

        # Validate primary_sku_ids against real M1 candidate_ids
        clean_primary_ids = [cid for cid in raw_script.primary_sku_ids if cid in sku_by_id]
        if not clean_primary_ids:
            if raw_script.script_format == "SINGLE_SKU_DEEP_DIVE":
                clean_primary_ids = [all_winner_ids[min(idx - 1, len(all_winner_ids) - 1)]]
            else:
                clean_primary_ids = all_winner_ids[:]

        clean_runner_ids = [
            cid for cid in raw_script.runner_up_sku_ids_referenced if cid in valid_runner_ids
        ]
        if not clean_runner_ids:
            for cat in m1_data.qualified_categories:
                if cat.primary_winner.candidate_id in clean_primary_ids and cat.runner_up:
                    clean_runner_ids.append(cat.runner_up.candidate_id)

        clean_foil_ids = [
            cid for cid in raw_script.foil_sku_ids_referenced if cid in valid_foil_ids
        ]
        if not clean_foil_ids:
            for cat in m1_data.qualified_categories:
                if cat.primary_winner.candidate_id in clean_primary_ids:
                    clean_foil_ids.extend(f.candidate_id for f in cat.rejected_foils)

        # Sort beats by beat_index and normalize timing deterministically
        sorted_beats = sorted(raw_script.beats, key=lambda b: b.beat_index)
        timed_beats: List[ScriptBeatSegment] = []
        cursor_sec = 0.0
        total_words = 0

        for pos, b in enumerate(sorted_beats, start=1):
            vo = _cap_beat_voiceover(b.voiceover_vi, max_words=31)
            wc = len(vo.split()) if vo else 0
            dur = round(min(10.5, max(MIN_BEAT_DURATION_SEC, wc / DEFAULT_SPEAKING_RATE_WPS)), 1)
            start_s = round(cursor_sec, 1)
            end_s = round(start_s + dur, 1)
            cursor_sec = end_s
            total_words += wc

            wps = round(wc / dur, 2) if dur > 0 else 0.0
            onscreen = _trim_onscreen_text(b.on_screen_text_vi, MAX_ONSCREEN_WORDS)
            clip = self._pick_clip_for_beat(
                beat_index=pos,
                target_candidate_id=b.target_candidate_id,
                primary_sku_ids=clean_primary_ids,
                sku_by_id=sku_by_id,
                fallback_winner=fallback_winner,
            )

            timed_beats.append(
                ScriptBeatSegment(
                    beat_index=pos,
                    beat_type=b.beat_type,
                    start_sec=start_s,
                    end_sec=end_s,
                    duration_sec=dur,
                    target_candidate_id=b.target_candidate_id,
                    voiceover_vi=vo,
                    word_count=wc,
                    estimated_speaking_rate_wps=wps,
                    on_screen_text_vi=onscreen,
                    visual_instruction_vi=b.visual_instruction_vi.strip(),
                    source_youtube_clip=clip,
                )
            )

        total_dur = round(cursor_sec, 1)
        avg_wps = round(total_words / total_dur, 2) if total_dur > 0 else 0.0

        # Build deterministic M3 audit ground-truth anchors from M1 data
        claims: List[FactualClaimReference] = []
        offers: Dict[str, Optional[AffiliateListingMatch]] = {}
        for cid in clean_primary_ids:
            sku = sku_by_id[cid]
            claims.append(
                FactualClaimReference(
                    candidate_id=sku.candidate_id,
                    brand=sku.brand,
                    model_code=sku.model_code,
                    claimed_price_vnd=sku.estimated_market_price_vnd,
                    claimed_specs_mentioned=[f"{k}: {v}" for k, v in sku.key_specs.items()],
                    mandatory_caveat_mentioned_vi=sku.honest_caveat_vi,
                )
            )
            offers[cid] = sku.best_affiliate_offer

        cat_by_cid: Dict[str, QualifiedCategoryResult] = {}
        cat_by_id: Dict[str, QualifiedCategoryResult] = {}
        for cat in m1_data.qualified_categories:
            cat_by_id[cat.category_id] = cat
            cat_by_cid[cat.primary_winner.candidate_id] = cat
            for cand in cat.all_qualified_ranked:
                cat_by_cid[cand.candidate_id] = cat

        resolved_cat = (
            cat_by_cid.get(clean_primary_ids[0])
            or cat_by_id.get(raw_script.category_id)
            or m1_data.qualified_categories[0]
        )
        resolved_domain: DomainId = resolved_cat.domain
        resolved_category_id: str = resolved_cat.category_id

        slug_suffix = "_".join(clean_primary_ids[:2])
        script_id = f"s{idx}_{_slugify(raw_script.script_format)}_{_slugify(slug_suffix)}"
        hook_preview = timed_beats[0].voiceover_vi if timed_beats else ""
        short_hook = enforce_short_hook_title(
            raw_script.hook_title_short_vi
            or (timed_beats[0].on_screen_text_vi if timed_beats else "")
            or raw_script.title_vi,
            max_chars=30,
        )

        return ScriptDraftItem(
            script_id=script_id,
            script_format=raw_script.script_format,
            domain=resolved_domain,
            category_id=resolved_category_id,
            narrative_angle=raw_script.narrative_angle,
            title_vi=raw_script.title_vi.strip(),
            hook_title_short_vi=short_hook,
            hook_preview_vi=hook_preview,
            selection_rationale_vi=raw_script.selection_rationale_vi.strip(),
            primary_sku_ids=clean_primary_ids,
            runner_up_sku_ids_referenced=list(dict.fromkeys(clean_runner_ids)),
            foil_sku_ids_referenced=list(dict.fromkeys(clean_foil_ids)),
            total_duration_sec=total_dur,
            total_word_count=total_words,
            average_speaking_rate_wps=avg_wps,
            beats=timed_beats,
            factual_claims_for_m3_audit=claims,
            affiliate_offers_attached=offers,
        )

    def run(
        self,
        input_path: Path = INPUT_M1_PATH,
        output_path: Path = OUTPUT_M2_PATH,
        input_m1_5_path: Optional[Path] = None,
    ) -> M2ScriptDraftsOutput:
        if not input_path.exists():
            raise FileNotFoundError(f"M2 input contract not found: {input_path}")

        print(f"[M2] 1/3 Reading M1 contract from: {input_path}...", flush=True)
        m1_data = M1QualifiedSKUsOutput.model_validate_json(
            input_path.read_text(encoding="utf-8")
        )
        if not m1_data.qualified_categories:
            raise RuntimeError("M1 input has 0 qualified categories; cannot generate scripts.")

        # Filter out any category whose primary winner is RETIRED_EXHAUSTED in sku_registry.json
        surviving_cats = [
            cat
            for cat in m1_data.qualified_categories
            if self.registry_mgr.is_solo_eligible(cat.primary_winner.candidate_id)
            or self.registry_mgr.is_combo_eligible(cat.primary_winner.candidate_id)
        ]
        if surviving_cats:
            m1_data = m1_data.model_copy(update={"qualified_categories": surviving_cats})

        m1_5_data: Optional[M15ComboCuratorOutput] = None
        cand_m1_5 = input_m1_5_path or OUTPUT_M1_5_PATH
        if cand_m1_5.exists():
            try:
                loaded_m1_5 = M15ComboCuratorOutput.model_validate_json(
                    cand_m1_5.read_text(encoding="utf-8")
                )
                if loaded_m1_5.source_m1_run_id == m1_data.m1_run_id or input_m1_5_path is not None:
                    m1_5_data = loaded_m1_5
            except Exception:
                pass

        sku_by_id, _, valid_runner_ids, valid_foil_ids = self._index_m1_entities(m1_data)
        if m1_5_data and m1_5_data.selected_combo:
            for mem in m1_5_data.selected_combo.members:
                if mem.candidate_id not in sku_by_id:
                    sku_by_id[mem.candidate_id] = EvaluatedCandidateSKU(
                        candidate_id=mem.candidate_id,
                        rank_in_category=1,
                        selection_role="PRIMARY_WINNER",
                        brand=mem.brand,
                        model_code=mem.model_code,
                        product_name_vi=mem.product_name_vi,
                        archetype="BRANDED_MAINSTREAM",
                        estimated_market_price_vnd=mem.estimated_market_price_vnd,
                        price_vs_median_ratio=1.0,
                        key_specs={},
                        highlight_pros_vi=[mem.role_in_combo_vi],
                        honest_caveat_vi=mem.honest_caveat_vi,
                        caveat_severity="MINOR",
                        score_breakdown=ScoreBreakdown100(
                            price_value_and_specs_score=30.0,
                            durability_and_caveat_score=24.0,
                            evidence_and_visual_score=22.0,
                            affiliate_readiness_bonus=0.0,
                            total_score_100=76.0,
                            score_explanation_vi="Injected from M1.5 Combo Curator registry",
                        ),
                        affiliate_status="NEEDS_LINK_LOOKUP",
                        best_affiliate_offer=mem.best_affiliate_offer,
                        youtube_evidence=mem.youtube_evidence,
                        web_evidence_snippets=[],
                    )

        print(
            f"[M2] 2/3 Classifying relationship & generating timed 9:16 scripts across "
            f"{len(m1_data.qualified_categories)} category winners via Gemini (1 call)...",
            flush=True,
        )

        system_instruction = (
            "Bạn là Đạo diễn Kịch bản Short-Video Review Trung Thực (Module M2) cho kênh Faceless 9:16.\n"
            "TRIẾT LÝ CỐT LÕI:\n"
            "1. KHÔNG GƯỢNG ÉP COMBO: Nếu Module M1.5 đã duyệt `COMBO_APPROVED`, hãy viết 1 kịch bản `COMBO_WORKFLOW_SYNERGY` bám sát kịch bản phối hợp của M1.5. "
            "Nếu M1.5 trả về `BYPASS` và các nhóm không phải cặp kích cỡ `SIZE_OR_TIER_PAIR`, hãy bỏ qua kịch bản combo.\n"
            "2. CHÍNH XÁC 100% DỮ LIỆU M1: Mọi tên hãng, model_code, dung tích, chất liệu, giá cả và nhược điểm phải khớp đúng dữ liệu M1 cung cấp. Tuyệt đối không bịa thêm thông số.\n"
            "3. TRUNG THỰC TĂNG UY TÍN: Mọi kịch bản đều bắt buộc có phân cảnh `HONEST_CAVEAT` ở `beat_index = 4` nói thẳng nhược điểm thực tế.\n"
            "4. TIẾNG VIỆT CÓ DẤU CHUẨN XÁC & HOOK NGẮN <= 30 KÝ TỰ: Mọi trường văn bản (`title_vi`, `hook_title_short_vi`, `voiceover_vi`, `on_screen_text_vi`, `selection_rationale_vi`, `synergy_classification_reasoning_vi`) "
            "BẮT BUỘC phải viết bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ, chuẩn chính tả (UTF-8), trong đó `hook_title_short_vi` dài từ 10 đến 30 ký tự."
        )

        prompt = self._build_prompt(m1_data, m1_5_data=m1_5_data)
        llm_resp, model_used = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMM2BatchGenerationResponse,
            system_instruction=system_instruction,
            prefer_lite=False,
            temperature=0.25,
        )

        print(
            f"[M2]     -> Classified as {llm_resp.multi_category_synergy_type} ({model_used}); "
            f"post-processing {len(llm_resp.scripts)} script draft(s)...",
            flush=True,
        )

        processed_scripts: List[ScriptDraftItem] = []
        used_angles: set[str] = set()
        solo_angle_pool: List[NarrativeAngle] = [
            "PAIN_POINT_REALITY_CHECK",
            "MYTH_VS_FACT_TEARDOWN",
            "TIME_AND_EFFORT_MATH",
            "DECISION_GUIDE_BY_USER_TYPE",
            "WORKFLOW_UPGRADE_JOURNEY",
        ]
        multi_angle_pool: List[NarrativeAngle] = [
            "DECISION_GUIDE_BY_USER_TYPE",
            "WORKFLOW_UPGRADE_JOURNEY",
            "TIME_AND_EFFORT_MATH",
            "MYTH_VS_FACT_TEARDOWN",
            "PAIN_POINT_REALITY_CHECK",
        ]

        for idx, raw_s in enumerate(llm_resp.scripts, start=1):
            draft = self._postprocess_script(
                idx=idx,
                raw_script=raw_s,
                m1_data=m1_data,
                sku_by_id=sku_by_id,
                valid_runner_ids=valid_runner_ids,
                valid_foil_ids=valid_foil_ids,
            )
            if draft.narrative_angle in used_angles:
                pool = multi_angle_pool if draft.script_format != "SINGLE_SKU_DEEP_DIVE" else solo_angle_pool
                for candidate_angle in pool:
                    if candidate_angle not in used_angles:
                        draft.narrative_angle = candidate_angle
                        break
            used_angles.add(draft.narrative_angle)
            processed_scripts.append(draft)
            print(
                f"     -> [{draft.script_format} | {draft.domain} | {draft.category_id} | {draft.narrative_angle}] "
                f"{draft.title_vi} | {len(draft.beats)} beats | {draft.total_word_count} words | {draft.total_duration_sec}s",
                flush=True,
            )

        now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        m2_run_id = f"m2_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{m1_data.sub_niche_id}"

        output_obj = M2ScriptDraftsOutput(
            m2_run_id=m2_run_id,
            source_m1_run_id=m1_data.m1_run_id,
            generated_at=now_iso,
            domain=m1_data.domain,
            domain_name_vi=m1_data.domain_name_vi,
            sub_niche_id=m1_data.sub_niche_id,
            sub_niche_name_vi=m1_data.sub_niche_name_vi,
            problem_context_vi=m1_data.problem_context_vi,
            multi_category_synergy_type=llm_resp.multi_category_synergy_type,
            synergy_classification_reasoning_vi=llm_resp.synergy_classification_reasoning_vi.strip(),
            llm_models_used=self.llm.models_used_history,
            script_drafts=processed_scripts,
        )

        print(f"[M2] 3/3 Writing M2 contract to: {output_path}...", flush=True)
        atomic_write_text(output_path, output_obj.model_dump_json(indent=2))
        print("[M2] Completed!", flush=True)
        return output_obj


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Module M2: Script Classifier & Generator.")
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_M1_PATH,
        help="Input JSON path from M1 (default: data/m1_qualified_skus.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_M2_PATH,
        help="Output JSON path for M3 (default: data/m2_script_drafts.json)",
    )
    args = parser.parse_args()

    engine = M2ScriptGeneratorEngine()
    engine.run(input_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
