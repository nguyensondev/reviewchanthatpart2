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
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import atomic_write_text
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.schemas_m0 import AffiliateListingMatch, DomainId, YouTubeEvidenceClip
from pipeline.schemas_m1 import EvaluatedCandidateSKU, M1QualifiedSKUsOutput, QualifiedCategoryResult, ScoreBreakdown100
from pipeline.schemas_m1_5 import M15ComboCuratorOutput
from pipeline.schemas_m_trend import load_active_trend_opportunities
from pipeline.shared_utils import check_vietnamese_diacritics, enforce_short_hook_title
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

DEFAULT_SPEAKING_RATE_WPS = 3.9
MIN_BEAT_DURATION_SEC = 5.0
MAX_ONSCREEN_WORDS = 8


def _trim_onscreen_text(text: str, max_words: int = MAX_ONSCREEN_WORDS) -> str:
    words = text.strip().split()
    if len(words) <= max_words:
        return " ".join(words)
    return " ".join(words[:max_words])


def _cap_beat_voiceover(text: str, max_words: int = 42) -> str:
    """Normalize whitespace and ensure complete punctuation within 24-42 words per beat (~35-52s total)."""
    clean = " ".join(text.strip().split())
    words = clean.split()
    if len(words) <= max_words:
        if clean and clean[-1] not in ".!?":
            return clean.rstrip(",;:") + "."
        return clean
    prefix = " ".join(words[:max_words])
    for sep in (".", "!", "?"):
        idx = prefix.rfind(sep)
        if idx > 0 and len(prefix[:idx].split()) >= 24:
            out = prefix[:idx + 1].strip()
            return out
    out = prefix.rstrip(",;:")
    return out + ("." if out and out[-1] not in ".!?" else "")


def _slugify(text: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower()).strip("_")
    return s or "script"


UPGRADE_1_ANGLE_POOL: List[NarrativeAngle] = [
    "REVERSE_CAVEAT_FIRST",
    "HYPE_VS_REALITY_TEST",
    "SPEC_TRAP_AND_HIDDEN_COST",
    "ANTI_DUST_DRAWER_TEST",
    "HABIT_DIAGNOSIS_STORY",
    "FAST_25S_MICRO_LOOP",
]


def recommend_angles_for_category(
    category_id: str,
    domain: str,
    is_combo: bool = False,
) -> List[NarrativeAngle]:
    """
    Smart Context-Matched Angle Recommender (Upgrade 1):
    Maps each product category to the storytelling formats that fit its real physical/usage nature
    so M2 never applies an awkward or hallucinated angle.
    """
    cid = (category_id or "").lower()
    if is_combo:
        return [
            "HABIT_DIAGNOSIS_STORY",
            "HYPE_VS_REALITY_TEST",
            "REVERSE_CAVEAT_FIRST",
            "SPEC_TRAP_AND_HIDDEN_COST",
        ]
    # 1. Power, hubs, cables, chargers, vacuum sealers, precision scales -> Spec traps & hidden cost shine!
    if any(k in cid for k in ("charger", "gan", "hub", "cable", "vacuum", "scale", "power")):
        return [
            "SPEC_TRAP_AND_HIDDEN_COST",
            "REVERSE_CAVEAT_FIRST",
            "FAST_25S_MICRO_LOOP",
            "ANTI_DUST_DRAWER_TEST",
            "HYPE_VS_REALITY_TEST",
        ]
    # 2. Viral desk/kitchen aesthetic gear -> "Thực chiến hay làm màu?" & Habit diagnosis shine!
    if any(k in cid for k in ("light", "arm", "sprayer", "soap", "dispenser")):
        return [
            "HYPE_VS_REALITY_TEST",
            "HABIT_DIAGNOSIS_STORY",
            "REVERSE_CAVEAT_FIRST",
            "FAST_25S_MICRO_LOOP",
            "SPEC_TRAP_AND_HIDDEN_COST",
        ]
    # 3. Handheld prep & cleaning tools -> "Bài test vứt xó / vệ sinh 10 giây" shines!
    if any(k in cid for k in ("frother", "grinder", "thermometer", "duster", "timer", "chopper")):
        return [
            "ANTI_DUST_DRAWER_TEST",
            "FAST_25S_MICRO_LOOP",
            "REVERSE_CAVEAT_FIRST",
            "SPEC_TRAP_AND_HIDDEN_COST",
            "HYPE_VS_REALITY_TEST",
        ]
    # 4. Ergonomic daily-driver gear & temperature kettles -> Habit diagnosis & Reverse caveat shine!
    if any(k in cid for k in ("mouse", "stand", "keyboard", "kettle")) or domain == "TECH_DESK":
        return [
            "HABIT_DIAGNOSIS_STORY",
            "REVERSE_CAVEAT_FIRST",
            "HYPE_VS_REALITY_TEST",
            "SPEC_TRAP_AND_HIDDEN_COST",
            "FAST_25S_MICRO_LOOP",
        ]
    return list(UPGRADE_1_ANGLE_POOL)


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
        brand_gate = OfficialBrandVideoGate()
        category_blocks: List[str] = []
        assigned_batch_angles: List[NarrativeAngle] = []
        fast_micro_used = False
        for idx, cat in enumerate(m1_data.qualified_categories, start=1):
            w = cat.primary_winner
            ru = cat.runner_up
            v = cat.head_to_head_verdict

            clean_footage_sec = brand_gate.estimate_sku_clean_footage_sec(
                w.brand, w.model_code, w.candidate_id, w.youtube_evidence
            )
            rec_angles = recommend_angles_for_category(cat.category_id, cat.domain, is_combo=False)
            # Deterministic day/category rotation across suitable angles while ensuring batch uniqueness
            rot_seed = sum(ord(ch) for ch in f"{m1_data.m1_run_id}::{cat.category_id}")
            rotated = [rec_angles[(rot_seed + i) % len(rec_angles)] for i in range(len(rec_angles))]
            chosen_angle: NarrativeAngle = rotated[0]
            for cand_ang in rotated + UPGRADE_1_ANGLE_POOL:
                if cand_ang == "FAST_25S_MICRO_LOOP" and (fast_micro_used or clean_footage_sec >= 50.0 and idx == 1):
                    continue
                if cand_ang not in assigned_batch_angles:
                    chosen_angle = cand_ang
                    break
            if chosen_angle == "FAST_25S_MICRO_LOOP":
                fast_micro_used = True
            assigned_batch_angles.append(chosen_angle)

            if chosen_angle == "FAST_25S_MICRO_LOOP":
                word_budget_str = "85 – 108 từ (mỗi beat 17 – 22 từ, nhịp siêu tốc dồn dập ~24s – 27s giúp kéo tỷ lệ xem lặp >100%)"
            elif clean_footage_sec < 38.0:
                word_budget_str = "125 – 145 từ (mỗi beat 24 – 30 từ, tương ứng ~33s – 38s để vừa khít thời lượng video gốc chính hãng mà KHÔNG BAO GIỜ phải lặp cảnh)"
            else:
                word_budget_str = "150 – 180 từ (mỗi beat 29 – 36 từ, tương ứng ~40s – 47s)"

            runner_line = (
                f"  - Mẫu so sánh trực tiếp (`{ru.candidate_id}`): {ru.brand} ({ru.model_code or 'N/A'}) — {ru.product_name_vi} | "
                f"Giá ~{ru.estimated_market_price_vnd:,}đ | Điểm {ru.score_breakdown.total_score_100}/100 | "
                f"Nhược điểm: {ru.honest_caveat_vi}"
                if ru
                else "  - Mẫu so sánh trực tiếp: (Không có)"
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
- FORMAT KỂ CHUYỆN KHỚP NGỮ CẢNH BẮT BUỘC CHO KỊCH BẢN SOLO NÀY (`narrative_angle`): **`{chosen_angle}`** (Các phương án dự phòng hợp sản phẩm: `{rec_angles[:3]}`)
- Vấn đề giải quyết: {cat.problem_solved_vi}
- SẢN PHẨM CHÍNH (`{w.candidate_id}`): {w.brand} | Model: {w.model_code or 'N/A'} | Tên: {w.product_name_vi}
  + Giá thị trường chuẩn: {w.estimated_market_price_vnd:,}đ ({w.price_vs_median_ratio}x trung vị) | Điểm M1: {w.score_breakdown.total_score_100}/100
  + Thời lượng cảnh quay sạch chính hãng hiện có: ~{clean_footage_sec:.0f}s -> NGÂN SÁCH ĐỘ DÀI KỊCH BẢN TỐI ƯU: **{word_budget_str}**
  + Thông số chuẩn: {specs_str}
  + Ưu điểm nổi bật: {"; ".join(w.highlight_pros_vi)}
  + Nhược điểm thực tế BẮT BUỘC PHẢI NÓI (`{w.caveat_severity}`): {w.honest_caveat_vi}
{runner_line}
- HÀNG GIÁ RẺ / LỖI NÊN TRÁNH (`rejected_foils`): {foil_lines}
- GHI CHÚ THẨM ĐỊNH TỪ M1:
  + Điểm ăn tiền của sản phẩm chính: {v.why_winner_chosen_vi}
  + Khác biệt thực tế so với mẫu phổ thông: {v.why_winner_beats_runner_up_vi}
  + Bẫy hàng rẻ/OEM: {v.why_avoid_rejected_foils_vi}
  + Góc nhìn công tâm về điểm trừ: {v.honest_caveat_framing_vi}
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
                f"   - {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE` (mỗi sản phẩm chính 1 kịch bản riêng).\n"
                f"   - 1 kịch bản `COMBO_WORKFLOW_SYNERGY` ghép đúng các SKU `{[m.candidate_id for m in cb.members]}` theo sát tình huống `joint_workflow_scenario_vi` của M1.5 ở trên."
            )
        elif m1_5_data and m1_5_data.status == "BYPASS":
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
                    f"và CHỈ TẠO ĐÚNG {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE` (mỗi sản phẩm chính 1 kịch bản riêng, 0 kịch bản combo)."
                )
        elif num_cats == 1:
            multi_rule = (
                "1. Chỉ có 1 nhóm công năng -> đặt `multi_category_synergy_type = \"SINGLE_CATEGORY_ONLY\"` và chỉ tạo 1 kịch bản `SINGLE_SKU_DEEP_DIVE`."
            )
        else:
            multi_rule = (
                "1. Có từ 2 nhóm công năng trở lên -> PHÂN LOẠI MỐI QUAN HỆ TRƯỚC:\n"
                "   - Nếu 2 sản phẩm chính thực chất là CÙNG MỘT DÒNG CÔNG DỤNG nhưng khác kích cỡ/dung tích/tầm giá "
                "-> KHÔNG ĐƯỢC ÉP THÀNH COMBO MUA CẢ HAI! Hãy chọn `multi_category_synergy_type = \"SIZE_OR_TIER_PAIR\"` và tạo kịch bản ghép dạng **`DUAL_TIER_COMPARISON`**.\n"
                "   - Nếu 2 sản phẩm chính thuộc 2 CÔNG ĐOẠN KHÁC NHAU bổ trợ liền mạch trong 1 quy trình "
                "-> chọn `multi_category_synergy_type = \"COMPLEMENTARY_WORKFLOW\"` và tạo kịch bản ghép dạng **`COMBO_WORKFLOW_SYNERGY`**.\n"
                f"2. SỐ LƯỢNG KỊCH BẢN PHẢI TẠO TRONG `scripts`: ĐÚNG {num_cats + 1} KỊCH BẢN gồm:\n"
                f"   - {num_cats} kịch bản `SINGLE_SKU_DEEP_DIVE`.\n"
                "   - 1 kịch bản ghép đa SKU (`DUAL_TIER_COMPARISON` hoặc `COMBO_WORKFLOW_SYNERGY`)."
            )

        # Upgrade 2 (M_TREND Hook Context): Inject live non-expired trend context if any M1 category matches
        active_trends_by_cat = {
            t.product_concept_id: t for t in load_active_trend_opportunities()
        }
        trend_context_lines: List[str] = []
        for cat in m1_data.qualified_categories:
            t_opp = active_trends_by_cat.get(cat.category_id)
            if t_opp:
                trend_context_lines.append(
                    f"- Nhóm `{cat.category_id}` đang có Trend [{t_opp.priority_tier} | Điểm={t_opp.score_breakdown.effective_score_now_100}/100]: "
                    f"Chủ đề='{t_opp.trend_title_vi}' | Nỗi đau='{t_opp.need_pain_point_vi}' | "
                    f"Gợi ý mở màn Hook 3s đầu: \"{t_opp.trend_hook_angle_vi}\""
                    + (
                        f" | ⚠️ TÁI NHẬP TREND MỚI: {t_opp.re_entry_note_vi}"
                        if t_opp.re_entry_action != "NEW_OPPORTUNITY"
                        else ""
                    )
                )
        trend_context_block = (
            "\nNGỮ CẢNH TREND THỜI SỰ TỪ MODULE M_TREND (LỒNG GHÉP TỰ NHIÊN VÀO HOOK BEAT 1, KHÔNG GHI NGÀY THÁNG CỨNG):\n"
            + "\n".join(trend_context_lines)
            + "\n"
            if trend_context_lines
            else ""
        )

        return f"""
Bối cảnh ngách từ M1:
- Lĩnh vực: `{m1_data.domain}` — {m1_data.sub_niche_name_vi}
- Nỗi đau người dùng: {m1_data.problem_context_vi}
{trend_context_block}
Dữ liệu các sản phẩm đã được M1 thẩm định:

{chr(10).join(category_blocks)}
{combo_section_str}
YÊU CẦU TẠO KỊCH BẢN (`LLMM2BatchGenerationResponse`):
{multi_rule}
- MỌI KỊCH BẢN BẮT BUỘC ĐIỀN `hook_title_short_vi` (tiêu đề mở bài hiển thị 2s đầu video, NGẮN GỌN TỐI ĐA 30 KÝ TỰ, có dấu tiếng Việt chuẩn, không dài dòng che nội dung).

KHO 6 FORMAT KỂ CHUYỆN MỚI LẠ KHỚP NGỮ CẢNH (`narrative_angle` — UPGRADE 1):
Mỗi kịch bản trong `scripts` BẮT BUỘC phải điền đúng `domain`, đúng `category_id`, và dùng đúng `narrative_angle` đã được ghép thông minh cho nhóm đó (TUYỆT ĐỐI KHÔNG HAI KỊCH BẢN NÀO TRONG ĐỢT TRÙNG `narrative_angle`):
1. `REVERSE_CAVEAT_FIRST` (**Bóc Điểm Trừ Ngay 3 Giây Đầu**): Mở màn Beat 1 bằng lời can ngăn thẳng thắn về giới hạn thật của sản phẩm (*"Khoan xuống tiền mua chiếc ... này nếu bạn..."*) kèm cảnh báo bẫy hàng rẻ -> Beat 2 & 3 lật ngược thế cờ chứng minh vì sao với đúng tệp người dùng thì nó lại cực kỳ đáng giá -> Beat 4 phân tích kỹ cách dùng khắc phục giới hạn đó -> Beat 5 chốt nhanh ai hợp/ai bỏ qua.
2. `HYPE_VS_REALITY_TEST` (**Thực Chiến Hay Chỉ Làm Màu?**): Mở màn Beat 1 đặt thẳng câu hỏi hoài nghi của đám đông (*"Món ... này thực sự hiệu quả hay chỉ mua về decor làm màu tốn tiền?"*) + so với hàng chợ làm màu -> Beat 2 & 3 giải mã bằng cơ chế vật lý/quang học/thiết kế cốt lõi -> Beat 4 nêu thẳng 1 điểm trừ thực tế -> Beat 5 chấm mức độ thực dụng & mức giá.
3. `SPEC_TRAP_AND_HIDDEN_COST` (**Thợ Soi Thông Số Ảo & Cái Giá Ngầm**): Mở màn Beat 1 vạch trần chiêu nổ thông số ảo hoặc cái giá ngầm khi ham hàng trôi nổi giá rẻ (dựa đúng dữ liệu `rejected_foils` của M1, không bịa chuyện vô lý) -> Beat 2 & 3 đối chứng thông số chuẩn xác và độ hoàn thiện của hàng chính hãng -> Beat 4 nêu rõ giới hạn thực tế -> Beat 5 chốt bài toán đáng tiền.
4. `ANTI_DUST_DRAWER_TEST` (**Bài Kiểm Tra Vứt Xó & Độ Lười Đời Thường**): Mở màn Beat 1 chạm trúng tâm lý *"mua về 3 bữa vứt xó đóng bụi vì lích kích/lười vệ sinh"* (như mấy mẫu rẻ khó dùng) -> Beat 2 & 3 chứng minh thao tác gọn lẹ, dễ lau rửa/sử dụng hàng ngày của sản phẩm chính -> Beat 4 nói thẳng điểm trừ thật -> Beat 5 chốt đối tượng dùng mỗi ngày.
5. `HABIT_DIAGNOSIS_STORY` (**Bắt Bệnh Thói Quen Đời Thường**): Mở màn Beat 1 đọc vị trúng phóc triệu chứng quen thuộc (4h chiều mỏi cổ tay, 11h đêm nhức mắt, căn bếp vướng víu...) -> Beat 2 & 3 dẫn mượt sang cơ chế giải quyết triệt để của sản phẩm chính so với đồ rẻ -> Beat 4 lưu ý thẳng nhược điểm thực tế -> Beat 5 chốt giá & nơi xem hàng chuẩn hãng.
6. `FAST_25S_MICRO_LOOP` (**Siêu Tốc 24–27 Giây Kéo Loop & Tương Tác**): Tổng độ dài **85 – 108 từ (mỗi beat 17 – 22 từ)**, nhịp dồn dập gãy gọn không rườm rà: Beat 1 vào thẳng vấn đề + bẫy đồ rẻ; Beat 2 & 3 chốt 2–3 điểm sáng ăn tiền nhất; Beat 4 chốt 1 điểm trừ thật trong 1 câu gọn; Beat 5 báo giá + đặt 1 câu hỏi chia 2 thói quen người dùng để kích thích bình luận + chỉ chỗ xem chính hãng.

NGUYÊN TẮC VÀNG: VĂN NÓI ĐỜI THƯỜNG CỦA NGƯỜI THẬT — CẤM TUYỆT ĐỐI VĂN MẪU AI / BROCHURE:
1. **CẤM TUYỆT ĐỐI các cụm từ sáo rỗng mang mùi AI hoặc thuật ngữ nội bộ**:
   - CẤM viết trong lời thoại: *"mẫu Quán quân"*, *"mẫu Á quân"*, *"mẫu đối chứng"*, *"Sản phẩm hoàn thiện chắc chắn từ..."*, *"Giải pháp chuẩn chỉnh nhất chính là..."*, *"Hãy đầu tư nghiêm túc vào..."*, *"Mã này trang bị..."*, *"sở hữu tính năng"*, *"mang lại trải nghiệm tuyệt vời"*, *"Tổng kết lại, với khoảng..."*.
   - Hãy viết đúng như một người bạn / reviewer có nghề đang trò chuyện trực tiếp, tự nhiên, gần gũi và **thể hiện rõ cá tính của `narrative_angle` được giao** ngay từ câu đầu tiên của Beat 1!
2. **ĐỘ DÀI TỰ NHIÊN THEO ĐÚNG `NGÂN SÁCH ĐỘ DÀI KỊCH BẢN TỐI ƯU` CỦA TỪNG SKU / FORMAT**:
   - Nếu `narrative_angle == "FAST_25S_MICRO_LOOP"`: viết gọn ghẽ **85 – 108 từ** (17 – 22 từ/beat, trọn câu không cụt ý).
   - Các format còn lại: bám sát ngân sách từ (`125 – 145 từ` hoặc `150 – 180 từ`) đã tính sẵn ở từng nhóm công năng bên trên.
3. **CẤU TRÚC 5 PHÂN CẢNH (`beat_index = 1..5`) MƯỢT MÀ & KHỚP CẢNH QUAY (`visual_instruction_vi`)**:
   - Beat 1 (`HOOK_FOIL_WARNING`): Mở màn sắc bén theo đúng tinh thần của `narrative_angle` được giao + đối chiếu tự nhiên với điểm yếu hàng rẻ (`rejected_foils`, đúng giá từ M1).
   - Beat 2 (`WINNER_REVEAL_SPECS` / `COMBO_STEP_1`): Lời dẫn tự nhiên giới thiệu thương hiệu + `model_code` + cơ chế/vật liệu/thông số cốt lõi giải quyết vấn đề ngoài đời thực.
   - Beat 3 (`HEAD_TO_HEAD_PROOF` / `COMBO_STEP_2_TRANSITION`): Đào sâu trải nghiệm thực tế các điểm sáng đắt giá nhất (`highlight_pros_vi`) vượt trội so với dòng phổ thông.
   - Beat 4 (`HONEST_CAVEAT`): Nói rõ nhược điểm thực tế từ `honest_caveat_vi` dưới góc nhìn công bằng (ai thấy bất tiện, ai dùng vẫn rất ổn).
   - Beat 5 (`VALUE_VERDICT_CTA`): Chốt nhanh ai nên mua ở mức giá ~`estimated_market_price_vnd` (kèm câu hỏi ngắn gợi bình luận tự nhiên nếu hợp mạch) và nhắn nhẹ tham khảo gian hàng chính hãng ở phần bình luận/bio.

RÀNG BUỘC KỸ THUẬT & NGÔN NGỮ TUYỆT ĐỐI (ZERO TOLERANCE):
- NGÔN NGỮ VOICEOVER, PHỤ ĐỀ & TIÊU ĐỀ: BẮT BUỘC 100% viết bằng **tiếng Việt CÓ DẤU ĐẦY ĐỦ (UTF-8 chuẩn)**, đúng chính tả, câu văn hoàn chỉnh không cụt ý. TUYỆT ĐỐI CẤM viết tiếng Việt không dấu (ASCII) ở bất kỳ từ hay câu nào — chỉ cần 1 cụm từ mất dấu là toàn bộ kịch bản bị hủy bỏ vì làm hỏng AI TTS!
- `target_candidate_id` ở mỗi beat phải dùng đúng chuỗi `candidate_id` có thật trong dữ liệu M1.
- `on_screen_text_vi`: Ngắn gọn, súc tích, tiếng Việt CÓ DẤU ĐẦY ĐỦ, TUYỆT ĐỐI KHÔNG QUÁ 8 TỪ.
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

        is_fast_micro = raw_script.narrative_angle == "FAST_25S_MICRO_LOOP"
        beat_word_cap = 24 if is_fast_micro else 42
        min_beat_dur = 4.2 if is_fast_micro else MIN_BEAT_DURATION_SEC

        for pos, b in enumerate(sorted_beats, start=1):
            vo = _cap_beat_voiceover(b.voiceover_vi, max_words=beat_word_cap)
            wc = len(vo.split()) if vo else 0
            dur = round(min(13.5, max(min_beat_dur, wc / DEFAULT_SPEAKING_RATE_WPS)), 1)
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

    def _build_rewrite_prompt(
        self,
        m1_data: M1QualifiedSKUsOutput,
        m1_5_data: Optional[M15ComboCuratorOutput],
        failed_audits: List[ dict ],
        existing_drafts_by_id: Dict[str, ScriptDraftItem],
    ) -> str:
        base_gt = self._build_prompt(m1_data, m1_5_data=m1_5_data)
        critique_blocks: List[str] = []
        for idx, fa in enumerate(failed_audits, start=1):
            sid = str(fa.get("script_id") or "")
            old_draft = existing_drafts_by_id.get(sid)
            old_beats_str = ""
            if old_draft:
                old_beats_str = "\n".join(
                    f"    Beat {b.beat_index} [{b.beat_type}] ({b.word_count} từ): \"{b.voiceover_vi}\""
                    for b in old_draft.beats
                )
            issues_str = "\n".join(
                f"    - [{iss.get('severity')}] ({iss.get('issue_category')} @ Beat {iss.get('beat_index')}): "
                f"{iss.get('description_vi')} -> Hướng xử lý: {iss.get('resolution_applied_vi')}"
                for iss in (fa.get("issues_found") or [])
            ) or "    - (Xem chỉ thị tổng hợp bên dưới)"
            verdict_str = str(fa.get("auditor_verdict_vi") or "")
            rewrite_inst = str(fa.get("rewrite_instructions_for_m2_vi") or "")

            critique_blocks.append(
                f"""
=== KỊCH BẢN BỊ ĐÁNH TRƯỢT CẦN VIẾT LẠI #{idx}: `{sid}` ===
- Định dạng (`script_format`): `{fa.get('script_format')}`
- Chủ đề (`domain`): `{fa.get('domain')}` | Nhóm (`category_id`): `{fa.get('category_id')}`
- Góc tiếp cận bắt buộc (`narrative_angle`): `{fa.get('narrative_angle')}`
- Các SKU chính (`primary_sku_ids`): {fa.get('primary_sku_ids')}
- Điểm QA/QC vòng trước: {fa.get('qaqc_score_breakdown', {}).get('total_qaqc_score_100', 0)}/100 (TRƯỢT - FAILED_NEEDS_REWRITE)
- Kết luận của Giám khảo M3 (`auditor_verdict_vi`): {verdict_str}
- CHỈ THỊ SỬA LỖI BẮT BUỘC TỪ M3 (`rewrite_instructions_for_m2_vi`):
  {rewrite_inst}
- Chi tiết các lỗi bị M3 bắt:
{issues_str}
- Bản nháp cũ bị đánh trượt (TUYỆT ĐỐI KHÔNG lặp lại lối viết sơ sài/lỗi này):
{old_beats_str}
""".strip()
            )

        return f"""
{base_gt}

======================================================================
🚨 YÊU CẦU VIẾT LẠI KỊCH BẢN THEO BIÊN BẢN PHÊ BÌNH CỦA MODULE M3 QA/QC 🚨
======================================================================
Module kiểm định chất lượng M3 vừa đánh trượt (`FAILED_NEEDS_REWRITE`) {len(failed_audits)} kịch bản dưới đây.
NHIỆM VỤ CỦA BẠN:
1. Chỉ xuất đúng {len(failed_audits)} kịch bản cần viết lại trong mảng `scripts` (theo đúng thứ tự bên dưới, giữ đúng `script_format`, `domain`, `category_id`, `narrative_angle` và `primary_sku_ids` của từng kịch bản).
2. KHẮC PHỤC TRIỆT ĐỂ 100% các lỗi và góp ý trong `rewrite_instructions_for_m2_vi` của M3:
   - Mở bài (Beat 1) phải có Hook giàu hình ảnh thực tế, chạm đúng nỗi đau người dùng.
   - Đầu các Beat 2, 3, 4, 5 BẮT BUỘC có lời dẫn chuyển ý mượt mà, tự nhiên.
   - BẮT BUỘC nêu bật đầy đủ các điểm sáng (`Ưu điểm nổi bật` & `Thông số chuẩn` từ M1), biến thông số thành lợi ích trải nghiệm thật, độ dài đúng chuẩn format (`85 – 110 từ` nếu `narrative_angle == 'FAST_25S_MICRO_LOOP'`; `135 – 185 từ` cho các format còn lại, trọn vẹn câu chữ).
   - Beat 4 (`HONEST_CAVEAT`) nói thẳng nhược điểm thực tế từ M1 nhưng đặt trong ngữ cảnh công bằng, khách quan.

{chr(10).join(critique_blocks)}
""".strip()

    def run(
        self,
        input_path: Path = INPUT_M1_PATH,
        output_path: Path = OUTPUT_M2_PATH,
        input_m1_5_path: Optional[Path] = None,
        m3_feedback_path: Optional[Path] = None,
        filter_retired_skus: bool = False,
    ) -> M2ScriptDraftsOutput:
        if not input_path.exists():
            raise FileNotFoundError(f"M2 input contract not found: {input_path}")

        print(f"[M2] 1/3 Reading M1 contract from: {input_path}...", flush=True)
        m1_data = M1QualifiedSKUsOutput.model_validate_json(
            input_path.read_text(encoding="utf-8")
        )
        if not m1_data.qualified_categories:
            raise RuntimeError("M1 input has 0 qualified categories; cannot generate scripts.")

        if filter_retired_skus:
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
            "1. KỂ CHUYỆN CUỐN HÚT & NỐI MẠCH MƯỢT MÀ: Mỗi kịch bản phải có Hook mở màn giàu hình ảnh thực tế, có lời dẫn chuyển ý (transition) tự nhiên giữa cả 5 phân cảnh, và biến thông số kỹ thuật khô khan thành trải nghiệm đời thực sống động.\n"
            "2. KHAI THÁC XỨNG TẦM ĐIỂM SÁNG CỦA SKU: Không viết sơ sài lướt qua! Phải làm nổi bật trọn vẹn các ưu điểm đắt giá (`highlight_pros_vi`) và thông số cốt lõi của SKU đã được M1 tuyển chọn (độ dài linh hoạt 145–195 từ tùy độ dày thông tin của SKU).\n"
            "3. KHÔNG GƯỢNG ÉP COMBO: Nếu Module M1.5 đã duyệt `COMBO_APPROVED`, hãy viết 1 kịch bản `COMBO_WORKFLOW_SYNERGY` bám sát kịch bản phối hợp của M1.5.\n"
            "4. CHÍNH XÁC 100% DỮ LIỆU M1 & NHƯỢC ĐIỂM CÔNG TÂM: Mọi tên hãng, model_code, thông số, giá cả phải khớp đúng M1. Phân cảnh `HONEST_CAVEAT` (Beat 4) nói thẳng nhược điểm thật nhưng đặt trong ngữ cảnh khách quan, công bằng.\n"
            "5. TIẾNG VIỆT CÓ DẤU CHUẨN XÁC & HOOK NGẮN <= 30 KÝ TỰ: Mọi trường văn bản phải viết bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ, câu văn hoàn chỉnh không cụt ý, `hook_title_short_vi` dài từ 10 đến 30 ký tự."
        )

        # Check if running in Targeted Rewrite Mode from M3 QA/QC feedback
        if m3_feedback_path and m3_feedback_path.exists() and output_path.exists():
            existing_m2 = M2ScriptDraftsOutput.model_validate_json(
                output_path.read_text(encoding="utf-8")
            )
            m3_raw = json.loads(m3_feedback_path.read_text(encoding="utf-8"))
            failed_audits = [
                a
                for a in (m3_raw.get("audited_scripts") or [])
                if isinstance(a, dict) and a.get("qaqc_status") == "FAILED_NEEDS_REWRITE"
            ]
            if not failed_audits:
                print("[M2 REWRITE] Không có kịch bản nào ở trạng thái FAILED_NEEDS_REWRITE. Giữ nguyên bản M2 hiện tại.", flush=True)
                return existing_m2

            print(
                f"[M2 REWRITE] Nhận biên bản phê bình từ M3 -> Viết lại {len(failed_audits)} kịch bản bị đánh trượt...",
                flush=True,
            )
            existing_by_id = {d.script_id: d for d in existing_m2.script_drafts}
            rewrite_prompt = self._build_rewrite_prompt(
                m1_data=m1_data,
                m1_5_data=m1_5_data,
                failed_audits=failed_audits,
                existing_drafts_by_id=existing_by_id,
            )
            llm_resp, model_used = self.llm.generate_structured(
                prompt=rewrite_prompt,
                schema=LLMM2BatchGenerationResponse,
                system_instruction=system_instruction,
                prefer_lite=False,
                temperature=0.35,
            )

            rewritten_by_id: Dict[str, ScriptDraftItem] = {}
            for pos, fa in enumerate(failed_audits):
                target_sid = str(fa.get("script_id") or "")
                old_draft = existing_by_id.get(target_sid)
                orig_idx = (
                    next((i for i, d in enumerate(existing_m2.script_drafts, start=1) if d.script_id == target_sid), pos + 1)
                )
                raw_s = llm_resp.scripts[min(pos, len(llm_resp.scripts) - 1)] if llm_resp.scripts else None
                if raw_s is None:
                    continue
                if old_draft:
                    raw_s.script_format = old_draft.script_format
                    raw_s.domain = old_draft.domain
                    raw_s.category_id = old_draft.category_id
                    if old_draft.narrative_angle in UPGRADE_1_ANGLE_POOL:
                        raw_s.narrative_angle = old_draft.narrative_angle  # type: ignore[assignment]
                    raw_s.primary_sku_ids = list(old_draft.primary_sku_ids)
                new_draft = self._postprocess_script(
                    idx=orig_idx,
                    raw_script=raw_s,
                    m1_data=m1_data,
                    sku_by_id=sku_by_id,
                    valid_runner_ids=valid_runner_ids,
                    valid_foil_ids=valid_foil_ids,
                )
                new_draft.script_id = target_sid
                rewritten_by_id[target_sid] = new_draft
                print(
                    f"     -> [REWRITTEN {new_draft.script_id} ({model_used})] "
                    f"{new_draft.total_word_count} words | {new_draft.total_duration_sec}s",
                    flush=True,
                )

            merged_drafts = [
                rewritten_by_id.get(d.script_id, d) for d in existing_m2.script_drafts
            ]
            existing_m2.script_drafts = merged_drafts
            existing_m2.generated_at = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
            existing_m2.llm_models_used = self.llm.models_used_history
            atomic_write_text(output_path, existing_m2.model_dump_json(indent=2))
            return existing_m2

        prompt = self._build_prompt(m1_data, m1_5_data=m1_5_data)
        llm_resp, model_used = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMM2BatchGenerationResponse,
            system_instruction=system_instruction,
            prefer_lite=False,
            temperature=0.3,
        )

        print(
            f"[M2]     -> Classified as {llm_resp.multi_category_synergy_type} ({model_used}); "
            f"post-processing {len(llm_resp.scripts)} script draft(s)...",
            flush=True,
        )

        processed_scripts: List[ScriptDraftItem] = []
        used_angles: set[str] = set()

        for idx, raw_s in enumerate(llm_resp.scripts, start=1):
            draft = self._postprocess_script(
                idx=idx,
                raw_script=raw_s,
                m1_data=m1_data,
                sku_by_id=sku_by_id,
                valid_runner_ids=valid_runner_ids,
                valid_foil_ids=valid_foil_ids,
            )
            if draft.narrative_angle in used_angles or (
                draft.narrative_angle == "FAST_25S_MICRO_LOOP" and draft.total_word_count > 125
            ):
                pool = recommend_angles_for_category(
                    draft.category_id,
                    draft.domain,
                    is_combo=(draft.script_format != "SINGLE_SKU_DEEP_DIVE"),
                )
                for candidate_angle in pool + UPGRADE_1_ANGLE_POOL:
                    if candidate_angle == "FAST_25S_MICRO_LOOP" and draft.total_word_count > 125:
                        continue
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
    parser.add_argument(
        "--rewrite-from-m3",
        type=Path,
        default=None,
        help="Optional M3 output JSON path to rewrite only FAILED_NEEDS_REWRITE scripts.",
    )
    args = parser.parse_args()

    engine = M2ScriptGeneratorEngine()
    engine.run(
        input_path=args.input,
        output_path=args.output,
        m3_feedback_path=args.rewrite_from_m3,
    )


if __name__ == "__main__":
    main()
