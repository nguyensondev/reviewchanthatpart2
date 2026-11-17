"""
Module M3: Independent QA/QC Auditor & Auto-Patcher (`pipeline/m3_qaqc.py`).

Responsibilities (Black-Box Contract):
  1. Reads `data/m2_script_drafts.json` and cross-checks every claim against
     `data/m1_qualified_skus.json` ground truth.
  2. Executes a 3-Layer QA/QC Audit:
     - Layer 1 (Python, 0 Gemini quota): Pacing/beat count, distinct visual clips,
       UTF-8 Vietnamese diacritics, and on-screen text length (<= 8 words).
     - Layer 2 (LLM Batch Semantic Audit, 1 Gemini call): Verifies specs, prices,
       honest caveats, anti-hype tone, and anti-forced-combo logic against M1.
     - Layer 3 (Auto-Patch & TTS/Subtitle Split): Patches detected factual/caveat
       defects automatically and splits every beat into spoken `tts_voiceover_vi`
       vs. compact display `subtitle_text_vi` (e.g., 180ml, 89k, 900ml, 195k).
  3. Outputs `data/m3_approved_scripts.json` ready for Module M4 video production.
"""
from __future__ import annotations

import argparse
import datetime as dt
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.core_resilience import atomic_write_text
from pipeline.daily_quota import DailyQuotaManager, MAX_FINAL_SCRIPTS_PER_DAY
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.shared_utils import enforce_short_hook_title
from pipeline.schemas_m1 import M1QualifiedSKUsOutput
from pipeline.schemas_m1_5 import normalize_sku_key
from pipeline.schemas_m2 import M2ScriptDraftsOutput, ScriptDraftItem
from pipeline.video_registry import VideoRegistryManager
from pipeline.schemas_m3 import (
    AuditedBeatSegment,
    AuditedScriptItem,
    LLMAuditedBeatPatch,
    LLMM3BatchAuditResponse,
    LLMScriptAuditResult,
    M3ApprovedScriptsOutput,
    QAQCIssueItem,
    QAQCScoreBreakdown100,
    QAQCStatus,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_M2_PATH = DATA_DIR / "m2_script_drafts.json"
INPUT_M1_PATH = DATA_DIR / "m1_qualified_skus.json"
OUTPUT_M3_PATH = DATA_DIR / "m3_approved_scripts.json"

DEFAULT_SPEAKING_RATE_WPS = 3.1
MIN_BEAT_DURATION_SEC = 4.5
MAX_ONSCREEN_WORDS = 8

_VI_ACCENTED_RE = re.compile(
    r"[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ"
    r"ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ]"
)

_VI_DIGIT_MAP: Dict[str, int] = {
    "không": 0,
    "một": 1,
    "mốt": 1,
    "hai": 2,
    "ba": 3,
    "bốn": 4,
    "tư": 4,
    "năm": 5,
    "lăm": 5,
    "sáu": 6,
    "bảy": 7,
    "bẩy": 7,
    "tám": 8,
    "chín": 9,
}


def _parse_vi_number_phrase(phrase: str) -> Optional[int]:
    """Parse spoken Vietnamese integer phrases (< 1000) into an integer (e.g. 'một trăm ba lăm' -> 135)."""
    tokens = [t for t in phrase.lower().strip().split() if t not in ("linh", "lẻ")]
    if not tokens:
        return None
    if not all(t in _VI_DIGIT_MAP or t in ("trăm", "mươi", "mười") for t in tokens):
        return None

    total = 0
    current = 0
    has_unit = False
    after_tram_digits: List[int] = []
    seen_tram = False
    seen_muoi = False

    for tok in tokens:
        if tok in _VI_DIGIT_MAP:
            d = _VI_DIGIT_MAP[tok]
            if seen_tram and not seen_muoi and has_unit:
                # Shorthand like "một trăm ba lăm" -> 100 + 30 + 5 = 135
                total += current * 10 + d
                current = 0
                has_unit = False
                seen_muoi = True
            else:
                current = d
                has_unit = True
                after_tram_digits.append(d)
        elif tok == "trăm":
            total += (current if has_unit else 1) * 100
            current = 0
            has_unit = False
            seen_tram = True
        elif tok == "mười":
            total += 10
            current = 0
            has_unit = False
            seen_muoi = True
        elif tok == "mươi":
            total += (current if has_unit else 1) * 10
            current = 0
            has_unit = False
            seen_muoi = True
    if has_unit:
        total += current
    return total if total > 0 else None


def _sanitize_brand_and_tech_terms(text: str) -> str:
    """Restore canonical brand/model/tech spellings if an LLM transliterated them phonetically."""
    s = " ".join((text or "").strip().split())
    replacements = [
        (r"\bBa[-\s]*se[-\s]*us\b", "Baseus"),
        (r"\bLô[-\s]*gi[-\s]*tếch\b", "Logitech"),
        (r"\bVéc[-\s]*ti[-\s]*cờ[-\s]*l\b", "Vertical"),
        (r"\bTa[-\s]*ni[-\s]*ta\b", "Tanita"),
        (r"\bKê[-\s]*đê\s+Sê[-\s]*ri\b", "KD-Series"),
        (r"\bKD\s+Sê[-\s]*ri\b", "KD-Series"),
        (r"\bi[-\s]*wok2\s+Sê[-\s]*ri\b", "i-wok2 Series"),
        (r"\bSê[-\s]*ri\b", "Series"),
        (r"\bU[-\s]*[^\s]+\s+Taip[-\s]*Xê\b", "USB Type-C"),
        (r"\bU[-\s]*ét[-\s]*bi\s+Taip[-\s]*Xê\b", "USB Type-C"),
        (r"\bTaip[-\s]*Xê\b", "Type-C"),
        (r"\bhàng\s+ô[-\s]*em\b", "hàng OEM"),
        (r"\bcân\s+ô[-\s]*em\b", "cân OEM"),
        (r"\bchuột\s+ô[-\s]*em\b", "chuột OEM"),
        (r"\bô[-\s]*em\b", "OEM"),
        (r"\bVết[-\s]*hát\s+Hai[-\s]*mươi\b", "WH-20"),
        (r"\bmô[-\s]*đen\b", "model"),
    ]
    for pat, rep in replacements:
        s = re.sub(pat, rep, s, flags=re.IGNORECASE)
    return s


def _normalize_subtitle_units(text: str) -> str:
    """Ensure compact numeric tokens (ml, mm, k, tr, g, kg, DPI, °, Inox 304) are used cleanly in subtitles."""
    s = _sanitize_brand_and_tech_terms(text)
    # Standardize spelled-out unit names first
    s = re.sub(r"\bmi[-\s]*li[-\s]*lít\b", "ml", s, flags=re.IGNORECASE)
    s = re.sub(r"\bmi[-\s]*li[-\s]*mét\b", "mm", s, flags=re.IGNORECASE)
    s = re.sub(r"\bkhông\s+phẩy\s+một\s+gram\b", "0.1g", s, flags=re.IGNORECASE)
    s = re.sub(r"\bba\s+đến\s+sáu\s+ký\b", "3-6kg", s, flags=re.IGNORECASE)
    s = re.sub(r"\bbốn\s+nghìn\s+dpi\b", "4000 DPI", s, flags=re.IGNORECASE)
    s = re.sub(r"\bnăm\s+mươi\s+bảy\s+độ\b", "57°", s, flags=re.IGNORECASE)

    num_word_alt = (
        r"(?:không|một|mốt|hai|ba|bốn|tư|năm|lăm|sáu|bảy|bẩy|tám|chín|"
        r"trăm|mươi|mười|linh|lẻ)"
    )

    # Millions + thousands like "một triệu tám trăm năm mươi nghìn đồng" -> "1,85tr"
    mil_pat = rf"\b(một|hai|ba|bốn|năm|sáu|bảy|tám|chín)\s+triệu\s+({num_word_alt}(?:\s+{num_word_alt})*)\s*(?:nghìn(?:\s+đồng)?|cành|k)\b"

    def _repl_mil(match: re.Match[str]) -> str:
        m_val = _VI_DIGIT_MAP.get(match.group(1).lower(), 1)
        k_val = _parse_vi_number_phrase(match.group(2))
        if k_val is not None:
            dec = str(k_val // 10).rstrip("0")
            return f"{m_val},{dec}tr" if dec else f"{m_val}tr"
        return match.group(0)

    s = re.sub(mil_pat, _repl_mil, s, flags=re.IGNORECASE)

    # Dimensions like "một trăm hai lăm nhân một trăm bốn mươi hai mm" -> "125x142mm"
    dim_pat = rf"\b({num_word_alt}(?:\s+{num_word_alt})*)\s+nhân\s+({num_word_alt}(?:\s+{num_word_alt})*)\s*(mm)\b"

    def _repl_dim(match: re.Match[str]) -> str:
        n1 = _parse_vi_number_phrase(match.group(1))
        n2 = _parse_vi_number_phrase(match.group(2))
        if n1 is not None and n2 is not None:
            return f"{n1}x{n2}{match.group(3).lower()}"
        return match.group(0)

    s = re.sub(dim_pat, _repl_dim, s, flags=re.IGNORECASE)

    # Material codes like "Inox ba trăm linh tư" -> "Inox 304"
    inox_pat = rf"\b(inox)\s+({num_word_alt}(?:\s+{num_word_alt})+)\b"

    def _repl_inox(match: re.Match[str]) -> str:
        val = _parse_vi_number_phrase(match.group(2))
        return f"Inox {val}" if val is not None else match.group(0)

    s = re.sub(inox_pat, _repl_inox, s, flags=re.IGNORECASE)

    # Quantities + units/prices
    pattern_num_words = rf"\b({num_word_alt}(?:\s+{num_word_alt})*)\s+(ml|mm|nghìn(?:\s+đồng)?|cành|k)\b"

    def _repl(match: re.Match[str]) -> str:
        num_phrase, unit = match.group(1), match.group(2).lower()
        val = _parse_vi_number_phrase(num_phrase)
        if val is None:
            return match.group(0)
        if unit.startswith("nghìn") or unit in ("cành", "k"):
            return f"{val}k"
        return f"{val}{unit}"

    s = re.sub(pattern_num_words, _repl, s, flags=re.IGNORECASE)
    # Handle cases where LLM already mixed digits like "125 nhân 142mm" or "89 nghìn đồng" or "hai triệu 270k"
    s = re.sub(r"\b(một|hai|ba|bốn|năm)\s+triệu\s+(\d)0k\b", lambda m: f"{_VI_DIGIT_MAP[m.group(1).lower()]},{m.group(2)}tr", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(một|hai|ba|bốn|năm)\s+triệu\s+(\d\d)0k\b", lambda m: f"{_VI_DIGIT_MAP[m.group(1).lower()]},{m.group(2)}tr", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s+(?:nhân|x)\s+(\d+)\s*mm\b", r"\1x\2mm", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*(?:nghìn(?:\s+đồng)?|cành)\b", r"\1k", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s+(ml|mm)\b", r"\1\2", s, flags=re.IGNORECASE)
    return s


def _trim_onscreen_text(text: str, max_words: int = MAX_ONSCREEN_WORDS) -> str:
    words = text.strip().split()
    return " ".join(words[:max_words]) if len(words) > max_words else " ".join(words)


class M3QAQCEngine:
    """Executes the 3-Layer QA/QC Audit & Auto-Patching workflow (M3.1 -> M3.3)."""

    def __init__(self, llm_client: Optional[FreeTierGeminiClient] = None) -> None:
        self.llm = llm_client or FreeTierGeminiClient()

    def _run_layer1_technical_checks(
        self, draft: ScriptDraftItem
    ) -> Tuple[List[QAQCIssueItem], bool]:
        """
        Deterministic Python Layer-1 technical & pacing audit.
        Returns (issues, has_critical_failure).
        """
        issues: List[QAQCIssueItem] = []
        has_critical = False

        if len(draft.beats) != 5:
            issues.append(
                QAQCIssueItem(
                    issue_category="PACING_OR_LENGTH",
                    severity="WARNING_AUTO_PATCHED",
                    beat_index=None,
                    description_vi=f"Kịch bản có {len(draft.beats)} phân cảnh thay vì 5 phân cảnh chuẩn.",
                    resolution_applied_vi="Chuẩn hóa nhịp thời lượng theo số phân cảnh hiện có.",
                )
            )

        if not (25.0 <= draft.total_duration_sec <= 55.0):
            issues.append(
                QAQCIssueItem(
                    issue_category="PACING_OR_LENGTH",
                    severity="WARNING_AUTO_PATCHED",
                    beat_index=None,
                    description_vi=(
                        f"Tổng thời lượng {draft.total_duration_sec}s nằm ngoài ngưỡng tối ưu 25s–55s."
                    ),
                    resolution_applied_vi="Yêu cầu tầng Auto-Patch tinh chỉnh độ dài lời thoại vừa nhịp 9:16.",
                )
            )

        clip_ids = [
            b.source_youtube_clip.video_id
            for b in draft.beats
            if b.source_youtube_clip is not None
        ]
        if len(clip_ids) == 0:
            has_critical = True
            issues.append(
                QAQCIssueItem(
                    issue_category="VISUAL_CLIP_MISSING",
                    severity="CRITICAL_REJECT",
                    beat_index=None,
                    description_vi="Không có video YouTube nguồn nào gắn trong kịch bản để M4 dựng hình.",
                    resolution_applied_vi="Đánh trượt REJECTED vì thiếu hoàn toàn tư liệu video.",
                )
            )
        elif len(set(clip_ids)) < 2:
            issues.append(
                QAQCIssueItem(
                    issue_category="VISUAL_CLIP_MISSING",
                    severity="INFO",
                    beat_index=None,
                    description_vi="Kịch bản hiện chỉ sử dụng 1 video nguồn duy nhất, độ đa dạng góc quay hạn chế.",
                    resolution_applied_vi="Ghi nhận lưu ý cho M4 khi cắt cảnh.",
                )
            )

        for beat in draft.beats:
            if len(beat.voiceover_vi.strip()) >= 15 and not _VI_ACCENTED_RE.search(beat.voiceover_vi):
                issues.append(
                    QAQCIssueItem(
                        issue_category="DIACRITIC_OR_SPELLING",
                        severity="WARNING_AUTO_PATCHED",
                        beat_index=beat.beat_index,
                        description_vi="Lời thoại bị mất dấu tiếng Việt, sẽ gây lỗi phát âm AI TTS.",
                        resolution_applied_vi="Khôi phục đầy đủ dấu tiếng Việt chuẩn UTF-8 ở tầng Auto-Patch.",
                    )
                )

            if beat.source_youtube_clip is None and not has_critical:
                issues.append(
                    QAQCIssueItem(
                        issue_category="VISUAL_CLIP_MISSING",
                        severity="WARNING_AUTO_PATCHED",
                        beat_index=beat.beat_index,
                        description_vi=f"Phân cảnh #{beat.beat_index} chưa gắn clip YouTube tham chiếu.",
                        resolution_applied_vi="M4 sẽ mượn clip từ phân cảnh lân cận của cùng SKU.",
                    )
                )

        return issues, has_critical

    def _build_ground_truth_context(self, m1_data: M1QualifiedSKUsOutput) -> str:
        blocks: List[str] = []
        for cat in m1_data.qualified_categories:
            w = cat.primary_winner
            aff_price = (
                f" (giá CSV Shopee: {w.best_affiliate_offer.price_vnd:,}đ)"
                if w.best_affiliate_offer
                else ""
            )
            ru = cat.runner_up
            ru_line = (
                f"  - Á quân (`{ru.candidate_id}`): {ru.brand} {ru.model_code or ''} — "
                f"Giá ~{ru.estimated_market_price_vnd:,}đ | Nhược điểm: {ru.honest_caveat_vi}"
                if ru
                else "  - Á quân: (Không có)"
            )
            foil_line = "; ".join(
                f"`{f.candidate_id}` ({f.brand}, ~{f.estimated_market_price_vnd:,}đ, lỗi: {f.rejection_reason_vi})"
                for f in cat.rejected_foils
            ) or "(Không có)"

            specs_str = ", ".join(f"{k}: {v}" for k, v in w.key_specs.items())
            blocks.append(
                f"""
- Nhóm công năng `{cat.category_id}` ({cat.category_name_vi}):
  + QUÁN QUÂN (`{w.candidate_id}`): Thương hiệu `{w.brand}` | Model `{w.model_code or 'N/A'}` | Tên `{w.product_name_vi}`
  + Giá thị trường chuẩn: {w.estimated_market_price_vnd:,}đ{aff_price}
  + Thông số thật duy nhất được M1 xác nhận: {specs_str}
  + Ưu điểm thật: {"; ".join(w.highlight_pros_vi)}
  + Nhược điểm thật BẮT BUỘC PHẢI NÓI (`{w.caveat_severity}`): {w.honest_caveat_vi}
{ru_line}
  + Hàng đối chứng bị loại (`rejected_foils`): {foil_line}
  + Lập luận thắng Á quân: {cat.head_to_head_verdict.why_winner_beats_runner_up_vi}
""".strip()
            )
        return "\n".join(blocks)

    def _run_llm_semantic_audit_and_patch(
        self,
        m2_data: M2ScriptDraftsOutput,
        m1_data: M1QualifiedSKUsOutput,
    ) -> LLMM3BatchAuditResponse:
        ground_truth_text = self._build_ground_truth_context(m1_data)

        draft_blocks: List[str] = []
        for s in m2_data.script_drafts:
            beats_lines = "\n".join(
                f"    Beat {b.beat_index} [{b.beat_type}] target={b.target_candidate_id}: "
                f"VO=\"{b.voiceover_vi}\" | Screen=\"{b.on_screen_text_vi}\""
                for b in s.beats
            )
            draft_blocks.append(
                f"""
  === KỊCH BẢN `{s.script_id}` (Format: {s.script_format} | Domain: {s.domain} | Category: {s.category_id} | Angle: {s.narrative_angle}) ===
  Tiêu đề: {s.title_vi} | Primary SKUs: {s.primary_sku_ids}
{beats_lines}
""".strip()
            )

        system_instruction = (
            "Bạn là Kiểm toán viên QA/QC Độc Lập (Module M3) kiểm định chéo kịch bản của M2 với hồ sơ sự thật gốc M1.\n"
            "NHIỆM VỤ BẮT BUỘC:\n"
            "1. ĐỐI CHIẾU SỰ THẬT (`FACT_HALLUCINATION`): Mọi thông số kỹ thuật (dung tích, chất liệu, công suất, cơ chế kéo tay hay điện, kích thước, giá bán, thời gian bảo hành) "
            "phải KHỚP 100% với HỒ SƠ M1. Nếu M2 tự bịa (ví dụ biến cối kéo tay nhựa PS 180ml 89k thành máy điện thủy tinh 500ml 300W 250k bảo hành 5 năm), "
            "PHẢI bắt lỗi `FACT_HALLUCINATION` (`WARNING_AUTO_PATCHED`), trừ điểm `fact_accuracy_score` của bản gốc và VIẾT LẠI cho đúng 100% sự thật M1 trong `beats_patched` (`was_auto_patched=true`).\n"
            "2. KIỂM TRA NHƯỢC ĐIỂM THẬT (`MISSING_HONEST_CAVEAT`): Phân cảnh `HONEST_CAVEAT` (Beat 4) PHẢI nói rõ đúng nhược điểm thực tế từ M1 (VD: 180ml chỉ xay gia vị ít không xay được thịt; 900ml cồng kềnh tốn diện tích & nặng tay khi đầy). "
            "Nếu M2 khen hoàn hảo tuyệt đối hoặc che giấu nhược điểm/nói sai khả năng, PHẢI bắt lỗi `MISSING_HONEST_CAVEAT` (`WARNING_AUTO_PATCHED`), trừ điểm `honest_caveat_score` và VIẾT LẠI beat 4 cho trung thực (`was_auto_patched=true`).\n"
            "3. CHỐNG GƯỢNG ÉP COMBO (`FORCED_COMBO_LOGIC`): Nếu hai SKU cùng công năng khác cỡ mà ép mua cả hai thay vì so sánh chọn 1, bắt lỗi `FORCED_COMBO_LOGIC`.\n"
            "4. GIỮ NGUYÊN GÓC KỂ CHUYỆN (`narrative_angle`) VÀ PHONG CÁCH MỞ BÀI RIÊNG: Khi sửa lỗi hoặc chuẩn hóa phụ đề (`APPROVED_WITH_AUTO_PATCH`), PHẢI tôn trọng và giữ nguyên cá tính mở bài cũng như cấu trúc dẫn chuyện đặc trưng theo `Angle` của từng kịch bản — tuyệt đối KHÔNG làm phẳng các kịch bản thành cùng một văn mẫu giống nhau.\n"
            "5. TÁCH ĐÔI TTS VOICEOVER & PHỤ ĐỀ HIỂN THỊ (BẮT BUỘC CHO MỌI BEAT):\n"
            "   - `tts_voiceover_vi`: Văn nói tiếng Việt CÓ DẤU ĐẦY ĐỦ tự nhiên cho máy đọc (VD: 'một trăm tám mươi mi-li-lít', 'tám mươi chín nghìn đồng').\n"
            "   - `subtitle_text_vi`: BẮT BUỘC LÀ CÂU PHỤ ĐỀ ĐẦY ĐỦ NỘI DUNG KHỚP VỚI `tts_voiceover_vi` (TUYỆT ĐỐI KHÔNG chép `on_screen_text_vi` vào đây!), chỉ đổi số/đơn vị sang ký hiệu gọn (`180ml`, `900ml`, `89k`, `195k`, `30k`, `39k`, `125x142mm`, `Inox 304`).\n"
            "   - `on_screen_text_vi`: Chữ tiêu đề ngắn gọn tối đa 8 từ, tiếng Việt có dấu.\n"
            "6. CHUẨN HÓA TIÊU ĐỀ (`patched_title_vi`): Luôn điền `patched_title_vi` là tiêu đề kịch bản bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ chuẩn UTF-8 (khôi phục dấu nếu `title_vi` gốc thiếu dấu).\n"
            "7. Lưu ý `was_auto_patched`: Chỉ đặt `true` cho những beat có lỗi nội dung/sự thật/nhược điểm/mất dấu cần viết lại; nếu beat gốc đã đúng sự thật và chỉ tách định dạng `subtitle_text_vi`, đặt `was_auto_patched=false`."
        )

        prompt = f"""
=== HỒ SƠ SỰ THẬT GỐC TỪ MODULE M1 (CHUẨN ĐỐI CHIẾU) ===
{ground_truth_text}

=== DANH SÁCH KỊCH BẢN TỪ MODULE M2 CẦN THẨM ĐỊNH ===
{chr(10).join(draft_blocks)}

Hãy xuất JSON theo đúng schema `LLMM3BatchAuditResponse` trả về đủ kết quả thẩm định (`script_audits`) cho từng `script_id`, kèm đầy đủ 5 phân cảnh trong `beats_patched` cho mỗi kịch bản.
""".strip()

        resp, _ = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMM3BatchAuditResponse,
            system_instruction=system_instruction,
            prefer_lite=True,
            temperature=0.1,
        )
        return resp

    def audit_drafts(
        self,
        m2_data: M2ScriptDraftsOutput,
        m1_data: M1QualifiedSKUsOutput,
    ) -> M3ApprovedScriptsOutput:
        """Run all 3 QA/QC layers on supplied M2 + M1 Pydantic objects (supports both file runs & in-memory fault injection)."""
        if not m2_data.script_drafts:
            raise RuntimeError("M2 input has 0 script drafts to audit.")

        print(
            f"[M3] 1/3 Running Layer-1 Technical & Pacing Audit across {len(m2_data.script_drafts)} script(s)...",
            flush=True,
        )
        layer1_map: Dict[str, Tuple[List[QAQCIssueItem], bool]] = {}
        for s in m2_data.script_drafts:
            l1_issues, l1_crit = self._run_layer1_technical_checks(s)
            layer1_map[s.script_id] = (l1_issues, l1_crit)

        print(
            "[M3] 2/3 Running Layer-2 & Layer-3 LLM Cross-Check against M1 + TTS/Subtitle Split (1 call)...",
            flush=True,
        )
        llm_audit_resp = self._run_llm_semantic_audit_and_patch(m2_data, m1_data)
        audit_by_id: Dict[str, LLMScriptAuditResult] = {
            a.script_id: a for a in llm_audit_resp.script_audits
        }

        audited_scripts: List[AuditedScriptItem] = []
        approved_cnt = 0
        patched_cnt = 0
        rejected_cnt = 0

        for draft in m2_data.script_drafts:
            l1_issues, l1_crit = layer1_map[draft.script_id]
            llm_res = audit_by_id.get(draft.script_id)

            combined_issues: List[QAQCIssueItem] = list(l1_issues)
            patch_by_idx: Dict[int, LLMAuditedBeatPatch] = {}

            if llm_res:
                combined_issues.extend(llm_res.issues_found)
                for pb in llm_res.beats_patched:
                    patch_by_idx[pb.beat_index] = pb

                f_score = round(min(30.0, max(0.0, llm_res.fact_accuracy_score)), 2)
                h_score = round(min(25.0, max(0.0, llm_res.honest_caveat_score)), 2)
                n_score = round(min(25.0, max(0.0, llm_res.natural_tone_and_logic_score)), 2)
                t_score = round(min(20.0, max(0.0, llm_res.tts_and_visual_readiness_score)), 2)
                verdict_vi = llm_res.auditor_verdict_vi.strip()
            else:
                f_score, h_score, n_score, t_score = 26.0, 22.0, 22.0, 18.0
                verdict_vi = "Đạt kiểm tra kỹ thuật cơ bản (fallback khi LLM không trả về mục riêng)."

            if l1_crit:
                t_score = 0.0

            total_qaqc = round(f_score + h_score + n_score + t_score, 2)
            score_breakdown = QAQCScoreBreakdown100(
                fact_accuracy_score=f_score,
                honest_caveat_score=h_score,
                natural_tone_and_logic_score=n_score,
                tts_and_visual_readiness_score=t_score,
                total_qaqc_score_100=total_qaqc,
            )

            audited_beats: List[AuditedBeatSegment] = []
            cursor_sec = 0.0
            total_words = 0
            any_beat_patched = False

            sorted_beats = sorted(draft.beats, key=lambda b: b.beat_index)
            for b in sorted_beats:
                patch = patch_by_idx.get(b.beat_index)
                if patch:
                    was_patched = bool(patch.was_auto_patched)
                    if was_patched:
                        tts_vo = _sanitize_brand_and_tech_terms(
                            " ".join(patch.tts_voiceover_vi.strip().split()) or b.voiceover_vi
                        )
                        candidate_sub = _sanitize_brand_and_tech_terms(patch.subtitle_text_vi.strip())
                        if not candidate_sub or len(candidate_sub.split()) < 0.6 * max(1, len(tts_vo.split())):
                            candidate_sub = tts_vo
                        sub_txt = _normalize_subtitle_units(candidate_sub)
                    else:
                        tts_vo = _sanitize_brand_and_tech_terms(b.voiceover_vi)
                        sub_txt = _normalize_subtitle_units(b.voiceover_vi)
                    onscreen = _trim_onscreen_text(
                        _sanitize_brand_and_tech_terms(patch.on_screen_text_vi or b.on_screen_text_vi),
                        MAX_ONSCREEN_WORDS,
                    )
                else:
                    tts_vo = _sanitize_brand_and_tech_terms(b.voiceover_vi)
                    sub_txt = _normalize_subtitle_units(b.voiceover_vi)
                    onscreen = _trim_onscreen_text(_sanitize_brand_and_tech_terms(b.on_screen_text_vi), MAX_ONSCREEN_WORDS)
                    was_patched = False

                if was_patched:
                    any_beat_patched = True

                wc = len(tts_vo.split()) if tts_vo else 0
                dur = round(max(MIN_BEAT_DURATION_SEC, wc / DEFAULT_SPEAKING_RATE_WPS), 1)
                start_s = round(cursor_sec, 1)
                end_s = round(start_s + dur, 1)
                cursor_sec = end_s
                total_words += wc
                wps = round(wc / dur, 2) if dur > 0 else 0.0

                audited_beats.append(
                    AuditedBeatSegment(
                        beat_index=b.beat_index,
                        beat_type=b.beat_type,
                        start_sec=start_s,
                        end_sec=end_s,
                        duration_sec=dur,
                        target_candidate_id=b.target_candidate_id,
                        tts_voiceover_vi=tts_vo,
                        subtitle_text_vi=sub_txt,
                        word_count=wc,
                        estimated_speaking_rate_wps=wps,
                        on_screen_text_vi=onscreen,
                        visual_instruction_vi=b.visual_instruction_vi,
                        source_youtube_clip=b.source_youtube_clip,
                        was_auto_patched=was_patched,
                    )
                )

            has_critical_issue = l1_crit or any(
                i.severity == "CRITICAL_REJECT" for i in combined_issues
            )
            has_warning_patched = any(
                i.severity == "WARNING_AUTO_PATCHED" for i in combined_issues
            )

            if has_critical_issue:
                status: QAQCStatus = "REJECTED"
                rejected_cnt += 1
            elif any_beat_patched or has_warning_patched:
                status = "APPROVED_WITH_AUTO_PATCH"
                patched_cnt += 1
            else:
                status = "APPROVED"
                approved_cnt += 1

            total_dur = round(cursor_sec, 1)
            avg_wps = round(total_words / total_dur, 2) if total_dur > 0 else 0.0

            print(
                f"     -> [{status}] {draft.script_id} | QAQC={total_qaqc}/100 | "
                f"Issues={len(combined_issues)} | Duration={total_dur}s ({total_words}w)",
                flush=True,
            )

            patched_title = (
                llm_res.patched_title_vi.strip()
                if llm_res and llm_res.patched_title_vi and llm_res.patched_title_vi.strip()
                else draft.title_vi
            )
            raw_short_hook = (
                llm_res.patched_hook_title_short_vi.strip()
                if llm_res and getattr(llm_res, "patched_hook_title_short_vi", "") and llm_res.patched_hook_title_short_vi.strip()
                else (draft.hook_title_short_vi or (audited_beats[0].on_screen_text_vi if audited_beats else "") or patched_title)
            )
            short_hook = enforce_short_hook_title(raw_short_hook, max_chars=30)

            audited_scripts.append(
                AuditedScriptItem(
                    script_id=draft.script_id,
                    script_format=draft.script_format,
                    domain=draft.domain,
                    category_id=draft.category_id,
                    narrative_angle=draft.narrative_angle,
                    qaqc_status=status,
                    qaqc_score_breakdown=score_breakdown,
                    auditor_verdict_vi=verdict_vi,
                    issues_found=combined_issues,
                    title_vi=patched_title,
                    hook_title_short_vi=short_hook,
                    primary_sku_ids=draft.primary_sku_ids,
                    runner_up_sku_ids_referenced=draft.runner_up_sku_ids_referenced,
                    foil_sku_ids_referenced=draft.foil_sku_ids_referenced,
                    total_duration_sec=total_dur,
                    total_word_count=total_words,
                    average_speaking_rate_wps=avg_wps,
                    beats=audited_beats,
                    verified_factual_claims=draft.factual_claims_for_m3_audit,
                    affiliate_offers_ready=draft.affiliate_offers_attached,
                )
            )

        now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        m3_run_id = f"m3_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{m2_data.sub_niche_id}"

        return M3ApprovedScriptsOutput(
            m3_run_id=m3_run_id,
            source_m2_run_id=m2_data.m2_run_id,
            source_m1_run_id=m1_data.m1_run_id,
            generated_at=now_iso,
            domain=m2_data.domain,
            domain_name_vi=m2_data.domain_name_vi,
            sub_niche_id=m2_data.sub_niche_id,
            sub_niche_name_vi=m2_data.sub_niche_name_vi,
            multi_category_synergy_type=m2_data.multi_category_synergy_type,
            llm_models_used=self.llm.models_used_history,
            approved_count=approved_cnt,
            auto_patched_count=patched_cnt,
            rejected_count=rejected_cnt,
            audited_scripts=audited_scripts,
        )

    def run(
        self,
        input_m2_path: Path = INPUT_M2_PATH,
        input_m1_path: Path = INPUT_M1_PATH,
        output_path: Path = OUTPUT_M3_PATH,
        quota_mgr: Optional[DailyQuotaManager] = None,
    ) -> M3ApprovedScriptsOutput:
        if not input_m2_path.exists():
            raise FileNotFoundError(f"M3 input M2 drafts not found: {input_m2_path}")
        if not input_m1_path.exists():
            raise FileNotFoundError(f"M3 input M1 ground truth not found: {input_m1_path}")

        m2_data = M2ScriptDraftsOutput.model_validate_json(
            input_m2_path.read_text(encoding="utf-8")
        )
        m1_data = M1QualifiedSKUsOutput.model_validate_json(
            input_m1_path.read_text(encoding="utf-8")
        )

        qm = quota_mgr or DailyQuotaManager()
        used_today, _, existing_ids = qm.get_script_quota_status()
        allowed_drafts: List[ScriptDraftItem] = []
        skipped_ids: List[str] = []
        pending_new = 0

        for s in m2_data.script_drafts:
            if s.script_id in existing_ids and qm.can_approve_script(s.script_id):
                allowed_drafts.append(s)
            elif (used_today + pending_new) < MAX_FINAL_SCRIPTS_PER_DAY and qm.can_approve_script(s.script_id):
                allowed_drafts.append(s)
                pending_new += 1
            else:
                skipped_ids.append(s.script_id)

        if skipped_ids:
            print(
                f"[M3 QUOTA] Daily limit {MAX_FINAL_SCRIPTS_PER_DAY} final scripts/day reached; "
                f"skipping {skipped_ids}",
                flush=True,
            )

        if not allowed_drafts:
            now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
            used_after, rem_after, _ = qm.get_script_quota_status()
            sleep_info = qm.get_sleep_until_tomorrow_info()
            print(
                f"[M3 QUOTA] Đã đủ {used_after}/{MAX_FINAL_SCRIPTS_PER_DAY} kịch bản final hôm nay! "
                f"Tạm nghỉ chờ reset lúc {sleep_info['next_reset_at']}.",
                flush=True,
            )
            result = M3ApprovedScriptsOutput(
                m3_run_id=f"m3_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{m2_data.sub_niche_id}",
                source_m2_run_id=m2_data.m2_run_id,
                source_m1_run_id=m1_data.m1_run_id,
                generated_at=now_iso,
                domain=m2_data.domain,
                domain_name_vi=m2_data.domain_name_vi,
                sub_niche_id=m2_data.sub_niche_id,
                sub_niche_name_vi=m2_data.sub_niche_name_vi,
                multi_category_synergy_type=m2_data.multi_category_synergy_type,
                llm_models_used=[],
                approved_count=0,
                auto_patched_count=0,
                rejected_count=0,
                daily_script_quota_limit=MAX_FINAL_SCRIPTS_PER_DAY,
                daily_scripts_used_today=used_after,
                remaining_script_quota_today=rem_after,
                quota_status="QUOTA_REACHED_SLEEP_UNTIL_TOMORROW",
                skipped_due_to_quota_script_ids=skipped_ids,
                audited_scripts=[],
            )
        else:
            m2_filtered = m2_data.model_copy(update={"script_drafts": allowed_drafts})
            result = self.audit_drafts(m2_data=m2_filtered, m1_data=m1_data)
            norm_keys_by_script: Dict[str, List[str]] = {}
            for s in result.audited_scripts:
                if s.qaqc_status in ("APPROVED", "APPROVED_WITH_AUTO_PATCH"):
                    qm.record_approved_script(s.script_id)
                    norm_keys_by_script[s.script_id] = [
                        normalize_sku_key(c.brand, c.model_code, c.candidate_id)
                        for c in s.verified_factual_claims
                    ]
            try:
                VideoRegistryManager().register_approved_scripts(
                    scripts=result.audited_scripts,
                    m3_run_id=result.m3_run_id,
                    normalized_keys_by_script=norm_keys_by_script,
                )
            except Exception as exc:
                print(f"[M3] Warning: video_registry sync error: {exc}", flush=True)

            used_after, rem_after, _ = qm.get_script_quota_status()
            result.daily_script_quota_limit = MAX_FINAL_SCRIPTS_PER_DAY
            result.daily_scripts_used_today = used_after
            result.remaining_script_quota_today = rem_after
            result.skipped_due_to_quota_script_ids = skipped_ids
            result.quota_status = (
                "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW" if rem_after == 0 else "WITHIN_DAILY_QUOTA"
            )
            print(
                f"[M3 QUOTA] Daily script quota: {used_after}/{MAX_FINAL_SCRIPTS_PER_DAY} used "
                f"(remaining={rem_after}, status={result.quota_status})",
                flush=True,
            )

        print(f"[M3] 3/3 Writing approved scripts contract to: {output_path}...", flush=True)
        atomic_write_text(output_path, result.model_dump_json(indent=2))
        print("[M3] Completed!", flush=True)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Module M3: Independent QA/QC Auditor & Auto-Patcher.")
    parser.add_argument(
        "--input-m2",
        type=Path,
        default=INPUT_M2_PATH,
        help="Input JSON path from M2 (default: data/m2_script_drafts.json)",
    )
    parser.add_argument(
        "--input-m1",
        type=Path,
        default=INPUT_M1_PATH,
        help="Ground-truth JSON path from M1 (default: data/m1_qualified_skus.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_M3_PATH,
        help="Output JSON path for M4 (default: data/m3_approved_scripts.json)",
    )
    args = parser.parse_args()

    engine = M3QAQCEngine()
    engine.run(
        input_m2_path=args.input_m2,
        input_m1_path=args.input_m1,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
