"""
Data Contract & Pydantic Schemas for Module M3 (Independent QA/QC Auditor & Auto-Patcher).

M3 acts as an independent verification gate comparing M2's `data/m2_script_drafts.json` against
M1's ground-truth `data/m1_qualified_skus.json`. It outputs `data/m3_approved_scripts.json`
with both spoken TTS voiceover (`tts_voiceover_vi`) and compact display subtitles
(`subtitle_text_vi`) ready for Module M4 video production.
"""
from __future__ import annotations

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field

from pipeline.schemas_m0 import (
    AffiliateListingMatch,
    DomainId,
    YouTubeEvidenceClip,
)
from pipeline.schemas_m2 import (
    BeatType,
    FactualClaimReference,
    MultiCategorySynergyType,
    ScriptFormat,
)


QAQCStatus = Literal[
    "APPROVED",
    "APPROVED_WITH_AUTO_PATCH",
    "FAILED_NEEDS_REWRITE",
    "REJECTED",
]

IssueSeverity = Literal[
    "INFO",
    "WARNING_AUTO_PATCHED",
    "NEEDS_M2_REWRITE",
    "CRITICAL_REJECT",
]

IssueCategory = Literal[
    "FACT_HALLUCINATION",
    "MISSING_HONEST_CAVEAT",
    "FORCED_COMBO_LOGIC",
    "WEAK_HOOK_OR_TRANSITION",
    "SHALLOW_SKU_HIGHLIGHTS",
    "PACING_OR_LENGTH",
    "TTS_OR_SUBTITLE_FORMAT",
    "VISUAL_CLIP_MISSING",
    "DIACRITIC_OR_SPELLING",
]


class QAQCIssueItem(BaseModel):
    issue_category: IssueCategory
    severity: IssueSeverity
    beat_index: Optional[int] = Field(
        default=None, description="Thứ tự phân cảnh gặp vấn đề (hoặc null nếu là lỗi toàn kịch bản)"
    )
    description_vi: str = Field(..., description="Mô tả cụ thể lỗi phát hiện được")
    resolution_applied_vi: str = Field(
        ..., description="Biện pháp xử lý / vá lỗi tự động đã áp dụng (hoặc lý do từ chối)"
    )


class QAQCScoreBreakdown100(BaseModel):
    fact_accuracy_score: float = Field(
        ..., description="Độ chính xác thông số, giá cả và mã model đối chiếu M1 (Tối đa 30.0)"
    )
    honest_caveat_score: float = Field(
        ..., description="Mức độ trung thực khi nêu nhược điểm thực tế của sản phẩm (Tối đa 25.0)"
    )
    natural_tone_and_logic_score: float = Field(
        ..., description="Văn phong tự nhiên, không tâng bốc lố, không gượng ép combo (Tối đa 25.0)"
    )
    tts_and_visual_readiness_score: float = Field(
        ..., description="Độ sẵn sàng cho AI TTS đọc, phụ đề 9:16 và tư liệu video nguồn (Tối đa 20.0)"
    )
    total_qaqc_score_100: float = Field(
        ..., description="Tổng điểm QA/QC sau kiểm định (Tối đa 100.0)"
    )


class AuditedBeatSegment(BaseModel):
    beat_index: int
    beat_type: BeatType
    start_sec: float
    end_sec: float
    duration_sec: float
    target_candidate_id: str
    tts_voiceover_vi: str = Field(
        ..., description="Lời thoại tiếng Việt tối ưu cho AI TTS đọc mượt mà tự nhiên"
    )
    subtitle_text_vi: str = Field(
        ...,
        description="Văn bản phụ đề hiển thị lên video, chuẩn hóa con số/đơn vị gọn (VD: 180ml, 89k, 900ml, 195k)",
    )
    word_count: int
    estimated_speaking_rate_wps: float
    on_screen_text_vi: str = Field(..., description="Chữ nhấn mạnh overlay màn hình dọc (tối đa 8 từ)")
    visual_instruction_vi: str
    source_youtube_clip: Optional[YouTubeEvidenceClip] = None
    was_auto_patched: bool = False


class AuditedScriptItem(BaseModel):
    script_id: str
    script_format: ScriptFormat
    domain: DomainId = "CLEANING_CARE"
    category_id: str = "general"
    narrative_angle: str = "PAIN_POINT_REALITY_CHECK"
    qaqc_status: QAQCStatus
    qaqc_score_breakdown: QAQCScoreBreakdown100
    auditor_verdict_vi: str = Field(..., description="Kết luận thẩm định tổng quan của M3")
    rewrite_instructions_for_m2_vi: str = Field(
        default="",
        description="Góp ý chi tiết từ M3 chỉ rõ lý do FAIL (thiếu highlight nào, câu nào rời rạc/sơ sài) để M2 viết lại",
    )
    issues_found: List[QAQCIssueItem] = Field(default_factory=list)
    title_vi: str
    hook_title_short_vi: str = Field(
        default="",
        description="Tiêu đề mở bài cực ngắn (0-30 ký tự) hiển thị ở 2s đầu video",
    )
    primary_sku_ids: List[str] = Field(default_factory=list)
    runner_up_sku_ids_referenced: List[str] = Field(default_factory=list)
    foil_sku_ids_referenced: List[str] = Field(default_factory=list)
    total_duration_sec: float
    total_word_count: int
    average_speaking_rate_wps: float
    beats: List[AuditedBeatSegment] = Field(default_factory=list)
    verified_factual_claims: List[FactualClaimReference] = Field(default_factory=list)
    affiliate_offers_ready: Dict[str, Optional[AffiliateListingMatch]] = Field(default_factory=dict)


class M3ApprovedScriptsOutput(BaseModel):
    schema_version: str = Field(default="1.0.0", description="Phiên bản schema hợp đồng dữ liệu M3 -> M4")
    m3_run_id: str
    source_m2_run_id: str
    source_m1_run_id: str
    generated_at: str
    domain: DomainId
    domain_name_vi: str
    sub_niche_id: str
    sub_niche_name_vi: str
    multi_category_synergy_type: MultiCategorySynergyType
    llm_models_used: List[str] = Field(default_factory=list)
    approved_count: int
    auto_patched_count: int
    rejected_count: int
    daily_script_quota_limit: int = Field(default=4)
    daily_scripts_used_today: int = Field(default=0)
    remaining_script_quota_today: int = Field(default=4)
    quota_status: str = Field(default="WITHIN_DAILY_QUOTA")
    skipped_due_to_quota_script_ids: List[str] = Field(default_factory=list)
    audited_scripts: List[AuditedScriptItem] = Field(default_factory=list)


# --- Internal LLM Batch Audit Schema (Steps M3.2 & M3.3) ---


class LLMAuditedBeatPatch(BaseModel):
    beat_index: int
    was_auto_patched: bool = Field(
        ...,
        description="True nếu beat này bị lỗi thực tế/nhược điểm/diễn đạt và đã được biên tập sửa lại; False nếu nội dung gốc đã chính xác",
    )
    tts_voiceover_vi: str = Field(
        ...,
        description="Lời thoại tiếng Việt tối ưu cho AI TTS đọc tự nhiên (có thể đọc chữ các đơn vị như mi-li-lít, nghìn đồng)",
    )
    subtitle_text_vi: str = Field(
        ...,
        description="Bản phụ đề đầy đủ cả câu khớp với tts_voiceover_vi (KHÔNG phải tiêu đề ngắn), chỉ đổi số/đơn vị chữ sang ký hiệu gọn như 180ml, 900ml, 89k, 195k, 30k, 39k, 125x142mm, Inox 304",
    )
    on_screen_text_vi: str = Field(
        ..., description="Chữ tiêu đề ngắn trên màn hình, TỐI ĐA 8 TỪ, có dấu tiếng Việt chuẩn"
    )


class LLMScriptAuditResult(BaseModel):
    script_id: str
    fact_accuracy_score: float = Field(..., description="Điểm chính xác sự thật (0..30)")
    honest_caveat_score: float = Field(..., description="Điểm trung thực nhược điểm (0..25)")
    natural_tone_and_logic_score: float = Field(..., description="Điểm văn phong tự nhiên & logic (0..25)")
    tts_and_visual_readiness_score: float = Field(..., description="Điểm sẵn sàng TTS/hình ảnh (0..20)")
    patched_title_vi: str = Field(
        default="",
        description="Tiêu đề kịch bản tiếng Việt CÓ DẤU ĐẦY ĐỦ chuẩn UTF-8",
    )
    patched_hook_title_short_vi: str = Field(
        default="",
        description="Tiêu đề mở bài cực ngắn (0-30 ký tự) bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ",
    )
    auditor_verdict_vi: str
    needs_m2_rewrite: bool = Field(
        default=False,
        description="True nếu kịch bản sơ sài, thiếu lời dẫn cuốn hút, bỏ sót highlight quan trọng của SKU hoặc dưới 82 điểm -> bắt buộc M2 viết lại",
    )
    rewrite_instructions_for_m2_vi: str = Field(
        default="",
        description="Chỉ thị góp ý cụ thể cho M2: nêu rõ beat nào cần viết lại, highlight nào của SKU bị thiếu, cách sửa lời dẫn",
    )
    issues_found: List[QAQCIssueItem] = Field(default_factory=list)
    beats_patched: List[LLMAuditedBeatPatch] = Field(default_factory=list)


class LLMM3BatchAuditResponse(BaseModel):
    script_audits: List[LLMScriptAuditResult]
