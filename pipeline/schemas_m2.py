"""
Data Contract & Pydantic Schemas for Module M2 (Script Classifier & Generator).

M2 reads ONLY `data/m1_qualified_skus.json` and outputs `data/m2_script_drafts.json`.
M2 is decoupled from M0 and M1 internals: it consumes only M1's ranked winners, runner-ups,
rejected foils, and comparative verdicts to classify suitable script formats (never forcing
unnatural combos) and draft timed 9:16 short-video scripts for M3 QA/QC.
"""
from __future__ import annotations

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field

from pipeline.schemas_m0 import (
    AffiliateListingMatch,
    DomainId,
    YouTubeEvidenceClip,
)


ScriptFormat = Literal[
    "SINGLE_SKU_DEEP_DIVE",    # Kịch bản đánh giá sâu 1 SKU Quán quân
    "COMBO_WORKFLOW_SYNERGY",  # Kịch bản Combo các món bổ trợ nhau trong 1 quy trình
    "DUAL_TIER_COMPARISON",    # Kịch bản đặt lên bàn cân 2 kích cỡ / phân khúc giúp người xem chọn 1
]

MultiCategorySynergyType = Literal[
    "COMPLEMENTARY_WORKFLOW",  # Các Quán quân phối hợp liền mạch theo công đoạn
    "SIZE_OR_TIER_PAIR",       # Các Quán quân cùng công năng nhưng khác kích cỡ / tầm giá
    "SINGLE_CATEGORY_ONLY",    # Đợt chạy chỉ có 1 nhóm công năng
]

# 6 Upgrade-1 High-Retention Story Formats (plus legacy literals for backward-compatible JSON loading)
LLMNarrativeAngle = Literal[
    "REVERSE_CAVEAT_FIRST",       # Bóc điểm trừ thật ngay 3s đầu rồi lật ngược giá trị thực
    "HYPE_VS_REALITY_TEST",       # Thực chiến hay chỉ làm màu decor? Giải mã bằng cơ chế thật
    "SPEC_TRAP_AND_HIDDEN_COST",  # Thợ soi thông số ảo trên sàn & Cái giá ngầm của đồ rẻ
    "ANTI_DUST_DRAWER_TEST",      # Bài test vứt xó: Vì sao dùng mỗi ngày không lười dọn/cất
    "HABIT_DIAGNOSIS_STORY",      # Bắt bệnh thói quen đời thường (mỏi cổ tay 4h chiều, chói mắt đêm...)
    "FAST_25S_MICRO_LOOP",        # Siêu tốc 24-28 giây dồn dập (85-110 từ) + câu hỏi chia 2 phe kéo bình luận
]

NarrativeAngle = Literal[
    "REVERSE_CAVEAT_FIRST",
    "HYPE_VS_REALITY_TEST",
    "SPEC_TRAP_AND_HIDDEN_COST",
    "ANTI_DUST_DRAWER_TEST",
    "HABIT_DIAGNOSIS_STORY",
    "FAST_25S_MICRO_LOOP",
    # Legacy angles preserved so existing JSON files never fail validation:
    "PAIN_POINT_REALITY_CHECK",
    "MYTH_VS_FACT_TEARDOWN",
    "TIME_AND_EFFORT_MATH",
    "DECISION_GUIDE_BY_USER_TYPE",
    "WORKFLOW_UPGRADE_JOURNEY",
]

BeatType = Literal[
    "HOOK_FOIL_WARNING",
    "WINNER_REVEAL_SPECS",
    "HEAD_TO_HEAD_PROOF",
    "HONEST_CAVEAT",
    "VALUE_VERDICT_CTA",
    "COMBO_STEP_1",
    "COMBO_STEP_2_TRANSITION",
    "DUAL_TIER_BREAKDOWN",
]


class ScriptBeatSegment(BaseModel):
    beat_index: int = Field(..., description="Thứ tự phân cảnh (1..5)")
    beat_type: BeatType = Field(..., description="Vai trò cấu trúc của phân cảnh")
    start_sec: float = Field(..., description="Thời điểm bắt đầu (giây)")
    end_sec: float = Field(..., description="Thời điểm kết thúc (giây)")
    duration_sec: float = Field(..., description="Thời lượng phân cảnh (giây)")
    target_candidate_id: str = Field(
        ..., description="Mã candidate_id chính được nhắc tới trong phân cảnh này"
    )
    voiceover_vi: str = Field(..., description="Lời thoại tiếng Việt đọc voiceover")
    word_count: int = Field(..., description="Số từ trong lời thoại của phân cảnh")
    estimated_speaking_rate_wps: float = Field(
        ..., description="Tốc độ đọc ước tính (từ/giây)"
    )
    on_screen_text_vi: str = Field(
        ..., description="Chữ hiển thị trên màn hình 9:16 (tối đa 8 từ)"
    )
    visual_instruction_vi: str = Field(
        ..., description="Hướng dẫn dựng hình / cắt cảnh cho khâu hậu kỳ"
    )
    source_youtube_clip: Optional[YouTubeEvidenceClip] = Field(
        default=None,
        description="Video nguồn tham chiếu thực tế gán từ youtube_evidence của M1",
    )


class FactualClaimReference(BaseModel):
    candidate_id: str
    brand: str
    model_code: Optional[str] = None
    claimed_price_vnd: int
    claimed_specs_mentioned: List[str] = Field(default_factory=list)
    mandatory_caveat_mentioned_vi: str


class ScriptDraftItem(BaseModel):
    script_id: str
    script_format: ScriptFormat
    domain: DomainId = Field(default="KITCHEN_TECH", description="Chủ đề gốc của kịch bản (TECH_DESK hoặc KITCHEN_TECH)")
    category_id: str = Field(default="general", description="Mã nhóm công năng chính của kịch bản")
    narrative_angle: NarrativeAngle = Field(
        default="PAIN_POINT_REALITY_CHECK",
        description="Góc tiếp cận kịch bản độc lập để chống trùng lặp cấu trúc",
    )
    title_vi: str
    hook_title_short_vi: str = Field(
        default="",
        description="Tiêu đề mở bài cực ngắn (0-30 ký tự) hiển thị ở 2s đầu video, tránh che nội dung",
    )
    hook_preview_vi: str
    selection_rationale_vi: str = Field(
        ..., description="Lý do M2 chọn dạng kịch bản này cho SKU/nhóm SKU"
    )
    primary_sku_ids: List[str] = Field(default_factory=list)
    runner_up_sku_ids_referenced: List[str] = Field(default_factory=list)
    foil_sku_ids_referenced: List[str] = Field(default_factory=list)
    total_duration_sec: float
    total_word_count: int
    average_speaking_rate_wps: float
    beats: List[ScriptBeatSegment] = Field(default_factory=list)
    factual_claims_for_m3_audit: List[FactualClaimReference] = Field(
        default_factory=list,
        description="Mốc thông số, giá và nhược điểm chuẩn từ M1 để M3 đối chiếu chống bịa đặt",
    )
    affiliate_offers_attached: Dict[str, Optional[AffiliateListingMatch]] = Field(
        default_factory=dict,
        description="Offer Shopee Affiliate gắn kèm theo từng candidate_id (nếu có trong M1)",
    )


class M2ScriptDraftsOutput(BaseModel):
    schema_version: str = Field(
        default="1.0.0", description="Phiên bản schema hợp đồng dữ liệu M2 -> M3"
    )
    m2_run_id: str
    source_m1_run_id: str
    generated_at: str
    domain: DomainId
    domain_name_vi: str
    sub_niche_id: str
    sub_niche_name_vi: str
    problem_context_vi: str
    multi_category_synergy_type: MultiCategorySynergyType
    synergy_classification_reasoning_vi: str = Field(
        ...,
        description="Giải thích vì sao các Quán quân được ghép Combo bổ trợ hay Đặt lên bàn cân so sánh",
    )
    llm_models_used: List[str] = Field(default_factory=list)
    script_drafts: List[ScriptDraftItem] = Field(default_factory=list)


# --- Internal LLM Structured Output Schemas (shallow nesting for Free Tier speed) ---


class LLMGeneratedBeat(BaseModel):
    beat_index: int
    beat_type: BeatType
    target_candidate_id: str
    voiceover_vi: str = Field(
        ...,
        description="Lời thoại đọc voiceover BẮT BUỘC viết bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ chuẩn UTF-8, tuyệt đối cấm viết không dấu",
    )
    on_screen_text_vi: str = Field(
        ...,
        description="Chữ overlay ngắn gọn trên màn hình, TỐI ĐA 8 TỪ, BẮT BUỘC tiếng Việt CÓ DẤU ĐẦY ĐỦ chuẩn UTF-8",
    )
    visual_instruction_vi: str = Field(
        ...,
        description="Mô tả cảnh quay bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ",
    )


class LLMGeneratedScriptDraft(BaseModel):
    script_format: ScriptFormat
    domain: DomainId = Field(default="KITCHEN_TECH", description="Chủ đề gốc của kịch bản (TECH_DESK hoặc KITCHEN_TECH)")
    category_id: str = Field(default="general", description="Mã nhóm công năng chính của kịch bản")
    narrative_angle: LLMNarrativeAngle = Field(
        default="REVERSE_CAVEAT_FIRST",
        description="1 trong 6 Format kể chuyện Upgrade-1 khớp ngữ cảnh sản phẩm, cấm trùng giữa các kịch bản trong cùng đợt",
    )
    title_vi: str = Field(
        ...,
        description="Tiêu đề kịch bản tiếng Việt CÓ DẤU ĐẦY ĐỦ chuẩn UTF-8 (cấm viết không dấu)",
    )
    hook_title_short_vi: str = Field(
        default="",
        description="Tiêu đề mở bài cực ngắn TỐI ĐA 30 KÝ TỰ (10-30 ký tự) bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ",
    )
    selection_rationale_vi: str = Field(
        ...,
        description="Lý do chọn kịch bản viết bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ",
    )
    primary_sku_ids: List[str]
    runner_up_sku_ids_referenced: List[str]
    foil_sku_ids_referenced: List[str]
    beats: List[LLMGeneratedBeat]


class LLMM2BatchGenerationResponse(BaseModel):
    multi_category_synergy_type: MultiCategorySynergyType
    synergy_classification_reasoning_vi: str = Field(
        ...,
        description="Giải thích phân loại mối quan hệ bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ chuẩn UTF-8",
    )
    scripts: List[LLMGeneratedScriptDraft]
