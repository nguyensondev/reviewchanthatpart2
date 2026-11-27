"""
Data Contract & Pydantic Schemas for Module M1 (Smart Filter & Multi-Brand Ranker).

M1 reads ONLY `data/m0_candidate_pool.json` and outputs `data/m1_qualified_skus.json`.
M1 is completely agnostic of M2's script format (Single-SKU vs Combo). Its sole responsibility
is objective product filtering, 100-point multi-criteria scoring (where Affiliate is capped at 15 pts),
selecting the Category Winner & Runner-Up, preserving Rejected Foils for comparison, and producing
a transparent Head-to-Head Verdict.
"""
from __future__ import annotations

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field

from pipeline.schemas_m0 import (
    AffiliateListingMatch,
    CandidateArchetype,
    CaveatSeverity,
    DomainId,
    MarketPriceRangeVND,
    YouTubeEvidenceClip,
)


AffiliateStatus = Literal[
    "READY_WITH_BEST_LINK",  # Có sẵn link Affiliate sạch, đúng mẫu từ kho CSV
    "NEEDS_LINK_LOOKUP",     # Sản phẩm tốt đạt chuẩn nhưng chưa có trong CSV (cần lấy link sau)
]


class ScoreBreakdown100(BaseModel):
    price_value_and_specs_score: float = Field(
        ..., description="Điểm Tương quan Giá / Trung vị ngành & Thông số kỹ thuật (Tối đa 35.0)"
    )
    durability_and_caveat_score: float = Field(
        ..., description="Điểm Độ bền & Mức độ lành tính của nhược điểm (Tối đa 25.0)"
    )
    evidence_and_visual_score: float = Field(
        ..., description="Điểm Bằng chứng Video YouTube & Đánh giá thực tế (Tối đa 25.0)"
    )
    affiliate_readiness_bonus: float = Field(
        ..., description="Điểm cộng Sẵn sàng Affiliate - Tie-breaker (Tối đa 15.0)"
    )
    total_score_100: float = Field(
        ..., description="Tổng điểm trên thang 100.0"
    )
    score_explanation_vi: str = Field(
        ..., description="Giải thích ngắn gọn cách tính từng đầu điểm của ứng viên này"
    )


class EvaluatedCandidateSKU(BaseModel):
    candidate_id: str
    rank_in_category: int = Field(..., description="Thứ hạng trong nhóm công năng (1 = Quán quân, 2 = Á quân...)")
    selection_role: Literal["PRIMARY_WINNER", "RUNNER_UP", "ALTERNATIVE_QUALIFIED"] = Field(
        ..., description="Vai trò sau khi xếp hạng trong nhóm công năng"
    )
    brand: str
    model_code: Optional[str] = None
    product_name_vi: str
    archetype: CandidateArchetype
    estimated_market_price_vnd: int
    price_vs_median_ratio: float = Field(
        ..., description="Tỉ lệ Giá ứng viên / Giá trung vị của nhóm công năng (VD: 1.05)"
    )
    key_specs: Dict[str, str] = Field(default_factory=dict)
    highlight_pros_vi: List[str] = Field(default_factory=list)
    honest_caveat_vi: str
    caveat_severity: CaveatSeverity
    score_breakdown: ScoreBreakdown100
    affiliate_status: AffiliateStatus
    best_affiliate_offer: Optional[AffiliateListingMatch] = Field(
        default=None,
        description="Offer Shopee Affiliate tốt nhất và sạch nhất (nếu có trong CSV)",
    )
    youtube_evidence: List[YouTubeEvidenceClip] = Field(default_factory=list)
    web_evidence_snippets: List[str] = Field(default_factory=list)


class RejectedFoilSKU(BaseModel):
    candidate_id: str
    brand: str
    model_code: Optional[str] = None
    product_name_vi: str
    archetype: CandidateArchetype
    estimated_market_price_vnd: int
    caveat_severity: CaveatSeverity
    honest_caveat_vi: str
    rejection_gate: Literal[
        "DEALBREAKER_DEFECT",
        "MULTI_MODEL_AMBIGUOUS",
        "INSUFFICIENT_VIDEO_EVIDENCE",
        "NO_CLEAN_OFFICIAL_FOOTAGE",
        "RECENT_BRAND_CATEGORY_SIBLING",
        "LOW_TIER_OEM_FOIL",
    ] = Field(..., description="Lý do chính khiến mẫu này bị đưa vào nhóm đối chứng nên tránh")
    rejection_reason_vi: str = Field(
        ..., description="Giải thích chi tiết vì sao không khuyên người xem mua mẫu này (dùng làm tư liệu đối chứng cho M2)"
    )


class HeadToHeadVerdict(BaseModel):
    winner_candidate_id: str
    winner_summary_title_vi: str = Field(
        ..., description="Tuyên bố ngắn gọn về Quán quân của nhóm công năng"
    )
    why_winner_chosen_vi: str = Field(
        ..., description="Lý do cốt lõi chọn Quán quân dựa trên thông số, độ bền và mức giá hợp lý"
    )
    why_winner_beats_runner_up_vi: str = Field(
        ..., description="Lập luận đối đầu trực diện: Tại sao chọn Quán quân mà không chọn Á quân hoặc các hãng khác cùng phân khúc?"
    )
    why_avoid_rejected_foils_vi: str = Field(
        ..., description="Cảnh báo thực tế: Tại sao người mua không nên ham rẻ chọn mẫu OEM/hàng lỗi trong rejected_foils?"
    )
    honest_caveat_framing_vi: str = Field(
        ..., description="Cách truyền đạt nhược điểm của Quán quân một cách trung thực để tăng độ tin cậy"
    )


class QualifiedCategoryResult(BaseModel):
    category_id: str
    domain: DomainId = Field(default="CLEANING_CARE", description="Chủ đề gốc (CLEANING_CARE hoặc MINI_APPLIANCE)")
    category_name_vi: str
    problem_solved_vi: str
    observed_market_price_range_vnd: MarketPriceRangeVND
    category_confidence_score: float = Field(
        ..., description="Điểm chất lượng tổng thể của nhóm công năng này (dựa trên điểm của Quán quân và độ dày bằng chứng)"
    )
    primary_winner: EvaluatedCandidateSKU = Field(
        ..., description="SKU Quán quân (Hạng 1) đại diện tốt nhất cho nhóm công năng này"
    )
    runner_up: Optional[EvaluatedCandidateSKU] = Field(
        default=None, description="SKU Á quân (Hạng 2) để so sánh đối đầu hoặc làm phương án thay thế"
    )
    all_qualified_ranked: List[EvaluatedCandidateSKU] = Field(
        default_factory=list, description="Toàn bộ các SKU vượt qua cổng lọc, xếp theo điểm giảm dần"
    )
    rejected_foils: List[RejectedFoilSKU] = Field(
        default_factory=list,
        description="Danh sách các mẫu bị loại hoặc hàng OEM giá rẻ kém bền được giữ lại làm đối chứng phản diện",
    )
    head_to_head_verdict: HeadToHeadVerdict


class M1QualifiedSKUsOutput(BaseModel):
    schema_version: str = Field(default="1.0.0", description="Phiên bản schema hợp đồng dữ liệu M1 -> M2")
    m1_run_id: str
    source_m0_run_id: str
    generated_at: str
    domain: DomainId
    domain_name_vi: str
    sub_niche_id: str
    sub_niche_name_vi: str
    problem_context_vi: str
    scoring_weights_policy: Dict[str, int] = Field(
        default_factory=lambda: {
            "price_value_and_specs_max": 35,
            "durability_and_caveat_max": 25,
            "evidence_and_visual_max": 25,
            "affiliate_readiness_bonus_max": 15,
        }
    )
    llm_models_used: List[str] = Field(default_factory=list)
    qualified_categories: List[QualifiedCategoryResult] = Field(default_factory=list)


# --- Internal LLM Schema for Step M1.3 (Batch Comparative Judge) ---

class LLMCategoryVerdictItem(BaseModel):
    category_id: str
    winner_candidate_id: str
    winner_summary_title_vi: str
    why_winner_chosen_vi: str
    why_winner_beats_runner_up_vi: str
    why_avoid_rejected_foils_vi: str
    honest_caveat_framing_vi: str


class LLMComparativeJudgeBatchResponse(BaseModel):
    verdicts: List[LLMCategoryVerdictItem]
