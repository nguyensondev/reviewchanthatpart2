"""
Pydantic Data Contracts for Module M_TREND (`pipeline/schemas_m_trend.py`).

Shared contract module (safe to import from M0, M1, M2, M4, PublishPackager, and M_TREND
with zero cross-stage coupling):
  - Compliance Gate Policy (`SourceCompliancePolicy`)
  - Normalized External & Owned Signals (`NormalizedTrendSignal`)
  - Exponential Half-Life Time-Decay & TTL (`compute_effective_score_now`, `load_active_trend_opportunities`)
  - 2-Tier Queue Priority (`NORMAL_TREND` vs `FAST_TRACK_TREND`) & Dynamic SEO fields
  - Diagnostics (`TrendStatusReasonCode`) & 3-Tier Re-Entry Policy (`ReEntryAction`)
  - Ephemeral Top-8 Active JSON Contract (`MTrendOpportunitiesOutput` -> `data/m_trend_opportunities.json`)
"""
from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

TrendHorizon = Literal["FLASH_EVENT", "SEASONAL_SHIFT"]
TrendPriorityTier = Literal["NORMAL_TREND", "FAST_TRACK_TREND"]
VerificationStatus = Literal[
    "EARLY_DISCOVERY",
    "CANDIDATE",
    "MARKETPLACE_CONFIRMED",
    "REJECTED",
    "EXPIRED",
]
ConfidenceLevel = Literal["LOW", "MEDIUM", "HIGH"]
ReEntryAction = Literal[
    "NEW_OPPORTUNITY",
    "SUPPRESSED_RECENTLY_COVERED",
    "DIVERSIFY_MODEL_OR_COMBO",
    "RESURGENCE_REQUIRE_NEW_ANGLE",
    "DUPLICATE_EVENT_IGNORED",
]
TrendStatusReasonCode = Literal[
    "OK_HAS_OPPORTUNITIES",
    "ERROR_COLLECTOR_FAILURE",
    "WARNING_LOGIC_ZERO_MATCH",
    "NORMAL_NO_NEW_TREND_OR_ALL_IN_COOLDOWN",
]
TrendDomain = Literal["TECH_DESK", "KITCHEN_TECH"]

FLASH_EVENT_HALF_LIFE_HOURS = 72.0
FLASH_EVENT_TTL_HOURS = 120.0  # 5 days
SEASONAL_SHIFT_HALF_LIFE_HOURS = 168.0
SEASONAL_SHIFT_TTL_HOURS = 336.0  # 14 days
MIN_ACTIVE_OPPORTUNITY_SCORE = 60.0
FAST_TRACK_MIN_SCORE = 78.0
MAX_ACTIVE_OPPORTUNITIES = 8

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TREND_OPPORTUNITIES_PATH = PROJECT_ROOT / "data" / "m_trend_opportunities.json"


def compute_effective_score_now(
    base_score: float,
    last_signal_at_iso: str,
    half_life_hours: float,
    expires_at_iso: str,
    now: Optional[dt.datetime] = None,
) -> tuple[float, bool]:
    """
    Re-evaluate an opportunity's score at read time (`t_now`) using exponential half-life decay.
    Returns `(effective_score_now, is_expired)`.
    """
    now_dt = now or dt.datetime.now(dt.timezone.utc)
    if now_dt.tzinfo is None:
        now_dt = now_dt.replace(tzinfo=dt.timezone.utc)

    try:
        exp_dt = dt.datetime.fromisoformat(expires_at_iso.replace("Z", "+00:00"))
        if exp_dt.tzinfo is None:
            exp_dt = exp_dt.replace(tzinfo=dt.timezone.utc)
        if now_dt >= exp_dt:
            return 0.0, True
    except Exception:
        pass

    try:
        sig_dt = dt.datetime.fromisoformat(last_signal_at_iso.replace("Z", "+00:00"))
        if sig_dt.tzinfo is None:
            sig_dt = sig_dt.replace(tzinfo=dt.timezone.utc)
        delta_hours = max(0.0, (now_dt - sig_dt).total_seconds() / 3600.0)
    except Exception:
        delta_hours = 0.0

    hl = max(12.0, float(half_life_hours or FLASH_EVENT_HALF_LIFE_HOURS))
    decayed = float(base_score) * math.pow(0.5, delta_hours / hl)
    decayed = round(max(0.0, min(100.0, decayed)), 1)
    return decayed, (decayed < MIN_ACTIVE_OPPORTUNITY_SCORE)


class SourceCompliancePolicy(BaseModel):
    source_id: str
    provider: str
    access_type: str
    feed_url: Optional[str] = None
    license_ref: str = ""
    commercial_use_permitted: bool = False
    authorization_status: Literal["approved", "pending", "suspended", "expired"] = "pending"
    enabled: bool = False
    checked_at: Optional[str] = None
    expires_at: Optional[str] = None
    max_requests_per_day: int = 0
    retention_days: int = 45
    notes: str = ""


class SourceRunAudit(BaseModel):
    source_id: str
    provider: str
    status: Literal[
        "SUCCESS",
        "NOT_MODIFIED_304",
        "SKIPPED_DISABLED",
        "BLOCKED_COMPLIANCE",
        "SUSPENDED_401_403",
        "ERROR_NETWORK",
    ]
    fetched_count: int = 0
    new_inserted_count: int = 0
    http_status: Optional[int] = None
    note: str = ""


class NormalizedTrendSignal(BaseModel):
    event_id: str
    source_id: str
    source_provider: str
    source_object_id: str
    canonical_event_id: str
    event_time_utc: str
    collected_at_utc: str
    title: str
    summary: str = ""
    reference_url: str = ""
    region: str = "VN"
    metric_name: str = "article_mention"
    metric_value: float = 1.0
    metric_kind: Literal["interval", "cumulative", "rank", "index"] = "interval"
    data_rights: str = "public_metadata_only"
    quality: Literal["observed", "estimated", "inferred"] = "observed"


class MatchedSKUCandidate(BaseModel):
    sku_key: str
    product_name: str
    brand: str = ""
    shop_name: str = ""
    price_vnd: int = 0
    sales_count: int = 0
    commission_rate_pct: float = 0.0
    short_link: str = ""
    source_id: str = "owned_shopee_affiliate_csv"
    resolution_status: Literal[
        "verified_csv", "verified_sku_registry", "unverified_concept_only"
    ] = "unverified_concept_only"


class OpportunityScoreBreakdown(BaseModel):
    momentum_100: float = Field(..., ge=0.0, le=100.0)
    relevance_100: float = Field(..., ge=0.0, le=100.0)
    cross_source_100: float = Field(..., ge=0.0, le=100.0)
    marketplace_verification_100: Optional[float] = Field(default=None, ge=0.0, le=100.0)
    freshness_100: float = Field(..., ge=0.0, le=100.0)
    base_opportunity_score_100: float = Field(..., ge=0.0, le=100.0)
    effective_score_now_100: float = Field(..., ge=0.0, le=100.0)
    confidence_level: ConfidenceLevel = "LOW"
    detector_stats: Dict[str, float | str | bool] = Field(default_factory=dict)


class TrendOpportunityItem(BaseModel):
    opportunity_id: str
    canonical_event_id: str
    trend_title_vi: str
    need_pain_point_vi: str
    domain: TrendDomain
    product_concept_id: str
    product_concept_name_vi: str
    priority_tier: TrendPriorityTier = "NORMAL_TREND"
    trend_horizon: TrendHorizon = "FLASH_EVENT"
    half_life_hours: float = FLASH_EVENT_HALF_LIFE_HOURS
    first_seen_at: str
    last_signal_at: str
    expires_at: str
    verification_status: VerificationStatus = "EARLY_DISCOVERY"
    re_entry_action: ReEntryAction = "NEW_OPPORTUNITY"
    re_entry_note_vi: str = ""
    score_breakdown: OpportunityScoreBreakdown
    independent_source_count: int = 1
    independent_sources: List[str] = Field(default_factory=list)
    evidence_titles: List[str] = Field(default_factory=list)
    evidence_urls: List[str] = Field(default_factory=list)
    matched_skus: List[MatchedSKUCandidate] = Field(default_factory=list)
    why_matched_vi: str = ""
    trend_hook_angle_vi: str = ""
    seo_context_prefix_vi: str = ""
    seo_trend_keywords_vi: List[str] = Field(default_factory=list)
    seo_context_hashtag: str = ""
    recommended_narrative_angles: List[str] = Field(default_factory=list)


class LLMTrendReRankItem(BaseModel):
    product_concept_id: str
    is_genuine_buying_need: bool = Field(
        ...,
        description="True CHỈ KHI các tin tức/tín hiệu thực sự phản ánh nhu cầu mua hoặc vấn đề đời sống liên quan tới nhóm sản phẩm này; False nếu chỉ trùng từ khóa vô nghĩa hoặc tin giải trí/chính trị không liên quan mua sắm.",
    )
    relevance_score_100: float = Field(..., ge=0.0, le=100.0)
    trend_horizon: TrendHorizon = Field(
        ...,
        description="'FLASH_EVENT' cho thời tiết/bão/nồm/sự kiện ngắn hạn (3-5 ngày); 'SEASONAL_SHIFT' cho nhu cầu mùa vụ/thói quen kéo dài (7-14 ngày).",
    )
    trend_title_vi: str = Field(..., description="Tên chủ đề trend ngắn gọn bằng tiếng Việt có dấu chuẩn.")
    need_pain_point_vi: str = Field(..., description="Nỗi đau / nhu cầu thực tế người Việt đang gặp (tiếng Việt có dấu chuẩn).")
    why_matched_vi: str = Field(
        ...,
        description="Giải thích ngắn theo đúng mẫu: 'Trend X -> nhu cầu Y -> nhóm sản phẩm Z vì công dụng A' (tiếng Việt có dấu chuẩn).",
    )
    trend_hook_angle_vi: str = Field(
        ...,
        description="Câu Hook 3 giây đầu cực cuốn bằng tiếng Việt có dấu chuẩn để M2 đưa vào video review sản phẩm.",
    )
    seo_context_prefix_vi: str = Field(
        default="",
        description="Cụm ngữ cảnh ngắn 3-5 từ bằng tiếng Việt có dấu (KHÔNG ghi ngày tháng) để ghép đầu tiêu đề khi là FAST_TRACK_TREND (VD: 'Trời Nồm Ẩm Nên Mua', 'Tránh Cháy Nổ Sạc').",
    )
    seo_trend_keywords_vi: List[str] = Field(
        default_factory=list,
        description="2-3 cụm từ khóa tìm kiếm theo vấn đề thực tế bằng tiếng Việt có dấu (VD: ['chống nồm ẩm', 'quần áo lâu khô']).",
    )
    seo_context_hashtag: str = Field(
        default="",
        description="1 hashtag ngữ cảnh viết liền không dấu bắt đầu bằng '#' (VD: '#ChongNomAm', '#AnToanDien', '#BanPhimVanPhong').",
    )


class LLMTrendReRankResponse(BaseModel):
    evaluated_concepts: List[LLMTrendReRankItem] = Field(default_factory=list)


class MTrendOpportunitiesOutput(BaseModel):
    schema_version: str = "1.1.0"
    m_trend_run_id: str
    generated_at_utc: str
    generated_at_local: str
    status_reason_code: TrendStatusReasonCode
    diagnostic_summary_vi: str
    raw_events_collected_now: int = 0
    raw_events_24h: int = 0
    active_sources_count: int = 0
    failed_sources_count: int = 0
    suppressed_or_expired_count: int = 0
    total_active_opportunities: int = 0
    collector_audits: List[SourceRunAudit] = Field(default_factory=list)
    opportunities: List[TrendOpportunityItem] = Field(default_factory=list)


def load_active_trend_opportunities(
    opportunities_path: Path = DEFAULT_TREND_OPPORTUNITIES_PATH,
    now: Optional[dt.datetime] = None,
) -> List[TrendOpportunityItem]:
    """
    Read `data/m_trend_opportunities.json` safely and RE-COMPUTE live `effective_score_now_100`
    at read time (`t_now`) using Half-Life Time-Decay + `expires_at` TTL.
    Any expired or decayed (`< 60`) opportunity is automatically discarded (and FAST_TRACK_TREND
    is downgraded to NORMAL_TREND if its decayed score drops below 78).
    Returns active opportunities sorted descending by `(priority_tier == FAST_TRACK_TREND, effective_score_now)`.
    """
    if not opportunities_path.exists():
        return []
    try:
        raw_txt = opportunities_path.read_text(encoding="utf-8")
        parsed = MTrendOpportunitiesOutput.model_validate_json(raw_txt)
    except Exception:
        return []

    now_dt = now or dt.datetime.now(dt.timezone.utc)
    live_items: List[TrendOpportunityItem] = []
    for opp in parsed.opportunities:
        eff, is_exp = compute_effective_score_now(
            base_score=opp.score_breakdown.base_opportunity_score_100,
            last_signal_at_iso=opp.last_signal_at,
            half_life_hours=opp.half_life_hours,
            expires_at_iso=opp.expires_at,
            now=now_dt,
        )
        if is_exp or eff < MIN_ACTIVE_OPPORTUNITY_SCORE:
            continue
        new_tier: TrendPriorityTier = (
            "FAST_TRACK_TREND"
            if (opp.priority_tier == "FAST_TRACK_TREND" and eff >= FAST_TRACK_MIN_SCORE)
            else "NORMAL_TREND"
        )
        updated_sb = opp.score_breakdown.model_copy(update={"effective_score_now_100": eff})
        live_items.append(
            opp.model_copy(update={"score_breakdown": updated_sb, "priority_tier": new_tier})
        )

    live_items.sort(
        key=lambda o: (
            1 if o.priority_tier == "FAST_TRACK_TREND" else 0,
            o.score_breakdown.effective_score_now_100,
        ),
        reverse=True,
    )
    return live_items
