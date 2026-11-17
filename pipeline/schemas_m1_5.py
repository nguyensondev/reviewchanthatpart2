"""
Data Contract & Pydantic Schemas for Module M1.5 (Independent Combo Curator & SKU Lifecycle Registry).

Responsibilities:
  1. Tracks normalized SKU lifecycle across runs in `data/sku_registry.json`:
     - Order between Solo and Combo videos is flexible (a SKU can enter a Solo video first and
       a Combo video later, OR enter a Combo video first and a Solo video later).
     - Counts `has_solo_video` / `has_combo_video` strictly when a video is `RENDERED` by M4.
     - Once `has_solo_video == True` AND `has_combo_video == True`, the SKU becomes
       `RETIRED_EXHAUSTED` and is permanently excluded from M0, M1.5, and M2.
  2. Evaluates whether 2..4 combo-eligible SKUs (`has_combo_video == False`) form a genuine,
     coherent workflow combo in the same `domain` and `MicroSceneId` with distinct `category_id`s,
     outputting `data/m1_5_combo_result.json` (`COMBO_APPROVED` or `BYPASS`).
"""
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field

from pipeline.schemas_m0 import (
    AffiliateListingMatch,
    CaveatSeverity,
    DomainId,
    YouTubeEvidenceClip,
)

MicroSceneId = Literal[
    "DESK_ERGONOMIC_WORKFLOW",    # Chuột/phím công thái học + đèn màn hình + giá đỡ laptop + kê tay
    "DESK_POWER_CONNECTIVITY",    # Sạc GaN để bàn + Hub USB-C + cáp từ tính + ổ cắm thông minh
    "KITCHEN_PRECISION_PREP",     # Cân điện tử + nhiệt kế nấu ăn + máy đánh bọt/pha chế + dụng cụ định lượng
    "KITCHEN_COOKING_STORAGE",    # Máy hút chân không + hộp bảo quản + bình xịt dầu + dụng cụ sơ chế nấu nướng
]

SKULifecycleStatus = Literal[
    "ACTIVE_UNRENDERED",          # Chưa có video lẻ và chưa có video combo (còn đủ cả 2 cơ hội)
    "SOLO_RENDERED_COMBO_OPEN",   # Đã render video lẻ (RENDERED), vẫn còn cơ hội vào video combo
    "COMBO_RENDERED_SOLO_OPEN",   # Đã render video combo (RENDERED), vẫn còn cơ hội vào video lẻ
    "RETIRED_EXHAUSTED",          # Đã render cả video lẻ VÀ video combo -> loại bỏ vĩnh viễn khỏi hệ thống
]

ComboCuratorStatus = Literal[
    "COMBO_APPROVED",             # Tìm được bộ 2..4 SKU phối hợp tự nhiên, bàn giao cho M2 viết kịch bản combo
    "BYPASS",                     # Không có bộ SKU nào đủ tự nhiên hoặc không đủ ứng viên -> M2 bỏ qua combo
]


def normalize_sku_key(
    brand: str,
    model_code: Optional[str] = None,
    fallback_id: str = "",
) -> str:
    """
    Canonical normalized SKU key (`brand_model`) so LLM slug variations
    (e.g. `baseus_iwok2` vs `iwok2_baseus` vs `DGIWK-P01`) map to one consistent key.
    """
    def _clean(s: str) -> str:
        s = (s or "").replace("đ", "d").replace("Đ", "D")
        nfkd = unicodedata.normalize("NFKD", s)
        ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c)).lower()
        return re.sub(r"[^a-z0-9]+", "_", ascii_str).strip("_")

    b_clean = _clean(brand)
    m_clean = _clean(model_code or "")
    if b_clean and m_clean and m_clean not in ("n_a", "none", "null", "no_model", "oem"):
        if m_clean.startswith(b_clean + "_"):
            return m_clean
        return f"{b_clean}_{m_clean}"
    f_clean = _clean(fallback_id)
    if b_clean and f_clean:
        if f_clean.startswith(b_clean + "_"):
            return f_clean
        return f"{b_clean}_{f_clean}"
    return f_clean or b_clean or "unknown_sku"


def compute_sku_lifecycle_status(has_solo_video: bool, has_combo_video: bool) -> SKULifecycleStatus:
    """Compute canonical SKU lifecycle status from `has_solo_video` and `has_combo_video` flags."""
    if has_solo_video and has_combo_video:
        return "RETIRED_EXHAUSTED"
    if has_solo_video and not has_combo_video:
        return "SOLO_RENDERED_COMBO_OPEN"
    if has_combo_video and not has_solo_video:
        return "COMBO_RENDERED_SOLO_OPEN"
    return "ACTIVE_UNRENDERED"


class SKURegistryEntry(BaseModel):
    sku_key: str = Field(..., description="Khóa chuẩn hóa duy nhất của SKU (brand + model_code)")
    candidate_id: str = Field(..., description="Mã candidate_id gần nhất từ M1")
    brand: str
    model_code: Optional[str] = None
    product_name_vi: str
    domain: DomainId
    category_id: str
    category_name_vi: str
    micro_scene_id: MicroSceneId
    estimated_market_price_vnd: int
    key_specs: Dict[str, str] = Field(default_factory=dict)
    highlight_pros_vi: List[str] = Field(default_factory=list)
    honest_caveat_vi: str
    caveat_severity: CaveatSeverity = "MINOR"
    m1_total_score_100: float = 80.0
    best_affiliate_offer: Optional[AffiliateListingMatch] = None
    youtube_evidence: List[YouTubeEvidenceClip] = Field(default_factory=list)
    has_solo_video: bool = Field(
        default=False,
        description="True khi SKU này đã được M4 render thành công ít nhất 1 video lẻ (RENDERED)",
    )
    has_combo_video: bool = Field(
        default=False,
        description="True khi SKU này đã được M4 render thành công trong ít nhất 1 video combo (RENDERED)",
    )
    solo_video_script_ids: List[str] = Field(default_factory=list)
    combo_video_script_ids: List[str] = Field(default_factory=list)
    lifecycle_status: SKULifecycleStatus = "ACTIVE_UNRENDERED"
    last_rendered_at: Optional[str] = Field(
        default=None,
        description="Mốc thời gian ISO gần nhất mà SKU này được M4 render xong video (Lẻ hoặc Combo)",
    )
    cooldown_days: int = Field(
        default=0,
        description="Số ngày nghỉ ngẫu nhiên (3..7 ngày) sau lần render gần nhất để tránh trùng lặp giữa Lẻ và Combo",
    )
    next_eligible_at: Optional[str] = Field(
        default=None,
        description="Mốc thời gian ISO sớm nhất SKU này được phép xuất hiện lại ở dạng video còn lại (last_rendered_at + 3..7 ngày)",
    )
    updated_at: str = ""


class ComboMemberRoleItem(BaseModel):
    sku_key: str
    candidate_id: str
    brand: str
    model_code: Optional[str] = None
    product_name_vi: str
    category_id: str
    estimated_market_price_vnd: int
    step_order: int = Field(..., description="Thứ tự xuất hiện trong quy trình combo (1..4)")
    role_in_combo_vi: str = Field(
        ...,
        description="Vai trò cụ thể của món này trong kịch bản phối hợp chung (tự nhiên, liên kết chặt với món kế tiếp)",
    )
    honest_caveat_vi: str
    youtube_evidence: List[YouTubeEvidenceClip] = Field(default_factory=list)
    best_affiliate_offer: Optional[AffiliateListingMatch] = None


class CuratedComboGroup(BaseModel):
    combo_id: str
    domain: DomainId
    micro_scene_id: MicroSceneId
    combo_title_vi: str = Field(..., description="Tiêu đề gợi ý cho kịch bản Combo (có dấu đầy đủ)")
    hook_title_short_vi: str = Field(
        ..., description="Tiêu đề mở bài cực ngắn (<= 30 ký tự) hiển thị 2s đầu video"
    )
    joint_workflow_scenario_vi: str = Field(
        ...,
        description="Kịch bản tình huống thực tế giải thích vì sao các SKU này dùng chung với nhau trong 1 bối cảnh",
    )
    why_not_forced_vi: str = Field(
        ...,
        description="Lý giải khách quan vì sao combo này hợp lý và không bị 'râu ông nọ cắm cằm bà kia'",
    )
    total_combo_price_vnd: int
    members: List[ComboMemberRoleItem] = Field(
        ..., description="Danh sách từ 2 đến 4 SKU cấu thành bộ Combo (tối đa 4 SKU)"
    )


class M15ComboCuratorOutput(BaseModel):
    schema_version: str = Field(default="1.0.0", description="Phiên bản schema hợp đồng dữ liệu M1.5 -> M2")
    m1_5_run_id: str
    source_m1_run_id: str
    generated_at: str
    status: ComboCuratorStatus
    bypass_reason_vi: str = Field(
        default="",
        description="Lý do bypass nếu status == 'BYPASS' (để M2 bỏ qua kịch bản combo một cách an toàn)",
    )
    active_solo_eligible_sku_keys: List[str] = Field(
        default_factory=list,
        description="Danh sách sku_key chưa có video lẻ (has_solo_video=False) và chưa bị RETIRED_EXHAUSTED",
    )
    active_combo_eligible_sku_keys: List[str] = Field(
        default_factory=list,
        description="Danh sách sku_key chưa có video combo (has_combo_video=False) và chưa bị RETIRED_EXHAUSTED",
    )
    retired_exhausted_sku_keys: List[str] = Field(
        default_factory=list,
        description="Danh sách sku_key đã có cả video lẻ lẫn video combo -> bị khóa vĩnh viễn",
    )
    cooldown_resting_sku_keys: List[str] = Field(
        default_factory=list,
        description="Danh sách sku_key vừa lên sóng (Lẻ hoặc Combo) và đang trong thời gian nghỉ ngẫu nhiên 3-7 ngày",
    )
    selected_combo: Optional[CuratedComboGroup] = None


# --- Internal LLM Schema for M1.5 Combo Evaluation ---

class LLMComboMemberDecision(BaseModel):
    sku_key: str
    step_order: int = Field(..., description="Thứ tự thao tác trong chuỗi quy trình (1, 2, 3, hoặc 4)")
    role_in_combo_vi: str = Field(
        ..., description="Vai trò cụ thể của SKU trong tình huống chung (1-2 câu tiếng Việt có dấu)"
    )


class LLMComboEvaluationResponse(BaseModel):
    should_create_combo: bool = Field(
        ...,
        description="True CHỈ KHI có từ 2 đến 4 SKU thực sự phối hợp tự nhiên trong cùng 1 tình huống thực tế; False nếu ghép bị khiên cưỡng",
    )
    bypass_reason_vi: str = Field(
        default="",
        description="Nếu should_create_combo=False, giải thích rõ vì sao các SKU hiện có không nên ghép combo",
    )
    domain: DomainId = "TECH_DESK"
    micro_scene_id: MicroSceneId = "DESK_ERGONOMIC_WORKFLOW"
    combo_title_vi: str = ""
    hook_title_short_vi: str = Field(
        default="",
        description="Tiêu đề mở bài cực ngắn TỐI ĐA 30 KÝ TỰ (0-30 ký tự) có dấu tiếng Việt",
    )
    joint_workflow_scenario_vi: str = ""
    why_not_forced_vi: str = ""
    selected_members: List[LLMComboMemberDecision] = Field(default_factory=list)
