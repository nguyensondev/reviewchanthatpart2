"""
Data Contract & Pydantic Schemas for Module M0 (Autonomous Multi-Brand SKU Discovery).

M0 knows ONLY the 2 root domains (`TECH_DESK`, `KITCHEN_TECH`), its own history (`data/m0_history.json`),
and optional local Shopee Affiliate CSV files. It outputs `data/m0_candidate_pool.json` as a strict
black-box contract for Module M1.
"""
from __future__ import annotations

from typing import Dict, List, Literal, Optional
from pydantic import BaseModel, Field


DomainId = Literal["TECH_DESK", "KITCHEN_TECH"]

CandidateArchetype = Literal[
    "BRANDED_MAINSTREAM",     # Thương hiệu phổ thông uy tín (VD: Lock&Lock, Inochi, Ugreen, Baseus, Bear, Elmich)
    "BRANDED_PREMIUM",        # Thương hiệu cao cấp / chuyên sâu (VD: Kai, HyperWork, Anker, WMF, Joseph Joseph)
    "OEM_GENERIC",            # Hàng OEM không thương hiệu / giá rẻ làm mốc đối chiếu trung vị ngành hàng
    "MULTI_MODEL_AMBIGUOUS",  # Tin đăng gộp nhiều mã lập lờ / treo đầu dê bán thịt chó (đánh dấu để M1 lọc bỏ)
]

CaveatSeverity = Literal[
    "MINOR",               # Nhược điểm nhỏ cần nói rõ để tăng uy tín (VD: dây rút trên nắp chỉ nên lau ẩm)
    "MODERATE",            # Giới hạn công năng cần lưu ý (VD: cối 150ml chỉ xay tỏi ớt, không xay thịt sống)
    "DEALBREAKER_DEFECT",  # Lỗi phần cứng / trải nghiệm nghiêm trọng từ review (để M1 loại bỏ thẳng tay)
]


class MarketPriceRangeVND(BaseModel):
    min_vnd: int = Field(..., description="Giá thấp nhất ghi nhận trong nhóm công năng (thường là hàng OEM)")
    median_vnd: int = Field(..., description="Giá trung vị hợp lý của nhóm công năng trên thị trường")
    max_vnd: int = Field(..., description="Giá phân khúc cao cấp trong nhóm công năng")


class YouTubeEvidenceClip(BaseModel):
    video_id: str = Field(..., description="YouTube Video ID")
    url: str = Field(..., description="Link YouTube đầy đủ")
    title: str = Field(..., description="Tiêu đề video review/unbox/test thực tế")
    channel: str = Field(default="", description="Tên kênh YouTube")
    duration_sec: int = Field(default=0, description="Thời lượng giây")
    max_height: int = Field(default=0, description="Độ phân giải chiều dọc tối đa (VD: 720, 1080)")
    view_count: int = Field(default=0, description="Lượt xem")
    matched_query: str = Field(default="", description="Từ khóa tìm thấy video này")


class AffiliateListingMatch(BaseModel):
    item_id: str = Field(..., description="Mã sản phẩm Shopee")
    product_name: str = Field(..., description="Tên sản phẩm trên sàn")
    shop_name: str = Field(..., description="Tên gian hàng")
    price_vnd: int = Field(..., description="Giá bán niêm yết (VNĐ)")
    sales_count: int = Field(default=0, description="Số lượng đã bán")
    commission_rate_pct: float = Field(default=0.0, description="Tỉ lệ hoa hồng (%)")
    commission_amount_vnd: int = Field(default=0, description="Hoa hồng ước tính trên 1 đơn (VNĐ)")
    product_link: str = Field(default="", description="Link gốc sản phẩm")
    short_link: str = Field(default="", description="Link rút gọn Affiliate (s.shopee.vn/...)")
    is_ambiguous_listing: bool = Field(
        default=False,
        description="True nếu tiêu đề gộp nhiều model khác nhau gây nhầm lẫn phân loại giá rẻ",
    )
    match_confidence: Literal["EXACT_MODEL", "BRAND_AND_CATEGORY", "CATEGORY_FALLBACK"] = Field(
        default="BRAND_AND_CATEGORY",
        description="Mức độ khớp giữa ứng viên và dòng dữ liệu CSV Affiliate",
    )


class DiscoveredCandidateSKU(BaseModel):
    candidate_id: str = Field(..., description="Mã định danh ứng viên duy nhất trong đợt chạy (VD: cks321_locknlock)")
    brand: str = Field(..., description="Tên thương hiệu (VD: Lock&Lock, Inochi, Elmich, Kai, OEM)")
    model_code: Optional[str] = Field(
        default=None,
        description="Mã model cụ thể nếu có (VD: CKS321, EJM416, CM01). Để null nếu là OEM hoặc không có mã riêng",
    )
    product_name_vi: str = Field(..., description="Tên gọi chuẩn tiếng Việt của sản phẩm")
    archetype: CandidateArchetype = Field(..., description="Phân nhóm bản chất ứng viên")
    estimated_market_price_vnd: int = Field(..., description="Giá bán thực tế ước tính trên thị trường (VNĐ)")
    key_specs: Dict[str, str] = Field(
        default_factory=dict,
        description="Các thông số kỹ thuật cốt lõi (VD: dung tích, chất liệu lưỡi dao, cơ chế, công suất...)",
    )
    highlight_pros_vi: List[str] = Field(
        default_factory=list,
        description="Danh sách 2-4 điểm mạnh thực tế rút ra từ thông số và đánh giá người dùng",
    )
    honest_caveat_vi: str = Field(
        ...,
        description="Nhược điểm thực tế / lưu ý trung thực khi sử dụng (bắt buộc phải có để đảm bảo tính khách quan)",
    )
    caveat_severity: CaveatSeverity = Field(
        default="MINOR",
        description="Mức độ nghiêm trọng của nhược điểm",
    )
    why_discovered_vi: str = Field(
        default="",
        description="Lý do M0 đưa ứng viên này vào danh sách so sánh (VD: Đại diện phân khúc bền bỉ / Đối trọng giá rẻ)",
    )
    web_evidence_snippets: List[str] = Field(
        default_factory=list,
        description="Các trích dẫn thực tế từ bài viết/review trên web",
    )
    youtube_evidence: List[YouTubeEvidenceClip] = Field(
        default_factory=list,
        description="Danh sách video thực tế trên YouTube phục vụ kiểm chứng hình ảnh & hậu kỳ",
    )
    affiliate_matches: List[AffiliateListingMatch] = Field(
        default_factory=list,
        description="Các link Shopee Affiliate khớp từ kho CSV nội bộ (rỗng nếu chưa có CSV, không bắt buộc)",
    )


class FunctionalCategoryGroup(BaseModel):
    category_id: str = Field(..., description="Mã nhóm công năng (VD: pull_cord_garlic_chopper)")
    domain: DomainId = Field(default="KITCHEN_TECH", description="Chủ đề gốc của nhóm công năng (TECH_DESK hoặc KITCHEN_TECH)")
    category_name_vi: str = Field(..., description="Tên nhóm công năng tiếng Việt (VD: Cối xay tỏi ớt kéo tay)")
    problem_solved_vi: str = Field(..., description="Vấn đề thực tế mà nhóm sản phẩm này giải quyết")
    generic_search_queries_used: List[str] = Field(
        default_factory=list,
        description="Các từ khóa công năng KHÔNG gắn tên thương hiệu đã dùng để quét thị trường",
    )
    observed_market_price_range_vnd: MarketPriceRangeVND = Field(
        ...,
        description="Phổ giá thực tế trên thị trường của nhóm công năng này (Min / Median / Max)",
    )
    candidates: List[DiscoveredCandidateSKU] = Field(
        default_factory=list,
        description="Danh sách 3-6 ứng viên cạnh tranh đa thương hiệu & OEM trong cùng nhóm công năng",
    )


class M0CandidatePoolOutput(BaseModel):
    schema_version: str = Field(default="1.0.0", description="Phiên bản schema hợp đồng dữ liệu M0 -> M1")
    run_id: str = Field(..., description="ID của lần chạy M0")
    generated_at: str = Field(..., description="Thời gian tạo (ISO 8601)")
    domain: DomainId = Field(..., description="Chủ đề gốc: TECH_DESK hoặc KITCHEN_TECH")
    domain_name_vi: str = Field(..., description="Tên chủ đề gốc tiếng Việt")
    sub_niche_id: str = Field(..., description="Mã ngách chủ đề cụ thể mà M0 tự chọn")
    sub_niche_name_vi: str = Field(..., description="Tên ngách chủ đề tiếng Việt")
    problem_context_vi: str = Field(..., description="Bối cảnh nỗi đau / nhu cầu thực tế của người dùng")
    planner_reasoning_vi: str = Field(..., description="Lý do M0 chọn ngách này dựa trên lịch sử m0_history.json")
    llm_models_used: List[str] = Field(default_factory=list, description="Danh sách các model Gemini đã sử dụng")
    csv_files_scanned: List[str] = Field(default_factory=list, description="Danh sách file CSV Shopee Affiliate đã quét")
    functional_categories: List[FunctionalCategoryGroup] = Field(
        default_factory=list,
        description="Danh sách các nhóm công năng được khám phá trong đợt chạy này",
    )


# --- Internal LLM Structured Output Schemas (for M0.1 and M0.3) ---

class LLMPlannedCategoryQuery(BaseModel):
    category_id: str = Field(..., description="Slug tiếng Anh ngắn gọn (VD: pull_cord_garlic_chopper)")
    domain: DomainId = Field(..., description="Chủ đề gốc của nhóm công năng này: TECH_DESK hoặc KITCHEN_TECH")
    category_name_vi: str = Field(..., description="Tên nhóm sản phẩm theo công năng (KHÔNG chứa tên hãng)")
    problem_solved_vi: str = Field(..., description="Nỗi đau / vấn đề thực tế nhóm sản phẩm này giải quyết")
    generic_web_queries_vi: List[str] = Field(
        ...,
        description="2-3 từ khóa tìm kiếm web khách quan, KHÔNG chứa tên thương hiệu (VD: 'cối xay tỏi ớt kéo tay loại nào tốt', 'so sánh các loại bình xịt dầu ăn phun sương')",
    )
    generic_youtube_queries_vi: List[str] = Field(
        ...,
        description="2 từ khóa tìm video review/so sánh trên YouTube, KHÔNG chứa tên thương hiệu",
    )


class LLMSubNichePlanResponse(BaseModel):
    domain: DomainId
    sub_niche_id: str = Field(..., description="Slug định danh ngách chủ đề (VD: quick_spice_prep_tools)")
    sub_niche_name_vi: str = Field(..., description="Tên ngách chủ đề hấp dẫn bằng tiếng Việt")
    problem_context_vi: str = Field(..., description="Mô tả ngắn gọn vấn đề thực tế người dùng gặp phải")
    planner_reasoning_vi: str = Field(..., description="Giải thích vì sao chọn ngách này (tránh trùng lịch sử, tính thực dụng cao)")
    categories: List[LLMPlannedCategoryQuery] = Field(
        ...,
        description="Danh sách từ 2 đến 3 nhóm công năng bổ trợ hoặc độc lập trong ngách này",
    )


class LLMExtractedCandidateItem(BaseModel):
    candidate_id: str = Field(..., description="Slug ngắn gọn (VD: locknlock_cks321, inochi_yoko_chopper, oem_mini_chopper)")
    brand: str = Field(..., description="Thương hiệu (VD: Lock&Lock, Inochi, Elmich, Bear, Kai, Ugreen, Baseus, HyperWork, OEM)")
    model_code: Optional[str] = Field(default=None, description="Mã model chính xác nếu có (VD: CKS321, EJM416, CM01)")
    product_name_vi: str = Field(..., description="Tên sản phẩm rõ ràng bằng tiếng Việt")
    archetype: CandidateArchetype
    estimated_market_price_vnd: int = Field(..., description="Giá thị trường ước tính hợp lý bằng VNĐ (VD: 89000)")
    key_specs_pairs: List[str] = Field(
        default_factory=list,
        description="Danh sách thông số dạng 'Tên thông số: Giá trị' (VD: ['Dung tích: 150ml', 'Lưỡi dao: 3 lưỡi Inox 304', 'Cơ chế: Dây rút trợ lực'])",
    )
    highlight_pros_vi: List[str] = Field(..., description="2-4 ưu điểm thực tế cụ thể")
    honest_caveat_vi: str = Field(..., description="1 nhược điểm thực tế / lưu ý trung thực bắt buộc phải có")
    caveat_severity: CaveatSeverity
    why_discovered_vi: str = Field(..., description="Vai trò của ứng viên này trong bảng đối chiếu đa thương hiệu")
    specific_youtube_search_query_vi: str = Field(
        ...,
        description="Từ khóa YouTube cụ thể gồm Tên sản phẩm + Thương hiệu + Mã model (nếu có) để tìm video thực tế",
    )


class LLMCategoryExtractionResponse(BaseModel):
    category_id: str
    observed_min_price_vnd: int
    observed_median_price_vnd: int
    observed_max_price_vnd: int
    candidates: List[LLMExtractedCandidateItem] = Field(
        ...,
        description="Danh sách 4-5 ứng viên cạnh tranh đa thương hiệu (bắt buộc phải có ít nhất 2 thương hiệu có tên tuổi khác nhau VÀ 1 đại diện OEM_GENERIC để đối chiếu giá)",
    )
