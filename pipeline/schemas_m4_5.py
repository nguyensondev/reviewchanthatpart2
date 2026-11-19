"""
Data Contract & Pydantic Schemas for Module M4.5 (AI Vision Video QA/QC Auditor).

M4.5 sits strictly between M4 (Video Producer) and PublishPackager / M5 (Social Publisher):
  - Reads rendered 9:16 MP4 files + `data/m4_production_output.json` + `data/video_registry.json`.
  - Extracts a lightweight 3x2 Storyboard Contact Sheet JPEG (6 frames across Beat 1..Beat 5)
    and inspects visual content via Gemini Full-Flash Multimodal Vision.
  - Verifies:
      1. Visual content matches the script's actual product category/brand (blocks war movies, gaming, wrong products).
      2. Zero CapCut / TikTok / 3rd-party shop watermarks or phone banners inside the video frames.
      3. Zero talking-head human reviewers.
      4. Clean 9:16 Layout B1 composition, readable Vietnamese subtitles, and top graphic cards.
  - Outputs `data/m4_5_video_qaqc_report.json` and automatically purges + blacklists any rejected video.
"""
from __future__ import annotations

from typing import List, Literal, Optional
from pydantic import BaseModel, Field


VideoQAQCStatus = Literal[
    "APPROVED_VIDEO",
    "REJECTED_BAD_VIDEO",
]

VideoDefectCode = Literal[
    "WRONG_VIDEO_CONTENT",
    "WATERMARK_OR_CAPCUT_DETECTED",
    "HUMAN_TALKING_HEAD_DETECTED",
    "CORRUPTED_OR_BLACK_FRAMES",
    "LAYOUT_OR_SUBTITLE_DEFECT",
]


class LLMM45VideoVisionVerdict(BaseModel):
    is_matching_product_footage: bool = Field(
        ...,
        description="True nếu khung hình video ở giữa thực sự quay đúng loại sản phẩm trong kịch bản; False nếu là phim ảnh, chiến tranh, game, hoạt hình hoặc sai hẳn loại sản phẩm",
    )
    has_capcut_or_tiktok_or_shop_watermark: bool = Field(
        ...,
        description="True nếu trong khung hình video sản phẩm có dính logo CapCut, watermark TikTok/Douyin, số điện thoại hoặc tên shop bán hàng bên thứ 3",
    )
    has_talking_head_human_reviewer: bool = Field(
        ...,
        description="True nếu khung hình chính xuất hiện khuôn mặt người/YouTuber đang ngồi nói chuyện trước ống kính (vi phạm chuẩn Faceless)",
    )
    layout_and_subtitles_clean: bool = Field(
        default=True,
        description="True nếu bố cục 9:16 rõ nét, phụ đề tiếng Việt và thẻ thông tin hiển thị sạch đẹp không bị lỗi đen màn hình",
    )
    product_visual_match_score: float = Field(
        ...,
        description="Điểm khớp nội dung hình ảnh sản phẩm với kịch bản (0..40). Chấm 0 nếu sai sản phẩm hoặc là cảnh phim/game",
    )
    watermark_cleanliness_score: float = Field(
        ...,
        description="Điểm sạch logo/watermark (0..30). Chấm 0 nếu có logo CapCut, TikTok, banner số điện thoại shop",
    )
    layout_quality_score: float = Field(
        ...,
        description="Điểm chất lượng bố cục 9:16, độ nét & phụ đề (0..30)",
    )
    detected_visual_content_vi: str = Field(
        ...,
        description="Mô tả khách quan bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ những gì thực sự đang hiển thị trong 6 khung hình của video",
    )
    auditor_verdict_vi: str = Field(
        ...,
        description="Kết luận thẩm định video bằng tiếng Việt CÓ DẤU ĐẦY ĐỦ (nêu rõ vì sao Đạt hoặc vì sao bị Loại)",
    )


class AuditedVideoQAQCItem(BaseModel):
    script_id: str
    title_vi: str
    category_id: str = "general"
    expected_product_summary_vi: str = ""
    video_path: str
    file_size_bytes: int = 0
    duration_sec: float = 0.0
    source_video_ids: List[str] = Field(default_factory=list)
    storyboard_image_path: str = ""
    qaqc_video_status: VideoQAQCStatus
    defect_codes: List[VideoDefectCode] = Field(default_factory=list)
    product_visual_match_score: float = 0.0
    watermark_cleanliness_score: float = 0.0
    layout_quality_score: float = 0.0
    total_video_qaqc_score_100: float = 0.0
    detected_visual_content_vi: str = ""
    auditor_verdict_vi: str = ""
    blacklisted_source_video_ids: List[str] = Field(default_factory=list)
    was_cached_pass: bool = False
    audited_at: str


class M45VideoQAQCReport(BaseModel):
    schema_version: str = "1.0.0"
    m4_5_run_id: str
    generated_at: str
    llm_models_used: List[str] = Field(default_factory=list)
    total_audited: int = 0
    approved_video_count: int = 0
    rejected_video_count: int = 0
    audited_videos: List[AuditedVideoQAQCItem] = Field(default_factory=list)
