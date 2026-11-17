"""
Data Contracts & Pydantic Schemas between Step A (`PublishPackager`) and Module M5 (`M5PublisherEngine`).
"""
from __future__ import annotations

from typing import Dict, List, Literal
from pydantic import BaseModel, Field


class PublishSKUAffiliateItem(BaseModel):
    sku_key: str
    candidate_id: str
    brand: str
    model_code: str = ""
    product_name_vi: str
    estimated_price_vnd: int = 0
    key_specs: Dict[str, str] = Field(default_factory=dict)
    highlight_pros_vi: List[str] = Field(default_factory=list)
    honest_caveat_vi: str = ""
    affiliate_status: Literal["READY_SHORT_LINK", "NEEDS_SHOPEE_LINK"] = "NEEDS_SHOPEE_LINK"
    short_link: str = Field(
        default="",
        description="Link rút gọn Shopee Affiliate chính (Official Mall Item hoặc Shopee Mall Search)",
    )
    mall_search_affiliate_link: str = Field(
        default="",
        description="Link Affiliate dẫn thẳng vào trang Shopee Mall của đúng mã Brand+Model (100% không bao giờ hết hàng)",
    )
    product_link: str = Field(default="", description="Link gốc sản phẩm trên Shopee")
    shop_name: str = Field(default="", description="Tên gian hàng trên Shopee")
    verification_tier: str = Field(
        default="TIER_1_OFFICIAL_MALL_ITEM",
        description="Cấp độ bảo chứng: TIER_1_OFFICIAL_MALL_ITEM / TIER_2_SHOPEE_MALL_SEARCH / SHOPEE_CSV / MANUAL_JSON",
    )
    commission_rate_pct: float = 0.0
    commission_amount_vnd: int = 0
    link_source: Literal["BROWSER_SESSION", "MANUAL_JSON", "SHOPEE_CSV", "M3_CONTRACT", "PENDING"] = "PENDING"
    shopee_search_keyword: str = ""
    shopee_mall_search_url: str = ""
    shopee_all_search_url: str = ""


class PublishVideoPackageItem(BaseModel):
    script_id: str
    script_format: str
    domain: str
    category_id: str
    video_status: str = "RENDERED"
    video_path: str
    video_exists_on_disk: bool = True
    duration_sec: float = 0.0
    voice_name: str = ""
    voice_gender: str = ""
    bgm_track: str = ""
    rendered_at: str = ""
    hook_title_on_screen_vi: str = Field(
        default="",
        description="Title ngắn <= 30 ký tự đã đóng lên 2s đầu của video",
    )
    title_vi: str
    youtube_shorts_title_vi: str = Field(
        ...,
        description="Tiêu đề chuẩn SEO 2 đầu từ khóa cho YouTube Shorts (<= 85 ký tự)",
    )
    facebook_reels_title_vi: str = Field(
        default="",
        description="Tiêu đề thu hút cho Facebook Reels",
    )
    youtube_shorts_description_vi: str = Field(
        default="",
        description="Mô tả 4 khối chuẩn Semantic SEO cho YouTube Shorts (kèm Timestamps & Long-tail Keywords)",
    )
    youtube_video_tags: List[str] = Field(
        default_factory=list,
        description="Danh sách 12-15 thẻ Tags ẩn (có dấu + không dấu) cho YouTube Data API v3",
    )
    chapter_timestamps_vi: List[str] = Field(
        default_factory=list,
        description="Các mốc thời gian thực tế 0:00 rút từ sub_shots của M4",
    )
    tiktok_caption_full_vi: str = Field(
        default="",
        description="Bản rút gọn tương thích ngược cho trường caption_vi của M4",
    )
    facebook_reels_caption_full_vi: str = Field(
        default="",
        description="Caption dạng Checklist ✅/⚠️ tối ưu tỷ lệ đọc và chốt đơn trên Facebook Reels",
    )
    pinned_comment_vi: str = Field(
        default="",
        description="Bình luận đầu tiên / Bình luận ghim gắn Link Shopee Mall Chính Hãng (giữ nguyên cho Facebook Reels)",
    )
    youtube_pinned_comment_vi: str = Field(
        default="",
        description="Bình luận đầu tiên tối ưu riêng cho YouTube Shorts (kèm hướng dẫn bấm Bio Hub kênh hoặc copy link)",
    )
    hashtags: List[str] = Field(default_factory=list)
    all_links_ready: bool = False
    skus: List[PublishSKUAffiliateItem] = Field(default_factory=list)


class PublishManifestOutput(BaseModel):
    schema_version: str = "2.0.0"
    generated_at: str
    total_rendered_unposted_videos: int
    videos_with_all_affiliate_links_ready: int
    videos_needing_shopee_links: int
    manual_links_json_path: str
    affiliate_csv_dir: str
    packages: List[PublishVideoPackageItem] = Field(default_factory=list)
