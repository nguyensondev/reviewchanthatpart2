"""
Data Contract & Pydantic Schemas for Module M4 (Autonomous 9:16 Video Producer — Layout B1).

M4 reads `data/m3_approved_scripts.json` (plus queued `SCRIPT_APPROVED` items in `data/video_registry.json`)
and outputs:
  - `output/videos/<script_id>.mp4` (1080x1920, 30fps, H.264/AAC, Layout B1 single-frame 16:9 + same-shot ambient blur)
  - `data/m4_production_output.json` (`M4ProductionOutput`)
  - Updates `data/video_registry.json` (`status = "RENDERED"`, never `"PUBLISHED"`)
  - Updates `data/sku_registry.json` (`has_solo_video` / `has_combo_video` -> `RETIRED_EXHAUSTED` when both True)
"""
from __future__ import annotations

from typing import Dict, List, Literal
from pydantic import BaseModel, Field


class SubShotClauseItem(BaseModel):
    clause_index: int
    beat_index: int
    source_video_id: str
    source_start_sec: float
    source_end_sec: float
    duration_sec: float
    face_free_verified: bool
    caption_free_verified: bool = True
    sharpness_score: float
    tts_text_vi: str
    subtitle_text_vi: str


class SocialPostPack(BaseModel):
    title_vi: str
    caption_vi: str
    hashtags: List[str] = Field(default_factory=list)
    pinned_comment_affiliate_vi: str
    affiliate_short_links: Dict[str, str] = Field(default_factory=dict)


class RenderedVideoItem(BaseModel):
    script_id: str
    script_format: str
    domain: str = "KITCHEN_TECH"
    category_id: str = "general"
    narrative_angle: str = "PAIN_POINT_REALITY_CHECK"
    title_vi: str
    hook_title_short_vi: str = Field(
        default="",
        description="Tiêu đề mở bài ngắn gọn (<= 30 ký tự) hiển thị ở 2s đầu video",
    )
    video_path: str
    video_status: Literal["RENDERED"] = Field(
        default="RENDERED",
        description="Trạng thái sản xuất tại M4: luôn là RENDERED (đã xuất file MP4, chưa đăng)",
    )
    layout_mode: Literal["B1", "cinema", "fullscreen"] = "B1"
    end_card_type: Literal["SCORE_VERDICT_CARD", "VERSUS_2COL_CARD", "BUY_SKIP_CARD"] = "SCORE_VERDICT_CARD"
    width: int = 1080
    height: int = 1920
    fps: int = 30
    duration_sec: float
    file_size_bytes: int
    voice_name: str = "BV074_streaming"
    voice_gender: Literal["FEMALE", "MALE"] = "FEMALE"
    single_voice_locked: bool = True
    bgm_track: str = ""
    render_started_at: str = ""
    render_finished_at: str = ""
    render_duration_sec: float = 0.0
    next_render_allowed_at: str = ""
    clauses_count: int
    unique_source_videos_used: List[str] = Field(default_factory=list)
    sub_shots: List[SubShotClauseItem] = Field(default_factory=list)
    social_post_pack: SocialPostPack


class M4ProductionOutput(BaseModel):
    schema_version: str = Field(default="2.0.0", description="Phiên bản schema hợp đồng dữ liệu M4")
    m4_run_id: str
    source_m3_run_id: str
    generated_at: str
    daily_video_quota_limit: int = Field(default=3)
    daily_videos_rendered_today: int
    remaining_video_quota_today: int
    min_render_cooldown_seconds: int = Field(default=7200)
    next_render_allowed_at: str = ""
    seconds_until_next_render: int = 0
    quota_status: Literal[
        "WITHIN_DAILY_QUOTA",
        "COOLDOWN_RESTING_2H",
        "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW",
        "WEEKLY_REST_DAY_OFF",
        "OUTSIDE_HUMAN_ACTIVE_HOURS",
    ]
    next_quota_reset_at: str
    seconds_until_quota_reset: int
    skipped_due_to_quota_script_ids: List[str] = Field(default_factory=list)
    rendered_videos: List[RenderedVideoItem] = Field(default_factory=list)
