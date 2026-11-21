"""
Module M5: Decoupled Multi-Platform SEO Publisher & Human-Like Anti-Spam Engine (`pipeline/m5_publisher.py`).

Black-Box Contract (Criterion #1):
  - Reads ONLY `output/publish_pack/publish_manifest.json` (`PublishManifestOutput` from `schemas_m5.py`)
    and `data/video_registry.json`. Zero code coupling to `M0..M4` or `PublishPackager`.

Crash Resilience & Per-Platform Resume (Criterion #2):
  - Writes all JSON reports and staged payloads atomically (`atomic_write_text` / `atomic_write_json`).
  - Tracks `published_platforms` per video in `data/video_registry.json`. If YouTube Shorts succeeded
    before a network drop/crash while Facebook Reels did not, resuming M5 skips YouTube Shorts and
    uploads ONLY the remaining Facebook Reel, transitioning `status = "PUBLISHED"` only when all
    configured platforms have succeeded.

Anti-Spam Human Behavior Guard (Criterion #5):
  - Active Vietnamese Creator Hours (`08:15 – 22:15` +- daily jitter): never uploads at 2-5 AM.
  - Strictly uploads at most `1` video per live invocation (`max_uploads_per_run = 1`), enforcing
    natural jittered cooldown (`2h12m .. 3h55m` with irregular seconds) between consecutive posts.
  - Waits a realistic human delay (`24s .. 62s`) after video upload before posting the First Comment,
    uses 8 varied natural Vietnamese comment intros, and staggers YouTube Shorts vs Facebook Reels
    by `35s .. 95s`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import shutil
import time
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

import httpx
from pydantic import BaseModel, Field

from pipeline.core_resilience import (
    HumanBehaviorGuard,
    MacOSLegacyRuntime,
    TelegramNotifier,
    atomic_write_json,
    atomic_write_text,
    safe_load_json,
)
from pipeline.schemas_m5 import PublishManifestOutput, PublishVideoPackageItem
from pipeline.shared_utils import sanitize_youtube_metadata_text, sanitize_youtube_tags
from pipeline.video_registry import VideoRegistryManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
OUTPUT_DIR = PROJECT_ROOT / "output"
PUBLISH_PACK_DIR = OUTPUT_DIR / "publish_pack"
DEFAULT_MANIFEST_PATH = PUBLISH_PACK_DIR / "publish_manifest.json"
DEFAULT_CREDENTIALS_PATH = CONFIG_DIR / "platform_credentials.json"
M5_PAYLOADS_DIR = PUBLISH_PACK_DIR / "m5_payloads"
DEFAULT_M5_REPORT_PATH = PROJECT_ROOT / "data" / "m5_publish_report.json"
DEFAULT_M5_COOLDOWN_PATH = PROJECT_ROOT / "data" / "m5_publish_state.json"

M5_MIN_DAILY_VIDEOS = 1
M5_MAX_DAILY_VIDEOS = 2


class M5ValidationCheck(BaseModel):
    script_id: str
    passed: bool
    checks: Dict[str, bool] = Field(default_factory=dict)
    notes_vi: List[str] = Field(default_factory=list)
    youtube_payload_path: str = ""
    facebook_reels_payload_path: str = ""


class M5PublishItemResult(BaseModel):
    script_id: str
    mode: Literal["STAGE_ONLY", "LIVE_UPLOAD", "MARK_PUBLISHED", "HUMAN_PACING_DEFERRED"]
    validation_passed: bool
    youtube_status: str = "STAGED"
    youtube_video_id: str = ""
    youtube_comment_id: str = ""
    facebook_reels_status: str = "STAGED"
    facebook_reel_id: str = ""
    facebook_comment_id: str = ""
    registry_status_after: str = "RENDERED"


class M5PublishReport(BaseModel):
    schema_version: str = "2.0.0"
    m5_run_id: str
    generated_at: str
    mode: str
    active_hours_ok: bool = True
    next_human_publish_allowed_at: str = ""
    total_candidates: int
    validated_ready_count: int
    published_count: int
    results: List[M5PublishItemResult] = Field(default_factory=list)


class M5PublisherEngine:
    """Module M5 Engine: Validates SEO + Official Affiliate links, stages API payloads, and publishes with human pacing."""

    def __init__(
        self,
        manifest_path: Path = DEFAULT_MANIFEST_PATH,
        credentials_path: Path = DEFAULT_CREDENTIALS_PATH,
        payloads_dir: Path = M5_PAYLOADS_DIR,
        report_path: Path = DEFAULT_M5_REPORT_PATH,
        state_path: Path = DEFAULT_M5_COOLDOWN_PATH,
    ) -> None:
        self.manifest_path = manifest_path
        self.credentials_path = credentials_path
        self.payloads_dir = payloads_dir
        self.report_path = report_path
        self.state_path = state_path
        self.video_registry = VideoRegistryManager()

    def _load_credentials(self) -> Dict[str, Any]:
        data = safe_load_json(self.credentials_path, lambda: {})
        return data if isinstance(data, dict) else {}

    def _load_m5_state(self) -> Dict[str, Any]:
        data = safe_load_json(self.state_path, lambda: {"next_publish_allowed_at": "", "events": [], "days": {}})
        if not isinstance(data, dict):
            return {"next_publish_allowed_at": "", "events": [], "days": {}}
        data.setdefault("days", {})
        return data

    def _save_m5_state(self, state: Dict[str, Any]) -> None:
        events = state.get("events")
        if isinstance(events, list) and len(events) > 60:
            state["events"] = events[-60:]
        days = state.get("days")
        if isinstance(days, dict) and len(days) > 30:
            for old_k in sorted(days.keys())[:-30]:
                days.pop(old_k, None)
        atomic_write_json(self.state_path, state)

    def get_daily_publish_quota_status(
        self, now: Optional[dt.datetime] = None
    ) -> Tuple[int, int, int, List[str]]:
        """
        Return `(used_today, daily_limit, remaining_today, published_ids_today)` for M5.
        Each day's `daily_limit` is hard-locked to a random integer in `[1, 2]` (`M5_MIN_DAILY_VIDEOS..M5_MAX_DAILY_VIDEOS`).
        """
        now_dt = now or dt.datetime.now().astimezone()
        day_key = now_dt.strftime("%Y-%m-%d")
        st = self._load_m5_state()
        days = st.setdefault("days", {})
        bucket = days.get(day_key)
        changed = False
        if not isinstance(bucket, dict):
            bucket = {
                "daily_limit": random.randint(M5_MIN_DAILY_VIDEOS, M5_MAX_DAILY_VIDEOS),
                "published_script_ids": [],
            }
            days[day_key] = bucket
            changed = True
        else:
            if "daily_limit" not in bucket or bucket["daily_limit"] not in (1, 2):
                bucket["daily_limit"] = random.randint(M5_MIN_DAILY_VIDEOS, M5_MAX_DAILY_VIDEOS)
                changed = True
            if not isinstance(bucket.get("published_script_ids"), list):
                bucket["published_script_ids"] = []
                changed = True
        if changed:
            self._save_m5_state(st)

        pub_ids = list(bucket["published_script_ids"])
        limit = int(bucket["daily_limit"])
        used = len(pub_ids)
        rem = max(0, limit - used)
        return used, limit, rem, pub_ids

    def _has_partial_video_to_resume(
        self,
        script_id: Optional[str] = None,
        pub_ids_today: Optional[List[str]] = None,
    ) -> bool:
        """
        Return True if `script_id` (or any currently `RENDERED` unposted video when `script_id is None`)
        has already uploaded to 1 platform (`len(published_platforms) > 0`) and needs to finish its 2nd platform.
        """
        today_set = set(pub_ids_today or [])
        if script_id:
            if script_id in today_set:
                return True
            entry = self.video_registry.get_entry(script_id)
            return bool(entry and entry.status == "RENDERED" and len(entry.published_platforms) > 0)

        for entry in self.video_registry.get_rendered_unposted_videos():
            if entry.script_id in today_set or len(entry.published_platforms) > 0:
                return True
        return False

    def can_publish_human_now(
        self,
        force_now: bool = False,
        now: Optional[dt.datetime] = None,
        script_id: Optional[str] = None,
    ) -> Tuple[bool, str, int]:
        """
        Check:
          1. Weekly Random Rest Day (`1` day/week Monday..Sunday when `not force_now`)
          2. Hard Daily Random Publish Quota (`1` or `2` videos/day max)
          3. Daily Randomized Human Active Hours (`06:00 – 09:30 AM` start to `22:00 – 22:30 PM`)
          4. Natural jittered inter-post cooldown (`2h12m .. 3h55m`)
        """
        now_dt = now or dt.datetime.now().astimezone()
        if not force_now:
            is_rest, _, rest_name, wait_rest = HumanBehaviorGuard.get_weekly_rest_day_info(now=now_dt)
            if is_rest:
                return False, f"WEEKLY_REST_DAY_OFF ({rest_name})", wait_rest

        used_today, limit_today, rem_today, pub_ids_today = self.get_daily_publish_quota_status(now=now_dt)

        # Allow finishing the second platform for a video already partially uploaded
        is_resuming_partial_video = self._has_partial_video_to_resume(
            script_id=script_id,
            pub_ids_today=pub_ids_today,
        )
        if not force_now and not is_resuming_partial_video and rem_today <= 0:
            tomorrow_start, _ = HumanBehaviorGuard.get_daily_active_window(now=now_dt + dt.timedelta(days=1))
            wait_tomorrow = max(60, int((tomorrow_start - now_dt).total_seconds()))
            return False, f"DAILY_M5_QUOTA_REACHED_{used_today}_OF_{limit_today}", wait_tomorrow

        if force_now:
            return True, "FORCED_NOW", 0

        in_hours, wait_hours_sec = HumanBehaviorGuard.is_within_human_active_hours(now=now_dt)
        if not in_hours:
            return False, "OUTSIDE_HUMAN_ACTIVE_HOURS", wait_hours_sec

        st = self._load_m5_state()
        next_str = str(st.get("next_publish_allowed_at") or "")
        if next_str:
            try:
                next_dt = dt.datetime.fromisoformat(next_str)
                if next_dt.tzinfo is None:
                    next_dt = next_dt.replace(tzinfo=now_dt.tzinfo)
                diff = int((next_dt - now_dt).total_seconds())
                if diff > 0:
                    return False, "HUMAN_INTER_POST_COOLDOWN", diff
            except Exception:
                pass
        return True, "READY", 0

    def get_next_publish_schedule_summary_vi(
        self,
        now: Optional[dt.datetime] = None,
        unposted_count: Optional[int] = None,
    ) -> str:
        """
        Compute a live, never-in-the-past Vietnamese schedule string for Module M5.
        """
        now_dt = now or dt.datetime.now().astimezone()
        if unposted_count is None:
            unposted_count = len(self.video_registry.get_rendered_unposted_videos())

        can_pub, reason, wait_sec = self.can_publish_human_now(force_now=False, now=now_dt)
        if can_pub:
            if unposted_count > 0:
                return f"🟢 Sẵn sàng đăng ngay ({unposted_count} video chờ, sẽ đăng ở chu kỳ kế tiếp)"
            return "🟢 Đã mở khóa (đang chờ M4 render video mới)"

        target_dt = now_dt + dt.timedelta(seconds=max(0, int(wait_sec)))
        target_str = target_dt.strftime("%d/%m %H:%M")
        mins_left = max(1, int(round(wait_sec / 60.0)))

        if reason.startswith("WEEKLY_REST_DAY_OFF"):
            return f"🏖️ Nghỉ ngày ngẫu nhiên trong tuần — Mở lại {target_str}"
        if reason.startswith("DAILY_M5_QUOTA_REACHED"):
            used_today, limit_today, _, _ = self.get_daily_publish_quota_status(now=now_dt)
            return f"✅ Đã đủ quota đăng hôm nay ({used_today}/{limit_today}) — Mở lại {target_str}"
        if reason == "OUTSIDE_HUMAN_ACTIVE_HOURS":
            return f"🌙 Chờ khung giờ hoạt động — Mở lúc {target_str} (còn {mins_left} phút)"
        if reason == "HUMAN_INTER_POST_COOLDOWN":
            return f"⏳ Nghỉ giãn cách sau bài trước — Tiếp theo lúc {target_str} (còn {mins_left} phút)"
        return f"⏳ {reason} — Mở lúc {target_str} (còn {mins_left} phút)"

    def _record_human_publish_event(self, script_id: str, platforms: List[str]) -> str:
        now_dt = dt.datetime.now().astimezone()
        day_key = now_dt.strftime("%Y-%m-%d")
        jitter_cd = HumanBehaviorGuard.sample_human_cooldown_seconds(min_floor_sec=7920)
        next_dt = now_dt + dt.timedelta(seconds=jitter_cd)
        next_iso = next_dt.isoformat(timespec="seconds")

        st = self._load_m5_state()
        days = st.setdefault("days", {})

        # Check if `script_id` was already counted in an earlier day's quota (partial upload resumed today)
        already_counted_prior_day = False
        for old_day_key, old_bucket in days.items():
            if old_day_key != day_key and isinstance(old_bucket, dict):
                old_list = old_bucket.get("published_script_ids")
                if isinstance(old_list, list) and script_id in old_list:
                    already_counted_prior_day = True
                    break

        bucket = days.setdefault(
            day_key,
            {
                "daily_limit": random.randint(M5_MIN_DAILY_VIDEOS, M5_MAX_DAILY_VIDEOS),
                "published_script_ids": [],
            },
        )
        pub_list = bucket.setdefault("published_script_ids", [])
        if script_id and script_id not in pub_list and not already_counted_prior_day:
            pub_list.append(script_id)

        st["next_publish_allowed_at"] = next_iso
        st.setdefault("events", []).append(
            {
                "script_id": script_id,
                "platforms": platforms,
                "published_at": now_dt.isoformat(timespec="seconds"),
                "cooldown_seconds_sampled": jitter_cd,
                "next_publish_allowed_at": next_iso,
            }
        )
        self._save_m5_state(st)
        return next_iso

    def _purge_completed_video_everywhere(self, script_id: str, video_path: str = "") -> None:
        """
        Once a video has been published to BOTH platforms (`status == 'PUBLISHED'`), completely
        remove the MP4 file, staged M5 payload JSONs, script records, and manifest entries so
        zero stale data ever accumulates on disk.
        """
        # 1. Delete MP4 file on disk
        try:
            resolved_mp4 = MacOSLegacyRuntime.resolve_portable_video_path(video_path, script_id)
            resolved_mp4.unlink(missing_ok=True)
        except Exception:
            pass
        (OUTPUT_DIR / "videos" / f"{script_id}.mp4").unlink(missing_ok=True)

        # 2. Delete staged M5 payloads and any M4 work dir
        for p in self.payloads_dir.glob(f"{script_id}_*.json"):
            p.unlink(missing_ok=True)
        work_dir = PROJECT_ROOT / "data" / "m4_work" / script_id
        if work_dir.exists():
            shutil.rmtree(work_dir, ignore_errors=True)

        # 3. Delete from video_registry.json & script files
        self.video_registry.delete_entry(script_id)
        self.video_registry.purge_script_from_m2_m3_files(script_id)

        # 4. Delete from data/m4_production_output.json
        m4_out_path = PROJECT_ROOT / "data" / "m4_production_output.json"
        if m4_out_path.exists():
            try:
                m4_raw = safe_load_json(m4_out_path, lambda: {})
                if isinstance(m4_raw, dict) and isinstance(m4_raw.get("rendered_videos"), list):
                    m4_raw["rendered_videos"] = [
                        rv for rv in m4_raw["rendered_videos"]
                        if isinstance(rv, dict) and rv.get("script_id") != script_id
                    ]
                    atomic_write_json(m4_out_path, m4_raw)
            except Exception:
                pass

        # 5. Delete from output/publish_pack/publish_manifest.json
        if self.manifest_path.exists():
            try:
                man_raw = safe_load_json(self.manifest_path, lambda: {})
                if isinstance(man_raw, dict) and isinstance(man_raw.get("packages"), list):
                    man_raw["packages"] = [
                        pk for pk in man_raw["packages"]
                        if isinstance(pk, dict) and pk.get("script_id") != script_id
                    ]
                    man_raw["total_rendered_unposted_videos"] = len(man_raw["packages"])
                    man_raw["ready_to_publish_count"] = sum(
                        1 for pk in man_raw["packages"] if isinstance(pk, dict) and pk.get("all_links_ready")
                    )
                    atomic_write_json(self.manifest_path, man_raw)
            except Exception:
                pass

        print(
            f"[M5 CLEANUP] Đã xóa hoàn toàn video & dữ liệu liên quan của `{script_id}` sau khi đăng đủ các nền tảng!",
            flush=True,
        )

    def validate_and_stage_package(self, pkg: PublishVideoPackageItem) -> M5ValidationCheck:
        """
        Verify all SEO & Affiliate requirements for `pkg` and write exact API request payloads
        atomically to `output/publish_pack/m5_payloads/`.
        """
        vpath = MacOSLegacyRuntime.resolve_portable_video_path(pkg.video_path, pkg.script_id)
        mp4_ok = vpath.exists() and vpath.stat().st_size > 100_000
        affiliate_ok = bool(pkg.all_links_ready and pkg.skus and all(s.short_link for s in pkg.skus))
        clean_yt_title = sanitize_youtube_metadata_text(pkg.youtube_shorts_title_vi, max_chars=95)
        clean_yt_desc = sanitize_youtube_metadata_text(pkg.youtube_shorts_description_vi, max_chars=4800)
        clean_yt_tags = sanitize_youtube_tags(pkg.youtube_video_tags, max_weighted_bytes=350, max_count=15)

        yt_title_ok = (
            20 <= len(clean_yt_title) <= 95
            and "#Shorts" in clean_yt_title
            and "<" not in clean_yt_title
            and ">" not in clean_yt_title
        )
        def _has_valid_aff_domain(txt: str) -> bool:
            return any(d in txt for d in ("s.shopee.vn", "shope.ee", ".workers.dev/", ".pages.dev/", "https://"))

        yt_desc_ok = (
            len(clean_yt_desc) >= 180
            and "0:00" in clean_yt_desc
            and "<" not in clean_yt_desc
            and ">" not in clean_yt_desc
            and _has_valid_aff_domain(clean_yt_desc)
        )
        tags_total_chars = sum(len(t) for t in clean_yt_tags)
        yt_tags_ok = 6 <= len(clean_yt_tags) <= 15 and tags_total_chars <= 400
        fb_caption_ok = (
            "✅" in pkg.facebook_reels_caption_full_vi
            and "⚠️" in pkg.facebook_reels_caption_full_vi
            and _has_valid_aff_domain(pkg.facebook_reels_caption_full_vi)
        )
        pinned_ok = _has_valid_aff_domain(pkg.pinned_comment_vi)

        checks = {
            "mp4_exists_and_valid": mp4_ok,
            "official_shopee_affiliate_links_ready": affiliate_ok,
            "youtube_dual_head_seo_title_ok": yt_title_ok,
            "youtube_4block_semantic_desc_and_timestamps_ok": yt_desc_ok,
            "youtube_backend_tags_ok": yt_tags_ok,
            "facebook_reels_checklist_caption_ok": fb_caption_ok,
            "pinned_comment_affiliate_ok": pinned_ok,
        }
        passed = all(checks.values())
        notes = [f"Chưa đạt kiểm định `{k}`" for k, ok in checks.items() if not ok]

        self.payloads_dir.mkdir(parents=True, exist_ok=True)
        yt_category_id = "28" if pkg.domain == "TECH_DESK" else "26"
        sampled_comment_delay = HumanBehaviorGuard.sample_post_upload_comment_delay_sec()

        yt_payload = {
            "platform": "youtube_shorts",
            "script_id": pkg.script_id,
            "video_file_path": pkg.video_path,
            "human_anti_spam_pacing": {
                "post_upload_comment_delay_sec": sampled_comment_delay,
                "active_window_local": "08:15-22:15",
            },
            "videos_insert_body": {
                "snippet": {
                    "title": clean_yt_title,
                    "description": clean_yt_desc,
                    "tags": clean_yt_tags,
                    "categoryId": yt_category_id,
                    "defaultLanguage": "vi",
                    "defaultAudioLanguage": "vi",
                },
                "status": {
                    "privacyStatus": "public",
                    "selfDeclaredMadeForKids": False,
                    "containsSyntheticMedia": True,
                },
            },
            "comment_threads_insert_body": {
                "snippet": {
                    "topLevelComment": {
                        "snippet": {
                            "textOriginal": pkg.youtube_pinned_comment_vi or pkg.pinned_comment_vi,
                        }
                    }
                }
            },
        }

        fb_payload = {
            "platform": "facebook_reels",
            "script_id": pkg.script_id,
            "video_file_path": pkg.video_path,
            "human_anti_spam_pacing": {
                "post_upload_comment_delay_sec": sampled_comment_delay,
                "active_window_local": "08:15-22:15",
            },
            "video_reels_publish_params": {
                "upload_phase": "finish",
                "video_state": "PUBLISHED",
                "description": pkg.facebook_reels_caption_full_vi,
            },
            "first_comment_post_params": {
                "message": pkg.pinned_comment_vi,
            },
        }

        yt_path = self.payloads_dir / f"{pkg.script_id}_youtube_shorts.json"
        fb_path = self.payloads_dir / f"{pkg.script_id}_facebook_reels.json"
        atomic_write_json(yt_path, yt_payload, keep_backup=False)
        atomic_write_json(fb_path, fb_payload, keep_backup=False)

        return M5ValidationCheck(
            script_id=pkg.script_id,
            passed=passed,
            checks=checks,
            notes_vi=notes,
            youtube_payload_path=str(yt_path),
            facebook_reels_payload_path=str(fb_path),
        )

    def _upload_youtube_short_and_comment(
        self, pkg: PublishVideoPackageItem, yt_creds: Dict[str, Any]
    ) -> Tuple[str, str]:
        client_id = str(yt_creds.get("client_id") or "").strip()
        client_secret = str(yt_creds.get("client_secret") or "").strip()
        refresh_token = urllib.parse.unquote(str(yt_creds.get("refresh_token") or "").strip()).split("&")[0].strip()
        if not (client_id and client_secret and refresh_token):
            raise RuntimeError("Missing YouTube OAuth2 credentials in config/platform_credentials.json")

        ca_bundle = MacOSLegacyRuntime.resolve_ca_bundle_path()
        with httpx.Client(timeout=60.0, verify=ca_bundle or True) as client:
            tok_resp = client.post(
                "https://oauth2.googleapis.com/token",
                data={
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "grant_type": "refresh_token",
                },
            )
            tok_resp.raise_for_status()
            access_token = tok_resp.json()["access_token"]
            headers = {"Authorization": f"Bearer {access_token}"}

            yt_cat = "28" if pkg.domain == "TECH_DESK" else "26"
            safe_title = sanitize_youtube_metadata_text(pkg.youtube_shorts_title_vi, max_chars=95)
            safe_desc = sanitize_youtube_metadata_text(pkg.youtube_shorts_description_vi, max_chars=4800)
            safe_tags = sanitize_youtube_tags(pkg.youtube_video_tags, max_weighted_bytes=350, max_count=15)

            meta_body: Dict[str, Any] = {
                "snippet": {
                    "title": safe_title,
                    "description": safe_desc,
                    "tags": safe_tags,
                    "categoryId": yt_cat,
                    "defaultLanguage": "vi",
                    "defaultAudioLanguage": "vi",
                },
                "status": {
                    "privacyStatus": str(yt_creds.get("privacy_status") or "public"),
                    "selfDeclaredMadeForKids": False,
                    "containsSyntheticMedia": True,
                },
            }

            vpath = MacOSLegacyRuntime.resolve_portable_video_path(pkg.video_path, pkg.script_id)
            upload_headers = {
                **headers,
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Length": str(vpath.stat().st_size),
                "X-Upload-Content-Type": "video/mp4",
            }
            init_resp = client.post(
                "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status",
                headers=upload_headers,
                json=meta_body,
            )
            if init_resp.status_code == 400 and (
                "invalidTags" in init_resp.text or "containsSyntheticMedia" in init_resp.text
            ):
                # Automatic self-healing retry if YouTube rejects a tag token or synthetic-media status field
                if "invalidTags" in init_resp.text:
                    meta_body["snippet"].pop("tags", None)
                if "containsSyntheticMedia" in init_resp.text:
                    meta_body["status"].pop("containsSyntheticMedia", None)
                init_resp = client.post(
                    "https://www.googleapis.com/upload/youtube/v3/videos?uploadType=resumable&part=snippet,status",
                    headers=upload_headers,
                    json=meta_body,
                )
            if init_resp.status_code >= 400:
                raise RuntimeError(
                    f"YouTube HTTP {init_resp.status_code} on videos.insert: {init_resp.text[:320]}"
                )
            upload_url = init_resp.headers["Location"]

            with vpath.open("rb") as vf:
                up_resp = client.put(
                    upload_url,
                    headers={**headers, "Content-Type": "video/mp4"},
                    content=vf.read(),
                    timeout=300.0,
                )
            up_resp.raise_for_status()
            video_id = up_resp.json()["id"]

            # Upload uniform 9:16 SKU cover thumbnail via YouTube Data API v3 `thumbnails.set`
            # (YouTube requires waiting until `processingDetails.processingStatus == 'succeeded'` on Shorts)
            cover_path = vpath.with_name(f"{vpath.stem}_cover.jpg")
            waited_for_thumb_sec = 0
            if cover_path.exists() and cover_path.stat().st_size > 10_000:
                try:
                    proc_ok = False
                    for _ in range(6):
                        st_resp = client.get(
                            f"https://www.googleapis.com/youtube/v3/videos?part=processingDetails&id={video_id}",
                            headers=headers,
                        )
                        if st_resp.status_code == 200:
                            items = st_resp.json().get("items") or []
                            p_stat = (items[0].get("processingDetails") or {}).get("processingStatus", "") if items else ""
                            if p_stat == "succeeded":
                                proc_ok = True
                                break
                        time.sleep(5)
                        waited_for_thumb_sec += 5
                    if proc_ok:
                        th_resp = client.post(
                            f"https://www.googleapis.com/upload/youtube/v3/thumbnails/set?videoId={video_id}&uploadType=media",
                            headers={**headers, "Content-Type": "image/jpeg"},
                            content=cover_path.read_bytes(),
                            timeout=60.0,
                        )
                        if th_resp.status_code == 200:
                            print(f"[M5 YOUTUBE] Đã áp ảnh bìa SKU chuẩn 9:16 ({cover_path.name}) thành công!", flush=True)
                        else:
                            print(
                                f"[M5 YOUTUBE] Đặt thumbnail tùy chỉnh chưa áp được (HTTP {th_resp.status_code}): {th_resp.text[:160]}",
                                flush=True,
                            )
                except Exception as th_exc:
                    print(f"[M5 YOUTUBE] Bỏ qua đặt thumbnail tùy chỉnh (không ảnh hưởng video): {th_exc}", flush=True)

            # Human delay before posting external-link comment (Anti-Spam Guard)
            delay_s = max(5, HumanBehaviorGuard.sample_post_upload_comment_delay_sec() - waited_for_thumb_sec)
            print(f"[M5 HUMAN GUARD] Chờ tự nhiên {delay_s}s trước khi thả Comment đầu tiên trên YouTube...", flush=True)
            time.sleep(delay_s)

            comment_id = ""
            try:
                c_resp = client.post(
                    "https://www.googleapis.com/youtube/v3/commentThreads?part=snippet",
                    headers=headers,
                    json={
                        "snippet": {
                            "videoId": video_id,
                            "topLevelComment": {
                                "snippet": {
                                    "textOriginal": pkg.youtube_pinned_comment_vi or pkg.pinned_comment_vi,
                                }
                            },
                        }
                    },
                )
                if c_resp.status_code == 200:
                    comment_id = c_resp.json().get("id", "")
            except Exception:
                pass

            return video_id, comment_id

    def _upload_facebook_reel_and_comment(
        self, pkg: PublishVideoPackageItem, fb_creds: Dict[str, Any]
    ) -> Tuple[str, str]:
        page_id = str(fb_creds.get("page_id") or "").strip()
        page_token = str(fb_creds.get("page_access_token") or "").strip()
        if not (page_id and page_token):
            raise RuntimeError("Missing Facebook Fanpage credentials in config/platform_credentials.json")

        vpath = MacOSLegacyRuntime.resolve_portable_video_path(pkg.video_path, pkg.script_id)
        file_size = vpath.stat().st_size

        ca_bundle = MacOSLegacyRuntime.resolve_ca_bundle_path()
        with httpx.Client(timeout=120.0, verify=ca_bundle or True) as client:
            start_resp = client.post(
                f"https://graph.facebook.com/v21.0/{page_id}/video_reels",
                data={"upload_phase": "start", "access_token": page_token},
            )
            start_resp.raise_for_status()
            start_data = start_resp.json()
            video_id = start_data["video_id"]
            upload_url = start_data["upload_url"]

            with vpath.open("rb") as vf:
                up_resp = client.post(
                    upload_url,
                    headers={
                        "Authorization": f"OAuth {page_token}",
                        "offset": "0",
                        "file_size": str(file_size),
                    },
                    content=vf.read(),
                    timeout=300.0,
                )
            up_resp.raise_for_status()

            fin_resp = client.post(
                f"https://graph.facebook.com/v21.0/{page_id}/video_reels",
                data={
                    "access_token": page_token,
                    "video_id": video_id,
                    "upload_phase": "finish",
                    "video_state": "PUBLISHED",
                    "description": pkg.facebook_reels_caption_full_vi,
                },
            )
            fin_resp.raise_for_status()

            # Upload uniform 9:16 SKU cover thumbnail for Facebook Reels (`POST /{video_id}/thumbnails`)
            cover_path = vpath.with_name(f"{vpath.stem}_cover.jpg")
            if cover_path.exists() and cover_path.stat().st_size > 10_000:
                try:
                    th_resp = client.post(
                        f"https://graph.facebook.com/v21.0/{video_id}/thumbnails",
                        data={"access_token": page_token, "is_preferred": "true"},
                        files={"source": (cover_path.name, cover_path.read_bytes(), "image/jpeg")},
                        timeout=60.0,
                    )
                    if th_resp.status_code == 200:
                        print(f"[M5 FACEBOOK] Đã áp ảnh bìa SKU chuẩn 9:16 ({cover_path.name}) thành công!", flush=True)
                    else:
                        print(
                            f"[M5 FACEBOOK] Đặt thumbnail Reels chưa áp được (HTTP {th_resp.status_code}): {th_resp.text[:160]}",
                            flush=True,
                        )
                except Exception as th_exc:
                    print(f"[M5 FACEBOOK] Bỏ qua đặt thumbnail Reels (không ảnh hưởng video): {th_exc}", flush=True)

            # Human delay before posting external-link comment (Anti-Spam Guard)
            delay_s = HumanBehaviorGuard.sample_post_upload_comment_delay_sec()
            print(f"[M5 HUMAN GUARD] Chờ tự nhiên {delay_s}s trước khi thả Comment đầu tiên trên Facebook Reels...", flush=True)
            time.sleep(delay_s)

            comment_id = ""
            try:
                c_resp = client.post(
                    f"https://graph.facebook.com/v21.0/{video_id}/comments",
                    data={
                        "access_token": page_token,
                        "message": pkg.pinned_comment_vi,
                    },
                )
                if c_resp.status_code == 200:
                    comment_id = c_resp.json().get("id", "")
            except Exception:
                pass

            return video_id, comment_id

    def run(
        self,
        live_upload: bool = False,
        force_now: bool = False,
        target_script_id: Optional[str] = None,
        mark_published_script_id: Optional[str] = None,
        mark_platform: str = "facebook_reels,youtube_shorts",
    ) -> M5PublishReport:
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        run_id = f"m5_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"

        if mark_published_script_id:
            platforms = [p.strip() for p in mark_platform.split(",") if p.strip()]
            entry = None
            for plat in platforms:
                entry = self.video_registry.mark_video_published(
                    mark_published_script_id,
                    platform=plat,
                    required_platforms=platforms,
                )
            status_after = entry.status if entry else "NOT_FOUND"
            if status_after == "PUBLISHED":
                self._purge_completed_video_everywhere(
                    mark_published_script_id,
                    (entry.video_path or "") if entry else "",
                )
            print(
                f"[M5] Đã đánh dấu `{mark_published_script_id}` -> status={status_after} (platforms={platforms})",
                flush=True,
            )
            report = M5PublishReport(
                m5_run_id=run_id,
                generated_at=now_iso,
                mode="MARK_PUBLISHED",
                total_candidates=1,
                validated_ready_count=1 if entry else 0,
                published_count=1 if entry else 0,
                results=[
                    M5PublishItemResult(
                        script_id=mark_published_script_id,
                        mode="MARK_PUBLISHED",
                        validation_passed=bool(entry),
                        youtube_status="MARKED_PUBLISHED" if "youtube_shorts" in platforms else "SKIPPED",
                        facebook_reels_status="MARKED_PUBLISHED" if "facebook_reels" in platforms else "SKIPPED",
                        registry_status_after=status_after,
                    )
                ],
            )
            atomic_write_text(self.report_path, report.model_dump_json(indent=2))
            return report

        if not self.manifest_path.exists():
            raise FileNotFoundError(
                f"M5 input contract not found: {self.manifest_path}. Run `python3 -m pipeline.publish_packager` first."
            )

        manifest = PublishManifestOutput.model_validate_json(
            self.manifest_path.read_text(encoding="utf-8")
        )
        creds = self._load_credentials()
        yt_creds = creds.get("youtube") or {}
        fb_creds = creds.get("facebook_reels") or {}
        has_yt_creds = bool(yt_creds.get("client_id") and yt_creds.get("refresh_token"))
        has_fb_creds = bool(fb_creds.get("page_id") and fb_creds.get("page_access_token"))

        required_platforms: List[str] = []
        if has_yt_creds:
            required_platforms.append("youtube_shorts")
        if has_fb_creds:
            required_platforms.append("facebook_reels")

        can_pub_now, pub_gate_reason, wait_pub_sec = self.can_publish_human_now(force_now=force_now)
        results: List[M5PublishItemResult] = []
        validated_count = 0
        published_count = 0
        next_allowed_str = str(self._load_m5_state().get("next_publish_allowed_at") or "")

        for pkg in manifest.packages:
            if target_script_id and pkg.script_id != target_script_id:
                continue

            v_check = self.validate_and_stage_package(pkg)
            if v_check.passed:
                validated_count += 1

            print(
                f"[M5 SEO GATE] {pkg.script_id}: passed={v_check.passed} | "
                f"title={pkg.youtube_shorts_title_vi!r} ({len(pkg.youtube_shorts_title_vi)}c) | "
                f"tags={len(pkg.youtube_video_tags)} | chapters={len(pkg.chapter_timestamps_vi)}",
                flush=True,
            )

            if not live_upload:
                results.append(
                    M5PublishItemResult(
                        script_id=pkg.script_id,
                        mode="STAGE_ONLY",
                        validation_passed=v_check.passed,
                    )
                )
                continue

            can_pkg_now, pkg_gate_reason, pkg_wait_sec = self.can_publish_human_now(
                force_now=force_now, script_id=pkg.script_id
            )
            # Anti-Spam Human Guard: Max 1-2 videos/day hard cap, max 1 video uploaded per invocation, and only inside active human window!
            if published_count >= 1 or not can_pkg_now:
                defer_note = pkg_gate_reason if not can_pkg_now else "MAX_1_VIDEO_PER_HUMAN_SESSION"
                results.append(
                    M5PublishItemResult(
                        script_id=pkg.script_id,
                        mode="HUMAN_PACING_DEFERRED",
                        validation_passed=v_check.passed,
                        youtube_status=f"DEFERRED ({defer_note}, wait={pkg_wait_sec}s)",
                        facebook_reels_status=f"DEFERRED ({defer_note}, wait={pkg_wait_sec}s)",
                    )
                )
                continue

            if not v_check.passed:
                results.append(
                    M5PublishItemResult(
                        script_id=pkg.script_id,
                        mode="LIVE_UPLOAD",
                        validation_passed=False,
                        youtube_status="BLOCKED_BY_SEO_GATE",
                        facebook_reels_status="BLOCKED_BY_SEO_GATE",
                    )
                )
                continue

            # Per-platform crash-resume lookup from video_registry.json
            reg_entry = self.video_registry.get_entry(pkg.script_id)
            already_done_platforms = set(reg_entry.published_platforms if reg_entry else [])

            item_res = M5PublishItemResult(
                script_id=pkg.script_id,
                mode="LIVE_UPLOAD",
                validation_passed=True,
            )
            uploaded_platforms_now: List[str] = []

            if has_yt_creds:
                if "youtube_shorts" in already_done_platforms:
                    item_res.youtube_status = "ALREADY_PUBLISHED_SKIPPED_ON_RESUME"
                else:
                    try:
                        vid_id, cid = self._upload_youtube_short_and_comment(pkg, yt_creds)
                        item_res.youtube_status = "PUBLISHED"
                        item_res.youtube_video_id = vid_id
                        item_res.youtube_comment_id = cid
                        updated = self.video_registry.mark_video_published(
                            pkg.script_id,
                            platform="youtube_shorts",
                            required_platforms=required_platforms,
                        )
                        if updated:
                            item_res.registry_status_after = updated.status
                        uploaded_platforms_now.append("youtube_shorts")
                    except Exception as exc:
                        item_res.youtube_status = f"ERROR: {exc}"
            else:
                item_res.youtube_status = "STAGED_AWAITING_OAUTH_CREDENTIALS"

            if has_fb_creds:
                if "facebook_reels" in already_done_platforms:
                    item_res.facebook_reels_status = "ALREADY_PUBLISHED_SKIPPED_ON_RESUME"
                else:
                    if uploaded_platforms_now:
                        stagger_s = HumanBehaviorGuard.sample_cross_platform_stagger_sec()
                        print(f"[M5 HUMAN GUARD] Lệch pha tự nhiên {stagger_s}s trước khi đăng sang Facebook Reels...", flush=True)
                        time.sleep(stagger_s)
                    try:
                        reel_id, cid = self._upload_facebook_reel_and_comment(pkg, fb_creds)
                        item_res.facebook_reels_status = "PUBLISHED"
                        item_res.facebook_reel_id = reel_id
                        item_res.facebook_comment_id = cid
                        updated = self.video_registry.mark_video_published(
                            pkg.script_id,
                            platform="facebook_reels",
                            required_platforms=required_platforms,
                        )
                        if updated:
                            item_res.registry_status_after = updated.status
                        uploaded_platforms_now.append("facebook_reels")
                    except Exception as exc:
                        item_res.facebook_reels_status = f"ERROR: {exc}"
            else:
                item_res.facebook_reels_status = "STAGED_AWAITING_PAGE_TOKEN"

            if uploaded_platforms_now:
                published_count += 1
                next_allowed_str = self._record_human_publish_event(pkg.script_id, uploaded_platforms_now)

            if item_res.registry_status_after == "PUBLISHED":
                self._purge_completed_video_everywhere(pkg.script_id, pkg.video_path)

            if uploaded_platforms_now or "ERROR" in item_res.youtube_status or "ERROR" in item_res.facebook_reels_status:
                TelegramNotifier().notify_publish_result(
                    script_id=pkg.script_id,
                    title_vi=pkg.youtube_shorts_title_vi,
                    yt_status=item_res.youtube_status,
                    yt_video_id=item_res.youtube_video_id,
                    fb_status=item_res.facebook_reels_status,
                    fb_reel_id=item_res.facebook_reel_id,
                    short_links=[s.short_link for s in pkg.skus if s.short_link],
                )

            results.append(item_res)

        can_after, _, wait_after = self.can_publish_human_now(force_now=force_now)
        if not can_after and wait_after > 0:
            effective_next_allowed = (
                dt.datetime.now().astimezone() + dt.timedelta(seconds=wait_after)
            ).isoformat(timespec="seconds")
        else:
            effective_next_allowed = ""

        report = M5PublishReport(
            m5_run_id=run_id,
            generated_at=now_iso,
            mode="LIVE_UPLOAD" if live_upload else "STAGE_ONLY",
            active_hours_ok=can_after,
            next_human_publish_allowed_at=effective_next_allowed,
            total_candidates=len(results),
            validated_ready_count=validated_count,
            published_count=published_count,
            results=results,
        )
        atomic_write_text(self.report_path, report.model_dump_json(indent=2))
        print(
            f"[M5] Đã kiểm định & đóng gói API Payload cho {validated_count}/{len(results)} video "
            f"tại: {self.payloads_dir}",
            flush=True,
        )
        return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Module M5: Decoupled Multi-Platform SEO Publisher & Human Anti-Spam Guard."
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=DEFAULT_MANIFEST_PATH,
        help="Input publish_manifest.json path (default: output/publish_pack/publish_manifest.json)",
    )
    parser.add_argument(
        "--live",
        action="store_true",
        default=False,
        help="Perform live upload (max 1 video per run, inside active human hours 08:15-22:15).",
    )
    parser.add_argument(
        "--force-now",
        action="store_true",
        default=False,
        help="Bypass active-hour / human-cooldown gate for manual one-off testing.",
    )
    parser.add_argument(
        "--script-id",
        type=str,
        default=None,
        help="Optional single script_id to stage or upload.",
    )
    parser.add_argument(
        "--mark-published",
        type=str,
        default=None,
        help="Mark a specific script_id as PUBLISHED in video_registry.json after manual upload.",
    )
    parser.add_argument(
        "--platform",
        type=str,
        default="facebook_reels,youtube_shorts",
        help="Platforms for --mark-published (default: facebook_reels,youtube_shorts).",
    )
    args = parser.parse_args()

    engine = M5PublisherEngine(manifest_path=args.manifest)
    engine.run(
        live_upload=args.live,
        force_now=args.force_now,
        target_script_id=args.script_id,
        mark_published_script_id=args.mark_published,
        mark_platform=args.platform,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
