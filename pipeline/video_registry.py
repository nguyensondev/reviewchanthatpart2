"""
Persistent Video & Surplus Script Queue Registry (`pipeline/video_registry.py`).

Enforces strict separation between production states so downstream publishing modules
never confuse a rendered local MP4 file with an already-published post:
  - `SCRIPT_APPROVED` : Script passed M3 QA/QC and is queued for M4 rendering (surplus scripts
                        beyond M4's 3/day cap remain queued for subsequent runs, up to 7 days TTL).
  - `RENDERED`        : M4 has rendered the MP4 file on disk (`output/videos/<script_id>.mp4`),
                        NOT yet published to social platforms.
  - `PUBLISHED`       : Reserved strictly for the future publishing module after actual upload.
                        M4 MUST NEVER set status to `PUBLISHED`.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from pipeline.core_resilience import atomic_write_json, safe_load_json
from pipeline.schemas_m3 import AuditedScriptItem

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_VIDEO_REGISTRY_PATH = PROJECT_ROOT / "data" / "video_registry.json"
SURPLUS_SCRIPT_TTL_DAYS = 14

VideoLifecycleStatus = Literal["SCRIPT_APPROVED", "RENDERED", "PUBLISHED"]


class VideoRegistryEntry(BaseModel):
    script_id: str
    script_format: str
    domain: str
    category_id: str
    narrative_angle: str
    title_vi: str
    hook_title_short_vi: str = ""
    primary_sku_ids: List[str] = Field(default_factory=list)
    normalized_sku_keys: List[str] = Field(default_factory=list)
    status: VideoLifecycleStatus = "SCRIPT_APPROVED"
    approved_at: str
    m3_run_id: str = ""
    rendered_at: Optional[str] = None
    render_duration_sec: float = 0.0
    video_path: Optional[str] = None
    voice_name: Optional[str] = None
    voice_gender: Optional[str] = None
    bgm_track: Optional[str] = None
    published_at: Optional[str] = None
    published_platforms: List[str] = Field(default_factory=list)
    script_payload: Optional[Dict[str, Any]] = None


class VideoRegistryManager:
    """Manages `data/video_registry.json` for M3 script queueing, M4 rendering, and future publishing."""

    def __init__(self, registry_path: Path = DEFAULT_VIDEO_REGISTRY_PATH) -> None:
        self.registry_path = registry_path

    def _load_raw(self) -> Dict[str, Any]:
        data = safe_load_json(self.registry_path, lambda: {"version": "1.0.0", "items": {}})
        if isinstance(data, dict) and isinstance(data.get("items"), dict):
            return data
        return {"version": "1.0.0", "items": {}}

    def _save_raw(self, data: Dict[str, Any]) -> None:
        atomic_write_json(self.registry_path, data)

    def register_approved_scripts(
        self,
        scripts: List[AuditedScriptItem],
        m3_run_id: str,
        normalized_keys_by_script: Optional[Dict[str, List[str]]] = None,
    ) -> int:
        """
        Register M3-approved scripts as `SCRIPT_APPROVED` if not already `RENDERED` or `PUBLISHED`.
        Preserves existing `RENDERED` / `PUBLISHED` statuses unless explicitly re-registered.
        """
        raw = self._load_raw()
        items = raw.setdefault("items", {})
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        added_or_updated = 0

        for s in scripts:
            if s.qaqc_status not in ("APPROVED", "APPROVED_WITH_AUTO_PATCH"):
                continue
            existing = items.get(s.script_id)
            current_status: VideoLifecycleStatus = "SCRIPT_APPROVED"
            if isinstance(existing, dict) and existing.get("status") in ("RENDERED", "PUBLISHED"):
                current_status = existing["status"]

            norm_keys = (
                (normalized_keys_by_script or {}).get(s.script_id)
                or (existing.get("normalized_sku_keys", []) if isinstance(existing, dict) else [])
            )
            entry = VideoRegistryEntry(
                script_id=s.script_id,
                script_format=s.script_format,
                domain=s.domain,
                category_id=s.category_id,
                narrative_angle=s.narrative_angle,
                title_vi=s.title_vi,
                hook_title_short_vi=getattr(s, "hook_title_short_vi", "") or "",
                primary_sku_ids=list(s.primary_sku_ids),
                normalized_sku_keys=list(norm_keys),
                status=current_status,
                approved_at=(
                    existing.get("approved_at", now_iso)
                    if isinstance(existing, dict) and existing.get("approved_at")
                    else now_iso
                ),
                m3_run_id=m3_run_id,
                rendered_at=existing.get("rendered_at") if isinstance(existing, dict) else None,
                render_duration_sec=float(existing.get("render_duration_sec", 0.0)) if isinstance(existing, dict) else 0.0,
                video_path=existing.get("video_path") if isinstance(existing, dict) else None,
                voice_name=existing.get("voice_name") if isinstance(existing, dict) else None,
                voice_gender=existing.get("voice_gender") if isinstance(existing, dict) else None,
                bgm_track=existing.get("bgm_track") if isinstance(existing, dict) else None,
                published_at=existing.get("published_at") if isinstance(existing, dict) else None,
                published_platforms=existing.get("published_platforms", []) if isinstance(existing, dict) else [],
                script_payload=s.model_dump(mode="json"),
            )
            items[s.script_id] = entry.model_dump(mode="json")
            added_or_updated += 1

        self._save_raw(raw)
        return added_or_updated

    def mark_video_rendered(
        self,
        script_id: str,
        video_path: str,
        render_duration_sec: float,
        voice_name: str,
        voice_gender: str,
        bgm_track: str,
        script_obj: Optional[AuditedScriptItem] = None,
        normalized_sku_keys: Optional[List[str]] = None,
    ) -> VideoRegistryEntry:
        """
        Mark a script as `RENDERED` (video produced on disk, NOT published yet).
        Never sets `PUBLISHED`.
        """
        raw = self._load_raw()
        items = raw.setdefault("items", {})
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        existing = items.get(script_id) or {}

        if script_obj is not None and not existing:
            entry = VideoRegistryEntry(
                script_id=script_obj.script_id,
                script_format=script_obj.script_format,
                domain=script_obj.domain,
                category_id=script_obj.category_id,
                narrative_angle=script_obj.narrative_angle,
                title_vi=script_obj.title_vi,
                hook_title_short_vi=getattr(script_obj, "hook_title_short_vi", "") or "",
                primary_sku_ids=list(script_obj.primary_sku_ids),
                normalized_sku_keys=list(normalized_sku_keys or []),
                status="RENDERED",
                approved_at=now_iso,
                rendered_at=now_iso,
                render_duration_sec=round(float(render_duration_sec), 2),
                video_path=video_path,
                voice_name=voice_name,
                voice_gender=voice_gender,
                bgm_track=bgm_track,
                script_payload=script_obj.model_dump(mode="json"),
            )
        else:
            existing["status"] = "RENDERED"
            existing["rendered_at"] = now_iso
            existing["render_duration_sec"] = round(float(render_duration_sec), 2)
            existing["video_path"] = video_path
            existing["voice_name"] = voice_name
            existing["voice_gender"] = voice_gender
            existing["bgm_track"] = bgm_track
            if normalized_sku_keys is not None:
                existing["normalized_sku_keys"] = list(normalized_sku_keys)
            if script_obj is not None:
                existing["script_payload"] = script_obj.model_dump(mode="json")
                existing.setdefault("script_id", script_obj.script_id)
                existing.setdefault("script_format", script_obj.script_format)
                existing.setdefault("domain", script_obj.domain)
                existing.setdefault("category_id", script_obj.category_id)
                existing.setdefault("narrative_angle", script_obj.narrative_angle)
                existing.setdefault("title_vi", script_obj.title_vi)
                existing.setdefault("primary_sku_ids", list(script_obj.primary_sku_ids))
                existing.setdefault("approved_at", now_iso)
            else:
                existing.setdefault("script_id", script_id)
                existing.setdefault("script_format", "SINGLE_SKU_DEEP_DIVE")
                existing.setdefault("domain", "TECH_DESK")
                existing.setdefault("category_id", "general")
                existing.setdefault("narrative_angle", "PAIN_POINT_REALITY_CHECK")
                existing.setdefault("title_vi", script_id)
                existing.setdefault("approved_at", now_iso)
            entry = VideoRegistryEntry.model_validate(existing)

        items[script_id] = entry.model_dump(mode="json")
        self._save_raw(raw)
        return entry

    def mark_video_published(
        self,
        script_id: str,
        platform: str = "youtube_shorts",
        required_platforms: Optional[List[str]] = None,
    ) -> Optional[VideoRegistryEntry]:
        """
        Record platform publication for `script_id`. Supports partial crash-resume:
        if `required_platforms` has multiple targets (e.g. `['youtube_shorts', 'facebook_reels']`),
        `status` remains `RENDERED` until every required platform is in `published_platforms`.
        """
        raw = self._load_raw()
        items = raw.setdefault("items", {})
        existing = items.get(script_id)
        if not isinstance(existing, dict):
            return None
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        platforms = list(existing.get("published_platforms") or [])
        if platform and platform not in platforms:
            platforms.append(platform)
        existing["published_platforms"] = platforms
        existing["published_at"] = now_iso

        if required_platforms:
            if all(req in platforms for req in required_platforms):
                existing["status"] = "PUBLISHED"
            else:
                existing["status"] = "RENDERED"
        else:
            existing["status"] = "PUBLISHED"

        entry = VideoRegistryEntry.model_validate(existing)
        items[script_id] = entry.model_dump(mode="json")
        self._save_raw(raw)
        return entry

    def get_queued_approved_scripts(self, ttl_days: int = SURPLUS_SCRIPT_TTL_DAYS) -> List[AuditedScriptItem]:
        """
        Return all scripts currently in `SCRIPT_APPROVED` status within the freshness TTL window.
        """
        raw = self._load_raw()
        items = raw.get("items") or {}
        now_dt = dt.datetime.now().astimezone()
        out: List[AuditedScriptItem] = []

        for _, val in items.items():
            if not isinstance(val, dict) or val.get("status") != "SCRIPT_APPROVED":
                continue
            approved_str = val.get("approved_at") or ""
            if approved_str:
                try:
                    app_dt = dt.datetime.fromisoformat(approved_str)
                    if app_dt.tzinfo is None:
                        app_dt = app_dt.replace(tzinfo=now_dt.tzinfo)
                    if (now_dt - app_dt).days > ttl_days:
                        continue
                except Exception:
                    pass
            payload = val.get("script_payload")
            if isinstance(payload, dict):
                try:
                    out.append(AuditedScriptItem.model_validate(payload))
                except Exception:
                    pass
        return out

    def get_rendered_unposted_videos(self) -> List[VideoRegistryEntry]:
        """Return all videos in `RENDERED` state (ready for publishing module, not yet `PUBLISHED`)."""
        raw = self._load_raw()
        items = raw.get("items") or {}
        out: List[VideoRegistryEntry] = []
        for _, val in items.items():
            if isinstance(val, dict) and val.get("status") == "RENDERED":
                try:
                    out.append(VideoRegistryEntry.model_validate(val))
                except Exception:
                    pass
        return out

    def get_entry(self, script_id: str) -> Optional[VideoRegistryEntry]:
        raw = self._load_raw()
        val = (raw.get("items") or {}).get(script_id)
        if isinstance(val, dict):
            return VideoRegistryEntry.model_validate(val)
        return None
