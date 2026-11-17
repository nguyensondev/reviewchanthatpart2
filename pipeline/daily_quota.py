"""
Persistent Daily Quota & 2-Hour Render Cooldown Manager for Modules M3 & M4 (`pipeline/daily_quota.py`).

Enforces strict daily production caps and render pacing:
  - MAX_FINAL_SCRIPTS_PER_DAY   = 4     (Module M3 final QA/QC approvals)
  - MAX_VIDEOS_PER_DAY          = 3     (Module M4 rendered 9:16 videos)
  - MIN_RENDER_COOLDOWN_SECONDS = 7200  (Minimum 2-hour rest after each rendered video in M4)

Once a cap is reached for today's local calendar date, the pipeline transitions into
`QUOTA_REACHED_SLEEP_UNTIL_TOMORROW` standby until 00:05:00 of the next day.
Between individual video renders on the same day, M4 transitions into `COOLDOWN_RESTING_2H`
for at least 7,200 seconds (2 hours).
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.core_resilience import (
    MAX_QUOTA_HISTORY_DAYS,
    HumanBehaviorGuard,
    atomic_write_json,
    safe_load_json,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_QUOTA_STATE_PATH = PROJECT_ROOT / "data" / "daily_quota_state.json"

MAX_FINAL_SCRIPTS_PER_DAY = 4
MAX_VIDEOS_PER_DAY = 3
MIN_RENDER_COOLDOWN_SECONDS = 7200  # 2 hours rest after each rendered video


class DailyQuotaManager:
    """Manages daily script & video quotas + 2-hour render cooldown in `data/daily_quota_state.json`."""

    def __init__(
        self,
        state_path: Path = DEFAULT_QUOTA_STATE_PATH,
        cooldown_seconds: int = MIN_RENDER_COOLDOWN_SECONDS,
    ) -> None:
        self.state_path = state_path
        self.cooldown_seconds = cooldown_seconds

    def today_key(self) -> str:
        """Return current local calendar date string (YYYY-MM-DD)."""
        return dt.datetime.now().strftime("%Y-%m-%d")

    def _load_state(self) -> Dict[str, Any]:
        data = safe_load_json(self.state_path, lambda: {"days": {}})
        if isinstance(data, dict) and isinstance(data.get("days"), dict):
            return data
        return {"days": {}}

    def _save_state(self, state: Dict[str, Any]) -> None:
        days = state.get("days")
        if isinstance(days, dict) and len(days) > MAX_QUOTA_HISTORY_DAYS:
            sorted_keys = sorted(days.keys())
            for old_k in sorted_keys[:-MAX_QUOTA_HISTORY_DAYS]:
                days.pop(old_k, None)
        atomic_write_json(self.state_path, state)

    def _ensure_day_bucket(self, state: Dict[str, Any], day_key: str) -> Dict[str, Any]:
        days = state.setdefault("days", {})
        bucket = days.get(day_key)
        if not isinstance(bucket, dict):
            bucket = {
                "approved_script_ids": [],
                "rendered_video_script_ids": [],
                "rendered_voices_today": [],
                "render_events": [],
                "last_render_started_at": "",
                "last_render_finished_at": "",
                "last_render_duration_sec": 0.0,
                "next_render_allowed_at": "",
                "updated_at": "",
            }
            days[day_key] = bucket
        bucket.setdefault("approved_script_ids", [])
        bucket.setdefault("rendered_video_script_ids", [])
        bucket.setdefault("rendered_voices_today", [])
        bucket.setdefault("render_events", [])
        bucket.setdefault("last_render_started_at", "")
        bucket.setdefault("last_render_finished_at", "")
        bucket.setdefault("last_render_duration_sec", 0.0)
        bucket.setdefault("next_render_allowed_at", "")
        bucket.setdefault("updated_at", "")
        return bucket

    def reset_today(self) -> None:
        """Clear today's quota counters and cooldown timers (used in verification suites)."""
        state = self._load_state()
        day = self.today_key()
        state.setdefault("days", {})[day] = {
            "approved_script_ids": [],
            "rendered_video_script_ids": [],
            "rendered_voices_today": [],
            "render_events": [],
            "last_render_started_at": "",
            "last_render_finished_at": "",
            "last_render_duration_sec": 0.0,
            "next_render_allowed_at": "",
            "updated_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        self._save_state(state)

    # --- Script Quota (M3: max 4/day) ---

    def get_script_quota_status(self) -> Tuple[int, int, List[str]]:
        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        ids = list(bucket["approved_script_ids"])
        used = len(ids)
        remaining = max(0, MAX_FINAL_SCRIPTS_PER_DAY - used)
        return used, remaining, ids

    def can_approve_script(self, script_id: str) -> bool:
        used, _, ids = self.get_script_quota_status()
        if script_id in ids:
            return True
        return used < MAX_FINAL_SCRIPTS_PER_DAY

    def record_approved_script(self, script_id: str) -> None:
        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        if script_id not in bucket["approved_script_ids"]:
            bucket["approved_script_ids"].append(script_id)
        bucket["updated_at"] = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        self._save_state(state)

    # --- Video Quota & 2-Hour Cooldown (M4: max 3/day, >=2h rest after each render) ---

    def get_video_quota_status(self) -> Tuple[int, int, List[str]]:
        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        ids = list(bucket["rendered_video_script_ids"])
        used = len(ids)
        remaining = max(0, MAX_VIDEOS_PER_DAY - used)
        return used, remaining, ids

    def get_rendered_voices_today(self) -> List[str]:
        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        return list(bucket.get("rendered_voices_today", []))

    def get_cooldown_status(self, now: Optional[dt.datetime] = None) -> Dict[str, Any]:
        """Return current 2-hour cooldown timer metadata."""
        now_dt = now or dt.datetime.now().astimezone()
        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        next_allowed_str = str(bucket.get("next_render_allowed_at") or "")
        seconds_to_wait = 0
        is_resting = False
        if next_allowed_str:
            try:
                next_dt = dt.datetime.fromisoformat(next_allowed_str)
                if next_dt.tzinfo is None:
                    next_dt = next_dt.replace(tzinfo=now_dt.tzinfo)
                diff = int((next_dt - now_dt).total_seconds())
                if diff > 0:
                    seconds_to_wait = diff
                    is_resting = True
            except Exception:
                pass
        return {
            "is_resting": is_resting,
            "seconds_until_next_render": seconds_to_wait,
            "next_render_allowed_at": next_allowed_str,
            "last_render_started_at": str(bucket.get("last_render_started_at") or ""),
            "last_render_finished_at": str(bucket.get("last_render_finished_at") or ""),
            "last_render_duration_sec": float(bucket.get("last_render_duration_sec") or 0.0),
        }

    def can_render_video(
        self,
        script_id: str,
        allow_rerender_same_id: bool = True,
        ignore_cooldown: bool = True,
    ) -> bool:
        allowed, _, _ = self.can_render_video_now(
            script_id=script_id,
            allow_rerender_same_id=allow_rerender_same_id,
            ignore_cooldown=ignore_cooldown,
        )
        return allowed

    def can_render_video_now(
        self,
        script_id: Optional[str] = None,
        allow_rerender_same_id: bool = True,
        ignore_cooldown: bool = False,
        now: Optional[dt.datetime] = None,
    ) -> Tuple[bool, str, int]:
        """
        Check both the 3-video daily cap and the 2-hour post-render cooldown.
        Returns `(allowed, status_code, seconds_to_wait)`:
          - status_code in ("READY", "COOLDOWN_RESTING_2H", "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW")
        """
        used, _, ids = self.get_video_quota_status()
        is_same_rerender = bool(allow_rerender_same_id and script_id and script_id in ids)

        if not is_same_rerender and used >= MAX_VIDEOS_PER_DAY:
            sleep_info = self.get_sleep_until_tomorrow_info()
            return False, "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW", int(sleep_info["seconds_until_reset"])

        if not ignore_cooldown:
            cd = self.get_cooldown_status(now=now)
            if cd["is_resting"]:
                return False, "COOLDOWN_RESTING_2H", int(cd["seconds_until_next_render"])

        return True, "READY", 0

    def record_render_started(self, script_id: str, now: Optional[dt.datetime] = None) -> str:
        now_dt = now or dt.datetime.now().astimezone()
        now_iso = now_dt.isoformat(timespec="seconds")
        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        bucket["last_render_started_at"] = now_iso
        bucket["updated_at"] = now_iso
        self._save_state(state)
        return now_iso

    def record_rendered_video(
        self,
        script_id: str,
        render_duration_sec: float = 0.0,
        voice_name: str = "",
        started_at: Optional[str] = None,
        now: Optional[dt.datetime] = None,
        use_human_jitter: bool = True,
    ) -> Dict[str, Any]:
        now_dt = now or dt.datetime.now().astimezone()
        now_iso = now_dt.isoformat(timespec="seconds")
        effective_cd = (
            HumanBehaviorGuard.sample_human_cooldown_seconds(min_floor_sec=self.cooldown_seconds)
            if use_human_jitter
            else self.cooldown_seconds
        )
        next_allowed_dt = now_dt + dt.timedelta(seconds=effective_cd)
        next_allowed_iso = next_allowed_dt.isoformat(timespec="seconds")

        state = self._load_state()
        bucket = self._ensure_day_bucket(state, self.today_key())
        if script_id not in bucket["rendered_video_script_ids"]:
            bucket["rendered_video_script_ids"].append(script_id)
        if voice_name:
            bucket.setdefault("rendered_voices_today", []).append(voice_name)

        start_iso = started_at or bucket.get("last_render_started_at") or now_iso
        bucket["last_render_started_at"] = start_iso
        bucket["last_render_finished_at"] = now_iso
        bucket["last_render_duration_sec"] = round(float(render_duration_sec), 2)
        bucket["next_render_allowed_at"] = next_allowed_iso
        bucket["updated_at"] = now_iso

        event = {
            "script_id": script_id,
            "voice_name": voice_name,
            "render_started_at": start_iso,
            "render_finished_at": now_iso,
            "render_duration_sec": round(float(render_duration_sec), 2),
            "next_render_allowed_at": next_allowed_iso,
        }
        bucket.setdefault("render_events", []).append(event)
        self._save_state(state)
        return event

    def get_sleep_until_tomorrow_info(self) -> Dict[str, Any]:
        now = dt.datetime.now().astimezone()
        tomorrow_date = (now + dt.timedelta(days=1)).date()
        next_reset = dt.datetime.combine(
            tomorrow_date, dt.time(hour=0, minute=5, second=0), tzinfo=now.tzinfo
        )
        seconds_left = max(60, int((next_reset - now).total_seconds()))
        return {
            "next_reset_at": next_reset.isoformat(timespec="seconds"),
            "seconds_until_reset": seconds_left,
        }
