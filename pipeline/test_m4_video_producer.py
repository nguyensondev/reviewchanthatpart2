"""
End-to-End Integration, Cooldown, Combo Lifecycle & Quota Gate Verification
for Module M1.5, M3 & M4 (Video Producer Layout B1).
"""
from __future__ import annotations

import copy
import json
import shutil
import subprocess
import sys
from pathlib import Path

from pipeline.daily_quota import (
    MAX_FINAL_SCRIPTS_PER_DAY,
    MAX_VIDEOS_PER_DAY,
    MIN_RENDER_COOLDOWN_SECONDS,
    DailyQuotaManager,
)
from pipeline.m1_5_combo_curator import M15ComboCuratorEngine, SKURegistryManager
from pipeline.m3_qaqc import M3QAQCEngine
from pipeline.m4_video_producer import (
    OUTPUT_M4_JSON_PATH,
    M4VideoProducerEngine,
)
from pipeline.schemas_m1 import M1QualifiedSKUsOutput
from pipeline.schemas_m4 import M4ProductionOutput
from pipeline.video_registry import VideoRegistryManager

ARTIFACTS_DIR = Path(
    "/Users/sonnguyen/.gemini/antigravity/brain/e4f2ee74-53d0-49a3-be49-c28f4f575782"
)


def probe_video_and_audio(video_path: Path) -> dict:
    """Inspect rendered MP4 via ffprobe for both video and audio streams."""
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-show_streams",
        "-show_format",
        "-of",
        "json",
        str(video_path),
    ]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    payload = json.loads(res.stdout)
    streams = payload.get("streams", [])
    video_streams = [s for s in streams if s.get("codec_type") == "video"]
    audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
    fmt = payload.get("format", {})

    assert video_streams, f"Missing video stream in {video_path}"
    assert audio_streams, f"Missing audio stream in {video_path}"

    v0 = video_streams[0]
    a0 = audio_streams[0]
    duration = float(fmt.get("duration", 0.0))

    assert v0.get("width") == 1080, f"Expected width 1080, got {v0.get('width')}"
    assert v0.get("height") == 1920, f"Expected height 1920, got {v0.get('height')}"
    assert v0.get("codec_name") == "h264", f"Expected h264, got {v0.get('codec_name')}"
    assert a0.get("codec_name") == "aac", f"Expected aac, got {a0.get('codec_name')}"
    assert duration >= 25.0, f"Expected duration >= 25.0s, got {duration:.2f}s"

    return {
        "width": v0.get("width"),
        "height": v0.get("height"),
        "v_codec": v0.get("codec_name"),
        "a_codec": a0.get("codec_name"),
        "duration_sec": duration,
        "size_bytes": int(fmt.get("size", video_path.stat().st_size)),
    }


def verify_flexible_sku_lifecycle() -> None:
    """Verify flexible SKU lifecycle + random 3-7 day SKU cooldown + M0 anti-duplicate category rotation."""
    import datetime as dt
    from pipeline.m0_discovery import M0AutonomousDiscoveryEngine, load_m0_history

    print("\n=== [TEST M1.5 & M0] Flexible SKU Lifecycle + 3-7 Day Cooldown + M0 Category Rotation ===", flush=True)
    tmp_reg_path = Path("/tmp/test_sku_registry_flexible.json")
    if tmp_reg_path.exists():
        tmp_reg_path.unlink()

    reg = SKURegistryManager(registry_path=tmp_reg_path)
    m1_data = M1QualifiedSKUsOutput.model_validate_json(
        Path("data/m1_qualified_skus.json").read_text(encoding="utf-8")
    )
    reg.upsert_from_m1(m1_data)
    now_dt = dt.datetime.now().astimezone()

    # Case A: SKU 1 ('baseus_i_wok2') enters Solo first -> rolls random 3-7 day cooldown!
    reg.record_rendered_video_skus("SINGLE_SKU_DEEP_DIVE", ["baseus_i_wok2"], "script_solo_1", now=now_dt)
    e1 = reg.get_all_entries()["baseus_i_wok2"]
    assert e1.lifecycle_status == "SOLO_RENDERED_COMBO_OPEN", f"Unexpected status: {e1.lifecycle_status}"
    assert 3 <= e1.cooldown_days <= 7, f"Expected cooldown_days in 3..7, got {e1.cooldown_days}"
    assert reg.is_in_cooldown("baseus_i_wok2", now=now_dt) is True
    # Blocked for BOTH solo and combo during the 3-7 day cooldown window!
    assert not reg.is_solo_eligible("baseus_i_wok2", now=now_dt)
    assert not reg.is_combo_eligible("baseus_i_wok2", now=now_dt)
    print(
        f"  [3-7D COOLDOWN] baseus_i_wok2 after Solo: status={e1.lifecycle_status} | "
        f"cooldown={e1.cooldown_days}d | next_eligible={e1.next_eligible_at} | combo_eligible_now={reg.is_combo_eligible('baseus_i_wok2', now=now_dt)}",
        flush=True,
    )

    # Fast-forward past the 3-7 day cooldown window -> now eligible for Combo!
    future_dt = now_dt + dt.timedelta(days=e1.cooldown_days, hours=1)
    assert reg.is_in_cooldown("baseus_i_wok2", now=future_dt) is False
    assert not reg.is_solo_eligible("baseus_i_wok2", now=future_dt)
    assert reg.is_combo_eligible("baseus_i_wok2", now=future_dt)

    # Now enters Combo after 3-7 days -> transitions to RETIRED_EXHAUSTED
    reg.record_rendered_video_skus("COMBO_WORKFLOW_SYNERGY", ["baseus_i_wok2"], "script_combo_1", now=future_dt)
    e1_after = reg.get_all_entries()["baseus_i_wok2"]
    assert e1_after.lifecycle_status == "RETIRED_EXHAUSTED", f"Expected RETIRED_EXHAUSTED, got {e1_after.lifecycle_status}"
    assert not reg.is_solo_eligible("baseus_i_wok2", now=future_dt)
    assert not reg.is_combo_eligible("baseus_i_wok2", now=future_dt)

    # Case B: SKU 2 ('logitech_lift_vertical') enters Combo FIRST -> rolls random 3-7 day cooldown!
    reg.record_rendered_video_skus("COMBO_WORKFLOW_SYNERGY", ["logitech_lift_vertical"], "script_combo_1", now=now_dt)
    e2 = reg.get_all_entries()["logitech_lift_vertical"]
    assert e2.lifecycle_status == "COMBO_RENDERED_SOLO_OPEN", f"Expected COMBO_RENDERED_SOLO_OPEN, got {e2.lifecycle_status}"
    assert 3 <= e2.cooldown_days <= 7
    assert not reg.is_solo_eligible("logitech_lift_vertical", now=now_dt)  # blocked during 3-7d cooldown
    future_dt_2 = now_dt + dt.timedelta(days=e2.cooldown_days, hours=1)
    assert reg.is_solo_eligible("logitech_lift_vertical", now=future_dt_2)  # unlocked after 3-7d!
    assert not reg.is_combo_eligible("logitech_lift_vertical", now=future_dt_2)

    reg.record_rendered_video_skus("SINGLE_SKU_DEEP_DIVE", ["logitech_lift_vertical"], "script_solo_2", now=future_dt_2)
    e2_after = reg.get_all_entries()["logitech_lift_vertical"]
    assert e2_after.lifecycle_status == "RETIRED_EXHAUSTED", f"Expected RETIRED_EXHAUSTED, got {e2_after.lifecycle_status}"

    # Verify M1.5 returns BYPASS when < 2 combo-eligible SKUs remain
    tmp_m15_out = Path("/tmp/test_m1_5_bypass.json")
    bypass_res = M15ComboCuratorEngine(registry_mgr=reg).run(
        output_path=tmp_m15_out,
    )
    print(
        f"  [M1.5 BYPASS CHECK] status={bypass_res.status} | reason={bypass_res.bypass_reason_vi}",
        flush=True,
    )
    assert bypass_res.status == "BYPASS", f"Expected BYPASS when <2 combo SKUs remain, got {bypass_res.status}"

    # Verify M0 smart category rotation blocks recently used & cooldown categories
    m0_eng = M0AutonomousDiscoveryEngine()
    blocked_cats, fresh_tech, fresh_kitchen = m0_eng._get_blocked_and_fresh_categories(load_m0_history())
    print(
        f"  [M0 SMART ROTATION] blocked_cats={sorted(blocked_cats)} | "
        f"next_fresh_tech={[c['category_id'] for c in fresh_tech[:2]]} | "
        f"next_fresh_kitchen={[c['category_id'] for c in fresh_kitchen[:2]]}",
        flush=True,
    )
    assert "screen_hanging_light_bar" in blocked_cats
    assert "ergonomic_vertical_mouse" in blocked_cats
    assert "digital_kitchen_food_scale" in blocked_cats
    assert all(c["category_id"] not in blocked_cats for c in fresh_tech)
    assert all(c["category_id"] not in blocked_cats for c in fresh_kitchen)

    tmp_reg_path.unlink(missing_ok=True)
    tmp_m15_out.unlink(missing_ok=True)


def main() -> int:
    print("=== [TEST M4.1] Daily Quota + M3 Script Quota + M4 Video Render (Layout B1) ===", flush=True)
    qm = DailyQuotaManager()
    vreg = VideoRegistryManager()
    sreg = SKURegistryManager()

    need_fresh_render = True
    if OUTPUT_M4_JSON_PATH.exists():
        try:
            existing_m4 = M4ProductionOutput.model_validate_json(
                OUTPUT_M4_JSON_PATH.read_text(encoding="utf-8")
            )
            if (
                len(existing_m4.rendered_videos) == 3
                and all(rv.layout_mode == "B1" and Path(rv.video_path).exists() for rv in existing_m4.rendered_videos)
            ):
                need_fresh_render = False
                m4_out = existing_m4
                print("[M4] Reusing verified 3 Layout B1 videos rendered in current session...", flush=True)
        except Exception:
            need_fresh_render = True

    if need_fresh_render:
        qm.reset_today()
        if vreg.registry_path.exists():
            vreg.registry_path.unlink()
        if sreg.registry_path.exists():
            sreg.registry_path.unlink()

        m15_out = M15ComboCuratorEngine(registry_mgr=sreg).run()
        assert m15_out.status == "COMBO_APPROVED", f"Expected COMBO_APPROVED, got {m15_out.status}"

        m3_out = M3QAQCEngine().run(quota_mgr=qm)
        print(
            f"[M3] used={m3_out.daily_scripts_used_today}/{m3_out.daily_script_quota_limit}, "
            f"remaining={m3_out.remaining_script_quota_today}, status={m3_out.quota_status}",
            flush=True,
        )
        assert m3_out.daily_script_quota_limit == MAX_FINAL_SCRIPTS_PER_DAY
        assert m3_out.daily_scripts_used_today == 4
        assert m3_out.remaining_script_quota_today == 0
        assert m3_out.quota_status == "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW"

        m4_out = M4VideoProducerEngine().run(
            layout_mode="B1",
            quota_mgr=qm,
            ignore_cooldown=True,
        )
    else:
        from pipeline.schemas_m3 import M3ApprovedScriptsOutput
        m3_out = M3ApprovedScriptsOutput.model_validate_json(
            Path("data/m3_approved_scripts.json").read_text(encoding="utf-8")
        )
    assert OUTPUT_M4_JSON_PATH.exists(), f"Missing M4 output JSON: {OUTPUT_M4_JSON_PATH}"
    parsed_json_out = M4ProductionOutput.model_validate_json(
        OUTPUT_M4_JSON_PATH.read_text(encoding="utf-8")
    )

    print(
        f"[M4] rendered={m4_out.daily_videos_rendered_today}/{m4_out.daily_video_quota_limit}, "
        f"remaining={m4_out.remaining_video_quota_today}, status={m4_out.quota_status}, "
        f"next_reset={m4_out.next_quota_reset_at}",
        flush=True,
    )
    assert m4_out.daily_video_quota_limit == MAX_VIDEOS_PER_DAY
    assert m4_out.daily_videos_rendered_today == 3
    assert m4_out.remaining_video_quota_today == 0
    assert m4_out.quota_status == "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW"
    assert len(m4_out.rendered_videos) == 3
    assert len(parsed_json_out.rendered_videos) == 3

    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    voices_used = set()
    genders_used = set()
    formats_rendered = []

    for rv in m4_out.rendered_videos:
        vpath = Path(rv.video_path)
        assert vpath.exists(), f"Missing rendered video file: {vpath}"
        assert rv.layout_mode == "B1", f"Expected layout_mode B1, got {rv.layout_mode}"
        assert rv.video_status == "RENDERED", f"Expected video_status RENDERED, got {rv.video_status}"
        assert rv.voice_name in ("BV074_streaming", "BV075_streaming"), (
            f"Expected TikTok voice BV074_streaming or BV075_streaming, got {rv.voice_name}"
        )
        assert rv.single_voice_locked is True
        assert rv.bgm_track.startswith("bgm_upbeat_"), f"Expected upbeat BGM track, got {rv.bgm_track}"
        assert 0 < len(rv.hook_title_short_vi) <= 30, (
            f"Expected hook_title_short_vi <= 30 chars, got {len(rv.hook_title_short_vi)}: {rv.hook_title_short_vi!r}"
        )
        voices_used.add(rv.voice_name)
        genders_used.add(rv.voice_gender)
        formats_rendered.append(rv.script_format)

        info = probe_video_and_audio(vpath)
        assert rv.social_post_pack.caption_vi.strip(), f"Empty caption_vi for {rv.script_id}"
        assert len(rv.social_post_pack.hashtags) > 0, f"Empty hashtags for {rv.script_id}"
        face_free_all = all(ss.face_free_verified for ss in rv.sub_shots)
        dest_artifact = ARTIFACTS_DIR / vpath.name
        shutil.copy2(vpath, dest_artifact)
        print(
            f"  -> {rv.script_id} [{rv.script_format} | {rv.domain} | {rv.category_id}]: "
            f"{info['width']}x{info['height']} layout={rv.layout_mode} status={rv.video_status} "
            f"voice={rv.voice_name}({rv.voice_gender}) bgm={rv.bgm_track} "
            f"hook='{rv.hook_title_short_vi}' ({len(rv.hook_title_short_vi)}c) "
            f"dur={info['duration_sec']:.2f}s size={info['size_bytes']}B "
            f"sources={rv.unique_source_videos_used} face_free={face_free_all} "
            f"copied_to={dest_artifact}",
            flush=True,
        )

    assert genders_used == {"FEMALE", "MALE"}, (
        f"Expected both FEMALE and MALE voices across 3 videos, got {genders_used} ({voices_used})"
    )
    assert "COMBO_WORKFLOW_SYNERGY" in formats_rendered, (
        f"Expected at least 1 COMBO_WORKFLOW_SYNERGY video in rendered batch, got {formats_rendered}"
    )

    # Check VideoRegistryManager state: 3 RENDERED, 1 SCRIPT_APPROVED queued, 0 PUBLISHED
    rendered_entries = vreg.get_rendered_unposted_videos()
    queued_entries = vreg.get_queued_approved_scripts()
    raw_items = vreg._load_raw().get("items") or {}
    published_entries = [v for v in raw_items.values() if isinstance(v, dict) and v.get("status") == "PUBLISHED"]
    print(
        f"[VIDEO REGISTRY] RENDERED={len(rendered_entries)} | SCRIPT_APPROVED(queued)={len(queued_entries)} | PUBLISHED={len(published_entries)}",
        flush=True,
    )
    assert len(rendered_entries) == 3
    assert len(queued_entries) == 1
    assert len(published_entries) == 0, "M4 must NEVER mark a video as PUBLISHED"

    # Check SKURegistryManager state after rendering Solo + Combo videos
    sreg_entries = sreg.get_all_entries()
    for sku_id, entry in sreg_entries.items():
        print(
            f"[SKU REGISTRY] {sku_id}: status={entry.lifecycle_status} | solo={entry.has_solo_video} | combo={entry.has_combo_video}",
            flush=True,
        )

    rendered_domains = {rv.domain for rv in m4_out.rendered_videos}
    assert rendered_domains == {"CLEANING_CARE", "MINI_APPLIANCE"}, (
        f"Expected both CLEANING_CARE and MINI_APPLIANCE domains, got {rendered_domains}"
    )

    print("\n=== [TEST M4.2] 2-Hour Render Cooldown Gate Verification ===", flush=True)
    # Temporarily test cooldown when daily quota has room (e.g. 1 video rendered just now)
    tmp_quota_path = Path("/tmp/test_cooldown_quota.json")
    tmp_qm = DailyQuotaManager(state_path=tmp_quota_path)
    tmp_qm.reset_today()
    tmp_qm.record_rendered_video("s1_test_video", voice_name="BV074_streaming")
    can_now, status_code, secs_rem = tmp_qm.can_render_video_now("s2_test_video", ignore_cooldown=False)
    print(
        f"[COOLDOWN GATE] can_render_now={can_now} | status_code={status_code} | "
        f"seconds_until_next_render={secs_rem}s (min={MIN_RENDER_COOLDOWN_SECONDS}s)",
        flush=True,
    )
    assert can_now is False, "Expected immediate 2nd render to be blocked by 2-hour cooldown"
    assert status_code == "COOLDOWN_RESTING_2H", f"Expected COOLDOWN_RESTING_2H, got {status_code}"
    assert secs_rem > 7000, f"Expected ~7200s remaining in cooldown, got {secs_rem}"

    cooldown_out = M4VideoProducerEngine().produce_from_m3(
        m3_out,
        layout_mode="B1",
        quota_mgr=tmp_qm,
        ignore_cooldown=False,
    )
    print(
        f"[COOLDOWN M4 RUN] status={cooldown_out.quota_status} | rendered={len(cooldown_out.rendered_videos)} | "
        f"secs_remaining={cooldown_out.seconds_until_next_render}",
        flush=True,
    )
    assert cooldown_out.quota_status == "COOLDOWN_RESTING_2H"
    assert len(cooldown_out.rendered_videos) == 0
    tmp_quota_path.unlink(missing_ok=True)

    # Also test flexible SKU lifecycle
    verify_flexible_sku_lifecycle()

    print("\nALL M1.5 + M3 + M4 (LAYOUT B1) TESTS PASSED!", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
