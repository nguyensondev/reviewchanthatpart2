"""
Master Daily Pipeline Orchestrator — Step C (`pipeline/run_daily_pipeline.py`).

Coordinates modular execution of:
  - Planning Batch (`M0 -> M1 -> M1.5 -> M2 -> M3`): Runs at most once per day when the
    eligible `SCRIPT_APPROVED` queue needs replenishment, rotating across 18+ functional
    categories (`TECH_DESK` + `KITCHEN_TECH`) and blocking any category/SKU on 3-7 day cooldown.
  - Render Worker (`M4`): Renders at most 1 video per invocation (up to 3 videos/day),
    enforcing the 2-hour post-render rest (`COOLDOWN_RESTING_2H`) AND the random 3-7 day
    per-SKU cooldown (`next_eligible_at`) so no SKU ever appears in both a Solo video and
    a Combo video within 3-7 days.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import List, Optional

from pipeline.core_resilience import (
    HumanBehaviorGuard,
    MacOSLegacyRuntime,
    SingleInstanceLock,
    StorageGarbageCollector,
    TelegramNotifier,
)
from pipeline.daily_quota import (
    MAX_FINAL_SCRIPTS_PER_DAY,
    MAX_VIDEOS_PER_DAY,
    MIN_RENDER_COOLDOWN_SECONDS,
    DailyQuotaManager,
)
from pipeline.m0_discovery import M0AutonomousDiscoveryEngine, load_m0_history
from pipeline.m1_5_combo_curator import M15ComboCuratorEngine
from pipeline.m1_filter import M1SmartFilterEngine
from pipeline.m2_script_gen import M2ScriptGeneratorEngine
from pipeline.m3_qaqc import M3QAQCEngine
from pipeline.m4_5_video_qaqc import M45VideoQAQCEngine
from pipeline.m4_video_producer import M4VideoProducerEngine
from pipeline.m5_publisher import M5PublisherEngine
from pipeline.publish_packager import (
    DEFAULT_MANUAL_LINKS_PATH,
    DEFAULT_PUBLISH_MARKDOWN_PATH,
    PublishPackager,
)
from pipeline.schemas_m1_5 import normalize_sku_key
from pipeline.schemas_m3 import AuditedScriptItem
from pipeline.shopee_browser_link_resolver import ShopeeAutoAffiliateResolver
from pipeline.sku_registry import SKURegistryManager
from pipeline.video_registry import VideoRegistryManager


def _get_script_sku_keys(script: AuditedScriptItem, sreg: SKURegistryManager) -> set[str]:
    keys: set[str] = set()
    for cid in script.primary_sku_ids:
        keys.add(cid)
        resolved = sreg.resolve_sku_key_by_candidate_id(cid)
        if resolved:
            keys.add(resolved)
    for c in script.verified_factual_claims:
        keys.add(c.candidate_id)
        keys.add(normalize_sku_key(c.brand, c.model_code, c.candidate_id))
    return keys


def get_ready_queued_scripts(
    vreg: VideoRegistryManager,
    sreg: SKURegistryManager,
    qm: DailyQuotaManager,
) -> tuple[List[AuditedScriptItem], List[tuple[AuditedScriptItem, str]]]:
    """
    Partition queued `SCRIPT_APPROVED` scripts into:
      - `ready`: scripts whose SKUs are NOT in 3-7 day cooldown and NOT rendered today
      - `waiting`: scripts temporarily held because at least one SKU is in 3-7 day cooldown
    """
    _, _, rendered_ids_today = qm.get_video_quota_status()
    rendered_skus_today: set[str] = set()
    for rid in rendered_ids_today:
        entry = vreg.get_entry(rid)
        if entry:
            rendered_skus_today.update(entry.primary_sku_ids)
            rendered_skus_today.update(entry.normalized_sku_keys)

    ready: List[AuditedScriptItem] = []
    waiting: List[tuple[AuditedScriptItem, str]] = []
    all_entries = sreg.get_all_entries()

    for sc in vreg.get_queued_approved_scripts():
        if sc.script_id in rendered_ids_today:
            continue
        skus = _get_script_sku_keys(sc, sreg)
        if skus & rendered_skus_today:
            waiting.append((sc, "Vừa xuất hiện trong ngày hôm nay (chờ hết kỳ nghỉ 3-7 ngày)"))
            continue
        cooldown_hits: List[str] = []
        for k in skus:
            if sreg.is_in_cooldown(k):
                resolved = sreg.resolve_sku_key_by_candidate_id(k) or k
                e = all_entries.get(resolved)
                until_str = e.next_eligible_at if e and e.next_eligible_at else "3-7d"
                cooldown_hits.append(f"{resolved} (mở khóa: {until_str})")
        if cooldown_hits:
            waiting.append((sc, f"SKU đang nghỉ 3-7 ngày: {', '.join(cooldown_hits)}"))
        else:
            ready.append(sc)

    return ready, waiting


def print_pipeline_status() -> None:
    qm = DailyQuotaManager()
    vreg = VideoRegistryManager()
    sreg = SKURegistryManager()
    m0_engine = M0AutonomousDiscoveryEngine()
    history = load_m0_history()

    s_used, s_rem, s_ids = qm.get_script_quota_status()
    v_used, v_rem, v_ids = qm.get_video_quota_status()
    cd = qm.get_cooldown_status()
    can_now, cd_status, wait_secs = qm.can_render_video_now(
        allow_rerender_same_id=False,
        ignore_cooldown=False,
    )

    print("======================================================================", flush=True)
    print("  BẢNG ĐIỀU KHIỂN TRẠNG THÁI PIPELINE HÀNG NGÀY (M0 -> M1.5 -> M4)", flush=True)
    print("======================================================================", flush=True)
    print(f"• Ngày hiện tại          : {qm.today_key()}", flush=True)
    print(
        f"• Quota Kịch bản (M3)    : {s_used}/{MAX_FINAL_SCRIPTS_PER_DAY} đã duyệt hôm nay (còn lại: {s_rem})",
        flush=True,
    )
    print(
        f"• Quota Render Video (M4): {v_used}/{MAX_VIDEOS_PER_DAY} đã render hôm nay (còn lại: {v_rem})",
        flush=True,
    )
    rendered_unposted = vreg.get_rendered_unposted_videos()
    m5_engine = M5PublisherEngine()
    m5_u, m5_lim, m5_rem, _ = m5_engine.get_daily_publish_quota_status()
    print(
        f"• Quota Đăng Bài (M5)    : {m5_u}/{m5_lim} đã đăng hôm nay (random 1-2 video/ngày, còn lại: {m5_rem})",
        flush=True,
    )
    print(
        f"• Lịch Render M4         : {qm.get_next_render_schedule_summary_vi(unposted_count=len(rendered_unposted))} "
        f"(status={cd_status} | wait={wait_secs}s)",
        flush=True,
    )
    print(
        f"• Lịch Đăng Bài M5       : {m5_engine.get_next_publish_schedule_summary_vi(unposted_count=len(rendered_unposted))}",
        flush=True,
    )

    ready_scripts, waiting_scripts = get_ready_queued_scripts(vreg, sreg, qm)

    print("\n--- [1] SỔ TRẠNG THÁI VIDEO (`data/video_registry.json`) ---", flush=True)
    print(f"• Video đã render (`RENDERED`, chờ module đăng bài): {len(rendered_unposted)}", flush=True)
    for rv in rendered_unposted:
        print(
            f"  - [{rv.status}] {rv.script_id} ({rv.script_format} | {rv.domain} | voice={rv.voice_name}/{rv.voice_gender}) "
            f"-> {rv.video_path}",
            flush=True,
        )
    print(f"• Kịch bản sẵn sàng render ngay (`SCRIPT_APPROVED` & SKU mở khóa): {len(ready_scripts)}", flush=True)
    for sc in ready_scripts:
        print(f"  - [READY_NOW] {sc.script_id} ({sc.script_format} | SKUs={sc.primary_sku_ids})", flush=True)
    print(f"• Kịch bản đang tạm giữ chờ hết kỳ nghỉ SKU 3-7 ngày: {len(waiting_scripts)}", flush=True)
    for sc, reason in waiting_scripts:
        print(f"  - [HOLD_3_7D] {sc.script_id} ({sc.script_format} | SKUs={sc.primary_sku_ids}) -> {reason}", flush=True)

    print("\n--- [2] VÒNG ĐỜI SKU & KHÓA NGẪU NHIÊN 3-7 NGÀY (`data/sku_registry.json`) ---", flush=True)
    sku_entries = sreg.get_all_entries()
    if not sku_entries:
        print("  (Chưa có SKU nào trong sổ đăng ký)", flush=True)
    for k, e in sku_entries.items():
        in_cd = sreg.is_in_cooldown(k)
        cd_tag = f"COOLDOWN_{e.cooldown_days}D (đến {e.next_eligible_at})" if in_cd else "OPEN"
        print(
            f"  - {k}: status={e.lifecycle_status} | solo={e.has_solo_video} | combo={e.has_combo_video} | "
            f"cooldown={cd_tag}",
            flush=True,
        )

    blocked_cats, fresh_tech, fresh_kitchen = m0_engine._get_blocked_and_fresh_categories(history)
    print("\n--- [3] XOAY VÒNG DANH MỤC THÔNG MINH M0 (`CATEGORY_ROTATION_TAXONOMY`) ---", flush=True)
    print(f"• Danh mục đang khóa (vừa chạy / đang nghỉ 3-7 ngày): {sorted(blocked_cats)}", flush=True)
    print(
        f"• Danh mục mới sẵn sàng cho lần quét kế tiếp: "
        f"TECH_DESK={[c['category_id'] for c in fresh_tech[:3]]} | "
        f"KITCHEN_TECH={[c['category_id'] for c in fresh_kitchen[:3]]}",
        flush=True,
    )

    in_active_hrs, wait_hrs = HumanBehaviorGuard.is_within_human_active_hours()
    win_start_dt, win_end_dt = HumanBehaviorGuard.get_daily_active_window()
    is_rest_day, _, rest_day_name, wait_rest_sec = HumanBehaviorGuard.get_weekly_rest_day_info()
    print("\n--- [4] GÓI ĐĂNG BÀI, CHỐNG BOT SHOPEE & NHỊP NGƯỜI THẬT (BƯỚC A + M5) ---", flush=True)
    print(
        f"• Ngày nghỉ ngẫu nhiên tuần này (M4 & M5): {rest_day_name} "
        f"({'🏖️ HÔM NAY TẠM NGHỈ' if is_rest_day else '🟢 Hôm nay làm việc bình thường'})",
        flush=True,
    )
    print(
        f"• Khung giờ người thật hôm nay ({win_start_dt.strftime('%H:%M')}–{win_end_dt.strftime('%H:%M')}, bắt đầu random 06:00-09:30): "
        f"{'ĐANG MỞ' if in_active_hrs else f'NGHỈ ĐÊM (còn {wait_hrs}s)'}",
        flush=True,
    )
    print(
        f"• Tối ưu máy Mac cũ     : FFmpeg threads={MacOSLegacyRuntime.optimal_ffmpeg_threads()} | font={MacOSLegacyRuntime.resolve_font_path(bold=True)}",
        flush=True,
    )
    print(f"• Bảng copy-paste đăng bài: {DEFAULT_PUBLISH_MARKDOWN_PATH}", flush=True)
    print(f"• Sổ điền link Shopee nhanh: {DEFAULT_MANUAL_LINKS_PATH}", flush=True)
    print("======================================================================", flush=True)


MAX_M2_M3_REWRITE_ROUNDS = 2


def run_m2_m3_with_qaqc_loop(qm: DailyQuotaManager, max_rewrite_rounds: int = MAX_M2_M3_REWRITE_ROUNDS) -> None:
    """
    Execute M2 -> M3 QA/QC with an closed-loop feedback cycle:
      - Round 0: M2 generates initial scripts -> M3 audits strictly (pass threshold >= 82/100).
      - If any script is marked `FAILED_NEEDS_REWRITE`, M3 provides `rewrite_instructions_for_m2_vi`
        and updates `video_registry.json` (`FAILED_NEEDS_REWRITE`).
      - M2 then rewrites ONLY the failed scripts using M3's critique (up to `max_rewrite_rounds`),
        and M3 re-audits until passed or max rounds reached.
    """
    m2_engine = M2ScriptGeneratorEngine()
    m3_engine = M3QAQCEngine()

    m2_engine.run()
    m3_out = m3_engine.run(quota_mgr=qm)

    for round_idx in range(1, max_rewrite_rounds + 1):
        failed_scripts = [
            s for s in m3_out.audited_scripts if s.qaqc_status == "FAILED_NEEDS_REWRITE"
        ]
        if not failed_scripts:
            break
        print(
            f"\n=== [QA/QC FEEDBACK LOOP - VÒNG {round_idx}/{max_rewrite_rounds}] "
            f"M3 đánh trượt {len(failed_scripts)} kịch bản -> Gửi biên bản góp ý bắt M2 viết lại ===",
            flush=True,
        )
        for fs in failed_scripts:
            print(
                f"  • [{fs.script_id}] ({fs.qaqc_score_breakdown.total_qaqc_score_100}/100): "
                f"{fs.rewrite_instructions_for_m2_vi}",
                flush=True,
            )
        m2_engine.run(m3_feedback_path=Path(__file__).resolve().parent.parent / "data" / "m3_approved_scripts.json")
        m3_out = m3_engine.run(quota_mgr=qm)


def run_planning_batch(topic_hint: Optional[str] = None, max_categories: int = 3) -> None:
    """Run M0 -> M1 -> M1.5 -> M2 <-> M3 once to discover fresh categories & queue approved scripts."""
    qm = DailyQuotaManager()
    s_used, s_rem, _ = qm.get_script_quota_status()
    if s_rem <= 0:
        print(
            f"[PLAN] Đã chạm trần {s_used}/{MAX_FINAL_SCRIPTS_PER_DAY} kịch bản trong ngày hôm nay. "
            "Bỏ qua M0->M3 để bảo toàn quota.",
            flush=True,
        )
        return

    print("\n=== [STEP C - PLAN] Running M0 -> M1 -> M1.5 -> M2 <-> M3 Discovery & Script Batch ===", flush=True)
    M0AutonomousDiscoveryEngine().run(topic_hint=topic_hint, max_categories=max_categories)
    M1SmartFilterEngine().run()
    M15ComboCuratorEngine().run()
    run_m2_m3_with_qaqc_loop(qm=qm)


def run_single_render_step(ignore_2h_cooldown: bool = False) -> None:
    """Run M4 (1 video) -> M4.5 (AI Vision Video QA/QC Gate) -> PublishPackager -> M5 (stage payloads) -> StorageGC."""
    qm = DailyQuotaManager()
    m4_out = M4VideoProducerEngine().run(
        layout_mode="B1",
        max_videos=1,
        allow_rerender_same_id=False,
        ignore_cooldown=ignore_2h_cooldown,
        enforce_sku_cooldown=True,
        quota_mgr=qm,
    )
    m4_5_report = M45VideoQAQCEngine().run(purge_on_reject=True)
    approved_video_ids = {
        v.script_id for v in m4_5_report.audited_videos if v.qaqc_video_status == "APPROVED_VIDEO"
    }
    manifest = PublishPackager().build_publish_pack()
    M5PublisherEngine().run(live_upload=False)
    StorageGarbageCollector().sweep_all()

    if m4_out and m4_out.rendered_videos:
        surviving_rendered = [rv for rv in m4_out.rendered_videos if rv.script_id in approved_video_ids]
        if surviving_rendered:
            rendered_summary = [
                f"🎬 Đã render & vượt qua M4.5 AI Vision QA/QC ({len(surviving_rendered)} video):",
                *[
                    f"  • {rv.script_id} ({rv.duration_sec}s | giọng {rv.voice_name})"
                    for rv in surviving_rendered
                ],
                f"📦 Tổng video sạch chờ đăng (RENDERED): {manifest.total_rendered_unposted_videos}",
            ]
            TelegramNotifier().notify_pipeline_event("🎬 HOÀN TẤT RENDER & KIỂM ĐỊNH VIDEO MỚI (M4 -> M4.5)", rendered_summary)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Master Daily Pipeline Orchestrator (M0 -> M1 -> M1.5 -> M2 -> M3 -> M4 -> M4.5 -> Step A + M5)."
    )
    parser.add_argument(
        "--mode",
        choices=["status", "plan", "render", "video-qaqc", "pack", "publish", "shopee-auto", "gc", "auto"],
        default="status",
        help=(
            "Execution mode: 'status', 'plan' (M0->M3 once), 'render' (M4 1 video + M4.5 Vision QA/QC), "
            "'video-qaqc' (run M4.5 AI Vision Video QA/QC on all unposted videos), "
            "'pack' (rebuild SEO + Official Shopee Mall publish pack & stage M5 payloads), "
            "'publish' (run Module M5 publisher with human pacing), 'shopee-auto' (re-verify Official Shopee Mall links), "
            "'gc' (run StorageGarbageCollector), 'auto' (smart daily tick)."
        ),
    )
    parser.add_argument(
        "--hint",
        type=str,
        default=None,
        help="Optional topic hint for M0 when running --mode plan or --mode auto.",
    )
    parser.add_argument(
        "--ignore-2h-cooldown",
        action="store_true",
        help="Bypass the 2-hour render cooldown (for manual testing only; 3-7d SKU cooldown still applies).",
    )
    args = parser.parse_args()
    MacOSLegacyRuntime.ensure_runtime_path()

    if args.mode == "status":
        print_pipeline_status()
        return 0

    lock = SingleInstanceLock()
    if not lock.acquire():
        print("[LOCK] Một tiến trình pipeline khác đang chạy. Bỏ qua lượt gọi chồng này để bảo vệ RAM/CPU!", flush=True)
        return 0

    try:
        MacOSLegacyRuntime.apply_gentle_background_priority()

        if args.mode == "gc":
            stats = StorageGarbageCollector().sweep_all()
            print(f"[GC] Đã giải phóng {stats['total_freed_bytes'] / (1024 * 1024):.2f} MB dung lượng rác: {stats}", flush=True)
            print_pipeline_status()
            return 0

        if args.mode == "shopee-auto":
            PublishPackager().build_publish_pack()
            ShopeeAutoAffiliateResolver().resolve_pending_skus(force_reverify_all=True)
            PublishPackager().build_publish_pack()
            M5PublisherEngine().run(live_upload=False)
            print_pipeline_status()
            return 0

        if args.mode == "video-qaqc":
            M45VideoQAQCEngine().run(force_reaudit=False, purge_on_reject=True)
            PublishPackager().build_publish_pack()
            M5PublisherEngine().run(live_upload=False)
            StorageGarbageCollector().sweep_all()
            print_pipeline_status()
            return 0

        if args.mode == "pack":
            M45VideoQAQCEngine().run(purge_on_reject=True)
            PublishPackager().build_publish_pack()
            M5PublisherEngine().run(live_upload=False)
            StorageGarbageCollector().sweep_all()
            print_pipeline_status()
            return 0

        if args.mode in ("plan", "render", "publish", "auto"):
            if not MacOSLegacyRuntime.wait_for_internet_ready(max_wait_sec=25):
                print(
                    f"[NETWORK] Wi-Fi/Internet tạm gián đoạn sau 25s chờ (mode={args.mode}). "
                    "Tạm hoãn an toàn sang chu kỳ chạy ngầm kế tiếp!",
                    flush=True,
                )
                return 0

        if args.mode == "publish":
            M45VideoQAQCEngine().run(purge_on_reject=True)
            PublishPackager().build_publish_pack()
            M5PublisherEngine().run(live_upload=True)
            print_pipeline_status()
            return 0

        if args.mode == "plan":
            run_planning_batch(topic_hint=args.hint)
            print_pipeline_status()
            return 0

        if args.mode == "render":
            run_single_render_step(ignore_2h_cooldown=args.ignore_2h_cooldown)
            print_pipeline_status()
            return 0

        # --mode auto
        is_rest_day, _, rest_day_name, wait_rest_sec = HumanBehaviorGuard.get_weekly_rest_day_info()
        if is_rest_day and not args.ignore_2h_cooldown:
            print(
                f"[AUTO] 🏖️ Hôm nay là ngày nghỉ ngẫu nhiên trong tuần ({rest_day_name})! "
                f"Module M4 (Render) và M5 (Upload) tạm nghỉ trọn ngày giống người thật (mở lại sau {wait_rest_sec}s).",
                flush=True,
            )
            StorageGarbageCollector().sweep_all()
            print_pipeline_status()
            return 0

        in_hrs, wait_hrs = HumanBehaviorGuard.is_within_human_active_hours()
        win_start_dt, win_end_dt = HumanBehaviorGuard.get_daily_active_window()
        if not in_hrs and not args.ignore_2h_cooldown:
            print(
                f"[AUTO] Đang ngoài khung giờ hoạt động hôm nay ({win_start_dt.strftime('%H:%M')}–{win_end_dt.strftime('%H:%M')}, "
                f"còn {wait_hrs}s đến giờ mở). Tạm nghỉ!",
                flush=True,
            )
            StorageGarbageCollector().sweep_all()
            print_pipeline_status()
            return 0

        # First sweep deleted/corrupted files & audit any unposted videos with M4.5 Vision QA/QC
        # so any bad video is immediately purged and its M4 quota/cooldown refunded before checking M4 queue
        StorageGarbageCollector().sweep_all()
        M45VideoQAQCEngine().run(purge_on_reject=True)

        qm = DailyQuotaManager()
        vreg = VideoRegistryManager()
        sreg = SKURegistryManager()

        can_now, cd_status, wait_secs = qm.can_render_video_now(
            allow_rerender_same_id=False,
            ignore_cooldown=args.ignore_2h_cooldown,
        )
        unposted_now = vreg.get_rendered_unposted_videos()
        if len(unposted_now) >= 2 and not args.ignore_2h_cooldown:
            print(
                f"[AUTO] Đang có sẵn {len(unposted_now)} video RENDERED chờ M5 đăng. Tạm ngưng render mới để tránh phình ổ cứng!",
                flush=True,
            )
        elif not can_now:
            print(
                f"[AUTO] M4 đang trong nhịp nghỉ tự nhiên (status={cd_status}, wait={wait_secs}s).",
                flush=True,
            )
        else:
            ready_scripts, _ = get_ready_queued_scripts(vreg, sreg, qm)
            _, s_rem, _ = qm.get_script_quota_status()
            if not ready_scripts and s_rem > 0:
                print(
                    "[AUTO] Hàng đợi kịch bản mở khóa đang trống -> Tự động chạy 1 lượt M0->M3 để tìm nhóm sản phẩm mới...",
                    flush=True,
                )
                run_planning_batch(topic_hint=args.hint)
                ready_scripts, _ = get_ready_queued_scripts(vreg, sreg, qm)

            if ready_scripts:
                run_single_render_step(ignore_2h_cooldown=args.ignore_2h_cooldown)
            else:
                print(
                    "[AUTO] Chưa có kịch bản nào đủ điều kiện mở khóa để render lúc này.",
                    flush=True,
                )

        # Autonomous M4.5 Video Vision QA/QC + M5 Live Publish tick inside active human hours
        M45VideoQAQCEngine().run(purge_on_reject=True)
        unposted = vreg.get_rendered_unposted_videos()
        m5_engine = M5PublisherEngine()
        can_pub, pub_reason, wait_pub = m5_engine.can_publish_human_now(force_now=args.ignore_2h_cooldown)
        if unposted and can_pub:
            print("[AUTO] Đã đến nhịp đăng bài tự nhiên -> Chạy M5 Live Upload cho 1 video...", flush=True)
            PublishPackager().build_publish_pack()
            m5_engine.run(live_upload=True, force_now=args.ignore_2h_cooldown)
        elif unposted:
            print(
                f"[AUTO] Có {len(unposted)} video chờ đăng nhưng M5 đang nghỉ nhịp người thật ({pub_reason}, wait={wait_pub}s).",
                flush=True,
            )

        StorageGarbageCollector().sweep_all()
        print_pipeline_status()
        return 0
    except Exception as exc:
        TelegramNotifier().notify_pipeline_event(
            "⚠️ CẢNH BÁO LỖI PIPELINE",
            [f"Mode: {args.mode}", f"Chi tiết: {exc}"],
        )
        raise
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
