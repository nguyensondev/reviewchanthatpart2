"""
Module M4.5: AI Vision Video QA/QC Auditor (`pipeline/m4_5_video_qaqc.py`).

Black-Box Contract (Criterion #1):
  - Zero cross-stage imports (`M0..M4`, `PublishPackager`, `M5` are NEVER imported).
  - Reads rendered 9:16 MP4 files referenced in `data/video_registry.json` and `data/m4_production_output.json`.
  - Builds a 6-frame 3x2 Storyboard Contact Sheet JPEG (`~120 KB`) and runs Gemini Full-Flash Vision
    to verify that the visual footage genuinely depicts the expected product, has zero CapCut/TikTok/shop
    watermarks, and has zero talking-head reviewers.
  - Any video failing QA/QC (`< 80/100` or flagged for wrong content / CapCut watermark / talking head)
    is immediately deleted from disk, purged from all registries/manifests so M5 never uploads it, and
    its offending YouTube `source_video_id` is permanently added to `data/blacklisted_video_ids.json`.
"""
from __future__ import annotations

import argparse
import datetime as dt
import io
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import cv2
from PIL import Image, ImageDraw, ImageFont

from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import (
    MacOSLegacyRuntime,
    TelegramNotifier,
    atomic_write_json,
    atomic_write_text,
    safe_load_json,
)
from pipeline.daily_quota import DailyQuotaManager
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.schemas_m4 import M4ProductionOutput, RenderedVideoItem
from pipeline.schemas_m4_5 import (
    AuditedVideoQAQCItem,
    LLMM45VideoVisionVerdict,
    M45VideoQAQCReport,
    VideoDefectCode,
)
from pipeline.sku_registry import SKURegistryManager
from pipeline.video_registry import VideoRegistryEntry, VideoRegistryManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
INPUT_M4_JSON_PATH = DATA_DIR / "m4_production_output.json"
OUTPUT_M4_5_REPORT_PATH = DATA_DIR / "m4_5_video_qaqc_report.json"
STORYBOARD_DIR = DATA_DIR / "m4_5_storyboards"

MIN_PASSING_VIDEO_QAQC_SCORE = 80.0
FONT_BOLD_PATH = MacOSLegacyRuntime.resolve_font_path(bold=True)


def build_video_storyboard_jpeg(
    video_path: Path,
    out_jpg_path: Optional[Path] = None,
) -> Tuple[bytes, int, int, float, int]:
    """
    Extract 6 evenly spaced frames across `video_path` (at 10%, 26%, 42%, 58%, 74%, 90% of duration)
    and composite them into a 3x2 Storyboard Contact Sheet JPEG (`990x1180`, ~110 KB).
    Returns `(jpeg_bytes, width, height, duration_sec, black_frame_count)`.
    """
    cap = cv2.VideoCapture(str(video_path))
    fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
    frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    duration = float(frame_count / fps) if fps > 0 and frame_count > 0 else 0.0

    sample_ratios = (0.10, 0.26, 0.42, 0.58, 0.74, 0.90)
    cell_w, cell_h = 320, 568
    grid_w = cell_w * 3 + 16
    grid_h = cell_h * 2 + 12
    canvas = Image.new("RGB", (grid_w, grid_h), (18, 22, 32))
    draw = ImageDraw.Draw(canvas)
    try:
        font_lbl = ImageFont.truetype(FONT_BOLD_PATH, 18)
    except Exception:
        font_lbl = ImageFont.load_default()

    black_frames = 0
    total_frames_int = max(1, int(frame_count))

    for idx, ratio in enumerate(sample_ratios):
        target_f = min(total_frames_int - 1, max(0, int(round(total_frames_int * ratio))))
        cap.set(cv2.CAP_PROP_POS_FRAMES, target_f)
        ok, frame_bgr = cap.read()
        col = idx % 3
        row = idx // 3
        x0 = 4 + col * (cell_w + 4)
        y0 = 4 + row * (cell_h + 4)

        if not ok or frame_bgr is None:
            black_frames += 1
            continue

        mean_luma = float(frame_bgr.mean())
        if mean_luma < 8.0:
            black_frames += 1

        small_bgr = cv2.resize(frame_bgr, (cell_w, cell_h))
        small_rgb = cv2.cvtColor(small_bgr, cv2.COLOR_BGR2RGB)
        pil_cell = Image.fromarray(small_rgb)
        canvas.paste(pil_cell, (x0, y0))

        ts_sec = round(duration * ratio, 1)
        draw.rounded_rectangle([x0 + 8, y0 + 8, x0 + 158, y0 + 36], radius=8, fill=(0, 0, 0))
        draw.text((x0 + 16, y0 + 12), f"#{idx + 1} ({ts_sec}s)", font=font_lbl, fill=(255, 214, 70))

    cap.release()

    buf = io.BytesIO()
    canvas.save(buf, format="JPEG", quality=82, optimize=True)
    jpeg_bytes = buf.getvalue()
    if out_jpg_path is not None:
        out_jpg_path.parent.mkdir(parents=True, exist_ok=True)
        out_jpg_path.write_bytes(jpeg_bytes)
    return jpeg_bytes, width, height, round(duration, 2), black_frames


class M45VideoQAQCEngine:
    """
    Independent Multimodal Vision Video QA/QC Gate between M4 and M5.
    Inspects rendered 9:16 MP4 videos, blocks & purges any video with mismatched visual content
    or watermarks (CapCut, TikTok, 3rd-party shops), and permanently blacklists bad source video IDs.
    """

    def __init__(
        self,
        llm_client: Optional[FreeTierGeminiClient] = None,
        m4_output_path: Path = INPUT_M4_JSON_PATH,
        report_path: Path = OUTPUT_M4_5_REPORT_PATH,
        storyboard_dir: Path = STORYBOARD_DIR,
    ) -> None:
        self.llm = llm_client or FreeTierGeminiClient()
        self.m4_output_path = m4_output_path
        self.report_path = report_path
        self.storyboard_dir = storyboard_dir
        self.storyboard_dir.mkdir(parents=True, exist_ok=True)
        self.brand_gate = OfficialBrandVideoGate()
        self.video_registry = VideoRegistryManager()
        self.sku_registry = SKURegistryManager()

    def _load_previous_approved_cache(self) -> Dict[Tuple[str, int], AuditedVideoQAQCItem]:
        """Cache previously approved `(script_id, file_size_bytes)` so unchanged videos aren't re-audited."""
        cache: Dict[Tuple[str, int], AuditedVideoQAQCItem] = {}
        if not self.report_path.exists():
            return cache
        try:
            prev = M45VideoQAQCReport.model_validate_json(self.report_path.read_text(encoding="utf-8"))
            for item in prev.audited_videos:
                if item.qaqc_video_status == "APPROVED_VIDEO" and item.file_size_bytes > 100_000:
                    if not any(self.brand_gate.is_video_blacklisted(vid) for vid in item.source_video_ids):
                        cache[(item.script_id, item.file_size_bytes)] = item
        except Exception:
            pass
        return cache

    def _load_m4_items_by_id(self) -> Dict[str, RenderedVideoItem]:
        if not self.m4_output_path.exists():
            return {}
        try:
            m4_out = M4ProductionOutput.model_validate_json(self.m4_output_path.read_text(encoding="utf-8"))
            return {rv.script_id: rv for rv in m4_out.rendered_videos}
        except Exception:
            return {}

    def _describe_expected_product(
        self,
        entry: VideoRegistryEntry,
        m4_item: Optional[RenderedVideoItem],
    ) -> Tuple[str, List[str]]:
        """Build human-readable expected product description & source YouTube video IDs for `entry`."""
        sku_map = self.sku_registry.get_all_entries()
        parts: List[str] = []
        for cid in list(entry.primary_sku_ids) + list(entry.normalized_sku_keys):
            rk = self.sku_registry.resolve_sku_key_by_candidate_id(cid) or cid
            reg_e = sku_map.get(rk)
            if reg_e:
                desc = f"{reg_e.brand} {reg_e.model_code or ''} — {reg_e.product_name_vi} (Nhóm: {reg_e.category_name_vi})".strip()
                if desc not in parts:
                    parts.append(desc)

        if not parts:
            parts.append(f"{entry.title_vi} (category_id={entry.category_id}, domain={entry.domain})")

        src_vids: List[str] = []
        if m4_item and m4_item.unique_source_videos_used:
            src_vids = list(m4_item.unique_source_videos_used)
        elif m4_item and m4_item.sub_shots:
            src_vids = list(
                dict.fromkeys(
                    v
                    for ss in m4_item.sub_shots
                    for v in ss.source_video_id.split("+")
                    if v
                )
            )
        return "; ".join(parts), src_vids

    def _purge_rejected_video_and_blacklist_sources(
        self,
        script_id: str,
        mp4_path: Path,
        source_video_ids: List[str],
        reason_vi: str,
    ) -> List[str]:
        """
        1. Add non-curated offending `source_video_ids` to `data/blacklisted_video_ids.json` & delete cached source MP4s.
        2. Delete rendered `output/videos/<script_id>.mp4`.
        3. Remove `script_id` from `video_registry.json`, `m4_production_output.json`, and `output/publish_pack/`.
        """
        curated_ids = {
            item.get("video_id")
            for clip_list in (self.brand_gate.data.get("curated_official_videos") or {}).values()
            for item in clip_list
            if isinstance(item, dict) and item.get("video_id")
        }
        blacklisted_now: List[str] = []
        for vid in source_video_ids:
            if vid and vid not in curated_ids:
                self.brand_gate.blacklist_video_id(vid, reason_vi=reason_vi, script_id=script_id)
                blacklisted_now.append(vid)

        if mp4_path.exists():
            try:
                mp4_path.unlink(missing_ok=True)
            except OSError:
                pass

        self.video_registry.delete_entry(script_id)
        self.video_registry.purge_script_from_m2_m3_files(script_id)
        DailyQuotaManager().refund_rejected_or_deleted_video(script_id)
        self.sku_registry.refund_rejected_video_skus(script_id)

        if self.m4_output_path.exists():
            try:
                m4_raw = safe_load_json(self.m4_output_path, lambda: {})
                if isinstance(m4_raw, dict) and isinstance(m4_raw.get("rendered_videos"), list):
                    m4_raw["rendered_videos"] = [
                        rv for rv in m4_raw["rendered_videos"]
                        if isinstance(rv, dict) and rv.get("script_id") != script_id
                    ]
                    atomic_write_json(self.m4_output_path, m4_raw)
            except Exception:
                pass

        payloads_dir = OUTPUT_DIR / "publish_pack" / "m5_payloads"
        if payloads_dir.exists():
            for pf in payloads_dir.glob(f"{script_id}_*.json"):
                try:
                    pf.unlink(missing_ok=True)
                except OSError:
                    pass

        manifest_path = OUTPUT_DIR / "publish_pack" / "publish_manifest.json"
        if manifest_path.exists():
            try:
                man_raw = safe_load_json(manifest_path, lambda: {})
                if isinstance(man_raw, dict) and isinstance(man_raw.get("packages"), list):
                    man_raw["packages"] = [
                        pk for pk in man_raw["packages"]
                        if isinstance(pk, dict) and pk.get("script_id") != script_id
                    ]
                    man_raw["total_rendered_unposted_videos"] = len(man_raw["packages"])
                    atomic_write_json(manifest_path, man_raw)
            except Exception:
                pass

        return blacklisted_now

    def audit_single_video(
        self,
        entry: VideoRegistryEntry,
        m4_item: Optional[RenderedVideoItem] = None,
        purge_on_reject: bool = True,
    ) -> AuditedVideoQAQCItem:
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        vpath = MacOSLegacyRuntime.resolve_portable_video_path(entry.video_path, entry.script_id)
        expected_summary, src_vids = self._describe_expected_product(entry, m4_item)
        sb_path = self.storyboard_dir / f"{entry.script_id}_storyboard.jpg"

        if not vpath.exists() or vpath.stat().st_size < 100_000:
            if purge_on_reject:
                self._purge_rejected_video_and_blacklist_sources(
                    entry.script_id, vpath, src_vids, "File MP4 không tồn tại hoặc bị hỏng (<100KB)"
                )
            return AuditedVideoQAQCItem(
                script_id=entry.script_id,
                title_vi=entry.title_vi,
                category_id=entry.category_id,
                expected_product_summary_vi=expected_summary,
                video_path=str(vpath),
                source_video_ids=src_vids,
                qaqc_video_status="REJECTED_BAD_VIDEO",
                defect_codes=["CORRUPTED_OR_BLACK_FRAMES"],
                auditor_verdict_vi="File MP4 không tồn tại hoặc bị lỗi dung lượng.",
                audited_at=now_iso,
            )

        fsize = vpath.stat().st_size
        jpg_bytes, w, h, dur_sec, black_cnt = build_video_storyboard_jpeg(vpath, out_jpg_path=sb_path)

        # Layer 1: Deterministic technical & blacklist checks
        defects: List[VideoDefectCode] = []
        if w != 1080 or h != 1920 or black_cnt >= 3:
            defects.append("CORRUPTED_OR_BLACK_FRAMES")
        if any(self.brand_gate.is_video_blacklisted(vid) for vid in src_vids):
            defects.append("WRONG_VIDEO_CONTENT")

        if defects:
            reason_l1 = f"Lỗi kỹ thuật Layer-1 ({', '.join(defects)}: {w}x{h}, black_frames={black_cnt}/6)"
            bl_ids = (
                self._purge_rejected_video_and_blacklist_sources(entry.script_id, vpath, src_vids, reason_l1)
                if purge_on_reject
                else []
            )
            return AuditedVideoQAQCItem(
                script_id=entry.script_id,
                title_vi=entry.title_vi,
                category_id=entry.category_id,
                expected_product_summary_vi=expected_summary,
                video_path=str(vpath),
                file_size_bytes=fsize,
                duration_sec=dur_sec,
                source_video_ids=src_vids,
                storyboard_image_path=str(sb_path),
                qaqc_video_status="REJECTED_BAD_VIDEO",
                defect_codes=defects,
                auditor_verdict_vi=reason_l1,
                blacklisted_source_video_ids=bl_ids,
                audited_at=now_iso,
            )

        # Layer 2: Gemini Full-Flash Multimodal Vision Inspection on the 6-frame Storyboard JPEG
        system_instruction = (
            "Bạn là Giám khảo Kiểm định Hình ảnh Video Khắt khe (Module M4.5 AI Vision QA/QC).\n"
            "Bạn được cung cấp 1 tấm ảnh Storyboard 3x2 gồm 6 khung hình cắt ra từ video 9:16 (Layout B1: phần giữa là khung hình video sản phẩm 16:9, phía trên là nền mờ/thẻ thông tin của kênh, phía dưới là phụ đề tiếng Việt của kênh).\n"
            "NHIỆM VỤ CỐT LÕI (BẮT LỖI TUYỆT ĐỐI, KHÔNG NƯƠNG TAY):\n"
            "1. KIỂM TRA ĐÚNG SẢN PHẨM (`is_matching_product_footage`): Nhìn kỹ vào KHUNG HÌNH VIDEO 16:9 Ở GIỮA trong cả 6 ô (#1..#6). Hình ảnh đó có thực sự đang quay đúng sản phẩm/nhóm thiết bị được mô tả trong kịch bản hay không?\n"
            "   - Nếu khung hình ở giữa lại là: cảnh phim chiến tranh, lính tráng, súng đạn, phim điện ảnh, hoạt hình, chơi game, xe cộ, hoặc một món đồ hoàn toàn khác không liên quan đến kịch bản -> BẮT BUỘC đặt `is_matching_product_footage = false` và `product_visual_match_score = 0.0`!\n"
            "2. KIỂM TRA LOGO RÁC / CAPCUT / TIKTOK / SHOP BÊN THỨ 3 (`has_capcut_or_tiktok_or_shop_watermark`):\n"
            "   - Lưu ý: Thẻ bo góc tiếng Việt ở phần nền mờ phía trên (VD: 'THỰC DỤNG • ĐÁNG TIỀN', 'CHỐT NHANH TRƯỚC KHI MUA', 'LƯU Ý THỰC TẾ') và dòng phụ đề tiếng Việt ở phía dưới là giao diện hợp lệ của chính kênh chúng ta (KHÔNG phải watermark rác).\n"
            "   - Nhưng nếu bên trong KHUNG HÌNH VIDEO 16:9 Ở GIỮA xuất hiện logo **CapCut**, logo/ID **TikTok**, **Douyin**, bảng số điện thoại hotline cửa hàng, hoặc watermark kênh review bên thứ 3 -> BẮT BUỘC đặt `has_capcut_or_tiktok_or_shop_watermark = true` và `watermark_cleanliness_score = 0.0`!\n"
            "3. KIỂM TRA CHUẨN FACELESS (`has_talking_head_human_reviewer`): Nếu có bàn tay thao tác với sản phẩm hoặc người mẫu dùng sản phẩm ở góc xa trong TVC hãng thì HỢP LỆ (`false`). Chỉ đặt `true` nếu là cảnh YouTuber/Reviewer đang ngồi nhìn thẳng ống kính nói chuyện (talking head)."
        )

        prompt = f"""
Thông tin Kịch bản & Sản phẩm BẮT BUỘC phải xuất hiện trong video này:
- Mã kịch bản (`script_id`): `{entry.script_id}`
- Tiêu đề kịch bản: {entry.title_vi}
- Nhóm công năng (`category_id`): `{entry.category_id}` (Chủ đề: `{entry.domain}`)
- Sản phẩm cần hiển thị trong khung hình: **{expected_summary}**
- Mã YouTube video nguồn đã dùng: `{src_vids}`

Hãy quan sát kỹ tấm ảnh Storyboard 6 khung hình đính kèm và xuất kết quả kiểm định theo đúng schema `LLMM45VideoVisionVerdict`.
""".strip()

        verdict, _ = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMM45VideoVisionVerdict,
            system_instruction=system_instruction,
            prefer_lite=False,
            temperature=0.1,
            image_bytes_list=[jpg_bytes],
        )

        p_score = round(min(40.0, max(0.0, verdict.product_visual_match_score)), 2)
        w_score = round(min(30.0, max(0.0, verdict.watermark_cleanliness_score)), 2)
        l_score = round(min(30.0, max(0.0, verdict.layout_quality_score)), 2)

        if not verdict.is_matching_product_footage:
            defects.append("WRONG_VIDEO_CONTENT")
            p_score = 0.0
        if verdict.has_capcut_or_tiktok_or_shop_watermark:
            defects.append("WATERMARK_OR_CAPCUT_DETECTED")
            w_score = 0.0
        if verdict.has_talking_head_human_reviewer:
            defects.append("HUMAN_TALKING_HEAD_DETECTED")
            w_score = min(w_score, 5.0)
        if not verdict.layout_and_subtitles_clean:
            defects.append("LAYOUT_OR_SUBTITLE_DEFECT")
            l_score = min(l_score, 10.0)

        total_score = round(p_score + w_score + l_score, 2)
        is_approved = (
            len(defects) == 0
            and verdict.is_matching_product_footage
            and not verdict.has_capcut_or_tiktok_or_shop_watermark
            and not verdict.has_talking_head_human_reviewer
            and total_score >= MIN_PASSING_VIDEO_QAQC_SCORE
        )

        status = "APPROVED_VIDEO" if is_approved else "REJECTED_BAD_VIDEO"
        bl_ids: List[str] = []
        if not is_approved and purge_on_reject:
            reject_reason = (
                f"[{', '.join(defects) or 'LOW_VISUAL_SCORE'}] {verdict.auditor_verdict_vi} "
                f"(Nhận diện trong hình: {verdict.detected_visual_content_vi})"
            )
            bl_ids = self._purge_rejected_video_and_blacklist_sources(
                entry.script_id, vpath, src_vids, reject_reason
            )
            print(
                f"[M4.5 VIDEO QA/QC] 🚫 LOẠI & XÓA VIDEO LỖI `{entry.script_id}` ({total_score}/100): {reject_reason} "
                f"| Blacklisted YouTube IDs={bl_ids}",
                flush=True,
            )
            TelegramNotifier().notify_pipeline_event(
                "🛡️ M4.5 CHẶN & XÓA VIDEO LỖI HÌNH ẢNH",
                [
                    f"🎬 Video bị chặn: {entry.script_id}",
                    f"📦 Sản phẩm kỳ vọng: {expected_summary}",
                    f"👁️ AI Vision nhìn thấy: {verdict.detected_visual_content_vi}",
                    f"🚨 Lý do loại ({total_score}/100): {verdict.auditor_verdict_vi}",
                    f"⛔ Đã khóa vĩnh viễn YouTube ID: {bl_ids or src_vids}",
                ],
            )
        else:
            print(
                f"[M4.5 VIDEO QA/QC] ✅ ĐẠT CHUẨN HÌNH ẢNH `{entry.script_id}` ({total_score}/100): "
                f"{verdict.detected_visual_content_vi[:110]}",
                flush=True,
            )

        return AuditedVideoQAQCItem(
            script_id=entry.script_id,
            title_vi=entry.title_vi,
            category_id=entry.category_id,
            expected_product_summary_vi=expected_summary,
            video_path=str(vpath),
            file_size_bytes=fsize,
            duration_sec=dur_sec,
            source_video_ids=src_vids,
            storyboard_image_path=str(sb_path),
            qaqc_video_status=status,
            defect_codes=defects,
            product_visual_match_score=p_score,
            watermark_cleanliness_score=w_score,
            layout_quality_score=l_score,
            total_video_qaqc_score_100=total_score,
            detected_visual_content_vi=verdict.detected_visual_content_vi,
            auditor_verdict_vi=verdict.auditor_verdict_vi,
            blacklisted_source_video_ids=bl_ids,
            was_cached_pass=False,
            audited_at=now_iso,
        )

    def run(
        self,
        target_script_id: Optional[str] = None,
        force_reaudit: bool = False,
        purge_on_reject: bool = True,
    ) -> M45VideoQAQCReport:
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        run_id = f"m4_5_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"

        prev_cache = {} if force_reaudit else self._load_previous_approved_cache()
        m4_by_id = self._load_m4_items_by_id()
        rendered_entries = self.video_registry.get_rendered_unposted_videos()

        audited_items: List[AuditedVideoQAQCItem] = []
        approved_cnt = 0
        rejected_cnt = 0

        for entry in rendered_entries:
            if target_script_id and entry.script_id != target_script_id:
                continue
            vpath = MacOSLegacyRuntime.resolve_portable_video_path(entry.video_path, entry.script_id)
            fsize = vpath.stat().st_size if vpath.exists() else 0
            cached_item = prev_cache.get((entry.script_id, fsize))
            if cached_item is not None:
                cached_copy = cached_item.model_copy(update={"was_cached_pass": True})
                audited_items.append(cached_copy)
                approved_cnt += 1
                print(
                    f"[M4.5 VIDEO QA/QC] ✅ CACHED PASS `{entry.script_id}` ({cached_copy.total_video_qaqc_score_100}/100)",
                    flush=True,
                )
                continue

            res = self.audit_single_video(
                entry=entry,
                m4_item=m4_by_id.get(entry.script_id),
                purge_on_reject=purge_on_reject,
            )
            audited_items.append(res)
            if res.qaqc_video_status == "APPROVED_VIDEO":
                approved_cnt += 1
            else:
                rejected_cnt += 1

        report = M45VideoQAQCReport(
            m4_5_run_id=run_id,
            generated_at=now_iso,
            llm_models_used=self.llm.models_used_history,
            total_audited=len(audited_items),
            approved_video_count=approved_cnt,
            rejected_video_count=rejected_cnt,
            audited_videos=audited_items,
        )
        atomic_write_text(self.report_path, report.model_dump_json(indent=2))
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Module M4.5: AI Vision Video QA/QC Auditor.")
    parser.add_argument("--script-id", type=str, default=None, help="Audit a specific script_id")
    parser.add_argument("--force", action="store_true", help="Re-audit even if previously cached as approved")
    args = parser.parse_args()

    MacOSLegacyRuntime.ensure_runtime_path()
    M45VideoQAQCEngine().run(target_script_id=args.script_id, force_reaudit=args.force)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
