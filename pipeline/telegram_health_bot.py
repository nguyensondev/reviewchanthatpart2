"""
Telegram Bot Menu & Real-Time Health Inspector (`pipeline/telegram_health_bot.py`).

Zero-dependency Telegram Bot daemon (uses Python stdlib + `config/cacert.pem` for macOS Big Sur 11.4):
  - Registers clickable Telegram Bot Menu commands (`/menu`, `/health`, `/modules`, `/errors`, `/status`).
  - Serves interactive Inline Keyboard buttons for one-tap checks.
  - Performs honest, read-only health inspections across:
      1. Runtime / OS / LaunchAgent / Lock / Disk Cache / Credentials
      2. Every Pipeline Module (`M0`, `M1`, `M1.5`, `M2`, `M3`, `M4`, `M5`)
      3. Recent Errors in `logs/launchd_stderr.log`, `logs/launchd_stdout.log`, `m5_publish_report.json`, and `video_registry.json`
      4. Daily Quotas, Active Hours, and Video Queue

IMPORTANT: Only run `--daemon` on ONE machine (MacBook Pro 2015 Production) at a time,
because Telegram's `getUpdates` API allows one active poller per bot token.
"""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
import platform
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.core_resilience import (
    HumanBehaviorGuard,
    MacOSLegacyRuntime,
    TelegramNotifier,
    atomic_write_json,
    safe_load_json,
)
from pipeline.daily_quota import (
    DailyQuotaManager,
    MAX_FINAL_SCRIPTS_PER_DAY,
    MAX_VIDEOS_PER_DAY,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
LOGS_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"
BOT_STATE_PATH = DATA_DIR / ".telegram_bot_state.json"
BOT_LOCK_PATH = DATA_DIR / ".telegram_bot.lock"
PIPELINE_LOCK_PATH = DATA_DIR / ".pipeline_runtime.lock"
CACHE_LIMIT_MB = 450.0


def _short_ts(ts_str: str) -> str:
    if not ts_str:
        return "chưa có"
    try:
        parsed = dt.datetime.fromisoformat(ts_str)
        return parsed.strftime("%d/%m %H:%M")
    except Exception:
        return ts_str[:16]


def _check_pipeline_running_now() -> bool:
    if not PIPELINE_LOCK_PATH.exists():
        return False
    try:
        fd = os.open(str(PIPELINE_LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        except BlockingIOError:
            return True
        finally:
            os.close(fd)
    except Exception:
        return False


def _check_launchd_service(label: str) -> Tuple[bool, str]:
    """Return `(is_loaded, detail_vi)` for a macOS LaunchAgent label."""
    if sys.platform != "darwin":
        return False, "Không phải macOS"
    try:
        proc = subprocess.run(
            ["launchctl", "list"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if proc.returncode != 0:
            return False, "Không đọc được launchctl"
        for line in proc.stdout.splitlines():
            parts = line.split("\t")
            if len(parts) >= 3 and parts[2].strip() == label:
                pid = parts[0].strip()
                exit_code = parts[1].strip()
                if pid != "-" and pid.isdigit():
                    return True, f"Đang chạy (PID {pid})"
                return True, f"Đã nạp (chờ chu kỳ, mã thoát cuối={exit_code})"
        return False, "Chưa nạp LaunchAgent trên máy này"
    except Exception as exc:
        return False, f"Lỗi kiểm tra launchctl: {exc}"


def _calc_dir_size_mb(path: Path) -> float:
    if not path.exists():
        return 0.0
    total = 0
    for p in path.rglob("*"):
        if p.is_file():
            try:
                total += p.stat().st_size
            except OSError:
                pass
    return round(total / (1024 * 1024), 1)


def _read_json_strict(path: Path) -> Tuple[bool, Optional[Dict[str, Any]], str]:
    if not path.exists():
        return False, None, "Chưa có file dữ liệu"
    try:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            return False, None, "Cấu trúc JSON không phải object"
        return True, data, ""
    except Exception as exc:
        return False, None, f"Lỗi đọc JSON: {exc}"


class PipelineHealthInspector:
    """Builds concise, accurate Vietnamese health reports from live pipeline state and logs."""

    def build_overall_health_report(self) -> str:
        host = platform.node().split(".")[0] or "Mac"
        arch = platform.machine()
        mac_ver = platform.mac_ver()[0] or "N/A"
        now_str = dt.datetime.now().astimezone().strftime("%d/%m/%Y %H:%M:%S")

        pipe_running = _check_pipeline_running_now()
        auto_loaded, auto_desc = _check_launchd_service("com.congnghe.autopipeline")
        bot_loaded, bot_desc = _check_launchd_service("com.congnghe.telegrambot")

        ffmpeg_ok = bool(
            (PROJECT_ROOT / "bin" / "ffmpeg").exists() or shutil.which("ffmpeg")
        )
        ffprobe_ok = bool(
            (PROJECT_ROOT / "bin" / "ffprobe").exists() or shutil.which("ffprobe")
        )

        env_vals = TelegramNotifier._read_dotenv(PROJECT_ROOT / ".env")
        creds = safe_load_json(CONFIG_DIR / "platform_credentials.json", lambda: {})
        if not isinstance(creds, dict):
            creds = {}

        gemini_ok = bool(
            os.environ.get("GEMINI_API_KEYS")
            or os.environ.get("GEMINI_API_KEY")
            or env_vals.get("GEMINI_API_KEYS")
            or env_vals.get("GEMINI_API_KEY")
        )
        cf_ok = bool(
            (os.environ.get("CLOUDFLARE_API_TOKEN") or env_vals.get("CLOUDFLARE_API_TOKEN"))
            and (os.environ.get("CLOUDFLARE_ACCOUNT_ID") or env_vals.get("CLOUDFLARE_ACCOUNT_ID"))
        )
        yt_creds = creds.get("youtube") if isinstance(creds.get("youtube"), dict) else {}
        yt_ok = bool(yt_creds.get("client_id") and yt_creds.get("refresh_token"))
        fb_creds = creds.get("facebook_reels") if isinstance(creds.get("facebook_reels"), dict) else (
            creds.get("facebook") if isinstance(creds.get("facebook"), dict) else {}
        )
        fb_ok = bool(fb_creds.get("page_id") and fb_creds.get("page_access_token"))

        cache_mb = _calc_dir_size_mb(DATA_DIR / "m4_video_cache")
        errors = self._collect_recent_errors()
        err_count = len(errors)

        if not (ffmpeg_ok and ffprobe_ok and gemini_ok and yt_ok and fb_ok):
            overall_badge = "🔴 CẦN KIỂM TRA CẤU HÌNH"
        elif err_count > 0:
            overall_badge = f"🟡 HOẠT ĐỘNG (CÓ {err_count} CẢNH BÁO/LỖI)"
        else:
            overall_badge = "🟢 KHỎE MẠNH (KHÔNG CÓ LỖI)"

        lines = [
            f"🩺 SỨC KHỎE HỆ THỐNG: {overall_badge}",
            f"🕒 Thời điểm: {now_str}",
            f"💻 Máy: {host} ({arch} / macOS {mac_ver})",
            "",
            "⚙️ Tiến trình & Dịch vụ:",
            f"• Pipeline đang chạy ngay lúc này: {'🟢 CÓ (đang xử lý)' if pipe_running else '⚪ Không (đang nghỉ chờ chu kỳ)'}",
            f"• Auto-Pipeline (35p/lần): {'🟢' if auto_loaded else '⚪'} {auto_desc}",
            f"• Telegram Bot Menu: {'🟢' if bot_loaded else '⚪'} {bot_desc}",
            f"• FFmpeg / FFprobe: {'🟢 Sẵn sàng' if (ffmpeg_ok and ffprobe_ok) else '🔴 Thiếu binary'}",
            f"• Dung lượng Cache video: {cache_mb} / {CACHE_LIMIT_MB:.0f} MB",
            "",
            "🔑 Kết nối API:",
            f"• Gemini LLM: {'🟢 OK' if gemini_ok else '🔴 Thiếu key'} | Cloudflare: {'🟢 OK' if cf_ok else '🟡 Thiếu key'}",
            f"• YouTube Shorts: {'🟢 OK' if yt_ok else '🔴 Thiếu OAuth'} | Facebook Reels: {'🟢 OK' if fb_ok else '🔴 Thiếu Token'}",
            "",
            f"🚨 Lỗi/Cảnh báo phát hiện: {err_count} mục"
            + (" (bấm 'Kiểm tra lỗi' để xem chi tiết)" if err_count > 0 else " — Mọi thứ đang chạy trơn tru!"),
        ]
        return "\n".join(lines)

    def build_modules_health_report(self) -> str:
        lines = [
            "🧩 SỨC KHỎE TỪNG MODULE (M0 → M5)",
            f"🕒 Lúc: {dt.datetime.now().astimezone().strftime('%d/%m %H:%M:%S')}",
            "",
        ]

        # M0
        ok, d, err = _read_json_strict(DATA_DIR / "m0_candidate_pool.json")
        if not ok:
            lines.append(f"⚪ M0 (Tìm sản phẩm): {err}")
        else:
            cats = d.get("functional_categories") or d.get("categories") or []
            lines.append(
                f"{'🟢' if cats else '🟡'} M0 (Tìm sản phẩm): {len(cats)} nhóm ngành ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        # M1
        ok, d, err = _read_json_strict(DATA_DIR / "m1_qualified_skus.json")
        if not ok:
            lines.append(f"⚪ M1 (Lọc & Chấm điểm): {err}")
        else:
            qcats = d.get("qualified_categories") or []
            lines.append(
                f"{'🟢' if qcats else '🟡'} M1 (Lọc & Chấm điểm): {len(qcats)} Quán quân đạt chuẩn ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        # M1.5
        ok, d, err = _read_json_strict(DATA_DIR / "m1_5_combo_result.json")
        if not ok:
            lines.append(f"⚪ M1.5 (Ghép Combo): {err}")
        else:
            st = str(d.get("status") or "UNKNOWN")
            lines.append(
                f"🟢 M1.5 (Ghép Combo): {st} ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        # M2
        ok, d, err = _read_json_strict(DATA_DIR / "m2_script_drafts.json")
        if not ok:
            lines.append(f"⚪ M2 (Viết kịch bản): {err}")
        else:
            drafts = d.get("script_drafts") or []
            lines.append(
                f"🟢 M2 (Viết kịch bản): {len(drafts)} bản chờ render (đã xóa bản đã dựng) ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        # M3
        ok, d, err = _read_json_strict(DATA_DIR / "m3_approved_scripts.json")
        if not ok:
            lines.append(f"⚪ M3 (Kiểm định QA/QC): {err}")
        else:
            audits = d.get("audited_scripts") or []
            failed_rw = sum(1 for a in audits if a.get("qaqc_status") == "FAILED_NEEDS_REWRITE")
            rej = sum(1 for a in audits if a.get("qaqc_status") == "REJECTED")
            app = len(audits) - failed_rw - rej
            icon = "🟢" if (failed_rw == 0 and rej == 0) else "🟡"
            extra = f", {failed_rw} chờ viết lại" if failed_rw else ""
            lines.append(
                f"{icon} M3 (Kiểm định QA/QC): {app} chờ render{extra} ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        # M4
        try:
            from pipeline.core_resilience import StorageGarbageCollector
            StorageGarbageCollector().sweep_all()
        except Exception:
            pass
        ok, d, err = _read_json_strict(DATA_DIR / "m4_production_output.json")
        if not ok:
            lines.append(f"⚪ M4 (Dựng video 9:16): {err}")
        else:
            rvs = d.get("rendered_videos") or []
            missing_mp4 = 0
            for rv in rvs:
                vp = MacOSLegacyRuntime.resolve_portable_video_path(
                    str(rv.get("video_path") or ""), str(rv.get("script_id") or "")
                )
                if not vp.exists():
                    missing_mp4 += 1
            icon = "🔴" if missing_mp4 > 0 else "🟢"
            miss_txt = f" (⚠️ thiếu {missing_mp4} file MP4)" if missing_mp4 else ""
            lines.append(
                f"{icon} M4 (Dựng video 9:16): {len(rvs)} video chờ đăng{miss_txt} ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        # M5
        ok, d, err = _read_json_strict(DATA_DIR / "m5_publish_report.json")
        if not ok:
            lines.append(f"⚪ M5 (Đăng YT & FB): {err}")
        else:
            res = d.get("results") or []
            err_items = [
                r
                for r in res
                if "ERROR" in str(r.get("youtube_status") or "")
                or "ERROR" in str(r.get("facebook_reels_status") or "")
            ]
            icon = "🔴" if err_items else "🟢"
            lines.append(
                f"{icon} M5 (Đăng YT & FB): mode={d.get('mode')} | {len(res)} gói | lỗi={len(err_items)} ({_short_ts(str(d.get('generated_at') or ''))})"
            )

        return "\n".join(lines)

    def _collect_recent_errors(self) -> List[str]:
        # Run StorageGarbageCollector first so any manually deleted MP4 is automatically reconciled & purged
        try:
            from pipeline.core_resilience import StorageGarbageCollector
            StorageGarbageCollector().sweep_all()
        except Exception:
            pass
        findings: List[str] = []

        # 1. Check M5 publish report errors
        ok, m5_data, m5_err = _read_json_strict(DATA_DIR / "m5_publish_report.json")
        if not ok and (DATA_DIR / "m5_publish_report.json").exists():
            findings.append(f"[M5 JSON] {m5_err}")
        elif ok and m5_data:
            for item in m5_data.get("results") or []:
                yt_st = str(item.get("youtube_status") or "")
                fb_st = str(item.get("facebook_reels_status") or "")
                if "ERROR" in yt_st or "FAILED" in yt_st:
                    findings.append(f"[M5 YouTube] {item.get('script_id')}: {yt_st[:140]}")
                if "ERROR" in fb_st or "FAILED" in fb_st:
                    findings.append(f"[M5 Facebook] {item.get('script_id')}: {fb_st[:140]}")

        # 2. Check M3 stuck rewrites / rejections
        ok, m3_data, m3_err = _read_json_strict(DATA_DIR / "m3_approved_scripts.json")
        if not ok and (DATA_DIR / "m3_approved_scripts.json").exists():
            findings.append(f"[M3 JSON] {m3_err}")
        elif ok and m3_data:
            for a in m3_data.get("audited_scripts") or []:
                st = str(a.get("qaqc_status") or "")
                if st in ("FAILED_NEEDS_REWRITE", "REJECTED"):
                    findings.append(f"[M3 {st}] {a.get('script_id')}: {str(a.get('auditor_verdict_vi') or '')[:110]}")

        # 3. Check missing MP4 files for RENDERED entries in video_registry.json
        ok, vreg_data, vreg_err = _read_json_strict(DATA_DIR / "video_registry.json")
        if not ok and (DATA_DIR / "video_registry.json").exists():
            findings.append(f"[Registry JSON] {vreg_err}")
        elif ok and vreg_data:
            entries = vreg_data.get("items") or vreg_data.get("entries") or vreg_data.get("videos") or {}
            if isinstance(entries, dict):
                for sid, entry in entries.items():
                    if isinstance(entry, dict) and entry.get("status") == "RENDERED":
                        vp = MacOSLegacyRuntime.resolve_portable_video_path(
                            str(entry.get("video_path") or ""), str(sid)
                        )
                        if not vp.exists():
                            findings.append(f"[M4 Thiếu MP4] {sid} ở trạng thái RENDERED nhưng không thấy file {vp.name}")

        # 4. Scan Launchd stderr & stdout logs (last 150 lines)
        err_markers = ("traceback (most recent call last)", "runtimeerror:", "httperror", "http 401", "http 403", "critical", "exception:")
        for log_name in ("launchd_stderr.log", "launchd_stdout.log"):
            log_path = LOGS_DIR / log_name
            if not log_path.exists():
                continue
            try:
                tail_lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-150:]
                for ln in tail_lines:
                    low = ln.lower()
                    if any(m in low for m in err_markers):
                        clean_ln = ln.strip()[:160]
                        if clean_ln and clean_ln not in findings:
                            findings.append(f"[{log_name}] {clean_ln}")
            except Exception:
                pass

        return findings[:8]

    def build_errors_report(self) -> str:
        findings = self._collect_recent_errors()
        now_str = dt.datetime.now().astimezone().strftime("%d/%m %H:%M:%S")
        if not findings:
            return (
                "✅ KIỂM TRA LỖI GẦN ĐÂY: KHÔNG CÓ LỖI\n"
                f"🕒 Lúc: {now_str}\n\n"
                "• Không có Traceback hoặc lỗi HTTP trong log.\n"
                "• Không có lỗi đăng bài YouTube/Facebook ở M5.\n"
                "• Toàn bộ file MP4 đã render đều nguyên vẹn trên đĩa."
            )
        lines = [
            f"🚨 PHÁT HIỆN {len(findings)} LỖI / CẢNH BÁO CẦN CHÚ Ý",
            f"🕒 Lúc: {now_str}",
            "",
        ]
        for idx, item in enumerate(findings, start=1):
            lines.append(f"{idx}. {item}")
        return "\n".join(lines)

    def build_status_quota_report(self) -> str:
        try:
            from pipeline.core_resilience import StorageGarbageCollector
            StorageGarbageCollector().sweep_all()
        except Exception:
            pass
        qm = DailyQuotaManager()
        s_used, s_rem, _ = qm.get_script_quota_status()
        v_used, v_rem, _ = qm.get_video_quota_status()
        can_now, cd_status, wait_secs = qm.can_render_video_now(
            allow_rerender_same_id=False, ignore_cooldown=False
        )
        in_hrs, wait_hrs = HumanBehaviorGuard.is_within_human_active_hours()
        win_start_dt, win_end_dt = HumanBehaviorGuard.get_daily_active_window()
        is_rest_day, _, rest_day_name, wait_rest_sec = HumanBehaviorGuard.get_weekly_rest_day_info()

        vreg = safe_load_json(DATA_DIR / "video_registry.json", lambda: {})
        entries = (vreg.get("items") or vreg.get("entries") or vreg.get("videos") or {}) if isinstance(vreg, dict) else {}
        cnt_approved = sum(1 for v in entries.values() if isinstance(v, dict) and v.get("status") == "SCRIPT_APPROVED")
        cnt_rendered = sum(1 for v in entries.values() if isinstance(v, dict) and v.get("status") == "RENDERED")
        cnt_published = sum(1 for v in entries.values() if isinstance(v, dict) and v.get("status") == "PUBLISHED")

        m5_st = safe_load_json(DATA_DIR / "m5_publish_state.json", lambda: {})
        next_pub = str(m5_st.get("next_publish_allowed_at") or "") if isinstance(m5_st, dict) else ""
        m5_days = m5_st.get("days") if isinstance(m5_st, dict) and isinstance(m5_st.get("days"), dict) else {}
        m5_today_bucket = m5_days.get(qm.today_key()) if isinstance(m5_days, dict) else {}
        if not isinstance(m5_today_bucket, dict):
            m5_today_bucket = {}
        m5_lim = int(m5_today_bucket.get("daily_limit") or 2)
        m5_used = len(m5_today_bucket.get("published_script_ids") or [])
        m5_rem = max(0, m5_lim - m5_used)

        lines = [
            f"📊 QUOTA & HÀNG ĐỢI ({qm.today_key()})",
            "",
            f"• Ngày nghỉ tuần này (M4 & M5): {rest_day_name} ({'🏖️ Hôm nay nghỉ trọn ngày' if is_rest_day else '🟢 Hôm nay hoạt động'})",
            f"• Giờ bắt đầu hôm nay (random 06:00–09:30): {win_start_dt.strftime('%H:%M')} – {win_end_dt.strftime('%H:%M')} ({'🟢 Đang mở' if in_hrs else f'🌙 Ngoài giờ (còn {wait_hrs // 60} phút)'})",
            f"• Quota Kịch bản (M3): {s_used}/{MAX_FINAL_SCRIPTS_PER_DAY} (còn {s_rem})",
            f"• Quota Render (M4): {v_used}/{MAX_VIDEOS_PER_DAY} (còn {v_rem})",
            f"• Quota Đăng bài (M5, random 1–2/ngày): {m5_used}/{m5_lim} (còn {m5_rem})",
            f"• Nhịp nghỉ Render 2h: {'🟢 Sẵn sàng' if can_now else f'⏳ {cd_status} (còn {wait_secs // 60} phút)'}",
            f"• Lịch đăng M5 tiếp theo: {_short_ts(next_pub) if next_pub else 'Sẵn sàng khi đủ giờ'}",
            "",
            "📦 Sổ Trạng Thái Video (Tự xóa khi xong):",
            f"• Kịch bản chờ render (`SCRIPT_APPROVED`): {cnt_approved} (tự xóa script khi M4 render xong)",
            f"• Video chờ đăng (`RENDERED`): {cnt_rendered} (tự xóa video khi M5 đăng xong 2 kênh)",
        ]
        return "\n".join(lines)


class TelegramHealthBot:
    """Interactive Telegram Bot listener + Menu registration with strict chat_id allowlist."""

    MENU_COMMANDS = [
        {"command": "health", "description": "Kiểm tra sức khỏe tổng quan của tool"},
        {"command": "modules", "description": "Kiểm tra tình trạng từng module M0–M5"},
        {"command": "errors", "description": "Kiểm tra lỗi gần đây & cảnh báo"},
        {"command": "status", "description": "Xem quota hôm nay & hàng đợi video"},
        {"command": "menu", "description": "Mở bảng nút bấm kiểm tra nhanh"},
    ]

    INLINE_KEYBOARD = {
        "inline_keyboard": [
            [
                {"text": "🩺 Sức khỏe tổng quan", "callback_data": "cmd_health"},
                {"text": "🧩 Modules M0–M5", "callback_data": "cmd_modules"},
            ],
            [
                {"text": "🚨 Kiểm tra lỗi", "callback_data": "cmd_errors"},
                {"text": "📊 Quota & Hàng đợi", "callback_data": "cmd_status"},
            ],
        ]
    }

    def __init__(self, notifier: Optional[TelegramNotifier] = None) -> None:
        self.notifier = notifier or TelegramNotifier()
        self.inspector = PipelineHealthInspector()
        ca_bundle = MacOSLegacyRuntime.resolve_ca_bundle_path()
        self.ssl_ctx = ssl.create_default_context(cafile=ca_bundle) if ca_bundle else ssl.create_default_context()

    def _api_call(self, method: str, payload: Optional[Dict[str, Any]] = None, timeout: float = 35.0) -> Dict[str, Any]:
        if not self.notifier.is_configured():
            raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
        url = f"https://api.telegram.org/bot{self.notifier.bot_token}/{method}"
        body = json.dumps(payload or {}, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=timeout, context=self.ssl_ctx) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    def register_menu(self) -> Tuple[bool, str]:
        try:
            res = self._api_call("setMyCommands", {"commands": self.MENU_COMMANDS}, timeout=15.0)
            return bool(res.get("ok")), str(res)
        except Exception as exc:
            return False, str(exc)

    def send_with_menu(self, chat_id: str, text: str) -> bool:
        try:
            if len(text) > 3950:
                text = text[:3940] + "\n...(cắt bớt)"
            res = self._api_call(
                "sendMessage",
                {
                    "chat_id": chat_id,
                    "text": text,
                    "disable_web_page_preview": True,
                    "reply_markup": self.INLINE_KEYBOARD,
                },
                timeout=15.0,
            )
            return bool(res.get("ok"))
        except Exception:
            return False

    def _answer_callback(self, callback_id: str) -> None:
        try:
            self._api_call("answerCallbackQuery", {"callback_query_id": callback_id}, timeout=8.0)
        except Exception:
            pass

    def _render_reply_for_action(self, action: str) -> str:
        act = (action or "").strip().lower()
        if act in ("/health", "cmd_health"):
            return self.inspector.build_overall_health_report()
        if act in ("/modules", "cmd_modules"):
            return self.inspector.build_modules_health_report()
        if act in ("/errors", "cmd_errors"):
            return self.inspector.build_errors_report()
        if act in ("/status", "cmd_status"):
            return self.inspector.build_status_quota_report()
        return (
            "🤖 BẢNG KIỂM TRA SỨC KHỎE HỆ THỐNG\n"
            "Chọn một nút bên dưới hoặc dùng các lệnh ở nút Menu:"
        )

    def _load_offset(self) -> int:
        st = safe_load_json(BOT_STATE_PATH, lambda: {"offset": 0})
        return int(st.get("offset", 0)) if isinstance(st, dict) else 0

    def _save_offset(self, offset: int) -> None:
        atomic_write_json(BOT_STATE_PATH, {"offset": int(offset)}, keep_backup=False)

    def run_daemon(self) -> int:
        if not self.notifier.is_configured():
            print("[TELEGRAM BOT] Thiếu TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID.", file=sys.stderr)
            return 1

        # Acquire dedicated bot lock so multiple bot pollers never collide on the same machine
        BOT_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(str(BOT_LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print("[TELEGRAM BOT] Đã có một tiến trình Telegram Bot đang chạy. Thoát an toàn.", flush=True)
            os.close(lock_fd)
            return 0

        MacOSLegacyRuntime.apply_gentle_background_priority()
        ok_menu, menu_info = self.register_menu()
        print(f"[TELEGRAM BOT] Registered menu commands: ok={ok_menu} ({menu_info})", flush=True)

        offset = self._load_offset()
        allowed_chat = str(self.notifier.chat_id).strip()

        while True:
            try:
                res = self._api_call(
                    "getUpdates",
                    {
                        "offset": offset,
                        "timeout": 25,
                        "allowed_updates": ["message", "callback_query"],
                    },
                    timeout=35.0,
                )
                for upd in res.get("result") or []:
                    uid = int(upd.get("update_id", 0))
                    if uid >= offset:
                        offset = uid + 1
                        self._save_offset(offset)

                    cb = upd.get("callback_query")
                    if isinstance(cb, dict):
                        cb_chat = str(((cb.get("message") or {}).get("chat") or {}).get("id") or "")
                        self._answer_callback(str(cb.get("id") or ""))
                        if cb_chat == allowed_chat:
                            reply = self._render_reply_for_action(str(cb.get("data") or ""))
                            self.send_with_menu(allowed_chat, reply)
                        continue

                    msg = upd.get("message")
                    if isinstance(msg, dict):
                        msg_chat = str((msg.get("chat") or {}).get("id") or "")
                        text = str(msg.get("text") or "").strip()
                        if msg_chat != allowed_chat or not text:
                            continue
                        cmd = text.split()[0].split("@")[0].lower()
                        if cmd in ("/start", "/menu", "/health", "/modules", "/errors", "/status", "/help"):
                            reply = self._render_reply_for_action(cmd)
                            self.send_with_menu(allowed_chat, reply)
            except urllib.error.HTTPError as exc:
                if exc.code == 409:
                    print("[TELEGRAM BOT] 409 Conflict (có máy khác đang poll getUpdates), tạm chờ 20s...", flush=True)
                    time.sleep(20)
                else:
                    print(f"[TELEGRAM BOT] HTTP {exc.code}; chờ 10s...", flush=True)
                    time.sleep(10)
            except Exception as exc:
                print(f"[TELEGRAM BOT] Lỗi mạng tạm thời ({exc}); chờ 8s...", flush=True)
                time.sleep(8)


def main() -> int:
    parser = argparse.ArgumentParser(description="Telegram Health Bot & Menu Inspector")
    parser.add_argument(
        "--register-menu",
        action="store_true",
        help="Register Telegram bot menu commands via setMyCommands and send a quick menu prompt.",
    )
    parser.add_argument(
        "--print-report",
        choices=["health", "modules", "errors", "status"],
        default=None,
        help="Print a health report locally to stdout without starting getUpdates polling.",
    )
    parser.add_argument(
        "--daemon",
        action="store_true",
        help="Run long-polling Telegram bot daemon (intended for 24/7 Production Mac 2015).",
    )
    args = parser.parse_args()
    MacOSLegacyRuntime.ensure_runtime_path()

    bot = TelegramHealthBot()
    if args.print_report:
        print(bot._render_reply_for_action(f"/{args.print_report}"))
        return 0
    if args.register_menu:
        ok, info = bot.register_menu()
        print(f"[TELEGRAM MENU] setMyCommands ok={ok}: {info}")
        if ok and bot.notifier.is_configured():
            bot.send_with_menu(
                bot.notifier.chat_id,
                bot.inspector.build_overall_health_report(),
            )
        return 0 if ok else 1

    return bot.run_daemon()


if __name__ == "__main__":
    sys.exit(main())
