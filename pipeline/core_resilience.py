"""
Core System Resilience, Legacy macOS Optimization, Storage GC & Human Anti-Spam Guard (`pipeline/core_resilience.py`).

Addresses the 5 Enterprise Reliability & Safety Pillars:
  1. Crash Resilience & Atomic Persistence:
     - `atomic_write_text` / `atomic_write_json` (`fsync` + `.bak` backup + POSIX `os.replace`)
     - `safe_load_json` (automatic `.bak` self-healing if primary file was interrupted)
     - `SingleInstanceLock` (`fcntl.flock` non-blocking file lock preventing overlapping runs).
  2. Storage Anti-Bloat & Memory Optimization (`StorageGarbageCollector`):
     - LRU byte-budget eviction on `data/m4_video_cache/` (default max `450 MB`)
     - Immediate cleanup of completed `data/m4_work/<script_id>` scratch folders (while preserving
       incomplete folders on crash so M4 resumes from the exact clause!)
     - Pruning of legacy unused `data/shopee_browser_profile/` and >30-day quota logs.
  3. Legacy macOS Compatibility (`MacOSLegacyRuntime`):
     - Multi-path font discovery across macOS 10.14 Mojave -> macOS 15+ (`/System/Library/Fonts/...`, `/Library/Fonts/...`)
     - `os.nice(10)` background CPU priority & adaptive FFmpeg `-threads` cap (`2` threads on older Macs)
       to prevent RAM spikes, UI freezes, and thermal throttling.
  4. Anti-Spam Human Behavior Guard (`HumanBehaviorGuard`):
     - Active Vietnamese creator hours (`08:15` – `22:15`)
     - Non-robotic jittered cooldown (`8,100s .. 14,800s`, i.e., `2h15m .. 4h06m` with odd seconds)
     - Varied natural Vietnamese pinned-comment templates (8 distinct human phrasings) + `25..65s`
       post-upload human comment delay + staggered cross-platform spacing.
"""
from __future__ import annotations

import datetime as dt
import fcntl
import hashlib
import json
import os
import random
import shutil
import socket
import ssl
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.shared_utils import check_vietnamese_diacritics

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
DEFAULT_LOCK_PATH = DATA_DIR / ".pipeline_runtime.lock"

# Maximum disk budget for cached source videos (450 MB keeps older Macs lean)
MAX_VIDEO_CACHE_BYTES = 450 * 1024 * 1024
MAX_QUOTA_HISTORY_DAYS = 30

# Active human publishing hours (local time):
# Daily start time is RANDOMIZED each day between 06:00 AM and 09:30 AM (210-minute window)
HUMAN_ACTIVE_START_MIN_HOUR_MIN = (6, 0)    # 06:00 AM earliest daily start
HUMAN_ACTIVE_START_MAX_HOUR_MIN = (9, 30)   # 09:30 AM latest daily start
HUMAN_ACTIVE_START_HOUR_MIN = (6, 0)        # Backwards-compatible alias
HUMAN_ACTIVE_END_HOUR_MIN = (22, 15)        # 10:15 PM (with daily offset)

WEEKDAY_NAMES_VI: List[str] = [
    "Thứ 2",
    "Thứ 3",
    "Thứ 4",
    "Thứ 5",
    "Thứ 6",
    "Thứ 7",
    "Chủ nhật",
]

# Natural human rest between videos (>= 2h minimum guaranteed: 2h12m to 3h55m)
HUMAN_COOLDOWN_MIN_SECONDS = 7920       # 2 hours 12 minutes
HUMAN_COOLDOWN_MODE_SECONDS = 9900      # ~2 hours 45 minutes
HUMAN_COOLDOWN_MAX_SECONDS = 14100      # 3 hours 55 minutes

PINNED_COMMENT_INTROS_VI: List[str] = [
    # Nhóm 1: Tự nhiên, gần gũi như người dùng thật chia sẻ kinh nghiệm tránh mua nhầm
    "Mình note sẵn gian hàng chính chủ đúng mã trong clip để mọi người đỡ mua nhầm bản trôi nổi nhé:",
    "Cho bạn nào ngại gõ tìm kiếm, đây là cửa hàng chính thức của hãng đúng phiên bản mình vừa review:",
    "💡 Kinh nghiệm là món này nhiều shop nhái mã lắm, mọi người cứ vào thẳng gian hàng chuẩn của hãng ở đây cho yên tâm:",
    "Bác nào đang định sắm món trong video thì xem giá cập nhật trực tiếp tại gian hàng chính hãng dưới này nha:",
    "✨ Đúng mã sản phẩm chuẩn thông số mình vừa phân tích, mình dẫn sẵn về cửa hàng ủy quyền chính thức cho cả nhà:",
    # Nhóm 2: Ngắn gọn, trực diện, tập trung vào kiểm chứng thông số & bảo hành
    "📍 Gian hàng Mall có bảo hành hãng đàng hoàng cho đúng mẫu xuất hiện trong bài đánh giá hôm nay:",
    "Mình đã lọc sẵn đúng phân loại chuẩn hãng (không bị gộp phụ kiện lẻ), anh em tham khảo chi tiết tại đây nhé:",
    "🔖 Lưu lại chỗ đặt hàng chính ngạch từ nhà sản xuất cho bạn nào cần đối chiếu kỹ thông số trước khi chốt:",
    "💬 Nhiều bạn hay hỏi chỗ mua chuẩn để được bảo hành hãng, mình để thông tin cửa hàng gốc ngay dưới này nhé:",
    "Đúng phiên bản chuẩn mình lên sóng trong clip, mọi người bấm vào đây để xem tồn kho và ưu đãi hiện tại nha:",
    # Nhóm 3: Văn phong tư vấn góc làm việc / căn bếp tinh tế
    "✅ Món đồ này mình đã check kỹ mã model, ai thấy hợp nhu cầu sử dụng thì ghé thẳng gian hàng hãng tham khảo nhé:",
    "🛍️ Tổng hợp địa chỉ đặt mua chính chủ từ thương hiệu cho từng món mình nhắc tới ở trên:",
    "Nếu quyết định lấy bản chính hãng cho bền lâu thì mọi người xem trực tiếp ở gian hàng Mall này là chuẩn nhất:",
    "👉 Khỏi mất công dò từng shop kẻo lệch đời máy, mình ghim sẵn đúng mã chuẩn hãng ở ngay bên dưới:",
    "Góc chia sẻ chỗ mua uy tín: đây là gian hàng phân phối chính thức của hãng cho sản phẩm trong video nhé:",
    # Nhóm 4: Đa dạng nhịp câu mở đầu không trùng n-gram với 3 nhóm trên
    "Cập nhật nơi bán chính hãng đúng model vừa đánh giá (kèm thông tin chi tiết từ hãng) cho ai đang quan tâm:",
    "🌟 Bạn nào ưng thiết kế và công năng của em nó sau khi xem ưu nhược điểm thì tham khảo gian hàng chính chủ tại đây:",
    "Tránh trường hợp bấm nhầm hàng OEM ngoại hình giống hệt, mình để đường dẫn tới thẳng cửa hàng hãng ở đây nha:",
    "📦 Chỗ xem giá thực tế hôm nay và đặt hàng trực tiếp từ gian hàng chính thức trên sàn cho mọi người tiện theo dõi:",
    "Ai cần xem thêm đánh giá từ người mua thực tế tại gian hàng chuẩn của hãng thì mở ngay bên dưới nhé:",
]


# ==============================================================================
# 1. ATOMIC PERSISTENCE & SELF-HEALING JSON IO
# ==============================================================================

def atomic_write_text(path: Path, content: str, keep_backup: bool = True) -> None:
    """
    Crash-safe atomic file write:
      1. Writes full UTF-8 content to `<path>.tmp.<pid>` in the same directory.
      2. Flushes Python buffers and calls `os.fsync()` to commit blocks to disk.
      3. Copies previous valid `<path>` to `<path>.bak` (if `keep_backup=True`).
      4. Atomically swaps `<path>.tmp.<pid>` -> `<path>` via POSIX `os.replace()`.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    bak_path = path.with_name(f"{path.name}.bak")

    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())

        if keep_backup and path.exists() and path.stat().st_size > 2:
            try:
                shutil.copy2(path, bak_path)
            except OSError:
                pass

        os.replace(tmp_path, path)
    finally:
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass


def atomic_write_json(path: Path, payload: Any, indent: int = 2, keep_backup: bool = True) -> None:
    """Serialize `payload` to JSON and write atomically with `fsync` + `.bak` backup."""
    text = json.dumps(payload, ensure_ascii=False, indent=indent)
    atomic_write_text(path, text, keep_backup=keep_backup)


def safe_load_json(path: Path, default_factory: Any) -> Any:
    """
    Load JSON from `path`; if missing or corrupted by an OS crash, automatically attempt
    self-healing recovery from `<path>.bak` before falling back to `default_factory()`.
    """
    bak_path = path.with_name(f"{path.name}.bak")
    for candidate in (path, bak_path):
        if not candidate.exists():
            continue
        try:
            raw = candidate.read_text(encoding="utf-8")
            if not raw.strip():
                continue
            parsed = json.loads(raw)
            if candidate == bak_path:
                # Self-heal primary file from valid backup
                atomic_write_text(path, raw, keep_backup=False)
                print(f"[RESILIENCE] Đã tự phục hồi file trạng thái `{path.name}` từ bản sao lưu `.bak`!", flush=True)
            return parsed
        except Exception:
            continue
    return default_factory() if callable(default_factory) else default_factory


class SingleInstanceLock:
    """Non-blocking POSIX `fcntl.flock` guard ensuring two pipeline processes never collide."""

    def __init__(self, lock_path: Path = DEFAULT_LOCK_PATH) -> None:
        self.lock_path = lock_path
        self._fd: Optional[int] = None

    def acquire(self) -> bool:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(str(self.lock_path), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            os.ftruncate(fd, 0)
            os.write(fd, f"pid={os.getpid()} started_at={dt.datetime.now().isoformat()}\n".encode("utf-8"))
            self._fd = fd
            return True
        except BlockingIOError:
            os.close(fd)
            return False

    def release(self) -> None:
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None

    def __enter__(self) -> bool:
        return self.acquire()

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.release()


# ==============================================================================
# 2. LEGACY MACOS COMPATIBILITY & LOW-RAM / CPU OPTIMIZER
# ==============================================================================

class MacOSLegacyRuntime:
    """Compatibility helpers for older Intel/Apple Silicon Macs (Catalina/Big Sur/Monterey+)."""

    BOLD_FONT_CANDIDATES = [
        "/System/Library/Fonts/Supplemental/Arial Bold.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "/System/Library/Fonts/HelveticaNeue.ttc",
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNS.ttf",
    ]

    REGULAR_FONT_CANDIDATES = [
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/HelveticaNeue.ttc",
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNS.ttf",
    ]

    @classmethod
    def resolve_font_path(cls, bold: bool = False) -> str:
        candidates = cls.BOLD_FONT_CANDIDATES if bold else cls.REGULAR_FONT_CANDIDATES
        for p_str in candidates:
            if Path(p_str).exists():
                return p_str
        return candidates[0]

    @staticmethod
    def resolve_ca_bundle_path() -> Optional[str]:
        """
        Return path to an up-to-date Mozilla Root CA bundle (`config/cacert.pem` or `certifi`).
        Essential on macOS Big Sur 11.4 (May 2021), whose system `/etc/ssl/cert.pem` predates the
        Sept 2021 Let's Encrypt DST Root CA X3 expiration.
        """
        bundled = PROJECT_ROOT / "config" / "cacert.pem"
        if bundled.exists() and bundled.stat().st_size > 50_000:
            return str(bundled)
        try:
            import certifi  # type: ignore
            cpath = Path(certifi.where())
            if cpath.exists():
                return str(cpath)
        except Exception:
            pass
        return None

    @classmethod
    def ensure_runtime_path(cls) -> None:
        """Ensure project-local `./bin`, Homebrew, `~/.local/bin` on PATH and fresh CA bundle in env."""
        preferred = [
            str(PROJECT_ROOT / "bin"),
            "/usr/local/bin",
            "/opt/homebrew/bin",
            str(Path.home() / ".local" / "bin"),
        ]
        current_parts = [p for p in os.environ.get("PATH", "").split(os.pathsep) if p]
        merged: List[str] = []
        for entry in [*preferred, *current_parts]:
            if entry not in merged:
                merged.append(entry)
        os.environ["PATH"] = os.pathsep.join(merged)

        ca_bundle = cls.resolve_ca_bundle_path()
        if ca_bundle:
            os.environ.setdefault("SSL_CERT_FILE", ca_bundle)
            os.environ.setdefault("REQUESTS_CA_BUNDLE", ca_bundle)
            os.environ.setdefault("CURL_CA_BUNDLE", ca_bundle)

    @staticmethod
    def resolve_portable_video_path(raw_path: Optional[str], script_id: str) -> Path:
        """
        Resolve MP4 path portably even when the project folder was copied from another Mac
        with a different username (e.g. `/Users/sonnguyen/...` -> `/Users/macbook/...`).
        """
        default_local = OUTPUT_DIR / "videos" / f"{script_id}.mp4"
        if raw_path:
            p = Path(raw_path)
            if p.exists():
                return p
            relocated_by_name = OUTPUT_DIR / "videos" / p.name
            if relocated_by_name.exists():
                return relocated_by_name
        return default_local

    @staticmethod
    def wait_for_internet_ready(max_wait_sec: int = 25) -> bool:
        """
        Wait gracefully up to `max_wait_sec` if Wi-Fi is momentarily reconnecting
        (e.g. DHCP lease renewal or router hiccup while running headless).
        """
        deadline = time.monotonic() + max(1, max_wait_sec)
        probes = [("1.1.1.1", 443), ("8.8.8.8", 53)]
        while True:
            for host, port in probes:
                try:
                    with socket.create_connection((host, port), timeout=3.0):
                        return True
                except OSError:
                    continue
            if time.monotonic() >= deadline:
                return False
            time.sleep(3.0)

    @classmethod
    def apply_gentle_background_priority(cls) -> None:
        """Ensure runtime PATH and lower CPU priority (`nice +10`) so background FFmpeg never lags older Macs."""
        cls.ensure_runtime_path()
        try:
            cur = os.nice(0)
            if cur < 10:
                os.nice(10 - cur)
        except OSError:
            pass

    @staticmethod
    def optimal_ffmpeg_threads() -> int:
        """Cap FFmpeg worker threads (`2` on <=4 core Macs, `3` on larger Macs) to bound RAM & heat."""
        cores = os.cpu_count() or 2
        if cores <= 4:
            return 2
        return 3


# ==============================================================================
# 3. STORAGE GARBAGE COLLECTOR (ANTI-BLOAT)
# ==============================================================================

class StorageGarbageCollector:
    """Keeps `data/` lean so disk usage never grows unboundedly over months of autonomous operation."""

    def __init__(
        self,
        data_dir: Path = DATA_DIR,
        max_video_cache_bytes: int = MAX_VIDEO_CACHE_BYTES,
    ) -> None:
        self.data_dir = data_dir
        self.max_video_cache_bytes = max_video_cache_bytes

    def cleanup_completed_work_dir(self, script_work_dir: Path) -> int:
        """Remove intermediate WAV/MP4 slice files in `data/m4_work/<script_id>` once final MP4 is verified."""
        if not script_work_dir.exists() or not script_work_dir.is_dir():
            return 0
        freed = sum(f.stat().st_size for f in script_work_dir.rglob("*") if f.is_file())
        shutil.rmtree(script_work_dir, ignore_errors=True)
        return freed

    def sweep_all(self, active_script_ids: Optional[set[str]] = None) -> Dict[str, int]:
        """
        Sweep obsolete caches:
          1. Remove deprecated `data/shopee_browser_profile/` (no longer used; zero-browser architecture).
          2. Remove `data/m4_work/<script_id>` folders whose final `output/videos/<script_id>.mp4` already exists
             (leaves incomplete work dirs intact for crash-resume!).
          3. Enforce LRU byte cap (`max_video_cache_bytes`) on `data/m4_video_cache/`.
        """
        active = active_script_ids or set()
        freed_profile = 0
        freed_work = 0
        freed_cache = 0

        # 1. Remove obsolete browser profile if present
        obsolete_profile = self.data_dir / "shopee_browser_profile"
        if obsolete_profile.exists():
            freed_profile = sum(f.stat().st_size for f in obsolete_profile.rglob("*") if f.is_file())
            shutil.rmtree(obsolete_profile, ignore_errors=True)

        # 2. Clean completed m4_work directories (preserve incomplete ones for crash resume)
        work_root = self.data_dir / "m4_work"
        out_videos = OUTPUT_DIR / "videos"
        if work_root.exists():
            for sub in work_root.iterdir():
                if not sub.is_dir() or sub.name in active:
                    continue
                done_mp4 = out_videos / f"{sub.name}.mp4"
                if done_mp4.exists() and done_mp4.stat().st_size > 100_000:
                    freed_work += self.cleanup_completed_work_dir(sub)

        # 3. Enforce LRU cap on m4_video_cache (also delete partial `.part` / `.ytdl` leftovers > 2h old)
        cache_root = self.data_dir / "m4_video_cache"
        if cache_root.exists():
            now_ts = dt.datetime.now().timestamp()
            files: List[Tuple[float, int, Path]] = []
            for f in cache_root.iterdir():
                if not f.is_file():
                    continue
                st = f.stat()
                if f.suffix in (".part", ".ytdl", ".temp") and (now_ts - st.st_mtime) > 7200:
                    freed_cache += st.st_size
                    f.unlink(missing_ok=True)
                    continue
                if f.name.endswith(".scan.json"):
                    continue
                files.append((st.st_mtime, st.st_size, f))

            total_bytes = sum(item[1] for item in files)
            if total_bytes > self.max_video_cache_bytes:
                # Sort oldest mtime first (LRU eviction)
                files.sort(key=lambda x: x[0])
                for _, f_size, f_path in files:
                    if total_bytes <= self.max_video_cache_bytes:
                        break
                    f_path.unlink(missing_ok=True)
                    scan_sidecar = cache_root / f"{f_path.stem}.scan.json"
                    scan_sidecar.unlink(missing_ok=True)
                    total_bytes -= f_size
                    freed_cache += f_size

        # 4. Rotate oversized launchd / runtime logs (> 10 MB -> keep tail 2 MB)
        freed_logs = 0
        logs_root = PROJECT_ROOT / "logs"
        max_log_bytes = 10 * 1024 * 1024
        keep_tail_bytes = 2 * 1024 * 1024
        if logs_root.exists():
            for lf in logs_root.glob("*.log"):
                if not lf.is_file():
                    continue
                try:
                    sz = lf.stat().st_size
                    if sz > max_log_bytes:
                        with lf.open("rb") as rf:
                            rf.seek(-keep_tail_bytes, os.SEEK_END)
                            tail = rf.read()
                        nl = tail.find(b"\n")
                        if nl != -1 and nl + 1 < len(tail):
                            tail = tail[nl + 1:]
                        with lf.open("wb") as wf:
                            wf.write(tail)
                        freed_logs += max(0, sz - len(tail))
                except OSError:
                    pass

        # 5. Purge PUBLISHED videos, strip script payloads for RENDERED videos, AND auto-purge any poisoned unaccented Vietnamese scripts/videos
        freed_published = 0
        vreg_path = self.data_dir / "video_registry.json"
        m4_out_p = self.data_dir / "m4_production_output.json"

        def _has_unaccented_vi_texts(texts: List[Any]) -> bool:
            for t in texts:
                if isinstance(t, str) and t.strip():
                    ok_d, _, _ = check_vietnamese_diacritics(t)
                    if not ok_d:
                        return True
            return False

        m4_rv_by_id: Dict[str, Dict[str, Any]] = {}
        if m4_out_p.exists():
            m4_loaded = safe_load_json(m4_out_p, lambda: {})
            if isinstance(m4_loaded, dict) and isinstance(m4_loaded.get("rendered_videos"), list):
                for rv in m4_loaded["rendered_videos"]:
                    if isinstance(rv, dict) and rv.get("script_id"):
                        m4_rv_by_id[str(rv["script_id"])] = rv

        if vreg_path.exists():
            try:
                vreg_raw = safe_load_json(vreg_path, lambda: {})
                items = vreg_raw.get("items") if isinstance(vreg_raw, dict) else None
                if isinstance(items, dict):
                    vreg_changed = False
                    rendered_ids_to_strip_scripts: set[str] = set()
                    published_ids_to_purge: set[str] = set()

                    for sid, val in list(items.items()):
                        if not isinstance(val, dict):
                            continue
                        st_val = val.get("status")

                        # Inspect entry + optional script_payload + optional M4 sub_shots for unaccented Vietnamese
                        sample_texts: List[Any] = [val.get("title_vi"), val.get("hook_title_short_vi")]
                        sp = val.get("script_payload")
                        if isinstance(sp, dict):
                            sample_texts.extend([sp.get("title_vi"), sp.get("hook_title_short_vi")])
                            for b in sp.get("beats") or []:
                                if isinstance(b, dict):
                                    sample_texts.extend([
                                        b.get("voiceover_vi"),
                                        b.get("tts_voiceover_vi"),
                                        b.get("subtitle_text_vi"),
                                        b.get("on_screen_text_vi"),
                                    ])
                        rv_info = m4_rv_by_id.get(str(sid))
                        if isinstance(rv_info, dict):
                            sample_texts.extend([rv_info.get("title_vi"), rv_info.get("hook_title_short_vi")])
                            for ss in rv_info.get("sub_shots") or []:
                                if isinstance(ss, dict):
                                    sample_texts.extend([ss.get("tts_text_vi"), ss.get("subtitle_text_vi")])

                        if _has_unaccented_vi_texts(sample_texts):
                            print(
                                f"[GC DIACRITIC PURGE] 🧹 Xóa vĩnh viễn kịch bản/video lỗi tiếng Việt không dấu: {sid}",
                                flush=True,
                            )
                            published_ids_to_purge.add(sid)
                            rendered_ids_to_strip_scripts.add(sid)
                            mp4_p = MacOSLegacyRuntime.resolve_portable_video_path(val.get("video_path"), sid)
                            if mp4_p.exists():
                                freed_published += mp4_p.stat().st_size
                                mp4_p.unlink(missing_ok=True)
                            items.pop(sid, None)
                            vreg_changed = True
                            continue

                        if st_val == "RENDERED":
                            rendered_ids_to_strip_scripts.add(sid)
                            mp4_p = MacOSLegacyRuntime.resolve_portable_video_path(val.get("video_path"), sid)
                            if not mp4_p.exists():
                                # User manually deleted the MP4 -> purge entry & payloads completely and never re-render
                                published_ids_to_purge.add(sid)
                                items.pop(sid, None)
                                vreg_changed = True
                                continue
                            if val.get("script_payload") is not None:
                                val["script_payload"] = None
                                vreg_changed = True
                        elif st_val == "PUBLISHED":
                            published_ids_to_purge.add(sid)
                            rendered_ids_to_strip_scripts.add(sid)
                            mp4_p = MacOSLegacyRuntime.resolve_portable_video_path(val.get("video_path"), sid)
                            if mp4_p.exists():
                                freed_published += mp4_p.stat().st_size
                                mp4_p.unlink(missing_ok=True)
                            items.pop(sid, None)
                            vreg_changed = True

                    if vreg_changed:
                        atomic_write_json(vreg_path, vreg_raw)

                    # Strip rendered/published OR unaccented scripts from m2_script_drafts.json & m3_approved_scripts.json
                    for fname, list_key in (
                        ("m2_script_drafts.json", "script_drafts"),
                        ("m3_approved_scripts.json", "audited_scripts"),
                    ):
                        fpath = self.data_dir / fname
                        if fpath.exists():
                            fdata = safe_load_json(fpath, lambda: {})
                            if isinstance(fdata, dict) and isinstance(fdata.get(list_key), list):
                                old_len = len(fdata[list_key])
                                kept_list = []
                                for x in fdata[list_key]:
                                    if not isinstance(x, dict):
                                        continue
                                    x_sid = str(x.get("script_id") or "")
                                    if x_sid in rendered_ids_to_strip_scripts:
                                        continue
                                    x_texts: List[Any] = [x.get("title_vi"), x.get("hook_title_short_vi")]
                                    for b in x.get("beats") or []:
                                        if isinstance(b, dict):
                                            x_texts.extend([
                                                b.get("voiceover_vi"),
                                                b.get("tts_voiceover_vi"),
                                                b.get("subtitle_text_vi"),
                                                b.get("on_screen_text_vi"),
                                            ])
                                    if _has_unaccented_vi_texts(x_texts):
                                        print(f"[GC DIACRITIC PURGE] 🧹 Xóa bản nháp không dấu `{x_sid}` khỏi {fname}", flush=True)
                                        continue
                                    kept_list.append(x)
                                if len(kept_list) != old_len:
                                    fdata[list_key] = kept_list
                                    atomic_write_json(fpath, fdata)

                    # Clean up M4 output & M5 payloads/manifest for purged PUBLISHED or UNACCENTED videos
                    if published_ids_to_purge:
                        payloads_dir = OUTPUT_DIR / "publish_pack" / "m5_payloads"
                        if payloads_dir.exists():
                            for pid in published_ids_to_purge:
                                for pf in payloads_dir.glob(f"{pid}_*.json"):
                                    freed_published += pf.stat().st_size
                                    pf.unlink(missing_ok=True)
                        if m4_out_p.exists():
                            m4_d = safe_load_json(m4_out_p, lambda: {})
                            if isinstance(m4_d, dict) and isinstance(m4_d.get("rendered_videos"), list):
                                old_l = len(m4_d["rendered_videos"])
                                m4_d["rendered_videos"] = [
                                    rv for rv in m4_d["rendered_videos"]
                                    if isinstance(rv, dict) and rv.get("script_id") not in published_ids_to_purge
                                ]
                                if len(m4_d["rendered_videos"]) != old_l:
                                    atomic_write_json(m4_out_p, m4_d)
            except Exception:
                pass

        return {
            "freed_browser_profile_bytes": freed_profile,
            "freed_work_bytes": freed_work,
            "freed_video_cache_bytes": freed_cache,
            "freed_logs_bytes": freed_logs,
            "freed_published_bytes": freed_published,
            "total_freed_bytes": freed_profile + freed_work + freed_cache + freed_logs + freed_published,
        }


# ==============================================================================
# 4. ANTI-SPAM HUMAN BEHAVIOR ENGINE (YOUTUBE SHORTS & FACEBOOK REELS)
# ==============================================================================

class HumanBehaviorGuard:
    """Generates non-robotic timings, active-hour gates, weekly rest days, and varied comment phrasings."""

    @staticmethod
    def get_daily_active_window(
        now: Optional[dt.datetime] = None,
        state_path: Optional[Path] = None,
    ) -> Tuple[dt.datetime, dt.datetime]:
        """
        Return `(start_dt, end_dt)` for the calendar day of `now`.
        - Daily start time is RANDOMIZED each day between `06:00:00` and `09:30:00` AM (`0 .. 12,600` seconds)
          and persisted in `data/daily_quota_state.json` under `daily_active_windows[YYYY-MM-DD]` so it never
          has a fixed starting clock time across days while staying consistent within the same day.
        - Daily end time varies naturally between `22:00` and `22:30` PM.
        """
        now_dt = now or dt.datetime.now().astimezone()
        day_key = now_dt.strftime("%Y-%m-%d")
        target_state_path = state_path or (DATA_DIR / "daily_quota_state.json")

        start_offset_sec: Optional[int] = None
        end_offset_sec: Optional[int] = None

        st = safe_load_json(target_state_path, lambda: {})
        if isinstance(st, dict):
            win_map = st.get("daily_active_windows")
            if isinstance(win_map, dict) and isinstance(win_map.get(day_key), dict):
                entry = win_map[day_key]
                s_val = entry.get("start_offset_sec")
                e_val = entry.get("end_offset_sec")
                if isinstance(s_val, int) and 0 <= s_val <= 12600 and isinstance(e_val, int) and 0 <= e_val <= 1800:
                    start_offset_sec = s_val
                    end_offset_sec = e_val

        if start_offset_sec is None or end_offset_sec is None:
            # Sample truly random offset for today (or deterministic fallback for non-today test dates)
            today_real_key = dt.datetime.now().astimezone().strftime("%Y-%m-%d")
            if day_key == today_real_key:
                start_offset_sec = random.randint(0, 210 * 60)  # 06:00:00 .. 09:30:00
                if start_offset_sec % 60 == 0:
                    start_offset_sec = min(12600, start_offset_sec + random.randint(5, 52))
                end_offset_sec = random.randint(0, 30 * 60)     # 22:00:00 .. 22:30:00
                try:
                    if not isinstance(st, dict):
                        st = {}
                    win_map = st.setdefault("daily_active_windows", {})
                    if not isinstance(win_map, dict):
                        win_map = {}
                        st["daily_active_windows"] = win_map
                    base_6am = now_dt.replace(hour=6, minute=0, second=0, microsecond=0) + dt.timedelta(seconds=start_offset_sec)
                    base_10pm = now_dt.replace(hour=22, minute=0, second=0, microsecond=0) + dt.timedelta(seconds=end_offset_sec)
                    win_map[day_key] = {
                        "start_offset_sec": start_offset_sec,
                        "end_offset_sec": end_offset_sec,
                        "start_hm": base_6am.strftime("%H:%M:%S"),
                        "end_hm": base_10pm.strftime("%H:%M:%S"),
                    }
                    atomic_write_json(target_state_path, st)
                except Exception:
                    pass
            else:
                day_seed = int(hashlib.sha256(f"human_window_v2::{day_key}".encode("utf-8")).hexdigest()[:8], 16)
                # Ensure full coverage across 06:05..09:30 (300..12600s)
                start_offset_sec = 300 + (day_seed % (12600 - 300 + 1))
                end_offset_sec = (day_seed >> 12) % (30 * 60 + 1)

        start_dt = now_dt.replace(hour=6, minute=0, second=0, microsecond=0) + dt.timedelta(seconds=int(start_offset_sec))
        end_dt = now_dt.replace(hour=22, minute=0, second=0, microsecond=0) + dt.timedelta(seconds=int(end_offset_sec))
        return start_dt, end_dt

    @staticmethod
    def is_within_human_active_hours(
        now: Optional[dt.datetime] = None,
        state_path: Optional[Path] = None,
    ) -> Tuple[bool, int]:
        """
        Return `(is_active_now, seconds_until_active_window)`.
        Active window starts at a daily randomized time between `06:00` and `09:30` AM
        and ends around `22:00 – 22:30` PM local time.
        """
        now_dt = now or dt.datetime.now().astimezone()
        start_dt, end_dt = HumanBehaviorGuard.get_daily_active_window(now=now_dt, state_path=state_path)

        if start_dt <= now_dt <= end_dt:
            return True, 0

        if now_dt < start_dt:
            return False, max(60, int((start_dt - now_dt).total_seconds()))

        tomorrow_start, _ = HumanBehaviorGuard.get_daily_active_window(
            now=now_dt + dt.timedelta(days=1),
            state_path=state_path,
        )
        return False, max(60, int((tomorrow_start - now_dt).total_seconds()))

    @staticmethod
    def get_weekly_rest_day_info(
        now: Optional[dt.datetime] = None,
        state_path: Optional[Path] = None,
    ) -> Tuple[bool, int, str, int]:
        """
        Pick and persist 1 random rest day per ISO week (`Monday=0 .. Sunday=6`).
        On the rest day, both `M4` (video rendering) and `M5` (social uploading) pause for the entire day.
        Returns `(is_rest_day_today, rest_weekday_idx, rest_weekday_name_vi, seconds_until_tomorrow_start)`.
        """
        now_dt = now or dt.datetime.now().astimezone()
        iso_year, iso_week, _ = now_dt.isocalendar()
        week_key = f"{iso_year}-W{iso_week:02d}"
        target_state_path = state_path or (DATA_DIR / "daily_quota_state.json")

        rest_weekday: Optional[int] = None
        st = safe_load_json(target_state_path, lambda: {})
        if isinstance(st, dict):
            rest_map = st.get("weekly_rest_days")
            if isinstance(rest_map, dict):
                val = rest_map.get(week_key)
                if isinstance(val, int) and 0 <= val <= 6:
                    rest_weekday = val
                elif isinstance(val, dict) and isinstance(val.get("weekday"), int) and 0 <= val["weekday"] <= 6:
                    rest_weekday = int(val["weekday"])

        if rest_weekday is None:
            rest_weekday = random.randint(0, 6)
            try:
                if not isinstance(st, dict):
                    st = {}
                rest_map = st.setdefault("weekly_rest_days", {})
                if not isinstance(rest_map, dict):
                    rest_map = {}
                    st["weekly_rest_days"] = rest_map
                rest_map[week_key] = {
                    "weekday": rest_weekday,
                    "weekday_name_vi": WEEKDAY_NAMES_VI[rest_weekday],
                    "selected_at": now_dt.isoformat(timespec="seconds"),
                }
                atomic_write_json(target_state_path, st)
            except Exception:
                pass

        rest_name_vi = WEEKDAY_NAMES_VI[rest_weekday]
        is_rest_today = now_dt.weekday() == rest_weekday
        if not is_rest_today:
            return False, rest_weekday, rest_name_vi, 0

        tomorrow_start, _ = HumanBehaviorGuard.get_daily_active_window(
            now=now_dt + dt.timedelta(days=1),
            state_path=target_state_path,
        )
        wait_sec = max(60, int((tomorrow_start - now_dt).total_seconds()))
        return True, rest_weekday, rest_name_vi, wait_sec

    @staticmethod
    def sample_human_cooldown_seconds(min_floor_sec: int = 7200) -> int:
        """
        Sample a natural, non-periodic rest duration strictly `>= 7200s` (2 hours),
        typically `2h12m` to `3h55m` with irregular seconds (never exact `:00`).
        """
        low = max(min_floor_sec, HUMAN_COOLDOWN_MIN_SECONDS)
        val = int(random.triangular(low, HUMAN_COOLDOWN_MAX_SECONDS, HUMAN_COOLDOWN_MODE_SECONDS))
        if val % 60 == 0:
            val += random.randint(7, 53)
        return max(min_floor_sec, val)

    @staticmethod
    def sample_post_upload_comment_delay_sec() -> float:
        """Human delay (24s to 62s) between publishing a video and posting the first affiliate comment."""
        return round(random.uniform(24.0, 62.0), 1)

    @staticmethod
    def sample_cross_platform_stagger_sec() -> float:
        """Delay (35s to 95s) between uploading to YouTube Shorts and uploading to Facebook Reels."""
        return round(random.uniform(35.0, 95.0), 1)

    @staticmethod
    def pick_varied_pinned_comment_intro(script_id: str, platform_salt: str = "") -> str:
        """Deterministically pick 1 of 20 distinct natural Vietnamese comment intros per `(script_id, platform_salt)`."""
        key = f"{script_id}::{platform_salt}" if platform_salt else script_id
        idx = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:8], 16) % len(PINNED_COMMENT_INTROS_VI)
        return PINNED_COMMENT_INTROS_VI[idx]


# ==============================================================================
# 5. TELEGRAM REAL-TIME MONITORING & ALERT NOTIFIER (STDLIB ONLY)
# ==============================================================================

class TelegramNotifier:
    """
    Zero-dependency Telegram Bot notifier using stdlib `urllib.request`.
    Reads `TELEGRAM_BOT_TOKEN` & `TELEGRAM_CHAT_ID` from env, `.env`, or `config/platform_credentials.json`.
    """

    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
        env_path: Path = PROJECT_ROOT / ".env",
        credentials_path: Path = PROJECT_ROOT / "config" / "platform_credentials.json",
    ) -> None:
        self.env_path = env_path
        self.credentials_path = credentials_path
        env_vals = self._read_dotenv(env_path)
        creds = safe_load_json(credentials_path, lambda: {})
        tg_creds = creds.get("telegram") if isinstance(creds, dict) else {}
        if not isinstance(tg_creds, dict):
            tg_creds = {}

        self.bot_token = (
            (bot_token or "").strip()
            or os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
            or env_vals.get("TELEGRAM_BOT_TOKEN", "").strip()
            or str(tg_creds.get("bot_token") or "").strip()
        )
        self.chat_id = (
            (chat_id or "").strip()
            or os.environ.get("TELEGRAM_CHAT_ID", "").strip()
            or env_vals.get("TELEGRAM_CHAT_ID", "").strip()
            or str(tg_creds.get("chat_id") or "").strip()
        )

    @staticmethod
    def _read_dotenv(env_path: Path) -> Dict[str, str]:
        parsed: Dict[str, str] = {}
        if not env_path.exists():
            return parsed
        try:
            for line in env_path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith("#") or "=" not in s:
                    continue
                k, v = s.split("=", 1)
                parsed[k.strip()] = v.strip().strip('"').strip("'")
        except OSError:
            pass
        return parsed

    def is_configured(self) -> bool:
        return bool(self.bot_token and self.chat_id)

    def send_message(self, text: str, disable_web_page_preview: bool = True) -> Tuple[bool, str]:
        """Send a plain-text Telegram message; never raises on network failure."""
        if not self.is_configured():
            return False, "TELEGRAM_BOT_TOKEN hoặc TELEGRAM_CHAT_ID chưa được cấu hình."

        msg_text = str(text or "").strip()
        if not msg_text:
            return False, "Empty message text."
        if len(msg_text) > 4000:
            msg_text = msg_text[:3990] + "\n...(cắt bớt)"

        url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        payload = json.dumps(
            {
                "chat_id": self.chat_id,
                "text": msg_text,
                "disable_web_page_preview": bool(disable_web_page_preview),
            },
            ensure_ascii=False,
        ).encode("utf-8")

        req = urllib.request.Request(
            url,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            ca_bundle = MacOSLegacyRuntime.resolve_ca_bundle_path()
            ssl_ctx = ssl.create_default_context(cafile=ca_bundle) if ca_bundle else ssl.create_default_context()
            with urllib.request.urlopen(req, timeout=15.0, context=ssl_ctx) as resp:
                raw = resp.read().decode("utf-8", errors="replace")
            data = json.loads(raw)
            if data.get("ok"):
                msg_id = str((data.get("result") or {}).get("message_id") or "")
                return True, msg_id
            return False, f"Telegram API returned ok=false: {raw[:300]}"
        except urllib.error.HTTPError as exc:
            err_body = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            return False, f"HTTP {exc.code}: {err_body[:300]}"
        except Exception as exc:
            return False, str(exc)

    def notify_publish_result(
        self,
        script_id: str,
        title_vi: str,
        yt_status: str,
        yt_video_id: str,
        fb_status: str,
        fb_reel_id: str,
        short_links: List[str],
    ) -> Tuple[bool, str]:
        """Format and send an upload/comment result notification after M5 live upload."""
        has_err = "ERROR" in (yt_status or "") or "ERROR" in (fb_status or "")
        all_ok = (yt_status == "PUBLISHED" or "ALREADY_PUBLISHED" in yt_status) and (
            fb_status == "PUBLISHED" or "ALREADY_PUBLISHED" in fb_status
        )
        if all_ok and not has_err:
            header = "✅ ĐĂNG VIDEO TỰ ĐỘNG THÀNH CÔNG (YT + FB)"
        elif has_err:
            header = "⚠️ BÁO CÁO ĐĂNG VIDEO (CÓ LỖI CẦN KIỂM TRA)"
        else:
            header = "ℹ️ CẬP NHẬT TRẠNG THÁI ĐĂNG VIDEO (M5)"

        yt_line = (
            f"https://youtube.com/shorts/{yt_video_id}"
            if yt_video_id
            else (yt_status or "N/A")
        )
        fb_line = (
            f"https://www.facebook.com/reel/{fb_reel_id}"
            if fb_reel_id
            else (fb_status or "N/A")
        )

        lines: List[str] = [
            f"🎬 Mã video: {script_id}",
            f"📝 Tiêu đề: {title_vi}",
            f"▶️ YouTube Shorts: {yt_line}",
            f"📘 Facebook Reels: {fb_line}",
        ]
        clean_links = [u.strip() for u in (short_links or []) if u and u.strip()]
        if clean_links:
            lines.append("🔗 Link Shopee Mall rút gọn:")
            for u in clean_links:
                lines.append(f"  • {u}")

        return self.notify_pipeline_event(header, lines)

    def notify_pipeline_event(self, title: str, lines: List[str]) -> Tuple[bool, str]:
        """Send a structured multi-line event alert to Telegram."""
        now_str = dt.datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        body_lines = [title.strip(), f"🕒 Lúc: {now_str}", ""]
        body_lines.extend(line for line in lines if line is not None)
        return self.send_message("\n".join(body_lines), disable_web_page_preview=True)
