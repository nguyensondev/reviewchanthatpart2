"""
Module M4: 100% Autonomous 9:16 Video Producer — Layout B1 (`pipeline/m4_video_producer.py`).

Responsibilities (Black-Box Contract):
  1. Reads `data/m3_approved_scripts.json` plus any queued `SCRIPT_APPROVED` scripts in `data/video_registry.json`.
  2. Enforces `DailyQuotaManager` limits:
     - `MAX_VIDEOS_PER_DAY = 3` (max 3 videos per calendar day).
     - `MIN_RENDER_COOLDOWN_SECONDS = 7200` (rest at least 2 hours after each rendered video).
  3. Step M4.1: Official Brand Video Resolver & Downloader (`data/m4_video_cache/` & `data/brand_official_cache/`),
     always stripping original audio (`-an`) and rejecting third-party watermarked clips.
  4. Step M4.2: Dense 0.5s (`fps=2`) OpenCV Faceless & Caption-Free Action Selector (avoids frontal faces,
     burned-in promotional text bands, and logo end-cards; prioritizes verified clean intervals).
  5. Step M4.3: Dual-gender TikTok TTS (`BV074_streaming` Female & `BV075_streaming` Male) randomized
     and balanced across daily videos, strictly locked to 1 voice per video (`single_voice_locked=True`),
     with same-gender Edge-TTS fallback (`vi-VN-HoaiMyNeural` / `vi-VN-NamMinhNeural`).
  6. Step M4.4: Layout `B1` Renderer:
     - Single centered 16:9 product frame (`1080x608` at `y=656..1264`, zero-zoom, zero-crop) over a
       `1080x1920` ambient blurred background of the exact same shot (`boxblur=16:2,eq=brightness=-0.10:saturation=0.9`),
       without black bars.
     - Opening Hook Title (`0..2s`): `Fontsize=64`, slide-up `200ms`, strictly `<= 30` characters.
     - Subtitles: `Fontsize=36` (10% smaller than demo's 40) at `y=1380` with gold keyword highlights.
     - PIL Rounded Graphic Cards in top zone (`y=310..360`): Spec Card (Beat 2), Honest Caveat Card with `⚠️` (Beat 4),
       and Price + CTA Card (Beat 5), plus `6px` top progress bar.
     - Upbeat copyright-free BGM (1 of 4 tracks in `assets/bgm/` picked at random), sidechain-ducked under
       voiceover at subtle low volume + subtle `whoosh`/`pop` SFX, normalized to `-14 LUFS`.
  7. Updates `data/video_registry.json` (`status = "RENDERED"`, never `"PUBLISHED"`) and updates
     `data/sku_registry.json` (`has_solo_video` / `has_combo_video` -> `RETIRED_EXHAUSTED` when both True).
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import datetime as dt
import json
import random
import re
import shutil
import ssl
import subprocess
import time
import urllib.request
import wave
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

import cv2
import edge_tts
import yt_dlp
from PIL import Image, ImageDraw, ImageFont, ImageOps

from pipeline.bgm_library import ensure_bgm_library, pick_random_bgm_track
from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import (
    MacOSLegacyRuntime,
    StorageGarbageCollector,
    atomic_write_json,
    atomic_write_text,
    safe_load_json,
)
from pipeline.daily_quota import (
    MAX_VIDEOS_PER_DAY,
    MIN_RENDER_COOLDOWN_SECONDS,
    DailyQuotaManager,
)
from pipeline.schemas_m1_5 import normalize_sku_key
from pipeline.schemas_m3 import AuditedBeatSegment, AuditedScriptItem, M3ApprovedScriptsOutput
from pipeline.schemas_m_trend import load_active_trend_opportunities
from pipeline.schemas_m4 import (
    M4ProductionOutput,
    RenderedVideoItem,
    SocialPostPack,
    SubShotClauseItem,
)
from pipeline.shared_utils import (
    check_vietnamese_diacritics,
    enforce_short_hook_title,
    format_vnd_short,
    normalize_vietnamese_for_tts,
    truncate_words_clean,
)
from pipeline.sku_registry import SKURegistryManager
from pipeline.video_registry import VideoRegistryManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_M3_PATH = DATA_DIR / "m3_approved_scripts.json"
OUTPUT_M4_JSON_PATH = DATA_DIR / "m4_production_output.json"
VIDEO_CACHE_DIR = DATA_DIR / "m4_video_cache"
BRAND_OFFICIAL_CACHE_DIR = DATA_DIR / "brand_official_cache"
WORK_DIR = DATA_DIR / "m4_work"
OUTPUT_VIDEOS_DIR = PROJECT_ROOT / "output" / "videos"
BGM_DIR = PROJECT_ROOT / "assets" / "bgm"
SKU_THUMBS_DIR = PROJECT_ROOT / "assets" / "sku_thumbs"

FONT_BOLD_PATH = MacOSLegacyRuntime.resolve_font_path(bold=True)
FONT_REG_PATH = MacOSLegacyRuntime.resolve_font_path(bold=False)
FFMPEG_THREADS = str(MacOSLegacyRuntime.optimal_ffmpeg_threads())
MacOSLegacyRuntime.ensure_runtime_path()
_CA_BUNDLE_PATH = MacOSLegacyRuntime.resolve_ca_bundle_path()
_SSL_CTX = ssl.create_default_context(cafile=_CA_BUNDLE_PATH) if _CA_BUNDLE_PATH else ssl.create_default_context()
MAX_CONTINUOUS_SHOT_SEC = 3.6

TIKTOK_TTS_API_URL = "https://ottsy.weilbyte.dev/api/generation"
GEMINI_TTS_MODEL = "gemini-2.5-flash-preview-tts"
GEMINI_VOICE_MAP: Dict[str, Tuple[str, str]] = {
    "gemini_aoede": (
        "Aoede",
        "Đọc bằng giọng nữ tiếng Việt tự nhiên, ấm áp, dễ nghe như người bạn chia sẻ kinh nghiệm mua đồ thật. "
        "Không tạo tiếng hít thở lớn hay tiếng lấy hơi rõ, phát âm tròn vành rõ chữ, ngắt nghỉ tự nhiên đúng dấu câu:\n",
    ),
    "gemini_puck": (
        "Puck",
        "Đọc bằng giọng nam reviewer công nghệ tiếng Việt trẻ trung, trầm ấm, gần gũi và điềm đạm dễ nghe. "
        "Không tạo tiếng hít thở lớn hay tiếng lấy hơi rõ, phát âm chuẩn tiếng Việt, ngắt nghỉ tự nhiên đúng dấu câu:\n",
    ),
}
# Primary pool combines Gemini 2.5 Flash TTS (Aoede/Puck) + TikTok TTS (BV074/BV075) randomly per gender;
# Microsoft Edge Neural TTS (vi-VN-HoaiMyNeural / vi-VN-NamMinhNeural) serves as fallback.
VOICE_POOL: List[Tuple[str, Literal["FEMALE", "MALE"], str]] = [
    ("BV074_streaming", "FEMALE", "vi-VN-HoaiMyNeural"),
    ("BV075_streaming", "MALE", "vi-VN-NamMinhNeural"),
]
VOICE_RATE = "+8%"

_HIGHLIGHT_PAT = re.compile(
    r"(\b(?:\d+(?:[.,]\d+)?(?:\s*x\s*\d+)?\s*(?:k|tr|ml|mm|cm|g|kg|W|V|độ|DPI|K|Ra\d+)|"
    r"Inox\s*\d+|USB[-\s]*C|Type[-\s]*C|Logi\s*Bolt|SmartWheel|LED|"
    r"Baseus|i-wok2|Logitech|Lift\s*Vertical|Tanita|KD-\d+|Xiaomi|Ugreen|Anker|Lock&Lock|Inochi|OEM)\b)",
    flags=re.IGNORECASE,
)


@dataclass
class SampledFrameMeta:
    timestamp: float
    has_face: bool
    has_caption_or_card: bool
    face_ratio: float
    edge_density: float
    sharpness: float
    std_luma: float
    mean_luma: float = 115.0


@dataclass
class VideoScanSummary:
    video_id: str
    path: Path
    width: int
    height: int
    duration_sec: float
    samples: List[SampledFrameMeta]
    verified_clean_intervals: List[Tuple[float, float]] = field(default_factory=list)


def _run_cmd(cmd: List[str], cwd: Optional[Path] = None) -> str:
    res = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd else None,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    if res.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)[:400]}\n{res.stderr[-2500:]}")
    return res.stdout


def _probe_media_info(file_path: Path) -> Dict[str, float]:
    out = _run_cmd(
        [
            "ffprobe",
            "-v",
            "error",
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,codec_name:format=duration",
            "-of",
            "json",
            str(file_path),
        ]
    )
    data = json.loads(out)
    streams = data.get("streams") or []
    fmt = data.get("format") or {}
    w = int(streams[0].get("width", 0)) if streams else 0
    h = int(streams[0].get("height", 0)) if streams else 0
    dur = float(fmt.get("duration", 0.0) or 0.0)
    return {"width": w, "height": h, "duration": dur}


def _get_audio_duration(file_path: Path) -> float:
    out = _run_cmd(
        [
            "ffprobe",
            "-v",
            "error",
            "-show_entries",
            "format=duration",
            "-of",
            "default=noprint_wrappers=1:nokey=1",
            str(file_path),
        ]
    )
    return float(out.strip())


def _format_ass_time(seconds: float) -> str:
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = seconds % 60
    return f"{h}:{m:02d}:{s:05.2f}"


def _highlight_ass_keywords(text: str) -> str:
    """Highlight numeric specs and brand/model tokens in gold while keeping neutral white body."""
    gold = r"{\c&H0034C4FF&}"
    white = r"{\c&H00FFFFFF&}"
    escaped = text.replace("{", "(").replace("}", ")")
    return _HIGHLIGHT_PAT.sub(lambda m: f"{gold}{m.group(1)}{white}", escaped)


def _split_aligned_clauses(tts_text: str, sub_text: str) -> List[Tuple[str, str]]:
    """
    Split a beat's (tts_voiceover_vi, subtitle_text_vi) cleanly into 1 or 2 natural spoken clauses.
    CRITICAL RHYTHM RULE:
      1. Strictly prefer true sentence boundaries (`. ! ?`) where both sides have >= 5 words.
      2. Otherwise, allow a natural clause boundary (`, ; :`) ONLY if both sides have >= 6 words
         (preserving a trailing `,` on `t1` so M4 uses a short 0.06s breath pause instead of a hard stop).
      3. NEVER blindly chop a sentence mid-phrase at `len(words) // 2` — keep it intact as 1 natural
         spoken clause so TTS preserves human prosody and sentence meaning.
    """
    tts = " ".join(tts_text.strip().split())
    sub = " ".join(sub_text.strip().split())
    if len(tts.split()) <= 15:
        return [(tts.rstrip(",;:"), sub.rstrip(",;:"))]

    def _align_sub_split(
        sub_str: str,
        target_ratio: float,
        sep_pattern: re.Pattern[str],
        tts_sep_count: int,
        tts_best_idx: int,
    ) -> Tuple[str, str]:
        sub_seps = list(sep_pattern.finditer(sub_str))
        if len(sub_seps) == tts_sep_count and tts_best_idx < len(sub_seps):
            sm = sub_seps[tts_best_idx]
            return sub_str[: sm.start() + 1].strip().rstrip(",;:"), sub_str[sm.end() :].strip().rstrip(",;:")
        if sub_seps:
            sm = min(sub_seps, key=lambda m: abs((m.end() / max(1, len(sub_str))) - target_ratio))
            return sub_str[: sm.start() + 1].strip().rstrip(",;:"), sub_str[sm.end() :].strip().rstrip(",;:")
        any_seps = list(re.finditer(r"[.!?;:,]\s+", sub_str))
        if any_seps:
            sm = min(any_seps, key=lambda m: abs((m.end() / max(1, len(sub_str))) - target_ratio))
            return sub_str[: sm.start() + 1].strip().rstrip(",;:"), sub_str[sm.end() :].strip().rstrip(",;:")
        sw = sub_str.split()
        s_mid = max(1, min(len(sw) - 1, int(round(len(sw) * target_ratio))))
        return " ".join(sw[:s_mid]).rstrip(",;:"), " ".join(sw[s_mid:]).rstrip(",;:")

    # Priority 1: True sentence boundaries (. ! ?)
    sent_re = re.compile(r"[.!?]\s+")
    sent_seps = list(sent_re.finditer(tts))
    sent_candidates = [
        (idx, m)
        for idx, m in enumerate(sent_seps)
        if len(tts[: m.start() + 1].split()) >= 5 and len(tts[m.end() :].split()) >= 5
    ]
    if sent_candidates:
        best_idx, best_m = min(
            sent_candidates,
            key=lambda pair: abs((pair[1].end() / max(1, len(tts))) - 0.50),
        )
        t1 = tts[: best_m.start() + 1].strip()
        t2 = tts[best_m.end() :].strip()
        s1, s2 = _align_sub_split(
            sub, best_m.end() / max(1, len(tts)), sent_re, len(sent_seps), best_idx
        )
        if t1 and t2 and s1 and s2:
            return [(t1, s1), (t2, s2)]

    # Priority 2: Natural clause boundaries (, ; :) with >= 6 words on BOTH sides
    clause_re = re.compile(r"[,;:]\s+")
    clause_seps = list(clause_re.finditer(tts))
    clause_candidates = [
        (idx, m)
        for idx, m in enumerate(clause_seps)
        if len(tts[: m.start()].split()) >= 6 and len(tts[m.end() :].split()) >= 6
    ]
    if clause_candidates:
        best_idx, best_m = min(
            clause_candidates,
            key=lambda pair: abs((pair[1].end() / max(1, len(tts))) - 0.50),
        )
        # Keep trailing comma on t1 as a signal to TTS synthesis to use a brief 0.06s breath pause
        t1 = tts[: best_m.start()].strip().rstrip(",;:") + ","
        t2 = tts[best_m.end() :].strip().rstrip(",;:")
        s1, s2 = _align_sub_split(
            sub, best_m.end() / max(1, len(tts)), clause_re, len(clause_seps), best_idx
        )
        if t1 and t2 and s1 and s2:
            return [(t1, s1), (t2, s2)]

    # Priority 3: Keep single continuous sentence intact — NEVER chop mid-phrase!
    return [(tts.rstrip(",;:"), sub.rstrip(",;:"))]


def _split_utf8_chunks(text: str, max_bytes: int = 240) -> List[str]:
    """Split text cleanly on sentence (`.!?`), then clause (`,;:`), then word boundaries within `max_bytes` UTF-8."""
    clean = " ".join(text.strip().split())
    if not clean:
        return []
    if len(clean.encode("utf-8")) <= max_bytes:
        return [clean]

    sent_pieces = re.split(r"(?<=[.!?])\s+", clean)
    raw_pieces: List[str] = []
    for sp in sent_pieces:
        if len(sp.encode("utf-8")) <= max_bytes:
            raw_pieces.append(sp)
        else:
            raw_pieces.extend(re.split(r"(?<=[,;:])\s+", sp))

    words_or_pieces: List[str] = []
    for piece in raw_pieces:
        if len(piece.encode("utf-8")) <= max_bytes:
            words_or_pieces.append(piece)
        else:
            words_or_pieces.extend(piece.split())

    chunks: List[str] = []
    buf = ""
    for unit in words_or_pieces:
        cand = f"{buf} {unit}".strip() if buf else unit
        if len(cand.encode("utf-8")) <= max_bytes:
            buf = cand
        else:
            if buf:
                chunks.append(buf)
                buf = ""
            if len(unit.encode("utf-8")) <= max_bytes:
                buf = unit
            else:
                sub_buf = ""
                for ch in unit:
                    if len((sub_buf + ch).encode("utf-8")) <= max_bytes:
                        sub_buf += ch
                    else:
                        if sub_buf:
                            chunks.append(sub_buf)
                        sub_buf = ch
                buf = sub_buf
    if buf:
        chunks.append(buf)
    return chunks


def _choose_voice_for_video(
    voices_used_today: List[str],
    preferred_voice: Optional[str] = None,
) -> List[Tuple[str, Literal["FEMALE", "MALE"], str]]:
    """
    Select ordered `(primary_tiktok_voice, gender, fallback_edge_voice)` candidates.
    Randomizes between Female (`BV074_streaming`) and Male (`BV075_streaming`) while
    ensuring gender balance across the 3 daily videos.
    """
    if preferred_voice == "BV075_streaming":
        return [VOICE_POOL[1], VOICE_POOL[0]]
    if preferred_voice == "BV074_streaming":
        return [VOICE_POOL[0], VOICE_POOL[1]]

    female_cnt = sum(1 for v in voices_used_today if v in ("gemini_aoede", "BV074_streaming", "vi-VN-HoaiMyNeural"))
    male_cnt = sum(1 for v in voices_used_today if v in ("gemini_puck", "BV075_streaming", "vi-VN-NamMinhNeural"))

    if female_cnt > male_cnt:
        return [VOICE_POOL[1], VOICE_POOL[0]]
    if male_cnt > female_cnt:
        return [VOICE_POOL[0], VOICE_POOL[1]]

    pool = list(VOICE_POOL)
    random.shuffle(pool)
    return pool


def _wrap_card_text(text: str, max_chars_per_line: int = 36, max_lines: int = 3) -> List[str]:
    words = " ".join((text or "").strip().split()).split()
    if not words:
        return []
    lines: List[str] = []
    cur: List[str] = []
    for w in words:
        cand = " ".join(cur + [w])
        if len(cand) <= max_chars_per_line:
            cur.append(w)
        else:
            if cur:
                lines.append(" ".join(cur))
            cur = [w]
            if len(lines) >= max_lines:
                break
    if cur and len(lines) < max_lines:
        lines.append(" ".join(cur))
    return lines[:max_lines]


def _draw_vector_check_badge(d: ImageDraw.ImageDraw, cx: int, cy: int, r: int = 16, color: Tuple[int, int, int] = (46, 204, 113)) -> None:
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(*color, 255))
    d.line([(cx - 7, cy + 1), (cx - 2, cy + 6), (cx + 8, cy - 5)], fill=(255, 255, 255, 255), width=3)


def _draw_vector_cross_badge(d: ImageDraw.ImageDraw, cx: int, cy: int, r: int = 16, color: Tuple[int, int, int] = (235, 87, 87)) -> None:
    d.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(*color, 255))
    d.line([(cx - 6, cy - 6), (cx + 6, cy + 6)], fill=(255, 255, 255, 255), width=3)
    d.line([(cx - 6, cy + 6), (cx + 6, cy - 6)], fill=(255, 255, 255, 255), width=3)


def _select_end_card_type(
    narrative_angle: str,
    script_id: str,
) -> Literal["SCORE_VERDICT_CARD", "VERSUS_2COL_CARD", "BUY_SKIP_CARD"]:
    """Map Upgrade-1 `narrative_angle` to its matching visual End-Card in Beat 5."""
    ang = (narrative_angle or "").strip().upper()
    if ang in ("REVERSE_CAVEAT_FIRST", "ANTI_DUST_DRAWER_TEST", "DECISION_GUIDE_BY_USER_TYPE"):
        return "BUY_SKIP_CARD"
    if ang in ("SPEC_TRAP_AND_HIDDEN_COST", "MYTH_VS_FACT_TEARDOWN", "TIME_AND_EFFORT_MATH"):
        return "VERSUS_2COL_CARD"
    if ang == "FAST_25S_MICRO_LOOP":
        choices: List[Literal["SCORE_VERDICT_CARD", "VERSUS_2COL_CARD", "BUY_SKIP_CARD"]] = [
            "SCORE_VERDICT_CARD",
            "VERSUS_2COL_CARD",
            "BUY_SKIP_CARD",
        ]
        return choices[sum(ord(ch) for ch in (script_id or "s")) % 3]
    return "SCORE_VERDICT_CARD"


def _render_pil_rounded_card(
    path: Path,
    w: int,
    h: int,
    title: str,
    lines: List[str],
    accent: Tuple[int, int, int] = (255, 196, 52),
) -> None:
    """Render a translucent dark rounded overlay card (`spec_card` or `caveat_card`) with PIL (zero missing-emoji tofu)."""
    clean_title = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%]", "", title or "").strip() or "THÔNG TIN"
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle(
        [0, 0, w - 1, h - 1],
        radius=28,
        fill=(26, 33, 48, 212),
        outline=(*accent, 255),
        width=3,
    )
    d.rounded_rectangle([0, 0, 12, h - 1], radius=6, fill=(*accent, 255))
    ft = ImageFont.truetype(FONT_BOLD_PATH, 38)
    fl = ImageFont.truetype(FONT_REG_PATH, 31)
    d.text((36, 22), truncate_words_clean(clean_title, max_chars=32), font=ft, fill=(*accent, 255))
    y = 22 + 56
    for ln in lines[:3]:
        clean_ln = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%°]", "", ln or "").strip()
        d.text((36, y), "• " + truncate_words_clean(clean_ln, max_chars=42), font=fl, fill=(244, 246, 250, 255))
        y += 48
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path)


def _extract_best_sku_frame_pil(
    video_path: Path,
    samples: Optional[List[SampledFrameMeta]] = None,
) -> Optional[Image.Image]:
    """
    Extract the single cleanest, sharpest, face-free & caption-free frame from a SKU's official brand video
    for use in the uniform 9:16 channel cover thumbnail.
    """
    if not video_path or not video_path.exists():
        return None

    cap = cv2.VideoCapture(str(video_path))
    if not cap.isOpened():
        return None
    try:
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0
        dur = float(frame_count / fps) if fps > 0 and frame_count > 0 else 0.0

        candidate_times: List[float] = []
        if samples:
            clean_samples = [
                s for s in samples
                if not s.has_face
                and not s.has_caption_or_card
                and 38.0 <= s.mean_luma <= 215.0
                and s.std_luma >= 22.0
            ]
            if not clean_samples:
                clean_samples = [s for s in samples if not s.has_face and 30.0 <= s.mean_luma <= 225.0]
            clean_samples.sort(key=lambda s: s.sharpness, reverse=True)
            candidate_times = [s.timestamp for s in clean_samples[:6]]

        if not candidate_times and dur > 1.0:
            candidate_times = [dur * r for r in (0.18, 0.30, 0.44, 0.58, 0.72)]
        elif not candidate_times:
            candidate_times = [0.5]

        best_rgb = None
        best_score = -1.0
        for ts in candidate_times:
            cap.set(cv2.CAP_PROP_POS_MSEC, max(0.0, ts * 1000.0))
            ok, bgr = cap.read()
            if not ok or bgr is None:
                continue
            gray = cv2.cvtColor(bgr, cv2.COLOR_BGR2GRAY)
            luma = float(gray.mean())
            std = float(gray.std())
            if luma < 24.0 or luma > 235.0 or std < 15.0:
                continue
            sharp = float(cv2.Laplacian(gray, cv2.CV_64F).var())
            # Penalize frames with baked-in lower-third subtitles/text banners (sharp text artificially spikes Laplacian!)
            edges = cv2.Canny(gray, 110, 210)
            fh_g = gray.shape[0]
            lower_edges = float((edges[int(fh_g * 0.68) :, :] > 0).mean())
            center_edges = float((edges[int(fh_g * 0.20) : int(fh_g * 0.72), :] > 0).mean())
            text_penalty = 0.15 if lower_edges > 0.075 else (0.55 if lower_edges > 0.045 else 1.0)
            # Prefer well-exposed sharp product close-ups in center with clean bottom edge
            luma_bonus = 1.0 - abs(luma - 128.0) / 140.0
            score = (sharp * (0.6 + min(1.0, center_edges * 8.0))) * max(0.35, luma_bonus) * text_penalty
            if score > best_score:
                best_score = score
                # If minor bottom caption still exists across all frames, safely crop bottom 14% off the cover shot
                if lower_edges > 0.065 and fh_g >= 240:
                    bgr_clean = bgr[: int(bgr.shape[0] * 0.84), :, :]
                else:
                    bgr_clean = bgr
                best_rgb = cv2.cvtColor(bgr_clean, cv2.COLOR_BGR2RGB)

        if best_rgb is not None:
            return Image.fromarray(best_rgb)
    finally:
        cap.release()
    return None


def _paste_rounded_cover_photo(
    canvas: Image.Image,
    photo: Image.Image,
    box: Tuple[int, int, int, int],
    radius: int = 36,
    rim_color: Tuple[int, int, int] = (255, 196, 52),
) -> None:
    """Crop/fit `photo` strictly into `box=(x0, y0, x1, y1)` with rounded corners and uniform studio rim."""
    x0, y0, x1, y1 = box
    bw, bh = x1 - x0, y1 - y0
    fitted = ImageOps.fit(photo.convert("RGB"), (bw, bh), method=Image.Resampling.LANCZOS, centering=(0.5, 0.5))

    mask = Image.new("L", (bw, bh), 0)
    md = ImageDraw.Draw(mask)
    md.rounded_rectangle([0, 0, bw - 1, bh - 1], radius=radius, fill=255)

    # Subtle drop shadow behind the locked SKU frame
    shadow = Image.new("RGBA", (bw + 16, bh + 16), (0, 0, 0, 0))
    sd = ImageDraw.Draw(shadow)
    sd.rounded_rectangle([8, 10, bw + 8, bh + 10], radius=radius, fill=(0, 0, 0, 120))
    canvas.paste(shadow, (x0 - 8, y0 - 4), shadow)

    canvas.paste(fitted, (x0, y0), mask)

    # Clean gold-accent border so every video cover shares identical framing geometry
    d = ImageDraw.Draw(canvas)
    d.rounded_rectangle([x0, y0, x1 - 1, y1 - 1], radius=radius, outline=(*rim_color, 255), width=4)


def render_uniform_sku_cover_jpeg(
    out_jpg_path: Path,
    sku_photos: List[Image.Image],
    brand_model_badge: str,
    price_badge: str,
    hook_title_vi: str,
    domain: str = "TECH_DESK",
) -> Path:
    """
    Render a strictly uniform 9:16 (`1080x1920`) studio cover thumbnail for YouTube Shorts & Facebook Reels.
    Every single video on the channel shares the exact same SKU frame size (`900x820`), header bar,
    price badge, and bottom 2-line Hook typography — eliminating messy auto-selected frames.
    """
    w, h = 1080, 1920
    canvas = Image.new("RGBA", (w, h), (13, 19, 32, 255))
    d = ImageDraw.Draw(canvas)

    # 1. Consistent vertical studio gradient (deep navy-slate for TECH_DESK, warm charcoal-slate for HOME_KITCHEN)
    top_rgb = (16, 25, 44) if domain == "TECH_DESK" else (24, 24, 34)
    mid_rgb = (26, 39, 64) if domain == "TECH_DESK" else (38, 36, 48)
    bot_rgb = (10, 14, 24)
    for row in range(0, h, 4):
        t = row / float(h)
        if t < 0.5:
            u = t * 2.0
            r = int(top_rgb[0] * (1 - u) + mid_rgb[0] * u)
            g = int(top_rgb[1] * (1 - u) + mid_rgb[1] * u)
            b = int(top_rgb[2] * (1 - u) + mid_rgb[2] * u)
        else:
            u = (t - 0.5) * 2.0
            r = int(mid_rgb[0] * (1 - u) + bot_rgb[0] * u)
            g = int(mid_rgb[1] * (1 - u) + bot_rgb[1] * u)
            b = int(mid_rgb[2] * (1 - u) + bot_rgb[2] * u)
        d.rectangle([0, row, w, row + 4], fill=(r, g, b, 255))

    # Top thin gold brand bar
    d.rectangle([0, 0, w, 10], fill=(255, 196, 52, 255))

    # 2. Fixed Channel Authority Header (`y=52`)
    f_ch = ImageFont.truetype(FONT_BOLD_PATH, 28)
    ch_txt = "GÓC REVIEW CHÂN THỰC  •  CHÍNH HÃNG"
    ch_bb = d.textbbox((0, 0), ch_txt, font=f_ch)
    d.text(((w - (ch_bb[2] - ch_bb[0])) // 2, 54), ch_txt, font=f_ch, fill=(168, 184, 210, 255))

    # 3. Fixed Top Brand/Model + Price Pill Bar (`y=124..228`)
    clean_bm = truncate_words_clean(
        re.sub(r"[^\w\sÀ-ỹ.\-+]", " ", brand_model_badge or "").strip().upper(),
        max_chars=24,
    ) or "REVIEW THỰC TẾ"
    clean_pr = (price_badge or "").strip().upper()[:10]

    f_bm = ImageFont.truetype(FONT_BOLD_PATH, 42)
    f_pr = ImageFont.truetype(FONT_BOLD_PATH, 42)
    bm_w = d.textbbox((0, 0), clean_bm, font=f_bm)[2]
    pr_w = d.textbbox((0, 0), clean_pr, font=f_pr)[2] if clean_pr else 0

    pill_h = 84
    pill_y0 = 128
    if clean_pr:
        gap = 18
        total_w = (bm_w + 56) + gap + (pr_w + 52)
        start_x = max(56, (w - total_w) // 2)
        bm_box = [start_x, pill_y0, start_x + bm_w + 56, pill_y0 + pill_h]
        pr_box = [bm_box[2] + gap, pill_y0, bm_box[2] + gap + pr_w + 52, pill_y0 + pill_h]
        d.rounded_rectangle(bm_box, radius=24, fill=(30, 42, 68, 240), outline=(96, 120, 162, 220), width=2)
        d.text((bm_box[0] + 28, pill_y0 + 18), clean_bm, font=f_bm, fill=(255, 255, 255, 255))
        d.rounded_rectangle(pr_box, radius=24, fill=(255, 196, 52, 255))
        d.text((pr_box[0] + 26, pill_y0 + 18), clean_pr, font=f_pr, fill=(18, 22, 34, 255))
    else:
        bm_box = [(w - (bm_w + 64)) // 2, pill_y0, (w + (bm_w + 64)) // 2, pill_y0 + pill_h]
        d.rounded_rectangle(bm_box, radius=24, fill=(30, 42, 68, 240), outline=(255, 196, 52, 220), width=2)
        d.text((bm_box[0] + 32, pill_y0 + 18), clean_bm, font=f_bm, fill=(255, 255, 255, 255))

    # 4. Fixed-Size Locked SKU Product Showcase Box (`x=90..990, y=266..1116` -> exact 900x850 px!)
    valid_photos = [p for p in sku_photos if p is not None]
    if len(valid_photos) >= 2:
        # Combo 2 SKU: 2 strictly equal stacked frames (900x410 each) + center '+' gold badge
        _paste_rounded_cover_photo(canvas, valid_photos[0], (90, 266, 990, 676), radius=34)
        _paste_rounded_cover_photo(canvas, valid_photos[1], (90, 706, 990, 1116), radius=34)
        cx, cy = 540, 691
        d.ellipse([cx - 38, cy - 38, cx + 38, cy + 38], fill=(255, 196, 52, 255), outline=(18, 24, 38, 255), width=5)
        f_plus = ImageFont.truetype(FONT_BOLD_PATH, 54)
        pb = d.textbbox((0, 0), "+", font=f_plus)
        d.text((cx - (pb[2] - pb[0]) // 2, cy - (pb[3] - pb[1]) // 2 - 6), "+", font=f_plus, fill=(18, 24, 38, 255))
    elif len(valid_photos) == 1:
        _paste_rounded_cover_photo(canvas, valid_photos[0], (90, 266, 990, 1116), radius=38)
    else:
        d.rounded_rectangle([90, 266, 990, 1116], radius=38, fill=(28, 38, 58, 255), outline=(255, 196, 52, 255), width=4)

    # 5. Fixed Bottom Bold 2-Line Hook Title (`y=1180..1490`)
    clean_hook = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%]", "", (hook_title_vi or "").strip()).upper()
    hook_lines = _wrap_card_text(clean_hook, max_chars_per_line=18, max_lines=2) or ["CÓ ĐÁNG MUA?"]
    f_hk = ImageFont.truetype(FONT_BOLD_PATH, 78)
    hk_y = 1205 if len(hook_lines) == 2 else 1255
    for idx_h, h_ln in enumerate(hook_lines):
        hb = d.textbbox((0, 0), h_ln, font=f_hk)
        hx = (w - (hb[2] - hb[0])) // 2
        fill_col = (255, 255, 255, 255) if idx_h == 0 else (255, 204, 64, 255)
        # Crisp dark shadow for legibility
        d.text((hx + 3, hk_y + 4), h_ln, font=f_hk, fill=(0, 0, 0, 210))
        d.text((hx, hk_y), h_ln, font=f_hk, fill=fill_col)
        hk_y += 104

    # 6. Fixed Bottom Trust Pill (`y=1475..1545`, safely above YouTube Shorts / FB Reels bottom UI!)
    trust_txt = "REVIEW THỰC TẾ  •  THÔNG SỐ CHUẨN HÃNG"
    f_tr = ImageFont.truetype(FONT_BOLD_PATH, 28)
    tb = d.textbbox((0, 0), trust_txt, font=f_tr)
    tw = tb[2] - tb[0]
    tx0 = (w - (tw + 64)) // 2
    d.rounded_rectangle([tx0, 1480, tx0 + tw + 64, 1546], radius=20, fill=(32, 46, 72, 235), outline=(88, 112, 154, 220), width=2)
    d.text((tx0 + 32, 1496), trust_txt, font=f_tr, fill=(224, 232, 246, 255))

    out_jpg_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_jpg = out_jpg_path.with_suffix(".tmp.jpg")
    canvas.convert("RGB").save(tmp_jpg, format="JPEG", quality=90, optimize=True)
    tmp_jpg.replace(out_jpg_path)
    return out_jpg_path


def _render_pil_score_verdict_card(
    path: Path,
    score_10: float,
    brand_model_label: str,
    verdict_line_vi: str,
    price_label: str,
) -> None:
    """Upgrade-1 Card #1 (`SCORE_VERDICT_CARD`, 840x290): Real M1 Score /10 + Practical Verdict + Price."""
    w, h = 840, 290
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle(
        [0, 0, w - 1, h - 1],
        radius=30,
        fill=(22, 29, 44, 222),
        outline=(255, 196, 52, 255),
        width=3,
    )
    # Left gold score badge box
    d.rounded_rectangle(
        [22, 22, 240, h - 22],
        radius=22,
        fill=(38, 49, 72, 245),
        outline=(255, 196, 52, 180),
        width=2,
    )
    clamped = min(9.6, max(8.0, round(float(score_10), 1)))
    d.text((44, 46), f"{clamped:.1f}", font=ImageFont.truetype(FONT_BOLD_PATH, 78), fill=(255, 196, 52, 255))
    d.text((164, 82), "/10", font=ImageFont.truetype(FONT_BOLD_PATH, 32), fill=(210, 218, 232, 255))
    d.text((44, 152), "ĐIỂM THỰC TẾ", font=ImageFont.truetype(FONT_BOLD_PATH, 22), fill=(235, 240, 248, 255))
    d.text((52, 186), "Kiểm chứng M1", font=ImageFont.truetype(FONT_REG_PATH, 20), fill=(175, 188, 208, 255))

    # Right verdict & price content
    d.rounded_rectangle([268, 26, 610, 70], radius=14, fill=(39, 174, 96, 235))
    d.text((286, 35), "THỰC DỤNG • ĐÁNG TIỀN", font=ImageFont.truetype(FONT_BOLD_PATH, 24), fill=(255, 255, 255, 255))

    clean_bm = re.sub(r"[^\w\sÀ-ỹ.\-+]", " ", brand_model_label or "").strip()[:28]
    d.text((268, 80), clean_bm or "Sản phẩm chính hãng", font=ImageFont.truetype(FONT_BOLD_PATH, 33), fill=(255, 255, 255, 255))

    clean_verdict = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%°]", "", verdict_line_vi or "").strip()
    wrapped_v = _wrap_card_text(clean_verdict, max_chars_per_line=40, max_lines=2) or ["Hoàn thiện chắc chắn, đúng thông số"]
    for idx_v, v_ln in enumerate(wrapped_v[:2]):
        d.text((268, 124 + idx_v * 30), v_ln, font=ImageFont.truetype(FONT_REG_PATH, 24), fill=(218, 226, 238, 255))

    d.line([(268, 192), (w - 30, 192)], fill=(70, 84, 110, 200), width=2)
    d.text((268, 206), f"Giá tham khảo: {price_label[:12]}", font=ImageFont.truetype(FONT_BOLD_PATH, 34), fill=(255, 196, 52, 255))
    d.text((268, 248), "Xem chỗ mua chính hãng ở bình luận / Bio", font=ImageFont.truetype(FONT_REG_PATH, 22), fill=(195, 205, 220, 255))

    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path)


def _render_pil_versus_2col_card(
    path: Path,
    cheap_price_label: str,
    genuine_brand_label: str,
    genuine_price_label: str,
    genuine_highlight_vi: str,
) -> None:
    """Upgrade-1 Card #2 (`VERSUS_2COL_CARD`, 840x290): Cheap Generic Trap vs Official Brand."""
    w, h = 840, 290
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, w - 1, h - 1], radius=30, fill=(20, 26, 40, 224), outline=(90, 106, 136, 230), width=2)

    # Left column: Cheap Generic / OEM
    d.rounded_rectangle([18, 18, 408, h - 18], radius=22, fill=(46, 28, 34, 225), outline=(235, 87, 87, 220), width=2)
    _draw_vector_cross_badge(d, 48, 50, r=16)
    d.text((74, 34), "HÀNG TRÔI NỔI", font=ImageFont.truetype(FONT_BOLD_PATH, 27), fill=(255, 138, 138, 255))
    d.text((36, 80), cheap_price_label[:16], font=ImageFont.truetype(FONT_BOLD_PATH, 40), fill=(240, 180, 180, 255))
    d.text((36, 142), "• Dễ nổ thông số ảo", font=ImageFont.truetype(FONT_REG_PATH, 25), fill=(228, 214, 214, 255))
    d.text((36, 182), "• Vật liệu ọp ẹp, mau hỏng", font=ImageFont.truetype(FONT_REG_PATH, 25), fill=(228, 214, 214, 255))
    d.text((36, 224), "• Không bảo hành hãng", font=ImageFont.truetype(FONT_REG_PATH, 24), fill=(200, 180, 180, 255))

    # Right column: Official Brand Winner
    d.rounded_rectangle([428, 18, w - 18, h - 18], radius=22, fill=(24, 48, 42, 232), outline=(46, 204, 113, 245), width=3)
    _draw_vector_check_badge(d, 458, 50, r=16)
    clean_b = re.sub(r"[^\w\sÀ-ỹ.\-+]", " ", genuine_brand_label or "").strip()[:17] or "CHÍNH HÃNG"
    d.text((484, 34), clean_b.upper(), font=ImageFont.truetype(FONT_BOLD_PATH, 27), fill=(110, 242, 165, 255))
    d.text((448, 80), genuine_price_label[:14], font=ImageFont.truetype(FONT_BOLD_PATH, 44), fill=(255, 196, 52, 255))

    clean_hl = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%°]", "", genuine_highlight_vi or "").strip()
    hl_short = (_wrap_card_text(clean_hl, max_chars_per_line=24, max_lines=1) or ["Chuẩn thông số thật"])[0]
    d.text((448, 142), f"• {hl_short}", font=ImageFont.truetype(FONT_REG_PATH, 25), fill=(242, 250, 245, 255))
    d.text((448, 182), "• Hoàn thiện bền, an tâm", font=ImageFont.truetype(FONT_REG_PATH, 25), fill=(242, 250, 245, 255))
    d.text((448, 224), "• Gian hàng Mall ở Bio/Cmt", font=ImageFont.truetype(FONT_BOLD_PATH, 23), fill=(255, 214, 102, 255))

    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path)


def _render_pil_buy_skip_card(
    path: Path,
    price_label: str,
    buy_if_vi: str,
    skip_if_vi: str,
) -> None:
    """Upgrade-1 Card #3 (`BUY_SKIP_CARD`, 840x290): Screenshot-friendly 'NÊN MUA NẾU / CÂN NHẮC NẾU' card."""
    w, h = 840, 290
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle([0, 0, w - 1, h - 1], radius=30, fill=(22, 29, 44, 224), outline=(255, 196, 52, 255), width=3)

    # Header bar
    d.text((34, 22), "CHỐT NHANH TRƯỚC KHI MUA", font=ImageFont.truetype(FONT_BOLD_PATH, 30), fill=(255, 196, 52, 255))
    d.rounded_rectangle([596, 16, w - 26, 66], radius=16, fill=(46, 58, 84, 245), outline=(255, 196, 52, 200), width=2)
    f_p = ImageFont.truetype(FONT_BOLD_PATH, 30)
    p_txt = price_label[:12]
    p_bbox = d.textbbox((0, 0), p_txt, font=f_p)
    p_w = p_bbox[2] - p_bbox[0]
    p_x = 596 + max(12, ((w - 26 - 596) - p_w) // 2)
    d.text((p_x, 23), p_txt, font=f_p, fill=(255, 214, 92, 255))
    d.line([(34, 76), (w - 34, 76)], fill=(68, 82, 108, 200), width=2)

    # Row 1: NÊN MUA
    d.rounded_rectangle([32, 96, 206, 146], radius=14, fill=(39, 174, 96, 240))
    _draw_vector_check_badge(d, 56, 121, r=13, color=(24, 128, 68))
    d.text((76, 106), "NÊN MUA", font=ImageFont.truetype(FONT_BOLD_PATH, 24), fill=(255, 255, 255, 255))
    clean_buy = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%°]", "", buy_if_vi or "").strip()
    buy_lines = _wrap_card_text(clean_buy, max_chars_per_line=38, max_lines=2) or ["Cần độ bền ổn định & dùng hàng ngày"]
    for idx_l, ln in enumerate(buy_lines[:2]):
        d.text((224, 96 + idx_l * 34), ln, font=ImageFont.truetype(FONT_REG_PATH, 26), fill=(242, 248, 244, 255))

    # Row 2: CÂN NHẮC
    d.rounded_rectangle([32, 186, 206, 236], radius=14, fill=(230, 126, 34, 240))
    d.text((52, 196), "! CÂN NHẮC", font=ImageFont.truetype(FONT_BOLD_PATH, 23), fill=(255, 255, 255, 255))
    clean_skip = re.sub(r"[^\w\sÀ-ỹ.,:;!?/()~+\-&%°]", "", skip_if_vi or "").strip()
    skip_lines = _wrap_card_text(clean_skip, max_chars_per_line=38, max_lines=2) or ["Xem kỹ kích thước & nhu cầu trước khi đặt"]
    for idx_l, ln in enumerate(skip_lines[:2]):
        d.text((224, 186 + idx_l * 34), ln, font=ImageFont.truetype(FONT_REG_PATH, 26), fill=(255, 232, 210, 255))

    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path)


def _render_pil_price_card(
    path: Path,
    price_label: str,
    subtitle_label: str = "Link chính hãng ở giỏ hàng",
) -> None:
    """Render the Beat 5 Price + CTA rounded card with PIL."""
    w, h = 660, 210
    img = Image.new("RGBA", (w, h), (0, 0, 0, 0))
    d = ImageDraw.Draw(img)
    d.rounded_rectangle(
        [0, 0, w - 1, h - 1],
        radius=30,
        fill=(34, 42, 58, 202),
        outline=(255, 196, 52, 255),
        width=3,
    )
    d.text((36, 20), "Giá tham khảo", font=ImageFont.truetype(FONT_REG_PATH, 32), fill=(220, 220, 220, 255))
    d.text((36, 64), price_label[:14], font=ImageFont.truetype(FONT_BOLD_PATH, 76), fill=(255, 196, 52, 255))
    d.text((320, 102), subtitle_label[:24], font=ImageFont.truetype(FONT_BOLD_PATH, 28), fill=(255, 255, 255, 255))
    path.parent.mkdir(parents=True, exist_ok=True)
    img.save(path)


def _build_sfx_files(work_dir: Path) -> Tuple[Path, Path]:
    """Synthesize 100% copyright-free subtle `whoosh.wav` and `pop.wav` SFX via FFmpeg lavfi."""
    whoosh = work_dir / "sfx_whoosh.wav"
    pop = work_dir / "sfx_pop.wav"
    if not whoosh.exists() or whoosh.stat().st_size < 1000:
        _run_cmd(
            [
                "ffmpeg",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "anoisesrc=color=pink:d=0.42:a=0.6",
                "-af",
                "highpass=f=500,lowpass=f=5000,afade=t=in:d=0.18,afade=t=out:st=0.2:d=0.22",
                "-ar",
                "44100",
                "-ac",
                "2",
                str(whoosh),
            ]
        )
    if not pop.exists() or pop.stat().st_size < 1000:
        _run_cmd(
            [
                "ffmpeg",
                "-y",
                "-f",
                "lavfi",
                "-i",
                "sine=f=880:d=0.09",
                "-af",
                "afade=t=out:st=0.01:d=0.08",
                "-ar",
                "44100",
                "-ac",
                "2",
                str(pop),
            ]
        )
    return whoosh, pop


class M4VideoProducerEngine:
    """Renders M3-approved scripts into Layout B1 9:16 MP4 videos under daily quota & 2h cooldown limits."""

    def __init__(
        self,
        cache_dir: Path = VIDEO_CACHE_DIR,
        brand_cache_dir: Path = BRAND_OFFICIAL_CACHE_DIR,
        work_dir: Path = WORK_DIR,
        output_dir: Path = OUTPUT_VIDEOS_DIR,
        bgm_dir: Path = BGM_DIR,
    ) -> None:
        self.cache_dir = cache_dir
        self.brand_cache_dir = brand_cache_dir
        self.work_dir = work_dir
        self.output_dir = output_dir
        self.bgm_dir = bgm_dir
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.brand_cache_dir.mkdir(parents=True, exist_ok=True)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        ensure_bgm_library(self.bgm_dir)
        self.brand_gate = OfficialBrandVideoGate()
        self.video_registry = VideoRegistryManager()
        self.sku_registry = SKURegistryManager()
        self._scan_cache: Dict[str, VideoScanSummary] = {}

    def _ensure_source_video(self, video_id: str, url: str) -> Optional[Path]:
        """Step M4.1: Locate or download official brand YouTube footage (<= 1080p MP4)."""
        for folder in (self.brand_cache_dir, self.cache_dir):
            for ext in ("mp4", "mkv", "webm"):
                existing = folder / f"{video_id}.{ext}"
                if existing.exists() and existing.stat().st_size > 100_000:
                    try:
                        info = _probe_media_info(existing)
                        if info["width"] > 0 and info["duration"] > 5.0:
                            if folder != self.cache_dir:
                                dst = self.cache_dir / existing.name
                                if not dst.exists():
                                    shutil.copy2(existing, dst)
                            return existing
                    except Exception:
                        pass

        print(f"[M4.1] Downloading video-only official brand clip {video_id} ({url})...", flush=True)
        raw_dl_tmpl = str(self.cache_dir / f"{video_id}.raw.%(ext)s")
        ydl_opts = {
            "format": "bestvideo[height<=720][ext=mp4]/best[height<=720][ext=mp4]/best[height<=720]/best",
            "outtmpl": raw_dl_tmpl,
            "merge_output_format": "mp4",
            "quiet": True,
            "no_warnings": True,
            "socket_timeout": 15,
            "retries": 2,
        }
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        except Exception as exc:
            print(f"[M4.1] Warning: failed to download {video_id}: {exc}", flush=True)
            return None

        muted_target = self.cache_dir / f"{video_id}.mp4"
        for ext in ("mp4", "mkv", "webm"):
            raw_cand = self.cache_dir / f"{video_id}.raw.{ext}"
            if raw_cand.exists() and raw_cand.stat().st_size > 100_000:
                tmp_muted = self.cache_dir / f"{video_id}.muted.tmp.mp4"
                try:
                    _run_cmd(
                        [
                            "ffmpeg",
                            "-y",
                            "-threads",
                            FFMPEG_THREADS,
                            "-i",
                            str(raw_cand),
                            "-c:v",
                            "copy",
                            "-an",
                            str(tmp_muted),
                        ]
                    )
                    tmp_muted.replace(muted_target)
                    raw_cand.unlink(missing_ok=True)
                    return muted_target
                except Exception:
                    tmp_muted.unlink(missing_ok=True)
                    raw_cand.replace(muted_target)
                    return muted_target
        return None

    def _scan_video_faceless(self, video_id: str, video_path: Path) -> VideoScanSummary:
        """
        Step M4.2: Low-RAM Sequential 0.5s (`fps=2`) OpenCV scan with persistent `.scan.json` sidecar cache.
        """
        if video_id in self._scan_cache:
            return self._scan_cache[video_id]

        verified_intervals = self.brand_gate.get_verified_clean_intervals(video_id)
        sidecar_path = self.cache_dir / f"{video_id}.scan.json"
        if sidecar_path.exists():
            cached_json = safe_load_json(sidecar_path, lambda: None)
            if (
                isinstance(cached_json, dict)
                and cached_json.get("samples")
                and isinstance(cached_json["samples"][0], dict)
                and "mean_luma" in cached_json["samples"][0]
            ):
                try:
                    summary = VideoScanSummary(
                        video_id=video_id,
                        path=video_path,
                        width=int(cached_json["width"]),
                        height=int(cached_json["height"]),
                        duration_sec=float(cached_json["duration_sec"]),
                        samples=[SampledFrameMeta(**s) for s in cached_json["samples"]],
                        verified_clean_intervals=verified_intervals,
                    )
                    self._scan_cache[video_id] = summary
                    return summary
                except Exception:
                    pass

        cascade_path = cv2.data.haarcascades + "haarcascade_frontalface_default.xml"
        face_cascade = cv2.CascadeClassifier(cascade_path)

        cap = cv2.VideoCapture(str(video_path))
        fps = cap.get(cv2.CAP_PROP_FPS) or 30.0
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0
        width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 1280)
        height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 720)
        duration = float(frame_count / fps) if fps > 0 and frame_count > 0 else 0.0
        if duration <= 0.0:
            duration = _probe_media_info(video_path)["duration"]

        start_bound = 0.0 if verified_intervals else min(max(2.0, 0.08 * duration), max(0.0, duration - 6.0))
        end_bound = max(start_bound + 2.0, duration - 1.5 if verified_intervals else min(duration - 2.5, 0.90 * duration))

        samples: List[SampledFrameMeta] = []
        step_frames = max(1, int(round(fps * 0.5)))
        frame_idx = 0
        max_frames = int((end_bound + 1.0) * fps) + 10

        while frame_idx <= max_frames:
            grabbed = cap.grab()
            if not grabbed:
                break
            cur_t = frame_idx / max(1.0, fps)
            if cur_t >= start_bound and (frame_idx % step_frames == 0) and cur_t <= end_bound:
                ok, frame = cap.retrieve()
                if ok and frame is not None:
                    fh, fw = frame.shape[:2]
                    target_w = 320
                    scale = target_w / max(1, fw)
                    small_h = max(1, int(fh * scale))
                    small = cv2.resize(frame, (target_w, small_h))
                    gray = cv2.cvtColor(small, cv2.COLOR_BGR2GRAY)

                    faces = face_cascade.detectMultiScale(
                        gray, scaleFactor=1.12, minNeighbors=4, minSize=(22, 22)
                    )
                    frame_area = float(target_w * small_h)
                    max_face_ratio = 0.0
                    if len(faces) > 0:
                        max_face_ratio = max(float(w_f * h_f) / frame_area for (_, _, w_f, h_f) in faces)

                    mean_luma = float(gray.mean())
                    std_luma = float(gray.std())
                    sharpness = float(cv2.Laplacian(gray, cv2.CV_64F).var())
                    edges = cv2.Canny(gray, 110, 210)
                    edge_density = float((edges > 0).mean())
                    # Detect baked-in subtitles / promotional text banners in lower-third or center band
                    lower_band = edges[int(small_h * 0.65) :, :]
                    lower_edge_density = float((lower_band > 0).mean()) if lower_band.size > 0 else 0.0

                    in_verified_clean = any(a <= cur_t <= b for a, b in verified_intervals)
                    if verified_intervals:
                        has_caption_or_card = not in_verified_clean
                        has_face = (not in_verified_clean) and (max_face_ratio > 0.02)
                    else:
                        has_caption_or_card = (
                            mean_luma < 20.0
                            or mean_luma > 232.0
                            or std_luma < 16.0
                            or edge_density > 0.115
                            or lower_edge_density > 0.125
                        )
                        has_face = max_face_ratio > 0.022

                    samples.append(
                        SampledFrameMeta(
                            timestamp=round(cur_t, 2),
                            has_face=has_face,
                            has_caption_or_card=has_caption_or_card,
                            face_ratio=round(max_face_ratio, 4),
                            edge_density=round(edge_density, 4),
                            sharpness=round(sharpness, 2),
                            std_luma=round(std_luma, 2),
                            mean_luma=round(mean_luma, 1),
                        )
                    )
            frame_idx += 1

        cap.release()
        summary = VideoScanSummary(
            video_id=video_id,
            path=video_path,
            width=width,
            height=height,
            duration_sec=duration,
            samples=samples,
            verified_clean_intervals=verified_intervals,
        )
        self._scan_cache[video_id] = summary
        try:
            atomic_write_json(
                sidecar_path,
                {
                    "video_id": video_id,
                    "width": width,
                    "height": height,
                    "duration_sec": duration,
                    "samples": [s.__dict__ for s in samples],
                },
                keep_backup=False,
            )
        except Exception:
            pass
        clean_cnt = sum(1 for s in samples if not s.has_face and not s.has_caption_or_card)
        print(
            f"[M4.2] Sequential-scanned {video_id} ({width}x{height}, {duration:.1f}s @ 0.5s step): "
            f"{clean_cnt}/{len(samples)} clean (face-free & caption-free).",
            flush=True,
        )
        return summary

    @staticmethod
    def _norm_scene_tokens(text: str) -> set[str]:
        import unicodedata

        s = (text or "").replace("đ", "d").replace("Đ", "D")
        nfkd = unicodedata.normalize("NFKD", s)
        ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c)).lower()
        return {tok for tok in re.sub(r"[^a-z0-9]+", " ", ascii_str).split() if len(tok) >= 2}

    def _try_pick_unused_from_summary(
        self,
        summary: VideoScanSummary,
        target_dur: float,
        used_windows: Dict[str, List[Tuple[float, float]]],
        use_opencv_fallback: bool = False,
        beat_index: int = 1,
        query_text_vi: str = "",
    ) -> Optional[Tuple[float, float, bool, float]]:
        """
        Select a 100% UNUSED (`0% overlap`) clean segment `[st, et]` from `summary` that best
        matches the current script beat & clause keywords (`beat_index`, `query_text_vi`).
        Dynamically slices long verified intervals and returns `None` if no unused footage remains.
        """
        existing = used_windows.setdefault(summary.video_id, [])

        def _overlaps(st: float, et: float, pad: float = 0.25) -> bool:
            for u_st, u_et in existing:
                if not (et + pad <= u_st or st - pad >= u_et):
                    return True
            return False

        desired_take = min(max(1.55, target_dur * 0.78), 3.2)
        query_tokens = self._norm_scene_tokens(query_text_vi)
        beat_tag = f"beat{beat_index}"
        tagged_intervals = self.brand_gate.get_verified_clean_intervals_with_tags(summary.video_id)

        if tagged_intervals or summary.verified_clean_intervals:
            raw_ivs = tagged_intervals or [
                (a, b, "") for a, b in summary.verified_clean_intervals
            ]
            scored_slices: List[Tuple[float, float, float]] = []
            for a, b, tag_str in raw_ivs:
                iv_len = b - a
                if iv_len < 1.45:
                    continue
                tag_tokens = self._norm_scene_tokens(tag_str)
                sem_score = 0.0
                if beat_tag in tag_tokens:
                    sem_score += 18.0
                shared = query_tokens.intersection(tag_tokens)
                sem_score += 4.5 * len(shared)
                # Gentle chronological tie-breaker so shots flow naturally with the 5 beats
                rel_pos = (a / max(1.0, summary.duration_sec))
                target_rel = (beat_index - 0.5) / 5.0
                sem_score -= abs(rel_pos - target_rel) * 3.0

                cursor = a
                while cursor + 1.45 <= b + 0.05:
                    avail = b - cursor
                    if 3.1 <= avail <= 4.3 and desired_take <= 2.3:
                        take_len = round((avail - 0.14) * 0.5, 2)
                    else:
                        take_len = min(avail, max(1.55, desired_take))
                    if take_len < 1.45:
                        break
                    st = round(cursor, 2)
                    et = round(cursor + take_len, 2)
                    if not _overlaps(st, et, pad=0.10):
                        scored_slices.append((sem_score, st, et))
                        break
                    cursor = round(cursor + 0.2, 2)

            if scored_slices:
                scored_slices.sort(key=lambda x: (-x[0], x[1]))
                _, st, et = scored_slices[0]
                existing.append((st, et))
                return st, et, True, 180.0

        if not use_opencv_fallback and summary.verified_clean_intervals:
            return None

        win_len = min(max(target_dur * 0.85, 2.3), max(2.3, summary.duration_sec - 2.0))
        candidates: List[Tuple[float, bool, bool, float, float]] = []
        for s in summary.samples:
            st = s.timestamp
            et = st + win_len
            if et > summary.duration_sec - 1.0:
                continue
            inner = [x for x in summary.samples if st <= x.timestamp <= et]
            any_face = any(x.has_face for x in inner) or s.has_face
            any_cap = any(x.has_caption_or_card for x in inner) or s.has_caption_or_card
            if any_face or any_cap:
                continue
            avg_sharp = sum(x.sharpness for x in inner) / len(inner) if inner else s.sharpness
            avg_std = sum(x.std_luma for x in inner) / len(inner) if inner else s.std_luma
            if avg_std < 14.0:
                continue
            if not _overlaps(st, et, pad=0.6):
                candidates.append((st, any_face, any_cap, avg_sharp, s.face_ratio))

        if candidates:
            best = sorted(candidates, key=lambda x: (x[4], -x[3]))[0]
            st = round(best[0], 2)
            et = round(min(st + win_len, summary.duration_sec - 0.5), 2)
            existing.append((st, et))
            return st, et, True, round(best[3], 2)

        return None

    def _pick_faceless_window_from_pool(
        self,
        pool: List[VideoScanSummary],
        target_dur: float,
        used_windows: Dict[str, List[Tuple[float, float]]],
        rotation_idx: int = 0,
        beat_index: int = 1,
        query_text_vi: str = "",
    ) -> Tuple[VideoScanSummary, float, float, bool, float]:
        """
        Multi-Video Pool Zero-Repeat & Semantic Scene Selector:
          1. Rotates across all verified official videos for the SKU (`pool`) and picks a 100% unused
             (`0% overlap`) sub-window that best matches `beat_index` and `query_text_vi`.
          2. If verified intervals are exhausted, checks 100% unused OpenCV clean windows
             across all videos in `pool`.
          3. Emergency Least-Used Fallback: if an ultra-long script ever exhausts every unique
             second across all videos in `pool`, selects the sub-window with the lowest usage count
             and furthest time distance from recent shots (NEVER repeating index 0 back-to-back).
        """
        if not pool:
            raise RuntimeError("Empty video summary pool passed to _pick_faceless_window_from_pool")

        n = len(pool)
        ordered_pool = [pool[(rotation_idx + i) % n] for i in range(n)]
        ordered_pool.sort(
            key=lambda s: sum(max(0.0, u_b - u_a) for u_a, u_b in used_windows.get(s.video_id, []))
        )

        # Pass 1: Strict zero-overlap from verified clean intervals across all videos in pool
        for summary in ordered_pool:
            res = self._try_pick_unused_from_summary(
                summary,
                target_dur,
                used_windows,
                use_opencv_fallback=False,
                beat_index=beat_index,
                query_text_vi=query_text_vi,
            )
            if res is not None:
                return summary, res[0], res[1], res[2], res[3]

        # Pass 2: Strict zero-overlap from OpenCV face-free & caption-free scan across all videos in pool
        for summary in ordered_pool:
            res = self._try_pick_unused_from_summary(
                summary,
                target_dur,
                used_windows,
                use_opencv_fallback=True,
                beat_index=beat_index,
                query_text_vi=query_text_vi,
            )
            if res is not None:
                return summary, res[0], res[1], res[2], res[3]

        # Pass 3: Least-used & furthest-back clean interval across all videos (strictly prevents repeating [0.0, 2.3])
        best_choice: Optional[Tuple[int, int, VideoScanSummary, float, float]] = None
        for summary in ordered_pool:
            existing = used_windows.setdefault(summary.video_id, [])
            intervals = summary.verified_clean_intervals or [
                (s.timestamp, min(summary.duration_sec - 0.5, s.timestamp + 2.6))
                for s in summary.samples
                if not s.has_face and not s.has_caption_or_card
            ]
            for a, b in intervals:
                if b - a < 1.6:
                    continue
                take_len = min(b - a, max(2.0, min(target_dur, 3.4)))
                st = round(a, 2)
                et = round(a + take_len, 2)
                overlap_cnt = sum(
                    1 for u_a, u_b in existing if not (et <= u_a + 0.1 or st >= u_b - 0.1)
                )
                last_seen_dist = 999
                for rev_idx, (u_a, u_b) in enumerate(reversed(existing), start=1):
                    if not (et <= u_a + 0.1 or st >= u_b - 0.1):
                        last_seen_dist = rev_idx
                        break
                cand_key = (overlap_cnt, -last_seen_dist)
                if best_choice is None or cand_key < (best_choice[0], best_choice[1]):
                    best_choice = (overlap_cnt, -last_seen_dist, summary, st, et)

        if best_choice is not None:
            chosen_sum, st, et = best_choice[2], best_choice[3], best_choice[4]
            used_windows.setdefault(chosen_sum.video_id, []).append((st, et))
            return chosen_sum, st, et, True, 160.0

        fallback_sum = ordered_pool[0]
        st, et, ff, sh = self._pick_faceless_window(fallback_sum, target_dur, used_windows)
        return fallback_sum, st, et, ff, sh

    def _pick_faceless_window(
        self,
        summary: VideoScanSummary,
        target_dur: float,
        used_windows: Dict[str, List[Tuple[float, float]]],
    ) -> Tuple[float, float, bool, float]:
        """Single-summary wrapper around `_try_pick_unused_from_summary`."""
        res = self._try_pick_unused_from_summary(
            summary, target_dur, used_windows, use_opencv_fallback=True
        )
        if res is not None:
            return res
        existing = used_windows.setdefault(summary.video_id, [])
        win_len = min(max(target_dur * 0.82, 2.4), max(2.4, summary.duration_sec - 2.0))
        fallback_st = round(min(5.0, max(0.0, summary.duration_sec * 0.18)), 2)
        fallback_et = round(min(fallback_st + win_len, max(fallback_st + 2.0, summary.duration_sec - 0.5)), 2)
        existing.append((fallback_st, fallback_et))
        return fallback_st, fallback_et, True, 120.0

    async def _synth_clause_with_locked_voice(
        self,
        text: str,
        out_mp3: Path,
        out_wav: Path,
        locked_voice: str,
        is_first_clause: bool = False,
    ) -> float:
        """Step M4.3: Synthesize one clause using ONLY `locked_voice` (strict 1-voice-per-video lock)."""
        if out_wav.exists() and out_wav.stat().st_size > 1000:
            return _get_audio_duration(out_wav)

        raw_stripped = (text or "").strip()
        is_comma_pause = raw_stripped.endswith((",", ";", ":"))
        clean_text = normalize_vietnamese_for_tts(raw_stripped).strip().rstrip(",;:")
        if clean_text and clean_text[-1] not in ".!?":
            # For comma-split clauses, leave comma or short breath; for full sentences, close with '.'
            clean_text += "," if is_comma_pause else "."
        pad_dur_sec = 0.06 if is_comma_pause else 0.12
        lead_delay_prefix = "adelay=300|300," if is_first_clause else ""

        tmp_mp3 = out_mp3.with_suffix(".tmp.mp3")
        tmp_mp3.unlink(missing_ok=True)
        atempo_prefix = ""
        provider_alive = getattr(self, "_tts_provider_alive", None)
        if provider_alive is None:
            provider_alive = {"gemini_tts": True, "tiktok": True, "edge": True}
            self._tts_provider_alive = provider_alive

        synth_done = False

        if locked_voice in GEMINI_VOICE_MAP:
            if not provider_alive.get("gemini_tts", True):
                raise RuntimeError("Gemini TTS marked unavailable for this run; switching to TikTok/Edge TTS.")
            g_voice_name, g_style_prompt = GEMINI_VOICE_MAP[locked_voice]
            raw_gemini_wav = out_mp3.with_suffix(".gemini_raw.wav")
            raw_gemini_wav.unlink(missing_ok=True)
            try:
                def _call_gemini_tts() -> bytes:
                    from dotenv import load_dotenv
                    from google import genai
                    from google.genai import types

                    load_dotenv()
                    api_key = (os.environ.get("GEMINI_API_KEY") or os.environ.get("GOOGLE_API_KEY") or "").strip()
                    if not api_key:
                        raise RuntimeError("Missing GEMINI_API_KEY for Gemini 2.5 Flash TTS")
                    g_client = getattr(self, "_gemini_tts_client", None)
                    if g_client is None:
                        g_client = genai.Client(api_key=api_key)
                        self._gemini_tts_client = g_client
                    resp = g_client.models.generate_content(
                        model=GEMINI_TTS_MODEL,
                        contents=g_style_prompt + clean_text,
                        config=types.GenerateContentConfig(
                            response_modalities=["AUDIO"],
                            speech_config=types.SpeechConfig(
                                voice_config=types.VoiceConfig(
                                    prebuilt_voice_config=types.PrebuiltVoiceConfig(voice_name=g_voice_name)
                                )
                            ),
                        ),
                    )
                    pcm_bytes = resp.candidates[0].content.parts[0].inline_data.data
                    if not pcm_bytes or len(pcm_bytes) < 2000:
                        raise RuntimeError("Gemini TTS returned empty PCM audio")
                    return pcm_bytes

                pcm_data = await asyncio.to_thread(_call_gemini_tts)
                with wave.open(str(raw_gemini_wav), "wb") as wf:
                    wf.setnchannels(1)
                    wf.setsampwidth(2)
                    wf.setframerate(24000)
                    wf.writeframes(pcm_data)

                # Studio de-breath gate + warm vocal EQ + crisp Shorts pacing (1.06x) + 0.30s opening cushion
                _run_cmd(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(raw_gemini_wav),
                        "-af",
                        "highpass=f=95,"
                        "agate=threshold=0.028:ratio=5:attack=12:release=140:knee=4,"
                        "atempo=1.06,"
                        "equalizer=f=3000:t=q:w=1.2:g=1.2,"
                        "equalizer=f=7000:t=q:w=1.5:g=-2.0,"
                        "silenceremove=start_periods=1:start_silence=0.03:start_threshold=-42dB,"
                        "areverse,silenceremove=start_periods=1:start_silence=0.05:start_threshold=-42dB,"
                        f"areverse,{lead_delay_prefix}apad=pad_dur={pad_dur_sec:.2f}",
                        "-ar",
                        "44100",
                        "-ac",
                        "2",
                        str(out_wav),
                    ]
                )
                raw_gemini_wav.unlink(missing_ok=True)
                await asyncio.sleep(1.1)
                return _get_audio_duration(out_wav)
            except Exception as exc:
                raw_gemini_wav.unlink(missing_ok=True)
                provider_alive["gemini_tts"] = False
                print(
                    f"  [M4.3 TTS] Gemini 2.5 Flash TTS '{locked_voice}' unavailable ({exc}); "
                    f"switching cleanly to TikTok/Edge Neural TTS.",
                    flush=True,
                )
                raise RuntimeError(f"Gemini TTS failed ({exc})") from exc

        if locked_voice in ("BV074_streaming", "BV075_streaming"):
            if not provider_alive.get("tiktok", True):
                raise RuntimeError(f"TikTok TTS marked unavailable; escalating to Edge Neural TTS.")
            chunks = _split_utf8_chunks(clean_text, max_bytes=240)
            chunk_mp3s: List[Path] = []
            try:
                for ch_idx, chunk in enumerate(chunks, start=1):
                    ch_path = out_mp3.with_name(f"{out_mp3.stem}_p{ch_idx:02d}.mp3")
                    ch_tmp = ch_path.with_suffix(".tmp.mp3")

                    def _call_tiktok(c_text: str = chunk) -> bytes:
                        payload = json.dumps({"text": c_text, "voice": locked_voice}).encode("utf-8")
                        req = urllib.request.Request(
                            TIKTOK_TTS_API_URL,
                            data=payload,
                            headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0"},
                            method="POST",
                        )
                        with urllib.request.urlopen(req, timeout=6, context=_SSL_CTX) as resp:
                            body = json.loads(resp.read().decode("utf-8"))
                        if not body.get("success") or not body.get("data"):
                            raise RuntimeError(f"TikTok TTS unsuccessful response: {body}")
                        return base64.b64decode(body["data"])

                    ch_tmp.unlink(missing_ok=True)
                    mp3_bytes = await asyncio.to_thread(_call_tiktok)
                    if len(mp3_bytes) <= 500:
                        raise RuntimeError("TikTok TTS returned empty audio payload")
                    ch_tmp.write_bytes(mp3_bytes)
                    ch_tmp.replace(ch_path)
                    chunk_mp3s.append(ch_path)
                    await asyncio.sleep(0.18)

                if len(chunk_mp3s) == 1:
                    chunk_mp3s[0].replace(out_mp3)
                else:
                    list_txt = out_mp3.with_suffix(".parts.txt")
                    list_txt.write_text(
                        "\n".join(f"file '{p.name}'" for p in chunk_mp3s),
                        encoding="utf-8",
                    )
                    try:
                        _run_cmd(
                            [
                                "ffmpeg",
                                "-y",
                                "-f",
                                "concat",
                                "-safe",
                                "0",
                                "-i",
                                list_txt.name,
                                "-c:a",
                                "libmp3lame",
                                "-b:a",
                                "64k",
                                str(tmp_mp3),
                            ],
                            cwd=out_mp3.parent,
                        )
                        tmp_mp3.replace(out_mp3)
                    finally:
                        list_txt.unlink(missing_ok=True)
                        for p in chunk_mp3s:
                            p.unlink(missing_ok=True)

                atempo_prefix = ""
                synth_done = True
            except Exception as exc:
                provider_alive["tiktok"] = False
                tmp_mp3.unlink(missing_ok=True)
                for p in chunk_mp3s:
                    p.unlink(missing_ok=True)
                print(
                    f"  [M4.3 TTS] TikTok proxy unavailable ({exc}); escalating to Microsoft Edge Neural TTS "
                    f"(never jumping straight to robotic Google Translate TTS).",
                    flush=True,
                )
                raise RuntimeError(f"TikTok TTS failed ({exc})") from exc

        elif locked_voice.startswith("vi-VN-"):
            if not provider_alive.get("edge", True):
                raise RuntimeError(f"Edge Neural TTS marked unavailable.")
            last_edge_exc: Optional[Exception] = None
            for attempt in range(1, 3):
                try:
                    tmp_mp3.unlink(missing_ok=True)
                    comm = edge_tts.Communicate(text=clean_text, voice=locked_voice, rate=VOICE_RATE)
                    await comm.save(str(tmp_mp3))
                    if tmp_mp3.exists() and tmp_mp3.stat().st_size > 500:
                        tmp_mp3.replace(out_mp3)
                        synth_done = True
                        await asyncio.sleep(0.25)
                        break
                    raise RuntimeError("Edge TTS returned empty file")
                except Exception as exc:
                    last_edge_exc = exc
                    tmp_mp3.unlink(missing_ok=True)
                    if attempt < 2:
                        await asyncio.sleep(0.5)
            if not synth_done:
                print(
                    f"  [M4.3 TTS] Edge Neural voice '{locked_voice}' failed after 2 tries ({last_edge_exc}).",
                    flush=True,
                )
                raise RuntimeError(f"Edge TTS '{locked_voice}' failed: {last_edge_exc}") from last_edge_exc

        if not synth_done:
            # Emergency last-resort fallback ONLY when both TikTok TTS & Edge Neural TTS are unreachable
            chunks = _split_utf8_chunks(clean_text, max_bytes=195)
            chunk_mp3s = []
            try:
                for ch_idx, chunk in enumerate(chunks, start=1):
                    ch_path = out_mp3.with_name(f"{out_mp3.stem}_g{ch_idx:02d}.mp3")
                    ch_tmp = ch_path.with_suffix(".tmp.mp3")

                    def _call_google_tts(c_text: str = chunk) -> bytes:
                        q_enc = urllib.parse.quote(c_text)
                        url = (
                            f"https://translate.googleapis.com/translate_tts"
                            f"?ie=UTF-8&client=tw-ob&tl=vi&q={q_enc}"
                        )
                        req = urllib.request.Request(
                            url,
                            headers={"User-Agent": "Mozilla/5.0"},
                            method="GET",
                        )
                        with urllib.request.urlopen(req, timeout=10, context=_SSL_CTX) as resp:
                            return resp.read()

                    last_g_exc: Optional[Exception] = None
                    ch_ok = False
                    for attempt, wait_s in enumerate((0.5, 1.5, 3.0), start=1):
                        ch_tmp.unlink(missing_ok=True)
                        try:
                            mp3_bytes = await asyncio.to_thread(_call_google_tts)
                            if len(mp3_bytes) > 400:
                                ch_tmp.write_bytes(mp3_bytes)
                                ch_tmp.replace(ch_path)
                                chunk_mp3s.append(ch_path)
                                ch_ok = True
                                await asyncio.sleep(0.12)
                                break
                        except Exception as exc:
                            last_g_exc = exc
                            await asyncio.sleep(wait_s)
                    if not ch_ok:
                        raise RuntimeError(
                            f"Google Neural TTS failed on chunk '{chunk[:40]}': {last_g_exc}"
                        )

                if len(chunk_mp3s) == 1:
                    chunk_mp3s[0].replace(out_mp3)
                else:
                    list_txt = out_mp3.with_suffix(".gparts.txt")
                    list_txt.write_text(
                        "\n".join(f"file '{p.name}'" for p in chunk_mp3s),
                        encoding="utf-8",
                    )
                    try:
                        _run_cmd(
                            [
                                "ffmpeg",
                                "-y",
                                "-f",
                                "concat",
                                "-safe",
                                "0",
                                "-i",
                                list_txt.name,
                                "-c:a",
                                "libmp3lame",
                                "-b:a",
                                "64k",
                                str(tmp_mp3),
                            ],
                            cwd=out_mp3.parent,
                        )
                        tmp_mp3.replace(out_mp3)
                    finally:
                        list_txt.unlink(missing_ok=True)
                        for p in chunk_mp3s:
                            p.unlink(missing_ok=True)

                atempo_prefix = "atempo=1.18,"
            except Exception:
                tmp_mp3.unlink(missing_ok=True)
                for p in chunk_mp3s:
                    p.unlink(missing_ok=True)
                raise

        _run_cmd(
            [
                "ffmpeg",
                "-y",
                "-i",
                str(out_mp3),
                "-af",
                f"{atempo_prefix}silenceremove=start_periods=1:start_silence=0.03:start_threshold=-45dB,"
                "areverse,silenceremove=start_periods=1:start_silence=0.05:start_threshold=-45dB,"
                f"areverse,{lead_delay_prefix}apad=pad_dur={pad_dur_sec:.2f}",
                "-ar",
                "44100",
                "-ac",
                "2",
                str(out_wav),
            ]
        )
        return _get_audio_duration(out_wav)

    def _write_ass_file_b1(
        self,
        ass_path: Path,
        timed_subtitles: List[Tuple[float, float, str]],
        hook_title_short_vi: str,
        hook_end_sec: float,
    ) -> None:
        """
        Write Layout B1 ASS file:
          - Style `Sub`: `Fontsize=36` (10% smaller than demo's 40), centered at `y=1380` with `120ms` fade.
          - Style `Hook`: `Fontsize=64`, slide-up `200ms` at `y=560`, strictly `<= 30` chars (`0..hook_end_sec`).
        """
        short_hook = enforce_short_hook_title(hook_title_short_vi, max_chars=30).upper()
        safe_hook = short_hook.replace("{", "(").replace("}", ")")

        header = """[Script Info]
ScriptType: v4.00+
PlayResX: 1080
PlayResY: 1920
WrapStyle: 0
ScaledBorderAndShadow: yes

[V4+ Styles]
Format: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding
Style: Sub,Arial,44,&H00FFFFFF,&H000000FF,&H000E1118,&HAA000000,-1,0,0,0,100,100,0.4,0,1,4.2,2.4,5,64,64,0,1
Style: Hook,Arial,68,&H00FFFFFF,&H000000FF,&H000E1118,&H00000000,-1,0,0,0,100,100,0.5,0,1,4.5,2.4,5,60,60,0,1

[Events]
Format: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text
"""
        lines = [header]
        h_end = min(max(1.8, hook_end_sec), 3.2)
        lines.append(
            f"Dialogue: 3,{_format_ass_time(0.05)},{_format_ass_time(h_end)},Hook,,0,0,0,,"
            f"{{\\move(540,590,540,560,0,220)\\fad(200,150)\\c&H0034C4FF&}}{safe_hook}"
        )
        for st, et, stxt in timed_subtitles:
            lines.append(
                f"Dialogue: 2,{_format_ass_time(st)},{_format_ass_time(et)},Sub,,0,0,0,,"
                f"{{\\pos(540,1380)\\fad(120,80)}}{stxt}"
            )
        ass_path.write_text("\n".join(lines), encoding="utf-8")

    def _render_segment_layout_b1(
        self,
        summary: VideoScanSummary,
        start_sec: float,
        end_sec: float,
        target_dur: float,
        out_mp4: Path,
        layout_mode: Literal["B1", "cinema", "fullscreen"] = "B1",
    ) -> None:
        """
        Step M4.4a: Render segment in approved Layout B1:
          - If source is vertical (`height > width`) or `layout_mode == "fullscreen"`: full `1080x1920`.
          - Otherwise (16:9 studio footage):
            * Background (`1080x1920`): exact same shot scaled to `270x480`, `boxblur=16:2`, scaled to `1080x1920`,
              `eq=brightness=-0.10:saturation=0.9` (NO black `drawbox` bars).
            * Foreground (`1080x608` at `y=656`): zero-zoom, zero-crop `scale=1080:608:flags=lanczos`.
        """
        if out_mp4.exists() and out_mp4.stat().st_size > 10_000:
            try:
                cached_dur = _probe_media_info(out_mp4)["duration"]
                if abs(cached_dur - target_dur) < 0.25:
                    return
            except Exception:
                pass

        src_dur = max(0.5, end_sec - start_sec)
        pts_factor = max(1.0, target_dur / src_dur)
        take_dur = min(src_dur, target_dur)
        base_chain = (
            f"trim=start={start_sec:.3f}:end={start_sec + take_dur:.3f},setpts=PTS-STARTPTS,"
            f"setpts={pts_factor:.5f}*PTS,fps=30"
        )

        # Keep 100% original video brightness, contrast, gamma, and color (zero `eq` code intervention)
        if layout_mode == "fullscreen" or summary.height > summary.width:
            vf = (
                f"[0:v]{base_chain},setsar=1:1,"
                f"scale=1080:1920:flags=lanczos:force_original_aspect_ratio=decrease,"
                f"pad=1080:1920:(ow-iw)/2:(oh-ih)/2:black,"
                f"setsar=1:1,setdar=9/16,"
                f"trim=duration={target_dur:.3f},format=yuv420p[vout]"
            )
        else:
            vf = (
                f"[0:v]{base_chain},split=2[vbg][vfg];"
                f"[vbg]scale=270:480:force_original_aspect_ratio=increase,crop=270:480,"
                f"boxblur=16:2,scale=1080:1920:flags=bilinear,setsar=1:1[bg];"
                f"[vfg]scale=1080:608:flags=lanczos,setsar=1:1[fg];"
                f"[bg][fg]overlay=x=0:y=656,setsar=1:1,setdar=9/16,"
                f"trim=duration={target_dur:.3f},format=yuv420p[vout]"
            )

        tmp_slice = out_mp4.with_suffix(".tmp.mp4")
        tmp_slice.unlink(missing_ok=True)
        try:
            _run_cmd(
                [
                    "ffmpeg",
                    "-y",
                    "-threads",
                    FFMPEG_THREADS,
                    "-i",
                    str(summary.path),
                    "-filter_complex",
                    vf,
                    "-map",
                    "[vout]",
                    "-an",
                    "-t",
                    f"{target_dur:.3f}",
                    "-c:v",
                    "libx264",
                    "-preset",
                    "fast",
                    "-crf",
                    "20",
                    "-aspect",
                    "9:16",
                    str(tmp_slice),
                ]
            )
            tmp_slice.replace(out_mp4)
        finally:
            tmp_slice.unlink(missing_ok=True)

    def _build_social_pack(self, script: AuditedScriptItem, m3_data: M3ApprovedScriptsOutput) -> SocialPostPack:
        links: Dict[str, str] = {}
        offer_lines: List[str] = []
        tags: List[str] = [
            "#reviewcogia",
            "#gocnhabep" if script.domain == "KITCHEN_TECH" else "#techsetup",
            "#muagiachinhhang",
        ]

        for cid, offer in script.affiliate_offers_ready.items():
            if offer and offer.short_link:
                links[cid] = offer.short_link
                offer_lines.append(f"• {offer.product_name[:55]}... 👉 {offer.short_link}")
            else:
                links[cid] = "(Chưa gắn link — tìm kiếm theo tên model chính hãng)"

        caveat_lines = [
            f"⚠️ Lưu ý thật ({c.brand} {c.model_code or ''}): {c.mandatory_caveat_mentioned_vi}".strip()
            for c in script.verified_factual_claims
        ]
        for c in script.verified_factual_claims:
            clean_brand = re.sub(r"[^a-zA-Z0-9]", "", c.brand)
            if clean_brand:
                tags.append(f"#{clean_brand}")
            if c.model_code:
                clean_model = re.sub(r"[^a-zA-Z0-9]", "", c.model_code)
                if clean_model:
                    tags.append(f"#{clean_model}")

        caption_vi = (
            f"{script.title_vi}\n\n"
            f"{chr(10).join(caveat_lines)}\n\n"
            "Xem chi tiết ưu/nhược điểm trong video trước khi xuống tiền nhé!"
        )
        pinned_comment = (
            "Link sản phẩm chính hãng mình đã kiểm chứng thông số:\n"
            + ("\n".join(offer_lines) if offer_lines else "(Đang cập nhật link chính hãng)")
        )

        return SocialPostPack(
            title_vi=script.title_vi,
            caption_vi=caption_vi,
            hashtags=list(dict.fromkeys(tags)),
            pinned_comment_affiliate_vi=pinned_comment,
            affiliate_short_links=links,
        )

    @staticmethod
    def _estimate_clean_footage_sec(summary: VideoScanSummary) -> float:
        if summary.verified_clean_intervals:
            return round(sum(max(0.0, b - a) for a, b in summary.verified_clean_intervals if b - a >= 1.6), 2)
        clean_cnt = sum(1 for s in summary.samples if not s.has_face and not s.has_caption_or_card)
        return round(clean_cnt * 0.5, 2)

    def _discover_supplemental_sku_videos(
        self,
        brand: str,
        model_code: Optional[str],
        candidate_id: str,
        existing_ids: set[str],
        needed_sec: float,
    ) -> List[VideoScanSummary]:
        """
        Automatic YouTube Supplemental Video Discovery:
        If a SKU's existing video pool has insufficient clean footage (< 55s) to cover a full
        48-55s video without repeating shots, automatically search YouTube via `yt-dlp` for
        additional official/clean product videos of the same SKU, filter out blocked 3rd-party
        reviewers, download and scan until `needed_sec` is satisfied.
        """
        from pipeline.schemas_m0 import YouTubeEvidenceClip

        found: List[VideoScanSummary] = []
        acc_sec = 0.0
        model_q = (model_code or candidate_id.replace("_", " ")).strip()
        queries = [
            f"ytsearch8:{brand} {model_q} official",
            f"ytsearch8:{brand} {model_q} unboxing",
        ]
        for q in queries:
            if acc_sec >= needed_sec or len(found) >= 2:
                break
            try:
                proc = subprocess.run(
                    ["yt-dlp", "--dump-json", "--flat-playlist", q],
                    capture_output=True,
                    text=True,
                    timeout=18,
                )
                if proc.returncode != 0:
                    continue
                for line in proc.stdout.strip().splitlines():
                    if acc_sec >= needed_sec or len(found) >= 2:
                        break
                    try:
                        d = json.loads(line)
                    except Exception:
                        continue
                    vid = d.get("id")
                    dur = int(d.get("duration") or 0)
                    title = str(d.get("title") or "")
                    channel = str(d.get("channel") or d.get("uploader") or "")
                    if not vid or vid in existing_ids or dur < 25 or dur > 240:
                        continue
                    clip_obj = YouTubeEvidenceClip(
                        video_id=vid,
                        url=f"https://www.youtube.com/watch?v={vid}",
                        title=title,
                        channel=channel,
                        duration_sec=dur,
                        max_height=1080,
                        view_count=int(d.get("view_count") or 0),
                        matched_query=q,
                    )
                    if not self.brand_gate.is_official_brand_clip(clip_obj, brand, model_code):
                        continue
                    existing_ids.add(vid)
                    local_p = self._ensure_source_video(vid, clip_obj.url)
                    if local_p:
                        scan_sum = self._scan_video_faceless(vid, local_p)
                        c_sec = self._estimate_clean_footage_sec(scan_sum)
                        if c_sec >= 8.0:
                            found.append(scan_sum)
                            acc_sec += c_sec
                            print(
                                f"[M4.1+] Auto-supplemented SKU '{candidate_id}' with official clip {vid} "
                                f"(+{c_sec:.1f}s clean footage)",
                                flush=True,
                            )
            except Exception:
                continue
        return found

    def _resolve_official_clips_for_script(self, script: AuditedScriptItem) -> Dict[str, VideoScanSummary]:
        """
        Resolve and scan ALL verified official brand videos for every SKU in `script`
        (forming a multi-video pool per SKU so M4 never has to repeat a clip segment).
        """
        clip_summaries: Dict[str, VideoScanSummary] = {}
        claims_by_cid = {c.candidate_id: c for c in script.verified_factual_claims}

        # 1. Resolve ALL curated/verified official videos for each primary SKU claim
        for claim in script.verified_factual_claims:
            official_clips = self.brand_gate.filter_and_resolve_official_clips(
                brand=claim.brand,
                model_code=claim.model_code,
                candidate_id=claim.candidate_id,
                raw_clips=[],
            )
            sku_clean_sec = 0.0
            for oc in official_clips:
                if oc.video_id not in clip_summaries:
                    local_p = self._ensure_source_video(oc.video_id, oc.url)
                    if local_p:
                        clip_summaries[oc.video_id] = self._scan_video_faceless(oc.video_id, local_p)
                if oc.video_id in clip_summaries:
                    sku_clean_sec += self._estimate_clean_footage_sec(clip_summaries[oc.video_id])

            # If a single-SKU video pool has < 55s of clean footage (or < 30s for 25s micro-loop), auto-discover supplemental official videos
            target_min_clean = (
                30.0
                if (len(script.verified_factual_claims) > 1 or script.narrative_angle == "FAST_25S_MICRO_LOOP")
                else 58.0
            )
            if sku_clean_sec < target_min_clean:
                extra_summaries = self._discover_supplemental_sku_videos(
                    brand=claim.brand,
                    model_code=claim.model_code,
                    candidate_id=claim.candidate_id,
                    existing_ids=set(clip_summaries.keys()),
                    needed_sec=target_min_clean - sku_clean_sec,
                )
                for es in extra_summaries:
                    clip_summaries[es.video_id] = es

        # 2. Check beat clips (passing through OfficialBrandVideoGate)
        for b in script.beats:
            if b.source_youtube_clip:
                claim = claims_by_cid.get(b.target_candidate_id) or (
                    script.verified_factual_claims[0] if script.verified_factual_claims else None
                )
                brand = claim.brand if claim else ""
                model_code = claim.model_code if claim else None
                cand_id = claim.candidate_id if claim else b.target_candidate_id
                verified_list = self.brand_gate.filter_and_resolve_official_clips(
                    brand=brand,
                    model_code=model_code,
                    candidate_id=cand_id,
                    raw_clips=[b.source_youtube_clip],
                )
                for vc in verified_list:
                    if vc.video_id not in clip_summaries:
                        local_p = self._ensure_source_video(vc.video_id, vc.url)
                        if local_p:
                            clip_summaries[vc.video_id] = self._scan_video_faceless(vc.video_id, local_p)
                if verified_list:
                    b.source_youtube_clip = verified_list[0]

        return clip_summaries

    async def _produce_single_video_async(
        self,
        script: AuditedScriptItem,
        m3_data: M3ApprovedScriptsOutput,
        layout_mode: Literal["B1", "cinema", "fullscreen"],
        voices_used_today: List[str],
        preferred_voice: Optional[str] = None,
    ) -> RenderedVideoItem:
        seg_dir = self.work_dir / script.script_id
        seg_dir.mkdir(parents=True, exist_ok=True)
        t_start_perf = time.perf_counter()
        render_started_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")

        # 1. Ensure official brand source videos for this script are downloaded & dense-scanned
        clip_summaries = self._resolve_official_clips_for_script(script)
        if not clip_summaries:
            raise RuntimeError(
                f"No verified official brand videos available for script {script.script_id}"
            )
        summary_list = list(clip_summaries.values())

        # Map each candidate_id to its FULL multi-video pool (never break after the 1st video!)
        cand_to_summaries: Dict[str, List[VideoScanSummary]] = {}
        for claim in script.verified_factual_claims:
            off_clips = self.brand_gate.filter_and_resolve_official_clips(
                brand=claim.brand,
                model_code=claim.model_code,
                candidate_id=claim.candidate_id,
                raw_clips=[],
            )
            pool: List[VideoScanSummary] = []
            for oc in off_clips:
                if oc.video_id in clip_summaries and clip_summaries[oc.video_id] not in pool:
                    pool.append(clip_summaries[oc.video_id])
            if pool:
                cand_to_summaries[claim.candidate_id] = pool

        # 2. Split every beat into short aligned clauses & synthesize voiceover with 1 locked voice
        clause_specs: List[Tuple[AuditedBeatSegment, int, str, str, List[VideoScanSummary]]] = []
        clause_counter = 0
        for b in sorted(script.beats, key=lambda x: x.beat_index):
            pool = cand_to_summaries.get(b.target_candidate_id) or []
            if not pool and b.source_youtube_clip and b.source_youtube_clip.video_id in clip_summaries:
                pool = [clip_summaries[b.source_youtube_clip.video_id]]
            if not pool:
                pool = summary_list
            pairs = _split_aligned_clauses(b.tts_voiceover_vi, b.subtitle_text_vi)
            for tts_part, sub_part in pairs:
                clause_counter += 1
                clause_specs.append((b, clause_counter, tts_part, sub_part, pool))

        voice_candidates = _choose_voice_for_video(voices_used_today, preferred_voice=preferred_voice)
        ordered_voice_attempts: List[Tuple[str, Literal["FEMALE", "MALE"]]] = []
        edge_fallbacks: List[Tuple[str, Literal["FEMALE", "MALE"]]] = []
        for primary_tiktok_voice, gender, fallback_edge in voice_candidates:
            gemini_voice = "gemini_aoede" if gender == "FEMALE" else "gemini_puck"
            # Randomly rotate within the primary pool (Gemini 2.5 Flash TTS + TikTok TTS), avoiding repeating last voice
            primary_pair = [gemini_voice, primary_tiktok_voice]
            if voices_used_today and voices_used_today[-1] == primary_pair[0]:
                primary_pair.reverse()
            elif not voices_used_today or voices_used_today[-1] != primary_pair[1]:
                random.shuffle(primary_pair)
            for pv in primary_pair:
                ordered_voice_attempts.append((pv, gender))
            edge_fallbacks.append((fallback_edge, gender))
        # Fallback tier: Microsoft Edge Neural TTS, then emergency Google TTS only if all neural engines fail
        ordered_voice_attempts.extend(edge_fallbacks)
        ordered_voice_attempts.append(("google_female", "FEMALE"))

        sem = asyncio.Semaphore(1)
        locked_voice: Optional[str] = None
        locked_gender: Literal["FEMALE", "MALE"] = "FEMALE"
        tts_map: Dict[int, Tuple[float, Path]] = {}

        for candidate_voice, gender in ordered_voice_attempts:
            try:
                candidate_map: Dict[int, Tuple[float, Path]] = {}
                async with sem:
                    for _, c_idx, tts_txt, _, _ in clause_specs:
                        mp3_p = seg_dir / f"c{c_idx:02d}_{candidate_voice}.mp3"
                        wav_p = seg_dir / f"c{c_idx:02d}_{candidate_voice}.wav"
                        dur = await self._synth_clause_with_locked_voice(
                            tts_txt,
                            mp3_p,
                            wav_p,
                            locked_voice=candidate_voice,
                            is_first_clause=(c_idx == 1),
                        )
                        candidate_map[c_idx] = (dur, wav_p)
                tts_map = candidate_map
                locked_voice = candidate_voice
                locked_gender = gender
                print(
                    f"[M4.3] Voice locked for {script.script_id}: {locked_voice} ({locked_gender}) "
                    f"[{len(clause_specs)}/{len(clause_specs)} clauses uniform]",
                    flush=True,
                )
                break
            except Exception as exc:
                print(
                    f"[M4.3] Whole-video voice '{candidate_voice}' failed on {script.script_id}: {exc}. "
                    f"Discarding partial clips to guarantee 1-voice uniformity...",
                    flush=True,
                )
                for p in seg_dir.glob(f"c*_{candidate_voice}.*"):
                    p.unlink(missing_ok=True)

        if not locked_voice or not tts_map:
            raise RuntimeError(
                f"Failed to synthesize locked single-voice track for {script.script_id}"
            )

        # 2b. Zero-Repeat Visual Capacity & YouTube Shorts (< 60s) Duration Guard:
        # Calculate the exact non-repeating clean footage capacity (`max_non_repeating_sec`)
        # of the script's official video pool so M4 NEVER has to repeat a video shot just to chase script length.
        unique_clean_src_sec = sum(self._estimate_clean_footage_sec(s) for s in summary_list)
        max_non_repeating_sec = min(55.5, max(31.0, unique_clean_src_sec * 1.28))
        raw_total_vo_sec = sum(d for d, _ in tts_map.values())
        if raw_total_vo_sec > max_non_repeating_sec:
            target_vo_sec = max(30.0, max_non_repeating_sec - 0.8)
            speed_factor = min(1.26, max(1.03, raw_total_vo_sec / target_vo_sec))
            print(
                f"[M4.3] Syncing voiceover {raw_total_vo_sec:.1f}s -> ~{raw_total_vo_sec/speed_factor:.1f}s "
                f"(atempo={speed_factor:.3f}) to fit {unique_clean_src_sec:.1f}s of unique official footage (0% repeat).",
                flush=True,
            )
            for c_idx, (_, wav_p) in list(tts_map.items()):
                tmp_scaled = wav_p.with_suffix(".scaled.wav")
                tmp_scaled.unlink(missing_ok=True)
                _run_cmd(
                    [
                        "ffmpeg",
                        "-y",
                        "-i",
                        str(wav_p),
                        "-af",
                        f"atempo={speed_factor:.4f}",
                        "-ar",
                        "44100",
                        "-ac",
                        "2",
                        str(tmp_scaled),
                    ]
                )
                tmp_scaled.replace(wav_p)
                tts_map[c_idx] = (_get_audio_duration(wav_p), wav_p)

        # 3. Pick non-overlapping clean official video sub-shots across the SKU's Multi-Video Pool
        used_windows: Dict[str, List[Tuple[float, float]]] = {}
        all_wavs: List[Path] = []
        all_vids: List[Path] = []
        timed_subtitles: List[Tuple[float, float, str]] = []
        beat_spans: Dict[int, List[float]] = {}
        sub_shots: List[SubShotClauseItem] = []

        cursor_sec = 0.0
        for b, c_idx, tts_part, sub_part, pool in clause_specs:
            dur, wav_p = tts_map[c_idx]
            all_wavs.append(wav_p)
            scene_query = f"{tts_part} {b.on_screen_text_vi}"

            vid_slice_path = seg_dir / f"v_{c_idx:02d}_B1_{locked_voice}.mp4"
            if dur > MAX_CONTINUOUS_SHOT_SEC:
                # Anti-Content-ID Visual Temporal Guard: split clauses > 3.6s into 2 distinct visual sub-cuts
                d1 = round(dur * 0.5, 2)
                d2 = round(dur - d1, 2)
                sum1, st1, et1, ff1, sh1 = self._pick_faceless_window_from_pool(
                    pool,
                    d1,
                    used_windows,
                    rotation_idx=c_idx * 2,
                    beat_index=b.beat_index,
                    query_text_vi=scene_query,
                )
                sum2, st2, et2, ff2, sh2 = self._pick_faceless_window_from_pool(
                    pool,
                    d2,
                    used_windows,
                    rotation_idx=c_idx * 2 + 1,
                    beat_index=b.beat_index,
                    query_text_vi=scene_query,
                )
                p1 = seg_dir / f"v_{c_idx:02d}a_B1_{locked_voice}.mp4"
                p2 = seg_dir / f"v_{c_idx:02d}b_B1_{locked_voice}.mp4"
                self._render_segment_layout_b1(sum1, st1, et1, d1, p1, layout_mode=layout_mode)
                self._render_segment_layout_b1(sum2, st2, et2, d2, p2, layout_mode=layout_mode)
                if not (vid_slice_path.exists() and vid_slice_path.stat().st_size > 10_000):
                    pair_txt = seg_dir / f"pair_{c_idx:02d}.txt"
                    pair_txt.write_text(f"file '{p1.name}'\nfile '{p2.name}'\n", encoding="utf-8")
                    tmp_pair = vid_slice_path.with_suffix(".tmp.mp4")
                    try:
                        _run_cmd(
                            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", pair_txt.name, "-c", "copy", tmp_pair.name],
                            cwd=seg_dir,
                        )
                        tmp_pair.replace(vid_slice_path)
                    finally:
                        pair_txt.unlink(missing_ok=True)
                        tmp_pair.unlink(missing_ok=True)
                chosen_vid_id = sum1.video_id if sum1.video_id == sum2.video_id else f"{sum1.video_id}+{sum2.video_id}"
                st_vid, et_vid, face_free, sharp = st1, et2, (ff1 and ff2), round((sh1 + sh2) * 0.5, 2)
            else:
                chosen_sum, st_vid, et_vid, face_free, sharp = self._pick_faceless_window_from_pool(
                    pool=pool,
                    target_dur=dur,
                    used_windows=used_windows,
                    rotation_idx=c_idx,
                    beat_index=b.beat_index,
                    query_text_vi=scene_query,
                )
                chosen_vid_id = chosen_sum.video_id
                self._render_segment_layout_b1(
                    summary=chosen_sum,
                    start_sec=st_vid,
                    end_sec=et_vid,
                    target_dur=dur,
                    out_mp4=vid_slice_path,
                    layout_mode=layout_mode,
                )
            all_vids.append(vid_slice_path)

            timed_subtitles.append(
                (cursor_sec, cursor_sec + dur, _highlight_ass_keywords(sub_part))
            )
            if b.beat_index not in beat_spans:
                beat_spans[b.beat_index] = [cursor_sec, cursor_sec + dur]
            else:
                beat_spans[b.beat_index][1] = cursor_sec + dur
            cursor_sec += dur

            sub_shots.append(
                SubShotClauseItem(
                    clause_index=c_idx,
                    beat_index=b.beat_index,
                    source_video_id=chosen_vid_id,
                    source_start_sec=st_vid,
                    source_end_sec=et_vid,
                    duration_sec=round(dur, 2),
                    face_free_verified=face_free,
                    caption_free_verified=True,
                    sharpness_score=sharp,
                    tts_text_vi=tts_part,
                    subtitle_text_vi=sub_part,
                )
            )

        total_dur = round(cursor_sec, 2)

        # 4. Concat video slices & WAV voiceover
        vid_list_txt = seg_dir / "vids_B1.txt"
        vid_list_txt.write_text(
            "\n".join(f"file '{p.name}'" for p in all_vids), encoding="utf-8"
        )
        concat_vid = seg_dir / "concat_video_B1.mp4"
        _run_cmd(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", vid_list_txt.name, "-c", "copy", concat_vid.name],
            cwd=seg_dir,
        )

        wav_list_txt = seg_dir / "wavs.txt"
        wav_list_txt.write_text(
            "\n".join(f"file '{p.name}'" for p in all_wavs), encoding="utf-8"
        )
        concat_voice = seg_dir / "concat_voice.wav"
        _run_cmd(
            ["ffmpeg", "-y", "-f", "concat", "-safe", "0", "-i", wav_list_txt.name, "-c", "copy", concat_voice.name],
            cwd=seg_dir,
        )

        # 5. Generate PIL Rounded Graphic Cards (Beat 2 Spec Card, Beat 4 Caveat Card, Beat 5 Price Card)
        primary_claim = script.verified_factual_claims[0] if script.verified_factual_claims else None
        if len(script.verified_factual_claims) >= 2:
            spec_title = " + ".join(
                f"{c.brand} {c.model_code or ''}".strip() for c in script.verified_factual_claims[:2]
            )
            spec_lines = [
                f"{c.brand} {c.model_code or ''}: ~{format_vnd_short(c.claimed_price_vnd)}"
                for c in script.verified_factual_claims[:3]
            ]
            total_price_vnd = sum(c.claimed_price_vnd for c in script.verified_factual_claims)
            price_str = f"~{format_vnd_short(total_price_vnd)}"
            caveat_lines = _wrap_card_text(
                "; ".join(c.mandatory_caveat_mentioned_vi for c in script.verified_factual_claims[:2]),
                max_chars_per_line=38,
                max_lines=3,
            )
        elif primary_claim:
            spec_title = f"{primary_claim.brand} {primary_claim.model_code or ''}".strip()
            spec_lines = [
                truncate_words_clean(s, max_chars=40) for s in primary_claim.claimed_specs_mentioned[:3]
            ] or ["Chính hãng kiểm chứng"]
            price_str = f"~{format_vnd_short(primary_claim.claimed_price_vnd)}"
            caveat_lines = _wrap_card_text(
                primary_claim.mandatory_caveat_mentioned_vi, max_chars_per_line=38, max_lines=3
            )
        else:
            spec_title = truncate_words_clean(script.title_vi, max_chars=26)
            spec_lines = ["Thông số chuẩn nhà sản xuất"]
            price_str = "Chính hãng"
            caveat_lines = ["Xem kỹ lưu ý trước khi mua"]

        spec_card_png = seg_dir / "card_spec.png"
        caveat_card_png = seg_dir / "card_caveat.png"
        price_card_png = seg_dir / "card_price.png"
        _render_pil_rounded_card(spec_card_png, 760, 270, spec_title, spec_lines, accent=(255, 196, 52))
        _render_pil_rounded_card(
            caveat_card_png, 800, 260, "LƯU Ý THỰC TẾ", caveat_lines, accent=(255, 145, 48)
        )

        # Upgrade-1: Select and render one of 3 dynamic Beat-5 Visual End-Cards matched to `script.narrative_angle`
        chosen_end_card = _select_end_card_type(script.narrative_angle, script.script_id)
        sku_entries_map = self.sku_registry.get_all_entries()
        real_m1_score_10 = 8.7
        primary_pro_vi = spec_lines[0] if spec_lines else "Đúng thông số chuẩn nhà sản xuất"
        if primary_claim:
            rk = self.sku_registry.resolve_sku_key_by_candidate_id(primary_claim.candidate_id)
            reg_e = sku_entries_map.get(rk) if rk else None
            if reg_e and reg_e.m1_total_score_100 > 0:
                real_m1_score_10 = round(reg_e.m1_total_score_100 / 10.0, 1)
            if reg_e and reg_e.highlight_pros_vi:
                primary_pro_vi = reg_e.highlight_pros_vi[0]

        if chosen_end_card == "VERSUS_2COL_CARD":
            base_vnd = primary_claim.claimed_price_vnd if primary_claim else 350_000
            cheap_k = max(45, int(round((base_vnd * 0.28) / 5000.0) * 5))
            _render_pil_versus_2col_card(
                price_card_png,
                cheap_price_label=f"~{cheap_k}k",
                genuine_brand_label=spec_title,
                genuine_price_label=price_str,
                genuine_highlight_vi=primary_pro_vi,
            )
        elif chosen_end_card == "BUY_SKIP_CARD":
            skip_summary = (
                primary_claim.mandatory_caveat_mentioned_vi
                if primary_claim and primary_claim.mandatory_caveat_mentioned_vi
                else (caveat_lines[0] if caveat_lines else "Cân nhắc kỹ kích thước trước khi mua")
            )
            _render_pil_buy_skip_card(
                price_card_png,
                price_label=price_str,
                buy_if_vi=primary_pro_vi,
                skip_if_vi=skip_summary,
            )
        else:
            _render_pil_score_verdict_card(
                price_card_png,
                score_10=real_m1_score_10,
                brand_model_label=spec_title,
                verdict_line_vi=primary_pro_vi,
                price_label=price_str,
            )

        # 6. Write Layout B1 ASS subtitles (`Fontsize=36`, Hook `Fontsize=64` <= 30 chars)
        hook_short = enforce_short_hook_title(
            getattr(script, "hook_title_short_vi", "")
            or (script.beats[0].on_screen_text_vi if script.beats else "")
            or script.title_vi,
            max_chars=30,
        )
        hook_end_s = min(2.4, beat_spans.get(1, [0.0, 2.2])[1])
        ass_filename = "subs_B1.ass"
        self._write_ass_file_b1(
            seg_dir / ass_filename,
            timed_subtitles=timed_subtitles,
            hook_title_short_vi=hook_short,
            hook_end_sec=hook_end_s,
        )

        # 7. Pick 1 random cheerful/upbeat BGM track & synthesize copyright-free SFX
        bgm_track_path = pick_random_bgm_track(self.bgm_dir)
        whoosh_wav, pop_wav = _build_sfx_files(self.work_dir)

        b2_span = beat_spans.get(2, [min(4.5, total_dur * 0.2), min(9.5, total_dur * 0.4)])
        b4_span = beat_spans.get(4, [min(16.0, total_dur * 0.62), min(22.0, total_dur * 0.82)])
        b5_span = beat_spans.get(5, [min(22.5, total_dur * 0.83), total_dur - 0.1])

        s2a, s2b = round(b2_span[0] + 0.15, 2), round(max(b2_span[0] + 1.2, b2_span[1] - 0.15), 2)
        s4a, s4b = round(b4_span[0] + 0.15, 2), round(max(b4_span[0] + 1.2, b4_span[1] - 0.15), 2)
        s5a, s5b = round(b5_span[0] + 0.10, 2), round(max(b5_span[0] + 1.0, total_dur - 0.10), 2)

        whoosh_times = [round(b2_span[0], 2), round(b4_span[0], 2)]
        pop_times = [s2a, s4a, s5a]

        v_fc = [
            f"[0:v]setsar=1:1,setdar=9/16[basev]",
            f"[1:v]loop=-1:size=1,trim=duration={total_dur:.3f},format=rgba,"
            f"fade=t=in:st={s2a}:d=0.2:alpha=1,fade=t=out:st={max(s2a+0.2, s2b-0.2):.2f}:d=0.2:alpha=1[c_spec]",
            f"[basev][c_spec]overlay=x='160-60*max(0\\,1-(t-{s2a})/0.2)':y=320:enable='between(t,{s2a},{s2b})'[v1]",
            f"[2:v]loop=-1:size=1,trim=duration={total_dur:.3f},format=rgba,"
            f"fade=t=in:st={s4a}:d=0.2:alpha=1,fade=t=out:st={max(s4a+0.2, s4b-0.2):.2f}:d=0.2:alpha=1[c_cav]",
            f"[v1][c_cav]overlay=x='140-60*max(0\\,1-(t-{s4a})/0.2)':y=325:enable='between(t,{s4a},{s4b})'[v2]",
            f"[3:v]loop=-1:size=1,trim=duration={total_dur:.3f},format=rgba,"
            f"fade=t=in:st={s5a}:d=0.2:alpha=1[c_prc]",
            f"[v2][c_prc]overlay=x='120-60*max(0\\,1-(t-{s5a})/0.2)':y=320:enable='between(t,{s5a},{s5b})'[v3]",
            f"[v3]ass='{ass_filename}'[v4]",
            f"color=c=0xFFC434:s=1080x6:d={total_dur:.3f}:r=30[pb]",
            f"[v4][pb]overlay=x='-W+W*t/{total_dur:.3f}':y=0,setsar=1:1,setdar=9/16,format=yuv420p[vout]",
        ]

        fade_out_st = max(0.0, total_dur - 1.5)
        a_fc = [
            "[4:a]aresample=44100,afade=t=in:st=0.30:d=1.8:curve=tri,volume=0.88,asplit=2[vo][vsc]",
            f"[5:a]aresample=44100,atrim=0:{total_dur:.3f},afade=t=in:d=0.8,"
            f"afade=t=out:st={fade_out_st:.3f}:d=1.5,volume=0.22[bgm]",
            "[bgm][vsc]sidechaincompress=threshold=0.03:ratio=8:attack=20:release=300[duck]",
            f"[6:a]asplit={len(whoosh_times)}" + "".join(f"[wi{i}]" for i in range(len(whoosh_times))),
            f"[7:a]asplit={len(pop_times)}" + "".join(f"[pi{i}]" for i in range(len(pop_times))),
        ]
        sfx_labels: List[str] = []
        for i, wt in enumerate(whoosh_times):
            ms = int(max(0.0, wt - 0.12) * 1000)
            a_fc.append(f"[wi{i}]adelay={ms}|{ms},volume=0.10[w{i}]")
            sfx_labels.append(f"[w{i}]")
        for i, pt in enumerate(pop_times):
            ms = int(max(0.0, pt) * 1000)
            a_fc.append(f"[pi{i}]adelay={ms}|{ms},volume=0.09[p{i}]")
            sfx_labels.append(f"[p{i}]")

        a_fc.append(
            f"[vo][duck]{''.join(sfx_labels)}amix=inputs={2 + len(sfx_labels)}:normalize=0:duration=first,"
            f"apad=whole_dur={total_dur:.3f},atrim=0:{total_dur:.3f},"
            f"loudnorm=I=-15.5:TP=-1.5:LRA=10[aout]"
        )

        final_out = self.output_dir / f"{script.script_id}.mp4"
        tmp_final_out = seg_dir / f"{script.script_id}.final.tmp.mp4"
        tmp_final_out.unlink(missing_ok=True)
        MacOSLegacyRuntime.apply_gentle_background_priority()

        _run_cmd(
            [
                "ffmpeg",
                "-y",
                "-threads",
                FFMPEG_THREADS,
                "-i",
                concat_vid.name,
                "-i",
                spec_card_png.name,
                "-i",
                caveat_card_png.name,
                "-i",
                price_card_png.name,
                "-i",
                concat_voice.name,
                "-stream_loop",
                "-1",
                "-i",
                str(bgm_track_path),
                "-i",
                str(whoosh_wav),
                "-i",
                str(pop_wav),
                "-filter_complex",
                ";".join(v_fc + a_fc),
                "-map",
                "[vout]",
                "-map",
                "[aout]",
                "-c:v",
                "libx264",
                "-preset",
                "fast",
                "-crf",
                "19",
                "-aspect",
                "9:16",
                "-c:a",
                "aac",
                "-b:a",
                "192k",
                "-ar",
                "44100",
                "-movflags",
                "+faststart",
                "-t",
                f"{total_dur:.3f}",
                str(tmp_final_out),
            ],
            cwd=seg_dir,
        )

        # Verify tmp_final_out integrity before atomic replace -> output/videos/<script_id>.mp4
        info = _probe_media_info(tmp_final_out)
        if int(info["width"]) != 1080 or int(info["height"]) != 1920 or info["duration"] < 10.0:
            tmp_final_out.unlink(missing_ok=True)
            raise RuntimeError(f"Rendered video failed integrity probe: {info}")
        final_out.parent.mkdir(parents=True, exist_ok=True)
        tmp_final_out.replace(final_out)

        # Generate uniform 9:16 SKU cover thumbnail (`<script_id>_cover.jpg`) for YouTube Shorts & Facebook Reels
        try:
            cover_photos: List[Image.Image] = []
            for claim in (script.verified_factual_claims or [])[:2]:
                sku_k = normalize_sku_key(claim.brand, claim.model_code, claim.candidate_id)
                manual_photo: Optional[Image.Image] = None
                for ext in (".jpg", ".jpeg", ".png", ".webp"):
                    mp = SKU_THUMBS_DIR / f"{sku_k}{ext}"
                    if mp.exists():
                        manual_photo = Image.open(mp).convert("RGB")
                        break
                if manual_photo is not None:
                    cover_photos.append(manual_photo)
                else:
                    pool_for_sku = cand_to_summaries.get(claim.candidate_id) or summary_list
                    for cand_sum in pool_for_sku:
                        best_im = _extract_best_sku_frame_pil(cand_sum.path, cand_sum.samples)
                        if best_im is not None:
                            cover_photos.append(best_im)
                            break
            if not cover_photos and summary_list:
                fb_im = _extract_best_sku_frame_pil(summary_list[0].path, summary_list[0].samples)
                if fb_im is not None:
                    cover_photos.append(fb_im)
            cover_out_jpg = final_out.with_name(f"{script.script_id}_cover.jpg")
            render_uniform_sku_cover_jpeg(
                out_jpg_path=cover_out_jpg,
                sku_photos=cover_photos,
                brand_model_badge=spec_title,
                price_badge=price_str,
                hook_title_vi=hook_short,
                domain=script.domain,
            )
            print(f"[M4.4] Generated uniform 9:16 SKU cover -> {cover_out_jpg.name}", flush=True)
        except Exception as cov_exc:
            print(f"[M4.4] Cover thumbnail generation warning (non-fatal): {cov_exc}", flush=True)

        # Immediately reclaim scratch WAV/MP4 work space now that final MP4 is atomically saved
        StorageGarbageCollector().cleanup_completed_work_dir(seg_dir)

        render_elapsed_sec = round(time.perf_counter() - t_start_perf, 2)
        render_finished_dt = dt.datetime.now().astimezone()
        render_finished_iso = render_finished_dt.isoformat(timespec="seconds")
        next_allowed_iso = (
            render_finished_dt + dt.timedelta(seconds=MIN_RENDER_COOLDOWN_SECONDS)
        ).isoformat(timespec="seconds")

        unique_vids = list(
            dict.fromkeys(
                vid_part
                for s in sub_shots
                for vid_part in s.source_video_id.split("+")
                if vid_part
            )
        )
        social_pack = self._build_social_pack(script, m3_data)

        print(
            f"[M4.4] Rendered Layout B1 {final_out.name} ({int(info['width'])}x{int(info['height'])}, "
            f"{info['duration']:.2f}s, voice={locked_voice}/{locked_gender}, bgm={bgm_track_path.name}, "
            f"render_time={render_elapsed_sec:.1f}s)",
            flush=True,
        )

        return RenderedVideoItem(
            script_id=script.script_id,
            script_format=script.script_format,
            domain=script.domain,
            category_id=script.category_id,
            narrative_angle=script.narrative_angle,
            title_vi=script.title_vi,
            hook_title_short_vi=hook_short,
            video_path=str(final_out),
            video_status="RENDERED",
            layout_mode="B1" if layout_mode != "fullscreen" else "fullscreen",
            end_card_type=chosen_end_card,
            width=int(info["width"]),
            height=int(info["height"]),
            fps=30,
            duration_sec=round(info["duration"], 2),
            file_size_bytes=final_out.stat().st_size,
            voice_name=locked_voice,
            voice_gender=locked_gender,
            single_voice_locked=True,
            bgm_track=bgm_track_path.name,
            render_started_at=render_started_iso,
            render_finished_at=render_finished_iso,
            render_duration_sec=render_elapsed_sec,
            next_render_allowed_at=next_allowed_iso,
            clauses_count=len(sub_shots),
            unique_source_videos_used=unique_vids,
            sub_shots=sub_shots,
            social_post_pack=social_pack,
        )

    def _get_script_sku_keys(self, script: AuditedScriptItem) -> set[str]:
        """Return all canonical SKU keys and candidate_ids referenced as primary SKUs in `script`."""
        keys: set[str] = set()
        for cid in script.primary_sku_ids:
            keys.add(cid)
            resolved = self.sku_registry.resolve_sku_key_by_candidate_id(cid)
            if resolved:
                keys.add(resolved)
        for c in script.verified_factual_claims:
            keys.add(c.candidate_id)
            keys.add(normalize_sku_key(c.brand, c.model_code, c.candidate_id))
        return keys

    def _select_diverse_daily_scripts(
        self,
        candidates: List[AuditedScriptItem],
        limit: int,
        enforce_sku_cooldown: bool = True,
        already_rendered_skus_today: Optional[set[str]] = None,
    ) -> List[AuditedScriptItem]:
        """
        Select up to `limit` scripts maximizing:
          1. Strict Zero SKU Overlap (within the same day AND across the 3-7 day SKU cooldown window
             when `enforce_sku_cooldown=True`).
          2. Domain harmony (`TECH_DESK` + `KITCHEN_TECH`), category uniqueness, and angle diversity.
        """
        if limit <= 0 or not candidates:
            return []

        remaining = candidates[:]
        selected: List[AuditedScriptItem] = []
        selected_domains: set[str] = set()
        selected_cats: set[str] = set()
        selected_skus: set[str] = set(already_rendered_skus_today or set())
        selected_angles: set[str] = set()

        while remaining and len(selected) < limit:
            # Filter out any candidate whose SKUs are in 3-7d cooldown or already used in `selected_skus`
            if enforce_sku_cooldown:
                non_overlapping = [
                    c
                    for c in remaining
                    if not (self._get_script_sku_keys(c) & selected_skus)
                    and not any(self.sku_registry.is_in_cooldown(k) for k in self._get_script_sku_keys(c))
                ]
            else:
                non_overlapping = remaining[:]

            if not non_overlapping:
                break

            available_domains = {c.domain for c in non_overlapping}
            missing_domains = available_domains - selected_domains
            fast_track_cats = {
                t.product_concept_id
                for t in load_active_trend_opportunities()
                if t.priority_tier == "FAST_TRACK_TREND"
            }

            def _diversity_score(item: AuditedScriptItem) -> tuple[int, int, int, int, int]:
                item_skus = self._get_script_sku_keys(item)
                # Prefer scripts that don't block 2 other solo scripts in the same batch when 3 distinct solos exist
                is_solo = item.script_format == "SINGLE_SKU_DEEP_DIVE"
                return (
                    1 if item.category_id in fast_track_cats else 0,
                    1 if item.domain in missing_domains else 0,
                    1 if item.category_id not in selected_cats else 0,
                    1 if not (item_skus & selected_skus) else 0,
                    1 if (item.narrative_angle not in selected_angles and is_solo) else 0,
                )

            best_item = max(non_overlapping, key=_diversity_score)
            remaining.remove(best_item)
            selected.append(best_item)
            selected_domains.add(best_item.domain)
            selected_cats.add(best_item.category_id)
            selected_skus.update(self._get_script_sku_keys(best_item))
            selected_angles.add(best_item.narrative_angle)

        return selected

    def produce_from_m3(
        self,
        m3_data: M3ApprovedScriptsOutput,
        layout_mode: Literal["B1", "cinema", "fullscreen"] = "B1",
        target_script_id: Optional[str] = None,
        max_videos: Optional[int] = None,
        allow_rerender_same_id: bool = True,
        ignore_cooldown: bool = True,
        enforce_sku_cooldown: bool = True,
        quota_mgr: Optional[DailyQuotaManager] = None,
    ) -> M4ProductionOutput:
        qm = quota_mgr or DailyQuotaManager()

        # Sync M3 approved scripts into VideoRegistryManager (`SCRIPT_APPROVED`)
        norm_keys_by_script: Dict[str, List[str]] = {}
        for s in m3_data.audited_scripts:
            norm_keys_by_script[s.script_id] = [
                normalize_sku_key(c.brand, c.model_code, c.candidate_id)
                for c in s.verified_factual_claims
            ]
        self.video_registry.register_approved_scripts(
            scripts=m3_data.audited_scripts,
            m3_run_id=m3_data.m3_run_id,
            normalized_keys_by_script=norm_keys_by_script,
        )

        # Merge current M3 scripts with any queued `SCRIPT_APPROVED` surplus scripts in VideoRegistry
        by_id: Dict[str, AuditedScriptItem] = {}
        for s in m3_data.audited_scripts:
            if s.qaqc_status in ("APPROVED", "APPROVED_WITH_AUTO_PATCH"):
                by_id[s.script_id] = s
        for qs in self.video_registry.get_queued_approved_scripts():
            if qs.script_id not in by_id and qs.qaqc_status in ("APPROVED", "APPROVED_WITH_AUTO_PATCH"):
                by_id[qs.script_id] = qs

        # Hard Pre-Render Vietnamese Diacritic Gate: refuse & purge any poisoned unaccented script before TTS!
        valid_eligible: List[AuditedScriptItem] = []
        for s in by_id.values():
            if target_script_id and s.script_id != target_script_id:
                continue
            bad_fields: List[str] = []
            for lbl, val in [
                ("title_vi", s.title_vi),
                ("hook_title_short_vi", s.hook_title_short_vi),
                *[(f"beat#{b.beat_index}.tts_voiceover_vi", b.tts_voiceover_vi) for b in s.beats],
                *[(f"beat#{b.beat_index}.subtitle_text_vi", b.subtitle_text_vi) for b in s.beats],
                *[(f"beat#{b.beat_index}.on_screen_text_vi", b.on_screen_text_vi) for b in s.beats],
            ]:
                ok_d, _, rsn_d = check_vietnamese_diacritics(val)
                if not ok_d:
                    bad_fields.append(f"{lbl} ({rsn_d})")
            if bad_fields:
                print(
                    f"[M4 DIACRITIC GUARD] 🚫 Chặn & xóa vĩnh viễn kịch bản mất dấu tiếng Việt `{s.script_id}`: {bad_fields[0]}",
                    flush=True,
                )
                self.video_registry.delete_entry(s.script_id)
                self.video_registry.purge_script_from_m2_m3_files(s.script_id)
                continue
            valid_eligible.append(s)

        eligible = valid_eligible

        used_today, _, existing_ids = qm.get_video_quota_status()
        skipped_ids: List[str] = []

        # Check weekly rest day, active hours, and 2-hour cooldown before selecting scripts when `ignore_cooldown=False`
        can_now, cd_status, wait_secs = qm.can_render_video_now(
            script_id=target_script_id,
            allow_rerender_same_id=allow_rerender_same_id,
            ignore_cooldown=ignore_cooldown,
        )
        if not can_now and (
            cd_status == "COOLDOWN_RESTING_2H"
            or cd_status == "OUTSIDE_HUMAN_ACTIVE_HOURS"
            or cd_status.startswith("WEEKLY_REST_DAY_OFF")
        ):
            cd_info = qm.get_cooldown_status()
            sleep_info = qm.get_sleep_until_tomorrow_info()
            now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
            m4_run_id = f"m4_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{m3_data.sub_niche_id}"
            mapped_status: Literal[
                "WITHIN_DAILY_QUOTA",
                "COOLDOWN_RESTING_2H",
                "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW",
                "WEEKLY_REST_DAY_OFF",
                "OUTSIDE_HUMAN_ACTIVE_HOURS",
            ] = (
                "WEEKLY_REST_DAY_OFF"
                if cd_status.startswith("WEEKLY_REST_DAY_OFF")
                else ("OUTSIDE_HUMAN_ACTIVE_HOURS" if cd_status == "OUTSIDE_HUMAN_ACTIVE_HOURS" else "COOLDOWN_RESTING_2H")
            )
            print(
                f"[M4 HUMAN GUARD] Tạm nghỉ tự nhiên ({cd_status}): "
                f"Còn {wait_secs}s nữa mới render tiếp.",
                flush=True,
            )
            return M4ProductionOutput(
                m4_run_id=m4_run_id,
                source_m3_run_id=m3_data.m3_run_id,
                generated_at=now_iso,
                daily_video_quota_limit=MAX_VIDEOS_PER_DAY,
                daily_videos_rendered_today=used_today,
                remaining_video_quota_today=max(0, MAX_VIDEOS_PER_DAY - used_today),
                min_render_cooldown_seconds=MIN_RENDER_COOLDOWN_SECONDS,
                next_render_allowed_at=cd_info["next_render_allowed_at"] or sleep_info["next_reset_at"],
                seconds_until_next_render=wait_secs,
                quota_status=mapped_status,
                next_quota_reset_at=sleep_info["next_reset_at"],
                seconds_until_quota_reset=sleep_info["seconds_until_reset"],
                skipped_due_to_quota_script_ids=[s.script_id for s in eligible],
                rendered_videos=[],
            )

        rerenderable: List[AuditedScriptItem] = []
        new_candidates: List[AuditedScriptItem] = []
        for s in eligible:
            if s.script_id in existing_ids:
                if allow_rerender_same_id:
                    rerenderable.append(s)
                else:
                    skipped_ids.append(s.script_id)
            else:
                if used_today < MAX_VIDEOS_PER_DAY and qm.can_render_video(
                    s.script_id, allow_rerender_same_id=allow_rerender_same_id
                ):
                    new_candidates.append(s)
                else:
                    skipped_ids.append(s.script_id)

        remaining_new_slots = max(0, MAX_VIDEOS_PER_DAY - used_today)
        if max_videos is not None:
            total_cap = max(0, max_videos)
        elif not ignore_cooldown:
            # When 2-hour cooldown is enforced, render at most 1 video per invocation, then rest 2 hours!
            total_cap = 1
        else:
            total_cap = MAX_VIDEOS_PER_DAY

        # Collect SKUs already rendered earlier today so subsequent runs on the same day never repeat a SKU
        rendered_skus_today: set[str] = set()
        for r_id in existing_ids:
            entry = self.video_registry.get_entry(r_id)
            if entry:
                rendered_skus_today.update(entry.primary_sku_ids)
                rendered_skus_today.update(entry.normalized_sku_keys)

        if allow_rerender_same_id and rerenderable:
            selected_rerender = self._select_diverse_daily_scripts(
                rerenderable,
                min(total_cap, len(rerenderable)),
                enforce_sku_cooldown=enforce_sku_cooldown,
            )
            rem_cap = max(0, total_cap - len(selected_rerender))
            used_skus = set(rendered_skus_today)
            for sr in selected_rerender:
                used_skus.update(self._get_script_sku_keys(sr))
            selected_new = self._select_diverse_daily_scripts(
                new_candidates,
                min(rem_cap, remaining_new_slots),
                enforce_sku_cooldown=enforce_sku_cooldown,
                already_rendered_skus_today=used_skus,
            )
            to_render = selected_rerender + selected_new
        else:
            to_render = self._select_diverse_daily_scripts(
                new_candidates,
                min(total_cap, remaining_new_slots),
                enforce_sku_cooldown=enforce_sku_cooldown,
                already_rendered_skus_today=rendered_skus_today,
            )

        selected_id_set = {s.script_id for s in to_render}
        for s in eligible:
            if s.script_id not in selected_id_set and s.script_id not in skipped_ids:
                skipped_ids.append(s.script_id)

        rendered_items: List[RenderedVideoItem] = []
        if to_render:
            async def _run_batch() -> List[RenderedVideoItem]:
                out: List[RenderedVideoItem] = []
                for idx, sc in enumerate(to_render, start=1):
                    voices_so_far = qm.get_rendered_voices_today() + [r.voice_name for r in out]
                    print(
                        f"\n[M4] Rendering video [{idx}/{len(to_render)}]: {sc.script_id} (Layout B1, zero-zoom)...",
                        flush=True,
                    )
                    started_iso = qm.record_render_started(sc.script_id)
                    try:
                        item = await self._produce_single_video_async(
                            sc,
                            m3_data,
                            layout_mode,
                            voices_used_today=voices_so_far,
                        )
                    except RuntimeError as render_exc:
                        if "No verified official brand videos available for script" in str(render_exc):
                            print(
                                f"[M4 SELF-HEAL] 🧹 Kịch bản `{sc.script_id}` không còn video gốc chính hãng hợp lệ "
                                f"-> tự động gỡ khỏi hàng đợi (video_registry + m2/m3) để không lặp lỗi và chuyển sang kịch bản khác!",
                                flush=True,
                            )
                            self.video_registry.delete_entry(sc.script_id)
                            self.video_registry.purge_script_from_m2_m3_files(sc.script_id)
                            if sc.script_id not in skipped_ids:
                                skipped_ids.append(sc.script_id)
                            continue
                        raise
                    ev = qm.record_rendered_video(
                        sc.script_id,
                        render_duration_sec=item.render_duration_sec,
                        voice_name=item.voice_name,
                        started_at=started_iso,
                    )
                    item.render_started_at = ev["render_started_at"]
                    item.render_finished_at = ev["render_finished_at"]
                    item.next_render_allowed_at = ev["next_render_allowed_at"]

                    norm_keys = [
                        normalize_sku_key(c.brand, c.model_code, c.candidate_id)
                        for c in sc.verified_factual_claims
                    ]
                    self.video_registry.mark_video_rendered(
                        script_id=sc.script_id,
                        video_path=item.video_path,
                        render_duration_sec=item.render_duration_sec,
                        voice_name=item.voice_name,
                        voice_gender=item.voice_gender,
                        bgm_track=item.bgm_track,
                        script_obj=sc,
                        normalized_sku_keys=norm_keys,
                    )
                    self.sku_registry.record_rendered_video_skus(
                        script_format=sc.script_format,
                        candidate_or_sku_keys=norm_keys + list(sc.primary_sku_ids),
                        script_id=sc.script_id,
                    )
                    out.append(item)
                return out

            rendered_items = asyncio.run(_run_batch())

        used_after, rem_after, _ = qm.get_video_quota_status()
        cd_after = qm.get_cooldown_status()
        sleep_info = qm.get_sleep_until_tomorrow_info()

        if rem_after == 0:
            quota_status: Literal[
                "WITHIN_DAILY_QUOTA",
                "COOLDOWN_RESTING_2H",
                "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW",
            ] = "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW"
        elif not ignore_cooldown and cd_after["is_resting"]:
            quota_status = "COOLDOWN_RESTING_2H"
        else:
            quota_status = "WITHIN_DAILY_QUOTA"

        if quota_status == "QUOTA_REACHED_SLEEP_UNTIL_TOMORROW":
            print(
                f"\n[M4 QUOTA] Đã xuất đủ {used_after}/{MAX_VIDEOS_PER_DAY} video trong ngày hôm nay! "
                f"Chuyển sang trạng thái ngủ (SLEEP) chờ đến ngày mai ({sleep_info['next_reset_at']}).",
                flush=True,
            )
            if skipped_ids:
                print(
                    f"[M4 QUOTA] Queued surplus scripts in video_registry (SCRIPT_APPROVED): {skipped_ids}",
                    flush=True,
                )
        elif quota_status == "COOLDOWN_RESTING_2H":
            print(
                f"\n[M4 COOLDOWN] Đã render xong 1 video ({used_after}/{MAX_VIDEOS_PER_DAY} trong ngày). "
                f"Module M4 nghỉ ngơi 2 tiếng đến {cd_after['next_render_allowed_at']}.",
                flush=True,
            )

        now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        m4_run_id = f"m4_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{m3_data.sub_niche_id}"

        return M4ProductionOutput(
            m4_run_id=m4_run_id,
            source_m3_run_id=m3_data.m3_run_id,
            generated_at=now_iso,
            daily_video_quota_limit=MAX_VIDEOS_PER_DAY,
            daily_videos_rendered_today=used_after,
            remaining_video_quota_today=rem_after,
            min_render_cooldown_seconds=MIN_RENDER_COOLDOWN_SECONDS,
            next_render_allowed_at=cd_after["next_render_allowed_at"],
            seconds_until_next_render=int(cd_after["seconds_until_next_render"]),
            quota_status=quota_status,
            next_quota_reset_at=sleep_info["next_reset_at"],
            seconds_until_quota_reset=sleep_info["seconds_until_reset"],
            skipped_due_to_quota_script_ids=skipped_ids,
            rendered_videos=rendered_items,
        )

    def run(
        self,
        input_path: Path = INPUT_M3_PATH,
        output_json_path: Path = OUTPUT_M4_JSON_PATH,
        layout_mode: Literal["B1", "cinema", "fullscreen"] = "B1",
        target_script_id: Optional[str] = None,
        max_videos: Optional[int] = None,
        allow_rerender_same_id: bool = True,
        ignore_cooldown: bool = True,
        enforce_sku_cooldown: bool = True,
        quota_mgr: Optional[DailyQuotaManager] = None,
    ) -> M4ProductionOutput:
        if not input_path.exists():
            raise FileNotFoundError(f"M4 input M3 contract not found: {input_path}")

        print(f"[M4] Reading M3 approved scripts from: {input_path}...", flush=True)
        m3_data = M3ApprovedScriptsOutput.model_validate_json(
            input_path.read_text(encoding="utf-8")
        )

        output_obj = self.produce_from_m3(
            m3_data=m3_data,
            layout_mode=layout_mode,
            target_script_id=target_script_id,
            max_videos=max_videos,
            allow_rerender_same_id=allow_rerender_same_id,
            ignore_cooldown=ignore_cooldown,
            enforce_sku_cooldown=enforce_sku_cooldown,
            quota_mgr=quota_mgr,
        )

        # Preserve previously rendered unposted videos in `m4_production_output.json` on disk
        # so single-video steps (`max_videos=1`) or cooldown checks never overwrite waiting videos' metadata
        existing_unposted_items: List[RenderedVideoItem] = []
        if output_json_path.exists():
            try:
                prev_m4 = M4ProductionOutput.model_validate_json(
                    output_json_path.read_text(encoding="utf-8")
                )
                unposted_ids = {
                    e.script_id for e in self.video_registry.get_rendered_unposted_videos()
                }
                new_ids = {rv.script_id for rv in output_obj.rendered_videos}
                for prv in prev_m4.rendered_videos:
                    if prv.script_id in unposted_ids and prv.script_id not in new_ids:
                        mp4_p = MacOSLegacyRuntime.resolve_portable_video_path(
                            prv.video_path, prv.script_id
                        )
                        if mp4_p.exists():
                            existing_unposted_items.append(prv)
            except Exception:
                pass

        persisted_obj = output_obj.model_copy(
            update={"rendered_videos": existing_unposted_items + list(output_obj.rendered_videos)}
        )
        atomic_write_text(output_json_path, persisted_obj.model_dump_json(indent=2))
        StorageGarbageCollector().sweep_all()
        print(f"[M4] Wrote production contract atomically to: {output_json_path}", flush=True)
        return output_obj


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Module M4: 100% Autonomous 9:16 Video Producer (Layout B1).")
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_M3_PATH,
        help="Input JSON path from M3 (default: data/m3_approved_scripts.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_M4_JSON_PATH,
        help="Output production JSON path (default: data/m4_production_output.json)",
    )
    parser.add_argument(
        "--layout",
        choices=["B1", "cinema", "fullscreen"],
        default="B1",
        help="Layout mode (default: B1 — single-frame 16:9 + same-shot ambient blur)",
    )
    parser.add_argument(
        "--script-id",
        type=str,
        default=None,
        help="Optional single script_id filter to render",
    )
    parser.add_argument(
        "--max-videos",
        type=int,
        default=None,
        help="Optional max videos to render in this run (capped at 3/day)",
    )
    parser.add_argument(
        "--ignore-cooldown",
        action="store_true",
        default=False,
        help="Bypass the 2-hour post-render cooldown (for batch verification runs)",
    )
    args = parser.parse_args()

    engine = M4VideoProducerEngine()
    engine.run(
        input_path=args.input,
        output_json_path=args.output,
        layout_mode=args.layout,  # type: ignore[arg-type]
        target_script_id=args.script_id,
        max_videos=args.max_videos,
        ignore_cooldown=args.ignore_cooldown,
    )


if __name__ == "__main__":
    main()
