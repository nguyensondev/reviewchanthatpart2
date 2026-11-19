"""
Official Manufacturer Video Verification Gate (`pipeline/brand_video_gate.py`).

Enforces User Requirement #2:
  - Only accept clean official brand/manufacturer promotional or product videos.
  - Reject user-filmed reviews, third-party shop videos with watermarks/phone banners
    (e.g., TechMall, Chube Review, Cân Hoàng Gia, Cân Bidica), and cross-brand/wrong-model clips.
  - If a SKU has no verified official brand footage, M1 disqualifies it (`NO_CLEAN_OFFICIAL_FOOTAGE`).
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.core_resilience import atomic_write_json, safe_load_json
from pipeline.schemas_m0 import YouTubeEvidenceClip
from pipeline.schemas_m1_5 import normalize_sku_key

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
WHITELIST_PATH = DATA_DIR / "brand_channel_whitelist.json"
BLACKLIST_VIDEO_IDS_PATH = DATA_DIR / "blacklisted_video_ids.json"

# Hard-blocked non-product / entertainment / movie / CapCut / gaming keywords in video titles or channel names
FORBIDDEN_NON_PRODUCT_KEYWORDS: set[str] = {
    "war", "battle", "soldier", "army", "military", "tank", "combat", "weapon",
    "movie", "movies", "film", "films", "trailer", "teaser", "cinema", "netflix",
    "capcut", "tiktok", "douyin", "edit", "edits", "amv", "fmv", "gmv",
    "gameplay", "walkthrough", "esports", "valorant", "pubg", "minecraft", "roblox", "fortnite",
    "music", "song", "songs", "remix", "ost", "lyrics", "karaoke", "concert",
    "anime", "manga", "cartoon", "meme", "memes", "funny", "prank", "comedy", "drama",
    "episode", "season", "vlog", "reaction", "podcast",
    "phim", "chien tranh", "hoat hinh", "nhac", "bai hat", "hai huoc", "review phim", "tom tat phim",
}

# Mandatory product / appliance / tech domain anchor keywords (at least 1 whole word/phrase must appear in dynamic video titles)
PRODUCT_ANCHOR_KEYWORDS: Tuple[str, ...] = (
    "light", "lamp", "screen", "monitor", "led", "iwok",
    "mouse", "ergonomic", "lift", "vertical", "wireless", "bluetooth",
    "stand", "laptop", "holder", "riser", "foldable", "aluminum",
    "charger", "gan", "charging", "power", "adapter", "station",
    "hub", "dock", "docking", "usb", "hdmi", "type c", "multiport",
    "keyboard", "mechanical", "keychron", "switch", "low profile",
    "arm", "mount", "gas spring", "vesa",
    "cable", "magnetic", "retractable", "cord",
    "duster", "vacuum", "cleaner", "blower", "compressed air", "suction",
    "scale", "weighing", "kitchen", "cooking", "gram", "tanita",
    "thermometer", "temperature", "probe", "meat", "food",
    "sealer", "vacuum sealer", "packaging", "preservation",
    "sprayer", "oil", "mist", "bottle", "dispenser", "soap", "foam", "touchless",
    "frother", "milk", "coffee", "matcha", "whisk", "espresso", "grinder", "burr",
    "kettle", "gooseneck", "boiling", "teapot", "timer", "rotary", "stopwatch",
    "appliance", "appliances", "gadget", "unboxing", "unbox", "setup", "desk",
    "den", "man hinh", "chuot", "cong thai hoc", "gia do", "ke laptop",
    "sac", "tram sac", "cu sac", "bo chia", "ban phim", "tay nang", "gia treo",
    "cap sac", "may thoi", "may hut", "hut bui", "can dien tu", "can tieu ly",
    "nhiet ke", "hut chan khong", "han mieng tui", "binh xit", "chai xit",
    "danh bot", "tao bot", "am sieu toc", "am dun", "may xay", "coi xay",
    "dong ho", "bam gio", "hen gio", "xa phong", "rua chen", "gia dung", "nha bep",
)

# Short brand names that collide with common words / movie / gaming channels on YouTube
AMBIGUOUS_SHORT_BRANDS: set[str] = {
    "bear", "mi", "delux", "simplus", "joyoung", "midea",
}


def _norm_text(s: str) -> str:
    s = (s or "").replace("đ", "d").replace("Đ", "D")
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", ascii_str).strip()


def _has_whole_word(haystack_norm: str, needle_norm: str) -> bool:
    if not haystack_norm or not needle_norm:
        return False
    return bool(re.search(rf"\b{re.escape(needle_norm)}\b", haystack_norm))


class OfficialBrandVideoGate:
    """Validates and resolves official brand YouTube videos for candidate SKUs."""

    def __init__(
        self,
        whitelist_path: Path = WHITELIST_PATH,
        blacklist_path: Path = BLACKLIST_VIDEO_IDS_PATH,
    ) -> None:
        self.whitelist_path = whitelist_path
        self.blacklist_path = blacklist_path
        self.data = self._load()

    def _load_blacklist_map(self) -> Dict[str, Any]:
        raw = safe_load_json(self.blacklist_path, lambda: {"blacklisted_videos": {}})
        if isinstance(raw, dict) and isinstance(raw.get("blacklisted_videos"), dict):
            return raw["blacklisted_videos"]
        return {}

    def is_video_blacklisted(self, video_id: str) -> bool:
        if not video_id:
            return True
        return video_id in self._load_blacklist_map()

    def blacklist_video_id(self, video_id: str, reason_vi: str, script_id: str = "") -> None:
        """Permanently blacklist a bad YouTube video_id and delete its local cached MP4/scan files."""
        if not video_id:
            return
        import datetime as dt

        raw = safe_load_json(self.blacklist_path, lambda: {"blacklisted_videos": {}})
        if not isinstance(raw, dict):
            raw = {"blacklisted_videos": {}}
        bmap = raw.setdefault("blacklisted_videos", {})
        bmap[video_id] = {
            "video_id": video_id,
            "reason_vi": reason_vi,
            "script_id": script_id,
            "blacklisted_at": dt.datetime.now().astimezone().isoformat(timespec="seconds"),
        }
        atomic_write_json(self.blacklist_path, raw)

        # Purge cached files for this video_id from m4_video_cache and brand_official_cache
        for cache_folder in (DATA_DIR / "m4_video_cache", DATA_DIR / "brand_official_cache"):
            if cache_folder.exists():
                for fp in cache_folder.glob(f"{video_id}.*"):
                    try:
                        fp.unlink(missing_ok=True)
                    except OSError:
                        pass

    def _load(self) -> Dict[str, Any]:
        if not self.whitelist_path.exists():
            return {
                "official_brand_channels": {},
                "blocked_third_party_channel_keywords": [],
                "curated_official_videos": {},
                "verified_clean_shot_intervals": {},
            }
        try:
            return json.loads(self.whitelist_path.read_text(encoding="utf-8"))
        except Exception:
            return {
                "official_brand_channels": {},
                "blocked_third_party_channel_keywords": [],
                "curated_official_videos": {},
                "verified_clean_shot_intervals": {},
            }

    def is_official_brand_clip(
        self,
        clip: YouTubeEvidenceClip,
        brand: str,
        model_code: Optional[str] = None,
    ) -> bool:
        """
        Return True ONLY if `clip` comes from the brand's official channel (not a 3rd-party shop
        or user reviewer) and its title matches the brand/product without referencing a competing brand.
        """
        brand_norm = _norm_text(brand)
        if not brand_norm or brand_norm in ("oem", "generic"):
            return False

        if self.is_video_blacklisted(clip.video_id):
            return False

        # 1. Check curated official video IDs first
        curated_map = self.data.get("curated_official_videos") or {}
        for _, clip_list in curated_map.items():
            for item in clip_list:
                if item.get("video_id") == clip.video_id:
                    if brand_norm in _norm_text(item.get("channel", "")) or brand_norm in _norm_text(item.get("title", "")):
                        return True

        channel_norm = _norm_text(clip.channel)
        title_norm = _norm_text(clip.title)
        if not channel_norm or not title_norm:
            return False

        # 2. Hard block non-product entertainment / war / movie / CapCut / gaming videos
        for bad_kw in FORBIDDEN_NON_PRODUCT_KEYWORDS:
            if _has_whole_word(title_norm, bad_kw) or _has_whole_word(channel_norm, bad_kw):
                return False

        # 3. Hard block third-party shops / review channels
        blocked_kws = self.data.get("blocked_third_party_channel_keywords") or []
        for kw in blocked_kws:
            kw_norm = _norm_text(kw)
            if kw_norm and (kw_norm in channel_norm or kw_norm in title_norm):
                return False

        # Reject obvious user-review / comparison titles
        review_markers = ("so sanh", "top 5", "top 10", "after 2 years", "sau 2 nam", "co nen mua")
        if any(m in title_norm for m in review_markers):
            return False

        # 4. Verify channel matches official brand channel whitelist (NEVER blindly trust unlisted brands or bare ambiguous names!)
        official_map: Dict[str, List[str]] = self.data.get("official_brand_channels") or {}
        allowed_channels = official_map.get(brand_norm, []) or official_map.get(brand.lower().strip(), [])
        allowed_norms = {_norm_text(ch) for ch in allowed_channels if ch}
        if allowed_channels:
            allowed_norms.add(f"{brand_norm} official")
            allowed_norms.add(f"{brand_norm} global")
            if brand_norm not in AMBIGUOUS_SHORT_BRANDS:
                allowed_norms.add(brand_norm)
        else:
            # Unlisted brand: only trust explicitly suffixed official/global manufacturer channels
            allowed_norms.add(f"{brand_norm} official")
            allowed_norms.add(f"{brand_norm} global")

        if channel_norm not in allowed_norms:
            return False

        # 5. For ambiguous short brand channels (like "Bear", "Mi"), require explicit official channel suffix or model+anchor match
        if brand_norm in AMBIGUOUS_SHORT_BRANDS and channel_norm in ("bear", "mi", "delux"):
            return False

        # 6. Mandatory Product Anchor Keyword Gate: video title MUST contain at least 1 real appliance/tech keyword
        if not any(_has_whole_word(title_norm, anchor) for anchor in PRODUCT_ANCHOR_KEYWORDS):
            return False

        # 7. Whole-Word Brand + Model Verification (no 2-char substring false matches like 'vs' or 'm1'!)
        if not _has_whole_word(title_norm, brand_norm):
            return False

        model_norm = _norm_text(model_code or "")
        model_tokens = [
            t for t in model_norm.split()
            if (len(t) >= 3 or (len(t) == 2 and any(ch.isdigit() for ch in t)))
            and t not in ("series", "pro", "plus", "mini", "max", "lite")
        ]
        if model_tokens:
            if not any(_has_whole_word(title_norm, tok) for tok in model_tokens):
                return False

        return True

    def filter_and_resolve_official_clips(
        self,
        brand: str,
        model_code: Optional[str],
        candidate_id: str,
        raw_clips: List[YouTubeEvidenceClip],
    ) -> List[YouTubeEvidenceClip]:
        """
        Return ONLY verified official brand clips for `(brand, model_code, candidate_id)`.
        Injects curated official brand studio videos when available for the SKU.
        """
        verified: List[YouTubeEvidenceClip] = []
        seen_ids: set[str] = set()

        sku_key = normalize_sku_key(brand, model_code, candidate_id)
        cand_key = re.sub(r"[^a-z0-9]+", "_", (candidate_id or "").lower()).strip("_")
        curated_map = self.data.get("curated_official_videos") or {}

        for key in (sku_key, cand_key):
            for item in curated_map.get(key, []):
                vid = item.get("video_id")
                if vid and vid not in seen_ids and not self.is_video_blacklisted(vid):
                    seen_ids.add(vid)
                    verified.append(YouTubeEvidenceClip.model_validate(item))

        for clip in raw_clips:
            if clip.video_id not in seen_ids and self.is_official_brand_clip(clip, brand, model_code):
                seen_ids.add(clip.video_id)
                verified.append(clip)

        return verified

    def get_verified_clean_intervals(self, video_id: str) -> List[Tuple[float, float]]:
        intervals = (self.data.get("verified_clean_shot_intervals") or {}).get(video_id) or []
        out: List[Tuple[float, float]] = []
        for pair in intervals:
            if isinstance(pair, list) and len(pair) >= 2:
                out.append((float(pair[0]), float(pair[1])))
        return out

    def get_verified_clean_intervals_with_tags(self, video_id: str) -> List[Tuple[float, float, str]]:
        intervals = (self.data.get("verified_clean_shot_intervals") or {}).get(video_id) or []
        out: List[Tuple[float, float, str]] = []
        for pair in intervals:
            if isinstance(pair, list) and len(pair) >= 2:
                tag = str(pair[2]) if len(pair) >= 3 else ""
                out.append((float(pair[0]), float(pair[1]), tag))
        return out

    def estimate_sku_clean_footage_sec(
        self,
        brand: str,
        model_code: Optional[str],
        candidate_id: str,
        raw_clips: List[YouTubeEvidenceClip],
    ) -> float:
        """
        Estimate total non-overlapping clean official footage (in seconds) available for a SKU
        so M2 can scale script word count naturally without ever forcing M4 to loop/repeat video.
        """
        clips = self.filter_and_resolve_official_clips(brand, model_code, candidate_id, raw_clips)
        total_sec = 0.0
        for c in clips:
            ivs = self.get_verified_clean_intervals(c.video_id)
            if ivs:
                total_sec += sum(max(0.0, b - a) for a, b in ivs if b - a >= 1.5)
            else:
                total_sec += min(38.0, max(18.0, float(c.duration_sec or 45) * 0.55))
        return round(total_sec, 1)
