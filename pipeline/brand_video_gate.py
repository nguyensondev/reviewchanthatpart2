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

from pipeline.schemas_m0 import YouTubeEvidenceClip
from pipeline.schemas_m1_5 import normalize_sku_key

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WHITELIST_PATH = PROJECT_ROOT / "data" / "brand_channel_whitelist.json"


def _norm_text(s: str) -> str:
    s = (s or "").replace("đ", "d").replace("Đ", "D")
    nfkd = unicodedata.normalize("NFKD", s)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", ascii_str).strip()


class OfficialBrandVideoGate:
    """Validates and resolves official brand YouTube videos for candidate SKUs."""

    def __init__(self, whitelist_path: Path = WHITELIST_PATH) -> None:
        self.whitelist_path = whitelist_path
        self.data = self._load()

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

        # 1. Check curated official video IDs first
        curated_map = self.data.get("curated_official_videos") or {}
        for _, clip_list in curated_map.items():
            for item in clip_list:
                if item.get("video_id") == clip.video_id:
                    # Ensure it belongs to the same brand
                    if brand_norm in _norm_text(item.get("channel", "")) or brand_norm in _norm_text(item.get("title", "")):
                        return True

        channel_norm = _norm_text(clip.channel)
        title_norm = _norm_text(clip.title)
        if not channel_norm:
            return False

        # 2. Hard block third-party shops / review channels
        blocked_kws = self.data.get("blocked_third_party_channel_keywords") or []
        for kw in blocked_kws:
            kw_norm = _norm_text(kw)
            if kw_norm and (kw_norm in channel_norm or kw_norm in title_norm):
                return False

        # Reject obvious user-review / comparison titles
        review_markers = ("so sanh", "top 5", "top 10", "after 2 years", "sau 2 nam", "co nen mua")
        if any(m in title_norm for m in review_markers):
            return False

        # 3. Verify channel matches official brand channel whitelist (or exact brand name)
        official_map: Dict[str, List[str]] = self.data.get("official_brand_channels") or {}
        allowed_channels = official_map.get(brand_norm, []) or official_map.get(brand.lower().strip(), [])
        allowed_norms = {_norm_text(ch) for ch in allowed_channels if ch}
        allowed_norms.add(brand_norm)
        allowed_norms.add(f"{brand_norm} official")
        allowed_norms.add(f"{brand_norm} global")

        if channel_norm not in allowed_norms:
            return False

        # 4. Verify title references the brand or model code
        model_norm = _norm_text(model_code or "")
        model_tokens = [t for t in model_norm.split() if len(t) >= 2 and t not in ("series", "pro", "plus")]
        if model_tokens:
            if not any(tok in title_norm for tok in model_tokens) and brand_norm not in title_norm:
                return False
        elif brand_norm not in title_norm:
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
                if vid and vid not in seen_ids:
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
