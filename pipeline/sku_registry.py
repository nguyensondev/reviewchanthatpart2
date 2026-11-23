"""
Persistent Crash-Safe SKU Lifecycle & Cooldown Registry (`pipeline/sku_registry.py`).

Decoupled state store (`data/sku_registry.json`) using atomic `fsync` + `.bak` writes from
`pipeline/core_resilience.py` so `M0`, `M1.5`, `M2`, `M4`, and `PublishPackager` never import
each other's stage modules.
"""
from __future__ import annotations

import datetime as dt
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import atomic_write_json, safe_load_json
from pipeline.schemas_m0 import DomainId
from pipeline.schemas_m1 import M1QualifiedSKUsOutput
from pipeline.schemas_m1_5 import (
    MicroSceneId,
    SKURegistryEntry,
    compute_sku_lifecycle_status,
    normalize_sku_key,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SKU_REGISTRY_PATH = DATA_DIR / "sku_registry.json"

MIN_SKU_COOLDOWN_DAYS = 3
MAX_SKU_COOLDOWN_DAYS = 7


def classify_micro_scene(domain: DomainId, category_id: str, product_name_vi: str = "") -> MicroSceneId:
    """Deterministically assign a canonical `MicroSceneId` based on domain and category/product type."""
    text = f"{category_id} {product_name_vi}".lower()
    if domain == "TECH_DESK":
        power_kws = ("hub", "charger", "gan", "cable", "cap", "sac", "adapter", "dock", "power", "usb_c_hub")
        if any(k in text for k in power_kws):
            return "DESK_POWER_CONNECTIVITY"
        return "DESK_ERGONOMIC_WORKFLOW"
    else:
        prep_kws = ("scale", "can", "thermometer", "nhiet_ke", "frother", "coffee", "timer", "measuring", "chopper", "xay")
        if any(k in text for k in prep_kws):
            return "KITCHEN_PRECISION_PREP"
        return "KITCHEN_COOKING_STORAGE"


class SKURegistryManager:
    """Persists SKU lifecycle states (`has_solo_video`, `has_combo_video`, `lifecycle_status`) atomically."""

    def __init__(self, registry_path: Path = SKU_REGISTRY_PATH) -> None:
        self.registry_path = registry_path
        self.brand_gate = OfficialBrandVideoGate()

    def _load_raw(self) -> Dict[str, Any]:
        data = safe_load_json(self.registry_path, lambda: {"version": "1.0.0", "items": {}})
        if isinstance(data, dict) and isinstance(data.get("items"), dict):
            return data
        return {"version": "1.0.0", "items": {}}

    def _save_raw(self, data: Dict[str, Any]) -> None:
        atomic_write_json(self.registry_path, data)

    def upsert_from_m1(self, m1_data: M1QualifiedSKUsOutput) -> List[SKURegistryEntry]:
        raw = self._load_raw()
        items = raw.setdefault("items", {})
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        synced: List[SKURegistryEntry] = []

        for cat in m1_data.qualified_categories:
            w = cat.primary_winner
            official_clips = self.brand_gate.filter_and_resolve_official_clips(
                brand=w.brand,
                model_code=w.model_code,
                candidate_id=w.candidate_id,
                raw_clips=w.youtube_evidence,
            )
            if not official_clips:
                continue

            sku_key = normalize_sku_key(w.brand, w.model_code, w.candidate_id)
            existing = items.get(sku_key) if isinstance(items.get(sku_key), dict) else {}

            has_solo = bool(existing.get("has_solo_video", False))
            has_combo = bool(existing.get("has_combo_video", False))
            solo_ids = list(existing.get("solo_video_script_ids") or [])
            combo_ids = list(existing.get("combo_video_script_ids") or [])
            last_rendered_at = existing.get("last_rendered_at")
            cooldown_days = int(existing.get("cooldown_days", 0) or 0)
            next_eligible_at = existing.get("next_eligible_at")
            status = compute_sku_lifecycle_status(has_solo, has_combo)
            micro_scene = classify_micro_scene(cat.domain, cat.category_id, w.product_name_vi)

            entry = SKURegistryEntry(
                sku_key=sku_key,
                candidate_id=w.candidate_id,
                brand=w.brand,
                model_code=w.model_code,
                product_name_vi=w.product_name_vi,
                domain=cat.domain,
                category_id=cat.category_id,
                category_name_vi=cat.category_name_vi,
                micro_scene_id=micro_scene,
                estimated_market_price_vnd=w.estimated_market_price_vnd,
                key_specs=dict(w.key_specs),
                highlight_pros_vi=list(w.highlight_pros_vi),
                honest_caveat_vi=w.honest_caveat_vi,
                caveat_severity=w.caveat_severity,
                m1_total_score_100=w.score_breakdown.total_score_100,
                best_affiliate_offer=w.best_affiliate_offer,
                youtube_evidence=official_clips,
                has_solo_video=has_solo,
                has_combo_video=has_combo,
                solo_video_script_ids=solo_ids,
                combo_video_script_ids=combo_ids,
                lifecycle_status=status,
                last_rendered_at=last_rendered_at,
                cooldown_days=cooldown_days,
                next_eligible_at=next_eligible_at,
                updated_at=now_iso,
            )
            items[sku_key] = entry.model_dump(mode="json")
            synced.append(entry)

        self._save_raw(raw)
        return synced

    def get_all_entries(self) -> Dict[str, SKURegistryEntry]:
        raw = self._load_raw()
        out: Dict[str, SKURegistryEntry] = {}
        for k, v in (raw.get("items") or {}).items():
            if isinstance(v, dict):
                try:
                    out[k] = SKURegistryEntry.model_validate(v)
                except Exception:
                    pass
        return out

    def resolve_sku_key_by_candidate_id(self, candidate_id: str) -> Optional[str]:
        entries = self.get_all_entries()
        if candidate_id in entries:
            return candidate_id
        for k, entry in entries.items():
            if entry.candidate_id == candidate_id:
                return k
        return None

    def is_in_cooldown(
        self,
        sku_key_or_candidate_id: str,
        now: Optional[dt.datetime] = None,
    ) -> bool:
        entries = self.get_all_entries()
        key = self.resolve_sku_key_by_candidate_id(sku_key_or_candidate_id) or sku_key_or_candidate_id
        entry = entries.get(key)
        if not entry or not entry.next_eligible_at:
            return False
        now_dt = now or dt.datetime.now().astimezone()
        try:
            next_dt = dt.datetime.fromisoformat(entry.next_eligible_at)
            if next_dt.tzinfo is None:
                next_dt = next_dt.replace(tzinfo=now_dt.tzinfo)
            return now_dt < next_dt
        except Exception:
            return False

    def is_solo_eligible(
        self,
        sku_key_or_candidate_id: str,
        now: Optional[dt.datetime] = None,
        ignore_sku_cooldown: bool = False,
    ) -> bool:
        entries = self.get_all_entries()
        key = self.resolve_sku_key_by_candidate_id(sku_key_or_candidate_id) or sku_key_or_candidate_id
        entry = entries.get(key)
        if not entry:
            return True
        if entry.has_solo_video or entry.lifecycle_status == "RETIRED_EXHAUSTED":
            return False
        if not ignore_sku_cooldown and self.is_in_cooldown(key, now=now):
            return False
        return True

    def is_combo_eligible(
        self,
        sku_key_or_candidate_id: str,
        now: Optional[dt.datetime] = None,
        ignore_sku_cooldown: bool = False,
    ) -> bool:
        entries = self.get_all_entries()
        key = self.resolve_sku_key_by_candidate_id(sku_key_or_candidate_id) or sku_key_or_candidate_id
        entry = entries.get(key)
        if not entry:
            return True
        if entry.has_combo_video or entry.lifecycle_status == "RETIRED_EXHAUSTED":
            return False
        if not ignore_sku_cooldown and self.is_in_cooldown(key, now=now):
            return False
        return True

    def get_recent_categories(
        self,
        within_days: int = 14,
        now: Optional[dt.datetime] = None,
    ) -> Dict[str, str]:
        """
        Return `{category_id: latest_iso}` for every category that had a rendered/published video
        within `within_days` (default 14 days), combining `sku_registry.json` and `video_registry.json`.
        """
        now_dt = now or dt.datetime.now().astimezone()
        cutoff = now_dt - dt.timedelta(days=max(1, int(within_days)))
        recent: Dict[str, str] = {}

        def _consider(cat_id: str, ts_iso: Optional[str]) -> None:
            if not cat_id or not ts_iso:
                return
            try:
                ts_dt = dt.datetime.fromisoformat(str(ts_iso))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=now_dt.tzinfo)
                if ts_dt >= cutoff:
                    prev = recent.get(cat_id)
                    if not prev or ts_iso > prev:
                        recent[cat_id] = str(ts_iso)
            except Exception:
                pass

        for e in self.get_all_entries().values():
            if e.has_solo_video or e.has_combo_video or e.last_rendered_at:
                _consider(e.category_id, e.last_rendered_at or e.updated_at)

        # Also cross-check video_registry.json (read-only) for RENDERED/PUBLISHED videos
        vreg_path = self.registry_path.parent / "video_registry.json"
        if vreg_path.exists():
            try:
                vdata = safe_load_json(vreg_path, lambda: {})
                vitems = vdata.get("items") or vdata.get("entries") or vdata.get("videos") or {}
                if isinstance(vitems, dict):
                    for ve in vitems.values():
                        if isinstance(ve, dict) and ve.get("status") in ("RENDERED", "PUBLISHED"):
                            _consider(
                                str(ve.get("category_id") or ""),
                                str(ve.get("published_at") or ve.get("rendered_at") or ve.get("updated_at") or ""),
                            )
            except Exception:
                pass
        return recent

    def get_recent_brand_categories(
        self,
        within_days: int = 30,
        now: Optional[dt.datetime] = None,
    ) -> Dict[str, List[str]]:
        """
        Return `{category_id: [brand_norm, ...]}` for every `(brand, category_id)` that had a
        rendered/published video within `within_days` (default 30 days).
        Prevents sibling-variant repeats like `Baseus i-wok2` followed 4 days later by `Baseus i-Wok Pro`.
        """
        now_dt = now or dt.datetime.now().astimezone()
        cutoff = now_dt - dt.timedelta(days=max(1, int(within_days)))
        out: Dict[str, List[str]] = {}
        for e in self.get_all_entries().values():
            if not (e.has_solo_video or e.has_combo_video or e.last_rendered_at):
                continue
            ts_iso = e.last_rendered_at or e.updated_at
            if not ts_iso:
                continue
            try:
                ts_dt = dt.datetime.fromisoformat(str(ts_iso))
                if ts_dt.tzinfo is None:
                    ts_dt = ts_dt.replace(tzinfo=now_dt.tzinfo)
                if ts_dt < cutoff:
                    continue
            except Exception:
                continue
            b_norm = re.sub(r"[^a-z0-9]", "", (e.brand or "").lower())
            if b_norm and e.category_id:
                bucket = out.setdefault(e.category_id, [])
                if b_norm not in bucket:
                    bucket.append(b_norm)
        return out

    def is_brand_category_recently_covered(
        self,
        brand: str,
        category_id: str,
        within_days: int = 30,
        now: Optional[dt.datetime] = None,
    ) -> bool:
        b_norm = re.sub(r"[^a-z0-9]", "", (brand or "").lower())
        if not b_norm or not category_id:
            return False
        recent_map = self.get_recent_brand_categories(within_days=within_days, now=now)
        return b_norm in recent_map.get(category_id, [])

    def get_blocked_categories_and_skus(
        self,
        now: Optional[dt.datetime] = None,
        category_cooldown_days: int = 14,
        brand_category_cooldown_days: int = 30,
    ) -> Dict[str, List[str]]:
        entries = self.get_all_entries()
        recent_cats = self.get_recent_categories(within_days=category_cooldown_days, now=now)
        cooldown_cats: List[str] = list(recent_cats.keys())
        blocked_skus: List[str] = []
        for k, e in entries.items():
            in_cd = self.is_in_cooldown(k, now=now)
            if in_cd and e.category_id not in cooldown_cats:
                cooldown_cats.append(e.category_id)
            if e.lifecycle_status == "RETIRED_EXHAUSTED" or in_cd:
                blocked_skus.append(f"{e.brand} {e.model_code or e.candidate_id}".strip())
        recent_bc = self.get_recent_brand_categories(within_days=brand_category_cooldown_days, now=now)
        blocked_bc_labels = [
            f"{b} ({cid})"
            for cid, brands in recent_bc.items()
            for b in brands
        ]
        return {
            "cooldown_categories": cooldown_cats,
            "blocked_sku_names": blocked_skus,
            "blocked_brand_categories": blocked_bc_labels,
        }

    def record_rendered_video_skus(
        self,
        script_format: str,
        candidate_or_sku_keys: List[str],
        script_id: str,
        now: Optional[dt.datetime] = None,
        override_cooldown_days: Optional[int] = None,
    ) -> List[SKURegistryEntry]:
        raw = self._load_raw()
        items = raw.setdefault("items", {})
        now_dt = now or dt.datetime.now().astimezone()
        now_iso = now_dt.isoformat(timespec="seconds")
        updated_entries: List[SKURegistryEntry] = []

        for cid_or_key in candidate_or_sku_keys:
            resolved_key = None
            if cid_or_key in items:
                resolved_key = cid_or_key
            else:
                for k, v in items.items():
                    if isinstance(v, dict) and v.get("candidate_id") == cid_or_key:
                        resolved_key = k
                        break
            if not resolved_key or not isinstance(items.get(resolved_key), dict):
                continue

            entry_dict = items[resolved_key]
            if script_format == "SINGLE_SKU_DEEP_DIVE":
                entry_dict["has_solo_video"] = True
                solo_list = list(entry_dict.get("solo_video_script_ids") or [])
                if script_id not in solo_list:
                    solo_list.append(script_id)
                entry_dict["solo_video_script_ids"] = solo_list
            elif script_format == "COMBO_WORKFLOW_SYNERGY":
                entry_dict["has_combo_video"] = True
                combo_list = list(entry_dict.get("combo_video_script_ids") or [])
                if script_id not in combo_list:
                    combo_list.append(script_id)
                entry_dict["combo_video_script_ids"] = combo_list

            has_solo = bool(entry_dict.get("has_solo_video", False))
            has_combo = bool(entry_dict.get("has_combo_video", False))
            new_status = compute_sku_lifecycle_status(has_solo, has_combo)
            entry_dict["lifecycle_status"] = new_status
            entry_dict["last_rendered_at"] = now_iso

            if new_status != "RETIRED_EXHAUSTED":
                cd_days = (
                    override_cooldown_days
                    if override_cooldown_days is not None
                    else random.randint(MIN_SKU_COOLDOWN_DAYS, MAX_SKU_COOLDOWN_DAYS)
                )
                entry_dict["cooldown_days"] = cd_days
                if cd_days > 0:
                    entry_dict["next_eligible_at"] = (
                        now_dt + dt.timedelta(days=cd_days)
                    ).isoformat(timespec="seconds")
                else:
                    entry_dict["next_eligible_at"] = None
            else:
                entry_dict["cooldown_days"] = 0
                entry_dict["next_eligible_at"] = None

            entry_dict["updated_at"] = now_iso
            items[resolved_key] = entry_dict
            updated_entries.append(SKURegistryEntry.model_validate(entry_dict))

        self._save_raw(raw)
        return updated_entries

    def refund_rejected_video_skus(self, script_id: str) -> bool:
        """
        Roll back `solo_video_script_ids` / `combo_video_script_ids` and SKU cooldown lock
        when a rendered video is rejected and purged by M4.5 Vision QA/QC.
        """
        if not script_id:
            return False
        raw = self._load_raw()
        items = raw.get("items")
        if not isinstance(items, dict):
            return False

        changed = False
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        for _, entry_dict in items.items():
            if not isinstance(entry_dict, dict):
                continue
            solo_list = list(entry_dict.get("solo_video_script_ids") or [])
            combo_list = list(entry_dict.get("combo_video_script_ids") or [])
            item_changed = False
            if script_id in solo_list:
                solo_list.remove(script_id)
                entry_dict["solo_video_script_ids"] = solo_list
                entry_dict["has_solo_video"] = bool(solo_list)
                item_changed = True
            if script_id in combo_list:
                combo_list.remove(script_id)
                entry_dict["combo_video_script_ids"] = combo_list
                entry_dict["has_combo_video"] = bool(combo_list)
                item_changed = True
            if item_changed:
                has_solo = bool(entry_dict.get("has_solo_video", False))
                has_combo = bool(entry_dict.get("has_combo_video", False))
                entry_dict["lifecycle_status"] = compute_sku_lifecycle_status(has_solo, has_combo)
                if not has_solo and not has_combo:
                    entry_dict["cooldown_days"] = 0
                    entry_dict["next_eligible_at"] = None
                    entry_dict["last_rendered_at"] = None
                entry_dict["updated_at"] = now_iso
                changed = True

        if changed:
            self._save_raw(raw)
        return changed
