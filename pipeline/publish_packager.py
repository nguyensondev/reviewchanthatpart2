"""
Step A + Semantic SEO Engine: Publish Packager & Official Shopee Affiliate Link Resolver (`pipeline/publish_packager.py`).

Enforces:
  1. 3-Tier Official Shopee Mall Affiliate Guarantee (`affiliate_id = 17337280644`):
     - Tier 1 (`SHOPEE_CSV` / `MANUAL_JSON`): Direct short link from user's Shopee Affiliate CSV or manual override.
     - Tier 2 (`TIER_1_OFFICIAL_MALL_ITEM`): Canonical product URL verified against `data/shopee_mall_shop_whitelist.json`.
     - Tier 3 (`TIER_2_SHOPEE_MALL_SEARCH`): Official Shopee Mall Search Affiliate Link (100% never out-of-stock).
  2. Dual-Platform Semantic SEO Metadata Engine (for Module M5):
     - Dual-Head Buyer-Intent Titles (`[Từ khóa công năng] + [Mốc giá / Câu hỏi] + [Brand + Model]`)
     - Exact `0:00` Chapter Timestamps derived from M4's rendered `sub_shots`
     - 4-Block Semantic SEO YouTube Shorts Description + 12-15 Accented/Unaccented Backend Video Tags (`youtube_video_tags`)
     - High-Retention `✅/⚠️` Checklist Caption for Facebook Reels + First Comment Affiliate Payload
     - Focused `3 + 2 + 1` Hashtag Strategy (6 sharp hashtags, zero spam).
"""
from __future__ import annotations

import datetime as dt
import json
import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field

from pipeline.cloudflare_shortener import CloudflareShortLinkManager
from pipeline.core_resilience import (
    HumanBehaviorGuard,
    MacOSLegacyRuntime,
    atomic_write_json,
    atomic_write_text,
    safe_load_json,
)
from pipeline.schemas_m0 import AffiliateListingMatch
from pipeline.schemas_m1_5 import SKURegistryEntry, normalize_sku_key
from pipeline.schemas_m3 import AuditedScriptItem
from pipeline.schemas_m4 import M4ProductionOutput, RenderedVideoItem, SocialPostPack
from pipeline.schemas_m5 import (
    PublishManifestOutput,
    PublishSKUAffiliateItem,
    PublishVideoPackageItem,
)
from pipeline.schemas_m_trend import load_active_trend_opportunities
from pipeline.shared_utils import (
    AffiliateCSVRepository,
    format_vnd_short,
    sanitize_youtube_metadata_text,
    sanitize_youtube_tags,
    strip_accents as _strip_accents,
    truncate_words_clean,
)
from pipeline.sku_registry import SKURegistryManager
from pipeline.video_registry import VideoRegistryEntry, VideoRegistryManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"
PUBLISH_PACK_DIR = OUTPUT_DIR / "publish_pack"

DEFAULT_MANUAL_LINKS_PATH = DATA_DIR / "manual_affiliate_links.json"
DEFAULT_PUBLISH_MANIFEST_PATH = PUBLISH_PACK_DIR / "publish_manifest.json"
DEFAULT_PUBLISH_MARKDOWN_PATH = PUBLISH_PACK_DIR / "BANG_DANG_BAI_HANG_NGAY.md"
DEFAULT_M4_OUTPUT_PATH = DATA_DIR / "m4_production_output.json"

CATEGORY_SHORT_KEYWORDS_VI: Dict[str, Tuple[str, List[str]]] = {
    # --- CLEANING_CARE (12 Categories) ---
    "mini_cordless_handheld_vacuum": (
        "Máy Hút Bụi Cầm Tay Không Dây Mini",
        ["#MayHutBuiMini", "#VeSinhNhaCua", "#MeoDonNha"],
    ),
    "uv_mattress_dust_mite_vacuum": (
        "Máy Hút Bụi Giường Nệm Diệt Khuẩn UV",
        ["#MayHutBuiGiuongNem", "#VeSinhChanNem", "#NhaSachTienNghi"],
    ),
    "cordless_electric_spin_scrubber": (
        "Chổi Cọ Nhà Tắm Xoay Điện Không Dây",
        ["#ChoiCoNhaTam", "#VeSinhNhaTam", "#MeoDonNha"],
    ),
    "spray_flat_mop_microfiber": (
        "Cây Lau Nhà Phun Sương Xoay 360 Độ",
        ["#CayLauNhaPhunSuong", "#MeoDonNha", "#VeSinhNhaCua"],
    ),
    "spin_mop_dual_bucket_self_wash": (
        "Bộ Cây Lau Nhà Tự Vắt Tách Nước Bẩn",
        ["#CayLauNhaTuVat", "#MeoDonNha", "#VeSinhNhaCua"],
    ),
    "spot_carpet_upholstery_cleaner": (
        "Máy Giặt Thảm & Sofa Cầm Tay Phun Hút",
        ["#MayGiatSofa", "#VeSinhSofa", "#NhaSachTienNghi"],
    ),
    "smart_touchless_trash_can": (
        "Thùng Rác Thông Minh Cảm Ứng Khử Mùi",
        ["#ThungRacThongMinh", "#GiaDungThongMinh", "#NhaSachTienNghi"],
    ),
    "automatic_touchless_foam_soap_dispenser": (
        "Máy Tạo Bọt Xà Phòng Cảm Ứng Tự Động",
        ["#MayTaoBotXaPhong", "#GiaDungMini", "#NhaSachTienNghi"],
    ),
    "rechargeable_fabric_lint_remover": (
        "Máy Cắt Lông Xù Quần Áo Sạc Pin",
        ["#MayCatLongXu", "#ChamSocQuanAo", "#GiaDungMini"],
    ),
    "ultrasonic_jewelry_glasses_cleaner": (
        "Máy Rửa Kính Mắt & Trang Sức Sóng Siêu Âm",
        ["#MayRuaSieuAm", "#VeSinhKinhMat", "#GiaDungMini"],
    ),
    "cordless_window_glass_vacuum_wiper": (
        "Máy Lau Kính Hút Nước Cầm Tay Không Vệt",
        ["#MayLauKinh", "#VeSinhNhaTam", "#MeoDonNha"],
    ),
    "cordless_turbo_jet_air_duster": (
        "Máy Thổi Bụi Phản Lực Cầm Tay Không Dây",
        ["#MayThoiBuiMini", "#VeSinhNhaCua", "#MeoDonNha"],
    ),
    # --- MINI_APPLIANCE (12 Categories) ---
    "handheld_foldable_garment_steamer": (
        "Bàn Ủi Hơi Nước Cầm Tay Gấp Gọn",
        ["#BanUiHoiNuoc", "#GiaDungMini", "#ChamSocQuanAo"],
    ),
    "mini_room_closet_dehumidifier": (
        "Máy Hút Ẩm Mini Phòng Ngủ Chống Nồm",
        ["#MayHutAmMini", "#ChongNomAm", "#GiaDungMini"],
    ),
    "hepa_desktop_bedroom_air_purifier": (
        "Máy Lọc Không Khí Mini Màng Lọc HEPA",
        ["#MayLocKhongKhi", "#LocBuiMin", "#GiaDungMini"],
    ),
    "electric_shoe_dryer_deodorizer": (
        "Máy Sấy Khô & Khử Mùi Giày Dép Mùa Mưa",
        ["#MaySayGiay", "#KhuMuiGiay", "#GiaDungMini"],
    ),
    "portable_mini_clothes_dryer": (
        "Máy Sấy Quần Áo Mini Gấp Gọn Khử Khuẩn",
        ["#MaySayQuanAoMini", "#ChongNomAm", "#GiaDungMini"],
    ),
    "quiet_air_circulator_desk_fan": (
        "Quạt Tuần Hoàn Không Khí & Tích Điện Mini",
        ["#QuatTuanHoan", "#QuatTichDien", "#GiaDungMini"],
    ),
    "ultrasonic_cool_mist_humidifier_diffuser": (
        "Máy Tạo Độ Ẩm Phun Sương Phòng Điều Hòa",
        ["#MayTaoAm", "#PhongNguTienNghi", "#GiaDungMini"],
    ),
    "uv_electric_mosquito_trap_swatter": (
        "Đèn Bắt Muỗi & Vợt Muỗi Điện Thông Minh",
        ["#DenBatMuoi", "#PhongChongMuoi", "#GiaDungMini"],
    ),
    "mini_portable_travel_electric_kettle": (
        "Bình Đun Nước Siêu Tốc Mini Giữ Nhiệt",
        ["#BinhDunNuocMini", "#GiaDungMini", "#DoDungTienIch"],
    ),
    "high_speed_ionic_compact_hair_dryer": (
        "Máy Sấy Tóc Ion Âm Tốc Độ Cao Nhỏ Gọn",
        ["#MaySayTocIon", "#ChamSocToc", "#GiaDungMini"],
    ),
    "motion_sensor_rechargeable_closet_light": (
        "Đèn LED Cảm Biến Chuyển Động Sạc Pin",
        ["#DenCamBien", "#NhaThongMinh", "#GiaDungMini"],
    ),
    "cordless_portable_oral_water_flosser": (
        "Máy Tăm Nước Cầm Tay Chống Nước IPX7",
        ["#MayTamNuoc", "#ChamSocRangMieng", "#GiaDungMini"],
    ),
}


def _format_mm_ss(seconds: float) -> str:
    sec_int = max(0, int(round(seconds)))
    mins = sec_int // 60
    secs = sec_int % 60
    return f"{mins}:{secs:02d}"


def _build_shopee_search_urls(brand: str, model_code: Optional[str], product_name_vi: str) -> tuple[str, str, str]:
    """Build clean search keyword + Shopee Mall & Shopee All search URLs for 1-click product lookup."""
    clean_brand = re.sub(r"\s+", " ", (brand or "").replace("_", " ")).strip()
    clean_model = re.sub(r"[_]+", " ", (model_code or ""))
    clean_model = re.sub(r"\b(?:series|general)\b", "", clean_model, flags=re.IGNORECASE)
    clean_model = re.sub(r"\s+", " ", clean_model).strip(" -")
    if clean_brand and clean_model and clean_model.lower().startswith(clean_brand.lower()):
        kw = clean_model
    elif clean_brand and clean_model:
        kw = f"{clean_brand} {clean_model}"
    elif clean_brand:
        short_func = " ".join((product_name_vi or "").split()[:5])
        kw = f"{clean_brand} {short_func}".strip()
    else:
        kw = (product_name_vi or "gia dụng chính hãng").strip()

    encoded = urllib.parse.quote_plus(kw)
    mall_url = f"https://shopee.vn/mall/search?keyword={encoded}"
    all_url = f"https://shopee.vn/search?keyword={encoded}"
    return kw, mall_url, all_url


class PublishPackager:
    """
    Step A + Semantic SEO Engine: Syncs Official Shopee Mall Affiliate links into rendered video
    packages and generates platform-specific SEO metadata for YouTube Shorts & Facebook Reels.
    """

    def __init__(
        self,
        manual_links_path: Path = DEFAULT_MANUAL_LINKS_PATH,
        manifest_path: Path = DEFAULT_PUBLISH_MANIFEST_PATH,
        markdown_path: Path = DEFAULT_PUBLISH_MARKDOWN_PATH,
        m4_output_path: Path = DEFAULT_M4_OUTPUT_PATH,
    ) -> None:
        self.manual_links_path = manual_links_path
        self.manifest_path = manifest_path
        self.markdown_path = markdown_path
        self.m4_output_path = m4_output_path
        self.video_registry = VideoRegistryManager()
        self.sku_registry = SKURegistryManager()
        self.csv_index = AffiliateCSVRepository()

    def _load_manual_links(self) -> Dict[str, Any]:
        default_obj = {
            "_instructions_vi": (
                "Điền link rút gọn Shopee Affiliate (VD: https://s.shopee.vn/...) vào trường 'short_link' "
                "của từng mã sản phẩm bên dưới, hoặc bỏ file CSV xuất từ Shopee Affiliate vào thư mục "
                "data/affiliate_csv/ rồi chạy: python3 -m pipeline.run_daily_pipeline --mode pack"
            ),
            "items": {},
        }
        data = safe_load_json(self.manual_links_path, lambda: default_obj)
        if isinstance(data, dict) and isinstance(data.get("items"), dict):
            return data
        return default_obj

    def _save_manual_links(self, data: Dict[str, Any]) -> None:
        atomic_write_json(self.manual_links_path, data)

    def _load_m4_rendered_map(self) -> Dict[str, RenderedVideoItem]:
        if not self.m4_output_path.exists():
            return {}
        try:
            m4_data = M4ProductionOutput.model_validate_json(
                self.m4_output_path.read_text(encoding="utf-8")
            )
            return {rv.script_id: rv for rv in m4_data.rendered_videos}
        except Exception:
            return {}

    def _match_from_csv_index(
        self,
        brand: str,
        model_code: Optional[str],
        product_name_vi: str,
    ) -> Optional[AffiliateListingMatch]:
        """Search all rows in `data/affiliate_csv/*.csv` for an exact or brand+category match."""
        if not self.csv_index.rows:
            return None

        brand_norm = _strip_accents(brand or "")
        brand_tokens = [t for t in re.split(r"\W+", brand_norm) if len(t) >= 2]
        model_norm = _strip_accents(model_code or "")
        model_alnum = re.sub(r"[^a-z0-9]", "", model_norm)

        name_tokens = [
            t
            for t in re.split(r"\W+", _strip_accents(product_name_vi or ""))
            if len(t) >= 3 and t not in {"chinh", "hang", "cao", "cap", "nhat", "ban", "series", "usb"}
        ]

        candidates: List[AffiliateListingMatch] = []
        for r in self.csv_index.rows:
            short_link = (r.get("short_link") or "").strip()
            if not short_link:
                continue
            title_norm = r["product_name_norm"]
            title_alnum = re.sub(r"[^a-z0-9]", "", title_norm)

            has_brand = bool(brand_tokens and all(bt in title_norm for bt in brand_tokens))
            has_model = bool(
                model_alnum
                and len(model_alnum) >= 3
                and (model_norm in title_norm or model_alnum in title_alnum)
            )

            if has_model and has_brand:
                confidence: Literal["EXACT_MODEL", "BRAND_AND_CATEGORY", "CATEGORY_FALLBACK"] = "EXACT_MODEL"
            elif has_brand:
                shared_tokens = sum(1 for nt in name_tokens if nt in title_norm)
                if shared_tokens < 2:
                    continue
                if model_alnum and len(model_alnum) >= 4:
                    other_models = re.findall(r"\b[a-z]{2,4}\d{2,4}[a-z0-9]*\b", title_norm)
                    if other_models and all(model_alnum not in re.sub(r"[^a-z0-9]", "", om) for om in other_models):
                        continue
                confidence = "BRAND_AND_CATEGORY"
            else:
                continue

            candidates.append(
                AffiliateListingMatch(
                    item_id=r["item_id"],
                    product_name=r["product_name"],
                    shop_name=r["shop_name"],
                    price_vnd=r["price_vnd"],
                    sales_count=r["sales_count"],
                    commission_rate_pct=r["commission_rate_pct"],
                    commission_amount_vnd=r["commission_amount_vnd"],
                    product_link=r["product_link"],
                    short_link=short_link,
                    is_ambiguous_listing=r["is_ambiguous_listing"],
                    match_confidence=confidence,
                )
            )

        if not candidates:
            return None

        candidates.sort(
            key=lambda m: (
                0 if m.match_confidence == "EXACT_MODEL" else 1,
                0 if not m.is_ambiguous_listing else 1,
                -m.commission_amount_vnd,
                -m.sales_count,
            )
        )
        return candidates[0]

    def _resolve_sku_affiliate_item(
        self,
        candidate_id: str,
        brand: str,
        model_code: str,
        product_name_vi: str,
        estimated_price_vnd: int,
        honest_caveat_vi: str,
        m3_offer: Optional[AffiliateListingMatch],
        manual_items: Dict[str, Any],
        reg_sku: Optional[SKURegistryEntry] = None,
    ) -> PublishSKUAffiliateItem:
        sku_key = (
            self.sku_registry.resolve_sku_key_by_candidate_id(candidate_id)
            or normalize_sku_key(brand, model_code, candidate_id)
        )
        kw, mall_url, all_url = _build_shopee_search_urls(brand, model_code, product_name_vi)

        manual_entry = manual_items.get(sku_key) or manual_items.get(candidate_id) or {}
        manual_short_link = str(manual_entry.get("short_link") or "").strip()
        manual_product_link = str(manual_entry.get("product_link") or "").strip()
        manual_shop_name = str(manual_entry.get("shop_name") or "").strip()
        manual_comm_pct = float(manual_entry.get("commission_rate_pct") or 0.0)

        short_link = ""
        mall_search_aff_link = ""
        product_link = ""
        shop_name = ""
        verification_tier = "PENDING"
        comm_pct = 0.0
        comm_vnd = 0
        link_source: Literal["BROWSER_SESSION", "MANUAL_JSON", "SHOPEE_CSV", "M3_CONTRACT", "PENDING"] = "PENDING"

        # Priority 1: Explicit user-pasted link in `data/manual_affiliate_links.json` (`link_source == "MANUAL_JSON"`)
        existing_src = str(manual_entry.get("link_source") or "")
        if (
            manual_short_link
            and ("shopee.vn" in manual_short_link or "shope.ee" in manual_short_link)
            and existing_src == "MANUAL_JSON"
        ):
            short_link = manual_short_link
            mall_search_aff_link = str(manual_entry.get("mall_search_affiliate_link") or "")
            product_link = manual_product_link
            shop_name = manual_shop_name or "Gian hàng Shopee chính hãng"
            verification_tier = "MANUAL_JSON"
            comm_pct = manual_comm_pct
            link_source = "MANUAL_JSON"

        # Priority 2: Automatic match from `data/affiliate_csv/*.csv`
        if not short_link:
            csv_match = self._match_from_csv_index(brand, model_code, product_name_vi)
            if csv_match and csv_match.short_link:
                short_link = csv_match.short_link
                product_link = csv_match.product_link
                shop_name = csv_match.shop_name
                verification_tier = "SHOPEE_CSV"
                comm_pct = csv_match.commission_rate_pct
                comm_vnd = csv_match.commission_amount_vnd
                link_source = "SHOPEE_CSV"

        # Priority 3: Existing offer attached during M0/M3
        if not short_link and m3_offer and m3_offer.short_link:
            short_link = m3_offer.short_link
            product_link = m3_offer.product_link
            shop_name = m3_offer.shop_name
            verification_tier = "SHOPEE_CSV"
            comm_pct = m3_offer.commission_rate_pct
            comm_vnd = m3_offer.commission_amount_vnd
            link_source = "M3_CONTRACT"

        # Priority 4: 3-Tier Official Shopee Mall Whitelist + Mall Search Affiliate Resolver (an_17337280644)
        if not short_link:
            try:
                from pipeline.shopee_browser_link_resolver import ShopeeAutoAffiliateResolver

                auto_res = ShopeeAutoAffiliateResolver(manual_links_path=self.manual_links_path).discover_shopee_offer_for_sku(
                    sku_key=sku_key,
                    brand=brand,
                    model_code=model_code,
                    product_name_vi=product_name_vi,
                    estimated_price_vnd=estimated_price_vnd,
                )
                if auto_res and auto_res.get("short_link"):
                    short_link = str(auto_res["short_link"])
                    mall_search_aff_link = str(auto_res.get("mall_search_affiliate_link") or "")
                    product_link = str(auto_res.get("product_link") or "")
                    shop_name = str(auto_res.get("shop_name") or "Gian hàng Shopee Mall")
                    verification_tier = str(auto_res.get("verification_tier") or "TIER_1_OFFICIAL_MALL_ITEM")
                    link_source = "BROWSER_SESSION"
            except Exception:
                pass

        if not mall_search_aff_link:
            try:
                from pipeline.shopee_browser_link_resolver import (
                    build_shopee_mall_search_an_redir_link,
                    get_brand_whitelist_entry,
                )

                b_wl = get_brand_whitelist_entry(brand)
                use_mall = bool(b_wl.get("prefer_mall_search", False)) if b_wl else False
                _, mall_search_aff_link = build_shopee_mall_search_an_redir_link(
                    brand=brand,
                    model_code=model_code,
                    product_name_vi=product_name_vi,
                    sub_id=f"{sku_key}_mall",
                    prefer_mall=use_mall,
                )
            except Exception:
                pass

        # Register clean slugs in data/short_links_registry.json for Cloudflare Edge Shortener
        cf_mgr = CloudflareShortLinkManager()
        p_slug, m_slug = "", ""
        if short_link.startswith("https://s.shopee.vn/an_redir"):
            p_slug, m_slug = cf_mgr.register_sku_routes(
                sku_key=sku_key,
                brand=brand,
                model_code=model_code,
                product_name_vi=product_name_vi,
                shop_name=shop_name,
                primary_shopee_affiliate_url=short_link,
                backup_mall_affiliate_url=mall_search_aff_link,
            )

        manual_items[sku_key] = {
            "sku_key": sku_key,
            "candidate_id": candidate_id,
            "brand": brand,
            "model_code": model_code,
            "product_name_vi": product_name_vi,
            "estimated_price_vnd": estimated_price_vnd,
            "short_link": short_link,
            "mall_search_affiliate_link": mall_search_aff_link,
            "cf_primary_slug": p_slug,
            "cf_mall_slug": m_slug,
            "product_link": product_link,
            "shop_name": shop_name,
            "verification_tier": verification_tier,
            "commission_rate_pct": comm_pct,
            "link_source": link_source,
            "shopee_mall_search_url": mall_url,
            "shopee_all_search_url": all_url,
        }

        display_short_link = cf_mgr.get_short_url_if_synced(p_slug) if p_slug else ""
        display_mall_link = cf_mgr.get_short_url_if_synced(m_slug) if m_slug else ""
        final_short = display_short_link or short_link
        final_mall = display_mall_link or mall_search_aff_link

        status: Literal["READY_SHORT_LINK", "NEEDS_SHOPEE_LINK"] = (
            "READY_SHORT_LINK" if bool(final_short) else "NEEDS_SHOPEE_LINK"
        )
        return PublishSKUAffiliateItem(
            sku_key=sku_key,
            candidate_id=candidate_id,
            brand=brand,
            model_code=model_code,
            product_name_vi=product_name_vi,
            estimated_price_vnd=estimated_price_vnd,
            key_specs=dict(reg_sku.key_specs) if reg_sku and reg_sku.key_specs else {},
            highlight_pros_vi=list(reg_sku.highlight_pros_vi) if reg_sku and reg_sku.highlight_pros_vi else [],
            honest_caveat_vi=honest_caveat_vi,
            affiliate_status=status,
            short_link=final_short,
            mall_search_affiliate_link=final_mall,
            product_link=product_link,
            shop_name=shop_name,
            verification_tier=verification_tier,
            commission_rate_pct=comm_pct,
            commission_amount_vnd=comm_vnd,
            link_source=link_source,
            shopee_search_keyword=kw,
            shopee_mall_search_url=mall_url,
            shopee_all_search_url=all_url,
        )

    def _build_dual_head_seo_titles(
        self,
        entry: VideoRegistryEntry,
        resolved_skus: List[PublishSKUAffiliateItem],
    ) -> Tuple[str, str]:
        """
        Generate Dual-Head Buyer-Intent SEO Titles:
          `[Từ khóa công năng người mua hay gõ] + [Mốc giá / Câu hỏi sát sườn] + [Brand + Model]`
        Returns `(youtube_shorts_title_vi, facebook_reels_title_vi)`.
        """
        cat_info = CATEGORY_SHORT_KEYWORDS_VI.get(entry.category_id)
        cat_short_vi = cat_info[0] if cat_info else ""

        if entry.script_format == "COMBO_WORKFLOW_SYNERGY" and len(resolved_skus) >= 2:
            s1, s2 = resolved_skus[0], resolved_skus[1]
            b1 = f"{s1.brand} {s1.model_code.replace('_', ' ')}".replace("-Series", "").strip()
            b2 = f"{s2.brand} {s2.model_code.replace('_', ' ')}".replace("-Series", "").strip()
            if entry.domain == "CLEANING_CARE":
                base_title = f"Combo Dọn Nhà Sạch Nhanh Đỡ Mỏi Lưng: {b1} + {b2}"
            else:
                base_title = f"Combo Gia Dụng Mini Gọn Nhẹ Cho Phòng Ở: {b1} + {b2}"
        elif resolved_skus:
            s1 = resolved_skus[0]
            price_str = format_vnd_short(s1.estimated_price_vnd)
            clean_mc = s1.model_code.replace("_", " ").strip()
            bm = f"{s1.brand} {clean_mc}".strip()
            if entry.category_id == "uv_mattress_dust_mite_vacuum":
                base_title = f"{bm} (~{price_str}): Máy Hút Bụi Giường Nệm UV Có Hiệu Quả?"
            elif entry.category_id == "handheld_foldable_garment_steamer":
                base_title = f"{bm} (~{price_str}): Bàn Ủi Hơi Nước Cầm Tay Có Đáng Mua?"
            elif entry.category_id == "cordless_electric_spin_scrubber":
                base_title = f"{bm} (~{price_str}): Chổi Cọ Nhà Tắm Điện Có Khỏe Không?"
            elif cat_short_vi:
                base_title = f"{bm} (~{price_str}): {cat_short_vi} Có Đáng Mua?"
            else:
                # Dynamic category fallback: extract short product type from product_name_vi without repeating "Đánh Giá"
                prod_head = s1.product_name_vi
                for token_rm in (s1.brand, clean_mc, s1.model_code):
                    if token_rm:
                        prod_head = re.sub(re.escape(token_rm), "", prod_head, flags=re.IGNORECASE)
                prod_head = " ".join(prod_head.split()[:5]).strip(" -–,:") or "Gia Dụng Chính Hãng"
                base_title = f"{bm} (~{price_str}): {prod_head} Có Đáng Mua?"
        else:
            base_title = entry.title_vi.replace("_", " ").strip()

        clean_base_for_yt = base_title.strip()
        if len(clean_base_for_yt) > 78:
            # Prefer trimming cleanly on a word boundary and removing dangling Vietnamese connectors ('Có', 'Có Đáng', 'Cho', 'Và')
            trimmed = truncate_words_clean(clean_base_for_yt, max_chars=78)
            trimmed = re.sub(
                r"\s+(?:có\s+đáng|có\s+nên|có|đáng|không|cho|với|của|và|tại|nhưng|mà)\s*$",
                "",
                trimmed,
                flags=re.IGNORECASE,
            ).strip(" :-–,")
            clean_base_for_yt = trimmed or truncate_words_clean(clean_base_for_yt, max_chars=78)

        yt_title = sanitize_youtube_metadata_text(f"{clean_base_for_yt} #Shorts", max_chars=95)
        fb_title = base_title.replace("->", "→")
        return yt_title, fb_title

    def _build_focused_hashtags(
        self,
        entry: VideoRegistryEntry,
        resolved_skus: List[PublishSKUAffiliateItem],
    ) -> List[str]:
        """
        Enforce the `3 + 2 + 1 = 6` Focused Hashtag Formula:
          - 3 Category/Niche search hashtags
          - 1-2 Brand/Model hashtags
          - Channel Authority + AI Review hashtags (`#ReviewGiaDungMini`, `#AIReview`)
        """
        cat_info = CATEGORY_SHORT_KEYWORDS_VI.get(entry.category_id)
        if cat_info:
            niche_tags = list(cat_info[1])
        elif entry.domain == "CLEANING_CARE":
            niche_tags = ["#VeSinhNhaCua", "#MeoDonNha", "#NhaSachTienNghi"]
        else:
            niche_tags = ["#GiaDungMini", "#DoGiaDungThongMinh", "#PhongNguTienNghi"]

        brand_model_tags: List[str] = []
        for s_item in resolved_skus:
            b_clean = re.sub(r"[^a-zA-Z0-9]", "", s_item.brand)
            m_clean = re.sub(r"[^a-zA-Z0-9]", "", s_item.model_code.replace("Series", ""))
            if b_clean:
                brand_model_tags.append(f"#{b_clean}")
            if b_clean and m_clean and m_clean.lower() != "general":
                brand_model_tags.append(f"#{b_clean}{m_clean}")
        brand_model_tags = list(dict.fromkeys(brand_model_tags))[:2]

        combined = list(dict.fromkeys(niche_tags[:3] + brand_model_tags[:1] + ["#ReviewGiaDungMini", "#AIReview"]))
        return combined[:6]

    def _build_chapter_timestamps(
        self,
        entry: VideoRegistryEntry,
        script: Optional[AuditedScriptItem],
        m4_item: Optional[RenderedVideoItem],
        resolved_skus: List[PublishSKUAffiliateItem],
    ) -> List[str]:
        """Compute exact `0:00` chapter timestamps from M4's rendered `sub_shots` (or M3 beats)."""
        primary_label = (
            " + ".join(f"{s.brand} {s.model_code}".strip() for s in resolved_skus[:2])
            if resolved_skus
            else entry.title_vi[:28]
        )
        primary_price = (
            f"~{format_vnd_short(sum(s.estimated_price_vnd for s in resolved_skus))}"
            if resolved_skus
            else "chính hãng"
        )
        beat_titles = {
            1: "Nỗi đau thực tế & Sai lầm khi mua hàng OEM giá rẻ",
            2: f"Thiết kế & Thông số chuẩn hãng {primary_label}",
            3: "Trải nghiệm thực tế & Hiệu năng sử dụng hàng ngày",
            4: "Nhược điểm thực tế cần biết trước khi xuống tiền",
            5: f"Tổng kết mức giá {primary_price}: Ai nên mua?",
        }

        beat_start_sec: Dict[int, float] = {1: 0.0}
        if m4_item and m4_item.sub_shots:
            cursor = 0.0
            for ss in m4_item.sub_shots:
                if ss.beat_index not in beat_start_sec:
                    beat_start_sec[ss.beat_index] = cursor
                cursor += ss.duration_sec
        elif script and script.beats:
            for b in script.beats:
                if b.beat_index not in beat_start_sec:
                    beat_start_sec[b.beat_index] = b.start_sec
        else:
            est_dur = max(32.0, float(entry.render_duration_sec or 45.0))
            beat_start_sec = {
                1: 0.0,
                2: round(est_dur * 0.20, 1),
                3: round(est_dur * 0.42, 1),
                4: round(est_dur * 0.65, 1),
                5: round(est_dur * 0.83, 1),
            }

        lines: List[str] = []
        for b_idx in sorted(beat_titles.keys()):
            if b_idx in beat_start_sec:
                ts_str = _format_mm_ss(beat_start_sec[b_idx])
                lines.append(f"{ts_str} {beat_titles[b_idx]}")
        return lines

    def _build_youtube_backend_tags(
        self,
        entry: VideoRegistryEntry,
        resolved_skus: List[PublishSKUAffiliateItem],
    ) -> List[str]:
        """Generate 12-15 high-intent accented + unaccented backend tags (`<= 450` chars total)."""
        raw_tags: List[str] = []
        cat_info = CATEGORY_SHORT_KEYWORDS_VI.get(entry.category_id)
        if cat_info:
            cat_phrase = cat_info[0].lower()
            raw_tags.append(cat_phrase)
            raw_tags.append(_strip_accents(cat_phrase))
            raw_tags.append(f"review {cat_phrase}")
            raw_tags.append(_strip_accents(f"review {cat_phrase}"))
            raw_tags.append(f"{cat_phrase} loại nào tốt")
            raw_tags.append(_strip_accents(f"{cat_phrase} loại nào tốt"))

        for s in resolved_skus:
            bm = f"{s.brand} {s.model_code}".replace("-Series", "").strip().lower()
            if bm:
                raw_tags.extend(
                    [
                        bm,
                        f"review {bm}",
                        f"đánh giá {bm}",
                        f"danh gia {bm}",
                        f"có nên mua {bm}",
                    ]
                )
            prod_short = " ".join(s.product_name_vi.lower().split()[:5])
            if prod_short:
                raw_tags.append(prod_short)
                raw_tags.append(_strip_accents(prod_short))

        if entry.domain == "CLEANING_CARE":
            raw_tags.extend(["đồ vệ sinh nhà cửa", "ve sinh nha cua", "mẹo dọn nhà", "ai review"])
        else:
            raw_tags.extend(["gia dụng mini", "gia dung thong minh", "review đồ gia dụng", "ai review"])

        return sanitize_youtube_tags(raw_tags, max_weighted_bytes=350, max_count=15)

    def _build_video_package(
        self,
        entry: VideoRegistryEntry,
        manual_items: Dict[str, Any],
        m4_map: Dict[str, RenderedVideoItem],
    ) -> PublishVideoPackageItem:
        script: Optional[AuditedScriptItem] = None
        if entry.script_payload:
            try:
                script = AuditedScriptItem.model_validate(entry.script_payload)
            except Exception:
                script = None

        m4_item = m4_map.get(entry.script_id)
        sku_entries = self.sku_registry.get_all_entries()
        resolved_skus: List[PublishSKUAffiliateItem] = []

        if script and script.verified_factual_claims:
            for claim in script.verified_factual_claims:
                sku_k = (
                    self.sku_registry.resolve_sku_key_by_candidate_id(claim.candidate_id)
                    or normalize_sku_key(claim.brand, claim.model_code, claim.candidate_id)
                )
                reg_sku = sku_entries.get(sku_k)
                prod_name = (
                    reg_sku.product_name_vi
                    if reg_sku
                    else f"{claim.brand} {claim.model_code or ''}".strip()
                )
                m3_offer = script.affiliate_offers_ready.get(claim.candidate_id) or (
                    reg_sku.best_affiliate_offer if reg_sku else None
                )
                resolved_skus.append(
                    self._resolve_sku_affiliate_item(
                        candidate_id=claim.candidate_id,
                        brand=claim.brand,
                        model_code=claim.model_code or "",
                        product_name_vi=prod_name,
                        estimated_price_vnd=claim.claimed_price_vnd,
                        honest_caveat_vi=claim.mandatory_caveat_mentioned_vi,
                        m3_offer=m3_offer,
                        manual_items=manual_items,
                        reg_sku=reg_sku,
                    )
                )
        else:
            for cid in entry.primary_sku_ids:
                sku_k = self.sku_registry.resolve_sku_key_by_candidate_id(cid) or cid
                reg_sku = sku_entries.get(sku_k)
                if reg_sku:
                    resolved_skus.append(
                        self._resolve_sku_affiliate_item(
                            candidate_id=reg_sku.candidate_id,
                            brand=reg_sku.brand,
                            model_code=reg_sku.model_code or "",
                            product_name_vi=reg_sku.product_name_vi,
                            estimated_price_vnd=reg_sku.estimated_market_price_vnd,
                            honest_caveat_vi=reg_sku.honest_caveat_vi,
                            m3_offer=reg_sku.best_affiliate_offer,
                            manual_items=manual_items,
                            reg_sku=reg_sku,
                        )
                    )

        yt_title, fb_title = self._build_dual_head_seo_titles(entry, resolved_skus)
        hashtags = self._build_focused_hashtags(entry, resolved_skus)
        chapters = self._build_chapter_timestamps(entry, script, m4_item, resolved_skus)
        yt_tags = self._build_youtube_backend_tags(entry, resolved_skus)

        # Upgrade 2 (Section 8.9 Dynamic Trend SEO): Apply disciplined FAST_TRACK vs NORMAL SEO enrichment
        trend_lead_sentence = ""
        active_trends_map = {t.product_concept_id: t for t in load_active_trend_opportunities()}
        trend_opp = active_trends_map.get(entry.category_id)
        if trend_opp:
            if trend_opp.priority_tier == "FAST_TRACK_TREND":
                if trend_opp.seo_context_prefix_vi:
                    prefix = trend_opp.seo_context_prefix_vi.strip().rstrip(":-")
                    base_no_shorts = yt_title.replace("#Shorts", "").strip()
                    cand_raw = sanitize_youtube_metadata_text(f"{prefix}: {base_no_shorts}", max_chars=200)
                    candidate_title = truncate_words_clean(cand_raw, max_chars=84)
                    candidate_title = re.sub(
                        r"\s+(?:có\s+đáng|có\s+nên|có|đáng|không|cho|với|của|và)\s*$",
                        "",
                        candidate_title,
                        flags=re.IGNORECASE,
                    ).strip(" :-–,")
                    yt_title = f"{candidate_title} #Shorts"
                if trend_opp.need_pain_point_vi:
                    trend_lead_sentence = f"🔥 {trend_opp.need_pain_point_vi.strip()}\n"
                # Swap 1 of 3 category hashtags with 1 contextual hashtag (keep strict 6-hashtag cap!)
                if trend_opp.seo_context_hashtag and trend_opp.seo_context_hashtag.startswith("#") and len(hashtags) >= 3:
                    if trend_opp.seo_context_hashtag not in hashtags:
                        hashtags[2] = trend_opp.seo_context_hashtag
                extra_kws: List[str] = []
                for kw in trend_opp.seo_trend_keywords_vi[:3]:
                    extra_kws.append(kw)
                    no_acc = _strip_accents(kw)
                    if no_acc and no_acc != kw.lower():
                        extra_kws.append(no_acc)
                yt_tags = sanitize_youtube_tags(yt_tags + extra_kws, max_weighted_bytes=350, max_count=15)
            else:
                # NORMAL_TREND: keep Evergreen Buyer-Intent title; add light description context + 1-2 need tags
                if trend_opp.need_pain_point_vi:
                    trend_lead_sentence = f"💡 {trend_opp.need_pain_point_vi.strip()}\n"
                extra_kws = []
                for kw in trend_opp.seo_trend_keywords_vi[:2]:
                    extra_kws.append(kw)
                    no_acc = _strip_accents(kw)
                    if no_acc and no_acc != kw.lower():
                        extra_kws.append(no_acc)
                yt_tags = sanitize_youtube_tags(yt_tags + extra_kws, max_weighted_bytes=350, max_count=15)

        hashtag_line = " ".join(hashtags)

        # Build Pros, Specs, Caveats & Pinned Affiliate Lines
        pros_bullets: List[str] = []
        spec_bullets: List[str] = []
        caveat_bullets: List[str] = []
        pinned_lines: List[str] = []

        for s_item in resolved_skus:
            price_str = format_vnd_short(s_item.estimated_price_vnd)
            label = f"{s_item.brand} {s_item.model_code}".strip()
            for p_line in s_item.highlight_pros_vi[:2]:
                pros_bullets.append(f"• {label}: {p_line}")
            if s_item.key_specs:
                kv_str = ", ".join(f"{k}: {v}" for k, v in list(s_item.key_specs.items())[:3])
                spec_bullets.append(f"• Thông số {label} (~{price_str}): {kv_str}")
            if s_item.honest_caveat_vi:
                caveat_bullets.append(f"⚠️ Lưu ý {label}: {s_item.honest_caveat_vi}")

            link_display = (
                s_item.short_link
                if s_item.short_link
                else f"[DÁN_LINK_S.SHOPEE.VN_CHO_{s_item.sku_key.upper()}]"
            )
            shop_badge = f" [{s_item.shop_name}]" if s_item.shop_name else ""
            pinned_lines.append(
                f"👉 {s_item.product_name_vi}{shop_badge} (~{price_str}): {link_display}"
            )
            if (
                s_item.mall_search_affiliate_link
                and s_item.mall_search_affiliate_link != s_item.short_link
            ):
                pinned_lines.append(
                    f"   • Link dự phòng toàn bộ gian hàng Shopee Mall ({s_item.brand}): {s_item.mall_search_affiliate_link}"
                )

        # 1. Pinned Comment (Facebook Reels keeps exact status quo; YouTube Shorts adds dynamic Bio Hub click guide WITHOUT '<' or '>')
        varied_intro = HumanBehaviorGuard.pick_varied_pinned_comment_intro(entry.script_id)
        pinned_comment = f"{varied_intro}\n" + "\n".join(pinned_lines)
        cf_bio_mgr = CloudflareShortLinkManager()
        cf_reg_info = cf_bio_mgr._load_registry()
        bio_base = (
            cf_bio_mgr.custom_base_url
            or str(cf_reg_info.get("base_url") or "").strip().rstrip("/")
            or f"{cf_bio_mgr.worker_name}.{cf_bio_mgr.desired_subdomain}.workers.dev"
        )
        bio_host_display = re.sub(r"^https?://", "", bio_base).strip("/")
        creds_raw = safe_load_json(PROJECT_ROOT / "config" / "platform_credentials.json", lambda: {})
        yt_cfg = creds_raw.get("youtube") if isinstance(creds_raw, dict) else {}
        yt_handle = str((yt_cfg or {}).get("channel_handle") or "").strip()
        if yt_handle and not yt_handle.startswith("@"):
            yt_handle = f"@{yt_handle}"
        channel_ref_vi = f"tên kênh {yt_handle}" if yt_handle else "ảnh đại diện kênh"
        yt_bio_guide = (
            f"💡 Do YouTube Shorts khóa bấm link trực tiếp ở bình luận, bạn bấm vào {channel_ref_vi} "
            f"→ ấn link Bio đầu trang Kênh {bio_host_display} (hoặc copy nhanh link bên dưới) để mở thẳng Shopee Mall nhé:"
        )
        yt_pinned_comment = sanitize_youtube_metadata_text(
            f"{varied_intro}\n{yt_bio_guide}\n" + "\n".join(pinned_lines),
            max_chars=2000,
        )

        ai_disclosure_line = (
            "🤖 Minh bạch nội dung: Video có sử dụng giọng đọc và biên tập hỗ trợ bởi AI "
            "(thông số đối chiếu trực tiếp từ nguồn chính hãng)."
        )

        # 2. Facebook Reels Checklist Caption (High-retention feed format + optional Trend lead context + AI disclosure)
        reels_caption = (
            f"{fb_title}\n"
            + (f"{trend_lead_sentence}\n" if trend_lead_sentence else "\n")
            + ("✅ Điểm đáng tiền qua kiểm chứng:\n" + "\n".join(pros_bullets[:3]) + "\n\n" if pros_bullets else "")
            + ("🔧 Thông số cốt lõi:\n" + "\n".join(spec_bullets) + "\n\n" if spec_bullets else "")
            + ("\n".join(caveat_bullets) + "\n\n" if caveat_bullets else "")
            + "🛒 Link gian hàng Shopee Mall Chính Hãng mình để ngay ở Bình luận đầu tiên bên dưới (hoặc bấm trực tiếp tại đây):\n"
            + "\n".join(pinned_lines)
            + f"\n\n{ai_disclosure_line}\n\n"
            + hashtag_line
        )

        # 3. YouTube Shorts 4-Block Semantic SEO Description (strictly sanitized against '<' / '>' which cause HTTP 400!)
        search_keywords_footer = ", ".join(yt_tags[:10])
        raw_yt_description = (
            f"{trend_lead_sentence}{fb_title} — Đánh giá chi tiết thông số kỹ thuật, ưu điểm và nhược điểm thực tế trước khi mua.\n\n"
            f"{yt_pinned_comment}\n\n"
            + ("🔧 THÔNG SỐ & ĐIỂM NỔI BẬT:\n" + "\n".join(spec_bullets + pros_bullets[:2]) + "\n\n" if (spec_bullets or pros_bullets) else "")
            + ("⚠️ NHƯỢC ĐIỂM CẦN LƯU Ý TRƯỚC KHI MUA:\n" + "\n".join(caveat_bullets) + "\n\n" if caveat_bullets else "")
            + ("⏱️ MỐC THỜI GIAN TRONG VIDEO (TIMESTAMPS):\n" + "\n".join(chapters) + "\n\n" if chapters else "")
            + f"{ai_disclosure_line}\n\n"
            + f"🔍 Từ khóa tìm kiếm liên quan: {search_keywords_footer}\n\n"
            + f"{hashtag_line} #Shorts"
        )
        yt_description = sanitize_youtube_metadata_text(raw_yt_description, max_chars=4800)

        # 4. TikTok High-SEO Caption (STRICTLY 0% Shopee links, 0% external URLs, 0% 'Shopee' keyword!)
        tk_pros_clean = [
            re.sub(r"\bshopee(?:\s+mall)?\b", "chính hãng", p, flags=re.IGNORECASE)
            for p in pros_bullets[:2]
        ]
        tk_caveat_clean = [
            re.sub(r"\bshopee(?:\s+mall)?\b", "chính hãng", c, flags=re.IGNORECASE)
            for c in caveat_bullets[:1]
        ]
        tk_hashtags = list(dict.fromkeys(hashtags + ["#MeoDonNha" if entry.domain == "CLEANING_CARE" else "#GiaDungMini"]))[:6]
        tk_hashtag_line = " ".join(tk_hashtags)
        tiktok_caption_seo = (
            f"{fb_title}\n\n"
            + ("✅ Điểm nổi bật thực tế:\n" + "\n".join(tk_pros_clean) + "\n\n" if tk_pros_clean else "")
            + ("\n".join(tk_caveat_clean) + "\n\n" if tk_caveat_clean else "")
            + "💬 Bạn thấy mẫu này có hợp nhu cầu dùng hằng ngày không? Comment bên dưới nhé!\n"
            + "🤖 Video có sử dụng giọng đọc và biên tập hỗ trợ bởi AI.\n\n"
            + tk_hashtag_line
        )
        # Double-check zero URLs or 'shopee' leaks in TikTok caption
        tiktok_caption_seo = re.sub(r"https?://\S+", "", tiktok_caption_seo)
        tiktok_caption_seo = re.sub(r"\bshopee\b", "chính hãng", tiktok_caption_seo, flags=re.IGNORECASE).strip()

        hook_short = (entry.hook_title_short_vi or entry.title_vi[:28]).strip()
        vid_path = MacOSLegacyRuntime.resolve_portable_video_path(entry.video_path, entry.script_id)
        vid_path_str = str(vid_path)
        exists_on_disk = vid_path.exists()

        dur_sec = 0.0
        if m4_item and m4_item.duration_sec > 0:
            dur_sec = round(float(m4_item.duration_sec), 2)
        elif script:
            dur_sec = round(float(script.total_duration_sec), 2)

        all_ready = bool(resolved_skus) and all(
            s.affiliate_status == "READY_SHORT_LINK" for s in resolved_skus
        )

        return PublishVideoPackageItem(
            script_id=entry.script_id,
            script_format=entry.script_format,
            domain=entry.domain,
            category_id=entry.category_id,
            video_status=entry.status,
            video_path=vid_path_str,
            video_exists_on_disk=exists_on_disk,
            duration_sec=dur_sec,
            voice_name=entry.voice_name or "",
            voice_gender=entry.voice_gender or "",
            bgm_track=entry.bgm_track or "",
            rendered_at=entry.rendered_at or "",
            hook_title_on_screen_vi=hook_short,
            title_vi=entry.title_vi,
            youtube_shorts_title_vi=yt_title,
            facebook_reels_title_vi=fb_title,
            youtube_shorts_description_vi=yt_description,
            youtube_video_tags=yt_tags,
            chapter_timestamps_vi=chapters,
            tiktok_caption_full_vi=tiktok_caption_seo,
            facebook_reels_caption_full_vi=reels_caption,
            pinned_comment_vi=pinned_comment,
            youtube_pinned_comment_vi=yt_pinned_comment,
            hashtags=hashtags,
            all_links_ready=all_ready,
            skus=resolved_skus,
        )

    def _sync_back_to_m4_output(self, packages_by_id: Dict[str, PublishVideoPackageItem]) -> None:
        """Update `data/m4_production_output.json` social_post_pack with newly resolved Affiliate links & SEO."""
        if not self.m4_output_path.exists():
            return
        try:
            m4_data = M4ProductionOutput.model_validate_json(
                self.m4_output_path.read_text(encoding="utf-8")
            )
            changed = False
            for rv in m4_data.rendered_videos:
                pkg = packages_by_id.get(rv.script_id)
                if not pkg:
                    continue
                links_dict = {
                    s.candidate_id: (
                        s.short_link
                        if s.short_link
                        else f"(Chưa gắn link — tìm Shopee: {s.shopee_search_keyword})"
                    )
                    for s in pkg.skus
                }
                rv.social_post_pack = SocialPostPack(
                    title_vi=pkg.youtube_shorts_title_vi,
                    caption_vi=pkg.facebook_reels_caption_full_vi,
                    hashtags=pkg.hashtags,
                    pinned_comment_affiliate_vi=pkg.pinned_comment_vi,
                    affiliate_short_links=links_dict,
                )
                if rv.duration_sec > 0:
                    pkg.duration_sec = rv.duration_sec
                changed = True
            if changed:
                self.m4_output_path.write_text(
                    m4_data.model_dump_json(indent=2),
                    encoding="utf-8",
                )
        except Exception:
            pass

    def _render_markdown_sheet(self, manifest: PublishManifestOutput) -> str:
        lines: List[str] = [
            "# 📦 BẢNG ĐÓNG GÓI XUẤT BẢN VIDEO, SEO ĐA NỀN TẢNG & LINK AFFILIATE SHOPEE (BƯỚC A + M5 SEO)",
            "",
            f"- **Cập nhật lúc:** `{manifest.generated_at}`",
            f"- **Tổng số video đã xuất (`RENDERED`, chờ đăng):** `{manifest.total_rendered_unposted_videos}` video",
            f"- **Đã xác thực đủ link Shopee Mall Chính Hãng:** `{manifest.videos_with_all_affiliate_links_ready}/{manifest.total_rendered_unposted_videos}` video",
            f"- **Sổ điền link nhanh:** `{manifest.manual_links_json_path}`",
            f"- **Thư mục tự nhận CSV Shopee Affiliate:** `{manifest.affiliate_csv_dir}`",
            "",
            "---",
            "",
            "## 🛡️ Cơ chế Bảo Chứng Link Affiliate 3 Lớp (`affiliate_id = 17337280644`) & Chuẩn SEO M5",
            "1. **Lớp 1 (`SHOPEE_CSV` / `MANUAL_JSON`) & Lớp 2 (`TIER_1_OFFICIAL_MALL_ITEM`):** Chỉ gắn link sản phẩm lẻ khi thuộc **Sổ Trắng Gian Hàng Chính Hãng (`data/shopee_mall_shop_whitelist.json`)** (Deerma, Xiaomi, Philips, Dreame, Lock&Lock, Inochi, Levoit, Bear...).",
            "2. **Lớp 3 (`TIER_2_SHOPEE_MALL_SEARCH`):** Luôn đính kèm **Link Affiliate Dự phòng dẫn thẳng vào trang Shopee Mall của đúng mã `Brand + Model`** (không bao giờ hết hàng).",
            "3. **Chuẩn SEO 2 Nền Tảng (`M5`):**",
            "   - **YouTube Shorts:** Tiêu đề 2 đầu từ khóa (`Công năng + Giá/Câu hỏi + Brand Model #Shorts`) + Mô tả 4 khối có `Timestamps 0:00` thực tế + `12-15` thẻ Tags ẩn (có dấu & không dấu).",
            "   - **Facebook Reels:** Caption dạng Checklist `✅ Điểm đáng tiền / ⚠️ Nhược điểm nói thật` + Tự động gắn link Shopee Mall ở cả Caption và Bình luận đầu tiên (`Pinned Comment`).",
            "",
            "---",
            "",
        ]

        for idx, pkg in enumerate(manifest.packages, start=1):
            status_badge = (
                "✅ ĐÃ XÁC THỰC CHÍNH HÃNG & CHUẨN SEO"
                if pkg.all_links_ready
                else "⏳ CHỜ XÁC THỰC LINK"
            )
            lines.extend(
                [
                    f"## 🎬 Video #{idx}: `{pkg.script_id}` ({status_badge})",
                    "",
                    f"- **File MP4:** `{pkg.video_path}`",
                    f"- **Định dạng:** `{pkg.script_format}` | **Chủ đề:** `{pkg.domain}` (`{pkg.category_id}`)",
                    f"- **Thời lượng:** `{pkg.duration_sec}s` | **Giọng đọc:** `{pkg.voice_name}` (`{pkg.voice_gender}`) | **Nhạc nền:** `{pkg.bgm_track}`",
                    f"- **Title Hook trên màn hình (2s đầu):** `{pkg.hook_title_on_screen_vi}` ({len(pkg.hook_title_on_screen_vi)} ký tự)",
                    "",
                    "### 🔗 Danh sách SKU & Link Shopee Affiliate Chính Hãng trong video",
                    "| Mã SKU (`sku_key`) | Sản phẩm | Gian hàng xác thực | Cấp bảo chứng | Link Affiliate Chính (`short_link`) | Link Affiliate Dự phòng Shopee Mall |",
                    "| :--- | :--- | :--- | :--- | :--- | :--- |",
                ]
            )
            for s in pkg.skus:
                link_cell = s.short_link if s.short_link else "*(Chưa có)*"
                mall_aff_cell = s.mall_search_affiliate_link if s.mall_search_affiliate_link else f"[Shopee Mall]({s.shopee_mall_search_url})"
                lines.append(
                    f"| `{s.sku_key}` | {s.product_name_vi} (~{max(1, s.estimated_price_vnd // 1000)}k) | **{s.shop_name}** | `{s.verification_tier}` | {link_cell} | {mall_aff_cell} |"
                )

            lines.extend(
                [
                    "",
                    "### 📋 1. Tiêu đề Chuẩn SEO 2 Đầu Từ Khóa (`YouTube Shorts`)",
                    "```text",
                    pkg.youtube_shorts_title_vi,
                    "```",
                    "",
                    "### 📝 2. Mô tả 4 Khối Semantic SEO + Timestamps (`YouTube Shorts Description`)",
                    "```text",
                    pkg.youtube_shorts_description_vi,
                    "```",
                    "",
                    "### 🏷️ 3. Thẻ Tags Ẩn Chuẩn SEO (`YouTube Backend Video Tags` - Copy vào ô Tags)",
                    "```text",
                    ", ".join(pkg.youtube_video_tags),
                    "```",
                    "",
                    "### 📱 4. Caption Checklist Chốt Đơn (`Facebook Reels Caption`)",
                    "```text",
                    pkg.facebook_reels_caption_full_vi,
                    "```",
                    "",
                    "### 📌 5. Bình Luận Đầu Tiên / Ghim Gắn Link Shopee Mall (`Pinned Comment`)",
                    "```text",
                    pkg.pinned_comment_vi,
                    "```",
                    "",
                    "---",
                    "",
                ]
            )

        return "\n".join(lines)

    def build_publish_pack(self) -> PublishManifestOutput:
        """
        Scan all `RENDERED` (unposted) videos in `data/video_registry.json`, resolve Official Shopee
        Mall Affiliate links, build Dual-Platform Semantic SEO packages, and write:
          - `output/publish_pack/publish_manifest.json`
          - `output/publish_pack/BANG_DANG_BAI_HANG_NGAY.md`
          - `data/manual_affiliate_links.json`
        """
        self.csv_index = AffiliateCSVRepository()
        manual_data = self._load_manual_links()
        manual_items = manual_data.setdefault("items", {})
        m4_map = self._load_m4_rendered_map()

        rendered_entries = self.video_registry.get_rendered_unposted_videos()
        # Soft Priority Ordering for M5 (Section 8.8):
        #   1. Partially-published videos resuming platform #2 (`len(published_platforms) > 0`) ALWAYS go first.
        #   2. Active non-expired `FAST_TRACK_TREND` videos go next.
        #   3. Evergreen / `NORMAL_TREND` videos follow in existing order.
        fast_track_cats = {
            t.product_concept_id
            for t in load_active_trend_opportunities()
            if t.priority_tier == "FAST_TRACK_TREND"
        }
        rendered_entries.sort(
            key=lambda e: (
                0 if len(e.published_platforms) > 0 else 1,
                0 if e.category_id in fast_track_cats else 1,
            )
        )
        packages: List[PublishVideoPackageItem] = []
        packages_by_id: Dict[str, PublishVideoPackageItem] = {}

        for entry in rendered_entries:
            pkg = self._build_video_package(entry, manual_items, m4_map)
            packages.append(pkg)
            packages_by_id[pkg.script_id] = pkg

        # If Cloudflare API Token is configured in .env, sync all registered slugs to Edge once and rebuild short URLs
        cf_mgr = CloudflareShortLinkManager()
        if cf_mgr.is_configured():
            try:
                ok_cf, _ = cf_mgr.sync_to_cloudflare()
                if ok_cf:
                    packages.clear()
                    packages_by_id.clear()
                    for entry in rendered_entries:
                        pkg = self._build_video_package(entry, manual_items, m4_map)
                        packages.append(pkg)
                        packages_by_id[pkg.script_id] = pkg
            except Exception as exc:
                print(f"[CLOUDFLARE SHORTENER] Cảnh báo đồng bộ (giữ link s.shopee.vn dự phòng): {exc}", flush=True)

        self._sync_back_to_m4_output(packages_by_id)
        self._save_manual_links(manual_data)

        ready_count = sum(1 for p in packages if p.all_links_ready)
        needing_count = len(packages) - ready_count
        now_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")

        manifest = PublishManifestOutput(
            generated_at=now_iso,
            total_rendered_unposted_videos=len(packages),
            videos_with_all_affiliate_links_ready=ready_count,
            videos_needing_shopee_links=needing_count,
            manual_links_json_path=str(self.manual_links_path),
            affiliate_csv_dir=str(self.csv_index.csv_dir),
            packages=packages,
        )

        atomic_write_text(
            self.manifest_path,
            manifest.model_dump_json(indent=2),
        )
        atomic_write_text(
            self.markdown_path,
            self._render_markdown_sheet(manifest),
        )

        print(
            f"[STEP A - SEO PACKAGER] Đã xuất gói SEO & Affiliate cho {len(packages)} video "
            f"(đủ link chính hãng: {ready_count}/{len(packages)}) -> {self.markdown_path}",
            flush=True,
        )
        return manifest


def main() -> None:
    packager = PublishPackager()
    packager.build_publish_pack()


if __name__ == "__main__":
    main()
