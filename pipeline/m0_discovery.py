"""
Module M0: Autonomous Multi-Brand SKU Discovery (`pipeline/m0_discovery.py`).

Responsibilities (Black-Box Contract):
  1. Knows ONLY the 2 root domains:
     - `TECH_DESK`    : Đồ công nghệ & Góc làm việc
     - `KITCHEN_TECH` : Đồ dùng & Công nghệ nhà bếp
  2. Reads `data/m0_history.json` to avoid repeating recently explored sub-niches.
  3. Step M0.1 (LLM Planner): Autonomously selects a high-demand problem/sub-niche and generates
     BRAND-AGNOSTIC functional search queries.
  4. Step M0.2 (Zero-Quota Harvester): Harvests multi-brand market evidence from DuckDuckGo Web,
     YouTube (`yt-dlp`), and any available Shopee Affiliate Bulk CSVs in `data/affiliate_csv/` or `~/Downloads`.
  5. Step M0.3 (LLM Multi-Brand Extractor): Extracts 4-5 competing candidates per functional category
     (multiple competing brands + OEM baseline + honest caveats) into strict Pydantic schemas.
  6. Step M0.4 (Targeted Video & CSV Linker): Enriches each candidate with specific YouTube review clips
     (in parallel via ThreadPoolExecutor) and optional Shopee Affiliate offers.
  7. Outputs `data/m0_candidate_pool.json` for Module M1.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import glob
import json
import re
import shutil
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
import yt_dlp
from bs4 import BeautifulSoup

from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import atomic_write_json, atomic_write_text, safe_load_json
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.sku_registry import SKURegistryManager
from pipeline.schemas_m0 import (
    AffiliateListingMatch,
    DiscoveredCandidateSKU,
    DomainId,
    FunctionalCategoryGroup,
    LLMCategoryExtractionResponse,
    LLMPlannedCategoryQuery,
    LLMSubNichePlanResponse,
    M0CandidatePoolOutput,
    MarketPriceRangeVND,
    YouTubeEvidenceClip,
)
from pipeline.schemas_m_trend import load_active_trend_opportunities

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
AFFILIATE_CSV_DIR = DATA_DIR / "affiliate_csv"
HISTORY_PATH = DATA_DIR / "m0_history.json"
OUTPUT_POOL_PATH = DATA_DIR / "m0_candidate_pool.json"

ROOT_DOMAINS: Dict[DomainId, str] = {
    "KITCHEN_TECH": "Đồ dùng & Công nghệ nhà bếp thông minh (Sơ chế, nấu nướng, bảo quản, tối ưu căn bếp)",
    "TECH_DESK": "Đồ công nghệ & Phụ kiện góc làm việc (Desk setup, cáp sạc, giá đỡ, chuột phím, phụ kiện năng suất)",
}

# Curated 18-category rotation taxonomy (9 TECH_DESK + 9 KITCHEN_TECH) with semantic synonym tokens
# so M0 can deterministically detect and block category repetition across runs and 3-7 day cooldowns.
CATEGORY_ROTATION_TAXONOMY: List[Dict[str, Any]] = [
    # --- TECH_DESK (Ergonomic & Productivity) ---
    {
        "category_id": "screen_hanging_light_bar",
        "domain": "TECH_DESK",
        "category_name_vi": "Đèn treo màn hình máy tính chống chói bảo vệ mắt",
        "problem_solved_vi": "Chống lóa màn hình và giảm mỏi mắt khi làm việc ban đêm mà không tốn diện tích mặt bàn",
        "synonyms_norm": ["screen_hanging_light", "monitor_light_bar", "den_treo_man_hinh", "den_man_hinh", "iwok"],
        "generic_web_queries_vi": [
            "đèn treo màn hình máy tính chống chói loại nào tốt",
            "so sánh đèn kẹp màn hình bảo vệ mắt",
        ],
        "generic_youtube_queries_vi": [
            "review đèn treo màn hình chống chói",
            "đánh giá đèn treo màn hình máy tính",
        ],
    },
    {
        "category_id": "ergonomic_vertical_mouse",
        "domain": "TECH_DESK",
        "category_name_vi": "Chuột công thái học dọc chống mỏi cổ tay",
        "problem_solved_vi": "Giảm áp lực ống cổ tay và chống tê mỏi khi dùng máy tính liên tục 8 tiếng mỗi ngày",
        "synonyms_norm": ["vertical_mouse", "ergonomic_mouse", "chuot_cong_thai_hoc", "chuot_doc"],
        "generic_web_queries_vi": [
            "chuột công thái học dọc không dây loại nào tốt",
            "kinh nghiệm chọn chuột dọc chống mỏi cổ tay",
        ],
        "generic_youtube_queries_vi": [
            "review chuột công thái học dọc",
            "so sánh chuột dọc chống mỏi tay",
        ],
    },
    {
        "category_id": "aluminum_laptop_stand_foldable",
        "domain": "TECH_DESK",
        "category_name_vi": "Giá đỡ laptop hợp kim nhôm tản nhiệt gấp gọn",
        "problem_solved_vi": "Nâng màn hình laptop ngang tầm mắt chống gù cổ và tăng lưu thông khí tản nhiệt",
        "synonyms_norm": ["laptop_stand", "gia_do_laptop", "ke_laptop_nhom", "de_tan_nhiet_laptop"],
        "generic_web_queries_vi": [
            "giá đỡ laptop hợp kim nhôm gấp gọn loại nào chắc chắn",
            "so sánh kệ đỡ laptop tản nhiệt chống mỏi cổ",
        ],
        "generic_youtube_queries_vi": [
            "review giá đỡ laptop nhôm gấp gọn",
            "đánh giá kệ nâng laptop tản nhiệt",
        ],
    },
    {
        "category_id": "gan_desktop_fast_charger_station",
        "domain": "TECH_DESK",
        "category_name_vi": "Trạm sạc nhanh GaN đa cổng để bàn gọn dây",
        "problem_solved_vi": "Thay thế 3-4 củ sạc rời rạc trên bàn làm việc bằng một trạm sạc GaN công suất cao an toàn",
        "synonyms_norm": ["gan_charger", "desktop_charger", "tram_sac_gan", "cu_sac_nhanh_gan", "sac_da_cong"],
        "generic_web_queries_vi": [
            "trạm sạc nhanh GaN để bàn nhiều cổng loại nào tốt",
            "so sánh củ sạc nhanh GaN 65W 100W đa cổng",
        ],
        "generic_youtube_queries_vi": [
            "review trạm sạc GaN để bàn",
            "đánh giá sạc nhanh GaN đa cổng",
        ],
    },
    {
        "category_id": "usb_c_multiport_docking_hub",
        "domain": "TECH_DESK",
        "category_name_vi": "Hub chuyển đổi USB-C đa năng xuất hình 4K HDMI",
        "problem_solved_vi": "Mở rộng cổng kết nối HDMI 4K, USB 3.0, thẻ nhớ và sạc PD cho laptop mỏng nhẹ mà không bị nóng",
        "synonyms_norm": ["usb_c_hub", "type_c_hub", "bo_chuyen_doi_usb_c", "docking_station"],
        "generic_web_queries_vi": [
            "hub chuyển đổi USB-C ra HDMI 4K loại nào ổn định ít nóng",
            "so sánh bộ chia cổng Type-C đa năng cho laptop",
        ],
        "generic_youtube_queries_vi": [
            "review hub USB-C đa năng",
            "đánh giá bộ chuyển đổi Type-C HDMI",
        ],
    },
    {
        "category_id": "low_profile_wireless_mechanical_keyboard",
        "domain": "TECH_DESK",
        "category_name_vi": "Bàn phím cơ không dây Low-Profile gõ êm văn phòng",
        "problem_solved_vi": "Mang lại cảm giác gõ phím cơ chính xác nhưng mỏng nhẹ, không cần kê tay và không gây ồn nơi công sở",
        "synonyms_norm": ["low_profile_keyboard", "mechanical_keyboard", "ban_phim_co_low_profile", "ban_phim_co_khong_day"],
        "generic_web_queries_vi": [
            "bàn phím cơ không dây low profile gõ êm cho văn phòng",
            "so sánh bàn phím cơ mỏng nhẹ kết nối đa thiết bị",
        ],
        "generic_youtube_queries_vi": [
            "review bàn phím cơ low profile",
            "đánh giá bàn phím cơ không dây văn phòng",
        ],
    },
    {
        "category_id": "gas_spring_monitor_arm",
        "domain": "TECH_DESK",
        "category_name_vi": "Tay nâng màn hình máy tính trợ lực lò xo khí nén",
        "problem_solved_vi": "Giải phóng toàn bộ diện tích chân đế màn hình và xoay chỉnh độ cao góc nhìn linh hoạt",
        "synonyms_norm": ["monitor_arm", "tay_nang_man_hinh", "gia_treo_man_hinh", "arm_man_hinh"],
        "generic_web_queries_vi": [
            "arm màn hình trợ lực khí nén loại nào chịu tải tốt",
            "kinh nghiệm chọn giá treo màn hình máy tính kẹp bàn",
        ],
        "generic_youtube_queries_vi": [
            "review arm màn hình máy tính",
            "so sánh tay nâng màn hình kẹp bàn",
        ],
    },
    {
        "category_id": "magnetic_retractable_fast_charging_cable",
        "domain": "TECH_DESK",
        "category_name_vi": "Cáp sạc nhanh tự cuộn gọn từ tính chống rối",
        "problem_solved_vi": "Xóa sổ cảnh dây cáp sạc rối tung trên mặt bàn và dễ đứt ngầm ở cổ cáp",
        "synonyms_norm": ["magnetic_cable", "retractable_cable", "cap_sac_tu_cuon", "cap_sac_tu_tinh"],
        "generic_web_queries_vi": [
            "cáp sạc nhanh Type-C tự cuộn từ tính loại nào bền",
            "đánh giá dây sạc nhanh 100W chống rối",
        ],
        "generic_youtube_queries_vi": [
            "review cáp sạc từ tính tự cuộn",
            "đánh giá cáp sạc nhanh chống rối",
        ],
    },
    {
        "category_id": "cordless_compressed_air_desk_duster",
        "domain": "TECH_DESK",
        "category_name_vi": "Máy thổi bụi vệ sinh bàn phím và góc máy tính cầm tay",
        "problem_solved_vi": "Thổi sạch bụi kẹt sâu dưới khe bàn phím cơ và quạt tản nhiệt nhanh chóng không dùng bình khí nén",
        "synonyms_norm": ["air_duster", "desk_vacuum", "may_thoi_bui_ban_phim", "may_hut_bui_ban_phim"],
        "generic_web_queries_vi": [
            "máy thổi bụi vệ sinh bàn phím cầm tay dùng pin loại nào mạnh",
            "so sánh máy hút thổi bụi mini cho góc làm việc",
        ],
        "generic_youtube_queries_vi": [
            "review máy thổi bụi bàn phím cầm tay",
            "đánh giá máy vệ sinh bàn phím mini",
        ],
    },
    # --- KITCHEN_TECH (Precision Prep & Smart Cooking) ---
    {
        "category_id": "digital_kitchen_food_scale",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Cân điện tử nhà bếp độ chính xác cao chống nước",
        "problem_solved_vi": "Cân đo chính xác từng 0.1g nguyên liệu làm bánh, pha cà phê và kiểm soát calo bữa ăn",
        "synonyms_norm": ["kitchen_scale", "food_scale", "can_dien_tu_nha_bep", "can_nha_bep", "can_lam_banh"],
        "generic_web_queries_vi": [
            "cân điện tử nhà bếp loại nào chính xác bền chống nước",
            "kinh nghiệm mua cân tiểu ly nhà bếp làm bánh",
        ],
        "generic_youtube_queries_vi": [
            "review cân điện tử nhà bếp",
            "so sánh cân điện tử làm bánh chính xác",
        ],
    },
    {
        "category_id": "instant_read_digital_food_thermometer",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Nhiệt kế nấu ăn điện tử đầu dò inox đo nhanh",
        "problem_solved_vi": "Kiểm soát chuẩn xác nhiệt độ lòng thịt nướng, dầu chiên và sữa pha mà không cần cắt thử",
        "synonyms_norm": ["food_thermometer", "meat_thermometer", "nhiet_ke_nau_an", "nhiet_ke_thuc_pham"],
        "generic_web_queries_vi": [
            "nhiệt kế nấu ăn điện tử đo nhiệt độ thịt dầu chiên loại nào tốt",
            "so sánh nhiệt kế thực phẩm đầu dò inox chống nước",
        ],
        "generic_youtube_queries_vi": [
            "review nhiệt kế nấu ăn điện tử",
            "đánh giá nhiệt kế đo thực phẩm nhà bếp",
        ],
    },
    {
        "category_id": "compact_vacuum_food_sealer",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Máy hút chân không thực phẩm mini gia đình",
        "problem_solved_vi": "Bảo quản thịt cá và đồ khô trong tủ lạnh lâu gấp 3-5 lần mà không bị lẫn mùi hay cháy lạnh",
        "synonyms_norm": ["vacuum_sealer", "may_hut_chan_khong", "hut_chan_khong_thuc_pham"],
        "generic_web_queries_vi": [
            "máy hút chân không thực phẩm gia đình loại nào hút khỏe không kén túi",
            "kinh nghiệm mua máy hàn miệng túi hút chân không nhà bếp",
        ],
        "generic_youtube_queries_vi": [
            "review máy hút chân không thực phẩm gia đình",
            "so sánh máy hút chân không mini nhà bếp",
        ],
    },
    {
        "category_id": "glass_mist_oil_sprayer_bottle",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Bình xịt dầu ăn phun sương thủy tinh dùng cho nồi chiên không dầu",
        "problem_solved_vi": "Phủ đều lớp dầu mỏng lên thực phẩm trong nồi chiên không dầu giúp giòn đều và giảm 70% lượng dầu mỡ",
        "synonyms_norm": ["oil_sprayer", "binh_xit_dau_an", "chai_xit_dau_phun_suong"],
        "generic_web_queries_vi": [
            "bình xịt dầu ăn phun sương thủy tinh loại nào không bị tắc tia",
            "review bình xịt dầu cho nồi chiên không dầu",
        ],
        "generic_youtube_queries_vi": [
            "review bình xịt dầu ăn phun sương",
            "đánh giá chai xịt dầu nồi chiên không dầu",
        ],
    },
    {
        "category_id": "handheld_rechargeable_milk_frother",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Máy đánh bọt cà phê và tạo bọt sữa cầm tay sạc pin",
        "problem_solved_vi": "Đánh bông bọt sữa cafe, matcha và trứng trong 20 giây ngay tại nhà với que inox 304 dễ rửa",
        "synonyms_norm": ["milk_frother", "coffee_frother", "may_danh_bot_ca_phe", "may_tao_bot_sua"],
        "generic_web_queries_vi": [
            "máy đánh bọt cà phê cầm tay sạc pin loại nào khỏe bền",
            "so sánh cây tạo bọt sữa inox 304 tại nhà",
        ],
        "generic_youtube_queries_vi": [
            "review máy đánh bọt cà phê cầm tay",
            "đánh giá máy tạo bọt sữa sạc pin",
        ],
    },
    {
        "category_id": "temperature_control_gooseneck_kettle",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Ấm đun nước siêu tốc chỉnh nhiệt độ giữ ấm pha trà cà phê",
        "problem_solved_vi": "Đun nước đúng chuẩn nhiệt độ (60-100 độ C) cho từng loại trà, cà phê và giữ ấm tự động",
        "synonyms_norm": ["temperature_kettle", "gooseneck_kettle", "am_sieu_toc_chinh_nhiet", "am_dun_nuoc_giu_nhiet"],
        "generic_web_queries_vi": [
            "ấm siêu tốc chỉnh nhiệt độ giữ ấm inox 304 loại nào tốt",
            "so sánh ấm đun nước pha trà cà phê có màn hình nhiệt độ",
        ],
        "generic_youtube_queries_vi": [
            "review ấm siêu tốc chỉnh nhiệt độ",
            "đánh giá ấm đun nước giữ nhiệt thông minh",
        ],
    },
    {
        "category_id": "electric_ceramic_burr_coffee_grinder",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Máy xay cà phê hạt mini dùng pin cối gốm/thép",
        "problem_solved_vi": "Xay hạt cà phê tươi đồng đều nhiều cỡ mịn ngay trước khi pha mà không làm nóng mất hương",
        "synonyms_norm": ["coffee_grinder", "may_xay_ca_phe", "coi_xay_ca_phe"],
        "generic_web_queries_vi": [
            "máy xay cà phê hạt mini tích điện loại nào xay đều bền",
            "so sánh cối xay cà phê cầm tay chỉnh cỡ mịn",
        ],
        "generic_youtube_queries_vi": [
            "review máy xay cà phê mini cầm tay",
            "đánh giá cối xay cà phê hạt dùng pin",
        ],
    },
    {
        "category_id": "magnetic_rotary_kitchen_timer",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Đồng hồ bấm giờ nấu ăn xoay cơ học/LED hít nam châm tủ lạnh",
        "problem_solved_vi": "Canh chuẩn từng giây thời gian ủ bột, luộc trứng, hầm thức ăn chỉ bằng thao tác xoay vòng nhanh",
        "synonyms_norm": ["kitchen_timer", "rotary_timer", "dong_ho_bam_gio_nau_an", "hen_gio_nha_bep"],
        "generic_web_queries_vi": [
            "đồng hồ bấm giờ nấu ăn hít nam châm loại nào bền dễ xoay",
            "so sánh đồng hồ hẹn giờ nhà bếp màn hình LED",
        ],
        "generic_youtube_queries_vi": [
            "review đồng hồ bấm giờ nấu ăn nam châm",
            "đánh giá timer hẹn giờ nhà bếp",
        ],
    },
    {
        "category_id": "automatic_touchless_dish_soap_dispenser",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Máy tạo bọt xà phòng/nước rửa chén cảm ứng tự động chống nước",
        "problem_solved_vi": "Lấy bọt nước rửa chén/rửa tay tự động không cần chạm tay đang dính dầu mỡ vào chai",
        "synonyms_norm": ["soap_dispenser", "may_tao_bot_xa_phong", "binh_xit_nuoc_rua_chen_tu_dong"],
        "generic_web_queries_vi": [
            "máy tạo bọt nước rửa chén cảm ứng tự động chống nước loại nào bền",
            "so sánh bình lấy xà phòng cảm ứng nhà bếp",
        ],
        "generic_youtube_queries_vi": [
            "review máy tạo bọt xà phòng cảm ứng",
            "đánh giá máy xịt nước rửa chén tự động",
        ],
    },
    # --- HIGH-VELOCITY SEASONAL / WEATHER CONCEPTS (Synced with M_TREND) ---
    {
        "category_id": "mini_clothes_dryer_dehumidifier",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Máy hút ẩm & máy sấy quần áo mini chống nồm mốc",
        "problem_solved_vi": "Hút ẩm phòng ngủ, tủ quần áo và sấy khô đồ nhanh chóng trong những ngày mưa nồm ẩm kéo dài",
        "synonyms_norm": ["dehumidifier", "may_hut_am", "may_say_quan_ao_mini", "chong_nom_am"],
        "generic_web_queries_vi": [
            "máy hút ẩm mini cho phòng ngủ chống nồm ẩm loại nào tốt",
            "so sánh máy sấy quần áo mini gia đình tiết kiệm điện",
        ],
        "generic_youtube_queries_vi": [
            "review máy hút ẩm mini phòng ngủ",
            "đánh giá máy sấy quần áo mini chống nồm",
        ],
    },
    {
        "category_id": "cordless_rechargeable_fan_powerbank",
        "domain": "TECH_DESK",
        "category_name_vi": "Quạt tích điện mini kiêm sạc dự phòng khi mất điện nắng nóng",
        "problem_solved_vi": "Làm mát linh hoạt trên bàn làm việc và dự phòng thiết yếu khi mất điện mùa nắng nóng",
        "synonyms_norm": ["rechargeable_fan", "quat_tich_dien", "quat_mini_de_ban", "quat_cam_tay"],
        "generic_web_queries_vi": [
            "quạt tích điện mini để bàn pin trâu chạy êm loại nào tốt",
            "so sánh quạt cầm tay mini tốc độ cao pin lâu",
        ],
        "generic_youtube_queries_vi": [
            "review quạt tích điện mini để bàn",
            "đánh giá quạt mini cầm tay pin trâu",
        ],
    },
    {
        "category_id": "hepa_air_purifier_mini",
        "domain": "TECH_DESK",
        "category_name_vi": "Máy lọc không khí mini để bàn màng lọc HEPA khử bụi mịn",
        "problem_solved_vi": "Lọc bụi mịn PM2.5, phấn hoa và mùi hôi trực tiếp tại bàn làm việc và phòng ngủ nhỏ",
        "synonyms_norm": ["air_purifier", "may_loc_khong_khi_mini", "loc_bui_min_hepa"],
        "generic_web_queries_vi": [
            "máy lọc không khí mini để bàn màng lọc HEPA loại nào tốt",
            "so sánh máy lọc không khí phòng ngủ nhỏ khử bụi mịn",
        ],
        "generic_youtube_queries_vi": [
            "review máy lọc không khí mini để bàn",
            "đánh giá máy lọc không khí phòng ngủ nhỏ",
        ],
    },
    {
        "category_id": "mini_cordless_handheld_vacuum",
        "domain": "KITCHEN_TECH",
        "category_name_vi": "Máy hút bụi cầm tay không dây mini cho sofa, giường & ô tô",
        "problem_solved_vi": "Hút sạch vụn thức ăn, tóc rụng, lông thú cưng trên sofa, nệm và nội thất ô tô gọn nhẹ",
        "synonyms_norm": ["handheld_vacuum", "may_hut_bui_cam_tay_mini", "may_hut_bui_o_to"],
        "generic_web_queries_vi": [
            "máy hút bụi cầm tay không dây mini lực hút mạnh loại nào tốt",
            "so sánh máy hút bụi mini cho sofa giường và ô tô",
        ],
        "generic_youtube_queries_vi": [
            "review máy hút bụi cầm tay không dây mini",
            "đánh giá máy hút bụi mini lực hút mạnh",
        ],
    },
]


def _strip_accents(text: str) -> str:
    if not text:
        return ""
    text = text.replace("đ", "d").replace("Đ", "D")
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


def _parse_int_number(raw: str) -> int:
    s = str(raw or "").strip().lower()
    if not s:
        return 0
    m_k = re.search(r"([\d.,]+)\s*(?:k|nghìn|nghin)", s)
    if m_k:
        val_str = m_k.group(1).replace(",", ".")
        try:
            return int(round(float(val_str) * 1000))
        except ValueError:
            pass
    m_tr = re.search(r"([\d.,]+)\s*(?:tr|triệu|trieu)", s)
    if m_tr:
        val_str = m_tr.group(1).replace(",", ".")
        try:
            return int(round(float(val_str) * 1_000_000))
        except ValueError:
            pass
    digits = re.sub(r"[^\d]", "", s)
    return int(digits) if digits else 0


def _parse_sales_count(raw: str) -> int:
    s = str(raw or "").strip().lower().replace(",", ".")
    if not s:
        return 0
    m_k = re.search(r"([\d.]+)\s*(?:k|nghìn|nghin)", s)
    if m_k:
        return int(float(m_k.group(1)) * 1000)
    m_tr = re.search(r"([\d.]+)\s*(?:tr|triệu|trieu)", s)
    if m_tr:
        return int(float(m_tr.group(1)) * 1_000_000)
    return _parse_int_number(s)


def _parse_pct(raw: str) -> float:
    s = str(raw or "").strip().replace("%", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return 0.0


def _detect_ambiguous_listing(title: str) -> bool:
    """Detect Shopee titles that bundle multiple distinct model codes or capacities to bait low anchor prices."""
    upper = title.upper()
    model_tokens = re.findall(r"\b[A-Z]{2,4}[-]?[0-9]{2,4}[A-Z0-9]*\b", upper)
    distinct_models = set(model_tokens)
    if len(distinct_models) >= 2:
        return True
    cap_tokens = re.findall(r"\b\d{3,4}\s*ML\b", upper)
    if len(set(cap_tokens)) >= 2:
        return True
    return False


class AffiliateCSVRepository:
    """Loads optional Shopee Affiliate Bulk CSV files from `data/affiliate_csv/` and `~/Downloads`."""

    def __init__(self, csv_dir: Path = AFFILIATE_CSV_DIR) -> None:
        self.csv_dir = csv_dir
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.scanned_files: List[str] = []
        self.rows: List[Dict[str, Any]] = []
        self._sync_downloads_and_load()

    def _sync_downloads_and_load(self) -> None:
        downloads_dir = Path.home() / "Downloads"
        if downloads_dir.exists():
            for src_str in glob.glob(str(downloads_dir / "*link*sản*phẩm*.csv")) + glob.glob(
                str(downloads_dir / "*Lấy link*.csv")
            ):
                src = Path(src_str)
                dst = self.csv_dir / src.name
                if not dst.exists():
                    try:
                        shutil.copy2(src, dst)
                    except OSError:
                        pass

        csv_paths = sorted(self.csv_dir.glob("*.csv"))
        seen_item_ids = set()
        for path in csv_paths:
            self.scanned_files.append(str(path))
            try:
                with path.open("r", encoding="utf-8-sig", errors="replace") as f:
                    reader = csv.DictReader(f)
                    for r in reader:
                        item_id = (r.get("Mã sản phẩm") or r.get("item_id") or "").strip()
                        title = (r.get("Tên sản phẩm") or r.get("product_name") or "").strip()
                        if not item_id or not title or item_id in seen_item_ids:
                            continue
                        seen_item_ids.add(item_id)
                        price_vnd = _parse_int_number(r.get("Giá") or r.get("price") or "0")
                        sales_count = _parse_sales_count(r.get("Doanh thu") or r.get("sales") or "0")
                        comm_pct = _parse_pct(r.get("Tỉ lệ hoa hồng") or r.get("commission_rate") or "0")
                        comm_vnd = _parse_int_number(r.get("Hoa hồng") or "0")
                        self.rows.append(
                            {
                                "item_id": item_id,
                                "product_name": title,
                                "product_name_norm": _strip_accents(title),
                                "shop_name": (r.get("Tên cửa hàng") or r.get("shop_name") or "").strip(),
                                "price_vnd": price_vnd,
                                "sales_count": sales_count,
                                "commission_rate_pct": comm_pct,
                                "commission_amount_vnd": comm_vnd,
                                "product_link": (r.get("Link sản phẩm") or "").strip(),
                                "short_link": (r.get("Link ưu đãi") or r.get("short_link") or "").strip(),
                                "is_ambiguous_listing": _detect_ambiguous_listing(title),
                            }
                        )
            except Exception:
                continue

    def find_category_rows(self, category_queries: List[str], limit: int = 12) -> List[Dict[str, Any]]:
        """Find CSV rows relevant to a functional category's generic search queries."""
        if not self.rows:
            return []
        keywords = set()
        stop_words = {"loai", "nao", "tot", "so", "sanh", "danh", "gia", "review", "nhat", "cho", "va", "co", "nen", "mua"}
        for q in category_queries:
            tokens = [t for t in re.split(r"\W+", _strip_accents(q)) if len(t) >= 3 and t not in stop_words]
            keywords.update(tokens)

        scored: List[Tuple[int, int, Dict[str, Any]]] = []
        for r in self.rows:
            norm = r["product_name_norm"]
            hits = sum(1 for kw in keywords if kw in norm)
            if hits >= 2:
                scored.append((hits, r["sales_count"], r))
        scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
        return [item[2] for item in scored[:limit]]

    def match_candidate_offers(
        self,
        brand: str,
        model_code: Optional[str],
        category_rows: List[Dict[str, Any]],
    ) -> List[AffiliateListingMatch]:
        """Attach matching Shopee Affiliate offers to a discovered candidate SKU."""
        matches: List[AffiliateListingMatch] = []
        brand_norm = _strip_accents(brand)
        brand_tokens = [t for t in re.split(r"\W+", brand_norm) if len(t) >= 2]
        model_norm = _strip_accents(model_code or "")

        for r in category_rows:
            title_norm = r["product_name_norm"]
            has_model = bool(model_norm and len(model_norm) >= 3 and model_norm in title_norm)
            has_brand = bool(brand_tokens and all(bt in title_norm for bt in brand_tokens))

            if has_model:
                confidence = "EXACT_MODEL"
            elif has_brand:
                if model_norm:
                    other_models = re.findall(r"\b[a-z]{2,4}\d{3,4}[a-z0-9]*\b", title_norm)
                    if other_models and model_norm not in other_models:
                        continue
                confidence = "BRAND_AND_CATEGORY"
            elif brand.upper() == "OEM" and not re.search(
                r"lock|inochi|elmich|bear|philips|ugreen|baseus|anker|hyperwork", title_norm
            ):
                confidence = "CATEGORY_FALLBACK"
            else:
                continue

            matches.append(
                AffiliateListingMatch(
                    item_id=r["item_id"],
                    product_name=r["product_name"],
                    shop_name=r["shop_name"],
                    price_vnd=r["price_vnd"],
                    sales_count=r["sales_count"],
                    commission_rate_pct=r["commission_rate_pct"],
                    commission_amount_vnd=r["commission_amount_vnd"],
                    product_link=r["product_link"],
                    short_link=r["short_link"],
                    is_ambiguous_listing=r["is_ambiguous_listing"],
                    match_confidence=confidence,
                )
            )

        matches.sort(
            key=lambda m: (
                0 if m.match_confidence == "EXACT_MODEL" else 1,
                0 if not m.is_ambiguous_listing else 1,
                -m.sales_count,
            )
        )
        return matches[:5]


def search_duckduckgo_web(queries: List[str], max_results_per_query: int = 5) -> List[Dict[str, str]]:
    """Zero-quota open-web search via DuckDuckGo HTML endpoint."""
    results: List[Dict[str, str]] = []
    seen_urls = set()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
    }
    with httpx.Client(timeout=8.0, follow_redirects=True, headers=headers) as client:
        for q in queries:
            try:
                resp = client.post("https://html.duckduckgo.com/html/", data={"q": q, "kl": "vn-vi"})
                if resp.status_code != 200:
                    continue
                soup = BeautifulSoup(resp.text, "html.parser")
                count = 0
                for block in soup.select(".result"):
                    title_el = block.select_one(".result__a")
                    snippet_el = block.select_one(".result__snippet")
                    if not title_el:
                        continue
                    title = title_el.get_text(" ", strip=True)
                    href = title_el.get("href", "")
                    snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""
                    if not title or href in seen_urls:
                        continue
                    seen_urls.add(href)
                    results.append({"query": q, "title": title, "snippet": snippet, "url": href})
                    count += 1
                    if count >= max_results_per_query:
                        break
            except Exception:
                continue
    return results


def _single_ytdlp_query(query: str, max_results: int = 3) -> List[YouTubeEvidenceClip]:
    clips: List[YouTubeEvidenceClip] = []
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "socket_timeout": 6,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(f"ytsearch{max_results}:{query}", download=False)
            entries = (info or {}).get("entries") or []
            for entry in entries:
                if not entry:
                    continue
                vid = entry.get("id") or ""
                title = entry.get("title") or ""
                if not vid or not title:
                    continue
                duration = int(entry.get("duration") or 0)
                view_count = int(entry.get("view_count") or 0)
                channel = entry.get("uploader") or entry.get("channel") or ""
                clips.append(
                    YouTubeEvidenceClip(
                        video_id=vid,
                        url=f"https://www.youtube.com/watch?v={vid}",
                        title=title,
                        channel=channel,
                        duration_sec=duration,
                        max_height=1080,
                        view_count=view_count,
                        matched_query=query,
                    )
                )
    except Exception:
        pass
    return clips


def search_youtube_ytdlp(queries: List[str], max_per_query: int = 3) -> List[YouTubeEvidenceClip]:
    """Zero-quota parallel YouTube search using yt-dlp flat extraction."""
    clips: List[YouTubeEvidenceClip] = []
    seen_ids = set()
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        future_map = {pool.submit(_single_ytdlp_query, q, max_per_query): q for q in queries}
        for fut in concurrent.futures.as_completed(future_map):
            for c in fut.result():
                if c.video_id not in seen_ids:
                    seen_ids.add(c.video_id)
                    clips.append(c)
    return clips


def load_m0_history(history_path: Path = HISTORY_PATH) -> Dict[str, Any]:
    if not history_path.exists():
        return {"runs": []}
    try:
        return json.loads(history_path.read_text(encoding="utf-8"))
    except Exception:
        return {"runs": []}


def save_m0_history(pool: M0CandidatePoolOutput, history_path: Path = HISTORY_PATH) -> None:
    history = load_m0_history(history_path)
    runs = history.get("runs", [])
    runs.append(
        {
            "run_id": pool.run_id,
            "generated_at": pool.generated_at,
            "domain": pool.domain,
            "sub_niche_id": pool.sub_niche_id,
            "sub_niche_name_vi": pool.sub_niche_name_vi,
            "category_ids": [c.category_id for c in pool.functional_categories],
            "sku_names": [
                f"{cand.brand} {cand.model_code or cand.candidate_id}"
                for grp in pool.functional_categories
                for cand in grp.candidates[:4]
            ],
        }
    )
    history["runs"] = runs[-50:]
    history_path.parent.mkdir(parents=True, exist_ok=True)
    history_path.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


class M0AutonomousDiscoveryEngine:
    """Executes the 4-stage autonomous discovery workflow (M0.1 -> M0.4)."""

    def __init__(self, llm_client: Optional[FreeTierGeminiClient] = None) -> None:
        self.llm = llm_client or FreeTierGeminiClient()
        self.csv_repo = AffiliateCSVRepository()
        self.brand_gate = OfficialBrandVideoGate()
        self.sku_registry = SKURegistryManager()

    def _resolve_canonical_category_id(self, cat_id: str, cat_name_vi: str = "") -> str:
        """Map any category_id or Vietnamese name to its canonical taxonomy ID if it matches synonyms."""
        raw_norm = re.sub(r"[^a-z0-9]+", "_", _strip_accents(f"{cat_id} {cat_name_vi}")).strip("_")
        for item in CATEGORY_ROTATION_TAXONOMY:
            cid = item["category_id"]
            if cid == cat_id or cid in raw_norm:
                return cid
            for syn in item.get("synonyms_norm", []):
                if syn in raw_norm:
                    return cid
        return cat_id

    def _get_blocked_and_fresh_categories(
        self,
        history: Dict[str, Any],
    ) -> Tuple[set[str], List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Determine which categories are blocked (used in recent runs or on 3-7 day SKU cooldown)
        and return fresh unexplored categories from `CATEGORY_ROTATION_TAXONOMY`.
        """
        blocked_ids: set[str] = {"pull_cord_garlic_chopper", "garlic_chopper"}
        # Last 4 runs (~12 categories) + 3-7d SKU cooldown categories
        for r in (history.get("runs") or [])[-4:]:
            for cid in r.get("category_ids") or []:
                blocked_ids.add(self._resolve_canonical_category_id(str(cid)))

        reg_blocks = self.sku_registry.get_blocked_categories_and_skus()
        for cid in reg_blocks.get("cooldown_categories", []):
            blocked_ids.add(self._resolve_canonical_category_id(str(cid)))

        fresh_tech = [
            item
            for item in CATEGORY_ROTATION_TAXONOMY
            if item["domain"] == "TECH_DESK" and item["category_id"] not in blocked_ids
        ]
        fresh_kitchen = [
            item
            for item in CATEGORY_ROTATION_TAXONOMY
            if item["domain"] == "KITCHEN_TECH" and item["category_id"] not in blocked_ids
        ]
        # If nearly all 18 categories have been explored, keep only active 3-7d cooldowns blocked
        if not fresh_tech:
            cd_set = {self._resolve_canonical_category_id(c) for c in reg_blocks.get("cooldown_categories", [])}
            fresh_tech = [
                item for item in CATEGORY_ROTATION_TAXONOMY if item["domain"] == "TECH_DESK" and item["category_id"] not in cd_set
            ] or [item for item in CATEGORY_ROTATION_TAXONOMY if item["domain"] == "TECH_DESK"]
        if not fresh_kitchen:
            cd_set = {self._resolve_canonical_category_id(c) for c in reg_blocks.get("cooldown_categories", [])}
            fresh_kitchen = [
                item for item in CATEGORY_ROTATION_TAXONOMY if item["domain"] == "KITCHEN_TECH" and item["category_id"] not in cd_set
            ] or [item for item in CATEGORY_ROTATION_TAXONOMY if item["domain"] == "KITCHEN_TECH"]

        return blocked_ids, fresh_tech, fresh_kitchen

    def _choose_next_domain(self, history: Dict[str, Any], override_domain: Optional[DomainId]) -> DomainId:
        if override_domain in ("TECH_DESK", "KITCHEN_TECH"):
            return override_domain
        runs = history.get("runs", [])
        if not runs:
            return "KITCHEN_TECH"
        last_domain = runs[-1].get("domain")
        return "TECH_DESK" if last_domain == "KITCHEN_TECH" else "KITCHEN_TECH"

    def step_m0_1_plan_subniche(
        self,
        domain: DomainId,
        history: Dict[str, Any],
        topic_hint: Optional[str] = None,
        max_categories: int = 3,
        ignore_trends: bool = False,
    ) -> LLMSubNichePlanResponse:
        blocked_cat_ids, fresh_tech, fresh_kitchen = self._get_blocked_and_fresh_categories(history)
        reg_blocks = self.sku_registry.get_blocked_categories_and_skus()

        recent_cats = [
            f"- {r.get('sub_niche_id')} ({r.get('sub_niche_name_vi')}): {r.get('category_ids')}"
            for r in history.get("runs", [])[-15:]
        ]
        if blocked_cat_ids:
            recent_cats.append(f"- Đang khóa (vừa lên sóng hoặc trong kỳ nghỉ 3-7 ngày): {sorted(blocked_cat_ids)}")
        past_str = "\n".join(recent_cats) if recent_cats else "(Chưa có lịch sử — đây là lần chạy đầu tiên)"

        recent_skus_list: List[str] = []
        for r in history.get("runs", [])[-15:]:
            recent_skus_list.extend(r.get("sku_names", []) or [])
        recent_skus_list.extend(reg_blocks.get("blocked_sku_names", []))
        recent_skus_str = (
            ", ".join(dict.fromkeys(recent_skus_list))
            if recent_skus_list
            else "(Mặc định tránh các SKU đã làm như cối xay tỏi ớt kéo tay Lock&Lock CKS321 / CKS314)"
        )

        suggested_fresh_lines = [
            f"  * [{item['domain']}] `{item['category_id']}` — {item['category_name_vi']} ({item['problem_solved_vi']})"
            for item in (fresh_tech[:3] + fresh_kitchen[:3])
        ]
        fresh_suggestions_str = "\n".join(suggested_fresh_lines)

        # Upgrade 2 (M_TREND Integration): Live-check active non-expired trends with Half-Life Time-Decay
        live_trends = (
            []
            if ignore_trends
            else [
                t
                for t in load_active_trend_opportunities()
                if self._resolve_canonical_category_id(t.product_concept_id) not in blocked_cat_ids
                and t.re_entry_action not in ("SUPPRESSED_RECENTLY_COVERED", "DUPLICATE_EVENT_IGNORED")
            ]
        )
        if live_trends and not topic_hint:
            trend_lines = [
                f"  * 🔥 [{t.priority_tier} | Điểm thực={t.score_breakdown.effective_score_now_100}/100 | {t.domain}] "
                f"`{t.product_concept_id}` — {t.product_concept_name_vi} | Bối cảnh: {t.trend_title_vi} ({t.need_pain_point_vi})"
                + (f" | LƯU Ý TÁI NHẬP: {t.re_entry_note_vi}" if t.re_entry_action != "NEW_OPPORTUNITY" else "")
                for t in live_trends[:max(1, max_categories)]
            ]
            print(
                f"[M0 TREND-DRIVEN] 📈 Phát hiện {len(live_trends)} cơ hội Trend đang hoạt động -> ưu tiên đưa lên đầu kế hoạch khám phá!",
                flush=True,
            )
            hint_directive = (
                "CƠ HỘI TREND ĐANG LÊN TỪ MODULE M_TREND (BẮT BUỘC ƯU TIÊN CHỌN TRƯỚC CÁC NHÓM NÀY):\n"
                + "\n".join(trend_lines)
                + "\n\nNếu cần thêm nhóm để đủ `max_categories`, chọn bổ sung từ danh sách Evergreen dưới đây:\n"
                + fresh_suggestions_str
            )
        elif topic_hint:
            hint_directive = (
                f"GỢI Ý ƯU TIÊN TỪ NGƯỜI DÙNG: '{topic_hint}'. Hãy lập kế hoạch xoay quanh gợi ý này nhưng vẫn bảo đảm đa dạng chủ đề."
            )
        else:
            print(
                "[M0 EVERGREEN] 🌿 Không có cơ hội Trend mới khả dụng -> tự động chạy chế độ EVERGREEN_ROTATION.",
                flush=True,
            )
            hint_directive = (
                "ƯU TIÊN CHỌN TỪ DANH SÁCH NHÓM SẢN PHẨM MỚI CHƯA LẶP LẠI DƯỚI ĐÂY (hoặc nhóm mới hoàn toàn chưa có trong danh sách khóa):\n"
                f"{fresh_suggestions_str}"
            )

        domain_balance_rule = (
            f"Chủ đề ưu tiên cân bằng kỳ này: `{domain}`, NHƯNG VÌ `max_categories = {max_categories} >= 2`, BẮT BUỘC PHẢI PHÂN BỔ HÀI HÒA GIỮA CẢ HAI CHỦ ĐỀ GỐC:\n"
            f"- `TECH_DESK`: {ROOT_DOMAINS['TECH_DESK']} (VD: giá đỡ laptop nhôm tản nhiệt, hub USB-C đa năng, đèn treo màn hình chống chói, sạc nhanh GaN để bàn, quạt mini để bàn êm).\n"
            f"- `KITCHEN_TECH`: {ROOT_DOMAINS['KITCHEN_TECH']} (VD: ấm đun nước pha trà giữ nhiệt, máy hút chân không thực phẩm mini, cân điện tử nhà bếp chống nước, nhiệt kế nấu ăn điện tử, máy đánh bọt cafe cầm tay, bình xịt dầu ăn phun sương).\n"
            + (
                "Cụ thể với 3 nhóm (`max_categories = 3`), PHẢI có ít nhất 1 nhóm thuộc `TECH_DESK` và ít nhất 1 nhóm thuộc `KITCHEN_TECH` (2-1 hoặc 1-2).\n"
                if max_categories == 3
                else "Với 2 nhóm, PHẢI có đúng 1 nhóm `TECH_DESK` và 1 nhóm `KITCHEN_TECH`.\n"
            )
            if max_categories >= 2
            else f"Chủ đề gốc cho nhóm duy nhất này: `{domain}` — {ROOT_DOMAINS[domain]}.\n"
        )

        system_instruction = (
            "Bạn là Trưởng bộ phận Nghiên cứu Sản phẩm (Module M0) cho kênh Review Đồ Công Nghệ & Đồ Dùng Bếp khách quan.\n"
            "NGUYÊN TẮC BẮT BUỘC:\n"
            "1. KHÔNG BAO GIỜ đưa tên thương hiệu (như Lock&Lock, Inochi, Baseus, Ugreen...) vào `generic_web_queries_vi` hoặc `generic_youtube_queries_vi`.\n"
            "2. Từ khóa tìm kiếm ở Bước M0.1 phải hoàn toàn dựa trên CÔNG NĂNG và VẤN ĐỀ (ví dụ: 'cân điện tử nhà bếp chống nước loại nào tốt', 'đèn treo màn hình chống chói mắt', 'giá đỡ laptop nhôm gấp gọn tản nhiệt').\n"
            "3. Mỗi nhóm `category` phải là MỘT LOẠI THIẾT BỊ HOÀN TOÀN KHÁC NHAU và bắt buộc gán chính xác `domain` (`TECH_DESK` hoặc `KITCHEN_TECH`).\n"
            "4. Không quyết định kịch bản 1 SKU hay Combo — chỉ tập trung khám phá đúng các nhóm công năng độc lập."
        )

        prompt = f"""
{domain_balance_rule}
Số lượng nhóm công năng (`categories`) cần lập kế hoạch: {max_categories} nhóm.

QUY TẮC CHỐNG TRÙNG LẶP & CHỐNG CHẺ NHỎ DUNG TÍCH (STRICT ANTI-SIBLING RULE):
1. CẤM chọn 2 nhóm công năng cùng một loại thiết bị chỉ khác dung tích/kích thước (VD: TUYỆT ĐỐI CẤM tách cối xay tỏi 180ml và 900ml thành 2 nhóm riêng).
2. CẤM chọn lại các nhóm sản phẩm đã xuất hiện trong `recent_cats` hoặc `recent_skus` (đặc biệt TUYỆT ĐỐI KHÔNG chọn lại cối xay tỏi ớt kéo tay `pull_cord_garlic_chopper` / `garlic_chopper` / `Lock&Lock CKS321` / `CKS314`; hãy chọn các thiết bị hoàn toàn khác).
3. Mỗi `category` phải gán đúng trường `domain` (`"TECH_DESK"` hoặc `"KITCHEN_TECH"`).

Lịch sử các ngách/nhóm đã chạy (`recent_cats` — CẤM LẶP LẠI):
{past_str}

Các SKU đã làm gần đây (`recent_skus` — CẤM LẶP LẠI DÒNG MÁY NÀY):
{recent_skus_str}

{hint_directive}

Hãy xuất JSON theo đúng schema `LLMSubNichePlanResponse` gồm ĐÚNG `{max_categories}` nhóm công năng (`categories`) khác biệt rõ rệt về công dụng.
Mỗi nhóm công năng phải có:
- `domain`: `"TECH_DESK"` hoặc `"KITCHEN_TECH"` (phải có cả 2 giá trị trong batch nếu `max_categories >= 2`).
- `generic_web_queries_vi`: 2-3 câu truy vấn tìm kiếm web khách quan, KHÔNG chứa tên hãng.
- `generic_youtube_queries_vi`: 2 câu truy vấn tìm video review/so sánh trên YouTube, KHÔNG chứa tên hãng.
""".strip()

        plan, _ = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMSubNichePlanResponse,
            system_instruction=system_instruction,
            prefer_lite=False,
            temperature=0.3,
        )
        plan.categories = plan.categories[:max_categories]

        # Deterministic Post-LLM Anti-Duplicate Category Replacement Gate (when no manual topic_hint overrides)
        if not topic_hint:
            seen_batch_canonical: set[str] = set()
            fresh_pool = fresh_tech + fresh_kitchen
            replaced_categories: List[LLMPlannedCategoryQuery] = []

            for c in plan.categories:
                canon_id = self._resolve_canonical_category_id(c.category_id, c.category_name_vi)
                if canon_id in blocked_cat_ids or canon_id in seen_batch_canonical:
                    # Pick a fresh non-overlapping replacement from the same domain (or any fresh domain)
                    replacement_dict = next(
                        (
                            item
                            for item in fresh_pool
                            if item["domain"] == c.domain
                            and item["category_id"] not in blocked_cat_ids
                            and item["category_id"] not in seen_batch_canonical
                        ),
                        None,
                    ) or next(
                        (
                            item
                            for item in fresh_pool
                            if item["category_id"] not in blocked_cat_ids
                            and item["category_id"] not in seen_batch_canonical
                        ),
                        None,
                    )
                    if replacement_dict:
                        print(
                            f"[M0 ROTATION] Auto-replacing repeated category '{c.category_id}' ({canon_id}) "
                            f"with fresh category '{replacement_dict['category_id']}' ({replacement_dict['category_name_vi']})",
                            flush=True,
                        )
                        c = LLMPlannedCategoryQuery(
                            category_id=replacement_dict["category_id"],
                            domain=replacement_dict["domain"],
                            category_name_vi=replacement_dict["category_name_vi"],
                            problem_solved_vi=replacement_dict["problem_solved_vi"],
                            generic_web_queries_vi=list(replacement_dict["generic_web_queries_vi"]),
                            generic_youtube_queries_vi=list(replacement_dict["generic_youtube_queries_vi"]),
                        )
                        canon_id = replacement_dict["category_id"]
                seen_batch_canonical.add(canon_id)
                replaced_categories.append(c)
            plan.categories = replaced_categories

        if max_categories >= 2 and len(plan.categories) >= 2:
            present_domains = {c.domain for c in plan.categories}
            if len(present_domains) == 1:
                missing_domain: DomainId = "TECH_DESK" if "KITCHEN_TECH" in present_domains else "KITCHEN_TECH"
                plan.categories[-1].domain = missing_domain
        return plan

    def step_m0_3_extract_multibrand_candidates(
        self,
        domain: DomainId,
        cat_plan: LLMPlannedCategoryQuery,
        web_snippets: List[Dict[str, str]],
        yt_clips: List[YouTubeEvidenceClip],
        csv_rows: List[Dict[str, Any]],
    ) -> LLMCategoryExtractionResponse:
        web_text = "\n".join(
            f"- [{w['title']}]: {w['snippet']}" for w in web_snippets[:10]
        ) or "(Không có kết quả web trực tiếp)"

        yt_text = "\n".join(
            f"- [YouTube: {c.title}] (Kênh: {c.channel}, {c.view_count} views, {c.duration_sec}s)"
            for c in yt_clips[:10]
        ) or "(Không có kết quả YouTube trực tiếp)"

        csv_text = "\n".join(
            f"- [Shopee CSV #{r['item_id']}] {r['product_name']} | Giá: {r['price_vnd']:,}đ | Đã bán: {r['sales_count']} | Shop: {r['shop_name']} | Ambiguous={r['is_ambiguous_listing']}"
            for r in csv_rows[:10]
        ) or "(Không có dữ liệu CSV nội bộ cho nhóm này)"

        system_instruction = (
            "Bạn là Chuyên gia Phân tích Thị trường Đa Thương Hiệu (Module M0.3).\n"
            "Nhiệm vụ của bạn là đọc dữ liệu thu thập từ Web, YouTube và sàn TMĐT của một nhóm công năng, sau đó bóc tách ra **4 đến 5 ứng viên (SKU) cạnh tranh đa thương hiệu** để bàn giao cho Module M1 chấm điểm.\n"
            "QUY TẮC CÔNG BẰNG & CHỐNG THIÊN VỊ:\n"
            "1. BẮT BUỘC phải có ít nhất **2 đến 3 thương hiệu có tên tuổi KHÁC NHAU** cạnh tranh trực tiếp trong cùng nhóm công năng (ví dụ với đồ bếp: Lock&Lock vs Inochi vs Elmich / Bear / Kai / Joseph Joseph; với đồ công nghệ: Ugreen vs Baseus vs HyperWork / Anker / Logitech / Orico).\n"
            "2. BẮT BUỘC phải có ít nhất **1 ứng viên thuộc nhóm `OEM_GENERIC`** (hàng không thương hiệu / phổ thông giá rẻ trên sàn) để Module M1 làm mốc đối chiếu giá trung vị và chất lượng.\n"
            "3. Nếu trong dữ liệu có tin đăng gộp nhiều mã lập lờ (ví dụ tiêu đề ghi chung CKS321 / EJM416 / nhiều dung tích để mồi giá rẻ) hoặc mẫu có lỗi nghiêm trọng, hãy tách riêng hoặc gắn đúng `archetype` / `caveat_severity`.\n"
            "4. Mọi ứng viên ĐỀU PHẢI CÓ `honest_caveat_vi` (nhược điểm thực tế hoặc giới hạn sử dụng thật, không được khen 100%).\n"
            "5. BẮT BUỘC 100% TIẾNG VIỆT CÓ DẤU ĐẦY ĐỦ CHUẨN UTF-8: Kể cả khi tiêu đề trên sàn TMĐT / YouTube viết không dấu, bạn BẮT BUỘC phải chuyển hóa `product_name_vi`, `key_specs_pairs`, `highlight_pros_vi`, `honest_caveat_vi` sang tiếng Việt CÓ DẤU ĐẦY ĐỦ."
        )

        prompt = f"""
Chủ đề gốc: `{domain}`
Nhóm công năng đang phân tích: `{cat_plan.category_id}` — **{cat_plan.category_name_vi}**
Vấn đề giải quyết: {cat_plan.problem_solved_vi}

=== DỮ LIỆU THU THẬP TỪ OPEN WEB ===
{web_text}

=== DỮ LIỆU VIDEO REVIEW TỪ YOUTUBE ===
{yt_text}

=== DỮ LIỆU KHO CSV SHOPEE AFFILIATE NỘI BỘ (NẾU CÓ) ===
{csv_text}

Hãy tổng hợp và xuất ra 4-5 ứng viên cạnh tranh đa thương hiệu theo đúng schema `LLMCategoryExtractionResponse`:
- Ước lượng phổ giá thực tế của nhóm công năng này (`observed_min_price_vnd`, `observed_median_price_vnd`, `observed_max_price_vnd`).
- Danh sách `candidates` (4-5 SKU) đại diện cho các hãng khác nhau + 1 đại diện `OEM_GENERIC`.
- Điền đầy đủ `key_specs_pairs`, `highlight_pros_vi`, `honest_caveat_vi`, `caveat_severity`, và `specific_youtube_search_query_vi` cho từng ứng viên.
""".strip()

        extracted, _ = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMCategoryExtractionResponse,
            system_instruction=system_instruction,
            prefer_lite=True,
            temperature=0.2,
        )
        return extracted

    def run(
        self,
        override_domain: Optional[DomainId] = None,
        topic_hint: Optional[str] = None,
        max_categories: int = 3,
        output_path: Path = OUTPUT_POOL_PATH,
        ignore_trends: bool = False,
    ) -> M0CandidatePoolOutput:
        history = load_m0_history()
        domain = self._choose_next_domain(history, override_domain)

        print(f"[M0] 1/4 Planning sub-niche for domain={domain} (hint={topic_hint!r}, ignore_trends={ignore_trends})...", flush=True)
        plan = self.step_m0_1_plan_subniche(
            domain=domain,
            history=history,
            topic_hint=topic_hint,
            max_categories=max_categories,
            ignore_trends=ignore_trends,
        )
        print(
            f"[M0] Planned sub-niche: {plan.sub_niche_id} ({plan.sub_niche_name_vi}) "
            f"with {len(plan.categories)} functional categories.",
            flush=True,
        )

        functional_groups: List[FunctionalCategoryGroup] = []

        for idx, cat_plan in enumerate(plan.categories, start=1):
            print(
                f"[M0] 2/4 Harvesting Web + YouTube + CSV for category [{idx}/{len(plan.categories)}]: "
                f"{cat_plan.category_name_vi}...",
                flush=True,
            )
            web_results = search_duckduckgo_web(cat_plan.generic_web_queries_vi, max_results_per_query=5)
            yt_generic_clips = search_youtube_ytdlp(cat_plan.generic_youtube_queries_vi, max_per_query=3)
            cat_csv_rows = self.csv_repo.find_category_rows(
                cat_plan.generic_web_queries_vi + [cat_plan.category_name_vi],
                limit=15,
            )

            print(
                f"     -> Harvested {len(web_results)} web snippets, "
                f"{len(yt_generic_clips)} generic YT clips, {len(cat_csv_rows)} CSV rows.",
                flush=True,
            )

            print(
                f"[M0] 3/4 Extracting multi-brand competing candidates for '{cat_plan.category_name_vi}'...",
                flush=True,
            )
            extraction = self.step_m0_3_extract_multibrand_candidates(
                domain=cat_plan.domain,
                cat_plan=cat_plan,
                web_snippets=web_results,
                yt_clips=yt_generic_clips,
                csv_rows=cat_csv_rows,
            )

            print(
                f"[M0] 4/4 Verifying targeted YouTube footage & Affiliate matches for "
                f"{len(extraction.candidates)} candidates in parallel...",
                flush=True,
            )

            # Run targeted official brand YouTube queries for all candidates in parallel
            def _fetch_cand_clips(item_cand) -> List[YouTubeEvidenceClip]:
                q_official = f"{item_cand.brand} {item_cand.model_code or ''} official".strip()
                raw_clips = _single_ytdlp_query(q_official, 3) + _single_ytdlp_query(
                    item_cand.specific_youtube_search_query_vi, 2
                )
                return self.brand_gate.filter_and_resolve_official_clips(
                    brand=item_cand.brand,
                    model_code=item_cand.model_code,
                    candidate_id=item_cand.candidate_id,
                    raw_clips=raw_clips,
                )

            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
                specific_results_list = list(pool.map(_fetch_cand_clips, extraction.candidates))

            discovered_skus: List[DiscoveredCandidateSKU] = []
            for item, specific_clips in zip(extraction.candidates, specific_results_list):
                specs_dict: Dict[str, str] = {}
                for pair in item.key_specs_pairs:
                    if ":" in pair:
                        k, v = pair.split(":", 1)
                        specs_dict[k.strip()] = v.strip()
                    elif pair.strip():
                        specs_dict[f"Thông số {len(specs_dict)+1}"] = pair.strip()

                combined_clips = specific_clips[:]
                seen_vids = {c.video_id for c in combined_clips}
                brand_norm = _strip_accents(item.brand)
                for gc in yt_generic_clips:
                    if gc.video_id not in seen_vids and self.brand_gate.is_official_brand_clip(
                        gc, item.brand, item.model_code
                    ):
                        combined_clips.append(gc)
                        seen_vids.add(gc.video_id)

                cand_snippets: List[str] = []
                for w in web_results:
                    combined_w = f"{w['title']} — {w['snippet']}"
                    if brand_norm in _strip_accents(combined_w) or len(cand_snippets) < 2:
                        cand_snippets.append(combined_w[:220])

                aff_matches = self.csv_repo.match_candidate_offers(
                    brand=item.brand,
                    model_code=item.model_code,
                    category_rows=cat_csv_rows,
                )

                discovered_skus.append(
                    DiscoveredCandidateSKU(
                        candidate_id=item.candidate_id,
                        brand=item.brand,
                        model_code=item.model_code,
                        product_name_vi=item.product_name_vi,
                        archetype=item.archetype,
                        estimated_market_price_vnd=item.estimated_market_price_vnd,
                        key_specs=specs_dict,
                        highlight_pros_vi=item.highlight_pros_vi,
                        honest_caveat_vi=item.honest_caveat_vi,
                        caveat_severity=item.caveat_severity,
                        why_discovered_vi=item.why_discovered_vi,
                        web_evidence_snippets=cand_snippets[:3],
                        youtube_evidence=combined_clips[:3],
                        affiliate_matches=aff_matches,
                    )
                )

            functional_groups.append(
                FunctionalCategoryGroup(
                    category_id=cat_plan.category_id,
                    domain=cat_plan.domain,
                    category_name_vi=cat_plan.category_name_vi,
                    problem_solved_vi=cat_plan.problem_solved_vi,
                    generic_search_queries_used=cat_plan.generic_web_queries_vi + cat_plan.generic_youtube_queries_vi,
                    observed_market_price_range_vnd=MarketPriceRangeVND(
                        min_vnd=extraction.observed_min_price_vnd,
                        median_vnd=extraction.observed_median_price_vnd,
                        max_vnd=extraction.observed_max_price_vnd,
                    ),
                    candidates=discovered_skus,
                )
            )

        now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        run_id = f"m0_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{plan.sub_niche_id}"

        output_pool = M0CandidatePoolOutput(
            run_id=run_id,
            generated_at=now_iso,
            domain=domain,
            domain_name_vi=ROOT_DOMAINS[domain],
            sub_niche_id=plan.sub_niche_id,
            sub_niche_name_vi=plan.sub_niche_name_vi,
            problem_context_vi=plan.problem_context_vi,
            planner_reasoning_vi=plan.planner_reasoning_vi,
            llm_models_used=self.llm.models_used_history,
            csv_files_scanned=self.csv_repo.scanned_files,
            functional_categories=functional_groups,
        )

        atomic_write_text(
            output_path,
            output_pool.model_dump_json(indent=2),
        )
        save_m0_history(output_pool)
        print(f"[M0] Completed! Wrote candidate pool to: {output_path}", flush=True)
        return output_pool


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Module M0: Autonomous Multi-Brand SKU Discovery.")
    parser.add_argument(
        "--domain",
        choices=["TECH_DESK", "KITCHEN_TECH"],
        default=None,
        help="Optional root domain override (defaults to auto-alternating via m0_history.json)",
    )
    parser.add_argument(
        "--hint",
        type=str,
        default=None,
        help="Optional topic hint in Vietnamese (defaults to 100%% autonomous selection)",
    )
    parser.add_argument(
        "--max-categories",
        type=int,
        default=3,
        help="Number of functional categories to discover in one run (default: 3)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_POOL_PATH,
        help="Output JSON path (default: data/m0_candidate_pool.json)",
    )
    args = parser.parse_args()

    engine = M0AutonomousDiscoveryEngine()
    engine.run(
        override_domain=args.domain,  # type: ignore[arg-type]
        topic_hint=args.hint,
        max_categories=args.max_categories,
        output_path=args.output,
    )


if __name__ == "__main__":
    main()
