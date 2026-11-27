"""
Module M0: Autonomous Multi-Brand SKU Discovery (`pipeline/m0_discovery.py`).

Responsibilities (Black-Box Contract):
  1. Knows ONLY the 2 root domains:
     - `CLEANING_CARE`  : Đồ vệ sinh & Chăm sóc nhà cửa thông minh
     - `MINI_APPLIANCE` : Gia dụng mini tiện ích phòng ở & Sinh hoạt
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
    "CLEANING_CARE": "Đồ vệ sinh & Chăm sóc nhà cửa thông minh (Hút bụi, lau sàn, cọ rửa nhà tắm, khử khuẩn giường nệm, chăm sóc bề mặt & vải vóc)",
    "MINI_APPLIANCE": "Gia dụng mini tiện ích phòng ở & Sinh hoạt (Hút ẩm, lọc không khí, bàn ủi hơi nước, sấy khô khử khuẩn, quạt tích điện, bắt muỗi)",
}

# Curated 24-category rotation taxonomy (12 CLEANING_CARE + 12 MINI_APPLIANCE) with semantic synonym tokens
# so M0 can deterministically detect and block category repetition across runs and 3-7 day cooldowns.
CATEGORY_ROTATION_TAXONOMY: List[Dict[str, Any]] = [
    # --- CLEANING_CARE (Smart Home Cleaning, Floor, Bathroom & Fabric Hygiene) ---
    {
        "category_id": "mini_cordless_handheld_vacuum",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy hút bụi cầm tay không dây mini cho sofa, khe cửa & ô tô",
        "problem_solved_vi": "Hút sạch vụn thức ăn, tóc rụng, lông thú cưng trên sofa, khe cửa sổ và nội thất ô tô gọn nhẹ không vướng dây điện",
        "synonyms_norm": ["handheld_vacuum", "may_hut_bui_cam_tay_mini", "may_hut_bui_o_to", "hut_bui_sofa"],
        "generic_web_queries_vi": [
            "máy hút bụi cầm tay không dây mini lực hút mạnh loại nào tốt",
            "so sánh máy hút bụi mini cho sofa giường và ô tô",
        ],
        "generic_youtube_queries_vi": [
            "review máy hút bụi cầm tay không dây mini",
            "đánh giá máy hút bụi mini lực hút mạnh",
        ],
    },
    {
        "category_id": "uv_mattress_dust_mite_vacuum",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy hút bụi giường nệm diệt khuẩn tia UV đập rung tần số cao",
        "problem_solved_vi": "Đập rung tách mạt bụi, tế bào chết, lông thú cưng ẩn sâu trong nệm và chăn gối giúp giảm ngứa da, viêm mũi dị ứng",
        "synonyms_norm": ["mattress_vacuum", "dust_mite_vacuum", "may_hut_bui_giuong_nem", "hut_mat_bui_uv"],
        "generic_web_queries_vi": [
            "máy hút bụi giường nệm diệt khuẩn tia UV loại nào lực hút khỏe",
            "kinh nghiệm mua máy hút mạt bụi chăn ga gối nệm gia đình",
        ],
        "generic_youtube_queries_vi": [
            "review máy hút bụi giường nệm tia UV",
            "so sánh máy hút bụi chăn nệm diệt khuẩn",
        ],
    },
    {
        "category_id": "cordless_electric_spin_scrubber",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Chổi cọ nhà tắm và sàn gạch xoay điện không dây chống nước",
        "problem_solved_vi": "Cọ sạch cặn canxi vách kính, ron gạch ố vàng và bồn rửa nhà tắm nhanh gọn mà không phải cúi gập lưng chà tay mỏi nhừ",
        "synonyms_norm": ["spin_scrubber", "electric_scrubber", "choi_co_nha_tam_dien", "may_co_rua_nha_vesinh"],
        "generic_web_queries_vi": [
            "chổi cọ nhà tắm điện không dây chống nước loại nào khỏe không đứng máy",
            "so sánh máy cọ rửa nhà vệ sinh đa năng cán dài",
        ],
        "generic_youtube_queries_vi": [
            "review chổi cọ nhà tắm điện không dây",
            "đánh giá máy cọ rửa nhà vệ sinh cầm tay",
        ],
    },
    {
        "category_id": "spray_flat_mop_microfiber",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Cây lau nhà phun sương bàn phẳng xoay 360 độ gọn nhẹ",
        "problem_solved_vi": "Xịt sương ẩm vừa đủ giúp sàn gỗ và gạch men khô ngay trong vài chục giây mà không phải xách xô nước nặng nề",
        "synonyms_norm": ["spray_mop", "cay_lau_nha_phun_suong", "choi_lau_nha_xit_nuoc"],
        "generic_web_queries_vi": [
            "cây lau nhà phun sương loại nào bền không tắc vòi xịt",
            "so sánh cây lau nhà xịt nước thông minh cho sàn gỗ chung cư",
        ],
        "generic_youtube_queries_vi": [
            "review cây lau nhà phun sương",
            "đánh giá chổi lau nhà phun sương thông minh",
        ],
    },
    {
        "category_id": "spin_mop_dual_bucket_self_wash",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Bộ cây lau nhà tự vắt thông minh tách nước bẩn và nước sạch",
        "problem_solved_vi": "Giặt sạch bông lau bằng nguồn nước sạch riêng biệt và vắt khô ráo hoàn toàn không cần chạm tay vào nước bẩn",
        "synonyms_norm": ["dual_bucket_mop", "self_wash_mop", "cay_lau_nha_tu_vat", "lau_nha_tach_nuoc_ban"],
        "generic_web_queries_vi": [
            "bộ cây lau nhà tự vắt tách nước bẩn loại nào bền sạch tóc",
            "kinh nghiệm chọn thùng lau nhà thông minh tách nước hai ngăn",
        ],
        "generic_youtube_queries_vi": [
            "review cây lau nhà tách nước bẩn",
            "so sánh bộ lau nhà tự vắt thông minh",
        ],
    },
    {
        "category_id": "spot_carpet_upholstery_cleaner",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy giặt thảm, ghế sofa vải và nệm cầm tay phun hút làm sạch sâu",
        "problem_solved_vi": "Phun dung dịch và hút ngược vết cà phê, sữa đổ, vết bẩn thú cưng bám sâu trong sofa nỉ và thảm trải sàn mà không cần tháo vỏ",
        "synonyms_norm": ["spot_cleaner", "carpet_cleaner", "may_giat_tham_sofa", "may_giat_nem_cam_tay"],
        "generic_web_queries_vi": [
            "máy giặt thảm sofa cầm tay gia đình loại nào hút khỏe nhanh khô",
            "so sánh máy phun hút giặt ghế sofa nệm tại nhà",
        ],
        "generic_youtube_queries_vi": [
            "review máy giặt sofa thảm cầm tay",
            "đánh giá máy phun hút giặt thảm gia đình",
        ],
    },
    {
        "category_id": "smart_touchless_trash_can",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Thùng rác thông minh cảm ứng tự mở nắp khử mùi chống nước",
        "problem_solved_vi": "Mở nắp tự động trong 0.3 giây không cần chạm tay và đậy kín ngăn mùi hôi ẩm, côn trùng trong nhà tắm và phòng bếp",
        "synonyms_norm": ["smart_trash_can", "sensor_trash_bin", "thung_rac_thong_minh", "thung_rac_cam_ung"],
        "generic_web_queries_vi": [
            "thùng rác thông minh cảm ứng chống nước nhà tắm loại nào bền",
            "so sánh thùng rác tự động đóng túi cảm biến hồng ngoại",
        ],
        "generic_youtube_queries_vi": [
            "review thùng rác thông minh cảm ứng",
            "đánh giá thùng rác tự động chống nước",
        ],
    },
    {
        "category_id": "automatic_touchless_foam_soap_dispenser",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy tạo bọt xà phòng rửa tay và nước rửa chén cảm ứng tự động",
        "problem_solved_vi": "Tạo bọt mịn tức thì khi đưa tay vào cảm biến giúp rửa sạch nhanh, tiết kiệm dung dịch và giữ chai lọ luôn khô sạch",
        "synonyms_norm": ["soap_dispenser", "foam_dispenser", "may_tao_bot_xa_phong", "binh_xit_xa_phong_cam_ung"],
        "generic_web_queries_vi": [
            "máy tạo bọt xà phòng rửa tay cảm ứng chống nước IPX5 loại nào bền",
            "so sánh bình lấy nước rửa chén tạo bọt tự động sạc pin",
        ],
        "generic_youtube_queries_vi": [
            "review máy tạo bọt xà phòng cảm ứng",
            "đánh giá máy rửa tay tạo bọt tự động",
        ],
    },
    {
        "category_id": "rechargeable_fabric_lint_remover",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy cắt lông xù quần áo và áo len sạc pin lưỡi thép an toàn",
        "problem_solved_vi": "Cắt sạch hạt xù lông trên áo khoác, đồ len, quần nỉ và vỏ ghế sofa trong vài phút mà không làm rách sợi vải",
        "synonyms_norm": ["lint_remover", "fabric_shaver", "may_cat_long_xu", "may_cao_long_xu_quan_ao"],
        "generic_web_queries_vi": [
            "máy cắt lông xù quần áo sạc pin loại nào sắc bền không rách vải",
            "so sánh máy tỉa lông xù áo len có khoang hút bụi vải",
        ],
        "generic_youtube_queries_vi": [
            "review máy cắt lông xù quần áo",
            "đánh giá máy cắt lông xù sạc pin",
        ],
    },
    {
        "category_id": "ultrasonic_jewelry_glasses_cleaner",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy rửa kính mắt, trang sức và dây đồng hồ bằng sóng siêu âm 45kHz",
        "problem_solved_vi": "Đánh bật dầu nhờn, bụi bẩn kẹt sâu trong khe bản lề gọng kính mắt, nhẫn và dây đồng hồ kim loại chỉ với nước sạch",
        "synonyms_norm": ["ultrasonic_cleaner", "may_rua_kinh_sieu_am", "be_rua_sieu_am_mini"],
        "generic_web_queries_vi": [
            "máy rửa kính mắt trang sức sóng siêu âm mini loại nào làm sạch thật",
            "so sánh bể rửa siêu âm 45kHz vệ sinh kính và đồng hồ tại nhà",
        ],
        "generic_youtube_queries_vi": [
            "review máy rửa kính mắt sóng siêu âm",
            "đánh giá máy làm sạch siêu âm mini",
        ],
    },
    {
        "category_id": "cordless_window_glass_vacuum_wiper",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy lau kính hút nước bẩn cầm tay chống đọng vệt cho cửa sổ & vách tắm",
        "problem_solved_vi": "Gạt phẳng và hút sạch nước bẩn trên vách kính phòng tắm, gương soi và cửa kính chung cư không nhỏ giọt xuống sàn",
        "synonyms_norm": ["window_vacuum", "glass_wiper", "may_lau_kinh_cam_tay", "cay_gat_kinh_hut_nuoc"],
        "generic_web_queries_vi": [
            "máy lau kính hút nước bẩn cầm tay dùng pin loại nào sạch không vệt",
            "kinh nghiệm mua máy vệ sinh vách kính nhà tắm chống ố canxi",
        ],
        "generic_youtube_queries_vi": [
            "review máy lau kính cầm tay hút nước",
            "đánh giá máy vệ sinh kính không dây",
        ],
    },
    {
        "category_id": "cordless_turbo_jet_air_duster",
        "domain": "CLEANING_CARE",
        "category_name_vi": "Máy thổi bụi phản lực cầm tay vệ sinh khe cửa sổ, lồng quạt & góc hẹp",
        "problem_solved_vi": "Thổi bay bụi đóng mảng trong rãnh trượt cửa lùa, lưới lọc điều hòa, lồng quạt và góc tủ mà chổi thường không luồn tới",
        "synonyms_norm": ["air_duster", "jet_fan_duster", "may_thoi_bui_cam_tay", "quat_phan_luc_thoi_bui"],
        "generic_web_queries_vi": [
            "máy thổi bụi phản lực cầm tay dùng pin không chổi than loại nào mạnh",
            "so sánh máy hút thổi bụi mini vệ sinh khe cửa và quạt điều hòa",
        ],
        "generic_youtube_queries_vi": [
            "review máy thổi bụi phản lực cầm tay",
            "đánh giá quạt phản lực mini vệ sinh bụi",
        ],
    },
    # --- MINI_APPLIANCE (Compact Home Comfort, Garment, Air & Daily Living Appliances) ---
    {
        "category_id": "handheld_foldable_garment_steamer",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Bàn ủi hơi nước cầm tay gấp gọn làm nóng nhanh không cần cầu là",
        "problem_solved_vi": "Ủi phẳng áo sơ mi, váy voan ngay trên móc treo trong 30 giây buổi sáng vội đi làm hoặc mang gọn trong vali công tác",
        "synonyms_norm": ["garment_steamer", "handheld_steamer", "ban_ui_hoi_nuoc_cam_tay", "ban_la_hoi_nuoc_gap_gon"],
        "generic_web_queries_vi": [
            "bàn ủi hơi nước cầm tay gấp gọn loại nào hơi mạnh không rỉ nước",
            "so sánh bàn là hơi nước cầm tay công suất lớn đi làm du lịch",
        ],
        "generic_youtube_queries_vi": [
            "review bàn ủi hơi nước cầm tay gấp gọn",
            "so sánh bàn là hơi nước cầm tay loại nào tốt",
        ],
    },
    {
        "category_id": "mini_room_closet_dehumidifier",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy hút ẩm mini phòng ngủ và tủ quần áo chống nồm ẩm nấm mốc",
        "problem_solved_vi": "Giữ phòng ngủ nhỏ, phòng thay đồ và tủ máy ảnh khô thoáng, ngăn mùi ẩm mốc khó chịu trong mùa mưa nồm kéo dài",
        "synonyms_norm": ["mini_dehumidifier", "dehumidifier", "may_hut_am_mini", "hut_am_phong_ngu", "chong_nom_am"],
        "generic_web_queries_vi": [
            "máy hút ẩm mini cho phòng ngủ nhỏ chống nồm ẩm loại nào chạy êm",
            "so sánh máy hút ẩm mini tủ quần áo tiết kiệm điện",
        ],
        "generic_youtube_queries_vi": [
            "review máy hút ẩm mini phòng ngủ",
            "đánh giá máy hút ẩm mini chống nồm",
        ],
    },
    {
        "category_id": "hepa_desktop_bedroom_air_purifier",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy lọc không khí mini phòng ngủ màng lọc HEPA H13 khử mùi & bụi mịn",
        "problem_solved_vi": "Lọc sạch bụi mịn PM2.5, lông mèo bay lơ lửng và mùi thức ăn bí bách trong phòng ngủ nhỏ dưới 20m2 với độ ồn êm ban đêm",
        "synonyms_norm": ["air_purifier", "hepa_purifier", "may_loc_khong_khi_mini", "loc_khong_khi_phong_ngu"],
        "generic_web_queries_vi": [
            "máy lọc không khí mini phòng ngủ màng lọc HEPA H13 loại nào êm bền",
            "so sánh máy lọc không khí nhỏ gọn khử mùi thú cưng và bụi mịn",
        ],
        "generic_youtube_queries_vi": [
            "review máy lọc không khí mini phòng ngủ",
            "đánh giá máy lọc không khí HEPA nhỏ gọn",
        ],
    },
    {
        "category_id": "electric_shoe_dryer_deodorizer",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy sấy khô và khử mùi giày dép bằng nhiệt ấm & tia UV mùa mưa",
        "problem_solved_vi": "Sấy khô đều từ mũi tới gót giày thể thao, giày da bị ướt mưa và khử mùi hôi ẩm qua đêm mà không làm bong keo giày",
        "synonyms_norm": ["shoe_dryer", "may_say_giay", "say_kho_khu_mui_giay"],
        "generic_web_queries_vi": [
            "máy sấy giày khử mùi hẹn giờ tự ngắt loại nào bền an toàn",
            "so sánh máy sấy khô giày dép mùa mưa không hỏng keo",
        ],
        "generic_youtube_queries_vi": [
            "review máy sấy giày khử mùi",
            "đánh giá máy sấy khô giày dép mùa mưa",
        ],
    },
    {
        "category_id": "portable_mini_clothes_dryer",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy sấy quần áo mini gấp gọn khử khuẩn cho căn hộ nhỏ & đồ em bé",
        "problem_solved_vi": "Sấy khô nhanh mẻ quần áo mặc hàng ngày, đồ lót và khăn em bé trong ngày mưa dầm thiếu nắng mà không chiếm chỗ ban công",
        "synonyms_norm": ["mini_clothes_dryer", "portable_dryer", "may_say_quan_ao_mini", "tu_say_quan_ao_gap_gon"],
        "generic_web_queries_vi": [
            "máy sấy quần áo mini gấp gọn cho phòng trọ chung cư loại nào tốt",
            "so sánh tủ sấy quần áo mini khử khuẩn tiết kiệm điện",
        ],
        "generic_youtube_queries_vi": [
            "review máy sấy quần áo mini gấp gọn",
            "đánh giá máy sấy quần áo mini gia đình",
        ],
    },
    {
        "category_id": "quiet_air_circulator_desk_fan",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Quạt đối lưu tuần hoàn không khí / quạt tích điện mini chạy êm",
        "problem_solved_vi": "Đẩy luồng khí mát điều hòa lan đều khắp phòng giúp tiết kiệm điện và dự phòng gió mát êm ái khi mất điện mùa hè",
        "synonyms_norm": ["air_circulator", "rechargeable_fan", "quat_tuan_hoan_khong_khi", "quat_doi_luu", "quat_tich_dien_mini"],
        "generic_web_queries_vi": [
            "quạt tuần hoàn không khí để bàn chạy êm cho phòng điều hòa loại nào tốt",
            "so sánh quạt tích điện mini đối lưu không khí pin lâu",
        ],
        "generic_youtube_queries_vi": [
            "review quạt tuần hoàn không khí mini",
            "đánh giá quạt đối lưu không khí phòng máy lạnh",
        ],
    },
    {
        "category_id": "ultrasonic_cool_mist_humidifier_diffuser",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy tạo độ ẩm & khuếch tán tinh dầu siêu âm chống khô họng phòng máy lạnh",
        "problem_solved_vi": "Bù ẩm sương mịn tự động giúp hết khô mũi họng, nứt môi khi bật điều hòa cả đêm và thư giãn nhẹ nhàng trong phòng ngủ",
        "synonyms_norm": ["humidifier", "aroma_diffuser", "may_tao_am_phong_ngu", "may_xong_tinh_dau_sieu_am"],
        "generic_web_queries_vi": [
            "máy tạo ẩm không khí phòng điều hòa siêu âm êm dễ vệ sinh loại nào tốt",
            "so sánh máy phun sương tạo ẩm khử khuẩn cho phòng ngủ",
        ],
        "generic_youtube_queries_vi": [
            "review máy tạo độ ẩm phòng điều hòa",
            "đánh giá máy tạo ẩm phun sương phòng ngủ",
        ],
    },
    {
        "category_id": "uv_electric_mosquito_trap_swatter",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Đèn bắt muỗi tia UV & vợt muỗi điện thông minh tự bẫy muỗi ban đêm",
        "problem_solved_vi": "Thu hút và tiêu diệt muỗi tự động ban đêm không khói hóa chất độc hại, bảo vệ giấc ngủ gia đình mùa mưa sốt xuất huyết",
        "synonyms_norm": ["mosquito_trap", "electric_swatter", "den_bat_muoi", "vot_bat_muoi_thong_minh"],
        "generic_web_queries_vi": [
            "đèn bắt muỗi thông minh và vợt muỗi kiêm đèn bẫy UV loại nào hiệu quả thật",
            "kinh nghiệm chọn máy bắt muỗi cho phòng ngủ an toàn không ồn",
        ],
        "generic_youtube_queries_vi": [
            "review đèn bắt muỗi thông minh",
            "đánh giá vợt muỗi điện kiêm đèn bắt muỗi",
        ],
    },
    {
        "category_id": "mini_portable_travel_electric_kettle",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Bình đun nước siêu tốc mini giữ nhiệt cá nhân ruột Inox 316 an toàn",
        "problem_solved_vi": "Đun sôi nước pha trà, pha sữa và giữ ấm gọn như bình giữ nhiệt ngay tại bàn phòng ngủ hoặc mang đi du lịch tránh ấm khách sạn bẩn",
        "synonyms_norm": ["portable_kettle", "travel_kettle", "binh_dun_nuoc_mini", "am_sieu_toc_du_lich"],
        "generic_web_queries_vi": [
            "bình đun nước siêu tốc mini giữ nhiệt inox 304 316 loại nào an toàn",
            "so sánh cốc đun nước cầm tay du lịch chỉnh nhiệt độ",
        ],
        "generic_youtube_queries_vi": [
            "review bình đun nước siêu tốc mini",
            "đánh giá bình đun nước giữ nhiệt du lịch",
        ],
    },
    {
        "category_id": "high_speed_ionic_compact_hair_dryer",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy sấy tóc ion âm tốc độ cao nhỏ gọn nhanh khô không xơ rối",
        "problem_solved_vi": "Làm khô tóc dày chỉ trong 3–5 phút bằng luồng gió mạnh kiểm soát nhiệt thông minh thay vì đốt nóng làm cháy xơ ngọn tóc",
        "synonyms_norm": ["ionic_hair_dryer", "high_speed_hair_dryer", "may_say_toc_ion_am", "may_say_toc_toc_do_cao"],
        "generic_web_queries_vi": [
            "máy sấy tóc ion âm tốc độ cao không chổi than loại nào êm bền",
            "so sánh máy sấy tóc nhỏ gọn kiểm soát nhiệt bảo vệ tóc",
        ],
        "generic_youtube_queries_vi": [
            "review máy sấy tóc ion âm tốc độ cao",
            "so sánh máy sấy tóc không chổi than nhỏ gọn",
        ],
    },
    {
        "category_id": "motion_sensor_rechargeable_closet_light",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Đèn LED cảm biến chuyển động sạc pin hít nam châm cho tủ áo & hành lang",
        "problem_solved_vi": "Tự bật sáng dịu mắt ngay khi mở tủ quần áo, bước xuống giường ban đêm hoặc đi cầu thang mà không cần khoan đục đi dây điện",
        "synonyms_norm": ["motion_sensor_light", "closet_light", "den_cam_bien_chuyen_dong", "den_led_dan_tu_quan_ao"],
        "generic_web_queries_vi": [
            "đèn LED thanh cảm biến chuyển động sạc pin dán tủ quần áo loại nào pin trâu",
            "so sánh đèn ngủ cảm biến hồng ngoại hành lang cầu thang",
        ],
        "generic_youtube_queries_vi": [
            "review đèn cảm biến chuyển động tủ quần áo",
            "đánh giá đèn LED cảm ứng sạc pin nam châm",
        ],
    },
    {
        "category_id": "cordless_portable_oral_water_flosser",
        "domain": "MINI_APPLIANCE",
        "category_name_vi": "Máy tăm nước cầm tay chống nước IPX7 vệ sinh kẽ răng & niềng răng",
        "problem_solved_vi": "Tia nước xung nhịp cuốn sạch mảng bám thức ăn sâu trong kẽ răng hàm và mắc cài niềng răng nơi bàn chải không chạm tới",
        "synonyms_norm": ["water_flosser", "oral_irrigator", "may_tam_nuoc_cam_tay", "tam_nuoc_du_lich"],
        "generic_web_queries_vi": [
            "máy tăm nước cầm tay chống nước IPX7 tia êm không chảy máu nướu loại nào bền",
            "kinh nghiệm chọn máy tăm nước cá nhân nhỏ gọn dễ vệ sinh bình nước",
        ],
        "generic_youtube_queries_vi": [
            "review máy tăm nước cầm tay loại nào bền",
            "so sánh máy tăm nước cá nhân chống nước IPX7",
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
                r"lock|inochi|elmich|bear|philips|deerma|xiaomi|dreame|roborock|tineco|dyson|karcher|levoit|panasonic|tefal|simplus|lumias",
                title_norm,
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
        self._llm: Optional[FreeTierGeminiClient] = llm_client
        self.csv_repo = AffiliateCSVRepository()
        self.brand_gate = OfficialBrandVideoGate()
        self.sku_registry = SKURegistryManager()

    @property
    def llm(self) -> FreeTierGeminiClient:
        if self._llm is None:
            self._llm = FreeTierGeminiClient()
        return self._llm

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
        blocked_ids: set[str] = set()
        # Last 4 runs (~12 categories) + 3-7d SKU cooldown categories
        for r in (history.get("runs") or [])[-4:]:
            for cid in r.get("category_ids") or []:
                blocked_ids.add(self._resolve_canonical_category_id(str(cid)))

        reg_blocks = self.sku_registry.get_blocked_categories_and_skus()
        for cid in reg_blocks.get("cooldown_categories", []):
            blocked_ids.add(self._resolve_canonical_category_id(str(cid)))

        fresh_cleaning = [
            item
            for item in CATEGORY_ROTATION_TAXONOMY
            if item["domain"] == "CLEANING_CARE" and item["category_id"] not in blocked_ids
        ]
        fresh_appliance = [
            item
            for item in CATEGORY_ROTATION_TAXONOMY
            if item["domain"] == "MINI_APPLIANCE" and item["category_id"] not in blocked_ids
        ]
        # If nearly all 24 categories have been explored within 14d, fall back to oldest-used categories
        # so the pipeline rotates cleanly without ever deadlocking or repeating yesterday's category
        recent_cat_ts = self.sku_registry.get_recent_categories(within_days=14)
        if not fresh_cleaning:
            clean_all = [item for item in CATEGORY_ROTATION_TAXONOMY if item["domain"] == "CLEANING_CARE"]
            clean_all.sort(key=lambda it: recent_cat_ts.get(it["category_id"], ""))
            fresh_cleaning = clean_all[: max(2, len(clean_all) // 2)]
        if not fresh_appliance:
            app_all = [item for item in CATEGORY_ROTATION_TAXONOMY if item["domain"] == "MINI_APPLIANCE"]
            app_all.sort(key=lambda it: recent_cat_ts.get(it["category_id"], ""))
            fresh_appliance = app_all[: max(2, len(app_all) // 2)]

        return blocked_ids, fresh_cleaning, fresh_appliance

    def _choose_next_domain(self, history: Dict[str, Any], override_domain: Optional[DomainId]) -> DomainId:
        if override_domain in ("CLEANING_CARE", "MINI_APPLIANCE"):
            return override_domain
        runs = history.get("runs", [])
        if not runs:
            return "CLEANING_CARE"
        last_domain = runs[-1].get("domain")
        return "MINI_APPLIANCE" if last_domain == "CLEANING_CARE" else "CLEANING_CARE"

    def step_m0_1_plan_subniche(
        self,
        domain: DomainId,
        history: Dict[str, Any],
        topic_hint: Optional[str] = None,
        max_categories: int = 3,
        ignore_trends: bool = False,
    ) -> LLMSubNichePlanResponse:
        blocked_cat_ids, fresh_cleaning, fresh_appliance = self._get_blocked_and_fresh_categories(history)
        reg_blocks = self.sku_registry.get_blocked_categories_and_skus()

        recent_cats = [
            f"- {r.get('sub_niche_id')} ({r.get('sub_niche_name_vi')}): {r.get('category_ids')}"
            for r in (history.get("runs") or [])[-15:]
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
            else "(Chưa có SKU nào bị khóa gần đây)"
        )

        suggested_fresh_lines = [
            f"  * [{item['domain']}] `{item['category_id']}` — {item['category_name_vi']} ({item['problem_solved_vi']})"
            for item in (fresh_cleaning[:3] + fresh_appliance[:3])
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
            f"- `CLEANING_CARE`: {ROOT_DOMAINS['CLEANING_CARE']} (VD: máy hút bụi giường nệm tia UV, chổi cọ nhà tắm điện không dây, cây lau nhà phun sương/tự vắt, máy giặt thảm sofa cầm tay, máy cắt lông xù quần áo, thùng rác cảm ứng).\n"
            f"- `MINI_APPLIANCE`: {ROOT_DOMAINS['MINI_APPLIANCE']} (VD: bàn ủi hơi nước cầm tay gấp gọn, máy hút ẩm mini chống nồm, máy lọc không khí HEPA phòng ngủ, máy sấy giày khử mùi, quạt tuần hoàn không khí, máy tăm nước cầm tay).\n"
            + (
                "Cụ thể với 3 nhóm (`max_categories = 3`), PHẢI có ít nhất 1 nhóm thuộc `CLEANING_CARE` và ít nhất 1 nhóm thuộc `MINI_APPLIANCE` (2-1 hoặc 1-2).\n"
                if max_categories == 3
                else "Với 2 nhóm, PHẢI có đúng 1 nhóm `CLEANING_CARE` và 1 nhóm `MINI_APPLIANCE`.\n"
            )
            if max_categories >= 2
            else f"Chủ đề gốc cho nhóm duy nhất này: `{domain}` — {ROOT_DOMAINS[domain]}.\n"
        )

        system_instruction = (
            "Bạn là Trưởng bộ phận Nghiên cứu Sản phẩm (Module M0) cho kênh Review Đồ Vệ Sinh Nhà Cửa & Gia Dụng Mini khách quan.\n"
            "NGUYÊN TẮC BẮT BUỘC:\n"
            "1. KHÔNG BAO GIỜ đưa tên thương hiệu (như Deerma, Xiaomi, Philips, Dreame, Lock&Lock, Inochi...) vào `generic_web_queries_vi` hoặc `generic_youtube_queries_vi`.\n"
            "2. Từ khóa tìm kiếm ở Bước M0.1 phải hoàn toàn dựa trên CÔNG NĂNG và VẤN ĐỀ (ví dụ: 'máy hút bụi giường nệm tia UV loại nào tốt', 'chổi cọ nhà tắm điện không dây chống nước', 'bàn ủi hơi nước cầm tay gấp gọn không rỉ nước').\n"
            "3. Mỗi nhóm `category` phải là MỘT LOẠI THIẾT BỊ HOÀN TOÀN KHÁC NHAU và bắt buộc gán chính xác `domain` (`CLEANING_CARE` hoặc `MINI_APPLIANCE`).\n"
            "4. TUYỆT ĐỐI KHÔNG chọn đồ phụ kiện máy tính/văn phòng (chuột, bàn phím, hub USB, arm màn hình) hay dụng cụ nấu bếp thuần túy — chỉ tập trung 100% vào Đồ vệ sinh & Gia dụng mini!"
        )

        prompt = f"""
{domain_balance_rule}
Số lượng nhóm công năng (`categories`) cần lập kế hoạch: {max_categories} nhóm.

QUY TẮC CHỐNG TRÙNG LẶP & CHỐNG CHẺ NHỎ DUNG TÍCH (STRICT ANTI-SIBLING RULE):
1. CẤM chọn 2 nhóm công năng cùng một loại thiết bị chỉ khác dung tích/công suất (VD: TUYỆT ĐỐI CẤM tách máy hút ẩm 1L và 2.5L thành 2 nhóm riêng).
2. CẤM chọn lại các nhóm sản phẩm đã xuất hiện trong `recent_cats` hoặc `recent_skus`.
3. Mỗi `category` phải gán đúng trường `domain` (`"CLEANING_CARE"` hoặc `"MINI_APPLIANCE"`).

Lịch sử các ngách/nhóm đã chạy (`recent_cats` — CẤM LẶP LẠI):
{past_str}

Các SKU đã làm gần đây (`recent_skus` — CẤM LẶP LẠI DÒNG MÁY NÀY):
{recent_skus_str}

{hint_directive}

Hãy xuất JSON theo đúng schema `LLMSubNichePlanResponse` gồm ĐÚNG `{max_categories}` nhóm công năng (`categories`) khác biệt rõ rệt về công dụng.
Mỗi nhóm công năng phải có:
- `domain`: `"CLEANING_CARE"` hoặc `"MINI_APPLIANCE"` (phải có cả 2 giá trị trong batch nếu `max_categories >= 2`).
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
            fresh_pool = fresh_cleaning + fresh_appliance
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
                missing_domain: DomainId = "MINI_APPLIANCE" if "CLEANING_CARE" in present_domains else "CLEANING_CARE"
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
            "Nhiệm vụ của bạn là đọc dữ liệu thu thập từ Web, YouTube và sàn TMĐT của một nhóm công năng Đồ Vệ Sinh / Gia Dụng Mini, sau đó bóc tách ra **4 đến 5 ứng viên (SKU) cạnh tranh đa thương hiệu** để bàn giao cho Module M1 chấm điểm.\n"
            "QUY TẮC CÔNG BẰNG & CHỐNG THIÊN VỊ:\n"
            "1. BẮT BUỘC phải có ít nhất **2 đến 3 thương hiệu có tên tuổi KHÁC NHAU** cạnh tranh trực tiếp trong cùng nhóm công năng (ví dụ với đồ vệ sinh: Deerma vs Xiaomi vs Dreame / Karcher / Lock&Lock / Inochi / HOTO; với gia dụng mini: Philips vs Tefal vs Panasonic / Levoit / Bear / Lumias / Simplus).\n"
            "2. BẮT BUỘC phải có ít nhất **1 ứng viên thuộc nhóm `OEM_GENERIC`** (hàng không thương hiệu / phổ thông giá rẻ trên sàn) để Module M1 làm mốc đối chiếu giá trung vị và chất lượng.\n"
            "3. Nếu trong dữ liệu có tin đăng gộp nhiều mã lập lờ (ví dụ tiêu đề ghi chung nhiều model/dung tích để mồi giá rẻ) hoặc mẫu có lỗi nghiêm trọng, hãy tách riêng hoặc gắn đúng `archetype` / `caveat_severity`.\n"
            "4. Mọi ứng viên ĐỀU PHẢI CÓ `honest_caveat_vi` (nhược điểm thực tế hoặc giới hạn sử dụng thật, không được khen 100%).\n"
            "5. BẮT BUỘC 100% TIẾNG VIỆT CÓ DẤU ĐẦY ĐỦ CHUẨN UTF-8: Kể cả khi tiêu đề trên sàn TMĐT / YouTube viết không dấu, bạn BẮT BUỘC phải chuyển hóa `product_name_vi`, `key_specs_pairs`, `highlight_pros_vi`, `honest_caveat_vi` sang tiếng Việt CÓ DẤU ĐẦY ĐỦ."
        )

        blocked_brands_30d = self.sku_registry.get_recent_brand_categories(within_days=30).get(
            cat_plan.category_id, []
        )
        anti_sibling_brand_rule = (
            f"\n⚠️ CẤM CHỌN BIẾN THỂ CÙNG HÃNG ĐÃ LÊN SÓNG TRONG 30 NGÀY QUA Ở NHÓM `{cat_plan.category_id}`: "
            f"{', '.join(b.upper() for b in blocked_brands_30d)} "
            "(VD đã làm Deerma CM800 thì CẤM chọn Deerma CM1300 / CM1900; hãy chọn các hãng đối thủ khác!).\n"
            if blocked_brands_30d
            else ""
        )

        prompt = f"""
Chủ đề gốc: `{domain}`
Nhóm công năng đang phân tích: `{cat_plan.category_id}` — **{cat_plan.category_name_vi}**
Vấn đề giải quyết: {cat_plan.problem_solved_vi}
{anti_sibling_brand_rule}

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
        choices=["CLEANING_CARE", "MINI_APPLIANCE"],
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
