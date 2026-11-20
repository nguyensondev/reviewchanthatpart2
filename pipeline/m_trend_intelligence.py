"""
Standalone Module M_TREND: Compliance-First External Trend & SKU Opportunity Engine
(`pipeline/m_trend_intelligence.py`).

Black-Box Contract (Zero cross-stage imports — never imports M0..M5):
  - Zero new external dependencies (uses Python 3.11 stdlib `sqlite3`, `xml.etree.ElementTree`,
    `math`, `statistics`, plus existing `httpx`, `pydantic`, `FreeTierGeminiClient`).
  - 100% compatible with macOS Big Sur 11.4 (`x86_64`, MacBook Pro 2015) via `config/cacert.pem`.
  - Implements:
      1. Default-Deny Compliance Gate (`config/trend_sources_registry.json`)
      2. SQLite3 WAL Time-Series Store (`data/trend_intelligence.sqlite3`) + Retention Purge
      3. Pure Python Trend Detector (Rolling Median 28d, Growth Ratio, Robust Z-Score / MAD, EWMA)
      4. 2-Stage Matching: Stage A (Demand Lexicon + TF-IDF) -> Stage B (Gemini Full-Flash Re-ranking)
      5. Exponential Half-Life Time-Decay + TTL + Top-8 Active Window (`data/m_trend_opportunities.json`)
      6. Diagnostics (`status_reason_code`) + 3-Tier Re-Entry Policy against `data/sku_registry.json`
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import email.utils
import fcntl
import hashlib
import html
import json
import math
import os
import random
import re
import shutil
import sqlite3
import statistics
import time
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx

from pipeline.core_resilience import (
    MacOSLegacyRuntime,
    atomic_write_json,
    atomic_write_text,
    safe_load_json,
)
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.schemas_m_trend import (
    FAST_TRACK_MIN_SCORE,
    FLASH_EVENT_HALF_LIFE_HOURS,
    FLASH_EVENT_TTL_HOURS,
    MAX_ACTIVE_OPPORTUNITIES,
    MIN_ACTIVE_OPPORTUNITY_SCORE,
    SEASONAL_SHIFT_HALF_LIFE_HOURS,
    SEASONAL_SHIFT_TTL_HOURS,
    ConfidenceLevel,
    LLMTrendReRankItem,
    LLMTrendReRankResponse,
    MTrendOpportunitiesOutput,
    MatchedSKUCandidate,
    NormalizedTrendSignal,
    OpportunityScoreBreakdown,
    ReEntryAction,
    SourceCompliancePolicy,
    SourceRunAudit,
    TrendDomain,
    TrendHorizon,
    TrendOpportunityItem,
    TrendPriorityTier,
    TrendStatusReasonCode,
    VerificationStatus,
    compute_effective_score_now,
)
from pipeline.shared_utils import (
    AffiliateCSVRepository,
    check_vietnamese_diacritics,
    strip_accents,
)
from pipeline.sku_registry import SKURegistryManager

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data"

DEFAULT_SOURCES_REGISTRY_PATH = CONFIG_DIR / "trend_sources_registry.json"
DEFAULT_TREND_DB_PATH = DATA_DIR / "trend_intelligence.sqlite3"
DEFAULT_TREND_OUTPUT_PATH = DATA_DIR / "m_trend_opportunities.json"
M_TREND_LOCK_PATH = DATA_DIR / ".m_trend.lock"


# ==============================================================================
# 1. CURATED PRODUCT CONCEPT TAXONOMY (24 CONCEPTS: TECH_DESK, KITCHEN_TECH, HOME_LIVING)
# ==============================================================================

PRODUCT_CONCEPT_TAXONOMY: List[Dict[str, Any]] = [
    # --- TECH_DESK ---
    {
        "concept_id": "screen_hanging_light_bar",
        "domain": "TECH_DESK",
        "name_vi": "Đèn treo màn hình máy tính chống chói bảo vệ mắt",
        "sku_anchors": ["den treo man hinh", "den man hinh", "light bar", "iwok"],
        "triggers": [
            "moi mat", "chong choi", "lam viec dem", "hoc dem", "den ban", "den man hinh",
            "anh sang xanh", "can thi", "bao ve mat", "goc lam viec", "setup ban", "man hinh may tinh",
        ],
        "exclusions": ["den giao thong", "den duong", "den o to", "den pha xe"],
    },
    {
        "concept_id": "ergonomic_vertical_mouse",
        "domain": "TECH_DESK",
        "name_vi": "Chuột công thái học dọc chống mỏi cổ tay",
        "sku_anchors": ["chuot cong thai hoc", "chuot doc", "vertical mouse", "ergonomic mouse"],
        "triggers": [
            "dau co tay", "ong co tay", "hoi chung ong co tay", "moi co tay",
            "chuot cong thai hoc", "chuot khong day", "lam viec may tinh", "dau vai gay", "ergonomic",
        ],
        "exclusions": ["chuot bach", "chuot cong", "diet chuot", "benh chuot"],
    },
    {
        "concept_id": "aluminum_laptop_stand_foldable",
        "domain": "TECH_DESK",
        "name_vi": "Giá đỡ laptop hợp kim nhôm tản nhiệt gấp gọn",
        "sku_anchors": ["gia do laptop", "ke laptop", "de tan nhiet laptop", "laptop stand"],
        "triggers": [
            "dau co vai gay", "thoai hoa dot song co", "laptop nong", "tan nhiet laptop",
            "gia do laptop", "tu the ngoi", "ngoi may tinh lau", "laptop van phong",
        ],
        "exclusions": ["gia do dien thoai xe may"],
    },
    {
        "concept_id": "gan_desktop_fast_charger_station",
        "domain": "TECH_DESK",
        "name_vi": "Trạm sạc nhanh GaN đa cổng để bàn an toàn",
        "sku_anchors": ["tram sac", "cu sac gan", "sac nhanh gan", "gan charger"],
        "triggers": [
            "sac nhanh", "cu sac", "sac gan", "chay no sac", "an toan sac pin", "sac nhieu thiet bi",
            "pin dien thoai", "sac laptop", "chay no pin", "no dien thoai khi sac",
        ],
        "exclusions": ["tram sac o to dien", "tram sac vinfast", "sac xe dien"],
    },
    {
        "concept_id": "usb_c_multiport_docking_hub",
        "domain": "TECH_DESK",
        "name_vi": "Hub chuyển đổi USB-C đa năng xuất hình 4K HDMI",
        "sku_anchors": ["hub usb", "hub type c", "bo chuyen doi", "docking"],
        "triggers": [
            "hub usb", "cong ket noi laptop", "macbook thieu cong", "hdmi 4k", "xuat man hinh roi",
            "docking station", "bo chia cong type-c",
        ],
        "exclusions": ["hub logistics", "trung tam trung chuyen"],
    },
    {
        "concept_id": "low_profile_wireless_mechanical_keyboard",
        "domain": "TECH_DESK",
        "name_vi": "Bàn phím cơ không dây Low-Profile gõ êm văn phòng",
        "sku_anchors": ["ban phim co", "ban phim khong day", "mechanical keyboard"],
        "triggers": [
            "ban phim co", "ban phim khong day", "go phim em",
            "setup goc lam viec", "phu kien may tinh", "ban phim bluetooth", "low profile",
        ],
        "exclusions": ["dan piano", "phim dan"],
    },
    {
        "concept_id": "gas_spring_monitor_arm",
        "domain": "TECH_DESK",
        "name_vi": "Tay nâng màn hình máy tính trợ lực khí nén",
        "sku_anchors": ["arm man hinh", "tay nang man hinh", "gia treo man hinh", "monitor arm"],
        "triggers": [
            "man hinh roi", "man hinh may tinh", "gia treo man hinh", "arm man hinh",
            "toi uu ban lam viec", "ban lam viec nho", "hai man hinh",
        ],
        "exclusions": ["man hinh tivi", "man hinh led san khau"],
    },
    {
        "concept_id": "magnetic_retractable_fast_charging_cable",
        "domain": "TECH_DESK",
        "name_vi": "Cáp sạc nhanh tự cuộn chống rối độ bền cao",
        "sku_anchors": ["cap sac", "day sac", "charging cable"],
        "triggers": [
            "day sac nhanh", "cap sac nhanh", "cap usb-c", "dut cap sac", "sac nhanh 100w", "cap tu cuon",
        ],
        "exclusions": ["cap quang bien", "cap dien cao the", "cap treo"],
    },
    {
        "concept_id": "cordless_compressed_air_desk_duster",
        "domain": "TECH_DESK",
        "name_vi": "Máy thổi bụi vệ sinh bàn phím và góc máy tính cầm tay",
        "sku_anchors": ["may thoi bui", "hut bui ban phim", "air duster"],
        "triggers": [
            "ve sinh ban phim", "ve sinh laptop", "bui ban phim", "may tinh bui", "quat tan nhiet",
            "may thoi bui", "ve sinh pc",
        ],
        "exclusions": ["bui duong pho", "cong truong"],
    },
    {
        "concept_id": "smart_power_strip_surge_protector",
        "domain": "TECH_DESK",
        "name_vi": "Ổ cắm điện chống sét quá tải tích hợp cổng sạc USB-C",
        "sku_anchors": ["o cam dien", "power strip", "chong set"],
        "triggers": [
            "chay no dien", "chap dien", "qua tai dien", "o cam dien", "an toan dien",
            "phong chay chua chay", "chong set", "chap chay",
        ],
        "exclusions": ["nha may dien", "thuy dien", "dien gio"],
    },
    # --- KITCHEN_TECH ---
    {
        "concept_id": "digital_kitchen_food_scale",
        "domain": "KITCHEN_TECH",
        "name_vi": "Cân điện tử nhà bếp độ chính xác cao chống nước",
        "sku_anchors": ["can dien tu", "can nha bep", "can tieu ly", "kitchen scale"],
        "triggers": [
            "eat clean", "tinh calo", "lam banh tai nha", "pha ca phe pour over",
            "can dien tu nha bep", "thuc don giam can", "che do an kieng",
        ],
        "exclusions": ["can nang tre em", "can xe tai", "can vang"],
    },
    {
        "concept_id": "instant_read_digital_food_thermometer",
        "domain": "KITCHEN_TECH",
        "name_vi": "Nhiệt kế nấu ăn điện tử đầu dò inox đo nhanh",
        "sku_anchors": ["nhiet ke nau an", "nhiet ke thuc pham", "food thermometer"],
        "triggers": [
            "nhiet do thit", "nuong thit", "beefsteak", "nhiet do dau",
            "nhiet ke nau an", "noi chien khong dau",
        ],
        "exclusions": ["nhiet ke do sot", "sot xuat huyet", "nhiet do ngoai troi"],
    },
    {
        "concept_id": "compact_vacuum_food_sealer",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy hút chân không thực phẩm mini gia đình",
        "sku_anchors": ["hut chan khong", "han mieng tui", "vacuum sealer"],
        "triggers": [
            "bao quan thuc pham", "tich tru do an", "thuc pham tuoi lau",
            "hut chan khong thuc pham", "ngo doc thuc pham", "do kho moc", "meal prep", "thit ca dong lanh",
        ],
        "exclusions": ["hut ham cau", "hut bun", "tin tac", "lo hong", "kho mau"],
    },
    {
        "concept_id": "glass_mist_oil_sprayer_bottle",
        "domain": "KITCHEN_TECH",
        "name_vi": "Bình xịt dầu ăn phun sương thủy tinh cho nồi chiên",
        "sku_anchors": ["binh xit dau", "oil sprayer"],
        "triggers": [
            "noi chien khong dau", "giam dau mo", "binh xit dau an", "do chien ran", "nau an it dau",
        ],
        "exclusions": ["dau tho", "gia xang dau", "dau nhot", "tieu duong thai ky"],
    },
    {
        "concept_id": "pull_cord_garlic_chopper",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy xay tỏi ớt gia vị mini cầm tay tiện lợi",
        "sku_anchors": ["xay toi", "bam nho thuc pham", "xay thuc pham mini", "garlic chopper"],
        "triggers": [
            "xay toi ot", "so che thuc pham", "nau an nhanh", "do bep thong minh", "an dam cho be",
            "may xay mini", "sinh vien nau an",
        ],
        "exclusions": ["may xay xat lua", "may xay xi mang"],
    },
    {
        "concept_id": "handheld_rechargeable_milk_frother",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy đánh bọt cà phê / matcha cầm tay sạc USB",
        "sku_anchors": ["danh bot", "tao bot sua", "milk frother"],
        "triggers": [
            "ca phe tai nha", "pha matcha", "matcha latte", "ca phe muoi", "danh bot sua",
            "tra sua tai nha", "tiet kiem tien ca phe",
        ],
        "exclusions": ["xuat khau ca phe", "gia ca phe the gioi"],
    },
    {
        "concept_id": "temperature_control_gooseneck_kettle",
        "domain": "KITCHEN_TECH",
        "name_vi": "Ấm siêu tốc chỉnh nhiệt độ giữ ấm cổ ngỗng",
        "sku_anchors": ["am sieu toc", "am dun nuoc", "gooseneck kettle"],
        "triggers": [
            "pha tra", "pha sua em be", "am sieu toc", "am dun nuoc",
            "khong khi lanh", "troi ret", "uong nuoc am",
        ],
        "exclusions": [],
    },
    {
        "concept_id": "electric_ceramic_burr_coffee_grinder",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy xay cà phê hạt mini lưỡi gốm dùng pin",
        "sku_anchors": ["xay ca phe", "coffee grinder"],
        "triggers": [
            "ca phe hat", "xay ca phe", "ca phe dac san", "ca phe pha phin", "pha espresso",
            "pha ca phe van phong",
        ],
        "exclusions": ["gia ca phe", "san luong ca phe", "khu du lich", "du an"],
    },
    {
        "concept_id": "magnetic_rotary_kitchen_timer",
        "domain": "KITCHEN_TECH",
        "name_vi": "Đồng hồ bấm giờ nấu ăn & học tập Pomodoro nam châm",
        "sku_anchors": ["dong ho bam gio", "hen gio nau an", "kitchen timer"],
        "triggers": [
            "pomodoro", "tap trung hoc", "quan ly thoi gian", "hen gio nau an", "on thi",
            "dong ho bam gio",
        ],
        "exclusions": ["dong ho thuy si", "dong ho deo tay"],
    },
    {
        "concept_id": "automatic_touchless_dish_soap_dispenser",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy tạo bọt xà phòng rửa tay / rửa bát cảm ứng tự động",
        "sku_anchors": ["tao bot xa phong", "rua tay cam ung", "soap dispenser"],
        "triggers": [
            "rua tay", "cum mua", "tay chan mieng", "ve sinh nha bep",
            "xa phong tao bot", "phong dich benh",
        ],
        "exclusions": [],
    },
    # --- HIGH-VELOCITY SEASONAL / WEATHER VIETNAM CONCEPTS (Mapped cleanly to KITCHEN_TECH / TECH_DESK) ---
    {
        "concept_id": "mini_clothes_dryer_dehumidifier",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy hút ẩm & máy sấy quần áo mini chống nồm mốc",
        "sku_anchors": ["may hut am", "may say quan ao", "dehumidifier"],
        "triggers": [
            "nom am", "mua keo dai", "quan ao kho kho", "am moc", "do am cao",
            "mua bao", "mua lon", "ngap lut", "khong khi lanh", "may hut am", "may say quan ao",
            "mui hoi quan ao", "mua phun", "nha do mo hoi",
        ],
        "exclusions": ["say nong san cong nghiep"],
    },
    {
        "concept_id": "cordless_rechargeable_fan_powerbank",
        "domain": "TECH_DESK",
        "name_vi": "Quạt tích điện mini & đèn dự phòng khi mất điện nắng nóng",
        "sku_anchors": ["quat tich dien", "quat mini", "rechargeable fan"],
        "triggers": [
            "nang nong", "oi buc", "cat dien", "mat dien", "quat tich dien",
            "quat mini cam tay", "chong nong",
        ],
        "exclusions": ["quat cong nghiep", "dien gio"],
    },
    {
        "concept_id": "hepa_air_purifier_mini",
        "domain": "TECH_DESK",
        "name_vi": "Máy lọc không khí mini màng lọc HEPA khử bụi mịn",
        "sku_anchors": ["may loc khong khi", "mang loc hepa", "air purifier"],
        "triggers": [
            "o nhiem khong khi", "bui min", "pm2.5", "aqi", "khoi bui", "viem mui di ung",
            "may loc khong khi", "suong mu o nhiem", "chat luong khong khi",
        ],
        "exclusions": ["loc dau xe"],
    },
    {
        "concept_id": "mini_cordless_handheld_vacuum",
        "domain": "KITCHEN_TECH",
        "name_vi": "Máy hút bụi cầm tay không dây mini cho sofa, giường & ô tô",
        "sku_anchors": ["may hut bui", "hut bui cam tay", "handheld vacuum"],
        "triggers": [
            "hut bui cam tay", "long thu cung", "ve sinh o to", "mat bui giuong",
            "di ung bui", "robot hut bui", "noi that o to",
        ],
        "exclusions": [],
    },
]


# ==============================================================================
# 2. COMPLIANCE POLICY GATE (DEFAULT-DENY, FAIL-CLOSED)
# ==============================================================================

class CompliancePolicyGate:
    """Enforces Section 1 & Section 10.3: Default-deny, fail-closed on 401/403/expiry."""

    def __init__(self, registry_path: Path = DEFAULT_SOURCES_REGISTRY_PATH) -> None:
        self.registry_path = registry_path
        self._raw = safe_load_json(registry_path, lambda: {"sources": {}})
        if not isinstance(self._raw, dict):
            self._raw = {"sources": {}}

    def get_all_policies(self) -> Dict[str, SourceCompliancePolicy]:
        out: Dict[str, SourceCompliancePolicy] = {}
        raw_sources = self._raw.get("sources") or {}
        if isinstance(raw_sources, dict):
            for sid, cfg in raw_sources.items():
                if isinstance(cfg, dict):
                    try:
                        out[sid] = SourceCompliancePolicy.model_validate(cfg)
                    except Exception:
                        pass
        return out

    def can_run(
        self,
        policy: SourceCompliancePolicy,
        now: Optional[dt.datetime] = None,
        requests_used_today: int = 0,
    ) -> Tuple[bool, str]:
        now_dt = now or dt.datetime.now(dt.timezone.utc)
        if now_dt.tzinfo is None:
            now_dt = now_dt.replace(tzinfo=dt.timezone.utc)
        if not policy.enabled:
            return False, "SKIPPED_DISABLED"
        if policy.authorization_status != "approved":
            return False, f"BLOCKED_AUTH_{policy.authorization_status.upper()}"
        if not policy.commercial_use_permitted:
            return False, "BLOCKED_NON_COMMERCIAL"
        if policy.expires_at:
            try:
                exp = dt.datetime.fromisoformat(policy.expires_at.replace("Z", "+00:00"))
                if exp.tzinfo is None:
                    exp = exp.replace(tzinfo=dt.timezone.utc)
                if now_dt >= exp:
                    return False, "BLOCKED_EXPIRED"
            except Exception:
                return False, "BLOCKED_INVALID_EXPIRY"
        if policy.max_requests_per_day > 0 and requests_used_today >= policy.max_requests_per_day:
            return False, "BLOCKED_DAILY_QUOTA"
        return True, "ALLOWED"

    def suspend_source(self, source_id: str, reason: str) -> None:
        """Fail-closed: immediately mark a source suspended on HTTP 401/403."""
        sources = self._raw.setdefault("sources", {})
        if isinstance(sources, dict) and isinstance(sources.get(source_id), dict):
            sources[source_id]["enabled"] = False
            sources[source_id]["authorization_status"] = "suspended"
            sources[source_id]["notes"] = f"[AUTO-SUSPENDED] {reason}"
            atomic_write_json(self.registry_path, self._raw)


# ==============================================================================
# 3. SQLITE3 TIME-SERIES STORE (STDLIB ONLY — 0 MB INSTALL ON MAC 2015)
# ==============================================================================

class TrendSQLiteStore:
    """
    Lightweight WAL-mode SQLite3 database with:
      - Self-Healing `PRAGMA quick_check` on startup (auto-quarantine corrupted DB + restore from `.bak` or recreate)
      - HTTP Conditional Cache (`ETag` / `Last-Modified` -> `304 Not Modified`)
      - Stage B Gemini Cache (`candidates_hash`, TTL 6h)
      - 24h Alert Dedup (max 3 alerts/day) & 48h Fast-Track Anti-Starvation Cap
    """

    def __init__(self, db_path: Path = DEFAULT_TREND_DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._verify_or_heal_db()
        self._init_schema()

    def _verify_or_heal_db(self) -> None:
        if not self.db_path.exists():
            return
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=10.0)
            row = conn.execute("PRAGMA quick_check;").fetchone()
            conn.close()
            if row and str(row[0]).lower() == "ok":
                return
            raise sqlite3.DatabaseError(f"quick_check failed: {row}")
        except Exception as exc:
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            corrupt_path = self.db_path.with_suffix(f".corrupt.{ts}.sqlite3")
            print(f"[M_TREND SELF-HEAL] ⚠️ SQLite DB lỗi ({exc}) -> cách ly sang {corrupt_path.name}", flush=True)
            try:
                self.db_path.replace(corrupt_path)
            except OSError:
                self.db_path.unlink(missing_ok=True)
            bak_path = self.db_path.with_suffix(".sqlite3.bak")
            if bak_path.exists():
                try:
                    shutil.copy2(bak_path, self.db_path)
                    print(f"[M_TREND SELF-HEAL] ✅ Đã tự phục hồi SQLite DB từ bản sao lưu {bak_path.name}!", flush=True)
                except OSError:
                    pass

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_events (
                    event_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    source_provider TEXT NOT NULL,
                    source_object_id TEXT,
                    canonical_event_id TEXT NOT NULL,
                    event_time_utc TEXT NOT NULL,
                    collected_at_utc TEXT NOT NULL,
                    title TEXT NOT NULL,
                    summary TEXT,
                    reference_url TEXT,
                    region TEXT DEFAULT 'VN',
                    payload_hash TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_raw_events_time ON raw_events(event_time_utc);
                CREATE INDEX IF NOT EXISTS idx_raw_events_canonical ON raw_events(canonical_event_id);

                CREATE TABLE IF NOT EXISTS concept_daily_observations (
                    concept_id TEXT NOT NULL,
                    obs_date TEXT NOT NULL,
                    mention_count REAL NOT NULL DEFAULT 0.0,
                    independent_sources INTEGER NOT NULL DEFAULT 0,
                    updated_at_utc TEXT NOT NULL,
                    PRIMARY KEY (concept_id, obs_date)
                );

                CREATE TABLE IF NOT EXISTS connector_runs (
                    run_id TEXT PRIMARY KEY,
                    source_id TEXT NOT NULL,
                    ran_at_utc TEXT NOT NULL,
                    status TEXT NOT NULL,
                    fetched_count INTEGER DEFAULT 0,
                    new_inserted_count INTEGER DEFAULT 0,
                    http_status INTEGER,
                    note TEXT
                );

                CREATE TABLE IF NOT EXISTS source_http_cache (
                    source_id TEXT PRIMARY KEY,
                    etag TEXT,
                    last_modified TEXT,
                    updated_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS llm_stage_b_cache (
                    candidates_hash TEXT PRIMARY KEY,
                    response_json TEXT NOT NULL,
                    model_used TEXT,
                    cached_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS alert_history (
                    alert_key TEXT NOT NULL,
                    sent_at_utc TEXT NOT NULL,
                    alert_date TEXT NOT NULL,
                    PRIMARY KEY (alert_key, alert_date)
                );

                CREATE TABLE IF NOT EXISTS fast_track_history (
                    opportunity_id TEXT PRIMARY KEY,
                    concept_id TEXT NOT NULL,
                    triggered_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS meta_kv (
                    k TEXT PRIMARY KEY,
                    v TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS zero_match_streak (
                    obs_date TEXT PRIMARY KEY,
                    raw_events_24h INTEGER NOT NULL,
                    matched_opportunities INTEGER NOT NULL,
                    recorded_at_utc TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS used_event_ids (
                    canonical_event_id TEXT PRIMARY KEY,
                    script_id TEXT,
                    concept_id TEXT,
                    recorded_at_utc TEXT NOT NULL
                );
                """
            )

    def get_meta(self, key: str, default: str = "") -> str:
        with self._connect() as conn:
            row = conn.execute("SELECT v FROM meta_kv WHERE k = ?", (key,)).fetchone()
            return str(row["v"]) if row else default

    def set_meta(self, key: str, val: str) -> None:
        with self._connect() as conn:
            conn.execute("INSERT OR REPLACE INTO meta_kv (k, v) VALUES (?, ?)", (key, str(val)))

    def get_http_cache(self, source_id: str) -> Tuple[str, str]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT etag, last_modified FROM source_http_cache WHERE source_id = ?",
                (source_id,),
            ).fetchone()
            if not row:
                return "", ""
            return str(row["etag"] or ""), str(row["last_modified"] or "")

    def set_http_cache(self, source_id: str, etag: str, last_modified: str, now_utc_iso: str) -> None:
        if not etag and not last_modified:
            return
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO source_http_cache (source_id, etag, last_modified, updated_at_utc) VALUES (?, ?, ?, ?)",
                (source_id, etag, last_modified, now_utc_iso),
            )

    def get_llm_stage_b_cache(self, candidates_hash: str, ttl_hours: float = 6.0) -> Optional[str]:
        cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=ttl_hours)).isoformat(timespec="seconds")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT response_json FROM llm_stage_b_cache WHERE candidates_hash = ? AND cached_at_utc >= ?",
                (candidates_hash, cutoff),
            ).fetchone()
            return str(row["response_json"]) if row else None

    def set_llm_stage_b_cache(
        self, candidates_hash: str, response_json: str, model_used: str, now_utc_iso: str
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO llm_stage_b_cache (candidates_hash, response_json, model_used, cached_at_utc) VALUES (?, ?, ?, ?)",
                (candidates_hash, response_json, model_used, now_utc_iso),
            )

    def can_send_telegram_alert(self, alert_key: str, max_per_day: int = 3) -> bool:
        now_utc = dt.datetime.now(dt.timezone.utc)
        today_str = now_utc.strftime("%Y-%m-%d")
        cutoff_24h = (now_utc - dt.timedelta(hours=24)).isoformat(timespec="seconds")
        with self._connect() as conn:
            dup = conn.execute(
                "SELECT 1 FROM alert_history WHERE alert_key = ? AND sent_at_utc >= ?",
                (alert_key, cutoff_24h),
            ).fetchone()
            if dup is not None:
                return False
            cnt_row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM alert_history WHERE alert_date = ?",
                (today_str,),
            ).fetchone()
            return (int(cnt_row["cnt"]) if cnt_row else 0) < max_per_day

    def record_telegram_alert(self, alert_key: str) -> None:
        now_utc = dt.datetime.now(dt.timezone.utc)
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO alert_history (alert_key, sent_at_utc, alert_date) VALUES (?, ?, ?)",
                (alert_key, now_utc.isoformat(timespec="seconds"), now_utc.strftime("%Y-%m-%d")),
            )

    def is_fast_track_48h_cap_available(self, now: Optional[dt.datetime] = None) -> bool:
        now_dt = now or dt.datetime.now(dt.timezone.utc)
        cutoff = (now_dt - dt.timedelta(hours=48)).isoformat(timespec="seconds")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM fast_track_history WHERE triggered_at_utc >= ?",
                (cutoff,),
            ).fetchone()
            return (int(row["cnt"]) if row else 0) < 1

    def record_fast_track_claimed(self, opportunity_id: str, concept_id: str) -> None:
        now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO fast_track_history (opportunity_id, concept_id, triggered_at_utc) VALUES (?, ?, ?)",
                (opportunity_id, concept_id, now_iso),
            )

    def record_used_event_id(self, canonical_event_id: str, concept_id: str, script_id: str = "") -> None:
        now_iso = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")
        with self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO used_event_ids (canonical_event_id, script_id, concept_id, recorded_at_utc) VALUES (?, ?, ?, ?)",
                (canonical_event_id, script_id, concept_id, now_iso),
            )

    def maybe_backup_and_vacuum(self) -> None:
        """Create daily `.bak` snapshot and weekly lightweight VACUUM to keep SQLite < 5-10 MB."""
        today_str = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
        if self.get_meta("last_backup_date") != today_str and self.db_path.exists():
            try:
                bak_path = self.db_path.with_suffix(".sqlite3.bak")
                with self._connect() as src_conn:
                    dst_conn = sqlite3.connect(str(bak_path))
                    try:
                        src_conn.backup(dst_conn)
                    finally:
                        dst_conn.close()
                self.set_meta("last_backup_date", today_str)
            except Exception:
                pass
        last_vac = self.get_meta("last_vacuum_date")
        need_vac = not last_vac
        if last_vac:
            try:
                d_old = dt.date.fromisoformat(last_vac)
                need_vac = (dt.date.today() - d_old).days >= 7
            except Exception:
                need_vac = True
        if need_vac:
            try:
                with self._connect() as conn:
                    conn.execute("VACUUM;")
                self.set_meta("last_vacuum_date", today_str)
            except Exception:
                pass

    def insert_signals_idempotent(self, signals: List[NormalizedTrendSignal]) -> int:
        inserted = 0
        with self._connect() as conn:
            for sig in signals:
                p_hash = hashlib.sha256(
                    f"{sig.source_id}|{sig.source_object_id}|{sig.title}".encode("utf-8")
                ).hexdigest()[:24]
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO raw_events (
                        event_id, source_id, source_provider, source_object_id,
                        canonical_event_id, event_time_utc, collected_at_utc,
                        title, summary, reference_url, region, payload_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        sig.event_id,
                        sig.source_id,
                        sig.source_provider,
                        sig.source_object_id,
                        sig.canonical_event_id,
                        sig.event_time_utc,
                        sig.collected_at_utc,
                        sig.title,
                        sig.summary,
                        sig.reference_url,
                        sig.region,
                        p_hash,
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1
        return inserted

    def get_requests_today(self, source_id: str, today_utc_prefix: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS cnt FROM connector_runs WHERE source_id = ? AND ran_at_utc LIKE ?",
                (source_id, f"{today_utc_prefix}%"),
            ).fetchone()
            return int(row["cnt"]) if row else 0

    def log_connector_run(self, run_id: str, audit: SourceRunAudit, ran_at_utc: str) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO connector_runs (
                    run_id, source_id, ran_at_utc, status, fetched_count, new_inserted_count, http_status, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"{run_id}_{audit.source_id}",
                    audit.source_id,
                    ran_at_utc,
                    audit.status,
                    audit.fetched_count,
                    audit.new_inserted_count,
                    audit.http_status,
                    audit.note[:300],
                ),
            )

    def fetch_recent_events(self, since_utc_iso: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM raw_events
                WHERE event_time_utc >= ?
                ORDER BY event_time_utc DESC
                """,
                (since_utc_iso,),
            ).fetchall()
            return [dict(r) for r in rows]

    def upsert_daily_observation(
        self,
        concept_id: str,
        obs_date: str,
        mention_count: float,
        independent_sources: int,
        now_utc_iso: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO concept_daily_observations (
                    concept_id, obs_date, mention_count, independent_sources, updated_at_utc
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(concept_id, obs_date) DO UPDATE SET
                    mention_count = MAX(mention_count, excluded.mention_count),
                    independent_sources = MAX(independent_sources, excluded.independent_sources),
                    updated_at_utc = excluded.updated_at_utc
                """,
                (concept_id, obs_date, float(mention_count), int(independent_sources), now_utc_iso),
            )

    def get_concept_history_series(self, concept_id: str, limit_days: int = 28) -> List[float]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT mention_count FROM concept_daily_observations
                WHERE concept_id = ?
                ORDER BY obs_date ASC
                LIMIT ?
                """,
                (concept_id, limit_days),
            ).fetchall()
            return [float(r["mention_count"]) for r in rows]

    def record_zero_match_day(
        self, obs_date: str, raw_events_24h: int, matched_opportunities: int, now_utc_iso: str
    ) -> int:
        """Record today's match count and return consecutive days with >=20 raw events but 0 matches."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO zero_match_streak (
                    obs_date, raw_events_24h, matched_opportunities, recorded_at_utc
                ) VALUES (?, ?, ?, ?)
                """,
                (obs_date, int(raw_events_24h), int(matched_opportunities), now_utc_iso),
            )
            rows = conn.execute(
                "SELECT raw_events_24h, matched_opportunities FROM zero_match_streak ORDER BY obs_date DESC LIMIT 5"
            ).fetchall()
            streak = 0
            for r in rows:
                if int(r["raw_events_24h"]) >= 20 and int(r["matched_opportunities"]) == 0:
                    streak += 1
                else:
                    break
            return streak

    def is_event_already_used(self, canonical_event_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM used_event_ids WHERE canonical_event_id = ?",
                (canonical_event_id,),
            ).fetchone()
            return row is not None

    def purge_expired_records(self, retention_days: int = 60) -> int:
        cutoff = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=retention_days)).isoformat(timespec="seconds")
        cutoff_date = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=retention_days)).strftime("%Y-%m-%d")
        with self._connect() as conn:
            c1 = conn.execute("DELETE FROM raw_events WHERE collected_at_utc < ?", (cutoff,)).rowcount
            c2 = conn.execute("DELETE FROM concept_daily_observations WHERE obs_date < ?", (cutoff_date,)).rowcount
            c3 = conn.execute("DELETE FROM connector_runs WHERE ran_at_utc < ?", (cutoff,)).rowcount
            return max(0, c1) + max(0, c2) + max(0, c3)


# ==============================================================================
# 4. PURE PYTHON TREND DETECTOR (ROLLING MEDIAN 28D, GROWTH RATIO, MAD Z-SCORE, EWMA)
# ==============================================================================

class PurePythonTrendDetector:
    """Zero-SciPy statistical anomaly detector (< 5ms CPU on MacBook Pro 2015)."""

    @staticmethod
    def evaluate_series(history: List[float], current_value: float) -> Dict[str, Any]:
        clean_hist = [float(v) for v in history if v >= 0.0]
        if len(clean_hist) < 7:
            # Cold-start conservative scoring (never fake a 28-day baseline!)
            momentum = min(82.0, round(38.0 + math.log1p(max(0.0, current_value)) * 22.0, 1))
            return {
                "cold_start": True,
                "baseline_label": "insufficient_history",
                "rolling_median": round(statistics.median(clean_hist), 2) if clean_hist else 0.0,
                "growth_ratio": round((current_value + 1.0) / ((statistics.median(clean_hist) if clean_hist else 0.5) + 1.0), 2),
                "robust_z_score": 0.0,
                "ewma": round(current_value, 2),
                "momentum_100": momentum,
            }

        med = statistics.median(clean_hist)
        abs_devs = [abs(x - med) for x in clean_hist]
        mad = statistics.median(abs_devs)
        if mad < 1e-6:
            mad = max(0.5, 0.25 * (med + 1.0))

        robust_z = 0.6745 * (current_value - med) / mad
        growth_ratio = (current_value + 1.0) / (med + 1.0)

        alpha = 0.35
        ewma = clean_hist[0]
        for val in clean_hist[1:]:
            ewma = alpha * val + (1.0 - alpha) * ewma
        ewma_now = alpha * current_value + (1.0 - alpha) * ewma

        # Map robust_z + growth_ratio cleanly into 0..100
        z_component = max(0.0, min(60.0, 25.0 + robust_z * 14.0))
        g_component = max(0.0, min(40.0, math.log1p(max(0.0, growth_ratio - 1.0)) * 28.0))
        momentum = round(min(100.0, z_component + g_component), 1)
        return {
            "cold_start": False,
            "baseline_label": f"rolling_{len(clean_hist)}d_median",
            "rolling_median": round(med, 2),
            "growth_ratio": round(growth_ratio, 2),
            "robust_z_score": round(robust_z, 2),
            "ewma": round(ewma_now, 2),
            "momentum_100": momentum,
        }


# ==============================================================================
# 5. EXTERNAL SIGNAL COLLECTORS (RSS ALLOWLIST + GDELT DOC 2.0)
# ==============================================================================

def _clean_html_text(raw: str) -> str:
    txt = html.unescape(raw or "")
    txt = re.sub(r"<[^>]+>", " ", txt)
    txt = re.sub(r"\s+", " ", txt).strip()
    return txt


def _parse_pubdate_to_utc_iso(raw_date: str, fallback_utc_iso: str) -> str:
    s = (raw_date or "").strip()
    if not s:
        return fallback_utc_iso
    try:
        parsed = email.utils.parsedate_to_datetime(s)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc).isoformat(timespec="seconds")
    except Exception:
        pass
    try:
        # GDELT format: YYYYMMDDTHHMMSSZ or ISO 8601
        if re.fullmatch(r"\d{8}T\d{6}Z", s):
            parsed = dt.datetime.strptime(s, "%Y%m%dT%H%M%SZ").replace(tzinfo=dt.timezone.utc)
            return parsed.isoformat(timespec="seconds")
        parsed = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc).isoformat(timespec="seconds")
    except Exception:
        return fallback_utc_iso


def _compute_canonical_event_id(title: str) -> str:
    """
    Canonicalize headline tokens so republished wire stories across 3 newspapers
    collapse into 1 `canonical_event_id` (preventing false cross-source inflation).
    """
    norm = strip_accents(title or "")
    tokens = [
        t for t in re.findall(r"[a-z0-9]+", norm)
        if len(t) >= 3 and t not in {"cho", "cua", "voi", "nhung", "cac", "trong", "khi", "sau", "tu", "den", "tai", "vao", "nay", "viet", "nam"}
    ]
    core = " ".join(sorted(set(tokens))[:8]) or norm[:48]
    return "ev_" + hashlib.sha256(core.encode("utf-8")).hexdigest()[:14]


def _extract_publisher_family(source_id: str) -> str:
    """Group multiple RSS sections from the same news organization as 1 independent publisher family."""
    low = (source_id or "").lower()
    for fam in ("vnexpress", "tuoitre", "thanhnien", "dantri", "tinhte", "gdelt", "youtube", "shopee"):
        if fam in low:
            return fam
    return low


class StandaloneMTrendEngine:
    """
    End-to-end Standalone M_TREND Engine:
      Compliance Gate -> Collectors (RSS + GDELT) -> SQLite3 -> Pure Python Detector
      -> Stage A (Demand Lexicon + TF-IDF) -> Stage B (Gemini Full-Flash Re-ranking)
      -> Owned Shopee CSV Verification -> Half-Life Decay + Top-8 + 3-Tier Re-Entry
      -> Atomic JSON Contract (`data/m_trend_opportunities.json`) + Telegram Preview
    """

    def __init__(
        self,
        registry_path: Path = DEFAULT_SOURCES_REGISTRY_PATH,
        db_path: Path = DEFAULT_TREND_DB_PATH,
        output_path: Path = DEFAULT_TREND_OUTPUT_PATH,
    ) -> None:
        self.gate = CompliancePolicyGate(registry_path=registry_path)
        self.store = TrendSQLiteStore(db_path=db_path)
        self.output_path = output_path
        self.csv_repo = AffiliateCSVRepository()
        self.sku_registry = SKURegistryManager()
        self.ca_bundle = MacOSLegacyRuntime.resolve_ca_bundle_path()
        self._last_domain_hit_ts: Dict[str, float] = {}

    def _polite_domain_wait(self, url: str) -> None:
        """Per-domain politeness jitter (0.6s - 1.4s) so multiple RSS feeds on the same publisher never burst."""
        try:
            host = (urllib.parse.urlparse(url).hostname or "").lower()
        except Exception:
            host = ""
        if not host:
            return
        last_ts = self._last_domain_hit_ts.get(host)
        if last_ts is not None:
            min_gap = random.uniform(0.6, 1.4)
            elapsed = time.monotonic() - last_ts
            if elapsed < min_gap:
                time.sleep(min_gap - elapsed)
        self._last_domain_hit_ts[host] = time.monotonic()

    def should_run_auto_tick(self, min_interval_hours: float = 6.0) -> bool:
        """Return True only if at least `min_interval_hours` (6h default) elapsed since last auto run."""
        last_iso = self.store.get_meta("last_run_utc")
        if not last_iso or not self.output_path.exists():
            return True
        try:
            last_dt = dt.datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=dt.timezone.utc)
            elapsed_h = (dt.datetime.now(dt.timezone.utc) - last_dt).total_seconds() / 3600.0
            return elapsed_h >= float(min_interval_hours)
        except Exception:
            return True

    def should_refresh_for_telegram(self, min_interval_minutes: float = 15.0) -> bool:
        """Return True if last run is older than `min_interval_minutes` (15m default) or JSON missing."""
        last_iso = self.store.get_meta("last_run_utc")
        if not last_iso or not self.output_path.exists():
            return True
        try:
            last_dt = dt.datetime.fromisoformat(last_iso.replace("Z", "+00:00"))
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=dt.timezone.utc)
            elapsed_m = (dt.datetime.now(dt.timezone.utc) - last_dt).total_seconds() / 60.0
            return elapsed_m >= float(min_interval_minutes)
        except Exception:
            return True

    def _http_client(self, timeout_sec: float = 12.0) -> httpx.Client:
        verify_arg: Any = self.ca_bundle if self.ca_bundle else True
        return httpx.Client(
            timeout=timeout_sec,
            verify=verify_arg,
            follow_redirects=True,
            headers={
                "User-Agent": "ReviewChanThat-TrendIntelligence/1.1 (+Compliance-First RSS/GDELT Reader)"
            },
        )

    def _fetch_rss_source(
        self, policy: SourceCompliancePolicy, now_utc_iso: str
    ) -> Tuple[List[NormalizedTrendSignal], SourceRunAudit]:
        if not policy.feed_url:
            return [], SourceRunAudit(
                source_id=policy.source_id,
                provider=policy.provider,
                status="BLOCKED_COMPLIANCE",
                note="Missing feed_url",
            )
        self._polite_domain_wait(policy.feed_url)
        etag, last_mod = self.store.get_http_cache(policy.source_id)
        cond_headers: Dict[str, str] = {}
        if etag:
            cond_headers["If-None-Match"] = etag
        if last_mod:
            cond_headers["If-Modified-Since"] = last_mod

        try:
            with self._http_client(timeout_sec=12.0) as client:
                resp = client.get(policy.feed_url, headers=cond_headers)
            if resp.status_code == 304:
                return [], SourceRunAudit(
                    source_id=policy.source_id,
                    provider=policy.provider,
                    status="NOT_MODIFIED_304",
                    fetched_count=0,
                    new_inserted_count=0,
                    http_status=304,
                    note="HTTP 304 Not Modified (0 bytes transferred)",
                )
            if resp.status_code in (401, 403):
                self.gate.suspend_source(policy.source_id, f"HTTP {resp.status_code}")
                return [], SourceRunAudit(
                    source_id=policy.source_id,
                    provider=policy.provider,
                    status="SUSPENDED_401_403",
                    http_status=resp.status_code,
                    note=f"Fail-closed: auto-suspended on HTTP {resp.status_code}",
                )
            if resp.status_code == 429:
                ra = resp.headers.get("Retry-After", "")
                return [], SourceRunAudit(
                    source_id=policy.source_id,
                    provider=policy.provider,
                    status="ERROR_NETWORK",
                    http_status=429,
                    note=f"Rate limited HTTP 429 (Retry-After={ra or 'N/A'}); skipped cleanly",
                )
            resp.raise_for_status()
            self.store.set_http_cache(
                policy.source_id,
                resp.headers.get("ETag", ""),
                resp.headers.get("Last-Modified", ""),
                now_utc_iso,
            )
            root = ET.fromstring(resp.content)
        except Exception as exc:
            return [], SourceRunAudit(
                source_id=policy.source_id,
                provider=policy.provider,
                status="ERROR_NETWORK",
                note=str(exc)[:180],
            )

        signals: List[NormalizedTrendSignal] = []
        items = root.findall(".//item")
        if not items:
            # Atom fallback
            items = root.findall(".//{http://www.w3.org/2005/Atom}entry")

        for item in items[:60]:
            title_el = item.find("title")
            if title_el is None:
                title_el = item.find("{http://www.w3.org/2005/Atom}title")
            link_el = item.find("link")
            if link_el is None:
                link_el = item.find("{http://www.w3.org/2005/Atom}link")
            desc_el = item.find("description")
            if desc_el is None:
                desc_el = item.find("{http://www.w3.org/2005/Atom}summary")
            pub_el = item.find("pubDate")
            if pub_el is None:
                pub_el = item.find("{http://www.w3.org/2005/Atom}updated")

            title = _clean_html_text(title_el.text if title_el is not None and title_el.text else "")
            if not title or len(title) < 8:
                continue
            link = ""
            if link_el is not None:
                link = (link_el.text or link_el.attrib.get("href") or "").strip()
            summary = _clean_html_text(desc_el.text if desc_el is not None and desc_el.text else "")[:320]
            pub_utc = _parse_pubdate_to_utc_iso(
                pub_el.text if pub_el is not None and pub_el.text else "", now_utc_iso
            )
            obj_id = hashlib.sha256((link or title).encode("utf-8")).hexdigest()[:16]
            ev_id = f"{policy.source_id}:{obj_id}"
            signals.append(
                NormalizedTrendSignal(
                    event_id=ev_id,
                    source_id=policy.source_id,
                    source_provider=policy.provider,
                    source_object_id=obj_id,
                    canonical_event_id=_compute_canonical_event_id(title),
                    event_time_utc=pub_utc,
                    collected_at_utc=now_utc_iso,
                    title=title,
                    summary=summary,
                    reference_url=link,
                    region="VN",
                    data_rights="public_rss_metadata_only",
                    quality="observed",
                )
            )

        inserted = self.store.insert_signals_idempotent(signals)
        return signals, SourceRunAudit(
            source_id=policy.source_id,
            provider=policy.provider,
            status="SUCCESS",
            fetched_count=len(signals),
            new_inserted_count=inserted,
            http_status=200,
            note=f"Fetched {len(signals)} RSS items ({inserted} new)",
        )

    def _fetch_gdelt_source(
        self, policy: SourceCompliancePolicy, now_utc_iso: str
    ) -> Tuple[List[NormalizedTrendSignal], SourceRunAudit]:
        if not policy.feed_url:
            return [], SourceRunAudit(
                source_id=policy.source_id,
                provider=policy.provider,
                status="BLOCKED_COMPLIANCE",
                note="Missing feed_url",
            )
        query = '(Vietnam OR "Việt Nam") (weather OR rain OR heatwave OR gadget OR kitchen OR tech OR "air quality" OR power OR laptop)'
        params = {
            "query": query,
            "mode": "ArtList",
            "maxrecords": "40",
            "timespan": "3d",
            "format": "json",
        }
        try:
            with self._http_client(timeout_sec=18.0) as client:
                resp = client.get(policy.feed_url, params=params)
            if resp.status_code in (401, 403):
                self.gate.suspend_source(policy.source_id, f"HTTP {resp.status_code}")
                return [], SourceRunAudit(
                    source_id=policy.source_id,
                    provider=policy.provider,
                    status="SUSPENDED_401_403",
                    http_status=resp.status_code,
                    note=f"Fail-closed: auto-suspended on HTTP {resp.status_code}",
                )
            if resp.status_code == 429:
                return [], SourceRunAudit(
                    source_id=policy.source_id,
                    provider=policy.provider,
                    status="ERROR_NETWORK",
                    http_status=429,
                    note="GDELT rate limited (429); skipped cleanly without retry storm",
                )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            return [], SourceRunAudit(
                source_id=policy.source_id,
                provider=policy.provider,
                status="ERROR_NETWORK",
                note=str(exc)[:180],
            )

        articles = data.get("articles") if isinstance(data, dict) else []
        signals: List[NormalizedTrendSignal] = []
        for art in (articles or [])[:40]:
            if not isinstance(art, dict):
                continue
            title = _clean_html_text(str(art.get("title") or ""))
            url = str(art.get("url") or "").strip()
            if not title or len(title) < 8:
                continue
            seen = _parse_pubdate_to_utc_iso(str(art.get("seendate") or ""), now_utc_iso)
            obj_id = hashlib.sha256((url or title).encode("utf-8")).hexdigest()[:16]
            signals.append(
                NormalizedTrendSignal(
                    event_id=f"{policy.source_id}:{obj_id}",
                    source_id=policy.source_id,
                    source_provider=policy.provider,
                    source_object_id=obj_id,
                    canonical_event_id=_compute_canonical_event_id(title),
                    event_time_utc=seen,
                    collected_at_utc=now_utc_iso,
                    title=title,
                    summary=str(art.get("domain") or ""),
                    reference_url=url,
                    region="VN",
                    data_rights="gdelt_open_metadata",
                    quality="observed",
                )
            )
        inserted = self.store.insert_signals_idempotent(signals)
        return signals, SourceRunAudit(
            source_id=policy.source_id,
            provider=policy.provider,
            status="SUCCESS",
            fetched_count=len(signals),
            new_inserted_count=inserted,
            http_status=200,
            note=f"Fetched {len(signals)} GDELT metadata records ({inserted} new)",
        )

    def _resolve_youtube_api_auth(self) -> Tuple[Optional[str], Optional[str], str]:
        """
        Resolve official YouTube Data API v3 auth:
          1. Prefer `YOUTUBE_API_KEY` / `YOUTUBE_DATA_API_KEY` from env or `.env`.
          2. Fallback to official OAuth Bearer token from `config/platform_credentials.json`.
        Returns `(api_key, bearer_token, note)`.
        """
        from pipeline.core_resilience import TelegramNotifier

        env_vals = TelegramNotifier._read_dotenv(PROJECT_ROOT / ".env")
        api_key = (
            os.environ.get("YOUTUBE_DATA_API_KEY")
            or os.environ.get("YOUTUBE_API_KEY")
            or env_vals.get("YOUTUBE_DATA_API_KEY")
            or env_vals.get("YOUTUBE_API_KEY")
            or ""
        ).strip()
        if api_key:
            return api_key, None, "api_key"

        creds = safe_load_json(CONFIG_DIR / "platform_credentials.json", lambda: {})
        yt = creds.get("youtube") if isinstance(creds, dict) else None
        if isinstance(yt, dict) and yt.get("client_id") and yt.get("client_secret") and yt.get("refresh_token"):
            try:
                with self._http_client(timeout_sec=15.0) as client:
                    tr = client.post(
                        "https://oauth2.googleapis.com/token",
                        data={
                            "client_id": yt["client_id"],
                            "client_secret": yt["client_secret"],
                            "refresh_token": yt["refresh_token"],
                            "grant_type": "refresh_token",
                        },
                    )
                if tr.status_code == 200:
                    tok = str((tr.json() or {}).get("access_token") or "").strip()
                    if tok:
                        return None, tok, "oauth_bearer"
                return None, None, f"OAuth token refresh failed (HTTP {tr.status_code})"
            except Exception as exc:
                return None, None, f"OAuth token refresh error: {exc}"
        return None, None, "Missing YOUTUBE_API_KEY and OAuth credentials"

    def _fetch_youtube_data_api_source(
        self, policy: SourceCompliancePolicy, now_utc_iso: str
    ) -> Tuple[List[NormalizedTrendSignal], SourceRunAudit]:
        """
        Official YouTube Data API v3 (`youtube/v3/search`):
          - Runs at most 4 broad Vietnamese consumer-review queries (~400 quota units/run, <4% of daily quota).
          - Strictly respects fail-closed on HTTP 401/403 (never falls back to scraping!).
        """
        endpoint = policy.feed_url or "https://www.googleapis.com/youtube/v3/search"
        api_key, bearer_token, auth_note = self._resolve_youtube_api_auth()
        if not api_key and not bearer_token:
            return [], SourceRunAudit(
                source_id=policy.source_id,
                provider=policy.provider,
                status="BLOCKED_COMPLIANCE",
                note=auth_note,
            )

        published_after = (
            dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

        queries = [
            "review phụ kiện bàn làm việc góc làm việc",
            "review đồ nhà bếp thông minh tiện ích",
            "review sạc nhanh GaN đèn màn hình chuột công thái học",
            "máy hút ẩm máy lọc không khí máy hút chân không review",
        ]

        headers: Dict[str, str] = {}
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"

        signals: List[NormalizedTrendSignal] = []
        seen_vids: set[str] = set()
        last_http = 200

        try:
            with self._http_client(timeout_sec=18.0) as client:
                for q in queries:
                    params: Dict[str, str] = {
                        "part": "snippet",
                        "type": "video",
                        "q": q,
                        "regionCode": "VN",
                        "relevanceLanguage": "vi",
                        "order": "viewCount",
                        "publishedAfter": published_after,
                        "maxResults": "15",
                    }
                    if api_key:
                        params["key"] = api_key

                    resp = client.get(endpoint, params=params, headers=headers)
                    last_http = resp.status_code
                    if resp.status_code in (401, 403):
                        err_body = resp.text[:200]
                        # Distinguish quotaExceeded (temporary) vs insufficientPermissions/invalid (suspend)
                        if "quotaExceeded" in err_body:
                            return [], SourceRunAudit(
                                source_id=policy.source_id,
                                provider=policy.provider,
                                status="ERROR_NETWORK",
                                http_status=resp.status_code,
                                note=f"YouTube API daily quotaExceeded ({auth_note}); skipped cleanly.",
                            )
                        return [], SourceRunAudit(
                            source_id=policy.source_id,
                            provider=policy.provider,
                            status="SUSPENDED_401_403",
                            http_status=resp.status_code,
                            note=f"YouTube API HTTP {resp.status_code} ({auth_note}): {err_body[:140]}",
                        )
                    resp.raise_for_status()
                    items = (resp.json() or {}).get("items") or []
                    for it in items:
                        vid_id = str(((it.get("id") or {}).get("videoId")) or "").strip()
                        snip = it.get("snippet") or {}
                        if not vid_id or vid_id in seen_vids:
                            continue
                        seen_vids.add(vid_id)
                        title = _clean_html_text(str(snip.get("title") or ""))
                        desc = _clean_html_text(str(snip.get("description") or ""))[:260]
                        chan = _clean_html_text(str(snip.get("channelTitle") or ""))
                        pub_utc = _parse_pubdate_to_utc_iso(str(snip.get("publishedAt") or ""), now_utc_iso)
                        if not title:
                            continue
                        signals.append(
                            NormalizedTrendSignal(
                                event_id=f"{policy.source_id}:{vid_id}",
                                source_id=policy.source_id,
                                source_provider=f"YouTube ({chan})" if chan else policy.provider,
                                source_object_id=vid_id,
                                canonical_event_id=_compute_canonical_event_id(title),
                                event_time_utc=pub_utc,
                                collected_at_utc=now_utc_iso,
                                title=title,
                                summary=desc,
                                reference_url=f"https://www.youtube.com/watch?v={vid_id}",
                                region="VN",
                                metric_name="youtube_recent_video",
                                data_rights="youtube_data_api_v3_metadata",
                                quality="observed",
                            )
                        )
        except Exception as exc:
            return [], SourceRunAudit(
                source_id=policy.source_id,
                provider=policy.provider,
                status="ERROR_NETWORK",
                http_status=last_http,
                note=str(exc)[:180],
            )

        inserted = self.store.insert_signals_idempotent(signals)
        return signals, SourceRunAudit(
            source_id=policy.source_id,
            provider=policy.provider,
            status="SUCCESS",
            fetched_count=len(signals),
            new_inserted_count=inserted,
            http_status=200,
            note=f"Fetched {len(signals)} VN YouTube videos via {auth_note} ({inserted} new, 4 queries)",
        )

    def _match_owned_skus_for_concept(self, concept: Dict[str, Any]) -> Tuple[List[MatchedSKUCandidate], Optional[float]]:
        """
        Match real SKUs from `data/affiliate_csv/` (user-owned export) and `data/sku_registry.json`.
        Never fabricates sales or verification scores when no real marketplace data exists!
        """
        cid = concept["concept_id"]
        sku_anchors = [strip_accents(a) for a in concept.get("sku_anchors") or [] if a]

        matched: List[MatchedSKUCandidate] = []
        seen_keys: set[str] = set()

        # 1. Check Owned Shopee Affiliate CSV rows (strict anchor matching — never cross-match wrong products!)
        for row in self.csv_repo.rows:
            p_norm = row.get("product_name_norm") or ""
            if not any(anc in p_norm for anc in sku_anchors):
                continue
            skey = f"csv_{row.get('item_id') or hashlib.md5(p_norm.encode()).hexdigest()[:10]}"
            if skey in seen_keys:
                continue
            seen_keys.add(skey)
            matched.append(
                MatchedSKUCandidate(
                    sku_key=skey,
                    product_name=str(row.get("product_name") or ""),
                    shop_name=str(row.get("shop_name") or ""),
                    price_vnd=int(row.get("price_vnd") or 0),
                    sales_count=int(row.get("sales_count") or 0),
                    commission_rate_pct=float(row.get("commission_rate_pct") or 0.0),
                    short_link=str(row.get("short_link") or ""),
                    source_id="owned_shopee_affiliate_csv",
                    resolution_status="verified_csv",
                )
            )

        # 2. Check existing verified SKU Registry entries for this category_id
        for skey, entry in self.sku_registry.get_all_entries().items():
            if entry.category_id == cid and skey not in seen_keys:
                seen_keys.add(skey)
                offer = entry.best_affiliate_offer
                matched.append(
                    MatchedSKUCandidate(
                        sku_key=skey,
                        product_name=entry.product_name_vi,
                        brand=entry.brand,
                        shop_name=(offer.shop_name if offer else ""),
                        price_vnd=(offer.price_vnd if offer else entry.estimated_market_price_vnd),
                        sales_count=(offer.sales_count if offer else 0),
                        commission_rate_pct=(offer.commission_rate_pct if offer else 0.0),
                        short_link=(offer.short_link if offer else ""),
                        source_id="sku_registry",
                        resolution_status="verified_sku_registry",
                    )
                )

        matched.sort(key=lambda m: (0 if m.resolution_status == "verified_csv" else 1, -m.sales_count, -m.commission_rate_pct))
        top_skus = matched[:3]

        # Only compute marketplace_verification_100 when real CSV sales/commission data exists!
        csv_verified = [m for m in top_skus if m.resolution_status == "verified_csv" and m.sales_count > 0]
        if not csv_verified:
            return top_skus, None

        best_sales = max(m.sales_count for m in csv_verified)
        best_comm = max(m.commission_rate_pct for m in csv_verified)
        mp_score = min(100.0, round(45.0 + min(35.0, math.log1p(best_sales) * 5.0) + min(20.0, best_comm * 1.5), 1))
        return top_skus, mp_score

    def _evaluate_re_entry_policy(
        self,
        concept_id: str,
        canonical_event_id: str,
        effective_score_now: float,
        now_dt: dt.datetime,
    ) -> Tuple[ReEntryAction, str, List[str]]:
        """
        Section 8.7 — 3-Tier Re-Entry Policy against `data/sku_registry.json` & `used_event_ids`:
          - Duplicate canonical_event_id -> DUPLICATE_EVENT_IGNORED
          - 0..14 days since last video in this category -> SUPPRESSED_RECENTLY_COVERED
          - 15..45 days & score >= 80 -> DIVERSIFY_MODEL_OR_COMBO (must pick different brand/model or combo)
          - > 45 days & score >= 80 -> RESURGENCE_REQUIRE_NEW_ANGLE (must pick a fresh Upgrade-1 narrative_angle)
        """
        if self.store.is_event_already_used(canonical_event_id):
            return (
                "DUPLICATE_EVENT_IGNORED",
                "Sự kiện nguồn này đã từng được sử dụng cho video trước đó -> tự động chặn lặp.",
                [],
            )

        most_recent_days: Optional[float] = None
        covered_skus: List[str] = []
        for _, entry in self.sku_registry.get_all_entries().items():
            if entry.category_id != concept_id:
                continue
            if entry.has_solo_video or entry.has_combo_video:
                covered_skus.append(f"{entry.brand} {entry.model_code or ''}".strip())
                ts_str = entry.last_rendered_at or entry.updated_at
                if ts_str:
                    try:
                        rdt = dt.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                        if rdt.tzinfo is None:
                            rdt = rdt.replace(tzinfo=dt.timezone.utc)
                        days_ago = max(0.0, (now_dt - rdt.astimezone(dt.timezone.utc)).total_seconds() / 86400.0)
                        if most_recent_days is None or days_ago < most_recent_days:
                            most_recent_days = days_ago
                    except Exception:
                        pass

        if most_recent_days is None:
            return (
                "NEW_OPPORTUNITY",
                "Nhóm sản phẩm mới hoàn toàn, chưa từng dựng video gần đây.",
                ["HYPE_VS_REALITY_TEST", "REVERSE_CAVEAT_FIRST", "HABIT_DIAGNOSIS_STORY"],
            )

        if most_recent_days <= 14.0:
            return (
                "SUPPRESSED_RECENTLY_COVERED",
                f"Nhóm này vừa dựng video cách đây {most_recent_days:.1f} ngày ({', '.join(covered_skus[:2])}) -> khóa cứng trong cửa sổ 0–14 ngày.",
                [],
            )

        if most_recent_days <= 45.0:
            if effective_score_now >= 80.0:
                return (
                    "DIVERSIFY_MODEL_OR_COMBO",
                    f"Trend đợt 2 bùng lại sau {most_recent_days:.0f} ngày (điểm {effective_score_now}/100) -> cho phép bắt trend NHƯNG bắt buộc đổi sang Model/Hãng khác ({', '.join(covered_skus[:2])}) hoặc ghép Combo.",
                    ["SPEC_TRAP_AND_HIDDEN_COST", "HABIT_DIAGNOSIS_STORY"],
                )
            return (
                "SUPPRESSED_RECENTLY_COVERED",
                f"Nhóm đã làm cách đây {most_recent_days:.0f} ngày và điểm đợt 2 ({effective_score_now}/100) chưa đạt ngưỡng bùng nổ >= 80.",
                [],
            )

        if effective_score_now >= 80.0:
            return (
                "RESURGENCE_REQUIRE_NEW_ANGLE",
                f"Trend mùa mới sống lại sau {most_recent_days:.0f} ngày -> cho phép tái sinh NHƯNG ép M2 đổi Format kể chuyện mới & bám Hook sự kiện mới.",
                ["ANTI_DUST_DRAWER_TEST", "SPEC_TRAP_AND_HIDDEN_COST", "FAST_25S_MICRO_LOOP"],
            )
        return (
            "SUPPRESSED_RECENTLY_COVERED",
            f"Nhóm đã làm cách đây {most_recent_days:.0f} ngày nhưng tín hiệu hiện tại ({effective_score_now}/100) chưa đủ mạnh (>= 80) để tái sinh.",
            [],
        )

    def _stage_a_retrieve_candidates(
        self, recent_events: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Stage A: Pure Python Demand Lexicon + TF-IDF weighting (0 MB extra RAM).
        Groups matching events per Product Concept, deduplicating by `canonical_event_id`.
        """
        if not recent_events:
            return []

        # Build lightweight document frequency across recent events for IDF weighting
        doc_texts = [
            strip_accents(f"{ev.get('title', '')} {ev.get('summary', '')}")
            for ev in recent_events
        ]
        n_docs = max(1, len(doc_texts))

        candidates: List[Dict[str, Any]] = []
        for concept in PRODUCT_CONCEPT_TAXONOMY:
            triggers = [strip_accents(t) for t in concept.get("triggers") or []]
            exclusions = [strip_accents(e) for e in concept.get("exclusions") or []]

            matched_by_canonical: Dict[str, Dict[str, Any]] = {}
            publisher_families: set[str] = set()
            total_tfidf_weight = 0.0

            # Precompute IDF per trigger
            trig_idf: Dict[str, float] = {}
            for tr in triggers:
                df = sum(1 for dt_txt in doc_texts if tr in dt_txt)
                trig_idf[tr] = math.log((n_docs + 1.0) / (df + 1.0)) + 1.0

            for ev, norm_txt in zip(recent_events, doc_texts):
                if any(ex and ex in norm_txt for ex in exclusions):
                    continue
                hit_trigs = [tr for tr in triggers if tr and tr in norm_txt]
                if not hit_trigs:
                    continue

                ev_score = sum(trig_idf.get(tr, 1.0) for tr in hit_trigs)
                # Require stronger title/multi-trigger match for single generic terms
                title_norm = strip_accents(str(ev.get("title") or ""))
                in_title = any(tr in title_norm for tr in hit_trigs)
                if not in_title and len(hit_trigs) < 2:
                    continue

                cid_ev = str(ev.get("canonical_event_id") or ev.get("event_id"))
                pub_fam = _extract_publisher_family(str(ev.get("source_id") or ""))
                if cid_ev not in matched_by_canonical:
                    matched_by_canonical[cid_ev] = {
                        "event": ev,
                        "hit_triggers": hit_trigs,
                        "tfidf_score": ev_score + (1.2 if in_title else 0.0),
                        "publisher_family": pub_fam,
                    }
                    publisher_families.add(pub_fam)
                    total_tfidf_weight += ev_score
                else:
                    # Same canonical event republished -> do NOT increment independent source count!
                    if ev_score > matched_by_canonical[cid_ev]["tfidf_score"]:
                        matched_by_canonical[cid_ev]["tfidf_score"] = ev_score

            if not matched_by_canonical:
                continue

            ranked_ev = sorted(
                matched_by_canonical.values(),
                key=lambda x: (x["tfidf_score"], str(x["event"].get("event_time_utc") or "")),
                reverse=True,
            )
            candidates.append(
                {
                    "concept": concept,
                    "unique_canonical_events": ranked_ev,
                    "mention_count": float(len(ranked_ev)),
                    "independent_publishers": sorted(publisher_families),
                    "tfidf_total": round(total_tfidf_weight, 2),
                }
            )

        candidates.sort(
            key=lambda c: (len(c["independent_publishers"]), c["mention_count"], c["tfidf_total"]),
            reverse=True,
        )
        return candidates[:10]

    def _stage_b_llm_rerank(
        self, stage_a_candidates: List[Dict[str, Any]], use_llm: bool = True
    ) -> Dict[str, Any]:
        """Stage B: Gemini Full-Flash Structured Re-ranking & Vietnamese Explainability."""
        if not stage_a_candidates or not use_llm:
            return {}

        compact_payload = []
        for cand in stage_a_candidates:
            c = cand["concept"]
            top_headlines = [
                {
                    "title": item["event"].get("title", ""),
                    "source": item["event"].get("source_provider", ""),
                    "event_time_utc": item["event"].get("event_time_utc", ""),
                    "matched_keywords": item["hit_triggers"][:4],
                }
                for item in cand["unique_canonical_events"][:4]
            ]
            compact_payload.append(
                {
                    "product_concept_id": c["concept_id"],
                    "domain": c["domain"],
                    "product_concept_name_vi": c["name_vi"],
                    "unique_event_count": int(cand["mention_count"]),
                    "independent_publishers": cand["independent_publishers"],
                    "sample_headlines": top_headlines,
                }
            )

        system_prompt = (
            "Bạn là Chuyên gia Phân tích Nhu cầu Thị trường & Thương mại Điện tử Việt Nam (Compliance-First).\n"
            "Nhiệm vụ: Đánh giá danh sách Product Concept được Stage A bắt từ tin tức/RSS/GDELT Việt Nam.\n"
            "QUY TẮC NGHIÊM NGẶT:\n"
            "1. KHÔNG suy diễn bừa: Nếu tiêu đề tin tức chỉ trùng từ khóa tình cờ (VD: tin thời sự/chính trị/giải trí không tạo ra nhu cầu mua sản phẩm), PHẢI đặt `is_genuine_buying_need = false` và `relevance_score_100 <= 30`.\n"
            "2. CHỈ đặt `is_genuine_buying_need = true` khi tin tức/vấn đề đời sống thực sự kích thích nhu cầu tìm mua nhóm sản phẩm đó tại Việt Nam (VD: mưa nồm kéo dài -> máy hút ẩm/sấy quần áo; cảnh báo cháy nổ sạc -> trạm sạc GaN/ổ cắm chống quá tải; ô nhiễm bụi mịn -> máy lọc không khí HEPA; dân văn phòng mỏi mắt/cổ tay -> đèn màn hình/chuột dọc).\n"
            "3. Toàn bộ trường tiếng Việt (`trend_title_vi`, `need_pain_point_vi`, `why_matched_vi`, `trend_hook_angle_vi`) BẮT BUỘC viết bằng TIẾNG VIỆT CÓ DẤU UTF-8 CHUẨN 100%.\n"
            "4. `why_matched_vi` viết gọn theo cấu trúc: 'Trend X -> nhu cầu Y -> nhóm sản phẩm Z vì công dụng A'."
        )
        user_prompt = (
            "Hãy thẩm định và chấm điểm liên quan thực tế cho các nhóm sản phẩm ứng viên sau:\n"
            + json.dumps(compact_payload, ensure_ascii=False, indent=2)
        )

        cand_hash = hashlib.sha256(
            json.dumps(compact_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:24]
        cached_raw = self.store.get_llm_stage_b_cache(cand_hash, ttl_hours=6.0)
        if cached_raw:
            try:
                cached_res = LLMTrendReRankResponse.model_validate_json(cached_raw)
                print(
                    f"[M_TREND STAGE B] ⚡ Dùng cache Gemini Stage B (0 quota, {len(cached_res.evaluated_concepts)} nhóm).",
                    flush=True,
                )
                return {item.product_concept_id: item for item in cached_res.evaluated_concepts}
            except Exception:
                pass

        try:
            client = FreeTierGeminiClient()
            res, model_used = client.generate_structured(
                prompt=user_prompt,
                schema=LLMTrendReRankResponse,
                system_instruction=system_prompt,
                temperature=0.2,
            )
            self.store.set_llm_stage_b_cache(
                cand_hash,
                res.model_dump_json(),
                model_used,
                dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            )
            print(
                f"[M_TREND STAGE B] ✅ Gemini Full-Flash ({model_used}) đã thẩm định {len(res.evaluated_concepts)} nhóm ứng viên.",
                flush=True,
            )
            for item in res.evaluated_concepts:
                badge = "PASS" if (item.is_genuine_buying_need and item.relevance_score_100 >= 50.0) else "REJECT_NOISE"
                print(
                    f"  • [{badge}] {item.product_concept_id} (rel={item.relevance_score_100:.0f}/100, {item.trend_horizon}): {item.trend_title_vi}",
                    flush=True,
                )
            return {item.product_concept_id: item for item in res.evaluated_concepts}
        except Exception as exc:
            print(f"[M_TREND STAGE B] ⚠️ Gemini Full-Flash tạm gián đoạn ({exc}) -> dùng fallback điểm Stage A.", flush=True)
            return {}

    def run(
        self,
        use_llm: bool = True,
        simulate_decay_days: float = 0.0,
        lookback_days: int = 7,
    ) -> MTrendOpportunitiesOutput:
        MacOSLegacyRuntime.apply_gentle_background_priority()
        M_TREND_LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
        lock_fd = os.open(str(M_TREND_LOCK_PATH), os.O_CREAT | os.O_RDWR, 0o644)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(lock_fd)
            if self.output_path.exists():
                try:
                    return MTrendOpportunitiesOutput.model_validate_json(
                        self.output_path.read_text(encoding="utf-8")
                    )
                except Exception:
                    pass
            raise RuntimeError("M_TREND đang chạy ở tiến trình khác.")

        try:
            return self._run_locked(
                use_llm=use_llm,
                simulate_decay_days=simulate_decay_days,
                lookback_days=lookback_days,
            )
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
                os.close(lock_fd)
            except OSError:
                pass

    def _run_locked(
        self,
        use_llm: bool = True,
        simulate_decay_days: float = 0.0,
        lookback_days: int = 7,
    ) -> MTrendOpportunitiesOutput:
        now_utc = dt.datetime.now(dt.timezone.utc)
        eval_now_utc = now_utc + dt.timedelta(days=max(0.0, float(simulate_decay_days)))
        now_utc_iso = now_utc.isoformat(timespec="seconds")
        now_local_iso = dt.datetime.now().astimezone().isoformat(timespec="seconds")
        today_utc_str = now_utc.strftime("%Y-%m-%d")
        run_id = f"mtrend_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}"

        # 1. Retention purge on SQLite3
        self.store.purge_expired_records(retention_days=60)

        # 2. Run all enabled & compliance-approved connectors
        policies = self.gate.get_all_policies()
        audits: List[SourceRunAudit] = []
        raw_collected_now = 0
        active_sources = 0
        failed_sources = 0

        for sid, policy in policies.items():
            used_today = self.store.get_requests_today(sid, today_utc_str)
            allowed, gate_reason = self.gate.can_run(policy, now=now_utc, requests_used_today=used_today)
            if not allowed:
                audits.append(
                    SourceRunAudit(
                        source_id=sid,
                        provider=policy.provider,
                        status="SKIPPED_DISABLED" if gate_reason == "SKIPPED_DISABLED" else "BLOCKED_COMPLIANCE",
                        note=gate_reason,
                    )
                )
                continue

            if policy.access_type == "owned_export":
                active_sources += 1
                audits.append(
                    SourceRunAudit(
                        source_id=sid,
                        provider=policy.provider,
                        status="SUCCESS",
                        fetched_count=len(self.csv_repo.rows),
                        new_inserted_count=0,
                        note=f"Loaded {len(self.csv_repo.rows)} owned Shopee Affiliate CSV rows",
                    )
                )
                continue

            active_sources += 1
            if policy.access_type == "public_rss_metadata":
                sigs, audit = self._fetch_rss_source(policy, now_utc_iso)
            elif policy.access_type == "open_dataset_api" and "gdelt" in sid:
                sigs, audit = self._fetch_gdelt_source(policy, now_utc_iso)
            elif policy.access_type == "official_api" and "youtube" in sid:
                sigs, audit = self._fetch_youtube_data_api_source(policy, now_utc_iso)
            else:
                sigs, audit = [], SourceRunAudit(
                    source_id=sid,
                    provider=policy.provider,
                    status="SKIPPED_DISABLED",
                    note="Connector not enabled in MVP Phase 1",
                )

            raw_collected_now += len(sigs)
            if audit.status in ("ERROR_NETWORK", "SUSPENDED_401_403"):
                failed_sources += 1
            audits.append(audit)
            self.store.log_connector_run(run_id, audit, now_utc_iso)

        # 3. Load rolling window events from SQLite3
        since_24h = (now_utc - dt.timedelta(hours=24)).isoformat(timespec="seconds")
        since_window = (now_utc - dt.timedelta(days=max(3, lookback_days))).isoformat(timespec="seconds")
        events_24h = self.store.fetch_recent_events(since_24h)
        recent_events = self.store.fetch_recent_events(since_window)

        # 4. Stage A Retrieval (Demand Lexicon + TF-IDF)
        stage_a_candidates = self._stage_a_retrieve_candidates(recent_events)

        # 5. Stage B Re-ranking (Gemini Full-Flash Structured Output)
        llm_map = self._stage_b_llm_rerank(stage_a_candidates, use_llm=use_llm)

        # 6. Score, Verify with Owned CSV, Apply Half-Life Time-Decay & 3-Tier Re-Entry Policy
        active_opportunities: List[TrendOpportunityItem] = []
        suppressed_or_expired = 0

        for cand in stage_a_candidates:
            concept = cand["concept"]
            cid = concept["concept_id"]
            domain: TrendDomain = concept["domain"]
            ranked_ev = cand["unique_canonical_events"]
            indep_pubs = cand["independent_publishers"]
            mention_cnt = float(cand["mention_count"])

            # Persist daily observation for 28-day rolling baseline
            self.store.upsert_daily_observation(
                concept_id=cid,
                obs_date=today_utc_str,
                mention_count=mention_cnt,
                independent_sources=len(indep_pubs),
                now_utc_iso=now_utc_iso,
            )
            hist_series = self.store.get_concept_history_series(cid, limit_days=28)
            det = PurePythonTrendDetector.evaluate_series(hist_series[:-1] if len(hist_series) > 1 else [], mention_cnt)
            momentum_100 = float(det["momentum_100"])

            llm_verdict = llm_map.get(cid)
            seo_prefix = ""
            seo_kws: List[str] = []
            seo_tag = ""
            if llm_verdict is not None:
                if not llm_verdict.is_genuine_buying_need or llm_verdict.relevance_score_100 < 50.0:
                    suppressed_or_expired += 1
                    continue
                relevance_100 = float(llm_verdict.relevance_score_100)
                horizon: TrendHorizon = llm_verdict.trend_horizon
                trend_title_vi = llm_verdict.trend_title_vi
                pain_point_vi = llm_verdict.need_pain_point_vi
                why_matched_vi = llm_verdict.why_matched_vi
                hook_vi = llm_verdict.trend_hook_angle_vi
                seo_prefix = (llm_verdict.seo_context_prefix_vi or "").strip()
                seo_kws = [k.strip() for k in (llm_verdict.seo_trend_keywords_vi or []) if k and k.strip()][:3]
                raw_ht = (llm_verdict.seo_context_hashtag or "").strip()
                if raw_ht and raw_ht.startswith("#") and len(raw_ht) <= 28:
                    seo_tag = re.sub(r"\s+", "", raw_ht)
                ok_d, _, _ = check_vietnamese_diacritics(f"{trend_title_vi} {why_matched_vi}")
                if not ok_d:
                    trend_title_vi = concept["name_vi"]
            else:
                # Pure-Python fallback when `--no-llm` is used
                relevance_100 = min(85.0, round(55.0 + cand["tfidf_total"] * 4.5, 1))
                horizon = "FLASH_EVENT" if cid in ("mini_clothes_dryer_dehumidifier", "cordless_rechargeable_fan_powerbank", "hepa_air_purifier_mini") else "SEASONAL_SHIFT"
                trend_title_vi = ranked_ev[0]["event"].get("title", concept["name_vi"])[:95]
                pain_point_vi = f"Tín hiệu quan tâm tăng từ {len(indep_pubs)} nguồn về {concept['name_vi'].lower()}."
                why_matched_vi = (
                    f"Trend '{trend_title_vi[:55]}' -> nhu cầu thực tế -> nhóm {concept['name_vi']} "
                    f"(khớp từ khóa: {', '.join(ranked_ev[0]['hit_triggers'][:3])})."
                )
                hook_vi = f"Đang rộ chuyện {concept['name_vi'].lower()}, mua sao cho khỏi hớ?"

            # Cross-source score (1 publisher = 45, 2 independent publishers = 82, 3+ = 96)
            n_pub = len(indep_pubs)
            cross_source_100 = 45.0 if n_pub <= 1 else (82.0 if n_pub == 2 else 96.0)

            # Freshness based on newest signal timestamp
            newest_iso = str(ranked_ev[0]["event"].get("event_time_utc") or now_utc_iso)
            oldest_iso = str(ranked_ev[-1]["event"].get("event_time_utc") or newest_iso)
            try:
                newest_dt = dt.datetime.fromisoformat(newest_iso.replace("Z", "+00:00"))
                if newest_dt.tzinfo is None:
                    newest_dt = newest_dt.replace(tzinfo=dt.timezone.utc)
                age_hours = max(0.0, (eval_now_utc - newest_dt).total_seconds() / 3600.0)
            except Exception:
                newest_dt = now_utc
                age_hours = 0.0
            freshness_100 = round(max(10.0, 100.0 * math.exp(-age_hours / 72.0)), 1)

            # Owned Shopee Affiliate CSV marketplace verification (None if absent — NEVER faked!)
            matched_skus, mp_score = self._match_owned_skus_for_concept(concept)

            # Weighted Base Opportunity Score at `t = last_signal_at` (freshness=100 at signal peak),
            # then `compute_effective_score_now()` applies Exponential Half-Life Time-Decay cleanly ONCE (no double age penalty!)
            if mp_score is not None:
                base_score = round(
                    0.25 * momentum_100
                    + 0.25 * relevance_100
                    + 0.20 * cross_source_100
                    + 0.20 * mp_score
                    + 0.10 * 100.0,
                    1,
                )
                verif_status: VerificationStatus = "MARKETPLACE_CONFIRMED"
                conf_level: ConfidenceLevel = "HIGH" if n_pub >= 2 else "MEDIUM"
            else:
                base_score = round(
                    (
                        0.25 * momentum_100
                        + 0.25 * relevance_100
                        + 0.20 * cross_source_100
                        + 0.10 * 100.0
                    )
                    / 0.80,
                    1,
                )
                verif_status = "CANDIDATE" if matched_skus else "EARLY_DISCOVERY"
                conf_level = "MEDIUM" if n_pub >= 2 else "LOW"

            hl_hours = FLASH_EVENT_HALF_LIFE_HOURS if horizon == "FLASH_EVENT" else SEASONAL_SHIFT_HALF_LIFE_HOURS
            ttl_hours = FLASH_EVENT_TTL_HOURS if horizon == "FLASH_EVENT" else SEASONAL_SHIFT_TTL_HOURS
            expires_iso = (newest_dt + dt.timedelta(hours=ttl_hours)).isoformat(timespec="seconds")

            eff_score, is_expired = compute_effective_score_now(
                base_score=base_score,
                last_signal_at_iso=newest_iso,
                half_life_hours=hl_hours,
                expires_at_iso=expires_iso,
                now=eval_now_utc,
            )
            if is_expired or eff_score < MIN_ACTIVE_OPPORTUNITY_SCORE:
                suppressed_or_expired += 1
                continue

            top_canonical = str(ranked_ev[0]["event"].get("canonical_event_id") or "")
            re_action, re_note, rec_angles = self._evaluate_re_entry_policy(
                concept_id=cid,
                canonical_event_id=top_canonical,
                effective_score_now=eff_score,
                now_dt=eval_now_utc,
            )
            if re_action in ("SUPPRESSED_RECENTLY_COVERED", "DUPLICATE_EVENT_IGNORED"):
                suppressed_or_expired += 1
                continue

            # Section 8.8 — 2-Tier Queue Priority (`FAST_TRACK_TREND` vs `NORMAL_TREND`)
            can_fast_track = (
                eff_score >= FAST_TRACK_MIN_SCORE
                and (horizon == "FLASH_EVENT" or n_pub >= 2 or verif_status == "MARKETPLACE_CONFIRMED")
                and self.store.is_fast_track_48h_cap_available(now=eval_now_utc)
            )
            priority_tier: TrendPriorityTier = "FAST_TRACK_TREND" if can_fast_track else "NORMAL_TREND"

            opp_id = f"opp_{cid}_{top_canonical[-6:]}"
            active_opportunities.append(
                TrendOpportunityItem(
                    opportunity_id=opp_id,
                    canonical_event_id=top_canonical,
                    trend_title_vi=trend_title_vi,
                    need_pain_point_vi=pain_point_vi,
                    domain=domain,
                    product_concept_id=cid,
                    product_concept_name_vi=concept["name_vi"],
                    priority_tier=priority_tier,
                    trend_horizon=horizon,
                    half_life_hours=hl_hours,
                    first_seen_at=oldest_iso,
                    last_signal_at=newest_iso,
                    expires_at=expires_iso,
                    verification_status=verif_status,
                    re_entry_action=re_action,
                    re_entry_note_vi=re_note,
                    score_breakdown=OpportunityScoreBreakdown(
                        momentum_100=momentum_100,
                        relevance_100=relevance_100,
                        cross_source_100=cross_source_100,
                        marketplace_verification_100=mp_score,
                        freshness_100=freshness_100,
                        base_opportunity_score_100=base_score,
                        effective_score_now_100=eff_score,
                        confidence_level=conf_level,
                        detector_stats=det,
                    ),
                    independent_source_count=n_pub,
                    independent_sources=indep_pubs,
                    evidence_titles=[str(x["event"].get("title") or "")[:120] for x in ranked_ev[:3]],
                    evidence_urls=[str(x["event"].get("reference_url") or "") for x in ranked_ev[:3] if x["event"].get("reference_url")],
                    matched_skus=matched_skus,
                    why_matched_vi=why_matched_vi,
                    trend_hook_angle_vi=hook_vi,
                    seo_context_prefix_vi=seo_prefix,
                    seo_trend_keywords_vi=seo_kws,
                    seo_context_hashtag=seo_tag,
                    recommended_narrative_angles=rec_angles,
                )
            )

        # 7. Enforce Hard Top-8 Active Window sorted by `(priority_tier, effective_score_now_100)`
        active_opportunities.sort(
            key=lambda o: (
                1 if o.priority_tier == "FAST_TRACK_TREND" else 0,
                o.score_breakdown.effective_score_now_100,
                o.independent_source_count,
            ),
            reverse=True,
        )
        if len(active_opportunities) > MAX_ACTIVE_OPPORTUNITIES:
            suppressed_or_expired += len(active_opportunities) - MAX_ACTIVE_OPPORTUNITIES
            active_opportunities = active_opportunities[:MAX_ACTIVE_OPPORTUNITIES]

        # 8. Diagnostics (`status_reason_code` — Section 8.6)
        zero_streak = self.store.record_zero_match_day(
            obs_date=today_utc_str,
            raw_events_24h=len(events_24h),
            matched_opportunities=len(active_opportunities),
            now_utc_iso=now_utc_iso,
        )
        if active_opportunities:
            reason_code: TrendStatusReasonCode = "OK_HAS_OPPORTUNITIES"
            diag_vi = (
                f"Phát hiện {len(active_opportunities)} cơ hội SKU đang hoạt động (Top-{MAX_ACTIVE_OPPORTUNITIES} window) "
                f"từ {len(recent_events)} sự kiện trong {lookback_days} ngày qua."
            )
        elif active_sources > 0 and failed_sources >= max(1, (active_sources + 1) // 2):
            reason_code = "ERROR_COLLECTOR_FAILURE"
            diag_vi = (
                f"Cảnh báo ĐỎ: {failed_sources}/{active_sources} nguồn thu thập bị lỗi mạng/HTTP -> "
                "M0 sẽ tự động dùng EVERGREEN_ROTATION cho tới khi nguồn hồi phục."
            )
        elif zero_streak >= 3:
            reason_code = "WARNING_LOGIC_ZERO_MATCH"
            diag_vi = (
                f"Cảnh báo VÀNG: Đã {zero_streak} ngày liên tiếp thu được >=20 sự kiện/ngày nhưng 0 cơ hội khớp -> "
                "cần kiểm tra lại từ điển nhu cầu hoặc ngưỡng điểm."
            )
        else:
            reason_code = "NORMAL_NO_NEW_TREND_OR_ALL_IN_COOLDOWN"
            diag_vi = (
                f"Trạng thái bình thường: Không có đột biến >= {MIN_ACTIVE_OPPORTUNITY_SCORE:.0f} điểm hoặc các nhóm đang trend "
                f"đều nằm trong cửa sổ khóa ({suppressed_or_expired} mục bị ẩn/hết hạn). M0 dùng EVERGREEN_ROTATION."
            )

        output = MTrendOpportunitiesOutput(
            m_trend_run_id=run_id,
            generated_at_utc=now_utc_iso,
            generated_at_local=now_local_iso,
            status_reason_code=reason_code,
            diagnostic_summary_vi=diag_vi,
            raw_events_collected_now=raw_collected_now,
            raw_events_24h=len(events_24h),
            active_sources_count=active_sources,
            failed_sources_count=failed_sources,
            suppressed_or_expired_count=suppressed_or_expired,
            total_active_opportunities=len(active_opportunities),
            collector_audits=audits,
            opportunities=active_opportunities,
        )
        atomic_write_text(self.output_path, output.model_dump_json(indent=2))
        self.store.set_meta("last_run_utc", now_utc_iso)
        self.store.maybe_backup_and_vacuum()
        return output


def format_telegram_trend_report(out: MTrendOpportunitiesOutput) -> str:
    """Format an explainable Telegram alert report matching Section 8.3 & 8.8 of the Technical Plan."""
    lines = [
        f"📈 BÁO CÁO SKU TREND INTELLIGENCE ({out.generated_at_local[:16].replace('T', ' ')})",
        f"• Trạng thái: {out.status_reason_code}",
        f"• Tóm tắt: {out.diagnostic_summary_vi}",
        f"• Nguồn bật: {out.active_sources_count} (lỗi: {out.failed_sources_count}) | Sự kiện 24h: {out.raw_events_24h} | Đã lọc/hết hạn: {out.suppressed_or_expired_count}",
        "",
    ]
    if not out.opportunities:
        lines.append("⚪ Hiện chưa có cơ hội Trend mới nào vượt ngưỡng 60/100 (M0 giữ nhịp EVERGREEN_ROTATION).")
        return "\n".join(lines)

    for idx, opp in enumerate(out.opportunities[:5], start=1):
        sb = opp.score_breakdown
        tier_badge = "🔥 FAST_TRACK" if opp.priority_tier == "FAST_TRACK_TREND" else "📌 NORMAL"
        mp_txt = (
            f"{sb.marketplace_verification_100}/100 (Shopee CSV)"
            if sb.marketplace_verification_100 is not None
            else "Thiếu (chỉ có tín hiệu ngoài sàn)"
        )
        sku_line = (
            f"{opp.matched_skus[0].product_name[:60]} (~{opp.matched_skus[0].price_vnd // 1000}k)"
            if opp.matched_skus
            else "Chưa có trong CSV/Registry (giao M0 tìm đa hãng)"
        )
        lines.extend(
            [
                f"🚨 #{idx} [{tier_badge} | {opp.verification_status}] — {opp.product_concept_name_vi}",
                f"   • Trend: {opp.trend_title_vi}",
                f"   • Điểm thực hiện tại: {sb.effective_score_now_100}/100 (Gốc: {sb.base_opportunity_score_100} | Horizon: {opp.trend_horizon} | Bán rã: {int(opp.half_life_hours)}h)",
                f"   • Độ tin cậy: {sb.confidence_level} | Nguồn độc lập: {opp.independent_source_count} ({', '.join(opp.independent_sources)})",
                f"   • Kiểm chứng sàn: {mp_txt}",
                f"   • SKU gợi ý: {sku_line}",
                f"   • Lý do khớp: {opp.why_matched_vi}",
                f"   • Gợi ý Hook M2: \"{opp.trend_hook_angle_vi}\"",
                f"   • Tái nhập (Re-entry): {opp.re_entry_action}",
                f"   • Bằng chứng: {opp.evidence_urls[0] if opp.evidence_urls else 'N/A'}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Standalone Module M_TREND: External Trend & SKU Opportunity Intelligence"
    )
    parser.add_argument(
        "--no-llm",
        action="store_true",
        default=False,
        help="Run Stage A pure-Python matching without calling Gemini Full-Flash Stage B.",
    )
    parser.add_argument(
        "--simulate-decay-days",
        type=float,
        default=0.0,
        help="Simulate reading opportunities N days in the future to verify Half-Life Decay & TTL expiration.",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Rolling lookback window in days (default: 7).",
    )
    args = parser.parse_args()
    MacOSLegacyRuntime.ensure_runtime_path()

    engine = StandaloneMTrendEngine()
    out = engine.run(
        use_llm=not args.no_llm,
        simulate_decay_days=args.simulate_decay_days,
        lookback_days=args.lookback_days,
    )
    print(format_telegram_trend_report(out), flush=True)
    print(f"\n[M_TREND] Đã lưu hợp đồng JSON tại: {engine.output_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
