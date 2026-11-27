"""
Shared Low-Level Pure Utilities & Local CSV Index (`pipeline/shared_utils.py`).

Decouples pure text normalization, CSV parsing, and DuckDuckGo HTML search from any specific
pipeline stage (`M0..M5`), enforcing Criterion #1 (Strict Black-Box Module Independence).
"""
from __future__ import annotations

import csv
import glob
import re
import shutil
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from bs4 import BeautifulSoup

from pipeline.schemas_m0 import AffiliateListingMatch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
AFFILIATE_CSV_DIR = DATA_DIR / "affiliate_csv"


def strip_accents(s: str) -> str:
    s = (s or "").replace("đ", "d").replace("Đ", "D")
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()


_VI_DIACRITIC_CHAR_RE = re.compile(
    r"[àáảãạăằắẳẵặâầấẩẫậèéẻẽẹêềếểễệìíỉĩịòóỏõọôồốổỗộơờớởỡợùúủũụưừứửữựỳýỷỹỵđ"
    r"ÀÁẢÃẠĂẰẮẲẴẶÂẦẤẨẪẬÈÉẺẼẸÊỀẾỂỄỆÌÍỈĨỊÒÓỎÕỌÔỒỐỔỖỘƠỜỚỞỠỢÙÚỦŨỤƯỪỨỬỮỰỲÝỶỸỴĐ]"
)

# Tokens that are orthographically IMPOSSIBLE in standard accented Vietnamese AND are not English words.
# Their presence proves 100% that Vietnamese text was written without diacritics (tiếng Việt không dấu).
_FORBIDDEN_UNACCENTED_VI_WORDS: set[str] = {
    "chiec", "thiet", "nhieu", "hieu", "diem", "kiem", "nghiem", "viec", "dien", "tieng",
    "khien", "nghieng", "biet", "nhiet", "tuyet", "lieu", "trieu", "dieu", "kieu",
    "duoc", "nuoc", "truoc", "nhuoc", "nguoi", "muoi", "luoi", "chuot", "truot", "muot",
    "vuot", "suot", "thuong", "luong", "truong", "huong", "suong", "cuon", "luon",
    "khong", "nhung", "khoang", "nhua", "giup", "chinh", "chong", "ngoi", "giua", "hoac",
    "phai", "khoi", "duoi", "cung_cap", "san", "pham", "chuan", "khoe", "gon", "nhe",
} - {"san"}  # keep 'san' out in case of proper names like SanDisk

# Known English / brand / tech / unit tokens allowed without Vietnamese diacritics
_KNOWN_ASCII_TECH_BRAND_TOKENS: set[str] = {
    "usb", "type", "led", "oem", "dpi", "hdmi", "gan", "abs", "ps", "pp", "tritan", "rgb",
    "lcd", "oled", "ips", "va", "bluetooth", "wifi", "smartwheel", "logi", "bolt", "unifying",
    "logitech", "lift", "vertical", "mx", "master", "anywhere", "keys", "pebble",
    "baseus", "iwok", "wok", "tanita", "ugreen", "anker", "hyperwork", "orico", "keychron",
    "akko", "xiaomi", "mijia", "deerma", "lock", "locknlock", "inochi", "elmich", "bear",
    "kai", "wmf", "joseph", "tefal", "philips", "panasonic", "bluestone", "sunhouse",
    "delites", "hafele", "timemore", "hario", "fellow", "tescoma", "ikea", "muji",
    "series", "inox", "silicon", "silicone", "matte", "low", "profile", "switch", "keycap",
    "wireless", "hub", "dock", "docking", "arm", "stand", "setup", "desk", "laptop",
    "macbook", "mac", "pc", "ipad", "iphone", "android", "windows", "typec", "usbc",
    "pd", "qc", "mah", "watt", "ml", "mm", "cm", "kg", "gram", "calo", "kcal",
    "cafe", "coffee", "espresso", "matcha", "latte", "steak", "salad", "decor",
    "combo", "solo", "review", "reviewer", "unbox", "test", "video", "clip", "shorts",
    "reels", "tiktok", "youtube", "facebook", "shopee", "mall", "shop", "link", "bio",
    "ok", "model", "size", "mini", "pro", "max", "plus", "ultra", "air", "auto", "smart",
    "touch", "sensor", "timer", "mode", "app", "full", "hd", "uhd", "fps", "hz", "cri", "ra", "format",
    # Common English product/model nouns embedded inside Vietnamese product titles & specs
    "mi", "yeelight", "lumias", "levoit", "sharp", "jisulife", "philips", "midea", "simplus", "gaabor",
    "dreame", "roborock", "tineco", "dyson", "karcher", "bissell", "jimmy", "uwant", "eufy",
    "ecovacs", "shark", "bosch", "scotch", "brite", "vileda", "townew", "ninestars", "eko",
    "hoto", "eraclean", "sothing", "simpleway", "laifen", "iris", "ohyama", "woozoo",
    "electrolux", "hitachi", "toshiba", "comet", "fujihome", "fujie", "kosmen", "tiross",
    "zojirushi", "tiger", "halio", "showsee", "flyco", "acerpure", "enpuly", "kitimop", "parroti",
    "computer", "monitor", "light", "bar", "screen", "desk", "desktop", "laptop", "stand",
    "wireless", "ergonomic", "vertical", "mouse", "keyboard", "mechanical", "low", "profile",
    "switch", "silent", "custom", "qmk", "via", "charger", "station", "power", "adapter",
    "cable", "magnetic", "retractable", "hub", "dock", "docking", "arm", "mount", "vesa",
    "purifier", "hepa", "filter", "compact", "dehumidifier", "humidifier", "dryer", "fan",
    "portable", "rechargeable", "vacuum", "sealer", "cleaner", "duster", "blower", "cordless",
    "mop", "spray", "flat", "spin", "scrubber", "brush", "mattress", "mite", "uv", "uvc",
    "spot", "carpet", "upholstery", "trash", "bin", "lint", "remover", "shaver", "fabric",
    "steamer", "garment", "iron", "foldable", "shoe", "circulator", "diffuser", "aroma",
    "ultrasonic", "wiper", "window", "glass", "glassvac", "mosquito", "trap", "swatter",
    "flosser", "oral", "sonicare", "ionic", "hair", "gleam", "glory", "swift", "motion",
    "ipx7", "ipx5", "ipx4", "pa", "kpa", "psi", "khz", "rpm", "cadrr", "cadr",
    "scale", "kitchen", "digital", "thermometer", "probe", "kettle", "gooseneck",
    "grinder", "burr", "frother", "milk", "sprayer", "oil", "mist", "bottle", "dispenser",
    "soap", "foam", "touchless", "chopper", "garlic", "series", "lite", "gen", "edition",
}


def check_vietnamese_diacritics(
    text: str,
    min_words_for_ratio: int = 4,
    min_accent_ratio: float = 0.40,
    allow_english_product_names: bool = False,
) -> Tuple[bool, float, str]:
    """
    Strictly verify that `text` is written in proper accented UTF-8 Vietnamese (tiếng Việt có dấu đầy đủ).
    Catches:
      1. Fully unaccented Vietnamese (`tieng Viet khong dau`), even if tricked by `420.000đ` currency `đ`.
      2. Partially unaccented clauses slipped inside a beat (`>= 6` consecutive unaccented non-brand words).
      3. Orthographically impossible unaccented Vietnamese tokens (`chiec`, `chuot`, `khong`, `duoc`, `thiet`, ...).
    Returns `(is_valid, accent_ratio, failure_reason_vi)`.
    """
    raw = (text or "").strip()
    if not raw:
        return True, 1.0, ""

    # Strip SNAKE_CASE enum identifiers (e.g. SPEC_TRAP_AND_HIDDEN_COST, SINGLE_SKU_DEEP_DIVE)
    cleaned = re.sub(r"\b[A-Z0-9]+(?:_[A-Z0-9]+)+\b", " ", raw)
    # Strip currency 'đ' attached to numbers (e.g., 420.000đ, 89k, 1,85tr) so 'đ' in prices never fakes accents
    cleaned = re.sub(r"\b\d[\d.,]*\s*(?:đ|vnd|vnđ|k|tr|ml|mm|cm|g|kg|w|v|mah|dpi|hz)\b", " ", cleaned, flags=re.IGNORECASE)
    # Extract pure alphabetic tokens (length >= 2, no digits)
    raw_tokens = re.findall(r"[^\W\d_]{2,}", cleaned, flags=re.UNICODE)

    vi_eval_tokens: List[str] = []
    accented_count = 0
    consecutive_unaccented = 0
    max_consecutive_unaccented = 0
    forbidden_hits: List[str] = []

    for tok in raw_tokens:
        low = tok.lower()
        if low in _KNOWN_ASCII_TECH_BRAND_TOKENS:
            continue
        has_accent = bool(_VI_DIACRITIC_CHAR_RE.search(tok))
        vi_eval_tokens.append(tok)
        if has_accent:
            accented_count += 1
            consecutive_unaccented = 0
        else:
            consecutive_unaccented += 1
            if consecutive_unaccented > max_consecutive_unaccented:
                max_consecutive_unaccented = consecutive_unaccented
            if low in _FORBIDDEN_UNACCENTED_VI_WORDS and low not in forbidden_hits:
                forbidden_hits.append(low)

    if forbidden_hits:
        ratio = round(accented_count / max(1, len(vi_eval_tokens)), 3)
        return (
            False,
            ratio,
            f"Phát hiện từ tiếng Việt mất dấu ({', '.join(repr(w) for w in forbidden_hits[:6])})",
        )

    if not vi_eval_tokens:
        return True, 1.0, ""

    ratio = round(accented_count / len(vi_eval_tokens), 3)

    if allow_english_product_names:
        # For mixed VN + English model names / search queries:
        # forbidden unaccented Vietnamese words were already blocked above;
        # require at least 1 accented Vietnamese character if >= 3 non-brand tokens exist
        if len(vi_eval_tokens) >= 3 and accented_count == 0:
            return (
                False,
                0.0,
                f"Chuỗi ({len(vi_eval_tokens)} từ) hoàn toàn không có dấu tiếng Việt",
            )
        return True, ratio, ""

    if (max_consecutive_unaccented >= 6 and ratio < 0.65) or max_consecutive_unaccented >= 9:
        return (
            False,
            ratio,
            f"Có chuỗi {max_consecutive_unaccented} từ liên tiếp không dấu (tỷ lệ dấu {ratio * 100:.0f}%)",
        )

    if len(vi_eval_tokens) >= min_words_for_ratio and ratio < min_accent_ratio:
        return (
            False,
            ratio,
            f"Tỷ lệ từ tiếng Việt có dấu quá thấp ({accented_count}/{len(vi_eval_tokens)} = {ratio * 100:.0f}% < {min_accent_ratio * 100:.0f}%)",
        )

    if 2 <= len(vi_eval_tokens) < min_words_for_ratio and accented_count == 0:
        return (
            False,
            0.0,
            f"Cụm tiêu đề ngắn ({len(vi_eval_tokens)} từ) hoàn toàn không có dấu tiếng Việt",
        )

    return True, ratio, ""


def normalize_vietnamese_for_tts(text: str) -> str:
    """
    Convert compact visual symbols, prices, technical units, and model codes inside spoken
    `tts_voiceover_vi` into natural Vietnamese words so TTS engines read with human-like clarity
    instead of spelling letter-by-letter or misreading `12.000Pa`, `4000mAh`, `CRI Ra>95`, `A3_Lite`.
    """
    s = " ".join((text or "").strip().split())
    if not s:
        return s

    # Clean markdown / stray formatting symbols that make TTS stall
    s = s.replace("*", "").replace("`", "").replace("|", ", ")
    # Remove tilde '~' before numbers so TTS doesn't read "ngã"
    s = re.sub(r"~\s*(?=\d)", "", s)
    # Replace underscores in model codes like A3_Lite -> A3 Lite
    s = re.sub(r"(?<=[A-Za-z0-9])_(?=[A-Za-z0-9])", " ", s)
    # Replace em-dash / en-dash between phrases with comma pause
    s = re.sub(r"\s+[—–]\s+", ", ", s)

    # Exact formatted VND prices like 1.850.000đ or 420.000đ or 89.000đ
    def _repl_full_vnd(m: re.Match[str]) -> str:
        digits = re.sub(r"[^\d]", "", m.group(1))
        if not digits:
            return m.group(0)
        val = int(digits)
        if val >= 1_000_000:
            tr = val // 1_000_000
            rem_k = (val % 1_000_000) // 1000
            if rem_k == 0:
                return f"{tr} triệu đồng"
            return f"{tr} triệu {rem_k} nghìn đồng"
        if val >= 1000:
            return f"{val // 1000} nghìn đồng"
        return f"{val} đồng"

    s = re.sub(r"\b(\d{1,3}(?:[.,]\d{3})+)\s*(?:đ|vnđ|vnd)\b", _repl_full_vnd, s, flags=re.IGNORECASE)

    # Shorthand millions like 1,85tr or 1.8tr
    def _repl_tr(m: re.Match[str]) -> str:
        tr = int(m.group(1))
        dec = m.group(2)
        if not dec:
            return f"{tr} triệu"
        k_val = int((dec + "000")[:3])
        return f"{tr} triệu {k_val} nghìn" if k_val > 0 else f"{tr} triệu"

    s = re.sub(r"\b(\d+)(?:[.,](\d{1,3}))?\s*tr\b", _repl_tr, s, flags=re.IGNORECASE)

    # Shorthand thousands like 420k, 89k
    s = re.sub(r"\b(\d+)\s*k\b", r"\1 nghìn", s, flags=re.IGNORECASE)

    # Round thousands with tech units: 12.000Pa, 15000Pa, 4.000mAh, 10000mAh, 4000 DPI
    def _repl_thousand_unit(unit_vi: str):
        def _fn(m: re.Match[str]) -> str:
            digits = re.sub(r"[^\d]", "", m.group(1))
            if not digits:
                return m.group(0)
            val = int(digits)
            if val >= 1000 and val % 1000 == 0:
                return f"{val // 1000} nghìn {unit_vi}"
            if val >= 1000 and val % 100 == 0:
                return f"{val // 1000} nghìn {(val % 1000) // 100} trăm {unit_vi}"
            return f"{val} {unit_vi}"
        return _fn

    s = re.sub(r"\b(\d{1,2}(?:[.,]\d{3})|\d{4,5})\s*pa\b", _repl_thousand_unit("pát can"), s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d{1,2}(?:[.,]\d{3})|\d{4,5})\s*mah\b", _repl_thousand_unit("mi li am pe giờ"), s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d{1,2}(?:[.,]\d{3})|\d{4,5})\s*dpi\b", _repl_thousand_unit("đi pi ai"), s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d{1,2}(?:[.,]\d{3})|\d{4,5})\s*rpm\b", _repl_thousand_unit("vòng mỗi phút"), s, flags=re.IGNORECASE)

    # Units preceded by Vietnamese number words like '15 nghìn Pa', '4 nghìn mAh', '4 nghìn DPI'
    s = re.sub(r"\b(nghìn|ngàn|trăm|triệu)\s+kpa\b", r"\1 ki lô pát can", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(nghìn|ngàn|trăm|triệu)\s+pa\b", r"\1 pát can", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(nghìn|ngàn|trăm|triệu)\s+mah\b", r"\1 mi li am pe giờ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(nghìn|ngàn|trăm|triệu)\s+dpi\b", r"\1 đi pi ai", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(nghìn|ngàn|trăm|triệu)\s+rpm\b", r"\1 vòng mỗi phút", s, flags=re.IGNORECASE)

    # CRI / Ra optical color rendering index
    s = re.sub(r"\bCRI\s*(?:Ra\s*)?(?:>=|>)\s*(\d+)\b", r"chỉ số hoàn màu trên \1", s, flags=re.IGNORECASE)
    s = re.sub(r"\bRa\s*(?:>=|>)\s*(\d+)\b", r"chỉ số hoàn màu trên \1", s, flags=re.IGNORECASE)
    s = re.sub(r"\bCRI\s*(?:Ra\s*)?(\d+)\b", r"chỉ số hoàn màu \1", s, flags=re.IGNORECASE)
    s = re.sub(r"\bRa\s*(\d+)\b", r"chỉ số hoàn màu \1", s, flags=re.IGNORECASE)

    # IP water/dust ratings, UV-C, PSI & PM2.5
    s = re.sub(r"\bIPX(\d)\b", r"I P X \1", s, flags=re.IGNORECASE)
    s = re.sub(r"\bIP(\d{2})\b", r"I P \1", s, flags=re.IGNORECASE)
    s = re.sub(r"\bUV[-\s]*C\b", "U V C", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*[-–]\s*(\d+)\s*psi\b", r"\1 đến \2 P S I", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*psi\b", r"\1 P S I", s, flags=re.IGNORECASE)
    s = re.sub(r"\bPM\s*2[.,]5\b", "P M 2 phẩy 5", s, flags=re.IGNORECASE)

    # Slash-based rate units like vòng/phút, lần/ngày, ml/giờ
    s = re.sub(r"\bvòng\s*/\s*phút\b", "vòng mỗi phút", s, flags=re.IGNORECASE)
    s = re.sub(r"\blần\s*/\s*ngày\b", "lần mỗi ngày", s, flags=re.IGNORECASE)
    s = re.sub(r"\bml\s*/\s*giờ\b", "mi li lít mỗi giờ", s, flags=re.IGNORECASE)
    s = re.sub(r"\bl\s*/\s*ngày\b", "lít mỗi ngày", s, flags=re.IGNORECASE)

    # Decimals + units like 0.1g, 1.5kg, 1.2L, 2.4GHz, 5.5W, 99.9%
    s = re.sub(r"\b(\d+)[.,](\d+)\s*g\b", r"\1 phẩy \2 gam", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*kg\b", r"\1 phẩy \2 ký", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*L\b", r"\1 phẩy \2 lít", s)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*m\b", r"\1 phẩy \2 mét", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*ghz\b", r"\1 phẩy \2 ghi ga héc", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*kpa\b", r"\1 phẩy \2 ki lô pát can", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*db\b", r"\1 phẩy \2 đề xi ben", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*w\b", r"\1 phẩy \2 oát", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*v\b", r"\1 phẩy \2 vôn", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*a\b", r"\1 phẩy \2 am pe", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)[.,](\d+)\s*%", r"\1 phẩy \2 phần trăm", s)

    # Ranges + units like 3-6kg, 60-100°C, 30-45dB, 2-3 giờ
    s = re.sub(r"\b(\d+)\s*[-–]\s*(\d+)\s*kg\b", r"\1 đến \2 ký", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*[-–]\s*(\d+)\s*°\s*c\b", r"\1 đến \2 độ C", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*[-–]\s*(\d+)\s*db\b", r"\1 đến \2 đề xi ben", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*[-–]\s*(\d+)\s*w\b", r"\1 đến \2 oát", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*[-–]\s*(\d+)\s*%", r"\1 đến \2 phần trăm", s)
    s = re.sub(
        r"\b(\d+)\s*[-–]\s*(\d+)\s*(giờ|phút|giây|ngày|tháng|năm|mức|cấp|chế độ|tốc độ)\b",
        r"\1 đến \2 \3",
        s,
        flags=re.IGNORECASE,
    )

    # Dimensions like 125x142mm
    s = re.sub(r"\b(\d+)\s*[x×]\s*(\d+)\s*mm\b", r"\1 nhân \2 mi li mét", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*[x×]\s*(\d+)\s*cm\b", r"\1 nhân \2 xen ti mét", s, flags=re.IGNORECASE)

    # Comparison symbols before numbers
    s = re.sub(r">=\s*(\d)", r"từ \1", s)
    s = re.sub(r"<=\s*(\d)", r"tối đa \1", s)
    s = re.sub(r">\s*(\d)", r"trên \1", s)
    s = re.sub(r"<\s*(\d)", r"dưới \1", s)

    # Standalone units attached to numbers
    s = re.sub(r"(\d+)\s*°\s*c\b", r"\1 độ C", s, flags=re.IGNORECASE)
    s = re.sub(r"(\d+)\s*°", r"\1 độ", s)
    s = re.sub(r"\b(\d+)\s*ml\b", r"\1 mi li lít", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*mm\b", r"\1 mi li mét", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*cm\b", r"\1 xen ti mét", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*kg\b", r"\1 ký", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*g\b", r"\1 gam", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*kpa\b", r"\1 ki lô pát can", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*pa\b", r"\1 pát can", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*mah\b", r"\1 mi li am pe giờ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*wh\b", r"\1 oát giờ", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*db\b", r"\1 đề xi ben", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*dpi\b", r"\1 đi pi ai", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*rpm\b", r"\1 vòng mỗi phút", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*ghz\b", r"\1 ghi ga héc", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*mhz\b", r"\1 mê ga héc", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*khz\b", r"\1 ki lô héc", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*hz\b", r"\1 héc", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*w\b", r"\1 oát", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*v\b", r"\1 vôn", s, flags=re.IGNORECASE)
    s = re.sub(r"\b(\d+)\s*L\b", r"\1 lít", s)
    s = re.sub(r"(\d+)\s*%", r"\1 phần trăm", s)

    # Standalone case-sensitive technical unit abbreviations even when not directly glued to a digit
    s = re.sub(r"\bkPa\b", "ki lô pát can", s)
    s = re.sub(r"\bPa\b", "pát can", s)
    s = re.sub(r"\bmAh\b", "mi li am pe giờ", s)
    s = re.sub(r"\bdB\b", "đề xi ben", s)
    s = re.sub(r"\bDPI\b", "đi pi ai", s)
    s = re.sub(r"\bRPM\b", "vòng mỗi phút", s)

    # Smooth tech connectors & common acronyms for Vietnamese TTS
    s = re.sub(r"\bUSB[-\s]*C\b", "USB Type C", s, flags=re.IGNORECASE)
    s = re.sub(r"\bType[-\s]*C\b", "Type C", s, flags=re.IGNORECASE)
    s = re.sub(r"\bHEPA\b", "Hê pa", s, flags=re.IGNORECASE)
    s = re.sub(r"\bGaN\b", "Gan", s)
    s = re.sub(r"\b4K\b", "4 ca", s)
    s = re.sub(r"\b2K\b", "2 ca", s)
    s = re.sub(r"\bi[-\s]*wok\s*2\b", "ai uốc 2", s, flags=re.IGNORECASE)
    s = re.sub(r"\bLite\b", "Lai", s)

    # Hyphenated model codes (KD-192 -> KD 192, SN-88 -> SN 88, VS-B01V1 -> VS B01V1)
    s = re.sub(r"\b([A-Za-z]{1,4})-([A-Za-z0-9]{2,8})\b", r"\1 \2", s)

    # Slash between words like bật/tắt, khô/ướt, nóng/lạnh -> " hoặc "
    s = re.sub(r"(?<=[A-Za-zÀ-ỹ])\s*/\s*(?=[A-Za-zÀ-ỹ])", " hoặc ", s)

    # Smooth parentheses into natural pauses
    s = re.sub(r"\s*\(\s*", ", ", s)
    s = re.sub(r"\s*\)\s*", ", ", s)
    s = re.sub(r",\s*,+", ", ", s)
    s = re.sub(r",\s*([.!?])", r"\1", s)
    return " ".join(s.split())


def audit_tts_readability(text: str) -> List[str]:
    """
    Deterministic TTS naturalness & rhythm audit for M3 QA/QC.
    Returns a list of Vietnamese issue descriptions (empty list = clean, human-like TTS flow).
    """
    s = " ".join((text or "").strip().split())
    if not s:
        return ["Lời thoại trống."]

    issues: List[str] = []
    # 1. Raw underscores in spoken voiceover (e.g., A3_Lite)
    if "_" in s:
        issues.append("Chứa dấu gạch dưới '_' trong lời thoại TTS (dễ đọc lỗi mã sản phẩm).")

    # 2. Ugly long factory SKU codes read letter-by-letter (e.g., JPF12AV2, KTB-3466, VS-B01V1, YLTD003, CKS321GRN)
    ugly_codes = re.findall(
        r"\b(?=[A-Z0-9-]{5,}\b)(?:[A-Z]{2,}\d+[A-Z0-9-]*|[A-Z]{1,4}-[A-Z0-9]{3,})\b",
        s,
    )
    if ugly_codes:
        issues.append(
            f"Chứa mã linh kiện/xuất xưởng dài khó đọc tự nhiên ({', '.join(ugly_codes[:3])}); "
            "nên đọc gọn bằng tên thương hiệu + dòng máy thân thiện (VD: 'mẫu máy hút ẩm nhà Daikin')."
        )

    # 2b. Long untranslated English product phrases (>= 3 consecutive English product words like 'Car Vacuum Cleaner')
    en_phrases = re.findall(
        r"\b(?:[A-Z][a-z]{2,}\s+){2,}[A-Z][a-z]{2,}\b",
        s,
    )
    if en_phrases:
        issues.append(
            f"Chứa cụm tên tiếng Anh dài gây cứng giọng TTS ({', '.join(en_phrases[:2])}); "
            "hãy Việt hóa tự nhiên trong lời thoại TTS."
        )

    # 3. Unpunctuated run-on segments (> 24 words without any pause . ! ? , ; :)
    segments = [seg.strip() for seg in re.split(r"[.!?,;:]", s) if seg.strip()]
    longest_seg_words = max((len(seg.split()) for seg in segments), default=0)
    if longest_seg_words > 24:
        issues.append(
            f"Có đoạn dài {longest_seg_words} từ liên tiếp không có dấu ngắt câu (. , ? !), "
            "khiến TTS bị hụt hơi hoặc cắt nhịp giữa chừng làm mất nghĩa."
        )

    return issues


def sanitize_youtube_metadata_text(text: str, max_chars: int = 4800) -> str:
    """
    Sanitize `snippet.title` or `snippet.description` for YouTube Data API v3 (`videos.insert`).
    YouTube strictly rejects `<` and `>` anywhere in title/description with `HTTP 400 Bad Request` (`invalidDescription` / `invalidTitle`).
    """
    s = str(text or "")
    s = s.replace("->", "→").replace("<-", "←").replace("<=", "≤").replace(">=", "≥")
    s = s.replace("<", "‹").replace(">", "›")
    s = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", s)
    if len(s) > max_chars:
        s = s[: max_chars - 1].rsplit(" ", 1)[0].strip()
    return s.strip()


def sanitize_youtube_tags(raw_tags: List[str], max_weighted_bytes: int = 350, max_count: int = 15) -> List[str]:
    """
    Sanitize `snippet.tags` for YouTube Data API v3 (`videos.insert`) so it never triggers `HTTP 400 invalidTags`:
      - Removes forbidden `<`, `>`, `,`, `"`, `#`, and replaces `_` with space (`A3_Lite` -> `a3 lite`).
      - Accounts for YouTube's internal `+3` quote/comma overhead on multi-word UTF-8 tags (`<= 350` bytes vs 500 limit).
    """
    cleaned_list: List[str] = []
    used_budget = 0
    for t in raw_tags or []:
        c = str(t or "").replace("_", " ")
        c = re.sub(r"[<>,\"#`]", " ", c)
        c = re.sub(r"[\x00-\x1f\x7f]", "", c)
        c = " ".join(c.split()).strip()
        if len(c) < 2 or len(c) > 45:
            continue
        if c.lower() in {x.lower() for x in cleaned_list}:
            continue
        # YouTube counts UTF-8 length + quotes for multi-word tags + comma separator
        tag_cost = len(c.encode("utf-8")) + (3 if " " in c else 1)
        if used_budget + tag_cost > max_weighted_bytes or len(cleaned_list) >= max_count:
            break
        cleaned_list.append(c)
        used_budget += tag_cost
    return cleaned_list


def enforce_short_hook_title(raw_title: str, max_chars: int = 30) -> str:
    """Ensure the opening hook title is strictly within 0..30 characters without cutting mid-word."""
    clean = " ".join((raw_title or "").strip().split())
    if not clean:
        return "ĐÁNG MUA HAY KHÔNG?"
    if len(clean) <= max_chars:
        return clean
    words = clean.split()
    out: List[str] = []
    for w in words:
        cand = " ".join(out + [w])
        if len(cand) <= max_chars:
            out.append(w)
        else:
            break
    return " ".join(out) if out else clean[:max_chars].strip()


def truncate_words_clean(text: str, max_chars: int = 42) -> str:
    """Truncate text cleanly at a word boundary without ever slicing mid-word (fixes 'tự n' cutoffs)."""
    clean = " ".join((text or "").strip().split())
    if len(clean) <= max_chars:
        return clean
    words = clean.split()
    out: List[str] = []
    for w in words:
        cand = " ".join(out + [w])
        if len(cand) <= max_chars:
            out.append(w)
        else:
            break
    if out:
        return " ".join(out).rstrip(",;:-")
    return clean[:max_chars].strip()


def format_vnd_short(price_vnd: int) -> str:
    """Format VND price naturally for Vietnamese titles & cards: 3_450_000 -> '3,45tr', 420_000 -> '420k'."""
    val = max(0, int(price_vnd or 0))
    if val >= 1_000_000:
        tr = val / 1_000_000.0
        # Format up to 2 decimals, strip trailing zeros (3.45 -> 3,45tr; 1.80 -> 1,8tr; 2.00 -> 2tr)
        s = f"{tr:.2f}".rstrip("0").rstrip(".").replace(".", ",")
        return f"{s}tr"
    k = max(1, int(round(val / 1000.0)))
    return f"{k}k"


def parse_int_number(raw: str) -> int:
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


def parse_sales_count(raw: str) -> int:
    s = str(raw or "").strip().lower().replace(",", ".")
    if not s:
        return 0
    m_k = re.search(r"([\d.]+)\s*(?:k|nghìn|nghin)", s)
    if m_k:
        return int(float(m_k.group(1)) * 1000)
    m_tr = re.search(r"([\d.]+)\s*(?:tr|triệu|trieu)", s)
    if m_tr:
        return int(float(m_tr.group(1)) * 1_000_000)
    return parse_int_number(s)


def parse_pct(raw: str) -> float:
    s = str(raw or "").strip().replace("%", "").replace(",", ".")
    try:
        return round(float(s), 2)
    except ValueError:
        return 0.0


def detect_ambiguous_listing(title: str) -> bool:
    """Detect Shopee titles that bundle multiple distinct model codes or capacities to bait low anchor prices."""
    upper = title.upper()
    model_tokens = re.findall(r"\b[A-Z]{2,4}[-]?[0-9]{2,4}[A-Z0-9]*\b", upper)
    if len(set(model_tokens)) >= 2:
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
                        self.rows.append(
                            {
                                "item_id": item_id,
                                "product_name": title,
                                "product_name_norm": strip_accents(title),
                                "shop_name": (r.get("Tên cửa hàng") or r.get("shop_name") or "").strip(),
                                "price_vnd": parse_int_number(r.get("Giá") or r.get("price") or "0"),
                                "sales_count": parse_sales_count(r.get("Doanh thu") or r.get("sales") or "0"),
                                "commission_rate_pct": parse_pct(r.get("Tỉ lệ hoa hồng") or r.get("commission_rate") or "0"),
                                "commission_amount_vnd": parse_int_number(r.get("Hoa hồng") or "0"),
                                "product_link": (r.get("Link sản phẩm") or "").strip(),
                                "short_link": (r.get("Link ưu đãi") or r.get("short_link") or "").strip(),
                                "is_ambiguous_listing": detect_ambiguous_listing(title),
                            }
                        )
            except Exception:
                continue

    def find_category_rows(self, category_queries: List[str], limit: int = 12) -> List[Dict[str, Any]]:
        if not self.rows:
            return []
        keywords = set()
        stop_words = {"loai", "nao", "tot", "so", "sanh", "danh", "gia", "review", "nhat", "cho", "va", "co", "nen", "mua"}
        for q in category_queries:
            tokens = [t for t in re.split(r"\W+", strip_accents(q)) if len(t) >= 3 and t not in stop_words]
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
        matches: List[AffiliateListingMatch] = []
        brand_norm = strip_accents(brand)
        brand_tokens = [t for t in re.split(r"\W+", brand_norm) if len(t) >= 2]
        model_norm = strip_accents(model_code or "")

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
                    match_confidence=confidence,  # type: ignore[arg-type]
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
