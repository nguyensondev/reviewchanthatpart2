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
