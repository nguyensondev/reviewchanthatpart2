"""
Module M1: Smart Filter & Multi-Brand Ranker (`pipeline/m1_filter.py`).

Responsibilities (Black-Box Contract):
  1. Reads ONLY `data/m0_candidate_pool.json` (zero code coupling to M0).
  2. Completely agnostic of M2's script format (Single-SKU vs Combo).
  3. Step M1.1 (Hard Disqualification & Foil Separation):
     - Filters out `DEALBREAKER_DEFECT`, `MULTI_MODEL_AMBIGUOUS`, and zero-video candidates.
     - Separates low-tier `OEM_GENERIC` foils into `rejected_foils` when stronger branded candidates exist,
       preserving their exact price and defects as comparative evidence for M2.
  4. Step M1.2 (Objective 100-Point Scorer):
     - Pillar 1 (35 pts): Price-to-Category-Median Sweet Spot (20 pts) + Spec & Model Completeness (15 pts).
     - Pillar 2 (25 pts): Durability & Caveat Manageability (`MINOR` vs `MODERATE`).
     - Pillar 3 (25 pts): Visual Demo & Web Evidence Strength (YouTube clips, resolution, views, web snippets).
     - Pillar 4 (15 pts): Affiliate Readiness Bonus (Tie-breaker only; products without CSV can still score 85/100).
  5. Step M1.3 (LLM Head-to-Head Comparative Judge):
     - Uses 1 Gemini call (`gemini-3.5-flash-lite`) to generate transparent comparative verdicts explaining
       why the Category Winner beats the Runner-Up and why viewers should avoid the Rejected Foils.
  6. Outputs `data/m1_qualified_skus.json` for Module M2.
"""
from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import atomic_write_text
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.schemas_m0 import (
    AffiliateListingMatch,
    DiscoveredCandidateSKU,
    FunctionalCategoryGroup,
    M0CandidatePoolOutput,
)
from pipeline.schemas_m1 import (
    AffiliateStatus,
    EvaluatedCandidateSKU,
    HeadToHeadVerdict,
    LLMComparativeJudgeBatchResponse,
    M1QualifiedSKUsOutput,
    QualifiedCategoryResult,
    RejectedFoilSKU,
    ScoreBreakdown100,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_M0_POOL_PATH = DATA_DIR / "m0_candidate_pool.json"
OUTPUT_M1_PATH = DATA_DIR / "m1_qualified_skus.json"


def _select_best_affiliate_offer(
    matches: List[AffiliateListingMatch],
) -> Tuple[Optional[AffiliateListingMatch], AffiliateStatus, float, str]:
    """
    Evaluate affiliate matches and return:
      (best_offer, affiliate_status, bonus_score_max_15, explanation_str)
    """
    if not matches:
        return (
            None,
            "NEEDS_LINK_LOOKUP",
            0.0,
            "Chưa có CSV Affiliate nội bộ (0/15đ phụ — không ảnh hưởng điểm chất lượng gốc)",
        )

    # Prefer non-ambiguous listings first
    clean_matches = [m for m in matches if not m.is_ambiguous_listing]
    pool = clean_matches if clean_matches else matches

    # Sort by: EXACT_MODEL > BRAND_AND_CATEGORY > CATEGORY_FALLBACK, then sales_count desc, then commission_rate_pct desc
    conf_rank = {"EXACT_MODEL": 0, "BRAND_AND_CATEGORY": 1, "CATEGORY_FALLBACK": 2}
    pool_sorted = sorted(
        pool,
        key=lambda m: (
            conf_rank.get(m.match_confidence, 3),
            -m.sales_count,
            -m.commission_rate_pct,
        ),
    )
    best = pool_sorted[0]

    # 1. Match confidence score (max 6.0)
    if best.match_confidence == "EXACT_MODEL":
        conf_pts = 6.0
    elif best.match_confidence == "BRAND_AND_CATEGORY":
        conf_pts = 4.0
    else:
        conf_pts = 1.5

    # Penalty if only ambiguous listing was available
    if best.is_ambiguous_listing:
        conf_pts = max(0.5, conf_pts - 3.0)

    # 2. Sales volume score (max 5.0)
    if best.sales_count >= 1000:
        sales_pts = 5.0
    elif best.sales_count >= 300:
        sales_pts = 4.0
    elif best.sales_count >= 50:
        sales_pts = 2.5
    elif best.sales_count > 0:
        sales_pts = 1.0
    else:
        sales_pts = 0.0

    # 3. Commission rate score (max 4.0)
    if best.commission_rate_pct >= 8.0:
        comm_pts = 4.0
    elif best.commission_rate_pct >= 5.0:
        comm_pts = 3.0
    elif best.commission_rate_pct >= 2.0:
        comm_pts = 2.0
    elif best.commission_rate_pct > 0:
        comm_pts = 1.0
    else:
        comm_pts = 0.0

    total_bonus = round(min(15.0, conf_pts + sales_pts + comm_pts), 2)
    status: AffiliateStatus = (
        "READY_WITH_BEST_LINK" if not best.is_ambiguous_listing else "NEEDS_LINK_LOOKUP"
    )
    expl = (
        f"Affiliate {total_bonus}/15đ ({best.match_confidence}, "
        f"bán {best.sales_count}, HH {best.commission_rate_pct}%)"
    )
    return best, status, total_bonus, expl


def score_qualified_candidate(
    cand: DiscoveredCandidateSKU,
    median_vnd: int,
) -> Tuple[ScoreBreakdown100, AffiliateStatus, Optional[AffiliateListingMatch], float]:
    """
    Compute the deterministic 100-point score breakdown for a candidate SKU.
    Returns (score_breakdown, affiliate_status, best_offer, price_vs_median_ratio).
    """
    safe_median = max(median_vnd, 1000)
    ratio = round(cand.estimated_market_price_vnd / safe_median, 2)

    # --- Pillar 1: Price-to-Median Sweet Spot (20 pts) + Spec Completeness (15 pts) = 35 pts max ---
    if 0.65 <= ratio <= 1.25:
        price_pts = 20.0
        price_note = f"Giá {cand.estimated_market_price_vnd:,}đ nằm chuẩn vùng tối ưu ({ratio}x trung vị {safe_median:,}đ)"
    elif 1.25 < ratio <= 1.55:
        price_pts = 16.5
        price_note = f"Giá {cand.estimated_market_price_vnd:,}đ nhỉnh nhẹ ({ratio}x trung vị)"
    elif 1.55 < ratio <= 2.00:
        price_pts = 13.0
        price_note = f"Giá phân khúc cao ({ratio}x trung vị), kén khách phổ thông hơn"
    elif ratio > 2.00:
        price_pts = 9.0
        price_note = f"Giá cao gấp {ratio}x trung vị ngành hàng"
    else:
        price_pts = 12.0
        price_note = f"Giá rẻ ({ratio}x trung vị), cần xét kỹ chất liệu"

    has_clear_model = bool(
        cand.model_code
        and cand.model_code.strip()
        and cand.model_code.strip().lower() not in ("no-model", "none", "null", "generic")
    )
    model_pts = 5.0 if has_clear_model else 1.5
    specs_count = len(cand.key_specs)
    spec_pts = 5.0 if specs_count >= 3 else (3.5 if specs_count >= 2 else 2.0)
    pros_count = len(cand.highlight_pros_vi)
    pros_pts = 5.0 if pros_count >= 3 else (4.0 if pros_count == 2 else 2.5)

    pillar1 = round(min(35.0, price_pts + model_pts + spec_pts + pros_pts), 2)

    # --- Pillar 2: Durability & Caveat Manageability = 25 pts max ---
    is_branded = cand.archetype in ("BRANDED_MAINSTREAM", "BRANDED_PREMIUM")
    if cand.caveat_severity == "MINOR":
        pillar2 = 24.0 if is_branded else 17.5
        caveat_note = "Nhược điểm nhỏ/lưu ý sử dụng lành tính (MINOR)"
    elif cand.caveat_severity == "MODERATE":
        pillar2 = 15.5 if is_branded else 11.0
        caveat_note = "Nhược điểm cơ học/công năng mức trung bình (MODERATE)"
    else:
        pillar2 = 0.0
        caveat_note = "Lỗi nghiêm trọng (DEALBREAKER_DEFECT)"

    # --- Pillar 3: Visual Demo & Evidence Strength = 25 pts max ---
    yt_count = len(cand.youtube_evidence)
    if yt_count >= 3:
        yt_cnt_pts = 12.0
    elif yt_count == 2:
        yt_cnt_pts = 10.0
    elif yt_count == 1:
        yt_cnt_pts = 6.0
    else:
        yt_cnt_pts = 0.0

    valid_hd_clips = [
        c for c in cand.youtube_evidence if c.max_height >= 720 and c.duration_sec >= 12
    ]
    hd_pts = 6.0 if len(valid_hd_clips) >= 2 else (4.0 if len(valid_hd_clips) == 1 else 2.0)

    total_views = sum(c.view_count for c in cand.youtube_evidence)
    web_cnt = len(cand.web_evidence_snippets)
    ev_pts = 4.0 if web_cnt >= 2 else (2.5 if web_cnt == 1 else 1.0)
    if total_views >= 10000:
        ev_pts += 3.0
    elif total_views >= 1000:
        ev_pts += 2.0
    else:
        ev_pts += 1.0

    pillar3 = round(min(25.0, yt_cnt_pts + hd_pts + ev_pts), 2)

    # --- Pillar 4: Affiliate Readiness Bonus = 15 pts max ---
    best_offer, aff_status, pillar4, aff_note = _select_best_affiliate_offer(cand.affiliate_matches)

    total_score = round(pillar1 + pillar2 + pillar3 + pillar4, 2)
    explanation = (
        f"[Giá & Spec: {pillar1}/35đ ({price_note})] + "
        f"[Độ bền & Nhược điểm: {pillar2}/25đ ({caveat_note})] + "
        f"[Video & Bằng chứng: {pillar3}/25đ ({yt_count} YT clips)] + "
        f"[{aff_note}] => Tổng: {total_score}/100đ"
    )

    breakdown = ScoreBreakdown100(
        price_value_and_specs_score=pillar1,
        durability_and_caveat_score=pillar2,
        evidence_and_visual_score=pillar3,
        affiliate_readiness_bonus=pillar4,
        total_score_100=total_score,
        score_explanation_vi=explanation,
    )
    return breakdown, aff_status, best_offer, ratio


class M1SmartFilterEngine:
    """Executes Stage M1.1 (Hard Gate) -> M1.2 (100-Pt Scorer) -> M1.3 (LLM Comparative Verdict)."""

    def __init__(self, llm_client: Optional[FreeTierGeminiClient] = None) -> None:
        self.llm = llm_client or FreeTierGeminiClient()
        self.brand_gate = OfficialBrandVideoGate()

    def _filter_and_score_category(
        self, cat: FunctionalCategoryGroup
    ) -> Tuple[List[EvaluatedCandidateSKU], List[RejectedFoilSKU]]:
        rejected_foils: List[RejectedFoilSKU] = []
        surviving_candidates: List[DiscoveredCandidateSKU] = []

        # Step M1.1: Hard Disqualification Gate (including Official Brand Video Gate)
        for cand in cat.candidates:
            official_clips = self.brand_gate.filter_and_resolve_official_clips(
                brand=cand.brand,
                model_code=cand.model_code,
                candidate_id=cand.candidate_id,
                raw_clips=cand.youtube_evidence,
            )
            if cand.caveat_severity == "DEALBREAKER_DEFECT":
                rejected_foils.append(
                    RejectedFoilSKU(
                        candidate_id=cand.candidate_id,
                        brand=cand.brand,
                        model_code=cand.model_code,
                        product_name_vi=cand.product_name_vi,
                        archetype=cand.archetype,
                        estimated_market_price_vnd=cand.estimated_market_price_vnd,
                        caveat_severity=cand.caveat_severity,
                        honest_caveat_vi=cand.honest_caveat_vi,
                        rejection_gate="DEALBREAKER_DEFECT",
                        rejection_reason_vi=(
                            f"Bị loại thẳng ở vòng lọc độ bền do lỗi nghiêm trọng (DEALBREAKER_DEFECT): "
                            f"{cand.honest_caveat_vi}"
                        ),
                    )
                )
            elif cand.archetype == "MULTI_MODEL_AMBIGUOUS":
                rejected_foils.append(
                    RejectedFoilSKU(
                        candidate_id=cand.candidate_id,
                        brand=cand.brand,
                        model_code=cand.model_code,
                        product_name_vi=cand.product_name_vi,
                        archetype=cand.archetype,
                        estimated_market_price_vnd=cand.estimated_market_price_vnd,
                        caveat_severity=cand.caveat_severity,
                        honest_caveat_vi=cand.honest_caveat_vi,
                        rejection_gate="MULTI_MODEL_AMBIGUOUS",
                        rejection_reason_vi=(
                            "Bị loại do tin đăng gộp nhiều mã/dung tích lập lờ, dễ gây nhầm lẫn khi người xem đặt mua."
                        ),
                    )
                )
            elif len(cand.youtube_evidence) == 0:
                rejected_foils.append(
                    RejectedFoilSKU(
                        candidate_id=cand.candidate_id,
                        brand=cand.brand,
                        model_code=cand.model_code,
                        product_name_vi=cand.product_name_vi,
                        archetype=cand.archetype,
                        estimated_market_price_vnd=cand.estimated_market_price_vnd,
                        caveat_severity=cand.caveat_severity,
                        honest_caveat_vi=cand.honest_caveat_vi,
                        rejection_gate="INSUFFICIENT_VIDEO_EVIDENCE",
                        rejection_reason_vi="Không tìm thấy video thực tế trên YouTube để kiểm chứng chất lượng và làm tư liệu hình ảnh.",
                    )
                )
            elif cand.archetype != "OEM_GENERIC" and len(official_clips) == 0:
                rejected_foils.append(
                    RejectedFoilSKU(
                        candidate_id=cand.candidate_id,
                        brand=cand.brand,
                        model_code=cand.model_code,
                        product_name_vi=cand.product_name_vi,
                        archetype=cand.archetype,
                        estimated_market_price_vnd=cand.estimated_market_price_vnd,
                        caveat_severity=cand.caveat_severity,
                        honest_caveat_vi=cand.honest_caveat_vi,
                        rejection_gate="NO_CLEAN_OFFICIAL_FOOTAGE",
                        rejection_reason_vi=(
                            "Bị loại do không có video quảng cáo/giới thiệu chính hãng sạch từ nhà sản xuất "
                            "(chỉ có video người dùng tự quay hoặc kênh bán hàng bên thứ 3 dính logo/bản quyền)."
                        ),
                    )
                )
            else:
                cand_clean = cand.model_copy(update={"youtube_evidence": official_clips})
                surviving_candidates.append(cand_clean)

        # Separate OEM_GENERIC into rejected_foils if at least 2 branded candidates survived and OEM has inferior durability/specs
        branded_survivors = [
            c for c in surviving_candidates if c.archetype in ("BRANDED_MAINSTREAM", "BRANDED_PREMIUM")
        ]
        final_survivors: List[DiscoveredCandidateSKU] = []
        for cand in surviving_candidates:
            if cand.archetype == "OEM_GENERIC" and len(branded_survivors) >= 2:
                rejected_foils.append(
                    RejectedFoilSKU(
                        candidate_id=cand.candidate_id,
                        brand=cand.brand,
                        model_code=cand.model_code,
                        product_name_vi=cand.product_name_vi,
                        archetype=cand.archetype,
                        estimated_market_price_vnd=cand.estimated_market_price_vnd,
                        caveat_severity=cand.caveat_severity,
                        honest_caveat_vi=cand.honest_caveat_vi,
                        rejection_gate="LOW_TIER_OEM_FOIL",
                        rejection_reason_vi=(
                            f"Hàng OEM không thương hiệu (~{cand.estimated_market_price_vnd:,}đ) kém xa các hãng chính hãng về độ hoàn thiện: "
                            f"{cand.honest_caveat_vi}"
                        ),
                    )
                )
            else:
                final_survivors.append(cand)

        # Step M1.2: Objective 100-Point Scoring
        median_vnd = cat.observed_market_price_range_vnd.median_vnd
        scored_items: List[EvaluatedCandidateSKU] = []
        for cand in final_survivors:
            breakdown, aff_status, best_offer, ratio = score_qualified_candidate(cand, median_vnd)
            scored_items.append(
                EvaluatedCandidateSKU(
                    candidate_id=cand.candidate_id,
                    rank_in_category=1,  # Assigned after sorting
                    selection_role="ALTERNATIVE_QUALIFIED",
                    brand=cand.brand,
                    model_code=cand.model_code,
                    product_name_vi=cand.product_name_vi,
                    archetype=cand.archetype,
                    estimated_market_price_vnd=cand.estimated_market_price_vnd,
                    price_vs_median_ratio=ratio,
                    key_specs=cand.key_specs,
                    highlight_pros_vi=cand.highlight_pros_vi,
                    honest_caveat_vi=cand.honest_caveat_vi,
                    caveat_severity=cand.caveat_severity,
                    score_breakdown=breakdown,
                    affiliate_status=aff_status,
                    best_affiliate_offer=best_offer,
                    youtube_evidence=cand.youtube_evidence,
                    web_evidence_snippets=cand.web_evidence_snippets,
                )
            )

        # Sort by total_score_100 descending
        scored_items.sort(key=lambda x: x.score_breakdown.total_score_100, reverse=True)
        for idx, item in enumerate(scored_items, start=1):
            item.rank_in_category = idx
            if idx == 1:
                item.selection_role = "PRIMARY_WINNER"
            elif idx == 2:
                item.selection_role = "RUNNER_UP"
            else:
                item.selection_role = "ALTERNATIVE_QUALIFIED"

        return scored_items, rejected_foils

    def _synthesize_comparative_verdicts(
        self,
        pool: M0CandidatePoolOutput,
        category_tuples: List[
            Tuple[FunctionalCategoryGroup, List[EvaluatedCandidateSKU], List[RejectedFoilSKU]]
        ],
    ) -> Dict[str, HeadToHeadVerdict]:
        """Step M1.3: Call Gemini once to synthesize comparative head-to-head verdicts for all categories."""
        blocks: List[str] = []
        for cat, ranked, foils in category_tuples:
            winner = ranked[0]
            runner = ranked[1] if len(ranked) > 1 else None
            others = ranked[2:] if len(ranked) > 2 else []

            runner_str = (
                f"{runner.brand} ({runner.model_code or 'N/A'}) - {runner.product_name_vi} | "
                f"Giá ~{runner.estimated_market_price_vnd:,}đ | Điểm: {runner.score_breakdown.total_score_100}/100 | "
                f"Nhược điểm ({runner.caveat_severity}): {runner.honest_caveat_vi}"
                if runner
                else "(Không có Á quân)"
            )
            others_str = "; ".join(
                f"{o.brand} ({o.model_code or 'N/A'}, ~{o.estimated_market_price_vnd:,}đ, {o.score_breakdown.total_score_100}đ, nhược điểm: {o.honest_caveat_vi})"
                for o in others
            ) or "(Không có)"

            foils_str = "; ".join(
                f"{f.brand} ({f.model_code or 'OEM'}, ~{f.estimated_market_price_vnd:,}đ, lý do loại: {f.rejection_reason_vi})"
                for f in foils
            ) or "(Không có mẫu bị loại)"

            blocks.append(
                f"""
--- NHÓM CÔNG NĂNG: `{cat.category_id}` ({cat.category_name_vi}) ---
- Vấn đề cần giải quyết: {cat.problem_solved_vi}
- Phổ giá thị trường: Min {cat.observed_market_price_range_vnd.min_vnd:,}đ | Trung vị {cat.observed_market_price_range_vnd.median_vnd:,}đ | Max {cat.observed_market_price_range_vnd.max_vnd:,}đ
- QUÁN QUÂN (Hạng 1): `{winner.candidate_id}` — {winner.brand} ({winner.model_code or 'N/A'}) - {winner.product_name_vi}
  + Giá: ~{winner.estimated_market_price_vnd:,}đ ({winner.price_vs_median_ratio}x trung vị) | Điểm: {winner.score_breakdown.total_score_100}/100
  + Thông số: {winner.key_specs}
  + Ưu điểm: {winner.highlight_pros_vi}
  + Nhược điểm thực tế ({winner.caveat_severity}): {winner.honest_caveat_vi}
- Á QUÂN (Hạng 2): {runner_str}
- ỨNG VIÊN KHÁC ĐẠT CHUẨN: {others_str}
- MẪU ĐỐI CHỨNG NÊN TRÁNH (`rejected_foils`): {foils_str}
""".strip()
            )

        system_instruction = (
            "Bạn là Giám khảo Kỹ thuật Độc lập (Module M1.3) chuyên lập biên bản thẩm định và so sánh đa thương hiệu.\n"
            "Nhiệm vụ của bạn là giải thích RÕ RÀNG, THÔNG MINH, KHÁCH QUAN bằng số liệu thực tế:\n"
            "1. Tại sao chọn Quán quân (Hạng 1) dựa trên thông số kỹ thuật, vật liệu và tương quan giá trung vị.\n"
            "2. Tại sao chọn Quán quân thay vì Á quân (Hạng 2) hoặc các đối thủ cùng phân khúc (chỉ rõ điểm hơn/thua về cơ học, dung tích, độ tiện dụng hoặc giá bán).\n"
            "3. Tại sao người xem tuyệt đối không nên ham rẻ mua mẫu trong `rejected_foils` (hàng OEM/hàng lỗi).\n"
            "4. Hướng dẫn cách nói thẳng nhược điểm (`honest_caveat_vi`) của Quán quân để giữ uy tín tuyệt đối với người xem."
        )

        prompt = f"""
Ngách chủ đề: `{pool.sub_niche_id}` — {pool.sub_niche_name_vi}
Bối cảnh: {pool.problem_context_vi}

Dưới đây là kết quả chấm điểm khách quan trên thang 100 của từng nhóm công năng:

{chr(10).join(blocks)}

Hãy xuất JSON theo schema `LLMComparativeJudgeBatchResponse` gồm đầy đủ biên bản thẩm định (`verdicts`) cho từng `category_id`.
""".strip()

        batch_resp, _ = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMComparativeJudgeBatchResponse,
            system_instruction=system_instruction,
            prefer_lite=True,
            temperature=0.2,
        )

        verdict_map: Dict[str, HeadToHeadVerdict] = {}
        for v in batch_resp.verdicts:
            verdict_map[v.category_id] = HeadToHeadVerdict(
                winner_candidate_id=v.winner_candidate_id,
                winner_summary_title_vi=v.winner_summary_title_vi,
                why_winner_chosen_vi=v.why_winner_chosen_vi,
                why_winner_beats_runner_up_vi=v.why_winner_beats_runner_up_vi,
                why_avoid_rejected_foils_vi=v.why_avoid_rejected_foils_vi,
                honest_caveat_framing_vi=v.honest_caveat_framing_vi,
            )
        return verdict_map

    def run(
        self,
        input_path: Path = INPUT_M0_POOL_PATH,
        output_path: Path = OUTPUT_M1_PATH,
    ) -> M1QualifiedSKUsOutput:
        if not input_path.exists():
            raise FileNotFoundError(f"M1 input contract not found: {input_path}")

        print(f"[M1] 1/3 Reading M0 contract from: {input_path}...", flush=True)
        pool = M0CandidatePoolOutput.model_validate_json(
            input_path.read_text(encoding="utf-8")
        )

        print(
            f"[M1] 2/3 Running Hard Disqualification Gate & 100-Point Scorer across "
            f"{len(pool.functional_categories)} categories...",
            flush=True,
        )

        category_tuples: List[
            Tuple[FunctionalCategoryGroup, List[EvaluatedCandidateSKU], List[RejectedFoilSKU]]
        ] = []
        used_winner_keys: set[str] = set()

        for cat in pool.functional_categories:
            ranked, foils = self._filter_and_score_category(cat)
            if not ranked:
                print(
                    f"     -> [M1 SKIP] Category {cat.category_id} has 0 surviving candidates after M1.1 hard gate — skipping gracefully.",
                    flush=True,
                )
                continue

            # Cross-category winner deduplication: ensure two different categories do not crown the exact same model
            def _sku_dedup_key(item: EvaluatedCandidateSKU) -> str:
                import re as _re
                b = item.brand.strip().lower()
                m = (item.model_code or "").strip().lower()
                # Normalize color suffix on model codes (e.g. cks321grn -> cks321)
                m_base = _re.sub(r"^([a-z]{2,4}\d{3,4})[a-z]+$", r"\1", m)
                return f"{b}::{m_base or item.product_name_vi.strip().lower()}"

            if len(ranked) > 1:
                non_dup = [it for it in ranked if _sku_dedup_key(it) not in used_winner_keys]
                if non_dup:
                    ranked = non_dup
                    for idx, it in enumerate(ranked, start=1):
                        it.rank_in_category = idx
                        it.selection_role = (
                            "PRIMARY_WINNER"
                            if idx == 1
                            else ("RUNNER_UP" if idx == 2 else "ALTERNATIVE_QUALIFIED")
                        )

            winner = ranked[0]
            used_winner_keys.add(_sku_dedup_key(winner))
            print(
                f"     -> [{cat.category_id}] Winner: {winner.brand} ({winner.model_code}) "
                f"with {winner.score_breakdown.total_score_100}/100 pts | "
                f"Qualified={len(ranked)} | Rejected Foils={len(foils)}",
                flush=True,
            )
            category_tuples.append((cat, ranked, foils))

        if not category_tuples:
            raise RuntimeError("All categories in M0 candidate pool had 0 surviving candidates after M1.1 hard gate!")

        print(
            "[M1] 3/3 Synthesizing Head-to-Head Multi-Brand Comparative Verdicts via Gemini (1 call)...",
            flush=True,
        )
        verdict_map = self._synthesize_comparative_verdicts(pool, category_tuples)

        qualified_results: List[QualifiedCategoryResult] = []
        for cat, ranked, foils in category_tuples:
            winner = ranked[0]
            runner = ranked[1] if len(ranked) > 1 else None
            verdict = verdict_map.get(cat.category_id)
            if not verdict:
                verdict = HeadToHeadVerdict(
                    winner_candidate_id=winner.candidate_id,
                    winner_summary_title_vi=f"Quán quân {cat.category_name_vi}: {winner.brand} {winner.model_code or ''}".strip(),
                    why_winner_chosen_vi=winner.score_breakdown.score_explanation_vi,
                    why_winner_beats_runner_up_vi="Đạt tổng điểm thông số, độ bền và bằng chứng thực tế cao nhất trong nhóm.",
                    why_avoid_rejected_foils_vi=foils[0].rejection_reason_vi if foils else "Tránh các mẫu giá rẻ không rõ nguồn gốc.",
                    honest_caveat_framing_vi=winner.honest_caveat_vi,
                )

            qualified_results.append(
                QualifiedCategoryResult(
                    category_id=cat.category_id,
                    domain=cat.domain,
                    category_name_vi=cat.category_name_vi,
                    problem_solved_vi=cat.problem_solved_vi,
                    observed_market_price_range_vnd=cat.observed_market_price_range_vnd,
                    category_confidence_score=winner.score_breakdown.total_score_100,
                    primary_winner=winner,
                    runner_up=runner,
                    all_qualified_ranked=ranked,
                    rejected_foils=foils,
                    head_to_head_verdict=verdict,
                )
            )

        now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        m1_run_id = f"m1_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{pool.sub_niche_id}"

        output_obj = M1QualifiedSKUsOutput(
            m1_run_id=m1_run_id,
            source_m0_run_id=pool.run_id,
            generated_at=now_iso,
            domain=pool.domain,
            domain_name_vi=pool.domain_name_vi,
            sub_niche_id=pool.sub_niche_id,
            sub_niche_name_vi=pool.sub_niche_name_vi,
            problem_context_vi=pool.problem_context_vi,
            llm_models_used=self.llm.models_used_history,
            qualified_categories=qualified_results,
        )

        atomic_write_text(output_path, output_obj.model_dump_json(indent=2))
        print(f"[M1] Completed! Wrote qualified SKUs to: {output_path}", flush=True)
        return output_obj


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Module M1: Smart Filter & Multi-Brand Ranker.")
    parser.add_argument(
        "--input",
        type=Path,
        default=INPUT_M0_POOL_PATH,
        help="Input JSON path from M0 (default: data/m0_candidate_pool.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_M1_PATH,
        help="Output JSON path for M2 (default: data/m1_qualified_skus.json)",
    )
    args = parser.parse_args()

    engine = M1SmartFilterEngine()
    engine.run(input_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
