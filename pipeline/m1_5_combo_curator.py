"""
Module M1.5: Independent Combo Curator & Flexible SKU Lifecycle Manager (`pipeline/m1_5_combo_curator.py`).

Responsibilities (User Requirements #1 & #3):
  1. Completely decoupled from M0/M1 search logic and M2 script drafting:
     - M0 and M1 never search for artificial "combo keywords".
     - M1.5 maintains the persistent SKU Lifecycle Registry (`data/sku_registry.json`) keyed by
       normalized `brand + model_code` (`sku_key`).
  2. Flexible Solo / Combo Order (Non-Rigid Lifecycle):
     - A SKU with `has_solo_video == False` is eligible for a Solo script (`SINGLE_SKU_DEEP_DIVE`),
       even if it has already appeared in a Combo video (`COMBO_RENDERED_SOLO_OPEN`).
     - A SKU with `has_combo_video == False` is eligible for a Combo script (`COMBO_WORKFLOW_SYNERGY`),
       even if it has already appeared in a Solo video (`SOLO_RENDERED_COMBO_OPEN`) or has not had a Solo video yet (`ACTIVE_UNRENDERED`).
     - Once a SKU has BOTH `has_solo_video == True` AND `has_combo_video == True` (recorded ONLY when M4
       finishes rendering the video to `RENDERED` status), it transitions to `RETIRED_EXHAUSTED` and is
       permanently excluded from M0, M1.5, and M2.
  3. Strict Anti-"Râu Ông Nọ Cắm Cằm Bà Kia" Combo Evaluation:
     - Hard Rule 1: 2 to 4 SKUs max per combo (`2 <= count <= 4`).
     - Hard Rule 2: All SKUs in a combo MUST share the same `domain` (`CLEANING_CARE` or `MINI_APPLIANCE`)
       and the same `micro_scene_id` enum.
     - Hard Rule 3: All SKUs in a combo MUST belong to DISTINCT `category_id`s (never two products of
       the same functional type).
     - LLM Concrete Scenario Check: Verifies that the SKUs interact naturally in a single workflow scene
       (`joint_workflow_scenario_vi`), or returns `status = "BYPASS"` so M2 cleanly skips combo generation.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import random
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pipeline.brand_video_gate import OfficialBrandVideoGate
from pipeline.core_resilience import atomic_write_text
from pipeline.llm_client import FreeTierGeminiClient
from pipeline.schemas_m0 import DomainId
from pipeline.schemas_m1 import EvaluatedCandidateSKU, M1QualifiedSKUsOutput, QualifiedCategoryResult
from pipeline.schemas_m1_5 import (
    ComboMemberRoleItem,
    CuratedComboGroup,
    LLMComboEvaluationResponse,
    M15ComboCuratorOutput,
    MicroSceneId,
    SKURegistryEntry,
    compute_sku_lifecycle_status,
    normalize_sku_key,
)
from pipeline.shared_utils import check_vietnamese_diacritics, enforce_short_hook_title
from pipeline.sku_registry import SKURegistryManager, classify_micro_scene

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
INPUT_M1_PATH = DATA_DIR / "m1_qualified_skus.json"
SKU_REGISTRY_PATH = DATA_DIR / "sku_registry.json"
OUTPUT_M1_5_PATH = DATA_DIR / "m1_5_combo_result.json"

MIN_SKU_COOLDOWN_DAYS = 3
MAX_SKU_COOLDOWN_DAYS = 7


class M15ComboCuratorEngine:
    """Evaluates combo-eligible SKUs and outputs `data/m1_5_combo_result.json` for Module M2."""

    def __init__(
        self,
        llm_client: Optional[FreeTierGeminiClient] = None,
        registry_mgr: Optional[SKURegistryManager] = None,
    ) -> None:
        self.llm = llm_client or FreeTierGeminiClient()
        self.registry_mgr = registry_mgr or SKURegistryManager()

    def _find_candidate_combo_clusters(
        self, entries: Dict[str, SKURegistryEntry]
    ) -> List[Tuple[DomainId, MicroSceneId, List[SKURegistryEntry]]]:
        """
        Group combo-eligible SKUs (`has_combo_video == False`, `lifecycle_status != 'RETIRED_EXHAUSTED'`,
        and NOT currently in 3-7 day cooldown) by `(domain, micro_scene_id)`, keeping at most 1
        highest-scoring SKU per `category_id`.
        """
        by_scene: Dict[Tuple[DomainId, MicroSceneId], Dict[str, SKURegistryEntry]] = {}
        for entry in entries.values():
            if entry.has_combo_video or entry.lifecycle_status == "RETIRED_EXHAUSTED":
                continue
            if self.registry_mgr.is_in_cooldown(entry.sku_key):
                continue
            if not entry.youtube_evidence:
                continue
            ok_n, _, _ = check_vietnamese_diacritics(entry.product_name_vi)
            ok_c, _, _ = check_vietnamese_diacritics(entry.honest_caveat_vi)
            if not (ok_n and ok_c):
                continue
            scene_key = (entry.domain, entry.micro_scene_id)
            cat_map = by_scene.setdefault(scene_key, {})
            existing = cat_map.get(entry.category_id)
            if existing is None or entry.m1_total_score_100 > existing.m1_total_score_100:
                cat_map[entry.category_id] = entry

        clusters: List[Tuple[DomainId, MicroSceneId, List[SKURegistryEntry]]] = []
        for (domain, micro_scene), cat_map in by_scene.items():
            members = sorted(
                cat_map.values(),
                key=lambda e: e.m1_total_score_100,
                reverse=True,
            )[:4]
            if len(members) >= 2:
                clusters.append((domain, micro_scene, members))

        # Sort clusters by member count desc, then average M1 score desc
        clusters.sort(
            key=lambda item: (
                len(item[2]),
                sum(m.m1_total_score_100 for m in item[2]) / len(item[2]),
            ),
            reverse=True,
        )
        return clusters

    def curate_from_m1(self, m1_data: M1QualifiedSKUsOutput) -> M15ComboCuratorOutput:
        self.registry_mgr.upsert_from_m1(m1_data)
        all_entries = self.registry_mgr.get_all_entries()

        cooldown_keys = [
            k for k in all_entries if self.registry_mgr.is_in_cooldown(k)
        ]
        solo_eligible_keys = [
            k
            for k, e in all_entries.items()
            if not e.has_solo_video
            and e.lifecycle_status != "RETIRED_EXHAUSTED"
            and k not in cooldown_keys
        ]
        combo_eligible_keys = [
            k
            for k, e in all_entries.items()
            if not e.has_combo_video
            and e.lifecycle_status != "RETIRED_EXHAUSTED"
            and k not in cooldown_keys
        ]
        retired_keys = [
            k for k, e in all_entries.items() if e.lifecycle_status == "RETIRED_EXHAUSTED"
        ]

        now_iso = dt.datetime.now(dt.timezone.utc).astimezone().isoformat(timespec="seconds")
        run_id = f"m1_5_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{m1_data.sub_niche_id}"

        clusters = self._find_candidate_combo_clusters(all_entries)
        if not clusters:
            return M15ComboCuratorOutput(
                m1_5_run_id=run_id,
                source_m1_run_id=m1_data.m1_run_id,
                generated_at=now_iso,
                status="BYPASS",
                bypass_reason_vi=(
                    "Không có đủ từ 2 đến 4 SKU khác nhóm công năng (`category_id`) trong cùng một bối cảnh "
                    "sử dụng (`domain` + `micro_scene_id`) chưa làm video combo (hoặc các SKU đang trong kỳ nghỉ 3-7 ngày) -> Tự động BYPASS để tránh ghép khiên cưỡng."
                ),
                active_solo_eligible_sku_keys=solo_eligible_keys,
                active_combo_eligible_sku_keys=combo_eligible_keys,
                retired_exhausted_sku_keys=retired_keys,
                cooldown_resting_sku_keys=cooldown_keys,
                selected_combo=None,
            )

        best_domain, best_scene, cluster_members = clusters[0]
        sku_lines: List[str] = []
        for idx, m in enumerate(cluster_members, start=1):
            specs_str = ", ".join(f"{k}: {v}" for k, v in m.key_specs.items())
            sku_lines.append(
                f"{idx}. `sku_key = \"{m.sku_key}\"` (candidate_id: `{m.candidate_id}`)\n"
                f"   - Sản phẩm: {m.brand} {m.model_code or ''} — {m.product_name_vi}\n"
                f"   - Nhóm công năng (`category_id`): `{m.category_id}` ({m.category_name_vi})\n"
                f"   - Giá chuẩn: ~{m.estimated_market_price_vnd:,}đ | Điểm M1: {m.m1_total_score_100}/100\n"
                f"   - Thông số: {specs_str}\n"
                f"   - Ưu điểm: {'; '.join(m.highlight_pros_vi)}\n"
                f"   - Nhược điểm thật: {m.honest_caveat_vi}"
            )

        system_instruction = (
            "Bạn là Chuyên gia Thẩm định Combo Sản phẩm Độc lập (Module M1.5).\n"
            "NHIỆM VỤ CỐT LÕI:\n"
            "1. Đánh giá xem nhóm 2–4 SKU dưới đây (đã cùng thuộc 1 `domain` và 1 `micro_scene_id`, khác `category_id`) "
            "có thực sự phối hợp tự nhiên trong CÙNG MỘT TÌNH HUỐNG SỬ DỤNG THỰC TẾ hay không.\n"
            "2. TUYỆT ĐỐI CẤM 'lấy râu ông nọ cắm cằm bà kia': Nếu các món đồ rời rạc, không liên quan thao tác hoặc ghép vào làm kịch bản khiên cưỡng, "
            "BẮT BUỘC trả về `should_create_combo = false` và nêu rõ `bypass_reason_vi`.\n"
            "3. Nếu các món thực sự bổ trợ hoàn hảo trong 1 quy trình/không gian (ví dụ: Bàn ủi hơi nước cầm tay Philips chống nhăn + Máy cắt lông xù quần áo Xiaomi chăm sóc đồ mặc đi làm; hoặc Chổi cọ điện xoay HOTO vệ sinh nhà tắm + Máy tạo bọt xà phòng cảm ứng tự động), "
            "hãy trả về `should_create_combo = true`, chọn từ 2 đến tối đa 4 SKU (`selected_members`), viết kịch bản tình huống phối hợp cụ thể (`joint_workflow_scenario_vi`), "
            "và đặt `hook_title_short_vi` NGẮN GỌN TỐI ĐA 30 KÝ TỰ (có dấu tiếng Việt chuẩn)."
        )

        prompt = f"""
Bối cảnh thẩm định Combo:
- Domain: `{best_domain}`
- Micro-Scene: `{best_scene}`
- Danh sách ứng viên đủ điều kiện (`has_combo_video = False`, có video chính hãng sạch):

{chr(10).join(sku_lines)}

Hãy đánh giá khách quan và xuất JSON theo schema `LLMComboEvaluationResponse`.
""".strip()

        eval_resp, _ = self.llm.generate_structured(
            prompt=prompt,
            schema=LLMComboEvaluationResponse,
            system_instruction=system_instruction,
            prefer_lite=True,
            temperature=0.15,
        )

        member_by_key = {m.sku_key: m for m in cluster_members}
        valid_decisions = [
            d for d in eval_resp.selected_members if d.sku_key in member_by_key
        ]
        # Deduplicate by category_id and cap to 2..4 members
        seen_cats: set[str] = set()
        dedup_decisions = []
        for d in sorted(valid_decisions, key=lambda x: x.step_order):
            cat_id = member_by_key[d.sku_key].category_id
            if cat_id not in seen_cats:
                seen_cats.add(cat_id)
                dedup_decisions.append(d)
        dedup_decisions = dedup_decisions[:4]

        if not eval_resp.should_create_combo or len(dedup_decisions) < 2:
            return M15ComboCuratorOutput(
                m1_5_run_id=run_id,
                source_m1_run_id=m1_data.m1_run_id,
                generated_at=now_iso,
                status="BYPASS",
                bypass_reason_vi=(
                    eval_resp.bypass_reason_vi.strip()
                    or "LLM đánh giá các SKU hiện tại chưa tạo thành chuỗi thao tác combo đủ tự nhiên -> BYPASS."
                ),
                active_solo_eligible_sku_keys=solo_eligible_keys,
                active_combo_eligible_sku_keys=combo_eligible_keys,
                retired_exhausted_sku_keys=retired_keys,
                cooldown_resting_sku_keys=cooldown_keys,
                selected_combo=None,
            )

        combo_members: List[ComboMemberRoleItem] = []
        total_price = 0
        for step_idx, dec in enumerate(dedup_decisions, start=1):
            entry = member_by_key[dec.sku_key]
            total_price += entry.estimated_market_price_vnd
            combo_members.append(
                ComboMemberRoleItem(
                    sku_key=entry.sku_key,
                    candidate_id=entry.candidate_id,
                    brand=entry.brand,
                    model_code=entry.model_code,
                    product_name_vi=entry.product_name_vi,
                    category_id=entry.category_id,
                    estimated_market_price_vnd=entry.estimated_market_price_vnd,
                    step_order=step_idx,
                    role_in_combo_vi=dec.role_in_combo_vi.strip(),
                    honest_caveat_vi=entry.honest_caveat_vi,
                    youtube_evidence=entry.youtube_evidence,
                    best_affiliate_offer=entry.best_affiliate_offer,
                )
            )

        combo_slug = "_".join(m.sku_key for m in combo_members[:3])
        short_hook = enforce_short_hook_title(
            eval_resp.hook_title_short_vi or "COMBO DỌN NHÀ NHÀN TÊNH", max_chars=30
        )

        curated_group = CuratedComboGroup(
            combo_id=f"combo_{best_scene.lower()}_{combo_slug}"[:72],
            domain=best_domain,
            micro_scene_id=best_scene,
            combo_title_vi=(
                eval_resp.combo_title_vi.strip()
                or f"Bộ đôi nâng cấp {best_scene}: {', '.join(m.brand + ' ' + (m.model_code or '') for m in combo_members)}"
            ),
            hook_title_short_vi=short_hook,
            joint_workflow_scenario_vi=eval_resp.joint_workflow_scenario_vi.strip(),
            why_not_forced_vi=eval_resp.why_not_forced_vi.strip(),
            total_combo_price_vnd=total_price,
            members=combo_members,
        )

        return M15ComboCuratorOutput(
            m1_5_run_id=run_id,
            source_m1_run_id=m1_data.m1_run_id,
            generated_at=now_iso,
            status="COMBO_APPROVED",
            bypass_reason_vi="",
            active_solo_eligible_sku_keys=solo_eligible_keys,
            active_combo_eligible_sku_keys=combo_eligible_keys,
            retired_exhausted_sku_keys=retired_keys,
            cooldown_resting_sku_keys=cooldown_keys,
            selected_combo=curated_group,
        )

    def run(
        self,
        input_m1_path: Path = INPUT_M1_PATH,
        output_path: Path = OUTPUT_M1_5_PATH,
    ) -> M15ComboCuratorOutput:
        if not input_m1_path.exists():
            raise FileNotFoundError(f"M1.5 input M1 contract not found: {input_m1_path}")

        print(f"[M1.5] Reading M1 qualified SKUs from: {input_m1_path}...", flush=True)
        m1_data = M1QualifiedSKUsOutput.model_validate_json(
            input_m1_path.read_text(encoding="utf-8")
        )
        result = self.curate_from_m1(m1_data)
        atomic_write_text(output_path, result.model_dump_json(indent=2))
        print(
            f"[M1.5] Status={result.status} | SoloEligible={len(result.active_solo_eligible_sku_keys)} | "
            f"ComboEligible={len(result.active_combo_eligible_sku_keys)} | Retired={len(result.retired_exhausted_sku_keys)} "
            f"-> {output_path}",
            flush=True,
        )
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Module M1.5: Independent Combo Curator & SKU Lifecycle Manager.")
    parser.add_argument("--input", type=Path, default=INPUT_M1_PATH)
    parser.add_argument("--output", type=Path, default=OUTPUT_M1_5_PATH)
    args = parser.parse_args()

    engine = M15ComboCuratorEngine()
    engine.run(input_m1_path=args.input, output_path=args.output)


if __name__ == "__main__":
    main()
