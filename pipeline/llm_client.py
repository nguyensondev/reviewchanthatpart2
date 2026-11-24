"""
Gemini Free-Tier LLM Client & Auto-Model Detector (`pipeline/llm_client.py`).

Empirically verified on the user's Free-Tier key (Nov 2026):
  - `gemini-3.5-flash-lite` : PASS (2.1s latency on nested Pydantic schemas, high RPD quota)
  - `gemini-3.1-flash-lite` : PASS (Fast secondary Lite model)
  - `gemini-3.8-flash`      : PASS (Fallback, slower on nested response_schema compilation)
  - `gemma-4-31b-it`        : PASS (Separate Quota Pool Fallback)
  - `gemini-2.5-*`          : 404 NOT_FOUND (Deprecated Nov 2026 — excluded)
"""
from __future__ import annotations

import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Type, TypeVar

from google import genai
from google.genai import types
from pydantic import BaseModel

from pipeline.shared_utils import check_vietnamese_diacritics

T = TypeVar("T", bound=BaseModel)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

# Verified working model chains: prioritize Full-Flash for deep reasoning, with Lite models ONLY as last-resort 429/503 fallback
REASONING_MODEL_CHAIN: List[str] = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.5-flash",
    "gemini-3-flash-preview",
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
]

EXTRACTION_MODEL_CHAIN: List[str] = [
    "gemini-3.8-flash",
    "gemini-3.7-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.5-flash",
    "gemini-3-flash-preview",
    "gemini-3.1-flash-lite",
]

MANDATORY_VI_DIACRITIC_RULE = (
    "\n\n[QUY TẮC NGÔN NGỮ BẮT BUỘC — ZERO TOLERANCE]: "
    "Tất cả mọi trường văn bản tiếng Việt (`*_vi`, lời thoại `voiceover_vi`, `tts_voiceover_vi`, "
    "`subtitle_text_vi`, `on_screen_text_vi`, tiêu đề, thông số, ưu nhược điểm) BẮT BUỘC phải viết bằng "
    "TIẾNG VIỆT CÓ DẤU ĐẦY ĐỦ CHUẨN UTF-8 (ví dụ: 'Chiếc đèn treo màn hình chống chói bảo vệ mắt'). "
    "TUYỆT ĐỐI CẤM viết tiếng Việt không dấu (ASCII) dù chỉ một câu hay một cụm từ!"
)


MIXED_EN_PRODUCT_FIELDS = {
    "product_name_vi",
    "specific_youtube_search_query_vi",
    "generic_web_queries_vi",
    "generic_youtube_queries_vi",
    "key_specs_pairs",
}


def _validate_pydantic_vietnamese_fields(obj: object, path_prefix: str = "") -> List[str]:
    """
    Recursively inspect every `*_vi` string/list field in a parsed Pydantic response.
    Returns a list of human-readable violation strings if any field contains unaccented Vietnamese.
    """
    violations: List[str] = []
    if isinstance(obj, BaseModel):
        for fname in obj.__class__.model_fields:
            val = getattr(obj, fname, None)
            sub_path = f"{path_prefix}.{fname}" if path_prefix else fname
            if fname.endswith("_vi") or fname in ("key_specs_pairs",):
                allow_en = fname in MIXED_EN_PRODUCT_FIELDS
                if isinstance(val, str) and val.strip():
                    ok, _, reason = check_vietnamese_diacritics(
                        val, allow_english_product_names=allow_en
                    )
                    if not ok:
                        violations.append(f"{sub_path}: {reason} -> {val[:80]!r}")
                elif isinstance(val, list):
                    for idx, item in enumerate(val):
                        if isinstance(item, str) and item.strip():
                            ok, _, reason = check_vietnamese_diacritics(
                                item, allow_english_product_names=allow_en
                            )
                            if not ok:
                                violations.append(f"{sub_path}[{idx}]: {reason} -> {item[:80]!r}")
                        else:
                            violations.extend(_validate_pydantic_vietnamese_fields(item, f"{sub_path}[{idx}]"))
            else:
                violations.extend(_validate_pydantic_vietnamese_fields(val, sub_path))
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            violations.extend(_validate_pydantic_vietnamese_fields(item, f"{path_prefix}[{idx}]"))
    elif isinstance(obj, dict):
        for k, val in obj.items():
            violations.extend(_validate_pydantic_vietnamese_fields(val, f"{path_prefix}.{k}"))
    return violations


def load_env_file(env_path: Path = DEFAULT_ENV_PATH) -> None:
    """Load key=value pairs from .env into os.environ without external dependencies."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip().strip("'").strip('"')
        if k and v and k not in os.environ:
            os.environ[k] = v


def get_api_keys(env_path: Path = DEFAULT_ENV_PATH) -> List[str]:
    """Retrieve one or more Gemini API keys from GEMINI_API_KEYS or GEMINI_API_KEY."""
    load_env_file(env_path)
    raw = os.environ.get("GEMINI_API_KEYS") or os.environ.get("GEMINI_API_KEY") or ""
    keys = [k.strip() for k in re.split(r"[,;\s]+", raw) if k.strip()]
    if not keys:
        raise RuntimeError(
            f"Missing GEMINI_API_KEYS / GEMINI_API_KEY in environment or {env_path}"
        )
    return keys


def _clean_json_text(raw_text: str) -> str:
    """Strip trailing/leading markdown code fences if a fallback model (e.g. gemma-4-31b-it) emits them."""
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```+$", "", text)
    first_brace = text.find("{")
    last_brace = text.rfind("}")
    if first_brace != -1 and last_brace != -1 and last_brace > first_brace:
        return text[first_brace : last_brace + 1]
    return text


class FreeTierGeminiClient:
    """Rate-limited, fault-tolerant Gemini Free-Tier client with strict HTTP timeouts & model rotation."""

    def __init__(
        self,
        min_interval_sec: float = 2.0,
        timeout_ms: int = 120000,
        env_path: Path = DEFAULT_ENV_PATH,
    ) -> None:
        self.api_keys = get_api_keys(env_path)
        self._key_idx = 0
        self.min_interval_sec = min_interval_sec
        self.timeout_ms = timeout_ms
        self._last_call_ts = 0.0
        self.models_used_history: List[str] = []

    def _get_client(self) -> genai.Client:
        key = self.api_keys[self._key_idx % len(self.api_keys)]
        return genai.Client(
            api_key=key,
            http_options=types.HttpOptions(timeout=self.timeout_ms),
        )

    def _rotate_key(self) -> None:
        if len(self.api_keys) > 1:
            self._key_idx = (self._key_idx + 1) % len(self.api_keys)

    def _pace(self) -> None:
        elapsed = time.time() - self._last_call_ts
        if self._last_call_ts > 0 and elapsed < self.min_interval_sec:
            time.sleep(self.min_interval_sec - elapsed)

    def detect_available_free_models(self) -> Dict[str, bool]:
        """List models visible to the API key and verify our Free-Tier candidate pool."""
        client = self._get_client()
        listed_names = set()
        for m in client.models.list():
            name = (m.name or "").replace("models/", "")
            listed_names.add(name)
        status: Dict[str, bool] = {}
        for candidate in REASONING_MODEL_CHAIN:
            status[candidate] = candidate in listed_names
        return status

    def generate_structured(
        self,
        prompt: str,
        schema: Type[T],
        system_instruction: Optional[str] = None,
        prefer_lite: bool = False,
        temperature: float = 0.2,
        image_bytes_list: Optional[List[bytes]] = None,
    ) -> Tuple[T, str]:
        """
        Generate a strictly validated Pydantic model instance using the Free-Tier model chain.
        Supports multimodal JPEG image inspection when `image_bytes_list` is supplied.
        Returns (validated_pydantic_object, model_name_used).
        """
        chain = EXTRACTION_MODEL_CHAIN if prefer_lite else REASONING_MODEL_CHAIN
        errors: List[str] = []

        for model_name in chain:
            last_diacritic_err = ""
            for attempt in range(2):
                self._pace()
                client = self._get_client()
                try:
                    full_sys = (system_instruction or "").strip() + MANDATORY_VI_DIACRITIC_RULE
                    effective_temp = temperature if attempt == 0 else max(0.1, temperature - 0.1)
                    cfg = types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema,
                        temperature=effective_temp,
                        system_instruction=full_sys,
                    )
                    if "flash" in model_name and "lite" not in model_name:
                        cfg.thinking_config = types.ThinkingConfig(thinking_budget=1024)
                    effective_prompt = prompt
                    if last_diacritic_err:
                        effective_prompt = (
                            f"⚠️ CẢNH BÁO SỬA LỖI NGHIÊM TRỌNG: Lần sinh trước bị TỪ CHỐI vì viết tiếng Việt KHÔNG DẤU ({last_diacritic_err}). "
                            f"BẮT BUỘC viết lại TOÀN BỘ các trường tiếng Việt CÓ DẤU ĐẦY ĐỦ CHUẨN UTF-8!\n\n{prompt}"
                        )

                    if image_bytes_list:
                        contents_payload = [
                            *(types.Part.from_bytes(data=b, mime_type="image/jpeg") for b in image_bytes_list if b),
                            effective_prompt,
                        ]
                    else:
                        contents_payload = effective_prompt  # type: ignore[assignment]

                    self._last_call_ts = time.time()
                    resp = client.models.generate_content(
                        model=model_name,
                        contents=contents_payload,
                        config=cfg,
                    )
                    raw_text = resp.text or ""
                    cleaned = _clean_json_text(raw_text)
                    parsed = schema.model_validate_json(cleaned)
                    diacritic_violations = _validate_pydantic_vietnamese_fields(parsed)
                    if diacritic_violations:
                        last_diacritic_err = diacritic_violations[0]
                        raise ValueError(f"DIACRITIC_REJECTED: {last_diacritic_err}")

                    if model_name not in self.models_used_history:
                        self.models_used_history.append(model_name)
                    return parsed, model_name
                except Exception as exc:
                    err_str = str(exc).splitlines()[0][:240]
                    errors.append(f"{model_name} (try {attempt+1}): {err_str}")
                    if "DIACRITIC_REJECTED" in err_str:
                        print(f"[LLM DIACRITIC GUARD] 🚫 Chặn đầu ra mất dấu từ {model_name}: {err_str}", flush=True)
                        continue
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        self._rotate_key()
                        time.sleep(3.0)
                    elif any(code in err_str for code in ("503", "504", "UNAVAILABLE", "DEADLINE_EXCEEDED")):
                        time.sleep(2.0)
                    else:
                        break

        raise RuntimeError(
            "All Free-Tier Gemini models failed for structured generation:\n  - "
            + "\n  - ".join(errors)
        )
