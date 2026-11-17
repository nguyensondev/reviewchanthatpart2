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

T = TypeVar("T", bound=BaseModel)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"

# Verified working Free-Tier model chains (ordered by quality & reliability on nested schemas)
REASONING_MODEL_CHAIN: List[str] = [
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-3.1-flash-lite",
    "gemma-4-31b-it",
]

EXTRACTION_MODEL_CHAIN: List[str] = [
    "gemini-3.5-flash-lite",
    "gemini-3.5-flash",
    "gemini-3.1-flash-lite",
    "gemma-4-31b-it",
]


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
        timeout_ms: int = 45000,
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
    ) -> Tuple[T, str]:
        """
        Generate a strictly validated Pydantic model instance using the Free-Tier model chain.
        Returns (validated_pydantic_object, model_name_used).
        """
        chain = EXTRACTION_MODEL_CHAIN if prefer_lite else REASONING_MODEL_CHAIN
        errors: List[str] = []

        for model_name in chain:
            for attempt in range(2):
                self._pace()
                client = self._get_client()
                try:
                    cfg = types.GenerateContentConfig(
                        response_mime_type="application/json",
                        response_schema=schema,
                        temperature=temperature,
                    )
                    if system_instruction:
                        cfg.system_instruction = system_instruction

                    self._last_call_ts = time.time()
                    resp = client.models.generate_content(
                        model=model_name,
                        contents=prompt,
                        config=cfg,
                    )
                    raw_text = resp.text or ""
                    cleaned = _clean_json_text(raw_text)
                    parsed = schema.model_validate_json(cleaned)
                    if model_name not in self.models_used_history:
                        self.models_used_history.append(model_name)
                    return parsed, model_name
                except Exception as exc:
                    err_str = str(exc).splitlines()[0][:200]
                    errors.append(f"{model_name} (try {attempt+1}): {err_str}")
                    if "429" in err_str or "RESOURCE_EXHAUSTED" in err_str:
                        self._rotate_key()
                        time.sleep(3.0)
                    elif "503" in err_str or "UNAVAILABLE" in err_str:
                        time.sleep(2.0)
                    else:
                        break

        raise RuntimeError(
            "All Free-Tier Gemini models failed for structured generation:\n  - "
            + "\n  - ".join(errors)
        )
