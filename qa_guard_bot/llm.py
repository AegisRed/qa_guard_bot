from __future__ import annotations

import asyncio
import json
import re
from typing import Any

from pydantic import BaseModel, Field

from .config import Settings, SiteConfig
from .schemas import LLMProblem, LLMVerdict, PageSnapshot, Status


class _LLMProblemModel(BaseModel):
    label: str = Field(description="Short anomaly label.")
    severity: str = Field(description="warning or fail.")
    reason: str = Field(description="Why this looks suspicious.")
    evidence: str = Field(description="Visible evidence from screenshot or DOM digest.")


class _LLMVerdictModel(BaseModel):
    status: str = Field(description="ok, warning, fail or unknown.")
    confidence: float = Field(description="0..1 confidence score.")
    summary: str = Field(description="Compact human-readable summary.")
    evidence: list[str] = Field(default_factory=list, description="Visible evidence only.")
    problems: list[_LLMProblemModel] = Field(default_factory=list, description="Observed anomalies.")


class GeminiJudge:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def evaluate(
        self,
        site: SiteConfig,
        snapshot: PageSnapshot,
        objective_summary: str,
        screenshot_bytes: bytes | None,
    ) -> LLMVerdict:
        return await asyncio.to_thread(
            self._evaluate_sync,
            site,
            snapshot,
            objective_summary,
            screenshot_bytes,
        )

    def _evaluate_sync(
        self,
        site: SiteConfig,
        snapshot: PageSnapshot,
        objective_summary: str,
        screenshot_bytes: bytes | None,
    ) -> LLMVerdict:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=self._settings.gemini_api_key)
        prompt = self._build_prompt(site, snapshot, objective_summary)

        contents: list[Any]
        if screenshot_bytes:
            contents = [
                prompt,
                types.Part.from_bytes(data=screenshot_bytes, mime_type="image/jpeg"),
            ]
        else:
            contents = [prompt]

        config = {
            "response_mime_type": "application/json",
            "response_json_schema": _LLMVerdictModel.model_json_schema(),
        }

        try:
            response = client.models.generate_content(
                model=self._settings.gemini_model,
                contents=contents,
                config=config,
            )
            payload = self._parse_json(response.text)
            data = _LLMVerdictModel.model_validate(payload)
        except Exception:
            fallback_prompt = (
                prompt
                + "\n\nReturn only valid JSON with keys: "
                + json.dumps(list(_LLMVerdictModel.model_json_schema()["properties"].keys()), ensure_ascii=False)
            )
            response = client.models.generate_content(
                model=self._settings.gemini_model,
                contents=[fallback_prompt] + ([contents[1]] if len(contents) > 1 else []),
            )
            payload = self._parse_json(response.text)
            data = _LLMVerdictModel.model_validate(payload)

        problems = [
            LLMProblem(
                label=item.label,
                severity=_safe_status(item.severity),
                reason=item.reason,
                evidence=item.evidence,
            )
            for item in data.problems
        ]
        return LLMVerdict(
            status=_safe_status(data.status),
            confidence=max(0.0, min(1.0, float(data.confidence))),
            summary=data.summary.strip(),
            evidence=[item.strip() for item in data.evidence if item.strip()],
            problems=problems,
            raw_json=json.dumps(payload, ensure_ascii=False, indent=2),
        )

    @staticmethod
    def _parse_json(raw_text: str) -> dict[str, Any]:
        candidate = raw_text.strip()
        if candidate.startswith("```"):
            candidate = re.sub(r"^```(?:json)?\s*", "", candidate)
            candidate = re.sub(r"\s*```$", "", candidate)

        try:
            payload = json.loads(candidate)
            if isinstance(payload, dict):
                return payload
        except json.JSONDecodeError:
            pass

        match = re.search(r"\{.*\}", raw_text, flags=re.DOTALL)
        if not match:
            raise ValueError(f"LLM did not return JSON: {raw_text[:300]}")
        payload = json.loads(match.group(0))
        if not isinstance(payload, dict):
            raise ValueError("LLM JSON root must be an object.")
        return payload

    @staticmethod
    def _build_prompt(site: SiteConfig, snapshot: PageSnapshot, objective_summary: str) -> str:
        headings = "\n".join(f"- {item}" for item in snapshot.headings[:12]) or "- none"
        buttons = "\n".join(f"- {item}" for item in snapshot.buttons[:12]) or "- none"
        links = "\n".join(f"- {item}" for item in snapshot.links[:12]) or "- none"

        return f"""You are a conservative web QA reviewer.

Task:
Evaluate whether the page looks normal for its intended purpose.

Rules:
1. Use only the provided screenshot and DOM digest.
2. Never assume hidden functionality.
3. If evidence is weak, return status=\"unknown\".
4. Do not restate generic praise.
5. Mention only visible evidence.
6. Return fail only for strong, user-visible issues.
7. Return warning for suspicious but not certainly broken cases.
8. If the objective checks already show a weak network or console issue but the visible page still looks healthy, keep it as warning, not fail.

Site:
- name: {site.name}
- expected URL: {site.url}
- final URL: {snapshot.final_url}

Soft expectations:
{site.soft_expectations or "No additional subjective expectation was provided."}

Objective summary:
{objective_summary}

DOM digest:
- title: {snapshot.title or "<empty>"}
- visible_text_chars: {snapshot.visible_text_chars}

Headings:
{headings}

Buttons / CTAs:
{buttons}

Links:
{links}

Text excerpt:
{snapshot.text_excerpt[:3000]}

Return JSON only.
"""


def _safe_status(value: str) -> Status:
    normalized = (value or "").strip().lower()
    if normalized == "ok":
        return Status.OK
    if normalized == "warning":
        return Status.WARNING
    if normalized == "fail":
        return Status.FAIL
    return Status.UNKNOWN
