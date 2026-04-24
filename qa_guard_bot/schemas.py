from __future__ import annotations

from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any


class Status(StrEnum):
    OK = "ok"
    WARNING = "warning"
    FAIL = "fail"
    UNKNOWN = "unknown"


@dataclass(slots=True)
class CheckItem:
    name: str
    status: Status
    detail: str
    critical: bool = False


@dataclass(slots=True)
class PageSnapshot:
    title: str
    final_url: str
    text_excerpt: str
    visible_text_chars: int
    headings: list[str] = field(default_factory=list)
    buttons: list[str] = field(default_factory=list)
    links: list[str] = field(default_factory=list)


@dataclass(slots=True)
class LLMProblem:
    label: str
    severity: Status
    reason: str
    evidence: str


@dataclass(slots=True)
class LLMVerdict:
    status: Status
    confidence: float
    summary: str
    evidence: list[str] = field(default_factory=list)
    problems: list[LLMProblem] = field(default_factory=list)
    raw_json: str = ""


@dataclass(slots=True)
class PageSummary:
    url: str
    final_url: str
    status: Status
    http_status: int | None
    duration_ms: int
    title: str = ""
    console_error_count: int = 0
    page_error_count: int = 0
    request_failure_count: int = 0
    notes: list[str] = field(default_factory=list)


@dataclass(slots=True)
class SiteCheckResult:
    site_id: int | None
    site_name: str
    site_url: str
    final_url: str
    status: Status
    http_status: int | None
    duration_ms: int
    checks: list[CheckItem] = field(default_factory=list)
    console_errors: list[str] = field(default_factory=list)
    page_errors: list[str] = field(default_factory=list)
    request_failures: list[str] = field(default_factory=list)
    snapshot: PageSnapshot | None = None
    llm_verdict: LLMVerdict | None = None
    screenshot_path: str | None = None
    notes: list[str] = field(default_factory=list)
    crawl_enabled: bool = False
    pages_checked: int = 1
    discovered_pages: int = 1
    page_summaries: list[PageSummary] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RunReport:
    run_id: str
    triggered_by: str
    started_at: str
    finished_at: str
    duration_ms: int
    status: Status
    results: list[SiteCheckResult] = field(default_factory=list)

    def counts(self) -> dict[str, int]:
        counts = {status.value: 0 for status in Status}
        for item in self.results:
            counts[item.status.value] += 1
        return counts

    def signature(self) -> tuple[str, ...]:
        return tuple(f"{item.site_name}:{item.status.value}" for item in self.results)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "triggered_by": self.triggered_by,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_ms": self.duration_ms,
            "status": self.status.value,
            "results": [item.to_dict() for item in self.results],
        }
