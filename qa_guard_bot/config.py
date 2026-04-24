from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dotenv import load_dotenv


DEFAULT_IGNORED_FAILURE_DOMAINS = [
    "google-analytics.com",
    "googletagmanager.com",
    "doubleclick.net",
    "facebook.net",
    "bat.bing.com",
    "clarity.ms",
    "hotjar.com",
]


def _parse_bool(value: str | None, default: bool) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}



def _parse_int(value: str | None, default: int) -> int:
    if value is None or value.strip() == "":
        return default
    return int(value)



def _parse_int_list(value: str | None) -> list[int]:
    if not value:
        return []
    return [int(item.strip()) for item in value.split(",") if item.strip()]


@dataclass(slots=True)
class SiteConfig:
    name: str
    url: str
    required_texts: list[str] = field(default_factory=list)
    required_selectors: list[str] = field(default_factory=list)
    soft_expectations: str = ""
    max_console_errors: int = 0
    max_page_errors: int = 0
    max_request_failures: int = 0
    min_visible_text_chars: int = 120
    llm_enabled: bool = True
    screenshot: bool = True
    crawl_enabled: bool = False
    crawl_max_pages: int = 1
    include_subdomains: bool = False
    ignored_failure_domains: list[str] = field(default_factory=lambda: list(DEFAULT_IGNORED_FAILURE_DOMAINS))
    enabled: bool = True

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "SiteConfig":
        name = str(payload.get("name", "")).strip()
        url = str(payload.get("url", "")).strip()
        if not name:
            name = url
        if not url:
            raise ValueError("Each site must have a non-empty 'url'.")
        return cls(
            name=name,
            url=url,
            required_texts=[str(item).strip() for item in payload.get("required_texts", []) if str(item).strip()],
            required_selectors=[str(item).strip() for item in payload.get("required_selectors", []) if str(item).strip()],
            soft_expectations=str(payload.get("soft_expectations", "")).strip(),
            max_console_errors=int(payload.get("max_console_errors", 0)),
            max_page_errors=int(payload.get("max_page_errors", 0)),
            max_request_failures=int(payload.get("max_request_failures", 0)),
            min_visible_text_chars=int(payload.get("min_visible_text_chars", 120)),
            llm_enabled=bool(payload.get("llm_enabled", True)),
            screenshot=bool(payload.get("screenshot", True)),
            crawl_enabled=bool(payload.get("crawl_enabled", False)),
            crawl_max_pages=max(1, int(payload.get("crawl_max_pages", 1))),
            include_subdomains=bool(payload.get("include_subdomains", False)),
            ignored_failure_domains=[
                str(item).strip().lower()
                for item in payload.get("ignored_failure_domains", DEFAULT_IGNORED_FAILURE_DOMAINS)
                if str(item).strip()
            ],
            enabled=bool(payload.get("enabled", True)),
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Settings:
    telegram_bot_token: str
    gemini_api_key: str
    gemini_model: str
    default_check_interval_minutes: int
    default_notify_only_on_changes: bool
    default_scheduler_enabled: bool
    playwright_headless: bool
    playwright_timeout_ms: int
    post_load_wait_ms: int
    llm_max_pages_per_run: int
    llm_fail_confidence: float
    telegram_allowed_user_ids: list[int]
    notify_chat_ids: list[int]
    reports_dir: Path
    db_path: Path
    bootstrap_sites: list[SiteConfig]

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()

        bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
        gemini_api_key = os.getenv("GEMINI_API_KEY", "").strip()
        gemini_model = os.getenv("GEMINI_MODEL", "gemini-3.1-flash-lite-preview").strip()
        if not bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is required.")
        if not gemini_api_key:
            raise ValueError("GEMINI_API_KEY is required.")

        sites_json = os.getenv("SITES_JSON", "").strip()
        bootstrap_sites: list[SiteConfig] = []
        if sites_json:
            raw_sites = json.loads(sites_json)
            if not isinstance(raw_sites, list):
                raise ValueError("SITES_JSON must be a JSON array if provided.")
            bootstrap_sites = [SiteConfig.from_dict(item) for item in raw_sites]

        reports_dir = Path(os.getenv("REPORTS_DIR", "reports")).resolve()
        reports_dir.mkdir(parents=True, exist_ok=True)

        db_path = Path(os.getenv("DB_PATH", "qa_guard_bot.sqlite3")).resolve()
        db_path.parent.mkdir(parents=True, exist_ok=True)

        llm_fail_confidence = float(os.getenv("LLM_FAIL_CONFIDENCE", "0.82").strip())
        if not 0.0 <= llm_fail_confidence <= 1.0:
            raise ValueError("LLM_FAIL_CONFIDENCE must be in range 0..1.")

        return cls(
            telegram_bot_token=bot_token,
            gemini_api_key=gemini_api_key,
            gemini_model=gemini_model,
            default_check_interval_minutes=_parse_int(os.getenv("CHECK_INTERVAL_MINUTES"), 30),
            default_notify_only_on_changes=_parse_bool(os.getenv("NOTIFY_ONLY_ON_CHANGES"), True),
            default_scheduler_enabled=_parse_bool(os.getenv("SCHEDULER_ENABLED"), True),
            playwright_headless=_parse_bool(os.getenv("PLAYWRIGHT_HEADLESS"), True),
            playwright_timeout_ms=_parse_int(os.getenv("PLAYWRIGHT_TIMEOUT_MS"), 25_000),
            post_load_wait_ms=_parse_int(os.getenv("POST_LOAD_WAIT_MS"), 1_500),
            llm_max_pages_per_run=_parse_int(os.getenv("LLM_MAX_PAGES_PER_RUN"), 3),
            llm_fail_confidence=llm_fail_confidence,
            telegram_allowed_user_ids=_parse_int_list(os.getenv("TELEGRAM_ALLOWED_USER_IDS")),
            notify_chat_ids=_parse_int_list(os.getenv("NOTIFY_CHAT_IDS")),
            reports_dir=reports_dir,
            db_path=db_path,
            bootstrap_sites=bootstrap_sites,
        )
