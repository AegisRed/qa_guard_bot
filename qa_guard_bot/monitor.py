from __future__ import annotations

import asyncio
import contextlib
import hashlib
import re
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urldefrag, urljoin, urlparse

from playwright.async_api import BrowserContext, Error as PlaywrightError, Page, async_playwright

from .config import Settings, SiteConfig
from .llm import GeminiJudge
from .reporting import persist_report
from .schemas import CheckItem, PageSnapshot, PageSummary, RunReport, SiteCheckResult, Status
from .storage import Storage, StoredSite


@dataclass(slots=True)
class _Inspection:
    url: str
    final_url: str
    http_status: int | None
    duration_ms: int
    checks: list[CheckItem]
    status: Status
    console_errors: list[str]
    page_errors: list[str]
    request_failures: list[str]
    snapshot: PageSnapshot | None
    screenshot_path: str | None
    raw_links: list[str]
    notes: list[str]


class QAMonitor:
    def __init__(self, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self._judge = GeminiJudge(settings)
        self._lock = asyncio.Lock()
        self._latest_report: RunReport | None = None
        self._last_notified_signature: tuple[str, ...] | None = None

    @property
    def latest_report(self) -> RunReport | None:
        return self._latest_report

    async def run_once(
        self,
        triggered_by: str = "manual",
        sites: list[StoredSite] | None = None,
        ad_hoc_sites: list[SiteConfig] | None = None,
    ) -> tuple[RunReport, Path, Path]:
        async with self._lock:
            started = datetime.now(timezone.utc)
            started_perf = perf_counter()
            run_id = started.strftime("%Y%m%d-%H%M%S")
            run_dir = self.settings.reports_dir / run_id
            run_dir.mkdir(parents=True, exist_ok=True)

            site_pool = sites if sites is not None else [site for site in self.storage.list_sites() if site.config.enabled]
            ad_hoc_pool = ad_hoc_sites or []

            results: list[SiteCheckResult] = []
            llm_budget = max(0, self.settings.llm_max_pages_per_run)

            async with async_playwright() as playwright:
                browser = await playwright.chromium.launch(headless=self.settings.playwright_headless)
                context = await browser.new_context(viewport={"width": 1440, "height": 900}, ignore_https_errors=True)
                try:
                    for stored_site in site_pool:
                        result = await self._check_site(context, stored_site.id, stored_site.config, run_dir)
                        llm_budget = await self._maybe_attach_llm(triggered_by, stored_site.config, result, llm_budget)
                        results.append(result)

                    for idx, site in enumerate(ad_hoc_pool, start=1):
                        result = await self._check_site(context, None, site, run_dir, ad_hoc_name_suffix=idx)
                        llm_budget = await self._maybe_attach_llm(triggered_by, site, result, llm_budget)
                        results.append(result)
                finally:
                    await context.close()
                    await browser.close()

            finished = datetime.now(timezone.utc)
            duration_ms = int((perf_counter() - started_perf) * 1000)
            report = RunReport(
                run_id=run_id,
                triggered_by=triggered_by,
                started_at=started.isoformat(),
                finished_at=finished.isoformat(),
                duration_ms=duration_ms,
                status=self._overall_status(results),
                results=results,
            )
            json_path, md_path = persist_report(report, self.settings.reports_dir)
            self.storage.record_report(report)
            self._latest_report = report
            return report, json_path, md_path

    async def periodic_loop(self, send_callback) -> None:
        while True:
            interval_minutes = self.storage.get_check_interval_minutes(self.settings.default_check_interval_minutes)
            scheduler_enabled = self.storage.get_scheduler_enabled(self.settings.default_scheduler_enabled)
            enabled_sites = [site for site in self.storage.list_sites() if site.config.enabled]
            if scheduler_enabled and enabled_sites:
                try:
                    report, json_path, md_path = await self.run_once(triggered_by="scheduler", sites=enabled_sites)
                    if self._should_notify(report):
                        await send_callback(report, json_path, md_path)
                        self._last_notified_signature = report.signature()
                except Exception as exc:
                    await send_callback(None, None, None, error_text=f"Background QA loop failed: {exc}")
            await asyncio.sleep(max(1, interval_minutes) * 60)

    def _should_notify(self, report: RunReport) -> bool:
        if not self.settings.notify_chat_ids:
            return False
        notify_only_on_changes = self.storage.get_notify_only_on_changes(self.settings.default_notify_only_on_changes)
        if not notify_only_on_changes:
            return True
        signature = report.signature()
        if self._last_notified_signature is None:
            return True
        return signature != self._last_notified_signature

    async def _maybe_attach_llm(
        self,
        triggered_by: str,
        site: SiteConfig,
        result: SiteCheckResult,
        llm_budget: int,
    ) -> int:
        if llm_budget <= 0 or not site.llm_enabled or result.snapshot is None:
            return llm_budget
        should_run = result.status != Status.OK or triggered_by.startswith("manual") or triggered_by.startswith("telegram") or triggered_by.startswith("adhoc")
        if not should_run:
            return llm_budget
        await self._attach_llm_verdict(site, result)
        result.status = self._merge_status(result)
        return llm_budget - 1

    async def _check_site(
        self,
        context: BrowserContext,
        site_id: int | None,
        site: SiteConfig,
        run_dir: Path,
        ad_hoc_name_suffix: int | None = None,
    ) -> SiteCheckResult:
        root = await self._inspect_page(
            context=context,
            site=site,
            target_url=site.url,
            run_dir=run_dir,
            label=f"{site.name}-{ad_hoc_name_suffix or 'root'}",
            apply_required_rules=True,
            save_screenshot=site.screenshot,
        )

        result = SiteCheckResult(
            site_id=site_id,
            site_name=site.name,
            site_url=site.url,
            final_url=root.final_url,
            status=root.status,
            http_status=root.http_status,
            duration_ms=root.duration_ms,
            checks=list(root.checks),
            console_errors=root.console_errors[:12],
            page_errors=root.page_errors[:12],
            request_failures=root.request_failures[:12],
            snapshot=root.snapshot,
            screenshot_path=root.screenshot_path,
            notes=list(root.notes),
            crawl_enabled=site.crawl_enabled,
            pages_checked=1,
            discovered_pages=1,
            page_summaries=[
                PageSummary(
                    url=site.url,
                    final_url=root.final_url,
                    status=root.status,
                    http_status=root.http_status,
                    duration_ms=root.duration_ms,
                    title=root.snapshot.title if root.snapshot else "",
                    console_error_count=len(root.console_errors),
                    page_error_count=len(root.page_errors),
                    request_failure_count=len(root.request_failures),
                    notes=list(root.notes),
                )
            ],
        )

        if not site.crawl_enabled or site.crawl_max_pages <= 1 or root.snapshot is None:
            return result

        discovered = self._collect_crawl_targets(root.raw_links, site.url, site.include_subdomains, site.crawl_max_pages)
        result.discovered_pages = 1 + len(discovered)
        page_statuses: list[Status] = [root.status]

        for index, page_url in enumerate(discovered, start=2):
            page_site = SiteConfig.from_dict({**site.to_dict(), "required_texts": [], "required_selectors": [], "screenshot": False})
            extra = await self._inspect_page(
                context=context,
                site=page_site,
                target_url=page_url,
                run_dir=run_dir,
                label=f"{site.name}-page-{index}",
                apply_required_rules=False,
                save_screenshot=False,
            )
            result.pages_checked += 1
            page_statuses.append(extra.status)
            result.page_summaries.append(
                PageSummary(
                    url=page_url,
                    final_url=extra.final_url,
                    status=extra.status,
                    http_status=extra.http_status,
                    duration_ms=extra.duration_ms,
                    title=extra.snapshot.title if extra.snapshot else "",
                    console_error_count=len(extra.console_errors),
                    page_error_count=len(extra.page_errors),
                    request_failure_count=len(extra.request_failures),
                    notes=list(extra.notes),
                )
            )
            if extra.status != Status.OK:
                result.notes.append(f"Crawl anomaly on {page_url}: {extra.status.value}.")

        if any(status == Status.FAIL for status in page_statuses):
            result.status = Status.FAIL if result.status == Status.FAIL else Status.WARNING
            result.checks.append(CheckItem("crawl_pages", Status.WARNING, f"Crawl found issues on {sum(1 for item in page_statuses if item != Status.OK)} pages."))
        elif any(status == Status.WARNING for status in page_statuses):
            result.status = Status.WARNING if result.status != Status.FAIL else Status.FAIL
            result.checks.append(CheckItem("crawl_pages", Status.WARNING, "Crawl found warning-level issues on inner pages."))
        else:
            result.checks.append(CheckItem("crawl_pages", Status.OK, f"Checked {result.pages_checked} pages without extra anomalies."))

        return result

    async def _inspect_page(
        self,
        context: BrowserContext,
        site: SiteConfig,
        target_url: str,
        run_dir: Path,
        label: str,
        apply_required_rules: bool,
        save_screenshot: bool,
    ) -> _Inspection:
        page = await context.new_page()
        console_errors: list[str] = []
        page_errors: list[str] = []
        request_failures: list[str] = []
        ignored_request_failures = 0

        parsed_target = urlparse(target_url)
        target_host = (parsed_target.hostname or "").lower()

        def on_console(message) -> None:
            if message.type == "error":
                console_errors.append(_compact(message.text))

        def on_page_error(error) -> None:
            page_errors.append(_compact(str(error)))

        def on_request_failed(request) -> None:
            nonlocal ignored_request_failures
            try:
                failure = request.failure
                failure_text = str(failure).strip() if failure else "unknown"
            except Exception as exc:
                failure_text = f"unreadable failure: {exc}"
            entry = f"{request.method} {request.url} :: {failure_text}"
            host = (urlparse(request.url).hostname or "").lower()
            if _should_ignore_failure(host, site.ignored_failure_domains):
                ignored_request_failures += 1
                return
            if host and target_host and host != target_host and not host.endswith("." + target_host):
                request_failures.append(entry + " [external]")
            else:
                request_failures.append(entry)

        page.on("console", on_console)
        page.on("pageerror", on_page_error)
        page.on("requestfailed", on_request_failed)

        started = perf_counter()
        checks: list[CheckItem] = []
        screenshot_path: Path | None = None
        raw_links: list[str] = []
        notes: list[str] = []

        try:
            response = await page.goto(target_url, wait_until="domcontentloaded", timeout=self.settings.playwright_timeout_ms)
            with contextlib.suppress(PlaywrightError):
                await page.wait_for_load_state("networkidle", timeout=5_000)
            await page.wait_for_timeout(self.settings.post_load_wait_ms)

            http_status = response.status if response else None
            final_url = page.url
            duration_ms = int((perf_counter() - started) * 1000)

            if http_status is None:
                checks.append(CheckItem("http_status", Status.WARNING, "No navigation response object.", True))
            elif 200 <= http_status < 400:
                checks.append(CheckItem("http_status", Status.OK, f"HTTP {http_status}", True))
            else:
                checks.append(CheckItem("http_status", Status.FAIL, f"HTTP {http_status}", True))

            title = _compact(await page.title())
            body_text = _compact(await page.locator("body").inner_text())
            visible_text_chars = len(body_text)
            headings = await _safe_texts(page, "h1, h2, h3", 12)
            buttons = await _safe_texts(page, "button, a[role='button'], input[type='submit']", 12)
            links = await _safe_link_texts(page, "a[href]", 12)
            raw_links = await _safe_href_list(page, "a[href]", 80)

            if title:
                checks.append(CheckItem("title", Status.OK, f"Title found: {title[:120]}"))
            else:
                checks.append(CheckItem("title", Status.WARNING, "Page title is empty."))

            if visible_text_chars >= site.min_visible_text_chars:
                checks.append(CheckItem("visible_text", Status.OK, f"Visible text looks non-empty ({visible_text_chars} chars)."))
            else:
                checks.append(CheckItem("visible_text", Status.FAIL, f"Visible text is too small ({visible_text_chars} chars).", True))

            if apply_required_rules:
                for expected_text in site.required_texts:
                    found = expected_text.casefold() in body_text.casefold()
                    checks.append(CheckItem(f"required_text:{expected_text}", Status.OK if found else Status.FAIL, "Found." if found else "Missing.", True))
                for selector in site.required_selectors:
                    count = await page.locator(selector).count()
                    checks.append(CheckItem(f"required_selector:{selector}", Status.OK if count > 0 else Status.FAIL, f"Matched {count} nodes.", True))

            fatal_signatures = _find_fatal_signatures(f"{title}\n{body_text}")
            if fatal_signatures:
                checks.append(CheckItem("fatal_signatures", Status.FAIL, "Crash-like text detected: " + ", ".join(fatal_signatures[:5]), True))
            else:
                checks.append(CheckItem("fatal_signatures", Status.OK, "No obvious crash signatures found."))

            console_status = Status.OK if len(console_errors) <= site.max_console_errors else Status.WARNING
            checks.append(CheckItem("console_errors", console_status, f"{len(console_errors)} console errors; allowed {site.max_console_errors}."))

            page_error_status = Status.OK if len(page_errors) <= site.max_page_errors else Status.WARNING
            checks.append(CheckItem("page_errors", page_error_status, f"{len(page_errors)} uncaught page errors; allowed {site.max_page_errors}."))

            request_failure_status = Status.OK if len(request_failures) <= site.max_request_failures else Status.WARNING
            checks.append(CheckItem("request_failures", request_failure_status, f"{len(request_failures)} failed requests; allowed {site.max_request_failures}."))
            if ignored_request_failures:
                notes.append(f"Ignored {ignored_request_failures} failed third-party requests from allowlisted noisy domains.")

            snapshot = PageSnapshot(
                title=title,
                final_url=final_url,
                text_excerpt=body_text[:3500],
                visible_text_chars=visible_text_chars,
                headings=headings,
                buttons=buttons,
                links=links,
            )

            if save_screenshot:
                screenshot_path = run_dir / f"{_slug(label)}.jpg"
                await page.screenshot(path=str(screenshot_path), type="jpeg", quality=70, full_page=False)

            return _Inspection(
                url=target_url,
                final_url=final_url,
                http_status=http_status,
                duration_ms=duration_ms,
                checks=checks,
                status=self._status_from_checks(checks),
                console_errors=console_errors[:12],
                page_errors=page_errors[:12],
                request_failures=request_failures[:12],
                snapshot=snapshot,
                screenshot_path=str(screenshot_path) if screenshot_path else None,
                raw_links=raw_links,
                notes=notes,
            )
        except Exception as exc:
            duration_ms = int((perf_counter() - started) * 1000)
            checks.append(CheckItem("navigation", Status.FAIL, f"Navigation failed: {exc}", True))
            notes.append("LLM review skipped because the page never reached a stable rendered state.")
            return _Inspection(
                url=target_url,
                final_url=target_url,
                http_status=None,
                duration_ms=duration_ms,
                checks=checks,
                status=Status.FAIL,
                console_errors=console_errors[:12],
                page_errors=page_errors[:12],
                request_failures=request_failures[:12],
                snapshot=None,
                screenshot_path=None,
                raw_links=[],
                notes=notes,
            )
        finally:
            await page.close()

    async def _attach_llm_verdict(self, site: SiteConfig, result: SiteCheckResult) -> None:
        assert result.snapshot is not None
        screenshot_bytes: bytes | None = None
        if result.screenshot_path:
            screenshot_bytes = Path(result.screenshot_path).read_bytes()
        objective_summary = "; ".join(f"{check.name}={check.status.value}:{check.detail}" for check in result.checks)
        if result.notes:
            objective_summary += "; notes=" + " | ".join(result.notes[:6])
        verdict = await self._judge.evaluate(site=site, snapshot=result.snapshot, objective_summary=objective_summary, screenshot_bytes=screenshot_bytes)
        result.llm_verdict = verdict

    def _status_from_checks(self, checks: list[CheckItem]) -> Status:
        if any(item.status == Status.FAIL and item.critical for item in checks):
            return Status.FAIL
        if any(item.status in {Status.FAIL, Status.WARNING} for item in checks):
            return Status.WARNING
        return Status.OK

    def _merge_status(self, result: SiteCheckResult) -> Status:
        hard_status = self._status_from_checks(result.checks)
        if result.llm_verdict is None:
            return hard_status
        verdict = result.llm_verdict
        if verdict.status == Status.FAIL and verdict.confidence >= self.settings.llm_fail_confidence:
            return Status.FAIL
        if hard_status == Status.FAIL:
            return Status.FAIL
        if verdict.status in {Status.FAIL, Status.WARNING, Status.UNKNOWN}:
            return Status.WARNING
        return hard_status

    @staticmethod
    def _overall_status(results: list[SiteCheckResult]) -> Status:
        if any(item.status == Status.FAIL for item in results):
            return Status.FAIL
        if any(item.status in {Status.WARNING, Status.UNKNOWN} for item in results):
            return Status.WARNING
        return Status.OK

    @staticmethod
    def _collect_crawl_targets(raw_links: list[str], root_url: str, include_subdomains: bool, max_pages: int) -> list[str]:
        queue: deque[str] = deque()
        seen: set[str] = { _normalize_url(root_url) }
        targets: list[str] = []
        for href in raw_links:
            normalized = _normalize_candidate_link(root_url, href)
            if not normalized:
                continue
            if normalized in seen:
                continue
            if not _is_allowed_crawl_target(normalized, root_url, include_subdomains):
                continue
            seen.add(normalized)
            queue.append(normalized)
        while queue and len(targets) < max_pages - 1:
            targets.append(queue.popleft())
        return targets


def _find_fatal_signatures(text: str) -> list[str]:
    lowered = text.casefold()
    patterns = [
        "404",
        "page not found",
        "not found",
        "something went wrong",
        "internal server error",
        "service unavailable",
        "application error",
        "uncaught",
        "traceback",
        "exception",
        "access denied",
    ]
    return [pattern for pattern in patterns if pattern in lowered]


async def _safe_texts(page: Page, selector: str, limit: int) -> list[str]:
    try:
        items = await page.locator(selector).all_inner_texts()
    except Exception:
        return []
    cleaned = [_compact(item) for item in items]
    return [item[:160] for item in cleaned if item][:limit]


async def _safe_link_texts(page: Page, selector: str, limit: int) -> list[str]:
    try:
        values = await page.locator(selector).evaluate_all(
            """els => els.map(el => {
                const text = (el.innerText || el.textContent || '').trim();
                const href = el.getAttribute('href') || '';
                return `${text} -> ${href}`.trim();
            })"""
        )
    except Exception:
        return []
    cleaned = [_compact(item) for item in values]
    return [item[:180] for item in cleaned if item][:limit]


async def _safe_href_list(page: Page, selector: str, limit: int) -> list[str]:
    try:
        values = await page.locator(selector).evaluate_all(
            """els => els.map(el => el.getAttribute('href') || '').filter(Boolean)"""
        )
    except Exception:
        return []
    cleaned = [_compact(item) for item in values]
    result: list[str] = []
    for item in cleaned:
        if item:
            result.append(item)
        if len(result) >= limit:
            break
    return result


def _compact(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]


def _should_ignore_failure(host: str, ignored_domains: list[str]) -> bool:
    if not host:
        return False
    return any(host == domain or host.endswith("." + domain) for domain in ignored_domains)


def _normalize_candidate_link(base_url: str, href: str) -> str | None:
    href = (href or "").strip()
    if not href or href.startswith("#"):
        return None
    lowered = href.lower()
    if lowered.startswith(("javascript:", "mailto:", "tel:", "data:")):
        return None
    absolute = urljoin(base_url, href)
    return _normalize_url(absolute)


def _normalize_url(url: str) -> str:
    cleaned, _fragment = urldefrag(url)
    parsed = urlparse(cleaned)
    path = parsed.path or "/"
    normalized = parsed._replace(path=path.rstrip("/") or "/", params="", query=parsed.query, fragment="")
    return normalized.geturl()


def _is_allowed_crawl_target(candidate_url: str, root_url: str, include_subdomains: bool) -> bool:
    candidate = urlparse(candidate_url)
    root = urlparse(root_url)
    if candidate.scheme not in {"http", "https"}:
        return False
    if not candidate.hostname or not root.hostname:
        return False
    candidate_host = candidate.hostname.lower()
    root_host = root.hostname.lower()
    if candidate_host == root_host:
        return True
    if include_subdomains:
        candidate_root = _registrable_domain(candidate_host)
        root_root = _registrable_domain(root_host)
        return bool(candidate_root and candidate_root == root_root)
    return False


def _registrable_domain(host: str) -> str:
    parts = host.split(".")
    if len(parts) <= 2:
        return host
    return ".".join(parts[-2:])
