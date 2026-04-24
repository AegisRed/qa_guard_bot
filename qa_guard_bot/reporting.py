from __future__ import annotations

import json
from pathlib import Path

from .schemas import RunReport, SiteCheckResult, Status


STATUS_LABEL = {
    Status.OK: "OK",
    Status.WARNING: "WARN",
    Status.FAIL: "FAIL",
    Status.UNKNOWN: "UNKNOWN",
}


def status_badge(status: Status) -> str:
    return STATUS_LABEL.get(status, status.value.upper())


def short_summary(report: RunReport) -> str:
    counts = report.counts()
    lines = [
        f"QA check finished — <b>{status_badge(report.status)}</b>",
        f"Run: <code>{report.run_id}</code>",
        (
            "Breakdown: "
            f"OK {counts['ok']} / WARN {counts['warning']} / "
            f"FAIL {counts['fail']} / UNKNOWN {counts['unknown']}"
        ),
        "",
    ]
    for item in report.results:
        lines.append(
            f"• <b>{item.site_name}</b> — <b>{status_badge(item.status)}</b> "
            f"({item.duration_ms} ms, pages {item.pages_checked}/{item.discovered_pages})"
        )
    return "\n".join(lines)


def build_markdown_report(report: RunReport) -> str:
    counts = report.counts()
    lines = [
        "# QA report",
        "",
        f"- Overall: **{status_badge(report.status)}**",
        f"- Run: `{report.run_id}`",
        f"- Triggered by: `{report.triggered_by}`",
        f"- Started: `{report.started_at}`",
        f"- Finished: `{report.finished_at}`",
        f"- Duration: `{report.duration_ms} ms`",
        f"- Sites: `{len(report.results)}`",
        (
            "- Breakdown: "
            f"`OK {counts['ok']}` / `WARN {counts['warning']}` / "
            f"`FAIL {counts['fail']}` / `UNKNOWN {counts['unknown']}`"
        ),
        "",
    ]
    for item in report.results:
        lines.extend(_site_section(item))
    return "\n".join(lines).strip() + "\n"



def _site_section(item: SiteCheckResult) -> list[str]:
    lines = [
        f"## {item.site_name}",
        "",
        f"- Site id: `{item.site_id if item.site_id is not None else 'adhoc'}`",
        f"- URL: {item.site_url}",
        f"- Final URL: {item.final_url}",
        f"- Status: **{status_badge(item.status)}**",
        f"- HTTP status: `{item.http_status if item.http_status is not None else 'n/a'}`",
        f"- Duration: `{item.duration_ms} ms`",
        f"- Crawl enabled: `{item.crawl_enabled}`",
        f"- Pages checked: `{item.pages_checked}` / discovered `{item.discovered_pages}`",
    ]
    if item.snapshot:
        lines.extend(
            [
                f"- Title: `{item.snapshot.title or '<empty>'}`",
                f"- Visible text chars: `{item.snapshot.visible_text_chars}`",
            ]
        )
    if item.screenshot_path:
        lines.append(f"- Screenshot: `{item.screenshot_path}`")
    lines.extend(["", "### Objective checks", ""])
    for check in item.checks:
        critical_suffix = " critical" if check.critical else ""
        lines.append(f"- `{status_badge(check.status)}` **{check.name}**{critical_suffix}: {check.detail}")
    if item.console_errors:
        lines.extend(["", "### Console errors", ""])
        for row in item.console_errors:
            lines.append(f"- {row}")
    if item.page_errors:
        lines.extend(["", "### Page errors", ""])
        for row in item.page_errors:
            lines.append(f"- {row}")
    if item.request_failures:
        lines.extend(["", "### Request failures", ""])
        for row in item.request_failures:
            lines.append(f"- {row}")
    if item.page_summaries:
        lines.extend(["", "### Checked pages", ""])
        for page in item.page_summaries:
            note_text = f" | notes: {'; '.join(page.notes[:2])}" if page.notes else ""
            lines.append(
                f"- `{status_badge(page.status)}` {page.url} -> {page.final_url} | "
                f"http `{page.http_status if page.http_status is not None else 'n/a'}` | "
                f"{page.duration_ms} ms{note_text}"
            )
    if item.llm_verdict:
        lines.extend(
            [
                "",
                "### LLM verdict",
                "",
                f"- Status: **{status_badge(item.llm_verdict.status)}**",
                f"- Confidence: `{item.llm_verdict.confidence:.2f}`",
                f"- Summary: {item.llm_verdict.summary}",
            ]
        )
        if item.llm_verdict.evidence:
            lines.append("- Evidence:")
            for row in item.llm_verdict.evidence:
                lines.append(f"  - {row}")
        if item.llm_verdict.problems:
            lines.append("- Problems:")
            for problem in item.llm_verdict.problems:
                lines.append(
                    "  - "
                    f"`{status_badge(problem.severity)}` {problem.label}: {problem.reason} "
                    f"| evidence: {problem.evidence}"
                )
    if item.notes:
        lines.extend(["", "### Notes", ""])
        for row in item.notes:
            lines.append(f"- {row}")
    lines.extend(["", "---", ""])
    return lines



def persist_report(report: RunReport, reports_dir: Path) -> tuple[Path, Path]:
    json_path = reports_dir / "latest_report.json"
    md_path = reports_dir / "latest_report.md"
    json_path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(build_markdown_report(report), encoding="utf-8")

    run_json_path = reports_dir / report.run_id / f"{report.run_id}.json"
    run_md_path = reports_dir / report.run_id / f"{report.run_id}.md"
    run_json_path.parent.mkdir(parents=True, exist_ok=True)
    run_json_path.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    run_md_path.write_text(build_markdown_report(report), encoding="utf-8")
    return json_path, md_path
