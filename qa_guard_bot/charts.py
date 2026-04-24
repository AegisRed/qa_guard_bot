from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


STATUS_SCORE = {
    "ok": 3,
    "warning": 2,
    "fail": 1,
    "unknown": 0,
}


def render_stability_chart(site_name: str, history: list[dict], output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not history:
        raise ValueError("No history available for chart.")

    x = list(range(1, len(history) + 1))
    y = [STATUS_SCORE.get(str(item.get("status", "unknown")), 0) for item in history]
    labels = [_short_label(str(item.get("created_at", ""))) for item in history]
    durations = [int(item.get("duration_ms") or 0) for item in history]

    fig = plt.figure(figsize=(10, 5), dpi=140)
    ax = fig.add_subplot(111)
    ax.plot(x, y, marker="o", linewidth=2)
    ax.set_title(f"Stability trend — {site_name}")
    ax.set_xlabel("Checks")
    ax.set_ylabel("Status")
    ax.set_yticks([0, 1, 2, 3], ["unknown", "fail", "warning", "ok"])
    ax.set_xticks(x, labels, rotation=35, ha="right")
    ax.grid(True, alpha=0.35)

    for idx, duration in enumerate(durations, start=1):
        ax.annotate(f"{duration} ms", (idx, y[idx - 1]), textcoords="offset points", xytext=(0, 8), ha="center", fontsize=8)

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    return output_path


def _short_label(value: str) -> str:
    if "T" in value:
        value = value.split("T", 1)[1]
    value = value.replace("+00:00", "Z")
    return value[:8]
