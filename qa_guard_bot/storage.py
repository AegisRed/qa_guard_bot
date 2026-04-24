from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import Settings, SiteConfig
from .schemas import RunReport


@dataclass(slots=True)
class StoredSite:
    id: int
    config: SiteConfig
    created_at: str
    updated_at: str


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sites (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    config_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    triggered_by TEXT NOT NULL,
                    site_id INTEGER,
                    site_name TEXT NOT NULL,
                    site_url TEXT NOT NULL,
                    status TEXT NOT NULL,
                    http_status INTEGER,
                    duration_ms INTEGER NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_history_site_time ON history(site_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_history_url_time ON history(site_url, created_at DESC);
                """
            )
            conn.commit()

    def bootstrap(self, settings: Settings) -> None:
        now = _utc_now()
        with self._connect() as conn:
            if conn.execute("SELECT COUNT(*) FROM sites").fetchone()[0] == 0:
                for site in settings.bootstrap_sites:
                    conn.execute(
                        """
                        INSERT INTO sites(name, url, config_json, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (site.name, site.url, json.dumps(site.to_dict(), ensure_ascii=False), now, now),
                    )
            self._set_setting_tx(conn, "check_interval_minutes", str(settings.default_check_interval_minutes), now)
            self._set_setting_tx(conn, "notify_only_on_changes", json.dumps(settings.default_notify_only_on_changes), now)
            self._set_setting_tx(conn, "scheduler_enabled", json.dumps(settings.default_scheduler_enabled), now)
            conn.commit()

    def _set_setting_tx(self, conn: sqlite3.Connection, key: str, value: str, now: str) -> None:
        current = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
        if current is None:
            conn.execute(
                "INSERT INTO app_settings(key, value, updated_at) VALUES (?, ?, ?)",
                (key, value, now),
            )

    def get_setting(self, key: str, default: str) -> str:
        with self._connect() as conn:
            row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO app_settings(key, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
                """,
                (key, value, now),
            )
            conn.commit()

    def get_check_interval_minutes(self, default: int) -> int:
        return max(1, int(self.get_setting("check_interval_minutes", str(default))))

    def set_check_interval_minutes(self, value: int) -> int:
        value = max(1, int(value))
        self.set_setting("check_interval_minutes", str(value))
        return value

    def get_notify_only_on_changes(self, default: bool) -> bool:
        return json.loads(self.get_setting("notify_only_on_changes", json.dumps(default)))

    def set_notify_only_on_changes(self, value: bool) -> bool:
        self.set_setting("notify_only_on_changes", json.dumps(bool(value)))
        return bool(value)

    def get_scheduler_enabled(self, default: bool) -> bool:
        return json.loads(self.get_setting("scheduler_enabled", json.dumps(default)))

    def set_scheduler_enabled(self, value: bool) -> bool:
        self.set_setting("scheduler_enabled", json.dumps(bool(value)))
        return bool(value)

    def list_sites(self) -> list[StoredSite]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM sites ORDER BY id ASC").fetchall()
        return [self._row_to_site(row) for row in rows]

    def get_site(self, site_id: int) -> StoredSite | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM sites WHERE id = ?", (site_id,)).fetchone()
        return self._row_to_site(row) if row else None

    def add_site(self, config: SiteConfig) -> StoredSite:
        now = _utc_now()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO sites(name, url, config_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (config.name, config.url, json.dumps(config.to_dict(), ensure_ascii=False), now, now),
            )
            site_id = int(cursor.lastrowid)
            conn.commit()
        site = self.get_site(site_id)
        assert site is not None
        return site

    def update_site(self, site_id: int, config: SiteConfig) -> StoredSite | None:
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE sites
                SET name = ?, url = ?, config_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (config.name, config.url, json.dumps(config.to_dict(), ensure_ascii=False), now, site_id),
            )
            conn.commit()
        return self.get_site(site_id)

    def toggle_site_enabled(self, site_id: int) -> StoredSite | None:
        site = self.get_site(site_id)
        if site is None:
            return None
        site.config.enabled = not site.config.enabled
        return self.update_site(site_id, site.config)

    def delete_site(self, site_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute("DELETE FROM sites WHERE id = ?", (site_id,))
            conn.commit()
            return cursor.rowcount > 0

    def record_report(self, report: RunReport) -> None:
        created_at = report.finished_at or _utc_now()
        with self._connect() as conn:
            for item in report.results:
                conn.execute(
                    """
                    INSERT INTO history(run_id, triggered_by, site_id, site_name, site_url, status, http_status, duration_ms, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        report.run_id,
                        report.triggered_by,
                        item.site_id,
                        item.site_name,
                        item.site_url,
                        item.status.value,
                        item.http_status,
                        item.duration_ms,
                        created_at,
                    ),
                )
            conn.commit()

    def get_site_history(self, site_id: int, limit: int = 40) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT created_at, status, duration_ms, http_status, run_id
                FROM history
                WHERE site_id = ?
                ORDER BY created_at DESC
                LIMIT ?
                """,
                (site_id, limit),
            ).fetchall()
        result = [dict(row) for row in rows]
        result.reverse()
        return result

    @staticmethod
    def _row_to_site(row: sqlite3.Row) -> StoredSite:
        payload = json.loads(row["config_json"])
        payload["name"] = row["name"]
        payload["url"] = row["url"]
        return StoredSite(
            id=int(row["id"]),
            config=SiteConfig.from_dict(payload),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
