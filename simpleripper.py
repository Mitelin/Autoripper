from __future__ import annotations

import argparse
import codecs
import ctypes
import hashlib
import json
import os
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import parse, request

import yaml


APP_NAME = "SimpleRipper"
SCAN_SCOPE_SCHEMA_VERSION = 1
POLICY_HASH_SCHEMA_VERSION = 2
WORKER_CACHE_BUSY_TIMEOUT_MS = 30000
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".wmv", ".m4v", ".ts"}
LANGUAGE_ALIASES = {
    "cze": {"cze", "ces", "cz", "czech", "cestina", "cesky", "cz dabing", "czech dub"},
    "slo": {"slo", "slk", "sk", "slovak", "slovencina", "sk dabing", "slovak dub"},
    "eng": {"eng", "en", "english", "anglicky", "anglictina", "english dub", "en dub"},
    "jpn": {"jpn", "ja", "jp", "japanese", "japonstina", "nihongo"},
}

_WORKER_CACHE_INIT_LOCK = threading.Lock()
_WORKER_CACHE_INITIALIZED: set[str] = set()


class ForceStopRequested(RuntimeError):
    pass


class FfmpegFailedError(RuntimeError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_config(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data["__config_path"] = str(path)
    return data


def save_config(config: dict[str, Any]) -> None:
    config_path = Path(str(config.get("__config_path") or "")).resolve() if config.get("__config_path") else None
    if not config_path:
        return
    payload = {key: value for key, value in config.items() if not str(key).startswith("__")}
    config_path.write_text(yaml.safe_dump(payload, sort_keys=False, allow_unicode=False), encoding="utf-8")


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def append_jsonl(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(data, ensure_ascii=False) + "\n")


def append_text_line(path: Path, line: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(line.rstrip() + "\n")


def tail_text_lines(path: Path, limit: int = 100) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-limit:]


def copy_file_interruptible(source: Path, destination: Path, should_stop: callable, chunk_size: int = 8 * 1024 * 1024) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        with source.open("rb") as src_handle, destination.open("wb") as dst_handle:
            while True:
                if should_stop():
                    raise ForceStopRequested("force stop requested")
                chunk = src_handle.read(chunk_size)
                if not chunk:
                    break
                dst_handle.write(chunk)
        shutil.copystat(str(source), str(destination))
    except Exception:
        destination.unlink(missing_ok=True)
        raise


def runtime_dir(config: dict[str, Any]) -> Path:
    return Path(str((config.get("app") or {}).get("runtime_dir") or "runtime"))


def work_dir(config: dict[str, Any]) -> Path:
    return Path(str((config.get("paths") or {}).get("local_work_dir") or (config.get("app") or {}).get("work_dir") or "work"))


def history_dir(config: dict[str, Any]) -> Path:
    return Path(str((config.get("paths") or {}).get("history_dir") or (config.get("app") or {}).get("history_dir") or "history"))


def log_dir(config: dict[str, Any]) -> Path:
    return Path(str((config.get("paths") or {}).get("log_dir") or (config.get("app") or {}).get("log_dir") or "logs"))


def current_job_path(config: dict[str, Any]) -> Path:
    return runtime_dir(config) / "current_job.json"


def worker_cache_path(config: dict[str, Any]) -> Path:
    explicit = str((config.get("paths") or {}).get("worker_cache_path") or "").strip()
    return Path(explicit) if explicit else runtime_dir(config) / "worker_cache.sqlite"


def scan_cache_settings(config: dict[str, Any]) -> dict[str, Any]:
    settings = config.get("scan_cache") or {}
    scan_settings = config.get("scan") or {}
    return {
        "enabled": bool(settings.get("enabled", scan_settings.get("inventory_cache_enabled", False))),
        "fast_inventory_rescan_hours": float(settings.get("fast_inventory_rescan_hours", scan_settings.get("inventory_refresh_hours", 24))),
        "max_deep_checks_per_cycle": int(settings.get("max_deep_checks_per_cycle", 50)),
        "queue_size": int(settings.get("queue_size", 25)),
        "failed_retry_hours": float(settings.get("failed_retry_hours", scan_settings.get("failed_retry_after_hours", 24))),
        "max_failures_before_block": int(settings.get("max_failures_before_block", scan_settings.get("failed_retry_limit", 3))),
        "blocked_retry_days": float(settings.get("blocked_retry_days", 30)),
        "folder_state_cache_enabled": bool(settings.get("folder_state_cache_enabled", scan_settings.get("folder_state_cache_enabled", True))),
        "skip_clean_folders": bool(settings.get("skip_clean_folders", scan_settings.get("skip_clean_folders", True))),
        "folder_clean_requires_full_inventory": bool(settings.get("folder_clean_requires_full_inventory", scan_settings.get("folder_clean_requires_full_inventory", True))),
    }


def scan_cache_enabled(config: dict[str, Any]) -> bool:
    return bool(scan_cache_settings(config).get("enabled"))


def folder_state_cache_enabled(config: dict[str, Any]) -> bool:
    settings = scan_cache_settings(config)
    return bool(settings.get("enabled") and settings.get("folder_state_cache_enabled"))


def selected_scope_entries(config: dict[str, Any], folders: list[Path] | None = None) -> list[dict[str, str]]:
    configured = normalize_selected_folder_entries((config.get("scan") or {}).get("selected_folders") or [])
    if not folders:
        return configured
    folder_keys = {normalize_path_for_match(folder) for folder in folders}
    matched = [item for item in configured if normalize_path_for_match(item["path"]) in folder_keys]
    if matched:
        return matched
    return [{"path": str(folder), "media_type": "auto"} for folder in folders]


def scan_scope_payload(config: dict[str, Any], folders: list[Path] | None = None) -> dict[str, Any]:
    entries = selected_scope_entries(config, folders)
    normalized_entries = [
        {"path": normalize_path_for_prefix_match(item["path"]), "media_type": media_type_value(item.get("media_type"))}
        for item in sorted(entries, key=lambda item: normalize_path_for_prefix_match(item["path"]))
    ]
    return {
        "schema_version": SCAN_SCOPE_SCHEMA_VERSION,
        "selected_folders": normalized_entries,
        "file_extensions": sorted(scan_file_extensions(config)),
        "skip_rules": config.get("skip_rules") or {},
        "quality_profiles": config.get("quality_profiles") or {},
        "retention_size_policy": config.get("retention_size_policy") or {},
        "track_policy": config.get("track_policy") or {},
        "verification": config.get("verification") or {},
    }


def scan_scope_fingerprint(config: dict[str, Any], folders: list[Path] | None = None) -> str:
    payload = scan_scope_payload(config, folders)
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def path_in_scope(path: Path, scope_entries: list[dict[str, str]]) -> bool:
    path_norm = normalize_path_for_prefix_match(path)
    for entry in scope_entries:
        prefix = normalize_path_for_prefix_match(entry["path"])
        if path_norm == prefix or path_norm.startswith(prefix + "/"):
            return True
    return False


def invalidate_scoped_file_decisions(connection: sqlite3.Connection, scope_entries: list[dict[str, str]]) -> None:
    if not scope_entries:
        return
    rows = connection.execute("SELECT path, decision FROM file_index").fetchall()
    affected = [str(row["path"]) for row in rows if path_in_scope(Path(str(row["path"])), scope_entries) and str(row["decision"] or "") in {"skip", "encode_candidate"}]
    if not affected:
        return
    connection.executemany(
        "UPDATE file_index SET decision = NULL, decision_reason = NULL, policy_hash = NULL, estimated_saved_bytes = NULL, last_deep_checked_at = NULL, updated_at = ? WHERE path = ?",
        [(utc_now(), item) for item in affected],
    )


def invalidate_candidate_queue_scope(config: dict[str, Any], reason: str, old_scope_fingerprint: str | None = None, new_scope_fingerprint: str | None = None, old_scope_entries: list[dict[str, str]] | None = None, new_scope_entries: list[dict[str, str]] | None = None) -> None:
    with open_worker_cache(config) as connection:
        for key in ("candidate_queue_scope_fingerprint", "candidate_queue_generated_at", "candidate_queue_generation_id"):
            connection.execute("DELETE FROM scan_state WHERE key = ?", (key,))
        scope_union: list[dict[str, str]] = []
        for entry in (old_scope_entries or []) + (new_scope_entries or []):
            if any(normalize_path_for_match(item["path"]) == normalize_path_for_match(entry["path"]) and item.get("media_type") == entry.get("media_type") for item in scope_union):
                continue
            scope_union.append(entry)
        invalidate_scoped_file_decisions(connection, scope_union)
    log_event(config, "candidate_queue_invalidated", reason=reason, old_scope=old_scope_fingerprint, new_scope=new_scope_fingerprint)


def policy_hash(config: dict[str, Any]) -> str:
    relevant = {
        "schema_version": POLICY_HASH_SCHEMA_VERSION,
        "quality_profiles": config.get("quality_profiles") or {},
        "skip_rules": config.get("skip_rules") or {},
        "track_policy": config.get("track_policy") or {},
        "retention_size_policy": config.get("retention_size_policy") or {},
        "verification": config.get("verification") or {},
        "libraries": (config.get("libraries") or {}).get("roots") or [],
    }
    return hashlib.sha256(json.dumps(relevant, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def ensure_worker_cache_initialized(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache_key = str(path.resolve())
    if path.exists() and cache_key in _WORKER_CACHE_INITIALIZED:
        return
    with _WORKER_CACHE_INIT_LOCK:
        if path.exists() and cache_key in _WORKER_CACHE_INITIALIZED:
            return
        connection = sqlite3.connect(path, timeout=WORKER_CACHE_BUSY_TIMEOUT_MS / 1000)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute(f"PRAGMA busy_timeout = {WORKER_CACHE_BUSY_TIMEOUT_MS}")
            connection.execute("PRAGMA journal_mode = WAL")
            initialize_worker_cache(connection)
            connection.commit()
            _WORKER_CACHE_INITIALIZED.add(cache_key)
        finally:
            connection.close()


@contextmanager
def open_worker_cache(config: dict[str, Any]) -> Any:
    path = worker_cache_path(config)
    ensure_worker_cache_initialized(path)
    connection = sqlite3.connect(path, timeout=WORKER_CACHE_BUSY_TIMEOUT_MS / 1000)
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout = {WORKER_CACHE_BUSY_TIMEOUT_MS}")
    try:
        yield connection
        connection.commit()
    finally:
        connection.close()


def initialize_worker_cache(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS file_index (
            path TEXT PRIMARY KEY,
            normalized_path TEXT,
            media_type TEXT,
            size_bytes INTEGER NOT NULL,
            mtime_ns INTEGER NOT NULL,
            suffix TEXT,
            parent_dir TEXT,
            duration_seconds REAL,
            video_codec TEXT,
            video_pix_fmt TEXT,
            width INTEGER,
            height INTEGER,
            overall_bitrate_kbps INTEGER,
            audio_stream_count INTEGER,
            subtitle_stream_count INTEGER,
            decision TEXT,
            decision_reason TEXT,
            score REAL DEFAULT 0,
            estimated_saved_bytes INTEGER,
            policy_hash TEXT,
            failure_count INTEGER DEFAULT 0,
            last_error TEXT,
            last_failure_at TEXT,
            retry_after TEXT,
            next_check_after TEXT,
            last_seen_at TEXT,
            last_fast_scanned_at TEXT,
            last_deep_checked_at TEXT,
            updated_at TEXT
        )
        """
    )
    file_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(file_index)").fetchall()}
    for column_name, definition in {
        "last_failure_at": "TEXT",
    }.items():
        if column_name not in file_columns:
            connection.execute(f"ALTER TABLE file_index ADD COLUMN {column_name} {definition}")
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS scan_state (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS folder_index (
            path TEXT PRIMARY KEY,
            normalized_path TEXT,
            state TEXT NOT NULL,
            reason TEXT,
            direct_file_count INTEGER NOT NULL DEFAULT 0,
            direct_total_size INTEGER NOT NULL DEFAULT 0,
            direct_latest_mtime_ns INTEGER NOT NULL DEFAULT 0,
            child_dir_count INTEGER NOT NULL DEFAULT 0,
            children_clean INTEGER NOT NULL DEFAULT 0,
            total_relevant_files INTEGER NOT NULL DEFAULT 0,
            terminal_files INTEGER NOT NULL DEFAULT 0,
            unknown_files INTEGER NOT NULL DEFAULT 0,
            failed_retry_blocked_files INTEGER NOT NULL DEFAULT 0,
            child_folders_total INTEGER NOT NULL DEFAULT 0,
            child_folders_clean INTEGER NOT NULL DEFAULT 0,
            child_folders_unknown INTEGER NOT NULL DEFAULT 0,
            scan_complete INTEGER NOT NULL DEFAULT 0,
            inventory_generation_id TEXT,
            last_error TEXT,
            policy_hash TEXT,
            checked_at TEXT,
            updated_at TEXT
        )
        """
    )
    existing_columns = {str(row[1]) for row in connection.execute("PRAGMA table_info(folder_index)").fetchall()}
    for column_name, definition in {
        "total_relevant_files": "INTEGER NOT NULL DEFAULT 0",
        "terminal_files": "INTEGER NOT NULL DEFAULT 0",
        "unknown_files": "INTEGER NOT NULL DEFAULT 0",
        "failed_retry_blocked_files": "INTEGER NOT NULL DEFAULT 0",
        "child_folders_total": "INTEGER NOT NULL DEFAULT 0",
        "child_folders_clean": "INTEGER NOT NULL DEFAULT 0",
        "child_folders_unknown": "INTEGER NOT NULL DEFAULT 0",
        "scan_complete": "INTEGER NOT NULL DEFAULT 0",
        "inventory_generation_id": "TEXT",
        "last_error": "TEXT",
    }.items():
        if column_name not in existing_columns:
            connection.execute(f"ALTER TABLE folder_index ADD COLUMN {column_name} {definition}")


def scan_state_get(config: dict[str, Any], key: str) -> str | None:
    with open_worker_cache(config) as connection:
        row = connection.execute("SELECT value FROM scan_state WHERE key = ?", (key,)).fetchone()
    return str(row[0]) if row else None


def scan_state_set(config: dict[str, Any], key: str, value: str) -> None:
    with open_worker_cache(config) as connection:
        now = utc_now()
        connection.execute(
            "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
            (key, value, now),
        )


def clear_worker_cache(config: dict[str, Any]) -> None:
    path = worker_cache_path(config)
    if path.exists():
        path.unlink()
    with open_worker_cache(config):
        pass


def clear_worker_failures(config: dict[str, Any]) -> None:
    with open_worker_cache(config) as connection:
        connection.execute(
            "UPDATE file_index SET decision = NULL, decision_reason = NULL, failure_count = 0, last_error = NULL, last_failure_at = NULL, retry_after = NULL, next_check_after = NULL WHERE decision IN ('failed', 'blocked')"
        )
        connection.execute("UPDATE folder_index SET state = 'dirty', reason = 'failures_cleared', checked_at = ?, updated_at = ?", (utc_now(), utc_now()))


def clear_worker_folder_cache(config: dict[str, Any]) -> None:
    with open_worker_cache(config) as connection:
        connection.execute("DELETE FROM folder_index")
        for key in ("last_skipped_folder", "last_completed_inventory_generation_id", "current_inventory_generation_id"):
            connection.execute("DELETE FROM scan_state WHERE key = ?", (key,))


def clear_worker_file_cache(config: dict[str, Any]) -> None:
    with open_worker_cache(config) as connection:
        connection.execute("DELETE FROM file_index")
        connection.execute("DELETE FROM folder_index")
        for key in ("last_fast_inventory_scan_at", "last_policy_hash", "last_skipped_folder", "last_completed_inventory_generation_id", "current_inventory_generation_id"):
            connection.execute("DELETE FROM scan_state WHERE key = ?", (key,))


def clear_worker_candidate_queue(config: dict[str, Any]) -> None:
    with open_worker_cache(config) as connection:
        connection.execute("UPDATE file_index SET decision = NULL, decision_reason = NULL, estimated_saved_bytes = NULL, updated_at = ? WHERE decision = 'encode_candidate'", (utc_now(),))
        for key in ("candidate_queue_scope_fingerprint", "candidate_queue_generated_at", "candidate_queue_generation_id"):
            connection.execute("DELETE FROM scan_state WHERE key = ?", (key,))


def worker_cache_summary(config: dict[str, Any]) -> dict[str, Any]:
    settings = scan_cache_settings(config)
    summary: dict[str, Any] = {
        "enabled": bool(settings.get("enabled")),
        "db_path": str(worker_cache_path(config)),
        "last_fast_inventory_scan_at": None,
        "indexed_files": 0,
        "encode_candidates": 0,
        "cached_skips": 0,
        "failed_cooldown": 0,
        "blocked": 0,
        "queue_size": settings.get("queue_size"),
        "candidate_queue_count": 0,
        "folder_states": {"clean": 0, "partial": 0, "dirty": 0, "stale": 0, "blocked": 0},
        "last_skipped_folder": None,
    }
    if not summary["enabled"]:
        return summary
    with open_worker_cache(config) as connection:
        row = connection.execute("SELECT value FROM scan_state WHERE key = 'last_fast_inventory_scan_at'").fetchone()
        summary["last_fast_inventory_scan_at"] = row["value"] if row else None
        rows = connection.execute("SELECT COALESCE(decision, 'pending') AS decision, COUNT(*) AS count FROM file_index GROUP BY COALESCE(decision, 'pending')").fetchall()
        counts = {str(row["decision"]): int(row["count"]) for row in rows}
        folder_rows = connection.execute("SELECT state, COUNT(*) AS count FROM folder_index GROUP BY state").fetchall()
        folder_counts = {str(row["state"]): int(row["count"]) for row in folder_rows}
        queue_row = connection.execute("SELECT COUNT(*) AS count FROM file_index WHERE decision IS NULL OR decision = 'encode_candidate'").fetchone()
        skipped_row = connection.execute("SELECT value FROM scan_state WHERE key = 'last_skipped_folder'").fetchone()
    summary["indexed_files"] = sum(counts.values())
    summary["encode_candidates"] = counts.get("encode_candidate", 0) + counts.get("pending", 0)
    summary["cached_skips"] = counts.get("skip", 0) + counts.get("done", 0)
    summary["failed_cooldown"] = counts.get("failed", 0)
    summary["blocked"] = counts.get("blocked", 0)
    summary["candidate_queue_count"] = int(queue_row["count"] or 0) if queue_row else 0
    summary["folder_states"] = {key: folder_counts.get(key, 0) for key in summary["folder_states"]}
    summary["last_skipped_folder"] = skipped_row["value"] if skipped_row else None
    return summary


def selected_folders_path(config: dict[str, Any]) -> Path:
    return runtime_dir(config) / "selected_folders.json"


def normalize_selected_folder_entry(entry: Any, default_media_type: str = "auto") -> dict[str, str] | None:
    if isinstance(entry, str) and entry.strip():
        return {"path": entry.strip(), "media_type": default_media_type}
    if isinstance(entry, dict):
        path = str(entry.get("path") or "").strip()
        if not path:
            return None
        media_type = str(entry.get("media_type") or default_media_type).strip().lower() or default_media_type
        if media_type not in {"auto", "movie", "series", "anime", "default"}:
            media_type = default_media_type
        return {"path": path, "media_type": media_type}
    return None


def normalize_selected_folder_entries(entries: list[Any] | None, default_media_type: str = "auto") -> list[dict[str, str]]:
    normalized: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in entries or []:
        entry = normalize_selected_folder_entry(item, default_media_type)
        if not entry:
            continue
        key = str(Path(entry["path"]))
        if key in seen:
            continue
        seen.add(key)
        normalized.append({"path": key, "media_type": entry["media_type"]})
    return normalized


def persisted_selected_folder_entries(config: dict[str, Any]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    payload: dict[str, Any] = {}
    if selected_folders_path(config).exists():
        try:
            payload = read_json(selected_folders_path(config))
        except (OSError, json.JSONDecodeError):
            payload = {}
    scan_settings = config.get("scan") or {}
    selected = normalize_selected_folder_entries(payload.get("selected_folders") or scan_settings.get("selected_folders") or [])
    custom = normalize_selected_folder_entries(payload.get("custom_folders") or [])
    return selected, custom


def persist_selected_folders_in_config(config: dict[str, Any], entries: list[dict[str, str]]) -> None:
    scan_settings = config.setdefault("scan", {})
    scan_settings["selected_folders"] = [{"path": item["path"], "media_type": item["media_type"]} for item in entries]
    save_config(config)


def media_type_value(value: str | None) -> str:
    text = str(value or "auto").strip().lower()
    return text if text in {"auto", "movie", "series", "anime", "default"} else "auto"


def app_log_path(config: dict[str, Any]) -> Path:
    return log_dir(config) / "app.log"


def ffmpeg_current_log_path(config: dict[str, Any]) -> Path:
    return log_dir(config) / "ffmpeg-current.log"


def log_event(config: dict[str, Any], event: str, **fields: Any) -> None:
    details = " ".join(f"{key}={json.dumps(value, ensure_ascii=False)}" for key, value in fields.items() if value is not None)
    append_text_line(app_log_path(config), f"{utc_now()} {event}{(' ' + details) if details else ''}")


def source_signature(path: Path) -> dict[str, Any]:
    stat_result = path.stat()
    return {"size_bytes": stat_result.st_size, "mtime_ns": getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1_000_000_000))}


def shared_history_root(config: dict[str, Any]) -> Path:
    explicit = str(((config.get("paths") or {}).get("shared_history_dir") or "")).strip()
    if explicit:
        return Path(explicit)
    roots = [Path(str(item)) for item in ((config.get("libraries") or {}).get("roots") or [])]
    if roots:
        return roots[0].parent / "RIPTEST" / "state"
    linux_output_root = str((((config.get("verification") or {}).get("linux-nas") or {}).get("output_root") or "")).strip()
    if linux_output_root:
        return Path(linux_output_root) / "state"
    return history_dir(config) / "shared_state"


def worker_history_file_name() -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", socket.gethostname()) + ".json"


def shared_worker_history_dir(config: dict[str, Any]) -> Path:
    return shared_history_root(config) / "workers"


def shared_worker_history_path(config: dict[str, Any]) -> Path:
    return shared_worker_history_dir(config) / worker_history_file_name()


def history_index_path(config: dict[str, Any], source: Path) -> Path:
    return history_dir(config) / "source_index" / f"{file_lock_id(source)}.json"


def load_history_index(config: dict[str, Any], source: Path) -> dict[str, Any] | None:
    path = history_index_path(config, source)
    if not path.exists():
        return None
    try:
        return read_json(path)
    except (OSError, json.JSONDecodeError):
        return None


def is_history_done_for_current_source(config: dict[str, Any], source: Path) -> bool:
    payload = load_history_index(config, source)
    if not payload or payload.get("status") != "done":
        return False
    signature = payload.get("source_signature") or {}
    current = source_signature(source)
    return signature.get("size_bytes") == current["size_bytes"] and signature.get("mtime_ns") == current["mtime_ns"]


def parse_utc_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def recent_ffmpeg_failure_info(config: dict[str, Any], source: Path) -> dict[str, Any] | None:
    payload = load_history_index(config, source)
    if not payload or payload.get("status") != "error" or payload.get("failure_type") != "ffmpeg":
        return None
    signature = payload.get("source_signature") or {}
    current = source_signature(source)
    if signature.get("size_bytes") != current["size_bytes"] or signature.get("mtime_ns") != current["mtime_ns"]:
        return None
    scan_settings = config.get("scan") or {}
    try:
        cooldown_hours = max(0.0, float(scan_settings.get("failed_retry_cooldown_hours", 24)))
    except (TypeError, ValueError):
        cooldown_hours = 24.0
    try:
        max_failures = max(1, int(scan_settings.get("max_failures_per_file", 1)))
    except (TypeError, ValueError):
        max_failures = 1
    failure_count = int(payload.get("failure_count") or 1)
    updated_at = parse_utc_datetime(payload.get("updated_at"))
    if failure_count < max_failures or updated_at is None:
        return None
    retry_after = updated_at.timestamp() + cooldown_hours * 3600
    if time.time() >= retry_after:
        return None
    return {"failure_count": failure_count, "retry_after": datetime.fromtimestamp(retry_after, timezone.utc).isoformat(timespec="seconds"), "error": payload.get("error")}


def write_history_index(config: dict[str, Any], source: Path, payload: dict[str, Any]) -> None:
    write_json(history_index_path(config, source), payload)


def write_shared_worker_history(config: dict[str, Any], source: Path, payload: dict[str, Any]) -> None:
    path = shared_worker_history_path(config)
    shared_payload = {
        **payload,
        "source_path": str(source),
        "hostname": socket.gethostname(),
    }
    document: dict[str, Any] = {"hostname": socket.gethostname(), "updated_at": utc_now(), "sources": {}}
    if path.exists():
        try:
            existing = read_json(path)
            if isinstance(existing, dict):
                document.update(existing)
        except (OSError, json.JSONDecodeError):
            pass
    sources = document.get("sources") if isinstance(document.get("sources"), dict) else {}
    sources[file_lock_id(source)] = shared_payload
    document["hostname"] = socket.gethostname()
    document["updated_at"] = utc_now()
    document["sources"] = sources
    write_json(path, document)


def sync_history_from_shared_workers(config: dict[str, Any]) -> dict[str, int]:
    worker_dir = shared_worker_history_dir(config)
    if not worker_dir.exists():
        return {"files": 0, "entries": 0, "updated": 0}
    files_seen = 0
    entries_seen = 0
    updated = 0
    for path in worker_dir.glob("*.json"):
        files_seen += 1
        try:
            document = read_json(path)
        except (OSError, json.JSONDecodeError):
            continue
        sources = document.get("sources") if isinstance(document, dict) else None
        if not isinstance(sources, dict):
            continue
        for entry in sources.values():
            if not isinstance(entry, dict):
                continue
            source_path = str(entry.get("source_path") or "").strip()
            if not source_path:
                continue
            entries_seen += 1
            source = Path(source_path)
            local_path = history_index_path(config, source)
            current: dict[str, Any] = {}
            if local_path.exists():
                try:
                    current = read_json(local_path)
                except (OSError, json.JSONDecodeError):
                    current = {}
            if str(current.get("updated_at") or "") >= str(entry.get("updated_at") or ""):
                continue
            write_json(local_path, entry)
            updated += 1
    return {"files": files_seen, "entries": entries_seen, "updated": updated}


def to_float(value: Any) -> float | None:
    if value in (None, "", "N/A"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def to_int(value: Any) -> int | None:
    number = to_float(value)
    return None if number is None else int(number)


def progress_seconds(value: Any) -> float | None:
    if value in (None, "", "N/A"):
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
        return seconds / 1_000_000.0 if seconds > 1_000_000 else seconds
    text = str(value).strip()
    if not text:
        return None
    direct = to_float(text)
    if direct is not None:
        return direct / 1_000_000.0 if direct > 1_000_000 else direct
    parts = text.split(":")
    if len(parts) != 3:
        return None
    try:
        hours = float(parts[0])
        minutes = float(parts[1])
        seconds = float(parts[2])
    except ValueError:
        return None
    return (hours * 3600.0) + (minutes * 60.0) + seconds


def normalize_text(value: Any) -> str:
    text = str(value or "").strip().casefold()
    for source, replacement in {"čeština": "cestina", "česky": "cesky", "angličtina": "anglictina", "komentář": "komentar"}.items():
        text = text.replace(source, replacement)
    text = re.sub(r"[._\-]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_language(value: Any) -> str:
    text = normalize_text(value)
    if text in {"", "und", "unknown", "none"}:
        return "und"
    for language, aliases in LANGUAGE_ALIASES.items():
        if text in aliases:
            return language
    return text or "und"


def detect_language(stream: dict[str, Any]) -> tuple[str, str]:
    tags = stream.get("tags") or {}
    language = normalize_language(tags.get("language") or stream.get("language"))
    if language != "und":
        return language, "high"
    title = normalize_text(tags.get("title") or stream.get("title"))
    for normalized, aliases in LANGUAGE_ALIASES.items():
        if any(re.search(rf"(^|\b){re.escape(normalize_text(alias))}(\b|$)", title) for alias in aliases):
            return normalized, "high"
    return "und", "low"


def is_local_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        process_query_limited_information = 0x1000
        still_active = 259
        handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, int(pid))
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if not ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return False
            return exit_code.value == still_active
        finally:
            ctypes.windll.kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


class InstanceLockError(RuntimeError):
    pass


class LocalInstanceLock:
    def __init__(self, runtime_dir: Path) -> None:
        self.runtime_dir = runtime_dir
        self.path = runtime_dir / "simpleripper.pid"
        self.acquired = False

    def acquire(self) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        if self.path.exists():
            try:
                payload = read_json(self.path)
            except (OSError, json.JSONDecodeError):
                payload = {}
            pid = int(payload.get("pid") or -1)
            host = str(payload.get("hostname") or "")
            if host == socket.gethostname() and is_local_pid_running(pid):
                raise InstanceLockError(f"Another local SimpleRipper instance is already running: pid={pid}, lock={self.path}")
            self.path.unlink(missing_ok=True)
        write_json(self.path, {"app": APP_NAME, "hostname": socket.gethostname(), "pid": os.getpid(), "started_at": utc_now()})
        self.acquired = True

    def release(self) -> None:
        if self.acquired:
            self.path.unlink(missing_ok=True)
            self.acquired = False

    def __enter__(self) -> "LocalInstanceLock":
        self.acquire()
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


def run_ffprobe(path: Path, ffprobe: str) -> tuple[bool, dict[str, Any], str | None]:
    command = [ffprobe, "-v", "error", "-print_format", "json", "-show_format", "-show_streams", "-show_chapters", str(path)]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
    except OSError as exc:
        return False, {}, str(exc)
    if completed.returncode != 0:
        return False, {}, completed.stderr.strip() or "ffprobe failed"
    try:
        return True, json.loads(completed.stdout), None
    except json.JSONDecodeError as exc:
        return False, {}, f"invalid ffprobe JSON: {exc}"


def ffprobe_metadata(config: dict[str, Any], path: Path, media_type: str) -> tuple[dict[str, Any], dict[str, Any]]:
    ok, probe, error = run_ffprobe(path, str((config.get("tools") or {}).get("ffprobe") or "ffprobe"))
    if not ok:
        raise RuntimeError(error or f"ffprobe failed for {path}")
    return probe, extract_metadata(path, probe, media_type)


def parse_ffmpeg_progress_line(line: str) -> tuple[str, Any] | None:
    text = line.strip()
    if not text or "=" not in text:
        return None
    key, value = text.split("=", 1)
    key = key.strip()
    value = value.strip()
    if key in {"frame", "fps", "stream_0_0_q", "bitrate", "total_size", "out_time_ms", "out_time_us", "dup_frames", "drop_frames", "speed", "progress", "out_time"}:
        return key, value
    return None


def consume_ffmpeg_progress(stream: Any, on_update: Any) -> None:
    if stream is None:
        return
    current: dict[str, Any] = {}
    reader = getattr(stream, "buffer", stream)
    read_chunk = getattr(reader, "read1", None) or getattr(reader, "read", None)
    decoder = codecs.getincrementaldecoder("utf-8")("replace")
    pending = ""
    try:
        while True:
            chunk = read_chunk(4096)
            if not chunk:
                tail = decoder.decode(b"", final=True)
                if tail:
                    pending += tail
                break
            if isinstance(chunk, bytes):
                text = decoder.decode(chunk)
            else:
                text = str(chunk)
            if not text:
                continue
            pending += text.replace("\r\n", "\n").replace("\r", "\n")
            while "\n" in pending:
                line, pending = pending.split("\n", 1)
                parsed = parse_ffmpeg_progress_line(line)
                if not parsed:
                    continue
                key, value = parsed
                current[key] = value
                if key == "progress":
                    on_update(dict(current))
        if pending.strip():
            parsed = parse_ffmpeg_progress_line(pending)
            if parsed:
                key, value = parsed
                current[key] = value
                if key == "progress":
                    on_update(dict(current))
    finally:
        try:
            stream.close()
        except OSError:
            pass


def bitrate_kbps(value: Any) -> int | None:
    number = to_int(value)
    return None if number is None else round(number / 1000)


def extract_metadata(path: Path, probe: dict[str, Any], media_type: str = "default") -> dict[str, Any]:
    streams = probe.get("streams") or []
    fmt = probe.get("format") or {}
    tags = fmt.get("tags") or {}
    video = next((stream for stream in streams if stream.get("codec_type") == "video"), None)
    audio = [stream for stream in streams if stream.get("codec_type") == "audio"]
    subtitles = [stream for stream in streams if stream.get("codec_type") == "subtitle"]
    duration = to_float(fmt.get("duration")) or (to_float(video.get("duration")) if video else None)
    return {
        "path": str(path),
        "media_type": media_type,
        "file_size_bytes": path.stat().st_size,
        "duration_seconds": duration,
        "video_codec": video.get("codec_name") if video else None,
        "video_pix_fmt": video.get("pix_fmt") if video else None,
        "video_width": video.get("width") if video else None,
        "video_height": video.get("height") if video else None,
        "overall_bitrate_kbps": bitrate_kbps(fmt.get("bit_rate")),
        "encoded_by": str(tags.get("encoded_by") or ""),
        "audio_stream_count": len(audio),
        "subtitle_stream_count": len(subtitles),
        "audio_streams": [{"index": item.get("index"), "codec": item.get("codec_name"), "language": (item.get("tags") or {}).get("language"), "title": (item.get("tags") or {}).get("title")} for item in audio],
        "subtitle_streams": [{"index": item.get("index"), "codec": item.get("codec_name"), "language": (item.get("tags") or {}).get("language"), "title": (item.get("tags") or {}).get("title")} for item in subtitles],
        "chapters_present": bool(probe.get("chapters")),
        "is_hdr": bool(video and any(str(video.get(key) or "").lower() in {"smpte2084", "arib-std-b67"} for key in ("color_transfer", "color_primaries"))),
    }


def select_streams(config: dict[str, Any], source: dict[str, Any]) -> dict[str, Any]:
    policy = config.get("track_policy") or {}
    default_maps = ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"]
    if not policy.get("enabled", True):
        return {"applied": False, "map_arguments": default_maps, "expected_audio_stream_count": source.get("audio_stream_count"), "expected_subtitle_stream_count": source.get("subtitle_stream_count")}
    target_languages = set(policy.get("target_audio_languages") or ["cze"])
    audio_streams = source.get("audio_streams") or []
    target_audio = [stream for stream in audio_streams if detect_language(stream)[0] in target_languages]
    if not target_audio or len(audio_streams) <= 1 or not policy.get("drop_other_audio_if_target_found", True):
        return {"applied": False, "map_arguments": default_maps, "expected_audio_stream_count": source.get("audio_stream_count"), "expected_subtitle_stream_count": source.get("subtitle_stream_count")}
    args = ["-map", "0:v:0"]
    for stream in target_audio:
        args.extend(["-map", f"0:{stream['index']}"])
    expected_subtitle_count = 0
    if policy.get("keep_subtitles", True):
        args.extend(["-map", "0:s?"])
        expected_subtitle_count = source.get("subtitle_stream_count")
    args.extend(["-map", "0:t?"])
    return {"applied": True, "map_arguments": args, "expected_audio_stream_count": len(target_audio), "expected_subtitle_stream_count": expected_subtitle_count}


def build_ffmpeg_command(config: dict[str, Any], source: Path, output: Path, metadata: dict[str, Any], stream_policy: dict[str, Any]) -> list[str]:
    settings = (config.get("quality_profiles") or {}).get(metadata.get("media_type") or "default") or (config.get("quality_profiles") or {}).get("default") or {}
    command = [str((config.get("tools") or {}).get("ffmpeg") or "ffmpeg"), "-hide_banner", "-nostats", "-progress", "pipe:1", "-y", "-i", str(source)]
    default_maps = ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"]
    map_arguments = list(stream_policy.get("map_arguments") or default_maps)
    if any(option == "-map" and index + 1 < len(map_arguments) and map_arguments[index + 1] == "0" for index, option in enumerate(map_arguments)):
        map_arguments = default_maps
    command.extend(map_arguments)
    command.extend(["-c:v", str(settings.get("encoder", "libx265")), "-preset", str(settings.get("preset", "medium")), "-crf", str(settings.get("crf", 24))])
    if settings.get("pix_fmt"):
        command.extend(["-pix_fmt", str(settings["pix_fmt"])])
    command.extend(["-c:a", str(settings.get("audio", "copy")), "-c:s", str(settings.get("subtitles", "copy")), "-c:t", "copy", "-map_metadata", "0", "-map_chapters", "0", "-metadata", f"encoded_by={APP_NAME}", str(output)])
    return command


def resolution_bucket(item: dict[str, Any]) -> str:
    height = to_int(item.get("video_height"))
    width = to_int(item.get("video_width"))
    largest = max(value for value in (height, width) if value is not None) if height or width else None
    if height and height >= 1800 or largest and largest >= 3000:
        return "4k"
    if height and height <= 800:
        return "720p"
    return "1080p"


def bitrate_threshold(config: dict[str, Any], media_type: str, item: dict[str, Any]) -> dict[str, Any]:
    defaults = {"anime": (500, 700), "series": (900, 1200), "movie": (1200, 1800), "default": (900, 1200)}
    bucket = resolution_bucket(item)
    hard, warning = defaults.get(media_type, defaults["default"])
    scale = {"720p": 0.6, "1080p": 1.0, "4k": 2.75}.get(bucket, 1.0)
    threshold = {"media_type": media_type, "resolution": bucket, "hard_fail_kbps": int(round(hard * scale)), "warning_kbps": int(round(warning * scale))}
    override = (((config.get("verification") or {}).get("bitrate_thresholds") or {}).get(media_type) or {}).get(bucket)
    if not override:
        override = (((config.get("verification") or {}).get("bitrate_thresholds") or {}).get("default") or {}).get(bucket)
    if override:
        threshold.update({key: int(value) for key, value in override.items() if value is not None})
    return threshold


def estimated_bitrate_kbps(size_bytes: int, duration_seconds: Any) -> int | None:
    duration = to_float(duration_seconds)
    return int(round(size_bytes * 8 / duration / 1000)) if duration and duration > 0 and size_bytes > 0 else None


def verify_output(config: dict[str, Any], source: dict[str, Any], output: Path, output_metadata: dict[str, Any], stream_policy: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    limits = config.get("verification") or {}
    settings = (config.get("quality_profiles") or {}).get(str(source.get("media_type") or "default")) or (config.get("quality_profiles") or {}).get("default") or {}
    output_size = output.stat().st_size if output.exists() else 0
    source_size = int(source.get("file_size_bytes") or 0)
    ratio = output_size / source_size if source_size else None
    threshold = bitrate_threshold(config, str(source.get("media_type") or "default"), output_metadata or source)
    bitrate = output_metadata.get("overall_bitrate_kbps") or estimated_bitrate_kbps(output_size, output_metadata.get("duration_seconds") or source.get("duration_seconds"))
    expected_audio = stream_policy.get("expected_audio_stream_count") if stream_policy.get("applied") else source.get("audio_stream_count")
    expected_subtitle = stream_policy.get("expected_subtitle_stream_count") if stream_policy.get("applied") else source.get("subtitle_stream_count")
    duration_diff = abs(float(source.get("duration_seconds") or 0) - float(output_metadata.get("duration_seconds") or 0)) if source.get("duration_seconds") and output_metadata.get("duration_seconds") else None
    warning_reasons = []
    if ratio is not None and ratio <= float(limits.get("low_ratio_warning", 0.15)):
        warning_reasons.append(f"low output/source ratio {ratio:.3f}")
    if bitrate is not None and bitrate < threshold["warning_kbps"]:
        warning_reasons.append(f"bitrate {bitrate} kbps below warning threshold {threshold['warning_kbps']} kbps")
    verification = {
        "output_exists": output.exists(),
        "output_non_empty": output_size > 0,
        "duration_ok": duration_diff is None or duration_diff <= float(limits.get("max_duration_diff_seconds", 2)),
        "video_stream_exists": bool(output_metadata.get("video_codec")),
        "video_codec_ok": not settings.get("encoder") or output_metadata.get("video_codec") in expected_video_codecs(settings.get("encoder")),
        "pix_fmt_ok": not settings.get("pix_fmt") or str(output_metadata.get("video_pix_fmt") or "") == str(settings.get("pix_fmt") or ""),
        "audio_streams_ok": output_metadata.get("audio_stream_count") == expected_audio and int(output_metadata.get("audio_stream_count") or 0) >= 1,
        "subtitle_streams_ok": output_metadata.get("subtitle_stream_count") == expected_subtitle,
        "size_reduction_ok": bool(source_size and output_size < source_size and ratio < float(limits.get("max_output_source_ratio", 0.95))),
        "not_suspiciously_tiny": output_size > 0 and not (bitrate is not None and bitrate < threshold["hard_fail_kbps"]),
        "source_size_bytes": source_size,
        "output_size_bytes": output_size,
        "output_to_source_ratio": ratio,
        "overall_bitrate_kbps": bitrate,
        "suspicious_size_warning": bool(warning_reasons),
        "suspicious_size_warning_reason": "; ".join(warning_reasons) if warning_reasons else None,
        "suspicious_size_hard_fail": bool(bitrate is not None and bitrate < threshold["hard_fail_kbps"]),
        "suspicious_size_threshold_used": threshold,
        "expected_audio_stream_count": expected_audio,
        "expected_subtitle_stream_count": expected_subtitle,
        "expected_stream_count_source": "track_policy" if stream_policy.get("applied") else "source",
    }
    hard_keys = ["output_exists", "output_non_empty", "duration_ok", "video_stream_exists", "video_codec_ok", "pix_fmt_ok", "audio_streams_ok", "subtitle_streams_ok", "size_reduction_ok", "not_suspiciously_tiny"]
    errors = [f"Verification failed: {key}" for key in hard_keys if not verification.get(key)]
    return verification, errors


def expected_video_codecs(encoder: Any) -> set[str]:
    text = str(encoder or "").strip().lower()
    mapping = {
        "libx265": {"hevc", "h265"},
        "hevc_nvenc": {"hevc", "h265"},
        "libx264": {"h264", "avc"},
        "h264_nvenc": {"h264", "avc"},
        "libsvtav1": {"av1"},
        "libaom-av1": {"av1"},
    }
    return mapping.get(text, {text} if text else set())


def terminate_process_gracefully(process: subprocess.Popen[Any] | None, timeout_seconds: float = 5.0) -> None:
    if process is None or process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_seconds)


def file_lock_id(source: Path) -> str:
    return hashlib.sha256(str(source.resolve()).encode("utf-8", errors="replace")).hexdigest()


def source_lock_path(source: Path, config: dict[str, Any]) -> Path:
    lock_dir = runtime_dir(config) / "file_locks"
    return lock_dir / f"{file_lock_id(source)}.json"


def write_sidecar_markers_enabled(config: dict[str, Any]) -> bool:
    return bool((config.get("scan") or {}).get("write_sidecar_markers", False))


def marker_path(source: Path, config: dict[str, Any]) -> Path | None:
    if not write_sidecar_markers_enabled(config):
        return None
    suffix = str(((config.get("scan") or {}).get("processed_marker_suffix")) or ".simpleripper.done.json")
    return source.with_name(source.name + suffix)


def write_source_lock(source: Path, config: dict[str, Any]) -> Path | None:
    path = source_lock_path(source, config)
    if path.exists():
        try:
            payload = read_json(path)
        except (OSError, json.JSONDecodeError):
            return None
        if payload.get("hostname") == socket.gethostname() and not is_local_pid_running(int(payload.get("pid") or -1)):
            path.unlink(missing_ok=True)
        else:
            return None
    payload = {"hostname": socket.gethostname(), "pid": os.getpid(), "created_at": utc_now(), "source_path": str(source)}
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("x", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False)
            handle.write("\n")
    except FileExistsError:
        return None
    return path


def clear_stale_locks(folders: list[Path], config: dict[str, Any]) -> list[str]:
    removed: list[str] = []
    for path in (runtime_dir(config) / "file_locks").glob("*.json"):
        try:
            payload = read_json(path)
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("hostname") == socket.gethostname() and not is_local_pid_running(int(payload.get("pid") or -1)):
            path.unlink(missing_ok=True)
            removed.append(str(path))
    return removed


def is_allowed_folder(folder: Path, roots: list[Path]) -> bool:
    try:
        resolved = folder.resolve()
    except OSError:
        return False
    for root in roots:
        try:
            resolved.relative_to(root.resolve())
            return True
        except ValueError:
            continue
    return False


def configured_folder_suggestions(config: dict[str, Any]) -> tuple[list[str], str]:
    suggestions: list[str] = []
    seen: set[str] = set()

    def add_path(value: Any) -> None:
        text = str(value or "").strip()
        if not text or text in seen:
            return
        seen.add(text)
        suggestions.append(text)

    if os.name == "nt":
        for item in ((config.get("libraries") or {}).get("roots") or []):
            add_path(item)
        return suggestions, "windows-roots"

    linux_mounts = config.get("linux-nas") or {}
    libraries = linux_mounts.get("libraries") or {}
    if isinstance(libraries, dict):
        for paths in libraries.values():
            for item in paths or []:
                add_path(item)
    for item in (linux_mounts.get("mounts") or []):
        add_path(item)
    if not suggestions:
        for item in ((config.get("libraries") or {}).get("roots") or []):
            add_path(item)
    return suggestions, "linux-mounts"


def cache_path_row(row: sqlite3.Row) -> Path:
    return Path(str(row["path"]))


def scan_excluded_tokens(config: dict[str, Any]) -> tuple[str, ...]:
    return tuple(str(item).lower() for item in ((config.get("scan") or {}).get("exclude_paths") or ["/.ripper_state/", "/.ripper_quarantine/", "/.simpleripper_quarantine/", "/.simpleripper_locks/", "/@eadir/", "/#recycle/"]))


def scan_file_extensions(config: dict[str, Any]) -> set[str]:
    return {str(item).lower() for item in ((config.get("scan") or {}).get("file_extensions") or VIDEO_EXTENSIONS)}


def path_is_scan_excluded(path: Path, config: dict[str, Any]) -> bool:
    normalized = str(path).replace("\\", "/").lower()
    return any(token in normalized for token in scan_excluded_tokens(config))


def path_is_ignored_video_name(path: Path) -> bool:
    name = path.name.lower()
    return any(token in name for token in (".sample.", "trailer", "extras", "behind the scenes")) or name.endswith((".original", ".tmp", ".partial"))


def direct_folder_signature(folder: Path, config: dict[str, Any]) -> dict[str, int]:
    direct_file_count = 0
    direct_total_size = 0
    direct_latest_mtime_ns = 0
    child_dir_count = 0
    listing_error = 0
    try:
        children = list(folder.iterdir())
    except OSError:
        return {"direct_file_count": 0, "direct_total_size": 0, "direct_latest_mtime_ns": 0, "child_dir_count": 0, "listing_error": 1}
    for child in children:
        if path_is_scan_excluded(child, config):
            continue
        try:
            if child.is_dir():
                child_dir_count += 1
                try:
                    direct_latest_mtime_ns = max(direct_latest_mtime_ns, int(child.stat().st_mtime_ns))
                except OSError:
                    listing_error = 1
            elif child.is_file():
                stat = child.stat()
                direct_file_count += 1
                direct_total_size += int(stat.st_size)
                direct_latest_mtime_ns = max(direct_latest_mtime_ns, int(stat.st_mtime_ns))
        except OSError:
            listing_error = 1
            continue
    return {
        "direct_file_count": direct_file_count,
        "direct_total_size": direct_total_size,
        "direct_latest_mtime_ns": direct_latest_mtime_ns,
        "child_dir_count": child_dir_count,
        "listing_error": listing_error,
    }


def folder_signature_matches(row: sqlite3.Row | None, signature: dict[str, int], current_policy_hash: str) -> bool:
    if signature.get("listing_error") or not row or str(row["state"]) != "clean" or str(row["policy_hash"] or "") != current_policy_hash:
        return False
    return all(int(row[key]) == int(signature[key]) for key in ("direct_file_count", "direct_total_size", "direct_latest_mtime_ns", "child_dir_count"))


def folder_cache_valid_for_skip(config: dict[str, Any], row: sqlite3.Row | None, signature: dict[str, int], current_policy_hash: str, completed_generation_id: str | None) -> bool:
    if not folder_signature_matches(row, signature, current_policy_hash) or not row or int(row["scan_complete"] or 0) != 1:
        return False
    if scan_cache_settings(config).get("folder_clean_requires_full_inventory") and str(row["inventory_generation_id"] or "") != str(completed_generation_id or ""):
        return False
    return True


def folder_summary_from_row(row: sqlite3.Row) -> dict[str, int | bool]:
    return {
        "total_relevant_files": int(row["total_relevant_files"] or 0),
        "terminal_files": int(row["terminal_files"] or 0),
        "unknown_files": int(row["unknown_files"] or 0),
        "failed_retry_blocked_files": int(row["failed_retry_blocked_files"] or 0),
        "child_folders_total": int(row["child_folders_total"] or 0),
        "child_folders_clean": int(row["child_folders_clean"] or 0),
        "child_folders_unknown": int(row["child_folders_unknown"] or 0),
        "scan_complete": bool(row["scan_complete"]),
    }


def direct_child_dirs_with_status(folder: Path, config: dict[str, Any]) -> tuple[list[Path], bool]:
    try:
        children = list(folder.iterdir())
    except OSError:
        return [], True
    result: list[Path] = []
    had_error = False
    for child in children:
        if path_is_scan_excluded(child, config):
            continue
        try:
            if child.is_dir():
                result.append(child)
        except OSError:
            had_error = True
    return sorted(result, key=lambda item: item.name.casefold()), had_error


def direct_child_dirs(folder: Path, config: dict[str, Any]) -> list[Path]:
    result, _had_error = direct_child_dirs_with_status(folder, config)
    return result


def direct_video_files_with_status(folder: Path, config: dict[str, Any]) -> tuple[list[Path], bool]:
    extensions = scan_file_extensions(config)
    try:
        children = list(folder.iterdir())
    except OSError:
        return [], True
    files: list[Path] = []
    had_error = False
    for child in children:
        if path_is_scan_excluded(child, config):
            continue
        try:
            if child.is_file() and child.suffix.lower() in extensions and not path_is_ignored_video_name(child):
                files.append(child)
        except OSError:
            had_error = True
    return sorted(files, key=lambda item: item.name.casefold()), had_error


def direct_video_files(folder: Path, config: dict[str, Any]) -> list[Path]:
    files, _had_error = direct_video_files_with_status(folder, config)
    return files


def child_folder_cache_clean(connection: sqlite3.Connection, folder: Path, config: dict[str, Any], current_policy_hash: str, completed_generation_id: str | None) -> bool:
    for child in direct_child_dirs(folder, config):
        row = connection.execute("SELECT * FROM folder_index WHERE path = ?", (str(child),)).fetchone()
        if not folder_cache_valid_for_skip(config, row, direct_folder_signature(child, config), current_policy_hash, completed_generation_id):
            return False
        if not child_folder_cache_clean(connection, child, config, current_policy_hash, completed_generation_id):
            return False
    return True


def update_folder_state_row(connection: sqlite3.Connection, folder: Path, config: dict[str, Any], state: str, reason: str, signature: dict[str, int], children_clean: bool, summary: dict[str, int | bool] | None = None, inventory_generation_id: str | None = None, last_error: str | None = None) -> None:
    now = utc_now()
    summary = summary or {}
    connection.execute(
        """
        INSERT INTO folder_index(path, normalized_path, state, reason, direct_file_count, direct_total_size, direct_latest_mtime_ns, child_dir_count, children_clean, total_relevant_files, terminal_files, unknown_files, failed_retry_blocked_files, child_folders_total, child_folders_clean, child_folders_unknown, scan_complete, inventory_generation_id, last_error, policy_hash, checked_at, updated_at)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(path) DO UPDATE SET
            normalized_path = excluded.normalized_path,
            state = excluded.state,
            reason = excluded.reason,
            direct_file_count = excluded.direct_file_count,
            direct_total_size = excluded.direct_total_size,
            direct_latest_mtime_ns = excluded.direct_latest_mtime_ns,
            child_dir_count = excluded.child_dir_count,
            children_clean = excluded.children_clean,
            total_relevant_files = excluded.total_relevant_files,
            terminal_files = excluded.terminal_files,
            unknown_files = excluded.unknown_files,
            failed_retry_blocked_files = excluded.failed_retry_blocked_files,
            child_folders_total = excluded.child_folders_total,
            child_folders_clean = excluded.child_folders_clean,
            child_folders_unknown = excluded.child_folders_unknown,
            scan_complete = excluded.scan_complete,
            inventory_generation_id = excluded.inventory_generation_id,
            last_error = excluded.last_error,
            policy_hash = excluded.policy_hash,
            checked_at = excluded.checked_at,
            updated_at = excluded.updated_at
        """,
        (
            str(folder), normalize_path_for_match(folder), state, reason, int(signature["direct_file_count"]), int(signature["direct_total_size"]), int(signature["direct_latest_mtime_ns"]), int(signature["child_dir_count"]), 1 if children_clean else 0,
            int(summary.get("total_relevant_files") or 0), int(summary.get("terminal_files") or 0), int(summary.get("unknown_files") or 0), int(summary.get("failed_retry_blocked_files") or 0), int(summary.get("child_folders_total") or 0), int(summary.get("child_folders_clean") or 0), int(summary.get("child_folders_unknown") or 0), 1 if summary.get("scan_complete") else 0,
            inventory_generation_id, last_error, policy_hash(config), now, now,
        ),
    )
    if state == "clean":
        log_event(config, "folder_state_updated", folder=str(folder), state=state, reason=reason, total_files=int(summary.get("total_relevant_files") or 0), terminal_files=int(summary.get("terminal_files") or 0), child_folders_clean=int(summary.get("child_folders_clean") or 0), child_folders_total=int(summary.get("child_folders_total") or 0), generation_id=inventory_generation_id)
    else:
        log_event(config, "folder_state_updated", folder=str(folder), state=state, reason=reason, unknown_files=int(summary.get("unknown_files") or 0), unclean_child_count=int(summary.get("child_folders_unknown") or 0), scan_complete=bool(summary.get("scan_complete")), generation_id=inventory_generation_id)


def direct_file_state_summary(connection: sqlite3.Connection, folder: Path, config: dict[str, Any]) -> dict[str, int]:
    current_policy_hash = policy_hash(config)
    now = utc_now()
    summary = {"total_relevant_files": 0, "terminal_files": 0, "unknown_files": 0, "failed_retry_blocked_files": 0}
    for file_path in direct_video_files(folder, config):
        summary["total_relevant_files"] += 1
        marker = marker_path(file_path, config)
        if marker is not None and marker.exists():
            summary["terminal_files"] += 1
            continue
        if source_lock_path(file_path, config).exists():
            summary["unknown_files"] += 1
            continue
        if is_history_done_for_current_source(config, file_path):
            summary["terminal_files"] += 1
            continue
        row = connection.execute("SELECT decision, policy_hash, size_bytes, mtime_ns, retry_after, next_check_after FROM file_index WHERE path = ?", (str(file_path),)).fetchone()
        try:
            stat = file_path.stat()
        except OSError:
            summary["unknown_files"] += 1
            continue
        if not row or str(row["policy_hash"] or "") != current_policy_hash or int(row["size_bytes"] or -1) != int(stat.st_size) or int(row["mtime_ns"] or -1) != int(stat.st_mtime_ns):
            summary["unknown_files"] += 1
            continue
        decision = str(row["decision"] or "")
        if decision in {"done", "skip"}:
            summary["terminal_files"] += 1
        elif decision == "failed" and str(row["retry_after"] or "") > now:
            summary["terminal_files"] += 1
            summary["failed_retry_blocked_files"] += 1
        elif decision == "blocked" and (not row["next_check_after"] or str(row["next_check_after"]) > now):
            summary["terminal_files"] += 1
            summary["failed_retry_blocked_files"] += 1
        else:
            summary["unknown_files"] += 1
    return summary


def refresh_folder_state(connection: sqlite3.Connection, folder: Path, config: dict[str, Any], inventory_generation_id: str | None = None) -> str:
    signature = direct_folder_signature(folder, config)
    child_folders, child_listing_error = direct_child_dirs_with_status(folder, config)
    child_folders_clean = 0
    child_folders_unknown = 0
    for child in child_folders:
        row = connection.execute("SELECT * FROM folder_index WHERE path = ?", (str(child),)).fetchone()
        child_signature = direct_folder_signature(child, config)
        child_current_generation = bool(row) and (not inventory_generation_id or str(row["inventory_generation_id"] or "") == str(inventory_generation_id))
        if row and child_current_generation and folder_signature_matches(row, child_signature, policy_hash(config)) and int(row["scan_complete"] or 0) == 1:
            child_folders_clean += 1
        else:
            child_folders_unknown += 1
    file_summary = direct_file_state_summary(connection, folder, config)
    scan_complete = not bool(signature.get("listing_error")) and not child_listing_error and child_folders_unknown == 0
    summary: dict[str, int | bool] = {
        **file_summary,
        "child_folders_total": len(child_folders),
        "child_folders_clean": child_folders_clean,
        "child_folders_unknown": child_folders_unknown,
        "scan_complete": scan_complete,
    }
    children_clean = child_folders_unknown == 0 and child_folders_clean == len(child_folders)
    has_required_generation = bool(inventory_generation_id) or not scan_cache_settings(config).get("folder_clean_requires_full_inventory")
    if has_required_generation and scan_complete and int(summary["unknown_files"]) == 0 and int(summary["terminal_files"]) == int(summary["total_relevant_files"]) and children_clean:
        state = "clean"
        reason = "all_files_done_or_skipped"
    else:
        state = "partial"
        reason = "requires_full_inventory" if not has_required_generation else "unknown_or_incomplete"
    update_folder_state_row(connection, folder, config, state, reason, signature, children_clean, summary, inventory_generation_id, "listing_error" if signature.get("listing_error") else None)
    return state


def refresh_folder_state_upwards(config: dict[str, Any], source: Path) -> None:
    if not folder_state_cache_enabled(config):
        return
    root_candidates = [Path(path) for path in ((config.get("libraries") or {}).get("roots") or [])]
    root_candidates.extend(Path(item["path"]) for item in normalize_selected_folder_entries((config.get("scan") or {}).get("selected_folders") or []))
    try:
        source_resolved = source.resolve()
    except OSError:
        source_resolved = source
    roots: list[Path] = []
    for root in root_candidates:
        try:
            root_resolved = root.resolve()
            source_resolved.relative_to(root_resolved)
            roots.append(root_resolved)
        except (OSError, ValueError):
            continue
    stop_root = max(roots, key=lambda item: len(str(item)), default=source.parent.resolve() if source.parent.exists() else source.parent)
    with open_worker_cache(config) as connection:
        current = source.parent if source.suffix else source
        while True:
            refresh_folder_state(connection, current, config)
            try:
                current_resolved = current.resolve()
            except OSError:
                current_resolved = current
            if current_resolved == stop_root:
                break
            parent = current.parent
            if parent == current:
                break
            current = parent


def fast_inventory_scan(folders: list[Path], config: dict[str, Any]) -> dict[str, Any]:
    now = utc_now()
    generation_id = f"{now}-{os.getpid()}"
    scope_fingerprint = scan_scope_fingerprint(config, folders)
    seen = 0
    changed = 0
    skipped_folders = 0
    queue_count = 0
    current_policy_hash = policy_hash(config)
    settings = scan_cache_settings(config)
    completed_generation_id = scan_state_get(config, "last_completed_inventory_generation_id")
    log_event(config, "inventory_refresh_started", folders=[str(folder) for folder in folders], generation_id=generation_id, scope=scope_fingerprint, cache_path=str(worker_cache_path(config)))

    def scan_folder(connection: sqlite3.Connection, folder: Path) -> None:
        nonlocal seen, changed, skipped_folders
        if not folder.exists() or not folder.is_dir() or path_is_scan_excluded(folder, config):
            return
        signature = direct_folder_signature(folder, config)
        row = connection.execute("SELECT * FROM folder_index WHERE path = ?", (str(folder),)).fetchone()
        if settings.get("skip_clean_folders") and folder_cache_valid_for_skip(config, row, signature, current_policy_hash, completed_generation_id) and child_folder_cache_clean(connection, folder, config, current_policy_hash, completed_generation_id):
            skipped_folders += 1
            assert row is not None
            update_folder_state_row(connection, folder, config, "clean", "folder_state_clean", signature, True, folder_summary_from_row(row), generation_id)
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("last_skipped_folder", str(folder), utc_now()),
            )
            log_event(config, "folder_scan_skipped", folder=str(folder), reason="folder_state_clean", generation_id=generation_id)
            return
        if row and str(row["state"] or "") == "clean":
            update_folder_state_row(connection, folder, config, "stale", "signature_or_policy_changed", signature, False, {"scan_complete": False}, generation_id)
        for child_dir in direct_child_dirs(folder, config):
            scan_folder(connection, child_dir)
        for path in direct_video_files(folder, config):
            marker = marker_path(path, config)
            if marker is not None and marker.exists():
                continue
            if source_lock_path(path, config).exists():
                continue
            stat = path.stat()
            path_text = str(path)
            row = connection.execute("SELECT size_bytes, mtime_ns FROM file_index WHERE path = ?", (path_text,)).fetchone()
            same_file = bool(row and int(row["size_bytes"]) == int(stat.st_size) and int(row["mtime_ns"]) == int(stat.st_mtime_ns))
            if not same_file:
                changed += 1
            decision = None
            decision_reason = None
            decision_policy_hash = None
            failure_count = None
            last_error = None
            retry_after = None
            if is_history_done_for_current_source(config, path):
                decision = "done"
                decision_reason = "history_done"
                decision_policy_hash = current_policy_hash
            else:
                failure_info = recent_ffmpeg_failure_info(config, path)
                if failure_info:
                    decision = "failed"
                    decision_reason = "recent_ffmpeg_failure"
                    decision_policy_hash = current_policy_hash
                    failure_count = int(failure_info.get("failure_count") or 1)
                    last_error = failure_info.get("error")
                    retry_after = failure_info.get("retry_after")
                    log_event(config, "candidate_scan_skipped", source_path=str(path), reason="recent_ffmpeg_failure", **failure_info)
            connection.execute(
                """
                INSERT INTO file_index(path, normalized_path, media_type, size_bytes, mtime_ns, suffix, parent_dir, score, decision, decision_reason, policy_hash, failure_count, last_error, retry_after, last_seen_at, last_fast_scanned_at, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    normalized_path = excluded.normalized_path,
                    media_type = excluded.media_type,
                    size_bytes = excluded.size_bytes,
                    mtime_ns = excluded.mtime_ns,
                    suffix = excluded.suffix,
                    parent_dir = excluded.parent_dir,
                    score = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.score ELSE excluded.score END,
                    duration_seconds = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.duration_seconds ELSE NULL END,
                    video_codec = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.video_codec ELSE NULL END,
                    video_pix_fmt = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.video_pix_fmt ELSE NULL END,
                    width = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.width ELSE NULL END,
                    height = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.height ELSE NULL END,
                    overall_bitrate_kbps = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.overall_bitrate_kbps ELSE NULL END,
                    audio_stream_count = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.audio_stream_count ELSE NULL END,
                    subtitle_stream_count = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.subtitle_stream_count ELSE NULL END,
                    decision = CASE
                        WHEN excluded.decision IS NOT NULL THEN excluded.decision
                        WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.decision
                        ELSE NULL
                    END,
                    decision_reason = CASE
                        WHEN excluded.decision_reason IS NOT NULL THEN excluded.decision_reason
                        WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.decision_reason
                        ELSE NULL
                    END,
                    estimated_saved_bytes = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.estimated_saved_bytes ELSE NULL END,
                    policy_hash = CASE
                        WHEN excluded.policy_hash IS NOT NULL THEN excluded.policy_hash
                        WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.policy_hash
                        ELSE NULL
                    END,
                    failure_count = CASE
                        WHEN excluded.failure_count IS NOT NULL THEN excluded.failure_count
                        WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.failure_count
                        ELSE 0
                    END,
                    last_error = CASE
                        WHEN excluded.last_error IS NOT NULL THEN excluded.last_error
                        WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.last_error
                        ELSE NULL
                    END,
                    retry_after = CASE
                        WHEN excluded.retry_after IS NOT NULL THEN excluded.retry_after
                        WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.retry_after
                        ELSE NULL
                    END,
                    next_check_after = CASE WHEN file_index.size_bytes = excluded.size_bytes AND file_index.mtime_ns = excluded.mtime_ns THEN file_index.next_check_after ELSE NULL END,
                    last_seen_at = excluded.last_seen_at,
                    last_fast_scanned_at = excluded.last_fast_scanned_at,
                    updated_at = excluded.updated_at
                """,
                (path_text, normalize_path_for_match(path), guess_media_type(path), int(stat.st_size), int(stat.st_mtime_ns), path.suffix.lower(), str(path.parent), float(stat.st_size), decision, decision_reason, decision_policy_hash, failure_count, last_error, retry_after, now, now, now),
            )
            seen += 1
        refresh_folder_state(connection, folder, config, generation_id)

    try:
        with open_worker_cache(config) as connection:
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("current_inventory_generation_id", generation_id, now),
            )
            for folder in folders:
                scan_folder(connection, folder)
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("last_fast_inventory_scan_at", now, now),
            )
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("last_completed_inventory_generation_id", generation_id, now),
            )
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("last_policy_hash", policy_hash(config), now),
            )
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("candidate_queue_scope_fingerprint", scope_fingerprint, now),
            )
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("candidate_queue_generated_at", now, now),
            )
            connection.execute(
                "INSERT INTO scan_state(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at",
                ("candidate_queue_generation_id", generation_id, now),
            )
            queue_row = connection.execute("SELECT COUNT(*) AS count FROM file_index WHERE decision IS NULL OR decision = 'encode_candidate'").fetchone()
            queue_count = int(queue_row["count"] or 0) if queue_row else 0
    except Exception:
        with open_worker_cache(config) as connection:
            connection.execute("UPDATE folder_index SET state = 'partial', reason = 'inventory_incomplete', scan_complete = 0, updated_at = ? WHERE inventory_generation_id = ? AND state = 'clean'", (utc_now(), generation_id))
        log_event(config, "inventory_refresh_incomplete", generation_id=generation_id)
        raise
    log_event(config, "inventory_refresh_done", indexed_files=seen, changed_files=changed, skipped_folders=skipped_folders, generation_id=generation_id, scope=scope_fingerprint, cache_path=str(worker_cache_path(config)))
    log_event(config, "candidate_queue_refreshed", count=queue_count, scope=scope_fingerprint)
    log_event(config, "candidate_queue_rebuilt", count=queue_count, scope=scope_fingerprint, generation_id=generation_id)
    log_event(config, "fast_inventory_scan_done", indexed_files=seen, changed_files=changed, skipped_folders=skipped_folders, cache_path=str(worker_cache_path(config)))
    return {"indexed_files": seen, "changed_files": changed, "skipped_folders": skipped_folders, "generation_id": generation_id, "scope_fingerprint": scope_fingerprint, "last_fast_inventory_scan_at": now}


def fast_inventory_due(config: dict[str, Any]) -> bool:
    last_scan = parse_utc_datetime(scan_state_get(config, "last_fast_inventory_scan_at"))
    if last_scan is None:
        return True
    return time.time() >= last_scan.timestamp() + scan_cache_settings(config)["fast_inventory_rescan_hours"] * 3600


def ensure_fast_inventory_scan(config: dict[str, Any], folders: list[Path], force: bool = False) -> dict[str, Any] | None:
    if not scan_cache_enabled(config):
        return None
    with open_worker_cache(config):
        pass
    if force or fast_inventory_due(config):
        return fast_inventory_scan(folders, config)
    return None


def cached_candidate_paths(config: dict[str, Any], folders: list[Path] | None = None, scope_match: bool = True) -> list[Path]:
    settings = scan_cache_settings(config)
    queue_size = max(1, int(settings.get("queue_size") or 25))
    current_policy_hash = policy_hash(config)
    now = utc_now()
    paths: list[Path] = []
    scope_entries = selected_scope_entries(config, folders)
    with open_worker_cache(config) as connection:
        rows = connection.execute(
            """
            SELECT path, size_bytes, mtime_ns FROM file_index
            WHERE
                decision IS NULL
                OR decision = 'encode_candidate'
                OR policy_hash IS NULL
                OR policy_hash != ?
                OR (decision = 'failed' AND retry_after IS NOT NULL AND retry_after <= ?)
            ORDER BY COALESCE(score, size_bytes) DESC, size_bytes DESC
            """,
            (current_policy_hash, now),
        ).fetchall()
        for row in rows:
            path = cache_path_row(row)
            if scope_entries and not path_in_scope(path, scope_entries):
                continue
            marker = marker_path(path, config)
            if not path.exists() or (marker is not None and marker.exists()) or source_lock_path(path, config).exists():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            if int(row["size_bytes"] or 0) != int(stat.st_size) or int(row["mtime_ns"] or 0) != int(stat.st_mtime_ns):
                connection.execute(
                    "UPDATE file_index SET size_bytes = ?, mtime_ns = ?, decision = NULL, decision_reason = NULL, policy_hash = NULL, updated_at = ? WHERE path = ?",
                    (int(stat.st_size), int(stat.st_mtime_ns), utc_now(), str(path)),
                )
            paths.append(path)
            if len(paths) >= queue_size:
                break
    log_event(config, "candidate_queue_loaded", count=len(paths), scope_match=scope_match)
    if not paths:
        log_event(config, "candidate_queue_empty")
    return paths


def estimated_saved_bytes_from_details(details: dict[str, Any]) -> int | None:
    retention = details.get("retention_size_policy") or {}
    actual_mb = retention.get("actual_mb")
    limit_mb = retention.get("limit_mb")
    if actual_mb is None or limit_mb is None:
        return None
    try:
        return max(0, int((float(actual_mb) - float(limit_mb)) * 1024 * 1024))
    except (TypeError, ValueError):
        return None


def update_cache_deep_check(config: dict[str, Any], details: dict[str, Any]) -> None:
    if not scan_cache_enabled(config):
        return
    source = Path(str(details.get("path")))
    if not source.exists():
        return
    metadata = details.get("metadata") or {}
    decision = "encode_candidate"
    decision_reason = details.get("candidate_reason") or "needs_encode"
    settings = scan_cache_settings(config)
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat(timespec="seconds")
    failure_count = 0
    last_error = None
    last_failure_at = None
    retry_after = None
    next_check_after = None
    if details.get("status") != "ok":
        with open_worker_cache(config) as connection:
            row = connection.execute("SELECT failure_count FROM file_index WHERE path = ?", (str(source),)).fetchone()
        failure_count = int(row["failure_count"] or 0) + 1 if row else 1
        decision = "blocked" if failure_count >= int(settings["max_failures_before_block"]) else "failed"
        decision_reason = "deep_check_blocked" if decision == "blocked" else str(details.get("status") or "deep_check_failed")
        last_error = str(details.get("error") or details.get("status") or "deep_check_failed")
        last_failure_at = now
        retry_after = datetime.fromtimestamp(now_dt.timestamp() + float(settings["failed_retry_hours"]) * 3600, timezone.utc).isoformat(timespec="seconds") if decision == "failed" else None
    elif details.get("skip_reason"):
        decision = "skip"
        decision_reason = str(details.get("skip_reason"))
    stat = source.stat()
    with open_worker_cache(config) as connection:
        connection.execute(
            """
            UPDATE file_index SET
                media_type = ?, size_bytes = ?, mtime_ns = ?, duration_seconds = ?, video_codec = ?, video_pix_fmt = ?, width = ?, height = ?,
                overall_bitrate_kbps = ?, audio_stream_count = ?, subtitle_stream_count = ?, decision = ?, decision_reason = ?, score = ?,
                estimated_saved_bytes = ?, policy_hash = ?, failure_count = ?, last_error = ?, last_failure_at = ?, retry_after = ?, next_check_after = ?, last_deep_checked_at = ?, updated_at = ?
            WHERE path = ?
            """,
            (
                metadata.get("media_type") or guess_media_type(source),
                int(stat.st_size),
                int(stat.st_mtime_ns),
                metadata.get("duration_seconds"),
                metadata.get("video_codec"),
                metadata.get("video_pix_fmt"),
                metadata.get("video_width"),
                metadata.get("video_height"),
                metadata.get("overall_bitrate_kbps"),
                metadata.get("audio_stream_count"),
                metadata.get("subtitle_stream_count"),
                decision,
                decision_reason,
                float(details.get("score") or 0),
                estimated_saved_bytes_from_details(details),
                policy_hash(config),
                failure_count,
                last_error,
                last_failure_at,
                retry_after,
                next_check_after,
                now,
                now,
                str(source),
            ),
        )
    log_event(config, "file_state_updated", source_path=str(source), state=decision, reason=decision_reason)
    if details.get("status") != "ok":
        log_event(config, "candidate_deep_check_failed", source_path=str(source), error=last_error, failure_count=failure_count, decision=decision)
        if decision == "blocked":
            log_event(config, "candidate_blocked", source_path=str(source), error=last_error, failure_count=failure_count)
        else:
            log_event(config, "candidate_retry_scheduled", source_path=str(source), error=last_error, failure_count=failure_count, retry_after=retry_after)
    refresh_folder_state_upwards(config, source)


def update_cache_job_success(config: dict[str, Any], source: Path, replacement: Path | None = None) -> None:
    if not scan_cache_enabled(config):
        return
    now = utc_now()
    with open_worker_cache(config) as connection:
        connection.execute(
            "UPDATE file_index SET decision = 'done', decision_reason = 'job_done', failure_count = 0, last_error = NULL, last_failure_at = NULL, retry_after = NULL, next_check_after = NULL, policy_hash = ?, updated_at = ? WHERE path = ?",
            (policy_hash(config), now, str(source)),
        )
        if replacement is not None:
            replacement_stat = replacement.stat() if replacement.exists() else None
            connection.execute(
                """
                INSERT INTO file_index(path, normalized_path, media_type, size_bytes, mtime_ns, suffix, parent_dir, decision, decision_reason, policy_hash, failure_count, last_error, last_failure_at, retry_after, next_check_after, updated_at)
                VALUES(?, ?, ?, ?, ?, ?, ?, 'done', 'job_done', ?, 0, NULL, NULL, NULL, NULL, ?)
                ON CONFLICT(path) DO UPDATE SET decision = 'done', decision_reason = 'job_done', policy_hash = excluded.policy_hash,
                    size_bytes = excluded.size_bytes, mtime_ns = excluded.mtime_ns, suffix = excluded.suffix, parent_dir = excluded.parent_dir,
                    failure_count = 0, last_error = NULL, last_failure_at = NULL, retry_after = NULL, next_check_after = NULL, updated_at = excluded.updated_at
                """,
                (
                    str(replacement),
                    normalize_path_for_match(replacement),
                    guess_media_type(replacement),
                    int(replacement_stat.st_size) if replacement_stat else 0,
                    int(replacement_stat.st_mtime_ns) if replacement_stat else 0,
                    replacement.suffix.lower(),
                    str(replacement.parent),
                    policy_hash(config),
                    now,
                ),
            )
    log_event(config, "file_state_updated", source_path=str(source), state="done", reason="job_done")
    refresh_folder_state_upwards(config, source)
    if replacement is not None:
        log_event(config, "file_state_updated", source_path=str(replacement), state="done", reason="job_done")
        refresh_folder_state_upwards(config, replacement)


def update_cache_job_failure(config: dict[str, Any], source: Path, error: str) -> dict[str, Any] | None:
    if not scan_cache_enabled(config):
        return None
    settings = scan_cache_settings(config)
    now_dt = datetime.now(timezone.utc)
    now = now_dt.isoformat(timespec="seconds")
    retry_after = (now_dt.timestamp() + float(settings["failed_retry_hours"]) * 3600)
    blocked_after = (now_dt.timestamp() + float(settings["blocked_retry_days"]) * 86400)
    with open_worker_cache(config) as connection:
        row = connection.execute("SELECT failure_count FROM file_index WHERE path = ?", (str(source),)).fetchone()
        failure_count = int(row["failure_count"] or 0) + 1 if row else 1
        decision = "blocked" if failure_count >= int(settings["max_failures_before_block"]) else "failed"
        next_check_after = datetime.fromtimestamp(blocked_after, timezone.utc).isoformat(timespec="seconds") if decision == "blocked" else None
        retry_text = datetime.fromtimestamp(retry_after, timezone.utc).isoformat(timespec="seconds") if decision == "failed" else None
        connection.execute(
            """
            INSERT INTO file_index(path, normalized_path, media_type, size_bytes, mtime_ns, suffix, parent_dir, decision, decision_reason, policy_hash, failure_count, last_error, last_failure_at, retry_after, next_check_after, updated_at)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET decision = excluded.decision, decision_reason = excluded.decision_reason, policy_hash = excluded.policy_hash,
                failure_count = excluded.failure_count, last_error = excluded.last_error, last_failure_at = excluded.last_failure_at, retry_after = excluded.retry_after, next_check_after = excluded.next_check_after, updated_at = excluded.updated_at
            """,
            (str(source), normalize_path_for_match(source), guess_media_type(source), source.stat().st_size if source.exists() else 0, source.stat().st_mtime_ns if source.exists() else 0, source.suffix.lower(), str(source.parent), decision, "repeated_ffmpeg_failure" if decision == "blocked" else "ffmpeg_failed", policy_hash(config), failure_count, error, now, retry_text, next_check_after, now),
        )
    log_event(config, "file_state_updated", source_path=str(source), state=decision, reason="repeated_ffmpeg_failure" if decision == "blocked" else "ffmpeg_failed")
    refresh_folder_state_upwards(config, source)
    return {"decision": decision, "failure_count": failure_count, "retry_after": retry_text, "next_check_after": next_check_after}


def uncached_scan_candidates(folders: list[Path], config: dict[str, Any]) -> list[Path]:
    sync_history_from_shared_workers(config)
    extensions = {str(item).lower() for item in ((config.get("scan") or {}).get("file_extensions") or VIDEO_EXTENSIONS)}
    excluded_tokens = tuple(str(item).lower() for item in ((config.get("scan") or {}).get("exclude_paths") or ["/.ripper_state/", "/.ripper_quarantine/", "/.simpleripper_quarantine/", "/.simpleripper_locks/", "/@eadir/", "/#recycle/"]))
    candidates: list[Path] = []
    for folder in folders:
        if not folder.exists() or not folder.is_dir():
            continue
        for path in folder.rglob("*"):
            normalized = str(path).replace("\\", "/").lower()
            if any(token in normalized for token in excluded_tokens):
                continue
            if any(token in path.name.lower() for token in (".sample.", "trailer", "extras", "behind the scenes")):
                continue
            marker = marker_path(path, config)
            if path.is_file() and path.suffix.lower() in extensions and (marker is None or not marker.exists()) and not source_lock_path(path, config).exists() and not is_history_done_for_current_source(config, path) and not path.name.endswith((".original", ".tmp", ".partial")):
                failure_info = recent_ffmpeg_failure_info(config, path)
                if failure_info:
                    log_event(config, "candidate_scan_skipped", source_path=str(path), reason="recent_ffmpeg_failure", **failure_info)
                    continue
                candidates.append(path)
    return sorted(candidates, key=lambda item: item.stat().st_size, reverse=True)


def scan_candidates(folders: list[Path], config: dict[str, Any]) -> list[Path]:
    if scan_cache_enabled(config):
        current_scope = scan_scope_fingerprint(config, folders)
        log_event(config, "scan_scope_fingerprint", current=current_scope)
        stored_scope = scan_state_get(config, "candidate_queue_scope_fingerprint")
        stored_generation = scan_state_get(config, "candidate_queue_generation_id")
        completed_generation = scan_state_get(config, "last_completed_inventory_generation_id")
        scope_match = bool(stored_scope and stored_scope == current_scope and stored_generation and stored_generation == completed_generation)
        if not scope_match:
            invalidate_candidate_queue_scope(
                config,
                "scope_changed" if stored_scope and stored_scope != current_scope else "missing_or_incomplete_scope",
                old_scope_fingerprint=stored_scope,
                new_scope_fingerprint=current_scope,
                old_scope_entries=selected_scope_entries(config),
                new_scope_entries=selected_scope_entries(config, folders),
            )
            ensure_fast_inventory_scan(config, folders, force=True)
            scope_match = True
        else:
            ensure_fast_inventory_scan(config, folders)
        return cached_candidate_paths(config, folders, scope_match=scope_match)
    return uncached_scan_candidates(folders, config)


def candidate_priority_score(metadata: dict[str, Any]) -> float:
    size_bytes = float(metadata.get("file_size_bytes") or 0)
    codec = str(metadata.get("video_codec") or "").lower()
    bitrate = float(metadata.get("overall_bitrate_kbps") or 0)
    audio_streams = int(metadata.get("audio_stream_count") or 0)
    subtitle_streams = int(metadata.get("subtitle_stream_count") or 0)
    encoded_by = str(metadata.get("encoded_by") or "").casefold()
    codec_bonus = {
        "vc1": 120,
        "mpeg4": 100,
        "msmpeg4v3": 95,
        "h264": 85,
        "avc": 85,
        "xvid": 90,
        "divx": 90,
        "hevc": 10,
        "h265": 10,
        "av1": 5,
    }.get(codec, 40)
    size_score = size_bytes / (1024 * 1024 * 1024)
    bitrate_score = min(bitrate / 300.0, 40.0)
    stream_score = (audio_streams * 8) + (subtitle_streams * 2)
    penalty = 0.0
    if encoded_by.casefold() == APP_NAME.casefold():
        penalty -= 500.0
    return round(size_score + codec_bonus + bitrate_score + stream_score + penalty, 2)


def should_reprocess_hevc(config: dict[str, Any], metadata: dict[str, Any]) -> bool:
    codec = str(metadata.get("video_codec") or "").lower()
    if codec not in {"hevc", "h265"}:
        return False
    rules = config.get("skip_rules") or {}
    size_mb = float(metadata.get("file_size_bytes") or 0) / (1024 * 1024)
    threshold = bitrate_threshold(config, str(metadata.get("media_type") or "default"), metadata)
    bitrate = float(metadata.get("overall_bitrate_kbps") or 0)
    min_size_mb = float(rules.get("hevc_reprocess_min_size_mb") or 12000)
    warning_multiplier = float(rules.get("hevc_reprocess_warning_multiplier") or 1.75)
    return size_mb >= min_size_mb and bitrate >= float(threshold["warning_kbps"]) * warning_multiplier


def history_summary_fields(source_meta: dict[str, Any], output_meta: dict[str, Any], verification: dict[str, Any], jellyfin_refresh: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "source_size_bytes": source_meta.get("file_size_bytes"),
        "output_size_bytes": output_meta.get("file_size_bytes") or verification.get("output_size_bytes"),
        "video_codec_before": source_meta.get("video_codec"),
        "video_codec_after": output_meta.get("video_codec"),
        "audio_stream_count_before": source_meta.get("audio_stream_count"),
        "audio_stream_count_after": output_meta.get("audio_stream_count"),
        "subtitle_stream_count_before": source_meta.get("subtitle_stream_count"),
        "subtitle_stream_count_after": output_meta.get("subtitle_stream_count"),
        "output_to_source_ratio": verification.get("output_to_source_ratio"),
        "overall_bitrate_kbps": verification.get("overall_bitrate_kbps"),
        "verification_warning": verification.get("suspicious_size_warning_reason"),
        "jellyfin_refresh": jellyfin_refresh,
    }


def summarize_result(entry: dict[str, Any] | None) -> dict[str, Any] | None:
    if not entry:
        return None
    source_size = to_int(entry.get("source_size_bytes"))
    output_size = to_int(entry.get("output_size_bytes"))
    bytes_saved = source_size - output_size if source_size is not None and output_size is not None else None
    return {
        "status": entry.get("status"),
        "source_path": entry.get("source_path"),
        "finished_at": entry.get("finished_at"),
        "skip_reason": entry.get("reason") or entry.get("skip_reason"),
        "error": entry.get("error"),
        "warning": entry.get("verification_warning"),
        "source_size_bytes": source_size,
        "output_size_bytes": output_size,
        "bytes_saved": bytes_saved,
        "output_to_source_ratio": to_float(entry.get("output_to_source_ratio")),
        "overall_bitrate_kbps": to_int(entry.get("overall_bitrate_kbps")),
        "video_codec_before": entry.get("video_codec_before"),
        "video_codec_after": entry.get("video_codec_after"),
        "audio_stream_count_before": to_int(entry.get("audio_stream_count_before")),
        "audio_stream_count_after": to_int(entry.get("audio_stream_count_after")),
        "subtitle_stream_count_before": to_int(entry.get("subtitle_stream_count_before")),
        "subtitle_stream_count_after": to_int(entry.get("subtitle_stream_count_after")),
        "quarantine_path": entry.get("quarantine_path"),
        "jellyfin_status": ((entry.get("jellyfin_refresh") or {}).get("status") if isinstance(entry.get("jellyfin_refresh"), dict) else None),
    }


def summarize_current_state(state: "RuntimeState") -> dict[str, Any]:
    progress = state.ffmpeg_progress if isinstance(state.ffmpeg_progress, dict) else {}
    progress_time = progress.get("out_time") or progress.get("time") or progress.get("out_time_us")
    progress_value_seconds = progress_seconds(progress_time)
    duration_seconds = to_float(state.current_duration_seconds)
    progress_percent = None
    if progress_value_seconds is not None and duration_seconds and duration_seconds > 0:
        progress_percent = round(min(100.0, max(0.0, (progress_value_seconds / duration_seconds) * 100.0)), 1)
    return {
        "status": state.current_phase,
        "running": state.running,
        "source_path": state.current_file,
        "next_scan_at": state.next_scan_at,
        "duration_seconds": duration_seconds,
        "progress_time": progress_time,
        "progress_percent": progress_percent,
        "progress_fps": progress.get("fps"),
        "progress_speed": progress.get("speed"),
        "output_size_bytes": state.output_size_bytes,
    }


def inspect_candidate(config: dict[str, Any], source: Path, media_type: str) -> dict[str, Any]:
    ok, probe, error = run_ffprobe(source, str((config.get("tools") or {}).get("ffprobe") or "ffprobe"))
    if not ok:
        return {"path": source, "status": "ffprobe_failed", "error": error}
    metadata = extract_metadata(source, probe, media_type)
    stream_policy = select_streams(config, metadata)
    retention_size_policy = retention_size_policy_evaluation(config, metadata, media_type)
    profile_matches, profile_mismatch_reasons = source_matches_target_profile(config, metadata, media_type, stream_policy)
    reason = skip_reason(config, metadata, stream_policy)
    score = candidate_priority_score(metadata)
    if should_reprocess_hevc(config, metadata):
        score += 35.0
    codec = str(metadata.get("video_codec") or "").lower()
    candidate_reason = None
    if retention_size_policy.get("oversized") and codec in {"hevc", "h265"}:
        candidate_reason = "hevc_oversized"
        score += 40.0
    elif retention_size_policy.get("oversized") and codec == "av1":
        candidate_reason = "av1_oversized"
        score += 40.0
    elif codec in {"hevc", "h265"} and not profile_matches:
        candidate_reason = "hevc_not_normalized"
    elif codec == "av1" and not profile_matches:
        candidate_reason = "av1_not_normalized"
    if candidate_reason:
        score += 20.0
    return {"path": source, "status": "ok", "metadata": metadata, "skip_reason": reason, "score": score, "track_policy": stream_policy, "target_profile_matches": profile_matches, "profile_mismatch_reasons": profile_mismatch_reasons, "candidate_reason": candidate_reason, "retention_size_policy": retention_size_policy}


def candidate_selection_key(candidate: dict[str, Any]) -> tuple[int, float]:
    metadata = candidate.get("metadata") or {}
    size_bytes = int(metadata.get("file_size_bytes") or 0)
    return (size_bytes, float(candidate.get("score") or 0.0))


def skip_reason(config: dict[str, Any], metadata: dict[str, Any], track_policy_result: dict[str, Any] | None = None) -> str | None:
    rules = config.get("skip_rules") or {}
    codec = str(metadata.get("video_codec") or "").lower()
    media_type = str(metadata.get("media_type") or "default")
    retention_size_policy = retention_size_policy_evaluation(config, metadata, media_type)
    oversized_reprocess = bool(retention_size_policy.get("oversized")) and codec in {"hevc", "h265", "av1"}
    if rules.get("skip_4k", True) and int(metadata.get("video_height") or 0) >= 1800:
        return "skip_4k"
    if rules.get("skip_hdr", True) and metadata.get("is_hdr") and not oversized_reprocess:
        return "skip_hdr"
    profile_matches, _profile_mismatch_reasons = source_matches_target_profile(config, metadata, media_type, track_policy_result)
    oversized = bool(retention_size_policy.get("oversized"))
    if rules.get("skip_hevc", True) and codec in {"hevc", "h265"} and profile_matches and not oversized:
        return "already_hevc"
    if rules.get("skip_av1", True) and codec == "av1" and profile_matches and not oversized:
        return "already_av1"
    if profile_matches and str(metadata.get("encoded_by") or "").casefold() == APP_NAME.casefold():
        return "already_simpleripper"
    min_duration = rules.get("min_duration_seconds")
    if min_duration is not None and (metadata.get("duration_seconds") or 0) < float(min_duration):
        return "below_min_duration"
    min_size_mb = rules.get("min_size_mb") or (config.get("scan") or {}).get("min_size_mb")
    if min_size_mb is not None and int(metadata.get("file_size_bytes") or 0) < int(min_size_mb) * 1024 * 1024:
        return "below_min_size"
    return None


def retention_size_policy_evaluation(config: dict[str, Any], source_meta: dict[str, Any], media_type: str) -> dict[str, Any]:
    policy = config.get("retention_size_policy") or {}
    enabled = bool(policy.get("enabled", False))
    bucket = media_type if media_type in {"series", "anime", "movie", "unknown"} else "unknown"
    per_media = policy.get(bucket) or {}
    try:
        max_mb_per_25min = float(per_media.get("max_mb_per_25min")) if per_media.get("max_mb_per_25min") is not None else None
    except (TypeError, ValueError):
        max_mb_per_25min = None
    duration_seconds = to_float(source_meta.get("duration_seconds"))
    duration_minutes = (duration_seconds / 60.0) if duration_seconds is not None else None
    actual_mb = (int(source_meta.get("file_size_bytes") or 0) / 1024 / 1024) if source_meta.get("file_size_bytes") is not None else None
    limit_mb = None
    oversized = False
    if enabled and max_mb_per_25min is not None and duration_minutes and duration_minutes > 0:
        limit_mb = duration_minutes / 25.0 * max_mb_per_25min
        oversized = actual_mb is not None and actual_mb > limit_mb
    return {
        "enabled": enabled,
        "media_type": bucket,
        "codec": str(source_meta.get("video_codec") or "").lower() or None,
        "max_mb_per_25min": max_mb_per_25min,
        "duration_minutes": round(duration_minutes, 1) if duration_minutes is not None else None,
        "actual_mb": round(actual_mb, 1) if actual_mb is not None else None,
        "limit_mb": round(limit_mb, 1) if limit_mb is not None else None,
        "oversized": oversized,
    }


def source_matches_target_profile(config: dict[str, Any], source_meta: dict[str, Any], media_type: str, track_policy_result: dict[str, Any] | None = None) -> tuple[bool, list[str]]:
    settings = (config.get("quality_profiles") or {}).get(media_type or "default") or (config.get("quality_profiles") or {}).get("default") or {}
    reasons: list[str] = []
    expected_codecs = expected_video_codecs(settings.get("encoder"))
    codec = str(source_meta.get("video_codec") or "").lower()
    if expected_codecs and codec not in expected_codecs:
        reasons.append(f"video_codec_mismatch:{codec or 'none'}!={','.join(sorted(expected_codecs))}")
    target_pix_fmt = str(settings.get("pix_fmt") or "").strip()
    source_pix_fmt = str(source_meta.get("video_pix_fmt") or "").strip()
    if target_pix_fmt and source_pix_fmt != target_pix_fmt:
        reasons.append(f"pix_fmt_mismatch:{source_pix_fmt or 'none'}!={target_pix_fmt}")
    retention_size_policy = retention_size_policy_evaluation(config, source_meta, media_type)
    if retention_size_policy.get("enabled") and retention_size_policy.get("oversized") and codec in {"hevc", "h265", "av1"}:
        reasons.append(
            "retention_size_exceeded:"
            f"{retention_size_policy.get('actual_mb')}MB>{retention_size_policy.get('limit_mb')}MB"
        )
    stream_policy = track_policy_result if track_policy_result is not None else select_streams(config, source_meta)
    if stream_policy.get("applied"):
        expected_audio = int(stream_policy.get("expected_audio_stream_count") or 0)
        actual_audio = int(source_meta.get("audio_stream_count") or 0)
        if actual_audio != expected_audio:
            target_languages = set((config.get("track_policy") or {}).get("target_audio_languages") or ["cze"])
            extra_languages: list[str] = []
            for stream in source_meta.get("audio_streams") or []:
                language = detect_language(stream)[0]
                if language not in target_languages:
                    extra_languages.append(language)
            if extra_languages:
                for language in sorted(set(extra_languages)):
                    reasons.append(f"audio_policy_mismatch:extra_{language}_audio")
            else:
                reasons.append(f"audio_policy_mismatch:expected_{expected_audio}_actual_{actual_audio}")
        expected_subtitles = stream_policy.get("expected_subtitle_stream_count")
        actual_subtitles = source_meta.get("subtitle_stream_count")
        if expected_subtitles is not None and actual_subtitles != expected_subtitles:
            reasons.append(f"subtitle_policy_mismatch:expected_{expected_subtitles}_actual_{actual_subtitles}")
    return not reasons, reasons


def ensure_local_free_space(config: dict[str, Any], source_size: int) -> None:
    minimum_gb = float((config.get("safety") or {}).get("min_free_disk_gb") or 0)
    if minimum_gb <= 0:
        return
    root = work_dir(config)
    root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(root).free
    required = max(int(minimum_gb * 1024 * 1024 * 1024), int(source_size * 1.5))
    if free < required:
        raise RuntimeError(f"not enough local free space: free={free}, required={required}")


LEGACY_CONTAINER_OUTPUT_SUFFIXES = {
    ".avi": ".mkv",
}


def target_output_suffix(source: Path) -> str:
    return LEGACY_CONTAINER_OUTPUT_SUFFIXES.get(source.suffix.lower(), source.suffix)


def target_output_path(source: Path) -> Path:
    suffix = target_output_suffix(source)
    return source.with_suffix(suffix) if suffix != source.suffix else source


def temp_upload_path(final_target: Path) -> Path:
    return final_target.with_name(f".{final_target.name}.simpleripper.tmp")


def quarantine_path_for_source(source: Path, config: dict[str, Any]) -> Path:
    root = Path(str((config.get("paths") or {}).get("quarantine_dir") or "quarantine"))
    relative_root_text = str((config.get("paths") or {}).get("quarantine_relative_root") or "").strip()
    if relative_root_text:
        try:
            relative_root = Path(relative_root_text).resolve()
            relative = source.resolve().relative_to(relative_root)
            return root / relative.with_name(relative.name + f".{int(time.time())}.original")
        except ValueError:
            log_event(
                config,
                "quarantine_relative_root_outside_source",
                source_path=str(source),
                quarantine_relative_root=relative_root_text,
            )
    for library_root in [Path(item) for item in ((config.get("libraries") or {}).get("roots") or [])]:
        try:
            relative = source.resolve().relative_to(library_root.resolve())
            return root / relative.with_name(relative.name + f".{int(time.time())}.original")
        except ValueError:
            continue
    return root / f"{source.name}.{int(time.time())}.original"


def is_test_mode(config: dict[str, Any]) -> bool:
    return bool(config.get("__test_mode", False))


def finalize_quarantined_original(quarantine_path: Path, config: dict[str, Any]) -> dict[str, Any]:
    retain = bool((config.get("paths") or {}).get("keep_quarantine_after_success", False)) or is_test_mode(config)
    if retain:
        reason = "test_mode" if is_test_mode(config) else "keep_quarantine_after_success"
        log_event(config, "quarantine_retained", quarantine_path=str(quarantine_path), reason=reason)
        return {"quarantine_retained": True, "quarantine_deleted": False, "quarantine_cleanup_error": None, "quarantine_retention_reason": reason}
    if not quarantine_path.exists():
        log_event(config, "quarantine_missing_after_success", quarantine_path=str(quarantine_path))
        return {"quarantine_retained": False, "quarantine_deleted": False, "quarantine_cleanup_error": None, "quarantine_retention_reason": None}
    try:
        quarantine_root = Path(str((config.get("paths") or {}).get("quarantine_dir") or "quarantine")).resolve()
        quarantine_path.unlink()
        parent = quarantine_path.parent
        while parent != quarantine_root and parent.exists():
            try:
                parent.rmdir()
            except OSError:
                break
            parent = parent.parent
        log_event(config, "quarantine_deleted", quarantine_path=str(quarantine_path))
        return {"quarantine_retained": False, "quarantine_deleted": True, "quarantine_cleanup_error": None, "quarantine_retention_reason": None}
    except OSError as exc:
        log_event(config, "quarantine_cleanup_failed", quarantine_path=str(quarantine_path), error=str(exc))
        return {"quarantine_retained": True, "quarantine_deleted": False, "quarantine_cleanup_error": str(exc), "quarantine_retention_reason": "cleanup_failed"}


def refresh_jellyfin(config: dict[str, Any], source: Path, replacement: Path | None = None) -> dict[str, Any]:
    settings = config.get("jellyfin") or {}
    if not settings.get("enabled", False):
        return {"status": "disabled"}
    server_url = str(settings.get("server_url") or "").rstrip("/")
    api_key = str(settings.get("api_key") or "")
    if not server_url or not api_key:
        return {"status": "skipped", "reason": "missing_server_url_or_api_key"}

    def refresh_item(item_id: str) -> None:
        refresh_req = request.Request(f"{server_url}/Items/{item_id}/Refresh?Recursive=false&MetadataRefreshMode=Default&ImageRefreshMode=Default", headers={"X-Emby-Token": api_key}, method="POST")
        with request.urlopen(refresh_req, timeout=10):
            pass

    lookup_path = replacement or source
    mapped_paths = jellyfin_mapped_paths(settings, lookup_path)
    try:
        lookup = jellyfin_lookup_item(server_url, api_key, lookup_path, mapped_paths, settings)
        if lookup.get("status") != "ok":
            if replacement is not None and replacement != source:
                parent_lookup = jellyfin_lookup_item(server_url, api_key, replacement.parent, jellyfin_mapped_paths(settings, replacement.parent), settings)
                if parent_lookup.get("status") != "ok":
                    return {**lookup, "fallback_status": parent_lookup.get("status"), "fallback_reason": parent_lookup.get("reason")}
                matches = parent_lookup.get("matches") or []
                if not matches and parent_lookup.get("item_id"):
                    matches = [{"item_id": parent_lookup.get("item_id"), "matched_path": parent_lookup.get("matched_path")}]
                refreshed: list[dict[str, Any]] = []
                for match in matches:
                    item_id = str(match.get("item_id") or "")
                    if not item_id:
                        continue
                    refresh_item(item_id)
                    refreshed.append({**match, "item_id": item_id, "matched_path": match.get("matched_path") or match.get("path")})
                return {
                    **parent_lookup,
                    "status": "ok",
                    "matches": refreshed,
                    "refreshed_count": len(refreshed),
                    "refresh_target": "parent",
                    "source_path": str(source),
                    "replacement_path": str(replacement),
                }
            return lookup
        matches = lookup.get("matches") or []
        if not matches and lookup.get("item_id"):
            matches = [{"item_id": lookup.get("item_id"), "matched_path": lookup.get("matched_path")}]
        refreshed: list[dict[str, Any]] = []
        for match in matches:
            item_id = str(match.get("item_id") or "")
            if not item_id:
                continue
            refresh_item(item_id)
            refreshed.append({**match, "item_id": item_id, "matched_path": match.get("matched_path") or match.get("path")})
        return {**lookup, "status": "ok", "matches": refreshed, "refreshed_count": len(refreshed), "source_path": str(source), "replacement_path": str(replacement or lookup_path)}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def normalize_path_for_match(value: str | Path) -> str:
    return str(value).replace("\\", "/").rstrip("/").casefold()


def normalize_path_for_prefix_match(value: str | Path) -> str:
    text = str(value or "").replace("\\", "/").strip()
    if not text:
        return ""
    unc_prefix = text.startswith("//")
    body = text[2:] if unc_prefix else text
    body = re.sub(r"/+", "/", body).rstrip("/")
    normalized = f"//{body}" if unc_prefix else body
    return normalized.casefold()


def split_mapped_suffix(source_text: str, fs_prefix: str) -> str | None:
    source_slash = re.sub(r"/+", "/", str(source_text or "").replace("\\", "/"))
    prefix_slash = re.sub(r"/+", "/", str(fs_prefix or "").replace("\\", "/")).rstrip("/")
    source_norm = normalize_path_for_prefix_match(source_text)
    prefix_norm = normalize_path_for_prefix_match(fs_prefix)
    if not prefix_norm or not source_norm.startswith(prefix_norm):
        return None
    remainder_norm = source_norm[len(prefix_norm):]
    if remainder_norm and not remainder_norm.startswith("/"):
        return None
    remainder = source_slash[len(prefix_slash):]
    return remainder if remainder.startswith("/") else f"/{remainder}" if remainder else ""


def jellyfin_mapped_paths(settings: dict[str, Any], source: Path) -> list[str]:
    candidates = [str(source)]
    source_text = str(source)
    mappings = sorted(
        (settings.get("path_mapping") or []),
        key=lambda item: len(normalize_path_for_prefix_match(str((item or {}).get("fs_prefix") or (item or {}).get("filesystem_prefix") or ""))),
        reverse=True,
    )
    for mapping in mappings:
        fs_prefix = str(mapping.get("fs_prefix") or mapping.get("filesystem_prefix") or "")
        jellyfin_prefix = str(mapping.get("jellyfin_prefix") or "").replace("\\", "/").rstrip("/")
        suffix = split_mapped_suffix(source_text, fs_prefix)
        if fs_prefix and jellyfin_prefix and suffix is not None:
            candidates.append(jellyfin_prefix + suffix)
    result: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        key = normalize_path_for_match(item)
        if key not in seen:
            seen.add(key)
            result.append(item)
    return result


def jellyfin_search_terms(source: Path) -> list[str]:
    parts = [source.stem, source.name, source.parent.name]
    grandparent = source.parent.parent.name if source.parent.parent != source.parent else ""
    if grandparent:
        parts.append(grandparent)
    match = re.search(r"(s\d{1,2}e\d{1,2}|\d{1,2}x\d{1,2})", source.stem, flags=re.IGNORECASE)
    if match:
        parts.append(match.group(1))
    cleaned: list[str] = []
    seen: set[str] = set()
    for item in parts:
        text = str(item or "").strip()
        if len(text) < 2:
            continue
        key = text.casefold()
        if key not in seen:
            seen.add(key)
            cleaned.append(text)
    return cleaned


def jellyfin_query_items(server_url: str, api_key: str, search_term: str) -> list[dict[str, Any]]:
    query = parse.urlencode({"Recursive": "true", "SearchTerm": search_term, "Fields": "Path,SeriesName,ParentIndexNumber,IndexNumber"})
    req = request.Request(f"{server_url}/Items?{query}", headers={"X-Emby-Token": api_key})
    with request.urlopen(req, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload.get("Items") or []


def jellyfin_query_path_items(server_url: str, api_key: str, settings: dict[str, Any]) -> list[dict[str, Any]]:
    if settings.get("path_lookup_enabled", True) is False:
        return []
    try:
        page_limit = max(1, int(settings.get("path_lookup_limit") or 1000))
    except (TypeError, ValueError):
        page_limit = 1000
    try:
        max_pages = max(1, int(settings.get("path_lookup_max_pages") or 20))
    except (TypeError, ValueError):
        max_pages = 20
    all_items: list[dict[str, Any]] = []
    for page in range(max_pages):
        start_index = page * page_limit
        query = parse.urlencode(
            {
                "Recursive": "true",
                "IncludeItemTypes": "Episode,Movie",
                "Fields": "Path,SeriesName,ParentIndexNumber,IndexNumber",
                "StartIndex": str(start_index),
                "Limit": str(page_limit),
            }
        )
        req = request.Request(f"{server_url}/Items?{query}", headers={"X-Emby-Token": api_key})
        with request.urlopen(req, timeout=20) as response:
            payload = json.loads(response.read().decode("utf-8"))
        items = payload.get("Items") or []
        all_items.extend(items)
        total = payload.get("TotalRecordCount")
        if not items or (isinstance(total, int) and len(all_items) >= total) or len(items) < page_limit:
            break
    return all_items


def jellyfin_query_sqlite_path_items(settings: dict[str, Any], candidate_paths: list[str]) -> list[dict[str, Any]]:
    db_path = str(settings.get("sqlite_db_path") or "").strip()
    if not db_path or not Path(db_path).is_file():
        return []
    placeholders = ", ".join("?" for _ in candidate_paths)
    if not placeholders:
        return []
    try:
        with sqlite3.connect(db_path) as connection:
            rows = connection.execute(f"SELECT Id, Name, Path FROM BaseItems WHERE Path IN ({placeholders})", candidate_paths).fetchall()
    except sqlite3.Error:
        return []
    return [{"Id": str(row[0]), "Name": row[1], "Path": row[2]} for row in rows]


def jellyfin_exact_path_matches(items: list[dict[str, Any]], candidate_paths: list[str]) -> list[dict[str, Any]]:
    candidate_by_norm = {normalize_path_for_match(path): path for path in candidate_paths}
    exact_matches: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in items:
        item_path = str(item.get("Path") or "")
        candidate_path = candidate_by_norm.get(normalize_path_for_match(item_path))
        if not candidate_path:
            continue
        item_id = str(item.get("Id") or "")
        key = (item_id, normalize_path_for_match(item_path or candidate_path))
        if not item_id or key in seen:
            continue
        seen.add(key)
        exact_matches.append({"item_id": item_id, "path": item_path or candidate_path, "matched_path": item_path or candidate_path, "name": item.get("Name")})
    return exact_matches


def jellyfin_item_score(item: dict[str, Any], source: Path, candidate_paths: list[str]) -> int:
    score = 0
    item_path = normalize_path_for_match(item.get("Path") or "")
    source_name = source.name.casefold()
    source_stem = source.stem.casefold()
    parent_name = source.parent.name.casefold()
    for candidate in candidate_paths:
        candidate_norm = normalize_path_for_match(candidate)
        if item_path == candidate_norm:
            score += 500
        elif item_path.endswith("/" + source_name):
            score += 90
        elif candidate_norm and item_path.endswith(candidate_norm):
            score += 160
    item_name = Path(str(item.get("Path") or "")).name.casefold() if item.get("Path") else ""
    if item_name == source_name:
        score += 80
    if Path(item_name).stem == source_stem:
        score += 60
    if parent_name and parent_name in item_path:
        score += 30
    if source_stem and source_stem in str(item.get("Name") or "").casefold():
        score += 25
    return score


def jellyfin_lookup_item(server_url: str, api_key: str, source: Path, candidate_paths: list[str], settings: dict[str, Any] | None = None) -> dict[str, Any]:
    search_terms = jellyfin_search_terms(source)
    items_by_id: dict[str, dict[str, Any]] = {}
    for search_term in search_terms:
        for item in jellyfin_query_items(server_url, api_key, search_term):
            item_id = str(item.get("Id") or "")
            if item_id:
                items_by_id[item_id] = item
    exact_matches = jellyfin_exact_path_matches(list(items_by_id.values()), candidate_paths)
    path_lookup_used = False
    if not exact_matches and settings is not None:
        sqlite_matches = jellyfin_exact_path_matches(jellyfin_query_sqlite_path_items(settings, candidate_paths), candidate_paths)
        path_lookup_used = path_lookup_used or bool(sqlite_matches)
        exact_matches = sqlite_matches
    if not exact_matches and settings is not None:
        path_items = jellyfin_query_path_items(server_url, api_key, settings)
        path_lookup_used = True
        exact_matches = jellyfin_exact_path_matches(path_items, candidate_paths)
    if exact_matches:
        result = {
            "status": "ok",
            "matched_count": len(exact_matches),
            "matches": exact_matches,
            "match_type": "exact_path",
            "search_terms": search_terms,
            "candidate_paths": candidate_paths,
            "path_lookup_used": path_lookup_used,
        }
        if len(exact_matches) == 1:
            result.update({"item_id": exact_matches[0]["item_id"], "matched_path": exact_matches[0]["matched_path"], "path": exact_matches[0]["path"], "name": exact_matches[0].get("name")})
        return result
    if not items_by_id:
        return {"status": "not_found", "search_terms": search_terms, "candidate_paths": candidate_paths, "reason": "no_exact_path_match", "path_lookup_used": path_lookup_used}
    scored = sorted(
        ((jellyfin_item_score(item, source, candidate_paths), item) for item in items_by_id.values()),
        key=lambda pair: pair[0],
        reverse=True,
    )
    best_score, best_item = scored[0]
    if best_score <= 0:
        return {"status": "not_found", "search_terms": search_terms, "candidate_paths": candidate_paths, "reason": "no_exact_path_match", "path_lookup_used": path_lookup_used}
    top_matches = [
        {
            "item_id": item.get("Id"),
            "path": item.get("Path"),
            "name": item.get("Name"),
            "score": score,
        }
        for score, item in scored[:10]
    ]
    return {"status": "not_found", "reason": "no_exact_path_match", "match_count": len(scored), "top_score": best_score, "candidate_paths": candidate_paths, "search_terms": search_terms, "matches": top_matches, "path_lookup_used": path_lookup_used}


def safe_unlink(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def recover_runtime_state(config: dict[str, Any]) -> None:
    path = current_job_path(config)
    if not path.exists():
        return
    try:
        payload = read_json(path)
    except (OSError, json.JSONDecodeError):
        safe_unlink(path)
        return
    phase = str(payload.get("phase") or payload.get("status") or "")
    source_path = Path(str(payload.get("source_path") or "")) if payload.get("source_path") else None
    replacement_path = Path(str(payload.get("replacement_path") or "")) if payload.get("replacement_path") else source_path
    local_output_path = Path(str(payload.get("local_output_path") or "")) if payload.get("local_output_path") else None
    temp_output_path = Path(str(payload.get("temp_output_path") or "")) if payload.get("temp_output_path") else None
    quarantine_path = Path(str(payload.get("quarantine_path") or "")) if payload.get("quarantine_path") else None
    ffmpeg_pid = int(payload.get("ffmpeg_pid") or -1)
    source_metadata = payload.get("source_metadata") or None
    stream_policy = payload.get("track_policy") or {"applied": False}
    recovered_status = None
    recovery_details: dict[str, Any] = {}
    if phase == "encoding" and not is_local_pid_running(ffmpeg_pid):
        if local_output_path and local_output_path.exists():
            safe_unlink(local_output_path)
        recovered_status = "interrupted"
    elif phase == "uploading":
        if temp_output_path and temp_output_path.exists():
            safe_unlink(temp_output_path)
        recovered_status = "upload_cleanup"
    elif phase in {"swapping", "final_verify"} and source_path and replacement_path:
        if not replacement_path.exists() and quarantine_path and quarantine_path.exists():
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(quarantine_path), str(source_path))
            recovered_status = "rollback_restored"
        elif replacement_path.exists():
            recovered_status = "replacement_present"
            if source_metadata:
                try:
                    _probe, final_meta = ffprobe_metadata(config, replacement_path, str(source_metadata.get("media_type") or "default"))
                    final_verification, final_errors = verify_output(config, source_metadata, replacement_path, final_meta, stream_policy)
                    if final_errors:
                        recovery_details["verification_errors"] = final_errors
                    else:
                        recovered_status = "replacement_verified"
                        recovery_details["verification"] = final_verification
                except Exception as exc:
                    recovery_details["verification_error"] = str(exc)
    elif phase == "refreshing_jellyfin":
        recovered_status = "replacement_present"
        if replacement_path and replacement_path.exists() and source_metadata:
            try:
                _probe, final_meta = ffprobe_metadata(config, replacement_path, str(source_metadata.get("media_type") or "default"))
                final_verification, final_errors = verify_output(config, source_metadata, replacement_path, final_meta, stream_policy)
                if not final_errors:
                    recovered_status = "refresh_retried"
                    recovery_details["verification"] = final_verification
                    recovery_details["jellyfin_refresh"] = refresh_jellyfin(config, source_path or replacement_path, replacement_path)
                else:
                    recovery_details["verification_errors"] = final_errors
            except Exception as exc:
                recovery_details["verification_error"] = str(exc)
    if recovered_status:
        event = {"job_id": payload.get("job_id"), "source_path": str(source_path) if source_path else None, "status": recovered_status, "recovered_at": utc_now(), "previous_phase": phase, **recovery_details}
        append_jsonl(history_dir(config) / "jobs.jsonl", event)
        log_event(config, "recovery", **event)
    shutil.rmtree(work_dir(config) / "current", ignore_errors=True)
    safe_unlink(path)


@dataclass
class RuntimeState:
    running: bool = False
    stop_after_current: bool = False
    force_stop: bool = False
    test_mode: bool = False
    next_scan_at: str | None = None
    current_duration_seconds: float | None = None
    current_file: str | None = None
    current_phase: str = "idle"
    ffmpeg_progress: str | None = None
    output_size_bytes: int = 0
    last_processed: list[dict[str, Any]] | None = None
    errors: list[str] | None = None

    def snapshot(self) -> dict[str, Any]:
        last_processed = self.last_processed or []
        return {"running": self.running, "stop_after_current": self.stop_after_current, "force_stop": self.force_stop, "test_mode": self.test_mode, "next_scan_at": self.next_scan_at, "current_file": self.current_file, "current_phase": self.current_phase, "ffmpeg_progress": self.ffmpeg_progress, "output_size_bytes": self.output_size_bytes, "current_summary": summarize_current_state(self), "last_processed": last_processed, "last_result": summarize_result(last_processed[0] if last_processed else None), "errors": self.errors or []}


class SimpleRipperApp:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        recover_runtime_state(config)
        self.roots = [Path(path) for path in ((config.get("libraries") or {}).get("roots") or [])]
        self.folder_suggestions, self.folder_suggestion_mode = configured_folder_suggestions(config)
        selected_entries, custom_entries = persisted_selected_folder_entries(config)
        self.selected_folder_entries: list[dict[str, str]] = selected_entries
        self.custom_folder_entries: list[dict[str, str]] = custom_entries
        self.selected_folders: list[Path] = [Path(item["path"]) for item in self.selected_folder_entries]
        self.custom_folders: list[Path] = [Path(item["path"]) for item in self.custom_folder_entries]
        self.state = RuntimeState(last_processed=[], errors=[], test_mode=is_test_mode(config))
        self._lock = threading.RLock()
        self._thread: threading.Thread | None = None
        self._ffmpeg: subprocess.Popen[Any] | None = None
        log_event(self.config, "app_initialized", selected_folders=self.selected_folder_entries, custom_folders=self.custom_folder_entries)

    def can_update(self) -> bool:
        with self._lock:
            return not self.state.running and self.state.current_phase == "idle"

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                **self.state.snapshot(),
                "can_update": (not self.state.running and self.state.current_phase == "idle"),
                "allowed_roots": [str(path) for path in self.roots],
                "folder_suggestions": list(self.folder_suggestions),
                "folder_suggestion_mode": self.folder_suggestion_mode,
                "selected_folders": list(self.selected_folder_entries),
                "selected_folder_paths": [str(path) for path in self.selected_folders],
                "custom_folders": list(self.custom_folder_entries),
                "test_mode_message": "Bezi v test modu. Original nikdy nebude po uspechu smazan, zustane v karantene." if self.state.test_mode else None,
                "scan_cache": worker_cache_summary(self.config),
                "recent_log_lines": tail_text_lines(app_log_path(self.config), 60),
            }

    def persist_selected_folders(self, previous_entries: list[dict[str, str]] | None = None) -> None:
        old_entries = list(previous_entries or [])
        old_scope = scan_scope_fingerprint(self.config, [Path(item["path"]) for item in old_entries]) if old_entries else scan_state_get(self.config, "candidate_queue_scope_fingerprint")
        write_json(selected_folders_path(self.config), {"selected_folders": self.selected_folder_entries, "custom_folders": self.custom_folder_entries, "updated_at": utc_now()})
        persist_selected_folders_in_config(self.config, self.selected_folder_entries)
        new_scope = scan_scope_fingerprint(self.config, [Path(item["path"]) for item in self.selected_folder_entries]) if self.selected_folder_entries else scan_scope_fingerprint(self.config, [])
        if old_scope != new_scope:
            invalidate_candidate_queue_scope(self.config, "selected_folders_changed", old_scope_fingerprint=old_scope, new_scope_fingerprint=new_scope, old_scope_entries=old_entries, new_scope_entries=list(self.selected_folder_entries))
        log_event(self.config, "selected_folders_updated", selected_folders=self.selected_folder_entries, custom_folders=self.custom_folder_entries)

    def _sync_folder_views(self) -> None:
        self.selected_folders = [Path(item["path"]) for item in self.selected_folder_entries]
        self.custom_folders = [Path(item["path"]) for item in self.custom_folder_entries]

    def media_type_for_source(self, source: Path) -> str:
        try:
            resolved = source.resolve()
        except OSError:
            resolved = source
        selected_sorted = sorted(self.selected_folder_entries, key=lambda item: len(item["path"]), reverse=True)
        for entry in selected_sorted:
            folder = Path(entry["path"])
            try:
                resolved.relative_to(folder.resolve())
            except (ValueError, OSError):
                continue
            media_type = media_type_value(entry.get("media_type"))
            if media_type in {"movie", "series", "anime", "default"}:
                return media_type
            break
        return guess_media_type(source)

    def set_phase(self, phase: str, file: Path | None = None, extra: dict[str, Any] | None = None) -> None:
        with self._lock:
            previous_file = self.state.current_file
            self.state.current_phase = phase
            self.state.current_file = str(file) if file else self.state.current_file
            source_metadata = (extra or {}).get("source_metadata") if isinstance(extra, dict) else None
            if isinstance(source_metadata, dict) and source_metadata.get("duration_seconds") is not None:
                self.state.current_duration_seconds = to_float(source_metadata.get("duration_seconds"))
            elif file and previous_file != self.state.current_file:
                self.state.current_duration_seconds = None
            existing = read_json(current_job_path(self.config)) if current_job_path(self.config).exists() else {}
            payload = {
                "status": phase,
                "phase": phase,
                "source_path": self.state.current_file,
                "updated_at": utc_now(),
                "started_at": existing.get("started_at") or utc_now(),
                "progress": self.state.ffmpeg_progress,
                "output_size_bytes": self.state.output_size_bytes,
            }
            payload.update(extra or {})
            write_json(current_job_path(self.config), payload)

    def log_error(self, message: str, source_path: str | None = None, failure_type: str | None = None) -> None:
        with self._lock:
            key_source = source_path or ""
            key_type = failure_type or ""
            existing = [item for item in (self.state.errors or []) if not (isinstance(item, dict) and str(item.get("source_path") or "") == key_source and str(item.get("failure_type") or "") == key_type)]
            self.state.errors = ([{"at": utc_now(), "message": message, "source_path": source_path, "failure_type": failure_type}] + existing)[:100]
        log_event(self.config, "error", message=message, source_path=source_path, failure_type=failure_type)

    def reset_runtime_state(self, clear_errors: bool = False) -> None:
        with self._lock:
            self.state.stop_after_current = False
            self.state.force_stop = False
            self.state.next_scan_at = None
            self.state.current_duration_seconds = None
            self.state.current_file = None
            self.state.current_phase = "idle"
            self.state.ffmpeg_progress = None
            self.state.output_size_bytes = 0
            if clear_errors:
                self.state.errors = []
        safe_unlink(ffmpeg_current_log_path(self.config))
        current_job_path(self.config).unlink(missing_ok=True)

    def set_selected_folders(self, folders: list[Any]) -> None:
        normalized = normalize_selected_folder_entries(folders)
        with self._lock:
            previous_entries = list(self.selected_folder_entries)
            existing_custom_paths = {str(item["path"]) for item in self.custom_folder_entries}
            self.selected_folder_entries = normalized
            self.custom_folder_entries = [item for item in normalized if item["path"] in existing_custom_paths]
            self._sync_folder_views()
            self.persist_selected_folders(previous_entries)

    def add_custom_folder(self, folder: str, media_type: str = "auto") -> None:
        path = Path(folder)
        with self._lock:
            previous_entries = list(self.selected_folder_entries)
            entry = {"path": str(path), "media_type": media_type_value(media_type)}
            self.custom_folder_entries = [item for item in self.custom_folder_entries if item["path"] != entry["path"]] + [entry]
            self.selected_folder_entries = [item for item in self.selected_folder_entries if item["path"] != entry["path"]] + [entry]
            self._sync_folder_views()
            self.persist_selected_folders(previous_entries)

    def remove_custom_folder(self, folder: str) -> None:
        path = Path(folder)
        with self._lock:
            previous_entries = list(self.selected_folder_entries)
            self.custom_folder_entries = [item for item in self.custom_folder_entries if Path(item["path"]) != path]
            self.selected_folder_entries = [item for item in self.selected_folder_entries if Path(item["path"]) != path]
            self._sync_folder_views()
            self.persist_selected_folders(previous_entries)

    def start(self) -> None:
        with self._lock:
            if self.state.running:
                raise RuntimeError("SimpleRipper is already running on this machine")
            self.state.running = True
            self.state.stop_after_current = False
            self.state.force_stop = False
            self._thread = threading.Thread(target=self._run_loop, name="simpleripper-worker", daemon=True)
            self._thread.start()
        log_event(self.config, "loop_start_requested")

    def stop_after_current(self) -> None:
        with self._lock:
            self.state.stop_after_current = True
        log_event(self.config, "stop_after_current_requested")

    def force_stop(self) -> None:
        with self._lock:
            self.state.force_stop = True
            process = self._ffmpeg
        log_event(self.config, "force_stop_requested", ffmpeg_pid=(process.pid if process else None))
        if process and process.poll() is None:
            terminate_process_gracefully(process)

    def set_test_mode(self, enabled: bool) -> None:
        with self._lock:
            self.state.test_mode = bool(enabled)
            self.config["__test_mode"] = self.state.test_mode
        log_event(self.config, "test_mode_changed", enabled=self.state.test_mode)

    def clear_stale_locks(self) -> list[str]:
        with self._lock:
            folders = list(self.selected_folders)
        return clear_stale_locks(folders, self.config)

    def begin_update(self) -> None:
        with self._lock:
            if self.state.running or self.state.current_phase != "idle":
                raise RuntimeError("Update lze provest pouze kdyz je system idle")
            self.state.current_phase = "updating"
            self.state.current_file = None
            self.state.ffmpeg_progress = None
            self.state.output_size_bytes = 0

    def perform_update(self, server: ThreadingHTTPServer) -> None:
        repo_root = Path(__file__).resolve().parent
        config_path = Path(str(self.config.get("__config_path") or "")).resolve() if self.config.get("__config_path") else None
        if not config_path:
            raise RuntimeError("Missing config path for app reload")
        log_event(self.config, "update_started", repo_root=str(repo_root), config_path=str(config_path))
        completed = subprocess.run(
            ["git", "pull"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        output = (completed.stdout or completed.stderr or "").strip()
        if completed.returncode != 0:
            raise RuntimeError(output or f"git pull failed with exit code {completed.returncode}")
        if output:
            log_event(self.config, "update_pulled", output=output)
        command = [sys.executable, str(Path(__file__).resolve()), "web", "--config", str(config_path)]
        log_event(self.config, "update_restart_scheduled", command=command)
        server.shutdown()
        server.server_close()
        subprocess.Popen(command, cwd=str(repo_root))

    def start_update(self, server: ThreadingHTTPServer) -> None:
        def run_update() -> None:
            try:
                self.perform_update(server)
            except Exception as exc:
                with self._lock:
                    self.state.current_phase = "idle"
                self.log_error(f"Update failed: {exc}")

        threading.Thread(target=run_update, name="simpleripper-update", daemon=False).start()

    def schedule_rescan_wait(self, delay_seconds: int, reason: str) -> bool:
        deadline = time.time() + max(1, int(delay_seconds))
        next_scan_at = datetime.fromtimestamp(deadline, timezone.utc).isoformat(timespec="seconds")
        with self._lock:
            self.state.current_file = None
            self.state.current_phase = "waiting_for_rescan"
            self.state.next_scan_at = next_scan_at
            self.state.ffmpeg_progress = None
            self.state.output_size_bytes = 0
        log_event(self.config, "scan_wait_scheduled", reason=reason, next_scan_at=next_scan_at, delay_seconds=int(delay_seconds))
        while time.time() < deadline:
            with self._lock:
                if self.state.stop_after_current or self.state.force_stop:
                    self.reset_runtime_state(clear_errors=False)
                    return False
            time.sleep(min(1.0, max(0.0, deadline - time.time())))
        with self._lock:
            self.state.next_scan_at = None
            if self.state.current_phase == "waiting_for_rescan":
                self.state.current_phase = "idle"
        return True

    def pick_next_candidate(self, candidates: list[Path]) -> Path | None:
        if not candidates:
            return None
        for candidate in candidates:
            self.set_phase("probing_candidate", candidate)
            log_event(self.config, "candidate_selected", source_path=str(candidate))
            log_event(self.config, "candidate_probe_start", source_path=str(candidate))
            details = inspect_candidate(self.config, candidate, self.media_type_for_source(candidate))
            update_cache_deep_check(self.config, details)
            log_event(
                self.config,
                "candidate_probe_done",
                source_path=str(candidate),
                status=details.get("status"),
                skip_reason=details.get("skip_reason"),
                candidate_reason=details.get("candidate_reason"),
                score=details.get("score"),
            )
            if details.get("status") != "ok":
                log_event(self.config, "candidate_probe_failed", source_path=str(candidate), error=details.get("error"))
                continue
            if details.get("skip_reason"):
                log_event(self.config, "candidate_scan_skipped", source_path=str(candidate), reason=details.get("skip_reason"), profile_mismatch_reasons=details.get("profile_mismatch_reasons"), retention_size_policy=details.get("retention_size_policy"))
                continue
            log_event(self.config, "candidate_ready", source_path=str(details["path"]), score=details.get("score"), codec=(details.get("metadata") or {}).get("video_codec"), candidate_reason=details.get("candidate_reason"), profile_mismatch_reasons=details.get("profile_mismatch_reasons"), retention_size_policy=details.get("retention_size_policy"))
            return Path(details["path"])
        return None

    def _run_loop(self) -> None:
        try:
            while True:
                with self._lock:
                    folders = list(self.selected_folders)
                    stop = self.state.stop_after_current or self.state.force_stop
                if stop:
                    self.reset_runtime_state(clear_errors=False)
                    break
                log_event(self.config, "scan_start", folders=[str(path) for path in folders])
                self.set_phase("scanning_inventory")
                candidates = scan_candidates(folders, self.config)
                self.set_phase("loading_queue")
                log_event(self.config, "scan_end", candidates=len(candidates))
                if not candidates:
                    if not self.schedule_rescan_wait(3600, "no_candidates"):
                        break
                    continue
                self.set_phase("selecting_candidate")
                candidate = self.pick_next_candidate(candidates)
                if candidate is None:
                    if scan_cache_enabled(self.config) and cached_candidate_paths(self.config):
                        continue
                    if not self.schedule_rescan_wait(3600, "no_usable_candidates"):
                        break
                    continue
                self.process_one(candidate)
                with self._lock:
                    if self.state.force_stop:
                        self.reset_runtime_state(clear_errors=True)
                        break
        except Exception as exc:
            self.log_error(str(exc))
        finally:
            with self._lock:
                self.state.running = False
                self.state.current_file = None
                self.state.stop_after_current = False
                self.state.force_stop = False
                if self.state.current_phase != "stopped":
                    self.state.current_phase = "idle"
                self._ffmpeg = None

    def process_one(self, source: Path) -> None:
        lock_path = write_source_lock(source, self.config)
        if lock_path is None:
            log_event(self.config, "candidate_locked", source_path=str(source))
            return
        job_id = f"{int(time.time())}-{file_lock_id(source)[:12]}"
        work_root = work_dir(self.config)
        work_dir_path = work_root / "current"
        if work_dir_path.exists():
            shutil.rmtree(work_dir_path, ignore_errors=True)
        work_dir_path.mkdir(parents=True, exist_ok=True)
        replacement_path = target_output_path(source)
        output = work_dir_path / "output" / replacement_path.name
        output.parent.mkdir(parents=True, exist_ok=True)
        copied_source = work_dir_path / "input" / source.name
        copied_source.parent.mkdir(parents=True, exist_ok=True)
        metadata_dir = work_dir_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        tmp_output = temp_upload_path(replacement_path)
        job_summary: dict[str, Any] = {"job_id": job_id, "source_path": str(source), "replacement_path": str(replacement_path), "started_at": utc_now(), "status": "running"}
        succeeded = False
        ffmpeg_started = False
        ffmpeg_completed = False
        try:
            log_event(self.config, "candidate_selected", job_id=job_id, source_path=str(source))
            ensure_local_free_space(self.config, source.stat().st_size)
            self.set_phase("copying_source", source, {"job_id": job_id, "replacement_path": str(replacement_path), "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output)})
            log_event(self.config, "copy_source_start", job_id=job_id, source_path=str(source), local_input_path=str(copied_source))
            copy_file_interruptible(source, copied_source, lambda: bool(self.state.force_stop))
            log_event(self.config, "copy_source_done", job_id=job_id, bytes=copied_source.stat().st_size)
            self.set_phase("probing_source", source, {"job_id": job_id, "replacement_path": str(replacement_path), "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output)})
            source_probe, source_meta = ffprobe_metadata(self.config, copied_source, self.media_type_for_source(source))
            source_meta["file_size_bytes"] = source.stat().st_size
            source_before_signature = source_signature(source)
            write_json(metadata_dir / "source.ffprobe.json", {"probe": source_probe, "metadata": source_meta})
            recovery_context = {"source_metadata": source_meta, "replacement_path": str(replacement_path)}
            stream_policy = select_streams(self.config, source_meta)
            profile_matches, profile_mismatch_reasons = source_matches_target_profile(self.config, source_meta, str(source_meta.get("media_type") or "default"), stream_policy)
            retention_size_policy = retention_size_policy_evaluation(self.config, source_meta, str(source_meta.get("media_type") or "default"))
            recovery_context["track_policy"] = stream_policy
            recovery_context["target_profile_matches"] = profile_matches
            recovery_context["profile_mismatch_reasons"] = profile_mismatch_reasons
            recovery_context["retention_size_policy"] = retention_size_policy
            reason = skip_reason(self.config, source_meta, stream_policy)
            if reason:
                job_summary.update({"status": "skipped", "skip_reason": reason, "finished_at": utc_now(), "source": source_meta, "target_profile_matches": profile_matches, "profile_mismatch_reasons": profile_mismatch_reasons, "track_policy": stream_policy, "retention_size_policy": retention_size_policy})
                marker = marker_path(source, self.config)
                if marker is not None:
                    write_json(marker, job_summary)
                append_jsonl(history_dir(self.config) / "jobs.jsonl", job_summary)
                history_payload = {"status": "skipped", "source_signature": source_signature(source), "job_id": job_id, "updated_at": utc_now(), "reason": reason, "retention_size_policy": retention_size_policy}
                write_history_index(self.config, source, history_payload)
                write_shared_worker_history(self.config, source, history_payload)
                update_cache_deep_check(
                    self.config,
                    {"path": source, "status": "ok", "metadata": source_meta, "skip_reason": reason, "score": candidate_priority_score(source_meta), "retention_size_policy": retention_size_policy},
                )
                log_event(self.config, "candidate_skipped", job_id=job_id, source_path=str(source), reason=reason, profile_mismatch_reasons=profile_mismatch_reasons, retention_size_policy=retention_size_policy)
                with self._lock:
                    self.state.last_processed = ([{"source_path": str(source), "finished_at": utc_now(), "status": "skipped", "reason": reason}] + (self.state.last_processed or []))[:20]
                succeeded = True
                return
            if profile_mismatch_reasons:
                log_event(self.config, "candidate_profile_mismatch", job_id=job_id, source_path=str(source), reasons=profile_mismatch_reasons, retention_size_policy=retention_size_policy)
            self.set_phase("encoding", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), **recovery_context})
            command = build_ffmpeg_command(self.config, copied_source, output, source_meta, stream_policy)
            job_summary["ffmpeg_command"] = command
            (work_dir_path / "logs").mkdir(parents=True, exist_ok=True)
            ffmpeg_log = work_dir_path / "logs" / "ffmpeg.log"
            log_event(self.config, "ffmpeg_start", job_id=job_id, command=command)
            with ffmpeg_log.open("w", encoding="utf-8", errors="replace") as log_handle:
                ffmpeg_started = True
                self._ffmpeg = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log_handle, text=True, encoding="utf-8", errors="replace")
                self.set_phase("encoding", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), "ffmpeg_pid": self._ffmpeg.pid, **recovery_context})
                progress_state: dict[str, Any] = {}

                def update_progress(snapshot: dict[str, Any]) -> None:
                    with self._lock:
                        progress_state.clear()
                        progress_state.update(snapshot)

                progress_thread = threading.Thread(target=consume_ffmpeg_progress, args=(self._ffmpeg.stdout, update_progress), daemon=True)
                progress_thread.start()
                while self._ffmpeg.poll() is None:
                    with self._lock:
                        self.state.output_size_bytes = output.stat().st_size if output.exists() else 0
                        progress_snapshot = dict(progress_state)
                        progress_snapshot["size_bytes"] = self.state.output_size_bytes
                        self.state.ffmpeg_progress = progress_snapshot
                        force = self.state.force_stop
                    write_json(ffmpeg_current_log_path(self.config), {"job_id": job_id, "source_path": str(source), "updated_at": utc_now(), "progress": progress_snapshot})
                    self.set_phase("encoding", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), "ffmpeg_pid": self._ffmpeg.pid, **recovery_context})
                    if force:
                        terminate_process_gracefully(self._ffmpeg)
                        raise ForceStopRequested("force stop requested")
                    time.sleep(0.5)
                progress_thread.join(timeout=1)
                if self._ffmpeg.returncode != 0:
                    raise FfmpegFailedError(f"ffmpeg failed with exit code {self._ffmpeg.returncode}")
            ffmpeg_completed = True
            log_event(self.config, "ffmpeg_done", job_id=job_id, returncode=self._ffmpeg.returncode)
            self.set_phase("probing_output", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), **recovery_context})
            output_probe, output_meta = ffprobe_metadata(self.config, output, source_meta.get("media_type") or "default")
            write_json(metadata_dir / "output.ffprobe.json", {"probe": output_probe, "metadata": output_meta})
            self.set_phase("verifying", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), **recovery_context})
            verification, errors = verify_output(self.config, source_meta, output, output_meta, stream_policy)
            if errors:
                raise RuntimeError("; ".join(errors))
            log_event(self.config, "local_verification_ok", job_id=job_id, output_size_bytes=output.stat().st_size)
            self.set_phase("uploading", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), **recovery_context})
            log_event(self.config, "upload_start", job_id=job_id, temp_output_path=str(tmp_output))
            copy_file_interruptible(output, tmp_output, lambda: bool(self.state.force_stop))
            temp_probe, temp_meta = ffprobe_metadata(self.config, tmp_output, source_meta.get("media_type") or "default")
            write_json(metadata_dir / "nas-temp.ffprobe.json", {"probe": temp_probe, "metadata": temp_meta})
            temp_verification, temp_errors = verify_output(self.config, source_meta, tmp_output, temp_meta, stream_policy)
            if temp_errors:
                raise RuntimeError("NAS temp verification failed: " + "; ".join(temp_errors))
            log_event(self.config, "upload_done", job_id=job_id, temp_output_path=str(tmp_output), output_size_bytes=tmp_output.stat().st_size)
            self.set_phase("swapping", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), **recovery_context})
            final_payload = replace_source_with_output(source, tmp_output, self.config, {"source": source_meta, "output": temp_meta, "verification": temp_verification, "track_policy": stream_policy, "job_id": job_id}, replacement_path)
            final_path = Path(str(final_payload["replacement_path"]))
            self.set_phase("final_verify", source, {"job_id": job_id, "replacement_path": str(final_path), "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), "quarantine_path": final_payload["quarantine_path"], **recovery_context})
            log_event(self.config, "swap_done", job_id=job_id, quarantine_path=final_payload["quarantine_path"], source_path=str(source), replacement_path=str(final_path))
            final_probe, final_meta = ffprobe_metadata(self.config, final_path, source_meta.get("media_type") or "default")
            final_verification, final_errors = verify_output(self.config, source_meta, final_path, final_meta, stream_policy)
            if final_errors:
                rollback_replacement(source, Path(str(final_payload["quarantine_path"])), self.config, final_path)
                raise RuntimeError("Final verification failed after swap: " + "; ".join(final_errors))
            quarantine_cleanup = finalize_quarantined_original(Path(str(final_payload["quarantine_path"])), self.config)
            final_payload.update(quarantine_cleanup)
            write_json(metadata_dir / "final.ffprobe.json", {"probe": final_probe, "metadata": final_meta})
            marker = marker_path(source, self.config)
            if marker is not None:
                write_json(marker, {"source": source_meta, "output": final_meta, "verification": final_verification, "track_policy": stream_policy, "job_id": job_id, **final_payload, "processed_at": utc_now()})
            history_payload = {"status": "done", "job_id": job_id, "source_signature": source_before_signature, "updated_at": utc_now(), "video_codec_after": final_meta.get("video_codec"), "replacement_path": str(final_path), "final_path": str(final_path)}
            write_history_index(self.config, source, history_payload)
            write_shared_worker_history(self.config, source, history_payload)
            replacement_history_payload = {"status": "done", "job_id": job_id, "source_signature": source_signature(final_path), "updated_at": utc_now(), "video_codec_after": final_meta.get("video_codec"), "source_path": str(source), "replacement_path": str(final_path), "final_path": str(final_path)}
            write_history_index(self.config, final_path, replacement_history_payload)
            write_shared_worker_history(self.config, final_path, replacement_history_payload)
            update_cache_job_success(self.config, source, final_path)
            log_event(self.config, "final_verification_ok", job_id=job_id, source_path=str(source), replacement_path=str(final_path), output_size_bytes=final_path.stat().st_size)
            self.set_phase("refreshing_jellyfin", source, {"job_id": job_id, "replacement_path": str(final_path), "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), "quarantine_path": final_payload["quarantine_path"], **recovery_context})
            jellyfin_refresh = refresh_jellyfin(self.config, source, final_path)
            log_event(self.config, "jellyfin_refresh", job_id=job_id, source_path=str(source), status=jellyfin_refresh.get("status"), item_id=jellyfin_refresh.get("item_id"))
            summary_fields = history_summary_fields(source_meta, final_meta, final_verification, jellyfin_refresh)
            job_summary.update({"status": "done", "finished_at": utc_now(), "source": source_meta, "output": final_meta, "verification": final_verification, "local_verification": verification, "nas_temp_verification": temp_verification, "track_policy": stream_policy, **summary_fields, **final_payload})
            append_jsonl(history_dir(self.config) / "jobs.jsonl", job_summary)
            write_json(history_dir(self.config) / f"{job_id}.json", job_summary)
            log_event(self.config, "job_done", job_id=job_id, source_path=str(source), jellyfin_status=jellyfin_refresh.get("status"))
            with self._lock:
                self.state.last_processed = ([{"source_path": str(source), "finished_at": utc_now(), "status": "done", **summary_fields}] + (self.state.last_processed or []))[:20]
            succeeded = True
        except ForceStopRequested:
            log_event(self.config, "force_stop_completed", job_id=job_id, source_path=str(source), phase=self.state.current_phase)
            self.reset_runtime_state(clear_errors=True)
        except Exception as exc:
            failure_type = "ffmpeg" if isinstance(exc, FfmpegFailedError) or ffmpeg_completed else "job"
            previous_failure_count = 0
            previous_payload = load_history_index(self.config, source)
            current_signature = source_signature(source) if source.exists() else None
            previous_signature = (previous_payload or {}).get("source_signature") or {}
            if previous_payload and previous_payload.get("failure_type") == failure_type and current_signature and previous_signature.get("size_bytes") == current_signature.get("size_bytes") and previous_signature.get("mtime_ns") == current_signature.get("mtime_ns"):
                previous_failure_count = int(previous_payload.get("failure_count") or 0)
            job_summary.update({"status": "error", "finished_at": utc_now(), "error": str(exc)})
            append_jsonl(history_dir(self.config) / "jobs.jsonl", job_summary)
            history_payload = {"status": "error", "job_id": job_id, "source_signature": current_signature, "updated_at": utc_now(), "error": str(exc), "failure_type": failure_type, "failure_count": previous_failure_count + 1, "replacement_path": str(replacement_path)}
            write_history_index(self.config, source, history_payload)
            write_shared_worker_history(self.config, source, history_payload)
            cache_failure = update_cache_job_failure(self.config, source, str(exc)) if ffmpeg_started else None
            if cache_failure:
                log_event(self.config, "candidate_cache_failure", job_id=job_id, source_path=str(source), **cache_failure)
            self.log_error(f"{source}: {exc}", source_path=str(source), failure_type=failure_type)
            preserve_failed_output(work_dir_path, source, self.config)
        finally:
            lock_path.unlink(missing_ok=True)
            tmp_output.unlink(missing_ok=True)
            safe_unlink(ffmpeg_current_log_path(self.config))
            if succeeded or self.state.current_phase == "idle" or not bool((self.config.get("paths") or {}).get("keep_failed_output_for_inspection", True)):
                shutil.rmtree(work_dir_path, ignore_errors=True)
            current_job_path(self.config).unlink(missing_ok=True)


def guess_media_type(path: Path) -> str:
    text = str(path).lower()
    if "anime" in text:
        return "anime"
    if "series" in text or "serial" in text:
        return "series"
    if "movie" in text or "film" in text:
        return "movie"
    return "default"

def replace_source_with_output(source: Path, output: Path, config: dict[str, Any], marker: dict[str, Any], replacement_path: Path | None = None) -> dict[str, Any]:
    final_path = replacement_path or target_output_path(source)
    quarantine_path = quarantine_path_for_source(source, config)
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(quarantine_path))
    try:
        final_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(output, final_path)
    except Exception:
        shutil.move(str(quarantine_path), str(source))
        raise
    marker = {**marker, "source_path": str(source), "replacement_path": str(final_path), "final_path": str(final_path), "quarantine_path": str(quarantine_path), "processed_at": utc_now()}
    write_json(history_dir(config) / "quarantine_index" / f"{file_lock_id(source)}.json", marker)
    sidecar_marker = marker_path(source, config)
    return {"quarantine_path": str(quarantine_path), "replacement_path": str(final_path), "final_path": str(final_path), "processed_marker_path": str(sidecar_marker) if sidecar_marker is not None else None}


def rollback_replacement(source: Path, quarantine_path: Path, config: dict[str, Any], replacement_path: Path | None = None) -> None:
    failed_target = replacement_path or target_output_path(source)
    inspection_root = Path(str((config.get("paths") or {}).get("inspection_dir") or "inspection")) / "failed_replacements"
    inspection_root.mkdir(parents=True, exist_ok=True)
    failed_output = inspection_root / f"{failed_target.name}.{int(time.time())}.failed-output"
    if failed_target.exists():
        shutil.move(str(failed_target), str(failed_output))
    if quarantine_path.exists():
        shutil.move(str(quarantine_path), str(source))
    log_event(config, "rollback_restored", source_path=str(source), replacement_path=str(failed_target), quarantine_path=str(quarantine_path), failed_output_path=str(failed_output))


def preserve_failed_output(work_dir: Path, source: Path, config: dict[str, Any]) -> None:
    if not bool((config.get("paths") or {}).get("keep_failed_output_for_inspection", True)):
        return
    inspection_root = Path(str((config.get("paths") or {}).get("inspection_dir") or "inspection"))
    inspection_root.mkdir(parents=True, exist_ok=True)
    target = inspection_root / f"{source.stem}-{int(time.time())}"
    if work_dir.exists():
        shutil.move(str(work_dir), str(target))


def is_path_inside_roots(path: Path, roots: list[Path]) -> bool:
    resolved_path = path.resolve()
    for root in roots:
        try:
            resolved_path.relative_to(root.resolve())
        except ValueError:
            continue
        return True
    return False


def safe_browse_folders(config: dict[str, Any], requested_path: str | None = None) -> dict[str, Any]:
    configured_roots = [Path(str(path)) for path in ((config.get("libraries") or {}).get("roots") or [])]
    allowed_roots = [root.resolve() for root in configured_roots]
    if not allowed_roots:
        raise ValueError("No allowed library roots are configured")

    allowed_root_strings = [str(root) for root in allowed_roots]
    if not requested_path:
        return {
            "current_path": None,
            "parent_path": None,
            "allowed_roots": allowed_root_strings,
            "directories": [{"name": root.name or str(root), "path": str(root)} for root in allowed_roots],
        }

    current = Path(requested_path).expanduser().resolve()
    containing_root = next((root for root in allowed_roots if is_path_inside_roots(current, [root])), None)
    if containing_root is None:
        raise PermissionError("Path is outside allowed library roots")
    if not current.exists():
        raise FileNotFoundError(f"Folder does not exist: {current}")
    if not current.is_dir():
        raise NotADirectoryError(f"Path is not a folder: {current}")

    directories: list[dict[str, str]] = []
    try:
        children = list(current.iterdir())
    except OSError:
        children = []
    for child in children:
        try:
            resolved_child = child.resolve()
            if not is_path_inside_roots(resolved_child, [containing_root]):
                continue
            if resolved_child.is_dir():
                directories.append({"name": child.name, "path": str(resolved_child)})
        except OSError:
            continue
    directories.sort(key=lambda item: item["name"].casefold())

    parent = current.parent.resolve()
    parent_path = str(parent) if parent != current and is_path_inside_roots(parent, [containing_root]) else None
    return {
        "current_path": str(current),
        "parent_path": parent_path,
        "allowed_roots": allowed_root_strings,
        "directories": directories,
    }


class SimpleRipperHandler(BaseHTTPRequestHandler):
    app: SimpleRipperApp

    def log_message(self, format: str, *args: Any) -> None:
        path = self.path.split("?", 1)[0] if getattr(self, "path", None) else ""
        if path in {"/api/status", "/favicon.ico"}:
            return
        super().log_message(format, *args)

    def send_no_cache_headers(self) -> None:
        self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")

    def send_json(self, payload: Any, status: int = 200, include_body: bool = True) -> None:
        body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_no_cache_headers()
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def send_index(self, include_body: bool = True) -> None:
        body = INDEX_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_no_cache_headers()
        self.end_headers()
        if include_body:
            self.wfile.write(body)

    def read_payload(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

    def handle_get_request(self, include_body: bool = True) -> None:
        parsed = parse.urlsplit(self.path)
        path = parsed.path
        if path == "/api/status":
            self.send_json(self.app.status(), include_body=include_body)
            return
        if path == "/api/config":
            self.send_json({"allowed_roots": [str(path) for path in self.app.roots]}, include_body=include_body)
            return
        if path == "/api/logs":
            self.send_json({"lines": tail_text_lines(app_log_path(self.app.config), 200)}, include_body=include_body)
            return
        if path == "/api/browse-folders":
            query = parse.parse_qs(parsed.query)
            requested_path = (query.get("path") or [None])[0]
            try:
                self.send_json(safe_browse_folders(self.app.config, requested_path), include_body=include_body)
            except PermissionError as exc:
                self.send_json({"error": str(exc)}, HTTPStatus.FORBIDDEN, include_body=include_body)
            except FileNotFoundError as exc:
                self.send_json({"error": str(exc)}, HTTPStatus.NOT_FOUND, include_body=include_body)
            except (NotADirectoryError, ValueError, OSError) as exc:
                self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST, include_body=include_body)
            return
        self.send_index(include_body=include_body)

    def do_GET(self) -> None:
        self.handle_get_request(include_body=True)

    def do_HEAD(self) -> None:
        self.handle_get_request(include_body=False)

    def do_POST(self) -> None:
        try:
            payload = self.read_payload()
            if self.path == "/api/folders":
                self.app.set_selected_folders(payload.get("folders") or [])
                self.send_json(self.app.status())
            elif self.path == "/api/custom-folder":
                if payload.get("remove"):
                    self.app.remove_custom_folder(str(payload.get("path") or ""))
                else:
                    folder = str(payload.get("path") or "").strip()
                    if not folder:
                        raise ValueError("Missing folder path. Use /api/browse-folders from the web UI.")
                    self.app.add_custom_folder(folder, str(payload.get("media_type") or "auto"))
                self.send_json(self.app.status())
            elif self.path == "/api/start":
                self.app.start()
                self.send_json(self.app.status())
            elif self.path == "/api/stop-after-current":
                self.app.stop_after_current()
                self.send_json(self.app.status())
            elif self.path == "/api/force-stop":
                self.app.force_stop()
                self.send_json(self.app.status())
            elif self.path == "/api/test-mode":
                enabled = bool(payload.get("enabled", True))
                self.app.set_test_mode(enabled)
                self.send_json(self.app.status())
            elif self.path == "/api/clear-stale-locks":
                self.send_json({"removed": self.app.clear_stale_locks()})
            elif self.path == "/api/update":
                self.app.begin_update()
                self.send_json(self.app.status())
                self.app.start_update(self.server)
            else:
                self.send_json({"error": "not found"}, HTTPStatus.NOT_FOUND)
        except Exception as exc:
            self.send_json({"error": str(exc)}, HTTPStatus.BAD_REQUEST)


INDEX_HTML = """<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>SimpleRipper</title>
<style>
 :root{--bg:#0d1520;--bg-soft:#142131;--paper:#121c29;--paper-strong:#182536;--line:#26384d;--text:#e8f0fb;--muted:#91a4bc;--accent:#59a6ff;--accent-soft:#183454;--accent-strong:#8cc3ff;--warn:#f0b35b;--warn-soft:#3f2e16;--danger:#f08a98;--danger-soft:#41202a;--shadow:0 24px 70px rgba(0,0,0,.34);--shadow-soft:0 14px 34px rgba(0,0,0,.24)}
*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{font-family:"Trebuchet MS","Gill Sans","Segoe UI",sans-serif;background:radial-gradient(circle at top left,#1a2940 0,#0d1520 42%,#091019 100%);color:var(--text);min-height:100vh}
button,input,select,textarea{font:inherit}
button{border:0;border-radius:14px;padding:11px 16px;font-weight:700;letter-spacing:.02em;cursor:pointer;transition:transform .18s ease,box-shadow .18s ease,background .18s ease,color .18s ease}
button:hover{transform:translateY(-1px);box-shadow:var(--shadow-soft)}
button:active{transform:translateY(0)}
button:disabled{opacity:.45;cursor:not-allowed;filter:saturate(.2);transform:none;box-shadow:none}
input[type=text],select{width:100%;padding:11px 12px;border:1px solid var(--line);border-radius:12px;background:#0f1824;color:var(--text)}
pre{margin:0;padding:16px 18px;border-radius:18px;background:#09111a;color:#dce9f8;overflow:auto;font-family:Consolas,"Cascadia Mono",monospace;font-size:13px;line-height:1.5}
.shell{max-width:1320px;margin:0 auto;padding:28px 22px 56px}
.hero{position:relative;overflow:hidden;border:1px solid rgba(108,137,171,.24);background:linear-gradient(135deg,rgba(20,33,49,.96),rgba(14,23,35,.94));border-radius:32px;padding:28px 30px;box-shadow:var(--shadow)}
.hero:before,.hero:after{content:"";position:absolute;border-radius:999px;pointer-events:none}
.hero:before{width:360px;height:360px;right:-120px;top:-180px;background:radial-gradient(circle,rgba(89,166,255,.22),rgba(89,166,255,0))}
.hero:after{width:260px;height:260px;left:-90px;bottom:-130px;background:radial-gradient(circle,rgba(31,76,122,.30),rgba(31,76,122,0))}
.hero-grid{position:relative;display:grid;grid-template-columns:minmax(0,1.4fr) minmax(340px,.9fr);gap:22px;align-items:start}
.eyebrow{display:inline-flex;align-items:center;gap:8px;padding:7px 12px;border-radius:999px;background:rgba(89,166,255,.12);color:var(--accent-strong);font-size:12px;font-weight:800;letter-spacing:.08em;text-transform:uppercase}
.title{margin:16px 0 10px;font:700 44px/1.03 Georgia,"Times New Roman",serif;letter-spacing:-.03em}
.lede{max-width:64ch;margin:0;color:#9fb2c7;font-size:16px;line-height:1.65}
.hero-stats{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin-top:22px}
.stat{padding:16px 18px;border-radius:20px;background:rgba(16,27,41,.76);border:1px solid rgba(54,81,110,.72);backdrop-filter:blur(8px)}
.stat-label{font-size:12px;letter-spacing:.08em;text-transform:uppercase;color:var(--muted);font-weight:800}
.stat-value{margin-top:7px;font:700 24px/1.1 Georgia,"Times New Roman",serif}
.hero-card{position:relative;padding:22px;border-radius:24px;background:rgba(18,28,41,.84);border:1px solid rgba(54,81,110,.86);box-shadow:var(--shadow-soft)}
.hero-card h2{margin:0 0 14px;font:700 26px/1.1 Georgia,"Times New Roman",serif}
.hero-card p{margin:0;color:#9db0c4;line-height:1.55}
.badge-row{display:flex;gap:10px;flex-wrap:wrap;margin-top:16px}
.badge{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;font-size:13px;font-weight:700;border:1px solid transparent}
.badge-ok{background:var(--accent-soft);color:var(--accent-strong);border-color:rgba(46,95,82,.15)}
.badge-warn{background:var(--warn-soft);color:#74420a;border-color:rgba(155,90,20,.15)}
.badge-idle{background:#192638;color:#a6bad0;border-color:#2d435a}
.grid{display:grid;grid-template-columns:minmax(320px,.95fr) minmax(0,1.45fr);gap:22px;margin-top:24px}
.stack{display:grid;gap:22px}
.panel{background:linear-gradient(180deg,var(--paper-strong),var(--paper));border:1px solid var(--line);border-radius:28px;padding:22px;box-shadow:var(--shadow-soft)}
.panel h2{margin:0 0 4px;font:700 27px/1.08 Georgia,"Times New Roman",serif}
.panel-intro{margin:0 0 18px;color:var(--muted);font-size:14px;line-height:1.5}
.folder-list,.custom-list{display:grid;gap:12px}
.folder-row,.custom-row{display:grid;grid-template-columns:auto minmax(0,1fr) 135px auto;gap:10px;align-items:center;padding:12px 14px;border-radius:18px;background:#162232;border:1px solid #29405b}
.folder-name{font-weight:700;line-height:1.4;word-break:break-word}
.folder-chip{display:inline-flex;align-items:center;justify-content:center;min-width:28px;height:28px;border-radius:999px;background:#203247;color:#8fc1ff;font-size:12px;font-weight:800}
.folder-actions{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px;margin-top:16px}
.folder-manual{display:grid;grid-template-columns:minmax(0,1fr) 150px;gap:10px;margin-top:10px}
.ghost{background:#182435;color:var(--text);border:1px solid #30465f}
.primary{background:var(--accent);color:#f5f4ef}
.warning{background:#66421f;color:#fff7ef}
.outline{background:transparent;border:1px solid rgba(46,111,163,.24);color:var(--accent-strong)}
.controls{display:grid;gap:12px;grid-template-columns:repeat(2,minmax(0,1fr))}
.controls button:last-child{grid-column:1/-1}
.status-shell{display:grid;gap:14px}
.status-banner{display:flex;align-items:center;justify-content:space-between;gap:16px;padding:16px 18px;border-radius:20px;background:#172434;border:1px solid #29405b}
.status-title{font-size:13px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;font-weight:800}
.status-value{margin-top:5px;font:700 26px/1.05 Georgia,"Times New Roman",serif}
.status-pill{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;font-size:12px;text-transform:uppercase;letter-spacing:.08em;font-weight:800}
.status-running{background:var(--accent-soft);color:var(--accent-strong)}
.status-idle{background:#1b293a;color:#9db1c6}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.metric{padding:14px 16px;border-radius:18px;background:#152231;border:1px solid #2a415b}
.metric-label{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:800}
.metric-value{margin-top:6px;font-size:16px;font-weight:700;line-height:1.5;word-break:break-word}
.path-block{padding:16px;border-radius:18px;background:#101b28;border:1px dashed #36506d}
.path-label{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;font-weight:800;margin-bottom:7px}
.code{font-family:Consolas,"Cascadia Mono",monospace;word-break:break-word;line-height:1.6}
.result-shell{display:grid;gap:14px}
.result-head{display:flex;justify-content:space-between;gap:14px;align-items:flex-start}
.result-state{display:inline-flex;align-items:center;gap:8px;padding:8px 12px;border-radius:999px;font-size:12px;text-transform:uppercase;letter-spacing:.08em;font-weight:800}
.state-done{background:var(--accent-soft);color:var(--accent-strong)}
.state-skipped{background:var(--warn-soft);color:#74420a}
.state-error{background:var(--danger-soft);color:var(--danger)}
.result-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.delta-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}
.summary-card{padding:16px;border-radius:18px;background:#152231;border:1px solid #2a415b}
.summary-kicker{font-size:12px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-weight:800}
.summary-main{margin-top:6px;font-size:17px;font-weight:700;line-height:1.4}
.summary-note{margin-top:4px;color:#91a5ba;font-size:13px;line-height:1.45}
.callout{padding:14px 16px;border-radius:18px;border:1px solid transparent;line-height:1.55}
.callout.warn{background:var(--warn-soft);border-color:#eed2a4;color:#6c420f}
.callout.err{background:var(--danger-soft);border-color:#ecc2c2;color:#772727}
.callout.info{background:#16314b;border-color:#356089;color:#cfe6ff}
.error-list{display:grid;gap:10px}
.err{padding:12px 14px;border-radius:16px;background:var(--danger-soft);color:#772727;border:1px solid #ecc2c2;line-height:1.5}
.log-shell{display:grid;gap:14px}
.log-head{display:flex;justify-content:space-between;gap:12px;align-items:center}
.log-meta{font-size:13px;color:var(--muted)}
.disclosure summary{cursor:pointer;user-select:none;font-weight:800;color:#3f5367}
.disclosure[open] summary{margin-bottom:12px}
.muted{color:var(--muted)}
.simple-list{display:grid;gap:12px}
.simple-item{display:flex;gap:12px;align-items:center;justify-content:space-between;padding:14px 16px;border-radius:18px;background:#162232;border:1px solid #29405b}
.simple-main{min-width:0;display:grid;gap:4px}
.simple-title{font-weight:700;word-break:break-word}
.simple-sub{font-size:13px;color:var(--muted)}
.empty{padding:16px;border-radius:18px;background:#101a27;border:1px dashed #314b67;color:var(--muted)}
.fade-in{animation:rise .35s ease}
@keyframes rise{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
@media (max-width:1060px){.hero-grid,.grid{grid-template-columns:1fr}.hero-stats{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media (max-width:760px){.shell{padding:18px 14px 36px}.hero{padding:22px 18px;border-radius:26px}.title{font-size:34px}.hero-stats,.metrics,.result-grid,.delta-grid,.controls,.folder-actions{grid-template-columns:1fr}.folder-row,.custom-row,.simple-item{grid-template-columns:auto 1fr;display:grid}.folder-row select,.custom-row select,.folder-row button,.custom-row button{grid-column:2}.status-banner,.result-head,.log-head{flex-direction:column;align-items:flex-start}}
</style></head>
<body><div class="shell"><section class="hero fade-in"><div class="hero-grid"><div><span class="eyebrow">Local only · one encode at a time</span><h1 class="title">SimpleRipper</h1><p class="lede">Simple local control panel for picking folders, watching the current job, and checking the last replacement result without a wall of debug noise.</p><div class="hero-stats"><article class="stat"><div class="stat-label">Selected folders</div><div class="stat-value" id="selectedCount">0</div></article><article class="stat"><div class="stat-label">Current phase</div><div class="stat-value" id="heroPhase">Idle</div></article><article class="stat"><div class="stat-label">Warnings</div><div class="stat-value" id="warningCount">0</div></article></div></div><aside class="hero-card"><h2>Session pulse</h2><p id="heroSummary">Waiting for the first status update.</p><div class="badge-row" id="heroBadges"></div></aside></div></section><main class="grid"><section class="stack"><section class="panel fade-in"><h2>Folders</h2><p class="panel-intro">Vyberte slozky v prohlizeci nebo je zadejte rucne. Browser povoli jen rooty z configu.</p><div class="folder-actions"><button class="primary" onclick="pickFolder()">Vybrat slozku</button><button class="ghost" onclick="pickFolder(lastSelectedFolderPath())">Vybrat dalsi pobliz</button></div><div class="folder-manual"><input id="manualFolderPath" type="text" list="folderSuggestionList" placeholder="\\\\server\\share\\FILMY nebo /mnt/nas/filmy/FILMY"><button class="ghost" onclick="addManualFolder()">Pridat cestu</button></div><datalist id="folderSuggestionList"></datalist><div id="folderPickerHint" class="panel-intro" style="margin-top:12px">Otevre webovy browser slozek pod povolenymi rooty. Cestu muzete stale vlozit rucne.</div><div id="selectedFolders" class="simple-list"></div></section><section class="panel fade-in"><h2>Controls</h2><p class="panel-intro">Start, stop after the current file, force-stop the local encode, switch into safe test mode, or run update when the app is idle. Update only triggers a brief app reload.</p><div class="controls"><button class="primary" onclick="post('/api/start')">Start</button><button class="ghost" onclick="post('/api/stop-after-current')">Stop after current</button><button class="warning" onclick="post('/api/force-stop')">Force stop</button><button class="outline" id="testModeButton" onclick="toggleTestMode()">Test mode</button><button class="outline" onclick="post('/api/clear-stale-locks')">Clear stale locks</button></div><div id="testModeBanner"></div></section><section class="panel fade-in"><h2>Errors</h2><p class="panel-intro">Recent app-level failures and warnings.</p><div id="errors" class="error-list"></div></section></section><section class="stack"><section class="panel fade-in"><h2>Current Job</h2><p class="panel-intro">Current local runtime state and ffmpeg progress.</p><div id="current"></div></section><section class="panel fade-in"><h2>Last Result</h2><p class="panel-intro">Most recent completed, skipped, or failed job.</p><div id="lastResult"></div></section><section class="panel fade-in"><div class="log-head"><div><h2>Activity Log</h2><p class="panel-intro">Recent local app events.</p></div><div class="log-meta" id="logMeta">0 lines</div></div><div class="log-shell"><pre id="log"></pre><details class="disclosure"><summary>Raw status payload</summary><pre id="status"></pre></details></div></section></section></main></div>
<script>
let lastStatus=null
let uiError=''
function formatRequestError(error){return error&&error.message?error.message:String(error||'Unknown UI error')}
async function readJsonResponse(response){const text=await response.text();let payload={};try{payload=text?JSON.parse(text):{}}catch(error){throw new Error(`Invalid server response (${response.status})`)}if(!response.ok){throw new Error(payload.error||`Request failed (${response.status})`)}return payload}
function renderUiError(message){uiError=message;if(lastStatus){render(lastStatus);return}const errors=document.getElementById('errors');if(errors){errors.innerHTML=`<div class="err">UI<br>${escapeHtml(message)}</div>`}}
async function getStatus(){try{const s=await fetch('/api/status',{cache:'no-store'}).then(readJsonResponse);uiError='';render(s)}catch(error){renderUiError(formatRequestError(error))}}
async function post(url,body={}){try{const s=await fetch(url,{method:'POST',cache:'no-store',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}).then(readJsonResponse);uiError='';render(s)}catch(error){renderUiError(formatRequestError(error))}}
function selectedMap(s){return Object.fromEntries((s.selected_folders||[]).map(item=>[item.path,item.media_type||'auto']))}
function mediaTypeSelect(value, attrs=''){const current=value||'auto';return `<select ${attrs}><option value="auto" ${current==='auto'?'selected':''}>Auto</option><option value="movie" ${current==='movie'?'selected':''}>Movie</option><option value="series" ${current==='series'?'selected':''}>Series</option><option value="anime" ${current==='anime'?'selected':''}>Anime</option></select>`}
function safeValue(value,fallback=''){return value===null||value===undefined?fallback:value}
function escapeHtml(value){return String(safeValue(value,'')).replace(/[&<>"']/g,function(ch){if(ch==='&'){return '&amp;'}if(ch==='<'){return '&lt;'}if(ch==='>'){return '&gt;'}if(ch==='"'){return '&quot;'}return '&#39;'})}
function formatBytes(value){if(value===null||value===undefined||value===''){return 'n/a'};const units=['B','KB','MB','GB','TB'];let size=Number(value);let unit=0;while(size>=1024&&unit<units.length-1){size/=1024;unit+=1}return `${size.toFixed(size>=10||unit===0?0:1)} ${units[unit]}`}
function formatPercent(value){return (value===null||value===undefined||value==='')?'n/a':`${Number(value).toFixed(1)} %`}
function formatRatio(value){return (value===null||value===undefined)?'n/a':`${(Number(value)*100).toFixed(1)} %`}
function formatTime(value){return value?escapeHtml(value):'n/a'}
function guessMediaType(path){const text=String(path||'').toLowerCase();if(text.includes('anime')){return 'anime'}if(text.includes('serial')||text.includes('series')){return 'series'}if(text.includes('film')||text.includes('movie')){return 'movie'}return 'auto'}
function statusTone(status){const text=String(status||'').toLowerCase();if(text==='done'||text==='refreshing_jellyfin'||text==='encoding'||text==='uploading'||text==='swapping'||text==='verifying'){return 'running'};return 'idle'}
function renderBadges(status){const items=[];items.push(`<span class="badge ${status.running?'badge-ok':'badge-idle'}">${status.running?'Loop active':'Idle'}</span>`);if(status.stop_after_current){items.push('<span class="badge badge-warn">Stop after current requested</span>')}if(status.force_stop){items.push('<span class="badge badge-warn">Force stop requested</span>')}if((status.errors||[]).length){items.push(`<span class="badge badge-warn">${status.errors.length} recent error${status.errors.length===1?'':'s'}</span>`)}if(status.last_result&&status.last_result.warning){items.push('<span class="badge badge-warn">Last job had warning</span>')}return items.join('')}
function renderCurrent(summary){if(!summary){return '<p class="muted">No active job.</p>'};const tone=statusTone(summary.status);return `<div class="status-shell"><div class="status-banner"><div><div class="status-title">Current phase</div><div class="status-value">${escapeHtml(summary.status||'idle')}</div></div><span class="status-pill status-${tone}">${summary.running?'active':'idle'}</span></div><div class="path-block"><div class="path-label">Current file</div><div class="code">${escapeHtml(summary.source_path||'No file selected yet')}</div></div><div class="metrics"><article class="metric"><div class="metric-label">Output size</div><div class="metric-value">${formatBytes(summary.output_size_bytes)}</div></article><article class="metric"><div class="metric-label">Progress time</div><div class="metric-value">${formatTime(summary.progress_time)}</div></article><article class="metric"><div class="metric-label">Progress</div><div class="metric-value">${formatPercent(summary.progress_percent)}</div></article><article class="metric"><div class="metric-label">FPS</div><div class="metric-value">${escapeHtml(summary.progress_fps||'n/a')}</div></article><article class="metric"><div class="metric-label">Speed</div><div class="metric-value">${escapeHtml(summary.progress_speed||'n/a')}</div></article></div></div>`}
function renderLastResult(result){if(!result){return '<p class="muted">No completed, skipped, or failed job yet.</p>'};const cls=result.status==='done'?'state-done':(result.status==='error'?'state-error':'state-skipped');const detail=result.error||result.skip_reason||'';const warning=result.warning?`<div class="callout warn">${escapeHtml(result.warning)}</div>`:'';const error=result.error?`<div class="callout err">${escapeHtml(result.error)}</div>`:'';return `<div class="result-shell"><div class="result-head"><div><span class="result-state ${cls}">${escapeHtml(result.status||'unknown')}</span><div class="summary-note" style="margin-top:10px">${escapeHtml(result.finished_at||'')}</div></div><div class="summary-note">Jellyfin: <strong>${escapeHtml(result.jellyfin_status||'n/a')}</strong></div></div><div class="path-block"><div class="path-label">Source path</div><div class="code">${escapeHtml(result.source_path||'')}</div></div>${warning}${error}<div class="delta-grid"><article class="summary-card"><div class="summary-kicker">Video</div><div class="summary-main">${escapeHtml(result.video_codec_before||'n/a')} → ${escapeHtml(result.video_codec_after||'n/a')}</div><div class="summary-note">Codec before and after replacement.</div></article><article class="summary-card"><div class="summary-kicker">Space</div><div class="summary-main">${formatBytes(result.bytes_saved)}</div><div class="summary-note">${formatBytes(result.source_size_bytes)} → ${formatBytes(result.output_size_bytes)}</div></article><article class="summary-card"><div class="summary-kicker">Bitrate</div><div class="summary-main">${result.overall_bitrate_kbps===null||result.overall_bitrate_kbps===undefined?'n/a':`${escapeHtml(result.overall_bitrate_kbps)} kbps`}</div><div class="summary-note">Final ratio ${formatRatio(result.output_to_source_ratio)}</div></article></div><div class="result-grid"><article class="summary-card"><div class="summary-kicker">Audio streams</div><div class="summary-main">${escapeHtml(safeValue(result.audio_stream_count_before,'n/a'))} → ${escapeHtml(safeValue(result.audio_stream_count_after,'n/a'))}</div><div class="summary-note">Track-policy result.</div></article><article class="summary-card"><div class="summary-kicker">Subtitle streams</div><div class="summary-main">${escapeHtml(safeValue(result.subtitle_stream_count_before,'n/a'))} → ${escapeHtml(safeValue(result.subtitle_stream_count_after,'n/a'))}</div><div class="summary-note">Conservative subtitle retention.</div></article></div>${detail&&!result.error?`<div class="callout warn">${escapeHtml(detail)}</div>`:''}</div>`}
function heroSummary(status){if(status.running&&status.current_summary&&status.current_summary.source_path){return `Working on ${status.current_summary.source_path}`};if(status.last_result&&status.last_result.status==='done'){return 'Last job finished successfully and the replacement passed final verification.'};if(status.last_result&&status.last_result.status==='error'){return 'The last job failed. Check the error list and log before restarting.'};if(status.last_result&&status.last_result.status==='skipped'){return 'The last scanned file was skipped by the current policy.'};return 'Idle and waiting for a deliberate start.'}
function collectSelectedFolders(){return [...document.querySelectorAll('#selectedFolders .simple-item')].map(item=>({path:item.getAttribute('data-path'),media_type:(item.querySelector('select')||{value:'auto'}).value}))}
function saveSelectedFolders(){post('/api/folders',{folders:collectSelectedFolders()})}
function addFolder(path){const current=collectSelectedFolders();if(current.some(item=>item.path===path)){return}current.push({path,media_type:guessMediaType(path)});post('/api/folders',{folders:current})}
function lastSelectedFolderPath(){const first=document.querySelector('#selectedFolders .simple-item');return first?first.getAttribute('data-path'):''}
function ensureFolderBrowser(){let browser=document.getElementById('folderBrowser');if(browser){return browser}const selected=document.getElementById('selectedFolders');browser=document.createElement('div');browser.id='folderBrowser';browser.className='simple-list';browser.style.marginTop='14px';if(selected&&selected.parentNode){selected.parentNode.insertBefore(browser,selected)}return browser}
async function browseFolder(path=''){try{const url=path?`/api/browse-folders?path=${encodeURIComponent(path)}`:'/api/browse-folders';const payload=await fetch(url,{cache:'no-store'}).then(readJsonResponse);uiError='';renderFolderBrowser(payload)}catch(error){renderUiError(formatRequestError(error))}}
function renderFolderBrowser(payload){const browser=ensureFolderBrowser();const current=payload.current_path||'';const directories=payload.directories||[];const controls=[];if(current){controls.push(`<button class="primary" data-action="select" data-path="${escapeHtml(current)}">Vybrat tuto slozku</button>`)}if(payload.parent_path){controls.push(`<button class="ghost" data-action="browse" data-path="${escapeHtml(payload.parent_path)}">O uroven vys</button>`)}controls.push('<button class="ghost" data-action="browse" data-path="">Rooty</button>');controls.push('<button class="ghost" data-action="close">Zavrit</button>');browser.innerHTML=`<div class="path-block"><div class="path-label">Browser slozek</div><div class="code">${escapeHtml(current||'Vyberte root')}</div></div><div class="folder-actions" style="margin-top:10px">${controls.join('')}</div>${directories.length?directories.map(item=>`<div class="simple-item"><div class="simple-main"><div class="simple-title">${escapeHtml(item.name)}</div><div class="simple-sub">${escapeHtml(item.path)}</div></div><button class="ghost" data-action="browse" data-path="${escapeHtml(item.path)}">Otevrit</button><button class="primary" data-action="select" data-path="${escapeHtml(item.path)}">Vybrat</button></div>`).join(''):'<div class="empty">Zadne podadresare.</div>'}`;browser.querySelectorAll('button[data-action]').forEach(button=>{button.onclick=()=>{const action=button.getAttribute('data-action');const path=button.getAttribute('data-path')||'';if(action==='browse'){browseFolder(path)}else if(action==='select'){selectBrowsedFolder(path)}else if(action==='close'){closeFolderBrowser()}}})}
function selectBrowsedFolder(path){post('/api/custom-folder',{path:path,media_type:guessMediaType(path)});closeFolderBrowser()}
function closeFolderBrowser(){const browser=document.getElementById('folderBrowser');if(browser){browser.innerHTML=''}}
function pickFolder(initialDir=''){browseFolder(initialDir||'')}
function addManualFolder(){const input=document.getElementById('manualFolderPath');const path=(input&&input.value?input.value:'').trim();if(!path){return}post('/api/custom-folder',{path:path,media_type:guessMediaType(path)});if(input){input.value=''}}
function removeFolder(path){post('/api/folders',{folders:collectSelectedFolders().filter(item=>item.path!==path)})}
function ensureUpdateButton(){const controls=document.querySelector('.controls');if(!controls||document.getElementById('updateButton')){return}const button=document.createElement('button');button.className='outline';button.id='updateButton';button.textContent='Update';button.onclick=triggerUpdate;const testModeButton=document.getElementById('testModeButton');if(testModeButton&&testModeButton.parentNode===controls){controls.insertBefore(button,testModeButton)}else{controls.appendChild(button)}}
function triggerUpdate(){const button=document.getElementById('updateButton');if(button&&button.disabled){return}post('/api/update')}
function toggleTestMode(){const button=document.getElementById('testModeButton');const enable=!(button&&button.getAttribute('data-enabled')==='true');post('/api/test-mode',{enabled:enable})}
function render(s){lastStatus=s;const selected=s.selected_folders||[];const combinedErrors=uiError?[{at:'UI',message:uiError},...(s.errors||[])]:[...(s.errors||[])];const warningCount=(combinedErrors.length?1:0)+(s.last_result&&s.last_result.warning?1:0);document.getElementById('status').textContent=JSON.stringify(s,null,2);document.getElementById('current').innerHTML=renderCurrent(s.current_summary);document.getElementById('lastResult').innerHTML=renderLastResult(s.last_result);document.getElementById('errors').innerHTML=combinedErrors.length?combinedErrors.map(e=>`<div class="err">${escapeHtml(e.at||'')}<br>${escapeHtml(e.message||e)}</div>`).join(''):'<p class="muted">No recent app-level errors.</p>';document.getElementById('log').textContent=(s.recent_log_lines||[]).join(String.fromCharCode(10));document.getElementById('logMeta').textContent=`${(s.recent_log_lines||[]).length} lines`;document.getElementById('selectedFolders').innerHTML=selected.length?selected.map(item=>`<div class="simple-item" data-path="${escapeHtml(item.path)}"><div class="simple-main"><div class="simple-title">${escapeHtml(item.path)}</div><div class="simple-sub">Selected for scan</div></div>${mediaTypeSelect(item.media_type||guessMediaType(item.path),'onchange="saveSelectedFolders()"')}<button class="ghost" data-path="${escapeHtml(item.path)}" onclick="removeFolder(this.getAttribute('data-path'))">Remove</button></div>`).join(''):'<div class="empty">No folders selected.</div>';document.getElementById('selectedCount').textContent=String(selected.length);document.getElementById('heroPhase').textContent=escapeHtml((s.current_summary&&s.current_summary.status)||s.current_phase||'Idle');document.getElementById('warningCount').textContent=String(warningCount);document.getElementById('heroSummary').textContent=heroSummary(s);document.getElementById('heroBadges').innerHTML=renderBadges(s);ensureUpdateButton();document.getElementById('updateButton').disabled=!s.can_update;document.getElementById('updateButton').textContent=s.current_phase==='updating'?'Updating...':'Update';document.getElementById('testModeButton').textContent=s.test_mode?'Test mode ON':'Test mode';document.getElementById('testModeButton').setAttribute('data-enabled',s.test_mode?'true':'false');document.getElementById('testModeBanner').innerHTML=s.test_mode?`<div class="callout info" style="margin-top:14px">${escapeHtml(s.test_mode_message||'Bezi v test modu.')}</div>`:'';document.getElementById('folderSuggestionList').innerHTML=(s.folder_suggestions||[]).map(path=>`<option value="${escapeHtml(path)}"></option>`).join('');document.getElementById('folderPickerHint').textContent=s.folder_suggestion_mode==='linux-mounts'?'Explorer fallback pouziva mount navrhy z linux configu. Sit lze zadat primo jako mount cestu.':'Explorer fallback pouziva Windows roots z configu. Kdyz sit v dialogu chybi, vlozte UNC cestu rucne.'}
function updateFolderPickerHint(s){const hint=document.getElementById('folderPickerHint');if(!hint){return}hint.textContent=s.folder_suggestion_mode==='linux-mounts'?'Webovy browser pouziva povolene linux mount rooty z configu. Sit lze zadat primo jako mount cestu.':'Webovy browser pouziva povolene rooty z configu. Cestu muzete stale vlozit rucne.'}
const renderBase=render
render=function(s){renderBase(s);updateFolderPickerHint(s)}
setInterval(getStatus,1500);getStatus();
</script></body></html>"""


def run_server(config: dict[str, Any]) -> None:
    runtime_dir = Path(str((config.get("app") or {}).get("runtime_dir") or "runtime"))
    with LocalInstanceLock(runtime_dir):
        app = SimpleRipperApp(config)
        handler = type("BoundSimpleRipperHandler", (SimpleRipperHandler,), {"app": app})
        host = str((config.get("app") or {}).get("host") or "127.0.0.1")
        port = int((config.get("app") or {}).get("port") or 5055)
        print(f"SimpleRipper running at http://{host}:{port}")
        ThreadingHTTPServer((host, port), handler).serve_forever()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="simpleripper")
    parser.add_argument("command", choices=["web", "check-config", "rebuild-index", "cache-summary", "clear-failures", "clear-cache", "clear-folder-cache", "clear-file-cache", "clear-candidate-queue"])
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "check-config":
            print(json.dumps({"ok": True, "config": str(args.config), "roots": (config.get("libraries") or {}).get("roots") or []}, indent=2))
            return 0
        if args.command == "rebuild-index":
            folders = [Path(item.get("path")) if isinstance(item, dict) else Path(item) for item in (((config.get("scan") or {}).get("selected_folders") or (config.get("libraries") or {}).get("roots") or []))]
            result = fast_inventory_scan(folders, config)
            print(json.dumps({"ok": True, **result, "cache": worker_cache_summary(config)}, indent=2))
            return 0
        if args.command == "cache-summary":
            print(json.dumps(worker_cache_summary(config), indent=2))
            return 0
        if args.command == "clear-failures":
            clear_worker_failures(config)
            print(json.dumps({"ok": True, "cache": worker_cache_summary(config)}, indent=2))
            return 0
        if args.command == "clear-cache":
            clear_worker_cache(config)
            print(json.dumps({"ok": True, "cache": worker_cache_summary(config)}, indent=2))
            return 0
        if args.command == "clear-folder-cache":
            clear_worker_folder_cache(config)
            print(json.dumps({"ok": True, "cache": worker_cache_summary(config)}, indent=2))
            return 0
        if args.command == "clear-file-cache":
            clear_worker_file_cache(config)
            print(json.dumps({"ok": True, "cache": worker_cache_summary(config)}, indent=2))
            return 0
        if args.command == "clear-candidate-queue":
            clear_worker_candidate_queue(config)
            print(json.dumps({"ok": True, "cache": worker_cache_summary(config)}, indent=2))
            return 0
        run_server(config)
        return 0
    except InstanceLockError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 3
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())