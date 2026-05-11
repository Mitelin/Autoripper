from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from queue_store import heartbeat_stale_after_seconds, init_state, parse_timestamp, read_json, sanitize_node_id, shared_state_dir, utc_now, write_json_atomic


LOCK_LIMIT_KEYS = {
    "nas_read": "max_concurrent_nas_reads",
    "nas_write": "max_concurrent_nas_writes",
    "active_encode": "max_concurrent_active_encodes",
    "finalizer": "max_concurrent_finalizers",
}
LOCK_DIRECTORIES = {
    "nas_read": "nas_read",
    "nas_write": "nas_write",
    "active_encode": "active_encode",
    "finalizer": "finalizer",
}


def io_limit(config: dict[str, Any], lock_type: str) -> int:
    settings = config.get("io_limits") or {}
    key = LOCK_LIMIT_KEYS.get(lock_type)
    if not key:
        raise ValueError(f"Unknown lock type: {lock_type}")
    return max(1, int(settings.get(key, 1)))


def lock_dir(config: dict[str, Any], lock_type: str) -> Path:
    directory = LOCK_DIRECTORIES.get(lock_type)
    if not directory:
        raise ValueError(f"Unknown lock type: {lock_type}")
    return shared_state_dir(config) / "locks" / directory


def active_locks(config: dict[str, Any], lock_type: str) -> list[Path]:
    init_state(config)
    return sorted(lock_dir(config, lock_type).glob("*.json"))


def lock_stale_after_seconds(config: dict[str, Any]) -> int:
    settings = config.get("io_limits") or {}
    return max(1, int(settings.get("lock_stale_after_seconds", heartbeat_stale_after_seconds(config))))


def lock_age_seconds(value: str | None) -> int | None:
    heartbeat_time = parse_timestamp(value)
    now = parse_timestamp(utc_now())
    if heartbeat_time is None or now is None:
        return None
    age = int((now - heartbeat_time).total_seconds())
    return max(age, 0)


def summarize_lock(config: dict[str, Any], payload: dict[str, Any], path: Path) -> dict[str, Any]:
    age_seconds = lock_age_seconds(payload.get("last_heartbeat"))
    stale_after = lock_stale_after_seconds(config)
    summary = dict(payload)
    summary["path"] = str(path)
    summary["heartbeat_age_seconds"] = age_seconds
    summary["heartbeat_stale"] = age_seconds is None or age_seconds > stale_after
    summary["stale_after_seconds"] = stale_after
    return summary


def acquire_lock(config: dict[str, Any], lock_type: str, node_id: str, job_id: str) -> dict[str, Any]:
    init_state(config)
    directory = lock_dir(config, lock_type)
    directory.mkdir(parents=True, exist_ok=True)
    limit = io_limit(config, lock_type)
    for slot in range(1, limit + 1):
        path = directory / f"slot_{slot}.json"
        payload = {
            "lock_type": lock_type,
            "slot": slot,
            "node_id": sanitize_node_id(node_id),
            "job_id": job_id,
            "acquired_at": utc_now(),
            "last_heartbeat": utc_now(),
        }
        try:
            handle = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            continue
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            import json

            json.dump(payload, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
        return {"acquired": True, "path": str(path), "slot": slot, "lock": payload}
    return {"acquired": False, "reason": "no_slot_available", "active_locks": len(active_locks(config, lock_type)), "limit": limit}


def refresh_lock(path: Path) -> None:
    lock = read_json(path)
    lock["last_heartbeat"] = utc_now()
    write_json_atomic(path, lock)


def release_lock(path: Path) -> bool:
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def recover_stale_locks(config: dict[str, Any], lock_types: list[str] | None = None) -> dict[str, Any]:
    init_state(config)
    selected_lock_types = lock_types or list(LOCK_DIRECTORIES)
    recovered: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for lock_type in selected_lock_types:
        for path in active_locks(config, lock_type):
            try:
                summary = summarize_lock(config, read_json(path), path)
            except Exception:
                errors.append({"lock_type": lock_type, "path": str(path), "error": "unreadable_lock"})
                continue
            if not bool(summary.get("heartbeat_stale")):
                continue
            release_lock(path)
            recovered.append(
                {
                    "lock_type": lock_type,
                    "slot": summary.get("slot"),
                    "node_id": summary.get("node_id"),
                    "job_id": summary.get("job_id"),
                    "path": str(path),
                    "heartbeat_age_seconds": summary.get("heartbeat_age_seconds"),
                    "stale_after_seconds": summary.get("stale_after_seconds"),
                    "reason": "lock_heartbeat_expired",
                }
            )
    return {
        "recovered_count": len(recovered),
        "recovered": recovered,
        "errors": errors,
        "lock_types": selected_lock_types,
    }


def lock_status(config: dict[str, Any]) -> dict[str, Any]:
    init_state(config)
    status: dict[str, Any] = {}
    for lock_type in LOCK_DIRECTORIES:
        locks = []
        for path in active_locks(config, lock_type):
            try:
                locks.append(summarize_lock(config, read_json(path), path))
            except Exception:
                locks.append({"path": str(path), "error": "unreadable lock"})
        status[lock_type] = {
            "limit": io_limit(config, lock_type),
            "active": len(locks),
            "stale": sum(1 for lock in locks if bool(lock.get("heartbeat_stale"))),
            "locks": locks,
        }
    return status
