from __future__ import annotations

import hashlib
import json
import os
import socket
from datetime import datetime
from pathlib import Path
from typing import Any


JOB_STATES = (
    "queue",
    "claimed",
    "running",
    "ready_for_finalize",
    "finalizing",
    "done",
    "failed",
    "failed_finalize",
    "interrupted",
    "stale",
)
SUPPORT_DIRS = (
    "logs/jobs",
    "logs/workers",
    "logs/manager",
    "manager",
    "workers",
    "ready_outputs",
    "quarantine_manifest",
    "locks/nas_read",
    "locks/nas_write",
    "locks/active_encode",
    "locks/finalizer",
    "control/nodes",
    "config",
    "production",
)


def utc_now() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def sanitize_node_id(value: str | None = None) -> str:
    node_id = value or socket.gethostname() or "unknown-node"
    safe = "".join(character if character.isalnum() or character in {"-", "_"} else "-" for character in node_id.strip())
    return safe or "unknown-node"


def shared_state_dir(config: dict[str, Any]) -> Path:
    configured = config.get("shared_state_dir")
    if configured:
        return Path(configured)
    return Path(config["output_root"]) / ".ripper_state"


def state_path(config: dict[str, Any], state: str) -> Path:
    return shared_state_dir(config) / state


def init_state(config: dict[str, Any]) -> Path:
    root = shared_state_dir(config)
    for directory in (*JOB_STATES, *SUPPORT_DIRS):
        (root / directory).mkdir(parents=True, exist_ok=True)
    global_control = root / "control" / "global.json"
    if not global_control.exists():
        write_json_atomic(
            global_control,
            {
                "queue_state": "running",
                "allow_new_claims": True,
                "allow_finalizer": True,
                "updated_at": utc_now(),
                "updated_by": "init_state",
            },
        )
    return root


def write_json_atomic(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temp_path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    os.replace(temp_path, path)


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def job_id_for_source(source_path: str, source_size_bytes: int | None = None, source_mtime_ns: int | None = None) -> str:
    identity = f"{os.path.normcase(os.path.abspath(source_path))}|{source_size_bytes or ''}|{source_mtime_ns or ''}"
    return "job_" + hashlib.sha256(identity.encode("utf-8", errors="surrogatepass")).hexdigest()[:16]


def job_exists(root: Path, job_id: str) -> Path | None:
    for state in JOB_STATES:
        matches = sorted((root / state).glob(f"{job_id}*.json"))
        if matches:
            return matches[0]
    return None


def build_job_payload(item: dict[str, Any], priority: str = "normal", created_by: str | None = None) -> dict[str, Any]:
    job_id = job_id_for_source(item["source_path"], item.get("file_size_bytes"), item.get("source_mtime_ns"))
    return {
        "schema_version": 1,
        "job_id": job_id,
        "status": "queue",
        "source_path": item.get("source_path"),
        "library_root": item.get("library_root"),
        "media_type": item.get("media_type"),
        "bucket": item.get("bucket"),
        "priority": priority,
        "source_size_bytes": item.get("file_size_bytes"),
        "source_size_mb": item.get("file_size_mb"),
        "duration_seconds": item.get("duration_seconds"),
        "video_codec": item.get("video_codec"),
        "audio_stream_count": item.get("audio_stream_count"),
        "subtitle_stream_count": item.get("subtitle_stream_count"),
        "created_at": utc_now(),
        "created_by": created_by or sanitize_node_id(),
    }


def enqueue_job(config: dict[str, Any], job: dict[str, Any]) -> tuple[bool, Path]:
    root = init_state(config)
    job_id = str(job["job_id"])
    existing = job_exists(root, job_id)
    if existing:
        return False, existing
    target = root / "queue" / f"{job_id}.json"
    write_json_atomic(target, job)
    return True, target


def list_state_files(config: dict[str, Any], state: str) -> list[Path]:
    return sorted(state_path(config, state).glob("*.json"))


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def heartbeat_stale_after_seconds(config: dict[str, Any]) -> int:
    heartbeat = config.get("heartbeat") or {}
    return int(heartbeat.get("stale_after_seconds", 60))


def heartbeat_age_seconds(value: str | None) -> int | None:
    heartbeat_time = parse_timestamp(value)
    now = parse_timestamp(utc_now())
    if heartbeat_time is None or now is None:
        return None
    age = int((now - heartbeat_time).total_seconds())
    return max(age, 0)


def summarize_heartbeat(config: dict[str, Any], payload: dict[str, Any], role: str) -> dict[str, Any]:
    state_key = f"{role}_state"
    age_seconds = heartbeat_age_seconds(payload.get("last_heartbeat"))
    stale_after = heartbeat_stale_after_seconds(config)
    return {
        "node_id": payload.get("node_id"),
        "hostname": payload.get("hostname"),
        "roles": payload.get("roles") or {},
        "state": payload.get(state_key),
        "current_phase": payload.get("current_phase"),
        "current_job_id": payload.get("current_job_id"),
        "last_heartbeat": payload.get("last_heartbeat"),
        "heartbeat_age_seconds": age_seconds,
        "heartbeat_stale": age_seconds is None or age_seconds > stale_after,
        "stale_after_seconds": stale_after,
        "raw": payload,
    }


def read_heartbeat_summaries(config: dict[str, Any], directory: Path, role: str) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(directory.glob("*.json")):
        summaries.append(summarize_heartbeat(config, read_json(path), role))
    return summaries


def aggregate_heartbeat_summaries(summaries: list[dict[str, Any]]) -> dict[str, Any]:
    state_counts: dict[str, int] = {}
    phase_counts: dict[str, int] = {}
    stale = 0
    with_current_job = 0
    for summary in summaries:
        state = str(summary.get("state") or "unknown")
        phase = str(summary.get("current_phase") or "unknown")
        state_counts[state] = state_counts.get(state, 0) + 1
        phase_counts[phase] = phase_counts.get(phase, 0) + 1
        if bool(summary.get("heartbeat_stale")):
            stale += 1
        if summary.get("current_job_id"):
            with_current_job += 1
    total = len(summaries)
    return {
        "total": total,
        "healthy": max(total - stale, 0),
        "stale": stale,
        "with_current_job": with_current_job,
        "idle_without_job": sum(
            1
            for summary in summaries
            if not summary.get("current_job_id") and str(summary.get("state") or "") in {"idle", "waiting"}
        ),
        "state_counts": state_counts,
        "phase_counts": phase_counts,
    }


def ready_outputs_disk_usage(config: dict[str, Any]) -> dict[str, Any]:
    root = init_state(config)
    ready_outputs_root = root / "ready_outputs"
    total_bytes = 0
    file_count = 0
    dir_count = 0
    if ready_outputs_root.exists():
        for child in ready_outputs_root.iterdir():
            if not child.is_dir():
                continue
            dir_count += 1
            for dirpath, _, filenames in os.walk(child):
                for filename in filenames:
                    path = Path(dirpath) / filename
                    try:
                        total_bytes += path.stat().st_size
                        file_count += 1
                    except OSError:
                        continue
    return {
        "ready_outputs_path": str(ready_outputs_root),
        "ready_outputs_total_size_bytes": total_bytes,
        "ready_outputs_total_size_gb": round(total_bytes / 1024 / 1024 / 1024, 3),
        "ready_outputs_dir_count": dir_count,
        "ready_outputs_file_count": file_count,
    }


def queue_status(config: dict[str, Any]) -> dict[str, Any]:
    root = init_state(config)
    states = {state: len(list((root / state).glob("*.json"))) for state in JOB_STATES}
    ready_outputs = ready_outputs_disk_usage(config)
    workers = len(list((root / "workers").glob("*.json")))
    worker_heartbeats = read_heartbeat_summaries(config, root / "workers", "worker")
    manager_heartbeats = read_heartbeat_summaries(config, root / "manager", "manager")
    global_control_path = root / "control" / "global.json"
    global_control = read_json(global_control_path) if global_control_path.exists() else {}
    node_controls = [read_json(path) for path in sorted((root / "control" / "nodes").glob("*.json"))]
    return {
        "shared_state_dir": str(root),
        "states": states,
        **ready_outputs,
        "workers": workers,
        "worker_heartbeats": worker_heartbeats,
        "worker_heartbeat_count": len(worker_heartbeats),
        "worker_summary": aggregate_heartbeat_summaries(worker_heartbeats),
        "managers": len(manager_heartbeats),
        "manager_heartbeats": manager_heartbeats,
        "manager_heartbeat_count": len(manager_heartbeats),
        "manager_summary": aggregate_heartbeat_summaries(manager_heartbeats),
        "global_control": global_control,
        "node_controls": node_controls,
        "node_control_count": len(node_controls),
    }


def set_global_control(config: dict[str, Any], queue_state: str, allow_new_claims: bool, allow_finalizer: bool, updated_by: str | None = None) -> Path:
    if queue_state not in {"running", "paused", "maintenance"}:
        raise ValueError("queue_state must be one of: running, paused, maintenance")
    root = init_state(config)
    path = root / "control" / "global.json"
    write_json_atomic(
        path,
        {
            "queue_state": queue_state,
            "allow_new_claims": allow_new_claims,
            "allow_finalizer": allow_finalizer,
            "updated_at": utc_now(),
            "updated_by": updated_by or sanitize_node_id(),
        },
    )
    return path


def node_control_path(config: dict[str, Any], node_id: str) -> Path:
    root = init_state(config)
    return root / "control" / "nodes" / f"{sanitize_node_id(node_id)}.json"


def read_node_control(config: dict[str, Any], node_id: str) -> dict[str, Any]:
    path = node_control_path(config, node_id)
    if not path.exists():
        return {
            "node_id": sanitize_node_id(node_id),
            "worker_command": None,
            "manager_command": None,
            "production_command": None,
            "updated_at": None,
            "updated_by": None,
        }
    control = read_json(path)
    control.setdefault("production_command", None)
    return control


def set_node_control(
    config: dict[str, Any],
    node_id: str,
    worker_command: str | None = None,
    manager_command: str | None = None,
    production_command: str | None = None,
    updated_by: str | None = None,
) -> Path:
    valid_worker_commands = {None, "stop_after_current", "hard_stop"}
    valid_manager_commands = {None, "stop_after_current"}
    valid_production_commands = {None, "running", "paused", "stop_after_current", "maintenance"}
    if worker_command not in valid_worker_commands:
        raise ValueError("worker_command must be one of: stop_after_current, hard_stop, none")
    if manager_command not in valid_manager_commands:
        raise ValueError("manager_command must be one of: stop_after_current, none")
    if production_command not in valid_production_commands:
        raise ValueError("production_command must be one of: running, paused, stop_after_current, maintenance, none")
    path = node_control_path(config, node_id)
    write_json_atomic(
        path,
        {
            "node_id": sanitize_node_id(node_id),
            "worker_command": worker_command,
            "manager_command": manager_command,
            "production_command": production_command,
            "updated_at": utc_now(),
            "updated_by": updated_by or sanitize_node_id(),
        },
    )
    return path


def claim_next_job(config: dict[str, Any], node_id: str | None = None, from_state: str = "queue", to_state: str = "running") -> dict[str, Any] | None:
    root = init_state(config)
    safe_node = sanitize_node_id(node_id)
    for source in sorted((root / from_state).glob("*.json")):
        job_id = source.stem.split(".", 1)[0]
        target = root / to_state / f"{job_id}.{safe_node}.json"

        try:
            job = read_json(source)
        except FileNotFoundError:
            continue

        job["status"] = to_state
        job["claimed_by"] = safe_node
        job["claimed_at"] = utc_now()

        try:
            write_json_atomic(target, job)
            source.unlink()
        except FileNotFoundError:
            target.unlink(missing_ok=True)
            continue
        except FileExistsError:
            continue

        return {"job": job, "path": str(target)}
    return None


def move_job_file(source: Path, target_dir: Path, status: str, updates: dict[str, Any] | None = None) -> Path:
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / source.name
    os.rename(source, target)
    job = read_json(target)
    job["status"] = status
    job.update(updates or {})
    write_json_atomic(target, job)
    return target


def recover_stale_running_jobs(config: dict[str, Any]) -> dict[str, Any]:
    root = init_state(config)
    summaries = read_heartbeat_summaries(config, root / "workers", "worker")
    recovered: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for summary in summaries:
        if not bool(summary.get("heartbeat_stale")):
            continue
        if str(summary.get("state") or "") != "running":
            continue
        node = sanitize_node_id(str(summary.get("node_id") or "unknown-node"))
        job_id = str(summary.get("current_job_id") or "")
        if not job_id:
            skipped.append({"node_id": node, "reason": "missing_current_job_id"})
            continue
        candidates = sorted((root / "running").glob(f"{job_id}*.json"))
        if not candidates:
            skipped.append({"node_id": node, "job_id": job_id, "reason": "running_job_missing"})
            continue
        source = next((path for path in candidates if read_json(path).get("claimed_by") == node), candidates[0])
        stale_timestamp = utc_now()
        target = move_job_file(
            source,
            root / "stale",
            "stale",
            {
                "stale_at": stale_timestamp,
                "stale_reason": "worker_heartbeat_expired",
                "stale_node_id": node,
                "last_known_worker_phase": summary.get("current_phase"),
                "last_known_heartbeat": summary.get("last_heartbeat"),
            },
        )
        job = read_json(target)
        safe_timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
        log_path = root / "logs" / "jobs" / f"{job_id}.stale.{safe_timestamp}.{node}.json"
        write_json_atomic(
            log_path,
            {
                "job_id": job_id,
                "node_id": node,
                "status": "stale",
                "reason": "worker_heartbeat_expired",
                "source_path": job.get("source_path"),
                "last_known_phase": summary.get("current_phase"),
                "last_known_heartbeat": summary.get("last_heartbeat"),
                "heartbeat_age_seconds": summary.get("heartbeat_age_seconds"),
                "timestamp": stale_timestamp,
            },
        )
        job["stale_log"] = str(log_path)
        write_json_atomic(target, job)
        recovered.append(
            {
                "node_id": node,
                "job_id": job_id,
                "source_path": str(source),
                "target_path": str(target),
                "stale_log": str(log_path),
                "heartbeat_age_seconds": summary.get("heartbeat_age_seconds"),
            }
        )
    return {
        "status": "complete",
        "checked_workers": len(summaries),
        "recovered": recovered,
        "recovered_count": len(recovered),
        "skipped": skipped,
        "skipped_count": len(skipped),
    }


def requeue_interrupted_jobs(config: dict[str, Any], job_ids: list[str] | None = None, limit: int | None = None) -> dict[str, Any]:
    root = init_state(config)
    selected_ids = {str(job_id) for job_id in (job_ids or []) if str(job_id).strip()}
    moved: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    candidates = list_state_files(config, "interrupted")
    for source in candidates:
        job = read_json(source)
        job_id = str(job.get("job_id") or source.stem.split(".", 1)[0])
        if selected_ids and job_id not in selected_ids:
            continue
        requeued_at = utc_now()
        target = move_job_file(
            source,
            root / "queue",
            "queue",
            {
                "requeued_at": requeued_at,
                "requeued_from_state": "interrupted",
                "last_requeue_reason": "manual_interrupted_requeue",
            },
        )
        safe_timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
        log_path = root / "logs" / "jobs" / f"{job_id}.requeue.{safe_timestamp}.json"
        write_json_atomic(
            log_path,
            {
                "job_id": job_id,
                "status": "queue",
                "reason": "manual_interrupted_requeue",
                "source_path": job.get("source_path"),
                "previous_state": "interrupted",
                "previous_error": job.get("error"),
                "timestamp": requeued_at,
            },
        )
        queued_job = read_json(target)
        queued_job["requeue_log"] = str(log_path)
        write_json_atomic(target, queued_job)
        moved.append(
            {
                "job_id": job_id,
                "source_path": str(source),
                "target_path": str(target),
                "requeue_log": str(log_path),
            }
        )
        if limit is not None and len(moved) >= int(limit):
            break
    if selected_ids:
        moved_ids = {item["job_id"] for item in moved}
        for missing_id in sorted(selected_ids - moved_ids):
            skipped.append({"job_id": missing_id, "reason": "interrupted_job_missing"})
    return {
        "status": "complete",
        "moved": moved,
        "moved_count": len(moved),
        "skipped": skipped,
        "skipped_count": len(skipped),
    }
