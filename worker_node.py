from __future__ import annotations

import hashlib
import json
import socket
import shutil
import time
from datetime import datetime, time as datetime_time
from pathlib import Path
from typing import Any, Callable

from queue_store import claim_next_job, init_state, read_json, read_node_control, sanitize_node_id, shared_state_dir, utc_now, write_json_atomic
from shared_locks import acquire_lock, release_lock


def parse_hhmm(value: str) -> datetime_time:
    hour_text, minute_text = value.split(":", 1)
    return datetime_time(hour=int(hour_text), minute=int(minute_text))


def time_is_inside_window(now_time: datetime_time, start: datetime_time, end: datetime_time) -> bool:
    if start == end:
        return True
    if start < end:
        return start <= now_time < end
    return now_time >= start or now_time < end


def node_id(config: dict[str, Any], override: str | None = None) -> str:
    configured = ((config.get("node") or {}).get("id"))
    return sanitize_node_id(override or configured or socket.gethostname())


def worker_settings(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("worker") or {}


def worker_execute_enabled(config: dict[str, Any], override: bool | None = None) -> bool:
    if override is not None:
        return override
    return bool(worker_settings(config).get("execute", False))


def worker_schedule_check(config: dict[str, Any]) -> dict[str, Any]:
    settings = worker_settings(config).get("schedule") or {}
    enabled = bool(settings.get("enabled", False))
    start_text = str(settings.get("start") or "00:00")
    end_text = str(settings.get("end") or "23:59")
    now = datetime.now().astimezone()
    allowed = True
    if enabled:
        allowed = time_is_inside_window(now.time(), parse_hhmm(start_text), parse_hhmm(end_text))
    return {
        "enabled": enabled,
        "start": start_text,
        "end": end_text,
        "outside_window_behavior": settings.get("outside_window_behavior", "finish_current_do_not_start_new"),
        "checked_at": now.isoformat(timespec="seconds"),
        "allowed_to_claim": allowed,
    }


def global_control_path(config: dict[str, Any]) -> Path:
    return shared_state_dir(config) / "control" / "global.json"


def read_global_control(config: dict[str, Any]) -> dict[str, Any]:
    init_state(config)
    path = global_control_path(config)
    if not path.exists():
        return {"queue_state": "running", "allow_new_claims": True, "allow_finalizer": True}
    return read_json(path)


def global_queue_check(config: dict[str, Any]) -> dict[str, Any]:
    control = read_global_control(config)
    queue_state = control.get("queue_state", "running")
    allowed = bool(control.get("allow_new_claims", True)) and queue_state == "running"
    return {
        "queue_state": queue_state,
        "allow_new_claims": bool(control.get("allow_new_claims", True)),
        "allowed_to_claim": allowed,
        "updated_at": control.get("updated_at"),
        "updated_by": control.get("updated_by"),
    }


def write_worker_heartbeat(
    config: dict[str, Any],
    node: str,
    worker_state: str,
    current_phase: str,
    current_job_id: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    root = init_state(config)
    roles = (config.get("node") or {}).get("roles") or {}
    payload = {
        "node_id": node,
        "hostname": socket.gethostname(),
        "roles": {
            "web_ui": bool(roles.get("web_ui", False)),
            "worker": bool(roles.get("worker", False)),
            "manager": bool(roles.get("manager", False)),
        },
        "worker_state": worker_state,
        "current_job_id": current_job_id,
        "current_phase": current_phase,
        "last_heartbeat": utc_now(),
        "schedule": worker_schedule_check(config),
        "local_work_dir": str(config.get("local_work_dir") or ""),
        "version": "0.1.0",
    }
    payload.update(extra or {})
    path = root / "workers" / f"{node}.json"
    write_json_atomic(path, payload)
    return path


def local_work_dir(config: dict[str, Any]) -> Path:
    return Path(config.get("local_work_dir") or ".work")


def _normalized_mapping_prefix(value: str) -> str:
    text = str(value or "")
    if text in {"/", "\\"}:
        return text
    return text.rstrip("/\\")


def _path_has_prefix(path: str, prefix: str) -> bool:
    if not prefix:
        return False
    return path == prefix or path.startswith(prefix + "/") or path.startswith(prefix + "\\")


def _apply_path_prefix_mapping(path: str, source_prefix: str, target_prefix: str) -> str:
    normalized_path = str(path or "")
    normalized_source = _normalized_mapping_prefix(source_prefix)
    normalized_target = _normalized_mapping_prefix(target_prefix)
    if not _path_has_prefix(normalized_path, normalized_source):
        return normalized_path
    suffix = normalized_path[len(normalized_source):]
    return f"{normalized_target}{suffix}"


def node_path_mappings(config: dict[str, Any]) -> list[dict[str, str]]:
    mappings = config.get("node_path_mappings") or []
    result: list[dict[str, str]] = []
    for item in mappings:
        if not isinstance(item, dict):
            continue
        canonical_prefix = _normalized_mapping_prefix(str(item.get("canonical_prefix") or ""))
        local_prefix = _normalized_mapping_prefix(str(item.get("local_prefix") or ""))
        if not canonical_prefix or not local_prefix:
            continue
        result.append({
            "canonical_prefix": canonical_prefix,
            "local_prefix": local_prefix,
        })
    return result


def map_canonical_to_local_path(config: dict[str, Any], path: str) -> str:
    mapped_path = str(path or "")
    for mapping in node_path_mappings(config):
        candidate = _apply_path_prefix_mapping(mapped_path, mapping["canonical_prefix"], mapping["local_prefix"])
        if candidate != mapped_path:
            return candidate
    return mapped_path


def map_local_to_canonical_path(config: dict[str, Any], path: str) -> str:
    mapped_path = str(path or "")
    for mapping in node_path_mappings(config):
        candidate = _apply_path_prefix_mapping(mapped_path, mapping["local_prefix"], mapping["canonical_prefix"])
        if candidate != mapped_path:
            return candidate
    return mapped_path


def shared_locks_enabled(config: dict[str, Any]) -> bool:
    settings = config.get("io_limits") or {}
    return bool(settings.get("use_shared_locks", True))


def worker_job_work_dir(config: dict[str, Any], node: str, job_id: str) -> Path:
    return local_work_dir(config) / sanitize_node_id(node) / str(job_id)


def prepare_local_source_cache(config: dict[str, Any], node: str, job: dict[str, Any]) -> dict[str, Any]:
    job_id = str(job.get("job_id") or "unknown-job")
    canonical_source_path = str(job.get("source_path") or "source.bin")
    worker_local_source_path = map_canonical_to_local_path(config, canonical_source_path)
    source_path = Path(worker_local_source_path)
    work_dir = worker_job_work_dir(config, node, job_id)
    input_dir = work_dir / "input"
    input_dir.mkdir(parents=True, exist_ok=True)
    local_source_path = input_dir / (source_path.name or "source.bin")
    if source_path.exists() and source_path.is_file():
        shutil.copy2(source_path, local_source_path)
        copy_mode = "copied"
    else:
        placeholder_text = f"dry-run local source cache for {job_id}\nsource={job.get('source_path') or ''}\nnode={node}\n"
        local_source_path.write_text(placeholder_text, encoding="utf-8")
        copy_mode = "placeholder"
    return {
        "work_dir": str(work_dir),
        "canonical_source_path": canonical_source_path,
        "worker_local_source_path": worker_local_source_path,
        "local_source_path": str(local_source_path),
        "source_copy_mode": copy_mode,
        "source_size_bytes": local_source_path.stat().st_size,
    }


def write_local_encoded_placeholder(config: dict[str, Any], node: str, job: dict[str, Any], local_cache: dict[str, Any]) -> dict[str, Any]:
    job_id = str(job.get("job_id") or "unknown-job")
    work_dir = Path(str(local_cache["work_dir"]))
    output_dir = work_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    local_output_path = output_dir / "output.mkv"
    placeholder_text = (
        f"dry-run encoded output for {job_id}\n"
        f"source={job.get('source_path') or ''}\n"
        f"node={node}\n"
        f"local_source={local_cache.get('local_source_path') or ''}\n"
    )
    local_output_path.write_text(placeholder_text, encoding="utf-8")
    return {
        "local_output_path": str(local_output_path),
        "local_output_size_bytes": local_output_path.stat().st_size,
    }


def run_local_ffmpeg_encode(config: dict[str, Any], job: dict[str, Any], local_cache: dict[str, Any]) -> dict[str, Any]:
    from media_normalizer import build_ffmpeg_command, verify_output
    from track_policy import apply_track_policy
    import subprocess

    local_source_path = Path(str(local_cache["local_source_path"]))
    work_dir = Path(str(local_cache["work_dir"]))
    output_dir = work_dir / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    local_output_path = output_dir / "output.mkv"
    source_item = job_source_summary(job)
    track_policy_result = apply_track_policy(config, job)
    command = build_ffmpeg_command(config, local_source_path, local_output_path, str(job.get("media_type") or "unknown"), track_policy_result)
    completed = shutil.which(command[0])
    if completed is None:
        return {
            "ok": False,
            "reason": "ffmpeg_not_found",
            "ffmpeg_command": command,
            "track_policy": track_policy_result,
        }

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace")
    hard_stop_reason = None
    while True:
        return_code = process.poll()
        if return_code is not None:
            break
        control = read_node_control(config, node_id(config))
        if control.get("worker_command") == "hard_stop":
            hard_stop_reason = "hard_stop_requested"
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            break
        schedule = worker_schedule_check(config)
        if schedule.get("enabled") and not schedule.get("allowed_to_claim") and schedule.get("outside_window_behavior") == "hard_stop":
            hard_stop_reason = "schedule_hard_stop_requested"
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            break
        time.sleep(0.2)

    stdout, stderr = process.communicate()
    if hard_stop_reason:
        partial_output_deleted = False
        if local_output_path.exists():
            local_output_path.unlink()
            partial_output_deleted = True
        return {
            "ok": False,
            "reason": hard_stop_reason,
            "ffmpeg_command": command,
            "track_policy": track_policy_result,
            "stderr": stderr.strip(),
            "stdout": stdout.strip(),
            "partial_output_deleted": partial_output_deleted,
            "source_untouched": True,
        }
    if process.returncode != 0:
        return {
            "ok": False,
            "reason": "ffmpeg_failed",
            "ffmpeg_command": command,
            "track_policy": track_policy_result,
            "stderr": stderr.strip(),
            "stdout": stdout.strip(),
        }
    verification, output_summary, errors = verify_output(config, source_item, local_output_path, track_policy_result)
    return {
        "ok": not errors,
        "reason": None if not errors else "verification_failed",
        "ffmpeg_command": command,
        "track_policy": track_policy_result,
        "verification": verification,
        "output_summary": output_summary,
        "errors": errors,
        "local_output_path": str(local_output_path),
        "local_output_size_bytes": local_output_path.stat().st_size if local_output_path.exists() else 0,
    }


def cleanup_local_job_workspace(work_dir: str | None) -> dict[str, Any]:
    if not work_dir:
        return {"attempted": False, "removed": False, "reason": "no_work_dir"}
    path = Path(str(work_dir))
    if not path.exists():
        return {"attempted": True, "removed": False, "reason": "already_missing", "path": str(path)}
    shutil.rmtree(path)
    return {"attempted": True, "removed": True, "path": str(path)}


def required_local_space_bytes(job: dict[str, Any]) -> int:
    source_size = int(job.get("source_size_bytes") or 0)
    safety_margin = 5 * 1024 * 1024 * 1024
    return int(source_size * 1.5 + safety_margin)


def local_space_check(config: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    work_dir = local_work_dir(config)
    work_dir.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(work_dir)
    required = required_local_space_bytes(job)
    return {
        "local_work_dir": str(work_dir),
        "required_bytes": required,
        "required_gb": round(required / 1024 / 1024 / 1024, 2),
        "available_bytes": usage.free,
        "available_gb": round(usage.free / 1024 / 1024 / 1024, 2),
        "enough_space": usage.free >= required,
    }


def canonical_job_filename(job: dict[str, Any]) -> str:
    return f"{job['job_id']}.json"


def move_claimed_job(config: dict[str, Any], claimed_path: Path, target_state: str, updates: dict[str, Any] | None = None) -> Path:
    root = init_state(config)
    job = read_json(claimed_path)
    target = root / target_state / canonical_job_filename(job)
    claimed_path.rename(target)
    job["status"] = target_state
    job.update(updates or {})
    write_json_atomic(target, job)
    return target


def write_interrupted_job_log(
    config: dict[str, Any],
    node: str,
    job: dict[str, Any],
    reason: str,
    space: dict[str, Any],
    local_processing: dict[str, Any] | None = None,
    execution_mode: str = "execute",
    requeued: bool = False,
) -> Path:
    root = init_state(config)
    timestamp = utc_now()
    safe_timestamp = datetime.now().astimezone().strftime("%Y%m%dT%H%M%S%z")
    path = root / "logs" / "jobs" / f"{job.get('job_id')}.interrupted.{safe_timestamp}.{sanitize_node_id(node)}.json"
    payload = {
        "job_id": job.get("job_id"),
        "node_id": node,
        "status": "interrupted",
        "reason": reason,
        "source_path": job.get("source_path"),
        "source_untouched": bool((local_processing or {}).get("source_untouched", False)),
        "partial_output_deleted": bool((local_processing or {}).get("partial_output_deleted", False)),
        "requeued": requeued,
        "execution_mode": execution_mode,
        "local_space_check": space,
        "local_processing": local_processing,
        "timestamp": timestamp,
    }
    write_json_atomic(path, payload)
    return path


def job_source_summary(job: dict[str, Any]) -> dict[str, Any]:
    return {
        "media_type": job.get("media_type", "unknown"),
        "file_size_bytes": job.get("source_size_bytes") or job.get("file_size_bytes"),
        "duration_seconds": job.get("duration_seconds"),
        "audio_stream_count": job.get("audio_stream_count"),
        "subtitle_stream_count": job.get("subtitle_stream_count"),
        "source_path": job.get("source_path"),
    }


def write_ready_output_artifacts(
    config: dict[str, Any],
    node: str,
    job: dict[str, Any],
    space: dict[str, Any],
    local_output_path: str | None = None,
    local_processing: dict[str, Any] | None = None,
) -> dict[str, Any]:
    root = init_state(config)
    ready_dir = root / "ready_outputs"
    job_id = str(job["job_id"])
    bundle_dir = ready_dir / job_id
    bundle_dir.mkdir(parents=True, exist_ok=True)
    placeholder_path = bundle_dir / "output.mkv"
    ffprobe_path = bundle_dir / "output.ffprobe.json"
    worker_log_path = bundle_dir / "worker_log.json"
    checksum_path = bundle_dir / "checksum.sha256"
    manifest_path = bundle_dir / "manifest.json"
    if local_output_path and Path(str(local_output_path)).exists():
        shutil.copy2(Path(str(local_output_path)), placeholder_path)
        placeholder_text = placeholder_path.read_text(encoding="utf-8")
    else:
        placeholder_text = f"dry-run ready output for {job_id}\nsource={job.get('source_path') or ''}\nnode={node}\n"
        placeholder_path.write_text(placeholder_text, encoding="utf-8")
    ffprobe_path.write_text(
        json.dumps(
            {
                "job_id": job_id,
                "source_path": job.get("source_path"),
                "node_id": node,
                "dry_run": True,
                "streams": [],
                "format": {
                    "filename": str(placeholder_path),
                    "size": placeholder_path.stat().st_size,
                    "format_name": "dry-run-placeholder",
                },
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    worker_log_payload = {
        "job_id": job_id,
        "node_id": node,
        "source_path": job.get("source_path"),
        "status": "ready_for_finalize",
        "dry_run": True,
        "local_space_check": space,
        "local_processing": local_processing,
        "written_at": utc_now(),
    }
    write_json_atomic(worker_log_path, worker_log_payload)
    checksum_path.write_text(f"{hashlib.sha256(placeholder_text.encode('utf-8')).hexdigest()}  output.mkv\n", encoding="utf-8")
    canonical_bundle_dir = map_local_to_canonical_path(config, str(bundle_dir))
    canonical_output_path = map_local_to_canonical_path(config, str(placeholder_path))
    canonical_ffprobe_path = map_local_to_canonical_path(config, str(ffprobe_path))
    canonical_worker_log_path = map_local_to_canonical_path(config, str(worker_log_path))
    canonical_checksum_path = map_local_to_canonical_path(config, str(checksum_path))
    canonical_manifest_path = map_local_to_canonical_path(config, str(manifest_path))
    source_summary = job_source_summary(job)
    output_summary = {
        "duration_seconds": source_summary.get("duration_seconds"),
        "audio_stream_count": source_summary.get("audio_stream_count"),
        "subtitle_stream_count": source_summary.get("subtitle_stream_count"),
        "video_codec": "hevc",
        "file_size_bytes": placeholder_path.stat().st_size,
        "file_size_mb": round(placeholder_path.stat().st_size / 1024 / 1024, 4),
        "container_format": "matroska",
        "video_width": 1920,
        "video_height": 1080,
        "video_pix_fmt": "yuv420p10le",
        "video_bitrate_kbps": None,
        "overall_bitrate_kbps": None,
    }
    write_json_atomic(
        ffprobe_path,
        {
            "job_id": job_id,
            "source_path": job.get("source_path"),
            "node_id": node,
            "dry_run": True,
            "source_summary": source_summary,
            "output_summary": output_summary,
            "streams": [],
            "format": {
                "filename": str(placeholder_path),
                "size": placeholder_path.stat().st_size,
                "format_name": "dry-run-placeholder",
            },
        },
    )
    manifest = {
        "job_id": job_id,
        "node_id": node,
        "source_path": job.get("source_path"),
        "ready_output_dir": canonical_bundle_dir,
        "ready_output_path": canonical_output_path,
        "ffprobe_path": canonical_ffprobe_path,
        "worker_log_path": canonical_worker_log_path,
        "checksum_path": canonical_checksum_path,
        "created_at": utc_now(),
        "dry_run": True,
        "source_summary": source_summary,
        "output_summary": output_summary,
        "local_space_check": space,
        "local_processing": local_processing,
    }
    write_json_atomic(manifest_path, manifest)
    return {
        "ready_output_dir": canonical_bundle_dir,
        "ready_output_path": canonical_output_path,
        "ready_output_manifest": canonical_manifest_path,
        "ready_output_ffprobe": canonical_ffprobe_path,
        "ready_output_worker_log": canonical_worker_log_path,
        "ready_output_checksum": canonical_checksum_path,
        "ready_output_created_at": manifest["created_at"],
    }


def worker_step(config: dict[str, Any], node_override: str | None = None, force: bool = False, dry_run_result: str = "requeue", execute: bool | None = None) -> dict[str, Any]:
    node = node_id(config, node_override)
    worker_enabled = bool(worker_settings(config).get("enabled", False))
    execute_mode = worker_execute_enabled(config, execute)
    schedule = worker_schedule_check(config)
    global_queue = global_queue_check(config)
    write_worker_heartbeat(config, node, "checking", "checking_controls", extra={"worker_enabled": worker_enabled})

    if execute_mode and dry_run_result != "ready":
        raise ValueError("worker execute mode requires dry_run_result=ready")

    if not worker_enabled and not force:
        write_worker_heartbeat(config, node, "disabled", "worker_disabled", extra={"worker_enabled": worker_enabled})
        return {"status": "skipped", "reason": "worker_disabled", "node_id": node}
    if not schedule["allowed_to_claim"] and not force:
        write_worker_heartbeat(config, node, "waiting", "outside_schedule", extra={"schedule": schedule})
        return {"status": "skipped", "reason": "outside_schedule", "node_id": node, "schedule": schedule}
    if not global_queue["allowed_to_claim"] and not force:
        write_worker_heartbeat(config, node, "waiting", "global_queue_paused", extra={"global_queue": global_queue})
        return {"status": "skipped", "reason": "global_queue_paused", "node_id": node, "global_queue": global_queue}

    claimed = claim_next_job(config, node_id=node)
    if not claimed:
        write_worker_heartbeat(config, node, "idle", "no_job_available")
        return {"status": "idle", "reason": "no_job_available", "node_id": node}

    job = claimed["job"]
    claimed_path = Path(claimed["path"])
    write_worker_heartbeat(config, node, "running", "checking_local_space", current_job_id=job.get("job_id"))
    space = local_space_check(config, job)
    if not space["enough_space"]:
        target = move_claimed_job(config, claimed_path, "queue", {"requeued_at": utc_now(), "dry_run": True, "last_skip_reason": "not_enough_local_space", "local_space_check": space})
        write_worker_heartbeat(config, node, "waiting", "not_enough_local_space", extra={"local_space_check": space})
        return {
            "status": "skipped",
            "reason": "not_enough_local_space",
            "node_id": node,
            "job_id": job.get("job_id"),
            "result_state": target.parent.name,
            "job_path": str(target),
            "local_space_check": space,
        }

    job_id = str(job.get("job_id") or "unknown-job")
    read_lock = None
    write_lock = None
    encode_lock = None
    local_cache = None
    local_output = None
    local_cleanup = {"attempted": False, "removed": False, "reason": "not_started"}
    encode_result = None
    try:
        write_worker_heartbeat(config, node, "running", "waiting_for_nas_read", current_job_id=job.get("job_id"), extra={"local_space_check": space})
        if shared_locks_enabled(config):
            read_lock = acquire_lock(config, "nas_read", node, job_id)
            if not read_lock.get("acquired"):
                target = move_claimed_job(config, claimed_path, "queue", {"requeued_at": utc_now(), "dry_run": True, "last_skip_reason": "no_nas_read_slot", "local_space_check": space, "nas_read_lock": read_lock})
                write_worker_heartbeat(config, node, "waiting", "no_nas_read_slot", current_job_id=job.get("job_id"), extra={"local_space_check": space, "nas_read_lock": read_lock})
                return {
                    "status": "skipped",
                    "reason": "no_nas_read_slot",
                    "node_id": node,
                    "job_id": job.get("job_id"),
                    "result_state": target.parent.name,
                    "job_path": str(target),
                    "local_space_check": space,
                    "nas_read_lock": read_lock,
                }
        write_worker_heartbeat(config, node, "running", "copying_source_to_local", current_job_id=job.get("job_id"), extra={"local_space_check": space, "nas_read_lock": read_lock})
        local_cache = prepare_local_source_cache(config, node, job)
    finally:
        if read_lock and read_lock.get("acquired"):
            release_lock(Path(str(read_lock["path"])))

    try:
        if execute_mode:
            write_worker_heartbeat(config, node, "running", "waiting_for_active_encode", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": local_cache, "execution_mode": "execute"})
            if shared_locks_enabled(config):
                encode_lock = acquire_lock(config, "active_encode", node, job_id)
                if not encode_lock.get("acquired"):
                    target = move_claimed_job(config, claimed_path, "queue", {"requeued_at": utc_now(), "last_skip_reason": "no_active_encode_slot", "local_space_check": space, "local_processing": local_cache, "active_encode_lock": encode_lock})
                    local_cleanup = cleanup_local_job_workspace((local_cache or {}).get("work_dir"))
                    write_worker_heartbeat(config, node, "waiting", "no_active_encode_slot", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": local_cache, "active_encode_lock": encode_lock, "execution_mode": "execute"})
                    return {
                        "status": "skipped",
                        "reason": "no_active_encode_slot",
                        "node_id": node,
                        "job_id": job.get("job_id"),
                        "result_state": target.parent.name,
                        "job_path": str(target),
                        "local_space_check": space,
                        "local_processing": local_cache,
                        "local_cleanup": local_cleanup,
                        "active_encode_lock": encode_lock,
                    }
            write_worker_heartbeat(config, node, "running", "encoding_execute", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": local_cache, "active_encode_lock": encode_lock, "execution_mode": "execute"})
            encode_result = run_local_ffmpeg_encode(config, job, local_cache or {})
            if not encode_result.get("ok"):
                interruption_reasons = {"hard_stop_requested", "schedule_hard_stop_requested"}
                failure_state = "interrupted" if encode_result.get("reason") in interruption_reasons else "failed"
                failure_timestamp_key = "interrupted_at" if failure_state == "interrupted" else "failed_at"
                local_processing_payload = {**(local_cache or {}), **(encode_result or {})}
                interrupted_log = None
                if failure_state == "interrupted":
                    interrupted_log = write_interrupted_job_log(
                        config,
                        node,
                        job,
                        str(encode_result.get("reason") or "hard_stop_requested"),
                        space,
                        local_processing=local_processing_payload,
                        execution_mode="execute",
                        requeued=False,
                    )
                target = move_claimed_job(config, claimed_path, failure_state, {failure_timestamp_key: utc_now(), "error": encode_result.get("reason") or "local_encode_failed", "local_space_check": space, "local_processing": local_processing_payload, "execution_mode": "execute", "source_untouched": bool(encode_result.get("source_untouched", False)), "partial_output_deleted": bool(encode_result.get("partial_output_deleted", False)), "interrupted_log": str(interrupted_log) if interrupted_log else None})
                local_cleanup = cleanup_local_job_workspace((local_cache or {}).get("work_dir"))
                failure_phase = str(encode_result.get("reason") or "local_encode_failed") if failure_state == "interrupted" else "local_encode_failed"
                write_worker_heartbeat(config, node, "waiting", failure_phase, current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": local_processing_payload, "execution_mode": "execute", "interrupted_log": str(interrupted_log) if interrupted_log else None})
                return {
                    "status": failure_state,
                    "reason": encode_result.get("reason") or "local_encode_failed",
                    "node_id": node,
                    "job_id": job.get("job_id"),
                    "result_state": target.parent.name,
                    "job_path": str(target),
                    "local_space_check": space,
                    "local_processing": local_processing_payload,
                    "local_cleanup": local_cleanup,
                    "interrupted_log": str(interrupted_log) if interrupted_log else None,
                }
            local_output = {"local_output_path": encode_result.get("local_output_path"), "local_output_size_bytes": encode_result.get("local_output_size_bytes")}
            write_worker_heartbeat(config, node, "running", "verifying_output_execute", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": {**(local_cache or {}), **(encode_result or {})}, "active_encode_lock": encode_lock, "execution_mode": "execute"})
        else:
            write_worker_heartbeat(config, node, "running", "encoding_dry_run", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": local_cache, "execution_mode": "dry_run"})
            local_output = write_local_encoded_placeholder(config, node, job, local_cache or {})
            write_worker_heartbeat(config, node, "running", "verifying_output_dry_run", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": {**(local_cache or {}), **(local_output or {})}, "execution_mode": "dry_run"})
    finally:
        if encode_lock and encode_lock.get("acquired"):
            release_lock(Path(str(encode_lock["path"])))

    if dry_run_result == "ready":
        try:
            processing_payload = {**(local_cache or {}), **(local_output or {}), **(encode_result or {})}
            write_worker_heartbeat(config, node, "running", "waiting_for_nas_write", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": processing_payload, "execution_mode": "execute" if execute_mode else "dry_run"})
            if shared_locks_enabled(config):
                write_lock = acquire_lock(config, "nas_write", node, job_id)
                if not write_lock.get("acquired"):
                    target = move_claimed_job(config, claimed_path, "queue", {"requeued_at": utc_now(), "dry_run": not execute_mode, "last_skip_reason": "no_nas_write_slot", "local_space_check": space, "local_processing": processing_payload, "nas_write_lock": write_lock, "execution_mode": "execute" if execute_mode else "dry_run"})
                    local_cleanup = cleanup_local_job_workspace((local_cache or {}).get("work_dir"))
                    write_worker_heartbeat(config, node, "waiting", "no_nas_write_slot", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": processing_payload, "nas_write_lock": write_lock, "execution_mode": "execute" if execute_mode else "dry_run"})
                    return {
                        "status": "skipped",
                        "reason": "no_nas_write_slot",
                        "node_id": node,
                        "job_id": job.get("job_id"),
                        "result_state": target.parent.name,
                        "job_path": str(target),
                        "local_space_check": space,
                        "local_processing": processing_payload,
                        "local_cleanup": local_cleanup,
                        "nas_write_lock": write_lock,
                    }
            write_worker_heartbeat(config, node, "running", "uploading_ready_output_dry_run" if not execute_mode else "uploading_ready_output_execute", current_job_id=job.get("job_id"), extra={"local_space_check": space, "local_processing": processing_payload, "nas_write_lock": write_lock, "execution_mode": "execute" if execute_mode else "dry_run"})
            ready_output = write_ready_output_artifacts(config, node, job, space, local_output_path=(local_output or {}).get("local_output_path"), local_processing=processing_payload)
            target = move_claimed_job(config, claimed_path, "ready_for_finalize", {"ready_for_finalize_at": utc_now(), "dry_run": not execute_mode, "local_space_check": space, "local_processing": processing_payload, "execution_mode": "execute" if execute_mode else "dry_run", **ready_output})
        finally:
            if write_lock and write_lock.get("acquired"):
                release_lock(Path(str(write_lock["path"])))
    elif dry_run_result == "failed":
        target = move_claimed_job(config, claimed_path, "failed", {"failed_at": utc_now(), "dry_run": not execute_mode, "error": "dry-run simulated failure", "local_space_check": space, "local_processing": {**(local_cache or {}), **(local_output or {}), **(encode_result or {})}, "execution_mode": "execute" if execute_mode else "dry_run"})
    else:
        target = move_claimed_job(config, claimed_path, "queue", {"requeued_at": utc_now(), "dry_run": not execute_mode, "local_space_check": space, "local_processing": {**(local_cache or {}), **(local_output or {}), **(encode_result or {})}, "execution_mode": "execute" if execute_mode else "dry_run"})
    local_cleanup = cleanup_local_job_workspace((local_cache or {}).get("work_dir"))
    write_worker_heartbeat(config, node, "idle", "execute_complete" if execute_mode else "dry_run_complete", current_job_id=None)
    return {
        "status": "dry_run_complete",
        "node_id": node,
        "job_id": job.get("job_id"),
        "source_path": job.get("source_path"),
        "execution_mode": "execute" if execute_mode else "dry_run",
        "result_state": target.parent.name,
        "job_path": str(target),
        "local_processing": {**(local_cache or {}), **(local_output or {}), **(encode_result or {})},
        "local_cleanup": local_cleanup,
    }


def worker_loop(
    config: dict[str, Any],
    node_override: str | None = None,
    force: bool = False,
    dry_run_result: str = "requeue",
    execute: bool | None = None,
    max_iterations: int | None = None,
    idle_sleep_seconds: float | None = None,
    stop_on_idle: bool = False,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    loop_settings = (worker_settings(config).get("loop") or {})
    effective_max_iterations = max_iterations if max_iterations is not None else loop_settings.get("max_iterations")
    if effective_max_iterations is not None:
        effective_max_iterations = int(effective_max_iterations)
        if effective_max_iterations <= 0:
            raise ValueError("max_iterations must be greater than 0")
    effective_idle_sleep_seconds = float(idle_sleep_seconds if idle_sleep_seconds is not None else loop_settings.get("idle_sleep_seconds", 5.0))
    if effective_idle_sleep_seconds < 0:
        raise ValueError("idle_sleep_seconds must be non-negative")
    effective_empty_backoff_seconds = float(loop_settings.get("empty_backoff_seconds", 3600.0))
    if effective_empty_backoff_seconds < 0:
        raise ValueError("empty_backoff_seconds must be non-negative")

    sleep_fn = sleeper or time.sleep
    results: list[dict[str, Any]] = []
    slept_intervals: list[float] = []
    stop_reason = "completed"
    stop_command = None

    while True:
        control = read_node_control(config, node_id(config, node_override))
        stop_command = control.get("worker_command")
        if stop_command in {"stop_after_current", "hard_stop"}:
            stop_reason = str(stop_command)
            break

        result = worker_step(config, node_override=node_override, force=force, dry_run_result=dry_run_result, execute=execute)
        results.append(result)

        if effective_max_iterations is not None and len(results) >= effective_max_iterations:
            stop_reason = "max_iterations"
            break

        status = str(result.get("status") or "")
        reason = str(result.get("reason") or "")
        if status == "idle" and reason == "no_job_available":
            if stop_on_idle:
                stop_reason = "idle"
                break
            sleep_fn(effective_empty_backoff_seconds)
            slept_intervals.append(effective_empty_backoff_seconds)
            continue

        if status == "skipped" and reason in {"worker_disabled", "outside_schedule", "global_queue_paused", "not_enough_local_space"}:
            sleep_fn(effective_idle_sleep_seconds)
            slept_intervals.append(effective_idle_sleep_seconds)
            continue

    status_counts: dict[str, int] = {}
    for item in results:
        key = str(item.get("status") or "unknown")
        status_counts[key] = status_counts.get(key, 0) + 1
    return {
        "status": "loop_complete",
        "node_id": node_id(config, node_override),
        "iterations": len(results),
        "stop_reason": stop_reason,
        "stop_on_idle": stop_on_idle,
        "idle_sleep_seconds": effective_idle_sleep_seconds,
        "empty_backoff_seconds": effective_empty_backoff_seconds,
        "max_iterations": effective_max_iterations,
        "stop_command": stop_command,
        "sleep_calls": len(slept_intervals),
        "slept_intervals": slept_intervals,
        "status_counts": status_counts,
        "last_result": results[-1] if results else None,
        "results": results,
    }
