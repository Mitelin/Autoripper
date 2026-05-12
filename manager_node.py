from __future__ import annotations

import shutil
import socket
import time
from pathlib import Path
from typing import Any, Callable

from queue_store import claim_next_job, init_state, move_job_file, read_json, read_node_control, sanitize_node_id, shared_state_dir, utc_now, write_json_atomic
from shared_locks import acquire_lock, release_lock
from worker_node import read_global_control


def node_id(config: dict[str, Any], override: str | None = None) -> str:
    configured = ((config.get("node") or {}).get("id"))
    return sanitize_node_id(override or configured or socket.gethostname())


def manager_settings(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("manager") or {}


def manager_execute_enabled(config: dict[str, Any], override: bool | None = None) -> bool:
    if override is not None:
        return override
    return bool(manager_settings(config).get("execute", False))


def manager_require_successful_jellyfin_refresh(config: dict[str, Any]) -> bool:
    return bool(manager_settings(config).get("require_successful_jellyfin_refresh", False))


def manager_enabled(config: dict[str, Any]) -> bool:
    roles = (config.get("node") or {}).get("roles") or {}
    return bool(manager_settings(config).get("enabled", False) and roles.get("manager", False))


def global_finalizer_check(config: dict[str, Any]) -> dict[str, Any]:
    control = read_global_control(config)
    queue_state = control.get("queue_state", "running")
    allow_finalizer = bool(control.get("allow_finalizer", True))
    return {
        "queue_state": queue_state,
        "allow_finalizer": allow_finalizer,
        "allowed_to_finalize": allow_finalizer and queue_state != "maintenance",
        "updated_at": control.get("updated_at"),
        "updated_by": control.get("updated_by"),
    }


def write_manager_heartbeat(
    config: dict[str, Any],
    node: str,
    manager_state: str,
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
        "manager_state": manager_state,
        "current_job_id": current_job_id,
        "current_phase": current_phase,
        "last_heartbeat": utc_now(),
        "run_continuously": bool(manager_settings(config).get("run_continuously", False)),
        "version": "0.1.0",
    }
    payload.update(extra or {})
    path = root / "manager" / f"{node}.json"
    write_json_atomic(path, payload)
    return path


def ready_output_check(job: dict[str, Any]) -> dict[str, Any]:
    ready_output_dir = job.get("ready_output_dir")
    ready_output_path = job.get("ready_output_path")
    ready_output_manifest = job.get("ready_output_manifest")
    if not ready_output_path:
        return {
            "ready_output_dir": ready_output_dir,
            "ready_output_path": None,
            "ready_output_manifest": ready_output_manifest,
            "exists": False,
            "manifest_exists": False,
            "required": False,
            "ok": True,
            "reason": "dry-run job has no ready output path",
        }
    bundle_dir = Path(str(ready_output_dir or Path(str(ready_output_path)).parent))
    path = Path(str(ready_output_path))
    exists = path.exists()
    manifest_exists = False
    manifest_matches_job = False
    manifest_matches_source = False
    required_files = {
        "bundle_dir": bundle_dir,
        "output": path,
        "ffprobe": bundle_dir / "output.ffprobe.json",
        "worker_log": bundle_dir / "worker_log.json",
        "checksum": bundle_dir / "checksum.sha256",
        "manifest": Path(str(ready_output_manifest)) if ready_output_manifest else bundle_dir / "manifest.json",
    }
    missing_files = [name for name, file_path in required_files.items() if not file_path.exists()]
    if required_files["manifest"].exists():
        manifest_exists = True
        manifest = read_json(required_files["manifest"])
        manifest_matches_job = manifest.get("job_id") == job.get("job_id")
        manifest_matches_source = manifest.get("source_path") == job.get("source_path")
    return {
        "ready_output_dir": str(bundle_dir),
        "ready_output_path": str(path),
        "ready_output_manifest": str(required_files["manifest"]),
        "exists": exists,
        "manifest_exists": manifest_exists,
        "manifest_matches_job": manifest_matches_job,
        "manifest_matches_source": manifest_matches_source,
        "required": True,
        "missing_files": missing_files,
        "ok": exists and manifest_exists and not missing_files and manifest_matches_job and manifest_matches_source,
    }


def build_manager_source_item(job: dict[str, Any], manifest: dict[str, Any] | None = None) -> dict[str, Any]:
    source_summary = (manifest or {}).get("source_summary") or {}
    return {
        "media_type": source_summary.get("media_type") or job.get("media_type") or "unknown",
        "file_size_bytes": source_summary.get("file_size_bytes") or job.get("source_size_bytes") or job.get("file_size_bytes") or 0,
        "duration_seconds": source_summary.get("duration_seconds") if source_summary.get("duration_seconds") is not None else job.get("duration_seconds"),
        "audio_stream_count": source_summary.get("audio_stream_count") if source_summary.get("audio_stream_count") is not None else job.get("audio_stream_count"),
        "subtitle_stream_count": source_summary.get("subtitle_stream_count") if source_summary.get("subtitle_stream_count") is not None else job.get("subtitle_stream_count"),
    }


def ready_output_worker_log_path(job: dict[str, Any], manifest: dict[str, Any]) -> Path | None:
    path_value = job.get("ready_output_worker_log") or manifest.get("worker_log_path")
    if path_value:
        return Path(str(path_value))
    if job.get("ready_output_dir"):
        return Path(str(job["ready_output_dir"])) / "worker_log.json"
    return None


def applied_track_policy_from_payloads(*payloads: dict[str, Any]) -> dict[str, Any]:
    fallback: dict[str, Any] = {}
    for payload in payloads:
        if not payload:
            continue
        candidates = [payload.get("track_policy"), ((payload.get("local_processing") or {}).get("track_policy") if isinstance(payload.get("local_processing"), dict) else None)]
        for candidate in candidates:
            if isinstance(candidate, dict) and candidate:
                if candidate.get("applied"):
                    return candidate
                if not fallback:
                    fallback = candidate
    return fallback


def run_manager_verification(config: dict[str, Any], job: dict[str, Any]) -> dict[str, Any]:
    if not job.get("ready_output_path"):
        return {
            "ok": True,
            "mode": "no_ready_output_path",
            "verification": {"skipped": True, "reason": "no_ready_output_path", "ok": True},
            "output_summary": None,
            "errors": [],
        }
    manifest_path = job.get("ready_output_manifest")
    ffprobe_path = job.get("ready_output_ffprobe") or (Path(str(job["ready_output_dir"])) / "output.ffprobe.json" if job.get("ready_output_dir") else None)
    manifest = read_json(Path(str(manifest_path))) if manifest_path and Path(str(manifest_path)).exists() else {}
    worker_log_path = ready_output_worker_log_path(job, manifest)
    worker_log = read_json(worker_log_path) if worker_log_path and worker_log_path.exists() else {}
    source_item = build_manager_source_item(job, manifest)
    if bool(job.get("dry_run") or manifest.get("dry_run")):
        ffprobe_payload = read_json(Path(str(ffprobe_path))) if ffprobe_path and Path(str(ffprobe_path)).exists() else {}
        output_summary = ffprobe_payload.get("output_summary") or {}
        track_policy_result = applied_track_policy_from_payloads(ffprobe_payload, manifest, job, worker_log)
        from media_normalizer import expected_stream_counts

        expected_counts = expected_stream_counts(source_item, track_policy_result)
        expected_audio_count = expected_counts["expected_audio_stream_count"]
        expected_subtitle_count = expected_counts["expected_subtitle_stream_count"]
        errors: list[str] = []
        output_audio_count = output_summary.get("audio_stream_count")
        output_subtitle_count = output_summary.get("subtitle_stream_count")
        audio_streams_ok = (output_audio_count == expected_audio_count and (output_audio_count or 0) >= 1) if expected_audio_count is not None else output_audio_count is None or output_audio_count >= 1
        subtitle_streams_ok = output_subtitle_count == expected_subtitle_count if expected_subtitle_count is not None else True
        verification = {
            "ffprobe_payload_ok": bool(ffprobe_payload),
            "job_id_matches": ffprobe_payload.get("job_id") == job.get("job_id"),
            "source_path_matches": ffprobe_payload.get("source_path") == job.get("source_path"),
            "video_stream_exists": bool(output_summary.get("video_codec")),
            "audio_streams_ok": audio_streams_ok,
            "subtitle_streams_ok": subtitle_streams_ok,
            **expected_counts,
            "duration_ok": source_item.get("duration_seconds") is None or output_summary.get("duration_seconds") == source_item.get("duration_seconds"),
            "output_non_empty": (output_summary.get("file_size_bytes") or 0) > 0,
        }
        hard_verification_keys = ["ffprobe_payload_ok", "job_id_matches", "source_path_matches", "video_stream_exists", "audio_streams_ok", "subtitle_streams_ok", "duration_ok", "output_non_empty"]
        verification["ok"] = all(bool(verification.get(key)) for key in hard_verification_keys)
        for key in hard_verification_keys:
            value = verification.get(key)
            if not value:
                errors.append(f"Verification failed: {key}")
        return {
            "ok": verification["ok"],
            "mode": "dry_run_bundle",
            "verification": verification,
            "output_summary": output_summary,
            "errors": errors,
        }

    from media_normalizer import verify_output

    track_policy_result = applied_track_policy_from_payloads(manifest, job, worker_log)
    verification, output_summary, errors = verify_output(config, source_item, Path(str(job["ready_output_path"])), track_policy_result)
    return {
        "ok": not errors,
        "mode": "ffprobe_output",
        "verification": verification,
        "output_summary": output_summary,
        "errors": errors,
    }


def shared_locks_enabled(config: dict[str, Any]) -> bool:
    settings = config.get("io_limits") or {}
    return bool(settings.get("use_shared_locks", True))


def write_finalization_log(config: dict[str, Any], job: dict[str, Any], payload: dict[str, Any]) -> Path:
    root = init_state(config)
    job_id = str(job.get("job_id") or "unknown_job")
    path = root / "logs" / "manager" / f"{job_id}.json"
    write_json_atomic(path, payload)
    return path


def cleanup_ready_output_bundle(job: dict[str, Any]) -> dict[str, Any]:
    bundle_dir = job.get("ready_output_dir")
    if not bundle_dir:
        return {"attempted": False, "removed": False, "reason": "no_ready_output_dir"}
    path = Path(str(bundle_dir))
    if not path.exists():
        return {"attempted": True, "removed": False, "reason": "already_missing", "path": str(path)}
    shutil.rmtree(path)
    return {"attempted": True, "removed": True, "path": str(path)}


def matching_library_root(config: dict[str, Any], source_path: str) -> Path | None:
    source = Path(str(source_path))
    matches: list[Path] = []
    for roots in (config.get("libraries") or {}).values():
        for root_value in roots or []:
            root = Path(str(root_value))
            try:
                source.relative_to(root)
            except ValueError:
                continue
            matches.append(root)
    if not matches:
        return None
    return max(matches, key=lambda path: len(path.parts))


def quarantine_root(config: dict[str, Any], source_path: str) -> Path:
    configured = ((config.get("quarantine") or {}).get("path"))
    if configured:
        return Path(str(configured))
    library_root = matching_library_root(config, source_path)
    if library_root is not None:
        return library_root.parent / ".ripper_quarantine"
    return shared_state_dir(config).parent / ".ripper_quarantine"


def quarantine_relative_path(config: dict[str, Any], source_path: str) -> Path:
    source = Path(str(source_path))
    library_root = matching_library_root(config, source_path)
    if library_root is not None:
        return source.relative_to(library_root.parent)
    parts = list(source.parts)
    if source.anchor and parts and parts[0] == source.anchor:
        parts = parts[1:]
    return Path(*parts) if parts else Path(source.name)


def build_finalization_plan(config: dict[str, Any], job: dict[str, Any], verification_result: dict[str, Any], dry_run: bool = True) -> dict[str, Any]:
    source_path = str(job.get("source_path") or "")
    original = Path(source_path)
    quarantine_path = quarantine_root(config, source_path) / quarantine_relative_path(config, source_path)
    quarantine_target = quarantine_path.with_name(quarantine_path.name + ".original")
    manifest_path = shared_state_dir(config) / "quarantine_manifest" / f"{job.get('job_id')}.json"
    return {
        "original_path": str(original),
        "replacement_path": source_path,
        "ready_output_path": job.get("ready_output_path"),
        "ready_output_dir": job.get("ready_output_dir"),
        "quarantine_path": str(quarantine_target),
        "quarantine_manifest_path": str(manifest_path),
        "verified_output_summary": verification_result.get("output_summary"),
        "planned_at": utc_now(),
        "dry_run": dry_run,
    }


def original_source_check(job: dict[str, Any]) -> dict[str, Any]:
    source_path = str(job.get("source_path") or "")
    if not source_path:
        return {"source_path": source_path, "exists": False, "ok": False, "reason": "source_path_missing"}
    path = Path(source_path)
    exists = path.exists()
    return {"source_path": source_path, "exists": exists, "ok": exists, "reason": None if exists else "source_missing"}


def write_quarantine_manifest_plan(config: dict[str, Any], job: dict[str, Any], plan: dict[str, Any]) -> Path:
    path = Path(str(plan["quarantine_manifest_path"]))
    payload = {
        "job_id": job.get("job_id"),
        "original_path": plan.get("original_path"),
        "quarantine_path": plan.get("quarantine_path"),
        "replacement_path": plan.get("replacement_path"),
        "planned_at": plan.get("planned_at"),
        "dry_run": True,
        "status": "planned",
        "delete_after_days": 14,
    }
    write_json_atomic(path, payload)
    return path


def write_quarantine_manifest_executed(config: dict[str, Any], job: dict[str, Any], plan: dict[str, Any], execution_result: dict[str, Any]) -> Path:
    path = Path(str(plan["quarantine_manifest_path"]))
    payload = {
        "job_id": job.get("job_id"),
        "original_path": plan.get("original_path"),
        "quarantine_path": plan.get("quarantine_path"),
        "replacement_path": plan.get("replacement_path"),
        "planned_at": plan.get("planned_at"),
        "executed_at": utc_now(),
        "dry_run": False,
        "status": "executed",
        "delete_after_days": 14,
        "execution_result": execution_result,
    }
    write_json_atomic(path, payload)
    return path


def build_jellyfin_refresh_plan(config: dict[str, Any], source_path: str) -> dict[str, Any]:
    from media_normalizer import jellyfin_enabled, jellyfin_map_path

    jellyfin_path = jellyfin_map_path(config, source_path)
    if not jellyfin_enabled(config):
        return {
            "enabled": False,
            "status": "skipped",
            "reason": "Jellyfin refresh is disabled or not configured",
            "jellyfin_path": jellyfin_path,
            "dry_run": True,
        }
    return {
        "enabled": True,
        "status": "planned",
        "reason": "manager dry-run only planned Jellyfin refresh",
        "jellyfin_path": jellyfin_path,
        "dry_run": True,
    }


def execute_jellyfin_refresh(config: dict[str, Any], source_path: str) -> dict[str, Any]:
    from media_normalizer import jellyfin_refresh_source

    return jellyfin_refresh_source(config, source_path)


def manager_loop(
    config: dict[str, Any],
    node_override: str | None = None,
    force: bool = False,
    dry_run_result: str = "done",
    execute: bool | None = None,
    max_iterations: int | None = None,
    idle_sleep_seconds: float | None = None,
    stop_on_idle: bool = False,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    loop_settings = (manager_settings(config).get("loop") or {})
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
        stop_command = control.get("manager_command")
        if stop_command == "stop_after_current":
            stop_reason = "stop_after_current"
            break

        result = manager_step(config, node_override=node_override, force=force, dry_run_result=dry_run_result, execute=execute)
        results.append(result)

        if effective_max_iterations is not None and len(results) >= effective_max_iterations:
            stop_reason = "max_iterations"
            break

        status = str(result.get("status") or "")
        reason = str(result.get("reason") or "")
        if status == "idle" and reason == "no_ready_job_available":
            if stop_on_idle:
                stop_reason = "idle"
                break
            sleep_fn(effective_empty_backoff_seconds)
            slept_intervals.append(effective_empty_backoff_seconds)
            continue

        if status == "skipped" and reason in {"manager_disabled", "global_finalizer_paused", "finalizer_lock_unavailable"}:
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


def execute_finalization_plan(job: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    original_path = Path(str(plan.get("original_path") or ""))
    replacement_path = Path(str(plan.get("replacement_path") or ""))
    quarantine_path = Path(str(plan.get("quarantine_path") or ""))
    ready_output_path_value = plan.get("ready_output_path")
    if not ready_output_path_value:
        return {"ok": False, "reason": "ready_output_path_missing", "executed": False}
    ready_output_path = Path(str(ready_output_path_value))
    if not ready_output_path.exists():
        return {"ok": False, "reason": "ready_output_missing", "executed": False, "ready_output_path": str(ready_output_path)}
    if quarantine_path.exists():
        return {"ok": False, "reason": "quarantine_target_exists", "executed": False, "quarantine_path": str(quarantine_path)}

    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    replacement_path.parent.mkdir(parents=True, exist_ok=True)
    result = {
        "ok": False,
        "executed": False,
        "original_move": {"attempted": False, "completed": False, "from": str(original_path), "to": str(quarantine_path)},
        "replacement_move": {"attempted": False, "completed": False, "from": str(ready_output_path), "to": str(replacement_path)},
        "rollback": {"attempted": False, "completed": False},
    }
    try:
        result["original_move"]["attempted"] = True
        shutil.move(str(original_path), str(quarantine_path))
        result["original_move"]["completed"] = True

        result["replacement_move"]["attempted"] = True
        shutil.move(str(ready_output_path), str(replacement_path))
        result["replacement_move"]["completed"] = True
        result["ok"] = True
        result["executed"] = True
        return result
    except Exception as exc:
        result["error"] = str(exc)
        if result["original_move"]["completed"] and not result["replacement_move"]["completed"] and quarantine_path.exists() and not original_path.exists():
            result["rollback"]["attempted"] = True
            try:
                shutil.move(str(quarantine_path), str(original_path))
                result["rollback"]["completed"] = True
            except Exception as rollback_exc:
                result["rollback"]["error"] = str(rollback_exc)
        return result


def manager_step(config: dict[str, Any], node_override: str | None = None, force: bool = False, dry_run_result: str = "done", execute: bool | None = None) -> dict[str, Any]:
    node = node_id(config, node_override)
    enabled = manager_enabled(config)
    finalizer = global_finalizer_check(config)
    execute_mode = manager_execute_enabled(config, execute)
    execution_mode = "execute" if execute_mode else "dry_run"
    write_manager_heartbeat(config, node, "checking", "checking_controls", extra={"manager_enabled": enabled})

    if not enabled and not force:
        write_manager_heartbeat(config, node, "disabled", "manager_disabled", extra={"manager_enabled": enabled})
        return {"status": "skipped", "reason": "manager_disabled", "node_id": node}
    if not finalizer["allowed_to_finalize"] and not force:
        write_manager_heartbeat(config, node, "waiting", "global_finalizer_paused", extra={"global_finalizer": finalizer})
        return {"status": "skipped", "reason": "global_finalizer_paused", "node_id": node, "global_finalizer": finalizer}

    lock_result = {"acquired": False, "skipped": True, "reason": "shared locks disabled"}
    lock_path = None
    if shared_locks_enabled(config):
        write_manager_heartbeat(config, node, "waiting", "waiting_for_finalizer_lock")
        lock_result = acquire_lock(config, "finalizer", node, "manager-step")
        if not lock_result.get("acquired"):
            write_manager_heartbeat(config, node, "waiting", "finalizer_lock_unavailable", extra={"finalizer_lock": lock_result})
            return {"status": "skipped", "reason": "finalizer_lock_unavailable", "node_id": node, "finalizer_lock": lock_result}
        lock_path = Path(str(lock_result["path"]))

    try:
        claimed = claim_next_job(config, node_id=node, from_state="ready_for_finalize", to_state="finalizing")
        if not claimed:
            write_manager_heartbeat(config, node, "idle", "no_ready_job_available", extra={"finalizer_lock": lock_result})
            return {"status": "idle", "reason": "no_ready_job_available", "node_id": node, "finalizer_lock": lock_result}

        job = claimed["job"]
        claimed_path = Path(claimed["path"])
        phases: list[str] = []
        verify_phase = "verifying_ready_output" if execute_mode else "verifying_ready_output_dry_run"
        write_manager_heartbeat(config, node, "running", verify_phase, current_job_id=job.get("job_id"), extra={"finalizer_lock": lock_result, "execution_mode": execution_mode})
        phases.append(verify_phase)
        output_check = ready_output_check(job)
        if not output_check["ok"]:
            target = move_job_file(claimed_path, shared_state_dir(config) / "failed_finalize", "failed_finalize", {"failed_finalize_at": utc_now(), "dry_run": not execute_mode, "execution_mode": execution_mode, "ready_output_check": output_check, "error": "ready output missing"})
            finalization_log = write_finalization_log(
                config,
                job,
                {
                    "job_id": job.get("job_id"),
                    "source_path": job.get("source_path"),
                    "node_id": node,
                    "status": "failed_finalize",
                    "reason": "ready_output_missing",
                    "dry_run": not execute_mode,
                    "execution_mode": execution_mode,
                    "phases": phases,
                    "ready_output_check": output_check,
                    "finalizer_lock": lock_result,
                    "job_path": str(target),
                    "written_at": utc_now(),
                },
            )
            write_manager_heartbeat(config, node, "waiting", "ready_output_missing", extra={"ready_output_check": output_check, "finalizer_lock": lock_result})
            return {"status": "failed_finalize", "reason": "ready_output_missing", "node_id": node, "job_id": job.get("job_id"), "result_state": target.parent.name, "job_path": str(target), "execution_mode": execution_mode, "ready_output_check": output_check, "finalizer_lock": lock_result, "finalization_log": str(finalization_log)}

        verification_result = run_manager_verification(config, job)
        if not verification_result["ok"]:
            target = move_job_file(claimed_path, shared_state_dir(config) / "failed_finalize", "failed_finalize", {"failed_finalize_at": utc_now(), "dry_run": not execute_mode, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "error": "manager verification failed"})
            finalization_log = write_finalization_log(
                config,
                job,
                {
                    "job_id": job.get("job_id"),
                    "source_path": job.get("source_path"),
                    "node_id": node,
                    "status": "failed_finalize",
                    "reason": "manager_verification_failed",
                    "dry_run": not execute_mode,
                    "execution_mode": execution_mode,
                    "phases": phases,
                    "ready_output_check": output_check,
                    "verification": verification_result,
                    "finalizer_lock": lock_result,
                    "job_path": str(target),
                    "written_at": utc_now(),
                },
            )
            write_manager_heartbeat(config, node, "waiting", "manager_verification_failed", extra={"ready_output_check": output_check, "verification": verification_result, "finalizer_lock": lock_result})
            return {"status": "failed_finalize", "reason": "manager_verification_failed", "node_id": node, "job_id": job.get("job_id"), "result_state": target.parent.name, "job_path": str(target), "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "finalizer_lock": lock_result, "finalization_log": str(finalization_log)}

        source_check = original_source_check(job)
        if not source_check["ok"]:
            target = move_job_file(claimed_path, shared_state_dir(config) / "failed_finalize", "failed_finalize", {"failed_finalize_at": utc_now(), "dry_run": not execute_mode, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "error": "original source missing"})
            finalization_log = write_finalization_log(
                config,
                job,
                {
                    "job_id": job.get("job_id"),
                    "source_path": job.get("source_path"),
                    "node_id": node,
                    "status": "failed_finalize",
                    "reason": "original_source_missing",
                    "dry_run": not execute_mode,
                    "execution_mode": execution_mode,
                    "phases": phases,
                    "ready_output_check": output_check,
                    "verification": verification_result,
                    "original_source_check": source_check,
                    "finalizer_lock": lock_result,
                    "job_path": str(target),
                    "written_at": utc_now(),
                },
            )
            write_manager_heartbeat(config, node, "waiting", "original_source_missing", extra={"ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalizer_lock": lock_result})
            return {"status": "failed_finalize", "reason": "original_source_missing", "node_id": node, "job_id": job.get("job_id"), "result_state": target.parent.name, "job_path": str(target), "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalizer_lock": lock_result, "finalization_log": str(finalization_log)}

        finalization_plan = build_finalization_plan(config, job, verification_result, dry_run=not execute_mode)
        quarantine_manifest_path = write_quarantine_manifest_plan(config, job, finalization_plan)
        jellyfin_refresh_result = build_jellyfin_refresh_plan(config, str(job.get("source_path") or ""))

        if execute_mode:
            for phase in ("checking_original_exists", "quarantine_original", "move_output_to_library"):
                write_manager_heartbeat(config, node, "running", phase, current_job_id=job.get("job_id"), extra={"ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "finalizer_lock": lock_result, "execution_mode": execution_mode})
                phases.append(phase)
            execution_result = execute_finalization_plan(job, finalization_plan)
            if not execution_result["ok"]:
                target = move_job_file(claimed_path, shared_state_dir(config) / "failed_finalize", "failed_finalize", {"failed_finalize_at": utc_now(), "dry_run": False, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "quarantine_manifest_path": str(quarantine_manifest_path), "jellyfin_refresh": jellyfin_refresh_result, "execution_result": execution_result, "error": "manager execution failed"})
                ready_output_cleanup = {"attempted": False, "removed": False, "reason": "finalize_failed"}
                result_status = "failed_finalize"
                result_reason = str(execution_result.get("reason") or "manager_execution_failed")
            else:
                write_manager_heartbeat(config, node, "running", "jellyfin_refresh", current_job_id=job.get("job_id"), extra={"ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "finalizer_lock": lock_result, "execution_mode": execution_mode})
                phases.append("jellyfin_refresh")
                jellyfin_refresh_result = execute_jellyfin_refresh(config, str(job.get("source_path") or ""))
                quarantine_manifest_path = write_quarantine_manifest_executed(config, job, finalization_plan, execution_result)
                ready_output_cleanup = cleanup_ready_output_bundle(job)
                if manager_require_successful_jellyfin_refresh(config) and jellyfin_refresh_result.get("status") != "refreshed":
                    target = move_job_file(claimed_path, shared_state_dir(config) / "failed_finalize", "failed_finalize", {"failed_finalize_at": utc_now(), "dry_run": False, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "quarantine_manifest_path": str(quarantine_manifest_path), "jellyfin_refresh": jellyfin_refresh_result, "execution_result": execution_result, "error": "required jellyfin refresh failed after file replacement"})
                    result_status = "failed_finalize"
                    result_reason = "jellyfin_refresh_failed"
                else:
                    target = move_job_file(claimed_path, shared_state_dir(config) / "done", "done", {"finalized_at": utc_now(), "dry_run": False, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "quarantine_manifest_path": str(quarantine_manifest_path), "jellyfin_refresh": jellyfin_refresh_result, "execution_result": execution_result})
                    result_status = "complete"
                    result_reason = None
        else:
            for phase in ("checking_original_exists_dry_run", "quarantine_original_dry_run", "move_output_to_library_dry_run", "jellyfin_refresh_dry_run"):
                write_manager_heartbeat(config, node, "running", phase, current_job_id=job.get("job_id"), extra={"ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "finalizer_lock": lock_result, "execution_mode": execution_mode})
                phases.append(phase)

            execution_result = {"ok": True, "executed": False, "reason": "dry_run_only"}
            if dry_run_result == "failed_finalize":
                target = move_job_file(claimed_path, shared_state_dir(config) / "failed_finalize", "failed_finalize", {"failed_finalize_at": utc_now(), "dry_run": True, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "quarantine_manifest_path": str(quarantine_manifest_path), "jellyfin_refresh": jellyfin_refresh_result, "execution_result": execution_result, "error": "dry-run simulated finalizer failure"})
                ready_output_cleanup = {"attempted": False, "removed": False, "reason": "finalize_failed"}
                result_status = "dry_run_complete"
                result_reason = None
            elif dry_run_result == "requeue":
                target = move_job_file(claimed_path, shared_state_dir(config) / "ready_for_finalize", "ready_for_finalize", {"requeued_for_finalize_at": utc_now(), "dry_run": True, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "quarantine_manifest_path": str(quarantine_manifest_path), "jellyfin_refresh": jellyfin_refresh_result, "execution_result": execution_result})
                ready_output_cleanup = {"attempted": False, "removed": False, "reason": "job_requeued"}
                result_status = "dry_run_complete"
                result_reason = None
            else:
                target = move_job_file(claimed_path, shared_state_dir(config) / "done", "done", {"finalized_at": utc_now(), "dry_run": True, "execution_mode": execution_mode, "ready_output_check": output_check, "verification": verification_result, "original_source_check": source_check, "finalization_plan": finalization_plan, "quarantine_manifest_path": str(quarantine_manifest_path), "jellyfin_refresh": jellyfin_refresh_result, "execution_result": execution_result})
                ready_output_cleanup = cleanup_ready_output_bundle(job)
                result_status = "dry_run_complete"
                result_reason = None
        finalization_log = write_finalization_log(
            config,
            job,
            {
                "job_id": job.get("job_id"),
                "source_path": job.get("source_path"),
                "node_id": node,
                "status": target.parent.name,
                "dry_run": not execute_mode,
                "execution_mode": execution_mode,
                "phases": phases,
                "ready_output_check": output_check,
                "verification": verification_result,
                "original_source_check": source_check,
                "finalization_plan": finalization_plan,
                "quarantine_manifest_path": str(quarantine_manifest_path),
                "jellyfin_refresh": jellyfin_refresh_result,
                "execution_result": execution_result,
                "ready_output_cleanup": ready_output_cleanup,
                "finalizer_lock": lock_result,
                "job_path": str(target),
                "written_at": utc_now(),
            },
        )
        write_manager_heartbeat(config, node, "idle", result_status, extra={"execution_mode": execution_mode})
        result = {
            "status": result_status,
            "node_id": node,
            "job_id": job.get("job_id"),
            "source_path": job.get("source_path"),
            "execution_mode": execution_mode,
            "result_state": target.parent.name,
            "job_path": str(target),
            "ready_output_check": output_check,
            "verification": verification_result,
            "original_source_check": source_check,
            "finalization_plan": finalization_plan,
            "quarantine_manifest_path": str(quarantine_manifest_path),
            "jellyfin_refresh": jellyfin_refresh_result,
            "execution_result": execution_result,
            "ready_output_cleanup": ready_output_cleanup,
            "finalizer_lock": lock_result,
            "finalization_log": str(finalization_log),
        }
        if result_reason:
            result["reason"] = result_reason
        return result
    finally:
        if lock_path is not None:
            release_lock(lock_path)
