from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import yaml

from manager_node import manager_step
from queue_store import JOB_STATES, init_state, list_state_files, queue_status, read_json, read_node_control, recover_stale_running_jobs, requeue_interrupted_jobs, sanitize_node_id, set_global_control, set_node_control, utc_now
from shared_locks import lock_status, recover_stale_locks
from worker_node import parse_hhmm, worker_schedule_check


def web_ui_settings(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("web_ui") or {}


def web_ui_host(config: dict[str, Any], override: str | None = None) -> str:
    if override:
        return override
    return str(web_ui_settings(config).get("host", "127.0.0.1"))


def web_ui_port(config: dict[str, Any], override: int | None = None) -> int:
    if override is not None:
        return int(override)
    return int(web_ui_settings(config).get("port", 5055))


def web_ui_auth_settings(config: dict[str, Any]) -> dict[str, Any]:
    return (web_ui_settings(config).get("auth") or {})


def web_ui_auth_enabled(config: dict[str, Any]) -> bool:
    settings = web_ui_auth_settings(config)
    return bool(settings.get("enabled", False) and settings.get("username") and settings.get("password_hash"))


def web_ui_auth_username(config: dict[str, Any]) -> str:
    return str(web_ui_auth_settings(config).get("username") or "admin")


def web_ui_session_cookie_name(config: dict[str, Any]) -> str:
    return str(web_ui_auth_settings(config).get("session_cookie_name") or "autoripper_session")


def web_ui_session_ttl_seconds(config: dict[str, Any]) -> int:
    return int(web_ui_auth_settings(config).get("session_ttl_seconds", 43200))


def hash_web_ui_password(password: str) -> str:
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def web_ui_auth_secret(config: dict[str, Any]) -> bytes:
    password_hash = str(web_ui_auth_settings(config).get("password_hash") or "")
    node = local_node_id(config)
    return hashlib.sha256(f"{password_hash}:{node}:web-ui".encode("utf-8")).digest()


def build_auth_token(config: dict[str, Any]) -> str:
    expires_at = int(time.time()) + web_ui_session_ttl_seconds(config)
    payload = str(expires_at)
    signature = hmac.new(web_ui_auth_secret(config), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload}.{signature}"


def is_valid_auth_token(config: dict[str, Any], token: str | None) -> bool:
    if not token:
        return False
    try:
        expires_at_text, signature = token.split(".", 1)
        expires_at = int(expires_at_text)
    except (ValueError, TypeError):
        return False
    expected_signature = hmac.new(web_ui_auth_secret(config), str(expires_at).encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected_signature):
        return False
    return expires_at >= int(time.time())


def build_login_html(config: dict[str, Any], error_message: str | None = None) -> str:
    username = web_ui_auth_username(config)
    error_block = ""
    if error_message:
        error_block = f'<p style="color:#fecaca; margin:0 0 14px;">{error_message}</p>'
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>Autoripper Login</title>
  <style>
    :root {{ color-scheme: dark; --bg: #0e141a; --panel: #1b2530; --text: #edf2f7; --muted: #98a7b8; }}
    body {{ margin: 0; min-height: 100vh; display:flex; align-items:center; justify-content:center; background: radial-gradient(circle at top, #1d2a36, #0b1117 62%); color: var(--text); font-family: Consolas, \"SFMono-Regular\", monospace; }}
    form {{ width:min(420px, calc(100vw - 32px)); background: rgba(27, 37, 48, 0.95); border:1px solid rgba(255,255,255,0.08); border-radius:18px; padding:24px; box-shadow: 0 18px 50px rgba(0,0,0,0.35); }}
    h1 {{ margin:0 0 8px; font-size:24px; }}
    p {{ margin:0 0 18px; color: var(--muted); }}
    label {{ display:block; margin: 0 0 14px; }}
    span {{ display:block; margin-bottom:6px; color: var(--muted); }}
    input {{ width:100%; box-sizing:border-box; border-radius:12px; border:1px solid rgba(255,255,255,0.12); background:#111923; color:var(--text); padding:10px 12px; font:inherit; }}
    button {{ border:0; border-radius:999px; padding:10px 14px; background:#1d4ed8; color:#eff6ff; font:inherit; cursor:pointer; }}
  </style>
</head>
<body>
  <form method=\"post\" action=\"/login\">
    <h1>Autoripper Login</h1>
    <p>Sign in for node <strong>{local_node_id(config)}</strong>.</p>
    {error_block}
    <label>
      <span>Username</span>
      <input type=\"text\" name=\"username\" value=\"{username}\" autocomplete=\"username\" required>
    </label>
    <label>
      <span>Password</span>
      <input type=\"password\" name=\"password\" autocomplete=\"current-password\" required>
    </label>
    <button type=\"submit\">Login</button>
  </form>
</body>
</html>
"""


def build_logout_cookie(config: dict[str, Any]) -> str:
    return f"{web_ui_session_cookie_name(config)}=; Expires=Thu, 01 Jan 1970 00:00:00 GMT; Max-Age=0; HttpOnly; SameSite=Strict; Path=/"


def build_status_payload(config: dict[str, Any]) -> dict[str, Any]:
    roles = ((config.get("node") or {}).get("roles") or {})
    worker = config.get("worker") or {}
    manager = config.get("manager") or {}
    status = queue_status(config)
    status["locks"] = lock_status(config)
    production = config.get("production") or {}
    return {
        "generated_at": utc_now(),
        "node_id": sanitize_node_id((config.get("node") or {}).get("id")),
        "roles": {
            "web_ui": bool(roles.get("web_ui", False)),
            "worker": bool(roles.get("worker", False)),
            "manager": bool(roles.get("manager", False)),
        },
        "services": {
            "worker": {
                "role_enabled": bool(roles.get("worker", False)),
                "configured_enabled": bool(worker.get("enabled", False)),
                "run_continuously": bool(worker.get("run_continuously", False)),
            },
            "manager": {
                "role_enabled": bool(roles.get("manager", False)),
                "configured_enabled": bool(manager.get("enabled", False)),
                "run_continuously": bool(manager.get("run_continuously", False)),
            },
            "production": {
                "role_enabled": bool(roles.get("manager", False)),
                "configured_enabled": bool(production.get("enabled", False)),
                "run_continuously": bool(production.get("enabled", False)),
            },
        },
        "active_profile": config.get("active_profile"),
        "status": status,
        "production": build_production_payload(config),
    }


def build_production_payload(config: dict[str, Any]) -> dict[str, Any]:
    root = init_state(config)
    node = local_node_id(config)
    status_path = root / "production" / f"{node}.json"
    status = read_json(status_path) if status_path.exists() else {}
    production = config.get("production") or {}
    backpressure = production.get("backpressure") or {}
    queue = queue_status(config)
    return {
        "generated_at": utc_now(),
        "node_id": node,
        "production_enabled": bool(production.get("enabled", False)),
        "control": read_node_control(config, node),
        "status": status,
        "ready_outputs_total_size_gb": queue.get("ready_outputs_total_size_gb", 0),
        "ready_outputs_dir_count": queue.get("ready_outputs_dir_count", 0),
        "ready_outputs_limit_gb": float(backpressure.get("max_ready_outputs_gb", 20)),
        "ready_outputs_backpressure_active": float(queue.get("ready_outputs_total_size_gb") or 0) >= float(backpressure.get("max_ready_outputs_gb", 20)),
    }


def build_workers_payload(config: dict[str, Any]) -> dict[str, Any]:
    status = queue_status(config)
    return {
        "generated_at": utc_now(),
        "node_id": sanitize_node_id((config.get("node") or {}).get("id")),
        "workers": status.get("worker_heartbeats") or [],
        "worker_summary": status.get("worker_summary") or {},
        "managers": status.get("manager_heartbeats") or [],
        "manager_summary": status.get("manager_summary") or {},
    }


def build_jobs_payload(config: dict[str, Any]) -> dict[str, Any]:
    jobs_by_state: dict[str, list[dict[str, Any]]] = {}
    for state in JOB_STATES:
        jobs_by_state[state] = [read_json(path) for path in list_state_files(config, state)]
    return {
        "generated_at": utc_now(),
        "node_id": sanitize_node_id((config.get("node") or {}).get("id")),
        "states": {state: len(jobs) for state, jobs in jobs_by_state.items()},
        "jobs": jobs_by_state,
    }


def read_log_entries(paths: list[Path], limit: int = 20) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in sorted(paths, key=lambda candidate: candidate.stat().st_mtime, reverse=True)[:limit]:
        payload = read_json(path)
        entries.append(
            {
                "path": str(path),
                "file_name": path.name,
                "modified_at": datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).astimezone().isoformat(timespec="seconds"),
                "payload": payload,
            }
        )
    return entries


def build_logs_payload(config: dict[str, Any], limit: int = 20) -> dict[str, Any]:
    root = init_state(config)
    manager_logs = read_log_entries(list((root / "logs" / "manager").glob("*.json")), limit=limit)
    worker_bundle_logs = read_log_entries(list((root / "ready_outputs").glob("*/worker_log.json")), limit=limit)
    worker_runtime_logs = read_log_entries(list((root / "logs" / "workers").glob("*.json")), limit=limit)
    job_logs = read_log_entries(list((root / "logs" / "jobs").glob("*.json")), limit=limit)
    return {
        "generated_at": utc_now(),
        "node_id": local_node_id(config),
        "manager_logs": manager_logs,
        "worker_bundle_logs": worker_bundle_logs,
        "worker_runtime_logs": worker_runtime_logs,
        "job_logs": job_logs,
        "counts": {
            "manager_logs": len(manager_logs),
            "worker_bundle_logs": len(worker_bundle_logs),
            "worker_runtime_logs": len(worker_runtime_logs),
            "job_logs": len(job_logs),
        },
    }


def build_locks_payload(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": utc_now(),
        "node_id": local_node_id(config),
        "locks": lock_status(config),
    }


def local_node_id(config: dict[str, Any]) -> str:
    return sanitize_node_id((config.get("node") or {}).get("id"))


def build_local_worker_control_payload(config: dict[str, Any]) -> dict[str, Any]:
    node = local_node_id(config)
    return {
        "generated_at": utc_now(),
        "node_id": node,
        "worker_enabled": bool((config.get("worker") or {}).get("enabled", False)),
        "schedule": worker_schedule_check(config),
        "control": read_node_control(config, node),
    }


def build_local_manager_control_payload(config: dict[str, Any]) -> dict[str, Any]:
    node = local_node_id(config)
    return {
        "generated_at": utc_now(),
        "node_id": node,
        "manager_enabled": bool((config.get("manager") or {}).get("enabled", False)),
        "control": read_node_control(config, node),
    }


def build_global_control_payload(config: dict[str, Any]) -> dict[str, Any]:
    status = queue_status(config)
    return {
        "generated_at": utc_now(),
        "node_id": local_node_id(config),
        "global_control": status.get("global_control") or {},
    }


def worker_schedule_config(config: dict[str, Any]) -> dict[str, Any]:
    worker = config.get("worker") or {}
    return worker.get("schedule") or {}


def build_worker_schedule_payload(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": utc_now(),
        "node_id": local_node_id(config),
        "worker_enabled": bool((config.get("worker") or {}).get("enabled", False)),
        "schedule": worker_schedule_check(config),
    }


def build_manager_settings_payload(config: dict[str, Any]) -> dict[str, Any]:
    roles = ((config.get("node") or {}).get("roles") or {})
    manager = config.get("manager") or {}
    jellyfin = config.get("jellyfin") or {}
    api_key = str(jellyfin.get("api_key") or "")
    return {
        "generated_at": utc_now(),
        "node_id": local_node_id(config),
        "manager_role_enabled": bool(roles.get("manager", False)),
        "manager": {
            "enabled": bool(manager.get("enabled", False)),
            "run_continuously": bool(manager.get("run_continuously", False)),
            "execute": bool(manager.get("execute", False)),
            "require_successful_jellyfin_refresh": bool(manager.get("require_successful_jellyfin_refresh", False)),
        },
        "jellyfin": {
            "enabled": bool(jellyfin.get("enabled", False)),
            "server_url": str(jellyfin.get("server_url") or ""),
            "timeout_seconds": int(jellyfin.get("timeout_seconds", 30)),
            "api_key_configured": bool(api_key),
            "api_key_masked": "********" if api_key else "",
        },
    }


def config_source_path(config: dict[str, Any]) -> Path:
    configured = config.get("__config_path")
    if not configured:
        raise ValueError("config_path_unavailable")
    return Path(str(configured))


def parse_request_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def normalize_worker_schedule_payload(payload: dict[str, Any]) -> dict[str, Any]:
    start_text = str(payload.get("start") or "00:00")
    end_text = str(payload.get("end") or "23:59")
    parse_hhmm(start_text)
    parse_hhmm(end_text)
    outside_window_behavior = str(payload.get("outside_window_behavior") or "finish_current_do_not_start_new")
    allowed_behaviors = {"finish_current_do_not_start_new", "stop_after_current", "hard_stop"}
    if outside_window_behavior not in allowed_behaviors:
        raise ValueError("invalid_outside_window_behavior")
    return {
        "enabled": parse_request_bool(payload.get("enabled", False)),
        "start": start_text,
        "end": end_text,
        "outside_window_behavior": outside_window_behavior,
    }


def normalize_manager_settings_payload(payload: dict[str, Any], existing_jellyfin: dict[str, Any]) -> dict[str, Any]:
    timeout_seconds = int(payload.get("timeout_seconds") or existing_jellyfin.get("timeout_seconds", 30))
    if timeout_seconds <= 0:
        raise ValueError("invalid_jellyfin_timeout_seconds")
    api_key = str(payload.get("api_key") or "").strip() or str(existing_jellyfin.get("api_key") or "")
    return {
        "manager": {
            "enabled": parse_request_bool(payload.get("enabled", False)),
            "run_continuously": parse_request_bool(payload.get("run_continuously", False)),
            "execute": parse_request_bool(payload.get("execute", False)),
            "require_successful_jellyfin_refresh": parse_request_bool(payload.get("require_successful_jellyfin_refresh", False)),
        },
        "jellyfin": {
            "enabled": parse_request_bool(payload.get("jellyfin_enabled", False)),
            "server_url": str(payload.get("server_url") or "").strip(),
            "timeout_seconds": timeout_seconds,
            "api_key": api_key,
        },
    }


def persist_worker_schedule(config: dict[str, Any], schedule: dict[str, Any]) -> Path:
    path = config_source_path(config)
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    selected_profile = config.get("__selected_profile")
    if selected_profile:
        profiles = raw_config.setdefault("profiles", {})
        profile_config = profiles.setdefault(str(selected_profile), {})
        worker = profile_config.setdefault("worker", {})
    else:
        worker = raw_config.setdefault("worker", {})
    worker["schedule"] = dict(schedule)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(raw_config, handle, sort_keys=False, allow_unicode=False)
    runtime_worker = config.setdefault("worker", {})
    runtime_worker["schedule"] = dict(schedule)
    return path


def persist_worker_enabled(config: dict[str, Any], enabled: bool) -> Path:
    path = config_source_path(config)
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    selected_profile = config.get("__selected_profile")
    if selected_profile:
        profiles = raw_config.setdefault("profiles", {})
        profile_config = profiles.setdefault(str(selected_profile), {})
        worker = profile_config.setdefault("worker", {})
    else:
        worker = raw_config.setdefault("worker", {})
    worker["enabled"] = bool(enabled)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(raw_config, handle, sort_keys=False, allow_unicode=False)
    runtime_worker = config.setdefault("worker", {})
    runtime_worker["enabled"] = bool(enabled)
    return path


def persist_manager_enabled(config: dict[str, Any], enabled: bool) -> Path:
    path = config_source_path(config)
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    selected_profile = config.get("__selected_profile")
    if selected_profile:
        profiles = raw_config.setdefault("profiles", {})
        profile_config = profiles.setdefault(str(selected_profile), {})
        manager = profile_config.setdefault("manager", {})
    else:
        manager = raw_config.setdefault("manager", {})
    manager["enabled"] = bool(enabled)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(raw_config, handle, sort_keys=False, allow_unicode=False)
    runtime_manager = config.setdefault("manager", {})
    runtime_manager["enabled"] = bool(enabled)
    return path


def persist_production_enabled(config: dict[str, Any], enabled: bool) -> Path:
    path = config_source_path(config)
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    selected_profile = config.get("__selected_profile")
    if selected_profile:
        profiles = raw_config.setdefault("profiles", {})
        profile_config = profiles.setdefault(str(selected_profile), {})
        production = profile_config.setdefault("production", {})
    else:
        production = raw_config.setdefault("production", {})
    production["enabled"] = bool(enabled)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(raw_config, handle, sort_keys=False, allow_unicode=False)
    runtime_production = config.setdefault("production", {})
    runtime_production["enabled"] = bool(enabled)
    return path


def persist_manager_settings(config: dict[str, Any], manager_settings: dict[str, Any], jellyfin_settings: dict[str, Any]) -> Path:
    path = config_source_path(config)
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    selected_profile = config.get("__selected_profile")
    if selected_profile:
        profiles = raw_config.setdefault("profiles", {})
        profile_config = profiles.setdefault(str(selected_profile), {})
        manager = profile_config.setdefault("manager", {})
        jellyfin = profile_config.setdefault("jellyfin", {})
    else:
        manager = raw_config.setdefault("manager", {})
        jellyfin = raw_config.setdefault("jellyfin", {})
    manager.update(manager_settings)
    jellyfin.update(jellyfin_settings)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(raw_config, handle, sort_keys=False, allow_unicode=False)
    runtime_manager = config.setdefault("manager", {})
    runtime_manager.update(manager_settings)
    runtime_jellyfin = config.setdefault("jellyfin", {})
    runtime_jellyfin.update(jellyfin_settings)
    return path


def build_dashboard_html(config: dict[str, Any]) -> str:
    title = f"Autoripper Node {sanitize_node_id((config.get('node') or {}).get('id'))}"
    roles = ((config.get("node") or {}).get("roles") or {})
    worker = config.get("worker") or {}
    manager_controls = ""
    production_panel = ""
    manager_settings_section = ""
    maintenance_controls = '<button class="btn-secondary" onclick="postAction(\'/api/maintenance/recover-stale-jobs\')">Recover Stale Jobs</button><button class="btn-secondary" onclick="confirmAction(\'This will release stale shared lock slots whose heartbeat has expired. Continue?\', \'/api/maintenance/recover-stale-locks\')">Recover Stale Locks</button><button class="btn-warning" onclick="confirmAction(\'This will move interrupted jobs back into queue for another claim attempt. Continue?\', \'/api/maintenance/requeue-interrupted-jobs\')">Requeue Interrupted Jobs</button>'
    auth_controls = ""
    if bool(roles.get("manager", False)):
        manager_controls = '<button class="btn-secondary" onclick="postAction(\'/api/manager/start\')">Start Manager</button><button class="btn-secondary" onclick="postAction(\'/api/manager/pause\')">Pause Manager</button><button class="btn-primary" onclick="postAction(\'/api/manager/finalize-now\')">Finalize Pending Now</button><button class="btn-secondary" onclick="postAction(\'/api/jellyfin/full-scan\')">Trigger Jellyfin Full Scan</button><button class="btn-primary" onclick="postAction(\'/api/manager/stop-after-current\')">Manager Stop After Current</button>'
        production_panel = '''
        <section class="panel span-12" id="production-panel">
            <div class="panel-header">
                <div>
                    <span class="eyebrow">Media server production</span>
                    <h2>Production Mode</h2>
                </div>
            </div>
            <div class="controls">
                <button class="btn-primary" onclick="postAction('/api/production/start')">Start Production</button>
                <button class="btn-secondary" onclick="postAction('/api/production/pause')">Pause Production</button>
                <button class="btn-warning" onclick="postAction('/api/production/maintenance')">Maintenance Mode</button>
                <button class="btn-primary" onclick="postAction('/api/production/stop-after-current')">Stop After Current</button>
                <button class="btn-secondary" onclick="postAction('/api/production/run-tick-now')">Run Tick Now</button>
                <button class="btn-secondary" onclick="postAction('/api/production/enqueue-now')">Enqueue Now</button>
                <button class="btn-primary" onclick="postAction('/api/manager/finalize-now')">Finalize Pending Now</button>
            </div>
            <div class="cards" id="production-cards" style="margin-top: 14px;"></div>
            <div class="summary-list" id="production-summary" style="margin-top: 14px;"></div>
        </section>'''
        manager_settings_section = '''
        <section class="panel span-12">
            <div class="panel-header">
                <div>
                    <span class="eyebrow">Media server only</span>
                    <h2>Manager Settings</h2>
                </div>
                <button class="btn-primary" onclick="saveManagerSettings()">Save Manager Settings</button>
            </div>
            <div class="schedule-meta" id="manager-meta-cards">
                <div class="card"><span class="card-label">Manager Status</span><div class="card-value">Loading...</div></div>
            </div>
            <div class="summary-list" id="manager-settings-summary" style="margin-bottom: 14px;"></div>
            <div class="form-grid">
                <label class="field check-field"><input id="manager-enabled" type="checkbox"><span>Manager Enabled</span></label>
                <label class="field check-field"><input id="manager-run-continuously" type="checkbox"><span>Run Continuously</span></label>
                <label class="field check-field"><input id="manager-execute" type="checkbox"><span>Execute Finalization</span></label>
                <label class="field check-field"><input id="manager-require-jellyfin" type="checkbox"><span>Require Successful Jellyfin Refresh</span></label>
                <label class="field check-field"><input id="manager-jellyfin-enabled" type="checkbox"><span>Jellyfin Enabled</span></label>
                <label class="field"><span>Jellyfin Server URL</span><input id="manager-jellyfin-server-url" type="url" placeholder="http://127.0.0.1:8096"></label>
                <label class="field"><span>Jellyfin API Key</span><input id="manager-jellyfin-api-key" type="password" placeholder="Leave blank to keep current key"></label>
                <label class="field"><span>Jellyfin Timeout Seconds</span><input id="manager-jellyfin-timeout" type="number" min="1" step="1" value="30"></label>
            </div>
        </section>'''
    if web_ui_auth_enabled(config):
        auth_controls = '<form method="post" action="/logout" class="logout-form"><button class="btn-secondary" type="submit">Logout</button></form>'
    worker_controls = '<button class="btn-secondary" onclick="postAction(\'/api/worker/start\')">Start Worker</button><button class="btn-secondary" onclick="postAction(\'/api/worker/pause\')">Pause Worker</button><button class="btn-primary" onclick="postAction(\'/api/worker/stop-after-current\')">Stop After Current</button>'
    danger_controls = '<button class="btn-danger" onclick="confirmAction(\'This will terminate the current local ffmpeg process, delete partial local output, and move the job to interrupted. The original media file will not be touched. Continue?\', \'/api/worker/hard-stop\')">Worker Hard Stop</button>'
    worker_controls_blocked = not bool(roles.get("worker", False)) or (bool(roles.get("manager", False)) and not bool(worker.get("enabled", False)))
    if worker_controls_blocked:
        worker_controls = '<span class="muted">Worker controls disabled by this node role/config.</span>'
        danger_controls = '<span class="muted">Worker hard stop disabled by this node role/config.</span>'
    return f"""<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>{title}</title>
  <style>
        :root {{ color-scheme: dark; --bg: #0d1412; --panel: #17211f; --panel-strong: #22332f; --text: #eef7f2; --muted: #9eb2aa; --accent: #8dd8bd; --accent-strong: #20b486; --ok: #34d399; --warn: #f59e0b; --danger: #f87171; --line: rgba(141, 216, 189, 0.2); }}
    body {{ margin: 0; font-family: "Aptos", "Segoe UI", sans-serif; background: radial-gradient(circle at 18% 0%, rgba(32, 180, 134, 0.18), transparent 34%), linear-gradient(155deg, #08100e, #14211d 52%, #0d1816); color: var(--text); }}
    main {{ max-width: 1180px; margin: 0 auto; padding: 34px 20px 54px; }}
    h1 {{ font-size: clamp(30px, 5vw, 48px); line-height: 1; margin: 0 0 10px; letter-spacing: -0.04em; }}
        h2 {{ margin: 0; font-size: 18px; letter-spacing: -0.01em; }}
    p {{ color: var(--muted); margin: 0 0 20px; }}
        .hero {{ display:flex; justify-content:space-between; align-items:flex-start; gap:20px; margin-bottom: 22px; }}
        .hero p {{ max-width: 820px; }}
        .panel {{ background: rgba(23, 33, 31, 0.92); border: 1px solid var(--line); border-radius: 20px; padding: 20px; box-shadow: 0 24px 70px rgba(0, 0, 0, 0.28); }}
        .panel-header {{ display:flex; align-items:flex-start; justify-content:space-between; gap:14px; margin-bottom: 14px; }}
        .eyebrow {{ display:block; color: var(--accent); font-size: 11px; text-transform: uppercase; letter-spacing: 0.14em; margin-bottom: 5px; }}
        .controls {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 0; }}
        .command-grid {{ display:grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; }}
        .command-group {{ border: 1px solid rgba(255,255,255,0.08); border-radius: 16px; padding: 14px; background: rgba(255,255,255,0.035); }}
        .command-group h3 {{ margin: 0 0 4px; font-size: 14px; }}
        .command-group p {{ font-size: 12px; line-height: 1.45; margin-bottom: 12px; }}
        .danger-zone {{ border-color: rgba(248, 113, 113, 0.35); background: rgba(248, 113, 113, 0.08); }}
        .layout {{ display: grid; grid-template-columns: repeat(12, minmax(0, 1fr)); gap: 18px; }}
        .span-12 {{ grid-column: span 12; }}
        .span-6 {{ grid-column: span 6; }}
        .span-4 {{ grid-column: span 4; }}
        .cards {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; }}
        .card {{ background: linear-gradient(180deg, rgba(34, 51, 47, 0.96), rgba(17, 27, 24, 0.96)); border:1px solid rgba(255,255,255,0.06); border-radius: 16px; padding: 14px; }}
        .card-label {{ display:block; color: var(--muted); font-size: 12px; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.08em; }}
        .card-value {{ font-size: 24px; font-weight: 700; }}
        .muted {{ color: var(--muted); }}
        .ok {{ color: var(--ok); }}
        .warn {{ color: var(--warn); }}
        .danger {{ color: var(--danger); }}
        .summary-list, .log-list {{ display:grid; gap: 10px; }}
        .summary-row, .log-row {{ display:flex; justify-content: space-between; gap: 12px; border-bottom: 1px solid rgba(255,255,255,0.06); padding-bottom: 8px; }}
        .summary-row:last-child, .log-row:last-child {{ border-bottom: 0; padding-bottom: 0; }}
        .mono {{ font-family: inherit; }}
        details {{ margin-top: 14px; }}
        summary {{ cursor: pointer; color: var(--accent); }}
        .log-toolbar {{ display:flex; flex-wrap:wrap; gap: 10px; margin: 14px 0; }}
        .log-toolbar label {{ display:grid; gap: 6px; min-width: 180px; color: var(--muted); }}
        input, select {{ width: 100%; box-sizing: border-box; border-radius: 12px; border: 1px solid rgba(255,255,255,0.12); background:#0f1916; color:var(--text); padding:10px 12px; font:inherit; }}
        input[type="checkbox"] {{ width: 18px; height: 18px; accent-color: var(--accent-strong); }}
        .form-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(210px, 1fr)); gap: 12px; }}
        .field {{ display:grid; gap: 7px; color: var(--muted); }}
        .field span {{ font-size: 13px; }}
        .check-field {{ grid-template-columns: 20px minmax(0, 1fr); align-items:center; min-height: 42px; border: 1px solid rgba(255,255,255,0.08); border-radius: 14px; padding: 10px 12px; background: rgba(255,255,255,0.03); color: var(--text); }}
        .log-viewer {{ display:grid; grid-template-columns: minmax(0, 1.1fr) minmax(0, 0.9fr); gap: 14px; }}
        .log-entry-list {{ display:grid; gap: 8px; max-height: 420px; overflow:auto; }}
        .log-entry {{ width:100%; text-align:left; background: rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 12px; color: var(--text); }}
        .log-entry.selected {{ border-color: rgba(125, 211, 252, 0.55); background: rgba(36, 50, 68, 0.95); }}
        .log-entry-header {{ display:flex; justify-content:space-between; gap: 12px; margin-bottom: 6px; }}
        .log-entry-title {{ font-weight: 700; }}
        .log-entry-meta {{ color: var(--muted); font-size: 12px; }}
        .log-detail {{ border:1px solid rgba(255,255,255,0.08); border-radius: 12px; background: rgba(255,255,255,0.03); padding: 14px; min-height: 220px; }}
        .log-detail-grid {{ display:grid; gap: 8px; margin-bottom: 14px; }}
        .heartbeat-list {{ display:grid; gap: 8px; margin-top: 14px; max-height: 250px; overflow:auto; }}
        .heartbeat-entry {{ width:100%; text-align:left; background: rgba(255,255,255,0.03); border:1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 12px; color: var(--text); }}
        .heartbeat-entry.selected {{ border-color: rgba(125, 211, 252, 0.55); background: rgba(36, 50, 68, 0.95); }}
        .heartbeat-detail {{ margin-top: 14px; border:1px solid rgba(255,255,255,0.08); border-radius: 12px; background: rgba(255,255,255,0.03); padding: 14px; min-height: 170px; }}
        .heartbeat-grid {{ display:grid; gap: 8px; margin-bottom: 12px; }}
        .action-banner {{ border-radius: 12px; padding: 12px 14px; border: 1px solid rgba(255,255,255,0.08); background: rgba(255,255,255,0.04); }}
        .action-banner.ok {{ border-color: rgba(52, 211, 153, 0.35); background: rgba(52, 211, 153, 0.12); }}
        .action-banner.warn {{ border-color: rgba(245, 158, 11, 0.35); background: rgba(245, 158, 11, 0.12); }}
        .action-banner.danger {{ border-color: rgba(248, 113, 113, 0.35); background: rgba(248, 113, 113, 0.12); }}
        .schedule-meta {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-bottom: 14px; }}
        .schedule-warning {{ margin-top: 12px; border-radius: 12px; padding: 12px 14px; border: 1px solid rgba(245, 158, 11, 0.35); background: rgba(245, 158, 11, 0.12); }}
        button {{ border: 0; border-radius: 999px; padding: 10px 14px; font: inherit; font-weight: 650; cursor: pointer; transition: transform 120ms ease, filter 120ms ease; }}
        button:hover {{ transform: translateY(-1px); filter: brightness(1.06); }}
        .btn-primary {{ background: #1f9f78; color: #ecfff8; }}
        .btn-secondary {{ background: #2d413c; color: #dff4ec; }}
        .btn-warning, button.warning {{ background: #b45309; color: #fff7ed; }}
        .btn-danger {{ background: #b91c1c; color: #fff1f2; }}
        .logout-form {{ display:inline; }}
    a {{ color: var(--accent); }}
    pre {{ white-space: pre-wrap; word-break: break-word; margin: 0; font-size: 13px; line-height: 1.5; }}
        @media (max-width: 980px) {{ .command-grid {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
        @media (max-width: 820px) {{ .span-6, .span-4 {{ grid-column: span 12; }} .log-viewer {{ grid-template-columns: 1fr; }} main {{ padding-inline: 14px; }} .hero, .panel-header {{ flex-direction: column; }} .command-grid {{ grid-template-columns: 1fr; }} }}
  </style>
</head>
<body>
  <main>
    <div class="hero">
        <div>
            <span class="eyebrow">Autonomous distributed node</span>
            <h1>{title}</h1>
            <p>Local control surface for this node. Raw JSON is available at <a href="/api/status">/api/status</a>, <a href="/api/workers">/api/workers</a>, <a href="/api/jobs">/api/jobs</a>, <a href="/api/logs">/api/logs</a>, and <a href="/api/locks">/api/locks</a>.</p>
        </div>
        <div>{auth_controls}</div>
    </div>
    <div class=\"layout\">
        <section class="panel span-12">
            <div class="panel-header">
                <div>
                    <span class="eyebrow">Command center</span>
                    <h2>Local Controls</h2>
                </div>
            </div>
            <div class="command-grid">
                <div class="command-group">
                    <h3>Worker</h3>
                    <p>Start, pause, or let the local worker finish its current encode cleanly.</p>
                    <div class="controls">
                        {worker_controls}
                    </div>
                </div>
                <div class="command-group">
                    <h3>Global Queue</h3>
                    <p>Use for shared queue brakes. These affect new claims across the shared state.</p>
                    <div class="controls">
                        <button class="btn-warning" onclick="postAction('/api/global/pause-queue')">Pause Global Queue</button>
                        <button class="btn-warning" onclick="postAction('/api/global/maintenance')">Enter Maintenance Mode</button>
                        <button class="btn-secondary" onclick="postAction('/api/global/resume-queue')">Resume Global Queue</button>
                    </div>
                </div>
                <div class="command-group">
                    <h3>Recovery</h3>
                    <p>Operator maintenance for stale jobs, stale locks, and interrupted work.</p>
                    <div class="controls">{maintenance_controls}</div>
                </div>
                <div class="command-group danger-zone">
                    <h3>Danger Zone</h3>
                    <p>Hard stop terminates local ffmpeg and moves the current job to interrupted.</p>
                    <div class="controls">
                        {danger_controls}
                    </div>
                </div>
            </div>
        </section>
        <section class="panel span-12">
            <h2>Action Status</h2>
            <div class="action-banner" id="action-status">No recent control action.</div>
        </section>
        <section class="panel span-12" id="manager-command-panel" style="display: {'block' if bool(roles.get('manager', False)) else 'none'};">
            <div class="panel-header">
                <div>
                    <span class="eyebrow">Finalizer authority</span>
                    <h2>Manager Controls</h2>
                </div>
            </div>
            <p>Visible only on manager-role nodes. These actions handle finalization and Jellyfin refresh from the media server side.</p>
            <div class="controls">{manager_controls}</div>
        </section>
        {production_panel}
        <section class=\"panel span-12\">
            <h2>Node Overview</h2>
            <div class=\"cards\" id=\"overview-cards\">
                <div class=\"card\"><span class=\"card-label\">Node</span><div class=\"card-value\">Loading...</div></div>
            </div>
            <div class=\"summary-list\" id=\"service-summary\" style=\"margin-top: 14px;\"></div>
        </section>
        <section class=\"panel span-6\">
            <h2>Global Queue</h2>
            <div class=\"cards\" id=\"queue-cards\"></div>
            <div class=\"summary-list\" id=\"queue-summary\" style=\"margin-top: 14px;\"></div>
        </section>
        <section class=\"panel span-6\">
            <h2>Workers</h2>
            <div class=\"cards\" id=\"worker-cards\"></div>
            <div class=\"summary-list\" id=\"worker-summary\" style=\"margin-top: 14px;\"></div>
            <div class=\"heartbeat-list\" id=\"worker-heartbeats\">Loading worker heartbeats...</div>
            <div class=\"heartbeat-detail\" id=\"worker-heartbeat-detail\"><strong>Worker Heartbeat Detail</strong><div class=\"muted\">Loading worker details...</div></div>
        </section>
        <section class=\"panel span-6\">
            <h2>Managers</h2>
            <div class=\"cards\" id=\"manager-cards\"></div>
            <div class=\"summary-list\" id=\"manager-summary\" style=\"margin-top: 14px;\"></div>
            <div class=\"heartbeat-list\" id=\"manager-heartbeats\">Loading manager heartbeats...</div>
            <div class=\"heartbeat-detail\" id=\"manager-heartbeat-detail\"><strong>Manager Heartbeat Detail</strong><div class=\"muted\">Loading manager details...</div></div>
        </section>
        <section class=\"panel span-6\">
            <h2>Shared Locks</h2>
            <div class=\"cards\" id=\"lock-cards\"></div>
            <div class=\"summary-list\" id=\"lock-summary\" style=\"margin-top: 14px;\"></div>
            <div class=\"heartbeat-detail\" id=\"lock-detail\"><strong>Shared Lock Detail</strong><div class=\"muted\">Loading lock details...</div></div>
        </section>
        <section class=\"panel span-6\">
            <h2>Logs Viewer</h2>
            <div class=\"cards\" id=\"log-count-cards\"></div>
            <div class=\"log-toolbar\">
                <label>Filter Logs
                    <select id=\"log-source-filter\">
                        <option value=\"all\">All Sources</option>
                        <option value=\"manager_logs\">Manager Logs</option>
                        <option value=\"worker_bundle_logs\">Worker Bundle Logs</option>
                        <option value=\"worker_runtime_logs\">Worker Runtime Logs</option>
                        <option value=\"job_logs\">Job Logs</option>
                    </select>
                </label>
                <label>Search
                    <input id=\"log-search-filter\" type=\"search\" placeholder=\"job id, node, status\">
                </label>
            </div>
            <div class=\"log-viewer\">
                <div class=\"log-entry-list\" id=\"latest-logs\">Loading logs...</div>
                <div class=\"log-detail\" id=\"log-detail\"><div class=\"muted\">Loading log details...</div></div>
            </div>
        </section>
        <section class="panel span-12">
            <div class="panel-header">
                <div>
                    <span class="eyebrow">Local worker policy</span>
                    <h2>Local Worker Schedule</h2>
                </div>
                <button class="btn-primary" onclick="saveSchedule()">Save Schedule</button>
            </div>
            <div class="schedule-meta" id="schedule-meta-cards">
                <div class="card"><span class="card-label">Schedule Status</span><div class="card-value">Loading...</div></div>
            </div>
            <div class="summary-list" id="schedule-summary" style="margin-bottom: 14px;"></div>
            <div class="form-grid">
                <label class="field check-field"><input id="schedule-enabled" type="checkbox"><span>Schedule Enabled</span></label>
                <label class="field"><span>Start</span><input id="schedule-start" type="time" value="00:00"></label>
                <label class="field"><span>End</span><input id="schedule-end" type="time" value="23:59"></label>
                <label class="field"><span>Outside Window</span><select id="schedule-behavior"><option value="finish_current_do_not_start_new">Finish Current, Do Not Start New</option><option value="stop_after_current">Stop After Current</option><option value="hard_stop">Hard Stop</option></select></label>
            </div>
            <div id="schedule-warning"></div>
        </section>
        {manager_settings_section}
        <section class=\"panel span-12\">
            <h2>Raw Status</h2>
            <details>
              <summary>Show raw JSON payloads</summary>
              <pre id=\"payload\">Loading status...</pre>
              <pre id=\"logs\" style=\"margin-top: 14px;\">Loading logs...</pre>
            </details>
        </section>
    </div>
  </main>
  <script>
        const LOG_SOURCE_LABELS = {{
            manager_logs: 'Manager',
            worker_bundle_logs: 'Worker Bundle',
            worker_runtime_logs: 'Worker Runtime',
            job_logs: 'Job',
        }};
        let currentLogsPayload = null;
        let currentWorkersPayload = null;
        let allLogEntries = [];
        let selectedLogKey = '';
        let selectedWorkerNodeId = '';
        let selectedManagerNodeId = '';
        function actionMessageFromPayload(payload, fallbackText) {{
            if (!payload || typeof payload !== 'object') {{
                return fallbackText;
            }}
            return payload.error || payload.status || payload.reason || payload.node_id || fallbackText;
        }}
        function setActionStatus(kind, message) {{
            const element = document.getElementById('action-status');
            element.className = 'action-banner' + (kind ? ' ' + kind : '');
            element.textContent = message;
        }}
        function confirmAction(message, path) {{
            if (!window.confirm(message)) {{
                return;
            }}
            postAction(path);
        }}
        function escapeHtml(value) {{
            return String(value ?? '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
        }}
        function card(label, value, tone='') {{
            const toneClass = tone ? ' ' + tone : '';
            return '<div class="card"><span class="card-label">' + escapeHtml(label) + '</span><div class="card-value' + toneClass + '">' + escapeHtml(value) + '</div></div>';
        }}
        function summaryRows(entries) {{
            return entries.map(([label, value]) => '<div class="summary-row"><span class="muted">' + escapeHtml(label) + '</span><strong>' + escapeHtml(value) + '</strong></div>').join('');
        }}
        function flattenLogEntries(payload) {{
            const sources = ['manager_logs', 'worker_bundle_logs', 'worker_runtime_logs', 'job_logs'];
            const entries = [];
            for (const sourceKey of sources) {{
                const sourceEntries = payload[sourceKey] || [];
                for (let index = 0; index < sourceEntries.length; index += 1) {{
                    const entry = sourceEntries[index] || {{}};
                    const data = entry.payload || {{}};
                    entries.push({{
                        key: sourceKey + '|' + (entry.path || entry.file_name || String(index)),
                        sourceKey,
                        sourceLabel: LOG_SOURCE_LABELS[sourceKey] || sourceKey,
                        fileName: entry.file_name || 'unknown',
                        modifiedAt: entry.modified_at || 'n/a',
                        jobId: data.job_id || data.source_path || entry.file_name || 'unknown',
                        status: data.status || data.reason || 'unknown',
                        nodeId: data.node_id || 'n/a',
                        path: entry.path || '',
                        payload: data,
                    }});
                }}
            }}
            return entries.sort((left, right) => String(right.modifiedAt).localeCompare(String(left.modifiedAt)));
        }}
        function currentFilteredLogs() {{
            const sourceFilter = document.getElementById('log-source-filter').value;
            const searchText = document.getElementById('log-search-filter').value.trim().toLowerCase();
            return allLogEntries.filter((entry) => {{
                if (sourceFilter !== 'all' && entry.sourceKey !== sourceFilter) {{
                    return false;
                }}
                if (!searchText) {{
                    return true;
                }}
                const haystack = [entry.jobId, entry.status, entry.nodeId, entry.fileName, entry.path, JSON.stringify(entry.payload || {{}})].join(' ').toLowerCase();
                return haystack.includes(searchText);
            }});
        }}
        function renderLogDetail(entry) {{
            if (!entry) {{
                document.getElementById('log-detail').innerHTML = '<div class="muted">No log entry matches the current filters.</div>';
                return;
            }}
            document.getElementById('log-detail').innerHTML = [
                '<h2 style="margin-top:0;">Log Details</h2>',
                '<div class="log-detail-grid">',
                '<div><span class="muted">Source</span><div><strong>' + escapeHtml(entry.sourceLabel) + '</strong></div></div>',
                '<div><span class="muted">Job</span><div><strong>' + escapeHtml(entry.jobId) + '</strong></div></div>',
                '<div><span class="muted">Status</span><div><strong>' + escapeHtml(entry.status) + '</strong></div></div>',
                '<div><span class="muted">Node</span><div><strong>' + escapeHtml(entry.nodeId) + '</strong></div></div>',
                '<div><span class="muted">Updated</span><div><strong>' + escapeHtml(entry.modifiedAt) + '</strong></div></div>',
                '</div>',
                '<pre>' + escapeHtml(JSON.stringify(entry.payload || {{}}, null, 2)) + '</pre>',
            ].join('');
        }}
        function renderLogsViewer() {{
            const payload = currentLogsPayload || {{ counts: {{}}, manager_logs: [], worker_bundle_logs: [], worker_runtime_logs: [], job_logs: [] }};
            allLogEntries = flattenLogEntries(payload);
            document.getElementById('log-count-cards').innerHTML = [
                card('Manager', (payload.counts || {{}}).manager_logs || 0),
                card('Worker Bundle', (payload.counts || {{}}).worker_bundle_logs || 0),
                card('Worker Runtime', (payload.counts || {{}}).worker_runtime_logs || 0),
                card('Job', (payload.counts || {{}}).job_logs || 0),
            ].join('');
            const filtered = currentFilteredLogs();
            if (!filtered.some((entry) => entry.key === selectedLogKey)) {{
                selectedLogKey = filtered.length ? filtered[0].key : '';
            }}
            document.getElementById('latest-logs').innerHTML = filtered.length ? filtered.map((entry) => {{
                const selectedClass = entry.key === selectedLogKey ? ' selected' : '';
                return '<button type="button" class="log-entry' + selectedClass + '" data-log-key="' + escapeHtml(entry.key) + '"><div class="log-entry-header"><span class="log-entry-title">' + escapeHtml(entry.jobId) + '</span><span class="log-entry-meta">' + escapeHtml(entry.modifiedAt) + '</span></div><div class="muted">' + escapeHtml(entry.sourceLabel) + ' | ' + escapeHtml(entry.status) + ' | ' + escapeHtml(entry.nodeId) + '</div></button>';
            }}).join('') : '<div class="muted">No logs match the current filters.</div>';
            document.querySelectorAll('[data-log-key]').forEach((element) => {{
                element.addEventListener('click', () => {{
                    selectedLogKey = element.getAttribute('data-log-key') || '';
                    renderLogsViewer();
                }});
            }});
            renderLogDetail(filtered.find((entry) => entry.key === selectedLogKey) || null);
        }}
        function heartbeatStatusTone(summary) {{
            if (!summary) {{
                return 'warn';
            }}
            return summary.heartbeat_stale ? 'danger' : 'ok';
        }}
        function renderHeartbeatDetail(role, summary) {{
            const detailId = role === 'worker' ? 'worker-heartbeat-detail' : 'manager-heartbeat-detail';
            if (!summary) {{
                document.getElementById(detailId).innerHTML = '<div class="muted">No ' + role + ' heartbeat is available.</div>';
                return;
            }}
            const raw = summary.raw || {{}};
            document.getElementById(detailId).innerHTML = [
                '<h2 style="margin-top:0;">' + escapeHtml(role === 'worker' ? 'Worker Heartbeat Detail' : 'Manager Heartbeat Detail') + '</h2>',
                '<div class="heartbeat-grid">',
                '<div><span class="muted">Node</span><div><strong>' + escapeHtml(summary.node_id || 'unknown') + '</strong></div></div>',
                '<div><span class="muted">Host</span><div><strong>' + escapeHtml(summary.hostname || 'unknown') + '</strong></div></div>',
                '<div><span class="muted">State</span><div><strong>' + escapeHtml(summary.state || 'unknown') + '</strong></div></div>',
                '<div><span class="muted">Phase</span><div><strong>' + escapeHtml(summary.current_phase || 'unknown') + '</strong></div></div>',
                '<div><span class="muted">Current Job</span><div><strong>' + escapeHtml(summary.current_job_id || 'none') + '</strong></div></div>',
                '<div><span class="muted">Heartbeat Age</span><div><strong>' + escapeHtml(summary.heartbeat_age_seconds ?? 'n/a') + 's</strong></div></div>',
                '<div><span class="muted">Last Heartbeat</span><div><strong>' + escapeHtml(summary.last_heartbeat || 'n/a') + '</strong></div></div>',
                '</div>',
                '<pre>' + escapeHtml(JSON.stringify(raw, null, 2)) + '</pre>',
            ].join('');
        }}
        function renderHeartbeatViewer(role) {{
            const payload = currentWorkersPayload || {{ workers: [], managers: [] }};
            const summaries = role === 'worker' ? (payload.workers || []) : (payload.managers || []);
            const listId = role === 'worker' ? 'worker-heartbeats' : 'manager-heartbeats';
            if (!(role === 'worker' ? selectedWorkerNodeId : selectedManagerNodeId) && summaries.length) {{
                if (role === 'worker') {{
                    selectedWorkerNodeId = summaries[0].node_id || '';
                }} else {{
                    selectedManagerNodeId = summaries[0].node_id || '';
                }}
            }}
            const activeNodeId = role === 'worker' ? selectedWorkerNodeId : selectedManagerNodeId;
            document.getElementById(listId).innerHTML = summaries.length ? summaries.map((summary) => {{
                const selectedClass = summary.node_id === activeNodeId ? ' selected' : '';
                const tone = heartbeatStatusTone(summary);
                const toneText = tone === 'danger' ? 'Stale' : 'Healthy';
                return '<button type="button" class="heartbeat-entry' + selectedClass + '" data-heartbeat-role="' + role + '" data-heartbeat-node="' + escapeHtml(summary.node_id || '') + '"><div class="log-entry-header"><span class="log-entry-title">' + escapeHtml(summary.node_id || 'unknown') + '</span><span class="log-entry-meta ' + tone + '">' + toneText + '</span></div><div class="muted">' + escapeHtml(summary.state || 'unknown') + ' | ' + escapeHtml(summary.current_phase || 'unknown') + ' | job ' + escapeHtml(summary.current_job_id || 'none') + '</div></button>';
            }}).join('') : '<div class="muted">No ' + role + ' heartbeats available.</div>';
            document.querySelectorAll('[data-heartbeat-role="' + role + '"]').forEach((element) => {{
                element.addEventListener('click', () => {{
                    const nodeId = element.getAttribute('data-heartbeat-node') || '';
                    if (role === 'worker') {{
                        selectedWorkerNodeId = nodeId;
                    }} else {{
                        selectedManagerNodeId = nodeId;
                    }}
                    renderHeartbeatViewer(role);
                }});
            }});
            const selectedSummary = summaries.find((summary) => summary.node_id === (role === 'worker' ? selectedWorkerNodeId : selectedManagerNodeId)) || summaries[0] || null;
            renderHeartbeatDetail(role, selectedSummary);
        }}
        function renderScheduleStatus(schedule) {{
            const enabled = !!schedule.enabled;
            const allowed = !!schedule.allowed_to_claim;
            const behavior = schedule.outside_window_behavior || 'finish_current_do_not_start_new';
            let tone = 'ok';
            let statusText = 'Allowed';
            if (!enabled) {{
                statusText = 'Disabled';
                tone = 'warn';
            }} else if (!allowed) {{
                statusText = 'Blocked By Window';
                tone = behavior === 'hard_stop' ? 'danger' : 'warn';
            }}
            document.getElementById('schedule-meta-cards').innerHTML = [
                card('Schedule Status', statusText, tone),
                card('Claims Allowed', allowed ? 'Yes' : 'No', allowed ? 'ok' : tone),
                card('Window', (schedule.start || '00:00') + ' - ' + (schedule.end || '23:59')),
                card('Outside Window Mode', behavior.replaceAll('_', ' '), behavior === 'hard_stop' ? 'danger' : 'warn'),
            ].join('');
            document.getElementById('schedule-summary').innerHTML = summaryRows([
                ['Enabled', enabled ? 'Yes' : 'No'],
                ['Checked At', schedule.checked_at || 'n/a'],
                ['Allowed To Claim', allowed ? 'Yes' : 'No'],
                ['Outside Window Behavior', behavior],
            ]);
            document.getElementById('schedule-warning').innerHTML = behavior === 'hard_stop'
                ? '<div class="schedule-warning"><strong>Hard Stop Warning</strong><div class="muted">At schedule end, the worker will terminate the current local ffmpeg process, delete partial local output, and move the job to interrupted. The original media file is not modified.</div></div>'
                : '';
        }}
        function renderLockStatus(lockPayload) {{
            const lockTypes = ['nas_read', 'nas_write', 'active_encode', 'finalizer'];
            let totalActive = 0;
            let totalStale = 0;
            for (const lockType of lockTypes) {{
                const entry = lockPayload[lockType] || {{}};
                totalActive += entry.active || 0;
                totalStale += entry.stale || 0;
            }}
            document.getElementById('lock-cards').innerHTML = [
                card('Active Locks', totalActive, totalActive ? 'warn' : 'ok'),
                card('Stale Locks', totalStale, totalStale ? 'danger' : 'ok'),
                card('NAS Read', ((lockPayload.nas_read || {{}}).active || 0) + ' / ' + ((lockPayload.nas_read || {{}}).limit || 0)),
                card('NAS Write', ((lockPayload.nas_write || {{}}).active || 0) + ' / ' + ((lockPayload.nas_write || {{}}).limit || 0)),
            ].join('');
            document.getElementById('lock-summary').innerHTML = summaryRows(lockTypes.map((lockType) => {{
                const entry = lockPayload[lockType] || {{}};
                return [lockType.replaceAll('_', ' '), 'active ' + (entry.active || 0) + ', stale ' + (entry.stale || 0) + ', limit ' + (entry.limit || 0)];
            }}));
            document.getElementById('lock-detail').innerHTML = [
                '<h2 style="margin-top:0;">Shared Lock Detail</h2>',
                '<pre>' + escapeHtml(JSON.stringify(lockPayload || {{}}, null, 2)) + '</pre>',
            ].join('');
        }}
        function localHeartbeat(payload, role) {{
            const status = payload.status || {{}};
            const nodeId = payload.node_id || '';
            const entries = role === 'worker' ? (status.worker_heartbeats || []) : (status.manager_heartbeats || []);
            return entries.find((entry) => entry.node_id === nodeId) || null;
        }}
        function serviceRuntimeCard(label, serviceConfig, heartbeat) {{
            if (!serviceConfig.role_enabled) {{
                return card(label, 'No Role', 'warn');
            }}
            if (!serviceConfig.configured_enabled) {{
                return card(label, 'Off', 'warn');
            }}
            if (!heartbeat) {{
                return card(label, serviceConfig.run_continuously ? 'Not Running' : 'Manual / Not Active', 'warn');
            }}
            if (heartbeat.heartbeat_stale) {{
                return card(label, 'Stale', 'danger');
            }}
            return card(label, heartbeat.state || 'Running', heartbeat.current_job_id ? 'ok' : 'ok');
        }}
        function renderStatusPanels(payload) {{
            const status = payload.status || {{}};
            const roles = payload.roles || {{}};
            const services = payload.services || {{ worker: {{}}, manager: {{}} }};
            const globalControl = status.global_control || {{}};
            const workerSummary = status.worker_summary || {{}};
            const managerSummary = status.manager_summary || {{}};
            const locks = status.locks || {{}};
            const states = status.states || {{}};
            const production = payload.production || {{}};
            const productionStatus = production.status || {{}};
            const queueState = globalControl.queue_state || 'unknown';
            const queueTone = queueState === 'running' ? 'ok' : (queueState === 'maintenance' ? 'danger' : 'warn');
            const localWorkerHeartbeat = localHeartbeat(payload, 'worker');
            const localManagerHeartbeat = localHeartbeat(payload, 'manager');
            document.getElementById('overview-cards').innerHTML = [
                card('Node', payload.node_id || 'unknown'),
                serviceRuntimeCard('Worker State', services.worker || {{}}, localWorkerHeartbeat),
                serviceRuntimeCard('Manager State', services.manager || {{}}, localManagerHeartbeat),
                card('Queue State', queueState, queueTone),
            ].join('');
            document.getElementById('service-summary').innerHTML = summaryRows([
                ['Worker Role Capability', roles.worker ? 'available on this node' : 'not available on this node'],
                ['Worker Config Enabled', (services.worker || {{}}).configured_enabled ? 'Yes' : 'No'],
                ['Worker Continuous Startup', (services.worker || {{}}).run_continuously ? 'Yes' : 'No'],
                ['Manager Role Capability', roles.manager ? 'available on this node' : 'not available on this node'],
                ['Manager Config Enabled', (services.manager || {{}}).configured_enabled ? 'Yes' : 'No'],
                ['Manager Continuous Startup', (services.manager || {{}}).run_continuously ? 'Yes' : 'No'],
            ]);
            document.getElementById('queue-cards').innerHTML = [
                card('Queued', states.queue || 0),
                card('Running', states.running || 0),
                card('Ready', states.ready_for_finalize || 0),
                card('Failed', (states.failed || 0) + (states.failed_finalize || 0), (states.failed || states.failed_finalize) ? 'danger' : 'ok'),
            ].join('');
            document.getElementById('queue-summary').innerHTML = summaryRows([
                ['Done', states.done || 0],
                ['Interrupted', states.interrupted || 0],
                ['Stale', states.stale || 0],
                ['Allow New Claims', globalControl.allow_new_claims ? 'Yes' : 'No'],
                ['Allow Finalizer', globalControl.allow_finalizer ? 'Yes' : 'No'],
            ]);
            document.getElementById('worker-cards').innerHTML = [
                card('Total Workers', workerSummary.total || 0),
                card('Healthy', workerSummary.healthy || 0, 'ok'),
                card('Stale', workerSummary.stale || 0, workerSummary.stale ? 'danger' : 'ok'),
                card('Busy', workerSummary.with_current_job || 0),
            ].join('');
            document.getElementById('worker-summary').innerHTML = summaryRows(Object.entries(workerSummary.state_counts || {{}}));
            document.getElementById('manager-cards').innerHTML = [
                card('Total Managers', managerSummary.total || 0),
                card('Healthy', managerSummary.healthy || 0, 'ok'),
                card('Stale', managerSummary.stale || 0, managerSummary.stale ? 'danger' : 'ok'),
                card('Busy', managerSummary.with_current_job || 0),
            ].join('');
            document.getElementById('manager-summary').innerHTML = summaryRows(Object.entries(managerSummary.state_counts || {{}}));
            renderLockStatus(locks);
            if (document.getElementById('production-cards')) {{
                const blockedReasons = productionStatus.blocked_reasons || [];
                const productionState = productionStatus.production_state || (production.production_enabled ? 'waiting' : 'disabled');
                const productionTone = productionState === 'running' || productionState === 'idle' ? 'ok' : (productionState === 'blocked' ? 'warn' : 'danger');
                document.getElementById('production-cards').innerHTML = [
                    card('Production State', productionState, productionTone),
                    card('Ready Outputs GB', production.ready_outputs_total_size_gb || 0, production.ready_outputs_backpressure_active ? 'danger' : 'ok'),
                    card('Ready Limit GB', production.ready_outputs_limit_gb || 0),
                    card('Last Enqueue', productionStatus.last_enqueue_count || 0),
                    card('Last Finalize', productionStatus.last_finalize_count || 0),
                ].join('');
                document.getElementById('production-summary').innerHTML = summaryRows([
                    ['Enabled', production.production_enabled ? 'Yes' : 'No'],
                    ['Phase', productionStatus.current_phase || 'n/a'],
                    ['Last Tick', productionStatus.last_tick_at || 'n/a'],
                    ['Ready Output Dirs', production.ready_outputs_dir_count || 0],
                    ['Blocked Reasons', blockedReasons.length ? blockedReasons.join('; ') : 'none'],
                ]);
            }}
        }}
        async function postAction(path) {{
            try {{
                const response = await fetch(path, {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify({{ updated_by: 'web-ui' }}) }});
                const payload = await response.json();
                document.getElementById('payload').textContent = JSON.stringify(payload, null, 2);
                if (!response.ok) {{
                    setActionStatus('danger', 'Action failed: ' + actionMessageFromPayload(payload, path));
                    return;
                }}
                setActionStatus('ok', 'Action completed: ' + actionMessageFromPayload(payload, path));
                await refreshStatus();
                await refreshWorkers();
                await refreshLogs();
            }} catch (error) {{
                setActionStatus('danger', 'Action failed: ' + (error && error.message ? error.message : 'network error'));
            }}
        }}
    async function refreshStatus() {{
      const response = await fetch('/api/status', {{ cache: 'no-store' }});
      const payload = await response.json();
      document.getElementById('payload').textContent = JSON.stringify(payload, null, 2);
            renderStatusPanels(payload);
    }}
        async function refreshSchedule() {{
            const response = await fetch('/api/settings/worker-schedule', {{ cache: 'no-store' }});
            const payload = await response.json();
            const schedule = payload.schedule || {{}};
            document.getElementById('schedule-enabled').checked = !!schedule.enabled;
            document.getElementById('schedule-start').value = schedule.start || '00:00';
            document.getElementById('schedule-end').value = schedule.end || '23:59';
            document.getElementById('schedule-behavior').value = schedule.outside_window_behavior || 'finish_current_do_not_start_new';
            renderScheduleStatus(schedule);
        }}
        function renderManagerSettings(payload) {{
            const manager = payload.manager || {{}};
            const jellyfin = payload.jellyfin || {{}};
            document.getElementById('manager-meta-cards').innerHTML = [
                card('Manager Enabled', manager.enabled ? 'Yes' : 'No', manager.enabled ? 'ok' : 'warn'),
                card('Run Continuously', manager.run_continuously ? 'Yes' : 'No', manager.run_continuously ? 'ok' : 'warn'),
                card('Execute', manager.execute ? 'Yes' : 'No', manager.execute ? 'danger' : 'warn'),
                card('Jellyfin', jellyfin.enabled ? 'Enabled' : 'Disabled', jellyfin.enabled ? 'ok' : 'warn'),
            ].join('');
            document.getElementById('manager-settings-summary').innerHTML = summaryRows([
                ['Require Successful Jellyfin Refresh', manager.require_successful_jellyfin_refresh ? 'Yes' : 'No'],
                ['Jellyfin Server', jellyfin.server_url || 'not configured'],
                ['Jellyfin API Key', jellyfin.api_key_configured ? 'Configured' : 'Not Configured'],
                ['Jellyfin Timeout Seconds', jellyfin.timeout_seconds || 30],
            ]);
        }}
        async function refreshManagerSettings() {{
            if (!document.getElementById('manager-enabled')) {{
                return;
            }}
            const response = await fetch('/api/settings/manager', {{ cache: 'no-store' }});
            const payload = await response.json();
            const manager = payload.manager || {{}};
            const jellyfin = payload.jellyfin || {{}};
            document.getElementById('manager-enabled').checked = !!manager.enabled;
            document.getElementById('manager-run-continuously').checked = !!manager.run_continuously;
            document.getElementById('manager-execute').checked = !!manager.execute;
            document.getElementById('manager-require-jellyfin').checked = !!manager.require_successful_jellyfin_refresh;
            document.getElementById('manager-jellyfin-enabled').checked = !!jellyfin.enabled;
            document.getElementById('manager-jellyfin-server-url').value = jellyfin.server_url || '';
            document.getElementById('manager-jellyfin-api-key').value = '';
            document.getElementById('manager-jellyfin-api-key').placeholder = jellyfin.api_key_configured ? 'Configured; leave blank to keep current key' : 'Paste API key';
            document.getElementById('manager-jellyfin-timeout').value = jellyfin.timeout_seconds || 30;
            renderManagerSettings(payload);
        }}
        async function saveSchedule() {{
            const body = {{
                enabled: document.getElementById('schedule-enabled').checked,
                start: document.getElementById('schedule-start').value,
                end: document.getElementById('schedule-end').value,
                outside_window_behavior: document.getElementById('schedule-behavior').value,
            }};
            try {{
                const response = await fetch('/api/settings/worker-schedule', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify(body) }});
                const payload = await response.json();
                document.getElementById('payload').textContent = JSON.stringify(payload, null, 2);
                if (!response.ok) {{
                    setActionStatus('danger', 'Schedule update failed: ' + actionMessageFromPayload(payload, '/api/settings/worker-schedule'));
                    return;
                }}
                setActionStatus('ok', 'Schedule saved: ' + actionMessageFromPayload(payload, 'worker schedule'));
                await refreshStatus();
                await refreshWorkers();
                await refreshSchedule();
            }} catch (error) {{
                setActionStatus('danger', 'Schedule update failed: ' + (error && error.message ? error.message : 'network error'));
            }}
        }}
        async function saveManagerSettings() {{
            if (!document.getElementById('manager-enabled')) {{
                return;
            }}
            const body = {{
                enabled: document.getElementById('manager-enabled').checked,
                run_continuously: document.getElementById('manager-run-continuously').checked,
                execute: document.getElementById('manager-execute').checked,
                require_successful_jellyfin_refresh: document.getElementById('manager-require-jellyfin').checked,
                jellyfin_enabled: document.getElementById('manager-jellyfin-enabled').checked,
                server_url: document.getElementById('manager-jellyfin-server-url').value,
                api_key: document.getElementById('manager-jellyfin-api-key').value,
                timeout_seconds: document.getElementById('manager-jellyfin-timeout').value,
            }};
            try {{
                const response = await fetch('/api/settings/manager', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify(body) }});
                const payload = await response.json();
                document.getElementById('payload').textContent = JSON.stringify(payload, null, 2);
                if (!response.ok) {{
                    setActionStatus('danger', 'Manager settings update failed: ' + actionMessageFromPayload(payload, '/api/settings/manager'));
                    return;
                }}
                setActionStatus('ok', 'Manager settings saved: ' + actionMessageFromPayload(payload, 'manager settings'));
                await refreshStatus();
                await refreshWorkers();
                await refreshManagerSettings();
            }} catch (error) {{
                setActionStatus('danger', 'Manager settings update failed: ' + (error && error.message ? error.message : 'network error'));
            }}
        }}
        async function refreshWorkers() {{
            try {{
                const response = await fetch('/api/workers', {{ cache: 'no-store' }});
                const payload = await response.json();
                currentWorkersPayload = payload;
                renderHeartbeatViewer('worker');
                renderHeartbeatViewer('manager');
            }} catch (error) {{
                setActionStatus('warn', 'Heartbeat refresh failed: ' + (error && error.message ? error.message : 'network error'));
            }}
        }}
        async function refreshLogs() {{
            try {{
                const response = await fetch('/api/logs', {{ cache: 'no-store' }});
                const payload = await response.json();
                currentLogsPayload = payload;
                document.getElementById('logs').textContent = JSON.stringify(payload, null, 2);
                renderLogsViewer();
            }} catch (error) {{
                setActionStatus('warn', 'Logs refresh failed: ' + (error && error.message ? error.message : 'network error'));
            }}
        }}
        document.getElementById('log-source-filter').addEventListener('change', renderLogsViewer);
        document.getElementById('log-search-filter').addEventListener('input', renderLogsViewer);
    refreshStatus();
        refreshWorkers();
        refreshSchedule();
        refreshManagerSettings();
        refreshLogs();
    setInterval(refreshStatus, 5000);
        setInterval(refreshWorkers, 5000);
        setInterval(refreshLogs, 5000);
  </script>
</body>
</html>
"""


def make_handler(config: dict[str, Any]) -> type[BaseHTTPRequestHandler]:
    def set_local_control_command(
        worker_command: str | None = None,
        manager_command: str | None = None,
        production_command: str | None = None,
        updated_by: str | None = None,
    ) -> None:
        node = local_node_id(config)
        current = read_node_control(config, node)
        set_node_control(
            config,
            node,
            worker_command=current.get("worker_command") if worker_command is None else worker_command,
            manager_command=current.get("manager_command") if manager_command is None else manager_command,
            production_command=current.get("production_command") if production_command is None else production_command,
            updated_by=updated_by,
        )

    class WebUiHandler(BaseHTTPRequestHandler):
        def _is_authenticated(self) -> bool:
            if not web_ui_auth_enabled(config):
                return True
            cookie_header = self.headers.get("Cookie") or ""
            cookie = SimpleCookie()
            cookie.load(cookie_header)
            morsel = cookie.get(web_ui_session_cookie_name(config))
            return is_valid_auth_token(config, morsel.value if morsel else None)

        def _write_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
            body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_html(self, html: str, status: int = HTTPStatus.OK) -> None:
            body = html.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _write_redirect(self, location: str, set_cookie: str | None = None) -> None:
            self.send_response(HTTPStatus.SEE_OTHER)
            self.send_header("Location", location)
            if set_cookie:
                self.send_header("Set-Cookie", set_cookie)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _read_json_body(self) -> dict[str, Any]:
            content_length = int(self.headers.get("Content-Length") or 0)
            if content_length <= 0:
                return {}
            body = self.rfile.read(content_length)
            if not body:
                return {}
            return json.loads(body.decode("utf-8"))

        def _read_form_body(self) -> dict[str, str]:
            content_length = int(self.headers.get("Content-Length") or 0)
            if content_length <= 0:
                return {}
            body = self.rfile.read(content_length).decode("utf-8")
            parsed = parse_qs(body, keep_blank_values=True)
            return {key: values[-1] for key, values in parsed.items() if values}

        def _write_auth_required(self, path: str) -> None:
            if path.startswith("/api/"):
                self._write_json({"error": "authentication_required", "node_id": local_node_id(config)}, status=HTTPStatus.UNAUTHORIZED)
                return
            self._write_redirect("/login")

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/healthz":
                self._write_json({"ok": True, "generated_at": utc_now()})
                return
            if path == "/login":
                if self._is_authenticated():
                    self._write_redirect("/")
                    return
                self._write_html(build_login_html(config))
                return
            if not self._is_authenticated():
                self._write_auth_required(path)
                return
            if path == "/api/status":
                self._write_json(build_status_payload(config))
                return
            if path == "/api/workers":
                self._write_json(build_workers_payload(config))
                return
            if path == "/api/jobs":
                self._write_json(build_jobs_payload(config))
                return
            if path == "/api/logs":
                self._write_json(build_logs_payload(config))
                return
            if path == "/api/locks":
                self._write_json(build_locks_payload(config))
                return
            if path == "/api/production":
                self._write_json(build_production_payload(config))
                return
            if path == "/api/settings/manager":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                self._write_json(build_manager_settings_payload(config))
                return
            if path == "/api/settings/worker-schedule":
                self._write_json(build_worker_schedule_payload(config))
                return
            if path == "/":
                self._write_html(build_dashboard_html(config))
                return
            self._write_json({"error": "not_found", "path": path}, status=HTTPStatus.NOT_FOUND)

        def do_POST(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/login":
                form = self._read_form_body()
                username = str(form.get("username") or "")
                password = str(form.get("password") or "")
                if username != web_ui_auth_username(config) or hash_web_ui_password(password) != str(web_ui_auth_settings(config).get("password_hash") or ""):
                    self._write_html(build_login_html(config, error_message="Invalid username or password."), status=HTTPStatus.UNAUTHORIZED)
                    return
                token = build_auth_token(config)
                cookie = f"{web_ui_session_cookie_name(config)}={token}; HttpOnly; SameSite=Strict; Path=/"
                self._write_redirect("/", set_cookie=cookie)
                return
            if path == "/logout":
                self._write_redirect("/login", set_cookie=build_logout_cookie(config))
                return
            if not self._is_authenticated():
                self._write_auth_required(path)
                return
            if path == "/api/worker/stop-after-current":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("worker", False)) or (bool(roles.get("manager", False)) and not bool((config.get("worker") or {}).get("enabled", False))):
                    self._write_json({"error": "worker_controls_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_local_control_command(worker_command="stop_after_current", updated_by=updated_by)
                self._write_json(build_local_worker_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/worker/hard-stop":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("worker", False)) or (bool(roles.get("manager", False)) and not bool((config.get("worker") or {}).get("enabled", False))):
                    self._write_json({"error": "worker_controls_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_local_control_command(worker_command="hard_stop", updated_by=updated_by)
                self._write_json(build_local_worker_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/worker/start":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("worker", False)) or (bool(roles.get("manager", False)) and not bool((config.get("worker") or {}).get("enabled", False))):
                    self._write_json({"error": "worker_controls_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                persist_worker_enabled(config, True)
                self._write_json(build_local_worker_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/worker/pause":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("worker", False)):
                    self._write_json({"error": "worker_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                persist_worker_enabled(config, False)
                self._write_json(build_local_worker_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/manager/start":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                persist_manager_enabled(config, True)
                self._write_json(build_local_manager_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/manager/pause":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                persist_manager_enabled(config, False)
                self._write_json(build_local_manager_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/manager/stop-after-current":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_local_control_command(manager_command="stop_after_current", updated_by=updated_by)
                self._write_json(build_local_manager_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/manager/finalize-now":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                self._write_json(manager_step(config), status=HTTPStatus.OK)
                return
            if path == "/api/production/start":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                persist_production_enabled(config, True)
                set_local_control_command(production_command="running", updated_by=updated_by)
                self._write_json(build_production_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/production/pause":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_local_control_command(production_command="paused", updated_by=updated_by)
                self._write_json(build_production_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/production/maintenance":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_global_control(config, "maintenance", allow_new_claims=False, allow_finalizer=False, updated_by=updated_by)
                set_local_control_command(production_command="maintenance", updated_by=updated_by)
                self._write_json(build_production_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/production/stop-after-current":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_local_control_command(production_command="stop_after_current", updated_by=updated_by)
                self._write_json(build_production_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/production/run-tick-now" or path == "/api/production/enqueue-now":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                from media_normalizer import production_tick

                execute = bool((config.get("manager") or {}).get("execute", False))
                result = production_tick(config, execute=execute, finalizer_enabled=path != "/api/production/enqueue-now")
                self._write_json(result, status=HTTPStatus.OK)
                return
            if path == "/api/jellyfin/full-scan":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                from media_normalizer import jellyfin_full_scan

                self._write_json(jellyfin_full_scan(config), status=HTTPStatus.OK)
                return
            if path == "/api/global/pause-queue":
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_global_control(config, "paused", allow_new_claims=False, allow_finalizer=True, updated_by=updated_by)
                self._write_json(build_global_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/global/maintenance":
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_global_control(config, "maintenance", allow_new_claims=False, allow_finalizer=False, updated_by=updated_by)
                self._write_json(build_global_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/maintenance/recover-stale-jobs":
                self._write_json(recover_stale_running_jobs(config), status=HTTPStatus.OK)
                return
            if path == "/api/maintenance/recover-stale-locks":
                self._write_json(recover_stale_locks(config), status=HTTPStatus.OK)
                return
            if path == "/api/maintenance/requeue-interrupted-jobs":
                payload = self._read_json_body()
                job_id = payload.get("job_id")
                job_ids = [str(job_id)] if job_id else None
                limit = payload.get("limit")
                self._write_json(requeue_interrupted_jobs(config, job_ids=job_ids, limit=limit), status=HTTPStatus.OK)
                return
            if path == "/api/global/resume-queue":
                payload = self._read_json_body()
                updated_by = sanitize_node_id(str(payload.get("updated_by") or local_node_id(config)))
                set_global_control(config, "running", allow_new_claims=True, allow_finalizer=True, updated_by=updated_by)
                self._write_json(build_global_control_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/settings/worker-schedule":
                try:
                    payload = self._read_json_body()
                    schedule = normalize_worker_schedule_payload(payload)
                    persist_worker_schedule(config, schedule)
                except ValueError as error:
                    self._write_json({"error": str(error), "node_id": local_node_id(config)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._write_json(build_worker_schedule_payload(config), status=HTTPStatus.OK)
                return
            if path == "/api/settings/manager":
                roles = ((config.get("node") or {}).get("roles") or {})
                if not bool(roles.get("manager", False)):
                    self._write_json({"error": "manager_role_disabled", "node_id": local_node_id(config)}, status=HTTPStatus.CONFLICT)
                    return
                try:
                    payload = self._read_json_body()
                    normalized = normalize_manager_settings_payload(payload, config.get("jellyfin") or {})
                    persist_manager_settings(config, normalized["manager"], normalized["jellyfin"])
                except ValueError as error:
                    self._write_json({"error": str(error), "node_id": local_node_id(config)}, status=HTTPStatus.BAD_REQUEST)
                    return
                self._write_json(build_manager_settings_payload(config), status=HTTPStatus.OK)
                return
            self._write_json({"error": "not_found", "path": path}, status=HTTPStatus.NOT_FOUND)

        def log_message(self, format: str, *args: Any) -> None:
            return

    return WebUiHandler


def create_web_ui_server(config: dict[str, Any], host: str | None = None, port: int | None = None) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((web_ui_host(config, host), web_ui_port(config, port)), make_handler(config))
    return server


def run_web_ui_server(config: dict[str, Any], host: str | None = None, port: int | None = None) -> int:
    server = create_web_ui_server(config, host=host, port=port)
    bound_host, bound_port = server.server_address[:2]
    print(
        json.dumps(
            {
                "status": "serving",
                "host": bound_host,
                "port": bound_port,
                "url": f"http://{bound_host}:{bound_port}/",
                "status_url": f"http://{bound_host}:{bound_port}/api/status",
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0