#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, time as datetime_time, timezone
from pathlib import Path
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

import yaml

from queue_store import JOB_STATES, build_job_payload, claim_next_job, enqueue_job, init_state, list_state_files, queue_status, read_json, read_node_control, recover_stale_running_jobs, requeue_interrupted_jobs, sanitize_node_id, set_global_control, set_node_control, shared_state_dir, write_json_atomic
from manager_node import manager_loop, manager_step, node_id as configured_manager_node_id, write_manager_heartbeat
from shared_locks import acquire_lock, lock_status, recover_stale_locks, release_lock
from track_policy import apply_track_policy
from web_ui import run_web_ui_server
from worker_node import node_id as configured_node_id
from worker_node import worker_loop, worker_step, write_worker_heartbeat


VIDEO_EXTENSIONS = {".mkv", ".mp4", ".m4v", ".avi", ".mov", ".webm"}
IGNORED_EXTENSIONS = {".nfo", ".srt", ".ass", ".ssa", ".sub", ".idx", ".jpg", ".png", ".webp", ".txt", ".json"}
SEASON_FOLDER_PATTERN = re.compile(r"^(?:season|series|specials?|ova|onas?|bonus|extras?)(?:\b|\s|[-_.]?\d)|^s\d{1,2}$", re.IGNORECASE)
NORMALIZER_APP_NAME = "autoripper-media-normalizer"


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    data: dict[str, Any] | None = None
    error: str | None = None


def utc_now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def make_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def local_now() -> datetime:
    return datetime.now().astimezone()


def merge_config(base: Any, override: Any) -> Any:
    if isinstance(base, dict) and isinstance(override, dict):
        merged = dict(base)
        for key, value in override.items():
            merged[key] = merge_config(merged.get(key), value) if key in merged else value
        return merged
    return override


def resolve_profile_config(profiles: dict[str, Any], profile_name: str, seen: set[str] | None = None) -> dict[str, Any]:
    seen = seen or set()
    if profile_name in seen:
        chain = " -> ".join([*seen, profile_name])
        raise ValueError(f"Circular config profile inheritance detected: {chain}")
    profile_config = profiles.get(profile_name)
    if profile_config is None:
        available = ", ".join(sorted(profiles)) or "none"
        raise ValueError(f"Unknown config profile '{profile_name}'. Available profiles: {available}")
    profile_config = dict(profile_config)
    parent_name = profile_config.pop("extends", None)
    if not parent_name:
        return profile_config
    parent_config = resolve_profile_config(profiles, str(parent_name), {*seen, profile_name})
    return merge_config(parent_config, profile_config)


def load_config(path: Path, profile: str | None = None) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        raw_config = yaml.safe_load(handle) or {}
    config = {key: value for key, value in raw_config.items() if key not in {"default_profile", "profiles"}}
    selected_profile = profile or raw_config.get("default_profile")
    if selected_profile:
        profiles = raw_config.get("profiles") or {}
        profile_config = resolve_profile_config(profiles, str(selected_profile))
        config = merge_config(config, profile_config)
        config["active_profile"] = selected_profile
    if "output_root" not in config:
        raise ValueError("Config must define output_root")
    if "libraries" not in config:
        raise ValueError("Config must define libraries")
    config["__config_path"] = str(path)
    config["__selected_profile"] = selected_profile
    return config


def require_tool(tool: str) -> None:
    if shutil.which(tool) is None:
        raise RuntimeError(f"Required tool not found on PATH: {tool}")


def ensure_report_dirs(output_root: Path, run_id: str) -> dict[str, Path]:
    run_dir = output_root / "reports" / run_id
    per_file_dir = run_dir / "per_file_logs"
    per_file_dir.mkdir(parents=True, exist_ok=True)
    return {"run": run_dir, "per_file": per_file_dir}


def registry_settings(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("processed_registry") or {}


def registry_enabled(config: dict[str, Any]) -> bool:
    return bool(registry_settings(config).get("enabled", True))


def registry_path(config: dict[str, Any]) -> Path:
    settings = registry_settings(config)
    configured_path = settings.get("path")
    if configured_path:
        return normalize_path(configured_path)
    return Path(config["output_root"]) / "state" / "processed_sources.json"


def load_registry(config: dict[str, Any]) -> dict[str, Any]:
    if not registry_enabled(config):
        return {"version": 1, "sources": {}}
    path = registry_path(config)
    if not path.exists():
        return {"version": 1, "created_at": utc_now(), "sources": {}}
    with path.open("r", encoding="utf-8") as handle:
        registry = json.load(handle)
    registry.setdefault("version", 1)
    registry.setdefault("sources", {})
    return registry


def save_registry(config: dict[str, Any], registry: dict[str, Any]) -> None:
    if not registry_enabled(config):
        return
    path = registry_path(config)
    path.parent.mkdir(parents=True, exist_ok=True)
    registry["updated_at"] = utc_now()
    write_json(path, registry)


def normalized_source_path(path: Path) -> str:
    return os.path.normcase(os.path.abspath(str(path)))


def source_identity(path: Path) -> dict[str, Any]:
    stat = path.stat()
    normalized_path = normalized_source_path(path)
    identity_text = f"{normalized_path}|{stat.st_size}|{stat.st_mtime_ns}"
    return {
        "source_path": str(path),
        "normalized_source_path": normalized_path,
        "source_size_bytes": stat.st_size,
        "source_mtime_ns": stat.st_mtime_ns,
        "fingerprint": hashlib.sha256(identity_text.encode("utf-8", errors="surrogatepass")).hexdigest(),
    }


def registry_record_for_source(registry: dict[str, Any], path: Path) -> dict[str, Any] | None:
    identity = source_identity(path)
    return (registry.get("sources") or {}).get(identity["fingerprint"])


def sampling_group_for_path(path: Path, library_root: str) -> str:
    root = normalize_path(library_root)
    try:
        relative_parts = list(path.relative_to(root).parts[:-1])
    except ValueError:
        relative_parts = list(path.parts[:-1])
    if not relative_parts:
        return path.stem
    for part in reversed(relative_parts):
        if not SEASON_FOLDER_PATTERN.match(part):
            return part
    return relative_parts[-1]


def processed_skip_item(candidate: dict[str, Any], path: Path, record: dict[str, Any]) -> dict[str, Any]:
    stat = path.stat()
    return {
        "source_path": str(path),
        "file_name": path.name,
        "library_root": candidate["library_root"],
        "media_type": candidate["media_type"],
        "sampling_group": sampling_group_for_path(path, candidate["library_root"]),
        "file_size_bytes": stat.st_size,
        "file_size_mb": round(stat.st_size / 1024 / 1024, 2),
        "duration_seconds": None,
        "video_codec": None,
        "bucket": None,
        "skip_reason": "SKIP_ALREADY_PROCESSED_REGISTRY",
        "processed_registry": {
            "processed_at": record.get("processed_at"),
            "output_path": record.get("output_path"),
            "run_id": record.get("run_id"),
            "status": record.get("status"),
        },
    }


def register_processed_source(config: dict[str, Any], registry: dict[str, Any], item: dict[str, Any], log: dict[str, Any], run_id: str) -> None:
    if not registry_enabled(config):
        return
    identity = source_identity(Path(item["source_path"]))
    sources = registry.setdefault("sources", {})
    sources[identity["fingerprint"]] = {
        **identity,
        "processed_at": utc_now(),
        "run_id": run_id,
        "media_type": item.get("media_type"),
        "bucket": item.get("bucket"),
        "output_path": log.get("output_path"),
        "output_size_bytes": (log.get("output") or {}).get("size_bytes"),
        "status": log.get("status"),
        "encode_settings": log.get("encode_settings"),
        "savings": log.get("savings"),
    }


def normalize_path(path: str | Path) -> Path:
    return Path(path).expanduser()


def is_video_file(path: Path) -> bool:
    suffix = path.suffix.lower()
    return suffix in VIDEO_EXTENSIONS and suffix not in IGNORED_EXTENSIONS


def walk_libraries(config: dict[str, Any]) -> list[dict[str, Any]]:
    files: list[dict[str, Any]] = []
    for media_type, roots in (config.get("libraries") or {}).items():
        for root_value in roots or []:
            root = normalize_path(root_value)
            if not root.exists():
                continue
            for dirpath, _, filenames in os.walk(root):
                for filename in filenames:
                    path = Path(dirpath) / filename
                    if is_video_file(path):
                        files.append({"source_path": str(path), "media_type": media_type, "library_root": str(root)})
    return files


def run_ffprobe(path: Path, ffprobe: str) -> ProbeResult:
    command = [
        ffprobe,
        "-v",
        "error",
        "-print_format",
        "json",
        "-show_format",
        "-show_streams",
        "-show_chapters",
        str(path),
    ]
    try:
        completed = subprocess.run(command, capture_output=True, text=True, check=False, encoding="utf-8", errors="replace")
    except OSError as exc:
        return ProbeResult(ok=False, error=str(exc))
    if completed.returncode != 0:
        return ProbeResult(ok=False, error=completed.stderr.strip() or "ffprobe failed")
    try:
        return ProbeResult(ok=True, data=json.loads(completed.stdout))
    except json.JSONDecodeError as exc:
        return ProbeResult(ok=False, error=f"Invalid ffprobe JSON: {exc}")


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


def bitrate_kbps(value: Any) -> int | None:
    number = to_int(value)
    return None if number is None else round(number / 1000)


def stream_tags(stream: dict[str, Any]) -> dict[str, Any]:
    tags = stream.get("tags") or {}
    return {str(key).lower(): value for key, value in tags.items()}


def disposition_flag(stream: dict[str, Any], name: str) -> bool | None:
    disposition = stream.get("disposition") or {}
    value = disposition.get(name)
    if value is None:
        return None
    return bool(value)


def detect_bit_depth(stream: dict[str, Any]) -> int | None:
    bits = to_int(stream.get("bits_per_raw_sample")) or to_int(stream.get("bits_per_sample"))
    if bits:
        return bits
    pix_fmt = str(stream.get("pix_fmt") or "")
    match = re.search(r"p(\d{2})(?:le|be)?$", pix_fmt)
    return int(match.group(1)) if match else None


def detect_hdr(video_stream: dict[str, Any] | None) -> bool:
    if not video_stream:
        return False
    values = " ".join(str(video_stream.get(key) or "").lower() for key in ("color_transfer", "color_primaries", "color_space"))
    if any(token in values for token in ("smpte2084", "arib-std-b67", "bt2020", "hlg", "pq")):
        return True
    side_data = json.dumps(video_stream.get("side_data_list") or [], sort_keys=True).lower()
    return any(token in side_data for token in ("mastering display", "content light", "hdr"))


def extract_metadata(path: Path, media_type: str, library_root: str, probe: dict[str, Any]) -> dict[str, Any]:
    streams = probe.get("streams") or []
    format_data = probe.get("format") or {}
    video_stream = next((stream for stream in streams if stream.get("codec_type") == "video"), None)
    audio_streams = [stream for stream in streams if stream.get("codec_type") == "audio"]
    subtitle_streams = [stream for stream in streams if stream.get("codec_type") == "subtitle"]
    stat = path.stat()

    duration = to_float(format_data.get("duration"))
    if duration is None and video_stream:
        duration = to_float(video_stream.get("duration"))

    audio = []
    for stream in audio_streams:
        tags = stream_tags(stream)
        audio.append(
            {
                "index": stream.get("index"),
                "codec": stream.get("codec_name"),
                "language": tags.get("language"),
                "channels": stream.get("channels"),
                "channel_layout": stream.get("channel_layout"),
                "bitrate_kbps": bitrate_kbps(stream.get("bit_rate")),
                "title": tags.get("title"),
            }
        )

    subtitles = []
    for stream in subtitle_streams:
        tags = stream_tags(stream)
        subtitles.append(
            {
                "index": stream.get("index"),
                "codec": stream.get("codec_name"),
                "language": tags.get("language"),
                "title": tags.get("title"),
                "forced": disposition_flag(stream, "forced"),
                "default": disposition_flag(stream, "default"),
            }
        )

    return {
        "source_path": str(path),
        "file_name": path.name,
        "library_root": library_root,
        "media_type": media_type,
        "sampling_group": sampling_group_for_path(path, library_root),
        "file_size_bytes": stat.st_size,
        "file_size_mb": round(stat.st_size / 1024 / 1024, 2),
        "duration_seconds": duration,
        "container_format": format_data.get("format_name"),
        "overall_bitrate_kbps": bitrate_kbps(format_data.get("bit_rate")),
        "video_stream_index": video_stream.get("index") if video_stream else None,
        "video_codec": video_stream.get("codec_name") if video_stream else None,
        "video_profile": video_stream.get("profile") if video_stream else None,
        "video_width": video_stream.get("width") if video_stream else None,
        "video_height": video_stream.get("height") if video_stream else None,
        "video_pix_fmt": video_stream.get("pix_fmt") if video_stream else None,
        "video_bit_depth": detect_bit_depth(video_stream or {}),
        "video_bitrate_kbps": bitrate_kbps(video_stream.get("bit_rate")) if video_stream else None,
        "is_hdr": detect_hdr(video_stream),
        "audio_stream_count": len(audio),
        "audio_streams": audio,
        "subtitle_stream_count": len(subtitles),
        "subtitle_streams": subtitles,
        "chapters_present": bool(probe.get("chapters")),
        "skip_reason": None,
        "bucket": None,
    }


def bucket_for_item(item: dict[str, Any], config: dict[str, Any]) -> str | None:
    media_type = item.get("media_type") or "unknown"
    buckets = (config.get("buckets") or {}).get(media_type) or {}
    size_mb = item.get("file_size_mb") or 0
    for bucket_name, limits in buckets.items():
        min_mb = limits.get("min_mb")
        max_mb = limits.get("max_mb")
        if min_mb is not None and size_mb < min_mb:
            continue
        if max_mb is not None and size_mb >= max_mb:
            continue
        return f"{media_type}_{bucket_name}"
    return None


def skip_reason(item: dict[str, Any], config: dict[str, Any]) -> str | None:
    rules = config.get("skip_rules") or {}
    filename = item.get("file_name") or ""
    markers = rules.get("normalized_markers") or []
    codec = str(item.get("video_codec") or "").lower()
    duration = item.get("duration_seconds")
    width = item.get("video_width") or 0
    height = item.get("video_height") or 0

    if rules.get("skip_normalized_marker", True) and any(marker in filename for marker in markers):
        return "SKIP_ALREADY_NORMALIZED_MARKER"
    if duration is None:
        return "SKIP_UNKNOWN_DURATION"
    if duration < int(rules.get("min_duration_seconds", 300)):
        return "SKIP_TOO_SHORT"
    if rules.get("skip_hevc", True) and codec in {"hevc", "h265", "h.265"}:
        return "SKIP_ALREADY_HEVC"
    if rules.get("skip_av1", True) and codec in {"av1"}:
        return "SKIP_AV1"
    if rules.get("skip_4k", True) and (width >= 3840 or height >= 2160):
        return "SKIP_4K"
    if rules.get("skip_hdr", True) and item.get("is_hdr"):
        return "SKIP_HDR"
    if not item.get("bucket"):
        return "SKIP_TOO_SMALL_OR_NO_BUCKET"
    return None


def scan(config: dict[str, Any], limit: int | None = None) -> list[dict[str, Any]]:
    ffprobe = (config.get("tools") or {}).get("ffprobe", "ffprobe")
    require_tool(ffprobe)
    registry = load_registry(config)
    results: list[dict[str, Any]] = []
    for candidate in walk_libraries(config):
        if limit is not None and len(results) >= limit:
            break
        path = Path(candidate["source_path"])
        if registry_enabled(config):
            record = registry_record_for_source(registry, path)
            if record:
                results.append(processed_skip_item(candidate, path, record))
                continue
        probe = run_ffprobe(path, ffprobe)
        if not probe.ok:
            results.append(
                {
                    "source_path": str(path),
                    "file_name": path.name,
                    "library_root": candidate["library_root"],
                    "media_type": candidate["media_type"],
                    "sampling_group": sampling_group_for_path(path, candidate["library_root"]),
                    "skip_reason": "SKIP_FFPROBE_FAILED",
                    "ffprobe_error": probe.error,
                    "bucket": None,
                }
            )
            continue
        item = extract_metadata(path, candidate["media_type"], candidate["library_root"], probe.data or {})
        item["bucket"] = bucket_for_item(item, config)
        item["skip_reason"] = skip_reason(item, config)
        results.append(item)
    return results


def bucket_filter_matches(bucket: str | None, bucket_filter: str | None) -> bool:
    if not bucket_filter:
        return True
    if not bucket:
        return False
    normalized_bucket = bucket.lower()
    normalized_filter = bucket_filter.lower()
    return normalized_bucket == normalized_filter or normalized_bucket.endswith(f"_{normalized_filter}")


def batch_settings(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("batch") or {}


def default_realtime_factor(config: dict[str, Any]) -> float:
    return float(batch_settings(config).get("default_realtime_factor", 2.9))


def parse_hhmm(value: str) -> datetime_time:
    try:
        hour_text, minute_text = value.split(":", 1)
        return datetime_time(hour=int(hour_text), minute=int(minute_text))
    except (ValueError, TypeError) as exc:
        raise ValueError(f"Invalid time value {value!r}; expected HH:MM") from exc


def time_is_inside_window(now_time: datetime_time, start: datetime_time, end: datetime_time) -> bool:
    if start == end:
        return True
    if start < end:
        return start <= now_time < end
    return now_time >= start or now_time < end


def batch_window_settings(config: dict[str, Any]) -> dict[str, Any]:
    return batch_settings(config).get("window") or {}


def batch_window_allows_start(config: dict[str, Any], enabled: bool, start_value: str | None, end_value: str | None) -> tuple[bool, dict[str, Any]]:
    settings = batch_window_settings(config)
    window_enabled = enabled or bool(settings.get("enabled", False))
    start_text = start_value or settings.get("start") or "02:00"
    end_text = end_value or settings.get("end") or "07:00"
    now = local_now()
    start = parse_hhmm(start_text)
    end = parse_hhmm(end_text)
    allowed = True if not window_enabled else time_is_inside_window(now.time(), start, end)
    return allowed, {
        "enabled": window_enabled,
        "start": start_text,
        "end": end_text,
        "checked_at": now.isoformat(timespec="seconds"),
        "allowed_to_start_next_job": allowed,
    }


def jellyfin_settings(config: dict[str, Any]) -> dict[str, Any]:
    return config.get("jellyfin") or {}


def jellyfin_enabled(config: dict[str, Any]) -> bool:
    settings = jellyfin_settings(config)
    return bool(settings.get("enabled", False) and settings.get("server_url") and settings.get("api_key"))


def jellyfin_map_path(config: dict[str, Any], source_path: str) -> str:
    mapped = source_path
    for mapping in jellyfin_settings(config).get("path_mappings") or []:
        local_prefix = str(mapping.get("local_prefix") or "")
        jellyfin_prefix = str(mapping.get("jellyfin_prefix") or "")
        if local_prefix and jellyfin_prefix and mapped.lower().startswith(local_prefix.lower()):
            mapped = jellyfin_prefix.rstrip("/\\") + mapped[len(local_prefix) :]
            break
    return mapped.replace("\\", "/")


def normalize_jellyfin_path(path: str) -> str:
    return str(path or "").replace("\\", "/").rstrip("/").lower()


def jellyfin_exact_path_item(items: list[dict[str, Any]], jellyfin_path: str) -> dict[str, Any] | None:
    normalized_target = normalize_jellyfin_path(jellyfin_path)
    for item in items:
        candidate_paths: list[str] = []

        item_path = str(item.get("Path") or "")
        if item_path:
            candidate_paths.append(item_path)

        for media_source in item.get("MediaSources") or []:
            media_source_path = str(media_source.get("Path") or "")
            if media_source_path:
                candidate_paths.append(media_source_path)

        if any(normalize_jellyfin_path(candidate_path) == normalized_target for candidate_path in candidate_paths):
            return item
    return None


def jellyfin_search_terms_for_path(source_path: str) -> list[str]:
    path = Path(source_path)
    raw_terms = [path.stem, path.parent.name]
    episode_match = re.search(r"\bs\d{1,2}e\d{1,3}\b[ ._\-]+(.+)$", path.stem, re.IGNORECASE)
    if episode_match:
        episode_title = episode_match.group(1)
        episode_title = re.sub(r"\b(?:WEB[- ._]?DL|WEBDL|BluRay|BRRip|HDRip|DVDRip|x264|x265|H\.?264|H\.?265|HEVC|AVC|AAC|DDP?5\.1|10bit|8bit|1080p|720p|2160p|480p)\b.*$", "", episode_title, flags=re.IGNORECASE)
        episode_title = episode_title.strip(" ._-")
        raw_terms.append(episode_title)
    cleaned_terms: list[str] = []
    seen: set[str] = set()
    for term in raw_terms:
        cleaned = re.sub(r"[._\-]+", " ", term).strip()
        cleaned = re.sub(r"\s+", " ", cleaned)
        variants = [cleaned]
        year_match = re.match(r"^(.*?)(?:\s+\d{4})\b", cleaned)
        if year_match:
            variants.append(year_match.group(1).strip())
        for variant in variants:
            normalized = variant.casefold()
            if len(variant) >= 3 and normalized not in seen:
                seen.add(normalized)
                cleaned_terms.append(variant)
    return cleaned_terms


def jellyfin_find_item_by_search(config: dict[str, Any], source_path: str, jellyfin_path: str) -> dict[str, Any] | None:
    for term in jellyfin_search_terms_for_path(source_path):
        query = urllib_parse.urlencode({"Recursive": "true", "Fields": "Path,MediaSources", "SearchTerm": term, "Limit": "50"})
        data = jellyfin_api_json(config, f"/Items?{query}")
        item = jellyfin_exact_path_item(data.get("Items") or [], jellyfin_path)
        if item is not None:
            return item
    return None


def jellyfin_api_json(config: dict[str, Any], path: str, method: str = "GET") -> dict[str, Any]:
    settings = jellyfin_settings(config)
    base_url = str(settings["server_url"]).rstrip("/")
    request = urllib_request.Request(
        f"{base_url}{path}",
        method=method,
        headers={"X-Emby-Token": str(settings["api_key"]), "Accept": "application/json"},
    )
    try:
        with urllib_request.urlopen(request, timeout=float(settings.get("timeout_seconds", 30))) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib_error.HTTPError as exc:
        raise RuntimeError(f"Jellyfin HTTP {exc.code}: {exc.read().decode('utf-8', errors='replace')}") from exc
    except urllib_error.URLError as exc:
        raise RuntimeError(f"Jellyfin request failed: {exc.reason}") from exc
    return json.loads(body) if body else {}


def jellyfin_find_item_id_by_path(config: dict[str, Any], source_path: str) -> str | None:
    jellyfin_path = jellyfin_map_path(config, source_path)
    query = urllib_parse.urlencode({"Recursive": "true", "Fields": "Path,MediaSources", "Path": jellyfin_path, "Limit": "10"})
    data = jellyfin_api_json(config, f"/Items?{query}")
    item = jellyfin_exact_path_item(data.get("Items") or [], jellyfin_path)
    if item is None:
        item = jellyfin_find_item_by_search(config, source_path, jellyfin_path)
    if item is not None:
        return str(item.get("Id"))
    return None


def jellyfin_refresh_source(config: dict[str, Any], source_path: str) -> dict[str, Any]:
    if not jellyfin_enabled(config):
        return {"enabled": False, "status": "skipped", "reason": "Jellyfin refresh is disabled or not configured"}
    jellyfin_path = jellyfin_map_path(config, source_path)
    try:
        item_id = jellyfin_find_item_id_by_path(config, source_path)
        if not item_id:
            return {"enabled": True, "status": "not_found", "jellyfin_path": jellyfin_path}
        query = urllib_parse.urlencode(
            {
                "Recursive": "false",
                "MetadataRefreshMode": "Default",
                "ImageRefreshMode": "Default",
                "ReplaceAllMetadata": "false",
                "ReplaceAllImages": "false",
            }
        )
        jellyfin_api_json(config, f"/Items/{urllib_parse.quote(item_id)}/Refresh?{query}", method="POST")
        return {"enabled": True, "status": "refreshed", "item_id": item_id, "jellyfin_path": jellyfin_path}
    except Exception as exc:
        return {"enabled": True, "status": "failed", "jellyfin_path": jellyfin_path, "error": str(exc)}


def jellyfin_full_scan(config: dict[str, Any]) -> dict[str, Any]:
    if not jellyfin_enabled(config):
        return {"enabled": False, "status": "skipped", "reason": "Jellyfin refresh is disabled or not configured"}
    try:
        jellyfin_api_json(config, "/Library/Refresh", method="POST")
        return {"enabled": True, "status": "scan_triggered"}
    except Exception as exc:
        return {"enabled": True, "status": "failed", "error": str(exc)}


def fast_top_filesystem_candidates(
    config: dict[str, Any],
    media_type_filter: str | None = None,
    bucket_filter: str | None = None,
    exclude_source_paths: set[str] | None = None,
    media_type_filters: set[str] | None = None,
) -> list[dict[str, Any]]:
    registry = load_registry(config)
    excluded = exclude_source_paths or set()
    candidates: list[dict[str, Any]] = []
    for candidate in walk_libraries(config):
        if media_type_filter and candidate["media_type"] != media_type_filter:
            continue
        if media_type_filters is not None and candidate["media_type"] not in media_type_filters:
            continue
        path = Path(candidate["source_path"])
        if normalized_source_path(path) in excluded:
            continue
        if registry_enabled(config) and registry_record_for_source(registry, path):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        item = {
            "source_path": str(path),
            "file_name": path.name,
            "library_root": candidate["library_root"],
            "media_type": candidate["media_type"],
            "sampling_group": sampling_group_for_path(path, candidate["library_root"]),
            "file_size_bytes": stat.st_size,
            "file_size_mb": round(stat.st_size / 1024 / 1024, 2),
            "bucket": None,
            "skip_reason": None,
        }
        item["bucket"] = bucket_for_item(item, config)
        if not item["bucket"]:
            continue
        if not bucket_filter_matches(item["bucket"], bucket_filter):
            continue
        candidates.append(item)
    return sorted(candidates, key=lambda item: item["file_size_bytes"], reverse=True)


def plan_top_candidates(
    config: dict[str, Any],
    count: int,
    media_type_filter: str | None = None,
    bucket_filter: str | None = None,
    filesystem_limit: int | None = None,
    min_duration: float | None = None,
    max_duration: float | None = None,
    exclude_source_paths: set[str] | None = None,
    dedupe_sampling_groups: bool = True,
    media_type_filters: set[str] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    ffprobe = (config.get("tools") or {}).get("ffprobe", "ffprobe")
    require_tool(ffprobe)
    filesystem_candidates = fast_top_filesystem_candidates(config, media_type_filter, bucket_filter, exclude_source_paths=exclude_source_paths, media_type_filters=media_type_filters)
    limited_candidates = filesystem_candidates[:filesystem_limit] if filesystem_limit else filesystem_candidates
    checked_items: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    seen_groups: set[str] = set()

    for candidate in limited_candidates:
        if len(selected) >= count:
            break
        path = Path(candidate["source_path"])
        probe = run_ffprobe(path, ffprobe)
        if not probe.ok:
            item = {**candidate, "skip_reason": "SKIP_FFPROBE_FAILED", "ffprobe_error": probe.error}
            checked_items.append(item)
            continue
        item = extract_metadata(path, candidate["media_type"], candidate["library_root"], probe.data or {})
        item["bucket"] = bucket_for_item(item, config)
        item["skip_reason"] = skip_reason(item, config)
        checked_items.append(item)
        if item.get("skip_reason"):
            continue
        duration = item.get("duration_seconds")
        if min_duration is not None and (duration is None or float(duration) < min_duration):
            item["skip_reason"] = "SKIP_BATCH_MIN_DURATION"
            continue
        if max_duration is not None and (duration is None or float(duration) > max_duration):
            item["skip_reason"] = "SKIP_BATCH_MAX_DURATION"
            continue
        if not bucket_filter_matches(item.get("bucket"), bucket_filter):
            continue
        if dedupe_sampling_groups:
            group_name = item.get("sampling_group") or Path(item["source_path"]).parent.name
            if group_name in seen_groups:
                continue
            seen_groups.add(group_name)
        selected.append(item)

    stats = {
        "selection_strategy": "top_largest_first",
        "requested_count": count,
        "media_type_filter": media_type_filter,
        "bucket_filter": bucket_filter,
        "min_duration_filter_seconds": min_duration,
        "max_duration_filter_seconds": max_duration,
        "filesystem_candidates_found": len(filesystem_candidates),
        "filesystem_candidates_considered": len(limited_candidates),
        "ffprobe_candidates_checked": len(checked_items),
        "selected_count": len(selected),
        "excluded_source_count": len(exclude_source_paths or set()),
        "dedupe_sampling_groups": dedupe_sampling_groups,
        "media_type_filters": sorted(media_type_filters) if media_type_filters is not None else None,
    }
    return checked_items, selected, stats


def add_batch_estimates(summary: dict[str, Any], selected: list[dict[str, Any]], config: dict[str, Any], stats: dict[str, Any]) -> dict[str, Any]:
    selected_duration = sum(float(item.get("duration_seconds") or 0) for item in selected)
    factor = default_realtime_factor(config)
    estimated_seconds = round(selected_duration / factor, 2) if factor and selected_duration else 0
    summary.update(stats)
    summary.update(
        {
            "batch_mode": True,
            "estimated_source_duration_seconds": round(selected_duration, 2),
            "estimated_realtime_factor": factor,
            "estimated_encode_time_seconds": estimated_seconds,
            "estimated_encode_time_hours": round(estimated_seconds / 3600, 2) if estimated_seconds else 0,
        }
    )
    return summary


def select_samples(items: list[dict[str, Any]], samples_per_bucket: int, seed: int | None, max_per_group: int = 1) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        if item.get("skip_reason") or not item.get("bucket"):
            continue
        grouped.setdefault(item["bucket"], []).append(item)

    selected: list[dict[str, Any]] = []
    group_selection_counts: dict[str, int] = {}
    for bucket in sorted(grouped):
        bucket_items = sorted(grouped[bucket], key=lambda item: item["source_path"])
        by_sampling_group: dict[str, list[dict[str, Any]]] = {}
        for item in bucket_items:
            group_name = item.get("sampling_group") or Path(item["source_path"]).parent.name
            by_sampling_group.setdefault(group_name, []).append(item)

        representatives: list[dict[str, Any]] = []
        per_group_limit = max(1, max_per_group)
        for group_name in sorted(by_sampling_group):
            already_selected = group_selection_counts.get(group_name, 0)
            if already_selected >= per_group_limit:
                continue
            candidates = by_sampling_group[group_name]
            take_count = min(per_group_limit - already_selected, len(candidates))
            representatives.extend(rng.sample(candidates, take_count))

        count = min(samples_per_bucket, len(representatives))
        bucket_selection = rng.sample(representatives, count)
        selected.extend(bucket_selection)
        for item in bucket_selection:
            group_name = item.get("sampling_group") or Path(item["source_path"]).parent.name
            group_selection_counts[group_name] = group_selection_counts.get(group_name, 0) + 1
    return sorted(selected, key=lambda item: (item["bucket"], item["source_path"]))


def sanitize_filename(name: str) -> str:
    return re.sub(r"[\\/:*?\"<>|]", "_", name).strip()


def unique_output_path(output_dir: Path, source: Path, crf: int, clip_index: int | None = None) -> Path:
    stem = sanitize_filename(source.stem)
    clip_suffix = f" CLIP{clip_index}" if clip_index is not None else ""
    base = f"{stem}{clip_suffix} [TEST-HEVC-CRF{crf}].mkv"
    candidate = output_dir / base
    counter = 1
    while candidate.exists():
        candidate = output_dir / f"{stem}{clip_suffix} [TEST-HEVC-CRF{crf}] ({counter}).mkv"
        counter += 1
    return candidate


def encoding_settings(config: dict[str, Any], media_type: str) -> dict[str, Any]:
    encoding = config.get("encoding") or {}
    return encoding.get(media_type) or encoding.get("unknown") or encoding.get("movie") or {}


def build_ffmpeg_command(
    config: dict[str, Any],
    source: Path,
    output: Path,
    media_type: str,
    track_policy_result: dict[str, Any] | None = None,
    clip_start: float | None = None,
    clip_duration: int | None = None,
) -> list[str]:
    ffmpeg = (config.get("tools") or {}).get("ffmpeg", "ffmpeg")
    settings = encoding_settings(config, media_type)
    command = [ffmpeg, "-hide_banner", "-y"]
    if clip_start is not None:
        command.extend(["-ss", f"{clip_start:.3f}"])
    command.extend(["-i", str(source)])
    if clip_duration is not None:
        command.extend(["-t", str(clip_duration)])
    command.extend((track_policy_result or {}).get("map_arguments") or ["-map", "0"])
    command.extend(
        [
            "-c:v",
            str(settings.get("encoder", "libx265")),
            "-preset",
            str(settings.get("preset", "medium")),
            "-crf",
            str(settings.get("crf", 24)),
        ]
    )
    if settings.get("pix_fmt"):
        command.extend(["-pix_fmt", str(settings["pix_fmt"])])
    command.extend(
        [
            "-c:a",
            str(settings.get("audio", "copy")),
            "-c:s",
            str(settings.get("subtitles", "copy")),
            "-c:t",
            "copy",
            "-map_metadata",
            "0",
            "-map_chapters",
            "0",
            "-metadata",
            f"encoded_by={NORMALIZER_APP_NAME}",
            str(output),
        ]
    )
    return command


def source_summary(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "size_bytes": item.get("file_size_bytes"),
        "size_mb": item.get("file_size_mb"),
        "duration_seconds": item.get("duration_seconds"),
        "container": item.get("container_format"),
        "video_codec": item.get("video_codec"),
        "video_width": item.get("video_width"),
        "video_height": item.get("video_height"),
        "pix_fmt": item.get("video_pix_fmt"),
        "video_bitrate_kbps": item.get("video_bitrate_kbps"),
        "overall_bitrate_kbps": item.get("overall_bitrate_kbps"),
        "audio_stream_count": item.get("audio_stream_count"),
        "subtitle_stream_count": item.get("subtitle_stream_count"),
    }


def verification_limits(config: dict[str, Any], media_type: str | None) -> dict[str, Any]:
    verification = dict(config.get("verification") or {})
    per_media_type = verification.get("per_media_type") or {}
    if media_type:
        verification.update(per_media_type.get(media_type, {}))
    verification.pop("per_media_type", None)
    return verification


DEFAULT_BITRATE_THRESHOLDS_1080P = {
    "anime": {"hard_fail_kbps": 500, "warning_kbps": 700},
    "series": {"hard_fail_kbps": 900, "warning_kbps": 1200},
    "movie": {"hard_fail_kbps": 1200, "warning_kbps": 1800},
    "unknown": {"hard_fail_kbps": 900, "warning_kbps": 1200},
}


def resolution_bucket(item: dict[str, Any]) -> str:
    height = to_int(item.get("video_height"))
    width = to_int(item.get("video_width"))
    largest_dimension = max(value for value in (height, width) if value is not None) if height or width else None
    if height and height >= 1800 or largest_dimension and largest_dimension >= 3000:
        return "4k"
    if height and height <= 800:
        return "720p"
    return "1080p"


def scaled_bitrate_thresholds(media_type: str, bucket: str) -> dict[str, int]:
    base = DEFAULT_BITRATE_THRESHOLDS_1080P.get(media_type) or DEFAULT_BITRATE_THRESHOLDS_1080P["unknown"]
    scale = {"720p": 0.6, "1080p": 1.0, "4k": 2.75}.get(bucket, 1.0)
    return {
        "hard_fail_kbps": int(round(float(base["hard_fail_kbps"]) * scale)),
        "warning_kbps": int(round(float(base["warning_kbps"]) * scale)),
    }


def suspicious_size_threshold(config: dict[str, Any], media_type: str | None, source_item: dict[str, Any], output_item: dict[str, Any]) -> dict[str, Any]:
    effective_media_type = str(media_type or "unknown")
    bucket = resolution_bucket(output_item if output_item.get("video_height") or output_item.get("video_width") else source_item)
    threshold = scaled_bitrate_thresholds(effective_media_type, bucket)
    configured = (((config.get("verification") or {}).get("bitrate_thresholds") or {}).get(effective_media_type) or {}).get(bucket) or {}
    if configured:
        threshold.update({key: int(value) for key, value in configured.items() if value is not None})
    return {"media_type": effective_media_type, "resolution": bucket, **threshold}


def estimated_overall_bitrate_kbps(size_bytes: int, duration_seconds: Any) -> int | None:
    duration = to_float(duration_seconds)
    if not duration or duration <= 0 or size_bytes <= 0:
        return None
    return int(round((size_bytes * 8) / duration / 1000))


def expected_stream_counts(source_item: dict[str, Any], track_policy_result: dict[str, Any] | None = None) -> dict[str, Any]:
    if track_policy_result and track_policy_result.get("applied"):
        return {
            "expected_audio_stream_count": track_policy_result.get("expected_audio_stream_count"),
            "expected_subtitle_stream_count": track_policy_result.get("expected_subtitle_stream_count"),
            "expected_stream_count_source": "track_policy",
        }
    return {
        "expected_audio_stream_count": source_item.get("audio_stream_count"),
        "expected_subtitle_stream_count": source_item.get("subtitle_stream_count"),
        "expected_stream_count_source": "source",
    }


def verify_output(
    config: dict[str, Any],
    source_item: dict[str, Any],
    output: Path,
    track_policy_result: dict[str, Any] | None = None,
    expected_duration_seconds: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    verification = {
        "output_exists": output.exists(),
        "output_non_empty": False,
        "ffprobe_ok": False,
        "duration_ok": False,
        "video_stream_exists": False,
        "audio_streams_ok": False,
        "subtitle_streams_ok": False,
        "size_reduction_ok": False,
        "not_suspiciously_tiny": False,
        "output_to_source_ratio": None,
        "source_size_bytes": source_item.get("file_size_bytes") or 0,
        "output_size_bytes": None,
        "overall_bitrate_kbps": None,
        "suspicious_size_warning": False,
        "suspicious_size_warning_reason": None,
        "suspicious_size_hard_fail": False,
        "suspicious_size_threshold_used": None,
        "expected_audio_stream_count": None,
        "expected_subtitle_stream_count": None,
        "expected_stream_count_source": "source",
    }
    if not output.exists():
        return verification, None, ["Output file does not exist"]
    output_size = output.stat().st_size
    verification["output_size_bytes"] = output_size
    verification["output_non_empty"] = output_size > 0
    if output_size <= 0:
        errors.append("Output file is empty")

    ffprobe = (config.get("tools") or {}).get("ffprobe", "ffprobe")
    probe = run_ffprobe(output, ffprobe)
    if not probe.ok:
        errors.append(probe.error or "Output ffprobe failed")
        return verification, None, errors

    verification["ffprobe_ok"] = True
    output_item = extract_metadata(output, source_item.get("media_type", "unknown"), "", probe.data or {})
    output_summary = source_summary(output_item)
    output_summary["video_codec"] = output_item.get("video_codec")

    limits = verification_limits(config, source_item.get("media_type"))
    if expected_duration_seconds is None:
        max_duration_diff = float(limits.get("max_duration_diff_seconds", 2))
    else:
        max_duration_diff = float(limits.get("clip_max_duration_diff_seconds", 6))
    source_duration = expected_duration_seconds if expected_duration_seconds is not None else source_item.get("duration_seconds")
    output_duration = output_item.get("duration_seconds")
    if source_duration is not None and output_duration is not None:
        verification["duration_ok"] = abs(float(source_duration) - float(output_duration)) <= max_duration_diff
    verification["video_stream_exists"] = bool(output_item.get("video_codec"))
    expected_counts = expected_stream_counts(source_item, track_policy_result)
    expected_audio_count = expected_counts["expected_audio_stream_count"]
    expected_subtitle_count = expected_counts["expected_subtitle_stream_count"]
    verification.update(expected_counts)
    verification["audio_streams_ok"] = output_item.get("audio_stream_count") == expected_audio_count and output_item.get("audio_stream_count", 0) >= 1
    verification["subtitle_streams_ok"] = output_item.get("subtitle_stream_count") == expected_subtitle_count

    source_size = source_item.get("file_size_bytes") or 0
    ratio = output_size / source_size if source_size else 0
    output_bitrate_kbps = output_item.get("overall_bitrate_kbps") or estimated_overall_bitrate_kbps(output_size, output_item.get("duration_seconds") or source_duration)
    threshold = suspicious_size_threshold(config, source_item.get("media_type"), source_item, output_item)
    verification["output_to_source_ratio"] = ratio if source_size else None
    verification["overall_bitrate_kbps"] = output_bitrate_kbps
    verification["suspicious_size_threshold_used"] = threshold
    if expected_duration_seconds is None:
        verification["size_reduction_ok"] = ratio < float(limits.get("max_output_source_ratio", 0.95))
        ratio_warning = bool(source_size and ratio <= float(limits.get("min_output_source_ratio", 0.15)))
        bitrate_warning = output_bitrate_kbps is not None and output_bitrate_kbps < int(threshold["warning_kbps"])
        bitrate_hard_fail = output_bitrate_kbps is not None and output_bitrate_kbps < int(threshold["hard_fail_kbps"])
        warning_reasons = []
        if ratio_warning:
            warning_reasons.append(f"low output/source ratio {ratio:.3f} below legacy warning threshold {float(limits.get('min_output_source_ratio', 0.15)):.3f}")
        if bitrate_warning:
            warning_reasons.append(f"{threshold['media_type']} {threshold['resolution']} bitrate {output_bitrate_kbps} kbps below warning threshold {threshold['warning_kbps']} kbps")
        verification["suspicious_size_warning"] = bool(warning_reasons)
        verification["suspicious_size_warning_reason"] = "; ".join(warning_reasons) if warning_reasons else None
        verification["suspicious_size_hard_fail"] = bool(bitrate_hard_fail)
        verification["not_suspiciously_tiny"] = not bitrate_hard_fail and output_size > 0
    else:
        verification["size_reduction_ok"] = output_size < source_size
        verification["not_suspiciously_tiny"] = output_size > 0

    hard_verification_keys = [
        "output_exists",
        "output_non_empty",
        "ffprobe_ok",
        "duration_ok",
        "video_stream_exists",
        "audio_streams_ok",
        "subtitle_streams_ok",
        "size_reduction_ok",
        "not_suspiciously_tiny",
    ]
    for key in hard_verification_keys:
        ok = verification.get(key)
        if not ok:
            errors.append(f"Verification failed: {key}")
    return verification, output_summary, errors


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, ensure_ascii=False)
        handle.write("\n")


def write_csv_summary(path: Path, summary: dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["key", "value"])
        for key, value in summary.items():
            writer.writerow([key, value])


def write_human_summary(path: Path, summary: dict[str, Any]) -> None:
    lines = ["# Autoripper Run Summary", ""]
    for key, value in summary.items():
        lines.append(f"- **{key}**: {value}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def make_run_summary(
    run_id: str,
    started_at: str,
    finished_at: str,
    config_path: Path,
    items: list[dict[str, Any]],
    selected: list[dict[str, Any]],
    logs: list[dict[str, Any]],
) -> dict[str, Any]:
    successes = [log for log in logs if log.get("status") == "success"]
    failures = [log for log in logs if log.get("status") == "failed"]
    clip_mode = any(log.get("clip") for log in logs)
    total_source = 0 if clip_mode else sum((log.get("source") or {}).get("size_bytes") or 0 for log in successes)
    total_output = sum((log.get("output") or {}).get("size_bytes") or 0 for log in successes)
    saved = 0 if clip_mode else total_source - total_output
    saved_percent = round((saved / total_source * 100), 2) if total_source and not clip_mode else 0
    encode_times = [float(log.get("encode_time_seconds") or 0) for log in successes if log.get("encode_time_seconds")]
    source_durations = [float((log.get("source") or {}).get("duration_seconds") or 0) for log in successes if (log.get("source") or {}).get("duration_seconds")]
    speed_factors = [float((log.get("speed") or {}).get("realtime_factor") or 0) for log in successes if (log.get("speed") or {}).get("realtime_factor")]
    total_encode_time = round(sum(encode_times), 2) if encode_times else 0
    total_source_duration = round(sum(source_durations), 2) if source_durations else 0
    overall_realtime_factor = round(total_source_duration / total_encode_time, 3) if total_encode_time and total_source_duration else 0
    average_realtime_factor = round(sum(speed_factors) / len(speed_factors), 3) if speed_factors else 0
    return {
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "config_path": str(config_path),
        "number_of_files_scanned": len(items),
        "number_of_files_classified": len([item for item in items if item.get("bucket")]),
        "number_of_files_skipped": len([item for item in items if item.get("skip_reason")]),
        "number_of_samples_selected": len(selected),
        "number_of_encodes_attempted": len(logs),
        "number_of_successes": len(successes),
        "number_of_failures": len(failures),
        "clip_mode": clip_mode,
        "clip_savings_not_estimated": clip_mode,
        "total_encode_time_seconds": total_encode_time,
        "total_source_duration_seconds": total_source_duration,
        "overall_realtime_factor": overall_realtime_factor,
        "average_realtime_factor": average_realtime_factor,
        "total_source_size_bytes": total_source,
        "total_output_size_bytes": total_output,
        "total_saved_size_bytes": saved,
        "total_saved_percent": saved_percent,
    }


def write_reports(report_dirs: dict[str, Path], summary: dict[str, Any], items: list[dict[str, Any]], selected: list[dict[str, Any]]) -> None:
    write_json(report_dirs["run"] / "run_summary.json", summary)
    write_csv_summary(report_dirs["run"] / "run_summary.csv", summary)
    write_human_summary(report_dirs["run"] / "human_readable_summary.md", summary)
    write_json(report_dirs["run"] / "scan_items.json", items)
    write_json(report_dirs["run"] / "selected_samples.json", selected)


def encode_one(
    config: dict[str, Any],
    report_dirs: dict[str, Path],
    item: dict[str, Any],
    clip_index: int | None = None,
    clip_start: float | None = None,
    clip_duration: int | None = None,
) -> dict[str, Any]:
    source = Path(item["source_path"])
    media_type = item.get("media_type") or "unknown"
    settings = encoding_settings(config, media_type)
    crf = int(settings.get("crf", 24))
    bucket = item.get("bucket") or f"{media_type}_unknown"
    output_dir = Path(config["output_root"]) / bucket
    output_dir.mkdir(parents=True, exist_ok=True)
    output = unique_output_path(output_dir, source, crf, clip_index)
    track_policy_result = apply_track_policy(config, item)
    command = build_ffmpeg_command(config, source, output, media_type, track_policy_result, clip_start, clip_duration)
    started_at = utc_now()
    started = time.monotonic()

    log: dict[str, Any] = {
        "source_path": str(source),
        "output_path": str(output),
        "media_type": media_type,
        "bucket": bucket,
        "status": "failed",
        "started_at": started_at,
        "finished_at": None,
        "encode_time_seconds": None,
        "speed": None,
        "source": source_summary(item),
        "output": None,
        "savings": None,
        "encode_settings": {
            "video_encoder": settings.get("encoder", "libx265"),
            "crf": crf,
            "preset": settings.get("preset", "medium"),
            "audio": settings.get("audio", "copy"),
            "subtitles": settings.get("subtitles", "copy"),
            "container": "mkv",
        },
        "track_policy": track_policy_result,
        "ffmpeg_command": command,
        "verification": None,
        "errors": [],
        "warnings": [],
    }
    if clip_index is not None:
        log["clip"] = {"index": clip_index, "start_seconds": clip_start, "duration_seconds": clip_duration}
        log["warnings"].append("Clip mode may not preserve subtitle behavior exactly for all subtitle formats")

    completed = subprocess.run(command, capture_output=True, text=True, check=False, encoding="utf-8", errors="replace")
    log["finished_at"] = utc_now()
    log["encode_time_seconds"] = round(time.monotonic() - started, 2)
    source_duration = float(item.get("duration_seconds") or 0)
    if log["encode_time_seconds"] and source_duration:
        log["speed"] = {
            "source_duration_seconds": round(source_duration, 2),
            "encode_time_seconds": log["encode_time_seconds"],
            "realtime_factor": round(source_duration / log["encode_time_seconds"], 3),
        }
    if completed.returncode != 0:
        log["errors"].append(completed.stderr.strip() or "ffmpeg failed")
    else:
        expected_duration = clip_duration if clip_duration is not None else None
        verification, output_summary, errors = verify_output(config, item, output, track_policy_result, expected_duration)
        log["verification"] = verification
        log["output"] = output_summary
        log["errors"].extend(errors)
        if output.exists() and output_summary:
            source_size = item.get("file_size_bytes") or 0
            output_size = output.stat().st_size
            if clip_duration is None:
                saved = source_size - output_size
                log["savings"] = {
                    "saved_bytes": saved,
                    "saved_mb": round(saved / 1024 / 1024, 2),
                    "saved_percent": round(saved / source_size * 100, 2) if source_size else 0,
                }
            else:
                log["savings"] = {
                    "not_estimated": True,
                    "reason": "Clip output cannot estimate full-file savings directly",
                    "output_source_ratio_percent": round(output_size / source_size * 100, 2) if source_size else 0,
                }
        if not errors:
            log["status"] = "success"

    safe_name = sanitize_filename(source.stem)[:120]
    suffix = f"_clip_{clip_index}" if clip_index is not None else ""
    write_json(report_dirs["per_file"] / f"{safe_name}{suffix}.json", log)
    return log


def run_scan_command(config_path: Path, config: dict[str, Any], limit: int | None) -> int:
    run_id = make_run_id()
    started_at = utc_now()
    report_dirs = ensure_report_dirs(Path(config["output_root"]), run_id)
    items = scan(config, limit)
    finished_at = utc_now()
    summary = make_run_summary(run_id, started_at, finished_at, config_path, items, [], [])
    write_reports(report_dirs, summary, items, [])
    print(f"Scanned {len(items)} files. Reports: {report_dirs['run']}")
    return 0


def run_dry_run_command(config_path: Path, config: dict[str, Any], seed: int | None, limit: int | None) -> int:
    run_id = make_run_id()
    started_at = utc_now()
    report_dirs = ensure_report_dirs(Path(config["output_root"]), run_id)
    items = scan(config, limit)
    sampling = config.get("sampling") or {}
    samples_per_bucket = int(sampling.get("samples_per_bucket", 3))
    max_per_group = int(sampling.get("max_per_group", 1))
    selected = select_samples(items, samples_per_bucket, seed if seed is not None else sampling.get("seed"), max_per_group)
    finished_at = utc_now()
    summary = make_run_summary(run_id, started_at, finished_at, config_path, items, selected, [])
    write_reports(report_dirs, summary, items, selected)
    print(f"Dry run selected {len(selected)} samples. Reports: {report_dirs['run']}")
    return 0


def run_track_audit_command(config_path: Path, config: dict[str, Any], limit: int | None) -> int:
    run_id = make_run_id()
    started_at = utc_now()
    report_dirs = ensure_report_dirs(Path(config["output_root"]), run_id)
    audit_config = merge_config(config, {"processed_registry": {"enabled": False}})
    items = scan(audit_config, limit)
    audit_items = []
    for item in items:
        if item.get("skip_reason"):
            continue
        audit_items.append(
            {
                "source_path": item.get("source_path"),
                "media_type": item.get("media_type"),
                "bucket": item.get("bucket"),
                "audio_stream_count": item.get("audio_stream_count"),
                "subtitle_stream_count": item.get("subtitle_stream_count"),
                "track_policy": apply_track_policy(config, item),
            }
        )
    finished_at = utc_now()
    summary = make_run_summary(run_id, started_at, finished_at, config_path, items, [], [])
    summary["track_policy_audit_count"] = len(audit_items)
    summary["track_policy_cleanup_candidates"] = len([item for item in audit_items if (item.get("track_policy") or {}).get("applied")])
    write_reports(report_dirs, summary, items, [])
    write_json(report_dirs["run"] / "track_policy_audit.json", audit_items)
    print(f"Audited track policy for {len(audit_items)} files. Reports: {report_dirs['run']}")
    return 0


def run_test_command(config_path: Path, config: dict[str, Any], seed: int | None, clips: bool, limit: int | None) -> int:
    ffmpeg = (config.get("tools") or {}).get("ffmpeg", "ffmpeg")
    require_tool(ffmpeg)
    run_id = make_run_id()
    started_at = utc_now()
    report_dirs = ensure_report_dirs(Path(config["output_root"]), run_id)
    items = scan(config, limit)
    sampling = config.get("sampling") or {}
    samples_per_bucket = int(sampling.get("samples_per_bucket", 3))
    max_per_group = int(sampling.get("max_per_group", 1))
    selected = select_samples(items, samples_per_bucket, seed if seed is not None else sampling.get("seed"), max_per_group)
    logs: list[dict[str, Any]] = []
    registry = load_registry(config)
    for item in selected:
        if clips:
            duration = float(item.get("duration_seconds") or 0)
            for clip_index, fraction in enumerate((0.15, 0.5, 0.8), start=1):
                start = max(0.0, min(duration * fraction, max(0.0, duration - 60)))
                logs.append(encode_one(config, report_dirs, item, clip_index, start, 60))
        else:
            log = encode_one(config, report_dirs, item)
            logs.append(log)
            if log.get("status") == "success":
                register_processed_source(config, registry, item, log, run_id)
                save_registry(config, registry)
                log["jellyfin_refresh"] = jellyfin_refresh_source(config, item["source_path"])
                safe_name = sanitize_filename(Path(item["source_path"]).stem)[:120]
                write_json(report_dirs["per_file"] / f"{safe_name}.json", log)
    finished_at = utc_now()
    summary = make_run_summary(run_id, started_at, finished_at, config_path, items, selected, logs)
    write_reports(report_dirs, summary, items, selected)
    print(f"Attempted {len(logs)} encodes. Reports: {report_dirs['run']}")
    return 1 if any(log.get("status") == "failed" for log in logs) else 0


def run_plan_top_command(
    config_path: Path,
    config: dict[str, Any],
    count: int,
    media_type: str | None,
    bucket: str | None,
    filesystem_limit: int | None,
    min_duration: float | None,
    max_duration: float | None,
) -> int:
    run_id = make_run_id()
    started_at = utc_now()
    report_dirs = ensure_report_dirs(Path(config["output_root"]), run_id)
    checked_items, selected, stats = plan_top_candidates(config, count, media_type, bucket, filesystem_limit, min_duration, max_duration)
    finished_at = utc_now()
    summary = make_run_summary(run_id, started_at, finished_at, config_path, checked_items, selected, [])
    summary = add_batch_estimates(summary, selected, config, stats)
    write_reports(report_dirs, summary, checked_items, selected)
    print(f"Planned {len(selected)} top candidates. Reports: {report_dirs['run']}")
    return 0


def run_batch_top_command(
    config_path: Path,
    config: dict[str, Any],
    count: int,
    media_type: str | None,
    bucket: str | None,
    filesystem_limit: int | None,
    min_duration: float | None,
    max_duration: float | None,
    respect_window: bool,
    window_start: str | None,
    window_end: str | None,
) -> int:
    ffmpeg = (config.get("tools") or {}).get("ffmpeg", "ffmpeg")
    require_tool(ffmpeg)
    run_id = make_run_id()
    started_at = utc_now()
    report_dirs = ensure_report_dirs(Path(config["output_root"]), run_id)
    checked_items, selected, stats = plan_top_candidates(config, count, media_type, bucket, filesystem_limit, min_duration, max_duration)
    logs: list[dict[str, Any]] = []
    registry = load_registry(config)
    window_checks: list[dict[str, Any]] = []
    stopped_reason = None
    for item in selected:
        allowed, window_check = batch_window_allows_start(config, respect_window, window_start, window_end)
        window_checks.append(window_check)
        if not allowed:
            stopped_reason = "BATCH_WINDOW_CLOSED"
            break
        log = encode_one(config, report_dirs, item)
        logs.append(log)
        if log.get("status") == "success":
            register_processed_source(config, registry, item, log, run_id)
            save_registry(config, registry)
            log["jellyfin_refresh"] = jellyfin_refresh_source(config, item["source_path"])
            safe_name = sanitize_filename(Path(item["source_path"]).stem)[:120]
            write_json(report_dirs["per_file"] / f"{safe_name}.json", log)
    finished_at = utc_now()
    summary = make_run_summary(run_id, started_at, finished_at, config_path, checked_items, selected, logs)
    summary = add_batch_estimates(summary, selected, config, stats)
    summary["batch_window_checks"] = window_checks
    summary["batch_stopped_reason"] = stopped_reason
    summary["number_of_selected_not_started"] = max(0, len(selected) - len(logs))
    write_reports(report_dirs, summary, checked_items, selected)
    print(f"Attempted {len(logs)} top encodes. Reports: {report_dirs['run']}")
    return 1 if any(log.get("status") == "failed" for log in logs) else 0


def run_distributed_init_command(config: dict[str, Any]) -> int:
    root = init_state(config)
    print(f"Initialized distributed state: {root}")
    return 0


def run_queue_status_command(config: dict[str, Any]) -> int:
    status = queue_status(config)
    print(json.dumps(status, indent=2, ensure_ascii=False))
    return 0


def run_queue_control_command(config: dict[str, Any], state: str, updated_by: str | None) -> int:
    allow_new_claims = state == "running"
    allow_finalizer = state != "maintenance"
    path = set_global_control(config, state, allow_new_claims, allow_finalizer, updated_by=updated_by)
    print(f"Updated global queue control: {path}")
    return 0


def run_node_control_command(
    config: dict[str, Any],
    node_id: str,
    worker_command: str | None,
    manager_command: str | None,
    production_command: str | None,
    updated_by: str | None,
) -> int:
    current = read_node_control(config, node_id)
    next_worker_command = current.get("worker_command")
    next_manager_command = current.get("manager_command")
    next_production_command = current.get("production_command")
    if worker_command is not None:
        next_worker_command = None if worker_command == "none" else worker_command
    if manager_command is not None:
        next_manager_command = None if manager_command == "none" else manager_command
    if production_command is not None:
        next_production_command = None if production_command == "none" else production_command
    if worker_command is not None or manager_command is not None or production_command is not None:
        set_node_control(config, node_id, worker_command=next_worker_command, manager_command=next_manager_command, production_command=next_production_command, updated_by=updated_by)
        current = read_node_control(config, node_id)
    print(json.dumps(current, indent=2, ensure_ascii=False))
    return 0


def run_enqueue_top_command(
    config: dict[str, Any],
    count: int,
    media_type: str | None,
    bucket: str | None,
    filesystem_limit: int | None,
    min_duration: float | None,
    max_duration: float | None,
    priority: str,
) -> int:
    _, selected, _ = plan_top_candidates(config, count, media_type, bucket, filesystem_limit, min_duration, max_duration)
    created = 0
    skipped_existing = 0
    for item in selected:
        job = build_job_payload(item, priority=priority)
        was_created, _ = enqueue_job(config, job)
        if was_created:
            created += 1
        else:
            skipped_existing += 1
    print(f"Enqueued {created} jobs. Existing/skipped: {skipped_existing}")
    return 0


def run_queue_claim_one_command(config: dict[str, Any], node_id: str | None) -> int:
    claimed = claim_next_job(config, node_id=node_id)
    if not claimed:
        print("No queued job available to claim")
        return 0
    job = claimed["job"]
    print(json.dumps({"claimed_path": claimed["path"], "job_id": job.get("job_id"), "source_path": job.get("source_path")}, indent=2, ensure_ascii=False))
    return 0


def run_worker_heartbeat_command(config: dict[str, Any], node_override: str | None) -> int:
    node = configured_node_id(config, node_override)
    path = write_worker_heartbeat(config, node, "idle", "manual_heartbeat")
    print(f"Wrote worker heartbeat: {path}")
    return 0


def run_worker_step_command(config: dict[str, Any], node_override: str | None, force: bool, dry_run_result: str, execute: bool, keep_failed_work_dir: bool = False) -> int:
    result = worker_step(config, node_override=node_override, force=force, dry_run_result=dry_run_result, execute=execute, keep_failed_work_dir=keep_failed_work_dir)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def run_worker_loop_command(
    config: dict[str, Any],
    node_override: str | None,
    force: bool,
    dry_run_result: str,
    execute: bool,
    keep_failed_work_dir: bool,
    max_iterations: int | None,
    idle_sleep_seconds: float | None,
    stop_on_idle: bool,
) -> int:
    result = worker_loop(
        config,
        node_override=node_override,
        force=force,
        dry_run_result=dry_run_result,
        execute=execute,
        keep_failed_work_dir=keep_failed_work_dir,
        max_iterations=max_iterations,
        idle_sleep_seconds=idle_sleep_seconds,
        stop_on_idle=stop_on_idle,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def run_lock_status_command(config: dict[str, Any]) -> int:
    print(json.dumps(lock_status(config), indent=2, ensure_ascii=False))
    return 0


def run_lock_acquire_command(config: dict[str, Any], lock_type: str, node_id: str, job_id: str) -> int:
    print(json.dumps(acquire_lock(config, lock_type, node_id, job_id), indent=2, ensure_ascii=False))
    return 0


def run_lock_release_command(path: Path) -> int:
    released = release_lock(path)
    print(json.dumps({"released": released, "path": str(path)}, indent=2, ensure_ascii=False))
    return 0


def run_recover_stale_jobs_command(config: dict[str, Any]) -> int:
    print(json.dumps(recover_stale_running_jobs(config), indent=2, ensure_ascii=False))
    return 0


def run_recover_stale_locks_command(config: dict[str, Any]) -> int:
    print(json.dumps(recover_stale_locks(config), indent=2, ensure_ascii=False))
    return 0


def run_requeue_interrupted_jobs_command(config: dict[str, Any], job_ids: list[str] | None, limit: int | None) -> int:
    print(json.dumps(requeue_interrupted_jobs(config, job_ids=job_ids, limit=limit), indent=2, ensure_ascii=False))
    return 0


def production_settings(config: dict[str, Any]) -> dict[str, Any]:
    defaults = {
        "enabled": False,
        "tick_seconds": 30,
        "enqueue": {
            "enabled": True,
            "priority": "production",
            "queue_target": 5,
            "queue_max": 10,
            "enqueue_count_per_tick": 2,
            "filesystem_limit": 300,
            "min_duration_seconds": 300,
            "max_duration_seconds": None,
            "media_types": ["anime", "series", "movie"],
            "strategy": "largest_first",
        },
        "finalizer": {"enabled": True, "max_finalize_per_tick": 2},
        "backpressure": {
            "max_ready_for_finalize_jobs": 5,
            "max_ready_outputs_gb": 20,
            "max_running_jobs": 3,
            "max_total_inflight_jobs": 10,
            "min_shared_state_free_gb": 0,
            "min_output_root_free_gb": 0,
        },
        "recovery": {"recover_stale_jobs": True, "recover_stale_locks": True, "requeue_interrupted_jobs": True},
        "safety": {
            "require_manager_execute": True,
            "require_jellyfin_enabled": True,
            "require_worker_disabled_on_manager_node": True,
            "require_successful_jellyfin_refresh": False,
        },
    }
    return merge_config(defaults, config.get("production") or {})


def configured_media_types_for_production(config: dict[str, Any]) -> set[str]:
    enqueue = production_settings(config).get("enqueue") or {}
    configured = enqueue.get("media_types") or list((config.get("libraries") or {}).keys())
    return {str(media_type) for media_type in configured}


def source_paths_in_job_states(config: dict[str, Any], states: tuple[str, ...] = JOB_STATES) -> set[str]:
    source_paths: set[str] = set()
    for state in states:
        for path in list_state_files(config, state):
            try:
                job = read_json(path)
            except (OSError, json.JSONDecodeError):
                continue
            source_path = job.get("source_path")
            if source_path:
                source_paths.add(normalized_source_path(Path(str(source_path))))
    return source_paths


def production_status_path(config: dict[str, Any], node_id: str) -> Path:
    return shared_state_dir(config) / "production" / f"{sanitize_filename(sanitize_node_id(node_id))}.json"


def read_production_status(config: dict[str, Any], node_id: str | None = None) -> dict[str, Any]:
    node = configured_manager_node_id(config, node_id)
    path = production_status_path(config, node)
    if not path.exists():
        return {}
    return read_json(path)


def write_production_status(config: dict[str, Any], node_id: str, payload: dict[str, Any]) -> Path:
    path = production_status_path(config, node_id)
    write_json_atomic(path, payload)
    return path


def production_safety_blockers(config: dict[str, Any], execute: bool) -> list[str]:
    settings = production_settings(config)
    safety = settings.get("safety") or {}
    roles = ((config.get("node") or {}).get("roles") or {})
    manager = config.get("manager") or {}
    worker = config.get("worker") or {}
    jellyfin = config.get("jellyfin") or {}
    blockers: list[str] = []
    if not bool(roles.get("manager", False)):
        blockers.append("manager role disabled")
    if not bool(manager.get("enabled", False)):
        blockers.append("manager disabled")
    if bool(safety.get("require_manager_execute", True)) and not bool(execute or manager.get("execute", False)):
        blockers.append("manager execute disabled")
    if bool(safety.get("require_jellyfin_enabled", True)) and not bool(jellyfin.get("enabled", False)):
        blockers.append("jellyfin disabled")
    if bool(safety.get("require_worker_disabled_on_manager_node", True)) and bool(worker.get("enabled", False)):
        blockers.append("worker enabled on manager node")
    return blockers


def production_backpressure(config: dict[str, Any], status: dict[str, Any], execute: bool) -> dict[str, Any]:
    settings = production_settings(config)
    enqueue = settings.get("enqueue") or {}
    limits = settings.get("backpressure") or {}
    states = status.get("states") or {}
    global_control = status.get("global_control") or {}
    queue_count = int(states.get("queue", 0))
    running_count = int(states.get("running", 0))
    ready_count = int(states.get("ready_for_finalize", 0))
    finalizing_count = int(states.get("finalizing", 0))
    inflight_count = queue_count + running_count + ready_count + finalizing_count
    ready_outputs_gb = float(status.get("ready_outputs_total_size_gb") or 0)
    reasons: list[str] = []
    if not bool(enqueue.get("enabled", True)):
        reasons.append("enqueue disabled")
    if queue_count >= int(enqueue.get("queue_max", 10)):
        reasons.append("queue is full")
    if ready_count >= int(limits.get("max_ready_for_finalize_jobs", 5)):
        reasons.append("too many ready_for_finalize jobs")
    if ready_outputs_gb >= float(limits.get("max_ready_outputs_gb", 20)):
        reasons.append("ready_outputs size limit exceeded")
    if running_count >= int(limits.get("max_running_jobs", 3)):
        reasons.append("too many running jobs")
    if inflight_count >= int(limits.get("max_total_inflight_jobs", 10)):
        reasons.append("too many inflight jobs")
    min_shared_state_free_gb = float(limits.get("min_shared_state_free_gb", 0) or 0)
    if min_shared_state_free_gb > 0:
        try:
            shared_free_gb = shutil.disk_usage(shared_state_dir(config)).free / 1024 / 1024 / 1024
            if shared_free_gb < min_shared_state_free_gb:
                reasons.append("shared state free space below limit")
        except OSError:
            reasons.append("shared state free space unavailable")
    min_output_root_free_gb = float(limits.get("min_output_root_free_gb", 0) or 0)
    if min_output_root_free_gb > 0:
        try:
            output_root = Path(str(config.get("output_root")))
            output_root.mkdir(parents=True, exist_ok=True)
            output_free_gb = shutil.disk_usage(output_root).free / 1024 / 1024 / 1024
            if output_free_gb < min_output_root_free_gb:
                reasons.append("output root free space below limit")
        except OSError:
            reasons.append("output root free space unavailable")
    queue_state = str(global_control.get("queue_state") or "running")
    if queue_state == "paused":
        reasons.append("global queue paused")
    if queue_state == "maintenance":
        reasons.append("global queue maintenance")
    if not bool(global_control.get("allow_new_claims", True)):
        reasons.append("new claims disabled")
    reasons.extend(production_safety_blockers(config, execute))
    return {
        "allowed": not reasons,
        "blocked_reasons": [f"enqueue blocked: {reason}" for reason in reasons],
        "counts": {
            "queue": queue_count,
            "running": running_count,
            "ready_for_finalize": ready_count,
            "finalizing": finalizing_count,
            "inflight": inflight_count,
        },
        "limits": {
            "queue_max": int(enqueue.get("queue_max", 10)),
            "max_ready_for_finalize_jobs": int(limits.get("max_ready_for_finalize_jobs", 5)),
            "max_ready_outputs_gb": float(limits.get("max_ready_outputs_gb", 20)),
            "max_running_jobs": int(limits.get("max_running_jobs", 3)),
            "max_total_inflight_jobs": int(limits.get("max_total_inflight_jobs", 10)),
            "min_shared_state_free_gb": min_shared_state_free_gb,
            "min_output_root_free_gb": min_output_root_free_gb,
        },
    }


def production_enqueue_once(config: dict[str, Any], max_count: int, priority: str) -> dict[str, Any]:
    media_types = configured_media_types_for_production(config)
    settings = production_settings(config)
    enqueue = settings.get("enqueue") or {}
    excluded = source_paths_in_job_states(config)
    media_type_filter = next(iter(media_types)) if len(media_types) == 1 else None
    filesystem_limit = enqueue.get("filesystem_limit")
    checked, selected, stats = plan_top_candidates(
        config,
        max_count,
        media_type_filter=media_type_filter,
        bucket_filter=None,
        filesystem_limit=int(filesystem_limit) if filesystem_limit is not None else None,
        min_duration=float(enqueue.get("min_duration_seconds")) if enqueue.get("min_duration_seconds") is not None else None,
        max_duration=float(enqueue.get("max_duration_seconds")) if enqueue.get("max_duration_seconds") is not None else None,
        exclude_source_paths=excluded,
        dedupe_sampling_groups=False,
        media_type_filters=media_types,
    )
    created = 0
    skipped_existing = 0
    enqueued: list[dict[str, Any]] = []
    for item in selected[:max_count]:
        job = build_job_payload(item, priority=priority, created_by="production")
        was_created, path = enqueue_job(config, job)
        if was_created:
            created += 1
            enqueued.append({"job_id": job.get("job_id"), "source_path": job.get("source_path"), "job_path": str(path)})
        else:
            skipped_existing += 1
    return {
        "created_count": created,
        "skipped_existing_count": skipped_existing,
        "enqueued": enqueued,
        "checked_count": len(checked),
        "selected_count": len(selected),
        "planning_stats": stats,
    }


def production_tick(
    config: dict[str, Any],
    node_override: str | None = None,
    execute: bool = False,
    enqueue_enabled: bool = True,
    finalizer_enabled: bool = True,
) -> dict[str, Any]:
    node = configured_manager_node_id(config, node_override)
    settings = production_settings(config)
    control = read_node_control(config, node)
    command = control.get("production_command")
    state = "running"
    phase = "checking"
    last_error = None
    recovery_result: dict[str, Any] = {}
    finalize_results: list[dict[str, Any]] = []
    enqueue_result: dict[str, Any] = {"created_count": 0, "skipped_existing_count": 0, "enqueued": []}
    blocked_reasons: list[str] = []

    try:
        init_state(config)
        status_before = queue_status(config)
        if not bool(settings.get("enabled", False)):
            state = "paused"
            phase = "disabled"
            blocked_reasons = ["production disabled"]
        elif command in {"paused", "stop_after_current"}:
            state = "paused"
            phase = str(command)
            blocked_reasons = [f"production {command}"]
        elif command == "maintenance" or str((status_before.get("global_control") or {}).get("queue_state") or "") == "maintenance":
            state = "paused"
            phase = "maintenance"
            blocked_reasons = ["production maintenance"]
        else:
            recovery = settings.get("recovery") or {}
            phase = "recovering"
            if bool(recovery.get("recover_stale_jobs", True)):
                recovery_result["stale_jobs"] = recover_stale_running_jobs(config)
            if bool(recovery.get("recover_stale_locks", True)):
                recovery_result["stale_locks"] = recover_stale_locks(config)
            if bool(recovery.get("requeue_interrupted_jobs", True)):
                recovery_result["interrupted"] = requeue_interrupted_jobs(config, limit=1)

            finalizer = settings.get("finalizer") or {}
            if finalizer_enabled and bool(finalizer.get("enabled", True)):
                phase = "finalizing"
                for _ in range(max(int(finalizer.get("max_finalize_per_tick", 2)), 0)):
                    result = manager_step(config, node_override=node, force=False, dry_run_result="done", execute=execute)
                    finalize_results.append(result)
                    if result.get("status") in {"idle", "skipped"}:
                        break

            status_after_finalize = queue_status(config)
            backpressure = production_backpressure(config, status_after_finalize, execute)
            blocked_reasons = backpressure["blocked_reasons"]
            if enqueue_enabled and backpressure["allowed"]:
                enqueue = settings.get("enqueue") or {}
                queue_target = int(enqueue.get("queue_target", 5))
                queue_count = int((status_after_finalize.get("states") or {}).get("queue", 0))
                enqueue_budget = max(queue_target - queue_count, 0)
                per_tick = int(enqueue.get("enqueue_count_per_tick", 2))
                enqueue_count = min(max(enqueue_budget, 0), max(per_tick, 0))
                if enqueue_count > 0:
                    phase = "enqueueing"
                    enqueue_result = production_enqueue_once(config, enqueue_count, str(enqueue.get("priority") or "production"))
                else:
                    phase = "idle"
            else:
                state = "blocked" if blocked_reasons else "idle"
                phase = "blocked" if blocked_reasons else "idle"

        final_status = queue_status(config)
        final_backpressure = production_backpressure(config, final_status, execute)
        if final_backpressure["blocked_reasons"] and state == "running":
            state = "blocked"
        payload = {
            "node_id": node,
            "production_state": state,
            "current_phase": phase,
            "last_tick_at": utc_now(),
            "last_enqueue_count": int(enqueue_result.get("created_count", 0)),
            "last_finalize_count": len([item for item in finalize_results if item.get("status") not in {"idle", "skipped"}]),
            "blocked_reasons": blocked_reasons or final_backpressure["blocked_reasons"],
            "state_counts": final_status.get("states") or {},
            "ready_outputs_total_size_gb": final_status.get("ready_outputs_total_size_gb", 0),
            "ready_outputs_dir_count": final_status.get("ready_outputs_dir_count", 0),
            "limits": final_backpressure.get("limits") or {},
            "last_error": last_error,
            "recovery": recovery_result,
            "finalize_results": finalize_results,
            "enqueue_result": enqueue_result,
            "control": control,
        }
    except Exception as exc:
        payload = {
            "node_id": node,
            "production_state": "error",
            "current_phase": phase,
            "last_tick_at": utc_now(),
            "last_enqueue_count": 0,
            "last_finalize_count": len([item for item in finalize_results if item.get("status") not in {"idle", "skipped"}]),
            "blocked_reasons": blocked_reasons,
            "state_counts": (queue_status(config).get("states") if config else {}) or {},
            "ready_outputs_total_size_gb": 0,
            "ready_outputs_dir_count": 0,
            "limits": {},
            "last_error": str(exc),
            "recovery": recovery_result,
            "finalize_results": finalize_results,
            "enqueue_result": enqueue_result,
            "control": control,
        }
    write_production_status(config, node, payload)
    return payload


def production_loop(
    config: dict[str, Any],
    node_override: str | None = None,
    execute: bool = False,
    max_iterations: int | None = None,
    tick_seconds: float | None = None,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    settings = production_settings(config)
    effective_tick_seconds = float(tick_seconds if tick_seconds is not None else settings.get("tick_seconds", 30))
    if effective_tick_seconds < 0:
        raise ValueError("tick_seconds must be non-negative")
    if max_iterations is not None and int(max_iterations) <= 0:
        raise ValueError("max_iterations must be greater than 0")
    sleep_fn = sleeper or time.sleep
    config_path = Path(str(config.get("__config_path"))) if config.get("__config_path") else None
    profile = config.get("__selected_profile")
    results: list[dict[str, Any]] = []
    iterations = 0
    while True:
        tick_config = load_config(config_path, profile) if config_path else config
        result = production_tick(tick_config, node_override=node_override, execute=execute)
        results.append(result)
        iterations += 1
        if max_iterations is not None and iterations >= int(max_iterations):
            break
        sleep_fn(effective_tick_seconds)
    return {
        "status": "loop_complete",
        "iterations": iterations,
        "tick_seconds": effective_tick_seconds,
        "max_iterations": max_iterations,
        "last_result": results[-1] if results else None,
        "results": results,
    }


def run_production_loop_command(config: dict[str, Any], node_override: str | None, execute: bool, max_iterations: int | None, tick_seconds: float | None) -> int:
    print(json.dumps(production_loop(config, node_override=node_override, execute=execute, max_iterations=max_iterations, tick_seconds=tick_seconds), indent=2, ensure_ascii=False))
    return 0


def maintenance_loop(
    config: dict[str, Any],
    max_iterations: int | None = None,
    idle_sleep_seconds: float | None = None,
    stop_on_idle: bool = False,
    sleeper: Callable[[float], None] | None = None,
) -> dict[str, Any]:
    settings = config.get("maintenance") or {}
    effective_max_iterations = max_iterations if max_iterations is not None else settings.get("max_iterations")
    if effective_max_iterations is not None:
        effective_max_iterations = int(effective_max_iterations)
        if effective_max_iterations <= 0:
            raise ValueError("max_iterations must be greater than 0")
    effective_idle_sleep_seconds = float(idle_sleep_seconds if idle_sleep_seconds is not None else settings.get("idle_sleep_seconds", 30.0))
    if effective_idle_sleep_seconds < 0:
        raise ValueError("idle_sleep_seconds must be non-negative")

    sleep_fn = sleeper or time.sleep
    results: list[dict[str, Any]] = []
    slept_intervals: list[float] = []
    stop_reason = "completed"

    while True:
        stale_jobs = recover_stale_running_jobs(config)
        stale_locks = recover_stale_locks(config)
        result = {
            "recovered_count": int(stale_jobs.get("recovered_count", 0)) + int(stale_locks.get("recovered_count", 0)),
            "job_recovered_count": int(stale_jobs.get("recovered_count", 0)),
            "lock_recovered_count": int(stale_locks.get("recovered_count", 0)),
            "stale_jobs": stale_jobs,
            "stale_locks": stale_locks,
        }
        results.append(result)

        if effective_max_iterations is not None and len(results) >= effective_max_iterations:
            stop_reason = "max_iterations"
            break

        if int(result.get("recovered_count", 0)) == 0:
            if stop_on_idle:
                stop_reason = "idle"
                break
            sleep_fn(effective_idle_sleep_seconds)
            slept_intervals.append(effective_idle_sleep_seconds)
            continue

    return {
        "status": "loop_complete",
        "iterations": len(results),
        "stop_reason": stop_reason,
        "stop_on_idle": stop_on_idle,
        "idle_sleep_seconds": effective_idle_sleep_seconds,
        "max_iterations": effective_max_iterations,
        "recovered_count": sum(int(item.get("recovered_count", 0)) for item in results),
        "job_recovered_count": sum(int(item.get("job_recovered_count", 0)) for item in results),
        "lock_recovered_count": sum(int(item.get("lock_recovered_count", 0)) for item in results),
        "sleep_calls": len(slept_intervals),
        "slept_intervals": slept_intervals,
        "last_result": results[-1] if results else None,
        "results": results,
    }


def run_maintenance_loop_command(
    config: dict[str, Any],
    max_iterations: int | None,
    idle_sleep_seconds: float | None,
    stop_on_idle: bool,
) -> int:
    print(json.dumps(maintenance_loop(config, max_iterations=max_iterations, idle_sleep_seconds=idle_sleep_seconds, stop_on_idle=stop_on_idle), indent=2, ensure_ascii=False))
    return 0


def run_manager_heartbeat_command(config: dict[str, Any], node_override: str | None) -> int:
    node = configured_manager_node_id(config, node_override)
    path = write_manager_heartbeat(config, node, "idle", "manual_heartbeat")
    print(f"Wrote manager heartbeat: {path}")
    return 0


def run_manager_step_command(config: dict[str, Any], node_override: str | None, force: bool, dry_run_result: str, execute: bool) -> int:
    result = manager_step(config, node_override=node_override, force=force, dry_run_result=dry_run_result, execute=execute)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def run_manager_loop_command(
    config: dict[str, Any],
    node_override: str | None,
    force: bool,
    dry_run_result: str,
    execute: bool,
    max_iterations: int | None,
    idle_sleep_seconds: float | None,
    stop_on_idle: bool,
) -> int:
    result = manager_loop(
        config,
        node_override=node_override,
        force=force,
        dry_run_result=dry_run_result,
        execute=execute,
        max_iterations=max_iterations,
        idle_sleep_seconds=idle_sleep_seconds,
        stop_on_idle=stop_on_idle,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


def run_node_command(
    config: dict[str, Any],
    node_override: str | None,
    worker_dry_run_result: str,
    manager_dry_run_result: str,
    worker_execute: bool,
    execute: bool,
    max_iterations: int | None,
    idle_sleep_seconds: float | None,
    stop_on_idle: bool,
) -> int:
    roles = ((config.get("node") or {}).get("roles") or {})
    worker_cfg = config.get("worker") or {}
    manager_cfg = config.get("manager") or {}
    maintenance_cfg = config.get("maintenance") or {}
    services: list[tuple[str, Any, dict[str, Any]]] = []
    if bool(roles.get("worker", False)) and bool(worker_cfg.get("run_continuously", False)):
        services.append(
            (
                "worker",
                worker_loop,
                {
                    "config": config,
                    "node_override": node_override,
                    "force": False,
                    "dry_run_result": worker_dry_run_result,
                    "execute": worker_execute,
                    "max_iterations": max_iterations,
                    "idle_sleep_seconds": idle_sleep_seconds,
                    "stop_on_idle": stop_on_idle,
                },
            )
        )
    if bool(roles.get("manager", False)) and bool(manager_cfg.get("run_continuously", False)):
        services.append(
            (
                "manager",
                manager_loop,
                {
                    "config": config,
                    "node_override": node_override,
                    "force": False,
                    "dry_run_result": manager_dry_run_result,
                    "execute": execute,
                    "max_iterations": max_iterations,
                    "idle_sleep_seconds": idle_sleep_seconds,
                    "stop_on_idle": stop_on_idle,
                },
            )
        )
    if bool(maintenance_cfg.get("run_continuously", False)):
        services.append(
            (
                "maintenance",
                maintenance_loop,
                {
                    "config": config,
                    "max_iterations": max_iterations,
                    "idle_sleep_seconds": idle_sleep_seconds,
                    "stop_on_idle": stop_on_idle,
                },
            )
        )

    if not services:
        print(
            json.dumps(
                {
                    "status": "skipped",
                    "reason": "no_continuous_services_enabled",
                    "node_id": configured_node_id(config, node_override),
                    "roles": {
                        "web_ui": bool(roles.get("web_ui", False)),
                        "worker": bool(roles.get("worker", False)),
                        "manager": bool(roles.get("manager", False)),
                    },
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return 0

    results: dict[str, Any] = {}
    errors: dict[str, str] = {}
    threads: list[threading.Thread] = []

    def run_service(service_name: str, service_fn: Any, kwargs: dict[str, Any]) -> None:
        try:
            results[service_name] = service_fn(**kwargs)
        except Exception as exc:
            errors[service_name] = str(exc)

    for service_name, service_fn, kwargs in services:
        thread = threading.Thread(target=run_service, args=(service_name, service_fn, kwargs), name=f"autoripper-{service_name}")
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    payload = {
        "status": "complete" if not errors else "error",
        "node_id": configured_node_id(config, node_override),
        "started_services": [service_name for service_name, _, _ in services],
        "results": results,
        "errors": errors,
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    if errors:
        return 2
    return 0


def run_web_ui_command(config: dict[str, Any], host: str | None, port: int | None) -> int:
    return run_web_ui_server(config, host=host, port=port)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safe media normalization test pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("scan", "dry-run", "test", "test-full", "test-clips", "track-audit"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--config", required=True, type=Path)
        subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
        subparser.add_argument("--seed", type=int, default=None)
        subparser.add_argument("--limit", type=int, default=None, help="Limit scanned video files for development smoke tests")
    for command in ("plan-top", "batch-top"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--config", required=True, type=Path)
        subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
        subparser.add_argument("--count", type=int, default=None, help="Number of largest eligible files to select")
        subparser.add_argument("--media-type", choices=["anime", "series", "movie", "unknown"], default=None)
        subparser.add_argument("--bucket", default=None, help="Bucket filter, e.g. high, anime_high, medium, huge")
        subparser.add_argument("--filesystem-limit", type=int, default=None, help="Only ffprobe within the first N largest filesystem candidates")
        subparser.add_argument("--min-duration", type=float, default=None, help="Minimum source duration in seconds after ffprobe validation")
        subparser.add_argument("--max-duration", type=float, default=None, help="Maximum source duration in seconds after ffprobe validation")
        if command == "batch-top":
            subparser.add_argument("--respect-window", action="store_true", help="Only start new encodes inside the configured processing window")
            subparser.add_argument("--window-start", default=None, help="Override processing window start time, HH:MM")
            subparser.add_argument("--window-end", default=None, help="Override processing window end time, HH:MM")
    for command in ("distributed-init", "queue-status", "recover-stale-jobs", "recover-stale-locks", "requeue-interrupted-jobs"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--config", required=True, type=Path)
        subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
        if command == "requeue-interrupted-jobs":
            subparser.add_argument("--job-id", action="append", default=None, help="Only requeue the specified interrupted job id; may be repeated")
            subparser.add_argument("--limit", type=int, default=None, help="Maximum number of interrupted jobs to requeue")
    subparser = subparsers.add_parser("web-ui")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--host", default=None, help="Override bind host for the local web UI")
    subparser.add_argument("--port", type=int, default=None, help="Override bind port for the local web UI")
    subparser = subparsers.add_parser("queue-control")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--state", choices=["running", "paused", "maintenance"], required=True)
    subparser.add_argument("--updated-by", default=None, help="Node/user label written into global control")
    subparser = subparsers.add_parser("node-control")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", required=True, help="Target node id for per-node loop commands")
    subparser.add_argument("--worker-command", choices=["stop_after_current", "none"], default=None, help="Set or clear the worker command for this node")
    subparser.add_argument("--manager-command", choices=["stop_after_current", "none"], default=None, help="Set or clear the manager command for this node")
    subparser.add_argument("--production-command", choices=["running", "paused", "stop_after_current", "maintenance", "none"], default=None, help="Set or clear the production command for this node")
    subparser.add_argument("--updated-by", default=None, help="Node/user label written into node control")
    subparser = subparsers.add_parser("enqueue-top")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--count", type=int, default=None, help="Number of largest eligible files to enqueue")
    subparser.add_argument("--media-type", choices=["anime", "series", "movie", "unknown"], default=None)
    subparser.add_argument("--bucket", default=None, help="Bucket filter, e.g. high, anime_high, medium, huge")
    subparser.add_argument("--filesystem-limit", type=int, default=None, help="Only ffprobe within the first N largest filesystem candidates")
    subparser.add_argument("--min-duration", type=float, default=None, help="Minimum source duration in seconds after ffprobe validation")
    subparser.add_argument("--max-duration", type=float, default=None, help="Maximum source duration in seconds after ffprobe validation")
    subparser.add_argument("--priority", default="normal", help="Queue priority label stored in the job JSON")
    subparser = subparsers.add_parser("queue-claim-one")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Node id to stamp on the claimed job")
    subparser = subparsers.add_parser("worker-heartbeat")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser = subparsers.add_parser("worker-step")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser.add_argument("--force", action="store_true", help="Ignore local worker enabled/schedule/global pause checks for development testing")
    subparser.add_argument("--dry-run-result", choices=["requeue", "ready", "failed"], default="requeue", help="Where to move the claimed job after a dry-run worker step")
    subparser.add_argument("--execute", action="store_true", help="Use real local ffmpeg encode into the ready bundle; requires --dry-run-result ready")
    subparser.add_argument("--keep-failed-work-dir", "--preserve-failed-output", dest="keep_failed_work_dir", action="store_true", help="Keep the local worker work dir when execute-mode verification fails after producing output.mkv")
    subparser = subparsers.add_parser("worker-loop")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser.add_argument("--force", action="store_true", help="Ignore local worker enabled/schedule/global pause checks for development testing")
    subparser.add_argument("--dry-run-result", choices=["requeue", "ready", "failed"], default="requeue", help="Where to move the claimed job after a dry-run worker step")
    subparser.add_argument("--execute", action="store_true", help="Use real local ffmpeg encode into the ready bundle; requires --dry-run-result ready")
    subparser.add_argument("--keep-failed-work-dir", "--preserve-failed-output", dest="keep_failed_work_dir", action="store_true", help="Keep the local worker work dir when execute-mode verification fails after producing output.mkv")
    subparser.add_argument("--max-iterations", type=int, default=None, help="Optional hard cap on worker loop iterations")
    subparser.add_argument("--idle-sleep-seconds", type=float, default=None, help="Sleep interval used after idle or gated iterations")
    subparser.add_argument("--stop-on-idle", action="store_true", help="Stop the loop after the first no_job_available result")
    subparser = subparsers.add_parser("node-run")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser.add_argument("--worker-dry-run-result", choices=["requeue", "ready", "failed"], default="requeue", help="Dry-run result used when starting the worker loop")
    subparser.add_argument("--manager-dry-run-result", choices=["done", "failed_finalize", "requeue"], default="done", help="Dry-run result used when starting the manager loop")
    subparser.add_argument("--worker-execute", action="store_true", help="Use real local ffmpeg encode for the worker loop; requires --worker-dry-run-result ready")
    subparser.add_argument("--execute", action="store_true", help="Allow manager loop finalization in execute mode")
    subparser.add_argument("--max-iterations", type=int, default=None, help="Optional hard cap applied to started loops")
    subparser.add_argument("--idle-sleep-seconds", type=float, default=None, help="Sleep interval used after idle or gated iterations")
    subparser.add_argument("--stop-on-idle", action="store_true", help="Stop started loops after their first idle result")
    subparser = subparsers.add_parser("lock-status")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser = subparsers.add_parser("lock-acquire")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--lock-type", choices=["nas_read", "nas_write", "active_encode", "finalizer"], required=True)
    subparser.add_argument("--node-id", required=True)
    subparser.add_argument("--job-id", required=True)
    subparser = subparsers.add_parser("lock-release")
    subparser.add_argument("--path", required=True, type=Path)
    subparser = subparsers.add_parser("maintenance-loop")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--max-iterations", type=int, default=None, help="Optional hard cap on maintenance loop iterations")
    subparser.add_argument("--idle-sleep-seconds", type=float, default=None, help="Sleep interval used after idle maintenance iterations")
    subparser.add_argument("--stop-on-idle", action="store_true", help="Stop the loop after the first maintenance iteration with no stale recovery")
    subparser = subparsers.add_parser("manager-heartbeat")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser = subparsers.add_parser("manager-step")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser.add_argument("--force", action="store_true", help="Ignore manager enabled/global finalizer checks for development testing")
    subparser.add_argument("--dry-run-result", choices=["done", "failed_finalize", "requeue"], default="done", help="Where to move the finalizing job after a dry-run manager step")
    subparser.add_argument("--execute", action="store_true", help="Actually quarantine the original source and move the ready output into the library")
    subparser = subparsers.add_parser("manager-loop")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured node id")
    subparser.add_argument("--force", action="store_true", help="Ignore manager enabled/global finalizer checks for development testing")
    subparser.add_argument("--dry-run-result", choices=["done", "failed_finalize", "requeue"], default="done", help="Where to move the finalizing job after a dry-run manager step")
    subparser.add_argument("--execute", action="store_true", help="Actually quarantine the original source and move the ready output into the library")
    subparser.add_argument("--max-iterations", type=int, default=None, help="Optional hard cap on manager loop iterations")
    subparser.add_argument("--idle-sleep-seconds", type=float, default=None, help="Sleep interval used after idle or gated iterations")
    subparser.add_argument("--stop-on-idle", action="store_true", help="Stop the loop after the first no_ready_job_available result")
    subparser = subparsers.add_parser("production-loop")
    subparser.add_argument("--config", required=True, type=Path)
    subparser.add_argument("--profile", default=None, help="Optional config profile from the YAML file")
    subparser.add_argument("--node-id", default=None, help="Override configured manager node id")
    subparser.add_argument("--execute", action="store_true", help="Allow production finalizer to execute file replacement")
    subparser.add_argument("--max-iterations", type=int, default=None, help="Optional hard cap for tests")
    subparser.add_argument("--tick-seconds", type=float, default=None, help="Override production tick interval")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "lock-release":
            return run_lock_release_command(args.path)
        config = load_config(args.config, args.profile)
        if args.command == "scan":
            return run_scan_command(args.config, config, args.limit)
        if args.command == "track-audit":
            return run_track_audit_command(args.config, config, args.limit)
        if args.command == "dry-run":
            return run_dry_run_command(args.config, config, args.seed, args.limit)
        if args.command in {"test", "test-full"}:
            return run_test_command(args.config, config, args.seed, clips=False, limit=args.limit)
        if args.command == "test-clips":
            return run_test_command(args.config, config, args.seed, clips=True, limit=args.limit)
        if args.command == "plan-top":
            count = args.count or int(batch_settings(config).get("default_count", 20))
            return run_plan_top_command(args.config, config, count, args.media_type, args.bucket, args.filesystem_limit, args.min_duration, args.max_duration)
        if args.command == "batch-top":
            count = args.count or int(batch_settings(config).get("default_count", 20))
            return run_batch_top_command(
                args.config,
                config,
                count,
                args.media_type,
                args.bucket,
                args.filesystem_limit,
                args.min_duration,
                args.max_duration,
                args.respect_window,
                args.window_start,
                args.window_end,
            )
        if args.command == "distributed-init":
            return run_distributed_init_command(config)
        if args.command == "queue-status":
            return run_queue_status_command(config)
        if args.command == "recover-stale-jobs":
            return run_recover_stale_jobs_command(config)
        if args.command == "recover-stale-locks":
            return run_recover_stale_locks_command(config)
        if args.command == "requeue-interrupted-jobs":
            return run_requeue_interrupted_jobs_command(config, args.job_id, args.limit)
        if args.command == "web-ui":
            return run_web_ui_command(config, args.host, args.port)
        if args.command == "queue-control":
            return run_queue_control_command(config, args.state, args.updated_by)
        if args.command == "node-control":
            return run_node_control_command(config, args.node_id, args.worker_command, args.manager_command, args.production_command, args.updated_by)
        if args.command == "enqueue-top":
            count = args.count or int(batch_settings(config).get("default_count", 20))
            return run_enqueue_top_command(config, count, args.media_type, args.bucket, args.filesystem_limit, args.min_duration, args.max_duration, args.priority)
        if args.command == "queue-claim-one":
            return run_queue_claim_one_command(config, args.node_id)
        if args.command == "worker-heartbeat":
            return run_worker_heartbeat_command(config, args.node_id)
        if args.command == "worker-step":
            return run_worker_step_command(config, args.node_id, args.force, args.dry_run_result, args.execute, args.keep_failed_work_dir)
        if args.command == "worker-loop":
            return run_worker_loop_command(config, args.node_id, args.force, args.dry_run_result, args.execute, args.keep_failed_work_dir, args.max_iterations, args.idle_sleep_seconds, args.stop_on_idle)
        if args.command == "node-run":
            return run_node_command(config, args.node_id, args.worker_dry_run_result, args.manager_dry_run_result, args.worker_execute, args.execute, args.max_iterations, args.idle_sleep_seconds, args.stop_on_idle)
        if args.command == "lock-status":
            return run_lock_status_command(config)
        if args.command == "lock-acquire":
            return run_lock_acquire_command(config, args.lock_type, args.node_id, args.job_id)
        if args.command == "maintenance-loop":
            return run_maintenance_loop_command(config, args.max_iterations, args.idle_sleep_seconds, args.stop_on_idle)
        if args.command == "manager-heartbeat":
            return run_manager_heartbeat_command(config, args.node_id)
        if args.command == "manager-step":
            return run_manager_step_command(config, args.node_id, args.force, args.dry_run_result, args.execute)
        if args.command == "manager-loop":
            return run_manager_loop_command(config, args.node_id, args.force, args.dry_run_result, args.execute, args.max_iterations, args.idle_sleep_seconds, args.stop_on_idle)
        if args.command == "production-loop":
            return run_production_loop_command(config, args.node_id, args.execute, args.max_iterations, args.tick_seconds)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    raise SystemExit(main())