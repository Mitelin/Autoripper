from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib import parse, request

import yaml


APP_NAME = "SimpleRipper"
VIDEO_EXTENSIONS = {".mkv", ".mp4", ".avi", ".mov", ".m4v", ".ts"}
LANGUAGE_ALIASES = {
    "cze": {"cze", "ces", "cz", "czech", "cestina", "cesky", "cz dabing", "czech dub"},
    "slo": {"slo", "slk", "sk", "slovak", "slovencina", "sk dabing", "slovak dub"},
    "eng": {"eng", "en", "english", "anglicky", "anglictina", "english dub", "en dub"},
    "jpn": {"jpn", "ja", "jp", "japanese", "japonstina", "nihongo"},
}


class ForceStopRequested(RuntimeError):
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
    try:
        for line in iter(stream.readline, ""):
            parsed = parse_ffmpeg_progress_line(line)
            if not parsed:
                continue
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
    if not policy.get("enabled", True):
        return {"applied": False, "map_arguments": ["-map", "0"], "expected_audio_stream_count": source.get("audio_stream_count"), "expected_subtitle_stream_count": source.get("subtitle_stream_count")}
    target_languages = set(policy.get("target_audio_languages") or ["cze"])
    audio_streams = source.get("audio_streams") or []
    target_audio = [stream for stream in audio_streams if detect_language(stream)[0] in target_languages]
    if not target_audio or len(audio_streams) <= 1 or not policy.get("drop_other_audio_if_target_found", True):
        return {"applied": False, "map_arguments": ["-map", "0"], "expected_audio_stream_count": source.get("audio_stream_count"), "expected_subtitle_stream_count": source.get("subtitle_stream_count")}
    args = ["-map", "0:v:0"]
    for stream in target_audio:
        args.extend(["-map", f"0:{stream['index']}"])
    if policy.get("keep_subtitles", True):
        args.extend(["-map", "0:s?"])
    args.extend(["-map", "0:t?"])
    return {"applied": True, "map_arguments": args, "expected_audio_stream_count": len(target_audio), "expected_subtitle_stream_count": source.get("subtitle_stream_count")}


def build_ffmpeg_command(config: dict[str, Any], source: Path, output: Path, metadata: dict[str, Any], stream_policy: dict[str, Any]) -> list[str]:
    settings = (config.get("quality_profiles") or {}).get(metadata.get("media_type") or "default") or (config.get("quality_profiles") or {}).get("default") or {}
    command = [str((config.get("tools") or {}).get("ffmpeg") or "ffmpeg"), "-hide_banner", "-nostats", "-progress", "pipe:1", "-y", "-i", str(source)]
    command.extend(stream_policy.get("map_arguments") or ["-map", "0"])
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


def marker_path(source: Path, config: dict[str, Any]) -> Path:
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


def _pick_folder_with_windows_dialog(initial_dir: str | None = None) -> str | None:
    selected = str(initial_dir or "").strip()
    script = "\n".join([
        "$shell = New-Object -ComObject Shell.Application",
        "$title = 'Vyberte slozku pro SimpleRipper'",
        "$flags = 0x0001 + 0x0040 + 0x0200 + 0x8000",
        f"$initial = @'\n{selected}\n'@",
        "$root = if ($initial) { $initial } else { 0 }",
        "$dialog = $shell.BrowseForFolder(0, $title, $flags, $root)",
        "if ($dialog -and $dialog.Self -and $dialog.Self.Path) {",
        "  [Console]::OutputEncoding = [System.Text.Encoding]::UTF8",
        "  Write-Output $dialog.Self.Path",
        "}",
    ])
    completed = subprocess.run(
        ["powershell.exe", "-NoProfile", "-STA", "-Command", script],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError((completed.stderr or completed.stdout or "Folder picker failed").strip())
    folder = (completed.stdout or "").strip()
    return folder or None


def _pick_folder_with_linux_dialog(initial_dir: str | None = None) -> str | None:
    selected = str(initial_dir or "").strip()
    commands: list[list[str]] = []
    if shutil.which("zenity"):
        command = ["zenity", "--file-selection", "--directory", "--title=Vyberte slozku pro SimpleRipper"]
        if selected:
            command.append(f"--filename={selected.rstrip('/')}" + "/")
        commands.append(command)
    if shutil.which("kdialog"):
        command = ["kdialog", "--getexistingdirectory"]
        if selected:
            command.append(selected)
        command.append("/")
        commands.append(command)
    if shutil.which("qarma"):
        command = ["qarma", "--file-selection", "--directory", "--title=Vyberte slozku pro SimpleRipper"]
        if selected:
            command.append(f"--filename={selected.rstrip('/')}" + "/")
        commands.append(command)
    if shutil.which("yad"):
        command = ["yad", "--file-selection", "--directory", "--title=Vyberte slozku pro SimpleRipper"]
        if selected:
            command.append(f"--filename={selected.rstrip('/')}" + "/")
        commands.append(command)

    last_error = ""
    for command in commands:
        completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if completed.returncode == 0:
            folder = (completed.stdout or "").strip()
            return folder or None
        if completed.returncode not in {1}:
            last_error = (completed.stderr or completed.stdout or "Folder picker failed").strip()
    if last_error:
        raise RuntimeError(last_error)
    raise RuntimeError("No supported Linux folder dialog found. Pouzijte manualni vlozeni cesty.")


def pick_folder_dialog(initial_dir: str | None = None) -> str | None:
    if os.name == "nt":
        try:
            return _pick_folder_with_windows_dialog(initial_dir)
        except Exception:
            pass
    else:
        try:
            return _pick_folder_with_linux_dialog(initial_dir)
        except Exception:
            pass
    try:
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        try:
            folder = filedialog.askdirectory(title="Vyberte slozku pro SimpleRipper", initialdir=initial_dir or None)
        finally:
            root.destroy()
        folder_text = str(folder).strip()
        return folder_text or None
    except Exception as exc:
        raise RuntimeError(f"Folder picker failed: {exc}. Pouzijte manualni vlozeni cesty.") from exc


def scan_candidates(folders: list[Path], config: dict[str, Any]) -> list[Path]:
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
            if path.is_file() and path.suffix.lower() in extensions and not marker_path(path, config).exists() and not source_lock_path(path, config).exists() and not is_history_done_for_current_source(config, path) and not path.name.endswith((".original", ".tmp", ".partial")):
                candidates.append(path)
    return sorted(candidates, key=lambda item: item.stat().st_size, reverse=True)


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
    reason = skip_reason(config, metadata)
    score = candidate_priority_score(metadata)
    if should_reprocess_hevc(config, metadata):
        score += 35.0
    return {"path": source, "status": "ok", "metadata": metadata, "skip_reason": reason, "score": score}


def candidate_selection_key(candidate: dict[str, Any]) -> tuple[int, float]:
    metadata = candidate.get("metadata") or {}
    size_bytes = int(metadata.get("file_size_bytes") or 0)
    return (size_bytes, float(candidate.get("score") or 0.0))


def skip_reason(config: dict[str, Any], metadata: dict[str, Any]) -> str | None:
    rules = config.get("skip_rules") or {}
    if str(metadata.get("encoded_by") or "").casefold() == APP_NAME.casefold():
        return "already_simpleripper"
    codec = str(metadata.get("video_codec") or "").lower()
    if rules.get("skip_hevc", True) and codec in {"hevc", "h265"} and not should_reprocess_hevc(config, metadata):
        return "already_hevc"
    if rules.get("skip_av1", True) and codec == "av1":
        return "already_av1"
    if rules.get("skip_4k", True) and int(metadata.get("video_height") or 0) >= 1800:
        return "skip_4k"
    if rules.get("skip_hdr", True) and metadata.get("is_hdr"):
        return "skip_hdr"
    min_duration = rules.get("min_duration_seconds")
    if min_duration is not None and (metadata.get("duration_seconds") or 0) < float(min_duration):
        return "below_min_duration"
    min_size_mb = rules.get("min_size_mb") or (config.get("scan") or {}).get("min_size_mb")
    if min_size_mb is not None and int(metadata.get("file_size_bytes") or 0) < int(min_size_mb) * 1024 * 1024:
        return "below_min_size"
    return None


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


def temp_upload_path(source: Path) -> Path:
    return source.with_name(f".{source.name}.simpleripper.tmp")


def quarantine_path_for_source(source: Path, config: dict[str, Any]) -> Path:
    root = Path(str((config.get("paths") or {}).get("quarantine_dir") or "quarantine"))
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


def refresh_jellyfin(config: dict[str, Any], source: Path) -> dict[str, Any]:
    settings = config.get("jellyfin") or {}
    if not settings.get("enabled", False):
        return {"status": "disabled"}
    server_url = str(settings.get("server_url") or "").rstrip("/")
    api_key = str(settings.get("api_key") or "")
    if not server_url or not api_key:
        return {"status": "skipped", "reason": "missing_server_url_or_api_key"}
    mapped_paths = jellyfin_mapped_paths(settings, source)
    try:
        lookup = jellyfin_lookup_item(server_url, api_key, source, mapped_paths)
        if lookup.get("status") != "ok":
            return lookup
        item_id = lookup.get("item_id")
        refresh_req = request.Request(f"{server_url}/Items/{item_id}/Refresh?Recursive=false&MetadataRefreshMode=Default&ImageRefreshMode=Default", headers={"X-Emby-Token": api_key}, method="POST")
        with request.urlopen(refresh_req, timeout=10):
            pass
        return {**lookup, "status": "ok"}
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


def normalize_path_for_match(value: str | Path) -> str:
    return str(value).replace("\\", "/").rstrip("/").casefold()


def jellyfin_mapped_paths(settings: dict[str, Any], source: Path) -> list[str]:
    candidates = [str(source)]
    source_text = str(source)
    for mapping in settings.get("path_mapping") or []:
        fs_prefix = str(mapping.get("filesystem_prefix") or "")
        jellyfin_prefix = str(mapping.get("jellyfin_prefix") or "")
        if fs_prefix and jellyfin_prefix and source_text.startswith(fs_prefix):
            candidates.append(jellyfin_prefix + source_text[len(fs_prefix):])
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
    query = parse.urlencode({"Recursive": "true", "SearchTerm": search_term, "Fields": "Path"})
    req = request.Request(f"{server_url}/Items?{query}", headers={"X-Emby-Token": api_key})
    with request.urlopen(req, timeout=10) as response:
        payload = json.loads(response.read().decode("utf-8"))
    return payload.get("Items") or []


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


def jellyfin_lookup_item(server_url: str, api_key: str, source: Path, candidate_paths: list[str]) -> dict[str, Any]:
    items_by_id: dict[str, dict[str, Any]] = {}
    for search_term in jellyfin_search_terms(source):
        for item in jellyfin_query_items(server_url, api_key, search_term):
            item_id = str(item.get("Id") or "")
            if item_id:
                items_by_id[item_id] = item
    if not items_by_id:
        return {"status": "not_found", "search_terms": jellyfin_search_terms(source)}
    scored = sorted(
        ((jellyfin_item_score(item, source, candidate_paths), item) for item in items_by_id.values()),
        key=lambda pair: pair[0],
        reverse=True,
    )
    best_score, best_item = scored[0]
    if best_score <= 0:
        return {"status": "not_found", "search_terms": jellyfin_search_terms(source)}
    if len(scored) > 1 and scored[1][0] == best_score:
        return {"status": "ambiguous", "match_count": len(scored), "top_score": best_score}
    return {"status": "ok", "item_id": best_item.get("Id"), "matched_path": best_item.get("Path"), "score": best_score, "search_terms": jellyfin_search_terms(source)}


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
    elif phase in {"swapping", "final_verify"} and source_path:
        if not source_path.exists() and quarantine_path and quarantine_path.exists():
            quarantine_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(quarantine_path), str(source_path))
            recovered_status = "rollback_restored"
        elif source_path.exists():
            recovered_status = "replacement_present"
            if source_metadata:
                try:
                    _probe, final_meta = ffprobe_metadata(config, source_path, str(source_metadata.get("media_type") or "default"))
                    final_verification, final_errors = verify_output(config, source_metadata, source_path, final_meta, stream_policy)
                    if final_errors:
                        recovery_details["verification_errors"] = final_errors
                    else:
                        recovered_status = "replacement_verified"
                        recovery_details["verification"] = final_verification
                except Exception as exc:
                    recovery_details["verification_error"] = str(exc)
    elif phase == "refreshing_jellyfin":
        recovered_status = "replacement_present"
        if source_path and source_path.exists() and source_metadata:
            try:
                _probe, final_meta = ffprobe_metadata(config, source_path, str(source_metadata.get("media_type") or "default"))
                final_verification, final_errors = verify_output(config, source_metadata, source_path, final_meta, stream_policy)
                if not final_errors:
                    recovered_status = "refresh_retried"
                    recovery_details["verification"] = final_verification
                    recovery_details["jellyfin_refresh"] = refresh_jellyfin(config, source_path)
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

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                **self.state.snapshot(),
                "allowed_roots": [str(path) for path in self.roots],
                "folder_suggestions": list(self.folder_suggestions),
                "folder_suggestion_mode": self.folder_suggestion_mode,
                "selected_folders": list(self.selected_folder_entries),
                "selected_folder_paths": [str(path) for path in self.selected_folders],
                "custom_folders": list(self.custom_folder_entries),
                "test_mode_message": "Bezi v test modu. Original nikdy nebude po uspechu smazan, zustane v karantene." if self.state.test_mode else None,
                "recent_log_lines": tail_text_lines(app_log_path(self.config), 60),
            }

    def persist_selected_folders(self) -> None:
        write_json(selected_folders_path(self.config), {"selected_folders": self.selected_folder_entries, "custom_folders": self.custom_folder_entries, "updated_at": utc_now()})
        persist_selected_folders_in_config(self.config, self.selected_folder_entries)
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

    def log_error(self, message: str) -> None:
        with self._lock:
            self.state.errors = ([{"at": utc_now(), "message": message}] + (self.state.errors or []))[:100]
        log_event(self.config, "error", message=message)

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
            existing_custom_paths = {str(item["path"]) for item in self.custom_folder_entries}
            self.selected_folder_entries = normalized
            self.custom_folder_entries = [item for item in normalized if item["path"] in existing_custom_paths]
            self._sync_folder_views()
            self.persist_selected_folders()

    def add_custom_folder(self, folder: str, media_type: str = "auto") -> None:
        path = Path(folder)
        with self._lock:
            entry = {"path": str(path), "media_type": media_type_value(media_type)}
            self.custom_folder_entries = [item for item in self.custom_folder_entries if item["path"] != entry["path"]] + [entry]
            self.selected_folder_entries = [item for item in self.selected_folder_entries if item["path"] != entry["path"]] + [entry]
            self._sync_folder_views()
            self.persist_selected_folders()

    def remove_custom_folder(self, folder: str) -> None:
        path = Path(folder)
        with self._lock:
            self.custom_folder_entries = [item for item in self.custom_folder_entries if Path(item["path"]) != path]
            self.selected_folder_entries = [item for item in self.selected_folder_entries if Path(item["path"]) != path]
            self._sync_folder_views()
            self.persist_selected_folders()

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
        probe_limit = int(((self.config.get("scan") or {}).get("priority_probe_limit") or 12))
        probe_limit = max(1, probe_limit)
        inspected: list[dict[str, Any]] = []
        for candidate in candidates[:probe_limit]:
            details = inspect_candidate(self.config, candidate, self.media_type_for_source(candidate))
            if details.get("status") != "ok":
                log_event(self.config, "candidate_probe_failed", source_path=str(candidate), error=details.get("error"))
                continue
            if details.get("skip_reason"):
                log_event(self.config, "candidate_scan_skipped", source_path=str(candidate), reason=details.get("skip_reason"))
                continue
            inspected.append(details)
        if inspected:
            inspected.sort(key=candidate_selection_key, reverse=True)
            best = inspected[0]
            log_event(self.config, "candidate_ranked", source_path=str(best["path"]), score=best.get("score"), codec=(best.get("metadata") or {}).get("video_codec"))
            return Path(best["path"])
        for candidate in candidates[probe_limit:]:
            details = inspect_candidate(self.config, candidate, self.media_type_for_source(candidate))
            if details.get("status") != "ok":
                log_event(self.config, "candidate_probe_failed", source_path=str(candidate), error=details.get("error"))
                continue
            if details.get("skip_reason"):
                log_event(self.config, "candidate_scan_skipped", source_path=str(candidate), reason=details.get("skip_reason"))
                continue
            log_event(self.config, "candidate_ranked_fallback", source_path=str(details["path"]), score=details.get("score"), codec=(details.get("metadata") or {}).get("video_codec"))
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
                candidates = scan_candidates(folders, self.config)
                log_event(self.config, "scan_end", candidates=len(candidates))
                if not candidates:
                    if not self.schedule_rescan_wait(3600, "no_candidates"):
                        break
                    continue
                candidate = self.pick_next_candidate(candidates)
                if candidate is None:
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
        output = work_dir_path / "output" / source.name
        output.parent.mkdir(parents=True, exist_ok=True)
        copied_source = work_dir_path / "input" / source.name
        copied_source.parent.mkdir(parents=True, exist_ok=True)
        metadata_dir = work_dir_path / "metadata"
        metadata_dir.mkdir(parents=True, exist_ok=True)
        tmp_output = temp_upload_path(source)
        job_summary: dict[str, Any] = {"job_id": job_id, "source_path": str(source), "started_at": utc_now(), "status": "running"}
        succeeded = False
        try:
            log_event(self.config, "candidate_selected", job_id=job_id, source_path=str(source))
            ensure_local_free_space(self.config, source.stat().st_size)
            self.set_phase("copying_source", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output)})
            log_event(self.config, "copy_source_start", job_id=job_id, source_path=str(source), local_input_path=str(copied_source))
            copy_file_interruptible(source, copied_source, lambda: bool(self.state.force_stop))
            log_event(self.config, "copy_source_done", job_id=job_id, bytes=copied_source.stat().st_size)
            self.set_phase("probing_source", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output)})
            source_probe, source_meta = ffprobe_metadata(self.config, copied_source, self.media_type_for_source(source))
            source_meta["file_size_bytes"] = source.stat().st_size
            write_json(metadata_dir / "source.ffprobe.json", {"probe": source_probe, "metadata": source_meta})
            recovery_context = {"source_metadata": source_meta}
            reason = skip_reason(self.config, source_meta)
            if reason:
                job_summary.update({"status": "skipped", "skip_reason": reason, "finished_at": utc_now(), "source": source_meta})
                write_json(marker_path(source, self.config), job_summary)
                append_jsonl(history_dir(self.config) / "jobs.jsonl", job_summary)
                history_payload = {"status": "skipped", "source_signature": source_signature(source), "job_id": job_id, "updated_at": utc_now(), "reason": reason}
                write_history_index(self.config, source, history_payload)
                write_shared_worker_history(self.config, source, history_payload)
                log_event(self.config, "candidate_skipped", job_id=job_id, source_path=str(source), reason=reason)
                with self._lock:
                    self.state.last_processed = ([{"source_path": str(source), "finished_at": utc_now(), "status": "skipped", "reason": reason}] + (self.state.last_processed or []))[:20]
                succeeded = True
                return
            stream_policy = select_streams(self.config, source_meta)
            recovery_context["track_policy"] = stream_policy
            self.set_phase("encoding", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), **recovery_context})
            command = build_ffmpeg_command(self.config, copied_source, output, source_meta, stream_policy)
            job_summary["ffmpeg_command"] = command
            (work_dir_path / "logs").mkdir(parents=True, exist_ok=True)
            ffmpeg_log = work_dir_path / "logs" / "ffmpeg.log"
            log_event(self.config, "ffmpeg_start", job_id=job_id, command=command)
            with ffmpeg_log.open("w", encoding="utf-8", errors="replace") as log_handle:
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
                        raise RuntimeError("force stop requested")
                    time.sleep(0.5)
                progress_thread.join(timeout=1)
                if self._ffmpeg.returncode != 0:
                    raise RuntimeError(f"ffmpeg failed with exit code {self._ffmpeg.returncode}")
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
            final_payload = replace_source_with_output(source, tmp_output, self.config, {"source": source_meta, "output": temp_meta, "verification": temp_verification, "track_policy": stream_policy, "job_id": job_id})
            self.set_phase("final_verify", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), "quarantine_path": final_payload["quarantine_path"], **recovery_context})
            log_event(self.config, "swap_done", job_id=job_id, quarantine_path=final_payload["quarantine_path"], replacement_path=str(source))
            final_probe, final_meta = ffprobe_metadata(self.config, source, source_meta.get("media_type") or "default")
            final_verification, final_errors = verify_output(self.config, source_meta, source, final_meta, stream_policy)
            if final_errors:
                rollback_replacement(source, Path(str(final_payload["quarantine_path"])), self.config)
                raise RuntimeError("Final verification failed after swap: " + "; ".join(final_errors))
            quarantine_cleanup = finalize_quarantined_original(Path(str(final_payload["quarantine_path"])), self.config)
            final_payload.update(quarantine_cleanup)
            write_json(metadata_dir / "final.ffprobe.json", {"probe": final_probe, "metadata": final_meta})
            write_json(marker_path(source, self.config), {"source": source_meta, "output": final_meta, "verification": final_verification, "track_policy": stream_policy, "job_id": job_id, **final_payload, "processed_at": utc_now()})
            history_payload = {"status": "done", "job_id": job_id, "source_signature": source_signature(source), "updated_at": utc_now(), "video_codec_after": final_meta.get("video_codec")}
            write_history_index(self.config, source, history_payload)
            write_shared_worker_history(self.config, source, history_payload)
            log_event(self.config, "final_verification_ok", job_id=job_id, replacement_path=str(source), output_size_bytes=source.stat().st_size)
            self.set_phase("refreshing_jellyfin", source, {"job_id": job_id, "local_input_path": str(copied_source), "local_output_path": str(output), "temp_output_path": str(tmp_output), "quarantine_path": final_payload["quarantine_path"], **recovery_context})
            jellyfin_refresh = refresh_jellyfin(self.config, source)
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
            job_summary.update({"status": "error", "finished_at": utc_now(), "error": str(exc)})
            append_jsonl(history_dir(self.config) / "jobs.jsonl", job_summary)
            history_payload = {"status": "error", "job_id": job_id, "source_signature": source_signature(source) if source.exists() else None, "updated_at": utc_now(), "error": str(exc)}
            write_history_index(self.config, source, history_payload)
            write_shared_worker_history(self.config, source, history_payload)
            self.log_error(f"{source}: {exc}")
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

def replace_source_with_output(source: Path, output: Path, config: dict[str, Any], marker: dict[str, Any]) -> dict[str, Any]:
    quarantine_path = quarantine_path_for_source(source, config)
    quarantine_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(source), str(quarantine_path))
    try:
        os.replace(output, source)
    except Exception:
        shutil.move(str(quarantine_path), str(source))
        raise
    marker = {**marker, "source_path": str(source), "quarantine_path": str(quarantine_path), "processed_at": utc_now()}
    write_json(history_dir(config) / "quarantine_index" / f"{file_lock_id(source)}.json", marker)
    return {"quarantine_path": str(quarantine_path), "replacement_path": str(source), "processed_marker_path": str(marker_path(source, config))}


def rollback_replacement(source: Path, quarantine_path: Path, config: dict[str, Any]) -> None:
    inspection_root = Path(str((config.get("paths") or {}).get("inspection_dir") or "inspection")) / "failed_replacements"
    inspection_root.mkdir(parents=True, exist_ok=True)
    failed_output = inspection_root / f"{source.name}.{int(time.time())}.failed-output"
    if source.exists():
        shutil.move(str(source), str(failed_output))
    if quarantine_path.exists():
        shutil.move(str(quarantine_path), str(source))
    log_event(config, "rollback_restored", source_path=str(source), quarantine_path=str(quarantine_path), failed_output_path=str(failed_output))


def preserve_failed_output(work_dir: Path, source: Path, config: dict[str, Any]) -> None:
    if not bool((config.get("paths") or {}).get("keep_failed_output_for_inspection", True)):
        return
    inspection_root = Path(str((config.get("paths") or {}).get("inspection_dir") or "inspection"))
    inspection_root.mkdir(parents=True, exist_ok=True)
    target = inspection_root / f"{source.stem}-{int(time.time())}"
    if work_dir.exists():
        shutil.move(str(work_dir), str(target))


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

    def send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_no_cache_headers()
        self.end_headers()
        self.wfile.write(body)

    def read_payload(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length).decode("utf-8")) if length else {}

    def do_GET(self) -> None:
        if self.path == "/api/status":
            self.send_json(self.app.status())
            return
        if self.path == "/api/config":
            self.send_json({"allowed_roots": [str(path) for path in self.app.roots]})
            return
        if self.path == "/api/logs":
            self.send_json({"lines": tail_text_lines(app_log_path(self.app.config), 200)})
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_no_cache_headers()
        self.end_headers()
        self.wfile.write(INDEX_HTML.encode("utf-8"))

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
                        folder = str(pick_folder_dialog(str(payload.get("initial_dir") or "").strip() or None) or "").strip()
                    if folder:
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
<body><div class="shell"><section class="hero fade-in"><div class="hero-grid"><div><span class="eyebrow">Local only · one encode at a time</span><h1 class="title">SimpleRipper</h1><p class="lede">Simple local control panel for picking folders, watching the current job, and checking the last replacement result without a wall of debug noise.</p><div class="hero-stats"><article class="stat"><div class="stat-label">Selected folders</div><div class="stat-value" id="selectedCount">0</div></article><article class="stat"><div class="stat-label">Current phase</div><div class="stat-value" id="heroPhase">Idle</div></article><article class="stat"><div class="stat-label">Warnings</div><div class="stat-value" id="warningCount">0</div></article></div></div><aside class="hero-card"><h2>Session pulse</h2><p id="heroSummary">Waiting for the first status update.</p><div class="badge-row" id="heroBadges"></div></aside></div></section><main class="grid"><section class="stack"><section class="panel fade-in"><h2>Folders</h2><p class="panel-intro">Vyberte slozky pres explorer nebo je zadejte rucne. Fallback bere navrhy z configu podle platformy.</p><div class="folder-actions"><button class="primary" onclick="pickFolder()">Vybrat slozku</button><button class="ghost" onclick="pickFolder(lastSelectedFolderPath())">Vybrat dalsi pobliz</button></div><div class="folder-manual"><input id="manualFolderPath" type="text" list="folderSuggestionList" placeholder="\\\\server\\share\\FILMY nebo /mnt/nas/filmy/FILMY"><button class="ghost" onclick="addManualFolder()">Pridat cestu</button></div><datalist id="folderSuggestionList"></datalist><div id="folderPickerHint" class="panel-intro" style="margin-top:12px">Otevira systemovy vyber slozky. Kdyz sit v dialogu zlobi, vlozte cestu rucne z config navrhu niz.</div><div id="selectedFolders" class="simple-list"></div></section><section class="panel fade-in"><h2>Controls</h2><p class="panel-intro">Start, stop after the current file, force-stop the local encode, or switch into safe test mode.</p><div class="controls"><button class="primary" onclick="post('/api/start')">Start</button><button class="ghost" onclick="post('/api/stop-after-current')">Stop after current</button><button class="warning" onclick="post('/api/force-stop')">Force stop</button><button class="outline" id="testModeButton" onclick="toggleTestMode()">Test mode</button><button class="outline" onclick="post('/api/clear-stale-locks')">Clear stale locks</button></div><div id="testModeBanner"></div></section><section class="panel fade-in"><h2>Errors</h2><p class="panel-intro">Recent app-level failures and warnings.</p><div id="errors" class="error-list"></div></section></section><section class="stack"><section class="panel fade-in"><h2>Current Job</h2><p class="panel-intro">Current local runtime state and ffmpeg progress.</p><div id="current"></div></section><section class="panel fade-in"><h2>Last Result</h2><p class="panel-intro">Most recent completed, skipped, or failed job.</p><div id="lastResult"></div></section><section class="panel fade-in"><div class="log-head"><div><h2>Activity Log</h2><p class="panel-intro">Recent local app events.</p></div><div class="log-meta" id="logMeta">0 lines</div></div><div class="log-shell"><pre id="log"></pre><details class="disclosure"><summary>Raw status payload</summary><pre id="status"></pre></details></div></section></section></main></div>
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
function pickFolder(initialDir=''){post('/api/custom-folder',initialDir?{initial_dir:initialDir}:{})}
function addManualFolder(){const input=document.getElementById('manualFolderPath');const path=(input&&input.value?input.value:'').trim();if(!path){return}post('/api/custom-folder',{path:path,media_type:guessMediaType(path)});if(input){input.value=''}}
function removeFolder(path){post('/api/folders',{folders:collectSelectedFolders().filter(item=>item.path!==path)})}
function toggleTestMode(){const button=document.getElementById('testModeButton');const enable=!(button&&button.getAttribute('data-enabled')==='true');post('/api/test-mode',{enabled:enable})}
function render(s){lastStatus=s;const selected=s.selected_folders||[];const combinedErrors=uiError?[{at:'UI',message:uiError},...(s.errors||[])]:[...(s.errors||[])];const warningCount=(combinedErrors.length?1:0)+(s.last_result&&s.last_result.warning?1:0);document.getElementById('status').textContent=JSON.stringify(s,null,2);document.getElementById('current').innerHTML=renderCurrent(s.current_summary);document.getElementById('lastResult').innerHTML=renderLastResult(s.last_result);document.getElementById('errors').innerHTML=combinedErrors.length?combinedErrors.map(e=>`<div class="err">${escapeHtml(e.at||'')}<br>${escapeHtml(e.message||e)}</div>`).join(''):'<p class="muted">No recent app-level errors.</p>';document.getElementById('log').textContent=(s.recent_log_lines||[]).join(String.fromCharCode(10));document.getElementById('logMeta').textContent=`${(s.recent_log_lines||[]).length} lines`;document.getElementById('selectedFolders').innerHTML=selected.length?selected.map(item=>`<div class="simple-item" data-path="${escapeHtml(item.path)}"><div class="simple-main"><div class="simple-title">${escapeHtml(item.path)}</div><div class="simple-sub">Selected for scan</div></div>${mediaTypeSelect(item.media_type||guessMediaType(item.path),'onchange="saveSelectedFolders()"')}<button class="ghost" data-path="${escapeHtml(item.path)}" onclick="removeFolder(this.getAttribute('data-path'))">Remove</button></div>`).join(''):'<div class="empty">No folders selected.</div>';document.getElementById('selectedCount').textContent=String(selected.length);document.getElementById('heroPhase').textContent=escapeHtml((s.current_summary&&s.current_summary.status)||s.current_phase||'Idle');document.getElementById('warningCount').textContent=String(warningCount);document.getElementById('heroSummary').textContent=heroSummary(s);document.getElementById('heroBadges').innerHTML=renderBadges(s);document.getElementById('testModeButton').textContent=s.test_mode?'Test mode ON':'Test mode';document.getElementById('testModeButton').setAttribute('data-enabled',s.test_mode?'true':'false');document.getElementById('testModeBanner').innerHTML=s.test_mode?`<div class="callout info" style="margin-top:14px">${escapeHtml(s.test_mode_message||'Bezi v test modu.')}</div>`:'';document.getElementById('folderSuggestionList').innerHTML=(s.folder_suggestions||[]).map(path=>`<option value="${escapeHtml(path)}"></option>`).join('');document.getElementById('folderPickerHint').textContent=s.folder_suggestion_mode==='linux-mounts'?'Explorer fallback pouziva mount navrhy z linux configu. Sit lze zadat primo jako mount cestu.':'Explorer fallback pouziva Windows roots z configu. Kdyz sit v dialogu chybi, vlozte UNC cestu rucne.'}
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
    parser.add_argument("command", choices=["web", "check-config"])
    parser.add_argument("--config", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "check-config":
            print(json.dumps({"ok": True, "config": str(args.config), "roots": (config.get("libraries") or {}).get("roots") or []}, indent=2))
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