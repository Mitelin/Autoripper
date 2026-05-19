# SimpleRipper

SimpleRipper is a new local-only media ripper. It intentionally does not continue the old Autoripper distributed worker, manager, queue, finalizer, heartbeat, or NAS orchestration design.

## Rules

- One machine runs one SimpleRipper process.
- Startup is guarded by a local PID lock at `app.runtime_dir/simpleripper.pid`.
- The app processes exactly one file at a time.
- Runtime state, current job, selected folders, history, and file locks are local files only.
- There is no shared queue and no distributed coordination.
- The original source is kept only as a temporary rollback copy until final verification succeeds.

## Processing Flow

For each selected source file, SimpleRipper:

1. Creates a local per-source lock under `runtime/file_locks/`.
2. Copies the source into `paths.local_work_dir/current/input/`.
3. Probes the local copy with ffprobe.
4. Applies skip rules and track policy.
5. Encodes only to local work storage.
6. Verifies the local output.
7. Copies the verified output to a temporary file beside the original source.
8. Verifies that temporary NAS-side file.
9. Moves the original into `paths.quarantine_dir`, preserving its relative library path when possible.
10. Renames the temporary output into the original source path.
11. Runs final verification on the replaced source path.
12. Deletes the quarantined original after final verification succeeds, unless development retention is enabled.
13. Writes marker/history records and optionally asks Jellyfin to refresh the item.

If final verification fails after the swap, SimpleRipper attempts to move the new output to `paths.inspection_dir/failed_replacements/` and restore the quarantined original.

## Run

```powershell
python simpleripper.py check-config --config config.example.yaml
python simpleripper.py web --config config.example.yaml
```

Run those commands in an environment where the project dependencies are installed.

Then open the printed local URL, browse folders from configured library roots in the built-in web picker, or enter a path manually, and press `START`.

Each selected folder can be tagged as `Auto`, `Movie`, `Series`, or `Anime`. That folder-level media type is used before path heuristics for bitrate thresholds, profile selection, and track-policy behavior.

The web UI folder browser is limited to `libraries.roots`, shows those roots first, lets you step into direct child directories, and exposes an up button when the current folder still lives inside an allowed root. This is the supported picker flow for headless Linux deployments.

Folder selections are persisted both to local runtime state and back into `scan.selected_folders` in the configured YAML file.

By default, SimpleRipper does not create sidecar `.simpleripper.done` files in media folders; it uses central history instead.

By default, existing HEVC files are skipped, but very large HEVC files with still-suspicious bitrate can be considered again through `skip_rules.hevc_reprocess_*` thresholds.

`STOP` means finish the current safe job and then stop. `FORCE STOP` terminates the local encode and cleans local work files without intentionally touching the original media unless the app is already inside the protected swap/rollback phase.

## Configuration

Copy `config.example.yaml` to `config.yaml` and edit:

- `libraries.roots`: allowed base folders shown in the GUI.
- `libraries.roots`: allowed base folders shown in the GUI and exposed by the web folder browser.
- `scan.selected_folders`: optional persisted `path + media_type` entries.
- `scan.write_sidecar_markers`: default `false`; when enabled, SimpleRipper also writes legacy `.simpleripper.done*` sidecar markers beside source files.
- `scan.failed_retry_cooldown_hours` and `scan.max_failures_per_file`: keep recent ffmpeg failures out of the next scan using central history, so one bad file cannot loop immediately.
- `scan.priority_probe_limit`: how many of the largest current candidates get ffprobe-based ranking before the next job is chosen.
- `scan.folder_clean_requires_full_inventory`: default `true`; a folder can become cache-clean only after a complete inventory generation verifies all relevant descendants are terminal.
- `scan_cache`: local SQLite worker cache for fast inventory scans, cached skip decisions, candidate queues, hierarchical folder clean/partial state, and failed/blocked cooldowns.
- `retention_size_policy`: keeps oversized HEVC/AV1 files eligible even when they already match the target codec, based on a per-media-type MB-per-25-minute limit.
- `paths.worker_cache_path`: optional explicit SQLite cache path; defaults to `<runtime_dir>/worker_cache.sqlite`.
- `paths.local_work_dir`: local temporary processing area.
- `paths.history_dir`: local job history JSON and JSONL records.
- `paths.log_dir`: reserved local app log directory.
- `paths.quarantine_dir`: temporary rollback area for original files during swap/final verify.
- `paths.keep_quarantine_after_success`: keep the quarantined original after success for development or manual inspection. Default is `false`.
- `paths.inspection_dir`: failed-output inspection area.
- `tools.ffmpeg` and `tools.ffprobe`.
- `downscale`: optional 4K and ultrawide 4K downscale to a 1080p-class width while preserving aspect ratio. Example: `3840x1608 -> 1920x804`. Keep `downscale.enabled: false` unless you explicitly want smaller outputs instead of preserving 4K resolution.
- `quality_profiles`, `track_policy`, `skip_rules`, and `verification`.
- `jellyfin`: optional item refresh after a successful final verify.

Successful job history includes flattened before/after summary fields such as source/output size, codec changes, stream counts, ratio, bitrate, and warning text so it can be inspected without reopening the nested ffprobe blocks.

## Tests

```powershell
python -m unittest discover -s tests -v
```

## Cache Commands

```powershell
python simpleripper.py rebuild-index --config config.yaml
python simpleripper.py cache-summary --config config.yaml
python simpleripper.py clear-failures --config config.yaml
python simpleripper.py clear-cache --config config.yaml
python simpleripper.py clear-folder-cache --config config.yaml
python simpleripper.py clear-file-cache --config config.yaml
python simpleripper.py clear-candidate-queue --config config.yaml
```

The old project is kept only as `ZAMEK_AUTORIPPER_OLD_READONLY_DO_NOT_CONTINUE/` for locked reference settings.

The cache is worker-local and only an optimization. Deleting `paths.worker_cache_path` makes the next run rebuild its inventory and folder states from the media tree. Clean folders are skipped by cheap folder signatures only after a completed inventory generation proved every relevant descendant file is terminal; no `.simpleripper.done` marker files are written to original media folders unless legacy sidecar markers are explicitly enabled.
