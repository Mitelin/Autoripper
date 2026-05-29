# SimpleRipper

SimpleRipper is the current local-only Autoripper rewrite.

It does not continue the old distributed manager/worker/queue design. One process owns one machine, processes one file at a time, and keeps its runtime state in local files plus one local SQLite cache.

The old codebase is kept only as reference in `ZAMEK_AUTORIPPER_OLD_READONLY_DO_NOT_CONTINUE/`.

## What It Does

SimpleRipper provides a small built-in web UI for:

- selecting scan folders under configured library roots
- tagging folders as `auto`, `movie`, `series`, or `anime`
- starting and stopping the local processing loop
- force-stopping the current encode
- toggling test mode
- clearing stale per-file locks
- running a git-based in-place update when the app is idle
- inspecting current status, last result, warnings, errors, and recent log lines

The worker itself:

- inventories candidate files from selected folders
- uses a local SQLite cache to skip clean folders and keep a ranked candidate queue
- probes media with ffprobe
- applies skip rules, quality profile checks, bitrate retention rules, and track policy
- encodes locally with ffmpeg
- verifies the local output
- uploads to a temporary file beside the source
- verifies the NAS-side temporary file
- swaps the source with the verified replacement
- performs final verification
- optionally refreshes Jellyfin
- writes local history and cache state

## Core Rules

- Only one local SimpleRipper process is allowed at a time.
- Startup is guarded by `app.runtime_dir/simpleripper.pid`.
- Each source file also gets its own lock under `runtime/file_locks/`.
- The worker processes exactly one file at a time.
- The queue, runtime state, selected folders, logs, and recovery markers are local files.
- The source original is quarantined during swap and is only deleted after the whole pipeline is actually finished.
- If the process crashes before `job_done`, the interrupted job is cleaned up and replayed from the beginning.

## Processing Flow

For each file, SimpleRipper runs this pipeline:

1. Acquire a per-source lock.
2. Copy the source into `paths.local_work_dir/current/input/`.
3. Probe the local copy with ffprobe.
4. Apply skip rules, target profile checks, bitrate retention logic, and track policy.
5. Encode to `paths.local_work_dir/current/output/`.
6. Verify the local encoded output.
7. Copy the verified output to a temporary file beside the source path.
8. Verify that NAS-side temporary file.
9. Move the original source into `paths.quarantine_dir`.
10. Move the verified replacement into the final source path.
11. Run final verification on the replaced file.
12. Refresh Jellyfin if enabled.
13. Delete the quarantined original only after the pipeline is complete, unless retention is enabled.
14. Write history, cache updates, and UI-visible summaries.

If final verification fails after swap, the replacement is moved to `paths.inspection_dir/failed_replacements/` and the quarantined original is restored.

## Crash Recovery

Crash recovery is intentionally conservative.

- If the process restarts and finds an unfinished job, it logs explicit recovery events into `paths.log_dir/app.log`.
- It cleans `paths.local_work_dir/current/` and removes the stale `current_job.json` record first.
- If swap already happened, it rolls the source back from quarantine before scheduling the retry.
- It writes a local `resume_request.json` and replays the same source from the beginning.
- It does not try to continue from a half-finished encode, upload, final verify, or Jellyfin refresh.
- The only expected non-crash stops are explicit user actions such as `Stop after current`, `Force stop`, or `Update`.

Recovery log events now include a clear sequence such as:

- `crash_recovery_detected`
- `crash_recovery_cleanup_started`
- `crash_recovery_cleanup_finished`
- `crash_recovery_rerun_scheduled`

## Runtime Files

SimpleRipper keeps its local state in predictable files:

- `app.runtime_dir/simpleripper.pid`: process ownership lock
- `app.runtime_dir/current_job.json`: in-flight job state used for crash recovery
- `app.runtime_dir/runtime_control.json`: persisted run intent so the loop can auto-resume after a crash
- `app.runtime_dir/resume_request.json`: pending full rerun after crash cleanup
- `app.runtime_dir/file_locks/*.json`: per-source locks
- `paths.worker_cache_path`: SQLite cache for inventory, queue, folder state, and failure cooldowns
- `paths.history_dir/jobs.jsonl`: append-only job and recovery history
- `paths.log_dir/app.log`: human-readable event log

## Installation

Install Python dependencies:

```powershell
python -m pip install -r requirements.txt
```

SimpleRipper also requires external `ffmpeg` and `ffprobe` binaries available either on `PATH` or configured explicitly in YAML.

## Quick Start

1. Copy `config.example.yaml` to `config.yaml`.
2. Adjust library roots, work paths, quality settings, and optional Jellyfin settings.
3. Validate the configuration.
4. Start the web UI.

```powershell
python simpleripper.py check-config --config config.yaml
python simpleripper.py web --config config.yaml
```

Then open the printed local URL and press `Start`.

## Web UI Behavior

The web UI is intentionally small and local-only.

- Folder browsing is restricted to `libraries.roots`.
- Selected folders are persisted into runtime state and back into `scan.selected_folders` in the YAML config.
- The UI shows the current phase, active file, ffmpeg progress, warnings, queued error actions, and recent log lines.
- `Stop after current` lets the current safe pipeline finish and then stops the loop.
- `Force stop` terminates the local encode and clears local work files. It is not intended to leave the pipeline in a recoverable half-step.
- `Test mode` keeps the quarantined original instead of deleting it after success.
- `Clear stale locks` removes per-source lock files whose owning local PID is gone.
- `Update` is only available when the app is idle and performs `git pull` followed by a local restart of the web process.

## Configuration

Start from [config.example.yaml](config.example.yaml).

Important sections:

- `app.host`, `app.port`, `app.runtime_dir`: web bind address and local runtime directory
- `paths.local_work_dir`: local staging area for in-progress work
- `paths.history_dir`: JSON and JSONL job history
- `paths.log_dir`: app log directory
- `paths.worker_cache_path`: explicit cache path, defaults to `<runtime_dir>/worker_cache.sqlite`
- `paths.quarantine_dir`: temporary rollback area for originals during swap
- `paths.keep_quarantine_after_success`: preserve quarantined originals after success
- `paths.inspection_dir`: storage for failed replacements and inspection artifacts
- `paths.keep_failed_output_for_inspection`: preserve failed local work for investigation
- `libraries.roots`: allowed base folders shown in the UI picker
- `scan.selected_folders`: persisted `path + media_type` entries
- `scan.write_sidecar_markers`: optional legacy `.simpleripper.done.json` markers beside media files
- `scan.file_extensions`: scanned suffixes
- `scan.exclude_paths`: path tokens to ignore during scans
- `scan.priority_probe_limit`: number of large candidates that get a deeper ranking pass
- `scan.failed_retry_cooldown_hours`, `scan.max_failures_per_file`: older scan-level cooldown controls kept for compatibility
- `scan_cache.*`: current SQLite cache behavior for inventory, queue, folder state, retries, and block windows
- `quality_profiles.*`: encoder, CRF, preset, pixel format, and stream-copy behavior per media type
- `downscale.*`: optional width-capped downscale for 4K-style sources while preserving aspect ratio
- `retention_size_policy.*`: keep suspiciously large HEVC or AV1 files eligible for re-encode
- `track_policy.*`: select preferred audio languages and subtitle retention behavior
- `skip_rules.*`: codec, resolution, HDR, size, and duration-based skip logic
- `verification.*`: duration tolerance, ratio checks, and bitrate thresholds
- `jellyfin.*`: optional post-success item refresh and path lookup behavior

## CLI Commands

```powershell
python simpleripper.py check-config --config config.yaml
python simpleripper.py web --config config.yaml
python simpleripper.py rebuild-index --config config.yaml
python simpleripper.py cache-summary --config config.yaml
python simpleripper.py clear-failures --config config.yaml
python simpleripper.py clear-cache --config config.yaml
python simpleripper.py clear-folder-cache --config config.yaml
python simpleripper.py clear-file-cache --config config.yaml
python simpleripper.py clear-candidate-queue --config config.yaml
```

Command summary:

- `check-config`: load the config and print a minimal OK payload
- `web`: start the local web UI and worker controller
- `rebuild-index`: force a fresh fast inventory over selected folders or roots
- `cache-summary`: print the current cache summary
- `clear-failures`: reset cached file failures and cooldowns
- `clear-cache`: remove the entire worker cache and rebuild later on demand
- `clear-folder-cache`: reset folder-state cache data
- `clear-file-cache`: reset file-level cache data
- `clear-candidate-queue`: clear only the cached candidate queue

## Cache Model

The cache is local optimization only.

- It is safe to delete `paths.worker_cache_path`.
- The next scan will rebuild inventory state from the media tree.
- Clean folders are skipped only after a completed inventory generation proved all relevant descendants are terminal.
- Failed files enter retry cooldowns and can eventually become blocked after repeated failures.
- The queue is regenerated from the cache and does not act as a shared or distributed queue.

## Logs And History

Useful files during troubleshooting:

- `paths.log_dir/app.log`: high-level app events and crash recovery events
- `paths.log_dir/ffmpeg-current.log`: latest ffmpeg progress snapshot
- `paths.history_dir/jobs.jsonl`: append-only history of successful jobs, errors, and recovery actions
- `paths.history_dir/*.json`: per-job records for completed work

Successful job records include flattened summary fields such as before/after size, codec, stream counts, ratio, bitrate, and warning text so you can inspect outcomes without reopening nested ffprobe payloads.

## Tests

Run the full test suite with:

```powershell
python -m unittest discover -s tests -v
```
