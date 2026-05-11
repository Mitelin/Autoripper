# Autoripper

Autoripper is a safe media normalization pipeline for Jellyfin/Sonarr/Radarr libraries.

The first version is development/test mode only. It never overwrites, renames, moves, or deletes source files. Encoded outputs are written to a separate `output_root` such as `/mnt/nas/filmy/RIPTEST`.

## Why ffmpeg/ffprobe

The program uses `ffprobe` for metadata and `ffmpeg` for encoding. Python owns orchestration, safety checks, deterministic sampling, verification, and reports. This keeps the ripping layer mature and predictable while avoiding custom media handling code.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy `config.example.yaml` to `config.yaml` and fill in your local paths and secrets before running commands.

The repository now uses a single `config.yaml` with named profiles. Common settings live at the top level and environment-specific or scenario-specific overrides live under `profiles:`.

The default profile is `windows-dev`. To use a different one, pass `--profile`:

```bash
python media_normalizer.py scan --config config.yaml --profile linux-nas
```

## Tests

Unit tests live in `tests/` and focus on the pure decision-making layers: config profiles, track policy, shared queue store, and worker skeleton behavior.

Run them with:

```bash
python -m unittest discover -s tests -v
```

UNC paths should be written in YAML with single quotes, for example:

```yaml
output_root: '\\192.168.50.23\admin\RIPTEST'
```

Current built-in profiles in `config.yaml`:

- `windows-dev`: active default for Windows development against the UNC share
- `windows-testfull`: same library roots, but `sampling.samples_per_bucket: 1`
- `linux-nas`: Linux/NAS paths under `/mnt/nas/filmy`

If VS Code blocks browsing that share, add `192.168.50.23` to `security.allowedUNCHosts`. The program itself can still use the UNC paths when Windows, Python, and FFmpeg have access to the share.

## Commands

```bash
python media_normalizer.py scan --config config.yaml
python media_normalizer.py dry-run --config config.yaml
python media_normalizer.py track-audit --config config.yaml --limit 50
python media_normalizer.py test --config config.yaml --seed 12345
python media_normalizer.py test-full --config config.yaml --seed 12345
python media_normalizer.py test-clips --config config.yaml --seed 12345
python media_normalizer.py plan-top --config config.yaml --media-type anime --bucket high --count 20
python media_normalizer.py batch-top --config config.yaml --media-type anime --bucket high --count 20
python media_normalizer.py distributed-init --config config.yaml
python media_normalizer.py enqueue-top --config config.yaml --media-type anime --bucket high --count 5 --max-duration 1800
python media_normalizer.py queue-status --config config.yaml
python media_normalizer.py queue-control --config config.yaml --state paused
python media_normalizer.py node-control --config config.yaml --node-id gaming-server --worker-command stop_after_current
python media_normalizer.py worker-heartbeat --config config.yaml --profile gaming-worker
python media_normalizer.py worker-step --config config.yaml --profile gaming-worker --dry-run-result requeue
python media_normalizer.py worker-loop --config config.yaml --profile gaming-worker --dry-run-result requeue --stop-on-idle --idle-sleep-seconds 5
python media_normalizer.py maintenance-loop --config config.yaml --stop-on-idle --idle-sleep-seconds 30
python media_normalizer.py node-run --config config.yaml --profile media-server-manager --stop-on-idle --max-iterations 2
python media_normalizer.py lock-status --config config.yaml
python media_normalizer.py manager-step --config config.yaml --profile media-server-manager --dry-run-result done
```

To run with a non-default profile, append `--profile <name>` to any command.

`test` is currently an alias for `test-full`.

For quick development checks against a large network library, use `--limit`:

```bash
python media_normalizer.py scan --config config.yaml --limit 10
python media_normalizer.py dry-run --config config.yaml --limit 50
```

Sampling keeps at most one selected file per title folder/group inside a bucket, which helps spread tests across different shows or movies instead of several episodes from one series.

## Track Policy

Track cleanup is enabled only inside the safe encode flow. It never modifies source files in place; it only changes which streams are mapped into the new test output.

The rule is conservative: if target audio is not detected with high confidence, or if there is only one audio stream, the encoder preserves everything with `-map 0`.

Current target audio languages:

- Anime: English
- Series: Czech
- Movies: Czech

Use `track-audit` to inspect keep/drop decisions without encoding:

```powershell
python media_normalizer.py track-audit --config config.yaml --limit 50
```

Per-file encode logs include a `track_policy` block with the stream decisions and the exact ffmpeg mapping used.

## Distributed Queue Foundation

The first distributed-worker pieces are filesystem-only and safe for development. They do not encode, finalize, replace, delete, or refresh media by themselves.

Initialize the shared state directory:

```powershell
python media_normalizer.py distributed-init --config config.yaml
```

This creates `.ripper_state` directories under `shared_state_dir`, including `queue`, `running`, `ready_for_finalize`, `done`, `failed`, worker heartbeat folders, lock folders, and global control files.

Enqueue jobs from the same largest-file planner used by `plan-top`:

```powershell
python media_normalizer.py enqueue-top --config config.yaml --media-type anime --bucket high --count 5 --max-duration 1800
```

Check shared queue state:

```powershell
python media_normalizer.py queue-status --config config.yaml
python media_normalizer.py recover-stale-jobs --config config.yaml
python media_normalizer.py recover-stale-locks --config config.yaml
python media_normalizer.py requeue-interrupted-jobs --config config.yaml
```

`queue-status` now also reports `node_controls`, parsed `worker_heartbeats`, parsed `manager_heartbeats`, and aggregated `worker_summary` / `manager_summary` blocks. That makes pending `stop_after_current` requests, current phase, current job id, heartbeat age, and per-role state/phase counts directly visible in the main operational status output.

`recover-stale-jobs` is the first explicit maintenance command for worker heartbeat expiry. It scans stale worker heartbeats that still claim a current running job, moves the matching job from `running/` to `stale/`, and writes a shared stale audit log under `.ripper_state/logs/jobs/`.

`recover-stale-locks` is the matching explicit maintenance command for expired shared lock slots. It scans `nas_read`, `nas_write`, `active_encode`, and `finalizer` locks, marks heartbeat-expired slots as stale, and releases those slot files so healthy workers or the manager can claim them again.

`requeue-interrupted-jobs` is the matching explicit recovery command for interrupted worker jobs. It moves jobs from `interrupted/` back to `queue/`, records that they were requeued from the interrupted state, and writes a shared requeue audit log under `.ripper_state/logs/jobs/`. Use `--job-id <id>` to target a specific interrupted job or `--limit <n>` to cap the number of requeued jobs.

Start the first local web UI/status server:

```powershell
python media_normalizer.py web-ui --config config.yaml --host 127.0.0.1 --port 5055
```

Optional local login for the web UI can be enabled in config:

```yaml
web_ui:
	enabled: true
	host: '127.0.0.1'
	port: 5055
	auth:
		enabled: true
		username: admin
		password_hash: '<sha256-of-password>'
		session_ttl_seconds: 43200
```

When auth is enabled, `/healthz` stays open for local monitoring, while the dashboard and `/api/*` endpoints require a login session cookie.

The current first slice serves a minimal local dashboard at `/` and raw JSON status at `/api/status`. It is intentionally small: it exposes the existing `queue-status` payload for the local node without changing worker or manager orchestration.

The web UI server also exposes narrower read-only endpoints for incremental UI loading:

```text
GET /api/status   -> full combined status payload
GET /api/workers  -> worker and manager heartbeat summaries
GET /api/jobs     -> jobs grouped by queue state
GET /api/logs     -> latest manager and worker-related JSON logs
GET /api/locks    -> shared lock status with stale/active counts
GET /api/settings/worker-schedule -> local worker schedule payload and runtime allowance
```

The first local control endpoint is also available:

```text
POST /api/worker/stop-after-current -> writes the local node's worker_command
POST /api/worker/hard-stop          -> writes the local node's worker_command=hard_stop
POST /api/worker/start              -> persists worker.enabled=true for the local node config
POST /api/worker/pause              -> persists worker.enabled=false for the local node config
POST /api/settings/worker-schedule  -> persists the local worker schedule back to config.yaml
POST /api/manager/start             -> persists manager.enabled=true for the local node config
POST /api/manager/pause             -> persists manager.enabled=false for the local node config
POST /api/manager/finalize-now      -> runs one immediate local manager finalization pass
POST /api/jellyfin/full-scan        -> triggers a Jellyfin full library refresh from the local manager node
```

These worker endpoints write the existing per-node control file for the local `node.id`. `stop-after-current` exits cleanly before the next iteration. `hard-stop` is now also honored during local execute encoding: the worker terminates the local ffmpeg process, deletes partial local output, and moves the job to `interrupted` without touching the original media source.

Additional control endpoints now exposed by the local web UI:

```text
POST /api/global/pause-queue           -> sets global queue_state=paused
POST /api/global/maintenance           -> sets global queue_state=maintenance and disables finalizer
POST /api/global/resume-queue          -> sets global queue_state=running
POST /api/maintenance/recover-stale-jobs   -> moves stale running jobs into stale/
POST /api/maintenance/recover-stale-locks  -> releases stale shared lock slots
POST /api/maintenance/requeue-interrupted-jobs -> moves interrupted jobs back to queue/
POST /api/manager/start                -> persists manager.enabled=true for the local node config
POST /api/manager/pause                -> persists manager.enabled=false for the local node config
POST /api/manager/stop-after-current   -> writes the local node's manager_command
```

The dashboard now includes matching buttons for these actions plus a shared lock panel backed by `/api/locks`. Manager controls are only shown on nodes where the local `manager` role is enabled, and `start` / `pause` use the same config-persistence pattern as the local worker enable toggle.

The schedule editor writes back to the original config file used to launch the node. If a config profile is active, the updated schedule is stored under that profile instead of flattening the merged runtime config into the top-level YAML.

Pause or resume new worker claims globally:

```powershell
python media_normalizer.py queue-control --config config.yaml --state paused
python media_normalizer.py queue-control --config config.yaml --state running
```

Write or inspect per-node loop commands:

```powershell
python media_normalizer.py node-control --config config.yaml --node-id gaming-server --worker-command stop_after_current
python media_normalizer.py node-control --config config.yaml --node-id media-server --manager-command stop_after_current
python media_normalizer.py node-control --config config.yaml --node-id media-server
```

`node-control` currently supports `stop_after_current` and `hard_stop` for worker loops, and `stop_after_current` for manager loops. The command is stored in `.ripper_state/control/nodes/<node_id>.json`. If a worker loop sees `hard_stop` before the next iteration, it exits immediately; if it sees `hard_stop` during local execute encoding, it interrupts the local ffmpeg run and records the job as `interrupted`. Clear a command with `--worker-command none` or `--manager-command none`.

For atomic-claim testing only, move one queued job to `running`:

```powershell
python media_normalizer.py queue-claim-one --config config.yaml --node-id desktop-pc
```

For worker skeleton testing, write a heartbeat or run one safe dry-run worker step:

```powershell
python media_normalizer.py worker-heartbeat --config config.yaml --profile gaming-worker
python media_normalizer.py worker-step --config config.yaml --profile gaming-worker --dry-run-result requeue
python media_normalizer.py worker-loop --config config.yaml --profile gaming-worker --dry-run-result requeue --stop-on-idle --idle-sleep-seconds 5
```

`worker-step` checks local worker enablement, local schedule, and global queue control before claiming. In dry-run mode it now performs the realistic local workflow skeleton: local source cache in `local_work_dir`, `nas_read`/`nas_write` lock coordination, ready bundle upload, and local workspace cleanup, while still using a placeholder encoded file.

For real local worker encoding, opt in explicitly and target the ready handoff:

```powershell
python media_normalizer.py worker-step --config config.yaml --profile gaming-worker --dry-run-result ready --execute
python media_normalizer.py worker-loop --config config.yaml --profile gaming-worker --dry-run-result ready --execute --stop-on-idle
python media_normalizer.py node-run --config config.yaml --profile gaming-worker --worker-dry-run-result ready --worker-execute --stop-on-idle
```

Worker `--execute` uses the existing ffmpeg/ffprobe verification path on the worker's local workspace and acquires an `active_encode` slot before starting the encode. For safety, execute mode currently requires `--dry-run-result ready`, because the worker only has a meaningful execute path when it is producing a ready bundle for manager finalization.

When local execute mode is active, manual local `hard_stop` is supported. If a worker receives `worker_command=hard_stop`, it terminates the local ffmpeg process, deletes partial local output, cleans the local workspace, and moves the claimed job into `interrupted`. The same interrupted path is also used if the worker is currently encoding and its local schedule becomes disallowed while `outside_window_behavior: hard_stop` is configured.

`worker-loop` is the matching continuous worker runner. It repeatedly calls `worker-step`, drains available queue work without sleeping between successful iterations, and only sleeps after `idle`, `worker_disabled`, `outside_schedule`, `global_queue_paused`, or `not_enough_local_space`. Use `--stop-on-idle` for bounded smoke runs and `--max-iterations` for safe development caps.

`maintenance-loop` is the periodic maintenance runner. In this milestone it repeatedly recovers both stale worker jobs and stale shared lock slots, so orphaned `running/` jobs move into `stale/` and expired slot files are released automatically. Use `--stop-on-idle` for bounded smoke runs and `--max-iterations` for safe development caps.

`node-run` is the small launcher above the loops. It inspects node roles plus `worker.run_continuously`, `manager.run_continuously`, and `maintenance.run_continuously`, starts the enabled continuous services for that profile, and waits for them. For bounded smoke tests, pass `--stop-on-idle` and `--max-iterations` so the started loops exit on their own.

Shared NAS I/O locks can be inspected and manually tested:

```powershell
python media_normalizer.py lock-status --config config.yaml
python media_normalizer.py recover-stale-locks --config config.yaml
python media_normalizer.py lock-acquire --config config.yaml --lock-type nas_read --node-id gaming-server --job-id job_test
python media_normalizer.py lock-acquire --config config.yaml --lock-type finalizer --node-id media-server --job-id job_test
python media_normalizer.py lock-release --path '\\192.168.50.23\admin\RIPTEST\.ripper_state\locks\nas_read\slot_1.json'
```

Set `io_limits.lock_stale_after_seconds` to override how long a shared lock heartbeat may stay quiet before `lock-status`, `recover-stale-locks`, and `maintenance-loop` treat it as stale. If omitted, the lock recovery path falls back to `heartbeat.stale_after_seconds`.

The manager/finalizer skeleton can also be tested without touching library files:

```powershell
python media_normalizer.py manager-heartbeat --config config.yaml --profile media-server-manager
python media_normalizer.py manager-step --config config.yaml --profile media-server-manager --dry-run-result done
python media_normalizer.py manager-step --config config.yaml --profile media-server-manager --execute
python media_normalizer.py manager-loop --config config.yaml --profile media-server-manager --stop-on-idle --idle-sleep-seconds 5
```

`manager-step` takes the single-slot `finalizer` lock, claims one `ready_for_finalize` job into `finalizing` atomically, writes manager heartbeat phases, releases the lock, and in this milestone only moves the job JSON to `done`, `failed_finalize`, or back to `ready_for_finalize` as a dry-run result.

`manager-step --execute` is now the explicit production switch for the finalizer. By default the manager still stays in dry-run mode. When `--execute` is provided, the manager moves the original source into the planned quarantine path, moves the ready output into the library path, and then triggers a real per-file Jellyfin refresh. If the second move fails, it attempts to roll the original file back into place before marking the job as `failed_finalize`. A Jellyfin refresh failure is logged into the job result and by default does not roll the file replacement back.

If `manager.require_successful_jellyfin_refresh: true` is enabled, the manager treats any non-`refreshed` Jellyfin result as a post-finalization failure and moves the job JSON into `failed_finalize` after the file replacement has already been applied.

`manager-loop` is the first continuous manager runner. It repeatedly calls `manager-step`, drains available `ready_for_finalize` work without sleeping between successful iterations, and only sleeps after `idle`, `manager_disabled`, `global_finalizer_paused`, or `finalizer_lock_unavailable`. Use `--stop-on-idle` for bounded smoke runs and `--max-iterations` for safe development caps.

Each manager step writes a per-job finalization log under `.ripper_state/logs/manager/<job_id>.json` so the finalizer decision is still auditable after the job JSON moves between state directories.

When `worker-step --dry-run-result ready` is used, the worker now also writes a dry-run placeholder artifact and manifest under `.ripper_state/ready_outputs/` and stores both paths into the job JSON. `manager-step` validates those paths when present, so the dry-run handoff now exercises a real worker-to-manager artifact chain.

The dry-run handoff now uses a per-job bundle layout closer to the target architecture:

```text
.ripper_state/ready_outputs/<job_id>/
├── output.mkv
├── output.ffprobe.json
├── worker_log.json
├── checksum.sha256
└── manifest.json
```

On successful dry-run finalization into `done`, the manager removes that per-job ready output bundle so shared state stays aligned with queue state. On `requeue` or `failed_finalize`, the bundle is kept for inspection.

The manager no longer treats the bundle as a dumb file drop only. For dry-run bundles it reads `manifest.json` and `output.ffprobe.json`, checks that job id and source path match, confirms the original source file still exists, and compares expected duration/audio/subtitle counts against the bundle metadata before allowing `done`. This gives the finalizer a real validation gate even before the pipeline writes real MKV outputs.

After that validation step, the manager also prepares a real finalization plan: it computes the replacement path, quarantine path, writes `.ripper_state/quarantine_manifest/<job_id>.json`, and stores a Jellyfin refresh payload into the job/log payload. In dry-run mode the manifest stays at `status: planned` and Jellyfin remains only planned. In execute mode the original is moved to quarantine, the ready output is moved into place, the manifest is updated to `status: executed`, and the manager attempts a real per-file Jellyfin refresh.

The current milestone now includes worker, manager, and maintenance loops on top of the shared state and atomic coordination layer. The worker path supports both dry-run orchestration and explicit local execute mode, where ffmpeg encodes into the local workspace, verifies the output, and uploads a ready bundle for manager finalization.

## Top Batch Workflow

Use `plan-top` before longer work. It walks the live library, skips entries already recorded in the processed registry, sorts remaining files by filesystem size, and only then runs `ffprobe` on the largest candidates until it has the requested batch.

```powershell
python media_normalizer.py plan-top --config config.yaml --media-type anime --bucket high --count 20
```

For normal anime episodes, add a duration cap so long movies and specials do not dominate the largest-file queue:

```powershell
python media_normalizer.py plan-top --config config.yaml --media-type anime --bucket high --count 20 --max-duration 1800
```

If the plan looks right, run the matching encode batch:

```powershell
python media_normalizer.py batch-top --config config.yaml --media-type anime --bucket high --count 20 --max-duration 1800
```

This is intentionally stateless except for the processed registry. If a batch is interrupted, the next run plans again from the current library state; completed successful files are skipped, unfinished files remain eligible.

The `batch.default_realtime_factor` config value is used for runtime estimates before encode results exist. Update it as real measurements improve.

For unattended daily runs, use the processing window. A job that is already encoding is allowed to finish after the window closes, but the next file will not start:

```powershell
python media_normalizer.py batch-top --config config.yaml --media-type anime --bucket high --count 20 --max-duration 1800 --respect-window --window-start 02:00 --window-end 07:00
```

You can also enable the same window in config:

```yaml
batch:
	window:
		enabled: true
		start: '02:00'
		end: '07:00'
```

Jellyfin refresh is optional and disabled by default. When enabled, successful full-file encodes try to find the matching Jellyfin item by mapped source path and refresh only that item:

```yaml
jellyfin:
	enabled: true
	server_url: 'http://jellyfin.example:8096'
	api_key: '...'
	path_mappings:
		- local_prefix: '\\192.168.50.23\admin'
			jellyfin_prefix: '/mnt/nas/filmy'
```

The refresh result is written into each per-file JSON log as `jellyfin_refresh`.
## Safety Rules

- Source files are opened read-only by `ffprobe`/`ffmpeg`.
- All encodes go under `output_root` in bucket folders.
- Existing outputs are never overwritten; a numeric suffix is added.
- Already HEVC/H.265 and AV1 files are skipped by default.
- Files with normalized filename markers such as `[TEST-HEVC-` are skipped by default.
- Successfully completed full-file encodes are recorded in a processed-source registry and skipped before `ffprobe` on later runs.
- HDR, 4K, too-short files, and unreadable files are skipped by default.

This idempotency rule is important: after a file has been normalized to HEVC, later scans should classify it as already processed and move on instead of repeatedly re-ripping it.

## Processed Registry

The registry defaults to:

```text
<output_root>/state/processed_sources.json
```

Each successful full-file encode stores a source fingerprint based on normalized source path, size, and modified time. Later scans can skip matching sources with `SKIP_ALREADY_PROCESSED_REGISTRY` before running `ffprobe`, which keeps normal operation from repeatedly probing tens of thousands of known files.

`test-clips` does not register sources as processed, because clips are only tuning artifacts and should not block a later full encode.

Clip duration verification uses a separate tolerance because stream timestamps and keyframe boundaries can make a 60 second requested clip a few seconds longer. The default clip tolerance is 6 seconds.

Verification thresholds can also be overridden per media type. The default config allows anime outputs down to 8% of source size before marking them as suspiciously tiny, because some clean 1080p anime sources compress far more aggressively than live-action content.

## Reports

Reports are written under:

```text
<output_root>/reports/<run_id>/
```

Each run can include:

- `run_summary.json`
- `run_summary.csv`
- `human_readable_summary.md`
- `scan_items.json`
- `selected_samples.json`
- `per_file_logs/*.json`

## Production Mode

Distributed production flow now includes manager-only final library replacement, quarantine moves, and Jellyfin refresh support.

Still intentionally out of scope for this first version:

- Sonarr/Radarr integration
- automatic quarantine cleanup/deletion