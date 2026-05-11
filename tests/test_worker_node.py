from __future__ import annotations

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import manager_node
import queue_store
import shared_locks
import worker_node


class WorkerNodeTests(unittest.TestCase):
    def make_config(self, temp_dir: str) -> dict:
        root = Path(temp_dir)
        return {
            "output_root": str(root / "output"),
            "libraries": {"anime": ["dummy"]},
            "shared_state_dir": str(root / ".ripper_state"),
            "local_work_dir": str(root / "work"),
            "node": {"id": "test-node", "roles": {"web_ui": False, "worker": True, "manager": False}},
            "worker": {
                "enabled": True,
                "schedule": {"enabled": False, "start": "02:00", "end": "07:00", "outside_window_behavior": "finish_current_do_not_start_new"},
            },
        }

    def enough_local_space(self, config: dict) -> dict:
        return {
            "local_work_dir": str(config["local_work_dir"]),
            "required_bytes": 1024,
            "required_gb": 0.0,
            "available_bytes": 1024 * 1024 * 1024,
            "available_gb": 1.0,
            "enough_space": True,
        }

    def enqueue_job(self, config: dict) -> None:
        queue_store.enqueue_job(
            config,
            {
                "schema_version": 1,
                "job_id": "job_worker123",
                "status": "queue",
                "source_path": "/library/file.mkv",
                "source_size_bytes": 100,
                "media_type": "anime",
                "bucket": "anime_high",
            },
        )

    def enqueue_real_source_job(self, config: dict, source: Path) -> None:
        queue_store.enqueue_job(
            config,
            {
                "schema_version": 1,
                "job_id": "job_worker_execute",
                "status": "queue",
                "source_path": str(source),
                "source_size_bytes": source.stat().st_size,
                "media_type": "anime",
                "bucket": "anime_high",
                "duration_seconds": 120.0,
                "audio_stream_count": 1,
                "subtitle_stream_count": 0,
            },
        )

    def test_required_local_space_uses_source_size_and_margin(self) -> None:
        required = worker_node.required_local_space_bytes({"source_size_bytes": 1024})

        self.assertEqual(required, int(1024 * 1.5 + 5 * 1024 * 1024 * 1024))

    def test_map_canonical_to_local_path_rewrites_worker_mount(self) -> None:
        config = {
            "node_path_mappings": [
                {
                    "canonical_prefix": "/mnt/nas/filmy/",
                    "local_prefix": "/mnt/nas-backup/",
                }
            ]
        }

        mapped = worker_node.map_canonical_to_local_path(config, "/mnt/nas/filmy/ANIME/a.mkv")

        self.assertEqual(mapped, "/mnt/nas-backup/ANIME/a.mkv")

    def test_map_local_to_canonical_path_rewrites_ready_output_mount(self) -> None:
        config = {
            "node_path_mappings": [
                {
                    "canonical_prefix": "/mnt/nas/filmy/",
                    "local_prefix": "/mnt/nas-backup/",
                }
            ]
        }

        mapped = worker_node.map_local_to_canonical_path(config, "/mnt/nas-backup/RIPTEST/.ripper_state/ready_outputs/job/output.mkv")

        self.assertEqual(mapped, "/mnt/nas/filmy/RIPTEST/.ripper_state/ready_outputs/job/output.mkv")

    def test_time_window_boundaries_match_finish_current_policy(self) -> None:
        start = worker_node.parse_hhmm("02:00")
        end = worker_node.parse_hhmm("07:00")

        self.assertTrue(worker_node.time_is_inside_window(worker_node.parse_hhmm("02:00"), start, end))
        self.assertTrue(worker_node.time_is_inside_window(worker_node.parse_hhmm("06:59"), start, end))
        self.assertFalse(worker_node.time_is_inside_window(worker_node.parse_hhmm("07:00"), start, end))

    def test_worker_step_skips_when_global_queue_paused(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)
            queue_store.set_global_control(config, "paused", allow_new_claims=False, allow_finalizer=True, updated_by="test")

            result = worker_node.worker_step(config)
            status = queue_store.queue_status(config)

            self.assertEqual(result["reason"], "global_queue_paused")
            self.assertEqual(status["states"]["queue"], 1)
            self.assertEqual(status["states"]["running"], 0)

    def test_worker_step_requeues_job_and_writes_heartbeat(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)

            with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                result = worker_node.worker_step(config, dry_run_result="requeue")
            heartbeat_path = Path(config["shared_state_dir"]) / "workers" / "test-node.json"
            status = queue_store.queue_status(config)

            self.assertEqual(result["status"], "dry_run_complete")
            self.assertEqual(result["result_state"], "queue")
            self.assertTrue(heartbeat_path.exists())
            self.assertEqual(status["states"]["queue"], 1)
            self.assertEqual(status["states"]["running"], 0)

    def test_worker_step_can_mark_ready_for_finalize(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)

            with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                result = worker_node.worker_step(config, dry_run_result="ready")
            job = queue_store.read_json(Path(result["job_path"]))
            status = queue_store.queue_status(config)

            self.assertEqual(result["result_state"], "ready_for_finalize")
            self.assertTrue(Path(job["ready_output_dir"]).is_dir())
            self.assertTrue(Path(job["ready_output_path"]).exists())
            self.assertTrue(Path(job["ready_output_manifest"]).exists())
            self.assertTrue(Path(job["ready_output_ffprobe"]).exists())
            self.assertTrue(Path(job["ready_output_worker_log"]).exists())
            self.assertTrue(Path(job["ready_output_checksum"]).exists())
            self.assertEqual(result["local_processing"]["source_copy_mode"], "placeholder")
            self.assertTrue(result["local_cleanup"]["removed"])
            self.assertFalse(Path(result["local_cleanup"]["path"]).exists())
            self.assertEqual(status["states"]["queue"], 0)
            self.assertEqual(status["states"]["ready_for_finalize"], 1)

    def test_worker_dry_run_handoff_uses_canonical_ready_paths_with_node_mappings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_root = Path(temp_dir)
            canonical_mount = temp_root / "canonical_mount"
            worker_mount = temp_root / "worker_mount"
            worker_source = worker_mount / "ANIME" / "episode01.mkv"
            canonical_source = canonical_mount / "ANIME" / "episode01.mkv"
            worker_source.parent.mkdir(parents=True, exist_ok=True)
            canonical_source.parent.mkdir(parents=True, exist_ok=True)
            worker_source.write_text("source-data\n", encoding="utf-8")
            canonical_source.write_text("source-data\n", encoding="utf-8")

            worker_config = {
                "output_root": str(worker_mount / "RIPTEST"),
                "libraries": {"anime": [str(canonical_mount / "ANIME")]},
                "shared_state_dir": str(worker_mount / "RIPTEST" / ".ripper_state"),
                "local_work_dir": str(temp_root / "work"),
                "node": {"id": "gaming-worker", "roles": {"web_ui": True, "worker": True, "manager": False}},
                "worker": {
                    "enabled": True,
                    "schedule": {"enabled": False, "start": "02:00", "end": "07:00", "outside_window_behavior": "finish_current_do_not_start_new"},
                },
                "node_path_mappings": [
                    {
                        "canonical_prefix": str(canonical_mount),
                        "local_prefix": str(worker_mount),
                    }
                ],
            }
            manager_config = {
                "output_root": str(canonical_mount / "RIPTEST"),
                "libraries": {"anime": [str(canonical_mount / "ANIME")]},
                "shared_state_dir": str(canonical_mount / "RIPTEST" / ".ripper_state"),
                "node": {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}},
                "manager": {"enabled": True, "run_continuously": True, "require_successful_jellyfin_refresh": False},
                "io_limits": {"use_shared_locks": True, "max_concurrent_finalizers": 1},
                "node_path_mappings": [
                    {
                        "canonical_prefix": str(canonical_mount),
                        "local_prefix": str(canonical_mount),
                    }
                ],
            }
            queue_store.enqueue_job(
                worker_config,
                {
                    "schema_version": 1,
                    "job_id": "job_mapped_ready",
                    "status": "queue",
                    "source_path": str(canonical_source),
                    "source_size_bytes": worker_source.stat().st_size,
                    "media_type": "anime",
                    "bucket": "anime_high",
                    "duration_seconds": 120.0,
                    "audio_stream_count": 1,
                    "subtitle_stream_count": 0,
                },
            )

            with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(worker_config)):
                result = worker_node.worker_step(worker_config, dry_run_result="ready")
            worker_job = queue_store.read_json(Path(result["job_path"]))
            canonical_manifest_path = Path(str(worker_job["ready_output_manifest"]))
            worker_manifest_path = Path(worker_node.map_canonical_to_local_path(worker_config, str(canonical_manifest_path)))
            worker_manifest = queue_store.read_json(worker_manifest_path)
            worker_heartbeat = queue_store.read_json(Path(worker_config["shared_state_dir"]) / "workers" / "gaming-worker.json")

            expected_worker_local_source = str(worker_source)
            expected_canonical_output_dir = str(canonical_mount / "RIPTEST" / ".ripper_state" / "ready_outputs" / "job_mapped_ready")
            expected_canonical_output_path = str(canonical_mount / "RIPTEST" / ".ripper_state" / "ready_outputs" / "job_mapped_ready" / "output.mkv")

            self.assertEqual(worker_job["source_path"], str(canonical_source))
            self.assertEqual(result["source_path"], str(canonical_source))
            self.assertEqual(result["local_processing"]["canonical_source_path"], str(canonical_source))
            self.assertEqual(result["local_processing"]["worker_local_source_path"], expected_worker_local_source)
            self.assertTrue(str(worker_job["ready_output_manifest"]).startswith(str(canonical_mount)))
            self.assertTrue(str(worker_job["ready_output_path"]).startswith(str(canonical_mount)))
            self.assertTrue(str(result["local_processing"]["worker_local_source_path"]).startswith(str(worker_mount)))
            self.assertEqual(worker_job["ready_output_dir"], expected_canonical_output_dir)
            self.assertEqual(worker_job["ready_output_path"], expected_canonical_output_path)
            self.assertEqual(worker_job["ready_output_manifest"], str(canonical_manifest_path))
            self.assertTrue(str(worker_manifest_path).startswith(str(worker_mount)))
            self.assertEqual(worker_manifest["ready_output_dir"], expected_canonical_output_dir)
            self.assertEqual(worker_manifest["ready_output_path"], expected_canonical_output_path)
            self.assertEqual(worker_manifest["ffprobe_path"], str(canonical_mount / "RIPTEST" / ".ripper_state" / "ready_outputs" / "job_mapped_ready" / "output.ffprobe.json"))
            self.assertEqual(worker_manifest["worker_log_path"], str(canonical_mount / "RIPTEST" / ".ripper_state" / "ready_outputs" / "job_mapped_ready" / "worker_log.json"))
            self.assertEqual(worker_manifest["checksum_path"], str(canonical_mount / "RIPTEST" / ".ripper_state" / "ready_outputs" / "job_mapped_ready" / "checksum.sha256"))
            self.assertEqual(worker_heartbeat["worker_state"], "idle")
            self.assertEqual(worker_heartbeat["current_phase"], "dry_run_complete")
            self.assertIsNone(worker_heartbeat["current_job_id"])

            manager_root = queue_store.init_state(manager_config)
            worker_root = Path(worker_config["shared_state_dir"])
            shutil.copy2(worker_root / "ready_for_finalize" / "job_mapped_ready.json", manager_root / "ready_for_finalize" / "job_mapped_ready.json")
            shutil.copytree(worker_manifest_path.parent, manager_root / "ready_outputs" / "job_mapped_ready", dirs_exist_ok=True)

            manager_result = manager_node.manager_step(manager_config, dry_run_result="done")
            status = queue_store.queue_status(manager_config)

            self.assertEqual(manager_result["status"], "dry_run_complete")
            self.assertEqual(manager_result["result_state"], "done")
            self.assertTrue(manager_result["ready_output_check"]["ok"])
            self.assertEqual(manager_result["ready_output_check"]["ready_output_path"], expected_canonical_output_path)
            self.assertTrue(manager_result["verification"]["ok"])
            self.assertEqual(status["states"]["done"], 1)

    def test_worker_step_requeues_when_nas_read_lock_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)
            held_lock = shared_locks.acquire_lock(config, "nas_read", "other-worker", "job-other")

            try:
                with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                    result = worker_node.worker_step(config, dry_run_result="ready")
            finally:
                shared_locks.release_lock(Path(held_lock["path"]))

            status = queue_store.queue_status(config)
            self.assertEqual(result["reason"], "no_nas_read_slot")
            self.assertEqual(result["result_state"], "queue")
            self.assertEqual(status["states"]["queue"], 1)
            self.assertEqual(status["states"]["ready_for_finalize"], 0)

    def test_worker_step_requeues_and_cleans_local_workspace_when_nas_write_lock_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)
            held_lock = shared_locks.acquire_lock(config, "nas_write", "other-worker", "job-other")

            try:
                with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                    result = worker_node.worker_step(config, dry_run_result="ready")
            finally:
                shared_locks.release_lock(Path(held_lock["path"]))

            status = queue_store.queue_status(config)
            self.assertEqual(result["reason"], "no_nas_write_slot")
            self.assertEqual(result["result_state"], "queue")
            self.assertTrue(result["local_cleanup"]["removed"])
            self.assertFalse(Path(result["local_cleanup"]["path"]).exists())
            self.assertEqual(status["states"]["queue"], 1)
            self.assertEqual(status["states"]["ready_for_finalize"], 0)

    def test_worker_step_execute_marks_ready_for_finalize_with_local_encode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            self.enqueue_real_source_job(config, source)

            def fake_encode(_config: dict, _job: dict, local_cache: dict, node: str | None = None) -> dict:
                output_path = Path(str(local_cache["work_dir"])) / "output" / "output.mkv"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text("encoded-data\n", encoding="utf-8")
                return {
                    "ok": True,
                    "ffmpeg_command": ["ffmpeg", "-i", str(local_cache["local_source_path"]), str(output_path)],
                    "verification": {"output_exists": True},
                    "output_summary": {"video_codec": "hevc", "audio_stream_count": 1, "subtitle_stream_count": 0},
                    "errors": [],
                    "local_output_path": str(output_path),
                    "local_output_size_bytes": output_path.stat().st_size,
                }

            with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                with patch.object(worker_node, "run_local_ffmpeg_encode", side_effect=fake_encode):
                    result = worker_node.worker_step(config, dry_run_result="ready", execute=True)

            status = queue_store.queue_status(config)
            job = queue_store.read_json(Path(result["job_path"]))
            self.assertEqual(result["result_state"], "ready_for_finalize")
            self.assertEqual(result["execution_mode"], "execute")
            self.assertEqual(result["local_processing"]["output_summary"]["video_codec"], "hevc")
            self.assertEqual(job["execution_mode"], "execute")
            self.assertFalse(job["dry_run"])
            self.assertTrue(Path(job["ready_output_path"]).exists())
            self.assertEqual(status["states"]["ready_for_finalize"], 1)

    def test_run_local_ffmpeg_encode_terminates_when_hard_stop_is_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            work_dir = Path(temp_dir) / "work" / "job"
            output_path = work_dir / "output" / "output.mkv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("partial-output\n", encoding="utf-8")
            queue_store.set_node_control(config, "test-node", worker_command="hard_stop")

            class FakeProcess:
                def __init__(self) -> None:
                    self.returncode = None
                    self.terminated = False

                def poll(self) -> int | None:
                    return None

                def terminate(self) -> None:
                    self.terminated = True
                    self.returncode = -15

                def wait(self, timeout: float | None = None) -> int:
                    self.returncode = -15
                    return -15

                def kill(self) -> None:
                    self.returncode = -9

                def communicate(self) -> tuple[str, str]:
                    return ("", "terminated")

            fake_process = FakeProcess()
            local_cache = {"local_source_path": str(source), "work_dir": str(work_dir)}
            job = {"job_id": "job_worker_execute", "media_type": "anime", "source_path": str(source), "source_size_bytes": source.stat().st_size}

            with patch("media_normalizer.build_ffmpeg_command", return_value=["ffmpeg", "-i", str(source), str(output_path)]), patch("track_policy.apply_track_policy", return_value={}), patch("subprocess.Popen", return_value=fake_process), patch("worker_node.shutil.which", return_value="ffmpeg"):
                result = worker_node.run_local_ffmpeg_encode(config, job, local_cache)

            self.assertFalse(result["ok"])
            self.assertEqual(result["reason"], "hard_stop_requested")
            self.assertTrue(result["partial_output_deleted"])
            self.assertTrue(result["source_untouched"])
            self.assertTrue(fake_process.terminated)
            self.assertFalse(output_path.exists())

    def test_run_local_ffmpeg_encode_terminates_when_schedule_hard_stop_is_reached(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            config["worker"]["schedule"] = {
                "enabled": True,
                "start": "02:00",
                "end": "07:00",
                "outside_window_behavior": "hard_stop",
            }
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            work_dir = Path(temp_dir) / "work" / "job"
            output_path = work_dir / "output" / "output.mkv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text("partial-output\n", encoding="utf-8")

            class FakeProcess:
                def __init__(self) -> None:
                    self.returncode = None
                    self.terminated = False

                def poll(self) -> int | None:
                    return None

                def terminate(self) -> None:
                    self.terminated = True
                    self.returncode = -15

                def wait(self, timeout: float | None = None) -> int:
                    self.returncode = -15
                    return -15

                def kill(self) -> None:
                    self.returncode = -9

                def communicate(self) -> tuple[str, str]:
                    return ("", "terminated")

            fake_process = FakeProcess()
            local_cache = {"local_source_path": str(source), "work_dir": str(work_dir)}
            job = {"job_id": "job_worker_execute", "media_type": "anime", "source_path": str(source), "source_size_bytes": source.stat().st_size}

            with patch("media_normalizer.build_ffmpeg_command", return_value=["ffmpeg", "-i", str(source), str(output_path)]), patch("track_policy.apply_track_policy", return_value={}), patch("subprocess.Popen", return_value=fake_process), patch("worker_node.shutil.which", return_value="ffmpeg"), patch.object(worker_node, "worker_schedule_check", return_value={"enabled": True, "start": "02:00", "end": "07:00", "outside_window_behavior": "hard_stop", "checked_at": "2026-05-10T07:00:00+02:00", "allowed_to_claim": False}):
                result = worker_node.run_local_ffmpeg_encode(config, job, local_cache)

            self.assertFalse(result["ok"])
            self.assertEqual(result["reason"], "schedule_hard_stop_requested")
            self.assertTrue(result["partial_output_deleted"])
            self.assertTrue(result["source_untouched"])
            self.assertTrue(fake_process.terminated)
            self.assertFalse(output_path.exists())

    def test_run_local_ffmpeg_encode_redirects_stderr_to_log_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            work_dir = Path(temp_dir) / "work" / "job"
            output_path = work_dir / "output" / "output.mkv"
            local_cache = {"local_source_path": str(source), "work_dir": str(work_dir)}
            job = {"job_id": "job_worker_execute", "media_type": "anime", "source_path": str(source), "source_size_bytes": source.stat().st_size}
            popen_kwargs: dict[str, Any] = {}

            class FakeProcess:
                def __init__(self) -> None:
                    self.returncode = 0
                    self.pid = 4242

                def poll(self) -> int | None:
                    return self.returncode

            def fake_popen(command: list[str], stdout: Any = None, stderr: Any = None) -> FakeProcess:
                popen_kwargs["command"] = command
                popen_kwargs["stdout"] = stdout
                popen_kwargs["stderr"] = stderr
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_text("encoded-data\n", encoding="utf-8")
                stderr.write("ffmpeg stderr line\n" + ("x" * 200000))
                stderr.flush()
                return FakeProcess()

            with patch("media_normalizer.build_ffmpeg_command", return_value=["ffmpeg", "-i", str(source), str(output_path)]), patch("track_policy.apply_track_policy", return_value={}), patch("media_normalizer.verify_output", return_value=({"output_exists": True}, {"video_codec": "hevc"}, [])), patch("subprocess.Popen", side_effect=fake_popen), patch("worker_node.shutil.which", return_value="ffmpeg"):
                result = worker_node.run_local_ffmpeg_encode(config, job, local_cache, node="test-node")

            self.assertTrue(result["ok"])
            self.assertIs(popen_kwargs["stdout"], subprocess.DEVNULL)
            self.assertIsNot(popen_kwargs["stderr"], subprocess.PIPE)
            self.assertEqual(result["ffmpeg_pid"], 4242)
            log_path = Path(str(result["ffmpeg_log_path"]))
            self.assertTrue(log_path.exists())
            self.assertIn("ffmpeg stderr line", log_path.read_text(encoding="utf-8", errors="replace"))

    def test_worker_step_execute_accepts_binary_output_with_invalid_utf8_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            self.enqueue_real_source_job(config, source)

            def fake_encode(_config: dict, _job: dict, local_cache: dict, node: str | None = None) -> dict:
                output_path = Path(str(local_cache["work_dir"])) / "output" / "output.mkv"
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(b"\xa3\xff\x00binary-output")
                return {
                    "ok": True,
                    "ffmpeg_command": ["ffmpeg", "-i", str(local_cache["local_source_path"]), str(output_path)],
                    "verification": {"output_exists": True},
                    "output_summary": {"video_codec": "hevc", "audio_stream_count": 1, "subtitle_stream_count": 0},
                    "errors": [],
                    "local_output_path": str(output_path),
                    "local_output_size_bytes": output_path.stat().st_size,
                    "ffmpeg_log_tail": "bad\ufffdtext",
                }

            with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                with patch.object(worker_node, "run_local_ffmpeg_encode", side_effect=fake_encode):
                    result = worker_node.worker_step(config, dry_run_result="ready", execute=True)

            job = queue_store.read_json(Path(result["job_path"]))
            checksum_path = Path(str(job["ready_output_checksum"]))
            self.assertEqual(result["result_state"], "ready_for_finalize")
            self.assertTrue(Path(job["ready_output_path"]).exists())
            self.assertTrue(checksum_path.exists())
            self.assertRegex(checksum_path.read_text(encoding="utf-8"), r"^[0-9a-f]{64}  output\.mkv\n$")

    def test_worker_step_execute_moves_job_to_interrupted_when_hard_stop_is_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            self.enqueue_real_source_job(config, source)

            with patch.object(
                worker_node,
                "run_local_ffmpeg_encode",
                return_value={
                    "ok": False,
                    "reason": "hard_stop_requested",
                    "partial_output_deleted": True,
                    "source_untouched": True,
                },
            ):
                with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                    result = worker_node.worker_step(config, dry_run_result="ready", execute=True)

            status = queue_store.queue_status(config)
            job = queue_store.read_json(Path(result["job_path"]))
            interrupted_log = Path(str(result["interrupted_log"]))
            interrupted_payload = queue_store.read_json(interrupted_log)
            self.assertEqual(result["status"], "interrupted")
            self.assertEqual(result["result_state"], "interrupted")
            self.assertTrue(result["local_cleanup"]["removed"])
            self.assertTrue(job["source_untouched"])
            self.assertTrue(job["partial_output_deleted"])
            self.assertEqual(job["interrupted_log"], str(interrupted_log))
            self.assertTrue(interrupted_log.exists())
            self.assertEqual(interrupted_payload["status"], "interrupted")
            self.assertEqual(interrupted_payload["reason"], "hard_stop_requested")
            self.assertFalse(interrupted_payload["requeued"])
            self.assertTrue(interrupted_payload["source_untouched"])
            self.assertTrue(interrupted_payload["partial_output_deleted"])
            self.assertEqual(status["states"]["interrupted"], 1)

    def test_worker_step_execute_moves_job_to_interrupted_when_schedule_hard_stop_is_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            self.enqueue_real_source_job(config, source)

            with patch.object(
                worker_node,
                "run_local_ffmpeg_encode",
                return_value={
                    "ok": False,
                    "reason": "schedule_hard_stop_requested",
                    "partial_output_deleted": True,
                    "source_untouched": True,
                },
            ):
                with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                    result = worker_node.worker_step(config, dry_run_result="ready", execute=True)

            status = queue_store.queue_status(config)
            job = queue_store.read_json(Path(result["job_path"]))
            self.assertEqual(result["status"], "interrupted")
            self.assertEqual(result["reason"], "schedule_hard_stop_requested")
            self.assertEqual(result["result_state"], "interrupted")
            self.assertTrue(job["source_untouched"])
            self.assertTrue(job["partial_output_deleted"])
            self.assertEqual(status["states"]["interrupted"], 1)

    def test_worker_step_execute_requeues_when_active_encode_lock_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            self.enqueue_real_source_job(config, source)
            held_lock = shared_locks.acquire_lock(config, "active_encode", "other-worker", "job-other")

            try:
                with patch.object(worker_node, "local_space_check", return_value=self.enough_local_space(config)):
                    result = worker_node.worker_step(config, dry_run_result="ready", execute=True)
            finally:
                shared_locks.release_lock(Path(held_lock["path"]))

            status = queue_store.queue_status(config)
            self.assertEqual(result["reason"], "no_active_encode_slot")
            self.assertEqual(result["result_state"], "queue")
            self.assertTrue(result["local_cleanup"]["removed"])
            self.assertEqual(status["states"]["queue"], 1)
            self.assertEqual(status["states"]["ready_for_finalize"], 0)

    def test_worker_step_requeues_when_local_space_is_low(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)

            with patch.object(worker_node, "local_space_check", return_value={"enough_space": False, "required_bytes": 1000, "available_bytes": 1}):
                result = worker_node.worker_step(config, dry_run_result="ready")

            status = queue_store.queue_status(config)
            self.assertEqual(result["reason"], "not_enough_local_space")
            self.assertEqual(result["result_state"], "queue")
            self.assertEqual(status["states"]["queue"], 1)
            self.assertEqual(status["states"]["ready_for_finalize"], 0)

    def test_worker_loop_stops_on_idle_without_sleep(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(worker_node, "worker_step", return_value={"status": "idle", "reason": "no_job_available", "node_id": "test-node"}):
                result = worker_node.worker_loop(config, stop_on_idle=True, sleeper=sleep_calls.append)

            self.assertEqual(result["status"], "loop_complete")
            self.assertEqual(result["stop_reason"], "idle")
            self.assertEqual(result["iterations"], 1)
            self.assertEqual(result["sleep_calls"], 0)
            self.assertEqual(sleep_calls, [])

    def test_worker_loop_sleeps_between_idle_iterations_until_max_iterations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            config["worker"]["loop"] = {"empty_backoff_seconds": 0.25}
            sleep_calls: list[float] = []
            side_effect = [
                {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
                {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
            ]

            with patch.object(worker_node, "worker_step", side_effect=side_effect):
                result = worker_node.worker_loop(config, max_iterations=2, idle_sleep_seconds=0.25, sleeper=sleep_calls.append)

            self.assertEqual(result["stop_reason"], "max_iterations")
            self.assertEqual(result["iterations"], 2)
            self.assertEqual(result["sleep_calls"], 1)
            self.assertEqual(sleep_calls, [0.25])

    def test_worker_loop_uses_hour_backoff_when_no_job_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(worker_node, "worker_step", side_effect=[
                {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
                {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
            ]):
                result = worker_node.worker_loop(config, max_iterations=2, sleeper=sleep_calls.append)

            self.assertEqual(result["empty_backoff_seconds"], 3600.0)
            self.assertEqual(sleep_calls, [3600.0])

    def test_worker_loop_retries_once_after_completed_job_before_hour_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(worker_node, "worker_step", side_effect=[
                {"status": "dry_run_complete", "reason": None, "node_id": "test-node", "job_id": "job-1"},
                {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
                {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
            ]):
                result = worker_node.worker_loop(config, max_iterations=3, sleeper=sleep_calls.append)

            self.assertEqual(result["iterations"], 3)
            self.assertEqual(result["status_counts"]["dry_run_complete"], 1)
            self.assertEqual(result["status_counts"]["idle"], 2)
            self.assertEqual(sleep_calls, [3600.0])

    def test_worker_loop_processes_job_then_stops_on_idle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(
                worker_node,
                "worker_step",
                side_effect=[
                    {"status": "dry_run_complete", "reason": None, "node_id": "test-node", "job_id": "job_worker123"},
                    {"status": "idle", "reason": "no_job_available", "node_id": "test-node"},
                ],
            ):
                result = worker_node.worker_loop(config, dry_run_result="ready", stop_on_idle=True, sleeper=sleep_calls.append)

            self.assertEqual(result["stop_reason"], "idle")
            self.assertEqual(result["iterations"], 2)
            self.assertEqual(result["status_counts"]["dry_run_complete"], 1)
            self.assertEqual(result["status_counts"]["idle"], 1)
            self.assertEqual(sleep_calls, [])

    def test_worker_loop_stops_immediately_when_stop_after_current_is_requested_before_start(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.set_node_control(config, "test-node", worker_command="stop_after_current")

            with patch.object(worker_node, "worker_step") as step_mock:
                result = worker_node.worker_loop(config)

            self.assertEqual(result["stop_reason"], "stop_after_current")
            self.assertEqual(result["iterations"], 0)
            self.assertEqual(result["stop_command"], "stop_after_current")
            step_mock.assert_not_called()

    def test_worker_loop_stops_immediately_when_hard_stop_is_requested_before_start(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.set_node_control(config, "test-node", worker_command="hard_stop")

            with patch.object(worker_node, "worker_step") as step_mock:
                result = worker_node.worker_loop(config)

            self.assertEqual(result["stop_reason"], "hard_stop")
            self.assertEqual(result["iterations"], 0)
            self.assertEqual(result["stop_command"], "hard_stop")
            step_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
