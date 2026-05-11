from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import manager_node
import queue_store
import shared_locks


class ManagerNodeTests(unittest.TestCase):
    def make_config(self, temp_dir: str, enabled: bool = True, require_successful_jellyfin_refresh: bool = False) -> dict:
        root = Path(temp_dir)
        return {
            "output_root": str(root / "output"),
            "libraries": {"anime": ["dummy"]},
            "shared_state_dir": str(root / ".ripper_state"),
            "node": {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}},
            "manager": {"enabled": enabled, "run_continuously": True, "require_successful_jellyfin_refresh": require_successful_jellyfin_refresh},
            "io_limits": {"use_shared_locks": True, "max_concurrent_finalizers": 1},
        }

    def enqueue_ready_job(self, config: dict, job: dict | None = None) -> None:
        root = queue_store.init_state(config)
        payload = job or {
            "schema_version": 1,
            "job_id": "job_ready123",
            "status": "ready_for_finalize",
            "source_path": "/library/file.mkv",
            "media_type": "anime",
            "bucket": "anime_high",
        }
        queue_store.write_json_atomic(root / "ready_for_finalize" / f"{payload['job_id']}.json", payload)

    def create_source_file(self, temp_dir: str, relative_path: str = "library/file.mkv") -> Path:
        source = Path(temp_dir) / relative_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("source-data\n", encoding="utf-8")
        return source

    def test_manager_step_skips_when_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir, enabled=False)
            self.enqueue_ready_job(config)

            result = manager_node.manager_step(config)
            status = queue_store.queue_status(config)

            self.assertEqual(result["reason"], "manager_disabled")
            self.assertEqual(status["states"]["ready_for_finalize"], 1)
            self.assertEqual(status["states"]["finalizing"], 0)

    def test_manager_step_skips_when_global_finalizer_paused(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_ready_job(config)
            queue_store.set_global_control(config, "maintenance", allow_new_claims=False, allow_finalizer=False, updated_by="test")

            result = manager_node.manager_step(config)
            status = queue_store.queue_status(config)

            self.assertEqual(result["reason"], "global_finalizer_paused")
            self.assertEqual(status["states"]["ready_for_finalize"], 1)
            self.assertEqual(status["states"]["finalizing"], 0)

    def test_manager_step_marks_ready_job_done_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_ready123"
            ready_dir.mkdir(parents=True)
            (ready_dir / "output.mkv").write_text("dry-run output\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_ready123", "source_path": str(source)})
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_ready123", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(
                ready_dir / "manifest.json",
                {
                    "job_id": "job_ready123",
                    "source_path": str(source),
                    "ready_output_path": str(ready_dir / "output.mkv"),
                    "dry_run": True,
                    "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1},
                    "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15, "file_size_mb": 0.001, "container_format": "matroska", "video_width": 1920, "video_height": 1080, "video_pix_fmt": "yuv420p10le"},
                },
            )
            queue_store.write_json_atomic(
                ready_dir / "output.ffprobe.json",
                {
                    "job_id": "job_ready123",
                    "source_path": str(source),
                    "dry_run": True,
                    "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1},
                    "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15, "file_size_mb": 0.001, "container_format": "matroska", "video_width": 1920, "video_height": 1080, "video_pix_fmt": "yuv420p10le"},
                },
            )
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_ready123", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "bucket": "anime_high", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(ready_dir / "output.mkv"), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            result = manager_node.manager_step(config, dry_run_result="done")
            heartbeat = Path(config["shared_state_dir"]) / "manager" / "media-server.json"
            finalization_log = Path(result["finalization_log"])
            status = queue_store.queue_status(config)

            self.assertEqual(result["result_state"], "done")
            self.assertTrue(result["finalizer_lock"]["acquired"])
            self.assertEqual(result["ready_output_check"]["missing_files"], [])
            self.assertEqual(result["verification"]["mode"], "dry_run_bundle")
            self.assertTrue(result["verification"]["ok"])
            self.assertTrue(result["original_source_check"]["ok"])
            self.assertEqual(result["finalization_plan"]["replacement_path"], str(source))
            self.assertTrue(result["finalization_plan"]["quarantine_path"].endswith(".original"))
            self.assertTrue(Path(result["quarantine_manifest_path"]).exists())
            self.assertEqual(queue_store.read_json(Path(result["quarantine_manifest_path"]))["status"], "planned")
            self.assertTrue(result["ready_output_cleanup"]["removed"])
            self.assertTrue(heartbeat.exists())
            self.assertTrue(finalization_log.exists())
            self.assertFalse(ready_dir.exists())
            self.assertEqual(queue_store.read_json(finalization_log)["status"], "done")
            self.assertEqual(status["states"]["ready_for_finalize"], 0)
            self.assertEqual(status["states"]["done"], 1)
            self.assertEqual(shared_locks.lock_status(config)["finalizer"]["active"], 0)

    def test_manager_step_skips_when_finalizer_lock_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_ready_job(config)
            held_lock = shared_locks.acquire_lock(config, "finalizer", "other-manager", "job-other")

            result = manager_node.manager_step(config)
            status = queue_store.queue_status(config)

            self.assertTrue(held_lock["acquired"])
            self.assertEqual(result["reason"], "finalizer_lock_unavailable")
            self.assertEqual(status["states"]["ready_for_finalize"], 1)
            self.assertEqual(status["states"]["finalizing"], 0)

    def test_manager_step_requeues_ready_job_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            self.enqueue_ready_job(
                config,
                {
                    "schema_version": 1,
                    "job_id": "job_ready123",
                    "status": "ready_for_finalize",
                    "source_path": str(source),
                },
            )

            result = manager_node.manager_step(config, dry_run_result="requeue")
            status = queue_store.queue_status(config)

            self.assertEqual(result["result_state"], "ready_for_finalize")
            self.assertIn("quarantine_manifest", result["quarantine_manifest_path"])
            self.assertFalse(result["ready_output_cleanup"]["removed"])
            self.assertEqual(status["states"]["ready_for_finalize"], 1)
            self.assertEqual(status["states"]["finalizing"], 0)

    def test_manager_step_failed_finalize_when_required_output_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            self.enqueue_ready_job(
                config,
                {
                    "schema_version": 1,
                    "job_id": "job_missing_output",
                    "status": "ready_for_finalize",
                    "source_path": str(source),
                    "ready_output_path": str(Path(temp_dir) / "missing.mkv"),
                },
            )

            result = manager_node.manager_step(config, dry_run_result="done")
            status = queue_store.queue_status(config)

            self.assertEqual(result["status"], "failed_finalize")
            self.assertEqual(result["reason"], "ready_output_missing")
            self.assertTrue(Path(result["finalization_log"]).exists())
            self.assertIn("output", result["ready_output_check"]["missing_files"])
            self.assertEqual(status["states"]["failed_finalize"], 1)
            self.assertEqual(status["states"]["done"], 0)

    def test_manager_step_failed_finalize_when_bundle_verification_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_bad_verify"
            ready_dir.mkdir(parents=True)
            (ready_dir / "output.mkv").write_text("dry-run output\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_bad_verify", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 100.0, "audio_stream_count": 1, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_bad_verify", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_bad_verify", "source_path": str(source), "ready_output_path": str(ready_dir / "output.mkv"), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_bad_verify", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(ready_dir / "output.mkv"), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            result = manager_node.manager_step(config, dry_run_result="done")
            status = queue_store.queue_status(config)

            self.assertEqual(result["reason"], "manager_verification_failed")
            self.assertFalse(result["verification"]["ok"])
            self.assertEqual(status["states"]["failed_finalize"], 1)

    def test_manager_step_failed_finalize_when_original_source_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_missing_source"
            ready_dir.mkdir(parents=True)
            (ready_dir / "output.mkv").write_text("dry-run output\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_missing_source", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_missing_source", "source_path": str(Path(temp_dir) / "missing-source.mkv"), "ready_output_path": str(ready_dir / "output.mkv"), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_missing_source", "source_path": str(Path(temp_dir) / "missing-source.mkv"), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_missing_source", "status": "ready_for_finalize", "source_path": str(Path(temp_dir) / "missing-source.mkv"), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(ready_dir / "output.mkv"), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            result = manager_node.manager_step(config, dry_run_result="done")
            status = queue_store.queue_status(config)

            self.assertEqual(result["reason"], "original_source_missing")
            self.assertFalse(result["original_source_check"]["ok"])
            self.assertEqual(status["states"]["failed_finalize"], 1)

    def test_manager_step_execute_moves_original_and_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_execute_ok"
            ready_dir.mkdir(parents=True)
            output_path = ready_dir / "output.mkv"
            output_path.write_text("encoded-data\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_execute_ok", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_execute_ok", "source_path": str(source), "ready_output_path": str(output_path), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_execute_ok", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_execute_ok", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(output_path), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            with patch.object(manager_node, "execute_jellyfin_refresh", return_value={"enabled": True, "status": "refreshed", "item_id": "abc123", "jellyfin_path": "/mnt/nas/filmy/library/file.mkv"}) as refresh_mock:
                result = manager_node.manager_step(config, execute=True)
            status = queue_store.queue_status(config)
            quarantine_path = Path(result["finalization_plan"]["quarantine_path"])

            self.assertEqual(result["status"], "complete")
            self.assertEqual(result["execution_mode"], "execute")
            self.assertTrue(result["execution_result"]["ok"])
            self.assertEqual(result["jellyfin_refresh"]["status"], "refreshed")
            refresh_mock.assert_called_once_with(config, str(source))
            self.assertFalse(output_path.exists())
            self.assertTrue(source.exists())
            self.assertEqual(source.read_text(encoding="utf-8"), "encoded-data\n")
            self.assertTrue(quarantine_path.exists())
            self.assertEqual(quarantine_path.read_text(encoding="utf-8"), "source-data\n")
            self.assertEqual(queue_store.read_json(Path(result["quarantine_manifest_path"]))["status"], "executed")
            self.assertEqual(status["states"]["done"], 1)

    def test_manager_step_execute_keeps_done_when_jellyfin_refresh_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_execute_jellyfin_fail"
            ready_dir.mkdir(parents=True)
            output_path = ready_dir / "output.mkv"
            output_path.write_text("encoded-data\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_execute_jellyfin_fail", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_execute_jellyfin_fail", "source_path": str(source), "ready_output_path": str(output_path), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_execute_jellyfin_fail", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_execute_jellyfin_fail", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(output_path), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            with patch.object(manager_node, "execute_jellyfin_refresh", return_value={"enabled": True, "status": "failed", "jellyfin_path": "/mnt/nas/filmy/library/file.mkv", "error": "simulated jellyfin outage"}):
                result = manager_node.manager_step(config, execute=True)

            status = queue_store.queue_status(config)
            self.assertEqual(result["status"], "complete")
            self.assertEqual(result["jellyfin_refresh"]["status"], "failed")
            self.assertTrue(result["execution_result"]["ok"])
            self.assertEqual(status["states"]["done"], 1)

    def test_manager_step_execute_fails_when_required_jellyfin_refresh_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir, require_successful_jellyfin_refresh=True)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_execute_jellyfin_required_fail"
            ready_dir.mkdir(parents=True)
            output_path = ready_dir / "output.mkv"
            output_path.write_text("encoded-data\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_execute_jellyfin_required_fail", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_execute_jellyfin_required_fail", "source_path": str(source), "ready_output_path": str(output_path), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_execute_jellyfin_required_fail", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_execute_jellyfin_required_fail", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(output_path), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            with patch.object(manager_node, "execute_jellyfin_refresh", return_value={"enabled": True, "status": "failed", "jellyfin_path": "/mnt/nas/filmy/library/file.mkv", "error": "simulated jellyfin outage"}):
                result = manager_node.manager_step(config, execute=True)

            status = queue_store.queue_status(config)
            quarantine_path = Path(result["finalization_plan"]["quarantine_path"])
            self.assertEqual(result["status"], "failed_finalize")
            self.assertEqual(result["reason"], "jellyfin_refresh_failed")
            self.assertEqual(result["jellyfin_refresh"]["status"], "failed")
            self.assertTrue(result["execution_result"]["ok"])
            self.assertEqual(source.read_text(encoding="utf-8"), "encoded-data\n")
            self.assertTrue(quarantine_path.exists())
            self.assertEqual(status["states"]["failed_finalize"], 1)

    def test_manager_step_execute_rolls_back_when_output_move_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_execute_fail"
            ready_dir.mkdir(parents=True)
            output_path = ready_dir / "output.mkv"
            output_path.write_text("encoded-data\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_execute_fail", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_execute_fail", "source_path": str(source), "ready_output_path": str(output_path), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_execute_fail", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_execute_fail", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(output_path), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            original_move = manager_node.shutil.move

            def fail_on_output_move(src: str, dst: str) -> str:
                if Path(src) == output_path:
                    raise OSError("simulated replacement failure")
                return original_move(src, dst)

            with patch.object(manager_node.shutil, "move", side_effect=fail_on_output_move):
                result = manager_node.manager_step(config, execute=True)

            status = queue_store.queue_status(config)
            quarantine_path = Path(result["finalization_plan"]["quarantine_path"])
            self.assertEqual(result["status"], "failed_finalize")
            self.assertEqual(result["reason"], "manager_execution_failed")
            self.assertFalse(result["execution_result"]["ok"])
            self.assertTrue(result["execution_result"]["rollback"]["attempted"])
            self.assertTrue(result["execution_result"]["rollback"]["completed"])
            self.assertTrue(source.exists())
            self.assertEqual(source.read_text(encoding="utf-8"), "source-data\n")
            self.assertTrue(output_path.exists())
            self.assertFalse(quarantine_path.exists())
            self.assertEqual(status["states"]["failed_finalize"], 1)

    def test_manager_loop_stops_on_idle_without_sleep(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(manager_node, "manager_step", return_value={"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"}):
                result = manager_node.manager_loop(config, stop_on_idle=True, sleeper=sleep_calls.append)

            self.assertEqual(result["status"], "loop_complete")
            self.assertEqual(result["stop_reason"], "idle")
            self.assertEqual(result["iterations"], 1)
            self.assertEqual(result["sleep_calls"], 0)
            self.assertEqual(sleep_calls, [])

    def test_manager_loop_sleeps_between_idle_iterations_until_max_iterations(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            config["manager"]["loop"] = {"empty_backoff_seconds": 0.25}
            sleep_calls: list[float] = []
            side_effect = [
                {"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"},
                {"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"},
            ]

            with patch.object(manager_node, "manager_step", side_effect=side_effect):
                result = manager_node.manager_loop(config, max_iterations=2, idle_sleep_seconds=0.25, sleeper=sleep_calls.append)

            self.assertEqual(result["stop_reason"], "max_iterations")
            self.assertEqual(result["iterations"], 2)
            self.assertEqual(result["sleep_calls"], 1)
            self.assertEqual(sleep_calls, [0.25])

    def test_manager_loop_uses_hour_backoff_when_no_ready_job_available(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(manager_node, "manager_step", side_effect=[
                {"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"},
                {"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"},
            ]):
                result = manager_node.manager_loop(config, max_iterations=2, sleeper=sleep_calls.append)

            self.assertEqual(result["empty_backoff_seconds"], 3600.0)
            self.assertEqual(sleep_calls, [3600.0])

    def test_manager_loop_retries_once_after_completed_job_before_hour_backoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            sleep_calls: list[float] = []

            with patch.object(manager_node, "manager_step", side_effect=[
                {"status": "dry_run_complete", "reason": None, "node_id": "media-server", "job_id": "job-1"},
                {"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"},
                {"status": "idle", "reason": "no_ready_job_available", "node_id": "media-server"},
            ]):
                result = manager_node.manager_loop(config, max_iterations=3, sleeper=sleep_calls.append)

            self.assertEqual(result["iterations"], 3)
            self.assertEqual(result["status_counts"]["dry_run_complete"], 1)
            self.assertEqual(result["status_counts"]["idle"], 2)
            self.assertEqual(sleep_calls, [3600.0])

    def test_manager_loop_processes_job_then_stops_on_idle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            source = self.create_source_file(temp_dir)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_loop_ready"
            ready_dir.mkdir(parents=True)
            output_path = ready_dir / "output.mkv"
            output_path.write_text("dry-run output\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_loop_ready", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_loop_ready", "source_path": str(source), "ready_output_path": str(output_path), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_loop_ready", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            self.enqueue_ready_job(config, {"schema_version": 1, "job_id": "job_loop_ready", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(output_path), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            result = manager_node.manager_loop(config, dry_run_result="done", stop_on_idle=True)
            status = queue_store.queue_status(config)

            self.assertEqual(result["stop_reason"], "idle")
            self.assertEqual(result["iterations"], 2)
            self.assertEqual(result["status_counts"]["dry_run_complete"], 1)
            self.assertEqual(result["status_counts"]["idle"], 1)
            self.assertEqual(status["states"]["done"], 1)

    def test_manager_loop_stops_immediately_when_stop_after_current_is_requested_before_start(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.set_node_control(config, "media-server", manager_command="stop_after_current")

            with patch.object(manager_node, "manager_step") as step_mock:
                result = manager_node.manager_loop(config)

            self.assertEqual(result["stop_reason"], "stop_after_current")
            self.assertEqual(result["iterations"], 0)
            self.assertEqual(result["stop_command"], "stop_after_current")
            step_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
