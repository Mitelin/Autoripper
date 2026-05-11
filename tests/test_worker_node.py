from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

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

    def test_worker_step_requeues_when_nas_read_lock_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            self.enqueue_job(config)
            held_lock = shared_locks.acquire_lock(config, "nas_read", "other-worker", "job-other")

            try:
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

            def fake_encode(_config: dict, _job: dict, local_cache: dict) -> dict:
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
            self.enqueue_job(config)

            result = worker_node.worker_loop(config, dry_run_result="ready", stop_on_idle=True)
            status = queue_store.queue_status(config)

            self.assertEqual(result["stop_reason"], "idle")
            self.assertEqual(result["iterations"], 2)
            self.assertEqual(result["status_counts"]["dry_run_complete"], 1)
            self.assertEqual(result["status_counts"]["idle"], 1)
            self.assertEqual(status["states"]["ready_for_finalize"], 1)

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
