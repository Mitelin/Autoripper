from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import queue_store


class QueueStoreTests(unittest.TestCase):
    def make_config(self, temp_dir: str) -> dict:
        root = Path(temp_dir)
        return {
            "output_root": str(root / "output"),
            "libraries": {"anime": ["dummy"]},
            "shared_state_dir": str(root / ".ripper_state"),
        }

    def make_job(self) -> dict:
        return {
            "schema_version": 1,
            "job_id": "job_test123",
            "status": "queue",
            "source_path": "/library/file.mkv",
            "media_type": "anime",
            "bucket": "anime_high",
        }

    def test_init_state_creates_required_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)

            self.assertTrue((root / "queue").is_dir())
            self.assertTrue((root / "running").is_dir())
            self.assertTrue((root / "workers").is_dir())
            self.assertTrue((root / "control" / "global.json").exists())

    def test_enqueue_job_skips_duplicates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            job = self.make_job()

            created, first_path = queue_store.enqueue_job(config, job)
            duplicate_created, duplicate_path = queue_store.enqueue_job(config, job)

            self.assertTrue(created)
            self.assertFalse(duplicate_created)
            self.assertEqual(first_path, duplicate_path)

    def test_claim_next_job_moves_file_to_running(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.enqueue_job(config, self.make_job())

            claimed = queue_store.claim_next_job(config, node_id="worker-a")
            status = queue_store.queue_status(config)

            self.assertIsNotNone(claimed)
            self.assertEqual(claimed["job"]["claimed_by"], "worker-a")
            self.assertEqual(status["states"]["queue"], 0)
            self.assertEqual(status["states"]["running"], 1)

    def test_set_global_control_updates_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)

            queue_store.set_global_control(config, "paused", allow_new_claims=False, allow_finalizer=True, updated_by="test")
            status = queue_store.queue_status(config)

            self.assertEqual(status["global_control"]["queue_state"], "paused")
            self.assertFalse(status["global_control"]["allow_new_claims"])
            self.assertEqual(status["global_control"]["updated_by"], "test")

    def test_queue_status_includes_node_controls(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)

            queue_store.set_node_control(config, "worker-a", worker_command="stop_after_current", updated_by="test")
            status = queue_store.queue_status(config)

            self.assertEqual(status["node_control_count"], 1)
            self.assertEqual(status["node_controls"][0]["node_id"], "worker-a")
            self.assertEqual(status["node_controls"][0]["worker_command"], "stop_after_current")
            self.assertIn("production_command", status["node_controls"][0])
            self.assertEqual(status["node_controls"][0]["updated_by"], "test")

    def test_queue_status_includes_ready_output_size_accounting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            ready_dir = root / "ready_outputs" / "job_size"
            nested_dir = ready_dir / "nested"
            nested_dir.mkdir(parents=True, exist_ok=True)
            (ready_dir / "output.mkv").write_bytes(b"x" * 1024)
            (nested_dir / "manifest.json").write_bytes(b"y" * 512)

            status = queue_store.queue_status(config)

            self.assertEqual(status["ready_outputs_dir_count"], 1)
            self.assertEqual(status["ready_outputs_file_count"], 2)
            self.assertEqual(status["ready_outputs_total_size_bytes"], 1536)
            self.assertIn("ready_outputs_total_size_gb", status)

    def test_queue_status_includes_worker_and_manager_heartbeat_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "workers" / "worker-a.json",
                {
                    "node_id": "worker-a",
                    "hostname": "worker-host",
                    "roles": {"web_ui": True, "worker": True, "manager": False},
                    "worker_state": "running",
                    "current_job_id": "job_worker123",
                    "current_phase": "encoding_execute",
                    "last_heartbeat": "2026-05-10T01:59:30+02:00",
                },
            )
            queue_store.write_json_atomic(
                root / "manager" / "manager-a.json",
                {
                    "node_id": "manager-a",
                    "hostname": "manager-host",
                    "roles": {"web_ui": True, "worker": True, "manager": True},
                    "manager_state": "waiting",
                    "current_job_id": None,
                    "current_phase": "idle",
                    "last_heartbeat": "2026-05-10T01:58:45+02:00",
                },
            )

            with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"):
                status = queue_store.queue_status(config)

            self.assertEqual(status["workers"], 1)
            self.assertEqual(status["worker_heartbeat_count"], 1)
            self.assertEqual(status["worker_heartbeats"][0]["node_id"], "worker-a")
            self.assertEqual(status["worker_heartbeats"][0]["state"], "running")
            self.assertEqual(status["worker_heartbeats"][0]["current_phase"], "encoding_execute")
            self.assertEqual(status["worker_heartbeats"][0]["heartbeat_age_seconds"], 30)
            self.assertFalse(status["worker_heartbeats"][0]["heartbeat_stale"])
            self.assertEqual(status["worker_summary"]["total"], 1)
            self.assertEqual(status["worker_summary"]["healthy"], 1)
            self.assertEqual(status["worker_summary"]["stale"], 0)
            self.assertEqual(status["worker_summary"]["with_current_job"], 1)
            self.assertEqual(status["worker_summary"]["state_counts"], {"running": 1})
            self.assertEqual(status["worker_summary"]["phase_counts"], {"encoding_execute": 1})
            self.assertEqual(status["managers"], 1)
            self.assertEqual(status["manager_heartbeat_count"], 1)
            self.assertEqual(status["manager_heartbeats"][0]["node_id"], "manager-a")
            self.assertEqual(status["manager_heartbeats"][0]["state"], "waiting")
            self.assertEqual(status["manager_heartbeats"][0]["heartbeat_age_seconds"], 75)
            self.assertTrue(status["manager_heartbeats"][0]["heartbeat_stale"])
            self.assertEqual(status["manager_summary"]["total"], 1)
            self.assertEqual(status["manager_summary"]["healthy"], 0)
            self.assertEqual(status["manager_summary"]["stale"], 1)
            self.assertEqual(status["manager_summary"]["with_current_job"], 0)
            self.assertEqual(status["manager_summary"]["idle_without_job"], 1)
            self.assertEqual(status["manager_summary"]["state_counts"], {"waiting": 1})
            self.assertEqual(status["manager_summary"]["phase_counts"], {"idle": 1})

    def test_recover_stale_running_jobs_moves_stale_worker_job_to_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "running" / "job_test123.worker-a.json",
                {
                    "schema_version": 1,
                    "job_id": "job_test123",
                    "status": "running",
                    "source_path": "/library/file.mkv",
                    "claimed_by": "worker-a",
                    "claimed_at": "2026-05-10T01:58:00+02:00",
                },
            )
            queue_store.write_json_atomic(
                root / "workers" / "worker-a.json",
                {
                    "node_id": "worker-a",
                    "hostname": "worker-host",
                    "roles": {"web_ui": True, "worker": True, "manager": False},
                    "worker_state": "running",
                    "current_job_id": "job_test123",
                    "current_phase": "encoding_execute",
                    "last_heartbeat": "2026-05-10T01:58:30+02:00",
                },
            )

            with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"):
                result = queue_store.recover_stale_running_jobs(config)

            stale_job = queue_store.read_json(root / "stale" / "job_test123.worker-a.json")
            stale_log_path = Path(stale_job["stale_log"])
            stale_log = queue_store.read_json(stale_log_path)
            self.assertEqual(result["recovered_count"], 1)
            self.assertEqual(result["recovered"][0]["job_id"], "job_test123")
            self.assertFalse((root / "running" / "job_test123.worker-a.json").exists())
            self.assertEqual(stale_job["status"], "stale")
            self.assertEqual(stale_job["stale_reason"], "worker_heartbeat_expired")
            self.assertEqual(stale_job["stale_node_id"], "worker-a")
            self.assertTrue(stale_log_path.exists())
            self.assertEqual(stale_log["status"], "stale")
            self.assertEqual(stale_log["reason"], "worker_heartbeat_expired")

    def test_requeue_interrupted_jobs_moves_job_back_to_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "interrupted" / "job_test123.worker-a.json",
                {
                    "schema_version": 1,
                    "job_id": "job_test123",
                    "status": "interrupted",
                    "source_path": "/library/file.mkv",
                    "error": "hard_stop_requested",
                    "interrupted_at": "2026-05-10T01:58:00+02:00",
                },
            )

            with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"):
                result = queue_store.requeue_interrupted_jobs(config)

            queued_job = queue_store.read_json(root / "queue" / "job_test123.worker-a.json")
            requeue_log_path = Path(queued_job["requeue_log"])
            requeue_log = queue_store.read_json(requeue_log_path)
            self.assertEqual(result["moved_count"], 1)
            self.assertFalse((root / "interrupted" / "job_test123.worker-a.json").exists())
            self.assertEqual(queued_job["status"], "queue")
            self.assertEqual(queued_job["requeued_from_state"], "interrupted")
            self.assertEqual(queued_job["last_requeue_reason"], "manual_interrupted_requeue")
            self.assertTrue(requeue_log_path.exists())
            self.assertEqual(requeue_log["status"], "queue")
            self.assertEqual(requeue_log["previous_state"], "interrupted")


if __name__ == "__main__":
    unittest.main()
