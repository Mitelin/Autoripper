from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import shared_locks


class SharedLocksTests(unittest.TestCase):
    def make_config(self, temp_dir: str) -> dict:
        root = Path(temp_dir)
        return {
            "output_root": str(root / "output"),
            "libraries": {"anime": ["dummy"]},
            "shared_state_dir": str(root / ".ripper_state"),
            "heartbeat": {"stale_after_seconds": 60},
            "io_limits": {
                "lock_stale_after_seconds": 30,
                "max_concurrent_nas_reads": 1,
                "max_concurrent_nas_writes": 2,
                "max_concurrent_active_encodes": 1,
                "max_concurrent_finalizers": 1,
            },
        }

    def test_acquire_lock_respects_slot_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)

            first = shared_locks.acquire_lock(config, "nas_read", "node-a", "job-1")
            second = shared_locks.acquire_lock(config, "nas_read", "node-b", "job-2")

            self.assertTrue(first["acquired"])
            self.assertFalse(second["acquired"])
            self.assertEqual(second["reason"], "no_slot_available")

    def test_release_lock_frees_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            first = shared_locks.acquire_lock(config, "nas_read", "node-a", "job-1")

            released = shared_locks.release_lock(Path(first["path"]))
            second = shared_locks.acquire_lock(config, "nas_read", "node-b", "job-2")

            self.assertTrue(released)
            self.assertTrue(second["acquired"])

    def test_lock_status_reports_active_locks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            shared_locks.acquire_lock(config, "nas_write", "node-a", "job-1")

            status = shared_locks.lock_status(config)

            self.assertEqual(status["nas_write"]["limit"], 2)
            self.assertEqual(status["nas_write"]["active"], 1)
            self.assertEqual(status["nas_write"]["locks"][0]["job_id"], "job-1")
            self.assertEqual(status["nas_write"]["stale"], 0)

    def test_lock_status_marks_stale_locks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T01:58:00+02:00"):
                shared_locks.acquire_lock(config, "nas_write", "node-a", "job-1")

            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T02:00:00+02:00"):
                status = shared_locks.lock_status(config)

            self.assertEqual(status["nas_write"]["stale"], 1)
            self.assertTrue(status["nas_write"]["locks"][0]["heartbeat_stale"])
            self.assertEqual(status["nas_write"]["locks"][0]["heartbeat_age_seconds"], 120)

    def test_recover_stale_locks_releases_expired_slots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T01:58:00+02:00"):
                first = shared_locks.acquire_lock(config, "nas_read", "node-a", "job-1")

            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T02:00:00+02:00"):
                result = shared_locks.recover_stale_locks(config, lock_types=["nas_read"])

            second = shared_locks.acquire_lock(config, "nas_read", "node-b", "job-2")
            self.assertTrue(first["acquired"])
            self.assertEqual(result["recovered_count"], 1)
            self.assertEqual(result["recovered"][0]["reason"], "lock_heartbeat_expired")
            self.assertTrue(second["acquired"])

    def test_finalizer_lock_is_single_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)

            first = shared_locks.acquire_lock(config, "finalizer", "manager-a", "job-1")
            second = shared_locks.acquire_lock(config, "finalizer", "manager-b", "job-2")
            status = shared_locks.lock_status(config)

            self.assertTrue(first["acquired"])
            self.assertFalse(second["acquired"])
            self.assertEqual(status["finalizer"]["limit"], 1)
            self.assertEqual(status["finalizer"]["active"], 1)


if __name__ == "__main__":
    unittest.main()
