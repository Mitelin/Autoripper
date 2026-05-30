from __future__ import annotations

import os
import io
import socket
import shutil
import subprocess
import tempfile
import threading
import errno
import unittest
from pathlib import Path
from unittest.mock import patch

import simpleripper
import yaml


class SimpleRipperTests(unittest.TestCase):
    def make_config(self, root: Path) -> dict:
        return {
            "app": {"runtime_dir": str(root / "runtime")},
            "paths": {"local_work_dir": str(root / "work"), "history_dir": str(root / "history"), "log_dir": str(root / "logs"), "quarantine_dir": str(root / "quarantine"), "inspection_dir": str(root / "inspection"), "keep_failed_output_for_inspection": True},
            "tools": {"ffmpeg": "ffmpeg", "ffprobe": "ffprobe"},
            "libraries": {"roots": [str(root / "library")]},
            "scan": {"file_extensions": [".mkv"], "processed_marker_suffix": ".simpleripper.done.json", "lock_suffix": ".simpleripper.lock", "write_sidecar_markers": False, "failed_retry_cooldown_hours": 24, "max_failures_per_file": 1},
            "retention_size_policy": {"enabled": True, "series": {"max_mb_per_25min": 500}, "anime": {"max_mb_per_25min": 500}, "movie": {"max_mb_per_25min": 500}, "unknown": {"max_mb_per_25min": 500}},
            "verification": {"max_duration_diff_seconds": 2, "max_output_source_ratio": 0.95, "low_ratio_warning": 0.15},
            "track_policy": {"enabled": True, "target_audio_languages": ["cze"], "drop_other_audio_if_target_found": True, "keep_subtitles": True},
        }

    def test_local_instance_lock_refuses_running_local_process(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir) / "runtime"
            runtime.mkdir()
            simpleripper.write_json(runtime / "simpleripper.pid", {"hostname": socket.gethostname(), "pid": os.getpid(), "started_at": simpleripper.utc_now()})

            with self.assertRaises(simpleripper.InstanceLockError):
                simpleripper.LocalInstanceLock(runtime).acquire()

    def test_source_lock_contains_required_fields_and_blocks_second_acquire(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir()
            source.write_text("x", encoding="utf-8")

            lock = simpleripper.write_source_lock(source, config)
            second = simpleripper.write_source_lock(source, config)

            self.assertIsNotNone(lock)
            self.assertIsNone(second)
            payload = simpleripper.read_json(lock or Path())
            self.assertEqual(payload["hostname"], socket.gethostname())
            self.assertEqual(payload["pid"], os.getpid())
            self.assertEqual(payload["source_path"], str(source))
            self.assertEqual(lock, Path(config["app"]["runtime_dir"]) / "file_locks" / f"{simpleripper.file_lock_id(source)}.json")

    def test_scan_skips_processed_and_locked_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["write_sidecar_markers"] = True
            library = root / "library"
            library.mkdir()
            ready = library / "ready.mkv"
            processed = library / "processed.mkv"
            locked = library / "locked.mkv"
            for path in (ready, processed, locked):
                path.write_text("x", encoding="utf-8")
            simpleripper.write_json(simpleripper.marker_path(processed, config), {"done": True})
            simpleripper.write_source_lock(locked, config)

            candidates = simpleripper.scan_candidates([library], config)

            self.assertEqual(candidates, [ready])

    def test_marker_path_is_disabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"

            self.assertIsNone(simpleripper.marker_path(source, config))

    def test_scan_skips_history_done_file_with_same_signature(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()
            processed = library / "processed.mkv"
            processed.write_text("same", encoding="utf-8")

            simpleripper.write_history_index(
                config,
                processed,
                {
                    "status": "done",
                    "job_id": "job-1",
                    "source_signature": simpleripper.source_signature(processed),
                    "updated_at": simpleripper.utc_now(),
                },
            )

            self.assertEqual(simpleripper.scan_candidates([library], config), [])

    def test_cached_scan_skips_source_after_successful_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            library = root / "library"
            library.mkdir()
            source = library / "processed.mkv"
            source.write_bytes(b"x" * 10)

            simpleripper.fast_inventory_scan([library], config)
            self.assertEqual(simpleripper.scan_candidates([library], config), [source])

            simpleripper.update_cache_job_success(config, source)

            self.assertEqual(simpleripper.scan_candidates([library], config), [])

    def test_cached_scan_skips_replacement_path_after_successful_job(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            library = root / "library"
            library.mkdir()
            source = library / "source.mkv"
            replacement = library / "source.simpleripper.mkv"
            source.write_bytes(b"x" * 10)
            replacement.write_bytes(b"y" * 12)

            simpleripper.fast_inventory_scan([library], config)
            self.assertEqual(simpleripper.scan_candidates([library], config), [replacement, source])

            simpleripper.update_cache_job_success(config, source, replacement)

            self.assertEqual(simpleripper.scan_candidates([library], config), [])

    def test_scan_skips_file_from_shared_nas_history_written_by_other_machine(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_a = self.make_config(root)
            config_b = self.make_config(root)
            config_b["paths"]["history_dir"] = str(root / "history-second-machine")
            library = root / "library"
            library.mkdir()
            processed = library / "processed.mkv"
            processed.write_text("same", encoding="utf-8")
            payload = {
                "status": "done",
                "job_id": "job-1",
                "source_signature": simpleripper.source_signature(processed),
                "updated_at": simpleripper.utc_now(),
            }

            simpleripper.write_shared_worker_history(config_a, processed, payload)

            self.assertEqual(simpleripper.shared_history_root(config_a), root / "RIPTEST" / "state")
            self.assertTrue(simpleripper.shared_worker_history_path(config_a).exists())
            self.assertFalse(simpleripper.history_index_path(config_b, processed).exists())
            self.assertEqual(simpleripper.scan_candidates([library], config_b), [])
            self.assertTrue(simpleripper.history_index_path(config_b, processed).exists())

    def test_scan_candidates_returns_largest_files_first(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()
            small = library / "small.mkv"
            medium = library / "medium.mkv"
            large = library / "large.mkv"
            small.write_bytes(b"x" * 10)
            medium.write_bytes(b"x" * 20)
            large.write_bytes(b"x" * 30)

            candidates = simpleripper.scan_candidates([library], config)

            self.assertEqual(candidates, [large, medium, small])

    def test_scan_skips_recent_ffmpeg_failure(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()
            failed = library / "failed.mkv"
            failed.write_bytes(b"x" * 10)
            simpleripper.write_history_index(
                config,
                failed,
                {
                    "status": "error",
                    "failure_type": "ffmpeg",
                    "failure_count": 1,
                    "source_signature": simpleripper.source_signature(failed),
                    "updated_at": simpleripper.utc_now(),
                    "error": "ffmpeg failed with exit code 1",
                },
            )

            with patch("simpleripper.log_event") as log_mock:
                candidates = simpleripper.scan_candidates([library], config)

            self.assertEqual(candidates, [])
            log_mock.assert_any_call(config, "candidate_scan_skipped", source_path=str(failed), reason="recent_ffmpeg_failure", failure_count=1, retry_after=unittest.mock.ANY, error="ffmpeg failed with exit code 1")

    def test_fast_inventory_scan_does_not_call_ffprobe(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            source = root / "library" / "movie.mkv"
            source.parent.mkdir()
            source.write_bytes(b"x" * 10)

            with patch("simpleripper.run_ffprobe", side_effect=AssertionError("ffprobe must not run")):
                result = simpleripper.fast_inventory_scan([source.parent], config)

            self.assertEqual(result["indexed_files"], 1)
            self.assertEqual(simpleripper.worker_cache_summary(config)["indexed_files"], 1)

    def test_cached_skip_is_not_selected_until_file_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            source = root / "library" / "movie.mkv"
            source.parent.mkdir()
            source.write_bytes(b"x" * 10)
            simpleripper.fast_inventory_scan([source.parent], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(source)))

            with patch("simpleripper.run_ffprobe", side_effect=AssertionError("ffprobe must not run")):
                self.assertEqual(simpleripper.scan_candidates([source.parent], config), [])

            source.write_bytes(b"changed" * 10)
            simpleripper.fast_inventory_scan([source.parent], config)

            self.assertEqual(simpleripper.scan_candidates([source.parent], config), [source])

    def test_policy_hash_change_invalidates_cached_skip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            source = root / "library" / "episode.mkv"
            source.parent.mkdir()
            source.write_bytes(b"x" * 10)
            simpleripper.fast_inventory_scan([source.parent], config)
            old_hash = simpleripper.policy_hash(config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (old_hash, str(source)))

            config["retention_size_policy"]["series"]["max_mb_per_25min"] = 650

            self.assertEqual(simpleripper.scan_candidates([source.parent], config), [source])

    def test_selected_folder_change_invalidates_empty_queue_scope(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            folder_a = root / "library" / "A"
            folder_b = root / "library" / "B"
            folder_a.mkdir(parents=True)
            folder_b.mkdir(parents=True)
            source_b = folder_b / "candidate.mkv"
            source_b.write_bytes(b"x" * 10)
            config["scan"]["selected_folders"] = [{"path": str(folder_a), "media_type": "auto"}]

            first = simpleripper.scan_candidates([folder_a], config)
            self.assertEqual(first, [])

            config["scan"]["selected_folders"] = [{"path": str(folder_b), "media_type": "auto"}]
            second = simpleripper.scan_candidates([folder_b], config)

            self.assertEqual(second, [source_b])
            self.assertEqual(simpleripper.scan_state_get(config, "candidate_queue_scope_fingerprint"), simpleripper.scan_scope_fingerprint(config, [folder_b]))

    def test_media_type_change_invalidates_scoped_skip_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            folder = root / "library" / "Scoped"
            folder.mkdir(parents=True)
            source = folder / "candidate.mkv"
            source.write_bytes(b"x" * 10)
            config["scan"]["selected_folders"] = [{"path": str(folder), "media_type": "auto"}]
            simpleripper.fast_inventory_scan([folder], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(source)))

            config["scan"]["selected_folders"] = [{"path": str(folder), "media_type": "movie"}]

            self.assertEqual(simpleripper.scan_candidates([folder], config), [source])

    def test_repeated_ffmpeg_failures_block_cached_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            source = root / "library" / "bad.mkv"
            source.parent.mkdir()
            source.write_bytes(b"x" * 10)
            simpleripper.fast_inventory_scan([source.parent], config)

            for _ in range(3):
                result = simpleripper.update_cache_job_failure(config, source, "ffmpeg failed")

            self.assertEqual((result or {})["decision"], "blocked")
            self.assertEqual(simpleripper.scan_candidates([source.parent], config), [])

    def test_clean_folder_is_skipped_by_fast_inventory(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            season = root / "library" / "Series" / "Show" / "Season 02"
            season.mkdir(parents=True)
            episode = season / "episode.mkv"
            episode.write_bytes(b"x" * 10)
            simpleripper.fast_inventory_scan([root / "library"], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(episode)))
            simpleripper.fast_inventory_scan([root / "library"], config)

            with patch("simpleripper.direct_video_files", side_effect=AssertionError("clean season should not be opened")):
                result = simpleripper.fast_inventory_scan([season], config)

            self.assertEqual(result["skipped_folders"], 1)
            self.assertEqual(simpleripper.worker_cache_summary(config)["folder_states"]["clean"], 4)

    def test_parent_not_clean_when_child_folder_unknown(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30, "folder_clean_requires_full_inventory": True}
            serialy = root / "library" / "SERIALY"
            folder_a = serialy / "A"
            folder_b = serialy / "B"
            folder_a.mkdir(parents=True)
            folder_b.mkdir(parents=True)
            file_a = folder_a / "a.mkv"
            file_b = folder_b / "b.mkv"
            file_a.write_bytes(b"a" * 10)
            file_b.write_bytes(b"b" * 10)
            simpleripper.fast_inventory_scan([folder_a], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(file_a)))
            simpleripper.fast_inventory_scan([folder_a], config)

            with simpleripper.open_worker_cache(config) as connection:
                simpleripper.refresh_folder_state(connection, serialy, config, "test-generation")
                row = connection.execute("SELECT state, unknown_files, child_folders_unknown, scan_complete FROM folder_index WHERE path = ?", (str(serialy),)).fetchone()

            self.assertEqual(row["state"], "partial")
            self.assertGreaterEqual(row["child_folders_unknown"], 1)
            self.assertEqual(row["scan_complete"], 0)

            simpleripper.fast_inventory_scan([folder_b], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(file_b)))
            simpleripper.fast_inventory_scan([serialy], config)

            with simpleripper.open_worker_cache(config) as connection:
                row = connection.execute("SELECT state, child_folders_clean, child_folders_total, scan_complete FROM folder_index WHERE path = ?", (str(serialy),)).fetchone()

            self.assertEqual(row["state"], "clean")
            self.assertEqual(row["child_folders_clean"], 2)
            self.assertEqual(row["child_folders_total"], 2)
            self.assertEqual(row["scan_complete"], 1)

    def test_interrupted_inventory_does_not_promote_ancestors_clean(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30, "folder_clean_requires_full_inventory": True}
            serialy = root / "library" / "SERIALY"
            folder_a = serialy / "A"
            folder_b = serialy / "B"
            folder_a.mkdir(parents=True)
            folder_b.mkdir(parents=True)
            file_a = folder_a / "a.mkv"
            file_b = folder_b / "b.mkv"
            file_a.write_bytes(b"a" * 10)
            file_b.write_bytes(b"b" * 10)
            simpleripper.fast_inventory_scan([serialy], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ?", (simpleripper.policy_hash(config),))

            original_direct_video_files = simpleripper.direct_video_files

            def fail_on_b(folder: Path, test_config: dict) -> list[Path]:
                if folder == folder_b:
                    raise RuntimeError("scan interrupted")
                return original_direct_video_files(folder, test_config)

            with patch("simpleripper.direct_video_files", side_effect=fail_on_b):
                with self.assertRaises(RuntimeError):
                    simpleripper.fast_inventory_scan([serialy], config)

            with simpleripper.open_worker_cache(config) as connection:
                row = connection.execute("SELECT state, scan_complete FROM folder_index WHERE path = ?", (str(serialy),)).fetchone()

            self.assertNotEqual(row["state"], "clean")
            self.assertEqual(row["scan_complete"], 0)

    def test_child_entry_oserror_keeps_parent_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30, "folder_clean_requires_full_inventory": True}
            parent = root / "library" / "SERIALY"
            broken_child = parent / "Broken"
            good_child = parent / "Good"
            parent.mkdir(parents=True)
            broken_child.mkdir()
            good_child.mkdir()
            (good_child / "episode.mkv").write_bytes(b"x" * 10)

            original_is_dir = Path.is_dir

            def broken_is_dir(path: Path) -> bool:
                if path == broken_child:
                    raise OSError("permission denied")
                return original_is_dir(path)

            with patch("pathlib.Path.is_dir", autospec=True, side_effect=broken_is_dir):
                simpleripper.fast_inventory_scan([parent], config)

            with simpleripper.open_worker_cache(config) as connection:
                row = connection.execute("SELECT state, scan_complete, child_folders_unknown FROM folder_index WHERE path = ?", (str(parent),)).fetchone()

            self.assertEqual(row["state"], "partial")
            self.assertEqual(row["scan_complete"], 0)
            self.assertGreaterEqual(row["child_folders_unknown"], 1)

    def test_new_file_invalidates_clean_folder_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            season = root / "library" / "Series" / "Show" / "Season 02"
            season.mkdir(parents=True)
            first = season / "episode1.mkv"
            first.write_bytes(b"x" * 10)
            simpleripper.fast_inventory_scan([root / "library"], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(first)))
            simpleripper.refresh_folder_state_upwards(config, first)

            second = season / "episode2.mkv"
            second.write_bytes(b"y" * 20)
            result = simpleripper.fast_inventory_scan([root / "library"], config)

            self.assertGreaterEqual(result["changed_files"], 1)
            self.assertIn(second, simpleripper.scan_candidates([root / "library"], config))
            with simpleripper.open_worker_cache(config) as connection:
                row = connection.execute("SELECT state FROM folder_index WHERE path = ?", (str(season),)).fetchone()
            self.assertEqual(row["state"], "partial")

    def test_policy_hash_change_marks_clean_folder_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            season = root / "library" / "Series" / "Show" / "Season 02"
            season.mkdir(parents=True)
            episode = season / "episode.mkv"
            episode.write_bytes(b"x" * 10)
            simpleripper.fast_inventory_scan([root / "library"], config)
            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET decision = 'skip', decision_reason = 'already_hevc', policy_hash = ? WHERE path = ?", (simpleripper.policy_hash(config), str(episode)))
            simpleripper.refresh_folder_state_upwards(config, episode)

            config["retention_size_policy"]["series"]["max_mb_per_25min"] = 650
            result = simpleripper.fast_inventory_scan([root / "library"], config)

            self.assertEqual(result["skipped_folders"], 0)
            self.assertIn(episode, simpleripper.scan_candidates([root / "library"], config))

    def test_cache_cli_summary_and_clear_cache(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            config_path = root / "config.yaml"
            config_path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

            with patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(simpleripper.main(["cache-summary", "--config", str(config_path)]), 0)
            self.assertTrue(simpleripper.worker_cache_path(config).exists())
            with patch("sys.stdout", new_callable=io.StringIO):
                self.assertEqual(simpleripper.main(["clear-cache", "--config", str(config_path)]), 0)
            self.assertTrue(simpleripper.worker_cache_path(config).exists())

    def test_main_accepts_core_override_for_web_command(self) -> None:
        config = self.make_config(Path("."))

        with patch("simpleripper.load_config", return_value=config), patch("simpleripper.run_server") as mocked_run_server:
            self.assertEqual(simpleripper.main(["web", "--config", "config.yaml", "--core", "6"]), 0)

        forwarded_config = mocked_run_server.call_args.args[0]
        self.assertEqual(forwarded_config["__ffmpeg_thread_limit"], 6)

    def test_main_accepts_cpu_percent_override_for_web_command(self) -> None:
        config = self.make_config(Path("."))

        with patch("simpleripper.load_config", return_value=config), patch("simpleripper.os.cpu_count", return_value=12), patch("simpleripper.run_server") as mocked_run_server:
            self.assertEqual(simpleripper.main(["web", "--config", "config.yaml", "--cpu50%"]), 0)

        forwarded_config = mocked_run_server.call_args.args[0]
        self.assertEqual(forwarded_config["__ffmpeg_thread_limit"], 6)

    def test_main_rejects_multiple_cpu_percent_overrides(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            simpleripper.main(["web", "--config", "config.yaml", "--cpu50%", "--cpu60%"])

        self.assertEqual(raised.exception.code, 2)

    def test_worker_cache_reuses_initialized_schema_under_concurrency(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            simpleripper.worker_cache_summary(config)
            original_initialize = simpleripper.initialize_worker_cache
            init_calls = 0
            errors: list[Exception] = []

            def tracked_initialize(connection: object) -> None:
                nonlocal init_calls
                init_calls += 1
                original_initialize(connection)  # type: ignore[arg-type]

            def read_summary() -> None:
                try:
                    for _ in range(20):
                        simpleripper.worker_cache_summary(config)
                except Exception as exc:
                    errors.append(exc)

            def write_state() -> None:
                try:
                    for index in range(20):
                        simpleripper.scan_state_set(config, "heartbeat", str(index))
                except Exception as exc:
                    errors.append(exc)

            with patch("simpleripper.initialize_worker_cache", side_effect=tracked_initialize):
                threads = [threading.Thread(target=read_summary) for _ in range(4)] + [threading.Thread(target=write_state)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

            self.assertEqual(init_calls, 0)
            self.assertEqual(errors, [])

    def test_set_phase_writes_current_job_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir()
            source.write_text("x", encoding="utf-8")

            app.set_phase("encoding", source, {"job_id": "job-1"})

            payload = simpleripper.read_json(simpleripper.current_job_path(config))
            self.assertEqual(payload["phase"], "encoding")
            self.assertEqual(payload["source_path"], str(source))
            self.assertEqual(payload["job_id"], "job-1")
            self.assertIn("started_at", payload)

    def test_configured_folder_suggestions_use_windows_roots(self) -> None:
        config = {
            "libraries": {
                "roots": [r"\\192.168.50.23\admin\FILMY", r"\\192.168.50.23\admin\SERIALY"],
            },
        }

        with patch("simpleripper.os.name", "nt"):
            suggestions, mode = simpleripper.configured_folder_suggestions(config)

        self.assertEqual(mode, "windows-roots")
        self.assertEqual(suggestions, [r"\\192.168.50.23\admin\FILMY", r"\\192.168.50.23\admin\SERIALY"])

    def test_configured_folder_suggestions_use_linux_mount_libraries(self) -> None:
        config = {
            "libraries": {"roots": ["/fallback/root"]},
            "linux-nas": {
                "libraries": {
                    "movie": ["/mnt/nas/filmy/FILMY"],
                    "series": ["/mnt/nas/filmy/SERIALY"],
                },
            },
        }

        with patch("simpleripper.os.name", "posix"):
            suggestions, mode = simpleripper.configured_folder_suggestions(config)

        self.assertEqual(mode, "linux-mounts")
        self.assertEqual(suggestions, ["/mnt/nas/filmy/FILMY", "/mnt/nas/filmy/SERIALY"])

    def test_browse_folders_returns_allowed_roots_without_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()

            payload = simpleripper.safe_browse_folders(config)

            self.assertEqual(payload["current_path"], None)
            self.assertEqual(payload["parent_path"], None)
            self.assertEqual(payload["allowed_roots"], [str(library.resolve())])
            self.assertEqual(payload["directories"], [{"name": "library", "path": str(library.resolve())}])

    def test_browse_folders_lists_direct_child_directories_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            czech = library / "Czech"
            english = library / "English"
            czech.mkdir(parents=True)
            english.mkdir(parents=True)
            (library / "movie.mkv").write_text("x", encoding="utf-8")

            payload = simpleripper.safe_browse_folders(config, str(library))

            self.assertEqual(payload["current_path"], str(library.resolve()))
            self.assertEqual(payload["parent_path"], None)
            self.assertEqual(payload["directories"], [
                {"name": "Czech", "path": str(czech.resolve())},
                {"name": "English", "path": str(english.resolve())},
            ])

    def test_browse_folders_rejects_path_outside_allowed_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            (root / "library").mkdir()
            outside = root / "outside"
            outside.mkdir()

            with self.assertRaises(PermissionError):
                simpleripper.safe_browse_folders(config, str(outside))

    def test_browse_folders_skips_symlink_that_resolves_outside_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            outside = root / "outside"
            library.mkdir()
            outside.mkdir()
            link = library / "escape"
            try:
                link.symlink_to(outside, target_is_directory=True)
            except OSError:
                self.skipTest("symlink creation is not available")

            payload = simpleripper.safe_browse_folders(config, str(library))

            self.assertEqual(payload["directories"], [])
            self.assertFalse(simpleripper.is_path_inside_roots(outside.resolve(), [library.resolve()]))

    def test_custom_folder_post_without_path_does_not_open_desktop_picker(self) -> None:
        class FakeHandler:
            def __init__(self, app: simpleripper.SimpleRipperApp) -> None:
                self.app = app
                self.path = "/api/custom-folder"
                self.responses: list[tuple[object, int]] = []

            def read_payload(self) -> dict:
                return {}

            def send_json(self, payload: object, status: int = 200) -> None:
                self.responses.append((payload, status))

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            handler = FakeHandler(app)

            simpleripper.SimpleRipperHandler.do_POST(handler)  # type: ignore[arg-type]

            self.assertEqual(handler.responses[0][1], simpleripper.HTTPStatus.BAD_REQUEST)
            self.assertEqual(handler.responses[0][0], {"error": "Missing folder path. Use /api/browse-folders from the web UI."})

    def test_browse_folders_endpoint_rejects_outside_path_with_403(self) -> None:
        class FakeHandler:
            def __init__(self, app: simpleripper.SimpleRipperApp, path: str) -> None:
                self.app = app
                self.path = path
                self.responses: list[tuple[object, int]] = []

            def send_json(self, payload: object, status: int = 200) -> None:
                self.responses.append((payload, status))

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            (root / "library").mkdir()
            outside = root / "outside"
            outside.mkdir()
            app = simpleripper.SimpleRipperApp(config)
            handler = FakeHandler(app, f"/api/browse-folders?path={simpleripper.parse.quote(str(outside))}")

            simpleripper.SimpleRipperHandler.do_GET(handler)  # type: ignore[arg-type]

            self.assertEqual(handler.responses[0][1], simpleripper.HTTPStatus.FORBIDDEN)
            self.assertIn("outside allowed library roots", handler.responses[0][0]["error"])  # type: ignore[index]

    def test_web_root_head_returns_headers_without_body(self) -> None:
        class FakeHandler:
            def __init__(self, app: simpleripper.SimpleRipperApp) -> None:
                self.app = app
                self.path = "/"
                self.status: int | None = None
                self.headers: dict[str, str] = {}
                self.wfile = io.BytesIO()

            def send_response(self, status: int) -> None:
                self.status = status

            def send_header(self, name: str, value: str) -> None:
                self.headers[name] = value

            def end_headers(self) -> None:
                pass

            def send_no_cache_headers(self) -> None:
                simpleripper.SimpleRipperHandler.send_no_cache_headers(self)  # type: ignore[arg-type]

            def send_index(self, include_body: bool = True) -> None:
                simpleripper.SimpleRipperHandler.send_index(self, include_body=include_body)  # type: ignore[arg-type]

            def send_json(self, payload: object, status: int = 200, include_body: bool = True) -> None:
                simpleripper.SimpleRipperHandler.send_json(self, payload, status=status, include_body=include_body)  # type: ignore[arg-type]

            def handle_get_request(self, include_body: bool = True) -> None:
                simpleripper.SimpleRipperHandler.handle_get_request(self, include_body=include_body)  # type: ignore[arg-type]

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            handler = FakeHandler(app)

            simpleripper.SimpleRipperHandler.do_HEAD(handler)  # type: ignore[arg-type]

            self.assertEqual(handler.status, 200)
            self.assertEqual(handler.headers["Content-Type"], "text/html; charset=utf-8")
            self.assertEqual(handler.headers["Content-Length"], str(len(simpleripper.INDEX_HTML.encode("utf-8"))))
            self.assertEqual(handler.wfile.getvalue(), b"")

    def test_web_ui_folder_picker_uses_browser_endpoint(self) -> None:
        self.assertIn("/api/browse-folders", simpleripper.INDEX_HTML)
        self.assertIn("function pickFolder(initialDir=''){browseFolder(initialDir||'')}", simpleripper.INDEX_HTML)
        self.assertIn("function selectBrowsedFolder(path){post('/api/custom-folder',{path:path,media_type:guessMediaType(path)});closeFolderBrowser()}", simpleripper.INDEX_HTML)
        self.assertNotIn("function pickFolder(initialDir=''){post('/api/custom-folder'", simpleripper.INDEX_HTML)
        self.assertFalse(hasattr(simpleripper, "pick_folder_dialog"))

    def test_web_ui_preserves_open_error_disclosures_between_refreshes(self) -> None:
        self.assertIn("function captureDisclosureState()", simpleripper.INDEX_HTML)
        self.assertIn("function restoreDisclosureState(openKeys)", simpleripper.INDEX_HTML)
        self.assertIn("data-ui-key=\"${escapeHtml(disclosureKey(item))}\"", simpleripper.INDEX_HTML)
        self.assertIn("const openDisclosures=captureDisclosureState();", simpleripper.INDEX_HTML)
        self.assertIn("restoreDisclosureState(openDisclosures)", simpleripper.INDEX_HTML)

    def test_web_ui_renders_approve_and_skip_actions_for_actionable_errors(self) -> None:
        self.assertIn("function approveError(id){post('/api/errors/action',{id:id,action:'approve'})}", simpleripper.INDEX_HTML)
        self.assertIn("function skipError(id){post('/api/errors/action',{id:id,action:'skip'})}", simpleripper.INDEX_HTML)
        self.assertIn("Approve</button>", simpleripper.INDEX_HTML)
        self.assertIn("Skip</button>", simpleripper.INDEX_HTML)

    def test_web_ui_marks_queued_error_as_warning(self) -> None:
        self.assertIn(".err.warn,.err-card.warn{background:var(--warn-soft)", simpleripper.INDEX_HTML)
        self.assertIn("const isQueued=queuedAction==='approve'||queuedAction==='skip'", simpleripper.INDEX_HTML)
        self.assertIn("const toneClass=isQueued?'warn':''", simpleripper.INDEX_HTML)
        self.assertIn("<div class=\"err-queued\">Queued ${escapeHtml(queuedAction)}</div>", simpleripper.INDEX_HTML)

    def test_web_ui_hides_actions_for_queued_error(self) -> None:
        self.assertIn("Array.isArray(item.actions)&&item.actions.length&&item.id", simpleripper.INDEX_HTML)

    def test_copy_file_interruptible_removes_partial_output_on_force_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            source = root / "source.bin"
            destination = root / "dest.bin"
            source.write_bytes(b"x" * 1024)

            with self.assertRaises(simpleripper.ForceStopRequested):
                simpleripper.copy_file_interruptible(source, destination, lambda: True, chunk_size=128)

            self.assertFalse(destination.exists())

    def test_process_one_force_stop_resets_runtime_without_error_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 1024)
            app.state.force_stop = True

            with patch("simpleripper.copy_file_interruptible", side_effect=simpleripper.ForceStopRequested("force stop requested")):
                app.process_one(source)

            status = app.status()
            self.assertEqual(status["current_phase"], "idle")
            self.assertTrue(status["force_stop"])
            self.assertEqual(status["errors"], [])
            self.assertFalse(simpleripper.current_job_path(config).exists())
            self.assertFalse((Path(config["paths"]["local_work_dir"]) / "current").exists())

    def test_process_one_force_stop_during_encoding_cleans_current_job_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 4096)

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    self._terminated = False
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int | None:
                    app.state.force_stop = True
                    return None if not self._terminated else 0

                def terminate(self) -> None:
                    self._terminated = True

                def wait(self, timeout: float | None = None) -> int:
                    self._terminated = True
                    return 0

                def kill(self) -> None:
                    self._terminated = True

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "h264", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}

            with patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe):
                app.process_one(source)

            status = app.status()
            self.assertEqual(status["current_phase"], "idle")
            self.assertTrue(status["force_stop"])
            self.assertEqual(status["errors"], [])
            self.assertFalse(simpleripper.current_job_path(config).exists())
            self.assertFalse((Path(config["paths"]["local_work_dir"]) / "current").exists())
            self.assertFalse(Path(config["paths"]["inspection_dir"]).exists())
            self.assertFalse(simpleripper.temp_upload_path(simpleripper.target_output_path(source)).exists())

    def test_run_loop_stops_after_force_stop_from_process_one(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 1024)
            app.state.running = True

            def fake_copy(_source: Path, _destination: Path, _should_stop: object, chunk_size: int = 8 * 1024 * 1024) -> None:
                app._running_requested = False
                app.state.force_stop = True
                raise simpleripper.ForceStopRequested("force stop requested")

            with patch("simpleripper.scan_candidates", side_effect=[[source], []]) as scan_candidates_mock, patch.object(app, "pick_next_candidate", return_value=source), patch("simpleripper.copy_file_interruptible", side_effect=fake_copy), patch.object(app, "schedule_rescan_wait", return_value=False) as wait_mock:
                app._run_loop()

            self.assertEqual(scan_candidates_mock.call_count, 1)
            wait_mock.assert_not_called()
            self.assertFalse(app.state.running)
            self.assertFalse(app.state.force_stop)

    def test_force_stop_persists_idle_intent_in_runtime_control(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)

            app.force_stop()

            runtime_control = simpleripper.load_runtime_control(config)
            self.assertFalse(runtime_control["running_requested"])
            self.assertEqual(runtime_control["stop_reason"], "force_stop")
            self.assertFalse(simpleripper.resume_request_path(config).exists())

    def test_recover_runtime_state_does_not_resume_after_force_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("source", encoding="utf-8")
            local_output = root / "work" / "current" / "output" / "movie.mkv"
            local_output.parent.mkdir(parents=True)
            local_output.write_text("partial", encoding="utf-8")
            simpleripper.set_running_requested(config, False, stop_reason="force_stop")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-force-stop",
                    "phase": "encoding",
                    "source_path": str(source),
                    "local_output_path": str(local_output),
                    "ffmpeg_pid": 999999,
                },
            )

            with patch("simpleripper.is_local_pid_running", return_value=False):
                simpleripper.recover_runtime_state(config)

            self.assertIsNone(simpleripper.load_resume_request(config))
            joined = "\n".join(simpleripper.tail_text_lines(simpleripper.app_log_path(config), 20))
            self.assertIn("crash_recovery_rerun_skipped", joined)
            self.assertIn("user_force_stop_requested", joined)

            with patch.object(simpleripper.SimpleRipperApp, "start", autospec=True) as start_mock:
                app = simpleripper.SimpleRipperApp(config)

            self.assertFalse(app.status()["running"])
            start_mock.assert_not_called()

    def test_run_loop_waits_one_hour_when_no_usable_job_is_found(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            candidate = root / "library" / "movie.mkv"
            candidate.parent.mkdir(parents=True, exist_ok=True)
            candidate.write_bytes(b"x" * 10)

            with patch("simpleripper.scan_candidates", return_value=[candidate]), patch.object(app, "pick_next_candidate", return_value=None), patch.object(app, "schedule_rescan_wait", return_value=False) as wait_mock:
                app._run_loop()

            wait_mock.assert_called_once_with(3600, "no_usable_candidates")

    def test_media_type_for_source_prefers_selected_folder_setting(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            movie_dir = root / "library" / "Movies"
            movie_dir.mkdir(parents=True)
            source = movie_dir / "clip.mkv"
            source.write_text("x", encoding="utf-8")

            app.set_selected_folders([{"path": str(movie_dir), "media_type": "movie"}])

            self.assertEqual(app.media_type_for_source(source), "movie")

    def test_set_selected_folders_accepts_path_and_media_type_objects(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            first = root / "library" / "Movies"
            second = root / "library" / "Series"
            first.mkdir(parents=True)
            second.mkdir(parents=True)

            app.set_selected_folders([
                {"path": str(first), "media_type": "movie"},
                {"path": str(second), "media_type": "series"},
            ])

            status = app.status()
            self.assertEqual(status["selected_folders"][0]["media_type"], "movie")
            self.assertEqual(status["selected_folders"][1]["media_type"], "series")

    def test_set_selected_folders_accepts_folder_outside_library_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            folder = root / "manual_pick"
            folder.mkdir(parents=True)

            app.set_selected_folders([{"path": str(folder), "media_type": "auto"}])

            status = app.status()
            self.assertEqual(status["selected_folders"][0]["path"], str(folder))

    def test_add_custom_folder_accepts_folder_outside_library_roots(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            folder = root / "manual_pick"
            folder.mkdir(parents=True)

            app.add_custom_folder(str(folder), "series")

            status = app.status()
            self.assertEqual(status["selected_folders"][0]["path"], str(folder))
            self.assertEqual(status["selected_folders"][0]["media_type"], "series")

    def test_status_exposes_current_and_last_result_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)

            app.state.running = True
            app.state.current_phase = "encoding"
            app.state.current_file = str(root / "library" / "Movie" / "movie.mkv")
            app.state.current_duration_seconds = 2400
            app.state.ffmpeg_progress = {"time": "00:10:00", "fps": 24, "speed": "1.2x"}
            app.state.output_size_bytes = 123456
            app.state.last_processed = [{
                "status": "done",
                "source_path": str(root / "library" / "Movie" / "movie.mkv"),
                "finished_at": simpleripper.utc_now(),
                "source_size_bytes": 1000,
                "output_size_bytes": 400,
                "video_codec_before": "h264",
                "video_codec_after": "hevc",
                "audio_stream_count_before": 2,
                "audio_stream_count_after": 1,
                "subtitle_stream_count_before": 1,
                "subtitle_stream_count_after": 1,
                "output_to_source_ratio": 0.4,
                "overall_bitrate_kbps": 1500,
                "verification_warning": "warn",
                "jellyfin_refresh": {"status": "ok"},
            }]

            status = app.status()

            self.assertEqual(status["current_summary"]["status"], "encoding")
            self.assertEqual(status["current_summary"]["progress_time"], "00:10:00")
            self.assertEqual(status["current_summary"]["progress_percent"], 25.0)
            self.assertFalse(status["can_update"])
            self.assertEqual(status["last_result"]["bytes_saved"], 600)
            self.assertEqual(status["last_result"]["warning"], "warn")
            self.assertEqual(status["last_result"]["jellyfin_status"], "ok")

    def test_status_restores_current_job_details_from_runtime_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "Movie" / "movie.mkv"

            app.state.running = True
            app.state.current_phase = "encoding"
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "phase": "encoding",
                    "source_path": str(source),
                    "output_size_bytes": 654321,
                    "source_metadata": {"duration_seconds": 2400},
                    "progress": {"out_time": "00:10:00.00", "fps": "24", "speed": "1.2x"},
                },
            )

            status = app.status()

            self.assertEqual(status["current_summary"]["source_path"], str(source))
            self.assertEqual(status["current_summary"]["progress_time"], "00:10:00.00")
            self.assertEqual(status["current_summary"]["progress_percent"], 25.0)
            self.assertEqual(status["current_summary"]["progress_speed"], "1.2x")
            self.assertEqual(status["current_summary"]["output_size_bytes"], 654321)

    def test_status_falls_back_to_ffmpeg_progress_log_when_current_job_has_no_progress(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "Movie" / "movie.mkv"

            app.state.running = True
            app.state.current_phase = "encoding"
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "phase": "encoding",
                    "source_path": str(source),
                    "source_metadata": {"duration_seconds": 3000},
                },
            )
            simpleripper.write_json(
                simpleripper.ffmpeg_current_log_path(config),
                {
                    "source_path": str(source),
                    "progress": {"out_time": "00:15:00.00", "fps": "30", "speed": "0.9x"},
                },
            )

            status = app.status()

            self.assertEqual(status["current_summary"]["progress_time"], "00:15:00.00")
            self.assertEqual(status["current_summary"]["progress_percent"], 30.0)
            self.assertEqual(status["current_summary"]["progress_fps"], "30")
            self.assertEqual(status["current_summary"]["progress_speed"], "0.9x")

    def test_status_allows_update_only_when_idle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)

            self.assertTrue(app.status()["can_update"])

            app.state.current_phase = "waiting_for_rescan"
            self.assertFalse(app.status()["can_update"])

    def test_begin_update_requires_idle_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            app.state.running = True

            with self.assertRaises(RuntimeError):
                app.begin_update()

    def test_perform_update_pulls_and_restarts_server(self) -> None:
        class FakeServer:
            def __init__(self) -> None:
                self.shutdown_called = False
                self.server_close_called = False

            def shutdown(self) -> None:
                self.shutdown_called = True

            def server_close(self) -> None:
                self.server_close_called = True

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "config.yaml"
            config_path.write_text(yaml.safe_dump(self.make_config(root), sort_keys=False), encoding="utf-8")
            config = simpleripper.load_config(config_path)
            app = simpleripper.SimpleRipperApp(config)
            server = FakeServer()
            pull_result = subprocess.CompletedProcess(args=["git", "pull"], returncode=0, stdout="Already up to date.\n", stderr="")

            with patch("simpleripper.subprocess.run", return_value=pull_result) as mocked_run, patch("simpleripper.subprocess.Popen") as mocked_popen:
                app.perform_update(server)

            self.assertTrue(server.shutdown_called)
            self.assertTrue(server.server_close_called)
            self.assertEqual(mocked_run.call_args.kwargs["cwd"], str(Path(simpleripper.__file__).resolve().parent))
            restart_command = mocked_popen.call_args.args[0]
            self.assertEqual(restart_command[0], simpleripper.sys.executable)
            self.assertEqual(restart_command[2:], ["web", "--config", str(config_path.resolve())])

    def test_status_exposes_test_mode_message_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)

            app.set_test_mode(True)
            status = app.status()

            self.assertTrue(status["test_mode"])
            self.assertIn("test modu", status["test_mode_message"])

    def test_pick_next_candidate_prefers_largest_file_before_higher_score(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["priority_probe_limit"] = 4
            app = simpleripper.SimpleRipperApp(config)
            library = root / "library"
            library.mkdir()
            large = library / "large.mkv"
            small = library / "small.mkv"
            large.write_bytes(b"x" * 30)
            small.write_bytes(b"x" * 10)

            def fake_inspect(_config: dict, source: Path, _media_type: str) -> dict:
                if source == large:
                    return {"path": source, "status": "ok", "metadata": {"file_size_bytes": 30}, "score": 10.0, "skip_reason": None}
                return {"path": source, "status": "ok", "metadata": {"file_size_bytes": 10}, "score": 500.0, "skip_reason": None}

            with patch("simpleripper.inspect_candidate", side_effect=fake_inspect):
                selected = app.pick_next_candidate([large, small])

            self.assertEqual(selected, large)

    def test_pick_next_candidate_stops_after_first_usable_candidate(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            first = root / "library" / "first.mkv"
            second = root / "library" / "second.mkv"
            first.parent.mkdir(parents=True, exist_ok=True)
            first.write_bytes(b"x" * 20)
            second.write_bytes(b"x" * 10)
            inspected: list[Path] = []

            def fake_inspect(_config: dict, source: Path, _media_type: str) -> dict:
                inspected.append(source)
                return {"path": source, "status": "ok", "metadata": {"file_size_bytes": source.stat().st_size, "video_codec": "h264"}, "score": 1.0, "skip_reason": None}

            with patch("simpleripper.inspect_candidate", side_effect=fake_inspect):
                selected = app.pick_next_candidate([first, second])

            self.assertEqual(selected, first)
            self.assertEqual(inspected, [first])

    def test_pick_next_candidate_lazily_skips_invalid_first_then_returns_second(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            first = root / "library" / "first.mkv"
            second = root / "library" / "second.mkv"
            third = root / "library" / "third.mkv"
            first.parent.mkdir(parents=True, exist_ok=True)
            first.write_bytes(b"x" * 30)
            second.write_bytes(b"x" * 20)
            third.write_bytes(b"x" * 10)
            inspected: list[Path] = []

            def fake_inspect(_config: dict, source: Path, _media_type: str) -> dict:
                inspected.append(source)
                if source == first:
                    return {"path": source, "status": "ok", "metadata": {"file_size_bytes": source.stat().st_size}, "score": 0.0, "skip_reason": "already ok"}
                return {"path": source, "status": "ok", "metadata": {"file_size_bytes": source.stat().st_size, "video_codec": "h264"}, "score": 5.0, "skip_reason": None}

            with patch("simpleripper.inspect_candidate", side_effect=fake_inspect):
                selected = app.pick_next_candidate([first, second, third])

            self.assertEqual(selected, second)
            self.assertEqual(inspected, [first, second])

    def test_lazy_deep_check_failure_uses_cooldown_then_blocks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "broken.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b"x" * 20)
            simpleripper.fast_inventory_scan([source.parent], config)

            with patch("simpleripper.inspect_candidate", return_value={"path": source, "status": "ffprobe_failed", "error": "probe crashed"}):
                selected = app.pick_next_candidate([source])

            self.assertIsNone(selected)
            with simpleripper.open_worker_cache(config) as connection:
                row = connection.execute("SELECT decision, failure_count, last_error, last_failure_at, retry_after FROM file_index WHERE path = ?", (str(source),)).fetchone()
            self.assertEqual(row["decision"], "failed")
            self.assertEqual(row["failure_count"], 1)
            self.assertEqual(row["last_error"], "probe crashed")
            self.assertTrue(row["last_failure_at"])
            self.assertTrue(row["retry_after"])
            self.assertEqual(simpleripper.cached_candidate_paths(config, [source.parent]), [])

            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET retry_after = ?, failure_count = 1 WHERE path = ?", ("2000-01-01T00:00:00+00:00", str(source)))

            self.assertEqual(simpleripper.cached_candidate_paths(config, [source.parent]), [source])

            with simpleripper.open_worker_cache(config) as connection:
                connection.execute("UPDATE file_index SET retry_after = ?, failure_count = 2 WHERE path = ?", ("2000-01-01T00:00:00+00:00", str(source)))

            with patch("simpleripper.inspect_candidate", return_value={"path": source, "status": "ffprobe_failed", "error": "probe crashed again"}):
                selected = app.pick_next_candidate([source])

            self.assertIsNone(selected)
            with simpleripper.open_worker_cache(config) as connection:
                blocked_row = connection.execute("SELECT decision, failure_count, last_error, retry_after FROM file_index WHERE path = ?", (str(source),)).fetchone()
            self.assertEqual(blocked_row["decision"], "blocked")
            self.assertEqual(blocked_row["failure_count"], 3)
            self.assertEqual(blocked_row["last_error"], "probe crashed again")
            self.assertIsNone(blocked_row["retry_after"])
            self.assertEqual(simpleripper.cached_candidate_paths(config, [source.parent]), [])

    def test_cached_candidate_paths_fills_queue_after_scope_filter(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan_cache"] = {"enabled": True, "queue_size": 2, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            in_scope = root / "library" / "SERIALY" / "Show"
            out_scope = root / "library" / "FILMY"
            in_scope.mkdir(parents=True, exist_ok=True)
            out_scope.mkdir(parents=True, exist_ok=True)
            wanted_a = in_scope / "episode-a.mkv"
            wanted_b = in_scope / "episode-b.mkv"
            outside_a = out_scope / "movie-a.mkv"
            outside_b = out_scope / "movie-b.mkv"
            wanted_a.write_bytes(b"a" * 100)
            wanted_b.write_bytes(b"b" * 90)
            outside_a.write_bytes(b"c" * 1000)
            outside_b.write_bytes(b"d" * 900)
            config["scan"]["selected_folders"] = [{"path": str(in_scope), "media_type": "series"}]
            simpleripper.fast_inventory_scan([wanted_a.parent, outside_a.parent], config)

            candidates = simpleripper.cached_candidate_paths(config, [wanted_a.parent])

            self.assertEqual(candidates, [wanted_a, wanted_b])

    def test_pick_next_candidate_returns_none_when_no_usable_job_exists(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["priority_probe_limit"] = 2
            app = simpleripper.SimpleRipperApp(config)
            first = root / "library" / "first.mkv"
            second = root / "library" / "second.mkv"
            first.parent.mkdir(parents=True, exist_ok=True)
            first.write_bytes(b"x" * 20)
            second.write_bytes(b"x" * 10)

            def fake_inspect(_config: dict, source: Path, _media_type: str) -> dict:
                return {"path": source, "status": "ok", "metadata": {"file_size_bytes": source.stat().st_size}, "score": 0.0, "skip_reason": "already ok"}

            with patch("simpleripper.inspect_candidate", side_effect=fake_inspect):
                selected = app.pick_next_candidate([first, second])

            self.assertIsNone(selected)

    def test_set_selected_folders_persists_into_config_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config_path = root / "config.yaml"
            config_path.write_text(yaml.safe_dump(self.make_config(root), sort_keys=False), encoding="utf-8")
            config = simpleripper.load_config(config_path)
            app = simpleripper.SimpleRipperApp(config)
            folder = root / "library" / "Movies"
            folder.mkdir(parents=True)

            app.set_selected_folders([{"path": str(folder), "media_type": "movie"}])

            saved = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["scan"]["selected_folders"][0]["path"], str(folder))
            self.assertEqual(saved["scan"]["selected_folders"][0]["media_type"], "movie")

    def test_replace_uses_relative_quarantine_and_deferred_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            nested = library / "Movies" / "A"
            nested.mkdir(parents=True)
            source = nested / "movie.mkv"
            output = nested / ".movie.mkv.simpleripper.tmp"
            source.write_text("original", encoding="utf-8")
            output.write_text("encoded", encoding="utf-8")

            result = simpleripper.replace_source_with_output(source, output, config, {"job_id": "job-1"})

            self.assertEqual(source.read_text(encoding="utf-8"), "encoded")
            self.assertFalse(output.exists())
            self.assertIsNone(simpleripper.marker_path(source, config))
            quarantine = Path(result["quarantine_path"])
            self.assertTrue(quarantine.exists())
            self.assertIn(str(root / "quarantine" / "Movies" / "A"), str(quarantine))
            self.assertIsNone(result["processed_marker_path"])

    def test_target_output_path_switches_avi_to_mkv(self) -> None:
        source = Path("The Chronicles of Narnia S01E02.avi")

        self.assertEqual(simpleripper.target_output_suffix(source), ".mkv")
        self.assertEqual(simpleripper.target_output_path(source), Path("The Chronicles of Narnia S01E02.mkv"))
        self.assertEqual(simpleripper.temp_upload_path(simpleripper.target_output_path(source)).name, ".The Chronicles of Narnia S01E02.mkv.simpleripper.tmp")

    def test_replace_avi_source_moves_verified_mkv_and_quarantines_original(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()
            source = library / "episode.avi"
            replacement = library / "episode.mkv"
            output = library / ".episode.mkv.simpleripper.tmp"
            source.write_text("original", encoding="utf-8")
            output.write_text("encoded", encoding="utf-8")

            result = simpleripper.replace_source_with_output(source, output, config, {"job_id": "job-avi"}, replacement)

            self.assertFalse(source.exists())
            self.assertTrue(replacement.exists())
            self.assertEqual(replacement.read_text(encoding="utf-8"), "encoded")
            self.assertFalse(output.exists())
            quarantine = Path(result["quarantine_path"])
            self.assertTrue(quarantine.exists())
            self.assertEqual(quarantine.read_text(encoding="utf-8"), "original")
            self.assertEqual(result["replacement_path"], str(replacement))
            self.assertEqual(result["final_path"], str(replacement))

    def test_replace_can_return_sidecar_marker_path_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["write_sidecar_markers"] = True
            library = root / "library"
            library.mkdir()
            source = library / "movie.mkv"
            output = library / ".movie.mkv.simpleripper.tmp"
            source.write_text("original", encoding="utf-8")
            output.write_text("encoded", encoding="utf-8")

            result = simpleripper.replace_source_with_output(source, output, config, {"job_id": "job-1"})

            self.assertEqual(result["processed_marker_path"], str(source.with_name(source.name + ".simpleripper.done.json")))

    def test_quarantine_path_can_use_configured_relative_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            quarantine_root = root / ".simpleripper_quarantine"
            relative_root = root / "nas-backup"
            source = relative_root / "SERIALY" / "Czech" / "Fallout" / "file.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("original", encoding="utf-8")
            config = self.make_config(root)
            config["paths"]["quarantine_dir"] = str(quarantine_root)
            config["paths"]["quarantine_relative_root"] = str(relative_root)
            config["libraries"]["roots"] = [str(relative_root / "SERIALY")]

            quarantine = simpleripper.quarantine_path_for_source(source, config)

            self.assertTrue(str(quarantine).startswith(str(quarantine_root / "SERIALY" / "Czech" / "Fallout" / "file.mkv")))

    def test_rollback_replacement_restores_quarantined_original(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()
            source = library / "movie.mkv"
            output = library / ".movie.mkv.simpleripper.tmp"
            source.write_text("original", encoding="utf-8")
            output.write_text("encoded", encoding="utf-8")
            result = simpleripper.replace_source_with_output(source, output, config, {"job_id": "job-1"})

            simpleripper.rollback_replacement(source, Path(result["quarantine_path"]), config)

            self.assertEqual(source.read_text(encoding="utf-8"), "original")
            failed_outputs = list((root / "inspection" / "failed_replacements").glob("*.failed-output"))
            self.assertEqual(len(failed_outputs), 1)
            self.assertEqual(failed_outputs[0].read_text(encoding="utf-8"), "encoded")

    def test_rollback_replacement_restores_avi_source_from_mkv_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            library = root / "library"
            library.mkdir()
            source = library / "movie.avi"
            replacement = library / "movie.mkv"
            output = library / ".movie.mkv.simpleripper.tmp"
            source.write_text("original", encoding="utf-8")
            output.write_text("encoded", encoding="utf-8")
            result = simpleripper.replace_source_with_output(source, output, config, {"job_id": "job-1"}, replacement)

            simpleripper.rollback_replacement(source, Path(result["quarantine_path"]), config, replacement)

            self.assertTrue(source.exists())
            self.assertEqual(source.read_text(encoding="utf-8"), "original")
            self.assertFalse(replacement.exists())
            failed_outputs = list((root / "inspection" / "failed_replacements").glob("*.failed-output"))
            self.assertEqual(len(failed_outputs), 1)
            self.assertEqual(failed_outputs[0].read_text(encoding="utf-8"), "encoded")

    def test_finalize_quarantined_original_removes_file_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            quarantine_file = root / "quarantine" / "Movies" / "movie.mkv.original"
            quarantine_file.parent.mkdir(parents=True)
            quarantine_file.write_text("original", encoding="utf-8")

            result = simpleripper.finalize_quarantined_original(quarantine_file, config)

            self.assertFalse(quarantine_file.exists())
            self.assertTrue(result["quarantine_deleted"])
            self.assertFalse(result["quarantine_retained"])

    def test_finalize_quarantined_original_can_keep_file_for_development(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["paths"]["keep_quarantine_after_success"] = True
            quarantine_file = root / "quarantine" / "Movies" / "movie.mkv.original"
            quarantine_file.parent.mkdir(parents=True)
            quarantine_file.write_text("original", encoding="utf-8")

            result = simpleripper.finalize_quarantined_original(quarantine_file, config)

            self.assertTrue(quarantine_file.exists())
            self.assertFalse(result["quarantine_deleted"])
            self.assertTrue(result["quarantine_retained"])

    def test_finalize_quarantined_original_keeps_file_in_test_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["__test_mode"] = True
            quarantine_file = root / "quarantine" / "Movies" / "movie.mkv.original"
            quarantine_file.parent.mkdir(parents=True)
            quarantine_file.write_text("original", encoding="utf-8")

            result = simpleripper.finalize_quarantined_original(quarantine_file, config)

            self.assertTrue(quarantine_file.exists())
            self.assertFalse(result["quarantine_deleted"])
            self.assertTrue(result["quarantine_retained"])
            self.assertEqual(result["quarantine_retention_reason"], "test_mode")

    def test_track_policy_selects_target_audio(self) -> None:
        source = {
            "media_type": "series",
            "audio_stream_count": 2,
            "subtitle_stream_count": 1,
            "audio_streams": [
                {"index": 1, "language": "eng", "title": "English"},
                {"index": 2, "language": "cze", "title": "Czech"},
            ],
        }

        result = simpleripper.select_streams({"track_policy": {"enabled": True, "target_audio_languages": ["cze"], "drop_other_audio_if_target_found": True, "keep_subtitles": True}}, source)

        self.assertTrue(result["applied"])
        self.assertEqual(result["expected_audio_stream_count"], 1)
        self.assertIn("0:2", result["map_arguments"])
        self.assertNotIn("0:1", result["map_arguments"])

    def test_track_policy_uses_legacy_media_type_config_for_anime(self) -> None:
        source = {
            "media_type": "anime",
            "audio_stream_count": 2,
            "subtitle_stream_count": 1,
            "audio_streams": [
                {"index": 1, "language": "eng", "title": "English"},
                {"index": 2, "language": "cze", "title": "Czech"},
            ],
            "subtitle_streams": [{"index": 3, "language": "eng", "title": "English"}],
        }

        result = simpleripper.select_streams(
            {
                "track_policy": {
                    "enabled": True,
                    "global": {"copy_selected_subtitles": True},
                    "anime": {"target_audio_languages": ["eng"], "drop_other_audio_if_target_found": True},
                    "series": {"target_audio_languages": ["cze"], "drop_other_audio_if_target_found": True},
                    "movie": {"target_audio_languages": ["cze"], "drop_other_audio_if_target_found": True},
                    "unknown": {"cleanup_enabled": False},
                }
            },
            source,
        )

        self.assertTrue(result["applied"])
        self.assertEqual(result["expected_audio_stream_count"], 1)
        self.assertIn("0:1", result["map_arguments"])
        self.assertNotIn("0:2", result["map_arguments"])
        self.assertIn("0:s?", result["map_arguments"])

    def test_track_policy_uses_legacy_unknown_cleanup_disabled(self) -> None:
        source = {
            "media_type": "unknown",
            "audio_stream_count": 2,
            "subtitle_stream_count": 0,
            "audio_streams": [
                {"index": 1, "language": "eng", "title": "English"},
                {"index": 2, "language": "cze", "title": "Czech"},
            ],
        }

        result = simpleripper.select_streams(
            {
                "track_policy": {
                    "enabled": True,
                    "anime": {"target_audio_languages": ["eng"], "drop_other_audio_if_target_found": True},
                    "unknown": {"cleanup_enabled": False},
                }
            },
            source,
        )

        self.assertFalse(result["applied"])
        self.assertEqual(result["expected_audio_stream_count"], 2)

    def test_track_policy_keeps_all_streams_when_target_audio_is_not_detected(self) -> None:
        source = {
            "media_type": "anime",
            "audio_stream_count": 2,
            "subtitle_stream_count": 1,
            "audio_streams": [
                {"index": 1, "language": "jpn", "title": "Japanese"},
                {"index": 2, "language": "und", "title": "Unknown"},
            ],
            "subtitle_streams": [{"index": 3, "language": "eng", "title": "English"}],
        }

        result = simpleripper.select_streams(
            {
                "track_policy": {
                    "enabled": True,
                    "anime": {"target_audio_languages": ["eng"], "drop_other_audio_if_target_found": True},
                }
            },
            source,
        )

        self.assertFalse(result["applied"])
        self.assertEqual(result["map_arguments"], ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"])
        self.assertEqual(result["expected_audio_stream_count"], 2)
        self.assertEqual(result["expected_subtitle_stream_count"], 1)

    def test_build_ffmpeg_command_enables_progress_pipe(self) -> None:
        command = simpleripper.build_ffmpeg_command(
            self.make_config(Path(".")),
            Path("input.mkv"),
            Path("output.mkv"),
            {"media_type": "default"},
            {"map_arguments": ["-map", "0"]},
        )

        self.assertIn("-progress", command)
        self.assertIn("pipe:1", command)
        self.assertIn("-nostats", command)

    def test_build_ffmpeg_command_adds_thread_limit_when_configured(self) -> None:
        config = self.make_config(Path("."))
        config["__ffmpeg_thread_limit"] = 6

        command = simpleripper.build_ffmpeg_command(
            config,
            Path("input.mkv"),
            Path("output.mkv"),
            {"media_type": "default"},
            {"map_arguments": ["-map", "0"]},
        )

        self.assertIn("-threads", command)
        self.assertEqual(command[command.index("-threads") + 1], "6")

    def test_downscale_settings_applies_for_4k_series(self) -> None:
        config = self.make_config(Path("."))
        config["downscale"] = {
            "enabled": True,
            "media_types": ["series", "movie"],
            "only_buckets": ["4k"],
            "max_width": 1920,
            "flags": "lanczos",
            "crf_override": 21,
        }

        result = simpleripper.downscale_settings(
            config,
            {"media_type": "series", "video_width": 3840, "video_height": 1608},
        )

        self.assertTrue(result["applied"])
        self.assertEqual(result["bucket"], "4k")
        self.assertEqual(result["filter"], "scale='min(1920,iw)':-2:flags=lanczos")
        self.assertEqual(result["crf_override"], 21)

    def test_build_ffmpeg_command_adds_scale_and_crf_override_for_4k_downscale(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {
            "series": {"encoder": "libx265", "crf": 24, "preset": "medium", "pix_fmt": "yuv420p10le", "audio": "copy", "subtitles": "copy"}
        }
        config["downscale"] = {
            "enabled": True,
            "media_types": ["series", "movie"],
            "only_buckets": ["4k"],
            "max_width": 1920,
            "flags": "lanczos",
            "crf_override": 21,
        }
        metadata = {"media_type": "series", "video_width": 3840, "video_height": 1608}

        command = simpleripper.build_ffmpeg_command(
            config,
            Path("input.mkv"),
            Path("output.mkv"),
            metadata,
            {"map_arguments": ["-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?"]},
        )

        self.assertIn("-vf", command)
        self.assertEqual(command[command.index("-vf") + 1], "scale='min(1920,iw)':-2:flags=lanczos")
        self.assertEqual(command[command.index("-crf") + 1], "21")

    def test_build_ffmpeg_command_skips_scale_for_1080p_source(self) -> None:
        config = self.make_config(Path("."))
        config["downscale"] = {
            "enabled": True,
            "media_types": ["series", "movie"],
            "only_buckets": ["4k"],
            "max_width": 1920,
            "flags": "lanczos",
            "crf_override": 21,
        }

        command = simpleripper.build_ffmpeg_command(
            config,
            Path("input.mkv"),
            Path("output.mkv"),
            {"media_type": "series", "video_width": 1920, "video_height": 1080},
            {"map_arguments": ["-map", "0"]},
        )

        self.assertNotIn("-vf", command)

    def test_consume_ffmpeg_progress_emits_updates_from_chunked_binary_stream(self) -> None:
        class FakeBinaryBuffer:
            def __init__(self, chunks: list[bytes]) -> None:
                self.chunks = list(chunks)

            def read1(self, _size: int) -> bytes:
                return self.chunks.pop(0) if self.chunks else b""

        class FakeStream:
            def __init__(self, chunks: list[bytes]) -> None:
                self.buffer = FakeBinaryBuffer(chunks)
                self.closed = False

            def close(self) -> None:
                self.closed = True

        updates: list[dict[str, object]] = []
        stream = FakeStream([
            b"frame=10\nout_tim",
            b"e=00:00:05.00\nspeed=1.1x\nprogress=continue\nframe=20\nout_time=00:00:10.00\n",
            b"speed=1.2x\nprogress=end\n",
        ])

        simpleripper.consume_ffmpeg_progress(stream, lambda snapshot: updates.append(snapshot))

        self.assertEqual(len(updates), 2)
        self.assertEqual(updates[0]["out_time"], "00:00:05.00")
        self.assertEqual(updates[0]["progress"], "continue")
        self.assertEqual(updates[1]["out_time"], "00:00:10.00")
        self.assertEqual(updates[1]["speed"], "1.2x")
        self.assertEqual(updates[1]["progress"], "end")
        self.assertTrue(stream.closed)

    def test_consume_ffmpeg_progress_accepts_carriage_return_delimiters(self) -> None:
        class FakeBinaryBuffer:
            def __init__(self, chunks: list[bytes]) -> None:
                self.chunks = list(chunks)

            def read1(self, _size: int) -> bytes:
                return self.chunks.pop(0) if self.chunks else b""

        class FakeStream:
            def __init__(self, chunks: list[bytes]) -> None:
                self.buffer = FakeBinaryBuffer(chunks)

            def close(self) -> None:
                pass

        updates: list[dict[str, object]] = []
        stream = FakeStream([b"frame=30\rout_time=00:00:15.00\rspeed=0.9x\rprogress=continue\r"])

        simpleripper.consume_ffmpeg_progress(stream, lambda snapshot: updates.append(snapshot))

        self.assertEqual(len(updates), 1)
        self.assertEqual(updates[0]["out_time"], "00:00:15.00")
        self.assertEqual(updates[0]["speed"], "0.9x")

    def test_build_ffmpeg_command_maps_attachments_without_transcoding_cover_images(self) -> None:
        command = simpleripper.build_ffmpeg_command(
            self.make_config(Path(".")),
            Path("input.mkv"),
            Path("output.mkv"),
            {"media_type": "default"},
            {"map_arguments": ["-map", "0"]},
        )

        map_pairs = [(command[index], command[index + 1]) for index, value in enumerate(command[:-1]) if value == "-map"]
        self.assertNotIn(("-map", "0"), map_pairs)
        self.assertIn(("-map", "0:v:0"), map_pairs)
        self.assertIn(("-map", "0:a?"), map_pairs)
        self.assertIn(("-map", "0:s?"), map_pairs)
        self.assertIn(("-map", "0:t?"), map_pairs)
        self.assertIn("-c:t", command)
        self.assertEqual(command[command.index("-c:t") + 1], "copy")

    def test_log_error_deduplicates_by_source_and_failure_type(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            app = simpleripper.SimpleRipperApp(self.make_config(Path(temp_dir)))

            app.log_error("first", source_path="movie.mkv", failure_type="ffmpeg")
            app.log_error("second", source_path="movie.mkv", failure_type="ffmpeg")

            self.assertEqual(len(app.state.errors or []), 1)
            self.assertEqual((app.state.errors or [])[0]["message"], "second")

    def test_verification_failed_error_exposes_summary_and_details(self) -> None:
        verification = {
            "source_size_bytes": 3 * 1024 * 1024 * 1024,
            "output_size_bytes": 300 * 1024 * 1024,
            "output_to_source_ratio": 0.097,
            "overall_bitrate_kbps": 552,
            "suspicious_size_threshold_used": {"hard_fail_kbps": 900, "warning_kbps": 1200},
            "suspicious_size_warning_reason": "low output/source ratio 0.097; bitrate 552 kbps below warning threshold 1200 kbps",
        }

        error = simpleripper.VerificationFailedError("Local verification failed", ["Verification failed: not_suspiciously_tiny"], verification)

        self.assertIn("Local verification failed", error.summary)
        self.assertTrue(any("552 kbps < hard fail 900 kbps" in line for line in error.details))

    def test_expected_video_codecs_maps_common_encoders(self) -> None:
        self.assertEqual(simpleripper.expected_video_codecs("libx265"), {"hevc", "h265"})
        self.assertEqual(simpleripper.expected_video_codecs("libx264"), {"h264", "avc"})

    def test_terminate_process_gracefully_kills_after_timeout(self) -> None:
        class FakeProcess:
            def __init__(self) -> None:
                self.killed = False
                self.terminated = False
                self.wait_calls = 0

            def poll(self) -> None:
                return None

            def terminate(self) -> None:
                self.terminated = True

            def wait(self, timeout: float | None = None) -> int:
                self.wait_calls += 1
                if self.wait_calls == 1:
                    raise subprocess.TimeoutExpired(cmd="ffmpeg", timeout=timeout or 0)
                return 0

            def kill(self) -> None:
                self.killed = True

        process = FakeProcess()

        simpleripper.terminate_process_gracefully(process)

        self.assertTrue(process.terminated)
        self.assertTrue(process.killed)

    def test_jellyfin_item_score_prefers_exact_path(self) -> None:
        source = Path("D:/Media/Movies/Test/Movie.mkv")
        exact = {"Id": "1", "Path": "D:/Media/Movies/Test/Movie.mkv", "Name": "Movie"}
        filename_only = {"Id": "2", "Path": "D:/Elsewhere/Movie.mkv", "Name": "Movie"}
        candidates = ["D:/Media/Movies/Test/Movie.mkv"]

        self.assertGreater(
            simpleripper.jellyfin_item_score(exact, source, candidates),
            simpleripper.jellyfin_item_score(filename_only, source, candidates),
        )

    def test_jellyfin_mapped_paths_maps_unc_source_with_broad_prefix(self) -> None:
        settings = {
            "path_mapping": [
                {"fs_prefix": r"\\192.168.50.23\admin", "jellyfin_prefix": "/mnt/nas/filmy"},
            ],
        }
        source = Path(r"\\192.168.50.23\admin\SERIALY\English\Futurama\file.mkv")

        candidates = simpleripper.jellyfin_mapped_paths(settings, source)

        self.assertEqual(candidates[0], str(source))
        self.assertIn("/mnt/nas/filmy/SERIALY/English/Futurama/file.mkv", candidates)

    def test_jellyfin_mapped_paths_prefers_specific_unc_prefix(self) -> None:
        settings = {
            "path_mapping": [
                {"fs_prefix": r"\\192.168.50.23\admin", "jellyfin_prefix": "/mnt/nas/filmy"},
                {"fs_prefix": r"\\192.168.50.23\admin\SERIALY\Czech", "jellyfin_prefix": "/mnt/jellyfin-cz/serialy"},
            ],
        }
        source = Path(r"\\192.168.50.23\admin\SERIALY\Czech\Fallout\file.mkv")

        candidates = simpleripper.jellyfin_mapped_paths(settings, source)

        self.assertEqual(candidates[0], str(source))
        self.assertIn("/mnt/jellyfin-cz/serialy/Fallout/file.mkv", candidates)
        self.assertIn("/mnt/nas/filmy/SERIALY/Czech/Fallout/file.mkv", candidates)
        self.assertLess(candidates.index("/mnt/jellyfin-cz/serialy/Fallout/file.mkv"), candidates.index("/mnt/nas/filmy/SERIALY/Czech/Fallout/file.mkv"))

    def test_refresh_jellyfin_refreshes_all_exact_path_matches(self) -> None:
        config = self.make_config(Path("."))
        config["jellyfin"] = {
            "enabled": True,
            "server_url": "http://jellyfin.local:8096",
            "api_key": "secret",
            "path_mapping": [
                {"filesystem_prefix": "Z:/nas-backup/SERIALY/Czech", "jellyfin_prefix": "J:/serialy"},
                {"filesystem_prefix": "Z:/nas-backup", "jellyfin_prefix": "K:/filmy"},
            ],
        }
        source = Path("Z:/nas-backup/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv")
        items = [
            {"Id": "1", "Path": "J:/serialy/Fallout/Season 02/S02E02 Zlate pravidlo.mkv", "Name": "Episode 2"},
            {"Id": "2", "Path": "K:/filmy/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv", "Name": "Episode 2"},
        ]
        requests_sent: list[str] = []

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return b"{}"

        def fake_urlopen(req: object, timeout: int = 10) -> FakeResponse:
            requests_sent.append(req.full_url)  # type: ignore[attr-defined]
            return FakeResponse()

        with patch("simpleripper.jellyfin_query_items", return_value=items), patch("simpleripper.request.urlopen", side_effect=fake_urlopen):
            result = simpleripper.refresh_jellyfin(config, source)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["refreshed_count"], 2)
        self.assertEqual(result["match_type"], "exact_path")
        self.assertEqual({match["item_id"] for match in result["matches"]}, {"1", "2"})
        self.assertEqual(len(requests_sent), 2)

    def test_refresh_jellyfin_refreshes_single_exact_path_match(self) -> None:
        config = self.make_config(Path("."))
        config["jellyfin"] = {
            "enabled": True,
            "server_url": "http://jellyfin.local:8096",
            "api_key": "secret",
            "path_mapping": [{"filesystem_prefix": "Z:/nas-backup", "jellyfin_prefix": "K:/filmy"}],
        }
        source = Path("Z:/nas-backup/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv")
        items = [{"Id": "1", "Path": "K:/filmy/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv", "Name": "Episode 2"}]

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return b"{}"

        with patch("simpleripper.jellyfin_query_items", return_value=items), patch("simpleripper.request.urlopen", return_value=FakeResponse()):
            result = simpleripper.refresh_jellyfin(config, source)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["refreshed_count"], 1)
        self.assertEqual(result["item_id"], "1")
        self.assertEqual(result["match_type"], "exact_path")

    def test_refresh_jellyfin_finds_localized_episode_by_path_lookup(self) -> None:
        config = self.make_config(Path("."))
        config["jellyfin"] = {
            "enabled": True,
            "server_url": "http://jellyfin.local:8096",
            "api_key": "secret",
            "path_mapping": [{"fs_prefix": r"\\192.168.50.23\admin", "jellyfin_prefix": "/mnt/nas/filmy"}],
        }
        source = Path(r"\\192.168.50.23\admin\SERIALY\English\Futurama\Season 10\Futurama - S10E05 - Scared Screenless WEBDL-1080p.mkv")
        target_path = "/mnt/nas/filmy/SERIALY/English/Futurama/Season 10/Futurama - S10E05 - Scared Screenless WEBDL-1080p.mkv"
        search_items = [{"Id": "s09e09", "Path": "/mnt/nas/filmy/SERIALY/English/Futurama/Season 09/Futurama - S09E09.mkv", "Name": "Unrelated"}]
        path_items = [
            {"Id": "92EB0B39-2C74-FE6E-1F2D-ED2BF27BCB4F", "Path": target_path, "Name": "Posvatny cas bezobrazovek"},
            {"Id": "extra", "Path": "/mnt/nas/filmy/SERIALY/English/Futurama/Season 10 Extras/Futurama - S10E05 - Scared Screenless WEBDL-1080p.mkv", "Name": "Extra"},
        ]
        refresh_urls: list[str] = []

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return b"{}"

        def fake_urlopen(req: object, timeout: int = 10) -> FakeResponse:
            refresh_urls.append(req.full_url)  # type: ignore[attr-defined]
            return FakeResponse()

        with patch("simpleripper.jellyfin_query_items", return_value=search_items), patch("simpleripper.jellyfin_query_path_items", return_value=path_items), patch("simpleripper.request.urlopen", side_effect=fake_urlopen):
            result = simpleripper.refresh_jellyfin(config, source)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["match_type"], "exact_path")
        self.assertEqual(result["refreshed_count"], 1)
        self.assertEqual(result["matches"][0]["item_id"], "92EB0B39-2C74-FE6E-1F2D-ED2BF27BCB4F")
        self.assertEqual(result["matches"][0]["path"], target_path)
        self.assertEqual(result["matches"][0]["name"], "Posvatny cas bezobrazovek")
        self.assertIn(target_path, result["candidate_paths"])
        self.assertEqual(len(refresh_urls), 1)

    def test_refresh_jellyfin_does_not_refresh_season_extras_without_exact_path(self) -> None:
        config = self.make_config(Path("."))
        config["jellyfin"] = {
            "enabled": True,
            "server_url": "http://jellyfin.local:8096",
            "api_key": "secret",
            "path_mapping": [{"fs_prefix": r"\\192.168.50.23\admin", "jellyfin_prefix": "/mnt/nas/filmy"}],
        }
        source = Path(r"\\192.168.50.23\admin\SERIALY\English\Futurama\Season 10\Futurama - S10E05 - Scared Screenless WEBDL-1080p.mkv")
        extras = [
            {"Id": "extra1", "Path": "/mnt/nas/filmy/SERIALY/English/Futurama/Season 10 Extras/Futurama - S10E05 - Scared Screenless WEBDL-1080p.mkv", "Name": "Scared Screenless Extra"},
            {"Id": "extra2", "Path": "/mnt/nas/filmy/SERIALY/English/Futurama/Season 10 Extras/Futurama - S10E05 Behind the Scenes.mkv", "Name": "Season 10"},
        ]

        with patch("simpleripper.jellyfin_query_items", return_value=extras), patch("simpleripper.jellyfin_query_path_items", return_value=extras), patch("simpleripper.request.urlopen", side_effect=AssertionError("refresh must not be called")):
            result = simpleripper.refresh_jellyfin(config, source)

        self.assertEqual(result["status"], "not_found")
        self.assertEqual(result["reason"], "no_exact_path_match")
        self.assertIn("candidate_paths", result)
        self.assertIn("search_terms", result)

    def test_refresh_jellyfin_extension_change_refreshes_parent_when_new_file_path_is_missing(self) -> None:
        config = self.make_config(Path("."))
        config["jellyfin"] = {
            "enabled": True,
            "server_url": "http://jellyfin.local:8096",
            "api_key": "secret",
            "path_mapping": [{"filesystem_prefix": "Z:/nas-backup", "jellyfin_prefix": "K:/filmy"}],
        }
        source = Path("Z:/nas-backup/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.avi")
        replacement = Path("Z:/nas-backup/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv")
        requests_sent: list[str] = []

        class FakeResponse:
            def __enter__(self) -> "FakeResponse":
                return self

            def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
                return None

            def read(self) -> bytes:
                return b"{}"

        def fake_urlopen(req: object, timeout: int = 10) -> FakeResponse:
            requests_sent.append(req.full_url)  # type: ignore[attr-defined]
            return FakeResponse()

        with patch("simpleripper.jellyfin_query_items", return_value=[]), patch("simpleripper.jellyfin_query_path_items", return_value=[{"Id": "season-2", "Path": "K:/filmy/SERIALY/Czech/Fallout/Season 02", "Name": "Season 02"}]), patch("simpleripper.request.urlopen", side_effect=fake_urlopen):
            result = simpleripper.refresh_jellyfin(config, source, replacement)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["refresh_target"], "parent")
        self.assertEqual(result["refreshed_count"], 1)
        self.assertEqual(result["source_path"], str(source))
        self.assertEqual(result["replacement_path"], str(replacement))
        self.assertEqual(len(requests_sent), 1)

    def test_jellyfin_lookup_item_reports_not_found_without_exact_match(self) -> None:
        source = Path("Z:/nas-backup/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv")
        candidates = ["K:/filmy/SERIALY/Czech/Fallout/Season 02/S02E02 Zlate pravidlo.mkv"]
        items = [
            {"Id": "1", "Path": "X:/other/library/Fallout/Season 02/S02E02 Zlate pravidlo.mkv", "Name": "S02E02 Zlate pravidlo"},
            {"Id": "2", "Path": "Y:/duplicate/library/Fallout/Season 02/S02E02 Zlate pravidlo.mkv", "Name": "S02E02 Zlate pravidlo"},
        ]

        with patch("simpleripper.jellyfin_query_items", return_value=items):
            result = simpleripper.jellyfin_lookup_item("http://jellyfin.local:8096", "secret", source, candidates)

        self.assertEqual(result["status"], "not_found")
        self.assertEqual(result["reason"], "no_exact_path_match")
        self.assertEqual(result["candidate_paths"], candidates)
        self.assertTrue(result["search_terms"])
        self.assertEqual(len(result["matches"]), 2)
        self.assertEqual({match["item_id"] for match in result["matches"]}, {"1", "2"})

    def test_candidate_priority_score_prefers_h264_over_hevc(self) -> None:
        h264 = {"file_size_bytes": 10 * 1024 * 1024 * 1024, "video_codec": "h264", "overall_bitrate_kbps": 9000, "audio_stream_count": 2, "subtitle_stream_count": 2}
        hevc = {"file_size_bytes": 10 * 1024 * 1024 * 1024, "video_codec": "hevc", "overall_bitrate_kbps": 9000, "audio_stream_count": 2, "subtitle_stream_count": 2}

        self.assertGreater(simpleripper.candidate_priority_score(h264), simpleripper.candidate_priority_score(hevc))

    def test_should_reprocess_hevc_only_when_large_and_still_bitrate_heavy(self) -> None:
        config = self.make_config(Path("."))
        config["skip_rules"] = {"skip_hevc": True, "hevc_reprocess_min_size_mb": 12000, "hevc_reprocess_warning_multiplier": 1.75}
        large_hevc = {"media_type": "movie", "video_codec": "hevc", "file_size_bytes": 14 * 1024 * 1024 * 1024, "overall_bitrate_kbps": 4000, "video_width": 1920, "video_height": 1080}
        small_hevc = {"media_type": "movie", "video_codec": "hevc", "file_size_bytes": 2 * 1024 * 1024 * 1024, "overall_bitrate_kbps": 4000, "video_width": 1920, "video_height": 1080}

        self.assertTrue(simpleripper.should_reprocess_hevc(config, large_hevc))
        self.assertFalse(simpleripper.should_reprocess_hevc(config, small_hevc))

    def test_skip_reason_flags_already_simpleripper(self) -> None:
        self.assertEqual(simpleripper.skip_reason({}, {"encoded_by": "SimpleRipper"}), "already_simpleripper")

    def test_hevc_source_with_wrong_pix_fmt_and_extra_audio_is_skipped_under_retention_limit(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
        config["skip_rules"] = {"skip_hevc": True, "skip_4k": True, "skip_hdr": True}
        metadata = {
            "media_type": "series",
            "video_codec": "hevc",
            "video_pix_fmt": "yuv420p",
            "video_height": 1080,
            "is_hdr": False,
            "audio_stream_count": 2,
            "subtitle_stream_count": 3,
            "audio_streams": [
                {"index": 1, "codec": "eac3", "language": "cze", "title": "Czech"},
                {"index": 2, "codec": "eac3", "language": "eng", "title": "English"},
            ],
            "subtitle_streams": [
                {"index": 3, "codec": "subrip", "language": "eng", "title": "English"},
                {"index": 4, "codec": "subrip", "language": "cze", "title": "Czech"},
                {"index": 5, "codec": "subrip", "language": "cze", "title": "Forced"},
            ],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        matches, reasons = simpleripper.source_matches_target_profile(config, metadata, "series", track_policy)

        self.assertFalse(matches)
        self.assertEqual(simpleripper.skip_reason(config, metadata, track_policy), "under_retention_size_limit")
        self.assertIn("pix_fmt_mismatch:yuv420p!=yuv420p10le", reasons)
        self.assertIn("audio_policy_mismatch:extra_eng_audio", reasons)

    def test_hevc_source_matching_target_profile_is_skipped_as_normalized(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
        config["skip_rules"] = {"skip_hevc": True, "skip_4k": True, "skip_hdr": True}
        metadata = {
            "media_type": "series",
            "video_codec": "hevc",
            "video_pix_fmt": "yuv420p10le",
            "video_height": 1080,
            "is_hdr": False,
            "duration_seconds": 1500,
            "file_size_bytes": 400 * 1024 * 1024,
            "audio_stream_count": 1,
            "subtitle_stream_count": 2,
            "audio_streams": [{"index": 1, "codec": "eac3", "language": "cze", "title": "Czech"}],
            "subtitle_streams": [
                {"index": 2, "codec": "subrip", "language": "cze", "title": "Czech"},
                {"index": 3, "codec": "subrip", "language": "cze", "title": "Forced"},
            ],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        matches, reasons = simpleripper.source_matches_target_profile(config, metadata, "series", track_policy)

        self.assertTrue(matches)
        self.assertEqual(reasons, [])
        self.assertEqual(simpleripper.skip_reason(config, metadata, track_policy), "already_hevc")

    def test_hevc_source_matching_profile_but_oversized_is_not_skipped(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
        config["skip_rules"] = {"skip_hevc": True, "skip_4k": True, "skip_hdr": True}
        metadata = {
            "media_type": "series",
            "video_codec": "hevc",
            "video_pix_fmt": "yuv420p10le",
            "video_height": 1080,
            "is_hdr": False,
            "duration_seconds": 3036,
            "file_size_bytes": 1554 * 1024 * 1024,
            "audio_stream_count": 1,
            "subtitle_stream_count": 2,
            "audio_streams": [{"index": 1, "codec": "eac3", "language": "cze", "title": "Czech"}],
            "subtitle_streams": [
                {"index": 2, "codec": "subrip", "language": "cze", "title": "Czech"},
                {"index": 3, "codec": "subrip", "language": "cze", "title": "Forced"},
            ],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        matches, reasons = simpleripper.source_matches_target_profile(config, metadata, "series", track_policy)
        retention = simpleripper.retention_size_policy_evaluation(config, metadata, "series")

        self.assertFalse(matches)
        self.assertIsNone(simpleripper.skip_reason(config, metadata, track_policy))
        self.assertTrue(retention["oversized"])
        self.assertEqual(retention["actual_mb"], 1554.0)
        self.assertEqual(retention["limit_mb"], 1012.0)
        self.assertIn("retention_size_exceeded:1554.0MB>1012.0MB", reasons)

    def test_hdr_hevc_source_matching_profile_but_oversized_is_not_skipped(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
        config["skip_rules"] = {"skip_hevc": True, "skip_4k": True, "skip_hdr": True}
        metadata = {
            "media_type": "series",
            "video_codec": "hevc",
            "video_pix_fmt": "yuv420p10le",
            "video_height": 1080,
            "is_hdr": True,
            "duration_seconds": 3036,
            "file_size_bytes": 1554 * 1024 * 1024,
            "audio_stream_count": 1,
            "subtitle_stream_count": 2,
            "audio_streams": [{"index": 1, "codec": "eac3", "language": "cze", "title": "Czech"}],
            "subtitle_streams": [
                {"index": 2, "codec": "subrip", "language": "cze", "title": "Czech"},
                {"index": 3, "codec": "subrip", "language": "cze", "title": "Forced"},
            ],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        self.assertIsNone(simpleripper.skip_reason(config, metadata, track_policy))

    def test_av1_source_matching_profile_but_oversized_is_not_skipped(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {"default": {"encoder": "libsvtav1", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libsvtav1", "pix_fmt": "yuv420p10le"}}
        config["skip_rules"] = {"skip_av1": True, "skip_4k": True, "skip_hdr": True}
        metadata = {
            "media_type": "series",
            "video_codec": "av1",
            "video_pix_fmt": "yuv420p10le",
            "video_height": 1080,
            "is_hdr": False,
            "duration_seconds": 1500,
            "file_size_bytes": 700 * 1024 * 1024,
            "audio_stream_count": 1,
            "subtitle_stream_count": 0,
            "audio_streams": [{"index": 1, "codec": "aac", "language": "cze", "title": "Czech"}],
            "subtitle_streams": [],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        matches, reasons = simpleripper.source_matches_target_profile(config, metadata, "series", track_policy)

        self.assertFalse(matches)
        self.assertIsNone(simpleripper.skip_reason(config, metadata, track_policy))
        self.assertIn("retention_size_exceeded:700.0MB>500.0MB", reasons)

    def test_inspect_candidate_marks_under_limit_hevc_mismatch_as_skipped(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            source = root / "library" / "Fallout" / "Season 02" / "S02E07 Predani.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 1024)
            probe = {
                "format": {"duration": "1800", "bit_rate": "4288000", "tags": {}},
                "streams": [
                    {"index": 0, "codec_type": "video", "codec_name": "hevc", "pix_fmt": "yuv420p", "width": 1920, "height": 1080},
                    {"index": 1, "codec_type": "audio", "codec_name": "eac3", "tags": {"language": "cze", "title": "Czech"}},
                    {"index": 2, "codec_type": "audio", "codec_name": "eac3", "tags": {"language": "eng", "title": "English"}},
                    {"index": 3, "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "cze"}},
                ],
                "chapters": [],
            }

            with patch("simpleripper.run_ffprobe", return_value=(True, probe, None)):
                result = simpleripper.inspect_candidate(config, source, "series")

            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["skip_reason"], "under_retention_size_limit")
            self.assertEqual(result["candidate_reason"], "under_retention_size_limit")
            self.assertIn("pix_fmt_mismatch:yuv420p!=yuv420p10le", result["profile_mismatch_reasons"])

    def test_inspect_candidate_marks_oversized_hevc_as_usable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "series": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            source = root / "library" / "Futurama" / "Season 10" / "S10E06.mkv"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 1024)
            probe = {
                "format": {"duration": "3036", "bit_rate": str(1554 * 1024 * 1024 * 8 // 3036), "tags": {}},
                "streams": [
                    {"index": 0, "codec_type": "video", "codec_name": "hevc", "pix_fmt": "yuv420p10le", "width": 1920, "height": 1080},
                    {"index": 1, "codec_type": "audio", "codec_name": "eac3", "tags": {"language": "cze", "title": "Czech"}},
                    {"index": 2, "codec_type": "subtitle", "codec_name": "subrip", "tags": {"language": "cze"}},
                ],
                "chapters": [],
            }

            with patch("simpleripper.extract_metadata", return_value={"media_type": "series", "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "video_height": 1080, "is_hdr": False, "duration_seconds": 3036, "file_size_bytes": 1554 * 1024 * 1024, "audio_stream_count": 1, "subtitle_stream_count": 1, "audio_streams": [{"index": 1, "codec": "eac3", "language": "cze", "title": "Czech"}], "subtitle_streams": [{"index": 2, "codec": "subrip", "language": "cze", "title": "CZ"}], "overall_bitrate_kbps": 4288}), patch("simpleripper.run_ffprobe", return_value=(True, probe, None)):
                result = simpleripper.inspect_candidate(config, source, "series")

            self.assertEqual(result["status"], "ok")
            self.assertIsNone(result["skip_reason"])
            self.assertEqual(result["candidate_reason"], "hevc_oversized")
            self.assertTrue(result["retention_size_policy"]["oversized"])

    def test_skip_reason_under_retention_limit_overrides_profile_mismatch_when_enabled(self) -> None:
        config = self.make_config(Path("."))
        config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "anime": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
        metadata = {
            "media_type": "anime",
            "video_codec": "h264",
            "video_pix_fmt": "yuv420p",
            "video_height": 1080,
            "is_hdr": False,
            "duration_seconds": 7752,
            "file_size_bytes": 1960 * 1024 * 1024,
            "audio_stream_count": 1,
            "subtitle_stream_count": 0,
            "audio_streams": [{"index": 1, "codec": "aac", "language": "eng", "title": "English"}],
            "subtitle_streams": [],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        matches, reasons = simpleripper.source_matches_target_profile(config, metadata, "anime", track_policy)
        reason = simpleripper.skip_reason(config, metadata, track_policy)
        retention = simpleripper.retention_size_policy_evaluation(config, metadata, "anime")

        self.assertFalse(matches)
        self.assertIn("video_codec_mismatch:h264!=h265,hevc", reasons)
        self.assertIn("pix_fmt_mismatch:yuv420p!=yuv420p10le", reasons)
        self.assertFalse(retention["oversized"])
        self.assertEqual(retention["actual_mb"], 1960.0)
        self.assertEqual(retention["limit_mb"], 2584.0)
        self.assertEqual(reason, "under_retention_size_limit")

    def test_skip_reason_profile_mismatch_stays_usable_when_retention_disabled(self) -> None:
        config = self.make_config(Path("."))
        config["retention_size_policy"]["enabled"] = False
        config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}, "anime": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
        metadata = {
            "media_type": "anime",
            "video_codec": "h264",
            "video_pix_fmt": "yuv420p",
            "video_height": 1080,
            "is_hdr": False,
            "duration_seconds": 7752,
            "file_size_bytes": 1960 * 1024 * 1024,
            "audio_stream_count": 1,
            "subtitle_stream_count": 0,
            "audio_streams": [{"index": 1, "codec": "aac", "language": "eng", "title": "English"}],
            "subtitle_streams": [],
        }
        track_policy = simpleripper.select_streams(config, metadata)

        self.assertIsNone(simpleripper.skip_reason(config, metadata, track_policy))

    def test_pick_next_candidate_logs_under_retention_limit_skip_with_profile_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "Bleach" / "episode.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(b"x" * 20)

            details = {
                "path": source,
                "status": "ok",
                "metadata": {"file_size_bytes": source.stat().st_size, "video_codec": "h264"},
                "score": 0.0,
                "skip_reason": "under_retention_size_limit",
                "candidate_reason": "under_retention_size_limit",
                "profile_mismatch_reasons": ["video_codec_mismatch:h264!=h265,hevc", "pix_fmt_mismatch:yuv420p!=yuv420p10le"],
                "retention_size_policy": {"enabled": True, "oversized": False, "actual_mb": 1960.0, "limit_mb": 2584.0},
            }

            with patch("simpleripper.inspect_candidate", return_value=details):
                selected = app.pick_next_candidate([source])

            self.assertIsNone(selected)
            joined = "\n".join(simpleripper.tail_text_lines(simpleripper.app_log_path(config), 20))
            self.assertIn("candidate_skip_under_retention_limit", joined)
            self.assertIn("skip_profile_mismatch_because_under_limit", joined)
            self.assertIn("video_codec_mismatch:h264!=h265,hevc", joined)

    def test_history_summary_fields_flattens_before_after_values(self) -> None:
        source_meta = {"file_size_bytes": 1000, "video_codec": "h264", "audio_stream_count": 2, "subtitle_stream_count": 1}
        output_meta = {"file_size_bytes": 400, "video_codec": "hevc", "audio_stream_count": 1, "subtitle_stream_count": 1}
        verification = {"output_size_bytes": 400, "output_to_source_ratio": 0.4, "overall_bitrate_kbps": 1500, "suspicious_size_warning_reason": "warn"}

        summary = simpleripper.history_summary_fields(source_meta, output_meta, verification, {"status": "ok"})

        self.assertEqual(summary["source_size_bytes"], 1000)
        self.assertEqual(summary["output_size_bytes"], 400)
        self.assertEqual(summary["video_codec_before"], "h264")
        self.assertEqual(summary["video_codec_after"], "hevc")
        self.assertEqual(summary["audio_stream_count_after"], 1)
        self.assertEqual(summary["verification_warning"], "warn")

    def test_verify_output_uses_bitrate_threshold_not_ratio_only(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "out.mkv"
            output.write_bytes(b"x" * 1024)
            source = {"media_type": "movie", "file_size_bytes": 21900000000, "duration_seconds": 9211.0, "audio_stream_count": 1, "subtitle_stream_count": 1}
            output_meta = {"file_size_bytes": 1780000000, "duration_seconds": 9211.0, "video_codec": "hevc", "video_width": 1920, "video_height": 1080, "audio_stream_count": 1, "subtitle_stream_count": 1, "overall_bitrate_kbps": 1622}
            policy = {"applied": True, "expected_audio_stream_count": 1, "expected_subtitle_stream_count": 1}

            original_stat = Path.stat

            def fake_stat(path: Path, *args: object, **kwargs: object) -> object:
                stat_result = original_stat(path, *args, **kwargs)
                if path == output:
                    return type("StatResult", (), {**{name: getattr(stat_result, name) for name in dir(stat_result) if name.startswith("st_")}, "st_size": 1780000000})()
                return stat_result

            with patch.object(Path, "stat", new=fake_stat):
                verification, errors = simpleripper.verify_output({}, source, output, output_meta, policy)

            self.assertTrue(verification["not_suspiciously_tiny"])
            self.assertTrue(verification["suspicious_size_warning"])
            self.assertFalse(verification["suspicious_size_hard_fail"])
            self.assertEqual(errors, [])

    def test_verify_output_checks_expected_codec_and_pix_fmt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "out.mkv"
            output.write_bytes(b"x" * 1024)
            config = self.make_config(Path(temp_dir))
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            source = {"media_type": "default", "file_size_bytes": 5000, "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0}
            output_meta = {"duration_seconds": 10.0, "video_codec": "h264", "video_pix_fmt": "yuv420p", "audio_stream_count": 1, "subtitle_stream_count": 0, "overall_bitrate_kbps": 1500}

            verification, errors = simpleripper.verify_output(config, source, output, output_meta, {"applied": False})

            self.assertFalse(verification["video_codec_ok"])
            self.assertFalse(verification["pix_fmt_ok"])
            self.assertTrue(any("video_codec_ok" in err for err in errors))
            self.assertTrue(any("pix_fmt_ok" in err for err in errors))

    def test_recover_runtime_state_cleans_interrupted_encoding(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("source", encoding="utf-8")
            local_output = root / "work" / "current" / "output" / "movie.mkv"
            local_output.parent.mkdir(parents=True)
            local_output.write_text("partial", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-1",
                    "phase": "encoding",
                    "source_path": str(source),
                    "local_output_path": str(local_output),
                    "ffmpeg_pid": 999999,
                },
            )

            with patch("simpleripper.is_local_pid_running", return_value=False):
                simpleripper.recover_runtime_state(config)

            self.assertFalse(local_output.exists())
            self.assertFalse(simpleripper.current_job_path(config).exists())
            resume_request = simpleripper.load_resume_request(config)
            self.assertIsNotNone(resume_request)
            self.assertEqual((resume_request or {})["source_path"], str(source))
            self.assertEqual((resume_request or {})["phase"], "encoding")
            jobs = (root / "history" / "jobs.jsonl").read_text(encoding="utf-8")
            self.assertIn("interrupted", jobs)

    def test_recover_runtime_state_cleans_workspace_before_writing_resume_request(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("source", encoding="utf-8")
            work_current = root / "work" / "current"
            local_output = work_current / "output" / "movie.mkv"
            temp_output = work_current / "temp" / "movie.mkv"
            local_output.parent.mkdir(parents=True)
            temp_output.parent.mkdir(parents=True)
            local_output.write_text("partial", encoding="utf-8")
            temp_output.write_text("temp", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-1",
                    "phase": "uploading",
                    "source_path": str(source),
                    "local_output_path": str(local_output),
                    "temp_output_path": str(temp_output),
                    "ffmpeg_pid": 999999,
                },
            )

            original_write_resume_request = simpleripper.write_resume_request

            def checking_write_resume_request(test_config: dict, resume_source: Path, phase: str, job_id: str | None = None) -> None:
                self.assertFalse(work_current.exists())
                self.assertFalse(simpleripper.current_job_path(test_config).exists())
                original_write_resume_request(test_config, resume_source, phase, job_id)

            with patch("simpleripper.write_resume_request", side_effect=checking_write_resume_request):
                simpleripper.recover_runtime_state(config)

            resume_request = simpleripper.load_resume_request(config)
            self.assertIsNotNone(resume_request)
            self.assertEqual((resume_request or {})["source_path"], str(source))
            self.assertFalse(work_current.exists())
            self.assertFalse(simpleripper.current_job_path(config).exists())

    def test_recover_runtime_state_requeues_all_incomplete_pre_swap_phases(self) -> None:
        phases = ["copying_source", "probing_source", "encoding", "probing_output", "verifying", "uploading"]
        for phase in phases:
            with self.subTest(phase=phase):
                with tempfile.TemporaryDirectory() as temp_dir:
                    root = Path(temp_dir)
                    config = self.make_config(root)
                    source = root / "library" / "movie.mkv"
                    source.parent.mkdir(parents=True)
                    source.write_text("source", encoding="utf-8")
                    work_current = root / "work" / "current"
                    local_output = work_current / "output" / "movie.mkv"
                    temp_output = work_current / "temp" / "movie.mkv"
                    local_output.parent.mkdir(parents=True)
                    temp_output.parent.mkdir(parents=True)
                    local_output.write_text("partial", encoding="utf-8")
                    temp_output.write_text("temp", encoding="utf-8")
                    simpleripper.write_json(
                        simpleripper.current_job_path(config),
                        {
                            "job_id": f"job-{phase}",
                            "phase": phase,
                            "source_path": str(source),
                            "local_output_path": str(local_output),
                            "temp_output_path": str(temp_output),
                            "ffmpeg_pid": 999999,
                        },
                    )

                    simpleripper.recover_runtime_state(config)

                    resume_request = simpleripper.load_resume_request(config)
                    self.assertIsNotNone(resume_request)
                    self.assertEqual((resume_request or {})["source_path"], str(source))
                    self.assertEqual((resume_request or {})["phase"], phase)
                    self.assertFalse(work_current.exists())
                    self.assertFalse(simpleripper.current_job_path(config).exists())

    def test_recover_runtime_state_writes_explicit_crash_recovery_log_events(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("source", encoding="utf-8")
            local_output = root / "work" / "current" / "output" / "movie.mkv"
            local_output.parent.mkdir(parents=True)
            local_output.write_text("partial", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-logs",
                    "phase": "encoding",
                    "source_path": str(source),
                    "local_output_path": str(local_output),
                    "ffmpeg_pid": 999999,
                },
            )

            simpleripper.recover_runtime_state(config)

            log_lines = simpleripper.tail_text_lines(simpleripper.app_log_path(config), 20)
            joined = "\n".join(log_lines)
            self.assertIn("crash_recovery_detected", joined)
            self.assertIn("crash_recovery_cleanup_started", joined)
            self.assertIn("crash_recovery_cleanup_finished", joined)
            self.assertIn("crash_recovery_rerun_scheduled", joined)
            self.assertLess(joined.index("crash_recovery_detected"), joined.index("crash_recovery_cleanup_started"))
            self.assertLess(joined.index("crash_recovery_cleanup_started"), joined.index("crash_recovery_cleanup_finished"))
            self.assertLess(joined.index("crash_recovery_cleanup_finished"), joined.index("crash_recovery_rerun_scheduled"))

    def test_app_init_autostarts_when_previous_session_was_running(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("source", encoding="utf-8")
            simpleripper.set_running_requested(config, True)
            simpleripper.write_resume_request(config, source, "encoding", "job-1")

            with patch.object(simpleripper.SimpleRipperApp, "start", autospec=True) as start_mock:
                app = simpleripper.SimpleRipperApp(config)

            self.assertEqual((app._resume_request or {})["source_path"], str(source))
            start_mock.assert_called_once_with(app, reason="auto_resume")

    def test_run_loop_processes_resume_request_before_scanning_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("source", encoding="utf-8")
            app = simpleripper.SimpleRipperApp(config)
            app._resume_request = {"source_path": str(source), "phase": "encoding", "job_id": "job-1"}
            app.state.running = True
            processed: list[Path] = []

            def fake_process_one(path: Path) -> None:
                processed.append(path)
                app.state.force_stop = True

            with patch.object(app, "process_one", side_effect=fake_process_one), patch("simpleripper.scan_candidates") as scan_candidates_mock:
                app._run_loop()

            self.assertEqual(processed, [source])
            scan_candidates_mock.assert_not_called()
            self.assertFalse(simpleripper.resume_request_path(config).exists())

    def test_recover_runtime_state_requeues_final_verify_from_start(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("encoded", encoding="utf-8")
            quarantine = root / "quarantine" / "movie.mkv.original"
            quarantine.parent.mkdir(parents=True)
            quarantine.write_text("original", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-2",
                    "phase": "final_verify",
                    "source_path": str(source),
                    "replacement_path": str(source),
                    "quarantine_path": str(quarantine),
                    "source_metadata": {"media_type": "default", "file_size_bytes": 6000, "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0},
                    "track_policy": {"applied": False},
                },
            )

            simpleripper.recover_runtime_state(config)

            self.assertTrue(source.exists())
            self.assertEqual(source.read_text(encoding="utf-8"), "original")
            self.assertFalse(quarantine.exists())
            resume_request = simpleripper.load_resume_request(config)
            self.assertIsNotNone(resume_request)
            self.assertEqual((resume_request or {})["source_path"], str(source))
            jobs = (root / "history" / "jobs.jsonl").read_text(encoding="utf-8")
            self.assertIn("rollback_restored", jobs)
            self.assertIn('"resume_requested": true', jobs)

    def test_recover_runtime_state_requeues_refreshing_jellyfin_from_start(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("encoded", encoding="utf-8")
            quarantine = root / "quarantine" / "movie.mkv.original"
            quarantine.parent.mkdir(parents=True)
            quarantine.write_text("original", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-3",
                    "phase": "refreshing_jellyfin",
                    "source_path": str(source),
                    "replacement_path": str(source),
                    "quarantine_path": str(quarantine),
                },
            )

            simpleripper.recover_runtime_state(config)

            self.assertTrue(source.exists())
            self.assertEqual(source.read_text(encoding="utf-8"), "original")
            self.assertFalse(quarantine.exists())
            resume_request = simpleripper.load_resume_request(config)
            self.assertIsNotNone(resume_request)
            self.assertEqual((resume_request or {})["phase"], "refreshing_jellyfin")

    def test_process_one_keeps_quarantine_until_after_jellyfin_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "episode.avi"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 6000)

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                if path.suffix.lower() == ".avi":
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 1500}

            def check_refresh(test_config: dict, original_source: Path, replacement: Path | None = None) -> dict[str, str]:
                quarantine_root = root / "quarantine"
                quarantined = list(quarantine_root.rglob("*.original"))
                self.assertEqual(len(quarantined), 1)
                self.assertTrue(quarantined[0].exists())
                return {"status": "ok"}

            with patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.refresh_jellyfin", side_effect=check_refresh):
                app.process_one(source)

            self.assertEqual(list((root / "quarantine").rglob("*.original")), [])

    def test_process_one_avi_replacement_verifies_final_mkv_and_marks_both_paths_done(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            config["paths"]["keep_quarantine_after_success"] = True
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "episode.avi"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 6000)
            replacement = source.with_suffix(".mkv")
            simpleripper.fast_inventory_scan([source.parent], config)
            probed_paths: list[Path] = []

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                probed_paths.append(path)
                if path == source.parent.parent / "work" / "current" / "input" / source.name:
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 1500}

            with patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.refresh_jellyfin", return_value={"status": "ok"}):
                app.process_one(source)

            self.assertFalse(source.exists())
            self.assertTrue(replacement.exists())
            self.assertIn(replacement, probed_paths)
            source_history = simpleripper.load_history_index(config, source)
            replacement_history = simpleripper.load_history_index(config, replacement)
            self.assertEqual((source_history or {})["status"], "done")
            self.assertEqual((source_history or {})["replacement_path"], str(replacement))
            self.assertEqual((replacement_history or {})["status"], "done")
            with simpleripper.open_worker_cache(config) as connection:
                source_row = connection.execute("SELECT decision FROM file_index WHERE path = ?", (str(source),)).fetchone()
                replacement_row = connection.execute("SELECT decision FROM file_index WHERE path = ?", (str(replacement),)).fetchone()
            self.assertEqual(source_row["decision"], "done")
            self.assertEqual(replacement_row["decision"], "done")

    def test_process_one_verification_failure_enters_cooldown_and_is_not_retried_immediately(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "episode.avi"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 6000)
            simpleripper.fast_inventory_scan([source.parent], config)

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            call_count = {"verify": 0}

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                if path.name.endswith(".avi"):
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 1500}

            def fake_verify(test_config: dict, source_meta: dict, output: Path, output_meta: dict, stream_policy: dict) -> tuple[dict, list[str]]:
                call_count["verify"] += 1
                if call_count["verify"] == 1:
                    return {"video_codec_ok": False, "pix_fmt_ok": False}, ["video codec mismatch", "pix fmt mismatch"]
                return {"video_codec_ok": True, "pix_fmt_ok": True}, []

            with patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.verify_output", side_effect=fake_verify):
                app.process_one(source)

            history = simpleripper.load_history_index(config, source)
            self.assertEqual((history or {})["status"], "error")
            self.assertEqual((history or {})["failure_type"], "ffmpeg")
            self.assertEqual(simpleripper.scan_candidates([source.parent], config), [])
            with simpleripper.open_worker_cache(config) as connection:
                row = connection.execute("SELECT decision, retry_after FROM file_index WHERE path = ?", (str(source),)).fetchone()
            self.assertEqual(row["decision"], "failed")
            self.assertTrue(row["retry_after"])

    def test_approve_verification_error_finishes_replacement_without_rerun(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "episode.avi"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 6000)
            replacement = source.with_suffix(".mkv")
            simpleripper.fast_inventory_scan([source.parent], config)

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                if path.name.endswith(".avi"):
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 850}

            with patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.verify_output", return_value=({"output_size_bytes": 1200, "output_to_source_ratio": 0.2, "overall_bitrate_kbps": 850}, ["bitrate too low"])), patch("simpleripper.refresh_jellyfin", return_value={"status": "ok"}):
                app.process_one(source)

            status = app.status()
            self.assertEqual(len(status["errors"]), 1)
            self.assertEqual(status["errors"][0]["actions"], ["approve", "skip"])

            app.queue_manual_error_action(status["errors"][0]["id"], "approve")

            self.assertFalse(source.exists())
            self.assertTrue(replacement.exists())
            self.assertEqual(app.status()["errors"], [])
            self.assertEqual(app.status()["current_phase"], "idle")
            source_history = simpleripper.load_history_index(config, source)
            replacement_history = simpleripper.load_history_index(config, replacement)
            self.assertEqual((source_history or {})["status"], "done")
            self.assertEqual((source_history or {})["approved_reason"], "user_approved_verification_error")
            self.assertEqual((replacement_history or {})["status"], "done")
            jobs = (root / "history" / "jobs.jsonl").read_text(encoding="utf-8")
            self.assertIn('"approved_reason": "user_approved_verification_error"', jobs)
            self.assertTrue((root / "history" / f'{(source_history or {})["job_id"]}.json').exists())
            self.assertEqual(simpleripper.scan_candidates([source.parent], config), [])

    def test_skip_verification_error_marks_source_done_without_reprocessing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            app = simpleripper.SimpleRipperApp(config)
            source = root / "library" / "episode.avi"
            source.parent.mkdir(parents=True)
            source.write_bytes(b"x" * 6000)
            replacement = source.with_suffix(".mkv")
            simpleripper.fast_inventory_scan([source.parent], config)

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                if path.name.endswith(".avi"):
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 850}

            with patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.verify_output", return_value=({"output_size_bytes": 1200, "output_to_source_ratio": 0.2, "overall_bitrate_kbps": 850}, ["bitrate too low"])):
                app.process_one(source)

            error_id = app.status()["errors"][0]["id"]
            app.queue_manual_error_action(error_id, "skip")

            self.assertTrue(source.exists())
            self.assertFalse(replacement.exists())
            self.assertEqual(app.status()["errors"], [])
            self.assertEqual(app.status()["current_phase"], "idle")
            source_history = simpleripper.load_history_index(config, source)
            self.assertEqual((source_history or {})["status"], "done")
            self.assertEqual((source_history or {})["approved_reason"], "user_skipped_verification_error")
            self.assertEqual(simpleripper.scan_candidates([source.parent], config), [])

    def test_queue_manual_error_action_marks_error_queued_while_worker_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            decision_id = "verify:test:local"
            app.state.running = True
            app.state.pending_decisions = {decision_id: {"id": decision_id, "source_path": "movie.mkv"}}
            app.state.errors = [{"id": decision_id, "summary": "needs decision", "source_path": "movie.mkv", "actions": ["approve", "skip"]}]

            result = app.queue_manual_error_action(decision_id, "approve")

            self.assertEqual(result["status"], "queued")
            self.assertEqual((app.state.pending_decisions or {})[decision_id]["queued_action"], "approve")
            self.assertEqual((app.state.queued_error_actions or [])[0]["action"], "approve")
            self.assertEqual((app.state.errors or [])[0]["actions"], [])
            self.assertEqual((app.state.errors or [])[0]["queued_action"], "approve")

    def test_run_loop_continues_to_next_item_after_pending_verification_decision(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            library = root / "library"
            library.mkdir(parents=True)
            first = library / "first.avi"
            second = library / "second.avi"
            first.write_bytes(b"x" * 7000)
            second.write_bytes(b"y" * 6000)
            app = simpleripper.SimpleRipperApp(config)
            app.set_selected_folders([{"path": str(library), "media_type": "auto"}])

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_inspect(test_config: dict, source: Path, media_type: str) -> dict:
                return {
                    "path": source,
                    "status": "ok",
                    "metadata": {"file_size_bytes": source.stat().st_size, "video_codec": "mpeg4"},
                    "skip_reason": None,
                    "score": float(source.stat().st_size),
                    "track_policy": {"applied": False},
                    "target_profile_matches": False,
                    "profile_mismatch_reasons": [],
                    "candidate_reason": "needs_encode",
                    "retention_size_policy": {"oversized": False},
                }

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                if path.name.endswith(".avi"):
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 1500}

            def fake_verify(test_config: dict, source_meta: dict, output: Path, output_meta: dict, stream_policy: dict) -> tuple[dict, list[str]]:
                if output.parent.name == "output" and output.name == "first.mkv":
                    return {"output_size_bytes": 1200, "output_to_source_ratio": 0.2, "overall_bitrate_kbps": 850}, ["bitrate too low"]
                return {"output_size_bytes": 1400, "output_to_source_ratio": 0.3, "overall_bitrate_kbps": 1500, "video_codec_ok": True, "pix_fmt_ok": True}, []

            with patch.object(app, "schedule_rescan_wait", return_value=False), patch("simpleripper.inspect_candidate", side_effect=fake_inspect), patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.verify_output", side_effect=fake_verify), patch("simpleripper.refresh_jellyfin", return_value={"status": "ok"}):
                app.start()
                assert app._thread is not None
                app._thread.join(timeout=5)

            self.assertFalse(app.state.running)
            self.assertTrue((library / "second.mkv").exists())
            self.assertFalse((library / "first.mkv").exists())
            self.assertEqual(len(app.status()["errors"]), 1)
            self.assertEqual(app.status()["errors"][0]["source_path"], str(first))
            self.assertEqual((app.status().get("pending_decision_count") or 0), 1)

    def test_worker_continues_normally_after_processing_queued_error_action(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            config["scan"]["file_extensions"] = [".mkv", ".avi"]
            config["scan_cache"] = {"enabled": True, "queue_size": 25, "fast_inventory_rescan_hours": 24, "max_deep_checks_per_cycle": 50, "failed_retry_hours": 24, "max_failures_before_block": 3, "blocked_retry_days": 30}
            config["quality_profiles"] = {"default": {"encoder": "libx265", "pix_fmt": "yuv420p10le"}}
            library = root / "library"
            library.mkdir(parents=True)
            source = library / "source.avi"
            source.write_bytes(b"x" * 7000)
            third = library / "third.avi"
            third.write_bytes(b"z" * 5000)
            replacement = source.with_suffix(".mkv")
            app = simpleripper.SimpleRipperApp(config)
            app.set_selected_folders([{"path": str(library), "media_type": "auto"}])

            class FakeProcess:
                def __init__(self, command: list[str]) -> None:
                    self.stdout = io.StringIO("")
                    self.pid = 4242
                    self.returncode = 0
                    Path(command[-1]).write_bytes(b"encoded-output")

                def poll(self) -> int:
                    return 0

            def fake_popen(command: list[str], stdout: object = None, stderr: object = None, text: bool = True, encoding: str = "utf-8", errors: str = "replace") -> FakeProcess:
                return FakeProcess(command)

            def fake_inspect(test_config: dict, candidate: Path, media_type: str) -> dict:
                return {
                    "path": candidate,
                    "status": "ok",
                    "metadata": {"file_size_bytes": candidate.stat().st_size, "video_codec": "mpeg4"},
                    "skip_reason": None,
                    "score": float(candidate.stat().st_size),
                    "track_policy": {"applied": False},
                    "target_profile_matches": False,
                    "profile_mismatch_reasons": [],
                    "candidate_reason": "needs_encode",
                    "retention_size_policy": {"oversized": False},
                }

            def fake_probe(test_config: dict, path: Path, media_type: str) -> tuple[dict, dict]:
                if path.name.endswith(".avi"):
                    return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "mpeg4", "video_pix_fmt": "yuv420p", "overall_bitrate_kbps": 4000}
                return {}, {"media_type": "default", "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "overall_bitrate_kbps": 1500}

            verify_calls = {"source_local": 0, "third": 0}

            def fake_verify(test_config: dict, source_meta: dict, output: Path, output_meta: dict, stream_policy: dict) -> tuple[dict, list[str]]:
                if output.parent.name == "output" and output.name == "source.mkv":
                    verify_calls["source_local"] += 1
                    return {"output_size_bytes": 1200, "output_to_source_ratio": 0.2, "overall_bitrate_kbps": 850}, ["bitrate too low"]
                if output.name == "third.mkv":
                    verify_calls["third"] += 1
                return {"output_size_bytes": 1400, "output_to_source_ratio": 0.3, "overall_bitrate_kbps": 1500, "video_codec_ok": True, "pix_fmt_ok": True}, []

            phases: list[str] = []
            original_set_phase = app.set_phase

            def tracked_set_phase(phase: str, file: Path | None = None, extra: dict | None = None) -> None:
                phases.append(phase)
                original_set_phase(phase, file, extra)

            queued_processed = threading.Event()
            original_process_next = app.process_next_queued_error_action

            def tracked_process_next() -> dict:
                result = original_process_next()
                queued_processed.set()
                return result

            with patch.object(app, "set_phase", side_effect=tracked_set_phase), patch.object(app, "schedule_rescan_wait", return_value=False), patch.object(app, "process_next_queued_error_action", side_effect=tracked_process_next), patch("simpleripper.inspect_candidate", side_effect=fake_inspect), patch("simpleripper.subprocess.Popen", side_effect=fake_popen), patch("simpleripper.ffprobe_metadata", side_effect=fake_probe), patch("simpleripper.verify_output", side_effect=fake_verify), patch("simpleripper.refresh_jellyfin", return_value={"status": "ok"}):
                app.process_one(source)
                error_id = app.status()["errors"][0]["id"]
                app.queue_manual_error_action(error_id, "approve")
                app.start()
                assert app._thread is not None
                app._thread.join(timeout=5)

            self.assertTrue(queued_processed.is_set())
            self.assertFalse(app.state.running)
            self.assertTrue(replacement.exists())
            self.assertTrue((library / "third.mkv").exists())
            self.assertEqual(app.status()["errors"], [])
            self.assertEqual((app.status().get("pending_decision_count") or 0), 0)
            self.assertEqual((app.status().get("queued_error_action_count") or 0), 0)
            self.assertIn("processing_error_action", phases)
            processing_index = phases.index("processing_error_action")
            self.assertIn("scanning_inventory", phases[processing_index + 1:])
            self.assertGreaterEqual(verify_calls["third"], 1)

    def test_replace_source_with_output_falls_back_when_replace_crosses_devices(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.avi"
            output = root / "inspection" / "movie.mkv"
            source.parent.mkdir(parents=True)
            output.parent.mkdir(parents=True)
            source.write_bytes(b"source-bytes")
            output.write_bytes(b"encoded-bytes")
            replacement = source.with_suffix(".mkv")

            original_replace = os.replace

            def fake_replace(src: str | bytes, dst: str | bytes) -> None:
                if Path(src) == output and Path(dst) == replacement:
                    raise OSError(errno.EXDEV, "Invalid cross-device link")
                original_replace(src, dst)

            with patch("simpleripper.os.replace", side_effect=fake_replace):
                result = simpleripper.replace_source_with_output(source, output, config, {"job_id": "job-1"}, replacement)

            self.assertTrue(replacement.exists())
            self.assertEqual(replacement.read_bytes(), b"encoded-bytes")
            self.assertFalse(output.exists())
            self.assertFalse(source.exists())
            self.assertTrue(Path(result["quarantine_path"]).exists())

    def test_failed_queued_error_action_restores_pending_decision_without_stopping_loop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            decision_id = "verify:test:local"
            app.state.running = True
            app.state.pending_decisions = {decision_id: {"id": decision_id, "source_path": "movie.mkv", "queued_action": "approve"}}
            app.state.errors = [{"id": decision_id, "summary": "needs decision", "source_path": "movie.mkv", "actions": [], "queued_action": "approve"}]
            app.state.queued_error_actions = [{"id": decision_id, "action": "approve"}]

            with patch.object(app, "resolve_error_action", side_effect=OSError(errno.EXDEV, "Invalid cross-device link")):
                result = app.process_next_queued_error_action()

            self.assertEqual(result["status"], "error")
            self.assertEqual((app.state.pending_decisions or {})[decision_id]["queued_action"], None)
            restored = next(item for item in (app.state.errors or []) if isinstance(item, dict) and item.get("id") == decision_id)
            self.assertEqual(restored["actions"], ["approve", "skip"])
            self.assertNotIn("queued_action", restored)
            self.assertEqual(app.state.current_phase, "idle")

    def test_multiple_queued_error_actions_are_processed_in_order(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            app.state.running = True
            first_id = "verify:first:local"
            second_id = "verify:second:final"
            app.state.pending_decisions = {
                first_id: {"id": first_id, "source_path": "first.mkv"},
                second_id: {"id": second_id, "source_path": "second.mkv"},
            }
            app.state.errors = [
                {"id": first_id, "summary": "first", "source_path": "first.mkv", "actions": ["approve", "skip"]},
                {"id": second_id, "summary": "second", "source_path": "second.mkv", "actions": ["approve", "skip"]},
            ]

            app.queue_manual_error_action(first_id, "approve")
            app.queue_manual_error_action(second_id, "skip")

            self.assertEqual(
                app.state.queued_error_actions,
                [{"id": first_id, "action": "approve"}, {"id": second_id, "action": "skip"}],
            )

            processed: list[tuple[str, str]] = []

            def fake_resolve(decision_id: str, action: str) -> dict[str, str]:
                processed.append((decision_id, action))
                with app._lock:
                    pending_map = app.state.pending_decisions or {}
                    pending_map.pop(decision_id, None)
                    app.state.pending_decisions = pending_map
                app.clear_error(decision_id)
                return {"status": "done", "decision_id": decision_id, "action": action}

            with patch.object(app, "resolve_error_action", side_effect=fake_resolve):
                first_result = app.process_next_queued_error_action()
                second_result = app.process_next_queued_error_action()

            self.assertEqual(first_result["decision_id"], first_id)
            self.assertEqual(second_result["decision_id"], second_id)
            self.assertEqual(processed, [(first_id, "approve"), (second_id, "skip")])
            self.assertEqual(app.state.queued_error_actions, [])
            self.assertEqual(app.state.pending_decisions, {})
            self.assertEqual(app.state.errors, [])

    def test_approving_one_pending_error_does_not_affect_other_pending_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            source_one = root / "library" / "first.avi"
            source_two = root / "library" / "second.avi"
            replacement_one = source_one.with_suffix(".mkv")
            replacement_two = source_two.with_suffix(".mkv")
            source_one.parent.mkdir(parents=True, exist_ok=True)
            source_one.write_bytes(b"first-source")
            source_two.write_bytes(b"second-source")
            replacement_one.write_bytes(b"first-output")
            replacement_two.write_bytes(b"second-output")
            quarantine_one = root / "inspection" / "quarantine" / "first.avi"
            quarantine_two = root / "inspection" / "quarantine" / "second.avi"
            quarantine_one.parent.mkdir(parents=True, exist_ok=True)
            quarantine_one.write_bytes(b"first-original")
            quarantine_two.write_bytes(b"second-original")
            work_dir_one = root / "inspection" / "pending_decisions" / "first"
            work_dir_two = root / "inspection" / "pending_decisions" / "second"
            work_dir_one.mkdir(parents=True, exist_ok=True)
            work_dir_two.mkdir(parents=True, exist_ok=True)
            first_id = "verify:first:final_verification"
            second_id = "verify:second:final_verification"
            first_pending = {
                "id": first_id,
                "source_path": str(source_one),
                "replacement_path": str(replacement_one),
                "work_dir_path": str(work_dir_one),
                "local_output_path": str(work_dir_one / "output" / replacement_one.name),
                "temp_output_path": str(work_dir_one / "preserved-temp" / replacement_one.name),
                "job_id": "job-first",
                "stage": "final_verification",
                "source_before_signature": {"path": str(source_one)},
                "source_metadata": {"path": str(source_one), "started_at": "2026-01-01T00:00:00Z", "video_codec": "mpeg4"},
                "output_metadata": {"path": str(replacement_one), "video_codec": "hevc"},
                "track_policy": {"applied": False},
                "downscale": {"applied": False},
                "verification": {"status": "failed"},
                "quarantine_path": str(quarantine_one),
                "created_at": "2026-01-01T00:00:00Z",
                "queued_action": None,
            }
            second_pending = {
                "id": second_id,
                "source_path": str(source_two),
                "replacement_path": str(replacement_two),
                "work_dir_path": str(work_dir_two),
                "local_output_path": str(work_dir_two / "output" / replacement_two.name),
                "temp_output_path": str(work_dir_two / "preserved-temp" / replacement_two.name),
                "job_id": "job-second",
                "stage": "final_verification",
                "source_before_signature": {"path": str(source_two)},
                "source_metadata": {"path": str(source_two), "started_at": "2026-01-01T00:00:01Z", "video_codec": "mpeg4"},
                "output_metadata": {"path": str(replacement_two), "video_codec": "hevc"},
                "track_policy": {"applied": False},
                "downscale": {"applied": False},
                "verification": {"status": "failed"},
                "quarantine_path": str(quarantine_two),
                "created_at": "2026-01-01T00:00:01Z",
                "queued_action": None,
            }
            app.state.pending_decisions = {first_id: first_pending, second_id: second_pending}
            app.state.errors = [
                {"id": first_id, "summary": "first", "source_path": str(source_one), "actions": ["approve", "skip"]},
                {"id": second_id, "summary": "second", "source_path": str(source_two), "actions": ["approve", "skip"], "queued_action": "skip"},
            ]
            app.state.queued_error_actions = [{"id": second_id, "action": "skip"}]

            with patch("simpleripper.refresh_jellyfin", return_value={"status": "ok"}), patch("simpleripper.source_signature", side_effect=lambda path: {"path": str(path)}):
                result = app.resolve_error_action(first_id, "approve")

            self.assertEqual(result["status"], "done")
            self.assertEqual(result["action"], "approve")
            self.assertNotIn(first_id, app.state.pending_decisions or {})
            self.assertIn(second_id, app.state.pending_decisions or {})
            self.assertEqual((app.state.pending_decisions or {})[second_id]["source_path"], str(source_two))
            self.assertEqual(app.state.queued_error_actions, [{"id": second_id, "action": "skip"}])
            remaining_errors = app.state.errors or []
            self.assertEqual(len(remaining_errors), 1)
            self.assertEqual(remaining_errors[0]["id"], second_id)
            self.assertEqual(remaining_errors[0]["queued_action"], "skip")

    def test_stop_after_current_processes_queued_error_actions_before_stopping(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            app = simpleripper.SimpleRipperApp(config)
            decision_id = "verify:test:local"
            app.state.running = True
            app.state.pending_decisions = {decision_id: {"id": decision_id, "source_path": "movie.mkv", "queued_action": "approve"}}
            app.state.errors = [{"id": decision_id, "summary": "needs decision", "source_path": "movie.mkv", "actions": [], "queued_action": "approve"}]
            app.state.queued_error_actions = [{"id": decision_id, "action": "approve"}]
            app.state.stop_after_current = True

            processed: list[tuple[str, str]] = []

            def fake_resolve(decision_id_arg: str, action_arg: str) -> dict[str, str]:
                processed.append((decision_id_arg, action_arg))
                with app._lock:
                    pending_map = app.state.pending_decisions or {}
                    pending_map.pop(decision_id_arg, None)
                    app.state.pending_decisions = pending_map
                app.clear_error(decision_id_arg)
                return {"status": "done", "decision_id": decision_id_arg, "action": action_arg}

            with patch.object(app, "resolve_error_action", side_effect=fake_resolve), patch("simpleripper.scan_candidates", return_value=[]):
                app._run_loop()

            self.assertFalse(app.state.running)
            self.assertEqual(processed, [(decision_id, "approve")])
            self.assertEqual(app.state.queued_error_actions, [])
            self.assertEqual(app.state.pending_decisions, {})
            self.assertEqual(app.state.errors, [])


if __name__ == "__main__":
    unittest.main()