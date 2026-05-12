from __future__ import annotations

import io
import tempfile
import textwrap
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

import media_normalizer as mn
import queue_store
import shared_locks


class CliIntegrationTests(unittest.TestCase):
    def write_config(self, temp_dir: str) -> Path:
                library_root = (Path(temp_dir) / "library").as_posix()
                config_text = textwrap.dedent(
                        f"""
                        output_root: '{(Path(temp_dir) / 'output').as_posix()}'
                        shared_state_dir: '{(Path(temp_dir) / '.ripper_state').as_posix()}'
                        libraries:
                            anime:
                                - '{library_root}'
                        batch:
                            default_count: 1
                        node:
                            id: test-node
                            roles:
                                web_ui: false
                                worker: true
                                manager: false
                        worker:
                            enabled: true
                            schedule:
                                enabled: false
                                start: '02:00'
                                end: '07:00'
                                outside_window_behavior: finish_current_do_not_start_new
                        """
                )
                path = Path(temp_dir) / "config.yaml"
                path.write_text(config_text, encoding="utf-8")
                return path

    def create_source_file(self, temp_dir: str, relative_path: str = "library/file.mkv") -> Path:
        source = Path(temp_dir) / relative_path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("source-data\n", encoding="utf-8")
        return source

    def test_main_distributed_init_creates_shared_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            buffer = io.StringIO()

            with redirect_stdout(buffer):
                result = mn.main(["distributed-init", "--config", str(config_path)])

            self.assertEqual(result, 0)
            self.assertTrue((Path(temp_dir) / ".ripper_state" / "queue").is_dir())
            self.assertIn("Initialized distributed state", buffer.getvalue())

    def test_main_enqueue_top_and_claim_one_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            item = {
                "source_path": "/library/file.mkv",
                "library_root": "/library",
                "media_type": "anime",
                "bucket": "anime_high",
                "file_size_bytes": 123,
                "file_size_mb": 0.12,
                "duration_seconds": 100.0,
                "video_codec": "h264",
                "audio_stream_count": 1,
                "subtitle_stream_count": 0,
            }

            with patch.object(mn, "plan_top_candidates", return_value=([], [item], {})):
                self.assertEqual(mn.main(["enqueue-top", "--config", str(config_path), "--count", "1"]), 0)

            config = mn.load_config(config_path)
            status_before = queue_store.queue_status(config)
            self.assertEqual(status_before["states"]["queue"], 1)

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                result = mn.main(["queue-claim-one", "--config", str(config_path), "--node-id", "worker-a"])

            status_after = queue_store.queue_status(config)
            self.assertEqual(result, 0)
            self.assertEqual(status_after["states"]["queue"], 0)
            self.assertEqual(status_after["states"]["running"], 1)
            self.assertIn("worker-a", buffer.getvalue())

    def test_main_lock_release_does_not_require_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lock_path = Path(temp_dir) / "slot_1.json"
            lock_path.write_text("{}\n", encoding="utf-8")

            result = mn.main(["lock-release", "--path", str(lock_path)])

            self.assertEqual(result, 0)
            self.assertFalse(lock_path.exists())

    def test_main_manager_step_marks_ready_job_done(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
            config["manager"] = {"enabled": True, "run_continuously": True}
            source = self.create_source_file(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "ready_for_finalize" / "job_cli_ready.json",
                {"schema_version": 1, "job_id": "job_cli_ready", "status": "ready_for_finalize", "source_path": str(source)},
            )

            with patch.object(mn, "load_config", return_value=config):
                result = mn.main(["manager-step", "--config", str(config_path), "--dry-run-result", "done"])

            status = queue_store.queue_status(config)
            self.assertEqual(result, 0)
            self.assertEqual(status["states"]["done"], 1)

    def test_worker_to_manager_dry_run_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
            config["manager"] = {"enabled": True, "run_continuously": True}
            source = self.create_source_file(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "queue" / "job_pipeline_ready.json",
                {"schema_version": 1, "job_id": "job_pipeline_ready", "status": "queue", "source_path": str(source), "source_size_bytes": 100, "media_type": "anime", "bucket": "anime_high"},
            )

            with patch.object(mn, "load_config", return_value=config):
                self.assertEqual(mn.main(["worker-step", "--config", str(config_path), "--dry-run-result", "ready"]), 0)
                self.assertEqual(mn.main(["manager-step", "--config", str(config_path), "--dry-run-result", "done"]), 0)

            status = queue_store.queue_status(config)
            ready_dir = Path(config["shared_state_dir"]) / "ready_outputs" / "job_pipeline_ready"
            quarantine_manifest = Path(config["shared_state_dir"]) / "quarantine_manifest" / "job_pipeline_ready.json"
            self.assertEqual(status["states"]["queue"], 0)
            self.assertEqual(status["states"]["ready_for_finalize"], 0)
            self.assertEqual(status["states"]["done"], 1)
            self.assertFalse(ready_dir.exists())
            self.assertTrue(quarantine_manifest.exists())

    def test_main_manager_step_execute_moves_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
            config["manager"] = {"enabled": True, "run_continuously": True, "execute": False}
            source = self.create_source_file(temp_dir)
            root = queue_store.init_state(config)
            ready_dir = root / "ready_outputs" / "job_cli_execute"
            ready_dir.mkdir(parents=True)
            output_path = ready_dir / "output.mkv"
            output_path.write_text("encoded-data\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "worker_log.json", {"job_id": "job_cli_execute", "status": "ready_for_finalize"})
            (ready_dir / "checksum.sha256").write_text("deadbeef  output.mkv\n", encoding="utf-8")
            queue_store.write_json_atomic(ready_dir / "manifest.json", {"job_id": "job_cli_execute", "source_path": str(source), "ready_output_path": str(output_path), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}})
            queue_store.write_json_atomic(ready_dir / "output.ffprobe.json", {"job_id": "job_cli_execute", "source_path": str(source), "dry_run": True, "source_summary": {"media_type": "anime", "file_size_bytes": 1000, "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1}, "output_summary": {"duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "video_codec": "hevc", "file_size_bytes": 15}})
            queue_store.write_json_atomic(root / "ready_for_finalize" / "job_cli_execute.json", {"schema_version": 1, "job_id": "job_cli_execute", "status": "ready_for_finalize", "source_path": str(source), "media_type": "anime", "duration_seconds": 120.0, "audio_stream_count": 2, "subtitle_stream_count": 1, "source_size_bytes": 1000, "dry_run": True, "ready_output_dir": str(ready_dir), "ready_output_path": str(output_path), "ready_output_manifest": str(ready_dir / "manifest.json"), "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json")})

            with patch.object(mn, "load_config", return_value=config):
                result = mn.main(["manager-step", "--config", str(config_path), "--execute"])

            status = queue_store.queue_status(config)
            quarantine_manifest = root / "quarantine_manifest" / "job_cli_execute.json"
            self.assertEqual(result, 0)
            self.assertEqual(status["states"]["done"], 1)
            self.assertEqual(queue_store.read_json(quarantine_manifest)["status"], "executed")
            self.assertEqual(source.read_text(encoding="utf-8"), "encoded-data\n")

    def test_main_manager_loop_wires_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
            config["manager"] = {"enabled": True, "run_continuously": True}

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "manager_loop", return_value={"status": "loop_complete", "iterations": 1, "stop_reason": "idle"}) as loop_mock:
                result = mn.main(["manager-loop", "--config", str(config_path), "--node-id", "manager-a", "--max-iterations", "3", "--idle-sleep-seconds", "0.1", "--stop-on-idle"])

            self.assertEqual(result, 0)
            loop_mock.assert_called_once_with(config, node_override="manager-a", force=False, dry_run_result="done", execute=False, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)

    def test_main_worker_loop_wires_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "gaming-server", "roles": {"web_ui": True, "worker": True, "manager": False}}
            config["worker"] = {"enabled": True, "run_continuously": True}

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "worker_loop", return_value={"status": "loop_complete", "iterations": 1, "stop_reason": "idle"}) as loop_mock:
                result = mn.main(["worker-loop", "--config", str(config_path), "--node-id", "worker-a", "--dry-run-result", "ready", "--execute", "--max-iterations", "3", "--idle-sleep-seconds", "0.1", "--stop-on-idle"])

            self.assertEqual(result, 0)
            loop_mock.assert_called_once_with(config, node_override="worker-a", force=False, dry_run_result="ready", execute=True, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)

    def production_config(self, temp_dir: str) -> tuple[Path, dict]:
        config_path = self.write_config(temp_dir)
        config = mn.load_config(config_path)
        config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
        config["worker"] = {"enabled": False, "run_continuously": False}
        config["manager"] = {"enabled": True, "run_continuously": True, "execute": True}
        config["jellyfin"] = {"enabled": True, "server_url": "http://jellyfin", "api_key": "test"}
        config["production"] = {
            "enabled": True,
            "tick_seconds": 0,
            "enqueue": {"queue_target": 2, "queue_max": 3, "enqueue_count_per_tick": 2, "filesystem_limit": 10, "min_duration_seconds": None},
            "finalizer": {"enabled": True, "max_finalize_per_tick": 1},
            "backpressure": {"max_ready_for_finalize_jobs": 5, "max_ready_outputs_gb": 20, "max_running_jobs": 3, "max_total_inflight_jobs": 10},
            "recovery": {"recover_stale_jobs": False, "recover_stale_locks": False, "requeue_interrupted_jobs": False},
            "safety": {"require_manager_execute": True, "require_jellyfin_enabled": True, "require_worker_disabled_on_manager_node": True},
        }
        return config_path, config

    def test_production_loop_cli_wires_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path, config = self.production_config(temp_dir)

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "production_loop", return_value={"status": "loop_complete", "iterations": 1}) as loop_mock:
                result = mn.main(["production-loop", "--config", str(config_path), "--node-id", "media-server", "--execute", "--max-iterations", "1", "--tick-seconds", "0"])

            self.assertEqual(result, 0)
            loop_mock.assert_called_once_with(config, node_override="media-server", execute=True, max_iterations=1, tick_seconds=0.0)

    def test_production_tick_blocks_enqueue_when_ready_outputs_limit_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, config = self.production_config(temp_dir)
            config["production"]["backpressure"]["max_ready_outputs_gb"] = 0
            root = queue_store.init_state(config)
            ready_dir = root / "ready_outputs" / "job_big"
            ready_dir.mkdir(parents=True, exist_ok=True)
            (ready_dir / "output.mkv").write_bytes(b"x" * 1024)

            with patch.object(mn, "plan_top_candidates") as plan_mock:
                result = mn.production_tick(config)

            self.assertEqual(result["production_state"], "blocked")
            self.assertIn("enqueue blocked: ready_outputs size limit exceeded", result["blocked_reasons"])
            plan_mock.assert_not_called()

    def test_production_tick_finalizes_before_enqueueing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, config = self.production_config(temp_dir)
            source = self.create_source_file(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(root / "ready_for_finalize" / "job_ready.json", {"schema_version": 1, "job_id": "job_ready", "status": "ready_for_finalize", "source_path": str(source)})
            item = {"source_path": str(source.parent / "next.mkv"), "library_root": str(source.parent), "media_type": "anime", "bucket": "anime_high", "file_size_bytes": 200, "file_size_mb": 0.1, "duration_seconds": 120.0, "video_codec": "h264", "audio_stream_count": 1, "subtitle_stream_count": 0}

            with patch.object(mn, "plan_top_candidates", return_value=([], [item], {"selection_strategy": "test"})):
                result = mn.production_tick(config)

            status = queue_store.queue_status(config)
            self.assertEqual(result["last_finalize_count"], 1)
            self.assertEqual(result["last_enqueue_count"], 1)
            self.assertEqual(status["states"]["done"], 1)
            self.assertEqual(status["states"]["queue"], 1)

    def test_production_enqueue_skips_duplicate_source_in_existing_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, config = self.production_config(temp_dir)
            source = self.create_source_file(temp_dir)
            root = queue_store.init_state(config)
            job_id = queue_store.job_id_for_source(str(source), source.stat().st_size, source.stat().st_mtime_ns)
            queue_store.write_json_atomic(root / "done" / f"{job_id}.json", {"schema_version": 1, "job_id": job_id, "status": "done", "source_path": str(source)})
            duplicate = {"source_path": str(source), "library_root": str(source.parent), "media_type": "anime", "bucket": "anime_high", "file_size_bytes": source.stat().st_size, "source_mtime_ns": source.stat().st_mtime_ns, "file_size_mb": 0.1, "duration_seconds": 120.0, "video_codec": "h264", "audio_stream_count": 1, "subtitle_stream_count": 0}

            with patch.object(mn, "plan_top_candidates", return_value=([], [duplicate], {"selection_strategy": "test"})):
                result = mn.production_enqueue_once(config, 1, "production")

            self.assertEqual(result["created_count"], 0)
            self.assertEqual(result["skipped_existing_count"], 1)
            self.assertEqual(queue_store.queue_status(config)["states"]["queue"], 0)

    def test_production_enqueue_filters_configured_media_types_before_planning(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            _, config = self.production_config(temp_dir)
            config["production"]["enqueue"]["media_types"] = ["anime", "series"]
            config["production"]["enqueue"]["filesystem_limit"] = 5
            config["tools"] = {"ffprobe": "ffprobe"}
            library = Path(temp_dir) / "library"
            anime = self.create_source_file(temp_dir, "library/anime.mkv")
            series = self.create_source_file(temp_dir, "library/series.mkv")
            movie = self.create_source_file(temp_dir, "library/movie.mkv")
            anime.write_bytes(b"a" * 300)
            series.write_bytes(b"s" * 500)
            movie.write_bytes(b"m" * 1000)
            config["libraries"] = {"anime": [str(library)], "series": [str(library)], "movie": [str(library)]}

            def fake_walk(_config: dict) -> list[dict]:
                return [
                    {"source_path": str(movie), "media_type": "movie", "library_root": str(library)},
                    {"source_path": str(series), "media_type": "series", "library_root": str(library)},
                    {"source_path": str(anime), "media_type": "anime", "library_root": str(library)},
                ]

            def fake_extract(path: Path, media_type: str, library_root: str, _probe: dict) -> dict:
                stat = path.stat()
                return {"source_path": str(path), "library_root": library_root, "media_type": media_type, "bucket": f"{media_type}_high", "file_size_bytes": stat.st_size, "file_size_mb": stat.st_size / 1024 / 1024, "source_mtime_ns": stat.st_mtime_ns, "duration_seconds": 120.0, "video_codec": "h264", "audio_stream_count": 1, "subtitle_stream_count": 0}

            with patch.object(mn, "require_tool"), patch.object(mn, "walk_libraries", side_effect=fake_walk), patch.object(mn, "run_ffprobe", return_value=mn.ProbeResult(ok=True, data={})), patch.object(mn, "extract_metadata", side_effect=fake_extract), patch.object(mn, "bucket_for_item", side_effect=lambda item, _config: item.get("bucket") or f"{item['media_type']}_high"), patch.object(mn, "skip_reason", return_value=None):
                result = mn.production_enqueue_once(config, 2, "production")

            queued = [queue_store.read_json(path) for path in queue_store.list_state_files(config, "queue")]
            self.assertEqual(result["created_count"], 2)
            self.assertEqual([Path(item["source_path"]).name for item in result["enqueued"]], ["series.mkv", "anime.mkv"])
            self.assertNotIn("movie", [job["media_type"] for job in queued])

    def test_main_worker_step_wires_execute_argument(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "worker_step", return_value={"status": "dry_run_complete", "result_state": "ready_for_finalize"}) as step_mock:
                result = mn.main(["worker-step", "--config", str(config_path), "--dry-run-result", "ready", "--execute"])

            self.assertEqual(result, 0)
            step_mock.assert_called_once_with(config, node_override=None, force=False, dry_run_result="ready", execute=True)

    def test_main_maintenance_loop_wires_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["maintenance"] = {"run_continuously": True}

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "maintenance_loop", return_value={"status": "loop_complete", "iterations": 1, "stop_reason": "idle"}) as loop_mock:
                result = mn.main(["maintenance-loop", "--config", str(config_path), "--max-iterations", "3", "--idle-sleep-seconds", "0.1", "--stop-on-idle"])

            self.assertEqual(result, 0)
            loop_mock.assert_called_once_with(config, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)

    def test_main_maintenance_loop_recovers_stale_locks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config.setdefault("heartbeat", {})["stale_after_seconds"] = 60
            config.setdefault("io_limits", {})["lock_stale_after_seconds"] = 30

            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T01:58:00+02:00"):
                held_lock = shared_locks.acquire_lock(config, "nas_read", "worker-a", "job-lock")

            buffer = io.StringIO()
            with patch.object(mn, "load_config", return_value=config), patch.object(shared_locks, "utc_now", return_value="2026-05-10T02:00:00+02:00"), redirect_stdout(buffer):
                result = mn.main(["maintenance-loop", "--config", str(config_path), "--max-iterations", "1"])

            self.assertEqual(result, 0)
            self.assertFalse(Path(held_lock["path"]).exists())
            self.assertIn('"lock_recovered_count": 1', buffer.getvalue())

    def test_main_node_run_starts_worker_and_manager_loops(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
            config["worker"] = {"enabled": True, "run_continuously": True}
            config["manager"] = {"enabled": True, "run_continuously": True, "execute": False}

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "worker_loop", return_value={"status": "loop_complete", "iterations": 1, "stop_reason": "idle"}) as worker_mock, patch.object(mn, "manager_loop", return_value={"status": "loop_complete", "iterations": 2, "stop_reason": "max_iterations"}) as manager_mock:
                result = mn.main(["node-run", "--config", str(config_path), "--node-id", "node-a", "--worker-dry-run-result", "ready", "--worker-execute", "--manager-dry-run-result", "done", "--max-iterations", "3", "--idle-sleep-seconds", "0.1", "--stop-on-idle"])

            self.assertEqual(result, 0)
            worker_mock.assert_called_once_with(config=config, node_override="node-a", force=False, dry_run_result="ready", execute=True, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)
            manager_mock.assert_called_once_with(config=config, node_override="node-a", force=False, dry_run_result="done", execute=False, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)

    def test_main_node_run_starts_worker_manager_and_maintenance_loops(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "media-server", "roles": {"web_ui": True, "worker": True, "manager": True}}
            config["worker"] = {"enabled": True, "run_continuously": True}
            config["manager"] = {"enabled": True, "run_continuously": True, "execute": False}
            config["maintenance"] = {"run_continuously": True}

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "worker_loop", return_value={"status": "loop_complete", "iterations": 1, "stop_reason": "idle"}) as worker_mock, patch.object(mn, "manager_loop", return_value={"status": "loop_complete", "iterations": 2, "stop_reason": "max_iterations"}) as manager_mock, patch.object(mn, "maintenance_loop", return_value={"status": "loop_complete", "iterations": 3, "stop_reason": "idle"}) as maintenance_mock:
                result = mn.main(["node-run", "--config", str(config_path), "--node-id", "node-a", "--worker-dry-run-result", "ready", "--worker-execute", "--manager-dry-run-result", "done", "--max-iterations", "3", "--idle-sleep-seconds", "0.1", "--stop-on-idle"])

            self.assertEqual(result, 0)
            worker_mock.assert_called_once_with(config=config, node_override="node-a", force=False, dry_run_result="ready", execute=True, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)
            manager_mock.assert_called_once_with(config=config, node_override="node-a", force=False, dry_run_result="done", execute=False, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)
            maintenance_mock.assert_called_once_with(config=config, max_iterations=3, idle_sleep_seconds=0.1, stop_on_idle=True)

    def test_main_node_run_skips_when_no_continuous_services_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config["node"] = {"id": "desktop-pc", "roles": {"web_ui": True, "worker": False, "manager": False}}
            config["worker"] = {"enabled": False, "run_continuously": False}
            config["manager"] = {"enabled": False, "run_continuously": False}

            with patch.object(mn, "load_config", return_value=config):
                result = mn.main(["node-run", "--config", str(config_path)])

            self.assertEqual(result, 0)

    def test_main_node_control_sets_and_reads_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            buffer = io.StringIO()

            with redirect_stdout(buffer):
                result = mn.main(["node-control", "--config", str(config_path), "--node-id", "worker-a", "--worker-command", "stop_after_current", "--updated-by", "test"])

            self.assertEqual(result, 0)
            self.assertIn('"worker_command": "stop_after_current"', buffer.getvalue())

    def test_main_queue_status_includes_node_controls(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)

            self.assertEqual(mn.main(["node-control", "--config", str(config_path), "--node-id", "worker-a", "--worker-command", "stop_after_current", "--updated-by", "test"]), 0)

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                result = mn.main(["queue-status", "--config", str(config_path)])

            self.assertEqual(result, 0)
            self.assertIn('"node_control_count": 1', buffer.getvalue())
            self.assertIn('"worker_command": "stop_after_current"', buffer.getvalue())

            buffer = io.StringIO()
            with redirect_stdout(buffer):
                result = mn.main(["node-control", "--config", str(config_path), "--node-id", "worker-a"])

            self.assertEqual(result, 0)
            self.assertIn('"worker_command": "stop_after_current"', buffer.getvalue())

    def test_main_queue_status_includes_heartbeat_summaries(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
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

            buffer = io.StringIO()
            with patch.object(mn, "load_config", return_value=config), patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"), redirect_stdout(buffer):
                result = mn.main(["queue-status", "--config", str(config_path)])

            self.assertEqual(result, 0)
            self.assertIn('"worker_heartbeat_count": 1', buffer.getvalue())
            self.assertIn('"worker_summary": {', buffer.getvalue())
            self.assertIn('"state_counts": {', buffer.getvalue())
            self.assertIn('"current_phase": "encoding_execute"', buffer.getvalue())
            self.assertIn('"heartbeat_age_seconds": 30', buffer.getvalue())

    def test_main_recover_stale_jobs_moves_running_job_to_stale(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "running" / "job_cli_stale.worker-a.json",
                {
                    "schema_version": 1,
                    "job_id": "job_cli_stale",
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
                    "roles": {"web_ui": False, "worker": True, "manager": False},
                    "worker_state": "running",
                    "current_job_id": "job_cli_stale",
                    "current_phase": "encoding_execute",
                    "last_heartbeat": "2026-05-10T01:58:30+02:00",
                },
            )

            buffer = io.StringIO()
            with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"), redirect_stdout(buffer):
                result = mn.main(["recover-stale-jobs", "--config", str(config_path)])

            self.assertEqual(result, 0)
            self.assertTrue((root / "stale" / "job_cli_stale.worker-a.json").exists())
            self.assertIn('"recovered_count": 1', buffer.getvalue())

    def test_main_recover_stale_locks_releases_expired_slot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            config.setdefault("heartbeat", {})["stale_after_seconds"] = 60
            config.setdefault("io_limits", {})["lock_stale_after_seconds"] = 30

            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T01:58:00+02:00"):
                held_lock = shared_locks.acquire_lock(config, "nas_read", "worker-a", "job-lock")

            buffer = io.StringIO()
            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T02:00:00+02:00"), redirect_stdout(buffer):
                result = mn.main(["recover-stale-locks", "--config", str(config_path)])

            self.assertEqual(result, 0)
            self.assertFalse(Path(held_lock["path"]).exists())
            self.assertIn('"recovered_count": 1', buffer.getvalue())

    def test_main_requeue_interrupted_jobs_moves_job_back_to_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "interrupted" / "job_cli_interrupt.worker-a.json",
                {
                    "schema_version": 1,
                    "job_id": "job_cli_interrupt",
                    "status": "interrupted",
                    "source_path": "/library/file.mkv",
                    "error": "hard_stop_requested",
                    "interrupted_at": "2026-05-10T01:58:00+02:00",
                },
            )

            buffer = io.StringIO()
            with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"), redirect_stdout(buffer):
                result = mn.main(["requeue-interrupted-jobs", "--config", str(config_path), "--job-id", "job_cli_interrupt"])

            self.assertEqual(result, 0)
            self.assertTrue((root / "queue" / "job_cli_interrupt.worker-a.json").exists())
            self.assertIn('"moved_count": 1', buffer.getvalue())

    def test_main_web_ui_wires_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config(temp_dir)
            config = mn.load_config(config_path)

            with patch.object(mn, "load_config", return_value=config), patch.object(mn, "run_web_ui_server", return_value=0) as web_mock:
                result = mn.main(["web-ui", "--config", str(config_path), "--host", "127.0.0.1", "--port", "6060"])

            self.assertEqual(result, 0)
            web_mock.assert_called_once_with(config, host="127.0.0.1", port=6060)


if __name__ == "__main__":
    unittest.main()