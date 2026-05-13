from __future__ import annotations

import os
import socket
import subprocess
import tempfile
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
            "scan": {"file_extensions": [".mkv"], "processed_marker_suffix": ".simpleripper.done.json", "lock_suffix": ".simpleripper.lock"},
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

    def test_web_ui_folder_picker_uses_browser_endpoint(self) -> None:
        self.assertIn("/api/browse-folders", simpleripper.INDEX_HTML)
        self.assertIn("function pickFolder(initialDir=''){browseFolder(initialDir||'')}", simpleripper.INDEX_HTML)
        self.assertIn("function selectBrowsedFolder(path){post('/api/custom-folder',{path:path,media_type:guessMediaType(path)});closeFolderBrowser()}", simpleripper.INDEX_HTML)
        self.assertNotIn("function pickFolder(initialDir=''){post('/api/custom-folder'", simpleripper.INDEX_HTML)
        self.assertFalse(hasattr(simpleripper, "pick_folder_dialog"))

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

            with patch("simpleripper.copy_file_interruptible", side_effect=simpleripper.ForceStopRequested("force stop requested")):
                app.process_one(source)

            status = app.status()
            self.assertEqual(status["current_phase"], "idle")
            self.assertFalse(status["force_stop"])
            self.assertEqual(status["errors"], [])
            self.assertFalse(simpleripper.current_job_path(config).exists())
            self.assertFalse((Path(config["paths"]["local_work_dir"]) / "current").exists())

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
            self.assertFalse(simpleripper.marker_path(source, config).exists())
            quarantine = Path(result["quarantine_path"])
            self.assertTrue(quarantine.exists())
            self.assertIn(str(root / "quarantine" / "Movies" / "A"), str(quarantine))

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
            local_output = root / "work" / "current" / "output" / "movie.mkv"
            local_output.parent.mkdir(parents=True)
            local_output.write_text("partial", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-1",
                    "phase": "encoding",
                    "source_path": str(root / "library" / "movie.mkv"),
                    "local_output_path": str(local_output),
                    "ffmpeg_pid": 999999,
                },
            )

            with patch("simpleripper.is_local_pid_running", return_value=False):
                simpleripper.recover_runtime_state(config)

            self.assertFalse(local_output.exists())
            self.assertFalse(simpleripper.current_job_path(config).exists())
            jobs = (root / "history" / "jobs.jsonl").read_text(encoding="utf-8")
            self.assertIn("interrupted", jobs)

    def test_recover_runtime_state_verifies_replaced_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            config = self.make_config(root)
            source = root / "library" / "movie.mkv"
            source.parent.mkdir(parents=True)
            source.write_text("encoded", encoding="utf-8")
            simpleripper.write_json(
                simpleripper.current_job_path(config),
                {
                    "job_id": "job-2",
                    "phase": "final_verify",
                    "source_path": str(source),
                    "source_metadata": {"media_type": "default", "file_size_bytes": 6000, "duration_seconds": 10.0, "audio_stream_count": 1, "subtitle_stream_count": 0},
                    "track_policy": {"applied": False},
                },
            )

            with patch("simpleripper.ffprobe_metadata", return_value=({}, {"duration_seconds": 10.0, "video_codec": "hevc", "video_pix_fmt": "yuv420p10le", "audio_stream_count": 1, "subtitle_stream_count": 0, "overall_bitrate_kbps": 1500})), patch(
                "simpleripper.verify_output", return_value=({"video_codec_ok": True, "pix_fmt_ok": True}, [])
            ):
                simpleripper.recover_runtime_state(config)

            jobs = (root / "history" / "jobs.jsonl").read_text(encoding="utf-8")
            self.assertIn("replacement_verified", jobs)


if __name__ == "__main__":
    unittest.main()