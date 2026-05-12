from __future__ import annotations

import json
import tempfile
import threading
import unittest
from http.cookiejar import CookieJar
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import HTTPCookieProcessor, Request, build_opener, urlopen

import media_normalizer as mn
import queue_store
import shared_locks
import web_ui
from unittest.mock import patch


class WebUiTests(unittest.TestCase):
    def make_config(self, temp_dir: str) -> dict:
        root = Path(temp_dir)
        return {
            "output_root": str(root / "output"),
            "libraries": {"anime": [str(root / "library")]},
            "shared_state_dir": str(root / ".ripper_state"),
            "node": {"id": "web-node", "roles": {"web_ui": True, "worker": True, "manager": False}},
            "web_ui": {"host": "127.0.0.1", "port": 0},
        }

    def make_auth_config(self, temp_dir: str) -> dict:
        config = self.make_config(temp_dir)
        config["web_ui"]["auth"] = {
            "enabled": True,
            "username": "admin",
            "password_hash": web_ui.hash_web_ui_password("secret-pass"),
        }
        return config

    def make_manager_config(self, temp_dir: str) -> dict:
        config = self.make_config(temp_dir)
        config["node"] = {"id": "manager-node", "roles": {"web_ui": True, "worker": True, "manager": True}}
        config["manager"] = {"enabled": True, "run_continuously": True, "execute": False}
        config["io_limits"] = {"use_shared_locks": True, "max_concurrent_finalizers": 1}
        return config

    def write_config_file(self, temp_dir: str, profile: str | None = None) -> Path:
        root = Path(temp_dir)
        yaml_lines = [
            f"output_root: {str(root / 'output').replace('\\', '/')}",
            "libraries:",
            "  anime:",
            f"    - {str(root / 'library').replace('\\', '/')}",
            f"shared_state_dir: {str(root / '.ripper_state').replace('\\', '/')}",
            "node:",
            "  id: web-node",
            "  roles:",
            "    web_ui: true",
            "    worker: true",
            "    manager: false",
            "web_ui:",
            "  host: 127.0.0.1",
            "  port: 0",
            "worker:",
            "  enabled: true",
            "  schedule:",
            "    enabled: false",
            "    start: '01:00'",
            "    end: '02:00'",
            "    outside_window_behavior: finish_current_do_not_start_new",
        ]
        if profile:
            yaml_lines.extend(
                [
                    f"default_profile: {profile}",
                    "profiles:",
                    f"  {profile}:",
                    "    worker:",
                    "      enabled: true",
                    "      schedule:",
                    "        enabled: false",
                    "        start: '03:00'",
                    "        end: '04:00'",
                    "        outside_window_behavior: finish_current_do_not_start_new",
                ]
            )
        path = root / "config.yaml"
        path.write_text("\n".join(yaml_lines) + "\n", encoding="utf-8")
        return path

    def write_manager_config_file(self, temp_dir: str, profile: str | None = None) -> Path:
        root = Path(temp_dir)
        yaml_lines = [
            f"output_root: {str(root / 'output').replace('\\', '/')}",
            "libraries:",
            "  anime:",
            f"    - {str(root / 'library').replace('\\', '/')}",
            f"shared_state_dir: {str(root / '.ripper_state').replace('\\', '/')}",
            "node:",
            "  id: manager-node",
            "  roles:",
            "    web_ui: true",
            "    worker: true",
            "    manager: true",
            "web_ui:",
            "  host: 127.0.0.1",
            "  port: 0",
            "worker:",
            "  enabled: true",
            "manager:",
            "  enabled: true",
            "  run_continuously: true",
            "  execute: false",
            "  require_successful_jellyfin_refresh: false",
            "jellyfin:",
            "  enabled: true",
            "  server_url: http://root-jellyfin:8096",
            "  api_key: root-key",
            "  timeout_seconds: 30",
            "io_limits:",
            "  use_shared_locks: true",
            "  max_concurrent_finalizers: 1",
        ]
        if profile:
            yaml_lines.extend(
                [
                    f"default_profile: {profile}",
                    "profiles:",
                    f"  {profile}:",
                    "    manager:",
                    "      enabled: true",
                    "      run_continuously: true",
                    "      execute: false",
                    "      require_successful_jellyfin_refresh: false",
                    "    jellyfin:",
                    "      enabled: true",
                    "      server_url: http://profile-jellyfin:8096",
                    "      api_key: profile-key",
                    "      timeout_seconds: 45",
                ]
            )
        path = root / "config.yaml"
        path.write_text("\n".join(yaml_lines) + "\n", encoding="utf-8")
        return path

    def test_build_status_payload_wraps_queue_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)

            payload = web_ui.build_status_payload(config)

            self.assertEqual(payload["node_id"], "web-node")
            self.assertIn("status", payload)
            self.assertIn("states", payload["status"])
            self.assertIn("services", payload)
            self.assertTrue(payload["services"]["worker"]["role_enabled"])
            self.assertFalse(payload["services"]["worker"]["configured_enabled"])
            self.assertFalse(payload["services"]["manager"]["role_enabled"])

    def test_web_ui_auth_requires_login_and_sets_session_cookie(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_auth_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                with self.assertRaises(HTTPError) as unauthorized_context:
                    urlopen(f"http://{host}:{port}/api/status")
                cookie_jar = CookieJar()
                opener = build_opener(HTTPCookieProcessor(cookie_jar))
                login_request = Request(
                    f"http://{host}:{port}/login",
                    data=b"username=admin&password=secret-pass",
                    headers={"Content-Type": "application/x-www-form-urlencoded"},
                    method="POST",
                )
                with opener.open(login_request) as response:
                    html = response.read().decode("utf-8")
                with opener.open(f"http://{host}:{port}/api/status") as response:
                    payload = json.loads(response.read().decode("utf-8"))
                has_session_cookie_after_login = any(cookie.name == "autoripper_session" for cookie in cookie_jar)
                logout_request = Request(f"http://{host}:{port}/logout", data=b"", method="POST")
                with opener.open(logout_request) as response:
                    logged_out_html = response.read().decode("utf-8")
                has_session_cookie_after_logout = any(cookie.name == "autoripper_session" for cookie in cookie_jar)
                with self.assertRaises(HTTPError) as logged_out_context:
                    opener.open(f"http://{host}:{port}/api/status")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(unauthorized_context.exception.code, 401)
            unauthorized_context.exception.close()
            self.assertIn("Autoripper Node web-node", html)
            self.assertIn("Logout", html)
            self.assertEqual(payload["node_id"], "web-node")
            self.assertTrue(has_session_cookie_after_login)
            self.assertIn("Autoripper Login", logged_out_html)
            self.assertFalse(has_session_cookie_after_logout)
            self.assertEqual(logged_out_context.exception.code, 401)
            logged_out_context.exception.close()

    def test_web_ui_auth_rejects_unauthenticated_control_post(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_auth_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/worker/pause",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(HTTPError) as context:
                    urlopen(request)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(context.exception.code, 401)
            context.exception.close()

    def test_web_ui_server_serves_status_json_and_html(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "queue" / "job_web_queue.json",
                {
                    "schema_version": 1,
                    "job_id": "job_web_queue",
                    "status": "queue",
                    "source_path": "/library/file.mkv",
                    "media_type": "anime",
                    "bucket": "anime_high",
                },
            )
            queue_store.write_json_atomic(
                root / "workers" / "web-node.json",
                {
                    "node_id": "web-node",
                    "hostname": "web-host",
                    "roles": {"web_ui": True, "worker": True, "manager": False},
                    "worker_state": "idle",
                    "current_job_id": None,
                    "current_phase": "idle",
                    "last_heartbeat": "2026-05-10T02:00:00+02:00",
                },
            )
            queue_store.write_json_atomic(
                root / "logs" / "manager" / "job_manager_log.json",
                {"job_id": "job_manager_log", "status": "done", "node_id": "web-node"},
            )
            ready_output_dir = root / "ready_outputs" / "job_web_queue"
            ready_output_dir.mkdir(parents=True, exist_ok=True)
            queue_store.write_json_atomic(
                ready_output_dir / "worker_log.json",
                {"job_id": "job_web_queue", "status": "ready_for_finalize", "node_id": "web-node"},
            )
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                with urlopen(f"http://{host}:{port}/api/status") as response:
                    payload = json.loads(response.read().decode("utf-8"))
                with urlopen(f"http://{host}:{port}/api/workers") as response:
                    workers_payload = json.loads(response.read().decode("utf-8"))
                with urlopen(f"http://{host}:{port}/api/jobs") as response:
                    jobs_payload = json.loads(response.read().decode("utf-8"))
                with urlopen(f"http://{host}:{port}/api/logs") as response:
                    logs_payload = json.loads(response.read().decode("utf-8"))
                with urlopen(f"http://{host}:{port}/api/locks") as response:
                    locks_payload = json.loads(response.read().decode("utf-8"))
                with urlopen(f"http://{host}:{port}/") as response:
                    html = response.read().decode("utf-8")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(payload["node_id"], "web-node")
            self.assertEqual(payload["status"]["worker_heartbeat_count"], 1)
            self.assertEqual(workers_payload["worker_summary"]["total"], 1)
            self.assertEqual(workers_payload["workers"][0]["node_id"], "web-node")
            self.assertEqual(jobs_payload["states"]["queue"], 1)
            self.assertEqual(jobs_payload["jobs"]["queue"][0]["job_id"], "job_web_queue")
            self.assertEqual(logs_payload["counts"]["manager_logs"], 1)
            self.assertEqual(logs_payload["counts"]["worker_bundle_logs"], 1)
            self.assertEqual(logs_payload["manager_logs"][0]["payload"]["job_id"], "job_manager_log")
            self.assertIn("locks", payload["status"])
            self.assertIn("locks", locks_payload)
            self.assertIn("/api/status", html)
            self.assertIn("/api/workers", html)
            self.assertIn("/api/jobs", html)
            self.assertIn("/api/logs", html)
            self.assertIn("/api/locks", html)
            self.assertIn("/api/settings/worker-schedule", html)
            self.assertIn("Save Schedule", html)
            self.assertIn("Action Status", html)
            self.assertIn("No recent control action.", html)
            self.assertIn("Schedule Status", html)
            self.assertIn("Outside Window Mode", html)
            self.assertIn("Hard Stop Warning", html)
            self.assertIn("Node Overview", html)
            self.assertIn("Global Queue", html)
            self.assertIn("Workers", html)
            self.assertIn("Managers", html)
            self.assertIn("Shared Locks", html)
            self.assertIn("Loading worker heartbeats", html)
            self.assertIn("Loading manager heartbeats", html)
            self.assertIn("Worker Heartbeat Detail", html)
            self.assertIn("Manager Heartbeat Detail", html)
            self.assertIn("Shared Lock Detail", html)
            self.assertIn("Logs Viewer", html)
            self.assertIn("Filter Logs", html)
            self.assertIn("All Sources", html)
            self.assertIn("Log Details", html)
            self.assertIn("Raw Status", html)
            self.assertIn("Start Worker", html)
            self.assertIn("Pause Worker", html)
            self.assertIn("Worker Hard Stop", html)
            self.assertIn("Worker State", html)
            self.assertIn("Manager State", html)
            self.assertIn("Worker Role Capability", html)
            self.assertNotIn("Worker Role</span><div class=\"card-value ok\">Enabled", html)
            self.assertNotIn("Manager Role</span><div class=\"card-value ok\">Enabled", html)
            self.assertIn("Pause Global Queue", html)
            self.assertIn("Enter Maintenance Mode", html)
            self.assertIn("Resume Global Queue", html)
            self.assertIn("Recover Stale Locks", html)
            self.assertIn("Autoripper Node web-node", html)

    def test_web_ui_server_can_recover_stale_locks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            config["heartbeat"] = {"stale_after_seconds": 60}
            config["io_limits"] = {"lock_stale_after_seconds": 30}
            queue_store.init_state(config)
            with patch.object(shared_locks, "utc_now", return_value="2026-05-10T01:58:00+02:00"):
                held_lock = shared_locks.acquire_lock(config, "nas_read", "web-node", "job_lock123")
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/maintenance/recover-stale-locks",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with patch.object(shared_locks, "utc_now", return_value="2026-05-10T02:00:00+02:00"), urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertTrue(held_lock["acquired"])
            self.assertEqual(payload["recovered_count"], 1)
            self.assertFalse(Path(held_lock["path"]).exists())

    def test_web_ui_server_can_finalize_pending_job_now(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_manager_config(temp_dir)
            root = queue_store.init_state(config)
            source = Path(temp_dir) / "library" / "file.mkv"
            source.parent.mkdir(parents=True, exist_ok=True)
            source.write_text("source-data\n", encoding="utf-8")
            ready_dir = root / "ready_outputs" / "job_ready123"
            ready_dir.mkdir(parents=True, exist_ok=True)
            (ready_dir / "output.mkv").write_text("dry-run output\n", encoding="utf-8")
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
            queue_store.write_json_atomic(
                root / "ready_for_finalize" / "job_ready123.json",
                {
                    "schema_version": 1,
                    "job_id": "job_ready123",
                    "status": "ready_for_finalize",
                    "source_path": str(source),
                    "media_type": "anime",
                    "bucket": "anime_high",
                    "duration_seconds": 120.0,
                    "audio_stream_count": 2,
                    "subtitle_stream_count": 1,
                    "source_size_bytes": 1000,
                    "dry_run": True,
                    "ready_output_dir": str(ready_dir),
                    "ready_output_path": str(ready_dir / "output.mkv"),
                    "ready_output_manifest": str(ready_dir / "manifest.json"),
                    "ready_output_ffprobe": str(ready_dir / "output.ffprobe.json"),
                },
            )
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/manager/finalize-now",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
                with urlopen(f"http://{host}:{port}/") as response:
                    html = response.read().decode("utf-8")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            status = queue_store.queue_status(config)
            self.assertEqual(payload["status"], "dry_run_complete")
            self.assertEqual(payload["result_state"], "done")
            self.assertEqual(payload["execution_mode"], "dry_run")
            self.assertTrue(Path(payload["finalization_log"]).exists())
            self.assertEqual(status["states"]["done"], 1)
            self.assertIn("Finalize Pending Now", html)
            self.assertIn("Production Mode", html)
            self.assertIn("Run Tick Now", html)

    def test_web_ui_server_can_trigger_jellyfin_full_scan(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_manager_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/jellyfin/full-scan",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with patch.object(mn, "jellyfin_full_scan", return_value={"enabled": True, "status": "scan_triggered"}) as scan_mock:
                    with urlopen(request) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                    with urlopen(f"http://{host}:{port}/") as response:
                        html = response.read().decode("utf-8")
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(payload["status"], "scan_triggered")
            scan_mock.assert_called_once_with(config)
            self.assertIn("Trigger Jellyfin Full Scan", html)

    def test_web_ui_server_can_read_and_persist_worker_schedule_for_active_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config_file(temp_dir, profile="night")
            config = mn.load_config(config_path)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                with urlopen(f"http://{host}:{port}/api/settings/worker-schedule") as response:
                    before_payload = json.loads(response.read().decode("utf-8"))
                request = Request(
                    f"http://{host}:{port}/api/settings/worker-schedule",
                    data=json.dumps(
                        {
                            "enabled": True,
                            "start": "04:15",
                            "end": "06:45",
                            "outside_window_behavior": "stop_after_current",
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    after_payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            reloaded = mn.load_config(config_path)
            raw_config = mn.yaml.safe_load(config_path.read_text(encoding="utf-8"))
            self.assertEqual(before_payload["schedule"]["start"], "03:00")
            self.assertFalse(before_payload["schedule"]["enabled"])
            self.assertTrue(after_payload["schedule"]["enabled"])
            self.assertEqual(after_payload["schedule"]["start"], "04:15")
            self.assertEqual(after_payload["schedule"]["end"], "06:45")
            self.assertEqual(after_payload["schedule"]["outside_window_behavior"], "stop_after_current")
            self.assertTrue(reloaded["worker"]["schedule"]["enabled"])
            self.assertEqual(reloaded["worker"]["schedule"]["start"], "04:15")
            self.assertEqual(raw_config["worker"]["schedule"]["start"], "01:00")
            self.assertEqual(raw_config["profiles"]["night"]["worker"]["schedule"]["start"], "04:15")

    def test_web_ui_server_can_pause_and_start_worker_via_config_toggle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_config_file(temp_dir, profile="night")
            config = mn.load_config(config_path)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                pause_request = Request(
                    f"http://{host}:{port}/api/worker/pause",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(pause_request) as response:
                    paused_payload = json.loads(response.read().decode("utf-8"))
                start_request = Request(
                    f"http://{host}:{port}/api/worker/start",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(start_request) as response:
                    started_payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            raw_config = mn.yaml.safe_load(config_path.read_text(encoding="utf-8"))
            reloaded = mn.load_config(config_path)
            self.assertFalse(paused_payload["worker_enabled"])
            self.assertTrue(started_payload["worker_enabled"])
            self.assertTrue(started_payload["schedule"]["enabled"] is False)
            self.assertTrue(reloaded["worker"]["enabled"])
            self.assertTrue(raw_config["worker"]["enabled"])
            self.assertTrue(raw_config["profiles"]["night"]["worker"]["enabled"])

    def test_web_ui_server_can_set_local_worker_stop_after_current(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/worker/stop-after-current",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            control = queue_store.read_node_control(config, "web-node")
            self.assertEqual(payload["node_id"], "web-node")
            self.assertEqual(payload["control"]["worker_command"], "stop_after_current")
            self.assertEqual(payload["control"]["updated_by"], "web-ui-test")
            self.assertEqual(control["worker_command"], "stop_after_current")
            self.assertEqual(control["updated_by"], "web-ui-test")

    def test_web_ui_server_can_set_local_worker_hard_stop(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/worker/hard-stop",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            control = queue_store.read_node_control(config, "web-node")
            self.assertEqual(payload["node_id"], "web-node")
            self.assertEqual(payload["control"]["worker_command"], "hard_stop")
            self.assertEqual(payload["control"]["updated_by"], "web-ui-test")
            self.assertEqual(control["worker_command"], "hard_stop")
            self.assertEqual(control["updated_by"], "web-ui-test")

    def test_web_ui_server_can_pause_and_resume_global_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                pause_request = Request(
                    f"http://{host}:{port}/api/global/pause-queue",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(pause_request) as response:
                    paused_payload = json.loads(response.read().decode("utf-8"))
                resume_request = Request(
                    f"http://{host}:{port}/api/global/resume-queue",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(resume_request) as response:
                    resumed_payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(paused_payload["global_control"]["queue_state"], "paused")
            self.assertFalse(paused_payload["global_control"]["allow_new_claims"])
            self.assertEqual(resumed_payload["global_control"]["queue_state"], "running")
            self.assertTrue(resumed_payload["global_control"]["allow_new_claims"])

    def test_web_ui_server_can_enter_maintenance_mode(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/global/maintenance",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(payload["global_control"]["queue_state"], "maintenance")
            self.assertFalse(payload["global_control"]["allow_new_claims"])
            self.assertFalse(payload["global_control"]["allow_finalizer"])

    def test_web_ui_server_can_recover_stale_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "running" / "job_web_stale.web-node.json",
                {
                    "schema_version": 1,
                    "job_id": "job_web_stale",
                    "status": "running",
                    "source_path": "/library/file.mkv",
                    "claimed_by": "web-node",
                    "claimed_at": "2026-05-10T01:58:00+02:00",
                },
            )
            queue_store.write_json_atomic(
                root / "workers" / "web-node.json",
                {
                    "node_id": "web-node",
                    "hostname": "worker-host",
                    "roles": {"web_ui": True, "worker": True, "manager": False},
                    "worker_state": "running",
                    "current_job_id": "job_web_stale",
                    "current_phase": "encoding_execute",
                    "last_heartbeat": "2026-05-10T01:58:30+02:00",
                },
            )
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/maintenance/recover-stale-jobs",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"), urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(payload["recovered_count"], 1)
            self.assertTrue((root / "stale" / "job_web_stale.web-node.json").exists())

    def test_web_ui_server_can_requeue_interrupted_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            root = queue_store.init_state(config)
            queue_store.write_json_atomic(
                root / "interrupted" / "job_web_interrupted.web-node.json",
                {
                    "schema_version": 1,
                    "job_id": "job_web_interrupted",
                    "status": "interrupted",
                    "source_path": "/library/file.mkv",
                    "error": "hard_stop_requested",
                    "interrupted_at": "2026-05-10T01:58:00+02:00",
                },
            )
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/maintenance/requeue-interrupted-jobs",
                    data=json.dumps({"job_id": "job_web_interrupted"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with patch.object(queue_store, "utc_now", return_value="2026-05-10T02:00:00+02:00"), urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertEqual(payload["moved_count"], 1)
            self.assertTrue((root / "queue" / "job_web_interrupted.web-node.json").exists())

    def test_web_ui_server_can_set_local_manager_stop_after_current(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_manager_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/manager/stop-after-current",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            control = queue_store.read_node_control(config, "manager-node")
            self.assertEqual(payload["node_id"], "manager-node")
            self.assertEqual(payload["control"]["manager_command"], "stop_after_current")
            self.assertEqual(control["manager_command"], "stop_after_current")

    def test_web_ui_server_can_pause_and_start_manager_via_config_toggle(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_manager_config_file(temp_dir, profile="night")
            config = mn.load_config(config_path)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                pause_request = Request(
                    f"http://{host}:{port}/api/manager/pause",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(pause_request) as response:
                    paused_payload = json.loads(response.read().decode("utf-8"))
                start_request = Request(
                    f"http://{host}:{port}/api/manager/start",
                    data=json.dumps({}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(start_request) as response:
                    started_payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            raw_config = mn.yaml.safe_load(config_path.read_text(encoding="utf-8"))
            reloaded = mn.load_config(config_path)
            self.assertFalse(paused_payload["manager_enabled"])
            self.assertTrue(started_payload["manager_enabled"])
            self.assertTrue(reloaded["manager"]["enabled"])
            self.assertTrue(raw_config["manager"]["enabled"])
            self.assertTrue(raw_config["profiles"]["night"]["manager"]["enabled"])

    def test_web_ui_server_can_read_and_persist_manager_settings_for_active_profile(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = self.write_manager_config_file(temp_dir, profile="night")
            config = mn.load_config(config_path)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                with urlopen(f"http://{host}:{port}/api/settings/manager") as response:
                    before_payload = json.loads(response.read().decode("utf-8"))
                request = Request(
                    f"http://{host}:{port}/api/settings/manager",
                    data=json.dumps(
                        {
                            "enabled": True,
                            "run_continuously": False,
                            "execute": True,
                            "require_successful_jellyfin_refresh": True,
                            "jellyfin_enabled": True,
                            "server_url": "http://updated-jellyfin:8096",
                            "api_key": "updated-key",
                            "timeout_seconds": 55,
                        }
                    ).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlopen(request) as response:
                    after_payload = json.loads(response.read().decode("utf-8"))
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            reloaded = mn.load_config(config_path)
            raw_config = mn.yaml.safe_load(config_path.read_text(encoding="utf-8"))
            self.assertTrue(before_payload["manager_role_enabled"])
            self.assertTrue(before_payload["manager"]["run_continuously"])
            self.assertEqual(before_payload["jellyfin"]["server_url"], "http://profile-jellyfin:8096")
            self.assertTrue(before_payload["jellyfin"]["api_key_configured"])
            self.assertEqual(before_payload["jellyfin"]["api_key_masked"], "********")
            self.assertFalse(after_payload["manager"]["run_continuously"])
            self.assertTrue(after_payload["manager"]["execute"])
            self.assertTrue(after_payload["manager"]["require_successful_jellyfin_refresh"])
            self.assertEqual(after_payload["jellyfin"]["server_url"], "http://updated-jellyfin:8096")
            self.assertEqual(after_payload["jellyfin"]["timeout_seconds"], 55)
            self.assertTrue(after_payload["jellyfin"]["api_key_configured"])
            self.assertTrue(reloaded["manager"]["execute"])
            self.assertTrue(reloaded["manager"]["require_successful_jellyfin_refresh"])
            self.assertEqual(reloaded["jellyfin"]["server_url"], "http://updated-jellyfin:8096")
            self.assertEqual(raw_config["manager"]["run_continuously"], True)
            self.assertEqual(raw_config["jellyfin"]["server_url"], "http://root-jellyfin:8096")
            self.assertFalse(raw_config["profiles"]["night"]["manager"]["run_continuously"])
            self.assertTrue(raw_config["profiles"]["night"]["manager"]["execute"])
            self.assertTrue(raw_config["profiles"]["night"]["manager"]["require_successful_jellyfin_refresh"])
            self.assertEqual(raw_config["profiles"]["night"]["jellyfin"]["server_url"], "http://updated-jellyfin:8096")
            self.assertEqual(raw_config["profiles"]["night"]["jellyfin"]["api_key"], "updated-key")
            self.assertEqual(raw_config["profiles"]["night"]["jellyfin"]["timeout_seconds"], 55)

    def test_web_ui_manager_stop_returns_conflict_when_manager_role_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/manager/stop-after-current",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(HTTPError) as context:
                    urlopen(request)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertIn("409", str(context.exception))
            context.exception.close()

    def test_web_ui_worker_start_returns_conflict_on_manager_node_when_worker_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_manager_config(temp_dir)
            config["worker"] = {"enabled": False, "run_continuously": False}
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/worker/start",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(HTTPError) as context:
                    urlopen(request)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertIn("409", str(context.exception))
            context.exception.close()

    def test_web_ui_manager_finalize_now_returns_conflict_when_manager_role_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/manager/finalize-now",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(HTTPError) as context:
                    urlopen(request)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertIn("409", str(context.exception))
            context.exception.close()

    def test_web_ui_jellyfin_full_scan_returns_conflict_when_manager_role_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = self.make_config(temp_dir)
            queue_store.init_state(config)
            server = web_ui.create_web_ui_server(config, host="127.0.0.1", port=0)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                host, port = server.server_address[:2]
                request = Request(
                    f"http://{host}:{port}/api/jellyfin/full-scan",
                    data=json.dumps({"updated_by": "web-ui-test"}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with self.assertRaises(HTTPError) as context:
                    urlopen(request)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(timeout=5)

            self.assertIn("409", str(context.exception))
            context.exception.close()


if __name__ == "__main__":
    unittest.main()