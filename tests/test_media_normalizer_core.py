from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import media_normalizer as mn


class MediaNormalizerCoreTests(unittest.TestCase):
    def make_config(self) -> dict:
        return {
            "tools": {"ffmpeg": "ffmpeg", "ffprobe": "ffprobe"},
            "encoding": {
                "movie": {
                    "encoder": "libx265",
                    "crf": 23,
                    "preset": "medium",
                    "pix_fmt": "yuv420p10le",
                    "audio": "copy",
                    "subtitles": "copy",
                },
                "unknown": {
                    "encoder": "libx265",
                    "crf": 24,
                    "preset": "medium",
                    "pix_fmt": "yuv420p10le",
                    "audio": "copy",
                    "subtitles": "copy",
                },
            },
            "verification": {
                "max_duration_diff_seconds": 2,
                "clip_max_duration_diff_seconds": 6,
                "min_output_source_ratio": 0.15,
                "max_output_source_ratio": 0.95,
            },
        }

    def make_source_item(self) -> dict:
        return {
            "media_type": "movie",
            "file_size_bytes": 1000,
            "duration_seconds": 100.0,
            "audio_stream_count": 2,
            "subtitle_stream_count": 2,
        }

    def test_build_ffmpeg_command_uses_track_policy_mapping_and_clip_args(self) -> None:
        command = mn.build_ffmpeg_command(
            self.make_config(),
            Path("input.mkv"),
            Path("output.mkv"),
            "movie",
            {"map_arguments": ["-map", "0:0", "-map", "0:1", "-map", "0:t?"]},
            clip_start=12.3456,
            clip_duration=60,
        )

        self.assertEqual(
            command,
            [
                "ffmpeg",
                "-hide_banner",
                "-y",
                "-ss",
                "12.346",
                "-i",
                "input.mkv",
                "-t",
                "60",
                "-map",
                "0:0",
                "-map",
                "0:1",
                "-map",
                "0:t?",
                "-c:v",
                "libx265",
                "-preset",
                "medium",
                "-crf",
                "23",
                "-pix_fmt",
                "yuv420p10le",
                "-c:a",
                "copy",
                "-c:s",
                "copy",
                "-c:t",
                "copy",
                "-map_metadata",
                "0",
                "-map_chapters",
                "0",
                "-metadata",
                f"encoded_by={mn.NORMALIZER_APP_NAME}",
                "output.mkv",
            ],
        )

    def test_build_ffmpeg_command_defaults_to_map_all(self) -> None:
        command = mn.build_ffmpeg_command(self.make_config(), Path("input.mkv"), Path("output.mkv"), "movie")

        self.assertIn("-map", command)
        self.assertIn("0", command)
        self.assertLess(command.index("-map"), command.index("-c:v"))

    def test_verify_output_accepts_track_policy_audio_reduction(self) -> None:
        config = self.make_config()
        source_item = self.make_source_item()
        track_policy_result = {"applied": True, "expected_audio_stream_count": 1, "expected_subtitle_stream_count": 2}
        output_item = {
            "duration_seconds": 100.5,
            "audio_stream_count": 1,
            "subtitle_stream_count": 2,
            "video_codec": "hevc",
            "file_size_bytes": 200,
            "file_size_mb": 0.2,
            "container_format": "matroska",
            "video_width": 1920,
            "video_height": 1080,
            "video_pix_fmt": "yuv420p10le",
            "video_bitrate_kbps": 800,
            "overall_bitrate_kbps": 1300,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "output.mkv"
            output.write_bytes(b"x" * 200)

            with patch.object(mn, "run_ffprobe", return_value=mn.ProbeResult(ok=True, data={})), patch.object(mn, "extract_metadata", return_value=output_item):
                verification, output_summary, errors = mn.verify_output(config, source_item, output, track_policy_result)

        self.assertTrue(verification["audio_streams_ok"])
        self.assertTrue(verification["subtitle_streams_ok"])
        self.assertTrue(verification["video_stream_exists"])
        self.assertEqual(errors, [])
        self.assertEqual(output_summary["audio_stream_count"], 1)

    def test_verify_output_warns_instead_of_failing_low_ratio_movie_with_acceptable_bitrate(self) -> None:
        config = self.make_config()
        source_item = {
            "media_type": "movie",
            "file_size_bytes": 21900000000,
            "duration_seconds": 9211.0,
            "audio_stream_count": 1,
            "subtitle_stream_count": 1,
            "video_width": 1920,
            "video_height": 1080,
        }
        output_size = 1780000000
        output_item = {
            "duration_seconds": 9211.0,
            "audio_stream_count": 1,
            "subtitle_stream_count": 1,
            "video_codec": "hevc",
            "file_size_bytes": output_size,
            "file_size_mb": round(output_size / 1024 / 1024, 2),
            "container_format": "matroska",
            "video_width": 1920,
            "video_height": 1080,
            "video_pix_fmt": "yuv420p10le",
            "overall_bitrate_kbps": 1622,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "output.mkv"
            output.write_bytes(b"x" * 1024)

            original_stat = Path.stat

            def fake_stat(path: Path, *args: object, **kwargs: object) -> object:
                if path == output:
                    stat_result = original_stat(path, *args, **kwargs)
                    return type("StatResult", (), {**{name: getattr(stat_result, name) for name in dir(stat_result) if name.startswith("st_")}, "st_size": output_size})()
                return original_stat(path, *args, **kwargs)

            with patch.object(Path, "stat", new=fake_stat), patch.object(mn, "run_ffprobe", return_value=mn.ProbeResult(ok=True, data={})), patch.object(mn, "extract_metadata", return_value=output_item):
                verification, _, errors = mn.verify_output(config, source_item, output)

        self.assertTrue(verification["not_suspiciously_tiny"])
        self.assertTrue(verification["suspicious_size_warning"])
        self.assertFalse(verification["suspicious_size_hard_fail"])
        self.assertAlmostEqual(verification["output_to_source_ratio"], output_size / source_item["file_size_bytes"])
        self.assertEqual(verification["source_size_bytes"], source_item["file_size_bytes"])
        self.assertEqual(verification["output_size_bytes"], output_size)
        self.assertEqual(verification["overall_bitrate_kbps"], 1622)
        self.assertEqual(verification["suspicious_size_threshold_used"]["hard_fail_kbps"], 1200)
        self.assertIn("low output/source ratio", verification["suspicious_size_warning_reason"])
        self.assertIn("warning threshold 1800 kbps", verification["suspicious_size_warning_reason"])
        self.assertNotIn("Verification failed: not_suspiciously_tiny", errors)
        self.assertEqual(errors, [])

    def test_verify_output_fails_tiny_1080p_movie_below_hard_bitrate_threshold(self) -> None:
        config = self.make_config()
        source_item = {
            "media_type": "movie",
            "file_size_bytes": 21900000000,
            "duration_seconds": 9211.0,
            "audio_stream_count": 1,
            "subtitle_stream_count": 1,
            "video_width": 1920,
            "video_height": 1080,
        }
        output_item = {
            "duration_seconds": 9211.0,
            "audio_stream_count": 1,
            "subtitle_stream_count": 1,
            "video_codec": "hevc",
            "file_size_bytes": 250000000,
            "file_size_mb": 238.42,
            "container_format": "matroska",
            "video_width": 1920,
            "video_height": 1080,
            "video_pix_fmt": "yuv420p10le",
            "overall_bitrate_kbps": 217,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "output.mkv"
            output.write_bytes(b"x" * 1024)

            with patch.object(mn, "run_ffprobe", return_value=mn.ProbeResult(ok=True, data={})), patch.object(mn, "extract_metadata", return_value=output_item):
                verification, _, errors = mn.verify_output(config, source_item, output)

        self.assertFalse(verification["not_suspiciously_tiny"])
        self.assertTrue(verification["suspicious_size_warning"])
        self.assertTrue(verification["suspicious_size_hard_fail"])
        self.assertEqual(verification["suspicious_size_threshold_used"]["hard_fail_kbps"], 1200)
        self.assertIn("Verification failed: not_suspiciously_tiny", errors)

    def test_verify_output_uses_configured_bitrate_threshold_override(self) -> None:
        config = self.make_config()
        config["verification"]["bitrate_thresholds"] = {"movie": {"1080p": {"hard_fail_kbps": 1700, "warning_kbps": 1900}}}
        source_item = {**self.make_source_item(), "file_size_bytes": 10000, "video_width": 1920, "video_height": 1080}
        output_item = {
            "duration_seconds": 100.0,
            "audio_stream_count": 2,
            "subtitle_stream_count": 2,
            "video_codec": "hevc",
            "file_size_bytes": 200,
            "file_size_mb": 0.2,
            "container_format": "matroska",
            "video_width": 1920,
            "video_height": 1080,
            "overall_bitrate_kbps": 1622,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "output.mkv"
            output.write_bytes(b"x" * 200)

            with patch.object(mn, "run_ffprobe", return_value=mn.ProbeResult(ok=True, data={})), patch.object(mn, "extract_metadata", return_value=output_item):
                verification, _, errors = mn.verify_output(config, source_item, output)

        self.assertFalse(verification["not_suspiciously_tiny"])
        self.assertEqual(verification["suspicious_size_threshold_used"]["hard_fail_kbps"], 1700)
        self.assertIn("Verification failed: not_suspiciously_tiny", errors)

    def test_verify_output_detects_audio_mismatch_without_track_policy(self) -> None:
        config = self.make_config()
        source_item = self.make_source_item()
        output_item = {
            "duration_seconds": 100.5,
            "audio_stream_count": 1,
            "subtitle_stream_count": 2,
            "video_codec": "hevc",
            "file_size_bytes": 200,
            "file_size_mb": 0.2,
            "container_format": "matroska",
            "video_width": 1920,
            "video_height": 1080,
            "video_pix_fmt": "yuv420p10le",
            "video_bitrate_kbps": 800,
            "overall_bitrate_kbps": 1000,
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "output.mkv"
            output.write_bytes(b"x" * 200)

            with patch.object(mn, "run_ffprobe", return_value=mn.ProbeResult(ok=True, data={})), patch.object(mn, "extract_metadata", return_value=output_item):
                verification, _, errors = mn.verify_output(config, source_item, output)

        self.assertFalse(verification["audio_streams_ok"])
        self.assertIn("Verification failed: audio_streams_ok", errors)


if __name__ == "__main__":
    unittest.main()