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
            "overall_bitrate_kbps": 1000,
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