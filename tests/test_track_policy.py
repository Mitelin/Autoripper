from __future__ import annotations

import unittest

from track_policy import apply_track_policy


def make_config() -> dict:
    return {
        "track_policy": {
            "enabled": True,
            "anime": {"target_audio_languages": ["eng"], "drop_other_audio_if_target_found": True},
            "series": {"target_audio_languages": ["cze"], "drop_other_audio_if_target_found": True},
            "movie": {"target_audio_languages": ["cze"], "drop_other_audio_if_target_found": True},
            "unknown": {"cleanup_enabled": False},
        }
    }


def make_item(media_type: str, audio_streams: list[dict], subtitle_streams: list[dict] | None = None) -> dict:
    return {
        "media_type": media_type,
        "video_stream_index": 0,
        "audio_stream_count": len(audio_streams),
        "audio_streams": audio_streams,
        "subtitle_stream_count": len(subtitle_streams or []),
        "subtitle_streams": subtitle_streams or [],
    }


class TrackPolicyTests(unittest.TestCase):
    def test_single_audio_stream_preserves_all(self) -> None:
        item = make_item("movie", [{"index": 1, "codec": "ac3", "language": "eng", "title": "English 5.1"}])

        result = apply_track_policy(make_config(), item)

        self.assertFalse(result["applied"])
        self.assertEqual(result["ffmpeg_mapping"], "map_all")
        self.assertEqual(result["map_arguments"], ["-map", "0"])

    def test_movie_keeps_czech_and_drops_english_when_confident(self) -> None:
        item = make_item(
            "movie",
            [
                {"index": 1, "codec": "ac3", "language": "cze", "title": "DD 5.1"},
                {"index": 2, "codec": "dts", "language": "eng", "title": "DTS 5.1"},
            ],
            [{"index": 3, "codec": "subrip", "language": "cze", "title": "CZ"}],
        )

        result = apply_track_policy(make_config(), item)

        self.assertTrue(result["applied"])
        self.assertEqual(result["expected_audio_stream_count"], 1)
        self.assertEqual(result["map_arguments"], ["-map", "0:0", "-map", "0:1", "-map", "0:3", "-map", "0:t?"])
        audio_decisions = [stream for stream in result["streams"] if stream["type"] == "audio"]
        self.assertEqual(audio_decisions[0]["decision"], "keep")
        self.assertEqual(audio_decisions[1]["decision"], "drop")

    def test_unknown_audio_language_disables_cleanup(self) -> None:
        item = make_item(
            "series",
            [
                {"index": 1, "codec": "aac", "language": "cze", "title": None},
                {"index": 2, "codec": "aac", "language": "und", "title": None},
            ],
        )

        result = apply_track_policy(make_config(), item)

        self.assertFalse(result["applied"])
        self.assertIn("unknown-language audio exists", result["decision_summary"])
        self.assertEqual(result["map_arguments"], ["-map", "0"])

    def test_anime_keeps_english_and_drops_japanese(self) -> None:
        item = make_item(
            "anime",
            [
                {"index": 1, "codec": "aac", "language": "jpn", "title": "Japanese 2.0"},
                {"index": 2, "codec": "aac", "language": "eng", "title": "English Dub 2.0"},
            ],
        )

        result = apply_track_policy(make_config(), item)

        self.assertTrue(result["applied"])
        audio_decisions = {stream["stream_index"]: stream for stream in result["streams"] if stream["type"] == "audio"}
        self.assertEqual(audio_decisions[1]["decision"], "drop")
        self.assertEqual(audio_decisions[2]["decision"], "keep")


if __name__ == "__main__":
    unittest.main()
