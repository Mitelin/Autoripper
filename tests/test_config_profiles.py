from __future__ import annotations

import tempfile
import textwrap
import unittest
from pathlib import Path

import media_normalizer as mn


class ConfigProfileTests(unittest.TestCase):
  def test_real_gaming_worker_profile_uses_linux_game_mount(self) -> None:
    config = mn.load_config(Path("config.example.yaml"), "gaming-worker")

    self.assertEqual(config["active_profile"], "gaming-worker")
    self.assertEqual(config["output_root"], "/mnt/nas-backup/RIPTEST")
    self.assertEqual(config["shared_state_dir"], "/mnt/nas-backup/RIPTEST/.ripper_state")
    self.assertEqual(config["node"]["id"], "gaming-server")
    self.assertEqual(
      config["node_path_mappings"],
      [{"canonical_prefix": "/mnt/nas/filmy", "local_prefix": "/mnt/nas-backup"}],
    )
    self.assertFalse(config["worker"]["enabled"])
    self.assertFalse(config["worker"]["run_continuously"])
    self.assertIn("anime", config["libraries"])

    def test_profile_extends_and_overrides_nested_values(self) -> None:
        yaml_text = textwrap.dedent(
            """
            output_root: base-output
            libraries:
              anime: [anime-root]
            worker:
              enabled: false
              schedule:
                enabled: false
            profiles:
              base:
                output_root: inherited-output
                worker:
                  enabled: true
                  schedule:
                    enabled: true
                    start: '01:00'
                    end: '02:00'
              child:
                extends: base
                worker:
                  schedule:
                    end: '03:00'
            """
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "config.yaml"
            path.write_text(yaml_text, encoding="utf-8")

            config = mn.load_config(path, "child")

        self.assertEqual(config["output_root"], "inherited-output")
        self.assertTrue(config["worker"]["enabled"])
        self.assertEqual(config["worker"]["schedule"]["start"], "01:00")
        self.assertEqual(config["worker"]["schedule"]["end"], "03:00")

    def test_circular_profile_inheritance_raises(self) -> None:
        yaml_text = textwrap.dedent(
            """
            output_root: base-output
            libraries:
              anime: [anime-root]
            profiles:
              one:
                extends: two
              two:
                extends: one
            """
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "config.yaml"
            path.write_text(yaml_text, encoding="utf-8")

            with self.assertRaisesRegex(ValueError, "Circular config profile inheritance"):
                mn.load_config(path, "one")


if __name__ == "__main__":
    unittest.main()
