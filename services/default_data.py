"""Seed runtime-editable JSON from tracked sample data."""

import json
import shutil


def load_seeded_json(runtime_path, sample_path):
    if runtime_path.exists():
        try:
            return json.loads(runtime_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    data = json.loads(sample_path.read_text(encoding="utf-8"))
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(sample_path, runtime_path)
    return data
