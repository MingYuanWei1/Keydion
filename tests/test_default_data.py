import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services import guides, news, papers


ROOT = Path(__file__).resolve().parents[1]


class RuntimeDefaultDataTest(unittest.TestCase):
    def test_news_sample_preserves_the_shipped_defaults(self):
        self.assertEqual(
            json.loads((ROOT / "data" / "news_categories.sample.json").read_text(encoding="utf-8")),
            [
                "活动回顾", "期刊发布", "讲座预告", "成果展示",
                "公告通知", "学术动态", "社团新闻", "其他",
            ],
        )

    def test_missing_runtime_files_are_seeded_from_tracked_samples(self):
        cases = (
            (news, "CATEGORIES_JSON", "NEWS_CATEGORIES_SAMPLE_JSON", news.load_categories),
            (guides, "GUIDE_CATEGORIES_JSON", "GUIDE_CATEGORIES_SAMPLE_JSON", guides._load_guide_categories),
            (papers, "PAPER_CATEGORIES_JSON", "PAPER_CATEGORIES_SAMPLE_JSON", papers.load_paper_categories),
            (papers, "_EE_SUBJECTS_PATH", "EE_SUBJECTS_SAMPLE_JSON", papers.load_ee_subjects),
            (papers, "_IA_SUBJECTS_PATH", "IA_SUBJECTS_SAMPLE_JSON", papers.load_ia_subjects),
        )
        with tempfile.TemporaryDirectory() as directory:
            for index, (module, runtime_name, sample_name, loader) in enumerate(cases):
                runtime = Path(directory) / f"runtime-{index}.json"
                sample = ROOT / "data" / getattr(module, sample_name).name
                with mock.patch.object(module, runtime_name, runtime), mock.patch.object(
                    module, sample_name, sample
                ):
                    self.assertEqual(loader(), json.loads(sample.read_text(encoding="utf-8")))
                    self.assertEqual(runtime.read_bytes(), sample.read_bytes())

    def test_existing_runtime_file_is_preserved_byte_for_byte(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / "news-categories.json"
            raw = '[\n  "Operator category"\n]\n'
            runtime.write_text(raw, encoding="utf-8")
            with mock.patch.object(news, "CATEGORIES_JSON", runtime):
                self.assertEqual(news.load_categories(), ["Operator category"])
            self.assertEqual(runtime.read_text(encoding="utf-8"), raw)

    def test_malformed_runtime_file_resets_to_tracked_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / "news-categories.json"
            runtime.write_text("not json", encoding="utf-8")
            sample = ROOT / "data" / "news_categories.sample.json"
            with mock.patch.object(news, "CATEGORIES_JSON", runtime), mock.patch.object(
                news, "NEWS_CATEGORIES_SAMPLE_JSON", sample
            ):
                self.assertEqual(news.load_categories(), json.loads(sample.read_text(encoding="utf-8")))
            self.assertEqual(json.loads(runtime.read_text(encoding="utf-8")), json.loads(sample.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
