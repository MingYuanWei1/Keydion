"""Contract: EE-subjects reconcile is a pure diff; cascade helpers update/
count papers by exact subject match; the save route wires guard + cascade."""
import json
import unittest
from unittest import mock

import services.papers as sp


def _old_tree():
    return {
        "groups": [
            {"id": 1, "name": "G1", "subjects": ["Alpha", "Beta"]},
            {"id": 2, "name": "G2", "subjects": ["Gamma"]},
        ],
        "interdisciplinary_subjects": ["Beta"],
    }


class ReconcileTest(unittest.TestCase):
    def test_rename_detected_interdisciplinary_rebuilt(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha2", "original_name": "Alpha", "interdisciplinary": False},
                {"name": "Beta", "original_name": "Beta", "interdisciplinary": True}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "interdisciplinary": False}]},
        ]}
        r = sp.reconcile_ee_subjects(_old_tree(), payload)
        self.assertEqual(r["errors"], [])
        self.assertIn(("Alpha", "Alpha2"), r["renames"])
        self.assertEqual(r["deletions"], [])
        self.assertEqual(r["tree"]["interdisciplinary_subjects"], ["Beta"])
        self.assertEqual(r["tree"]["groups"][0]["subjects"], ["Alpha2", "Beta"])

    def test_deletion_detected(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": "Alpha", "interdisciplinary": False}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "interdisciplinary": False}]},
        ]}
        r = sp.reconcile_ee_subjects(_old_tree(), payload)
        self.assertEqual(r["deletions"], ["Beta"])
        self.assertEqual(r["renames"], [])

    def test_new_subject_and_group_id_assigned(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": "Alpha", "interdisciplinary": False},
                {"name": "Beta", "original_name": "Beta", "interdisciplinary": True},
                {"name": "Delta", "original_name": None, "interdisciplinary": False}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "interdisciplinary": False}]},
            {"id": None, "name": "G3", "subjects": []},
        ]}
        r = sp.reconcile_ee_subjects(_old_tree(), payload)
        self.assertEqual(r["errors"], [])
        self.assertEqual(r["deletions"], [])
        self.assertEqual([g["id"] for g in r["tree"]["groups"]], [1, 2, 3])

    def test_validation_empty_and_duplicate(self):
        payload = {"groups": [{"id": 1, "name": "", "subjects": [
            {"name": "X", "original_name": None, "interdisciplinary": False},
            {"name": "X", "original_name": None, "interdisciplinary": False}]}]}
        r = sp.reconcile_ee_subjects(_old_tree(), payload)
        self.assertGreaterEqual(len(r["errors"]), 2)

    def test_new_subject_reusing_name_not_a_deletion(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": None, "interdisciplinary": False},
                {"name": "Beta", "original_name": "Beta", "interdisciplinary": True}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "interdisciplinary": False}]},
        ]}
        r = sp.reconcile_ee_subjects(_old_tree(), payload)
        self.assertEqual(r["deletions"], [])


def _rows():
    return [
        {"filename": "a.pdf", "ib_ee_data": json.dumps({"is_ib_ee": True, "core_subject": "Alpha"})},
        {"filename": "b.pdf", "ib_ee_data": json.dumps({"is_ib_ee": True, "core_subject": "Beta", "interdisciplinary_subject": "Alpha"})},
        {"filename": "c.pdf", "ib_ee_data": ""},
    ]


class CascadeTest(unittest.TestCase):
    def test_count_exact_match_both_fields(self):
        with mock.patch.object(sp, "load_paper_metadata", return_value=_rows()):
            self.assertEqual(sp.count_papers_using_ee_subject("Alpha"), 2)
            self.assertEqual(sp.count_papers_using_ee_subject("Beta"), 1)
            self.assertEqual(sp.count_papers_using_ee_subject("Zeta"), 0)

    def test_legacy_rename_writer_is_removed(self):
        self.assertFalse(hasattr(sp, "rename_ee_subject_in_papers"))
        self.assertFalse(hasattr(sp, "save_paper_metadata"))


if __name__ == "__main__":
    unittest.main()
