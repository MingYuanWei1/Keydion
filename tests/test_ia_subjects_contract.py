"""Contract: IA-subjects reconcile is a pure diff carrying per-subject criteria;
cascade helpers update/count papers by exact subject match; the save route wires
guard + cascade. No interdisciplinary handling for IA."""
import json
import unittest
from unittest import mock

import services.papers as sp
from tests.support import source_of


def _old_tree():
    return {
        "groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "criteria": [{"name": "C1", "max": 6}]},
                {"name": "Beta", "criteria": [{"name": "C1", "max": 4}, {"name": "C2", "max": 2}]}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "criteria": [{"name": "C1", "max": 3}]}]},
        ],
    }


class ReconcileTest(unittest.TestCase):
    def test_rename_detected_criteria_carried_through(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha2", "original_name": "Alpha",
                 "criteria": [{"name": "C1", "max": 6}]},
                {"name": "Beta", "original_name": "Beta",
                 "criteria": [{"name": "C1", "max": 4}, {"name": "C2", "max": 2}]}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma",
                 "criteria": [{"name": "C1", "max": 3}]}]},
        ]}
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertEqual(r["errors"], [])
        self.assertIn(("Alpha", "Alpha2"), r["renames"])
        self.assertEqual(r["deletions"], [])
        # criteria objects carried into the output tree
        g1 = r["tree"]["groups"][0]["subjects"]
        self.assertEqual(g1[0], {"name": "Alpha2", "criteria": [{"name": "C1", "max": 6}]})
        self.assertEqual(g1[1]["criteria"], [{"name": "C1", "max": 4}, {"name": "C2", "max": 2}])

    def test_no_interdisciplinary_key_in_output(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": "Alpha", "criteria": [{"name": "C1", "max": 6}]},
                {"name": "Beta", "original_name": "Beta", "criteria": [{"name": "C1", "max": 4}, {"name": "C2", "max": 2}]}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "criteria": [{"name": "C1", "max": 3}]}]},
        ]}
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertNotIn("interdisciplinary_subjects", r["tree"])

    def test_deletion_detected(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": "Alpha", "criteria": [{"name": "C1", "max": 6}]}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "criteria": [{"name": "C1", "max": 3}]}]},
        ]}
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertEqual(r["deletions"], ["Beta"])
        self.assertEqual(r["renames"], [])

    def test_new_subject_and_group_id_assigned(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": "Alpha", "criteria": [{"name": "C1", "max": 6}]},
                {"name": "Beta", "original_name": "Beta", "criteria": [{"name": "C1", "max": 4}, {"name": "C2", "max": 2}]},
                {"name": "Delta", "original_name": None, "criteria": [{"name": "C1", "max": 5}]}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "criteria": [{"name": "C1", "max": 3}]}]},
            {"id": None, "name": "G3", "subjects": []},
        ]}
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertEqual(r["errors"], [])
        self.assertEqual(r["deletions"], [])
        self.assertEqual([g["id"] for g in r["tree"]["groups"]], [1, 2, 3])

    def test_validation_empty_group_and_duplicate_subject(self):
        payload = {"groups": [{"id": 1, "name": "", "subjects": [
            {"name": "X", "original_name": None, "criteria": [{"name": "C1", "max": 1}]},
            {"name": "X", "original_name": None, "criteria": [{"name": "C1", "max": 1}]}]}]}
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertGreaterEqual(len(r["errors"]), 2)

    def test_validation_criteria_empty_name_and_bad_max(self):
        payload = {"groups": [{"id": 1, "name": "G1", "subjects": [
            {"name": "Alpha", "original_name": "Alpha", "criteria": [
                {"name": "  ", "max": 6},        # empty after strip
                {"name": "C2", "max": 0},        # max < 1
                {"name": "C3", "max": "x"}]}]}]} # max not an int
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertGreaterEqual(len(r["errors"]), 3)

    def test_new_subject_reusing_name_not_a_deletion(self):
        payload = {"groups": [
            {"id": 1, "name": "G1", "subjects": [
                {"name": "Alpha", "original_name": None, "criteria": [{"name": "C1", "max": 6}]},
                {"name": "Beta", "original_name": "Beta", "criteria": [{"name": "C1", "max": 4}, {"name": "C2", "max": 2}]}]},
            {"id": 2, "name": "G2", "subjects": [
                {"name": "Gamma", "original_name": "Gamma", "criteria": [{"name": "C1", "max": 3}]}]},
        ]}
        r = sp.reconcile_ia_subjects(_old_tree(), payload)
        self.assertEqual(r["deletions"], [])


class SubjectListTest(unittest.TestCase):
    def test_flat_sorted_list_reads_name_field(self):
        with mock.patch.object(sp, "load_ia_subjects", return_value=_old_tree()):
            self.assertEqual(sp._get_ia_subjects_list(), ["Alpha", "Beta", "Gamma"])


def _rows():
    return [
        {"filename": "a.pdf", "ia_data": json.dumps({"is_ia": True, "subject": "Alpha"})},
        {"filename": "b.pdf", "ia_data": json.dumps({"is_ia": True, "subject": "Beta"})},
        {"filename": "c.pdf", "ia_data": json.dumps({"is_ia": True, "subject": "Alpha"})},
        {"filename": "d.pdf", "ia_data": ""},
    ]


class CascadeTest(unittest.TestCase):
    def test_count_exact_match(self):
        with mock.patch.object(sp, "load_paper_metadata", return_value=_rows()):
            self.assertEqual(sp.count_papers_using_ia_subject("Alpha"), 2)
            self.assertEqual(sp.count_papers_using_ia_subject("Beta"), 1)
            self.assertEqual(sp.count_papers_using_ia_subject("Zeta"), 0)

    def test_rename_updates_subject(self):
        rows = _rows()
        captured = {}
        with mock.patch.object(sp, "load_paper_metadata", return_value=rows), \
             mock.patch.object(sp, "save_paper_metadata", side_effect=lambda r: captured.update(rows=r)):
            n = sp.rename_ia_subject_in_papers("Alpha", "Omega")
        self.assertEqual(n, 2)
        self.assertEqual(json.loads(captured["rows"][0]["ia_data"])["subject"], "Omega")
        self.assertEqual(json.loads(captured["rows"][2]["ia_data"])["subject"], "Omega")

    def test_noop_rename_does_not_save(self):
        called = {"save": False}
        with mock.patch.object(sp, "load_paper_metadata", return_value=_rows()), \
             mock.patch.object(sp, "save_paper_metadata", side_effect=lambda r: called.update(save=True)):
            n = sp.rename_ia_subject_in_papers("Alpha", "Alpha")
        self.assertEqual(n, 0)
        self.assertFalse(called["save"])


class RouteWiringTest(unittest.TestCase):
    def test_save_route_guards_and_cascades(self):
        src = source_of("admin_ia_subjects_save")
        self.assertIn("reconcile_ia_subjects", src)
        self.assertIn("count_papers_using_ia_subject", src)
        self.assertIn("rename_ia_subject_in_papers", src)
        self.assertIn("409", src)


if __name__ == "__main__":
    unittest.main()
