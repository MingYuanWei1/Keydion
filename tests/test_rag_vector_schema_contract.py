# tests/test_rag_vector_schema_contract.py
"""Contracts for the MySQL 9 VECTOR schema and the version-stamp tables.

Pure AST tests — no DB connection needed.
"""
import ast
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import support


class VectorTypeContract(unittest.TestCase):
    def test_vector_type_emits_vector_ddl(self):
        node, text = support.find_class("VectorType")
        src = ast.get_source_segment(text, node)
        self.assertIn("VECTOR(", src)
        self.assertIn("STRING_TO_VECTOR", src)


class PaperChunkModelContract(unittest.TestCase):
    def test_uses_binary_vector_column(self):
        node, text = support.find_class("PaperChunkModel")
        src = ast.get_source_segment(text, node)
        self.assertIn("embedding_vec", src)
        self.assertIn("VectorType", src)

    def test_legacy_json_column_is_unmapped(self):
        # The MEDIUMTEXT JSON column must no longer be an ORM attribute, so that
        # full-entity queries (e.g. _lib_full_text) survive the column drop.
        node, text = support.find_class("PaperChunkModel")
        src = ast.get_source_segment(text, node)
        self.assertNotRegex(src, r"\n\s+embedding\s*=")


class RagIndexMetaContract(unittest.TestCase):
    def test_meta_model_exists(self):
        node, text = support.find_class("RagIndexMetaModel")
        src = ast.get_source_segment(text, node)
        self.assertIn("rag_index_meta", src)

    def test_init_db_adds_vector_column(self):
        src = support.source_of("init_db")
        self.assertIn("ADD COLUMN embedding_vec VECTOR(", src)


if __name__ == "__main__":
    unittest.main()
