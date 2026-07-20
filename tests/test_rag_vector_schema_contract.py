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
        self.assertIn("paper_id", src)
        self.assertIn("revision_number", src)
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


class StoreLayerContract(unittest.TestCase):
    def test_store_replace_writes_vector_and_bumps_stamp(self):
        src = support.source_of("_rag_store_replace")
        self.assertIn("embedding_vec", src)
        self.assertIn("bump_chunks_version(db)", src)
        self.assertNotIn("embedding=", src.replace("embedding_vec=", ""))

    def test_store_delete_bumps_stamp(self):
        src = support.source_of("_rag_store_delete")
        self.assertIn("bump_chunks_version(db)", src)

    def test_configure_rag_wires_stamp_deps(self):
        src = support.source_of("configure_rag")
        for dep in ("store_version=", "store_vectors=", "fetch_chunks=",
                    "indexed_filenames="):
            self.assertIn(dep, src)
        self.assertNotIn("store_all", src)

    def test_forced_grounding_reads_binary_column(self):
        src = support.source_of("_forced_grounding")
        self.assertIn("embedding_vec", src)
        self.assertNotIn("json.loads", src)


if __name__ == "__main__":
    unittest.main()
