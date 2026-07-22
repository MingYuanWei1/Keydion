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

    def test_runtime_schema_check_is_verification_only(self):
        verifier = support.source_of("ensure_schema_current")
        bootstrap = support.source_of("bootstrap_empty_database")
        self.assertNotIn("BASE.metadata.create_all", verifier)
        self.assertIn("get_current_heads", verifier)
        self.assertIn("BASE.metadata.create_all", bootstrap)


class StoreLayerContract(unittest.TestCase):
    def test_vector_snapshot_is_uuid_revision_joined_and_current_visible(self):
        src = support.source_of("_rag_store_vectors")
        for contract in (
            "PaperChunkModel.paper_id",
            "PaperChunkModel.revision_number",
            ".join(PaperMetadataModel",
            'lifecycle_state == "published"',
            "current_revision == PaperChunkModel.revision_number",
        ):
            self.assertIn(contract, src)

    def test_post_score_fetch_repeats_current_visible_join(self):
        src = support.source_of("_rag_fetch_chunks")
        for contract in (
            ".join(PaperMetadataModel",
            'lifecycle_state == "published"',
            "current_revision == PaperChunkModel.revision_number",
            '"paper_id"',
            '"revision_number"',
        ):
            self.assertIn(contract, src)

    def test_semantic_paper_fetch_is_fresh_and_visible(self):
        src = support.source_of("_rag_fetch_papers")
        self.assertIn('lifecycle_state == "published"', src)
        self.assertIn("current_revision.isnot(None)", src)

    def test_configure_rag_wires_stamp_deps(self):
        src = support.source_of("configure_rag")
        for dep in (
            "store_version=",
            "store_vectors=",
            "fetch_chunks=",
            "fetch_papers=",
        ):
            self.assertIn(dep, src)
        for retired in (
            "store_replace=",
            "store_delete=",
            "indexed_filenames=",
            "iter_papers=",
            "paper_text=",
            "paper_meta=",
        ):
            self.assertNotIn(retired, src)

    def test_lifecycle_completion_disables_legacy_filename_write(self):
        src = support.source_of("_complete_index")
        self.assertIn("filename=None", src)

    def test_forced_grounding_reads_binary_column(self):
        src = support.source_of("_forced_grounding")
        self.assertIn("embedding_vec", src)
        self.assertIn("PaperChunkModel.paper_id", src)
        self.assertIn("current_revision == PaperChunkModel.revision_number", src)
        self.assertNotIn("json.loads", src)


if __name__ == "__main__":
    unittest.main()
