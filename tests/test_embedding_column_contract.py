# tests/test_embedding_column_contract.py
"""Embedding columns must hold a full Gemini vector.

gemini-embedding-001 is 3072-dim. papers_chunks stores it as a binary
VECTOR(RAG_EMBED_DIM) column (MySQL 9); attachment_chunks still stores
JSON text, which overflows MySQL TEXT's 64KB cap and must therefore
compile to MEDIUMTEXT (16MB).
"""
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module
from config import RAG_EMBED_DIM
from sqlalchemy import select
from sqlalchemy.dialects import mysql, sqlite


class EmbeddingColumnWidth(unittest.TestCase):
    def test_paper_chunk_identity_includes_paper_and_revision(self):
        columns = app_module.PaperChunkModel.__table__.c
        self.assertIn("paper_id", columns)
        self.assertIn("revision_number", columns)

    def test_paper_chunk_embedding_is_vector_of_configured_dim(self):
        col_type = app_module.PaperChunkModel.__table__.c.embedding_vec.type
        self.assertEqual(
            col_type.compile(dialect=mysql.dialect()),
            f"VECTOR({RAG_EMBED_DIM})",
        )

    def test_mysql_vector_reads_compile_to_binary_casts(self):
        statement = select(app_module.PaperChunkModel.embedding_vec)

        compiled = str(statement.compile(dialect=mysql.dialect()))

        self.assertIn(
            "CAST(papers_chunks.embedding_vec AS BINARY)",
            compiled,
        )

    def test_sqlite_vector_reads_keep_the_original_expression(self):
        statement = select(app_module.PaperChunkModel.embedding_vec)

        compiled = str(statement.compile(dialect=sqlite.dialect()))

        self.assertIn("papers_chunks.embedding_vec", compiled)
        self.assertNotIn("CAST(", compiled.upper())

    def test_attachment_chunk_embedding_not_plain_text(self):
        col_type = app_module.AttachmentChunkModel.__table__.c.embedding.type
        self.assertEqual(col_type.compile(dialect=mysql.dialect()), "MEDIUMTEXT")


if __name__ == "__main__":
    unittest.main()
