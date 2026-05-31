# tests/test_embedding_column_contract.py
"""Embedding columns must hold a full Gemini vector.

gemini-embedding-001 (3072-dim) serializes to ~68KB of JSON, which overflows
MySQL TEXT's 64KB cap and silently breaks index writes. The columns must
compile to MEDIUMTEXT (16MB) on MySQL.
"""
import os
import unittest

os.environ.setdefault("PAPERQUERY_SECRET", "test-secret")

import app as app_module
from sqlalchemy.dialects import mysql


class EmbeddingColumnWidth(unittest.TestCase):
    def _mysql_type(self, model):
        return model.__table__.c.embedding.type.compile(dialect=mysql.dialect())

    def test_paper_chunk_embedding_not_plain_text(self):
        self.assertEqual(self._mysql_type(app_module.PaperChunkModel), "MEDIUMTEXT")

    def test_attachment_chunk_embedding_not_plain_text(self):
        self.assertEqual(self._mysql_type(app_module.AttachmentChunkModel), "MEDIUMTEXT")


if __name__ == "__main__":
    unittest.main()
