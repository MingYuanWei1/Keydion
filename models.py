"""ORM models + init_db() schema setup/migrations."""
from sqlalchemy import (
    Boolean, Column, Date, DateTime, Integer, Unicode, UnicodeText,
    create_engine, func,
)
from sqlalchemy.dialects.mysql import MEDIUMTEXT
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import UserDefinedType

import db
from db import BASE
from config import RAG_EMBED_DIM


class LocalUser(BASE):
    __tablename__ = "local_users"
    username = Column(Unicode(255), primary_key=True)
    password = Column(Unicode(255), nullable=False)
    registration_date = Column(Date)
    expiry_date = Column(Date)
    role = Column(Unicode(10), nullable=False)
    email = Column(Unicode(255))
    first_name = Column(Unicode(255))
    last_name = Column(Unicode(255))
    school = Column(Unicode(255))


class MsUser(BASE):
    __tablename__ = "ms_users"
    ms_id = Column(Unicode(255), primary_key=True)
    tenant_id = Column(Unicode(255))
    email = Column(Unicode(255))
    display_name = Column(Unicode(255))
    first_name = Column(Unicode(255))
    last_name = Column(Unicode(255))
    school = Column(Unicode(255))
    grade = Column(Unicode(255))
    role = Column(Unicode(10))
    password = Column(Unicode(255))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)

class JournalModel(BASE):
    __tablename__ = "journals"
    id = Column(Unicode(255), primary_key=True)
    name = Column(Unicode(255))
    slug = Column(Unicode(255))
    cover_image = Column(Unicode(255))
    introduction = Column(UnicodeText)
    created_at = Column(Unicode(255))

class PaperMetadataModel(BASE):
    __tablename__ = "papers_metadata"
    filename = Column(Unicode(255), primary_key=True)
    title = Column(Unicode(255))
    journal = Column(Unicode(255))
    category = Column(Unicode(255))
    language = Column(Unicode(255))
    keywords = Column(UnicodeText)
    abstract = Column(UnicodeText)
    author_name = Column(Unicode(255))
    author_email = Column(Unicode(255))
    author_school = Column(Unicode(255))
    published_at = Column(Unicode(255))
    ib_ee_data = Column(UnicodeText)
    is_ib_sample = Column(Unicode(10))
    is_anonymous = Column(Unicode(10))
    cp_data = Column(UnicodeText)
    ia_data = Column(UnicodeText)


class VectorType(UserDefinedType):
    """MySQL 9 VECTOR(n) column. Python-side values are JSON-text vectors
    ("[0.1, 0.2, ...]") bound through STRING_TO_VECTOR(); reads come back as
    little-endian IEEE-754 float32 bytes (decode with numpy.frombuffer)."""
    cache_ok = True

    def __init__(self, dim):
        self.dim = dim

    def get_col_spec(self, **kw):
        return f"VECTOR({self.dim})"

    def bind_expression(self, bindvalue):
        return func.STRING_TO_VECTOR(bindvalue)


class PaperChunkModel(BASE):
    __tablename__ = "papers_chunks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    filename = Column(Unicode(255), index=True)
    chunk_index = Column(Integer)
    content = Column(UnicodeText)
    # Binary chunk vector (MySQL 9 VECTOR). The legacy JSON `embedding` column
    # is intentionally unmapped; tools/migrate_chunk_vectors.py backfills it
    # into this column and drops it.
    embedding_vec = Column(VectorType(RAG_EMBED_DIM))
    lang = Column(Unicode(10))


class RagIndexMetaModel(BASE):
    __tablename__ = "rag_index_meta"
    name = Column(Unicode(32), primary_key=True)
    value = Column(Integer, nullable=False, default=0)


class ConversationModel(BASE):
    __tablename__ = "conversations"
    id = Column(Integer, primary_key=True, autoincrement=True)
    serial = Column(Unicode(6), unique=True, index=True)
    owner_key = Column(Unicode(64), index=True)
    title = Column(Unicode(255))
    created_at = Column(Unicode(40))
    updated_at = Column(Unicode(40))


class ChatMessageModel(BASE):
    __tablename__ = "chat_messages"
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, index=True)
    role = Column(Unicode(16))          # "user" | "assistant"
    content = Column(UnicodeText)
    citations = Column(UnicodeText)     # JSON-encoded list
    attachments = Column(UnicodeText)   # JSON-encoded list of filenames (display-only)
    cited_papers = Column(UnicodeText)  # JSON-encoded list of {filename, title} cited from the library
    created_at = Column(Unicode(40))


class AttachmentChunkModel(BASE):
    __tablename__ = "attachment_chunks"
    id = Column(Integer, primary_key=True, autoincrement=True)
    conversation_id = Column(Integer, index=True)
    filename = Column(Unicode(255))
    chunk_index = Column(Integer)
    content = Column(UnicodeText)
    embedding = Column(UnicodeText().with_variant(MEDIUMTEXT(), "mysql"))   # JSON list[float]; MEDIUMTEXT: Gemini vectors exceed TEXT's 64KB
    created_at = Column(Unicode(40))


class NewsArticleModel(BASE):
    __tablename__ = "news_articles"
    id = Column(Unicode(255), primary_key=True)
    title = Column(Unicode(255))
    category = Column(Unicode(255))
    abstract = Column(UnicodeText)
    body = Column(UnicodeText)
    author = Column(Unicode(255))
    image_url = Column(Unicode(255))
    published_at = Column(Unicode(255))
    status = Column(Unicode(20), default="published")


class GuideModel(BASE):
    __tablename__ = "guides"
    id = Column(Integer, primary_key=True, autoincrement=True)
    slug = Column(Unicode(120), unique=True, index=True, nullable=False)
    category = Column(Unicode(80), default="")
    sort_order = Column(Integer, default=100)
    published = Column(Boolean, default=False)
    title_en = Column(Unicode(200), default="")
    title_zh = Column(Unicode(200), default="")
    summary_en = Column(Unicode(300), default="")
    summary_zh = Column(Unicode(300), default="")
    body_en = Column(UnicodeText, default="")
    body_zh = Column(UnicodeText, default="")
    created_at = Column(Unicode(40), default="")
    updated_at = Column(Unicode(40), default="")


class ResourceNode(BASE):
    __tablename__ = "resource_nodes"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, index=True, nullable=True)   # null = top level; no FK (app-managed)
    node_type = Column(Unicode(10))                          # "folder" | "file"
    name = Column(Unicode(255))
    stored_filename = Column(Unicode(255))                   # uuid name on disk (files only)
    original_filename = Column(Unicode(255))                 # original upload name -> download name
    mime_type = Column(Unicode(120))
    size_bytes = Column(Integer)
    description = Column(UnicodeText)
    min_role = Column(Integer, default=1)                    # min role to view THIS node
    created_at = Column(Unicode(40), default="")


class SubmissionModel(BASE):
    __tablename__ = "submissions"
    id = Column(Unicode(255), primary_key=True)
    pdf_filename = Column(Unicode(255))
    pending_filename = Column(Unicode(255))
    title = Column(Unicode(255))
    author_name = Column(Unicode(255))
    author_email = Column(Unicode(255))
    author_school = Column(Unicode(255))
    status = Column(Unicode(50))
    submitted_at = Column(Unicode(255))
    feedback = Column(UnicodeText)
    abstract = Column(UnicodeText)
    keywords = Column(UnicodeText)
    journal = Column(Unicode(255))
    category = Column(Unicode(255))
    language = Column(Unicode(255))
    submitted_by = Column(Unicode(255))
    original_filename = Column(Unicode(255))
    ib_ee_data = Column(UnicodeText)
    is_ib_sample = Column(Unicode(10))
    is_anonymous = Column(Unicode(10))
    cp_data = Column(UnicodeText)
    ia_data = Column(UnicodeText)


class SessionModel(BASE):
    __tablename__ = "sessions"
    username = Column(Unicode(255), primary_key=True)
    token = Column(Unicode(255))
    last_seen = Column(Unicode(255))


def init_db() -> None:
    if db._ENGINE is None:
        db._ENGINE = create_engine(db.DB_URL, pool_pre_ping=True, pool_recycle=3600)
        db._SESSION_LOCAL = sessionmaker(bind=db._ENGINE)
        BASE.metadata.create_all(db._ENGINE)
        # Migrate: add password column to ms_users if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE ms_users ADD COLUMN password VARCHAR(255) NULL"))
                conn.commit()
        except Exception:
            pass  # Column already exists
        # Migrate: add serial column to conversations if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                import secrets
                try:
                    conn.execute(text("ALTER TABLE conversations ADD COLUMN serial VARCHAR(6)"))
                    conn.commit()
                except Exception:
                    pass
                try:
                    conn.execute(text("CREATE UNIQUE INDEX ix_conversations_serial ON conversations(serial)"))
                    conn.commit()
                except Exception:
                    pass
                
                rows = conn.execute(text("SELECT id FROM conversations WHERE serial IS NULL")).fetchall()
                for row in rows:
                    serial = secrets.token_urlsafe(5)[:6]
                    conn.execute(text("UPDATE conversations SET serial = :s WHERE id = :id"), {"s": serial, "id": row[0]})
                conn.commit()
        except Exception:
            pass
        # Migrate: add is_ib_sample column to papers_metadata if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE papers_metadata ADD COLUMN is_ib_sample VARCHAR(10) DEFAULT ''"))
                conn.commit()
        except Exception:
            pass
        # Migrate: widen embedding columns — Gemini vectors (gemini-embedding-001,
        # 3072-dim) serialize to ~68KB JSON, over MySQL TEXT's 64KB cap.
        for _emb_tbl in ("papers_chunks", "attachment_chunks"):
            try:
                with db._ENGINE.connect() as conn:
                    from sqlalchemy import text
                    conn.execute(text(f"ALTER TABLE {_emb_tbl} MODIFY embedding MEDIUMTEXT"))
                    conn.commit()
            except Exception:
                pass
        # Migrate: convert chunk tables to utf8mb4 — PDF-extracted text contains
        # 4-byte chars (e.g. math-italic 𝑅/𝐵 from equations) that 3-byte utf8mb3
        # columns reject with "Incorrect string value". Guarded on the current
        # charset so the (table-rebuilding) CONVERT runs only when needed.
        for _u8_tbl in ("papers_chunks", "attachment_chunks"):
            try:
                with db._ENGINE.connect() as conn:
                    from sqlalchemy import text
                    charset = conn.execute(text(
                        "SELECT character_set_name FROM information_schema.columns "
                        "WHERE table_schema = DATABASE() AND table_name = :t "
                        "AND column_name = 'content'"
                    ), {"t": _u8_tbl}).scalar()
                    if charset and charset != "utf8mb4":
                        conn.execute(text(
                            f"ALTER TABLE {_u8_tbl} CONVERT TO CHARACTER SET utf8mb4 "
                            "COLLATE utf8mb4_unicode_ci"
                        ))
                        conn.commit()
            except Exception:
                pass
        # Migrate: add the binary vector column. Requires MySQL 9.x — the
        # VECTOR type does not exist on 8.x, where this ALTER fails and is
        # swallowed (the app then needs MySQL 9 before RAG features work).
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text(
                    f"ALTER TABLE papers_chunks ADD COLUMN embedding_vec VECTOR({RAG_EMBED_DIM}) NULL"
                ))
                conn.commit()
        except Exception:
            pass  # column already exists (or pre-9.x MySQL)
        # Migrate: add is_ib_sample column to submissions if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE submissions ADD COLUMN is_ib_sample VARCHAR(10) DEFAULT ''"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add is_anonymous column to papers_metadata / submissions
        for _anon_tbl in ("papers_metadata", "submissions"):
            try:
                with db._ENGINE.connect() as conn:
                    from sqlalchemy import text
                    conn.execute(text(f"ALTER TABLE {_anon_tbl} ADD COLUMN is_anonymous VARCHAR(10) DEFAULT ''"))
                    conn.commit()
            except Exception:
                pass
        # Migrate: add cp_data column to papers_metadata if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE papers_metadata ADD COLUMN cp_data TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add cp_data column to submissions if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE submissions ADD COLUMN cp_data TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add ia_data column to papers_metadata if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE papers_metadata ADD COLUMN ia_data TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add ia_data column to submissions if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE submissions ADD COLUMN ia_data TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add status column to news_articles if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE news_articles ADD COLUMN status VARCHAR(20) DEFAULT 'published'"))
                conn.execute(text("UPDATE news_articles SET status = 'published' WHERE status IS NULL OR status = ''"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add attachments column to chat_messages if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN attachments TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add cited_papers column to chat_messages if it doesn't exist
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE chat_messages ADD COLUMN cited_papers TEXT"))
                conn.commit()
        except Exception:
            pass
        # Migrate: add slug column to journals + backfill name-based slugs
        try:
            with db._ENGINE.connect() as conn:
                from sqlalchemy import text
                conn.execute(text("ALTER TABLE journals ADD COLUMN slug VARCHAR(255)"))
                conn.commit()
        except Exception:
            pass
        try:
            from services.journals import ensure_journal_slugs
            ensure_journal_slugs()
        except Exception:
            pass
