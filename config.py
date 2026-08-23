"""Configuration: .env loading + all application constants.

MUST be imported before any module reads os.environ (it loads .env/.env.prod).
"""
from __future__ import annotations
import os
from datetime import timedelta
from enum import IntEnum
from pathlib import Path
from dotenv import load_dotenv, dotenv_values

# Prefer .env.prod (production) when present; fall back to .env only if it is absent.
_ENV_DIR = os.path.dirname(os.path.abspath(__file__))
_ENV_FILE = os.path.join(_ENV_DIR, ".env.prod")
if not os.path.exists(_ENV_FILE):
    _ENV_FILE = os.path.join(_ENV_DIR, ".env")
load_dotenv(_ENV_FILE)
# File-wins semantics for the admin AI-models panel's keys ONLY: the panel
# edits LLM_*/WEB_SEARCH_* in this file, and without re-applying them over the
# process environment, values snapshotted by systemd's EnvironmentFile= at
# master start would mask every panel edit even after a worker restart.
# Everything else (secrets, DB URL, ...) keeps normal precedence, so external
# environment overrides still win outside the panel's scope.
PANEL_ENV_KEYS = (
    "LLM_API_KEY", "LLM_BASE_URL", "LLM_DEFAULT_FLASH", "LLM_DEFAULT_THINK",
    "LLM_EMBED_API_KEY", "LLM_EMBED_BASE_URL", "LLM_EMBED_MODEL", "LLM_EMBED_BATCH",
    "LLM_VISION", "LLM_VISION_API_KEY", "LLM_VISION_BASE_URL",
    "WEB_SEARCH_PROVIDER", "WEB_SEARCH_API_KEY",
)
_panel_env = dotenv_values(_ENV_FILE)
for _panel_key in PANEL_ENV_KEYS:
    if _panel_key in _panel_env:
        os.environ[_panel_key] = _panel_env[_panel_key] or ""

from flask_babel import lazy_gettext as _l  # noqa: E402  (ROLE_OPTIONS etc. use _l)

# --- constants moved verbatim from app.py below this line ---

BASE_DIR = Path(__file__).resolve().parent


def _absolute_configured_path(value: str | os.PathLike[str]) -> Path:
    """Make configured paths absolute without hiding a symlink from validation."""
    return Path(os.path.abspath(os.fspath(value)))


def _configured_path_value(name: str, default: Path) -> str | Path:
    """Treat documented blank path overrides as requests for their defaults."""
    configured = os.environ.get(name)
    if configured is None or not configured.strip():
        return default
    return configured


DATA_DIR = Path(
    _configured_path_value("PAPERQUERY_DATA_DIR", BASE_DIR / "data")
).resolve()
PAPERS_DIR = _absolute_configured_path(
    _configured_path_value("PAPERQUERY_UPLOAD_DIR", BASE_DIR / "papers")
)
LOCAL_USER_FIELDS = ["username", "password", "registration_date", "expiry_date", "role", "email", "first_name", "last_name", "school"]
NEWS_FIELDS = ["id", "title", "category", "abstract", "body", "author", "image_url", "published_at", "status"]
GUIDE_FIELDS = [
    "id", "slug", "category", "sort_order", "published",
    "title_en", "title_zh", "summary_en", "summary_zh",
    "body_en", "body_zh", "created_at", "updated_at",
]
GUIDE_CATEGORIES_JSON = DATA_DIR / "guide_categories.json"
_DEFAULT_GUIDE_CATEGORIES = [
    "Getting Started", "Account", "Submissions", "News", "Other",
]
_DEFAULT_NEWS_CATEGORIES = [
    "活动回顾", "期刊发布", "讲座预告", "成果展示",
    "公告通知", "学术动态", "社团新闻", "其他",
]
CATEGORIES_JSON = DATA_DIR / "news_categories.json"
JOURNALS_JSON = DATA_DIR / "paper_journals.json"
_DEFAULT_PAPER_CATEGORIES = ["literature", "natural-science", "social-science", "humanities"]
PENDING_PAPERS_DIR = DATA_DIR / "pending_papers"


# ---- IB EE Subject helpers ----

_EE_SUBJECTS_PATH = DATA_DIR / "ee_subjects.json"

_EE_SUBJECTS_DEFAULT = {
    "groups": [
        {
            "id": 1,
            "name": "Group 1: Studies in Language and Literature",
            "subjects": [
                "Language A: Literature",
                "Language A: Language and Literature",
                "Literature and Performance"
            ]
        },
        {
            "id": 2,
            "name": "Group 2: Language Acquisition",
            "subjects": [
                "Language B",
                "Language ab initio",
                "Classical Languages"
            ]
        },
        {
            "id": 3,
            "name": "Group 3: Individuals and Societies",
            "subjects": [
                "Business Management",
                "Economics",
                "Geography",
                "Global Politics",
                "History",
                "Information Technology in a Global Society",
                "Philosophy",
                "Psychology",
                "Social and Cultural Anthropology",
                "World Religions"
            ]
        },
        {
            "id": 4,
            "name": "Group 4: Sciences",
            "subjects": [
                "Biology",
                "Chemistry",
                "Computer Science",
                "Design Technology",
                "Environmental Systems and Societies",
                "Physics",
                "Sports, Exercise and Health Science"
            ]
        },
        {
            "id": 5,
            "name": "Group 5: Mathematics",
            "subjects": [
                "Mathematics: Analysis and Approaches",
                "Mathematics: Applications and Interpretation"
            ]
        },
        {
            "id": 6,
            "name": "Group 6: The Arts",
            "subjects": [
                "Dance",
                "Film",
                "Music",
                "Theatre",
                "Visual Arts"
            ]
        }
    ],
    "interdisciplinary_subjects": [
        "Environmental Systems and Societies",
        "Literature and Performance",
        "World Studies"
    ]
}

# ---- IB IA (Internal Assessment) Subject helpers ----
# Mirrors the EE group/subject NAMES, but each subject carries its own IA rubric.
# Rubrics pre-filled ONLY where the IB internal-assessment criteria are well
# established; every other subject ships with an empty criteria list for an
# admin to fill via the manage page.  # verify against current IB subject guides
_IA_SUBJECTS_PATH = DATA_DIR / "ia_subjects.json"

# Group 4 Sciences individual-investigation rubric (24 marks).
_IA_SCIENCE_CRITERIA = [
    {"name": "Personal engagement", "max": 2},
    {"name": "Exploration", "max": 6},
    {"name": "Analysis", "max": 6},
    {"name": "Evaluation", "max": 6},
    {"name": "Communication", "max": 4},
]
# Mathematics exploration rubric (20 marks).
_IA_MATH_CRITERIA = [
    {"name": "Presentation", "max": 4},
    {"name": "Mathematical communication", "max": 4},
    {"name": "Personal engagement", "max": 3},
    {"name": "Reflection", "max": 3},
    {"name": "Use of mathematics", "max": 6},
]

_IA_SUBJECTS_DEFAULT = {
    "groups": [
        {
            "id": 1,
            "name": "Group 1: Studies in Language and Literature",
            "subjects": [
                {"name": "Language A: Literature", "criteria": []},
                {"name": "Language A: Language and Literature", "criteria": []},
                {"name": "Literature and Performance", "criteria": []},
            ],
        },
        {
            "id": 2,
            "name": "Group 2: Language Acquisition",
            "subjects": [
                {"name": "Language B", "criteria": []},
                {"name": "Language ab initio", "criteria": []},
                {"name": "Classical Languages", "criteria": []},
            ],
        },
        {
            "id": 3,
            "name": "Group 3: Individuals and Societies",
            "subjects": [
                {"name": "Business Management", "criteria": []},
                {"name": "Economics", "criteria": []},
                {"name": "Geography", "criteria": []},
                {"name": "Global Politics", "criteria": []},
                {"name": "History", "criteria": []},
                {"name": "Information Technology in a Global Society", "criteria": []},
                {"name": "Philosophy", "criteria": []},
                {"name": "Psychology", "criteria": []},
                {"name": "Social and Cultural Anthropology", "criteria": []},
                {"name": "World Religions", "criteria": []},
            ],
        },
        {
            "id": 4,
            "name": "Group 4: Sciences",
            "subjects": [
                {"name": "Biology", "criteria": list(_IA_SCIENCE_CRITERIA)},
                {"name": "Chemistry", "criteria": list(_IA_SCIENCE_CRITERIA)},
                {"name": "Computer Science", "criteria": list(_IA_SCIENCE_CRITERIA)},
                {"name": "Design Technology", "criteria": list(_IA_SCIENCE_CRITERIA)},
                {"name": "Environmental Systems and Societies", "criteria": list(_IA_SCIENCE_CRITERIA)},
                {"name": "Physics", "criteria": list(_IA_SCIENCE_CRITERIA)},
                {"name": "Sports, Exercise and Health Science", "criteria": list(_IA_SCIENCE_CRITERIA)},
            ],
        },
        {
            "id": 5,
            "name": "Group 5: Mathematics",
            "subjects": [
                {"name": "Mathematics: Analysis and Approaches", "criteria": list(_IA_MATH_CRITERIA)},
                {"name": "Mathematics: Applications and Interpretation", "criteria": list(_IA_MATH_CRITERIA)},
            ],
        },
        {
            "id": 6,
            "name": "Group 6: The Arts",
            "subjects": [
                {"name": "Dance", "criteria": []},
                {"name": "Film", "criteria": []},
                {"name": "Music", "criteria": []},
                {"name": "Theatre", "criteria": []},
                {"name": "Visual Arts", "criteria": []},
            ],
        },
    ],
}

JOURNAL_COVERS_DIR = BASE_DIR / "static" / "uploads" / "journal_covers"
ALLOWED_EXTENSIONS = {"pdf"}
ALLOWED_IMAGE_EXTENSIONS = {"png", "jpg", "jpeg", "gif", "webp"}
NEWS_IMAGES_DIR = BASE_DIR / "static" / "uploads" / "news"
GUIDE_IMAGES_DIR = BASE_DIR / "static" / "uploads" / "guides"
GUIDE_IMAGE_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
RESOURCES_DIR = Path(
    _configured_path_value("PAPERQUERY_RESOURCES_DIR", BASE_DIR / "resource_files")
).resolve()
# Resource library upload allowlist: extension -> MIME type.
RESOURCE_ALLOWED_EXTENSIONS = {
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "ppt": "application/vnd.ms-powerpoint",
    "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    "xls": "application/vnd.ms-excel",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "png": "image/png",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "gif": "image/gif",
    "webp": "image/webp",
}
# MIME types the browser can render inline (everything else is download-only).
PREVIEWABLE_MIMES = {"application/pdf", "image/png", "image/jpeg", "image/gif", "image/webp"}
RESOURCE_MAX_BYTES = int(os.environ.get("PAPERQUERY_RESOURCE_MAX_MB", "50")) * 1024 * 1024
MAX_SEARCH_RESULTS = 20
MIN_SEMANTIC_QUERY_LEN = 2   # skip embedding for 1-char queries (idea #4)
# Public /search admission control (security finding: anonymous queries drove
# corpus-wide work + paid embeddings with no budget). Query work is throttled
# per account/IP and over-long queries are rejected before any corpus work.
MAX_SEARCH_QUERY_CHARS = 200
SEARCH_RATE_LIMIT = 30       # query searches per window
SEARCH_RATE_WINDOW = 60      # seconds
# Restore duplicates whole immutable PDFs server-side; throttle it per user so
# tiny repeated requests cannot churn storage/indexing (see publishing's
# MAX_REVISIONS_PER_PAPER for the hard cap).
RESTORE_RATE_LIMIT = 10      # restores per window
RESTORE_RATE_WINDOW = 600    # seconds
# Untrusted PDF structural budgets (security finding: synchronous parsing
# without structural limits). Intake and interactive parse paths reject a PDF
# whose page count exceeds this before any rewrite/extraction work. Real
# EE/IA/CP/research papers are far shorter; overridable for odd corpora.
MAX_PDF_PAGES = int(os.environ.get("PAPERQUERY_MAX_PDF_PAGES", "1000"))
# Cumulative Reader-intake storage budgets (security finding: uploads
# allocated unbounded persistent storage). Draft + pending rows share the
# count cap; the byte cap covers the pending PDFs one submitter holds.
MAX_ACTIVE_SUBMISSIONS_PER_USER = 25
MAX_PENDING_BYTES_PER_USER = 200 * 1024 * 1024
# News inline/cover images (security finding: unowned assets without quota).
NEWS_IMAGE_MAX_BYTES = 5 * 1024 * 1024              # one image
NEWS_IMAGES_MAX_FILES = 2000                        # directory-wide count
NEWS_IMAGES_MAX_TOTAL_BYTES = 1024 * 1024 * 1024    # directory-wide bytes
# Keydion AI budgets (security finding: no owner/conversation/turn-wide
# limits on history, sources, tool output, or conversation rows).
MAX_CONVERSATIONS_PER_OWNER = 100
MAX_ASK_HISTORY_MESSAGES = 40          # prior messages resent to the model
MAX_ASK_HISTORY_MESSAGE_CHARS = 8000   # per-history-message content cap
MAX_FORCED_PAPERS_PER_TURN = 8         # cited papers reread into context
# Microsoft first-password enrollment requires a fresh Microsoft login within
# this window (security finding: a stolen SSO session could enroll a durable
# password without reauthentication).
MS_STEP_UP_WINDOW_SECONDS = 600
# VECTOR(n) column dimension for RAG chunk embeddings. Must match the embedding
# model's output (gemini-embedding-001: 3072). Changing it requires a column
# migration + full re-index, not just an env flip.
RAG_EMBED_DIM = int(os.environ.get("RAG_EMBED_DIM", "3072"))
PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS = int(
    os.environ.get("PAPERQUERY_PUBLISHING_INLINE_INDEX_TIMEOUT_SECONDS", "45")
)
PUBLISHING_WORKER_POLL_SECONDS = int(
    os.environ.get("PAPERQUERY_PUBLISHING_WORKER_POLL_SECONDS", "5")
)
PUBLISHING_JOB_LEASE_SECONDS = int(
    os.environ.get("PAPERQUERY_PUBLISHING_JOB_LEASE_SECONDS", "1800")
)
PUBLISHING_RESERVATION_GRACE_SECONDS = int(
    os.environ.get("PAPERQUERY_PUBLISHING_RESERVATION_GRACE_SECONDS", "3600")
)
PASSWORD_SCHEME = "pbkdf2_sha256"
SUPPORTED_LOCALES = ("en", "zh")
SESSION_TIMEOUT_SECONDS = int(os.environ.get("PAPERQUERY_SESSION_TIMEOUT", "3600"))
OPEN_ACCESS = os.environ.get("PAPERQUERY_OPEN_ACCESS", "0").strip().lower() in ("1", "true", "yes", "on")
SESSION_TIMEOUT = timedelta(seconds=SESSION_TIMEOUT_SECONDS)
REMEMBER_SESSION_LIFETIME = timedelta(days=7)
METADATA_FIELDS = ["filename", "title", "journal", "category", "language", "keywords", "abstract", "author_name", "author_email", "author_school", "published_at", "ib_ee_data", "is_ib_sample", "cp_data", "is_anonymous", "ia_data"]
MS_USER_FIELDS = [
    "ms_id",
    "tenant_id",
    "email",
    "display_name",
    "first_name",
    "last_name",
    "school",
    "grade",
    "role",
    "password",
    "created_at",
    "updated_at",
]
MS_CLIENT_ID = os.environ.get("PAPERQUERY_MS_CLIENT_ID")
MS_CLIENT_SECRET = os.environ.get("PAPERQUERY_MS_CLIENT_SECRET")
MS_REDIRECT_URI = os.environ.get("PAPERQUERY_MS_REDIRECT_URI", "http://127.0.0.1:5000/auth/callback")
MS_AUTHORITY = os.environ.get("PAPERQUERY_MS_AUTHORITY", "https://login.microsoftonline.com/common")
MS_SCOPES = ["User.Read"]
MS_GRAPH_ME_URL = "https://graph.microsoft.com/v1.0/me"
class Role(IntEnum):
    """Canonical authorization levels and public terminology."""

    READER = 1
    CONTRIBUTOR = 2
    CURATOR = 3


ROLE_NAMES = {
    Role.READER: "Reader",
    Role.CONTRIBUTOR: "Contributor",
    Role.CURATOR: "Curator",
}
ROLE_OPTIONS = [(str(int(role)), label) for role, label in ROLE_NAMES.items()]

_MISSING_FIELD_MESSAGES = {
    "title": _l("Please enter the paper title"),
    "category": _l("Please select a subject category"),
    "language": _l("Please select a language"),
    "keywords": _l("Please enter keywords"),
    "abstract": _l("Please enter the abstract"),
    "author_name": _l("Please enter the author name"),
    "author_email": _l("Please enter the contact email"),
    "author_school": _l("Please enter the school name"),
}

# CP Paper (MYP Community Project) constants
IB_EE_CRITERIA_DEFS = [
    ("A", "Framework for the essay", 6),
    ("B", "Knowledge and understanding", 6),
    ("C", "Analysis and line of argument", 6),
    ("D", "Discussion and evaluation", 8),
    ("E", "Reflection", 4),
]
CP_GLOBAL_CONTEXTS = [
    "Identities and Relationships",
    "Orientation in Space and Time",
    "Personal and Cultural Expression",
    "Scientific and Technical Innovation",
    "Globalization and Sustainability",
    "Fairness and Development",
]
CP_ACTION_TYPES = ["Direct Service", "Indirect Service", "Research", "Advocacy"]
CP_CRITERIA_DEFS = [
    ("A", "Investigating", 8),
    ("B", "Planning", 8),
    ("C", "Taking Action", 8),
    ("D", "Reflecting", 8),
]

ROLE_LABELS = {
    Role.READER: _l("Reader - View & Download"),
    Role.CONTRIBUTOR: _l("Contributor - Upload Enabled"),
    Role.CURATOR: _l("Curator - Full Access"),
}
LANGUAGE_NAMES = {
    "en": _l("English"),
    "zh": _l("Chinese"),
}
