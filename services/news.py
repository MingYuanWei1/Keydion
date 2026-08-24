"""News articles: storage and category helpers."""
import json
from datetime import datetime
from html import escape
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

from config import (
    CATEGORIES_JSON,
    NEWS_FIELDS,
    NEWS_CATEGORIES_SAMPLE_JSON,
)
from db import db_session
from models import NewsArticleModel
from services.default_data import load_seeded_json
from services.guides import _sanitize_guide_html


# ==================== NEWS HELPERS ====================

def sanitize_news_body(raw: str) -> str:
    """Sanitize current Quill HTML or legacy block JSON."""
    if not raw:
        return ""
    try:
        blocks = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        cleaned = _sanitize_guide_html(raw)
        soup = BeautifulSoup(cleaned, "html.parser")
        return cleaned if soup.get_text(strip=True) or soup.find("img") else ""
    if not isinstance(blocks, list):
        return _sanitize_guide_html(raw)
    for block in blocks:
        if isinstance(block, dict) and block.get("type") == "text":
            block["content"] = _sanitize_guide_html(block.get("content", ""))
    return json.dumps(blocks, ensure_ascii=False)


def news_body_html(raw: str, fallback_alt: str = "") -> str:
    """Return sanitized HTML for either current Quill or legacy block bodies."""
    if not raw:
        return ""
    try:
        blocks = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        cleaned = _sanitize_guide_html(raw)
        if BeautifulSoup(raw, "html.parser").find() is None:
            return "".join(
                f'<div class="body-paragraph">{escape(line.strip())}</div>'
                for line in raw.splitlines()
                if line.strip()
            )
        return cleaned
    if not isinstance(blocks, list):
        return _sanitize_guide_html(raw)

    parts = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "text":
            content = str(block.get("content") or "")
            if content.strip():
                parts.append(f'<div class="body-paragraph">{content}</div>')
        elif block.get("type") == "image" and block.get("url"):
            url = escape(str(block["url"]), quote=True)
            caption = escape(str(block.get("caption") or ""))
            alt = caption or escape(fallback_alt, quote=True)
            parts.append(
                f'<div class="news-figure"><img src="{url}" alt="{alt}">'
                + (f'<div class="news-figure-caption">{caption}</div>' if caption else "")
                + "</div>"
            )
        elif block.get("type") == "divider":
            parts.append('<hr class="news-divider">')
    return _sanitize_guide_html("".join(parts))


CATEGORY_NAME_MAX_LENGTH = 50
# Characters that can break out of the manager's attribute-context string
# building (security finding: stored XSS via category names). Names carrying
# them are rejected at add/rename instead of relying on client escaping alone.
_CATEGORY_NAME_FORBIDDEN = frozenset('<>"\'')


def category_name_validation_error(name: str) -> Optional[str]:
    """Why a news category name is unsafe to persist, or None when it is safe."""
    if not isinstance(name, str) or not name:
        return "required"
    if len(name) > CATEGORY_NAME_MAX_LENGTH:
        return "too_long"
    if any(character in _CATEGORY_NAME_FORBIDDEN for character in name):
        return "unsafe_characters"
    if any(ord(character) < 32 for character in name):
        return "unsafe_characters"
    return None


def load_categories() -> list:
    """Load categories from JSON file, seeding from defaults if needed."""
    return load_seeded_json(CATEGORIES_JSON, NEWS_CATEGORIES_SAMPLE_JSON)


def save_categories(cats: list) -> None:
    CATEGORIES_JSON.parent.mkdir(parents=True, exist_ok=True)
    CATEGORIES_JSON.write_text(json.dumps(cats, ensure_ascii=False, indent=2), encoding="utf-8")


def load_news_articles(status: Optional[str] = None) -> List[Dict[str, str]]:
    """Return news articles sorted by published_at descending.

    When status is provided, only articles with that status are returned.
    With status=None (default), all articles are returned regardless of status.
    """
    with db_session() as db:
        query = db.query(NewsArticleModel)
        if status:
            query = query.filter_by(status=status)
        articles = query.all()
        rows = [{field: (getattr(a, field) or "") for field in NEWS_FIELDS} for a in articles]
        rows.sort(key=lambda r: r.get("published_at", ""), reverse=True)
        return rows


def get_news_article(article_id: str) -> Optional[Dict[str, str]]:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            return {field: (getattr(article, field) or "") for field in NEWS_FIELDS}
        return None


def save_news_article(article: Dict[str, str]) -> None:
    with db_session() as db:
        db.add(NewsArticleModel(**{field: article.get(field, "") for field in NEWS_FIELDS}))
        db.commit()


def update_news_article(article_id: str, data: Dict[str, str]) -> bool:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            for field in NEWS_FIELDS:
                if field in ("id", "published_at"):
                    continue
                if field in data:
                    setattr(article, field, data[field])
            # When transitioning from draft to published, refresh published_at.
            if data.get("status") == "published" and (article.published_at is None or article.published_at == ""):
                article.published_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
            db.commit()
            return True
        return False


def delete_news_article(article_id: str) -> bool:
    with db_session() as db:
        article = db.query(NewsArticleModel).filter_by(id=article_id).first()
        if article:
            db.delete(article)
            db.commit()
            return True
        return False
