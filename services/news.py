"""News articles: storage and category helpers."""
import json
from datetime import datetime
from typing import Dict, List, Optional

from config import (
    CATEGORIES_JSON,
    NEWS_FIELDS,
    _DEFAULT_NEWS_CATEGORIES,
)
from db import db_session
from models import NewsArticleModel
from services.guides import _sanitize_guide_html


# ==================== NEWS HELPERS ====================

def sanitize_news_body(raw: str) -> str:
    """Sanitize the HTML inside each text block of a JSON news body.

    News body is a JSON array of blocks; only text blocks carry rich HTML
    (caption/url are auto-escaped at render). Falls back to sanitizing the
    whole string for legacy plain-text/HTML bodies.
    """
    if not raw:
        return ""
    try:
        blocks = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return _sanitize_guide_html(raw)
    if not isinstance(blocks, list):
        return _sanitize_guide_html(raw)
    for block in blocks:
        if isinstance(block, dict) and block.get("type") == "text":
            block["content"] = _sanitize_guide_html(block.get("content", ""))
    return json.dumps(blocks, ensure_ascii=False)

def load_categories() -> list:
    """Load categories from JSON file, seeding from defaults if needed."""
    if CATEGORIES_JSON.exists():
        try:
            return json.loads(CATEGORIES_JSON.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    save_categories(_DEFAULT_NEWS_CATEGORIES)
    return list(_DEFAULT_NEWS_CATEGORIES)


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
