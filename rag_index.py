# rag_index.py
"""In-process RAG index for Ask-the-Library.

Paper text -> overlapping chunks -> embeddings (Gemini OpenAI-compatible) stored
in MySQL (papers_chunks) -> pure-Python cosine retrieval over an in-memory cache.
No numpy / vector DB dependency.
"""

from __future__ import annotations

import math


def chunk_text(text: str, size: int = 800, overlap: int = 120) -> list[str]:
    """Split text into overlapping character chunks. Returns [] for blank text."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= size:
        return [text]
    if overlap >= size:
        overlap = size // 4
    chunks: list[str] = []
    start = 0
    step = size - overlap
    while start < len(text):
        chunks.append(text[start:start + size])
        if start + size >= len(text):
            break
        start += step
    return chunks


def cosine(a: list[float], b: list[float]) -> float:
    """Cosine similarity; 0.0 if either vector is zero-length or empty."""
    if not a or not b:
        return 0.0
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b):
        dot += x * y
        na += x * x
        nb += y * y
    if na == 0.0 or nb == 0.0:
        return 0.0
    return dot / (math.sqrt(na) * math.sqrt(nb))
