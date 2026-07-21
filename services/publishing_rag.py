"""Strict, side-effect-free preparation of one immutable Paper revision."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Iterable

import llm_client
import pdf_text
import rag_index
import vision_read
from services.publishing_contracts import (
    IndexDeadlineExceeded,
    PreparedChunk,
    PreparedRevisionIndex,
)


def _index_ocr_languages(language: str) -> str:
    normalized = (language or "").strip().lower()
    if normalized == "en":
        return "eng"
    if normalized == "zh":
        return "chi_sim+eng"
    return "eng+chi_sim"


class StrictRagAdapter:
    """Prepare frozen chunks without opening a database session or writing state."""

    def __init__(
        self,
        *,
        embedding_available: Callable[[], bool] | None = None,
        extract_text: Callable | None = None,
        chunker: Callable[[str], list[str]] | None = None,
        embed_texts: Callable | None = None,
        vision_available: Callable[[], bool] | None = None,
        transcribe: Callable | None = None,
        monotonic: Callable[[], float] | None = None,
    ):
        self._embedding_available = (
            embedding_available or llm_client.embedding_enabled
        )
        self._extract_text = extract_text or pdf_text.extract_pdf_text
        self._chunker = chunker or rag_index.chunk_text
        self._embed_texts = embed_texts or self._embed_with_provider
        self._vision_available = vision_available or llm_client.vision_enabled
        self._transcribe = transcribe or vision_read.transcribe_pdf
        self._monotonic = monotonic or time.monotonic

    def enabled(self) -> bool:
        return bool(self._embedding_available())

    def _check_deadline(self, deadline: float | None) -> None:
        if deadline is not None and self._monotonic() >= float(deadline):
            raise IndexDeadlineExceeded()

    def _run_stage(self, deadline: float | None, callback):
        """Run one stage and make an exhausted deadline authoritative on error."""
        self._check_deadline(deadline)
        try:
            result = callback()
        except IndexDeadlineExceeded:
            raise
        except Exception as exc:
            if deadline is not None and self._monotonic() >= float(deadline):
                raise IndexDeadlineExceeded() from exc
            raise
        self._check_deadline(deadline)
        return result

    @staticmethod
    def _embed_with_provider(texts, *, deadline=None):
        return rag_index.embed_texts(
            texts,
            deadline=deadline,
            build_embed_client=llm_client.build_embed_client,
            embed_model=llm_client.embed_model,
            embed_batch_size=llm_client.embed_batch_size,
        )

    @staticmethod
    def _validated_vector(raw: Iterable, *, dimension: int | None) -> tuple[float, ...]:
        try:
            vector = tuple(float(value) for value in raw)
        except (TypeError, ValueError) as exc:
            raise ValueError("embedding values must be numeric") from exc
        if not vector or not all(math.isfinite(value) for value in vector):
            raise ValueError("embedding must be nonempty and finite")
        if dimension is not None and len(vector) != dimension:
            raise ValueError("embedding dimensions must be consistent")
        return vector

    def prepare(
        self,
        *,
        paper_id: str,
        revision_number: int,
        pdf_bytes: bytes,
        language: str,
        deadline: float | None,
    ) -> PreparedRevisionIndex:
        if type(pdf_bytes) is not bytes:
            raise TypeError("PDF revision must be immutable bytes")
        self._check_deadline(deadline)

        vision_fallback = None
        if self._vision_available():
            vision_fallback = lambda raw, max_pages: self._transcribe(
                raw,
                max_pages=max_pages,
                language=language or "en",
                deadline=deadline,
                strict=True,
            )
        text = self._run_stage(
            deadline,
            lambda: self._extract_text(
                pdf_bytes,
                ocr_langs=_index_ocr_languages(language),
                max_ocr_pages=50,
                vision_fallback=vision_fallback,
                deadline=deadline,
                strict=True,
            ),
        )
        chunks = self._run_stage(deadline, lambda: tuple(self._chunker(text)))
        if not chunks:
            raise ValueError("PDF produced no indexable text")

        raw_vectors = self._run_stage(
            deadline,
            lambda: list(self._embed_texts(list(chunks), deadline=deadline)),
        )
        if len(raw_vectors) != len(chunks):
            raise ValueError("embedding result count does not match chunks")

        prepared_chunks = []
        dimension = None
        for index, (content, raw_vector) in enumerate(zip(chunks, raw_vectors)):
            vector = self._validated_vector(raw_vector, dimension=dimension)
            if dimension is None:
                dimension = len(vector)
            prepared_chunks.append(
                PreparedChunk(
                    chunk_index=index,
                    content=content,
                    embedding=vector,
                    language=language or "",
                )
            )
        return PreparedRevisionIndex(
            paper_id=paper_id,
            revision=revision_number,
            chunks=tuple(prepared_chunks),
        )
