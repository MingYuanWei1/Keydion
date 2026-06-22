"""Vision-first metadata extraction — the shared cascade behind the three
metadata auto-fill extractors (EE marks, IA scores, abstract & keywords).

Each extractor subclasses VisionFirstExtractor and overrides the four hooks;
the concrete extract() drives the vision-first-then-fallback skeleton once.
See CONTEXT.md ("Vision-first extraction") and
docs/adr/0001-vision-first-extractor-template-method.md.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import llm_client
import vision_read

_log = logging.getLogger(__name__)


class VisionFirstExtractor(ABC):
    """Template-method base for vision-first metadata extraction.

    extract() runs the skeleton: gate on the vision model, read the rendered
    pages with it, and on any VisionError fall back to the text path; either
    branch's result is passed through post(). Subclasses supply the prompt,
    the vision-result shaper, the fallback, and (optionally) a post step.
    """

    MAX_PAGES: int = 10        # page budget for the vision read
    language: str = "en"       # forwarded to the vision read; subclasses may override

    @abstractmethod
    def build_prompt(self) -> str:
        """The system prompt sent to the vision model."""

    @abstractmethod
    def shape_vision(self, data: dict) -> dict:
        """Coerce extract_with_vision()'s dict into this extractor's result shape."""

    @abstractmethod
    def fallback(self, file_bytes: bytes) -> dict:
        """Produce the same result shape without the vision model."""

    def post(self, result: dict) -> dict:
        """Shared finalisation applied to whichever branch ran. Default: identity."""
        return result

    def extract(self, file_bytes: bytes) -> dict:
        if llm_client.vision_enabled():
            try:
                data = vision_read.extract_with_vision(
                    file_bytes, self.build_prompt(),
                    max_pages=self.MAX_PAGES, language=self.language,
                )
                return self.post(self.shape_vision(data))
            except vision_read.VisionError:
                _log.warning(
                    "vision extraction failed for %s; falling back to the text path",
                    type(self).__name__, exc_info=True,
                )
        return self.post(self.fallback(file_bytes))
