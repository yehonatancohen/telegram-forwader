#!/usr/bin/env python3
"""Free translation using Google Translate (no API key required)."""

from __future__ import annotations

import asyncio
import logging
import re
from functools import lru_cache

logger = logging.getLogger("translate")

# Detect if text contains significant Arabic/Farsi characters
_ARABIC_RE = re.compile(r"[\u0600-\u06FF\u0750-\u077F\uFB50-\uFDFF\uFE70-\uFEFF]")


def _has_arabic(text: str) -> bool:
    """Check if text contains substantial Arabic/Farsi content."""
    arabic_chars = len(_ARABIC_RE.findall(text))
    return arabic_chars > len(text) * 0.3  # more than 30% Arabic chars


@lru_cache(maxsize=256)
def _translate_sync(text: str, source: str = "auto", target: str = "iw") -> str:
    """Synchronous translation (cached). Uses 'iw' for Hebrew (Google's code)."""
    try:
        from deep_translator import GoogleTranslator
        translator = GoogleTranslator(source=source, target=target)
        # Google Translate has a 5000 char limit per request
        if len(text) > 4500:
            text = text[:4500]
        result = translator.translate(text)
        return result or text
    except Exception as e:
        logger.warning("translation failed: %s", e)
        return text


async def translate_to_hebrew(text: str) -> str:
    """Translate Arabic/Farsi text to Hebrew. Returns original if not Arabic."""
    if not text or not _has_arabic(text):
        return text

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(None, _translate_sync, text)
        if result and result != text:
            logger.info("[translate] translated %d chars ar→he", len(text))
            return result
    except Exception as e:
        logger.warning("[translate] failed: %s", e)

    return text
