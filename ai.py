#!/usr/bin/env python3
"""Gemini 2.0 Flash AI client — signature extraction & summarisation."""

from __future__ import annotations

import asyncio, json, logging, re, time

import httpx

from config import GEMINI_API_KEY, GEMINI_URL, LLM_BUDGET_HOURLY, LLM_RPM_LIMIT
from models import EventSignature

logger = logging.getLogger("ai")

# ───── Prompts ────────────────────────────────────────────────────────────

EXTRACT_PROMPT = """\
Extract the key intelligence elements from the following message.
The message may be in Arabic, Hebrew, or English — handle all three.
Normalize location names to their most common English or Arabic form.
Return ONLY valid JSON (no markdown fences, no extra text):
{
  "location": "specific place name or null",
  "region": "broader area (e.g. south lebanon, gaza, west bank, iran) or null",
  "event_type": "one of: strike, rocket, clash, arrest, movement, statement, casualty, other, irrelevant",
  "entities": ["named groups, people, or armed forces mentioned"],
  "keywords": ["2-3 key descriptive terms"],
  "is_urgent": true or false,
  "credibility_indicators": {
    "has_media_reference": true or false,
    "cites_named_source": true or false,
    "uses_vague_language": true or false,
    "is_forwarded_claim": true or false
  }
}
If the message is not about a security/military/political event, return: {"event_type":"irrelevant"}

Message:
"""

SUMMARY_PROMPT = """\
סכם בקצרה בעברית את הנקודות העיקריות מההודעות הבאות.
כתוב 2-3 שורות תמציתיות, בלי סגנון כתב חדשות.
אם מספר מקורות מדווחים על אותו אירוע, ציין זאת.
{authority_context}

ההודעות:
{messages}"""

TREND_PROMPT = """\
סכם במדויק בשורה אחת בעברית את המידע העיקרי שדווח במספר ערוצים.
המטרה – דיווח תמציתי וברור, בלי סגנון כתב חדשות.
לאחר מכן החזר שורה שנייה שמתחילה ב-"> " ומכילה תרגום לעברית של ציטוט מייצג מתוך ההודעה.
אל תכתוב שום דבר מעבר לשתי השורות.

{authority_context}

הטקסט המקורי:
{text}"""


class AIClient:
    def __init__(self):
        self._calls_used = 0
        self._budget_reset_ts = time.time()
        self._sem = asyncio.Semaphore(LLM_RPM_LIMIT)

    def _charge(self) -> bool:
        now = time.time()
        if now - self._budget_reset_ts >= 3600:
            self._calls_used = 0
            self._budget_reset_ts = now
        if self._calls_used >= LLM_BUDGET_HOURLY:
            logger.warning("LLM budget exhausted (%d/%d)", self._calls_used, LLM_BUDGET_HOURLY)
            return False
        self._calls_used += 1
        return True

    async def _call(self, prompt: str, timeout: int = 20) -> str:
        if not self._charge():
            return ""
        async with self._sem:
            try:
                async with httpx.AsyncClient(timeout=timeout) as c:
                    r = await c.post(
                        f"{GEMINI_URL}?key={GEMINI_API_KEY}",
                        json={
                            "contents": [{"parts": [{"text": prompt}]}],
                            "generationConfig": {"temperature": 0.2, "maxOutputTokens": 512},
                        },
                    )
                r.raise_for_status()
                return (
                    r.json()["candidates"][0]["content"]["parts"][0]["text"].strip()
                )
            except Exception as exc:
                logger.error("Gemini call failed: %s", exc)
                return ""

    # ─── Signature extraction ─────────────────────────────────────────────
    async def extract_signature(self, text: str) -> EventSignature | None:
        raw = await self._call(EXTRACT_PROMPT + text[:1500])
        if not raw:
            return None
        try:
            parsed = _parse_json(raw)
            if not parsed or parsed.get("event_type") == "irrelevant":
                return None
            return EventSignature.from_dict(parsed)
        except Exception as exc:
            logger.warning("signature parse failed: %s | raw: %s", exc, raw[:200])
            return None

    # ─── Batch summary ────────────────────────────────────────────────────
    async def summarize_batch(self, texts: list[str],
                              authority_context: str = "") -> str:
        blob = "\n---\n".join(t[:500] for t in texts[:20])
        prompt = SUMMARY_PROMPT.format(messages=blob,
                                       authority_context=authority_context)
        return await self._call(prompt)

    # ─── Trend report ─────────────────────────────────────────────────────
    async def summarize_trend(self, text: str,
                              authority_context: str = "") -> str:
        prompt = TREND_PROMPT.format(text=text[:800],
                                     authority_context=authority_context)
        return await self._call(prompt)


def _parse_json(raw: str) -> dict | None:
    """Extract JSON from LLM output, stripping markdown fences if present."""
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    cleaned = re.sub(r"```\s*$", "", cleaned, flags=re.MULTILINE).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find first { ... } block
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            try:
                return json.loads(m.group())
            except json.JSONDecodeError:
                pass
    return None
