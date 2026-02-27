#!/usr/bin/env python3
"""Event correlation — signature-based semantic matching & event pool."""

from __future__ import annotations

import logging, re, time, unicodedata
from hashlib import sha1
from typing import Dict
from uuid import uuid4

from ai import AIClient
from config import (
    EVENT_MERGE_WINDOW,
    SIGNATURE_MATCH_THRESHOLD,
)
from db import Database
from models import AggEvent, EventSignature, MessageInfo

logger = logging.getLogger("correlation")

# Arabic tashkeel (diacritics) range
_ARABIC_DIACRITICS = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670]")

# ───── Urgent keywords (Arabic + Hebrew) ─────────────────────────────────
URGENT_KW = [
    # Arabic
    "عاجل", "انفجار", "انفجارات", "اشتباك", "هجوم", "غارة",
    "قتلى", "مقتل", "إصابة", "ازدحام", "قطع طرق", "أزمة سير",
    "احتجاج", "إغلاق", "زحمة", "طوارئ", "حرائق", "حريق", "صاروخ", "درون",
    # Hebrew
    "דחוף", "פיגוע", "ירי", "רקטה", "רקטות", "חיסול", "פיצוץ",
    "אירוע ביטחוני", "חדירה", "עימות", "הרוגים", "פצועים", "התקפה",
]


def looks_urgent(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in URGENT_KW) or any(s in text for s in ("🚨", "🔴"))


# ───── Cheap SHA1 key (fallback / pre-filter) ────────────────────────────
def sha1_event_key(text: str) -> str:
    text = unicodedata.normalize("NFC", text)
    text = _ARABIC_DIACRITICS.sub("", text)    # strip tashkeel
    cleaned = "".join(ch for ch in text.lower() if not ch.isdigit())
    return sha1(cleaned[:120].encode()).hexdigest()


# ───── Semantic matching ─────────────────────────────────────────────────

def signatures_match(a: EventSignature, b: EventSignature) -> float:
    """Return 0.0–1.0 similarity score between two event signatures."""
    score = 0.0

    # Location (strongest signal)
    if a.location and b.location:
        if _norm(a.location) == _norm(b.location):
            score += 0.5
        elif a.region and b.region and _norm(a.region) == _norm(b.region):
            score += 0.2
    elif a.region and b.region and _norm(a.region) == _norm(b.region):
        score += 0.2

    # Event type
    if a.event_type == b.event_type and a.event_type != "other":
        score += 0.3

    # Entity overlap (Jaccard)
    ea = {_norm(e) for e in a.entities} if a.entities else set()
    eb = {_norm(e) for e in b.entities} if b.entities else set()
    if ea and eb:
        jaccard = len(ea & eb) / len(ea | eb)
        score += 0.2 * jaccard

    return min(score, 1.0)


def _norm(s: str) -> str:
    return s.strip().lower()


# ───── Event Pool ────────────────────────────────────────────────────────

class EventPool:
    def __init__(self, db: Database, ai: AIClient):
        self.db = db
        self.ai = ai
        self.active: Dict[str, AggEvent] = {}
        self._sha1_index: Dict[str, str] = {}  # sha1_key -> event_id

    async def load_from_db(self):
        """Reload pending events from SQLite on startup."""
        rows = await self.db.get_pending_events()
        for r in rows:
            eid = r["event_id"]
            sig = EventSignature.from_dict(r["signature"])
            sources = await self.db.get_event_sources(eid)
            ev = AggEvent(
                event_id=eid,
                signature=sig,
                texts=[s["raw_text"] for s in sources if s["raw_text"]],
                channels={s["channel"] for s in sources},
                links=[s["link"] for s in sources if s["link"]],
                first_ts=r["first_seen"],
            )
            self.active[eid] = ev
        if self.active:
            logger.info("restored %d pending events from DB", len(self.active))

    async def ingest_with_signature(self, sig: EventSignature,
                                    info: MessageInfo) -> AggEvent:
        """Add a message with an AI-extracted signature to the event pool."""
        best_id, best_score = None, 0.0
        for eid, ev in self.active.items():
            s = signatures_match(sig, ev.signature)
            if s > best_score:
                best_id, best_score = eid, s

        if best_score >= SIGNATURE_MATCH_THRESHOLD and best_id:
            ev = self.active[best_id]
            if info.channel not in ev.channels:
                ev.texts.append(info.text)
                ev.channels.add(info.channel)
                ev.channel_types[info.channel] = info.channel_type
                if info.link:
                    ev.links.append(info.link)
                await self.db.add_event_source(best_id, info.channel,
                                               info.text, info.link)
                logger.info("corroboration: event %s now has %d sources",
                            best_id[:8], len(ev.channels))
            return ev

        # New event
        eid = str(uuid4())
        ev = AggEvent(
            event_id=eid,
            signature=sig,
            texts=[info.text],
            channels={info.channel},
            channel_types={info.channel: info.channel_type},
            links=[info.link] if info.link else [],
            first_ts=time.time(),
        )
        self.active[eid] = ev
        # Index by SHA1 for cheap lookups
        sha_key = sha1_event_key(info.text)
        self._sha1_index[sha_key] = eid
        await self.db.record_event(eid, sig.to_dict(), info.channel,
                                   info.text, info.link)
        return ev

    def sha1_match(self, text: str) -> str | None:
        """Cheap pre-check: does this text's SHA1 match any active event?"""
        key = sha1_event_key(text)
        return self._sha1_index.get(key)

    async def ingest_by_sha1(self, info: MessageInfo,
                             event_id: str) -> AggEvent:
        """Add a message to an existing event matched by SHA1."""
        ev = self.active[event_id]
        if info.channel not in ev.channels:
            ev.texts.append(info.text)
            ev.channels.add(info.channel)
            ev.channel_types[info.channel] = info.channel_type
            if info.link:
                ev.links.append(info.link)
            await self.db.add_event_source(event_id, info.channel,
                                           info.text, info.link)
        return ev

    def expire(self, event_id: str):
        self.active.pop(event_id, None)
        # Clean sha1 index
        self._sha1_index = {k: v for k, v in self._sha1_index.items()
                            if v != event_id}
