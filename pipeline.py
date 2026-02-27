#!/usr/bin/env python3
"""Message processing pipeline: ingest → extract → correlate → score → dispatch."""

from __future__ import annotations

import asyncio, logging, time
from collections import deque
from dataclasses import dataclass
from hashlib import sha1
from typing import Deque, List, Optional

from ai import AIClient
from translate import translate_to_hebrew
from authority import AuthorityTracker
from config import (
    AUTHORITY_HIGH_THRESHOLD,
    BATCH_SIZE,
    EVENT_MERGE_WINDOW,
    FLUSH_EVERY,
    MAX_BATCH_AGE,
    MIN_SOURCES,
    SUMMARY_MIN_INTERVAL,
)
from correlation import EventPool, looks_urgent
from db import Database
from models import MessageInfo
from sender import Sender

logger = logging.getLogger("pipeline")


class Pipeline:
    def __init__(self, db: Database, ai: AIClient, authority: AuthorityTracker,
                 event_pool: EventPool, sender: Sender):
        self.db = db
        self.ai = ai
        self.authority = authority
        self.pool = event_pool
        self.sender = sender

        self._dup_cache: Deque[str] = deque(maxlen=500)

        # Stats
        self._stats = {"messages": 0, "events": 0, "errors": 0}

    async def process(self, info: MessageInfo):
        """Main entry point — every message (arab or smart) flows through here."""
        self._stats["messages"] += 1
        logger.info("[pipeline] msg from @%s (%s) len=%d",
                    info.channel or "?", info.channel_type, len(info.text))
        # Quick in-memory dedup
        h = sha1(info.text.encode()).hexdigest()
        if h in self._dup_cache:
            logger.debug("[pipeline] in-memory dedup skip @%s", info.channel)
            return
        self._dup_cache.append(h)

        # Also check SQLite dedup
        if await self.db.is_dup(h):
            logger.debug("[pipeline] sqlite dedup skip @%s", info.channel)
            return

        score = self.authority.get_score(info.channel)
        logger.debug("[pipeline] @%s score=%.1f", info.channel, score)

        # 1) Fast path: Deduplicate highly identical forwarded text
        match_id = self.pool.sha1_match(info.text)
        if match_id:
            await self.pool.ingest_by_sha1(info, match_id)
            logger.debug("SHA1 match for @%s on event %s", info.channel, match_id[:8])
            return

        # 2) AI extraction
        # All distinct messages now go through Groq AI (14,400 requests/day allows ~10/min)
        # Irrelevant or spammy messages will return `None` (event_type="irrelevant") and be dropped.
        logger.info("[pipeline] extracting AI signature for @%s...", info.channel)
        sig = await self.ai.extract_signature(info.text)
        if sig:
            self._stats["events"] += 1
            await self.pool.ingest_with_signature(sig, info)
            logger.info("[pipeline] signature: type=%s location=%s from @%s",
                        sig.event_type, sig.location or "?", info.channel)
        else:
            logger.debug("[pipeline] message dropped (irrelevant/no sig) from @%s", info.channel)


    # ─── Aggregator loop (runs in background) ────────────────────────────
    async def aggregator_loop(self):
        """Periodically check event pool for mature events."""
        while True:
            await asyncio.sleep(FLUSH_EVERY)
            now = time.time()
            for eid, ev in list(self.pool.active.items()):
                if now - ev.first_ts < EVENT_MERGE_WINDOW:
                    continue
                if ev.sent:
                    self.pool.expire(eid)
                    continue

                if len(ev.channels) >= MIN_SOURCES:
                    await self._dispatch_trend(ev)
                    await self.authority.on_event_corroborated(ev)
                    await self.db.mark_event_sent(eid)
                elif len(ev.channels) == 1:
                    ch = next(iter(ev.channels))
                    score = self.authority.get_score(ch)
                    if score >= 80:
                        await self._dispatch_single(ev)
                        await self.db.mark_event_sent(eid)
                    else:
                        await self.authority.on_event_expired_uncorroborated(ev)
                        await self.db.mark_event_expired(eid)

                ev.sent = True
                self.pool.expire(eid)

    async def _dispatch_trend(self, ev):
        """Send a trend report with translated text."""
        text = max(ev.texts, key=len) if ev.texts else ""
        n = len(ev.channels)
        summary = await translate_to_hebrew(text[:500])
        if not summary:
            summary = f"דיווחים חוזרים ({n} ערוצים) על אירוע חריג."
        logger.info("[pipeline] dispatching trend report (%d sources)", n)
        await self.sender.send_trend_report(ev, summary)

    async def _dispatch_single(self, ev):
        """Send a single high-authority source alert with translated text."""
        text = ev.texts[0][:500] if ev.texts else ""
        summary = await translate_to_hebrew(text)
        ch = next(iter(ev.channels))
        logger.info("[pipeline] dispatching single-source alert (@%s)", ch)
        await self.sender.send_single_source_alert(ev, summary)

    # ─── Periodic maintenance ─────────────────────────────────────────────
    async def decay_loop(self):
        """Hourly authority decay + DB cleanup + stats."""
        while True:
            await asyncio.sleep(3600)
            await self.authority.apply_decay()
            await self.db.cleanup_old()
            logger.info(
                "hourly maintenance | stats: msgs=%d events=%d errors=%d",
                self._stats["messages"], self._stats["events"],
                self._stats["errors"],
            )
