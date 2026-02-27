#!/usr/bin/env python3
"""Message processing pipeline: ingest → extract → correlate → score → dispatch."""

from __future__ import annotations

import asyncio, logging, time
from collections import deque
from dataclasses import dataclass
from hashlib import sha1
from typing import Deque, List, Optional

from ai import AIClient
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


@dataclass
class _BatchState:
    msgs: List[MessageInfo]
    timer: Optional[asyncio.Task]


class Pipeline:
    def __init__(self, db: Database, ai: AIClient, authority: AuthorityTracker,
                 event_pool: EventPool, sender: Sender):
        self.db = db
        self.ai = ai
        self.authority = authority
        self.pool = event_pool
        self.sender = sender

        self._batch = _BatchState([], None)
        self._batch_lock = asyncio.Lock()
        self._summary_lock = asyncio.Lock()
        self._last_summary_ts = 0.0
        self._dup_cache: Deque[str] = deque(maxlen=500)

        # Stats
        self._stats = {"messages": 0, "events": 0, "summaries": 0, "errors": 0}

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
        urgent = looks_urgent(info.text)
        logger.debug("[pipeline] @%s score=%.1f urgent=%s", info.channel, score, urgent)

        if urgent or score >= AUTHORITY_HIGH_THRESHOLD:
            # High-priority path: AI signature extraction → event pool
            logger.info("[pipeline] HIGH PRIORITY: @%s (urgent=%s, score=%.1f)",
                        info.channel, urgent, score)
            await self._high_priority(info, urgent)
        else:
            # Medium/low: batch collector + cheap SHA1 pre-check
            match_id = self.pool.sha1_match(info.text)
            if match_id:
                # Text matches an existing event — add corroboration
                await self.pool.ingest_by_sha1(info, match_id)
                logger.debug("SHA1 match for @%s on event %s",
                             info.channel, match_id[:8])
            else:
                await self._batch_push(info)

    async def _high_priority(self, info: MessageInfo, urgent: bool):
        """Extract signature via AI and feed into event pool."""
        logger.info("[pipeline] extracting AI signature for @%s...", info.channel)
        sig = await self.ai.extract_signature(info.text)
        if sig:
            self._stats["events"] += 1
            await self.pool.ingest_with_signature(sig, info)
            logger.info("[pipeline] signature: type=%s location=%s from @%s",
                        sig.event_type, sig.location or "?", info.channel)
        elif urgent:
            logger.info("[pipeline] AI returned no signature but msg is urgent, batching @%s",
                        info.channel)
            # AI failed but message looks urgent — still add to batch
            await self._batch_push(info)
        else:
            logger.debug("[pipeline] AI returned no signature for @%s", info.channel)

    # ─── Batch collector ──────────────────────────────────────────────────
    async def _batch_push(self, info: MessageInfo):
        async with self._batch_lock:
            self._batch.msgs.append(info)
            if len(self._batch.msgs) >= BATCH_SIZE:
                await self._flush_batch()
            elif not self._batch.timer or self._batch.timer.done():
                self._batch.timer = asyncio.create_task(self._auto_flush())

    async def _auto_flush(self):
        await asyncio.sleep(MAX_BATCH_AGE)
        async with self._batch_lock:
            await self._flush_batch()

    async def _flush_batch(self):
        if not self._batch.msgs:
            return
        msgs = self._batch.msgs
        self._batch.msgs = []
        logger.info("[pipeline] flushing batch of %d messages for summary", len(msgs))
        asyncio.create_task(self._send_summary(msgs))

    async def _send_summary(self, msgs: List[MessageInfo]):
        try:
            async with self._summary_lock:
                wait = SUMMARY_MIN_INTERVAL - (time.time() - self._last_summary_ts)
                if wait > 0:
                    await asyncio.sleep(wait)
                self._last_summary_ts = time.time()

            # Build authority context
            channels = {m.channel for m in msgs if m.channel}
            if channels:
                scores = {c: self.authority.get_score(c) for c in channels}
                top = sorted(scores.items(), key=lambda x: -x[1])[:3]
                ctx_parts = [f"@{c} (אמינות: {self.authority.get_label(s)})"
                             for c, s in top]
                authority_ctx = "מקורות עיקריים: " + ", ".join(ctx_parts)
            else:
                authority_ctx = ""

            texts = [m.text for m in msgs]
            summary = await self.ai.summarize_batch(texts, authority_ctx)
            await self.sender.send_batch_summary(summary)
            self._stats["summaries"] += 1
        except Exception:
            self._stats["errors"] += 1
            logger.exception("batch summary failed — %d messages dropped", len(msgs))

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
        """Generate and send a trend report."""
        # Build authority context for the summary
        scores = {c: self.authority.get_score(c) for c in ev.channels}
        top = sorted(scores.items(), key=lambda x: -x[1])[:3]
        ctx = "מקורות: " + ", ".join(
            f"@{c} ({self.authority.get_label(s)})" for c, s in top
        )

        summary = await self.ai.summarize_trend(ev.texts[0], ctx)
        if not summary:
            n = len(ev.channels)
            quote = ev.texts[0][:120]
            summary = (
                f"עדכון מגמה: דיווחים חוזרים ({n} ערוצים) "
                f"על אירוע/תנועה חריגה.\n> \"{quote}...\""
            )
        await self.sender.send_trend_report(ev, summary)

    async def _dispatch_single(self, ev):
        """Generate and send a single high-authority source alert."""
        ch = next(iter(ev.channels))
        ctx = f"מקור: @{ch} (אמינות {self.authority.get_label(self.authority.get_score(ch))})"

        summary = await self.ai.summarize_trend(ev.texts[0], ctx)
        if not summary:
            summary = ev.texts[0][:200]
        await self.sender.send_single_source_alert(ev, summary)

    # ─── Periodic maintenance ─────────────────────────────────────────────
    async def decay_loop(self):
        """Hourly authority decay + DB cleanup + stats."""
        while True:
            await asyncio.sleep(3600)
            await self.authority.apply_decay()
            await self.db.cleanup_old()
            logger.info(
                "hourly maintenance | stats: msgs=%d events=%d summaries=%d errors=%d",
                self._stats["messages"], self._stats["events"],
                self._stats["summaries"], self._stats["errors"],
            )
