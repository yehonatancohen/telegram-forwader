#!/usr/bin/env python3
"""Output formatting and sending to Telegram output channels."""

from __future__ import annotations

import logging
from hashlib import sha1
from collections import deque
from typing import Deque

from telethon import TelegramClient

from authority import AuthorityTracker
from models import AggEvent

logger = logging.getLogger("sender")

SENT_CACHE: Deque[str] = deque(maxlen=800)


def _reliability_badge(score: float) -> str:
    """Return emoji badge based on authority score."""
    if score >= 75:
        return "🟢"
    if score >= 55:
        return "🟡"
    return "🔴"


def _source_badge(n_sources: int) -> str:
    """Return badge based on number of corroborating sources."""
    if n_sources >= 3:
        return "✅ מאומת"
    if n_sources == 2:
        return "🔄 חוזר"
    return "⚠️ מקור בודד"


class Sender:
    def __init__(self, client: TelegramClient, authority: AuthorityTracker,
                 arabs_chat: int, smart_chat: int):
        self.client = client
        self.authority = authority
        self.arabs_chat = arabs_chat
        self.smart_chat = smart_chat

    async def send_trend_report(self, event: AggEvent, summary_text: str):
        """Send a multi-source trend report to the arab output channel."""
        srcs = ", ".join(f"@{c}" for c in sorted(event.channels) if c)
        link = event.links[0] if event.links else ""
        n = len(event.channels)

        # Authority context
        scores = [self.authority.get_score(c) for c in event.channels]
        avg_score = sum(scores) / len(scores) if scores else 50
        badge = _reliability_badge(avg_score)
        label = self.authority.get_label(avg_score)
        src_badge = _source_badge(n)

        # Cross-source detection
        types = set(event.channel_types.values())
        cross_note = ""
        if "arab" in types and "smart" in types:
            cross_note = "\n🔗 אושש גם ע\"י מקורות ישראליים"

        lines = [
            f"{badge} {src_badge} | אמינות: {label}",
            f"━━━━━━━━━━━━━━━━━━━━",
            summary_text,
            f"━━━━━━━━━━━━━━━━━━━━",
            f"📡 {n} ערוצים: {srcs}",
        ]
        if link:
            lines.append(f"🔗 {link}")
        if cross_note:
            lines.append(cross_note)

        report = "\n".join(lines)

        h = sha1(report.encode()).hexdigest()[:16]
        if h in SENT_CACHE:
            return
        SENT_CACHE.append(h)

        try:
            await self.client.send_message(self.arabs_chat, report,
                                           link_preview=False)
            logger.info("trend report sent (%d channels, reliability: %s)",
                        n, label)
        except Exception as exc:
            logger.error("trend send fail: %s", exc)

    async def send_single_source_alert(self, event: AggEvent,
                                       summary_text: str):
        """Send a high-authority single-source alert."""
        ch = next(iter(event.channels))
        score = self.authority.get_score(ch)
        badge = _reliability_badge(score)
        label = self.authority.get_label(score)
        link = event.links[0] if event.links else ""

        lines = [
            f"{badge} ⚠️ מקור בודד | אמינות: {label}",
            f"━━━━━━━━━━━━━━━━━━━━",
            summary_text,
            f"━━━━━━━━━━━━━━━━━━━━",
            f"📡 @{ch}",
        ]
        if link:
            lines.append(f"🔗 {link}")

        report = "\n".join(lines)

        h = sha1(report.encode()).hexdigest()[:16]
        if h in SENT_CACHE:
            return
        SENT_CACHE.append(h)

        try:
            await self.client.send_message(self.arabs_chat, report,
                                           link_preview=False)
            logger.info("single-source alert sent (@%s, score: %.0f)", ch, score)
        except Exception as exc:
            logger.error("single-source send fail: %s", exc)

    async def send_batch_summary(self, summary: str):
        """Send a periodic batch summary."""
        if not summary:
            return
        report = f"📋 סיכום תקופתי\n━━━━━━━━━━━━━━━━━━━━\n{summary}"
        try:
            await self.client.send_message(self.arabs_chat, report,
                                           link_preview=False)
            logger.info("batch summary sent")
        except Exception as exc:
            logger.error("summary send error: %s", exc)
