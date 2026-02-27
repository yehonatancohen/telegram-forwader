#!/usr/bin/env python3
"""Output formatting and sending to Telegram output channel."""

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
BOT_GROUP_LINK = "https://t.me/+QlRlin8-CU0yMjZk"


def _reliability_badge(score: float) -> str:
    if score >= 75:
        return "🟢"
    if score >= 55:
        return "🟡"
    return "🔴"


def _source_badge(n_sources: int) -> str:
    if n_sources >= 3:
        return "✅ מאומת"
    if n_sources == 2:
        return "🔄 חוזר"
    return "⚠️ מקור בודד"


def _credit_footer(sources: str, links: list[str]) -> str:
    """Build the footer with sources and bot group credit."""
    lines = [
        "━━━━━━━━━━━━━━━━━━━━",
        f"📡 {sources}",
    ]
    for link in links[:3]:
        if link:
            lines.append(f"🔗 {link}")
    lines.append(f"📢 מקור: {BOT_GROUP_LINK}")
    return "\n".join(lines)


class Sender:
    def __init__(self, client: TelegramClient, authority: AuthorityTracker,
                 output_chat: int):
        self.client = client
        self.authority = authority
        self.output_chat = output_chat

    async def send_trend_report(self, event: AggEvent, summary_text: str):
        """Send a multi-source trend report."""
        srcs = ", ".join(f"@{c}" for c in sorted(event.channels) if c)
        n = len(event.channels)

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
            "━━━━━━━━━━━━━━━━━━━━",
            summary_text,
            _credit_footer(f"{n} ערוצים: {srcs}", event.links),
        ]
        if cross_note:
            lines.append(cross_note)

        report = "\n".join(lines)

        h = sha1(report.encode()).hexdigest()[:16]
        if h in SENT_CACHE:
            return
        SENT_CACHE.append(h)

        try:
            await self.client.send_message(self.output_chat, report,
                                           link_preview=False)
            logger.info("[sender] trend report SENT (%d channels, reliability=%s)",
                        n, label)
        except Exception as exc:
            logger.error("[sender] trend send FAILED: %s", exc)

    async def send_single_source_alert(self, event: AggEvent,
                                       summary_text: str):
        """Send a high-authority single-source alert."""
        ch = next(iter(event.channels))
        score = self.authority.get_score(ch)
        badge = _reliability_badge(score)
        label = self.authority.get_label(score)

        lines = [
            f"{badge} ⚠️ מקור בודד | אמינות: {label}",
            "━━━━━━━━━━━━━━━━━━━━",
            summary_text,
            _credit_footer(f"@{ch}", event.links),
        ]

        report = "\n".join(lines)

        h = sha1(report.encode()).hexdigest()[:16]
        if h in SENT_CACHE:
            return
        SENT_CACHE.append(h)

        try:
            await self.client.send_message(self.output_chat, report,
                                           link_preview=False)
            logger.info("[sender] single-source alert SENT (@%s, score=%.0f)", ch, score)
        except Exception as exc:
            logger.error("[sender] single-source send FAILED: %s", exc)

    async def send_batch_summary(self, summary: str):
        """Send a periodic batch summary."""
        if not summary:
            return
        lines = [
            "📋 סיכום תקופתי",
            "━━━━━━━━━━━━━━━━━━━━",
            summary,
            "━━━━━━━━━━━━━━━━━━━━",
            f"📢 מקור: {BOT_GROUP_LINK}",
        ]
        report = "\n".join(lines)
        try:
            await self.client.send_message(self.output_chat, report,
                                           link_preview=False)
            logger.info("[sender] batch summary SENT (len=%d)", len(summary))
        except Exception as exc:
            logger.error("[sender] summary send FAILED: %s", exc)
