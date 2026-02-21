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
        link = event.links[0] if event.links else "ללא-קישור"
        n = len(event.channels)

        # Authority context
        scores = [self.authority.get_score(c) for c in event.channels]
        avg_score = sum(scores) / len(scores) if scores else 50
        label = self.authority.get_label(avg_score)

        # Cross-source detection
        types = set(event.channel_types.values())
        cross_note = ""
        if "arab" in types and "smart" in types:
            cross_note = " | אושש גם ע\"י מקורות ישראליים"

        report = (
            f"{summary_text}\n"
            f"(מקור: {link} | דווח ב-{n} ערוצים: {srcs} | "
            f"אמינות: {label}{cross_note})"
        )

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
        label = self.authority.get_label(score)
        link = event.links[0] if event.links else ""

        report = (
            f"{summary_text}\n"
            f"[מקור בודד באמינות {label}: @{ch}]"
        )
        if link:
            report += f"\n{link}"

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
        try:
            await self.client.send_message(self.arabs_chat, summary,
                                           link_preview=False)
            logger.info("batch summary sent")
        except Exception as exc:
            logger.error("summary send error: %s", exc)
