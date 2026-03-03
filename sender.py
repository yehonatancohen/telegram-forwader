#!/usr/bin/env python3
"""Output formatting and sending to Telegram output channel."""

from __future__ import annotations

import logging
from hashlib import sha1
from collections import deque
from typing import Deque, List

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


def _credit_footer(n_channels: int, links: list[str], channels: set[str]) -> str:
    """Build the footer with sources and bot group credit."""
    lines = ["━━━━━━━━━━━━━━━━━━━━"]
    if links:
        lines.append(f"📡 מקורות ({n_channels}):")
        for link in links[:5]:
            lines.append(f"🔗 {link}")
    else:
        srcs = ", ".join(f"@{c}" for c in sorted(channels) if c)
        lines.append(f"📡 מקורות ({n_channels}): {srcs}")
        
    lines.append(f"📢 הצטרפו לערוץ הדיווחים: {BOT_GROUP_LINK}")
    return "\n".join(lines)


def _is_sent(text: str) -> bool:
    """Check if this content was already sent (dedup)."""
    h = sha1(text.encode()).hexdigest()[:16]
    if h in SENT_CACHE:
        return True
    SENT_CACHE.append(h)
    return False


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
            _credit_footer(n, event.links, event.channels),
        ]
        if cross_note:
            lines.append(cross_note)

        report = "\n".join(lines)

        if _is_sent(summary_text):
            return

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
            _credit_footer(1, event.links, event.channels),
        ]

        report = "\n".join(lines)

        if _is_sent(summary_text):
            return

        try:
            await self.client.send_message(self.output_chat, report,
                                           link_preview=False)
            logger.info("[sender] single-source alert SENT (@%s, score=%.0f)", ch, score)
        except Exception as exc:
            logger.error("[sender] single-source send FAILED: %s", exc)

    async def send_digest(self, events: List[tuple[AggEvent, str]]):
        """Send a single consolidated digest containing multiple events.

        events: list of (AggEvent, translated_summary) tuples.
        """
        if not events:
            return

        # Single event — use the normal per-event format
        if len(events) == 1:
            ev, summary = events[0]
            if len(ev.channels) >= 2:
                await self.send_trend_report(ev, summary)
            else:
                await self.send_single_source_alert(ev, summary)
            return

        sections = [f"🔔 עדכון מודיעין ({len(events)} אירועים)"]

        for ev, summary in events:
            n = len(ev.channels)
            scores = [self.authority.get_score(c) for c in ev.channels]
            avg_score = sum(scores) / len(scores) if scores else 50
            badge = _reliability_badge(avg_score)
            src_badge = _source_badge(n)

            # Source line
            if ev.links:
                src_line = f"📡 {n} מקורות | " + " ".join(
                    f"🔗 {lnk}" for lnk in ev.links[:3]
                )
            else:
                srcs = ", ".join(f"@{c}" for c in sorted(ev.channels) if c)
                src_line = f"📡 {srcs}"

            sections.append("━━━━━━━━━━━━━━━━━━━━")
            sections.append(f"{badge} {src_badge} | {ev.signature.event_type}")
            sections.append(summary)
            sections.append(src_line)

        sections.append("━━━━━━━━━━━━━━━━━━━━")
        sections.append(f"📢 הצטרפו לערוץ הדיווחים: {BOT_GROUP_LINK}")

        report = "\n".join(sections)

        if _is_sent(report):
            return

        try:
            await self.client.send_message(self.output_chat, report,
                                           link_preview=False)
            logger.info("[sender] digest SENT (%d events)", len(events))
        except Exception as exc:
            logger.error("[sender] digest send FAILED: %s", exc)

    async def send_batch_summary(self, summary: str):
        """Send a batch intel digest."""
        if not summary:
            return

        lines = [
            "🔔 עדכון מודיעין",
            "━━━━━━━━━━━━━━━━━━━━",
            summary,
            "━━━━━━━━━━━━━━━━━━━━",
            f"📢 הצטרפו לערוץ הדיווחים: {BOT_GROUP_LINK}",
        ]
        report = "\n".join(lines)

        if _is_sent(report):
            return

        try:
            await self.client.send_message(self.output_chat, report,
                                           link_preview=False)
            logger.info("[sender] intel digest SENT (len=%d)", len(summary))
        except Exception as exc:
            logger.error("[sender] digest send FAILED: %s", exc)
