#!/usr/bin/env python3
"""Auto-calculated channel authority scoring."""

from __future__ import annotations

import logging
from typing import Dict

from config import (
    AUTHORITY_ARAB_DEFAULT,
    AUTHORITY_SMART_DEFAULT,
)
from db import Database
from models import AggEvent

logger = logging.getLogger("authority")

# Score bounds
MAX_SCORE = 95.0
MIN_SCORE = 10.0

# Adjustments
CORROBORATION_BOOST          = 2.0   # report backed by 2+ others
FIRST_TO_REPORT_BOOST        = 3.0   # was first on a corroborated event
UNCORROBORATED_URGENT_PENALTY = -1.5  # urgent claim nobody backed
DECAY_RATE                   = 0.01  # per-hour regression toward default


class AuthorityTracker:
    def __init__(self, db: Database):
        self.db = db
        self._cache: Dict[str, float] = {}
        self._defaults: Dict[str, float] = {}

    async def load(self, arab_channels: list[str], smart_channels: list[str]):
        for ch in arab_channels:
            await self.db.ensure_channel(ch, "arab", AUTHORITY_ARAB_DEFAULT)
            self._defaults[ch] = AUTHORITY_ARAB_DEFAULT
        for ch in smart_channels:
            await self.db.ensure_channel(ch, "smart", AUTHORITY_SMART_DEFAULT)
            self._defaults[ch] = AUTHORITY_SMART_DEFAULT
        self._cache = await self.db.get_all_authorities()
        logger.info("loaded authority scores for %d channels", len(self._cache))

    def get_score(self, channel: str) -> float:
        return self._cache.get(channel, AUTHORITY_ARAB_DEFAULT)

    def get_label(self, score: float) -> str:
        if score >= 80:
            return "גבוהה"
        if score >= 60:
            return "בינונית"
        return "נמוכה"

    async def on_event_corroborated(self, event: AggEvent):
        """Call when an event reaches MIN_SOURCES."""
        sorted_channels = sorted(event.channels,
                                 key=lambda c: event.first_ts)  # rough proxy
        first_channel = sorted_channels[0] if sorted_channels else None

        for ch in event.channels:
            await self._adjust(ch, CORROBORATION_BOOST,
                               corroborated_delta=1)
        if first_channel:
            await self._adjust(first_channel, FIRST_TO_REPORT_BOOST,
                               first_to_report_delta=1)
        logger.debug("corroboration boost for %s", event.channels)

    async def on_event_expired_uncorroborated(self, event: AggEvent):
        """Call when an urgent event expires with only 1 source."""
        if event.signature.is_urgent and len(event.channels) == 1:
            ch = next(iter(event.channels))
            await self._adjust(ch, UNCORROBORATED_URGENT_PENALTY,
                               uncorroborated_urgent_delta=1)
            logger.debug("uncorroborated urgent penalty for @%s", ch)

    async def apply_decay(self):
        """Hourly regression toward channel-type baseline."""
        updated = {}
        for ch, score in self._cache.items():
            baseline = self._defaults.get(ch, AUTHORITY_ARAB_DEFAULT)
            diff = score - baseline
            new_score = score - (diff * DECAY_RATE)
            new_score = max(MIN_SCORE, min(MAX_SCORE, new_score))
            if abs(new_score - score) > 0.01:
                updated[ch] = new_score
                self._cache[ch] = new_score
        if updated:
            await self.db.bulk_update_scores(updated)
            logger.debug("decay applied to %d channels", len(updated))

    async def _adjust(self, channel: str, delta: float,
                      corroborated_delta: int = 0,
                      first_to_report_delta: int = 0,
                      uncorroborated_urgent_delta: int = 0):
        current = self.get_score(channel)
        new_score = max(MIN_SCORE, min(MAX_SCORE, current + delta))
        self._cache[channel] = new_score
        await self.db.update_authority(
            channel, new_score,
            corroborated_delta=corroborated_delta,
            first_to_report_delta=first_to_report_delta,
            uncorroborated_urgent_delta=uncorroborated_urgent_delta,
        )
