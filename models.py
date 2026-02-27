#!/usr/bin/env python3
"""Shared data classes used across modules."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Set


@dataclass(slots=True)
class MessageInfo:
    text: str
    link: str
    channel: str
    media_id: str | None = None
    channel_type: str = "arab"      # "arab" or "smart"
    timestamp: float = 0.0


@dataclass
class EventSignature:
    location: str | None = None
    region: str | None = None
    event_type: str = "other"       # strike|rocket|clash|arrest|movement|statement|casualty|other|irrelevant
    entities: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)
    is_urgent: bool = False
    credibility_indicators: Dict[str, bool] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "location": self.location,
            "region": self.region,
            "event_type": self.event_type,
            "entities": self.entities,
            "keywords": self.keywords,
            "is_urgent": self.is_urgent,
            "credibility_indicators": self.credibility_indicators,
        }

    @classmethod
    def from_dict(cls, d: dict) -> EventSignature:
        return cls(
            location=d.get("location"),
            region=d.get("region"),
            event_type=d.get("event_type", "other"),
            entities=d.get("entities") or [],
            keywords=d.get("keywords") or [],
            is_urgent=d.get("is_urgent", False),
            credibility_indicators=d.get("credibility_indicators") or {},
        )


@dataclass
class AggEvent:
    event_id: str
    signature: EventSignature
    texts: List[str] = field(default_factory=list)
    channels: Set[str] = field(default_factory=set)
    channel_types: Dict[str, str] = field(default_factory=dict)   # channel -> type
    links: List[str] = field(default_factory=list)
    first_ts: float = field(default_factory=time.time)
    sent: bool = False
