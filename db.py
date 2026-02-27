#!/usr/bin/env python3
"""Async SQLite persistence for authority scores, events, and dedup cache."""

from __future__ import annotations

import json, logging, time
from pathlib import Path

import aiosqlite

logger = logging.getLogger("db")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS channels (
    username              TEXT PRIMARY KEY,
    channel_type          TEXT NOT NULL DEFAULT 'arab',
    authority_score       REAL NOT NULL DEFAULT 50.0,
    total_reports         INTEGER NOT NULL DEFAULT 0,
    corroborated          INTEGER NOT NULL DEFAULT 0,
    first_to_report       INTEGER NOT NULL DEFAULT 0,
    uncorroborated_urgent INTEGER NOT NULL DEFAULT 0,
    last_updated          REAL NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    signature_json TEXT NOT NULL,
    first_seen     REAL NOT NULL,
    last_updated   REAL NOT NULL,
    source_count   INTEGER NOT NULL DEFAULT 1,
    status         TEXT NOT NULL DEFAULT 'pending',
    sent_at        REAL
);

CREATE TABLE IF NOT EXISTS event_sources (
    event_id     TEXT NOT NULL REFERENCES events(event_id),
    channel      TEXT NOT NULL,
    reported_at  REAL NOT NULL,
    raw_text     TEXT,
    message_link TEXT,
    PRIMARY KEY (event_id, channel)
);

CREATE TABLE IF NOT EXISTS dedup_cache (
    hash_key   TEXT PRIMARY KEY,
    created_at REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
"""


class Database:
    def __init__(self, path: Path):
        self._path = path
        self._db: aiosqlite.Connection | None = None

    async def init(self):
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self._path))
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.executescript(_SCHEMA)
        await self._db.commit()
        logger.info("database ready at %s", self._path)

    async def close(self):
        if self._db:
            await self._db.close()

    # ─── Channel authority ────────────────────────────────────────────────
    async def ensure_channel(self, username: str, channel_type: str = "arab",
                             default_score: float = 50.0):
        await self._db.execute(
            "INSERT OR IGNORE INTO channels (username, channel_type, authority_score, last_updated) "
            "VALUES (?, ?, ?, ?)",
            (username, channel_type, default_score, time.time()),
        )
        await self._db.commit()

    async def get_authority(self, channel: str) -> float:
        cur = await self._db.execute(
            "SELECT authority_score FROM channels WHERE username = ?", (channel,)
        )
        row = await cur.fetchone()
        return row[0] if row else 50.0

    async def get_all_authorities(self) -> dict[str, float]:
        cur = await self._db.execute("SELECT username, authority_score FROM channels")
        return {r[0]: r[1] for r in await cur.fetchall()}

    async def update_authority(self, channel: str, new_score: float,
                               corroborated_delta: int = 0,
                               first_to_report_delta: int = 0,
                               uncorroborated_urgent_delta: int = 0):
        await self._db.execute(
            "UPDATE channels SET authority_score = ?, "
            "total_reports = total_reports + 1, "
            "corroborated = corroborated + ?, "
            "first_to_report = first_to_report + ?, "
            "uncorroborated_urgent = uncorroborated_urgent + ?, "
            "last_updated = ? "
            "WHERE username = ?",
            (new_score, corroborated_delta, first_to_report_delta,
             uncorroborated_urgent_delta, time.time(), channel),
        )
        await self._db.commit()

    async def bulk_update_scores(self, scores: dict[str, float]):
        for ch, score in scores.items():
            await self._db.execute(
                "UPDATE channels SET authority_score = ?, last_updated = ? WHERE username = ?",
                (score, time.time(), ch),
            )
        await self._db.commit()

    # ─── Events ───────────────────────────────────────────────────────────
    async def record_event(self, event_id: str, signature: dict,
                           channel: str, text: str, link: str):
        now = time.time()
        await self._db.execute(
            "INSERT INTO events (event_id, signature_json, first_seen, last_updated) "
            "VALUES (?, ?, ?, ?)",
            (event_id, json.dumps(signature, ensure_ascii=False), now, now),
        )
        await self._db.execute(
            "INSERT OR IGNORE INTO event_sources (event_id, channel, reported_at, raw_text, message_link) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, channel, now, text[:2000], link),
        )
        await self._db.commit()

    async def add_event_source(self, event_id: str, channel: str,
                               text: str, link: str):
        now = time.time()
        await self._db.execute(
            "INSERT OR IGNORE INTO event_sources (event_id, channel, reported_at, raw_text, message_link) "
            "VALUES (?, ?, ?, ?, ?)",
            (event_id, channel, now, text[:2000], link),
        )
        await self._db.execute(
            "UPDATE events SET source_count = source_count + 1, last_updated = ? "
            "WHERE event_id = ?",
            (now, event_id),
        )
        await self._db.commit()

    async def mark_event_sent(self, event_id: str):
        await self._db.execute(
            "UPDATE events SET status = 'sent', sent_at = ? WHERE event_id = ?",
            (time.time(), event_id),
        )
        await self._db.commit()

    async def mark_event_expired(self, event_id: str):
        await self._db.execute(
            "UPDATE events SET status = 'expired' WHERE event_id = ?",
            (event_id,),
        )
        await self._db.commit()

    async def get_pending_events(self) -> list[dict]:
        cur = await self._db.execute(
            "SELECT event_id, signature_json, first_seen, source_count "
            "FROM events WHERE status = 'pending'"
        )
        rows = await cur.fetchall()
        return [
            {"event_id": r[0], "signature": json.loads(r[1]),
             "first_seen": r[2], "source_count": r[3]}
            for r in rows
        ]

    async def get_event_sources(self, event_id: str) -> list[dict]:
        cur = await self._db.execute(
            "SELECT channel, reported_at, raw_text, message_link "
            "FROM event_sources WHERE event_id = ? ORDER BY reported_at",
            (event_id,),
        )
        return [
            {"channel": r[0], "reported_at": r[1], "raw_text": r[2], "link": r[3]}
            for r in await cur.fetchall()
        ]

    # ─── Dedup cache ──────────────────────────────────────────────────────
    async def is_dup(self, hash_key: str) -> bool:
        cur = await self._db.execute(
            "SELECT 1 FROM dedup_cache WHERE hash_key = ?", (hash_key,)
        )
        if await cur.fetchone():
            return True
        await self._db.execute(
            "INSERT OR IGNORE INTO dedup_cache (hash_key, created_at) VALUES (?, ?)",
            (hash_key, time.time()),
        )
        await self._db.commit()
        return False

    # ─── Cleanup ──────────────────────────────────────────────────────────
    async def cleanup_old(self, max_age: int = 86400):
        cutoff = time.time() - max_age
        await self._db.execute("DELETE FROM dedup_cache WHERE created_at < ?", (cutoff,))
        await self._db.execute(
            "DELETE FROM event_sources WHERE event_id IN "
            "(SELECT event_id FROM events WHERE last_updated < ? AND status != 'pending')",
            (cutoff,),
        )
        await self._db.execute(
            "DELETE FROM events WHERE last_updated < ? AND status != 'pending'",
            (cutoff,),
        )
        await self._db.commit()
        # Checkpoint WAL to prevent unbounded growth
        try:
            await self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except Exception:
            pass
        logger.debug("cleaned up records older than %ds", max_age)
