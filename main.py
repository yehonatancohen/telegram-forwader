#!/usr/bin/env python3
"""
Telegram intel monitor — slim entry point.
Wires: listener → pipeline → correlation → authority → sender
"""

from __future__ import annotations

import asyncio, logging, sys
from pathlib import Path

from telethon import TelegramClient
from telethon.sessions import StringSession

import config
from ai import AIClient
from authority import AuthorityTracker
from correlation import EventPool
from db import Database
from listener import init_listeners
from pipeline import Pipeline
from sender import Sender

config.validate()

logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger("main")

# Use StringSession if available, otherwise fall back to file-based session
_session = (StringSession(config.SESSION_STRING) if config.SESSION_STRING
            else str(config.SESSION_PATH))
client = TelegramClient(
    _session, config.API_ID, config.API_HASH,
    connection_retries=-1, retry_delay=5, timeout=10,
)


def _load_usernames(path: Path) -> list[str]:
    if not path.is_file():
        logger.warning("channels file missing: %s", path)
        return []
    return sorted({
        line.strip().lstrip("@").lower()
        for line in path.read_text().splitlines()
        if line.strip()
    })


async def main():
    if not config.SESSION_STRING and not config.SESSION_PATH.is_file():
        logger.critical("no session — set TG_SESSION_STRING or provide %s file",
                        config.SESSION_PATH)
        sys.exit(1)

    await client.start(phone=lambda: config.PHONE)
    if not await client.is_user_authorized():
        logger.critical("interactive login needed")
        sys.exit(1)
    logger.info("authorised")

    # ─── Init components ──────────────────────────────────────────────
    db = Database(config.DB_PATH)
    await db.init()

    ai = AIClient()
    authority = AuthorityTracker(db)
    event_pool = EventPool(db, ai)
    sender = Sender(client, authority, config.ARABS_SUMMARY_OUT, config.SMART_CHAT)
    pipeline = Pipeline(db, ai, authority, event_pool, sender)

    # Load channel lists
    arab = _load_usernames(config.ARAB_SOURCES_FILE)
    smart = _load_usernames(config.SMART_SOURCES_FILE)

    # Init authority scores & restore pending events
    await authority.load(arab, smart)
    await event_pool.load_from_db()

    # ─── Wire listeners ───────────────────────────────────────────────
    await init_listeners(
        client=client,
        arab_channels=arab,
        smart_channels=smart,
        pipeline_push=pipeline.process,
        smart_chat_id=config.SMART_CHAT or None,
    )

    # ─── Background tasks ─────────────────────────────────────────────
    asyncio.create_task(pipeline.aggregator_loop(), name="aggregator")
    asyncio.create_task(pipeline.decay_loop(), name="decay")

    logger.info("bot online – %d arab | %d smart", len(arab), len(smart))
    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
