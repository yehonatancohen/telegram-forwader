#!/usr/bin/env python3
"""
Telegram intel monitor — slim entry point.
Wires: listener → pipeline → correlation → authority → sender

If the userbot session is invalid, falls back to companion bot mode
so the admin can renew the session via Telegram chat.
"""

from __future__ import annotations

import asyncio, logging, signal, sys
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
from session_manager import SessionManager, get_session_string

config.validate()

logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, config.LOG_LEVEL),
    format="%(asctime)s %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger("main")


def _load_usernames(path: Path) -> list[str]:
    if not path.is_file():
        logger.warning("channels file missing: %s", path)
        return []
    return sorted({
        line.strip().lstrip("@").lower()
        for line in path.read_text().splitlines()
        if line.strip()
    })


async def _start_companion_bot() -> SessionManager | None:
    """Start the companion bot if BOT_TOKEN + ADMIN_ID are configured."""
    if not config.BOT_TOKEN or not config.ADMIN_ID:
        return None
    try:
        mgr = SessionManager()
        await mgr.start()
        return mgr
    except Exception:
        logger.exception("companion bot failed to start")
        return None


async def _recovery_mode(mgr: SessionManager):
    """Session is dead — keep companion bot alive so admin can /login."""
    logger.warning("entering recovery mode — waiting for session renewal via companion bot")
    mgr.set_userbot_status(False)
    await mgr.notify_session_expired()
    # Block forever — the companion bot's /login handler will os._exit(0)
    # when a new session is saved, and Docker restarts us.
    await asyncio.Event().wait()


async def main():
    # ─── Companion bot (always starts first) ──────────────────────────
    mgr = await _start_companion_bot()

    # ─── Resolve session string ───────────────────────────────────────
    session_str = get_session_string()  # checks override file first

    if not session_str and not config.SESSION_PATH.is_file():
        logger.critical("no session — set TG_SESSION_STRING or provide %s file",
                        config.SESSION_PATH)
        if mgr:
            await _recovery_mode(mgr)
        sys.exit(1)

    _session = StringSession(session_str) if session_str else str(config.SESSION_PATH)
    client = TelegramClient(
        _session, config.API_ID, config.API_HASH,
        connection_retries=-1, retry_delay=5, timeout=10,
    )

    # ─── Userbot login ────────────────────────────────────────────────
    try:
        await client.start(phone=lambda: config.PHONE)
    except Exception:
        logger.exception("userbot connection failed")
        if mgr:
            await _recovery_mode(mgr)
        sys.exit(1)

    if not await client.is_user_authorized():
        logger.critical("userbot session expired / not authorized")
        await client.disconnect()
        if mgr:
            await _recovery_mode(mgr)
        sys.exit(1)

    if mgr:
        mgr.set_userbot_status(True)

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

    # ─── Graceful shutdown ────────────────────────────────────────────
    async def _shutdown():
        logger.info("shutting down gracefully...")
        if mgr:
            await mgr.stop()
        await db.close()
        await client.disconnect()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(_shutdown()))
        except NotImplementedError:
            pass

    # ─── Startup banner ───────────────────────────────────────────────
    session_type = "override file" if get_session_string() != config.SESSION_STRING \
        else ("StringSession" if session_str else f"file ({config.SESSION_PATH})")
    me = await client.get_me()
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    logger.info("  Telegram Intel Monitor — ONLINE")
    logger.info("  Account : %s (id=%d)", me.first_name, me.id)
    logger.info("  Session : %s", session_type)
    logger.info("  Channels: %d arab | %d smart", len(arab), len(smart))
    logger.info("  Output  : %s", config.ARABS_SUMMARY_OUT)
    logger.info("  AI model: %s", config.GEMINI_MODEL)
    logger.info("  Helper  : %s", "companion bot active" if mgr else "no companion bot")
    logger.info("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")

    await client.run_until_disconnected()


if __name__ == "__main__":
    asyncio.run(main())
