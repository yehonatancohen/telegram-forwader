#!/usr/bin/env python3
"""Telegram listener

Responsibilities
----------------
* **Realtime forwarding** of *smart* channels to `smart_chat_id`.
* **Batch summarising** of *arab* channels through the caller‑provided
  `batch_push(MessageInfo)` coroutine (the summariser lives elsewhere).

Design goals
------------
* Crash‑proof: any unexpected exception is logged and the task restarts.
* Low memory: **no media is ever downloaded**.
* Extensive DEBUG logging to help diagnose live issues.

The module exposes a single public coroutine `init_listeners(...)` that
attaches handlers to an existing `TelegramClient` plus any auxiliary
reader accounts defined in the environment variable **TG_READERS_JSON**.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Sequence, Tuple

from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message

# ────────────────────────── configuration & logging ─────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("listener")

SUMMARY_GAP = int(os.getenv("SUMMARY_GAP", 300))  # used by external summariser

# ──────────────────────────────── constants ─────────────────────────────────
URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST = {
    "צבע אדום",
    "גרם",
    "היכנסו למרחב המוגן",
    "חדירת כלי טיס עוין",
}

_DUP_TEXT_CACHE: Deque[str] = deque(maxlen=1_000)  # for arab deduplication
_RECENT_MEDIA: Deque[str] = deque(maxlen=5_000)    # for smart deduplication

START_TS = time.time()  # messages older than this are ignored


@dataclass(slots=True)
class MessageInfo:
    text: str
    link: str
    channel: str
    media_id: Optional[str]


# ───────────────────────────── helper utilities ─────────────────────────────

def _clean_text(t: str) -> str:
    """Strip links & collapse whitespace."""
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()


def _blocked(t: str) -> bool:
    return any(k in t for k in BLOCKLIST)


def _dup_text(text: str) -> bool:
    dig = hashlib.sha1(text.encode()).hexdigest()
    if dig in _DUP_TEXT_CACHE:
        logger.debug("↻ dup‑skip text digest %s", dig[:10])
        return True
    _DUP_TEXT_CACHE.append(dig)
    return False


def _permalink(msg: Message) -> str:
    if (u := getattr(msg.chat, "username", None)):
        return f"https://t.me/{u}/{msg.id}"
    return ""


def _dedup_key(msg: Message) -> str:
    if msg.grouped_id:
        return f"album:{msg.grouped_id}"
    if msg.photo:
        return f"photo:{msg.photo.id}"
    if msg.document:
        return f"doc:{msg.document.id}"
    return f"text:{hashlib.sha1((msg.text or '').strip().encode()).hexdigest()}"


async def _ensure_join(cli: TelegramClient, channels: Sequence[str]):
    """Join channels if the account has not joined yet."""
    joined = {
        getattr(d.entity, "username", "").lower()
        for d in await cli.get_dialogs()
        if getattr(d.entity, "username", None)
    }
    for ch in channels:
        if ch.lower() in joined:
            continue
        try:
            await cli(JoinChannelRequest(ch))
            logger.info("➕ %s joined @%s", cli.session.filename, ch)
        except errors.FloodWaitError as e:
            logger.warning("⏳ flood‑wait join @%s (%s s)", ch, e.seconds)
            await asyncio.sleep(e.seconds)
        except Exception as exc:
            logger.error("❌ join failed @%s on %s: %s", ch, cli.session.filename, exc)


# ─────────────────────────────── main entrypoint ────────────────────────────
async def init_listeners(
    *,
    client: TelegramClient,  # primary account (sender)
    arab_channels: Sequence[str],
    smart_channels: Sequence[str] | None,
    batch_push: Callable[[MessageInfo], "asyncio.Future | asyncio.Task | None"],
    smart_chat_id: int | None = None,
    max_req_per_min: int = 18,
    scan_batch_limit: int = 100,
):
    """Attach realtime handlers & scanners on all accounts.

    * **arab** messages → cleaned & queued to *batch_push*.
    * **smart** messages → forwarded immediately to *smart_chat_id*.
    """

    # ── build client pool (main + optional readers) ───────────────────────
    pool: List[Tuple[TelegramClient, bool]] = [(client, True)]  # (cli, is_primary)

    for cfg in json.loads(os.getenv("TG_READERS_JSON", "[]")):
        try:
            cli = TelegramClient(
                cfg["session"],
                cfg["api_id"],
                cfg["api_hash"],
                connection_retries=3,
                retry_delay=2,
                request_retries=2,
                timeout=10,
            )
            await cli.start(phone=lambda: cfg.get("phone", ""))
            if not await cli.is_user_authorized():
                logger.critical("No valid .session for %s – aborting", cfg["session"])
                sys.exit(1)
            pool.append((cli, False))
            logger.info("🔌 reader %s connected", cfg["session"])
        except Exception as e:
            logger.error("reader %s failed: %s", cfg.get("session"), e)

    n_clients = len(pool)
    logger.info("👥 %d clients active", n_clients)

    # ── partition channels evenly among accounts ─────────────────────────
    def _split(seq: Sequence[str]) -> List[List[str]]:
        return [list(seq[i::n_clients]) for i in range(n_clients)]

    arab_parts, smart_parts = _split(arab_channels), _split(smart_channels or [])
    pacing = 60 / max_req_per_min

    # ── arab handler (batch summary) ─────────────────────────────────────
    async def _arab(msg: Message):
        if msg.date.timestamp() < START_TS:
            logger.debug("⏩ old msg skipped"),
            return
        if _blocked(msg.text or "") and not msg.media:
            logger.debug("🚫 blocked keywords – %s", msg.id)
            return
        clean = _clean_text(msg.text or "")
        if not clean or _dup_text(clean):
            return
        info = MessageInfo(
            text=clean,
            link=_permalink(msg),
            channel=getattr(msg.chat, "username", ""),
            media_id=None,
        )
        logger.debug("🆕 arab %s | %s", info.channel, clean[:60])
        batch_push(info)

    # ── smart handler factory (per‑reader) ───────────────────────────────
    def _smart_factory(origin: TelegramClient):
        async def _smart(msg):  # "msg" can be any type – we guard below
            if not isinstance(msg, Message):
                logger.warning("⚠️  _smart received non‑Message %r (%s)", msg, type(msg))
                return
            if (
                not smart_chat_id
                or msg.out
                or msg.via_bot_id
                or msg.date.timestamp() < START_TS
            ):
                return
            key = _dedup_key(msg)
            if key in _RECENT_MEDIA:
                logger.debug("↻ dup‑skip media key %s", key)
                return
            _RECENT_MEDIA.append(key)

            # collect grouped album if any (cheap – no downloads)
            items: List[Message]
            if msg.grouped_id:
                items = [m async for m in origin.iter_messages(
                    msg.chat_id, grouped_id=msg.grouped_id, reverse=True
                )]
            else:
                items = [msg]

            # strip items that have no media (Telethon rejects them in albums)
            items = [m for m in items if m.media]
            caption = f"{msg.text or ''}\n\n{_permalink(msg)}".strip()

            try:
                if len(items) == 0:
                    await client.send_message(smart_chat_id, caption, link_preview=False)
                elif len(items) == 1:
                    await client.send_file(smart_chat_id, items[0], caption=caption, link_preview=False)
                else:
                    await client.send_file(smart_chat_id, items, caption=caption, link_preview=False)
                logger.info("➡️  smart fwd %s id=%s (%d items)",
                            getattr(msg.chat, "username", "?"), msg.id, len(items))
            except errors.FloodWaitError as e:
                logger.warning("⏳ flood‑wait forward (%s s)", e.seconds)
                await asyncio.sleep(e.seconds)
            except Exception:
                logger.exception("smart fwd failed id=%s", msg.id)

        return _smart

    # keep a mapping so scanners can call the handler
    smart_handler_map: Dict[TelegramClient, Callable[[Message], asyncio.Task]] = {}

    # ── resilient scanner task builder ───────────────────────────────────
    def _spawn_scanner(cli: TelegramClient, chan_usernames: List[str]):
        async def _scanner():
            """Continuously scan for new history in each channel."""
            logger.debug("scanner starting on %s (chans=%d)", cli.session.filename, len(chan_usernames))
            last_seen: Dict[int, int] = {}
            # initialise cursors to latest message so we don't replay history
            for uname in chan_usernames:
                try:
                    msg = await cli.get_messages(uname, limit=1)
                    if msg:
                        last_seen[await cli.get_peer_id(f"@{uname}")] = msg[0].id
                except Exception:
                    logger.debug("init cursor failed for @%s on %s", uname, cli.session.filename, exc_info=True)

            while True:
                try:
                    for uname in chan_usernames:
                        cid = await cli.get_peer_id(f"@{uname}")
                        async for m in cli.iter_messages(
                            cid,
                            limit=scan_batch_limit,
                            min_id=last_seen.get(cid, 0),
                            reverse=True,
                        ):
                            last_seen[cid] = m.id
                            if uname in arab_channels:
                                await _arab(m)
                            else:
                                await smart_handler_map[cli](m)
                        await asyncio.sleep(pacing)
                except Exception:
                    logger.exception("scanner crash on %s – restarting in 5 s", cli.session.filename)
                    await asyncio.sleep(5)
        asyncio.create_task(_scanner(), name=f"scanner-{cli.session.filename}")

    # ── wire everything for each client in pool ──────────────────────────
    for idx, (cli, is_primary) in enumerate(pool):
        my_arab = arab_parts[idx]
        my_smart = smart_parts[idx]

        await _ensure_join(cli, my_arab + my_smart)

        if my_arab:
            ids = [await cli.get_peer_id(f"@{u}") for u in my_arab]
            cli.add_event_handler(_arab, events.NewMessage(chats=ids))
            logger.info("📡 arab realtime on %s (%d chans)", cli.session.filename, len(my_arab))

        if my_smart:
            handler = _smart_factory(cli)
            smart_handler_map[cli] = handler
            ids = [await cli.get_peer_id(f"@{u}") for u in my_smart]
            cli.add_event_handler(handler, events.NewMessage(chats=ids))
            logger.info("📡 smart realtime on %s (%d chans)", cli.session.filename, len(my_smart))
        else:
            smart_handler_map[cli] = lambda _m: None  # type: ignore[assignment]

        _spawn_scanner(cli, my_arab + my_smart)
        logger.info("🚀 scanner task launched for %s", cli.session.filename)

    logger.info(
        "✅ listener ready – %d clients | %d arab chans | %d smart chans",
        n_clients,
        len(arab_channels),
        len(smart_channels or []),
    )
