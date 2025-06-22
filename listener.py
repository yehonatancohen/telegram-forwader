#!/usr/bin/env python3
"""
Fully‑instrumented Telegram listener
===================================

* **Two independent jobs**
  * **Arab channels**  → push to an external summariser through `batch_push()`.
  * **Smart channels** → realtime forward (albums supported) to a single chat.
* **Zero media downloads** – we stream/forward only.
* **Rich DEBUG‑level logging** across every step so we can reconstruct any issue.
* **Crash‑proof scanners** – unexpected errors are logged and the scanner restarts
  after a brief back‑off.
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
from typing import Awaitable, Callable, Deque, Dict, List, Optional, Sequence, Tuple

from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message

# ───────────────────────── configuration / logging ────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    format="%(asctime)s %(levelname)s | %(message)s", level=getattr(logging, LOG_LEVEL)
)
logger = logging.getLogger("listener")

URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST = {
    "צבע אדום",
    "גרם",
    "היכנסו למרחב המוגן",
    "חדירת כלי טיס עוין",
}
_DUP_CACHE: Deque[str] = deque(maxlen=500)
_RECENT_MEDIA: Deque[str] = deque(maxlen=2_000)
START_TS = time.time()  # ignore messages created before the bot was started

# ──────────────────────────────── models ─────────────────────────────────────
@dataclass(slots=True)
class MessageInfo:
    text: str
    link: str
    channel: str

# ────────────────────────────── helpers ───────────────────────────────────────

def _clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()


def _is_blocked(t: str) -> bool:
    return any(k in t for k in BLOCKLIST)


def _is_dup(text: str) -> bool:
    dig = hashlib.sha1(text.encode()).hexdigest()
    if dig in _DUP_CACHE:
        return True
    _DUP_CACHE.append(dig)
    return False


def _permalink(msg: Message) -> str:
    if uname := getattr(msg.chat, "username", None):
        return f"https://t.me/{uname}/{msg.id}"
    return ""


def _dedup_key(msg: Message) -> str:
    if msg.grouped_id:
        return f"album:{msg.grouped_id}"
    if msg.photo:
        return f"photo:{msg.photo.id}"
    if msg.document:
        return f"doc:{msg.document.id}"
    return f"text:{hashlib.sha1((msg.text or '').encode()).hexdigest()}"

async def _maybe_await(x):
    """Helper so we can accept both sync & async batch_push callables."""
    if asyncio.iscoroutine(x):
        await x

async def _get_album(origin: TelegramClient, m: Message) -> List[Message]:
    """Collect all messages with the same grouped_id around *m* (no downloads)."""
    if not m.grouped_id:
        return [m]

    msgs: List[Message] = [m]

    async for prev in origin.iter_messages(m.chat_id, reverse=True, offset_id=m.id, limit=20):
        if prev.grouped_id != m.grouped_id or prev.date.timestamp() < START_TS:
            break
        msgs.insert(0, prev)

    async for nxt in origin.iter_messages(m.chat_id, offset_id=m.id, limit=20):
        if nxt.grouped_id != m.grouped_id or nxt.date.timestamp() < START_TS:
            break
        msgs.append(nxt)

    # de‑dup by id – album pages can overlap when Telegram inserts sponsored msgs
    return [x for _, x in sorted({x.id: x for x in msgs}.items())]

async def _ensure_join(cli: TelegramClient, chans: List[str]):
    """Join channels that the account hasn't joined yet."""
    dialogs = await cli.get_dialogs()
    joined = {getattr(d.entity, "username", "").lower() for d in dialogs if getattr(d.entity, "username", None)}
    for ch in chans:
        if ch.lower() in joined:
            continue
        try:
            await cli(JoinChannelRequest(ch))
            logger.info("➕ %s joined @%s", cli.session.filename, ch)
        except errors.FloodWaitError as e:
            logger.warning("⏳ flood‑wait join @%s on %s – sleeping %ss", ch, cli.session.filename, e.seconds)
            await asyncio.sleep(e.seconds)
        except Exception as exc:
            logger.exception("❌ join failed @%s on %s", ch, cli.session.filename, exc)

# ──────────────────────────── core entrypoint ───────────────────────────────
async def init_listeners(
    *,
    client: TelegramClient,  # main account – used for sending
    arab_channels: Sequence[str],
    smart_channels: Sequence[str] | None,
    batch_push: Callable[[MessageInfo], Awaitable[None] | None],
    smart_chat_id: int | None = None,
    max_req_per_min: int = 18,
    scan_batch_limit: int = 100,
):
    """Attach realtime handlers & background scanners on **all** accounts."""

    # 0. build reader pool ------------------------------------------------------------------
    pool: List[Tuple[TelegramClient, bool]] = [(client, True)]
    for cfg in json.loads(os.getenv("TG_READERS_JSON", "[]")):
        try:
            cli = TelegramClient(cfg["session"], cfg["api_id"], cfg["api_hash"], connection_retries=-1, retry_delay=5, timeout=10)
            await cli.start(phone=lambda: cfg.get("phone", ""))
            if not await cli.is_user_authorized():
                logger.critical("No valid .session for %s – aborting", cfg["session"])
                sys.exit(1)
            pool.append((cli, False))
            logger.info("🔌 reader %s connected", cfg["session"])
        except Exception:  # pragma: no cover – startup failures must be logged verbosely
            logger.exception("Reader startup failed for %s", cfg.get("session"))

    n_clients = len(pool)
    logger.info("👥 %d clients active", n_clients)

    # 1. distribute channels evenly ---------------------------------------------------------
    def _split(src: Sequence[str]) -> List[List[str]]:
        return [list(src[i::n_clients]) for i in range(n_clients)]

    arab_parts = _split(arab_channels)
    smart_parts = _split(smart_channels or [])
    pacing = 60 / max_req_per_min

    # 2. create handlers --------------------------------------------------------------------
    async def _arab(evt: events.NewMessage.Event):
        msg = evt.message if hasattr(evt, "message") else evt  # type: ignore[assignment]
        if not isinstance(msg, Message):
            logger.warning("⚠️  _arab received non‑Message %s (%s)", msg, type(msg))
            return
        if msg.date.timestamp() < START_TS:
            return
        if _is_blocked(msg.text or "") and not msg.media:
            return
        text = _clean_text(msg.text or "")
        if _is_dup(text):
            return
        info = MessageInfo(text=text, link=_permalink(msg), channel=getattr(msg.chat, "username", ""))
        logger.debug("⬇️  arab %s | %s", info.channel, info.text[:80])
        await _maybe_await(batch_push(info))

    def _make_smart_handler(origin: TelegramClient):
        async def _smart(evt: events.NewMessage.Event):
            nonlocal origin
            msg = evt.message if hasattr(evt, "message") else evt  # type: ignore[assignment]
            if not isinstance(msg, Message):
                logger.warning("⚠️  _smart received non‑Message %s (%s)", msg, type(msg))
                return
            if msg.out or msg.via_bot_id or msg.date.timestamp() < START_TS:
                return
            if not smart_chat_id:
                return
            dedup = _dedup_key(msg)
            if dedup in _RECENT_MEDIA:
                return
            _RECENT_MEDIA.append(dedup)

            album = await _get_album(origin, msg)
            album = [m for m in album if m.media]  # ignore pure‑text elements inside albums

            link = _permalink(msg)
            caption = f"{msg.text or ''}\n\n{link}".strip()

            try:
                if not album:
                    # nothing to send – fallback to text only
                    await client.send_message(smart_chat_id, caption, link_preview=False)
                elif len(album) == 1:
                    await client.send_file(smart_chat_id, album[0], caption=caption, link_preview=False)
                else:
                    await client.send_file(smart_chat_id, album, caption=caption, link_preview=False)
                logger.info("➡️  smart fwd %s id=%s (%d items)", getattr(msg.chat, "username", "?"), msg.id, len(album))
            except errors.FloodWaitError as e:
                logger.warning("⏳ flood‑wait (%ss) while fwd %s id=%s", e.seconds, getattr(msg.chat, "username", "?"), msg.id)
                await asyncio.sleep(e.seconds)
            except errors.MediaEmptyError:
                # Some media (e.g., games) can't be sent as files – fallback to forward_messages
                try:
                    await client.forward_messages(smart_chat_id, msg)
                    logger.info("➡️  smart fwd (fallback) %s id=%s", getattr(msg.chat, "username", "?"), msg.id)
                except Exception:
                    logger.exception("smart fwd fallback failed id=%s", msg.id)
            except Exception:
                logger.exception("smart fwd failed id=%s", msg.id)
        return _smart

    # 3. per‑client wiring ------------------------------------------------------------------
    smart_handler_map: Dict[TelegramClient, Callable[[events.NewMessage.Event], Awaitable[None]]] = {}

    async def _scanner(cli: TelegramClient, chans: List[str]):
        """Background poller to catch anything missed by realtime updates."""
        last_seen: Dict[int, int] = {}
        for uname in chans:
            try:
                msgs = await cli.get_messages(uname, limit=1)
                if msgs:
                    last_seen[await cli.get_peer_id(f"@{uname}")] = msgs[0].id
            except Exception:
                logger.exception("init cursor fail %s on %s", uname, cli.session.filename)

        while True:
            try:
                for uname in chans:
                    cid = await cli.get_peer_id(f"@{uname}")
                    async for m in cli.iter_messages(cid, min_id=last_seen.get(cid, 0), reverse=True, limit=scan_batch_limit):
                        last_seen[cid] = m.id
                        if m.date.timestamp() < START_TS:
                            continue
                        if uname in arab_channels:
                            await _arab(m)  # type: ignore[arg-type]
                        else:
                            await smart_handler_map[cli](m)  # type: ignore[arg-type]
                    await asyncio.sleep(pacing)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scanner crash on %s – restarting in 5 s", cli.session.filename)
                await asyncio.sleep(5)

    # 4. activate each client ---------------------------------------------------------------
    for idx, (cli, is_main) in enumerate(pool):
        my_arab = arab_parts[idx]
        my_smart = smart_parts[idx]

        await _ensure_join(cli, my_arab + my_smart)

        if my_arab:
            ids = [await cli.get_peer_id(f"@{u}") for u in my_arab]
            cli.add_event_handler(_arab, events.NewMessage(chats=ids))
            logger.info("📡 arab realtime on %s (%d chans)", cli.session.filename, len(my_arab))

        if my_smart:
            handler = _make_smart_handler(cli)
            smart_handler_map[cli] = handler
            ids = [await cli.get_peer_id(f"@{u}") for u in my_smart]
            cli.add_event_handler(handler, events.NewMessage(chats=ids))
            logger.info("📡 smart realtime on %s (%d chans)", cli.session.filename, len(my_smart))
        else:
            smart_handler_map[cli] = lambda _ev: asyncio.sleep(0)  # noop

        # launch scanner
        asyncio.create_task(_scanner(cli, my_arab + my_smart), name=f"scanner-{cli.session.filename}")
        logger.info("🚀 scanner task launched for %s", cli.session.filename)

    logger.info(
        "✅ listener ready – %d clients | %d arab chans | %d smart chans",
        n_clients,
        len(arab_channels),
        len(smart_channels or []),
    )
