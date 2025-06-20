#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
import sys
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# ───────────────────────── logging ────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("listener")

# ───────────────────── constants / helpers ────────────────────────────────
URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST: Set[str] = {
    "צבע אדום",
    "גרם",
    "היכנסו למרחב המוגן",
    "חדירת כלי טיס עוין",
}
_DUP_TEXT_CACHE: Deque[str] = deque(maxlen=500)
_RECENT_MEDIA: Deque[str] = deque(maxlen=2_000)

START_TS = time.time()  # bot start timestamp -> ignore older msgs

@dataclass
class MessageInfo:
    text: str
    link: str
    channel: str
    media_id: Optional[str]

# ── tiny utils ────────────────────────────────────────────────────────────

def _clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()


def _blocked(t: str) -> bool:
    return any(k in t for k in BLOCKLIST)


async def _media_sha(msg: Message) -> Optional[str]:
    if not isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
        return None
    try:
        raw: bytes = await msg.download_media(bytes)  # type: ignore[arg-type]
        return hashlib.sha256(raw[:262144]).hexdigest()
    except Exception as e:
        logger.debug("media‑sha failed id=%s: %s", msg.id, e)
        return None


def _permalink(msg: Message) -> str:
    if getattr(msg.chat, "username", None):
        return f"https://t.me/{msg.chat.username}/{msg.id}"
    return ""


def _is_dup_text(text: str) -> bool:
    dig = hashlib.sha1(text.encode()).hexdigest()
    if dig in _DUP_TEXT_CACHE:
        logger.debug("↻ dup‑skip text digest %s", dig[:8])
        return True
    _DUP_TEXT_CACHE.append(dig)
    return False


def _dedup_key(msg: Message) -> str:
    if msg.grouped_id:
        return f"album:{msg.grouped_id}"
    if msg.photo:
        return f"photo:{msg.photo.id}"
    if msg.document:
        return f"doc:{msg.document.id}"
    return f"text:{hashlib.sha1((msg.text or '').strip().encode()).hexdigest()}"


async def _collect_album(cli: TelegramClient, m: Message) -> List[Message]:
    if not m.grouped_id:
        return [m]
    result: List[Message] = [m]
    async for prev in cli.iter_messages(m.chat_id, offset_id=m.id, reverse=True, limit=20):
        if prev.grouped_id != m.grouped_id or prev.date.timestamp() < START_TS:
            break
        result.insert(0, prev)
    async for nxt in cli.iter_messages(m.chat_id, offset_id=m.id, limit=20):
        if nxt.grouped_id != m.grouped_id or nxt.date.timestamp() < START_TS:
            break
        result.append(nxt)
    res_sorted = [x for _, x in sorted({x.id: x for x in result}.items())]
    logger.debug("album %s collected (%d items)", m.grouped_id, len(res_sorted))
    return res_sorted


async def _get_link(cli: TelegramClient, chat_id: int, msg_id: int) -> str:
    try:
        ent = await cli.get_entity(chat_id)
        if (u := getattr(ent, "username", None)):
            return f"https://t.me/{u}/{msg_id}"
    except Exception as e:
        logger.debug("permalink fail chat=%s id=%s: %s", chat_id, msg_id, e)
    return ""

# ─────────────────────── main entrypoint ──────────────────────────────────
async def init_listeners(
    *,
    client: TelegramClient,               # main (sending) account
    arab_channels: Sequence[str],
    smart_channels: Sequence[str] | None,
    batch_push: Callable[[MessageInfo], "asyncio.Future | asyncio.Task | None"],
    smart_chat_id: int | None = None,
    # tuning
    max_req_per_min: int = 18,
    scan_batch_limit: int = 100,
):
    """Attach handlers & scanners on all accounts; ignore pre‑startup history."""

    # 0 ░ build client pool ------------------------------------------------
    pool: List[Tuple[TelegramClient, bool]] = [(client, True)]
    for cfg in json.loads(os.getenv("TG_READERS_JSON", "[]")):
        try:
            cli = TelegramClient(cfg["session"], cfg["api_id"], cfg["api_hash"], connection_retries=-1, retry_delay=5, timeout=10)
            await cli.start(phone=lambda: cfg.get("phone", ""))
            if not await cli.is_user_authorized():
                logger.critical("No valid .session – aborting to avoid hang")
                sys.exit(1)
            pool.append((cli, False))
            logger.info("🔌 reader %s connected", cfg["session"])
        except Exception as e:
            logger.error("reader %s failed: %s", cfg.get("session"), e)

    n = len(pool)
    logger.info("👥 %d clients active", n)

    # 1 ░ distribute channels --------------------------------------------
    def _split(seq: Sequence[str]) -> List[List[str]]:
        return [list(seq[i::n]) for i in range(n)]
    arab_parts, smart_parts = _split(arab_channels), _split(smart_channels or [])

    pacing = 60 / max_req_per_min

    # 2 ░ handlers --------------------------------------------------------
    async def _handle_arab(msg: Message):
        if msg.date.timestamp() < START_TS or (_blocked(msg.text or "") and not msg.media):
            return
        clean = _clean_text(msg.text or "")
        if _is_dup_text(clean):
            return
        logger.debug("⬇️  arab %s | %s", getattr(msg.chat, "username", "?"), clean[:60])
        await batch_push(MessageInfo(clean, _permalink(msg), getattr(msg.chat, "username", ""), await _media_sha(msg)))

    def _smart_factory(origin: TelegramClient):
        async def _smart(msg: Message):
            if not smart_chat_id or msg.out or msg.via_bot_id or msg.date.timestamp() < START_TS:
                return
            key = _dedup_key(msg)
            if key in _RECENT_MEDIA:
                return
            _RECENT_MEDIA.append(key)
            items = await _collect_album(origin, msg)

            # ▶︎ NEW: drop album elements that have no media (Telegram bug-workaround)
            items_media = [i for i in items if i.media]
            if not items_media:                       # nothing left; just text
                await client.send_message(smart_chat_id, caption, link_preview=False)
                return

            caption = f"{msg.text or ''}\\n\\n{await _get_link(origin, ...)}".strip()

            if len(items_media) == 1:                 # single photo/doc
                await client.send_file(smart_chat_id, items_media[0],
                                    caption=caption, link_preview=False)
            else:                                     # true album
                await client.send_file(smart_chat_id, items_media,
                                    caption=caption, link_preview=False)

            logger.info("➡️  smart fwd from %s id=%s (%d items)",
                        getattr(msg.chat, "username", "?"), msg.id, len(items_media))

        return _smart

    _smap: Dict[TelegramClient, Callable[[Message], asyncio.Future]] = {}

    # helper join ---------------------------------------------------------
    async def _ensure_join(cli: TelegramClient, chans: List[str]):
        joined = {getattr(d.entity, "username", "").lower() for d in await cli.get_dialogs() if getattr(d.entity, "username", None)}
        for ch in chans:
            if ch.lower() in joined:
                continue
            try:
                await cli(JoinChannelRequest(ch))
                logger.info("➕ %s joined @%s", cli.session.filename, ch)
            except errors.FloodWaitError as e:
                logger.warning("⏳ flood‑wait join @%s on %s – sleeping %s s", ch, cli.session.filename, e.seconds)
                await asyncio.sleep(e.seconds)
            except Exception as exc:
                logger.error("❌ join failed @%s on %s: %s", ch, cli.session.filename, exc)

    # backlog scanner -----------------------------------------------------
    async def _scanner(cli: TelegramClient, chans: List[str]):
        # start from the *latest* message at startup — no history rewind
        last_seen: Dict[int, int] = {}
        for uname in chans:
            try:
                msg = await cli.get_messages(uname, limit=1)
                if msg:
                    last_seen[await cli.get_peer_id(f"@{uname}")] = msg[0].id
            except Exception as exc:
                logger.debug("init cursor failed %s on %s: %s", uname, cli.session.filename, exc)

        while True:
            for uname in chans:
                try:
                    cid = await cli.get_peer_id(f"@{uname}")
                    async for m in cli.iter_messages(cid, limit=scan_batch_limit, min_id=last_seen.get(cid, 0), reverse=True):
                        if m.date.timestamp() < START_TS:
                            continue
                        last_seen[cid] = m.id
                        if uname in arab_channels:
                            await _handle_arab(m)
                        else:
                            await _smap[cli](m)
                except errors.FloodWaitError as e:
                    logger.warning("⏳ flood‑wait history on %s (%s s)", cli.session.filename, e.seconds)
                    await asyncio.sleep(e.seconds)
                except Exception as exc:
                    logger.error("scan err %s on %s: %s", uname, cli.session.filename, exc)
                await asyncio.sleep(pacing)

    # 3 ░ wire clients ----------------------------------------------------
    for idx, (cli, _) in enumerate(pool):
        my_arab = arab_parts[idx]
        my_smart = smart_parts[idx]

        await _ensure_join(cli, my_arab + my_smart)

        if my_arab:
            ids = [await cli.get_peer_id(f"@{u}") for u in my_arab]
            cli.add_event_handler(_handle_arab, events.NewMessage(chats=ids))
            logger.info("📡 realtime handler (arab) on %s (%d chans)", cli.session.filename, len(my_arab))

        if my_smart:
            handler = _smart_factory(cli)
            _smap[cli] = handler
            ids = [await cli.get_peer_id(f"@{u}") for u in my_smart]
            cli.add_event_handler(handler, events.NewMessage(chats=ids))
            logger.info("📡 realtime handler (smart) on %s (%d chans)", cli.session.filename, len(my_smart))
        else:
            _smap[cli] = lambda _m: None  # no‑op

        asyncio.create_task(_scanner(cli, my_arab + my_smart), name=f"scanner-{cli.session.filename}")
        logger.info("🚀 scanner started for %s (%d chans)", cli.session.filename, len(my_arab)+len(my_smart))

    logger.info("✅ listener ready – %d clients | %d arab chans | %d smart chans", n, len(arab_channels), len(smart_channels or []))
