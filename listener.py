#!/usr/bin/env python3
from __future__ import annotations
"""listener.py (multi-account)
--------------------------------
* Supports one **main** Telegram client (used for sending) and an arbitrary
    number of **reader** accounts taken from the env‑var ``TG_READERS_JSON``.
* Distributes arab/smart channels round‑robin across the pool so reading
    throughput scales with the number of accounts.
* Write operations (summaries, forwards) still go through the main client
    only, so rate‑limits on sending stay unchanged.
"""

import asyncio
import hashlib
import json
import logging
import os
import re
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import (Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple)

from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

logger = logging.getLogger("listener")
logging.basicConfig(level=logging.DEBUG)

# ---------------------------------------------------------------------------
# Constants & helpers (unchanged from previous versions)
# ---------------------------------------------------------------------------
URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST: Set[str] = {
        "צבע אדום",
        "גרם",
        "היכנסו למרחב המוגן",
        "חדירת כלי טיס עוין",
}

_DUP_TEXT_CACHE: Deque[str] = deque(maxlen=500)
_RECENT_MEDIA: Deque[str] = deque(maxlen=2_000)

@dataclass
class MessageInfo:
        text: str
        link: str
        channel: str
        media_id: Optional[str]

# ── low‑level helpers ───────────────────────────────────────────────────────

def _clean_text(t: str) -> str:
        cleaned = re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()
        logger.debug("Cleaned text: %s", cleaned)
        return cleaned


def _blocked(t: str) -> bool:
        is_blocked = any(k in t for k in BLOCKLIST)
        if is_blocked:
                logger.debug("Blocked text detected: %s", t)
        return is_blocked


async def _media_sha(msg: Message) -> Optional[str]:
        if not isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                return None
        try:
                raw: bytes = await msg.download_media(bytes)  # type: ignore[arg-type]
                sha = hashlib.sha256(raw[:262144]).hexdigest()
                logger.debug("Calculated media sha: %s for msg id: %s", sha, msg.id)
                return sha
        except Exception as ex:
                logger.error("Error calculating media sha for msg id %s: %s", msg.id, ex)
                return None


def _permalink(msg: Message) -> str:
        if getattr(msg.chat, "username", None):
                link = f"https://t.me/{msg.chat.username}/{msg.id}"
                logger.debug("Generated permalink: %s", link)
                return link
        return ""


def _is_dup_text(text: str) -> bool:
        h = hashlib.sha1(text.encode()).hexdigest()
        if h in _DUP_TEXT_CACHE:
                logger.debug("Duplicate text detected: %s", text)
                return True
        _DUP_TEXT_CACHE.append(h)
        return False


def _dedup_key(msg: Message) -> str:
        if msg.grouped_id:
                key = f"album:{msg.grouped_id}"
        elif msg.photo:
                key = f"photo:{msg.photo.id}"
        elif msg.document:
                key = f"doc:{msg.document.id}"
        else:
                key = f"text:{hashlib.sha1((msg.text or '').strip().encode()).hexdigest()}"
        logger.debug("Dedup key for msg id %s: %s", msg.id, key)
        return key


async def _collect_album(cli: TelegramClient, message: Message) -> List[Message]:
        if not message.grouped_id:
                return [message]
        album: List[Message] = [message]
        async for m in cli.iter_messages(message.chat_id, offset_id=message.id, reverse=True, limit=20):
                if m.grouped_id != message.grouped_id:
                        break
                album.insert(0, m)
        async for m in cli.iter_messages(message.chat_id, offset_id=message.id, limit=20):
                if m.grouped_id != message.grouped_id:
                        break
                album.append(m)
        sorted_album = [m for _, m in sorted({m.id: m for m in album}.items())]
        logger.debug("Collected album with %d messages for grouped_id %s", len(sorted_album), message.grouped_id)
        return sorted_album


async def _get_message_link(cli: TelegramClient, chat_id: int, msg_id: int) -> str:
        try:
                ent = await cli.get_entity(chat_id)
                if (u := getattr(ent, "username", None)):
                        link = f"https://t.me/{u}/{msg_id}"
                        logger.debug("Message link from get_message_link: %s", link)
                        return link
        except Exception as e:
                logger.error("Error getting message link for chat_id %s, msg_id %s: %s", chat_id, msg_id, e)
        return ""

# ---------------------------------------------------------------------------
# Public: init_listeners
# ---------------------------------------------------------------------------
async def init_listeners(
        *,
        client: TelegramClient,               # main client (sending)
        arab_channels: Sequence[str],
        smart_channels: Sequence[str] | None,
        batch_push: Callable[[MessageInfo], "asyncio.Future | asyncio.Task | None"],
        smart_chat_id: int | None = None,
        # tuning
        max_req_per_min: int = 18,
        scan_batch_limit: int = 100,
        round_gap: int = 300,
):
        """Spin up listeners on the main client **plus optional readers**.

        Reader accounts are supplied via env‑var ``TG_READERS_JSON`` containing
        a JSON array with ``session``, ``api_id``, ``api_hash`` and ``phone``.
        """

        logger.info("Initialising listeners...")

        # ── 0. build client pool ─────────────────────────────────────────────
        pool: List[Tuple[TelegramClient, bool]] = [(client, True)]  # (cli, is_main)

        try:
                reader_cfgs = json.loads(os.getenv("TG_READERS_JSON", "[]"))
        except json.JSONDecodeError:
                reader_cfgs = []
                logger.warning("TG_READERS_JSON malformed – ignoring")

        for cfg in reader_cfgs:
                try:
                        cli = TelegramClient(cfg["session"], cfg["api_id"], cfg["api_hash"],
                                                                 connection_retries=-1, retry_delay=5, timeout=10)
                        await cli.start(phone=lambda: cfg.get("phone", ""))
                        pool.append((cli, False))
                        logger.info("Reader %s connected", cfg["session"])
                except Exception as exc:
                        logger.error("Failed to start reader %s – %s", cfg.get("session"), exc)

        n_clients = len(pool)
        logger.info("listener: %d clients active (1 main + %d readers)", n_clients, n_clients - 1)

        # ── 1. distribute channels ───────────────────────────────────────────
        def _split(lst: Sequence[str]) -> List[List[str]]:
                return [list(lst[i::n_clients]) for i in range(n_clients)]

        arab_chunks  = _split(arab_channels)
        smart_chunks = _split(smart_channels or [])

        request_spacing = 60 / max_req_per_min

        # ── common handlers ──────────────────────────────────────────────────
        async def _process_arab(msg: Message):
                if _blocked(msg.text or "") and not msg.media:
                        logger.debug("Message %s blocked due to filtered content", msg.id)
                        return
                txt = _clean_text(msg.text or "")
                if _is_dup_text(txt):
                        return
                msg_info = MessageInfo(txt, _permalink(msg), getattr(msg.chat, "username", ""), await _media_sha(msg))
                logger.debug("Processing arab message id %s: %s", msg.id, msg_info)
                await batch_push(msg_info)

        def _smart_handler_factory(origin_cli: TelegramClient):
                async def _smart(msg: Message):
                        if not smart_chat_id or msg.out or msg.via_bot_id:
                                logger.debug("Skipping smart message id %s due to conditions", msg.id)
                                return
                        key = _dedup_key(msg)
                        if key in _RECENT_MEDIA:
                                logger.debug("Smart message id %s skipped as duplicate", msg.id)
                                return
                        _RECENT_MEDIA.append(key)
                        try:
                                link = await _get_message_link(origin_cli, msg.chat_id, msg.id)
                                caption = f"{msg.text or ''}\n\n{link}".strip()
                                items = await _collect_album(origin_cli, msg)
                                if msg.grouped_id and msg.id != min(i.id for i in items):
                                        logger.debug("Smart message id %s skipped (not first in album)", msg.id)
                                        return
                                if any(i.media for i in items):
                                        logger.debug("Forwarding file(s) from smart message id %s", msg.id)
                                        await client.send_file(smart_chat_id, items, caption=caption, link_preview=False)
                                else:
                                        logger.debug("Forwarding text from smart message id %s", msg.id)
                                        await client.send_message(smart_chat_id, caption or link, link_preview=False)
                        except Exception as exc:
                                logger.error("smart fwd error for msg id %s: %s", msg.id, exc)
                return _smart

        _smart_map: Dict[TelegramClient, Callable[[Message], asyncio.Future]] = {}

        # ── 2. per‑client setup ──────────────────────────────────────────────
        async def _ensure_join(cli: TelegramClient, names: List[str]):
                current = {getattr(d.entity, "username", "").lower() for d in await cli.get_dialogs() if getattr(d.entity, "username", None)}
                for n in names:
                        if n.lower() in current:
                                continue
                        try:
                                await cli(JoinChannelRequest(n))
                                logger.info("%s joined @%s", cli.session.filename, n)
                        except errors.FloodWaitError as e:
                                logger.warning("FloodWaitError on join @%s for %s, sleeping for %s seconds", n, cli.session.filename, e.seconds)
                                await asyncio.sleep(e.seconds)
                        except Exception as exc:
                                logger.error("join error @%s on %s: %s", n, cli.session.filename, exc)

        async def _scanner(cli: TelegramClient, my_names: List[str]):
                logger.debug("Scanner started for client %s on channels: %s", cli.session.filename, my_names)
                last_seen: Dict[int, int] = defaultdict(int)
                while True:
                        for uname in my_names:
                                try:
                                        cid = await cli.get_peer_id(f"@{uname}")
                                        logger.debug("Scanning channel %s (id: %s)", uname, cid)
                                        async for msg in cli.iter_messages(cid, limit=scan_batch_limit, min_id=last_seen[cid], reverse=True):
                                                last_seen[cid] = msg.id
                                                if uname in arab_channels:
                                                        await _process_arab(msg)
                                                else:
                                                        await _smart_map[cli](msg)
                                except errors.FloodWaitError as e:
                                        logger.warning("FloodWaitError scanning %s on %s, sleeping for %s seconds", uname, cli.session.filename, e.seconds)
                                        await asyncio.sleep(e.seconds)
                                except Exception as exc:
                                        logger.error("scan error %s on %s: %s", uname, cli.session.filename, exc)
                                await asyncio.sleep(request_spacing)
                        logger.debug("Scanner loop complete for client %s, waiting for round_gap: %s seconds", cli.session.filename, round_gap)
                        await asyncio.sleep(round_gap)

        for idx, (cli, _) in enumerate(pool):
                my_arabs  = arab_chunks[idx]
                my_smarts = smart_chunks[idx]
                await _ensure_join(cli, my_arabs + my_smarts)

                if my_arabs:
                        ids = [await cli.get_peer_id(f"@{u}") for u in my_arabs]
                        cli.add_event_handler(_process_arab, events.NewMessage(chats=ids))
                        logger.debug("Registered arab handler for client %s on channels: %s", cli.session.filename, my_arabs)
                if my_smarts:
                        s_handler = _smart_handler_factory(cli)
                        _smart_map[cli] = s_handler
                        ids = [await cli.get_peer_id(f"@{u}") for u in my_smarts]
                        cli.add_event_handler(s_handler, events.NewMessage(chats=ids))
                        logger.debug("Registered smart handler for client %s on channels: %s", cli.session.filename, my_smarts)
                else:
                        _smart_map[cli] = lambda _msg: None  # no‑op

                # background backlog scanner for this client
                asyncio.create_task(_scanner(cli, my_arabs + my_smarts), name=f"scanner-{cli.session.filename}")
                logger.debug("Launched scanner task for client %s", cli.session.filename)

        logger.info("listener initialised – scanners running on %d clients", n_clients)
