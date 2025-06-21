#!/usr/bin/env python3
from __future__ import annotations

import asyncio, hashlib, json, logging, os, re, sys, time
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# ─── logging ──────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("listener")

# ─── constants / helpers ──────────────────────────────────────────────────
URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST: Set[str] = {
    "צבע אדום", "גרם", "היכנסו למרחב המוגן", "חדירת כלי טיס עוין",
}
_DUP_TEXT_CACHE: Deque[str]  = deque(maxlen=500)
_RECENT_MEDIA:   Deque[str]  = deque(maxlen=2_000)
START_TS = time.time()       # ignore anything older than launch

@dataclass
class MessageInfo:
    text: str
    link: str
    channel: str
    media_id: Optional[str]

# ─── tiny utils ───────────────────────────────────────────────────────────
def _clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()

def _blocked(t: str) -> bool:
    return any(k in t for k in BLOCKLIST)

async def _media_sha(msg: Message) -> Optional[str]:
    if not isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
        return None
    try:
        raw: bytes = await msg.download_media(bytes)
        return hashlib.sha256(raw[:262144]).hexdigest()
    except Exception as e:
        logger.debug("media-sha fail id=%s: %s", msg.id, e)
        return None

def _permalink(msg: Message) -> str:
    if getattr(msg.chat, "username", None):
        return f"https://t.me/{msg.chat.username}/{msg.id}"
    return ""

def _is_dup_text(text: str) -> bool:
    h = hashlib.sha1(text.encode()).hexdigest()
    if h in _DUP_TEXT_CACHE:
        return True
    _DUP_TEXT_CACHE.append(h)
    return False

def _dedup_key(msg: Message) -> str:
    if msg.grouped_id:    return f"album:{msg.grouped_id}"
    if msg.photo:         return f"photo:{msg.photo.id}"
    if msg.document:      return f"doc:{msg.document.id}"
    return f"text:{hashlib.sha1((msg.text or '').strip().encode()).hexdigest()}"

async def _collect_album(cli: TelegramClient, m: Message) -> List[Message]:
    if not m.grouped_id:
        return [m]
    res: List[Message] = [m]
    async for prev in cli.iter_messages(m.chat_id, offset_id=m.id, reverse=True, limit=20):
        if prev.grouped_id != m.grouped_id or prev.date.timestamp() < START_TS:
            break
        res.insert(0, prev)
    async for nxt in cli.iter_messages(m.chat_id, offset_id=m.id, limit=20):
        if nxt.grouped_id != m.grouped_id or nxt.date.timestamp() < START_TS:
            break
        res.append(nxt)
    return [x for _, x in sorted({x.id: x for x in res}.items())]

async def _get_link(cli: TelegramClient, chat_id: int, msg_id: int) -> str:
    try:
        ent = await cli.get_entity(chat_id)
        if (u := getattr(ent, "username", None)):
            return f"https://t.me/{u}/{msg_id}"
    except Exception:
        pass
    return ""

# ─── entrypoint ───────────────────────────────────────────────────────────
async def init_listeners(
    *,
    client: TelegramClient,
    arab_channels: Sequence[str],
    smart_channels: Sequence[str] | None,
    batch_push: Callable[[MessageInfo], "asyncio.Future | asyncio.Task | None"],
    smart_chat_id: int | None = None,
    max_req_per_min: int = 18,
    scan_batch_limit: int = 100,
):
    # 0 ░ build pool -------------------------------------------------------
    pool: List[Tuple[TelegramClient, bool]] = [(client, True)]
    for cfg in json.loads(os.getenv("TG_READERS_JSON", "[]")):
        try:
            cli = TelegramClient(cfg["session"], cfg["api_id"], cfg["api_hash"],
                                 connection_retries=-1, retry_delay=5, timeout=10)
            await cli.start(phone=lambda: cfg.get("phone", ""))
            if not await cli.is_user_authorized():
                logger.critical("%s not authorised – check .session", cfg["session"])
                continue
            pool.append((cli, False))
            logger.info("🔌 reader %s connected", cfg["session"])
        except Exception as e:
            logger.error("reader %s failed: %s", cfg.get("session"), e)

    n = len(pool)
    logger.info("👥 %d clients active", n)

    # 1 ░ split channels ---------------------------------------------------
    def _split(seq: Sequence[str]) -> List[List[str]]:
        return [list(seq[i::n]) for i in range(n)]
    arab_parts, smart_parts = _split(arab_channels), _split(smart_channels or [])
    pacing = 60 / max_req_per_min

    # 2 ░ handlers ---------------------------------------------------------
    async def _handle_arab(msg: Message):
        if msg.date.timestamp() < START_TS or (_blocked(msg.text or "") and not msg.media):
            return
        txt = _clean_text(msg.text or "")
        if _is_dup_text(txt):
            return
        await batch_push(MessageInfo(txt, _permalink(msg),
                                     getattr(msg.chat, "username", ""), await _media_sha(msg)))

    def _smart_factory(origin: TelegramClient):
        async def _smart(msg: Message):
            if not smart_chat_id or msg.out or msg.via_bot_id or msg.date.timestamp() < START_TS:
                return
            key = _dedup_key(msg)
            if key in _RECENT_MEDIA:
                return
            _RECENT_MEDIA.append(key)

            items = [i for i in await _collect_album(origin, msg) if i.media]
            link = await _get_link(origin, msg.chat_id, msg.id)
            caption = f"{msg.text or ''}\n\n{link}".strip()

            if not items:                     # text only
                await client.send_message(smart_chat_id, caption, link_preview=False)
                return
            if len(items) == 1:
                await client.send_file(smart_chat_id, items[0], caption=caption, link_preview=False)
            else:
                await client.send_file(smart_chat_id, items,    caption=caption, link_preview=False)
        return _smart

    _smap: Dict[TelegramClient, Callable[[Message], asyncio.Future]] = {}

    # join helper ----------------------------------------------------------
    async def _ensure_join(cli: TelegramClient, names: List[str]):
        joined = {getattr(d.entity, "username", "").lower()
                  for d in await cli.get_dialogs() if getattr(d.entity, "username", None)}
        for n in names:
            if n.lower() in joined:
                continue
            try:
                await cli(JoinChannelRequest(n))
                logger.info("➕ %s joined @%s", cli.session.filename, n)
            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)

    # backlog scanner ------------------------------------------------------
    async def _scanner(cli: TelegramClient, names: List[str]):
        last: Dict[int, int] = {}
        for u in names:
            m = await cli.get_messages(u, limit=1)
            if m:
                last[await cli.get_peer_id(f"@{u}")] = m[0].id
        while True:
            for u in names:
                try:
                    cid = await cli.get_peer_id(f"@{u}")
                    async for m in cli.iter_messages(cid, limit=scan_batch_limit,
                                                     min_id=last.get(cid, 0), reverse=True):
                        if m.date.timestamp() < START_TS:
                            continue
                        last[cid] = m.id
                        await (_handle_arab(m) if u in arab_channels else _smap[cli](m))
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                await asyncio.sleep(pacing)

    # 3 ░ wire clients -----------------------------------------------------
    for idx, (cli, _) in enumerate(pool):
        my_arab, my_smart = arab_parts[idx], smart_parts[idx]
        await _ensure_join(cli, my_arab + my_smart)

        if my_arab:
            cli.add_event_handler(_handle_arab,
                events.NewMessage(chats=[await cli.get_peer_id(f"@{u}") for u in my_arab]))
        if my_smart:
            h = _smart_factory(cli)
            _smap[cli] = h
            cli.add_event_handler(h,
                events.NewMessage(chats=[await cli.get_peer_id(f"@{u}") for u in my_smart]))
        else:
            _smap[cli] = lambda _m: None

        asyncio.create_task(_scanner(cli, my_arab + my_smart), name=f"scanner-{cli.session.filename}")

    logger.info("✅ listener ready – %d clients | %d arab chans | %d smart chans",
                n, len(arab_channels), len(smart_channels or []))
