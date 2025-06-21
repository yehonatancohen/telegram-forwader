#!/usr/bin/env python3
# listener.py – multi-account readers + smart forwarder
# -----------------------------------------------------
# * Main client = sender; N “readers” defined in $TG_READERS_JSON
# * Ignores history before START_TS
# * Filters phantom album items; falls back to forward if send_file fails

from __future__ import annotations

import asyncio, hashlib, json, logging, os, re, sys, time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Deque, Dict, List, Optional, Sequence, Set, Tuple

from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto

# ─── logging ──────────────────────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO))
logger = logging.getLogger("listener")

# ─── constants / caches ───────────────────────────────────────────────────
URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST: Set[str] = {
    "צבע אדום", "גרם", "היכנסו למרחב המוגן", "חדירת כלי טיס עוין",
}
_DUP_TEXT:   Deque[str] = deque(maxlen=500)
_DUP_MEDIA:  Deque[str] = deque(maxlen=2_000)
START_TS = time.time()          # ignore messages earlier than this

@dataclass
class MessageInfo:
    text: str
    link: str
    channel: str
    media_id: Optional[str]

# ─── tiny helpers ─────────────────────────────────────────────────────────
def _clean(t: str) -> str:
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()

def _txt_dup(t: str) -> bool:
    h = hashlib.sha1(t.encode()).hexdigest()
    if h in _DUP_TEXT:
        return True
    _DUP_TEXT.append(h)
    return False

def _media_key(m: Message) -> str:
    if m.grouped_id: return f"album:{m.grouped_id}"
    if m.photo:      return f"photo:{m.photo.id}"
    if m.document:   return f"doc:{m.document.id}"
    return f"text:{hashlib.sha1((m.text or '').encode()).hexdigest()}"

async def _media_sha(m: Message) -> Optional[str]:
    if not isinstance(m.media, (MessageMediaPhoto, MessageMediaDocument)):
        return None
    try:
        raw = await m.download_media(bytes)  # first 256 KiB is enough
        return hashlib.sha256(raw[:262144]).hexdigest()
    except Exception:
        return None

async def _link(cli: TelegramClient, chat_id: int, msg_id: int) -> str:
    try:
        e = await cli.get_entity(chat_id)
        if (u := getattr(e, "username", None)):
            return f"https://t.me/{u}/{msg_id}"
    except Exception:
        pass
    return ""

async def _album(cli: TelegramClient, m: Message) -> List[Message]:
    """Return *all* siblings of an album, sans dups, sans history rewind."""
    if not m.grouped_id:
        return [m]
    res: List[Message] = [m]
    async for prev in cli.iter_messages(m.chat_id, offset_id=m.id,
                                        reverse=True, limit=20):
        if prev.grouped_id != m.grouped_id or prev.date.timestamp() < START_TS:
            break
        res.insert(0, prev)
    async for nxt in cli.iter_messages(m.chat_id, offset_id=m.id, limit=20):
        if nxt.grouped_id != m.grouped_id or nxt.date.timestamp() < START_TS:
            break
        res.append(nxt)
    # unique & ordered
    return [x for _, x in sorted({x.id: x for x in res}.items())]

# ─── main init_listeners ──────────────────────────────────────────────────
async def init_listeners(
    *,
    client: TelegramClient,               # main (sending) account
    arab_channels: Sequence[str],
    smart_channels: Sequence[str] | None,
    batch_push: Callable[[MessageInfo], "asyncio.Future|asyncio.Task|None"],
    smart_chat_id: int | None = None,
    max_req_per_min: int = 18,
    scan_batch_limit: int = 100,
    round_gap: int = 300,
):
    # 0 ░ build pool -------------------------------------------------------
    pool: List[Tuple[TelegramClient, bool]] = [(client, True)]
    for cfg in json.loads(os.getenv("TG_READERS_JSON", "[]")):
        try:
            cli = TelegramClient(cfg["session"], cfg["api_id"], cfg["api_hash"],
                                 connection_retries=-1, retry_delay=5, timeout=10)
            await cli.start(phone=lambda: cfg.get("phone", ""))
            if not await cli.is_user_authorized():
                logger.warning("%s not authorised – skip", cfg["session"])
                continue
            pool.append((cli, False))
            logger.info("🔌 reader %s connected", cfg["session"])
        except Exception as e:
            logger.error("reader %s failed: %s", cfg.get("session"), e)

    n = len(pool)
    logger.info("👥 %d clients active", n)

    # 1 ░ distribute channels ---------------------------------------------
    def _split(seq: Sequence[str]) -> List[List[str]]:
        return [list(seq[i::n]) for i in range(n)]
    parts_arab, parts_smart = _split(arab_channels), _split(smart_channels or [])
    spacing = 60 / max_req_per_min

    # 2 ░ handlers ---------------------------------------------------------
    async def _arab(msg: Message):
        if msg.date.timestamp() < START_TS or (_clean(msg.text or "") in BLOCKLIST and not msg.media):
            return
        txt = _clean(msg.text or "")
        if _txt_dup(txt):
            return
        await batch_push(MessageInfo(txt, await _link(client, msg.chat_id, msg.id),
                                     getattr(msg.chat, "username", ""), await _media_sha(msg)))

    def _smart_factory(origin: TelegramClient):
        async def _smart(msg: Message):
            if not smart_chat_id or msg.out or msg.via_bot_id or msg.date.timestamp() < START_TS:
                return
            k = _media_key(msg)
            if k in _DUP_MEDIA:
                return
            _DUP_MEDIA.append(k)

            # collect, drop phantom items
            items = [i for i in await _album(origin, msg) if i.media]
            link  = await _link(origin, msg.chat_id, msg.id)
            caption = f"{msg.text or ''}\n\n{link}".strip()

            if not items:                                   # text only
                await client.send_message(smart_chat_id, caption, link_preview=False)
                return

            try:                                            # attempt re-upload
                if len(items) == 1:
                    await client.send_file(smart_chat_id, items[0],
                                           caption=caption, link_preview=False)
                else:
                    await client.send_file(smart_chat_id, items,
                                           caption=caption, link_preview=False)
            except errors.MediaEmptyError:                  # fallback: forward
                await client.forward_messages(smart_chat_id, msg.id, msg.chat_id,
                                              with_my_score=False)
            except Exception as exc:
                logger.warning("smart fwd failed id=%s: %s", msg.id, exc)
        return _smart

    _smap: Dict[TelegramClient, Callable[[Message], asyncio.Future]] = {}

    # helper join ----------------------------------------------------------
    async def _join(cli: TelegramClient, names: List[str]):
        joined = {getattr(d.entity, "username", "").lower()
                  for d in await cli.get_dialogs() if getattr(d.entity, "username", None)}
        for n in names:
            if n.lower() in joined:
                continue
            try:
                await cli(JoinChannelRequest(n))
            except errors.FloodWaitError as e:
                await asyncio.sleep(e.seconds)

    # backlog scanner ------------------------------------------------------
    async def _scanner(cli: TelegramClient, names: List[str]):
        last: Dict[int, int] = {}
        for u in names:                         # initialise cursors
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
                        await (_arab(m) if u in arab_channels else _smap[cli](m))
                except errors.FloodWaitError as e:
                    await asyncio.sleep(e.seconds)
                await asyncio.sleep(spacing)
            await asyncio.sleep(round_gap)

    # 3 ░ wire clients -----------------------------------------------------
    for idx, (cli, _) in enumerate(pool):
        my_arab, my_smart = parts_arab[idx], parts_smart[idx]
        await _join(cli, my_arab + my_smart)

        if my_arab:
            cli.add_event_handler(_arab, events.NewMessage(
                chats=[await cli.get_peer_id(f"@{u}") for u in my_arab]))
        if my_smart:
            h = _smart_factory(cli)
            _smap[cli] = h
            cli.add_event_handler(h, events.NewMessage(
                chats=[await cli.get_peer_id(f"@{u}") for u in my_smart]))
        else:
            _smap[cli] = lambda _m: None

        asyncio.create_task(_scanner(cli, my_arab + my_smart),
                            name=f"scanner-{cli.session.filename}")

    logger.info("✅ listener ready – %d clients | %d arab chans | %d smart chans",
                n, len(arab_channels), len(smart_channels or []))
