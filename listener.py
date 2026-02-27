#!/usr/bin/env python3
"""Message listener — ingests from Telegram channels, routes to pipeline."""

from __future__ import annotations

import asyncio, hashlib, json, logging, os, re, sys, time, unicodedata
from collections import deque
from typing import Awaitable, Callable, Deque, Dict, List, Sequence, Tuple

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message

from models import MessageInfo

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(format="%(asctime)s %(levelname)s | %(message)s",
                    level=getattr(logging, LOG_LEVEL))
logger = logging.getLogger("listener")

URL_RE    = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
# Arabic tashkeel (diacritics) range: U+0610-U+061A, U+064B-U+065F, U+0670
_ARABIC_DIACRITICS = re.compile(r"[\u0610-\u061A\u064B-\u065F\u0670]")
BLOCKLIST = {
    # Hebrew alert phrases
    "צבע אדום", "גרם", "היכנסו למרחב המוגן", "חדירת כלי טיס עוין",
    # Common Arabic spam / non-intel
    "قناتنا الرسمية", "تابعونا على", "رابط الانضمام", "انضموا الينا",
    "للإعلان والتواصل", "مشاركة الرابط",
}


_DUP_CACHE: Deque[str] = deque(maxlen=500)
_RECENT_MEDIA: Deque[str] = deque(maxlen=2_000)
START_TS = time.time()


# ───── Helper utilities ──────────────────────────────────────────────────
def _clean_text(t: str) -> str:
    t = unicodedata.normalize("NFC", t)
    t = _ARABIC_DIACRITICS.sub("", t)          # strip tashkeel
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()

def _is_blocked(t: str) -> bool:
    return any(k in t for k in BLOCKLIST)

def _is_dup(text: str) -> bool:
    dig = hashlib.sha1(text.encode()).hexdigest()
    if dig in _DUP_CACHE:
        return True
    _DUP_CACHE.append(dig)
    return False

def _permalink(m: Message) -> str:
    return (f"https://t.me/{m.chat.username}/{m.id}"
            if getattr(m.chat, "username", None) else "")

def _dedup_key(m: Message) -> str:
    if m.grouped_id:  return f"album:{m.grouped_id}"
    if m.photo:       return f"photo:{m.photo.id}"
    if m.document:    return f"doc:{m.document.id}"
    return f"text:{hashlib.sha1((m.text or '').encode()).hexdigest()}"

async def _maybe_await(x):
    if asyncio.iscoroutine(x):
        await x


# ───── Album helper ──────────────────────────────────────────────────────
async def _get_album(origin: TelegramClient, m: Message) -> List[Message]:
    if not m.grouped_id:
        return [m]
    msgs = [m]
    async for prev in origin.iter_messages(m.chat_id, reverse=True,
                                            offset_id=m.id, limit=20):
        if prev.grouped_id != m.grouped_id or prev.date.timestamp() < START_TS:
            break
        msgs.insert(0, prev)
    async for nxt in origin.iter_messages(m.chat_id, offset_id=m.id, limit=20):
        if nxt.grouped_id != m.grouped_id or nxt.date.timestamp() < START_TS:
            break
        msgs.append(nxt)
    return [x for _, x in sorted({x.id: x for x in msgs}.items())]


# ───── Join helper ───────────────────────────────────────────────────────
async def _ensure_join(cli: TelegramClient, chans: List[str]):
    joined = {
        getattr(d.entity, "username", "").lower()
        for d in await cli.get_dialogs()
        if getattr(d.entity, "username", None)
    }
    for ch in chans:
        if ch.lower() in joined:
            continue
        try:
            await cli(JoinChannelRequest(ch))
            logger.info("joined @%s on %s", ch, cli.session.filename)
        except errors.FloodWaitError as e:
            logger.warning("flood-wait join @%s – sleep %ss", ch, e.seconds)
            await asyncio.sleep(e.seconds)
        except Exception:
            logger.exception("join failed @%s on %s", ch, cli.session.filename)


# ───── Main init_listeners ───────────────────────────────────────────────
async def init_listeners(
    *,
    client: TelegramClient,
    arab_channels: Sequence[str],
    smart_channels: Sequence[str] | None,
    pipeline_push: Callable[[MessageInfo], Awaitable[None] | None],
    smart_chat_id: int | None = None,
    max_req_per_min: int = 18,
    scan_batch_limit: int = 100,
):
    """Wire up real-time event handlers and polling scanners.

    pipeline_push: called for EVERY message (arab and smart) to feed the
                   correlation/authority pipeline.
    smart_chat_id: smart messages are ALSO forwarded directly here (media-aware).
    """

    pool: List[Tuple[TelegramClient, bool]] = [(client, True)]
    for cfg in json.loads(os.getenv("TG_READERS_JSON", "[]")):
        try:
            # Prefer StringSession if provided, fall back to file-based
            session = (StringSession(cfg["session_string"]) if cfg.get("session_string")
                       else cfg["session"])
            cli = TelegramClient(session, cfg["api_id"], cfg["api_hash"],
                                 connection_retries=-1, retry_delay=5, timeout=10)
            await cli.start(phone=lambda: cfg.get("phone", ""))
            if not await cli.is_user_authorized():
                sys.exit("bad session")
            pool.append((cli, False))
            logger.info("reader %s connected", cfg.get("session", "string-session"))
        except Exception:
            logger.exception("reader start fail %s", cfg.get("session"))

    n = len(pool)
    logger.info("%d clients active", n)
    split = lambda src: [list(src[i::n]) for i in range(n)]
    arab_parts = split(arab_channels)
    smart_parts = split(smart_channels or [])
    pacing = 60 / max_req_per_min

    # ─── Inner handlers ──────────────────────────────────────────────────
    async def _arab(evt):
        if isinstance(evt, str):
            txt = _clean_text(evt)
            if not txt or _is_blocked(txt) or _is_dup(txt):
                return
            await _maybe_await(
                pipeline_push(MessageInfo(txt, "", "", None, "arab", time.time()))
            )
            return

        msg = evt.message if isinstance(evt, events.NewMessage.Event) else evt
        if not isinstance(msg, Message):
            return
        if msg.date.timestamp() < START_TS:
            return
        if _is_blocked(msg.text or "") and not msg.media:
            logger.debug("[arab] blocked msg id=%s from @%s",
                         msg.id, getattr(msg.chat, "username", "?"))
            return
        txt = _clean_text(msg.text or "")
        if _is_dup(txt):
            logger.debug("[arab] dedup skip id=%s from @%s",
                         msg.id, getattr(msg.chat, "username", "?"))
            return
        logger.info("[arab] msg id=%s from @%s len=%d",
                    msg.id, getattr(msg.chat, "username", "?"), len(txt))
        mid = None
        if msg.photo:        mid = str(msg.photo.id)
        elif msg.document:   mid = str(msg.document.id)
        elif msg.grouped_id: mid = str(msg.grouped_id)
        await _maybe_await(
            pipeline_push(MessageInfo(txt, _permalink(msg),
                                      getattr(msg.chat, "username", ""),
                                      mid, "arab", time.time()))
        )

    def _mk_smart(origin: TelegramClient):
        async def _smart(evt):
            msg = evt.message if isinstance(evt, events.NewMessage.Event) else evt
            if not isinstance(msg, Message):
                return
            if msg.out or msg.via_bot_id or msg.date.timestamp() < START_TS:
                return

            # Dedup for smart forwarding
            d = _dedup_key(msg)
            if d in list(_RECENT_MEDIA)[:-1]:
                return
            _RECENT_MEDIA.append(d)

            # 1) Direct-forward to smart chat (preserve current behavior)
            if smart_chat_id:
                album = [m for m in await _get_album(origin, msg) if m.media]
                caption = f"{msg.text or ''}\n\n{_permalink(msg)}".strip()
                try:
                    if not album:
                        await client.send_message(smart_chat_id, caption,
                                                  link_preview=False)
                    elif len(album) == 1:
                        await client.send_file(smart_chat_id, album[0],
                                               caption=caption, link_preview=False)
                    else:
                        await client.send_file(smart_chat_id, album,
                                               caption=caption, link_preview=False)
                    logger.info("smart fwd @%s id=%s (%d items)",
                                getattr(msg.chat, "username", "?"),
                                msg.id, len(album))
                except errors.MediaEmptyError:
                    try:
                        await client.forward_messages(smart_chat_id, msg)
                    except Exception:
                        logger.exception("smart fwd fallback failed %s", msg.id)
                except errors.FloodWaitError as e:
                    logger.warning("flood %ss on fwd", e.seconds)
                    await asyncio.sleep(e.seconds)
                except Exception:
                    logger.exception("smart fwd err %s", msg.id)

            # 2) ALSO feed into the pipeline for cross-correlation
            txt = _clean_text(msg.text or "")
            if txt and not _is_blocked(txt):
                logger.info("[smart] msg id=%s from @%s len=%d",
                            msg.id, getattr(msg.chat, "username", "?"), len(txt))
                mid = None
                if msg.photo:        mid = str(msg.photo.id)
                elif msg.document:   mid = str(msg.document.id)
                elif msg.grouped_id: mid = str(msg.grouped_id)
                await _maybe_await(
                    pipeline_push(MessageInfo(txt, _permalink(msg),
                                              getattr(msg.chat, "username", ""),
                                              mid, "smart", time.time()))
                )

        return _smart

    # ─── Per-client wiring & scanners ────────────────────────────────────
    smart_map: Dict[TelegramClient, Callable] = {}

    async def _scanner(cli: TelegramClient, chans: List[str]):
        curs: dict = {}
        for u in chans:
            try:
                m = (await cli.get_messages(u, limit=1))[0]
                curs[await cli.get_peer_id(f"@{u}")] = m.id
            except Exception:
                pass
        while True:
            try:
                for u in chans:
                    cid = await cli.get_peer_id(f"@{u}")
                    async for m in cli.iter_messages(cid, min_id=curs.get(cid, 0),
                                                     reverse=True,
                                                     limit=scan_batch_limit):
                        curs[cid] = m.id
                        if m.date.timestamp() < START_TS:
                            continue
                        if u in arab_channels:
                            await _arab(m)
                        else:
                            await smart_map[cli](m)
                    await asyncio.sleep(pacing)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("scanner crash %s – retry", cli.session.filename)
                await asyncio.sleep(5)

    for i, (cli, _) in enumerate(pool):
        a, s = arab_parts[i], smart_parts[i]
        await _ensure_join(cli, a + s)
        if a:
            ids = [await cli.get_peer_id(f"@{u}") for u in a]
            cli.add_event_handler(_arab, events.NewMessage(chats=ids))
            logger.info("arab realtime on %s (%d)", cli.session.filename, len(a))
        if s:
            h = _mk_smart(cli)
            smart_map[cli] = h
            ids = [await cli.get_peer_id(f"@{u}") for u in s]
            cli.add_event_handler(h, events.NewMessage(chats=ids))
            logger.info("smart realtime on %s (%d)", cli.session.filename, len(s))
        else:
            smart_map[cli] = lambda _ev: asyncio.sleep(0)
        asyncio.create_task(_scanner(cli, a + s),
                            name=f"scanner-{cli.session.filename}")
        logger.info("scanner %s launched", cli.session.filename)

    logger.info("listener ready – %d clients | %d arab | %d smart",
                n, len(arab_channels), len(smart_channels or []))
