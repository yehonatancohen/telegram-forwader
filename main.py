#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, List, Optional, Set

from deep_translator import GoogleTranslator
from dotenv import load_dotenv
from langdetect import detect
from prometheus_client import Counter as PmCounter, start_http_server
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.types import Message, MessageMediaDocument, MessageMediaPhoto
from transformers import pipeline, Pipeline

# -------------------------
# Configuration & Logging
# -------------------------
DEV = False
config_file = "config_dev.env" if DEV else "config.env"
load_dotenv(dotenv_path=Path(config_file))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger("arab-ai-bot")
for noisy in ("telethon", "httpx", "deep_translator"):
    logging.getLogger(noisy).setLevel(logging.WARNING)

# ----------------
# Environment vars
# ----------------
API_ID = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE = os.getenv("PHONE_NUMBER")
SESSION = os.getenv("SESSION_NAME", "arab-ai")
ARABS_SUMMARY_OUT = int(os.getenv("ARABS_SUMMARY_OUT", "0"))
SMART_CHAT = int(os.getenv("SMART_CHAT", "0"))  # optional

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "12"))
MAX_BATCH_AGE = int(os.getenv("MAX_BATCH_AGE", "300"))
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "30"))
MEDIA_THRESHOLD = int(os.getenv("MEDIA_THRESHOLD", "3"))
HASH_CACHE_SIZE = int(os.getenv("HASH_CACHE_SIZE", "500"))
PROM_PORT = int(os.getenv("PROM_PORT", "9000"))

SOURCES_FILE = Path(os.getenv("ARAB_SOURCES_FILE", "arab_channels.txt"))
SMART_SOURCES_FILE = Path(os.getenv("SMART_SOURCES_FILE", "smart_channels.txt"))

required = [("TELEGRAM_API_ID", API_ID), ("TELEGRAM_API_HASH", API_HASH), ("PHONE_NUMBER", PHONE), ("ARABS_SUMMARY_OUT", ARABS_SUMMARY_OUT)]
for n, v in required:
    if not v:
        logger.error("Missing env var %s", n)
        sys.exit(1)

# --------------
# AI components
# --------------
MODEL = os.getenv("SUMMARY_MODEL", "facebook/bart-large-cnn")
logger.info("Loading summariser %s", MODEL)
summary_model: Pipeline = pipeline(
    "summarization",
    model=MODEL,
    device="cuda:0" if os.getenv("USE_CUDA", "false").lower() == "true" else -1,
)
translator_he = GoogleTranslator(source="auto", target="iw")
translator_en = GoogleTranslator(source="auto", target="en")

# ----------
# Telethon
# ----------
client = TelegramClient(SESSION, API_ID, API_HASH)

# -------------
# Prom metrics
# -------------
MSG_TOTAL = PmCounter("arab_msgs_total", "Batched arab messages")
DUP_SKIPPED = PmCounter("arab_msgs_dupes", "Duplicates skipped")
SUMMARY_SENT = PmCounter("arab_summaries", "Summaries sent")
MEDIA_SENT = PmCounter("arab_media", "Hot media forwarded")
SMART_FWD = PmCounter("smart_forwarded", "Messages forwarded to smart chat")
POLL_ERRORS = PmCounter("poll_errors", "Polling loop errors")

# ----------
# Constants
# ----------
URL_RE = re.compile(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|[\w\d_-]+)")
BLOCKLIST: Set[str] = {"צבע אדום", "גרם", "היכנסו למרחב המוגן", "חדירת כלי טיס עוין"}
START_TS = time.time()
recent_hashes: Deque[str] = deque(maxlen=HASH_CACHE_SIZE)

@dataclass
class MessageInfo:
    eng: str
    heb: str
    link: str
    channel: str
    media_id: Optional[str]

# ----------
# Utilities
# ----------

def clean_text(t: str) -> str:
    return re.sub(r"\s+", " ", URL_RE.sub("", t)).strip()


def blocked(t: str) -> bool:
    return any(k in t for k in BLOCKLIST)


async def media_sha(msg: Message) -> Optional[str]:
    if not isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
        return None
    try:
        raw: bytes = await msg.download_media(bytes)  # type: ignore[arg-type]
        return hashlib.sha256(raw[:262144]).hexdigest()
    except Exception:
        return None


def permalink(msg: Message) -> str:
    if getattr(msg.chat, "username", None):
        return f"https://t.me/{msg.chat.username}/{msg.id}"
    return ""


def is_dup(text: str) -> bool:
    h = hashlib.sha1(text.encode()).hexdigest()
    if h in recent_hashes:
        return True
    recent_hashes.append(h)
    return False

# -------------------
# Batch & summariser
# -------------------
class BatchCollector:
    def __init__(self, size: int, age: int):
        self.size, self.age = size, age
        self.msgs: List[MessageInfo] = []
        self.lock = asyncio.Lock()
        self.timer: Optional[asyncio.Task] = None

    async def push(self, info: MessageInfo):
        async with self.lock:
            self.msgs.append(info)
            if len(self.msgs) >= self.size:
                await self.flush()
            elif not self.timer or self.timer.done():
                self.timer = asyncio.create_task(self._age_flush())

    async def _age_flush(self):
        await asyncio.sleep(self.age)
        async with self.lock:
            await self.flush()

    async def flush(self):
        if not self.msgs:
            return
        to_summarise, self.msgs = self.msgs, []
        asyncio.create_task(build_and_send_summary(to_summarise))

batcher = BatchCollector(BATCH_SIZE, MAX_BATCH_AGE)

async def build_and_send_summary(msgs: List[MessageInfo]):
    msg_count = len(msgs)
    ch_count = len({m.channel for m in msgs})
    header = f"🗞️ סיכום ({msg_count} הודעות, {ch_count} ערוצים)\n\n"

    eng_blob = "\n".join(m.eng for m in msgs)[:4096]
    try:
        summ_en = summary_model(eng_blob, max_length=180, min_length=60, do_sample=False)[0]["summary_text"]
    except Exception as e:
        logger.error("Summariser failed: %s", e)
        return
    summ_he = translator_he.translate(summ_en)

    full_text = header + summ_he
    await client.send_message(ARABS_SUMMARY_OUT, full_text, link_preview=False)
    SUMMARY_SENT.inc()

    # log summary & links
    links = [m.link for m in msgs if m.link]
    logger.info("SUMMARY SENT → %s", full_text.replace("\n", " | ")[:300])
    if links:
        logger.info("Source links: %s", ", ".join(links))

    # hot media
    counts: Counter[str] = Counter(m.media_id for m in msgs if m.media_id)
    for mid, ct in counts.items():
        if mid and ct >= MEDIA_THRESHOLD:
            cap = f"📷 פריט מדיה הופץ ב‑{ct} ערוצים."
            try:
                await client.send_message(ARABS_SUMMARY_OUT, cap, file=mid, link_preview=False)  # type: ignore[arg-type]
                MEDIA_SENT.inc()
            except Exception as e:
                logger.error("Media send error: %s", e)

# ------------------------
# Message processing logic
# ------------------------
async def process_arab(msg: Message):
    if msg.date.timestamp() < START_TS:
        return
    raw = (msg.text or "").strip()
    logger.info("NEW %s | %s", getattr(msg.chat, "username", "?"), raw[:90])
    if blocked(raw) and not msg.media:
        return
    cleaned = clean_text(raw)
    if is_dup(cleaned):
        DUP_SKIPPED.inc()
        return
    try:
        lang = detect(cleaned)
    except Exception:
        lang = "ar"
    eng = translator_en.translate(cleaned) if lang != "en" else cleaned
    heb = translator_he.translate(cleaned) if lang != "he" else cleaned
    await batcher.push(MessageInfo(eng, heb, permalink(msg), getattr(msg.chat, "username", ""), await media_sha(msg)))
    MSG_TOTAL.inc()

# -------------
# Channel join
# -------------
async def load_usernames(file: Path) -> List[str]:
    if not file.is_file():
        return []
    return sorted({l.strip().lstrip("@").lower() for l in file.read_text().splitlines() if l.strip()})

async def ensure_join(usernames: List[str]):
    dialogs = await client.get_dialogs()
    joined = {d.entity.username.lower() for d in dialogs if getattr(d.entity, "username", None)}
    for uname in usernames:
        try:
            if uname not in joined:
                await client(JoinChannelRequest(uname))
                logger.info("Joined @%s", uname)
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error("Join error @%s: %s", uname, e)

# -----------------
# Polling helper
# -----------------
async def poll_channel(uname: str, handler):
    last_msgs = await client.get_messages(uname, limit=1)
    last_id = last_msgs[0].id if last_msgs else 0
    while True:
        try:
            async for msg in client.iter_messages(uname, min_id=last_id):
                last_id = msg.id
                await handler(msg)
        except errors.FloodWaitError as e:
            await asyncio.sleep(e.seconds)
        except Exception as e:
            POLL_ERRORS.inc()
            logger.error("Poll error @%s: %s", uname, e)
        await asyncio.sleep(POLL_INTERVAL)

# -----------------
# Smart forwarder
# -----------------
smart_usernames: List[str] = []
_sender_cache = {}

if SMART_CHAT:
    async def get_message_link(chat_id: int, msg_id: int) -> str:
        """Return the public t.me link to a message if the source chat is public."""
        try:
            ent = await client.get_entity(chat_id)
            username = getattr(ent, "username", None)
            if username:
                return f"https://t.me/{username}/{msg_id}"
        except Exception:
            pass
        return ""

    async def _collect_album(message: Message) -> list[Message]:
        """
        If the message belongs to a media-group (album) fetch *all* items in that
        group, otherwise just return [message] so downstream code can treat both
        cases uniformly.
        """
        if not message.grouped_id:
            return [message]

        album: list[Message] = [message]

        # Earlier items (walk backwards until the group breaks or we hit the limit)
        async for m in client.iter_messages(
            message.chat_id, offset_id=message.id, reverse=True, limit=20
        ):
            if m.grouped_id != message.grouped_id:
                break
            album.insert(0, m)

        # Later items (walk forwards)
        async for m in client.iter_messages(
            message.chat_id, offset_id=message.id, limit=20
        ):
            if m.grouped_id != message.grouped_id:
                break
            album.append(m)

        # De-duplicate just in case and keep original chronological order
        album = [m for _, m in sorted({m.id: m for m in album}.items())]
        return album

    async def smart_handler(msg: Message):
        """
        Forward messages to SMART_CHAT, preserving any media (single file or album)
        and appending a deep-link to the original message.
        """
        if msg.out or msg.via_bot_id:
            return  # ignore our own messages and bot-generated content

        try:
            link = await get_message_link(msg.chat_id, msg.id)
            caption = f"{msg.text or ''}\n\n{link}".strip()

            # Grab the full album if necessary
            items = await _collect_album(msg)
            has_media = any(x.media for x in items)

            if not has_media:
                # Plain text message
                await client.send_message(
                    SMART_CHAT,
                    caption or link,          # ensure we send *something*
                    link_preview=False,
                )
            else:
                # Single media or album → Telethon treats a list as an album
                files = [itm if itm.media else itm.media for itm in items]
                await client.send_file(
                    SMART_CHAT,
                    files,                    # accepts Message objects directly
                    caption=caption,          # caption is shown only once
                    link_preview=False,
                )

            SMART_FWD.inc()

        except errors.rpcerrorlist.UsernameInvalidError as exc:
            logger.debug("Smart skip – %s", exc)
        except Exception as exc:
            logger.error("Smart fwd error: %s", exc)

# -----------------
# Main entrypoint
# -----------------
async def main():
    print("Starting Arab AI Bot...")
    start_http_server(PROM_PORT)
    print(f"Prometheus metrics server started on port {PROM_PORT}")
    await client.start(phone=lambda: PHONE)
    print("Telegram client started.")

    arab_usernames = await load_usernames(SOURCES_FILE)
    print(f"Loaded {len(arab_usernames)} arab source channels.")
    await ensure_join(arab_usernames)
    print("Joined arab channels.")

    arab_ids = [await client.get_peer_id(f"@{u}") for u in arab_usernames]
    client.add_event_handler(lambda e: asyncio.create_task(process_arab(e.message)), events.NewMessage(chats=arab_ids))
    print("Event handler for arab channels registered.")
    for u in arab_usernames:
        asyncio.create_task(poll_channel(u, process_arab))
        print(f"Started polling for arab channel: {u}")

    if SMART_CHAT:
        global smart_usernames
        smart_usernames = await load_usernames(SMART_SOURCES_FILE)
        print(f"Loaded {len(smart_usernames)} smart source channels.")
        await ensure_join(smart_usernames)
        print("Joined smart channels.")
        smart_ids = [await client.get_peer_id(f"@{u}") for u in smart_usernames]
        client.add_event_handler(lambda e: asyncio.create_task(smart_handler(e.message)), events.NewMessage(chats=smart_ids))
        print("Event handler for smart channels registered.")
        for u in smart_usernames:
            asyncio.create_task(poll_channel(u, smart_handler))
            print(f"Started polling for smart channel: {u}")
        logger.info("Smart forwarder active: %d channels", len(smart_usernames))

    logger.info("Bot online. Arab=%d Smart=%d", len(arab_usernames), len(smart_usernames))
    print("Bot is online and running.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
