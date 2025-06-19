#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import logging
import os
import sys
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, List, Optional

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient

from listener import MessageInfo, init_listeners  # our helper module

# ---------------------------------------------------------------------------
# 1. Configuration & logging
# ---------------------------------------------------------------------------
DEV = False
config_file = "config_dev.env" if DEV else "config.env"
load_dotenv(Path(config_file))

logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO),
    format="%(asctime)s %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger("arab-ai-main")

# ---------------------------------------------------------------------------
# 2. Required environment variables
# ---------------------------------------------------------------------------
API_ID  = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE    = os.getenv("PHONE_NUMBER")
SESSION  = os.getenv("SESSION_NAME", "arab-ai")

ARABS_SUMMARY_OUT = int(os.getenv("ARABS_SUMMARY_OUT", "0"))
SMART_CHAT        = int(os.getenv("SMART_CHAT", "0"))  # optional

# batching & media handling
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "12"))
MAX_BATCH_AGE  = int(os.getenv("MAX_BATCH_AGE", "300"))
MEDIA_THRESHOLD = int(os.getenv("MEDIA_THRESHOLD", "3"))

# channel lists
SOURCES_FILE       = Path(os.getenv("ARAB_SOURCES_FILE", "arab_channels.txt"))
SMART_SOURCES_FILE = Path(os.getenv("SMART_SOURCES_FILE", "smart_channels.txt"))

# Cloudflare Workers AI summariser
CF_ACCOUNT = os.getenv("CF_ACCOUNT_ID")
CF_TOKEN   = os.getenv("CF_API_TOKEN")
CF_MODEL   = os.getenv("CF_MODEL", "@cf/google/gemma-3-12b-it")
CF_URL     = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT}/ai/run/{CF_MODEL}"
HEADERS    = {"Authorization": f"Bearer {CF_TOKEN}"}

# sanity‑check required vars
for var, val in (
    ("TELEGRAM_API_HASH", API_HASH),
    ("PHONE_NUMBER", PHONE),
    ("ARABS_SUMMARY_OUT", ARABS_SUMMARY_OUT),
    ("CF_ACCOUNT_ID", CF_ACCOUNT),
    ("CF_API_TOKEN", CF_TOKEN),
):
    if not val:
        logger.error("Missing env var %s", var)
        sys.exit(1)

# ---------------------------------------------------------------------------
# 3. Batch collector & summariser
# ---------------------------------------------------------------------------
@dataclass
class _BatchState:
    msgs: List[MessageInfo]
    timer: Optional[asyncio.Task]


a_RECENT_MEDIA: Deque[str] = deque(maxlen=2_000)  # de‑dup hot media

class BatchCollector:
    """Collects messages, flushes by size or age, then summarises."""

    def __init__(self, size: int, age: int):
        self.size = size
        self.age  = age
        self._state = _BatchState([], None)
        self._lock  = asyncio.Lock()

    async def push(self, info: MessageInfo):
        async with self._lock:
            self._state.msgs.append(info)
            logger.info("Message added to batch collector. Queue size: %d", len(self._state.msgs))
            if len(self._state.msgs) >= self.size:
                logger.info("Batch size reached (%d messages). Flushing batch.", self.size)
                await self._flush_locked()
            elif not self._state.timer or self._state.timer.done():
                logger.info("Starting auto flush timer for %d seconds.", self.age)
                self._state.timer = asyncio.create_task(self._auto_flush())

    async def _auto_flush(self):
        await asyncio.sleep(self.age)
        logger.info("Auto flush timer expired.")
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self):
        if not self._state.msgs:
            logger.info("Flush called but batch is empty. Skipping.")
            return
        msgs, self._state.msgs = self._state.msgs, []
        logger.info("Flushing batch of %d messages.", len(msgs))
        asyncio.create_task(build_and_send_summary(msgs))

batcher = BatchCollector(BATCH_SIZE, MAX_BATCH_AGE)


async def _remote_summary(text_en: str) -> str:
    payload = {
        "messages": [
            {
                "role": "system",
                "content": (
                    "את/ה כתב/ת חדשות אנרגטי/ת שמושך/ת את הקוראים.\n"
                    "פתח/י בכותרת מודגשת ( **ככה** ) ולאחריה סיכום קצר בעברית."
                ),
            },
            {"role": "user", "content": text_en[:5000]},
        ]
    }
    logger.info("Sending summarisation request.")
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(CF_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    response = r.json()["result"]["response"].strip()
    logger.info("Received summarisation response of length %d.", len(response))
    return response


async def build_and_send_summary(msgs: List[MessageInfo]):
    """Summarise a batch and forward it to the output channel."""
    blob = "\n".join(m.text for m in msgs)[:2500]
    try:
        summ = await _remote_summary(blob)
    except Exception as exc:
        logger.error("Summariser error: %s", exc)
        return

    try:
        await client.send_message(ARABS_SUMMARY_OUT, summ, link_preview=False)
        logger.info("Summary message sent to %d.", ARABS_SUMMARY_OUT)
    except Exception as exc:
        logger.error("Error sending summary message: %s", exc)
        return

    # forward frequently‑shared media
    counts = Counter(m.media_id for m in msgs if m.media_id)
    for mid, ct in counts.items():
        if mid and ct >= MEDIA_THRESHOLD and mid not in a_RECENT_MEDIA:
            a_RECENT_MEDIA.append(mid)
            caption = f"📷 פריט מדיה הופץ ב‑{ct} ערוצים."
            try:
                await client.send_message(ARABS_SUMMARY_OUT, caption, file=mid, link_preview=False)  # type: ignore[arg-type]
                logger.info("Forwarded media (ID: %s) with count %d.", mid, ct)
            except Exception as exc:
                logger.error("Media send error: %s", exc)

# ---------------------------------------------------------------------------
# 4. Utility: read channel lists
# ---------------------------------------------------------------------------

def _load_usernames(file: Path) -> List[str]:
    if not file.is_file():
        logger.warning("Channel file %s not found.", file)
        return []
    usernames = sorted({l.strip().lstrip("@").lower() for l in file.read_text().splitlines() if l.strip()})
    logger.info("Loaded %d usernames from %s.", len(usernames), file)
    return usernames

# ---------------------------------------------------------------------------
# 5. Telegram client
# ---------------------------------------------------------------------------
client = TelegramClient(
    SESSION,
    API_ID,
    API_HASH,
    connection_retries=-1,
    retry_delay=5,
    timeout=10,
)

# ---------------------------------------------------------------------------
# 6. Main entry‑point
# ---------------------------------------------------------------------------
async def main():
    await client.start(phone=lambda: PHONE)
    logger.info("Client started.")

    arab_usernames  = _load_usernames(SOURCES_FILE)
    smart_usernames = _load_usernames(SMART_SOURCES_FILE)

    await init_listeners(
        client        = client,
        arab_channels = arab_usernames,
        smart_channels= smart_usernames,
        batch_push    = batcher.push,
        smart_chat_id = SMART_CHAT or None,
    )

    logger.info("Bot online – %d arab channels, %d smart channels", len(arab_usernames), len(smart_usernames))
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
