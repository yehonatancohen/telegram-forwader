#!/usr/bin/env python3

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, List, Optional

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient

from listener import MessageInfo, init_listeners

# ─── configuration & logging ─────────────────────────────────────────────
load_dotenv(Path("config_dev.env") if os.getenv("DEV") else Path("config.env"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(stream=sys.stdout,
                    level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s %(levelname)s | %(message)s",
                    force=True)
logger = logging.getLogger("arab-ai-main")

# ─── environment variables ───────────────────────────────────────────────
API_ID  = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE    = os.getenv("PHONE_NUMBER")
SESSION  = os.getenv("SESSION_NAME", "arab-ai")
if not SESSION.endswith(".session"):
    SESSION += ".session"
SESSION_PATH = Path(SESSION).expanduser().resolve()

ARABS_SUMMARY_OUT = int(os.getenv("ARABS_SUMMARY_OUT", "0"))
SMART_CHAT        = int(os.getenv("SMART_CHAT", "0"))

BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "24"))
MAX_BATCH_AGE     = int(os.getenv("MAX_BATCH_AGE", "300"))
MEDIA_THRESHOLD   = int(os.getenv("MEDIA_THRESHOLD", "3"))
SUMMARY_MIN_INTERVAL = int(os.getenv("SUMMARY_MIN_INTERVAL", "300"))

SOURCES_FILE       = Path(os.getenv("ARAB_SOURCES_FILE", "arab_channels.txt"))
SMART_SOURCES_FILE = Path(os.getenv("SMART_SOURCES_FILE", "smart_channels.txt"))

CF_ACCOUNT = os.getenv("CF_ACCOUNT_ID")
CF_TOKEN   = os.getenv("CF_API_TOKEN")
CF_MODEL   = os.getenv("CF_MODEL", "@cf/google/gemma-3-12b-it")
CF_URL     = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT}/ai/run/{CF_MODEL}"
HEADERS    = {"Authorization": f"Bearer {CF_TOKEN}"}

if not (API_HASH and PHONE and ARABS_SUMMARY_OUT and CF_ACCOUNT and CF_TOKEN):
    logger.critical("Missing required env vars – aborting")
    sys.exit(1)

# ─── batching ----------------------------------------------------------------
@dataclass
class _BatchState:
    msgs: List[MessageInfo]
    timer: Optional[asyncio.Task]

a_RECENT_MEDIA: Deque[str] = deque(maxlen=2_000)
_last_summary_ts: float = 0.0
_summary_lock = asyncio.Lock()

class BatchCollector:
    def __init__(self, size: int, age: int):
        self.size, self.age = size, age
        self._state = _BatchState([], None)
        self._lock  = asyncio.Lock()

    async def push(self, info: MessageInfo):
        async with self._lock:
            self._state.msgs.append(info)
            logger.debug("batch push (len=%d) – %s…", len(self._state.msgs), info.text[:40])
            if len(self._state.msgs) >= self.size:
                await self._flush_locked()
            elif not self._state.timer or self._state.timer.done():
                self._state.timer = asyncio.create_task(self._auto_flush())

    async def _auto_flush(self):
        await asyncio.sleep(self.age)
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self):
        if not self._state.msgs:
            return
        msgs, self._state.msgs = self._state.msgs, []
        asyncio.create_task(_send_summary_throttled(msgs))

batcher = BatchCollector(BATCH_SIZE, MAX_BATCH_AGE)

# ─── summariser helpers ──────────────────────────────────────────────────
async def _remote_summary(blob: str) -> str:
    payload = {
        "messages": [
            {
                "role": "system",
                "content": (
                    "את/ה כתב/ת חדשות אנרגטי/ת שמושך/ת את הקוראים.\n"
                    "פתח/י בכותרת מודגשת ( **ככה** ) ולאחריה סיכום קצר בעברית."
                    "אל תכתוב שום דבר אחר, רק את הסיכום.\n"
                    "השתדל/י לא לחרוג מ-5000 תווים.\n"
                )
            },
            {"role": "user", "content": blob[:5000]},
        ]
    }
    async with httpx.AsyncClient(timeout=20) as c:
        r = await c.post(CF_URL, headers=HEADERS, json=payload)
    r.raise_for_status()
    return r.json()["result"]["response"].strip()

async def _send_summary_throttled(msgs: List[MessageInfo]):
    global _last_summary_ts
    async with _summary_lock:
        wait = SUMMARY_MIN_INTERVAL - (time.time() - _last_summary_ts)
        if wait > 0:
            logger.info("summary rate‑limit – sleeping %.1fs", wait)
            await asyncio.sleep(wait)
        _last_summary_ts = time.time()
    await _really_send_summary(msgs)

async def _really_send_summary(msgs: List[MessageInfo]):
    blob = "\n".join(m.text for m in msgs)[:2500]
    try:
        summ = await _remote_summary(blob)
    except Exception as exc:
        logger.error("summariser fail: %s", exc)
        return

    # send summary
    try:
        await client.send_message(ARABS_SUMMARY_OUT, summ, link_preview=False)
        logger.info("📝 summary sent (%d chars / %d msgs)", len(summ), len(msgs))
    except Exception as exc:
        logger.error("summary send error: %s", exc)
        return

    # log links
    links = [m.link for m in msgs if m.link]
    if links:
        logger.info("🔗 links: %s", ", ".join(links))

    # hot media
    for mid, ct in Counter(m.media_id for m in msgs if m.media_id).items():
        if mid and ct >= MEDIA_THRESHOLD and mid not in a_RECENT_MEDIA:
            a_RECENT_MEDIA.append(mid)
            try:
                await client.send_message(ARABS_SUMMARY_OUT,
                                          f"📷 פריט מדיה הופץ ב-{ct} ערוצים.",
                                          file=mid, link_preview=False)  # type: ignore[arg-type]
                logger.debug("forwarded media %s (x%d)", mid, ct)
            except Exception as exc:
                logger.error("media send err: %s", exc)

# ─── utils ───────────────────────────────────────────────────────────────

def _load_usernames(path: Path) -> List[str]:
    if not path.is_file():
        logger.warning("channels file missing: %s", path)
        return []
    names = sorted({l.strip().lstrip("@").lower() for l in path.read_text().splitlines() if l.strip()})
    logger.info("loaded %d channels from %s", len(names), path)
    return names

# ─── telegram client & main ──────────────────────────────────────────────
client = TelegramClient(str(SESSION_PATH), API_ID, API_HASH, connection_retries=-1, retry_delay=5, timeout=10)

async def main():
    logger.info("Starting Telegram client with session: %s", SESSION_PATH)
    if not SESSION_PATH.is_file():
        logger.critical("Session file %s does not exist – aborting", SESSION_PATH)
        sys.exit(1)
    await client.start(phone=lambda: PHONE)
    if not await client.is_user_authorized():
        logger.critical("🛑  Telethon entered interactive login — "
                        "the .session file is missing or unwritable.")
        sys.exit(1)

    logger.info("✅  client authorised as %s", PHONE)

    arab  = _load_usernames(SOURCES_FILE)
    smart = _load_usernames(SMART_SOURCES_FILE)

    await init_listeners(client=client, arab_channels=arab, smart_channels=smart,
                         batch_push=batcher.push, smart_chat_id=SMART_CHAT or None)

    logger.info("🚀 bot online – arab=%d smart=%d batch=%d age≤%ds summary_gap=%ds",
                len(arab), len(smart), BATCH_SIZE, MAX_BATCH_AGE, SUMMARY_MIN_INTERVAL)
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
