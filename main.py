#!/usr/bin/env python3
# arab_ai_bot.py
# ============================================================================
#  בוט טלגרם שקורא ערוצים ערביים, מסכם רגיל, ומאתר אירועים חריגים
#  שולח סיכום באצ'ים כל X דקות + מאחד אירועים חריגים ל-5-10 דק'
#  חוסך קריאות LLM בעזרת “תקציב” אסימונים לשעה (קבוע בקוד)
# ============================================================================

from __future__ import annotations

# ───── stdlib / third-party ────────────────────────────────────────────────
import asyncio
import logging
import os
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Deque, Dict, List, Optional, Set

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient

from listener import MessageInfo, init_listeners

# ───── basic config / logging ─────────────────────────────────────────────
load_dotenv(Path("config_dev.env") if os.getenv("DEV") else Path("config.env"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    stream=sys.stdout,
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s | %(message)s",
    force=True,
)
logger = logging.getLogger("arab-ai-main")

# ───── env / constants (ללא ENV חדשים) ───────────────────────────────────
API_ID  = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE    = os.getenv("PHONE_NUMBER")
SESSION  = os.getenv("SESSION_NAME", "arab-ai")
if not SESSION.endswith(".session"):
    SESSION += ".session"
SESSION_PATH = Path(SESSION).expanduser().resolve()

ARABS_SUMMARY_OUT = int(os.getenv("ARABS_SUMMARY_OUT", "0"))
SMART_CHAT        = int(os.getenv("SMART_CHAT", "0"))

BATCH_SIZE           = int(os.getenv("BATCH_SIZE", "24"))
MAX_BATCH_AGE        = int(os.getenv("MAX_BATCH_AGE", "300"))
MEDIA_THRESHOLD      = int(os.getenv("MEDIA_THRESHOLD", "3"))
SUMMARY_MIN_INTERVAL = int(os.getenv("SUMMARY_MIN_INTERVAL", "300"))

SOURCES_FILE       = Path(os.getenv("ARAB_SOURCES_FILE",  "arab_channels.txt"))
SMART_SOURCES_FILE = Path(os.getenv("SMART_SOURCES_FILE", "smart_channels.txt"))

CF_ACCOUNT = os.getenv("CF_ACCOUNT_ID")
CF_TOKEN   = os.getenv("CF_API_TOKEN")
CF_MODEL   = os.getenv("CF_MODEL", "@cf/google/gemma-3-12b-it")
CF_URL     = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT}/ai/run/{CF_MODEL}"
HEADERS    = {"Authorization": f"Bearer {CF_TOKEN}"}

if not (API_HASH and PHONE and ARABS_SUMMARY_OUT and CF_ACCOUNT and CF_TOKEN):
    logger.critical("Missing required env vars – aborting")
    sys.exit(1)

# ───── LLM budget – NO extra ENV (קבוע קשיח) ─────────────────────────────
LLM_BUDGET_HOURLY = 120          # כמה קריאות LLM מותרות בשעה
_calls_used       = 0
_budget_reset_ts  = time.time()

def _charge_llm(cost: int = 1) -> bool:
    """ True-אם יש אסימונים פנויים, אחרת False """
    global _calls_used, _budget_reset_ts
    now = time.time()
    if now - _budget_reset_ts >= 3600:            # reset every hour
        _calls_used = 0
        _budget_reset_ts = now
    if _calls_used + cost > LLM_BUDGET_HOURLY:
        return False
    _calls_used += cost
    return True

# ───── אירועים חריגים – זיהוי ומיזוג ───────────────────────────────────────
URGENT_KW = [
    "عاجل", "انفجار", "انفجارات", "اشتباك", "هجوم", "غارة",
    "قتلى", "מقتل", "إصابة", "ازدحام", "قطع طرق", "أزمة سير",
    "احتجاج", "إغلاق", "زحمة", "طوارئ",
    "حرائق", "حريق", "صاروخ", "درون"
]
EVENT_KEY_LEN = 120         # אורך טקסט לחישוב מפתח
MERGE_WINDOW  = 600         # 10 דקות
FLUSH_EVERY   = 120         # בדיקת flush כל 2 דקות
MIN_SOURCES   = 2           # כמה ערוצים דרושים כדי “להצדיק” שליחה

@dataclass
class _AggEvent:
    texts: List[str]
    channels: Set[str]
    first_ts: float

_event_pool: Dict[str, _AggEvent] = {}
EXCEPTION_CACHE: Deque[str] = deque(maxlen=500)    # מניעת כפילויות send

# ───── batching עבור סיכומים רגילים ──────────────────────────────────────
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
            logger.debug("batch push (len=%d) – %s…",
                         len(self._state.msgs), info.text[:40])
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

# ───── urgent detection helpers ──────────────────────────────────────────
def _looks_urgent(text: str) -> bool:
    low = text.lower()
    return any(k in low for k in URGENT_KW) or any(sym in text for sym in ("🚨", "🔴"))

def _event_key(text: str) -> str:
    """מפתח להשוואה – מוריד ספרות ומרווחים, לוקח n תווים, sha1"""
    cleaned = ''.join(ch for ch in text.lower() if not ch.isdigit())
    snippet = cleaned[:EVENT_KEY_LEN]
    return sha1(snippet.encode()).hexdigest()

async def _add_event(info: MessageInfo):
    """שומר אירוע בבריכה למיזוג"""
    k = _event_key(info.text)
    ev = _event_pool.get(k)
    if ev:
        ev.texts.append(info.text)
        ev.channels.add(info.channel)
    else:
        _event_pool[k] = _AggEvent([info.text], {info.channel}, time.time())

async def _aggregator_loop():
    """ריצה ברקע – כל FLUSH_EVERY שניות שולחת אירועים מאוחדים"""
    while True:
        await asyncio.sleep(FLUSH_EVERY)
        now = time.time()
        for k, ev in list(_event_pool.items()):
            if now - ev.first_ts < MERGE_WINDOW:
                continue
            if len(ev.channels) >= MIN_SOURCES:
                await _send_aggregated_report(ev)
            _event_pool.pop(k, None)

# ───── LLM wrappers ──────────────────────────────────────────────────────
async def _remote_llm(messages, timeout=20) -> str:
    if not _charge_llm():
        logger.warning("LLM budget exhausted – skipping call")
        return ""
    async with httpx.AsyncClient(timeout=timeout) as c:
        r = await c.post(CF_URL, headers=HEADERS, json={"messages": messages})
    r.raise_for_status()
    return r.json()["result"]["response"].strip()

async def _send_aggregated_report(ev: _AggEvent):
    """יוצר דיווח מאוחד של אירוע"""
    snippet = ev.texts[0][:1000]
    srcs = ", ".join(f"@{c}" for c in sorted(ev.channels) if c)
    prompt = [
        {
            "role": "system",
            "content": (
                "את/ה כתב/ת חדשות. צרפ/י כותרת מודגשת, סכם בקצרה בעברית "
                f"והוסף שורה בסוף: (דווח ב-{len(ev.channels)} ערוצים: {srcs})"
            ),
        },
        {"role": "user", "content": snippet},
    ]
    report = await _remote_llm(prompt)
    if not report:
        # fallback פשוט
        report = (
            f"**דיווח חריג**\n{snippet[:180]}...\n"
            f"(דווח ב-{len(ev.channels)} ערוצים: {srcs})"
        )
    # מניעת כפילויות
    h = sha1(report.encode()).hexdigest()[:16]
    if h in EXCEPTION_CACHE:
        return
    EXCEPTION_CACHE.append(h)
    try:
        await client.send_message(ARABS_SUMMARY_OUT, report, link_preview=False)
        logger.info("🚨 merged event sent (%d ch)", len(ev.channels))
    except Exception as exc:
        logger.error("send merged event fail: %s", exc)

# ───── regular summary helpers ───────────────────────────────────────────
async def _remote_summary(blob: str) -> str:
    prompt = [
        {
            "role": "system",
            "content": (
                "את/ה כתב/ת חדשות אנרגטי/ת שמושך/ת את הקוראים.\n"
                "פתח/י בכותרת מודגשת ( **ככה** ) ולאחריה סיכום קצר בעברית.\n"
                "אל תכתוב/י שום דבר אחר.\n"
            ),
        },
        {"role": "user", "content": blob[:5000]},
    ]
    return await _remote_llm(prompt)

async def _send_summary_throttled(msgs: List[MessageInfo]):
    global _last_summary_ts
    async with _summary_lock:
        wait = SUMMARY_MIN_INTERVAL - (time.time() - _last_summary_ts)
        if wait > 0:
            logger.info("summary rate-limit – sleeping %.1fs", wait)
            await asyncio.sleep(wait)
        _last_summary_ts = time.time()
    await _really_send_summary(msgs)

async def _really_send_summary(msgs: List[MessageInfo]):
    blob = "\n".join(m.text for m in msgs)[:2500]
    summ = await _remote_summary(blob)
    if not summ:
        logger.debug("skip empty summary")
        return
    try:
        await client.send_message(ARABS_SUMMARY_OUT, summ, link_preview=False)
        logger.info("📝 summary sent (%d chars / %d msgs)",
                    len(summ), len(msgs))
    except Exception as exc:
        logger.error("summary send error: %s", exc)

    links = [m.link for m in msgs if m.link]
    if links:
        logger.info("🔗 links: %s", ", ".join(links))

    for mid, ct in Counter(m.media_id for m in msgs if m.media_id).items():
        if mid and ct >= MEDIA_THRESHOLD and mid not in a_RECENT_MEDIA:
            a_RECENT_MEDIA.append(mid)
            try:
                await client.send_message(
                    ARABS_SUMMARY_OUT,
                    f"📷 פריט מדיה הופץ ב-{ct} ערוצים.",
                    file=mid, link_preview=False  # type: ignore[arg-type]
                )
            except Exception as exc:
                logger.error("media send err: %s", exc)

# ───── utils ─────────────────────────────────────────────────────────────
def _load_usernames(path: Path) -> List[str]:
    if not path.is_file():
        logger.warning("channels file missing: %s", path)
        return []
    names = sorted({
        l.strip().lstrip("@").lower()
        for l in path.read_text().splitlines() if l.strip()
    })
    logger.info("loaded %d channels from %s", len(names), path)
    return names

# ───── telegram client & main ────────────────────────────────────────────
client = TelegramClient(
    str(SESSION_PATH),
    API_ID,
    API_HASH,
    connection_retries=-1,
    retry_delay=5,
    timeout=10,
)

async def _maybe_handle_exception(info: MessageInfo) -> bool:
    """מזהה אירוע חריג → שומר ב-pool ומונע כניסה לבאצ' הרגיל."""
    if _looks_urgent(info.text):
        await _add_event(info)
        return True
    return False

async def main():
    logger.info("Starting Telegram client with session: %s", SESSION_PATH)
    if not SESSION_PATH.is_file():
        logger.critical("Session file %s does not exist – aborting", SESSION_PATH)
        sys.exit(1)

    await client.start(phone=lambda: PHONE)
    if not await client.is_user_authorized():
        logger.critical("🛑 interactive login needed – aborting")
        sys.exit(1)
    logger.info("✅ client authorised as %s", PHONE)

    arab_channels  = _load_usernames(SOURCES_FILE)
    smart_channels = _load_usernames(SMART_SOURCES_FILE)

    async def _batch_or_exception(info: MessageInfo):
        if not await _maybe_handle_exception(info):
            await batcher.push(info)

    await init_listeners(
        client=client,
        arab_channels=arab_channels,
        smart_channels=smart_channels,
        batch_push=_batch_or_exception,
        smart_chat_id=SMART_CHAT or None,
    )

    # הפעל לולאת איחוד אירועים ברקע
    asyncio.create_task(_aggregator_loop(), name="event-aggregator")

    logger.info(
        "🚀 bot online – arab=%d smart=%d batch=%d age≤%ds summary_gap=%ds",
        len(arab_channels),
        len(smart_channels),
        BATCH_SIZE,
        MAX_BATCH_AGE,
        SUMMARY_MIN_INTERVAL,
    )
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
