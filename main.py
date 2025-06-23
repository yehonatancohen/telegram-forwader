#!/usr/bin/env python3
# arab_ai_bot.py
# ============================================================================
#  קורא ערוצים ערביים, מסכם (batch) + מאגד "מגמות" דיווח שחוזרות בכמה ערוצים
#  שולח אחת לכ-10 דק’ עדכון מגמה בעברית: שורת סיכום + ציטוט מתורגם + קישור מקור
#  שומר על תקציב LLM (120 קריאות/שעה, קבוע בקוד)
# ============================================================================

from __future__ import annotations

import asyncio, logging, os, sys, time
from collections import Counter, deque
from dataclasses import dataclass
from hashlib import sha1
from pathlib import Path
from typing import Deque, Dict, List, Optional, Set

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient

from listener import MessageInfo, init_listeners

# ───── config / logging ─────────────────────────────────────────────────
load_dotenv(Path("config_dev.env") if os.getenv("DEV") else Path("config.env"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(stream=sys.stdout,
                    level=getattr(logging, LOG_LEVEL),
                    format="%(asctime)s %(levelname)s | %(message)s",
                    force=True)
logger = logging.getLogger("arab-ai-main")

# ───── env vars (ללא חדשים) ────────────────────────────────────────────
API_ID  = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH = os.getenv("TELEGRAM_API_HASH")
PHONE    = os.getenv("PHONE_NUMBER")
SESSION  = os.getenv("SESSION_NAME", "arab-ai")
if not SESSION.endswith(".session"):
    SESSION += ".session"
SESSION_PATH = Path(SESSION).expanduser().resolve()

ARABS_SUMMARY_OUT = int(os.getenv("ARABS_SUMMARY_OUT", "0"))
SMART_CHAT        = int(os.getenv("SMART_CHAT", "0"))

BATCH_SIZE           = int(os.getenv("BATCH_SIZE",            "24"))
MAX_BATCH_AGE        = int(os.getenv("MAX_BATCH_AGE",         "300"))
MEDIA_THRESHOLD      = int(os.getenv("MEDIA_THRESHOLD",       "3"))
SUMMARY_MIN_INTERVAL = int(os.getenv("SUMMARY_MIN_INTERVAL",  "300"))

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

# ───── LLM budget (קבוע בקוד) ──────────────────────────────────────────
LLM_BUDGET_HOURLY = 120
_calls_used = 0
_budget_reset_ts = time.time()

def _charge_llm(cost: int = 1) -> bool:
    global _calls_used, _budget_reset_ts
    now = time.time()
    if now - _budget_reset_ts >= 3600:
        _calls_used = 0
        _budget_reset_ts = now
    if _calls_used + cost > LLM_BUDGET_HOURLY:
        return False
    _calls_used += cost
    return True

# ───── זיהוי אירועים ומיזוג מגמות ──────────────────────────────────────
URGENT_KW = [
    "عاجل","انفجار","انفجارات","اشتباك","هجوم","غارة",
    "قتلى","مقتل","إصابة","ازدحام","قطع طرق","أزمة سير",
    "احتجاج","إغلاق","زحمة","طوارئ","حرائق","حريق","صاروخ","درون"
]
EVENT_KEY_LEN = 120   # כמה תווים נחתכים לחישוב hash
MERGE_WINDOW  = 600   # חלון מיזוג 10 דק'
FLUSH_EVERY   = 120   # בדיקת flush כל 2 דק'
MIN_SOURCES   = 2     # כמה ערוצים לפחות לתקן “מגמה”

@dataclass
class _AggEvent:
    texts   : List[str]
    channels: Set[str]
    links   : List[str]
    first_ts: float

_event_pool: Dict[str, _AggEvent] = {}
SENT_CACHE : Deque[str] = deque(maxlen=800)   # מניעת כפילויות דיווח

def _looks_urgent(t:str)->bool:
    low=t.lower()
    return any(k in low for k in URGENT_KW) or any(sym in t for sym in("🚨","🔴"))

def _event_key(text:str)->str:
    cleaned=''.join(ch for ch in text.lower() if not ch.isdigit())
    snippet=cleaned[:EVENT_KEY_LEN]
    return sha1(snippet.encode()).hexdigest()

async def _add_event(info:MessageInfo):
    k=_event_key(info.text)
    ev=_event_pool.get(k)
    if ev:
        ev.texts.append(info.text)
        ev.channels.add(info.channel)
        if info.link: ev.links.append(info.link)
    else:
        _event_pool[k]=_AggEvent([info.text],[info.channel], [info.link] if info.link else [], time.time())

async def _aggregator_loop():
    while True:
        await asyncio.sleep(FLUSH_EVERY)
        now=time.time()
        for k,ev in list(_event_pool.items()):
            if now-ev.first_ts < MERGE_WINDOW:
                continue
            if len(ev.channels) >= MIN_SOURCES:
                await _send_trend_report(ev)
            _event_pool.pop(k,None)

# ───── LLM wrapper ───────────────────────────────────────────────────────
async def _remote_llm(messages,timeout=20)->str:
    if not _charge_llm():
        logger.warning("LLM budget exhausted – skipping call")
        return ""
    async with httpx.AsyncClient(timeout=timeout) as c:
        r=await c.post(CF_URL,headers=HEADERS,json={"messages":messages})
    r.raise_for_status()
    return r.json()["result"]["response"].strip()

# ───── דיווח מגמה ───────────────────────────────────────────────────────
async def _send_trend_report(ev:_AggEvent):
    quote_src=ev.texts[0]
    link= ev.links[0] if ev.links else ""
    srcs = ", ".join(f"@{c}" for c in sorted(ev.channels) if c)

    prompt=[
        {
            "role":"system",
            "content":(
                "סכם במדויק בשורה אחת בעברית את המידע העיקרי שנמסר במספר ערוצים. "
                "המטרה – דיווח תמציתי וברור, בלי סגנון 'כתב חדשות'. "
                "לאחר מכן החזר שורה שנייה שמתחילה ב־\"> \" ומכילה תרגום לעברית של ציטוט מייצג מתוך ההודעה. "
                "אל תכתוב שום דבר מעבר לשתי השורות."
            )
        },
        {"role":"user","content":quote_src[:800]}
    ]
    summary=_remote_llm   # alias for readability
    first_two=await summary(prompt)
    if not first_two:
        first_two=f"עדכון מגמה: דיווחים חוזרים ({len(ev.channels)} ערוצים) על אירוע/תנועה חריגה.\n> \"{quote_src[:120]}...\""

    report=f"{first_two}\n(מקור: {link or 'ללא־קישור'} | דווח ב-{len(ev.channels)} ערוצים: {srcs})"

    h=sha1(report.encode()).hexdigest()[:16]
    if h in SENT_CACHE:
        return
    SENT_CACHE.append(h)
    try:
        await client.send_message(ARABS_SUMMARY_OUT, report, link_preview=False)
        logger.info("📊 trend report sent (%d ch)", len(ev.channels))
    except Exception as exc:
        logger.error("trend send fail: %s", exc)

# ───── batching (סיכומים רגילים) ────────────────────────────────────────
@dataclass
class _BatchState:
    msgs: List[MessageInfo]
    timer: Optional[asyncio.Task]

a_RECENT_MEDIA:Deque[str]=deque(maxlen=2_000)
_last_summary_ts=0.0
_summary_lock=asyncio.Lock()

class BatchCollector:
    def __init__(self,size:int,age:int):
        self.size,self.age=size,age
        self._state=_BatchState([],None)
        self._lock=asyncio.Lock()
    async def push(self,info:MessageInfo):
        async with self._lock:
            self._state.msgs.append(info)
            if len(self._state.msgs)>=self.size:
                await self._flush_locked()
            elif not self._state.timer or self._state.timer.done():
                self._state.timer=asyncio.create_task(self._auto_flush())
    async def _auto_flush(self):
        await asyncio.sleep(self.age)
        async with self._lock: await self._flush_locked()
    async def _flush_locked(self):
        if not self._state.msgs: return
        msgs,self._state.msgs=self._state.msgs,[]
        asyncio.create_task(_send_summary_throttled(msgs))

batcher=BatchCollector(BATCH_SIZE,MAX_BATCH_AGE)

async def _remote_summary(blob:str)->str:
    prompt=[
        {"role":"system",
         "content":"סכם בקצרה בעברית את הנקודות העיקריות – שורה־שתיים, ללא סגנון כתבים."},
        {"role":"user","content":blob[:5000]}
    ]
    return await _remote_llm(prompt)

async def _send_summary_throttled(msgs:List[MessageInfo]):
    global _last_summary_ts
    async with _summary_lock:
        wait=SUMMARY_MIN_INTERVAL-(time.time()-_last_summary_ts)
        if wait>0:
            await asyncio.sleep(wait)
        _last_summary_ts=time.time()
    await _really_send_summary(msgs)

async def _really_send_summary(msgs:List[MessageInfo]):
    text="\n".join(m.text for m in msgs)[:2500]
    summ=await _remote_summary(text)
    if not summ: return
    try:
        await client.send_message(ARABS_SUMMARY_OUT,summ,link_preview=False)
        logger.info("📝 summary sent (%d msgs)",len(msgs))
    except Exception as exc:
        logger.error("summary send error: %s", exc)

# ───── utils ────────────────────────────────────────────────────────────
def _load_usernames(path:Path)->List[str]:
    if not path.is_file():
        logger.warning("channels file missing: %s",path); return []
    return sorted({l.strip().lstrip("@").lower()
                   for l in path.read_text().splitlines() if l.strip()})

# ───── telegram client & main ───────────────────────────────────────────
client=TelegramClient(str(SESSION_PATH),API_ID,API_HASH,
                      connection_retries=-1,retry_delay=5,timeout=10)

async def _maybe_handle_event(info:MessageInfo)->bool:
    if _looks_urgent(info.text):
        await _add_event(info)
        return True
    return False

async def main():
    if not SESSION_PATH.is_file():
        logger.critical("session %s missing",SESSION_PATH); sys.exit(1)
    await client.start(phone=lambda:PHONE)
    if not await client.is_user_authorized():
        logger.critical("🛑 interactive login needed"); sys.exit(1)
    logger.info("✅ authorised")

    arab=_load_usernames(SOURCES_FILE)
    smart=_load_usernames(SMART_SOURCES_FILE)

    async def _route(info:MessageInfo):
        if not await _maybe_handle_event(info):
            await batcher.push(info)

    await init_listeners(client=client,
                         arab_channels=arab,
                         smart_channels=smart,
                         batch_push=_route,
                         smart_chat_id=SMART_CHAT or None)

    asyncio.create_task(_aggregator_loop(),name="trend-aggregator")
    logger.info("🚀 bot online – %d arab | %d smart",len(arab),len(smart))
    await client.run_until_disconnected()

if __name__=="__main__":
    asyncio.run(main())
