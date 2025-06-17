#!/usr/bin/env python3
import os
import re
import sys
import logging
import asyncio
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
from telethon.tl.functions.account import UpdateStatusRequest
from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
from deep_translator import GoogleTranslator
from easygoogletranslate import EasyGoogleTranslate
from langdetect import detect

# ------------------------
# Configuration & Logging 
# ------------------------
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Load environment
DEV = os.getenv("DEV_MODE", "false").lower() == "true"
config_file = "config_dev.env" if DEV else "config.env"
load_dotenv(dotenv_path=Path(config_file))

# Credentials
API_ID      = int(os.getenv("TELEGRAM_API_ID", "0"))
API_HASH    = os.getenv("TELEGRAM_API_HASH")
PHONE       = os.getenv("PHONE_NUMBER")
ARABS_CHAT  = int(os.getenv("ARABS", "0"))
SMART_CHAT  = int(os.getenv("SMART", "0"))

for name, val in [
    ("TELEGRAM_API_ID", API_ID),
    ("TELEGRAM_API_HASH", API_HASH),
    ("PHONE_NUMBER", PHONE),
    ("ARABS", ARABS_CHAT),
    ("SMART", SMART_CHAT),
]:
    if not val:
        logger.error(f"Missing env var {name}")
        sys.exit(1)

# Initialize client
session = "bot-dev" if DEV else "bot"
client = TelegramClient(session, API_ID, API_HASH)

# Translators
translator        = GoogleTranslator(source="auto", target="iw")
backup_translator = EasyGoogleTranslate()

# Blocked keywords
BLOCKED_KEYWORDS = {
    "צבע אדום", "גרם", "היכנסו למרחב המוגן", "חדירת כלי טיס עוין"
}

# Static channel usernames (no '@')
arab_channels = [
    "a7rarjenin", "QudsN", "Electrohizbullah", "SerajSat", "shadysopoh",
    "anas_hoshia", "abohamzahasanat", "sarayajneen", "C_Military1", "mmirleb",
    "SabrenNews22", "IraninArabic", "iraninarabic_ir", "meshheek",
    "qassam1brigades", "qassambrigades", "alghalebun3", "areennabluss"
]
smart_channels = [
    "abualiexpress", "arabworld301news", "AlealamAlearabiuEranMalca",
    "HallelBittonRosen", "amitsegal", "moriahdoron", "amirbohbot",
    "Middle_East_Insight"
]

def load_channels_from_file():
    for fname, lst in [("arab", arab_channels), ("smart", smart_channels)]:
        path = Path(f"{fname}_channels.txt")
        if path.is_file():
            for line in path.read_text().splitlines():
                name = line.strip().lstrip("@")
                if name and name not in lst:
                    lst.append(name)
                    logger.info(f"Loaded channel {name} from {fname}_channels.txt")

async def init_channels():
    load_channels_from_file()
    dialogs = await client.get_dialogs()
    joined = {
        d.entity.username.lower()
        for d in dialogs
        if getattr(d.entity, "username", None)
    }
    ids = {}
    for uname in set(arab_channels + smart_channels):
        u = uname.lower()
        try:
            if u not in joined:
                await client(JoinChannelRequest(u))
                logger.info(f"Joined @{u}")
            ent = await client.get_entity(u)
            ids[u] = ent.id
        except errors.FloodWaitError as e:
            logger.warning(f"Rate-limited on @{u}, sleeping {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error with @{u}: {e}")
    return ids

async def keep_online():
    while True:
        try:
            await client(UpdateStatusRequest(offline=False))
        except Exception as e:
            logger.warning(f"keep_online error: {e}")
        await asyncio.sleep(60)

async def get_message_link(chat_id, msg_id):
    try:
        ent = await client.get_entity(chat_id)
        username = getattr(ent, "username", None)
        if username:
            return f"https://t.me/{username}/{msg_id}"
    except Exception:
        pass
    return ""

def is_blocked(text: str) -> bool:
    return any(kw in text for kw in BLOCKED_KEYWORDS)

async def process_message(msg):
    text = (msg.text or "").strip()
    text = re.sub(r"(https?://)?(t\.me|telegram\.me)/(joinchat/|\w+)", "", text).strip()
    if (not text and not msg.file) or is_blocked(text):
        return None
    try:
        lang = detect(text)
    except:
        lang = "iw"
    if lang not in ("he", "iw"):
        try:
            text = translator.translate(text)
        except:
            try:
                text = backup_translator.translate(text, "iw")
            except:
                text = "[Translation failed]\n" + text
    link = await get_message_link(msg.chat_id, msg.id)
    return f"{text}\n\n{link}"

async def check_duplicate(dest, cap, msg):
    try:
        history = await client.get_messages(dest, limit=50)
        for m in history:
            if (
                m.media
                and msg.media
                and type(m.media) == type(msg.media)
                and m.media == msg.media
            ):
                return True
            if m.message == cap:
                return True
    except:
        pass
    return False

async def forward_message(msg, target):
    cap = await process_message(msg)
    if not cap or await check_duplicate(target, cap, msg):
        return
    if msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
        await client.send_file(target, msg.media, caption=cap, link_preview=False)
    else:
        await client.send_message(target, cap, link_preview=False)

async def on_new(event):
    msg = event.message
    if msg.out or msg.via_bot_id:
        return
    cid = event.chat_id
    if cid in arab_ids:
        logger.info(f"[live] Arab msg {msg.id} in {cid}")
        await forward_message(msg, ARABS_CHAT)
    elif cid in smart_ids:
        if "°תוכן שיווקי" in (msg.text or ""):
            return
        logger.info(f"[live] Smart msg {msg.id} in {cid}")
        await forward_message(msg, SMART_CHAT)

async def poll_channel(uname, cid, target):
    # initialize last_id to the newest existing message
    msgs = await client.get_messages(uname, limit=1)
    last_id = msgs[0].id if msgs else 0

    while True:
        try:
            async for msg in client.iter_messages(uname, min_id=last_id):
                # iter_messages yields only msg.id > last_id
                logger.info(f"[poll] New {msg.id} in {cid}")
                await forward_message(msg, target)
                last_id = msg.id
        except errors.FloodWaitError as e:
            logger.warning(f"Poll flood-wait on {uname}: {e.seconds}s")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error polling {uname}: {e}")
        await asyncio.sleep(30)

async def main():
    await client.start(phone=lambda: PHONE)
    logger.info("Client started")
    client.loop.create_task(keep_online())
    # preload dialogs so events fire
    await client.get_dialogs()
    ids = await init_channels()
    global arab_ids, smart_ids
    arab_ids  = {ids[u] for u in arab_channels if u in ids}
    smart_ids = {ids[u] for u in smart_channels if u in ids}

    # live handler
    client.add_event_handler(on_new, events.NewMessage())

    # start polling for channels that don't push
    for uname, cid in ids.items():
        if cid in arab_ids:
            asyncio.create_task(poll_channel(uname, cid, ARABS_CHAT))
        elif cid in smart_ids:
            asyncio.create_task(poll_channel(uname, cid, SMART_CHAT))

    logger.info("Handlers & pollers running; bot is live.")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())