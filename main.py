import os
import re
import sys
import logging
import asyncio
from pathlib import Path
from time import sleep

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest
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
    format='%(asctime)s %(levelname)s %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment
DEV = os.getenv('DEV_MODE', 'false').lower() == 'true'
config_file = 'config_dev.env' if DEV else 'config.env'
load_dotenv(dotenv_path=Path(config_file))

# Credentials
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('PHONE_NUMBER')
OWNER_ID = os.getenv('OWNER_ID')
ARABS_CHAT = os.getenv('ARABS')
SMART_CHAT = os.getenv('SMART')

# Verify credentials
required = {
    'TELEGRAM_API_ID': API_ID,
    'TELEGRAM_API_HASH': API_HASH,
    'PHONE_NUMBER': PHONE,
    'OWNER_ID': OWNER_ID,
    'ARABS': ARABS_CHAT,
    'SMART': SMART_CHAT
}
missing = [k for k, v in required.items() if not v]
if missing:
    logger.error(f"Missing environment vars: {', '.join(missing)}")
    sys.exit(1)

API_ID = int(API_ID)
OWNER_ID = int(OWNER_ID)
ARABS_CHAT = int(ARABS_CHAT)
SMART_CHAT = int(SMART_CHAT)

# Initialize client
session = 'bot-dev' if DEV else 'bot'
client = TelegramClient(session, API_ID, API_HASH)

# Translators
translator = GoogleTranslator(source='auto', target='iw')
backup_translator = EasyGoogleTranslate()

# Blocked keywords
BLOCKED_KEYWORDS = {
    'צבע אדום', 'גרם', 'היכנסו למרחב המוגן', 'חדירת כלי טיס עוין'
}

# Static channel lists (without '@')
arab_channels = [
    'a7rarjenin', 'QudsN', 'Electrohizbullah', 'SerajSat', 'shadysopoh',
    'anas_hoshia', 'abohamzahasanat', 'sarayajneen', 'C_Military1', 'mmirleb',
    'SabrenNews22', 'IraninArabic', 'iraninarabic_ir', 'meshheek',
    'qassam1brigades', 'qassambrigades', 'alghalebun3', 'areennabluss'
]
smart_channels = [
    'abualiexpress', 'arabworld301news', 'AlealamAlearabiuEranMalca', 'HallelBittonRosen',
    'amitsegal', 'moriahdoron', 'amirbohbot', 'Middle_East_Insight'
]

# Load additional channels from files

def load_channels_from_file():
    for fname, lst in [('arab', arab_channels), ('smart', smart_channels)]:
        path = Path(f'{fname}_channels.txt')
        if path.is_file():
            with path.open() as f:
                for line in f:
                    name = line.strip().lstrip('@')
                    if name and name not in lst:
                        lst.append(name)
                        logger.info(f"Loaded channel {name} from {fname}_channels.txt")

# Join channels and build ID maps
async def init_channels():
    load_channels_from_file()
    dialogs = await client.get_dialogs()
    joined = {d.entity.username.lower() for d in dialogs if getattr(d.entity, 'username', None)}
    channel_ids = {}
    for name in set(arab_channels + smart_channels):
        uname = name.lower()
        try:
            if uname not in joined:
                await client(JoinChannelRequest(uname))
                logger.info(f"Joined @{uname}")
            entity = await client.get_entity(uname)
            channel_ids[uname] = entity.id
        except errors.FloodWaitError as e:
            logger.warning(f"Rate limited joining @{uname}, waiting {e.seconds}s")
            sleep(e.seconds)
        except Exception as e:
            logger.error(f"Error with channel @{uname}: {e}")
    return channel_ids

# Utility: construct message link
async def get_message_link(chat_id, msg_id):
    try:
        ent = await client.get_entity(chat_id)
        username = getattr(ent, 'username', None)
        if username:
            return f"https://t.me/{username}/{msg_id}"
    except Exception as e:
        logger.warning(f"Failed to construct message link for {chat_id}/{msg_id}: {e}")
    return ''

# Message processing and forwarding helpers
def is_blocked(text):
    return any(kw in text for kw in BLOCKED_KEYWORDS)

async def process_message(msg):
    text = msg.text or ''
    text = re.sub(r'(https?://)?(t\.me|telegram\.me)/(joinchat/|\w+)', '', text)
    if (not text and not msg.file) or is_blocked(text):
        return None
    try:
        lang = detect(text)
    except:
        lang = 'iw'
    if lang not in ('he', 'iw'):
        try:
            text = translator.translate(text)
        except:
            try:
                text = backup_translator.translate(text, 'iw')
            except:
                text = '[Translation failed]\n' + text
    link = await get_message_link(msg.chat_id, msg.id)
    return f"{text}\n\n{link}"

async def check_duplicate(dest, caption, msg):
    try:
        history = await client.get_messages(dest, limit=50)
        for m in history:
            if m.media and msg.media and type(m.media) == type(msg.media) and m.media == msg.media:
                return True
            if m.message == caption:
                return True
    except:
        pass
    return False

async def forward_message(msg, target):
    processed = await process_message(msg)
    if not processed:
        return
    if await check_duplicate(target, processed, msg):
        return
    try:
        if msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
            await client.send_file(target, msg.media, caption=processed, link_preview=False)
        else:
            await client.send_message(target, processed, link_preview=False)
    except Exception as e:
        logger.error(f"Failed to forward message {msg.id} to {target}: {e}")

# Single event handler
async def on_new(event):
    msg = event.message
    cid = msg.chat.id
    if not cid or not msg or msg.out or msg.via_bot_id:
        return
    if cid == OWNER_ID and msg.text and msg.text.startswith('/'):
        # handle owner commands...
        return
    if cid in arab_ids:
        await forward_message(msg, ARABS_CHAT)
    elif cid in smart_ids:
        if '°תוכן שיווקי' in (msg.text or ''):
            return
        await forward_message(msg, SMART_CHAT)

# Main entry point
async def main():
    await client.start(phone=lambda: PHONE)
    logger.info("Client started")
    ids = await init_channels()
    global arab_ids, smart_ids
    arab_ids = {ids.get(name.lower()) for name in arab_channels if name.lower() in ids}
    smart_ids = {ids.get(name.lower()) for name in smart_channels if name.lower() in ids}
    client.add_event_handler(on_new, events.NewMessage(incoming=True))
    logger.info("Handlers registered. Bot is running.")
    await client.run_until_disconnected()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
