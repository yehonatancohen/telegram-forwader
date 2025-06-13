import os
import re
import sys
import logging
from pathlib import Path
from time import sleep

from dotenv import load_dotenv
from telethon import TelegramClient, events, errors
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import InputPeerChannel, PeerChannel, MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, User
from telethon.errors import ChannelPrivateError, MediaCaptionTooLongError, SessionPasswordNeededError

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
config_file = 'dev-config.env' if DEV else 'config.env'
load_dotenv(dotenv_path=Path(config_file))

# Credentials
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')
PHONE = os.getenv('PHONE_NUMBER')
OWNER_ID = os.getenv('OWNER_ID')
ARABS_CHAT = os.getenv('ARABS')
SMART_CHAT = os.getenv('SMART')

# Verify credentials
missing = [k for k,v in {
    'TELEGRAM_API_ID': API_ID,
    'TELEGRAM_API_HASH': API_HASH,
    'PHONE_NUMBER': PHONE,
    'OWNER_ID': OWNER_ID,
    'ARABS': ARABS_CHAT,
    'SMART': SMART_CHAT
}.items() if not v]
if missing:
    logger.error(f"Missing env vars: {', '.join(missing)}")
    sys.exit(1)

API_ID = int(API_ID)
OWNER_ID = int(OWNER_ID)
ARABS_CHAT = int(ARABS_CHAT)
SMART_CHAT = int(SMART_CHAT)

# Initialize client
session_name = 'bot-dev' if DEV else 'bot'
client = TelegramClient(session_name, API_ID, API_HASH)

# Translators
translator = GoogleTranslator(source='auto', target='iw')
backup_translator = EasyGoogleTranslate()

# Blocked keywords
BLOCKED_KEYWORDS = {
    'צבע אדום', 'גרם', 'היכנסו למרחב המוגן', 'חדירת כלי טיס עוין'
}

# Channels lists
arab_channels = [
    '@a7rarjenin', '@QudsN', '@Electrohizbullah', '@SerajSat', '@shadysopoh',
    '@anas_hoshia', '@abohamzahasanat', '@sarayajneen', '@C_Military1', '@mmirleb',
    '@SabrenNews22', '@IraninArabic', '@iraninarabic_ir', '@meshheek',
    '@qassam1brigades', '@qassambrigades', '@alghalebun3', '@areennabluss'
]
smart_channels = [
    '@abualiexpress', '@AlealamAlearabiuEranMalca', '@HallelBittonRosen',
    '@amitsegal', '@moriahdoron', '@amirbohbot', '@Middle_East_Insight'
]

# Load additional channels from files

def load_channels_from_file():
    for fname, lst in [('arab', arab_channels), ('smart', smart_channels)]:
        path = Path(f'{fname}_channels.txt')
        if path.is_file():
            with path.open('r') as f:
                for line in f:
                    ch = line.strip()
                    if ch and ch not in lst:
                        lst.append(ch)

# Join a single channel if not already in
async def join_channel(channel):
    try:
        peer = await client.get_input_entity(channel)
        if not isinstance(peer, InputPeerChannel):
            logger.warning(f"Skipping join: {channel} is not a channel or supergroup.")
            return
        if not await check_client_in_channel(peer):
            await client(JoinChannelRequest(channel=peer))
            logger.info(f"Joined channel {channel}")
    except errors.FloodWaitError as e:
        logger.warning(f"Flood wait {e.seconds}s when joining {channel}")
        sleep(e.seconds)
    except Exception as e:
        logger.error(f"Error joining {channel}: {e}")

# Join all configured channels
async def join_all_channels():
    load_channels_from_file()
    for ch in set(arab_channels + smart_channels):
        await join_channel(ch.lstrip('@'))

# Check if bot is in channel
async def check_client_in_channel(channel_peer):
    try:
        if not isinstance(channel_peer, InputPeerChannel):
            return False
        full = await client(GetFullChannelRequest(channel=channel_peer))
        return getattr(full.full_chat, 'participants_count', 0) > 0
    except ChannelPrivateError:
        logger.info(f"Private channel: {channel_peer}")
        return False
    except Exception as e:
        logger.error(f"Error checking channel {channel_peer}: {e}")
        return False

# De-duplicate sending
async def check_if_message_sent(channel, caption, message_obj):
    try:
        peer = await client.get_input_entity(channel)
        history = await client.get_messages(peer, limit=50)
        for msg in history:
            if msg.media and message_obj.media:
                if type(msg.media) == type(message_obj.media) and msg.media == message_obj.media:
                    return True
            if msg.message and caption == msg.message:
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking history for {channel}: {e}")
        return False

# Handlers
async def general_handler(event):
    message = event.message
    chat_id = message.chat_id
    if chat_id == OWNER_ID and message.text.startswith('/'):
        parts = message.text.split()
        cmd = parts[0][1:]
        args = parts[1:]
        await command_handler(cmd, chat_id, args)

async def arab_handler(event):
    await send_message_to_telegram_chat(event.message, ARABS_CHAT)

async def smart_handler(event):
    msg = event.message
    if '°תוכן שיווקי' in msg.text:
        return
    await send_message_to_telegram_chat(msg, SMART_CHAT)

# Message processing

def is_blocked_message(text):
    return any(kw in text for kw in BLOCKED_KEYWORDS)

async def process_message(message):
    # Strip Telegram links
    url_pattern = re.compile(r'(https?://)?(t\.me|telegram\.me)/(joinchat/|\w+)')
    caption = url_pattern.sub('', message.text or '')
    if not caption and not message.file or is_blocked_message(caption):
        logger.info(f"Blocked or empty message: {caption}")
        return None
    try:
        lang = detect(caption)
    except:
        lang = 'iw'
    if lang not in ('he', 'iw'):
        try:
            caption = translator.translate(caption)
        except:
            try:
                caption = backup_translator.translate(caption, 'iw')
            except:
                caption = "[Translation failed]\n" + caption
    link = await get_message_link(message.chat_id, message.id)
    return f"{caption}\n\n{link}"

# Sending logic with media grouping
async def send_message_to_telegram_chat(message, target_chat_id):
    caption = await process_message(message)
    if not caption:
        return
    if await check_if_message_sent(target_chat_id, caption, message):
        return
    if message.media and isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
        await client.send_file(target_chat_id, message.media, caption=caption, link_preview=False)
    else:
        await client.send_message(target_chat_id, caption, link_preview=False)
    logger.info(f"Sent message to {target_chat_id}")

# Utility: construct message link
async def get_message_link(channel_username, message_id):
    try:
        ent = await client.get_entity(channel_username)
        return f"https://t.me/{ent.username}/{message_id}" if ent.username else ''
    except:
        return ''

# Channel management commands
async def add_channel(channel_id, lst, fname):
    match = re.match(r'(https?://t\.me/)?(?P<u>\w+)', channel_id)
    if not match:
        return
    username = match.group('u')
    entry = f"@{username}"
    if entry not in lst:
        lst.append(entry)
        with open(f'{fname}_channels.txt','a') as f:
            f.write(username + '\n')
        await join_channel(username)

async def remove_channel(channel_id, lst):
    if channel_id in lst:
        lst.remove(channel_id)

async def list_channels(chat_id):
    await client.send_message(chat_id, f"Arab: {arab_channels}\nSmart: {smart_channels}")

async def command_handler(command, chat_id, args):
    cmds = {
        'add_arab': lambda: add_channel(args[0], arab_channels, 'arab'),
        'add_smart': lambda: add_channel(args[0], smart_channels, 'smart'),
        'remove': lambda: remove_channel(args[0], arab_channels),
        'list': lambda: list_channels(chat_id),
    }
    if command in cmds:
        await cmds[command]()

# Entry point

def main():
    client.start(phone=lambda: PHONE)
    client.loop.run_until_complete(join_all_channels())
    client.add_event_handler(general_handler, events.NewMessage)
    client.add_event_handler(arab_handler, events.NewMessage(chats=arab_channels))
    client.add_event_handler(smart_handler, events.NewMessage(chats=smart_channels))
    client.run_until_disconnected()

if __name__ == '__main__':
    main()