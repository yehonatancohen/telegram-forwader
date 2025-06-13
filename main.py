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
    logger.error(f"Missing required environment variables: {', '.join(missing)}. Please check your config file.")
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
                        logger.info(f"Loaded channel @{ch} from {fname}_channels.txt")

# Join a single channel if not already in
async def join_channel(channel):
    try:
        peer = await client.get_input_entity(channel)
        if not isinstance(peer, InputPeerChannel):
            logger.warning(f"Skipping join: {channel} is not a channel or supergroup.")
            return
        if not await check_client_in_channel(peer):
            await client(JoinChannelRequest(channel=peer))
            logger.info(f"Successfully joined channel: {channel}")
        else:
            logger.info(f"Already a member of channel: {channel}")
    except errors.FloodWaitError as e:
        logger.warning(f"Flood wait for {e.seconds}s when joining channel: {channel}. Sleeping...")
        sleep(e.seconds)
    except Exception as e:
        logger.error(f"Failed to join channel {channel}: {e}")

# Join all configured channels
async def join_all_channels():
    load_channels_from_file()
    logger.info("Attempting to join all configured channels...")
    for ch in set(arab_channels + smart_channels):
        logger.info(f"Joining channel: {ch}")
        await join_channel(ch.lstrip('@'))

# Check if bot is in channel
async def check_client_in_channel(channel_peer):
    try:
        if not isinstance(channel_peer, InputPeerChannel):
            logger.debug(f"Entity {channel_peer} is not an InputPeerChannel.")
            return False
        full = await client(GetFullChannelRequest(channel=channel_peer))
        in_channel = getattr(full.full_chat, 'participants_count', 0) > 0
        logger.debug(f"Checked membership for {channel_peer.channel_id}: {'IN' if in_channel else 'NOT IN'}")
        return in_channel
    except ChannelPrivateError:
        logger.info(f"Cannot check membership: Channel {channel_peer.channel_id} is private.")
        return False
    except Exception as e:
        logger.error(f"Error checking channel membership for {channel_peer}: {e}")
        return False

# De-duplicate sending
async def check_if_message_sent(channel, caption, message_obj):
    try:
        peer = await client.get_input_entity(channel)
        history = await client.get_messages(peer, limit=50)
        for msg in history:
            if msg.media and message_obj.media:
                if type(msg.media) == type(message_obj.media) and msg.media == message_obj.media:
                    logger.info(f"Duplicate media detected in channel {channel}, skipping send.")
                    return True
            if msg.message and caption == msg.message:
                logger.info(f"Duplicate text detected in channel {channel}, skipping send.")
                return True
        return False
    except Exception as e:
        logger.error(f"Error checking message history for channel {channel}: {e}")
        return False

# Handlers
async def general_handler(event):
    message = event.message
    chat_id = message.chat_id
    logger.info(f"Received message in chat {chat_id} from user {message.sender_id}: {message.text[:50] if message.text else '[no text]'}")
    if chat_id == OWNER_ID and message.text and message.text.startswith('/'):
        parts = message.text.split()
        cmd = parts[0][1:]
        args = parts[1:]
        logger.info(f"Owner command received: /{cmd} {' '.join(args)}")
        await command_handler(cmd, chat_id, args)

async def arab_handler(event):
    logger.info(f"Forwarding message from Arab channel {event.chat_id} (msg id {event.message.id})")
    await send_message_to_telegram_chat(event.message, ARABS_CHAT)

async def smart_handler(event):
    msg = event.message
    if '°תוכן שיווקי' in msg.text:
        logger.info(f"Blocked marketing content in smart channel {event.chat_id} (msg id {msg.id})")
        return
    logger.info(f"Forwarding message from Smart channel {event.chat_id} (msg id {msg.id})")
    await send_message_to_telegram_chat(msg, SMART_CHAT)

# Message processing

def is_blocked_message(text):
    blocked = any(kw in text for kw in BLOCKED_KEYWORDS)
    if blocked:
        logger.info(f"Blocked message due to keyword: {text[:50]}")
    return blocked

async def process_message(message):
    url_pattern = re.compile(r'(https?://)?(t\.me|telegram\.me)/(joinchat/|\w+)')
    caption = url_pattern.sub('', message.text or '')
    if not caption and not message.file or is_blocked_message(caption):
        logger.info(f"Message blocked or empty. Message id: {message.id}")
        return None
    try:
        lang = detect(caption)
        logger.debug(f"Detected language for message id {message.id}: {lang}")
    except Exception as e:
        logger.warning(f"Language detection failed for message id {message.id}: {e}")
        lang = 'iw'
    if lang not in ('he', 'iw'):
        try:
            caption = translator.translate(caption)
            logger.info(f"Translated message id {message.id} to Hebrew using GoogleTranslator.")
        except Exception as e:
            logger.warning(f"GoogleTranslator failed for message id {message.id}: {e}")
            try:
                caption = backup_translator.translate(caption, 'iw')
                logger.info(f"Translated message id {message.id} to Hebrew using EasyGoogleTranslate.")
            except Exception as e2:
                logger.error(f"Backup translation failed for message id {message.id}: {e2}")
                caption = "[Translation failed]\n" + caption
    link = await get_message_link(message.chat_id, message.id)
    return f"{caption}\n\n{link}"

# Sending logic with media grouping
async def send_message_to_telegram_chat(message, target_chat_id):
    caption = await process_message(message)
    if not caption:
        logger.info(f"Message id {message.id} not sent (blocked or empty).")
        return
    if await check_if_message_sent(target_chat_id, caption, message):
        logger.info(f"Message id {message.id} already sent to chat {target_chat_id}. Skipping.")
        return
    try:
        if message.media and isinstance(message.media, (MessageMediaPhoto, MessageMediaDocument)):
            await client.send_file(target_chat_id, message.media, caption=caption, link_preview=False)
            logger.info(f"Sent media message id {message.id} to chat {target_chat_id}.")
        else:
            await client.send_message(target_chat_id, caption, link_preview=False)
            logger.info(f"Sent text message id {message.id} to chat {target_chat_id}.")
    except Exception as e:
        logger.error(f"Failed to send message id {message.id} to chat {target_chat_id}: {e}")

# Utility: construct message link
async def get_message_link(channel_username, message_id):
    try:
        ent = await client.get_entity(channel_username)
        if hasattr(ent, 'username') and ent.username:
            link = f"https://t.me/{ent.username}/{message_id}"
            logger.debug(f"Constructed message link: {link}")
            return link
        else:
            logger.debug(f"No username found for entity {channel_username}")
            return ''
    except Exception as e:
        logger.warning(f"Failed to construct message link for {channel_username} {message_id}: {e}")
        return ''

# Channel management commands
async def add_channel(channel_id, lst, fname):
    match = re.match(r'(https?://t\.me/)?(?P<u>\w+)', channel_id)
    if not match:
        logger.warning(f"Invalid channel format: {channel_id}")
        return
    username = match.group('u')
    entry = f"@{username}"
    if entry not in lst:
        lst.append(entry)
        with open(f'{fname}_channels.txt','a') as f:
            f.write(username + '\n')
        logger.info(f"Added channel @{username} to {fname}_channels.txt and list.")
        await join_channel(username)
    else:
        logger.info(f"Channel @{username} already in {fname} list.")

async def remove_channel(channel_id, lst):
    if channel_id in lst:
        lst.remove(channel_id)
        logger.info(f"Removed channel {channel_id} from list.")
    else:
        logger.info(f"Channel {channel_id} not found in list.")

async def list_channels(chat_id):
    msg = f"Arab channels: {arab_channels}\nSmart channels: {smart_channels}"
    logger.info(f"Listing channels to chat {chat_id}")
    await client.send_message(chat_id, msg)

async def command_handler(command, chat_id, args):
    cmds = {
        'add_arab': lambda: add_channel(args[0], arab_channels, 'arab'),
        'add_smart': lambda: add_channel(args[0], smart_channels, 'smart'),
        'remove': lambda: remove_channel(args[0], arab_channels),
        'list': lambda: list_channels(chat_id),
    }
    if command in cmds:
        logger.info(f"Executing command: {command} with args: {args}")
        await cmds[command]()
    else:
        logger.warning(f"Unknown command: {command}")

# Entry point

def main():
    logger.info("Starting Telegram Forwarder Bot...")
    client.start(phone=lambda: PHONE)
    logger.info("Telegram client started.")
    client.loop.run_until_complete(join_all_channels())
    logger.info("All channels joined. Adding event handlers.")
    client.add_event_handler(general_handler, events.NewMessage)
    client.add_event_handler(arab_handler, events.NewMessage(chats=arab_channels))
    client.add_event_handler(smart_handler, events.NewMessage(chats=smart_channels))
    logger.info("Event handlers added. Bot is running.")
    client.run_until_disconnected()
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()