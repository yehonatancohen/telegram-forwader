import os, re, sys
from telethon import errors
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerChannel, MessageMediaPhoto, MessageMediaDocument
from telethon.errors import ChannelPrivateError, MediaCaptionTooLongError, SessionPasswordNeededError
from telethon import TelegramClient, events
from dotenv import load_dotenv
from easygoogletranslate import EasyGoogleTranslate
from pathlib import Path
from deep_translator import GoogleTranslator
from time import sleep
from langdetect import detect
import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

dotenv_path = Path('./config.env')
load_dotenv(dotenv_path=dotenv_path)

api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('PHONE_NUMBER')
arabs_chat = int(os.getenv('ARABS'))
smart_chat = int(os.getenv('SMART'))
owner_id = int(os.getenv('OWNER_ID'))

last_message = None
last_adv = False
adv_chat = None
media_groups = {}
translator = GoogleTranslator(source="auto", target='iw')
backup_translator = EasyGoogleTranslate()

blocked_message = [
    'צבע אדום',
    'גרם',
    'היכנסו למרחב המוגן',
    'חדירת כלי טיס עוין'
]

arab_channels = [
    '@a7rarjenin',
    '@QudsN',
    '@Electrohizbullah',
    '@SerajSat',
    '@shadysopoh',
    '@jeninqassam',
    '@Janin324',
    '@jenin4',
    '@anas_hoshia',
    '@abohamzahasanat',
    '@sarayajneen',
    '@abohamzahasanat',
    '@C_Military1',
    '@mmirleb',
    '@SabrenNews22',
    '@IraninArabic',
    '@iraninarabic_ir',
    '@meshheek',
    '@qassam1brigades',
    '@qassambrigades',
    '@duyuf1',
    '@Ail_2_9',
    '@alghalebun3',
    '@areennabluss'
]

smart_channels = [
    '@abualiexpress',
    '@arabworld301',
    '@AlealamAlearabiuEranMalca',
    '@AsrarLubnan'
]

if not all([api_id, api_hash, phone, arabs_chat, smart_chat]):
    raise ValueError("One or more environment variables are missing.")

client = TelegramClient('bot', api_id, api_hash)

def load_channels():
    with open('arab_channels.txt', 'r') as f:
        for line in f:
            arab_channels.append(line.strip())
    with open('smart_channels.txt', 'r') as f:
        for line in f:
            smart_channels.append(line.strip())

async def join_channel(channel):
    try:
        if not await check_client_in_channel(channel):
            await client(JoinChannelRequest(channel))
            logger.info(f"Joined channel {channel}")
    except errors.FloodWaitError as e:
        sleep(e.seconds)

async def join_channels():
    for channel in arab_channels + smart_channels:
        channel = channel[1:] if channel.startswith('@') else channel
        try:
            if not await check_client_in_channel(channel):
                await client(JoinChannelRequest(channel))
                logger.info(f"Joined channel {channel}")
        except errors.FloodWaitError as e:
            sleep(e.seconds)

async def check_client_in_channel(channel_username):
    try:
        channel = await client.get_entity(channel_username)
        
        full_channel = await client(GetFullChannelRequest(channel=channel))

        if full_channel.full_chat.participants_count > 0:
            logger.info(f"The client is in the channel {channel_username}")
            return True
        else:
            logger.info(f"The client is not in the channel {channel_username}")
            return False
    except ChannelPrivateError:
        logger.error(f"The channel {channel_username} is private or not accessible.")
        return False
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

async def general_handler(event):
    message = event.message
    logger.info(f"Received message from {message.chat_id}")
    if message.chat_id == owner_id:
        if message.message.startswith("/"):
            await command_handler(message.message.split(' ')[0].split('/')[1], message.chat_id, message.message.split(' ')[1:])

async def arab_handler(event):
    message = event.message
    await send_message_to_telegram_chat(message, arabs_chat)

async def smart_handler(event):
    global last_adv
    global adv_chat
    if ("°תוכן שיווקי" in event.message.message):
        last_adv = True
        adv_chat = event.chat_id
        logger.info("Promotional message")
        return
    if ((last_adv and adv_chat == event.chat_id)):
        logger.info("Promotional message")
        last_adv = False
        return
    message = event.message
    await send_message_to_telegram_chat(message, smart_chat)

def is_blocked_message(message):
    for blocked in blocked_message:
        if blocked in message.split():
            logger.error(f'Blocked message, cause: {blocked} - full message: {message}')
            return True
    return False

async def send_message_to_telegram_chat(message, target_chat_id):
    caption = await process_message(message)
    if (await check_if_message_sent(target_chat_id, caption, message)):
        logger.error("Message already sent")
        return
    if (message.media != None):
        if type(message.media) == MessageMediaPhoto:
            if message.media.photo.dc_id > 1:
                await grouped_handler(message, target_chat_id, caption)
                return
        elif type(message.media) == MessageMediaDocument:
            await client.send_file(entity=target_chat_id, file=message.media.document, caption=caption)
            logger.info(f'Sent message with document to {target_chat_id}')
            return
        else:
            await client.send_file(entity=target_chat_id, file=message.media.file, caption=caption)
            logger.info(f'Sent message with media to {target_chat_id}')
            return
    else:
        await client.send_message(entity=target_chat_id, message=caption, link_preview=False)
        logger.info(f'Sent message to {target_chat_id}')


async def grouped_handler(message, target_chat_id, caption):
    grouped_message = await fetch_media_groups_as_objects(message)
    if (grouped_message == 'working'):
        return
    try:
        album = grouped_message[message.grouped_id]
        if (album[0] == "sent"):
            return
        await client.send_file(entity=target_chat_id, file=album[1:], caption=grouped_message[message.grouped_id][0], link_preview=False)
        grouped_message[message.grouped_id].append("sent")
        grouped_message.pop(message.grouped_id)
        logger.info(f'Sent message with photos to {target_chat_id}')
    except Exception as e:
        if isinstance(e, MediaCaptionTooLongError):
            sent_file = await client.send_file(entity=target_chat_id, file=grouped_message[message.grouped_id])
            await client.send_message(entity=target_chat_id, message=caption, link_preview=False, reply_to=sent_file)
            grouped_message.pop(message.grouped_id)
            logger.info(f'Sent message with photos to {target_chat_id}')
        else:
            logger.error(f"An error occurred: {e}")

async def fetch_media_groups_as_objects(message):
    global media_groups
    offset_id = 0
    limit = 30
    chat = message.chat_id
    if message.grouped_id in media_groups:
        return 'working'
    media_groups[message.grouped_id] = []
    messages = await client.get_messages(chat, limit=limit, offset_id=offset_id)
    messages = [msg for msg in messages if msg.grouped_id == message.grouped_id]
    if not messages:
        return
    for message in messages:
        if message.grouped_id:
            media_groups[message.grouped_id].append(message.media)
            if message.message != '' and type(media_groups[message.grouped_id][0]) != type(''):
                caption = await process_message(message)
                media_groups[message.grouped_id].insert(0, caption)   
    return media_groups

async def process_message(message):
    global translator
    url_pattern = re.compile(r'http[s]?://\S+|www\.\S+')
    caption = url_pattern.sub('', message.message)
    if ((caption == '' and not message.file) or is_blocked_message(caption)):
        logger.error(f'Blocked message: {caption}')
        return
    try:
        lang = detect(caption)
    except Exception as e:
        lang = 'iw'
    try:
        if (lang != 'iw' and lang != 'he'):
            caption = translator.translate(caption)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        try:
            caption = backup_translator.translate(caption, 'iw')
        except Exception as e:
            caption = "Couldn't translate message.\n" + caption
    link = await get_message_link(message.chat_id, message.id)
    caption += f'\n\n{link}'
    return caption

async def add_channel(channel_id, list, list_name):
    match = re.match(r'(https://t\.me/)?(?P<username>[a-zA-Z0-9_]+)', channel_id)
    if match:
        username = match.group('username')
        list.append(f"@{username}")
        await join_channel(username)
    with open(f'{list_name}_channels.txt', 'a') as f:
        f.write(f'{username}\n')

def remove_channel(channel_id, list):
    list.remove(channel_id)

async def command_handler(command, chat_id, args):
    match command:
        case "add_channel_arab":
            await add_channel(args[0], arab_channels, "arab")
        case "add_channel_smart":
            await add_channel(args[0], smart_channels, "smart")
        case "remove_channel":
            remove_channel(args[0], arab_channels)
        case "list_channels":
            client.send_message(chat_id, f'Arab channels: {arab_channels}')
            client.send_message(chat_id, f'Smart channels: {smart_channels}')
        case "help":
            client.send_message(chat_id, 'Commands: add_channel_arab, add_channel_smart, remove_channel, list_channels')
        case _:
            pass

async def get_message_link(channel_username, message_id):
    # Get the input entity for the channel
    channel = await client.get_entity(channel_username)
    # Construct the message link
    if hasattr(channel, 'username') and channel.username:
        message_link = f"https://t.me/{channel.username}/{message_id}"
        return message_link
    
async def check_if_message_sent(channel_username, caption, message_obj):
    try:
        channel = await client.get_entity(channel_username)
        history = await client(GetHistoryRequest(
            peer=PeerChannel(channel.id),
            limit=50,
            offset_date=None,
            offset_id=0,
            max_id=0,
            min_id=0,
            add_offset=0,
            hash=0
        ))
        for message in history.messages:
            if message.media != None and message_obj.media != None:
                if type(message.media) == type(message_obj.media):
                    if message_obj.media == message.media:
                        return True
            if message.message:
                if caption == message.message:
                    return True
        return False
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

async def main():
    try:
        logger.info("Connecting to Telegram...")
        await client.start(phone=lambda: phone)
        if not await client.is_user_authorized():
            await client.send_code_request(phone)
            logger.info("Check your phone for the authentication code.")

            @client.on(events.NewMessage)
            async def handler(event):
                if event.text.isdigit():
                    code = event.text.strip()
                    try:
                        await client.sign_in(phone, code)
                        logger.info("Successfully signed in!")
                    except SessionPasswordNeededError:
                        logger.error("2FA enabled. Please provide your password.")
                    except Exception as e:
                        logger.error(f"Failed to sign in: {e}")
                else:
                    logger.error("Please send a valid authentication code.")
        else:
            logger.info("Already authorized.")
        logger.info("Connected to Telegram successfully!")
        load_channels()
        logger.info("Loaded channels")
        await join_channels()
        logger.info("Joined channels")
        client.add_event_handler(general_handler, events.NewMessage)
        client.add_event_handler(arab_handler, events.NewMessage(chats=arab_channels))
        client.add_event_handler(smart_handler, events.NewMessage(chats=smart_channels))
        await client.run_until_disconnected()
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        
async def run():
    await main()
    await client.run_until_disconnected()

client.loop.run_until_complete(run())