#!/usr/bin/env python3
"""
Companion bot for remote session renewal and bot management.

Uses a standard Telegram Bot (@BotFather token) that is always online.
When the userbot session expires, the admin can renew it via Telegram chat.
Also provides management commands: add/remove channels, view stats, restart.

Commands:
    /start    — show help
    /status   — check if the userbot is alive
    /login    — start interactive session renewal
    /channels — list all monitored channels
    /add_arab <link or username> — add an arab source channel
    /add_smart <link or username> — add a smart source channel
    /remove <username> — remove a channel from any list
    /stats    — show pipeline statistics
    /restart  — restart the bot (picks up new channels)
"""

from __future__ import annotations

import asyncio, logging, os, re
from pathlib import Path

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

import config

logger = logging.getLogger("session_mgr")

# Persistent file on the data volume — survives container restarts
SESSION_OVERRIDE_FILE = config.DB_PATH.parent / ".session_override"

# Channel list file paths
ARAB_FILE = config.ARAB_SOURCES_FILE
SMART_FILE = config.SMART_SOURCES_FILE

# Regex to extract username from t.me links
_TG_LINK_RE = re.compile(
    r"(?:https?://)?(?:t\.me|telegram\.me)/(?:joinchat/)?@?([A-Za-z0-9_]+)"
)


def get_session_string() -> str:
    """Return the best available session string: override file > env var."""
    if SESSION_OVERRIDE_FILE.is_file():
        stored = SESSION_OVERRIDE_FILE.read_text().strip()
        if stored:
            logger.info("using session override from %s", SESSION_OVERRIDE_FILE)
            return stored
    return config.SESSION_STRING


def _parse_username(text: str) -> str | None:
    """Extract a channel username from a link or raw text."""
    text = text.strip().lstrip("@")
    m = _TG_LINK_RE.match(text)
    if m:
        return m.group(1).lower()
    # Plain username
    if re.match(r"^[A-Za-z0-9_]{3,}$", text):
        return text.lower()
    return None


def _read_channels(path: Path) -> list[str]:
    if not path.is_file():
        return []
    return sorted({
        line.strip().lstrip("@").lower()
        for line in path.read_text().splitlines()
        if line.strip()
    })


def _write_channels(path: Path, channels: list[str]):
    path.write_text("\n".join(sorted(set(channels))) + "\n")


class SessionManager:
    """Companion bot for session renewal + channel management."""

    def __init__(self):
        self.bot = TelegramClient(
            StringSession(), config.API_ID, config.API_HASH
        )
        self._login_state: dict = {}
        self._userbot_alive = False
        self._pipeline_stats: dict | None = None  # set by main.py

    async def start(self):
        await self.bot.start(bot_token=config.BOT_TOKEN)
        self._register_handlers()
        me = await self.bot.get_me()
        logger.info("companion bot @%s started", me.username)

    async def stop(self):
        await self.bot.disconnect()

    def set_userbot_status(self, alive: bool):
        self._userbot_alive = alive

    def set_pipeline_stats(self, stats: dict):
        """Called from main.py to share pipeline stats reference."""
        self._pipeline_stats = stats

    async def notify_session_expired(self):
        """Alert the admin when the userbot session dies."""
        try:
            await self.bot.send_message(
                config.ADMIN_ID,
                "⚠️ **Userbot session expired!**\n\n"
                "The intel bot cannot connect to Telegram.\n"
                "Send /login to renew the session right here.",
                parse_mode="md",
            )
            logger.info("admin notified about session expiry")
        except Exception as e:
            logger.error(
                "failed to notify admin (id=%d): %s — "
                "make sure you've sent /start to the bot first!",
                config.ADMIN_ID, e,
            )

    def _is_admin(self, sender_id: int) -> bool:
        return sender_id == config.ADMIN_ID

    # ─── Handler registration ────────────────────────────────────────────
    def _register_handlers(self):

        @self.bot.on(events.NewMessage(pattern="/start"))
        async def start_handler(event):
            if not self._is_admin(event.sender_id):
                await event.respond("⛔ Unauthorized.")
                return
            await event.respond(
                "🤖 **Intel Bot Manager**\n\n"
                "**Session:**\n"
                "/status — check if userbot is online\n"
                "/login — renew expired session\n"
                "/restart — restart the bot\n\n"
                "**Channels:**\n"
                "/channels — list all monitored channels\n"
                "/add\\_arab `<link or username>` — add arab source\n"
                "/add\\_smart `<link or username>` — add smart source\n"
                "/remove `<username>` — remove from any list\n\n"
                "**Info:**\n"
                "/stats — pipeline statistics",
                parse_mode="md",
            )

        @self.bot.on(events.NewMessage(pattern="/status"))
        async def status_handler(event):
            if not self._is_admin(event.sender_id):
                return
            if self._userbot_alive:
                arab = _read_channels(ARAB_FILE)
                smart = _read_channels(SMART_FILE)
                await event.respond(
                    f"🟢 Userbot is **online**\n"
                    f"📡 {len(arab)} arab | {len(smart)} smart channels",
                )
            else:
                await event.respond(
                    "🔴 Userbot is **offline**.\n"
                    "Send /login to renew the session."
                )

        # ─── Channel management ──────────────────────────────────────────
        @self.bot.on(events.NewMessage(pattern=r"/channels"))
        async def channels_handler(event):
            if not self._is_admin(event.sender_id):
                return
            arab = _read_channels(ARAB_FILE)
            smart = _read_channels(SMART_FILE)

            lines = ["📡 **Monitored Channels**\n"]
            lines.append(f"**Arab ({len(arab)}):**")
            for ch in arab:
                lines.append(f"  • @{ch}")
            lines.append(f"\n**Smart ({len(smart)}):**")
            for ch in smart:
                lines.append(f"  • @{ch}")
            lines.append(f"\nTotal: {len(arab) + len(smart)}")

            await event.respond("\n".join(lines), parse_mode="md")

        @self.bot.on(events.NewMessage(pattern=r"/add_arab\s+(.+)"))
        async def add_arab_handler(event):
            if not self._is_admin(event.sender_id):
                return
            username = _parse_username(event.pattern_match.group(1))
            if not username:
                await event.respond("❌ Invalid username or link.")
                return
            channels = _read_channels(ARAB_FILE)
            if username in channels:
                await event.respond(f"⚠️ @{username} already in arab list.")
                return
            channels.append(username)
            _write_channels(ARAB_FILE, channels)
            await event.respond(
                f"✅ Added @{username} to **arab** sources ({len(channels)} total).\n"
                "Send /restart to apply.",
                parse_mode="md",
            )
            logger.info("admin added @%s to arab channels", username)

        @self.bot.on(events.NewMessage(pattern=r"/add_smart\s+(.+)"))
        async def add_smart_handler(event):
            if not self._is_admin(event.sender_id):
                return
            username = _parse_username(event.pattern_match.group(1))
            if not username:
                await event.respond("❌ Invalid username or link.")
                return
            channels = _read_channels(SMART_FILE)
            if username in channels:
                await event.respond(f"⚠️ @{username} already in smart list.")
                return
            channels.append(username)
            _write_channels(SMART_FILE, channels)
            await event.respond(
                f"✅ Added @{username} to **smart** sources ({len(channels)} total).\n"
                "Send /restart to apply.",
                parse_mode="md",
            )
            logger.info("admin added @%s to smart channels", username)

        @self.bot.on(events.NewMessage(pattern=r"/remove\s+(.+)"))
        async def remove_handler(event):
            if not self._is_admin(event.sender_id):
                return
            username = _parse_username(event.pattern_match.group(1))
            if not username:
                await event.respond("❌ Invalid username.")
                return

            removed_from = []
            for label, path in [("arab", ARAB_FILE), ("smart", SMART_FILE)]:
                channels = _read_channels(path)
                if username in channels:
                    channels.remove(username)
                    _write_channels(path, channels)
                    removed_from.append(label)

            if removed_from:
                await event.respond(
                    f"✅ Removed @{username} from: {', '.join(removed_from)}.\n"
                    "Send /restart to apply.",
                    parse_mode="md",
                )
                logger.info("admin removed @%s from %s", username, removed_from)
            else:
                await event.respond(f"⚠️ @{username} not found in any list.")

        # ─── Stats ───────────────────────────────────────────────────────
        @self.bot.on(events.NewMessage(pattern="/stats"))
        async def stats_handler(event):
            if not self._is_admin(event.sender_id):
                return
            if self._pipeline_stats:
                s = self._pipeline_stats
                await event.respond(
                    "📊 **Pipeline Stats**\n\n"
                    f"📨 Messages processed: {s.get('messages', 0)}\n"
                    f"🔍 Events detected: {s.get('events', 0)}\n"
                    f"📤 Summaries sent: {s.get('summaries', 0)}\n"
                    f"❌ Errors: {s.get('errors', 0)}",
                    parse_mode="md",
                )
            else:
                await event.respond("📊 Stats not available (pipeline not started).")

        # ─── Restart ─────────────────────────────────────────────────────
        @self.bot.on(events.NewMessage(pattern="/restart"))
        async def restart_handler(event):
            if not self._is_admin(event.sender_id):
                return
            await event.respond(
                "🔄 Restarting bot in 3 seconds...\n"
                "Docker will bring it back up automatically."
            )
            logger.info("admin requested restart")
            await asyncio.sleep(3)
            os._exit(0)

        # ─── Session login flow ──────────────────────────────────────────
        @self.bot.on(events.NewMessage(pattern="/login"))
        async def login_handler(event):
            if not self._is_admin(event.sender_id):
                return
            self._login_state[event.sender_id] = {"step": "phone"}
            await event.respond(
                "📱 Send your phone number (with country code, e.g. +972...):"
            )

        @self.bot.on(events.NewMessage())
        async def message_handler(event):
            if not self._is_admin(event.sender_id):
                return
            state = self._login_state.get(event.sender_id)
            if not state:
                return
            text = event.raw_text.strip()
            if text.startswith("/"):
                return

            if state["step"] == "phone":
                await self._handle_phone(event, text)
            elif state["step"] == "code":
                await self._handle_code(event, text)
            elif state["step"] == "2fa":
                await self._handle_2fa(event, text)

    # ─── Login flow steps ────────────────────────────────────────────────
    async def _handle_phone(self, event, phone: str):
        sid = event.sender_id
        try:
            temp_client = TelegramClient(
                StringSession(), config.API_ID, config.API_HASH
            )
            await temp_client.connect()
            result = await temp_client.send_code_request(phone)

            self._login_state[sid] = {
                "step": "code",
                "phone": phone,
                "phone_code_hash": result.phone_code_hash,
                "client": temp_client,
            }
            await event.respond(
                "✉️ Code sent to your phone! Enter the login code:"
            )
        except Exception as e:
            logger.exception("send_code_request failed")
            await event.respond(f"❌ Failed to send code: `{e}`")
            self._login_state.pop(sid, None)

    async def _handle_code(self, event, code: str):
        sid = event.sender_id
        state = self._login_state[sid]
        client: TelegramClient = state["client"]

        try:
            await client.sign_in(
                phone=state["phone"],
                code=code,
                phone_code_hash=state["phone_code_hash"],
            )
            await self._save_and_restart(event, client)
        except errors.SessionPasswordNeededError:
            self._login_state[sid]["step"] = "2fa"
            await event.respond("🔐 2FA is enabled. Enter your password:")
        except Exception as e:
            logger.exception("sign_in failed")
            await event.respond(
                f"❌ Login failed: `{e}`\nSend /login to try again."
            )
            await client.disconnect()
            self._login_state.pop(sid, None)

    async def _handle_2fa(self, event, password: str):
        sid = event.sender_id
        state = self._login_state[sid]
        client: TelegramClient = state["client"]

        try:
            await client.sign_in(password=password)
            await self._save_and_restart(event, client)
        except Exception as e:
            logger.exception("2fa sign_in failed")
            await event.respond(
                f"❌ 2FA failed: `{e}`\nSend /login to try again."
            )
            await client.disconnect()
            self._login_state.pop(sid, None)

    async def _save_and_restart(self, event, client: TelegramClient):
        """Save new session string to persistent file and restart."""
        sid = event.sender_id
        session_str = client.session.save()
        await client.disconnect()

        SESSION_OVERRIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
        SESSION_OVERRIDE_FILE.write_text(session_str)
        logger.info("new session string saved to %s", SESSION_OVERRIDE_FILE)

        await event.respond(
            "✅ **Session renewed successfully!**\n\n"
            "Restarting the bot in 3 seconds...",
            parse_mode="md",
        )

        self._login_state.pop(sid, None)

        await asyncio.sleep(3)
        logger.info("exiting for restart with new session...")
        os._exit(0)
