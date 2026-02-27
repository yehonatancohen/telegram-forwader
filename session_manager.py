#!/usr/bin/env python3
"""
Companion bot for remote session renewal.

Uses a standard Telegram Bot (@BotFather token) that is always online.
When the userbot session expires, the admin can renew it via Telegram chat
without SSH access to the server.

Commands:
    /status  — check if the userbot is alive
    /login   — start interactive session renewal
"""

from __future__ import annotations

import asyncio, logging, os
from pathlib import Path

from telethon import TelegramClient, events, errors
from telethon.sessions import StringSession

import config

logger = logging.getLogger("session_mgr")

# Persistent file on the data volume — survives container restarts
SESSION_OVERRIDE_FILE = config.DB_PATH.parent / ".session_override"


def get_session_string() -> str:
    """Return the best available session string: override file > env var."""
    if SESSION_OVERRIDE_FILE.is_file():
        stored = SESSION_OVERRIDE_FILE.read_text().strip()
        if stored:
            logger.info("using session override from %s", SESSION_OVERRIDE_FILE)
            return stored
    return config.SESSION_STRING


class SessionManager:
    """Companion bot that can renew the userbot session via Telegram chat."""

    def __init__(self):
        self.bot = TelegramClient(
            StringSession(), config.API_ID, config.API_HASH
        )
        self._login_state: dict = {}     # sender_id -> state dict
        self._userbot_alive = False

    async def start(self):
        await self.bot.start(bot_token=config.BOT_TOKEN)
        self._register_handlers()
        me = await self.bot.get_me()
        logger.info("companion bot @%s started", me.username)

    async def stop(self):
        await self.bot.disconnect()

    def set_userbot_status(self, alive: bool):
        self._userbot_alive = alive

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
                "🤖 **Intel Bot Companion**\n\n"
                "Commands:\n"
                "/status — check if the userbot is online\n"
                "/login — renew the userbot session",
                parse_mode="md",
            )
            logger.info("admin %d sent /start", event.sender_id)

        @self.bot.on(events.NewMessage(pattern="/status"))
        async def status_handler(event):
            if not self._is_admin(event.sender_id):
                return
            if self._userbot_alive:
                await event.respond("🟢 Userbot is **online** and running.")
            else:
                await event.respond(
                    "🔴 Userbot is **offline**.\n"
                    "Send /login to renew the session."
                )

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

        # Write to persistent data volume
        SESSION_OVERRIDE_FILE.parent.mkdir(parents=True, exist_ok=True)
        SESSION_OVERRIDE_FILE.write_text(session_str)
        logger.info("new session string saved to %s", SESSION_OVERRIDE_FILE)

        await event.respond(
            "✅ **Session renewed successfully!**\n\n"
            "Restarting the bot in 3 seconds...",
            parse_mode="md",
        )

        self._login_state.pop(sid, None)

        # Give time for the message to send, then exit.
        # Docker (restart: unless-stopped) will restart the container,
        # and on startup it reads the new session from the override file.
        await asyncio.sleep(3)
        logger.info("exiting for restart with new session...")
        os._exit(0)
