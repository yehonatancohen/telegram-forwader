#!/usr/bin/env python3
"""
One-time helper — run locally to generate a StringSession.
Paste the output into your config.env / environment variables.

Usage:
    python gen_session.py                   # generate primary session
    python gen_session.py --reader B        # generate reader session B
"""

from __future__ import annotations

import argparse, asyncio, json, os, sys
from pathlib import Path

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv(Path("config.env"))


async def generate(api_id: int, api_hash: str, phone: str, label: str):
    client = TelegramClient(StringSession(), api_id, api_hash)
    await client.start(phone=lambda: phone)

    if not await client.is_user_authorized():
        print("Login failed.", file=sys.stderr)
        sys.exit(1)

    session_str = client.session.save()
    await client.disconnect()

    print(f"\n{'='*60}")
    print(f"  Session for: {label} ({phone})")
    print(f"{'='*60}")
    print(f"\n{session_str}\n")

    # Offer to auto-write to config.env
    config_path = Path("config.env")
    if config_path.is_file():
        answer = input("Write session string to config.env? [y/N] ").strip().lower()
        if answer == "y":
            content = config_path.read_text()
            if "TG_SESSION_STRING=" in content:
                import re
                content = re.sub(
                    r"^TG_SESSION_STRING=.*$",
                    f"TG_SESSION_STRING={session_str}",
                    content,
                    flags=re.MULTILINE,
                )
            else:
                content += f"\nTG_SESSION_STRING={session_str}\n"
            config_path.write_text(content)
            print(f"✓ Written to {config_path}")
        else:
            print("Skipped. Add manually to config.env:")
            print(f"  TG_SESSION_STRING={session_str}")
    else:
        print("Add this to your config.env:")
        print(f"  TG_SESSION_STRING={session_str}")

    print(f"\n{'='*60}")
    print("  Next steps:")
    print("  1. Add TG_SESSION_STRING to ~/telegram-intel/config.env on your VM")
    print("  2. Push code to main → CircleCI builds → Watchtower deploys")
    print(f"{'='*60}\n")
    return session_str


def main():
    parser = argparse.ArgumentParser(description="Generate Telethon StringSession")
    parser.add_argument("--reader", type=str, default=None,
                        help="Reader label (e.g. 'B' or 'C') to generate a reader session")
    args = parser.parse_args()

    if args.reader:
        readers = json.loads(os.getenv("TG_READERS_JSON", "[]"))
        match = [r for r in readers if r["session"].endswith(f"-{args.reader}")]
        if not match:
            print(f"No reader '{args.reader}' found in TG_READERS_JSON", file=sys.stderr)
            sys.exit(1)
        cfg = match[0]
        asyncio.run(generate(cfg["api_id"], cfg["api_hash"],
                             cfg.get("phone", ""), f"reader-{args.reader}"))
    else:
        api_id = int(os.getenv("TELEGRAM_API_ID", "0"))
        api_hash = os.getenv("TELEGRAM_API_HASH", "")
        phone = os.getenv("PHONE_NUMBER", "")
        if not (api_id and api_hash and phone):
            print("Set TELEGRAM_API_ID, TELEGRAM_API_HASH, PHONE_NUMBER in config.env",
                  file=sys.stderr)
            sys.exit(1)
        asyncio.run(generate(api_id, api_hash, phone, "primary"))


if __name__ == "__main__":
    main()
