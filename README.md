# Telegram Intel Monitor

Centralizes intelligence from multiple Telegram groups (Arabic + Israeli-smart sources) into one output group. Uses AI (Gemini) to extract, correlate, deduplicate, and score the credibility of reports — acting like a credible reporter.

## Architecture

```
Channels (arab + smart) → Listener → Pipeline → AI Extraction → Event Pool → Sender → Output Group
                                          ↕                          ↕
                                    Authority Tracker          Correlation Engine
                                          ↕
                                       SQLite DB

Companion Bot (@BotFather) — always online, handles remote session renewal via /login
```

## Features

- **Multi-source correlation** — detects when multiple channels report the same event
- **Authority scoring** — channels earn/lose credibility based on corroboration history
- **AI extraction** — Gemini 2.0 Flash extracts locations, entities, event types from Arabic/Hebrew/English
- **Credibility indicators** — output messages show emoji badges (🟢🟡🔴) and source counts
- **Deduplication** — SHA1 + Arabic diacritics normalization + in-memory cache + SQLite
- **Multi-account** — supports spreading channels across reader accounts to avoid rate limits
- **Remote session renewal** — companion bot lets you renew expired sessions from Telegram (no SSH)
- **Auto-deploy** — push to `main` → CircleCI builds & pushes to Docker Hub → Watchtower pulls on VM

---

## Setup Guide

### Prerequisites

- A Telegram account with [API credentials](https://my.telegram.org/apps) (`API_ID` + `API_HASH`)
- A [Gemini API key](https://aistudio.google.com/apikey)
- A Telegram Bot via [@BotFather](https://t.me/BotFather) (for remote session renewal)
- Your Telegram user ID (send `/start` to [@userinfobot](https://t.me/userinfobot))
- Docker + docker-compose on your Oracle VM
- A [Docker Hub](https://hub.docker.com/) account
- [CircleCI](https://circleci.com/) connected to your GitHub repo

### Step 1 — Generate a session string (run on your local machine)

```bash
cp config.env_example config.env
# Fill in: TELEGRAM_API_ID, TELEGRAM_API_HASH, PHONE_NUMBER

python gen_session.py
# → Sends you a code on Telegram
# → Enter it in the terminal
# → It prints a StringSession and offers to write it to config.env
```

### Step 2 — Set up CircleCI

In your CircleCI project settings → **Environment Variables**, add:

| Variable | Value |
|----------|-------|
| `DOCKERHUB_USERNAME` | Your Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub [access token](https://hub.docker.com/settings/security) |

That's it. CircleCI will build and push the image on every push to `main`.

### Step 3 — Set up the Oracle VM

SSH into your VM and create the project folder:

```bash
mkdir -p ~/telegram-intel
cd ~/telegram-intel
```

Create the config file:

```bash
nano config.env
```

Paste these values (fill in your own):

```env
# ─── Required ────────────────────────────────────────────
TELEGRAM_API_ID=12345678
TELEGRAM_API_HASH=abcdef1234567890abcdef1234567890
PHONE_NUMBER=+972501234567
TG_SESSION_STRING=1BVtsOH...your-string-from-step-1...

ARABS_SUMMARY_OUT=-1001234567890
SMART_CHAT=-1009876543210
GEMINI_API_KEY=AIza...your-gemini-key...

# ─── Companion bot ───────────────────────────────────────
BOT_TOKEN=7123456789:AAF...your-botfather-token...
ADMIN_ID=123456789

# ─── Optional (defaults are fine) ────────────────────────
GEMINI_MODEL=gemini-2.0-flash
LLM_BUDGET_HOURLY=120
LLM_RPM_LIMIT=14
BATCH_SIZE=24
MAX_BATCH_AGE=300
SUMMARY_MIN_INTERVAL=300
MIN_SOURCES=2
DB_PATH=data/intel.db
```

Create the channel lists:

```bash
nano arab_channels.txt    # one username per line, no @
nano smart_channels.txt   # one username per line, no @
```

Create `docker-compose.yml`:

```bash
nano docker-compose.yml
```

Paste:

```yaml
version: "3.8"

services:
  telegram-intel:
    image: YOUR_DOCKERHUB_USERNAME/telegram-intel:latest
    container_name: telegram-intel
    restart: unless-stopped
    env_file:
      - config.env
    volumes:
      - intel-data:/usr/src/app/data
      - ./arab_channels.txt:/usr/src/app/arab_channels.txt:ro
      - ./smart_channels.txt:/usr/src/app/smart_channels.txt:ro
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"

  watchtower:
    image: containrrr/watchtower
    container_name: watchtower
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - WATCHTOWER_CLEANUP=true
      - WATCHTOWER_POLL_INTERVAL=300
    command: telegram-intel

volumes:
  intel-data:
```

Start it:

```bash
docker compose up -d
docker compose logs -f telegram-intel
```

You should see the startup banner:
```
Telegram Intel Monitor — ONLINE
Account : YourName (id=123456789)
Session : StringSession
Channels: 21 arab | 7 smart
Helper  : companion bot active
```

### Step 4 — Verify the companion bot

Open Telegram and send `/status` to your bot. It should reply:

> 🟢 Userbot is **online** and running.

---

## How Updates Work

```
git push (main) → CircleCI builds image → pushes to Docker Hub
                                            ↓
                             Watchtower detects new image (every 5 min)
                                            ↓
                             Pulls → stops old container → starts new one
                                            ↓
                             Bot reconnects using same session + data volume
```

No SSH needed. Just push code.

## How Session Renewal Works

If Telegram revokes your session:

1. The bot enters **recovery mode** — companion bot stays alive
2. You get a message: *"⚠️ Userbot session expired! Send /login to renew."*
3. Send `/login` → enter your phone → enter the code → (2FA password if enabled)
4. New session is saved to the persistent Docker volume
5. Container restarts automatically → bot comes back online

No SSH needed. Everything happens in Telegram chat.

---

## Environment Variables Reference

### Required

| Variable | Description |
|----------|-------------|
| `TELEGRAM_API_ID` | From [my.telegram.org/apps](https://my.telegram.org/apps) |
| `TELEGRAM_API_HASH` | From [my.telegram.org/apps](https://my.telegram.org/apps) |
| `PHONE_NUMBER` | Your phone number with country code |
| `TG_SESSION_STRING` | Generated by `gen_session.py` |
| `ARABS_SUMMARY_OUT` | Chat ID for output messages |
| `GEMINI_API_KEY` | Google Gemini API key |

### Companion Bot (strongly recommended)

| Variable | Description |
|----------|-------------|
| `BOT_TOKEN` | Token from [@BotFather](https://t.me/BotFather) |
| `ADMIN_ID` | Your Telegram user ID (from [@userinfobot](https://t.me/userinfobot)) |

### Optional

| Variable | Default | Description |
|----------|---------|-------------|
| `SMART_CHAT` | `0` | Chat ID for smart-source forwarding |
| `GEMINI_MODEL` | `gemini-2.0-flash` | Gemini model to use |
| `LLM_BUDGET_HOURLY` | `120` | Max AI calls per hour |
| `MIN_SOURCES` | `2` | Sources needed for a trend report |
| `AUTHORITY_HIGH_THRESHOLD` | `75` | Score for high-authority fast-track |
| `BATCH_SIZE` | `24` | Messages per summary batch |
| `DB_PATH` | `data/intel.db` | SQLite database path |

---

## Project Structure

```
├── main.py              # Entry point — wires everything together
├── listener.py          # Telegram message ingestion
├── pipeline.py          # Message processing pipeline
├── correlation.py       # Event matching & dedup
├── ai.py                # Gemini AI client
├── authority.py         # Channel credibility scoring
├── sender.py            # Output formatting & sending
├── session_manager.py   # Companion bot for remote session renewal
├── models.py            # Shared data classes
├── db.py                # SQLite persistence
├── config.py            # Configuration loader
├── gen_session.py       # One-time session string generator (run locally)
├── Dockerfile           # Container image definition
├── docker-compose.yml   # VM deployment (intel bot + watchtower)
├── .circleci/config.yml # CI/CD pipeline
├── arab_channels.txt    # Arabic source channels
├── smart_channels.txt   # Israeli/smart source channels
└── tests/               # Unit tests
```

## Testing

```bash
pip install -r requirements.txt pytest
python -m pytest tests/ -v
```
