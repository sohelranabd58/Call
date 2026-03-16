# Telegram SIP Bot

## Overview
Telegram bot for scheduling automated SIP voice calls. Users add their SIP account, upload an audio file, pick a date/time, and the bot places the call automatically when the scheduled time arrives.

## Project Files
| File | Purpose |
|---|---|
| `main.py` | Bot entry point — aiogram handlers, FSM flows, polling |
| `database.py` | PostgreSQL CRUD — SIP accounts & scheduled calls |
| `scheduler.py` | APScheduler background job — fires pending calls every 30 s |
| `sip_call.py` | Places outbound SIP call via `pjsua` CLI tool |
| `config.py` | Loads env vars from `.env` using python-dotenv |
| `bot_requirements.txt` | Python dependencies |
| `audio/` | Uploaded audio files (git-ignored except `.gitkeep`) |

## Setup

### 1. Environment variables (.env)
```
BOT_TOKEN=your_telegram_bot_token
DATABASE_URL=postgresql://user:password@host:5432/dbname
```

### 2. Install dependencies
```bash
pip install -r bot_requirements.txt
```

### 3. Install pjsua (SIP client)
```bash
apt-get install pjsua
```

### 4. Run the bot
```bash
python main.py
```

## Tech Stack
- **aiogram 3.x** — async Telegram bot framework
- **APScheduler** — background job scheduler
- **psycopg2** — PostgreSQL driver
- **pydub** — audio duration validation
- **phonenumbers** — phone number validation
- **pjsua** (system tool) — SIP call placement

## Bot Flows
1. `/start` — shows main menu
2. **Add SIP Account** — saves domain/username/password per user
3. **Schedule Call** — phone → audio upload → date → time → saved
4. **My Scheduled Calls** — list with inline delete buttons
5. **Delete SIP Account** — removes SIP credentials

## Architecture Notes
- All secrets loaded from `.env` — nothing hardcoded
- Database tables auto-created on first run (`init_db()`)
- Scheduler checks every 30 seconds for due calls
- Audio files stored in `audio/` directory
