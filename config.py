import os
import sys

BOT_TOKEN = os.environ.get("BOT_TOKEN", "8621797741:AAGN_W1nu5jWFBqlncKe8jIzWaHeK38qsKo")
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN environment variable is required. Set it and restart.")
    sys.exit(1)

AUDIO_DIR = "audio"
MAX_AUDIO_DURATION_SECONDS = 60
SIP_DEFAULT_PORT = 5060

COUNTRY_CODE_PREFIX = "+88"

MAX_CALL_RETRIES = 0

SCHEDULER_INTERVAL_SECONDS = 30

CALL_TIMEOUT_SECONDS = 90
PJSUA_DURATION_SECONDS = 55
