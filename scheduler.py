import asyncio
import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import database
import sip_call
from config import (
    AUDIO_DIR,
    MAX_CALL_RETRIES,
    RETRY_DELAY_SECONDS,
    SCHEDULER_INTERVAL_SECONDS,
)

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
_bot = None
_processing = False


def start_scheduler(bot):
    global _bot
    _bot = bot

    scheduler.add_job(
        _process_pending_calls,
        trigger=IntervalTrigger(seconds=SCHEDULER_INTERVAL_SECONDS),
        id="check_pending_calls",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        _run_cleanup,
        trigger=IntervalTrigger(hours=24),
        id="daily_cleanup",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    logger.info("Scheduler started (interval=%ds, max_retries=%d).",
                SCHEDULER_INTERVAL_SECONDS, MAX_CALL_RETRIES)


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


async def _process_pending_calls():
    global _processing
    if _processing:
        logger.debug("Previous processing cycle still running, skipping.")
        return
    _processing = True

    try:
        calls = database.get_pending_calls()
    except Exception as e:
        logger.error("Failed to fetch pending calls: %s", e)
        _processing = False
        return

    if not calls:
        _processing = False
        return

    logger.info("Processing %d pending call(s) sequentially.", len(calls))
    for call in calls:
        try:
            await _handle_call(call)
        except Exception as e:
            logger.exception("Unhandled error processing call %s: %s", call.get("id"), e)

    _processing = False


async def _handle_call(call):
    call_id = call["id"]
    telegram_id = call["telegram_id"]
    phone = call["phone_number"]
    audio_path = call["audio_path"]
    retry_count = call.get("retry_count", 0)

    try:
        database.update_call_status(call_id, "in_progress")
    except Exception as e:
        logger.error("Could not mark call %s in_progress: %s", call_id, e)
        return

    attempt_label = f"attempt {retry_count + 1}" if retry_count > 0 else "first attempt"
    logger.info("Processing call id=%s to %s (%s)", call_id, phone, attempt_label)

    country_code_prefix = call.get("country_code_prefix", "+88")

    loop = asyncio.get_running_loop()
    try:
        result, detail = await loop.run_in_executor(
            None,
            sip_call.place_sip_call,
            call["sip_domain"],
            call["sip_username"],
            call["sip_password"],
            phone,
            audio_path,
            country_code_prefix,
        )
    except Exception as e:
        logger.exception("Executor error for call %s: %s", call_id, e)
        result, detail = "failed", f"Internal error: {e}"

    if result == "answered":
        database.update_call_status(call_id, "completed", last_result=detail)
        msg = (
            f"<b>Call Answered!</b>\n\n"
            f"Number: <b>{phone}</b>\n"
            f"Call ID: <b>{call_id}</b>\n"
            f"Audio was played successfully and the call ended."
        )
        _cleanup_audio(audio_path)
        await _notify(telegram_id, msg)

    elif result in ("not_answered", "failed") and retry_count < MAX_CALL_RETRIES:
        database.increment_retry(call_id, delay_seconds=RETRY_DELAY_SECONDS)
        status_label = "Not Answered" if result == "not_answered" else "Failed"
        logger.info("Call %s %s (%s). Retry scheduled in %ds. (retry %d/%d)",
                     call_id, result, detail, RETRY_DELAY_SECONDS, retry_count + 1, MAX_CALL_RETRIES)
        await _notify(telegram_id,
            f"<b>Call {status_label}</b>\n\n"
            f"Number: <b>{phone}</b>\n"
            f"Call ID: <b>{call_id}</b>\n"
            f"Reason: {detail}\n\n"
            f"Retrying automatically in {RETRY_DELAY_SECONDS} seconds..."
        )

    elif result == "not_answered":
        database.update_call_status(call_id, "not_answered", last_result=detail)
        msg = (
            f"<b>Call Not Answered</b>\n\n"
            f"Number: <b>{phone}</b>\n"
            f"Call ID: <b>{call_id}</b>\n"
            f"Reason: {detail}\n"
            f"All retry attempts exhausted."
        )
        _cleanup_audio(audio_path)
        await _notify(telegram_id, msg)

    else:
        database.update_call_status(call_id, "failed", last_result=detail)
        msg = (
            f"<b>Call Failed</b>\n\n"
            f"Number: <b>{phone}</b>\n"
            f"Call ID: <b>{call_id}</b>\n"
            f"Reason: {detail}\n"
            f"All retry attempts exhausted."
        )
        _cleanup_audio(audio_path)
        await _notify(telegram_id, msg)


def _cleanup_audio(audio_path):
    if os.path.isfile(audio_path):
        try:
            os.remove(audio_path)
            logger.info("Audio deleted: %s", audio_path)
        except Exception as e:
            logger.warning("Could not delete audio %s: %s", audio_path, e)


async def _notify(telegram_id, text):
    if not _bot:
        return
    try:
        await _bot.send_message(telegram_id, text, parse_mode="HTML")
    except Exception as e:
        logger.warning("Could not notify user %s: %s", telegram_id, e)


async def _run_cleanup():
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do_cleanup)


def _do_cleanup():
    try:
        deleted_rows = database.cleanup_old_calls(keep_days=7)
        if deleted_rows:
            logger.info("Cleanup: deleted %d old call records from DB.", deleted_rows)
    except Exception as e:
        logger.error("Cleanup DB error: %s", e)

    try:
        active_paths = database.get_all_audio_paths()
        if not os.path.isdir(AUDIO_DIR):
            return
        cleaned = 0
        for fname in os.listdir(AUDIO_DIR):
            fpath = os.path.join(AUDIO_DIR, fname)
            if os.path.isfile(fpath) and fpath not in active_paths:
                try:
                    os.remove(fpath)
                    cleaned += 1
                    logger.debug("Cleanup: removed orphaned audio %s", fpath)
                except Exception as e:
                    logger.warning("Cleanup: could not remove %s: %s", fpath, e)
        if cleaned:
            logger.info("Cleanup: removed %d orphaned audio files.", cleaned)
    except Exception as e:
        logger.error("Cleanup audio error: %s", e)
