import asyncio
import logging
import os

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

import database
import sip_call
from config import AUDIO_DIR

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()
_bot = None


def start_scheduler(bot):
    global _bot
    _bot = bot

    scheduler.add_job(
        _process_pending_calls,
        trigger=IntervalTrigger(seconds=30),
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
    logger.info("Scheduler started.")


def stop_scheduler():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


async def _process_pending_calls():
    try:
        calls = database.get_pending_calls()
    except Exception as e:
        logger.error("Failed to fetch pending calls: %s", e)
        return

    if not calls:
        return

    tasks = [_handle_call(call) for call in calls]
    await asyncio.gather(*tasks, return_exceptions=True)


async def _handle_call(call):
    call_id     = call["id"]
    telegram_id = call["telegram_id"]
    phone       = call["phone_number"]
    audio_path  = call["audio_path"]

    try:
        database.update_call_status(call_id, "in_progress")
    except Exception as e:
        logger.error("Could not mark call %s in_progress: %s", call_id, e)
        return

    logger.info("Processing call id=%s to %s", call_id, phone)

    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            sip_call.place_sip_call,
            call["sip_domain"],
            call["sip_username"],
            call["sip_password"],
            phone,
            audio_path,
        )
    except Exception as e:
        logger.exception("Executor error for call %s: %s", call_id, e)
        result = "failed"

    if result == "answered":
        status = "completed"
        msg = (
            f"✅ <b>Call Answered!</b>\n\n"
            f"📞 Number: <b>{phone}</b>\n"
            f"🆔 Call ID: <b>{call_id}</b>\n"
            f"🎵 Audio was played successfully and the call ended."
        )
    elif result == "not_answered":
        status = "not_answered"
        msg = (
            f"📵 <b>Call Not Answered.</b>\n\n"
            f"📞 Number: <b>{phone}</b>\n"
            f"🆔 Call ID: <b>{call_id}</b>\n"
            f"ℹ️ The recipient did not answer or the line was busy."
        )
    else:
        status = "failed"
        msg = (
            f"❌ <b>Call Failed.</b>\n\n"
            f"📞 Number: <b>{phone}</b>\n"
            f"🆔 Call ID: <b>{call_id}</b>\n"
            f"⚠️ A technical error occurred while placing the call."
        )

    try:
        database.update_call_status(call_id, status)
    except Exception as e:
        logger.error("Could not update call %s status: %s", call_id, e)

    if os.path.isfile(audio_path):
        try:
            os.remove(audio_path)
            logger.info("Audio deleted: %s", audio_path)
        except Exception as e:
            logger.warning("Could not delete audio %s: %s", audio_path, e)

    if _bot:
        try:
            await _bot.send_message(telegram_id, msg, parse_mode="HTML")
        except Exception as e:
            logger.warning("Could not notify user %s: %s", telegram_id, e)


async def _run_cleanup():
    """Delete old completed/failed calls from DB and orphaned audio files."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _do_cleanup)


def _do_cleanup():
    # 1. Delete DB rows for calls older than 7 days
    try:
        deleted_rows = database.cleanup_old_calls(keep_days=7)
        if deleted_rows:
            logger.info("Cleanup: deleted %d old call records from DB.", deleted_rows)
    except Exception as e:
        logger.error("Cleanup DB error: %s", e)

    # 2. Delete audio files that are no longer referenced by any active call
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
