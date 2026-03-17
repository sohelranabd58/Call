import json
import os
import tempfile
import threading
import logging
from datetime import datetime, timedelta

import pytz

logger = logging.getLogger(__name__)

DATA_DIR = "data"
SIP_FILE = os.path.join(DATA_DIR, "sip_accounts.json")
CALLS_FILE = os.path.join(DATA_DIR, "scheduled_calls.json")

_lock = threading.Lock()

BD_TZ = pytz.timezone("Asia/Dhaka")
BD_TIME_FMT = "%Y-%m-%d %H:%M:%S"


def now_bd() -> datetime:
    return datetime.now(BD_TZ).replace(tzinfo=None)


def now_bd_str() -> str:
    return now_bd().strftime(BD_TIME_FMT)


def _ensure_data_dir():
    os.makedirs(DATA_DIR, exist_ok=True)


def _read_json(filepath, default):
    if not os.path.exists(filepath):
        return default
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return default


def _write_json(filepath, data):
    _ensure_data_dir()
    dir_name = os.path.dirname(filepath) or "."
    fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def init_db():
    _ensure_data_dir()
    with _lock:
        sip = _read_json(SIP_FILE, {})
        _write_json(SIP_FILE, sip)

        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        for call in calls.get("records", []):
            if call.get("status") == "in_progress":
                call["status"] = "pending"
                call["retry_count"] = call.get("retry_count", 0)
        _write_json(CALLS_FILE, calls)


def save_sip_account(telegram_id, sip_domain, sip_username, sip_password, country_code_prefix=None):
    from config import COUNTRY_CODE_PREFIX
    with _lock:
        sip = _read_json(SIP_FILE, {})
        sip[str(telegram_id)] = {
            "telegram_id": telegram_id,
            "sip_domain": sip_domain,
            "sip_username": sip_username,
            "sip_password": sip_password,
            "country_code_prefix": country_code_prefix if country_code_prefix is not None else COUNTRY_CODE_PREFIX,
            "created_at": now_bd_str(),
        }
        _write_json(SIP_FILE, sip)


def get_sip_account(telegram_id):
    with _lock:
        sip = _read_json(SIP_FILE, {})
        return sip.get(str(telegram_id))


def delete_sip_account(telegram_id):
    with _lock:
        sip = _read_json(SIP_FILE, {})
        key = str(telegram_id)
        if key in sip:
            del sip[key]
            _write_json(SIP_FILE, sip)
            return True
        return False


def save_scheduled_call(telegram_id, phone_number, audio_path, scheduled_at_bd_str):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        new_id = calls.get("next_id", 1)
        calls["next_id"] = new_id + 1
        calls["records"].append({
            "id": new_id,
            "telegram_id": telegram_id,
            "phone_number": phone_number,
            "audio_path": audio_path,
            "scheduled_at": scheduled_at_bd_str,
            "status": "pending",
            "retry_count": 0,
            "last_result": None,
            "created_at": now_bd_str(),
        })
        _write_json(CALLS_FILE, calls)
        return new_id


def get_scheduled_calls(telegram_id):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        result = [
            c for c in calls["records"]
            if c["telegram_id"] == telegram_id and c["status"] in ("pending", "retry_pending")
        ]
        result.sort(key=lambda c: c["scheduled_at"])
        return result


def get_pending_calls():
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        sip = _read_json(SIP_FILE, {})
        now = now_bd_str()
        result = []
        for c in calls["records"]:
            if c["status"] in ("pending", "retry_pending") and c["scheduled_at"] <= now:
                account = sip.get(str(c["telegram_id"]))
                if account:
                    merged = dict(c)
                    merged["sip_domain"] = account["sip_domain"]
                    merged["sip_username"] = account["sip_username"]
                    merged["sip_password"] = account["sip_password"]
                    merged["country_code_prefix"] = account.get("country_code_prefix", "+88")
                    result.append(merged)
        return result


def update_call_status(call_id, status, last_result=None):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        for c in calls["records"]:
            if c["id"] == call_id:
                c["status"] = status
                if last_result is not None:
                    c["last_result"] = last_result
                break
        _write_json(CALLS_FILE, calls)


def increment_retry(call_id, delay_seconds=60):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        for c in calls["records"]:
            if c["id"] == call_id:
                c["retry_count"] = c.get("retry_count", 0) + 1
                c["status"] = "retry_pending"
                next_at = now_bd() + timedelta(seconds=delay_seconds)
                c["scheduled_at"] = next_at.strftime(BD_TIME_FMT)
                break
        _write_json(CALLS_FILE, calls)


def get_retry_count(call_id):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        for c in calls["records"]:
            if c["id"] == call_id:
                return c.get("retry_count", 0)
        return 0


def delete_scheduled_call(call_id, telegram_id):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        before = len(calls["records"])
        calls["records"] = [
            c for c in calls["records"]
            if not (c["id"] == call_id and c["telegram_id"] == telegram_id)
        ]
        if len(calls["records"]) < before:
            _write_json(CALLS_FILE, calls)
            return True
        return False


def cleanup_old_calls(keep_days=7):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        cutoff = (now_bd() - timedelta(days=keep_days)).strftime(BD_TIME_FMT)
        terminal = {"completed", "failed", "not_answered"}
        before = len(calls["records"])
        calls["records"] = [
            c for c in calls["records"]
            if not (c["status"] in terminal and c.get("created_at", "") < cutoff)
        ]
        removed = before - len(calls["records"])
        if removed:
            _write_json(CALLS_FILE, calls)
        return removed


def get_all_audio_paths():
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        return {
            c["audio_path"]
            for c in calls["records"]
            if c["status"] in ("pending", "in_progress", "retry_pending")
        }
