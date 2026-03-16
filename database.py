import json
import os
import threading
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

DATA_DIR = "data"
SIP_FILE = os.path.join(DATA_DIR, "sip_accounts.json")
CALLS_FILE = os.path.join(DATA_DIR, "scheduled_calls.json")

_lock = threading.Lock()


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
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def init_db():
    _ensure_data_dir()
    with _lock:
        sip = _read_json(SIP_FILE, {})
        _write_json(SIP_FILE, sip)

        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        for call in calls.get("records", []):
            if call.get("status") == "in_progress":
                call["status"] = "pending"
        _write_json(CALLS_FILE, calls)


def save_sip_account(telegram_id, sip_domain, sip_username, sip_password):
    with _lock:
        sip = _read_json(SIP_FILE, {})
        sip[str(telegram_id)] = {
            "telegram_id": telegram_id,
            "sip_domain": sip_domain,
            "sip_username": sip_username,
            "sip_password": sip_password,
            "created_at": datetime.now().isoformat(),
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


def save_scheduled_call(telegram_id, phone_number, audio_path, scheduled_at):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        new_id = calls.get("next_id", 1)
        calls["next_id"] = new_id + 1
        calls["records"].append({
            "id": new_id,
            "telegram_id": telegram_id,
            "phone_number": phone_number,
            "audio_path": audio_path,
            "scheduled_at": str(scheduled_at),
            "status": "pending",
            "created_at": datetime.now().isoformat(),
        })
        _write_json(CALLS_FILE, calls)
        return new_id


def get_scheduled_calls(telegram_id):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        result = [
            c for c in calls["records"]
            if c["telegram_id"] == telegram_id and c["status"] == "pending"
        ]
        result.sort(key=lambda c: c["scheduled_at"])
        return result


def get_pending_calls():
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        sip = _read_json(SIP_FILE, {})
        now = datetime.now().isoformat()
        result = []
        for c in calls["records"]:
            if c["status"] == "pending" and c["scheduled_at"] <= now:
                account = sip.get(str(c["telegram_id"]))
                if account:
                    merged = dict(c)
                    merged["sip_domain"] = account["sip_domain"]
                    merged["sip_username"] = account["sip_username"]
                    merged["sip_password"] = account["sip_password"]
                    result.append(merged)
        return result


def update_call_status(call_id, status):
    with _lock:
        calls = _read_json(CALLS_FILE, {"next_id": 1, "records": []})
        for c in calls["records"]:
            if c["id"] == call_id:
                c["status"] = status
                break
        _write_json(CALLS_FILE, calls)


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
        cutoff = (datetime.now() - timedelta(days=keep_days)).isoformat()
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
            if c["status"] in ("pending", "in_progress")
        }
