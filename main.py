import subprocess
import asyncio
import calendar
import logging
import os
import re
import pytz


def _install_system_dependencies():
    packages = ["pjsua", "ffmpeg"]
    missing = []
    for pkg in packages:
        result = subprocess.run(["which", pkg], capture_output=True, text=True)
        if result.returncode != 0:
            missing.append(pkg)

    if not missing:
        return

    print(f"[setup] Installing missing system packages: {missing}")

    managers = [
        ["apt-get", "install", "-y", "-qq"],
        ["apt", "install", "-y", "-qq"],
        ["yum", "install", "-y"],
        ["dnf", "install", "-y"],
        ["apk", "add", "--no-cache"],
    ]

    for mgr in managers:
        cmd_check = subprocess.run(["which", mgr[0]], capture_output=True)
        if cmd_check.returncode != 0:
            continue

        try:
            if mgr[0] in ("apt-get", "apt"):
                subprocess.run([mgr[0], "update", "-qq"], capture_output=True)
            result = subprocess.run(mgr + missing, capture_output=True)
            if result.returncode == 0:
                print(f"[setup] Successfully installed {missing} via {mgr[0]}")
                return
        except Exception:
            continue

    print(f"[setup] WARNING: Could not auto-install {missing}.")
    print("[setup] Please install manually: pjsua ffmpeg")


_install_system_dependencies()

from datetime import datetime, timedelta, date

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from pydub import AudioSegment

import phonenumbers
import database
import scheduler as sched
import sip_call
from config import BOT_TOKEN, AUDIO_DIR, MAX_AUDIO_DURATION_SECONDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

os.makedirs(AUDIO_DIR, exist_ok=True)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


_picker_locks: dict[int, asyncio.Lock] = {}


def _picker_lock(user_id: int) -> asyncio.Lock:
    if user_id not in _picker_locks:
        _picker_locks[user_id] = asyncio.Lock()
    return _picker_locks[user_id]


_BD_TZ = pytz.timezone("Asia/Dhaka")
BD_TIME_FMT = "%Y-%m-%d %H:%M:%S"


def now_bd() -> datetime:
    return datetime.now(_BD_TZ).replace(tzinfo=None)


class AddSIP(StatesGroup):
    domain = State()
    username = State()
    password = State()


class DeleteSIP(StatesGroup):
    confirm_username = State()


class ScheduleCall(StatesGroup):
    phone = State()
    audio = State()
    date = State()
    time = State()


MAIN_MENU = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="📞 Add SIP Account"), KeyboardButton(text="📅 Schedule Call")],
        [KeyboardButton(text="📋 My Scheduled Calls"), KeyboardButton(text="🗑 Delete SIP Account")],
    ],
    resize_keyboard=True,
)

MENU_TEXTS = {"📞 Add SIP Account", "📅 Schedule Call", "📋 My Scheduled Calls", "🗑 Delete SIP Account"}


MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def date_picker_kb(day: int, month: int, year: int) -> InlineKeyboardMarkup:
    mn = MONTH_NAMES[month - 1]
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="◀️", callback_data="dp:d:-"),
            InlineKeyboardButton(text=f"  {day:02d}  ", callback_data="dp:noop"),
            InlineKeyboardButton(text="▶️", callback_data="dp:d:+"),
        ],
        [
            InlineKeyboardButton(text="◀️", callback_data="dp:m:-"),
            InlineKeyboardButton(text=f"  {mn}  ", callback_data="dp:noop"),
            InlineKeyboardButton(text="▶️", callback_data="dp:m:+"),
        ],
        [
            InlineKeyboardButton(text="◀️", callback_data="dp:y:-"),
            InlineKeyboardButton(text=f"  {year}  ", callback_data="dp:noop"),
            InlineKeyboardButton(text="▶️", callback_data="dp:y:+"),
        ],
        [InlineKeyboardButton(text="✅  Confirm Date  →", callback_data="dp:ok")],
        [InlineKeyboardButton(text="✏️  Type date manually", callback_data="dp:type")],
    ])


def time_picker_kb(hour: int, minute: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="◀️", callback_data="tp:h:-"),
            InlineKeyboardButton(text=f"  {hour:02d} hr  ", callback_data="tp:noop"),
            InlineKeyboardButton(text="▶️", callback_data="tp:h:+"),
        ],
        [
            InlineKeyboardButton(text="◀️", callback_data="tp:n:-"),
            InlineKeyboardButton(text=f"  {minute:02d} min  ", callback_data="tp:noop"),
            InlineKeyboardButton(text="▶️", callback_data="tp:n:+"),
        ],
        [InlineKeyboardButton(text="✅  Confirm & Schedule", callback_data="tp:ok")],
        [InlineKeyboardButton(text="✏️  Type time manually", callback_data="tp:type")],
    ])


def calls_keyboard(calls):
    buttons = []
    for call in calls:
        scheduled_str = call.get("scheduled_at", "")
        try:
            dt = datetime.strptime(scheduled_str, BD_TIME_FMT)
            label = f"📞 {call['phone_number']} — {dt.strftime('%d.%m.%Y %H:%M')} BD"
        except (ValueError, TypeError):
            label = f"📞 {call['phone_number']}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"viewcall:{call['id']}")])
    buttons.append([InlineKeyboardButton(text="❌ Close", callback_data="delcall:close")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def call_detail_keyboard(call_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Delete This Call", callback_data=f"delcall:{call_id}")],
        [InlineKeyboardButton(text="⬅️ Back to List", callback_data="delcall:back")],
    ])


def confirm_delete_keyboard(call_id):
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Yes, Delete", callback_data=f"confirmdelete:{call_id}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="delcall:back"),
        ],
    ])


def is_valid_phone(number: str) -> bool:
    try:
        parsed = phonenumbers.parse(number, None)
        return phonenumbers.is_valid_number(parsed)
    except Exception:
        return False


def is_valid_sip_domain(domain: str) -> bool:
    domain = domain.strip()
    ip_pattern = re.compile(r"^(\d{1,3}\.){3}\d{1,3}$")
    if ip_pattern.match(domain):
        parts = domain.split(".")
        return all(0 <= int(p) <= 255 for p in parts)
    domain_pattern = re.compile(
        r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$"
    )
    return bool(domain_pattern.match(domain))


def _safe_filename(name: str) -> str:
    name = os.path.basename(name)
    name = re.sub(r"[^\w.\-]", "_", name)
    return name[:100]


def _get_audio_duration(path: str) -> float:
    try:
        return len(AudioSegment.from_file(path)) / 1000.0
    except Exception:
        return 0.0


@dp.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        f"👋 Hello, <b>{message.from_user.first_name}</b>!\n\n"
        "This bot lets you schedule automated SIP voice calls.\n"
        "Use the menu below to get started:",
        reply_markup=MAIN_MENU,
        parse_mode="HTML",
    )


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    await message.answer(
        "❌ <b>Cancelled.</b> Returning to main menu.",
        reply_markup=MAIN_MENU,
        parse_mode="HTML",
    )


@dp.message(F.text == "📞 Add SIP Account")
async def add_sip_start(message: Message, state: FSMContext):
    await state.clear()
    await state.set_state(AddSIP.domain)
    await message.answer(
        "Enter your <b>SIP Domain</b>:\n"
        "<i>Examples: sip.example.com or 192.168.1.1</i>",
        parse_mode="HTML",
    )


@dp.message(AddSIP.domain, ~F.text.in_(MENU_TEXTS))
async def add_sip_domain(message: Message, state: FSMContext):
    domain = message.text.strip()
    if not is_valid_sip_domain(domain):
        await message.answer(
            "❌ Invalid SIP domain. Please enter a valid domain or IP address.\n"
            "<i>Examples: sip.example.com or 192.168.1.1</i>",
            parse_mode="HTML",
        )
        return
    await state.update_data(domain=domain)
    await state.set_state(AddSIP.username)
    await message.answer("Enter your <b>SIP Username</b>:", parse_mode="HTML")


@dp.message(AddSIP.username, ~F.text.in_(MENU_TEXTS))
async def add_sip_username(message: Message, state: FSMContext):
    await state.update_data(username=message.text.strip())
    await state.set_state(AddSIP.password)
    await message.answer("Enter your <b>SIP Password</b>:", parse_mode="HTML")


@dp.message(AddSIP.password, ~F.text.in_(MENU_TEXTS))
async def add_sip_password(message: Message, state: FSMContext):
    data = await state.get_data()
    password = message.text.strip()
    domain = data["domain"]
    username = data["username"]
    loop = asyncio.get_running_loop()

    status_msg = await message.answer(
        "🔍 <b>Step 1/4:</b> Resolving domain…",
        parse_mode="HTML",
    )

    async def edit(text: str):
        try:
            await status_msg.edit_text(text, parse_mode="HTML")
        except Exception:
            pass

    ip, dns_err = await loop.run_in_executor(None, sip_call.resolve_domain, domain)
    if not ip:
        reason = "Domain name not found." if dns_err != "timeout" else "DNS lookup timed out."
        await edit(
            f"❌ <b>SIP Test Failed — {reason}</b>\n\n"
            f"🌐 Domain: <code>{domain}</code>\n\n"
            "Please re-enter your <b>SIP Password</b> or go back to fix other details.\n"
            "Send /cancel to cancel."
        )
        return

    await edit(f"🔄 <b>Step 2/4:</b> Trying TCP port 5060…\n<i>({domain} → {ip})</i>")
    tcp_5060 = await loop.run_in_executor(None, sip_call.try_tcp, domain, 5060)
    if tcp_5060:
        final_msg = "✅ SIP server reachable via <b>TCP:5060</b>."
        await _save_sip_and_reply(status_msg, state, message, domain, username, password, final_msg)
        return

    await edit(f"🔄 <b>Step 3/4:</b> Trying TCP port 5061…\n<i>(5060 not available)</i>")
    tcp_5061 = await loop.run_in_executor(None, sip_call.try_tcp, domain, 5061)
    if tcp_5061:
        final_msg = "✅ SIP server reachable via <b>TCP:5061</b>."
        await _save_sip_and_reply(status_msg, state, message, domain, username, password, final_msg)
        return

    for port in (5060, 5061):
        await edit(
            f"🔄 <b>Step 4/4:</b> Trying UDP port {port}…\n"
            f"<i>(TCP not available, falling back to UDP)</i>"
        )
        ok, udp_msg = await loop.run_in_executor(
            None, sip_call.try_udp, domain, username, port
        )
        if ok is True:
            await _save_sip_and_reply(status_msg, state, message, domain, username, password, udp_msg)
            return
        if ok is False:
            await edit(
                f"❌ <b>SIP Test Failed</b>\n\n"
                f"{udp_msg}\n\n"
                "Please re-enter your <b>SIP Password</b> or go back to fix other details.\n"
                "Send /cancel to cancel."
            )
            return

    final_msg = (
        f"⚠️ <b>Domain verified</b> ({domain} → {ip})\n"
        "Full SIP handshake not available in this environment, "
        "but credentials will be tested on the first call."
    )
    await _save_sip_and_reply(status_msg, state, message, domain, username, password, final_msg)


async def _save_sip_and_reply(status_msg, state, message, domain, username, password, result_msg):
    try:
        database.save_sip_account(
            telegram_id=message.from_user.id,
            sip_domain=domain,
            sip_username=username,
            sip_password=password,
        )
        await state.clear()
        await status_msg.edit_text(
            f"{result_msg}\n\n"
            "✅ <b>SIP account saved successfully!</b>\n\n"
            f"🌐 Domain: <code>{domain}</code>\n"
            f"👤 Username: <code>{username}</code>",
            parse_mode="HTML",
        )
        await message.answer("What would you like to do next?", reply_markup=MAIN_MENU)
    except Exception as e:
        logger.error("save_sip_account error: %s", e)
        try:
            await status_msg.edit_text(
                "❌ An error occurred while saving. Please try again.",
                parse_mode="HTML",
            )
        except Exception:
            pass
        await message.answer("Use the menu below:", reply_markup=MAIN_MENU)
        await state.clear()


@dp.message(F.text == "🗑 Delete SIP Account")
async def delete_sip_start(message: Message, state: FSMContext):
    account = database.get_sip_account(message.from_user.id)
    if not account:
        await message.answer("⚠️ You don't have a saved SIP account.", reply_markup=MAIN_MENU)
        return

    await state.clear()
    await state.set_state(DeleteSIP.confirm_username)
    await state.update_data(expected_username=account["sip_username"])

    cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Cancel", callback_data="canceldeletesip")]
    ])

    await message.answer(
        f"🗑 <b>Delete SIP Account</b>\n\n"
        f"🌐 Domain: <code>{account['sip_domain']}</code>\n"
        f"👤 Username: <code>{account['sip_username']}</code>\n\n"
        f"To confirm deletion, please <b>send the username</b> below:\n\n"
        f"<code>{account['sip_username']}</code>",
        reply_markup=cancel_kb,
        parse_mode="HTML",
    )


@dp.message(DeleteSIP.confirm_username, ~F.text.in_(MENU_TEXTS))
async def confirm_delete_sip_username(message: Message, state: FSMContext):
    data = await state.get_data()
    expected = data.get("expected_username", "")
    entered = message.text.strip()

    if entered != expected:
        cancel_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="canceldeletesip")]
        ])
        await message.answer(
            f"❌ Wrong username. Please send exactly:\n\n"
            f"<code>{expected}</code>",
            reply_markup=cancel_kb,
            parse_mode="HTML",
        )
        return

    try:
        deleted = database.delete_sip_account(message.from_user.id)
        await state.clear()
        if deleted:
            await message.answer(
                "✅ <b>SIP account deleted successfully.</b>",
                reply_markup=MAIN_MENU,
                parse_mode="HTML",
            )
        else:
            await message.answer("⚠️ Account not found.", reply_markup=MAIN_MENU)
    except Exception as e:
        logger.error("delete_sip_account error: %s", e)
        await state.clear()
        await message.answer("❌ An error occurred. Please try again.", reply_markup=MAIN_MENU)


@dp.callback_query(F.data == "canceldeletesip")
async def cancel_delete_sip(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("✅ Cancelled. Your SIP account is kept.")
    await callback.answer()


@dp.message(F.text == "📅 Schedule Call")
async def schedule_call_start(message: Message, state: FSMContext):
    account = database.get_sip_account(message.from_user.id)
    if not account:
        await message.answer(
            "⚠️ You need to add a SIP account first.\n"
            "👉 Tap <b>📞 Add SIP Account</b>.",
            reply_markup=MAIN_MENU, parse_mode="HTML",
        )
        return
    await state.clear()
    await state.set_state(ScheduleCall.phone)
    await message.answer(
        "📞 Enter the phone number to call:\n"
        "<i>Example: +8801712345678</i>",
        parse_mode="HTML",
    )


@dp.message(ScheduleCall.phone, ~F.text.in_(MENU_TEXTS))
async def schedule_phone(message: Message, state: FSMContext):
    number = message.text.strip()
    if not is_valid_phone(number):
        await message.answer(
            "❌ Invalid phone number. Use international format:\n"
            "<i>Example: +8801712345678</i>",
            parse_mode="HTML",
        )
        return
    await state.update_data(phone=number)
    await state.set_state(ScheduleCall.audio)
    await message.answer(
        f"✅ Number accepted: <b>{number}</b>\n\n"
        f"🎵 Now send the audio file to play during the call.\n"
        f"<i>Supported: MP3, OGG, WAV — max {MAX_AUDIO_DURATION_SECONDS} seconds</i>",
        parse_mode="HTML",
    )


@dp.message(ScheduleCall.audio, F.content_type.in_({"audio", "voice", "document"}))
async def schedule_audio(message: Message, state: FSMContext):
    if message.audio:
        file_id = message.audio.file_id
        file_name = _safe_filename(message.audio.file_name or f"{file_id}.mp3")
    elif message.voice:
        file_id = message.voice.file_id
        file_name = f"{file_id}.ogg"
    elif message.document:
        file_id = message.document.file_id
        file_name = _safe_filename(message.document.file_name or f"{file_id}.bin")
    else:
        await message.answer("❌ Please send an audio file.")
        return

    processing_msg = await message.answer("⏳ <b>Receiving audio file, please wait...</b>", parse_mode="HTML")

    audio_path = os.path.join(AUDIO_DIR, f"{message.from_user.id}_{file_name}")

    try:
        file = await bot.get_file(file_id)
        await bot.download_file(file.file_path, audio_path)
    except Exception as e:
        logger.error("Audio download error: %s", e)
        await processing_msg.edit_text("❌ Failed to download the file. Please send it again.")
        return

    await processing_msg.edit_text("⏳ <b>Checking audio file...</b>", parse_mode="HTML")

    loop = asyncio.get_running_loop()
    duration = await loop.run_in_executor(None, _get_audio_duration, audio_path)

    if duration == 0.0:
        os.remove(audio_path)
        await processing_msg.edit_text("❌ Could not read the audio file. Please send MP3, OGG or WAV.")
        return

    if duration > MAX_AUDIO_DURATION_SECONDS:
        os.remove(audio_path)
        await processing_msg.edit_text(
            f"❌ Audio is too long ({duration:.0f}s). "
            f"Maximum allowed is {MAX_AUDIO_DURATION_SECONDS} seconds."
        )
        return

    await processing_msg.edit_text("⏳ <b>Converting audio to call format...</b>", parse_mode="HTML")
    wav_path = await loop.run_in_executor(None, sip_call.convert_to_wav, audio_path)

    if wav_path is None:
        os.remove(audio_path)
        await processing_msg.edit_text("❌ Failed to convert audio. Please send a different file.")
        return

    if wav_path != audio_path:
        try:
            os.remove(audio_path)
        except Exception:
            pass
        audio_path = wav_path

    init_dt = now_bd() + timedelta(hours=1)

    await state.update_data(
        audio_path=audio_path,
        pick_day=init_dt.day,
        pick_month=init_dt.month,
        pick_year=init_dt.year,
        pick_hour=init_dt.hour,
        pick_min=init_dt.minute,
    )
    await state.set_state(ScheduleCall.date)

    await processing_msg.edit_text(
        f"✅ <b>Audio ready!</b> Duration: {duration:.0f}s\n\n"
        f"📅 <b>Select the call date:</b>\n"
        f"<i>Use ◀️ ▶️ to change, then tap Confirm</i>",
        reply_markup=date_picker_kb(init_dt.day, init_dt.month, init_dt.year),
        parse_mode="HTML",
    )


@dp.message(ScheduleCall.audio, ~F.text.in_(MENU_TEXTS))
async def schedule_audio_wrong(message: Message):
    await message.answer("❌ Please send an audio file (MP3, OGG or WAV).")


@dp.callback_query(ScheduleCall.date, F.data.startswith("dp:"))
async def date_picker_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    parts = callback.data.split(":")
    action = parts[1]

    if action == "noop":
        return

    async with _picker_lock(callback.from_user.id):
        _now = now_bd()
        data = await state.get_data()
        day = data.get("pick_day", _now.day)
        month = data.get("pick_month", _now.month)
        year = data.get("pick_year", _now.year)

        if action in ("d", "m", "y"):
            delta = 1 if parts[2] == "+" else -1
            if action == "d":
                max_d = calendar.monthrange(year, month)[1]
                day = (day - 1 + delta) % max_d + 1
            elif action == "m":
                month = (month - 1 + delta) % 12 + 1
                day = min(day, calendar.monthrange(year, month)[1])
            elif action == "y":
                year = max(now_bd().year, year + delta)
                day = min(day, calendar.monthrange(year, month)[1])

            await state.update_data(pick_day=day, pick_month=month, pick_year=year)
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=date_picker_kb(day, month, year)
                )
            except Exception:
                pass
            return

        if action == "type":
            await state.update_data(date_manual_mode=True)
            await callback.message.answer(
                "✏️ <b>Type the date manually:</b>\n\n"
                "Accepted formats:\n"
                "• <code>DD/MM/YYYY</code>  e.g. <code>25/12/2026</code>\n"
                "• <code>DD-MM-YYYY</code>  e.g. <code>25-12-2026</code>\n"
                "• <code>YYYY-MM-DD</code>  e.g. <code>2026-12-25</code>\n\n"
                "Or tap /cancel to go back.",
                parse_mode="HTML",
            )
            return

        if action == "ok":
            day = min(day, calendar.monthrange(year, month)[1])
            date_str = f"{year:04d}-{month:02d}-{day:02d}"
            hour = data.get("pick_hour", (now_bd() + timedelta(hours=1)).hour)
            minute = data.get("pick_min", 0)

            await state.update_data(selected_date=date_str)
            await state.set_state(ScheduleCall.time)

            mn = MONTH_NAMES[month - 1]
            await callback.message.edit_text(
                f"📅 <b>Date:</b> {day:02d} {mn} {year}\n\n"
                f"🕐 <b>Select the call time:</b>\n"
                f"<i>Use ◀️ ▶️ to change, then tap Confirm</i>",
                reply_markup=time_picker_kb(hour, minute),
                parse_mode="HTML",
            )


def _parse_date_input(text: str):
    text = text.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(text, fmt)
            return dt.year, dt.month, dt.day
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {text!r}")


@dp.message(ScheduleCall.date, ~F.text.in_(MENU_TEXTS))
async def schedule_date_text(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("date_manual_mode"):
        await message.answer("👆 Use the ◀️ ▶️ buttons to select the date, or tap ✏️ Type date manually.")
        return

    try:
        year, month, day = _parse_date_input(message.text)
    except ValueError:
        await message.answer(
            "❌ <b>Invalid date format.</b>\n\n"
            "Please use one of:\n"
            "• <code>DD/MM/YYYY</code> — e.g. <code>25/12/2026</code>\n"
            "• <code>DD-MM-YYYY</code> — e.g. <code>25-12-2026</code>\n"
            "• <code>YYYY-MM-DD</code> — e.g. <code>2026-12-25</code>",
            parse_mode="HTML",
        )
        return

    if date(year, month, day) < now_bd().date():
        await message.answer(
            "❌ <b>That date is in the past.</b> Please enter a future date.",
            parse_mode="HTML",
        )
        return

    date_str = f"{year:04d}-{month:02d}-{day:02d}"
    hour = data.get("pick_hour", (now_bd() + timedelta(hours=1)).hour)
    minute = data.get("pick_min", 0)

    await state.update_data(selected_date=date_str, date_manual_mode=False,
                            pick_day=day, pick_month=month, pick_year=year)
    await state.set_state(ScheduleCall.time)

    mn = MONTH_NAMES[month - 1]
    await message.answer(
        f"📅 <b>Date:</b> {day:02d} {mn} {year}\n\n"
        f"🕐 <b>Select the call time:</b>\n"
        f"<i>Use ◀️ ▶️ to change, or tap ✏️ to type</i>",
        reply_markup=time_picker_kb(hour, minute),
        parse_mode="HTML",
    )


@dp.callback_query(ScheduleCall.time, F.data.startswith("tp:"))
async def time_picker_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()

    parts = callback.data.split(":")
    action = parts[1]

    if action == "noop":
        return

    async with _picker_lock(callback.from_user.id):
        data = await state.get_data()
        hour = data.get("pick_hour", 9)
        minute = data.get("pick_min", 0)

        if action in ("h", "n"):
            delta = 1 if parts[2] == "+" else -1
            if action == "h":
                hour = (hour + delta) % 24
            elif action == "n":
                minute = (minute + delta) % 60

            await state.update_data(pick_hour=hour, pick_min=minute)
            try:
                await callback.message.edit_reply_markup(
                    reply_markup=time_picker_kb(hour, minute)
                )
            except Exception:
                pass
            return

        if action == "type":
            await state.update_data(time_manual_mode=True)
            await callback.message.answer(
                "✏️ <b>Type the time manually (Bangladesh time):</b>\n\n"
                "Format: <code>HH:MM</code>  (24-hour)\n"
                "Examples: <code>09:30</code> · <code>14:05</code> · <code>23:00</code>\n\n"
                "Or tap /cancel to go back.",
                parse_mode="HTML",
            )
            return

        if action == "ok":
            time_str = f"{hour:02d}:{minute:02d}"
            await _finalize_schedule(callback.message, state, time_str, user_id=callback.from_user.id)


@dp.message(ScheduleCall.time, ~F.text.in_(MENU_TEXTS))
async def schedule_time_text(message: Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("time_manual_mode"):
        await message.answer("👆 Use the ◀️ ▶️ buttons to select the time, or tap ✏️ Type time manually.")
        return

    text = message.text.strip()
    if not re.match(r"^\d{1,2}:\d{2}$", text):
        await message.answer(
            "❌ <b>Invalid format.</b>\n"
            "Use <code>HH:MM</code> (24-hour), e.g. <code>14:30</code>",
            parse_mode="HTML",
        )
        return

    h, m = text.split(":")
    hour, minute = int(h), int(m)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        await message.answer(
            "❌ <b>Invalid time.</b>\n"
            "Hours must be 00–23, minutes 00–59.",
            parse_mode="HTML",
        )
        return

    await state.update_data(time_manual_mode=False, pick_hour=hour, pick_min=minute)
    time_str = f"{hour:02d}:{minute:02d}"
    await _finalize_schedule(message, state, time_str)


async def _finalize_schedule(message: Message, state: FSMContext, time_str: str, user_id: int = None):
    telegram_id = user_id if user_id is not None else message.from_user.id

    data = await state.get_data()

    missing = [k for k in ("phone", "audio_path", "selected_date") if k not in data]
    if missing:
        logger.warning("FSM data missing keys %s — session likely expired", missing)
        await state.clear()
        await message.answer(
            "⚠️ <b>Session expired.</b>\n\n"
            "Please start scheduling again from the beginning.\n"
            "Tap <b>📅 Schedule Call</b>.",
            parse_mode="HTML",
            reply_markup=MAIN_MENU,
        )
        return

    date_str = data["selected_date"]
    bd_datetime_str = f"{date_str} {time_str}:00"

    try:
        scheduled_bd = datetime.strptime(bd_datetime_str, BD_TIME_FMT)
    except ValueError:
        await message.answer("❌ Invalid date or time.")
        return

    if scheduled_bd < now_bd():
        await message.answer(
            "❌ <b>Cannot schedule a call in the past.</b>\n"
            f"<i>Current BD time: {now_bd().strftime('%d %b %Y %H:%M')}</i>",
            parse_mode="HTML",
        )
        return

    try:
        call_id = database.save_scheduled_call(
            telegram_id=telegram_id,
            phone_number=data["phone"],
            audio_path=data["audio_path"],
            scheduled_at_bd_str=bd_datetime_str,
        )
    except Exception as e:
        logger.error("save_scheduled_call error: %s", e)
        await state.clear()
        await message.answer(
            "❌ <b>Failed to save.</b> Please start again.\n"
            "Tap <b>📅 Schedule Call</b>.",
            parse_mode="HTML",
            reply_markup=MAIN_MENU,
        )
        return

    await state.clear()
    mn_name = MONTH_NAMES[scheduled_bd.month - 1]
    await message.answer(
        f"✅ <b>Call Scheduled!</b>\n\n"
        f"🆔 ID: <b>{call_id}</b>\n"
        f"📞 Number: <b>{data['phone']}</b>\n"
        f"📅 Date: <b>{scheduled_bd.day:02d} {mn_name} {scheduled_bd.year}</b>\n"
        f"🕐 Time: <b>{scheduled_bd.strftime('%H:%M')} (BD)</b>\n\n"
        f"⏳ You will be notified when the call is completed.",
        parse_mode="HTML",
        reply_markup=MAIN_MENU,
    )


@dp.message(F.text == "📋 My Scheduled Calls")
async def my_calls(message: Message, state: FSMContext):
    await state.clear()
    try:
        calls = database.get_scheduled_calls(message.from_user.id)
    except Exception as e:
        logger.error("get_scheduled_calls error: %s", e)
        await message.answer("❌ Failed to load calls.", reply_markup=MAIN_MENU)
        return

    if not calls:
        await message.answer(
            "📭 You have no scheduled calls.",
            reply_markup=MAIN_MENU,
        )
        return

    await message.answer(
        f"📋 <b>Your Scheduled Calls</b> ({len(calls)})\n\n"
        "Tap a call to view details or delete it:",
        reply_markup=calls_keyboard(calls),
        parse_mode="HTML",
    )


@dp.callback_query(F.data.startswith("viewcall:"))
async def view_call(callback: CallbackQuery):
    call_id = int(callback.data.split(":", 1)[1])
    try:
        calls = database.get_scheduled_calls(callback.from_user.id)
    except Exception as e:
        logger.error("get_scheduled_calls error: %s", e)
        await callback.answer("❌ Error loading call.", show_alert=True)
        return

    call = next((c for c in calls if c["id"] == call_id), None)
    if not call:
        await callback.answer("⚠️ Call not found.", show_alert=True)
        return

    scheduled_str = call.get("scheduled_at", "")
    try:
        dt = datetime.strptime(scheduled_str, BD_TIME_FMT)
        date_str = dt.strftime("%d.%m.%Y")
        time_str = dt.strftime("%H:%M") + " (BD)"
    except (ValueError, TypeError):
        date_str = scheduled_str
        time_str = ""

    await callback.message.edit_text(
        f"📞 <b>Call Details</b>\n\n"
        f"🆔 ID: <b>{call['id']}</b>\n"
        f"📱 Number: <b>{call['phone_number']}</b>\n"
        f"📅 Date: <b>{date_str}</b>\n"
        f"🕐 Time: <b>{time_str}</b>\n"
        f"📊 Status: <b>{call['status']}</b>",
        reply_markup=call_detail_keyboard(call_id),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("delcall:"))
async def handle_delcall(callback: CallbackQuery):
    value = callback.data.split(":", 1)[1]

    if value == "close":
        await callback.message.delete()
        await callback.answer()
        return

    if value == "back":
        try:
            calls = database.get_scheduled_calls(callback.from_user.id)
        except Exception:
            await callback.answer("❌ Error.", show_alert=True)
            return
        if calls:
            await callback.message.edit_text(
                f"📋 <b>Your Scheduled Calls</b> ({len(calls)})\n\n"
                "Tap a call to view details or delete it:",
                reply_markup=calls_keyboard(calls),
                parse_mode="HTML",
            )
        else:
            await callback.message.edit_text("📭 You have no scheduled calls.")
        await callback.answer()
        return

    call_id = int(value)
    await callback.message.edit_text(
        "⚠️ <b>Are you sure you want to delete this call?</b>\n"
        "This action cannot be undone.",
        reply_markup=confirm_delete_keyboard(call_id),
        parse_mode="HTML",
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("confirmdelete:"))
async def confirm_delete_call(callback: CallbackQuery):
    call_id = int(callback.data.split(":", 1)[1])
    try:
        deleted = database.delete_scheduled_call(call_id, callback.from_user.id)
    except Exception as e:
        logger.error("delete_scheduled_call error: %s", e)
        await callback.answer("❌ An error occurred.", show_alert=True)
        return

    if deleted:
        calls = database.get_scheduled_calls(callback.from_user.id)
        if calls:
            await callback.message.edit_text(
                f"✅ Call deleted.\n\n"
                f"📋 <b>Your Scheduled Calls</b> ({len(calls)})\n\n"
                "Tap a call to view details or delete it:",
                reply_markup=calls_keyboard(calls),
                parse_mode="HTML",
            )
        else:
            await callback.message.edit_text("✅ Call deleted.\n\n📭 You have no more scheduled calls.")
    else:
        await callback.answer("⚠️ Call not found.", show_alert=True)
    await callback.answer()


async def main():
    database.init_db()
    sched.start_scheduler(bot)
    logger.info("Bot starting...")
    try:
        await dp.start_polling(
            bot,
            allowed_updates=dp.resolve_used_update_types(),
            drop_pending_updates=True,
        )
    finally:
        sched.stop_scheduler()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
