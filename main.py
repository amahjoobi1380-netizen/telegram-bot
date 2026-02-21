import os
TOKEN = os.environ["BOT_TOKEN"]
import re
import asyncio
import logging
from datetime import datetime, timedelta

from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
)
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage

from db import (
    init_db,
    upsert_user,
    get_user,
    set_referrer_if_empty,
    add_referral,
    get_referral_stats,
    get_wallet_balance,
    add_wallet_balance,
    add_ref_profit,
    try_deduct_wallet,
    create_deposit_request,
    get_deposit_request,
    set_deposit_status,
    list_pending_deposits,
    create_order,
    get_order_with_user,
    list_orders,
    search_orders,
    get_user_orders,
    get_subscription,
    set_subscription,
    fetch_expiring_soon_not_reminded,
    mark_reminded_before_expiry,
    fetch_expired_not_notified,
    mark_notified_expired,
    admin_counts,
    add_links,
    count_links,
    list_available_links,
    delete_link,
    pop_available_link_for_order,
    list_pending_orders,
    set_order_delivered,
    # ✅ new for admin links
    list_all_links,
    update_link,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("BOT")


BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN در فایل .env تنظیم نشده!")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# ====== تنظیمات شما ======
CARD_NUMBER = "6037 6982 8557 7503"
CARD_OWNER = "سعید رنج بخش"

PLANS = {2: 150_000, 4: 265_000, 6: 350_000, 12: 600_000}
REF_PERCENT = 0.15  # 15%

# ====== منطقه زمانی ایران ======
IRAN_OFFSET = timedelta(hours=3, minutes=30)


def to_iran(dt_utc: datetime) -> datetime:
    return dt_utc + IRAN_OFFSET


def from_iran(dt_iran: datetime) -> datetime:
    return dt_iran - IRAN_OFFSET


def row_to_dict(x):
    return dict(x) if x is not None else None


# ========= ابزارها =========
PERSIAN_DIGITS = str.maketrans("۰۱۲۳۴۵۶۷۸۹", "0123456789")
ARABIC_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def format_toman(amount: int) -> str:
    return f"{amount:,}".replace(",", "٬") + " تومان"


def normalize_digits(s: str) -> str:
    return s.translate(PERSIAN_DIGITS).translate(ARABIC_DIGITS)


def parse_amount(text: str) -> int | None:
    t = normalize_digits(text).replace("٬", "").replace(",", " ").strip().lower()
    m = re.search(r"\d+", t)
    if not m:
        return None
    val = int(m.group())
    if "هزار" in t and val < 10000:
        val *= 1000
    if "میلیون" in t and val < 10000:
        val *= 1_000_000
    return val if val > 0 else None


async def safe_edit(callback: CallbackQuery, text: str, kb: InlineKeyboardMarkup | None):
    try:
        await callback.message.edit_text(text, reply_markup=kb)
    except Exception:
        await callback.message.answer(text, reply_markup=kb)


# ================== تاریخ شمسی (با نام ماه فارسی) ==================
PERSIAN_MONTHS = [
    "",
    "فروردین",
    "اردیبهشت",
    "خرداد",
    "تیر",
    "مرداد",
    "شهریور",
    "مهر",
    "آبان",
    "آذر",
    "دی",
    "بهمن",
    "اسفند",
]


def gregorian_to_jalali(gy: int, gm: int, gd: int):
    g_d_m = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334]
    if gy > 1600:
        jy = 979
        gy -= 1600
    else:
        jy = 0
        gy -= 621

    gy2 = gy + 1 if gm > 2 else gy
    days = (
        (365 * gy)
        + ((gy2 + 3) // 4)
        - ((gy2 + 99) // 100)
        + ((gy2 + 399) // 400)
        - 80
        + gd
        + g_d_m[gm - 1]
    )

    jy += 33 * (days // 12053)
    days %= 12053
    jy += 4 * (days // 1461)
    days %= 1461

    if days > 365:
        jy += (days - 1) // 365
        days = (days - 1) % 365

    if days < 186:
        jm = 1 + (days // 31)
        jd = 1 + (days % 31)
    else:
        jm = 7 + ((days - 186) // 30)
        jd = 1 + ((days - 186) % 30)

    return jy, jm, jd


def jalali_to_gregorian(jy: int, jm: int, jd: int):
    jy += 1595
    days = -355668 + (365 * jy) + ((jy // 33) * 8) + (((jy % 33) + 3) // 4) + jd

    if jm < 7:
        days += (jm - 1) * 31
    else:
        days += ((jm - 7) * 30) + 186

    gy = 400 * (days // 146097)
    days %= 146097

    if days > 36524:
        gy += 100 * ((days - 1) // 36524)
        days = (days - 1) % 36524
        if days >= 365:
            days += 1

    gy += 4 * (days // 1461)
    days %= 1461

    if days > 365:
        gy += (days - 1) // 365
        days = (days - 1) % 365

    gd = days + 1

    sal_a = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    leap = (gy % 4 == 0 and gy % 100 != 0) or (gy % 400 == 0)
    if leap:
        sal_a[2] = 29

    gm = 1
    while gm <= 12 and gd > sal_a[gm]:
        gd -= sal_a[gm]
        gm += 1

    return gy, gm, gd


def jalali_month_days(jy: int, jm: int) -> int:
    if jm <= 6:
        return 31
    if jm <= 11:
        return 30
    a = jy - 474
    b = a % 2820 + 474
    leap = (((b + 38) * 682) % 2816) < 682
    return 30 if leap else 29


def add_months_shamsi(dt: datetime, months: int) -> datetime:
    jy, jm, jd = gregorian_to_jalali(dt.year, dt.month, dt.day)

    total = (jm - 1) + months
    new_jy = jy + (total // 12)
    new_jm = (total % 12) + 1

    max_day = jalali_month_days(new_jy, new_jm)
    new_jd = min(jd, max_day)

    gy, gm, gd = jalali_to_gregorian(new_jy, new_jm, new_jd)
    return dt.replace(year=gy, month=gm, day=gd)


def to_jalali_pretty(dt_greg: datetime) -> str:
    jy, jm, jd = gregorian_to_jalali(dt_greg.year, dt_greg.month, dt_greg.day)
    month_name = PERSIAN_MONTHS[jm]
    return f"{jd} {month_name} {jy} - {dt_greg.hour:02d}:{dt_greg.minute:02d}"


def parse_sqlite_dt(s: str) -> datetime:
    s = (s or "").strip()
    if "T" not in s and " " in s:
        s = s.replace(" ", "T", 1)
    return datetime.fromisoformat(s)


# ============= Reply Keyboard (منوی دائمی پایین) =============
def reply_main_menu(user_id: int):
    rows = [
        [KeyboardButton(text="🛒 خرید اشتراک"), KeyboardButton(text="💰 کیف پول")],
        [KeyboardButton(text="👥 زیرمجموعه‌ها"), KeyboardButton(text="📦 وضعیت اشتراک")],
        [KeyboardButton(text="📜 تاریخچه خرید"), KeyboardButton(text="🧑‍💻 پشتیبانی")],
    ]
    if is_admin(user_id):
        rows.append([KeyboardButton(text="🛠 پنل ادمین")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)


def reply_back_to_main():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🔙 بازگشت به منوی اصلی")]],
        resize_keyboard=True,
    )


# ========= Inline keyboards =========
def back_to_main_inline():
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="🔙 بازگشت به منوی اصلی", callback_data="back_to_main")]]
    )


def plans_menu():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔥 دو ماهه نامحدود — ۱۵۰ هزار", callback_data="plan_2")],
            [InlineKeyboardButton(text="🔥 چهار ماهه نامحدود — ۲۶۵ هزار", callback_data="plan_4")],
            [InlineKeyboardButton(text="🔥 شش ماهه نامحدود — ۳۵۰ هزار", callback_data="plan_6")],
            [InlineKeyboardButton(text="🏆 دوازده ماهه — ۶۰۰ هزار 💎", callback_data="plan_12")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="back_to_main")],
        ]
    )


def wallet_menu_inline():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ افزایش اعتبار", callback_data="wallet_topup")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="back_to_main")],
        ]
    )


def not_enough_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ افزایش اعتبار کیف پول", callback_data="wallet_topup")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="back_to_main")],
        ]
    )


def confirm_purchase_kb(months: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ تایید خرید", callback_data=f"confirm_{months}"),
                InlineKeyboardButton(text="❌ انصراف", callback_data="back_to_main"),
            ]
        ]
    )


def deposit_review_kb(dep_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ تایید", callback_data=f"dep_appr_{dep_id}"),
                InlineKeyboardButton(text="❌ رد", callback_data=f"dep_rej_{dep_id}"),
            ]
        ]
    )


# ---- Admin panel keyboards ----
def admin_menu_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 داشبورد سریع", callback_data="admin_dash")],
            [InlineKeyboardButton(text="🧾 مدیریت سفارش‌ها", callback_data="admin_orders")],
            [InlineKeyboardButton(text="💳 شارژهای در انتظار تایید", callback_data="admin_deposits")],
            [InlineKeyboardButton(text="🔗 مدیریت لینک‌ها", callback_data="admin_links")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="back_to_main")],
        ]
    )


def admin_orders_root_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="📅 امروز", callback_data="admin_orders_tf_today"),
                InlineKeyboardButton(text="📆 هفته", callback_data="admin_orders_tf_week"),
                InlineKeyboardButton(text="🗓 ماه", callback_data="admin_orders_tf_month"),
            ],
            [InlineKeyboardButton(text="🔎 جستجو", callback_data="admin_orders_search")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_panel")],
        ]
    )


def admin_orders_filter_kb(tf: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⏳ در انتظار لینک", callback_data=f"admin_orders_list_{tf}_paid_waiting_link")],
            [InlineKeyboardButton(text="✅ تحویل شده", callback_data=f"admin_orders_list_{tf}_delivered")],
            [InlineKeyboardButton(text="❌ لغو شده", callback_data=f"admin_orders_list_{tf}_cancelled")],
            [InlineKeyboardButton(text="📃 همه", callback_data=f"admin_orders_list_{tf}_all")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_orders")],
        ]
    )


def admin_order_actions_kb(order_id: int):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⏳ تمدید +1 ماه", callback_data=f"admin_order_extend_{order_id}_1"),
                InlineKeyboardButton(text="⏳ تمدید +3 ماه", callback_data=f"admin_order_extend_{order_id}_3"),
            ],
            [InlineKeyboardButton(text="💬 پیام به کاربر", callback_data=f"admin_order_msg_{order_id}")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_orders")],
        ]
    )


def admin_links_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="➕ اضافه کردن لینک", callback_data="admin_links_add")],
            [InlineKeyboardButton(text="📃 لیست لینک‌های آماده", callback_data="admin_links_list")],
            [InlineKeyboardButton(text="🗂 مشاهده تمام لینک‌ها", callback_data="admin_links_all")],  # ✅ جدید
            [InlineKeyboardButton(text="🧠 ارسال لینک برای سفارش‌های معوق", callback_data="admin_links_fulfill")],
            [InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_panel")],
        ]
    )


def admin_links_list_kb(items):
    rows = []
    for it in items:
        it = row_to_dict(it)
        rows.append(
            [
                InlineKeyboardButton(text=f"✏️ ادیت لینک #{it['id']}", callback_data=f"admin_links_edit_{it['id']}"),
                InlineKeyboardButton(text=f"🗑 حذف لینک #{it['id']}", callback_data=f"admin_links_del_{it['id']}"),
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_links")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_links_all_list_kb(items):
    rows = []
    for it in items:
        it = row_to_dict(it)
        label = f"#{it['id']} {'✅مصرف‌شده' if int(it['is_used'])==1 else '🟢آماده'}"
        rows.append(
            [
                InlineKeyboardButton(text=f"✏️ ادیت {label}", callback_data=f"admin_links_edit_{it['id']}"),
                InlineKeyboardButton(text=f"🗑 حذف {label}", callback_data=f"admin_links_del_{it['id']}"),
            ]
        )
    rows.append([InlineKeyboardButton(text="🔙 بازگشت", callback_data="admin_links")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ========= State ها =========
class TopUpFlow(StatesGroup):
    waiting_amount = State()
    waiting_receipt = State()


class SupportFlow(StatesGroup):
    waiting_support = State()


class AdminOrderSearchFlow(StatesGroup):
    waiting_query = State()


class AdminOrderMessageFlow(StatesGroup):
    waiting_text = State()


class AdminLinksAddFlow(StatesGroup):
    waiting_links = State()


class AdminLinkEditFlow(StatesGroup):  # ✅ جدید
    waiting_new_value = State()


# ================= بازگشت متنی (برای پشتیبانی و ...) =================
@dp.message(F.text == "🔙 بازگشت به منوی اصلی")
async def rk_back_text(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("منو اصلی:", reply_markup=reply_main_menu(message.from_user.id))


# ================= START / Referral =================
@dp.message(CommandStart())
async def start(message: Message):
    await upsert_user(message.from_user.id, message.from_user.username, message.from_user.first_name)

    payload = None
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) == 2:
        payload = parts[1].strip()

    if payload and payload.isdigit():
        referrer_id = int(payload)
        user_id = message.from_user.id
        if referrer_id != user_id:
            changed = await set_referrer_if_empty(user_id, referrer_id)
            if changed:
                ok = await add_referral(referrer_id, user_id)
                if ok:
                    try:
                        await bot.send_message(
                            referrer_id,
                            f"🎉 یک کاربر با لینک شما داخل ربات ثبت‌نام کرد.\n"
                            f"user_id: {user_id}\n"
                            f"نام: {message.from_user.full_name} (@{message.from_user.username})",
                        )
                    except Exception:
                        pass

    await message.answer(
        "به ربات فروش اشتراک خوش آمدید 👋",
        reply_markup=reply_main_menu(message.from_user.id),
    )


# -------------- Reply menu handlers --------------
@dp.message(F.text == "🛒 خرید اشتراک")
async def rk_buy(message: Message):
    await message.answer("یک پلن را انتخاب کنید:", reply_markup=plans_menu())


@dp.message(F.text == "💰 کیف پول")
async def rk_wallet(message: Message):
    bal = await get_wallet_balance(message.from_user.id)
    await message.answer(
        f"💰 کیف پول شما\n\nاعتبار فعلی: {format_toman(bal)}",
        reply_markup=wallet_menu_inline(),
    )


@dp.message(F.text == "👥 زیرمجموعه‌ها")
async def rk_ref(message: Message):
    me = await bot.get_me()
    link = f"https://t.me/{me.username}?start={message.from_user.id}"
    count, total_profit = await get_referral_stats(message.from_user.id)
    await message.answer(
        f"👥 زیرمجموعه‌ها\n\n"
        f"🔗 لینک شما:\n{link}\n\n"
        f"👤 تعداد: {count}\n"
        f"💸 سود کل: {format_toman(total_profit)}"
    )


@dp.message(F.text == "📦 وضعیت اشتراک")
async def rk_status(message: Message):
    orders = await get_user_orders(message.from_user.id, 50)
    if not orders:
        await message.answer(
            "📦 شما هنوز هیچ اشتراکی خریداری نکرده‌اید.",
            reply_markup=back_to_main_inline(),
        )
        return

    orders = [row_to_dict(o) for o in orders][::-1]  # قدیمی → جدید
    now_iran = to_iran(datetime.utcnow().replace(microsecond=0))

    lines = ["📦 لیست اشتراک‌های خریداری‌شده شما:\n"]
    for o in orders:
        created_utc = parse_sqlite_dt(o["created_at"])
        created_iran = to_iran(created_utc)

        months = int(o["plan_months"])
        expiry_iran = add_months_shamsi(created_iran, months)

        delivered_link = o.get("delivered_link")
        link_line = f"🔗 لینک اشتراک:\n{delivered_link}" if delivered_link else "🔗 لینک اشتراک: ⏳ در انتظار ارسال لینک"

        status_sub = "✅ فعال" if expiry_iran > now_iran else "⛔ منقضی"
        status_order = o.get("status", "-")

        lines.append(
            f"🧾 سفارش #{o['id']}\n"
            f"⏱ مدت: {months} ماه | ♾️ نامحدود\n"
            f"💰 مبلغ: {format_toman(int(o['amount']))}\n"
            f"📌 وضعیت سفارش: {status_order}\n"
            f"{link_line}\n"
            f"🗓 تاریخ خرید (ایران): {to_jalali_pretty(created_iran)}\n"
            f"⏳ تاریخ انقضا (ایران): {to_jalali_pretty(expiry_iran)}\n"
            f"📌 وضعیت اشتراک: {status_sub}\n"
            "────────────"
        )

    await message.answer("\n".join(lines), reply_markup=back_to_main_inline())


@dp.message(F.text == "📜 تاریخچه خرید")
async def rk_history(message: Message):
    orders = await get_user_orders(message.from_user.id, 20)
    if not orders:
        await message.answer("📜 هنوز خریدی ثبت نشده.", reply_markup=back_to_main_inline())
        return

    orders = [row_to_dict(o) for o in orders][::-1]  # قدیمی → جدید
    lines = ["📜 تاریخچه خرید شما (۲۰ خرید آخر):\n"]
    for o in orders:
        created_iran = to_iran(parse_sqlite_dt(o["created_at"]))
        lines.append(
            f"#{o['id']} | {o['plan_months']} ماه | {format_toman(int(o['amount']))} | {o['status']} | {to_jalali_pretty(created_iran)}"
        )
    await message.answer("\n".join(lines), reply_markup=back_to_main_inline())


@dp.message(F.text == "🧑‍💻 پشتیبانی")
async def rk_support(message: Message, state: FSMContext):
    await state.set_state(SupportFlow.waiting_support)
    await message.answer(
        "پیام پشتیبانی را ارسال کنید (متن/عکس).\nبرای برگشت، دکمه زیر را بزنید.",
        reply_markup=reply_back_to_main(),
    )


@dp.message(F.text == "🛠 پنل ادمین")
async def rk_admin(message: Message):
    if not is_admin(message.from_user.id):
        return
    await message.answer("🛠 پنل ادمین:", reply_markup=admin_menu_kb())


# ---------------- Inline back ----------------
@dp.callback_query(F.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    try:
        await callback.message.delete()
    except Exception:
        pass
    await callback.message.answer("منو اصلی:", reply_markup=reply_main_menu(callback.from_user.id))


# ================= Wallet TopUp (inline flow) =================
@dp.callback_query(F.data == "wallet_topup")
async def wallet_topup_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.set_state(TopUpFlow.waiting_amount)
    await callback.message.answer(
        "➕ افزایش اعتبار\n\nمبلغ را ارسال کنید.\nمثال: 150000 یا «150 هزار»",
        reply_markup=ReplyKeyboardRemove(),
    )
    await callback.message.answer("برای بازگشت:", reply_markup=reply_back_to_main())


@dp.message(TopUpFlow.waiting_amount)
async def topup_amount_received(message: Message, state: FSMContext):
    amount = parse_amount(message.text or "")
    if not amount:
        await message.answer("مبلغ نامعتبر است. مثال: 150000 یا 150 هزار")
        return

    await state.update_data(amount=amount)
    await state.set_state(TopUpFlow.waiting_receipt)

    await message.answer(
        f"✅ مبلغ: {format_toman(amount)}\n\n"
        f"💳 شماره کارت:\n{CARD_NUMBER}\n"
        f"👤 به نام: {CARD_OWNER}\n\n"
        "بعد از واریز، رسید را ارسال کنید (عکس یا متن/کد پیگیری).",
        reply_markup=reply_back_to_main(),
    )


@dp.message(TopUpFlow.waiting_receipt)
async def topup_receipt_received(message: Message, state: FSMContext):
    data = await state.get_data()
    amount = int(data.get("amount", 0))

    receipt_text = None
    receipt_file_id = None

    if message.text:
        receipt_text = message.text.strip()
    elif message.photo:
        receipt_file_id = message.photo[-1].file_id
        receipt_text = (message.caption or "").strip() or None
    elif message.document:
        receipt_file_id = message.document.file_id
        receipt_text = (message.caption or "").strip() or None
    else:
        await message.answer("فقط متن یا عکس/فایل رسید بفرست.")
        return

    dep_id = await create_deposit_request(message.from_user.id, amount, receipt_text, receipt_file_id)
    await state.clear()

    await message.answer(
        f"✅ رسید ثبت شد.\nشماره درخواست: #{dep_id}\nمنتظر تایید ادمین باشید.",
        reply_markup=reply_main_menu(message.from_user.id),
    )

    header = (
        f"💳 درخواست افزایش اعتبار\n"
        f"شماره: #{dep_id}\n"
        f"user_id: {message.from_user.id}\n"
        f"کاربر: {message.from_user.full_name} (@{message.from_user.username})\n"
        f"مبلغ: {format_toman(amount)}"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, header, reply_markup=deposit_review_kb(dep_id))
            await message.copy_to(admin_id)
        except Exception as e:
            log.warning("admin notify failed: %s", e)


# ================= Deposit approve/reject =================
@dp.callback_query(F.data.startswith("dep_appr_"))
async def deposit_approve(callback: CallbackQuery):
    await callback.answer("در حال ثبت…")
    if not is_admin(callback.from_user.id):
        await callback.answer("اجازه ندارید.", show_alert=True)
        return

    dep_id = int(callback.data.split("_")[2])
    dep = row_to_dict(await get_deposit_request(dep_id))
    if not dep or dep["status"] != "pending_admin":
        await callback.answer("این درخواست قابل بررسی نیست.", show_alert=True)
        return

    user_id = int(dep["user_id"])
    amount = int(dep["amount"])

    await set_deposit_status(dep_id, "approved")
    new_balance = await add_wallet_balance(user_id, amount)

    u = row_to_dict(await get_user(user_id))
    referrer_id = int(u["referrer_id"]) if u and u["referrer_id"] is not None else None
    if referrer_id and referrer_id != user_id:
        profit = int(amount * REF_PERCENT)
        if profit > 0:
            ref_new_balance = await add_wallet_balance(referrer_id, profit)
            await add_ref_profit(referrer_id, profit)
            try:
                await bot.send_message(
                    referrer_id,
                    f"🎁 سود زیرمجموعه!\nسود شما (۱۵٪): {format_toman(profit)}\nموجودی شما: {format_toman(ref_new_balance)}",
                    reply_markup=reply_main_menu(referrer_id),
                )
            except Exception:
                pass

    try:
        await bot.send_message(
            user_id,
            f"✅ شارژ کیف پول تایید شد.\nافزایش: {format_toman(amount)}\nموجودی جدید: {format_toman(new_balance)}",
            reply_markup=reply_main_menu(user_id),
        )
    except Exception:
        pass

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer(f"✅ تایید شد. موجودی جدید کاربر: {format_toman(new_balance)}")


@dp.callback_query(F.data.startswith("dep_rej_"))
async def deposit_reject(callback: CallbackQuery):
    await callback.answer("در حال ثبت…")
    if not is_admin(callback.from_user.id):
        await callback.answer("اجازه ندارید.", show_alert=True)
        return

    dep_id = int(callback.data.split("_")[2])
    dep = row_to_dict(await get_deposit_request(dep_id))
    if not dep or dep["status"] != "pending_admin":
        await callback.answer("این درخواست قابل بررسی نیست.", show_alert=True)
        return

    await set_deposit_status(dep_id, "rejected")
    try:
        await bot.send_message(
            int(dep["user_id"]),
            f"❌ درخواست شارژ #{dep_id} رد شد.",
            reply_markup=reply_main_menu(int(dep["user_id"])),
        )
    except Exception:
        pass

    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await callback.message.answer("❌ رد شد.")


# ================= Buy with confirmation + auto link =================
@dp.callback_query(F.data.startswith("plan_"))
async def plan_selected(callback: CallbackQuery):
    await callback.answer()
    months = int(callback.data.split("_")[1])
    price = PLANS[months]
    user_id = callback.from_user.id

    sub = row_to_dict(await get_subscription(user_id))
    now_utc = datetime.utcnow().replace(microsecond=0)
    base_utc = now_utc
    if sub:
        cur_exp_utc = datetime.fromisoformat(sub["expires_at"])
        base_utc = cur_exp_utc if cur_exp_utc > now_utc else now_utc

    base_iran = to_iran(base_utc)
    new_exp_iran = add_months_shamsi(base_iran, months)

    text = (
        "🧾 تایید خرید اشتراک\n\n"
        f"⏱ مدت اشتراک: {months} ماهه\n"
        f"♾️ نوع سرویس: نامحدود ✅\n"
        f"💰 هزینه پرداختی: {format_toman(price)}\n"
        f"📅 تاریخ انقضای اشتراک (ایران): {to_jalali_pretty(new_exp_iran)}\n\n"
        "آیا خرید را تایید می‌کنید؟"
    )
    await safe_edit(callback, text, confirm_purchase_kb(months))


@dp.callback_query(F.data.startswith("confirm_"))
async def confirm_purchase(callback: CallbackQuery):
    await callback.answer()
    months = int(callback.data.split("_")[1])
    price = PLANS[months]
    user_id = callback.from_user.id

    bal = await get_wallet_balance(user_id)
    if bal < price:
        await safe_edit(
            callback,
            f"❌ موجودی کافی نیست.\n\nموجودی: {format_toman(bal)}\nهزینه پلن: {format_toman(price)}",
            not_enough_kb(),
        )
        return

    ok, new_balance = await try_deduct_wallet(user_id, price)
    if not ok:
        await safe_edit(callback, "❌ خطا در کسر موجودی. دوباره تلاش کنید.", back_to_main_inline())
        return

    order_id = await create_order(user_id, months, price)

    sub = row_to_dict(await get_subscription(user_id))
    now_utc = datetime.utcnow().replace(microsecond=0)
    base_utc = now_utc
    if sub:
        cur_exp_utc = datetime.fromisoformat(sub["expires_at"])
        base_utc = cur_exp_utc if cur_exp_utc > now_utc else now_utc

    base_iran = to_iran(base_utc)
    new_exp_iran = add_months_shamsi(base_iran, months)
    new_exp_utc = from_iran(new_exp_iran)

    await set_subscription(user_id, new_exp_utc.isoformat())

    link = await pop_available_link_for_order(order_id, user_id)
    if link:
        try:
            await bot.send_message(
                user_id,
                f"✅ خرید شما ثبت شد و لینک اشتراک آماده است.\n\n"
                f"🔗 لینک اشتراک:\n{link}\n\n"
                f"⏳ انقضا (ایران): {to_jalali_pretty(new_exp_iran)}",
                reply_markup=reply_main_menu(user_id),
            )
        except Exception:
            pass
        await set_order_delivered(order_id, link)

        await safe_edit(
            callback,
            f"✅ خرید انجام شد.\n"
            f"موجودی بعد از خرید: {format_toman(new_balance)}\n"
            f"📅 انقضا (ایران): {to_jalali_pretty(new_exp_iran)}\n\n"
            "🔗 لینک برای شما ارسال شد.",
            back_to_main_inline(),
        )
    else:
        await safe_edit(
            callback,
            f"✅ خرید ثبت شد.\n"
            f"موجودی بعد از خرید: {format_toman(new_balance)}\n"
            f"📅 انقضا (ایران): {to_jalali_pretty(new_exp_iran)}\n\n"
            "⏳ در صف ارسال لینک هستید. به‌محض اضافه شدن لینک، خودکار برای شما ارسال می‌شود.",
            back_to_main_inline(),
        )
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"⚠️ لینک‌ها تمام شد!\n"
                    f"سفارش جدید در انتظار لینک: #{order_id}\n"
                    f"user_id: {user_id}\n"
                    "از پنل ادمین > مدیریت لینک‌ها، لینک‌های جدید اضافه کن.",
                )
            except Exception:
                pass


# ================= Support =================
@dp.message(SupportFlow.waiting_support)
async def support_message(message: Message, state: FSMContext):
    await state.clear()
    header = (
        f"🧑‍💻 پشتیبانی\n"
        f"user_id: {message.from_user.id}\n"
        f"کاربر: {message.from_user.full_name} (@{message.from_user.username})"
    )
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, header)
            await message.copy_to(admin_id)
        except Exception:
            pass
    await message.answer("✅ پیام شما ارسال شد.", reply_markup=reply_main_menu(message.from_user.id))


# ================= Admin Panel =================
@dp.callback_query(F.data == "admin_panel")
async def admin_panel(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        await callback.answer("اجازه ندارید.", show_alert=True)
        return
    await safe_edit(callback, "🛠 پنل ادمین:", admin_menu_kb())


@dp.callback_query(F.data == "admin_dash")
async def admin_dash(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    c = await admin_counts()
    av, used = await count_links()
    text = (
        "📊 داشبورد سریع\n\n"
        f"👤 کاربران کل: {c['users_total']} | امروز: {c['users_today']}\n\n"
        f"👥 زیرمجموعه‌ها: {c['referrals_total']}\n"
        f"💸 مجموع پورسانت پرداخت‌شده: {format_toman(c['ref_profit_total'])}\n\n"
        f"🛒 خریدهای امروز: {c['orders_today_count']}\n"
        f"💰 درآمد امروز: {format_toman(c['orders_today_sum'])}\n\n"
        f"⏳ سفارش‌های در انتظار لینک: {c['pending_orders']}\n"
        f"💳 شارژهای در انتظار تایید: {c['pending_deposits']}\n\n"
        f"🔗 لینک‌های آماده: {av} | مصرف‌شده: {used}"
    )
    await safe_edit(callback, text, admin_menu_kb())


@dp.callback_query(F.data == "admin_deposits")
async def admin_deposits(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    deps = await list_pending_deposits(10)
    if not deps:
        await safe_edit(callback, "✅ هیچ شارژ در انتظار تاییدی وجود ندارد.", admin_menu_kb())
        return

    deps = [row_to_dict(d) for d in deps][::-1]
    lines = ["💳 شارژهای در انتظار تایید:\n"]
    for d in deps:
        uname = ("@" + d["username"]) if d.get("username") else "بدون یوزرنیم"
        lines.append(f"#{d['id']} | user_id:{d['user_id']} | {uname} | مبلغ:{format_toman(int(d['amount']))}")
    lines.append("\nروی پیام رسیدی که قبلاً آمده، تایید/رد کن.")
    await safe_edit(callback, "\n".join(lines), admin_menu_kb())


@dp.callback_query(F.data == "admin_orders")
async def admin_orders(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    await safe_edit(callback, "🧾 مدیریت سفارش‌ها:\n\nابتدا بازه را انتخاب کن یا جستجو بزن.", admin_orders_root_kb())


@dp.callback_query(F.data.startswith("admin_orders_tf_"))
async def admin_orders_tf(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    tf = callback.data.split("_")[-1]
    await safe_edit(callback, f"فیلتر وضعیت برای بازه «{tf}»:", admin_orders_filter_kb(tf))


@dp.callback_query(F.data.startswith("admin_orders_list_"))
async def admin_orders_list(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    _, _, _, tf, status = callback.data.split("_", 4)
    status = status if status != "all" else None
    rows = await list_orders(tf, status, limit=10)

    if not rows:
        await safe_edit(callback, "موردی پیدا نشد.", admin_orders_root_kb())
        return

    rows = [row_to_dict(o) for o in rows][::-1]
    out = [f"🧾 سفارش‌ها (حداکثر ۱۰) | بازه: {tf} | وضعیت: {status or 'همه'}\n"]
    for o in rows:
        uname = ("@" + o["username"]) if o.get("username") else "بدون یوزرنیم"
        created_iran = to_iran(parse_sqlite_dt(o["created_at"]))
        out.append(
            f"#{o['id']} | user_id:{o['user_id']} | {uname} | {o['plan_months']} ماه | "
            f"{format_toman(int(o['amount']))} | {o['status']} | {to_jalali_pretty(created_iran)}"
        )
    out.append("\nبرای دیدن جزئیات: شماره سفارش را در «جستجو» وارد کن.")
    await safe_edit(callback, "\n".join(out), admin_orders_root_kb())


@dp.callback_query(F.data == "admin_orders_search")
async def admin_orders_search(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminOrderSearchFlow.waiting_query)
    await callback.message.answer("🔎 جستجو\n\nuser_id یا @username یا شماره سفارش را بفرست:")


@dp.message(AdminOrderSearchFlow.waiting_query)
async def admin_orders_search_do(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    q = (message.text or "").strip()
    await state.clear()

    rows = await search_orders(q, limit=10)
    if not rows:
        await message.answer("موردی پیدا نشد.", reply_markup=reply_main_menu(message.from_user.id))
        return

    rows = [row_to_dict(o) for o in rows][::-1]

    if len(rows) == 1:
        await _send_order_details(message, int(rows[0]["id"]))
        return

    out = ["نتایج جستجو (حداکثر ۱۰):\n"]
    for o in rows:
        uname = ("@" + o["username"]) if o.get("username") else "بدون یوزرنیم"
        created_iran = to_iran(parse_sqlite_dt(o["created_at"]))
        out.append(
            f"#{o['id']} | user_id:{o['user_id']} | {uname} | {o['plan_months']} ماه | "
            f"{format_toman(int(o['amount']))} | {o['status']} | {to_jalali_pretty(created_iran)}"
        )
    out.append("\nبرای دیدن جزئیات، شماره سفارش را دقیقاً بفرست.")
    await message.answer("\n".join(out), reply_markup=reply_main_menu(message.from_user.id))


async def _send_order_details(message: Message, order_id: int):
    o = row_to_dict(await get_order_with_user(order_id))
    if not o:
        await message.answer("سفارش پیدا نشد.")
        return
    uname = ("@" + o["username"]) if o.get("username") else "بدون یوزرنیم"
    created_iran = to_iran(parse_sqlite_dt(o["created_at"]))
    text = (
        f"📄 جزئیات سفارش #{o['id']}\n\n"
        f"user_id: {o['user_id']} | {uname}\n"
        f"پلن: {o['plan_months']} ماه | ♾️ نامحدود\n"
        f"مبلغ: {format_toman(int(o['amount']))}\n"
        f"وضعیت: {o['status']}\n"
        f"ثبت (ایران): {to_jalali_pretty(created_iran)}\n"
        f"لینک تحویلی: {o.get('delivered_link') or '—'}"
    )
    await message.answer(text, reply_markup=admin_order_actions_kb(int(o["id"])))


@dp.callback_query(F.data.startswith("admin_order_extend_"))
async def admin_order_extend(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split("_")
    order_id = int(parts[3])
    addm = int(parts[4])

    o = row_to_dict(await get_order_with_user(order_id))
    if not o:
        await callback.message.answer("سفارش پیدا نشد.")
        return

    user_id = int(o["user_id"])
    sub = row_to_dict(await get_subscription(user_id))

    now_utc = datetime.utcnow().replace(microsecond=0)
    base_utc = now_utc
    if sub:
        cur_exp_utc = datetime.fromisoformat(sub["expires_at"])
        base_utc = cur_exp_utc if cur_exp_utc > now_utc else now_utc

    base_iran = to_iran(base_utc)
    new_exp_iran = add_months_shamsi(base_iran, addm)
    new_exp_utc = from_iran(new_exp_iran)

    await set_subscription(user_id, new_exp_utc.isoformat())

    try:
        await bot.send_message(
            user_id,
            f"✅ اشتراک شما توسط ادمین تمدید شد.\n"
            f"تمدید: +{addm} ماه\n"
            f"تاریخ پایان جدید (ایران): {to_jalali_pretty(new_exp_iran)}",
            reply_markup=reply_main_menu(user_id),
        )
    except Exception:
        pass

    await callback.message.answer(
        f"✅ انجام شد.\nuser_id: {user_id}\nپایان جدید (ایران): {to_jalali_pretty(new_exp_iran)}"
    )


@dp.callback_query(F.data.startswith("admin_order_msg_"))
async def admin_order_msg_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    order_id = int(callback.data.split("_")[3])
    await state.set_state(AdminOrderMessageFlow.waiting_text)
    await state.update_data(order_id=order_id)
    await callback.message.answer("💬 متن پیام را ارسال کن تا برای کاربر ارسال شود:")


@dp.message(AdminOrderMessageFlow.waiting_text)
async def admin_order_msg_send(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    data = await state.get_data()
    await state.clear()
    order_id = int(data.get("order_id", 0))

    o = row_to_dict(await get_order_with_user(order_id))
    if not o:
        await message.answer("سفارش پیدا نشد.")
        return

    user_id = int(o["user_id"])
    txt = (message.text or "").strip()
    if not txt:
        await message.answer("پیام خالی است.")
        return

    try:
        await bot.send_message(user_id, f"📩 پیام ادمین:\n{txt}", reply_markup=reply_main_menu(user_id))
        await message.answer("✅ ارسال شد.", reply_markup=reply_main_menu(message.from_user.id))
    except Exception:
        await message.answer("❌ ارسال ناموفق بود.", reply_markup=reply_main_menu(message.from_user.id))


# ---- Admin links ----
@dp.callback_query(F.data == "admin_links")
async def admin_links(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    av, used = await count_links()
    pend = [row_to_dict(x) for x in (await list_pending_orders(50))]
    txt = (
        "🔗 مدیریت لینک‌ها\n\n"
        f"لینک‌های آماده: {av}\n"
        f"لینک‌های مصرف‌شده: {used}\n"
        f"سفارش‌های معوق (در انتظار لینک): {len(pend)}\n\n"
        "اگر لینک اضافه کنی، می‌توانی با گزینه «ارسال لینک برای سفارش‌های معوق» لینک‌ها را خودکار ارسال کنی."
    )
    await safe_edit(callback, txt, admin_links_kb())


@dp.callback_query(F.data == "admin_links_add")
async def admin_links_add_cb(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminLinksAddFlow.waiting_links)
    await callback.message.answer(
        "➕ لینک‌ها را ارسال کن.\n"
        "می‌تونی چند لینک را پشت‌سرهم بفرستی (هر لینک در یک خط).\n"
        "وقتی تموم شد، بنویس: done"
    )


@dp.message(AdminLinksAddFlow.waiting_links)
async def admin_links_add_receive(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    txt = (message.text or "").strip()
    if txt.lower() == "done":
        await state.clear()
        await message.answer("✅ پایان افزودن لینک‌ها.", reply_markup=reply_main_menu(message.from_user.id))
        return

    lines = [l.strip() for l in txt.splitlines() if l.strip()]
    inserted = await add_links(lines)
    av, used = await count_links()
    await message.answer(f"✅ {inserted} لینک اضافه شد.\nلینک آماده: {av} | مصرف‌شده: {used}\n(برای پایان: done)")


@dp.callback_query(F.data == "admin_links_list")
async def admin_links_list(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    items = await list_available_links(20)
    if not items:
        await safe_edit(callback, "هیچ لینک آماده‌ای وجود ندارد.", admin_links_kb())
        return
    items = [row_to_dict(x) for x in items]
    lines = ["📃 لینک‌های آماده (۲۰ تای اول):\n"]
    for it in items:
        lines.append(f"#{it['id']} | {it['link']}")
    await safe_edit(callback, "\n".join(lines), admin_links_list_kb(items))


@dp.callback_query(F.data == "admin_links_all")
async def admin_links_all(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return

    items = await list_all_links(200)
    if not items:
        await safe_edit(callback, "هیچ لینکی ثبت نشده است.", admin_links_kb())
        return

    items = [row_to_dict(x) for x in items][::-1]  # قدیمی → جدید
    lines = ["🗂 همه لینک‌ها (۲۰۰ تای آخر):\n"]
    for it in items:
        st = "✅ مصرف‌شده" if int(it["is_used"]) == 1 else "🟢 آماده"
        lines.append(f"#{it['id']} | {st}\n{it['link']}\n────────────")

    await safe_edit(callback, "\n".join(lines), admin_links_all_list_kb(items))


@dp.callback_query(F.data.startswith("admin_links_del_"))
async def admin_links_del(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    link_id = int(callback.data.split("_")[3])
    ok = await delete_link(link_id)
    if ok:
        await callback.message.answer(f"✅ لینک #{link_id} حذف شد.")
    else:
        await callback.message.answer("❌ حذف نشد (ممکن است قبلاً مصرف شده باشد).")


@dp.callback_query(F.data.startswith("admin_links_edit_"))
async def admin_links_edit_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return
    link_id = int(callback.data.split("_")[3])
    await state.set_state(AdminLinkEditFlow.waiting_new_value)
    await state.update_data(link_id=link_id)
    await callback.message.answer(
        f"✏️ ادیت لینک #{link_id}\n\n"
        "لینک جدید را ارسال کنید.\n"
        "⚠️ لینک‌های مصرف‌شده قابل ادیت نیستند.",
        reply_markup=ReplyKeyboardRemove(),
    )


@dp.message(AdminLinkEditFlow.waiting_new_value)
async def admin_links_edit_save(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return

    data = await state.get_data()
    link_id = int(data.get("link_id", 0))
    new_link = (message.text or "").strip()
    await state.clear()

    ok = await update_link(link_id, new_link)
    if ok:
        await message.answer(f"✅ لینک #{link_id} با موفقیت تغییر کرد.", reply_markup=reply_main_menu(message.from_user.id))
    else:
        await message.answer(
            "❌ تغییر لینک انجام نشد.\n"
            "دلایل رایج:\n"
            "1) لینک مصرف‌شده است و قابل ادیت نیست\n"
            "2) لینک تکراری است\n"
            "3) متن لینک خالی/نامعتبر است",
            reply_markup=reply_main_menu(message.from_user.id),
        )


@dp.callback_query(F.data == "admin_links_fulfill")
async def admin_links_fulfill(callback: CallbackQuery):
    await callback.answer()
    if not is_admin(callback.from_user.id):
        return

    pending = [row_to_dict(x) for x in (await list_pending_orders(50))]
    if not pending:
        await callback.message.answer("✅ سفارش معوقی وجود ندارد.")
        return

    sent = 0
    for o in pending:
        order_id = int(o["id"])
        user_id = int(o["user_id"])
        link = await pop_available_link_for_order(order_id, user_id)
        if not link:
            break
        try:
            await bot.send_message(user_id, f"🔗 لینک اشتراک شما:\n{link}", reply_markup=reply_main_menu(user_id))
        except Exception:
            pass
        await set_order_delivered(order_id, link)
        sent += 1

    av, _ = await count_links()
    if sent == 0 and av == 0:
        await callback.message.answer("⚠️ هیچ لینکی موجود نیست. اول لینک اضافه کن.")
    else:
        await callback.message.answer(f"✅ برای {sent} سفارش معوق لینک ارسال شد.\nلینک‌های باقی‌مانده: {av}")


# ================= Expiry watcher =================
async def subscription_watcher():
    while True:
        try:
            now_utc = datetime.utcnow().replace(microsecond=0)
            now_iso = now_utc.isoformat()
            soon_iso = (now_utc + timedelta(days=1)).isoformat()

            expiring = [row_to_dict(x) for x in (await fetch_expiring_soon_not_reminded(soon_iso, now_iso))]
            for r in expiring:
                uid = int(r["user_id"])
                exp_iran = to_iran(datetime.fromisoformat(r["expires_at"]))
                try:
                    await bot.send_message(
                        uid,
                        "⏰ یادآوری:\nاشتراک شما کمتر از ۲۴ ساعت دیگر تمام می‌شود.\n"
                        f"تاریخ پایان (ایران): {to_jalali_pretty(exp_iran)}\n"
                        "برای تمدید از «خرید اشتراک» استفاده کنید.",
                        reply_markup=reply_main_menu(uid),
                    )
                except Exception:
                    pass
                await mark_reminded_before_expiry(uid)

            expired = [row_to_dict(x) for x in (await fetch_expired_not_notified(now_iso))]
            for r in expired:
                uid = int(r["user_id"])
                exp_iran = to_iran(datetime.fromisoformat(r["expires_at"]))
                for admin_id in ADMIN_IDS:
                    try:
                        await bot.send_message(
                            admin_id,
                            f"⛔ اشتراک کاربر تمام شد!\nuser_id: {uid}\nپایان (ایران): {to_jalali_pretty(exp_iran)}",
                        )
                    except Exception:
                        pass
                await mark_notified_expired(uid)

        except Exception as e:
            log.warning("watcher error: %s", e)

        await asyncio.sleep(1800)


async def main():
    await init_db()
    log.info("🤖 Bot is running...")
    asyncio.create_task(subscription_watcher())
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())