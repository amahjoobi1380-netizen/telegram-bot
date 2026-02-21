import aiosqlite
from datetime import datetime, timedelta

DB_PATH = "bot.db"


def _utc_now_str() -> str:
    # sqlite friendly
    return datetime.utcnow().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("PRAGMA foreign_keys = ON;")

        # users
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                referrer_id INTEGER,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )

        # referrals (each referred once)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                referrer_id INTEGER NOT NULL,
                referred_id INTEGER NOT NULL UNIQUE,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )

        # wallet
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS wallets (
                user_id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # referral profit tracker (total paid commission)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_profits (
                referrer_id INTEGER PRIMARY KEY,
                total_profit INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # deposits
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS deposit_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending_admin',
                receipt_text TEXT,
                receipt_file_id TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposit_requests(status);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_deposits_user ON deposit_requests(user_id);")

        # subscriptions (single active expiry per user; orders history is separate)
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id INTEGER PRIMARY KEY,
                expires_at TEXT NOT NULL,
                reminded_before_expiry INTEGER NOT NULL DEFAULT 0,
                notified_expired INTEGER NOT NULL DEFAULT 0
            );
            """
        )

        # orders
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                plan_months INTEGER NOT NULL,
                amount INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'paid_waiting_link',
                delivered_link TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);")

        # links pool
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                link TEXT NOT NULL UNIQUE,
                is_used INTEGER NOT NULL DEFAULT 0,
                used_by_order_id INTEGER,
                used_by_user_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                used_at TEXT
            );
            """
        )
        await db.execute("CREATE INDEX IF NOT EXISTS idx_links_used ON links(is_used);")

        await db.commit()


# ---------------- Users / Referral ----------------
async def upsert_user(user_id: int, username: str | None, first_name: str | None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO users(user_id, username, first_name)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name;
            """,
            (user_id, username, first_name),
        )
        # ensure wallet row exists
        await db.execute(
            "INSERT INTO wallets(user_id, balance) VALUES(?, 0) ON CONFLICT(user_id) DO NOTHING;",
            (user_id,),
        )
        # ensure referral_profits row exists if ever needed
        await db.execute(
            "INSERT INTO referral_profits(referrer_id, total_profit) VALUES(?, 0) ON CONFLICT(referrer_id) DO NOTHING;",
            (user_id,),
        )
        await db.commit()


async def get_user(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM users WHERE user_id=?;", (user_id,))
        return await cur.fetchone()


async def set_referrer_if_empty(user_id: int, referrer_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT referrer_id FROM users WHERE user_id=?;", (user_id,))
        row = await cur.fetchone()
        if not row:
            return False
        if row[0] is not None:
            return False
        await db.execute("UPDATE users SET referrer_id=? WHERE user_id=?;", (referrer_id, user_id))
        await db.commit()
        return True


async def add_referral(referrer_id: int, referred_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        try:
            await db.execute(
                "INSERT INTO referrals(referrer_id, referred_id) VALUES(?, ?);",
                (referrer_id, referred_id),
            )
            await db.commit()
            return True
        except Exception:
            return False


async def get_referral_stats(referrer_id: int) -> tuple[int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur1 = await db.execute("SELECT COUNT(*) FROM referrals WHERE referrer_id=?;", (referrer_id,))
        count = (await cur1.fetchone())[0]

        cur2 = await db.execute("SELECT total_profit FROM referral_profits WHERE referrer_id=?;", (referrer_id,))
        row = await cur2.fetchone()
        total_profit = int(row[0]) if row else 0
        return count, total_profit


async def add_ref_profit(referrer_id: int, amount: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO referral_profits(referrer_id, total_profit)
            VALUES(?, ?)
            ON CONFLICT(referrer_id) DO UPDATE SET
                total_profit = total_profit + excluded.total_profit;
            """,
            (referrer_id, amount),
        )
        await db.commit()


# ---------------- Wallet ----------------
async def get_wallet_balance(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT balance FROM wallets WHERE user_id=?;", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def add_wallet_balance(user_id: int, amount: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO wallets(user_id, balance) VALUES(?, 0) ON CONFLICT(user_id) DO NOTHING;",
            (user_id,),
        )
        await db.execute("UPDATE wallets SET balance = balance + ? WHERE user_id=?;", (amount, user_id))
        await db.commit()
        cur = await db.execute("SELECT balance FROM wallets WHERE user_id=?;", (user_id,))
        row = await cur.fetchone()
        return int(row[0]) if row else 0


async def try_deduct_wallet(user_id: int, amount: int) -> tuple[bool, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO wallets(user_id, balance) VALUES(?, 0) ON CONFLICT(user_id) DO NOTHING;",
            (user_id,),
        )
        cur = await db.execute("SELECT balance FROM wallets WHERE user_id=?;", (user_id,))
        bal = int((await cur.fetchone())[0])
        if bal < amount:
            return False, bal
        await db.execute("UPDATE wallets SET balance = balance - ? WHERE user_id=?;", (amount, user_id))
        await db.commit()
        cur2 = await db.execute("SELECT balance FROM wallets WHERE user_id=?;", (user_id,))
        new_bal = int((await cur2.fetchone())[0])
        return True, new_bal


# ---------------- Deposits ----------------
async def create_deposit_request(user_id: int, amount: int, receipt_text: str | None, receipt_file_id: str | None) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO deposit_requests(user_id, amount, status, receipt_text, receipt_file_id)
            VALUES(?, ?, 'pending_admin', ?, ?);
            """,
            (user_id, amount, receipt_text, receipt_file_id),
        )
        await db.commit()
        return cur.lastrowid


async def get_deposit_request(dep_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM deposit_requests WHERE id=?;", (dep_id,))
        return await cur.fetchone()


async def set_deposit_status(dep_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE deposit_requests SET status=? WHERE id=?;", (status, dep_id))
        await db.commit()


async def list_pending_deposits(limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT d.*, u.username
            FROM deposit_requests d
            LEFT JOIN users u ON u.user_id = d.user_id
            WHERE d.status='pending_admin'
            ORDER BY d.id DESC
            LIMIT ?;
            """,
            (limit,),
        )
        return await cur.fetchall()


# ---------------- Orders ----------------
async def create_order(user_id: int, plan_months: int, amount: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO orders(user_id, plan_months, amount, status)
            VALUES(?, ?, ?, 'paid_waiting_link');
            """,
            (user_id, plan_months, amount),
        )
        await db.commit()
        return cur.lastrowid


async def set_order_delivered(order_id: int, delivered_link: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE orders SET status='delivered', delivered_link=? WHERE id=?;",
            (delivered_link, order_id),
        )
        await db.commit()


async def get_order_with_user(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT o.*, u.username
            FROM orders o
            LEFT JOIN users u ON u.user_id = o.user_id
            WHERE o.id=?;
            """,
            (order_id,),
        )
        return await cur.fetchone()


async def get_user_orders(user_id: int, limit: int = 50):
    """
    IMPORTANT: this must include delivered_link + status + amount + created_at + plan_months
    so "وضعیت اشتراک" بتواند لینک هر خرید را هم نشان دهد.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
                id,
                user_id,
                plan_months,
                amount,
                status,
                delivered_link,
                created_at
            FROM orders
            WHERE user_id=?
            ORDER BY id DESC
            LIMIT ?;
            """,
            (user_id, limit),
        )
        return await cur.fetchall()


async def list_pending_orders(limit: int = 50):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT o.*
            FROM orders o
            WHERE o.status='paid_waiting_link'
            ORDER BY o.id ASC
            LIMIT ?;
            """,
            (limit,),
        )
        return await cur.fetchall()


def _timeframe_to_sql(tf: str) -> tuple[str, tuple]:
    # uses UTC-based sqlite now; good enough for filters
    if tf == "today":
        return "date(o.created_at) = date('now')", ()
    if tf == "week":
        return "o.created_at >= datetime('now','-7 days')", ()
    if tf == "month":
        return "o.created_at >= datetime('now','-30 days')", ()
    return "1=1", ()


async def list_orders(tf: str, status: str | None, limit: int = 10):
    where_tf, params_tf = _timeframe_to_sql(tf)
    where_status = "1=1"
    params_status: tuple = ()
    if status:
        where_status = "o.status=?"
        params_status = (status,)

    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            f"""
            SELECT o.*, u.username
            FROM orders o
            LEFT JOIN users u ON u.user_id=o.user_id
            WHERE {where_tf} AND {where_status}
            ORDER BY o.id DESC
            LIMIT ?;
            """,
            (*params_tf, *params_status, limit),
        )
        return await cur.fetchall()


async def search_orders(q: str, limit: int = 10):
    q = (q or "").strip()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row

        # order id
        if q.isdigit():
            cur = await db.execute(
                """
                SELECT o.*, u.username
                FROM orders o
                LEFT JOIN users u ON u.user_id=o.user_id
                WHERE o.id=? OR o.user_id=?
                ORDER BY o.id DESC
                LIMIT ?;
                """,
                (int(q), int(q), limit),
            )
            return await cur.fetchall()

        # @username
        if q.startswith("@"):
            q2 = q[1:]
            cur = await db.execute(
                """
                SELECT o.*, u.username
                FROM orders o
                LEFT JOIN users u ON u.user_id=o.user_id
                WHERE u.username LIKE ?
                ORDER BY o.id DESC
                LIMIT ?;
                """,
                (q2 + "%", limit),
            )
            return await cur.fetchall()

        # generic
        cur = await db.execute(
            """
            SELECT o.*, u.username
            FROM orders o
            LEFT JOIN users u ON u.user_id=o.user_id
            WHERE u.username LIKE ?
            ORDER BY o.id DESC
            LIMIT ?;
            """,
            (q + "%", limit),
        )
        return await cur.fetchall()


# ---------------- Subscription ----------------
async def get_subscription(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM subscriptions WHERE user_id=?;", (user_id,))
        return await cur.fetchone()


async def set_subscription(user_id: int, expires_at_iso: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO subscriptions(user_id, expires_at, reminded_before_expiry, notified_expired)
            VALUES(?, ?, 0, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at=excluded.expires_at,
                reminded_before_expiry=0,
                notified_expired=0;
            """,
            (user_id, expires_at_iso),
        )
        await db.commit()


async def fetch_expiring_soon_not_reminded(soon_iso: str, now_iso: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT user_id, expires_at
            FROM subscriptions
            WHERE reminded_before_expiry=0
              AND expires_at <= ?
              AND expires_at > ?;
            """,
            (soon_iso, now_iso),
        )
        return await cur.fetchall()


async def mark_reminded_before_expiry(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subscriptions SET reminded_before_expiry=1 WHERE user_id=?;", (user_id,))
        await db.commit()


async def fetch_expired_not_notified(now_iso: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT user_id, expires_at
            FROM subscriptions
            WHERE notified_expired=0
              AND expires_at <= ?;
            """,
            (now_iso,),
        )
        return await cur.fetchall()


async def mark_notified_expired(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE subscriptions SET notified_expired=1 WHERE user_id=?;", (user_id,))
        await db.commit()


# ---------------- Admin counts / dashboard ----------------
async def admin_counts() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        cur1 = await db.execute("SELECT COUNT(*) FROM users;")
        users_total = int((await cur1.fetchone())[0])

        cur2 = await db.execute("SELECT COUNT(*) FROM users WHERE date(created_at)=date('now');")
        users_today = int((await cur2.fetchone())[0])

        cur3 = await db.execute("SELECT COUNT(*) FROM referrals;")
        referrals_total = int((await cur3.fetchone())[0])

        cur4 = await db.execute("SELECT COALESCE(SUM(total_profit),0) FROM referral_profits;")
        ref_profit_total = int((await cur4.fetchone())[0])

        cur5 = await db.execute(
            "SELECT COUNT(*), COALESCE(SUM(amount),0) FROM orders WHERE date(created_at)=date('now');"
        )
        row5 = await cur5.fetchone()
        orders_today_count = int(row5[0])
        orders_today_sum = int(row5[1])

        cur6 = await db.execute("SELECT COUNT(*) FROM orders WHERE status='paid_waiting_link';")
        pending_orders = int((await cur6.fetchone())[0])

        cur7 = await db.execute("SELECT COUNT(*) FROM deposit_requests WHERE status='pending_admin';")
        pending_deposits = int((await cur7.fetchone())[0])

        return {
            "users_total": users_total,
            "users_today": users_today,
            "referrals_total": referrals_total,
            "ref_profit_total": ref_profit_total,
            "orders_today_count": orders_today_count,
            "orders_today_sum": orders_today_sum,
            "pending_orders": pending_orders,
            "pending_deposits": pending_deposits,
        }


# ---------------- Links pool ----------------
async def add_links(links: list[str]) -> int:
    if not links:
        return 0
    inserted = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for ln in links:
            ln = (ln or "").strip()
            if not ln:
                continue
            try:
                await db.execute("INSERT INTO links(link, is_used) VALUES(?, 0);", (ln,))
                inserted += 1
            except Exception:
                # duplicate link => ignore
                pass
        await db.commit()
    return inserted


async def count_links() -> tuple[int, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur1 = await db.execute("SELECT COUNT(*) FROM links WHERE is_used=0;")
        available = int((await cur1.fetchone())[0])
        cur2 = await db.execute("SELECT COUNT(*) FROM links WHERE is_used=1;")
        used = int((await cur2.fetchone())[0])
        return available, used


async def list_available_links(limit: int = 20):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT id, link FROM links WHERE is_used=0 ORDER BY id ASC LIMIT ?;",
            (limit,),
        )
        return await cur.fetchall()


async def delete_link(link_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT is_used FROM links WHERE id=?;", (link_id,))
        row = await cur.fetchone()
        if not row:
            return False
        if int(row[0]) == 1:
            return False
        await db.execute("DELETE FROM links WHERE id=?;", (link_id,))
        await db.commit()
        return True


async def pop_available_link_for_order(order_id: int, user_id: int) -> str | None:
    """
    Atomically pick one unused link, mark used, assign to order.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("BEGIN IMMEDIATE;")
        db.row_factory = aiosqlite.Row

        cur = await db.execute(
            "SELECT id, link FROM links WHERE is_used=0 ORDER BY id ASC LIMIT 1;"
        )
        row = await cur.fetchone()
        if not row:
            await db.execute("COMMIT;")
            return None

        link_id = int(row["id"])
        link = str(row["link"])

        await db.execute(
            """
            UPDATE links
            SET is_used=1,
                used_by_order_id=?,
                used_by_user_id=?,
                used_at=datetime('now')
            WHERE id=?;
            """,
            (order_id, user_id, link_id),
        )

        await db.execute(
            "UPDATE orders SET delivered_link=?, status='delivered' WHERE id=?;",
            (link, order_id),
        )

        await db.execute("COMMIT;")
        return link
async def list_all_links(limit: int = 200):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT id, link, is_used, used_by_order_id, used_by_user_id, created_at, used_at
            FROM links
            ORDER BY id DESC
            LIMIT ?;
            """,
            (limit,),
        )
        return await cur.fetchall()


async def update_link(link_id: int, new_link: str) -> bool:
    new_link = (new_link or "").strip()
    if not new_link:
        return False
    async with aiosqlite.connect(DB_PATH) as db:
        # اجازه ادیت فقط برای لینک‌های مصرف نشده
        cur = await db.execute("SELECT is_used FROM links WHERE id=?;", (link_id,))
        row = await cur.fetchone()
        if not row:
            return False
        if int(row[0]) == 1:
            return False

        try:
            await db.execute("UPDATE links SET link=? WHERE id=?;", (new_link, link_id))
            await db.commit()
            return True
        except Exception:
            # duplicate link etc.
            return False