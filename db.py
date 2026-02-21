import os
import asyncpg
from datetime import datetime

# ---------- Pool ----------
_pool: asyncpg.Pool | None = None


def _utc_now_str() -> str:
    return datetime.utcnow().replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


async def init_db():
    """
    Creates the asyncpg pool and ensures tables/indexes exist.
    Requires DATABASE_URL env var (Railway provides it when you add Postgres).
    """
    global _pool

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL is not set (Railway Postgres).")

    # Railway Postgres usually requires SSL
    # If you ever need to disable: set DB_SSL=disable
    ssl_mode = os.getenv("DB_SSL", "require").lower()
    ssl = "require" if ssl_mode != "disable" else None

    _pool = await asyncpg.create_pool(
        db_url,
        ssl=ssl,
        min_size=1,
        max_size=5,
    )

    async with _pool.acquire() as conn:
        # users
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                referrer_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # referrals
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referrals (
                id BIGSERIAL PRIMARY KEY,
                referrer_id BIGINT NOT NULL,
                referred_id BIGINT NOT NULL UNIQUE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # wallet
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS wallets (
                user_id BIGINT PRIMARY KEY,
                balance BIGINT NOT NULL DEFAULT 0
            );
            """
        )

        # referral profits
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS referral_profits (
                referrer_id BIGINT PRIMARY KEY,
                total_profit BIGINT NOT NULL DEFAULT 0
            );
            """
        )

        # deposits
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS deposit_requests (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                amount BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending_admin',
                receipt_text TEXT,
                receipt_file_id TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposit_requests(status);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_deposits_user ON deposit_requests(user_id);")

        # subscriptions
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriptions (
                user_id BIGINT PRIMARY KEY,
                expires_at TIMESTAMPTZ NOT NULL,
                reminded_before_expiry BOOLEAN NOT NULL DEFAULT FALSE,
                notified_expired BOOLEAN NOT NULL DEFAULT FALSE
            );
            """
        )

        # orders
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id BIGSERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                plan_months INTEGER NOT NULL,
                amount BIGINT NOT NULL,
                status TEXT NOT NULL DEFAULT 'paid_waiting_link',
                delivered_link TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_user ON orders(user_id);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_created ON orders(created_at);")

        # links
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS links (
                id BIGSERIAL PRIMARY KEY,
                link TEXT NOT NULL UNIQUE,
                is_used BOOLEAN NOT NULL DEFAULT FALSE,
                used_by_order_id BIGINT,
                used_by_user_id BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                used_at TIMESTAMPTZ
            );
            """
        )
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_links_used ON links(is_used);")


def _ensure_pool():
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call await init_db() first.")


# ---------------- Users / Referral ----------------
async def upsert_user(user_id: int, username: str | None, first_name: str | None):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users(user_id, username, first_name)
            VALUES($1, $2, $3)
            ON CONFLICT(user_id) DO UPDATE SET
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name;
            """,
            user_id,
            username,
            first_name,
        )

        await conn.execute(
            """
            INSERT INTO wallets(user_id, balance)
            VALUES($1, 0)
            ON CONFLICT(user_id) DO NOTHING;
            """,
            user_id,
        )

        await conn.execute(
            """
            INSERT INTO referral_profits(referrer_id, total_profit)
            VALUES($1, 0)
            ON CONFLICT(referrer_id) DO NOTHING;
            """,
            user_id,
        )


async def get_user(user_id: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1;", user_id)
        return dict(row) if row else None


async def set_referrer_if_empty(user_id: int, referrer_id: int) -> bool:
    _ensure_pool()
    async with _pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow("SELECT referrer_id FROM users WHERE user_id=$1 FOR UPDATE;", user_id)
            if not row:
                return False
            if row["referrer_id"] is not None:
                return False
            await conn.execute("UPDATE users SET referrer_id=$1 WHERE user_id=$2;", referrer_id, user_id)
            return True


async def add_referral(referrer_id: int, referred_id: int) -> bool:
    _ensure_pool()
    async with _pool.acquire() as conn:
        try:
            await conn.execute(
                "INSERT INTO referrals(referrer_id, referred_id) VALUES($1, $2);",
                referrer_id,
                referred_id,
            )
            return True
        except Exception:
            return False


async def get_referral_stats(referrer_id: int) -> tuple[int, int]:
    _ensure_pool()
    async with _pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id=$1;", referrer_id)
        total_profit = await conn.fetchval(
            "SELECT total_profit FROM referral_profits WHERE referrer_id=$1;",
            referrer_id,
        )
        return int(count or 0), int(total_profit or 0)


async def add_ref_profit(referrer_id: int, amount: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO referral_profits(referrer_id, total_profit)
            VALUES($1, $2)
            ON CONFLICT(referrer_id) DO UPDATE SET
                total_profit = referral_profits.total_profit + EXCLUDED.total_profit;
            """,
            referrer_id,
            amount,
        )


# ---------------- Wallet ----------------
async def get_wallet_balance(user_id: int) -> int:
    _ensure_pool()
    async with _pool.acquire() as conn:
        bal = await conn.fetchval("SELECT balance FROM wallets WHERE user_id=$1;", user_id)
        return int(bal or 0)


async def add_wallet_balance(user_id: int, amount: int) -> int:
    _ensure_pool()
    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO wallets(user_id, balance)
                VALUES($1, 0)
                ON CONFLICT(user_id) DO NOTHING;
                """,
                user_id,
            )
            await conn.execute("UPDATE wallets SET balance = balance + $1 WHERE user_id=$2;", amount, user_id)
            new_bal = await conn.fetchval("SELECT balance FROM wallets WHERE user_id=$1;", user_id)
            return int(new_bal or 0)


async def try_deduct_wallet(user_id: int, amount: int) -> tuple[bool, int]:
    _ensure_pool()
    async with _pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO wallets(user_id, balance)
                VALUES($1, 0)
                ON CONFLICT(user_id) DO NOTHING;
                """,
                user_id,
            )
            bal = await conn.fetchval("SELECT balance FROM wallets WHERE user_id=$1 FOR UPDATE;", user_id)
            bal = int(bal or 0)
            if bal < amount:
                return False, bal
            await conn.execute("UPDATE wallets SET balance = balance - $1 WHERE user_id=$2;", amount, user_id)
            new_bal = await conn.fetchval("SELECT balance FROM wallets WHERE user_id=$1;", user_id)
            return True, int(new_bal or 0)


# ---------------- Deposits ----------------
async def create_deposit_request(user_id: int, amount: int, receipt_text: str | None, receipt_file_id: str | None) -> int:
    _ensure_pool()
    async with _pool.acquire() as conn:
        dep_id = await conn.fetchval(
            """
            INSERT INTO deposit_requests(user_id, amount, status, receipt_text, receipt_file_id)
            VALUES($1, $2, 'pending_admin', $3, $4)
            RETURNING id;
            """,
            user_id,
            amount,
            receipt_text,
            receipt_file_id,
        )
        return int(dep_id)


async def get_deposit_request(dep_id: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM deposit_requests WHERE id=$1;", dep_id)
        return dict(row) if row else None


async def set_deposit_status(dep_id: int, status: str):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE deposit_requests SET status=$1 WHERE id=$2;", status, dep_id)


async def list_pending_deposits(limit: int = 10):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT d.*, u.username
            FROM deposit_requests d
            LEFT JOIN users u ON u.user_id = d.user_id
            WHERE d.status='pending_admin'
            ORDER BY d.id DESC
            LIMIT $1;
            """,
            limit,
        )
        return [dict(r) for r in rows]


# ---------------- Orders ----------------
async def create_order(user_id: int, plan_months: int, amount: int) -> int:
    _ensure_pool()
    async with _pool.acquire() as conn:
        oid = await conn.fetchval(
            """
            INSERT INTO orders(user_id, plan_months, amount, status)
            VALUES($1, $2, $3, 'paid_waiting_link')
            RETURNING id;
            """,
            user_id,
            plan_months,
            amount,
        )
        return int(oid)


async def set_order_delivered(order_id: int, delivered_link: str):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE orders SET status='delivered', delivered_link=$1 WHERE id=$2;",
            delivered_link,
            order_id,
        )


async def get_order_with_user(order_id: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT o.*, u.username
            FROM orders o
            LEFT JOIN users u ON u.user_id = o.user_id
            WHERE o.id=$1;
            """,
            order_id,
        )
        return dict(row) if row else None


async def get_user_orders(user_id: int, limit: int = 50):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
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
            WHERE user_id=$1
            ORDER BY id DESC
            LIMIT $2;
            """,
            user_id,
            limit,
        )
        return [dict(r) for r in rows]


async def list_pending_orders(limit: int = 50):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT o.*
            FROM orders o
            WHERE o.status='paid_waiting_link'
            ORDER BY o.id ASC
            LIMIT $1;
            """,
            limit,
        )
        return [dict(r) for r in rows]


def _timeframe_where(tf: str) -> str:
    if tf == "today":
        return "o.created_at::date = CURRENT_DATE"
    if tf == "week":
        return "o.created_at >= NOW() - INTERVAL '7 days'"
    if tf == "month":
        return "o.created_at >= NOW() - INTERVAL '30 days'"
    return "TRUE"


async def list_orders(tf: str, status: str | None, limit: int = 10):
    _ensure_pool()
    where_parts = [_timeframe_where(tf)]
    params = []
    idx = 1

    if status:
        where_parts.append(f"o.status = ${idx}")
        params.append(status)
        idx += 1

    where_parts.append(f"TRUE")
    where_sql = " AND ".join(where_parts)

    params.append(limit)
    limit_param = f"${idx}"

    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            f"""
            SELECT o.*, u.username
            FROM orders o
            LEFT JOIN users u ON u.user_id=o.user_id
            WHERE {where_sql}
            ORDER BY o.id DESC
            LIMIT {limit_param};
            """,
            *params,
        )
        return [dict(r) for r in rows]


async def search_orders(q: str, limit: int = 10):
    _ensure_pool()
    q = (q or "").strip()

    async with _pool.acquire() as conn:
        if q.isdigit():
            rows = await conn.fetch(
                """
                SELECT o.*, u.username
                FROM orders o
                LEFT JOIN users u ON u.user_id=o.user_id
                WHERE o.id=$1 OR o.user_id=$1
                ORDER BY o.id DESC
                LIMIT $2;
                """,
                int(q),
                limit,
            )
            return [dict(r) for r in rows]

        if q.startswith("@"):
            q2 = q[1:] + "%"
            rows = await conn.fetch(
                """
                SELECT o.*, u.username
                FROM orders o
                LEFT JOIN users u ON u.user_id=o.user_id
                WHERE u.username ILIKE $1
                ORDER BY o.id DESC
                LIMIT $2;
                """,
                q2,
                limit,
            )
            return [dict(r) for r in rows]

        rows = await conn.fetch(
            """
            SELECT o.*, u.username
            FROM orders o
            LEFT JOIN users u ON u.user_id=o.user_id
            WHERE u.username ILIKE $1
            ORDER BY o.id DESC
            LIMIT $2;
            """,
            q + "%",
            limit,
        )
        return [dict(r) for r in rows]


# ---------------- Subscription ----------------
async def get_subscription(user_id: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM subscriptions WHERE user_id=$1;", user_id)
        return dict(row) if row else None


async def set_subscription(user_id: int, expires_at_iso: str):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO subscriptions(user_id, expires_at, reminded_before_expiry, notified_expired)
            VALUES($1, $2::timestamptz, FALSE, FALSE)
            ON CONFLICT(user_id) DO UPDATE SET
                expires_at=EXCLUDED.expires_at,
                reminded_before_expiry=FALSE,
                notified_expired=FALSE;
            """,
            user_id,
            expires_at_iso,
        )


async def fetch_expiring_soon_not_reminded(soon_iso: str, now_iso: str):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, expires_at
            FROM subscriptions
            WHERE reminded_before_expiry=FALSE
              AND expires_at <= $1::timestamptz
              AND expires_at >  $2::timestamptz;
            """,
            soon_iso,
            now_iso,
        )
        return [dict(r) for r in rows]


async def mark_reminded_before_expiry(user_id: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE subscriptions SET reminded_before_expiry=TRUE WHERE user_id=$1;", user_id)


async def fetch_expired_not_notified(now_iso: str):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id, expires_at
            FROM subscriptions
            WHERE notified_expired=FALSE
              AND expires_at <= $1::timestamptz;
            """,
            now_iso,
        )
        return [dict(r) for r in rows]


async def mark_notified_expired(user_id: int):
    _ensure_pool()
    async with _pool.acquire() as conn:
        await conn.execute("UPDATE subscriptions SET notified_expired=TRUE WHERE user_id=$1;", user_id)


# ---------------- Admin counts / dashboard ----------------
async def admin_counts() -> dict:
    _ensure_pool()
    async with _pool.acquire() as conn:
        users_total = int(await conn.fetchval("SELECT COUNT(*) FROM users;") or 0)
        users_today = int(await conn.fetchval("SELECT COUNT(*) FROM users WHERE created_at::date = CURRENT_DATE;") or 0)
        referrals_total = int(await conn.fetchval("SELECT COUNT(*) FROM referrals;") or 0)
        ref_profit_total = int(await conn.fetchval("SELECT COALESCE(SUM(total_profit),0) FROM referral_profits;") or 0)

        row5 = await conn.fetchrow(
            """
            SELECT COUNT(*) AS c, COALESCE(SUM(amount),0) AS s
            FROM orders
            WHERE created_at::date = CURRENT_DATE;
            """
        )
        orders_today_count = int(row5["c"] or 0)
        orders_today_sum = int(row5["s"] or 0)

        pending_orders = int(await conn.fetchval("SELECT COUNT(*) FROM orders WHERE status='paid_waiting_link';") or 0)
        pending_deposits = int(await conn.fetchval("SELECT COUNT(*) FROM deposit_requests WHERE status='pending_admin';") or 0)

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
    _ensure_pool()
    if not links:
        return 0

    inserted = 0
    async with _pool.acquire() as conn:
        for ln in links:
            ln = (ln or "").strip()
            if not ln:
                continue
            try:
                await conn.execute("INSERT INTO links(link, is_used) VALUES($1, FALSE);", ln)
                inserted += 1
            except Exception:
                pass
    return inserted


async def count_links() -> tuple[int, int]:
    _ensure_pool()
    async with _pool.acquire() as conn:
        available = int(await conn.fetchval("SELECT COUNT(*) FROM links WHERE is_used=FALSE;") or 0)
        used = int(await conn.fetchval("SELECT COUNT(*) FROM links WHERE is_used=TRUE;") or 0)
        return available, used


async def list_available_links(limit: int = 20):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, link FROM links WHERE is_used=FALSE ORDER BY id ASC LIMIT $1;",
            limit,
        )
        return [dict(r) for r in rows]


async def delete_link(link_id: int) -> bool:
    _ensure_pool()
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_used FROM links WHERE id=$1;", link_id)
        if not row:
            return False
        if bool(row["is_used"]) is True:
            return False
        await conn.execute("DELETE FROM links WHERE id=$1;", link_id)
        return True


async def pop_available_link_for_order(order_id: int, user_id: int) -> str | None:
    """
    Atomically pick one unused link, mark used, assign to order.
    PostgreSQL-safe with FOR UPDATE SKIP LOCKED.
    """
    _ensure_pool()
    async with _pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT id, link
                FROM links
                WHERE is_used=FALSE
                ORDER BY id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1;
                """
            )
            if not row:
                return None

            link_id = int(row["id"])
            link = str(row["link"])

            await conn.execute(
                """
                UPDATE links
                SET is_used=TRUE,
                    used_by_order_id=$1,
                    used_by_user_id=$2,
                    used_at=NOW()
                WHERE id=$3;
                """,
                order_id,
                user_id,
                link_id,
            )

            await conn.execute(
                "UPDATE orders SET delivered_link=$1, status='delivered' WHERE id=$2;",
                link,
                order_id,
            )

            return link


async def list_all_links(limit: int = 200):
    _ensure_pool()
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, link, is_used, used_by_order_id, used_by_user_id, created_at, used_at
            FROM links
            ORDER BY id DESC
            LIMIT $1;
            """,
            limit,
        )
        return [dict(r) for r in rows]


async def update_link(link_id: int, new_link: str) -> bool:
    _ensure_pool()
    new_link = (new_link or "").strip()
    if not new_link:
        return False

    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_used FROM links WHERE id=$1;", link_id)
        if not row:
            return False
        if bool(row["is_used"]) is True:
            return False

        try:
            await conn.execute("UPDATE links SET link=$1 WHERE id=$2;", new_link, link_id)
            return True
        except Exception:
            return False