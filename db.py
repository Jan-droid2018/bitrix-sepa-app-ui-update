from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path


DB_PATH = Path(__file__).resolve().parent / "database.db"
_INITIALIZED_DB_PATH: str | None = None


@contextmanager
def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    try:
        yield conn
    finally:
        conn.close()


def _today_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _current_month_key() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _month_key(value: str | None) -> str | None:
    if not value:
        return None
    return str(value)[:7]


def _needs_month_reset(last_reset: str | None) -> bool:
    return _month_key(last_reset) != _current_month_key()


def _row_to_dict(row: sqlite3.Row | None) -> dict | None:
    if row is None:
        return None
    return dict(row)


def _init_db():
    global _INITIALIZED_DB_PATH

    current_db_path = str(DB_PATH.resolve())
    if _INITIALIZED_DB_PATH == current_db_path and DB_PATH.exists():
        return

    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                member_id TEXT PRIMARY KEY,
                plan TEXT DEFAULT 'free',
                exports_used INTEGER DEFAULT 0,
                last_reset TEXT,
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_stripe_customer_id ON users(stripe_customer_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_stripe_subscription_id ON users(stripe_subscription_id)"
        )
        conn.commit()
    _INITIALIZED_DB_PATH = current_db_path


def _fetch_user(member_id: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT member_id, plan, exports_used, last_reset, stripe_customer_id, stripe_subscription_id
            FROM users
            WHERE member_id = ?
            """,
            (member_id,),
        ).fetchone()
    return _row_to_dict(row)


def create_user_if_not_exists(member_id: str | None) -> dict | None:
    if not member_id:
        return None

    _init_db()
    with _connect() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO users (member_id, last_reset)
            VALUES (?, ?)
            """,
            (member_id, _today_str()),
        )
        conn.commit()
    return _fetch_user(member_id)


def get_user(member_id: str | None) -> dict | None:
    if not member_id:
        return None

    create_user_if_not_exists(member_id)
    user = _fetch_user(member_id)
    if user and _needs_month_reset(user.get("last_reset")):
        reset_user(member_id)
        user = _fetch_user(member_id)
    return user


def can_export(user: dict | None) -> bool:
    if not user:
        return False

    plan = (user.get("plan") or "free").strip().lower()
    if plan == "pro":
        return True

    return int(user.get("exports_used") or 0) < 1


def increase_export(member_id: str | None) -> dict | None:
    if not member_id:
        return None

    user = get_user(member_id)
    if not user:
        return None

    if (user.get("plan") or "free").strip().lower() == "pro":
        return user

    with _connect() as conn:
        conn.execute(
            """
            UPDATE users
            SET exports_used = COALESCE(exports_used, 0) + 1,
                last_reset = ?
            WHERE member_id = ?
            """,
            (_today_str(), member_id),
        )
        conn.commit()
    return get_user(member_id)


def reset_user(member_id: str | None) -> dict | None:
    if not member_id:
        return None

    create_user_if_not_exists(member_id)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE users
            SET exports_used = 0,
                last_reset = ?
            WHERE member_id = ?
            """,
            (_today_str(), member_id),
        )
        conn.commit()
    return _fetch_user(member_id)


def set_plan(member_id: str | None, plan: str) -> dict | None:
    if not member_id:
        return None

    normalized_plan = "pro" if (plan or "").strip().lower() == "pro" else "free"
    create_user_if_not_exists(member_id)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE users
            SET plan = ?
            WHERE member_id = ?
            """,
            (normalized_plan, member_id),
        )
        conn.commit()
    return get_user(member_id)


def set_stripe_ids(
    member_id: str | None,
    stripe_customer_id: str | None,
    stripe_subscription_id: str | None,
) -> dict | None:
    if not member_id:
        return None

    create_user_if_not_exists(member_id)
    with _connect() as conn:
        conn.execute(
            """
            UPDATE users
            SET stripe_customer_id = ?,
                stripe_subscription_id = ?
            WHERE member_id = ?
            """,
            (stripe_customer_id, stripe_subscription_id, member_id),
        )
        conn.commit()
    return get_user(member_id)


def find_user_by_stripe_ids(
    *,
    stripe_customer_id: str | None = None,
    stripe_subscription_id: str | None = None,
) -> dict | None:
    if not stripe_customer_id and not stripe_subscription_id:
        return None

    _init_db()
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT member_id, plan, exports_used, last_reset, stripe_customer_id, stripe_subscription_id
            FROM users
            WHERE stripe_customer_id = ? OR stripe_subscription_id = ?
            LIMIT 1
            """,
            (stripe_customer_id, stripe_subscription_id),
        ).fetchone()
    user = _row_to_dict(row)
    if user and _needs_month_reset(user.get("last_reset")):
        reset_user(user["member_id"])
        return _fetch_user(user["member_id"])
    return user


_init_db()
