"""PostgreSQL adapter: all SQL and connection management live here."""

from contextlib import contextmanager
import json
import logging
import queue
import time

import psycopg

from domain import Delivery, OutgoingEvent, PendingOrder

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE orders ADD COLUMN IF NOT EXISTS stage TEXT NOT NULL DEFAULT 'reserve';
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE TABLE IF NOT EXISTS outbox (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES orders(id),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_outbox_unsent ON outbox(occurred_at) WHERE sent_at IS NULL;
CREATE TABLE IF NOT EXISTS bonus_deliveries (
    event_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    bonus_points BIGINT NOT NULL CHECK (bonus_points > 0)
);
CREATE INDEX IF NOT EXISTS idx_bonus_user ON bonus_deliveries(user_id);
INSERT INTO outbox (id, order_id)
SELECT 'order.paid:' || id, id FROM orders WHERE status = 'paid'
ON CONFLICT (order_id) DO NOTHING;
"""


def initialize_schema(dsn: str) -> None:
    """Retry startup until the database becomes available, like the original."""
    for _ in range(60):
        try:
            with psycopg.connect(dsn, autocommit=True) as conn:
                conn.execute(SCHEMA)
            log.info("Database initialized successfully")
            return
        except psycopg.OperationalError as exc:
            log.info("Database not ready (%s), waiting...", exc)
            time.sleep(1)
    raise SystemExit("Could not connect to database after 60s")


class ConnectionPool:
    """Simple connection reuse from the original code; NOT a hard connection limit."""

    def __init__(self, dsn: str, min_size: int = 5, max_size: int = 40):
        self.dsn = dsn
        self.pool = queue.Queue(maxsize=max_size)
        for _ in range(min_size):
            try:
                self.pool.put(psycopg.connect(dsn, autocommit=True))
            except Exception:
                pass

    @contextmanager
    def connection(self):
        conn = None
        try:
            conn = self.pool.get_nowait()
            if conn.closed:
                conn = psycopg.connect(self.dsn, autocommit=True)
        except queue.Empty:
            conn = psycopg.connect(self.dsn, autocommit=True)
        try:
            yield conn
        except Exception:
            if conn and not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            if conn and not conn.closed:
                try:
                    self.pool.put_nowait(conn)
                except queue.Full:
                    try:
                        conn.close()
                    except Exception:
                        pass
            elif conn:
                try:
                    conn.close()
                except Exception:
                    pass


class PostgresOrderRepository:
    def __init__(self, connections: ConnectionPool):
        self.connections = connections

    def insert(self, order_id: str, user_id: str, status: str, amount_cents: int, items: list[dict]) -> None:
        with self.connections.connection() as conn:
            conn.execute(
                """
                INSERT INTO orders (id, user_id, status, amount_cents, items)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (order_id, user_id, status, amount_cents, json.dumps(items)),
            )

    def update_status(self, order_id: str, status: str) -> None:
        with self.connections.connection() as conn:
            conn.execute("UPDATE orders SET status = %s WHERE id = %s", (status, order_id))

    def get(self, order_id: str) -> dict | None:
        with self.connections.connection() as conn:
            row = conn.execute(
                "SELECT id, status, amount_cents FROM orders WHERE id = %s", (order_id,)
            ).fetchone()
        if row is None:
            return None
        return {"id": row[0], "status": row[1], "amount_cents": row[2]}

    def set_stage(self, order_id: str, stage: str) -> None:
        with self.connections.connection() as conn:
            conn.execute("UPDATE orders SET stage = %s WHERE id = %s AND status = 'pending'", (stage, order_id))

    def mark_paid(self, order_id: str) -> None:
        # autocommit connections still need an explicit transaction for these two writes.
        with self.connections.connection() as conn, conn.transaction():
            row = conn.execute(
                "UPDATE orders SET status = 'paid' WHERE id = %s "
                "AND status IN ('pending', 'paid') RETURNING id", (order_id,)
            ).fetchone()
            if row is None:
                raise ValueError("cannot mark a missing or rejected order paid")
            conn.execute(
                "INSERT INTO outbox (id, order_id) VALUES (%s, %s) "
                "ON CONFLICT (order_id) DO NOTHING", ("order.paid:" + order_id, order_id)
            )

    def get_pending(self, order_id: str) -> PendingOrder | None:
        with self.connections.connection() as conn:
            row = conn.execute(
                "SELECT id, amount_cents, items, stage FROM orders WHERE id = %s AND status = 'pending'",
                (order_id,),
            ).fetchone()
        return PendingOrder(*row) if row else None

    def pending(self, limit: int = 100) -> list[PendingOrder]:
        with self.connections.connection() as conn:
            rows = conn.execute(
                "SELECT id, amount_cents, items, stage FROM orders "
                "WHERE status = 'pending' ORDER BY created_at ASC LIMIT %s", (limit,)
            ).fetchall()
        return [PendingOrder(*row) for row in rows]

    def unsent_events(self, limit: int = 100) -> list[OutgoingEvent]:
        with self.connections.connection() as conn:
            rows = conn.execute(
                "SELECT id, order_id, occurred_at FROM outbox "
                "WHERE sent_at IS NULL ORDER BY occurred_at, id LIMIT %s", (limit,)
            ).fetchall()
        return [OutgoingEvent(row[0], row[1], row[2].isoformat()) for row in rows]

    def mark_sent(self, event_id: str) -> None:
        with self.connections.connection() as conn:
            conn.execute("UPDATE outbox SET sent_at = now() WHERE id = %s", (event_id,))

    def record_delivery(self, event: Delivery) -> None:
        # One autocommitted statement is the entire atomic business operation.
        with self.connections.connection() as conn:
            conn.execute(
                "INSERT INTO bonus_deliveries (event_id, user_id, bonus_points) VALUES (%s, %s, %s) "
                "ON CONFLICT (event_id) DO NOTHING", (event.id, event.user_id, event.bonus_points)
            )

    def bonus_points(self, user_id: str) -> int:
        with self.connections.connection() as conn:
            row = conn.execute(
                "SELECT COALESCE(SUM(bonus_points), 0) FROM bonus_deliveries WHERE user_id = %s", (user_id,)
            ).fetchone()
        return int(row[0])
