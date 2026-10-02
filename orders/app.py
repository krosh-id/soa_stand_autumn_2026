import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
import queue
import re
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import contextmanager

import psycopg

PORT = int(os.environ.get("PORT", "8080"))
DATABASE_URL = os.environ["DATABASE_URL"]
PAYMENT_URL = os.environ["PAYMENT_URL"].rstrip("/")
INVENTORY_URL = os.environ["INVENTORY_URL"].rstrip("/")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
"""


class ConnectionPool:
    def __init__(self, dsn, min_size=5, max_size=30):
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


pool = None


def init_db():
    global pool
    for attempt in range(60):
        try:
            with psycopg.connect(DATABASE_URL, autocommit=True) as conn:
                conn.execute(SCHEMA)
            pool = ConnectionPool(DATABASE_URL, min_size=5, max_size=40)
            log.info("Database initialized successfully")
            return
        except psycopg.OperationalError as e:
            log.info("Database not ready (%s), waiting...", e)
            time.sleep(1)
    raise SystemExit("Could not connect to database after 60s")


class CircuitBreaker:
    def __init__(self, failure_threshold=3, cooldown=5.0):
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self.consecutive_failures = 0
        self.opened_at = 0.0
        self.lock = threading.Lock()

    def can_attempt(self) -> bool:
        with self.lock:
            if self.consecutive_failures < self.failure_threshold:
                return True
            now = time.time()
            if now - self.opened_at >= self.cooldown:
                self.opened_at = now
                return True
            return False

    def record_success(self):
        with self.lock:
            self.consecutive_failures = 0
            self.opened_at = 0.0

    def record_failure(self):
        with self.lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.failure_threshold:
                self.opened_at = time.time()


breaker = CircuitBreaker(failure_threshold=3, cooldown=5.0)


def reserve_inventory(order_id, items):
    body = json.dumps({
        "order_id": order_id,
        "items": [{"sku": i["sku"], "qty": i["qty"]} for i in items],
    }).encode("utf-8")
    req = urllib.request.Request(f"{INVENTORY_URL}/reservations", data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Connection", "close")
    with urllib.request.urlopen(req, timeout=0.5) as resp:
        if resp.status not in (200, 201):
            raise RuntimeError(f"Inventory reservation returned status {resp.status}")


def release_inventory(order_id):
    req = urllib.request.Request(
        f"{INVENTORY_URL}/reservations/{urllib.parse.quote(order_id)}",
        method="DELETE"
    )
    req.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(req, timeout=0.5):
            pass
    except Exception as e:
        log.warning("Inventory release failed for %s: %s", order_id, e)


def check_payment_charges(order_id, timeout=0.5):
    url = f"{PAYMENT_URL}/payments?order_id={urllib.parse.quote(order_id)}"
    req = urllib.request.Request(url, method="GET")
    req.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status == 200:
                data = json.loads(resp.read() or b"[]")
                return True, data
    except Exception:
        pass
    return False, None


def call_payment_charge(order_id, amount_cents, timeout=0.35):
    url = f"{PAYMENT_URL}/payments"
    body = json.dumps({
        "order_id": order_id,
        "amount_cents": amount_cents,
        "currency": "RUB",
    }).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Idempotency-Key", order_id)
    req.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status in (200, 201):
                return "paid"
            return "network_error"
    except urllib.error.HTTPError as e:
        if e.code == 402:
            return "declined"
        if e.code == 500:
            return "error_500"
        return "network_error"
    except (socket.timeout, TimeoutError):
        return "timeout"
    except Exception:
        return "network_error"


def insert_order(order_id, user_id, status, amount_cents, items):
    with pool.connection() as conn:
        conn.execute(
            """
            INSERT INTO orders (id, user_id, status, amount_cents, items)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status
            """,
            (order_id, user_id, status, amount_cents, json.dumps(items)),
        )


def update_order_status(order_id, status):
    with pool.connection() as conn:
        conn.execute(
            "UPDATE orders SET status = %s WHERE id = %s",
            (status, order_id),
        )


def get_order(order_id):
    with pool.connection() as conn:
        row = conn.execute(
            "SELECT id, status, amount_cents FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()
    if row is None:
        return None
    return {"id": row[0], "status": row[1], "amount_cents": row[2]}


def get_pending_orders(limit=100):
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, amount_cents FROM orders WHERE status = 'pending' ORDER BY created_at ASC LIMIT %s",
            (limit,),
        ).fetchall()
    return [(r[0], r[1]) for r in rows]


def background_worker_loop():
    while True:
        try:
            time.sleep(0.2)
            pending = get_pending_orders(limit=100)
            if not pending:
                continue
            for order_id, amount_cents in pending:
                if not breaker.can_attempt():
                    break

                # 1. First check if charge was already recorded (e.g. S3 slow response, or S5 dropped response)
                ok, charges = check_payment_charges(order_id, timeout=1.0)
                if ok:
                    breaker.record_success()
                    if charges:
                        update_order_status(order_id, "paid")
                        continue
                else:
                    breaker.record_failure()
                    break

                # 2. Charge with Idempotency-Key
                if not breaker.can_attempt():
                    break

                res = call_payment_charge(order_id, amount_cents, timeout=2.5)
                if res == "paid":
                    breaker.record_success()
                    update_order_status(order_id, "paid")
                elif res == "declined":
                    breaker.record_success()
                    release_inventory(order_id)
                    update_order_status(order_id, "rejected")
                elif res == "network_error":
                    ok, charges = check_payment_charges(order_id, timeout=1.0)
                    if ok and charges:
                        breaker.record_success()
                        update_order_status(order_id, "paid")
                    else:
                        breaker.record_failure()
                        break
                elif res == "error_500":
                    breaker.record_failure()
                    time.sleep(0.05)
                elif res == "timeout":
                    breaker.record_failure()
                    break
        except Exception as e:
            log.error("Error in background worker: %s", e)
            time.sleep(0.5)


def valid(body):
    if not isinstance(body, dict):
        return False
    user_id = body.get("user_id")
    if not isinstance(user_id, str) or not user_id.strip():
        return False
    items = body.get("items")
    if not isinstance(items, list) or not items:
        return False
    for i in items:
        if not isinstance(i, dict):
            return False
        sku = i.get("sku")
        if not isinstance(sku, str) or not sku.strip():
            return False
        qty = i.get("qty")
        if not isinstance(qty, int) or isinstance(qty, bool) or qty < 1:
            return False
        price_cents = i.get("price_cents")
        if not isinstance(price_cents, int) or isinstance(price_cents, bool) or price_cents < 0:
            return False
    return True


def create_order(body):
    order_id = str(uuid.uuid4())
    amount = sum(it["qty"] * it["price_cents"] for it in body["items"])
    items = body["items"]
    user_id = body["user_id"]

    # 1. Reserve inventory
    try:
        reserve_inventory(order_id, items)
    except Exception as e:
        log.error("Failed to reserve inventory for %s: %s", order_id, e)
        insert_order(order_id, user_id, "pending", amount, items)
        return {"id": order_id, "status": "pending"}

    # 2. Check circuit breaker before attempting Payment
    if not breaker.can_attempt():
        insert_order(order_id, user_id, "pending", amount, items)
        return {"id": order_id, "status": "pending"}

    # 3. Synchronous Payment attempt with short timeout (0.35s)
    status = "pending"
    try:
        result = call_payment_charge(order_id, amount, timeout=0.35)
        if result == "paid":
            breaker.record_success()
            status = "paid"
        elif result == "declined":
            breaker.record_success()
            release_inventory(order_id)
            status = "rejected"
        elif result == "error_500":
            # Quick immediate retry if budget permits
            retry_res = call_payment_charge(order_id, amount, timeout=0.15)
            if retry_res == "paid":
                breaker.record_success()
                status = "paid"
            elif retry_res == "declined":
                breaker.record_success()
                release_inventory(order_id)
                status = "rejected"
            else:
                breaker.record_failure()
                status = "pending"
        elif result == "network_error":
            # In S5, response was lost after charge completed
            ok, charges = check_payment_charges(order_id, timeout=0.15)
            if ok and charges:
                breaker.record_success()
                status = "paid"
            else:
                breaker.record_failure()
                status = "pending"
        else:  # timeout (S3 or S4)
            breaker.record_failure()
            status = "pending"
    except Exception as e:
        log.warning("Payment attempt exception for %s: %s", order_id, e)
        breaker.record_failure()
        status = "pending"

    insert_order(order_id, user_id, status, amount, items)
    return {"id": order_id, "status": status}


ORDER_PATH = re.compile(r"^/orders/([^/]+)$")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def send_json(self, code, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        path = self.path.split("?")[0]
        if path == "/healthz":
            return self.send_json(200, {"status": "ok"})
        m = ORDER_PATH.match(path)
        if m:
            order_id = urllib.parse.unquote(m.group(1))
            order = get_order(order_id)
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)
        self.send_json(404, {"error": "not_found"})

    def do_POST(self):
        path = self.path.split("?")[0]
        if path != "/orders":
            return self.send_json(404, {"error": "not_found"})
        try:
            length = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(length) or b"null")
        except Exception:
            body = None

        if not valid(body):
            return self.send_json(400, {"error": "bad_request"})

        # Never return 5xx on POST /orders
        try:
            resp = create_order(body)
            return self.send_json(201, resp)
        except Exception as e:
            log.exception("Unexpected error in create_order: %s", e)
            order_id = str(uuid.uuid4())
            amount = sum(it["qty"] * it["price_cents"] for it in body["items"])
            try:
                insert_order(order_id, body["user_id"], "pending", amount, body["items"])
            except Exception:
                pass
            return self.send_json(201, {"id": order_id, "status": "pending"})

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    init_db()
    worker_thread = threading.Thread(target=background_worker_loop, daemon=True)
    worker_thread.start()
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    server.daemon_threads = True
    log.info("orders service running on port %d", PORT)
    server.serve_forever()
