"""Real PostgreSQL checks. TEST_DATABASE_URL must point to a disposable test DB."""

from concurrent.futures import ThreadPoolExecutor
import os
import unittest

from domain import Delivery

TEST_DSN = os.environ.get("TEST_DATABASE_URL")


@unittest.skipUnless(TEST_DSN, "set TEST_DATABASE_URL to a disposable PostgreSQL database")
class RepositoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from repository import ConnectionPool, PostgresOrderRepository, initialize_schema
        cls.initialize_schema = staticmethod(initialize_schema)
        initialize_schema(TEST_DSN)
        cls.pool = ConnectionPool(TEST_DSN, min_size=1)
        cls.repo = PostgresOrderRepository(cls.pool)

    @classmethod
    def tearDownClass(cls):
        while not cls.pool.pool.empty():
            cls.pool.pool.get_nowait().close()

    def setUp(self):
        with self.pool.connection() as conn:
            conn.execute("TRUNCATE outbox, orders, bonus_deliveries")

    def insert_order(self, order_id="o1"):
        self.repo.insert(order_id, "u1", "pending", 100,
                         [{"sku": "a", "qty": 1, "price_cents": 100}])

    def test_paid_and_outbox_commit_together_and_are_idempotent(self):
        self.insert_order()
        self.repo.mark_paid("o1")
        event = self.repo.unsent_events()[0]
        self.repo.mark_paid("o1")
        self.assertEqual(self.repo.get("o1")["status"], "paid")
        self.assertEqual(self.repo.unsent_events(), [event])
        self.repo.mark_sent(event.id)
        self.initialize_schema(TEST_DSN)
        self.assertEqual(self.repo.unsent_events(), [])

    def test_failed_outbox_insert_rolls_back_paid(self):
        import psycopg
        self.insert_order()
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE FUNCTION fail_outbox_test() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'simulated outbox failure'; END $$;
                CREATE TRIGGER fail_outbox_test BEFORE INSERT ON outbox
                FOR EACH ROW EXECUTE FUNCTION fail_outbox_test();
            """)
        try:
            with self.assertRaises(psycopg.Error):
                self.repo.mark_paid("o1")
            self.assertEqual(self.repo.get("o1")["status"], "pending")
            self.assertEqual(self.repo.unsent_events(), [])
        finally:
            with self.pool.connection() as conn:
                conn.execute("DROP TRIGGER fail_outbox_test ON outbox; DROP FUNCTION fail_outbox_test()")

    def test_rejected_or_missing_orders_cannot_publish(self):
        self.insert_order()
        self.repo.update_status("o1", "rejected")
        for order_id in ("o1", "missing"):
            with self.assertRaises(ValueError):
                self.repo.mark_paid(order_id)
        self.assertEqual(self.repo.unsent_events(), [])

    def test_parallel_duplicates_and_different_events_for_same_user(self):
        events = [Delivery(f"d{i}", "u1", i + 1) for i in range(20)]
        with ThreadPoolExecutor(max_workers=12) as executor:
            list(executor.map(self.repo.record_delivery, events * 4))
        self.assertEqual(self.repo.bonus_points("u1"), 210)
        self.assertEqual(self.repo.bonus_points("unknown"), 0)
        self.repo.record_delivery(events[0])
        self.assertEqual(self.repo.bonus_points("u1"), 210)

    def test_uncommitted_delivery_rolls_back_then_can_be_retried(self):
        with self.assertRaises(RuntimeError):
            with self.pool.connection() as conn, conn.transaction():
                conn.execute("INSERT INTO bonus_deliveries VALUES ('d1', 'u1', 50)")
                raise RuntimeError("process dies before commit")
        self.assertEqual(self.repo.bonus_points("u1"), 0)
        event = Delivery("d1", "u1", 50)
        self.repo.record_delivery(event)
        # Simulate lost HTTP acknowledgement, then delivery to a restarted application.
        from repository import PostgresOrderRepository
        restarted = PostgresOrderRepository(self.pool)
        restarted.record_delivery(event)
        self.assertEqual(restarted.bonus_points("u1"), 50)

    def test_pending_stage_and_items_survive_new_repository_instance(self):
        self.insert_order()
        self.repo.set_stage("o1", "release")
        from repository import PostgresOrderRepository
        pending = PostgresOrderRepository(self.pool).get_pending("o1")
        self.assertEqual(pending.stage, "release")
        self.assertEqual(pending.items[0]["sku"], "a")

    def test_upgrade_legacy_schema_preserves_orders_and_backfills_outbox(self):
        with self.pool.connection() as conn:
            conn.execute("""
                DROP TABLE outbox;
                DROP TABLE orders;
                CREATE TABLE orders (
                    id TEXT PRIMARY KEY, user_id TEXT NOT NULL, status TEXT NOT NULL,
                    amount_cents BIGINT NOT NULL, items JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                INSERT INTO orders (id, user_id, status, amount_cents, items) VALUES
                    ('old-paid', 'u1', 'paid', 100, '[]'),
                    ('old-pending', 'u1', 'pending', 100, '[]');
            """)
        self.initialize_schema(TEST_DSN)
        self.initialize_schema(TEST_DSN)
        self.assertEqual(self.repo.get("old-paid")["status"], "paid")
        self.assertEqual(self.repo.get_pending("old-pending").stage, "reserve")
        self.assertEqual([e.order_id for e in self.repo.unsent_events()], ["old-paid"])
