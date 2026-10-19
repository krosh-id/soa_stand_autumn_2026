"""Unit tests for the application layer only: no DB and no network."""

import threading
import unittest
from unittest.mock import patch

from domain import InvalidOrderRequest, PaymentOutcome, PaymentResult, PendingOrder
from resilience import CircuitBreaker
from service import OrderService
from worker import PendingOrdersWorker

ITEMS = [{"sku": "sku-1", "qty": 2, "price_cents": 49900}]
REQUEST = {"user_id": "u-17", "items": ITEMS}


class FakeRepository:
    def __init__(self):
        self.orders = {}
        self.events = set()

    def insert(self, order_id, user_id, status, amount_cents, items):
        self.orders[order_id] = {
            "id": order_id,
            "user_id": user_id,
            "status": status,
            "amount_cents": amount_cents,
            "items": items,
            "stage": "reserve",
        }

    def update_status(self, order_id, status):
        self.orders[order_id]["status"] = status

    def set_stage(self, order_id, stage):
        self.orders[order_id]["stage"] = stage

    def mark_paid(self, order_id):
        self.orders[order_id]["status"] = "paid"
        self.events.add("order.paid:" + order_id)

    def get_pending(self, order_id):
        o = self.orders[order_id]
        if o["status"] == "pending":
            return PendingOrder(o["id"], o["amount_cents"], o["items"], o["stage"])
        return None

    def get(self, order_id):
        order = self.orders.get(order_id)
        if order is None:
            return None
        return {"id": order["id"], "status": order["status"], "amount_cents": order["amount_cents"]}

    def pending(self, limit=100):
        return [
            PendingOrder(o["id"], o["amount_cents"], o["items"], o["stage"])
            for o in self.orders.values()
            if o["status"] == "pending"
        ][:limit]


class FakeInventory:
    def __init__(self):
        self.reserved = []
        self.released = []
        self.fail_reserve = False
        self.fail_release = False

    def reserve(self, order_id, items, timeout=0.5):
        if self.fail_reserve:
            raise ConnectionError("Inventory is unavailable")
        self.reserved.append((order_id, items))

    def release(self, order_id, timeout=0.5):
        if self.fail_release:
            raise ConnectionError("release unavailable")
        self.released.append(order_id)


class FakePayment:
    def __init__(self, outcomes=None, lookup_result=(True, [])):
        self.outcomes = list(outcomes or [])
        self.lookup_result = lookup_result
        self.charge_calls = []
        self.lookup_calls = []

    def charge(self, order_id, amount_cents, timeout=0.35):
        self.charge_calls.append((order_id, amount_cents, timeout))
        return self.outcomes.pop(0) if self.outcomes else PaymentResult(PaymentOutcome.UNKNOWN, "timeout")

    def lookup(self, order_id, timeout=0.5):
        self.lookup_calls.append((order_id, timeout))
        return self.lookup_result


class OrderServiceTests(unittest.TestCase):
    def setUp(self):
        self.repo = FakeRepository()
        self.inventory = FakeInventory()
        self.payment = FakePayment()
        self.breaker = CircuitBreaker()
        self.orders = OrderService(self.repo, self.inventory, self.payment, self.breaker)
        self.worker = PendingOrdersWorker(self.repo, self.orders)

    def test_validation_is_owned_by_application_service(self):
        with self.assertRaises(InvalidOrderRequest):
            self.orders.create({"user_id": "", "items": ITEMS})
        with self.assertRaises(InvalidOrderRequest):
            self.orders.create({"user_id": "u", "items": [{"sku": "a", "qty": True, "price_cents": 5}]})
        self.assertEqual(self.repo.orders, {})

    def test_successful_payment(self):
        self.payment.outcomes = [PaymentResult(PaymentOutcome.PAID)]
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "paid")
        self.assertEqual(self.orders.get(result["id"])["amount_cents"], 99800)
        self.assertEqual(len(self.inventory.reserved), 1)
        self.assertEqual(self.payment.charge_calls[0][2], 0.35)

    def test_decline_releases_reservation(self):
        self.payment.outcomes = [PaymentResult(PaymentOutcome.DECLINED)]
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "rejected")
        self.assertEqual(self.inventory.released, [result["id"]])

    def test_retryable_error_is_retried_once_with_same_operation_key(self):
        self.payment.outcomes = [
            PaymentResult(PaymentOutcome.RETRYABLE_ERROR, "http_500"),
            PaymentResult(PaymentOutcome.PAID),
        ]
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "paid")
        self.assertEqual(len(self.payment.charge_calls), 2)
        self.assertEqual([c[0] for c in self.payment.charge_calls], [result["id"], result["id"]])
        self.assertEqual([c[2] for c in self.payment.charge_calls], [0.35, 0.15])

    def test_unknown_result_is_reconciled_by_lookup(self):
        self.payment.outcomes = [PaymentResult(PaymentOutcome.UNKNOWN, "lost_response")]
        self.payment.lookup_result = (True, [{"charge_id": "charge-1"}])
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "paid")
        self.assertEqual(len(self.payment.lookup_calls), 1)

    def test_unknown_without_confirmed_charge_stays_pending(self):
        self.payment.outcomes = [PaymentResult(PaymentOutcome.UNKNOWN, "timeout")]
        self.payment.lookup_result = (False, None)
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "pending")

    def test_reservation_error_leaves_pending_without_payment(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(self.payment.charge_calls, [])

    def test_open_breaker_leaves_pending_without_payment(self):
        for _ in range(3):
            self.breaker.record_failure()
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(self.payment.charge_calls, [])

    def test_worker_delegates_pending_business_logic_to_order_service(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        self.payment.lookup_result = (True, [{"charge_id": "c1"}])
        self.inventory.fail_reserve = False
        self.worker.run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "paid")
        self.assertEqual(self.payment.charge_calls, [])

    def test_process_pending_charges_after_empty_lookup(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        self.payment.lookup_result = (True, [])
        self.payment.outcomes = [PaymentResult(PaymentOutcome.PAID)]
        self.inventory.fail_reserve = False
        self.worker.run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "paid")
        self.assertEqual(self.payment.charge_calls[0][0], result["id"])

    def test_process_pending_unknown_reconciles_before_deciding(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        calls = iter([(True, []), (True, [{"charge_id": "c2"}])])
        self.payment.lookup = lambda order_id, timeout=0.5: next(calls)
        self.payment.outcomes = [PaymentResult(PaymentOutcome.UNKNOWN, "lost_response")]
        self.inventory.fail_reserve = False
        self.worker.run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "paid")

    def test_worker_keeps_pending_when_lookup_unavailable(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        self.payment.lookup_result = (False, None)
        self.inventory.fail_reserve = False
        self.worker.run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "pending")
        self.assertEqual(self.payment.charge_calls, [])

    def test_order_is_durable_before_reservation_and_charge(self):
        original_reserve = self.inventory.reserve
        original_charge = self.payment.charge

        def reserve(order_id, items, timeout):
            self.assertEqual(self.repo.orders[order_id]["status"], "pending")
            self.assertEqual(self.repo.orders[order_id]["stage"], "reserve")
            return original_reserve(order_id, items, timeout)

        def charge(order_id, amount_cents, timeout):
            self.assertEqual(self.repo.orders[order_id]["stage"], "pay")
            return original_charge(order_id, amount_cents, timeout)

        self.inventory.reserve = reserve
        self.payment.charge = charge
        self.payment.outcomes = [PaymentResult(PaymentOutcome.PAID)]
        result = self.orders.create(REQUEST)
        self.assertEqual(self.repo.events, {"order.paid:" + result["id"]})

    def test_release_failure_survives_restart_without_charging_again(self):
        self.inventory.fail_release = True
        self.payment.outcomes = [PaymentResult(PaymentOutcome.DECLINED)]
        result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(self.repo.orders[result["id"]]["stage"], "release")
        restarted = OrderService(self.repo, self.inventory, self.payment, CircuitBreaker())
        self.inventory.fail_release = False
        PendingOrdersWorker(self.repo, restarted).run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "rejected")
        self.assertEqual(len(self.payment.charge_calls), 1)
        self.assertEqual(self.repo.events, set())

    def test_worker_does_not_pay_without_reservation(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        self.worker.run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "pending")
        self.assertEqual(self.payment.charge_calls, [])

    def test_worker_skips_order_while_http_is_processing_it(self):
        entered, finish = threading.Event(), threading.Event()
        original = self.inventory.reserve

        def reserve(order_id, items, timeout):
            entered.set()
            if not finish.wait(2):
                raise TimeoutError("test did not release reservation")
            original(order_id, items, timeout)

        self.inventory.reserve = reserve
        self.payment.outcomes = [PaymentResult(PaymentOutcome.PAID)]
        thread = threading.Thread(target=self.orders.create, args=(REQUEST,))
        thread.start()
        try:
            self.assertTrue(entered.wait(1))
            self.worker.run_once()
            self.assertEqual(self.payment.charge_calls, [])
        finally:
            finish.set()
            thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(self.payment.charge_calls), 1)
        self.assertEqual(self.orders._active, set())

    def test_stale_worker_candidate_cannot_change_terminal_order(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        candidate = self.repo.pending()[0]
        self.repo.mark_paid(result["id"])
        self.orders.process_pending(candidate)
        self.assertEqual(self.payment.charge_calls, [])

    def test_fast_path_stops_at_shared_deadline(self):
        # Start at 10; by reservation time the entire 0.8s budget has elapsed.
        with patch("service.time.monotonic", side_effect=[10, 10.9]):
            result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(self.inventory.reserved, [])
        self.assertEqual(self.payment.charge_calls, [])

    def test_charge_survives_failure_before_paid_commit(self):
        self.payment.outcomes = [PaymentResult(PaymentOutcome.PAID)]
        with patch.object(self.repo, "mark_paid", side_effect=ConnectionError("commit failed")):
            result = self.orders.create(REQUEST)
        self.assertEqual(result["status"], "pending")
        self.assertEqual(self.repo.events, set())
        self.payment.lookup_result = (True, [{"charge_id": "already-captured"}])
        restarted = OrderService(self.repo, self.inventory, self.payment, CircuitBreaker())
        PendingOrdersWorker(self.repo, restarted).run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "paid")
        self.assertEqual(len(self.payment.charge_calls), 1)
        self.assertEqual(self.repo.events, {"order.paid:" + result["id"]})

    def test_failed_initial_insert_has_no_external_effect_and_releases_claim(self):
        with patch.object(self.repo, "insert", side_effect=ConnectionError("insert failed")):
            with self.assertRaises(ConnectionError):
                self.orders.create(REQUEST)
        self.assertEqual(self.inventory.reserved, [])
        self.assertEqual(self.payment.charge_calls, [])
        self.assertEqual(self.orders._active, set())

    def test_successful_lookup_closes_breaker_for_pending_recovery(self):
        self.inventory.fail_reserve = True
        result = self.orders.create(REQUEST)
        self.inventory.fail_reserve = False
        for _ in range(3):
            self.breaker.record_failure()
        self.payment.outcomes = [PaymentResult(PaymentOutcome.RETRYABLE_ERROR),
                                 PaymentResult(PaymentOutcome.PAID)]
        with patch("resilience.time.time", return_value=self.breaker.opened_at + 6):
            self.worker.run_once()
        # A responsive lookup closed the old failure streak. One 500 is one new failure.
        self.assertEqual(self.breaker.consecutive_failures, 1)
        self.worker.run_once()
        self.assertEqual(self.repo.get(result["id"])["status"], "paid")

    def test_circuit_breaker_cooldown(self):
        for _ in range(3):
            self.breaker.record_failure()
        self.assertFalse(self.breaker.can_attempt())
        with patch("resilience.time.time", return_value=self.breaker.opened_at + 10):
            self.assertTrue(self.breaker.can_attempt())
        self.breaker.record_success()
        self.assertTrue(self.breaker.can_attempt())


if __name__ == "__main__":
    unittest.main()
