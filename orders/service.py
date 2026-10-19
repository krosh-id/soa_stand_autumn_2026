"""Application use cases; SQL and HTTP details belong to adapters."""

from contextlib import contextmanager
import logging
import threading
import time
import uuid

from domain import InvalidOrderRequest, OrderStatus, PaymentOutcome, PendingOrder, parse_order_request
from ports import InventoryGateway, OrderRepository, PaymentCircuit, PaymentGateway

log = logging.getLogger(__name__)


class OrderService:
    def __init__(self, repository: OrderRepository, inventory: InventoryGateway,
                 payment: PaymentGateway, breaker: PaymentCircuit):
        self.repository = repository
        self.inventory = inventory
        self.payment = payment
        self.breaker = breaker
        self._active: set[str] = set()
        self._lock = threading.Lock()

    @contextmanager
    def _claim(self, order_id: str):
        # Only the set is protected; never keep this mutex during SQL or HTTP.
        with self._lock:
            acquired = order_id not in self._active
            if acquired:
                self._active.add(order_id)
        try:
            yield acquired
        finally:
            if acquired:
                with self._lock:
                    self._active.remove(order_id)

    def get(self, order_id: str) -> dict | None:
        return self.repository.get(order_id)

    def create(self, body: object) -> dict:
        deadline = time.monotonic() + 0.8
        draft = parse_order_request(body)
        order_id = str(uuid.uuid4())
        # Claim before INSERT: the worker must not race the initial HTTP attempt.
        with self._claim(order_id):
            self.repository.insert(order_id, draft.user_id, "pending", draft.amount_cents, draft.items)
            order = PendingOrder(order_id, draft.amount_cents, draft.items, "reserve")
            self._try_advance(order, deadline)
            result = self.repository.get(order_id)
            return {"id": order_id, "status": result["status"]}

    def process_pending(self, order: PendingOrder) -> None:
        with self._claim(order.id) as acquired:
            if not acquired:
                return
            # A candidate may have become terminal since the worker fetched it.
            current = self.repository.get_pending(order.id)
            if current is not None:
                self._try_advance(current, None)

    def _try_advance(self, order: PendingOrder, deadline: float | None) -> None:
        try:
            self._advance(order, deadline)
        except Exception as exc:
            # The durable stage is unchanged or already advanced. Retry later.
            log.warning("Order %s remains recoverable: %s", order.id, exc)

    @staticmethod
    def _timeout(deadline: float | None, maximum: float) -> float:
        if deadline is None:
            return maximum
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("fast path budget exhausted")
        return min(maximum, remaining)

    def _advance(self, order: PendingOrder, deadline: float | None) -> None:
        stage = order.stage
        if stage == "reserve":
            self.inventory.reserve(order.id, order.items, timeout=self._timeout(deadline, 0.5))
            self.repository.set_stage(order.id, "pay")
            stage = "pay"
        if stage == "release":
            self._release(order.id, deadline)
            return
        if not self.breaker.can_attempt():
            return

        # Recovery first reconciles an operation that could have succeeded before a crash.
        if deadline is None:
            ok, charges = self.payment.lookup(order.id, timeout=1.0)
            if not ok:
                self.breaker.record_failure()
                return
            # Preserve task 1 policy: a successful Payment lookup closes the breaker.
            self.breaker.record_success()
            if charges:
                self.repository.mark_paid(order.id)
                return
            # An empty lookup does not prove an older request has finished.
            # Retrying is safe because the operation key remains order.id.

        result = self.payment.charge(order.id, order.amount_cents,
                                     timeout=self._timeout(deadline, 0.35 if deadline is not None else 2.5))
        if result.outcome == PaymentOutcome.RETRYABLE_ERROR and deadline is not None:
            result = self.payment.charge(order.id, order.amount_cents,
                                         timeout=self._timeout(deadline, 0.15))
        if result.outcome == PaymentOutcome.PAID:
            self.breaker.record_success()
            self.repository.mark_paid(order.id)
        elif result.outcome == PaymentOutcome.DECLINED:
            self.breaker.record_success()
            self.repository.set_stage(order.id, "release")
            self._release(order.id, deadline)
        elif result.outcome == PaymentOutcome.UNKNOWN:
            ok, charges = self.payment.lookup(order.id, timeout=self._timeout(deadline, 0.15 if deadline is not None else 1.0))
            if ok and charges:
                self.breaker.record_success()
                self.repository.mark_paid(order.id)
            else:
                self.breaker.record_failure()
        else:
            self.breaker.record_failure()

    def _release(self, order_id: str, deadline: float | None) -> None:
        self.inventory.release(order_id, timeout=self._timeout(deadline, 0.5))
        self.repository.update_status(order_id, OrderStatus.REJECTED.value)


__all__ = ["OrderService", "InvalidOrderRequest"]
