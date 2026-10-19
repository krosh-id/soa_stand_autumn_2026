"""Ports (interfaces) owned by the application layer.

The application depends on these abstractions; PostgreSQL and HTTP are adapters.
"""

from typing import Protocol

from domain import Delivery, OutgoingEvent, PendingOrder, PaymentResult


class OrderRepository(Protocol):
    def insert(
        self,
        order_id: str,
        user_id: str,
        status: str,
        amount_cents: int,
        items: list[dict],
    ) -> None: ...

    def update_status(self, order_id: str, status: str) -> None: ...

    def set_stage(self, order_id: str, stage: str) -> None: ...

    def mark_paid(self, order_id: str) -> None: ...

    def get_pending(self, order_id: str) -> PendingOrder | None: ...

    def get(self, order_id: str) -> dict | None: ...

    def pending(self, limit: int = 100) -> list[PendingOrder]: ...


class InventoryGateway(Protocol):
    def reserve(self, order_id: str, items: list[dict], timeout: float = 0.5) -> None: ...

    def release(self, order_id: str, timeout: float = 0.5) -> None: ...


class PaymentGateway(Protocol):
    def charge(
        self,
        order_id: str,
        amount_cents: int,
        timeout: float = 0.35,
    ) -> PaymentResult: ...

    def lookup(self, order_id: str, timeout: float = 0.5) -> tuple[bool, list | None]: ...


class PaymentCircuit(Protocol):
    def can_attempt(self) -> bool: ...

    def record_success(self) -> None: ...

    def record_failure(self) -> None: ...


class EventRepository(Protocol):
    def unsent_events(self, limit: int = 100) -> list[OutgoingEvent]: ...

    def mark_sent(self, event_id: str) -> None: ...


class EventsGateway(Protocol):
    def publish(self, event: OutgoingEvent) -> None: ...


class BonusRepository(Protocol):
    def record_delivery(self, event: Delivery) -> None: ...

    def bonus_points(self, user_id: str) -> int: ...
