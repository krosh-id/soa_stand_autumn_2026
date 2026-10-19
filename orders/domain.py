"""Domain model and pure business data types.

No HTTP, SQL or framework imports belong here.
"""

from dataclasses import dataclass
from enum import Enum


class OrderStatus(str, Enum):
    PENDING = "pending"
    PAID = "paid"
    REJECTED = "rejected"


class PaymentOutcome(str, Enum):
    """Business-relevant result of a charge attempt.

    PAID             - payment is known to have succeeded.
    DECLINED         - bank explicitly rejected it; money was not charged.
    RETRYABLE_ERROR  - explicit temporary failure where retry is safe.
    UNKNOWN          - no reliable result is known; reconcile before deciding.
    """

    PAID = "paid"
    DECLINED = "declined"
    RETRYABLE_ERROR = "retryable_error"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class PaymentResult:
    outcome: PaymentOutcome
    reason: str | None = None


@dataclass(frozen=True)
class PendingOrder:
    id: str
    amount_cents: int
    items: list[dict]
    stage: str


@dataclass(frozen=True)
class OutgoingEvent:
    id: str
    order_id: str
    occurred_at: str

    def payload(self) -> dict:
        return {"id": self.id, "type": "order.paid", "order_id": self.order_id,
                "occurred_at": self.occurred_at}


@dataclass(frozen=True)
class Delivery:
    id: str
    user_id: str
    bonus_points: int


@dataclass(frozen=True)
class OrderDraft:
    user_id: str
    items: list[dict]

    @property
    def amount_cents(self) -> int:
        return sum(item["qty"] * item["price_cents"] for item in self.items)


class InvalidOrderRequest(ValueError):
    """Raised by the application service when an order request is invalid."""


def parse_order_request(body: object) -> OrderDraft:
    """Validate transport data and turn it into an application/domain input.

    Keeping this conversion outside the HTTP handler means validation rules can
    be reused by another transport without duplicating business/application rules.
    """
    if not isinstance(body, dict):
        raise InvalidOrderRequest("body must be an object")

    user_id = body.get("user_id")
    if not isinstance(user_id, str) or not user_id.strip():
        raise InvalidOrderRequest("user_id must be a non-empty string")

    items = body.get("items")
    if not isinstance(items, list) or not items:
        raise InvalidOrderRequest("items must be a non-empty list")

    normalized_items: list[dict] = []
    for item in items:
        if not isinstance(item, dict):
            raise InvalidOrderRequest("every item must be an object")
        sku = item.get("sku")
        qty = item.get("qty")
        price_cents = item.get("price_cents")
        if not isinstance(sku, str) or not sku.strip():
            raise InvalidOrderRequest("sku must be a non-empty string")
        if not isinstance(qty, int) or isinstance(qty, bool) or qty < 1:
            raise InvalidOrderRequest("qty must be a positive integer")
        if not isinstance(price_cents, int) or isinstance(price_cents, bool) or price_cents < 0:
            raise InvalidOrderRequest("price_cents must be a non-negative integer")
        normalized_items.append({"sku": sku, "qty": qty, "price_cents": price_cents})

    return OrderDraft(user_id=user_id, items=normalized_items)


def parse_delivery(body: object) -> Delivery:
    if not isinstance(body, dict) or body.get("type") != "delivery.confirmed":
        raise InvalidOrderRequest("expected delivery.confirmed event")
    for field in ("id", "user_id"):
        if not isinstance(body.get(field), str) or not body[field].strip():
            raise InvalidOrderRequest(f"{field} must be a non-empty string")
    points = body.get("bonus_points")
    if not isinstance(points, int) or isinstance(points, bool) or points < 1:
        raise InvalidOrderRequest("bonus_points must be a positive integer")
    return Delivery(body["id"], body["user_id"], points)
