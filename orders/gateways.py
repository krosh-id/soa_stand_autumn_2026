"""HTTP adapters for external services.

This is the only module that knows about urllib and remote HTTP details.
"""

import json
import logging
import socket
import urllib.error
import urllib.parse
import urllib.request

from domain import OutgoingEvent, PaymentOutcome, PaymentResult

log = logging.getLogger(__name__)


class HttpInventoryGateway:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def reserve(self, order_id: str, items: list[dict], timeout: float = 0.5) -> None:
        body = json.dumps({
            "order_id": order_id,
            "items": [{"sku": item["sku"], "qty": item["qty"]} for item in items],
        }).encode("utf-8")
        req = urllib.request.Request(f"{self.base_url}/reservations", data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        req.add_header("Connection", "close")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status not in (200, 201):
                raise RuntimeError(f"Inventory reservation returned status {resp.status}")

    def release(self, order_id: str, timeout: float = 0.5) -> None:
        req = urllib.request.Request(
            f"{self.base_url}/reservations/{urllib.parse.quote(order_id)}", method="DELETE"
        )
        req.add_header("Connection", "close")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 204:
                raise RuntimeError(f"Inventory release returned status {resp.status}")


class HttpPaymentGateway:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def lookup(self, order_id: str, timeout: float = 0.5) -> tuple[bool, list | None]:
        url = f"{self.base_url}/payments?order_id={urllib.parse.quote(order_id)}"
        req = urllib.request.Request(url, method="GET")
        req.add_header("Connection", "close")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status == 200:
                    return True, json.loads(resp.read() or b"[]")
        except Exception:
            pass
        return False, None

    def charge(self, order_id: str, amount_cents: int, timeout: float = 0.35) -> PaymentResult:
        body = json.dumps({
            "order_id": order_id,
            "amount_cents": amount_cents,
            "currency": "RUB",
        }).encode("utf-8")
        req = urllib.request.Request(f"{self.base_url}/payments", data=body, method="POST")
        req.add_header("Content-Type", "application/json")
        # Stable across retries and restarts: one payment operation per order.
        req.add_header("Idempotency-Key", order_id)
        req.add_header("Connection", "close")

        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                if resp.status in (200, 201):
                    return PaymentResult(PaymentOutcome.PAID)
                return PaymentResult(PaymentOutcome.UNKNOWN, f"unexpected_http_{resp.status}")
        except urllib.error.HTTPError as exc:
            if exc.code == 402:
                return PaymentResult(PaymentOutcome.DECLINED, "http_402")
            if exc.code == 500:
                # Contract guarantees that HTTP 500 means money was not charged.
                return PaymentResult(PaymentOutcome.RETRYABLE_ERROR, "http_500")
            return PaymentResult(PaymentOutcome.UNKNOWN, f"http_{exc.code}")
        except (socket.timeout, TimeoutError):
            return PaymentResult(PaymentOutcome.UNKNOWN, "timeout")
        except Exception as exc:
            return PaymentResult(PaymentOutcome.UNKNOWN, type(exc).__name__)


class HttpEventsGateway:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def publish(self, event: OutgoingEvent) -> None:
        req = urllib.request.Request(
            f"{self.base_url}/events", data=json.dumps(event.payload()).encode("utf-8"),
            headers={"Content-Type": "application/json", "Connection": "close"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=0.5) as resp:
            if resp.status != 202:
                raise RuntimeError(f"Events returned status {resp.status}")
