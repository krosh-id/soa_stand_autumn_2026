"""Payment/Inventory outbound contract checks with fake HTTP responses."""

import json
import socket
import unittest
import urllib.error
from unittest.mock import patch

from domain import PaymentOutcome
from gateways import HttpPaymentGateway, HttpInventoryGateway


class Response:
    def __init__(self, status=201, payload=b"[]"):
        self.status = status
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.payload


class GatewayTests(unittest.TestCase):
    def test_payment_key_is_stable_and_body_correct(self):
        payment = HttpPaymentGateway("http://payment")
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            return Response(status=201)

        with patch("gateways.urllib.request.urlopen", side_effect=fake_urlopen):
            self.assertEqual(payment.charge("order-123", 99800).outcome, PaymentOutcome.PAID)
            self.assertEqual(payment.charge("order-123", 99800).outcome, PaymentOutcome.PAID)
        self.assertEqual([req.get_header("Idempotency-key") for req, _ in requests], ["order-123", "order-123"])
        self.assertEqual(json.loads(requests[0][0].data), {"order_id": "order-123", "amount_cents": 99800, "currency": "RUB"})

    def test_payment_402_and_500_are_distinct(self):
        payment = HttpPaymentGateway("http://payment")
        for code, outcome in [(402, PaymentOutcome.DECLINED), (500, PaymentOutcome.RETRYABLE_ERROR)]:
            error = urllib.error.HTTPError("http://payment/payments", code, "error", {}, None)
            with patch("gateways.urllib.request.urlopen", side_effect=error):
                self.assertEqual(payment.charge("order-1", 100).outcome, outcome)

    def test_timeout_means_unknown_not_declined(self):
        payment = HttpPaymentGateway("http://payment")
        with patch("gateways.urllib.request.urlopen", side_effect=socket.timeout()):
            self.assertEqual(payment.charge("order-1", 100).outcome, PaymentOutcome.UNKNOWN)

    def test_payment_lookup(self):
        payment = HttpPaymentGateway("http://payment")
        with patch("gateways.urllib.request.urlopen", return_value=Response(200, b'[{"charge_id":"c-1"}]')):
            ok, charges = payment.lookup("order-1")
        self.assertTrue(ok)
        self.assertEqual(charges[0]["charge_id"], "c-1")

    def test_inventory_reserve_uses_sku_qty_only(self):
        inventory = HttpInventoryGateway("http://inventory")
        requests = []

        def fake_urlopen(req, timeout):
            requests.append(req)
            return Response(201)

        with patch("gateways.urllib.request.urlopen", side_effect=fake_urlopen):
            inventory.reserve("order-123", [{"sku": "a", "qty": 2, "price_cents": 100}])
        self.assertEqual(json.loads(requests[0].data), {"order_id": "order-123", "items": [{"sku": "a", "qty": 2}]})


if __name__ == "__main__":
    unittest.main()
