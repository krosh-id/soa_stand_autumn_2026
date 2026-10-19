"""HTTP contract smoke tests with an in-memory application service."""

from http.server import ThreadingHTTPServer
import json
import threading
import unittest
import urllib.error
import urllib.request

from bonuses import BonusService
from domain import parse_order_request
from unittest.mock import Mock
from http_api import create_handler


class FakeService:
    def __init__(self):
        self.orders = {}
        self.raise_on_create = False

    def get(self, order_id):
        return self.orders.get(order_id)

    def create(self, body):
        parse_order_request(body)
        if self.raise_on_create:
            raise RuntimeError("database cannot store the order")
        result = {"id": "order-1", "status": "pending"}
        self.orders[result["id"]] = {**result, "amount_cents": 100}
        return result


class HttpTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.service = FakeService()
        cls.bonus_repository = Mock()
        cls.bonus_repository.bonus_points.return_value = 0
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), create_handler(cls.service, BonusService(cls.bonus_repository)))
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def request(self, path, body=None):
        if body is None:
            return urllib.request.urlopen(self.url + path, timeout=2)
        data = json.dumps(body).encode("utf-8")
        request = urllib.request.Request(self.url + path, data=data, method="POST")
        return urllib.request.urlopen(request, timeout=2)

    def test_health(self):
        with self.request("/healthz") as response:
            self.assertEqual(response.status, 200)

    def test_create_and_get(self):
        with self.request("/orders", {"user_id": "user", "items": [{"sku": "a", "qty": 1, "price_cents": 100}]}) as response:
            self.assertEqual(response.status, 201)
            self.assertEqual(json.load(response)["status"], "pending")
        with self.request("/orders/order-1") as response:
            self.assertEqual(json.load(response), {"id": "order-1", "status": "pending", "amount_cents": 100})

    def test_invalid_request(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.request("/orders", {"user_id": "u", "items": []})
        self.assertEqual(ctx.exception.code, 400)
        ctx.exception.close()

    def test_unknown_order(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.request("/orders/not-found")
        self.assertEqual(ctx.exception.code, 404)
        ctx.exception.close()

    def test_storage_failure_never_returns_a_fake_201(self):
        self.service.raise_on_create = True
        try:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                self.request("/orders", {"user_id": "u", "items": [{"sku": "a", "qty": 1, "price_cents": 100}]})
            self.assertEqual(ctx.exception.code, 503)
            ctx.exception.close()
        finally:
            self.service.raise_on_create = False

    def test_bonus_for_unknown_user(self):
        with self.request("/users/u-0/bonus") as response:
            self.assertEqual(response.status, 200)
            self.assertEqual(json.load(response), {"points": 0})

    def test_delivery_and_duplicate_acknowledged(self):
        event = {"id": "d1", "type": "delivery.confirmed", "user_id": "u1", "bonus_points": 10}
        for _ in range(2):
            with self.request("/webhooks/delivery", event) as response:
                self.assertEqual(response.status, 200)

    def test_invalid_delivery(self):
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            self.request("/webhooks/delivery", {"type": "delivery.confirmed"})
        self.assertEqual(ctx.exception.code, 400)
        ctx.exception.close()

    def test_delivery_storage_error_is_not_acknowledged(self):
        self.bonus_repository.record_delivery.side_effect = RuntimeError("database unavailable")
        try:
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                self.request("/webhooks/delivery", {"id": "d2", "type": "delivery.confirmed",
                                                     "user_id": "u1", "bonus_points": 10})
            self.assertEqual(ctx.exception.code, 503)
            ctx.exception.close()
        finally:
            self.bonus_repository.record_delivery.side_effect = None
