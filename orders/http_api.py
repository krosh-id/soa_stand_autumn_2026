"""HTTP transport adapter: routing, JSON serialization and status mapping only."""

from http.server import BaseHTTPRequestHandler
import json
import logging
import re
import urllib.parse

from domain import InvalidOrderRequest
from service import OrderService
from bonuses import BonusService

log = logging.getLogger(__name__)
BONUS_PATH = re.compile(r"^/users/([^/]+)/bonus$")
ORDER_PATH = re.compile(r"^/orders/([^/]+)$")


def create_handler(order_service: OrderService, bonus_service: BonusService | None = None) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def send_json(self, code: int, obj: dict) -> None:
            data = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def read_json_body(self) -> object:
            length = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(length) or b"null")

        def do_GET(self) -> None:
            path = self.path.split("?")[0]
            if path == "/healthz":
                return self.send_json(200, {"status": "ok"})

            bonus_match = BONUS_PATH.match(path)
            if bonus_match and bonus_service is not None:
                try:
                    return self.send_json(200, bonus_service.balance(urllib.parse.unquote(bonus_match.group(1))))
                except Exception:
                    log.exception("Could not read bonus balance")
                    return self.send_json(503, {"error": "service_unavailable"})

            match = ORDER_PATH.match(path)
            if not match:
                return self.send_json(404, {"error": "not_found"})

            order_id = urllib.parse.unquote(match.group(1))
            order = order_service.get(order_id)
            if order is None:
                return self.send_json(404, {"error": "not_found"})
            return self.send_json(200, order)

        def do_POST(self) -> None:
            path = self.path.split("?")[0]
            if path not in ("/orders", "/webhooks/delivery"):
                return self.send_json(404, {"error": "not_found"})

            try:
                body = self.read_json_body()
            except Exception:
                return self.send_json(400, {"error": "bad_request"})

            try:
                if path == "/webhooks/delivery":
                    if bonus_service is None:
                        return self.send_json(404, {"error": "not_found"})
                    bonus_service.receive(body)
                    return self.send_json(200, {"status": "ok"})
                result = order_service.create(body)
                return self.send_json(201, result)
            except InvalidOrderRequest:
                return self.send_json(400, {"error": "bad_request"})
            except Exception:
                log.exception("Could not persist request")
                return self.send_json(503, {"error": "service_unavailable"})

        def log_message(self, fmt: str, *args) -> None:
            pass

    return Handler
