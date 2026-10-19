"""Composition root: assemble adapters and start the single Orders service."""

from http.server import ThreadingHTTPServer
import logging
import threading

from config import Settings
from gateways import HttpEventsGateway, HttpInventoryGateway, HttpPaymentGateway
from http_api import create_handler
from repository import ConnectionPool, PostgresOrderRepository, initialize_schema
from resilience import CircuitBreaker
from service import OrderService
from worker import OutboxWorker, PendingOrdersWorker
from bonuses import BonusService

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orders")


def main() -> None:
    settings = Settings.from_env()
    initialize_schema(settings.database_url)

    repository = PostgresOrderRepository(ConnectionPool(settings.database_url))
    inventory = HttpInventoryGateway(settings.inventory_url)
    payment = HttpPaymentGateway(settings.payment_url)
    breaker = CircuitBreaker(failure_threshold=3, cooldown=5.0)
    orders = OrderService(repository, inventory, payment, breaker)
    worker = PendingOrdersWorker(repository, orders)

    threading.Thread(target=worker.run_forever, name="payment-worker", daemon=True).start()
    outbox = OutboxWorker(repository, HttpEventsGateway(settings.events_url))
    threading.Thread(target=outbox.run_forever, name="outbox-worker", daemon=True).start()
    server = ThreadingHTTPServer(("0.0.0.0", settings.port), create_handler(orders, BonusService(repository)))
    server.daemon_threads = True
    log.info("orders service running on port %d", settings.port)
    server.serve_forever()


if __name__ == "__main__":
    main()
