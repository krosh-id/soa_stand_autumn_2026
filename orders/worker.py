"""Background scheduling only; business decisions stay in OrderService."""

import logging
import time

from ports import EventRepository, EventsGateway, OrderRepository
from service import OrderService

log = logging.getLogger(__name__)


class PendingOrdersWorker:
    def __init__(self, repository: OrderRepository, order_service: OrderService):
        self.repository = repository
        self.order_service = order_service

    def run_once(self) -> None:
        for order in self.repository.pending(limit=100):
            self.order_service.process_pending(order)

    def run_forever(self) -> None:
        while True:
            try:
                time.sleep(0.2)
                self.run_once()
            except Exception:
                log.exception("Error in background worker")
                time.sleep(0.5)


class OutboxWorker:
    """Publication is independent of HTTP requests and payment recovery."""

    def __init__(self, repository: EventRepository, gateway: EventsGateway):
        self.repository = repository
        self.gateway = gateway

    def run_once(self) -> bool:
        for event in self.repository.unsent_events(limit=100):
            try:
                self.gateway.publish(event)
                self.repository.mark_sent(event.id)
            except Exception as exc:
                log.warning("Event %s will be retried: %s", event.id, exc)
                return False
        return True

    def run_forever(self) -> None:
        while True:
            try:
                success = self.run_once()
            except Exception:
                log.exception("Could not read outbox")
                success = False
            time.sleep(0.2 if success else 1.0)
