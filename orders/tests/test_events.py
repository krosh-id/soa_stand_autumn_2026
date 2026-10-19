"""Delivery validation and retry boundaries, without network or PostgreSQL."""

import unittest
from unittest.mock import Mock, patch

from bonuses import BonusService
from domain import InvalidOrderRequest, OutgoingEvent
from gateways import HttpEventsGateway, HttpInventoryGateway
from test_gateways import Response
from worker import OutboxWorker


class EventTests(unittest.TestCase):
    def setUp(self):
        self.event = OutgoingEvent("order.paid:o1", "o1", "2026-10-19T12:00:00+00:00")
        self.repo = Mock()
        self.repo.unsent_events.return_value = [self.event]
        self.gateway = Mock()
        self.worker = OutboxWorker(self.repo, self.gateway)

    def test_publish_then_crash_before_mark_retries_identical_event(self):
        self.repo.mark_sent.side_effect = [RuntimeError("database unavailable"), None]
        self.assertFalse(self.worker.run_once())
        self.assertTrue(self.worker.run_once())
        self.assertEqual([call.args[0] for call in self.gateway.publish.call_args_list],
                         [self.event, self.event])

    def test_bus_failure_keeps_event_and_stops_batch(self):
        self.repo.unsent_events.return_value = [self.event, self.event]
        self.gateway.publish.side_effect = TimeoutError()
        self.assertFalse(self.worker.run_once())
        self.repo.mark_sent.assert_not_called()
        self.assertEqual(self.gateway.publish.call_count, 1)

    def test_gateway_requires_202(self):
        gateway = HttpEventsGateway("http://events")
        with patch("gateways.urllib.request.urlopen", return_value=Response(202)) as send:
            gateway.publish(self.event)
        self.assertEqual(send.call_args.kwargs["timeout"], 0.5)
        import json
        self.assertEqual(json.loads(send.call_args.args[0].data), self.event.payload())
        with patch("gateways.urllib.request.urlopen", return_value=Response(200)):
            with self.assertRaises(RuntimeError):
                gateway.publish(self.event)

    def test_inventory_release_does_not_hide_failure(self):
        with patch("gateways.urllib.request.urlopen", side_effect=TimeoutError()):
            with self.assertRaises(TimeoutError):
                HttpInventoryGateway("http://inventory").release("o1")

    def test_invalid_delivery_never_reaches_storage(self):
        service = BonusService(self.repo)
        valid = {"id": "d1", "type": "delivery.confirmed", "user_id": "u1", "bonus_points": 10}
        for body in (None, {}, {**valid, "type": "order.paid"}, {**valid, "id": ""},
                     {**valid, "user_id": ""}, {**valid, "bonus_points": True},
                     {**valid, "bonus_points": 0}):
            with self.subTest(body=body), self.assertRaises(InvalidOrderRequest):
                service.receive(body)
        self.repo.record_delivery.assert_not_called()
        service.receive(valid)
        self.assertEqual(self.repo.record_delivery.call_args.args[0].bonus_points, 10)
