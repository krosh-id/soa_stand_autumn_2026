"""Delivery events credit the buyer once, independently of Orders records."""

from domain import parse_delivery
from ports import BonusRepository


class BonusService:
    def __init__(self, repository: BonusRepository):
        self.repository = repository

    def receive(self, body: object) -> None:
        self.repository.record_delivery(parse_delivery(body))

    def balance(self, user_id: str) -> dict:
        return {"points": self.repository.bonus_points(user_id)}
