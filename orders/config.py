"""Process configuration. Environment variables are read only at startup."""

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class Settings:
    port: int
    database_url: str
    payment_url: str
    events_url: str
    inventory_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            port=int(os.environ.get("PORT", "8080")),
            database_url=os.environ["DATABASE_URL"],
            payment_url=os.environ["PAYMENT_URL"].rstrip("/"),
            events_url=os.environ["EVENTS_URL"].rstrip("/"),
            inventory_url=os.environ["INVENTORY_URL"].rstrip("/"),
        )
