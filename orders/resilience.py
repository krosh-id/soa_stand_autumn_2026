"""Concurrency-safe circuit breaker (the original policy is preserved)."""

import threading
import time


class CircuitBreaker:
    def __init__(self, failure_threshold: int = 3, cooldown: float = 5.0):
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self.consecutive_failures = 0
        self.opened_at = 0.0
        self.lock = threading.Lock()

    def can_attempt(self) -> bool:
        with self.lock:
            if self.consecutive_failures < self.failure_threshold:
                return True
            now = time.time()
            if now - self.opened_at >= self.cooldown:
                self.opened_at = now
                return True
            return False

    def record_success(self) -> None:
        with self.lock:
            self.consecutive_failures = 0
            self.opened_at = 0.0

    def record_failure(self) -> None:
        with self.lock:
            self.consecutive_failures += 1
            if self.consecutive_failures >= self.failure_threshold:
                self.opened_at = time.time()
