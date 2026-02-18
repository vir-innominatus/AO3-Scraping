from __future__ import annotations

import random
import time
from dataclasses import dataclass


@dataclass(slots=True)
class DelayPolicy:
    base_seconds: float = 3.0
    jitter_seconds: float = 0.8


class RequestThrottler:
    def __init__(self, policy: DelayPolicy) -> None:
        self.policy = policy
        self._next_allowed_at = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if now < self._next_allowed_at:
            time.sleep(self._next_allowed_at - now)

    def mark_request(self) -> float:
        delay = self.policy.base_seconds + random.uniform(0.0, self.policy.jitter_seconds)
        self._next_allowed_at = time.monotonic() + delay
        return delay
