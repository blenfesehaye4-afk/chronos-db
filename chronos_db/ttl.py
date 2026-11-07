"""Time-To-Live (TTL) & Auto-Expiration Subsystem for ChronosDB."""

import random
import threading
import time
from typing import Dict, List, Optional, Tuple


class TTLManager:
    """Thread-safe TTL manager handling passive and active key expirations."""

    def __init__(self):
        self._lock = threading.RLock()
        # key -> expiration unix epoch timestamp in seconds (float)
        self._expiries: Dict[str, float] = {}

    def set_expiry(self, key: str, ttl_seconds: float) -> None:
        """Set a key to expire after ttl_seconds from now."""
        with self._lock:
            if ttl_seconds <= 0:
                self._expiries[key] = time.time() - 1.0  # Already expired
            else:
                self._expiries[key] = time.time() + ttl_seconds

    def set_deadline(self, key: str, deadline_timestamp: float) -> None:
        """Set a key to expire at a specific unix timestamp."""
        with self._lock:
            self._expiries[key] = deadline_timestamp

    def get_ttl(self, key: str) -> Optional[float]:
        """
        Get remaining time to live in seconds.
        Returns:
            - Float > 0: seconds remaining until expiry.
            - -2: key does not exist or has already expired.
            - -1: key has no associated expiration.
        """
        with self._lock:
            if key not in self._expiries:
                return -1.0

            remaining = self._expiries[key] - time.time()
            if remaining <= 0:
                # Expired
                del self._expiries[key]
                return -2.0
            return remaining

    def is_expired(self, key: str) -> bool:
        """Check if a key has expired. Removes key if expired."""
        with self._lock:
            if key not in self._expiries:
                return False

            if time.time() >= self._expiries[key]:
                del self._expiries[key]
                return True
            return False

    def remove_expiry(self, key: str) -> None:
        """Remove expiration deadline for a key (e.g. on overwrite or delete)."""
        with self._lock:
            self._expiries.pop(key, None)

    def active_expire_sample(self, max_sample: int = 50) -> List[str]:
        """
        Sample a subset of keys and return the ones that have expired,
        simulating Redis-style probabilistic active expiration.
        """
        with self._lock:
            if not self._expiries:
                return []

            keys = list(self._expiries.keys())
            sample_keys = random.sample(keys, min(len(keys), max_sample))
            now = time.time()
            expired = []

            for k in sample_keys:
                if now >= self._expiries[k]:
                    expired.append(k)
                    del self._expiries[k]

            return expired

    def count(self) -> int:
        """Return the number of keys tracked with a TTL."""
        with self._lock:
            return len(self._expiries)

    def clear(self) -> None:
        """Clear all registered expirations."""
        with self._lock:
            self._expiries.clear()
