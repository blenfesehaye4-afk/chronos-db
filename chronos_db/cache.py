"""High-performance thread-safe LRU Block Cache for SSTable data and hot keys."""

import threading
from collections import OrderedDict
from typing import Dict, Optional, Tuple


class LRUBlockCache:
    """Thread-safe LRU cache with entry capacity and approximate memory tracking."""

    def __init__(self, max_entries: int = 10000, max_bytes: int = 16 * 1024 * 1024):
        self.max_entries = max_entries
        self.max_bytes = max_bytes
        self._lock = threading.RLock()
        self._cache: OrderedDict[str, bytes] = OrderedDict()
        self._current_bytes = 0
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    def get(self, key: str) -> Optional[bytes]:
        """Look up key in cache, updating LRU recency on hit."""
        with self._lock:
            if key in self._cache:
                self._cache.move_to_end(key)
                self.hits += 1
                return self._cache[key]
            self.misses += 1
            return None

    def put(self, key: str, value: bytes) -> None:
        """Insert or update entry in cache, evicting oldest if capacity reached."""
        val_bytes = bytes(value)
        entry_size = len(key.encode("utf-8")) + len(val_bytes) + 32

        with self._lock:
            if key in self._cache:
                old_val = self._cache.pop(key)
                self._current_bytes -= (len(key.encode("utf-8")) + len(old_val) + 32)

            self._cache[key] = val_bytes
            self._current_bytes += entry_size

            # Evict if exceeding capacity or memory budget
            while (
                len(self._cache) > self.max_entries
                or (self._current_bytes > self.max_bytes and len(self._cache) > 1)
            ):
                evicted_key, evicted_val = self._cache.popitem(last=False)
                self._current_bytes -= (len(evicted_key.encode("utf-8")) + len(evicted_val) + 32)
                self.evictions += 1

    def delete(self, key: str) -> None:
        """Remove key from cache if present."""
        with self._lock:
            if key in self._cache:
                old_val = self._cache.pop(key)
                self._current_bytes -= (len(key.encode("utf-8")) + len(old_val) + 32)

    def clear(self) -> None:
        """Clear all entries and reset counters."""
        with self._lock:
            self._cache.clear()
            self._current_bytes = 0

    def stats(self) -> dict:
        """Return cache performance statistics."""
        with self._lock:
            total_requests = self.hits + self.misses
            hit_ratio = (self.hits / total_requests) if total_requests > 0 else 0.0
            return {
                "cached_entries": len(self._cache),
                "cached_bytes": self._current_bytes,
                "hits": self.hits,
                "misses": self.misses,
                "evictions": self.evictions,
                "hit_ratio": round(hit_ratio, 4),
            }

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)
