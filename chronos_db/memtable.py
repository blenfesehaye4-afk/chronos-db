"""In-memory sorted buffer (MemTable) with size threshold tracking."""

from typing import Dict, List, Optional, Tuple


class MemTable:
    """Sorted in-memory buffer storing mutations before flushing to disk."""

    def __init__(self, max_bytes: int = 2 * 1024 * 1024):
        self.max_bytes = max_bytes
        self._data: Dict[str, Tuple[Optional[bytes], bool]] = {}  # key -> (value, is_tombstone)
        self.approximate_size = 0

    def put(self, key: str, value: bytes) -> None:
        """Store or update key-value pair in memory."""
        old = self._data.get(key)
        old_size = (len(key.encode("utf-8")) + (len(old[0]) if old and old[0] else 0) + 16) if old else 0
        new_size = len(key.encode("utf-8")) + len(value) + 16

        self._data[key] = (value, False)
        self.approximate_size += (new_size - old_size)

    def delete(self, key: str) -> None:
        """Mark a key as deleted using a tombstone entry."""
        old = self._data.get(key)
        old_size = (len(key.encode("utf-8")) + (len(old[0]) if old and old[0] else 0) + 16) if old else 0
        new_size = len(key.encode("utf-8")) + 16

        self._data[key] = (None, True)
        self.approximate_size += (new_size - old_size)

    def get(self, key: str) -> Optional[Tuple[Optional[bytes], bool]]:
        """Look up key in MemTable. Returns (value, is_tombstone) or None."""
        return self._data.get(key)

    def items(self) -> List[Tuple[str, Optional[bytes], bool]]:
        """Return all items sorted lexicographically by key."""
        sorted_keys = sorted(self._data.keys())
        return [(k, self._data[k][0], self._data[k][1]) for k in sorted_keys]

    def is_full(self) -> bool:
        """Check if MemTable has exceeded its size threshold."""
        return self.approximate_size >= self.max_bytes

    def __len__(self) -> int:
        return len(self._data)

    def clear(self) -> None:
        self._data.clear()
        self.approximate_size = 0
