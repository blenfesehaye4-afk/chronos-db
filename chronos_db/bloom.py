"""Counting & probabilistic Bloom filter implementation for SSTables."""

import hashlib
import math
from typing import List


class BloomFilter:
    """Probabilistic data structure for O(1) set membership with zero false negatives."""

    def __init__(self, expected_elements: int = 10000, false_positive_rate: float = 0.01):
        if expected_elements <= 0:
            expected_elements = 100
        if false_positive_rate <= 0 or false_positive_rate >= 1:
            false_positive_rate = 0.01

        self.expected_elements = expected_elements
        self.false_positive_rate = false_positive_rate

        # Optimal bit array size m = - (n * ln(p)) / (ln(2)^2)
        self.bit_size = max(64, int(- (expected_elements * math.log(false_positive_rate)) / (math.log(2) ** 2)))
        # Optimal hash functions count k = (m / n) * ln(2)
        self.hash_count = max(1, int((self.bit_size / expected_elements) * math.log(2)))

        self.byte_size = (self.bit_size + 7) // 8
        self.bit_array = bytearray(self.byte_size)
        self.count = 0

    def _hashes(self, key: str) -> List[int]:
        """Generate k hash indexes using double hashing technique."""
        key_bytes = key.encode("utf-8")
        h1 = int.from_bytes(hashlib.sha256(key_bytes).digest()[:8], "big")
        h2 = int.from_bytes(hashlib.sha256(key_bytes + b"\x01").digest()[:8], "big")

        indexes = []
        for i in range(self.hash_count):
            combined = (h1 + i * h2) % self.bit_size
            indexes.append(combined)
        return indexes

    def add(self, key: str) -> None:
        """Insert a key into the Bloom filter."""
        for bit_idx in self._hashes(key):
            byte_idx = bit_idx // 8
            bit_pos = bit_idx % 8
            self.bit_array[byte_idx] |= (1 << bit_pos)
        self.count += 1

    def contains(self, key: str) -> bool:
        """Query if key may be in the set. False means key is guaranteed NOT in set."""
        for bit_idx in self._hashes(key):
            byte_idx = bit_idx // 8
            bit_pos = bit_idx % 8
            if not (self.bit_array[byte_idx] & (1 << bit_pos)):
                return False
        return True

    def to_bytes(self) -> bytes:
        """Serialize filter metadata and bit array."""
        meta = f"{self.expected_elements}:{self.false_positive_rate}:{self.bit_size}:{self.hash_count}:".encode("ascii")
        meta_len = len(meta).to_bytes(2, "big")
        return meta_len + meta + bytes(self.bit_array)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        """Deserialize filter from bytes."""
        meta_len = int.from_bytes(data[:2], "big")
        meta_str = data[2:2 + meta_len].decode("ascii")
        parts = meta_str.split(":")
        expected = int(parts[0])
        fp_rate = float(parts[1])
        bit_size = int(parts[2])
        hash_count = int(parts[3])

        bf = cls(expected_elements=expected, false_positive_rate=fp_rate)
        bf.bit_size = bit_size
        bf.hash_count = hash_count
        raw_bits = data[2 + meta_len:]
        bf.bit_array = bytearray(raw_bits)
        bf.byte_size = len(raw_bits)
        return bf
