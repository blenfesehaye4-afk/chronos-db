"""Immutable Sorted String Table (SSTable) file writer and reader."""

import bisect
import struct
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from .bloom import BloomFilter

# SSTable Binary Layout:
# -------------------------------------------------------------
# [Data Block]
#   Sequence of entries: [flag: 1B] [klen: 4B] [vlen: 4B] [key] [val]
# [Sparse Index Block]
#   Sequence of index entries: [klen: 4B] [key] [offset: 8B]
# [Bloom Filter Block]
#   Raw serialized bloom filter bytes
# [Footer] (44 bytes fixed at EOF)
#   [index_offset: 8B] [index_len: 8B] [bloom_offset: 8B] [bloom_len: 8B] [count: 8B] [magic: 4B]
FOOTER_STRUCT = struct.Struct(">QQQQQI")
FOOTER_SIZE = FOOTER_STRUCT.size
MAGIC = 0x4348524F  # "CHRO" in ASCII hex


class SSTableWriter:
    """Constructs an immutable sorted string table with sparse indexing and bloom filter."""

    @classmethod
    def write(
        cls,
        filepath: Path,
        entries: List[Tuple[str, Optional[bytes], bool]],
        index_interval: int = 16,
    ) -> Path:
        filepath.parent.mkdir(parents=True, exist_ok=True)
        sorted_entries = sorted(entries, key=lambda x: x[0])

        bloom = BloomFilter(expected_elements=max(10, len(sorted_entries)), false_positive_rate=0.01)
        sparse_index: List[Tuple[str, int]] = []

        with open(filepath, "wb") as f:
            # 1. Write Data Block
            for i, (key, value, is_tombstone) in enumerate(sorted_entries):
                offset = f.tell()
                bloom.add(key)
                if i % index_interval == 0:
                    sparse_index.append((key, offset))

                key_bytes = key.encode("utf-8")
                val_bytes = value if (value is not None and not is_tombstone) else b""
                flag = 1 if is_tombstone else 0

                entry_hdr = struct.pack(">BII", flag, len(key_bytes), len(val_bytes))
                f.write(entry_hdr + key_bytes + val_bytes)

            data_end = f.tell()

            # 2. Write Sparse Index Block
            index_offset = data_end
            for idx_key, idx_off in sparse_index:
                idx_key_bytes = idx_key.encode("utf-8")
                f.write(struct.pack(">IQ", len(idx_key_bytes), idx_off) + idx_key_bytes)
            index_len = f.tell() - index_offset

            # 3. Write Bloom Filter Block
            bloom_offset = f.tell()
            bloom_bytes = bloom.to_bytes()
            f.write(bloom_bytes)
            bloom_len = len(bloom_bytes)

            # 4. Write Fixed Footer
            footer_data = FOOTER_STRUCT.pack(
                index_offset,
                index_len,
                bloom_offset,
                bloom_len,
                len(sorted_entries),
                MAGIC,
            )
            f.write(footer_data)

        return filepath


class SSTableReader:
    """Fast indexed reader with bloom filter negative skipping and sparse index search."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self._file = open(self.filepath, "rb")
        self.entry_count = 0
        self.sparse_keys: List[str] = []
        self.sparse_offsets: List[int] = []
        self.bloom: Optional[BloomFilter] = None
        self.data_end = 0

        self._load_metadata()

    def _load_metadata(self) -> None:
        self._file.seek(-FOOTER_SIZE, 2)
        footer_bytes = self._file.read(FOOTER_SIZE)
        index_offset, index_len, bloom_offset, bloom_len, count, magic = FOOTER_STRUCT.unpack(footer_bytes)

        if magic != MAGIC:
            raise ValueError(f"Corrupted SSTable file: {self.filepath} (invalid magic)")

        self.entry_count = count
        self.data_end = index_offset

        # Load Bloom Filter
        self._file.seek(bloom_offset)
        bloom_data = self._file.read(bloom_len)
        self.bloom = BloomFilter.from_bytes(bloom_data)

        # Load Sparse Index
        self._file.seek(index_offset)
        idx_data = self._file.read(index_len)
        pos = 0
        while pos < index_len:
            klen, off = struct.unpack_from(">IQ", idx_data, pos)
            pos += 12
            key = idx_data[pos:pos + klen].decode("utf-8")
            pos += klen
            self.sparse_keys.append(key)
            self.sparse_offsets.append(off)

    def get(self, key: str) -> Optional[Tuple[Optional[bytes], bool]]:
        """Look up key in SSTable. Returns (value, is_tombstone) or None if absent."""
        # 1. Bloom filter check for O(1) negative skipping
        if self.bloom and not self.bloom.contains(key):
            return None

        # 2. Binary search in sparse index
        if not self.sparse_keys:
            return None

        idx = bisect.bisect_right(self.sparse_keys, key) - 1
        start_offset = self.sparse_offsets[idx] if idx >= 0 else 0
        end_offset = self.sparse_offsets[idx + 1] if idx + 1 < len(self.sparse_offsets) else self.data_end

        # 3. Linear scan within the bounded interval
        self._file.seek(start_offset)
        while self._file.tell() < end_offset:
            hdr = self._file.read(9)
            if len(hdr) < 9:
                break
            flag, klen, vlen = struct.unpack(">BII", hdr)
            curr_key = self._file.read(klen).decode("utf-8")
            val_bytes = self._file.read(vlen)

            if curr_key == key:
                is_tombstone = (flag == 1)
                return (None if is_tombstone else val_bytes, is_tombstone)
            elif curr_key > key:
                break  # Keys are ordered, passed it

        return None

    def scan_all(self) -> List[Tuple[str, Optional[bytes], bool]]:
        """Scan all entries in this SSTable."""
        entries: List[Tuple[str, Optional[bytes], bool]] = []
        self._file.seek(0)
        while self._file.tell() < self.data_end:
            hdr = self._file.read(9)
            if len(hdr) < 9:
                break
            flag, klen, vlen = struct.unpack(">BII", hdr)
            curr_key = self._file.read(klen).decode("utf-8")
            val_bytes = self._file.read(vlen)
            is_tombstone = (flag == 1)
            entries.append((curr_key, None if is_tombstone else val_bytes, is_tombstone))
        return entries

    def close(self) -> None:
        if not self._file.closed:
            self._file.close()
