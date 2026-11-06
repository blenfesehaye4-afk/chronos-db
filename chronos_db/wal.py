"""Write-Ahead Log (WAL) with CRC32 integrity checks for crash durability."""

import os
import struct
import zlib
from pathlib import Path
from typing import List, Optional, Tuple


# Record format:
# [total_len: 4B] [crc32: 4B] [flag: 1B] [klen: 4B] [vlen: 4B] [key] [val]
# Flag: 0 = PUT, 1 = DELETE (tombstone)
HEADER_STRUCT = struct.Struct(">IIBII")


class WriteAheadLog:
    """Append-only write-ahead log ensuring durable mutations before memory storage."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.filepath, "a+b")

    def append(self, key: str, value: Optional[bytes], is_tombstone: bool = False) -> int:
        """Append a mutation entry with CRC32 verification to the log."""
        key_bytes = key.encode("utf-8")
        val_bytes = value if value is not None and not is_tombstone else b""

        klen = len(key_bytes)
        vlen = len(val_bytes)
        flag = 1 if is_tombstone else 0

        # Compute payload and checksum
        payload_data = struct.pack(">BII", flag, klen, vlen) + key_bytes + val_bytes
        crc = zlib.crc32(payload_data) & 0xFFFFFFFF
        total_len = len(payload_data)

        record = struct.pack(">II", total_len, crc) + payload_data
        offset = self._file.tell()
        self._file.write(record)
        self._file.flush()
        os.fsync(self._file.fileno())
        return offset

    def recover(self) -> List[Tuple[str, Optional[bytes], bool]]:
        """Recover records sequentially from disk, skipping any torn/corrupted writes."""
        entries: List[Tuple[str, Optional[bytes], bool]] = []
        if not self.filepath.exists():
            return entries

        file_size = self.filepath.stat().st_size
        if file_size == 0:
            return entries

        with open(self.filepath, "rb") as f:
            while True:
                prefix = f.read(8)
                if len(prefix) < 8:
                    break
                total_len, expected_crc = struct.unpack(">II", prefix)

                payload = f.read(total_len)
                if len(payload) < total_len:
                    # Torn write encountered at the end of log
                    break

                actual_crc = zlib.crc32(payload) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    # Checksum mismatch, halt recovery
                    break

                flag, klen, vlen = struct.unpack(">BII", payload[:9])
                key = payload[9:9 + klen].decode("utf-8")
                val_data = payload[9 + klen:9 + klen + vlen]

                is_tombstone = (flag == 1)
                value = None if is_tombstone else val_data
                entries.append((key, value, is_tombstone))

        return entries

    def truncate(self) -> None:
        """Clear the WAL after MemTable successfully flushes to an SSTable."""
        self._file.close()
        with open(self.filepath, "wb") as f:
            f.truncate(0)
        self._file = open(self.filepath, "a+b")

    def close(self) -> None:
        if not self._file.closed:
            self._file.flush()
            self._file.close()
