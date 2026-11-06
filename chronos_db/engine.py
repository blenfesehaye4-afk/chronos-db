"""LSM-Tree storage engine coordinating MemTable, WAL, SSTables, and Compaction."""

import os
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from .memtable import MemTable
from .sstable import SSTableReader, SSTableWriter
from .wal import WriteAheadLog


class ChronosEngine:
    """Log-Structured Merge (LSM) Tree Key-Value Engine with ACID durability."""

    def __init__(
        self,
        data_dir: Union[str, Path],
        memtable_max_bytes: int = 1024 * 1024,  # 1 MB default
        compaction_threshold: int = 4,          # Compact when 4 SSTables accumulate
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.memtable_max_bytes = memtable_max_bytes
        self.compaction_threshold = compaction_threshold
        self._lock = threading.RLock()

        self.wal_path = self.data_dir / "chronos.wal"
        self.wal = WriteAheadLog(self.wal_path)
        self.memtable = MemTable(max_bytes=self.memtable_max_bytes)
        self.sstables: List[SSTableReader] = []

        self._load_existing_sstables()
        self._recover_from_wal()

    def _load_existing_sstables(self) -> None:
        """Discover and load existing SSTables sorted by creation time (newest first)."""
        sst_files = sorted(self.data_dir.glob("*.sst"), key=lambda p: p.stat().st_mtime, reverse=True)
        for sst in sst_files:
            try:
                reader = SSTableReader(sst)
                self.sstables.append(reader)
            except Exception as e:
                # Corrupt SSTable log/skip
                pass

    def _recover_from_wal(self) -> None:
        """Replay mutations from WAL into the in-memory MemTable."""
        recovered = self.wal.recover()
        for key, val, is_tombstone in recovered:
            if is_tombstone:
                self.memtable.delete(key)
            else:
                self.memtable.put(key, val or b"")

    def put(self, key: str, value: Union[str, bytes]) -> None:
        """Insert or update a key-value record."""
        val_bytes = value.encode("utf-8") if isinstance(value, str) else value
        with self._lock:
            # 1. Append mutation to WAL for durability
            self.wal.append(key, val_bytes, is_tombstone=False)
            # 2. Insert into in-memory MemTable
            self.memtable.put(key, val_bytes)

            # 3. Check flush condition
            if self.memtable.is_full():
                self._flush_memtable()

    def get(self, key: str) -> Optional[bytes]:
        """Look up a key. Returns value bytes, or None if key does not exist or was deleted."""
        with self._lock:
            # 1. Check active in-memory MemTable first
            mem_result = self.memtable.get(key)
            if mem_result is not None:
                val, is_tombstone = mem_result
                return None if is_tombstone else val

            # 2. Check SSTables in reverse chronological order (newest to oldest)
            for sst in self.sstables:
                sst_result = sst.get(key)
                if sst_result is not None:
                    val, is_tombstone = sst_result
                    return None if is_tombstone else val

            return None

    def delete(self, key: str) -> bool:
        """Mark a key as deleted via tombstone record."""
        with self._lock:
            # Check existence
            exists = self.get(key) is not None
            self.wal.append(key, None, is_tombstone=True)
            self.memtable.delete(key)

            if self.memtable.is_full():
                self._flush_memtable()

            return exists

    def scan(
        self,
        start_key: Optional[str] = None,
        end_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Tuple[str, bytes]]:
        """Range scan keys lexicographically with tombstone filtering."""
        with self._lock:
            # Merge records across MemTable and SSTables, giving priority to newest versions
            merged: Dict[str, Tuple[Optional[bytes], bool]] = {}

            # Oldest to newest SSTables
            for sst in reversed(self.sstables):
                for k, v, is_tomb in sst.scan_all():
                    merged[k] = (v, is_tomb)

            # Overlay MemTable (newest)
            for k, v, is_tomb in self.memtable.items():
                merged[k] = (v, is_tomb)

            # Filter range and tombstones
            results = []
            for k in sorted(merged.keys()):
                if start_key and k < start_key:
                    continue
                if end_key and k > end_key:
                    break
                val, is_tomb = merged[k]
                if not is_tomb and val is not None:
                    results.append((k, val))
                if len(results) >= limit:
                    break

            return results

    def _flush_memtable(self) -> Optional[Path]:
        """Flush current MemTable to a new immutable SSTable file on disk."""
        if len(self.memtable) == 0:
            return None

        entries = self.memtable.items()
        timestamp_ms = int(time.time() * 1000)
        sst_path = self.data_dir / f"sstable_{timestamp_ms}.sst"

        # Write SSTable
        SSTableWriter.write(sst_path, entries)
        new_reader = SSTableReader(sst_path)

        # Prepend new SSTable to list (newest first)
        self.sstables.insert(0, new_reader)

        # Clear MemTable and truncate WAL
        self.memtable.clear()
        self.wal.truncate()

        # Check compaction threshold
        if len(self.sstables) >= self.compaction_threshold:
            self.compact()

        return sst_path

    def compact(self) -> Optional[Path]:
        """Merge all SSTables into a single compacted SSTable, purging tombstones."""
        with self._lock:
            if len(self.sstables) < 2:
                return None

            merged: Dict[str, Tuple[Optional[bytes], bool]] = {}
            # Oldest to newest
            for sst in reversed(self.sstables):
                for k, v, is_tomb in sst.scan_all():
                    merged[k] = (v, is_tomb)

            # Purge tombstones during full compaction
            compacted_entries = [
                (k, v, is_tomb) for k, (v, is_tomb) in merged.items()
                if not is_tomb and v is not None
            ]

            timestamp_ms = int(time.time() * 1000)
            compact_path = self.data_dir / f"sstable_{timestamp_ms}_compacted.sst"
            SSTableWriter.write(compact_path, compacted_entries)
            compact_reader = SSTableReader(compact_path)

            # Close and remove old SSTable files
            old_sstables = list(self.sstables)
            self.sstables = [compact_reader]

            for sst in old_sstables:
                sst.close()
                try:
                    sst.filepath.unlink(missing_ok=True)
                except OSError:
                    pass

            return compact_path

    def stats(self) -> dict:
        """Return engine operational telemetry."""
        with self._lock:
            total_disk_bytes = sum(s.filepath.stat().st_size for s in self.sstables if s.filepath.exists())
            wal_size = self.wal_path.stat().st_size if self.wal_path.exists() else 0

            return {
                "memtable_entries": len(self.memtable),
                "memtable_bytes": self.memtable.approximate_size,
                "sstable_count": len(self.sstables),
                "total_sstable_disk_bytes": total_disk_bytes,
                "wal_disk_bytes": wal_size,
                "compaction_threshold": self.compaction_threshold,
            }

    def close(self) -> None:
        """Safely close open file handles."""
        with self._lock:
            self._flush_memtable()
            self.wal.close()
            for sst in self.sstables:
                sst.close()
