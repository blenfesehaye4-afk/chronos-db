"""LSM-Tree storage engine coordinating MemTable, WAL, SSTables, MVCC, TTL, Cache, and Compaction."""

import os
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

from .backup import SnapshotManager
from .cache import LRUBlockCache
from .memtable import MemTable
from .mvcc import MVCCStore, Transaction
from .sstable import SSTableReader, SSTableWriter
from .structures import DataStructures
from .ttl import TTLManager
from .wal import WriteAheadLog


class ChronosEngine:
    """Log-Structured Merge (LSM) Tree Key-Value Engine with ACID durability, MVCC, TTL & LRU caching."""

    def __init__(
        self,
        data_dir: Union[str, Path],
        memtable_max_bytes: int = 1024 * 1024,  # 1 MB default
        compaction_threshold: int = 4,          # Compact when 4 SSTables accumulate
        cache_max_entries: int = 10000,
        cache_max_bytes: int = 16 * 1024 * 1024,
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
        self.mvcc = MVCCStore()
        self.cache = LRUBlockCache(max_entries=cache_max_entries, max_bytes=cache_max_bytes)
        self.ttl = TTLManager()
        self.structures = DataStructures(self)

        self._load_existing_sstables()
        self._recover_from_wal()

    def _load_existing_sstables(self) -> None:
        """Discover and load existing SSTables sorted by creation time (newest first)."""
        sst_files = sorted(self.data_dir.glob("*.sst"), key=lambda p: p.stat().st_mtime, reverse=True)
        for sst in sst_files:
            try:
                reader = SSTableReader(sst)
                self.sstables.append(reader)
            except Exception:
                pass

    def _recover_from_wal(self) -> None:
        """Replay mutations from WAL into the in-memory MemTable."""
        recovered = self.wal.recover()
        for key, val, is_tombstone in recovered:
            if is_tombstone:
                self.memtable.delete(key)
            else:
                self.memtable.put(key, val or b"")

    def begin_transaction(self) -> Transaction:
        """Create a new ACID transaction with Snapshot Isolation."""
        return self.mvcc.begin_transaction()

    def put(self, key: str, value: Union[str, bytes], ttl: Optional[float] = None) -> None:
        """Insert or update a key-value record, with optional TTL in seconds."""
        val_bytes = value.encode("utf-8") if isinstance(value, str) else value
        with self._lock:
            # 1. Update TTL
            if ttl is not None:
                self.ttl.set_expiry(key, ttl)
            else:
                self.ttl.remove_expiry(key)

            # 2. Append mutation to WAL for durability
            self.wal.append(key, val_bytes, is_tombstone=False)

            # 3. Insert into in-memory MemTable & Cache
            self.memtable.put(key, val_bytes)
            self.cache.put(key, val_bytes)

            # 4. Check flush condition
            if self.memtable.is_full():
                self._flush_memtable()

    def get(self, key: str) -> Optional[bytes]:
        """Look up a key. Returns value bytes, or None if key does not exist or expired."""
        with self._lock:
            # Check passive expiration
            if self.ttl.is_expired(key):
                self.delete(key)
                return None

            # 1. Check active in-memory MemTable first
            mem_result = self.memtable.get(key)
            if mem_result is not None:
                val, is_tombstone = mem_result
                if is_tombstone:
                    return None
                self.cache.put(key, val)
                return val

            # 2. Check LRU block cache
            cached_val = self.cache.get(key)
            if cached_val is not None:
                return cached_val

            # 3. Check SSTables in reverse chronological order (newest to oldest)
            for sst in self.sstables:
                sst_result = sst.get(key)
                if sst_result is not None:
                    val, is_tombstone = sst_result
                    if is_tombstone:
                        self.cache.delete(key)
                        return None
                    if val is not None:
                        self.cache.put(key, val)
                    return val

            return None

    def delete(self, key: str) -> bool:
        """Mark a key as deleted via tombstone record."""
        with self._lock:
            exists = self.get(key) is not None if not self.ttl.is_expired(key) else False
            self.ttl.remove_expiry(key)
            self.cache.delete(key)

            self.wal.append(key, None, is_tombstone=True)
            self.memtable.delete(key)

            if self.memtable.is_full():
                self._flush_memtable()

            return exists

    def expire(self, key: str, seconds: float) -> bool:
        """Set a timeout on key in seconds. Returns True if key exists and TTL was set."""
        with self._lock:
            if self.get(key) is None:
                return False
            self.ttl.set_expiry(key, seconds)
            return True

    def ttl_seconds(self, key: str) -> float:
        """
        Return the remaining time to live of a key that has a timeout.
        Returns -2 if key does not exist, -1 if key has no associated expire.
        """
        with self._lock:
            if self.get(key) is None:
                return -2.0
            return self.ttl.get_ttl(key)

    def scan(
        self,
        start_key: Optional[str] = None,
        end_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Tuple[str, bytes]]:
        """Range scan keys lexicographically with tombstone and TTL filtering."""
        with self._lock:
            merged: Dict[str, Tuple[Optional[bytes], bool]] = {}

            # Oldest to newest SSTables
            for sst in reversed(self.sstables):
                for k, v, is_tomb in sst.scan_all():
                    merged[k] = (v, is_tomb)

            # Overlay MemTable (newest)
            for k, v, is_tomb in self.memtable.items():
                merged[k] = (v, is_tomb)

            results = []
            for k in sorted(merged.keys()):
                if start_key and k < start_key:
                    continue
                if end_key and k > end_key:
                    break
                # Filter expired keys
                if self.ttl.is_expired(k):
                    continue

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

        SSTableWriter.write(sst_path, entries)
        new_reader = SSTableReader(sst_path)

        self.sstables.insert(0, new_reader)
        self.memtable.clear()
        self.wal.truncate()

        if len(self.sstables) >= self.compaction_threshold:
            self.compact()

        return sst_path

    def compact(self) -> Optional[Path]:
        """Merge all SSTables into a single compacted SSTable, purging tombstones and expired keys."""
        with self._lock:
            if len(self.sstables) < 2:
                return None

            merged: Dict[str, Tuple[Optional[bytes], bool]] = {}
            for sst in reversed(self.sstables):
                for k, v, is_tomb in sst.scan_all():
                    merged[k] = (v, is_tomb)

            compacted_entries = []
            for k, (v, is_tomb) in merged.items():
                if is_tomb or v is None:
                    continue
                if self.ttl.is_expired(k):
                    continue
                compacted_entries.append((k, v, is_tomb))

            timestamp_ms = int(time.time() * 1000)
            compact_path = self.data_dir / f"sstable_{timestamp_ms}_compacted.sst"
            SSTableWriter.write(compact_path, compacted_entries)
            compact_reader = SSTableReader(compact_path)

            old_sstables = list(self.sstables)
            self.sstables = [compact_reader]

            for sst in old_sstables:
                sst.close()
                try:
                    sst.filepath.unlink(missing_ok=True)
                except OSError:
                    pass

            return compact_path

    def export_snapshot(self, target_dir: Union[str, Path]) -> dict:
        """Export point-in-time consistent hot backup with manifest."""
        return SnapshotManager.export_snapshot(self, target_dir)

    def restore_snapshot(self, source_dir: Union[str, Path]) -> dict:
        """Restore database from verified snapshot."""
        return SnapshotManager.restore_snapshot(self, source_dir)

    def stats(self) -> dict:
        """Return engine operational telemetry including cache and TTL metrics."""
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
                "active_ttl_keys": self.ttl.count(),
                "cache": self.cache.stats(),
            }

    def close(self) -> None:
        """Safely close open file handles."""
        with self._lock:
            self._flush_memtable()
            self.wal.close()
            for sst in self.sstables:
                sst.close()
            self.cache.clear()
