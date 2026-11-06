"""Multi-Version Concurrency Control (MVCC) and Snapshot Isolation for ChronosDB."""

import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class VersionedRecord:
    value: Optional[bytes]  # None indicates tombstone
    created_txn_id: int
    expired_txn_id: Optional[int] = None  # Replaced or deleted by this txn_id


class TransactionConflictError(Exception):
    """Raised when an optimistic concurrency write conflict occurs."""
    pass


class Transaction:
    """Atomic transaction supporting Snapshot Isolation and rollback."""

    def __init__(self, txn_id: int, store: "MVCCStore"):
        self.txn_id = txn_id
        self.store = store
        self.read_txn_id = txn_id
        self.writes: Dict[str, Optional[bytes]] = {}  # key -> value (None = delete)
        self.reads: Set[str] = set()
        self.is_committed = False
        self.is_aborted = False

    def get(self, key: str) -> Optional[bytes]:
        """Read key with snapshot isolation visibility."""
        if self.is_committed or self.is_aborted:
            raise RuntimeError("Transaction is closed")

        self.reads.add(key)

        # Check local uncommitted writes first
        if key in self.writes:
            return self.writes[key]

        # Read visible version from MVCC store
        return self.store.get_visible(key, self.read_txn_id)

    def put(self, key: str, value: bytes) -> None:
        """Stage write mutation."""
        if self.is_committed or self.is_aborted:
            raise RuntimeError("Transaction is closed")
        self.writes[key] = value

    def delete(self, key: str) -> None:
        """Stage deletion (tombstone)."""
        if self.is_committed or self.is_aborted:
            raise RuntimeError("Transaction is closed")
        self.writes[key] = None

    def commit(self) -> None:
        """Commit staged mutations atomically with write-conflict detection."""
        if self.is_committed or self.is_aborted:
            raise RuntimeError("Transaction is closed")
        self.store.commit_transaction(self)
        self.is_committed = True

    def rollback(self) -> None:
        """Discard staged mutations."""
        self.writes.clear()
        self.is_aborted = True


class MVCCStore:
    """In-memory multi-version concurrency control coordinator."""

    def __init__(self):
        self._lock = threading.RLock()
        self._next_txn_id = 1
        # key -> list of VersionedRecord ordered by created_txn_id descending
        self.versions: Dict[str, List[VersionedRecord]] = {}

    def begin_transaction(self) -> Transaction:
        with self._lock:
            txn_id = self._next_txn_id
            self._next_txn_id += 1
            return Transaction(txn_id=txn_id, store=self)

    def get_visible(self, key: str, read_txn_id: int) -> Optional[bytes]:
        with self._lock:
            records = self.versions.get(key, [])
            for rec in records:
                # Record visible if created <= read_txn_id and not expired or expired after read_txn_id
                if rec.created_txn_id <= read_txn_id:
                    if rec.expired_txn_id is None or rec.expired_txn_id > read_txn_id:
                        return rec.value
            return None

    def commit_transaction(self, txn: Transaction) -> None:
        with self._lock:
            commit_txn_id = self._next_txn_id
            self._next_txn_id += 1

            # 1. Conflict detection: First-Committer-Wins
            for key in txn.writes:
                records = self.versions.get(key, [])
                for rec in records:
                    # If another transaction committed a write to this key after our read snapshot
                    if rec.created_txn_id > txn.read_txn_id:
                        raise TransactionConflictError(
                            f"Write conflict on key '{key}': modified by txn {rec.created_txn_id} after snapshot {txn.read_txn_id}"
                        )

            # 2. Apply writes
            for key, val in txn.writes.items():
                if key not in self.versions:
                    self.versions[key] = []

                # Expire previous latest version
                if self.versions[key]:
                    self.versions[key][0].expired_txn_id = commit_txn_id

                new_rec = VersionedRecord(value=val, created_txn_id=commit_txn_id)
                self.versions[key].insert(0, new_rec)

    def vacuum(self, oldest_active_txn_id: int) -> int:
        """Purge obsolete record versions older than the oldest active reader."""
        purged = 0
        with self._lock:
            for key in list(self.versions.keys()):
                kept = []
                for rec in self.versions[key]:
                    if rec.expired_txn_id is not None and rec.expired_txn_id <= oldest_active_txn_id:
                        purged += 1
                    else:
                        kept.append(rec)
                if not kept:
                    del self.versions[key]
                else:
                    self.versions[key] = kept
        return purged
