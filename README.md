# 🗄️ ChronosDB

[![CI](https://github.com/blenfesehaye4-afk/chronos-db/actions/workflows/ci.yml/badge.svg)](https://github.com/blenfesehaye4-afk/chronos-db/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **High-performance Log-Structured Merge (LSM) Tree Key-Value Engine, MVCC ACID Transactions, TTL Expiration, LRU Cache & Redis Wire Protocol in Python.**

ChronosDB is an embeddable, append-only LSM-Tree storage engine featuring Write-Ahead Logging (WAL) with CRC32 checksums, Multi-Version Concurrency Control (MVCC) with Snapshot Isolation, memory-efficient sparse indexed SSTables, probabilistic Bloom Filters, size-tiered compaction, time-to-live (TTL) expiration, an in-memory LRU block cache, point-in-time snapshot backup & restore, and a Redis-compatible (RESP) asynchronous TCP server supporting Hashes, Lists, and Sets.

---

## 🌟 What's New in v0.3.0

- ⏱️ **Time-To-Live (TTL) & Auto-Expiration** ([`chronos_db/ttl.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/chronos-db/chronos_db/ttl.py)):
  - Millisecond and second-precision expiration timeouts via `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, and `SETEX`.
  - Both passive on-read expiration and active background sampling eviction.
- ⚡ **High-Performance LRU Block Cache** ([`chronos_db/cache.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/chronos-db/chronos_db/cache.py)):
  - Thread-safe in-memory LRU cache with configurable entry count and byte memory budgets.
  - Accelerates hot reads, drastically reducing disk lookups with full hit/miss/eviction telemetry.
- 📦 **Redis Data Structures (Hashes, Lists, Sets)** ([`chronos_db/structures.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/chronos-db/chronos_db/structures.py)):
  - **Hashes**: `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`.
  - **Lists**: `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`.
  - **Sets**: `SADD`, `SMEMBERS`, `SREM`, `SISMEMBER`, `SCARD`.
- 💾 **Point-In-Time Hot Backup & Restore** ([`chronos_db/backup.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/chronos-db/chronos_db/backup.py)):
  - Consistent atomic backup export via `SNAPSHOT` / `BGSAVE` generating cryptographic SHA-256 manifest files.
  - Safe, verified snapshot restoration with automatic database integrity checks.

---

## 🏗️ Architecture

```
[ Client Writes (PUT / DEL / MULTI / HSET / LPUSH) ]
                 │
                 ├── 1. Append with CRC32 Checksum
                 ▼
       ┌──────────────────────┐
       │ Write-Ahead Log (WAL)│ ➔ Durable crash-safe append log
       └──────────────────────┘
                 │
                 ├── 2. In-Memory MemTable, LRU Cache & TTL Registry
                 ▼
       ┌──────────────────────┐   ┌─────────────────┐   ┌─────────────────┐
       │  MemTable & MVCC     │   │ LRU Block Cache │   │  TTL Registry   │
       │  (RAM Sorted Buffer) │   │ (Hot Key Accel) │   │ (Timeouts/Expir)│
       └──────────┬───────────┘   └─────────────────┘   └─────────────────┘
                  │ (Threshold reached: Flush to Disk)
                  ▼
       ┌────────────────────────────────────────────────────────┐
       │ Immutable SSTable (Disk)                               │
       │ ┌────────────────┬─────────────────┬─────────────────┐ │
       │ │  Data Block    │  Sparse Index   │  Bloom Filter   │ │
       │ │  (Sorted Key)  │  (Jump Pointers)│  (O(1) Negatives│ │
       │ └────────────────┴─────────────────┴─────────────────┘ │
       └──────────────────────────┬─────────────────────────────┘
                                  │
                        [ Compaction Engine ]
                                  ▼
                    Merge SSTables, Purge Tombstones & Expired Keys
```

---

## 💻 Python Quickstart

### 1. Basic Key-Value, TTL Expiration & LRU Caching

```python
import time
from chronos_db import ChronosEngine

db = ChronosEngine(data_dir="./chronos_data")

# 1. Set key with 2.5s TTL
db.put("session:token", "jwt_secret_val", ttl=2.5)
print("Initial value:", db.get("session:token").decode())
print("Remaining TTL:", db.ttl_seconds("session:token"))

# 2. Wait for auto-expiration
time.sleep(2.6)
print("After expiry:", db.get("session:token"))  # None (expired!)

# 3. Inspect LRU Cache statistics
print("Cache stats:", db.stats()["cache"])
db.close()
```

### 2. Redis Data Structures (Hashes, Lists, Sets)

```python
from chronos_db import ChronosEngine

db = ChronosEngine(data_dir="./chronos_data")
s = db.structures

# Hashes
s.hset("user:101", "name", "Blen")
s.hset("user:101", "role", "Lead Architect")
print("User profile:", s.hgetall("user:101"))

# Lists (FIFO / LIFO Queue)
s.rpush("task_queue", "build_indexer", "run_compaction", "verify_checksums")
print("Pending tasks:", s.lrange("task_queue", 0, -1))
print("Next task:", s.lpop("task_queue"))

# Sets (Unique Tags)
s.sadd("article:tags", "database", "python", "lsm_tree", "storage")
print("Tags:", s.smembers("article:tags"))
print("Has 'database' tag?:", s.sismember("article:tags", "database"))

db.close()
```

### 3. ACID Transactions with Snapshot Isolation

```python
from chronos_db import ChronosEngine, TransactionConflictError

db = ChronosEngine(data_dir="./chronos_data")
db.put("balance:alice", "100")
db.put("balance:bob", "50")

txn = db.begin_transaction()
try:
    alice_bal = int(txn.get("balance:alice").decode())
    bob_bal = int(txn.get("balance:bob").decode())

    txn.put("balance:alice", str(alice_bal - 25).encode())
    txn.put("balance:bob", str(bob_bal + 25).encode())
    txn.commit()
    print("Atomic transfer complete!")
except TransactionConflictError:
    txn.rollback()
    print("Write conflict detected; transaction aborted.")

db.close()
```

### 4. Hot Backup & Point-in-time Restore

```python
from chronos_db import ChronosEngine

db = ChronosEngine(data_dir="./chronos_data")

# Export point-in-time snapshot with SHA256 manifest
manifest = db.export_snapshot("./backup_2026")
print(f"Backed up {manifest['sstable_count']} SSTables.")

# Restore database from snapshot
db.restore_snapshot("./backup_2026")
print("Restored database to verified snapshot state.")

db.close()
```

---

## 🖥️ Command-Line Interface (CLI)

```bash
# Launch RESP TCP server (compatible with redis-cli)
chronos-db server --host 127.0.0.1 --port 6380 --dir ./chronos_data

# Run interactive showcase of v0.3.0 features
chronos-db demo

# Run write & read throughput benchmark
chronos-db bench --ops 10000 --dir ./bench_data

# Connect interactive terminal client
chronos-db repl --host 127.0.0.1 --port 6380
```

---

## 🧪 Running Tests

```bash
pytest -v
```

All 23 unit and integration tests validate the full storage engine lifecycle including WAL durability, CRC32 verification, SSTable sparse indexing, Bloom filter bounds, MVCC transactions, TTL expiration, LRU caching, Hashes/Lists/Sets, and hot backup/restores.

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
