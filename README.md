# 🗄️ ChronosDB

[![CI](https://github.com/blenfesehaye4-afk/chronos-db/actions/workflows/ci.yml/badge.svg)](https://github.com/blenfesehaye4-afk/chronos-db/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **High-performance Log-Structured Merge (LSM) Tree Key-Value Engine, MVCC ACID Transactions & RESP Wire Server in Python.**

ChronosDB is an embeddable, append-only LSM-Tree storage engine featuring Write-Ahead Logging (WAL) with CRC32 checksums, Multi-Version Concurrency Control (MVCC) with Snapshot Isolation, memory-efficient sparse indexed SSTables, probabilistic Bloom Filters, size-tiered compaction, and a Redis-compatible (RESP) asynchronous TCP server.

---

## 🌟 What's New in v0.2.0

- 🔒 **MVCC & Snapshot Isolation** ([`chronos_db/mvcc.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/chronos-db/chronos_db/mvcc.py)): Monotonic transaction identifiers provide consistent, point-in-time snapshot isolation. Long-running readers are never blocked by writers.
- ⚡ **Optimistic Concurrency Control (OCC)**: Atomic `MULTI` / `EXEC` / `DISCARD` write batches with First-Committer-Wins conflict detection.
- 📡 **RESP Wire Protocol** ([`chronos_db/resp.py`](file:///home/moaa/Desktop/my%20programs/bibi%20github/chronos-db/chronos_db/resp.py)): Fully compatible with standard Redis clients and toolchains using the RESP2 wire protocol specification.
- 🧹 **MVCC Vacuuming**: Automated collection and purging of expired version records older than the oldest active reader snapshot.

---

## 🏗️ Architecture

```
[ Client Writes (PUT / DEL / MULTI) ]
                 │
                 ├── 1. Append with CRC32
                 ▼
      ┌──────────────────────┐
      │ Write-Ahead Log (WAL)│ ➔ Durable on-disk crash protection
      └──────────────────────┘
                 │
                 ├── 2. In-Memory Store & MVCC Snapshot
                 ▼
      ┌──────────────────────┐
      │  MemTable & MVCC     │ ➔ Sorted RAM buffer & version history
      └──────────┬───────────┘
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
                   Merge SSTables & Purge Tombstones
```

---

## 💻 Python Quickstart (Transactions & MVCC)

```python
from chronos_db import ChronosEngine, TransactionConflictError

db = ChronosEngine(data_dir="./chronos_data")

# 1. Direct operations
db.put("balance:alice", "100")
db.put("balance:bob", "50")

# 2. ACID Transaction with Snapshot Isolation
txn = db.begin_transaction()
try:
    alice_bal = int(txn.get("balance:alice").decode())
    bob_bal = int(txn.get("balance:bob").decode())

    txn.put("balance:alice", str(alice_bal - 20).encode())
    txn.put("balance:bob", str(bob_bal + 20).encode())
    txn.commit()
    print("Transfer successful!")
except TransactionConflictError:
    txn.rollback()
    print("Conflict detected; transaction rolled back.")

db.close()
```

---

## 🖥️ Command-Line Interface (CLI)

```bash
# Launch RESP TCP server
chronos-db server --host 127.0.0.1 --port 6380 --dir ./chronos_data

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

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
