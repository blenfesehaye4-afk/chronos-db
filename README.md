# 🗄️ ChronosDB

[![CI](https://github.com/blenfesehaye4-afk/chronos-db/actions/workflows/ci.yml/badge.svg)](https://github.com/blenfesehaye4-afk/chronos-db/actions/workflows/ci.yml)
[![Python Version](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> **High-performance Log-Structured Merge (LSM) Tree Key-Value Engine & Async Network Server in Python.**

ChronosDB is an embeddable, append-only LSM-Tree storage engine featuring Write-Ahead Logging (WAL) with CRC32 checksums, memory-efficient sparse indexed SSTables, probabilistic Bloom Filters for $O(1)$ negative key skipping, size-tiered compaction, and an asynchronous TCP wire protocol server.

---

## 🌟 Key Architecture & Features

```
[ Client Writes (PUT / DEL) ]
              │
              ├── 1. Append with CRC32
              ▼
   ┌──────────────────────┐
   │ Write-Ahead Log (WAL)│ ➔ Durable on-disk crash protection
   └──────────────────────┘
              │
              ├── 2. In-Memory Store
              ▼
   ┌──────────────────────┐
   │  MemTable (RAM)      │ ➔ Sorted in-memory dictionary
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

- 📝 **Write-Ahead Log (WAL)**: Ensures crash durability by fsyncing records with CRC32 checksums before memory admission.
- ⚡ **MemTable**: Fast in-memory sorted write buffer with real-time byte tracking.
- 🗃️ **Immutable SSTables**: Binary data layout with a 44-byte footer, sparse jump indexes, and integrated Bloom Filters.
- 🌸 **Probabilistic Bloom Filters**: Double-hashing Bloom filters for instant negative lookups with 0% false negative rate.
- 🔄 **Size-Tiered Compaction**: Automatic background or manual compaction merging multi-generation SSTables and discarding tombstone records.
- 🌐 **Async TCP Wire Server & REPL**: Non-blocking client connection pool supporting `PING`, `SET`, `GET`, `DEL`, `SCAN`, `STATS`, `COMPACT`.
- 📊 **Rich CLI**: Built-in throughput benchmark suite and live terminal inspection.

---

## 📦 Installation

```bash
pip install chronos-db
```

Or clone and install in development mode:

```bash
git clone https://github.com/blenfesehaye4-afk/chronos-db.git
cd chronos-db
pip install -e .[dev]
```

---

## 💻 Python Quickstart (Embeddable Engine)

```python
from chronos_db import ChronosEngine

# Initialize engine with custom data directory
db = ChronosEngine(data_dir="./chronos_data", memtable_max_bytes=1024 * 1024)

# Set and get keys
db.put("user:001", '{"name": "Alice", "role": "admin"}')
val = db.get("user:001")
print(val.decode("utf-8"))

# Range scan
results = db.scan(start_key="user:000", end_key="user:999", limit=50)
for k, v in results:
    print(k, v)

# Delete (writes tombstone)
db.delete("user:001")
assert db.get("user:001") is None

# Trigger compaction
db.compact()
db.close()
```

---

## 🖥️ Command-Line Interface (CLI)

### 1. Run Storage Benchmarks
```bash
chronos-db bench --ops 10000 --dir ./bench_data
```

### 2. Launch the TCP Server
```bash
chronos-db server --host 127.0.0.1 --port 6380 --dir ./chronos_data
```

### 3. Connect via Interactive REPL
```bash
chronos-db repl --host 127.0.0.1 --port 6380
```
```text
chronos:6380> SET cluster:status running
OK
chronos:6380> GET cluster:status
"running"
chronos:6380> STATS
{'memtable_entries': 1, 'memtable_bytes': 38, 'sstable_count': 1, ...}
```

---

## 🧪 Running Tests

```bash
pytest -v
```

---

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
