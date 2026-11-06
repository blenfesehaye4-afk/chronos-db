from pathlib import Path
from chronos_db.engine import ChronosEngine


def test_engine_basic_crud(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path, memtable_max_bytes=10000)

    engine.put("name", "Chronos")
    engine.put("version", "1.0")

    assert engine.get("name") == b"Chronos"
    assert engine.get("version") == b"1.0"
    assert engine.get("missing") is None

    # Delete
    deleted = engine.delete("name")
    assert deleted is True
    assert engine.get("name") is None

    engine.close()


def test_engine_flushing_and_compaction(tmp_path: Path):
    # Tiny memtable triggers frequent flushes
    engine = ChronosEngine(data_dir=tmp_path, memtable_max_bytes=200, compaction_threshold=3)

    for i in range(50):
        engine.put(f"item:{i:03d}", f"data_{i}")

    # Overwrite and delete some keys
    engine.put("item:010", "overwritten_10")
    engine.delete("item:020")

    assert engine.get("item:010") == b"overwritten_10"
    assert engine.get("item:020") is None

    # Verify range scan
    scanned = engine.scan(start_key="item:005", end_key="item:015")
    scanned_keys = [k for k, _ in scanned]
    assert "item:010" in scanned_keys
    assert "item:005" in scanned_keys
    assert "item:020" not in scanned_keys

    # Trigger compaction
    compact_path = engine.compact()
    assert len(engine.sstables) == 1
    assert engine.get("item:010") == b"overwritten_10"

    engine.close()


def test_engine_crash_recovery(tmp_path: Path):
    engine1 = ChronosEngine(data_dir=tmp_path, memtable_max_bytes=100000)
    engine1.put("k1", "v1")
    engine1.put("k2", "v2")
    engine1.wal.close()  # Simulate sudden termination without flush

    # Reopen
    engine2 = ChronosEngine(data_dir=tmp_path)
    assert engine2.get("k1") == b"v1"
    assert engine2.get("k2") == b"v2"
    engine2.close()
