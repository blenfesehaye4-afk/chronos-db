from pathlib import Path
from chronos_db.engine import ChronosEngine


def test_hot_backup_and_restore(tmp_path: Path):
    db_dir = tmp_path / "main_db"
    backup_dir = tmp_path / "backup_export"

    engine = ChronosEngine(data_dir=db_dir)

    # Insert diverse data
    engine.put("k1", "val1")
    engine.put("k2", "val2")
    engine.structures.hset("hash:1", "f1", "v1")
    engine.structures.rpush("list:1", "item1", "item2")

    # Export snapshot
    manifest = engine.export_snapshot(backup_dir)
    assert manifest["format_version"] == "1.0"
    assert manifest["sstable_count"] >= 1
    assert (backup_dir / "manifest.json").exists()

    # Mutate & delete keys in engine
    engine.put("k1", "mutated_value")
    engine.delete("k2")
    assert engine.get("k1") == b"mutated_value"
    assert engine.get("k2") is None

    # Restore from snapshot
    restore_info = engine.restore_snapshot(backup_dir)
    assert restore_info["status"] == "success"

    # Verify original state is restored
    assert engine.get("k1") == b"val1"
    assert engine.get("k2") == b"val2"
    assert engine.structures.hget("hash:1", "f1") == "v1"
    assert engine.structures.lrange("list:1", 0, -1) == ["item1", "item2"]

    engine.close()
