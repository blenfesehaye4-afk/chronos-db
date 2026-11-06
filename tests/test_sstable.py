from pathlib import Path
from chronos_db.sstable import SSTableReader, SSTableWriter


def test_sstable_write_and_indexed_read(tmp_path: Path):
    sst_file = tmp_path / "test.sst"
    entries = [
        ("key:001", b"val_1", False),
        ("key:005", b"val_5", False),
        ("key:010", None, True),  # tombstone
        ("key:020", b"val_20", False),
        ("key:099", b"val_99", False),
    ]

    SSTableWriter.write(sst_file, entries, index_interval=2)

    reader = SSTableReader(sst_file)
    assert reader.entry_count == 5

    # Found existing values
    val1, is_tomb1 = reader.get("key:001")
    assert val1 == b"val_1"
    assert is_tomb1 is False

    val20, is_tomb20 = reader.get("key:020")
    assert val20 == b"val_20"
    assert is_tomb20 is False

    # Found tombstone
    val10, is_tomb10 = reader.get("key:010")
    assert val10 is None
    assert is_tomb10 is True

    # Missing key
    assert reader.get("key:nonexistent") is None

    # Scan all
    all_items = reader.scan_all()
    assert len(all_items) == 5
    reader.close()
