from pathlib import Path
from chronos_db.wal import WriteAheadLog


def test_wal_append_and_recovery(tmp_path: Path):
    wal_file = tmp_path / "test.wal"
    wal = WriteAheadLog(wal_file)

    wal.append("alpha", b"123")
    wal.append("beta", b"456")
    wal.append("alpha", None, is_tombstone=True)
    wal.append("gamma", b"789")
    wal.close()

    # Recover
    wal2 = WriteAheadLog(wal_file)
    entries = wal2.recover()
    wal2.close()

    assert len(entries) == 4
    assert entries[0] == ("alpha", b"123", False)
    assert entries[1] == ("beta", b"456", False)
    assert entries[2] == ("alpha", None, True)
    assert entries[3] == ("gamma", b"789", False)


def test_wal_truncation(tmp_path: Path):
    wal_file = tmp_path / "test.wal"
    wal = WriteAheadLog(wal_file)
    wal.append("foo", b"bar")
    assert wal.filepath.stat().st_size > 0

    wal.truncate()
    assert wal.filepath.stat().st_size == 0
    assert len(wal.recover()) == 0
    wal.close()
