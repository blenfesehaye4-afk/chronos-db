import time
from pathlib import Path
from chronos_db.engine import ChronosEngine
from chronos_db.ttl import TTLManager


def test_ttl_manager_lifecycle():
    ttl = TTLManager()

    # Set expiry
    ttl.set_expiry("k1", 10.0)
    assert ttl.get_ttl("k1") > 0
    assert ttl.is_expired("k1") is False

    # Expired key directly queried via get_ttl
    ttl.set_expiry("k2", -1.0)
    assert ttl.get_ttl("k2") == -2.0

    # Key evaluated by is_expired
    ttl.set_expiry("k3", -1.0)
    assert ttl.is_expired("k3") is True
    assert ttl.get_ttl("k3") == -1.0  # Evicted from tracking

    # Nonexistent key
    assert ttl.get_ttl("k_none") == -1.0

    # Remove expiry
    ttl.remove_expiry("k1")
    assert ttl.get_ttl("k1") == -1.0


def test_engine_passive_key_expiration(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path)

    # Insert with short TTL (0.1s)
    engine.put("temp:code", "123456", ttl=0.1)
    assert engine.get("temp:code") == b"123456"

    # Sleep past expiration
    time.sleep(0.15)
    assert engine.get("temp:code") is None

    # TTL query returns -2
    assert engine.ttl_seconds("temp:code") == -2.0

    engine.close()


def test_engine_expire_command(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path)

    # Key doesn't exist
    assert engine.expire("missing", 60) is False

    # Key exists
    engine.put("session:user", "token_abc")
    assert engine.ttl_seconds("session:user") == -1.0  # No expiry
    assert engine.expire("session:user", 5.0) is True
    assert engine.ttl_seconds("session:user") > 0

    engine.close()
