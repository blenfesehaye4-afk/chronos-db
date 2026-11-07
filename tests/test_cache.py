from pathlib import Path
from chronos_db.cache import LRUBlockCache
from chronos_db.engine import ChronosEngine


def test_lru_cache_operations():
    cache = LRUBlockCache(max_entries=3)

    cache.put("a", b"1")
    cache.put("b", b"2")
    cache.put("c", b"3")

    assert cache.get("a") == b"1"
    assert cache.get("b") == b"2"

    # Insert 4th element -> 'c' was least recently used
    cache.put("d", b"4")
    assert cache.get("c") is None
    assert cache.get("d") == b"4"
    assert cache.get("a") == b"1"

    assert cache.stats()["hits"] >= 3
    assert cache.stats()["evictions"] == 1


def test_engine_lru_cache_integration(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path, cache_max_entries=100)

    engine.put("cached_key", "cached_val")
    # First get hits MemTable/Cache
    assert engine.get("cached_key") == b"cached_val"

    # Flush to SSTable on disk
    engine._flush_memtable()

    # Clear cache to force SSTable read
    engine.cache.clear()
    assert len(engine.cache) == 0

    # Get from SSTable -> populates cache
    val = engine.get("cached_key")
    assert val == b"cached_val"
    assert len(engine.cache) == 1

    # Second get hits cache
    hits_before = engine.cache.hits
    val2 = engine.get("cached_key")
    assert val2 == b"cached_val"
    assert engine.cache.hits == hits_before + 1

    engine.close()
