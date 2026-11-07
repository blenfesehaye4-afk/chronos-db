from pathlib import Path
from chronos_db.engine import ChronosEngine


def test_hash_operations(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path)
    h = engine.structures

    # HSET & HGET
    assert h.hset("user:profile", "name", "Blen") == 1
    assert h.hset("user:profile", "role", "Engineer") == 1
    assert h.hset("user:profile", "name", "Blen F") == 0  # Updated
    assert h.hget("user:profile", "name") == "Blen F"
    assert h.hget("user:profile", "missing") is None

    # HEXISTS & HLEN
    assert h.hexists("user:profile", "role") is True
    assert h.hexists("user:profile", "unknown") is False
    assert h.hlen("user:profile") == 2

    # HGETALL
    all_fields = h.hgetall("user:profile")
    assert all_fields == {"name": "Blen F", "role": "Engineer"}

    # HDEL
    assert h.hdel("user:profile", "role") == 1
    assert h.hlen("user:profile") == 1

    engine.close()


def test_list_operations(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path)
    l = engine.structures

    # RPUSH & LPUSH
    assert l.rpush("queue", "task_2", "task_3") == 2
    assert l.lpush("queue", "task_1") == 3
    assert l.llen("queue") == 3

    # LRANGE
    assert l.lrange("queue", 0, -1) == ["task_1", "task_2", "task_3"]
    assert l.lrange("queue", 0, 1) == ["task_1", "task_2"]

    # LPOP & RPOP
    assert l.lpop("queue") == "task_1"
    assert l.rpop("queue") == "task_3"
    assert l.llen("queue") == 1
    assert l.lpop("queue") == "task_2"
    assert l.lpop("queue") is None

    engine.close()


def test_set_operations(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path)
    s = engine.structures

    # SADD & SMEMBERS
    assert s.sadd("tech_stack", "python", "fastapi", "rocksdb") == 3
    assert s.sadd("tech_stack", "python") == 0  # duplicate
    assert s.scard("tech_stack") == 3
    assert s.smembers("tech_stack") == ["fastapi", "python", "rocksdb"]

    # SISMEMBER
    assert s.sismember("tech_stack", "python") is True
    assert s.sismember("tech_stack", "ruby") is False

    # SREM
    assert s.srem("tech_stack", "rocksdb") == 1
    assert s.scard("tech_stack") == 2
    assert s.sismember("tech_stack", "rocksdb") is False

    engine.close()
