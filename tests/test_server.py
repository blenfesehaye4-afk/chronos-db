import asyncio
import pytest
from pathlib import Path
from chronos_db.engine import ChronosEngine
from chronos_db.server import ChronosClient, ChronosServer


@pytest.mark.asyncio
async def test_tcp_server_commands(tmp_path: Path):
    engine = ChronosEngine(data_dir=tmp_path)
    server = ChronosServer(engine, host="127.0.0.1", port=6481)
    await server.start()

    client = ChronosClient(host="127.0.0.1", port=6481)
    await client.connect()

    # PING
    assert await client.ping() == "+PONG"

    # SET & GET
    assert await client.set("session:123", "active") is True
    assert await client.get("session:123") == "active"
    assert await client.get("nonexistent") is None

    # STATS / INFO
    stats = await client.stats()
    assert "memtable_entries" in stats
    assert "cache" in stats

    # DEL
    assert await client.delete("session:123") is True
    assert await client.get("session:123") is None

    # SCAN
    await client.set("user:a", "1")
    await client.set("user:b", "2")
    results = await client.scan(prefix="user:")
    assert len(results) >= 2

    # COMPACT
    assert await client.compact() is True

    # TTL & EXPIRE
    assert await client.set("token:temp", "secret123") is True
    assert await client.expire("token:temp", 10) is True
    assert await client.ttl("token:temp") > 0

    # SETEX
    assert await client.setex("otp:short", 30, "998877") is True
    assert await client.get("otp:short") == "998877"
    assert await client.ttl("otp:short") > 0

    # HASHES (HSET, HGET, HGETALL)
    assert await client.hset("huser:1", "name", "Blen") == 1
    assert await client.hset("huser:1", "role", "dev") == 1
    assert await client.hget("huser:1", "name") == "Blen"
    hdict = await client.hgetall("huser:1")
    assert hdict == {"name": "Blen", "role": "dev"}

    # LISTS (LPUSH, RPUSH, LRANGE, LPOP, RPOP)
    assert await client.rpush("lqueue", "item1", "item2") == 2
    assert await client.lpush("lqueue", "item0") == 3
    assert await client.lrange("lqueue", 0, -1) == ["item0", "item1", "item2"]
    assert await client.lpop("lqueue") == "item0"
    assert await client.rpop("lqueue") == "item2"

    # SETS (SADD, SMEMBERS, SISMEMBER)
    assert await client.sadd("set:tags", "db", "lsm") == 2
    assert await client.sismember("set:tags", "db") is True
    assert await client.sismember("set:tags", "nosql") is False
    assert sorted(await client.smembers("set:tags")) == ["db", "lsm"]

    # SNAPSHOT
    snapshot_dir = str(tmp_path / "server_snapshot")
    snap = await client.snapshot(snapshot_dir)
    assert snap.get("format_version") == "1.0"
    assert snap.get("sstable_count") >= 0

    await client.close()
    await server.stop()
    engine.close()
