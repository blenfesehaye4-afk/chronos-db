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

    # STATS
    stats = await client.stats()
    assert "memtable_entries" in stats

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

    await client.close()
    await server.stop()
    engine.close()
