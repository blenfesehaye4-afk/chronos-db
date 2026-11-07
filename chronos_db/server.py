"""AsyncIO TCP Wire Protocol Server & Client for ChronosDB with RESP & plain text."""

import asyncio
import json
import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from .engine import ChronosEngine
from .mvcc import Transaction
from .resp import RESPParser

logger = logging.getLogger("chronos.server")


class ChronosServer:
    """High-throughput async TCP server supporting RESP wire protocol & data structures."""

    def __init__(self, engine: ChronosEngine, host: str = "127.0.0.1", port: int = 6380):
        self.engine = engine
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None
        self._is_running = False

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        buffer = bytearray()
        active_txn: Optional[Transaction] = None

        while self._is_running:
            chunk = await reader.read(4096)
            if not chunk:
                break
            buffer.extend(chunk)

            while buffer:
                tokens, consumed = RESPParser.parse_command(bytes(buffer))
                if not tokens or consumed == 0:
                    break
                del buffer[:consumed]

                cmd = tokens[0].upper()
                try:
                    if cmd == "PING":
                        writer.write(RESPParser.serialize_simple_string("PONG"))

                    elif cmd == "SET":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SET'"))
                        else:
                            key, val = tokens[1], tokens[2]
                            ttl = None
                            if len(tokens) >= 5 and tokens[3].upper() in ("EX", "PX"):
                                ttl_val = float(tokens[4])
                                ttl = ttl_val if tokens[3].upper() == "EX" else (ttl_val / 1000.0)

                            if active_txn:
                                active_txn.put(key, val.encode("utf-8"))
                            else:
                                self.engine.put(key, val, ttl=ttl)
                            writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd == "SETEX":
                        if len(tokens) < 4:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SETEX'"))
                        else:
                            key = tokens[1]
                            seconds = float(tokens[2])
                            val = tokens[3]
                            self.engine.put(key, val, ttl=seconds)
                            writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd == "GET":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'GET'"))
                        else:
                            key = tokens[1]
                            res = active_txn.get(key) if active_txn else self.engine.get(key)
                            writer.write(RESPParser.serialize_bulk_string(res))

                    elif cmd == "DEL":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'DEL'"))
                        else:
                            deleted_count = 0
                            for k in tokens[1:]:
                                if active_txn:
                                    active_txn.delete(k)
                                    deleted_count += 1
                                else:
                                    if self.engine.delete(k):
                                        deleted_count += 1
                            writer.write(RESPParser.serialize_integer(deleted_count))

                    elif cmd == "EXPIRE":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'EXPIRE'"))
                        else:
                            key, sec = tokens[1], float(tokens[2])
                            ok = self.engine.expire(key, sec)
                            writer.write(RESPParser.serialize_integer(1 if ok else 0))

                    elif cmd == "PEXPIRE":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'PEXPIRE'"))
                        else:
                            key, ms = tokens[1], float(tokens[2])
                            ok = self.engine.expire(key, ms / 1000.0)
                            writer.write(RESPParser.serialize_integer(1 if ok else 0))

                    elif cmd == "TTL":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'TTL'"))
                        else:
                            key = tokens[1]
                            rem = self.engine.ttl_seconds(key)
                            writer.write(RESPParser.serialize_integer(int(rem) if rem > 0 else int(rem)))

                    elif cmd == "PTTL":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'PTTL'"))
                        else:
                            key = tokens[1]
                            rem = self.engine.ttl_seconds(key)
                            if rem > 0:
                                writer.write(RESPParser.serialize_integer(int(rem * 1000)))
                            else:
                                writer.write(RESPParser.serialize_integer(int(rem)))

                    # ----------------- Hashes -----------------
                    elif cmd == "HSET":
                        if len(tokens) < 4:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'HSET'"))
                        else:
                            key = tokens[1]
                            # Support multiple field-value pairs
                            mapping = {}
                            i = 2
                            while i < len(tokens) - 1:
                                mapping[tokens[i]] = tokens[i + 1]
                                i += 2
                            added = self.engine.structures.hset_multiple(key, mapping)
                            writer.write(RESPParser.serialize_integer(added))

                    elif cmd == "HGET":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'HGET'"))
                        else:
                            key, field = tokens[1], tokens[2]
                            val = self.engine.structures.hget(key, field)
                            writer.write(RESPParser.serialize_bulk_string(val))

                    elif cmd == "HDEL":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'HDEL'"))
                        else:
                            key = tokens[1]
                            fields = tokens[2:]
                            removed = self.engine.structures.hdel(key, *fields)
                            writer.write(RESPParser.serialize_integer(removed))

                    elif cmd == "HGETALL":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'HGETALL'"))
                        else:
                            key = tokens[1]
                            hdict = self.engine.structures.hgetall(key)
                            elements = []
                            for f, v in hdict.items():
                                elements.append(f)
                                elements.append(v)
                            writer.write(RESPParser.serialize_array(elements))

                    elif cmd == "HEXISTS":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'HEXISTS'"))
                        else:
                            key, field = tokens[1], tokens[2]
                            exists = self.engine.structures.hexists(key, field)
                            writer.write(RESPParser.serialize_integer(1 if exists else 0))

                    elif cmd == "HLEN":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'HLEN'"))
                        else:
                            key = tokens[1]
                            writer.write(RESPParser.serialize_integer(self.engine.structures.hlen(key)))

                    # ----------------- Lists -----------------
                    elif cmd == "LPUSH":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'LPUSH'"))
                        else:
                            key = tokens[1]
                            vals = tokens[2:]
                            new_len = self.engine.structures.lpush(key, *vals)
                            writer.write(RESPParser.serialize_integer(new_len))

                    elif cmd == "RPUSH":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'RPUSH'"))
                        else:
                            key = tokens[1]
                            vals = tokens[2:]
                            new_len = self.engine.structures.rpush(key, *vals)
                            writer.write(RESPParser.serialize_integer(new_len))

                    elif cmd == "LPOP":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'LPOP'"))
                        else:
                            key = tokens[1]
                            val = self.engine.structures.lpop(key)
                            writer.write(RESPParser.serialize_bulk_string(val))

                    elif cmd == "RPOP":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'RPOP'"))
                        else:
                            key = tokens[1]
                            val = self.engine.structures.rpop(key)
                            writer.write(RESPParser.serialize_bulk_string(val))

                    elif cmd == "LRANGE":
                        if len(tokens) < 4:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'LRANGE'"))
                        else:
                            key = tokens[1]
                            s, e = int(tokens[2]), int(tokens[3])
                            sublist = self.engine.structures.lrange(key, s, e)
                            writer.write(RESPParser.serialize_array(sublist))

                    elif cmd == "LLEN":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'LLEN'"))
                        else:
                            key = tokens[1]
                            writer.write(RESPParser.serialize_integer(self.engine.structures.llen(key)))

                    # ----------------- Sets -----------------
                    elif cmd == "SADD":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SADD'"))
                        else:
                            key = tokens[1]
                            members = tokens[2:]
                            added = self.engine.structures.sadd(key, *members)
                            writer.write(RESPParser.serialize_integer(added))

                    elif cmd == "SMEMBERS":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SMEMBERS'"))
                        else:
                            key = tokens[1]
                            members = self.engine.structures.smembers(key)
                            writer.write(RESPParser.serialize_array(members))

                    elif cmd == "SREM":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SREM'"))
                        else:
                            key = tokens[1]
                            members = tokens[2:]
                            removed = self.engine.structures.srem(key, *members)
                            writer.write(RESPParser.serialize_integer(removed))

                    elif cmd == "SISMEMBER":
                        if len(tokens) < 3:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SISMEMBER'"))
                        else:
                            key, member = tokens[1], tokens[2]
                            is_mem = self.engine.structures.sismember(key, member)
                            writer.write(RESPParser.serialize_integer(1 if is_mem else 0))

                    elif cmd == "SCARD":
                        if len(tokens) < 2:
                            writer.write(RESPParser.serialize_error("wrong number of arguments for 'SCARD'"))
                        else:
                            key = tokens[1]
                            writer.write(RESPParser.serialize_integer(self.engine.structures.scard(key)))

                    # ----------------- Generic / Admin -----------------
                    elif cmd == "SCAN":
                        prefix = tokens[1] if len(tokens) > 1 and tokens[1] != "*" else None
                        limit = int(tokens[2]) if len(tokens) > 2 else 50
                        results = self.engine.scan(start_key=prefix, limit=limit)
                        res_list = [f"{k}:{v.decode('utf-8', errors='replace')}" for k, v in results]
                        writer.write(RESPParser.serialize_array(res_list))

                    elif cmd in ("MULTI", "BEGIN"):
                        if active_txn:
                            writer.write(RESPParser.serialize_error("Transaction already in progress"))
                        else:
                            active_txn = self.engine.begin_transaction()
                            writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd in ("EXEC", "COMMIT"):
                        if not active_txn:
                            writer.write(RESPParser.serialize_error("EXEC without MULTI"))
                        else:
                            try:
                                active_txn.commit()
                                active_txn = None
                                writer.write(RESPParser.serialize_simple_string("OK"))
                            except Exception as e:
                                active_txn = None
                                writer.write(RESPParser.serialize_error(f"Transaction conflict: {str(e)}"))

                    elif cmd in ("DISCARD", "ROLLBACK"):
                        if not active_txn:
                            writer.write(RESPParser.serialize_error("DISCARD without MULTI"))
                        else:
                            active_txn.rollback()
                            active_txn = None
                            writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd in ("STATS", "INFO"):
                        stats = self.engine.stats()
                        writer.write(RESPParser.serialize_bulk_string(json.dumps(stats)))

                    elif cmd == "COMPACT":
                        self.engine.compact()
                        writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd in ("SNAPSHOT", "BGSAVE"):
                        target_dir = tokens[1] if len(tokens) > 1 else str(self.engine.data_dir / "snapshots" / "latest")
                        manifest = self.engine.export_snapshot(target_dir)
                        writer.write(RESPParser.serialize_bulk_string(json.dumps(manifest)))

                    elif cmd == "QUIT":
                        writer.write(RESPParser.serialize_simple_string("OK"))
                        await writer.drain()
                        break

                    else:
                        writer.write(RESPParser.serialize_error(f"unknown command '{cmd}'"))

                    await writer.drain()

                except Exception as e:
                    writer.write(RESPParser.serialize_error(str(e)))
                    await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def start(self) -> None:
        self._is_running = True
        self._server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info(f"ChronosDB server listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        self._is_running = False
        if self._server:
            self._server.close()
            await self._server.wait_closed()


class ChronosClient:
    """Async client for interacting with ChronosDB server over TCP using RESP protocol."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6380):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def _send_cmd(self, *tokens: str) -> Any:
        if not self.writer or not self.reader:
            raise ConnectionError("Client is not connected")

        # Encode as RESP array
        cmd_bytes = RESPParser.serialize_array(list(tokens))
        self.writer.write(cmd_bytes)
        await self.writer.drain()

        # Read response
        line = await self.reader.readline()
        if not line:
            return None
        line_str = line.decode("utf-8").strip()

        if line_str.startswith("+"):
            return line_str  # e.g. "+OK", "+PONG"

        elif line_str.startswith(":"):
            return int(line_str[1:])

        elif line_str.startswith("-"):
            raise RuntimeError(line_str[1:])

        elif line_str.startswith("$"):
            length = int(line_str[1:])
            if length == -1:
                return None
            data = await self.reader.readexactly(length + 2)
            return data[:-2].decode("utf-8")

        elif line_str.startswith("*"):
            count = int(line_str[1:])
            if count == -1:
                return None
            items = []
            for _ in range(count):
                item_hdr = await self.reader.readline()
                item_hdr_str = item_hdr.decode("utf-8").strip()
                if item_hdr_str.startswith("$"):
                    item_len = int(item_hdr_str[1:])
                    if item_len == -1:
                        items.append(None)
                    else:
                        d = await self.reader.readexactly(item_len + 2)
                        items.append(d[:-2].decode("utf-8"))
                elif item_hdr_str.startswith(":"):
                    items.append(int(item_hdr_str[1:]))
                else:
                    items.append(item_hdr_str)
            return items

        return line_str

    async def ping(self) -> str:
        return await self._send_cmd("PING")

    async def set(self, key: str, val: str, ex: Optional[int] = None) -> bool:
        tokens = ["SET", key, val]
        if ex is not None:
            tokens.extend(["EX", str(ex)])
        res = await self._send_cmd(*tokens)
        return res == "+OK"

    async def setex(self, key: str, seconds: int, val: str) -> bool:
        res = await self._send_cmd("SETEX", key, str(seconds), val)
        return res == "+OK"

    async def get(self, key: str) -> Optional[str]:
        return await self._send_cmd("GET", key)

    async def delete(self, key: str) -> bool:
        res = await self._send_cmd("DEL", key)
        return res >= 1 if isinstance(res, int) else False

    async def expire(self, key: str, seconds: int) -> bool:
        res = await self._send_cmd("EXPIRE", key, str(seconds))
        return res == 1

    async def ttl(self, key: str) -> int:
        res = await self._send_cmd("TTL", key)
        return int(res) if isinstance(res, int) else -2

    # Hashes
    async def hset(self, key: str, field: str, val: str) -> int:
        res = await self._send_cmd("HSET", key, field, val)
        return int(res) if isinstance(res, int) else 0

    async def hget(self, key: str, field: str) -> Optional[str]:
        return await self._send_cmd("HGET", key, field)

    async def hgetall(self, key: str) -> Dict[str, str]:
        arr = await self._send_cmd("HGETALL", key)
        if not arr:
            return {}
        result = {}
        for i in range(0, len(arr), 2):
            if i + 1 < len(arr):
                result[arr[i]] = arr[i + 1]
        return result

    # Lists
    async def lpush(self, key: str, *vals: str) -> int:
        res = await self._send_cmd("LPUSH", key, *vals)
        return int(res) if isinstance(res, int) else 0

    async def rpush(self, key: str, *vals: str) -> int:
        res = await self._send_cmd("RPUSH", key, *vals)
        return int(res) if isinstance(res, int) else 0

    async def lpop(self, key: str) -> Optional[str]:
        return await self._send_cmd("LPOP", key)

    async def rpop(self, key: str) -> Optional[str]:
        return await self._send_cmd("RPOP", key)

    async def lrange(self, key: str, start: int, stop: int) -> List[str]:
        res = await self._send_cmd("LRANGE", key, str(start), str(stop))
        return res if isinstance(res, list) else []

    # Sets
    async def sadd(self, key: str, *members: str) -> int:
        res = await self._send_cmd("SADD", key, *members)
        return int(res) if isinstance(res, int) else 0

    async def smembers(self, key: str) -> List[str]:
        res = await self._send_cmd("SMEMBERS", key)
        return res if isinstance(res, list) else []

    async def sismember(self, key: str, member: str) -> bool:
        res = await self._send_cmd("SISMEMBER", key, member)
        return res == 1

    # Transactions
    async def multi(self) -> bool:
        res = await self._send_cmd("MULTI")
        return res == "+OK"

    async def exec(self) -> bool:
        res = await self._send_cmd("EXEC")
        return res == "+OK"

    async def discard(self) -> bool:
        res = await self._send_cmd("DISCARD")
        return res == "+OK"

    async def stats(self) -> dict:
        res = await self._send_cmd("STATS")
        return json.loads(res) if res else {}

    async def compact(self) -> bool:
        res = await self._send_cmd("COMPACT")
        return res == "+OK"

    async def scan(self, prefix: str = "*", limit: int = 50) -> List[str]:
        res = await self._send_cmd("SCAN", prefix, str(limit))
        return res if isinstance(res, list) else []

    async def snapshot(self, target_dir: Optional[str] = None) -> dict:
        tokens = ["SNAPSHOT"] if not target_dir else ["SNAPSHOT", target_dir]
        res = await self._send_cmd(*tokens)
        return json.loads(res) if res else {}

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
