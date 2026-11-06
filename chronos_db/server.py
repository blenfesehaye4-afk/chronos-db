"""AsyncIO TCP Wire Protocol Server & Client for ChronosDB with RESP & plain text."""

import asyncio
import json
import logging
from typing import Dict, List, Optional, Tuple

from .engine import ChronosEngine
from .mvcc import Transaction
from .resp import RESPParser

logger = logging.getLogger("chronos.server")


class ChronosServer:
    """High-throughput async TCP server supporting plain commands and RESP wire protocol."""

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
                            if active_txn:
                                active_txn.put(key, val.encode("utf-8"))
                            else:
                                self.engine.put(key, val)
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
                            key = tokens[1]
                            if active_txn:
                                active_txn.delete(key)
                                writer.write(RESPParser.serialize_integer(1))
                            else:
                                deleted = self.engine.delete(key)
                                writer.write(RESPParser.serialize_integer(1 if deleted else 0))

                    elif cmd == "SCAN":
                        prefix = tokens[1] if len(tokens) > 1 and tokens[1] != "*" else None
                        limit = int(tokens[2]) if len(tokens) > 2 else 50
                        results = self.engine.scan(start_key=prefix, limit=limit)
                        res_list = [f"{k}:{v.decode('utf-8', errors='replace')}" for k, v in results]
                        writer.write(RESPParser.serialize_array(res_list))

                    elif cmd == "MULTI" or cmd == "BEGIN":
                        if active_txn:
                            writer.write(RESPParser.serialize_error("Transaction already in progress"))
                        else:
                            active_txn = self.engine.begin_transaction()
                            writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd == "EXEC" or cmd == "COMMIT":
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

                    elif cmd == "DISCARD" or cmd == "ROLLBACK":
                        if not active_txn:
                            writer.write(RESPParser.serialize_error("DISCARD without MULTI"))
                        else:
                            active_txn.rollback()
                            active_txn = None
                            writer.write(RESPParser.serialize_simple_string("OK"))

                    elif cmd == "STATS":
                        stats = self.engine.stats()
                        writer.write(RESPParser.serialize_bulk_string(json.dumps(stats)))

                    elif cmd == "COMPACT":
                        self.engine.compact()
                        writer.write(RESPParser.serialize_simple_string("OK"))

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
    """Async client for interacting with ChronosDB server over TCP."""

    def __init__(self, host: str = "127.0.0.1", port: int = 6380):
        self.host = host
        self.port = port
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None

    async def connect(self) -> None:
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)

    async def _send_cmd(self, cmd_str: str) -> str:
        if not self.writer or not self.reader:
            raise ConnectionError("Client is not connected")
        self.writer.write(f"{cmd_str}\r\n".encode("utf-8"))
        await self.writer.drain()
        line = await self.reader.readline()
        line_str = line.decode("utf-8").strip()

        if line_str.startswith("$"):
            length = int(line_str[1:])
            if length == -1:
                return None
            data = await self.reader.readexactly(length + 2)
            return data[:-2].decode("utf-8")

        return line_str

    async def ping(self) -> str:
        return await self._send_cmd("PING")

    async def set(self, key: str, val: str) -> bool:
        res = await self._send_cmd(f"SET {key} {val}")
        return res == "+OK"

    async def get(self, key: str) -> Optional[str]:
        return await self._send_cmd(f"GET {key}")

    async def delete(self, key: str) -> bool:
        res = await self._send_cmd(f"DEL {key}")
        return res == ":1"

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

    async def close(self) -> None:
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

    async def scan(self, prefix: str = "*", limit: int = 50) -> List[str]:
        if not self.writer or not self.reader:
            raise ConnectionError("Client is not connected")
        self.writer.write(f"SCAN {prefix} {limit}\r\n".encode("utf-8"))
        await self.writer.drain()
        line = await self.reader.readline()
        line_str = line.decode("utf-8").strip()

        if line_str.startswith("*"):
            count = int(line_str[1:])
            items = []
            for _ in range(count):
                len_line = await self.reader.readline()
                str_len = int(len_line.decode("utf-8").strip()[1:])
                data = await self.reader.readexactly(str_len + 2)
                items.append(data[:-2].decode("utf-8"))
            return items
        return []
