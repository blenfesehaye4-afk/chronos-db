"""AsyncIO TCP Wire Protocol Server & Client for ChronosDB."""

import asyncio
import json
import logging
from typing import List, Optional, Tuple

from .engine import ChronosEngine

logger = logging.getLogger("chronos.server")


class ChronosServer:
    """High-throughput async TCP server exposing ChronosDB key-value engine."""

    def __init__(self, engine: ChronosEngine, host: str = "127.0.0.1", port: int = 6380):
        self.engine = engine
        self.host = host
        self.port = port
        self._server: Optional[asyncio.Server] = None
        self._is_running = False

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Parse and execute client commands."""
        while self._is_running:
            line = await reader.readline()
            if not line:
                break

            cmd_line = line.decode("utf-8").strip()
            if not cmd_line:
                continue

            parts = cmd_line.split(" ", 2)
            cmd = parts[0].upper()

            try:
                if cmd == "PING":
                    writer.write(b"+PONG\r\n")

                elif cmd == "SET":
                    if len(parts) < 3:
                        writer.write(b"-ERR wrong number of arguments for 'SET'\r\n")
                    else:
                        key = parts[1]
                        val = parts[2]
                        self.engine.put(key, val)
                        writer.write(b"+OK\r\n")

                elif cmd == "GET":
                    if len(parts) < 2:
                        writer.write(b"-ERR wrong number of arguments for 'GET'\r\n")
                    else:
                        key = parts[1]
                        res = self.engine.get(key)
                        if res is None:
                            writer.write(b"$-1\r\n")
                        else:
                            writer.write(f"${len(res)}\r\n".encode("utf-8") + res + b"\r\n")

                elif cmd == "DEL":
                    if len(parts) < 2:
                        writer.write(b"-ERR wrong number of arguments for 'DEL'\r\n")
                    else:
                        key = parts[1]
                        deleted = self.engine.delete(key)
                        writer.write(b":1\r\n" if deleted else b":0\r\n")

                elif cmd == "SCAN":
                    prefix = parts[1] if len(parts) > 1 and parts[1] != "*" else None
                    limit = int(parts[2]) if len(parts) > 2 else 50
                    results = self.engine.scan(start_key=prefix, limit=limit)
                    payload = json.dumps([{"key": k, "value": v.decode("utf-8", errors="replace")} for k, v in results])
                    writer.write(f"${len(payload)}\r\n{payload}\r\n".encode("utf-8"))

                elif cmd == "STATS":
                    stats = self.engine.stats()
                    payload = json.dumps(stats)
                    writer.write(f"${len(payload)}\r\n{payload}\r\n".encode("utf-8"))

                elif cmd == "COMPACT":
                    self.engine.compact()
                    writer.write(b"+OK\r\n")

                elif cmd == "QUIT":
                    writer.write(b"+OK\r\n")
                    await writer.drain()
                    break

                else:
                    writer.write(f"-ERR unknown command '{cmd}'\r\n".encode("utf-8"))

                await writer.drain()

            except Exception as e:
                err_msg = f"-ERR {str(e)}\r\n".encode("utf-8")
                writer.write(err_msg)
                await writer.drain()

        writer.close()
        await writer.wait_closed()

    async def start(self) -> None:
        """Start listening for incoming TCP client connections."""
        self._is_running = True
        self._server = await asyncio.start_server(self.handle_client, self.host, self.port)
        logger.info(f"ChronosDB server listening on {self.host}:{self.port}")

    async def stop(self) -> None:
        """Gracefully stop server."""
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
            data = await self.reader.readexactly(length + 2)  # includes \r\n
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

    async def scan(self, prefix: str = "*", limit: int = 50) -> List[dict]:
        res = await self._send_cmd(f"SCAN {prefix} {limit}")
        return json.loads(res) if res else []

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
