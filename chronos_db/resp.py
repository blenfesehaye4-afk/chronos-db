"""Redis Serialization Protocol (RESP2) parser and serializer."""

from typing import Any, List, Optional, Tuple, Union


class RESPParser:
    """Parses and serializes RESP wire protocol commands."""

    @staticmethod
    def serialize_simple_string(s: str) -> bytes:
        return f"+{s}\r\n".encode("utf-8")

    @staticmethod
    def serialize_error(err: str) -> bytes:
        return f"-ERR {err}\r\n".encode("utf-8")

    @staticmethod
    def serialize_integer(num: int) -> bytes:
        return f":{num}\r\n".encode("utf-8")

    @staticmethod
    def serialize_bulk_string(data: Optional[Union[str, bytes]]) -> bytes:
        if data is None:
            return b"$-1\r\n"
        b = data.encode("utf-8") if isinstance(data, str) else data
        return f"${len(b)}\r\n".encode("utf-8") + b + b"\r\n"

    @staticmethod
    def serialize_array(elements: List[Any]) -> bytes:
        out = [f"*{len(elements)}\r\n".encode("utf-8")]
        for elem in elements:
            if elem is None:
                out.append(b"$-1\r\n")
            elif isinstance(elem, (bytes, str)):
                out.append(RESPParser.serialize_bulk_string(elem))
            elif isinstance(elem, int):
                out.append(RESPParser.serialize_integer(elem))
            else:
                out.append(RESPParser.serialize_bulk_string(str(elem)))
        return b"".join(out)

    @classmethod
    def parse_command(cls, payload: bytes) -> Tuple[Optional[List[str]], int]:
        """Parse a RESP or plain line command. Returns (tokens, consumed_bytes)."""
        if not payload:
            return None, 0

        # Check if RESP array: *<num_elements>\r\n...
        if payload.startswith(b"*"):
            try:
                line_end = payload.index(b"\r\n")
                count = int(payload[1:line_end].decode("ascii"))
                pos = line_end + 2
                tokens = []

                for _ in range(count):
                    if pos >= len(payload) or payload[pos:pos + 1] != b"$":
                        return None, 0
                    str_len_end = payload.index(b"\r\n", pos)
                    str_len = int(payload[pos + 1:str_len_end].decode("ascii"))
                    pos = str_len_end + 2

                    if pos + str_len + 2 > len(payload):
                        return None, 0  # Incomplete data

                    raw_val = payload[pos:pos + str_len].decode("utf-8", errors="replace")
                    tokens.append(raw_val)
                    pos += str_len + 2

                return tokens, pos
            except (ValueError, IndexError):
                return None, 0

        # Fallback to plain text line
        if b"\r\n" in payload:
            line_end = payload.index(b"\r\n")
            raw_line = payload[:line_end].decode("utf-8", errors="replace").strip()
            return raw_line.split(), line_end + 2
        elif b"\n" in payload:
            line_end = payload.index(b"\n")
            raw_line = payload[:line_end].decode("utf-8", errors="replace").strip()
            return raw_line.split(), line_end + 1

        return None, 0
