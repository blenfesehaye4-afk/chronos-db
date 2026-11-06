from chronos_db.resp import RESPParser


def test_resp_parse_array_command():
    raw = b"*3\r\n$3\r\nSET\r\n$4\r\nuser\r\n$5\r\nalice\r\n"
    tokens, consumed = RESPParser.parse_command(raw)
    assert tokens == ["SET", "user", "alice"]
    assert consumed == len(raw)


def test_resp_serialize_types():
    assert RESPParser.serialize_simple_string("OK") == b"+OK\r\n"
    assert RESPParser.serialize_error("failed") == b"-ERR failed\r\n"
    assert RESPParser.serialize_integer(42) == b":42\r\n"
    assert RESPParser.serialize_bulk_string("hello") == b"$5\r\nhello\r\n"
    assert RESPParser.serialize_bulk_string(None) == b"$-1\r\n"
