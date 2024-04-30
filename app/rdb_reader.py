import io
import struct
from typing import Dict, Tuple
from app.common import Value
_rdb_kv = {}
def handle_fa(bf: io.BufferedReader):
    bf.read(1)
    # one fa entry contains
    # one string encoded key
    # one string encoded value
    k = read_string(bf)
    v = read_string(bf)
    return k, v
def read_signed_short(bf):
    return struct.unpack("h", bf.read(2))[0]
def read_signed_int(bf):
    return struct.unpack("i", bf.read(4))[0]
def read_signed_byte(bf):
    return struct.unpack("b", bf.read(1))[0]
# len, special_format
# len == -1, when special_format
def read_length(bf: io.BufferedReader) -> Tuple[int, int]:
    # [0] needs to convert bytes -> int for single bytes
    b1 = bf.read(1)[0]
    control = (b1 & 0b11000000) >> 6
    if control == 0b00:
        # next 6 bits represent length
        len = int(b1 & 0b00111111)
        return len, -1
    elif control == 0b1:
        # read one byte
        p1 = b1 & 0b00111111
        len = int(p1 << 8 | bf.read(1)[0])
        return len, -1
    elif control == 0b11:
        v = int(b1 & 0b00111111)
        return -1, v
    else:
        # 0b10
        raise ValueError("NYI")
def read_string(bf: io.BufferedReader):
    len, format = read_length(bf)
    if len == -1:
        if format == 0:
            # 8 byte int
            return str(read_signed_byte(bf))
        elif format == 1:
            # 16 bytes
            return str(read_signed_short(bf))
        else:
            # 32 bytes
            return str(read_signed_int(bf))
    else:
        return bf.read(len).decode()
def handle_fe(bf: io.BufferedReader):
    # eat op
    bf.read(1)
    db_number = int(bf.read(1)[0])
    assert 0xFB == bf.read(1)[0]
    hash_table_len, _ = read_length(bf)
    expire_table_len, _ = read_length(bf)
    print(
        f"db_num { db_number} hash_table_len: {hash_table_len} expire_table_len: {expire_table_len}"
    )
def handle_str_val(bf: io.BufferedReader, expire_ms=None):
    bf.read(1)
    k = read_string(bf)
    v = read_string(bf)
    print(f"KV: k: {k} v: {v}")
    _rdb_kv[k] = Value(v=v, ts=expire_ms)
def handle_eof(bf: io.BufferedReader):
    bf.read(1)
    # skip 8 bytes checksum
    bf.read(8)
def handle_expire_ms(bf: io.BufferedReader):
    bf.read(1)
    # ts_ms as unsigned long
    ts_ms = int(struct.unpack("L", bf.read(8))[0])
    handle_str_val(bf, ts_ms)
def handle_expire_s(bf: io.BufferedReader):
    bf.read(1)
    # ts_ms as unsigned long
    ts_s = int(struct.unpack("I", bf.read(4))[0])

    handle_str_val(bf, ts_s * 1000)
_op_map = {
    0xFA: handle_fa,
    0xFE: handle_fe,
    0x00: handle_str_val,
    0xFF: handle_eof,
    0xFC: handle_expire_ms,
    0xFD: handle_expire_s,
}
def read_rdb(path) -> Dict[str, str]:
    with open(path, "rb") as f:
        bf = io.BufferedReader(f)
        magic = bf.read(5)
        assert magic == b"REDIS"
        ver = bf.read(4)
        assert int(ver) > 0
        while True:
            op = bf.peek(1)
            if not op:
                print("EOF")
                break
            if len(op) > 1:
                op = op[0]
            if op in _op_map:
                _op_map[op](bf)
            else:
                print(f"what??  {op}")
                # for anything not implemented
                break

    return _rdb_kv