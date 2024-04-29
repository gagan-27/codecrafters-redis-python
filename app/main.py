import base64
import random
import select
import socket
import string
import sys
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional
class Role(Enum):
    MASTER = "master"
    SLAVE = "slave"
@dataclass
class Value:
    v: any
    ts: Optional[int] = None
_kv: Dict[str, Value] = {}
_role = None
_replid = None
_port = None
_master_addr = None
_master_port = None
def handle_clients(read_sockets, all_sockets, server_socket):
    for sock in read_sockets:
        if sock == server_socket:
            cs, _ = server_socket.accept()
            all_sockets.append(cs)
            print("accepted a new connection")
        else:
            try:
                data = sock.recv(1024)
                if not data:
                    print("client disconnect")
                    all_sockets.remove(sock)
                    sock.close()
                else:
                    handle_req(sock, data.decode())
            except socket.error as e:
                print(f"Socket error: {e}")
                all_sockets.remove(sock)
                sock.close()
def main():
    if _role == Role.SLAVE:
        cs = socket.socket()
        cs.connect((_master_addr, _master_port))
        try:
            cs.sendall("*1\r\n$4\r\nping\r\n".encode())
            # wait for pong
            res = cs.recv(1024).decode()
            assert "pong" in res.lower(), "Should get pong"
            # send two REPLCONF
            cs.sendall(
                encode_array(["REPLCONF", "listening-port", str(_port)]).encode()
            )
            res = cs.recv(1024).decode()
            assert res == "+OK\r\n"
            cs.sendall(encode_array(["REPLCONF", "capa", "psync2"]).encode())
            res = cs.recv(1024).decode()
            assert res == "+OK\r\n"
            cs.sendall(encode_array(["PSYNC", "?", "-1"]).encode())
            # TODO: decoding issue
            res = cs.recv(1024)
            # print(res[1:-2].split(" "))
        finally:
            cs.close()
    server_socket = socket.create_server(
        ("localhost", _port), backlog=5, reuse_port=True
    )
    all_sockets = [server_socket]
    while True:
        read_sockets, _, _ = select.select(all_sockets, [], [])
        handle_clients(read_sockets, all_sockets, server_socket)
def encode_array(v: List[str]) -> str:
    prefix = f"*{len(v)}\r\n"
    suffix = "".join([bulk_string(e) for e in v])
    return f"{prefix}{suffix}"
def bulk_string(v: str):
    return f"${len(v)}\r\n{v}\r\n"
def null_bulk_string():
    return "$-1\r\n"
def ts_ms():
    return int(round(time.time() * 1000))

def handle_req(sock, req: str) -> List[str]:
    r = req.split("\r\n")
    assert r[0][0] == "*", "req is array"
    match r[2].lower():
        case "ping":
            
            sock.sendall("+PONG\r\n".encode())
        case "echo":
            v = r[4]
            sock.sendall(bulk_string(v).encode())
        case "set":
            k = r[4]
            v = r[6]
            if len(r) > 8:
                assert r[8].lower() == "px", "Only px option supported now."
                expire_ts = int(r[10]) + ts_ms()
            else:
                expire_ts = None
            _kv[k] = Value(v=v, ts=expire_ts)
            
            sock.sendall("+OK\r\n".encode())
        case "get":
            k = r[4]
            v = _kv.get(k, None)
            get_return = null_bulk_string()
            if v is not None:
                if v.ts is None or ts_ms() < v.ts:
                    
                    get_return = bulk_string(v.v)
            sock.sendall(get_return.encode())
        case "info":
            assert r[4].lower() == "replication"
            d = {"role": _role.value}
            if _role == Role.MASTER:
                d["master_replid"] = _replid
                d["master_repl_offset"] = 0
            res = "\n".join([f"{k}:{v}" for k, v in d.items()])
            sock.sendall(bulk_string(res).encode())
        case "replconf":
            
            sock.sendall("+OK\r\n".encode())
        case "psync":
            
            sock.sendall(f"+FULLRESYNC {_replid} 0\r\n".encode())

            rdb_msg = f"${len(_rdb_content)}\r\n"
            sock.sendall(rdb_msg.encode() + _rdb_content)
        case cmd:
            raise RuntimeError(f"{cmd} is not supported yet.")
def get_port():
    if "--port" in sys.argv:
        idx = sys.argv.index("--port")
        port = int(sys.argv[idx + 1])
    else:
        port = 6379
    return port
def get_role():
    if "--replicaof" in sys.argv:
        idx = sys.argv.index("--replicaof")
        addr, port = sys.argv[idx + 1], int(sys.argv[idx + 2])
        return Role.SLAVE, addr, port
    else:
        return Role.MASTER, None, None
def get_replid():
    chrs = string.ascii_letters + string.digits
    return "".join(random.choices(chrs, k=40))
if __name__ == "__main__":
    role, _master_addr, _master_port = get_role()
    _role = role
    _port = get_port()
    _rdb_content = base64.b64decode(
        "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="

    )
    if _role == Role.MASTER:
        _replid = get_replid()
    main()

