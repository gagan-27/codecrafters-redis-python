import base64
import random

import socket
import string
import threading
import sys
import time
from dataclasses import dataclass
from enum import Enum

from typing import Dict, List, Optional
from app.resp import RespHandler
class Role(Enum):
    MASTER = "master"
    SLAVE = "slave"
@dataclass
class Value:
    v: any
    ts: Optional[int] = None
class State:
    def __init__(self):
        self.kv: Dict[str, Value] = {}
        self.role = None
        # only for master
        self.replid = None
        self.port = None
        # only for slave
        self.master_addr = None
        # only for slave
        self.master_port = None
        self.repl_socks = []
        self.sock_handler_map = {}
        self.threads = []
        self.slave_offset = 0

        self.lock = threading.Lock()
_state = State()
def handle_new_client(server_socket, state):
    while True:
        cs, _ = server_socket.accept()
        with state.lock:
            state.sock_handler_map[cs] = RespHandler(cs)
            print("Start a new thread for new connection")
            t = threading.Thread(target=handle_msg, args=(cs, state))
            t.start()

            state.threads.append(t)
def handle_msg(sock, state, for_replica=False):
    with state.lock:
        handler = state.sock_handler_map[sock]
    while True:
        cmds = handler.next_msg()
        msg_len = handler.last_msg_len()
        # hacky, could be binary
        cmds = [cmd.decode() for cmd in cmds]
        print("next msg:", cmds, "len: ", msg_len)
        match cmds[0].lower():
            case "ping":
                if not for_replica:
                    sock.sendall("+PONG\r\n".encode())
            case "replconf":
                if state.role == Role.SLAVE:
                    if cmds[1].lower() == "getack":
                        sock.sendall(
                            encode_array(
                                ["REPLCONF", "ACK", str(state.slave_offset)]
                            ).encode()
                        )
                else:
                    sock.sendall("+OK\r\n".encode())
            case "psync":
                if state.role == Role.MASTER:
                    sock.sendall(f"+FULLRESYNC {state.replid} 0\r\n".encode())
                    rdb_msg = f"${len(state.rdb_content)}\r\n"
                    sock.sendall(rdb_msg.encode() + state.rdb_content)
                    # Start to track replica
                    with state.lock:
                        state.repl_socks.append(sock)
            case "set":
                k = cmds[1]
                v = cmds[2]
                if len(cmds) > 3:
                    assert cmds[3].lower() == "px", "Only px option supported now."
                    expire_ts = int(cmds[4]) + ts_ms()
                else:
                    expire_ts = None
                with state.lock:
                    state.kv[k] = Value(v=v, ts=expire_ts)
                    if state.role == Role.MASTER:
                        with state.lock:
                            if len(state.repl_socks) > 0:
                                for slave in state.repl_socks:
                                    slave.sendall(encode_array(cmds).encode())
                        sock.sendall("+OK\r\n".encode())
            case "get":
                k = cmds[1]
                with state.lock:
                    v = state.kv.get(k, None)
                get_return = null_bulk_string()
                if v is not None:
                    if v.ts is None or ts_ms() < v.ts:
                        get_return = bulk_string(v.v)
                sock.sendall(get_return.encode())
            case "info":
                if not for_replica:
                    assert cmds[1].lower() == "replication"
                    d = {"role": state.role.value}
                    if state.role == Role.MASTER:
                        d["master_replid"] = state.replid
                        d["master_repl_offset"] = 0
                    res = "\n".join([f"{k}:{v}" for k, v in d.items()])
                    sock.sendall(bulk_string(res).encode())
            case "echo":
                if state.role == Role.MASTER:
                    v = cmds[1]
                    sock.sendall(bulk_string(v).encode())
            case cmd:
                raise RuntimeError(f"{cmd} is not supported yet.")

        state.slave_offset += msg_len
# def handle_clients(read_sockets, all_sockets, server_socket):
#     for sock in read_sockets:
#         if sock == server_socket:
#             cs, _ = server_socket.accept()
#             all_sockets.append(cs)
#             _sock_handler_map[cs] = RespHandler(cs)
#             print("Accepted a new connection.")
#         else:
#             try:
#                 data = sock.recv(1024)
#                 if not data:
#                     print("Client disconnect")
#                     all_sockets.remove(sock)
#                     sock.close()
#                 else:
#                     handle_req(sock, data.decode())
#             except socket.error as e:
#                 print(f"Socket error: {e}")
#                 all_sockets.remove(sock)
#                 sock.close()
def main():
    server_socket = socket.create_server(
        ("localhost", _state.port), backlog=5, reuse_port=True
    )
    with _state.lock:
        _state.sock_handler_map[server_socket] = RespHandler(server_socket)
        t = threading.Thread(target=handle_new_client, args=(server_socket, _state))
        t.start()

        _state.threads.append(t)
    if _state.role == Role.SLAVE:
        cs = socket.socket()
        rh = RespHandler(cs)
        with _state.lock:
            _state.sock_handler_map[cs] = rh
        cs.connect((_state.master_addr, _state.master_port))
        cs.sendall("*1\r\n$4\r\nping\r\n".encode())
        # wait for pong
        msg_pong = rh.next_msg().decode().lower()
        assert msg_pong == "pong", "We should get PONG back"
        # send two REPLCONF
        cs.sendall(
            encode_array(["REPLCONF", "listening-port", str(_state.port)]).encode()
        )
        msg_ok = rh.next_msg().decode()
        assert msg_ok == "OK"
        cs.sendall(encode_array(["REPLCONF", "capa", "psync2"]).encode())
        msg_ok = rh.next_msg().decode()
        assert msg_ok == "OK"
        cs.sendall(encode_array(["PSYNC", "?", "-1"]).encode())
        msg_fullsync = rh.next_msg().decode()
        msg_fullsync_arr = msg_fullsync.split(" ")
        assert msg_fullsync_arr[0].lower() == "fullresync"
        assert len(msg_fullsync_arr) == 3, "Should receive 3 parts for FULLRESYNC"
        # RDB message is ugly as it's not bulk string
        rdb_file_length = int(rh.extract_by_target()[1:])
        print(f"rdb_file_length {rdb_file_length}")
        _rdb_file = rh.extract_by_length(rdb_file_length, eat_crlf=False)
        print(f"Handshake with master is done. RDB file length: {len(_rdb_file)}")
        t = threading.Thread(target=handle_msg, args=(cs, _state,True))
        t.start()
        with _state.lock:

            _state.threads.append(t)
def encode_array(v: List[str]) -> str:
    prefix = f"*{len(v)}\r\n"
    suffix = "".join([bulk_string(e) for e in v])
    return f"{prefix}{suffix}"

def bulk_string(v: str | bytearray):

    return f"${len(v)}\r\n{v}\r\n"
def null_bulk_string():
    return "$-1\r\n"
def ts_ms():
    return int(round(time.time() * 1000))
# def handle_req(sock, req: str) -> List[str]:
#     global _repl_socks
#     r = req.split("\r\n")
#     assert r[0][0] == "*", "req is array"
#     match r[2].lower():
#         case "ping":
#             sock.sendall("+PONG\r\n".encode())
#         case "echo":
#             v = r[4]
#             sock.sendall(bulk_string(v).encode())
#         case "set":
#             k = r[4]
#             v = r[6]
#             if len(r) > 8:
#                 assert r[8].lower() == "px", "Only px option supported now."
#                 expire_ts = int(r[10]) + ts_ms()
#             else:
#                 expire_ts = None
#             _kv[k] = Value(v=v, ts=expire_ts)
#             if len(_repl_socks) > 0:
#                 for slave in _repl_socks:
#                     slave.sendall(req.encode())
#             sock.sendall("+OK\r\n".encode())
#         case "get":
#             k = r[4]
#             v = _kv.get(k, None)
#             get_return = null_bulk_string()
#             if v is not None:
#                 if v.ts is None or ts_ms() < v.ts:
#                     get_return = bulk_string(v.v)
#             sock.sendall(get_return.encode())
#         case "info":
#             assert r[4].lower() == "replication"
#             d = {"role": _role.value}
#             if _role == Role.MASTER:
#                 d["master_replid"] = _replid
#                 d["master_repl_offset"] = 0
#             res = "\n".join([f"{k}:{v}" for k, v in d.items()])
#             sock.sendall(bulk_string(res).encode())
#         case "replconf":
#             sock.sendall("+OK\r\n".encode())
#         case "psync":
#             sock.sendall(f"+FULLRESYNC {_replid} 0\r\n".encode())
#             rdb_msg = f"${len(_rdb_content)}\r\n"
#             sock.sendall(rdb_msg.encode() + _rdb_content)
#             # Start to track replica
#             _repl_socks.append(sock)
#         case cmd:
#             raise RuntimeError(f"{cmd} is not supported yet.")
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
    _state.role, _state.master_addr, _state.master_port = get_role()
    _state.port = get_port()
    _state.rdb_content = base64.b64decode(    "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    )
    
    if _state.role == Role.MASTER:
        replid = get_replid()
        _state.replid = replid
    main() #ggwp

    _state.threads[0].join()