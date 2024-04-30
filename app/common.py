import socket
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Set, Tuple
from app.resp import RespHandler
def ts_ms():
    return int(round(time.time() * 1000))
class Role(Enum):
    MASTER = "master"
    SLAVE = "slave"
@dataclass
class Value:
    v: any
    ts: Optional[int] = None
@dataclass
class Wait:
    offset: int
    num_replicas: int
    deadline_ms: int
    return_sock: socket.socket
    dones: Set[socket.socket] = field(default_factory=set)
class State:
    def __init__(self):
        self.kv: Dict[str, Value] = {}
        self.skv: Dict[str, Dict[str, Dict[str, str]]] = {}
        self.role = None
        # only for master
        self.replid = None
        self.port = None
        # only for slave
        self.master_addr = None
        # only for slave
        self.master_port = None
        # only for master
        self.repl_socks = []
        self.sock_handler_map: Dict[socket.socket, RespHandler] = {}
        self.threads = []
        self.lock = threading.Lock()
        # only for slave
        self.slave_offset = 0
        # only for masters
        self.master_offset = 0
        # only for master
        self.waits: List[Wait] = []
        self.dir: str = ""
        self.dbfilename: str = ""
def encode_array(v: List[str], bulk_encode=True) -> str:
    prefix = f"*{len(v)}\r\n"
    if bulk_encode:
        suffix = "".join([bulk_string(e) for e in v])
    else:
        suffix = "".join(v)
    return f"{prefix}{suffix}"
def bulk_string(v: str | bytearray) -> str:
    return f"${len(v)}\r\n{v}\r\n"
def null_bulk_string() -> str:
    return "$-1\r\n"
def simple_string(v: str) -> str:

    return f"+{v}\r\n"
stream_entry_key_func = lambda e: tuple(map(int, e.split("-")))

def simple_error(msg: str) -> str:
    return f"-{msg}\r\n"