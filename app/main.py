import base64
import random
import socket
import string
import threading
import sys
from app import master_handler, slave_handler, slave_replication_handler
from app.common import Role, State, encode_array
from app.resp import RespHandler

_state = State()
def handle_new_client(server_socket, state):
    while True:
        cs, _ = server_socket.accept()
        with state.lock:
            state.sock_handler_map[cs] = RespHandler(cs)
            print("Start a new thread for new connection")
            if state.role == Role.MASTER:
                t = threading.Thread(target=master_handler.handle_msg, args=(cs, state))
            else:
                t = threading.Thread(target=slave_handler.handle_msg, args=(cs, state))
            t.start()
            state.threads.append(t)
def main():
    server_socket = socket.create_server(
        ("localhost", _state.port), backlog=5, reuse_port=True
    )
    with _state.lock:
        _state.sock_handler_map[server_socket] = RespHandler(server_socket)
        t1 = threading.Thread(target=handle_new_client, args=(server_socket, _state))
        t1.start()
        _state.threads.append(t1)
        if _state.role == Role.MASTER:
            t2 = threading.Thread(target=master_handler.handle_waits, args=(_state,))
            t2.start()
            _state.threads.append(t2)
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
        t = threading.Thread(
            target=slave_replication_handler.handle_msg, args=(cs, _state)
        )
        t.start()
        with _state.lock:

            _state.threads.append(t)

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
    main() 

    _state.threads[0].join()