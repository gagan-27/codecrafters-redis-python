import socket
from app.common import State, Value, bulk_string, encode_array, null_bulk_string, ts_ms
def handle_msg(sock: socket.socket, state: State):
    with state.lock:
        handler = state.sock_handler_map[sock]
    while True:
        cmds = handler.next_msg()
        msg_len = handler.last_msg_len()
        # hacky, could be binary
        cmds = [cmd.decode() for cmd in cmds]
        print("[slave] next msg:", cmds, "len: ", msg_len)
        match cmds[0].lower():
            case "ping":
                sock.sendall("+PONG\r\n".encode())
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
                assert cmds[1].lower() == "replication"
                d = {"role": state.role.value}
                res = "\n".join([f"{k}:{v}" for k, v in d.items()])
                sock.sendall(bulk_string(res).encode())
            case cmd:
                raise RuntimeError(f"{cmd} is not supported yet on slave.")