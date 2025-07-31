import socket
from common import Role, State, Value, encode_array, ts_ms
def handle_msg(sock: socket.socket, state: State):
    assert state.role == Role.SLAVE
    with state.lock:
        handler = state.sock_handler_map[sock]
    while True:
        cmds = handler.next_msg()
        msg_len = handler.last_msg_len()
        # hacky, could be binary
        cmds = [cmd.decode() for cmd in cmds]
        print("[slave for master] next msg:", cmds, "len: ", msg_len)
        match cmds[0].lower():
            case "ping":
                # do nothing, master just send heartbeat
                pass
            case "replconf":
                if cmds[1].lower() == "getack":
                    print(f"Received offset request from master: {state.slave_offset}")
                    sock.sendall(
                        encode_array(
                            ["REPLCONF", "ACK", str(state.slave_offset)]
                        ).encode()
                    )
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
            case cmd:
                raise RuntimeError(f"{cmd} is not supported by replication socket.")

        state.slave_offset += msg_len