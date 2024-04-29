import socket
import threading
import time
from app.common import (
    State,
    Value,
    Wait,
    bulk_string, #bc
    encode_array,
    null_bulk_string,
    ts_ms,
)
def handle_waits(state):
    while True:
        with state.lock:
            if len(state.waits) > 0:
                for wait in state.waits[:]:
                    if (
                        len(wait.dones) >= wait.num_replicas
                        or wait.deadline_ms < ts_ms()
                    ):
                        print("handled one wait in thread")
                        wait.return_sock.sendall(f":{len(wait.dones)}\r\n".encode())
                        state.waits.remove(wait)
        time.sleep(0.05)
def handle_msg(sock: socket.socket, state: State):
    with state.lock:
        handler = state.sock_handler_map[sock]
    while True:
        # Msg handling
        print("master wait for next msg")
        cmds = handler.next_msg()
        if not cmds:
            return
        msg_len = handler.last_msg_len()
        # hacky, could be binary
        cmds = [cmd.decode() for cmd in cmds]
        print("[master] next msg:", cmds, "len: ", msg_len)
        match cmds[0].lower():
            case "ping":
                sock.sendall("+PONG\r\n".encode())
            case "replconf":
                if cmds[1].lower() == "ack":
                    slave_offset = int(cmds[2])
                    print(f"One ACK from replica. offset: {slave_offset}")
                    with state.lock:
                        for wait in state.waits:
                            if (
                                ts_ms() < wait.deadline_ms
                                and slave_offset >= wait.offset
                            ):
                                wait.dones.add(sock)
                                if len(wait.dones) >= wait.num_replicas:
                                    print("Pending one wait happy case.")
                                    wait.return_sock.sendall(
                                        f":{len(wait.dones)}\r\n".encode()
                                    )
                                    state.waits.remove(wait)
                                    print("One wait happy case.")
                                print("One ACK handled from replica.")
                else:
                    sock.sendall("+OK\r\n".encode())
            case "psync":
                sock.sendall(
                    f"+FULLRESYNC {state.replid} {state.master_offset}\r\n".encode()
                )
                rdb_msg = f"${len(state.rdb_content)}\r\n"
                sock.sendall(rdb_msg.encode() + state.rdb_content)
                # Start to track replica
                with state.lock:
                    state.repl_socks.append(sock)
            case "wait":
                # One edge case not handling:
                #   get req time, replica X did not exist
                #   but it fullresync really fast and get past the
                #   `target_offset` before wait `timeout`
                num_replicas = int(cmds[1])
                timeout_ms = int(cmds[2])
                with state.lock:
                    target_offset = state.master_offset
                    if target_offset > 0:
                        state.waits.append(
                            Wait(
                                offset=target_offset,
                                num_replicas=num_replicas,
                                deadline_ms=timeout_ms + ts_ms(),
                                return_sock=sock,
                            )
                        )
                        print(
                            f"Request offset from {len(state.repl_socks)} replicas, target: {target_offset}"
                        )
                        for replica in state.repl_socks:
                            replica.sendall(
                                encode_array(["REPLCONF", "GETACK", "*"]).encode()
                            )
                    else:
                        sock.sendall(f":{len(state.repl_socks)}\r\n".encode())
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
                    # replicate
                    state.master_offset += msg_len
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
                assert cmds[1].lower() == "replication"
                d = {"role": state.role.value}
                d["master_replid"] = state.replid
                d["master_repl_offset"] = 0
                res = "\n".join([f"{k}:{v}" for k, v in d.items()])
                sock.sendall(bulk_string(res).encode())
            case "echo":
                v = cmds[1]
                sock.sendall(bulk_string(v).encode())
            case cmd:

                raise RuntimeError(f"{cmd} is not supported yet on master.")