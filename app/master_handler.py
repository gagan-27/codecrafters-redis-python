import socket
import bisect
import time
from typing import Tuple
from common import (
    State,
    StreamBlock,
    Value,
    Wait,
    bulk_string, 
    encode_array,
    null_bulk_string,
    simple_error,
    simple_string,
    stream_entry_key_func,
    ts_ms,
)
def handle_waits(state: State):
    while True:
        with state.lock:
            for wait in state.waits[:]:
                if len(wait.dones) >= wait.num_replicas or wait.deadline_ms < ts_ms():
                    wait.return_sock.sendall(f":{len(wait.dones)}\r\n".encode())
                    state.waits.remove(wait)
            # handle stream blocks, only handle expired case
            for block in state.blocks[:]:
                if block.deadline_ms != -1 and block.deadline_ms < ts_ms():
                    # expired
                    block.return_sock.sendall(null_bulk_string().encode())
                    state.blocks.remove(block)
                    print(f"Resolved timeout block for {block.key} {block.entry}")
            
        time.sleep(0.05)
def handle_msg(sock: socket.socket, state: State):
    with state.lock:
        handler = state.sock_handler_map[sock]
    while True:
        # Msg handling
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
            case "keys":
                with state.lock:
                    if len(state.kv.keys()) > 0:
                        sock.sendall(encode_array(list(state.kv.keys())).encode())
            case "type":
                with state.lock:
                    key = cmds[1]
                    if key in state.kv:
                        sock.sendall(simple_string("string").encode())
                    elif key in state.skv:
                        sock.sendall(simple_string("stream").encode())
                    else:

                        sock.sendall(simple_string("none").encode())
            case "config":
                if cmds[1].lower() == "get":
                    if cmds[2].lower() == "dir":
                        sock.sendall(encode_array(["dir", state.dir]).encode())
                    elif cmds[2].lower() == "dbfilename":
                        sock.sendall(
                            encode_array(["dbfilename", state.dbfilename]).encode()
                        )
                    else:
                        raise RuntimeError(f"Not valid config key {cmds[2]}")
            case "replconf":
                if cmds[1].lower() == "ack":
                    slave_offset = int(cmds[2])
                    with state.lock:
                        for wait in state.waits:
                            if (
                                ts_ms() < wait.deadline_ms
                                and slave_offset >= wait.offset
                            ):
                                wait.dones.add(sock)
                                if len(wait.dones) >= wait.num_replicas:
                                    wait.return_sock.sendall(
                                        f":{len(wait.dones)}\r\n".encode()
                                    )
                                    state.waits.remove(wait)
                                    
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
            case "xadd":
                sk = cmds[1]
                eid = cmds[2]
                # full auto is a special case of right auto with left as timestamp
                full_auto = eid == "*"
                right_auto = full_auto or eid.split("-")[1] == "*"
                left_eid = int(eid.split("-")[0]) if not full_auto else ts_ms()
                if not right_auto and not stream_entry_key_func(eid) > (0, 0):
                    sock.sendall(
                        simple_error(
                            "ERR The ID specified in XADD must be greater than 0-0"
                        ).encode()
                    )
                    continue
                with state.lock:
                    if sk not in state.skv:
                        state.skv[sk] = {}
                        if right_auto:
                            right_eid = 0 if left_eid != 0 else 1
                            eid = f"{left_eid}-{right_eid}"
                    else:
                        # if existed key, check entryID
                        latest = max(state.skv[sk].keys(), key=stream_entry_key_func)
                        if right_auto:
                            (ll, lr) = stream_entry_key_func(latest)
                            if left_eid < ll:
                                sock.sendall(
                                    simple_error(
                                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                    ).encode()
                                )
                                continue
                            elif left_eid == ll:
                                right_eid = lr + 1
                                eid = f"{left_eid}-{right_eid}"
                            else:
                                # largest left eid, start from 0
                                eid = f"{left_eid}-0"
                        else:
                            if stream_entry_key_func(eid) <= stream_entry_key_func(
                                latest
                            ):
                                sock.sendall(
                                    simple_error(
                                        "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                    ).encode()
                                )
                                continue
                    state.skv[sk][eid] = {}
                    for k, v in zip(cmds[3::2], cmds[4::2]):
                        state.skv[sk][eid][k] = v
                    sock.sendall(bulk_string(eid).encode())
                    # not blocking current client, handle blocks
                    for block in state.blocks[:]:
                        if (
                            block.key == sk
                            and (block.deadline_ms == -1 or block.deadline_ms > ts_ms())
                            and stream_entry_key_func(block.entry)
                            < stream_entry_key_func(eid)
                        ):
                            # not expired, same key, larger new entity, yes
                            flat = []
                            for k, v in state.skv[sk][eid].items():
                                flat.append(k)
                                flat.append(v)
                            encode_flat = encode_array(flat)
                            one_entry_res = encode_array(
                                [bulk_string(eid), encode_flat],
                                bulk_encode=False,
                            )
                            entry_res = encode_array([one_entry_res], bulk_encode=False)
                            key_res = encode_array(
                                [bulk_string(sk), entry_res], bulk_encode=False
                            )
                            keys_res = encode_array([key_res], bulk_encode=False)
                            print("block resolved: ", keys_res)
                            block.return_sock.sendall(keys_res.encode())
                            state.blocks.remove(block)
                            print(f"Resolved block for {block.key} {block.entry}")
            case "xrange":
                stream_key = cmds[1]
                start_entry = cmds[2]
                end_entry = cmds[3]
                res = []
                with state.lock:
                    arr = sorted(
                        state.skv[stream_key].keys(), key=stream_entry_key_func
                    )
                    if start_entry == "-":
                        l = 0
                    else:
                        l = bisect.bisect_left(
                            arr,
                            stream_entry_key_func(start_entry),
                            key=stream_entry_key_func,
                        )
                        if l == len(arr):
                            sock.sendall(encode_array(res).encode())
                        if l + 1 < len(arr) and arr[l + 1] == start_entry:
                            l += 1
                    if end_entry == "+":
                        r = max(len(arr) - 1, 0)
                    else:
                        r = bisect.bisect_left(
                            arr,
                            stream_entry_key_func(end_entry),
                            key=stream_entry_key_func,
                        )
                        if r + 1 < len(arr) and arr[r + 1] == end_entry:
                            r += 1
                        if r == len(arr):
                            r = max(len(arr) - 1, 0)
                    for i in range(l, r + 1):
                        entry = arr[i]
                        kvs = state.skv[stream_key][entry]
                        flat = []
                        for k, v in kvs.items():
                            flat.append(k)
                            flat.append(v)
                        encode_flat = encode_array(flat)
                        res.append(
                            encode_array(
                                [bulk_string(entry), encode_flat], bulk_encode=False
                            )
                        )
                    to_return = encode_array(res, bulk_encode=False)
                    print("XRANGE return: ", to_return)

                    sock.sendall(to_return.encode())
            case "xread":
                if cmds[1].lower() == "block":
                    if int(cmds[2]) == 0:
                        deadline_ms = -1
                    else:
                        deadline_ms = int(cmds[2]) + ts_ms()        
                    stream_key = cmds[4]
                    start_entry = cmds[5]
                    if start_entry == "$":
                        # get latest entry
                        start_entry = max(
                            state.skv[stream_key].keys(), key=stream_entry_key_func
                        )

                    if len(cmds) > 6:
                        print("lhwlhw guess wrong: ", cmds)
                    with state.lock:
                        state.blocks.append(
                            StreamBlock(
                                key=stream_key,
                                entry=start_entry,
                                deadline_ms=deadline_ms,
                                return_sock=sock,
                            )
                        )
                else:
                    assert cmds[1].lower() == "streams"
                    stream_key_start_pairs = extract_key_start_pairs(cmds[2:])
                    res = []
                    with state.lock:
                        for stream_key, start_entry in stream_key_start_pairs:
                            one_return = fetch_all_given_stream_key(
                                state, stream_key, start_entry
                            )
                            res.append(one_return)
                        final_returns = encode_array(res, bulk_encode=False)
                        print("XREAD return: ", final_returns)
                        sock.sendall(final_returns.encode())

            case cmd:
                raise RuntimeError(f"{cmd} is not supported yet on master.")
def extract_key_start_pairs(x: list[str]) -> list[Tuple[str, str]]:
    res = []
    e = len(x) // 2
    for key, start in zip(x[0:e], x[e:]):
        res.append((key, start))
    return res
def fetch_all_given_stream_key(state, stream_key, start_entry):
    res_entry_list = []
    arr = sorted(state.skv[stream_key].keys(), key=stream_entry_key_func)
    l = bisect.bisect_right(
        arr,
        stream_entry_key_func(start_entry),
        key=stream_entry_key_func,
    )
    if l == len(arr):
        res_encoded = encode_array(res_entry_list).encode()
    else:
        r = max(len(arr) - 1, 0)
        for i in range(l, r + 1):
            entry = arr[i]
            kvs = state.skv[stream_key][entry]
            flat = []
            for k, v in kvs.items():
                flat.append(k)
                flat.append(v)
            encode_flat = encode_array(flat)
            res_entry_list.append(
                encode_array([bulk_string(entry), encode_flat], bulk_encode=False)
            )
        res_encoded = encode_array(res_entry_list, bulk_encode=False)
    # needs to add one extra wrap
    key_res_encoded = encode_array(
        [bulk_string(stream_key), res_encoded], bulk_encode=False
    )
    return key_res_encoded