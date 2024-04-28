import socket
from _thread import *
import threading
from datetime import datetime, timedelta
import sys
import argparse
REDIS_STORE_VAL = "val"
EXPIRY_START_TIME = "expiry_start_time"
EXPIRY_DURATION = "expiry_duration"
MASTER_ROLE = "master"
SLAVE_ROLE = "slave"
MY_DELIMITER="\r\n"

conn_lock = threading.Lock()
redis_store = {}
is_master = True
def parse_resp_protocal(resp_str):
    splited_txt = resp_str.split("\r\n")
    print(splited_txt)
    num_string = int(splited_txt[0][1])
    count = 1
    strs = []
    while count < num_string * 2:
        strs.append(splited_txt[count + 1])
        count += 2
    command = strs[0]
    arguments = strs[1:]
    print("command: ", command)
    print("arguments: ", arguments)
    return command, arguments
def build_resp_protocal(resp_data_type, response_str):
    result = ""
    delimiter = "\r\n"
    if resp_data_type == "+":  # simple strings
        result = resp_data_type + response_str + delimiter
    # $3\r\nhey\r\n
    elif resp_data_type == "$":  # bulk strings
        if response_str:
            
            result = (
                resp_data_type
                + str(len(response_str))
                + delimiter
                + response_str
                + delimiter
            )
            print("result: ", result)
        else:
            # $-1\r\n
            result = resp_data_type + "-1" + delimiter
    return result
def execute_set(args):
    # redis-cli set foo bar px 100
    if len(args) == 2:
        redis_store[args[0]] = args[1]
    elif len(args) > 2:
        if "px" in map(str.lower, args):
            tmp_list = [x.lower() for x in args]
            idx = tmp_list.index("px")
            val_dict = {
                REDIS_STORE_VAL: args[1],
                EXPIRY_START_TIME: datetime.now(),
                EXPIRY_DURATION: idx,
            }
            redis_store[args[0]] = val_dict
def execute_get(args):
    # get foo
    val = redis_store[args[0]]
    if val:
        pass
        if isinstance(val, dict):
            print("val:",val)
            if EXPIRY_START_TIME in val and EXPIRY_DURATION in val:
                if (
                    val[EXPIRY_START_TIME]
                    + timedelta(milliseconds=int(val[EXPIRY_DURATION]))
                ) < datetime.now():
                    del redis_store[args[0]]
                    return None
                else:
                    return val[REDIS_STORE_VAL]
            else:
                return val[REDIS_STORE_VAL]
        else:
            return val
    else:
        return None
        #    val = redis_store[args[0]]
        # if val:
        #     response = build_resp_protocal("$", val)
        # else:
        #     response = build_resp_protocal("$", none)
def execute_info(args):
    # info replication
    # $11\r\nrole:master\r\n
    role = MASTER_ROLE if is_master == True else SLAVE_ROLE
    replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset = 0
    resp_str = "role:" + role + MY_DELIMITER
    resp_str += "master_replid:" + replid + MY_DELIMITER

    resp_str += "master_repl_offset:" + str(master_repl_offset)
    print("resp_str: ", resp_str)
    return build_resp_protocal("$", resp_str)

    

def threading_connect(conn) -> None:
    with conn:
        while True:
            data = conn.recv(1024)
            print("data received", data)
            if data:
                cmd, args = parse_resp_protocal(data.decode())
                cmd = cmd.lower()
                response = None
                if cmd == "ping":
                    response = build_resp_protocal("+", "PONG")
                elif cmd == "echo":
                    response = build_resp_protocal("$", args[0])
                elif cmd == "set":
                    execute_set(args)
                    
                    response = build_resp_protocal("+", "OK")
                elif cmd == "get":
                    
                    val = execute_get(args)
                    if val:
                        response = build_resp_protocal("$", val)
                        
                    else:
                        response = build_resp_protocal("$", None)
                elif cmd == "info":
                    response=execute_info(args)       
                else:
                    print("unknown command")
                if response:
                    conn.sendall(str.encode(response))
            else:
                break
def parse_args():
    
    parser = argparse.ArgumentParser(description="Optional app description")
    parser.add_argument("--port", type=int, required=False)
    parser.add_argument("--replicaof", action="store_true", required=False)
    parser.add_argument("args", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    
    # args = parser.parse_known_args(["--port", "--replicaof"])
    print(args)
    return args
def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    # print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    print("original args: ",sys.argv)
    host = "localhost"
    parsed_args = parse_args()
    print("args: ", parsed_args)
    port = parsed_args.port if parsed_args.port else 6379
    global is_master
    is_master = not parsed_args.replicaof if parsed_args.replicaof else True
    print("is_master: ", is_master)
    
    
    server_socket = socket.create_server((host, port), reuse_port=True)
    print("socket is created")
    while True:
        conn, addr = server_socket.accept()  # wait for client
        # conn_lock.acquire()
        print("Connectd to :", addr[0], ":", addr[1])
        start_new_thread(threading_connect, (conn,))
        # conn_lock.release()
    server_socket.close()
if __name__ == "__main__":
    main()