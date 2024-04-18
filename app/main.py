# Uncomment this to pass the first stage
import socket
import threading
from .myredis import MyRedis


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    redis_db=MyRedis()
    while True:
        client_conn, client_addr = server_socket.accept()
        t = threading.Thread(target=handle_connection, args=[client_conn, redis_db])
        t.start()
def handle_connection(conn, redis_db):
    while data := conn.recv(1024):
        if b"ping" in data:
            conn.sendall(b"+PONG\r\n")
        elif b"echo" in data:
            array_elems: list[str] = data.split(b"$")
            for elem in array_elems:
                if elem.startswith(b"*") or b"echo" in elem:
                    continue
                conn.sendall(b"$" + elem)
        else:
            if b"set" in data:
                key = data.split()[4]
                value = data.split()[6]
                redis_db.set_value(key, value)
                conn.sendall(b"+OK\r\n")
            elif b"get" in data:
                key = data.split()[4]
                value = redis_db.get_value(key)
                if key:
                    conn.sendall(
                        b"$" + str(len(value)).encode() + b"\r\n" + value + b"\r\n"
                    )
                else:
                    conn.sendall(b"$-1\r\n")
"""decoded_request = received_data.decode("utf-8").split(CRLF)
    cleaned_data = [decoded_request[i] for i in range(2, len(decoded_request), 2)]
    print(cleaned_data)
    match cleaned_data[0].upper():
        case "PING":
            return b"+PONG\r\n"
        case "ECHO":

            return f"+{cleaned_data[1]}\r\n".encode()"""
if __name__ == "__main__":
    main()