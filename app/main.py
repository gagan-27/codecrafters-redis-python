import socket
from time import sleep

from datetime import datetime, timedelta
import threading
setter = {}  # Dictionary to store key-value pairs
def handle_connection(conn):
    while True:
        decoded_data = conn.recv(8000).decode()
        parts = decoded_data.strip().split("\r\n")
        commands = parts[2]

        expiry = ""
        print(commands, "received command")
        if commands.lower() == "echo":
            print(f"Echo: {parts[4]}")
            conn.send(f"+{parts[4]}\r\n".encode())
        elif commands.lower() == "set":
            print(f"parts: {parts}")
            data = parts[4]
            print(f"data: {data}")
            key, value = parts[4], parts[6]
            
            setter[key] = (value,)
            if len(parts) > 8 and parts[8].lower() == "px":
                expiry = parts[10]
                if expiry:
                    setter[key] = (value, int(expiry), datetime.now())
                else:

                    setter[key] = (value,)
            conn.send("+OK\r\n".encode())
        elif commands.lower() == "get":
            data = parts[4]
            val = setter.get(data)
            try:
                if isinstance(val, tuple) and len(val) == 3:
                    if datetime.now() > val[2] + timedelta(milliseconds=val[1]):
                        del setter[key]
                        print("$-1\r\n".encode())
                        conn.send("$-1\r\n".encode())
                    else:
                        print(value)
                        conn.send(f"+{val[0]}\r\n".encode())
                else:
                    print("NOT A TUPLE")
                    conn.send(f"+{value}\r\n".encode())
            except KeyError:
                
                conn.send("$-1\r\n".encode())
        else:
            conn.send("+PONG\r\n".encode())
def main() -> None:
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        conn, addr = server_socket.accept()  # wait for client
        connection_handler = threading.Thread(target=handle_connection, args=(conn,))
        connection_handler.start()
if __name__ == "__main__":
    main()