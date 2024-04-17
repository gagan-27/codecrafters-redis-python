# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, add = server_socket.accept()  # wait for client
    while True:
        message = conn.recv(1024).decode()
        if "ping" in message:
            pong_string = "+PONG\r\n"
            conn.send(pong_string.encode())


if __name__ == "__main__":
    main()
