# Uncomment this to pass the first stage
import socket
import threading
def receive_client(conn):
    pong = "+PONG\r\n"
    while conn.recv(1024):
        conn.send(pong.encode())


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
     
    while True:
        conn, addr = server_socket.accept()
        t1 = threading.Thread(target=receive_client, args=(conn,))
        t2 = threading.Thread(target=receive_client, args=(conn,))
        t1.start()
        t2.start()


if __name__ == "__main__":
    main()
