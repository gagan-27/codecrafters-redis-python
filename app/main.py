# Uncomment this to pass the first stage
import socket
import threading
CRLF= "\r\n"
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
def handle_connection(client_conn, client_addr):
    with client_conn:
        while True:
            data = client_conn.recv(1024)
            if not data:
                break
            response = handle_commands(data)
            client_conn.sendall(response)
def handle_commands(received_data: bytes) -> bytes:
    decoded_request = received_data.decode("utf-8").split(CRLF)
    cleaned_data = [decoded_request[i] for i in range(2, len(decoded_request), 2)]
    print(cleaned_data)
    match cleaned_data[0].upper():
        case "PING":
            return b"+PONG\r\n"
        case "ECHO":
            return f"+{cleaned_data[1]}\r\n".encode()

if __name__ == "__main__":
    main()
