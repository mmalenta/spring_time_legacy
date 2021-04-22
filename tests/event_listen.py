import socket

HOST = "localhost"
PORT = 8090

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))
    s.listen(1)
    conn, addr = s.accept()
    with conn:
      print("Connected by", addr)
      while True:
        data = conn.recv(1024)
        print("Received data")
        print(repr(data))
