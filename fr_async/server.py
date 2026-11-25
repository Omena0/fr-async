import socket

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('127.0.0.1', 5000))
s.listen(5)

while True:
    cs, addr = s.accept()
    print(addr)
    cs.sendall(b"test")
    print('Close')
    cs.close()
