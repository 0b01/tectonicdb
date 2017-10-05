"""
python client for tectonic server
"""

import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_address = ('localhost', 9001)
sock.connect(server_address)

try:
    message = 'INFO\n'
    sock.sendall(message)

    amount_received = 0
    amount_expected = 10
    while amount_received < amount_expected:
        data = sock.recv(100000)
        amount_received += len(data)

    print data

finally:
    sock.close()