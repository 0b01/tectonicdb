"""
python client for tectonic server
"""

import socket
import json
import struct
import time
import sys

class TectonicDB():
    def __init__(self, host="localhost", port=9001):

        self.subscribed = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_address = (host, port)
        self.sock.connect(server_address)

    def cmd(self, command):
        if type(command) != str:
            message = (command.decode() + '\n').encode()
        else:
            message = (command+'\n').encode()
        self.sock.sendall(message)

        header = self.sock.recv(9)
        current_len = len(header)
        while current_len < 9:
            header += self.sock.recv(9-current_len)
            current_len = len(header)

        success, bytes_to_read = struct.unpack('>?Q', header)
        body = self.sock.recv(8)
        body_len = len(body)
        while body_len < bytes_to_read:
            len_to_read = bytes_to_read - body_len
            if len_to_read > 32:
                len_to_read = 32
            body += self.sock.recv(len_to_read)
            body_len = len(body)
        return success, body

    def destroy(self):
        self.sock.close()

    def info(self):
        return self.cmd("INFO")

    def countall(self):
        return self.cmd("COUNT ALL")

    def countall_in_mem(self):
        return self.cmd("COUNT ALL IN MEM")

    def ping(self):
        return self.cmd("PING")
    
    def help(self):
        return self.cmd("HELP")

    def insert(self, ts, seq, is_trade, is_bid, price, size, dbname):
        return self.cmd("INSERT {}, {}, {} ,{}, {}, {}; INTO {}"
                        .format( ts, seq, 
                            't' if is_trade else 'f',
                            't' if is_bid else 'f', price, size,
                            dbname))

    def add(self, ts, seq, is_trade, is_bid, price, size):
        return self.cmd("ADD {}, {}, {} ,{}, {}, {};"
                        .format( ts, seq, 
                            't' if is_trade else 'f',
                            't' if is_bid else 'f', price, size))
    def bulkadd(self, updates):
        self.cmd("BULKADD")
        for update in updates:
            ts, seq, is_trade, is_bid, price, size = update

            self.cmd("{}, {}, {} ,{}, {}, {};"
                    .format( ts, seq,
                            't' if is_trade else 'f', 
                            't' if is_bid else 'f', price, size))
        self.cmd("DDAKLUB")

    def getall(self):
        return json.loads(self.cmd("GET ALL AS JSON"));

    def get(self, n):
        success, ret = self.cmd("GET {} AS JSON".format(n))
        if success:
            return json.loads(ret)
        else:
            return None

    def clear(self):
        return self.cmd("CLEAR")

    def clearall(self):
        return self.cmd("CLEAR ALL")

    def flush(self):
        return self.cmd("FLUSH")

    def flushall(self):
        return self.cmd("FLUSH ALL")

    def create(self, dbname):
        return self.cmd("CREATE {}".format(dbname))

    def use(self, dbname):
        return self.cmd("USE {}".format(dbname))

    def unsubscribe(self):
        self.cmd("UNSUBSCRIBE")
        self.subscribed = False

    def subscribe(self, dbname):
        res = self.cmd("SUBSCRIBE {}".format(dbname))
        if res[0]:
            self.subscribed = True
        return res
    
    def poll(self):
        return self.cmd("")


