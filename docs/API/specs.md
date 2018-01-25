# Socket Specification

Tectonic communicates over TCP with a simple scheme.

This is an example of API implementation [in Python](https://github.com/rickyhan/tectonicdb/blob/master/cli/python/tectonic.py):

```python
def __init__(self, host="localhost", port=9001):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_address = (host, port)
    self.sock.connect(server_address)

async def cmd(self, cmd):
    loop = asyncio.get_event_loop()

    if type(cmd) != str:
        message = (cmd.decode() + '\n').encode()
    else:
        message = (cmd+'\n').encode()
    loop.sock_sendall(self.sock, message)

    header = await loop.sock_recv(self.sock, 9)
    current_len = len(header)
    while current_len < 9:
        header += await loop.sock_recv(self.sock, 9-current_len)
        current_len = len(header)

    success, bytes_to_read = struct.unpack('>?Q', header)
    if bytes_to_read == 0:
        return success, ""

    body = await loop.sock_recv(self.sock, 1)
    body_len = len(body)
    while body_len < bytes_to_read:
        len_to_read = bytes_to_read - body_len
        if len_to_read > 32:
            len_to_read = 32
        body += await loop.sock_recv(self.sock, len_to_read)
        body_len = len(body)
    return success, body

def destroy(self):
    self.sock.close()

async def info(self):
    return await self.cmd("INFO")
```



