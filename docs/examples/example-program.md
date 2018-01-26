## Example Algorithmic Trading bot

```python
from tectonic import TectonicDB
import json
import asyncio

async def subscribe(name):
    db = TectonicDB(host="localhost", port=9001)
    _success, _text = await db.subscribe(name)
    while 1:
        _, item = await db.poll()
        if b"NONE" == item:
            await asyncio.sleep(0.01)
        else:
            yield json.loads(item)

class TickBatcher(object):
    def __init__(self, db_name):
        self.one_batch = []
        self.db_name = db_name

    async def batch(self):
        async for item in subscribe(self.db_name):
            self.one_batch.append(item)

    async def timer(self):
        while 1:
            await asyncio.sleep(1)     # do work every n seconds
            print(len(self.one_batch)) # do work here
            self.one_batch = []        # clear queue

if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    proc = TickBatcher("bnc_xrp_btc")
    loop.create_task(proc.batch())
    loop.create_task(proc.timer())

    loop.run_forever()
    loop.close()
```

This program operates on a batch of updates every n seconds. Could be useful for detecting pump and dumps.

