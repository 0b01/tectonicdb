from tectonic import TectonicDB
import json
import asyncio

async def subscribe(name):
    db = TectonicDB()
    print(await db.subscribe(name))
    while 1:
        _, item = await db.poll()
        if item == b"NONE":
            await asyncio.sleep(0.01)
        else:
            yield json.loads(item)

class TickBatcher(object):
    def __init__(self, db_name):
        self.one_batch = []
        self.db_name = db_name

    async def batch(self):
        generator = subscribe(self.db_name)
        async for item in generator:
            self.one_batch.append(item)
    
    async def timer(self):
        while 1:
            await asyncio.sleep(5)
            print(len(self.one_batch))
        
    
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    proc = TickBatcher("bnc_xrp_btc")
    loop.create_task(proc.batch())
    loop.create_task(proc.timer())
    loop.run_forever()
    loop.close()
