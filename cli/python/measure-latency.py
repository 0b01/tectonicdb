from tectonic import TectonicDB
import time
from asyncio import get_event_loop

async def measure_latency():
    dts = []

    db = TectonicDB()

    t = time.time()
    for i in range(10000):
        await db.insert(0,0,True, True, 0., 0., 'default')
        t_ = time.time()
        dt = t_ - t
        t = t_
        dts.append(dt)
    print("AVG:", sum(dts) / len(dts))
    db.destroy()


if __name__ == "__main__":
    loop = get_event_loop()
    loop.create_task(measure_latency())
    loop.run_forever()
