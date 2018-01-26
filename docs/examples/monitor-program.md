## Example Tectonic Monitor

```python
from tectonic import TectonicDB
import time
import asyncio

async def monitor():
    db = TectonicDB()
    res = await db.countall()
    init = int(res[1])
    while 1:
        asyncio.sleep(1)
        res = await db.countall()
        new_count = int(res[1])
        print(new_count - init)
        init = new_count

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(monitor())
    loop.run_forever()
    loop.close()
```

This program prints the count delta every few seconds.
