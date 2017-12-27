from tectonic import TectonicDB
import time

def measure_latency():
    dts = []

    db = TectonicDB()

    t = time.time()
    for i in range(10000):
        db.insert(0,0,True, True, 0., 0., 'default')
        t_ = time.time()
        dt = t_ - t
        t = t_
        # print dt
        dts.append(dt)
    print "AVG:", sum(dts) / len(dts)
    db.destroy()
